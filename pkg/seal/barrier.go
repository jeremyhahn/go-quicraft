// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
	"golang.org/x/crypto/hkdf"
)

const (
	// rootKeySize is the size of the generated root key in bytes.
	rootKeySize = 32

	// hkdfSaltSize is the size of the HKDF salt generated during Initialize.
	hkdfSaltSize = 32

	// epochSize is the byte size of an encoded epoch in ciphertext.
	epochSize = 8

	// algIDSize is the byte size of the algorithm identifier in ciphertext.
	algIDSize = 1

	// minCiphertextHeader is the fixed-size portion of the wire format that
	// precedes the variable-length nonce and ciphertext+tag. The minimum
	// valid ciphertext also requires at least a nonce and a tag, but those
	// sizes depend on the algorithm and are checked at parse time.
	minCiphertextHeader = epochSize + algIDSize

	// hkdfInfoPrefix is the fixed prefix for HKDF info strings.
	hkdfInfoPrefix = "quicraft/barrier/v1/"

	// hkdfInfoLenV1 is the length of the v1 HKDF info:
	// 20 bytes prefix + 8 bytes deploymentID + 8 bytes epoch.
	hkdfInfoLenV1 = 36

	// hkdfInfoLenV2 is the length of the v2 HKDF info: v1 plus the
	// algorithm identifier and the DEK length.
	hkdfInfoLenV2 = 38

	// bruteForceThreshold is the number of consecutive failed Unseal
	// attempts before exponential backoff activates.
	bruteForceThreshold = 5

	// bruteForceBaseDelay is the initial backoff delay after exceeding the threshold.
	bruteForceBaseDelay = 1 * time.Second

	// bruteForceMaxDelay is the maximum backoff delay.
	bruteForceMaxDelay = 60 * time.Second

	// bruteForceErrorThreshold is the number of failures that triggers ERROR-level logging.
	bruteForceErrorThreshold = 20
)

// HKDFVersion identifies the layout of the HKDF info string used to derive
// epoch DEKs. It is persisted in SealedRootKey so that data derived under an
// older layout keeps deriving under that layout after an upgrade.
type HKDFVersion uint8

const (
	// HKDFV1 is the original info layout: [prefix:20][deploymentID:8][epoch:8].
	// It has no algorithm/key-size domain separation: because HKDF is a
	// stream, the 16-byte AES-128 DEK for a given (deployment, epoch) is the
	// first half of that pair's 32-byte AES-256 DEK. HKDFV1 is the fallback
	// for sealed blobs persisted before the version field existed and must
	// stay derivable; new deployments use HKDFV2.
	HKDFV1 HKDFVersion = 1

	// HKDFV2 appends [algID:1][keyLen:1] to the v1 info, so a change of
	// BarrierConfig.Algorithm yields unrelated keys instead of related ones
	// and the unauthenticated algID header byte cannot select a key that is
	// a prefix of another.
	HKDFV2 HKDFVersion = 2

	// currentHKDFVersion is stamped into every newly initialized barrier.
	currentHKDFVersion = HKDFV2
)

// barrierState represents the lifecycle state of a Barrier.
type barrierState uint8

const (
	barrierUninitialized barrierState = iota
	barrierSealed
	barrierUnsealed
)

// Barrier provides authenticated encryption for at-rest data. It supports
// AES-128-GCM, AES-192-GCM, AES-256-GCM, ChaCha20-Poly1305, and
// XChaCha20-Poly1305, selected via BarrierConfig.Algorithm.
//
// The algorithm identifier is embedded in the ciphertext wire format and is
// used to size the nonce on the decrypt path. Note: decryption uses the AEAD
// the barrier is currently configured with (keyed by epoch), NOT an AEAD
// reconstructed from the header algID. A single deployment is therefore
// expected to use ONE algorithm for the life of its data; mixing algorithms
// within a deployment is not supported and a mismatch fails closed via AEAD
// authentication (ErrDecryptionFailed). The embedded algID exists for
// nonce-sizing and forensic/diagnostic purposes, not for runtime negotiation.
//
// Wire format (v2):
//
//	[epoch:8][algID:1][nonce:N][ciphertext+tag]
//
// where N is the algorithm's nonce size (12 for GCM/ChaCha20, 24 for XChaCha20).
//
// The barrier holds a root key, derives data encryption keys (DEKs) via
// HKDF-SHA256, and manages key rotation epochs. It enforces AEAD usage limits
// through InvocationCounter (NIST SP 800-38D invocation limits) and NonceTracker
// (nonce reuse detection). The barrier must be initialized once and unsealed before use.
//
// Hardware mode: When a SealingStrategy provides a BarrierEncryptor, the barrier
// delegates all encryption/decryption to the hardware-backed encryptor and skips
// epoch-based DEK derivation. In this mode, Rotate and PurgeEpochsBefore return
// ErrHardwareMode. Delegation is total: the epoch DEK/header, the nonce-reuse
// tracker and the NIST SP 800-38D invocation counter are all bypassed because
// the delegated encryptor owns its own key and nonce management (see the
// hwEncryptor field). A caller-supplied context is authenticated only when that
// encryptor implements AADSymmetricEncrypter; see ContextBinding,
// SupportsContextAAD and BarrierConfig.RequireContextAAD.
//
// Concurrency: Encrypt and Decrypt use a two-phase fast path:
//  1. Atomic check of b.unsealed (no contention).
//  2. RLock for the actual cipher operation (prevents TOCTOU with Seal).
//
// Seal, Unseal, Initialize, and Rotate acquire the exclusive write lock.
type Barrier struct {
	mu       sync.RWMutex
	unsealed atomic.Bool // fast-path check, avoids RWMutex contention on hot path

	config BarrierConfig // algorithm selection, deployment identity, tracking config

	state   barrierState
	rootKey *enclave.ProtectedKey // 32 bytes, encrypted at rest in memory via memguard enclave
	salt    []byte                // 32 bytes, generated at Initialize, used for HKDF

	// hkdfVersion is the DEK-derivation info layout in force. Set to
	// currentHKDFVersion for a fresh barrier and overwritten from the
	// persisted SealedRootKey by SetSealedRootKey.
	hkdfVersion HKDFVersion

	epoch      uint64                           // current key rotation epoch
	currentDEK *enclave.ProtectedKey            // derived via HKDF from rootKey + epoch, encrypted at rest
	cipher     cipher.AEAD                      // current epoch's AEAD cipher (cached, opaque)
	deks       map[uint64]*enclave.ProtectedKey // epoch -> DEK encrypted at rest for re-derivation
	ciphers    map[uint64]cipher.AEAD

	// knownEpochs tracks all epochs that have been used (via Initialize or
	// Rotate). Unlike deks/ciphers, this set is NOT cleared on seal(), so
	// Unseal can re-derive DEKs for all historical epochs.
	knownEpochs map[uint64]struct{}

	// sealed is the persisted sealed root key, held in memory so that
	// the barrier can be re-sealed without requiring the strategy.
	sealed *SealedRootKey

	// hwEncryptor is non-nil when a strategy provides its own symmetric
	// encryptor. In this mode, the barrier delegates all encrypt/decrypt
	// operations to that encryptor and skips DEK derivation.
	//
	// EVERY encrypt/decrypt path returns early here, so delegated mode bypasses
	// more than the cipher. It also bypasses, by design and not by accident:
	//
	//   - epoch DEK derivation and the [epoch:8][algID:1] ciphertext header;
	//   - the nonce-reuse tracker (NonceTrackerProvider);
	//   - the NIST SP 800-38D invocation counter (InvocationCounter);
	//   - the epoch AAD, unless the encryptor implements AADSymmetricEncrypter.
	//
	// The delegated encryptor owns its own key and nonce management, so
	// duplicating those safeguards here would count invocations against a key
	// the barrier does not control. This is stated rather than changed;
	// logDelegatedMode reports it at WARN at every barrier setup.
	//
	// Caller context is carried through only when the encryptor implements
	// AADSymmetricEncrypter; otherwise the *Context methods fail closed with
	// ErrHardwareMode. See ContextBinding.
	hwEncryptor SymmetricEncrypter

	// plaintextMode records that the installed hwEncryptor belongs to
	// StrategyNone, whose "encryptor" is an identity pass-through. It separates
	// "nothing is encrypted, so there is nothing to bind" from "an encryptor is
	// protecting this data but cannot bind it" — two states IsHardwareMode
	// reports identically. Set at Initialize/Unseal, cleared by seal().
	plaintextMode bool

	// strategyID and strategyHardwareBacked record the identity of the strategy
	// that produced the current encryptor, for the delegated-mode warning and
	// for operator-facing diagnostics. Set at Initialize/Unseal.
	strategyID             StrategyID
	strategyHardwareBacked bool

	// aeadProvider creates cipher.AEAD instances for each epoch's DEK. Defaults
	// to SoftwareAEADProvider; may be replaced with a hardware-backed provider
	// via BarrierConfig.AEADProvider.
	aeadProvider AEADProvider

	// AEAD safety trackers (interface types for IoC).
	invocations  InvocationCounter    // per-epoch AEAD invocation counter
	nonceTracker NonceTrackerProvider // per-epoch nonce reuse detection

	// Brute-force protection state.
	failedAttempts int64
	lastFailedAt   time.Time

	// In-flight epoch reference counts for purge coordination.
	inFlightMu     sync.Mutex
	inFlightEpochs map[uint64]int64

	// timeNow is injectable for testing brute-force backoff timing.
	timeNow func() time.Time

	// randReader is the source of cryptographic randomness. Defaults to
	// crypto/rand.Reader. Injectable for testing error paths.
	randReader io.Reader
}

// EpochBarrier is an alias for Barrier. Use this name in new code to
// distinguish from go-qrdb's StorageBarrier.
type EpochBarrier = Barrier

// NewBarrier creates a new Barrier in the uninitialized state.
//
// The config.DeploymentID is bound into the HKDF info parameter during DEK
// derivation, ensuring cryptographic isolation between clusters. Two clusters
// with identical root keys but different deployment IDs produce independent DEKs.
//
// The config.Algorithm selects the symmetric encryption algorithm. Both AEAD
// safety trackers (InvocationCounter and NonceTracker) are initialized from config.
//
// Panics if the configuration is invalid — an unknown algorithm, or a
// non-FIPS-approved cipher while FIPS mode is active. That is appropriate for
// a config built from compile-time constants, where an invalid value is a
// programmer error that should never reach production.
//
// A config assembled at RUNTIME — from a file, an API request, or per-tenant
// settings — should be checked with BarrierConfig.Validate first, so an
// operator's bad algorithm surfaces as an error instead of taking down the
// embedding process.
func NewBarrier(config BarrierConfig) *Barrier {
	if err := config.Validate(); err != nil {
		panic("seal.NewBarrier: " + err.Error())
	}

	var ic InvocationCounter
	if config.InvocationCounter != nil {
		ic = config.InvocationCounter
	} else {
		ic = NewInvocationCounter(true, config.InvocationLimit, config.RotationThreshold)
	}

	// Nonce tracking retains every nonce until Clear, and Clear is reached only
	// through a successful Rotate — which fails closed with ErrNoStatePersister
	// when no persister is configured. With no injected InvocationCounter
	// either, nothing else can release the tracked set, so this combination
	// provably cannot rotate and the tracker grows for the process lifetime.
	// Reported rather than rejected: it is what DefaultBarrierConfig produces,
	// and NewBarrier's only rejection mechanism is a panic. The tracker itself
	// fails safe at DefaultMaxTrackedNonces.
	if config.NonceTracking && config.InvocationCounter == nil && config.StatePersister == nil {
		slog.Warn("quicraft/seal: barrier configured with nonce tracking but no StatePersister and no InvocationCounter; the epoch cannot rotate, so tracked nonces are never released",
			"deployment_id", config.DeploymentID,
			"remedy", "configure StatePersister (and a rotation loop), inject an InvocationCounter, or disable NonceTracking",
		)
	}

	var nt NonceTrackerProvider
	if config.NonceTrackerProvider != nil {
		nt = config.NonceTrackerProvider
	} else if config.NonceTracking {
		nt = NewNonceTracker(true)
	}

	var ap AEADProvider
	if config.AEADProvider != nil {
		ap = config.AEADProvider
	} else {
		ap = NewSoftwareAEADProvider()
	}

	return &Barrier{
		config:         config,
		state:          barrierUninitialized,
		hkdfVersion:    currentHKDFVersion,
		deks:           make(map[uint64]*enclave.ProtectedKey),
		ciphers:        make(map[uint64]cipher.AEAD),
		knownEpochs:    make(map[uint64]struct{}),
		inFlightEpochs: make(map[uint64]int64),
		invocations:    ic,
		nonceTracker:   nt,
		aeadProvider:   ap,
		timeNow:        time.Now,
		randReader:     rand.Reader,
	}
}

// NewEpochBarrier is an alias for NewBarrier. Use this name in new code to
// distinguish from go-qrdb's NewStorageBarrier.
var NewEpochBarrier = NewBarrier

// InvocationCounter returns the barrier's AEAD invocation counter, or nil if
// invocation counting is disabled.
func (b *Barrier) InvocationCounter() InvocationCounter { return b.invocations }

// NonceTracker returns the barrier's nonce reuse detection tracker, or nil if
// nonce tracking is disabled.
func (b *Barrier) NonceTracker() NonceTrackerProvider { return b.nonceTracker }

// ShouldRotateKey returns true when the AEAD invocation counter has reached
// the configured rotation threshold percentage. Returns false if invocation
// counting is disabled.
func (b *Barrier) ShouldRotateKey() bool {
	if b.invocations == nil {
		return false
	}
	return b.invocations.ShouldRotate()
}

// Config returns the barrier's configuration.
func (b *Barrier) Config() BarrierConfig { return b.config }

// Initialize generates a 32-byte root key, derives the initial DEK via
// HKDF-SHA256, seals the root key using the provided strategy, and
// transitions the barrier to the unsealed state.
//
// If the strategy provides a BarrierEncryptor (hardware mode), the barrier
// delegates all encrypt/decrypt operations to the hardware encryptor and
// skips DEK derivation.
//
// The ctx and creds are forwarded to the strategy's SealRootKey method.
// Hardware-backed strategies may require credentials for authorization; the
// software strategy ignores both parameters.
//
// Initialize must be called exactly once (first boot). Subsequent calls
// return ErrBarrierAlreadyInit.
func (b *Barrier) Initialize(ctx context.Context, strategy SealingStrategy, creds Credentials) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUninitialized {
		return ErrBarrierAlreadyInit
	}

	// Generate root key from crypto/rand.
	rootKey := make([]byte, rootKeySize)
	if _, err := io.ReadFull(b.randReader, rootKey); err != nil {
		return &Error{Strategy: "initialize", Err: err}
	}

	// Generate HKDF salt.
	salt := make([]byte, hkdfSaltSize)
	if _, err := io.ReadFull(b.randReader, salt); err != nil {
		wipeBytes(rootKey)
		return &Error{Strategy: "initialize", Err: err}
	}

	const initialEpoch uint64 = 1

	// Seal the root key for persistent storage.
	sealed, err := strategy.SealRootKey(ctx, rootKey, creds)
	if err != nil {
		wipeBytes(rootKey)
		return err
	}
	sealed.Epoch = initialEpoch
	sealed.Salt = salt
	sealed.KnownEpochs = []uint64{initialEpoch}
	// Record the DEK-derivation layout so a future version can still derive
	// this deployment's keys after the layout changes again.
	sealed.HKDFVersion = b.hkdfVersion

	// Check if strategy provides a hardware encryptor.
	hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
	if hwErr != nil {
		wipeBytes(rootKey)
		return hwErr
	}

	// Resolve the context-binding capability BEFORE any state transition, so a
	// deployment that requires positional binding is refused at setup rather
	// than on its first context-bearing write.
	binding := contextBindingFor(hwEnc, strategy.ID() == StrategyNone)
	if err := b.checkContextAADPolicy(binding); err != nil {
		wipeBytes(rootKey)
		return err
	}
	b.strategyID = strategy.ID()
	b.strategyHardwareBacked = strategy.HardwareBacked()
	// Derived from the binding, not from the strategy ID alone: a strategy that
	// names itself "none" but supplies no encryptor takes the real epoch-AEAD
	// path, and reporting it as plaintext would be a lie in the direction that
	// hides an actual capability.
	b.plaintextMode = binding == ContextBindingPlaintext

	if hwEnc != nil {
		// Hardware mode: skip DEK derivation, use hardware encryptor.
		b.hwEncryptor = hwEnc
		b.rootKey = enclave.NewProtectedKey(rootKey)
		b.salt = salt
		b.epoch = initialEpoch
		b.knownEpochs[initialEpoch] = struct{}{}
		b.sealed = sealed
		b.state = barrierUnsealed
		b.unsealed.Store(true)
		b.logDelegatedMode("initialize", binding)
		slog.Info("barrier initialized (hardware mode)", "epoch", initialEpoch, "context_binding", binding.String())
		return nil
	}

	// Derive initial DEK and cipher for epoch 1.
	dek, aead, err := b.deriveEpochCipher(rootKey, salt, initialEpoch)
	if err != nil {
		wipeBytes(rootKey)
		return err
	}

	// Wrap key material in memguard enclaves (wipes the plain byte slices).
	dekPK := enclave.NewProtectedKey(dek)

	// Transition to unsealed state.
	b.rootKey = enclave.NewProtectedKey(rootKey)
	b.salt = salt
	b.epoch = initialEpoch
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks[initialEpoch] = dekPK
	b.ciphers[initialEpoch] = aead
	b.knownEpochs[initialEpoch] = struct{}{}
	b.sealed = sealed
	b.state = barrierUnsealed
	b.unsealed.Store(true)

	slog.Info("barrier initialized", "epoch", initialEpoch, "algorithm", b.config.Algorithm.String())
	return nil
}

// Unseal decrypts the root key using the provided strategy and derives the
// DEK for the current epoch. The barrier must be in the sealed state.
//
// If the strategy provides a BarrierEncryptor (hardware mode), the barrier
// delegates all encrypt/decrypt operations to the hardware encryptor and
// skips DEK re-derivation.
//
// The ctx and creds are forwarded to the strategy's UnsealRootKey method.
// Hardware-backed strategies may require credentials for authorization; the
// software strategy ignores both parameters.
//
// Brute-force protection: After 5 consecutive failed attempts, an exponential
// backoff delay is enforced (1s, 2s, 4s, ... capped at 60s). The counter
// resets on successful unseal.
func (b *Barrier) Unseal(ctx context.Context, strategy SealingStrategy, creds Credentials) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == barrierUninitialized {
		return ErrBarrierNotInit
	}
	if b.state == barrierUnsealed {
		return ErrBarrierUnsealed
	}

	// Brute-force backoff check.
	if b.failedAttempts >= bruteForceThreshold {
		elapsed := b.timeNow().Sub(b.lastFailedAt)
		// Guard against a backward clock step. If the persisted lastFailedAt is
		// in the future (e.g. the state was restored on a host whose clock is
		// behind, or the wall clock was stepped back), elapsed is negative and
		// would otherwise stay below delay forever, permanently locking out
		// unseal independently of any attacker. Treat a non-monotonic clock as
		// satisfying the delay and re-anchor lastFailedAt to now so subsequent
		// backoff intervals are measured from a sane baseline.
		if elapsed < 0 {
			b.lastFailedAt = b.timeNow()
			b.persistBackoffState()
		} else if elapsed < b.backoffDelay() {
			return ErrUnsealBackoff
		}
	}

	// Attempt to unseal the root key.
	rootKey, err := strategy.UnsealRootKey(ctx, b.sealed, creds)
	if err != nil {
		b.failedAttempts++
		b.lastFailedAt = b.timeNow()
		b.persistBackoffState()
		if b.failedAttempts >= bruteForceErrorThreshold {
			slog.Error("barrier unseal failed", "attempt", b.failedAttempts)
		} else if b.failedAttempts >= bruteForceThreshold {
			slog.Warn("barrier unseal failed", "attempt", b.failedAttempts)
		}
		return err
	}

	// No-encryption mode: NoneStrategy intentionally holds no root-key material
	// and returns an empty key. Wire up its identity encryptor directly,
	// BEFORE the 32-byte size gate below — otherwise the empty key is rejected
	// with ErrInvalidRootKeySize on every restart and, after enough restarts,
	// the persisted brute-force backoff would lock the node out of its own
	// plaintext data. The identity encryptor ignores key material, so no root
	// key is stored.
	//
	// The branch is gated on the CONSTRUCTED strategy, never on
	// b.sealed.Strategy: that field is plaintext and unauthenticated (the
	// AEAD AAD is a constant), so flipping it to "none" on disk would
	// otherwise steer a passphrase deployment into this branch and leave the
	// barrier "unsealed" with no cipher at all.
	if strategy.ID() == StrategyNone {
		hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
		if hwErr != nil {
			wipeBytes(rootKey)
			return hwErr
		}
		wipeBytes(rootKey)
		// A nil encryptor here would leave state=unsealed with cipher==nil
		// and hwEncryptor==nil, and the first Encrypt would nil-dereference.
		if hwEnc == nil {
			return ErrNoBarrierEncryptor
		}
		// No-encryption mode can never satisfy a demand for authenticated
		// positional binding: there is no AEAD to bind anything to.
		binding := contextBindingFor(hwEnc, true)
		if err := b.checkContextAADPolicy(binding); err != nil {
			return err
		}
		b.strategyID = strategy.ID()
		b.strategyHardwareBacked = strategy.HardwareBacked()
		b.plaintextMode = true
		b.hwEncryptor = hwEnc
		// Mirror Initialize's epoch assignment so CurrentEpoch() is symmetric
		// across Initialize and restart-via-Unseal. The value is cosmetic in
		// None mode (the identity encrypter ignores the epoch), but keeping it
		// consistent avoids surprising any consumer that reads CurrentEpoch().
		if b.sealed != nil {
			b.epoch = b.sealed.Epoch
		}
		b.state = barrierUnsealed
		b.unsealed.Store(true)
		b.failedAttempts = 0
		b.lastFailedAt = time.Time{}
		b.persistBackoffState()
		b.logDelegatedMode("unseal", binding)
		slog.Info("barrier unsealed (no-encryption mode)", "context_binding", binding.String())
		return nil
	}

	if len(rootKey) != rootKeySize {
		wipeBytes(rootKey)
		b.failedAttempts++
		b.lastFailedAt = b.timeNow()
		b.persistBackoffState()
		return ErrInvalidRootKeySize
	}

	// Check if strategy provides a hardware encryptor.
	hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
	if hwErr != nil {
		wipeBytes(rootKey)
		return hwErr
	}

	// Resolve the context-binding capability BEFORE any state transition; a
	// deployment that requires positional binding must be refused at setup.
	binding := contextBindingFor(hwEnc, false)
	if err := b.checkContextAADPolicy(binding); err != nil {
		wipeBytes(rootKey)
		return err
	}
	b.strategyID = strategy.ID()
	b.strategyHardwareBacked = strategy.HardwareBacked()
	b.plaintextMode = false

	if hwEnc != nil {
		// Hardware mode: skip DEK re-derivation, use hardware encryptor.
		b.hwEncryptor = hwEnc
		b.rootKey = enclave.NewProtectedKey(rootKey)
		b.state = barrierUnsealed
		b.unsealed.Store(true)
		b.failedAttempts = 0
		b.lastFailedAt = time.Time{}
		b.persistBackoffState()
		b.logDelegatedMode("unseal", binding)
		slog.Info("barrier unsealed (hardware mode)", "epoch", b.epoch, "context_binding", binding.String())
		return nil
	}

	// Derive DEK and cipher for the current epoch.
	dek, aead, err := b.deriveEpochCipher(rootKey, b.salt, b.epoch)
	if err != nil {
		wipeBytes(rootKey)
		return err
	}

	// Re-derive DEKs for all known epochs (survives seal/unseal cycles).
	// The current epoch is skipped: its DEK/cipher were already derived
	// above and are installed below. Deriving it again here would wrap a
	// second ProtectedKey enclave that the newDEKs[b.epoch] assignment
	// silently overwrites, leaving an unwiped duplicate of the current
	// epoch's DEK behind on every unseal.
	newDEKs := make(map[uint64]*enclave.ProtectedKey)
	newCiphers := make(map[uint64]cipher.AEAD)
	var plainDEKs [][]byte // track for cleanup on error
	for epoch := range b.knownEpochs {
		if epoch == b.epoch {
			continue
		}
		d, a, derr := b.deriveEpochCipher(rootKey, b.salt, epoch)
		if derr != nil {
			wipeBytes(rootKey)
			wipeBytes(dek)
			for _, v := range plainDEKs {
				wipeBytes(v)
			}
			return derr
		}
		plainDEKs = append(plainDEKs, d)
		newDEKs[epoch] = enclave.NewProtectedKey(d)
		newCiphers[epoch] = a
	}

	// Wrap current epoch DEK in enclave (wipes dek).
	dekPK := enclave.NewProtectedKey(dek)

	// Install the current epoch (skipped by the loop above; this also
	// covers the case where b.epoch is not in knownEpochs).
	newDEKs[b.epoch] = dekPK
	newCiphers[b.epoch] = aead

	// Transition to unsealed state. Wrap root key last (wipes rootKey).
	b.rootKey = enclave.NewProtectedKey(rootKey)
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks = newDEKs
	b.ciphers = newCiphers
	b.state = barrierUnsealed
	b.unsealed.Store(true)
	b.failedAttempts = 0
	b.lastFailedAt = time.Time{}
	b.persistBackoffState()

	// Enforce the NIST SP 800-38D invocation budget ACROSS process
	// lifetimes. The default in-memory counter does not survive restarts,
	// so after an unseal the number of AEAD invocations already performed
	// under the current epoch is unknown — and with random 96-bit nonces
	// the 2^32 budget per DEK is a hard nonce-collision safety bound, not
	// advisory. Unless the embedder owns seeding (injected counter) or
	// has already seeded this one, seed the counter pessimistically to
	// the rotation threshold so ShouldRotate fires immediately and the
	// epoch is rotated to a fresh DEK with a known-zero count. The
	// headroom between threshold and hard limit (default 5% ≈ 2×10^8
	// invocations) covers encryption traffic until rotation completes.
	b.seedInvocationsAfterUnseal()

	slog.Info("barrier unsealed", "epoch", b.epoch, "algorithm", b.config.Algorithm.String())
	return nil
}

// seedInvocationsAfterUnseal pessimistically seeds the AEAD invocation
// counter after an unseal when the true per-epoch count is unknown. See
// the call site in Unseal for the rationale. The caller must hold b.mu.
func (b *Barrier) seedInvocationsAfterUnseal() {
	if b.invocations == nil || !b.invocations.IsEnabled() {
		return
	}
	if b.config.InvocationCounter != nil {
		// The embedder injected its own counter and owns persistence and
		// seeding (the documented contract of
		// BarrierConfig.InvocationCounter). Trust its state.
		return
	}
	if b.invocations.Count() > 0 {
		// Already seeded — e.g. a seal/unseal cycle within one process,
		// where the in-memory count is still accurate, or an explicit
		// Seed() by the embedder before Unseal.
		return
	}
	limit := b.invocations.Limit()
	if limit <= 0 {
		return
	}
	// Ceil so the seeded usage is AT the threshold (a floor would land
	// fractionally below it and ShouldRotate would not fire).
	seed := int64(math.Ceil(float64(limit) * b.invocations.GetRotationThreshold() / 100.0))
	if seed <= 0 || seed >= limit {
		seed = limit - 1
	}
	b.invocations.Seed(seed)
	slog.Warn("AEAD invocation count for current epoch unknown after restart; "+
		"seeded counter to rotation threshold to force prompt key rotation",
		"epoch", b.epoch,
		"seeded_count", seed,
		"limit", limit,
	)
}

// seal zeroes the root key and all DEKs from memory, transitioning the
// barrier to the sealed state. Data is inaccessible until Unseal is called.
//
// This method is unexported because it must only be called through
// Host.Seal() or Host.Close(), which quiesce the engine pipeline before
// sealing to prevent ErrBarrierSealed storms.
func (b *Barrier) seal() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return ErrBarrierSealed
	}

	// Drop every live key enclave before dropping the map/field references.
	//
	// Precisely what this buys: enclave.ProtectedKey.Clear() releases the
	// enclave reference so the key can no longer be OPENED — it does NOT zero
	// the enclave's ciphertext, because memguard.Enclave exposes no API to do
	// so (see enclave.ProtectedKey.Clear). After seal(), the encrypted DEK
	// bytes and memguard's session coffer key therefore remain resident until
	// the GC reclaims them; what is gone is the barrier's ability to decrypt
	// them, and the plaintext key material (which lives only inside a
	// short-lived LockedBuffer that Open's callers Destroy). Clear is
	// idempotent and safe on an already-cleared key.
	if b.rootKey != nil {
		b.rootKey.Clear()
	}
	if b.currentDEK != nil {
		b.currentDEK.Clear()
	}
	for epoch, dek := range b.deks {
		if dek != nil {
			dek.Clear()
		}
		delete(b.deks, epoch)
	}

	b.rootKey = nil
	b.currentDEK = nil
	b.cipher = nil
	b.deks = make(map[uint64]*enclave.ProtectedKey)
	// Caveat: each cipher.AEAD below holds a COPY of its DEK inside its internal
	// key schedule (AES expanded key / ChaCha key). Go's stdlib and x/crypto
	// AEADs expose no key-wipe API, so dropping these references cannot zero
	// that copy — it lingers in freed heap until GC reclaims it. The enclave
	// DEKs above ARE actively zeroed; this AEAD-resident copy is a known stdlib
	// limitation (GC-bounded) and is the only key material not wiped here.
	b.ciphers = make(map[uint64]cipher.AEAD)
	b.hwEncryptor = nil
	// The encryptor is gone, so the capability it carried is gone with it. A
	// subsequent Unseal re-derives all three from the strategy it is given.
	b.plaintextMode = false
	b.strategyID = ""
	b.strategyHardwareBacked = false
	b.state = barrierSealed
	b.unsealed.Store(false)

	slog.Info("barrier sealed")
	return nil
}

// Seal is the exported variant of seal for testing and Host integration.
// Production code should prefer Host.Seal() which quiesces the pipeline first.
func (b *Barrier) Seal() error {
	return b.seal()
}

// Encrypt encrypts plaintext using the configured AEAD algorithm with the
// current epoch DEK. The output format is:
//
//	[epoch:8][algID:1][nonce:N][ciphertext+tag]
//
// In hardware mode, encryption is delegated to the hardware-backed encryptor
// and the output format is determined by the hardware strategy.
//
// dst may be provided as a pre-allocated buffer to reduce allocations. If dst
// is nil or too small, a new buffer is allocated. The dst parameter is ignored
// in hardware mode.
//
// dst MUST NOT overlap plaintext except exactly (crypto/cipher's in-place
// requirement): the AEAD seals directly into dst, and an inexact overlap
// PANICS inside crypto/cipher. Pass nil when in doubt.
//
// The method uses a two-phase fast path:
//  1. Atomic check of b.unsealed (no contention).
//  2. RLock for the cipher operation (prevents TOCTOU with Seal).
func (b *Barrier) Encrypt(dst, plaintext []byte) ([]byte, error) {
	// Phase 1: fast-path atomic check.
	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	// Phase 2: acquire RLock and re-check under lock.
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return b.hwEncryptor.Encrypt(plaintext)
	}

	return b.encryptLocked(dst, plaintext, b.epoch, b.cipher, nil)
}

// EncryptWithEpoch encrypts plaintext and returns both the ciphertext and the
// epoch used for encryption. This is used by WAL headers to record which epoch
// was used for each segment.
//
// dst MUST NOT overlap plaintext except exactly; see Encrypt.
//
// In hardware mode, the epoch is returned but the ciphertext format is
// determined by the hardware strategy.
func (b *Barrier) EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error) {
	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		ct, err := b.hwEncryptor.Encrypt(plaintext)
		if err != nil {
			return nil, 0, err
		}
		return ct, b.epoch, nil
	}

	ct, err := b.encryptLocked(dst, plaintext, b.epoch, b.cipher, nil)
	if err != nil {
		return nil, 0, err
	}
	return ct, b.epoch, nil
}

// EncryptWithEpochPinned encrypts plaintext and ATOMICALLY registers the epoch
// it sealed under as in-flight, returning the ciphertext and that epoch. The
// caller MUST DeregisterInFlightEpoch the returned epoch once the artifact's
// epoch is durably recorded.
//
// This closes a TOCTOU that EncryptWithEpoch + a separate RegisterInFlightEpoch
// cannot: EncryptWithEpoch releases b.mu before returning, so a Rotate followed
// by PurgeEpochsBefore — both of which take b.mu exclusively — can run to
// completion in the gap before the caller pins the returned epoch, dropping the
// DEK of a frame that was already sealed. A snapshot containing such a frame is
// permanently undecryptable. Registering the pin while the same b.mu read lock
// that produced the ciphertext is STILL HELD makes the seal and the pin a single
// atomic step with respect to any purge.
//
// Lock order matches PurgeEpochsBefore (b.mu then inFlightMu), so this cannot
// deadlock. Every returned epoch is pinned exactly once per successful call.
func (b *Barrier) EncryptWithEpochPinned(dst, plaintext []byte) ([]byte, uint64, error) {
	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		ct, err := b.hwEncryptor.Encrypt(plaintext)
		if err != nil {
			return nil, 0, err
		}
		b.pinInFlightLocked(b.epoch)
		return ct, b.epoch, nil
	}

	ct, err := b.encryptLocked(dst, plaintext, b.epoch, b.cipher, nil)
	if err != nil {
		return nil, 0, err
	}
	b.pinInFlightLocked(b.epoch)
	return ct, b.epoch, nil
}

// EncryptWithContext behaves like Encrypt but ADDITIONALLY folds a caller-supplied
// context into the AEAD AAD alongside the epoch. When the context uniquely
// identifies the frame's position or artifact (e.g. a WAL segment's
// SegmentID||RecordSeq or a snapshot's SnapshotIndex||ChunkSeq), it prevents an
// at-rest attacker from reordering, splicing, or truncating same-epoch frames:
// authentication fails unless the exact same context is supplied at decrypt time.
//
// The context is NOT stored in the ciphertext; the caller must reproduce the same
// bytes when decrypting. Passing a nil/empty context is equivalent to Encrypt and
// yields byte-identical, epoch-only-AAD output.
//
// Positional binding is opt-in: it exists ONLY when the caller passes a context.
// The wire format is unchanged; see Encrypt for dst/overlap requirements.
//
// In delegated (hardware) mode the context is carried through to the encryptor
// when it implements AADSymmetricEncrypter, using the same [epoch][context] AAD
// the software path builds. An encryptor WITHOUT that capability takes no AAD at
// all, so a non-empty context returns ErrHardwareMode rather than silently
// dropping the binding — a caller never believes it has positional protection it
// lacks. A nil/empty context is delegated to the encryptor unchanged.
func (b *Barrier) EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error) {
	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return b.delegatedEncrypt(plaintext, contextAAD)
	}

	return b.encryptLocked(dst, plaintext, b.epoch, b.cipher, contextAAD)
}

// EncryptWithEpochContext is EncryptWithEpoch with an additional caller context
// folded into the AAD; see EncryptWithContext for the context semantics and the
// hardware-mode behavior. It returns the epoch the frame was sealed under, as
// EncryptWithEpoch does, for callers (e.g. WAL) that record the epoch alongside
// the frame.
func (b *Barrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		ct, err := b.delegatedEncrypt(plaintext, contextAAD)
		if err != nil {
			return nil, 0, err
		}
		return ct, b.epoch, nil
	}

	ct, err := b.encryptLocked(dst, plaintext, b.epoch, b.cipher, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	return ct, b.epoch, nil
}

// EncryptWithEpochPinnedContext is EncryptWithEpochPinned with an additional
// caller context folded into the AAD; see EncryptWithContext for the context
// semantics and hardware-mode behavior, and EncryptWithEpochPinned for the pin
// contract (the caller MUST DeregisterInFlightEpoch the returned epoch once the
// artifact's epoch is durably recorded). The seal and the pin remain a single
// atomic step with respect to any purge.
func (b *Barrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		ct, err := b.delegatedEncrypt(plaintext, contextAAD)
		if err != nil {
			return nil, 0, err
		}
		b.pinInFlightLocked(b.epoch)
		return ct, b.epoch, nil
	}

	ct, err := b.encryptLocked(dst, plaintext, b.epoch, b.cipher, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.pinInFlightLocked(b.epoch)
	return ct, b.epoch, nil
}

// pinInFlightLocked increments the in-flight refcount for epoch. The caller must
// hold b.mu (read or write), which is what makes the pin atomic with respect to
// PurgeEpochsBefore's exclusive b.mu.
func (b *Barrier) pinInFlightLocked(epoch uint64) {
	b.inFlightMu.Lock()
	b.inFlightEpochs[epoch]++
	b.inFlightMu.Unlock()
}

// epochAAD constructs the Additional Authenticated Data (AAD) for AEAD
// operations. The AAD binds each ciphertext to its epoch, preventing cross-epoch
// ciphertext splicing attacks. The epoch is encoded as a fixed 8-byte
// little-endian uint64.
//
// Endianness note: The AAD uses little-endian encoding, while the epoch in the
// ciphertext header (written by encryptLocked) uses big-endian. These serve
// different purposes and are never compared directly:
//   - The ciphertext header epoch (big-endian) is for quick epoch identification
//     without decryption, matching the convention for network-visible fields.
//   - The AAD epoch (little-endian) is internal to AEAD authentication; the byte
//     order only needs to be consistent between encrypt and decrypt calls.
func epochAAD(epoch uint64) []byte {
	var aad [epochSize]byte
	binary.LittleEndian.PutUint64(aad[:], epoch)
	return aad[:]
}

// aadForEpoch composes the AEAD Additional Authenticated Data from the epoch and
// an OPTIONAL caller-supplied context. The context lets a caller additionally
// bind a ciphertext frame to its position or artifact (e.g. a snapshot's
// SnapshotIndex||ChunkSeq or a WAL segment's SegmentID||RecordSeq), so two frames
// sealed under the same DEK+epoch no longer authenticate identically and cannot
// be reordered, spliced, or truncated by an at-rest attacker.
//
// The epoch always occupies the fixed 8-byte little-endian prefix and the context
// is appended after it, so the epoch/context boundary is unambiguous: because the
// epoch is a fixed width, no context value can shift or alias the epoch bytes.
//
// A nil or empty context returns EXACTLY epochAAD(epoch) — the same bytes the
// epoch-only path uses — so context-free ciphertext produced by Encrypt/Decrypt
// stays byte-compatible and existing callers and stored ciphertext are unaffected.
func aadForEpoch(epoch uint64, contextAAD []byte) []byte {
	if len(contextAAD) == 0 {
		return epochAAD(epoch)
	}
	aad := make([]byte, epochSize+len(contextAAD))
	binary.LittleEndian.PutUint64(aad[:epochSize], epoch)
	copy(aad[epochSize:], contextAAD)
	return aad
}

// encryptLocked performs AEAD encryption with the algorithm-agile wire format.
// Caller must hold at least b.mu.RLock().
//
// contextAAD is OPTIONAL caller context folded into the AEAD AAD alongside the
// epoch (see aadForEpoch). A nil/empty contextAAD yields epoch-only AAD, keeping
// the wire format and stored bytes identical to the epoch-only path.
//
// Wire format: [epoch:8][algID:1][nonce:N][ciphertext+tag]
func (b *Barrier) encryptLocked(dst, plaintext []byte, epoch uint64, aead cipher.AEAD, contextAAD []byte) ([]byte, error) {
	nonceSize := aead.NonceSize()
	overhead := aead.Overhead()
	totalSize := epochSize + algIDSize + nonceSize + len(plaintext) + overhead

	// Reuse or allocate dst.
	var out []byte
	if cap(dst) >= totalSize {
		out = dst[:totalSize]
	} else {
		out = make([]byte, totalSize)
	}

	// Write epoch (big-endian) into the ciphertext header for quick epoch
	// identification without decryption. This deliberately differs from the
	// little-endian encoding used in epochAAD(); see epochAAD() for rationale.
	binary.BigEndian.PutUint64(out[:epochSize], epoch)

	// Write algorithm identifier.
	out[epochSize] = byte(b.config.Algorithm)

	// Generate random nonce.
	nonce := out[epochSize+algIDSize : epochSize+algIDSize+nonceSize]
	if _, err := io.ReadFull(b.randReader, nonce); err != nil {
		return nil, &Error{Strategy: "encrypt", Err: err}
	}

	// AEAD safety: nonce reuse detection.
	if b.nonceTracker != nil {
		if err := b.nonceTracker.CheckAndRecordNonce(nonce); err != nil {
			return nil, err
		}
	}

	// AEAD safety: invocation counting (NIST SP 800-38D).
	if b.invocations != nil {
		if err := b.invocations.Increment(); err != nil {
			return nil, err
		}
	}

	// Construct AAD binding the ciphertext to its epoch and, when supplied, to
	// the caller's positional/artifact context.
	aad := aadForEpoch(epoch, contextAAD)

	// Encrypt in place after epoch+algID+nonce prefix.
	// G407 false positive: nonce is randomly generated above via io.ReadFull(b.randReader, nonce).
	// The nonce slice points to a random-filled portion of out[epochSize+algIDSize:epochSize+algIDSize+nonceSize].
	ciphertextStart := epochSize + algIDSize + nonceSize
	aead.Seal(out[ciphertextStart:ciphertextStart], nonce, plaintext, aad) // #nosec G407 -- nonce is random, not hardcoded

	return out, nil
}

// Decrypt decrypts ciphertext using the algorithm-agile wire format. The epoch
// and algorithm identifier are extracted from the ciphertext prefix and the
// corresponding DEK is used for decryption.
//
// In hardware mode, decryption is delegated to the hardware-backed encryptor.
//
// dst MUST NOT overlap ciphertext except exactly (crypto/cipher's in-place
// requirement): the AEAD opens directly into dst, and an inexact overlap
// PANICS inside crypto/cipher. Pass nil when in doubt.
//
// Returns a single generic ErrDecryptionFailed for ALL failure modes (unknown
// epoch, authentication failure, corrupted ciphertext, unsupported algorithm)
// to prevent error-type oracle attacks.
func (b *Barrier) Decrypt(dst, ciphertext []byte) ([]byte, error) {
	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return b.hwEncryptor.Decrypt(ciphertext)
	}

	// Minimum: epoch(8) + algID(1) + smallest nonce(12) + tag(16) = 37
	if len(ciphertext) < minCiphertextHeader+12+16 {
		return nil, ErrDecryptionFailed
	}

	epoch := binary.BigEndian.Uint64(ciphertext[:epochSize])
	algID := SymmetricAlgorithm(ciphertext[epochSize])

	aead, ok := b.ciphers[epoch]
	if !ok {
		return nil, ErrDecryptionFailed
	}

	return b.decryptLocked(dst, ciphertext, aead, algID, nil)
}

// DecryptForEpoch decrypts ciphertext using a specific epoch's DEK. This is
// used when reading old WAL segments that record the epoch in their header.
//
// In hardware mode, the epoch parameter is ignored and decryption is delegated
// to the hardware-backed encryptor.
//
// dst MUST NOT overlap ciphertext except exactly; see Decrypt.
//
// Returns ErrDecryptionFailed for all failure modes to prevent error-type oracles.
func (b *Barrier) DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error) {
	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return b.hwEncryptor.Decrypt(ciphertext)
	}

	// Minimum: epoch(8) + algID(1) + smallest nonce(12) + tag(16) = 37
	if len(ciphertext) < minCiphertextHeader+12+16 {
		return nil, ErrDecryptionFailed
	}

	algID := SymmetricAlgorithm(ciphertext[epochSize])

	aead, ok := b.ciphers[epoch]
	if !ok {
		return nil, ErrDecryptionFailed
	}

	return b.decryptLocked(dst, ciphertext, aead, algID, nil)
}

// DecryptWithContext behaves like Decrypt but ADDITIONALLY requires the same
// caller context that was folded into the AAD at encryption time (see
// EncryptWithContext). Supplying a different context — or omitting one that was
// present at encryption — fails authentication with the generic
// ErrDecryptionFailed, exactly like any other tamper. Passing a nil/empty context
// reproduces the epoch-only AAD, so ciphertext produced by Encrypt decrypts
// unchanged.
//
// In delegated (hardware) mode the context is carried through to an encryptor
// that implements AADSymmetricEncrypter, under the current epoch's AAD (see
// delegatedDecrypt). An encryptor without that capability returns
// ErrHardwareMode for a non-empty context; a nil/empty context is delegated
// unchanged.
func (b *Barrier) DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error) {
	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return b.delegatedDecrypt(ciphertext, contextAAD, b.epoch)
	}

	// Minimum: epoch(8) + algID(1) + smallest nonce(12) + tag(16) = 37
	if len(ciphertext) < minCiphertextHeader+12+16 {
		return nil, ErrDecryptionFailed
	}

	epoch := binary.BigEndian.Uint64(ciphertext[:epochSize])
	algID := SymmetricAlgorithm(ciphertext[epochSize])

	aead, ok := b.ciphers[epoch]
	if !ok {
		return nil, ErrDecryptionFailed
	}

	return b.decryptLocked(dst, ciphertext, aead, algID, contextAAD)
}

// DecryptForEpochContext is DecryptForEpoch with the same caller context that was
// folded into the AAD at encryption time; see DecryptWithContext for the context
// semantics and hardware-mode behavior. It decrypts under the explicitly named
// epoch's DEK, as DecryptForEpoch does, for callers (e.g. WAL) that carry the
// epoch in an out-of-band header.
func (b *Barrier) DecryptForEpochContext(dst, ciphertext []byte, epoch uint64, contextAAD []byte) ([]byte, error) {
	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.unsealed.Load() {
		return nil, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return b.delegatedDecrypt(ciphertext, contextAAD, epoch)
	}

	// Minimum: epoch(8) + algID(1) + smallest nonce(12) + tag(16) = 37
	if len(ciphertext) < minCiphertextHeader+12+16 {
		return nil, ErrDecryptionFailed
	}

	algID := SymmetricAlgorithm(ciphertext[epochSize])

	aead, ok := b.ciphers[epoch]
	if !ok {
		return nil, ErrDecryptionFailed
	}

	return b.decryptLocked(dst, ciphertext, aead, algID, contextAAD)
}

// decryptLocked performs AEAD decryption with the algorithm-agile wire format.
// Caller must hold at least b.mu.RLock().
//
// The AAD is derived from the epoch encoded in the ciphertext header and, when
// supplied, the caller's contextAAD, both of which must match the values used at
// encryption for authentication to succeed. A nil/empty contextAAD reproduces the
// epoch-only AAD, so epoch-only ciphertext decrypts unchanged.
func (b *Barrier) decryptLocked(dst, ciphertext []byte, aead cipher.AEAD, alg SymmetricAlgorithm, contextAAD []byte) ([]byte, error) {
	nonceSize := alg.NonceSize()
	// alg comes from the UNAUTHENTICATED algID header byte and never selects the
	// cipher (the active AEAD is fixed by the authenticated epoch DEK). A tampered
	// or corrupted algID that names a different nonce-size class (e.g. 24-byte
	// XChaCha20 against a 12-byte AES-GCM cipher) would slice a wrong-length nonce
	// and PANIC inside aead.Open — both crypto/cipher GCM and chacha20poly1305
	// panic on a wrong nonce length. The pre-Open length gate below uses this same
	// nonceSize, so it cannot catch the mismatch. Reject any algID whose nonce
	// length disagrees with the active cipher, failing closed with the generic
	// error rather than crashing the process on corrupt WAL/snapshot input.
	if nonceSize == 0 || nonceSize != aead.NonceSize() {
		return nil, ErrDecryptionFailed
	}

	// Validate that the ciphertext is long enough for the algorithm's nonce
	// plus at least a tag (overhead).
	if len(ciphertext) < minCiphertextHeader+nonceSize+aead.Overhead() {
		return nil, ErrDecryptionFailed
	}

	// Extract the epoch from the ciphertext header for AAD construction.
	headerEpoch := binary.BigEndian.Uint64(ciphertext[:epochSize])

	// Extract nonce and encrypted payload (after epoch + algID prefix).
	nonceStart := epochSize + algIDSize
	nonce := ciphertext[nonceStart : nonceStart+nonceSize]
	encrypted := ciphertext[nonceStart+nonceSize:]

	// Construct AAD binding the ciphertext to its epoch and, when supplied, the
	// caller's positional/artifact context.
	aad := aadForEpoch(headerEpoch, contextAAD)

	plaintext, err := aead.Open(dst[:0], nonce, encrypted, aad)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// Rotate increments the epoch, derives a new DEK, and installs it as the
// current cipher. Old DEKs remain available for reading data encrypted with
// previous epochs.
//
// AEAD safety trackers (InvocationCounter, NonceTracker) are reset for the new
// epoch.
//
// Rotate is not supported in hardware mode (returns ErrHardwareMode).
//
// Rotate must only be called while the barrier is unsealed. It acquires the
// exclusive write lock for the entire duration.
//
// PERSIST-THEN-ACTIVATE: a rotated epoch that is not durably recorded is a
// data-loss bug, not an inconvenience. The write path stamps the new epoch
// into WAL segments and snapshots immediately, while the read path
// (DecryptForEpoch) fails closed on an epoch the restored blob does not name.
// Rotate therefore requires BarrierConfig.StatePersister: it hands the NEW
// sealed state to the persister and only installs the new key once the
// persister reports the state durable. With no persister configured Rotate
// fails closed with ErrNoStatePersister and changes nothing.
//
// Ordering — derive, persist, commit, then fail-safe tracker reset:
//
//  1. Derive the new DEK and AEAD cipher. On a derivation error nothing has
//     changed; Rotate returns 0 (old epoch still active) and zeroes the
//     partial DEK.
//  2. Persist the new sealed state (new Epoch + KnownEpochs). On a persist
//     error nothing is committed: no DEK installed, no knownEpochs mutation,
//     no tracker reset. The old epoch stays fully active and usable, and the
//     error is returned as *StatePersistError.
//  3. Commit the epoch switch. All of these mutations are in-memory and
//     infallible, performed under the already-held b.mu write lock, so the new
//     epoch becomes active atomically with respect to every other locked path.
//  4. Reset the AEAD safety trackers (invocations.Reset, nonceTracker.Clear).
//     These run AFTER the commit and must NOT revert the epoch on failure.
//
// The reason resets run last — and never trigger a rollback — is that the
// trackers are SHARED, in-place state with no rollback API: Reset zeroes the
// shared invocation counter and Clear swaps the shared nonce map. If resets ran
// BEFORE the commit and one succeeded while the next failed, the still-active
// OLD key would inherit a freshly zeroed invocation counter and thus a brand-new
// 2^32 invocation budget, blowing past its AEAD nonce-collision safety limit
// with no way to restore the prior count. Committing first removes that hazard:
// the trackers are only ever reset once the NEW key is the active key.
//
// A tracker reset failure after the commit is FAIL-SAFE, so Rotate logs a
// slog.Warn and still returns (newEpoch, nil):
//   - A non-reset invocation counter only makes the NEW key reach its limit
//     sooner, forcing an earlier (still-safe) re-rotation; it never exceeds the
//     limit.
//   - A non-cleared nonce set only yields conservative false-positive reuse
//     rejections; it never accepts a reused nonce.
//
// Returning an error alongside a committed new epoch is deliberately avoided: a
// caller treating the error as "rotation failed" might retry, driving yet
// another rotation. The degraded-but-safe tracker state is surfaced via the log
// instead.
//
// Concurrency note: Encrypt/encryptLocked (and Decrypt) acquire b.mu.RLock, and
// Rotate holds b.mu.Lock for its full duration. No encryption can therefore
// observe the window between the epoch commit (step 2) and the tracker reset
// (step 3); both happen under the exclusive write lock before any reader runs.
func (b *Barrier) Rotate() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return 0, ErrHardwareMode
	}

	if b.sealed == nil {
		return 0, ErrNilSealedData
	}
	persist := b.config.StatePersister
	if persist == nil {
		return 0, ErrNoStatePersister
	}

	oldEpoch := b.epoch
	newEpoch := b.epoch + 1

	// Step 1: derive the new DEK and cipher. Open the root key enclave
	// temporarily for HKDF derivation.
	rootBuf, err := b.rootKey.Open()
	if err != nil {
		return 0, err
	}
	dek, aead, err := b.deriveEpochCipher(rootBuf.Bytes(), b.salt, newEpoch)
	rootBuf.Destroy()
	if err != nil {
		// Nothing committed yet; the old epoch remains active. deriveEpochCipher
		// already zeroed any partial key material on its own error path, but the
		// returned dek (if any) is wiped here for defense in depth.
		wipeBytes(dek)
		return 0, err
	}

	// Step 2: persist the new sealed state BEFORE any of it becomes active.
	// The candidate is a private deep copy, so a failed persist leaves the
	// live state byte-for-byte unchanged; the persister gets its own copy it
	// may retain. It runs with b.mu held and must not re-enter the barrier.
	candidate := cloneSealedRootKey(b.sealed)
	candidate.Epoch = newEpoch
	candidate.KnownEpochs = sortedEpochsWith(b.knownEpochs, newEpoch)
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		wipeBytes(dek)
		slog.Error("key rotation aborted: sealed state not persisted; previous epoch remains active",
			"from_epoch", oldEpoch, "to_epoch", newEpoch, "error", perr)
		return 0, &StatePersistError{Op: "rotate", Epoch: newEpoch, Err: perr}
	}

	// Step 3: commit the epoch switch. Wrap the DEK in an enclave (wipes dek)
	// then perform the in-memory, infallible state mutations under the held write
	// lock. After this point the NEW key is active and the rotation has
	// succeeded; the trackers below are reset against the new epoch.
	dekPK := enclave.NewProtectedKey(dek)

	b.epoch = newEpoch
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks[newEpoch] = dekPK
	b.ciphers[newEpoch] = aead
	b.knownEpochs[newEpoch] = struct{}{}

	// Adopt the state that was just persisted, so SealedRootKeyData() and the
	// durable blob agree exactly.
	b.sealed = candidate

	// Step 4: reset the AEAD safety trackers for the new epoch. These run AFTER
	// the commit and never revert it. A failure here is fail-safe (see the doc
	// comment): a non-reset counter only forces an earlier re-rotation; a
	// non-cleared nonce set only yields conservative false-positive rejections.
	// Log the degraded-but-safe state and still return (newEpoch, nil) so a
	// caller does not retry into yet another rotation.
	if b.invocations != nil {
		if rerr := b.invocations.Reset(); rerr != nil {
			slog.Warn("key rotated but invocation counter reset failed; new key will re-rotate earlier (fail-safe)",
				"from_epoch", oldEpoch, "to_epoch", newEpoch, "tracker", "invocation", "op", "reset", "error", rerr)
		}
	}
	if b.nonceTracker != nil {
		if cerr := b.nonceTracker.Clear(); cerr != nil {
			slog.Warn("key rotated but nonce tracker clear failed; stale nonces may be conservatively rejected (fail-safe)",
				"from_epoch", oldEpoch, "to_epoch", newEpoch, "tracker", "nonce", "op", "clear", "error", cerr)
		}
	}

	slog.Info("key rotated", "from_epoch", oldEpoch, "to_epoch", newEpoch, "algorithm", b.config.Algorithm.String())
	return newEpoch, nil
}

// PurgeEpochsBefore removes DEKs for epochs strictly less than minEpoch.
// The current epoch is never purged. Epochs with in-flight references
// (registered via RegisterInFlightEpoch) are skipped. Returns the number
// of epochs actually purged.
//
// A purge shrinks KnownEpochs, which is persisted state: if the reduced set
// were not written durably, a restart would resurrect the purged epochs and
// re-derive the DEKs the purge floor retired. PurgeEpochsBefore therefore
// persists the reduced state through BarrierConfig.StatePersister BEFORE
// dropping any key, and fails closed with ErrNoStatePersister when there is
// something to purge and no persister is configured. A purge that would drop
// nothing needs no persister and returns (0, nil).
//
// Crash safety of the persist-then-drop order: if the process dies after the
// persist, the restored blob simply omits the epochs the purge had already
// decided to retire — exactly the intended outcome.
//
// PurgeEpochsBefore is not supported in hardware mode (returns ErrHardwareMode).
func (b *Barrier) PurgeEpochsBefore(minEpoch uint64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return 0, ErrHardwareMode
	}

	if minEpoch > b.epoch {
		return 0, ErrPurgeCurrentEpoch
	}

	b.inFlightMu.Lock()
	defer b.inFlightMu.Unlock()

	// Select the purgeable epochs first; nothing is mutated until the reduced
	// state is durable.
	var purgeable []uint64
	for epoch := range b.deks {
		if epoch >= minEpoch || epoch == b.epoch {
			continue
		}
		if b.inFlightEpochs[epoch] > 0 {
			continue
		}
		purgeable = append(purgeable, epoch)
	}
	if len(purgeable) == 0 {
		return 0, nil
	}

	if b.sealed == nil {
		return 0, ErrNilSealedData
	}
	persist := b.config.StatePersister
	if persist == nil {
		return 0, ErrNoStatePersister
	}

	retire := make(map[uint64]struct{}, len(purgeable))
	for _, epoch := range purgeable {
		retire[epoch] = struct{}{}
	}
	candidate := cloneSealedRootKey(b.sealed)
	candidate.KnownEpochs = sortedEpochsWithout(b.knownEpochs, retire)
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		slog.Error("epoch purge aborted: reduced sealed state not persisted; DEKs retained",
			"min_epoch", minEpoch, "error", perr)
		return 0, &StatePersistError{Op: "purge", Epoch: b.epoch, Err: perr}
	}

	for _, epoch := range purgeable {
		// Actively zero the purged DEK's enclave rather than letting the
		// GC/finalizer eventually reclaim it: a purged epoch's key must
		// not linger in memory.
		if dek := b.deks[epoch]; dek != nil {
			dek.Clear()
		}
		delete(b.deks, epoch)
		delete(b.ciphers, epoch)
		delete(b.knownEpochs, epoch)
		slog.Info("old DEK purged", "epoch", epoch)
	}
	b.sealed = candidate

	return len(purgeable), nil
}

// sortedEpochsWith returns the epoch set plus add as an ascending slice.
// The order is deterministic so that persisted blobs are reproducible.
func sortedEpochsWith(set map[uint64]struct{}, add uint64) []uint64 {
	epochs := make([]uint64, 0, len(set)+1)
	for ep := range set {
		epochs = append(epochs, ep)
	}
	if _, ok := set[add]; !ok {
		epochs = append(epochs, add)
	}
	slices.Sort(epochs)
	return epochs
}

// sortedEpochsWithout returns the epoch set minus drop as an ascending slice.
// The order is deterministic so that persisted blobs are reproducible.
func sortedEpochsWithout(set map[uint64]struct{}, drop map[uint64]struct{}) []uint64 {
	epochs := make([]uint64, 0, len(set))
	for ep := range set {
		if _, dropped := drop[ep]; dropped {
			continue
		}
		epochs = append(epochs, ep)
	}
	slices.Sort(epochs)
	return epochs
}

// RegisterInFlightEpoch increments the in-flight reference count for an epoch.
// This prevents PurgeEpochsBefore from removing the epoch's DEK while it is
// still in use (e.g., during snapshot receive).
func (b *Barrier) RegisterInFlightEpoch(epoch uint64) {
	b.inFlightMu.Lock()
	b.inFlightEpochs[epoch]++
	b.inFlightMu.Unlock()
}

// DeregisterInFlightEpoch decrements the in-flight reference count for an epoch.
// If the count reaches zero, the entry is removed from the map.
func (b *Barrier) DeregisterInFlightEpoch(epoch uint64) {
	b.inFlightMu.Lock()
	b.inFlightEpochs[epoch]--
	if b.inFlightEpochs[epoch] <= 0 {
		delete(b.inFlightEpochs, epoch)
	}
	b.inFlightMu.Unlock()
}

// IsSealed returns true if the barrier is in the sealed state.
func (b *Barrier) IsSealed() bool {
	return !b.unsealed.Load()
}

// IsInitialized returns true if the barrier has been initialized (is not in
// the uninitialized state). A sealed barrier is still considered initialized.
func (b *Barrier) IsInitialized() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state != barrierUninitialized
}

// IsHardwareMode returns true if the barrier is using a delegated encryptor for
// all encrypt/decrypt operations.
//
// It is deliberately COARSE and is kept as-is for compatibility: it is also true
// for no-encryption mode (StrategyNone installs an identity pass-through
// encryptor) and it says nothing about whether that encryptor can authenticate a
// caller-supplied context. Use SupportsContextAAD to decide whether to pass a
// positional context, IsPlaintextMode to tell no-encryption apart from real
// delegation, and ContextBinding for the full classification.
func (b *Barrier) IsHardwareMode() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.hwEncryptor != nil
}

// RootKey returns a copy of the decrypted root key. The barrier must be
// unsealed. The returned slice is owned by the caller and should be zeroed
// after use.
func (b *Barrier) RootKey() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != barrierUnsealed {
		return nil, ErrBarrierSealed
	}
	// No-encryption mode (StrategyNone) holds no root key material, so
	// b.rootKey is nil. Return a defined error instead of nil-dereferencing.
	if b.rootKey == nil {
		return nil, ErrNoRootKeyAvailable
	}
	buf, err := b.rootKey.Open()
	if err != nil {
		return nil, err
	}
	keyCopy := make([]byte, buf.Size())
	copy(keyCopy, buf.Bytes())
	buf.Destroy()
	return keyCopy, nil
}

// CurrentEpoch returns the current key rotation epoch. Returns 0 if the
// barrier has not been initialized.
func (b *Barrier) CurrentEpoch() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.epoch
}

// SealedRootKeyData returns a copy of the sealed root key for persistence.
// Returns nil if the barrier has not been initialized.
func (b *Barrier) SealedRootKeyData() *SealedRootKey {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return cloneSealedRootKey(b.sealed)
}

// cloneSealedRootKey returns a deep copy of a sealed root key, so that a
// caller (or a StatePersister) can retain and mutate it without aliasing the
// barrier's own state. Returns nil for a nil input.
func cloneSealedRootKey(src *SealedRootKey) *SealedRootKey {
	if src == nil {
		return nil
	}
	cp := &SealedRootKey{
		Strategy:           src.Strategy,
		Version:            src.Version,
		Epoch:              src.Epoch,
		FailedAttempts:     src.FailedAttempts,
		LastFailedUnixNano: src.LastFailedUnixNano,
		KDFVersion:         src.KDFVersion,
		HKDFVersion:        src.HKDFVersion,
		ShamirThreshold:    src.ShamirThreshold,
		ShamirTotal:        src.ShamirTotal,
		CreatedAt:          src.CreatedAt,
	}
	cp.Salt = make([]byte, len(src.Salt))
	copy(cp.Salt, src.Salt)
	cp.Ciphertext = make([]byte, len(src.Ciphertext))
	copy(cp.Ciphertext, src.Ciphertext)
	if src.Metadata != nil {
		cp.Metadata = make([]byte, len(src.Metadata))
		copy(cp.Metadata, src.Metadata)
	}
	if len(src.KnownEpochs) > 0 {
		cp.KnownEpochs = make([]uint64, len(src.KnownEpochs))
		copy(cp.KnownEpochs, src.KnownEpochs)
	}
	if src.Nonce != nil {
		cp.Nonce = make([]byte, len(src.Nonce))
		copy(cp.Nonce, src.Nonce)
	}
	if src.HardwarePayload != nil {
		cp.HardwarePayload = make([]byte, len(src.HardwarePayload))
		copy(cp.HardwarePayload, src.HardwarePayload)
	}
	return cp
}

// SetSealedRootKey sets the sealed root key data for subsequent Unseal calls.
// This is used when loading persisted sealed key data during node startup.
//
// Validation ensures that the sealed data is structurally sound:
//   - Epoch must be greater than zero.
//   - Salt must have the expected HKDF salt length (32 bytes).
//   - KnownEpochs must contain the current Epoch.
//
// Brute-force backoff state (FailedAttempts, LastFailedUnixNano) is restored
// from the persisted sealed key, preventing attackers from resetting the
// backoff counter by restarting the process.
func (b *Barrier) SetSealedRootKey(sealed *SealedRootKey) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Reject nil before any field access so a missing/corrupt persisted blob
	// surfaces as a sentinel error instead of a nil-pointer panic.
	if sealed == nil {
		return ErrNilSealedData
	}

	// Validate epoch is non-zero.
	if sealed.Epoch == 0 {
		return ErrInvalidSealedEpoch
	}

	// Validate salt length.
	if len(sealed.Salt) != hkdfSaltSize {
		return ErrInvalidSealedSalt
	}

	// Validate that KnownEpochs contains the current epoch.
	epochFound := false
	for _, ep := range sealed.KnownEpochs {
		if ep == sealed.Epoch {
			epochFound = true
			break
		}
	}
	if !epochFound {
		return ErrInvalidSealedEpochs
	}

	// Reject a DEK-derivation layout this build does not implement rather
	// than failing later with an opaque decryption error.
	if sealed.HKDFVersion > currentHKDFVersion {
		return ErrUnsupportedHKDFVersion
	}

	b.sealed = sealed
	b.salt = sealed.Salt
	b.epoch = sealed.Epoch

	// A blob persisted before the version field existed carries 0 and must
	// keep deriving with the v1 info layout.
	if sealed.HKDFVersion == 0 {
		b.hkdfVersion = HKDFV1
	} else {
		b.hkdfVersion = sealed.HKDFVersion
	}

	// Restore known epochs from persisted data so Unseal can re-derive DEKs for
	// all historical epochs. Allocate a FRESH set rather than merging into the
	// existing one: the loaded blob is authoritative. Merging would let a second
	// SetSealedRootKey after a PurgeEpochsBefore had reduced and persisted the set
	// resurrect the purged epochs (which a later Rotate would then re-persist,
	// making the resurrection durable). A fresh map makes the blob's KnownEpochs
	// exactly the set Unseal re-derives.
	b.knownEpochs = make(map[uint64]struct{}, len(sealed.KnownEpochs))
	for _, ep := range sealed.KnownEpochs {
		b.knownEpochs[ep] = struct{}{}
	}

	// Restore brute-force backoff state from persistence. Zero values
	// (from v1 sealed keys without these fields) result in no backoff,
	// preserving backward compatibility.
	b.failedAttempts = sealed.FailedAttempts
	if sealed.LastFailedUnixNano > 0 {
		b.lastFailedAt = time.Unix(0, sealed.LastFailedUnixNano)
	}

	if b.state == barrierUninitialized {
		b.state = barrierSealed
	}
	return nil
}

// persistBackoffState writes the current brute-force backoff counters to the
// in-memory sealed key so that SealedRootKeyData() returns them for persistence.
// Caller must hold b.mu (write lock).
func (b *Barrier) persistBackoffState() {
	if b.sealed == nil {
		return
	}
	b.sealed.FailedAttempts = b.failedAttempts
	if b.lastFailedAt.IsZero() {
		b.sealed.LastFailedUnixNano = 0
	} else {
		b.sealed.LastFailedUnixNano = b.lastFailedAt.UnixNano()
	}
}

// backoffDelay computes the exponential backoff delay based on the number
// of consecutive failed attempts. Delay starts at bruteForceBaseDelay after
// bruteForceThreshold failures and doubles per attempt, capped at bruteForceMaxDelay.
func (b *Barrier) backoffDelay() time.Duration {
	excess := b.failedAttempts - bruteForceThreshold
	if excess < 0 {
		return 0
	}
	delay := bruteForceBaseDelay
	for i := int64(0); i < excess; i++ {
		delay *= 2
		if delay > bruteForceMaxDelay {
			return bruteForceMaxDelay
		}
	}
	return delay
}

// deriveDEK derives a data encryption key from the root key using HKDF-SHA256.
// Each (deploymentID, epoch) pair produces a cryptographically independent DEK
// through a unique info string. The deploymentID is bound into the HKDF info to
// ensure that two clusters sharing the same root key derive completely different
// DEKs, preventing cross-cluster ciphertext portability.
//
// The output length is the algorithm's key size (e.g. 16 for AES-128-GCM,
// 32 for AES-256-GCM).
//
// Info layout by version:
//
//	v1: [prefix:20][deploymentID:8][epoch:8]            = 36 bytes
//	v2: [prefix:20][deploymentID:8][epoch:8][alg:1][len:1] = 38 bytes
//
// v2 adds algorithm/key-length domain separation; see HKDFV2. The version
// comes from the persisted SealedRootKey so existing data keeps deriving
// under the layout it was written with.
func deriveDEK(rootKey, salt []byte, deploymentID, epoch uint64, alg SymmetricAlgorithm, version HKDFVersion) ([]byte, error) {
	keySize := alg.KeySize()
	if keySize == 0 {
		return nil, &KeyDerivationError{Epoch: epoch, Err: ErrUnsupportedAlgorithm}
	}

	var info [hkdfInfoLenV2]byte
	copy(info[:20], hkdfInfoPrefix)
	binary.BigEndian.PutUint64(info[20:], deploymentID)
	binary.BigEndian.PutUint64(info[28:], epoch)

	infoLen := hkdfInfoLenV1
	switch version {
	case 0, HKDFV1:
		// Legacy layout; nothing to append.
	case HKDFV2:
		info[36] = byte(alg)
		info[37] = byte(keySize)
		infoLen = hkdfInfoLenV2
	default:
		return nil, &KeyDerivationError{Epoch: epoch, Err: ErrUnsupportedHKDFVersion}
	}

	hkdfReader := hkdf.New(sha256.New, rootKey, salt, info[:infoLen])
	dek := make([]byte, keySize)
	if _, err := io.ReadFull(hkdfReader, dek); err != nil {
		return nil, &KeyDerivationError{Epoch: epoch, Err: err}
	}
	runtime.KeepAlive(rootKey)
	return dek, nil
}

// deriveEpochCipher derives a DEK for the given epoch using the barrier's
// configured algorithm and creates the corresponding AEAD cipher. On error,
// any partially-derived key material is zeroed. This method reduces
// duplication across Initialize, Unseal, and Rotate.
func (b *Barrier) deriveEpochCipher(rootKey, salt []byte, epoch uint64) ([]byte, cipher.AEAD, error) {
	alg := b.config.Algorithm
	dek, err := deriveDEK(rootKey, salt, b.config.DeploymentID, epoch, alg, b.hkdfVersion)
	if err != nil {
		return nil, nil, err
	}
	aead, err := b.aeadProvider.NewAEAD(alg, dek)
	if err != nil {
		wipeBytes(dek)
		return nil, nil, err
	}
	return dek, aead, nil
}
