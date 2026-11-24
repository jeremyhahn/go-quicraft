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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-enclave"
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

	// minCiphertextHeader is the fixed header before the nonce and ciphertext+tag.
	minCiphertextHeader = epochSize + algIDSize

	// hkdfInfoPrefix is the fixed prefix for HKDF info strings.
	hkdfInfoPrefix = "quicraft/barrier/v1/"

	// hkdfInfoLen is the length of the HKDF info: 20 bytes prefix + 8 bytes
	// deploymentID + 8 bytes epoch + the algorithm identifier and the DEK
	// length.
	hkdfInfoLen = 38

	// bruteForceThreshold is the number of consecutive failed Unseal
	// attempts before exponential backoff activates.
	bruteForceThreshold = 5

	// bruteForceBaseDelay is the initial backoff delay after exceeding the threshold.
	bruteForceBaseDelay = 1 * time.Second

	// bruteForceMaxDelay is the maximum backoff delay.
	bruteForceMaxDelay = 60 * time.Second

	// bruteForceErrorThreshold is the number of failures that triggers ERROR-level logging.
	bruteForceErrorThreshold = 20

	// MaxKnownEpochs bounds SealedRootKey.KnownEpochs at load, capping the derivation and mlock work an altered blob can force.
	MaxKnownEpochs = 4096

	// firstLoadGeneration is the LoadGeneration Initialize stamps, 1 so it differs from an unclaimed blob's 0.
	firstLoadGeneration uint64 = 1
)

// HKDFVersion identifies the HKDF info layout for epoch DEKs; any value but currentHKDFVersion is ErrUnsupportedHKDFVersion.
type HKDFVersion uint8

const (
	// HKDFV2 is [prefix:20][deploymentID:8][epoch:8][algID:1][keyLen:1], separating keys by algorithm and key length.
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

// Barrier provides authenticated at-rest encryption with one AEAD per deployment, epoch DEKs via HKDF-SHA256, and wire format [epoch:8][algID:1][nonce:N][ciphertext+tag].
type Barrier struct {
	mu       sync.RWMutex
	unsealed atomic.Bool // fast-path check, avoids RWMutex contention on hot path

	// unsealWait is the channel UnsealedC returns while sealed, closed on unseal; guarded by notifyMu.
	notifyMu   sync.Mutex
	unsealWait chan struct{}

	config BarrierConfig // algorithm selection, deployment identity, tracking config

	state   barrierState
	rootKey enclave.Sealed // 32 bytes, held as ciphertext at rest; opened into locked memory
	salt    []byte         // 32 bytes, generated at Initialize, used for HKDF

	// hkdfVersion is the DEK-derivation info layout in force. Set to
	// currentHKDFVersion for a fresh barrier and overwritten from the
	// persisted SealedRootKey by SetSealedRootKey.
	hkdfVersion HKDFVersion

	epoch      uint64                    // current key rotation epoch
	currentDEK enclave.Sealed            // derived via HKDF from rootKey + epoch, encrypted at rest
	cipher     cipher.AEAD               // current epoch's AEAD cipher (cached, opaque)
	deks       map[uint64]enclave.Sealed // epoch -> DEK encrypted at rest for re-derivation
	ciphers    map[uint64]cipher.AEAD

	// knownEpochs tracks all epochs that have been used (via Initialize or
	// Rotate). Unlike deks/ciphers, this set is NOT cleared on seal(), so
	// Unseal can re-derive DEKs for all historical epochs.
	knownEpochs map[uint64]struct{}

	// sealed is the persisted sealed root key, held in memory so that
	// the barrier can be re-sealed without requiring the strategy.
	sealed *SealedRootKey

	// hwEncryptor is the strategy's own encryptor; when set, every encrypt/decrypt delegates to it, bypassing epoch DEKs, nonce tracking and invocation counting.
	hwEncryptor SymmetricEncrypter

	// plaintextMode records that hwEncryptor is StrategyNone's identity pass-through.
	plaintextMode bool

	// binding is the installed encryptor's context-AAD capability, resolved at Initialize/Unseal and reset by seal().
	binding ContextBinding

	// strategyID and strategyHardwareBacked record the identity of the strategy
	// that produced the current encryptor, for the delegated-mode warning and
	// for operator-facing diagnostics. Set at Initialize/Unseal.
	strategyID             StrategyID
	strategyHardwareBacked bool

	// aeadProvider creates cipher.AEAD instances for each epoch's DEK. Defaults
	// to SoftwareAEADProvider; may be replaced with a hardware-backed provider
	// via BarrierConfig.AEADProvider.
	aeadProvider AEADProvider

	// maxPlaintext is the algorithm's single-invocation plaintext ceiling; see SymmetricAlgorithm.MaxPlaintextSize.
	maxPlaintext uint64

	// AEAD safety trackers (interface types for IoC).
	invocations  InvocationCounter    // per-epoch AEAD invocation counter
	nonceTracker NonceTrackerProvider // per-epoch nonce reuse detection

	// invocationsAuthoritative is true when b.invocations is the true count for b.epoch, i.e. this process started the epoch.
	invocationsAuthoritative bool

	// firstBoot records that the root key came from this process's Initialize and never left epoch 1; only then may Reset discard the epoch.
	firstBoot bool

	// loadGeneration is the LoadGeneration this barrier claimed; guarded by b.mu.
	loadGeneration uint64

	// superseded records that another loader claimed this sealed state; never cleared. See ErrBarrierSuperseded.
	superseded atomic.Bool

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

	// lockRootKey allocates the locked memory holding the root key; tests replace it, otherwise enclave.NewMemorySized.
	lockRootKey func(int) (enclave.ByteAccess, error)
}

// releaseEnclave destroys a locked-memory enclave and logs a refused release; a refused release of a frozen region may leave its pages unwiped until WipeAll.
func releaseEnclave(e enclave.ByteAccess, what string) {
	if e == nil {
		return
	}
	if err := e.Destroy(); err != nil {
		slog.Warn("quicraft/seal: locked memory release was refused or ran without a wipe",
			"enclave", what,
			"error", err,
		)
	}
}

// freezeEnclave makes e read-only through its Freezer capability. A handle
// without the capability is refused with a *enclave.CapabilityError rather
// than handed back writable.
func freezeEnclave(e enclave.ByteAccess) error {
	var f enclave.Freezer
	if !enclave.As(e, &f) {
		return &enclave.CapabilityError{Backend: e.Backend().Name(), Capability: enclave.CapabilityFreeze}
	}
	return f.Freeze()
}

// releaseSealed destroys an enclave.Sealed on a cleanup path, logging any failure.
func releaseSealed(s enclave.Sealed, what string) {
	if s == nil {
		return
	}
	if err := s.Destroy(); err != nil {
		slog.Warn("quicraft/seal: sealed key release failed",
			"enclave", what,
			"error", err,
		)
	}
}

// EpochBarrier is an alias for Barrier. Use this name in new code to
// distinguish from go-qrdb's StorageBarrier.
type EpochBarrier = Barrier

// NewBarrier creates an uninitialized Barrier, binding DeploymentID into DEK derivation; an invalid config returns the Validate error.
func NewBarrier(config BarrierConfig) (*Barrier, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	var ic InvocationCounter
	if config.InvocationCounter != nil {
		ic = config.InvocationCounter
	} else {
		ic = NewInvocationCounter(true, config.InvocationLimit, config.RotationThreshold)
	}

	// Without a persister or an injected counter the tracker can never be cleared; reported, and bounded by DefaultMaxTrackedNonces.
	if config.GetNonceTracking() && config.InvocationCounter == nil && config.StatePersister == nil {
		slog.Warn("quicraft/seal: barrier configured with nonce tracking but no StatePersister and no InvocationCounter; the epoch cannot rotate, so tracked nonces are never released",
			"deployment_id", config.DeploymentID,
			"remedy", "configure StatePersister (and a rotation loop), inject an InvocationCounter, or disable NonceTracking",
		)
	}

	var nt NonceTrackerProvider
	if config.NonceTrackerProvider != nil {
		nt = config.NonceTrackerProvider
	} else if config.GetNonceTracking() {
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
		deks:           make(map[uint64]enclave.Sealed),
		ciphers:        make(map[uint64]cipher.AEAD),
		knownEpochs:    make(map[uint64]struct{}),
		inFlightEpochs: make(map[uint64]int64),
		invocations:    ic,
		nonceTracker:   nt,
		aeadProvider:   ap,
		maxPlaintext:   config.Algorithm.MaxPlaintextSize(),
		timeNow:        time.Now,
		randReader:     rand.Reader,
		lockRootKey:    enclave.NewMemorySized,
	}, nil
}

// MustNewBarrier is NewBarrier for a compile-time config, panicking if it is invalid; never use it on a runtime config.
func MustNewBarrier(config BarrierConfig) *Barrier {
	b, err := NewBarrier(config)
	if err != nil {
		panic("seal.MustNewBarrier: " + err.Error())
	}
	return b
}

// NewEpochBarrier calls NewBarrier; it is a function, not a variable, so importers cannot replace it.
func NewEpochBarrier(config BarrierConfig) (*Barrier, error) { return NewBarrier(config) }

// MustNewEpochBarrier calls MustNewBarrier, completing the EpochBarrier alias
// set. It panics on an invalid config; see MustNewBarrier for when that is the
// right form and when it is not.
func MustNewEpochBarrier(config BarrierConfig) *Barrier { return MustNewBarrier(config) }

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

// Initialize generates the root key, derives the first DEK, seals the root key with strategy and unseals the barrier; a second call is ErrBarrierAlreadyInit unless Reset ran.
func (b *Barrier) Initialize(ctx context.Context, strategy SealingStrategy, creds Credentials) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUninitialized {
		return ErrBarrierAlreadyInit
	}

	// The root key is generated in place into a writable locked enclave, never the heap, and destroyed by the deferred Destroy.
	rootEnc, err := b.lockRootKey(rootKeySize)
	if err != nil {
		return &Error{Strategy: "initialize", Err: err}
	}
	if rootEnc == nil {
		return &Error{Strategy: "initialize", Err: enclave.ErrEnclaveDestroyed}
	}
	defer releaseEnclave(rootEnc, "root key")

	// Everything touching the root key runs inside the borrow.
	return rootEnc.WithBytes(func(rootKey []byte) error {
		if _, err := io.ReadFull(b.randReader, rootKey); err != nil {
			return &Error{Strategy: "initialize", Err: err}
		}
		return b.initializeWithRootKey(ctx, strategy, creds, rootKey)
	})
}

// initializeWithRootKey is Initialize's body under b.mu; rootKey is valid only during the caller's borrow and must not be retained.
func (b *Barrier) initializeWithRootKey(ctx context.Context, strategy SealingStrategy, creds Credentials, rootKey []byte) error {
	// Generate HKDF salt. Not secret — it is stored in the sealed blob — so it
	// stays an ordinary allocation.
	salt := make([]byte, hkdfSaltSize)
	if _, err := io.ReadFull(b.randReader, salt); err != nil {
		return &Error{Strategy: "initialize", Err: err}
	}

	const initialEpoch uint64 = 1

	// Seal the root key for persistent storage.
	sealed, err := strategy.SealRootKey(ctx, rootKey, creds)
	if err != nil {
		return err
	}
	sealed.Epoch = initialEpoch
	sealed.Salt = salt
	sealed.KnownEpochs = []uint64{initialEpoch}
	// The process that creates the blob holds its first claim. Stamped here,
	// before the MAC below covers it, so the very first persisted state already
	// carries a generation and the next loader has something to increment.
	sealed.LoadGeneration = firstLoadGeneration
	b.loadGeneration = firstLoadGeneration
	// Record the DEK-derivation layout so a future version can still derive
	// this deployment's keys after the layout changes again.
	sealed.HKDFVersion = b.hkdfVersion
	// Record the data-path AEAD; see SealedRootKey.Algorithm.
	sealed.Algorithm = b.config.Algorithm
	// MAC the metadata so Epoch and KnownEpochs cannot be forged; StrategyNone has none.
	if strategy.ID() != StrategyNone {
		mac, macErr := sealedStateMAC(rootKey, b.config.DeploymentID, sealed)
		if macErr != nil {
			return macErr
		}
		sealed.StateMAC = mac
	}

	// Check if strategy provides a hardware encryptor.
	hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
	if hwErr != nil {
		return hwErr
	}

	// Resolve the context binding before any state transition.
	binding := contextBindingFor(hwEnc, strategy.ID() == StrategyNone, b.randReader)
	if err := b.checkContextAADPolicy(binding); err != nil {
		return err
	}
	b.binding = binding
	b.strategyID = strategy.ID()
	b.strategyHardwareBacked = strategy.HardwareBacked()
	// Plaintext mode follows the binding, not the strategy ID.
	b.plaintextMode = binding == ContextBindingPlaintext

	if hwEnc != nil {
		// Hardware mode: seal before any state change so a refusal leaves the barrier uninitialized; no-encryption mode wipes the key and keeps none.
		if strategy.ID() == StrategyNone {
			enclave.Wipe(rootKey)
			b.hwEncryptor = hwEnc
			b.rootKey = nil
		} else {
			rootPK, sealErr := enclave.NewSealed(ctx, rootKey)
			if sealErr != nil {
				return &Error{Strategy: "initialize", Err: sealErr}
			}
			b.hwEncryptor = hwEnc
			b.rootKey = rootPK
		}
		b.salt = salt
		b.epoch = initialEpoch
		b.knownEpochs[initialEpoch] = struct{}{}
		b.sealed = sealed
		b.state = barrierUnsealed
		b.setUnsealed(true)
		b.firstBoot = true
		b.logDelegatedMode("initialize", binding)
		slog.Info("barrier initialized (hardware mode)", "epoch", initialEpoch, "context_binding", binding.String())
		return nil
	}

	// Derive initial DEK and cipher for epoch 1.
	dek, aead, err := b.deriveEpochCipher(rootKey, salt, initialEpoch)
	if err != nil {
		return err
	}

	// Seal both keys before any state change, the root key last; enclave.NewSealed consumes its argument on every path.
	dekPK, err := enclave.NewSealed(ctx, dek)
	if err != nil {
		return &Error{Strategy: "initialize", Err: err}
	}
	rootPK, err := enclave.NewSealed(ctx, rootKey)
	if err != nil {
		releaseSealed(dekPK, "initial DEK")
		return &Error{Strategy: "initialize", Err: err}
	}

	// Transition to unsealed state.
	b.rootKey = rootPK
	b.salt = salt
	b.epoch = initialEpoch
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks[initialEpoch] = dekPK
	b.ciphers[initialEpoch] = aead
	b.knownEpochs[initialEpoch] = struct{}{}
	b.sealed = sealed
	b.state = barrierUnsealed
	b.setUnsealed(true)
	// This process created the epoch, so its counter starts at a known zero and
	// stays authoritative until a blob describing some other lifetime is loaded.
	b.invocationsAuthoritative = true
	// ...and it is a first boot Reset may still roll back, until the epoch
	// advances or another blob is loaded over it.
	b.firstBoot = true

	slog.Info("barrier initialized", "epoch", initialEpoch, "algorithm", b.config.Algorithm.String())
	return nil
}

// unsealPreconditionLocked reports why the barrier may not unseal, or nil; a superseded barrier may never re-claim. The caller must hold b.mu.
func (b *Barrier) unsealPreconditionLocked() error {
	switch {
	case b.state == barrierUninitialized:
		return ErrBarrierNotInit
	case b.state == barrierUnsealed:
		return ErrBarrierUnsealed
	case b.superseded.Load():
		return ErrBarrierSuperseded
	}
	return nil
}

// abandonedWithoutVerdict reports whether err is only ctx ending, carrying no authentication verdict, so it does not count as a failed attempt.
func abandonedWithoutVerdict(ctx context.Context, err error) bool {
	ended := ctx.Err()
	if ended == nil || !errors.Is(err, ended) {
		return false
	}
	return !errors.Is(err, enclave.ErrSealedAuthentication) && !errors.Is(err, ErrDecryptionFailed)
}

// Unseal decrypts the root key with strategy, authenticates the persisted metadata, claims the blob and derives the epoch DEKs; repeated failures back off exponentially.
func (b *Barrier) Unseal(ctx context.Context, strategy SealingStrategy, creds Credentials) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.unsealPreconditionLocked(); err != nil {
		return err
	}

	// A caller that has already stopped waiting is refused before the backoff
	// check and before the strategy: nothing is attempted, so nothing is
	// counted and nothing is written — not even the clock re-anchor below.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Brute-force backoff check.
	if b.failedAttempts >= bruteForceThreshold {
		elapsed := b.timeNow().Sub(b.lastFailedAt)
		// A persisted lastFailedAt in the future satisfies the delay and is re-anchored to now.
		if elapsed < 0 {
			b.lastFailedAt = b.timeNow()
			b.persistBackoffState()
		} else if elapsed < b.backoffDelay() {
			return ErrUnsealBackoff
		}
	}

	// Attempt to unseal the root key.
	heapKey, err := strategy.UnsealRootKey(ctx, b.sealed, creds)
	// Wipe the strategy's heap copy of the root key on every return, including error returns; the eager wipes below stay.
	defer enclave.Wipe(heapKey)
	if err != nil && abandonedWithoutVerdict(ctx, err) {
		// A strategy that failed only because ctx ended does not count as a failed attempt.
		return err
	}
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

	// No-encryption mode, gated on the constructed strategy, never on the unauthenticated b.sealed.Strategy: install the identity encryptor before the key-size check.
	if strategy.ID() == StrategyNone {
		hwEnc, hwErr := strategy.BarrierEncryptor(ctx, heapKey)
		if hwErr != nil {
			return hwErr
		}
		// Wipe now, before the persister and log calls below.
		enclave.Wipe(heapKey)
		// A nil encryptor here would leave state=unsealed with cipher==nil
		// and hwEncryptor==nil, and the first Encrypt would nil-dereference.
		if hwEnc == nil {
			return ErrNoBarrierEncryptor
		}
		// No-encryption mode can never satisfy a demand for authenticated
		// positional binding: there is no AEAD to bind anything to.
		binding := contextBindingFor(hwEnc, true, b.randReader)
		if err := b.checkContextAADPolicy(binding); err != nil {
			return err
		}
		b.binding = binding
		b.strategyID = strategy.ID()
		b.strategyHardwareBacked = strategy.HardwareBacked()
		b.plaintextMode = true
		b.hwEncryptor = hwEnc
		// Set the epoch as Initialize does, so CurrentEpoch agrees.
		if b.sealed != nil {
			b.epoch = b.sealed.Epoch
		}
		b.state = barrierUnsealed
		b.setUnsealed(true)
		b.failedAttempts = 0
		b.lastFailedAt = time.Time{}
		b.persistBackoffState()
		b.logDelegatedMode("unseal", binding)
		slog.Info("barrier unsealed (no-encryption mode)", "context_binding", binding.String())
		return nil
	}

	if len(heapKey) != rootKeySize {
		// Eager for the same reason: persistBackoffState reaches the embedder's
		// StatePersister, and a wrong-sized key is still key material.
		enclave.Wipe(heapKey)
		b.failedAttempts++
		b.lastFailedAt = b.timeNow()
		b.persistBackoffState()
		return ErrInvalidRootKeySize
	}

	// Copy the strategy's heap root key into a writable locked enclave and wipe the heap copy at once.
	rootEnc, encErr := b.lockRootKey(rootKeySize)
	if encErr != nil {
		return &Error{Strategy: "unseal", Err: encErr}
	}
	// A nil enclave with a nil error is a refusal too: go-enclave's handles are
	// interfaces, and a nil one cannot be borrowed from.
	if rootEnc == nil {
		return &Error{Strategy: "unseal", Err: enclave.ErrEnclaveDestroyed}
	}
	defer releaseEnclave(rootEnc, "root key")

	return rootEnc.WithBytes(func(rootKey []byte) error {
		copy(rootKey, heapKey)
		enclave.Wipe(heapKey)
		return b.unsealWithRootKey(ctx, strategy, rootKey)
	})
}

// unsealWithRootKey is the rest of Unseal under b.mu; rootKey is valid only during the caller's borrow and must not be retained.
func (b *Barrier) unsealWithRootKey(ctx context.Context, strategy SealingStrategy, rootKey []byte) error {
	// Authenticate the persisted metadata before deriving any DEK; a failure here is not a failed unseal attempt.
	if err := b.verifySealedStateMAC(rootKey); err != nil {
		return err
	}

	// Check if strategy provides a hardware encryptor.
	hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
	if hwErr != nil {
		return hwErr
	}

	// Resolve the context binding before any state transition.
	binding := contextBindingFor(hwEnc, false, b.randReader)
	if err := b.checkContextAADPolicy(binding); err != nil {
		return err
	}
	b.binding = binding
	b.strategyID = strategy.ID()
	b.strategyHardwareBacked = strategy.HardwareBacked()
	b.plaintextMode = false

	if hwEnc != nil {
		// Hardware mode: seal before any state change, so a refusal leaves the barrier sealed.
		rootPK, sealErr := enclave.NewSealed(ctx, rootKey)
		if sealErr != nil {
			return &Error{Strategy: "unseal", Err: sealErr}
		}
		b.hwEncryptor = hwEnc
		b.rootKey = rootPK
		b.state = barrierUnsealed
		b.setUnsealed(true)
		b.failedAttempts = 0
		b.lastFailedAt = time.Time{}
		b.persistBackoffState()
		b.logDelegatedMode("unseal", binding)
		slog.Info("barrier unsealed (hardware mode)", "epoch", b.epoch, "context_binding", binding.String())
		return nil
	}

	// Claim the blob durably before this barrier can encrypt; a failed claim aborts with the barrier sealed.
	if err := b.claimLoadGenerationLocked(rootKey); err != nil {
		return err
	}

	// Derive DEK and cipher for the current epoch.
	dek, aead, err := b.deriveEpochCipher(rootKey, b.salt, b.epoch)
	if err != nil {
		return err
	}

	// Re-derive DEKs for every known epoch but the current one; failed() destroys what was built, and dek is wiped by hand.
	newDEKs := make(map[uint64]enclave.Sealed)
	newCiphers := make(map[uint64]cipher.AEAD)
	failed := func(err error) error {
		for _, pk := range newDEKs {
			releaseSealed(pk, "unsealed epoch DEK")
		}
		return err
	}
	for epoch := range b.knownEpochs {
		if epoch == b.epoch {
			continue
		}
		d, a, derr := b.deriveEpochCipher(rootKey, b.salt, epoch)
		if derr != nil {
			enclave.Wipe(dek)
			return failed(derr)
		}
		// Consumes d on every path, so there is no plain DEK left to wipe by
		// hand — which is what the plainDEKs slice this replaced was for.
		pk, serr := enclave.NewSealed(ctx, d)
		if serr != nil {
			enclave.Wipe(dek)
			return failed(&Error{Strategy: "unseal", Err: serr})
		}
		newDEKs[epoch] = pk
		newCiphers[epoch] = a
	}

	// Seal the current DEK and then the root key before any state change.
	dekPK, err := enclave.NewSealed(ctx, dek)
	if err != nil {
		return failed(&Error{Strategy: "unseal", Err: err})
	}

	// Install the current epoch (skipped by the loop above; this also
	// covers the case where b.epoch is not in knownEpochs).
	newDEKs[b.epoch] = dekPK
	newCiphers[b.epoch] = aead

	rootPK, err := enclave.NewSealed(ctx, rootKey)
	if err != nil {
		return failed(&Error{Strategy: "unseal", Err: err})
	}

	// Transition to unsealed state.
	b.rootKey = rootPK
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks = newDEKs
	b.ciphers = newCiphers
	b.state = barrierUnsealed
	b.setUnsealed(true)
	b.failedAttempts = 0
	b.lastFailedAt = time.Time{}
	b.persistBackoffState()

	// Enforce the invocation budget across restarts; see resolveInvocationBudget.
	b.resolveInvocationBudget(ctx)

	slog.Info("barrier unsealed", "epoch", b.epoch, "algorithm", b.config.Algorithm.String())
	return nil
}

// claimLoadGenerationLocked writes back LoadGeneration + 1 so a later loader can detect this one; it is checked at load, not a lock, and inert without a persister. The caller must hold b.mu.
func (b *Barrier) claimLoadGenerationLocked(rootKey []byte) error {
	if b.sealed == nil {
		return ErrNilSealedData
	}
	persist := b.config.StatePersister
	if persist == nil {
		slog.Warn("quicraft/seal: no StatePersister, so the load-generation fence is inert; "+
			"a second process loading this same sealed blob cannot be detected",
			"epoch", b.epoch,
			"load_generation", b.sealed.LoadGeneration,
			"deployment_id", b.config.DeploymentID,
			"remedy", "configure BarrierConfig.StatePersister so each load can claim the blob",
		)
		b.loadGeneration = b.sealed.LoadGeneration
		return nil
	}

	claim := b.sealed.LoadGeneration + 1

	// Persist a private copy first, exactly as Rotate does: the live state must
	// not advance to a claim that never reached storage, or this barrier would
	// believe it holds a generation the next loader will hand out again.
	candidate := cloneSealedRootKey(b.sealed)
	candidate.LoadGeneration = claim
	if macErr := b.restampStateMACLocked(rootKey, candidate); macErr != nil {
		return macErr
	}
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		slog.Error("quicraft/seal: unseal aborted; the load-generation claim was not persisted",
			"epoch", b.epoch,
			"claim", claim,
			"error", perr,
		)
		return &StatePersistError{Op: "claim", Epoch: b.epoch, Err: perr}
	}

	b.sealed = candidate
	b.loadGeneration = claim
	return nil
}

// resolveInvocationBudget rotates to a fresh epoch when it can persist, else fails encryption closed, since a loaded epoch's spent budget is unknowable. The caller must hold b.mu.
func (b *Barrier) resolveInvocationBudget(ctx context.Context) {
	if b.invocations == nil || !b.invocations.IsEnabled() {
		// Counting is off by configuration (InvocationLimit == -1). The
		// embedder has explicitly opted out of the bound; nothing to enforce.
		return
	}
	if b.config.InvocationCounter != nil {
		// The embedder injected its own counter and owns persistence and
		// seeding (the documented contract of BarrierConfig.InvocationCounter).
		// Trust its state.
		return
	}
	if b.invocationsAuthoritative {
		// This process created the epoch (Initialize or Rotate) and the counter
		// has tracked every invocation since; a seal/unseal cycle within one
		// process lands here and must NOT be disturbed.
		return
	}
	limit := b.invocations.Limit()
	if limit <= 0 {
		return
	}

	if b.config.StatePersister != nil {
		newEpoch, err := b.rotateLocked(context.WithoutCancel(ctx))
		if err == nil {
			// The new DEK is known-zero for this process only; another loader of the same blob derives it too.
			slog.Warn("AEAD invocation count for the loaded epoch is unrecoverable; "+
				"rotated to a fresh epoch whose count is zero FOR THIS PROCESS (sound only "+
				"if this process is the sole writer of the sealed blob)",
				"from_epoch", newEpoch-1,
				"epoch", newEpoch,
				"limit", limit,
				"requires", "exclusive ownership of the persisted sealed root key",
			)
			// Each forced rotation grows KnownEpochs toward MaxKnownEpochs; warn while there is room to purge.
			if len(b.knownEpochs) > MaxKnownEpochs/2 {
				slog.Error("quicraft/seal: the known-epoch set is approaching the "+
					"load-time bound; a blob naming more than MaxKnownEpochs epochs "+
					"CANNOT BE LOADED, and every restart adds one",
					"known_epochs", len(b.knownEpochs),
					"limit", MaxKnownEpochs,
					"remedy", "call PurgeEpochsBefore with the minimum live epoch on a retention schedule",
				)
			}
			return
		}
		slog.Error("AEAD invocation count for the loaded epoch is unrecoverable and the rotation "+
			"that would restore a provable budget failed; encryption is now refused",
			"epoch", b.epoch,
			"error", err,
		)
	}

	b.invocations.Seed(limit)
	slog.Error("AEAD invocation budget for the current epoch is unrecoverable after restart and the "+
		"epoch cannot rotate; encryption is refused until it does (decryption is unaffected)",
		"epoch", b.epoch,
		"seeded_count", limit,
		"limit", limit,
		"cause", ErrInvocationBudgetUnknown.Error(),
		"remedy", "configure BarrierConfig.StatePersister so the epoch can rotate, or inject an "+
			"InvocationCounter that persists its own count across restarts",
	)
}

// seal wipes the root key and DEKs and seals the barrier; call it only through Host.Close or Host.EmergencySeal.
func (b *Barrier) seal() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return ErrBarrierSealed
	}

	// Destroy (wipe) every live key enclave before dropping references; enclaves already handed out belong to their callers.
	releaseSealed(b.rootKey, "root key")
	releaseSealed(b.currentDEK, "current DEK")
	for epoch, dek := range b.deks {
		releaseSealed(dek, "epoch DEK")
		delete(b.deks, epoch)
	}

	b.rootKey = nil
	b.currentDEK = nil
	b.cipher = nil
	b.deks = make(map[uint64]enclave.Sealed)
	// Each cipher.AEAD keeps a DEK copy in its key schedule that cannot be wiped; it lasts until GC.
	b.ciphers = make(map[uint64]cipher.AEAD)
	b.hwEncryptor = nil
	// The encryptor is gone, so the capability it carried is gone with it. A
	// subsequent Unseal re-derives all three from the strategy it is given.
	b.binding = ContextBindingUnresolved
	b.plaintextMode = false
	b.strategyID = ""
	b.strategyHardwareBacked = false
	b.state = barrierSealed
	b.setUnsealed(false)

	slog.Info("barrier sealed")
	return nil
}

// Seal is the exported variant of seal for testing and Host integration.
// Production code should prefer Host.Close(), which quiesces the pipeline
// first; Host.EmergencySeal() calls it directly, without quiescing.
func (b *Barrier) Seal() error {
	return b.seal()
}

// Reset returns a sealed first-boot barrier to uninitialized, destroying all in-memory key state; it never touches persisted state.
func (b *Barrier) Reset() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch {
	case b.state == barrierUninitialized:
		return ErrBarrierNotInit
	case b.state == barrierUnsealed:
		return ErrBarrierUnsealed
	case b.superseded.Load():
		return ErrBarrierSuperseded
	case !b.firstBoot:
		return ErrResetNotFirstBoot
	}

	// Lock order matches PurgeEpochsBefore (b.mu then inFlightMu). Held for the
	// rest of the reset so a pin cannot be registered against an epoch that is
	// in the middle of being discarded.
	b.inFlightMu.Lock()
	defer b.inFlightMu.Unlock()
	for _, n := range b.inFlightEpochs {
		if n > 0 {
			return ErrEpochsInFlight
		}
	}

	// seal() has already destroyed these in every reachable sealed state; doing
	// it again is idempotent and nil-safe, and keeps Reset correct on its own
	// rather than by reliance on the path that led here.
	releaseSealed(b.rootKey, "root key")
	releaseSealed(b.currentDEK, "current DEK")
	for _, dek := range b.deks {
		releaseSealed(dek, "epoch DEK")
	}

	b.rootKey = nil
	b.currentDEK = nil
	b.cipher = nil
	b.deks = make(map[uint64]enclave.Sealed)
	b.ciphers = make(map[uint64]cipher.AEAD)
	b.knownEpochs = make(map[uint64]struct{})
	b.salt = nil
	b.epoch = 0
	b.sealed = nil
	b.hkdfVersion = currentHKDFVersion
	b.loadGeneration = 0
	b.invocationsAuthoritative = false
	b.firstBoot = false
	b.failedAttempts = 0
	b.lastFailedAt = time.Time{}
	b.hwEncryptor = nil
	b.binding = ContextBindingUnresolved
	b.plaintextMode = false
	b.strategyID = ""
	b.strategyHardwareBacked = false
	b.state = barrierUninitialized
	b.setUnsealed(false)

	if b.invocations != nil {
		if err := b.invocations.Reset(); err != nil {
			slog.Warn("barrier reset but invocation counter reset failed; the next key will re-rotate earlier (fail-safe)",
				"tracker", "invocation", "op", "reset", "error", err)
		}
	}
	if b.nonceTracker != nil {
		if err := b.nonceTracker.Clear(); err != nil {
			slog.Warn("barrier reset but nonce tracker clear failed; stale nonces may be conservatively rejected (fail-safe)",
				"tracker", "nonce", "op", "clear", "error", err)
		}
	}

	slog.Info("barrier reset to uninitialized")
	return nil
}

// Encrypt seals plaintext under the current epoch DEK as [epoch:8][algID:1][nonce:N][ciphertext+tag]; dst must not overlap plaintext inexactly.
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

// EncryptWithEpoch encrypts plaintext and returns the epoch used; dst must not overlap plaintext inexactly.
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

// EncryptWithEpochPinned encrypts and atomically pins the epoch used; the caller must DeregisterInFlightEpoch it once the epoch is durably recorded.
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

// EncryptWithContext is Encrypt with context also bound into the AAD, defeating reordering and splicing but not tail truncation; nil context equals Encrypt.
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

// EncryptWithEpochContext is EncryptWithEpoch with a caller context in the AAD; see EncryptWithContext.
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

// EncryptWithEpochPinnedContext is EncryptWithEpochPinned with a caller context in the AAD; see EncryptWithContext.
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

// epochAAD returns the AEAD AAD binding a ciphertext to its epoch, as 8 little-endian bytes.
func epochAAD(epoch uint64) []byte {
	var aad [epochSize]byte
	binary.LittleEndian.PutUint64(aad[:], epoch)
	return aad[:]
}

// aadForEpoch returns epochAAD(epoch) followed by the optional context; an empty context gives exactly epochAAD(epoch).
func aadForEpoch(epoch uint64, contextAAD []byte) []byte {
	if len(contextAAD) == 0 {
		return epochAAD(epoch)
	}
	aad := make([]byte, epochSize+len(contextAAD))
	binary.LittleEndian.PutUint64(aad[:epochSize], epoch)
	copy(aad[epochSize:], contextAAD)
	return aad
}

// encryptLocked seals plaintext as [epoch:8][algID:1][nonce:N][ciphertext+tag], with contextAAD optional. The caller must hold b.mu.RLock.
func (b *Barrier) encryptLocked(dst, plaintext []byte, epoch uint64, aead cipher.AEAD, contextAAD []byte) ([]byte, error) {
	// Load-generation fence: a superseded barrier must not encrypt; see ErrBarrierSuperseded.
	if b.superseded.Load() {
		return nil, ErrBarrierSuperseded
	}

	// Bound the plaintext first: GCM and ChaCha20-Poly1305 panic on over-long input.
	if uint64(len(plaintext)) > b.maxPlaintext {
		return nil, ErrPlaintextTooLarge
	}

	// wireNonceSize is the nonce bytes this frame carries before the payload, zero for a module-nonce AEAD; the frame bytes are the same either way.
	wireNonceSize := aead.NonceSize()
	overhead := aead.Overhead()
	totalSize := epochSize + algIDSize + wireNonceSize + len(plaintext) + overhead

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

	payloadStart := epochSize + algIDSize

	// Generate the nonce, unless the AEAD generates its own.
	var nonce []byte
	if wireNonceSize > 0 {
		nonce = out[payloadStart : payloadStart+wireNonceSize]
		if _, err := io.ReadFull(b.randReader, nonce); err != nil {
			return nil, &Error{Strategy: "encrypt", Err: err}
		}

		// AEAD safety: nonce reuse detection.
		if b.nonceTracker != nil {
			if err := b.nonceTracker.CheckAndRecordNonce(nonce); err != nil {
				return nil, err
			}
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

	// Seal in place after the prefix; nonce is nil for a module-nonce AEAD. G407: nonce is nil or random.
	ciphertextStart := payloadStart + wireNonceSize
	aead.Seal(out[ciphertextStart:ciphertextStart], nonce, plaintext, aad) // #nosec G407 -- nonce is random, not hardcoded

	// A module-nonce AEAD reveals its nonce only in its output, so reuse is checked after Seal and the ciphertext withheld.
	if wireNonceSize == 0 && b.nonceTracker != nil {
		n := b.config.Algorithm.NonceSize()
		if n > 0 && len(out) >= payloadStart+n {
			if err := b.nonceTracker.CheckAndRecordNonce(out[payloadStart : payloadStart+n]); err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}

// Decrypt opens ciphertext with the DEK of the epoch in its header; dst must not overlap inexactly, and every failure is ErrDecryptionFailed.
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

// DecryptForEpoch opens ciphertext with a given epoch's DEK; every failure is ErrDecryptionFailed.
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

// DecryptWithContext is Decrypt requiring the context bound at encryption; a mismatch is ErrDecryptionFailed.
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

// DecryptForEpochContext is DecryptForEpoch with the context bound at encryption; see DecryptWithContext.
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

// decryptLocked opens ciphertext with the AAD from its header epoch and optional contextAAD. The caller must hold b.mu.RLock.
func (b *Barrier) decryptLocked(dst, ciphertext []byte, aead cipher.AEAD, alg SymmetricAlgorithm, contextAAD []byte) ([]byte, error) {
	nonceSize := alg.NonceSize()

	// wireNonceSize is the nonce bytes before the payload; activeNonceSize is the configured algorithm's nonce width.
	wireNonceSize := aead.NonceSize()
	activeNonceSize := wireNonceSize
	if activeNonceSize == 0 {
		activeNonceSize = b.config.Algorithm.NonceSize()
	}

	// Reject an algID whose nonce size differs from the active cipher, since a wrong nonce length panics in aead.Open.
	if nonceSize == 0 || nonceSize != activeNonceSize {
		return nil, ErrDecryptionFailed
	}

	// Validate that the ciphertext is long enough for the algorithm's nonce
	// plus at least a tag (overhead). Overhead() already counts the nonce for a
	// module-nonce AEAD, so the floor is the same 37 bytes either way.
	if len(ciphertext) < minCiphertextHeader+wireNonceSize+aead.Overhead() {
		return nil, ErrDecryptionFailed
	}

	// Extract the epoch from the ciphertext header for AAD construction.
	headerEpoch := binary.BigEndian.Uint64(ciphertext[:epochSize])

	// Extract nonce and encrypted payload (after epoch + algID prefix).
	nonceStart := epochSize + algIDSize
	var nonce []byte
	if wireNonceSize > 0 {
		nonce = ciphertext[nonceStart : nonceStart+wireNonceSize]
	}
	encrypted := ciphertext[nonceStart+wireNonceSize:]

	// Bound the payload: ChaCha20-Poly1305 Open panics on over-long input.
	if uint64(len(encrypted)) > b.maxPlaintext+uint64(aead.Overhead()) {
		return nil, ErrDecryptionFailed
	}

	// Construct AAD binding the ciphertext to its epoch and, when supplied, the
	// caller's positional/artifact context.
	aad := aadForEpoch(headerEpoch, contextAAD)

	plaintext, err := aead.Open(dst[:0], nonce, encrypted, aad)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// Rotate derives and persists a new epoch DEK, then installs it and resets the AEAD trackers; it requires a StatePersister and is not supported in hardware mode.
func (b *Barrier) Rotate(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rotateLocked(ctx)
}

// rotateLocked is Rotate's body. It is factored out because Unseal must also be
// able to rotate — see resolveInvocationBudget — while already holding b.mu.
// The caller must hold the exclusive write lock.
func (b *Barrier) rotateLocked(ctx context.Context) (uint64, error) {
	if b.state != barrierUnsealed {
		return 0, ErrBarrierSealed
	}

	// A superseded barrier must not write the blob.
	if b.superseded.Load() {
		return 0, ErrBarrierSuperseded
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

	// Step 1: derive the new DEK; Destroy is deferred because the AEADProvider is caller code.
	rootBuf, err := b.rootKey.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer releaseEnclave(rootBuf, "root key")
	// The root key is borrowed twice, each borrow scoped to the one call that
	// reads it: the derivation here and the state MAC re-stamp in step 2. Neither
	// borrow spans the sealing in step 1b, which may block on a provider.
	var (
		dek  []byte
		aead cipher.AEAD
	)
	err = rootBuf.WithBytes(func(root []byte) error {
		var derr error
		dek, aead, derr = b.deriveEpochCipher(root, b.salt, newEpoch)
		return derr
	})
	if err != nil {
		// Nothing committed yet; the old epoch remains active. deriveEpochCipher
		// already zeroed any partial key material on its own error path, but the
		// returned dek (if any) is wiped here for defense in depth.
		enclave.Wipe(dek)
		return 0, err
	}

	// Step 1b: seal the new DEK before persisting, so the steps after the persist cannot fail.
	dekPK, sealErr := enclave.NewSealed(ctx, dek)
	if sealErr != nil {
		return 0, &Error{Strategy: "rotate", Err: sealErr}
	}

	// Step 2: persist a private copy of the new state before activating it; the persister must not re-enter the barrier.
	candidate := cloneSealedRootKey(b.sealed)
	candidate.Epoch = newEpoch
	candidate.KnownEpochs = sortedEpochsWith(b.knownEpochs, newEpoch)
	// Re-authenticate the metadata: Epoch and KnownEpochs both changed, and a
	// blob whose MAC still covered the OLD values would be rejected by the next
	// Unseal (correctly — it would be indistinguishable from an edited one).
	if macErr := rootBuf.WithBytes(func(root []byte) error {
		return b.restampStateMACLocked(root, candidate)
	}); macErr != nil {
		releaseSealed(dekPK, "rotated DEK")
		return 0, macErr
	}
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		releaseSealed(dekPK, "rotated DEK")
		slog.Error("key rotation aborted: sealed state not persisted; previous epoch remains active",
			"from_epoch", oldEpoch, "to_epoch", newEpoch, "error", perr)
		return 0, &StatePersistError{Op: "rotate", Epoch: newEpoch, Err: perr}
	}

	// Step 3: commit the epoch switch — in-memory, infallible state mutations
	// under the held write lock. After this point the NEW key is active and the
	// rotation has succeeded; the trackers below are reset against the new epoch.
	b.epoch = newEpoch
	// An epoch above the first exists; Reset may no longer drop the floor.
	b.firstBoot = false
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks[newEpoch] = dekPK
	b.ciphers[newEpoch] = aead
	b.knownEpochs[newEpoch] = struct{}{}

	// Adopt the state that was just persisted, so SealedRootKeyData() and the
	// durable blob agree exactly.
	b.sealed = candidate

	// The new DEK's invocation count is known to be zero.
	b.invocationsAuthoritative = true

	// Step 4: reset the trackers; a failure is logged and fail-safe.
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

// PurgeEpochsBefore persists then drops DEKs of epochs below minEpoch, skipping the current and in-flight epochs, and returns the count purged.
func (b *Barrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return 0, ErrBarrierSealed
	}

	// A superseded barrier must not purge.
	if b.superseded.Load() {
		return 0, ErrBarrierSuperseded
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
	// KnownEpochs is covered by the metadata MAC, so a purge must re-stamp it
	// for the same reason Rotate does: otherwise the reduced blob fails its own
	// authentication on the next Unseal.
	if macErr := b.restampStateMACFromEnclaveLocked(ctx, candidate); macErr != nil {
		return 0, macErr
	}
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		slog.Error("epoch purge aborted: reduced sealed state not persisted; DEKs retained",
			"min_epoch", minEpoch, "error", perr)
		return 0, &StatePersistError{Op: "purge", Epoch: b.epoch, Err: perr}
	}

	for _, epoch := range purgeable {
		// Destroy the epoch's sealed DEK and drop its AEAD; the AEAD's key schedule copy lasts until GC.
		releaseSealed(b.deks[epoch], "purged epoch DEK")
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

// DeregisterInFlightEpoch releases one pin on epoch; a release with no pin changes nothing and is logged at ERROR.
func (b *Barrier) DeregisterInFlightEpoch(epoch uint64) {
	b.inFlightMu.Lock()
	n, held := b.inFlightEpochs[epoch]
	if !held || n <= 0 {
		b.inFlightMu.Unlock()
		slog.Error("quicraft/seal: unbalanced DeregisterInFlightEpoch; epoch holds no in-flight reference",
			"epoch", epoch,
			"deployment_id", b.config.DeploymentID,
			"impact", "ignored; a release without a matching Register would otherwise unpin an "+
				"epoch another writer still holds and expose its DEK to PurgeEpochsBefore",
		)
		return
	}
	n--
	if n == 0 {
		delete(b.inFlightEpochs, epoch)
	} else {
		b.inFlightEpochs[epoch] = n
	}
	b.inFlightMu.Unlock()
}

// IsSealed returns true if the barrier is in the sealed state.
func (b *Barrier) IsSealed() bool {
	return !b.unsealed.Load()
}

// UnsealedC returns a channel closed once the barrier is unsealed; call it again after each wake that finds it sealed.
func (b *Barrier) UnsealedC() <-chan struct{} {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	if b.unsealed.Load() {
		return closedUnsealC
	}
	if b.unsealWait == nil {
		b.unsealWait = make(chan struct{})
	}
	return b.unsealWait
}

// closedUnsealC is the channel UnsealedC returns for a barrier that is
// already unsealed.
var closedUnsealC = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// setUnsealed records the sealed state and, on the transition to unsealed,
// wakes every UnsealedC waiter. Every write of b.unsealed goes through here.
func (b *Barrier) setUnsealed(unsealed bool) {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	b.unsealed.Store(unsealed)
	if unsealed && b.unsealWait != nil {
		close(b.unsealWait)
		b.unsealWait = nil
	}
}

// IsInitialized returns true if the barrier has been initialized (is not in
// the uninitialized state). A sealed barrier is still considered initialized.
func (b *Barrier) IsInitialized() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state != barrierUninitialized
}

// IsHardwareMode reports whether a delegated encryptor handles all encryption, no-encryption mode included; see ContextBinding.
func (b *Barrier) IsHardwareMode() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.hwEncryptor != nil
}

// RootKey returns a heap copy of the root key that the caller should zero; prefer RootKeyProtected.
func (b *Barrier) RootKey(ctx context.Context) ([]byte, error) {
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
	buf, err := b.rootKey.Open(ctx)
	if err != nil {
		return nil, err
	}
	// Deferred, so a stranded enclave cannot hold the root key.
	defer releaseEnclave(buf, "root key")
	keyCopy := make([]byte, buf.Size())
	if err := buf.WithBytes(func(b []byte) error {
		copy(keyCopy, b)
		return nil
	}); err != nil {
		enclave.Wipe(keyCopy)
		return nil, err
	}
	return keyCopy, nil
}

// RootKeyProtected returns the root key in a frozen locked Memory the caller must Destroy and must never wipe by hand; ErrNoRootKeyAvailable in no-encryption mode.
func (b *Barrier) RootKeyProtected(ctx context.Context) (enclave.ByteAccess, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != barrierUnsealed {
		return nil, ErrBarrierSealed
	}
	if b.rootKey == nil {
		return nil, ErrNoRootKeyAvailable
	}
	// Returned unwrapped: go-enclave's error names its own origin.
	return b.rootKey.Open(ctx)
}

// CurrentEpoch returns the current key rotation epoch. Returns 0 if the
// barrier has not been initialized.
func (b *Barrier) CurrentEpoch() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.epoch
}

// LoadGeneration returns the load generation this barrier claimed, or 0 if it never loaded or cannot claim. See SealedRootKey.LoadGeneration.
func (b *Barrier) LoadGeneration() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.loadGeneration
}

// IsSuperseded reports whether another loader claimed this barrier's sealed state; see ErrBarrierSuperseded.
func (b *Barrier) IsSuperseded() bool { return b.superseded.Load() }

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
		LoadGeneration:     src.LoadGeneration,
		FailedAttempts:     src.FailedAttempts,
		LastFailedUnixNano: src.LastFailedUnixNano,
		KDFVersion:         src.KDFVersion,
		HKDFVersion:        src.HKDFVersion,
		Algorithm:          src.Algorithm,
		ShamirThreshold:    src.ShamirThreshold,
		ShamirTotal:        src.ShamirTotal,
		CreatedAt:          src.CreatedAt,
	}
	cp.Salt = make([]byte, len(src.Salt))
	copy(cp.Salt, src.Salt)
	cp.Ciphertext = make([]byte, len(src.Ciphertext))
	copy(cp.Ciphertext, src.Ciphertext)
	if src.StateMAC != nil {
		cp.StateMAC = make([]byte, len(src.StateMAC))
		copy(cp.StateMAC, src.StateMAC)
	}
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

// sealedStateMACInfo is the HKDF info string that separates the metadata
// authentication key from every epoch DEK derived from the same root key. The
// two derivations share a root key and must never share an output.
const sealedStateMACInfo = "quicraft/barrier/state-mac/v1"

// sealedStateMACVersion is the version byte of the MAC's canonical encoding, fed first to the HMAC; version 1 is retired.
const sealedStateMACVersion byte = 2

// previousSealedStateMACVersion is the other member of the read window (docs/compatibility, "The accept sets"); equal to sealedStateMACVersion in v1.0.
const previousSealedStateMACVersion = sealedStateMACVersion

// sealedStateMAC returns HMAC-SHA256, keyed by HKDF-SHA256(rootKey, sealedStateMACInfo), over a length-prefixed encoding of every field deciding key use; backoff and custody fields are not covered.
func sealedStateMAC(rootKey []byte, deploymentID uint64, s *SealedRootKey) ([]byte, error) {
	return sealedStateMACAt(sealedStateMACVersion, rootKey, deploymentID, s)
}

// sealedStateMACAt computes the tag with version as its first input; only sealedStateMACVersion is written.
func sealedStateMACAt(version byte, rootKey []byte, deploymentID uint64, s *SealedRootKey) ([]byte, error) {
	macKey := make([]byte, sha256.Size)
	if _, err := io.ReadFull(hkdf.New(sha256.New, rootKey, nil, []byte(sealedStateMACInfo)), macKey); err != nil {
		return nil, &Error{Strategy: "state-mac", Err: err}
	}
	defer enclave.Wipe(macKey)

	mac := hmac.New(sha256.New, macKey)
	var scratch [8]byte
	putUint := func(v uint64) {
		binary.BigEndian.PutUint64(scratch[:], v)
		mac.Write(scratch[:])
	}
	putBytes := func(p []byte) {
		putUint(uint64(len(p)))
		mac.Write(p)
	}

	mac.Write([]byte{version})
	putBytes([]byte(s.Strategy))
	putUint(uint64(s.Version))
	putUint(deploymentID)
	putUint(s.Epoch)
	putUint(s.LoadGeneration)
	putBytes(s.Salt)
	putUint(uint64(len(s.KnownEpochs)))
	for _, ep := range s.KnownEpochs {
		putUint(ep)
	}
	putUint(uint64(s.HKDFVersion))
	putUint(uint64(s.Algorithm))
	putBytes(s.Ciphertext)
	putBytes(s.Metadata)
	putBytes(s.Nonce)
	putBytes(s.HardwarePayload)

	runtime.KeepAlive(rootKey)
	return mac.Sum(nil), nil
}

// verifySealedStateMAC checks b.sealed's tag against the recovered root key; a missing tag is refused. The caller must hold b.mu.
func (b *Barrier) verifySealedStateMAC(rootKey []byte) error {
	if b.sealed == nil {
		return ErrNilSealedData
	}
	if len(b.sealed.StateMAC) == 0 {
		slog.Error("quicraft/seal: unseal refused; sealed root key carries no metadata MAC",
			"epoch", b.sealed.Epoch,
			"deployment_id", b.config.DeploymentID,
			"remedy", "wipe and reseal",
		)
		return ErrSealedStateUnauthenticated
	}
	want, err := sealedStateMAC(rootKey, b.config.DeploymentID, b.sealed)
	if err != nil {
		return err
	}
	if hmac.Equal(want, b.sealed.StateMAC) {
		return nil
	}
	// The read window is {sealedStateMACVersion, previousSealedStateMACVersion};
	// the second tag is computed only once the two differ.
	if previousSealedStateMACVersion != sealedStateMACVersion {
		prev, prevErr := sealedStateMACAt(previousSealedStateMACVersion, rootKey, b.config.DeploymentID, b.sealed)
		if prevErr == nil && hmac.Equal(prev, b.sealed.StateMAC) {
			return nil
		}
	}
	slog.Error("quicraft/seal: unseal refused; sealed root key metadata failed authentication",
		"epoch", b.sealed.Epoch,
		"deployment_id", b.config.DeploymentID,
	)
	return ErrSealedStateUnauthenticated
}

// restampStateMACLocked recomputes s.StateMAC from a plaintext root key. It is
// called by every operation that changes a covered field. The caller must hold
// b.mu.
func (b *Barrier) restampStateMACLocked(rootKey []byte, s *SealedRootKey) error {
	mac, err := sealedStateMAC(rootKey, b.config.DeploymentID, s)
	if err != nil {
		return err
	}
	s.StateMAC = mac
	return nil
}

// restampStateMACFromEnclaveLocked is restampStateMACLocked for callers without the root key open. The caller must hold b.mu, unsealed in software mode.
func (b *Barrier) restampStateMACFromEnclaveLocked(ctx context.Context, s *SealedRootKey) error {
	if b.rootKey == nil {
		return ErrNoRootKeyAvailable
	}
	rootBuf, err := b.rootKey.Open(ctx)
	if err != nil {
		return err
	}
	defer releaseEnclave(rootBuf, "root key")
	return rootBuf.WithBytes(func(root []byte) error {
		return b.restampStateMACLocked(root, s)
	})
}

// StampSealedStateMAC re-authenticates a blob the embedder re-wrapped itself; call it after every covered field is final. The barrier must be unsealed.
func (b *Barrier) StampSealedStateMAC(ctx context.Context, sealed *SealedRootKey) error {
	if sealed == nil {
		return ErrNilSealedData
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return ErrBarrierSealed
	}
	return b.restampStateMACFromEnclaveLocked(ctx, sealed)
}

// StampSealedStateMACWithRootKey is StampSealedStateMAC from a plaintext root key, for importing before any barrier exists; deploymentID must match the unsealing barrier's.
func StampSealedStateMACWithRootKey(rootKey []byte, deploymentID uint64, sealed *SealedRootKey) error {
	if sealed == nil {
		return ErrNilSealedData
	}
	if len(rootKey) == 0 {
		return ErrNoRootKeyAvailable
	}
	mac, err := sealedStateMAC(rootKey, deploymentID, sealed)
	if err != nil {
		return err
	}
	sealed.StateMAC = mac
	return nil
}

// SetSealedRootKey validates and copies a persisted sealed root key for Unseal, refusing epoch rollbacks below MinEpoch and marking the barrier superseded on a higher LoadGeneration.
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

	// Reject an epoch below MinEpoch or the epoch already held.
	floor := b.config.MinEpoch
	if b.state != barrierUninitialized && b.epoch > floor {
		floor = b.epoch
	}
	if sealed.Epoch < floor {
		slog.Error("quicraft/seal: sealed root key rejected; its epoch is below the rollback floor",
			"blob_epoch", sealed.Epoch,
			"floor", floor,
			"configured_min_epoch", b.config.MinEpoch,
			"deployment_id", b.config.DeploymentID,
		)
		return ErrSealedEpochRollback
	}

	// Validate salt length.
	if len(sealed.Salt) != hkdfSaltSize {
		return ErrInvalidSealedSalt
	}

	// Bound the derivation work Unseal will do before doing any of it.
	if len(sealed.KnownEpochs) > MaxKnownEpochs {
		slog.Error("quicraft/seal: sealed root key rejected; KnownEpochs exceeds the derivation bound",
			"known_epochs", len(sealed.KnownEpochs),
			"limit", MaxKnownEpochs,
			"epoch", sealed.Epoch,
		)
		return ErrTooManyKnownEpochs
	}

	// KnownEpochs must contain the current epoch and nothing above it or equal to 0.
	epochFound := false
	for _, ep := range sealed.KnownEpochs {
		if ep == 0 {
			return ErrInvalidSealedEpoch
		}
		if ep > sealed.Epoch {
			return ErrKnownEpochAboveCurrent
		}
		if ep == sealed.Epoch {
			epochFound = true
		}
	}
	if !epochFound {
		return ErrInvalidSealedEpochs
	}

	// Refuse any HKDF layout but currentHKDFVersion.
	if sealed.HKDFVersion != currentHKDFVersion {
		return ErrUnsupportedHKDFVersion
	}

	// Refuse a recorded algorithm other than the configured one; zero means unrecorded.
	if sealed.Algorithm != 0 && sealed.Algorithm != b.config.Algorithm {
		return &AlgorithmMismatchError{Sealed: sealed.Algorithm, Configured: b.config.Algorithm}
	}

	// Load-generation fence, checked last: a generation above this barrier's claim supersedes it permanently, without adopting the state.
	if b.loadGeneration > 0 && sealed.LoadGeneration > b.loadGeneration {
		b.superseded.Store(true)
		slog.Error("quicraft/seal: sealed root key was claimed by another loader; this barrier is "+
			"now refused encryption (decryption is unaffected)",
			"observed_generation", sealed.LoadGeneration,
			"claimed_generation", b.loadGeneration,
			"epoch", b.epoch,
			"deployment_id", b.config.DeploymentID,
			"remedy", "stop one of the two processes holding this sealed state; which instance "+
				"should own it is an operator decision and is never resolved automatically",
		)
		return &SupersededError{Observed: sealed.LoadGeneration, Claimed: b.loadGeneration}
	}
	// A generation below the claim is a rollback; report it and continue.
	if b.loadGeneration > 0 && sealed.LoadGeneration < b.loadGeneration {
		slog.Error("quicraft/seal: sealed root key load generation went BACKWARDS; the persisted "+
			"state is older than the one this barrier claimed",
			"observed_generation", sealed.LoadGeneration,
			"claimed_generation", b.loadGeneration,
			"epoch", sealed.Epoch,
			"deployment_id", b.config.DeploymentID,
			"remedy", "set BarrierConfig.MinEpoch from the highest live epoch to refuse replayed state",
		)
	}

	// Copy before retaining: b.salt would otherwise alias the caller's slice,
	// and persistBackoffState would write into the caller's live struct.
	sealed = cloneSealedRootKey(sealed)
	b.sealed = sealed
	b.salt = sealed.Salt
	b.epoch = sealed.Epoch
	// A loaded root key ends first boot; see firstBoot.
	b.firstBoot = false

	// The loaded epoch's invocation count is unknown; see resolveInvocationBudget.
	b.invocationsAuthoritative = false

	// Adopt the blob's generation as the baseline the next Unseal increments.
	// It is the on-disk state that is authoritative here, exactly as it is for
	// KnownEpochs below.
	b.loadGeneration = sealed.LoadGeneration

	b.hkdfVersion = sealed.HKDFVersion

	// Replace known epochs with a fresh set from the blob, never a merge.
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

// persistBackoffState writes the backoff counters to the sealed key and the StatePersister. The caller must hold b.mu.
func (b *Barrier) persistBackoffState() {
	if b.sealed == nil {
		return
	}
	var lastFailed int64
	if !b.lastFailedAt.IsZero() {
		lastFailed = b.lastFailedAt.UnixNano()
	}
	if b.sealed.FailedAttempts == b.failedAttempts && b.sealed.LastFailedUnixNano == lastFailed {
		return
	}
	b.sealed.FailedAttempts = b.failedAttempts
	b.sealed.LastFailedUnixNano = lastFailed

	// Persist only a change, best effort; the backoff fields are outside StateMAC.
	persist := b.config.StatePersister
	if persist == nil {
		return
	}
	if err := persist(cloneSealedRootKey(b.sealed)); err != nil {
		slog.Warn("quicraft/seal: unseal backoff state was not persisted; it resets on restart",
			"failed_attempts", b.failedAttempts,
			"error", err,
		)
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

// deriveDEK derives an algorithm-sized DEK with HKDF-SHA256 under the blob's HKDF version, independent per (deploymentID, epoch); see HKDFV2.
func deriveDEK(rootKey, salt []byte, deploymentID, epoch uint64, alg SymmetricAlgorithm, version HKDFVersion) ([]byte, error) {
	keySize := alg.KeySize()
	if keySize == 0 {
		return nil, &KeyDerivationError{Epoch: epoch, Err: ErrUnsupportedAlgorithm}
	}

	if version != HKDFV2 {
		return nil, &KeyDerivationError{Epoch: epoch, Err: ErrUnsupportedHKDFVersion}
	}

	var info [hkdfInfoLen]byte
	copy(info[:20], hkdfInfoPrefix)
	binary.BigEndian.PutUint64(info[20:], deploymentID)
	binary.BigEndian.PutUint64(info[28:], epoch)

	info[36] = byte(alg)
	info[37] = byte(keySize)

	hkdfReader := hkdf.New(sha256.New, rootKey, salt, info[:])
	dek := make([]byte, keySize)
	if _, err := io.ReadFull(hkdfReader, dek); err != nil {
		return nil, &KeyDerivationError{Epoch: epoch, Err: err}
	}
	runtime.KeepAlive(rootKey)
	return dek, nil
}

// deriveEpochCipher derives epoch's DEK and AEAD for the configured algorithm, zeroing partial key material on error.
func (b *Barrier) deriveEpochCipher(rootKey, salt []byte, epoch uint64) ([]byte, cipher.AEAD, error) {
	alg := b.config.Algorithm
	dek, err := deriveDEK(rootKey, salt, b.config.DeploymentID, epoch, alg, b.hkdfVersion)
	if err != nil {
		return nil, nil, err
	}
	aead, err := b.aeadProvider.NewAEAD(alg, dek)
	if err != nil {
		enclave.Wipe(dek)
		return nil, nil, err
	}
	return dek, aead, nil
}
