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

	// MaxKnownEpochs bounds SealedRootKey.KnownEpochs at load time. Unseal
	// re-derives one HKDF DEK, one AEAD and one mlock'd enclave per entry, and
	// the set is plaintext, unauthenticated metadata, so without a bound an
	// attacker with write access to the persisted blob turns every start into
	// an unbounded derivation and mlock storm.
	//
	// The value is far above any legitimate deployment: epochs are retired by
	// PurgeEpochsBefore once no WAL segment or snapshot still references them,
	// so the live set tracks retention, not the lifetime rotation count. A
	// cluster rotating hourly and purging daily holds tens; 4096 leaves four
	// orders of magnitude of headroom while capping the work an attacker can
	// force to milliseconds.
	MaxKnownEpochs = 4096

	// firstLoadGeneration is the LoadGeneration Initialize stamps into a new
	// sealed blob. It starts at 1 rather than 0 so that "this blob has never
	// been claimed" (a blob written before the fence existed, which decodes to
	// 0) stays distinguishable from "claimed once by the process that created
	// it". See SealedRootKey.LoadGeneration.
	firstLoadGeneration uint64 = 1
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
// Because that one algorithm is a property of the DATA and not of the host,
// Initialize records it in SealedRootKey.Algorithm and SetSealedRootKey refuses
// a blob that disagrees with BarrierConfig.Algorithm, so a data directory moved
// to a host whose CPU would auto-select differently is rejected by name rather
// than read back as ErrDecryptionFailed on every record.
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
// Seal, Unseal, Initialize, Reset, and Rotate acquire the exclusive write lock.
//
// Lifecycle: Initialize moves Uninitialized to Unsealed; Seal and Unseal move
// between Unsealed and Sealed; SetSealedRootKey moves Uninitialized to Sealed;
// Reset moves Sealed back to Uninitialized, and only while the root key in hand
// is still the uncompleted first boot of this process's own Initialize (see
// firstBoot).
type Barrier struct {
	mu       sync.RWMutex
	unsealed atomic.Bool // fast-path check, avoids RWMutex contention on hot path

	config BarrierConfig // algorithm selection, deployment identity, tracking config

	state   barrierState
	rootKey *enclave.SealedEnclave // 32 bytes, held as ciphertext at rest; opened into locked memory
	salt    []byte                 // 32 bytes, generated at Initialize, used for HKDF

	// hkdfVersion is the DEK-derivation info layout in force. Set to
	// currentHKDFVersion for a fresh barrier and overwritten from the
	// persisted SealedRootKey by SetSealedRootKey.
	hkdfVersion HKDFVersion

	epoch      uint64                            // current key rotation epoch
	currentDEK *enclave.SealedEnclave            // derived via HKDF from rootKey + epoch, encrypted at rest
	cipher     cipher.AEAD                       // current epoch's AEAD cipher (cached, opaque)
	deks       map[uint64]*enclave.SealedEnclave // epoch -> DEK encrypted at rest for re-derivation
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

	// binding is the context-AAD capability of the installed encryptor, resolved
	// once by Initialize/Unseal (which is where the behavioural probe in
	// contextBindingFor runs) and read back by ContextBinding. Reset to
	// ContextBindingUnresolved by seal(), because the capability belongs to the
	// encryptor and the encryptor is gone.
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

	// maxPlaintext is the configured algorithm's single-invocation plaintext
	// ceiling, resolved once at construction because the algorithm is fixed for
	// the barrier's lifetime (SetSealedRootKey refuses a blob that disagrees
	// with it). It exists as a field rather than a call per Encrypt so the bound
	// is one named value at the single place that enforces it; see
	// SymmetricAlgorithm.MaxPlaintextSize for why a bound is required at all.
	maxPlaintext uint64

	// AEAD safety trackers (interface types for IoC).
	invocations  InvocationCounter    // per-epoch AEAD invocation counter
	nonceTracker NonceTrackerProvider // per-epoch nonce reuse detection

	// invocationsAuthoritative records that b.invocations reflects the TRUE
	// number of AEAD invocations performed under b.epoch, and is the whole
	// basis on which Unseal decides whether it may keep encrypting under the
	// loaded epoch. It is true only for an epoch this process itself started
	// from zero: Initialize and Rotate set it, and seal() leaves it alone so an
	// in-process seal/unseal cycle keeps its accurate count. SetSealedRootKey
	// clears it, because a blob loaded from disk describes an epoch some
	// EARLIER process encrypted under for an unknown number of invocations.
	//
	// A plain "is the count zero" test cannot express this: zero is both "never
	// encrypted" and "fresh process, counter not yet seeded", and reading the
	// second as the first is what re-granted the NIST budget on every restart.
	invocationsAuthoritative bool

	// firstBoot records that the root key in hand was created by THIS
	// process's Initialize and has never left its first epoch: no blob has been
	// applied through SetSealedRootKey and no rotation (Rotate, or Unseal's
	// forced rotation) has committed. Initialize sets it; SetSealedRootKey,
	// rotateLocked and Reset clear it, and nothing else ever sets it.
	//
	// It is the whole basis on which Reset may discard the barrier's epoch.
	// Reset zeroes b.epoch and b.state, and those are the in-process rollback
	// floor SetSealedRootKey enforces; once any epoch above the first has been
	// derived, or a blob from an earlier lifetime has been loaded, zeroing them
	// would let an older authentic blob load and Unseal's forced rotation
	// re-derive an epoch whose DEK was already used, with fresh counters —
	// nonce and DEK reuse. A first boot that never advanced past epoch 1 has no
	// such epoch: the only blob that can name its key also names epoch 1, and
	// the forced rotation on loading it derives epoch 2, which never existed.
	firstBoot bool

	// loadGeneration is the LoadGeneration this barrier CLAIMED — stamped by
	// Initialize (1) or written by the claim step in Unseal (loaded + 1). It is
	// the number a later observation is compared against, and it is guarded by
	// b.mu like the rest of the sealed state.
	loadGeneration uint64

	// superseded records that a blob naming a generation above b.loadGeneration
	// was observed, i.e. another loader has taken this sealed state. It is an
	// atomic.Bool rather than plain state under b.mu so the Encrypt fast path
	// can consult it with a single load and no additional lock, matching
	// b.unsealed; every write path re-checks it under the lock it already
	// holds. Once set it is never cleared — see ErrBarrierSuperseded.
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

	// lockRootKey allocates the locked memory Initialize and Unseal hold the
	// root key in. It is a FIELD rather than a package variable for the same
	// reason randReader above is one: a mutable global on the root-key path is
	// exactly what this package avoids, and a field cannot be reached from
	// another test, another barrier, or production code that did not ask for it.
	//
	// It exists because the failure it stands in for cannot be produced
	// otherwise. A refused mlock means RLIMIT_MEMLOCK is exhausted process-wide,
	// which a test cannot arrange without taking every other enclave in the
	// binary with it. What has to be provable is that the refusal is CLEAN: the
	// barrier stays uninitialized or sealed, and on the unseal path the
	// strategy's heap copy is still wiped.
	//
	// Defaults to enclave.NewMemoryEnclaveSized; nothing but a test assigns it.
	lockRootKey func(int) (*enclave.MemoryEnclave, error)
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
// It returns the error from BarrierConfig.Validate when the configuration is
// invalid — an unknown algorithm, or a non-FIPS-approved cipher while FIPS
// mode is active — and never panics. This package is embedded in other
// products' processes, where a config assembled at RUNTIME from a file, an API
// request, or per-tenant settings is the normal case; an operator's bad
// algorithm must surface as an error the embedder can reject, not as a crash
// of the host process.
//
// Use MustNewBarrier for a config built entirely from compile-time constants,
// where an invalid value is a programmer error.
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

	// Nonce tracking retains every nonce until Clear, and Clear is reached only
	// through a successful Rotate — which fails closed with ErrNoStatePersister
	// when no persister is configured. With no injected InvocationCounter
	// either, nothing else can release the tracked set, so this combination
	// provably cannot rotate and the tracker grows for the process lifetime.
	// Reported rather than rejected: it is what DefaultBarrierConfig produces,
	// so rejecting it would fail the default. The tracker itself fails safe at
	// DefaultMaxTrackedNonces.
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
		deks:           make(map[uint64]*enclave.SealedEnclave),
		ciphers:        make(map[uint64]cipher.AEAD),
		knownEpochs:    make(map[uint64]struct{}),
		inFlightEpochs: make(map[uint64]int64),
		invocations:    ic,
		nonceTracker:   nt,
		aeadProvider:   ap,
		maxPlaintext:   config.Algorithm.MaxPlaintextSize(),
		timeNow:        time.Now,
		randReader:     rand.Reader,
		lockRootKey:    enclave.NewMemoryEnclaveSized,
	}, nil
}

// MustNewBarrier is NewBarrier for a configuration known to be valid at
// compile time. It panics when the configuration is invalid, following the
// regexp.MustCompile precedent: it is for package-level variables and
// constant configs, where a bad value is a programmer error that must never
// reach production.
//
// Never call it on a config assembled at runtime from a file, an API request,
// or per-tenant settings. Use NewBarrier there so the operator's mistake is an
// error the embedder can reject, not a crash of the embedding process.
func MustNewBarrier(config BarrierConfig) *Barrier {
	b, err := NewBarrier(config)
	if err != nil {
		panic("seal.MustNewBarrier: " + err.Error())
	}
	return b
}

// NewEpochBarrier calls NewBarrier. Use this name in new code to distinguish
// from go-qrdb's NewStorageBarrier.
//
// These two are FUNCTIONS rather than `var NewEpochBarrier = NewBarrier`, for
// the reason given at NewProtectedCredentials: an exported function variable in
// a crypto package is writable by every importer, so an init() in a transitive
// dependency can substitute the constructor for the barrier that holds the root
// key. A named alias is worth having; a reassignable one is not (#95).
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
// return ErrBarrierAlreadyInit, unless Reset has returned the sealed barrier
// to the uninitialized state to roll back an initialization the caller could
// not complete.
func (b *Barrier) Initialize(ctx context.Context, strategy SealingStrategy, creds Credentials) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUninitialized {
		return ErrBarrierAlreadyInit
	}

	// THE ROOT KEY IS GENERATED INTO LOCKED MEMORY AND NEVER EXISTS OUTSIDE IT.
	//
	// This used to be `rootKey := make([]byte, rootKeySize)` — an ordinary heap
	// slice, swappable and core-dumpable, kept alive across sealing, MAC
	// stamping, encryptor resolution and DEK derivation, and wiped by hand on
	// each of the seven error paths that could return in between. The wipes were
	// correct; what they could not do was stop the key being written to disk by
	// the pager, or captured in a core file, while it was in use.
	//
	// A SIZED enclave, not a from-bytes one: the material is produced IN PLACE by
	// the read below, so it never transits the heap on its way in. The pages stay
	// writable for the enclave's life, and DELIBERATELY are not frozen even though
	// MemoryEnclave.Freeze now exists — the terminal enclave.NewSealed wipes this
	// slice as it consumes it, and a wipe of frozen read-only pages faults the
	// process. Freezing here would trade a stray-write fault for a certain one.
	// The borrowed slice never escapes initializeWithRootKey, which is the
	// condition that makes a writable enclave safe.
	//
	// Destroy replaces every enclave.Wipe(rootKey) that used to stand on the error
	// paths: it wipes and unmaps, and being deferred it cannot be skipped by a
	// return that a later edit adds.
	rootEnc, err := b.lockRootKey(rootKeySize)
	if err != nil {
		return &Error{Strategy: "initialize", Err: err}
	}
	defer rootEnc.Destroy()

	// Everything that touches the root key runs inside the borrow, so a Destroy
	// cannot unmap the pages underneath it. Nesting a second enclave operation in
	// here — enclave.NewSealed below does exactly that — is legal since the
	// go-enclave adoption; under this repository's old process-wide purgeMu it
	// was a self-deadlock, which is why this could not be written before #54.
	return rootEnc.WithBytes(func(rootKey []byte) error {
		if _, err := io.ReadFull(b.randReader, rootKey); err != nil {
			return &Error{Strategy: "initialize", Err: err}
		}
		return b.initializeWithRootKey(ctx, strategy, creds, rootKey)
	})
}

// initializeWithRootKey is Initialize's body, running with b.mu held and with
// rootKey aliasing the locked pages of the enclave its caller borrowed.
//
// It must not retain rootKey past its own return: the slice is only backed while
// the borrow is open, and the enclave is destroyed immediately afterwards. It
// must not destroy or re-borrow that enclave either — the caller owns it.
//
// There is deliberately no enclave.Wipe(rootKey) anywhere below. The borrow's
// lifetime is the wipe now, which is the point of the change: an error path
// added later cannot forget one.
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
	// Record the data-path AEAD. It is a derivation input (HKDFV2 domain
	// separator) and the cipher Decrypt selects by epoch, and nothing else on
	// disk names it, so a blob that does not carry it cannot be moved to a host
	// whose CPU makes SelectOptimalAlgorithm choose differently without every
	// read failing as ErrDecryptionFailed. See SealedRootKey.Algorithm.
	sealed.Algorithm = b.config.Algorithm
	// Authenticate the metadata just written. Everything above this line is
	// plaintext beside the root-key ciphertext and covered by nothing; the MAC
	// is what makes Epoch and KnownEpochs unforgeable to anyone who does not
	// hold the root key. StrategyNone keeps no root key, so it has no MAC and
	// none is expected of it (see SealedRootKey.StateMAC).
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

	// Resolve the context-binding capability BEFORE any state transition, so a
	// deployment that requires positional binding is refused at setup rather
	// than on its first context-bearing write.
	binding := contextBindingFor(hwEnc, strategy.ID() == StrategyNone, b.randReader)
	if err := b.checkContextAADPolicy(binding); err != nil {
		return err
	}
	b.binding = binding
	b.strategyID = strategy.ID()
	b.strategyHardwareBacked = strategy.HardwareBacked()
	// Derived from the binding, not from the strategy ID alone: a strategy that
	// names itself "none" but supplies no encryptor takes the real epoch-AEAD
	// path, and reporting it as plaintext would be a lie in the direction that
	// hides an actual capability.
	b.plaintextMode = binding == ContextBindingPlaintext

	if hwEnc != nil {
		// Hardware mode: skip DEK derivation, use hardware encryptor.
		//
		// Sealed BEFORE the first state assignment, so a refusal leaves the
		// barrier uninitialized and retryable rather than half-installed. It
		// consumes rootKey on EVERY path including this one, so a failure here
		// still leaves nothing readable behind.
		rootPK, sealErr := enclave.NewSealed(rootKey)
		if sealErr != nil {
			return &Error{Strategy: "initialize", Err: sealErr}
		}
		b.hwEncryptor = hwEnc
		b.rootKey = rootPK
		b.salt = salt
		b.epoch = initialEpoch
		b.knownEpochs[initialEpoch] = struct{}{}
		b.sealed = sealed
		b.state = barrierUnsealed
		b.unsealed.Store(true)
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

	// Seal both keys BEFORE the first state assignment, so that everything below
	// is infallible and a refused allocation cannot leave a barrier half in the
	// unsealed state. enclave.NewSealed consumes its argument on every path, so
	// the plain dek and the borrowed rootKey are wiped whether it succeeds or not
	// — which is why neither needs a hand-written wipe on the error paths here.
	//
	// The root key goes LAST because deriveEpochCipher above still needed it in
	// the clear, and sealing it is what takes it away.
	dekPK, err := enclave.NewSealed(dek)
	if err != nil {
		return &Error{Strategy: "initialize", Err: err}
	}
	rootPK, err := enclave.NewSealed(rootKey)
	if err != nil {
		dekPK.Destroy()
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
	b.unsealed.Store(true)
	// This process created the epoch, so its counter starts at a known zero and
	// stays authoritative until a blob describing some other lifetime is loaded.
	b.invocationsAuthoritative = true
	// ...and it is a first boot Reset may still roll back, until the epoch
	// advances or another blob is loaded over it.
	b.firstBoot = true

	slog.Info("barrier initialized", "epoch", initialEpoch, "algorithm", b.config.Algorithm.String())
	return nil
}

// unsealPreconditionLocked reports why this barrier may not attempt an unseal
// at all, or nil when it may. The caller must hold b.mu.
//
// Extracted from Unseal to keep it under the cyclomatic limit, which the
// load-generation claim and its pre-fence migration path pushed it over.
//
// The superseded case is the interesting one: a barrier that has lost the claim
// must not re-claim by sealing and unsealing again. Allowing it would launder
// the supersession into a fresh claim and put two instances into a loop of
// stealing the blob back from each other, which is precisely the outcome
// leaving the choice to an operator is meant to avoid.
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
//
// Unseal AUTHENTICATES the persisted metadata (SealedRootKey.StateMAC) against
// the recovered root key before deriving anything from it, and fails with
// ErrSealedStateUnauthenticated if the Epoch, KnownEpochs, Salt, HKDFVersion or
// Algorithm were edited by something that does not hold that key.
//
// Unseal also ROTATES when it restores a blob this process did not create — one
// epoch per restart — because the loaded epoch's spent invocation budget is
// unknowable; see resolveInvocationBudget. Two consequences follow. The
// StatePersister is called from here, under the write lock. And KnownEpochs
// grows by one per restart until PurgeEpochsBefore retires the old epochs, so a
// deployment that never purges walks toward MaxKnownEpochs; the engine's
// retention-driven purge is what keeps the set bounded.
//
// Unseal also CLAIMS the blob: it writes back SealedRootKey.LoadGeneration + 1
// through the StatePersister before the barrier will encrypt, so a later loader
// can tell that this one was here. The claim is what makes ErrBarrierSuperseded
// possible; see claimLoadGenerationLocked for what it does and does not detect,
// and for the modes (delegated, no-encryption, no persister) in which it is
// inert.
func (b *Barrier) Unseal(ctx context.Context, strategy SealingStrategy, creds Credentials) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.unsealPreconditionLocked(); err != nil {
		return err
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
	heapKey, err := strategy.UnsealRootKey(ctx, b.sealed, creds)
	// THE BACKSTOP FOR THE STRATEGY'S HEAP COPY, standing from the moment the
	// call returns — before the error check, not after it. heapKey is the full
	// plaintext root key on the ordinary Go heap, swappable and core-dumpable,
	// and every return below this line has to leave it zeroed. Nothing in
	// SealingStrategy.UnsealRootKey's contract forbids an implementation from
	// returning partial material ALONGSIDE an error, and a backstop registered
	// after the check would step over exactly that case.
	//
	// It replaces two of the five hand-written wipes this used to rely on. The
	// one that mattered lived INSIDE the borrow at the bottom:
	// MemoryEnclave.WithBytes returns ErrEnclaveDestroyed BEFORE it calls fn
	// when the enclave is nil, destroyed OR wiped, so on that path the key was
	// never wiped and survived to the collector (#95).
	//
	// That path is REACHABLE IN PRODUCTION, which an earlier draft of this
	// comment denied. The (nil, nil) shape needs the lockRootKey seam, true —
	// but enclave.NewMemoryEnclaveSized documents that a successful return can
	// hand back an ALREADY-DESTROYED enclave when enclave.WipeAll runs between
	// publication and return, and WipeAll is a shutdown sweep belonging to
	// whoever owns the process (docs/memory-enclave/architecture.md). A consumer
	// sweeping locked memory while a late Unseal is in flight is an ordinary
	// shutdown race, not a contrived one.
	//
	// The three eager wipes below STAY. This defer is a floor, not a
	// replacement: it fires at return, and each of those three has real work —
	// a persister call, a consumer's slog handler, or the whole of
	// unsealWithRootKey — standing between the point the key stops being needed
	// and the point this fires. Wipe is idempotent, so the pair never conflicts:
	// the second zeroes zeroes. Where a return is IMMEDIATE the eager wipe is
	// redundant and is gone.
	defer enclave.Wipe(heapKey)
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
		hwEnc, hwErr := strategy.BarrierEncryptor(ctx, heapKey)
		if hwErr != nil {
			return hwErr
		}
		// Eager, because the defer does not fire until after the ten field
		// assignments, the persister call and the two slog lines below — and
		// slog handlers are the CONSUMER's code, which may do I/O. NoneStrategy
		// returns an empty key so there is nothing here to erase, but this
		// branch is gated on strategy.ID() precisely so a consumer's own
		// no-encryption strategy can reach it, and that one's key is its own.
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

	if len(heapKey) != rootKeySize {
		// Eager for the same reason: persistBackoffState reaches the embedder's
		// StatePersister, and a wrong-sized key is still key material.
		enclave.Wipe(heapKey)
		b.failedAttempts++
		b.lastFailedAt = b.timeNow()
		b.persistBackoffState()
		return ErrInvalidRootKeySize
	}

	// THE STRATEGY'S HEAP COPY DIES HERE, at the earliest point it can — the
	// defer above is only the floor under it.
	//
	// SealingStrategy.UnsealRootKey returns an ordinary []byte and stays that way
	// (this issue's scope note is explicit that the interface does not move), so
	// the root key does reach the heap on this path — unlike Initialize, which
	// generates straight into locked memory. What this controls is how LONG it
	// stays there: it is copied into locked pages and wiped immediately, instead
	// of living across MAC verification, encryptor resolution, the load-generation
	// claim and one DEK derivation per known epoch, guarded by nine hand-written
	// wipes on the error paths in between.
	//
	// Sized rather than from-bytes for the same reason as Initialize: the pages
	// must stay writable so the terminal enclave.NewSealed can wipe them as it
	// consumes them. MemoryEnclave.Freeze exists now and is deliberately NOT used
	// here — a wipe of frozen pages faults the process.
	rootEnc, encErr := b.lockRootKey(rootKeySize)
	if encErr != nil {
		return &Error{Strategy: "unseal", Err: encErr}
	}
	defer rootEnc.Destroy()

	return rootEnc.WithBytes(func(rootKey []byte) error {
		copy(rootKey, heapKey)
		enclave.Wipe(heapKey)
		return b.unsealWithRootKey(ctx, strategy, rootKey)
	})
}

// unsealWithRootKey is the rest of Unseal, running with b.mu held and with
// rootKey aliasing the locked pages of the enclave its caller borrowed.
//
// Same contract as initializeWithRootKey: it must not retain rootKey past its
// own return, and must not destroy or re-borrow the enclave, which the caller
// owns. creds is not threaded through because nothing below this point needs it
// — the credential was already spent by UnsealRootKey.
//
// There is deliberately no enclave.Wipe(rootKey) below. The borrow's lifetime is
// the wipe.
func (b *Barrier) unsealWithRootKey(ctx context.Context, strategy SealingStrategy, rootKey []byte) error {
	// Authenticate the persisted metadata BEFORE deriving a single DEK from it.
	// The passphrase (or HSM) has just proved the ciphertext is genuine; this
	// proves that the Epoch, KnownEpochs, Salt, HKDFVersion and Algorithm
	// standing next to it belong to that ciphertext and were not edited by an
	// attacker with write access to the blob. It is deliberately NOT counted as
	// a failed unseal attempt: the credential was correct, the STATE is not,
	// and driving the brute-force backoff from a file an attacker can write
	// would hand them a lockout primitive.
	if err := b.verifySealedStateMAC(rootKey); err != nil {
		return err
	}

	// Check if strategy provides a hardware encryptor.
	hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
	if hwErr != nil {
		return hwErr
	}

	// Resolve the context-binding capability BEFORE any state transition; a
	// deployment that requires positional binding must be refused at setup.
	binding := contextBindingFor(hwEnc, false, b.randReader)
	if err := b.checkContextAADPolicy(binding); err != nil {
		return err
	}
	b.binding = binding
	b.strategyID = strategy.ID()
	b.strategyHardwareBacked = strategy.HardwareBacked()
	b.plaintextMode = false

	if hwEnc != nil {
		// Hardware mode: skip DEK re-derivation, use hardware encryptor.
		//
		// Sealed before the first state assignment, so a refused allocation leaves
		// the barrier SEALED rather than half-unsealed. rootKey is consumed on
		// every path, failure included.
		rootPK, sealErr := enclave.NewSealed(rootKey)
		if sealErr != nil {
			return &Error{Strategy: "unseal", Err: sealErr}
		}
		b.hwEncryptor = hwEnc
		b.rootKey = rootPK
		b.state = barrierUnsealed
		b.unsealed.Store(true)
		b.failedAttempts = 0
		b.lastFailedAt = time.Time{}
		b.persistBackoffState()
		b.logDelegatedMode("unseal", binding)
		slog.Info("barrier unsealed (hardware mode)", "epoch", b.epoch, "context_binding", binding.String())
		return nil
	}

	// CLAIM THE BLOB before deriving or installing anything. Ordering is the
	// whole point: the claim must be durable BEFORE this barrier can encrypt,
	// or a process that dies between encrypting and persisting would leave a
	// generation on disk that the next loader increments without ever learning
	// that a DEK was already used under it. A claim that cannot be persisted
	// aborts the unseal with the barrier still sealed.
	if err := b.claimLoadGenerationLocked(rootKey); err != nil {
		return err
	}

	// Derive DEK and cipher for the current epoch.
	dek, aead, err := b.deriveEpochCipher(rootKey, b.salt, b.epoch)
	if err != nil {
		return err
	}

	// Re-derive DEKs for all known epochs (survives seal/unseal cycles).
	// The current epoch is skipped: its DEK/cipher were already derived
	// above and are installed below. Deriving it again here would seal a
	// second enclave that the newDEKs[b.epoch] assignment silently
	// overwrites, leaving an unreachable duplicate of the current epoch's
	// DEK behind on every unseal.
	//
	// Every sealed enclave built in this function is UNREACHABLE by the caller
	// until the commit below installs it, so any early return has to tear down
	// what it already built or leak it for the life of the process. failed() is
	// that teardown, and it is the whole of it: the plain per-epoch DEKs need no
	// wipe of their own because enclave.NewSealed consumes each one on every
	// path. The one plain slice that outlives a failure is `dek`, derived before
	// this loop and not yet sealed, which is why the two returns below wipe it
	// by hand.
	newDEKs := make(map[uint64]*enclave.SealedEnclave)
	newCiphers := make(map[uint64]cipher.AEAD)
	failed := func(err error) error {
		for _, pk := range newDEKs {
			pk.Destroy()
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
		pk, serr := enclave.NewSealed(d)
		if serr != nil {
			enclave.Wipe(dek)
			return failed(&Error{Strategy: "unseal", Err: serr})
		}
		newDEKs[epoch] = pk
		newCiphers[epoch] = a
	}

	// Seal the current epoch's DEK and then the root key, in that order and
	// both BEFORE the first state assignment: the root key must stay in the
	// clear until every derivation above has read it, and nothing below may be
	// able to fail once the barrier starts calling itself unsealed.
	dekPK, err := enclave.NewSealed(dek)
	if err != nil {
		return failed(&Error{Strategy: "unseal", Err: err})
	}

	// Install the current epoch (skipped by the loop above; this also
	// covers the case where b.epoch is not in knownEpochs).
	newDEKs[b.epoch] = dekPK
	newCiphers[b.epoch] = aead

	rootPK, err := enclave.NewSealed(rootKey)
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
	b.unsealed.Store(true)
	b.failedAttempts = 0
	b.lastFailedAt = time.Time{}
	b.persistBackoffState()

	// Enforce the NIST SP 800-38D invocation budget ACROSS process lifetimes.
	// See resolveInvocationBudget: the loaded epoch's spent budget is
	// unknowable, so the barrier either moves to a fresh epoch or refuses to
	// encrypt under the loaded one.
	b.resolveInvocationBudget()

	slog.Info("barrier unsealed", "epoch", b.epoch, "algorithm", b.config.Algorithm.String())
	return nil
}

// claimLoadGenerationLocked takes ownership of the sealed blob by writing back
// LoadGeneration + 1, and is what lets a later loader discover that this
// barrier was here. The caller must hold b.mu and must pass the plaintext root
// key, which is needed to re-stamp the metadata MAC over the new generation.
//
// WHAT THE FENCE DETECTS. Two barriers loaded from one blob derive the same
// epoch DEK with two zeroed invocation counters (see resolveInvocationBudget),
// and nothing about that is visible from inside either process. After this
// claim it is visible in the one place both processes share: the blob. A
// barrier handed a blob whose generation exceeds its own claim knows another
// loader has taken the state and fails closed — the silently shared DEK becomes
// a loud, attributable error.
//
// WHAT IT DOES NOT DETECT, stated plainly rather than implied away:
//
//   - It is not continuous. The generation is compared when state is LOADED
//     (SetSealedRootKey) and enforced when state is WRITTEN (Rotate,
//     PurgeEpochsBefore) or USED (the Encrypt family). The barrier never
//     re-reads the persisted blob on its own — that would put a disk read on
//     the encrypt hot path — so a barrier superseded while running keeps
//     encrypting until the embedder hands it the newer state.
//   - It is not a lock. Two loaders that read the SAME generation before either
//     writes both claim the same number and neither can tell. Detection lands
//     on whichever loader reads after the other has written, which is the
//     common case (a second process starting later) but not a guarantee. Only
//     an exclusive lock or a compare-and-swap persister makes it one.
//   - It is inert where there is no epoch DEK to duplicate: no-encryption
//     (StrategyNone) and delegated/hardware mode both return from Unseal before
//     this point, because the delegated encryptor owns its own key and nonce
//     management, exactly as it owns the invocation counter and nonce tracker.
//   - It is inert without a StatePersister: a claim that cannot be written down
//     is not a claim. That deployment already cannot rotate, and Unseal already
//     refuses to encrypt under an unprovable budget, so the fence adds nothing
//     it did not already have — but an embedder that injects its own
//     InvocationCounter and no persister keeps encrypting with no fence at all,
//     which is why this is reported at WARN rather than passed over in silence.
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

// resolveInvocationBudget re-establishes a SOUND NIST SP 800-38D invocation
// budget after an unseal whose epoch this process did not create. The caller
// must hold b.mu.
//
// The problem it solves: with random 96-bit nonces the 2^32 invocations per DEK
// is a hard nonce-collision bound (probability below 2^-32), not advice, and the
// default in-memory counter does not survive a restart. After loading a
// persisted blob the count already spent under that epoch is not merely unknown
// — it is UNKNOWABLE, because nothing on disk records it.
//
// The previous behaviour seeded the counter to the rotation threshold (95%),
// leaving 5% — over 2x10^8 encryptions — of headroom "until rotation
// completes". That headroom was re-granted in full on EVERY restart against the
// SAME DEK, so ~20 restarts of a deployment that never rotates (and one with no
// StatePersister provably cannot rotate: Rotate fails closed with
// ErrNoStatePersister) push the DEK past 2^32 invocations. A GCM nonce collision
// there yields keystream XOR and recovery of the GHASH authentication key, i.e.
// arbitrary forgery of at-rest records.
//
// An unprovable budget therefore buys nothing, and the barrier takes one of two
// sound paths instead:
//
//   - A fresh epoch, when it can be made durable. Rotating derives a new DEK
//     whose count genuinely starts at zero, which is the only way to keep
//     encrypting with a bound anyone can defend. The rotation is performed HERE,
//     inside Unseal and under the same write lock, rather than left to the
//     embedder's rotation loop: a loop that reacts a tick later leaves a window
//     in which writes land on the exhausted DEK, and an embedder without such a
//     loop leaves it open forever.
//   - Otherwise, fail closed. The counter is seeded to the FULL limit, so every
//     Encrypt returns ErrInvocationLimitExceeded until the epoch is rotated,
//     while Decrypt keeps working — a node with no persister can still read and
//     serve its data, it just cannot add to a key whose remaining budget it
//     cannot prove.
//
// WHAT THE FRESH EPOCH ASSUMES — SINGLE-WRITER OWNERSHIP OF THE BLOB. The
// rotation above yields a DEK whose count starts at a provable zero for THIS
// process. It says nothing about any other process. Every input to
// deriveDEK — root key, salt, deployment ID, epoch — comes from the loaded
// blob, and the epoch it rotates to is a pure function of the loaded one
// (epoch+1), so TWO barriers that load the same blob derive the SAME new DEK,
// each with a zeroed invocation counter and an empty nonce tracker. That is the
// duplicate-key-with-reset-counter condition the whole invocation cap exists to
// prevent: n writers each spend up to the full 2^32 budget against one key,
// and neither tracker can see the other's nonces.
//
// The LOAD-GENERATION FENCE is what makes that state detectable rather than
// silent: each Unseal claims the blob by writing back LoadGeneration + 1, and a
// barrier that is later handed a blob naming a higher generation fails closed
// with ErrBarrierSuperseded. See claimLoadGenerationLocked for what the fence
// detects and — importantly — what it does not: it is checked at load and
// enforced at write/encrypt, never polled, so it turns a silently shared DEK
// into a loud failure at the next load rather than at the instant of sharing.
//
// The fence does not make the barrier self-sufficient. Two loaders that read
// the same generation before either writes both claim the same number.
// BarrierConfig.MinEpoch does not close that either — it is a floor against
// REPLAY of an older blob, and this hazard needs no rollback at all — and
// StateMAC cannot, because both copies are authentic. Exclusive ownership of
// the blob remains the embedder's to guarantee; see the StatePersister
// contract in barrier_config.go.
func (b *Barrier) resolveInvocationBudget() {
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
		newEpoch, err := b.rotateLocked()
		if err == nil {
			// "known-zero for this process" is the accurate claim, and the
			// qualifier is not pedantry: the rotated-to epoch is a pure function
			// of the loaded blob, so a second process loading the same blob
			// derives this very DEK and also starts from zero. Naming the
			// assumption in the log is what makes an operator who is running two
			// writers over one data directory able to recognise it here.
			slog.Warn("AEAD invocation count for the loaded epoch is unrecoverable; "+
				"rotated to a fresh epoch whose count is zero FOR THIS PROCESS (sound only "+
				"if this process is the sole writer of the sealed blob)",
				"from_epoch", newEpoch-1,
				"epoch", newEpoch,
				"limit", limit,
				"requires", "exclusive ownership of the persisted sealed root key",
			)
			// This forced rotation is what grows KnownEpochs by one PER
			// RESTART, and SetSealedRootKey refuses to load a blob naming more
			// than MaxKnownEpochs. Nothing in this package reconciles the two:
			// the bound exists as a DoS guard against a forged blob, and it
			// also caps the barrier's own legitimate growth.
			//
			// An embedder that has not wired up PurgeEpochsBefore — or one in
			// a crash loop, which reaches 4096 in minutes rather than years —
			// therefore walks into a node that cannot start at all, with data
			// that needs bespoke tooling to recover (unwrap the root key, trim
			// KnownEpochs, re-stamp the MAC). Warn while there is still room
			// to act rather than at the point of failure.
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

	// Destroy every live key enclave before dropping the map/field references.
	//
	// WHAT THIS BUYS CHANGED WITH THE TYPE, and it changed for the better.
	// pkg/enclave's ProtectedKey could only Clear — drop the reference, so the
	// key could no longer be OPENED — because memguard.Enclave exposed no API to
	// zero its ciphertext and registered no finalizer. The encrypted DEK bytes
	// therefore stayed resident until the collector reclaimed them, and seal()
	// could promise revocation but not erasure.
	//
	// enclave.SealedEnclave.Destroy WIPES the ciphertext and the wrapped-key blob
	// before releasing them, under the value's write lock, waiting out any Open
	// in flight. So after seal() the sealed copy of every key is gone rather than
	// merely unreachable. Destroy is idempotent and safe on nil, and currentDEK
	// is also present in b.deks — destroying it twice is deliberate and free.
	//
	// It does NOT touch an enclave a previous Open handed out: those belong to
	// their callers, which Destroy them on their own paths.
	b.rootKey.Destroy()
	b.currentDEK.Destroy()
	for epoch, dek := range b.deks {
		dek.Destroy()
		delete(b.deks, epoch)
	}

	b.rootKey = nil
	b.currentDEK = nil
	b.cipher = nil
	b.deks = make(map[uint64]*enclave.SealedEnclave)
	// Caveat, and it is now the ONLY one: each cipher.AEAD below holds a COPY of
	// its DEK inside its internal key schedule (AES expanded key / ChaCha key).
	// Go's stdlib and x/crypto AEADs expose no key-wipe API, so dropping these
	// references cannot zero that copy — it lingers in freed heap until GC
	// reclaims it. The sealed copies above ARE zeroed now (see the Destroy calls),
	// so what used to be two GC-bounded copies is one.
	b.ciphers = make(map[uint64]cipher.AEAD)
	b.hwEncryptor = nil
	// The encryptor is gone, so the capability it carried is gone with it. A
	// subsequent Unseal re-derives all three from the strategy it is given.
	b.binding = ContextBindingUnresolved
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

// Reset returns a sealed barrier to the uninitialized state, so that
// Initialize may be called on it again. It exists to roll back a first-boot
// initialization that the embedder could not complete — for example one whose
// root-key shares were never distributed — without replacing the barrier
// instance, which the WAL and the host already hold.
//
// Reset succeeds only from the sealed state, and only for a FIRST BOOT: a root
// key this process created with Initialize, onto which no blob has since been
// loaded with SetSealedRootKey and whose epoch has never advanced past the
// first (no Rotate, and no forced rotation inside Unseal). It returns
// ErrBarrierNotInit on a barrier that is already uninitialized,
// ErrBarrierUnsealed on an unsealed one (Seal it first), ErrBarrierSuperseded
// once another loader has claimed this barrier's sealed state,
// ErrResetNotFirstBoot when the barrier is not an uncompleted first boot, and
// ErrEpochsInFlight while any epoch is pinned by RegisterInFlightEpoch or an
// EncryptWithEpochPinned* call.
//
// The first-boot restriction protects SetSealedRootKey's in-process rollback
// floor. That floor is the epoch the barrier holds, and Reset discards it; on a
// barrier that had reached epoch N, a Reset would let an older authentic blob
// load and Unseal's forced rotation re-derive an epoch below N whose DEK was
// already used — with its invocation counter and nonce tracker freshly reset,
// which is exactly the nonce and DEK reuse those trackers exist to prevent. A
// first boot never derived any epoch but the first, so it has no such epoch to
// re-derive. The flag is cleared by Reset itself, so a successful Reset re-arms
// nothing until the next Initialize.
//
// The superseded refusal keeps that condition terminal for the instance, as
// ErrBarrierSuperseded documents: a Reset followed by Initialize must not
// become a way to shed it in-process.
//
// Everything the barrier knew about its root key is discarded: the sealed blob
// held in memory, the salt, the epoch and the known-epoch set, every DEK and
// AEAD, the claimed load generation and the brute-force backoff counters. Any
// key enclave still held is Destroyed (wiped, not merely dropped) exactly as
// seal() does. The AEAD invocation counter is Reset and the nonce tracker
// Cleared, since their counts describe a key that no longer exists here; as in
// Rotate, a failure of either is logged and is fail-safe — a stale count only
// forces an earlier rotation under the next key, and a stale nonce set only
// yields conservative rejections.
//
// Reset does NOT touch anything persisted. A blob already written through
// BarrierConfig.StatePersister, or saved by the caller from SealedRootKeyData,
// still exists and still names the discarded key; removing or replacing it is
// the caller's responsibility. Data encrypted under the discarded root key is
// unrecoverable once its sealed blob is gone, and is never decryptable under a
// root key produced by a later Initialize.
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
	b.rootKey.Destroy()
	b.currentDEK.Destroy()
	for _, dek := range b.deks {
		dek.Destroy()
	}

	b.rootKey = nil
	b.currentDEK = nil
	b.cipher = nil
	b.deks = make(map[uint64]*enclave.SealedEnclave)
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
	b.unsealed.Store(false)

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
// A plaintext longer than the algorithm's per-invocation ceiling
// (SymmetricAlgorithm.MaxPlaintextSize) is refused with ErrPlaintextTooLarge
// rather than passed to a cipher that would panic on it. The bound applies to
// the barrier's own AEAD path only; in delegated (hardware) mode the encryptor
// owns its own limits, as it owns its own key and nonce management.
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
// SegmentID||RecordSeq or a snapshot's SnapshotIndex||ChunkSeq), authentication
// fails unless the exact same context is supplied at decrypt time.
//
// WHAT THAT DOES AND DOES NOT DELIVER. It is a per-frame binding, so it defeats
// REORDERING (frame k opened at position j fails) and SPLICING (a frame from
// another artifact, shard or index fails). It does NOT defeat TAIL TRUNCATION:
// nothing in the AAD names the number of frames or the total length, so every
// surviving frame of a shortened artifact still authenticates perfectly and a
// reader that stops at EOF sees a complete, shorter artifact. Detecting a
// removed tail requires a count or a terminal marker that the reader can check
// against, and only the caller — which owns the frame loop and the artifact
// header — is in a position to supply one: fold the total frame count into
// every frame's context, or distinguish the final frame's context from all
// others, so a truncated artifact fails to authenticate rather than merely
// ending early.
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
	// The load-generation fence. It sits here, at the single choke point every
	// Encrypt variant funnels through, rather than being repeated at six public
	// entry points — and it costs one atomic load, no lock and no disk read, so
	// the hot path is unchanged in shape. A superseded barrier must not add to
	// an epoch DEK another loader is also using; see ErrBarrierSuperseded.
	//
	// Delegated (hardware) mode never reaches this function, and is out of the
	// fence's scope for the same reason it is out of the invocation counter's
	// and the nonce tracker's: the delegated encryptor owns its own key.
	if b.superseded.Load() {
		return nil, ErrBarrierSuperseded
	}

	// Bound the frame BEFORE allocating or sealing. Both GCM and
	// ChaCha20-Poly1305 answer an over-long plaintext with a panic rather than
	// an error (see SymmetricAlgorithm.MaxPlaintextSize), so without this check
	// an oversized frame takes down the embedding process from inside
	// crypto/cipher — a library killing its host over an input it could have
	// rejected. Checking first also avoids allocating the output buffer for a
	// message that can never be sealed.
	if uint64(len(plaintext)) > b.maxPlaintext {
		return nil, ErrPlaintextTooLarge
	}

	// wireNonceSize is how many nonce bytes THIS FRAME carries ahead of the AEAD
	// payload, which is zero for a module-nonce AEAD: cipher.NewGCMWithRandomNonce
	// draws the nonce inside the FIPS module boundary and prepends it to its own
	// Seal output, reporting NonceSize() == 0 and counting those bytes in
	// Overhead() instead. The resulting frame is byte-for-byte what the
	// caller-nonce path below produces, so totalSize, the header, and every
	// stored blob are unchanged; what changed is who generates the nonce, which
	// is precisely what SP 800-38D requires. See newAESGCM.
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

	// Encrypt in place after the epoch+algID (+nonce, when the caller owns it)
	// prefix. nonce is nil for a module-nonce AEAD, which REQUIRES an empty
	// nonce argument and panics on any other.
	// G407 false positive: nonce is either nil or randomly generated above via
	// io.ReadFull(b.randReader, nonce).
	ciphertextStart := payloadStart + wireNonceSize
	aead.Seal(out[ciphertextStart:ciphertextStart], nonce, plaintext, aad) // #nosec G407 -- nonce is random, not hardcoded

	// A module-nonce AEAD only reveals its nonce after the fact, as the leading
	// bytes of its own output, so reuse detection runs here instead of before
	// the Seal. The check is still fail-closed — a reused nonce returns the
	// error and the ciphertext never reaches the caller — but it can no longer
	// prevent the AEAD invocation that produced it, only refuse to hand it out.
	// That is the correct trade: the alternative is a caller-chosen IV, which is
	// the violation this construction exists to remove.
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

	// wireNonceSize is how many nonce bytes precede the AEAD payload in the
	// frame: the full nonce for a caller-nonce AEAD, and none for a module-nonce
	// AEAD (cipher.NewGCMWithRandomNonce), whose Open consumes the prepended
	// nonce out of the payload itself. Both spellings describe the SAME bytes in
	// the same positions, which is why a frame written before this migration and
	// a frame written after it are indistinguishable on disk and each is
	// readable by the other's reader.
	//
	// activeNonceSize is the nonce WIDTH of the cipher in force, which the
	// module-nonce AEAD cannot report (it accepts no caller nonce and answers
	// zero), so the barrier's CONFIGURED algorithm fixes it instead — the header
	// byte cannot be trusted to, and is compared against it below.
	wireNonceSize := aead.NonceSize()
	activeNonceSize := wireNonceSize
	if activeNonceSize == 0 {
		activeNonceSize = b.config.Algorithm.NonceSize()
	}

	// alg comes from the UNAUTHENTICATED algID header byte and never selects the
	// cipher (the active AEAD is fixed by the authenticated epoch DEK). A tampered
	// or corrupted algID that names a different nonce-size class (e.g. 24-byte
	// XChaCha20 against a 12-byte AES-GCM cipher) would slice a wrong-length nonce
	// and PANIC inside aead.Open — both crypto/cipher GCM and chacha20poly1305
	// panic on a wrong nonce length. The pre-Open length gate below uses this same
	// nonceSize, so it cannot catch the mismatch. Reject any algID whose nonce
	// length disagrees with the active cipher, failing closed with the generic
	// error rather than crashing the process on corrupt WAL/snapshot input.
	//
	// A FIPS build reaches this with nonceSize == 0 for any ChaCha20-family
	// algID, because that family is not registered there — the same fail-closed
	// rejection, arrived at by the algorithm being absent from the binary.
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

	// Bound the payload for the same reason encryptLocked bounds the plaintext,
	// and with more urgency: this input comes off disk or the wire. x/crypto's
	// ChaCha20-Poly1305 Open PANICS on an over-long ciphertext (GCM's Open
	// returns an error), so an oversized frame would crash the process rather
	// than fail authentication. Rejected with the generic ErrDecryptionFailed
	// like every other decrypt failure, so the length reveals nothing the caller
	// could use as an oracle.
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
	return b.rotateLocked()
}

// rotateLocked is Rotate's body. It is factored out because Unseal must also be
// able to rotate — see resolveInvocationBudget — while already holding b.mu.
// The caller must hold the exclusive write lock.
func (b *Barrier) rotateLocked() (uint64, error) {
	if b.state != barrierUnsealed {
		return 0, ErrBarrierSealed
	}

	// A superseded barrier must not WRITE the blob either. Rotating would
	// persist this instance's state — including its stale load generation —
	// over the state of the loader that superseded it, erasing that loader's
	// claim and with it the evidence either instance could act on.
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

	// Step 1: derive the new DEK and cipher. Open the root key enclave
	// temporarily for HKDF derivation.
	//
	// The Destroy is DEFERRED rather than placed after the derivation call:
	// deriveEpochCipher runs the caller-injected BarrierConfig.AEADProvider,
	// i.e. arbitrary third-party code, on the plaintext root key. A panic there
	// that some caller recovers would otherwise strand an mlock'd buffer holding
	// the root key in the clear for the life of the process — a whole locked page
	// that is never released and never re-readable, charged against the process's
	// RLIMIT_MEMLOCK for as long as it runs. Under memguard the leak was worse
	// still: a later lock the kernel refused TERMINATED THE PROCESS. Since the
	// go-enclave adoption that refusal is a returned ErrLockedMemoryUnavailable on
	// exactly the allocation that hit the ceiling, which lowers the stakes without
	// making a stranded root key acceptable.
	rootBuf, err := b.rootKey.Open()
	if err != nil {
		return 0, err
	}
	defer rootBuf.Destroy()
	// Aliases the locked pages for the rest of this function. rootBuf is opened
	// and destroyed here and never escapes, so no concurrent Destroy can unmap it
	// underneath the reads below — the condition go-enclave's Open requires.
	rootBytes, err := rootBuf.Open()
	if err != nil {
		return 0, err
	}
	dek, aead, err := b.deriveEpochCipher(rootBytes, b.salt, newEpoch)
	if err != nil {
		// Nothing committed yet; the old epoch remains active. deriveEpochCipher
		// already zeroed any partial key material on its own error path, but the
		// returned dek (if any) is wiped here for defense in depth.
		enclave.Wipe(dek)
		return 0, err
	}

	// Step 1b: seal the new DEK BEFORE the state is persisted, so that step 3
	// stays what its comment claims — a run of INFALLIBLE in-memory mutations.
	//
	// The ordering is load-bearing rather than tidy. Sealing allocates locked
	// pages transiently and can be refused by the kernel, and a refusal after
	// the persist would leave the durable blob describing an epoch this barrier
	// never adopted: the old epoch keeps encrypting under a generation the disk
	// says has moved on. Doing it here makes a refused allocation abort the
	// rotation with nothing written, which is the same outcome as a failed
	// persist and the one a caller already knows how to handle.
	//
	// It consumes dek on every path, which is why the two error paths below no
	// longer wipe it by hand and destroy the sealed enclave instead.
	dekPK, sealErr := enclave.NewSealed(dek)
	if sealErr != nil {
		return 0, &Error{Strategy: "rotate", Err: sealErr}
	}

	// Step 2: persist the new sealed state BEFORE any of it becomes active.
	// The candidate is a private deep copy, so a failed persist leaves the
	// live state byte-for-byte unchanged; the persister gets its own copy it
	// may retain. It runs with b.mu held and must not re-enter the barrier.
	candidate := cloneSealedRootKey(b.sealed)
	candidate.Epoch = newEpoch
	candidate.KnownEpochs = sortedEpochsWith(b.knownEpochs, newEpoch)
	// Re-authenticate the metadata: Epoch and KnownEpochs both changed, and a
	// blob whose MAC still covered the OLD values would be rejected by the next
	// Unseal (correctly — it would be indistinguishable from an edited one).
	if macErr := b.restampStateMACLocked(rootBytes, candidate); macErr != nil {
		dekPK.Destroy()
		return 0, macErr
	}
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		dekPK.Destroy()
		slog.Error("key rotation aborted: sealed state not persisted; previous epoch remains active",
			"from_epoch", oldEpoch, "to_epoch", newEpoch, "error", perr)
		return 0, &StatePersistError{Op: "rotate", Epoch: newEpoch, Err: perr}
	}

	// Step 3: commit the epoch switch — in-memory, infallible state mutations
	// under the held write lock. After this point the NEW key is active and the
	// rotation has succeeded; the trackers below are reset against the new epoch.
	b.epoch = newEpoch
	// An epoch above the first now exists; Reset may no longer drop the floor.
	b.firstBoot = false
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks[newEpoch] = dekPK
	b.ciphers[newEpoch] = aead
	b.knownEpochs[newEpoch] = struct{}{}

	// Adopt the state that was just persisted, so SealedRootKeyData() and the
	// durable blob agree exactly.
	b.sealed = candidate

	// The new DEK has performed zero invocations and this process knows it, so
	// the counter reset below is a TRUE count again — including for an epoch
	// that was loaded from disk a moment ago (see resolveInvocationBudget).
	b.invocationsAuthoritative = true

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

	// Purging writes the reduced state through the persister, so a superseded
	// barrier is refused here for the same reason as in rotateLocked — and with
	// an extra edge: it would retire DEKs on behalf of a blob it no longer
	// owns, for epochs the current owner may still be reading.
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
	if macErr := b.restampStateMACFromEnclaveLocked(candidate); macErr != nil {
		return 0, macErr
	}
	if perr := persist(cloneSealedRootKey(candidate)); perr != nil {
		slog.Error("epoch purge aborted: reduced sealed state not persisted; DEKs retained",
			"min_epoch", minEpoch, "error", perr)
		return 0, &StatePersistError{Op: "purge", Epoch: b.epoch, Err: perr}
	}

	for _, epoch := range purgeable {
		// Destroy the purged epoch's key enclave and drop its AEAD.
		//
		// What this guarantees, stated the same way seal() states it:
		// SealedEnclave.Destroy WIPES the epoch's sealed DEK, so that copy is
		// gone rather than merely unreachable — which is what changed when
		// ProtectedKey (memguard, no way to zero an Enclave's ciphertext) was
		// replaced. Deleting b.ciphers[epoch] is still only a reference drop:
		// the cipher.AEAD holds a verbatim copy of the DEK in its key schedule
		// (x/crypto's ChaCha20-Poly1305 keeps `key [32]byte`; AES-GCM keeps the
		// expanded round keys, from which the key is recoverable), and neither
		// stdlib nor x/crypto offers a key-wipe API.
		//
		// So purge is still a CAPABILITY revocation rather than crypto-shredding:
		// after it returns the barrier can no longer decrypt the epoch, but DEK
		// confidentiality against memory disclosure (a core dump, a live process
		// read) remains GC-bounded through the cipher cache. Restructuring that
		// cache to hold wipeable key material is a hot-path change and is
		// deliberately not made here.
		b.deks[epoch].Destroy()
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

// DeregisterInFlightEpoch releases one reference taken by
// RegisterInFlightEpoch (or by one of the EncryptWithEpochPinned* methods).
// When the last reference is released the entry is removed and the epoch
// becomes purgeable again.
//
// A call for an epoch that holds no reference changes nothing and is reported
// at ERROR level. It is a caller bug — releases must match Registers one for
// one — and the consequence of getting it wrong is severe and remote from its
// cause: one release too many drops a pin some other writer is relying on, the
// next PurgeEpochsBefore retires that epoch's DEK, and the snapshot or WAL
// segment holding the frame sealed under it becomes permanently undecryptable
// with nothing in the failure pointing back at the extra release.
//
// The guard is honest about its reach. It enforces the invariant that IS
// decidable here — the count never goes below zero, and an epoch nobody holds is
// never "released" — and it makes the caller bug loud at the moment it happens.
// It CANNOT repair a duplicate release that arrives while the count is still
// positive: from the barrier's side that call is indistinguishable from the
// legitimate one, and no reference count can tell them apart. Preventing that
// requires a single-use release handle rather than a bare epoch number, which is
// a public API change and is deliberately not made here. The previous
// unconditional decrement additionally erased its own underflow (`<= 0` deleted
// the entry), so the caller bug left no trace at all.
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
//
// The copy lands on the ORDINARY Go heap: it is not mlock'd, so it can be
// swapped to disk and it appears verbatim in a core dump, and a caller that
// forgets to zero it leaves the key resident until the GC reclaims the backing
// array. Prefer RootKeyProtected, which hands back the same material inside a
// memory-locked, guard-paged buffer. This form stays for callers that must pass
// a plain []byte to an API they do not control.
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
	// Deferred rather than sequential: the copy below cannot panic today, but a
	// stranded enclave holds the PLAINTEXT root key mlock'd for the life of the
	// process and consumes RLIMIT_MEMLOCK, which then refuses later locks. The
	// cleanup of key material must not depend on the happy path.
	defer buf.Destroy()
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

// RootKeyProtected returns the decrypted root key inside a memory-locked,
// guard-paged MemoryEnclave. The barrier must be unsealed. The caller OWNS
// the enclave and MUST Destroy it when done, which zeroes the memory
// deterministically.
//
// It is the form to prefer over RootKey for anything that keeps the key for
// more than a statement: the returned memory is excluded from core dumps and
// never paged to disk, and Destroy is a real wipe rather than a request to the
// garbage collector. Returns ErrNoRootKeyAvailable in no-encryption mode, which
// holds no root key at all.
//
// The enclave is returned FROZEN: it is a key to read and destroy, not a buffer
// to edit, so a stray write through a slice retained past its borrow faults
// instead of silently rewriting the caller's copy of the key. (Only the
// caller's copy was ever at stake — SealedEnclave.Open decrypts into a fresh,
// independent enclave each time, so the barrier's own root key was never
// reachable through this.) SealedEnclave.Open hands back a
// SIZED enclave, whose pages would otherwise stay writable for its whole
// lifetime — the window MemoryEnclave.Freeze exists to close — and this accessor
// has nothing left to fill. That makes it agree with
// ProtectedSealingStrategy.UnsealRootKeyProtected, the other method in this
// package that hands a caller a root key it owns; the two differed for no reason
// anyone had stated (#95).
//
// A refused freeze destroys the enclave rather than returning it, for the reason
// spelled out at that sibling: handing back a writable enclave from a method
// whose documentation says the contents are final would report a guarantee that
// was not established.
//
// DO NOT WIPE THE RETURNED BYTES YOURSELF. Destroy is the release, and it is
// the only one: a defensive enclave.Wipe or hand-rolled zeroing of the borrowed
// slice before Destroy is a WRITE to a read-only region and faults the process.
// This is the one habit a careful caller is likely to bring to a key it owns,
// and it is the one habit the freeze punishes. pkg/crypto/shamir's
// ProtectedShare.Value carries the same warning for the same reason.
//
// THE TRADE, stated rather than left for a reader to find in go-enclave: a
// frozen region has to be made writable again before Destroy can wipe it, so the
// wipe now sits behind an mprotect the kernel can deny. On a kernel that denies
// it the pages are released still holding the key, and MemoryEnclave.DestroyErr
// reports ErrReleasedWithoutWipe — where an unfrozen region would have been
// overwritten before anything deniable was attempted. Nothing leaks either way,
// since the pages were mlock'd for their whole life and the kernel zeroes an
// anonymous page before reuse; a caller that wants to know its environment is
// denying mprotect calls DestroyErr rather than Destroy. This is the same trade
// UnsealRootKeyProtected and every from-bytes enclave already make.
func (b *Barrier) RootKeyProtected() (*enclave.MemoryEnclave, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != barrierUnsealed {
		return nil, ErrBarrierSealed
	}
	if b.rootKey == nil {
		return nil, ErrNoRootKeyAvailable
	}
	out, err := b.rootKey.Open()
	if err != nil {
		return nil, err
	}
	if ferr := out.Freeze(); ferr != nil {
		// Returned UNWRAPPED, as the Open failure above is, and for the reason
		// NewCredentials gives for enclave.NewSealed's: it already names its own
		// origin — a *LockedMemoryError matching ErrLockedMemoryUnavailable —
		// and a refused mprotect is not a strategy-level failure. Wrapping it in
		// a *seal.Error would print a strategy name over a kernel refusal.
		out.Destroy()
		return nil, ferr
	}
	return out, nil
}

// CurrentEpoch returns the current key rotation epoch. Returns 0 if the
// barrier has not been initialized.
func (b *Barrier) CurrentEpoch() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.epoch
}

// LoadGeneration returns the sealed blob's load generation as CLAIMED by this
// barrier: 1 after Initialize, and (loaded + 1) after an Unseal that could
// persist the claim. Returns 0 for a barrier that has never loaded or created a
// blob, and for one whose deployment cannot claim (no StatePersister), where the
// loaded value is adopted unchanged.
//
// It is exported for operators and embedders: comparing it with the generation
// in the blob on disk is how you tell which live instance owns the sealed state.
// See SealedRootKey.LoadGeneration.
func (b *Barrier) LoadGeneration() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.loadGeneration
}

// IsSuperseded reports whether this barrier has observed a sealed blob claimed
// by another loader. A superseded barrier refuses to encrypt, rotate or purge —
// permanently, and by design — while decryption continues to work; see
// ErrBarrierSuperseded.
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

// sealedStateMACVersion is the version byte of the canonical encoding the MAC
// is computed over. It is the first byte fed to the HMAC, so extending the
// covered field set later produces unrelated tags rather than an ambiguous
// re-parse of the same bytes.
//
// v2 adds LoadGeneration to the covered set. Blobs stamped under v1 do not
// authenticate under v2 — the format is pinned pre-release and changed in
// place, so a pre-fence blob is migrated by loading it once with
// BarrierConfig.AllowUnauthenticatedSealedState and re-persisting, not by a
// compatibility path that would let an attacker choose the older, weaker
// coverage.
const sealedStateMACVersion byte = 2

// sealedStateMAC computes the metadata authentication tag for a sealed root
// key: HMAC-SHA256, keyed by HKDF-SHA256(rootKey, info=sealedStateMACInfo),
// over a canonical encoding of the fields that decide how the barrier will use
// the key.
//
// Every variable-length field is length-prefixed with a fixed 8-byte big-endian
// count, so no concatenation of one field's contents can imitate the boundary
// of another (a Salt ending in what looks like a length header cannot shift the
// KnownEpochs that follow).
//
// COVERED: Strategy, Version, DeploymentID, Epoch, LoadGeneration, Salt,
// KnownEpochs, HKDFVersion, Algorithm, Ciphertext, Metadata, Nonce,
// HardwarePayload. The ciphertext is included even though it authenticates
// itself, so that genuine metadata from one blob cannot be paired with a
// genuine ciphertext from another. Metadata carries the KDF version byte, which
// selects the unwrapping KDF and is otherwise unauthenticated. LoadGeneration
// is covered because the barrier now REFUSES TO ENCRYPT on the strength of it:
// left unauthenticated, inflating one number in a plaintext field would be a
// durable, passphrase-free way to stop a node from writing.
//
// NOT COVERED, deliberately: FailedAttempts and LastFailedUnixNano, which
// persistBackoffState writes after a FAILED unseal — the point at which no root
// key is available to re-MAC with; CreatedAt, ShamirThreshold and ShamirTotal,
// which are higher-layer custody bookkeeping this package neither reads nor
// acts on (go-qrdb owns them and may legitimately rewrite them).
func sealedStateMAC(rootKey []byte, deploymentID uint64, s *SealedRootKey) ([]byte, error) {
	return sealedStateMACAt(sealedStateMACVersion, rootKey, deploymentID, s)
}

// sealedStateMACAt computes the tag under a SPECIFIC covered-field version.
//
// Only the current version is ever WRITTEN. Older versions are computed solely
// to recognise a blob this build has not yet migrated: see
// verifySealedStateMAC, which accepts a valid v1 tag once and immediately
// restamps at the current version.
//
// Verifying an old version is not a downgrade. Forging a v1 tag requires the
// same HKDF-derived MAC key as forging a v2 one, and that key comes from the
// root key the sealing strategy just recovered — an attacker with write access
// to the blob but no root key cannot produce either. What v1 gives up is
// coverage of LoadGeneration, which a pre-fence blob does not carry at all, so
// there is nothing for the older encoding to leave unprotected.
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
	// v1 did not cover LoadGeneration; the field did not exist. Reproducing the
	// v1 encoding exactly is what makes a pre-fence blob recognisable.
	if version >= 2 {
		putUint(s.LoadGeneration)
	}
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

// verifySealedStateMAC checks b.sealed's metadata tag against the root key just
// recovered by the sealing strategy. The caller must hold b.mu and must have
// verified that rootKey is real key material (no-encryption mode has none and
// never reaches here).
//
// A blob with NO tag is a blob written before the barrier authenticated its own
// metadata. Accepting it unconditionally would make the whole mechanism
// optional at the attacker's discretion — strip the field, keep the forgery —
// so it is refused unless the deployment opts in through
// BarrierConfig.AllowUnauthenticatedSealedState.
func (b *Barrier) verifySealedStateMAC(rootKey []byte) error {
	if b.sealed == nil {
		return ErrNilSealedData
	}
	if len(b.sealed.StateMAC) == 0 {
		if b.config.AllowUnauthenticatedSealedState {
			slog.Warn("quicraft/seal: sealed root key carries no metadata MAC; its epoch and known-epoch "+
				"set are unauthenticated and an attacker with write access to the blob can rewrite them",
				"epoch", b.sealed.Epoch,
				"deployment_id", b.config.DeploymentID,
				"remedy", "re-persist SealedRootKeyData() from this process to stamp a MAC, then clear "+
					"BarrierConfig.AllowUnauthenticatedSealedState",
			)
			return nil
		}
		slog.Error("quicraft/seal: unseal refused; sealed root key carries no metadata MAC",
			"epoch", b.sealed.Epoch,
			"deployment_id", b.config.DeploymentID,
			"remedy", "set BarrierConfig.AllowUnauthenticatedSealedState once to migrate a pre-MAC blob",
		)
		return ErrSealedStateUnauthenticated
	}
	want, err := sealedStateMAC(rootKey, b.config.DeploymentID, b.sealed)
	if err != nil {
		return err
	}
	if !hmac.Equal(want, b.sealed.StateMAC) {
		// Before refusing, check whether this is simply a blob written by a
		// build that predates the load-generation fence. Such a blob carries a
		// PRESENT tag computed over the v1 field set, so it fails the
		// comparison above while being entirely authentic — and the
		// AllowUnauthenticatedSealedState hatch does not apply to it, because
		// that hatch is for a blob with NO tag at all. Without this path a
		// pre-fence deployment could never load its own sealed state again.
		//
		// Accepting it is not a downgrade: forging a v1 tag needs the same
		// HKDF-derived MAC key as forging a v2 one, so an attacker with blob
		// write access and no root key cannot produce either. The tag is
		// restamped at the current version by the normal persist path, so a
		// blob is recognised this way exactly once.
		if b.sealed.LoadGeneration == 0 {
			legacy, legacyErr := sealedStateMACAt(1, rootKey, b.config.DeploymentID, b.sealed)
			if legacyErr == nil && hmac.Equal(legacy, b.sealed.StateMAC) {
				slog.Warn("quicraft/seal: sealed root key authenticated under the pre-fence "+
					"metadata encoding; it will be restamped at the current version on the "+
					"next persist",
					"epoch", b.sealed.Epoch,
					"deployment_id", b.config.DeploymentID,
				)
				return nil
			}
		}
		slog.Error("quicraft/seal: unseal refused; sealed root key metadata failed authentication",
			"epoch", b.sealed.Epoch,
			"deployment_id", b.config.DeploymentID,
		)
		return ErrSealedStateUnauthenticated
	}
	return nil
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

// restampStateMACFromEnclaveLocked is restampStateMACLocked for callers that do
// not already hold the root key open. The caller must hold b.mu and the barrier
// must be unsealed in software mode (the delegated paths never reach here: both
// Rotate and PurgeEpochsBefore refuse with ErrHardwareMode).
func (b *Barrier) restampStateMACFromEnclaveLocked(s *SealedRootKey) error {
	if b.rootKey == nil {
		return ErrNoRootKeyAvailable
	}
	rootBuf, err := b.rootKey.Open()
	if err != nil {
		return err
	}
	defer rootBuf.Destroy()
	return rootBuf.WithBytes(func(root []byte) error {
		return b.restampStateMACLocked(root, s)
	})
}

// StampSealedStateMAC re-authenticates a sealed blob's metadata under this
// barrier's root key, and is how an embedder that re-wraps the root key ITSELF
// produces a blob that will survive its own next Unseal.
//
// The barrier stamps the MAC for every operation it performs (Initialize,
// Rotate, PurgeEpochsBefore). It cannot do so for a re-wrap it does not
// perform: a SealingStrategy produces only the root-key ciphertext and its own
// strategy fields, so an embedder that changes a barrier password, rekeys, or
// re-splits a Shamir quorum assembles the new blob outside the barrier and
// hands it back through SetSealedRootKey. Without this method that blob
// carries no StateMAC, and the next Unseal refuses it with
// ErrSealedStateUnauthenticated -- correctly, since an absent MAC is
// indistinguishable from a stripped one.
//
// Call it AFTER every covered field (Epoch, KnownEpochs, Salt, HKDFVersion,
// Algorithm, Strategy) has its final value, and before persisting the blob:
// the MAC covers exactly those fields, so stamping first and editing after
// produces a blob that fails its own authentication.
//
// The barrier must be unsealed, because the root key is the MAC key. A
// StrategyNone deployment keeps no root key and expects no MAC; calling this
// on one returns ErrNoRootKeyAvailable rather than inventing an authenticator
// there is nothing to authenticate with.
func (b *Barrier) StampSealedStateMAC(sealed *SealedRootKey) error {
	if sealed == nil {
		return ErrNilSealedData
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return ErrBarrierSealed
	}
	return b.restampStateMACFromEnclaveLocked(sealed)
}

// StampSealedStateMACWithRootKey is StampSealedStateMAC for a caller that
// holds the plaintext root key but has no unsealed barrier to stamp with.
//
// That is not a loophole around the method's unsealed requirement -- it is the
// import case, which cannot satisfy it. A replica importing a root key
// exported by a leader seals it under its own strategy and persists the blob
// BEFORE any barrier exists to unseal: the persisted blob is what the
// subsequent Unseal reads. Stamping has to happen at assembly time, from the
// key in hand.
//
// The deploymentID must be the one the barrier that will later unseal this
// blob is configured with, because it is covered by the MAC -- a blob stamped
// for one deployment does not authenticate under another, which is the
// property that stops a sealed blob from being replayed into a different
// cluster.
//
// The same ordering rule applies: stamp only once every covered field has its
// final value. See StampSealedStateMAC for the full list.
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

// SetSealedRootKey sets the sealed root key data for subsequent Unseal calls.
// This is used when loading persisted sealed key data during node startup.
//
// Validation ensures that the sealed data is structurally sound:
//   - Epoch must be greater than zero.
//   - Salt must have the expected HKDF salt length (32 bytes).
//   - KnownEpochs must contain the current Epoch, must name no epoch above it,
//     and must hold at most MaxKnownEpochs entries.
//   - Algorithm, when recorded, must match the barrier's configured algorithm.
//   - Epoch must not go BACKWARDS: it must be at least BarrierConfig.MinEpoch
//     and, on a barrier that already holds state, at least the epoch it already
//     holds.
//   - LoadGeneration must not EXCEED this barrier's own claim. This is the
//     observation point of the load-generation fence: a higher generation means
//     another loader has claimed the sealed state since this barrier did, so the
//     barrier marks itself superseded — permanently refusing to encrypt, rotate
//     or purge, while decryption continues — and returns *SupersededError
//     (errors.Is ErrBarrierSuperseded). The loaded state is NOT adopted in that
//     case; the marking happens before the return so a caller that ignores the
//     error is still not left encrypting. A barrier that has never claimed a
//     generation simply adopts the blob's. See SealedRootKey.LoadGeneration and
//     Barrier.claimLoadGenerationLocked for the fence's limits.
//
// The epoch floor is not tidiness either, and it is not made redundant by
// SealedRootKey.StateMAC. The MAC proves the metadata was written by something
// holding the root key; it cannot prove the blob is the LATEST such state, so
// an attacker who keeps a copy of the persisted blob from epoch 2 and restores
// it over epoch 5 presents a perfectly authentic older state. Accepting it
// resurrects epoch 2's already-spent DEK together with a zeroed invocation
// counter and an empty nonce tracker — two independent random-nonce sequences
// under one key, which is exactly the condition the invocation cap bounds.
// Detecting that requires state the attacker cannot roll back in the same step,
// which lives in the embedder (the highest epoch any live WAL segment or
// retained snapshot still references), not here; BarrierConfig.MinEpoch is
// where it is supplied.
//
// The structural bounds are load-bearing, not tidiness: the blob is plaintext,
// unauthenticated metadata (only the root-key ciphertext is protected, and its
// AAD is a constant), so every field here is attacker-controlled under the same
// write-access model the sealing strategies already defend against. Unseal
// derives one HKDF DEK, one AEAD and one mlock'd enclave per KnownEpochs entry,
// so an unbounded set is a durable, passphrase-free denial of service against
// every subsequent start.
//
// The argument is COPIED. The barrier retains the copy and writes backoff state
// into it, so an embedder may reuse or mutate its decode buffer afterwards
// without silently changing the HKDF salt out from under every DEK derivation.
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

	// Reject an epoch that goes backwards. floor is the strongest lower bound
	// available: the embedder's declared minimum live epoch, raised to whatever
	// epoch this barrier already holds — reloading a state older than the one
	// in hand is a rollback whether or not the embedder configured a floor.
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

	// Validate that KnownEpochs contains the current epoch, nothing above it,
	// and no epoch 0. Epochs only advance and Epoch is the highest one reached,
	// so a larger entry is corruption or padding — never a legitimate historical
	// DEK.
	//
	// Epoch 0 is rejected for the same reason sealed.Epoch == 0 is: epochs start
	// at 1 (Initialize) and Rotate only ever increments, so no data was ever
	// sealed under epoch 0 and no DEK for it can be legitimate. Accepting it was
	// not inert. Unseal derives and installs a full epoch-0 DEK, AEAD and
	// mlock'd enclave for it; Unseal's forced rotation then re-persists it
	// through sortedEpochsWith, so a single accepted zero becomes a DURABLE
	// member of the derivation set that every subsequent start pays for and no
	// purge floor can retire (PurgeEpochsBefore drops epochs strictly BELOW
	// minEpoch, and nothing is below 0). It also leaves b.ciphers[0] populated,
	// which is the one epoch value a corrupt or zero-filled ciphertext header
	// decodes to, turning a header that should fail closed on an unknown epoch
	// into one that reaches the AEAD.
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

	// Reject a DEK-derivation layout this build does not implement rather
	// than failing later with an opaque decryption error.
	if sealed.HKDFVersion > currentHKDFVersion {
		return ErrUnsupportedHKDFVersion
	}

	// Reject a data-path algorithm this barrier is not configured for. Without
	// this the mismatch surfaces only as ErrDecryptionFailed on every read,
	// which is indistinguishable from tampering and names nothing the operator
	// could act on. A zero Algorithm is a blob written before the field existed
	// and is accepted as "unrecorded".
	if sealed.Algorithm != 0 && sealed.Algorithm != b.config.Algorithm {
		return &AlgorithmMismatchError{Sealed: sealed.Algorithm, Configured: b.config.Algorithm}
	}

	// THE LOAD-GENERATION FENCE, checked last so that a malformed or foreign
	// blob is rejected on its own merits first and cannot poison a healthy,
	// running barrier with a fabricated generation.
	//
	// A generation above this barrier's own claim means another loader has taken
	// this sealed state since this barrier claimed it. That is the one thing the
	// barrier could never previously see, and it is not survivable: both
	// instances derive the same epoch DEK from the same blob and each believes
	// it owns the whole NIST SP 800-38D budget. The barrier fails closed
	// PERMANENTLY — marking comes before returning the error, because a caller
	// that ignores the error must still not be left encrypting.
	//
	// The loaded state is deliberately NOT adopted: this instance is done, and
	// quietly picking up the winner's epoch would make it a second writer of the
	// winner's DEK, which is the failure being prevented.
	//
	// A barrier that has never claimed a generation (b.loadGeneration == 0 — a
	// fresh instance, or a blob from before the fence existed) has nothing to be
	// superseded from and simply adopts what it is given.
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
	// A generation BELOW this barrier's claim is a rollback of the blob, not a
	// competing claim, and it is the case BarrierConfig.MinEpoch and StateMAC
	// own. Report it and carry on: refusing here would let anyone who can
	// restore an old backup stop a node from writing.
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
	// The root key is now one some earlier lifetime used; Reset may no longer
	// roll it back (see firstBoot).
	b.firstBoot = false

	// The blob describes an epoch some EARLIER process encrypted under, for an
	// unknown number of invocations. Whatever the in-memory counter holds, it is
	// no longer a count of this epoch's usage. See resolveInvocationBudget.
	b.invocationsAuthoritative = false

	// Adopt the blob's generation as the baseline the next Unseal increments.
	// It is the on-disk state that is authoritative here, exactly as it is for
	// KnownEpochs below.
	b.loadGeneration = sealed.LoadGeneration

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
		enclave.Wipe(dek)
		return nil, nil, err
	}
	return dek, aead, nil
}
