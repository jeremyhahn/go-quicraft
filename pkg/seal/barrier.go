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
	"runtime"
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

	// hkdfInfoLen is the total length of the HKDF info:
	// 20 bytes prefix + 8 bytes deploymentID + 8 bytes epoch.
	hkdfInfoLen = 36

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

// barrierState represents the lifecycle state of a Barrier.
type barrierState uint8

const (
	barrierUninitialized barrierState = iota
	barrierSealed
	barrierUnsealed
)

// Barrier provides algorithm-agile authenticated encryption for at-rest data.
// It supports AES-128-GCM, AES-192-GCM, AES-256-GCM, ChaCha20-Poly1305, and
// XChaCha20-Poly1305, selected via BarrierConfig.Algorithm. The algorithm
// identifier is embedded in the ciphertext wire format to enable decryption
// without out-of-band negotiation.
//
// Wire format (v2, algorithm-agile):
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
// ErrHardwareMode.
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

	// hwEncryptor is non-nil when a hardware-backed strategy provides its
	// own symmetric encryptor. In this mode, the barrier delegates all
	// encrypt/decrypt operations to the hardware and skips DEK derivation.
	hwEncryptor SymmetricEncrypter

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

	// Shamir secret sharing support.
	shamirConfig *ShamirConfig
	accumulator  atomic.Value // holds ShareAccumulator
	splitter     ShareSplitter
	accFactory   func(threshold, total int) ShareAccumulator

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
// Panics if config.Algorithm is not a valid algorithm (programmer error).
func NewBarrier(config BarrierConfig) *Barrier {
	if err := config.validate(); err != nil {
		panic("seal.NewBarrier: " + err.Error())
	}

	var ic InvocationCounter
	if config.InvocationCounter != nil {
		ic = config.InvocationCounter
	} else {
		ic = NewInvocationCounter(true, config.InvocationLimit, config.RotationThreshold)
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

	// Check if strategy provides a hardware encryptor.
	hwEnc, hwErr := strategy.BarrierEncryptor(ctx, rootKey)
	if hwErr != nil {
		wipeBytes(rootKey)
		return hwErr
	}

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
		slog.Info("barrier initialized (hardware mode)", "epoch", initialEpoch)
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
		delay := b.backoffDelay()
		if elapsed < delay {
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

	if hwEnc != nil {
		// Hardware mode: skip DEK re-derivation, use hardware encryptor.
		b.hwEncryptor = hwEnc
		b.rootKey = enclave.NewProtectedKey(rootKey)
		b.state = barrierUnsealed
		b.unsealed.Store(true)
		b.failedAttempts = 0
		b.lastFailedAt = time.Time{}
		b.persistBackoffState()
		slog.Info("barrier unsealed (hardware mode)", "epoch", b.epoch)
		return nil
	}

	// Derive DEK and cipher for the current epoch.
	dek, aead, err := b.deriveEpochCipher(rootKey, b.salt, b.epoch)
	if err != nil {
		wipeBytes(rootKey)
		return err
	}

	// Re-derive DEKs for all known epochs (survives seal/unseal cycles).
	newDEKs := make(map[uint64]*enclave.ProtectedKey)
	newCiphers := make(map[uint64]cipher.AEAD)
	var plainDEKs [][]byte // track for cleanup on error
	for epoch := range b.knownEpochs {
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

	// Also ensure the current epoch is in the maps.
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

	slog.Info("barrier unsealed", "epoch", b.epoch, "algorithm", b.config.Algorithm.String())
	return nil
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

	// Clear enclave references (memory protection handled by memguard).
	b.rootKey = nil
	b.currentDEK = nil
	b.cipher = nil
	b.deks = make(map[uint64]*enclave.ProtectedKey)
	b.ciphers = make(map[uint64]cipher.AEAD)
	b.hwEncryptor = nil
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

	return b.encryptLocked(dst, plaintext, b.epoch, b.cipher)
}

// EncryptWithEpoch encrypts plaintext and returns both the ciphertext and the
// epoch used for encryption. This is used by WAL headers to record which epoch
// was used for each segment.
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

	ct, err := b.encryptLocked(dst, plaintext, b.epoch, b.cipher)
	if err != nil {
		return nil, 0, err
	}
	return ct, b.epoch, nil
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

// encryptLocked performs AEAD encryption with the algorithm-agile wire format.
// Caller must hold at least b.mu.RLock().
//
// Wire format: [epoch:8][algID:1][nonce:N][ciphertext+tag]
func (b *Barrier) encryptLocked(dst, plaintext []byte, epoch uint64, aead cipher.AEAD) ([]byte, error) {
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

	// Construct AAD binding the ciphertext to its epoch.
	aad := epochAAD(epoch)

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

	return b.decryptLocked(dst, ciphertext, aead, algID)
}

// DecryptForEpoch decrypts ciphertext using a specific epoch's DEK. This is
// used when reading old WAL segments that record the epoch in their header.
//
// In hardware mode, the epoch parameter is ignored and decryption is delegated
// to the hardware-backed encryptor.
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

	return b.decryptLocked(dst, ciphertext, aead, algID)
}

// decryptLocked performs AEAD decryption with the algorithm-agile wire format.
// Caller must hold at least b.mu.RLock().
//
// The AAD is derived from the epoch encoded in the ciphertext header, which must
// match the epoch used during encryption for authentication to succeed.
func (b *Barrier) decryptLocked(dst, ciphertext []byte, aead cipher.AEAD, alg SymmetricAlgorithm) ([]byte, error) {
	nonceSize := alg.NonceSize()
	if nonceSize == 0 {
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

	// Construct AAD binding the ciphertext to its epoch.
	aad := epochAAD(headerEpoch)

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
// AEAD safety trackers (InvocationCounter, NonceTracker) are reset for the new epoch.
//
// Rotate is not supported in hardware mode (returns ErrHardwareMode).
//
// Rotate must only be called while the barrier is unsealed. It acquires the
// exclusive write lock for the entire duration.
func (b *Barrier) Rotate() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != barrierUnsealed {
		return 0, ErrBarrierSealed
	}

	if b.hwEncryptor != nil {
		return 0, ErrHardwareMode
	}

	newEpoch := b.epoch + 1

	// Open root key enclave temporarily for HKDF derivation.
	rootBuf, err := b.rootKey.Open()
	if err != nil {
		return 0, err
	}
	dek, aead, err := b.deriveEpochCipher(rootBuf.Bytes(), b.salt, newEpoch)
	rootBuf.Destroy()
	if err != nil {
		return 0, err
	}

	// Wrap DEK in enclave (wipes dek).
	dekPK := enclave.NewProtectedKey(dek)

	oldEpoch := b.epoch
	b.epoch = newEpoch
	b.currentDEK = dekPK
	b.cipher = aead
	b.deks[newEpoch] = dekPK
	b.ciphers[newEpoch] = aead
	b.knownEpochs[newEpoch] = struct{}{}

	// Update persisted sealed state so SealedRootKeyData() returns
	// the current epoch and all known epochs for re-derivation on restart.
	b.sealed.Epoch = newEpoch
	epochs := make([]uint64, 0, len(b.knownEpochs))
	for ep := range b.knownEpochs {
		epochs = append(epochs, ep)
	}
	b.sealed.KnownEpochs = epochs

	// Reset AEAD safety trackers for the new epoch.
	if b.invocations != nil {
		if err := b.invocations.Reset(); err != nil {
			return 0, &TrackerError{Tracker: "invocation", Op: "reset", Err: err}
		}
	}
	if b.nonceTracker != nil {
		if err := b.nonceTracker.Clear(); err != nil {
			return 0, &TrackerError{Tracker: "nonce", Op: "clear", Err: err}
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

	purged := 0
	for epoch := range b.deks {
		if epoch >= minEpoch || epoch == b.epoch {
			continue
		}
		if b.inFlightEpochs[epoch] > 0 {
			continue
		}
		delete(b.deks, epoch)
		delete(b.ciphers, epoch)
		delete(b.knownEpochs, epoch)
		purged++
		slog.Info("old DEK purged", "epoch", epoch)
	}

	return purged, nil
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

// IsHardwareMode returns true if the barrier is using a hardware-backed
// encryptor for all encrypt/decrypt operations.
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
	if b.sealed == nil {
		return nil
	}
	// Return a defensive copy.
	cp := &SealedRootKey{
		Strategy:           b.sealed.Strategy,
		Version:            b.sealed.Version,
		Epoch:              b.sealed.Epoch,
		FailedAttempts:     b.sealed.FailedAttempts,
		LastFailedUnixNano: b.sealed.LastFailedUnixNano,
		KDFVersion:         b.sealed.KDFVersion,
		ShamirThreshold:    b.sealed.ShamirThreshold,
		ShamirTotal:        b.sealed.ShamirTotal,
		CreatedAt:          b.sealed.CreatedAt,
	}
	cp.Salt = make([]byte, len(b.sealed.Salt))
	copy(cp.Salt, b.sealed.Salt)
	cp.Ciphertext = make([]byte, len(b.sealed.Ciphertext))
	copy(cp.Ciphertext, b.sealed.Ciphertext)
	if b.sealed.Metadata != nil {
		cp.Metadata = make([]byte, len(b.sealed.Metadata))
		copy(cp.Metadata, b.sealed.Metadata)
	}
	if len(b.sealed.KnownEpochs) > 0 {
		cp.KnownEpochs = make([]uint64, len(b.sealed.KnownEpochs))
		copy(cp.KnownEpochs, b.sealed.KnownEpochs)
	}
	if b.sealed.Nonce != nil {
		cp.Nonce = make([]byte, len(b.sealed.Nonce))
		copy(cp.Nonce, b.sealed.Nonce)
	}
	if b.sealed.HardwarePayload != nil {
		cp.HardwarePayload = make([]byte, len(b.sealed.HardwarePayload))
		copy(cp.HardwarePayload, b.sealed.HardwarePayload)
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

	b.sealed = sealed
	b.salt = sealed.Salt
	b.epoch = sealed.Epoch

	// Restore known epochs from persisted data so Unseal can re-derive
	// DEKs for all historical epochs.
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

// --- Shamir support ---

// SetShamirSupport configures Shamir secret sharing for the barrier. The
// splitter is used during InitializeShamir to split the root key into shares.
// The accFactory creates fresh accumulators for each UnsealWithShare session.
func (b *Barrier) SetShamirSupport(config *ShamirConfig, splitter ShareSplitter, accFactory func(threshold, total int) ShareAccumulator) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shamirConfig = config
	b.splitter = splitter
	b.accFactory = accFactory
}

// InitializeShamir initializes the barrier and splits the root key into
// Shamir shares. The barrier is initialized normally via the provided
// strategy, then the root key is split into threshold-of-total shares
// using the configured ShareSplitter.
func (b *Barrier) InitializeShamir(ctx context.Context, strategy SealingStrategy, creds Credentials) (*ShamirInitResult, error) {
	b.mu.Lock()
	if b.shamirConfig == nil || b.splitter == nil {
		b.mu.Unlock()
		return nil, ErrShamirNotConfigured
	}
	cfg := *b.shamirConfig
	splitter := b.splitter
	b.mu.Unlock()

	if cfg.Threshold < 2 || cfg.Threshold > cfg.TotalShares {
		return nil, ErrShamirThresholdInvalid
	}

	// Initialize the barrier normally.
	if err := b.Initialize(ctx, strategy, creds); err != nil {
		return nil, err
	}

	// Get the root key to split.
	rootKey, err := b.RootKey()
	if err != nil {
		return nil, err
	}
	defer wipeBytes(rootKey)

	// Split root key into shares.
	shares, err := splitter.Split(rootKey, cfg.Threshold, cfg.TotalShares)
	if err != nil {
		return nil, err
	}

	// Update the sealed key with Shamir metadata.
	b.mu.Lock()
	b.sealed.ShamirThreshold = cfg.Threshold
	b.sealed.ShamirTotal = cfg.TotalShares
	b.mu.Unlock()

	return &ShamirInitResult{
		Shares:      shares,
		Threshold:   cfg.Threshold,
		TotalShares: cfg.TotalShares,
	}, nil
}

// UnsealWithShare submits a single Shamir share toward the quorum. When the
// threshold is met, the root key is reconstructed and the barrier is unsealed
// using the provided strategy and credentials.
//
// Returns QuorumProgress indicating the current state. If the quorum is not
// yet complete, the barrier remains sealed.
func (b *Barrier) UnsealWithShare(ctx context.Context, share string, strategy SealingStrategy, creds Credentials) (*QuorumProgress, error) {
	b.mu.Lock()
	if b.shamirConfig == nil || b.accFactory == nil {
		b.mu.Unlock()
		return nil, ErrShamirNotConfigured
	}
	cfg := *b.shamirConfig

	// Get or create accumulator.
	accRaw := b.accumulator.Load()
	var acc ShareAccumulator
	if accRaw != nil {
		acc = accRaw.(ShareAccumulator)
	}
	if acc == nil || acc.Expired() {
		acc = b.accFactory(cfg.Threshold, cfg.TotalShares)
		b.accumulator.Store(acc)
	}
	b.mu.Unlock()

	// Add the share.
	progress, err := acc.AddShare(ctx, share)
	if err != nil {
		return nil, err
	}

	if !progress.Complete {
		return progress, nil
	}

	// Threshold met: unseal using the strategy.
	if err := b.Unseal(ctx, strategy, creds); err != nil {
		return nil, err
	}

	return progress, nil
}

// UnsealWithShares is a convenience method that submits all shares at once.
// This is useful for automated/testing scenarios where all shares are
// available simultaneously.
func (b *Barrier) UnsealWithShares(ctx context.Context, shares []string, strategy SealingStrategy, creds Credentials) error {
	for _, share := range shares {
		progress, err := b.UnsealWithShare(ctx, share, strategy, creds)
		if err != nil {
			return err
		}
		if progress.Complete {
			return nil
		}
	}
	return ErrShamirNotConfigured
}

// GetQuorumProgress returns the current quorum progress, or nil if no
// accumulator session is active.
func (b *Barrier) GetQuorumProgress() *QuorumProgress {
	b.mu.RLock()
	defer b.mu.RUnlock()
	accRaw := b.accumulator.Load()
	if accRaw == nil {
		return nil
	}
	acc, ok := accRaw.(ShareAccumulator)
	if !ok {
		return nil
	}
	return acc.GetProgress()
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
// The keySize parameter determines the output length and is derived from the
// configured algorithm (e.g. 16 for AES-128-GCM, 32 for AES-256-GCM).
//
// Info layout: [prefix:20][deploymentID:8][epoch:8] = 36 bytes.
func deriveDEK(rootKey, salt []byte, deploymentID, epoch uint64, keySize int) ([]byte, error) {
	var info [hkdfInfoLen]byte
	copy(info[:20], hkdfInfoPrefix)
	binary.BigEndian.PutUint64(info[20:], deploymentID)
	binary.BigEndian.PutUint64(info[28:], epoch)

	hkdfReader := hkdf.New(sha256.New, rootKey, salt, info[:])
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
	dek, err := deriveDEK(rootKey, salt, b.config.DeploymentID, epoch, alg.KeySize())
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
