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
	"errors"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/jeremyhahn/go-enclave"
)

const (
	// passphraseStrategyName is the identifier stored in SealedRootKey.Strategy
	// and used as AAD.
	passphraseStrategyName = "passphrase"

	// kdfSaltLen is the KDF salt length prepended to the ciphertext.
	kdfSaltLen = 32

	// metadataKDFVersionLen is the length of the KDF version metadata.
	// The metadata is a single byte encoding the KDFVersion.
	metadataKDFVersionLen = 1

	// wrappingGCMNonceSize is the standard AES-GCM nonce size used for root key wrapping.
	wrappingGCMNonceSize = 12

	// wrappingGCMTagSize is the standard AES-GCM authentication tag size used for root key wrapping.
	wrappingGCMTagSize = 16
)

// SoftwareStrategy seals the root key with AES-256-GCM under a passphrase-derived key (Argon2id, or PBKDF2-SHA256 under FIPS); the KDF version is stored in Metadata[0]. A deferred strategy takes the passphrase from Credentials.Secret per call.
type SoftwareStrategy struct {
	// passphrase is atomic so Close cannot race Seal/Unseal, which load it once.
	passphrase atomic.Pointer[enclave.Sealed]
	kdfVersion KDFVersion
	kdfParams  KDFParams

	// randReader is the source of cryptographic randomness. Defaults to
	// crypto/rand.Reader. Injectable for testing error paths.
	randReader io.Reader

	// deferPassphrase indicates that the passphrase is provided at
	// Seal/Unseal time via Credentials.Secret rather than at construction.
	deferPassphrase bool
}

// NewSoftwareStrategy creates a SoftwareStrategy with the KDF selected by FIPS mode; ctx bounds sealing the passphrase into protected memory.
func NewSoftwareStrategy(ctx context.Context, passphrase []byte) (*SoftwareStrategy, error) {
	version := DefaultKDFVersion()
	var params KDFParams
	if version == KDFPbkdf2SHA256 {
		params = DefaultFIPSKDFParams()
	} else {
		params = DefaultKDFParams()
	}
	return newSoftwareStrategy(ctx, passphrase, version, params)
}

// NewSoftwareStrategyWithParams creates a SoftwareStrategy with custom Argon2id parameters; ErrKDFNotFIPSApproved under FIPS, ErrKDFUnsupportedVersion in a -tags fips build.
func NewSoftwareStrategyWithParams(ctx context.Context, passphrase []byte, time, memory uint32, threads uint8) (*SoftwareStrategy, error) {
	params := KDFParams{
		Time:    time,
		Memory:  memory,
		Threads: threads,
		KeyLen:  32,
		SaltLen: 32,
	}
	return newSoftwareStrategy(ctx, passphrase, KDFArgon2id, params)
}

// NewSoftwareStrategyFIPS creates a SoftwareStrategy that always uses PBKDF2-SHA256 with the given iterations.
func NewSoftwareStrategyFIPS(ctx context.Context, passphrase []byte, iterations int) (*SoftwareStrategy, error) {
	params := KDFParams{
		Iterations: iterations,
		KeyLen:     32,
		SaltLen:    32,
	}
	return newSoftwareStrategy(ctx, passphrase, KDFPbkdf2SHA256, params)
}

// NewDeferredSoftwareStrategy creates a SoftwareStrategy taking its passphrase from Credentials.Secret per call, with the default KDF.
func NewDeferredSoftwareStrategy() *SoftwareStrategy {
	version := DefaultKDFVersion()
	var params KDFParams
	if version == KDFPbkdf2SHA256 {
		params = DefaultFIPSKDFParams()
	} else {
		params = DefaultKDFParams()
	}
	return &SoftwareStrategy{
		kdfVersion:      version,
		kdfParams:       params,
		randReader:      rand.Reader,
		deferPassphrase: true,
	}
}

// NewDeferredSoftwareStrategyWithParams creates a deferred SoftwareStrategy with an explicit KDF version and params, e.g. NewDeferredSoftwareStrategyWithParams(old.KDFVersion(), old.KDFParams()); floors are checked at DeriveKey.
func NewDeferredSoftwareStrategyWithParams(version KDFVersion, params KDFParams) (*SoftwareStrategy, error) {
	// Same order, and the same two errors, as newSoftwareStrategy: absence of
	// the deriver first, then the runtime policy.
	if _, ok := kdfDispatchers[version]; !ok {
		return nil, ErrKDFUnsupportedVersion
	}
	if err := checkKDFFIPS(version); err != nil {
		return nil, err
	}
	return &SoftwareStrategy{
		kdfVersion:      version,
		kdfParams:       params,
		randReader:      rand.Reader,
		deferPassphrase: true,
	}, nil
}

// minRecommendedPassphraseLen is the length below which a passphrase is reported weak (NIST SP 800-63B); a warning, not a floor.
const minRecommendedPassphraseLen = 12

// warnIfPassphraseWeak warns when length is below the recommendation.
func warnIfPassphraseWeak(length int) {
	if length >= minRecommendedPassphraseLen || allowWeakKDFParams.Load() {
		return
	}
	slog.Warn("quicraft/seal: passphrase is shorter than the recommended minimum; "+
		"the KDF work factor cannot compensate for a small guess space, and "+
		"the sealed root key is an offline attack target",
		"length", length,
		"recommended_minimum", minRecommendedPassphraseLen,
	)
}

func newSoftwareStrategy(ctx context.Context, passphrase []byte, version KDFVersion, params KDFParams) (*SoftwareStrategy, error) {
	if len(passphrase) == 0 {
		return nil, ErrInvalidPassphrase
	}
	// Warn, do not refuse, on a short passphrase: refusing would lock existing deployments out at Unseal.
	warnIfPassphraseWeak(len(passphrase))
	if _, ok := kdfDispatchers[version]; !ok {
		return nil, ErrKDFUnsupportedVersion
	}
	// Refuse a non-approved KDF at construction under FIPS.
	if err := checkKDFFIPS(version); err != nil {
		return nil, err
	}
	// Copy the passphrase: enclave.NewSealed wipes what it is handed, and the caller owns the original.
	p := make([]byte, len(passphrase))
	copy(p, passphrase)
	// Unwrapped, like this constructor's other refusals.
	sealed, err := enclave.NewSealed(ctx, p)
	if err != nil {
		return nil, err
	}
	s := &SoftwareStrategy{
		kdfVersion: version,
		kdfParams:  params,
		randReader: rand.Reader,
	}
	s.passphrase.Store(&sealed)
	return s, nil
}

// ID returns the strategy identifier StrategyPassphrase.
func (s *SoftwareStrategy) ID() StrategyID { return StrategyPassphrase }

// Available always returns true because the software strategy has zero
// external dependencies.
func (s *SoftwareStrategy) Available() bool { return true }

// HardwareBacked returns false because the software strategy uses only
// in-process KDF + AES-GCM.
func (s *SoftwareStrategy) HardwareBacked() bool { return false }

// KDFVersion returns the KDF version this strategy uses for Seal operations.
func (s *SoftwareStrategy) KDFVersion() KDFVersion { return s.kdfVersion }

// KDFParams returns the work factors this strategy derives with, so a replacement can preserve them.
func (s *SoftwareStrategy) KDFParams() KDFParams { return s.kdfParams }

// DeferredPassphrase reports whether the passphrase is bound per call (true) or at construction; a replacement strategy must use the same mode.
func (s *SoftwareStrategy) DeferredPassphrase() bool { return s.deferPassphrase }

// BarrierEncryptor returns (nil, nil) because the software strategy uses
// epoch-based HKDF DEKs managed by the barrier itself.
func (s *SoftwareStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return nil, nil
}

// SealRootKey wraps the root key with AES-256-GCM under a passphrase-derived key with a fresh salt. Format: [KDFSalt:32 | Nonce:12 | EncryptedRootKey | Tag:16].
func (s *SoftwareStrategy) SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error) {
	passphrase, err := s.resolvePassphrase(creds, sealStrategyError)
	if err != nil {
		return nil, err
	}
	return s.sealRootKeyWith(ctx, passphrase, rootKey)
}

// sealRootKeyWith performs the seal operation under an already-resolved
// passphrase — the credentials' for a deferred strategy, the bound one
// otherwise. See resolvePassphrase for why the two cases stopped differing.
func (s *SoftwareStrategy) sealRootKeyWith(ctx context.Context, passphrase enclave.Sealed, rootKey []byte) (*SealedRootKey, error) {
	if len(rootKey) != 32 {
		return nil, ErrInvalidRootKeySize
	}

	// Generate a random salt for KDF.
	salt := make([]byte, kdfSaltLen)
	if _, err := io.ReadFull(s.randReader, salt); err != nil {
		return nil, &Error{Strategy: passphraseStrategyName, Err: err}
	}

	// Derive the wrapping key and build the AES-256-GCM cipher. The passphrase
	// enclave is opened, used and destroyed inside this call; see
	// wrappingCipher for why that is a separate step rather than inline here.
	aead, err := wrappingCipher(ctx, passphrase, salt, s.kdfVersion, s.kdfParams, sealStrategyError)
	if err != nil {
		return nil, err
	}

	return s.sealedFromCipher(aead, salt, rootKey), nil
}

// sealedFromCipher wraps rootKey under aead and assembles the blob [KDFSalt | Nonce | EncryptedRootKey | Tag], with the strategy name as AAD.
func (s *SoftwareStrategy) sealedFromCipher(aead cipher.AEAD, salt, rootKey []byte) *SealedRootKey {
	ciphertext := make([]byte, kdfSaltLen, kdfSaltLen+len(rootKey)+aead.Overhead())
	copy(ciphertext, salt)
	// G407 false positive: NewGCMWithRandomNonce generates the nonce inside the module.
	ciphertext = aead.Seal(ciphertext, nil, rootKey, []byte(passphraseStrategyName)) // #nosec G407 -- module-generated nonce; see newAESGCM

	return &SealedRootKey{
		Strategy:   StrategyPassphrase,
		Ciphertext: ciphertext,
		// The KDF version travels in metadata for cross-version unsealing.
		Metadata: []byte{byte(s.kdfVersion)},
	}
}

// UnsealRootKey derives the wrapping key from the passphrase and decrypts the root key; missing or unknown KDF metadata returns ErrKDFUnsupportedVersion.
func (s *SoftwareStrategy) UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	passphrase, err := s.resolvePassphrase(creds, unsealStrategyError)
	if err != nil {
		return nil, err
	}
	return s.unsealRootKeyWith(ctx, passphrase, sealed)
}

// unsealCipher validates a sealed blob and returns the AEAD that opens it and the [Nonce | EncryptedRootKey | Tag] payload.
func (s *SoftwareStrategy) unsealCipher(ctx context.Context, passphrase enclave.Sealed, sealed *SealedRootKey) (cipher.AEAD, []byte, error) {
	if sealed == nil {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrBarrierNotInit}
	}

	// Refuse a blob naming another strategy, or none: the Strategy field is not in the AAD.
	if sealed.Strategy != StrategyPassphrase {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrStrategyMismatch}
	}

	// Minimum ciphertext: kdfSalt(32) + nonce(12) + tag(16) = 60 bytes.
	minLen := kdfSaltLen + wrappingGCMNonceSize + wrappingGCMTagSize
	if len(sealed.Ciphertext) < minLen {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrCiphertextTooShort}
	}

	// Determine KDF version from metadata; absent or unknown is refused.
	version, params, err := s.unsealKDFVersionAndParams(sealed.Metadata)
	if err != nil {
		return nil, nil, err
	}

	// Extract KDF salt from the ciphertext prefix.
	salt := sealed.Ciphertext[:kdfSaltLen]
	rest := sealed.Ciphertext[kdfSaltLen:]

	// Derive the wrapping key from passphrase + KDF salt and build the AEAD
	// (cipher.NewGCMWithRandomNonce; see newAESGCM).
	aead, err := wrappingCipher(ctx, passphrase, salt, version, params, unsealStrategyError)
	if err != nil {
		return nil, nil, err
	}

	// rest is [Nonce | EncryptedRootKey | Tag]; the module-nonce AEAD consumes the nonce itself.
	if len(rest) < wrappingGCMNonceSize+wrappingGCMTagSize {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrCiphertextTooShort}
	}

	return aead, rest, nil
}

// unsealRootKeyWith performs the unseal operation under an already-resolved
// passphrase, returning the root key as an ordinary slice the caller must wipe.
// The protected sibling is UnsealRootKeyProtected.
func (s *SoftwareStrategy) unsealRootKeyWith(ctx context.Context, passphrase enclave.Sealed, sealed *SealedRootKey) ([]byte, error) {
	aead, rest, err := s.unsealCipher(ctx, passphrase, sealed)
	if err != nil {
		return nil, err
	}

	// Decrypt. The AAD must match the strategy name used during SealRootKey to
	// authenticate that this ciphertext was produced by the same strategy type,
	// preventing cross-context key substitution.
	plaintext, err := aead.Open(nil, nil, rest, []byte(passphraseStrategyName))
	if err != nil {
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrDecryptionFailed}
	}

	return plaintext, nil
}

// unsealKDFVersionAndParams returns the KDF version from Metadata[0] and the matching params, the strategy's own when versions match; otherwise ErrKDFUnsupportedVersion in an UnsealError.
func (s *SoftwareStrategy) unsealKDFVersionAndParams(metadata []byte) (KDFVersion, KDFParams, error) {
	if len(metadata) < metadataKDFVersionLen {
		return 0, KDFParams{}, &UnsealError{Strategy: passphraseStrategyName, Err: ErrKDFUnsupportedVersion}
	}

	version := KDFVersion(metadata[0])
	switch version {
	case KDFPbkdf2SHA256, KDFArgon2id:
	default:
		return 0, KDFParams{}, &UnsealError{Strategy: passphraseStrategyName, Err: ErrKDFUnsupportedVersion}
	}

	// If the sealed version matches the strategy's version, use the
	// strategy's params (which may be custom-tuned).
	if version == s.kdfVersion {
		return version, s.kdfParams, nil
	}

	// Versions differ: use defaults for the sealed version.
	if version == KDFPbkdf2SHA256 {
		return KDFPbkdf2SHA256, DefaultFIPSKDFParams(), nil
	}
	return KDFArgon2id, DefaultKDFParams(), nil
}

// sealStrategyError and unsealStrategyError wrap KDF and enclave failures in the caller's error type.
func sealStrategyError(err error) error {
	return &Error{Strategy: passphraseStrategyName, Err: err}
}

func unsealStrategyError(err error) error {
	return &UnsealError{Strategy: passphraseStrategyName, Err: err}
}

// wrappingCipher derives the wrapping key from the passphrase and salt and returns the AES-256-GCM cipher; the enclave and key are released by defer.
func wrappingCipher(ctx context.Context, passphrase enclave.Sealed, salt []byte, version KDFVersion, params KDFParams, wrap func(error) error) (cipher.AEAD, error) {
	ppBuf, err := passphrase.Open(ctx)
	if err != nil {
		return nil, wrap(closedIfEnclaveGone(passphrase, err))
	}
	defer releaseEnclave(ppBuf, "passphrase")

	var wrappingKey []byte
	if berr := ppBuf.WithBytes(func(pp []byte) error {
		var derr error
		wrappingKey, derr = DeriveKey(pp, salt, version, params)
		return derr
	}); berr != nil {
		return nil, wrap(berr)
	}
	defer enclave.Wipe(wrappingKey)

	// newAESGCM's own error is returned unwrapped: a
	// CipherError names its own origin and is not a strategy-level failure.
	return newAESGCM(wrappingKey)
}

// resolvePassphrase returns the sealed passphrase enclave for this call: the credentials' when deferred, else the bound one. Never copied out.
func (s *SoftwareStrategy) resolvePassphrase(creds Credentials, wrap func(error) error) (enclave.Sealed, error) {
	if s.deferPassphrase {
		if creds.Secret == nil {
			return nil, ErrEmptyCredentials
		}
		return creds.Secret, nil
	}
	// Load the passphrase pointer once so a concurrent Close cannot invalidate it.
	passphrase := s.passphrase.Load()
	if passphrase == nil {
		return nil, wrap(ErrStrategyClosed)
	}
	return *passphrase, nil
}

// SealRootKeyProtected is SealRootKey with the root key and passphrase both in enclaves; the KDF runs before the borrow.
func (s *SoftwareStrategy) SealRootKeyProtected(ctx context.Context, rootKey enclave.ByteAccess, creds Credentials) (*SealedRootKey, error) {
	passphrase, err := s.resolvePassphrase(creds, sealStrategyError)
	if err != nil {
		return nil, err
	}
	// A nil or already-destroyed enclave both report zero here. Neither is an
	// empty root key: the size check refuses them before the KDF is paid for,
	// and WithBytes below would refuse a destroyed one again.
	if rootKey == nil || rootKey.Size() != rootKeySize {
		return nil, ErrInvalidRootKeySize
	}

	salt := make([]byte, kdfSaltLen)
	if _, err := io.ReadFull(s.randReader, salt); err != nil {
		return nil, &Error{Strategy: passphraseStrategyName, Err: err}
	}

	aead, err := wrappingCipher(ctx, passphrase, salt, s.kdfVersion, s.kdfParams, sealStrategyError)
	if err != nil {
		return nil, err
	}

	// WithBytes keeps the region mapped for the whole call.
	var sealed *SealedRootKey
	if err := rootKey.WithBytes(func(key []byte) error {
		sealed = s.sealedFromCipher(aead, salt, key)
		return nil
	}); err != nil {
		return nil, &Error{Strategy: passphraseStrategyName, Err: err}
	}
	return sealed, nil
}

// UnsealRootKeyProtected decrypts the root key directly into a locked enclave the caller must Destroy.
func (s *SoftwareStrategy) UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (enclave.ByteAccess, error) {
	passphrase, err := s.resolvePassphrase(creds, unsealStrategyError)
	if err != nil {
		return nil, err
	}

	aead, rest, err := s.unsealCipher(ctx, passphrase, sealed)
	if err != nil {
		return nil, err
	}

	// The plaintext length must equal rootKeySize, checked before allocating locked memory from the unauthenticated blob.
	plaintextLen := len(rest) - wrappingGCMNonceSize - wrappingGCMTagSize
	if plaintextLen != rootKeySize {
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrInvalidSealedKey}
	}

	out, err := enclave.NewMemorySized(plaintextLen)
	if err != nil {
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: err}
	}
	// Destroyed on every failure below, and only on failure: a successful return
	// hands ownership to the caller, which the contract on
	// ProtectedSealingStrategy.UnsealRootKeyProtected makes explicit.
	failed := func(err error) (enclave.ByteAccess, error) {
		releaseEnclave(out, "unsealed root key")
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: err}
	}
	if err := out.WithBytes(func(dst []byte) error {
		plaintext, oerr := aead.Open(dst[:0], nil, rest, []byte(passphraseStrategyName))
		if oerr != nil {
			return ErrDecryptionFailed
		}
		// If Open allocated its own array, wipe it and fail.
		if len(plaintext) != len(dst) {
			enclave.Wipe(plaintext)
			return ErrDecryptionFailed
		}
		return nil
	}); err != nil {
		return failed(err)
	}

	// Freeze the enclave read-only; destroy it if the freeze fails.
	if ferr := freezeEnclave(out); ferr != nil {
		return failed(ferr)
	}
	return out, nil
}

// closedIfEnclaveGone maps a destroyed passphrase enclave to ErrStrategyClosed, keeping ErrEnclaveDestroyed when the passphrase is intact.
func closedIfEnclaveGone(passphrase enclave.Sealed, err error) error {
	if errors.Is(err, enclave.ErrEnclaveDestroyed) && !passphrase.IsAlive() {
		return ErrStrategyClosed
	}
	return err
}

// Close destroys the passphrase enclave; idempotent, safe concurrently with Seal/Unseal, and a no-op for deferred strategies.
func (s *SoftwareStrategy) Close() error {
	if passphrase := s.passphrase.Swap(nil); passphrase != nil {
		return (*passphrase).Destroy()
	}
	return nil
}
