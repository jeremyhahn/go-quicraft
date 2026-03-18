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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

const (
	// softwareStrategyName is the identifier stored in SealedRootKey.Strategy
	// and used as AAD. Kept as a string constant for backward compatibility
	// with existing sealed data.
	softwareStrategyName = "software"

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

// SoftwareStrategy implements SealingStrategy using a dual KDF system
// (Argon2id for standard mode, PBKDF2-SHA256 for FIPS mode) + AES-256-GCM.
// It derives a wrapping key from a passphrase and uses it to encrypt/decrypt
// the root key. This strategy has zero external dependencies and is suitable
// for development, testing, and deployments without hardware-backed key
// protection.
//
// The KDF version is auto-selected via DefaultKDFVersion() (which checks
// GOFIPS140) and stored in SealedRootKey.Metadata[0] so that Unseal can
// always derive the correct wrapping key regardless of the current FIPS mode.
// Backward compatibility: sealed data without metadata defaults to KDFArgon2id.
//
// When deferPassphrase is true, the passphrase is not bound at construction
// time. Instead, SealRootKey and UnsealRootKey extract the passphrase from
// the Credentials.Secret field at call time, creating a temporary inner
// strategy for each operation. This mode is useful when the passphrase is
// not known until seal/unseal time (e.g., derived from user credentials).
type SoftwareStrategy struct {
	passphrase *enclave.ProtectedKey
	kdfVersion KDFVersion
	kdfParams  KDFParams

	// randReader is the source of cryptographic randomness. Defaults to
	// crypto/rand.Reader. Injectable for testing error paths.
	randReader io.Reader

	// deferPassphrase indicates that the passphrase is provided at
	// Seal/Unseal time via Credentials.Secret rather than at construction.
	deferPassphrase bool
}

// NewSoftwareStrategy creates a SoftwareStrategy that auto-selects the KDF
// based on FIPS mode: PBKDF2-SHA256 when GOFIPS140 is set, Argon2id otherwise.
// Default parameters are used for the selected algorithm.
func NewSoftwareStrategy(passphrase []byte) (*SoftwareStrategy, error) {
	version := DefaultKDFVersion()
	var params KDFParams
	if version == KDFPbkdf2SHA256 {
		params = DefaultFIPSKDFParams()
	} else {
		params = DefaultKDFParams()
	}
	return newSoftwareStrategy(passphrase, version, params)
}

// NewSoftwareStrategyWithParams creates a SoftwareStrategy with custom Argon2id
// parameters for backward compatibility. This always uses KDFArgon2id regardless
// of FIPS mode, allowing explicit Argon2id tuning for constrained environments.
func NewSoftwareStrategyWithParams(passphrase []byte, time, memory uint32, threads uint8) (*SoftwareStrategy, error) {
	params := KDFParams{
		Time:    time,
		Memory:  memory,
		Threads: threads,
		KeyLen:  32,
		SaltLen: 32,
	}
	return newSoftwareStrategy(passphrase, KDFArgon2id, params)
}

// NewSoftwareStrategyFIPS creates a SoftwareStrategy that always uses
// PBKDF2-SHA256 with the provided iteration count, regardless of FIPS mode.
// This is useful when FIPS compliance is required by policy even when the
// GOFIPS140 environment variable is not set.
func NewSoftwareStrategyFIPS(passphrase []byte, iterations int) (*SoftwareStrategy, error) {
	params := KDFParams{
		Iterations: iterations,
		KeyLen:     32,
		SaltLen:    32,
	}
	return newSoftwareStrategy(passphrase, KDFPbkdf2SHA256, params)
}

// NewDeferredSoftwareStrategy creates a SoftwareStrategy that defers passphrase
// binding to Seal/Unseal time. The passphrase is extracted from
// Credentials.Secret on each SealRootKey and UnsealRootKey call, enabling
// scenarios where the passphrase is not known at construction time (e.g.,
// derived from per-request user credentials).
//
// The KDF version is auto-selected via DefaultKDFVersion() with default
// parameters for the selected algorithm.
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

// newSoftwareStrategy is the shared constructor for all SoftwareStrategy variants.
func newSoftwareStrategy(passphrase []byte, version KDFVersion, params KDFParams) (*SoftwareStrategy, error) {
	if len(passphrase) == 0 {
		return nil, ErrInvalidPassphrase
	}
	if _, ok := kdfDispatchers[version]; !ok {
		return nil, ErrKDFUnsupportedVersion
	}
	p := make([]byte, len(passphrase))
	copy(p, passphrase)
	return &SoftwareStrategy{
		passphrase: enclave.NewProtectedKey(p),
		kdfVersion: version,
		kdfParams:  params,
		randReader: rand.Reader,
	}, nil
}

// ID returns the strategy identifier StrategySoftware.
func (s *SoftwareStrategy) ID() StrategyID { return StrategySoftware }

// Available always returns true because the software strategy has zero
// external dependencies.
func (s *SoftwareStrategy) Available() bool { return true }

// HardwareBacked returns false because the software strategy uses only
// in-process KDF + AES-GCM.
func (s *SoftwareStrategy) HardwareBacked() bool { return false }

// KDFVersion returns the KDF version this strategy uses for Seal operations.
func (s *SoftwareStrategy) KDFVersion() KDFVersion { return s.kdfVersion }

// BarrierEncryptor returns (nil, nil) because the software strategy uses
// epoch-based HKDF DEKs managed by the barrier itself.
func (s *SoftwareStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return nil, nil
}

// SealRootKey derives a wrapping key from the passphrase via the configured KDF,
// then encrypts the root key using AES-256-GCM. A fresh random salt is
// generated for each SealRootKey call and prepended to the ciphertext for
// self-contained unsealing.
//
// The KDF version is stored in SealedRootKey.Metadata[0] so that UnsealRootKey
// can always use the correct algorithm regardless of the current FIPS mode.
//
// When deferPassphrase is false (default), the ctx and creds parameters are
// unused; the passphrase is provided at construction time. When
// deferPassphrase is true, the passphrase is extracted from creds.Secret.
//
// Ciphertext format: [KDFSalt:32 | Nonce:12 | EncryptedRootKey | Tag:16]
func (s *SoftwareStrategy) SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error) {
	if s.deferPassphrase {
		return s.sealRootKeyDeferred(ctx, rootKey, creds)
	}
	return s.sealRootKeyBound(rootKey)
}

// sealRootKeyDeferred creates a temporary bound strategy from creds.Secret
// and delegates the seal operation to it.
func (s *SoftwareStrategy) sealRootKeyDeferred(_ context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error) {
	if len(creds.Secret) == 0 {
		return nil, ErrEmptyCredentials
	}
	inner, err := newSoftwareStrategy([]byte(creds.Secret), s.kdfVersion, s.kdfParams)
	if err != nil {
		return nil, err
	}
	defer func() { _ = inner.Close() }()
	return inner.sealRootKeyBound(rootKey)
}

// sealRootKeyBound performs the seal operation using the bound passphrase.
func (s *SoftwareStrategy) sealRootKeyBound(rootKey []byte) (*SealedRootKey, error) {
	if len(rootKey) != 32 {
		return nil, ErrInvalidRootKeySize
	}

	// Generate a random salt for KDF.
	salt := make([]byte, kdfSaltLen)
	if _, err := io.ReadFull(s.randReader, salt); err != nil {
		return nil, &Error{Strategy: softwareStrategyName, Err: err}
	}

	// Open passphrase enclave for KDF derivation.
	ppBuf, err := s.passphrase.Open()
	if err != nil {
		return nil, &Error{Strategy: softwareStrategyName, Err: err}
	}

	// Derive wrapping key from passphrase using the configured KDF.
	wrappingKey, err := DeriveKey(ppBuf.Bytes(), salt, s.kdfVersion, s.kdfParams)
	ppBuf.Destroy()
	if err != nil {
		return nil, &Error{Strategy: softwareStrategyName, Err: err}
	}
	defer wipeBytes(wrappingKey)

	// Create AES-256-GCM cipher.
	block, err := aes.NewCipher(wrappingKey)
	if err != nil {
		return nil, &CipherError{Err: err}
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, &CipherError{Err: err}
	}

	// Generate a random nonce.
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(s.randReader, nonce); err != nil {
		return nil, &Error{Strategy: softwareStrategyName, Err: err}
	}

	// Build ciphertext: [KDFSalt | Nonce | EncryptedRootKey | Tag]
	// The KDF salt is stored inside the ciphertext so that UnsealRootKey is
	// self-contained and does not depend on the SealedRootKey.Salt field.
	//
	// AAD binds the strategy name to the ciphertext, preventing cross-context
	// key substitution where a ciphertext sealed by one strategy could be
	// presented to another.
	aad := []byte(softwareStrategyName)
	ciphertext := make([]byte, kdfSaltLen, kdfSaltLen+aead.NonceSize()+len(rootKey)+aead.Overhead())
	copy(ciphertext, salt)
	ciphertext = append(ciphertext, nonce...)
	ciphertext = aead.Seal(ciphertext, nonce, rootKey, aad)

	// Store KDF version in metadata for cross-version unsealing.
	metadata := []byte{byte(s.kdfVersion)}

	return &SealedRootKey{
		Strategy:   StrategySoftware,
		Ciphertext: ciphertext,
		Metadata:   metadata,
	}, nil
}

// UnsealRootKey derives the wrapping key from the passphrase and decrypts the root key.
// The KDF salt is extracted from the ciphertext prefix, and the KDF version is
// read from SealedRootKey.Metadata[0]. If metadata is absent (pre-FIPS sealed
// data), KDFArgon2id is assumed for backward compatibility.
//
// When deferPassphrase is false (default), the ctx and creds parameters are
// unused; the passphrase is provided at construction time. When
// deferPassphrase is true, the passphrase is extracted from creds.Secret.
//
// Expected ciphertext format: [KDFSalt:32 | Nonce:12 | EncryptedRootKey | Tag:16]
func (s *SoftwareStrategy) UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	if s.deferPassphrase {
		return s.unsealRootKeyDeferred(ctx, sealed, creds)
	}
	return s.unsealRootKeyBound(sealed)
}

// unsealRootKeyDeferred creates a temporary bound strategy from creds.Secret
// and delegates the unseal operation to it.
func (s *SoftwareStrategy) unsealRootKeyDeferred(_ context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	if len(creds.Secret) == 0 {
		return nil, ErrEmptyCredentials
	}
	inner, err := newSoftwareStrategy([]byte(creds.Secret), s.kdfVersion, s.kdfParams)
	if err != nil {
		return nil, err
	}
	defer func() { _ = inner.Close() }()
	return inner.unsealRootKeyBound(sealed)
}

// unsealRootKeyBound performs the unseal operation using the bound passphrase.
func (s *SoftwareStrategy) unsealRootKeyBound(sealed *SealedRootKey) ([]byte, error) {
	if sealed == nil {
		return nil, &UnsealError{Strategy: softwareStrategyName, Err: ErrBarrierNotInit}
	}

	// Minimum ciphertext: kdfSalt(32) + nonce(12) + tag(16) = 60 bytes.
	minLen := kdfSaltLen + wrappingGCMNonceSize + wrappingGCMTagSize
	if len(sealed.Ciphertext) < minLen {
		return nil, &UnsealError{Strategy: softwareStrategyName, Err: ErrCiphertextTooShort}
	}

	// Determine KDF version from metadata. Backward compatibility: absent
	// metadata means the data was sealed before FIPS support, so default
	// to KDFArgon2id with the strategy's current params.
	version, params := s.unsealKDFVersionAndParams(sealed.Metadata)

	// Extract KDF salt from the ciphertext prefix.
	salt := sealed.Ciphertext[:kdfSaltLen]
	rest := sealed.Ciphertext[kdfSaltLen:]

	// Open passphrase enclave for KDF derivation.
	ppBuf, err := s.passphrase.Open()
	if err != nil {
		return nil, &UnsealError{Strategy: softwareStrategyName, Err: err}
	}

	// Derive the wrapping key from passphrase + KDF salt.
	wrappingKey, err := DeriveKey(ppBuf.Bytes(), salt, version, params)
	ppBuf.Destroy()
	if err != nil {
		return nil, &UnsealError{Strategy: softwareStrategyName, Err: err}
	}
	defer wipeBytes(wrappingKey)

	// Create AES-256-GCM cipher.
	block, err := aes.NewCipher(wrappingKey)
	if err != nil {
		return nil, &CipherError{Err: err}
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, &CipherError{Err: err}
	}

	nonceSize := aead.NonceSize()
	if len(rest) < nonceSize {
		return nil, &UnsealError{Strategy: softwareStrategyName, Err: ErrCiphertextTooShort}
	}

	// Extract nonce and decrypt. The AAD must match the strategy name used
	// during SealRootKey to authenticate that this ciphertext was produced by the
	// same strategy type, preventing cross-context key substitution.
	aad := []byte(softwareStrategyName)
	nonce := rest[:nonceSize]
	encrypted := rest[nonceSize:]

	plaintext, err := aead.Open(nil, nonce, encrypted, aad)
	if err != nil {
		return nil, &UnsealError{Strategy: softwareStrategyName, Err: ErrDecryptionFailed}
	}

	return plaintext, nil
}

// unsealKDFVersionAndParams determines the KDF version and parameters to use
// for unsealing based on the stored metadata. If metadata is nil or empty,
// KDFArgon2id is assumed with the strategy's current params for backward
// compatibility with pre-FIPS sealed data.
//
// When the metadata version matches the strategy's configured version, the
// strategy's own params are used (preserving custom tuning like iteration
// counts or Argon2id memory/time settings). When the versions differ,
// defaults for the metadata version are returned. This means a key sealed
// with non-default params can only be unsealed by a strategy configured
// with matching params.
func (s *SoftwareStrategy) unsealKDFVersionAndParams(metadata []byte) (KDFVersion, KDFParams) {
	if len(metadata) < metadataKDFVersionLen {
		// Pre-FIPS sealed data: no metadata present, use the strategy's
		// configured KDF (which is KDFArgon2id for legacy constructors).
		return s.kdfVersion, s.kdfParams
	}

	version := KDFVersion(metadata[0])

	// If the sealed version matches the strategy's version, use the
	// strategy's params (which may be custom-tuned).
	if version == s.kdfVersion {
		return version, s.kdfParams
	}

	// Versions differ: use defaults for the sealed version.
	switch version {
	case KDFPbkdf2SHA256:
		return KDFPbkdf2SHA256, DefaultFIPSKDFParams()
	case KDFArgon2id:
		return KDFArgon2id, DefaultKDFParams()
	default:
		// Unknown version: fall back to strategy's configured KDF.
		// The AES-GCM authentication will catch mismatches.
		return s.kdfVersion, s.kdfParams
	}
}

// Close releases strategy-held resources by clearing the passphrase enclave
// reference, allowing memguard's finalizer to securely wipe the key material.
// This method is idempotent. For deferred strategies (where passphrase is nil),
// Close is a no-op.
func (s *SoftwareStrategy) Close() error {
	if s.passphrase != nil {
		s.passphrase.Clear()
		s.passphrase = nil
	}
	return nil
}
