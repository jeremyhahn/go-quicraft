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

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
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
	// passphrase is an atomic.Pointer so that Close (which drops the
	// reference) cannot race the Seal/Unseal paths that read it. Those paths
	// load the pointer ONCE into a local and operate on the local; a Close
	// that lands mid-operation therefore cannot turn a validated non-nil
	// reference into a nil dereference.
	passphrase atomic.Pointer[enclave.ProtectedKey]
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
// parameters, allowing explicit Argon2id tuning for constrained environments.
//
// It selects KDFArgon2id explicitly, so under FIPS mode it FAILS with
// ErrKDFNotFIPSApproved rather than quietly wrapping a "FIPS" deployment's root
// key with a non-approved KDF. Use NewSoftwareStrategyFIPS (PBKDF2-SHA256) or
// NewSoftwareStrategy (which auto-selects by mode) there.
//
// IT EXISTS IN EVERY BUILD, INCLUDING -tags fips, and cannot succeed there. The
// exported surface of a package must not move with a build tag: a consumer that
// guards this call behind its own runtime FIPS check would otherwise fail to
// COMPILE in a FIPS build over a line it never executes, and go-quicraft is a
// Layer 1 dependency, so that breakage would propagate through every consumer.
// What the tag removes is the Argon2id DERIVER (kdf_argon2.go), and that is
// enough: newSoftwareStrategy looks the version up in kdfDispatchers, finds
// nothing, and returns ErrKDFUnsupportedVersion before any key material or work
// factor is touched. Absence of the algorithm is what refuses the call, not a
// policy branch — the same mechanism, and the same error, that DeriveKey gives
// for KDFArgon2id in a FIPS build. Identifiers and signatures stay; ciphers
// leave.
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
// minRecommendedPassphraseLen is the length below which a passphrase is
// reported as weak. 12 bytes is the NIST SP 800-63B minimum for a memorized
// secret with no composition rules.
//
// It is a RECOMMENDATION, not a floor: see newSoftwareStrategy for why this
// warns rather than refuses.
const minRecommendedPassphraseLen = 12

// warnIfPassphraseWeak reports a passphrase shorter than the recommendation. It
// takes the LENGTH rather than the secret: every caller already knows it, and a
// helper that took the bytes would be one more place holding them.
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

func newSoftwareStrategy(passphrase []byte, version KDFVersion, params KDFParams) (*SoftwareStrategy, error) {
	if len(passphrase) == 0 {
		return nil, ErrInvalidPassphrase
	}
	// The KDF work-factor floors (kdf.go) are enforced precisely because "the
	// sealed root key protects all data at rest and is a high-value OFFLINE
	// attack target". That argument has two halves and only one was enforced:
	// a work factor multiplies the per-guess cost, it cannot make a small
	// guess space large. At Argon2id t=4/m=128MiB a single-character, PIN, or
	// top-1000 password space still falls in minutes on one machine, and the
	// recovered root key yields every epoch DEK.
	//
	// This WARNS rather than refuses. Rejecting a short passphrase would fire
	// at Unseal as well as Seal, so it would lock existing deployments out of
	// their own data — a policy decision for the operator, not one this
	// constructor should make silently. Making the weakness loud is the part
	// that is unambiguously right.
	warnIfPassphraseWeak(len(passphrase))
	if _, ok := kdfDispatchers[version]; !ok {
		return nil, ErrKDFUnsupportedVersion
	}
	// Refuse a non-approved KDF at CONSTRUCTION under FIPS, not at the first
	// derivation: the strategy is built once and used for every seal and unseal,
	// so the earliest possible refusal is the one an operator can act on.
	if err := checkKDFFIPS(version); err != nil {
		return nil, err
	}
	p := make([]byte, len(passphrase))
	copy(p, passphrase)
	s := &SoftwareStrategy{
		kdfVersion: version,
		kdfParams:  params,
		randReader: rand.Reader,
	}
	s.passphrase.Store(enclave.NewProtectedKey(p))
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

// DeferredPassphrase reports whether this strategy binds its passphrase at
// Seal/Unseal time (true, NewDeferredSoftwareStrategy) or at construction
// (false, every other constructor).
//
// The two modes have OPPOSITE credential semantics, which is why the
// distinction is exported rather than inferred. A BOUND strategy ignores the
// Credentials passed to SealRootKey and UnsealRootKey — it already holds the
// passphrase — so an unseal attempt with the wrong secret SUCCEEDS. A DEFERRED
// strategy takes the passphrase from Credentials.Secret on every call, so the
// same attempt FAILS.
//
// A consumer that REPLACES a registered strategy (re-sealing the root key under
// a new passphrase, say) must construct the replacement in the same mode as the
// one it replaces. Installing a bound strategy where a deferred one stood turns
// a barrier that authenticates its unsealer into one that accepts any
// passphrase for the lifetime of the process — a silent authentication bypass,
// measured on the production shape in go-qrdb #157. Without this accessor the
// mode is invisible and the replacement is a coin flip.
//
// The mode is fixed at construction and never changes, so the value is stable
// for the lifetime of the strategy and safe to read concurrently.
func (s *SoftwareStrategy) DeferredPassphrase() bool { return s.deferPassphrase }

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
	inner, err := newSoftwareStrategy(creds.Secret, s.kdfVersion, s.kdfParams)
	if err != nil {
		return nil, err
	}
	defer func() { _ = inner.Close() }()
	return inner.sealRootKeyBound(rootKey)
}

// sealRootKeyBound performs the seal operation using the bound passphrase.
func (s *SoftwareStrategy) sealRootKeyBound(rootKey []byte) (*SealedRootKey, error) {
	// Close() drops the passphrase enclave; calling Open on the nil
	// *ProtectedKey would panic, so a closed (or never-bound) strategy
	// fails with a typed error instead. The pointer is loaded ONCE so a
	// concurrent Close cannot invalidate it after this check.
	passphrase := s.passphrase.Load()
	if passphrase == nil {
		return nil, &Error{Strategy: passphraseStrategyName, Err: ErrStrategyClosed}
	}
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
	aead, err := wrappingCipher(passphrase, salt, s.kdfVersion, s.kdfParams, sealStrategyError)
	if err != nil {
		return nil, err
	}

	return s.sealedFromCipher(aead, salt, rootKey), nil
}

// sealedFromCipher wraps rootKey under the prepared AEAD and assembles the
// persisted blob. It is the half of sealRootKeyBound that touches key material,
// split out so that the protected path can run exactly this and nothing else
// inside its enclave borrow — the KDF above it opens a second enclave, which a
// borrow may not do (see wrappingCipher).
//
// Ciphertext layout: [KDFSalt | Nonce | EncryptedRootKey | Tag]. The KDF salt is
// stored inside the ciphertext so that UnsealRootKey is self-contained and does
// not depend on the SealedRootKey.Salt field.
//
// The nonce is not appended by hand: aead.Seal emits it as the first
// wrappingGCMNonceSize bytes of its own output, in the same position the
// hand-written append used to put it, so this blob has the identical layout to
// every blob this strategy has ever written.
//
// AAD binds the strategy name to the ciphertext, preventing cross-context key
// substitution where a ciphertext sealed by one strategy could be presented to
// another.
func (s *SoftwareStrategy) sealedFromCipher(aead cipher.AEAD, salt, rootKey []byte) *SealedRootKey {
	ciphertext := make([]byte, kdfSaltLen, kdfSaltLen+len(rootKey)+aead.Overhead())
	copy(ciphertext, salt)
	// G407 false positive: the nil nonce is not a hardcoded IV, it is the
	// ABSENCE of a caller-supplied one. cipher.NewGCMWithRandomNonce reports
	// NonceSize() == 0 and panics if handed any nonce at all; it generates one
	// inside the FIPS module per Seal and prepends it to this output. A nonce
	// literal here is the construction that rule exists to catch, and is exactly
	// what this call site removed.
	ciphertext = aead.Seal(ciphertext, nil, rootKey, []byte(passphraseStrategyName)) // #nosec G407 -- module-generated nonce; see newAESGCM

	return &SealedRootKey{
		Strategy:   StrategyPassphrase,
		Ciphertext: ciphertext,
		// The KDF version travels in metadata for cross-version unsealing.
		Metadata: []byte{byte(s.kdfVersion)},
	}
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
	inner, err := newSoftwareStrategy(creds.Secret, s.kdfVersion, s.kdfParams)
	if err != nil {
		return nil, err
	}
	defer func() { _ = inner.Close() }()
	return inner.unsealRootKeyBound(sealed)
}

// unsealCipher validates a sealed blob and prepares the AEAD that opens it,
// under the passphrase its caller resolved,
// returning the cipher alongside the [Nonce | EncryptedRootKey | Tag] payload
// it applies to. It is everything unsealing does BEFORE the plaintext exists,
// split out so the []byte and protected paths cannot drift in what they
// validate — and so the protected path can prepare its cipher before it takes
// an enclave borrow, which is the ordering wrappingCipher explains.
func (s *SoftwareStrategy) unsealCipher(passphrase *enclave.ProtectedKey, sealed *SealedRootKey) (cipher.AEAD, []byte, error) {
	if sealed == nil {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrBarrierNotInit}
	}

	// Reject a sealed blob that claims a different strategy. The Strategy
	// field is plaintext and NOT covered by the AEAD AAD (which is the
	// constant strategy name), so without this check an attacker who flips
	// the field to "none" still gets a successful decrypt here while
	// steering Barrier.Unseal down its no-encryption branch — a downgrade
	// that leaves the barrier "unsealed" with no cipher. Empty is accepted
	// for sealed data written before the field was populated.
	if sealed.Strategy != "" && sealed.Strategy != StrategyPassphrase {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrStrategyMismatch}
	}

	// Minimum ciphertext: kdfSalt(32) + nonce(12) + tag(16) = 60 bytes.
	minLen := kdfSaltLen + wrappingGCMNonceSize + wrappingGCMTagSize
	if len(sealed.Ciphertext) < minLen {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrCiphertextTooShort}
	}

	// Determine KDF version from metadata. Backward compatibility: absent
	// metadata means the data was sealed before FIPS support, so default
	// to KDFArgon2id with the strategy's current params.
	version, params := s.unsealKDFVersionAndParams(sealed.Metadata)

	// Extract KDF salt from the ciphertext prefix.
	salt := sealed.Ciphertext[:kdfSaltLen]
	rest := sealed.Ciphertext[kdfSaltLen:]

	// Derive the wrapping key from passphrase + KDF salt and build the AEAD
	// (cipher.NewGCMWithRandomNonce; see newAESGCM).
	aead, err := wrappingCipher(passphrase, salt, version, params, unsealStrategyError)
	if err != nil {
		return nil, nil, err
	}

	// rest is [Nonce | EncryptedRootKey | Tag]. The nonce is not sliced off
	// here: a module-nonce AEAD expects it at the head of the payload it is
	// given and consumes it internally, which is the same byte range the
	// pre-migration code passed separately. Blobs written by either version
	// therefore open identically — see TestLegacyFormat_UnsealRootKey, which
	// opens checked-in pre-migration bytes.
	if len(rest) < wrappingGCMNonceSize+wrappingGCMTagSize {
		return nil, nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrCiphertextTooShort}
	}

	return aead, rest, nil
}

// unsealRootKeyBound performs the unseal operation using the bound passphrase,
// returning the root key as an ordinary slice the caller must wipe. The
// protected sibling is unsealRootKeyBoundProtected.
func (s *SoftwareStrategy) unsealRootKeyBound(sealed *SealedRootKey) ([]byte, error) {
	// Close() drops the passphrase enclave; calling Open on the nil
	// *ProtectedKey would panic, so a closed (or never-bound) strategy fails
	// with a typed error instead. The pointer is loaded ONCE so a concurrent
	// Close cannot invalidate it after this check.
	passphrase := s.passphrase.Load()
	if passphrase == nil {
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrStrategyClosed}
	}

	aead, rest, err := s.unsealCipher(passphrase, sealed)
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
		// Metadata-less (pre-FIPS) sealed data does not record which KDF or
		// params produced it, so the only correct choice is the strategy's
		// OWN configured KDF/params — that data was sealed by a strategy with
		// the same configuration. (Using a hardcoded default would break
		// round-tripping data sealed with custom Argon2id params.) A genuine
		// KDF-config change between seal and unseal on metadata-less data is
		// inherently ambiguous and fails closed via AES-GCM authentication.
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

// sealStrategyError and unsealStrategyError are the two error shapes this
// strategy reports its KDF and enclave failures in. They exist as functions so
// that wrappingCipher — which is shared by the seal and unseal paths — wraps in
// whichever one its caller uses, instead of the shared helper flattening two
// distinct error types into one and changing what callers can match on.
func sealStrategyError(err error) error {
	return &Error{Strategy: passphraseStrategyName, Err: err}
}

func unsealStrategyError(err error) error {
	return &UnsealError{Strategy: passphraseStrategyName, Err: err}
}

// wrappingCipher derives the wrapping key from the passphrase and the given KDF
// salt and returns the AES-256-GCM cipher built from it. The passphrase enclave
// is opened, read and destroyed inside this call, and the derived key is wiped
// on the way out, so nothing it allocates outlives it.
//
// The Destroy and the wipe are DEFERRED: a panic inside the KDF (or anything a
// caller injects on that path) would otherwise strand an mlock'd buffer holding
// the PLAINTEXT passphrase for the life of the process, and leaked locked
// buffers consume RLIMIT_MEMLOCK, which memguard answers by aborting the
// process.
//
// It is a separate step, rather than inline in the seal/unseal bodies, for a
// reason that outlives the deduplication: the protected paths run their
// key-touching half inside a MemoryEnclave borrow, and a borrow may not open a
// second enclave. pkg/enclave holds a process-wide purgeMu across every borrow,
// and ProtectedKey.Open allocates locked memory, which takes that same mutex
// exclusively — so opening the passphrase inside a root-key borrow is a
// self-deadlock, not merely a documented hazard. Preparing the cipher first is
// what makes the borrow that follows safe, and the AEAD itself holds only the
// expanded key schedule in ordinary memory, which the borrow may touch freely.
func wrappingCipher(passphrase *enclave.ProtectedKey, salt []byte, version KDFVersion, params KDFParams, wrap func(error) error) (cipher.AEAD, error) {
	ppBuf, err := passphrase.Open()
	if err != nil {
		return nil, wrap(closedIfEnclaveGone(err))
	}
	defer ppBuf.Destroy()

	wrappingKey, err := DeriveKey(ppBuf.Bytes(), salt, version, params)
	if err != nil {
		return nil, wrap(err)
	}
	defer wipeBytes(wrappingKey)

	// newAESGCM's own error is returned unwrapped, as it always has been: a
	// CipherError names its own origin and is not a strategy-level failure.
	return newAESGCM(wrappingKey)
}

// protectedPassphrase resolves which ProtectedKey holds the passphrase for a
// protected call: the credentials for a deferred strategy, the one bound at
// construction otherwise. Neither is opened here and neither is ever copied out
// — the returned pointer is handed straight to wrappingCipher.
//
// The deferred branch is why the protected paths do not build a temporary inner
// strategy the way their []byte siblings do. That construction exists to copy a
// plain passphrase into locked memory; a ProtectedCredentials passphrase is
// already there, so building a strategy around it would allocate a second
// enclave holding the same secret for the life of the call and buy nothing.
//
// One behaviour moves with it: newSoftwareStrategy refuses a FIPS-unapproved KDF
// at construction, and without that construction the refusal lands at DeriveKey
// instead. Same error, same call, one frame later.
func (s *SoftwareStrategy) protectedPassphrase(creds ProtectedCredentials, wrap func(error) error) (*enclave.ProtectedKey, error) {
	if s.deferPassphrase {
		if creds.Secret == nil {
			return nil, ErrEmptyCredentials
		}
		return creds.Secret, nil
	}
	// Close() drops the passphrase enclave; the pointer is loaded ONCE so a
	// concurrent Close cannot invalidate it after this check.
	passphrase := s.passphrase.Load()
	if passphrase == nil {
		return nil, wrap(ErrStrategyClosed)
	}
	return passphrase, nil
}

// SealRootKeyProtected is SealRootKey with BOTH secrets on the protected side —
// the root key in the caller's enclave, the passphrase in the credentials'.
// See ProtectedSealingStrategy for the contract; the produced blob is
// byte-compatible with SealRootKey's and opens with either unseal method.
//
// The order of operations is the point of this function. The KDF runs FIRST,
// outside any borrow, because it opens the passphrase enclave; only the AEAD
// Seal — which touches no protected memory but the root key itself — runs
// inside the borrow. The key is therefore never copied out of its enclave, and
// the borrow cannot deadlock against a process-wide purge. See wrappingCipher.
func (s *SoftwareStrategy) SealRootKeyProtected(_ context.Context, rootKey *enclave.MemoryEnclave, creds ProtectedCredentials) (*SealedRootKey, error) {
	passphrase, err := s.protectedPassphrase(creds, sealStrategyError)
	if err != nil {
		return nil, err
	}
	// A nil or already-destroyed enclave both report zero here. Neither is an
	// empty root key: the size check refuses them before the KDF is paid for,
	// and WithBytes below would refuse a destroyed one again.
	if rootKey.Size() != rootKeySize {
		return nil, ErrInvalidRootKeySize
	}

	salt := make([]byte, kdfSaltLen)
	if _, err := io.ReadFull(s.randReader, salt); err != nil {
		return nil, &Error{Strategy: passphraseStrategyName, Err: err}
	}

	aead, err := wrappingCipher(passphrase, salt, s.kdfVersion, s.kdfParams, sealStrategyError)
	if err != nil {
		return nil, err
	}

	// WithBytes rather than Bytes: it holds the region mapped across the whole
	// call, so an owner destroying the enclave concurrently is reported as an
	// error instead of unmapping memory the AEAD is reading. Its only failure
	// is exactly that — the size is settled above and the callback cannot fail.
	var sealed *SealedRootKey
	if err := rootKey.WithBytes(func(key []byte) error {
		sealed = s.sealedFromCipher(aead, salt, key)
		return nil
	}); err != nil {
		return nil, &Error{Strategy: passphraseStrategyName, Err: err}
	}
	return sealed, nil
}

// UnsealRootKeyProtected decrypts the root key straight into locked memory,
// with the passphrase never leaving protected memory either. See
// ProtectedSealingStrategy for the ownership contract: the caller owns the
// returned enclave and must Destroy it.
//
// The plaintext root key never exists as an ordinary heap allocation, which is
// the whole difference from UnsealRootKey. GCM's plaintext length is known
// before the decryption — it is the payload minus the nonce and the tag — so
// the enclave is allocated at exactly that size and aead.Open is handed the
// enclave's own storage as its append destination. crypto/cipher writes into a
// destination with sufficient capacity rather than allocating a new one, so the
// bytes land inside the locked region and nowhere else.
//
// That length comes from an unauthenticated blob, so it is REFUSED unless it is
// exactly the root-key size — before anything is locked. See below.
//
// The allocation happens BEFORE the borrow, because allocating locked memory is
// a process-wide purge-exclusive operation that a borrow may not nest (see
// wrappingCipher), and the KDF runs before that for the same reason.
func (s *SoftwareStrategy) UnsealRootKeyProtected(_ context.Context, sealed *SealedRootKey, creds ProtectedCredentials) (*enclave.MemoryEnclave, error) {
	passphrase, err := s.protectedPassphrase(creds, unsealStrategyError)
	if err != nil {
		return nil, err
	}

	aead, rest, err := s.unsealCipher(passphrase, sealed)
	if err != nil {
		return nil, err
	}

	// Exact plaintext length for AES-GCM with a module-generated nonce: the
	// payload carries the nonce ahead of the ciphertext and the tag after it.
	//
	// It must equal the root-key size, and the check runs BEFORE the allocation
	// rather than after the decrypt. The length is a function of the sealed
	// blob, which is UNAUTHENTICATED at this point — the AEAD has not run — so
	// sizing an mlock'd allocation from it hands whoever can write the blob a
	// way to demand an arbitrary amount of a finite kernel resource, and
	// memguard answers a lock it cannot take by TERMINATING THE PROCESS. A
	// corrupt or hostile blob would take the service down before the tag that
	// would have rejected it was ever checked (Codex review of PR #40).
	//
	// Pinning it to rootKeySize rather than merely bounding it costs nothing:
	// both seal paths refuse anything but a 32-byte key, so no blob this
	// strategy has ever written has another length, and Barrier.Unseal refuses
	// one that does immediately after the string-typed call returns.
	plaintextLen := len(rest) - wrappingGCMNonceSize - wrappingGCMTagSize
	if plaintextLen != rootKeySize {
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: ErrInvalidSealedKey}
	}

	out := enclave.NewMemoryEnclave(plaintextLen)
	if err := out.WithBytes(func(dst []byte) error {
		plaintext, oerr := aead.Open(dst[:0], nil, rest, []byte(passphraseStrategyName))
		if oerr != nil {
			return ErrDecryptionFailed
		}
		// If Open had needed more room than dst's capacity it would have
		// allocated its own array, and the key would be sitting on the heap
		// instead of in here. It cannot, given the length computed above — but
		// "cannot" is what this check is for, and the escape hatch is to wipe
		// what escaped rather than to return a half-protected key.
		if len(plaintext) != len(dst) {
			wipeBytes(plaintext)
			return ErrDecryptionFailed
		}
		return nil
	}); err != nil {
		out.Destroy()
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: err}
	}

	// Frozen once the material is final: the contract hands the caller a key to
	// read and destroy, not a buffer to edit, and a read-only region faults on a
	// stray write instead of silently corrupting the root key.
	out.Freeze()
	return out, nil
}

// closedIfEnclaveGone maps the enclave-nil error to ErrStrategyClosed. Close
// clears the enclave INSIDE the ProtectedKey, so a Close that lands after a
// seal/unseal loaded the reference surfaces as ErrEnclaveNil from Open; that
// is the same condition as "used after Close" and must report the same error.
func closedIfEnclaveGone(err error) error {
	if errors.Is(err, enclave.ErrEnclaveNil) {
		return ErrStrategyClosed
	}
	return err
}

// Close releases strategy-held resources by clearing the passphrase enclave
// reference, after which the passphrase can no longer be opened. It does not
// WIPE the enclave: memguard exposes no API to zero an Enclave's ciphertext and
// registers no finalizer that would (v0.23.0 has none outside its own tests),
// so the encrypted passphrase stays on the heap until the GC reclaims it —
// unreadable without memguard's per-process session coffer key. See
// enclave.ProtectedKey.Clear.
//
// This method is idempotent and safe to call concurrently with Seal/Unseal:
// an operation that already loaded the reference completes against it, and any
// operation that starts afterwards fails with ErrStrategyClosed. For deferred
// strategies (where the passphrase is bound per call), Close is a no-op.
func (s *SoftwareStrategy) Close() error {
	if passphrase := s.passphrase.Swap(nil); passphrase != nil {
		passphrase.Clear()
	}
	return nil
}
