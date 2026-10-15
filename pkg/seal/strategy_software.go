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

// NewSoftwareStrategy creates a SoftwareStrategy that auto-selects the KDF
// based on FIPS mode: PBKDF2-SHA256 when GOFIPS140 is set, Argon2id otherwise.
// Default parameters are used for the selected algorithm.
//
// ctx bounds sealing a copy of the passphrase into protected memory, the one
// step here that go-enclave may hand to a blocking provider. Every bound
// constructor (this one, NewSoftwareStrategyWithParams and
// NewSoftwareStrategyFIPS) takes it for that reason; the deferred constructors
// hold no passphrase and take none.
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

// NewSoftwareStrategyFIPS creates a SoftwareStrategy that always uses
// PBKDF2-SHA256 with the provided iteration count, regardless of FIPS mode.
// This is useful when FIPS compliance is required by policy even when the
// GOFIPS140 environment variable is not set.
func NewSoftwareStrategyFIPS(ctx context.Context, passphrase []byte, iterations int) (*SoftwareStrategy, error) {
	params := KDFParams{
		Iterations: iterations,
		KeyLen:     32,
		SaltLen:    32,
	}
	return newSoftwareStrategy(ctx, passphrase, KDFPbkdf2SHA256, params)
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

// NewDeferredSoftwareStrategyWithParams creates a deferred SoftwareStrategy that
// derives with an EXPLICIT KDF version and work factor, instead of the defaults
// NewDeferredSoftwareStrategy selects.
//
// It is the only constructor that is both deferred and tunable: the two
// tunable constructors above are both BOUND, and NewDeferredSoftwareStrategy
// takes no parameters. A consumer that must preserve a work factor across a
// strategy replacement that installs a deferred strategy — a password change,
// say — needs it, or every hardened deployment would be reset to defaults on
// each such replacement.
//
// # Why it takes the version as well as the params
//
// The two bound constructors split by algorithm: NewSoftwareStrategyWithParams
// is Argon2id, NewSoftwareStrategyFIPS is PBKDF2. Mirroring that split here
// would force a consumer that is merely COPYING another strategy's settings to
// branch on the algorithm first — and a consumer that guessed wrong would
// compile everywhere and fail at runtime on the FIPS leg only. Taking
// (version, params) means KDFParams() and this constructor compose directly:
//
//	replacement, err := NewDeferredSoftwareStrategyWithParams(old.KDFVersion(), old.KDFParams())
//
// which is the whole point, and which cannot pick the wrong algorithm.
//
// # FIPS behaviour, which is the bound constructors' exactly
//
// Like NewSoftwareStrategyWithParams, this EXISTS IN EVERY BUILD including
// -tags fips: the exported surface of a Layer 1 package must not move with a
// build tag, or a consumer guarding the call behind its own runtime check fails
// to COMPILE over a line it never executes.
//
// Which error you get depends on which FIPS you mean, and both are the same
// mechanism the bound constructors use:
//
//   - In a -tags fips BUILD there is no Argon2id deriver (kdf_argon2.go is
//     excluded), so KDFArgon2id is absent from kdfDispatchers and returns
//     ErrKDFUnsupportedVersion. Absence of the algorithm refuses the call, not a
//     policy branch.
//   - With FIPS active at RUNTIME (GOFIPS140, fips140.Enabled) in a build that
//     does have the deriver, KDFArgon2id returns ErrKDFNotFIPSApproved.
//
// KDFPbkdf2SHA256 is accepted in both.
//
// # Work-factor floors are NOT checked here, deliberately
//
// newSoftwareStrategy does not check them either; DeriveKey does, on every
// derivation (see validateKDFParams). Validating at construction here alone
// would make this constructor stricter than the three beside it, so a consumer
// copying params off a bound strategy could be refused a configuration that
// strategy was allowed to hold. Nothing escapes the floor either way — a weak
// factor still cannot derive.
//
// The returned strategy takes its passphrase from Credentials.Secret on every
// SealRootKey and UnsealRootKey call, exactly as NewDeferredSoftwareStrategy's
// does. Note that a blob sealed at a non-default work factor can only be
// unsealed by a strategy carrying the SAME factor, so preserving it is a
// correctness requirement and not only a hardening one.
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

func newSoftwareStrategy(ctx context.Context, passphrase []byte, version KDFVersion, params KDFParams) (*SoftwareStrategy, error) {
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
	// A COPY, because this constructor does not own the caller's slice: the
	// public constructors take a passphrase the caller may still be using, and
	// enclave.NewSealed wipes what it is handed. The copy is what gets wiped;
	// the caller's original stays the caller's to wipe, which is the contract
	// those constructors have always had. NewCredentials is the shape that
	// consumes instead, and is what callers with nothing else to do with the
	// bytes should reach for.
	p := make([]byte, len(passphrase))
	copy(p, passphrase)
	// Unwrapped, like every other refusal this constructor reports
	// (ErrInvalidPassphrase, ErrKDFUnsupportedVersion, ErrKDFNotFIPSApproved) and
	// for the reason NewCredentials gives: go-enclave's error names its own
	// origin, and a constructor failing is not a seal failing.
	sealed, err := enclave.NewSealed(ctx, p)
	if err != nil {
		return nil, err
	}
	s := &SoftwareStrategy{
		kdfVersion: version,
		kdfParams:  params,
		randReader: rand.Reader,
	}
	s.passphrase.Store(sealed)
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

// KDFParams returns the key-derivation WORK FACTORS this strategy derives with.
// KDFVersion above names the algorithm; this is the cost.
//
// It exists so a consumer replacing a strategy can PRESERVE the work factor it
// finds rather than resetting it. A consumer that installs a replacement
// strategy on every password change would otherwise silently discard the tuned
// factor of a hardened deployment — drifting toward the WEAKER setting, on the
// credential that wraps the root key. Pair it with
// NewDeferredSoftwareStrategyWithParams, which is the other half: this reads
// the factor off the old strategy, that carries it into the new one. Either
// alone is insufficient.
//
// The work factor is not recoverable any other way. A sealed blob records only
// the KDF VERSION byte, never the params — see unsealKDFVersionAndParams, whose
// contract is that "a key sealed with non-default params can only be unsealed by
// a strategy configured with matching params". So the live strategy object is
// the only place the number exists, and this is the only way to read it.
//
// KDFParams is a plain value struct, so the caller gets a copy and cannot mutate
// the strategy through it.
func (s *SoftwareStrategy) KDFParams() KDFParams { return s.kdfParams }

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
// passphrase for the lifetime of the process — a silent authentication bypass.
// Without this accessor the mode is invisible and the replacement is a coin flip.
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
// When deferPassphrase is false (default), creds is unused; the passphrase is
// the one bound at construction. When deferPassphrase is true, the passphrase is
// creds.Secret, and a zero credential is ErrEmptyCredentials. ctx is handed to
// go-enclave when the passphrase enclave is opened, which is the one step on
// this path that can reach a sealing provider; an ended context fails the call
// before the KDF runs.
//
// Ciphertext format: [KDFSalt:32 | Nonce:12 | EncryptedRootKey | Tag:16]
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
func (s *SoftwareStrategy) sealRootKeyWith(ctx context.Context, passphrase *enclave.Sealed, rootKey []byte) (*SealedRootKey, error) {
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
// wrappingGCMNonceSize bytes of its own output, which is the position the
// layout above names.
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
// When deferPassphrase is false (default), creds is unused; the passphrase is
// the one bound at construction. When deferPassphrase is true, the passphrase is
// creds.Secret, and a zero credential is ErrEmptyCredentials. ctx is handed to
// go-enclave when the passphrase enclave is opened, which is the one step on
// this path that can reach a sealing provider; an ended context fails the call
// before the KDF runs.
//
// Expected ciphertext format: [KDFSalt:32 | Nonce:12 | EncryptedRootKey | Tag:16]
func (s *SoftwareStrategy) UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	passphrase, err := s.resolvePassphrase(creds, unsealStrategyError)
	if err != nil {
		return nil, err
	}
	return s.unsealRootKeyWith(ctx, passphrase, sealed)
}

// unsealCipher validates a sealed blob and prepares the AEAD that opens it,
// under the passphrase its caller resolved,
// returning the cipher alongside the [Nonce | EncryptedRootKey | Tag] payload
// it applies to. It is everything unsealing does BEFORE the plaintext exists,
// split out so the []byte and protected paths cannot drift in what they
// validate — and so the protected path can prepare its cipher before it takes
// an enclave borrow, which is the ordering wrappingCipher explains.
func (s *SoftwareStrategy) unsealCipher(ctx context.Context, passphrase *enclave.Sealed, sealed *SealedRootKey) (cipher.AEAD, []byte, error) {
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
	aead, err := wrappingCipher(ctx, passphrase, salt, version, params, unsealStrategyError)
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

// unsealRootKeyWith performs the unseal operation under an already-resolved
// passphrase, returning the root key as an ordinary slice the caller must wipe.
// The protected sibling is UnsealRootKeyProtected.
func (s *SoftwareStrategy) unsealRootKeyWith(ctx context.Context, passphrase *enclave.Sealed, sealed *SealedRootKey) ([]byte, error) {
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
// the PLAINTEXT passphrase for the life of the process — a whole locked page that
// is never released, charged against RLIMIT_MEMLOCK for as long as the process
// runs, which degrades every later locked allocation in the process.
//
// It is a separate step, rather than inline in the seal/unseal bodies, because
// the protected paths run their key-touching half inside a enclave.Memory
// borrow. go-enclave borrows take only the borrowed enclave's own read lock, so
// opening the passphrase inside that borrow would be legal — but preparing the
// cipher first keeps the key-touching window as short as it can be, and the
// AEAD holds only the expanded key schedule in ordinary memory, which the
// borrow may touch freely.
func wrappingCipher(ctx context.Context, passphrase *enclave.Sealed, salt []byte, version KDFVersion, params KDFParams, wrap func(error) error) (cipher.AEAD, error) {
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

// resolvePassphrase resolves which sealed enclave holds the passphrase for this
// call: the credentials' for a deferred strategy, the one bound at construction
// otherwise. Neither is opened here and neither is ever copied out — the
// returned pointer is handed straight to wrappingCipher.
//
// EVERY root-key method goes through this. creds.Secret arrives already
// protected, so the deferred and bound paths differ only in which pointer they
// read; no per-call inner strategy holding a second copy of the passphrase is
// ever built.
//
// Consequences a consumer can observe, named here so none is a surprise:
//
//   - A deferred strategy with a FIPS-unapproved KDF is refused at DeriveKey,
//     on the seal or unseal call, rather than at construction. All four
//     root-key methods agree on this.
//   - The deferred path draws its KDF salt from THIS strategy's randReader.
//     Nothing in production sets randReader; the default is crypto/rand.Reader
//     on every constructor.
//   - warnIfPassphraseWeak fires ONCE, at NewCredentials, rather than on every
//     deferred seal and unseal. The warning is about the credential, not the
//     operation, and the point at which an operator can act on it is the point
//     at which the credential is built.
//
// ONE ROUGH EDGE, stated rather than papered over: a credential the caller has
// already Destroyed reaches wrappingCipher, whose Open fails with
// ErrEnclaveDestroyed, which closedIfEnclaveGone reports as ErrStrategyClosed —
// naming the strategy for a fault in the caller's credential. Liveness is not
// checked here because checking it would not fix it: the caller owns the
// credential and can destroy it between any check and the Open. Distinguishing
// the two would mean threading the passphrase's PROVENANCE into that mapping,
// which is a new public error for a case a caller reaches only by using a
// credential it has already released. It is uniform across all four root-key
// methods.
func (s *SoftwareStrategy) resolvePassphrase(creds Credentials, wrap func(error) error) (*enclave.Sealed, error) {
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
func (s *SoftwareStrategy) SealRootKeyProtected(ctx context.Context, rootKey *enclave.Memory, creds Credentials) (*SealedRootKey, error) {
	passphrase, err := s.resolvePassphrase(creds, sealStrategyError)
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

	aead, err := wrappingCipher(ctx, passphrase, salt, s.kdfVersion, s.kdfParams, sealStrategyError)
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
func (s *SoftwareStrategy) UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (*enclave.Memory, error) {
	passphrase, err := s.resolvePassphrase(creds, unsealStrategyError)
	if err != nil {
		return nil, err
	}

	aead, rest, err := s.unsealCipher(ctx, passphrase, sealed)
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
	// way to demand an arbitrary amount of a finite process-wide resource,
	// before the tag that would have rejected the blob is ever checked —
	// locked memory denied to every other caller.
	//
	// Pinning it to rootKeySize rather than merely bounding it costs nothing:
	// both seal paths refuse anything but a 32-byte key, so no blob this
	// strategy writes has another length, and Barrier.Unseal refuses
	// one that does immediately after the string-typed call returns.
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
	failed := func(err error) (*enclave.Memory, error) {
		releaseEnclave(out, "unsealed root key")
		return nil, &UnsealError{Strategy: passphraseStrategyName, Err: err}
	}
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
			enclave.Wipe(plaintext)
			return ErrDecryptionFailed
		}
		return nil
	}); err != nil {
		return failed(err)
	}

	// FROZEN once the material is final: the contract hands the caller a key to
	// read and destroy, not a buffer to edit, and a read-only region faults on a
	// stray write instead of silently corrupting the root key. Freeze mprotects
	// the region this decrypt already wrote into: one region, no copy.
	//
	// A refused freeze leaves the enclave ALIVE and WRITABLE, so it is destroyed
	// rather than returned: handing back a writable enclave from a method whose
	// documentation says the contents are final would be reporting a guarantee
	// that was not established.
	if ferr := out.Freeze(); ferr != nil {
		return failed(ferr)
	}
	return out, nil
}

// closedIfEnclaveGone maps a destroyed passphrase enclave to ErrStrategyClosed.
// Close destroys the enclave.Sealed, so a Close that lands after a seal or unseal
// has already loaded the pointer surfaces as ErrEnclaveDestroyed from Open; that
// is the same condition as "used after Close" and must report the same error.
//
// ErrEnclaveDestroyed alone does not prove that, so the passphrase is asked.
// Since go-enclave v0.7 an Open can also fail with ErrEnclaveDestroyed when a
// concurrent enclave.WipeAll destroys one of the open's PER-CALL enclaves (its
// destination or its DEK) while the passphrase envelope itself is intact. That
// is a shutdown sweep taking the operation's working key, not this strategy
// being closed, and it is returned as ErrEnclaveDestroyed rather than named as
// a Close that never happened. Both answers fail the call; only the label
// differs.
//
// For a deferred strategy the passphrase is the caller's credential, so a
// credential the caller destroyed still reports ErrStrategyClosed — the rough
// edge resolvePassphrase documents.
func closedIfEnclaveGone(passphrase *enclave.Sealed, err error) error {
	if errors.Is(err, enclave.ErrEnclaveDestroyed) && !passphrase.IsAlive() {
		return ErrStrategyClosed
	}
	return err
}

// Close releases strategy-held resources by destroying the passphrase enclave,
// after which the passphrase can no longer be opened.
//
// enclave.Sealed.Destroy zeroes both the ciphertext and the wrapped-key
// blob before releasing them, so Close is erasure rather than revocation.
//
// This method is idempotent and safe to call concurrently with Seal/Unseal, and
// every outcome is fail-closed. go-enclave's Sealed.Destroy does NOT wait
// for an Open in flight. An operation whose Open has already pinned the envelope
// completes against it — it is ordered before the Destroy — and the ciphertext
// is wiped by whichever of Destroy or the last such Open finishes last. An
// operation that starts after the swap sees a nil pointer and fails with
// ErrStrategyClosed.
//
// Between those two lies a window: resolvePassphrase loads the pointer and
// wrappingCipher Opens it later, so an operation can hold a live pointer and
// still find the envelope destroyed by the time its Open runs. That Open
// returns ErrEnclaveDestroyed, which closedIfEnclaveGone reports as
// ErrStrategyClosed.
//
// PINNING the envelope inside Open is the line, not loading the pointer.
// Nothing opens a passphrase whose Destroy was ordered first, and nothing
// stays readable after the last open out: the plaintext each Open returned is
// the operation's own enclave, released by wrappingCipher before it returns.
//
// For deferred strategies (where the passphrase arrives in the credentials)
// Close is a no-op, and the credential is the caller's to Destroy.
//
// Close returns the passphrase Destroy's error, which go-enclave documents as
// always nil today.
func (s *SoftwareStrategy) Close() error {
	if passphrase := s.passphrase.Swap(nil); passphrase != nil {
		return passphrase.Destroy()
	}
	return nil
}
