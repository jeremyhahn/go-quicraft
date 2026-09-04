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

// Package seal provides encryption primitives for the QuicRaft barrier/seal/unseal
// pattern. The Barrier holds a root key, derives data encryption keys (DEKs) via
// HKDF-SHA256, and provides AES-256-GCM encrypt/decrypt operations for all at-rest data.
//
// Sealing strategies abstract the key-wrapping mechanism. The built-in software
// (passphrase) strategy uses Argon2id + AES-GCM. Hardware/cloud strategies
// (PKCS#11, TPM2, AWS/GCP/Azure KMS, Vault) live in go-xkms; the Shamir
// strategy lives in go-qrdb. This package defines only StrategyPassphrase and
// StrategyNone.
//
// # Delegated (hardware) mode
//
// When a strategy returns a BarrierEncryptor, the barrier hands that encryptor
// the WHOLE data path and keeps none of its own: epoch DEK derivation, the
// [epoch][algID] ciphertext header, the nonce-reuse tracker and the NIST
// SP 800-38D invocation counter are all bypassed, because the delegated
// encryptor owns its key and nonce management and the barrier cannot count
// invocations against a key it does not control. Rotate and PurgeEpochsBefore
// return ErrHardwareMode for the same reason. Every barrier setup logs this at
// WARN, naming what is delegated.
//
// A delegated encryptor authenticates the caller's positional context only if it
// implements AADSymmetricEncrypter; otherwise the barrier's *Context methods
// fail closed with ErrHardwareMode rather than drop the binding. Callers ask
// Barrier.SupportsContextAAD before building a context, and a deployment that
// requires the binding sets BarrierConfig.RequireContextAAD so an incapable
// encryptor — or the no-encryption strategy — is refused at setup instead of at
// the first write.
package seal

import (
	"context"
	"time"

	"github.com/jeremyhahn/go-enclave"
)

// StrategyID uniquely identifies a sealing strategy.
type StrategyID string

// Strategy identifiers for the strategies go-quicraft (Layer 1) actually
// implements. The consensus engine intentionally does NOT name strategies it
// does not implement: hardware/cloud strategy IDs (tpm2/pkcs11/awskms/gcpkms/
// azurekv/vault) are owned by go-xkms, and shamir by go-qrdb (which owns the
// StrategyID namespace + barrier registry). There is no implicit
// preference-order default; strategy selection is always explicit.
const (
	// StrategyPassphrase uses password-based encryption (Argon2id/PBKDF2 + AEAD).
	StrategyPassphrase StrategyID = "passphrase"

	// StrategyNone disables barrier encryption entirely; data is stored in
	// plaintext. Intended for development, testing, or deployments that rely
	// on OS-level full-disk encryption. Selected explicitly; never auto-selected.
	StrategyNone StrategyID = "none"
)

// Credentials holds authentication material for sealing strategies. The Secret
// is interpreted differently by each strategy: the software strategy treats it
// as an Argon2id (or PBKDF2) passphrase, TPM2 may use it as an authorization
// value, and cloud KMS strategies typically ignore it, relying on ambient
// credentials.
//
// # Secret is protected memory, and that is the whole point (#41)
//
// It used to be a []byte, documented as "wipeable from this boundary inward":
// the seal layer copied it into protected memory and the caller's original
// stayed the caller's obligation. Two things were wrong with that. The
// passphrase spent the whole call in collectable, swappable, core-dumpable
// memory — and for a deferred strategy the passphrase is not a lesser secret
// than the root key it wraps, it IS that key's protection. And an obligation
// written in a doc comment is weaker than a type that cannot be got wrong: every
// strategy in the portfolio implements against this struct, so every one of them
// had to remember.
//
// A [enclave.SealedEnclave] holds the passphrase as CIPHERTEXT at rest and
// produces plaintext only inside an Open, which is the posture this material
// wants: a credential is consulted once per seal or unseal and held for a long
// time in between. A locked-plaintext [enclave.MemoryEnclave] would keep it
// present-but-locked continuously and buy less.
//
// Build one with [NewCredentials], which CONSUMES the caller's slice — that is
// the "wipeable from this boundary inward" contract made mechanical, since the
// only thing left worth doing about material that already exists unprotected is
// not leaving it there.
//
// The ZERO VALUE is valid and means "no credentials", which is what a
// passphrase-bound or ambient-credential strategy expects; it ignores this
// entirely. The CALLER owns Secret: this package opens it, derives from it and
// destroys the opened copy, and never destroys the SealedEnclave itself, so one
// credential serves many calls and is released by the caller's own Destroy.
type Credentials struct {
	// Secret is the authentication material, encrypted at rest in memory. Nil
	// means the caller supplied none.
	Secret *enclave.SealedEnclave // #nosec G117 -- this IS a secret field by design
}

// NewCredentials moves a secret into protected memory and returns the
// credentials holding it. The source slice is WIPED — that is the whole point,
// and it is why this constructor exists rather than leaving callers to build the
// struct: material that reaches here already existed unprotected, and the only
// thing left worth doing about it is not leaving it there.
//
// An empty secret is refused rather than stored. A zero-length passphrase is not
// a passphrase, and the sealer refuses one in any case: go-enclave answers
// zero-length material with ErrEmptyEnclave. Refusing it here names the actual
// fault (ErrInvalidPassphrase) at the point the caller can act on, rather than
// surfacing a construction error two layers down.
func NewCredentials(secret []byte) (Credentials, error) {
	if len(secret) == 0 {
		return Credentials{}, ErrInvalidPassphrase
	}
	// Warned about HERE, at construction, exactly as newSoftwareStrategy warns
	// for a bound passphrase: the earliest point an operator can act on, and the
	// last point at which the length is known without opening the enclave again.
	warnIfPassphraseWeak(len(secret))
	// go-enclave's error is returned UNWRAPPED, for the reason wrappingCipher
	// gives for newAESGCM's: it already names its own origin — a
	// *LockedMemoryError matching ErrLockedMemoryUnavailable, or ErrSealFailed —
	// and it is not a strategy-level failure. Wrapping it in a *seal.Error would
	// print "seal failed (strategy=passphrase)" over a refused mlock, which is
	// both untrue and specific to a strategy this constructor knows nothing
	// about: Credentials is what a TPM2 or KMS strategy takes too.
	sealed, err := enclave.NewSealed(secret)
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Secret: sealed}, nil
}

// SealingStrategy defines the interface for wrapping and unwrapping the root key.
// Implementations protect the root key at rest using different backends:
// software (passphrase + Argon2id), PKCS#11 (HSM), TPM2, or cloud KMS.
//
// WHETHER AN IMPLEMENTATION HONOURS creds IS IMPLEMENTATION-DEFINED, and it is
// part of the deployment's authentication posture rather than an internal
// detail. A cloud-KMS strategy typically ignores Credentials and relies on
// ambient credentials; SoftwareStrategy honours them only when constructed by
// NewDeferredSoftwareStrategy, and ignores them when the passphrase was bound
// at construction.
//
// So REPLACING a registered strategy with a differently-constructed one — when
// rotating a passphrase, for instance — can silently change whether the barrier
// authenticates the caller that unseals it, with no signature, error or type
// change to notice. Preserve the mode of the strategy being replaced rather
// than picking one; SoftwareStrategy exports DeferredPassphrase() so the mode
// can be read off the strategy in hand. Where a passphrase can be rotated
// without swapping the strategy, that is the safer path.
type SealingStrategy interface {
	// ID returns a unique identifier for this strategy (e.g., StrategyPassphrase
	// or StrategyNone for the L1 strategies; higher layers define their own IDs
	// such as "tpm2"/"shamir").
	ID() StrategyID

	// Available reports whether this strategy can be used on the current
	// system. For example, a TPM2 strategy returns false if no TPM device
	// is accessible.
	Available() bool

	// HardwareBacked reports whether this strategy uses a hardware security
	// module or external KMS for key protection.
	HardwareBacked() bool

	// SealRootKey encrypts the root key and returns a SealedRootKey that can
	// be persisted to stable storage.
	SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error)

	// UnsealRootKey decrypts the root key from a SealedRootKey. The returned
	// byte slice is owned by the caller and must be zeroed after use.
	//
	// An implementation that ignores creds unseals successfully for ANY
	// credentials, including wrong ones; see the credential note on
	// SealingStrategy. A caller must not read a nil error from this method as
	// proof that the supplied credentials were correct.
	UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error)

	// BarrierEncryptor returns a SymmetricEncrypter for hardware-backed strategies
	// where keys should never leave the hardware boundary. Software strategies
	// return (nil, nil) to indicate the barrier should use epoch-based HKDF DEKs.
	//
	// The returned encryptor should ALSO implement AADSymmetricEncrypter when it
	// can authenticate caller-supplied AAD end to end; that is what lets the
	// barrier keep the positional binding of snapshot frames and WAL records in
	// delegated mode. It must NOT implement that interface if it would ignore
	// the AAD.
	BarrierEncryptor(ctx context.Context, rootKey []byte) (SymmetricEncrypter, error)

	// Close releases strategy-held resources (PKCS#11 sessions, TPM2 handles,
	// KMS client connections). Implementations must be idempotent.
	Close() error
}

// SealedRootKey is the persisted form of an encrypted root key. It contains all
// metadata required to unseal the key using the appropriate strategy.
type SealedRootKey struct {
	// Strategy identifies which sealing strategy produced this sealed key.
	Strategy StrategyID `json:"strategy"`

	// Version is the sealed root key format version for forward compatibility.
	Version int `json:"version,omitempty"`

	// Epoch is the key rotation epoch at the time of sealing.
	Epoch uint64 `json:"epoch,omitempty"`

	// Salt is a 32-byte random value generated during Initialize and used
	// for HKDF key derivation. The salt is NOT secret.
	Salt []byte `json:"salt,omitempty"`

	// Ciphertext is the encrypted root key produced by the sealing strategy.
	Ciphertext []byte `json:"ciphertext,omitempty"`

	// Metadata holds strategy-specific information (PKCS#11 key ID, KMS ARN, etc.).
	Metadata []byte `json:"metadata,omitempty"`

	// KnownEpochs lists all epoch numbers whose DEKs should be re-derived
	// on unseal. Populated by Initialize and Rotate, persisted by the caller.
	KnownEpochs []uint64 `json:"known_epochs,omitempty"`

	// LoadGeneration counts how many times this blob has been claimed by a
	// barrier. Initialize stamps 1; every subsequent Unseal that can persist
	// increments it and writes it back BEFORE the barrier will encrypt.
	//
	// It is the load-generation fence, and it exists because nothing else in
	// this package can tell a barrier that another process has taken its sealed
	// state. Every input to the epoch DEK — root key, salt, deployment ID,
	// epoch — is a function of this blob, and Unseal's forced rotation moves to
	// epoch+1, so two barriers loaded from one blob derive the SAME DEK, each
	// with a zeroed invocation counter and an empty nonce tracker: n writers
	// spending the full NIST SP 800-38D budget against one key, neither able to
	// see the other's nonces. The generation makes that state OBSERVABLE — a
	// barrier handed a blob whose generation is above the one it claimed knows
	// another loader has been here and fails closed (ErrBarrierSuperseded).
	//
	// WHAT IT DOES NOT DO: it is not a lock and not a lease. It is checked when
	// state is loaded (SetSealedRootKey) and enforced when state is written
	// (Rotate, PurgeEpochsBefore) or used (Encrypt); the barrier never re-reads
	// the persisted blob on its own, so a barrier that is superseded while
	// running keeps encrypting until something hands it the newer state. See
	// Barrier.SetSealedRootKey for the full enforcement contract.
	//
	// Covered by StateMAC: a barrier refuses to encrypt on the strength of this
	// number, so leaving it unauthenticated would hand anyone with write access
	// to the blob a way to stop a node from writing by inflating it.
	LoadGeneration uint64 `json:"load_generation,omitempty"`

	// FailedAttempts is the number of consecutive failed Unseal attempts.
	// Persisted alongside the sealed key so that brute-force backoff state
	// survives process restarts. A value of 0 indicates no recent failures.
	// Added in v2; zero value preserves backward compatibility with v1 data.
	FailedAttempts int64 `json:"failed_attempts,omitempty"`

	// LastFailedUnixNano is the wall-clock time (UnixNano) of the most recent
	// failed Unseal attempt. Used to compute brute-force backoff delay on
	// restart. A value of 0 means no failure has been recorded.
	// Added in v2; zero value preserves backward compatibility with v1 data.
	LastFailedUnixNano int64 `json:"last_failed_unix_nano,omitempty"`

	// KDFVersion identifies the KDF algorithm used when sealing. Allows
	// unseal to select the correct KDF regardless of the current FIPS mode.
	KDFVersion int `json:"kdf_version,omitempty"`

	// HKDFVersion identifies the info-string layout used to derive epoch
	// DEKs from the root key. Stamped by Initialize; a zero value means the
	// blob predates the field and must derive with the v1 layout.
	HKDFVersion HKDFVersion `json:"hkdf_version,omitempty"`

	// Algorithm records the data-path AEAD this deployment's epoch DEKs were
	// derived for and encrypted with. Stamped by Initialize from
	// BarrierConfig.Algorithm; SetSealedRootKey rejects a blob whose Algorithm
	// disagrees with the barrier's configuration (AlgorithmMismatchError).
	//
	// It is recorded because the algorithm is NOT recoverable from the data:
	// under HKDFV2 it is an HKDF domain separator, under HKDFV1 it is not
	// bound at all (so the identical DEK would be used as an AES-256-GCM key
	// on one host and a ChaCha20-Poly1305 key on another), and Decrypt selects
	// the AEAD by EPOCH rather than from the ciphertext header's algID. Without
	// this field, a data directory restored onto a host whose CPU makes
	// SelectOptimalAlgorithm choose differently reads back as ErrDecryptionFailed
	// with nothing on disk naming the algorithm that would work.
	//
	// A zero value means "unrecorded" — a blob written before this field
	// existed. It loads without complaint under whatever algorithm the barrier
	// is configured with, which is exactly the pre-existing behavior; such a
	// deployment keeps carrying the original risk and should pin
	// BarrierConfig.Algorithm explicitly rather than rely on auto-selection.
	Algorithm SymmetricAlgorithm `json:"algorithm,omitempty"`

	// StateMAC authenticates this blob's METADATA under the root key it
	// describes. It is HMAC-SHA256 over a canonical, length-prefixed encoding
	// of (Strategy, Version, Epoch, Salt, KnownEpochs, HKDFVersion, Algorithm,
	// DeploymentID), keyed by a key HKDF'd from the root key; see
	// Barrier.stateMAC.
	//
	// It exists because the wrapping AEAD covers only Ciphertext — its AAD is
	// the constant strategy name — so every other field is plaintext beside it
	// and bound to nothing. The epoch is the field that matters: rewriting it
	// downward resurrects an already-spent DEK with a freshly zeroed invocation
	// counter and an empty nonce tracker, giving two independent random-nonce
	// sequences under one key.
	//
	// Stamped by Initialize, Rotate and PurgeEpochsBefore (every operation that
	// changes the covered fields), verified by Unseal once the root key is
	// recovered. NOT covered: FailedAttempts and LastFailedUnixNano, which are
	// written by persistBackoffState on a FAILED unseal, where no root key is
	// available to re-MAC with.
	//
	// A MAC cannot detect REPLAY of an older, genuinely authenticated blob; see
	// BarrierConfig.MinEpoch for that half.
	StateMAC []byte `json:"state_mac,omitempty"`

	// Nonce is the AES-GCM nonce used during sealing, when stored separately
	// from the ciphertext (hardware strategies may store nonce externally).
	Nonce []byte `json:"nonce,omitempty"`

	// ShamirThreshold is the minimum number of shares required to reconstruct
	// the root key when Shamir secret sharing is used.
	ShamirThreshold int `json:"shamir_threshold,omitempty"`

	// ShamirTotal is the total number of shares generated.
	ShamirTotal int `json:"shamir_total,omitempty"`

	// HardwarePayload holds opaque data for hardware-backed strategies
	// (e.g., TPM2 sealed blob, PKCS#11 key handle metadata).
	HardwarePayload []byte `json:"hardware_payload,omitempty"`

	// CreatedAt is the time the sealed root key was created.
	CreatedAt time.Time `json:"created_at,omitempty"`
}
