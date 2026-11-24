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

// Package seal provides the barrier: a root key, HKDF-SHA256 DEKs and AES-256-GCM at-rest encryption, with passphrase and none sealing strategies; a strategy's BarrierEncryptor takes over the whole data path.
package seal

import (
	"context"
	"time"

	"github.com/jeremyhahn/go-enclave"
)

// StrategyID uniquely identifies a sealing strategy.
type StrategyID string

// Strategy identifiers implemented here; other strategies are named by go-xkms and go-qrdb.
const (
	// StrategyPassphrase uses password-based encryption (Argon2id/PBKDF2 + AEAD).
	StrategyPassphrase StrategyID = "passphrase"

	// StrategyNone disables barrier encryption entirely; data is stored in
	// plaintext. Intended for development, testing, or deployments that rely
	// on OS-level full-disk encryption. Selected explicitly; never auto-selected.
	StrategyNone StrategyID = "none"
)

// Credentials holds a strategy's authentication Secret in protected memory; the zero value means none, and the caller owns and destroys Secret.
type Credentials struct {
	// Secret is the authentication material, encrypted at rest in memory. Nil
	// means the caller supplied none.
	Secret enclave.Sealed // #nosec G117 -- this IS a secret field by design
}

// NewCredentials moves secret into protected memory and wipes it, refusing an empty secret with ErrInvalidPassphrase; an ended ctx fails with enclave.ErrSealFailed.
func NewCredentials(ctx context.Context, secret []byte) (Credentials, error) {
	if len(secret) == 0 {
		return Credentials{}, ErrInvalidPassphrase
	}
	// Warn at construction, while the length is known.
	warnIfPassphraseWeak(len(secret))
	// go-enclave's error is returned unwrapped; it names its own origin.
	sealed, err := enclave.NewSealed(ctx, secret)
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Secret: sealed}, nil
}

// SealingStrategy wraps and unwraps the root key; whether it honours creds is implementation-defined, so a replacement must keep the mode of the strategy it replaces.
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

	// UnsealRootKey decrypts the root key, which the caller must zero; a nil error does not prove creds were correct.
	UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error)

	// BarrierEncryptor returns a hardware SymmetricEncrypter, or (nil, nil) for epoch-based HKDF DEKs; it implements AADSymmetricEncrypter only if it authenticates AAD.
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

	// LoadGeneration counts barrier loads of this blob; a barrier seeing a higher value than it claimed fails with ErrBarrierSuperseded. Covered by StateMAC.
	LoadGeneration uint64 `json:"load_generation,omitempty"`

	// FailedAttempts is the number of consecutive failed Unseal attempts, persisted for brute-force backoff; 0 means none.
	FailedAttempts int64 `json:"failed_attempts,omitempty"`

	// LastFailedUnixNano is the time of the last failed Unseal; 0 means none.
	LastFailedUnixNano int64 `json:"last_failed_unix_nano,omitempty"`

	// KDFVersion identifies the KDF algorithm used when sealing. Allows
	// unseal to select the correct KDF regardless of the current FIPS mode.
	KDFVersion int `json:"kdf_version,omitempty"`

	// HKDFVersion identifies the info-string layout used to derive epoch
	// DEKs from the root key. Stamped by Initialize; a zero value means the
	// blob predates the field and must derive with the v1 layout.
	HKDFVersion HKDFVersion `json:"hkdf_version,omitempty"`

	// Algorithm is the data-path AEAD the epoch DEKs use; SetSealedRootKey refuses a mismatch with AlgorithmMismatchError, and zero means unrecorded.
	Algorithm SymmetricAlgorithm `json:"algorithm,omitempty"`

	// StateMAC is HMAC-SHA256 under a root-key-derived key over the blob's metadata (not the backoff fields); see Barrier.stateMAC and BarrierConfig.MinEpoch.
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
