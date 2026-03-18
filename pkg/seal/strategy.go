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
// Sealing strategies abstract the key-wrapping mechanism. The built-in SoftwareStrategy
// uses Argon2id + AES-GCM for passphrase-based protection. Optional strategies for
// PKCS#11, TPM2, and cloud KMS are available in the quicraft/crypto/xkms module.
package seal

import (
	"context"
	"time"
)

// StrategyID uniquely identifies a sealing strategy.
type StrategyID string

// Strategy identifiers for supported sealing backends.
const (
	StrategyTPM2     StrategyID = "tpm2"
	StrategyPKCS11   StrategyID = "pkcs11"
	StrategyAWSKMS   StrategyID = "awskms"
	StrategyGCPKMS   StrategyID = "gcpkms"
	StrategyAzureKV  StrategyID = "azurekv"
	StrategyVault    StrategyID = "vault"
	StrategyShamir   StrategyID = "shamir"
	StrategySoftware StrategyID = "software"
)

// DefaultPreferenceOrder defines the priority order for selecting a sealing
// strategy when multiple strategies are available. Hardware-backed strategies
// are preferred over software-only strategies.
var DefaultPreferenceOrder = []StrategyID{
	StrategyTPM2, StrategyPKCS11, StrategyAWSKMS, StrategyGCPKMS,
	StrategyAzureKV, StrategyVault, StrategyShamir, StrategySoftware,
}

// Credentials holds authentication material for sealing strategies. The Secret
// field is interpreted differently by each strategy: the software strategy treats
// it as an Argon2id passphrase, TPM2 may use it as an authorization value, and
// cloud KMS strategies typically ignore it (relying on ambient credentials).
type Credentials struct {
	Secret string // #nosec G117 -- this IS a secret field by design
}

// SealingStrategy defines the interface for wrapping and unwrapping the root key.
// Implementations protect the root key at rest using different backends:
// software (passphrase + Argon2id), PKCS#11 (HSM), TPM2, or cloud KMS.
type SealingStrategy interface {
	// ID returns a unique identifier for this strategy (e.g., StrategySoftware,
	// StrategyTPM2, StrategyPKCS11, StrategyAWSKMS).
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
	UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error)

	// BarrierEncryptor returns a SymmetricEncrypter for hardware-backed strategies
	// where keys should never leave the hardware boundary. Software strategies
	// return (nil, nil) to indicate the barrier should use epoch-based HKDF DEKs.
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
