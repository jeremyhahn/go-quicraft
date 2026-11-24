// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

// StatePersister durably writes the sealed root-key state; nil means durable, it runs under the write lock and must not re-enter the barrier, and only one live barrier may write a blob (the load-generation fence detects most violations).
type StatePersister func(*SealedRootKey) error

// BarrierConfig configures the algorithm-agile epoch barrier.
type BarrierConfig struct {
	// Algorithm selects the data encryption algorithm; pin it explicitly when data outlives the host, since the CPU-derived default is part of key derivation.
	Algorithm SymmetricAlgorithm

	// DeploymentID is bound into the HKDF info parameter to ensure that two
	// clusters sharing the same root key produce independent DEKs.
	DeploymentID uint64

	// NonceTracking enables nonce reuse detection (nil means true); the tracker covers only the first DefaultMaxTrackedNonces encryptions of an epoch, then saturates.
	NonceTracking *bool

	// InvocationLimit is the max encrypt operations per epoch: 0 for 2^32 (NIST SP 800-38D), -1 to disable.
	InvocationLimit int64

	// RotationThreshold is the percentage of InvocationLimit at which
	// ShouldRotate() returns true. Default: 95.0.
	RotationThreshold float64

	// StatePersister is required for Rotate, unseal after a restart, and purging; when nil those fail with ErrNoStatePersister and writes are refused after a restart.
	StatePersister StatePersister

	// InvocationCounter is a custom counter that owns persisting per-epoch counts; nil means each restart rotates to a fresh epoch, so StatePersister is then required.
	InvocationCounter InvocationCounter

	// NonceTrackerProvider allows injection of a custom nonce tracker.
	// Nil = default InMemoryNonceTracker.
	NonceTrackerProvider NonceTrackerProvider

	// AEADProvider allows injection of a custom cipher.AEAD factory.
	// Nil = default SoftwareAEADProvider.
	AEADProvider AEADProvider

	// RequireContextAAD makes Initialize and Unseal fail with ErrContextAADUnsupported unless context binding is available; StrategyNone and plain SymmetricEncrypter are refused.
	RequireContextAAD bool

	// RequireFIPS makes Validate and NewBarrier fail with ErrFIPSUnavailable unless the FIPS 140-3 module is active.
	RequireFIPS bool

	// MinEpoch is the lowest epoch SetSealedRootKey accepts (ErrSealedEpochRollback), a floor against replay; 0 disables it.
	MinEpoch uint64
}

// BoolPtr returns a pointer to v for *bool config fields; duplicated from config.BoolPtr to keep pkg/seal dependency-free.
func BoolPtr(v bool) *bool { return &v }

// GetNonceTracking returns the effective NonceTracking value, true when unset.
func (c *BarrierConfig) GetNonceTracking() bool {
	if c.NonceTracking == nil {
		return true
	}
	return *c.NonceTracking
}

// DefaultBarrierConfig returns defaults: CPU-selected algorithm, nonce tracking on, invocation limit 2^32.
func DefaultBarrierConfig(deploymentID uint64) BarrierConfig {
	return BarrierConfig{
		Algorithm:     SelectOptimalAlgorithm(false),
		DeploymentID:  deploymentID,
		NonceTracking: BoolPtr(true),
	}
}

// Validate reports whether the config can build a Barrier: ErrUnsupportedAlgorithm, ErrAlgorithmNotFIPSApproved or ErrFIPSUnavailable.
func (c *BarrierConfig) Validate() error {
	if !c.Algorithm.Valid() {
		return ErrUnsupportedAlgorithm
	}
	// Check RequireFIPS before the cipher rule.
	if c.RequireFIPS && !FIPSEnabled() {
		return ErrFIPSUnavailable
	}
	// FIPS mode rejects the ChaCha20 family.
	if FIPSEnabled() &&
		(c.Algorithm == AlgChaCha20Poly1305 || c.Algorithm == AlgXChaCha20Poly1305) {
		return ErrAlgorithmNotFIPSApproved
	}
	return nil
}
