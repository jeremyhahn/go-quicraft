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

// BarrierConfig configures the algorithm-agile epoch barrier.
type BarrierConfig struct {
	// Algorithm selects the symmetric encryption algorithm used for barrier
	// data encryption. Default: auto-selected via SelectOptimalAlgorithm().
	Algorithm SymmetricAlgorithm

	// DeploymentID is bound into the HKDF info parameter to ensure that two
	// clusters sharing the same root key produce independent DEKs.
	DeploymentID uint64

	// NonceTracking enables per-nonce reuse detection. Default: true.
	// Disable only if you need to trade memory for extreme throughput and
	// can guarantee nonce uniqueness through other means.
	NonceTracking bool

	// InvocationLimit is the max encrypt operations per key epoch.
	// Default: 2^32 (NIST SP 800-38D for random 96-bit nonces).
	// Set to 0 for default; -1 to disable.
	InvocationLimit int64

	// RotationThreshold is the percentage of InvocationLimit at which
	// ShouldRotate() returns true. Default: 95.0.
	RotationThreshold float64

	// InvocationCounter allows injection of a custom counter.
	// Nil = default InMemoryInvocationCounter.
	InvocationCounter InvocationCounter

	// NonceTrackerProvider allows injection of a custom nonce tracker.
	// Nil = default InMemoryNonceTracker.
	NonceTrackerProvider NonceTrackerProvider

	// AEADProvider allows injection of a custom cipher.AEAD factory.
	// Nil = default SoftwareAEADProvider.
	AEADProvider AEADProvider
}

// DefaultBarrierConfig returns a BarrierConfig with sensible defaults:
// auto-selected algorithm, nonce tracking enabled, invocation limit at 2^32.
func DefaultBarrierConfig(deploymentID uint64) BarrierConfig {
	return BarrierConfig{
		Algorithm:     SelectOptimalAlgorithm(false),
		DeploymentID:  deploymentID,
		NonceTracking: true,
	}
}

// validate checks that config values are consistent.
func (c *BarrierConfig) validate() error {
	if !c.Algorithm.Valid() {
		return ErrUnsupportedAlgorithm
	}
	return nil
}
