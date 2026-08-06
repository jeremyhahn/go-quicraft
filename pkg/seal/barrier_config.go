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

// StatePersister durably writes the barrier's sealed root-key state to the
// embedder's storage. The barrier owns no storage of its own: go-qrdb (or
// whichever embedder loads the blob at boot via SetSealedRootKey) owns the
// file/KV record and its fsync.
//
// Contract:
//   - Returning nil MUST mean the blob is durable — it will be read back
//     verbatim after a crash. The barrier activates the new epoch only after
//     a nil return, so a premature nil re-introduces the data-loss window this
//     hook exists to close.
//   - The argument is a private copy; the persister may retain or mutate it.
//   - The persister is invoked with the barrier's write lock HELD. It MUST NOT
//     call back into the same Barrier (Encrypt, Rotate, SealedRootKeyData, …)
//     or it will deadlock.
//   - Returning an error aborts the operation with the previous epoch intact.
type StatePersister func(*SealedRootKey) error

// BarrierConfig configures the algorithm-agile epoch barrier.
type BarrierConfig struct {
	// Algorithm selects the symmetric encryption algorithm used for barrier
	// data encryption. Default: auto-selected via SelectOptimalAlgorithm().
	Algorithm SymmetricAlgorithm

	// DeploymentID is bound into the HKDF info parameter to ensure that two
	// clusters sharing the same root key produce independent DEKs.
	DeploymentID uint64

	// NonceTracking enables per-nonce reuse detection. Default: true.
	//
	// MEMORY: the in-memory tracker retains EVERY unique nonce for the life of
	// the epoch, so its memory grows LINEARLY with the number of encrypt
	// operations. It is therefore inconsistent with riding the default
	// InvocationLimit of 2^32 to its end (that would retain ~4x10^9 nonces, well
	// over 100 GB). When tracking is on, you MUST rotate the epoch far earlier
	// than the invocation limit (the random 96-bit nonce + invocation cap alone
	// already bound reuse probability per NIST SP 800-38D); the tracker is
	// belt-and-suspenders detection for that pre-rotation window, NOT a license
	// to approach the invocation limit. Disable it (and rely on the
	// random-nonce + invocation-cap argument) for extreme-throughput, long-epoch
	// deployments where per-epoch memory must stay flat.
	//
	// The default tracker ENFORCES this rather than only documenting it: past
	// DefaultMaxTrackedNonces it logs at error level and disables reuse
	// detection for the epoch instead of growing until the process is killed.
	// NewBarrier additionally warns when this is on with neither a
	// StatePersister nor an InvocationCounter — the combination that provably
	// cannot rotate, and therefore can never release the tracked set.
	NonceTracking bool

	// InvocationLimit is the max encrypt operations per key epoch.
	// Default: 2^32 (NIST SP 800-38D for random 96-bit nonces).
	// Set to 0 for default; -1 to disable. See NonceTracking: with tracking on,
	// the epoch must rotate well before this limit or the tracker exhausts memory.
	InvocationLimit int64

	// RotationThreshold is the percentage of InvocationLimit at which
	// ShouldRotate() returns true. Default: 95.0.
	RotationThreshold float64

	// StatePersister durably persists the sealed root-key state. It is
	// REQUIRED for Rotate and for any PurgeEpochsBefore that actually purges:
	// both change state that must survive a restart (the active epoch, the
	// KnownEpochs set), and data written under an epoch the persisted blob
	// does not name is undecryptable after a restart. When nil, those
	// operations fail closed with ErrNoStatePersister and the current epoch
	// stays active — writes continue until the hard invocation limit, at
	// which point Encrypt itself fails closed. Initialize does NOT call the
	// persister: the embedder persists SealedRootKeyData() directly at
	// bootstrap.
	StatePersister StatePersister

	// InvocationCounter allows injection of a custom counter.
	// Nil = default InMemoryInvocationCounter.
	//
	// Contract: an injected counter OWNS the persistence of per-epoch
	// invocation counts across process restarts (Seed it from durable
	// state before Unseal). When nil, the barrier cannot know how many
	// AEAD invocations the current epoch has already performed after a
	// restart, so Unseal pessimistically seeds the default counter to
	// the rotation threshold — forcing a prompt key rotation — to keep
	// the NIST SP 800-38D 2^32 budget sound across process lifetimes.
	//
	// CONSEQUENCE — a deployment that leaves this nil MUST both run a
	// rotation loop AND configure StatePersister. The post-unseal seeding
	// makes ShouldRotateKey() true almost immediately after every restart,
	// so an embedder that never rotates burns the remaining headroom
	// between the rotation threshold and InvocationLimit (default 5%) and
	// then fails every Encrypt with the invocation-limit error. Inject a
	// counter that persists its own count to avoid the forced rotation.
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

// Validate reports whether the configuration can be used to construct a
// Barrier, returning ErrUnsupportedAlgorithm for an unknown algorithm and
// ErrAlgorithmNotFIPSApproved when FIPS mode is active and the configured
// cipher is not FIPS-approved.
//
// NewBarrier PANICS on an invalid configuration, which is appropriate for a
// compile-time-constant config but not for one assembled at runtime from a
// file, an API request, or per-tenant settings. Embedders building a config
// dynamically should call Validate first and surface the error, rather than
// letting a bad algorithm take the process down.
func (c *BarrierConfig) Validate() error {
	if !c.Algorithm.Valid() {
		return ErrUnsupportedAlgorithm
	}
	// FIPS 140 mode constrains the data-path cipher: the ChaCha20 family
	// is not FIPS-approved and must be rejected at configuration time
	// rather than silently encrypting a "FIPS" deployment's data with a
	// non-approved cipher.
	if FIPSEnabled() &&
		(c.Algorithm == AlgChaCha20Poly1305 || c.Algorithm == AlgXChaCha20Poly1305) {
		return ErrAlgorithmNotFIPSApproved
	}
	return nil
}
