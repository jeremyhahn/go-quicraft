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
//
//   - Returning nil MUST mean the blob is durable — it will be read back
//     verbatim after a crash. The barrier activates the new epoch only after
//     a nil return, so a premature nil re-introduces the data-loss window this
//     hook exists to close.
//
//   - The argument is a private copy; the persister may retain or mutate it.
//
//   - The persister is invoked with the barrier's write lock HELD. It MUST NOT
//     call back into the same Barrier (Encrypt, Rotate, SealedRootKeyData, …)
//     or it will deadlock.
//
//   - Returning an error aborts the operation with the previous epoch intact.
//
//   - EXCLUSIVITY: only ONE live barrier may write a given sealed blob. This is
//     a cryptographic requirement, not an operational preference. Every DEK
//     input (root key, salt, deployment ID, epoch) is a function of the blob,
//     and Unseal's forced rotation moves to epoch+1, so two barriers loaded from
//     the same blob derive the SAME DEK, each with a zeroed invocation counter
//     and an empty nonce tracker. Both then believe they own the full NIST
//     SP 800-38D budget for that key and neither can see the other's nonces —
//     the exact duplicate-key condition the invocation cap exists to bound.
//
//     WHAT THE BARRIER NOW ENFORCES — the load-generation fence. This persister
//     is the mechanism: every Unseal writes back SealedRootKey.LoadGeneration+1
//     through it BEFORE the barrier will encrypt, so each load leaves a claim in
//     the blob. A barrier later handed a blob whose generation is above its own
//     claim knows another loader has taken the state and fails closed,
//     permanently, with ErrBarrierSuperseded — Encrypt, Rotate and
//     PurgeEpochsBefore all refuse, while Decrypt keeps working. The persister
//     is therefore called once per Unseal even when nothing else changed.
//
//     WHAT IT DOES NOT DETECT. The fence is checked when state is LOADED
//     (SetSealedRootKey) and enforced when state is WRITTEN or USED; the barrier
//     never re-reads the blob on its own, because that would put a disk read on
//     the encrypt hot path. So a barrier superseded while running keeps
//     encrypting until the embedder hands it the newer state, and two loaders
//     that both read the same generation before either writes both claim the
//     same number and neither can tell. The fence converts the common case — a
//     second process starting on a data directory that is already in use — from
//     silent DEK sharing into a loud, attributable failure; it is not a lock.
//
//     For a hard guarantee, keep enforcing exclusivity outside: an exclusive
//     data-directory lock, or a persister backed by a compare-and-swap that
//     rejects a write whose expected prior generation no longer matches.
type StatePersister func(*SealedRootKey) error

// BarrierConfig configures the algorithm-agile epoch barrier.
type BarrierConfig struct {
	// Algorithm selects the symmetric encryption algorithm used for barrier
	// data encryption. Default: auto-selected via SelectOptimalAlgorithm().
	//
	// PIN THIS EXPLICITLY for any deployment whose at-rest data outlives the
	// host that wrote it. The default is derived from the local CPU, and the
	// algorithm is part of the key derivation (an HKDFV2 domain separator) as
	// well as the cipher Decrypt selects by epoch — a data directory restored
	// onto a host without AES-NI, or a node added to a mixed-generation
	// cluster, otherwise reads back as ErrDecryptionFailed. Initialize records
	// the choice in SealedRootKey.Algorithm and SetSealedRootKey refuses a
	// mismatch (AlgorithmMismatchError) so the failure names the algorithm that
	// would work; see SelectOptimalAlgorithm.
	Algorithm SymmetricAlgorithm

	// DeploymentID is bound into the HKDF info parameter to ensure that two
	// clusters sharing the same root key produce independent DEKs.
	DeploymentID uint64

	// NonceTracking enables per-nonce reuse detection. It is a *bool so that
	// the ZERO VALUE means "unset, use the default" rather than "off": a plain
	// bool made every hand-built BarrierConfig — the normal case for an
	// embedder that pins Algorithm and DeploymentID explicitly — silently
	// construct a barrier with NO nonce tracker at all. Nil defaults to TRUE;
	// use GetNonceTracking to read the effective value and BoolPtr(false) to
	// disable deliberately.
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
	// CEILING — tracking covers a PREFIX of the epoch, never the whole of it.
	// The default tracker enforces the memory bound above rather than only
	// documenting it: past DefaultMaxTrackedNonces (4Mi nonces, ~160 MB) it logs
	// at error level and SATURATES — it admits no further nonces, so memory
	// stays flat, and keeps rejecting repeats of the ones it already holds,
	// which costs nothing. Exact detection therefore covers the first
	// DefaultMaxTrackedNonces encryptions of each epoch and no more: with the
	// default 2^32 invocation limit that is 0.0977% of the epoch's permitted
	// invocations, and the remaining 99.9% rests on the random-96-bit-nonce plus
	// invocation-cap argument alone. Read the coverage state from
	// InMemoryNonceTracker.Saturated and Count, not from IsEnabled, which
	// reports only the operator's switch. Raise or remove the ceiling with
	// InMemoryNonceTracker.SetMaxTrackedNonces (via NonceTrackerProvider) when
	// the epoch is guaranteed to rotate well before memory becomes a concern.
	// NewBarrier additionally warns when tracking is on with neither a
	// StatePersister nor an InvocationCounter — the combination that provably
	// cannot rotate, and therefore can never release the tracked set, so the
	// ceiling is reached and detection stops for good.
	NonceTracking *bool

	// InvocationLimit is the max encrypt operations per key epoch.
	// Default: 2^32 (NIST SP 800-38D for random 96-bit nonces).
	// Set to 0 for default; -1 to disable. See NonceTracking: with tracking on,
	// the epoch must rotate well before this limit or the tracker exhausts memory.
	InvocationLimit int64

	// RotationThreshold is the percentage of InvocationLimit at which
	// ShouldRotate() returns true. Default: 95.0.
	RotationThreshold float64

	// StatePersister durably persists the sealed root-key state. It is
	// REQUIRED for Rotate, for UNSEAL AFTER A RESTART (which rotates, because
	// the loaded epoch's spent NIST SP 800-38D invocation budget is unknowable
	// — see Barrier.resolveInvocationBudget; without a persister the barrier
	// unseals read-only and every Encrypt fails closed), and for any
	// PurgeEpochsBefore that actually purges. All three change state that must
	// survive a restart (the active epoch, the KnownEpochs set), and data
	// written under an epoch the persisted blob does not name is undecryptable
	// after that restart. When nil, they fail closed with ErrNoStatePersister
	// and the current epoch stays active for READS; writes under it are refused
	// from the first post-restart unseal onward, because a budget nobody can
	// prove is not a budget.
	//
	// Initialize does NOT call the persister: the embedder persists
	// SealedRootKeyData() directly at bootstrap. Unseal DOES, when it rotates.
	// The persister is invoked with the barrier's write lock held on every one
	// of these paths and must not re-enter the barrier.
	StatePersister StatePersister

	// InvocationCounter allows injection of a custom counter.
	// Nil = default InMemoryInvocationCounter.
	//
	// Contract: an injected counter OWNS the persistence of per-epoch
	// invocation counts across process restarts (Seed it from durable
	// state before Unseal). When nil, the barrier cannot know how many
	// AEAD invocations the current epoch has already performed after a
	// restart, so Unseal moves to a FRESH epoch whose count is provably zero
	// (and refuses to encrypt under the loaded one if it cannot) to keep the
	// NIST SP 800-38D 2^32 budget sound across process lifetimes. See
	// Barrier.resolveInvocationBudget.
	//
	// CONSEQUENCE — a deployment that leaves this nil MUST configure
	// StatePersister, because every restart costs it an epoch: the barrier
	// rotates during Unseal rather than keep writing against an unprovable
	// budget. Inject a counter that persists its own count to avoid the forced
	// rotation; it then owns the correctness of that count, and seeding it too
	// low is exactly as unsound as not counting at all.
	InvocationCounter InvocationCounter

	// NonceTrackerProvider allows injection of a custom nonce tracker.
	// Nil = default InMemoryNonceTracker.
	NonceTrackerProvider NonceTrackerProvider

	// AEADProvider allows injection of a custom cipher.AEAD factory.
	// Nil = default SoftwareAEADProvider.
	AEADProvider AEADProvider

	// RequireContextAAD declares that this deployment REQUIRES positional
	// binding of at-rest ciphertext: every frame the embedder seals with a
	// caller context (a snapshot frame's shard/replica/index/ordinal, a WAL
	// record's shard/segment/ordinal) must have that context authenticated, so
	// a same-epoch frame cannot be moved, spliced, or truncated undetected.
	//
	// When true, Initialize and Unseal FAIL CLOSED with ErrContextAADUnsupported
	// if the constructed barrier cannot provide the binding. The check runs at
	// barrier setup — not at the first encrypt — so a misconfigured deployment
	// is refused before it writes a single unbound byte. Per mode:
	//
	//   - Software (the strategy supplies no BarrierEncryptor): satisfied. The
	//     barrier folds the context into its own AEAD AAD.
	//   - Delegated/hardware whose encryptor implements AADSymmetricEncrypter:
	//     satisfied. The context is authenticated by the encryptor.
	//   - Delegated/hardware whose encryptor implements only SymmetricEncrypter:
	//     REFUSED. There is no way to carry the context, and a non-empty context
	//     would fail with ErrHardwareMode on every write.
	//   - StrategyNone (no encryption at all): REFUSED. None mode stores
	//     plaintext, so there is no AEAD, no tag, and nothing that could
	//     authenticate a position. Requiring authenticated positional binding
	//     while selecting no encryption is a misconfiguration, not a
	//     satisfiable request, and it is rejected rather than silently treated
	//     as "nothing to bind".
	//
	// Default false preserves the historical behavior: a barrier that cannot
	// bind context is constructed normally and callers suppress their context
	// (see Barrier.SupportsContextAAD and Barrier.ContextBinding).
	RequireContextAAD bool

	// RequireFIPS declares that this deployment REQUIRES the FIPS 140-3 module.
	// When true, Validate (and therefore NewBarrier) fails closed with
	// ErrFIPSUnavailable unless FIPSEnabled() reports the module active.
	//
	// It exists because FIPS enforcement is otherwise entirely ENVIRONMENT
	// derived: without this flag "FIPS was not requested" and "FIPS was
	// requested but this binary was not built with the module" are
	// indistinguishable, and the second fails OPEN — the barrier quietly wraps
	// its root key with Argon2id and encrypts with whatever
	// SelectOptimalAlgorithm picked, which may be ChaCha20-Poly1305. Setting
	// this turns that silent downgrade into a refusal at construction.
	RequireFIPS bool

	// MinEpoch is a floor on the epoch a persisted SealedRootKey may name.
	// SetSealedRootKey rejects a blob whose Epoch is below it with
	// ErrSealedEpochRollback.
	//
	// The sealed blob's metadata is authenticated (SealedRootKey.StateMAC) but
	// authentication cannot detect REPLAY: an attacker with write access to the
	// persisted state can restore an OLDER, genuinely-MACed copy of the same
	// blob, which resurrects a retired epoch's DEK together with a fresh
	// invocation counter and an empty nonce tracker — two independent
	// random-nonce sequences under one key, exactly what the invocation cap
	// exists to prevent. Only state the attacker cannot roll back in the same
	// step can detect that, and the barrier holds none: the embedder does.
	//
	// Set it to the highest epoch any live WAL segment or retained snapshot
	// still references (the engine computes exactly this as its global
	// safe-purge floor / minimum live epoch). Zero disables the check, which is
	// the historical behavior.
	//
	// SCOPE — this is a floor against REPLAY, and replay only. The other route
	// to a duplicated DEK — two live barriers loading the SAME current blob —
	// needs no rollback and therefore clears any floor this field can express;
	// that one is the load-generation fence's job (see the StatePersister
	// contract and SealedRootKey.LoadGeneration). The two are complementary and
	// neither substitutes for the other: the fence detects a competing claim,
	// this floor refuses an older state.
	MinEpoch uint64

	// AllowUnauthenticatedSealedState permits loading a SealedRootKey that
	// carries no StateMAC — a blob written before the barrier authenticated its
	// own metadata.
	//
	// The metadata (Epoch, KnownEpochs, Salt, HKDFVersion, Algorithm, Strategy)
	// is plaintext next to the root-key ciphertext and is NOT covered by that
	// ciphertext's AAD, so without the MAC every one of those fields is
	// attacker-controlled under the write-access threat model this package
	// already defends elsewhere. Accepting an ABSENT MAC is therefore a
	// downgrade an attacker can trigger by simply deleting the field, which is
	// why it fails closed by default and this escape hatch is explicit,
	// per-deployment, and named for what it gives up.
	//
	// ON-DISK CONSEQUENCE: leaving this false requires every persisted blob to
	// have been written by a build that stamps StateMAC. Migrate a pre-MAC
	// deployment by starting once with this set to true and re-persisting
	// SealedRootKeyData() (Unseal restamps the MAC), then clearing it.
	AllowUnauthenticatedSealedState bool
}

// BoolPtr returns a pointer to the given bool value, for setting *bool config
// fields where nil means "use the default". It mirrors config.BoolPtr and is
// duplicated here so that pkg/seal — the foundational encryptor, which imports
// nothing from the rest of the engine — stays dependency-free.
func BoolPtr(v bool) *bool { return &v }

// GetNonceTracking returns the effective NonceTracking value. A nil pointer
// means the field was never set and yields the safe default, TRUE.
func (c *BarrierConfig) GetNonceTracking() bool {
	if c.NonceTracking == nil {
		return true
	}
	return *c.NonceTracking
}

// DefaultBarrierConfig returns a BarrierConfig with sensible defaults:
// auto-selected algorithm, nonce tracking enabled, invocation limit at 2^32.
//
// The auto-selected algorithm is a property of the LOCAL CPU, so a deployment
// whose data outlives one host must override Algorithm with an explicit,
// cluster-wide value rather than take this default. See BarrierConfig.Algorithm
// and SelectOptimalAlgorithm.
func DefaultBarrierConfig(deploymentID uint64) BarrierConfig {
	return BarrierConfig{
		Algorithm:     SelectOptimalAlgorithm(false),
		DeploymentID:  deploymentID,
		NonceTracking: BoolPtr(true),
	}
}

// Validate reports whether the configuration can be used to construct a
// Barrier, returning ErrUnsupportedAlgorithm for an unknown algorithm,
// ErrAlgorithmNotFIPSApproved when FIPS mode is active and the configured
// cipher is not FIPS-approved, and ErrFIPSUnavailable when RequireFIPS is set
// but the FIPS 140-3 module is not active.
//
// NewBarrier applies exactly these rules and returns this error, so calling
// Validate first is an optimization, not a safety requirement. It is useful
// when a config is validated at admission time — an operator's bad algorithm
// can be rejected where it was entered, before anything tries to build a
// barrier from it. MustNewBarrier turns the same error into a panic and is
// only for compile-time-constant configs.
func (c *BarrierConfig) Validate() error {
	if !c.Algorithm.Valid() {
		return ErrUnsupportedAlgorithm
	}
	// A deployment that DEMANDS FIPS must not start on a binary that cannot
	// provide it. Checked before the cipher rule below so the operator is told
	// the module is missing rather than that some algorithm is unapproved.
	if c.RequireFIPS && !FIPSEnabled() {
		return ErrFIPSUnavailable
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
