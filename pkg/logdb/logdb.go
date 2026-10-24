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

// Package logdb defines the interface for persistent Raft log storage.
//
// LogDB stores log entries, hard state, snapshot metadata, and bootstrap
// information for all Raft shards on a node. Implementations must be safe
// for concurrent use from multiple goroutines.
//
// The types in this package (Entry, State, Membership, Snapshot, Bootstrap,
// Update) are storage-layer types separate from proto types to maintain a
// clean abstraction boundary between serialization and persistence.
package logdb

// NodeInfo identifies a specific replica within a shard.
type NodeInfo struct {
	// ShardID is the unique identifier for the Raft shard.
	ShardID uint64
	// ReplicaID is the unique identifier for the replica within the shard.
	ReplicaID uint64
}

// EntryOverheadBytes is the fixed per-entry byte overhead every LogDB
// implementation charges against the IterateEntries maxSize budget, in
// addition to len(Entry.Cmd). It equals the WAL entry record header size
// (waldb entryRecordHeaderSize), so size budgets computed by callers are
// portable across implementations: the same maxSize admits the same entries
// regardless of the backing store.
//
// It MUST track waldb.entryRecordHeaderSize exactly. The two are separate
// constants only because this package cannot import that one, so a layout
// change has to move both: the WAL charges what it actually writes, and a
// budget computed from a stale value here admits a different set of entries
// than the store it is describing.
const EntryOverheadBytes = 84

// LogDB is the interface for persistent Raft log storage.
// Implementations must be safe for concurrent use from multiple goroutines.
type LogDB interface {
	// Name returns the name/type of the LogDB implementation.
	Name() string

	// SaveState persists a batch of Updates atomically.
	// Each Update contains entries and/or hard state for a single shard.
	//
	// Implementations must not retain or alias the caller's Entry.Cmd
	// backing arrays: callers may reuse those buffers after SaveState
	// returns, so the stored log must hold its own copies.
	//
	// When Update.Snapshot has Index > 0 and is newer than the stored
	// snapshot for the node, the snapshot metadata (including its
	// Membership) is persisted exactly as SaveSnapshot would persist it;
	// stale snapshots in an Update are silently ignored.
	//
	// An Update whose first entry sits at or below the node's compaction
	// floor is REFUSED with ErrEntriesBelowCompactionFloor, and the whole
	// batch is rejected without storing any of it. Those indexes are already
	// deleted, so honouring the write would replace the live suffix above the
	// floor with entries nothing can serve; see the sentinel's documentation.
	SaveState(updates []Update) error

	// IterateEntries returns log entries in the range [low, high) for the
	// given node, up to maxSize bytes total. Results are appended to the
	// entries slice. Returns the modified entries slice and the cumulative
	// size in bytes (the incoming size plus each returned entry's size).
	//
	// Contract (identical for every implementation):
	//   - Each entry is charged EntryOverheadBytes + len(Cmd) against
	//     maxSize. maxSize of 0 means unlimited. At least one entry is
	//     returned when any is available, even if it alone exceeds
	//     maxSize; otherwise iteration stops BEFORE the entry that would
	//     push the cumulative size past maxSize.
	//   - low >= high returns (entries, size, nil) without touching store
	//     state.
	//   - low at or below the node's compaction floor returns ErrCompacted.
	//   - An unknown node, or a range holding no entries, returns
	//     ErrUnavailable.
	//   - A stored range that does not begin exactly at low, or that is
	//     missing an interior index, returns ErrLogGapDetected — a log
	//     hole must never be served as a silently shortened result.
	//
	// Implementations must not return an Entry whose Cmd aliases their own
	// storage: the returned payloads are the caller's to keep and to modify.
	// This is the mirror of the no-aliasing rule SaveState states above, and it
	// matters more, because what a caller would be writing through into is the
	// live log this replica serves to its followers and replays to itself on
	// restart. See CopyEntryCmd. (The rule covers the entries a successful call
	// APPENDS; on an error return the whole result must be discarded.)
	IterateEntries(entries []Entry, size uint64, shardID uint64, replicaID uint64,
		low uint64, high uint64, maxSize uint64) ([]Entry, uint64, error)

	// ReadState reads the persisted hard state and membership for the given node.
	ReadState(shardID uint64, replicaID uint64) (State, Membership, error)

	// EntryRange returns the first index and total count of available log
	// entries for the given node. Returns (0, 0, nil) when no entries exist.
	// Used during recovery to initialize the LogReader's tracked range
	// without loading all entries into memory.
	//
	// The returned length is the CONTIGUOUS span lastIndex-firstIndex+1.
	// Implementations must verify that the stored entry count matches that
	// span and return ErrLogGapDetected on a mismatch: reporting a bare
	// count over a holed range would make the caller compute a short
	// lastIndex and silently hide the gap.
	EntryRange(shardID uint64, replicaID uint64) (firstIndex uint64, length uint64, err error)

	// RemoveEntriesTo removes log entries up to and including the given index
	// for the specified node. Used for log compaction.
	RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error

	// Compact compacts the underlying storage for the given node.
	Compact(shardID uint64, replicaID uint64) error

	// SaveSnapshot persists snapshot metadata for the given node.
	SaveSnapshot(shardID uint64, replicaID uint64, snapshot Snapshot) error

	// GetSnapshot returns the most recent snapshot metadata for the given node.
	GetSnapshot(shardID uint64, replicaID uint64) (Snapshot, error)

	// RemoveNodeData removes all data for the given node and RETIRES the
	// (shardID, replicaID) pair: NodeRemoved reports it from then on, across
	// restarts. Raft never lets a removed replica ID return — its term and
	// vote are gone, so it could vote twice in one term — and the host refuses
	// to start or import a retired replica.
	RemoveNodeData(shardID uint64, replicaID uint64) error

	// NodeRemoved reports whether RemoveNodeData has ever removed the given
	// node, and so retired its replica ID.
	NodeRemoved(shardID uint64, replicaID uint64) (bool, error)

	// ListNodeInfo returns all NodeInfo entries in the LogDB.
	ListNodeInfo() ([]NodeInfo, error)

	// SaveBootstrap persists bootstrap information for a node. A backend
	// persists every field, DataDir included, in every bootstrap record:
	// DataDir "" is a stated value (the tree is rooted under NodeHostDir),
	// so GetBootstrap must never return a record that did not state it.
	SaveBootstrap(shardID uint64, replicaID uint64, bootstrap Bootstrap) error

	// GetBootstrap returns the bootstrap information for a node.
	// The bool return value indicates whether bootstrap info was found.
	GetBootstrap(shardID uint64, replicaID uint64) (Bootstrap, bool, error)

	// Close closes the LogDB and releases resources.
	Close() error
}

// EpochAwareLogDB is an optional interface that LogDB implementations may
// support to expose the minimum barrier encryption epoch across all live
// WAL records. This is used by the snapshot pool to prevent premature
// epoch DEK purging: if WAL segments still contain records encrypted at
// an older epoch, purging that epoch's DEK would cause WALDecryptError
// on crash recovery.
//
// Implementations that do not support encryption or epoch tracking need
// not implement this interface. Callers should use a type assertion to
// check for support before calling.
type EpochAwareLogDB interface {
	// MinLiveEpoch returns the minimum barrier encryption epoch across
	// all live (non-compacted, non-garbage-collected) WAL records.
	// Returns 0 when no encrypted records exist (all plaintext or empty).
	// Returns 0 with a nil error when no barrier is configured.
	MinLiveEpoch() (uint64, error)
}

// PerBarrierEpochAwareLogDB is an optional interface for LogDB
// implementations that route different Raft shards to different at-rest
// barriers (per-tenant WAL encryption). It exposes the minimum live WAL
// epoch restricted to the records encrypted under a SINGLE barrier, so the
// snapshot pool can compute an independent DEK-purge floor per barrier:
// purging barrier B's old DEKs must use min(B's snapshots, B's WAL records)
// and must never be lowered or raised by another barrier's records.
//
// The barrier argument is an opaque handle (the same barrier value the caller
// installed, typically a *seal.Barrier). Its dynamic value is NOT guaranteed
// to be comparable, so implementations must never compare it with == or use
// it as a map key: either panics when the value holds a slice, map or func.
// Implementations match it with SameBarrier, the one identity rule: identity
// is pointer identity, and a handle that is not a non-nil pointer is the same
// barrier as nothing, not even a copy of itself, so it must be answered
// known == false. A single-barrier LogDB satisfies this by
// returning its global MinLiveEpoch when the handle matches its own barrier;
// a multiplexing LogDB delegates to the underlying store that owns the
// barrier.
//
// Implementations that do not support per-barrier routing need not implement
// this interface. Callers should use a type assertion to check for support
// and fall back to EpochAwareLogDB / fail-safe behavior otherwise.
type PerBarrierEpochAwareLogDB interface {
	// MinLiveEpochForBarrier returns the minimum barrier encryption epoch
	// across all live WAL records encrypted under the given barrier handle.
	//
	// The three return values form a fail-safe contract that callers MUST
	// honor before purging any DEK:
	//
	//   - known == false: this LogDB does NOT own / cannot attribute the
	//     barrier (it stores no records under it, or cannot prove which of its
	//     records belong to it). The epoch is meaningless and callers MUST NOT
	//     purge. This is the critical distinction from a true non-constraint:
	//     an "unknown barrier" is indistinguishable from "zero live records"
	//     in a two-value signature, and treating it as the latter over-purges.
	//   - known == true, epoch == 0: this LogDB owns the barrier and it has
	//     zero live encrypted records (all plaintext, empty, or fully
	//     compacted). This is a true non-constraint — the WAL imposes no purge
	//     floor, though other floors (e.g. retained snapshots) still apply.
	//   - known == true, epoch > 0: this LogDB owns the barrier and its oldest
	//     live encrypted record is at epoch. DEKs strictly below it must not be
	//     purged.
	//
	// The returned error is non-nil only when the value cannot be determined
	// (e.g. the store is closed); callers MUST treat any error as fail-safe
	// (do not purge), exactly like known == false.
	MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error)
}

// BarrierShardEnumerator is an optional interface a per-barrier LogDB may
// implement to report the COMPLETE, stable set of Raft shards whose records are
// encrypted under a given barrier. The snapshot pool uses it to verify that its
// per-barrier retained-snapshot scan covers every shard mapped to the barrier:
// a shard that maps to the barrier but is not currently loaded in the engine
// cannot have its retained snapshots scanned, so its (possibly older) snapshot
// epochs would be silently dropped from the purge floor — over-purging the
// SHARED barrier's DEKs and losing the data those snapshots still need to
// decrypt. When a mapped shard is absent the pool fails safe (no purge).
//
// known is true ONLY when the barrier's shard set is a fixed, fully enumerable
// set (e.g. a registered dedicated per-tenant barrier). It is false for a
// barrier whose shard membership is open-ended (such as a shared/system barrier
// that is the fallback for every non-dedicated shard) and for any barrier the
// implementation does not own; in those cases the caller falls back to scanning
// the currently-loaded shards, matching the global single-barrier behavior.
//
// The barrier argument follows PerBarrierEpochAwareLogDB's identity rule: it
// is matched with SameBarrier, never with == or as a map key, and a handle
// that is not a pointer is answered known == false.
//
// Implementations that cannot enumerate shards per barrier need not implement
// this interface; callers use a type assertion to check for support.
type BarrierShardEnumerator interface {
	// ShardsForBarrier returns the complete set of shard IDs whose records are
	// encrypted under barrier, and whether that set is fully enumerable (known).
	ShardsForBarrier(barrier any) (shards []uint64, known bool)
}

// AtRestProtection classifies what a LogDB guarantees about the records it
// holds once they leave the process: whether a Raft log entry -- and therefore
// the user command inside it -- can be read off the machine's storage.
//
// It exists because "does this LogDB implement the per-barrier interfaces" is
// NOT the same question as "is this LogDB encrypted". waldb.DB implements
// PerBarrierEpochAwareLogDB whether or not it was opened WithBarrier, so a type
// assertion accepts a store that writes every command to disk in cleartext. The
// three states below are the smallest set that separates the two facts a caller
// must not conflate: reaching durable storage at all, and being sealed once
// there.
type AtRestProtection uint8

const (
	// AtRestUnprotected means records reach durable storage in cleartext, OR
	// the implementation cannot prove they do not. It is the ZERO VALUE
	// deliberately: an implementation that says nothing must be read as
	// unprotected, never as safe.
	AtRestUnprotected AtRestProtection = iota

	// AtRestNonDurable means this LogDB writes NOTHING to durable storage --
	// every record lives in process memory and is lost on exit. There is no
	// at-rest artifact to encrypt, so the at-rest guarantee holds vacuously.
	AtRestNonDurable

	// AtRestBarrierEncrypted means every record this LogDB writes to durable
	// storage is encrypted under an at-rest barrier. It says nothing about
	// WHICH barrier; that question belongs to
	// PerBarrierEpochAwareLogDB.MinLiveEpochForBarrier, which answers it by
	// barrier identity.
	AtRestBarrierEncrypted

	// AtRestHostRouted means every record this LogDB writes to durable storage
	// takes one of two routes, each of which it names by barrier identity
	// through PerBarrierEpochAwareLogDB:
	//
	//   - a dedicated route, under the per-shard barrier that shard resolves
	//     to (MinLiveEpochForBarrier answers known == true for it); or
	//   - ONE host-wide route, written through a single barrier that may be in
	//     plaintext mode, or through no barrier at all.
	//
	// It does NOT say any record is encrypted. It says the host-wide route is
	// the only place a cleartext record can be, so a host judges that route
	// against the posture IT declared: under WithBarrier the route must write
	// through that very barrier (checked by identity), exactly as the default
	// WAL does; under WithoutEncryption a plaintext route is what was declared;
	// with neither there is nothing to accept it against, and it is refused.
	// Every per-shard route is still checked by identity when its shard starts.
	//
	// A multiplexing LogDB that keeps tenant shards in dedicated WALs and every
	// other shard in a shared one reports it. So does a single waldb.DB whose
	// one barrier is in plaintext mode: its only route is that barrier.
	AtRestHostRouted
)

// String renders the classification for structured logs and error messages.
func (p AtRestProtection) String() string {
	switch p {
	case AtRestUnprotected:
		return "unprotected"
	case AtRestNonDurable:
		return "non_durable"
	case AtRestBarrierEncrypted:
		return "barrier_encrypted"
	case AtRestHostRouted:
		return "host_routed"
	default:
		return "invalid"
	}
}

// AtRestProtectedLogDB is an optional interface a LogDB implements to declare
// how the records it holds are protected at rest. Hosts that require at-rest
// encryption consult it before accepting a caller-supplied LogDB.
//
// The declaration is a CONTRACT ASSERTION, not a cryptographic proof: a host
// cannot verify another package's storage layer, so the only thing it can do is
// require the implementation to state the guarantee and refuse the ones that do
// not. That is why the missing-implementation case and the zero value both mean
// AtRestUnprotected -- the caller fails closed on silence.
type AtRestProtectedLogDB interface {
	// AtRestProtection reports this LogDB's at-rest guarantee. It must not
	// depend on the records currently stored: an empty encrypted store still
	// reports AtRestBarrierEncrypted.
	AtRestProtection() AtRestProtection
}
