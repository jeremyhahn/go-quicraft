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

// Package logdb defines persistent Raft log storage: entries, hard state, snapshots and bootstrap records for every shard; implementations must be safe for concurrent use.
package logdb

// NodeInfo identifies a specific replica within a shard.
type NodeInfo struct {
	// ShardID is the unique identifier for the Raft shard.
	ShardID uint64
	// ReplicaID is the unique identifier for the replica within the shard.
	ReplicaID uint64
}

// EntryOverheadBytes is the per-entry overhead charged against IterateEntries maxSize besides len(Cmd); it must equal waldb's entryRecordHeaderSize.
const EntryOverheadBytes = 84

// LogDB is the interface for persistent Raft log storage.
// Implementations must be safe for concurrent use from multiple goroutines.
type LogDB interface {
	// Name returns the name/type of the LogDB implementation.
	Name() string

	// SaveState persists a batch of Updates atomically without aliasing Entry.Cmd, saves a newer snapshot it carries, and refuses with ErrEntriesBelowCompactionFloor an Update starting at or below the floor.
	SaveState(updates []Update) error

	// IterateEntries appends entries in [low, high) up to maxSize (0 is unlimited, at least one entry) and returns the cumulative size; ErrCompacted, ErrUnavailable and ErrLogGapDetected as documented, and returned Cmd never aliases storage.
	IterateEntries(entries []Entry, size uint64, shardID uint64, replicaID uint64,
		low uint64, high uint64, maxSize uint64) ([]Entry, uint64, error)

	// ReadState reads the persisted hard state and membership for the given node.
	ReadState(shardID uint64, replicaID uint64) (State, Membership, error)

	// EntryRange returns the first index and contiguous count of entries, (0, 0, nil) when none, or ErrLogGapDetected on a hole.
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

	// RemoveNodeData removes a node's data and retires the (shardID, replicaID) pair permanently; NodeRemoved reports it.
	RemoveNodeData(shardID uint64, replicaID uint64) error

	// NodeRemoved reports whether RemoveNodeData has ever removed the given
	// node, and so retired its replica ID.
	NodeRemoved(shardID uint64, replicaID uint64) (bool, error)

	// ListNodeInfo returns all NodeInfo entries in the LogDB.
	ListNodeInfo() ([]NodeInfo, error)

	// SaveBootstrap persists a node's bootstrap record with every field, DataDir included.
	SaveBootstrap(shardID uint64, replicaID uint64, bootstrap Bootstrap) error

	// GetBootstrap returns the bootstrap information for a node.
	// The bool return value indicates whether bootstrap info was found.
	GetBootstrap(shardID uint64, replicaID uint64) (Bootstrap, bool, error)

	// Close closes the LogDB and releases resources.
	Close() error
}

// EpochAwareLogDB is an optional LogDB interface exposing the minimum live WAL barrier epoch, so the snapshot pool does not purge DEKs still in use.
type EpochAwareLogDB interface {
	// MinLiveEpoch returns the minimum barrier epoch across live WAL records, or 0 when none are encrypted or no barrier is configured.
	MinLiveEpoch() (uint64, error)
}

// PerBarrierEpochAwareLogDB is an optional interface giving the minimum live WAL epoch per barrier; barrier is matched with SameBarrier, never == or as a map key.
type PerBarrierEpochAwareLogDB interface {
	// MinLiveEpochForBarrier returns the minimum live epoch under barrier; known false or a non-nil error means do not purge, epoch 0 means no WAL constraint.
	MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error)
}

// BarrierShardEnumerator is an optional interface reporting the complete shard set encrypted under barrier; known is true only for a fixed set, matched with SameBarrier.
type BarrierShardEnumerator interface {
	// ShardsForBarrier returns the complete set of shard IDs whose records are
	// encrypted under barrier, and whether that set is fully enumerable (known).
	ShardsForBarrier(barrier any) (shards []uint64, known bool)
}

// AtRestProtection classifies whether a LogDB's records can be read off storage; implementing the barrier interfaces does not imply encryption.
type AtRestProtection uint8

const (
	// AtRestUnprotected means records may reach storage in cleartext; it is the zero value.
	AtRestUnprotected AtRestProtection = iota

	// AtRestNonDurable means this LogDB writes NOTHING to durable storage --
	// every record lives in process memory and is lost on exit. There is no
	// at-rest artifact to encrypt, so the at-rest guarantee holds vacuously.
	AtRestNonDurable

	// AtRestBarrierEncrypted means every record is encrypted under an at-rest barrier; which one is answered by MinLiveEpochForBarrier.
	AtRestBarrierEncrypted

	// AtRestHostRouted means each record goes through a dedicated per-shard barrier or one host-wide route, which the host checks against its own declared posture.
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

// AtRestProtectedLogDB is an optional interface declaring at-rest protection; a missing implementation counts as AtRestUnprotected.
type AtRestProtectedLogDB interface {
	// AtRestProtection reports this LogDB's at-rest guarantee. It must not
	// depend on the records currently stored: an empty encrypted store still
	// reports AtRestBarrierEncrypted.
	AtRestProtection() AtRestProtection
}
