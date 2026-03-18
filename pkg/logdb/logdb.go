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

// LogDB is the interface for persistent Raft log storage.
// Implementations must be safe for concurrent use from multiple goroutines.
type LogDB interface {
	// Name returns the name/type of the LogDB implementation.
	Name() string

	// SaveState persists a batch of Updates atomically.
	// Each Update contains entries and/or hard state for a single shard.
	SaveState(updates []Update) error

	// IterateEntries returns log entries in the range [low, high) for the given node,
	// up to maxSize bytes total. Results are appended to the entries slice.
	// Returns the modified entries slice and the total size in bytes.
	IterateEntries(entries []Entry, size uint64, shardID uint64, replicaID uint64,
		low uint64, high uint64, maxSize uint64) ([]Entry, uint64, error)

	// ReadState reads the persisted hard state and membership for the given node.
	ReadState(shardID uint64, replicaID uint64) (State, Membership, error)

	// EntryRange returns the first index and total count of available log
	// entries for the given node. Returns (0, 0, nil) when no entries exist.
	// Used during recovery to initialize the LogReader's tracked range
	// without loading all entries into memory.
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

	// RemoveNodeData removes all data for the given node.
	RemoveNodeData(shardID uint64, replicaID uint64) error

	// ListNodeInfo returns all NodeInfo entries in the LogDB.
	ListNodeInfo() ([]NodeInfo, error)

	// SaveBootstrap persists bootstrap information for a node.
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
