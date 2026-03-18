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

package logdb

// LogDB-layer types define the storage-specific structures used by the
// persistence layer. These are separate from proto types to maintain a
// clean boundary between the serialization format and storage format.

// Entry represents a single Raft log entry.
type Entry struct {
	// Index is the log position of this entry.
	Index uint64
	// Term is the Raft term when this entry was created.
	Term uint64
	// Type identifies the entry type (normal, config change, etc.).
	Type uint64
	// Cmd is the serialized command payload.
	Cmd []byte
}

// State represents the persisted Raft hard state.
type State struct {
	// Term is the current Raft term.
	Term uint64
	// Vote is the candidate this node voted for in the current term.
	Vote uint64
	// Commit is the highest log index known to be committed.
	Commit uint64
}

// Membership represents a Raft cluster membership configuration.
type Membership struct {
	// ConfigChangeID is the log index of the configuration change
	// that produced this membership.
	ConfigChangeID uint64
	// Addresses maps replica IDs to their network addresses for
	// full voting members.
	Addresses map[uint64]string
	// Observers maps replica IDs to their network addresses for
	// non-voting observer replicas.
	Observers map[uint64]string
	// Witnesses maps replica IDs to their network addresses for
	// witness replicas that only vote but do not store log entries.
	Witnesses map[uint64]string
	// Removed tracks replica IDs that have been removed from the cluster.
	Removed map[uint64]bool
}

// Snapshot represents persisted snapshot metadata.
type Snapshot struct {
	// Index is the last included log index in this snapshot.
	Index uint64
	// Term is the term of the last included log entry.
	Term uint64
	// Membership is the cluster membership at the snapshot index.
	Membership Membership
	// Filepath is the path to the snapshot data file on disk.
	Filepath string
	// FileSize is the size of the snapshot data file in bytes.
	FileSize uint64
	// OnDiskIndex is the index tracked for on-disk state machines.
	OnDiskIndex uint64
	// Epoch is the barrier encryption epoch used when this snapshot was
	// saved. Zero indicates no encryption (plaintext snapshot).
	Epoch uint64
}

// Bootstrap contains the initial cluster configuration used to
// bootstrap a new Raft node.
type Bootstrap struct {
	// Addresses maps initial replica IDs to their network addresses.
	Addresses map[uint64]string
	// Join indicates whether this node is joining an existing cluster
	// rather than forming a new one.
	Join bool
	// Type identifies the state machine type for this shard.
	Type uint64
}

// Update represents a batch of state changes for a single shard
// to be persisted atomically.
type Update struct {
	// ShardID identifies the Raft shard this update belongs to.
	ShardID uint64
	// ReplicaID identifies the replica within the shard.
	ReplicaID uint64
	// State is the hard state to persist.
	State State
	// Entries contains the log entries to append.
	Entries []Entry
	// Snapshot contains snapshot metadata to persist.
	Snapshot Snapshot
}
