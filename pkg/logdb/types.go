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

import "github.com/jeremyhahn/go-quicraft/pkg/config"

// LogDB-layer types define the storage-specific structures used by the
// persistence layer. These are separate from proto types to maintain a
// clean boundary between the serialization format and storage format.

// Entry represents a single Raft log entry.
//
// The session fields (Key, ClientID, SeriesID, RespondedTo) MUST be
// persisted: at-most-once delivery depends on them surviving crash
// recovery. ClientID=0 means "not session-managed", so dropping these
// fields on the WAL round-trip would silently disable session dedup for
// every entry replayed after a restart.
type Entry struct {
	// Index is the log position of this entry.
	Index uint64
	// Term is the Raft term when this entry was created.
	Term uint64
	// Type identifies the entry type (normal, config change, etc.).
	Type uint64
	// Key is the caller-assigned completion key for this proposal.
	Key uint64
	// ClientID identifies the client session that proposed this entry.
	ClientID uint64
	// SeriesID is the per-session sequence number used for dedup.
	SeriesID uint64
	// RespondedTo is the highest responded-to series for this client.
	RespondedTo uint64
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
	// MaxWitnesses is the shard's replicated witness cap (0 = no
	// explicit cap). Part of the replicated membership state.
	MaxWitnesses uint64
	// RequireFullReplicaMajority is the shard's replicated
	// full-replica-majority witness policy.
	RequireFullReplicaMajority bool
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
	// Compression is the algorithm the snapshot data file was written
	// through. It describes the ARTIFACT, not the writer's configuration:
	// every recovery path must build its reader chain from this field, not
	// from the local Config.SnapshotCompressionType, which is per-shard,
	// mutable across restarts, and independent on every host. The zero
	// value is config.NoCompression, which matches both the package default
	// and every record written before this field existed.
	Compression config.CompressionType
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
	// DataDir is the shard's Config.DataDir, empty when the shard roots its
	// on-disk tree under the host's NodeHostDir.
	//
	// It is recorded because the shard's snapshot tree — the tree snapshot
	// garbage collection and, critically, the DEK purge-floor scan enumerate —
	// hangs off it, and operations that run while the shard is STOPPED
	// (Host.ImportSnapshot) have no other way to learn it. Writing a snapshot
	// artifact outside that tree leaks it and, because the purge floor is
	// computed by SCANNING the tree, lets the next PurgeEpochsBefore destroy
	// the DEK the artifact needs.
	//
	// A record written before this field existed carries no DataDir at all.
	// DataDirRecorded — not the emptiness of this string — is what tells the
	// two apart.
	DataDir string

	// DataDirRecorded reports whether the persisted record actually CARRIED
	// the DataDir field, as opposed to predating it.
	//
	// It is the same distinction snapshot.meta draws with EpochRecorded, and
	// for the same reason: DataDir "" is a meaningful VALUE ("this shard roots
	// its tree under the host's NodeHostDir"), so a caller that reads a legacy
	// record's zero value as that statement is trusting a field nobody wrote.
	// Host.ImportSnapshot would then write the artifact under NodeHostDir for a
	// shard whose tree is actually under its own DataDir — outside everything
	// snapshot garbage collection and the DEK purge-floor scan enumerate — and
	// simultaneously REFUSE the operator's WithImportDataDir override as a
	// contradiction. Every shard bootstrapped before this field existed is
	// broken both ways unless "absent" is distinguishable from "empty".
	//
	// It is a property of the RECORD, not of the caller's request: a backend
	// sets it when it stores a record (every record this version writes carries
	// the field) and derives it on replay from what was actually on the wire.
	// A value passed to SaveBootstrap is ignored.
	DataDirRecorded bool
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
