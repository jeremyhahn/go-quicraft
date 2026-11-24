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

import (
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// LogDB-layer types define the storage-specific structures used by the
// persistence layer. These are separate from proto types to maintain a
// clean boundary between the serialization format and storage format.

// Entry is a Raft log entry; the session fields and OwnerID must be persisted for dedup and the per-owner session cap.
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
	// OwnerID is the principal the embedder declared for the session this
	// entry registers; 0 means none. See proto.Entry.OwnerID.
	OwnerID uint64
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
	// VoteWindow is the vote window, in nanoseconds, this replica may still
	// owe a leader's read lease. See proto.State.VoteWindow.
	VoteWindow uint64
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
	// SessionExpiryEntries, MaxTotalSessions, MaxResponsesPerSession and MaxSessionsPerOwner are the replicated session policy and must survive this round-trip.
	SessionExpiryEntries uint64
	// MaxTotalSessions is the replicated cap on registered sessions. It is
	// non-zero in every committed policy and is therefore the field that says
	// whether a policy was recorded at all: see proto.SessionPolicy.IsSet.
	MaxTotalSessions uint64
	// MaxResponsesPerSession is the per-session response-cache depth; see proto.SessionPolicy.
	MaxResponsesPerSession uint64
	// MaxSessionsPerOwner is the replicated cap on concurrent sessions per
	// declared owner; 0 disables it. See proto.SessionPolicy.MaxSessionsPerOwner.
	MaxSessionsPerOwner uint64
	// WireVersion is the shard's committed wire version (0 = never
	// switched). Replicated state; see proto.Membership.WireVersion.
	WireVersion uint64
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
	// Identities maps replica IDs to committed node identities (proto.Membership.Identities) and must survive the snapshot-metadata round-trip.
	Identities map[uint64]string
	// RemovedIdentities maps removed replica IDs to their identity at removal (the receive-side deny-list) and must survive this round-trip; its keys are a subset of Removed.
	RemovedIdentities map[uint64]string
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
	// Compression is the algorithm the snapshot data file was written through; recovery reads it from here, not from Config.
	Compression config.CompressionType
}

// CheckBounds is proto.Snapshot.CheckBounds for the storage type.
func (s *Snapshot) CheckBounds() error {
	return (&proto.Snapshot{Index: s.Index, Term: s.Term}).CheckBounds()
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
	// DataDir is the shard's Config.DataDir, "" for the host's NodeHostDir; every bootstrap record states it.
	DataDir string
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

// CopyEntryCmd returns e with a private per-entry copy of its Cmd payload, so callers cannot write into the stored log.
func CopyEntryCmd(e Entry) Entry {
	if len(e.Cmd) == 0 {
		return e
	}
	cmd := make([]byte, len(e.Cmd))
	copy(cmd, e.Cmd)
	e.Cmd = cmd
	return e
}
