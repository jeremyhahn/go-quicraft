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

// Entry represents a single Raft log entry.
//
// The session fields (Key, ClientID, SeriesID, RespondedTo, OwnerID) MUST be
// persisted: at-most-once delivery depends on them surviving crash
// recovery. ClientID=0 means "not session-managed", so dropping these
// fields on the WAL round-trip would silently disable session dedup for
// every entry replayed after a restart.
//
// OwnerID is in that set for a related but distinct reason: it is the input to
// a deterministic apply-path decision (the per-owner session cap, see
// proto.SessionPolicy.MaxSessionsPerOwner). OwnerID=0 means "no owner", which
// is also what a dropped field decodes to — so losing it on the round-trip does
// not fail, it just silently lifts the cap on every entry replayed after a
// restart, and the replica then admits registrations its peers refuse.
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
	// SessionExpiryEntries, MaxTotalSessions, MaxSessionsPerClient and
	// MaxSessionsPerOwner are the shard's replicated SESSION POLICY (see
	// proto.SessionPolicy). They must
	// survive this round-trip for the same reason Identities must: they enter
	// membership through the log — the EntryInitialMembership at index 1 — so
	// a replica that rebuilds its configuration from a SNAPSHOT whose log has
	// been compacted past that entry has only this record to relearn them
	// from.
	//
	// A backend that drops them does not fail. The replica comes back with an
	// unset policy, falls back to its own Config, and expires sessions on a
	// schedule its peers do not share — a state-machine divergence with every
	// Raft invariant intact and no error raised anywhere.
	SessionExpiryEntries uint64
	// MaxTotalSessions is the replicated cap on registered sessions. It is
	// non-zero in every committed policy and is therefore the field that says
	// whether a policy was recorded at all: see proto.SessionPolicy.IsSet.
	MaxTotalSessions uint64
	// MaxSessionsPerClient is the replicated RESPONSE-CACHE DEPTH: the number
	// of cached results one session retains, not a session count. See
	// proto.SessionPolicy.MaxSessionsPerClient for why the name says otherwise.
	MaxSessionsPerClient uint64
	// MaxSessionsPerOwner is the replicated cap on concurrent sessions per
	// declared owner; 0 disables it. See proto.SessionPolicy.MaxSessionsPerOwner.
	MaxSessionsPerOwner uint64
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
	// Identities maps replica IDs to the committed cryptographic node
	// identity the receive-side gate authorizes a claimed msg.From against
	// (see proto.Membership.Identities). It is a single map spanning every
	// role, and on a shard bootstrapped behind a transport it is complete: a
	// replica with no entry has every message refused.
	//
	// It MUST survive the snapshot-metadata round-trip, for the same reason
	// the session fields on Entry must survive the WAL one. Identities reach
	// membership through the log — the EntryInitialMembership at index 1 and
	// every EntryConfigChange — so a replica that rebuilds its configuration
	// from a SNAPSHOT whose log has been compacted past those entries has
	// only this record to learn them from. A backend that drops the field
	// therefore does not fail; it comes back with an identity-free
	// configuration and silently downgrades every member to address-based
	// authorization, which is exactly the forgery the identity binding
	// closes. Under the strict posture the same replica instead drops all
	// quorum-bearing traffic and the shard stops making progress.
	Identities map[uint64]string
	// RemovedIdentities maps a REMOVED replica ID to the identity the
	// configuration held for it when the removal committed (see
	// proto.Membership.RemovedIdentities). It is the deny-list the receive-side
	// gate evaluates against the AUTHENTICATED connection identity, rather than
	// against the claimed msg.From that a removed replica can simply change.
	//
	// It must survive this round-trip for a sharper reason than Identities. A
	// dropped Identities map degrades authorization to the address check; a
	// dropped RemovedIdentities map REVOKES A REVOCATION. The replica comes
	// back having forgotten which keypairs it ejected, so a decommissioned node
	// that still holds its certificate is readmitted — silently, with no error
	// anywhere, and precisely on the restart an operator performs after
	// removing it. Certificate revocation is the only remaining control at that
	// point, and it is opt-in.
	//
	// Its key set is a strict subset of Removed's: an entry appears only when a
	// removal commits for a member that had a committed identity. A backend
	// that persists Removed and drops this map is therefore internally
	// inconsistent, not merely lossy.
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
	// Compression is the algorithm the snapshot data file was written
	// through. It describes the ARTIFACT, not the writer's configuration:
	// every recovery path must build its reader chain from this field, not
	// from the local Config.SnapshotCompressionType, which is per-shard,
	// mutable across restarts, and independent on every host. The zero
	// value is config.NoCompression, which matches both the package default
	// and every record written before this field existed.
	Compression config.CompressionType
}

// CheckBounds is proto.Snapshot.CheckBounds for the storage-layer type: it
// refuses a position at or above proto.MaxLogIndex / proto.MaxTerm with the
// same sentinels, so a LogDB can apply the one protocol bound before a
// snapshot becomes durable without converting the type to do it.
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

// CopyEntryCmd returns e with its Cmd payload replaced by a private copy, or
// unchanged when there is no payload to copy.
//
// It is the READ-side counterpart of the deep copy every LogDB already performs
// on the write side, and it exists for the same reason pointed the other way.
// A backend stores entries in memory and serves them from there; returning the
// stored Entry by value copies the slice HEADER but not the bytes, so the
// caller and the store end up sharing one payload array. Nothing stops the
// caller writing through it — and what it would be writing into is the live log
// this replica serves to its followers, and replays to itself on restart. The
// corruption would be silent, unattributable, and would differ per replica,
// which is the worst shape a consensus bug can take.
//
// Copying per entry rather than once per batch is deliberate. One shared
// backing array would save allocations, but a caller that retains a SINGLE
// entry's Cmd — a state machine storing a value, say — would then pin the whole
// batch's payloads for as long as it holds that one entry. Per-entry copies
// make the retained memory exactly what was retained.
func CopyEntryCmd(e Entry) Entry {
	if len(e.Cmd) == 0 {
		return e
	}
	cmd := make([]byte, len(e.Cmd))
	copy(cmd, e.Cmd)
	e.Cmd = cmd
	return e
}
