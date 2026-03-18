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

package proto

// UpdateCommit tracks what has been committed from an Update. It is used
// internally by the engine to coordinate between the commit and apply
// workers. The wire format is 48 bytes (6 fields, Processed stored as
// uint64 on wire):
//
//	[StableLogTo:8][StableLogTerm:8][StableSnapshotTo:8]
//	[Processed:8][LastApplied:8][ReadyToRead:8]
type UpdateCommit struct {
	// StableLogTo is the highest log index that has been persisted to LogDB.
	StableLogTo uint64
	// StableLogTerm is the term of the entry at StableLogTo.
	StableLogTerm uint64
	// StableSnapshotTo is the snapshot index that has been persisted.
	StableSnapshotTo uint64
	// Processed indicates whether this update has been fully processed.
	Processed bool
	// LastApplied is the highest log index applied to the state machine.
	LastApplied uint64
	// ReadyToRead is the number of confirmed read index requests.
	ReadyToRead uint64
}

// updateCommitSize is the fixed wire format size: 6 fields * 8 bytes.
const updateCommitSize = 6 * 8 // 48 bytes

// Size returns the marshaled size in bytes. Always returns 48.
func (uc *UpdateCommit) Size() int {
	return updateCommitSize
}

// MarshalTo writes the update commit into buf and returns bytes written.
// buf must be at least 48 bytes.
func (uc *UpdateCommit) MarshalTo(buf []byte) (int, error) {
	if len(buf) < updateCommitSize {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], uc.StableLogTo)
	putUint64(buf[8:], uc.StableLogTerm)
	putUint64(buf[16:], uc.StableSnapshotTo)
	putBool(buf[24:], uc.Processed)
	putUint64(buf[32:], uc.LastApplied)
	putUint64(buf[40:], uc.ReadyToRead)
	return updateCommitSize, nil
}

// UnmarshalFrom reads the update commit from buf and returns bytes consumed.
// Returns ErrBufferTooSmall on truncated input.
func (uc *UpdateCommit) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < updateCommitSize {
		return 0, ErrBufferTooSmall
	}
	uc.StableLogTo = getUint64(buf[0:])
	uc.StableLogTerm = getUint64(buf[8:])
	uc.StableSnapshotTo = getUint64(buf[16:])
	uc.Processed = getBool(buf[24:])
	uc.LastApplied = getUint64(buf[32:])
	uc.ReadyToRead = getUint64(buf[40:])
	return updateCommitSize, nil
}

// Reset clears all fields for reuse.
func (uc *UpdateCommit) Reset() {
	uc.StableLogTo = 0
	uc.StableLogTerm = 0
	uc.StableSnapshotTo = 0
	uc.Processed = false
	uc.LastApplied = 0
	uc.ReadyToRead = 0
}

// Update represents a batch of changes produced by a Raft step for a
// single shard. It is the primary data structure that flows between the
// engine and LogDB. Update is an internal struct that does not require
// wire serialization (it is never sent over the network). Components
// pass it by reference within the same process.
type Update struct {
	// ShardID identifies the Raft shard this update belongs to.
	ShardID uint64
	// ReplicaID identifies the replica within the shard.
	ReplicaID uint64
	// State is the hard state to persist.
	State State
	// FirstIndex is the first log index in EntriesToSave.
	FirstIndex uint64
	// LastIndex is the last log index in EntriesToSave.
	LastIndex uint64
	// EntriesToSave contains log entries to append to the log.
	EntriesToSave []Entry
	// CommittedEntries contains entries committed and ready to apply.
	CommittedEntries []Entry
	// Messages contains Raft protocol messages to send.
	Messages []Message
	// Snapshot contains snapshot metadata to persist.
	Snapshot Snapshot
	// ReadyToRead contains confirmed read index requests.
	ReadyToRead []ReadyToRead
	// UpdateCommit tracks commit progress.
	UpdateCommit UpdateCommit
	// LastApplied is the highest applied log index.
	LastApplied uint64
	// ReadOnly indicates this update contains only ReadyToRead states
	// with no entries, state changes, or snapshots requiring persistence.
	// The engine step worker bypasses the commit/apply pipeline for
	// read-only updates, avoiding unnecessary LogDB writes and goroutine
	// hops that would otherwise add ~300us per ReadIndex request.
	ReadOnly bool
	// committedEntriesPooled is the pooled slice backing CommittedEntries.
	// When non-nil, the apply worker returns this to proto.PutEntrySlice
	// after applying entries to reclaim the allocation. This field is
	// unexported because it is an internal optimization detail: only the
	// raft log's entriesToApply sets it, and only the engine apply worker
	// returns it.
	committedEntriesPooled *[]Entry
}

// SetCommittedEntriesPooled associates a pooled []Entry slice with this
// Update. The apply worker uses ReleaseCommittedEntries to return the
// slice to the pool after entries are applied. The caller must ensure
// that CommittedEntries is a sub-slice of the pooled slice.
func (u *Update) SetCommittedEntriesPooled(s *[]Entry) {
	u.committedEntriesPooled = s
}

// ReleaseCommittedEntries returns the pooled committed entries slice to
// the entry slice pool, if one was associated via SetCommittedEntriesPooled.
// After this call, CommittedEntries must not be accessed. This method is
// idempotent; subsequent calls are no-ops.
func (u *Update) ReleaseCommittedEntries() {
	if u.committedEntriesPooled != nil {
		PutEntrySlice(u.committedEntriesPooled)
		u.committedEntriesPooled = nil
		u.CommittedEntries = nil
	}
}

// Reset clears all fields for reuse. If a pooled committed entries
// slice is associated, it is returned to the pool first.
func (u *Update) Reset() {
	u.ReleaseCommittedEntries()
	u.ShardID = 0
	u.ReplicaID = 0
	u.State.Reset()
	u.FirstIndex = 0
	u.LastIndex = 0
	u.EntriesToSave = nil
	u.CommittedEntries = nil
	u.Messages = nil
	u.Snapshot.Reset()
	u.ReadyToRead = nil
	u.UpdateCommit.Reset()
	u.LastApplied = 0
	u.ReadOnly = false
}
