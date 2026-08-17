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
	"errors"
	"strconv"
)

// Sentinel errors for LogDB operations. These are pre-allocated for
// zero-alloc returns and use with errors.Is().
var (
	// ErrNoState is returned by ReadState when no persisted hard state
	// or membership exists for the requested node.
	ErrNoState = errors.New("logdb: no saved state")

	// ErrNoSnapshot is returned by GetSnapshot when no snapshot metadata
	// has been persisted for the requested node.
	ErrNoSnapshot = errors.New("logdb: no saved snapshot")

	// ErrNoBootstrap is returned by GetBootstrap when no bootstrap
	// information has been saved for the requested node.
	ErrNoBootstrap = errors.New("logdb: no bootstrap info")

	// ErrClosed is returned when an operation is attempted on a LogDB
	// instance that has already been closed.
	ErrClosed = errors.New("logdb: database closed")

	// ErrCompacted is returned when a requested log entry has already
	// been removed by compaction.
	ErrCompacted = errors.New("logdb: entry compacted")

	// ErrUnavailable is returned when requested log entries are beyond
	// the available range (index exceeds last known entry).
	ErrUnavailable = errors.New("logdb: entry unavailable")

	// ErrSnapshotOutOfDate is returned when a snapshot is older than
	// or equal to the currently recorded snapshot.
	ErrSnapshotOutOfDate = errors.New("logdb: snapshot out of date")

	// ErrLogGapDetected is returned when a hole in the stored log is
	// detected: SetRange sees new entries that are not contiguous with the
	// existing range, EntryRange finds an entry count that does not match
	// the first..last index span, or IterateEntries finds that the stored
	// range does not begin at the requested index. A gap indicates a bug in
	// the entry persistence pipeline or on-disk corruption; it must never
	// be served as a silently shortened result (Log Matching violation).
	ErrLogGapDetected = errors.New("logdb: gap detected in log entries")

	// ErrEntriesBelowCompactionFloor is returned by SaveState when an Update
	// carries entries at or below the node's compaction floor.
	//
	// Those indexes are provably applied and already deleted, so the write can
	// only be a caller bug — raft sends a lagging follower an InstallSnapshot,
	// never entries beneath its own floor, and a local proposer never proposes
	// below its applied index. The write is refused rather than clamped because
	// every other outcome is worse. Storing it destroys the live suffix above
	// the floor (truncate-and-append semantics replace everything from the
	// first new index onward) and leaves EntryRange reporting a first index
	// that IterateEntries then rejects as ErrCompacted, so the log describes
	// itself inconsistently. Clamping silently discards entries the caller was
	// told were persisted. Failing loudly is the only response that neither
	// loses data nor lies about it.
	ErrEntriesBelowCompactionFloor = errors.New("logdb: entries at or below the compaction floor")
)

// EntriesBelowCompactionFloorError reports which node and which indexes were
// refused, so the caller bug can be located rather than merely observed.
//
// It wraps ErrEntriesBelowCompactionFloor, so errors.Is against the sentinel
// holds for every backend.
type EntriesBelowCompactionFloorError struct {
	// ShardID and ReplicaID identify the node whose write was refused.
	ShardID   uint64
	ReplicaID uint64
	// FirstIndex is the lowest index the refused Update carried.
	FirstIndex uint64
	// CompactedTo is the node's compaction floor at the moment of the refusal:
	// every index at or below it has been removed.
	CompactedTo uint64
}

// Error implements the error interface.
func (e *EntriesBelowCompactionFloorError) Error() string {
	return ErrEntriesBelowCompactionFloor.Error() +
		" (shard " + strconv.FormatUint(e.ShardID, 10) +
		", replica " + strconv.FormatUint(e.ReplicaID, 10) +
		", first index " + strconv.FormatUint(e.FirstIndex, 10) +
		", compacted to " + strconv.FormatUint(e.CompactedTo, 10) + ")"
}

// Unwrap returns the sentinel so errors.Is holds against it.
func (e *EntriesBelowCompactionFloorError) Unwrap() error {
	return ErrEntriesBelowCompactionFloor
}
