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

package engine

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// removeFailingLogDB fails RemoveEntriesTo and passes everything else through.
type removeFailingLogDB struct{ logdb.LogDB }

var errRemoveRefused = errors.New("injected RemoveEntriesTo failure")

func (removeFailingLogDB) RemoveEntriesTo(_, _, _ uint64) error { return errRemoveRefused }

// advancingReader returns a log reader holding entries 1..last at term 1, so
// Compact to any index at or below last really advances its marker.
func advancingReader(t *testing.T, shardID, replicaID uint64, db logdb.LogDB, last uint64) *logdb.LogReader {
	t.Helper()
	lr := logdb.NewLogReader(shardID, replicaID, db)
	entries := make([]proto.Entry, last)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i) + 1, Term: 1}
	}
	if err := lr.Append(entries); err != nil {
		t.Fatalf("LogReader.Append: %v", err)
	}
	return lr
}

// TestSaveSnapshot_ReportsWhatCompactionDid pins the outcome the pool hands to
// OnLogCompacted, which is what the host's compaction events report: a
// compaction is reported only when THIS save advanced the reader's marker.
// The listener must not hear "compacted" for every case below.
func TestSaveSnapshot_ReportsWhatCompactionDid(t *testing.T) {
	t.Run("compacted", func(t *testing.T) {
		f := newResaveFixture(t, 200)
		f.node.logReader = advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 200)
		_, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if want := (LogCompaction{Index: 190, Reader: true, LogDB: true}); got != want {
			t.Fatalf("compaction = %+v, want %+v", got, want)
		}
	})
	t.Run("LogDB step failed", func(t *testing.T) {
		f := newResaveFixtureDB(t, 200, func(db *snapshotTestLogDB) logdb.LogDB {
			return removeFailingLogDB{db}
		})
		f.node.logReader = advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 200)
		_, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if want := (LogCompaction{Index: 190, Reader: true}); got != want {
			t.Fatalf("compaction = %+v, want %+v: storage was not reclaimed", got, want)
		}
	})
	t.Run("marker already past", func(t *testing.T) {
		// An earlier snapshot, a receive or an import took the marker past
		// compactIndex, so Compact answers ErrCompacted and this save
		// compacted nothing — it must not be reported at index 190.
		f := newResaveFixture(t, 200)
		lr := advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 200)
		if err := lr.Compact(195); err != nil {
			t.Fatalf("pre-compacting the reader: %v", err)
		}
		f.node.logReader = lr
		_, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if got != (LogCompaction{}) {
			t.Fatalf("compaction = %+v, want none: the marker was already past 190", got)
		}
	})
	t.Run("marker exactly at compactIndex", func(t *testing.T) {
		// Compact at the current marker is a nil
		// no-op, so a nil error alone reported a compaction that moved nothing
		// — reachable when a received snapshot set the marker to S and a later
		// save lands at exactly S + CompactionOverhead.
		f := newResaveFixture(t, 200)
		lr := advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 200)
		if err := lr.Compact(190); err != nil {
			t.Fatalf("pre-compacting the reader: %v", err)
		}
		f.node.logReader = lr
		_, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if got != (LogCompaction{}) {
			t.Fatalf("compaction = %+v, want none: the marker was already at 190", got)
		}
	})
	t.Run("no reader", func(t *testing.T) {
		// The skip guard is scoped to a reader that FAILED to advance, so the
		// LogDB removal still runs on the nil-reader path that predates it;
		// otherwise "skip the removal" could be satisfied by never compacting.
		f := newResaveFixture(t, 200)
		f.node.logReader = nil
		_, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if got != (LogCompaction{}) {
			t.Fatalf("compaction = %+v, want none: a nil reader proves nothing advanced", got)
		}
		f.ldb.mu.Lock()
		removedTo := f.ldb.removedTo
		f.ldb.mu.Unlock()
		if removedTo != 190 {
			t.Fatalf("RemoveEntriesTo ran with index %d, want 190: log compaction must "+
				"still happen when nothing blocked the marker", removedTo)
		}
	})
	t.Run("reader marker could not advance", func(t *testing.T) {
		// Regression: a non-ErrCompacted LogReader.Compact failure was only
		// logged and RemoveEntriesTo ran regardless, leaving the reader's
		// markerIndex stale over entries the LogDB had discarded. The snapshot
		// is durable by then, so the removal must be skipped and retried by
		// the next save. An empty reader makes Compact(190) return
		// ErrUnavailable, a real non-ErrCompacted error.
		f := newResaveFixture(t, 200)
		lr := logdb.NewLogReader(f.req.shardID, f.req.replicaID, f.ldb)
		f.node.logReader = lr
		if err := lr.Compact(190); err == nil {
			t.Fatal("test premise broken: LogReader.Compact(190) was expected to fail " +
				"on an empty reader")
		}
		index, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if index != 200 {
			t.Fatalf("saveSnapshotCompacting index = %d, want 200", index)
		}
		if got != (LogCompaction{}) {
			t.Fatalf("compaction = %+v, want none: the removal was skipped", got)
		}
		f.ldb.mu.Lock()
		removedTo := f.ldb.removedTo
		f.ldb.mu.Unlock()
		if removedTo != 0 {
			t.Fatalf("RemoveEntriesTo(%d) ran after LogReader.Compact failed: the "+
				"LogReader's markerIndex is still stale, so raft keeps probing for "+
				"entries the LogDB has discarded", removedTo)
		}
	})
	t.Run("within CompactionOverhead", func(t *testing.T) {
		f := newResaveFixture(t, 8) // overhead is 10
		f.node.logReader = advancingReader(t, f.req.shardID, f.req.replicaID, f.ldb, 8)
		_, got, err := f.pool.saveSnapshotCompacting(f.req)
		if err != nil {
			t.Fatalf("saveSnapshotCompacting: %v", err)
		}
		if got != (LogCompaction{}) {
			t.Fatalf("compaction = %+v, want none below the overhead", got)
		}
		f.ldb.mu.Lock()
		removedTo := f.ldb.removedTo
		f.ldb.mu.Unlock()
		if removedTo != 0 {
			t.Fatalf("RemoveEntriesTo(%d) ran within the overhead", removedTo)
		}
	})
}
