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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSaveSnapshot_CompactFailureSkipsLogDBRemoval is the H3 regression test.
//
// saveSnapshot advances the LogReader's compaction marker BEFORE removing the
// entries from LogDB, and the comment above that ordering states exactly why:
// LogReader.Compact resolves the term at compactIndex, which stops being
// readable once RemoveEntriesTo has run, so the reverse order leaves markerIndex
// stale and "raft keeps probing for entries the LogDB has already discarded".
//
// The failure branch then did the reverse anyway. A non-ErrCompacted error was
// only slog.Warn'd and RemoveEntriesTo ran regardless, producing precisely the
// stale-marker state the ordering exists to prevent — and by that point
// gcSnapshotDirs has already reclaimed older artifacts, so the entries are gone
// from both the log and the snapshot tree.
//
// Compaction is discretionary: the snapshot is durable, recorded and published
// by this point. So a marker that could not be advanced must skip the removal
// entirely and let the next snapshot retry.
//
// The failure is injected honestly: the LogReader has no entries, so
// Compact(compactIndex) with compactIndex above its last index returns
// ErrUnavailable — a real, reachable non-ErrCompacted error.
func TestSaveSnapshot_CompactFailureSkipsLogDBRemoval(t *testing.T) {
	f := newResaveFixture(t, 200)

	// An empty LogReader: lastIndex is 0, so Compact(190) — snapshotIndex 200
	// minus CompactionOverhead 10 — returns ErrUnavailable.
	lr := logdb.NewLogReader(f.req.shardID, f.req.replicaID, f.ldb)
	f.node.logReader = lr

	if err := lr.Compact(190); err == nil {
		t.Fatal("test premise broken: LogReader.Compact(190) was expected to fail " +
			"on an empty reader")
	}

	index, err := f.pool.saveSnapshot(f.req)
	if err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}
	if index != 200 {
		t.Fatalf("saveSnapshot index = %d, want 200", index)
	}

	f.ldb.mu.Lock()
	removedTo := f.ldb.removedTo
	f.ldb.mu.Unlock()
	if removedTo != 0 {
		t.Fatalf("RemoveEntriesTo(%d) ran after LogReader.Compact failed: the "+
			"LogReader's markerIndex is still stale, so raft keeps probing for "+
			"entries the LogDB has discarded", removedTo)
	}
}

// TestSaveSnapshot_CompactSuccessStillRemovesEntries pins the other side of the
// guard: when the marker DOES advance, compaction proceeds exactly as before.
// Without this, "skip the removal" could be satisfied by never compacting at
// all.
func TestSaveSnapshot_CompactSuccessStillRemovesEntries(t *testing.T) {
	f := newResaveFixture(t, 200)

	// No LogReader at all: the guard is scoped to a reader that FAILED to
	// advance, so the removal must still run on the nil-reader path that
	// predates it.
	f.node.logReader = nil

	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}

	f.ldb.mu.Lock()
	removedTo := f.ldb.removedTo
	f.ldb.mu.Unlock()
	if removedTo != 190 {
		t.Fatalf("RemoveEntriesTo ran with index %d, want 190: log compaction must "+
			"still happen when nothing blocked the marker", removedTo)
	}
}
