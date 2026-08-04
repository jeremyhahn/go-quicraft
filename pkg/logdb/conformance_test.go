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

// Conformance suite pinning the LogDB interface contract against every
// implementation (memdb and waldb). It guards the contract items that
// previously diverged: IterateEntries error semantics and maxSize budget
// accounting, EntryRange span semantics, snapshot persistence through
// SaveState, and Cmd deep-copy on SaveState.

package logdb_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// backends enumerates every LogDB implementation under conformance test.
func backends(t *testing.T) map[string]func(t *testing.T) logdb.LogDB {
	t.Helper()
	return map[string]func(t *testing.T) logdb.LogDB{
		"memdb": func(t *testing.T) logdb.LogDB {
			db := memdb.New()
			t.Cleanup(func() { _ = db.Close() })
			return db
		},
		"waldb": func(t *testing.T) logdb.LogDB {
			db, err := waldb.Open(t.TempDir(), waldb.WithNoSync(true))
			if err != nil {
				t.Fatalf("waldb.Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			return db
		},
	}
}

// forEachBackend runs fn as a subtest against every implementation.
func forEachBackend(t *testing.T, fn func(t *testing.T, db logdb.LogDB)) {
	for name, open := range backends(t) {
		t.Run(name, func(t *testing.T) {
			fn(t, open(t))
		})
	}
}

// saveEntries persists sequential entries [start, start+count) with the
// given per-entry Cmd size.
func saveEntries(t *testing.T, db logdb.LogDB, shardID, replicaID, start, count uint64, cmdSize int) {
	t.Helper()
	entries := make([]logdb.Entry, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = logdb.Entry{Index: start + i, Term: 1, Cmd: make([]byte, cmdSize)}
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID, Entries: entries,
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
}

func TestConformanceIterateEntriesRoundTrip(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 5, 8)

		got, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 6, 0)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("got %d entries, want 5", len(got))
		}
		wantSize := uint64(5 * (logdb.EntryOverheadBytes + 8))
		if size != wantSize {
			t.Fatalf("size = %d, want %d (EntryOverheadBytes budget)", size, wantSize)
		}
		for i, e := range got {
			if e.Index != uint64(i+1) {
				t.Fatalf("entry %d has index %d, want %d", i, e.Index, i+1)
			}
		}
	})
}

func TestConformanceIterateEntriesUnknownNode(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		if _, _, err := db.IterateEntries(nil, 0, 99, 99, 1, 5, 0); !errors.Is(err, logdb.ErrUnavailable) {
			t.Fatalf("IterateEntries unknown node = %v, want ErrUnavailable", err)
		}
	})
}

func TestConformanceIterateEntriesEmptyRangeIsNoop(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		got, size, err := db.IterateEntries(nil, 7, 99, 99, 5, 5, 0)
		if err != nil {
			t.Fatalf("IterateEntries low>=high = %v, want nil", err)
		}
		if len(got) != 0 || size != 7 {
			t.Fatalf("IterateEntries low>=high = (%d entries, size %d), want (0, 7)", len(got), size)
		}
	})
}

func TestConformanceIterateEntriesRangeWithoutEntries(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 3, 4)
		// The node exists but [10, 12) holds nothing.
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 10, 12, 0); !errors.Is(err, logdb.ErrUnavailable) {
			t.Fatalf("IterateEntries beyond last = %v, want ErrUnavailable", err)
		}
	})
}

func TestConformanceCompactionFloorReturnsErrCompacted(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		saveEntries(t, db, 1, 1, 1, 10, 4)
		if err := db.RemoveEntriesTo(1, 1, 4); err != nil {
			t.Fatalf("RemoveEntriesTo: %v", err)
		}

		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 3, 11, 0); !errors.Is(err, logdb.ErrCompacted) {
			t.Fatalf("IterateEntries below floor = %v, want ErrCompacted", err)
		}
		got, _, err := db.IterateEntries(nil, 0, 1, 1, 5, 11, 0)
		if err != nil {
			t.Fatalf("IterateEntries above floor: %v", err)
		}
		if len(got) != 6 || got[0].Index != 5 {
			t.Fatalf("live suffix = %d entries starting %d, want 6 starting 5", len(got), got[0].Index)
		}
	})
}

func TestConformanceMaxSizeBudget(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		const cmdSize = 100
		perEntry := uint64(logdb.EntryOverheadBytes + cmdSize)
		saveEntries(t, db, 1, 1, 1, 10, cmdSize)

		// Budget for exactly two entries plus slack smaller than a third:
		// iteration stops BEFORE the entry that would exceed the budget.
		got, size, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, 2*perEntry+10)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d entries under 2-entry budget, want 2", len(got))
		}
		if size != 2*perEntry {
			t.Fatalf("size = %d, want %d", size, 2*perEntry)
		}

		// A budget below a single entry still returns at least one entry.
		got, _, err = db.IterateEntries(nil, 0, 1, 1, 1, 11, 1)
		if err != nil {
			t.Fatalf("IterateEntries tiny budget: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d entries under tiny budget, want 1", len(got))
		}
	})
}

func TestConformanceEntryRangeSpan(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		first, length, err := db.EntryRange(1, 1)
		if err != nil || first != 0 || length != 0 {
			t.Fatalf("EntryRange empty = (%d, %d, %v), want (0, 0, nil)", first, length, err)
		}

		saveEntries(t, db, 1, 1, 3, 5, 4)
		first, length, err = db.EntryRange(1, 1)
		if err != nil {
			t.Fatalf("EntryRange: %v", err)
		}
		if first != 3 || length != 5 {
			t.Fatalf("EntryRange = (%d, %d), want (3, 5)", first, length)
		}
	})
}

func TestConformanceSaveStatePersistsSnapshot(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		snap := logdb.Snapshot{
			Index:    10,
			Term:     3,
			Filepath: "snap-10.dat",
			Membership: logdb.Membership{
				ConfigChangeID: 7,
				Addresses:      map[uint64]string{1: "a1"},
			},
		}
		if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: snap}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}

		got, err := db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot after SaveState: %v", err)
		}
		if got.Index != 10 || got.Term != 3 || got.Filepath != "snap-10.dat" {
			t.Fatalf("GetSnapshot = %+v, want the SaveState snapshot", got)
		}

		// A stale snapshot in a later Update must not regress it.
		stale := logdb.Snapshot{Index: 5, Term: 2}
		if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: stale}}); err != nil {
			t.Fatalf("SaveState stale: %v", err)
		}
		got, err = db.GetSnapshot(1, 1)
		if err != nil {
			t.Fatalf("GetSnapshot: %v", err)
		}
		if got.Index != 10 {
			t.Fatalf("GetSnapshot.Index = %d after stale update, want 10", got.Index)
		}
	})
}

func TestConformanceSaveStateDeepCopiesCmd(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		buf := []byte("payload-owned-by-caller")
		if err := db.SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1,
			Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: buf}},
		}}); err != nil {
			t.Fatalf("SaveState: %v", err)
		}

		// Caller reuses its buffer after SaveState returns.
		copy(buf, "XXXXXXXXXX")

		got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0)
		if err != nil {
			t.Fatalf("IterateEntries: %v", err)
		}
		if len(got) != 1 || !bytes.Equal(got[0].Cmd, []byte("payload-owned-by-caller")) {
			t.Fatalf("stored Cmd = %q, want %q (caller buffer aliased)",
				got[0].Cmd, "payload-owned-by-caller")
		}
	})
}

func TestConformanceClosedErrors(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db logdb.LogDB) {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 0); !errors.Is(err, logdb.ErrClosed) {
			t.Fatalf("IterateEntries after Close = %v, want ErrClosed", err)
		}
		if _, _, err := db.EntryRange(1, 1); !errors.Is(err, logdb.ErrClosed) {
			t.Fatalf("EntryRange after Close = %v, want ErrClosed", err)
		}
		if err := db.SaveState(nil); !errors.Is(err, logdb.ErrClosed) {
			t.Fatalf("SaveState after Close = %v, want ErrClosed", err)
		}
	})
}
