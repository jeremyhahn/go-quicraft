package logdb

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestLogReaderAppend_DoesNotCacheTermsBelowMarker verifies that Append caches
// terms only for entries that are actually inside the tracked range.
//
// setRangeLocked clamps an append whose head falls below the current first
// index — those entries are already superseded by a snapshot or compaction —
// but the term cache is populated from the caller's slice. Caching the trimmed
// prefix leaves terms for indices the log no longer holds, and termLocked
// consults termBuf BEFORE the range check, so those indices answer with a term
// instead of ErrCompacted.
//
// The consequence is not a stale read: the leader's sendAppend resolves
// term(prevLogIndex) for a slow follower, gets a term for an index that was
// compacted away instead of the ErrCompacted that triggers the snapshot
// fallback, and so never sends InstallSnapshot. The follower can never catch up.
func TestLogReaderAppend_DoesNotCacheTermsBelowMarker(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// Snapshot at 100: entries at or below 100 are gone, first index is 101.
	if err := lr.ApplySnapshot(proto.Snapshot{Index: 100, Term: 5}); err != nil {
		t.Fatalf("ApplySnapshot failed: %v", err)
	}

	// An append whose head overlaps the compacted region. Only 101 and 102
	// are live; 98, 99 and 100 are superseded by the snapshot.
	entries := []proto.Entry{
		{Index: 98, Term: 3},
		{Index: 99, Term: 3},
		{Index: 100, Term: 3},
		{Index: 101, Term: 5},
		{Index: 102, Term: 5},
	}
	if err := lr.Append(entries); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Every compacted index must report ErrCompacted, not a cached term.
	for _, idx := range []uint64{98, 99} {
		term, err := lr.Term(idx)
		if err == nil {
			t.Errorf("Term(%d) = %d with nil error; want ErrCompacted "+
				"(a term for a compacted index suppresses the leader's "+
				"InstallSnapshot fallback)", idx, term)
			continue
		}
		if !errors.Is(err, ErrCompacted) {
			t.Errorf("Term(%d) error = %v, want ErrCompacted", idx, err)
		}
	}

	// The snapshot index itself resolves from the marker, and the live
	// entries keep their terms.
	if term, err := lr.Term(100); err != nil || term != 5 {
		t.Errorf("Term(100) = %d, %v; want 5, nil (the snapshot marker)", term, err)
	}
	for _, tc := range []struct{ index, want uint64 }{{101, 5}, {102, 5}} {
		term, err := lr.Term(tc.index)
		if err != nil {
			t.Errorf("Term(%d) failed: %v", tc.index, err)
			continue
		}
		if term != tc.want {
			t.Errorf("Term(%d) = %d, want %d", tc.index, term, tc.want)
		}
	}
}

// TestLogReaderAppend_WhollySupersededIsNoop verifies that an append entirely
// below the marker leaves the reader untouched rather than caching terms for
// indices the log does not hold.
func TestLogReaderAppend_WhollySupersededIsNoop(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	if err := lr.ApplySnapshot(proto.Snapshot{Index: 100, Term: 5}); err != nil {
		t.Fatalf("ApplySnapshot failed: %v", err)
	}
	if err := lr.Append([]proto.Entry{{Index: 50, Term: 2}, {Index: 51, Term: 2}}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if _, err := lr.Term(50); !errors.Is(err, ErrCompacted) {
		t.Errorf("Term(50) error = %v, want ErrCompacted", err)
	}
	first, last := lr.IndexRange()
	if first != 101 || last != 100 {
		t.Errorf("IndexRange() = (%d, %d), want (101, 100) — an empty range "+
			"above the snapshot marker", first, last)
	}
}
