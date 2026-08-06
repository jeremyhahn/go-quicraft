package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestNewRaftLog_ClampsCommittedToLastIndex asserts that a recovered commit
// index ahead of the last recovered entry is clamped down.
//
// getConflictIndex skips every entry at or below committed as "committed
// entries cannot conflict". With committed past the end of the log, tryAppend
// reports success for indices this replica does not hold, handleAppend replies
// with a NON-rejecting ReplicateResp, and the leader counts that phantom match
// toward quorum -- committing entries a majority does not actually have. The
// invariant in commitTo would panic eventually, but only after the false
// acknowledgement was already on the wire.
//
// Reachable from a torn WAL batch that kept the state record while losing
// entries behind it, or a snapshot record that outlived them.
func TestNewRaftLog_ClampsCommittedToLastIndex(t *testing.T) {
	// Log holds entries 1..5, but the recovered hard state claims 99.
	entries := []proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	logdb := newTestLogReader(entries, proto.State{Term: 1, Commit: 99})
	l := newRaftLog(logdb)

	last := l.lastIndex()
	if l.committed > last {
		t.Fatalf("committed = %d with lastIndex = %d: a commit index past the "+
			"end of the log makes getConflictIndex skip entries this replica "+
			"does not have, producing a phantom ACK", l.committed, last)
	}
	if l.processed > l.committed {
		t.Errorf("processed = %d exceeds committed = %d after clamping",
			l.processed, l.committed)
	}
}

// TestNewRaftLog_KeepsValidCommitted verifies the clamp does not lower a
// legitimate commit index, which would silently un-commit entries.
func TestNewRaftLog_KeepsValidCommitted(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	}
	logdb := newTestLogReader(entries, proto.State{Term: 1, Commit: 3})
	l := newRaftLog(logdb)

	if l.committed != 3 {
		t.Errorf("committed = %d, want 3: a commit index within the log must "+
			"survive recovery unchanged", l.committed)
	}
}
