package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestNewRaftLog_ProcessedNeverExceedsCommitted verifies processed is clamped to committed on a gapped recovery the commit clamp skips.
func TestNewRaftLog_ProcessedNeverExceedsCommitted(t *testing.T) {
	entries := []proto.Entry{
		{Index: 100, Term: 7}, {Index: 101, Term: 7}, {Index: 102, Term: 7},
	}
	lr := newTestLogReader(entries, proto.State{Term: 7, Commit: 50})
	l := newRaftLog(lr)

	if l.processed > l.committed {
		t.Fatalf("processed = %d exceeds committed = %d: appliedTo asserts "+
			"monotonicity, so a later snapshot install at an index between "+
			"them panics the step worker", l.processed, l.committed)
	}
}

// TestNewRaftLog_ProcessedPreservedWhenWithinCommitted is the control: the
// clamp must not lower a legitimate applied frontier, which would re-apply
// entries the state machine has already consumed.
func TestNewRaftLog_ProcessedPreservedWhenWithinCommitted(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	}
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 3})
	if err := lr.ApplySnapshot(proto.Snapshot{Index: 2, Term: 1}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	l := newRaftLog(lr)

	if l.processed != 2 {
		t.Errorf("processed = %d, want 2: a snapshot-seeded frontier within "+
			"committed must survive recovery unchanged", l.processed)
	}
}
