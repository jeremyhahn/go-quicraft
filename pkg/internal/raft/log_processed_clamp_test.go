package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestNewRaftLog_ProcessedNeverExceedsCommitted asserts the frontier invariant
// holds on a recovery shape where the COMMIT clamp does not fire.
//
// processed is seeded from ss.Index, or firstIdx-1 when there is no snapshot,
// and neither is bounded by the recovered HardState.Commit. With a gapped
// entry range (firstIndex 100) and a lower commit (50), committed <=
// lastIndex() so the commit clamp is skipped entirely, and the log came up
// with processed 99 > committed 50. handleSnapshot's matching branch then
// calls appliedTo(ss.Index) for an index below processed, tripping appliedTo's
// monotonicity assertion and taking the step worker down.
//
// Found by FuzzRaftLogRecovery. Not reachable through recoverLogReader today
// (a gapped start with no snapshot fails with ErrLogGapDetected before the
// shard starts), which is exactly why it needs its own test: the reachability
// argument lives in a different file from the invariant.
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
