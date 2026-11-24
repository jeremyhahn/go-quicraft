package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestNewRaftLog_RecordsRecoveryClamp asserts a recovered commit index past the last entry is clamped and recorded in Peer.RecoveryClamp.
func TestNewRaftLog_RecordsRecoveryClamp(t *testing.T) {
	entries := []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}

	if c := newRaftLog(newTestLogReader(entries, proto.State{Term: 1, Commit: 3})).recoveryClamp; c.Applied() {
		t.Fatalf("recoveryClamp = %+v for a commit index within the log, want none", c)
	}

	// Log holds entries 1..3, but the recovered hard state claims 9.
	l := newRaftLog(newTestLogReader(entries, proto.State{Term: 1, Commit: 9}))
	if l.committed != 3 {
		t.Fatalf("committed = %d with lastIndex = %d, want 3: a commit index past "+
			"the end of the log makes getConflictIndex skip entries this replica "+
			"does not have, producing a phantom ACK", l.committed, l.lastIndex())
	}
	if l.processed > l.committed {
		t.Fatalf("processed = %d exceeds committed = %d after clamping",
			l.processed, l.committed)
	}
	c := l.recoveryClamp
	want := RecoveryClamp{RecoveredCommit: 9, RecoveredTerm: 1, LastIndex: 3}
	if c != want {
		t.Fatalf("recoveryClamp = %+v, want %+v", c, want)
	}
	if !c.Applied() || c.EmptyLog() {
		t.Fatalf("partial clamp: Applied = %v, EmptyLog = %v, want true, false", c.Applied(), c.EmptyLog())
	}

	c = newRaftLog(newTestLogReader(nil, proto.State{Term: 5, Commit: 100})).recoveryClamp
	if !c.EmptyLog() || c.RecoveredCommit != 100 || c.RecoveredTerm != 5 {
		t.Fatalf("total-loss recoveryClamp = %+v, want an EmptyLog clamp of commit 100 at term 5", c)
	}
}

// TestNewRaftLog_TotalEntryLoss_ClampsCommittedToZero: with no entries and no snapshot, committed is clamped to zero.
func TestNewRaftLog_TotalEntryLoss_ClampsCommittedToZero(t *testing.T) {
	// Entry records lost (disk wipe, partial restore, truncated WAL); the
	// state record survived claiming 100.
	logdb := newTestLogReader(nil, proto.State{Term: 5, Commit: 100})
	l := newRaftLog(logdb)

	if l.committed != 0 {
		t.Fatalf("committed = %d over an EMPTY log (no entries, no snapshot): "+
			"the replica holds nothing at any index, so every index at or "+
			"below committed is skipped by getConflictIndex as \"cannot "+
			"conflict\" and handleAppend answers with a NON-rejecting "+
			"ReplicateResp for entries it does not have", l.committed)
	}
	if last := l.lastIndex(); last != 0 {
		t.Fatalf("lastIndex = %d over an EMPTY log: a last index fabricated "+
			"from the commit index makes the commit clamp compare "+
			"state.Commit against itself", last)
	}
	if l.processed != 0 {
		t.Fatalf("processed = %d, want 0", l.processed)
	}
	// processed=0 with firstIndex past the end was the second half of the
	// failure: entriesToApply could never return anything, so the shard sat
	// at "committed, 0 applied" forever with no error surfaced.
	if fi := l.firstIndex(); fi != 1 {
		t.Fatalf("firstIndex = %d over an EMPTY log, want 1: a first index "+
			"beyond committed makes entriesToApply return nothing forever",
			fi)
	}
}

// TestTotalEntryLoss_NoPhantomReplicateAck: a replica that lost its entries does not acknowledge indices it lacks.
func TestTotalEntryLoss_NoPhantomReplicateAck(t *testing.T) {
	lr := newTestLogReader(nil, proto.State{Term: 5, Commit: 100})
	lr.membership = threeNodeMembership()

	r, err := newRaft(raftTestCfg(1, 3), lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	// Leader 1 backtracks and probes at index 50.
	err = r.step(proto.Message{
		Type:     proto.Replicate,
		Term:     5,
		From:     1,
		To:       3,
		LogIndex: 50,
		LogTerm:  5,
		Commit:   100,
	})
	if err != nil {
		t.Fatalf("step Replicate: %v", err)
	}

	msgs := r.drainMessages()
	var resp *proto.Message
	for i := range msgs {
		if msgs[i].Type == proto.ReplicateResp {
			resp = &msgs[i]
			break
		}
	}
	if resp == nil {
		t.Fatal("no ReplicateResp produced for the leader's probe")
	}
	if !resp.Reject {
		t.Fatalf("replica with an EMPTY log answered the probe at index 50 "+
			"with a NON-rejecting ReplicateResp{LogIndex:%d}: the leader "+
			"counts that phantom match toward quorum and commits entries "+
			"this replica holds nothing for", resp.LogIndex)
	}
}

// The inner clamp: a snapshot outliving its entries must leave processed at or below the clamped committed.
func TestNewRaftLog_SnapshotAheadOfEntries_ClampsProcessedToCommitted(t *testing.T) {
	// Entries 1..5 survived; the snapshot record claims index 10.
	entries := []proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	lr := newTestLogReader(entries, proto.State{Term: 2, Commit: 5})
	lr.snapshot = proto.Snapshot{Index: 10, Term: 2}

	l := newRaftLog(lr)

	last := l.lastIndex()
	if last != 5 {
		t.Fatalf("fixture: lastIndex = %d, want 5 (the entries the log holds)", last)
	}
	if l.committed > last {
		t.Fatalf("committed = %d exceeds lastIndex = %d", l.committed, last)
	}
	if l.processed > l.committed {
		t.Fatalf("processed = %d exceeds committed = %d after clamping: "+
			"commitUpdate asserts LastApplied <= processed <= committed, so "+
			"the first Commit after this restart panics the commit worker",
			l.processed, l.committed)
	}
	if l.processed != 5 {
		t.Fatalf("processed = %d, want 5 (clamped to the clamped committed)",
			l.processed)
	}
}
