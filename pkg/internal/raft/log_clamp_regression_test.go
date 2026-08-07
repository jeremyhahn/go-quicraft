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

// TestNewRaftLog_TotalEntryLoss_ClampsCommittedToZero covers the case the
// partial-loss test above does NOT reach: the log holds NOTHING — no entries
// and no snapshot — while the recovered hard state still claims a commit
// index.
//
// The clamp was VACUOUS here. newRaftLog fabricated the in-memory boundary
// from state.Commit when the entry range was empty, so newInMemory set
// markerIndex = state.Commit+1 and lastIndex() fell through to
// inmem.lastIndex() == state.Commit. `l.committed > l.lastIndex()` therefore
// compared state.Commit against itself and could never fire, in exactly the
// scenario the clamp was written for.
//
// The in-memory boundary must come only from evidence the log actually holds
// something: recovered entries or a recovered snapshot.
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

// TestTotalEntryLoss_NoPhantomReplicateAck is the end-to-end consequence of
// the clamp above: a replica that lost its entries must not ACK indices it
// does not hold.
//
// handleAppend answers any Replicate whose prevLogIndex is BELOW the
// committed index with a non-rejecting ReplicateResp carrying the committed
// index. With committed left at the recovered 100 over an empty log, a
// backtracking leader's probe draws `ReplicateResp{Reject:false,
// LogIndex:100}` — a claim to hold entries 1..100 that the leader counts
// toward the commit quorum. A majority containing this replica then commits
// entries that a majority does not actually hold: Leader Completeness fails.
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

// TG-3. The INNER clamp — processed down to the clamped committed — has its
// own reachable case, and TestNewRaftLog_ClampsCommittedToLastIndex does not
// reach it: that fixture has no snapshot, so processed is derived as
// firstIndex-1 == 0 and `processed > committed` compares 0 against 5. The
// assertion there is vacuously true and would survive deleting the clamp.
//
// The real case is a SNAPSHOT record that outlives the entries it was taken
// over: processed is seeded from the snapshot index, committed is raised to it
// (a snapshot index is committed by construction), and the outer clamp then
// pulls committed back down to the last entry the log actually holds. Without
// the inner clamp processed is left ABOVE committed, which is the exact state
// commitUpdate asserts against — the first Commit after restart panics and
// takes the commit worker, and with it every shard on that worker, down.
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
