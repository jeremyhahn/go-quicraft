package quicraft

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// inspectionSample is one reading of every read-only view of a replica's
// state that the docs describe (docs/raft-consensus/api.md "inspect
// consensus state", docs/multi-group-host/api.md diagnostics and
// ReadonlyLogReader).
type inspectionSample struct {
	logRangeLast  uint64
	getRangeLast  uint64
	nodeStateLast uint64
	nodeStateTerm uint64
	diag          RaftStateDiag
	leaderID      uint64
	leaderTerm    uint64
	leaderOK      bool
	readyLeaderID uint64
	readyTerm     uint64
	ready         bool
	appliedIndex  uint64
	hashSupported bool
}

func takeInspectionSample(t *testing.T, h *Host, shardID uint64) inspectionSample {
	t.Helper()
	var s inspectionSample

	lr, err := h.LogRange(shardID)
	if err != nil {
		t.Fatalf("LogRange: %v", err)
	}
	s.logRangeLast = lr.LastIndex

	reader, err := h.LogReader(shardID)
	if err != nil {
		t.Fatalf("LogReader: %v", err)
	}
	_, s.getRangeLast = reader.GetRange()
	s.nodeStateLast, s.nodeStateTerm = reader.NodeState()

	s.diag, err = h.DiagRaftState(shardID)
	if err != nil {
		t.Fatalf("DiagRaftState: %v", err)
	}

	s.leaderID, s.leaderTerm, s.leaderOK, err = h.LeaderIDAndTerm(shardID)
	if err != nil {
		t.Fatalf("LeaderIDAndTerm: %v", err)
	}

	s.readyLeaderID, s.readyTerm, s.ready, err = h.ShardReadReady(shardID)
	if err != nil {
		t.Fatalf("ShardReadReady: %v", err)
	}

	_, s.appliedIndex, s.hashSupported, err = h.StateMachineHash(shardID)
	if err != nil {
		t.Fatalf("StateMachineHash: %v", err)
	}
	return s
}

// waitInspectionQuiescent waits until two samples taken apart agree with
// each other on every field: the applied index is stable and the
// diagnostics snapshot (published by the step worker each tick) has caught
// up with it. It returns the second sample. It does not require the views
// to agree with one another; the caller asserts that.
func waitInspectionQuiescent(t *testing.T, h *Host, shardID uint64) inspectionSample {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	prev := takeInspectionSample(t, h, shardID)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		cur := takeInspectionSample(t, h, shardID)
		if cur == prev {
			return cur
		}
		prev = cur
	}
	t.Fatalf("shard %d did not become quiescent: last sample %+v", shardID, prev)
	return inspectionSample{}
}

// assertInspectionAgree checks that one quiescent sample of every view
// reports the same replica state.
func assertInspectionAgree(t *testing.T, label string, s inspectionSample) {
	t.Helper()
	var errs []string
	fail := func(format string, args ...interface{}) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	// (a) last index: LogRange, LogReader.GetRange, LogReader.NodeState and
	// DiagRaftState read the same log.
	if s.logRangeLast != s.getRangeLast || s.logRangeLast != s.nodeStateLast ||
		s.logRangeLast != s.diag.LastIndex {
		fail("(a) last index disagrees: LogRange=%d GetRange=%d NodeState=%d DiagRaftState=%d",
			s.logRangeLast, s.getRangeLast, s.nodeStateLast, s.diag.LastIndex)
	}

	// (b) commit and apply: on a quiescent single voter every entry in the
	// log is committed and applied, and DiagRaftState.Processed is the
	// applied index StateMachineHash reports.
	if !s.hashSupported {
		fail("(b) StateMachineHash reported ok=false for a sm.Hasher state machine")
	}
	if s.diag.Processed != s.appliedIndex {
		fail("(b) DiagRaftState.Processed=%d != StateMachineHash applied index=%d",
			s.diag.Processed, s.appliedIndex)
	}
	if s.diag.Committed != s.logRangeLast {
		fail("(b) DiagRaftState.Committed=%d != last index=%d on a quiescent single voter",
			s.diag.Committed, s.logRangeLast)
	}
	if s.appliedIndex != s.diag.Committed {
		fail("(b) applied index=%d != DiagRaftState.Committed=%d on a quiescent shard",
			s.appliedIndex, s.diag.Committed)
	}

	// (c) term and leadership.
	if s.diag.Term == 0 {
		fail("(c) DiagRaftState.Term is 0 on an elected shard")
	}
	if s.diag.Term != s.leaderTerm || s.diag.Term != s.readyTerm || s.diag.Term != s.nodeStateTerm {
		fail("(c) term disagrees: DiagRaftState=%d LeaderIDAndTerm=%d ShardReadReady=%d NodeState=%d",
			s.diag.Term, s.leaderTerm, s.readyTerm, s.nodeStateTerm)
	}
	if !s.leaderOK || s.leaderID != 1 {
		fail("(c) LeaderIDAndTerm leader=%d ok=%v, want 1 true", s.leaderID, s.leaderOK)
	}
	if !s.ready || s.readyLeaderID != 1 {
		fail("(c) ShardReadReady leader=%d ready=%v, want 1 true", s.readyLeaderID, s.ready)
	}
	if !strings.HasPrefix(s.diag.State, "leader") {
		fail("(c) DiagRaftState.State=%q, want a leader(...) role", s.diag.State)
	}

	if len(errs) > 0 {
		t.Errorf("%s: the inspection views disagree on a quiescent shard:\n  %s\nsample: %+v",
			label, strings.Join(errs, "\n  "), s)
	}
}

// TestHost_InspectionAgree checks that LogRange, LogReader (GetRange and
// NodeState), DiagRaftState, LeaderIDAndTerm, ShardReadReady and
// StateMachineHash report the same state of one quiescent replica, and that
// they all move by exactly the number of entries proposed.
func TestHost_InspectionAgree(t *testing.T) {
	const shardID = 1
	const proposals = 25

	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, hashingCreateFunc, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	// Not waitForLeader: it rewrites the host's cached leader with term 0 and
	// clears readReady, which would make LeaderIDAndTerm and ShardReadReady
	// disagree for a reason in the test helper, not the host.
	deadline := time.Now().Add(10 * time.Second)
	for {
		leader, _, ready, err := h.ShardReadReady(shardID)
		if err == nil && ready && leader == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard %d never became read-ready: leader=%d err=%v", shardID, leader, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitForDiagLive(t, h, shardID)

	before := waitInspectionQuiescent(t, h, shardID)
	assertInspectionAgree(t, "before", before)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < proposals; i++ {
		cmd := []byte(fmt.Sprintf("set:k%d=%d", i, i))
		if _, err := h.SyncPropose(ctx, shardID, cmd); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}

	after := waitInspectionQuiescent(t, h, shardID)
	assertInspectionAgree(t, "after", after)

	// (d) every index moved by exactly the number of proposals and the term
	// held: a single voter with no elections appends one entry per proposal.
	if got := after.diag.LastIndex - before.diag.LastIndex; got != proposals {
		t.Errorf("(d) LastIndex grew by %d (%d -> %d), want %d",
			got, before.diag.LastIndex, after.diag.LastIndex, proposals)
	}
	if got := after.diag.Committed - before.diag.Committed; got != proposals {
		t.Errorf("(d) Committed grew by %d (%d -> %d), want %d",
			got, before.diag.Committed, after.diag.Committed, proposals)
	}
	if got := after.appliedIndex - before.appliedIndex; got != proposals {
		t.Errorf("(d) applied index grew by %d (%d -> %d), want %d",
			got, before.appliedIndex, after.appliedIndex, proposals)
	}
	if after.diag.Term != before.diag.Term {
		t.Errorf("(d) term changed from %d to %d with no election",
			before.diag.Term, after.diag.Term)
	}
}
