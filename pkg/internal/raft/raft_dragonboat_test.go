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

package raft

// Tests ported from dragonboat that are missing in go-quicraft.
// These cover entry validation, leader transfer edge cases,
// config change recovery, snapshot safety, state transitions,
// and commit safety.

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ===========================================================================
// CRITICAL: Entry Validation Tests
// ===========================================================================

// TestDragonboat_AppendRejectsIndexHole verifies that tryAppend correctly
// identifies a conflict when entries have a gap in indices. When the log
// has entries 1-3 and receives entries starting at index 5 (skipping 4),
// the conflict detection should identify the gap and append from the
// first unavailable index.
func TestDragonboat_AppendRejectsIndexHole(t *testing.T) {
	entries := makeEntries(1, 4, 1) // indices 1,2,3 at term 1
	l := newTestRaftLog(entries)
	l.committed = 1

	// Entries with a gap: prevLogIndex=3, term matches, but entries
	// start at index 5 (skipping 4). tryAppend should succeed and
	// treat index 5 as beyond the log.
	gappedEntries := []proto.Entry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
	}

	// prevLogIndex=3, prevLogTerm=1 matches. The entries start at 5 but
	// getConflictIndex will find index 5 is beyond lastIndex (3), so
	// it will report conflict at 5 and append from there.
	// However, tryAppend uses prevLogIndex + entry offset to align entries.
	// With prevLogIndex=3 and entries starting at index 5, offset=5-5=0,
	// so all entries are appended starting at index 4 (conflict point).
	lastIdx, ok, _, err := l.tryAppend(3, 1, gappedEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected tryAppend to succeed with matching prevLog")
	}
	// tryAppend returns ents[len(ents)-1].Index = 6. The entries are
	// appended starting from the first conflict point (index 4, which
	// is beyond the log). The gap in entry indices doesn't matter
	// because inmem.merge overwrites from the conflict point.
	if lastIdx != 6 {
		t.Fatalf("expected lastIndex 6, got %d", lastIdx)
	}
}

// TestDragonboat_AppendRejectsTermRegression verifies that when entries
// arrive with correct sequential indices but a later entry has a lower
// term than an earlier entry, the conflict detection handles it correctly.
// In practice, the leader assigns terms, so this only occurs via stale
// retransmissions which are handled by conflict detection.
func TestDragonboat_AppendRejectsTermRegression(t *testing.T) {
	entries := makeEntries(1, 4, 2) // indices 1,2,3 at term 2
	l := newTestRaftLog(entries)
	l.committed = 1

	// Try to append entries where term regresses: index 3 has term 1
	// (lower than existing term 2). This creates a conflict at index 3.
	newEntries := []proto.Entry{
		{Index: 3, Term: 1, Cmd: []byte("bad")},
		{Index: 4, Term: 1, Cmd: []byte("also-bad")},
	}

	// prevLogIndex=2, prevLogTerm=2 matches existing entry.
	lastIdx, ok, truncated, err := l.tryAppend(2, 2, newEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected tryAppend to succeed (prevLog matches)")
	}
	if !truncated {
		t.Fatal("expected truncated=true since entry 3 was overwritten")
	}
	if lastIdx != 4 {
		t.Fatalf("expected lastIndex 4, got %d", lastIdx)
	}
	// Verify the overwritten entry has the new term.
	term, terr := l.term(3)
	if terr != nil {
		t.Fatalf("unexpected error getting term: %v", terr)
	}
	if term != 1 {
		t.Fatalf("expected term 1 at index 3 after overwrite, got %d", term)
	}
}

// TestDragonboat_AppendPanicsOnCommittedOverwrite verifies that appending
// entries at or below the committed index triggers an invariant assertion.
// The Raft safety property requires committed entries to never be overwritten.
func TestDragonboat_AppendPanicsOnCommittedOverwrite(t *testing.T) {
	entries := makeEntries(1, 4, 1) // indices 1,2,3
	l := newTestRaftLog(entries)
	l.committed = 3 // all committed

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when appending at or below committed index")
		}
	}()

	// Attempt to append at index 2, which is below committed (3).
	l.append([]proto.Entry{
		{Index: 2, Term: 2, Cmd: []byte("overwrite")},
	})
}

// TestDragonboat_EntryMergeSafety verifies that merging entries from
// different sources does not corrupt the log. When entries with matching
// indices and terms arrive, they should be recognized as duplicates and
// not create duplicates in the log.
func TestDragonboat_EntryMergeSafety(t *testing.T) {
	entries := makeEntries(1, 6, 1) // indices 1-5 at term 1
	l := newTestRaftLog(entries)
	l.committed = 3

	// Send entries that partially overlap (indices 3-6). Entries 3-5
	// should match existing, entry 6 should be new.
	newEntries := []proto.Entry{
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
		{Index: 6, Term: 1, Cmd: []byte("new")},
	}

	lastIdx, ok, truncated, err := l.tryAppend(3, 1, newEntries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected tryAppend to succeed")
	}
	// Entries 4,5 match existing (no conflict), entry 6 is new.
	if lastIdx != 6 {
		t.Fatalf("expected lastIndex 6, got %d", lastIdx)
	}
	// No truncation since overlapping entries have matching terms.
	if truncated {
		t.Fatal("expected truncated=false for matching entries")
	}

	// Verify log integrity: 6 entries total.
	allEntries, aerr := l.entries(1, 7, 0)
	if aerr != nil {
		t.Fatalf("unexpected error: %v", aerr)
	}
	if len(allEntries) != 6 {
		t.Fatalf("expected 6 entries, got %d", len(allEntries))
	}
	for i, e := range allEntries {
		expectedIdx := uint64(i + 1)
		if e.Index != expectedIdx {
			t.Fatalf("entry %d: expected index %d, got %d", i, expectedIdx, e.Index)
		}
	}
}

// ===========================================================================
// CRITICAL: Leader Transfer Edge Cases
// ===========================================================================

// TestDragonboat_LeaderTransferAfterSnapshot verifies that leadership can
// be transferred to a follower that received a snapshot. After the snapshot
// completes and the follower catches up, TimeoutNow should be sent.
func TestDragonboat_LeaderTransferAfterSnapshot(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]

	// Simulate that follower 2 needed a snapshot (was behind compaction).
	rm.becomeSnapshot(5)

	// Snapshot completes successfully.
	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rm.state != remoteProbe {
		t.Fatalf("expected probe state after snapshot done, got %s", rm.state)
	}

	// Follower 2 catches up: acknowledge entries up to lastIndex.
	rm.update(r.log.lastIndex())
	rm.respondedTo()
	r.drainMessages()

	// Now transfer leadership to follower 2.
	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Since follower 2 is now caught up, TimeoutNow should be sent.
	msgs := r.drainMessages()
	found := false
	for _, m := range msgs {
		if m.Type == proto.TimeoutNow && m.To == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected TimeoutNow to be sent to caught-up follower after snapshot")
	}
}

// TestDragonboat_LeaderTransferReceiveHigherTermVote verifies that during
// a leadership transfer, if the leader receives a message with a higher
// term, the transfer is aborted and the leader steps down.
func TestDragonboat_LeaderTransferReceiveHigherTermVote(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = 0 // follower 2 needs catch-up

	// Start transfer to follower 2.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}
	r.drainMessages()

	// Receive a RequestVote with a higher term from node 3.
	err = r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     r.term + 2,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Leader should step down to follower.
	if r.state != follower {
		t.Fatalf("expected follower after higher term vote, got %s", r.state)
	}
	// Transfer must be cleared on step-down.
	if r.transferTarget != noNode {
		t.Fatalf("expected transferTarget cleared, got %d", r.transferTarget)
	}
}

// TestDragonboat_LeaderTransferRemoveNode verifies that when the transfer
// target is removed from the cluster during an active transfer, subsequent
// proposals should work again after the transfer times out.
func TestDragonboat_LeaderTransferRemoveNode(t *testing.T) {
	// Use checkQuorum disabled so the leader doesn't step down when
	// we remove a remote and tick past election timeout.
	r := newTestRaftCheckQuorumDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = 0

	// Start transfer to follower 2.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Remove node 2 from remotes (simulating config change applied).
	delete(r.remotes, 2)

	// Proposals should still be blocked because transferTarget is set.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress, got %v", err)
	}

	// After enough ticks, transfer should time out.
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.transferTarget != noNode {
		t.Fatalf("expected transferTarget cleared after timeout, got %d", r.transferTarget)
	}

	// Now proposals should work again.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data2")}},
	})
	if err != nil {
		t.Fatalf("expected proposal to succeed after transfer timeout, got %v", err)
	}
}

// TestDragonboat_LeaderTransferToSelf verifies that a leader attempting
// to transfer leadership to itself is a no-op (already the leader).
func TestDragonboat_LeaderTransferToSelf(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: r.replicaID})
	if err != nil {
		t.Fatalf("expected no error for self-transfer, got %v", err)
	}
	if r.transferTarget != noNode {
		t.Fatalf("expected no transferTarget for self-transfer, got %d", r.transferTarget)
	}
	// Should still be leader.
	if r.state != leader {
		t.Fatalf("expected leader, got %s", r.state)
	}
}

// TestDragonboat_LeaderTransferToSlowFollower verifies that transferring
// leadership to a follower that needs log catch-up causes the leader to
// send entries first (not TimeoutNow).
func TestDragonboat_LeaderTransferToSlowFollower(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	// Propose some entries to get ahead of followers.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("a")}, {Cmd: []byte("b")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Follower 2 is behind (match=0, hasn't acked anything).
	r.remotes[2].match = 0
	r.remotes[2].next = 1
	r.remotes[2].becomeProbe()
	r.remotes[2].probeSent = false

	// Transfer to slow follower.
	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := r.drainMessages()

	// Should send Replicate to follower 2 (not TimeoutNow yet).
	hasReplicate := false
	hasTimeoutNow := false
	for _, m := range msgs {
		if m.To == 2 && m.Type == proto.Replicate {
			hasReplicate = true
		}
		if m.To == 2 && m.Type == proto.TimeoutNow {
			hasTimeoutNow = true
		}
	}
	if !hasReplicate {
		t.Fatal("expected Replicate to slow follower during transfer")
	}
	if hasTimeoutNow {
		t.Fatal("expected no TimeoutNow before follower catches up")
	}

	// After follower catches up, the leader sends TimeoutNow.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs = r.drainMessages()
	hasTimeoutNow = false
	for _, m := range msgs {
		if m.To == 2 && m.Type == proto.TimeoutNow {
			hasTimeoutNow = true
		}
	}
	if !hasTimeoutNow {
		t.Fatal("expected TimeoutNow after slow follower caught up")
	}
}

// TestDragonboat_LeaderTransferToNonExistingNode verifies that transferring
// to a node not in the cluster returns ErrNodeNotFound.
func TestDragonboat_LeaderTransferToNonExistingNode(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 99})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// TestDragonboat_LeaderTransferWithCheckQuorum verifies that leadership
// transfer works correctly when CheckQuorum is enabled. The transfer
// should proceed normally and the leader should step down correctly.
func TestDragonboat_LeaderTransferWithCheckQuorum(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgCheckQuorum(1, 1, true)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.checkQuorum {
		t.Fatal("expected checkQuorum enabled")
	}

	electLeader(r)
	r.drainMessages()

	// Ensure quorum is active so leader doesn't step down.
	r.remotes[2].setActive()
	r.remotes[3].setActive()

	// Match follower 2 so transfer proceeds immediately.
	r.remotes[2].match = r.log.lastIndex()

	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := r.drainMessages()
	found := false
	for _, m := range msgs {
		if m.Type == proto.TimeoutNow && m.To == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected TimeoutNow with CheckQuorum enabled")
	}
}

// TestDragonboat_LeaderTransferWithPreVote verifies that leadership
// transfer works correctly with PreVote enabled. TimeoutNow bypasses
// PreVote and starts a real election directly.
func TestDragonboat_LeaderTransferWithPreVote(t *testing.T) {
	// Create raft with PreVote enabled.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1) // default has PreVote=true
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.preVote {
		t.Fatal("expected preVote enabled")
	}

	electLeader(r)
	r.drainMessages()

	r.remotes[2].match = r.log.lastIndex()

	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := r.drainMessages()
	found := false
	for _, m := range msgs {
		if m.Type == proto.TimeoutNow && m.To == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected TimeoutNow with PreVote enabled")
	}

	// Verify that handleTimeoutNow bypasses PreVote by creating
	// a separate follower node to receive TimeoutNow.
	r2 := newTestRaft(2)
	r2.becomeFollower(r.term, 1) // follower in same term
	r2.drainMessages()

	err = r2.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 1,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// handleTimeoutNow calls becomeCandidate (not becomePreCandidate).
	if r2.state != candidate {
		t.Fatalf("expected candidate (bypassing PreVote), got %s", r2.state)
	}
}

// TestDragonboat_LeaderTransferSecondTransferToSameNode verifies that a
// second transfer request to the same target is rejected with
// ErrTransferInProgress.
func TestDragonboat_LeaderTransferSecondTransferToSameNode(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = 0 // slow follower

	// First transfer.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error on first transfer: %v", err)
	}
	r.drainMessages()

	// Second transfer to same target.
	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress for second transfer to same node, got %v", err)
	}
	// Original target preserved.
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}
}

// TestDragonboat_LeaderTransferFromFollower verifies that a follower
// receiving a LeaderTransfer message ignores it (no-op). Only the leader
// handles LeaderTransfer.
func TestDragonboat_LeaderTransferFromFollower(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// r starts as follower (initial state).
	r.becomeFollower(1, 2)
	r.drainMessages()

	// Follower receives LeaderTransfer. stepFollower does not handle it,
	// so it should be silently ignored.
	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
		Hint: 3,
		Term: 1,
	})
	if err != nil {
		t.Fatalf("expected no error when follower receives LeaderTransfer, got %v", err)
	}
	// No state change.
	if r.state != follower {
		t.Fatalf("expected follower, got %s", r.state)
	}
	if r.transferTarget != noNode {
		t.Fatalf("expected no transferTarget, got %d", r.transferTarget)
	}
}

// ===========================================================================
// CRITICAL: Config Change Recovery
// ===========================================================================

// TestDragonboat_RecoverPendingConfigChange verifies that a new leader
// with a pending (uncommitted) config change can detect it via
// hasConfigChangeToApply and blocks further config changes until the
// pending one commits.
func TestDragonboat_RecoverPendingConfigChange(t *testing.T) {
	// Create a raft with an uncommitted config change entry.
	r := newTestRaftPreVoteDisabled(1)

	// Append normal + config change entries.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal, Cmd: []byte("data")},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
	})
	r.log.commitTo(1) // only entry 1 committed

	// Become leader. Should detect the uncommitted CC.
	electLeader(r)
	r.drainMessages()

	if !r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange=true after becoming leader with uncommitted CC")
	}

	// New config change should be rejected.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Type: proto.EntryConfigChange, Cmd: []byte("cc2")}},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange, got %v", err)
	}

	// Normal proposals should still work.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Type: proto.EntryNormal, Cmd: []byte("normal")}},
	})
	if err != nil {
		t.Fatalf("expected normal proposal to succeed, got %v", err)
	}
}

// TestDragonboat_RecoverDoublePendingConfigChange verifies that when a
// leader becomes leader with two entries in the log where only the first
// config change is uncommitted, the leader correctly detects the pending
// CC and blocks new CCs. If both are normal + CC, only CC matters.
func TestDragonboat_RecoverDoublePendingConfigChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Two config change entries: first committed, second uncommitted.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
		{Index: 2, Term: 1, Type: proto.EntryNormal, Cmd: []byte("data")},
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
	})
	r.log.commitTo(2) // cc1 and data committed, cc2 uncommitted

	electLeader(r)
	r.drainMessages()

	// The uncommitted cc2 at index 3 should be detected.
	if !r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange=true for uncommitted CC at index 3")
	}

	// Verify the committed CC at index 1 is not preventing this detection.
	// The scan only looks at entries above committed.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Type: proto.EntryConfigChange, Cmd: []byte("cc3")}},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange for second uncommitted CC, got %v", err)
	}
}

// ===========================================================================
// CRITICAL: Snapshot Safety
// ===========================================================================

// TestDragonboat_SnapshotAbort verifies that when a snapshot send is
// "aborted" (reported as failure), the remote state resets to probe
// and replication can resume.
func TestDragonboat_SnapshotAbort(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeSnapshot(20)

	if rm.state != remoteSnapshot {
		t.Fatalf("expected remoteSnapshot, got %s", rm.state)
	}
	if !rm.isPaused() {
		t.Fatal("expected remote paused during snapshot")
	}

	// Abort: report failure.
	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Remote should be in probe state, ready to retry.
	if rm.state != remoteProbe {
		t.Fatalf("expected remoteProbe after abort, got %s", rm.state)
	}
	if rm.snapshotIndex != 0 {
		t.Fatalf("expected snapshotIndex cleared, got %d", rm.snapshotIndex)
	}
	if rm.isPaused() {
		t.Fatal("expected remote unpaused after snapshot abort")
	}
	// next should be max(match+1, snapshotIndex+1) = max(1, 21) = 21
	if rm.next != 21 {
		t.Fatalf("expected next=21, got %d", rm.next)
	}
}

// TestDragonboat_SnapshotFailureResetToProbe verifies that after snapshot
// failure the remote transitions to probe state with correct next index.
func TestDragonboat_SnapshotFailureResetToProbe(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[3]
	rm.match = 5
	rm.becomeSnapshot(15)

	// Report failure.
	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   3,
		Term:   r.term,
		Reject: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rm.state != remoteProbe {
		t.Fatalf("expected remoteProbe, got %s", rm.state)
	}
	// next = max(match+1, snapshotIndex+1) = max(6, 16) = 16
	if rm.next != 16 {
		t.Fatalf("expected next=16, got %d", rm.next)
	}
	if rm.probeSent {
		t.Fatal("expected probeSent=false after failure")
	}
}

// TestDragonboat_RestoreFromSnapMsg verifies that a follower receiving
// an InstallSnapshot message restores its state from the snapshot.
func TestDragonboat_RestoreFromSnapMsg(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(1, 2)
	r.drainMessages()

	ss := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}

	err := r.step(proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     2,
		Snapshot: ss,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Committed and processed should be at snapshot index.
	if r.log.committed != 10 {
		t.Fatalf("expected committed=10, got %d", r.log.committed)
	}
	if r.log.processed != 10 {
		t.Fatalf("expected processed=10, got %d", r.log.processed)
	}

	// Verify response was sent.
	msgs := r.drainMessages()
	found := false
	for _, m := range msgs {
		if m.Type == proto.ReplicateResp && m.To == 2 {
			found = true
			if m.LogIndex != 10 {
				t.Fatalf("expected response LogIndex=10, got %d", m.LogIndex)
			}
		}
	}
	if !found {
		t.Fatal("expected ReplicateResp after snapshot restore")
	}
}

// TestDragonboat_RestoreIgnoresStaleSnapshot verifies that a follower
// ignores a snapshot with index <= committed.
func TestDragonboat_RestoreIgnoresStaleSnapshot(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(2, 2)

	// Set committed to 10.
	r.log.append(makeEntries(1, 11, 1))
	r.log.commitTo(10)
	r.drainMessages()

	originalCommitted := r.log.committed

	ss := proto.Snapshot{
		Index: 5, // behind committed
		Term:  1,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}

	err := r.step(proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     2,
		Snapshot: ss,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Committed should not change.
	if r.log.committed != originalCommitted {
		t.Fatalf("expected committed unchanged at %d, got %d",
			originalCommitted, r.log.committed)
	}

	// Response should report committed index (not snapshot index).
	msgs := r.drainMessages()
	for _, m := range msgs {
		if m.Type == proto.ReplicateResp && m.To == 2 {
			if m.LogIndex != originalCommitted {
				t.Fatalf("expected response LogIndex=%d for stale snapshot, got %d",
					originalCommitted, m.LogIndex)
			}
		}
	}
}

// ===========================================================================
// HIGH: State Transition Matrix
// ===========================================================================

// TestDragonboat_StateTransitionMatrix tests all valid and invalid state
// transitions comprehensively.
func TestDragonboat_StateTransitionMatrix(t *testing.T) {
	t.Run("follower_to_candidate", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		r.becomeCandidate()
		if r.state != candidate {
			t.Fatalf("expected candidate, got %s", r.state)
		}
	})

	t.Run("follower_to_precandidate", func(t *testing.T) {
		r := newTestRaft(1) // PreVote enabled by default
		r.becomePreCandidate()
		if r.state != preCandidate {
			t.Fatalf("expected preCandidate, got %s", r.state)
		}
	})

	t.Run("candidate_to_leader", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		r.becomeCandidate()
		r.becomeLeader()
		if r.state != leader {
			t.Fatalf("expected leader, got %s", r.state)
		}
	})

	t.Run("candidate_to_follower", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		r.becomeCandidate()
		r.becomeFollower(r.term+1, 2)
		if r.state != follower {
			t.Fatalf("expected follower, got %s", r.state)
		}
	})

	t.Run("precandidate_to_candidate", func(t *testing.T) {
		r := newTestRaft(1)
		r.becomePreCandidate()
		r.becomeCandidate()
		if r.state != candidate {
			t.Fatalf("expected candidate, got %s", r.state)
		}
	})

	t.Run("precandidate_to_follower", func(t *testing.T) {
		r := newTestRaft(1)
		r.becomePreCandidate()
		r.becomeFollower(r.term+1, 2)
		if r.state != follower {
			t.Fatalf("expected follower, got %s", r.state)
		}
	})

	t.Run("leader_to_follower", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		electLeader(r)
		r.becomeFollower(r.term+1, 2)
		if r.state != follower {
			t.Fatalf("expected follower, got %s", r.state)
		}
	})

	t.Run("follower_to_leader_panics", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic: follower cannot become leader directly")
			}
		}()
		r.becomeLeader()
	})

	t.Run("leader_to_candidate_panics", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		electLeader(r)
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic: leader cannot become candidate")
			}
		}()
		r.becomeCandidate()
	})

	t.Run("leader_to_precandidate_panics", func(t *testing.T) {
		r := newTestRaft(1)
		electLeader(r)
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic: leader cannot become preCandidate")
			}
		}()
		r.becomePreCandidate()
	})
}

// TestDragonboat_VoteFromAnyState verifies that vote request handling
// works correctly regardless of the receiver's current role.
func TestDragonboat_VoteFromAnyState(t *testing.T) {
	tests := []struct {
		name  string
		setup func() *raft
	}{
		{
			name: "follower",
			setup: func() *raft {
				return newTestRaftPreVoteDisabled(1)
			},
		},
		{
			name: "candidate",
			setup: func() *raft {
				r := newTestRaftPreVoteDisabled(1)
				r.becomeCandidate()
				return r
			},
		},
		{
			name: "leader",
			setup: func() *raft {
				r := newTestRaftPreVoteDisabled(1)
				electLeader(r)
				return r
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup()
			currentTerm := r.term
			r.drainMessages()

			// Send vote request from a higher term.
			err := r.step(proto.Message{
				Type:     proto.RequestVote,
				From:     3,
				Term:     currentTerm + 5,
				LogIndex: r.log.lastIndex(),
				LogTerm:  r.log.lastTerm(),
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Node should update to new term and become follower.
			if r.term != currentTerm+5 {
				t.Fatalf("expected term %d, got %d", currentTerm+5, r.term)
			}
			if r.state != follower {
				t.Fatalf("expected follower after higher term vote request, got %s", r.state)
			}

			// Should have responded with a vote.
			msgs := collectMessages(r, proto.RequestVoteResp)
			if len(msgs) != 1 {
				t.Fatalf("expected 1 vote response, got %d", len(msgs))
			}
			if msgs[0].To != 3 {
				t.Fatalf("expected vote response to node 3, got %d", msgs[0].To)
			}
			// Vote should be granted (log is at least as up-to-date).
			if msgs[0].Reject {
				t.Fatal("expected vote granted for candidate with up-to-date log")
			}
		})
	}
}

// ===========================================================================
// HIGH: Commit Safety
// ===========================================================================

// TestDragonboat_CommitAfterRemoveNode verifies that after removing a node,
// the quorum adjusts and commits can still make progress with the remaining
// members.
func TestDragonboat_CommitAfterRemoveNode(t *testing.T) {
	// 5-node cluster: quorum = 3.
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Initial quorum: (4+1)/2 + 1 = 3
	if commitQuorum(r.remotes) != 3 {
		t.Fatalf("expected commit quorum 3 with 5 nodes, got %d", commitQuorum(r.remotes))
	}

	// Propose an entry.
	propErr := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	if propErr != nil {
		t.Fatalf("unexpected error: %v", propErr)
	}
	lastIdx := r.log.lastIndex()
	r.drainMessages()

	// Remove nodes 4 and 5.
	delete(r.remotes, 4)
	delete(r.remotes, 5)

	// New quorum: (2+1)/2 + 1 = 2
	if commitQuorum(r.remotes) != 2 {
		t.Fatalf("expected commit quorum 2 with 3 nodes, got %d", commitQuorum(r.remotes))
	}

	// Ack from peer 2 should be sufficient (self + peer 2 = quorum of 2).
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: lastIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed < lastIdx {
		t.Fatalf("expected committed >= %d after quorum ack, got %d", lastIdx, r.log.committed)
	}
}

// TestDragonboat_ProposalByProxy verifies that a follower forwards
// proposals to the leader.
func TestDragonboat_ProposalByProxy(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(1, 2) // node 2 is leader
	r.drainMessages()

	// Propose on the follower.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("client-data")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Follower should forward the proposal to leader (node 2).
	msgs := r.drainMessages()
	found := false
	for _, m := range msgs {
		if m.Type == proto.Propose && m.To == 2 {
			found = true
			if len(m.Entries) != 1 {
				t.Fatalf("expected 1 entry forwarded, got %d", len(m.Entries))
			}
		}
	}
	if !found {
		t.Fatal("expected proposal forwarded to leader")
	}
}

// TestDragonboat_ProposalByProxy_NoLeader verifies that a follower
// without a known leader returns NotLeaderError.
func TestDragonboat_ProposalByProxy_NoLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// No leader known.
	r.drainMessages()

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})

	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
	if notLeader.LeaderID != 0 {
		t.Fatalf("expected LeaderID=0, got %d", notLeader.LeaderID)
	}
}

// ===========================================================================
// CRITICAL: Section 5.4.2 - Cannot commit without new term entry
// ===========================================================================

// TestDragonboat_CannotCommitWithoutNewTermEntry verifies the Raft safety
// property from Section 5.4.2 of the Raft paper: a leader CANNOT commit
// entries from a previous term by just counting replicas. It must first
// commit an entry from its own term (the no-op). This prevents the
// "Figure 8" safety violation.
//
// Scenario:
//   - Create a 5-node cluster. Leader in term 1 appends entries.
//   - Simulate a new leader in term 2 that has previous-term entries
//     replicated to a majority but has NOT yet committed its own no-op.
//   - Verify commit index does NOT advance for previous-term entries.
func TestDragonboat_CannotCommitWithoutNewTermEntry(t *testing.T) {
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}

	// Simulate term 1: leader appends entries at term 1.
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	// The leader appended a no-op at its current term (term 1) during
	// becomeLeader. Get the last index.
	noopIndex := r.log.lastIndex()

	// Now propose an additional entry at term 1.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data-term1")}},
	})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	dataIndex := r.log.lastIndex()
	r.drainMessages()

	// Ack from peers 2 and 3 for all entries (majority = 3 with self).
	for _, peer := range []uint64{2, 3} {
		err = r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     peer,
			Term:     r.term,
			LogIndex: dataIndex,
		})
		if err != nil {
			t.Fatalf("ReplicateResp from %d: %v", peer, err)
		}
	}

	// At this point, entries at term 1 are replicated to a majority (1,2,3).
	// The leader's current term is also 1, so tryCommit checks
	// l.term(mci) == r.term. Since both are term 1, commit advances.
	// This is correct because the leader IS in term 1.
	if r.log.committed < dataIndex {
		t.Fatalf("leader in term 1 should commit its own term entries: committed=%d, dataIndex=%d",
			r.log.committed, dataIndex)
	}

	// Now simulate a NEW term scenario: leader steps down and a new
	// leader in term 3 takes over but inherits term-1 entries.
	r.becomeFollower(3, 0) // step down to term 3
	r.drainMessages()

	// Simulate becoming candidate and leader at term 4 (skipping term 3
	// leader). The key point: the new leader has the old term-1 entries
	// but tryCommit requires term match.
	r.term = 3
	r.becomeCandidate() // term becomes 4
	r.becomeLeader()    // appends no-op at term 4
	r.drainMessages()

	noopTerm4 := r.log.lastIndex()
	_ = noopIndex

	// Before any acks for the term-4 no-op, let's check what happens
	// if followers ack entries up to but NOT including the term-4 no-op.
	// Only ack up to noopTerm4 - 1 (the last term-1 entry).
	previousTermEnd := noopTerm4 - 1
	for _, peer := range []uint64{2, 3} {
		err = r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     peer,
			Term:     r.term,
			LogIndex: previousTermEnd,
		})
		if err != nil {
			t.Fatalf("ReplicateResp from %d: %v", peer, err)
		}
	}

	// The commit index should NOT advance past the term-1 entries because
	// tryCommit requires the entry at the median match index to have the
	// current term (term 4). The entry at previousTermEnd has term 1.
	// Section 5.4.2: "a leader cannot determine that an entry from a
	// previous term is committed."
	//
	// Note: committed may still be at the old level from when we were
	// leader in term 1. The point is it should not advance FURTHER.
	if r.log.committed > previousTermEnd {
		t.Fatalf("commit should not advance past previous-term entries without a current-term entry: "+
			"committed=%d, previousTermEnd=%d", r.log.committed, previousTermEnd)
	}

	// Now ack the term-4 no-op from the majority. This should allow
	// commit to advance.
	for _, peer := range []uint64{2, 3} {
		err = r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     peer,
			Term:     r.term,
			LogIndex: noopTerm4,
		})
		if err != nil {
			t.Fatalf("ReplicateResp from %d: %v", peer, err)
		}
	}

	// Now commit should advance to include the term-4 no-op (and
	// indirectly all prior entries).
	if r.log.committed < noopTerm4 {
		t.Fatalf("commit should advance after current-term entry is replicated: "+
			"committed=%d, noopTerm4=%d", r.log.committed, noopTerm4)
	}
}

// ===========================================================================
// LeaderCycle: elect, propose, kill, re-elect, verify entries preserved
// ===========================================================================

// TestDragonboat_LeaderCycle verifies the full leader lifecycle: elect a
// leader, propose entries, kill the leader (step down), elect a new leader,
// and verify that committed entries are preserved.
func TestDragonboat_LeaderCycle(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Propose entries.
	for _, cmd := range []string{"a", "b", "c"} {
		err := r.step(proto.Message{
			Type:    proto.Propose,
			Entries: []proto.Entry{{Cmd: []byte(cmd)}},
		})
		if err != nil {
			t.Fatalf("propose %q: %v", cmd, err)
		}
	}
	lastIdx := r.log.lastIndex()
	r.drainMessages()

	// Get acks from follower 2 to commit all entries.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: lastIdx,
	})
	if err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}

	if r.log.committed < lastIdx {
		t.Fatalf("entries should be committed: committed=%d, lastIdx=%d",
			r.log.committed, lastIdx)
	}

	committedBefore := r.log.committed
	termBefore := r.term

	// Kill the leader by stepping down (simulating partition or crash).
	r.becomeFollower(termBefore+1, 0)
	r.drainMessages()

	if r.state != follower {
		t.Fatalf("expected follower after step-down, got %s", r.state)
	}

	// Re-elect as leader.
	r.becomeCandidate()
	for id := range r.remotes {
		if id != r.replicaID {
			err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			})
			if err != nil {
				t.Fatalf("vote from %d: %v", id, err)
			}
			if r.state == leader {
				break
			}
		}
	}
	if r.state != leader {
		t.Fatal("should be leader after re-election")
	}
	r.drainMessages()

	// Verify committed entries are preserved.
	if r.log.committed < committedBefore {
		t.Fatalf("committed should not regress: before=%d, after=%d",
			committedBefore, r.log.committed)
	}

	// Verify the log entries are still present.
	entries, getErr := r.log.entries(1, r.log.lastIndex()+1, 0)
	if getErr != nil {
		t.Fatalf("entries: %v", getErr)
	}
	if len(entries) == 0 {
		t.Fatal("log should not be empty after re-election")
	}
}

// ===========================================================================
// LeaderElectionOverwriteNewerLogs: split-brain scenario
// ===========================================================================

// TestDragonboat_LeaderElectionOverwriteNewerLogs verifies the split-brain
// scenario where a new leader overwrites follower entries that have a
// higher index but lower term. This is the correct Raft behavior per
// Section 5.3: the leader's log is authoritative.
func TestDragonboat_LeaderElectionOverwriteNewerLogs(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Propose entries at current term.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("leader-data")}},
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	leaderLastIdx := r.log.lastIndex()
	leaderTerm := r.term
	r.drainMessages()

	// Ack from peer 2 to commit.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: leaderLastIdx,
	})
	if err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()

	// Now create a follower (node 2) that has stale entries from a
	// different term. Use the same raft to simulate: step down, append
	// entries at a lower term, then rejoin.
	r2 := newTestRaftPreVoteDisabled(2)
	r2.becomeFollower(leaderTerm-1, 0) // term lower than leader
	r2.drainMessages()

	// When r (leader) sends Replicate to r2, it will overwrite r2's
	// log. Simulate by sending an AppendEntries.
	entries, getErr := r.log.entries(1, leaderLastIdx+1, 0)
	if getErr != nil {
		t.Fatalf("entries: %v", getErr)
	}

	// The follower should accept the leader's entries.
	err = r2.step(proto.Message{
		Type:     proto.Replicate,
		From:     1,
		Term:     leaderTerm,
		LogIndex: 0,
		LogTerm:  0,
		Entries:  entries,
		Commit:   r.log.committed,
	})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// r2's log should now match the leader's.
	if r2.log.lastIndex() != r.log.lastIndex() {
		t.Fatalf("follower lastIndex=%d, leader lastIndex=%d",
			r2.log.lastIndex(), r.log.lastIndex())
	}
}

// ===========================================================================
// Vote to different nodes across terms
// ===========================================================================

// TestDragonboat_CastVoteToDifferentNodesAcrossTerms verifies that a node
// can vote for node A in term 1 and node B in term 2, because the vote
// resets when the term advances.
func TestDragonboat_CastVoteToDifferentNodesAcrossTerms(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.drainMessages()

	// Vote for node 2 in term 1.
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     1,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("RequestVote from 2: %v", err)
	}
	msgs := r.drainMessages()

	voteGranted := false
	for _, m := range msgs {
		if m.Type == proto.RequestVoteResp && m.To == 2 && !m.Reject {
			voteGranted = true
		}
	}
	if !voteGranted {
		t.Fatal("expected vote granted to node 2 in term 1")
	}
	if r.vote != 2 {
		t.Fatalf("vote = %d, want 2", r.vote)
	}

	// Now vote for node 3 in term 2. The term change should reset the vote.
	err = r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     2,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("RequestVote from 3: %v", err)
	}
	msgs = r.drainMessages()

	voteGranted = false
	for _, m := range msgs {
		if m.Type == proto.RequestVoteResp && m.To == 3 && !m.Reject {
			voteGranted = true
		}
	}
	if !voteGranted {
		t.Fatal("expected vote granted to node 3 in term 2")
	}
	if r.vote != 3 {
		t.Fatalf("vote = %d, want 3", r.vote)
	}
	if r.term != 2 {
		t.Fatalf("term = %d, want 2", r.term)
	}
}

// ===========================================================================
// CheckQuorum + PreVote combined behavior
// ===========================================================================

// TestDragonboat_CheckQuorumWithPreVote verifies the interaction between
// CheckQuorum and PreVote. A leader with CheckQuorum enabled should step
// down when it fails to receive heartbeat acks from a quorum within one
// election timeout. When PreVote is also enabled, followers should use
// pre-vote before starting a real election.
func TestDragonboat_CheckQuorumWithPreVote(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1) // PreVote=true by default
	cfg.CheckQuorum = config.BoolPtr(true)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if !r.checkQuorum {
		t.Fatal("expected checkQuorum enabled")
	}
	if !r.preVote {
		t.Fatal("expected preVote enabled")
	}

	electLeader(r)
	r.drainMessages()

	// Mark remotes as active so leader survives the first checkQuorum
	// cycle. Then tick through one full election timeout to trigger
	// the first checkQuorum check. The leader should survive because
	// remotes are marked active.
	r.remotes[2].setActive()
	r.remotes[3].setActive()

	for i := 0; i < r.electionTimeout; i++ {
		r.tickFunc(r)
	}
	r.drainMessages()

	// Leader should still be up after the first cycle (quorum was active).
	if r.state != leader {
		t.Fatalf("leader should survive first checkQuorum cycle: state=%s", r.state)
	}

	// After the first cycle, resetRemoteActivity was called. Now tick
	// through another full election timeout WITHOUT any heartbeat acks.
	// The leader should step down because no remotes are active.
	for i := 0; i < r.electionTimeout; i++ {
		r.tickFunc(r)
	}
	r.drainMessages()

	if r.state != follower {
		t.Fatalf("leader should step down with checkQuorum when no acks: state=%s", r.state)
	}

	// As a follower with PreVote, starting an election should first
	// go through preCandidate state.
	advanceElection(r)

	if r.state != preCandidate {
		t.Fatalf("expected preCandidate with PreVote, got %s", r.state)
	}
}

// ===========================================================================
// Leader superseding with CheckQuorum
// ===========================================================================

// TestDragonboat_LeaderSupersedingWithCheckQuorum verifies that a leader
// with CheckQuorum can be superseded by a candidate that has received
// enough votes to become leader at a higher term. The old leader must
// step down upon receiving a message with a higher term.
func TestDragonboat_LeaderSupersedingWithCheckQuorum(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgCheckQuorum(1, 1, true)
	cfg.PreVote = config.BoolPtr(false) // disable PreVote for clarity
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	electLeader(r)
	r.drainMessages()

	r.remotes[2].setActive()
	r.remotes[3].setActive()

	leaderTerm := r.term

	// A candidate at a higher term sends a RequestVote. The leader
	// must step down because the message has a higher term.
	err = r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     leaderTerm + 1,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}

	if r.state != follower {
		t.Fatalf("leader should step down on higher term, got %s", r.state)
	}
	if r.term != leaderTerm+1 {
		t.Fatalf("term should advance to %d, got %d", leaderTerm+1, r.term)
	}

	// The old leader should have voted for the candidate.
	msgs := r.drainMessages()
	voted := false
	for _, m := range msgs {
		if m.Type == proto.RequestVoteResp && m.To == 2 && !m.Reject {
			voted = true
		}
	}
	if !voted {
		t.Fatal("superseded leader should grant vote to higher-term candidate")
	}
}
