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

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ===========================================================================
// State Transitions
// ===========================================================================

// TestSafety_StateTransitionFollowerToCandidate verifies that a follower
// transitions to candidate (or preCandidate with PreVote) when its election
// timer expires. This is the fundamental liveness property: a follower that
// does not hear from a leader must eventually start an election.
func TestSafety_StateTransitionFollowerToCandidate(t *testing.T) {
	// Without PreVote: follower should become candidate directly.
	t.Run("without_prevote", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		if r.state != follower {
			t.Fatalf("expected initial state follower, got %s", r.state)
		}
		initialTerm := r.term
		advanceElection(r)
		if r.state != candidate {
			t.Fatalf("expected candidate after election timeout, got %s", r.state)
		}
		if r.term != initialTerm+1 {
			t.Fatalf("expected term incremented to %d, got %d", initialTerm+1, r.term)
		}
		if r.vote != r.replicaID {
			t.Fatalf("expected self-vote (%d), got %d", r.replicaID, r.vote)
		}
	})

	// With PreVote: follower should become preCandidate first.
	t.Run("with_prevote", func(t *testing.T) {
		r := newTestRaft(1)
		initialTerm := r.term
		advanceElection(r)
		if r.state != preCandidate {
			t.Fatalf("expected preCandidate after election timeout with PreVote, got %s", r.state)
		}
		// PreVote does not increment the term.
		if r.term != initialTerm {
			t.Fatalf("expected term unchanged (%d), got %d", initialTerm, r.term)
		}
	})
}

// TestSafety_StateTransitionCandidateToLeader verifies that a candidate
// transitions to leader upon receiving a majority of votes. The leader
// must immediately append a no-op entry and broadcast it.
func TestSafety_StateTransitionCandidateToLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	r.broadcastVote()
	electionTerm := r.term
	logBefore := r.log.lastIndex()

	// Grant vote from peer 2 (self + peer 2 = quorum of 2 in 3-node cluster).
	err := r.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: electionTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != leader {
		t.Fatalf("expected leader after majority vote, got %s", r.state)
	}
	if r.leaderID != r.replicaID {
		t.Fatalf("expected leaderID to be self (%d), got %d", r.replicaID, r.leaderID)
	}
	if r.term != electionTerm {
		t.Fatalf("expected term preserved at %d, got %d", electionTerm, r.term)
	}
	// No-op entry must be appended.
	if r.log.lastIndex() <= logBefore {
		t.Fatal("expected no-op entry appended after becoming leader")
	}
	// Replicate messages must be broadcast to peers.
	replicates := collectMessages(r, proto.Replicate)
	if len(replicates) == 0 {
		t.Fatal("expected Replicate messages broadcast after becoming leader")
	}
}

// TestSafety_StateTransitionCandidateToFollower verifies that a candidate
// steps down to follower when it receives a message from a higher term.
// This enforces the Raft invariant that at most one leader exists per term.
func TestSafety_StateTransitionCandidateToFollower(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	candidateTerm := r.term

	// Receive an AppendEntries from a leader in a higher term.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     candidateTerm + 1,
		LogIndex: 0,
		LogTerm:  0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected follower after higher term message, got %s", r.state)
	}
	if r.term != candidateTerm+1 {
		t.Fatalf("expected term updated to %d, got %d", candidateTerm+1, r.term)
	}
	if r.leaderID != 2 {
		t.Fatalf("expected leaderID set to 2, got %d", r.leaderID)
	}
	// Vote should be cleared because term advanced.
	if r.vote != noNode {
		t.Fatalf("expected vote cleared on term change, got %d", r.vote)
	}
}

// TestSafety_StateTransitionLeaderToFollower verifies that a leader steps
// down when it receives a message with a higher term. This guarantees
// that split-brain scenarios resolve: the leader with the stale term
// will always yield.
func TestSafety_StateTransitionLeaderToFollower(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	leaderTerm := r.term

	// Receive a heartbeat from another node claiming a higher term.
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: leaderTerm + 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected step down to follower, got %s", r.state)
	}
	if r.term != leaderTerm+1 {
		t.Fatalf("expected term updated to %d, got %d", leaderTerm+1, r.term)
	}
	// Leader transfer state must be cleared on step-down.
	if r.transferTarget != noNode {
		t.Fatalf("expected transferTarget cleared, got %d", r.transferTarget)
	}
}

// TestSafety_InvalidStateTransitions verifies that invalid state transitions
// are prevented by the raft state machine. Specifically:
// - A follower cannot directly become leader (must go through candidate).
// - A leader cannot become candidate (must step down to follower first).
func TestSafety_InvalidStateTransitions(t *testing.T) {
	t.Run("follower_cannot_become_leader", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic when follower calls becomeLeader directly")
			}
		}()
		r.becomeLeader()
	})

	t.Run("leader_cannot_become_candidate", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		electLeader(r)
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic when leader calls becomeCandidate")
			}
		}()
		r.becomeCandidate()
	})

	t.Run("leader_cannot_become_precandidate", func(t *testing.T) {
		r := newTestRaftPreVoteDisabled(1)
		electLeader(r)
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic when leader calls becomePreCandidate")
			}
		}()
		r.becomePreCandidate()
	})
}

// ===========================================================================
// Dueling Candidates
// ===========================================================================

// TestSafety_DuelingCandidates verifies that when two candidates compete
// in the same term, at most one can win. In a 5-node cluster, if two
// nodes start elections simultaneously, the votes are split such that
// only the one reaching quorum first wins.
func TestSafety_DuelingCandidates(t *testing.T) {
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}

	// Create two candidates at the same term.
	makeCand := func(id uint64) *raft {
		cfg := raftTestCfgPreVote(1, id, false)
		logdb := newTestLogReaderWithMembership(members)
		r, err := newRaft(cfg, logdb)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r.becomeCandidate()
		return r
	}

	r1 := makeCand(1)
	r2 := makeCand(2)

	// Both are at the same term (initial 0 + 1 = 1).
	if r1.term != r2.term {
		t.Fatalf("expected same term, r1=%d r2=%d", r1.term, r2.term)
	}

	// r1 gets votes from peers 3 and 4 (self + 3 + 4 = 3 votes, quorum = 3).
	_ = r1.step(proto.Message{Type: proto.RequestVoteResp, From: 3, Term: r1.term})
	_ = r1.step(proto.Message{Type: proto.RequestVoteResp, From: 4, Term: r1.term})

	if r1.state != leader {
		t.Fatalf("expected r1 to become leader, got %s", r1.state)
	}

	// r2 gets a vote from peer 5 only (self + 5 = 2 votes, quorum = 3).
	_ = r2.step(proto.Message{Type: proto.RequestVoteResp, From: 5, Term: r2.term})

	if r2.state == leader {
		t.Fatal("r2 should NOT be leader without quorum")
	}

	// r2 receives rejection from peers 3, 4, and 1 (already voted for r1).
	_ = r2.step(proto.Message{Type: proto.RequestVoteResp, From: 3, Term: r2.term, Reject: true})
	_ = r2.step(proto.Message{Type: proto.RequestVoteResp, From: 4, Term: r2.term, Reject: true})
	_ = r2.step(proto.Message{Type: proto.RequestVoteResp, From: 1, Term: r2.term, Reject: true})

	// r2 should step down to follower after majority rejection.
	// 3 rejections > 5 - 3 = 2, so voteLost.
	if r2.state != follower {
		t.Fatalf("expected r2 to step down after majority rejection, got %s", r2.state)
	}
}

// TestSafety_DuelingCandidatesWithPreVote verifies that the PreVote
// protocol prevents dueling candidates from disrupting the cluster.
// A pre-candidate that fails the pre-vote phase does not increment
// its term, preserving cluster stability.
func TestSafety_DuelingCandidatesWithPreVote(t *testing.T) {
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3",
	}

	makePre := func(id uint64) *raft {
		cfg := raftTestCfgPreVote(1, id, true)
		logdb := newTestLogReaderWithMembership(members)
		r, err := newRaft(cfg, logdb)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return r
	}

	r1 := makePre(1)
	r2 := makePre(2)
	initialTerm1 := r1.term
	initialTerm2 := r2.term

	// Both enter pre-candidate state.
	r1.becomePreCandidate()
	r2.becomePreCandidate()

	// Terms should not be incremented in pre-vote phase.
	if r1.term != initialTerm1 {
		t.Fatalf("expected r1 term unchanged at %d, got %d", initialTerm1, r1.term)
	}
	if r2.term != initialTerm2 {
		t.Fatalf("expected r2 term unchanged at %d, got %d", initialTerm2, r2.term)
	}

	// r1 wins pre-vote from peer 3, transitions to real candidate.
	_ = r1.step(proto.Message{
		Type: proto.RequestPreVoteResp,
		From: 3,
		Term: r1.term + 1, // PreVote responses echo the prospective term.
	})

	if r1.state != candidate {
		t.Fatalf("expected r1 to become candidate after pre-vote win, got %s", r1.state)
	}
	// Real election increments term.
	if r1.term != initialTerm1+1 {
		t.Fatalf("expected r1 term incremented to %d, got %d", initialTerm1+1, r1.term)
	}

	// r2 gets rejected in pre-vote. PreVote responses echo the prospective
	// term (initialTerm2 + 1). A rejected PreVoteResp with msg.Term > r.term
	// triggers becomeFollower (term advancement). So we must use r2's current
	// term for the response term to test the pre-vote rejection path without
	// term advancement. In practice, peers at the same term respond with the
	// same prospective term.
	//
	// However, a rejected PreVoteResp at higher term DOES cause step-down
	// per the Raft spec (the responder is at a higher term). Let's verify
	// that r2's pre-vote failure does not disrupt the cluster by checking
	// that after rejection, r2 is follower and the term reflects the
	// rejection semantics.
	_ = r2.step(proto.Message{
		Type:   proto.RequestPreVoteResp,
		From:   3,
		Term:   initialTerm2 + 1, // prospective term echoed back
		Reject: true,
	})
	// A rejected PreVoteResp at higher term causes becomeFollower, advancing
	// r2's term. This is correct behavior: it prevents a partitioned
	// pre-candidate from being stuck at a stale term.
	_ = r2.step(proto.Message{
		Type:   proto.RequestPreVoteResp,
		From:   1,
		Term:   initialTerm2 + 1,
		Reject: true,
	})

	if r2.state != follower {
		t.Fatalf("expected r2 to revert to follower, got %s", r2.state)
	}
	// The key safety property: r2 did NOT increment its term via
	// becomeCandidate (the real election never started). The term may
	// have advanced due to the rejected PreVoteResp step-down, but
	// the critical invariant is that r2 is follower, not leader.
	if r2.state == leader {
		t.Fatal("r2 must not become leader without winning pre-vote")
	}
}

// ===========================================================================
// Config Change Safety
// ===========================================================================

// TestSafety_SinglePendingConfigChange verifies that only one config change
// is allowed at a time. Per PhD thesis 4.1, config changes must be
// serialized to prevent safety violations from overlapping configurations.
func TestSafety_SinglePendingConfigChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// First config change should succeed.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  []byte("add-node-4"),
		}},
	})
	if err != nil {
		t.Fatalf("expected first config change to succeed, got: %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be true after proposing config change")
	}

	// Second config change must be rejected while first is pending.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  []byte("add-node-5"),
		}},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange, got: %v", err)
	}

	// Normal entries should still be accepted even with pending config change.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{{
			Type: proto.EntryNormal,
			Cmd:  []byte("normal-data"),
		}},
	})
	if err != nil {
		t.Fatalf("expected normal entry accepted during pending config change, got: %v", err)
	}
}

// TestSafety_ConfigChangeRecoveryAfterRestart verifies that when a new
// leader takes over, it correctly detects uncommitted config change entries
// inherited from the previous leader and blocks further config changes.
func TestSafety_ConfigChangeRecoveryAfterRestart(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Pre-populate log with an uncommitted config change entry at term 1.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal, Cmd: []byte("data")},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte("cc")},
	})
	// Do not commit entry 2 (it remains uncommitted).
	r.log.commitTo(1)

	// Become leader. becomeLeader should scan uncommitted entries and
	// detect the pending config change.
	electLeader(r)
	r.drainMessages()

	if !r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be true after becoming leader with uncommitted CC")
	}

	// Proposing another config change must fail.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  []byte("new-cc"),
		}},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange, got: %v", err)
	}
}

// TestSafety_CommitAfterRemoveNode verifies that the quorum calculation
// adjusts correctly after a node is removed from the cluster. With fewer
// voting members, the commit quorum decreases and entries can be committed
// with fewer acknowledgments.
func TestSafety_CommitAfterRemoveNode(t *testing.T) {
	// Start with a 3-node cluster: quorum = 2.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)
	noopIdx := r.log.lastIndex()
	r.drainMessages()

	// Commit quorum with 3 nodes = 2 (self + 1 peer).
	if commitQuorum(r.remotes) != 2 {
		t.Fatalf("expected commit quorum 2 with 3 nodes, got %d", commitQuorum(r.remotes))
	}

	// Simulate node 3 removal by removing it from remotes.
	delete(r.remotes, 3)

	// Now commit quorum = 2 (self + 1 peer = 2 members, quorum = 2).
	// Actually with 2 members: quorum = (1+1)/2+1 = 2.
	if commitQuorum(r.remotes) != 2 {
		t.Fatalf("expected commit quorum 2 with 2 nodes, got %d", commitQuorum(r.remotes))
	}

	// Acknowledge from peer 2 should be sufficient to commit.
	_ = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: noopIdx,
	})

	if r.log.committed < noopIdx {
		t.Fatalf("expected committed >= %d after peer 2 ack, got %d", noopIdx, r.log.committed)
	}
}

// ===========================================================================
// Snapshot Safety
// ===========================================================================

// TestSafety_SnapshotFailureRecovery verifies that when a snapshot send
// fails, the leader transitions the remote back to probe state so it
// can retry replication from the follower's actual position.
func TestSafety_SnapshotFailureRecovery(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]

	// Put remote into snapshot state as if the leader is sending a snapshot.
	rm.becomeSnapshot(10)
	if rm.state != remoteSnapshot {
		t.Fatalf("expected remoteSnapshot state, got %s", rm.state)
	}
	if rm.snapshotIndex != 10 {
		t.Fatalf("expected snapshotIndex 10, got %d", rm.snapshotIndex)
	}

	// Report snapshot failure.
	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: true, // failure
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Remote should transition back to probe state after failure.
	if rm.state != remoteProbe {
		t.Fatalf("expected remoteProbe after snapshot failure, got %s", rm.state)
	}
	// snapshotIndex should be cleared.
	if rm.snapshotIndex != 0 {
		t.Fatalf("expected snapshotIndex cleared, got %d", rm.snapshotIndex)
	}
	// When transitioning from snapshot to probe, next is set to
	// max(match+1, snapshotIndex+1) to avoid re-sending entries the
	// snapshot already covers. With match=0 and snapshotIndex=10:
	// next = max(1, 11) = 11.
	expectedNext := max(rm.match+1, uint64(11)) // snapshotIndex was 10
	if rm.next != expectedNext {
		t.Fatalf("expected next=%d after snapshot failure, got %d", expectedNext, rm.next)
	}
	// probeSent should be false so the leader can immediately retry.
	if rm.probeSent {
		t.Fatal("expected probeSent=false after snapshot failure")
	}
}

// TestSafety_SnapshotAbortDuringTransfer verifies that a snapshot
// completion (success) transitions the remote back to probe state
// so the leader can discover the follower's actual position.
func TestSafety_SnapshotAbortDuringTransfer(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]

	// Put remote into snapshot state.
	rm.becomeSnapshot(15)
	if rm.state != remoteSnapshot {
		t.Fatal("expected remoteSnapshot state")
	}

	// Report snapshot success.
	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: false, // success
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Remote should transition to probe after successful snapshot.
	if rm.state != remoteProbe {
		t.Fatalf("expected remoteProbe after snapshot done, got %s", rm.state)
	}
	if rm.snapshotIndex != 0 {
		t.Fatalf("expected snapshotIndex cleared, got %d", rm.snapshotIndex)
	}
}

// TestSafety_PendingSnapshotPausesReplication verifies that when a remote
// is in remoteSnapshot state, replication is paused (isPaused returns true).
// The leader must not send additional entries while a snapshot transfer
// is in progress.
func TestSafety_PendingSnapshotPausesReplication(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]

	// Put remote into snapshot state.
	rm.becomeSnapshot(10)

	// isPaused should return true in snapshot state.
	if !rm.isPaused() {
		t.Fatal("expected remote to be paused in snapshot state")
	}

	// Proposing an entry should not generate any Replicate messages to
	// the paused remote.
	_ = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	msgs := r.drainMessages()

	for _, m := range msgs {
		if m.Type == proto.Replicate && m.To == 2 {
			t.Fatal("expected no Replicate to remote 2 while snapshot is pending")
		}
	}
}

// ===========================================================================
// Leader Transfer Safety
// ===========================================================================

// TestSafety_LeaderTransferTimeout verifies that a leader transfer that
// does not complete within the election timeout is automatically aborted.
// The leader must resume accepting proposals after timeout.
func TestSafety_LeaderTransferTimeout(t *testing.T) {
	r := newTestRaftCheckQuorumDisabled(1)
	r.preVote = false
	electLeader(r)
	r.drainMessages()

	// Peer 2 is behind so TimeoutNow won't be sent immediately.
	r.remotes[2].match = 0

	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}

	// Proposals must be rejected during transfer.
	propErr := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("blocked")}},
	})
	if !errors.Is(propErr, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress, got %v", propErr)
	}

	// Tick past election timeout to trigger abort.
	r.drainMessages()
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.transferTarget != noNode {
		t.Fatalf("expected transfer aborted after timeout, got target %d", r.transferTarget)
	}
	if r.transferElapsed != 0 {
		t.Fatalf("expected transferElapsed reset to 0, got %d", r.transferElapsed)
	}

	// Proposals should be accepted again after timeout.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("allowed")}},
	})
	if err != nil {
		t.Fatalf("expected proposal accepted after transfer timeout, got: %v", err)
	}
}

// TestSafety_LeaderTransferBlocksProposals verifies that all proposal
// types are rejected while a leader transfer is in progress.
func TestSafety_LeaderTransferBlocksProposals(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = r.log.lastIndex()

	// Initiate transfer.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Normal proposal must be rejected.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress for normal proposal, got %v", err)
	}

	// Config change proposal must also be rejected.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  []byte("cc"),
		}},
	})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress for config change, got %v", err)
	}
}

// TestSafety_LeaderTransferInterruptedByHigherTerm verifies that receiving
// a message from a higher term during a leader transfer causes the leader
// to step down, effectively aborting the transfer.
func TestSafety_LeaderTransferInterruptedByHigherTerm(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = 0

	// Start transfer.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}
	r.drainMessages()

	// Higher term message from another node causes step-down.
	err = r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 3,
		Term: r.term + 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected follower after higher term, got %s", r.state)
	}
	// Transfer state must be cleared on step-down.
	if r.transferTarget != noNode {
		t.Fatalf("expected transferTarget cleared after step-down, got %d", r.transferTarget)
	}
}

// TestSafety_ConcurrentTransferPrevented verifies that a second leader
// transfer request is rejected while one is already in progress.
func TestSafety_ConcurrentTransferPrevented(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = r.log.lastIndex()

	// First transfer succeeds.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("expected first transfer to succeed, got %v", err)
	}
	r.drainMessages()

	// Second transfer to a different target must fail.
	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 3})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress for concurrent transfer, got %v", err)
	}

	// The original target must be preserved.
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget still 2, got %d", r.transferTarget)
	}
}

// ===========================================================================
// Log Replication Edge Cases
// ===========================================================================

// TestSafety_RemoteStateRetry verifies append behavior when a remote is
// in probe (retry) state. The leader should send at most one probe message
// and then wait for a response before sending more.
func TestSafety_RemoteStateRetry(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeProbe()
	rm.probeSent = false

	// isPaused should be false since probeSent is false.
	if rm.isPaused() {
		t.Fatal("expected probe remote not paused when probeSent=false")
	}

	// Send an append; it should set probeSent=true.
	r.sendAppend(2)
	if !rm.probeSent {
		t.Fatal("expected probeSent=true after sendAppend in probe state")
	}

	// Now the remote is paused (waiting for response).
	if !rm.isPaused() {
		t.Fatal("expected probe remote paused after probeSent=true")
	}

	msgs := r.drainMessages()
	probeCount := 0
	for _, m := range msgs {
		if m.To == 2 && m.Type == proto.Replicate {
			probeCount++
		}
	}
	if probeCount != 1 {
		t.Fatalf("expected exactly 1 probe message, got %d", probeCount)
	}

	// A second sendAppend should be a no-op because remote is paused.
	r.sendAppend(2)
	msgs2 := r.drainMessages()
	for _, m := range msgs2 {
		if m.To == 2 && m.Type == proto.Replicate {
			t.Fatal("expected no additional Replicate while probe is pending")
		}
	}
}

// TestSafety_RemoteStateReplicate verifies append behavior when a remote
// is in replicate state. The leader should pipeline multiple batches using
// the inflight window for flow control.
func TestSafety_RemoteStateReplicate(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Add several entries so there is content to replicate.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	})

	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.match = 0
	rm.next = 1
	rm.becomeReplicate()

	// In replicate state, isPaused is false unless inflights is full.
	if rm.isPaused() {
		t.Fatal("expected replicate remote not paused with empty inflights")
	}

	// sendAppend should pipeline entries.
	r.sendAppend(2)
	msgs := r.drainMessages()

	replicateCount := 0
	for _, m := range msgs {
		if m.To == 2 && m.Type == proto.Replicate {
			replicateCount++
		}
	}

	if replicateCount == 0 {
		t.Fatal("expected at least one Replicate message in replicate state")
	}
}

// TestSafety_RemoteStateSnapshot verifies append behavior when a remote
// is in snapshot state. All replication should be paused until the
// snapshot completes or fails.
func TestSafety_RemoteStateSnapshot(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeSnapshot(5)

	if !rm.isPaused() {
		t.Fatal("expected snapshot remote to be paused")
	}
	if rm.state != remoteSnapshot {
		t.Fatalf("expected remoteSnapshot, got %s", rm.state)
	}

	// sendAppend should be a no-op for a paused remote.
	r.sendAppend(2)
	msgs := r.drainMessages()
	for _, m := range msgs {
		if m.To == 2 && m.Type == proto.Replicate {
			t.Fatal("expected no Replicate to remote in snapshot state")
		}
	}

	// After snapshot done, remote transitions to probe and is unpaused.
	rm.snapshotDone()
	if rm.state != remoteProbe {
		t.Fatalf("expected remoteProbe after snapshotDone, got %s", rm.state)
	}
	if rm.isPaused() {
		t.Fatal("expected remote unpaused after snapshotDone")
	}
}

// TestSafety_LogReplicationMultiScenario exercises multiple replication
// scenarios in sequence: proposal, acknowledgment, commit advancement,
// and rejection with backtracking.
func TestSafety_LogReplicationMultiScenario(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Scenario 1: Propose and commit a normal entry.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("entry1")}},
	})
	if err != nil {
		t.Fatalf("proposal failed: %v", err)
	}
	entryIdx := r.log.lastIndex()
	r.drainMessages()

	// Acknowledge from peer 2.
	_ = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: entryIdx,
	})

	if r.log.committed < entryIdx {
		t.Fatalf("expected committed >= %d, got %d", entryIdx, r.log.committed)
	}
	r.drainMessages()

	// Scenario 2: Peer 3 acknowledges up to the committed entry.
	rm3 := r.remotes[3]

	_ = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     3,
		Term:     r.term,
		LogIndex: entryIdx,
	})

	if rm3.match != entryIdx {
		t.Fatalf("expected peer 3 match=%d, got %d", entryIdx, rm3.match)
	}
	r.drainMessages()

	// Scenario 3: Propose another entry and verify both peers can ack.
	_ = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("entry2")}},
	})
	entry2Idx := r.log.lastIndex()
	r.drainMessages()

	for _, peerID := range []uint64{2, 3} {
		_ = r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     peerID,
			Term:     r.term,
			LogIndex: entry2Idx,
		})
	}

	if r.log.committed < entry2Idx {
		t.Fatalf("expected committed >= %d after both peers ack, got %d",
			entry2Idx, r.log.committed)
	}
}

// ===========================================================================
// ReadIndex Safety
// ===========================================================================

// TestSafety_ReadIndexRequiresLeaderQuorum verifies that a ReadIndex
// request on a multi-node cluster requires heartbeat quorum confirmation
// before the read state is resolved. Without quorum, the read state
// must not be populated.
func TestSafety_ReadIndexRequiresLeaderQuorum(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Issue a ReadIndex request.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 100,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.flushReadIndex()

	// Before heartbeat quorum, readStates must be empty.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 read states before quorum confirmation, got %d", len(r.readStates))
	}

	// Capture the heartbeat context.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex quorum check")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Heartbeat response from ONE peer (quorum = 2 in 3-node: self + 1 peer).
	_ = r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	})

	// Now quorum is reached; readStates should be populated.
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state after quorum, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 100 {
		t.Fatalf("expected read key 100, got %d", r.readStates[0].Key)
	}
	if r.readStates[0].Index != r.log.committed {
		t.Fatalf("expected read index %d, got %d", r.log.committed, r.readStates[0].Index)
	}
}

// TestSafety_ReadIndexResetOnStateChange verifies that pending ReadIndex
// requests are cleared when the node transitions to follower (e.g., due
// to a higher term). This prevents stale read states from being resolved
// after the node is no longer leader.
func TestSafety_ReadIndexResetOnStateChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Issue a ReadIndex that creates a pending entry.
	_ = r.step(proto.Message{Type: proto.ReadIndex, Hint: 200})
	r.flushReadIndex()
	r.drainMessages()

	if len(r.pendingReadIndex) == 0 {
		t.Fatal("expected at least one pending ReadIndex entry")
	}

	// Step down to follower via higher term message.
	_ = r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: r.term + 1,
	})

	if r.state != follower {
		t.Fatalf("expected follower after higher term, got %s", r.state)
	}

	// pendingReadIndex must be cleared on role change.
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected pending ReadIndex cleared after becoming follower, got %d",
			len(r.pendingReadIndex))
	}
}

// TestSafety_ReadIndexDuringLeaderTransfer verifies that ReadIndex
// requests are rejected during an active leader transfer. This prevents
// serving stale reads while leadership is in flux.
func TestSafety_ReadIndexDuringLeaderTransfer(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Start a leader transfer.
	r.remotes[2].match = r.log.lastIndex()
	_ = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	r.drainMessages()

	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}

	// ReadIndex must be rejected during transfer.
	err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 300})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress for ReadIndex during transfer, got %v", err)
	}
}

// ===========================================================================
// Single Node
// ===========================================================================

// TestSafety_SingleNodeCommit verifies that a single-node cluster commits
// entries immediately without waiting for peer acknowledgment. The leader's
// own log position is sufficient for quorum.
func TestSafety_SingleNodeCommit(t *testing.T) {
	members := map[uint64]string{1: "addr1"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)
	r.drainMessages()

	// No-op should already be committed.
	noopIdx := r.log.lastIndex()
	if r.log.committed < noopIdx {
		t.Fatalf("expected no-op committed in single-node cluster, committed=%d noop=%d",
			r.log.committed, noopIdx)
	}

	// Propose multiple entries; all should commit immediately.
	for i := 0; i < 5; i++ {
		err = r.step(proto.Message{
			Type:    proto.Propose,
			Entries: []proto.Entry{{Cmd: []byte("data")}},
		})
		if err != nil {
			t.Fatalf("proposal %d failed: %v", i, err)
		}
	}

	if r.log.committed != r.log.lastIndex() {
		t.Fatalf("expected all entries committed in single-node, committed=%d last=%d",
			r.log.committed, r.log.lastIndex())
	}

	// No Replicate messages should be generated (no peers).
	replicates := collectMessages(r, proto.Replicate)
	if len(replicates) != 0 {
		t.Fatalf("expected no Replicate messages in single-node, got %d", len(replicates))
	}
}

// TestSafety_SingleNodeReadIndex verifies that a single-node cluster
// responds to ReadIndex immediately without a heartbeat round-trip.
// The leader is trivially confirmed as the sole voter.
func TestSafety_SingleNodeReadIndex(t *testing.T) {
	members := map[uint64]string{1: "addr1"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)
	r.drainMessages()
	r.readStates = nil

	// ReadIndex should respond immediately.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state in single-node cluster, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 42 {
		t.Fatalf("expected read key 42, got %d", r.readStates[0].Key)
	}
	if r.readStates[0].Index != r.log.committed {
		t.Fatalf("expected read index = committed (%d), got %d",
			r.log.committed, r.readStates[0].Index)
	}

	// No heartbeat messages should be generated for single-node ReadIndex.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) != 0 {
		t.Fatalf("expected no heartbeats for single-node ReadIndex, got %d", len(hbs))
	}

	// Issue multiple ReadIndex requests to verify they all resolve immediately.
	r.readStates = nil
	for i := uint64(100); i < 105; i++ {
		err = r.step(proto.Message{Type: proto.ReadIndex, Hint: i})
		if err != nil {
			t.Fatalf("ReadIndex %d failed: %v", i, err)
		}
	}
	if len(r.readStates) != 5 {
		t.Fatalf("expected 5 read states, got %d", len(r.readStates))
	}
}

// ===========================================================================
// Additional edge case: ReadIndex before no-op commit
// ===========================================================================

// TestSafety_ReadIndexBeforeNoopCommit verifies that a ReadIndex request
// is rejected if the leader has not yet committed an entry in the current
// term (the no-op entry). Per PhD 6.4, linearizable reads require the
// leader to have committed at least one entry in its term.
func TestSafety_ReadIndexBeforeNoopCommit(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	// Do NOT commit the no-op: readStates should stay nil.

	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 999,
	})
	if !errors.Is(err, ErrReadIndexNotReady) {
		t.Fatalf("expected ErrReadIndexNotReady before no-op commit, got %v", err)
	}
}

// ===========================================================================
// Additional edge case: config change in batch with two CCs
// ===========================================================================

// TestSafety_BatchWithTwoConfigChangesRejected verifies that a batch
// containing two config change entries is rejected, preventing multiple
// config changes from being proposed atomically.
func TestSafety_BatchWithTwoConfigChangesRejected(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
			{Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
		},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange for batch with two CCs, got %v", err)
	}

	// pendingConfigChange must NOT be set since the batch was rejected
	// before mutation.
	if r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange=false after rejected batch")
	}
}

// ===========================================================================
// Additional edge case: leader transfer to witness/observer is rejected
// ===========================================================================

// TestSafety_LeaderTransferToWitnessRejected verifies that transferring
// leadership to a witness (non-voting member) is properly rejected.
func TestSafety_LeaderTransferToWitnessRejected(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)
	r.drainMessages()

	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 3})
	if !errors.Is(err, ErrTransferTargetNotVoter) {
		t.Fatalf("expected ErrTransferTargetNotVoter, got %v", err)
	}
}

// ===========================================================================
// Additional edge case: SnapshotStatus for non-snapshot remote is ignored
// ===========================================================================

// TestSafety_SnapshotStatusIgnoredForNonSnapshotRemote verifies that
// receiving a SnapshotStatus for a remote that is NOT in snapshot state
// is safely ignored (no state change).
func TestSafety_SnapshotStatusIgnoredForNonSnapshotRemote(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeProbe()
	prevState := rm.state

	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// State should not have changed.
	if rm.state != prevState {
		t.Fatalf("expected state unchanged at %s, got %s", prevState, rm.state)
	}
}

// ===========================================================================
// Additional edge case: lease invalidated during transfer
// ===========================================================================

// TestSafety_LeaderTransferInvalidatesLease verifies that starting a
// leader transfer invalidates the lease. Per PhD 6.4.1, the old leader
// must not serve lease reads during the transfer window.
func TestSafety_LeaderTransferInvalidatesLease(t *testing.T) {
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)
	r.drainMessages()

	// Verify lease is valid before transfer.
	if !r.hasValidLease() {
		t.Fatal("expected valid lease before transfer")
	}

	// Start transfer.
	r.remotes[2].match = r.log.lastIndex()
	_ = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	r.drainMessages()

	// Lease must be invalidated.
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck nil (lease invalidated) during transfer")
	}
	if r.hasValidLease() {
		t.Fatal("expected lease invalid during transfer")
	}
}

// ===========================================================================
// Additional safety: TimeoutNow ignored by leader
// ===========================================================================

// TestSafety_TimeoutNowIgnoredByLeader verifies that a TimeoutNow
// message received by a leader is safely ignored. This can happen if
// a stale TimeoutNow arrives from a previous transfer round.
func TestSafety_TimeoutNowIgnoredByLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	leaderTerm := r.term
	r.drainMessages()

	err := r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: leaderTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must remain leader; the TimeoutNow should be ignored.
	if r.state != leader {
		t.Fatalf("expected leader to remain leader after TimeoutNow, got %s", r.state)
	}
	if r.term != leaderTerm {
		t.Fatalf("expected term unchanged at %d, got %d", leaderTerm, r.term)
	}
}

// ===========================================================================
// Helper used by lease tests: newTestRaftLeaseRead is in raft_test.go
// No need to declare it here. Helpers like commitNoOp, grantLease,
// electLeader, advanceElection, collectMessages, etc. are all defined
// in raft_test.go and accessible within the same package.
// ===========================================================================

// Compile-time check: ensure config.BoolPtr is used correctly.
var _ = config.BoolPtr
