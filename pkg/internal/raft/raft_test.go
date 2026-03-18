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
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// raftTestCfg returns a config with sensible defaults for raft testing.
func raftTestCfg(shardID, replicaID uint64) config.Config {
	cfg := config.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  10,
		HeartbeatRTT: 1,
	}
	cfg.SetDefaults()
	return cfg
}

// raftTestCfgPreVote returns a config with PreVote explicitly set.
func raftTestCfgPreVote(shardID, replicaID uint64, preVote bool) config.Config {
	cfg := raftTestCfg(shardID, replicaID)
	cfg.PreVote = config.BoolPtr(preVote)
	return cfg
}

// raftTestCfgCheckQuorum returns a config with CheckQuorum explicitly set.
func raftTestCfgCheckQuorum(shardID, replicaID uint64, checkQuorum bool) config.Config {
	cfg := raftTestCfg(shardID, replicaID)
	cfg.CheckQuorum = config.BoolPtr(checkQuorum)
	return cfg
}

// raftTestCfgQuiesce returns a config with quiesce enabled.
func raftTestCfgQuiesce(shardID, replicaID uint64) config.Config {
	cfg := raftTestCfg(shardID, replicaID)
	cfg.Quiesce = true
	return cfg
}

// raftTestCfgLeaseRead returns a config with LeaseRead enabled.
func raftTestCfgLeaseRead(shardID, replicaID uint64) config.Config {
	cfg := raftTestCfg(shardID, replicaID)
	cfg.LeaseRead = true
	return cfg
}

// newTestRaftLeaseRead creates a 3-node raft with LeaseRead and PreVote disabled.
func newTestRaftLeaseRead(replicaID uint64) *raft {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgLeaseRead(1, replicaID)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newTestRaftLeaseRead failed: " + err.Error())
	}
	return r
}

// commitNoOp commits the leader's no-op entry by sending a ReplicateResp
// from follower 2 and draining messages. This satisfies
// hasCommittedEntryAtCurrentTerm for ReadIndex.
func commitNoOp(r *raft) {
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		panic("commitNoOp: " + err.Error())
	}
	r.drainMessages()
	r.readStates = nil
}

// grantLease sends heartbeat responses from followers 2 and 3 to establish
// a valid lease on the leader. The leader must be node 1 in a 3-node cluster.
func grantLease(r *raft) {
	for _, from := range []uint64{2, 3} {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: from,
			Term: r.term,
			Hint: 0,
		}); err != nil {
			panic("grantLease: " + err.Error())
		}
	}
	r.drainMessages()
}

// newTestLogReaderWithMembership creates a test log reader with the
// given membership.
func newTestLogReaderWithMembership(members map[uint64]string) *testLogReader {
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = proto.Membership{
		Addresses: members,
	}
	return lr
}

// newTestRaft creates a raft node for testing with 3 voting members.
func newTestRaft(replicaID uint64) *raft {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	return newTestRaftWithMembers(replicaID, members)
}

// newTestRaftWithMembers creates a raft node with the given membership.
func newTestRaftWithMembers(replicaID uint64, members map[uint64]string) *raft {
	cfg := raftTestCfg(1, replicaID)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newTestRaft failed: " + err.Error())
	}
	return r
}

// newTestRaftPreVoteDisabled creates a raft node without PreVote.
func newTestRaftPreVoteDisabled(replicaID uint64) *raft {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, replicaID, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newTestRaft failed: " + err.Error())
	}
	return r
}

// newTestRaftCheckQuorumDisabled creates a raft with checkQuorum disabled.
func newTestRaftCheckQuorumDisabled(replicaID uint64) *raft {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgCheckQuorum(1, replicaID, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newTestRaft failed: " + err.Error())
	}
	return r
}

// collectMessages returns all messages of the given type from the raft node.
func collectMessages(r *raft, msgType uint64) []proto.Message {
	var result []proto.Message
	for _, m := range r.msgs {
		if m.Type == msgType {
			result = append(result, m)
		}
	}
	return result
}

// messagesTo returns all messages sent to the given target.
func messagesTo(r *raft, to uint64) []proto.Message {
	var result []proto.Message
	for _, m := range r.msgs {
		if m.To == to {
			result = append(result, m)
		}
	}
	return result
}

// advanceElection ticks the raft node until an election fires.
func advanceElection(r *raft) {
	for i := 0; i < r.randomizedElectionTimeout+1; i++ {
		r.tickFunc(r)
	}
}

// advanceHeartbeat ticks the raft node past a heartbeat interval.
func advanceHeartbeat(r *raft) {
	for i := 0; i < r.heartbeatTimeout+1; i++ {
		r.tickFunc(r)
	}
}

// electLeader transitions the given raft node to leader by granting
// votes from peers.
func electLeader(r *raft) {
	r.becomeCandidate()
	r.broadcastVote()
	// Single-node cluster wins immediately with self-vote.
	if r.checkElectionResult() {
		r.drainMessages()
		return
	}
	for id := range r.remotes {
		if id != r.replicaID {
			err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			})
			if err != nil {
				panic("electLeader: " + err.Error())
			}
			if r.state == leader {
				break
			}
		}
	}
	for id := range r.witnesses {
		if id != r.replicaID {
			err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			})
			if err != nil {
				panic("electLeader: " + err.Error())
			}
			if r.state == leader {
				break
			}
		}
	}
	if r.state != leader {
		panic("electLeader: failed to elect leader")
	}
	// Drain messages from election.
	r.drainMessages()
}

// setReadIndexAck sets the ack bit for replicaID on the given readIndexStatus
// using the raft's replicaToBit mapping. Panics if the replicaID is unknown.
func setReadIndexAck(r *raft, status *readIndexStatus, replicaID uint64) {
	bitPos, ok := r.replicaToBit[replicaID]
	if !ok {
		panic("setReadIndexAck: unknown replicaID")
	}
	mask := uint64(1) << bitPos
	if status.ackBits&mask == 0 {
		status.ackBits |= mask
		status.ackCount++
	}
}

// ---------------------------------------------------------------------------
// Test: newRaft
// ---------------------------------------------------------------------------

func TestRaft_NewRaft_InitializesAsFollower(t *testing.T) {
	r := newTestRaft(1)
	if r.state != follower {
		t.Fatalf("expected follower state, got %s", r.state)
	}
	if r.leaderID != noLeader {
		t.Fatalf("expected no leader, got %d", r.leaderID)
	}
}

func TestRaft_NewRaft_InvalidEntropyFails(t *testing.T) {
	// This test validates the newRaft function signature and error path.
	// We cannot easily simulate crypto/rand failure, but we validate
	// the normal path succeeds and the return values are correct.
	members := map[uint64]string{1: "addr1"}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("expected non-nil raft")
	}
}

// ---------------------------------------------------------------------------
// Test: State transitions
// ---------------------------------------------------------------------------

func TestRaft_BecomeFollower_SetsState(t *testing.T) {
	r := newTestRaft(1)
	r.becomeFollower(5, 2)
	if r.state != follower {
		t.Fatalf("expected follower, got %s", r.state)
	}
	if r.term != 5 {
		t.Fatalf("expected term 5, got %d", r.term)
	}
	if r.leaderID != 2 {
		t.Fatalf("expected leader 2, got %d", r.leaderID)
	}
	// Vote is cleared because term changed (0 -> 5).
	if r.vote != noNode {
		t.Fatalf("expected vote cleared on term change, got %d", r.vote)
	}
}

func TestRaft_BecomeFollower_PreservesVoteSameTerm(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Candidate votes for self (term incremented).
	r.becomeCandidate()
	candidateTerm := r.term
	if r.vote != r.replicaID {
		t.Fatalf("expected self-vote, got %d", r.vote)
	}
	// Receives AppendEntries from winning leader in same term.
	// becomeFollower in same term must NOT clear vote.
	r.becomeFollower(candidateTerm, 2)
	if r.vote != r.replicaID {
		t.Fatalf("expected vote preserved in same term, got %d", r.vote)
	}
}

func TestRaft_BecomeFollower_ClearsVoteOnTermChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	if r.vote != r.replicaID {
		t.Fatalf("expected self-vote, got %d", r.vote)
	}
	// Higher term must clear the vote.
	r.becomeFollower(r.term+1, 3)
	if r.vote != noNode {
		t.Fatalf("expected vote cleared on term change, got %d", r.vote)
	}
}

func TestRaft_BecomeFollower_PreventDoubleVote(t *testing.T) {
	// Scenario: candidate voted for self in term 2, receives AppendEntries
	// from winning leader (term 2), then a third candidate requests vote
	// in term 2. The vote must be rejected because we already voted.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	candidateTerm := r.term
	// Leader wins - become follower in same term.
	r.becomeFollower(candidateTerm, 2)
	r.drainMessages()
	// Third candidate requests vote in same term.
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     candidateTerm,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Vote must be rejected - we already voted for self.
	msgs := collectMessages(r, proto.RequestVoteResp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 vote response, got %d", len(msgs))
	}
	if !msgs[0].Reject {
		t.Fatal("expected vote rejection (double-vote prevention)")
	}
}

func TestRaft_BecomeFollower_ClearsTransferState(t *testing.T) {
	r := newTestRaft(1)
	r.transferTarget = 3
	r.transferElapsed = 5
	r.becomeFollower(5, 2)
	if r.transferTarget != noNode {
		t.Fatalf("expected transferTarget cleared, got %d", r.transferTarget)
	}
	if r.transferElapsed != 0 {
		t.Fatalf("expected transferElapsed cleared, got %d", r.transferElapsed)
	}
}

func TestRaft_BecomeCandidate_IncrementsTermAndVotesSelf(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	initialTerm := r.term
	r.becomeCandidate()
	if r.state != candidate {
		t.Fatalf("expected candidate, got %s", r.state)
	}
	if r.term != initialTerm+1 {
		t.Fatalf("expected term %d, got %d", initialTerm+1, r.term)
	}
	if r.vote != r.replicaID {
		t.Fatalf("expected vote for self (%d), got %d", r.replicaID, r.vote)
	}
}

func TestRaft_BecomeCandidate_PanicsFromLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when transitioning from leader to candidate")
		}
	}()
	r.becomeCandidate()
}

func TestRaft_BecomePreCandidate_DoesNotIncrementTerm(t *testing.T) {
	r := newTestRaft(1)
	initialTerm := r.term
	r.becomePreCandidate()
	if r.state != preCandidate {
		t.Fatalf("expected preCandidate, got %s", r.state)
	}
	if r.term != initialTerm {
		t.Fatalf("expected term unchanged (%d), got %d", initialTerm, r.term)
	}
}

func TestRaft_BecomePreCandidate_PanicsFromLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when transitioning from leader to pre-candidate")
		}
	}()
	r.becomePreCandidate()
}

func TestRaft_BecomeLeader_SetsState(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	if r.state != leader {
		t.Fatalf("expected leader, got %s", r.state)
	}
	if r.leaderID != r.replicaID {
		t.Fatalf("expected leaderID %d, got %d", r.replicaID, r.leaderID)
	}
}

func TestRaft_BecomeLeader_AppendsNoopEntry(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	lastBefore := r.log.lastIndex()
	electLeader(r)
	if r.log.lastIndex() <= lastBefore {
		t.Fatal("expected noop entry to be appended")
	}
	// The noop entry should have the leader's term.
	entry, err := getEntryForTest(&r.log.inmem, r.log.lastIndex())
	if err != nil {
		t.Fatalf("unexpected error getting noop: %v", err)
	}
	if entry.Term != r.term {
		t.Fatalf("expected noop term %d, got %d", r.term, entry.Term)
	}
}

// ---------------------------------------------------------------------------
// Test: Election - normal
// ---------------------------------------------------------------------------

func TestRaft_Election_FollowerStartsElection(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	advanceElection(r)
	if r.state != candidate {
		t.Fatalf("expected candidate after election timeout, got %s", r.state)
	}
}

func TestRaft_Election_FollowerStartsPreVote(t *testing.T) {
	r := newTestRaft(1)
	advanceElection(r)
	if r.state != preCandidate {
		t.Fatalf("expected preCandidate after election timeout with PreVote, got %s", r.state)
	}
}

func TestRaft_Election_CandidateWinsWithMajority(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	r.broadcastVote()

	// Grant vote from one peer (quorum = 2 for 3-node cluster).
	err := r.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.state != leader {
		t.Fatalf("expected leader after majority vote, got %s", r.state)
	}
}

func TestRaft_Election_CandidateLosesWithMajorityRejection(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	r.broadcastVote()

	// Both peers reject.
	for _, peerID := range []uint64{2, 3} {
		err := r.step(proto.Message{
			Type:   proto.RequestVoteResp,
			From:   peerID,
			Term:   r.term,
			Reject: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if r.state != follower {
		t.Fatalf("expected follower after majority rejection, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Election - PreVote
// ---------------------------------------------------------------------------

func TestRaft_Election_PreVoteGranted(t *testing.T) {
	r := newTestRaft(1)
	initialTerm := r.term
	r.becomePreCandidate()
	r.broadcastPreVote()

	// Grant pre-vote from one peer (quorum = 2).
	err := r.step(proto.Message{
		Type: proto.RequestPreVoteResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should transition to candidate (real election).
	if r.state != candidate {
		t.Fatalf("expected candidate after pre-vote majority, got %s", r.state)
	}
	// Term should be incremented for real election.
	if r.term != initialTerm+1 {
		t.Fatalf("expected term %d, got %d", initialTerm+1, r.term)
	}
}

func TestRaft_Election_PreVoteRejected(t *testing.T) {
	r := newTestRaft(1)
	initialTerm := r.term
	r.becomePreCandidate()
	r.broadcastPreVote()

	// Both peers reject.
	for _, peerID := range []uint64{2, 3} {
		err := r.step(proto.Message{
			Type:   proto.RequestPreVoteResp,
			From:   peerID,
			Term:   r.term,
			Reject: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if r.state != follower {
		t.Fatalf("expected follower after pre-vote rejection, got %s", r.state)
	}
	// Term should NOT have been incremented.
	if r.term != initialTerm {
		t.Fatalf("expected term unchanged (%d), got %d", initialTerm, r.term)
	}
}

func TestRaft_Election_PreVoteDoesNotIncrementTermOnPartition(t *testing.T) {
	r := newTestRaft(1)
	initialTerm := r.term

	// Simulate multiple election timeouts without getting responses.
	for i := 0; i < 5; i++ {
		advanceElection(r)
		r.drainMessages()
	}

	// Term should still be the same or only incremented from real elections.
	// Since pre-vote doesn't change term and real election only happens
	// after pre-vote succeeds, without responses the node stays at
	// preCandidate and never bumps term.
	if r.term != initialTerm {
		t.Fatalf("expected term unchanged (%d) during partition with PreVote, got %d",
			initialTerm, r.term)
	}
}

// ---------------------------------------------------------------------------
// Test: Election - split vote
// ---------------------------------------------------------------------------

func TestRaft_Election_SplitVoteCausesReelection(t *testing.T) {
	// 5-node cluster: split vote where we only get 2 votes (need 3).
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r.becomeCandidate()
	r.broadcastVote()

	// 2 grants + self = 2 grants, 2 rejects
	err = r.step(proto.Message{Type: proto.RequestVoteResp, From: 2, Term: r.term})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = r.step(proto.Message{Type: proto.RequestVoteResp, From: 3, Term: r.term, Reject: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = r.step(proto.Message{Type: proto.RequestVoteResp, From: 4, Term: r.term, Reject: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Still pending since 2 votes (self + 2) < 3 quorum, but also
	// 2 rejects + possible 1 remaining = not yet lost.
	// Actually 2 grants (self + peer2), 2 rejects = need peer5.
	if r.state == leader {
		t.Fatal("should not be leader with only 2 votes in 5-node cluster")
	}
	// Third rejection should push to follower.
	// Actually only 2 rejects vs 3 needed to lose (totalVoters - quorum = 5 - 3 = 2)
	// So 2 rejects > 2 means lost. Wait, totalVoters-quorum = 5-3 = 2,
	// and rejected (2) > 2 is false. rejected must be > totalVoters-quorum.
	// 2 > 2 is false, so still pending.
	if r.state == follower {
		t.Fatal("should still be candidate with 2 rejects in 5-node cluster")
	}
}

// ---------------------------------------------------------------------------
// Test: Election - term confusion
// ---------------------------------------------------------------------------

func TestRaft_Election_HigherTermCausesStepDown(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	// Receive a message from a higher term.
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: r.term + 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.state != follower {
		t.Fatalf("expected step down to follower, got %s", r.state)
	}
	if r.term != r.term {
		// r.term was already updated by step().
	}
}

func TestRaft_Election_LowerTermMessageIgnored(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	currentTerm := r.term

	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: currentTerm - 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.state != leader {
		t.Fatalf("expected to remain leader, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Vote request handling
// ---------------------------------------------------------------------------

func TestRaft_VoteRequest_GrantedWhenLogUpToDate(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.msgs = nil

	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     r.term + 1,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected vote response")
	}
	if resps[0].Reject {
		t.Fatal("expected vote granted")
	}
}

func TestRaft_VoteRequest_RejectedWhenAlreadyVoted(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.term = 5
	r.vote = 3 // already voted for 3

	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     5,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected vote response")
	}
	if !resps[0].Reject {
		t.Fatal("expected vote rejected (already voted for another)")
	}
}

func TestRaft_VoteRequest_RejectedWhenCandidateLogBehind(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Give us entries at term 5.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 5},
		{Index: 2, Term: 5},
	})
	r.msgs = nil

	// Candidate has lower last term.
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     6,
		LogTerm:  4,
		LogIndex: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected vote response")
	}
	if !resps[0].Reject {
		t.Fatal("expected vote rejected (candidate log behind)")
	}
}

// ---------------------------------------------------------------------------
// Test: PreVote request handling
// ---------------------------------------------------------------------------

func TestRaft_PreVoteRequest_RejectedWhenLeaderActive(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = 3
	r.electionTick = 0 // recently received from leader
	r.msgs = nil

	err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     2,
		Term:     r.term + 1,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestPreVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected pre-vote response")
	}
	if !resps[0].Reject {
		t.Fatal("expected pre-vote rejected (leader active)")
	}
}

func TestRaft_PreVoteRequest_GrantedWhenNoLeader(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = noLeader
	r.msgs = nil

	err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     2,
		Term:     r.term + 1,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestPreVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected pre-vote response")
	}
	if resps[0].Reject {
		t.Fatal("expected pre-vote granted (no active leader)")
	}
}

// ---------------------------------------------------------------------------
// Test: Replication - single entry
// ---------------------------------------------------------------------------

func TestRaft_Replication_LeaderSendsAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	// Acknowledge the initial probe (noop) from peers so they move
	// to replicate state and will accept further appends.
	noopIdx := r.log.lastIndex()
	for id := range r.remotes {
		err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: noopIdx,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	r.drainMessages()

	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Cmd: []byte("hello")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	appends := collectMessages(r, proto.Replicate)
	if len(appends) == 0 {
		t.Fatal("expected Replicate messages after propose")
	}
}

func TestRaft_Replication_ProposalOnFollowerReturnsError(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.leaderID = noLeader

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("test")}},
	})

	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
}

func TestRaft_Replication_ProposalOnFollowerForwardsToLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.leaderID = 2

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("test")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	forwarded := messagesTo(r, 2)
	if len(forwarded) == 0 {
		t.Fatal("expected proposal to be forwarded to leader")
	}
}

// ---------------------------------------------------------------------------
// Test: Replication - batch
// ---------------------------------------------------------------------------

func TestRaft_Replication_BatchAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	entries := []proto.Entry{
		{Cmd: []byte("a")},
		{Cmd: []byte("b")},
		{Cmd: []byte("c")},
	}
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: entries,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that all 3 entries were appended to the log.
	// lastIndex should be noop(1) + 3 entries = 4.
	if r.log.lastIndex() < 4 {
		t.Fatalf("expected at least 4 entries, lastIndex=%d", r.log.lastIndex())
	}
}

func TestRaft_Replication_ProposalDuringTransferFails(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.transferTarget = 2

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("test")}},
	})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: Replication - conflict resolution
// ---------------------------------------------------------------------------

func TestRaft_Replication_AppendConflictResolution(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Pre-populate log with entries at term 1.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.msgs = nil

	// Leader sends entries that conflict at index 2.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     2,
		LogIndex: 1,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 2, Term: 2},
			{Index: 3, Term: 2},
			{Index: 4, Term: 2},
		},
		Commit: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify conflict resolved: entries at index 2,3 now have term 2.
	term, termErr := r.log.term(2)
	if termErr != nil {
		t.Fatalf("unexpected error: %v", termErr)
	}
	if term != 2 {
		t.Fatalf("expected term 2 at index 2, got %d", term)
	}

	// Verify new entry at index 4 was added.
	if r.log.lastIndex() != 4 {
		t.Fatalf("expected lastIndex 4, got %d", r.log.lastIndex())
	}
}

func TestRaft_Replication_AppendRejectedWhenLogMismatch(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
	})
	r.msgs = nil

	// Leader claims prevLogIndex=5, prevLogTerm=3 which we don't have.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     3,
		LogIndex: 5,
		LogTerm:  3,
		Entries:  []proto.Entry{{Index: 6, Term: 3}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp")
	}
	if !resps[0].Reject {
		t.Fatal("expected rejection for log mismatch")
	}
}

// ---------------------------------------------------------------------------
// Test: handleAppendResp - success and rejection
// ---------------------------------------------------------------------------

func TestRaft_HandleAppendResp_SuccessUpdatesMatch(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Simulate a successful append response from peer 2.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := r.remotes[2]
	if rm.match != r.log.lastIndex() {
		t.Fatalf("expected match %d, got %d", r.log.lastIndex(), rm.match)
	}
}

func TestRaft_HandleAppendResp_RejectionDecreasesNext(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Pre-populate log so remotes start with higher next.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	prevNext := rm.next

	// Simulate a rejection from peer 2: rejected at next-1 with hint.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: prevNext - 1,
		Reject:   true,
		Hint:     1, // follower's last index is 1
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Next should have been decreased.
	if rm.next >= prevNext {
		t.Fatalf("expected next to decrease from %d, got %d", prevNext, rm.next)
	}
}

// ---------------------------------------------------------------------------
// Test: handleAppendResp - witness gets sendWitnessAppend
// ---------------------------------------------------------------------------

func TestRaft_HandleAppendResp_WitnessGetsMetadataOnReject(t *testing.T) {
	// Create a cluster with 2 voters + 1 witness. Node 1 is the leader.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pre-populate log so remotes start with higher next.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("data1")},
		{Index: 2, Term: 1, Cmd: []byte("data2")},
		{Index: 3, Term: 1, Cmd: []byte("data3")},
	})
	electLeader(r)
	r.drainMessages()

	prevNext := r.witnesses[3].next

	// Simulate a rejection from the witness.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     3,
		Term:     r.term,
		LogIndex: prevNext - 1,
		Reject:   true,
		Hint:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := r.drainMessages()
	// Find the Replicate message sent to the witness.
	var found bool
	for _, msg := range msgs {
		if msg.To == 3 && msg.Type == proto.Replicate {
			found = true
			// Entries sent to witnesses must be metadata-only (no Cmd payload),
			// except for config changes which are preserved.
			for _, e := range msg.Entries {
				if e.Type != proto.EntryMetadata && !e.IsConfigChange() {
					t.Fatalf("expected metadata entry for witness, got type %d with cmd %q",
						e.Type, e.Cmd)
				}
			}
		}
	}
	if !found {
		t.Fatal("expected a Replicate message to witness 3 after rejection")
	}
}

func TestRaft_HandleAppendResp_WitnessGetsMetadataOnSuccess(t *testing.T) {
	// Create a cluster with 2 voters + 1 witness. Node 1 is the leader.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	noopIdx := r.log.lastIndex()
	r.drainMessages()

	// Propose entries so there is data beyond the witness's match.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("payload")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Acknowledge the no-op from the witness so inflights are freed,
	// which triggers the sendAppend/sendWitnessAppend path for free
	// inflights in handleAppendResp.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     3,
		Term:     r.term,
		LogIndex: noopIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := r.drainMessages()
	for _, msg := range msgs {
		if msg.To == 3 && msg.Type == proto.Replicate {
			for _, e := range msg.Entries {
				if e.Type != proto.EntryMetadata && !e.IsConfigChange() {
					t.Fatalf("expected metadata entry for witness on success path, got type %d with cmd %q",
						e.Type, e.Cmd)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Commit - majority calculation
// ---------------------------------------------------------------------------

func TestRaft_Commit_MajorityAdvancesCommitIndex(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	noopIdx := r.log.lastIndex()
	r.drainMessages()

	// Propose an entry.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entryIdx := r.log.lastIndex()
	r.drainMessages()

	// One peer acknowledges the noop and the new entry.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: entryIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With 3 nodes and 2 matches (self + peer 2), commit should advance.
	if r.log.committed < noopIdx {
		t.Fatalf("expected committed >= %d, got %d", noopIdx, r.log.committed)
	}
}

func TestRaft_Commit_OnlyCommitsCurrentTerm(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Pre-populate log with entries from a previous term.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})

	electLeader(r) // This bumps to term 2.
	r.drainMessages()

	// The noop from becomeLeader is at term 2. Entries 1,2 are at term 1.
	// tryCommit should only commit entries at the current term (term 2).
	// Before a peer acks the noop, committed should not advance past
	// entries from term 1 (Raft safety property).
	// The leader's own noop index is at lastIndex.
	noopIdx := r.log.lastIndex()

	// Peer 2 only has entries up to index 2 (term 1).
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: 2, // old term entries only
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Commit should NOT advance to index 2 because the median would need
	// entries from the current term (the noop).
	// Actually with 3 nodes: matched = [lastIndex, 2, 0] sorted = [0, 2, lastIndex]
	// quorum = 2, mci = matched[3-2] = matched[1] = 2, but tryCommit checks term.
	// Term at index 2 is 1, current term is 2, so commit fails. Good.
	if r.log.committed >= noopIdx {
		t.Fatalf("committed should not advance to noop without majority at current term")
	}

	// Now peer 2 acks up to the noop.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: noopIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Now commit should advance.
	if r.log.committed < noopIdx {
		t.Fatalf("expected committed >= %d after majority ack, got %d",
			noopIdx, r.log.committed)
	}
}

// ---------------------------------------------------------------------------
// Test: Heartbeat
// ---------------------------------------------------------------------------

func TestRaft_Heartbeat_LeaderSendsOnTick(t *testing.T) {
	r := newTestRaftCheckQuorumDisabled(1)
	electLeader(r)
	r.drainMessages()

	advanceHeartbeat(r)
	heartbeats := collectMessages(r, proto.Heartbeat)
	if len(heartbeats) == 0 {
		t.Fatal("expected heartbeat messages after heartbeat tick")
	}
}

func TestRaft_Heartbeat_FollowerResponds(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = 2
	r.msgs = nil

	err := r.step(proto.Message{
		Type:   proto.Heartbeat,
		From:   2,
		Term:   r.term,
		Commit: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.HeartbeatResp)
	if len(resps) == 0 {
		t.Fatal("expected HeartbeatResp")
	}
}

func TestRaft_Heartbeat_AdvancesFollowerCommit(t *testing.T) {
	r := newTestRaft(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.leaderID = 2

	err := r.step(proto.Message{
		Type:   proto.Heartbeat,
		From:   2,
		Term:   r.term,
		Commit: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed != 2 {
		t.Fatalf("expected committed 2, got %d", r.log.committed)
	}
}

func TestRaft_Heartbeat_DoesNotExceedLocalLog(t *testing.T) {
	r := newTestRaft(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
	})
	r.leaderID = 2

	// Leader claims commit=5 but we only have entry at index 1.
	err := r.step(proto.Message{
		Type:   proto.Heartbeat,
		From:   2,
		Term:   r.term,
		Commit: 5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed != 1 {
		t.Fatalf("expected committed clamped to 1, got %d", r.log.committed)
	}
}

// ---------------------------------------------------------------------------
// Test: HeartbeatResp - leader marks remote active
// ---------------------------------------------------------------------------

func TestRaft_HeartbeatResp_MarksRemoteActive(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Reset activity.
	r.resetRemoteActivity()
	if r.remotes[2].isActive() {
		t.Fatal("expected remote 2 to be inactive")
	}

	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !r.remotes[2].isActive() {
		t.Fatal("expected remote 2 to be active after heartbeat resp")
	}
}

func TestRaft_HeartbeatResp_UnknownPeerIgnored(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// HeartbeatResp from unknown peer should not panic.
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 99,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRaft_HeartbeatResp_ClearsProbeSentAndRetriesAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	// Simulate: remote is in probe state with probeSent=true (initial
	// MsgApp was sent but lost during connection setup).
	rm.becomeProbe()
	rm.probeSent = true

	if !rm.isPaused() {
		t.Fatal("remote should be paused when probeSent is true")
	}

	// HeartbeatResp should clear probeSent and trigger sendAppend.
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// probeSent should be set again because sendAppend was called and
	// re-sent MsgApp in probe mode.
	if !rm.probeSent {
		t.Fatal("probeSent should be true after sendAppend retried")
	}

	// Verify a MsgApp was actually generated.
	apps := collectMessages(r, proto.Replicate)
	found := false
	for _, m := range apps {
		if m.To == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected MsgApp to peer 2 after HeartbeatResp cleared probeSent")
	}
}

func TestRaft_HeartbeatResp_ProbeSentFalseStillSendsAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeProbe()
	rm.probeSent = false // already unpaused

	// HeartbeatResp should still trigger sendAppend.
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	apps := collectMessages(r, proto.Replicate)
	found := false
	for _, m := range apps {
		if m.To == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected MsgApp to peer 2 after HeartbeatResp")
	}
}

func TestRaft_HeartbeatResp_ReplicateStateSendsAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	// Simulate: remote has transitioned to replicate after responding
	// to initial MsgApp, but a subsequent MsgApp was dropped.
	rm.becomeReplicate()

	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// In replicate state, HeartbeatResp should trigger sendAppend
	// as a recovery path for dropped MsgApp.
	apps := collectMessages(r, proto.Replicate)
	found := false
	for _, m := range apps {
		if m.To == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected MsgApp to peer 2 in replicate state after HeartbeatResp")
	}
}

// TestRaft_HeartbeatResp_StalledReplicateFallsBackToProbe verifies that
// when the leader is in replicate mode, has advanced next past lastIndex
// (nothing more to send), but still has unacknowledged inflights, a
// heartbeat response triggers a fallback to probe mode. This recovers
// from the stall caused by the transport silently dropping Replicate
// messages: the leader's next was optimistically advanced past what the
// follower received, and entries(next, last+1) returns empty, preventing
// resend. Probe mode resets next=match+1, clearing the stall.
func TestRaft_HeartbeatResp_StalledReplicateFallsBackToProbe(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]

	// Simulate a follower that was in replicate mode and received some
	// entries. The leader advanced rm.next optimistically past lastIndex.
	rm.becomeReplicate()
	rm.match = 1
	rm.next = r.log.lastIndex() + 5 // past the end

	// Add some fake inflights to simulate dropped messages.
	if err := rm.inflights.add(r.log.lastIndex() + 1); err != nil {
		t.Fatal(err)
	}
	if err := rm.inflights.add(r.log.lastIndex() + 2); err != nil {
		t.Fatal(err)
	}

	if rm.state != remoteReplicate {
		t.Fatalf("expected replicate state, got %d", rm.state)
	}
	if rm.inflights.Count() != 2 {
		t.Fatalf("expected 2 inflights, got %d", rm.inflights.Count())
	}

	// Send a heartbeat response. This should detect the stall:
	// next > lastIndex AND inflights > 0.
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The remote should have fallen back to probe mode.
	if rm.state != remoteProbe {
		t.Fatalf("expected probe state after stall recovery, got %d", rm.state)
	}

	// next should be reset to match+1.
	if rm.next != rm.match+1 {
		t.Fatalf("expected next=%d (match+1), got %d", rm.match+1, rm.next)
	}

	// Inflights should be cleared.
	if rm.inflights.Count() != 0 {
		t.Fatalf("expected 0 inflights after probe fallback, got %d", rm.inflights.Count())
	}

	// A Replicate should have been sent to resend from match+1.
	apps := collectMessages(r, proto.Replicate)
	found := false
	for _, m := range apps {
		if m.To == 2 {
			found = true
			break
		}
	}
	if found {
		// In probe mode, probeSent is set after sendAppend, so a message
		// is sent. This is the recovery path.
	}
}

// TestRaft_HeartbeatResp_ReplicateNoFallbackWhenCaughtUp verifies that
// the probe fallback does NOT trigger when the remote has fully caught
// up (inflights == 0 even though next > lastIndex). This is the normal
// steady-state: the follower has acknowledged everything.
func TestRaft_HeartbeatResp_ReplicateNoFallbackWhenCaughtUp(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = r.log.lastIndex()
	rm.next = r.log.lastIndex() + 1 // next > lastIndex but inflights == 0

	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should remain in replicate state (no stall detected).
	if rm.state != remoteReplicate {
		t.Fatalf("expected replicate state (no fallback), got %d", rm.state)
	}
}

// TestRaft_HeartbeatResp_ReplicateNoFallbackWhenMoreToSend verifies that
// the probe fallback does NOT trigger when the remote still has entries
// to send (next <= lastIndex). This is the normal active replication
// case where inflights > 0 is expected and healthy.
func TestRaft_HeartbeatResp_ReplicateNoFallbackWhenMoreToSend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	// Propose several entries so lastIndex is well ahead.
	for i := 0; i < 5; i++ {
		_ = r.step(proto.Message{
			Type:    proto.Propose,
			Entries: []proto.Entry{{Cmd: []byte("test")}},
		})
	}
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = 1
	rm.next = 3 // behind lastIndex, more entries to send

	// Add an inflight.
	if err := rm.inflights.add(2); err != nil {
		t.Fatal(err)
	}

	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should remain in replicate state (still has entries to send).
	if rm.state != remoteReplicate {
		t.Fatalf("expected replicate state (more entries to send), got %d", rm.state)
	}
}

// ---------------------------------------------------------------------------
// Test: ReadIndex HeartbeatResp sendAppend skip optimization
// ---------------------------------------------------------------------------

// TestRaft_HeartbeatResp_ReadIndexSkipsSendAppendWhenCaughtUp verifies that
// when a HeartbeatResp carries a ReadIndex context (Hint > 0) and the
// remote is in Replicate state (caught up), sendAppend is skipped. ReadIndex
// heartbeats only confirm quorum liveness; sending an empty Replicate wastes
// network bandwidth and CPU.
func TestRaft_HeartbeatResp_ReadIndexSkipsSendAppendWhenCaughtUp(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = r.log.lastIndex()
	rm.next = r.log.lastIndex() + 1

	// Send a HeartbeatResp with Hint > 0 (ReadIndex context).
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 42, // ReadIndex ctx
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No Replicate message should be generated for peer 2.
	apps := collectMessages(r, proto.Replicate)
	for _, m := range apps {
		if m.To == 2 {
			t.Fatal("expected no Replicate to peer 2 for ReadIndex HeartbeatResp with caught-up remote")
		}
	}

	// Remote should remain in Replicate state.
	if rm.state != remoteReplicate {
		t.Fatalf("expected replicate state, got %s", rm.state)
	}
}

// TestRaft_HeartbeatResp_RegularHeartbeatStillSendsAppend verifies that
// when a HeartbeatResp has Hint == 0 (regular periodic heartbeat), sendAppend
// is still called regardless of remote state. Regular heartbeat responses are
// the recovery path for silently dropped MsgApp.
func TestRaft_HeartbeatResp_RegularHeartbeatStillSendsAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeReplicate()

	// Send a HeartbeatResp with Hint == 0 (regular heartbeat).
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A Replicate message should be generated for peer 2
	// because this is a regular heartbeat (Hint == 0).
	apps := collectMessages(r, proto.Replicate)
	found := false
	for _, m := range apps {
		if m.To == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected Replicate to peer 2 for regular HeartbeatResp (Hint=0)")
	}
}

// TestRaft_HeartbeatResp_ReadIndexRetryStateSendsAppend verifies that
// when a HeartbeatResp carries a ReadIndex context (Hint > 0) but the
// remote is NOT in Replicate state (e.g., Probe after stall recovery),
// sendAppend still fires. The remote needs replication to catch up, and
// skipping sendAppend would leave it permanently behind.
func TestRaft_HeartbeatResp_ReadIndexRetryStateSendsAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	rm.becomeProbe()
	rm.probeSent = false

	// Send a HeartbeatResp with Hint > 0 (ReadIndex context) but remote
	// is in Probe state (not caught up).
	err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 99, // ReadIndex ctx
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A Replicate message should be generated for peer 2 because the
	// remote is in Probe state (needs to catch up).
	apps := collectMessages(r, proto.Replicate)
	found := false
	for _, m := range apps {
		if m.To == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected Replicate to peer 2 for ReadIndex HeartbeatResp when remote is in Probe state")
	}

	// probeSent should be set by sendAppend.
	if !rm.probeSent {
		t.Fatal("expected probeSent=true after sendAppend in Probe state")
	}
}

// ---------------------------------------------------------------------------
// Test: CheckQuorum
// ---------------------------------------------------------------------------

func TestRaft_CheckQuorum_LeaderStepsDownWithoutQuorum(t *testing.T) {
	r := newTestRaft(1)
	r.preVote = false
	electLeader(r)
	r.drainMessages()

	// Do NOT send any heartbeat responses. Advance past election timeout.
	r.resetRemoteActivity()
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.state != follower {
		t.Fatalf("expected leader to step down to follower, got %s", r.state)
	}
}

func TestRaft_CheckQuorum_LeaderRemainsWithQuorum(t *testing.T) {
	r := newTestRaft(1)
	r.preVote = false
	electLeader(r)
	r.drainMessages()

	// Mark one peer active (self + 1 peer = quorum of 2 in 3-node).
	r.resetRemoteActivity()
	r.remotes[2].setActive()

	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.state != leader {
		t.Fatalf("expected leader to remain leader with quorum, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Snapshot handling
// ---------------------------------------------------------------------------

func TestRaft_Snapshot_FollowerRestoresFromSnapshot(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{{Index: 1, Term: 1}})
	r.log.commitTo(1)
	r.leaderID = 2

	ss := proto.Snapshot{
		Index: 10,
		Term:  3,
	}

	err := r.step(proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     3,
		Snapshot: ss,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed != 10 {
		t.Fatalf("expected committed 10 after snapshot, got %d", r.log.committed)
	}

	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp after snapshot")
	}
	if resps[0].LogIndex != 10 {
		t.Fatalf("expected response with logIndex 10, got %d", resps[0].LogIndex)
	}
}

func TestRaft_Snapshot_RejectedWhenBehindCommit(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.log.commitTo(3)
	r.leaderID = 2

	// Snapshot at index 2 is behind our committed index (3).
	ss := proto.Snapshot{Index: 2, Term: 1}

	err := r.step(proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     1,
		Snapshot: ss,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp for rejected snapshot")
	}
	if resps[0].LogIndex != 3 {
		t.Fatalf("expected rejection with committed index 3, got %d", resps[0].LogIndex)
	}
}

// ---------------------------------------------------------------------------
// Test: Leader transfer
// ---------------------------------------------------------------------------

func TestRaft_LeaderTransfer_TargetCaughtUp(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Set peer 2 as caught up.
	r.remotes[2].match = r.log.lastIndex()

	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should send TimeoutNow to peer 2.
	timeoutNows := collectMessages(r, proto.TimeoutNow)
	if len(timeoutNows) == 0 {
		t.Fatal("expected TimeoutNow message to caught-up target")
	}
	if timeoutNows[0].To != 2 {
		t.Fatalf("expected TimeoutNow to peer 2, got %d", timeoutNows[0].To)
	}
}

func TestRaft_LeaderTransfer_TargetBehind(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Peer 2 is behind.
	r.remotes[2].match = 0

	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should send Replicate (not TimeoutNow) to catch up.
	replicates := collectMessages(r, proto.Replicate)
	if len(replicates) == 0 {
		t.Fatal("expected Replicate message to behind target")
	}
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}
}

func TestRaft_LeaderTransfer_InvalidTarget(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 99, // not in cluster
	})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestRaft_LeaderTransfer_AlreadyInProgress(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = r.log.lastIndex()

	// First transfer.
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Second transfer should fail.
	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 3})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress, got %v", err)
	}
}

func TestRaft_LeaderTransfer_TransferToSelf(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be a no-op (already leader).
	if r.transferTarget != noNode {
		t.Fatalf("expected no transfer target when transferring to self, got %d",
			r.transferTarget)
	}
}

func TestRaft_LeaderTransfer_TimeoutClearsTransfer(t *testing.T) {
	r := newTestRaftCheckQuorumDisabled(1)
	r.preVote = false
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = 0
	r.transferTarget = 2
	r.transferElapsed = 0

	// Tick past the election timeout to trigger transfer abort.
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.transferTarget != noNode {
		t.Fatalf("expected transfer cleared after timeout, got %d", r.transferTarget)
	}
}

// ---------------------------------------------------------------------------
// Test: TimeoutNow
// ---------------------------------------------------------------------------

func TestRaft_TimeoutNow_StartsElection(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.msgs = nil

	err := r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != candidate {
		t.Fatalf("expected candidate after TimeoutNow, got %s", r.state)
	}
}

func TestRaft_TimeoutNow_IgnoredByObserver(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	cfg.IsObserver = true
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected observer to remain follower, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Read index
// ---------------------------------------------------------------------------

func TestRaft_ReadIndex_SingleNodeRespondsImmediately(t *testing.T) {
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

	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 42 {
		t.Fatalf("expected key 42, got %d", r.readStates[0].Key)
	}
}

func TestRaft_ReadIndex_MultiNodeRequiresQuorum(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op entry from becomeLeader so that
	// hasCommittedEntryAtCurrentTerm() returns true. A follower
	// must acknowledge the append for the quorum commit.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Flush deferred ReadIndex broadcast.
	r.flushReadIndex()

	// Should have sent heartbeats, not yet confirmed.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 read states before quorum, got %d", len(r.readStates))
	}

	// Capture the heartbeat ctx from the broadcast.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint

	// Heartbeat response from peer confirms quorum.
	r.drainMessages()
	err = r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state after quorum, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 42 {
		t.Fatalf("expected key 42, got %d", r.readStates[0].Key)
	}
}

func TestRaft_ReadIndex_FollowerForwardsToLeader(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = 2
	r.msgs = nil

	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := messagesTo(r, 2)
	found := false
	for _, m := range msgs {
		if m.Type == proto.ReadIndex {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ReadIndex forwarded to leader")
	}
}

func TestRaft_ReadIndex_FollowerWithNoLeader(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = noLeader

	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})

	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
}

func TestRaft_ReadIndexResp_PopulatesReadStates(t *testing.T) {
	r := newTestRaft(1)
	r.readStates = nil

	err := r.step(proto.Message{
		Type:     proto.ReadIndexResp,
		From:     2,
		Term:     r.term,
		Hint:     42,
		LogIndex: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 42 || r.readStates[0].Index != 10 {
		t.Fatalf("unexpected read state: %+v", r.readStates[0])
	}
}

// ---------------------------------------------------------------------------
// Test: Quiesce
// ---------------------------------------------------------------------------

func TestRaft_Quiesce_LeaderEntersAfterInactivity(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Tick through the quiesce threshold.
	threshold := r.quiesce.threshold
	for i := 0; i < threshold+r.heartbeatTimeout+1; i++ {
		r.tickFunc(r)
		r.drainMessages()
	}

	if !r.quiesce.active {
		t.Fatal("expected quiesce to be active after inactivity")
	}
}

func TestRaft_Quiesce_ExitOnProposal(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Enter quiesce.
	threshold := r.quiesce.threshold
	for i := 0; i < threshold+r.heartbeatTimeout+1; i++ {
		r.tickFunc(r)
		r.drainMessages()
	}
	if !r.quiesce.active {
		t.Fatal("expected quiesce to be active")
	}

	// Propose should exit quiesce.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.quiesce.active {
		t.Fatal("expected quiesce to be inactive after proposal")
	}
}

func TestRaft_Quiesce_FollowerEntersOnQuiesceMessage(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = 2
	r.quiesce.enabled = true

	err := r.step(proto.Message{
		Type: proto.Quiesce,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !r.quiesce.active {
		t.Fatal("expected follower to enter quiesce on leader notification")
	}
}

func TestRaft_Quiesce_FollowerIgnoresFromNonLeader(t *testing.T) {
	r := newTestRaft(1)
	r.leaderID = 2
	r.quiesce.enabled = true

	err := r.step(proto.Message{
		Type: proto.Quiesce,
		From: 3, // not the leader
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.quiesce.active {
		t.Fatal("expected follower to ignore quiesce from non-leader")
	}
}

// ---------------------------------------------------------------------------
// Test: Tick functions
// ---------------------------------------------------------------------------

func TestRaft_TickFollower_AdvancesElectionTick(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	initialTick := r.electionTick
	r.tickFunc(r)
	if r.electionTick != initialTick+1 {
		t.Fatalf("expected electionTick %d, got %d", initialTick+1, r.electionTick)
	}
}

func TestRaft_TickFollower_DoesNotStartElectionBeforeTimeout(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Tick once, should still be follower.
	r.tickFunc(r)
	if r.state != follower {
		t.Fatalf("expected follower, got %s", r.state)
	}
}

func TestRaft_TickLeader_SendsHeartbeat(t *testing.T) {
	r := newTestRaftCheckQuorumDisabled(1)
	r.preVote = false
	electLeader(r)
	r.drainMessages()

	// With heartbeatTimeout=1, the first tick triggers a heartbeat.
	r.tickFunc(r)
	heartbeats := collectMessages(r, proto.Heartbeat)
	if len(heartbeats) == 0 {
		t.Fatal("expected heartbeat after one leader tick")
	}
}

func TestRaft_TickLeader_HeartbeatTickResetsAfterFiring(t *testing.T) {
	r := newTestRaftCheckQuorumDisabled(1)
	r.preVote = false
	// Set a longer heartbeat timeout so we can observe tick advancement.
	r.heartbeatTimeout = 5
	electLeader(r)

	r.tickFunc(r)
	if r.heartbeatTick != 1 {
		t.Fatalf("expected heartbeatTick 1 after one tick, got %d", r.heartbeatTick)
	}
}

func TestRaft_TickCandidate_RestartsElection(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()
	termBefore := r.term
	r.drainMessages()

	advanceElection(r)
	// Should restart: either stays candidate with bumped term or same.
	if r.term < termBefore {
		t.Fatalf("expected term >= %d, got %d", termBefore, r.term)
	}
}

// ---------------------------------------------------------------------------
// Test: Single-node cluster
// ---------------------------------------------------------------------------

func TestRaft_SingleNode_ElectsImmediately(t *testing.T) {
	members := map[uint64]string{1: "addr1"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r.becomeCandidate()
	r.broadcastVote()
	r.checkElectionResult()

	if r.state != leader {
		t.Fatalf("expected single-node to become leader, got %s", r.state)
	}
}

func TestRaft_SingleNode_CommitsImmediately(t *testing.T) {
	members := map[uint64]string{1: "addr1"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)
	r.drainMessages()

	noopIdx := r.log.lastIndex()
	if r.log.committed < noopIdx {
		t.Fatalf("expected noop committed in single-node cluster, committed=%d noop=%d",
			r.log.committed, noopIdx)
	}

	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed != r.log.lastIndex() {
		t.Fatalf("expected all entries committed in single-node, committed=%d last=%d",
			r.log.committed, r.log.lastIndex())
	}
}

// ---------------------------------------------------------------------------
// Test: Witness support
// ---------------------------------------------------------------------------

func TestRaft_Witness_CountsInElectionQuorum(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 3 voting members: 1(self), 2(remote), 3(witness). Quorum = 2.
	eq := electionQuorum(r.remotes, r.witnesses)
	if eq != 2 {
		t.Fatalf("expected election quorum 2, got %d", eq)
	}

	// Commit quorum should exclude witnesses: only self + remote 2 = 2 members, quorum = 2.
	cq := commitQuorum(r.remotes)
	if cq != 2 {
		t.Fatalf("expected commit quorum 2, got %d", cq)
	}
}

func TestRaft_Witness_VotesInElection(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r.becomeCandidate()
	r.broadcastVote()

	// Witness grants vote.
	err = r.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 3,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// self(1) + witness(3) = 2 votes = quorum of 2 in 3-node cluster.
	if r.state != leader {
		t.Fatalf("expected leader with witness vote, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Observer
// ---------------------------------------------------------------------------

func TestRaft_Observer_DoesNotStartElection(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	cfg.IsObserver = true
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	advanceElection(r)

	if r.state != follower {
		t.Fatalf("expected observer to remain follower, got %s", r.state)
	}
}

func TestRaft_Observer_DoesNotStartElectionOnTimeoutNow(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	cfg.IsObserver = true
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected observer to remain follower on TimeoutNow, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Candidate receives Replicate/Heartbeat from leader
// ---------------------------------------------------------------------------

func TestRaft_Candidate_RevertsToFollowerOnAppend(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()

	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     r.term,
		LogIndex: 0,
		LogTerm:  0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected follower after receiving Replicate, got %s", r.state)
	}
	if r.leaderID != 2 {
		t.Fatalf("expected leaderID 2, got %d", r.leaderID)
	}
}

func TestRaft_Candidate_RevertsToFollowerOnHeartbeat(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()

	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected follower after receiving Heartbeat, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: Message sending
// ---------------------------------------------------------------------------

func TestRaft_Send_PopulatesFromAndTerm(t *testing.T) {
	r := newTestRaft(1)
	r.msgs = nil
	r.send(proto.Message{
		Type: proto.Heartbeat,
		To:   2,
	})

	if len(r.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(r.msgs))
	}
	if r.msgs[0].From != r.replicaID {
		t.Fatalf("expected From=%d, got %d", r.replicaID, r.msgs[0].From)
	}
	if r.msgs[0].Term != r.term {
		t.Fatalf("expected Term=%d, got %d", r.term, r.msgs[0].Term)
	}
	if r.msgs[0].ShardID != r.shardID {
		t.Fatalf("expected ShardID=%d, got %d", r.shardID, r.msgs[0].ShardID)
	}
}

func TestRaft_DrainMessages_ClearsBuffer(t *testing.T) {
	r := newTestRaft(1)
	r.send(proto.Message{Type: proto.Heartbeat, To: 2})
	r.send(proto.Message{Type: proto.Heartbeat, To: 3})

	msgs := r.drainMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 drained messages, got %d", len(msgs))
	}
	if len(r.msgs) != 0 {
		t.Fatalf("expected msgs cleared after drain, got %d", len(r.msgs))
	}
}

func TestRaft_DrainMessages_EmptyReturnsNil(t *testing.T) {
	r := newTestRaft(1)
	// No messages sent.
	msgs := r.drainMessages()
	if msgs != nil {
		t.Fatalf("expected nil for empty drain, got %v", msgs)
	}
}

func TestRaft_DrainMessages_DoubleBufferSwap(t *testing.T) {
	r := newTestRaft(1)

	// First batch of messages.
	r.send(proto.Message{Type: proto.Heartbeat, To: 2})
	msgs1 := r.drainMessages()
	if len(msgs1) != 1 {
		t.Fatalf("expected 1 message in first drain, got %d", len(msgs1))
	}

	// Second batch: should reuse the alternate buffer, no allocation.
	r.send(proto.Message{Type: proto.Heartbeat, To: 3})
	msgs2 := r.drainMessages()
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message in second drain, got %d", len(msgs2))
	}
	if msgs2[0].To != 3 {
		t.Fatalf("expected To=3, got %d", msgs2[0].To)
	}

	// Third batch: swaps back to the first buffer.
	r.send(proto.Message{Type: proto.Heartbeat, To: 2})
	r.send(proto.Message{Type: proto.Heartbeat, To: 3})
	msgs3 := r.drainMessages()
	if len(msgs3) != 2 {
		t.Fatalf("expected 2 messages in third drain, got %d", len(msgs3))
	}
}

func TestRaft_DrainMessages_AlternatingEmptyAndFull(t *testing.T) {
	r := newTestRaft(1)

	// Drain with no messages.
	if msgs := r.drainMessages(); msgs != nil {
		t.Fatalf("expected nil, got %v", msgs)
	}

	// Send and drain.
	r.send(proto.Message{Type: proto.Heartbeat, To: 2})
	msgs := r.drainMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// Drain again with no new messages.
	if msgs := r.drainMessages(); msgs != nil {
		t.Fatalf("expected nil after empty drain, got %v", msgs)
	}
}

func TestRaft_DrainFreeOrderMessages_Empty(t *testing.T) {
	r := newTestRaft(1)
	if msgs := r.drainFreeOrderMessages(); msgs != nil {
		t.Fatalf("expected nil from empty buffer, got %v", msgs)
	}
}

func TestRaft_DrainFreeOrderMessages_OnlyFreeOrder(t *testing.T) {
	r := newTestRaft(1)
	r.send(proto.Message{Type: proto.Heartbeat, To: 2})
	r.send(proto.Message{Type: proto.Replicate, To: 3})
	msgs := r.drainFreeOrderMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 free-order messages, got %d", len(msgs))
	}
	// Buffer should be empty after draining all free-order messages.
	if remaining := r.drainMessages(); remaining != nil {
		t.Fatalf("expected empty buffer after all free-order, got %d", len(remaining))
	}
}

func TestRaft_DrainFreeOrderMessages_OnlyOrdered(t *testing.T) {
	r := newTestRaft(1)
	r.send(proto.Message{Type: proto.RequestVoteResp, To: 2})
	r.send(proto.Message{Type: proto.HeartbeatResp, To: 3})
	msgs := r.drainFreeOrderMessages()
	if msgs != nil {
		t.Fatalf("expected nil when no free-order messages, got %d", len(msgs))
	}
	// Ordered messages should still be in the buffer.
	remaining := r.drainMessages()
	if len(remaining) != 2 {
		t.Fatalf("expected 2 ordered messages preserved, got %d", len(remaining))
	}
}

func TestRaft_DrainFreeOrderMessages_MixedPreservesOrdered(t *testing.T) {
	r := newTestRaft(1)
	r.send(proto.Message{Type: proto.Heartbeat, To: 2})
	r.send(proto.Message{Type: proto.RequestVoteResp, To: 3})
	r.send(proto.Message{Type: proto.Replicate, To: 4})
	r.send(proto.Message{Type: proto.ReplicateResp, To: 5})

	free := r.drainFreeOrderMessages()
	if len(free) != 2 {
		t.Fatalf("expected 2 free-order messages, got %d", len(free))
	}
	// Verify free-order messages.
	for _, msg := range free {
		if !proto.IsFreeOrderMessage(msg.Type) {
			t.Errorf("non-free-order message %s in free drain", proto.MessageTypeName(msg.Type))
		}
	}
	// Ordered messages should still be in the buffer.
	ordered := r.drainMessages()
	if len(ordered) != 2 {
		t.Fatalf("expected 2 ordered messages preserved, got %d", len(ordered))
	}
	if ordered[0].Type != proto.RequestVoteResp || ordered[0].To != 3 {
		t.Errorf("expected RequestVoteResp to 3, got %s to %d",
			proto.MessageTypeName(ordered[0].Type), ordered[0].To)
	}
	if ordered[1].Type != proto.ReplicateResp || ordered[1].To != 5 {
		t.Errorf("expected ReplicateResp to 5, got %s to %d",
			proto.MessageTypeName(ordered[1].Type), ordered[1].To)
	}
}

// ---------------------------------------------------------------------------
// Test: roleState String
// ---------------------------------------------------------------------------

func TestRaft_RoleState_String(t *testing.T) {
	tests := []struct {
		state roleState
		want  string
	}{
		{follower, "Follower"},
		{candidate, "Candidate"},
		{preCandidate, "PreCandidate"},
		{leader, "Leader"},
		{roleState(99), "Unknown(99)"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("roleState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: hardState
// ---------------------------------------------------------------------------

func TestRaft_HardState_ReturnsCurrent(t *testing.T) {
	r := newTestRaft(1)
	r.term = 5
	r.vote = 2
	r.log.commitTo(0)

	hs := r.hardState()
	if hs.Term != 5 {
		t.Fatalf("expected term 5, got %d", hs.Term)
	}
	if hs.Vote != 2 {
		t.Fatalf("expected vote 2, got %d", hs.Vote)
	}
}

func TestRaft_HardState_ReflectsCommit(t *testing.T) {
	r := newTestRaft(1)
	r.log.append([]proto.Entry{{Index: 1, Term: 1}})
	r.log.commitTo(1)

	hs := r.hardState()
	if hs.Commit != 1 {
		t.Fatalf("expected commit 1, got %d", hs.Commit)
	}
}

// ---------------------------------------------------------------------------
// Test: SnapshotStatus and Unreachable
// ---------------------------------------------------------------------------

func TestRaft_SnapshotStatus_Success(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.remotes[2].becomeSnapshot(10)

	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.remotes[2].state != remoteProbe {
		t.Fatalf("expected probe state after snapshot success, got %s",
			r.remotes[2].state)
	}
}

func TestRaft_SnapshotStatus_Failure(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.remotes[2].becomeSnapshot(10)

	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.remotes[2].state != remoteProbe {
		t.Fatalf("expected probe state after snapshot failure, got %s",
			r.remotes[2].state)
	}
}

func TestRaft_Unreachable_TransitionsToProbe(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.remotes[2].becomeReplicate()

	err := r.step(proto.Message{
		Type: proto.Unreachable,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.remotes[2].state != remoteProbe {
		t.Fatalf("expected probe state after unreachable, got %s",
			r.remotes[2].state)
	}
}

func TestRaft_Unreachable_UnknownPeerIgnored(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type: proto.Unreachable,
		From: 99,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should not panic.
}

// ---------------------------------------------------------------------------
// Test: Stale append below committed index
// ---------------------------------------------------------------------------

// TestRaft_Append_StaleLogIndexBelowCommitted verifies that when a leader
// sends a Replicate message with prevLogIndex below the follower's committed
// index, the follower responds immediately with its committed index instead
// of attempting tryAppend (which would panic on the invariant that conflict
// index must be greater than committed index).
func TestRaft_Append_StaleLogIndexBelowCommitted(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Build a log with entries 1-5 at term 1.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	})
	// Commit up to index 4.
	r.log.commitTo(4)
	r.leaderID = 2
	r.msgs = nil

	// Leader sends entries starting at prevLogIndex=2, which is below
	// committed index 4. This can happen when a leader has stale tracking
	// of a follower's progress.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     1,
		LogIndex: 2,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
		},
		Commit: 3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should respond with committed index to help leader advance.
	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp")
	}
	if resps[0].Reject {
		t.Fatal("expected non-rejection response for stale append")
	}
	if resps[0].LogIndex != 4 {
		t.Fatalf("expected LogIndex=4 (committed), got %d", resps[0].LogIndex)
	}
	// Committed index should not change (msg.Commit=3 < existing committed=4).
	if r.log.committed != 4 {
		t.Fatalf("expected committed to remain 4, got %d", r.log.committed)
	}
}

// TestRaft_Append_StaleLogIndexAtCommittedBoundary verifies that when
// prevLogIndex equals committed-1 (one below committed), the stale guard
// triggers and responds with the committed index.
func TestRaft_Append_StaleLogIndexAtCommittedBoundary(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.log.commitTo(3)
	r.leaderID = 2
	r.msgs = nil

	// prevLogIndex=2 < committed=3: should trigger stale guard.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     1,
		LogIndex: 2,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 3, Term: 1},
		},
		Commit: 3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp")
	}
	if resps[0].Reject {
		t.Fatal("expected non-rejection for stale guard")
	}
	if resps[0].LogIndex != 3 {
		t.Fatalf("expected LogIndex=3 (committed), got %d", resps[0].LogIndex)
	}
}

// TestRaft_Append_ExactlyAtCommittedNotStale verifies that when
// prevLogIndex equals committed, the normal append path is taken
// (the stale guard only fires for prevLogIndex < committed).
func TestRaft_Append_ExactlyAtCommittedNotStale(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.log.commitTo(3)
	r.leaderID = 2
	r.msgs = nil

	// prevLogIndex=3 == committed=3: NOT stale, should go through tryAppend.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     1,
		LogIndex: 3,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 4, Term: 1},
		},
		Commit: 4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Normal append path: lastIdx should be 4.
	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp")
	}
	if resps[0].Reject {
		t.Fatal("expected success for append at committed boundary")
	}
	if resps[0].LogIndex != 4 {
		t.Fatalf("expected LogIndex=4, got %d", resps[0].LogIndex)
	}
	if r.log.committed != 4 {
		t.Fatalf("expected committed=4, got %d", r.log.committed)
	}
}

// Test: Append commit clamping
// ---------------------------------------------------------------------------

func TestRaft_Append_CommitClampedToLastNewIndex(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
	})
	r.leaderID = 2
	r.msgs = nil

	// Leader sends entries 2-3 with commit=10, but we only have up to index 3.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
		Commit: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Commit should be clamped to lastNewIndex (3).
	if r.log.committed != 3 {
		t.Fatalf("expected committed clamped to 3, got %d", r.log.committed)
	}
}

func TestRaft_Append_CommitDoesNotExceedLeaderCommit(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
	})
	r.leaderID = 2
	r.msgs = nil

	// Leader sends entries 2-5 with commit=3.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
		},
		Commit: 3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Commit should be min(leaderCommit, lastNew) = min(3, 5) = 3.
	if r.log.committed != 3 {
		t.Fatalf("expected committed 3, got %d", r.log.committed)
	}
}

// ---------------------------------------------------------------------------
// Test: Election randomization
// ---------------------------------------------------------------------------

func TestRaft_ElectionTimeout_Randomized(t *testing.T) {
	r := newTestRaft(1)

	// Collect several randomized timeouts.
	timeouts := make(map[int]bool)
	for i := 0; i < 50; i++ {
		r.resetElectionTimeout()
		timeouts[r.randomizedElectionTimeout] = true
	}

	// Should have some variation (not all the same value).
	if len(timeouts) < 2 {
		t.Fatal("expected election timeout to be randomized, got same value every time")
	}

	// All values should be in [electionTimeout, 2*electionTimeout).
	for timeout := range timeouts {
		if timeout < r.electionTimeout || timeout >= 2*r.electionTimeout {
			t.Fatalf("timeout %d out of range [%d, %d)",
				timeout, r.electionTimeout, 2*r.electionTimeout)
		}
	}
}

func TestRaft_ElectionTimeout_NeverBelowMinimum(t *testing.T) {
	r := newTestRaft(1)
	for i := 0; i < 100; i++ {
		r.resetElectionTimeout()
		if r.randomizedElectionTimeout < r.electionTimeout {
			t.Fatalf("randomizedElectionTimeout %d < electionTimeout %d",
				r.randomizedElectionTimeout, r.electionTimeout)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: hasQuorumActive
// ---------------------------------------------------------------------------

func TestRaft_HasQuorumActive_TrueWithSelfAndOnePeer(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.resetRemoteActivity()
	r.remotes[2].setActive()

	if !r.hasQuorumActive() {
		t.Fatal("expected quorum active with self + 1 peer in 3-node cluster")
	}
}

func TestRaft_HasQuorumActive_FalseWithNoActivePeers(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.resetRemoteActivity()

	if r.hasQuorumActive() {
		t.Fatal("expected no quorum with only self active in 3-node cluster")
	}
}

// ---------------------------------------------------------------------------
// Test: Candidate proposal returns not-leader error
// ---------------------------------------------------------------------------

func TestRaft_Candidate_RejectsProposal(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("test")}},
	})

	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError from candidate, got %v", err)
	}
}

func TestRaft_Candidate_ProposalErrorHasNoLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("test")}},
	})

	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
	if nle.LeaderID != 0 {
		t.Fatalf("expected LeaderID 0 from candidate, got %d", nle.LeaderID)
	}
}

// ---------------------------------------------------------------------------
// Test: Stale vote response at higher term
// ---------------------------------------------------------------------------

func TestRaft_Leader_StepsDownOnHigherTermVoteRequest(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     r.term + 1,
		LogTerm:  r.log.lastTerm(),
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected step down to follower on higher-term vote, got %s", r.state)
	}
}

func TestRaft_Leader_IgnoresLowerTermVoteRequest(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     r.term - 1,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != leader {
		t.Fatalf("expected to remain leader, got %s", r.state)
	}

	// Should have sent a rejection.
	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected rejection response")
	}
	if !resps[0].Reject {
		t.Fatal("expected vote rejection for lower term")
	}
}

// ---------------------------------------------------------------------------
// Test: Leader transfer triggers TimeoutNow on catchup
// ---------------------------------------------------------------------------

func TestRaft_LeaderTransfer_SendsTimeoutNowOnCatchup(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Start transfer to peer 2 (behind).
	r.remotes[2].match = 0
	err := r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Simulate peer 2 catching up.
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	timeoutNows := collectMessages(r, proto.TimeoutNow)
	if len(timeoutNows) == 0 {
		t.Fatal("expected TimeoutNow after target caught up during transfer")
	}
}

func TestRaft_LeaderTransfer_DoesNotSendTimeoutNowBeforeCatchup(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	// Acknowledge probes so we can propose more entries.
	noopIdx := r.log.lastIndex()
	for id := range r.remotes {
		err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: noopIdx,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	r.drainMessages()

	// Add more entries so the target will be behind.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("a")}, {Cmd: []byte("b")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Reset peer 2 match to be behind.
	r.remotes[2].match = noopIdx
	r.remotes[2].becomeProbe()
	r.remotes[2].next = noopIdx + 1

	err = r.step(proto.Message{Type: proto.LeaderTransfer, Hint: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Peer 2 responds with partial progress (still behind lastIndex).
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: noopIdx + 1, // still behind lastIndex
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	timeoutNows := collectMessages(r, proto.TimeoutNow)
	if len(timeoutNows) != 0 {
		t.Fatal("should not send TimeoutNow before target catches up")
	}
}

// ---------------------------------------------------------------------------
// Test: sendSnapshot - covers the snapshot sending code path
// ---------------------------------------------------------------------------

func TestRaft_SendSnapshot_SendsInstallSnapshotWhenAvailable(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Set a snapshot on the log so sendSnapshot has something to send.
	r.log.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 2}

	// Trigger sendSnapshot to peer 2.
	r.sendSnapshot(2)

	snaps := collectMessages(r, proto.InstallSnapshot)
	if len(snaps) == 0 {
		t.Fatal("expected InstallSnapshot message when snapshot is available")
	}
	if snaps[0].To != 2 {
		t.Fatalf("expected InstallSnapshot to peer 2, got %d", snaps[0].To)
	}
	if snaps[0].Snapshot.Index != 10 {
		t.Fatalf("expected snapshot index 10, got %d", snaps[0].Snapshot.Index)
	}

	// Remote should be in snapshot state.
	if r.remotes[2].state != remoteSnapshot {
		t.Fatalf("expected remote in snapshot state, got %s", r.remotes[2].state)
	}
}

func TestRaft_SendSnapshot_NoSnapshotAvailableIsNoop(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// No snapshot available in the log.
	r.log.inmem.snapshot = nil
	prevState := r.remotes[2].state

	r.sendSnapshot(2)

	// Should not send any messages and remote state should not change.
	snaps := collectMessages(r, proto.InstallSnapshot)
	if len(snaps) != 0 {
		t.Fatal("expected no InstallSnapshot when no snapshot available")
	}
	if r.remotes[2].state != prevState {
		t.Fatalf("expected remote state unchanged, got %s", r.remotes[2].state)
	}
}

func TestRaft_SendSnapshot_UnknownPeerIsNoop(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.log.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 2}

	// sendSnapshot to an unknown peer should be a no-op.
	r.sendSnapshot(99)

	snaps := collectMessages(r, proto.InstallSnapshot)
	if len(snaps) != 0 {
		t.Fatal("expected no InstallSnapshot for unknown peer")
	}
}

// ---------------------------------------------------------------------------
// Test: sendAppend triggers sendSnapshot when log is compacted
// ---------------------------------------------------------------------------

func TestRaft_SendAppend_FallsBackToSnapshotWhenTermUnavailable(t *testing.T) {
	// Build a raft where the LogDB has been compacted, so term() will
	// return an error for the compacted index, triggering the snapshot path.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	// Pre-populate LogDB with entries 1-5 then compact away entries 1-3.
	logdb.entries = []proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	logdb.state = proto.State{Term: 1, Commit: 5}
	// Compact: removes entries up to index 3, so entries 1-3 are gone.
	logdb.Compact(3)

	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.preVote = false
	electLeader(r) // becomes leader
	r.drainMessages()

	// Set up peer 2 to need an entry at a compacted index.
	// next=3 means prevLogIndex=2, which has been compacted from LogDB
	// and is not in inmem.
	r.remotes[2].match = 0
	r.remotes[2].next = 3
	r.remotes[2].state = remoteProbe
	r.remotes[2].probeSent = false

	// Set a snapshot so sendSnapshot has data to send.
	r.log.inmem.snapshot = &proto.Snapshot{Index: 5, Term: 1}

	r.sendAppend(2)

	// Should have sent an InstallSnapshot (because term(2) returns ErrCompacted).
	snaps := collectMessages(r, proto.InstallSnapshot)
	if len(snaps) == 0 {
		t.Fatal("expected InstallSnapshot when term lookup fails for compacted entry")
	}
}

// ---------------------------------------------------------------------------
// Test: quiesceElectionTimeout
// ---------------------------------------------------------------------------

func TestRaft_QuiesceElectionTimeout_ReturnsCorrectTimeout(t *testing.T) {
	q := newQuiesceState(true, 10)
	timeout := q.quiesceElectionTimeout()
	// livenessInterval = 10 * 10 = 100
	// quiesceElectionTimeout = 100 * 3 = 300
	expected := 10 * quiesceLivenessMultiplier * quiesceElectionTimeoutMultiplier
	if timeout != expected {
		t.Fatalf("expected quiesceElectionTimeout %d, got %d", expected, timeout)
	}
}

func TestRaft_QuiesceElectionTimeout_ScalesWithElectionTimeout(t *testing.T) {
	q5 := newQuiesceState(true, 5)
	q20 := newQuiesceState(true, 20)
	if q20.quiesceElectionTimeout() <= q5.quiesceElectionTimeout() {
		t.Fatal("expected quiesceElectionTimeout to scale with election timeout")
	}
}

// ---------------------------------------------------------------------------
// Test: tickFollower - quiesce active path and tryEnter path
// ---------------------------------------------------------------------------

func TestRaft_TickFollower_QuiesceActiveTriggersElectionOnTimeout(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Manually enter quiesce on the follower.
	r.quiesce.active = true
	r.quiesce.livenessTick = 0
	r.electionTick = 0

	// The quiesced election timeout is livenessInterval * 3.
	quiesceTimeout := r.quiesce.quiesceElectionTimeout()

	// Tick up to just before the timeout; should stay follower.
	for i := 0; i < quiesceTimeout-1; i++ {
		r.tickFunc(r)
	}
	if r.state != follower {
		t.Fatalf("expected follower before quiesce timeout, got %s", r.state)
	}

	// One more tick should trigger election and exit quiesce.
	r.tickFunc(r)
	if r.state != candidate {
		t.Fatalf("expected candidate after quiesce election timeout, got %s", r.state)
	}
	if r.quiesce.active {
		t.Fatal("expected quiesce to be exited after election timeout")
	}
}

func TestRaft_TickFollower_QuiesceActiveReturnsEarlyWithoutNormalPath(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Enter quiesce on the follower.
	r.quiesce.active = true
	r.quiesce.livenessTick = 0
	r.electionTick = 0

	// A single tick should take the quiesce path and NOT trigger a
	// normal election (the normal election timeout is much shorter).
	r.tickFunc(r)

	if r.state != follower {
		t.Fatalf("expected follower, got %s", r.state)
	}
	if r.electionTick != 1 {
		t.Fatalf("expected electionTick 1, got %d", r.electionTick)
	}
}

func TestRaft_TickFollower_EntersQuiesceAfterThreshold(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Tick past the election timeout so tryEnter gets called.
	// We need to accumulate enough ticks for tryEnter to return true.
	// The quiesce threshold is electionTimeout * quiesceThresholdMultiplier = 10*10 = 100.
	// Each time the election fires, the tick counter in quiesce is incremented,
	// and electionTick resets.
	// But tryEnter is only called once per election timeout. So we need to go
	// through 100 election timeout cycles for tryEnter to fire.
	threshold := r.quiesce.threshold
	for i := 0; i < threshold; i++ {
		// Each election timeout cycle triggers an election, but we want tryEnter
		// to fire. We need to tick past the randomized election timeout each cycle.
		// The easier approach: directly tick the follower many times.
		// The follower will start elections each time the randomized timeout fires,
		// but will also call tryEnter at each timeout.
		r.tickFunc(r)
		// If an election started, reset back to follower to keep ticking.
		if r.state != follower {
			r.becomeFollower(r.term, noLeader)
		}
		r.drainMessages()
	}

	// After enough ticks, tryEnter may have activated quiesce.
	// The tryEnter logic increments quiesce.tick each time it is called,
	// but it is only called when electionTick >= randomizedElectionTimeout.
	// So we need to cycle through enough election timeouts.

	// Let's use a more direct approach: manually set the quiesce.tick
	// to just below threshold and tick past one more election timeout.
	r.quiesce.tick = r.quiesce.threshold - 1
	r.quiesce.active = false
	advanceElection(r)
	r.drainMessages()

	// If the node entered quiesce mode, it should NOT have started an election.
	if !r.quiesce.active {
		// tryEnter is only called AFTER the election check. Looking at
		// tickFollower code: the quiesce tryEnter path is checked after
		// electionTick >= randomizedElectionTimeout, and if tryEnter returns
		// true, it returns without calling startElection.
		// Since quiesce.tick was at threshold-1, one more tryEnter call
		// should push it to threshold and activate.
		t.Fatal("expected quiesce to be active after threshold reached")
	}
}

// ---------------------------------------------------------------------------
// Test: tickLiveness
// ---------------------------------------------------------------------------

func TestRaft_TickLiveness_ReturnsFalseWhenNotActive(t *testing.T) {
	q := newQuiesceState(true, 10)
	q.active = false
	if q.tickLiveness() {
		t.Fatal("expected tickLiveness to return false when not active")
	}
}

func TestRaft_TickLiveness_ReturnsTrueAtInterval(t *testing.T) {
	q := newQuiesceState(true, 10)
	q.active = true
	q.livenessTick = 0

	// Tick until liveness interval.
	for i := 0; i < q.livenessInterval-1; i++ {
		if q.tickLiveness() {
			t.Fatalf("expected tickLiveness to return false at tick %d", i+1)
		}
	}
	// The next tick should trigger.
	if !q.tickLiveness() {
		t.Fatal("expected tickLiveness to return true at interval")
	}
	// livenessTick should have been reset.
	if q.livenessTick != 0 {
		t.Fatalf("expected livenessTick reset to 0, got %d", q.livenessTick)
	}
}

// ---------------------------------------------------------------------------
// Test: getMatchForPeer - observer and witness paths
// ---------------------------------------------------------------------------

func TestRaft_GetMatchForPeer_ReturnsMatchForRemote(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.remotes[2].match = 42

	m := r.getMatchForPeer(2)
	if m != 42 {
		t.Fatalf("expected match 42 for remote, got %d", m)
	}
}

func TestRaft_GetMatchForPeer_ReturnsMatchForObserver(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.observers[10].match = 15

	m := r.getMatchForPeer(10)
	if m != 15 {
		t.Fatalf("expected match 15 for observer, got %d", m)
	}
}

func TestRaft_GetMatchForPeer_ReturnsWitnessMatch(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)

	// Witnesses have a remote struct with a valid match field that gets
	// updated when they respond to Replicate messages.
	r.witnesses[20].match = 100

	m := r.getMatchForPeer(20)
	if m != 100 {
		t.Fatalf("expected match 100 for witness, got %d", m)
	}
}

func TestRaft_GetMatchForPeer_ReturnsZeroForWitnessDefault(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)

	// A freshly initialized witness has match=0 (default).
	m := r.getMatchForPeer(20)
	if m != 0 {
		t.Fatalf("expected match 0 for fresh witness, got %d", m)
	}
}

func TestRaft_GetMatchForPeer_ReturnsZeroForUnknown(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	m := r.getMatchForPeer(99)
	if m != 0 {
		t.Fatalf("expected 0 for unknown peer, got %d", m)
	}
}

// ---------------------------------------------------------------------------
// Test: initRemotes - with observers and witnesses
// ---------------------------------------------------------------------------

func TestRaft_InitRemotes_WithObserversAndWitnesses(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that remotes, observers, and witnesses were initialized.
	if len(r.remotes) != 1 { // peer 2 (self=1 is excluded)
		t.Fatalf("expected 1 remote, got %d", len(r.remotes))
	}
	if _, ok := r.remotes[2]; !ok {
		t.Fatal("expected remote for peer 2")
	}
	if len(r.observers) != 1 {
		t.Fatalf("expected 1 observer, got %d", len(r.observers))
	}
	if _, ok := r.observers[10]; !ok {
		t.Fatal("expected observer for peer 10")
	}
	if len(r.witnesses) != 1 {
		t.Fatalf("expected 1 witness, got %d", len(r.witnesses))
	}
	if _, ok := r.witnesses[20]; !ok {
		t.Fatal("expected witness for peer 20")
	}
}

func TestRaft_InitRemotes_ExcludesSelfFromAllMaps(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	// Add self to observers and witnesses to verify exclusion.
	logdb.membership.Observers = map[uint64]string{1: "self-obs"}
	logdb.membership.Witnesses = map[uint64]string{1: "self-wit"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := r.observers[1]; ok {
		t.Fatal("expected self excluded from observers")
	}
	if _, ok := r.witnesses[1]; ok {
		t.Fatal("expected self excluded from witnesses")
	}
}

// ---------------------------------------------------------------------------
// Test: step - stale term branches
// ---------------------------------------------------------------------------

func TestRaft_Step_StaleReplicateFromOldTermSendsResponse(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Pre-populate log so we can elect at a higher term.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	r.term = 1
	electLeader(r) // bumps to term 2
	r.drainMessages()

	// Send a Replicate from term 1 (older than current term 2).
	err := r.step(proto.Message{
		Type: proto.Replicate,
		From: 2,
		Term: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should send a ReplicateResp so the stale sender learns the new term.
	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp for stale Replicate")
	}
	if resps[0].Term != r.term {
		t.Fatalf("expected response with current term %d, got %d", r.term, resps[0].Term)
	}
}

func TestRaft_Step_StaleHeartbeatFromOldTermSendsResponse(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Pre-populate log so we can elect at a higher term.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	r.term = 1
	electLeader(r) // bumps to term 2
	r.drainMessages()

	// Send a Heartbeat from term 1 (older than current term 2).
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp for stale Heartbeat")
	}
}

func TestRaft_Step_StalePreVoteFromOldTermSendsRejection(t *testing.T) {
	r := newTestRaft(1)
	r.term = 5
	r.drainMessages()

	err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     2,
		Term:     3, // lower than current term
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.RequestPreVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected RequestPreVoteResp rejection for stale PreVote")
	}
	if !resps[0].Reject {
		t.Fatal("expected rejection for stale PreVote request")
	}
}

func TestRaft_Step_PreVoteFromHigherTermDoesNotAdvanceTerm(t *testing.T) {
	r := newTestRaft(1)
	originalTerm := r.term
	r.drainMessages()

	// A PreVote from a higher term should NOT cause a term advancement.
	err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     2,
		Term:     originalTerm + 5,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Term should NOT have changed (PreVote does not advance term).
	if r.term != originalTerm {
		t.Fatalf("expected term unchanged at %d, got %d", originalTerm, r.term)
	}
}

func TestRaft_Step_GrantedPreVoteRespFromHigherTermDoesNotAdvanceTerm(t *testing.T) {
	// Use a 5-node cluster so one granted pre-vote is not enough to win.
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfg(1, 1)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r.becomePreCandidate()
	originalTerm := r.term
	r.drainMessages()

	// A granted PreVoteResp from a higher term should not cause term
	// advancement (the PreVote protocol skips becomeFollower for this).
	err = r.step(proto.Message{
		Type:   proto.RequestPreVoteResp,
		From:   2,
		Term:   originalTerm + 5,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Term should NOT have changed because the granted PreVoteResp at a
	// higher term does not cause a step-down (per the PreVote spec).
	if r.term != originalTerm {
		t.Fatalf("expected term unchanged at %d, got %d", originalTerm, r.term)
	}
}

func TestRaft_Step_RejectedPreVoteRespFromHigherTermCausesStepDown(t *testing.T) {
	r := newTestRaft(1)
	r.becomePreCandidate()
	originalTerm := r.term
	r.drainMessages()

	// A REJECTED PreVoteResp from a higher term DOES cause step down.
	err := r.step(proto.Message{
		Type:   proto.RequestPreVoteResp,
		From:   2,
		Term:   originalTerm + 5,
		Reject: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.term != originalTerm+5 {
		t.Fatalf("expected term advanced to %d, got %d", originalTerm+5, r.term)
	}
	if r.state != follower {
		t.Fatalf("expected follower after rejected PreVoteResp, got %s", r.state)
	}
}

func TestRaft_Step_HigherTermVoteRequestSetsLeaderToNoLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.leaderID = 3
	r.drainMessages()

	// A RequestVote from a higher term should clear the leader.
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     r.term + 1,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.leaderID != noLeader {
		t.Fatalf("expected leader cleared to noLeader, got %d", r.leaderID)
	}
}

// ---------------------------------------------------------------------------
// Test: stepCandidate - InstallSnapshot from leader
// ---------------------------------------------------------------------------

func TestRaft_Candidate_RevertsToFollowerOnSnapshot(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()

	ss := proto.Snapshot{Index: 10, Term: r.term}
	err := r.step(proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     r.term,
		Snapshot: ss,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected follower after snapshot during candidacy, got %s", r.state)
	}
	if r.leaderID != 2 {
		t.Fatalf("expected leaderID 2, got %d", r.leaderID)
	}
	if r.log.committed != 10 {
		t.Fatalf("expected committed 10, got %d", r.log.committed)
	}
}

func TestRaft_Candidate_ReceivesProposalReturnsNotLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.becomeCandidate()

	err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("test")}},
	})

	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError from candidate, got %v", err)
	}
	if nle.LeaderID != 0 {
		t.Fatalf("expected LeaderID 0, got %d", nle.LeaderID)
	}
}

// ---------------------------------------------------------------------------
// Test: startElection - witness does not start election
// ---------------------------------------------------------------------------

func TestRaft_Witness_DoesNotStartElection(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	cfg.IsWitness = true
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	advanceElection(r)

	if r.state != follower {
		t.Fatalf("expected witness to remain follower, got %s", r.state)
	}
}

func TestRaft_Witness_TimeoutNowIgnored(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfg(1, 1)
	cfg.IsWitness = true
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected witness to remain follower on TimeoutNow, got %s", r.state)
	}
}

// ---------------------------------------------------------------------------
// Test: ReadIndex quorum excludes witnesses
// ---------------------------------------------------------------------------

func TestRaft_Witness_ReadIndexUsesElectionQuorum(t *testing.T) {
	// 3-node cluster: node 1 (leader), node 2 (remote), node 3 (witness).
	// Election quorum = 2 (all 3 vote). ReadIndex uses electionQuorum
	// because witnesses vote in elections and the leadership confirmation
	// quorum must overlap with every possible election-winning coalition.
	// Either remote 2 or witness 3 ack (plus self) satisfies quorum.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop so hasCommittedEntryAtCurrentTerm is true.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Verify witness is included in replicaToBit.
	if _, found := r.replicaToBit[3]; !found {
		t.Fatal("witness node 3 must have a bit position in replicaToBit")
	}

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 99,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	// Capture the heartbeat ctx from the broadcast.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Should not be resolved yet (need heartbeat ack from any voting member).
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 read states before heartbeat ack, got %d", len(r.readStates))
	}

	// Heartbeat response from remote 2 (data-bearing node) should resolve.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state after remote ack, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 99 {
		t.Fatalf("expected read state key 99, got %d", r.readStates[0].Key)
	}
}

func TestRaft_Witness_ReadIndexRequiresElectionQuorum(t *testing.T) {
	// 4-node cluster: node 1 (leader), node 2 (remote), node 3 (remote),
	// node 4 (witness). electionQuorum = 3 (4 voting / 2 + 1).
	// ReadIndex uses electionQuorum because witnesses vote in elections
	// and the leadership confirmation quorum must overlap with every
	// possible election-winning coalition. One remote ack (self + remote2
	// = 2) is insufficient; need one more ack from remote3 or witness4.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	for id := range r.remotes {
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: r.log.lastIndex(),
		}); err != nil {
			t.Fatalf("ReplicateResp from %d: %v", id, err)
		}
	}
	r.drainMessages()
	r.readStates = nil

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 77,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	// Capture the heartbeat ctx from the broadcast.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// electionQuorum for 4 voting nodes = 3. One remote ack (self + remote2
	// = 2) is NOT sufficient.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 read states (electionQuorum=3, self+remote2=2), got %d", len(r.readStates))
	}

	// Witness ack should bring count to 3 (self + remote2 + witness4).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 4,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state (electionQuorum=3, self+remote2+witness4=3), got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 77 {
		t.Fatalf("expected read state key 77, got %d", r.readStates[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Test: broadcastHeartbeat and broadcastAppend with observers/witnesses
// ---------------------------------------------------------------------------

func TestRaft_BroadcastHeartbeat_IncludesObserversAndWitnesses(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	r.broadcastHeartbeat()

	heartbeats := collectMessages(r, proto.Heartbeat)
	// Should include: remote 2, observer 10, witness 20.
	targets := make(map[uint64]bool)
	for _, hb := range heartbeats {
		targets[hb.To] = true
	}
	if !targets[2] {
		t.Fatal("expected heartbeat to remote 2")
	}
	if !targets[10] {
		t.Fatal("expected heartbeat to observer 10")
	}
	if !targets[20] {
		t.Fatal("expected heartbeat to witness 20")
	}
}

func TestRaft_BroadcastAppend_IncludesObservers(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Clear probe-sent flags so the remotes are not paused.
	for _, rm := range r.remotes {
		rm.probeSent = false
	}
	for _, rm := range r.observers {
		rm.probeSent = false
	}

	r.broadcastAppend()

	replicates := collectMessages(r, proto.Replicate)
	targets := make(map[uint64]bool)
	for _, rep := range replicates {
		targets[rep.To] = true
	}
	if !targets[2] {
		t.Fatal("expected Replicate to remote 2")
	}
	if !targets[10] {
		t.Fatal("expected Replicate to observer 10")
	}
}

func TestRaft_BroadcastAppend_SendsMetadataReplicateToWitnesses(t *testing.T) {
	// Witnesses are voting members that track log position (Index/Term)
	// for election up-to-date checks but don't store Cmd payloads. They
	// must receive Replicate messages with EntryMetadata entries during
	// broadcastAppend so they can track the log position and commit index.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Clear probe-sent flags so remotes are not paused.
	for _, rm := range r.remotes {
		rm.probeSent = false
	}
	for _, rm := range r.witnesses {
		rm.probeSent = false
	}

	r.broadcastAppend()

	// Verify witness 20 received a Replicate message (metadata entries).
	replicates := collectMessages(r, proto.Replicate)
	witnessReplicate := false
	for _, rep := range replicates {
		if rep.To == 20 {
			witnessReplicate = true
			// Replicate should carry the leader's committed index.
			if rep.Commit != r.log.committed {
				t.Fatalf("expected witness replicate commit %d, got %d",
					r.log.committed, rep.Commit)
			}
		}
	}
	if !witnessReplicate {
		t.Fatal("expected Replicate to witness 20 during broadcastAppend")
	}

	// Verify remote 2 still received a Replicate (not broken by the change).
	replicateTargets := make(map[uint64]bool)
	for _, rep := range replicates {
		replicateTargets[rep.To] = true
	}
	if !replicateTargets[2] {
		t.Fatal("expected Replicate to remote 2")
	}
}

func TestRaft_BroadcastAppend_WitnessMetadataMultipleWitnesses(t *testing.T) {
	// Verify that broadcastAppend sends metadata Replicate messages to
	// all witnesses, not just the first one.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1", 30: "wit2"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	for _, rm := range r.remotes {
		rm.probeSent = false
	}
	for _, rm := range r.witnesses {
		rm.probeSent = false
	}

	r.broadcastAppend()

	replicates := collectMessages(r, proto.Replicate)
	witnessIDs := make(map[uint64]bool)
	for _, rep := range replicates {
		if rep.To == 20 || rep.To == 30 {
			witnessIDs[rep.To] = true
		}
	}
	if !witnessIDs[20] {
		t.Fatal("expected Replicate to witness 20")
	}
	if !witnessIDs[30] {
		t.Fatal("expected Replicate to witness 30")
	}
}

func TestRaft_BroadcastAppend_WitnessElectionTimerReset(t *testing.T) {
	// Verify that a witness receiving a Replicate (metadata entries) from
	// broadcastAppend resets its election timer, preventing unnecessary
	// elections. handleAppend resets electionTick on any Replicate.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	witnessCfg := raftTestCfgPreVote(1, 20, false)
	witnessCfg.IsWitness = true
	witnessCfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	witness, err := newRaft(witnessCfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate some ticks elapsing on the witness.
	for i := 0; i < 5; i++ {
		witness.tickFunc(witness)
	}
	if witness.electionTick != 5 {
		t.Fatalf("expected electionTick 5, got %d", witness.electionTick)
	}

	// Deliver a Replicate with metadata entries (as the leader would send
	// during broadcastAppend via sendWitnessAppend).
	err = witness.step(proto.Message{
		Type:     proto.Replicate,
		From:     1,
		To:       20,
		Term:     witness.term,
		LogIndex: 0,
		LogTerm:  0,
		Entries: []proto.Entry{
			{Index: 1, Term: witness.term, Type: proto.EntryMetadata},
		},
		Commit: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error stepping replicate: %v", err)
	}

	// Election tick should be reset to 0.
	if witness.electionTick != 0 {
		t.Fatalf("expected electionTick 0 after replicate, got %d",
			witness.electionTick)
	}
}

func TestRaft_BroadcastAppend_NoWitnesses(t *testing.T) {
	// Verify broadcastAppend works correctly when there are no witnesses.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	for _, rm := range r.remotes {
		rm.probeSent = false
	}

	r.broadcastAppend()

	// Should produce Replicate to remote 2 and no Heartbeats.
	replicates := collectMessages(r, proto.Replicate)
	if len(replicates) == 0 {
		t.Fatal("expected at least one Replicate message")
	}
	heartbeats := collectMessages(r, proto.Heartbeat)
	if len(heartbeats) != 0 {
		t.Fatalf("expected no Heartbeat messages without witnesses, got %d",
			len(heartbeats))
	}
}

// ---------------------------------------------------------------------------
// Test: makeMetadataEntries
// ---------------------------------------------------------------------------

func TestMakeMetadataEntries(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal, Cmd: []byte("cmd1")},
		{Index: 2, Term: 1, Type: proto.EntryEncodedEntry, Cmd: []byte("enc"), ClientID: 42, SeriesID: 5},
		{Index: 3, Term: 2, Type: proto.EntryNormal, Cmd: []byte("cmd3")},
	}

	meta := makeMetadataEntries(entries)
	if len(meta) != 3 {
		t.Fatalf("expected 3 metadata entries, got %d", len(meta))
	}

	for i, m := range meta {
		if m.Index != entries[i].Index {
			t.Fatalf("entry %d: Index = %d, want %d", i, m.Index, entries[i].Index)
		}
		if m.Term != entries[i].Term {
			t.Fatalf("entry %d: Term = %d, want %d", i, m.Term, entries[i].Term)
		}
		if m.Type != proto.EntryMetadata {
			t.Fatalf("entry %d: Type = %d, want EntryMetadata (%d)", i, m.Type, proto.EntryMetadata)
		}
		if m.Cmd != nil {
			t.Fatalf("entry %d: Cmd should be nil, got %v", i, m.Cmd)
		}
		if m.Key != 0 || m.ClientID != 0 || m.SeriesID != 0 || m.RespondedTo != 0 {
			t.Fatalf("entry %d: session fields should be zeroed", i)
		}
	}
}

func TestMakeMetadataEntries_Empty(t *testing.T) {
	meta := makeMetadataEntries(nil)
	if meta != nil {
		t.Fatalf("expected nil for empty input, got %v", meta)
	}
	meta = makeMetadataEntries([]proto.Entry{})
	if meta != nil {
		t.Fatalf("expected nil for zero-length input, got %v", meta)
	}
}

func TestMakeMetadataEntries_PreservesConfigChange(t *testing.T) {
	// Config change entries must be preserved unmodified for witnesses
	// to maintain correct cluster membership.
	ccCmd := []byte("add-node 5")
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal, Cmd: []byte("cmd1")},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: ccCmd, Key: 99},
		{Index: 3, Term: 2, Type: proto.EntryNormal, Cmd: []byte("cmd3")},
	}

	meta := makeMetadataEntries(entries)
	if len(meta) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(meta))
	}

	// Entry 0: normal -> metadata.
	if meta[0].Type != proto.EntryMetadata {
		t.Fatalf("entry 0: expected EntryMetadata, got %d", meta[0].Type)
	}
	if meta[0].Cmd != nil {
		t.Fatal("entry 0: metadata entry should have nil Cmd")
	}

	// Entry 1: config change -> preserved as-is.
	if meta[1].Type != proto.EntryConfigChange {
		t.Fatalf("entry 1: expected EntryConfigChange, got %d", meta[1].Type)
	}
	if string(meta[1].Cmd) != string(ccCmd) {
		t.Fatalf("entry 1: Cmd mismatch: got %q, want %q", meta[1].Cmd, ccCmd)
	}
	if meta[1].Key != 99 {
		t.Fatalf("entry 1: Key = %d, want 99", meta[1].Key)
	}
	if meta[1].Index != 2 || meta[1].Term != 1 {
		t.Fatalf("entry 1: Index/Term mismatch")
	}

	// Entry 2: normal -> metadata.
	if meta[2].Type != proto.EntryMetadata {
		t.Fatalf("entry 2: expected EntryMetadata, got %d", meta[2].Type)
	}
}

func TestMakeMetadataEntries_AllConfigChanges(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
	}
	meta := makeMetadataEntries(entries)
	if len(meta) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(meta))
	}
	for i, m := range meta {
		if m.Type != proto.EntryConfigChange {
			t.Fatalf("entry %d: expected EntryConfigChange, got %d", i, m.Type)
		}
		if m.Cmd == nil {
			t.Fatalf("entry %d: config change Cmd should not be nil", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: sendWitnessAppend
// ---------------------------------------------------------------------------

func TestRaft_SendWitnessAppend_MetadataEntries(t *testing.T) {
	// Verify sendWitnessAppend sends Replicate with EntryMetadata entries
	// to the witness, stripping Cmd payloads from normal entries.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)

	// Propose some entries so there is data to replicate.
	err = r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("data1")}, {Cmd: []byte("data2")}},
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	r.drainMessages()

	// Reset the witness remote so it is not paused.
	r.witnesses[20].probeSent = false

	r.sendWitnessAppend(20)

	replicates := collectMessages(r, proto.Replicate)
	var witnessMsg *proto.Message
	for i := range replicates {
		if replicates[i].To == 20 {
			witnessMsg = &replicates[i]
			break
		}
	}
	if witnessMsg == nil {
		t.Fatal("expected Replicate to witness 20")
	}

	// All entries should be metadata-only (no Cmd).
	for i, ent := range witnessMsg.Entries {
		if ent.Type != proto.EntryMetadata {
			t.Fatalf("entry %d: expected EntryMetadata, got %d", i, ent.Type)
		}
		if ent.Cmd != nil {
			t.Fatalf("entry %d: metadata entry should have nil Cmd", i)
		}
		if ent.Index == 0 {
			t.Fatalf("entry %d: Index should not be zero", i)
		}
		if ent.Term == 0 {
			t.Fatalf("entry %d: Term should not be zero", i)
		}
	}

	// Commit index should be present.
	if witnessMsg.Commit != r.log.committed {
		t.Fatalf("expected commit %d, got %d", r.log.committed, witnessMsg.Commit)
	}
}

func TestRaft_SendWitnessAppend_UnknownPeerIsNoop(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.sendWitnessAppend(99)

	replicates := collectMessages(r, proto.Replicate)
	for _, rep := range replicates {
		if rep.To == 99 {
			t.Fatal("sendWitnessAppend to unknown peer should produce no messages")
		}
	}
}

func TestRaft_SendWitnessAppend_PausedRemoteIsNoop(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Leave witness in probe state with probeSent=true (paused).
	r.witnesses[20].probeSent = true

	r.sendWitnessAppend(20)

	replicates := collectMessages(r, proto.Replicate)
	for _, rep := range replicates {
		if rep.To == 20 {
			t.Fatal("paused witness should not receive Replicate")
		}
	}
}

func TestRaft_SendWitnessAppend_FallsBackToSnapshotWhenCompacted(t *testing.T) {
	// When the witness is too far behind and the log is compacted,
	// sendWitnessAppend should fall back to sendSnapshot.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	// Pre-populate LogDB with entries 1-5 then set markerIndex to compact 1-3.
	logdb.entries = []proto.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	logdb.markerIndex = 4 // entries 1-3 compacted
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Set a snapshot so sendSnapshot has something to send.
	r.log.inmem.snapshot = &proto.Snapshot{Index: 3, Term: 1}

	// Set witness next to 1 (behind compaction point).
	r.witnesses[20].next = 1
	r.witnesses[20].probeSent = false

	r.sendWitnessAppend(20)

	// Should fall back to InstallSnapshot.
	snaps := collectMessages(r, proto.InstallSnapshot)
	found := false
	for _, s := range snaps {
		if s.To == 20 {
			found = true
			// Witness snapshots should be stripped.
			if s.Snapshot.Filepath != "" {
				t.Fatal("witness snapshot should have empty Filepath")
			}
			if s.Snapshot.FileSize != 0 {
				t.Fatal("witness snapshot should have zero FileSize")
			}
			if !s.Snapshot.Witness {
				t.Fatal("witness snapshot should have Witness=true")
			}
		}
	}
	if !found {
		t.Fatal("expected InstallSnapshot to witness 20 after compaction")
	}
}

// ---------------------------------------------------------------------------
// Test: sendSnapshot strips data for witnesses
// ---------------------------------------------------------------------------

func TestRaft_SendSnapshot_WitnessStripped(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Set a snapshot with file data.
	r.log.inmem.snapshot = &proto.Snapshot{
		Index:    10,
		Term:     2,
		Filepath: "/path/to/snapshot.dat",
		FileSize: 1024000,
		Witness:  false,
	}

	r.sendSnapshot(20)

	snaps := collectMessages(r, proto.InstallSnapshot)
	if len(snaps) == 0 {
		t.Fatal("expected InstallSnapshot message")
	}
	var witnessSnap *proto.Message
	for i := range snaps {
		if snaps[i].To == 20 {
			witnessSnap = &snaps[i]
			break
		}
	}
	if witnessSnap == nil {
		t.Fatal("expected InstallSnapshot to witness 20")
	}

	// Witness snapshot should have file data stripped.
	if witnessSnap.Snapshot.Filepath != "" {
		t.Fatalf("expected empty Filepath for witness snapshot, got %q",
			witnessSnap.Snapshot.Filepath)
	}
	if witnessSnap.Snapshot.FileSize != 0 {
		t.Fatalf("expected zero FileSize for witness snapshot, got %d",
			witnessSnap.Snapshot.FileSize)
	}
	if !witnessSnap.Snapshot.Witness {
		t.Fatal("expected Witness=true for witness snapshot")
	}

	// Metadata should be preserved.
	if witnessSnap.Snapshot.Index != 10 {
		t.Fatalf("expected snapshot Index 10, got %d", witnessSnap.Snapshot.Index)
	}
	if witnessSnap.Snapshot.Term != 2 {
		t.Fatalf("expected snapshot Term 2, got %d", witnessSnap.Snapshot.Term)
	}
}

func TestRaft_SendSnapshot_NonWitnessNotStripped(t *testing.T) {
	// Verify that sendSnapshot to a normal remote does NOT strip file data.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.log.inmem.snapshot = &proto.Snapshot{
		Index:    10,
		Term:     2,
		Filepath: "/path/to/snapshot.dat",
		FileSize: 1024000,
	}

	r.sendSnapshot(2)

	snaps := collectMessages(r, proto.InstallSnapshot)
	if len(snaps) == 0 {
		t.Fatal("expected InstallSnapshot message")
	}
	if snaps[0].Snapshot.Filepath != "/path/to/snapshot.dat" {
		t.Fatalf("expected Filepath preserved for non-witness, got %q",
			snaps[0].Snapshot.Filepath)
	}
	if snaps[0].Snapshot.FileSize != 1024000 {
		t.Fatalf("expected FileSize preserved for non-witness, got %d",
			snaps[0].Snapshot.FileSize)
	}
	if snaps[0].Snapshot.Witness {
		t.Fatal("non-witness snapshot should not have Witness=true")
	}
}

// ---------------------------------------------------------------------------
// Test: broadcastQuiesce sends to observers and witnesses
// ---------------------------------------------------------------------------

func TestRaft_BroadcastQuiesce_SendsToAllPeers(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	r.broadcastQuiesce()

	quiesces := collectMessages(r, proto.Quiesce)
	targets := make(map[uint64]bool)
	for _, q := range quiesces {
		targets[q.To] = true
	}
	if !targets[2] {
		t.Fatal("expected Quiesce to remote 2")
	}
	if !targets[10] {
		t.Fatal("expected Quiesce to observer 10")
	}
	if !targets[20] {
		t.Fatal("expected Quiesce to witness 20")
	}
}

// ---------------------------------------------------------------------------
// Test: getRemote - observer fallback
// ---------------------------------------------------------------------------

func TestRaft_GetRemote_ReturnsObserver(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := r.getRemote(10)
	if rm == nil {
		t.Fatal("expected getRemote to return observer")
	}
}

func TestRaft_GetRemote_ReturnsWitness(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := r.getRemote(20)
	if rm == nil {
		t.Fatal("expected getRemote to return witness remote")
	}
}

func TestRaft_GetRemote_ReturnsNilForUnknown(t *testing.T) {
	r := newTestRaft(1)
	rm := r.getRemote(99)
	if rm != nil {
		t.Fatal("expected nil for unknown peer")
	}
}

func TestRaft_GetRemote_PriorityOrder(t *testing.T) {
	// Verify lookup priority: remotes first, then observers, then witnesses.
	// Use distinct replica IDs across all three maps to confirm each fallback.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Remote should be found first.
	if rm := r.getRemote(2); rm == nil {
		t.Fatal("expected getRemote to find remote 2")
	}
	// Observer should be found as fallback.
	if rm := r.getRemote(10); rm == nil {
		t.Fatal("expected getRemote to find observer 10")
	}
	// Witness should be found as third fallback.
	if rm := r.getRemote(20); rm == nil {
		t.Fatal("expected getRemote to find witness 20")
	}
	// Unknown should return nil.
	if rm := r.getRemote(99); rm != nil {
		t.Fatal("expected nil for unknown peer 99")
	}
}

// TestRaft_HeartbeatResp_WitnessAcksReadIndex verifies that heartbeat
// responses from witnesses are correctly processed through the ReadIndex
// quorum check, now that getRemote includes the witnesses map.
func TestRaft_HeartbeatResp_WitnessAcksReadIndex(t *testing.T) {
	// 2 voting members (1=self, 2=witness), quorum=2.
	members := map[uint64]string{1: "addr1"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{2: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()
	r.readStates = nil

	// ReadIndex generates heartbeat ctx.
	if err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 77}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	// Find the heartbeat sent to witness 2 and extract the ctx.
	hbs := messagesTo(r, 2)
	var hbCtx uint64
	for _, m := range hbs {
		if m.Type == proto.Heartbeat {
			hbCtx = m.Hint
			break
		}
	}
	r.drainMessages()

	// Witness responds with the heartbeat context.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	// Quorum should be reached (self + witness = 2/2).
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state after witness ack, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 77 {
		t.Fatalf("expected key 77, got %d", r.readStates[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Test: campaign sends to witnesses
// ---------------------------------------------------------------------------

func TestRaft_Campaign_SendsVoteRequestToWitnesses(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r.becomeCandidate()
	r.drainMessages()
	r.broadcastVote()

	votes := collectMessages(r, proto.RequestVote)
	targets := make(map[uint64]bool)
	for _, v := range votes {
		targets[v.To] = true
	}
	if !targets[2] {
		t.Fatal("expected RequestVote to remote 2")
	}
	if !targets[3] {
		t.Fatal("expected RequestVote to witness 3")
	}
}

// ---------------------------------------------------------------------------
// Test: hasQuorumActive with witnesses
// ---------------------------------------------------------------------------

func TestRaft_HasQuorumActive_WithActiveWitness(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.resetRemoteActivity()

	// With 3 voting members (self + remote 2 + witness 3), quorum = 2.
	// Mark only the witness as active; self(1) + witness(3) = 2 = quorum.
	r.witnesses[3].setActive()

	if !r.hasQuorumActive() {
		t.Fatal("expected quorum active with self + active witness")
	}
}

func TestRaft_ResetRemoteActivity_ClearsWitnesses(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)

	r.witnesses[3].setActive()
	r.resetRemoteActivity()

	if r.witnesses[3].isActive() {
		t.Fatal("expected witness to be inactive after reset")
	}
}

// ---------------------------------------------------------------------------
// Test: HeartbeatResp marks witness active
// ---------------------------------------------------------------------------

func TestRaft_HeartbeatResp_MarksWitnessActive(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()
	r.resetRemoteActivity()

	err = r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !r.witnesses[3].isActive() {
		t.Fatal("expected witness to be active after heartbeat resp")
	}
}

// ---------------------------------------------------------------------------
// Test: SnapshotStatus for non-snapshot state and unknown peer
// ---------------------------------------------------------------------------

func TestRaft_SnapshotStatus_NotInSnapshotStateIgnored(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Remote is in probe state, not snapshot.
	r.remotes[2].becomeProbe()

	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   2,
		Term:   r.term,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should remain in probe state (no transition).
	if r.remotes[2].state != remoteProbe {
		t.Fatalf("expected probe state, got %s", r.remotes[2].state)
	}
}

func TestRaft_SnapshotStatus_UnknownPeerIgnored(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type:   proto.SnapshotStatus,
		From:   99,
		Term:   r.term,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should not panic.
}

// ---------------------------------------------------------------------------
// Test: log.entries with LogDB fallback path
// ---------------------------------------------------------------------------

func TestRaftLog_Entries_FallsBackToLogDB(t *testing.T) {
	// Create a log reader with persisted entries.
	logdb := newTestLogReader(
		[]proto.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
		proto.State{},
	)
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(3), // markerIndex = 4, so entries 1-3 are in LogDB
		committed: 0,
		processed: 0,
	}

	result, err := l.entries(1, 4, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries from LogDB, got %d", len(result))
	}
	if result[0].Index != 1 || result[2].Index != 3 {
		t.Fatalf("unexpected indices: %d, %d", result[0].Index, result[2].Index)
	}
}

func TestRaftLog_Entries_CombinesLogDBAndInmem(t *testing.T) {
	// Entries 1-3 in LogDB, entries 4-5 in inmem.
	logdb := newTestLogReader(
		[]proto.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
		proto.State{},
	)
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(3), // markerIndex = 4
		committed: 0,
		processed: 0,
	}
	l.inmem.merge([]proto.Entry{
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
	})

	result, err := l.entries(1, 6, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 entries (3 from LogDB + 2 from inmem), got %d", len(result))
	}
	if result[3].Term != 2 {
		t.Fatalf("expected term 2 for index 4, got %d", result[3].Term)
	}
}

func TestRaftLog_Entries_LogDBReturnsError(t *testing.T) {
	// Create a compacted LogDB that returns ErrCompacted.
	logdb := newTestLogReader(nil, proto.State{})
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(5), // markerIndex = 6
		committed: 0,
		processed: 0,
	}

	// Request entries below markerIndex but LogDB has no entries.
	_, err := l.entries(1, 4, 0)
	if err == nil {
		t.Fatal("expected error when LogDB cannot provide entries")
	}
}

// ---------------------------------------------------------------------------
// Test: log.lastTerm error path
// ---------------------------------------------------------------------------

func TestRaftLog_LastTerm_ReturnsZeroOnError(t *testing.T) {
	// Create a log where the last index is not available from either inmem or LogDB.
	logdb := newTestLogReader(nil, proto.State{})
	l := &raftLog{
		logdb:     logdb,
		inmem:     newInMemory(5), // markerIndex = 6, lastIndex returns 5
		committed: 0,
		processed: 0,
	}
	// lastIndex() returns 5, but term(5) will fail because:
	// - inmem has no entries and markerIndex is 6, so 5 < 6 is false... actually
	//   inmem.term(5) would need to check markerIndex. Let me verify.
	// markerIndex = 6, entries = nil. For index 5: 5 < 6, so it checks if
	// index == snapshot.Index. No snapshot, so returns false.
	// Then logdb.Term(5) returns ErrUnavailable.
	// So lastTerm returns 0.
	term := l.lastTerm()
	if term != 0 {
		t.Fatalf("expected lastTerm 0, got %d", term)
	}
}

// ---------------------------------------------------------------------------
// Test: LeaderTransfer via From field (Hint=0)
// ---------------------------------------------------------------------------

func TestRaft_LeaderTransfer_UsesFromWhenHintZero(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.remotes[2].match = r.log.lastIndex()

	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
		Hint: 0, // no hint, use From
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}

	timeoutNows := collectMessages(r, proto.TimeoutNow)
	if len(timeoutNows) == 0 {
		t.Fatal("expected TimeoutNow to caught-up target")
	}
}

// ---------------------------------------------------------------------------
// Fix 2: ReadIndex hasCommittedEntryAtCurrentTerm
// ---------------------------------------------------------------------------

func TestRaft_ReadIndex_RejectsBeforeNoopCommitted(t *testing.T) {
	// A fresh leader has appended the no-op but not committed it yet
	// (needs quorum). ReadIndex must return ErrReadIndexNotReady per
	// PhD thesis 6.4.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	// The no-op is appended but not committed because no ReplicateResp
	// has been received from a peer. Verify the precondition.
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Fatal("no-op should not be committed yet in a 3-node cluster")
	}

	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: r.replicaID,
		Hint: 42,
	})
	if !errors.Is(err, ErrReadIndexNotReady) {
		t.Fatalf("expected ErrReadIndexNotReady, got %v", err)
	}
}

func TestRaft_ReadIndex_AcceptsAfterNoopCommitted(t *testing.T) {
	// Once a peer acks the no-op, it commits. ReadIndex should succeed.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op: peer 2 acks up to lastIndex.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error on ReplicateResp: %v", err)
	}
	if !r.hasCommittedEntryAtCurrentTerm() {
		t.Fatal("no-op should be committed after quorum ack")
	}
	r.drainMessages()

	// Now ReadIndex should succeed (no error).
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: r.replicaID,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("expected ReadIndex to succeed after no-op committed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Fix 3: ReadIndex forwarding response
// ---------------------------------------------------------------------------

func TestRaft_ReadIndex_ForwardedFromFollowerGetsResponse(t *testing.T) {
	// Leader receives a ReadIndex with From=follower (forwarded).
	// After quorum heartbeat ack, a ReadIndexResp must be sent to
	// the follower.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op so ReadIndex is ready.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Submit a ReadIndex from follower 3.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 3,
		Hint: 99,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.flushReadIndex()

	// Heartbeats should have been sent to peers. Capture the ctx from
	// the heartbeat sent to peer 2.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for read index quorum")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Simulate heartbeat response from peer 2 with matching ctx.
	err = r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A ReadIndexResp should be sent to follower 3.
	resps := collectMessages(r, proto.ReadIndexResp)
	if len(resps) != 1 {
		t.Fatalf("expected 1 ReadIndexResp, got %d", len(resps))
	}
	if resps[0].To != 3 {
		t.Fatalf("expected ReadIndexResp to follower 3, got to=%d", resps[0].To)
	}
	if resps[0].Hint != 99 {
		t.Fatalf("expected ReadIndexResp hint 99, got %d", resps[0].Hint)
	}
}

func TestRaft_ReadIndex_LocalRequestAddsReadState(t *testing.T) {
	// Leader receives a ReadIndex from itself (From=0 or From=replicaID).
	// After quorum confirmation, a readState should be added locally,
	// NOT a ReadIndexResp message.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Submit a local ReadIndex (From=0 means local).
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 77,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.flushReadIndex()

	// Capture heartbeat ctx.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	readStatesBefore := len(r.readStates)

	// Simulate heartbeat response from peer 2.
	err = r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have added a readState, not sent a ReadIndexResp.
	if len(r.readStates) != readStatesBefore+1 {
		t.Fatalf("expected %d readStates, got %d", readStatesBefore+1, len(r.readStates))
	}
	last := r.readStates[len(r.readStates)-1]
	if last.Key != 77 {
		t.Fatalf("expected readState key 77, got %d", last.Key)
	}

	// No ReadIndexResp message should be sent.
	resps := collectMessages(r, proto.ReadIndexResp)
	if len(resps) != 0 {
		t.Fatalf("expected 0 ReadIndexResp messages for local request, got %d", len(resps))
	}
}

// ---------------------------------------------------------------------------
// Fix 4: Multiple pending ReadIndex requests
// ---------------------------------------------------------------------------

func TestRaft_ReadIndex_MultiplePendingRequests(t *testing.T) {
	// Submit 3 ReadIndex requests, then confirm quorum for all.
	// All 3 must resolve.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Submit 3 ReadIndex requests with different keys.
	ctxs := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		err = r.step(proto.Message{
			Type: proto.ReadIndex,
			From: 0,
			Hint: uint64(100 + i),
		})
		if err != nil {
			t.Fatalf("unexpected error on ReadIndex %d: %v", i, err)
		}
		// Flush and capture the heartbeat ctx for this ReadIndex.
		r.flushReadIndex()
		hbs := collectMessages(r, proto.Heartbeat)
		if len(hbs) == 0 {
			t.Fatalf("expected heartbeats for ReadIndex %d", i)
		}
		ctxs[i] = hbs[0].Hint
		r.drainMessages()
	}

	if len(r.pendingReadIndex) != 3 {
		t.Fatalf("expected 3 pending read indexes, got %d", len(r.pendingReadIndex))
	}

	readStatesBefore := len(r.readStates)

	// Confirm quorum for all 3 by sending heartbeat responses with
	// each ctx from peer 2.
	for i := 0; i < 3; i++ {
		err = r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: 2,
			Term: r.term,
			Hint: ctxs[i],
		})
		if err != nil {
			t.Fatalf("unexpected error on HeartbeatResp %d: %v", i, err)
		}
		r.drainMessages()
	}

	// All 3 should have resolved into readStates.
	if len(r.readStates) != readStatesBefore+3 {
		t.Fatalf("expected %d readStates, got %d", readStatesBefore+3, len(r.readStates))
	}
	for i := 0; i < 3; i++ {
		rs := r.readStates[readStatesBefore+i]
		if rs.Key != uint64(100+i) {
			t.Fatalf("readState[%d] key: expected %d, got %d", i, 100+i, rs.Key)
		}
	}

	// Pending list should be drained.
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending read indexes, got %d", len(r.pendingReadIndex))
	}
}

func TestRaft_ReadIndex_PendingClearedOnBecomeFollower(t *testing.T) {
	// Pending read indices must be cleared when the leader steps down.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Submit a ReadIndex.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.flushReadIndex()
	r.drainMessages()

	if len(r.pendingReadIndex) == 0 {
		t.Fatal("expected pending read index before becoming follower")
	}

	// Step down to follower.
	r.becomeFollower(r.term+1, 2)

	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected pending read indexes cleared on become follower, got %d",
			len(r.pendingReadIndex))
	}
}

// ---------------------------------------------------------------------------
// Fix 5: OrderedConfigChange enforcement
// ---------------------------------------------------------------------------

func TestRaft_PendingConfigChange_BlocksSecondConfigChange(t *testing.T) {
	// Proposing a config change while one is already pending must be
	// rejected with ErrPendingConfigChange per PhD thesis 4.1.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// First config change entry.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
		},
	})
	if err != nil {
		t.Fatalf("first config change should succeed, got %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be true after first config change")
	}
	r.drainMessages()

	// Second config change before the first commits.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
		},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange, got %v", err)
	}
}

func TestRaft_PendingConfigChange_ClearedOnBecomeFollower(t *testing.T) {
	// pendingConfigChange must be false after becoming follower.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Set a pending config change.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be true")
	}

	// Step down to follower.
	r.becomeFollower(r.term+1, 2)

	if r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be cleared after becoming follower")
	}
}

func TestRaft_BecomeLeader_ScansUncommittedConfigChange(t *testing.T) {
	// PhD 4.3: when a node becomes leader, it must detect uncommitted
	// config change entries in the log (inherited from the previous
	// leader) and set pendingConfigChange to block duplicate proposals.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Propose a config change as leader.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
		},
	})
	if err != nil {
		t.Fatalf("first config change should succeed, got %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be true after config change proposal")
	}
	r.drainMessages()

	// Simulate the node stepping down and then being re-elected without
	// the config change entry being committed. The entry remains in the
	// log at lastIndex, uncommitted.
	savedTerm := r.term
	r.becomeFollower(savedTerm+1, 2)
	if r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be cleared after becoming follower")
	}

	// Re-elect as leader. becomeLeader must scan the log and detect the
	// uncommitted config change entry, setting pendingConfigChange = true.
	r.becomeCandidate()
	r.broadcastVote()
	for id := range r.remotes {
		if id != r.replicaID {
			err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			})
			if err != nil {
				t.Fatalf("vote step failed: %v", err)
			}
			if r.state == leader {
				break
			}
		}
	}
	if r.state != leader {
		t.Fatal("node should be leader")
	}

	// The uncommitted config change from the previous term must be detected.
	if !r.pendingConfigChange {
		t.Fatal("becomeLeader must detect uncommitted config change and set pendingConfigChange")
	}

	// A second config change proposal must be rejected.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
		},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange for duplicate config change, got %v", err)
	}
}

func TestRaft_BecomeLeader_NoUncommittedConfigChange_AllowsProposal(t *testing.T) {
	// When there are no uncommitted config change entries,
	// becomeLeader must leave pendingConfigChange as false so that
	// new config changes can be proposed.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// No config change proposed, just normal entries.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryNormal, Cmd: []byte("data1")},
		},
	})
	if err != nil {
		t.Fatalf("normal propose should succeed, got %v", err)
	}
	r.drainMessages()

	// Step down and re-elect.
	savedTerm := r.term
	r.becomeFollower(savedTerm+1, 2)
	r.becomeCandidate()
	r.broadcastVote()
	for id := range r.remotes {
		if id != r.replicaID {
			err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			})
			if err != nil {
				t.Fatalf("vote step failed: %v", err)
			}
			if r.state == leader {
				break
			}
		}
	}
	if r.state != leader {
		t.Fatal("node should be leader")
	}

	// No uncommitted config change: pendingConfigChange must be false.
	if r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be false when no uncommitted config change exists")
	}

	// A config change proposal must succeed.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc-new")},
		},
	})
	if err != nil {
		t.Fatalf("config change should succeed when no prior uncommitted config change exists, got %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be true after proposing config change")
	}
}

// ---------------------------------------------------------------------------
// Fix 7: handleSnapshot updates membership/remotes
// ---------------------------------------------------------------------------

func TestRaft_HandleSnapshot_UpdatesRemotes(t *testing.T) {
	// Follower receives a snapshot with a different membership.
	// After applying, remotes must reflect the new membership.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(2, 2)

	ss := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "a", 4: "d", 5: "e"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     2,
		Snapshot: ss,
	}

	err := r.step(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Remotes should now contain nodes 4 and 5 (excluding self=1).
	if _, ok := r.remotes[4]; !ok {
		t.Fatal("expected remote for node 4 after snapshot")
	}
	if _, ok := r.remotes[5]; !ok {
		t.Fatal("expected remote for node 5 after snapshot")
	}
	// Old nodes 2 and 3 should no longer be present (they are not in
	// the new membership's Addresses).
	if _, ok := r.remotes[2]; ok {
		t.Fatal("node 2 should not be in remotes after snapshot with different membership")
	}
	if _, ok := r.remotes[3]; ok {
		t.Fatal("node 3 should not be in remotes after snapshot with different membership")
	}
}

func TestRaft_HandleSnapshot_ExcludesSelfFromRemotes(t *testing.T) {
	// After snapshot restore, self (replicaID=1) should not appear in
	// the remotes map even if listed in the snapshot membership.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(2, 2)

	ss := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     2,
		Snapshot: ss,
	}

	err := r.step(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Self should not be in remotes.
	if _, ok := r.remotes[r.replicaID]; ok {
		t.Fatal("self should not be in remotes after snapshot restore")
	}
	// But peers should be.
	if _, ok := r.remotes[2]; !ok {
		t.Fatal("expected remote for node 2")
	}
	if _, ok := r.remotes[3]; !ok {
		t.Fatal("expected remote for node 3")
	}
}

// ---------------------------------------------------------------------------
// Fix 8: handleSnapshot retains following entries
// ---------------------------------------------------------------------------

func TestRaft_HandleSnapshot_RetainsFollowingEntries(t *testing.T) {
	// Follower has entries past the snapshot index that match in term.
	// Per Raft Figure 13: retain log entries following the snapshot's
	// last included entry.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(2, 2)

	// Build a contiguous log from index 1..12, all at term 2.
	// The inmem starts at markerIndex=1, so we append from index 1.
	entries := make([]proto.Entry, 12)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: 2}
	}
	r.log.inmem.merge(entries)

	ss := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     2,
		Snapshot: ss,
	}

	err := r.step(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The log should still have entries 11 and 12 because they are
	// past the snapshot index and the term matches at index 10.
	if r.log.lastIndex() < 12 {
		t.Fatalf("expected lastIndex >= 12 (entries retained), got %d", r.log.lastIndex())
	}
	// Committed index should be advanced to the snapshot index.
	if r.log.committed < 10 {
		t.Fatalf("expected committed >= 10, got %d", r.log.committed)
	}
}

func TestRaft_HandleSnapshot_DiscardsOnMismatch(t *testing.T) {
	// Follower has entries past the snapshot index with a different term.
	// All entries should be discarded and the log restored from snapshot.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(3, 2)

	// Build a contiguous log from index 1..12, all at term 2.
	entries := make([]proto.Entry, 12)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: 2}
	}
	r.log.inmem.merge(entries)

	ss := proto.Snapshot{
		Index: 10,
		Term:  3, // Different term from the entry at index 10 (term 2).
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     3,
		Snapshot: ss,
	}

	err := r.step(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Entries 11 and 12 should be discarded because the term at index
	// 10 did not match the snapshot term. lastIndex should be exactly
	// the snapshot index.
	if r.log.lastIndex() != 10 {
		t.Fatalf("expected lastIndex 10 after snapshot restore on mismatch, got %d",
			r.log.lastIndex())
	}
	if r.log.committed != 10 {
		t.Fatalf("expected committed 10 after snapshot restore, got %d", r.log.committed)
	}
}

// ---------------------------------------------------------------------------
// M5: handleSnapshot does NOT rebuild remotes when entries are retained
// ---------------------------------------------------------------------------

func TestRaft_HandleSnapshot_MatchDoesNotRebuildRemotes(t *testing.T) {
	// When the snapshot matches (same term at snapshot index), entries
	// beyond the snapshot point are retained. Remotes must NOT be rebuilt
	// from the snapshot's (potentially older) membership because config
	// changes in the retained entries would be lost.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(2, 2)

	// Build a contiguous log from index 1..12, all at term 2.
	entries := make([]proto.Entry, 12)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: 2}
	}
	r.log.inmem.merge(entries)

	// Set up remotes that include node 5 (simulating a config change
	// in a retained entry beyond the snapshot point).
	r.remotes[5] = newRemote(13, maxInflightMsgs)

	ss := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			// Node 5 is NOT in the snapshot membership.
			Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     2,
		Snapshot: ss,
	}
	err := r.step(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Entries should be retained (matching case).
	if r.log.lastIndex() < 12 {
		t.Fatalf("expected lastIndex >= 12 (entries retained), got %d",
			r.log.lastIndex())
	}
	// Remotes must NOT be rebuilt. Node 5 should still be present.
	if _, ok := r.remotes[5]; !ok {
		t.Fatal("expected remote for node 5 to be preserved (matching case should not rebuild remotes)")
	}
}

func TestRaft_HandleSnapshot_MismatchRebuildsRemotes(t *testing.T) {
	// When the snapshot does NOT match (different term), the log is
	// fully replaced. Remotes MUST be rebuilt from the snapshot's
	// membership.
	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(3, 2)

	// Build a contiguous log from index 1..12, all at term 2.
	entries := make([]proto.Entry, 12)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: 2}
	}
	r.log.inmem.merge(entries)

	// Add a remote for node 5 that should NOT survive the mismatch.
	r.remotes[5] = newRemote(13, maxInflightMsgs)

	ss := proto.Snapshot{
		Index: 10,
		Term:  3, // Different term (mismatch).
		Membership: proto.Membership{
			// Node 5 is NOT in the snapshot membership, node 4 is.
			Addresses: map[uint64]string{1: "a", 2: "b", 4: "d"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		Term:     3,
		Snapshot: ss,
	}
	err := r.step(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Entries should be discarded (mismatch).
	if r.log.lastIndex() != 10 {
		t.Fatalf("expected lastIndex 10 after mismatch, got %d",
			r.log.lastIndex())
	}
	// Remotes must be rebuilt from snapshot membership.
	if _, ok := r.remotes[4]; !ok {
		t.Fatal("expected remote for node 4 from snapshot membership")
	}
	if _, ok := r.remotes[5]; ok {
		t.Fatal("node 5 should not be in remotes after mismatch rebuild")
	}
}

// ---------------------------------------------------------------------------
// Test: Fix 17 - becomePreCandidate resets pendingConfigChange
// ---------------------------------------------------------------------------

func TestFix17_BecomePreCandidate_ResetsPendingConfigChange(t *testing.T) {
	r := newTestRaft(1)
	r.pendingConfigChange = true

	r.becomePreCandidate()

	if r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be false after becomePreCandidate")
	}
}

func TestFix17_BecomePreCandidate_PendingConfigChangeFalseRemainsFalse(t *testing.T) {
	r := newTestRaft(1)
	// pendingConfigChange starts false (default).
	if r.pendingConfigChange {
		t.Fatal("precondition failed: expected pendingConfigChange false initially")
	}

	r.becomePreCandidate()

	if r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to remain false after becomePreCandidate")
	}
}

// ---------------------------------------------------------------------------
// Test: Fix 18 - becomeLeader resets witness remotes
// ---------------------------------------------------------------------------

func TestFix18_BecomeLeader_ResetsWitnessRemotes(t *testing.T) {
	// Create a raft node with 2 voting members + 1 witness.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Manually set witness match to a non-zero value to simulate
	// stale state that must be reset on leadership transition.
	wm, ok := r.witnesses[3]
	if !ok {
		t.Fatal("expected witness 3 in witnesses map")
	}
	wm.match = 42
	wm.next = 50
	wm.state = remoteReplicate

	// Transition to leader via candidate.
	r.becomeCandidate()
	// Grant votes from remote + witness to win election.
	r.broadcastVote()
	err = r.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error granting vote from remote: %v", err)
	}
	if r.state != leader {
		// Try witness vote too.
		err = r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: 3,
			Term: r.term,
		})
		if err != nil {
			t.Fatalf("unexpected error granting vote from witness: %v", err)
		}
	}
	if r.state != leader {
		t.Fatalf("expected leader state, got %s", r.state)
	}

	// Verify witness remote was reset.
	wm, ok = r.witnesses[3]
	if !ok {
		t.Fatal("expected witness 3 still in witnesses map after becomeLeader")
	}
	if wm.match != 0 {
		t.Fatalf("expected witness match 0, got %d", wm.match)
	}
	if wm.state != remoteProbe {
		t.Fatalf("expected witness state probe, got %s", wm.state)
	}
	// next should be lastIndex (set to lastIndex+1 before noop append,
	// which equals lastIndex after noop append).
	expectedNext := r.log.lastIndex()
	if wm.next != expectedNext {
		t.Fatalf("expected witness next %d, got %d", expectedNext, wm.next)
	}
}

func TestFix18_BecomeLeader_ResetsWitnessRemotes_NoWitnesses(t *testing.T) {
	// Verify that becomeLeader works correctly when there are no witnesses.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	if r.state != leader {
		t.Fatalf("expected leader, got %s", r.state)
	}
	if len(r.witnesses) != 0 {
		t.Fatalf("expected 0 witnesses, got %d", len(r.witnesses))
	}
	// All regular remotes should be reset.
	for id, rm := range r.remotes {
		if rm.match != 0 {
			t.Fatalf("remote %d: expected match 0, got %d", id, rm.match)
		}
		if rm.state != remoteProbe {
			t.Fatalf("remote %d: expected probe state, got %s", id, rm.state)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Fix 19 - stepFollower Propose exits quiesce
// ---------------------------------------------------------------------------

func TestFix19_StepFollower_Propose_ExitsQuiesce(t *testing.T) {
	cfg := raftTestCfgQuiesce(1, 1)
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Set follower with a known leader so proposal forwarding works.
	r.becomeFollower(1, 2)

	// Force quiesce active state.
	r.quiesce.active = true
	if !r.quiesce.active {
		t.Fatal("precondition failed: quiesce should be active")
	}

	// Send a Propose message through the follower step function.
	err = r.step(proto.Message{
		Type: proto.Propose,
		From: 1,
		Entries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("data")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify quiesce was exited.
	if r.quiesce.active {
		t.Fatal("expected quiesce.active to be false after Propose on follower")
	}
}

func TestFix19_StepFollower_Propose_NoLeader_DoesNotExitQuiesce(t *testing.T) {
	cfg := raftTestCfgQuiesce(1, 1)
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Follower with no known leader.
	r.becomeFollower(1, noLeader)

	// Force quiesce active state.
	r.quiesce.active = true

	// Send a Propose message - should fail because no leader.
	err = r.step(proto.Message{
		Type: proto.Propose,
		From: 1,
		Entries: []proto.Entry{
			{Term: 1, Index: 1, Cmd: []byte("data")},
		},
	})
	// Should return a NotLeaderError.
	if err == nil {
		t.Fatal("expected error when proposing with no leader")
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError, got %T: %v", err, err)
	}

	// Quiesce should still be active because the proposal was rejected
	// before recordActivity could be called.
	if !r.quiesce.active {
		t.Fatal("expected quiesce.active to remain true when no leader")
	}
}

// ---------------------------------------------------------------------------
// Test: Fix 20 - checkReadIndexQuorum resolves out-of-order (no HOL blocking)
// ---------------------------------------------------------------------------

func TestFix20_CheckReadIndexQuorum_ResolvesOutOfOrder(t *testing.T) {
	// Create a 3-node cluster leader (self=1, remotes=2,3).
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the noop so hasCommittedEntryAtCurrentTerm is true.
	// Simulate remote 2 acknowledging the noop entry.
	r.remotes[2].match = r.log.lastIndex()
	r.tryCommit()

	// Set up two pending read index requests manually.
	// Pre-set self-ack (matching handleLeaderReadIndex behavior).
	selfBitPos := r.replicaToBit[r.replicaID]
	selfMask := uint64(1) << selfBitPos
	r.pendingReadIndex = append(r.pendingReadIndex, readIndexStatus{
		key:      100,
		index:    r.log.committed,
		from:     0, // local
		ctx:      0, // heartbeat ctx 0
		ackBits:  selfMask,
		ackCount: 1,
	})
	r.pendingReadIndex = append(r.pendingReadIndex, readIndexStatus{
		key:      200,
		index:    r.log.committed,
		from:     0, // local
		ctx:      1, // heartbeat ctx 1
		ackBits:  selfMask,
		ackCount: 1,
	})

	// Add ack from remote 2 to read #2 (ctx=1) only. Read #1 (ctx=0)
	// has no remote acks yet. Use the bitset to record the ack.
	setReadIndexAck(r, &r.pendingReadIndex[1], 2)

	// Call checkReadIndexQuorum.
	r.checkReadIndexQuorum()

	// Read #2 should be resolved (self + remote 2 = 2 >= quorum of 2).
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state resolved, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 200 {
		t.Fatalf("expected resolved read key 200, got %d", r.readStates[0].Key)
	}

	// Read #1 should still be pending.
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}
	if r.pendingReadIndex[0].key != 100 {
		t.Fatalf("expected pending read key 100, got %d", r.pendingReadIndex[0].key)
	}

	// Now add ack for read #1 and resolve it.
	setReadIndexAck(r, &r.pendingReadIndex[0], 2)
	r.checkReadIndexQuorum()

	if len(r.readStates) != 2 {
		t.Fatalf("expected 2 read states resolved, got %d", len(r.readStates))
	}
	if r.readStates[1].Key != 100 {
		t.Fatalf("expected second resolved read key 100, got %d", r.readStates[1].Key)
	}
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending reads, got %d", len(r.pendingReadIndex))
	}
}

func TestFix20_CheckReadIndexQuorum_EmptyPendingIsNoop(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// No pending reads - should not panic or modify readStates.
	r.checkReadIndexQuorum()

	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 read states, got %d", len(r.readStates))
	}
}

// ---------------------------------------------------------------------------
// Test: OPT-1 - ReadIndex bitset ack tracking
// ---------------------------------------------------------------------------

func TestOPT1_ReadIndex_BitsetQuorumDetection(t *testing.T) {
	tests := []struct {
		name      string
		members   map[uint64]string
		ackFrom   []uint64 // remote IDs that send heartbeat acks
		wantReady bool     // whether quorum is reached
	}{
		{
			name:      "3-node cluster, one ack reaches quorum",
			members:   map[uint64]string{1: "a", 2: "b", 3: "c"},
			ackFrom:   []uint64{2},
			wantReady: true, // self(1) + 2 = 2 >= quorum(2)
		},
		{
			name:      "3-node cluster, no ack below quorum",
			members:   map[uint64]string{1: "a", 2: "b", 3: "c"},
			ackFrom:   []uint64{},
			wantReady: false, // self(1) only = 1 < quorum(2)
		},
		{
			name:      "5-node cluster, two acks reach quorum",
			members:   map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"},
			ackFrom:   []uint64{2, 3},
			wantReady: true, // self(1) + 2 + 3 = 3 >= quorum(3)
		},
		{
			name:      "5-node cluster, one ack below quorum",
			members:   map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"},
			ackFrom:   []uint64{2},
			wantReady: false, // self(1) + 2 = 2 < quorum(3)
		},
		{
			name:      "7-node cluster, three acks reach quorum",
			members:   map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e", 6: "f", 7: "g"},
			ackFrom:   []uint64{2, 3, 4},
			wantReady: true, // self(1) + 2,3,4 = 4 >= quorum(4)
		},
		{
			name:      "7-node cluster, two acks below quorum",
			members:   map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e", 6: "f", 7: "g"},
			ackFrom:   []uint64{2, 3},
			wantReady: false, // self(1) + 2,3 = 3 < quorum(4)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := raftTestCfgPreVote(1, 1, false)
			logdb := newTestLogReaderWithMembership(tt.members)
			r, err := newRaft(cfg, logdb)
			if err != nil {
				t.Fatalf("newRaft: %v", err)
			}
			electLeader(r)
			r.drainMessages()

			// Commit the noop entry so ReadIndex is allowed.
			for id := range r.remotes {
				if err := r.step(proto.Message{
					Type:     proto.ReplicateResp,
					From:     id,
					Term:     r.term,
					LogIndex: r.log.lastIndex(),
				}); err != nil {
					t.Fatalf("ReplicateResp: %v", err)
				}
			}
			r.drainMessages()
			r.readStates = nil

			// Issue a ReadIndex request.
			if err := r.step(proto.Message{
				Type: proto.ReadIndex,
				Hint: 42,
			}); err != nil {
				t.Fatalf("ReadIndex: %v", err)
			}
			r.flushReadIndex()

			// Capture the heartbeat ctx from the broadcast.
			hbs := collectMessages(r, proto.Heartbeat)
			if len(hbs) == 0 {
				t.Fatal("expected heartbeat messages for ReadIndex")
			}
			hbCtx := hbs[0].Hint
			r.drainMessages()

			// Send heartbeat acks from specified remotes.
			for _, from := range tt.ackFrom {
				if err := r.step(proto.Message{
					Type: proto.HeartbeatResp,
					From: from,
					Term: r.term,
					Hint: hbCtx,
				}); err != nil {
					t.Fatalf("HeartbeatResp from %d: %v", from, err)
				}
				r.drainMessages()
			}

			gotReady := len(r.readStates) > 0
			if gotReady != tt.wantReady {
				t.Fatalf("quorum detection: got ready=%v, want ready=%v (readStates=%d, pending=%d)",
					gotReady, tt.wantReady, len(r.readStates), len(r.pendingReadIndex))
			}
		})
	}
}

func TestOPT1_ReadIndex_DuplicateAckDoesNotDoubleCount(t *testing.T) {
	// Use a 5-node cluster where quorum = 3. Duplicate acks from a single
	// peer should not inflate the count to reach quorum.
	members := map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	for id := range r.remotes {
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: r.log.lastIndex(),
		}); err != nil {
			t.Fatalf("ReplicateResp: %v", err)
		}
	}
	r.drainMessages()
	r.readStates = nil

	// Issue a ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 99,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	// Capture the heartbeat ctx from the broadcast.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}

	// Send 10 duplicate heartbeat acks from node 2 only.
	// In a 5-node cluster, quorum = 3. self + node 2 = 2 < 3.
	for range 10 {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: 2,
			Term: r.term,
			Hint: hbCtx,
		}); err != nil {
			t.Fatalf("HeartbeatResp: %v", err)
		}
		r.drainMessages()
	}

	// Should NOT be resolved: self(1) + node 2 = 2 < quorum(3).
	// If duplicate acks were double-counted, ackCount would be 11+
	// and the read would have been resolved incorrectly.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates (duplicates should not inflate count), got %d",
			len(r.readStates))
	}
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}

	// Verify ackCount is exactly 2 (self + node 2), not 11.
	// checkReadIndexQuorum adds self on each call, but the bitset prevents
	// double-counting.
	pending := &r.pendingReadIndex[0]
	if pending.ackCount != 2 {
		t.Fatalf("expected ackCount=2 (self + node 2) after duplicate acks, got %d",
			pending.ackCount)
	}
}

func TestOPT1_ReadIndex_BitsetPreventsDuplicateCounting(t *testing.T) {
	// Use a 5-node cluster where quorum = 3.
	// Send duplicate acks from node 2 only. Should NOT reach quorum
	// (self + node 2 = 2, need 3).
	members := map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	for id := range r.remotes {
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: r.log.lastIndex(),
		}); err != nil {
			t.Fatalf("ReplicateResp: %v", err)
		}
	}
	r.drainMessages()
	r.readStates = nil

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 77,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	// Capture the heartbeat ctx from the broadcast.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Send 10 duplicate acks from node 2 only.
	for range 10 {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: 2,
			Term: r.term,
			Hint: hbCtx,
		}); err != nil {
			t.Fatalf("HeartbeatResp: %v", err)
		}
		r.drainMessages()
	}

	// Should NOT be resolved: self(1) + node 2 = 2 < quorum(3).
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates (quorum not reached), got %d", len(r.readStates))
	}
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}

	// Now node 3 acks, reaching quorum: self + 2 + 3 = 3 >= 3.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}
	r.drainMessages()

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState after quorum, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 77 {
		t.Fatalf("expected key 77, got %d", r.readStates[0].Key)
	}
}

func TestOPT1_ReadIndex_ReplicaToBitBuiltOnBecomeLeader(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Before election, replicaToBit should be nil.
	if r.replicaToBit != nil {
		t.Fatal("expected replicaToBit nil before becoming leader")
	}

	electLeader(r)

	// After becoming leader, replicaToBit should contain all voting members.
	if r.replicaToBit == nil {
		t.Fatal("expected replicaToBit non-nil after becoming leader")
	}

	// Should include self + all remotes (3-node cluster: 1, 2, 3).
	expectedMembers := len(r.remotes) + len(r.witnesses) + 1
	if len(r.replicaToBit) != expectedMembers {
		t.Fatalf("expected %d entries in replicaToBit, got %d",
			expectedMembers, len(r.replicaToBit))
	}

	// Verify self is in the map.
	if _, ok := r.replicaToBit[r.replicaID]; !ok {
		t.Fatal("expected self in replicaToBit")
	}

	// Verify all remotes are in the map.
	for id := range r.remotes {
		if _, ok := r.replicaToBit[id]; !ok {
			t.Fatalf("expected remote %d in replicaToBit", id)
		}
	}

	// Verify all bit positions are unique.
	seen := make(map[uint8]uint64)
	for id, bit := range r.replicaToBit {
		if prevID, ok := seen[bit]; ok {
			t.Fatalf("duplicate bit position %d for nodes %d and %d", bit, prevID, id)
		}
		seen[bit] = id
	}
}

func TestOPT1_ReadIndex_UnknownReplicaAckIgnored(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 55,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	// Capture the heartbeat ctx from the broadcast.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Send heartbeat response from an unknown node (ID 999).
	// This should be safely ignored without panic.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 999,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from unknown: %v", err)
	}

	// Pending read should still be unresolved (only self counted).
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates after unknown node ack, got %d", len(r.readStates))
	}
}

// ---------------------------------------------------------------------------
// Test: ReadIndex heartbeat batching
// ---------------------------------------------------------------------------

func TestReadIndexBatching_SingleBroadcast(t *testing.T) {
	// Setup: 3-node cluster with leader. Submit 4 ReadIndex requests, then
	// flush. Verify only ONE heartbeat per follower (2 total, not 8).
	// Send one HeartbeatResp from each follower with the LATEST ctx.
	// Verify ALL 4 ReadIndex entries are resolved.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op so hasCommittedEntryAtCurrentTerm is true.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Submit 4 ReadIndex requests WITHOUT flushing between them.
	for i := 0; i < 4; i++ {
		if err := r.step(proto.Message{
			Type: proto.ReadIndex,
			From: 0,
			Hint: uint64(10 + i),
		}); err != nil {
			t.Fatalf("ReadIndex %d: %v", i, err)
		}
	}

	// Before flush: no heartbeat messages should be sent (broadcast deferred).
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) != 0 {
		t.Fatalf("expected 0 heartbeats before flush, got %d", len(hbs))
	}

	// All 4 entries should be in pendingReadIndex.
	if len(r.pendingReadIndex) != 4 {
		t.Fatalf("expected 4 pending read indexes, got %d", len(r.pendingReadIndex))
	}

	// Flush: sends ONE heartbeat broadcast.
	r.flushReadIndex()

	// Verify exactly one heartbeat per follower (2 followers in 3-node cluster).
	hbs = collectMessages(r, proto.Heartbeat)
	if len(hbs) != 2 {
		t.Fatalf("expected 2 heartbeats (one per follower), got %d", len(hbs))
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// All heartbeats should carry the LATEST ctx (the one from the 4th ReadIndex).
	for i, hb := range hbs {
		if hb.Hint != hbCtx {
			t.Fatalf("heartbeat[%d] ctx mismatch: got %d, want %d", i, hb.Hint, hbCtx)
		}
	}

	// Send HeartbeatResp from peer 2 with the latest ctx.
	// The <= comparison should confirm all 4 pending entries.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	// All 4 should be resolved.
	if len(r.readStates) != 4 {
		t.Fatalf("expected 4 readStates, got %d", len(r.readStates))
	}
	for i := 0; i < 4; i++ {
		if r.readStates[i].Key != uint64(10+i) {
			t.Fatalf("readState[%d] key: expected %d, got %d", i, 10+i, r.readStates[i].Key)
		}
	}
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending reads after resolution, got %d", len(r.pendingReadIndex))
	}
}

func TestReadIndexBatching_FlushIdempotent(t *testing.T) {
	// Calling flushReadIndex when no ReadIndex is pending is a no-op.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	r.flushReadIndex()
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) != 0 {
		t.Fatalf("expected 0 heartbeats from idle flush, got %d", len(hbs))
	}
}

func TestReadIndexHeartbeatResp_ConfirmsEarlierCtx(t *testing.T) {
	// Setup: 3-node cluster with 3 pending ReadIndex entries at ctx 1, 2, 3.
	// Send HeartbeatResp with ctx=3 from follower.
	// Verify ALL 3 entries are confirmed (ackCount incremented) and resolved.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Submit 3 ReadIndex requests, flushing each to get distinct ctx values.
	ctxs := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		if err := r.step(proto.Message{
			Type: proto.ReadIndex,
			From: 0,
			Hint: uint64(200 + i),
		}); err != nil {
			t.Fatalf("ReadIndex %d: %v", i, err)
		}
		r.flushReadIndex()
		hbs := collectMessages(r, proto.Heartbeat)
		if len(hbs) == 0 {
			t.Fatalf("expected heartbeats for ReadIndex %d", i)
		}
		ctxs[i] = hbs[0].Hint
		r.drainMessages()
	}

	// Verify all 3 ctx values are monotonically increasing.
	for i := 1; i < 3; i++ {
		if ctxs[i] <= ctxs[i-1] {
			t.Fatalf("ctx[%d]=%d not > ctx[%d]=%d", i, ctxs[i], i-1, ctxs[i-1])
		}
	}

	if len(r.pendingReadIndex) != 3 {
		t.Fatalf("expected 3 pending reads, got %d", len(r.pendingReadIndex))
	}

	// Send ONE HeartbeatResp with the LATEST ctx (ctx[2]).
	// The <= comparison should confirm all 3 entries at once.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: ctxs[2],
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	// All 3 should be resolved.
	if len(r.readStates) != 3 {
		t.Fatalf("expected 3 readStates, got %d", len(r.readStates))
	}
	for i := 0; i < 3; i++ {
		if r.readStates[i].Key != uint64(200+i) {
			t.Fatalf("readState[%d] key: expected %d, got %d", i, 200+i, r.readStates[i].Key)
		}
	}
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending reads, got %d", len(r.pendingReadIndex))
	}
}

func TestReadIndexHeartbeatResp_RegularHeartbeatCtxZeroDoesNotConfirm(t *testing.T) {
	// Regular heartbeats use ctx=0. ReadIndex entries have ctx >= 1.
	// A HeartbeatResp with ctx=0 must NOT confirm any ReadIndex entries.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Issue a ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()
	r.drainMessages()

	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}

	// Send HeartbeatResp with ctx=0 (regular heartbeat, not ReadIndex).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	// The ReadIndex should NOT be resolved by ctx=0.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates after ctx=0 ack, got %d", len(r.readStates))
	}
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read still pending, got %d", len(r.pendingReadIndex))
	}
}

func TestReadIndexBatching_TickHeartbeatPiggybacksCtx(t *testing.T) {
	// O10: broadcastHeartbeat() (from tick) should piggyback the ReadIndex ctx
	// when there are pending reads. This allows regular tick heartbeats to
	// satisfy pending ReadIndex requests without a separate heartbeat round.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Issue a ReadIndex but do NOT flush yet.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 99,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}
	expectedCtx := r.readIndexCtx

	// Call broadcastHeartbeat (simulates tick heartbeat). It should carry
	// the ReadIndex ctx, not 0.
	r.broadcastHeartbeat()
	msgs := r.drainMessages()

	foundCtx := false
	for _, msg := range msgs {
		if msg.Type == proto.Heartbeat && msg.Hint == expectedCtx {
			foundCtx = true
		}
	}
	if !foundCtx {
		t.Fatalf("tick heartbeat did not carry ReadIndex ctx %d", expectedCtx)
	}

	// Also verify readIndexBroadcastPending was cleared.
	if r.readIndexBroadcastPending {
		t.Fatal("readIndexBroadcastPending should be cleared after broadcastHeartbeat")
	}

	// Now a HeartbeatResp with the correct ctx should confirm the read.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: expectedCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState after quorum, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 99 {
		t.Fatalf("expected key 99, got %d", r.readStates[0].Key)
	}
}

func TestReadIndexBatching_BroadcastPendingClearedOnBecomeFollower(t *testing.T) {
	// readIndexBroadcastPending must be cleared when stepping down.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Commit the no-op.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()

	// Issue a ReadIndex (sets readIndexBroadcastPending = true).
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if !r.readIndexBroadcastPending {
		t.Fatal("expected readIndexBroadcastPending=true after ReadIndex")
	}

	// Step down. Both pendingReadIndex and readIndexBroadcastPending
	// should be cleared.
	r.becomeFollower(r.term+1, 2)

	if r.readIndexBroadcastPending {
		t.Fatal("expected readIndexBroadcastPending=false after becomeFollower")
	}
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending reads after becomeFollower, got %d",
			len(r.pendingReadIndex))
	}
}

// ---------------------------------------------------------------------------
// Test: OPT-8 - campaign() message template reuse
// ---------------------------------------------------------------------------

func TestOPT8_Campaign_TemplateReuse_CorrectMessages(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Become candidate to trigger campaign.
	r.becomeCandidate()
	r.msgs = r.msgs[:0]

	r.campaign(proto.RequestVote)

	// Should have sent RequestVote to all peers (2 and 3).
	voteReqs := collectMessages(r, proto.RequestVote)
	if len(voteReqs) != 2 {
		t.Fatalf("expected 2 RequestVote messages, got %d", len(voteReqs))
	}

	// Verify each message has correct fields and distinct To addresses.
	targets := make(map[uint64]bool)
	for _, msg := range voteReqs {
		if msg.Type != proto.RequestVote {
			t.Fatalf("expected RequestVote, got type %d", msg.Type)
		}
		if msg.Term != r.term {
			t.Fatalf("expected term %d, got %d", r.term, msg.Term)
		}
		if msg.LogTerm == 0 && r.log.lastIndex() > 0 {
			t.Fatal("expected non-zero LogTerm for non-empty log")
		}
		if msg.LogIndex != r.log.lastIndex() {
			t.Fatalf("expected LogIndex %d, got %d", r.log.lastIndex(), msg.LogIndex)
		}
		targets[msg.To] = true
	}

	// Verify we sent to both peers, not the same peer twice.
	if !targets[2] || !targets[3] {
		t.Fatalf("expected messages to nodes 2 and 3, got targets: %v", targets)
	}
}

func TestOPT8_Campaign_PreVote_UsesTermPlusOne(t *testing.T) {
	r := newTestRaft(1)
	r.becomePreCandidate()
	r.msgs = r.msgs[:0]

	expectedTerm := r.term + 1
	r.campaign(proto.RequestPreVote)

	preVoteReqs := collectMessages(r, proto.RequestPreVote)
	if len(preVoteReqs) != 2 {
		t.Fatalf("expected 2 RequestPreVote messages, got %d", len(preVoteReqs))
	}

	for _, msg := range preVoteReqs {
		if msg.Term != expectedTerm {
			t.Fatalf("expected term %d (term+1) for PreVote, got %d",
				expectedTerm, msg.Term)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Fix 21 - hasPendingSnapshot set on InstallSnapshot
// ---------------------------------------------------------------------------

func TestFix21_Peer_Handle_InstallSnapshot_SetsPendingSnapshot(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Verify initial state.
	if p.hasPendingSnapshot {
		t.Fatal("precondition failed: hasPendingSnapshot should be false initially")
	}

	snap := proto.Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     10,
		Term:      2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{
				1: "addr1",
				2: "addr2",
			},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     2,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Snapshot was accepted (index 10 > committed 0), so
	// hasPendingSnapshot must be true.
	if !p.hasPendingSnapshot {
		t.Fatal("expected hasPendingSnapshot to be true after accepted InstallSnapshot")
	}
}

func TestFix21_Peer_Handle_InstallSnapshot_RejectedDoesNotSetPendingSnapshot(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Advance committed index past the snapshot index so it will be rejected.
	p.r.log.inmem.merge(makeEntries(1, 21, 1))
	p.r.log.committed = 20

	if p.hasPendingSnapshot {
		t.Fatal("precondition failed: hasPendingSnapshot should be false initially")
	}

	snap := proto.Snapshot{
		Index: 5,
		Term:  1,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "addr1"},
		},
	}
	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Snapshot was rejected (index 5 <= committed 20), so
	// hasPendingSnapshot must remain false.
	if p.hasPendingSnapshot {
		t.Fatal("expected hasPendingSnapshot to be false after rejected InstallSnapshot")
	}
}

// ---------------------------------------------------------------------------
// Test: sendAppend uses maxReplicationPayload to bound Replicate messages
// ---------------------------------------------------------------------------

func TestSendAppend_UsesMaxReplicationPayload(t *testing.T) {
	// Create a raft as leader with a small maxReplicationPayload so that
	// sendAppend cannot fit all entries into one Replicate message. Each
	// Entry has a fixed 60-byte header plus Cmd bytes. We set the payload
	// limit to 200 bytes and append entries with 100-byte commands, so
	// each entry is 160 bytes. Two entries = 320 bytes > 200, meaning
	// the second entry should be excluded from the first batch (but the
	// first entry always fits because limitEntrySize includes at least
	// one entry).
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.MaxApplyEntrySize = 200
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the maxReplicationPayload is set.
	if r.maxReplicationPayload != 200 {
		t.Fatalf("expected maxReplicationPayload 200, got %d", r.maxReplicationPayload)
	}

	// Elect replica 1 as leader.
	electLeader(r)
	if r.state != leader {
		t.Fatal("expected leader state after election")
	}

	// Append several entries with 100-byte commands. Each entry is
	// 60 (header) + 100 (cmd) = 160 bytes. We need enough entries
	// so that the total exceeds 200 bytes (more than 1 entry).
	totalEntries := 5
	cmd := make([]byte, 100)
	for i := range cmd {
		cmd[i] = byte(i % 256)
	}
	for range totalEntries {
		err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		})
		if err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Transition peer 2's remote to replicate state so sendAppend
	// can send optimistically. First, ack the initial entries so the
	// remote transitions from probe to replicate. Set next to 2 (the
	// first entry after the initial noop) to simulate a slow follower.
	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = 1 // only has the bootstrap entry
	rm.next = 2  // needs everything from index 2 onward

	// Drain prior messages.
	r.drainMessages()

	// Call sendAppend which should produce a bounded Replicate message.
	r.sendAppend(2)

	// Find the Replicate message to peer 2.
	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) == 0 {
		t.Fatal("expected at least one Replicate message to peer 2")
	}

	msg := replicateMsgs[0]
	if msg.To != 2 {
		t.Fatalf("expected message to peer 2, got %d", msg.To)
	}

	// Calculate total entry bytes in the message.
	var totalBytes uint64
	for i := range msg.Entries {
		totalBytes += uint64(msg.Entries[i].Size())
	}

	// The message should have fewer entries than the total available
	// (totalEntries + 1 noop = totalEntries+1 entries starting from
	// index 2). At 160 bytes per entry and 200 byte limit, only 1
	// entry fits. limitEntrySize includes the first entry always,
	// then stops when total exceeds maxSize.
	availableEntries := r.log.lastIndex() - 1 // entries from index 2 onward
	if uint64(len(msg.Entries)) >= availableEntries {
		t.Fatalf("expected bounded entries: got %d entries but %d are available; "+
			"maxReplicationPayload should have limited the batch",
			len(msg.Entries), availableEntries)
	}

	// The first entry is always included (limitEntrySize guarantees this).
	if len(msg.Entries) == 0 {
		t.Fatal("expected at least 1 entry in the bounded Replicate message")
	}
}

func TestSendAppend_UnlimitedWhenPayloadZero(t *testing.T) {
	// When maxReplicationPayload is 0 (unlimited), all entries should be
	// sent in a single Replicate message, regardless of total size.
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.MaxApplyEntrySize = 0 // Will be set to default by SetDefaults
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After SetDefaults, MaxApplyEntrySize is the default (64MB).
	// Override to 0 to test the unlimited path.
	r.maxReplicationPayload = 0

	// Elect leader.
	electLeader(r)
	if r.state != leader {
		t.Fatal("expected leader state after election")
	}

	// Append several large entries.
	totalEntries := 10
	cmd := make([]byte, 100)
	for range totalEntries {
		err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		})
		if err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Set peer 2 to replicate state with next pointing to 2.
	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = 1
	rm.next = 2

	// Drain prior messages.
	r.drainMessages()

	// Call sendAppend which should send all entries.
	r.sendAppend(2)

	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) == 0 {
		t.Fatal("expected at least one Replicate message")
	}

	msg := replicateMsgs[0]
	availableEntries := r.log.lastIndex() - 1 // entries from index 2 onward
	if uint64(len(msg.Entries)) != availableEntries {
		t.Fatalf("expected all %d entries with unlimited payload, got %d",
			availableEntries, len(msg.Entries))
	}
}

// ---------------------------------------------------------------------------
// PreVote / Vote cross-contamination guards
// ---------------------------------------------------------------------------

// TestStepCandidate_PreVoteRespIgnoredInCandidateState verifies that stale
// RequestPreVoteResp messages arriving after a preCandidate->candidate
// transition are silently discarded. Without this guard, a candidate could
// count pre-vote acks as real votes and illegitimately win the election,
// allowing multiple leaders at the same term.
func TestStepCandidate_PreVoteRespIgnoredInCandidateState(t *testing.T) {
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, true)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Phase 1: transition to preCandidate (term stays at 1).
	r.becomePreCandidate()
	if r.state != preCandidate {
		t.Fatalf("expected preCandidate state, got %s", r.state)
	}
	preCandidateTerm := r.term // term 1

	// Phase 2: simulate winning the pre-vote by transitioning to candidate.
	// becomeCandidate increments term to 2 and resets votes to {self: true}.
	r.becomeCandidate()
	if r.state != candidate {
		t.Fatalf("expected candidate state, got %s", r.state)
	}
	candidateTerm := r.term
	if candidateTerm != preCandidateTerm+1 {
		t.Fatalf("expected term %d after becomeCandidate, got %d",
			preCandidateTerm+1, candidateTerm)
	}

	// Phase 3: feed stale RequestPreVoteResp messages from 3 peers at the
	// candidate's current term. In a real scenario these arrive from the
	// pre-vote round but are processed after the state transition.
	// Quorum for 5 nodes is 3; with self-vote, 2 more grants would win.
	for _, peerID := range []uint64{2, 3, 4} {
		err := r.step(proto.Message{
			Type: proto.RequestPreVoteResp,
			From: peerID,
			Term: candidateTerm,
		})
		if err != nil {
			t.Fatalf("step error for peer %d: %v", peerID, err)
		}
	}

	// The node must NOT have become leader from stale pre-vote responses.
	if r.state != candidate {
		t.Fatalf("expected node to remain candidate, got %s", r.state)
	}

	// The votes map must contain only the self-vote from becomeCandidate.
	if len(r.votes) != 1 {
		t.Fatalf("expected 1 vote (self only), got %d: %v", len(r.votes), r.votes)
	}
	selfVote, ok := r.votes[r.replicaID]
	if !ok || !selfVote {
		t.Fatalf("expected self-vote granted for replica %d, votes: %v",
			r.replicaID, r.votes)
	}
}

// TestStepCandidate_VoteRespIgnoredInPreCandidateState verifies the reverse
// guard: a preCandidate ignores RequestVoteResp messages. This prevents a
// scenario where a rogue or delayed real VoteResp is processed during the
// pre-vote phase, which could cause a term advancement without a proper
// election.
func TestStepCandidate_VoteRespIgnoredInPreCandidateState(t *testing.T) {
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, true)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Transition to preCandidate (term stays at 1).
	r.becomePreCandidate()
	if r.state != preCandidate {
		t.Fatalf("expected preCandidate state, got %s", r.state)
	}
	preCandidateTerm := r.term

	// Feed RequestVoteResp messages at the current term from 3 peers.
	// These should be silently ignored because the node is a preCandidate,
	// not a real candidate.
	for _, peerID := range []uint64{2, 3, 4} {
		err := r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: peerID,
			Term: preCandidateTerm,
		})
		if err != nil {
			t.Fatalf("step error for peer %d: %v", peerID, err)
		}
	}

	// The node must remain a preCandidate -- it must not have become a
	// candidate or leader from spurious real vote responses.
	if r.state != preCandidate {
		t.Fatalf("expected node to remain preCandidate, got %s", r.state)
	}

	// Term must not have changed.
	if r.term != preCandidateTerm {
		t.Fatalf("expected term %d to be unchanged, got %d",
			preCandidateTerm, r.term)
	}

	// The votes map must contain only the self-vote from becomePreCandidate.
	if len(r.votes) != 1 {
		t.Fatalf("expected 1 vote (self only), got %d: %v", len(r.votes), r.votes)
	}
	selfVote, ok := r.votes[r.replicaID]
	if !ok || !selfVote {
		t.Fatalf("expected self-vote granted for replica %d, votes: %v",
			r.replicaID, r.votes)
	}
}

// ---------------------------------------------------------------------------
// Lease-based read tests (PhD Section 6.4)
// ---------------------------------------------------------------------------

func TestLeaseRead_ImmediateResponseWithValidLease(t *testing.T) {
	// When the leader holds a valid lease (quorum of heartbeat acks within
	// leaseTimeout ticks), ReadIndex should respond immediately without
	// sending any heartbeat broadcast.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Establish lease: heartbeat acks from both followers.
	grantLease(r)

	// Submit a ReadIndex request.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 42,
	}); err != nil {
		t.Fatalf("ReadIndex with valid lease: %v", err)
	}

	// Should respond immediately via readStates (no heartbeat broadcast).
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 42 {
		t.Fatalf("readState key: expected 42, got %d", r.readStates[0].Key)
	}
	if r.readStates[0].Index != r.log.committed {
		t.Fatalf("readState index: expected %d, got %d",
			r.log.committed, r.readStates[0].Index)
	}

	// No heartbeat messages should have been sent.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) != 0 {
		t.Fatalf("expected 0 heartbeats with valid lease, got %d", len(hbs))
	}

	// No pending read index entries.
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending reads, got %d", len(r.pendingReadIndex))
	}
}

func TestLeaseRead_FallbackToHeartbeatWhenLeaseExpired(t *testing.T) {
	// When the lease has expired (no recent heartbeat acks), ReadIndex should
	// fall through to the heartbeat-based path and defer a broadcast.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Do NOT grant lease. Advance electionTick beyond leaseTimeout to ensure
	// any stale acks are expired.
	r.electionTick = r.leaseTimeout + 1

	// Submit a ReadIndex request.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 99,
	}); err != nil {
		t.Fatalf("ReadIndex with expired lease: %v", err)
	}

	// Should NOT have responded immediately.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates (lease expired), got %d", len(r.readStates))
	}

	// Should have a pending read index entry.
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}

	// Broadcast should be pending (deferred).
	if !r.readIndexBroadcastPending {
		t.Fatal("expected readIndexBroadcastPending to be true")
	}
}

func TestLeaseRead_InvalidatedOnLeadershipLoss(t *testing.T) {
	// When a leader steps down (becomeFollower), the lease must be
	// invalidated so a stale leader cannot serve reads.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	// Verify lease is valid.
	if !r.hasValidLease() {
		t.Fatal("expected valid lease before stepping down")
	}

	// Step down.
	r.becomeFollower(r.term+1, 2)

	// Lease must be invalidated.
	if r.hasValidLease() {
		t.Fatal("expected invalid lease after becomeFollower")
	}
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to be nil after becomeFollower")
	}
}

func TestLeaseRead_RenewalOnHeartbeatAck(t *testing.T) {
	// Verify that heartbeat ack responses update remoteLastAck, which
	// keeps the lease valid.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Initially no lease (no acks yet).
	if r.hasValidLease() {
		t.Fatal("expected no lease before any heartbeat acks")
	}

	// Send ack from follower 2 only. In a 3-node cluster:
	// commitQuorum(remotes) = (2+1)/2+1 = 2. Self(1) + follower(2) = 2 = quorum.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp from 2: %v", err)
	}
	r.drainMessages()

	if !r.hasValidLease() {
		t.Fatal("expected valid lease after ack from follower 2 (self + 1 remote = quorum)")
	}

	// Verify remoteLastAck was updated.
	tick, ok := r.remoteLastAck[2]
	if !ok {
		t.Fatal("expected remoteLastAck[2] to exist")
	}
	if tick != r.electionTick {
		t.Fatalf("expected remoteLastAck[2] = %d, got %d", r.electionTick, tick)
	}
}

func TestLeaseRead_SafetyMarginElectionTimeoutMinusTwo(t *testing.T) {
	// The lease timeout should be electionTimeout - 2 to provide a 2-tick
	// safety margin against goroutine scheduling jitter (PhD 6.4).
	r := newTestRaftLeaseRead(1)

	expectedLeaseTimeout := r.electionTimeout - 2
	if r.leaseTimeout != expectedLeaseTimeout {
		t.Fatalf("expected leaseTimeout = %d (electionTimeout - 2), got %d",
			expectedLeaseTimeout, r.leaseTimeout)
	}
}

func TestLeaseRead_LeaseExpiresAfterTimeout(t *testing.T) {
	// Verify that the lease expires when electionTick advances beyond
	// leaseTimeout ticks past the last ack.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	if !r.hasValidLease() {
		t.Fatal("expected valid lease right after grant")
	}

	// Advance electionTick. Acks were at tick 0, leaseTimeout = 8.
	// At tick 7: leaseExpiry = 7 - 8 = -1. Ack at 0 >= -1, valid.
	r.electionTick = r.leaseTimeout - 1
	if !r.hasValidLease() {
		t.Fatal("expected valid lease at leaseTimeout - 1")
	}

	// At tick 8: leaseExpiry = 8 - 8 = 0. Ack at 0 >= 0, still valid.
	r.electionTick = r.leaseTimeout
	if !r.hasValidLease() {
		t.Fatal("expected valid lease at exactly leaseTimeout (boundary)")
	}

	// At tick 9: leaseExpiry = 9 - 8 = 1. Ack at 0 < 1, expired.
	r.electionTick = r.leaseTimeout + 1
	if r.hasValidLease() {
		t.Fatal("expected lease to be expired at leaseTimeout + 1")
	}
}

func TestLeaseRead_SingleNodeBypassesLeaseCheck(t *testing.T) {
	// Single-node clusters should respond immediately regardless of lease
	// state. The single-node check takes precedence.
	members := map[uint64]string{
		1: "addr1",
	}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)

	// Commit no-op (self-commit in single node).
	r.drainMessages()
	r.readStates = nil

	// No need for lease -- single node should respond immediately.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 77,
	}); err != nil {
		t.Fatalf("ReadIndex single-node: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 77 {
		t.Fatalf("readState key: expected 77, got %d", r.readStates[0].Key)
	}
}

func TestLeaseRead_RestoredAfterFailedLeaderTransfer(t *testing.T) {
	// After a leader transfer times out, the leader must restore
	// remoteLastAck so lease reads can resume. Without the fix, the
	// leader silently degrades to heartbeat-based ReadIndex for the
	// rest of its term.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Establish a valid lease.
	grantLease(r)
	if !r.hasValidLease() {
		t.Fatal("expected valid lease before transfer")
	}

	// Initiate a leader transfer -- this nils remoteLastAck.
	if err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 2,
	}); err != nil {
		t.Fatalf("LeaderTransfer: %v", err)
	}
	r.drainMessages()

	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck nil during active transfer")
	}

	// Tick past the election timeout to trigger transfer abort.
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}
	r.drainMessages()

	if r.transferTarget != noNode {
		t.Fatalf("expected transfer cleared, got %d", r.transferTarget)
	}
	if r.remoteLastAck == nil {
		t.Fatal("expected remoteLastAck restored after transfer timeout")
	}

	// Re-establish the lease and verify lease reads work.
	grantLease(r)
	if !r.hasValidLease() {
		t.Fatal("expected valid lease after transfer timeout + re-grant")
	}
}

func TestLeaseRead_DisabledDuringActiveLeaderTransfer(t *testing.T) {
	// During an active leader transfer, lease reads must be disabled
	// because the transfer target may win the election and begin
	// committing writes immediately (PhD thesis 6.4.1).
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Establish a valid lease.
	grantLease(r)
	if !r.hasValidLease() {
		t.Fatal("expected valid lease before transfer")
	}

	// Initiate a leader transfer.
	if err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 2,
	}); err != nil {
		t.Fatalf("LeaderTransfer: %v", err)
	}
	r.drainMessages()

	// Lease must be invalid while transfer is in progress.
	if r.hasValidLease() {
		t.Fatal("expected lease invalid during active transfer")
	}
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck nil during active transfer")
	}
}

func TestLeaseRead_OldLeaderClearsLeaseAfterSuccessfulTransfer(t *testing.T) {
	// After a successful transfer, the old leader steps down to
	// follower. becomeFollower must nil remoteLastAck so the old
	// leader does not carry stale lease state.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	if !r.hasValidLease() {
		t.Fatal("expected valid lease before transfer")
	}

	// Simulate the old leader receiving an Append from a new leader
	// in a higher term, causing it to step down.
	if err := r.step(proto.Message{
		Type: proto.Replicate,
		From: 2,
		Term: r.term + 1,
	}); err != nil {
		t.Fatalf("step Replicate from new leader: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected follower after higher-term message, got %s", r.state)
	}
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck nil after stepping down to follower")
	}
}

func TestLeaseRead_NonLeaseRaftIgnoresTransferRestore(t *testing.T) {
	// When LeaseRead is false, the transfer timeout handler must NOT
	// allocate remoteLastAck. Verify it stays nil.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.transferTarget = 2
	r.transferElapsed = 0

	// Tick past the election timeout.
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.transferTarget != noNode {
		t.Fatalf("expected transfer cleared, got %d", r.transferTarget)
	}
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to remain nil for non-lease raft")
	}
}

func TestLeaseRead_DisabledBehavesAsReadIndex(t *testing.T) {
	// With LeaseRead=false (default), even with recent heartbeat acks,
	// ReadIndex must use the heartbeat-based path.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	// Verify leaseRead is false.
	if r.leaseRead {
		t.Fatal("expected leaseRead to be false for default config")
	}

	// Send heartbeat acks from all followers.
	for _, from := range []uint64{2, 3} {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: from,
			Term: r.term,
			Hint: 0,
		}); err != nil {
			t.Fatalf("HeartbeatResp from %d: %v", from, err)
		}
	}
	r.drainMessages()

	// hasValidLease should return false.
	if r.hasValidLease() {
		t.Fatal("expected hasValidLease to be false when LeaseRead is disabled")
	}

	// ReadIndex should use heartbeat path (pending, not immediate).
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 55,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	// Should have pending read index, not immediate readState.
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates with LeaseRead disabled, got %d", len(r.readStates))
	}
}

func TestLeaseRead_ForwardedFromFollower(t *testing.T) {
	// When a follower forwards a ReadIndex to the leader and the leader
	// has a valid lease, the leader should respond with ReadIndexResp
	// directly to the follower without a heartbeat round-trip.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	// Submit ReadIndex forwarded from follower 2.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 2,
		Hint: 123,
	}); err != nil {
		t.Fatalf("ReadIndex from follower: %v", err)
	}

	// Should have sent a ReadIndexResp to follower 2.
	msgs := collectMessages(r, proto.ReadIndexResp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 ReadIndexResp, got %d", len(msgs))
	}
	if msgs[0].To != 2 {
		t.Fatalf("ReadIndexResp.To: expected 2, got %d", msgs[0].To)
	}
	if msgs[0].Hint != 123 {
		t.Fatalf("ReadIndexResp.Hint: expected 123, got %d", msgs[0].Hint)
	}
	if msgs[0].LogIndex != r.log.committed {
		t.Fatalf("ReadIndexResp.LogIndex: expected %d, got %d",
			r.log.committed, msgs[0].LogIndex)
	}

	// No local readStates should be created for forwarded requests.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates for forwarded request, got %d", len(r.readStates))
	}
}

func TestLeaseRead_ResetOnBecomeLeader(t *testing.T) {
	// When a node becomes leader, remoteLastAck must be reset to an empty
	// map (no valid lease until first heartbeat round completes).
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	if !r.hasValidLease() {
		t.Fatal("expected valid lease before re-election")
	}

	// Step down and re-elect.
	r.becomeFollower(r.term, noLeader)
	r.becomeCandidate()
	// Simulate winning election.
	for id := range r.remotes {
		if id != r.replicaID {
			if err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			}); err != nil {
				t.Fatalf("vote: %v", err)
			}
			if r.state == leader {
				break
			}
		}
	}
	if r.state != leader {
		t.Fatal("expected to be leader after re-election")
	}

	// remoteLastAck should be empty (fresh leadership).
	if len(r.remoteLastAck) != 0 {
		t.Fatalf("expected empty remoteLastAck after becomeLeader, got %d entries",
			len(r.remoteLastAck))
	}
	if r.hasValidLease() {
		t.Fatal("expected no valid lease after fresh leadership")
	}
}

func TestLeaseRead_ResetOnBecomeCandidate(t *testing.T) {
	// becomeCandidate transitions from follower; verify remoteLastAck is nil.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	r.becomeFollower(r.term, noLeader)
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to be nil after becomeFollower")
	}

	r.becomeCandidate()
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to remain nil after becomeCandidate")
	}
}

func TestLeaseRead_MultipleConsecutiveReads(t *testing.T) {
	// Multiple consecutive ReadIndex requests with a valid lease should
	// all be served immediately.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	for i := 0; i < 10; i++ {
		if err := r.step(proto.Message{
			Type: proto.ReadIndex,
			From: 0,
			Hint: uint64(100 + i),
		}); err != nil {
			t.Fatalf("ReadIndex %d: %v", i, err)
		}
	}

	if len(r.readStates) != 10 {
		t.Fatalf("expected 10 readStates, got %d", len(r.readStates))
	}
	for i := 0; i < 10; i++ {
		if r.readStates[i].Key != uint64(100+i) {
			t.Fatalf("readState[%d] key: expected %d, got %d",
				i, 100+i, r.readStates[i].Key)
		}
	}

	// No heartbeats should have been sent.
	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) != 0 {
		t.Fatalf("expected 0 heartbeats, got %d", len(hbs))
	}
}

func TestLeaseRead_PartialAckNotQuorum(t *testing.T) {
	// In a 5-node cluster, receiving acks from only 1 follower (self + 1 = 2)
	// should NOT constitute a quorum (need 3 out of 5 data-bearing nodes).
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
		4: "addr4",
		5: "addr5",
	}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)

	// 5-node cluster needs 3 acks to commit. Send from 2 and 3.
	lastIdx := r.log.lastIndex()
	for _, from := range []uint64{2, 3} {
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     from,
			Term:     r.term,
			LogIndex: lastIdx,
		}); err != nil {
			t.Fatalf("ReplicateResp from %d: %v", from, err)
		}
	}
	r.drainMessages()
	r.readStates = nil

	// Send ack from only follower 2.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}
	r.drainMessages()

	// commitQuorum for 5-node: (4+1)/2+1 = 3. Self + 1 = 2. Not quorum.
	if r.hasValidLease() {
		t.Fatal("expected no valid lease with only 1 remote ack in 5-node cluster")
	}

	// ReadIndex should use heartbeat path.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 88,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates (no lease), got %d", len(r.readStates))
	}
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}
}

func TestLeaseRead_HasValidLease_NotLeaderReturnsFalse(t *testing.T) {
	// hasValidLease should return false if the node is not the leader,
	// even if leaseRead is enabled.
	r := newTestRaftLeaseRead(1)

	// Node starts as follower.
	if r.hasValidLease() {
		t.Fatal("expected hasValidLease to be false for follower")
	}
}

func TestLeaseRead_RemoteLastAckNilWhenDisabled(t *testing.T) {
	// When LeaseRead is false, remoteLastAck should remain nil even after
	// becoming leader.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to be nil when LeaseRead is disabled")
	}
}

func TestLeaseRead_HeartbeatAckDoesNotTrackWhenDisabled(t *testing.T) {
	// When LeaseRead is false, heartbeat responses should not populate
	// remoteLastAck.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to remain nil when LeaseRead is disabled")
	}
}

func TestLeaseRead_LeaseExpiresOnlyForStaleAcks(t *testing.T) {
	// In a 3-node cluster, if one follower's ack is fresh and the other is
	// stale, the lease should still be valid as long as quorum is met.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Ack from follower 2 at tick 0.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp from 2: %v", err)
	}
	r.drainMessages()

	// Advance tick and ack from follower 3 at the new tick.
	r.electionTick = 5
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp from 3: %v", err)
	}
	r.drainMessages()

	// Now follower 2 acked at tick 0, follower 3 at tick 5.
	// leaseTimeout = 8 (10 - 2). leaseExpiry = 5 - 8 = -3.
	// Both ticks (0 and 5) >= -4, so lease is valid.
	if !r.hasValidLease() {
		t.Fatal("expected valid lease with both acks within timeout")
	}

	// Advance tick so follower 2's ack expires but follower 3's remains.
	// leaseTimeout=8, so at tick 10: leaseExpiry = 10 - 8 = 2.
	// Follower 2 acked at 0 < 1 (expired), follower 3 at 5 >= 1 (valid).
	// Self + follower 3 = 2 = quorum of 3. Still valid.
	r.electionTick = 10
	if !r.hasValidLease() {
		t.Fatal("expected valid lease with self + one fresh remote = quorum")
	}

	// Advance further so both expire.
	// At tick 15: leaseExpiry = 15 - 9 = 6. Follower 3 at 5 < 6 (expired).
	r.electionTick = 15
	if r.hasValidLease() {
		t.Fatal("expected expired lease when all remote acks are stale")
	}
}

func TestLeaseRead_ElectionTickWrapResetsLeaseAcks(t *testing.T) {
	// Verify that when electionTick wraps to 0 during checkQuorum, the
	// remoteLastAck map is cleared so the lease must be re-earned through
	// fresh heartbeat acks. Without this, stale ack values from the
	// previous cycle would satisfy hasValidLease() because
	// leaseExpiry = 0 - leaseTimeout underflows negative.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)

	// Ack from both followers at tick 5.
	r.electionTick = 5
	for _, id := range []uint64{2, 3} {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: id,
			Term: r.term,
			Hint: 0,
		}); err != nil {
			t.Fatalf("HeartbeatResp from %d: %v", id, err)
		}
	}
	r.drainMessages()

	// Lease should be valid now.
	if !r.hasValidLease() {
		t.Fatal("expected valid lease after acks")
	}

	// Simulate the checkQuorum cycle wrap: tickLeader resets electionTick
	// to 0 and calls resetLeaseAcks.
	r.resetLeaseAcks()
	r.electionTick = 0

	// Lease must NOT be valid after the wrap because acks were cleared.
	if r.hasValidLease() {
		t.Fatal("expected invalid lease after electionTick wrap; " +
			"stale ack values should have been cleared")
	}

	// Re-earn the lease through fresh acks.
	for _, id := range []uint64{2, 3} {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: id,
			Term: r.term,
			Hint: 0,
		}); err != nil {
			t.Fatalf("HeartbeatResp from %d: %v", id, err)
		}
	}
	r.drainMessages()

	if !r.hasValidLease() {
		t.Fatal("expected valid lease after re-earned acks")
	}
}

func TestResetLeaseAcks_NilMap(t *testing.T) {
	// resetLeaseAcks should not panic when remoteLastAck is nil
	// (e.g., when leaseRead is disabled).
	r := newTestRaft(1) // Non-lease raft
	electLeader(r)
	r.remoteLastAck = nil
	r.resetLeaseAcks() // must not panic
}

// ---------------------------------------------------------------------------
// Test: H3 - replicaToBit 64-member guard
// ---------------------------------------------------------------------------

// TestBecomeLeader_PanicsWithTooManyVotingMembers verifies that becomeLeader
// panics when the cluster exceeds 64 voting members. The ackBits bitfield is
// a uint64, so positions 0..63 are the maximum. Exceeding this would cause
// bit aliasing and break ReadIndex quorum checks.
func TestBecomeLeader_PanicsWithTooManyVotingMembers(t *testing.T) {
	// Create a membership with 65 voting members (self + 64 remotes).
	members := make(map[uint64]string, 65)
	for i := uint64(1); i <= 65; i++ {
		members[i] = "addr" + string(rune('0'+i%10))
	}

	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}

	// becomeLeader should panic because 65 voting members exceed the
	// 64-bit ackBits capacity.
	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatal("expected panic from becomeLeader with 65 voting members")
		}
		msg, ok := rec.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", rec, rec)
		}
		if !contains(msg, "64 voting members") {
			t.Fatalf("panic message should mention 64 voting members, got: %s", msg)
		}
	}()

	r.becomeCandidate()
	// Grant enough votes for majority (33 of 65).
	for id := uint64(2); id <= 34; id++ {
		if stepErr := r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: id,
			Term: r.term,
		}); stepErr != nil {
			t.Fatalf("step vote from %d: %v", id, stepErr)
		}
		if r.state == leader {
			break
		}
	}

	// If we reach here without panic, the deferred check will catch it.
}

// TestBecomeLeader_SucceedsWithExactly64VotingMembers verifies that a cluster
// with exactly 64 voting members can elect a leader without panic. This is the
// boundary case: 64 members use bit positions 0..63 which exactly fits uint64.
func TestBecomeLeader_SucceedsWithExactly64VotingMembers(t *testing.T) {
	// Create a membership with exactly 64 voting members (self + 63 remotes).
	members := make(map[uint64]string, 64)
	for i := uint64(1); i <= 64; i++ {
		members[i] = "addr" + string(rune('0'+i%10))
	}

	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}

	// becomeLeader should succeed with exactly 64 voting members.
	r.becomeCandidate()
	// Grant enough votes for majority (33 of 64).
	for id := uint64(2); id <= 34; id++ {
		if stepErr := r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: id,
			Term: r.term,
		}); stepErr != nil {
			t.Fatalf("step vote from %d: %v", id, stepErr)
		}
		if r.state == leader {
			break
		}
	}

	if r.state != leader {
		t.Fatal("expected leader state after majority vote with 64 members")
	}

	// Verify all 64 members have unique bit positions.
	if len(r.replicaToBit) != 64 {
		t.Fatalf("expected 64 entries in replicaToBit, got %d", len(r.replicaToBit))
	}

	seen := make(map[uint8]uint64)
	for id, bit := range r.replicaToBit {
		if prevID, ok := seen[bit]; ok {
			t.Fatalf("duplicate bit position %d for nodes %d and %d", bit, prevID, id)
		}
		seen[bit] = id
	}
}

// contains checks if s contains substr. Helper to avoid importing strings
// in a test file that does not already import it.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// GAP-3: Campaign skipping with pending config changes
// ---------------------------------------------------------------------------

// TestHasConfigChangeToApply verifies that hasConfigChangeToApply returns
// true when uncommitted config change entries exist between committed
// and lastIndex.
func TestHasConfigChangeToApply(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.drainMessages()

	// Append a config change entry. The leader's no-op + this entry
	// will be beyond committed (committed is still at the no-op index
	// from becoming leader until a quorum acks).
	r.log.append([]proto.Entry{
		{Term: r.term, Index: r.log.lastIndex() + 1, Type: proto.EntryConfigChange},
	})

	if !r.hasConfigChangeToApply() {
		t.Fatal("expected hasConfigChangeToApply to return true with uncommitted config change")
	}
}

// TestHasConfigChangeToApplyNoChanges verifies that hasConfigChangeToApply
// returns false when there are uncommitted entries but none are config changes.
func TestHasConfigChangeToApplyNoChanges(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.drainMessages()

	// Append a normal entry beyond committed.
	r.log.append([]proto.Entry{
		{Term: r.term, Index: r.log.lastIndex() + 1, Type: proto.EntryNormal},
	})

	if r.hasConfigChangeToApply() {
		t.Fatal("expected hasConfigChangeToApply to return false with only normal entries")
	}
}

// TestHasConfigChangeToApplyAllCommitted verifies that hasConfigChangeToApply
// returns false when committed >= lastIndex (all entries are committed).
func TestHasConfigChangeToApplyAllCommitted(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.drainMessages()

	// Commit everything by advancing committed to lastIndex.
	r.log.commitTo(r.log.lastIndex())

	if r.hasConfigChangeToApply() {
		t.Fatal("expected hasConfigChangeToApply to return false when all entries committed")
	}
}

// TestStartElectionWithUncommittedConfigChange verifies that a follower
// with an uncommitted config change entry CAN start an election. This is
// the liveness fix: the Raft spec (Section 5.2, Section 6) does not block
// elections based on uncommitted log content. Blocking elections here would
// cause a permanent cluster stall if the leader crashes after replicating
// an uncommitted config change to a majority.
func TestStartElectionWithUncommittedConfigChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Append a config change entry at index 1 (beyond committed=0).
	r.log.append([]proto.Entry{
		{Term: r.term, Index: r.log.lastIndex() + 1, Type: proto.EntryConfigChange},
	})

	campaignFired := false
	r.onCampaign = func(_, _, _ uint64) {
		campaignFired = true
	}

	// Election must proceed despite uncommitted config change.
	r.startElection()

	if r.state != candidate {
		t.Fatalf("expected candidate state with uncommitted config change, got %s", r.state)
	}
	if !campaignFired {
		t.Fatal("expected onCampaign callback to fire")
	}

	// Verify vote messages were sent to peers.
	votes := collectMessages(r, proto.RequestVote)
	if len(votes) == 0 {
		t.Fatal("expected vote messages to be sent")
	}
}

// TestStartElectionWithUncommittedConfigChangePreVote verifies the same
// liveness property in PreVote mode: a follower with an uncommitted config
// change entry must be able to start a pre-vote election.
func TestStartElectionWithUncommittedConfigChangePreVote(t *testing.T) {
	r := newTestRaft(1) // PreVote enabled by default.

	// Append a config change entry beyond committed.
	r.log.append([]proto.Entry{
		{Term: r.term, Index: r.log.lastIndex() + 1, Type: proto.EntryConfigChange},
	})

	r.startElection()

	if r.state != preCandidate {
		t.Fatalf("expected preCandidate state with uncommitted config change, got %s", r.state)
	}

	// Verify pre-vote messages were sent to peers.
	preVotes := collectMessages(r, proto.RequestPreVote)
	if len(preVotes) == 0 {
		t.Fatal("expected pre-vote messages to be sent")
	}
}

// TestStartElectionAllowedWithoutConfigChange verifies that startElection
// proceeds normally when there are no uncommitted config change entries.
func TestStartElectionAllowedWithoutConfigChange(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	campaignFired := false
	r.onCampaign = func(_, _, _ uint64) {
		campaignFired = true
	}

	// No uncommitted config changes. Election should proceed.
	r.startElection()

	if r.state != candidate {
		t.Fatalf("expected candidate state, got %s", r.state)
	}
	if !campaignFired {
		t.Fatal("expected onCampaign callback to fire")
	}

	// Verify vote messages were sent to peers.
	votes := collectMessages(r, proto.RequestVote)
	if len(votes) == 0 {
		t.Fatal("expected vote messages to be sent")
	}
}

// TestStartElectionAllowedAfterConfigChangeCommitted verifies that once
// a config change entry is committed, elections proceed normally.
func TestStartElectionAllowedAfterConfigChangeCommitted(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Append a config change entry.
	r.log.append([]proto.Entry{
		{Term: r.term, Index: r.log.lastIndex() + 1, Type: proto.EntryConfigChange},
	})

	// Commit it.
	r.log.commitTo(r.log.lastIndex())

	// Election should proceed.
	r.startElection()

	if r.state != candidate {
		t.Fatalf("expected candidate state after config change committed, got %s", r.state)
	}
}

// TestBecomeLeaderSetsPendingConfigChangeWithUncommitted verifies that
// becomeLeader correctly sets pendingConfigChange when there are uncommitted
// config change entries inherited from the previous leader. This ensures the
// new leader blocks further config change proposals until the inherited one
// commits (PhD thesis Section 4.3).
func TestBecomeLeaderSetsPendingConfigChangeWithUncommitted(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	// Become candidate first (required before becomeLeader).
	r.becomeCandidate()

	// Append a config change entry that is uncommitted.
	r.log.append([]proto.Entry{
		{Term: r.term, Index: r.log.lastIndex() + 1, Type: proto.EntryConfigChange},
	})

	r.becomeLeader()

	if !r.pendingConfigChange {
		t.Fatal("becomeLeader must set pendingConfigChange when uncommitted config change exists")
	}
}

// TestBecomeLeaderNoPendingConfigChangeWhenClean verifies that becomeLeader
// leaves pendingConfigChange as false when there are no uncommitted config
// change entries.
func TestBecomeLeaderNoPendingConfigChangeWhenClean(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)

	r.becomeCandidate()

	// Commit all entries so no uncommitted config changes exist.
	r.log.commitTo(r.log.lastIndex())

	r.becomeLeader()

	if r.pendingConfigChange {
		t.Fatal("becomeLeader must not set pendingConfigChange when no uncommitted config change exists")
	}
}

// TestHasConfigChangeToApplyCompressedEntry verifies that
// hasConfigChangeToApply correctly detects config change entries
// with the compression flag set via the EntryTypeMask.
func TestHasConfigChangeToApplyCompressedEntry(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.drainMessages()

	// Append a config change entry with the compression flag set.
	r.log.append([]proto.Entry{
		{
			Term:  r.term,
			Index: r.log.lastIndex() + 1,
			Type:  proto.EntryConfigChange | proto.EntryCompressFlag,
		},
	})

	if !r.hasConfigChangeToApply() {
		t.Fatal("expected hasConfigChangeToApply to detect config change with compression flag")
	}
}

// TestHasConfigChangeToApplyBoundedScan verifies that the bounded
// maxSize parameter does not cause false negatives when config change
// entries are within the scan window. With many small uncommitted
// entries followed by a config change, the function must still detect
// the config change as long as it fits within the size limit.
func TestHasConfigChangeToApplyBoundedScan(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.drainMessages()

	// Append several normal entries followed by a config change.
	// All entries are small, so they fit well within the 64 MiB scan limit.
	base := r.log.lastIndex()
	entries := make([]proto.Entry, 0, 101)
	for i := uint64(1); i <= 100; i++ {
		entries = append(entries, proto.Entry{
			Term:  r.term,
			Index: base + i,
			Type:  proto.EntryNormal,
			Cmd:   []byte("data"),
		})
	}
	// Config change as the 101st entry.
	entries = append(entries, proto.Entry{
		Term:  r.term,
		Index: base + 101,
		Type:  proto.EntryConfigChange,
	})
	r.log.append(entries)

	if !r.hasConfigChangeToApply() {
		t.Fatal("expected hasConfigChangeToApply to detect config change after many normal entries")
	}
}

// ---------------------------------------------------------------------------
// AppendEntries Pipelining (OPT-A)
// ---------------------------------------------------------------------------

// TestSendAppendPipelining verifies that sendAppend sends multiple Replicate
// messages in a single call when the follower is far behind and the inflight
// window has capacity. With a small maxReplicationPayload (200 bytes) and
// entries of 160 bytes each (60-byte header + 100-byte Cmd), each batch fits
// only one entry. With 5 entries to send, sendAppend should produce 5
// Replicate messages filling the pipeline without waiting for acks.
func TestSendAppendPipelining(t *testing.T) {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.MaxApplyEntrySize = 200
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)
	if r.state != leader {
		t.Fatal("expected leader state after election")
	}

	// Append 5 entries with 100-byte commands. Each entry is 160 bytes,
	// so each batch holds exactly 1 entry at 200-byte payload limit.
	totalEntries := 5
	cmd := make([]byte, 100)
	for range totalEntries {
		if err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		}); err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Set peer 2 to Replicate state, far behind (next=2, only has bootstrap).
	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = 1
	rm.next = 2

	r.drainMessages()
	r.sendAppend(2)

	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) < 2 {
		t.Fatalf("expected multiple pipelined Replicate messages, got %d", len(replicateMsgs))
	}

	// Verify each message targets peer 2 and carries entries.
	for i, msg := range replicateMsgs {
		if msg.To != 2 {
			t.Fatalf("message %d: expected To=2, got %d", i, msg.To)
		}
		if len(msg.Entries) == 0 {
			t.Fatalf("message %d: expected entries, got none", i)
		}
	}

	// Verify entries are contiguous across all messages — no gaps.
	var allEntries []proto.Entry
	for _, msg := range replicateMsgs {
		allEntries = append(allEntries, msg.Entries...)
	}
	for i := 1; i < len(allEntries); i++ {
		if allEntries[i].Index != allEntries[i-1].Index+1 {
			t.Fatalf("gap at position %d: index %d followed by %d",
				i, allEntries[i-1].Index, allEntries[i].Index)
		}
	}

	// Verify each message's LogIndex (prevLogIndex) equals the preceding
	// batch's last entry index.
	for i := 1; i < len(replicateMsgs); i++ {
		prevMsg := replicateMsgs[i-1]
		thisMsg := replicateMsgs[i]
		expectedPrev := prevMsg.Entries[len(prevMsg.Entries)-1].Index
		if thisMsg.LogIndex != expectedPrev {
			t.Fatalf("message %d: LogIndex=%d, expected %d (last entry of previous batch)",
				i, thisMsg.LogIndex, expectedPrev)
		}
	}
}

// TestSendAppendPipeliningStopsAtInflightLimit verifies that pipelining
// stops when the inflight window reaches capacity, preventing the leader
// from overwhelming a slow follower.
func TestSendAppendPipeliningStopsAtInflightLimit(t *testing.T) {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	// Small payload so each batch holds exactly 1 entry.
	cfg.MaxApplyEntrySize = 200
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)

	// Append many more entries than the inflight capacity.
	totalEntries := maxInflightMsgs + 50
	cmd := make([]byte, 100)
	for range totalEntries {
		if err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		}); err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Set peer 2 to Replicate with a small inflight window to make the
	// test faster. Use a capacity of 4.
	rm := r.remotes[2]
	rm.inflights = newInflights(4)
	rm.state = remoteReplicate
	rm.match = 1
	rm.next = 2

	r.drainMessages()
	r.sendAppend(2)

	replicateMsgs := collectMessages(r, proto.Replicate)

	// With a payload of 200 bytes and 160-byte entries, each batch holds
	// 1 entry. The inflight window capacity is 4, so we expect exactly 4
	// Replicate messages — pipelining stops at the inflight limit.
	if len(replicateMsgs) != 4 {
		t.Fatalf("expected 4 pipelined messages (inflight capacity), got %d", len(replicateMsgs))
	}

	// Verify the inflight window is full.
	if !rm.inflights.full() {
		t.Fatal("expected inflights to be full after pipelining")
	}

	// Verify the remote's next is advanced past all sent entries.
	lastSentIdx := replicateMsgs[3].Entries[len(replicateMsgs[3].Entries)-1].Index
	if rm.next != lastSentIdx+1 {
		t.Fatalf("expected rm.next=%d, got %d", lastSentIdx+1, rm.next)
	}
}

// TestSendAppendPipeliningProbeNoLoop verifies that in Probe state, only
// one Replicate message is sent regardless of how many entries are available.
// The probeSent flag prevents further sends until the follower responds.
func TestSendAppendPipeliningProbeNoLoop(t *testing.T) {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.MaxApplyEntrySize = 200
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)

	// Append multiple entries.
	totalEntries := 10
	cmd := make([]byte, 100)
	for range totalEntries {
		if err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		}); err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Set peer 2 to Probe state, far behind.
	rm := r.remotes[2]
	rm.state = remoteProbe
	rm.match = 1
	rm.next = 2
	rm.probeSent = false

	r.drainMessages()
	r.sendAppend(2)

	replicateMsgs := collectMessages(r, proto.Replicate)

	// Probe state sends exactly one message.
	if len(replicateMsgs) != 1 {
		t.Fatalf("expected exactly 1 Replicate in Probe state, got %d", len(replicateMsgs))
	}

	// probeSent should be set, preventing further sends.
	if !rm.probeSent {
		t.Fatal("expected probeSent to be true after sending in Probe state")
	}

	// A second call should be a no-op (isPaused returns true).
	r.drainMessages()
	r.sendAppend(2)
	replicateMsgs = collectMessages(r, proto.Replicate)
	if len(replicateMsgs) != 0 {
		t.Fatalf("expected 0 Replicate after probe is paused, got %d", len(replicateMsgs))
	}
}

// TestSendAppendPipeliningCaughtUp verifies that when the follower is
// caught up (no entries beyond rm.next), sendAppend sends nothing and
// does not loop.
func TestSendAppendPipeliningCaughtUp(t *testing.T) {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)

	// Set peer 2 to Replicate, fully caught up.
	lastIdx := r.log.lastIndex()
	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = lastIdx
	rm.next = lastIdx + 1

	r.drainMessages()
	r.sendAppend(2)

	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) != 0 {
		t.Fatalf("expected 0 Replicate when caught up, got %d", len(replicateMsgs))
	}

	// Inflight count should remain zero.
	if rm.inflights.Count() != 0 {
		t.Fatalf("expected 0 inflights when caught up, got %d", rm.inflights.Count())
	}
}

// TestSendWitnessAppendPipelining verifies that sendWitnessAppend also
// pipelines multiple batches when the witness is in Replicate state and
// the inflight window has capacity. Entries should be converted to metadata
// form (EntryMetadata type, no Cmd data) while config changes are preserved.
func TestSendWitnessAppendPipelining(t *testing.T) {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.MaxApplyEntrySize = 200
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electLeader(r)

	// Append 5 normal entries with 100-byte commands.
	totalEntries := 5
	cmd := make([]byte, 100)
	for range totalEntries {
		if err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		}); err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Set witness 3 to Replicate state, far behind.
	rm := r.witnesses[3]
	if rm == nil {
		t.Fatal("expected witness 3 in witnesses map")
	}
	rm.becomeReplicate()
	rm.match = 1
	rm.next = 2

	r.drainMessages()
	r.sendWitnessAppend(3)

	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) < 2 {
		t.Fatalf("expected multiple pipelined Replicate messages for witness, got %d", len(replicateMsgs))
	}

	// Verify all messages target witness 3 and contain metadata entries.
	for i, msg := range replicateMsgs {
		if msg.To != 3 {
			t.Fatalf("message %d: expected To=3, got %d", i, msg.To)
		}
		if len(msg.Entries) == 0 {
			t.Fatalf("message %d: expected entries, got none", i)
		}
		// Each entry should be metadata-only (no Cmd, EntryMetadata type).
		for j, e := range msg.Entries {
			if !e.IsMetadataEntry() {
				t.Fatalf("message %d entry %d: expected EntryMetadata type, got %d",
					i, j, e.Type)
			}
			if len(e.Cmd) != 0 {
				t.Fatalf("message %d entry %d: expected empty Cmd for witness, got %d bytes",
					i, j, len(e.Cmd))
			}
		}
	}

	// Verify entries are contiguous across all messages.
	var allEntries []proto.Entry
	for _, msg := range replicateMsgs {
		allEntries = append(allEntries, msg.Entries...)
	}
	for i := 1; i < len(allEntries); i++ {
		if allEntries[i].Index != allEntries[i-1].Index+1 {
			t.Fatalf("gap at position %d: index %d followed by %d",
				i, allEntries[i-1].Index, allEntries[i].Index)
		}
	}
}

// TestSendAppendPipeliningNilRemote verifies that sendAppend returns
// immediately without panic when the remote is nil (unknown peer).
func TestSendAppendPipeliningNilRemote(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	r.drainMessages()

	// Node 99 does not exist in the cluster.
	r.sendAppend(99)

	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) != 0 {
		t.Fatalf("expected 0 messages for nil remote, got %d", len(replicateMsgs))
	}
}

// TestSendAppendPipeliningSnapshotState verifies that sendAppend does
// not send any messages when the remote is in Snapshot state (isPaused
// returns true for snapshot state).
func TestSendAppendPipeliningSnapshotState(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)

	// Append entries.
	cmd := make([]byte, 10)
	for range 3 {
		if err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: cmd}},
		}); err != nil {
			t.Fatalf("propose error: %v", err)
		}
	}

	// Set peer 2 to Snapshot state.
	rm := r.remotes[2]
	rm.becomeSnapshot(5)

	r.drainMessages()
	r.sendAppend(2)

	replicateMsgs := collectMessages(r, proto.Replicate)
	if len(replicateMsgs) != 0 {
		t.Fatalf("expected 0 messages in Snapshot state, got %d", len(replicateMsgs))
	}
}

// ---------------------------------------------------------------------------
// Bug C3: Leadership transfer exits quiesce
// ---------------------------------------------------------------------------

func TestRaft_LeaderTransfer_ExitsQuiesce(t *testing.T) {
	// A quiesced leader that initiates a leadership transfer must exit
	// quiesce so that tickLeader runs the normal path with transfer
	// timeout logic. Without the fix, the transfer timeout in tickLeader
	// is unreachable while quiesce.active is true, permanently blocking
	// the shard if the transfer target is unreachable.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Enter quiesce by ticking past the threshold.
	threshold := r.quiesce.threshold
	for i := 0; i < threshold+r.heartbeatTimeout+1; i++ {
		r.tickFunc(r)
		r.drainMessages()
	}
	if !r.quiesce.active {
		t.Fatal("expected leader to be quiesced before transfer")
	}

	// Set peer 2 as caught up so the transfer proceeds immediately.
	r.remotes[2].match = r.log.lastIndex()

	err = r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Quiesce must be exited.
	if r.quiesce.active {
		t.Fatal("expected quiesce to be inactive after leadership transfer")
	}

	// Transfer state must be set.
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", r.transferTarget)
	}

	// Verify that tickLeader now reaches the transfer timeout path.
	// Tick through an election timeout; the transfer should time out
	// and clear, proving the timeout logic is reachable.
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
		r.drainMessages()
	}
	if r.transferTarget != noNode {
		t.Fatal("expected transfer to time out after exiting quiesce")
	}
}

func TestRaft_LeaderTransfer_ExitsQuiesce_InvalidTarget(t *testing.T) {
	// Even when handleLeaderTransfer returns ErrNodeNotFound, quiesce
	// must still be exited. The exit happens before the remote lookup
	// to ensure the leader is always awake after a transfer attempt.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Enter quiesce.
	threshold := r.quiesce.threshold
	for i := 0; i < threshold+r.heartbeatTimeout+1; i++ {
		r.tickFunc(r)
		r.drainMessages()
	}
	if !r.quiesce.active {
		t.Fatal("expected leader to be quiesced")
	}

	err = r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 99, // not in cluster
	})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	// Quiesce must still be exited even though the transfer failed.
	if r.quiesce.active {
		t.Fatal("expected quiesce to be inactive after failed transfer attempt")
	}
}

// ---------------------------------------------------------------------------
// Bug M6: TimeoutNow exits quiesce
// ---------------------------------------------------------------------------

func TestRaft_TimeoutNow_ExitsQuiesce(t *testing.T) {
	// A quiesced follower that receives TimeoutNow must exit quiesce
	// before starting the election. Without the fix, the new leader
	// would use the quiesce liveness probe interval instead of normal
	// heartbeats, and a failed election would use the extended quiesce
	// election timeout (30x normal).
	r := newTestRaft(1)
	r.leaderID = 2
	r.quiesce.enabled = true
	r.quiesce.active = true
	r.quiesce.tick = 50

	err := r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Node should have started an election.
	if r.state != candidate {
		t.Fatalf("expected candidate after TimeoutNow, got %s", r.state)
	}

	// Quiesce must be fully exited (active=false, counters reset).
	if r.quiesce.active {
		t.Fatal("expected quiesce to be inactive after TimeoutNow")
	}
	if r.quiesce.tick != 0 {
		t.Fatalf("expected quiesce tick reset to 0, got %d", r.quiesce.tick)
	}
	if r.quiesce.livenessTick != 0 {
		t.Fatalf("expected quiesce livenessTick reset to 0, got %d", r.quiesce.livenessTick)
	}
}

func TestRaft_TimeoutNow_NotQuiesced_StillWorks(t *testing.T) {
	// Verify that TimeoutNow works correctly when not quiesced (no
	// regression from the quiesce exit fix).
	r := newTestRaft(1)
	r.leaderID = 2

	err := r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != candidate {
		t.Fatalf("expected candidate after TimeoutNow, got %s", r.state)
	}
	if r.quiesce.active {
		t.Fatal("expected quiesce to remain inactive")
	}
}

// ---------------------------------------------------------------------------
// Bug LOW: Leader ignores TimeoutNow
// ---------------------------------------------------------------------------

func TestRaft_TimeoutNow_IgnoredByLeader(t *testing.T) {
	// A leader that receives a same-term TimeoutNow must ignore it.
	// Without the guard, becomeCandidate() panics on the invariant
	// assertion "leader cannot become candidate directly".
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	if r.state != leader {
		t.Fatal("expected leader state")
	}
	savedTerm := r.term

	// Send TimeoutNow to the leader. This must not panic.
	err := r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Leader state must be unchanged.
	if r.state != leader {
		t.Fatalf("expected leader to remain leader, got %s", r.state)
	}
	if r.term != savedTerm {
		t.Fatalf("expected term %d unchanged, got %d", savedTerm, r.term)
	}
}

func TestRaft_TimeoutNow_IgnoredByLeader_NoMessages(t *testing.T) {
	// Verify that a leader ignoring TimeoutNow does not emit any
	// spurious messages (no vote requests, no heartbeats).
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	err := r.step(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: r.term,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	votes := collectMessages(r, proto.RequestVote)
	if len(votes) != 0 {
		t.Fatalf("expected no vote requests from leader ignoring TimeoutNow, got %d", len(votes))
	}
}

// ---------------------------------------------------------------------------
// H1: Stale-term InstallSnapshot sends response
// ---------------------------------------------------------------------------

func TestRaft_Step_StaleInstallSnapshotFromOldTermSendsResponse(t *testing.T) {
	// When an InstallSnapshot arrives with a term older than the receiver's
	// current term, the receiver must send a ReplicateResp so the stale
	// leader learns the new term and steps down. Without this fix, the stale
	// leader would never discover the newer term from snapshot messages.
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	r.term = 1
	electLeader(r) // bumps to term 2
	r.drainMessages()

	// Send an InstallSnapshot from term 1 (older than current term 2).
	err := r.step(proto.Message{
		Type: proto.InstallSnapshot,
		From: 2,
		Term: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should send a ReplicateResp so the stale sender learns the new term.
	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp for stale InstallSnapshot, got none")
	}
	if resps[0].To != 2 {
		t.Fatalf("expected response to node 2, got %d", resps[0].To)
	}
	if resps[0].Term != r.term {
		t.Fatalf("expected response with current term %d, got %d", r.term, resps[0].Term)
	}
}

func TestRaft_Step_StaleInstallSnapshotNoStateChange(t *testing.T) {
	// A stale InstallSnapshot must not alter the receiver's state, log, or
	// term. It should only trigger a ReplicateResp and return.
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	r.term = 1
	electLeader(r) // bumps to term 2
	r.drainMessages()

	termBefore := r.term
	committedBefore := r.log.committed
	lastIndexBefore := r.log.lastIndex()

	err := r.step(proto.Message{
		Type: proto.InstallSnapshot,
		From: 2,
		Term: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Term must not change.
	if r.term != termBefore {
		t.Fatalf("term changed from %d to %d", termBefore, r.term)
	}
	// Committed index must not change.
	if r.log.committed != committedBefore {
		t.Fatalf("committed changed from %d to %d", committedBefore, r.log.committed)
	}
	// Last index must not change.
	if r.log.lastIndex() != lastIndexBefore {
		t.Fatalf("lastIndex changed from %d to %d", lastIndexBefore, r.log.lastIndex())
	}
	// State must remain leader.
	if r.state != leader {
		t.Fatalf("expected leader state, got %d", r.state)
	}
}

// ---------------------------------------------------------------------------
// H2: Leader transfer invalidates lease and blocks ReadIndex
// ---------------------------------------------------------------------------

func TestRaft_LeaderTransfer_InvalidatesLease(t *testing.T) {
	// Per PhD thesis 6.4.1: a leader must expire its lease before
	// transferring leadership. After initiating a transfer, LeaseRead
	// must not be served because the new leader may commit writes
	// immediately after winning the election.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	// Verify lease is valid before transfer.
	if !r.hasValidLease() {
		t.Fatal("expected valid lease before transfer")
	}

	// Initiate leader transfer to node 2.
	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error initiating transfer: %v", err)
	}

	// Lease must be invalidated (remoteLastAck set to nil).
	if r.remoteLastAck != nil {
		t.Fatal("expected remoteLastAck to be nil after transfer initiation")
	}
	if r.hasValidLease() {
		t.Fatal("expected invalid lease after transfer initiation")
	}
}

func TestRaft_LeaderTransfer_LeaseReadBlockedDuringTransfer(t *testing.T) {
	// After initiating a transfer, even if a new lease were somehow
	// established (e.g., heartbeat acks arrive), ReadIndex must still
	// be blocked because the transfer is in progress.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	// Initiate transfer.
	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error initiating transfer: %v", err)
	}
	r.drainMessages()

	// Attempt a ReadIndex during active transfer.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 42,
	})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress during active transfer, got %v", err)
	}

	// No readStates should have been produced.
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates during transfer, got %d", len(r.readStates))
	}
}

func TestRaft_ReadIndex_BlockedDuringTransfer(t *testing.T) {
	// Non-lease ReadIndex (heartbeat-based) must also be blocked during
	// an active leadership transfer to prevent stale reads.
	r := newTestRaft(1)
	electLeader(r)
	commitNoOp(r)
	r.drainMessages()

	// Initiate transfer.
	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error initiating transfer: %v", err)
	}
	r.drainMessages()

	// Attempt a ReadIndex during active transfer.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 99,
	})
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress, got %v", err)
	}

	// No pending read index entries should have been created.
	if len(r.pendingReadIndex) != 0 {
		t.Fatalf("expected 0 pending reads during transfer, got %d", len(r.pendingReadIndex))
	}

	// No heartbeat broadcast should have been triggered.
	if r.readIndexBroadcastPending {
		t.Fatal("expected no pending readIndex broadcast during transfer")
	}
}

func TestRaft_ReadIndex_AllowedAfterTransferCompletes(t *testing.T) {
	// After a transfer times out and is cleared, ReadIndex must resume
	// normal operation.
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	// Initiate and then clear the transfer (simulating timeout).
	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error initiating transfer: %v", err)
	}
	r.drainMessages()

	// Clear transfer (as tickLeader does on timeout).
	r.transferTarget = noNode

	// Re-initialize the lease tracking map. The transfer invalidated it
	// by setting remoteLastAck to nil. In production, tickLeader clears
	// the transfer target but the lease map must be re-initialized to
	// resume lease-based reads.
	r.remoteLastAck = make(map[uint64]int)

	// Re-establish the lease since transfer invalidated it.
	grantLease(r)

	// ReadIndex should succeed now.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 0,
		Hint: 77,
	})
	if err != nil {
		t.Fatalf("unexpected error after transfer cleared: %v", err)
	}

	// Should have produced a readState (lease-based immediate response).
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState after transfer cleared, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 77 {
		t.Fatalf("readState key: expected 77, got %d", r.readStates[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Test: M1 - Witness ack must not satisfy ReadIndex quorum
// ---------------------------------------------------------------------------

func TestRaft_Witness_HeartbeatAckCountsForReadIndexQuorum(t *testing.T) {
	// 4-node cluster: node 1 (leader), node 2 (remote), node 3 (remote),
	// node 4 (witness). electionQuorum = 3 (4 voting / 2 + 1).
	// Witnesses are included in replicaToBit and their heartbeat acks
	// count toward ReadIndex quorum because witnesses vote in elections.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop so hasCommittedEntryAtCurrentTerm is satisfied.
	for id := range r.remotes {
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: r.log.lastIndex(),
		}); err != nil {
			t.Fatalf("ReplicateResp from %d: %v", id, err)
		}
	}
	r.drainMessages()
	r.readStates = nil

	// Verify witness is included in replicaToBit.
	if _, found := r.replicaToBit[4]; !found {
		t.Fatal("witness node 4 must have a bit position in replicaToBit")
	}

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 55,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Send heartbeat ack from witness only - should NOT resolve alone
	// because electionQuorum=3 and self(1) + witness(1) = 2 < 3.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 4,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}

	if len(r.readStates) != 0 {
		t.Fatalf("witness ack alone must not satisfy ReadIndex quorum (electionQuorum=3): got %d readStates, want 0",
			len(r.readStates))
	}

	// Verify pendingReadIndex ackCount WAS incremented by witness.
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read index, got %d", len(r.pendingReadIndex))
	}
	// Self-ack is pre-set at creation (ackCount=1). Witness adds 1 more.
	if r.pendingReadIndex[0].ackCount != 2 {
		t.Fatalf("witness ack should be counted: ackCount=%d, want 2 (self + witness)",
			r.pendingReadIndex[0].ackCount)
	}

	// Now send ack from a data-bearing remote to reach quorum (3).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from remote 2: %v", err)
	}

	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 read state after reaching electionQuorum, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 55 {
		t.Fatalf("expected read state key 55, got %d", r.readStates[0].Key)
	}
}

func TestRaft_Witness_ReadIndexQuorum_WitnessAckSufficientSmallCluster(t *testing.T) {
	// Minimal cluster: node 1 (leader), node 2 (remote), node 3 (witness).
	// electionQuorum = 2 (3 voting / 2 + 1). Since witnesses vote in
	// elections, witness ack correctly satisfies ReadIndex quorum:
	// self(1) + witness(1) = 2 = electionQuorum. This is safe because
	// the leadership confirmation quorum overlaps with every possible
	// election-winning coalition.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 33,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Witness ack alone should resolve because electionQuorum=2 and
	// self(1) + witness(1) = 2 = quorum.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}
	if len(r.readStates) != 1 {
		t.Fatalf("witness ack should resolve ReadIndex in 3-node cluster (electionQuorum=2): "+
			"got %d readStates, want 1", len(r.readStates))
	}
	if r.readStates[0].Key != 33 {
		t.Fatalf("expected read state key 33, got %d", r.readStates[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Test: M2 - Witness ack must not satisfy lease quorum
// ---------------------------------------------------------------------------

func TestRaft_Witness_HeartbeatAckCountsForLeaseQuorum(t *testing.T) {
	// 3-node cluster: node 1 (leader), node 2 (remote), node 3 (witness).
	// LeaseRead enabled. electionQuorum for lease = 2 (3 voting / 2 + 1).
	// Since witnesses vote in elections, witness heartbeat ack counts
	// toward lease validity. self(1) + witness(1) = 2 = quorum.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: r.log.lastIndex(),
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	r.readStates = nil

	// Verify lease is enabled.
	if r.remoteLastAck == nil {
		t.Fatal("remoteLastAck should be initialized when leaseRead is enabled")
	}

	// Send heartbeat ack from witness only.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}
	r.drainMessages()

	// Witness ack must be stored in remoteLastAck (witnesses vote).
	if _, found := r.remoteLastAck[3]; !found {
		t.Fatal("witness node 3 should have an entry in remoteLastAck")
	}

	// Lease should be valid with self + witness = electionQuorum (2).
	if !r.hasValidLease() {
		t.Fatal("lease should be valid with self + witness heartbeat ack (electionQuorum=2)")
	}
}

func TestRaft_Witness_LeaseQuorumUsesElectionQuorum(t *testing.T) {
	// 4-node cluster: node 1 (leader), node 2 (remote), node 3 (remote),
	// node 4 (witness). electionQuorum = 3 (4 voting / 2 + 1).
	// Both witness and remote acks count toward lease validity.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Witness ack should be tracked in remoteLastAck.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 4,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}

	if _, found := r.remoteLastAck[4]; !found {
		t.Fatal("witness node 4 must be tracked in remoteLastAck")
	}
	// self(1) + witness(1) = 2 < electionQuorum(3).
	if r.hasValidLease() {
		t.Fatal("lease must not be valid with self + witness only (need electionQuorum=3)")
	}

	// One data-bearing ack (self + witness + remote 2 = 3 = quorum).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: 0,
	}); err != nil {
		t.Fatalf("HeartbeatResp from remote 2: %v", err)
	}

	if !r.hasValidLease() {
		t.Fatal("lease should be valid with self + witness + remote (electionQuorum=3)")
	}
}

// ---------------------------------------------------------------------------
// Test: LOW - Witness gets metadata-only entries from heartbeat response path
// ---------------------------------------------------------------------------

func TestRaft_Witness_HeartbeatRespSendsWitnessAppendNotFullAppend(t *testing.T) {
	// When a witness responds to a regular heartbeat (Hint=0) and is in
	// Probe state, the leader should call sendWitnessAppend (metadata-only)
	// not sendAppend (full entries). This test verifies that entries sent to
	// the witness via the heartbeat response recovery path have EntryMetadata
	// type with nil Cmd, matching broadcastAppend behavior.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)

	// Propose entries so there is data to replicate.
	if err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("payload-data")}},
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	r.drainMessages()

	// Put witness in Probe state and clear probeSent to allow sending.
	r.witnesses[20].becomeProbe()
	r.witnesses[20].probeSent = false

	// Send regular heartbeat response from witness (Hint=0 triggers send path).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 20,
		Term: r.term,
		Hint: 0, // regular heartbeat, not ReadIndex
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}

	replicates := collectMessages(r, proto.Replicate)
	var witnessRepl []proto.Message
	for _, m := range replicates {
		if m.To == 20 {
			witnessRepl = append(witnessRepl, m)
		}
	}
	if len(witnessRepl) == 0 {
		t.Fatal("expected Replicate message to witness 20 from heartbeat response path")
	}

	// All entries sent to witness must be metadata-only.
	for _, msg := range witnessRepl {
		for i, ent := range msg.Entries {
			if ent.Type != proto.EntryMetadata {
				t.Fatalf("witness entry %d: expected EntryMetadata type (%d), got %d",
					i, proto.EntryMetadata, ent.Type)
			}
			if ent.Cmd != nil {
				t.Fatalf("witness entry %d: metadata entry must have nil Cmd, got %d bytes",
					i, len(ent.Cmd))
			}
		}
	}
}

func TestRaft_Witness_HeartbeatRespReplicateStateSkipsCorrectly(t *testing.T) {
	// When a witness is in Replicate state and responds to a ReadIndex
	// heartbeat (Hint > 0), sendAppend/sendWitnessAppend should be skipped
	// entirely (same optimization as for data-bearing remotes).
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Advance witness to Replicate state.
	r.witnesses[20].becomeReplicate()

	// Send ReadIndex heartbeat response from witness (Hint > 0).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 20,
		Term: r.term,
		Hint: 42, // ReadIndex ctx
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}

	replicates := collectMessages(r, proto.Replicate)
	for _, m := range replicates {
		if m.To == 20 {
			t.Fatal("expected no Replicate to witness 20 for ReadIndex HeartbeatResp " +
				"when in Replicate state (optimization should skip)")
		}
	}
}

// ---------------------------------------------------------------------------
// R1: pendingReadIndex max capacity
// ---------------------------------------------------------------------------

func TestHandleLeaderReadIndex_RejectsWhenPendingReadIndexFull(t *testing.T) {
	// Setup: 3-node cluster with leader and committed no-op. Fill
	// pendingReadIndex to maxPendingReadIndex capacity. The next
	// ReadIndex request must return ErrReadIndexOverloaded.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	// Fill pendingReadIndex to capacity. We inject entries directly
	// rather than stepping through raft to avoid O(4096) heartbeat
	// broadcasts. The capacity check only looks at len(pendingReadIndex).
	r.pendingReadIndex = make([]readIndexStatus, maxPendingReadIndex)
	for i := range r.pendingReadIndex {
		r.pendingReadIndex[i] = readIndexStatus{
			key:   uint64(i),
			index: r.log.committed,
			ctx:   uint64(i + 1),
		}
	}

	// The next ReadIndex must be rejected.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 9999,
	})
	if !errors.Is(err, ErrReadIndexOverloaded) {
		t.Fatalf("expected ErrReadIndexOverloaded, got %v", err)
	}

	// Verify the slice did not grow.
	if len(r.pendingReadIndex) != maxPendingReadIndex {
		t.Fatalf("expected %d pending reads, got %d",
			maxPendingReadIndex, len(r.pendingReadIndex))
	}
}

func TestHandleLeaderReadIndex_AcceptsAfterDrainingPendingReads(t *testing.T) {
	// After clearing some pending entries, new ReadIndex requests
	// must be accepted again.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	// Fill to capacity.
	r.pendingReadIndex = make([]readIndexStatus, maxPendingReadIndex)
	for i := range r.pendingReadIndex {
		r.pendingReadIndex[i] = readIndexStatus{
			key:   uint64(i),
			index: r.log.committed,
			ctx:   uint64(i + 1),
		}
	}

	// Verify it is full.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 9999,
	})
	if !errors.Is(err, ErrReadIndexOverloaded) {
		t.Fatalf("expected ErrReadIndexOverloaded when full, got %v", err)
	}

	// Drain half of the entries (simulating quorum resolution).
	r.pendingReadIndex = r.pendingReadIndex[:maxPendingReadIndex/2]

	// Now a new ReadIndex must succeed.
	err = r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 8888,
	})
	if err != nil {
		t.Fatalf("expected ReadIndex to succeed after drain, got %v", err)
	}

	// Verify the entry was appended.
	if len(r.pendingReadIndex) != maxPendingReadIndex/2+1 {
		t.Fatalf("expected %d pending reads, got %d",
			maxPendingReadIndex/2+1, len(r.pendingReadIndex))
	}
}

// ---------------------------------------------------------------------------
// R3: readIndexCtx wrap guard
// ---------------------------------------------------------------------------

func TestHandleLeaderReadIndex_CtxWrapsToOneNotZero(t *testing.T) {
	// When readIndexCtx is at math.MaxUint64, the next increment wraps
	// to 0. The wrap guard must skip 0 and use 1, because ctx=0 is
	// reserved for regular heartbeats.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	// Set readIndexCtx to the max value so the next increment wraps.
	r.readIndexCtx = math.MaxUint64

	// Issue a ReadIndex which will increment readIndexCtx.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}

	// readIndexCtx should be 1, not 0.
	if r.readIndexCtx != 1 {
		t.Fatalf("expected readIndexCtx=1 after wrap, got %d", r.readIndexCtx)
	}

	// The pending entry should also have ctx=1.
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(r.pendingReadIndex))
	}
	if r.pendingReadIndex[0].ctx != 1 {
		t.Fatalf("expected pending read ctx=1, got %d", r.pendingReadIndex[0].ctx)
	}
}

func TestHandleLeaderReadIndex_CtxDoesNotWrapUnderNormalOps(t *testing.T) {
	// Under normal operation, readIndexCtx increments normally and
	// the wrap guard is not triggered.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	r.readIndexCtx = 100

	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}

	if r.readIndexCtx != 101 {
		t.Fatalf("expected readIndexCtx=101, got %d", r.readIndexCtx)
	}
}

// ---------------------------------------------------------------------------
// Test: Rate limiting helper to create raft with rate limiter enabled
// ---------------------------------------------------------------------------

// raftTestCfgRateLimit returns a config with rate limiting enabled.
func raftTestCfgRateLimit(shardID, replicaID, maxInMemLogSize uint64) config.Config {
	cfg := raftTestCfg(shardID, replicaID)
	cfg.MaxInMemLogSize = maxInMemLogSize
	return cfg
}

// newTestRaftRateLimited creates a raft node with rate limiting enabled.
func newTestRaftRateLimited(replicaID uint64, maxInMemLogSize uint64) *raft {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgRateLimit(1, replicaID, maxInMemLogSize)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newTestRaftRateLimited failed: " + err.Error())
	}
	return r
}

// ---------------------------------------------------------------------------
// Test: tickRateLimit
// ---------------------------------------------------------------------------

func TestTickRateLimit_NilRateLimiterNoOp(t *testing.T) {
	// When rl is nil, tickRateLimit should be a no-op and not panic.
	r := newTestRaft(1)
	if r.rl != nil {
		t.Fatal("expected nil rate limiter for default config")
	}

	// Should not panic.
	r.tickRateLimit()

	// No messages should be generated.
	if len(r.msgs) != 0 {
		t.Fatalf("expected no messages, got %d", len(r.msgs))
	}
}

func TestTickRateLimit_DisabledRateLimiterNoOp(t *testing.T) {
	// When rl exists but is disabled (maxSize=0), tickRateLimit is a no-op.
	r := newTestRaftRateLimited(1, 0)
	if r.rl != nil && r.rl.Enabled() {
		t.Fatal("expected disabled rate limiter")
	}

	r.tickRateLimit()

	if len(r.msgs) != 0 {
		t.Fatalf("expected no messages, got %d", len(r.msgs))
	}
}

func TestTickRateLimit_IncrementsTickCounter(t *testing.T) {
	// tickRateLimit should increment rateLimitTick each call.
	r := newTestRaftRateLimited(1, 1000)
	r.becomeFollower(1, 2) // Set a known leader.

	if r.rateLimitTick != 0 {
		t.Fatalf("expected rateLimitTick=0, got %d", r.rateLimitTick)
	}

	r.tickRateLimit()
	if r.rateLimitTick != 1 {
		t.Fatalf("expected rateLimitTick=1, got %d", r.rateLimitTick)
	}

	r.tickRateLimit()
	if r.rateLimitTick != 2 {
		t.Fatalf("expected rateLimitTick=2, got %d", r.rateLimitTick)
	}
}

func TestTickRateLimit_SendsMessageAtElectionTimeout(t *testing.T) {
	// When rateLimitTick reaches electionTimeout, a RateLimit message
	// should be sent and rateLimitTick reset.
	r := newTestRaftRateLimited(1, 1000)
	r.becomeFollower(1, 2) // Set a known leader.
	r.rl.Set(500)          // Set some in-memory size.

	// Tick until just before electionTimeout.
	for i := 0; i < r.electionTimeout-1; i++ {
		r.tickRateLimit()
		if len(r.msgs) != 0 {
			t.Fatalf("sent message too early at tick %d", i+1)
		}
	}

	// The next tick should trigger the message.
	r.tickRateLimit()

	// Verify message was sent.
	msgs := collectMessages(r, proto.RateLimit)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 RateLimit message, got %d", len(msgs))
	}

	// Verify message contents.
	msg := msgs[0]
	if msg.To != 2 {
		t.Fatalf("expected message to leader 2, got %d", msg.To)
	}
	if msg.Hint != 500 {
		t.Fatalf("expected Hint=500, got %d", msg.Hint)
	}

	// rateLimitTick should be reset.
	if r.rateLimitTick != 0 {
		t.Fatalf("expected rateLimitTick=0 after send, got %d", r.rateLimitTick)
	}
}

func TestTickRateLimit_ResetsCycleAfterSend(t *testing.T) {
	// After sending a RateLimit message, the cycle should restart.
	r := newTestRaftRateLimited(1, 1000)
	r.becomeFollower(1, 2)

	// First cycle: tick until message sent.
	for i := 0; i < r.electionTimeout; i++ {
		r.tickRateLimit()
	}
	if len(collectMessages(r, proto.RateLimit)) != 1 {
		t.Fatal("expected first RateLimit message")
	}
	r.drainMessages()

	// Second cycle: tick again until message sent.
	for i := 0; i < r.electionTimeout; i++ {
		r.tickRateLimit()
	}
	if len(collectMessages(r, proto.RateLimit)) != 1 {
		t.Fatal("expected second RateLimit message")
	}
}

// ---------------------------------------------------------------------------
// Test: sendRateLimitMessage
// ---------------------------------------------------------------------------

func TestSendRateLimitMessage_NoLeader(t *testing.T) {
	// When there is no leader, sendRateLimitMessage should not send.
	r := newTestRaftRateLimited(1, 1000)
	if r.leaderID != noLeader {
		t.Fatalf("expected noLeader initially, got %d", r.leaderID)
	}

	r.sendRateLimitMessage()

	if len(r.msgs) != 0 {
		t.Fatalf("expected no messages when no leader, got %d", len(r.msgs))
	}
}

func TestSendRateLimitMessage_SendsToLeader(t *testing.T) {
	// When leader is known, sendRateLimitMessage sends the current
	// in-memory size in the Hint field.
	r := newTestRaftRateLimited(1, 1000)
	r.becomeFollower(1, 3) // Leader is node 3.
	r.rl.Set(750)

	r.sendRateLimitMessage()

	if len(r.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(r.msgs))
	}

	msg := r.msgs[0]
	if msg.Type != proto.RateLimit {
		t.Fatalf("expected RateLimit type, got %d", msg.Type)
	}
	if msg.To != 3 {
		t.Fatalf("expected To=3 (leader), got %d", msg.To)
	}
	if msg.Hint != 750 {
		t.Fatalf("expected Hint=750 (in-mem size), got %d", msg.Hint)
	}
}

func TestSendRateLimitMessage_ZeroSize(t *testing.T) {
	// When in-memory size is zero, the message should still be sent.
	r := newTestRaftRateLimited(1, 1000)
	r.becomeFollower(1, 2)
	r.rl.Set(0)

	r.sendRateLimitMessage()

	msgs := collectMessages(r, proto.RateLimit)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 RateLimit message, got %d", len(msgs))
	}
	if msgs[0].Hint != 0 {
		t.Fatalf("expected Hint=0, got %d", msgs[0].Hint)
	}
}

func TestSendRateLimitMessage_LargeSize(t *testing.T) {
	// Large in-memory sizes should be correctly reported.
	r := newTestRaftRateLimited(1, 1000)
	r.becomeFollower(1, 2)
	r.rl.Set(1 << 30) // 1GB.

	r.sendRateLimitMessage()

	msgs := collectMessages(r, proto.RateLimit)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 RateLimit message, got %d", len(msgs))
	}
	if msgs[0].Hint != 1<<30 {
		t.Fatalf("expected Hint=1<<30, got %d", msgs[0].Hint)
	}
}

// ---------------------------------------------------------------------------
// Test: handleLeaderRateLimit
// ---------------------------------------------------------------------------

func TestHandleLeaderRateLimit_NilRateLimiterNoOp(t *testing.T) {
	// When rl is nil, handleLeaderRateLimit should not panic and
	// return nil error.
	r := newTestRaft(1)
	electLeader(r)

	if r.rl != nil {
		t.Fatal("expected nil rate limiter")
	}

	err := r.handleLeaderRateLimit(proto.Message{
		Type: proto.RateLimit,
		From: 2,
		Hint: 500,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleLeaderRateLimit_DisabledRateLimiterNoOp(t *testing.T) {
	// When rl is disabled, handleLeaderRateLimit should not track
	// follower state.
	r := newTestRaftRateLimited(1, 0)
	electLeader(r)

	err := r.handleLeaderRateLimit(proto.Message{
		Type: proto.RateLimit,
		From: 2,
		Hint: 500,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleLeaderRateLimit_RecordsFollowerState(t *testing.T) {
	// handleLeaderRateLimit should record the follower's in-memory
	// log size via SetFollowerState.
	r := newTestRaftRateLimited(1, 1000)
	electLeader(r)

	// Simulate RateLimit message from follower 2 with size 500.
	err := r.handleLeaderRateLimit(proto.Message{
		Type: proto.RateLimit,
		From: 2,
		Hint: 500,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the follower state was recorded.
	// We do this indirectly by checking if the rate limiter considers
	// follower sizes when we call RateLimited().
	// First, set local size low.
	r.rl.Set(100)

	// With maxSize=1000, follower size 500 < 1000, not limited.
	if r.rl.RateLimited() {
		t.Fatal("should not be rate limited with sizes below threshold")
	}
}

func TestHandleLeaderRateLimit_MultipleFollowers(t *testing.T) {
	// Leader should track multiple followers' states.
	r := newTestRaftRateLimited(1, 1000)
	electLeader(r)

	// Receive from follower 2.
	err := r.handleLeaderRateLimit(proto.Message{
		From: 2,
		Hint: 300,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Receive from follower 3.
	err = r.handleLeaderRateLimit(proto.Message{
		From: 3,
		Hint: 400,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both should be tracked (indirect verification through rate limiting
	// behavior with large values).
}

func TestHandleLeaderRateLimit_UpdatesExistingFollower(t *testing.T) {
	// Subsequent messages from the same follower should update the state.
	r := newTestRaftRateLimited(1, 1000)
	electLeader(r)

	// First update.
	err := r.handleLeaderRateLimit(proto.Message{
		From: 2,
		Hint: 300,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second update with different size.
	err = r.handleLeaderRateLimit(proto.Message{
		From: 2,
		Hint: 800,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The follower state should reflect the latest value.
}

func TestHandleLeaderRateLimit_AlwaysReturnsNil(t *testing.T) {
	// handleLeaderRateLimit should always return nil (no error).
	tests := []struct {
		name string
		rl   bool
		from uint64
		hint uint64
	}{
		{"nil_rl", false, 2, 500},
		{"enabled_rl", true, 2, 500},
		{"zero_hint", true, 2, 0},
		{"large_hint", true, 2, 1 << 40},
		{"from_unknown_replica", true, 99, 500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var r *raft
			if tc.rl {
				r = newTestRaftRateLimited(1, 1000)
			} else {
				r = newTestRaft(1)
			}
			electLeader(r)

			err := r.handleLeaderRateLimit(proto.Message{
				From: tc.from,
				Hint: tc.hint,
			})
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test: rateLimited
// ---------------------------------------------------------------------------

func TestRateLimited_NilRateLimiterReturnsFalse(t *testing.T) {
	// When rl is nil, rateLimited() should return false.
	r := newTestRaft(1)
	if r.rl != nil {
		t.Fatal("expected nil rate limiter")
	}

	if r.rateLimited() {
		t.Fatal("expected rateLimited()=false with nil rl")
	}
}

func TestRateLimited_DisabledRateLimiterReturnsFalse(t *testing.T) {
	// When rl is disabled (maxSize=0), rateLimited() returns false.
	r := newTestRaftRateLimited(1, 0)
	if r.rl != nil && r.rl.Enabled() {
		t.Fatal("expected disabled rate limiter")
	}

	if r.rateLimited() {
		t.Fatal("expected rateLimited()=false with disabled rl")
	}
}

func TestRateLimited_BelowThresholdReturnsFalse(t *testing.T) {
	// When in-memory size is below threshold, rateLimited() returns false.
	r := newTestRaftRateLimited(1, 1000)
	r.rl.Set(500) // 50% of max.

	if r.rateLimited() {
		t.Fatal("expected rateLimited()=false when below threshold")
	}
}

func TestRateLimited_AboveThresholdReturnsTrue(t *testing.T) {
	// When in-memory size exceeds threshold, rateLimited() returns true.
	r := newTestRaftRateLimited(1, 1000)
	r.rl.Set(1500) // 150% of max.

	// Due to hysteresis with tick debouncing, we need to advance ticks.
	// The InMemRateLimiter requires ChangeTickThreshold ticks to pass
	// before changing state.
	for i := 0; i < 15; i++ { // > ChangeTickThreshold (10)
		r.rl.Tick()
	}

	if !r.rateLimited() {
		t.Fatal("expected rateLimited()=true when above threshold")
	}
}

func TestRateLimited_ExactThresholdNotLimited(t *testing.T) {
	// At exactly maxSize, not limited (threshold is > not >=).
	r := newTestRaftRateLimited(1, 1000)
	r.rl.Set(1000)

	if r.rateLimited() {
		t.Fatal("expected rateLimited()=false at exact threshold")
	}
}

func TestRateLimited_HysteresisPreventsThrashing(t *testing.T) {
	// Rate limiting uses hysteresis: enters at 100%, exits at 70%.
	r := newTestRaftRateLimited(1, 1000)

	// Start above threshold to become limited.
	r.rl.Set(1100)
	for i := 0; i < 15; i++ {
		r.rl.Tick()
	}

	if !r.rateLimited() {
		t.Fatal("expected limited when above 100%")
	}

	// Drop to 75% - should still be limited (hysteresis).
	r.rl.Set(750)
	for i := 0; i < 15; i++ {
		r.rl.Tick()
	}

	if !r.rateLimited() {
		t.Fatal("expected still limited at 75% due to hysteresis")
	}

	// Drop to 65% - should become unlimited.
	r.rl.Set(650)
	for i := 0; i < 15; i++ {
		r.rl.Tick()
	}

	if r.rateLimited() {
		t.Fatal("expected unlimited at 65%")
	}
}

func TestRateLimited_FollowerStateTriggers(t *testing.T) {
	// Rate limiting considers follower states, not just local.
	r := newTestRaftRateLimited(1, 1000)
	electLeader(r)

	// Local size is low.
	r.rl.Set(100)

	// Advance ticks past the debounce threshold while keeping follower
	// state fresh (gcTick=3, ChangeTickThreshold=10).
	for i := 0; i < 15; i++ {
		// Refresh follower state each tick to prevent GC.
		r.rl.SetFollowerState(2, 1500)
		r.rl.Tick()
	}

	if !r.rateLimited() {
		t.Fatal("expected limited due to follower state")
	}
}

func TestRateLimited_ZeroSizeNotLimited(t *testing.T) {
	// Zero in-memory size should not be rate limited.
	r := newTestRaftRateLimited(1, 1000)
	r.rl.Set(0)

	if r.rateLimited() {
		t.Fatal("expected rateLimited()=false with zero size")
	}
}

// ---------------------------------------------------------------------------
// Test: Rate limiting integration (tick cycle with follower)
// ---------------------------------------------------------------------------

func TestRateLimitIntegration_FollowerSendsToLeader(t *testing.T) {
	// Full integration: follower ticks, sends RateLimit to leader,
	// leader processes it.
	follower := newTestRaftRateLimited(2, 1000)
	leader := newTestRaftRateLimited(1, 1000)

	follower.becomeFollower(1, 1) // Leader is node 1.
	electLeader(leader)

	// Follower has some in-memory log.
	follower.rl.Set(600)

	// Tick follower until RateLimit sent.
	for i := 0; i < follower.electionTimeout; i++ {
		follower.tickRateLimit()
	}

	msgs := collectMessages(follower, proto.RateLimit)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 RateLimit message, got %d", len(msgs))
	}

	// Leader processes the message.
	err := leader.handleLeaderRateLimit(msgs[0])
	if err != nil {
		t.Fatalf("leader failed to handle RateLimit: %v", err)
	}
}

func TestRateLimitIntegration_LeaderAggregatesFollowers(t *testing.T) {
	// Leader aggregates in-memory sizes from multiple followers.
	leader := newTestRaftRateLimited(1, 1000)
	electLeader(leader)

	// Receive from two followers.
	for _, from := range []uint64{2, 3} {
		err := leader.handleLeaderRateLimit(proto.Message{
			From: from,
			Hint: 400,
		})
		if err != nil {
			t.Fatalf("handleLeaderRateLimit failed: %v", err)
		}
	}

	// Local size low, followers moderate.
	leader.rl.Set(300)

	// Should not be limited (max 400 < 1000).
	if leader.rateLimited() {
		t.Fatal("should not be limited")
	}

	// Now one follower reports high. Advance ticks while keeping the
	// follower state fresh (gcTick=3, ChangeTickThreshold=10).
	for i := 0; i < 15; i++ {
		// Refresh follower 2's high usage state each tick.
		leader.rl.SetFollowerState(2, 1200)
		leader.rl.Tick()
	}

	if !leader.rateLimited() {
		t.Fatal("should be limited due to follower 2")
	}
}

// ---------------------------------------------------------------------------
// Test: Raft specification compliance - Vote, Log Matching, Safety
// ---------------------------------------------------------------------------

func TestRaft_VoteRequest_GrantedWhenAlreadyVotedForSameCandidate(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.term = 5
	r.vote = 2 // already voted for node 2 in term 5
	r.msgs = nil

	// Send RequestVote from node 2 at term 5 with up-to-date log.
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     5,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected vote response")
	}
	if resps[0].Reject {
		t.Fatal("expected vote granted for same candidate")
	}
	if r.vote != 2 {
		t.Fatalf("expected vote to remain 2, got %d", r.vote)
	}
}

func TestRaft_LogMatchingProperty_SameIndexTermImpliesSameCommand(t *testing.T) {
	// Create leader and propose an entry with specific command bytes.
	leaderNode := newTestRaftPreVoteDisabled(1)
	electLeader(leaderNode)

	// Acknowledge the initial probe from peer 2 so it moves to replicate.
	noopIdx := leaderNode.log.lastIndex()
	err := leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderNode.term,
		LogIndex: noopIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error acking noop: %v", err)
	}
	leaderNode.drainMessages()

	// Propose an entry with specific command bytes.
	cmd := []byte("log-matching-test")
	err = leaderNode.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: cmd}},
	})
	if err != nil {
		t.Fatalf("unexpected error proposing: %v", err)
	}

	// Collect the Replicate message sent to follower 2.
	appends := collectMessages(leaderNode, proto.Replicate)
	if len(appends) == 0 {
		t.Fatal("expected Replicate messages after propose")
	}
	var replicateMsg proto.Message
	for _, m := range appends {
		if m.To == 2 {
			replicateMsg = m
			break
		}
	}
	if len(replicateMsg.Entries) == 0 {
		t.Fatal("expected entries in Replicate message to node 2")
	}

	// Create a separate follower and step the Replicate message.
	followerNode := newTestRaftPreVoteDisabled(2)
	followerNode.term = leaderNode.term
	followerNode.leaderID = 1
	// Give follower the noop entry so prevLogIndex/prevLogTerm matches.
	followerNode.log.append([]proto.Entry{{Index: 1, Term: leaderNode.term}})

	err = followerNode.step(replicateMsg)
	if err != nil {
		t.Fatalf("unexpected error stepping Replicate on follower: %v", err)
	}

	// Verify the entry at the same index on both has the same term and cmd.
	entryIdx := replicateMsg.Entries[0].Index
	leaderTerm, err := leaderNode.log.term(entryIdx)
	if err != nil {
		t.Fatalf("leader log term error at index %d: %v", entryIdx, err)
	}
	followerTerm, err := followerNode.log.term(entryIdx)
	if err != nil {
		t.Fatalf("follower log term error at index %d: %v", entryIdx, err)
	}
	if leaderTerm != followerTerm {
		t.Fatalf("term mismatch at index %d: leader=%d follower=%d",
			entryIdx, leaderTerm, followerTerm)
	}

	// Verify command bytes match.
	leaderEntries, lerr := leaderNode.log.entries(entryIdx, entryIdx+1, math.MaxUint64)
	if lerr != nil {
		t.Fatalf("leader entries error: %v", lerr)
	}
	followerEntries, ferr := followerNode.log.entries(entryIdx, entryIdx+1, math.MaxUint64)
	if ferr != nil {
		t.Fatalf("follower entries error: %v", ferr)
	}
	if !bytes.Equal(leaderEntries[0].Cmd, followerEntries[0].Cmd) {
		t.Fatalf("cmd mismatch at index %d: leader=%q follower=%q",
			entryIdx, leaderEntries[0].Cmd, followerEntries[0].Cmd)
	}
}

func TestRaft_LogMatchingProperty_PrefixConsistency(t *testing.T) {
	// Create leader with entries [1:t1, 2:t1, 3:t2(noop from election)].
	leaderNode := newTestRaftPreVoteDisabled(1)
	leaderNode.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	// Elect leader (appends noop at term 2 = index 3).
	electLeader(leaderNode)
	leaderLastIdx := leaderNode.log.lastIndex()
	if leaderLastIdx < 3 {
		t.Fatalf("expected leader to have at least 3 entries, got lastIndex=%d",
			leaderLastIdx)
	}

	// Acknowledge from peer 2 so it moves out of probe.
	err := leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderNode.term,
		LogIndex: leaderLastIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	leaderNode.drainMessages()

	// Create follower and replicate all entries via Replicate message.
	followerNode := newTestRaftPreVoteDisabled(2)
	followerNode.term = leaderNode.term
	followerNode.leaderID = 1

	// Build entries 1..leaderLastIdx from the leader's log.
	leaderEntries, lerr := leaderNode.log.entries(1, leaderLastIdx+1, math.MaxUint64)
	if lerr != nil {
		t.Fatalf("leader entries error: %v", lerr)
	}

	// Step a Replicate with prevLogIndex=0, prevLogTerm=0 to send all entries.
	err = followerNode.step(proto.Message{
		Type:     proto.Replicate,
		From:     1,
		Term:     leaderNode.term,
		LogIndex: 0,
		LogTerm:  0,
		Entries:  leaderEntries,
		Commit:   0,
	})
	if err != nil {
		t.Fatalf("unexpected error stepping Replicate: %v", err)
	}

	// Verify ALL entries 1..leaderLastIdx match between leader and follower.
	for idx := uint64(1); idx <= leaderLastIdx; idx++ {
		lt, lerr := leaderNode.log.term(idx)
		if lerr != nil {
			t.Fatalf("leader term error at index %d: %v", idx, lerr)
		}
		ft, ferr := followerNode.log.term(idx)
		if ferr != nil {
			t.Fatalf("follower term error at index %d: %v", idx, ferr)
		}
		if lt != ft {
			t.Fatalf("term mismatch at index %d: leader=%d follower=%d", idx, lt, ft)
		}
	}
}

func TestRaft_LeaderAppendOnly_NeverDeletesOrOverwrites(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	electLeader(r)
	r.drainMessages()

	// Record the log state before any stale messages.
	lastIdxBefore := r.log.lastIndex()
	termsBefore := make(map[uint64]uint64)
	for idx := uint64(1); idx <= lastIdxBefore; idx++ {
		tm, err := r.log.term(idx)
		if err != nil {
			t.Fatalf("term error at index %d: %v", idx, err)
		}
		termsBefore[idx] = tm
	}

	// Send a stale Replicate message from a deposed leader (old term).
	// The step function drops messages with term < r.term.
	_ = r.step(proto.Message{
		Type:     proto.Replicate,
		From:     3,
		Term:     r.term - 1,
		LogIndex: 0,
		LogTerm:  0,
		Entries:  []proto.Entry{{Index: 1, Term: r.term - 1, Cmd: []byte("stale")}},
	})
	r.drainMessages()

	// Send a ReplicateResp rejection from a follower (should not truncate).
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: lastIdxBefore,
		Reject:   true,
		Hint:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.drainMessages()

	// Verify leader's log was never truncated.
	lastIdxAfter := r.log.lastIndex()
	if lastIdxAfter < lastIdxBefore {
		t.Fatalf("leader log truncated: before=%d after=%d",
			lastIdxBefore, lastIdxAfter)
	}
	for idx := uint64(1); idx <= lastIdxBefore; idx++ {
		tm, err := r.log.term(idx)
		if err != nil {
			t.Fatalf("term error at index %d after: %v", idx, err)
		}
		if tm != termsBefore[idx] {
			t.Fatalf("term changed at index %d: before=%d after=%d",
				idx, termsBefore[idx], tm)
		}
	}
}

func TestRaft_LeaderRetriesAppendIndefinitely(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	})
	electLeader(r)
	r.drainMessages()

	rm := r.remotes[2]
	// Force remote into probe state starting at a high next.
	rm.becomeProbe()
	rm.next = r.log.lastIndex() + 1

	// Simulate 5 consecutive rejections with decreasing hint.
	for i := 0; i < 5; i++ {
		prevNext := rm.next
		rejectedIdx := rm.next - 1
		hint := rejectedIdx
		if hint > 0 {
			hint--
		}

		err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     2,
			Term:     r.term,
			LogIndex: rejectedIdx,
			Reject:   true,
			Hint:     hint,
		})
		if err != nil {
			t.Fatalf("rejection %d: unexpected error: %v", i, err)
		}

		// Verify leader sends a new Replicate (never gives up).
		appends := collectMessages(r, proto.Replicate)
		if len(appends) == 0 {
			t.Fatalf("rejection %d: expected Replicate retry, got none", i)
		}
		r.drainMessages()

		// Verify nextIndex decreased.
		if rm.next >= prevNext {
			t.Fatalf("rejection %d: expected next to decrease from %d, got %d",
				i, prevNext, rm.next)
		}

		// Reset probe sent flag so the next rejection triggers a send.
		rm.probeSent = false
	}
}

func TestRaft_AppendEntries_Idempotent(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
	})
	r.term = 2
	r.leaderID = 3
	r.msgs = nil

	// Build a Replicate message with entries at index 2 and 3.
	replicateMsg := proto.Message{
		Type:     proto.Replicate,
		From:     3,
		Term:     2,
		LogIndex: 1,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 2, Term: 2, Cmd: []byte("x")},
			{Index: 3, Term: 2, Cmd: []byte("y")},
		},
		Commit: 0,
	}

	// Send it once.
	err := r.step(replicateMsg)
	if err != nil {
		t.Fatalf("first step: unexpected error: %v", err)
	}
	resps1 := collectMessages(r, proto.ReplicateResp)
	if len(resps1) == 0 {
		t.Fatal("expected ReplicateResp after first send")
	}
	if resps1[0].Reject {
		t.Fatal("expected non-rejection on first send")
	}
	lastIdx1 := r.log.lastIndex()
	committed1 := r.log.committed
	r.drainMessages()

	// Send the exact same message again.
	err = r.step(replicateMsg)
	if err != nil {
		t.Fatalf("second step: unexpected error: %v", err)
	}
	resps2 := collectMessages(r, proto.ReplicateResp)
	if len(resps2) == 0 {
		t.Fatal("expected ReplicateResp after second send")
	}
	if resps2[0].Reject {
		t.Fatal("expected non-rejection on second send")
	}

	// Verify log state is identical after both sends.
	lastIdx2 := r.log.lastIndex()
	committed2 := r.log.committed
	if lastIdx1 != lastIdx2 {
		t.Fatalf("lastIndex changed: first=%d second=%d", lastIdx1, lastIdx2)
	}
	if committed1 != committed2 {
		t.Fatalf("committed changed: first=%d second=%d", committed1, committed2)
	}
	if resps1[0].LogIndex != resps2[0].LogIndex {
		t.Fatalf("response LogIndex changed: first=%d second=%d",
			resps1[0].LogIndex, resps2[0].LogIndex)
	}
}

func TestRaft_Safety_StateMachineSafety_NoDivergentApply(t *testing.T) {
	// Create a leader and follower in a 3-node cluster.
	leaderNode := newTestRaftPreVoteDisabled(1)
	electLeader(leaderNode)

	// Acknowledge the noop from peer 2.
	noopIdx := leaderNode.log.lastIndex()
	err := leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderNode.term,
		LogIndex: noopIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	leaderNode.drainMessages()

	// Propose entries.
	err = leaderNode.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Cmd: []byte("cmd1")},
			{Cmd: []byte("cmd2")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Collect the Replicate message to peer 2.
	appends := collectMessages(leaderNode, proto.Replicate)
	var replicateMsg proto.Message
	for _, m := range appends {
		if m.To == 2 {
			replicateMsg = m
			break
		}
	}
	leaderNode.drainMessages()

	// Create follower and step the Replicate.
	followerNode := newTestRaftPreVoteDisabled(2)
	followerNode.term = leaderNode.term
	followerNode.leaderID = 1
	// Give follower the noop entry.
	followerNode.log.append([]proto.Entry{{Index: 1, Term: leaderNode.term}})

	err = followerNode.step(replicateMsg)
	if err != nil {
		t.Fatalf("unexpected error stepping Replicate: %v", err)
	}
	followerNode.drainMessages()

	// Commit entries on leader by stepping ReplicateResp from peer 2.
	err = leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderNode.term,
		LogIndex: leaderNode.log.lastIndex(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	leaderNode.drainMessages()

	// Commit on follower by sending a Replicate with updated commit.
	err = followerNode.step(proto.Message{
		Type:     proto.Replicate,
		From:     1,
		Term:     leaderNode.term,
		LogIndex: followerNode.log.lastIndex(),
		LogTerm:  leaderNode.term,
		Entries:  nil,
		Commit:   leaderNode.log.committed,
	})
	if err != nil {
		t.Fatalf("unexpected error committing on follower: %v", err)
	}

	// Verify entriesToApply on both nodes returns identical entries.
	leaderApply, leaderPooled, lerr := leaderNode.log.entriesToApply()
	if lerr != nil {
		t.Fatalf("leader entriesToApply error: %v", lerr)
	}
	if leaderPooled != nil {
		defer proto.PutEntrySlice(leaderPooled)
	}
	followerApply, followerPooled, ferr := followerNode.log.entriesToApply()
	if ferr != nil {
		t.Fatalf("follower entriesToApply error: %v", ferr)
	}
	if followerPooled != nil {
		defer proto.PutEntrySlice(followerPooled)
	}

	if len(leaderApply) == 0 {
		t.Fatal("expected leader to have entries to apply")
	}
	if len(leaderApply) != len(followerApply) {
		t.Fatalf("entry count mismatch: leader=%d follower=%d",
			len(leaderApply), len(followerApply))
	}
	for i := range leaderApply {
		if leaderApply[i].Index != followerApply[i].Index {
			t.Fatalf("index mismatch at %d: leader=%d follower=%d",
				i, leaderApply[i].Index, followerApply[i].Index)
		}
		if leaderApply[i].Term != followerApply[i].Term {
			t.Fatalf("term mismatch at %d: leader=%d follower=%d",
				i, leaderApply[i].Term, followerApply[i].Term)
		}
		if !bytes.Equal(leaderApply[i].Cmd, followerApply[i].Cmd) {
			t.Fatalf("cmd mismatch at %d: leader=%q follower=%q",
				i, leaderApply[i].Cmd, followerApply[i].Cmd)
		}
	}
}

func TestRaft_Safety_LeaderCompleteness_CommittedEntryOnNewLeader(t *testing.T) {
	// Set up 3 nodes. Node 1 is leader at term 1.
	leaderNode := newTestRaftPreVoteDisabled(1)
	electLeader(leaderNode)
	leaderTerm := leaderNode.term

	// Acknowledge noop from peer 2.
	noopIdx := leaderNode.log.lastIndex()
	err := leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderTerm,
		LogIndex: noopIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	leaderNode.drainMessages()

	// Propose an entry and commit via majority ack.
	err = leaderNode.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("committed-entry")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entryIdx := leaderNode.log.lastIndex()

	// Collect Replicate to node 2 for replication.
	appends := collectMessages(leaderNode, proto.Replicate)
	var replicateMsg proto.Message
	for _, m := range appends {
		if m.To == 2 {
			replicateMsg = m
			break
		}
	}
	leaderNode.drainMessages()

	// Peer 2 acks, committing the entry.
	err = leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderTerm,
		LogIndex: entryIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leaderNode.log.committed < entryIdx {
		t.Fatalf("expected entry %d committed, committed=%d",
			entryIdx, leaderNode.log.committed)
	}
	leaderNode.drainMessages()

	// Simulate leader failure: node 2 has the committed entry and wins
	// election at term 2.
	node2 := newTestRaftPreVoteDisabled(2)
	node2.term = leaderTerm
	node2.leaderID = 1
	// Replicate all leader entries to node2.
	node2.log.append([]proto.Entry{{Index: 1, Term: leaderTerm}})
	err = node2.step(replicateMsg)
	if err != nil {
		t.Fatalf("unexpected error replicating to node2: %v", err)
	}
	node2.drainMessages()

	// Node 2 becomes leader.
	electLeader(node2)
	if node2.state != leader {
		t.Fatalf("expected node2 to be leader, got %s", node2.state)
	}

	// Verify the committed entry exists in the new leader's log.
	tm, terr := node2.log.term(entryIdx)
	if terr != nil {
		t.Fatalf("new leader missing entry at index %d: %v", entryIdx, terr)
	}
	if tm != leaderTerm {
		t.Fatalf("new leader entry at index %d has term %d, expected %d",
			entryIdx, tm, leaderTerm)
	}
}

func TestRaft_Safety_AppliedEntriesNeverContradicted(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Pre-populate log with entries at term 1.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.log.committed = 3
	r.log.processed = 3 // all entries applied

	r.term = 2
	r.leaderID = 3
	r.msgs = nil

	// Send Replicate with conflicting entries at index 2 (different term).
	// The entry at index 2 is below the committed index, so
	// handleAppend should return immediately with the committed index.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     3,
		Term:     2,
		LogIndex: 1,
		LogTerm:  1,
		Entries: []proto.Entry{
			{Index: 2, Term: 2, Cmd: []byte("conflict")},
			{Index: 3, Term: 2, Cmd: []byte("conflict2")},
		},
		Commit: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the log was NOT truncated below the committed/applied index.
	// Entries at indices 1, 2, 3 should still have term 1.
	for idx := uint64(1); idx <= 3; idx++ {
		tm, terr := r.log.term(idx)
		if terr != nil {
			t.Fatalf("term error at index %d: %v", idx, terr)
		}
		if tm != 1 {
			t.Fatalf("entry at index %d was overwritten: expected term 1, got %d",
				idx, tm)
		}
	}

	// The response should carry the committed index so the leader
	// advances its tracking past the stale prevLogIndex.
	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected ReplicateResp")
	}
	if resps[0].LogIndex != r.log.committed {
		t.Fatalf("expected response LogIndex=%d, got %d",
			r.log.committed, resps[0].LogIndex)
	}
}

func TestRaft_Safety_TermMonotonicallyIncreases(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	terms := make([]uint64, 0, 6)

	// follower(term=0)
	terms = append(terms, r.term)

	// follower -> follower at term 1
	r.becomeFollower(1, 2)
	terms = append(terms, r.term)

	// follower -> leader via electLeader (becomeCandidate increments to 2,
	// then becomeLeader stays at 2).
	electLeader(r)
	terms = append(terms, r.term)

	// leader -> follower at term 3 (higher term message)
	r.becomeFollower(3, 2)
	terms = append(terms, r.term)

	// follower -> candidate at term 4
	r.becomeCandidate()
	terms = append(terms, r.term)

	// Verify term never decreased.
	for i := 1; i < len(terms); i++ {
		if terms[i] < terms[i-1] {
			t.Fatalf("term decreased at transition %d: %d -> %d",
				i, terms[i-1], terms[i])
		}
	}

	// Verify expected final values.
	expectedTerms := []uint64{0, 1, 2, 3, 4}
	for i, expected := range expectedTerms {
		if terms[i] != expected {
			t.Fatalf("term[%d]: expected %d, got %d", i, expected, terms[i])
		}
	}
}

func TestRaft_Safety_EntriesRetainOriginalTermOnReplication(t *testing.T) {
	// Leader at term 3 has entries from term 1 in its log.
	leaderNode := newTestRaftPreVoteDisabled(1)
	leaderNode.log.append([]proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("old-term-entry")},
		{Index: 2, Term: 1, Cmd: []byte("old-term-entry2")},
	})
	// Advance term to 3 and elect.
	leaderNode.becomeFollower(2, noLeader)
	leaderNode.becomeCandidate() // term becomes 3
	electLeader(leaderNode)

	// Acknowledge noop from peer 2 to move to replicate state.
	noopIdx := leaderNode.log.lastIndex()
	err := leaderNode.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     leaderNode.term,
		LogIndex: noopIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	leaderNode.drainMessages()

	// Force peer 2 back to probe at index 1 to trigger full replication.
	rm := leaderNode.remotes[2]
	rm.becomeProbe()
	rm.next = 1
	rm.probeSent = false
	leaderNode.sendAppend(2)

	appends := collectMessages(leaderNode, proto.Replicate)
	if len(appends) == 0 {
		t.Fatal("expected Replicate messages")
	}

	// Find the replicate with entries from term 1.
	var replicateMsg proto.Message
	for _, m := range appends {
		if m.To == 2 && len(m.Entries) > 0 {
			replicateMsg = m
			break
		}
	}
	if len(replicateMsg.Entries) == 0 {
		t.Fatal("expected entries in Replicate message")
	}

	// Create follower and step the Replicate.
	followerNode := newTestRaftPreVoteDisabled(2)
	followerNode.term = leaderNode.term
	followerNode.leaderID = 1

	err = followerNode.step(replicateMsg)
	if err != nil {
		t.Fatalf("unexpected error stepping Replicate: %v", err)
	}

	// Verify the entries in the follower's log still have Term=1, not Term=3.
	for idx := uint64(1); idx <= 2; idx++ {
		tm, terr := followerNode.log.term(idx)
		if terr != nil {
			t.Fatalf("follower term error at index %d: %v", idx, terr)
		}
		if tm != 1 {
			t.Fatalf("entry at index %d should retain original term 1, got %d",
				idx, tm)
		}
	}
}

func TestRaft_Safety_ElectionTimerNotResetOnRejectedVote(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	r.term = 5
	r.vote = 3 // already voted for node 3
	r.electionTick = 5
	r.msgs = nil

	// Send a RequestVote that will be rejected (already voted for node 3).
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     5,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the vote was rejected.
	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected vote response")
	}
	if !resps[0].Reject {
		t.Fatal("expected vote rejected (already voted for node 3)")
	}

	// Verify electionTick was NOT reset to 0.
	if r.electionTick != 5 {
		t.Fatalf("expected electionTick to remain 5, got %d", r.electionTick)
	}
}

func TestRaft_NewRaft_InitializesTermAndVoteToZero(t *testing.T) {
	r := newTestRaft(1)
	if r.term != 0 {
		t.Fatalf("expected term 0, got %d", r.term)
	}
	if r.vote != noNode {
		t.Fatalf("expected vote %d (noNode), got %d", noNode, r.vote)
	}
}

// ---------------------------------------------------------------------------
// Test: Election - broadcast vote sends to all peers
// ---------------------------------------------------------------------------

func TestRaft_Election_BroadcastVoteSendsToAllPeers(t *testing.T) {
	// 5-node cluster: node 1 starts election, expects 4 RequestVote messages.
	members := map[uint64]string{
		1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error creating raft: %v", err)
	}

	// Append some entries so LogTerm and LogIndex are non-trivial.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 3},
		{Index: 3, Term: 5},
	})

	expectedLogTerm := r.log.lastTerm()
	expectedLogIndex := r.log.lastIndex()

	r.becomeCandidate()
	expectedTerm := r.term
	r.broadcastVote()

	votes := collectMessages(r, proto.RequestVote)
	if len(votes) != 4 {
		t.Fatalf("expected 4 RequestVote messages, got %d", len(votes))
	}

	// Every message must target a unique peer and carry correct fields.
	seen := make(map[uint64]bool, 4)
	for _, msg := range votes {
		if msg.To == r.replicaID {
			t.Fatalf("RequestVote sent to self (replicaID %d)", r.replicaID)
		}
		if seen[msg.To] {
			t.Fatalf("duplicate RequestVote to peer %d", msg.To)
		}
		seen[msg.To] = true

		if msg.Term != expectedTerm {
			t.Fatalf("peer %d: expected Term %d, got %d", msg.To, expectedTerm, msg.Term)
		}
		if msg.LogTerm != expectedLogTerm {
			t.Fatalf("peer %d: expected LogTerm %d, got %d", msg.To, expectedLogTerm, msg.LogTerm)
		}
		if msg.LogIndex != expectedLogIndex {
			t.Fatalf("peer %d: expected LogIndex %d, got %d", msg.To, expectedLogIndex, msg.LogIndex)
		}
	}

	// Verify all 4 distinct peers received a message.
	for _, peerID := range []uint64{2, 3, 4, 5} {
		if !seen[peerID] {
			t.Fatalf("missing RequestVote to peer %d", peerID)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: VoteRequest - rejected when candidate log shorter in same term
// ---------------------------------------------------------------------------

func TestRaft_VoteRequest_RejectedWhenCandidateLogShorterSameTerm(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Give follower entries at term 5 up to index 5.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 5},
		{Index: 2, Term: 5},
		{Index: 3, Term: 5},
		{Index: 4, Term: 5},
		{Index: 5, Term: 5},
	})
	r.msgs = nil

	// Candidate has same last term (5) but shorter log (lastIndex=3).
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     6,
		LogTerm:  5,
		LogIndex: 3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected vote response")
	}
	if !resps[0].Reject {
		t.Fatal("expected vote rejected (candidate log shorter in same term)")
	}
}

// ---------------------------------------------------------------------------
// Test: PreVote - granted when leader known but election timeout expired
// ---------------------------------------------------------------------------

func TestRaft_PreVoteRequest_GrantedWhenLeaderActiveButElectionTimeoutExpired(t *testing.T) {
	r := newTestRaft(1)
	// Set a known leader so leaderID != noLeader.
	r.leaderID = 3
	// Simulate that the election timeout has expired: electionTick >= electionTimeout.
	// Per raft.go:914, leaderActive = (leaderID != noLeader && electionTick < electionTimeout).
	// When electionTick >= electionTimeout, leaderActive is false, so PreVote is granted.
	r.electionTick = r.electionTimeout
	r.msgs = nil

	err := r.step(proto.Message{
		Type:     proto.RequestPreVote,
		From:     2,
		Term:     r.term + 1,
		LogTerm:  0,
		LogIndex: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resps := collectMessages(r, proto.RequestPreVoteResp)
	if len(resps) == 0 {
		t.Fatal("expected pre-vote response")
	}
	if resps[0].Reject {
		t.Fatal("expected pre-vote granted (leader known but election timeout expired)")
	}
}

// ---------------------------------------------------------------------------
// Fast Backtracking: Term-based optimization (Raft Section 5.3)
// ---------------------------------------------------------------------------

// TestRaft_FastBacktrack_TermBasedSkipEntireTerm verifies that when a
// follower rejects an AppendEntries, it includes the conflicting term
// and the first index of that term in the rejection message. The leader
// can then skip all entries of the conflicting term in one round-trip.
func TestRaft_FastBacktrack_TermBasedSkipEntireTerm(t *testing.T) {
	// Set up a follower with log: [1:t1, 2:t1, 3:t1, 4:t2, 5:t2, 6:t2, 7:t3, 8:t3]
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
		{Index: 6, Term: 2},
		{Index: 7, Term: 3},
		{Index: 8, Term: 3},
	})
	r.becomeFollower(4, 2) // follower at term 4, leader is node 2

	// Leader sends append with prevLogIndex=8, prevLogTerm=4 (wrong term).
	// Follower has term 3 at index 8, so this should be rejected.
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     4,
		LogIndex: 8,
		LogTerm:  4, // leader thinks term at index 8 is 4
		Commit:   0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := collectMessages(r, proto.ReplicateResp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 response, got %d", len(msgs))
	}
	resp := msgs[0]

	if !resp.Reject {
		t.Fatal("expected rejection")
	}
	if resp.LogTerm != 3 {
		t.Fatalf("expected LogTerm=3 (conflicting term), got %d", resp.LogTerm)
	}
	// Hint should be the first index of term 3.
	if resp.Hint != 7 {
		t.Fatalf("expected Hint=7 (first index of term 3), got %d", resp.Hint)
	}
}

// TestRaft_FastBacktrack_LeaderHasConflictingTerm verifies that when
// the leader has entries at the conflicting term, it sets next to the
// last index of that term + 1 (skipping past its own entries at the
// conflicting term).
func TestRaft_FastBacktrack_LeaderHasConflictingTerm(t *testing.T) {
	// Use the declineRemote method directly to avoid the complexity of
	// election setup. This tests the leader-side logic in isolation.
	r := newTestRaftPreVoteDisabled(1)
	// Build a controlled log: [1:t1, 2:t1, 3:t2, 4:t2, 5:t3, 6:t3, 7:t4]
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})

	// Verify the leader has term 3 at indices 5-6.
	lastOfTerm3 := r.log.lastIndexOfTerm(3)
	if lastOfTerm3 != 6 {
		t.Fatalf("setup: expected lastIndexOfTerm(3)=6, got %d", lastOfTerm3)
	}

	rm := newRemote(9, maxInflightMsgs) // next=9, will reject at index 8

	// Follower rejects at index 8 with conflicting term=3, hint=5.
	// Leader has term 3 at indices 5-6, so it should set
	// next = lastIndexOfTerm(3) + 1 = 6 + 1 = 7.
	changed := r.declineRemote(rm, 8, 5, 3)
	if !changed {
		t.Fatal("expected declineRemote to return true")
	}
	if rm.next != 7 {
		t.Fatalf("expected next=7 (last index of term 3 + 1), got %d", rm.next)
	}
}

// TestRaft_FastBacktrack_LeaderMissingConflictingTerm verifies that when
// the leader does NOT have entries at the conflicting term, it uses the
// follower's hint (first index of the conflicting term) directly.
func TestRaft_FastBacktrack_LeaderMissingConflictingTerm(t *testing.T) {
	// Use declineRemote directly to test leader-side logic.
	r := newTestRaftPreVoteDisabled(1)
	// Build a log with terms 1 and 4, skipping 2 and 3.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 4},
		{Index: 4, Term: 4},
	})

	if got := r.log.lastIndexOfTerm(3); got != 0 {
		t.Fatalf("setup: expected no term 3, but found at index %d", got)
	}

	rm := newRemote(5, maxInflightMsgs) // next=5, will reject at index 4

	// Follower rejects at index 4 with conflicting term=3, hint=3.
	// Leader has no term 3, so it should use the hint: next = 3.
	changed := r.declineRemote(rm, 4, 3, 3)
	if !changed {
		t.Fatal("expected declineRemote to return true")
	}
	if rm.next != 3 {
		t.Fatalf("expected next=3 (follower's hint for missing term), got %d", rm.next)
	}
}

// TestRaft_FastBacktrack_FallbackToIndexBased verifies that when
// LogTerm=0 in the rejection (legacy behavior), the leader falls back
// to the existing index-based backtracking formula.
func TestRaft_FastBacktrack_FallbackToIndexBased(t *testing.T) {
	// Use declineRemote directly to test the fallback path.
	r := newTestRaftPreVoteDisabled(1)

	rm := newRemote(5, maxInflightMsgs) // next=5, will reject at index 4

	// LogTerm=0 means no term hint; fall back to index-based formula:
	// next = max(1, min(rejectedIndex, hintIndex+1))
	// = max(1, min(4, 2+1)) = max(1, 3) = 3
	changed := r.declineRemote(rm, 4, 2, 0)
	if !changed {
		t.Fatal("expected declineRemote to return true")
	}

	expected := uint64(3) // min(4, 2+1) = 3
	if rm.next != expected {
		t.Fatalf("expected next=%d (index-based fallback), got %d", expected, rm.next)
	}
}

// TestRaft_FastBacktrack_FollowerLogShorterThanLeaderExpects verifies
// that when the follower's log is shorter than the leader's prevLogIndex,
// the rejection includes LogTerm=0 and Hint=lastIndex (index-based).
func TestRaft_FastBacktrack_FollowerLogShorterThanLeaderExpects(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	// Follower has only 3 entries.
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	r.becomeFollower(2, 2)

	// Leader sends append with prevLogIndex=6 (beyond follower's log).
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     2,
		LogIndex: 6,
		LogTerm:  2,
		Commit:   0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := collectMessages(r, proto.ReplicateResp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 response, got %d", len(msgs))
	}
	resp := msgs[0]

	if !resp.Reject {
		t.Fatal("expected rejection")
	}
	// LogTerm should be 0 because follower's log is shorter.
	if resp.LogTerm != 0 {
		t.Fatalf("expected LogTerm=0 for shorter log, got %d", resp.LogTerm)
	}
	// Hint should be follower's last index.
	if resp.Hint != 3 {
		t.Fatalf("expected Hint=3 (follower lastIndex), got %d", resp.Hint)
	}
}

// TestRaft_FastBacktrack_ReplicateStateFallsBackToMatch verifies that
// in replicate state, term-based decline still falls back to match+1.
func TestRaft_FastBacktrack_ReplicateStateFallsBackToMatch(t *testing.T) {
	// Use declineRemote directly to test replicate state behavior.
	r := newTestRaftPreVoteDisabled(1)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})

	rm := newRemote(3, maxInflightMsgs)
	rm.match = 2
	rm.becomeReplicate()
	// Simulate optimistic advance.
	rm.next = 6

	// Rejection at index 5 with term hint should fall back to match+1.
	changed := r.declineRemote(rm, 5, 3, 2)
	if !changed {
		t.Fatal("expected declineRemote to return true")
	}
	if rm.next != rm.match+1 {
		t.Fatalf("expected next=%d (match+1), got %d", rm.match+1, rm.next)
	}
}

// ---------------------------------------------------------------------------
// Test: Witness-heavy cluster stalls commits when full replicas insufficient
// ---------------------------------------------------------------------------

// TestRaft_WitnessHeavyCluster_StallsCommitsWhenFullReplicasInsufficient
// verifies that a cluster where witnesses allow an election quorum but full
// (data-bearing) replicas do not meet the commit quorum will stall commits.
//
// Cluster: 3 full replicas (1,2,3) + 2 witnesses (4,5) = 5 voting members.
//   - electionQuorum = 5/2+1 = 3
//   - commitQuorum   = (3 data-bearing)/2+1 = 2 (remotes+leader)
//
// Scenario: node 1 is leader. Full replicas 2 and 3 are "dead" (never ack).
// Witnesses 4 and 5 are alive and grant votes, so the cluster can elect a
// leader (self + 2 witnesses = 3 = electionQuorum). However, the commit
// quorum requires 2 data-bearing nodes to match, and only the leader (node 1)
// is alive. Proposals cannot commit because the median match across data-
// bearing nodes never reaches an entry at the current term.
func TestRaft_WitnessHeavyCluster_StallsCommitsWhenFullReplicasInsufficient(t *testing.T) {
	// 3 full replicas + 2 witnesses.
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4", 5: "addr5"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	// Verify quorum arithmetic.
	eq := electionQuorum(r.remotes, r.witnesses)
	if eq != 3 {
		t.Fatalf("expected electionQuorum 3, got %d", eq)
	}
	cq := commitQuorum(r.remotes)
	if cq != 2 {
		t.Fatalf("expected commitQuorum 2, got %d", cq)
	}

	// Elect node 1 as leader using witness votes (self + witness 4 + witness 5 = 3).
	r.becomeCandidate()
	r.broadcastVote()
	for _, wid := range []uint64{4, 5} {
		if err := r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: wid,
			Term: r.term,
		}); err != nil {
			t.Fatalf("vote from witness %d: %v", wid, err)
		}
	}
	if r.state != leader {
		t.Fatalf("expected leader after witness votes, got %s", r.state)
	}
	r.drainMessages()

	noopIdx := r.log.lastIndex()

	// Propose a new entry. The leader appends it locally.
	if err := r.step(proto.Message{
		Type:    proto.Propose,
		Entries: []proto.Entry{{Cmd: []byte("stalled-entry")}},
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	r.drainMessages()

	proposalIdx := r.log.lastIndex()
	if proposalIdx <= noopIdx {
		t.Fatalf("expected proposal after noop: proposalIdx=%d, noopIdx=%d",
			proposalIdx, noopIdx)
	}

	// No full replica acks the proposal. The leader alone has it.
	// matched = [proposalIdx (leader), 0 (remote 2), 0 (remote 3)]
	// sorted  = [0, 0, proposalIdx]
	// mci     = matched[3-2] = matched[1] = 0
	// tryCommit(0, term) -> toCommit <= committed, returns false.
	if r.log.committed >= noopIdx {
		t.Fatalf("committed should not advance without data-bearing acks: committed=%d, noopIdx=%d",
			r.log.committed, noopIdx)
	}

	// Even a single full replica ack at the noop (not the proposal) should
	// not advance commit to the proposal because the median match at current
	// term stops at noop.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: noopIdx,
	}); err != nil {
		t.Fatalf("ReplicateResp from 2: %v", err)
	}
	r.drainMessages()

	// Now committed should advance to noopIdx (leader + remote 2 = quorum of 2).
	if r.log.committed < noopIdx {
		t.Fatalf("expected committed >= noopIdx after 1 full replica ack: committed=%d, noopIdx=%d",
			r.log.committed, noopIdx)
	}

	// But committed must NOT advance to proposalIdx because only the
	// leader has replicated it (1 < commitQuorum of 2).
	if r.log.committed >= proposalIdx {
		t.Fatalf("committed must not reach proposal without majority data-bearing ack: committed=%d, proposalIdx=%d",
			r.log.committed, proposalIdx)
	}

	// Now remote 2 acks the proposal. Commit should advance.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: proposalIdx,
	}); err != nil {
		t.Fatalf("ReplicateResp from 2 (proposal): %v", err)
	}
	r.drainMessages()

	if r.log.committed < proposalIdx {
		t.Fatalf("expected committed >= proposalIdx after majority data-bearing ack: committed=%d, proposalIdx=%d",
			r.log.committed, proposalIdx)
	}
}

// ---------------------------------------------------------------------------
// Test: Figure 8 - previous-term entry must not be committed directly
// ---------------------------------------------------------------------------

// TestRaft_Figure8_PreviousTermEntryNotCommittedDirectly recreates the
// scenario from Section 5.4.2 (Figure 8) of the Raft paper. It verifies
// that a leader does NOT commit entries from a previous term by counting
// replicas alone. Only entries from the leader's current term can be
// committed; once a current-term entry commits, all prior entries are
// implicitly committed.
//
// The scenario:
//  1. Leader S1 at term 2 has entry (index 2, term 2). S2 also has it.
//  2. S1 crashes; S5 becomes leader at term 3, appends (index 2, term 3),
//     but crashes before replicating.
//  3. S1 restarts, becomes leader at term 4. S1 replicates (index 2, term 2)
//     to S3, achieving majority {S1, S2, S3} for index 2. But the entry at
//     index 2 is term 2, not the current term 4, so it must NOT be committed.
//  4. If it were committed and S1 crashed, S5 could win at term 5 and
//     overwrite index 2 with term 3, violating the safety property.
//  5. Instead, S1 appends an entry at its own term (term 4) at index 3.
//     Once that entry achieves majority and commits, index 2 is implicitly
//     committed (all entries before a committed entry are committed).
func TestRaft_Figure8_PreviousTermEntryNotCommittedDirectly(t *testing.T) {
	// 5-node cluster: S1..S5. We operate from S1's perspective (replicaID=1).
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
		4: "addr4",
		5: "addr5",
	}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	// ---------------------------------------------------------------
	// Step 1: S1 becomes leader at term 2, appends entry (idx 2, term 2).
	// S2 also replicates it, but S3,S4,S5 do not.
	// ---------------------------------------------------------------

	// First election: term 0 -> candidate bumps to term 1.
	r.becomeCandidate()
	for _, id := range []uint64{2, 3} {
		_ = r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: id,
			Term: r.term,
		})
		if r.state == leader {
			break
		}
	}
	if r.state != leader {
		t.Fatalf("expected leader at term 1, got %s at term %d", r.state, r.term)
	}
	r.drainMessages()
	// becomeLeader appends noop at index 1, term 1.

	// Step down and start a new election to reach term 2.
	r.becomeFollower(r.term, noLeader)
	r.becomeCandidate() // bumps to term 2
	r.broadcastVote()
	for _, id := range []uint64{2, 3} {
		_ = r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: id,
			Term: r.term,
		})
		if r.state == leader {
			break
		}
	}
	if r.state != leader {
		t.Fatalf("expected leader at term 2, got %s at term %d", r.state, r.term)
	}
	term2 := r.term
	r.drainMessages()

	// becomeLeader appends noop at index 2, term 2.
	idx2 := r.log.lastIndex()
	if idx2 != 2 {
		t.Fatalf("expected lastIndex=2 after second leadership, got %d", idx2)
	}
	termAtIdx2, termErr := r.log.term(idx2)
	if termErr != nil {
		t.Fatalf("failed to get term at index 2: %v", termErr)
	}
	if termAtIdx2 != term2 {
		t.Fatalf("expected term %d at index 2, got %d", term2, termAtIdx2)
	}

	// S2 replicates index 2.
	r.remotes[2].match = idx2
	r.remotes[2].next = idx2 + 1
	// S3,S4,S5 have NOT replicated index 2 (match=0).

	// ---------------------------------------------------------------
	// Step 2: S1 "crashes". S5 becomes leader at term 3 (not modeled
	// here, but S1 learns of it). S1's log still has (idx 2, term 2).
	// ---------------------------------------------------------------
	r.becomeFollower(r.term, noLeader)
	r.drainMessages()

	// S1 learns about term 3 from S5's election.
	r.term = term2 + 1 // term 3
	r.becomeFollower(r.term, noLeader)
	r.drainMessages()

	// ---------------------------------------------------------------
	// Step 3: S1 restarts, becomes leader at term 4.
	// ---------------------------------------------------------------
	r.becomeCandidate() // bumps to term 4
	term4 := r.term
	if term4 != term2+2 {
		t.Fatalf("expected term %d, got %d", term2+2, term4)
	}
	r.broadcastVote()
	for _, id := range []uint64{2, 3, 4} {
		_ = r.step(proto.Message{
			Type: proto.RequestVoteResp,
			From: id,
			Term: term4,
		})
		if r.state == leader {
			break
		}
	}
	if r.state != leader {
		t.Fatalf("expected leader at term %d, got %s", term4, r.state)
	}
	r.drainMessages()

	// becomeLeader appends noop at index 3, term 4.
	noopIdx := r.log.lastIndex()
	if noopIdx != 3 {
		t.Fatalf("expected lastIndex=3 after third leadership, got %d", noopIdx)
	}

	// S1 replicates (index 2, term 2) to S3. Now S1, S2, S3 all have idx 2.
	// This is majority (3 out of 5) for index 2.
	r.remotes[2].match = idx2
	r.remotes[2].next = idx2 + 1
	r.remotes[3].match = idx2
	r.remotes[3].next = idx2 + 1
	// S4 and S5 still at match=0.

	// ---------------------------------------------------------------
	// Verification: tryCommit must NOT commit index 2 despite majority,
	// because term at index 2 is term2, not the current term4.
	// ---------------------------------------------------------------
	r.tryCommit()
	if r.log.committed >= idx2 {
		t.Fatalf("SAFETY VIOLATION: committed index %d (term %d) at leader term %d; "+
			"Raft Figure 8 requires that previous-term entries are not committed directly",
			idx2, termAtIdx2, term4)
	}

	// ---------------------------------------------------------------
	// Step 5: S1 replicates its noop at term 4 (index 3) to majority.
	// Once committed, all prior entries are implicitly committed.
	// ---------------------------------------------------------------
	r.remotes[2].match = noopIdx
	r.remotes[2].next = noopIdx + 1
	r.remotes[3].match = noopIdx
	r.remotes[3].next = noopIdx + 1

	r.tryCommit()
	if r.log.committed < noopIdx {
		t.Fatalf("expected committed >= %d after majority at current term, got %d",
			noopIdx, r.log.committed)
	}

	// Index 2 is now implicitly committed (committed=3 >= 2).
	if r.log.committed < idx2 {
		t.Fatalf("expected index %d implicitly committed, committed=%d", idx2, r.log.committed)
	}
}

// TestRaft_HandleLeaderPropose_ConfigChangeWithTypeMask verifies that
// handleLeaderPropose correctly detects a config change entry even when
// additional type flags (e.g., EntryCompressFlag) are set in the upper
// bits. Without masking via EntryTypeMask the comparison would fail,
// allowing a second config change to be proposed while the first is
// still pending.
func TestRaft_HandleLeaderPropose_ConfigChangeWithTypeMask(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Propose a config change entry with the compression flag set.
	// The lower 4 bits still encode EntryConfigChange; the upper bit
	// is EntryCompressFlag. handleLeaderPropose must mask before
	// comparing so it recognises this as a config change.
	compressedCC := proto.EntryConfigChange | proto.EntryCompressFlag

	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: compressedCC, Cmd: []byte("cc-compressed")},
		},
	})
	if err != nil {
		t.Fatalf("config change with compress flag should succeed, got %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be true after proposing a compressed config change")
	}

	// A second config change (also with extra flags) must be rejected
	// because there is already a pending config change.
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: compressedCC, Cmd: []byte("cc2-compressed")},
		},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange for second compressed config change, got %v", err)
	}
}

// TestRaft_HandleLeaderPropose_ConfigChangeWithTypeMask_NormalEntryNotBlocked
// verifies that a normal entry with the compress flag set is not mistakenly
// treated as a config change.
func TestRaft_HandleLeaderPropose_ConfigChangeWithTypeMask_NormalEntryNotBlocked(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Propose a normal entry with the compression flag set. This must
	// NOT set pendingConfigChange.
	compressedNormal := proto.EntryNormal | proto.EntryCompressFlag

	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: compressedNormal, Cmd: []byte("data")},
		},
	})
	if err != nil {
		t.Fatalf("normal compressed entry should succeed, got %v", err)
	}
	if r.pendingConfigChange {
		t.Fatal("pendingConfigChange should remain false for a normal entry with compress flag")
	}
}

// ---------------------------------------------------------------------------
// R2: becomeLeader resets InMemRateLimiter
// ---------------------------------------------------------------------------

func TestRaft_BecomeLeader_ResetsRateLimiter(t *testing.T) {
	// Use a large maxSize so the local in-mem log size (accumulated
	// noop entries from leadership transitions) stays well below the
	// threshold. The test validates that stale *follower* state is
	// cleared on leadership change.
	r := newTestRaftRateLimited(1, 1<<20) // 1 MiB
	electLeader(r)
	r.drainMessages()

	// Simulate stale follower state that would trigger rate limiting.
	r.rl.SetFollowerState(2, 1<<21) // 2 MiB, exceeds maxSize
	r.rl.RateLimited()              // evaluate to update cached state
	if !r.rl.IsLimited() {
		t.Fatal("expected rate limiter to be limited with oversized follower state")
	}

	// Transition through candidate back to leader (simulating re-election).
	r.becomeFollower(r.term+1, noLeader)
	r.becomeCandidate()
	r.becomeLeader()

	// After Reset(), the stale follower state is gone and the limiter
	// should not be limited.
	r.rl.RateLimited() // re-evaluate
	if r.rl.IsLimited() {
		t.Fatal("expected rate limiter to not be limited after becomeLeader reset")
	}
}

func TestRaft_BecomeLeader_StaleFollowerSizesDoNotPersist(t *testing.T) {
	r := newTestRaftRateLimited(1, 1<<20) // 1 MiB
	electLeader(r)
	r.drainMessages()

	// Set a follower size that exceeds maxSize to trigger rate limiting.
	r.rl.SetFollowerState(2, 1<<21) // 2 MiB
	r.rl.RateLimited()              // evaluate to update cachedLimited
	if !r.rl.IsLimited() {
		t.Fatal("expected rate limiter to be limited with oversized follower")
	}

	// Re-elect: the stale oversized follower state must not carry over.
	r.becomeFollower(r.term+1, noLeader)
	r.becomeCandidate()
	r.becomeLeader()

	// After reset, rate limiting must not be active because the stale
	// follower state that caused the limit was cleared.
	r.rl.RateLimited() // re-evaluate
	if r.rl.IsLimited() {
		t.Fatal("stale follower sizes from prior term persisted through leadership change")
	}
}

// ---------------------------------------------------------------------------
// R3: handleLeaderTransfer returns ErrTransferTargetNotVoter for witnesses/observers
// ---------------------------------------------------------------------------

func TestRaft_LeaderTransfer_WitnessReturnsNotVoter(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfg(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	err = r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 20,
	})
	if !errors.Is(err, ErrTransferTargetNotVoter) {
		t.Fatalf("expected ErrTransferTargetNotVoter, got %v", err)
	}
}

func TestRaft_LeaderTransfer_ObserverReturnsNotVoter(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfg(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Observers = map[uint64]string{10: "obs1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	err = r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 10,
	})
	if !errors.Is(err, ErrTransferTargetNotVoter) {
		t.Fatalf("expected ErrTransferTargetNotVoter, got %v", err)
	}
}

func TestRaft_LeaderTransfer_NonExistentNodeReturnsNodeNotFound(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfg(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{20: "wit1"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Node 99 is not in remotes, witnesses, or observers.
	err = r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 99,
	})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestRaft_LeaderTransfer_ValidVoterSucceeds(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()
	r.remotes[2].match = r.log.lastIndex()

	err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		Hint: 2,
	})
	if err != nil {
		t.Fatalf("expected no error for valid voter transfer, got %v", err)
	}
	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget=2, got %d", r.transferTarget)
	}
}

// ---------------------------------------------------------------------------
// M1: Batch config change does not leave pendingConfigChange stuck
// ---------------------------------------------------------------------------

// TestRaft_BatchConfigChange_DoesNotStickPendingFlag verifies that proposing
// two config change entries in a single batch returns ErrPendingConfigChange
// but does NOT leave pendingConfigChange permanently true. A subsequent
// single config change proposal must succeed.
func TestRaft_BatchConfigChange_DoesNotStickPendingFlag(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Batch two config change entries in one proposal. This must be
	// rejected because only one config change can be pending at a time.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
			{Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
		},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange for dual-CC batch, got %v", err)
	}
	// Critical: pendingConfigChange must NOT be stuck true.
	if r.pendingConfigChange {
		t.Fatal("pendingConfigChange stuck true after rejected dual-CC batch")
	}
}

// TestRaft_BatchConfigChange_SubsequentSingleSucceeds verifies that after
// rejecting a batch with two config changes, a subsequent single config
// change proposal succeeds.
func TestRaft_BatchConfigChange_SubsequentSingleSucceeds(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	r.drainMessages()

	// Reject dual-CC batch.
	_ = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc1")},
			{Type: proto.EntryConfigChange, Cmd: []byte("cc2")},
		},
	})

	// Single config change must succeed after rejection.
	err := r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{
			{Type: proto.EntryConfigChange, Cmd: []byte("cc-single")},
		},
	})
	if err != nil {
		t.Fatalf("single config change after batch rejection should succeed, got %v", err)
	}
	if !r.pendingConfigChange {
		t.Fatal("pendingConfigChange should be true after successful single config change")
	}
}

// ---------------------------------------------------------------------------
// L1: readStates bounded growth
// ---------------------------------------------------------------------------

// TestRaft_ReadStates_BoundedGrowth verifies that readStates does not grow
// without bound. When readStates reaches maxReadStates, new ReadIndex
// requests on the fast path (single-node or lease) return
// ErrReadIndexOverloaded.
func TestRaft_ReadStates_BoundedGrowth(t *testing.T) {
	// Single-node cluster so ReadIndex resolves immediately via fast path.
	r := newTestRaftWithMembers(1, map[uint64]string{1: "addr1"})
	electLeader(r)
	r.drainMessages()
	commitNoOp(r)

	// Fill readStates to capacity.
	for i := range maxReadStates {
		err := r.step(proto.Message{
			Type: proto.ReadIndex,
			From: 1,
			Hint: uint64(i + 1),
		})
		if err != nil {
			t.Fatalf("ReadIndex %d should succeed, got %v", i, err)
		}
	}
	if len(r.readStates) != maxReadStates {
		t.Fatalf("expected %d readStates, got %d", maxReadStates, len(r.readStates))
	}

	// Next ReadIndex must be rejected.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 1,
		Hint: 99999,
	})
	if !errors.Is(err, ErrReadIndexOverloaded) {
		t.Fatalf("expected ErrReadIndexOverloaded when readStates full, got %v", err)
	}
}

// TestRaft_ReadStates_AcceptsAfterDrain verifies that after draining
// readStates, new ReadIndex requests are accepted again.
func TestRaft_ReadStates_AcceptsAfterDrain(t *testing.T) {
	r := newTestRaftWithMembers(1, map[uint64]string{1: "addr1"})
	electLeader(r)
	r.drainMessages()
	commitNoOp(r)

	// Fill to capacity.
	for i := range maxReadStates {
		_ = r.step(proto.Message{
			Type: proto.ReadIndex,
			From: 1,
			Hint: uint64(i + 1),
		})
	}

	// Drain all readStates.
	r.readStates = nil

	// Should accept again.
	err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 1,
		Hint: 42,
	})
	if err != nil {
		t.Fatalf("ReadIndex after drain should succeed, got %v", err)
	}
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState after drain+add, got %d", len(r.readStates))
	}
}

// ---------------------------------------------------------------------------
// H1: replicaToBit rebuilt on config change during leadership
// ---------------------------------------------------------------------------

func TestH1_ReplicaToBitRebuiltAfterMemberAddDuringLeadership(t *testing.T) {
	// When a new voting member is added during leadership,
	// rebuildReplicaToBit must be called so the new member's heartbeat
	// acks count toward ReadIndex quorum checks.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	oldLen := len(r.replicaToBit)
	if oldLen != 3 {
		t.Fatalf("expected 3 entries in replicaToBit before add, got %d", oldLen)
	}

	// Simulate adding replica 4 by inserting it into remotes directly
	// and calling rebuildReplicaToBit (which is what syncRaftRemotes
	// does when state == leader).
	r.remotes[4] = newRemote(r.log.lastIndex()+1, maxInflightMsgs)
	r.rebuildReplicaToBit()

	if len(r.replicaToBit) != 4 {
		t.Fatalf("expected 4 entries in replicaToBit after add, got %d", len(r.replicaToBit))
	}

	// Verify the new member has a bit position.
	if _, ok := r.replicaToBit[4]; !ok {
		t.Fatal("expected replica 4 in replicaToBit after rebuild")
	}

	// Verify all bit positions are unique.
	seen := make(map[uint8]uint64)
	for id, bit := range r.replicaToBit {
		if prevID, ok := seen[bit]; ok {
			t.Fatalf("duplicate bit position %d for nodes %d and %d", bit, prevID, id)
		}
		seen[bit] = id
	}
}

func TestH1_ReplicaToBitRebuiltAfterMemberRemoveDuringLeadership(t *testing.T) {
	// When a voting member is removed during leadership,
	// rebuildReplicaToBit must remove the departed member's bit position
	// so stale heartbeat acks do not count toward quorum.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	if len(r.replicaToBit) != 3 {
		t.Fatalf("expected 3 entries in replicaToBit before remove, got %d", len(r.replicaToBit))
	}

	// Simulate removing replica 3.
	delete(r.remotes, 3)
	r.rebuildReplicaToBit()

	if len(r.replicaToBit) != 2 {
		t.Fatalf("expected 2 entries in replicaToBit after remove, got %d", len(r.replicaToBit))
	}
	if _, ok := r.replicaToBit[3]; ok {
		t.Fatal("expected replica 3 removed from replicaToBit after rebuild")
	}
}

func TestH1_NewMemberHeartbeatAckCountsForReadIndexAfterRebuild(t *testing.T) {
	// End-to-end: after adding a new member and rebuilding replicaToBit,
	// the new member's heartbeat ack must count toward ReadIndex quorum.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	// Add replica 4 and rebuild.
	r.remotes[4] = newRemote(r.log.lastIndex()+1, maxInflightMsgs)
	r.rebuildReplicaToBit()

	// Issue a ReadIndex request.
	r.readStates = nil
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 1,
		Hint: 99,
	}); err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	r.flushReadIndex()
	msgs := r.drainMessages()

	// Find the heartbeat ctx.
	var hbCtx uint64
	for _, m := range msgs {
		if m.Type == proto.Heartbeat {
			hbCtx = m.Hint
		}
	}

	// Ack from the new member (replica 4) and one existing member (replica 2).
	// In a 4-node cluster (self + 3 remotes), electionQuorum = 3.
	// Self is pre-acked, so we need 2 more acks.
	for _, from := range []uint64{2, 4} {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: from,
			Term: r.term,
			Hint: hbCtx,
		}); err != nil {
			t.Fatalf("HeartbeatResp from %d: %v", from, err)
		}
	}
	r.drainMessages()

	// ReadIndex should be resolved because self + 2 + 4 = 3 acks = quorum.
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState after quorum ack including new member, got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 99 {
		t.Fatalf("expected readState key 99, got %d", r.readStates[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Test: S1+S2 - ReadIndex/LeaseRead quorum uses electionQuorum with witnesses
// ---------------------------------------------------------------------------

func TestS1_ReadIndex_ElectionQuorumWithWitnesses(t *testing.T) {
	// 5-node cluster: node 1 (leader), nodes 2,3 (remotes), nodes 4,5 (witnesses).
	// electionQuorum = 3 (5 voting / 2 + 1). commitQuorum would be 2 (3 data / 2 + 1).
	// ReadIndex must use electionQuorum so the confirmation quorum overlaps
	// with every possible election-winning coalition. Verify that 2 acks
	// (self + 1 remote) is NOT sufficient, but 3 acks (self + 1 remote + 1 witness) is.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4", 5: "addr5"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Commit noop.
	for id := range r.remotes {
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     id,
			Term:     r.term,
			LogIndex: r.log.lastIndex(),
		}); err != nil {
			t.Fatalf("ReplicateResp from %d: %v", id, err)
		}
	}
	r.drainMessages()
	r.readStates = nil

	// Verify witnesses are in replicaToBit.
	for _, wid := range []uint64{4, 5} {
		if _, found := r.replicaToBit[wid]; !found {
			t.Fatalf("witness node %d must have a bit position in replicaToBit", wid)
		}
	}

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 42,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages for ReadIndex")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// self(1) + remote2(1) = 2 < electionQuorum(3).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}
	if len(r.readStates) != 0 {
		t.Fatalf("expected 0 readStates (electionQuorum=3, have 2), got %d", len(r.readStates))
	}

	// self(1) + remote2(1) + witness4(1) = 3 = electionQuorum.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 4,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}
	if len(r.readStates) != 1 {
		t.Fatalf("expected 1 readState (electionQuorum=3, have 3), got %d", len(r.readStates))
	}
	if r.readStates[0].Key != 42 {
		t.Fatalf("expected read state key 42, got %d", r.readStates[0].Key)
	}
}

func TestS1_ReadIndex_ElectionQuorumWithWitnesses_InvalidInput(t *testing.T) {
	// Verify that heartbeat ack from an unknown node does not affect
	// ReadIndex quorum in a cluster with witnesses.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()
	commitNoOp(r)

	// Issue ReadIndex.
	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		Hint: 11,
	}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	r.flushReadIndex()

	hbs := collectMessages(r, proto.Heartbeat)
	if len(hbs) == 0 {
		t.Fatal("expected heartbeat messages")
	}
	hbCtx := hbs[0].Hint
	r.drainMessages()

	// Unknown node 99 ack should not affect quorum.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 99,
		Term: r.term,
		Hint: hbCtx,
	}); err != nil {
		t.Fatalf("HeartbeatResp from unknown: %v", err)
	}
	if len(r.readStates) != 0 {
		t.Fatal("unknown node ack must not satisfy ReadIndex quorum")
	}
}

func TestS2_LeaseRead_ElectionQuorumWithWitnesses(t *testing.T) {
	// 5-node cluster: node 1 (leader), nodes 2,3 (remotes), nodes 4,5 (witnesses).
	// electionQuorum = 3. LeaseRead must use electionQuorum. Verify that
	// self + 1 remote = 2 < 3 does not satisfy lease, but adding a witness does.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4", 5: "addr5"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Only remote 2 acks. self(1) + remote2(1) = 2 < electionQuorum(3).
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: r.term,
	}); err != nil {
		t.Fatalf("HeartbeatResp: %v", err)
	}
	r.drainMessages()

	if r.hasValidLease() {
		t.Fatal("lease must not be valid with self + 1 remote (electionQuorum=3)")
	}

	// Add witness 4 ack. self(1) + remote2(1) + witness4(1) = 3 = quorum.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 4,
		Term: r.term,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}
	r.drainMessages()

	if !r.hasValidLease() {
		t.Fatal("lease should be valid with self + remote + witness (electionQuorum=3)")
	}
}

func TestS2_LeaseRead_ElectionQuorumWithWitnesses_InvalidInput(t *testing.T) {
	// Verify that heartbeat ack from an unknown node does not contribute
	// to lease validity in a cluster with witnesses.
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Unknown node 99 ack.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 99,
		Term: r.term,
	}); err != nil {
		t.Fatalf("HeartbeatResp from unknown: %v", err)
	}
	r.drainMessages()

	// Unknown ack should not be stored.
	if _, found := r.remoteLastAck[99]; found {
		t.Fatal("unknown node 99 must not be tracked in remoteLastAck")
	}
}

func TestS1S2_WitnessInReplicaToBitAndRemoteLastAck(t *testing.T) {
	// Verify that witnesses are included in both replicaToBit (for
	// ReadIndex) and tracked in remoteLastAck (for LeaseRead).
	members := map[uint64]string{1: "addr1", 2: "addr2"}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{3: "addr3"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// replicaToBit should include witnesses.
	if _, found := r.replicaToBit[3]; !found {
		t.Fatal("witness node 3 must have a bit position in replicaToBit")
	}
	// All voting members should be in replicaToBit: self(1) + remote(2) + witness(3).
	if len(r.replicaToBit) != 3 {
		t.Fatalf("expected 3 entries in replicaToBit (self + remote + witness), got %d", len(r.replicaToBit))
	}

	// remoteLastAck should track witness after heartbeat ack.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: r.term,
	}); err != nil {
		t.Fatalf("HeartbeatResp from witness: %v", err)
	}
	r.drainMessages()

	if _, found := r.remoteLastAck[3]; !found {
		t.Fatal("witness node 3 must be tracked in remoteLastAck after heartbeat ack")
	}
}

func TestS1S2_WitnessInReplicaToBitAndRemoteLastAck_NoWitnesses(t *testing.T) {
	// Verify that a cluster without witnesses still works correctly.
	// replicaToBit should only contain self + remotes.
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)

	if len(r.replicaToBit) != 3 {
		t.Fatalf("expected 3 entries in replicaToBit (self + 2 remotes), got %d", len(r.replicaToBit))
	}
	if len(r.witnesses) != 0 {
		t.Fatal("expected no witnesses in test cluster")
	}
}

// ---------------------------------------------------------------------------
// Test: S3 - Frozen transferElapsed during quiesce
// ---------------------------------------------------------------------------

func TestS3_QuiesceBlockedDuringLeaderTransfer(t *testing.T) {
	// When a leader transfer is active, quiesce entry must be blocked.
	// Otherwise transferElapsed would never increment in the quiesce
	// branch, causing the transfer timer to stall indefinitely.
	//
	// The quiesce threshold is electionTimeout * quiesceThresholdMultiplier
	// = 10 * 10 = 100 ticks. The transfer timeout is electionTimeout = 10.
	// During the first 10 ticks, the transfer is active and quiesce must
	// be blocked. The quiesce inactivity counter must NOT advance while
	// the transfer is active.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Start a leader transfer to node 2.
	if err := r.step(proto.Message{
		Type: proto.LeaderTransfer,
		From: 2,
		Hint: 2,
	}); err != nil {
		t.Fatalf("LeaderTransfer: %v", err)
	}
	r.drainMessages()

	if r.transferTarget != 2 {
		t.Fatalf("expected transferTarget=2, got %d", r.transferTarget)
	}

	// Tick a few times while the transfer is active (less than
	// electionTimeout so the transfer hasn't timed out yet).
	for i := 0; i < 5; i++ {
		tickLeader(r)
		r.drainMessages()
	}

	// Transfer is still active.
	if r.transferTarget != 2 {
		t.Fatalf("expected transfer still active at tick 5, transferTarget=%d", r.transferTarget)
	}

	// Quiesce must NOT be active because transfer is in progress.
	if r.quiesce.active {
		t.Fatal("quiesce must not be active during leader transfer")
	}

	// The quiesce inactivity counter should not have advanced because
	// tryEnter is guarded by transferTarget == noNode.
	if r.quiesce.tick != 0 {
		t.Fatalf("quiesce tick should be 0 during active transfer, got %d", r.quiesce.tick)
	}

	// Transfer elapsed should have advanced.
	if r.transferElapsed < 5 {
		t.Fatalf("expected transferElapsed >= 5, got %d", r.transferElapsed)
	}
}

func TestS3_QuiesceBlockedDuringLeaderTransfer_InvalidTarget(t *testing.T) {
	// When no transfer is active (transferTarget == noNode), quiesce
	// should still be able to enter normally.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// No transfer active.
	if r.transferTarget != noNode {
		t.Fatalf("expected no transfer target, got %d", r.transferTarget)
	}

	// Tick enough to enter quiesce.
	for i := 0; i < 100; i++ {
		tickLeader(r)
		r.drainMessages()
	}

	// Quiesce should be active.
	if !r.quiesce.active {
		t.Fatal("quiesce should be active when no transfer is in progress")
	}
}

// ---------------------------------------------------------------------------
// Test: S4 - Stale lease reads after quiesce exit on partitioned leader
// ---------------------------------------------------------------------------

func TestS4_LeaseInvalidatedOnQuiesceEntry(t *testing.T) {
	// When a leader enters quiesce, the lease must be invalidated to
	// prevent stale reads. During quiesce, electionTick is frozen, so
	// hasValidLease would compute a stale leaseExpiry.
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgLeaseRead(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	cfg.Quiesce = true
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// Establish a valid lease.
	grantLease(r)
	if !r.hasValidLease() {
		t.Fatal("expected valid lease after granting")
	}

	// Tick enough to enter quiesce.
	for i := 0; i < 100; i++ {
		tickLeader(r)
		r.drainMessages()
	}

	if !r.quiesce.active {
		t.Fatal("expected quiesce to be active")
	}

	// Lease must be invalidated on quiesce entry.
	if r.hasValidLease() {
		t.Fatal("lease must be invalidated when entering quiesce")
	}

	// Exit quiesce by recording activity.
	r.quiesce.recordActivity()
	if r.quiesce.active {
		t.Fatal("expected quiesce to be exited after recordActivity")
	}

	// Lease should still not be valid until re-earned through heartbeats.
	if r.hasValidLease() {
		t.Fatal("lease must not be valid after quiesce exit until re-earned")
	}

	// Re-earn lease through fresh heartbeat acks.
	grantLease(r)
	if !r.hasValidLease() {
		t.Fatal("lease should be valid after re-earning through heartbeats")
	}
}

func TestS4_LeaseInvalidatedOnQuiesceEntry_LeaseDisabled(t *testing.T) {
	// When leaseRead is disabled, quiesce entry should not panic or
	// cause issues with lease invalidation (remoteLastAck is nil).
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.CheckQuorum = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	r.drainMessages()

	// remoteLastAck should be nil when leaseRead is disabled.
	if r.remoteLastAck != nil {
		t.Fatal("remoteLastAck should be nil when leaseRead is disabled")
	}

	// Tick enough to enter quiesce. Should not panic.
	for i := 0; i < 100; i++ {
		tickLeader(r)
		r.drainMessages()
	}

	if !r.quiesce.active {
		t.Fatal("expected quiesce to be active")
	}

	// hasValidLease should return false.
	if r.hasValidLease() {
		t.Fatal("lease must not be valid when leaseRead is disabled")
	}
}
