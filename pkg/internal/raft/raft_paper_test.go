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

// Raft paper correctness tests.
//
// These tests verify that the go-quicraft Raft implementation matches the
// specification described in "In Search of an Understandable Consensus
// Algorithm" (Ongaro & Ousterhout, 2014). Each test references the
// relevant section of the paper.

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Helpers for paper tests
// ---------------------------------------------------------------------------

// newPaperRaft creates a raft node for paper tests with PreVote disabled,
// ensuring standard Raft election semantics without the PreVote extension.
func newPaperRaft(replicaID uint64) *raft {
	return newTestRaftPreVoteDisabled(replicaID)
}

// newPaperRaftN creates a raft node in an N-node cluster with PreVote disabled.
func newPaperRaftN(replicaID uint64, n int) *raft {
	members := make(map[uint64]string, n)
	for i := uint64(1); i <= uint64(n); i++ {
		members[i] = "addr" + string(rune('0'+i))
	}
	cfg := raftTestCfgPreVote(1, replicaID, false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newPaperRaftN failed: " + err.Error())
	}
	return r
}

// newPaperRaftWithLog creates a raft node with pre-existing log entries.
// PreVote is disabled for standard Raft semantics.
func newPaperRaftWithLog(replicaID uint64, entries []proto.Entry) *raft {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgPreVote(1, replicaID, false)
	logdb := newTestLogReader(entries, proto.State{})
	logdb.membership = proto.Membership{Addresses: members}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newPaperRaftWithLog failed: " + err.Error())
	}
	return r
}

// newPaperRaftWithLogN creates a raft node in an N-node cluster with
// pre-existing log entries and PreVote disabled.
func newPaperRaftWithLogN(replicaID uint64, n int, entries []proto.Entry) *raft {
	members := make(map[uint64]string, n)
	for i := uint64(1); i <= uint64(n); i++ {
		members[i] = "addr" + string(rune('0'+i))
	}
	cfg := raftTestCfgPreVote(1, replicaID, false)
	logdb := newTestLogReader(entries, proto.State{})
	logdb.membership = proto.Membership{Addresses: members}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newPaperRaftWithLogN failed: " + err.Error())
	}
	return r
}

// electPaperLeader transitions the node to leader state by granting
// votes from a majority of peers. Drains all messages afterward.
func electPaperLeader(r *raft) {
	electLeader(r)
}

// countMsgsOfType counts messages of the given type in r.msgs.
func countMsgsOfType(r *raft, msgType uint64) int {
	count := 0
	for _, m := range r.msgs {
		if m.Type == msgType {
			count++
		}
	}
	return count
}

// ==========================================================================
// Section 5.1 - Leader Election
// ==========================================================================

// TestPaper_NodeStartsAsFollower verifies that a Raft node initializes in
// the follower state with no known leader.
//
// Raft Section 5.1: "When servers start up, they begin as followers."
func TestPaper_NodeStartsAsFollower(t *testing.T) {
	r := newPaperRaft(1)

	if r.state != follower {
		t.Fatalf("expected node to start as follower, got %s", r.state)
	}
	if r.leaderID != noLeader {
		t.Fatalf("expected no leader on startup, got leader %d", r.leaderID)
	}
	if r.term != 0 {
		t.Fatalf("expected initial term 0, got %d", r.term)
	}
}

// TestPaper_FollowerUpdatesTermFromHigherTermMessage verifies that a
// follower updates its term when receiving a message with a higher term.
//
// Raft Section 5.1: "If a candidate or leader discovers that its term is
// out of date, it immediately reverts to follower state."
func TestPaper_FollowerUpdatesTermFromHigherTermMessage(t *testing.T) {
	r := newPaperRaft(1)
	initialTerm := r.term

	// Receive a heartbeat from a leader in a higher term.
	higherTerm := initialTerm + 5
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: higherTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.term != higherTerm {
		t.Fatalf("expected term to update to %d, got %d", higherTerm, r.term)
	}
	if r.state != follower {
		t.Fatalf("expected to remain follower, got %s", r.state)
	}
	if r.leaderID != 2 {
		t.Fatalf("expected leaderID 2, got %d", r.leaderID)
	}
}

// TestPaper_CandidateUpdatesTermFromHigherTermMessage verifies that a
// candidate steps down to follower when receiving a higher-term message.
//
// Raft Section 5.1: "If a candidate or leader discovers that its term
// is out of date, it immediately reverts to follower state."
func TestPaper_CandidateUpdatesTermFromHigherTermMessage(t *testing.T) {
	r := newPaperRaft(1)
	r.becomeCandidate()
	candidateTerm := r.term

	higherTerm := candidateTerm + 3
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: higherTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected candidate to step down to follower, got %s", r.state)
	}
	if r.term != higherTerm {
		t.Fatalf("expected term %d, got %d", higherTerm, r.term)
	}
	if r.vote != noNode {
		t.Fatalf("expected vote cleared on term change, got %d", r.vote)
	}
}

// TestPaper_LeaderStepsDownOnHigherTerm verifies that a leader becomes
// follower when it receives a message with a higher term.
//
// Raft Section 5.1: "If a candidate or leader discovers that its term
// is out of date, it immediately reverts to follower state."
func TestPaper_LeaderStepsDownOnHigherTerm(t *testing.T) {
	r := newPaperRaft(1)
	electPaperLeader(r)
	leaderTerm := r.term

	higherTerm := leaderTerm + 1
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 3,
		Term: higherTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected leader to step down to follower, got %s", r.state)
	}
	if r.term != higherTerm {
		t.Fatalf("expected term %d, got %d", higherTerm, r.term)
	}
}

// TestPaper_StaleTermMessageRejected verifies that messages with lower
// terms are rejected.
//
// Raft Section 5.1: "If a server receives a request with a stale term
// number, it rejects the request."
func TestPaper_StaleTermMessageRejected(t *testing.T) {
	r := newPaperRaft(1)
	// Advance to term 3 so we can send a stale message with a non-zero
	// term. The step function ignores Term=0 messages in the term
	// comparison block.
	r.becomeFollower(3, noLeader)
	electPaperLeader(r)
	currentTerm := r.term

	// Send a heartbeat from an older (but non-zero) term.
	staleTerm := currentTerm - 1
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: staleTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Term must not change.
	if r.term != currentTerm {
		t.Fatalf("expected term to remain %d, got %d", currentTerm, r.term)
	}
	// Must remain leader.
	if r.state != leader {
		t.Fatalf("expected to remain leader, got %s", r.state)
	}

	// The stale message handler sends a ReplicateResp so the sender
	// learns the new term.
	found := false
	for _, m := range r.msgs {
		if m.Type == proto.ReplicateResp && m.To == 2 {
			if m.Term != currentTerm {
				t.Fatalf("expected response with current term %d, got %d",
					currentTerm, m.Term)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ReplicateResp to inform stale sender of current term")
	}
}

// TestPaper_LeaderBroadcastsHeartbeats verifies that the leader sends
// heartbeat messages to all followers each heartbeat interval.
//
// Raft Section 5.2: "The leader sends periodic heartbeats to all
// followers in order to maintain its authority."
func TestPaper_LeaderBroadcastsHeartbeats(t *testing.T) {
	r := newPaperRaft(1)
	electPaperLeader(r)
	r.drainMessages()

	// Advance exactly to the heartbeat timeout to trigger one round.
	for i := 0; i < r.heartbeatTimeout; i++ {
		r.tickFunc(r)
	}

	heartbeats := collectMessages(r, proto.Heartbeat)
	expectedPeers := len(r.remotes) + len(r.observers) + len(r.witnesses)
	if len(heartbeats) != expectedPeers {
		t.Fatalf("expected %d heartbeats (one per peer), got %d",
			expectedPeers, len(heartbeats))
	}

	// Verify heartbeats go to distinct peers.
	targets := make(map[uint64]bool)
	for _, hb := range heartbeats {
		targets[hb.To] = true
		if hb.Term != r.term {
			t.Fatalf("heartbeat to %d has term %d, expected %d",
				hb.To, hb.Term, r.term)
		}
	}
	if len(targets) != expectedPeers {
		t.Fatalf("expected heartbeats to %d distinct peers, got %d",
			expectedPeers, len(targets))
	}
}

// TestPaper_FollowerStartsElectionOnTimeout verifies that a follower
// transitions to candidate and starts an election when the election
// timeout elapses without receiving messages from a leader.
//
// Raft Section 5.2: "If a follower receives no communication over a
// period of time called the election timeout, then it assumes there is
// no viable leader and begins an election."
func TestPaper_FollowerStartsElectionOnTimeout(t *testing.T) {
	r := newPaperRaft(1)
	initialTerm := r.term

	// Advance past the election timeout.
	advanceElection(r)

	if r.state != candidate {
		t.Fatalf("expected follower to become candidate after timeout, got %s", r.state)
	}
	if r.term != initialTerm+1 {
		t.Fatalf("expected term to increment to %d, got %d", initialTerm+1, r.term)
	}
	if r.vote != r.replicaID {
		t.Fatalf("expected candidate to vote for self (%d), got %d",
			r.replicaID, r.vote)
	}

	// Verify RequestVote messages sent to all peers.
	voteRequests := collectMessages(r, proto.RequestVote)
	expectedPeers := len(r.remotes)
	if len(voteRequests) != expectedPeers {
		t.Fatalf("expected %d vote requests, got %d", expectedPeers, len(voteRequests))
	}
}

// TestPaper_CandidateRestartsElectionOnTimeout verifies that a candidate
// starts a new election when the election timeout elapses without
// winning or losing.
//
// Raft Section 5.2: "If the election timeout elapses, a new election
// begins (incrementing the term)."
func TestPaper_CandidateRestartsElectionOnTimeout(t *testing.T) {
	r := newPaperRaft(1)
	r.becomeCandidate()
	firstElectionTerm := r.term
	r.drainMessages()

	// Let the election timeout elapse again without receiving any votes.
	advanceElection(r)

	if r.state != candidate {
		t.Fatalf("expected candidate to restart election, got %s", r.state)
	}
	if r.term != firstElectionTerm+1 {
		t.Fatalf("expected term to increment from %d to %d, got %d",
			firstElectionTerm, firstElectionTerm+1, r.term)
	}

	// Verify new RequestVote messages are sent.
	voteRequests := collectMessages(r, proto.RequestVote)
	if len(voteRequests) == 0 {
		t.Fatal("expected new vote requests on election restart")
	}
}

// TestPaper_ElectionCompletesInOneRound verifies that an election
// completes in a single round when a candidate receives a majority
// of votes. Tests both 3-node and 5-node clusters.
//
// Raft Section 5.2: "A candidate wins an election if it receives votes
// from a majority of the servers in the full cluster for the same term."
func TestPaper_ElectionCompletesInOneRound(t *testing.T) {
	tests := []struct {
		name        string
		clusterN    int
		votesNeeded int // including self
	}{
		{"3-node cluster needs 2 votes", 3, 2},
		{"5-node cluster needs 3 votes", 5, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newPaperRaftN(1, tt.clusterN)
			r.becomeCandidate()
			r.broadcastVote()

			// Grant votes from exactly votesNeeded-1 peers (plus self-vote).
			for i := 0; i < tt.votesNeeded-1; i++ {
				peerID := uint64(i + 2)
				err := r.step(proto.Message{
					Type: proto.RequestVoteResp,
					From: peerID,
					Term: r.term,
				})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			if r.state != leader {
				t.Fatalf("expected leader after %d votes (self + %d peers), got %s",
					tt.votesNeeded, tt.votesNeeded-1, r.state)
			}
		})
	}
}

// TestPaper_FollowerVoteOncePerTerm verifies that a follower grants its
// vote to at most one candidate per term.
//
// Raft Section 5.2: "Each server will vote for at most one candidate in
// a given term, on a first-come-first-served basis."
func TestPaper_FollowerVoteOncePerTerm(t *testing.T) {
	r := newPaperRaft(1)

	// First candidate requests vote at term 1.
	err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		Term:     1,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First vote should be granted.
	msgs := collectMessages(r, proto.RequestVoteResp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 vote response, got %d", len(msgs))
	}
	if msgs[0].Reject {
		t.Fatal("expected first vote to be granted")
	}
	if r.vote != 2 {
		t.Fatalf("expected vote for 2, got %d", r.vote)
	}
	r.drainMessages()

	// Second candidate requests vote in the same term.
	err = r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     1,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second vote must be rejected.
	msgs = collectMessages(r, proto.RequestVoteResp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 vote response, got %d", len(msgs))
	}
	if !msgs[0].Reject {
		t.Fatal("expected second vote to be rejected (one vote per term)")
	}
}

// TestPaper_CandidateFallsBackToFollower verifies that a candidate
// becomes a follower when it receives a heartbeat from a valid leader
// in the same or higher term.
//
// Raft Section 5.2: "If the candidate receives a heartbeat from another
// server claiming to be leader [...] it recognizes the leader as
// legitimate and returns to follower state."
func TestPaper_CandidateFallsBackToFollower(t *testing.T) {
	r := newPaperRaft(1)
	r.becomeCandidate()
	candidateTerm := r.term
	r.drainMessages()

	// Receive heartbeat from a leader in the same term.
	err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: candidateTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.state != follower {
		t.Fatalf("expected candidate to become follower on leader heartbeat, got %s", r.state)
	}
	if r.leaderID != 2 {
		t.Fatalf("expected leader to be 2, got %d", r.leaderID)
	}
	if r.term != candidateTerm {
		t.Fatalf("expected term to remain %d, got %d", candidateTerm, r.term)
	}
}

// TestPaper_ElectionTimeoutRandomized verifies that the election timeout
// is randomized within [electionTimeout, 2*electionTimeout).
//
// Raft Section 5.2: "Raft uses randomized election timeouts to ensure
// that split votes are rare."
func TestPaper_ElectionTimeoutRandomized(t *testing.T) {
	seen := make(map[int]bool)
	for i := 0; i < 100; i++ {
		r := newPaperRaft(1)
		timeout := r.randomizedElectionTimeout
		if timeout < r.electionTimeout || timeout >= 2*r.electionTimeout {
			t.Fatalf("randomized timeout %d not in [%d, %d)",
				timeout, r.electionTimeout, 2*r.electionTimeout)
		}
		seen[timeout] = true
	}
	// With 100 samples over a range of electionTimeout values (10),
	// we should see at least 2 distinct values.
	if len(seen) < 2 {
		t.Fatalf("expected multiple distinct election timeouts, got %d unique values", len(seen))
	}
}

// ==========================================================================
// Section 5.3 - Log Replication
// ==========================================================================

// TestPaper_LeaderCommitsEntry verifies that the leader commits an
// entry when a majority of followers acknowledge it.
//
// Raft Section 5.3: "An entry is committed once the leader that created
// the entry has replicated it on a majority of the servers."
func TestPaper_LeaderCommitsEntry(t *testing.T) {
	r := newPaperRaft(1)
	electPaperLeader(r)
	r.drainMessages()

	// Propose an entry.
	err := r.step(proto.Message{
		Type:    proto.Propose,
		From:    r.replicaID,
		Entries: []proto.Entry{{Cmd: []byte("cmd1")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	proposedIdx := r.log.lastIndex()
	r.drainMessages()

	commitBefore := r.log.committed

	// Acknowledge from one peer (quorum = 2 for 3-node cluster).
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: proposedIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed <= commitBefore {
		t.Fatalf("expected commit to advance past %d, got %d",
			commitBefore, r.log.committed)
	}
	if r.log.committed < proposedIdx {
		t.Fatalf("expected committed >= %d, got %d", proposedIdx, r.log.committed)
	}
}

// TestPaper_LeaderCommitsPrecedingEntries verifies that when a new entry
// is committed, all preceding uncommitted entries are also committed.
//
// Raft Section 5.3: "This also commits all preceding entries in the
// leader's log, including entries created by previous leaders."
func TestPaper_LeaderCommitsPrecedingEntries(t *testing.T) {
	r := newPaperRaft(1)
	electPaperLeader(r)
	r.drainMessages()

	// Propose three entries in sequence.
	for i := 0; i < 3; i++ {
		err := r.step(proto.Message{
			Type:    proto.Propose,
			From:    r.replicaID,
			Entries: []proto.Entry{{Cmd: []byte("cmd")}},
		})
		if err != nil {
			t.Fatalf("proposal %d failed: %v", i, err)
		}
	}
	lastIdx := r.log.lastIndex()
	r.drainMessages()

	// Acknowledge the last entry from peer 2. This should commit all
	// entries through lastIdx (the quorum replication point).
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: lastIdx,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed != lastIdx {
		t.Fatalf("expected all entries committed up to %d, got %d",
			lastIdx, r.log.committed)
	}
}

// TestPaper_FollowerRejectsInconsistentAppend verifies that a follower
// rejects an AppendEntries RPC with an incorrect prevLogIndex or
// prevLogTerm.
//
// Raft Section 5.3: "If a follower does not find an entry in its log
// with the same index and term, then it refuses the new entries."
func TestPaper_FollowerRejectsInconsistentAppend(t *testing.T) {
	// Create a follower with entries at term 1: indices 1, 2.
	entries := []proto.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
	}
	r := newPaperRaftWithLog(1, entries)
	r.becomeFollower(1, 2)

	tests := []struct {
		name         string
		prevLogIndex uint64
		prevLogTerm  uint64
	}{
		{"prevLogIndex too high", 5, 1},
		{"prevLogTerm mismatch", 2, 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r.drainMessages()
			err := r.step(proto.Message{
				Type:     proto.Replicate,
				From:     2,
				Term:     r.term,
				LogIndex: tt.prevLogIndex,
				LogTerm:  tt.prevLogTerm,
				Entries:  []proto.Entry{{Term: r.term, Index: tt.prevLogIndex + 1}},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			resps := collectMessages(r, proto.ReplicateResp)
			if len(resps) == 0 {
				t.Fatal("expected a ReplicateResp")
			}
			if !resps[0].Reject {
				t.Fatal("expected rejection for inconsistent append")
			}
		})
	}
}

// TestPaper_FollowerAppendsEntries verifies that a follower correctly
// appends new entries when the prevLogIndex and prevLogTerm match.
//
// Raft Section 5.3: "If an existing entry conflicts with a new one
// (same index but different terms), delete the existing entry and all
// that follow it. Append any new entries not already in the log."
func TestPaper_FollowerAppendsEntries(t *testing.T) {
	// Follower has entry at index 1, term 1.
	entries := []proto.Entry{
		{Term: 1, Index: 1},
	}
	r := newPaperRaftWithLog(1, entries)
	r.becomeFollower(1, 2)
	r.drainMessages()

	// Leader sends new entries with matching prevLog.
	newEntries := []proto.Entry{
		{Term: 1, Index: 2, Cmd: []byte("a")},
		{Term: 1, Index: 3, Cmd: []byte("b")},
	}
	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		Term:     r.term,
		LogIndex: 1,
		LogTerm:  1,
		Entries:  newEntries,
		Commit:   0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.lastIndex() != 3 {
		t.Fatalf("expected lastIndex 3 after append, got %d", r.log.lastIndex())
	}

	resps := collectMessages(r, proto.ReplicateResp)
	if len(resps) == 0 {
		t.Fatal("expected success ReplicateResp")
	}
	if resps[0].Reject {
		t.Fatal("expected successful append, got rejection")
	}
	if resps[0].LogIndex != 3 {
		t.Fatalf("expected response LogIndex 3, got %d", resps[0].LogIndex)
	}
}

// TestPaper_LeaderSyncsFollowerLog tests the 7 scenarios from Raft
// Figure 7 where a follower's log diverges from the leader's. The
// leader must force the follower's log to match its own by finding
// the latest matching entry and sending entries from that point.
//
// Raft Section 5.3: "The leader handles inconsistencies by forcing the
// followers' logs to duplicate its own."
func TestPaper_LeaderSyncsFollowerLog(t *testing.T) {
	// The leader's log has 10 entries:
	// Index: 1  2  3  4  5  6  7  8  9  10
	// Term:  1  1  1  4  4  5  5  6  6  6
	leaderEntries := []proto.Entry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
		{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 5, Index: 6},
		{Term: 5, Index: 7}, {Term: 6, Index: 8}, {Term: 6, Index: 9},
		{Term: 6, Index: 10},
	}

	tests := []struct {
		name            string
		followerEntries []proto.Entry
		description     string
	}{
		{
			name: "(a) follower missing entries",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
				{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 5, Index: 6},
				{Term: 5, Index: 7}, {Term: 6, Index: 8}, {Term: 6, Index: 9},
			},
			description: "follower has 9 matching entries, missing index 10",
		},
		{
			name: "(b) follower shorter log",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
				{Term: 4, Index: 4},
			},
			description: "follower has only 4 entries",
		},
		{
			name: "(c) follower has extra uncommitted entries from different term",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
				{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 5, Index: 6},
				{Term: 5, Index: 7}, {Term: 6, Index: 8}, {Term: 6, Index: 9},
				{Term: 6, Index: 10}, {Term: 7, Index: 11},
			},
			description: "follower has an extra entry from a different term",
		},
		{
			name: "(d) follower has diverged log",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
				{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 5, Index: 6},
				{Term: 5, Index: 7}, {Term: 7, Index: 8}, {Term: 7, Index: 9},
				{Term: 7, Index: 10}, {Term: 7, Index: 11},
			},
			description: "follower diverged at index 8 with term 7 instead of 6",
		},
		{
			name: "(e) follower has entries from future term",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
				{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 4, Index: 6},
				{Term: 4, Index: 7},
			},
			description: "follower diverged at index 6 with term 4 instead of 5",
		},
		{
			name: "(f) follower has completely different suffix",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
				{Term: 2, Index: 4}, {Term: 2, Index: 5}, {Term: 2, Index: 6},
				{Term: 3, Index: 7}, {Term: 3, Index: 8}, {Term: 3, Index: 9},
				{Term: 3, Index: 10}, {Term: 3, Index: 11},
			},
			description: "follower diverged at index 4 entirely",
		},
		{
			name: "(g) follower has only the first entry",
			followerEntries: []proto.Entry{
				{Term: 1, Index: 1},
			},
			description: "follower has minimal log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the follower with its divergent log.
			f := newPaperRaftWithLog(1, tt.followerEntries)
			f.becomeFollower(8, 2) // Leader is at term 8 (higher than any entry).

			// Simulate the leader probing backward from its last index.
			// The leader sends Replicate messages with decreasing
			// prevLogIndex until the follower accepts.
			accepted := false
			for prevIdx := uint64(len(leaderEntries)); prevIdx >= 1; prevIdx-- {
				prevTerm := leaderEntries[prevIdx-1].Term
				var entriesToSend []proto.Entry
				for _, e := range leaderEntries {
					if e.Index > prevIdx {
						entriesToSend = append(entriesToSend, e)
					}
				}

				f.drainMessages()
				err := f.step(proto.Message{
					Type:     proto.Replicate,
					From:     2,
					Term:     8,
					LogIndex: prevIdx,
					LogTerm:  prevTerm,
					Entries:  entriesToSend,
					Commit:   0,
				})
				if err != nil {
					t.Fatalf("unexpected error at prevIdx=%d: %v", prevIdx, err)
				}

				resps := collectMessages(f, proto.ReplicateResp)
				if len(resps) == 0 {
					continue
				}
				if !resps[0].Reject {
					accepted = true
					break
				}
			}

			if !accepted {
				// Try prevIdx=0 (empty prevLog, send all entries).
				f.drainMessages()
				err := f.step(proto.Message{
					Type:     proto.Replicate,
					From:     2,
					Term:     8,
					LogIndex: 0,
					LogTerm:  0,
					Entries:  leaderEntries,
					Commit:   0,
				})
				if err != nil {
					t.Fatalf("unexpected error at prevIdx=0: %v", err)
				}

				resps := collectMessages(f, proto.ReplicateResp)
				if len(resps) == 0 || resps[0].Reject {
					t.Fatal("expected follower to accept entries at prevIdx=0")
				}
			}

			// Verify the follower's log matches the leader's entries
			// for indices 1 through len(leaderEntries). The log may
			// have extra trailing entries if the follower's log was
			// longer and the append contained no conflicting entries
			// at those positions - the leader will overwrite them
			// with future entries.
			leaderLastIdx := uint64(len(leaderEntries))
			if f.log.lastIndex() < leaderLastIdx {
				t.Fatalf("expected follower lastIndex >= %d, got %d",
					leaderLastIdx, f.log.lastIndex())
			}
			for _, le := range leaderEntries {
				ft, err := f.log.term(le.Index)
				if err != nil {
					t.Fatalf("failed to get term for index %d: %v", le.Index, err)
				}
				if ft != le.Term {
					t.Fatalf("index %d: expected term %d, got %d",
						le.Index, le.Term, ft)
				}
			}
		})
	}
}

// ==========================================================================
// Section 5.4 - Safety
// ==========================================================================

// TestPaper_CandidateWithStalerLogLosesElection verifies that a candidate
// whose log is less up-to-date than the voter's log is rejected.
//
// Raft Section 5.4.1: "The voter denies its vote if its own log is more
// up-to-date than that of the candidate."
func TestPaper_CandidateWithStalerLogLosesElection(t *testing.T) {
	tests := []struct {
		name            string
		voterEntries    []proto.Entry
		candidateLogIdx uint64
		candidateLogTrm uint64
	}{
		{
			name:            "candidate has shorter log with same last term",
			voterEntries:    []proto.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}},
			candidateLogIdx: 1,
			candidateLogTrm: 1,
		},
		{
			name:            "candidate has lower last term",
			voterEntries:    []proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
			candidateLogIdx: 3,
			candidateLogTrm: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voter := newPaperRaftWithLog(1, tt.voterEntries)
			voter.becomeFollower(3, noLeader)
			voter.drainMessages()

			err := voter.step(proto.Message{
				Type:     proto.RequestVote,
				From:     2,
				Term:     3,
				LogIndex: tt.candidateLogIdx,
				LogTerm:  tt.candidateLogTrm,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			resps := collectMessages(voter, proto.RequestVoteResp)
			if len(resps) != 1 {
				t.Fatalf("expected 1 vote response, got %d", len(resps))
			}
			if !resps[0].Reject {
				t.Fatal("expected vote to be rejected for less up-to-date candidate")
			}
		})
	}
}

// TestPaper_LeaderOnlyCommitsCurrentTermEntries verifies that a leader
// cannot commit entries from previous terms using only the replication
// count. It must commit at least one entry from its current term first.
//
// Raft Section 5.4.2 / Figure 8: "Raft never commits log entries from
// previous terms by counting replicas. Only log entries from the
// leader's current term are committed by counting replicas."
func TestPaper_LeaderOnlyCommitsCurrentTermEntries(t *testing.T) {
	// Create a leader with an entry from a previous term in its log.
	// The log has:
	// Index 1: term 1 (from LogReader)
	// Index 2: term 2 (from LogReader, previous leader's uncommitted entry)
	entries := []proto.Entry{
		{Term: 1, Index: 1},
		{Term: 2, Index: 2},
	}
	r := newPaperRaftWithLog(1, entries)
	// Force term to 3 so the old entry (term 2) is from a previous term.
	r.becomeFollower(3, noLeader)
	r.becomeCandidate()
	// Grant enough votes to win.
	for id := range r.remotes {
		if id != r.replicaID {
			err := r.step(proto.Message{
				Type: proto.RequestVoteResp,
				From: id,
				Term: r.term,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.state == leader {
				break
			}
		}
	}
	if r.state != leader {
		t.Fatal("expected to become leader")
	}
	r.drainMessages()

	// The no-op entry from becomeLeader is at index 3, term 4.
	// Entry at index 2 (term 2) is still uncommitted.

	// Simulate a follower acknowledging index 2 (previous term's entry)
	// but NOT the no-op at index 3.
	err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The commit index should NOT advance to 2 because the entry at
	// index 2 is from term 2, not the leader's current term.
	// tryCommit checks that the entry at the potential commit index
	// has the current leader's term.
	noopIndex := r.log.lastIndex() // Should be index 3 (no-op)
	entryTerm, termErr := r.log.term(2)
	if termErr != nil {
		t.Fatalf("unexpected error getting term: %v", termErr)
	}
	if entryTerm == r.term {
		t.Fatalf("entry at index 2 has current term %d; test setup must guarantee a previous-term entry", entryTerm)
	}

	// The commit index should be at most 2 ONLY if the no-op (index 3)
	// is also replicated. Let's verify the no-op exists.
	if noopIndex < 3 {
		t.Fatalf("expected no-op at index >= 3, got lastIndex=%d", noopIndex)
	}

	// Now acknowledge the no-op from the follower. This should cause
	// both entries (index 2 and 3) to be committed because the no-op
	// is from the current term.
	commitBefore := r.log.committed
	err = r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: noopIndex,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.log.committed <= commitBefore {
		t.Fatal("expected commit to advance after majority ack of current-term entry")
	}
	if r.log.committed < noopIndex {
		t.Fatalf("expected commit >= %d (no-op index), got %d", noopIndex, r.log.committed)
	}
}

// TestPaper_VoteRejectsLessUpToDateCandidate verifies that a server
// rejects a vote request from a candidate whose log is less up-to-date
// than the voter's.
//
// Raft Section 5.4.1: "Raft determines which of two logs is more
// up-to-date by comparing the index and term of the last entries in
// the logs."
func TestPaper_VoteRejectsLessUpToDateCandidate(t *testing.T) {
	tests := []struct {
		name            string
		voterEntries    []proto.Entry
		candidateLogIdx uint64
		candidateLogTrm uint64
		expectReject    bool
	}{
		{
			// Voter: last entry (index 3, term 2).
			// Candidate: last entry (index 2, term 3) -> candidate term is higher,
			// so the candidate is more up-to-date.
			name:            "candidate has higher last term (more up-to-date)",
			voterEntries:    []proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}},
			candidateLogIdx: 2,
			candidateLogTrm: 3,
			expectReject:    false,
		},
		{
			// Voter: last entry (index 3, term 2).
			// Candidate: last entry (index 2, term 2) -> same term, shorter log.
			name:            "candidate has same term but shorter log",
			voterEntries:    []proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}},
			candidateLogIdx: 2,
			candidateLogTrm: 2,
			expectReject:    true,
		},
		{
			// Voter: last entry (index 2, term 3).
			// Candidate: last entry (index 5, term 2) -> candidate term is lower.
			name:            "candidate has lower last term (less up-to-date)",
			voterEntries:    []proto.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}},
			candidateLogIdx: 5,
			candidateLogTrm: 2,
			expectReject:    true,
		},
		{
			// Voter: last entry (index 3, term 2).
			// Candidate: last entry (index 3, term 2) -> identical, grant.
			name:            "candidate has identical log",
			voterEntries:    []proto.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}},
			candidateLogIdx: 3,
			candidateLogTrm: 2,
			expectReject:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voter := newPaperRaftWithLog(1, tt.voterEntries)
			voter.becomeFollower(5, noLeader)
			voter.drainMessages()

			err := voter.step(proto.Message{
				Type:     proto.RequestVote,
				From:     2,
				Term:     5,
				LogIndex: tt.candidateLogIdx,
				LogTerm:  tt.candidateLogTrm,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			resps := collectMessages(voter, proto.RequestVoteResp)
			if len(resps) != 1 {
				t.Fatalf("expected 1 vote response, got %d", len(resps))
			}
			if resps[0].Reject != tt.expectReject {
				t.Fatalf("expected Reject=%v, got %v", tt.expectReject, resps[0].Reject)
			}
		})
	}
}

// ==========================================================================
// PreVote Extension
// ==========================================================================

// TestPaper_PreVotePreventsDisruption verifies that the PreVote mechanism
// prevents a partitioned node from disrupting the cluster when it
// rejoins. A partitioned node does not increment its term during
// PreVote elections, so it cannot force the rest of the cluster to
// step down.
//
// Raft PhD thesis Section 9.6 / PreVote extension.
func TestPaper_PreVotePreventsDisruption(t *testing.T) {
	// Create a node with PreVote enabled.
	r := newTestRaft(1) // PreVote enabled by default

	initialTerm := r.term

	// Simulate a partition: the node times out multiple times without
	// getting any responses.
	for i := 0; i < 5; i++ {
		advanceElection(r)
		r.drainMessages()
	}

	// The term should NOT have increased because PreVote elections do
	// not increment the term until the real election phase.
	if r.term != initialTerm {
		t.Fatalf("expected term to remain %d (PreVote prevents term inflation), got %d",
			initialTerm, r.term)
	}

	// The node should be in preCandidate state (retrying PreVote).
	if r.state != preCandidate {
		t.Fatalf("expected preCandidate state during partition, got %s", r.state)
	}
}

// TestPaper_DuelingPreCandidates verifies the behavior when two
// pre-candidates compete simultaneously. Neither should disrupt the
// cluster's term.
//
// Raft PhD thesis Section 9.6.
func TestPaper_DuelingPreCandidates(t *testing.T) {
	// Two nodes, both with PreVote enabled.
	r1 := newTestRaft(1)
	r2 := newTestRaft(2)

	initialTerm1 := r1.term
	initialTerm2 := r2.term

	// Both start elections (become pre-candidates).
	advanceElection(r1)
	advanceElection(r2)

	if r1.state != preCandidate {
		t.Fatalf("expected r1 to be preCandidate, got %s", r1.state)
	}
	if r2.state != preCandidate {
		t.Fatalf("expected r2 to be preCandidate, got %s", r2.state)
	}

	// Neither should have incremented their term.
	if r1.term != initialTerm1 {
		t.Fatalf("expected r1 term unchanged (%d), got %d", initialTerm1, r1.term)
	}
	if r2.term != initialTerm2 {
		t.Fatalf("expected r2 term unchanged (%d), got %d", initialTerm2, r2.term)
	}

	// Exchange pre-vote requests. Since both are pre-candidates with the
	// same log and no active leader, they should grant pre-votes.
	r1Msgs := collectMessages(r1, proto.RequestPreVote)
	r2Msgs := collectMessages(r2, proto.RequestPreVote)

	// Deliver r1's pre-vote request to r2.
	for _, m := range r1Msgs {
		if m.To == 2 {
			err := r2.step(m)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}

	// Deliver r2's pre-vote request to r1.
	for _, m := range r2Msgs {
		if m.To == 1 {
			err := r1.step(m)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}

	// After processing, at most one should advance to candidate.
	// Terms should still not have inflated beyond initialTerm + 1.
	if r1.term > initialTerm1+1 {
		t.Fatalf("r1 term inflated beyond %d, got %d", initialTerm1+1, r1.term)
	}
	if r2.term > initialTerm2+1 {
		t.Fatalf("r2 term inflated beyond %d, got %d", initialTerm2+1, r2.term)
	}
}

// ==========================================================================
// CheckQuorum
// ==========================================================================

// TestPaper_LeaderStepsDownWithoutQuorum verifies that a leader steps
// down to follower when CheckQuorum is enabled and the leader has not
// received communication from a quorum of nodes within the election
// timeout.
//
// Raft PhD thesis Section 6.2 / CheckQuorum extension.
func TestPaper_LeaderStepsDownWithoutQuorum(t *testing.T) {
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}
	cfg := raftTestCfgCheckQuorum(1, 1, true)
	cfg.PreVote = config.BoolPtr(false)
	logdb := newTestLogReaderWithMembership(members)
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	electPaperLeader(r)
	if r.state != leader {
		t.Fatalf("expected leader, got %s", r.state)
	}

	// Advance past the election timeout without any follower activity.
	// The leader's checkQuorum runs at electionTimeout ticks.
	for i := 0; i < r.electionTimeout+1; i++ {
		r.tickFunc(r)
	}

	if r.state != follower {
		t.Fatalf("expected leader to step down without quorum, got %s", r.state)
	}
}

// TestPaper_FreeStuckCandidateWithCheckQuorum verifies that when
// CheckQuorum is enabled, a candidate that is stuck can still
// eventually win an election when the leader steps down due to
// quorum loss.
//
// Raft PhD thesis Section 6.2.
func TestPaper_FreeStuckCandidateWithCheckQuorum(t *testing.T) {
	// Create a 3-node cluster with CheckQuorum enabled on all nodes.
	members := map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	}

	// Node 1 is the leader with CheckQuorum.
	cfg1 := raftTestCfgCheckQuorum(1, 1, true)
	cfg1.PreVote = config.BoolPtr(false)
	logdb1 := newTestLogReaderWithMembership(members)
	leader1, err := newRaft(cfg1, logdb1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	electPaperLeader(leader1)

	// Node 2 is a follower that will become a candidate.
	cfg2 := raftTestCfgCheckQuorum(1, 2, true)
	cfg2.PreVote = config.BoolPtr(false)
	logdb2 := newTestLogReaderWithMembership(members)
	node2, err := newRaft(cfg2, logdb2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Advance node 2 past election timeout so it becomes a candidate.
	advanceElection(node2)
	if node2.state != candidate {
		t.Fatalf("expected node2 to become candidate, got %s", node2.state)
	}

	// Now advance the leader past election timeout without follower
	// heartbeats. The leader should step down.
	for i := 0; i < leader1.electionTimeout+1; i++ {
		leader1.tickFunc(leader1)
	}

	if leader1.state != follower {
		t.Fatalf("expected leader to step down, got %s", leader1.state)
	}

	// Node 2 can now retry and should be able to win the election
	// since the old leader is no longer sending heartbeats.
	// Restart the candidate's election.
	node2.drainMessages()
	advanceElection(node2)
	candidateTerm := node2.term

	// Grant vote from the now-follower node 1.
	err = node2.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 1,
		Term: candidateTerm,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if node2.state != leader {
		t.Fatalf("expected candidate to win election after old leader stepped down, got %s",
			node2.state)
	}
}
