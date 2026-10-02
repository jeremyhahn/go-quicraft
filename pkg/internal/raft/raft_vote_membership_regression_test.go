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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Regression tests pinning that:
//
//	handleVoteRequest grants no same-term vote while a live leader is known
//	a self-removed replica cannot campaign or self-elect
//	becomeLeader does not inherit remote.active flags from a previous tenure
//	handleQuiesceMessage honours the local quiesce.enabled setting
//
// plus direct coverage for dropHighTermVote, purgeDepartedVotes and
// isVotingMember.

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestRaftQuiesce creates a 3-node raft with quiesce enabled.
func newTestRaftQuiesce(replicaID uint64) *raft {
	cfg := raftTestCfgQuiesce(1, replicaID)
	logdb := newTestLogReaderWithMembership(map[uint64]string{
		1: "addr1",
		2: "addr2",
		3: "addr3",
	})
	r, err := newRaft(cfg, logdb)
	if err != nil {
		panic("newTestRaftQuiesce failed: " + err.Error())
	}
	return r
}

// followerWithKnownLeader returns replica 2 as a follower that learned
// leader 1 at the given term from a heartbeat, WITHOUT having voted. This
// is the state the M1 finding turns on: term advanced by the leader, so
// r.vote is noNode while r.leaderID names a live leader.
func followerWithKnownLeader(t *testing.T, term uint64) *raft {
	t.Helper()
	r := newTestRaft(2)
	if err := r.step(proto.Message{
		Type: proto.Heartbeat,
		From: 1,
		Term: term,
	}); err != nil {
		t.Fatalf("heartbeat step: %v", err)
	}
	r.drainMessages()
	if r.state != follower {
		t.Fatalf("precondition: state = %s, want follower", r.state)
	}
	if r.leaderID != 1 {
		t.Fatalf("precondition: leaderID = %d, want 1", r.leaderID)
	}
	if r.vote != noNode {
		t.Fatalf("precondition: vote = %d, want noNode", r.vote)
	}
	if r.term != term {
		t.Fatalf("precondition: term = %d, want %d", r.term, term)
	}
	return r
}

// voteResp returns the single RequestVoteResp in the outbound buffer.
func voteResp(t *testing.T, r *raft) proto.Message {
	t.Helper()
	msgs := collectMessages(r, proto.RequestVoteResp)
	if len(msgs) != 1 {
		t.Fatalf("got %d RequestVoteResp messages, want 1", len(msgs))
	}
	return msgs[0]
}

// ---------------------------------------------------------------------------
// M1: no same-term vote while a leader is known
// ---------------------------------------------------------------------------

// TestRaft_VoteRequest_RejectedWhenLeaderKnownAtSameTerm asserts the
// etcd/raft canVote rule: a vote is granted only on a repeat of the vote
// already cast, or when no vote has been cast AND no leader is known for
// the term. A follower that learned its term from the leader has vote ==
// noNode but leaderID != noLeader, and must NOT grant.
func TestRaft_VoteRequest_RejectedWhenLeaderKnownAtSameTerm(t *testing.T) {
	r := followerWithKnownLeader(t, 5)

	// Replica 3 campaigns at the SAME term. dropHighTermVote does not fire
	// (msg.Term is not greater than r.term), so the decision rests entirely
	// on handleVoteRequest.
	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     5,
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("vote request step: %v", err)
	}

	resp := voteResp(t, r)
	if !resp.Reject {
		t.Fatal("granted a same-term vote while a live leader was known; " +
			"this unseats a healthy leader (etcd/raft requires lead == None)")
	}
	if r.vote != noNode {
		t.Fatalf("vote = %d, want noNode: a rejected request must not record a vote", r.vote)
	}
	if r.leaderID != 1 {
		t.Fatalf("leaderID = %d, want 1: rejecting a vote must not clear the leader", r.leaderID)
	}
}

// TestRaft_VoteRequest_GrantDoesNotArmVoteBlackout is the second half of
// the M1 finding. Granting sets electionTick = 0 but leaves leaderID
// pointing at the old leader, which re-arms dropHighTermVote and makes the
// follower SILENTLY DISCARD every higher-term RequestVote for a full
// election timeout. With the leader-lease already expired, a stale
// same-term candidate must not be able to re-arm that guard.
func TestRaft_VoteRequest_GrantDoesNotArmVoteBlackout(t *testing.T) {
	r := followerWithKnownLeader(t, 5)

	// Let the leader lease expire: dropHighTermVote is now disarmed.
	r.electionTick = r.electionTimeout

	// A stale candidate campaigns at the same term.
	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     5,
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("same-term vote request: %v", err)
	}
	r.drainMessages()

	// A legitimate higher-term candidate now campaigns. It must be heard.
	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     6,
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("higher-term vote request: %v", err)
	}

	if r.term != 6 {
		t.Fatalf("term = %d, want 6: the higher-term RequestVote was silently "+
			"dropped because the same-term grant reset electionTick without "+
			"clearing leaderID (self-inflicted vote blackout)", r.term)
	}
	resp := voteResp(t, r)
	if resp.Reject {
		t.Fatal("higher-term vote rejected; the follower has no leader at term 6")
	}
	if r.vote != 3 {
		t.Fatalf("vote = %d, want 3", r.vote)
	}
}

// TestRaft_VoteRequest_GrantedWhenNoLeaderKnown is the liveness control for
// the M1 fix: with no leader known for the term the vote must still be
// granted, otherwise no election could ever complete.
func TestRaft_VoteRequest_GrantedWhenNoLeaderKnown(t *testing.T) {
	r := newTestRaft(2)
	r.becomeFollower(5, noLeader)
	r.drainMessages()

	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     5,
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("vote request step: %v", err)
	}

	if resp := voteResp(t, r); resp.Reject {
		t.Fatal("rejected a vote with no leader known; elections cannot complete")
	}
	if r.vote != 3 {
		t.Fatalf("vote = %d, want 3", r.vote)
	}
}

// TestRaft_VoteRequest_RepeatVoteStillGranted covers the idempotent
// re-grant arm of canVote: once a vote is cast, a retransmitted request
// from the SAME candidate is granted again even after that candidate has
// been installed as leader in this node's view.
func TestRaft_VoteRequest_RepeatVoteStillGranted(t *testing.T) {
	r := newTestRaft(2)
	r.becomeFollower(5, noLeader)
	r.vote = 3
	r.leaderID = 3
	r.drainMessages()

	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     5,
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("vote request step: %v", err)
	}

	if resp := voteResp(t, r); resp.Reject {
		t.Fatal("a repeat of an already-cast vote must be granted")
	}
}

// TestRaft_VoteRequest_TransferHintOverridesKnownLeader asserts that a
// leadership-transfer vote is exempt from the known-leader clause, exactly
// as it is exempt from dropHighTermVote: the outgoing leader asked for
// this election, so its own lease must not veto it.
func TestRaft_VoteRequest_TransferHintOverridesKnownLeader(t *testing.T) {
	r := followerWithKnownLeader(t, 5)

	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     5,
		Hint:     3, // Hint == From marks a leadership-transfer vote.
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("transfer vote request: %v", err)
	}

	if resp := voteResp(t, r); resp.Reject {
		t.Fatal("rejected a leadership-transfer vote; transfers would never complete")
	}
	if r.vote != 3 {
		t.Fatalf("vote = %d, want 3", r.vote)
	}
}

// ---------------------------------------------------------------------------
// dropHighTermVote on a follower
// ---------------------------------------------------------------------------

// TestRaft_DropHighTermVote_FollowerBranches exercises every arm of
// dropHighTermVote on a FOLLOWER, including the `return true` branch.
func TestRaft_DropHighTermVote_FollowerBranches(t *testing.T) {
	newFollower := func() *raft { return followerWithKnownLeader(t, 5) }

	tests := []struct {
		name  string
		setup func(r *raft)
		msg   proto.Message
		want  bool
	}{
		{
			name: "higher term with fresh leader lease is dropped",
			msg:  proto.Message{Type: proto.RequestVote, From: 3, Term: 6},
			want: true,
		},
		{
			name: "same term is never dropped",
			msg:  proto.Message{Type: proto.RequestVote, From: 3, Term: 5},
			want: false,
		},
		{
			name:  "expired leader lease is not dropped",
			setup: func(r *raft) { r.electionTick = r.electionTimeout },
			msg:   proto.Message{Type: proto.RequestVote, From: 3, Term: 6},
			want:  false,
		},
		{
			name:  "no known leader is not dropped",
			setup: func(r *raft) { r.leaderID = noLeader },
			msg:   proto.Message{Type: proto.RequestVote, From: 3, Term: 6},
			want:  false,
		},
		{
			name: "leadership transfer hint exempts the vote",
			msg:  proto.Message{Type: proto.RequestVote, From: 3, Term: 6, Hint: 3},
			want: false,
		},
		{
			name:  "checkQuorum disabled never drops",
			setup: func(r *raft) { r.checkQuorum = false },
			msg:   proto.Message{Type: proto.RequestVote, From: 3, Term: 6},
			want:  false,
		},
		{
			name: "PreVote is never dropped",
			msg:  proto.Message{Type: proto.RequestPreVote, From: 3, Term: 6},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newFollower()
			if tt.setup != nil {
				tt.setup(r)
			}
			if got := r.dropHighTermVote(tt.msg); got != tt.want {
				t.Fatalf("dropHighTermVote = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRaft_DropHighTermVote_FollowerSilentlyDiscards asserts the observable
// effect of the `return true` branch on a follower: the message produces no
// response and does not advance the term.
func TestRaft_DropHighTermVote_FollowerSilentlyDiscards(t *testing.T) {
	r := followerWithKnownLeader(t, 5)

	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     3,
		Term:     9,
		LogIndex: r.log.lastIndex(),
		LogTerm:  5,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}

	if r.term != 5 {
		t.Fatalf("term = %d, want 5: a dropped vote must not advance the term", r.term)
	}
	if r.vote != noNode {
		t.Fatalf("vote = %d, want noNode", r.vote)
	}
	if r.leaderID != 1 {
		t.Fatalf("leaderID = %d, want 1", r.leaderID)
	}
	if msgs := r.drainMessages(); len(msgs) != 0 {
		t.Fatalf("got %d outbound messages, want 0: the vote must be discarded silently",
			len(msgs))
	}
}

// ---------------------------------------------------------------------------
// M2: a self-removed replica must not campaign
// ---------------------------------------------------------------------------

// TestRaft_SelfRemoved_DerivedFromMembership asserts the flag is derived at
// construction from every membership role.
func TestRaft_SelfRemoved_DerivedFromMembership(t *testing.T) {
	tests := []struct {
		name string
		ms   proto.Membership
		want bool
	}{
		{
			name: "voter",
			ms:   proto.Membership{Addresses: map[uint64]string{1: "a", 2: "b"}},
			want: false,
		},
		{
			name: "observer",
			ms: proto.Membership{
				Addresses: map[uint64]string{2: "b"},
				Observers: map[uint64]string{1: "a"},
			},
			want: false,
		},
		{
			name: "witness",
			ms: proto.Membership{
				Addresses: map[uint64]string{2: "b"},
				Witnesses: map[uint64]string{1: "a"},
			},
			want: false,
		},
		{
			name: "removed",
			ms:   proto.Membership{Addresses: map[uint64]string{2: "b", 3: "c"}},
			want: true,
		},
		{
			name: "empty configuration",
			ms:   proto.Membership{},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newTestLogReader(nil, proto.State{})
			lr.membership = tt.ms
			r, err := newRaft(raftTestCfg(1, 1), lr)
			if err != nil {
				t.Fatalf("newRaft: %v", err)
			}
			if r.selfRemoved != tt.want {
				t.Fatalf("selfRemoved = %v, want %v", r.selfRemoved, tt.want)
			}
		})
	}
}

// TestRaft_SelfRemoved_DoesNotCampaignOnElectionTimeout is the core M2
// regression: replica 1 has been removed from a configuration that still
// holds {2,3}. Its own quorum arithmetic counts self (quorum 2 of
// remotes+self), so 1 real grant would "win" a term the surviving
// configuration is also electing a leader in.
func TestRaft_SelfRemoved_DoesNotCampaignOnElectionTimeout(t *testing.T) {
	r := newTestRaftWithMembers(1, map[uint64]string{2: "addr2", 3: "addr3"})
	r.preVote = false
	r.becomeFollower(5, noLeader)
	r.drainMessages()

	if !r.selfRemoved {
		t.Fatal("precondition: replica 1 must be outside the configuration")
	}

	for range 2*r.electionTimeout + 1 {
		r.tickFunc(r)
	}

	if r.state != follower {
		t.Fatalf("state = %s, want follower: a replica outside the configuration "+
			"must never campaign (it can win a term the real configuration also "+
			"decides -- two leaders in one term)", r.state)
	}
	if r.term != 5 {
		t.Fatalf("term = %d, want 5: a removed replica must not burn terms", r.term)
	}
	if msgs := collectMessages(r, proto.RequestVote); len(msgs) != 0 {
		t.Fatalf("got %d RequestVote messages, want 0", len(msgs))
	}
}

// TestRaft_SelfRemoved_IgnoresTimeoutNow asserts a TimeoutNow in flight when
// the removal committed cannot hand leadership to a departed replica.
func TestRaft_SelfRemoved_IgnoresTimeoutNow(t *testing.T) {
	r := newTestRaftWithMembers(1, map[uint64]string{2: "addr2", 3: "addr3"})
	r.becomeFollower(5, 2)
	r.drainMessages()

	if err := r.step(proto.Message{Type: proto.TimeoutNow, From: 2, Term: 5}); err != nil {
		t.Fatalf("TimeoutNow step: %v", err)
	}

	if r.state != follower {
		t.Fatalf("state = %s, want follower", r.state)
	}
	if r.term != 5 {
		t.Fatalf("term = %d, want 5", r.term)
	}
}

// TestRaft_SelfRemoved_OwnVoteNotTallied asserts the removed replica's own
// vote no longer counts toward a quorum computed over a configuration it is
// not in. This is defence in depth behind the campaign guard.
func TestRaft_SelfRemoved_OwnVoteNotTallied(t *testing.T) {
	r := newTestRaft(1)
	r.votes = map[uint64]bool{1: true, 2: true}

	granted, _ := r.countVotes()
	if granted != 2 {
		t.Fatalf("granted = %d, want 2 while still a member", granted)
	}

	r.setSelfMembership(false)
	granted, _ = r.countVotes()
	if granted != 1 {
		t.Fatalf("granted = %d, want 1: a self-removed replica must not tally "+
			"its own vote", granted)
	}
	if r.isVotingMember(r.replicaID) {
		t.Fatal("a self-removed replica is not a voting member")
	}
}

// TestPeer_SelfRemovedFollower_DoesNotCampaign drives the whole production
// path: propose RemoveNode(self) on the leader, commit it, watch the leader
// step down via checkLeaderSelfRemoval, and then confirm the resulting
// follower never campaigns. Before the fix the stepped-down leader simply
// started a new election at the next election timeout and could win it with
// a single grant while {2,3} elected their own leader in the same term.
func TestPeer_SelfRemovedFollower_DoesNotCampaign(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	if err := p.ProposeConfigChange(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: p.replicaID,
	}); err != nil {
		t.Fatalf("self-removal proposal: %v", err)
	}

	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: p.r.log.lastIndex(),
	})
	runUpdateCycles(t, p)

	if p.membership.isMember(p.replicaID) {
		t.Fatal("precondition: the self-removal must have committed")
	}
	if p.r.state == leader {
		t.Fatal("precondition: the leader must have stepped down")
	}
	if !p.r.selfRemoved {
		t.Fatal("selfRemoved was not synced from the committed membership")
	}

	termAfterRemoval := p.r.term
	for range 4 * p.r.electionTimeout {
		p.Tick()
	}

	if p.r.state != follower {
		t.Fatalf("state = %s, want follower: a removed replica must not campaign",
			p.r.state)
	}
	if p.r.term != termAfterRemoval {
		t.Fatalf("term advanced from %d to %d: a removed replica campaigned and "+
			"can be elected leader of a configuration it is not in",
			termAfterRemoval, p.r.term)
	}
}

// TestPeer_ReAddedReplica_CampaignsAgain is the liveness control: a replica
// added back to the configuration must resume campaigning, otherwise the
// M2 guard would permanently disable a legitimate voter.
func TestPeer_ReAddedReplica_CampaignsAgain(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	// Replica 1 starts outside the configuration, as a joining node does.
	p := newTestPeer(t, cfg, proto.State{}, proto.Membership{
		Addresses: map[uint64]string{2: "addr2", 3: "addr3"},
	})
	if !p.r.selfRemoved {
		t.Fatal("precondition: replica 1 must start outside the configuration")
	}

	p.membership.addresses[p.replicaID] = "addr1"
	p.syncRaftRemotes()
	if p.r.selfRemoved {
		t.Fatal("selfRemoved must clear once the replica rejoins the configuration")
	}

	for range 4 * p.r.electionTimeout {
		p.Tick()
	}
	if p.r.state == follower {
		t.Fatal("a replica back in the configuration must campaign again")
	}
}

// ---------------------------------------------------------------------------
// purgeDepartedVotes / isVotingMember
// ---------------------------------------------------------------------------

func TestRaft_IsVotingMember(t *testing.T) {
	r := newTestRaftWithMembers(1, map[uint64]string{1: "a", 2: "b"})
	r.witnesses[4] = newRemote(1, maxInflightMsgs)
	r.observers[5] = newRemote(1, maxInflightMsgs)

	tests := []struct {
		name        string
		id          uint64
		selfRemoved bool
		want        bool
	}{
		{name: "self while a member", id: 1, want: true},
		{name: "self after removal", id: 1, selfRemoved: true, want: false},
		{name: "remote", id: 2, want: true},
		{name: "witness", id: 4, want: true},
		{name: "observer is not a voter", id: 5, want: false},
		{name: "unknown replica", id: 9, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r.selfRemoved = tt.selfRemoved
			defer func() { r.selfRemoved = false }()
			if got := r.isVotingMember(tt.id); got != tt.want {
				t.Fatalf("isVotingMember(%d) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestRaft_PurgeDepartedVotes(t *testing.T) {
	r := newTestRaft(1) // remotes 2 and 3, self 1.
	r.witnesses[4] = newRemote(1, maxInflightMsgs)
	r.votes = map[uint64]bool{1: true, 2: true, 3: false, 4: true, 9: true}

	r.purgeDepartedVotes()
	if _, ok := r.votes[9]; ok {
		t.Fatal("a vote from a replica outside the configuration must be purged")
	}
	for _, id := range []uint64{1, 2, 3, 4} {
		if _, ok := r.votes[id]; !ok {
			t.Fatalf("vote from voting member %d was purged", id)
		}
	}

	// Shrinking the voting set purges the departed member's grant, so it
	// cannot be tallied against the now-smaller quorum.
	delete(r.remotes, 3)
	delete(r.witnesses, 4)
	r.purgeDepartedVotes()
	if _, ok := r.votes[3]; ok {
		t.Fatal("vote from departed remote 3 was not purged")
	}
	if _, ok := r.votes[4]; ok {
		t.Fatal("vote from departed witness 4 was not purged")
	}

	// A self vote cast before a self-removal committed must go too.
	r.setSelfMembership(false)
	r.purgeDepartedVotes()
	if _, ok := r.votes[1]; ok {
		t.Fatal("self vote survived self-removal")
	}
}

// TestRaft_RebuildRemotesFromMembership_UpdatesSelfMembership asserts the
// snapshot-restore path re-derives self membership: a replica removed while
// it lagged learns of its own removal only from the snapshot.
func TestRaft_RebuildRemotesFromMembership_UpdatesSelfMembership(t *testing.T) {
	r := newTestRaft(1)
	if r.selfRemoved {
		t.Fatal("precondition: replica 1 starts inside the configuration")
	}
	r.votes = map[uint64]bool{1: true}

	r.rebuildRemotesFromMembership(proto.Membership{
		Addresses: map[uint64]string{2: "addr2", 3: "addr3"},
	})
	if !r.selfRemoved {
		t.Fatal("a snapshot whose membership excludes self must set selfRemoved")
	}
	if _, ok := r.votes[1]; ok {
		t.Fatal("the self vote must be purged after a restored self-removal")
	}

	r.rebuildRemotesFromMembership(proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
	})
	if r.selfRemoved {
		t.Fatal("a snapshot whose membership includes self must clear selfRemoved")
	}
}

// ---------------------------------------------------------------------------
// M3: becomeLeader must clear inherited remote.active flags
// ---------------------------------------------------------------------------

// TestRaft_BecomeLeader_ClearsStaleRemoteActivity asserts that a node
// regaining leadership does not inherit the activity evidence of a previous
// tenure. Without the reset, hasQuorumActive() answers the first CheckQuorum
// window with acks a full leadership term old and the leader survives an
// extra election timeout having contacted nobody.
func TestRaft_BecomeLeader_ClearsStaleRemoteActivity(t *testing.T) {
	r := newTestRaft(1)
	r.preVote = false
	r.witnesses[4] = newRemote(1, maxInflightMsgs)

	// Term 1: lead and record activity from every peer.
	electLeader(r)
	r.drainMessages()
	for _, rm := range r.remotes {
		rm.setActive()
	}
	for _, wm := range r.witnesses {
		wm.setActive()
	}

	// Step down, then regain leadership at a later term.
	r.becomeFollower(r.term+1, noLeader)
	electLeader(r)
	r.drainMessages()
	if r.state != leader {
		t.Fatalf("precondition: state = %s, want leader", r.state)
	}

	for id, rm := range r.remotes {
		if rm.isActive() {
			t.Fatalf("remote %d is active on the first tick of a new leadership "+
				"term; the flag was inherited from a previous tenure", id)
		}
	}
	for id, wm := range r.witnesses {
		if wm.isActive() {
			t.Fatalf("witness %d is active on the first tick of a new leadership "+
				"term; the flag was inherited from a previous tenure", id)
		}
	}
}

// TestRaft_BecomeLeader_StaleActivityDoesNotSatisfyCheckQuorum is the
// behavioural half of M3: a re-elected leader that hears from nobody must
// step down at the end of its FIRST CheckQuorum window.
func TestRaft_BecomeLeader_StaleActivityDoesNotSatisfyCheckQuorum(t *testing.T) {
	r := newTestRaft(1)
	r.preVote = false

	electLeader(r)
	r.drainMessages()
	// Peers acknowledged during the first tenure.
	r.remotes[2].setActive()
	r.remotes[3].setActive()

	// Step down and regain leadership. No peer says anything afterwards.
	r.becomeFollower(r.term+1, noLeader)
	electLeader(r)
	r.drainMessages()
	if r.state != leader {
		t.Fatalf("precondition: state = %s, want leader", r.state)
	}

	for range r.electionTimeout + 1 {
		r.tickFunc(r)
	}

	if r.state != follower {
		t.Fatalf("state = %s, want follower: CheckQuorum accepted activity "+
			"recorded during a previous leadership term", r.state)
	}
}

// ---------------------------------------------------------------------------
// M4: quiesce is a local setting a peer cannot impose
// ---------------------------------------------------------------------------

// TestRaft_QuiesceMessage_IgnoredWhenDisabled asserts a node configured with
// Quiesce:false stays on its normal election timer even when a peer with
// Quiesce:true broadcasts a quiesce notification.
func TestRaft_QuiesceMessage_IgnoredWhenDisabled(t *testing.T) {
	r := followerWithKnownLeader(t, 5)
	r.preVote = false
	if r.quiesce.enabled {
		t.Fatal("precondition: quiesce must be disabled")
	}

	if err := r.step(proto.Message{Type: proto.Quiesce, From: 1, Term: 5}); err != nil {
		t.Fatalf("quiesce step: %v", err)
	}
	if r.quiesce.active {
		t.Fatal("a peer imposed quiesce on a node whose operator disabled it")
	}

	// The election timer must still run at its configured speed. Under the
	// bug the node uses quiesceElectionTimeout() = electionTimeout * 30 and
	// stays leaderless for that whole window.
	for range 2*r.electionTimeout + 1 {
		r.tickFunc(r)
	}
	if r.state == follower {
		t.Fatalf("still a follower after %d ticks: the node is running the "+
			"quiesce election timeout it never opted into", 2*r.electionTimeout+1)
	}
}

// TestRaft_QuiesceMessage_HonouredWhenEnabled is the control: with quiesce
// configured the leader's notification is still honoured.
func TestRaft_QuiesceMessage_HonouredWhenEnabled(t *testing.T) {
	r := newTestRaftQuiesce(2)
	if !r.quiesce.enabled {
		t.Fatal("precondition: quiesce must be enabled")
	}
	if err := r.step(proto.Message{Type: proto.Heartbeat, From: 1, Term: 5}); err != nil {
		t.Fatalf("heartbeat step: %v", err)
	}
	r.drainMessages()

	if err := r.step(proto.Message{Type: proto.Quiesce, From: 1, Term: 5}); err != nil {
		t.Fatalf("quiesce step: %v", err)
	}
	if !r.quiesce.active {
		t.Fatal("a quiesce-enabled follower must honour the leader's notification")
	}
}

// TestRaft_QuiesceMessage_IgnoredFromNonLeader keeps the existing sender
// check covered alongside the new enabled check.
func TestRaft_QuiesceMessage_IgnoredFromNonLeader(t *testing.T) {
	r := newTestRaftQuiesce(2)
	if err := r.step(proto.Message{Type: proto.Heartbeat, From: 1, Term: 5}); err != nil {
		t.Fatalf("heartbeat step: %v", err)
	}
	r.drainMessages()

	if err := r.step(proto.Message{Type: proto.Quiesce, From: 3, Term: 5}); err != nil {
		t.Fatalf("quiesce step: %v", err)
	}
	if r.quiesce.active {
		t.Fatal("quiesce accepted from a replica that is not the leader")
	}
}
