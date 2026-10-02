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
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// In-place observer promotion
//
// A replica's observer role follows the COMMITTED membership. config.IsObserver
// is only the initial hint, consulted while the configuration does not mention
// the replica. An observer whose own AddNode commits becomes a voter in place:
// it campaigns, can be elected, promises vote windows and serves ReadIndex,
// with no restart — etcd's learner promotion. The witness role is NOT derived:
// a witness holds no state machine, so a disagreement there is still latched.
// ---------------------------------------------------------------------------

// selfObserverMembership lists replica 1 as a non-voting observer beside two
// voters: the configuration an observer started with IsObserver=true agrees
// with.
func selfObserverMembership() proto.Membership {
	return proto.Membership{
		ConfigChangeID: 4,
		Addresses:      map[uint64]string{2: "addr2", 3: "addr3"},
		Observers:      map[uint64]string{1: "addr1"},
	}
}

// promotionPeer builds replica 4, started as an observer, in a shard whose
// committed membership lists it as one beside three voters.
func promotionPeer(t *testing.T) *Peer {
	t.Helper()
	cfg := testConfigWithID(1, 4)
	cfg.IsObserver = true
	return newTestPeer(t, cfg, proto.State{Term: 2}, proto.Membership{
		ConfigChangeID: 4,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		Observers:      map[uint64]string{4: "a4"},
	})
}

// promote applies the committed AddNode that promotes replica 4, through the
// same membership.apply + syncRaftRemotes pair the commit path runs.
func promote(t *testing.T, p *Peer) {
	t.Helper()
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "a4",
	}, 5); err != nil {
		t.Fatalf("apply AddNode promotion: %v", err)
	}
	p.publishMembership()
	if err := p.syncRaftRemotes(); err != nil {
		t.Fatalf("syncRaftRemotes: %v", err)
	}
}

// TestPromotion_ObserverBecomesVoterInPlace is the raft-level test of in-place
// promotion. Latching roleMismatch on the committed promotion (configured
// observer, listed voter) would make the replica refuse every campaign and
// every linearizable read until its shard was stopped and restarted with
// IsObserver=false.
func TestPromotion_ObserverBecomesVoterInPlace(t *testing.T) {
	quietLogs(t)
	p := promotionPeer(t)

	// Fixture: an agreeing observer — not latched, not a campaigner.
	if !p.r.isObserver || p.r.roleMismatch {
		t.Fatalf("fixture: isObserver=%v roleMismatch=%v, want true/false",
			p.r.isObserver, p.r.roleMismatch)
	}
	if !p.IsObserver() || p.IsFollower() {
		t.Fatalf("fixture: Peer.IsObserver=%v IsFollower=%v, want true/false",
			p.IsObserver(), p.IsFollower())
	}
	p.r.startElection()
	if p.r.state != follower {
		t.Fatalf("fixture: an observer campaigned (state=%v)", p.r.state)
	}

	promote(t, p)

	if p.r.isObserver {
		t.Fatal("the committed promotion did not make the replica a voter: it " +
			"stays an observer until its shard is restarted")
	}
	if p.r.roleMismatch {
		t.Fatal("the committed promotion latched a role mismatch: the promoted " +
			"replica refuses to campaign and to serve reads")
	}
	if p.IsObserver() || !p.IsFollower() {
		t.Fatalf("Peer.IsObserver=%v IsFollower=%v after promotion, want false/true",
			p.IsObserver(), p.IsFollower())
	}
	if got := quorum(p.r.remotes, p.r.witnesses); got != 3 {
		t.Fatalf("quorum after promotion = %d, want 3 of the 4 voters", got)
	}

	// It serves linearizable reads: a follower ReadIndex is forwarded.
	p.r.leaderID = 1
	p.r.msgs = p.r.msgs[:0]
	if err := p.r.handleFollowerReadIndex(proto.Message{Type: proto.ReadIndex, Hint: 9}); err != nil {
		t.Fatalf("the promoted replica refused a ReadIndex: %v", err)
	}
	if len(p.r.msgs) != 1 || p.r.msgs[0].To != 1 {
		t.Fatalf("ReadIndex was not forwarded to the leader: %+v", p.r.msgs)
	}

	// It campaigns.
	p.r.leaderID = noLeader
	p.r.startElection()
	if p.r.state != preCandidate && p.r.state != candidate {
		t.Fatalf("the promoted replica did not campaign (state=%v)", p.r.state)
	}
}

// TestPromotion_PromotedReplicaAcceptsTimeoutNow covers the other campaign
// entry point: a leader transferring leadership to the promoted replica.
func TestPromotion_PromotedReplicaAcceptsTimeoutNow(t *testing.T) {
	quietLogs(t)
	p := promotionPeer(t)

	// As an observer it ignores the transfer, without error.
	if err := p.r.handleTimeoutNow(proto.Message{Type: proto.TimeoutNow, From: 1, Term: p.r.term}); err != nil {
		t.Fatalf("observer TimeoutNow: %v", err)
	}
	if p.r.state != follower {
		t.Fatalf("an observer took a TimeoutNow (state=%v)", p.r.state)
	}

	promote(t, p)

	if err := p.r.handleTimeoutNow(proto.Message{Type: proto.TimeoutNow, From: 1, Term: p.r.term}); err != nil {
		t.Fatalf("promoted replica refused TimeoutNow: %v", err)
	}
	if p.r.state != candidate {
		t.Fatalf("promoted replica did not campaign on TimeoutNow (state=%v)", p.r.state)
	}
}

// TestPromotion_JoinerKeepsTheHintUntilListed pins config.IsObserver as the
// INITIAL hint: a replica the configuration does not mention keeps whatever it
// was started as, in both directions.
func TestPromotion_JoinerKeepsTheHintUntilListed(t *testing.T) {
	quietLogs(t)
	joining := proto.Membership{
		ConfigChangeID: 3,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
	}
	tests := []struct {
		name string
		hint bool
	}{
		{name: "started as observer", hint: true},
		{name: "started as voter", hint: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfigWithID(1, 4)
			cfg.IsObserver = tt.hint
			p := newTestPeer(t, cfg, proto.State{}, joining)
			if p.r.isObserver != tt.hint {
				t.Fatalf("raft isObserver = %v, want the hint %v", p.r.isObserver, tt.hint)
			}
			if p.IsObserver() != tt.hint {
				t.Fatalf("Peer.IsObserver = %v, want the hint %v", p.IsObserver(), tt.hint)
			}
			if p.r.roleMismatch {
				t.Fatal("a replica the configuration does not mention was latched")
			}
		})
	}
}

// TestPromotion_VoterAddedAsObserverBecomesOne: a joiner started as a voter
// that the leader added as an observer. The role follows the membership, so it is an ordinary observer — not
// latched, still able to serve reads — rather than a replica that refuses
// reads and makes its host exit.
func TestPromotion_VoterAddedAsObserverBecomesOne(t *testing.T) {
	quietLogs(t)
	cfg := testConfigWithID(1, 4)
	cfg.IsObserver = false
	p := newTestPeer(t, cfg, proto.State{}, proto.Membership{
		ConfigChangeID: 3,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
	})
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 4,
		Address:   "a4",
	}, 4); err != nil {
		t.Fatalf("apply AddNonVoting: %v", err)
	}
	p.publishMembership()
	if err := p.syncRaftRemotes(); err != nil {
		t.Fatalf("syncRaftRemotes: %v", err)
	}

	if !p.r.isObserver || p.r.roleMismatch {
		t.Fatalf("isObserver=%v roleMismatch=%v, want true/false", p.r.isObserver, p.r.roleMismatch)
	}
	if !p.IsObserver() {
		t.Fatal("Peer.IsObserver did not report the committed observer role")
	}
	p.r.leaderID = 1
	if err := p.r.handleFollowerReadIndex(proto.Message{Type: proto.ReadIndex, Hint: 3}); err != nil {
		t.Fatalf("an observer refused a ReadIndex: %v", err)
	}
	p.r.leaderID = noLeader
	p.r.startElection()
	if p.r.state != follower {
		t.Fatalf("a replica the configuration lists as an observer campaigned (state=%v)", p.r.state)
	}
}

// TestPromotion_DemotedLeaderStepsDown covers the reverse edge. A live voter
// cannot be asked to become an observer, but a snapshot (or a committed
// AddNonVoting converging a members-map seed) can deliver a configuration that
// lists this replica as one. A non-voter must not keep leading.
func TestPromotion_DemotedLeaderStepsDown(t *testing.T) {
	quietLogs(t)
	r := newTestRaft(1)
	r.becomeCandidate()
	r.becomeLeader()
	if r.state != leader {
		t.Fatalf("fixture: expected leader, got %v", r.state)
	}

	r.rebuildRemotesFromMembership(selfObserverMembership())

	if !r.isObserver {
		t.Fatal("a configuration listing this replica as an observer was not adopted")
	}
	if r.roleMismatch {
		t.Fatal("an observer demotion was latched as a witness role mismatch")
	}
	if r.state != follower {
		t.Fatalf("a leader the configuration calls a non-voter kept state %v", r.state)
	}
}

// TestPromotion_DemotedCandidateStepsDown: a campaign in flight when the
// replica learns it is a non-voter must not be allowed to finish, because its
// quorum counts the replica's own vote.
func TestPromotion_DemotedCandidateStepsDown(t *testing.T) {
	quietLogs(t)
	r := newTestRaft(1)
	r.becomeCandidate()

	r.rebuildRemotesFromMembership(selfObserverMembership())

	if r.state != follower {
		t.Fatalf("a candidate the configuration calls a non-voter kept state %v", r.state)
	}
}

// TestPromotion_WitnessRoleIsStillLatched: only the observer role follows the
// membership. A configured witness the membership lists as an observer is
// latched, because its missing state machine is not something a gate can fix.
func TestPromotion_WitnessRoleIsStillLatched(t *testing.T) {
	quietLogs(t)
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsWitness = true
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = selfObserverMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if !r.roleMismatch {
		t.Fatal("a configured WITNESS listed as an observer was not latched")
	}
	r.leaderID = 2
	if err := r.handleFollowerReadIndex(proto.Message{Type: proto.ReadIndex, Hint: 1}); !errors.Is(err, ErrWitnessRoleMismatch) {
		t.Fatalf("a witness with no state machine served a read: %v", err)
	}
}

// TestPromotion_LeaseReadPromiseIsRecordedBeforeAnyAck pins the vote-window
// obligation promotion creates. An observer promises nothing in its heartbeat
// acks; a voter running LeaseRead promises a window, and that promise must be
// in the hard state (VoteWindow) before any ack carries it, or a restart right
// after the promotion forgets it and votes inside a lease that still counts
// the ack.
//
// It also pins the hold-down: a LeaseRead observer's post-restart hold-down
// is sized from a zero promise, which holds votes down indefinitely. Promotion
// must re-size it, or the promoted replica can never vote or campaign.
func TestPromotion_LeaseReadPromiseIsRecordedBeforeAnyAck(t *testing.T) {
	quietLogs(t)
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsObserver = true
	cfg.LeaseRead = true
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = selfObserverMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.setTickInterval(10 * time.Millisecond)
	anchor := r.voteHoldStart
	r.now = func() time.Time { return anchor.Add(time.Hour) }

	if w := r.voteWindow(); w != 0 {
		t.Fatalf("fixture: an observer promised a vote window of %d", w)
	}
	if !r.voteHeldDown() {
		t.Fatal("fixture: a LeaseRead observer's hold-down was expected to be " +
			"unsized (held indefinitely); the test would prove nothing")
	}

	r.checkSelfRole(true, false, false) // the committed promotion

	want := uint64(r.electionSpan)
	if want == 0 {
		t.Fatal("fixture: no election span after setTickInterval")
	}
	if r.voteWindow() != want {
		t.Fatalf("promoted voteWindow = %d, want %d", r.voteWindow(), want)
	}
	if got := r.hardState().VoteWindow; got != want {
		t.Fatalf("hard state VoteWindow after promotion = %d, want %d: the "+
			"promise the next ack makes is not durable", got, want)
	}
	if r.voteHeldDown() {
		t.Fatal("the promoted replica's vote hold-down never ends: it can " +
			"neither vote nor campaign")
	}
}

// TestPromotion_RestartedPromotedObserverFoldsTailBeforeCampaign: a replica
// restarted after its promotion committed, from a base (snapshot) that still
// lists it as an observer, must fold the committed tail when its election
// timer fires — and then campaign as the voter it is. The observer test used
// to run BEFORE the fold, so such a replica refused every campaign until the
// commit pipeline happened to replay the tail.
func TestPromotion_RestartedPromotedObserverFoldsTailBeforeCampaign(t *testing.T) {
	quietLogs(t)
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	entries := []proto.Entry{
		bootstrap,
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t,
			proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 4, Address: "a4"})},
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t,
			proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"})},
	}
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 3})
	// The recovered base: a snapshot taken after the AddNonVoting, before the
	// promotion.
	lr.membership = proto.Membership{
		ConfigChangeID: 2,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		Observers:      map[uint64]string{4: "a4"},
	}
	cfg := testConfigWithID(1, 4)
	cfg.IsObserver = true // the operator restarted it with the original hint
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if !p.r.isObserver {
		t.Fatal("fixture: the recovered base must list the replica as an observer")
	}

	targets := tickToCampaign(t, p)

	if p.r.isObserver {
		t.Fatal("the committed promotion in the recovered tail was never folded")
	}
	if p.r.state == follower {
		t.Fatal("the promoted replica never campaigned after a restart")
	}
	if len(targets) != 3 {
		t.Fatalf("campaign solicited %v, want the three other voters", targets)
	}
}

// TestMembershipSnapshot_IsObserverOr covers the published-snapshot form of the
// rule, which Peer.IsObserver (and through it ShardInfo.IsObserver) reads.
func TestMembershipSnapshot_IsObserverOr(t *testing.T) {
	snap := newMembership(proto.Membership{
		Addresses: map[uint64]string{1: "a1"},
		Observers: map[uint64]string{2: "a2"},
		Witnesses: map[uint64]string{3: "a3"},
	}).snapshot()
	tests := []struct {
		name    string
		replica uint64
		hint    bool
		want    bool
	}{
		{name: "voter overrides an observer hint", replica: 1, hint: true, want: false},
		{name: "observer overrides a voter hint", replica: 2, hint: false, want: true},
		{name: "witness is not an observer", replica: 3, hint: true, want: false},
		{name: "unlisted keeps an observer hint", replica: 4, hint: true, want: true},
		{name: "unlisted keeps a voter hint", replica: 4, hint: false, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := snap.isObserverOr(tt.replica, tt.hint); got != tt.want {
				t.Fatalf("isObserverOr(%d, %v) = %v, want %v", tt.replica, tt.hint, got, tt.want)
			}
		})
	}
}
