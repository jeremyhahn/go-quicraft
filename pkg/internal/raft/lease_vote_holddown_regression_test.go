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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Regression: a restarted replica of a LeaseRead shard must withhold its
// vote and pre-vote for one election timeout of real time.
//
// The lease rests on every follower that acked a heartbeat round granting no
// vote until its election timeout has passed since. A restart discards that
// timer: the replica comes back with no known leader, so without a hold-down it
// would grant the first up-to-date candidate at once, while the old leader kept
// counting its pre-restart ack toward a lease — a new leader elected inside the
// old one's lease, and a stale read. voteHeldDown restores the promise from the
// replica's own monotonic clock, anchored at construction.

// holdDownTestTerm is the persisted term a restarted replica recovers with.
const holdDownTestTerm = 5

// newRestartedReplica builds replica id of a three-voter shard as it comes
// back from a restart: persisted at holdDownTestTerm, no known leader, and
// constructed at the frozen test clock's t0, which is where the vote
// hold-down (if any) is anchored.
func newRestartedReplica(t *testing.T, id uint64, leaseRead, preVote bool) *raft {
	t.Helper()
	cfg := raftTestCfgPreVote(1, id, preVote)
	cfg.LeaseRead = leaseRead
	lr := newTestLogReaderWithMembership(map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"})
	lr.state = proto.State{Term: holdDownTestTerm}
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	setTestLeaseClock(r)
	return r
}

// voteReply steps a higher-term vote request (pre-vote when preVote) from
// candidate 2 into r and returns r's single reply to it.
func voteReply(t *testing.T, r *raft, preVote, transferHint bool) proto.Message {
	t.Helper()
	req := proto.Message{Type: proto.RequestVote, From: 2, Term: r.term + 1}
	want := proto.RequestVoteResp
	if preVote {
		req.Type = proto.RequestPreVote
		want = proto.RequestPreVoteResp
	}
	if transferHint {
		req.Hint = req.From
	}
	if err := r.step(req); err != nil {
		t.Fatalf("step(%s): %v", proto.MessageTypeName(req.Type), err)
	}
	var replies []proto.Message
	for _, m := range r.drainMessages() {
		if m.Type == want && m.To == 2 {
			replies = append(replies, m)
		}
	}
	if len(replies) != 1 {
		t.Fatalf("expected one %s to candidate 2, got %d", proto.MessageTypeName(want), len(replies))
	}
	return replies[0]
}

// tickWithoutTime delivers n ticks without moving the clock: a ticker that
// runs ahead of real time, or a burst of coalesced ticks after a pause.
func tickWithoutTime(r *raft, n int) {
	for range n {
		r.tickFunc(r)
	}
}

// campaignMessages returns how many vote requests r sent since the last drain.
func campaignMessages(r *raft) int {
	n := 0
	for _, m := range r.drainMessages() {
		if m.Type == proto.RequestVote || m.Type == proto.RequestPreVote {
			n++
		}
	}
	return n
}

func TestLeaseVoteHoldDown_RestartedFollowerWithholdsVotesUntilWindowEnds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ticks bool
	}{
		{name: "clock_only"},
		{name: "ticks_delivered", ticks: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, kind := range []struct {
				name     string
				preVote  bool
				transfer bool
			}{
				{name: "pre_vote", preVote: true},
				{name: "vote"},
				{name: "transfer_hinted_vote", transfer: true},
			} {
				t.Run(kind.name, func(t *testing.T) {
					r := newRestartedReplica(t, 3, true, kind.preVote)
					hold := r.voteHoldDuration
					if want := time.Duration(r.electionTimeout) * testLeaseTick; hold != want {
						t.Fatalf("hold-down window = %v, want electionTimeout ticks = %v", hold, want)
					}

					// At startup, and still one nanosecond before the window
					// ends, every grant is refused through the normal path.
					for _, at := range []time.Duration{0, hold - time.Nanosecond} {
						advanceLeaseClock(r, at-r.now().Sub(r.voteHoldStart))
						if tc.ticks {
							// Enough ticks for several election timeouts: a
							// tick count must not end the window early.
							tickWithoutTime(r, 3*r.electionTimeout)
							r.drainMessages()
						}
						resp := voteReply(t, r, kind.preVote, kind.transfer)
						if !resp.Reject {
							t.Fatalf("at +%v: vote granted inside the hold-down window", at)
						}
						if kind.preVote && resp.Term != r.term {
							t.Fatalf("at +%v: pre-vote refusal carries term %d, want own term %d",
								at, resp.Term, r.term)
						}
						if r.vote != noNode {
							t.Fatalf("at +%v: refused vote was recorded for %d", at, r.vote)
						}
					}

					// The window ends exactly electionTimeout ticks of real
					// time after construction; the same request is granted.
					advanceLeaseClock(r, time.Nanosecond)
					if resp := voteReply(t, r, kind.preVote, kind.transfer); resp.Reject {
						t.Fatal("vote refused after the hold-down window ended")
					}
					if !kind.preVote && r.vote != 2 {
						t.Fatalf("granted vote not recorded: vote = %d, want 2", r.vote)
					}
				})
			}
		})
	}
}

func TestLeaseVoteHoldDown_NonLeaseShardVotesImmediately(t *testing.T) {
	for _, kind := range []struct {
		name    string
		preVote bool
	}{
		{name: "pre_vote", preVote: true},
		{name: "vote"},
	} {
		t.Run(kind.name, func(t *testing.T) {
			r := newRestartedReplica(t, 3, false, kind.preVote)
			if r.voteHeldDown() {
				t.Fatal("a shard without LeaseRead reported a vote hold-down")
			}
			if resp := voteReply(t, r, kind.preVote, false); resp.Reject {
				t.Fatal("restarted non-LeaseRead follower refused an up-to-date candidate at startup")
			}
		})
	}
}

func TestLeaseVoteHoldDown_WitnessWithholdsVote(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 3, false)
	cfg.LeaseRead = true
	cfg.IsWitness = true
	lr := newTestLogReader(nil, proto.State{Term: holdDownTestTerm})
	lr.membership = proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
		Witnesses: map[uint64]string{3: "addr3"},
	}
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	setTestLeaseClock(r)

	// Witness acks count toward the lease (hasValidLease), so a restarted
	// witness is exactly as able to elect a second leader inside it.
	if resp := voteReply(t, r, false, false); !resp.Reject {
		t.Fatal("restarted witness granted a vote inside the hold-down window")
	}
	passVoteHoldDown(r)
	if resp := voteReply(t, r, false, false); resp.Reject {
		t.Fatal("witness refused a vote after the hold-down window ended")
	}
}

func TestLeaseVoteHoldDown_RestartedReplicaDoesNotCampaignInsideWindow(t *testing.T) {
	r := newRestartedReplica(t, 3, true, true)
	r.drainMessages()

	// A self-vote counts toward the quorum like any grant, so the election
	// timer firing inside the window must not open a campaign, however many
	// ticks arrive.
	tickWithoutTime(r, 3*r.electionTimeout)
	if n := campaignMessages(r); n != 0 || r.state != follower {
		t.Fatalf("campaigned inside the hold-down window: state %s, %d vote requests", r.state, n)
	}

	passVoteHoldDown(r)
	tickWithoutTime(r, 2*r.electionTimeout)
	if n := campaignMessages(r); n == 0 {
		t.Fatal("no campaign after the hold-down window ended")
	}
}

func TestLeaseVoteHoldDown_TimeoutNowRefusedInsideWindow(t *testing.T) {
	r := newRestartedReplica(t, 3, true, true)
	term := r.term

	err := r.step(proto.Message{Type: proto.TimeoutNow, From: 1, Term: r.term})
	if !errors.Is(err, ErrLeaseVoteHoldDown) {
		t.Fatalf("TimeoutNow inside the window: err = %v, want ErrLeaseVoteHoldDown", err)
	}
	if r.state != follower || r.term != term || campaignMessages(r) != 0 {
		t.Fatalf("TimeoutNow campaigned inside the window: state %s, term %d", r.state, r.term)
	}

	passVoteHoldDown(r)
	if err := r.step(proto.Message{Type: proto.TimeoutNow, From: 1, Term: r.term}); err != nil {
		t.Fatalf("TimeoutNow after the window: %v", err)
	}
	if r.state != candidate {
		t.Fatalf("TimeoutNow after the window: state %s, want candidate", r.state)
	}
}

func TestLeaseVoteHoldDown_UnknownTickIntervalHoldsVotes(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 3, false)
	cfg.LeaseRead = true
	lr := newTestLogReaderWithMembership(map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"})
	lr.state = proto.State{Term: holdDownTestTerm}
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	t0 := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return t0 }
	r.voteHoldStart = t0

	// Without the tick interval the window's length is unknown. A leader on
	// another host may still hold a lease on this replica's old ack, so the
	// vote stays held however much time passes.
	advanceLeaseClock(r, time.Hour)
	if resp := voteReply(t, r, false, false); !resp.Reject {
		t.Fatal("vote granted with no tick interval to size the hold-down")
	}

	// Once the interval arrives the window is measured from construction,
	// which here is long past.
	r.setTickInterval(testLeaseTick)
	if resp := voteReply(t, r, false, false); resp.Reject {
		t.Fatal("vote refused after the tick interval showed the window had ended")
	}
}

// holdDownClock is one monotonic clock shared by every replica of a
// simulated shard, so their elapsed times are directly comparable.
type holdDownClock struct{ t time.Time }

func (c *holdDownClock) now() time.Time          { return c.t }
func (c *holdDownClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// holdDownShard is a three-voter LeaseRead shard driven by hand: messages are
// routed between replicas, cut on partitioned links, and every replica reads
// the same clock.
type holdDownShard struct {
	t     *testing.T
	clock *holdDownClock
	nodes map[uint64]*raft
	cut   map[[2]uint64]bool
	// configure, when set, adjusts replica id's configuration before it is
	// constructed, for scenarios where replicas are configured differently.
	configure func(id uint64, cfg *config.Config)
}

// construct builds replica id from lr as a process started at the shard
// clock's current time would.
func (s *holdDownShard) construct(id uint64, lr *testLogReader) *raft {
	s.t.Helper()
	cfg := raftTestCfg(1, id) // PreVote and CheckQuorum on by default
	cfg.LeaseRead = true
	if s.configure != nil {
		s.configure(id, &cfg)
	}
	r, err := newRaft(cfg, lr)
	if err != nil {
		s.t.Fatalf("newRaft(%d): %v", id, err)
	}
	r.setTickInterval(testLeaseTick)
	r.now = s.clock.now
	r.voteHoldStart = s.clock.now()
	return r
}

// route delivers every outstanding message until none remain, persisting
// each replica's log first (the engine's commit worker does so before a
// durability-bearing message leaves). Messages on a cut link are dropped.
func (s *holdDownShard) route() {
	s.t.Helper()
	for range 1000 {
		var pending []proto.Message
		for _, n := range s.nodes {
			persistLog(n)
			pending = append(pending, n.drainMessages()...)
		}
		if len(pending) == 0 {
			return
		}
		for _, m := range pending {
			to, ok := s.nodes[m.To]
			if !ok || s.cut[[2]uint64{m.From, m.To}] {
				continue
			}
			_ = to.step(m) // refusals are protocol outcomes, not test failures
		}
	}
	s.t.Fatal("messages still circulating after 1000 rounds")
}

// partition cuts the links between a and b in both directions.
func (s *holdDownShard) partition(a, b uint64) {
	s.cut[[2]uint64{a, b}] = true
	s.cut[[2]uint64{b, a}] = true
}

// leaders returns the replicas that currently believe they lead.
func (s *holdDownShard) leaders() []uint64 {
	var ids []uint64
	for id, n := range s.nodes {
		if n.state == leader {
			ids = append(ids, id)
		}
	}
	return ids
}

// TestLeaseVoteHoldDown_RestartedFollowerCannotElectInsideOldLease drives the
// reported scenario end to end. L (1) leads; F2 (2) has lost it and its
// election timer is about to fire; L holds a lease on its own ack and F1's
// (3). F1 restarts, and F2 campaigns for a higher term. Without the hold-down
// F1 grants at once, F1 and F2 elect F2, and L keeps serving lease reads on
// F1's pre-restart ack. With the hold-down no replica other than L may lead
// while L's lease holds, and a new leader is elected once F1's window ends.
func TestLeaseVoteHoldDown_RestartedFollowerCannotElectInsideOldLease(t *testing.T) {
	clock := &holdDownClock{t: time.Unix(1_700_000_000, 0)}
	s := &holdDownShard{t: t, clock: clock, nodes: map[uint64]*raft{}, cut: map[[2]uint64]bool{}}
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	for id := uint64(1); id <= 3; id++ {
		s.nodes[id] = s.construct(id, newTestLogReaderWithMembership(members))
	}

	// The shard has been running longer than the hold-down.
	clock.advance(s.nodes[1].voteHoldDuration)

	// 1 campaigns and wins; its term-opening entry reaches both followers.
	s.nodes[1].startElection()
	s.route()
	if got := s.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected replica 1 to lead, leaders = %v", got)
	}

	// F2 loses L. L keeps running: it still hears from F1, so CheckQuorum
	// keeps it leader. F2's election timer runs to one tick short of firing,
	// in real time.
	s.partition(1, 2)
	f2 := s.nodes[2]
	for f2.electionTick < f2.randomizedElectionTimeout-1 {
		clock.advance(testLeaseTick)
		f2.tickFunc(f2)
	}
	s.route()

	// L broadcasts a heartbeat round and F1 acks it: with L's own, a quorum.
	s.nodes[1].broadcastHeartbeatCtx(0)
	s.route()
	if !s.nodes[1].hasValidLease() {
		t.Fatal("leader holds no lease after a quorum acked its heartbeat round")
	}

	// F1 restarts with what it had persisted, at the current time.
	old := s.nodes[3]
	ents, err := old.log.entries(1, old.log.lastIndex()+1, 0)
	if err != nil {
		t.Fatalf("reading replica 3's log: %v", err)
	}
	lr := newTestLogReader(ents, old.hardState())
	lr.membership = proto.Membership{Addresses: members}
	s.nodes[3] = s.construct(3, lr)
	restarted := clock.t

	// Step real time forward a quarter tick at a time, ticking F1 and F2 once
	// per tick interval and routing whatever they send. F2's timer fires on
	// its first tick. Invariant: while L's lease is valid no other replica
	// leads.
	elected := time.Time{}
	for step := 0; step < 400 && elected.IsZero(); step++ {
		clock.advance(testLeaseTick / 4)
		if step%4 == 3 {
			for _, id := range []uint64{2, 3} {
				s.nodes[id].tickFunc(s.nodes[id])
			}
		}
		s.route()
		for _, id := range s.leaders() {
			if id == 1 {
				continue
			}
			if s.nodes[1].hasValidLease() {
				t.Fatalf("replica %d elected %v after F1 restarted, while the old leader's lease is still valid",
					id, clock.t.Sub(restarted))
			}
			elected = clock.t
		}
	}
	if elected.IsZero() {
		t.Fatal("no new leader elected after the hold-down window: the hold-down must cost liveness only for its window")
	}
	if since := elected.Sub(restarted); since < s.nodes[3].voteHoldDuration {
		t.Fatalf("new leader elected %v after F1 restarted, inside its %v hold-down", since, s.nodes[3].voteHoldDuration)
	}
}

// TestLeaseVoteHoldDown_ConfigDefaultOff pins that the hold-down follows the
// shard's LeaseRead flag and nothing else: the zero Config has none.
func TestLeaseVoteHoldDown_ConfigDefaultOff(t *testing.T) {
	var cfg config.Config
	if cfg.LeaseRead {
		t.Fatal("LeaseRead must default to off")
	}
	r := newTestRaft(3)
	r.setTickInterval(testLeaseTick)
	if r.voteHeldDown() {
		t.Fatal("default shard reported a vote hold-down")
	}
}
