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
	"math"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Regression: a read lease must rest only on acknowledgements whose vote
// window the acknowledging replica actually promises — and keeps across a
// restart.
//
// The lease is sound while every ack it counts comes from a replica that will
// help elect no one else until the lease has ended. If that promise were
// assumed from the LEADER's configuration, and kept by the follower's own
// configuration and, after a restart, by a hold-down sized from the
// configuration it restarted into, a follower configured with a shorter
// election timeout, a shorter tick, no CheckQuorum or no LeaseRead — while
// running, or only after a restart — would make a weaker promise than the
// leader counted on, and a new leader could be elected inside the old one's
// lease. So every ack states its window (voteWindow), the leader counts only acks
// whose window covers its lease span (ackBacksLease), and the window is
// recorded in the hard state before any ack makes it, so a restarted replica
// honours it whatever it restarted into (armVoteHoldDown).

// promisedAck steps a heartbeat from leader 1 into follower f and returns
// f's HeartbeatResp to it.
func promisedAck(t *testing.T, f *raft) proto.Message {
	t.Helper()
	if err := f.step(proto.Message{Type: proto.Heartbeat, From: 1, Term: max(f.term, 1), LogIndex: 1}); err != nil {
		t.Fatalf("step(Heartbeat): %v", err)
	}
	for _, m := range f.drainMessages() {
		if m.Type == proto.HeartbeatResp && m.To == 1 {
			return m
		}
	}
	t.Fatal("follower sent no HeartbeatResp")
	return proto.Message{}
}

// TestLeaseVoteWindow_FollowerPromisesItsElectionSpan pins what an ack
// promises: the replica's own election timeout in real time, and nothing at
// all when a restart or a higher-term vote could break the promise.
func TestLeaseVoteWindow_FollowerPromisesItsElectionSpan(t *testing.T) {
	want := uint64(10 * testLeaseTick) // raftTestCfg's ElectionRTT of 10

	t.Run("lease_read", func(t *testing.T) {
		f := newTestRaftLeaseRead(2)
		if got := promisedAck(t, f).LogTerm; got != want {
			t.Fatalf("ack promises %d, want the election span %d", got, want)
		}
	})
	t.Run("no_lease_read", func(t *testing.T) {
		f := newTestRaft(2)
		setTestLeaseClock(f)
		if got := promisedAck(t, f).LogTerm; got != 0 {
			t.Fatalf("a replica without LeaseRead promises %d; it applies no hold-down, so it must promise nothing", got)
		}
	})
	t.Run("no_check_quorum", func(t *testing.T) {
		// The promise is kept in real time (votePromised), not by the
		// known-leader tests CheckQuorum enables, so it holds without them.
		f := newTestRaftLeaseRead(2)
		f.checkQuorum = false
		if got := promisedAck(t, f).LogTerm; got != want {
			t.Fatalf("a replica without CheckQuorum promises %d, want the election span %d", got, want)
		}
	})
	t.Run("tick_interval_unknown", func(t *testing.T) {
		cfg := raftTestCfgLeaseRead(1, 2)
		f, err := newRaft(cfg, newTestLogReaderWithMembership(map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}))
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		if got := promisedAck(t, f).LogTerm; got != 0 {
			t.Fatalf("a replica that does not know its tick promises %d", got)
		}
	})
}

// TestLeaseVoteWindow_LeaderCountsOnlyAcksThatCoverItsLease pins the
// leader's side at its boundary: an ack counts toward the lease exactly when
// its promised window is at least the lease span.
func TestLeaseVoteWindow_LeaderCountsOnlyAcksThatCoverItsLease(t *testing.T) {
	cases := []struct {
		name   string
		window func(span time.Duration) uint64
		valid  bool
	}{
		{name: "no_window", window: func(time.Duration) uint64 { return 0 }},
		{name: "one_below_span", window: func(s time.Duration) uint64 { return uint64(s) - 1 }},
		{name: "exactly_span", window: func(s time.Duration) uint64 { return uint64(s) }, valid: true},
		{name: "beyond_int64", window: func(time.Duration) uint64 { return math.MaxUint64 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRaftLeaseRead(1)
			electLeader(r)
			commitNoOp(r)
			round := sendLeaseRound(r)
			for _, from := range []uint64{2, 3} {
				if err := r.step(proto.Message{
					Type:     proto.HeartbeatResp,
					From:     from,
					Term:     r.term,
					LogIndex: round,
					LogTerm:  tc.window(r.leaseSpan),
				}); err != nil {
					t.Fatalf("step(HeartbeatResp): %v", err)
				}
			}
			if got := r.hasValidLease(); got != tc.valid {
				t.Fatalf("hasValidLease = %v, want %v for a window of %d against a span of %d",
					got, tc.valid, tc.window(r.leaseSpan), r.leaseSpan)
			}
		})
	}
}

// TestLeaseVoteWindow_PromiseIsRecordedBeforeAnyAck pins the ordering the
// restart guarantee rests on: the window a LeaseRead replica promises is in
// its hard state from the moment the promise is known, so the update that
// carries its first ack also carries the record, and the engine persists hard
// state before any ordered message leaves.
func TestLeaseVoteWindow_PromiseIsRecordedBeforeAnyAck(t *testing.T) {
	p, err := NewPeer(raftTestCfgLeaseRead(1, 2), func() *testLogReader {
		return newTestLogReaderWithMembership(map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"})
	}(), nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if got := p.r.hardState().VoteWindow; got != 0 {
		t.Fatalf("hard state owes %d before the promise is known", got)
	}
	p.SetTickInterval(testLeaseTick)
	want := uint64(10 * testLeaseTick)
	if !p.hasStateChanged() {
		t.Fatal("raising the owed window did not mark the hard state for persistence")
	}
	if err := p.Handle(proto.Message{Type: proto.Heartbeat, From: 1, Term: 1, LogIndex: 1}); err != nil {
		t.Fatalf("Handle(Heartbeat): %v", err)
	}
	upd, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if upd.State.VoteWindow != want {
		t.Fatalf("update records a window of %d, want %d", upd.State.VoteWindow, want)
	}
	var acked bool
	for _, m := range upd.Messages {
		if m.Type == proto.HeartbeatResp {
			acked = true
			if m.LogTerm > upd.State.VoteWindow {
				t.Fatalf("ack promises %d, more than the %d the same update records", m.LogTerm, upd.State.VoteWindow)
			}
		}
	}
	if !acked {
		t.Fatal("the update carries no HeartbeatResp")
	}
}

// newReplicaOwing builds replica 2 of a three-voter shard recovering a hard
// state that owes window, configured by cfg, on the frozen test clock.
func newReplicaOwing(t *testing.T, cfg config.Config, window time.Duration) *raft {
	t.Helper()
	lr := newTestLogReaderWithMembership(map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"})
	lr.state = proto.State{Term: holdDownTestTerm, VoteWindow: uint64(window)}
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	setTestLeaseClock(r)
	return r
}

// TestLeaseVoteWindow_RestartHonoursTheOwedWindow pins that a restarted
// replica withholds its vote for the window it recorded, whatever it
// restarted into, and then lowers the record to what it now promises.
func TestLeaseVoteWindow_RestartHonoursTheOwedWindow(t *testing.T) {
	owed := 10 * testLeaseTick
	cases := []struct {
		name      string
		cfg       func() config.Config
		wantAfter uint64 // the owed window once the hold-down has ended
	}{
		{name: "into_no_lease_read", cfg: func() config.Config {
			return raftTestCfgPreVote(1, 2, false)
		}},
		{name: "into_shorter_election_timeout", cfg: func() config.Config {
			cfg := raftTestCfgPreVote(1, 2, false)
			cfg.LeaseRead = true
			cfg.ElectionRTT = 5
			return cfg
		}, wantAfter: uint64(5 * testLeaseTick)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReplicaOwing(t, tc.cfg(), owed)
			if r.voteHoldDuration != owed {
				t.Fatalf("hold-down lasts %v, want the owed %v", r.voteHoldDuration, owed)
			}
			advanceLeaseClock(r, owed-time.Nanosecond)
			if resp := voteReply(t, r, false, false); !resp.Reject {
				t.Fatal("vote granted inside the window a previous incarnation owed")
			}
			if got := r.hardState().VoteWindow; got != uint64(owed) {
				t.Fatalf("owed window lowered to %d before the hold-down ended", got)
			}
			advanceLeaseClock(r, time.Nanosecond)
			if resp := voteReply(t, r, false, false); resp.Reject {
				t.Fatal("vote refused once the owed window had passed")
			}
			if got := r.hardState().VoteWindow; got != tc.wantAfter {
				t.Fatalf("owed window after the hold-down = %d, want %d", got, tc.wantAfter)
			}
		})
	}
}

// TestLeaseVoteWindow_TickLowersTheOwedWindowWithoutVoteTraffic pins that the
// record is lowered when the hold-down ends even on a replica no candidate
// asks, so a later restart does not pay again for a promise nobody holds.
func TestLeaseVoteWindow_TickLowersTheOwedWindowWithoutVoteTraffic(t *testing.T) {
	owed := 10 * testLeaseTick
	cfg := raftTestCfgPreVote(1, 2, false) // no LeaseRead: nothing is promised now
	p := newTestPeer(t, cfg, proto.State{Term: holdDownTestTerm, VoteWindow: uint64(owed)}, threeNodeMembership())
	p.prevState = p.r.hardState() // as recovered: nothing to persist yet
	advanceLeaseClock(p.r, owed)
	p.Tick()
	if got := p.r.hardState().VoteWindow; got != 0 {
		t.Fatalf("owed window after the hold-down = %d, want 0", got)
	}
	if !p.hasStateChanged() {
		t.Fatal("lowering the owed window did not mark the hard state for persistence")
	}
}

// restartIn replaces replica id of s with one recovered from its current hard
// state and log, constructed now, and returns when it restarted.
func restartIn(s *holdDownShard, id uint64, members map[uint64]string) time.Time {
	s.t.Helper()
	old := s.nodes[id]
	ents, err := old.log.entries(1, old.log.lastIndex()+1, 0)
	if err != nil {
		s.t.Fatalf("reading replica %d's log: %v", id, err)
	}
	lr := newTestLogReader(ents, old.hardState())
	lr.membership = proto.Membership{Addresses: members}
	s.nodes[id] = s.construct(id, lr)
	return s.clock.t
}

// leaderLeasingOnF1 builds the reported shard: L (1) leads, F2 (2) has lost
// it with its election timer one tick short of firing, and L holds a lease on
// its own ack plus F1's (3). It fails the test if L has no lease, and returns
// the shard.
func leaderLeasingOnF1(t *testing.T, configure func(id uint64, cfg *config.Config)) *holdDownShard {
	t.Helper()
	clock := &holdDownClock{t: time.Unix(1_700_000_000, 0)}
	s := &holdDownShard{t: t, clock: clock, nodes: map[uint64]*raft{}, cut: map[[2]uint64]bool{}, configure: configure}
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	for id := uint64(1); id <= 3; id++ {
		s.nodes[id] = s.construct(id, newTestLogReaderWithMembership(members))
	}
	clock.advance(s.nodes[1].voteHoldDuration)
	s.nodes[1].startElection()
	s.route()
	if got := s.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected replica 1 to lead, leaders = %v", got)
	}
	s.partition(1, 2)
	f2 := s.nodes[2]
	for f2.electionTick < f2.randomizedElectionTimeout-1 {
		clock.advance(testLeaseTick)
		f2.tickFunc(f2)
	}
	s.route()
	s.nodes[1].broadcastHeartbeatCtx(0)
	s.route()
	return s
}

// TestLeaseVoteWindow_RestartIntoWeakerConfigCannotElectInsideOldLease drives
// the reported scenario end to end: F1 acks under LeaseRead, then restarts
// into a configuration that promises less — or nothing — and F2 campaigns.
// No replica but L may lead while L's lease holds.
func TestLeaseVoteWindow_RestartIntoWeakerConfigCannotElectInsideOldLease(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	weaker := map[string]func(cfg *config.Config){
		"no_lease_read":              func(cfg *config.Config) { cfg.LeaseRead = false },
		"shorter_election_timeout":   func(cfg *config.Config) { cfg.ElectionRTT = 5 },
		"no_lease_read_no_prevote":   func(cfg *config.Config) { cfg.LeaseRead = false; cfg.PreVote = config.BoolPtr(false) },
		"shorter_timeout_no_prevote": func(cfg *config.Config) { cfg.ElectionRTT = 5; cfg.PreVote = config.BoolPtr(false) },
	}
	for name, weaken := range weaker {
		t.Run(name, func(t *testing.T) {
			restarting := false
			s := leaderLeasingOnF1(t, func(id uint64, cfg *config.Config) {
				if restarting && id == 3 {
					weaken(cfg)
				}
			})
			if !s.nodes[1].hasValidLease() {
				t.Fatal("leader holds no lease after F1 acked its heartbeat round")
			}
			restarting = true
			restarted := restartIn(s, 3, members)

			elected := time.Time{}
			for step := 0; step < 400 && elected.IsZero(); step++ {
				s.clock.advance(testLeaseTick / 4)
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
						t.Fatalf("replica %d elected %v after F1 restarted into a weaker configuration, while the old leader's lease is still valid",
							id, s.clock.t.Sub(restarted))
					}
					elected = s.clock.t
				}
			}
			if elected.IsZero() {
				t.Fatal("no new leader elected after the owed window: the hold-down must cost liveness only for its window")
			}
		})
	}
}

// TestLeaseVoteWindow_WeakerRunningFollowerBacksNoLease pins the running
// case: a follower whose configuration promises less than the leader's lease
// — or nothing — never lets the leader take a lease on its ack.
func TestLeaseVoteWindow_WeakerRunningFollowerBacksNoLease(t *testing.T) {
	weaker := map[string]func(cfg *config.Config){
		"no_lease_read":            func(cfg *config.Config) { cfg.LeaseRead = false },
		"shorter_election_timeout": func(cfg *config.Config) { cfg.ElectionRTT = 5 },
	}
	for name, weaken := range weaker {
		t.Run(name, func(t *testing.T) {
			s := leaderLeasingOnF1(t, func(id uint64, cfg *config.Config) {
				if id == 3 {
					weaken(cfg)
				}
			})
			if s.nodes[1].hasValidLease() {
				t.Fatal("leader took a lease on the ack of a follower that promises less than the lease")
			}
		})
	}
}

// ackedFollower returns replica 2 of a LeaseRead shard, past its post-restart
// hold-down, that has just acked a heartbeat from leader 1 at term 5: it owes
// leader 1 a vote window from the frozen clock's current time.
func ackedFollower(t *testing.T) *raft {
	t.Helper()
	f := newReplicaOwing(t, raftTestCfgLeaseRead(1, 2), 0)
	passVoteHoldDown(f)
	if err := f.step(proto.Message{Type: proto.Heartbeat, From: 1, Term: f.term}); err != nil {
		t.Fatalf("step(Heartbeat): %v", err)
	}
	f.drainMessages()
	if !f.votePromised() {
		t.Fatal("acking a heartbeat made no vote promise")
	}
	return f
}

// TestLeaseVotePromise_SurvivesAHigherTermMessage drives the reported
// scenario: a follower acks leader L, then a delayed higher-term response
// from an old campaign moves it to a new term with no known leader. The
// known-leader tests no longer apply, so without the window it would grant the
// next candidate's vote at once while L still counted its ack. The promise holds
// in real time until the window has passed.
func TestLeaseVotePromise_SurvivesAHigherTermMessage(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(map[bool]string{false: "vote", true: "pre_vote"}[preVote], func(t *testing.T) {
			f := ackedFollower(t)
			// The delayed rejection of an old pre-vote, at a higher term.
			if err := f.step(proto.Message{Type: proto.RequestPreVoteResp, From: 3, Term: f.term + 1, Reject: true}); err != nil {
				t.Fatalf("step(RequestPreVoteResp): %v", err)
			}
			f.drainMessages()
			if f.leaderID != noLeader {
				t.Fatalf("fixture: the higher-term response left leader %d known", f.leaderID)
			}
			advanceLeaseClock(f, time.Duration(f.voteWindow())-time.Nanosecond)
			if resp := voteReply(t, f, preVote, false); !resp.Reject {
				t.Fatal("vote granted inside the window this replica's ack promised")
			}
			advanceLeaseClock(f, time.Nanosecond)
			if resp := voteReply(t, f, preVote, false); resp.Reject {
				t.Fatal("vote refused once the promised window had passed")
			}
		})
	}
}

// TestLeaseVotePromise_CoalescedTicksDoNotCampaignEarly pins the ticker-phase
// case: a heartbeat resets the election count but not the ticker, so the
// timer can fire about one tick short of the promised window in real time.
// The campaign it would start is refused until the window has passed.
func TestLeaseVotePromise_CoalescedTicksDoNotCampaignEarly(t *testing.T) {
	f := ackedFollower(t)
	window := time.Duration(f.voteWindow())
	advanceLeaseClock(f, window-testLeaseTick) // the timer's minimum, one tick short
	for range 2 * f.electionTimeout {
		f.tickFunc(f)
	}
	if n := campaignMessages(f); n != 0 {
		t.Fatalf("follower sent %d campaign messages inside the window its ack promised", n)
	}
	// The timer that fired inside the window retries on the very next tick,
	// not a whole election timeout later.
	advanceLeaseClock(f, testLeaseTick)
	f.tickFunc(f)
	if n := campaignMessages(f); n == 0 {
		t.Fatal("follower did not campaign on the first tick after the promised window had passed")
	}
}

// TestLeaseVotePromise_OnlyThePromiseesTransferReleasesIt pins the one
// release: a leadership transfer started by the leader the promise was made
// to, which cleared its lease before starting it. Its TimeoutNow and its
// candidate's transfer-hinted vote at the next term go through; a TimeoutNow
// from anyone else, or a transfer hint at another term, does not.
func TestLeaseVotePromise_OnlyThePromiseesTransferReleasesIt(t *testing.T) {
	t.Run("hinted_vote_next_term", func(t *testing.T) {
		f := ackedFollower(t)
		if resp := voteReply(t, f, false, true); resp.Reject {
			t.Fatal("refused the transfer candidate of the leader this replica promised")
		}
	})
	t.Run("hinted_vote_later_term", func(t *testing.T) {
		f := ackedFollower(t)
		f.becomeFollower(f.term+1, noLeader)
		if resp := voteReply(t, f, false, true); !resp.Reject {
			t.Fatal("a transfer hint two terms past the promise released it")
		}
	})
	t.Run("timeout_now_from_promisee", func(t *testing.T) {
		f := ackedFollower(t)
		if err := f.step(proto.Message{Type: proto.TimeoutNow, From: 1, Term: f.term}); err != nil {
			t.Fatalf("TimeoutNow from the promisee: %v", err)
		}
		if f.state != candidate {
			t.Fatalf("state = %v after the promisee's TimeoutNow, want candidate", f.state)
		}
	})
	t.Run("timeout_now_from_another", func(t *testing.T) {
		f := ackedFollower(t)
		if err := f.handleTimeoutNow(proto.Message{Type: proto.TimeoutNow, From: 3, Term: f.term}); !errors.Is(err, ErrLeaseVoteHoldDown) {
			t.Fatalf("TimeoutNow from a replica the promise was not made to = %v, want ErrLeaseVoteHoldDown", err)
		}
	})
}

// BenchmarkFollowerHeartbeat measures a follower handling a heartbeat and
// producing its ack, with and without LeaseRead: the lease path takes on the
// vote promise (promiseVotes, one monotonic clock read per heartbeat).
func BenchmarkFollowerHeartbeat(b *testing.B) {
	for _, lease := range []bool{false, true} {
		b.Run(map[bool]string{false: "no_lease_read", true: "lease_read"}[lease], func(b *testing.B) {
			cfg := raftTestCfg(1, 2)
			cfg.LeaseRead = lease
			f, err := newRaft(cfg, newTestLogReaderWithMembership(map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}))
			if err != nil {
				b.Fatalf("newRaft: %v", err)
			}
			f.setTickInterval(testLeaseTick)
			f.becomeFollower(1, 1)
			hb := proto.Message{Type: proto.Heartbeat, From: 1, Term: 1, LogIndex: 1}
			b.ReportAllocs()
			for b.Loop() {
				if err := f.step(hb); err != nil {
					b.Fatalf("step(Heartbeat): %v", err)
				}
				f.drainMessages()
			}
		})
	}
}

// TestLeaseRead_ForfeitForTheTermOnceTimeoutNowIsSent pins the leader side
// of the transfer release: once a TimeoutNow has been sent, the leader takes
// no lease for the rest of its term, even after the transfer is abandoned,
// because the message may still arrive and release the promises a new lease
// would rest on. The next term may take a lease again.
func TestLeaseRead_ForfeitForTheTermOnceTimeoutNowIsSent(t *testing.T) {
	r := newTestRaftLeaseRead(1)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)

	if err := r.step(proto.Message{Type: proto.LeaderTransfer, Origin: proto.OriginLocal, Hint: 2}); err != nil {
		t.Fatalf("LeaderTransfer: %v", err)
	}
	var sent bool
	for _, m := range r.drainMessages() {
		sent = sent || m.Type == proto.TimeoutNow
	}
	if !sent {
		t.Fatal("fixture: no TimeoutNow sent to a caught-up target")
	}
	for range r.electionTimeout + 1 {
		r.tickFunc(r)
	}
	r.drainMessages()
	if r.transferTarget != noNode {
		t.Fatalf("fixture: transfer to %d not abandoned", r.transferTarget)
	}
	grantLease(r)
	if r.hasValidLease() {
		t.Fatal("leader took a lease in the term it sent TimeoutNow in")
	}

	r.becomeFollower(r.term+1, noLeader)
	electLeader(r)
	commitNoOp(r)
	grantLease(r)
	if !r.hasValidLease() {
		t.Fatal("leader could not take a lease in a later term")
	}
}

// TestLeaseVotePromise_DelayedTimeoutNowCannotElectInsideALaterLease drives
// the reported scenario: L starts a transfer to A, the TimeoutNow is
// delayed, the transfer times out, heartbeats overtake it and L's followers
// promise again; then the stale TimeoutNow arrives, A campaigns with the
// transfer hint and B's promise to L at that term releases its vote. No
// replica but L may lead while L holds a valid lease.
func TestLeaseVotePromise_DelayedTimeoutNowCannotElectInsideALaterLease(t *testing.T) {
	s := leaderLeasingOnF1(t, nil)
	l := s.nodes[1]
	s.cut = map[[2]uint64]bool{} // heal: every replica hears L again
	l.broadcastHeartbeatCtx(0)
	s.route()

	// Transfer to 3; hold its TimeoutNow back and deliver everything else.
	if err := l.step(proto.Message{Type: proto.LeaderTransfer, Origin: proto.OriginLocal, Hint: 3}); err != nil {
		t.Fatalf("LeaderTransfer: %v", err)
	}
	var stale []proto.Message
	for _, m := range l.drainMessages() {
		if m.Type == proto.TimeoutNow {
			stale = append(stale, m)
			continue
		}
		_ = s.nodes[m.To].step(m)
	}
	if len(stale) == 0 {
		t.Fatal("fixture: no TimeoutNow sent to a caught-up target")
	}
	s.route()

	// The transfer times out; L keeps heartbeating and its followers keep
	// acking — making fresh promises to L at the same term.
	for range l.electionTimeout + 1 {
		s.clock.advance(testLeaseTick / 4)
		l.tickFunc(l)
		s.route()
	}
	if l.transferTarget != noNode {
		t.Fatalf("fixture: transfer to %d not abandoned", l.transferTarget)
	}
	l.broadcastHeartbeatCtx(0)
	s.route()

	// The stale TimeoutNow arrives and A campaigns; L no longer hears the
	// vote traffic.
	s.partition(1, 2)
	s.partition(1, 3)
	for _, m := range stale {
		_ = s.nodes[m.To].step(m)
	}
	s.route()
	elected := false
	for _, id := range s.leaders() {
		if id == 1 {
			continue
		}
		elected = true
		if l.hasValidLease() {
			t.Fatalf("replica %d elected by a stale TimeoutNow while the old leader holds a lease", id)
		}
	}
	if !elected {
		t.Fatal("fixture: the stale TimeoutNow elected no one, so the scenario was not exercised")
	}
}
