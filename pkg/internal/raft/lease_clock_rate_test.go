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
	"slices"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// The read lease and the vote window that backs it are measured on two
// different clocks: the leader's lease on its own monotonic clock from the
// time it SENT the acked heartbeat round, the follower's promise on the
// follower's monotonic clock from the time it acked. Clock OFFSET cancels
// out; clock RATE does not. A leader clock running slow stretches the lease
// in real time, a follower clock running fast shrinks the promise, and the
// lease is shortened by the drift bound (leaseDriftDivisor, or
// Config.MaxClockDrift) so that it still ends first.
//
// These tests run each replica on its own clock rate and check the bound is
// exact. The worst case the leader admits is a follower whose promise equals
// the lease span (ackBacksLease accepts promise >= leaseSpan): replica 2 runs
// ElectionRTT 8 against the leader's 10, so it promises exactly the leader's
// 8-tick lease span. Within the bound, the lease must have expired — and a
// read must take the heartbeat round — before that follower can grant a
// vote. Beyond it, the follower frees its vote while the lease it backs
// still holds and a new leader is elected inside the old one's lease, which
// is why the bound is a safety assumption and not a tuning knob.

// rateClock is one replica's monotonic clock in a shard whose clocks run at
// different rates. It reads the shard's real time scaled by 1 + ppm/1e6
// from the instant it was started: a positive ppm runs fast, a negative one
// slow.
type rateClock struct {
	real  *holdDownClock
	start time.Time
	ppm   int64
}

func (c *rateClock) now() time.Time {
	d := c.real.t.Sub(c.start)
	return c.start.Add(d + time.Duration(int64(d)*c.ppm/partsPerMillion))
}

// rateDriftStep is the real-time resolution of the simulation. Every case
// keeps the lease end and the promise end at least 7ms apart, so 1ms orders
// them without ambiguity.
const rateDriftStep = time.Millisecond

// rateDriftOutcome is what one run of the simulation observed, in real time
// since the acked heartbeat round was sent.
type rateDriftOutcome struct {
	leaseEnd   time.Duration // first instant the leader no longer held the lease
	promiseEnd time.Duration // first instant replica 2 could grant a vote
	elected    time.Duration // first instant a replica other than 1 led; 0 = none
	// electedInsideLease is set when that replica was elected while the
	// isolated leader still held its lease: a stale read was possible.
	electedInsideLease bool
}

// runRateDrift builds the three-replica shard with the leader (1) on a clock
// leaderPPM off real time and both followers on one followerPPM off it,
// elects 1, lets replica 2 alone ack a heartbeat round, isolates 1 and walks
// real time forward with the followers ticking freely until a new leader is
// elected or the window is long past.
func runRateDrift(t *testing.T, maxClockDrift float64, leaderPPM, followerPPM int64) rateDriftOutcome {
	t.Helper()
	clock := &holdDownClock{t: time.Unix(1_700_000_000, 0)}
	s := &holdDownShard{
		t: t, clock: clock, nodes: map[uint64]*raft{}, cut: map[[2]uint64]bool{},
		configure: func(id uint64, cfg *config.Config) {
			cfg.MaxClockDrift = maxClockDrift
			if id == 2 {
				// Promise exactly the leader's lease span: the least an ack
				// may promise and still back the lease.
				cfg.ElectionRTT = config.DefaultElectionRTT - 2
			}
		},
	}
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	for id := uint64(1); id <= 3; id++ {
		n := s.construct(id, newTestLogReaderWithMembership(members))
		ppm := followerPPM
		if id == 1 {
			ppm = leaderPPM
		}
		c := &rateClock{real: clock, start: clock.t, ppm: ppm}
		n.now = c.now
		n.voteHoldStart = c.now()
		s.nodes[id] = n
	}
	leader, f2 := s.nodes[1], s.nodes[2]
	if leader.leaseSpan != time.Duration(f2.voteWindow()) {
		t.Fatalf("replica 2 promises %v, want exactly the leader's lease span %v",
			time.Duration(f2.voteWindow()), leader.leaseSpan)
	}

	clock.advance(2 * leader.voteHoldDuration)
	leader.startElection()
	s.route()
	if got := s.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected replica 1 to lead, leaders = %v", got)
	}

	// Replica 3 backs no lease from here on, and every ack and promise made
	// during the election is let lapse, so the lease rests on replica 2's
	// next ack alone.
	s.partition(1, 3)
	clock.advance(2 * leader.electionSpan)
	if leader.hasValidLease() {
		t.Fatal("lease still held after every ack had lapsed")
	}

	sent := clock.t
	leader.broadcastHeartbeatCtx(0)
	s.route()
	if !leader.hasValidLease() {
		t.Fatal("leader holds no lease after replica 2 acked its heartbeat round")
	}
	if !leaseServesRead(t, leader, 1) {
		t.Fatal("read inside the lease was not served from it")
	}

	// Isolate the leader. It is not ticked, so CheckQuorum does not step it
	// down: the lease alone decides what it serves, which is the case the
	// bound has to cover. Replica 2 ticks once per step, far faster than real
	// time — the promise is kept on the clock, never in ticks — so its own
	// election timer has long expired and only the promise holds its vote.
	//
	// Replica 3 is not ticked, so it still counts leader 1 as live and
	// refuses replica 2's own pre-votes: no election starts by chance. The
	// test starts one itself, from replica 3, at the first instant replica 2
	// is free to vote, so whether a new leader can be elected inside the
	// lease does not depend on randomized election timing.
	s.partition(1, 2)
	f3 := s.nodes[3]
	var out rateDriftOutcome
	for elapsed := rateDriftStep; elapsed <= 2*leader.electionSpan; elapsed += rateDriftStep {
		clock.advance(rateDriftStep)
		f2.tickFunc(f2)
		s.route()
		leaseHeld := leader.hasValidLease()
		if !leaseHeld && out.leaseEnd == 0 {
			out.leaseEnd = elapsed
			if leaseServesRead(t, leader, 2) {
				t.Fatal("read after the lease expired was served from the lease")
			}
		}
		if got := s.leaders(); out.elected == 0 && (len(got) != 1 || got[0] != 1) {
			t.Fatalf("leaders %v at %v: an election started before replica 3 was told to campaign", got, elapsed)
		}
		if out.promiseEnd == 0 && !f2.votePromised() && !f2.voteHeldDown() {
			out.promiseEnd = elapsed
			f3.startElection()
			s.route()
			if got := s.leaders(); len(got) != 2 || !slices.Contains(got, 3) {
				t.Fatalf("replica 3 was not elected at %v, the first instant replica 2 was free to vote (leaders %v)",
					elapsed, got)
			}
			out.elected = elapsed
			out.electedInsideLease = leader.hasValidLease()
		}
		if out.elected != 0 && out.leaseEnd != 0 {
			break
		}
	}
	if out.leaseEnd == 0 || out.promiseEnd == 0 || out.elected == 0 {
		t.Fatalf("simulation did not run its course: %+v (sent at %v)", out, sent)
	}
	return out
}

// leaseServesRead steps a local ReadIndex into the leader and reports
// whether the lease answered it at once. Otherwise the read must be left
// pending on the heartbeat round.
func leaseServesRead(t *testing.T, r *raft, key uint64) bool {
	t.Helper()
	r.readStates = nil
	pending := len(r.pendingReadIndex)
	if err := r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: key}); err != nil {
		t.Fatalf("step(ReadIndex): %v", err)
	}
	if len(r.readStates) != 0 {
		r.readStates = nil
		return true
	}
	if len(r.pendingReadIndex) != pending+1 || !r.readIndexBroadcastPending {
		t.Fatalf("read neither served from the lease nor left on the heartbeat round (pending %d -> %d)",
			pending, len(r.pendingReadIndex))
	}
	return false
}

// rateDriftCase is one pair of clock rates against one drift bound. Drifts
// are in ppm of real time; within reports whether the pair's combined drift,
// (1+follower)/(1+leader) - 1, is inside the bound.
type rateDriftCase struct {
	name          string
	maxClockDrift float64 // 0 = the default, 1/leaseDriftDivisor
	leaderPPM     int64
	followerPPM   int64
	within        bool
}

func checkRateDrift(t *testing.T, cases []rateDriftCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := runRateDrift(t, tc.maxClockDrift, tc.leaderPPM, tc.followerPPM)
			if tc.within {
				if out.promiseEnd < out.leaseEnd || out.electedInsideLease {
					t.Fatalf("clock drift inside the bound, but replica 2 could vote %v after the round was sent and the lease its ack backs held until %v (new leader at %v, inside the lease: %v)",
						out.promiseEnd, out.leaseEnd, out.elected, out.electedInsideLease)
				}
				return
			}
			// Beyond the bound the window must open: this is what shows the
			// bound is exact, and what fails if the lease is shortened by
			// more than the bound claims.
			if out.promiseEnd >= out.leaseEnd {
				t.Fatalf("clock drift beyond the bound, yet the lease (ends %v) expired before replica 2's promise (ends %v): the lease is shorter than the bound it is documented to tolerate",
					out.leaseEnd, out.promiseEnd)
			}
			// The window is real: a candidate that campaigns the instant
			// replica 2 is free to vote is elected while the old leader still
			// serves reads from its lease.
			if !out.electedInsideLease {
				t.Fatalf("clock drift beyond the bound opened a window [%v, %v), yet the leader's lease had ended when replica 3 was elected at %v",
					out.promiseEnd, out.leaseEnd, out.elected)
			}
		})
	}
}

// TestLeaseRead_ClockRateDriftWithinDefaultBound holds the default 1% bound
// to its claim: 0.9% of drift, all on the leader, all on the follower, or
// split between them, leaves the lease safe; 1.1% does not. The reverse
// direction — a leader clock running FAST or a follower clock running SLOW —
// only shortens the lease in real time, so it is safe at any drift.
func TestLeaseRead_ClockRateDriftWithinDefaultBound(t *testing.T) {
	checkRateDrift(t, []rateDriftCase{
		{name: "leader_slow_0.9pct", leaderPPM: -9_000, within: true},
		{name: "leader_slow_1.1pct", leaderPPM: -11_000, within: false},
		{name: "follower_fast_0.9pct", followerPPM: 9_000, within: true},
		{name: "follower_fast_1.1pct", followerPPM: 11_000, within: false},
		{name: "split_0.9pct", leaderPPM: -4_500, followerPPM: 4_500, within: true},
		{name: "split_1.1pct", leaderPPM: -5_500, followerPPM: 5_500, within: false},
		{name: "reverse_leader_fast_1.1pct", leaderPPM: 11_000, within: true},
		{name: "reverse_follower_slow_1.1pct", followerPPM: -11_000, within: true},
		{name: "reverse_split_5pct", leaderPPM: 25_000, followerPPM: -25_000, within: true},
	})
}

// TestLeaseRead_ClockRateDriftWithinConfiguredBound runs the same check at
// non-default Config.MaxClockDrift values. A looser 2% bound covers the
// 1.1% drift the default does not, and is itself exact at 2%; a tighter
// 0.5% bound is exact at 0.5%.
func TestLeaseRead_ClockRateDriftWithinConfiguredBound(t *testing.T) {
	checkRateDrift(t, []rateDriftCase{
		{name: "2pct_leader_slow_1.1pct", maxClockDrift: 0.02, leaderPPM: -11_000, within: true},
		{name: "2pct_follower_fast_1.1pct", maxClockDrift: 0.02, followerPPM: 11_000, within: true},
		{name: "2pct_leader_slow_1.9pct", maxClockDrift: 0.02, leaderPPM: -19_000, within: true},
		{name: "2pct_leader_slow_2.1pct", maxClockDrift: 0.02, leaderPPM: -21_000, within: false},
		{name: "2pct_follower_fast_1.9pct", maxClockDrift: 0.02, followerPPM: 19_000, within: true},
		{name: "2pct_follower_fast_2.1pct", maxClockDrift: 0.02, followerPPM: 21_000, within: false},
		{name: "0.5pct_follower_fast_0.4pct", maxClockDrift: 0.005, followerPPM: 4_000, within: true},
		{name: "0.5pct_follower_fast_0.6pct", maxClockDrift: 0.005, followerPPM: 6_000, within: false},
		{name: "0.5pct_reverse_leader_fast_5pct", maxClockDrift: 0.005, leaderPPM: 50_000, within: true},
	})
}

// TestLeaseRead_MaxClockDriftSizesTheLease pins the lease length the bound
// produces, so the default stays exactly the old 1% (rounded up) and a
// configured bound replaces it.
func TestLeaseRead_MaxClockDriftSizesTheLease(t *testing.T) {
	span := time.Duration(config.DefaultElectionRTT-2) * testLeaseTick
	cases := []struct {
		drift float64
		want  time.Duration
	}{
		{0, span - span/leaseDriftDivisor},
		{0.01, span - span/100},
		{0.02, span - span/50},
		{0.005, span - span/200},
		{config.MaxClockDriftMax, span / 2},
	}
	for _, tc := range cases {
		cfg := raftTestCfgLeaseRead(1, 1)
		cfg.MaxClockDrift = tc.drift
		r, err := newRaft(cfg, newTestLogReaderWithMembership(map[uint64]string{1: "a", 2: "b", 3: "c"}))
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		r.setTickInterval(testLeaseTick)
		if r.leaseSpan != span || r.leaseDuration != tc.want {
			t.Fatalf("MaxClockDrift %v: lease span %v duration %v, want %v and %v",
				tc.drift, r.leaseSpan, r.leaseDuration, span, tc.want)
		}
	}
}

// TestLeaseDriftShortfall checks the shortfall rounds up, never overflows
// and consumes the whole span at a bound of a million ppm or more.
func TestLeaseDriftShortfall(t *testing.T) {
	cases := []struct {
		d    time.Duration
		ppm  uint64
		want time.Duration
	}{
		{0, 10_000, 0},
		{1, 10_000, 1},              // rounds up
		{100, 10_000, 1},            // exact
		{101, 10_000, 2},            // rounds up
		{7_999_999, 10_000, 80_000}, // (7_999_999+99)/100
		{time.Duration(1 << 62), 1, 4_611_686_018_428},
		{time.Duration(1 << 62), 500_000, time.Duration(1 << 61)},
		{time.Second, partsPerMillion, time.Second},
		{time.Second, 2 * partsPerMillion, time.Second},
	}
	for _, tc := range cases {
		if got := leaseDriftShortfall(tc.d, tc.ppm); got != tc.want {
			t.Errorf("leaseDriftShortfall(%d, %d) = %d, want %d", tc.d, tc.ppm, got, tc.want)
		}
	}
	for d := time.Duration(0); d < 1000; d++ {
		if got, want := leaseDriftShortfall(d, partsPerMillion/leaseDriftDivisor), (d+leaseDriftDivisor-1)/leaseDriftDivisor; got != want {
			t.Fatalf("default shortfall of %d = %d, want the old ceil(d/%d) = %d", d, got, leaseDriftDivisor, want)
		}
	}
}
