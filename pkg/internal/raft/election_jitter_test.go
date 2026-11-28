package raft

import (
	mathrand "math/rand"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// jitterCampaign is one election a replica started: when, who, at which term.
type jitterCampaign struct {
	at   time.Duration
	id   uint64
	term uint64
}

// leaderLossRun builds a five-replica lease-read shard whose replicas tick
// with the given phase offsets within one tick interval, elects replica 1,
// runs it for steady ticks, then removes it (a closed host) and records
// every campaign the survivors start until a new leader is elected or
// horizon passes. It returns the campaigns and the time from the loss to the
// new leader (0 when none was elected).
func leaderLossRun(t *testing.T, seed int64, leaseRead bool, phases [5]time.Duration, latency time.Duration, timeouts map[uint64]int, steady int, horizon time.Duration) ([]jitterCampaign, time.Duration) {
	t.Helper()
	clock := &holdDownClock{t: time.Unix(1_700_000_000, 0)}
	s := &holdDownShard{t: t, clock: clock, nodes: map[uint64]*raft{}, cut: map[[2]uint64]bool{}}
	s.configure = func(_ uint64, cfg *config.Config) { cfg.LeaseRead = leaseRead }
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5"}
	for id := uint64(1); id <= 5; id++ {
		r := s.construct(id, newTestLogReaderWithMembership(members))
		r.rng = mathrand.New(mathrand.NewSource(seed*31 + int64(id)))
		r.resetElectionTimeout()
		s.nodes[id] = r
	}
	// Past every replica's start-up hold-down.
	clock.advance(s.nodes[1].voteHoldDuration + testLeaseTick)
	s.nodes[1].startElection()
	s.route()
	if got := s.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected replica 1 to lead, leaders = %v", got)
	}

	// Every message is delivered latency after it is sent: the time a
	// replica takes to persist its term and vote and put the message on
	// the wire. With latency 0 messages circulate to quiescence at once.
	type inflight struct {
		due time.Time
		m   proto.Message
	}
	var queue []inflight
	deliver := func() {
		if latency == 0 {
			s.route()
			return
		}
		for range 1000 {
			for _, n := range s.nodes {
				persistLog(n)
				for _, m := range n.drainMessages() {
					queue = append(queue, inflight{due: clock.t.Add(latency), m: m})
				}
			}
			var due []proto.Message
			kept := queue[:0]
			for _, f := range queue {
				if !f.due.After(clock.t) {
					due = append(due, f.m)
				} else {
					kept = append(kept, f)
				}
			}
			queue = kept
			if len(due) == 0 {
				return
			}
			for _, m := range due {
				if to, ok := s.nodes[m.To]; ok {
					_ = to.step(m) // refusals are protocol outcomes
				}
			}
		}
		t.Fatal("messages still circulating after 1000 rounds")
	}

	const step = testLeaseTick / 1000
	origin := clock.t
	var lost time.Time
	var campaigns []jitterCampaign
	for id, n := range s.nodes {
		n.onCampaign = func(_, _, term uint64) {
			if !lost.IsZero() {
				campaigns = append(campaigns, jitterCampaign{at: clock.t.Sub(lost), id: id, term: term})
			}
		}
	}
	tickDue := func(id uint64) bool {
		off := (clock.t.Sub(origin) - phases[id-1]) % testLeaseTick
		return off >= 0 && off < step
	}
	for elapsed := time.Duration(0); ; elapsed += step {
		if lost.IsZero() && elapsed >= time.Duration(steady)*testLeaseTick {
			lost = clock.t
			delete(s.nodes, 1)
			for id, ticks := range timeouts {
				s.nodes[id].randomizedElectionTimeout = ticks
			}
		}
		if !lost.IsZero() && clock.t.Sub(lost) > horizon {
			return campaigns, 0
		}
		clock.advance(step)
		for id := uint64(1); id <= 5; id++ {
			if n, ok := s.nodes[id]; ok && tickDue(id) {
				n.tickFunc(n)
			}
		}
		deliver()
		if !lost.IsZero() {
			if got := s.leaders(); len(got) == 1 {
				return campaigns, clock.t.Sub(lost)
			}
		}
	}
}

// jitterPhases staggers the five replicas' tickers 3ms apart within one tick
// interval, as hosts started a few milliseconds apart are: replicas whose
// timers expire on the same tick number campaign 3ms apart.
var jitterPhases = [5]time.Duration{0, 3 * time.Millisecond, 6 * time.Millisecond, 9 * time.Millisecond, 12 * time.Millisecond}

// jitterLatency is the delivery delay of every message: the time a replica
// takes to persist its term and vote and put the message on the wire. It is
// longer than the ticker skew, so replicas expiring on the same tick number
// campaign concurrently.
const jitterLatency = 20 * time.Millisecond

// TestElectionJitter_SameTickTieElectsInOneRound reproduces the soak failure
// in TestSoak_LeaderCloseDoesNotStallWrites: the leader of a five-replica
// group is closed and three of the four followers time out on the same tick.
// All three pass PreVote — each follower grants a pre-vote to every
// candidate prospecting a higher term once the leader is quiet — so all
// three become candidates for the same term and split it: each holds its own
// vote, the lone follower can make only one of them a pair, and none reaches
// three of five. The group then waits a second full randomized timeout.
//
// Randomized timeouts are whole ticks, electionTimeout + Intn(electionTimeout):
// ten values at the default ElectionRTT of 10, so the earliest expiry is
// shared by two of four followers about 19% of the time and by three about
// 2% — a three-way tie needs no correlated reset, only the draw.
//
// The group must elect within the round the tie started.
func TestElectionJitter_SameTickTieElectsInOneRound(t *testing.T) {
	timeouts := map[uint64]int{2: 12, 3: 12, 4: 12, 5: 19}
	c, d := leaderLossRun(t, 1, false, jitterPhases, jitterLatency, timeouts, 20, 60*testLeaseTick)
	if d == 0 || d > 14*testLeaseTick {
		t.Fatalf("leader elected after %v (0 = none within the horizon), want within the first round (14 ticks); campaigns %v", d, c)
	}
}

// TestElectionJitter_LeaderLossElectsInOneRound sweeps seeded draws over the
// same leader loss with LeaseRead off (the soak's configuration) and requires
// every run to elect in the first round: within the longest timeout a
// follower can draw (2*electionTimeout-1 ticks) plus slack for the round, and
// never a second randomized timeout later.
func TestElectionJitter_LeaderLossElectsInOneRound(t *testing.T) {
	const bound = 21 * testLeaseTick
	for seed := int64(1); seed <= 200; seed++ {
		c, d := leaderLossRun(t, seed, false, jitterPhases, jitterLatency, nil, 20, 60*testLeaseTick)
		if d == 0 || d > bound {
			t.Errorf("seed %d: leader elected after %v (0 = none within the horizon), want within %v; campaigns %v", seed, d, bound, c)
		}
	}
}
