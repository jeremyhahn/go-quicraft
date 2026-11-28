package raft

import (
	mathrand "math/rand"
	"testing"
	"time"
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
func leaderLossRun(t *testing.T, seed int64, phases [5]time.Duration, steady int, horizon time.Duration) ([]jitterCampaign, time.Duration) {
	t.Helper()
	clock := &holdDownClock{t: time.Unix(1_700_000_000, 0)}
	s := &holdDownShard{t: t, clock: clock, nodes: map[uint64]*raft{}, cut: map[[2]uint64]bool{}}
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

	const step = testLeaseTick / 100
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
		s.route()
		if !lost.IsZero() {
			if got := s.leaders(); len(got) == 1 {
				return campaigns, clock.t.Sub(lost)
			}
		}
	}
}

func TestElectionJitter_Explore(t *testing.T) {
	phases := [5]time.Duration{0, 3 * time.Millisecond, 6 * time.Millisecond, 9 * time.Millisecond, 12 * time.Millisecond}
	for seed := int64(1); seed <= 20; seed++ {
		c, d := leaderLossRun(t, seed, phases, 20, 60*testLeaseTick)
		t.Logf("seed %d: elected after %v; campaigns %v", seed, d, c)
	}
}
