package raft

import (
	mathrand "math/rand"
	"testing"
)

// TestElectionReview_TimeoutDrawHasElectionRTTSlots pins the timeout draw at
// raft.go:5122: randomizedElectionTimeout is a whole number of ticks in
// [electionTimeout, 2*electionTimeout), so there are exactly electionTimeout
// distinct values. With the default ElectionRTT of 10 and the harness's
// 500ms WAL-sync tick, two followers that draw the same value and were reset
// by the same heartbeat campaign within the same tick. For the four
// survivors of a closed leader in a five-node shard the lowest draw is tied
// about 19% of the time, which is the split-vote rate behind the
// TestSoak_LeaderCloseDoesNotStallWrites outages. This test passes on
// 88b940be: it records the behaviour, it does not judge it.
func TestElectionReview_TimeoutDrawHasElectionRTTSlots(t *testing.T) {
	const et = 10
	r := &raft{electionTimeout: et, rng: mathrand.New(mathrand.NewSource(1))}
	seen := map[int]bool{}
	for range 10000 {
		r.resetElectionTimeout()
		v := r.randomizedElectionTimeout
		if v < et || v >= 2*et {
			t.Fatalf("draw %d outside [%d, %d)", v, et, 2*et)
		}
		seen[v] = true
	}
	if len(seen) != et {
		t.Fatalf("draw has %d distinct values, want %d (one per tick)", len(seen), et)
	}

	// Rate at which the earliest of four followers' timers is tied.
	ties := 0
	const trials = 20000
	for range trials {
		minV, minN := 1<<30, 0
		for range 4 {
			r.resetElectionTimeout()
			switch v := r.randomizedElectionTimeout; {
			case v < minV:
				minV, minN = v, 1
			case v == minV:
				minN++
			}
		}
		if minN > 1 {
			ties++
		}
	}
	rate := float64(ties) / trials
	if rate < 0.15 || rate > 0.24 {
		t.Fatalf("tied-earliest-timer rate %.3f, want about 0.19", rate)
	}
}
