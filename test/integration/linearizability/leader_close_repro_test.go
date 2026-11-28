//go:build integration

package linearizability_test

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// leaderCloseReproClosesEnv overrides how many times
// TestRepro_LeaderCloseSplitVote closes the leader.
const leaderCloseReproClosesEnv = "QUICRAFT_REPRO_CLOSES"

// defaultLeaderCloseReproCloses is the default number of leader closes. It is
// enough samples for a p95 (the 19th of 20) and for the tie-share bound below
// to sit well under the share observed on main 88b940be (6 of 11 soak runs).
const defaultLeaderCloseReproCloses = 20

// electionEvent is one "election timeout reached" record raft logged.
type electionEvent struct {
	at      time.Time
	replica string
	role    string
	draw    int // the randomized election timeout, in ticks
}

// electionRecorder is a slog.Handler that records raft's election-timeout
// events and forwards every record to the handler it wraps.
type electionRecorder struct {
	next slog.Handler
	mu   *sync.Mutex
	evs  *[]electionEvent
}

func (h electionRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h electionRecorder) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "election timeout reached" {
		ev := electionEvent{at: r.Time}
		r.Attrs(func(a slog.Attr) bool {
			switch a.Key {
			case "replica":
				ev.replica = a.Value.String()
			case "role":
				ev.role = a.Value.String()
			case "election_timeout":
				ev.draw, _ = strconv.Atoi(a.Value.String())
			}
			return true
		})
		h.mu.Lock()
		*h.evs = append(*h.evs, ev)
		h.mu.Unlock()
	}
	if h.next.Enabled(ctx, r.Level) {
		return h.next.Handle(ctx, r)
	}
	return nil
}

func (h electionRecorder) WithAttrs(as []slog.Attr) slog.Handler {
	return electionRecorder{next: h.next.WithAttrs(as), mu: h.mu, evs: h.evs}
}

func (h electionRecorder) WithGroup(name string) slog.Handler {
	return electionRecorder{next: h.next.WithGroup(name), mu: h.mu, evs: h.evs}
}

// since returns the recorded events at or after t, in time order.
func (h electionRecorder) since(t time.Time) []electionEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []electionEvent
	for _, ev := range *h.evs {
		if !ev.at.Before(t) {
			out = append(out, ev)
		}
	}
	slices.SortFunc(out, func(a, b electionEvent) int { return a.at.Compare(b.at) })
	return out
}

// installElectionRecorder makes an electionRecorder the default slog handler
// for the rest of the test.
func installElectionRecorder(t *testing.T) electionRecorder {
	t.Helper()
	prev := slog.Default()
	// slog's built-in default handler writes through the log package, which
	// SetDefault points back at slog: wrapping it deadlocks. Forward to a
	// text handler of our own unless the soak's debug handler is installed.
	next := prev.Handler()
	if os.Getenv("QUICRAFT_SOAK_DEBUG") == "" {
		next = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	}
	rec := electionRecorder{next: next, mu: &sync.Mutex{}, evs: &[]electionEvent{}}
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return rec
}

// closeSample is what one leader close measured.
type closeSample struct {
	outage    time.Duration // close to the first write committed by a new leader
	termDelta uint64        // new leader's term minus the closed leader's
	firstDraw int           // the earliest follower timeout after the close, in ticks
	tied      []string      // followers that fired on that same draw within a tick
	gap       time.Duration // between the first two tied timeouts
	draws     []string      // every follower timeout after the close: replica:draw@offset
	// firstLost is set when the follower whose timeout fired first is not the
	// replica that became leader: its campaign failed (typically its pre-vote
	// was refused by followers that had heard from the old leader less than
	// ElectionRTT ticks earlier) and the outage ran on to a later draw.
	firstLost bool
}

// expectedMinTieShare is the chance that the smallest of k independent draws,
// uniform over n values, is shared by at least two of them:
// 1 - sum_{j=0}^{n-1} k * (1/n) * (j/n)^(k-1).
func expectedMinTieShare(k, n int) float64 {
	unique := 0.0
	for j := range n {
		unique += float64(k) / float64(n) * math.Pow(float64(j)/float64(n), float64(k-1))
	}
	return 1 - unique
}

// TestRepro_LeaderCloseSplitVote closes the leader of a five-replica group
// repeatedly and reports the write-outage distribution, the share of closes
// whose earliest follower timeout was a tie, and the share that split the
// vote.
//
// Each follower draws its election timeout uniformly from
// [ElectionRTT, 2*ElectionRTT) ticks, so with four followers left the
// earliest draw is shared with probability expectedMinTieShare(4, ElectionRTT)
// (0.19 for ElectionRTT 10). The hosts of this harness run their tickers
// within a few milliseconds of each other, so a tied draw means the two
// followers campaign together, and three or more together always split the
// vote. A split vote costs another full randomized timeout, which is what
// pushed TestSoak_LeaderCloseDoesNotStallWrites past 12s on main.
//
// The test fails when the p95 outage exceeds the no-split bound, or when the
// tie share or the split share exceeds the independent-draw expectation by
// more than three standard deviations. Neither is a bound on one sample.
func TestRepro_LeaderCloseSplitVote(t *testing.T) {
	const (
		numNodes = 5
		shardID  = 1
	)
	closes := defaultLeaderCloseReproCloses
	if v := os.Getenv(leaderCloseReproClosesEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("%s=%q: want a positive integer", leaderCloseReproClosesEnv, v)
		}
		closes = n
	}

	rec := installElectionRecorder(t)
	cluster := newTestCluster(t, numNodes)
	defer cluster.close()
	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	shardTerm := func(idx int) uint64 {
		cluster.hostsMu.RLock()
		defer cluster.hostsMu.RUnlock()
		info, err := cluster.hosts[idx].ShardInfo(shardID)
		if err != nil {
			return 0
		}
		return info.Term
	}

	samples := make([]closeSample, 0, closes)
	for i := range closes {
		leaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
		oldTerm := shardTerm(leaderIdx)

		closeStart := time.Now()
		cluster.stopNode(leaderIdx)
		newIdx := -1
		waitForCondition(t, failoverTimeout, fmt.Sprintf("a new leader after close %d", i), func() bool {
			newIdx = cluster.findLeader(shardID)
			return newIdx >= 0
		})
		proposeToLeader(t, cluster, shardID, encodeKVPut(fmt.Sprintf("repro-%d", i), "ok"), failoverTimeout)
		s := closeSample{outage: time.Since(closeStart)}
		if term := shardTerm(newIdx); term > oldTerm {
			s.termDelta = term - oldTerm
		}

		var followers []electionEvent
		for _, ev := range rec.since(closeStart) {
			if ev.role == "follower" {
				followers = append(followers, ev)
				s.draws = append(s.draws, fmt.Sprintf("r%s:%d@%v", ev.replica, ev.draw,
					ev.at.Sub(closeStart).Round(time.Millisecond)))
			}
		}
		if len(followers) > 0 {
			first := followers[0]
			s.firstDraw = first.draw
			s.firstLost = first.replica != strconv.Itoa(newIdx+1)
			for _, ev := range followers {
				if ev.draw == first.draw && ev.at.Sub(first.at) < raftTick {
					s.tied = append(s.tied, ev.replica)
				}
			}
			if len(s.tied) > 1 {
				s.gap = followers[1].at.Sub(first.at)
			}
		}
		samples = append(samples, s)
		t.Logf("close %d: leader node[%d] term %d -> node[%d] (+%d); outage %v; earliest draw %d tied=%v gap=%v first-lost=%v; draws %s",
			i, leaderIdx, oldTerm, newIdx, s.termDelta, s.outage.Round(time.Millisecond),
			s.firstDraw, s.tied, s.gap, s.firstLost, strings.Join(s.draws, " "))

		cluster.restartNode(leaderIdx, shardID)
		waitForCatchUp(t, cluster, shardID, leaderIdx, newIdx, failoverTimeout, func() bool { return false })
	}

	outages := make([]time.Duration, len(samples))
	ties, splits, lost := 0, 0, 0
	for i, s := range samples {
		outages[i] = s.outage
		if s.firstLost {
			lost++
		}
		if len(s.tied) > 1 {
			ties++
		}
		if s.termDelta >= 2 {
			splits++
		}
	}
	slices.Sort(outages)
	pct := func(p float64) time.Duration {
		return outages[int(math.Ceil(p*float64(len(outages))))-1]
	}
	p50, p95, worst := pct(0.50), pct(0.95), outages[len(outages)-1]

	n := float64(len(samples))
	expected := expectedMinTieShare(numNodes-1, int(testElectionRTT))
	tieBound := expected + 3*math.Sqrt(expected*(1-expected)/n)
	tieShare := float64(ties) / n
	splitShare := float64(splits) / n

	// Without a split the new leader is elected within the largest draw,
	// 2*ElectionRTT-1 ticks, plus a pre-vote and a vote round; a split costs
	// at least another ElectionRTT ticks on top. The slack covers the write
	// and a loaded host, and is the soak's 12s at the WAL-sync tick.
	p95Bound := time.Duration(2*testElectionRTT+4)*raftTick + 500*time.Millisecond

	t.Logf("leader-close distribution over %d closes (tick %v, ElectionRTT %d): p50 %v p95 %v max %v",
		len(samples), raftTick, testElectionRTT, p50.Round(time.Millisecond),
		p95.Round(time.Millisecond), worst.Round(time.Millisecond))
	t.Logf("earliest-tick ties: %d/%d = %.2f (independent draws expect %.2f, bound %.2f); split votes (term +2 or more): %d/%d = %.2f; first campaigner lost: %d/%d",
		ties, len(samples), tieShare, expected, tieBound, splits, len(samples), splitShare, lost, len(samples))

	if p95 > p95Bound {
		t.Errorf("p95 write outage after a leader close is %v, above the no-split bound %v", p95, p95Bound)
	}
	if tieShare > tieBound {
		t.Errorf("the earliest follower timeout was tied in %d of %d closes (%.2f), above %.2f: "+
			"the randomized election timeouts are not separating the followers", ties, len(samples), tieShare, tieBound)
	}
	if splitShare > tieBound {
		t.Errorf("%d of %d leader closes split the vote (%.2f), above %.2f", splits, len(samples), splitShare, tieBound)
	}
}
