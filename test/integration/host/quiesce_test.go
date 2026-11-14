//go:build integration

package host_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const quiesceShard uint64 = 917

// quiesceElectionTimeout is one election timeout in wall-clock time; the
// quiesce thresholds in docs/raft-consensus/engineering.md section 10 are
// multiples of it.
const quiesceElectionTimeout = time.Duration(testElectionRTT) * raftTick

// quiesceLivenessInterval is the leader's liveness-probe period under
// quiesce: 10 election timeouts.
const quiesceLivenessInterval = 10 * quiesceElectionTimeout

// quiesceIdle is the idle window each phase waits: 12 election timeouts,
// past the 10 that put the shard into quiesce.
const quiesceIdle = 12 * quiesceElectionTimeout

// quiesceLog records the election events one Host reports for the shard.
type quiesceLog struct {
	mu        sync.Mutex
	campaigns []config.EventShardInfo
	leaders   []config.LeaderInfo
	// quiesces and heartbeats are the arrival times of the Quiesce and
	// Heartbeat messages this host received for quiesceShard.
	quiesces   []time.Time
	heartbeats []time.Time
}

// quiesceMetrics records, on one host, the arrival of every Quiesce and
// Heartbeat message for quiesceShard. A follower that receives Quiesce has
// seen its leader enter quiescence; the Heartbeat arrivals give the leader's
// heartbeat cadence, which quiescence cuts from one per heartbeat RTT to one
// liveness probe per quiesceLivenessInterval.
type quiesceMetrics struct {
	quicraft.NoOpMetricsCollector
	log *quiesceLog
}

func (m quiesceMetrics) IncMessagesReceived(shardID, _ uint64, msgType uint64) {
	if shardID != quiesceShard || (msgType != proto.Quiesce && msgType != proto.Heartbeat) {
		return
	}
	now := time.Now()
	m.log.mu.Lock()
	defer m.log.mu.Unlock()
	if msgType == proto.Quiesce {
		m.log.quiesces = append(m.log.quiesces, now)
	} else {
		m.log.heartbeats = append(m.log.heartbeats, now)
	}
}

// quiescedWithin reports whether this host has received a Quiesce message
// and at most maxHeartbeats Heartbeat messages in the window ending now. It
// also returns both figures for diagnostics.
func (l *quiesceLog) quiescedWithin(window time.Duration, maxHeartbeats int) (ok bool, quiesces, heartbeats int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	from := time.Now().Add(-window)
	for _, at := range l.heartbeats {
		if at.After(from) {
			heartbeats++
		}
	}
	quiesces = len(l.quiesces)
	return quiesces > 0 && heartbeats <= maxHeartbeats, quiesces, heartbeats
}

// quiesceWindow is the trailing window over which assertQuiesced measures
// the leader's heartbeat cadence. Unquiesced, a follower receives one
// heartbeat per heartbeat RTT (one tick): 20 in this window. Quiesced, the
// leader sends only a liveness probe every quiesceLivenessInterval, so at
// most one lands in it; quiesceMaxHeartbeats allows one more for a
// heartbeat in flight when quiescence began.
const (
	quiesceWindow        = 2 * quiesceElectionTimeout
	quiesceMaxHeartbeats = 2
)

// assertQuiesced fails the test unless every live follower of the leader at
// leaderIdx has received a Quiesce message and, within quiesceIdle, sees the
// leader's heartbeats drop to the quiesced cadence for a whole
// quiesceWindow. With Config.Quiesce ignored neither holds, so the wake and
// failover paths that follow it are known to start from quiesced replicas.
func assertQuiesced(t *testing.T, c *testCluster, logs []*quiesceLog, leaderIdx int, phase string) {
	t.Helper()
	waitForConditionDiag(t, quiesceIdle, phase+": followers see the leader quiesced", func() bool {
		for _, i := range c.liveNodeIndexes() {
			if i == leaderIdx {
				continue
			}
			if ok, _, _ := logs[i].quiescedWithin(quiesceWindow, quiesceMaxHeartbeats); !ok {
				return false
			}
		}
		return true
	}, func() string {
		var b strings.Builder
		for _, i := range c.liveNodeIndexes() {
			if i == leaderIdx {
				continue
			}
			_, q, hb := logs[i].quiescedWithin(quiesceWindow, quiesceMaxHeartbeats)
			fmt.Fprintf(&b, "host[%d]: %d Quiesce received, %d heartbeats in the last %v (max %d)\n",
				i, q, hb, quiesceWindow, quiesceMaxHeartbeats)
		}
		return b.String()
	})
}

func (l *quiesceLog) listener() *config.EventListener {
	return &config.EventListener{
		OnCampaignLaunched: func(i config.EventShardInfo) {
			if i.ShardID != quiesceShard {
				return
			}
			l.mu.Lock()
			l.campaigns = append(l.campaigns, i)
			l.mu.Unlock()
		},
		OnLeaderUpdated: func(i config.LeaderInfo) {
			if i.ShardID != quiesceShard {
				return
			}
			l.mu.Lock()
			l.leaders = append(l.leaders, i)
			l.mu.Unlock()
		},
	}
}

// counts returns how many campaigns and leader updates were recorded.
func (l *quiesceLog) counts() (campaigns, leaders int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.campaigns), len(l.leaders)
}

// since returns the campaigns and leader updates recorded after the given
// counts.
func (l *quiesceLog) since(campaigns, leaders int) ([]config.EventShardInfo, []config.LeaderInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]config.EventShardInfo(nil), l.campaigns[campaigns:]...),
		append([]config.LeaderInfo(nil), l.leaders[leaders:]...)
}

// newQuiesceCluster builds a three-Host cluster like newTestCluster, with
// each Host reporting election events into its own quiesceLog.
func newQuiesceCluster(t *testing.T, logs []*quiesceLog) *testCluster {
	t.Helper()
	assertApplyOrder(t)

	n := len(logs)
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, n)
	addrs := make([]string, n)
	hosts := make([]*quicraft.Host, n)
	dirs := make([]string, n)
	for i := range n {
		cfg := testHostConfig(t, kernelAddr, leaves[i])
		cfg.EventListener = logs[i].listener()
		dirs[i] = cfg.NodeHostDir
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption(),
			quicraft.WithMetrics(quiesceMetrics{log: logs[i]}))
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] failed: %v", i, err)
		}
		addrs[i] = h.RaftAddress()
		hosts[i] = h
	}
	return &testCluster{
		hosts:        hosts,
		addrs:        addrs,
		stopped:      make([]atomic.Bool, n),
		nodeHostDirs: dirs,
		ca:           ca,
		leaves:       leaves,
		ids:          nodeIdentities(hosts),
		t:            t,
	}
}

// quiesceSnapshot is every log's counts at one instant.
func quiesceSnapshot(logs []*quiesceLog) [][2]int {
	out := make([][2]int, len(logs))
	for i, l := range logs {
		c, ld := l.counts()
		out[i] = [2]int{c, ld}
	}
	return out
}

// assertQuietSince fails if any live Host launched a campaign or saw a
// leader change since snap, or if a live Host does not report leaderID.
func assertQuietSince(t *testing.T, c *testCluster, logs []*quiesceLog, snap [][2]int, leaderID uint64, phase string) {
	t.Helper()
	for _, i := range c.liveNodeIndexes() {
		campaigns, leaders := logs[i].since(snap[i][0], snap[i][1])
		if len(campaigns) != 0 {
			t.Errorf("%s: host[%d] launched %d campaign(s) while idle: %+v", phase, i, len(campaigns), campaigns)
		}
		for _, l := range leaders {
			if l.LeaderID != leaderID {
				t.Errorf("%s: host[%d] saw leader change to %d (term %d) while idle; leader was %d",
					phase, i, l.LeaderID, l.Term, leaderID)
			}
		}
		info, err := c.hosts[i].ShardInfo(quiesceShard)
		if err != nil {
			t.Errorf("%s: host[%d] ShardInfo: %v", phase, i, err)
			continue
		}
		if info.LeaderID != leaderID {
			t.Errorf("%s: host[%d] reports leader %d, want %d", phase, i, info.LeaderID, leaderID)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

// proposeWithin proposes key=value through host idx and waits until every
// live replica applies it, failing past budget measured from the call.
func proposeWithin(t *testing.T, c *testCluster, idx int, key, value string, budget time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	//quicraft:direct-propose the caller names the leader it just observed
	rs, err := c.hosts[idx].Propose(ctx, quiesceShard, encodeKVPut(key, value))
	if err != nil {
		t.Fatalf("Propose(%s) through host[%d]: %v", key, idx, err)
	}
	rs.Release()
	live := c.liveNodeIndexes()
	waitForConditionDiag(t, budget-time.Since(start),
		fmt.Sprintf("%s=%s applied on every live replica", key, value),
		func() bool {
			for _, i := range live {
				m := resolveStateMachine(c, c.trackers, quiesceShard, i)
				if m == nil {
					return false
				}
				if v, ok := m.Get(key); !ok || v != value {
					return false
				}
			}
			return true
		},
		func() string { return shardStateDump(c, quiesceShard) },
	)
	return time.Since(start)
}

// TestQuiesce_IdleShardKeepsLeaderAndWakes drives the documented Quiesce
// behaviour on three Hosts over the real QUIC transport: an idle shard
// keeps its leader without elections, wakes on the next proposal and read,
// and a quiesced follower still elects a new leader when the leader dies.
// Before each wake and the failover, assertQuiesced checks the followers
// received the leader's Quiesce message and see its heartbeats cut to the
// quiesced cadence, so the test fails when Config.Quiesce is ignored.
func TestQuiesce_IdleShardKeepsLeaderAndWakes(t *testing.T) {
	logs := []*quiesceLog{{}, {}, {}}
	c := newQuiesceCluster(t, logs)
	defer c.close()

	c.startShardWithConfig(quiesceShard, func(shardID, replicaID uint64) config.Config {
		cfg := testShardConfig(shardID, replicaID)
		cfg.Quiesce = true
		return cfg
	})

	leaderIdx := c.waitForClusterLeader(quiesceShard, leaderElectionTimeout)
	for i := range 10 {
		proposeAndWait(t, c, c.trackers, quiesceShard, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	waitForReplication(t, c, c.trackers, quiesceShard, "k9", "v9", defaultTimeout)
	leaderIdx = c.findLeader(quiesceShard)
	if leaderIdx < 0 {
		t.Fatal("no leader after the initial proposals")
	}
	leaderID := uint64(leaderIdx + 1)
	waitForCondition(t, defaultTimeout, "every host reports the same leader", func() bool {
		for _, i := range c.liveNodeIndexes() {
			info, err := c.hosts[i].ShardInfo(quiesceShard)
			if err != nil || info.LeaderID != leaderID {
				return false
			}
		}
		return true
	})

	// (a) Idle for 12 election timeouts: no campaign, same leader.
	snap := quiesceSnapshot(logs)
	time.Sleep(quiesceIdle)
	assertQuiesced(t, c, logs, leaderIdx, "(a) idle")
	assertQuietSince(t, c, logs, snap, leaderID, "(a) idle")
	t.Logf("(a) idle %v: no campaign, leader %d on every host", quiesceIdle, leaderID)

	// (b) A proposal after the idle period applies everywhere within 5
	// election timeouts.
	took := proposeWithin(t, c, leaderIdx, "wake", "1", 5*quiesceElectionTimeout)
	t.Logf("(b) proposal after idle applied on all replicas in %v", took)

	// (c) After a further idle period a SyncRead on the leader returns the
	// last write.
	snap = quiesceSnapshot(logs)
	time.Sleep(quiesceIdle)
	assertQuiesced(t, c, logs, leaderIdx, "(c) idle")
	assertQuietSince(t, c, logs, snap, leaderID, "(c) idle")
	start := time.Now()
	got := readFromLeader(t, c, quiesceShard, "wake", 5*quiesceElectionTimeout)
	if got != "1" {
		t.Fatalf("(c) SyncRead after idle = %v, want 1", got)
	}
	t.Logf("(c) SyncRead after idle returned the last write in %v", time.Since(start))

	// (d) Idle again, then stop the leader: the survivors elect a new leader
	// within 4 liveness intervals and it serves a proposal.
	snap = quiesceSnapshot(logs)
	time.Sleep(quiesceIdle)
	assertQuiesced(t, c, logs, leaderIdx, "(d) idle")
	assertQuietSince(t, c, logs, snap, leaderID, "(d) idle")
	stopped := time.Now()
	c.stopNode(leaderIdx)
	budget := 4 * quiesceLivenessInterval
	newIdx := -1
	waitForConditionDiag(t, budget, "survivors elect a new leader", func() bool {
		newIdx = c.findLeader(quiesceShard)
		return newIdx >= 0 && newIdx != leaderIdx
	}, func() string { return shardStateDump(c, quiesceShard) })
	t.Logf("(d) host[%d] elected after the leader stopped in %v (budget %v)",
		newIdx, time.Since(stopped), budget)
	took = proposeWithin(t, c, newIdx, "after-failover", "2", 5*quiesceElectionTimeout)
	t.Logf("(d) proposal through the new leader applied on both survivors in %v", took)
}
