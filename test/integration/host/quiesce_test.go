//go:build integration

package host_test

import (
	"context"
	"fmt"
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
func newQuiesceCluster(t *testing.T, logs []*quiesceLog, ms []*quiesceMetrics) *testCluster {
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
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption(), quicraft.WithMetrics(ms[i]))
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

// quiesceMetrics counts, for quiesceShard on one host, the Heartbeat and
// Quiesce messages its transport sent and the Quiesce messages it received.
// They are the observable signals of quiesce: a leader entering quiesce
// broadcasts Quiesce, and while quiesced it sends a heartbeat only once per
// liveness interval instead of every heartbeat RTT.
type quiesceMetrics struct {
	quicraft.NoOpMetricsCollector
	heartbeatsSent  atomic.Uint64
	quiesceSent     atomic.Uint64
	quiesceReceived atomic.Uint64
}

// IncMessagesSent counts the shard's Heartbeat and Quiesce sends.
func (m *quiesceMetrics) IncMessagesSent(shardID, _ uint64, msgType uint64) {
	if shardID != quiesceShard {
		return
	}
	switch msgType {
	case proto.Heartbeat:
		m.heartbeatsSent.Add(1)
	case proto.Quiesce:
		m.quiesceSent.Add(1)
	}
}

// IncMessagesReceived counts the shard's Quiesce receipts.
func (m *quiesceMetrics) IncMessagesReceived(shardID, _ uint64, msgType uint64) {
	if shardID == quiesceShard && msgType == proto.Quiesce {
		m.quiesceReceived.Add(1)
	}
}

// quiesceCounts is a point-in-time copy of every host's quiesceMetrics.
type quiesceCounts struct {
	heartbeatsSent, quiesceSent, quiesceReceived []uint64
}

func quiesceMetricsSnapshot(ms []*quiesceMetrics) quiesceCounts {
	out := quiesceCounts{
		heartbeatsSent:  make([]uint64, len(ms)),
		quiesceSent:     make([]uint64, len(ms)),
		quiesceReceived: make([]uint64, len(ms)),
	}
	for i, m := range ms {
		out.heartbeatsSent[i] = m.heartbeatsSent.Load()
		out.quiesceSent[i] = m.quiesceSent.Load()
		out.quiesceReceived[i] = m.quiesceReceived.Load()
	}
	return out
}

// quiesceCadenceWindow is how long assertQuiesced watches the leader's
// heartbeats once it has broadcast Quiesce. An active leader heartbeats each
// follower every heartbeat RTT (one tick), so it sends about
// 3*testElectionRTT heartbeats per follower in the window; a quiesced leader
// sends one liveness probe per follower per quiesceLivenessInterval, which is
// longer than the window, so at most one per follower falls inside it.
const quiesceCadenceWindow = 3 * quiesceElectionTimeout

// assertQuiesced waits until the shard has entered quiesce since snap and
// then checks the heartbeat cadence is suppressed. Entry is observed as the
// leader sending a Quiesce message to every live follower and every live
// follower receiving one; suppression as the leader sending at most one
// heartbeat per live follower over quiesceCadenceWindow. With Quiesce off,
// or its configuration not reaching the raft layer, no Quiesce message is
// ever sent and the wait fails.
func assertQuiesced(t *testing.T, c *testCluster, ms []*quiesceMetrics, snap quiesceCounts, leaderIdx int, phase string) {
	t.Helper()
	live := c.liveNodeIndexes()
	followers := 0
	for _, i := range live {
		if i != leaderIdx {
			followers++
		}
	}
	waitForConditionDiag(t, quiesceIdle+quiesceCadenceWindow,
		phase+": the leader broadcasts Quiesce and every live follower receives it",
		func() bool {
			if ms[leaderIdx].quiesceSent.Load()-snap.quiesceSent[leaderIdx] < uint64(followers) {
				return false
			}
			for _, i := range live {
				if i != leaderIdx && ms[i].quiesceReceived.Load() == snap.quiesceReceived[i] {
					return false
				}
			}
			return true
		},
		func() string {
			return fmt.Sprintf("now %+v, since %+v\n%s",
				quiesceMetricsSnapshot(ms), snap, shardStateDump(c, quiesceShard))
		},
	)
	before := ms[leaderIdx].heartbeatsSent.Load()
	time.Sleep(quiesceCadenceWindow)
	sent := ms[leaderIdx].heartbeatsSent.Load() - before
	if sent > uint64(followers) {
		t.Fatalf("%s: quiesced leader host[%d] sent %d heartbeats in %v, want at most %d (one liveness probe per follower)",
			phase, leaderIdx, sent, quiesceCadenceWindow, followers)
	}
	t.Logf("%s: quiesce entered; leader host[%d] sent %d heartbeats in %v", phase, leaderIdx, sent, quiesceCadenceWindow)
}

// idleQuiesced keeps the shard idle for at least quiesceIdle from start and
// asserts it entered quiesce during that time.
func idleQuiesced(t *testing.T, c *testCluster, ms []*quiesceMetrics, leaderIdx int, phase string) {
	t.Helper()
	start := time.Now()
	assertQuiesced(t, c, ms, quiesceMetricsSnapshot(ms), leaderIdx, phase)
	if rest := quiesceIdle - time.Since(start); rest > 0 {
		time.Sleep(rest)
	}
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
func TestQuiesce_IdleShardKeepsLeaderAndWakes(t *testing.T) {
	logs := []*quiesceLog{{}, {}, {}}
	ms := []*quiesceMetrics{{}, {}, {}}
	c := newQuiesceCluster(t, logs, ms)
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

	// (a) Idle for 12 election timeouts: the shard enters quiesce, no
	// campaign, same leader.
	snap := quiesceSnapshot(logs)
	idleQuiesced(t, c, ms, leaderIdx, "(a) idle")
	assertQuietSince(t, c, logs, snap, leaderID, "(a) idle")
	t.Logf("(a) idle %v: no campaign, leader %d on every host", quiesceIdle, leaderID)

	// (b) A proposal after the idle period applies everywhere within 5
	// election timeouts.
	took := proposeWithin(t, c, leaderIdx, "wake", "1", 5*quiesceElectionTimeout)
	t.Logf("(b) proposal after idle applied on all replicas in %v", took)

	// (c) After a further idle period a SyncRead on the leader returns the
	// last write.
	snap = quiesceSnapshot(logs)
	idleQuiesced(t, c, ms, leaderIdx, "(c) idle")
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
	idleQuiesced(t, c, ms, leaderIdx, "(d) idle")
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
