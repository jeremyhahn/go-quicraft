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

//go:build integration

package raft_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// Multi-Node Cluster Types
// ---------------------------------------------------------------------------

// testCluster manages a group of Hosts for multi-node integration tests.
// Each Host runs on a separate listen address with real QUIC transport.
type testCluster struct {
	hosts []*quicraft.Host
	addrs []string
	// nodeHostDirs[i] is hosts[i]'s NodeHostDir. Snapshot artifacts are
	// written beneath it, so tests that must prove a snapshot actually
	// reached disk need the per-host root.
	nodeHostDirs []string
	trackers     []*smTracker
	t            *testing.T
}

// newTestCluster creates a 3-node cluster with real QUIC transport.
// Each host gets a unique listen address on the loopback interface.
// The cluster is not started (no shards); call startShard to begin.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	addrs := make([]string, numNodes)
	for i := range numNodes {
		// Reserve from a window outside the kernel's ephemeral range, so the
		// port cannot be handed to anything else before NewHost binds it.
		// Binding 127.0.0.1:0 and closing it would leave nothing holding the
		// port between the close and the bind, and two nodes of one cluster
		// could be given the same one.
		addrs[i] = netport.MustReserveUDP(t.Fatalf)
	}

	// One CA, one leaf — one keypair, one node identity — per host; see
	// test/testpki.
	ca := testpki.NewCA(t)
	hosts := make([]*quicraft.Host, numNodes)
	nodeHostDirs := make([]string, numNodes)
	for i := range numNodes {
		cfg := testHostConfig(t, addrs[i], ca.Leaf(t))
		nodeHostDirs[i] = cfg.NodeHostDir
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
		if err != nil {
			// Clean up any previously created hosts.
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] at %s failed: %v", i, addrs[i], err)
		}
		hosts[i] = h
	}

	return &testCluster{
		hosts:        hosts,
		addrs:        addrs,
		nodeHostDirs: nodeHostDirs,
		t:            t,
	}
}

// startShard starts a shard on all nodes in the cluster. Returns the
// smTrackers for each node so tests can inspect state machine state.
func (c *testCluster) startShard(shardID uint64) []*smTracker {
	c.t.Helper()

	members := make(map[uint64]string, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	// Every founder names every member's identity: the same map on each.
	identities := make(map[uint64]string, len(c.hosts))
	for i, h := range c.hosts {
		identities[uint64(i+1)] = h.NodeIdentity()
	}

	trackers := make([]*smTracker, len(c.hosts))
	for i, h := range c.hosts {
		replicaID := uint64(i + 1)
		createFn, tracker := kvCreateFunc()
		trackers[i] = tracker

		scfg := testShardConfig(shardID, replicaID)
		scfg.MemberIdentities = identities
		if err := h.StartShard(members, false, createFn, scfg); err != nil {
			c.t.Fatalf("StartShard[%d] failed: %v", i, err)
		}
	}

	c.trackers = trackers
	return trackers
}

// close shuts down all hosts in the cluster.
func (c *testCluster) close() {
	for _, h := range c.hosts {
		h.Close()
	}
}

// findLeader returns the index of the node that is the current leader
// for the given shard, or -1 if no leader is known.
func (c *testCluster) findLeader(shardID uint64) int {
	for i, h := range c.hosts {
		info, err := h.GetShardInfo(shardID)
		if err != nil {
			continue
		}
		if info.IsLeader {
			return i
		}
	}
	return -1
}

// leaderHost returns the Host that currently leads shardID, or nil when no
// node reports leadership. Returned as a Host rather than an index so callers
// resolve and address the leader in one step.
func (c *testCluster) leaderHost(shardID uint64) *quicraft.Host {
	for _, h := range c.hosts {
		info, err := h.GetShardInfo(shardID)
		if err != nil {
			continue
		}
		if info.IsLeader {
			return h
		}
	}
	return nil
}

// shardView renders every host's view of shardID — replica, term, the leader
// it believes in and whether it believes it leads — for failure messages. A
// leader-only call that fails with "leader unknown" is only diagnosable with
// this: a term that advanced means leadership legitimately moved, while the
// same term with no leader anywhere means a node gave up leadership without
// an election.
func (c *testCluster) shardView(shardID uint64) string {
	var b strings.Builder
	for i, h := range c.hosts {
		if i > 0 {
			b.WriteString("; ")
		}
		if h == nil {
			fmt.Fprintf(&b, "host[%d]=stopped", i)
			continue
		}
		info, err := h.GetShardInfo(shardID)
		if err != nil {
			fmt.Fprintf(&b, "host[%d]=%v", i, err)
			continue
		}
		fmt.Fprintf(&b, "host[%d] replica=%d term=%d leader=%d isLeader=%t",
			i, info.ReplicaID, info.Term, info.LeaderID, info.IsLeader)
	}
	return b.String()
}

// onLeader runs fn against whichever host leads shardID AT THE MOMENT OF EACH
// ATTEMPT, re-finding the leader and retrying while fn is refused as
// not-leader, for at most budget. Any other error is returned on its first
// occurrence, and so is success: the operation still has to succeed on a
// leader, it just need not be the leader observed before the call.
//
// It exists because observing a leader does not pin it. Between
// waitForClusterLeader and a leader-only call a loaded host can miss a
// heartbeat, so a pre-vote or CheckQuorum moves leadership and the call is
// refused with "leader unknown" (#309). That is Raft working, not the
// operation failing.
//
// Only use it for operations whose not-leader refusal is safe to repeat:
// a refusal wrapped in MaybeApplied means the first attempt may still take
// effect, which for a session register leaves at most an unused session.
func (c *testCluster) onLeader(shardID uint64, budget time.Duration, fn func(h *quicraft.Host) error) error {
	retry := time.NewTicker(pollInterval)
	defer retry.Stop()
	deadline := time.NewTimer(budget)
	defer deadline.Stop()

	// Reported if the budget runs out without a leader ever being found,
	// which is itself a not-leader condition.
	var lastErr error = quicraft.ErrNotLeader
	for {
		if h := c.leaderHost(shardID); h != nil {
			err := fn(h)
			if err == nil || !proposer.IsNotLeaderRejection(err) {
				return err
			}
			lastErr = err
		}
		select {
		case <-deadline.C:
			return lastErr
		case <-retry.C:
		}
	}
}

// waitForClusterLeader waits until one of the nodes in the cluster
// becomes leader for the given shard.
func (c *testCluster) waitForClusterLeader(shardID uint64, timeout time.Duration) int {
	c.t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		idx := c.findLeader(shardID)
		if idx >= 0 {
			return idx
		}
		select {
		case <-deadline.C:
			c.t.Fatalf("timed out after %v waiting for cluster leader on shard %d", timeout, shardID)
			return -1
		case <-ticker.C:
		}
	}
}

// waitForAgreedLeader waits until every host knows a leader for shardID, all
// of them name the same replica, and the host with that replica ID reports
// itself leader. It returns the agreed leader's replica ID.
//
// It does not pin the leader seen first: CheckQuorum may step a leader down
// and a new election move leadership while the followers are still learning
// it, and nodes that converge on the new leader satisfy the property.
func (c *testCluster) waitForAgreedLeader(shardID uint64, timeout time.Duration) uint64 {
	c.t.Helper()
	agreed := func() (uint64, bool) {
		var leaderID uint64
		for _, h := range c.hosts {
			if h == nil {
				return 0, false
			}
			lid, known, err := h.GetLeaderID(shardID)
			if err != nil || !known || lid == 0 || (leaderID != 0 && lid != leaderID) {
				return 0, false
			}
			leaderID = lid
		}
		for _, h := range c.hosts {
			info, err := h.GetShardInfo(shardID)
			if err == nil && info.ReplicaID == leaderID {
				return leaderID, info.IsLeader
			}
		}
		return 0, false
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if lid, ok := agreed(); ok {
			return lid
		}
		select {
		case <-deadline.C:
			c.t.Fatalf("timed out after %v waiting for all nodes to agree on one leader of shard %d: %s",
				timeout, shardID, c.shardView(shardID))
			return 0
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Leader Election
// ---------------------------------------------------------------------------

// TestMultiNode_LeaderElection verifies that a 3-node cluster elects
// exactly one leader for a shard. This exercises the full QUIC transport
// path with real peer-to-peer communication.
func TestMultiNode_LeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	if leaderIdx < 0 || leaderIdx >= 3 {
		t.Fatalf("invalid leader index: %d", leaderIdx)
	}

	// Wait for exactly one leader. During election transitions, multiple
	// nodes may momentarily report IsLeader before convergence.
	waitForCondition(t, leaderElectionTimeout, "exactly 1 leader", func() bool {
		count := 0
		for _, h := range cluster.hosts {
			info, err := h.GetShardInfo(1)
			if err != nil {
				return false
			}
			if info.IsLeader {
				count++
			}
		}
		return count == 1
	})
}

// TestMultiNode_LeaderElection_AllNodesKnowLeader verifies that after
// election, all nodes in the cluster agree on who the leader is.
func TestMultiNode_LeaderElection_AllNodesKnowLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Every node, the leader included, must converge on one leader that
	// names itself leader. Leadership may move after the first leader is
	// seen; that is allowed, a split or unknown view is not.
	cluster.waitForAgreedLeader(1, leaderElectionTimeout)
}

// ---------------------------------------------------------------------------
// Multi-Node Proposals and Replication
// ---------------------------------------------------------------------------

// TestMultiNode_ProposeAndReplicate verifies that a proposal submitted
// to the leader is replicated to all followers. This is the core Raft
// replication test, exercising: proposal submission, log replication via
// QUIC, commit notification, and state machine apply on all nodes.
func TestMultiNode_ProposeAndReplicate(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	// Propose via whoever leads now, and wait for the commit: the old form
	// released the RequestState without reading the result, so it asserted only
	// that the proposal was accepted.
	proposeToLeader(t, cluster, 1, encodeKVPut("replicated-key", "replicated-value"), defaultTimeout)

	// Verify the leader's SM has the data.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	if leaderSM == nil {
		t.Fatal("leader state machine not found")
	}
	waitForCondition(t, defaultTimeout, "leader SM apply", func() bool {
		_, ok := leaderSM.Get("replicated-key")
		return ok
	})

	// Verify all followers replicate the data.
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		replicaID := uint64(i + 1)
		followerSM := trackers[i].Get(1, replicaID)
		if followerSM == nil {
			t.Fatalf("follower[%d] state machine not found", i)
		}
		followerTicker := time.NewTicker(10 * time.Millisecond)
		followerDeadline := time.NewTimer(defaultTimeout)
	pollLoop:
		for {
			val, ok := followerSM.Get("replicated-key")
			if ok && val == "replicated-value" {
				followerTicker.Stop()
				followerDeadline.Stop()
				goto nextFollower
			}
			select {
			case <-followerDeadline.C:
				followerTicker.Stop()
				break pollLoop
			case <-followerTicker.C:
			}
		}
		// Timeout - dump diagnostics.
		for di, dh := range cluster.hosts {
			dReplicaID := uint64(di + 1)
			info, derr := dh.GetShardInfo(1)
			if derr != nil {
				t.Logf("  node[%d]: GetShardInfo error: %v", di, derr)
			} else {
				t.Logf("  node[%d]: isLeader=%v leaderID=%d lastApplied=%d",
					di, info.IsLeader, info.LeaderID, info.LastApplied)
			}
			dSM := trackers[di].Get(1, dReplicaID)
			if dSM != nil {
				t.Logf("  node[%d] SM: len=%d smApplied=%d data=%v",
					di, dSM.Len(), dSM.LastApplied(), dSM.Snapshot())
			} else {
				t.Logf("  node[%d] SM: nil", di)
			}
		}
		t.Fatalf("follower[%d] (replica %d): SM never applied 'replicated-key' within %v",
			i, replicaID, defaultTimeout)
	nextFollower:
	}
}

// TestMultiNode_ProposeAndReplicate_MultipleEntries verifies that
// multiple sequential proposals are all replicated to all nodes
// in order.
func TestMultiNode_ProposeAndReplicate_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	numEntries := 10
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		proposeToLeader(t, cluster, 1, encodeKVPut(key, value), defaultTimeout)
	}

	// Wait for all entries to be applied on all nodes.
	lastKey := fmt.Sprintf("key-%03d", numEntries-1)
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] all entries applied", i),
			func() bool {
				_, ok := sm.Get(lastKey)
				return ok
			},
		)

		// Verify all entries are present.
		for j := 0; j < numEntries; j++ {
			key := fmt.Sprintf("key-%03d", j)
			expectedVal := fmt.Sprintf("value-%03d", j)
			val, ok := sm.Get(key)
			if !ok {
				t.Errorf("node[%d]: key %q not found", i, key)
				continue
			}
			if val != expectedVal {
				t.Errorf("node[%d]: key %q = %q, want %q", i, key, val, expectedVal)
			}
		}
	}
}

// TestMultiNode_ProposeToFollower verifies that proposing to a follower
// succeeds via automatic forwarding to the leader. Raft followers forward
// proposals to the current leader when the leader ID is known.
func TestMultiNode_ProposeToFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Find a follower.
	followerIdx := -1
	for i := range cluster.hosts {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Warmup: write via leader so the follower learns the leader ID
	// through the replication heartbeat.
	// Bound for the StaleRead assertion at the end; the WRITE below goes
	// through the current leader rather than this cached host.
	leaderHost := cluster.hosts[leaderIdx]
	proposeToLeader(t, cluster, 1, encodeKVPut("warmup", "init"), defaultTimeout)

	// Wait until the follower knows who the leader is.
	waitForLeader(t, cluster.hosts[followerIdx], 1, leaderElectionTimeout)

	// Propose via follower — this should succeed via leader forwarding.
	// The FOLLOWER is the subject of this assertion, so this one call must keep
	// its cached host: routing it to the leader would test nothing.
	followerHost := cluster.hosts[followerIdx]
	//quicraft:direct-propose the follower is the subject: this asserts leader forwarding
	rs, err := followerHost.Propose(ctx, 1, encodeKVPut("follower-forwarded", "value"))
	if err != nil {
		t.Fatalf("Propose via follower failed: %v", err)
	}
	rs.Release()

	// Wait for the forwarded proposal to be applied on the leader, then verify.
	waitForCondition(t, defaultTimeout, "follower-forwarded applied", func() bool {
		result, readErr := leaderHost.StaleRead(ctx, 1, "follower-forwarded")
		if readErr != nil {
			return false
		}
		val, ok := result.(string)
		return ok && val == "value"
	})
}

// ---------------------------------------------------------------------------
// Multi-Node Leader Failure and Re-election
// ---------------------------------------------------------------------------

// TestMultiNode_LeaderFailover verifies that when the leader is stopped,
// a new leader is elected from the remaining nodes. This exercises the
// Raft election timeout, candidate promotion, and vote exchange over
// QUIC transport.
//
// It also verifies LEADER COMPLETENESS: every entry committed under the old
// leader is present in the new leader's log and state machine, in the new
// term. Electing a leader and then only writing something new is satisfied by
// a leader that silently lost all pre-failover committed state, so the
// pre-failover entries are re-read through the new leader — linearizably, and
// directly out of the new leader's own state machine.
func TestMultiNode_LeaderFailover(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	oldLeaderReplicaID := uint64(leaderIdx + 1)

	oldLeaderInfo, err := cluster.hosts[leaderIdx].GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on the original leader failed: %v", err)
	}
	oldTerm := oldLeaderInfo.Term
	if oldTerm == 0 {
		t.Fatal("original leader reports term 0")
	}

	// Write initial data via the first leader, awaiting apply on each so the
	// entries are known-committed before the leader is killed. Values embed
	// the old leader's identity and term so a value that survives is
	// provably the one written pre-failover.
	preFailover := make(map[string]string, 6)
	preFailover["before-failover"] = "original"
	for i := 0; i < 5; i++ {
		preFailover[fmt.Sprintf("before-failover-%d", i)] =
			fmt.Sprintf("committed-by-replica-%d-term-%d-seq-%d", oldLeaderReplicaID, oldTerm, i)
	}
	for key, value := range preFailover {
		proposeCommitted(t, cluster.hosts[leaderIdx], 1, key, value, defaultTimeout)
	}

	// Wait for the proposals to be applied on ALL followers. In a 3-node
	// cluster, both remaining followers must have established QUIC
	// connections (via replication from the leader) before we stop the
	// leader, otherwise re-election between the two survivors can stall.
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm == nil {
			t.Fatalf("follower[%d] state machine not found", i)
		}
		for key, value := range preFailover {
			k, v := key, value
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("follower[%d] apply %q before failover", i, k),
				func() bool {
					got, ok := sm.Get(k)
					return ok && got == v
				},
			)
		}
	}

	// Stop the leader.
	cluster.hosts[leaderIdx].Close()

	// Wait for a new leader to be elected among the remaining nodes.
	newLeaderIdx := -1
	waitForCondition(t, leaderElectionTimeout*2, "new leader elected", func() bool {
		for i, h := range cluster.hosts {
			if i == leaderIdx {
				continue
			}
			info, getErr := h.GetShardInfo(1)
			if getErr == nil && info.IsLeader {
				newLeaderIdx = i
				return true
			}
		}
		return false
	})

	if newLeaderIdx < 0 {
		t.Fatal("no new leader elected after failover")
	}
	newLeaderReplicaID := uint64(newLeaderIdx + 1)
	if newLeaderReplicaID == oldLeaderReplicaID {
		t.Errorf("new leader %d should differ from old leader %d",
			newLeaderReplicaID, oldLeaderReplicaID)
	}
	newLeaderHost := cluster.hosts[newLeaderIdx]

	newLeaderInfo, err := newLeaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on the new leader failed: %v", err)
	}
	if newLeaderInfo.Term <= oldTerm {
		t.Errorf("new leader term = %d, want > old leader term %d: the failover did not start a new term",
			newLeaderInfo.Term, oldTerm)
	}

	// Propose via the new leader and wait for apply. Once this resolves the
	// new leader has applied through an index strictly above every
	// pre-failover entry, so its state machine can be inspected directly
	// without racing catch-up.
	proposeCommitted(t, newLeaderHost, 1, "after-failover", "survived", defaultTimeout*2)

	newLeaderSM := trackers[newLeaderIdx].Get(1, newLeaderReplicaID)
	if newLeaderSM == nil {
		t.Fatalf("new leader[%d] state machine not found", newLeaderIdx)
	}
	assertSMHasExact(t, newLeaderSM, fmt.Sprintf("new leader[%d]", newLeaderIdx),
		"after-failover", "survived")

	// LEADER COMPLETENESS: every entry committed in the old term must be in
	// the new leader's own applied state...
	for key, value := range preFailover {
		assertSMHasExact(t, newLeaderSM, fmt.Sprintf("new leader[%d]", newLeaderIdx), key, value)
	}

	// ...and must still be readable linearizably through the new leader.
	for key, value := range preFailover {
		syncReadExact(t, newLeaderHost, 1, key, value, defaultTimeout)
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Reads
// ---------------------------------------------------------------------------

// TestMultiNode_StaleRead_FromFollower verifies that stale reads from
// a follower return data that was replicated from the leader.
func TestMultiNode_StaleRead_FromFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	// Write via whoever leads now; leaderIdx below only picks a follower.
	proposeToLeader(t, cluster, 1, encodeKVPut("follower-read-key", "follower-read-value"), defaultTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Find a follower and wait for replication.
	followerIdx := -1
	for i := range cluster.hosts {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}

	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	waitForCondition(t, defaultTimeout, "follower replication", func() bool {
		_, ok := followerSM.Get("follower-read-key")
		return ok
	})

	// Stale read from follower.
	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "follower-read-key")
	if err != nil {
		t.Fatalf("StaleRead from follower failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("StaleRead returned unexpected type: %T", result)
	}
	if val != "follower-read-value" {
		t.Errorf("StaleRead returned %q, want %q", val, "follower-read-value")
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Session Management
// ---------------------------------------------------------------------------

// TestMultiNode_Session_OnLeader verifies that session creation and
// close work on the leader node in a multi-node cluster.
func TestMultiNode_Session_OnLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Both calls go to whoever leads at the moment of the call: leadership
	// can legitimately move after waitForClusterLeader returns (#309).
	var session *quicraft.Session
	err := cluster.onLeader(1, leaderElectionTimeout, func(h *quicraft.Host) error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		var err error
		session, err = h.GetNewSession(ctx, 1)
		return err
	})
	if err != nil {
		t.Fatalf("GetNewSession failed: %v\nshard view: %s", err, cluster.shardView(1))
	}
	if session == nil {
		t.Fatal("session is nil")
	}
	if session.ShardID() != 1 {
		t.Errorf("ShardID = %d, want 1", session.ShardID())
	}
	if session.ClientID() == 0 {
		t.Error("ClientID should not be 0")
	}

	err = cluster.onLeader(1, leaderElectionTimeout, func(h *quicraft.Host) error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		rs, err := h.CloseSession(ctx, 1, session)
		if err != nil {
			return err
		}
		rs.Release()
		return nil
	})
	if err != nil {
		t.Fatalf("CloseSession failed: %v\nshard view: %s", err, cluster.shardView(1))
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Snapshot Request
// ---------------------------------------------------------------------------

// TestMultiNode_RequestSnapshot_OnLeader verifies that snapshot requests
// work on the leader in a multi-node cluster.
//
// RequestSnapshot is ASYNCHRONOUS: the returned RequestState resolves only
// when the snapshot has been saved, so a test that releases it without
// reading the result waits for nothing and would pass even if no snapshot
// were ever taken. This test reads the result, requires a non-zero snapshot
// index covering the applied state, requires the durable snapshot.dat
// artifact on disk, and requires a second snapshot to ADVANCE past the first.
//
// Note that automatic snapshots never fire here: Config.SnapshotEntries has
// no default (SetDefaults leaves it 0) and testShardConfig does not set it,
// so every snapshot in this package is the one explicitly requested below.
func TestMultiNode_RequestSnapshot_OnLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	leaderReplicaID := uint64(leaderIdx + 1)
	leaderDir := cluster.nodeHostDirs[leaderIdx]

	// Write some data first, awaiting apply so the snapshot has committed
	// state to capture.
	for i := 0; i < 5; i++ {
		proposeCommitted(t, leaderHost, 1,
			fmt.Sprintf("snap-key-%d", i), fmt.Sprintf("snap-val-%d", i), defaultTimeout)
	}

	firstIndex := awaitSnapshot(t, leaderHost, leaderDir, 1, leaderReplicaID)

	// More writes, then a second snapshot. A snapshot index that never moves
	// is indistinguishable from no snapshot being taken at all.
	for i := 5; i < 10; i++ {
		proposeCommitted(t, leaderHost, 1,
			fmt.Sprintf("snap-key-%d", i), fmt.Sprintf("snap-val-%d", i), defaultTimeout)
	}

	secondIndex := awaitSnapshot(t, leaderHost, leaderDir, 1, leaderReplicaID)
	if secondIndex <= firstIndex {
		t.Fatalf("second snapshot index %d did not advance past the first (%d) after 5 more committed entries",
			secondIndex, firstIndex)
	}

	t.Logf("shard 1 replica %d: snapshot index advanced %d -> %d, both artifacts present on disk under %s",
		leaderReplicaID, firstIndex, secondIndex, leaderDir)
}

// ---------------------------------------------------------------------------
// Multi-Node NodeHostInfo
// ---------------------------------------------------------------------------

// TestMultiNode_GetNodeHostInfo verifies that each node in the cluster
// returns accurate shard information from GetNodeHostInfo.
func TestMultiNode_GetNodeHostInfo(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		if len(info.ShardInfoList) != 1 {
			t.Errorf("node[%d] ShardInfoList length = %d, want 1", i, len(info.ShardInfoList))
		}
		if info.RaftAddress != cluster.addrs[i] {
			t.Errorf("node[%d] RaftAddress = %q, want %q", i, info.RaftAddress, cluster.addrs[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Multiple Shards
// ---------------------------------------------------------------------------

// TestMultiNode_MultipleShards verifies that multiple shards can run
// concurrently across a 3-node cluster, each with independent leader
// election and replication.
func TestMultiNode_MultipleShards(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	numShards := 3
	allTrackers := make([][]*smTracker, numShards)

	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		allTrackers[s] = cluster.startShard(shardID)
	}

	// Wait for each shard to elect a leader.
	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	}

	// Propose to each shard via its leader. proposeToLeader awaits Result() so
	// the proposal is committed before replication is checked.
	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		key := fmt.Sprintf("multi-shard-%d", shardID)
		value := fmt.Sprintf("value-%d", shardID)
		proposeToLeader(t, cluster, shardID, encodeKVPut(key, value), defaultTimeout)
	}

	// Verify replication on all nodes for each shard.
	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		key := fmt.Sprintf("multi-shard-%d", shardID)
		expectedVal := fmt.Sprintf("value-%d", shardID)

		for i := range cluster.hosts {
			replicaID := uint64(i + 1)
			sm := allTrackers[s][i].Get(shardID, replicaID)
			if sm == nil {
				continue
			}
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("shard %d node %d apply", shardID, i),
				func() bool {
					val, ok := sm.Get(key)
					return ok && val == expectedVal
				},
			)
		}
	}
}

// ===========================================================================
// 5-Node Cluster Tests
// ===========================================================================
//
// These tests exercise the Raft protocol with a 5-node cluster to verify
// correctness at the larger quorum size (majority = 3). A 5-node cluster
// tolerates up to 2 node failures while maintaining quorum.
// ===========================================================================

// ---------------------------------------------------------------------------
// 5-Node Leader Election
// ---------------------------------------------------------------------------

// TestFiveNode_LeaderElection verifies that a 5-node cluster elects exactly
// one leader for a shard.
func TestFiveNode_LeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	if leaderIdx < 0 || leaderIdx >= 5 {
		t.Fatalf("invalid leader index: %d", leaderIdx)
	}

	// Wait for exactly one leader. During election transitions, multiple
	// nodes may momentarily report IsLeader before convergence.
	waitForCondition(t, leaderElectionTimeout, "exactly 1 leader in 5-node cluster", func() bool {
		count := 0
		for _, h := range cluster.hosts {
			info, err := h.GetShardInfo(1)
			if err != nil {
				return false
			}
			if info.IsLeader {
				count++
			}
		}
		return count == 1
	})
}

// TestFiveNode_LeaderElection_AllNodesKnowLeader verifies that after
// election, all 5 nodes in the cluster agree on who the leader is.
func TestFiveNode_LeaderElection_AllNodesKnowLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Every node, the leader included, must converge on one leader that
	// names itself leader. Leadership may move after the first leader is
	// seen; that is allowed, a split or unknown view is not.
	cluster.waitForAgreedLeader(1, leaderElectionTimeout)
}

// ---------------------------------------------------------------------------
// 5-Node Proposals and Replication
// ---------------------------------------------------------------------------

// TestFiveNode_ProposeAndReplicate verifies that a proposal submitted to
// the leader is replicated to all 5 nodes.
func TestFiveNode_ProposeAndReplicate(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeToLeader(t, cluster, 1, encodeKVPut("five-node-key", "five-node-value"), defaultTimeout)

	// Verify the entry is applied on all 5 nodes.
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] apply five-node-key", i),
			func() bool {
				val, ok := nodeSM.Get("five-node-key")
				return ok && val == "five-node-value"
			},
		)
	}
}

// TestFiveNode_ProposeAndReplicate_MultipleEntries verifies that 10
// sequential proposals are all replicated in order to all 5 nodes.
func TestFiveNode_ProposeAndReplicate_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	numEntries := 10
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("five-key-%03d", i)
		value := fmt.Sprintf("five-val-%03d", i)
		proposeToLeader(t, cluster, 1, encodeKVPut(key, value), defaultTimeout)
	}

	// Wait for all entries to be applied on all 5 nodes.
	lastKey := fmt.Sprintf("five-key-%03d", numEntries-1)
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] all entries applied", i),
			func() bool {
				_, ok := nodeSM.Get(lastKey)
				return ok
			},
		)

		// Verify all entries are present with correct values.
		for j := 0; j < numEntries; j++ {
			key := fmt.Sprintf("five-key-%03d", j)
			expectedVal := fmt.Sprintf("five-val-%03d", j)
			val, ok := nodeSM.Get(key)
			if !ok {
				t.Errorf("node[%d]: key %q not found", i, key)
				continue
			}
			if val != expectedVal {
				t.Errorf("node[%d]: key %q = %q, want %q", i, key, val, expectedVal)
			}
		}
	}
}

// TestFiveNode_ProposeToFollower verifies that proposing to a follower
// in a 5-node cluster succeeds via automatic leader forwarding.
func TestFiveNode_ProposeToFollower(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Find a follower.
	followerIdx := -1
	for i := range cluster.hosts {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Warmup: write via leader so the follower learns the leader ID
	// through the replication heartbeat.
	// Bound for the StaleRead assertion at the end; the WRITE below goes
	// through the current leader rather than this cached host.
	leaderHost := cluster.hosts[leaderIdx]
	proposeToLeader(t, cluster, 1, encodeKVPut("warmup-5n", "init"), defaultTimeout)

	// Wait until the follower knows who the leader is.
	waitForLeader(t, cluster.hosts[followerIdx], 1, leaderElectionTimeout)

	// Propose via follower — this should succeed via leader forwarding.
	// The FOLLOWER is the subject of this assertion, so this one call must keep
	// its cached host: routing it to the leader would test nothing.
	followerHost := cluster.hosts[followerIdx]
	//quicraft:direct-propose the follower is the subject: this asserts leader forwarding
	rs, err := followerHost.Propose(ctx, 1, encodeKVPut("5n-follower-forwarded", "value5"))
	if err != nil {
		t.Fatalf("Propose via follower failed: %v", err)
	}
	rs.Release()

	// Wait for the forwarded proposal to be applied on the leader, then verify.
	waitForCondition(t, defaultTimeout, "5n-follower-forwarded applied", func() bool {
		result, readErr := leaderHost.StaleRead(ctx, 1, "5n-follower-forwarded")
		if readErr != nil {
			return false
		}
		val, ok := result.(string)
		return ok && val == "value5"
	})
}

// ---------------------------------------------------------------------------
// 5-Node Failover
// ---------------------------------------------------------------------------

// TestFiveNode_LeaderFailover verifies that when the leader is stopped in
// a 5-node cluster, a new leader is elected from the remaining 4 nodes
// and proposals succeed.
//
// As in the 3-node case, it also verifies LEADER COMPLETENESS by re-reading
// the entries committed before the failover through the new leader; a new
// leader that accepts writes but lost pre-failover committed state must fail
// here.
func TestFiveNode_LeaderFailover(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	oldLeaderReplicaID := uint64(leaderIdx + 1)

	oldLeaderInfo, err := cluster.hosts[leaderIdx].GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on the original leader failed: %v", err)
	}
	oldTerm := oldLeaderInfo.Term
	if oldTerm == 0 {
		t.Fatal("original leader reports term 0")
	}

	// Write initial data via the leader, awaiting apply on each. This also
	// ensures all QUIC connections are fully established before stopping the
	// leader.
	preFailover := make(map[string]string, 6)
	preFailover["before-failover-5n"] = "original"
	for i := 0; i < 5; i++ {
		preFailover[fmt.Sprintf("before-failover-5n-%d", i)] =
			fmt.Sprintf("committed-by-replica-%d-term-%d-seq-%d", oldLeaderReplicaID, oldTerm, i)
	}
	for key, value := range preFailover {
		proposeCommitted(t, cluster.hosts[leaderIdx], 1, key, value, defaultTimeout)
	}

	// Wait for the pre-failover entries to be applied on EVERY follower, so
	// the surviving four all carry the committed state into the election.
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		replicaID := uint64(i + 1)
		followerSM := trackers[i].Get(1, replicaID)
		if followerSM == nil {
			t.Fatalf("follower[%d] state machine not found", i)
		}
		for key, value := range preFailover {
			k, v := key, value
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("follower[%d] apply %q before failover", i, k),
				func() bool {
					got, ok := followerSM.Get(k)
					return ok && got == v
				},
			)
		}
	}

	// Stop the leader.
	cluster.hosts[leaderIdx].Close()

	// Wait for a new leader to be elected among the remaining nodes.
	newLeaderIdx := -1
	waitForCondition(t, leaderElectionTimeout*2, "new leader elected", func() bool {
		for i, h := range cluster.hosts {
			if i == leaderIdx {
				continue
			}
			info, getErr := h.GetShardInfo(1)
			if getErr == nil && info.IsLeader {
				newLeaderIdx = i
				return true
			}
		}
		return false
	})

	if newLeaderIdx < 0 {
		t.Fatal("no new leader elected after failover")
	}
	newLeaderReplicaID := uint64(newLeaderIdx + 1)
	if newLeaderReplicaID == oldLeaderReplicaID {
		t.Errorf("new leader %d should differ from old leader %d",
			newLeaderReplicaID, oldLeaderReplicaID)
	}
	newLeaderHost := cluster.hosts[newLeaderIdx]

	newLeaderInfo, err := newLeaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on the new leader failed: %v", err)
	}
	if newLeaderInfo.Term <= oldTerm {
		t.Errorf("new leader term = %d, want > old leader term %d: the failover did not start a new term",
			newLeaderInfo.Term, oldTerm)
	}

	// Propose via the new leader and wait for apply, which pins the new
	// leader's applied index above every pre-failover entry.
	proposeCommitted(t, newLeaderHost, 1, "after-failover-5n", "survived", defaultTimeout*2)

	newLeaderSM := trackers[newLeaderIdx].Get(1, newLeaderReplicaID)
	if newLeaderSM == nil {
		t.Fatalf("new leader[%d] state machine not found", newLeaderIdx)
	}
	assertSMHasExact(t, newLeaderSM, fmt.Sprintf("new leader[%d]", newLeaderIdx),
		"after-failover-5n", "survived")

	// LEADER COMPLETENESS: everything committed in the old term must live on
	// in the new leader's applied state and be readable linearizably through
	// it.
	for key, value := range preFailover {
		assertSMHasExact(t, newLeaderSM, fmt.Sprintf("new leader[%d]", newLeaderIdx), key, value)
	}
	for key, value := range preFailover {
		syncReadExact(t, newLeaderHost, 1, key, value, defaultTimeout)
	}
}

// TestFiveNode_Failover_TwoNodesDown verifies that a 5-node cluster
// maintains quorum (3/5) and can still accept proposals when 2 nodes
// are stopped. This is the key advantage of a 5-node cluster over a
// 3-node cluster: tolerating 2 simultaneous failures.
func TestFiveNode_Failover_TwoNodesDown(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup: write initial data to ensure all QUIC connections are
	// fully established before stopping nodes.
	proposeToLeader(t, cluster, 1, encodeKVPut("warmup-2down", "warmup-value"), defaultTimeout)

	// Wait for warmup replication to all nodes.
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] warmup applied", i),
			func() bool {
				_, ok := nodeSM.Get("warmup-2down")
				return ok
			},
		)
	}

	// Stop 2 non-leader nodes to bring the cluster to 3 live nodes. The leader
	// is resolved here rather than reused from the election wait above: the
	// warmup write may have spanned a re-election, and closing the node that
	// is actually leading is not what this test set out to do.
	leaderIdx := cluster.findLeader(1)
	if leaderIdx < 0 {
		t.Fatal("no leader for shard 1 before stopping followers")
	}
	stoppedNodes := make(map[int]bool)
	stoppedCount := 0
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		if stoppedCount >= 2 {
			break
		}
		cluster.hosts[i].Close()
		stoppedNodes[i] = true
		stoppedCount++
	}

	// The shard should still be able to accept proposals with 3/5 nodes alive
	// (quorum = 3). The property under test is that a majority of 3 suffices,
	// not that the SAME replica keeps leading, so the write goes to whoever
	// leads at this moment: closing two peers can itself trigger an election.
	proposeToLeader(t, cluster, 1, encodeKVPut("after-2down", "still-alive"), defaultTimeout)

	// Verify the entry is applied on all remaining live nodes.
	for i := range cluster.hosts {
		if stoppedNodes[i] {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("live node[%d] apply after-2down", i),
			func() bool {
				val, ok := nodeSM.Get("after-2down")
				return ok && val == "still-alive"
			},
		)
	}
}

// ---------------------------------------------------------------------------
// 5-Node Reads
// ---------------------------------------------------------------------------

// TestFiveNode_StaleRead_FromFollower verifies that stale reads from a
// follower in a 5-node cluster return data replicated from the leader.
func TestFiveNode_StaleRead_FromFollower(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	// Write via whoever leads now; leaderIdx below only picks a follower.
	proposeToLeader(t, cluster, 1, encodeKVPut("five-follower-read", "five-follower-value"), defaultTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Find a follower and wait for replication.
	followerIdx := -1
	for i := range cluster.hosts {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}

	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	waitForCondition(t, defaultTimeout, "follower replication", func() bool {
		_, ok := followerSM.Get("five-follower-read")
		return ok
	})

	// Stale read from follower.
	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "five-follower-read")
	if err != nil {
		t.Fatalf("StaleRead from follower failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("StaleRead returned unexpected type: %T", result)
	}
	if val != "five-follower-value" {
		t.Errorf("StaleRead returned %q, want %q", val, "five-follower-value")
	}
}

// ---------------------------------------------------------------------------
// 5-Node Session Management
// ---------------------------------------------------------------------------

// TestFiveNode_Session_OnLeader verifies that session creation and close
// work on the leader node in a 5-node cluster.
func TestFiveNode_Session_OnLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Both calls go to whoever leads at the moment of the call: leadership
	// can legitimately move after waitForClusterLeader returns (#309).
	var session *quicraft.Session
	err := cluster.onLeader(1, leaderElectionTimeout, func(h *quicraft.Host) error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		var err error
		session, err = h.GetNewSession(ctx, 1)
		return err
	})
	if err != nil {
		t.Fatalf("GetNewSession failed: %v\nshard view: %s", err, cluster.shardView(1))
	}
	if session == nil {
		t.Fatal("session is nil")
	}
	if session.ShardID() != 1 {
		t.Errorf("ShardID = %d, want 1", session.ShardID())
	}
	if session.ClientID() == 0 {
		t.Error("ClientID should not be 0")
	}

	err = cluster.onLeader(1, leaderElectionTimeout, func(h *quicraft.Host) error {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		rs, err := h.CloseSession(ctx, 1, session)
		if err != nil {
			return err
		}
		rs.Release()
		return nil
	})
	if err != nil {
		t.Fatalf("CloseSession failed: %v\nshard view: %s", err, cluster.shardView(1))
	}
}

// ---------------------------------------------------------------------------
// 5-Node Snapshot Request
// ---------------------------------------------------------------------------

// TestFiveNode_RequestSnapshot_OnLeader verifies that snapshot requests
// work on the leader in a 5-node cluster. Like the 3-node case it awaits the
// asynchronous snapshot result, requires a non-zero advancing snapshot index,
// and requires the durable snapshot.dat artifact on disk.
func TestFiveNode_RequestSnapshot_OnLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	leaderReplicaID := uint64(leaderIdx + 1)
	leaderDir := cluster.nodeHostDirs[leaderIdx]

	// Write some data first, awaiting apply on each.
	for i := 0; i < 5; i++ {
		proposeCommitted(t, leaderHost, 1,
			fmt.Sprintf("five-snap-key-%d", i), fmt.Sprintf("five-snap-val-%d", i), defaultTimeout)
	}

	firstIndex := awaitSnapshot(t, leaderHost, leaderDir, 1, leaderReplicaID)

	for i := 5; i < 10; i++ {
		proposeCommitted(t, leaderHost, 1,
			fmt.Sprintf("five-snap-key-%d", i), fmt.Sprintf("five-snap-val-%d", i), defaultTimeout)
	}

	secondIndex := awaitSnapshot(t, leaderHost, leaderDir, 1, leaderReplicaID)
	if secondIndex <= firstIndex {
		t.Fatalf("second snapshot index %d did not advance past the first (%d) after 5 more committed entries",
			secondIndex, firstIndex)
	}

	t.Logf("shard 1 replica %d: snapshot index advanced %d -> %d, both artifacts present on disk under %s",
		leaderReplicaID, firstIndex, secondIndex, leaderDir)
}

// ---------------------------------------------------------------------------
// 5-Node NodeHostInfo
// ---------------------------------------------------------------------------

// TestFiveNode_GetNodeHostInfo verifies that each node in a 5-node cluster
// returns accurate shard information from GetNodeHostInfo.
func TestFiveNode_GetNodeHostInfo(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		if len(info.ShardInfoList) != 1 {
			t.Errorf("node[%d] ShardInfoList length = %d, want 1",
				i, len(info.ShardInfoList))
		}
		if info.RaftAddress != cluster.addrs[i] {
			t.Errorf("node[%d] RaftAddress = %q, want %q",
				i, info.RaftAddress, cluster.addrs[i])
		}
	}
}

// ---------------------------------------------------------------------------
// 5-Node Multiple Shards
// ---------------------------------------------------------------------------

// TestFiveNode_MultipleShards verifies that 3 shards can run concurrently
// across a 5-node cluster, each with independent leader election and
// replication.
func TestFiveNode_MultipleShards(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	numShards := 3
	allTrackers := make([][]*smTracker, numShards)

	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		allTrackers[s] = cluster.startShard(shardID)
	}

	// Wait for each shard to elect a leader.
	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	}

	// Propose to each shard via its leader. proposeToLeader awaits Result() so
	// the proposal is committed before replication is checked.
	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		key := fmt.Sprintf("five-multi-shard-%d", shardID)
		value := fmt.Sprintf("five-value-%d", shardID)
		proposeToLeader(t, cluster, shardID, encodeKVPut(key, value), defaultTimeout)
	}

	// Verify replication on all 5 nodes for each shard. Use extended
	// timeout for 5-node x 3-shard replication under Docker CI contention.
	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		key := fmt.Sprintf("five-multi-shard-%d", shardID)
		expectedVal := fmt.Sprintf("five-value-%d", shardID)

		for i := range cluster.hosts {
			replicaID := uint64(i + 1)
			nodeSM := allTrackers[s][i].Get(shardID, replicaID)
			if nodeSM == nil {
				continue
			}
			waitForCondition(t, defaultTimeout*3,
				fmt.Sprintf("shard %d node %d apply", shardID, i),
				func() bool {
					val, ok := nodeSM.Get(key)
					return ok && val == expectedVal
				},
			)
		}
	}
}
