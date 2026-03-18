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
	"net"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg"
)

// ---------------------------------------------------------------------------
// Multi-Node Cluster Types
// ---------------------------------------------------------------------------

// testCluster manages a group of Hosts for multi-node integration tests.
// Each Host runs on a separate listen address with real QUIC transport.
type testCluster struct {
	hosts    []*quicraft.Host
	addrs    []string
	trackers []*smTracker
	t        *testing.T
}

// newTestCluster creates a 3-node cluster with real QUIC transport.
// Each host gets a unique listen address on the loopback interface.
// The cluster is not started (no shards); call startShard to begin.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	addrs := make([]string, numNodes)
	for i := range numNodes {
		// Bind a temporary UDP listener to let the OS assign a free port,
		// then close it so NewHost can bind to the same address. This
		// avoids hardcoded ports that cause "address already in use"
		// errors when tests run sequentially.
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to find free port for node %d: %v", i, err)
		}
		addrs[i] = pc.LocalAddr().String()
		pc.Close()
	}

	mtls := testMTLSConfig(t)
	hosts := make([]*quicraft.Host, numNodes)
	for i := range numNodes {
		cfg := testHostConfig(t, addrs[i], mtls)
		h, err := quicraft.NewHost(cfg)
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
		hosts: hosts,
		addrs: addrs,
		t:     t,
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

	trackers := make([]*smTracker, len(c.hosts))
	for i, h := range c.hosts {
		replicaID := uint64(i + 1)
		createFn, tracker := kvCreateFunc()
		trackers[i] = tracker

		scfg := testShardConfig(shardID, replicaID)
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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// The leader node should know its own ID.
	leaderHost := cluster.hosts[leaderIdx]
	leaderInfo, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on leader failed: %v", err)
	}
	expectedLeaderID := leaderInfo.ReplicaID

	// Wait for all followers to learn the leader.
	for i, h := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node %d learns leader", i),
			func() bool {
				lid, known, getErr := h.GetLeaderID(1)
				return getErr == nil && known && lid == expectedLeaderID
			},
		)
	}
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
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose via leader.
	cmd := encodeKVPut("replicated-key", "replicated-value")
	rs, err := leaderHost.Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose to leader failed: %v", err)
	}
	rs.Release()

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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	numEntries := 10
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		cmd := encodeKVPut(key, value)

		rs, propErr := leaderHost.Propose(ctx, 1, cmd)
		if propErr != nil {
			t.Fatalf("Propose[%d] failed: %v", i, propErr)
		}
		rs.Release()
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
	leaderHost := cluster.hosts[leaderIdx]
	warmupRS, warmupErr := leaderHost.Propose(ctx, 1, encodeKVPut("warmup", "init"))
	if warmupErr != nil {
		t.Fatalf("warmup Propose failed: %v", warmupErr)
	}
	warmupRS.Release()

	// Wait until the follower knows who the leader is.
	waitForLeader(t, cluster.hosts[followerIdx], 1, leaderElectionTimeout)

	// Propose via follower — this should succeed via leader forwarding.
	followerHost := cluster.hosts[followerIdx]
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
func TestMultiNode_LeaderFailover(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	oldLeaderReplicaID := uint64(leaderIdx + 1)

	// Write initial data via the first leader.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("before-failover", "original")
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	rs.Release()

	// Wait for the proposal to be applied on ALL followers. In a 3-node
	// cluster, both remaining followers must have established QUIC
	// connections (via replication from the leader) before we stop the
	// leader, otherwise re-election between the two survivors can stall.
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(1, replicaID)
		if sm != nil {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("follower[%d] apply before failover", i),
				func() bool {
					_, ok := sm.Get("before-failover")
					return ok
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

	// Propose via the new leader.
	cmd2 := encodeKVPut("after-failover", "survived")
	rs2, err := cluster.hosts[newLeaderIdx].Propose(ctx, 1, cmd2)
	if err != nil {
		t.Fatalf("Propose to new leader failed: %v", err)
	}
	rs2.Release()

	// Verify the new entry is applied on the new leader.
	newLeaderSM := trackers[newLeaderIdx].Get(1, newLeaderReplicaID)
	if newLeaderSM != nil {
		waitForCondition(t, defaultTimeout, "new leader SM apply after failover", func() bool {
			val, ok := newLeaderSM.Get("after-failover")
			return ok && val == "survived"
		})
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
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write via leader.
	cmd := encodeKVPut("follower-read-key", "follower-read-value")
	rs, err := leaderHost.Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	rs.Release()

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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session, err := leaderHost.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession failed: %v", err)
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

	rs, err := leaderHost.CloseSession(ctx, 1, session)
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
	rs.Release()
}

// ---------------------------------------------------------------------------
// Multi-Node Snapshot Request
// ---------------------------------------------------------------------------

// TestMultiNode_RequestSnapshot_OnLeader verifies that snapshot requests
// work on the leader in a multi-node cluster.
func TestMultiNode_RequestSnapshot_OnLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write some data first.
	for i := 0; i < 5; i++ {
		cmd := encodeKVPut(fmt.Sprintf("snap-key-%d", i), fmt.Sprintf("snap-val-%d", i))
		rs, err := leaderHost.Propose(ctx, 1, cmd)
		if err != nil {
			t.Fatalf("Propose[%d] failed: %v", i, err)
		}
		rs.Release()
	}

	rs, err := leaderHost.RequestSnapshot(ctx, 1, quicraft.SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil")
	}
	rs.Release()
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

	// Propose to each shard via its leader. Await Result() to ensure
	// the proposal is committed before checking replication.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			t.Fatalf("no leader for shard %d", shardID)
		}

		key := fmt.Sprintf("multi-shard-%d", shardID)
		value := fmt.Sprintf("value-%d", shardID)
		cmd := encodeKVPut(key, value)

		rs, err := cluster.hosts[leaderIdx].Propose(ctx, shardID, cmd)
		if err != nil {
			t.Fatalf("Propose to shard %d leader failed: %v", shardID, err)
		}
		_, err = rs.Result()
		if err != nil {
			t.Fatalf("Result for shard %d failed: %v", shardID, err)
		}
		rs.Release()
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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// The leader node should know its own ID.
	leaderHost := cluster.hosts[leaderIdx]
	leaderInfo, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on leader failed: %v", err)
	}
	expectedLeaderID := leaderInfo.ReplicaID

	// Wait for all 4 followers to learn the leader.
	for i, h := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node %d learns leader", i),
			func() bool {
				lid, known, getErr := h.GetLeaderID(1)
				return getErr == nil && known && lid == expectedLeaderID
			},
		)
	}
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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("five-node-key", "five-node-value")
	rs, err := leaderHost.Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose to leader failed: %v", err)
	}
	rs.Release()

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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	numEntries := 10
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("five-key-%03d", i)
		value := fmt.Sprintf("five-val-%03d", i)
		cmd := encodeKVPut(key, value)

		rs, propErr := leaderHost.Propose(ctx, 1, cmd)
		if propErr != nil {
			t.Fatalf("Propose[%d] failed: %v", i, propErr)
		}
		rs.Release()
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
	leaderHost := cluster.hosts[leaderIdx]
	warmupRS, warmupErr := leaderHost.Propose(ctx, 1, encodeKVPut("warmup-5n", "init"))
	if warmupErr != nil {
		t.Fatalf("warmup Propose failed: %v", warmupErr)
	}
	warmupRS.Release()

	// Wait until the follower knows who the leader is.
	waitForLeader(t, cluster.hosts[followerIdx], 1, leaderElectionTimeout)

	// Propose via follower — this should succeed via leader forwarding.
	followerHost := cluster.hosts[followerIdx]
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
func TestFiveNode_LeaderFailover(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	oldLeaderReplicaID := uint64(leaderIdx + 1)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Warmup: write initial data via the leader to ensure all QUIC
	// connections are fully established before stopping the leader.
	cmd := encodeKVPut("before-failover-5n", "original")
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	rs.Release()

	// Wait for the warmup proposal to be applied on at least one follower.
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		replicaID := uint64(i + 1)
		followerSM := trackers[i].Get(1, replicaID)
		if followerSM != nil {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("follower[%d] apply before failover", i),
				func() bool {
					_, ok := followerSM.Get("before-failover-5n")
					return ok
				},
			)
			break
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

	// Propose via the new leader.
	cmd2 := encodeKVPut("after-failover-5n", "survived")
	rs2, err := cluster.hosts[newLeaderIdx].Propose(ctx, 1, cmd2)
	if err != nil {
		t.Fatalf("Propose to new leader failed: %v", err)
	}
	rs2.Release()

	// Verify the new entry is applied on the new leader.
	newLeaderSM := trackers[newLeaderIdx].Get(1, newLeaderReplicaID)
	if newLeaderSM != nil {
		waitForCondition(t, defaultTimeout, "new leader SM apply after failover", func() bool {
			val, ok := newLeaderSM.Get("after-failover-5n")
			return ok && val == "survived"
		})
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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Warmup: write initial data to ensure all QUIC connections are
	// fully established before stopping nodes.
	cmd := encodeKVPut("warmup-2down", "warmup-value")
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("warmup Propose failed: %v", err)
	}
	rs.Release()

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

	// Stop 2 non-leader nodes to bring the cluster to 3 live nodes.
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

	// The leader should still be able to accept proposals with 3/5
	// nodes alive (quorum = 3).
	cmd2 := encodeKVPut("after-2down", "still-alive")
	rs2, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd2)
	if err != nil {
		t.Fatalf("Propose with 2 nodes down failed: %v", err)
	}
	rs2.Release()

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
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write via leader.
	cmd := encodeKVPut("five-follower-read", "five-follower-value")
	rs, err := leaderHost.Propose(ctx, 1, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	rs.Release()

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
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session, err := leaderHost.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession failed: %v", err)
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

	rs, err := leaderHost.CloseSession(ctx, 1, session)
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
	rs.Release()
}

// ---------------------------------------------------------------------------
// 5-Node Snapshot Request
// ---------------------------------------------------------------------------

// TestFiveNode_RequestSnapshot_OnLeader verifies that snapshot requests
// work on the leader in a 5-node cluster.
func TestFiveNode_RequestSnapshot_OnLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write some data first.
	for i := 0; i < 5; i++ {
		cmd := encodeKVPut(
			fmt.Sprintf("five-snap-key-%d", i),
			fmt.Sprintf("five-snap-val-%d", i),
		)
		rs, err := leaderHost.Propose(ctx, 1, cmd)
		if err != nil {
			t.Fatalf("Propose[%d] failed: %v", i, err)
		}
		rs.Release()
	}

	rs, err := leaderHost.RequestSnapshot(ctx, 1, quicraft.SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil")
	}
	rs.Release()
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

	// Propose to each shard via its leader. Await Result() to ensure
	// the proposal is committed before checking replication.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	for s := 0; s < numShards; s++ {
		shardID := uint64(s + 1)
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			t.Fatalf("no leader for shard %d", shardID)
		}

		key := fmt.Sprintf("five-multi-shard-%d", shardID)
		value := fmt.Sprintf("five-value-%d", shardID)
		cmd := encodeKVPut(key, value)

		rs, err := cluster.hosts[leaderIdx].Propose(ctx, shardID, cmd)
		if err != nil {
			t.Fatalf("Propose to shard %d leader failed: %v", shardID, err)
		}
		_, err = rs.Result()
		if err != nil {
			t.Fatalf("Result for shard %d failed: %v", shardID, err)
		}
		rs.Release()
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
