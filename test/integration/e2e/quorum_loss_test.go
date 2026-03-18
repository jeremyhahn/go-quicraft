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

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestE2E_QuorumLoss_RejectsWrites verifies that a 5-node cluster
// rejects proposals when quorum is lost. In a 5-node cluster, quorum
// requires 3 nodes. Stopping 3 nodes leaves only 2 alive, which is
// insufficient for consensus. Proposals must fail with a timeout or
// error rather than blocking indefinitely.
func TestE2E_QuorumLoss_RejectsWrites(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Write data before quorum loss for later recovery verification.
	proposeAndWait(t, cluster, trackers, 1, "pre-quorum-loss", "intact")
	waitForReplication(t, cluster, trackers, 1, "pre-quorum-loss", "intact", defaultTimeout)

	t.Logf("initial leader: node[%d], pre-quorum-loss data replicated", leaderIdx)

	// Stop 3 nodes to lose quorum (need 3 of 5 for quorum, leaving only 2).
	// Always stop the leader last to maximize the chance that followers
	// detect the stop quickly.
	stoppedCount := 0
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		if stoppedCount < 2 {
			t.Logf("stopping follower node[%d]", i)
			cluster.stopNode(i)
			stoppedCount++
		}
	}
	// Stop the leader to ensure quorum is fully lost.
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes after quorum loss, got %d", cluster.liveNodes())
	}

	// Verify no leader can be elected in the 2-node remainder.
	// Wait several election cycles to confirm.
	noLeaderDuration := 3 * time.Second
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	noLeaderTimer := time.NewTimer(noLeaderDuration)
	defer noLeaderTimer.Stop()
noLeaderLoop:
	for {
		idx := cluster.findLeader(1)
		if idx >= 0 {
			t.Fatalf("leader elected at node[%d] despite quorum loss; "+
				"only %d of 5 nodes alive", idx, cluster.liveNodes())
		}
		select {
		case <-noLeaderTimer.C:
			break noLeaderLoop
		case <-ticker.C:
		}
	}

	t.Logf("confirmed: no leader elected with only %d nodes alive", cluster.liveNodes())

	// Attempt proposals on the surviving nodes. Each should fail because
	// there is no leader and quorum cannot be achieved.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		// Use a short timeout so we don't block the test for long.
		// The proposal should fail quickly with ErrTimeout, ErrClosed,
		// or context.DeadlineExceeded.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cmd := encodeKVPut("should-fail", "never-committed")
		_, err := cluster.hosts[i].Propose(ctx, 1, cmd)
		cancel()

		if err == nil {
			t.Fatalf("node[%d]: Propose succeeded despite quorum loss; "+
				"writes should be rejected", i)
		}

		t.Logf("node[%d]: Propose correctly failed: %v", i, err)
	}

	t.Logf("quorum loss correctly prevents all writes with %d of 5 nodes alive",
		cluster.liveNodes())
}

// TestE2E_QuorumLoss_Recovery verifies the full quorum loss and recovery
// lifecycle:
//  1. Start 5-node cluster, write initial data
//  2. Stop 3 nodes (quorum lost)
//  3. Verify writes are rejected
//  4. Add replacement nodes to restore quorum via membership changes
//  5. Verify the restored cluster elects a leader and accepts writes
//  6. Verify pre-quorum-loss data is intact on all live nodes
func TestE2E_QuorumLoss_Recovery(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up and write pre-quorum-loss data.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	expected := make(map[string]string)
	expected["warmup"] = "ok"

	for i := range 5 {
		key := fmt.Sprintf("pre-loss-%03d", i)
		value := fmt.Sprintf("pre-val-%03d", i)
		proposeAndWait(t, cluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Wait for full replication before quorum loss.
	for key, val := range expected {
		waitForReplication(t, cluster, trackers, 1, key, val, defaultTimeout)
	}

	t.Logf("pre-quorum-loss data (%d entries) replicated to all 5 nodes, leader is node[%d]",
		len(expected), leaderIdx)

	// Identify which nodes to stop: 2 followers only.
	// Keep the leader alive so we can perform membership changes
	// through it once quorum is restored.
	stoppedNodes := make([]int, 0, 2)
	for i := range cluster.hosts {
		if i == leaderIdx || len(stoppedNodes) >= 2 {
			continue
		}
		stoppedNodes = append(stoppedNodes, i)
	}

	// Stop the 2 followers.
	for _, idx := range stoppedNodes {
		t.Logf("stopping follower node[%d]", idx)
		cluster.stopNode(idx)
	}

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes, got %d", cluster.liveNodes())
	}

	// At this point we have exactly quorum (3 of 5). The cluster should
	// still function. Let's verify the leader is still active.
	activeLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("leader during partial loss: node[%d]", activeLeaderIdx)

	// Now stop one more follower to actually lose quorum (2 of 5 alive).
	var thirdStop int
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i == activeLeaderIdx {
			continue
		}
		thirdStop = i
		break
	}

	t.Logf("stopping node[%d] to lose quorum", thirdStop)
	cluster.stopNode(thirdStop)
	stoppedNodes = append(stoppedNodes, thirdStop)

	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes after quorum loss, got %d", cluster.liveNodes())
	}

	// Wait for the leader to detect quorum loss and step down.
	// The leader checks quorum on every heartbeat interval (~1 tick),
	// so it needs a few ticks to notice the lost peers.
	waitForCondition(t, failoverTimeout,
		"leader steps down after quorum loss",
		func() bool {
			return cluster.findLeader(1) < 0
		},
	)
	t.Logf("leader stepped down after quorum loss")

	// Verify writes are rejected during quorum loss.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	cmd := encodeKVPut("during-loss", "should-fail")
	_, err := cluster.hosts[activeLeaderIdx].Propose(ctx, 1, cmd)
	cancel()

	if err == nil {
		t.Fatal("Propose succeeded during quorum loss; expected failure")
	}
	t.Logf("proposal correctly rejected during quorum loss: %v", err)

	// Restore quorum: remove stopped nodes from membership and add
	// replacement nodes. We need to restore at least 1 node to get
	// back to quorum (3 of N). Since we can't modify membership
	// without quorum, we use a different approach: add a new node
	// to one of the surviving hosts by restarting the stopped follower
	// as a fresh node.
	//
	// However, since we need quorum to perform membership changes and
	// we currently don't have it, we simulate recovery by restarting
	// the third stopped node. Since e2e test nodes use ephemeral dirs,
	// we add a replacement node instead.
	//
	// Strategy: We cannot add nodes without quorum, so we need to
	// restore at least one stopped node. Since the e2e helpers don't
	// support WAL-based restart, we instead create a brand new host
	// and start the shard with the original initial members on it.
	// This simulates a node that was temporarily partitioned but
	// comes back with fresh state.
	//
	// The practical approach: create a new host, add it to the cluster
	// addresses as the "returned" third node. Since it starts fresh,
	// it will bootstrap and connect to the existing members, helping
	// restore quorum.

	// Actually, the simplest correct recovery path is to create a new
	// host that starts the same shard with the same initial members.
	// This mimics the stopped node coming back online after losing
	// its partition.
	addr := freePort(t)
	hostCfg := testHostConfig(t, addr, cluster.mtls)
	newHost, hostErr := quicraft.NewHost(hostCfg)
	if hostErr != nil {
		t.Fatalf("NewHost for recovery node failed: %v", hostErr)
	}

	// Build the original members map with the new address for the
	// recovered replica.
	recoveredReplicaID := uint64(thirdStop + 1)
	members := make(map[uint64]string, len(cluster.addrs))
	for i, a := range cluster.addrs[:5] {
		rid := uint64(i + 1)
		if i == thirdStop {
			// Use the new address for the recovered node.
			members[rid] = addr
		} else {
			members[rid] = a
		}
	}

	createFn, newTracker := kvCreateFunc()
	scfg := testShardConfig(1, recoveredReplicaID)
	if startErr := newHost.StartShard(members, false, createFn, scfg); startErr != nil {
		newHost.Close()
		t.Fatalf("StartShard for recovery node failed: %v", startErr)
	}

	// Replace the stopped node in the cluster tracking.
	cluster.hosts[thirdStop] = newHost
	cluster.addrs[thirdStop] = addr
	cluster.trackers[thirdStop] = newTracker
	cluster.stopped[thirdStop].Store(false)

	if cluster.liveNodes() != 3 {
		t.Fatalf("expected 3 live nodes after recovery, got %d", cluster.liveNodes())
	}

	t.Logf("recovered node[%d] (replica=%d) at %s, quorum restored",
		thirdStop, recoveredReplicaID, addr)

	// Wait for the recovered cluster to elect a leader.
	recoveredLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	t.Logf("leader after quorum recovery: node[%d]", recoveredLeaderIdx)

	// Verify writes succeed after quorum recovery.
	proposeAndWait(t, cluster, trackers, 1, "post-recovery", "restored")
	expected["post-recovery"] = "restored"

	// Verify pre-quorum-loss data is intact on all live nodes.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		replicaID := uint64(i + 1)
		var tracker *smTracker
		if i < len(trackers) && trackers[i] != nil {
			tracker = trackers[i]
		} else if i < len(cluster.trackers) {
			tracker = cluster.trackers[i]
		}
		if tracker == nil {
			continue
		}

		nodeSM := tracker.Get(1, replicaID)
		if nodeSM == nil {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] SM registered", i),
				func() bool {
					nodeSM = tracker.Get(1, replicaID)
					return nodeSM != nil
				},
			)
		}

		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q after recovery", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}

	t.Logf("quorum recovery complete: all %d live nodes have %d entries",
		cluster.liveNodes(), len(expected))
}
