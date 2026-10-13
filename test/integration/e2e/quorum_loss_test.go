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

// assertKeyAbsentEverywhere fails if key is present on the state machine of any
// live node, after a settling window long enough for a commit to have
// propagated had one occurred.
//
// This is the assertion that actually says "the write did not happen". An error
// from Propose says only that the CALLER gave up waiting; an entry can still be
// in the leader's log, and if it were ever committed and applied without a
// majority that is the safety violation, whatever the caller was told.
func assertKeyAbsentEverywhere(
	t *testing.T,
	cluster *testCluster,
	trackers []*smTracker,
	shardID uint64,
	key string,
	settle time.Duration,
) {
	t.Helper()

	settleTimer := time.NewTimer(settle)
	<-settleTimer.C
	settleTimer.Stop()

	checked := 0
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		nodeSM := resolveStateMachine(cluster, trackers, shardID, i)
		if nodeSM == nil {
			t.Fatalf("node[%d] is live but has no state machine; the absence of %q there proves nothing", i, key)
		}
		if got, ok := nodeSM.Get(key); ok {
			t.Fatalf("node[%d] applied %q=%q without a majority: the write was committed while quorum was lost",
				i, key, got)
		}
		checked++
	}
	if checked == 0 {
		t.Fatalf("assertKeyAbsentEverywhere(%q) checked no nodes", key)
	}
	t.Logf("%q is absent from all %d live state machines", key, checked)
}

// TestE2E_QuorumLoss_RejectsWrites verifies that a 5-node cluster cannot commit
// a proposal once quorum is lost. In a 5-node cluster quorum requires 3 nodes;
// three followers are stopped, leaving the LEADER plus one follower.
//
// The leader is deliberately left RUNNING. The previous version stopped the
// leader before proposing anything, so every remaining node answered Propose
// with ErrNotLeader — an outcome produced by the absence of a leader, not by
// the quorum rule. That test passed identically on a build with no quorum
// requirement at all. Keeping the old leader up puts the quorum rule itself on
// the only path to a commit, and the decisive assertion is that the value never
// reaches ANY state machine rather than that the caller received an error.
func TestE2E_QuorumLoss_RejectsWrites(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warm up: ensure all QUIC connections are established.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Positive control: the SAME leader commits without difficulty while it
	// still has a majority. Without this, a build that cannot commit anything
	// would pass the quorum-loss assertion for the wrong reason.
	proposeAndWait(t, cluster, trackers, 1, "pre-quorum-loss", "intact")
	waitForReplication(t, cluster, trackers, 1, "pre-quorum-loss", "intact", defaultTimeout)

	t.Logf("initial leader: node[%d], pre-quorum-loss data replicated", leaderIdx)

	// Stop 3 followers, leaving the leader and one follower: 2 of 5, a
	// minority. The leader stays up so it is the quorum rule, not the absence
	// of a leader, that has to stop the write.
	stoppedCount := 0
	for i := range cluster.hosts {
		if i == leaderIdx || stoppedCount >= 3 {
			continue
		}
		t.Logf("stopping follower node[%d]", i)
		cluster.stopNode(i)
		stoppedCount++
	}
	if stoppedCount != 3 {
		t.Fatalf("expected to stop 3 followers, stopped %d", stoppedCount)
	}
	if cluster.liveNodes() != 2 {
		t.Fatalf("expected 2 live nodes after quorum loss, got %d", cluster.liveNodes())
	}
	if cluster.stopped[leaderIdx].Load() {
		t.Fatalf("node[%d] was the leader and must still be running for this test", leaderIdx)
	}

	// Propose through the still-running old leader. It may accept the proposal
	// into its log — that is legal — but it must never report a commit.
	const lostKey = "should-fail"
	const lostVal = "never-committed"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//quicraft:direct-propose the still-running old leader IS the subject: 2 of 5 must not commit
	rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, encodeKVPut(lostKey, lostVal))
	if err == nil {
		_, resErr := rs.Result()
		rs.Release()
		if resErr == nil {
			cancel()
			t.Fatalf("node[%d] reported %q as committed with only %d of 5 nodes alive: "+
				"a majority was not required", leaderIdx, lostKey, cluster.liveNodes())
		}
		t.Logf("node[%d]: proposal accepted but never committed: %v", leaderIdx, resErr)
	} else {
		t.Logf("node[%d]: Propose rejected outright: %v", leaderIdx, err)
	}
	cancel()

	// The decisive assertion.
	assertKeyAbsentEverywhere(t, cluster, trackers, 1, lostKey, 2*time.Second)

	// The leader must also step down once CheckQuorum notices the lost peers,
	// and no leader may be elected from the 2-node remainder thereafter.
	waitForCondition(t, failoverTimeout,
		"leader steps down after losing quorum",
		func() bool { return cluster.findLeader(1) < 0 },
	)
	t.Logf("old leader node[%d] stepped down after losing quorum", leaderIdx)

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

	// With no leader, every surviving node must also refuse a fresh proposal.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		// Use a short timeout so we don't block the test for long.
		// The proposal should fail quickly with ErrTimeout, ErrClosed,
		// or context.DeadlineExceeded.
		propCtx, propCancel := context.WithTimeout(context.Background(), 2*time.Second)
		cmd := encodeKVPut("should-fail-leaderless", "never-committed")
		//quicraft:direct-propose every surviving node in turn must refuse; there is no leader to find
		_, propErr := cluster.hosts[i].Propose(propCtx, 1, cmd)
		propCancel()

		if propErr == nil {
			t.Fatalf("node[%d]: Propose succeeded despite quorum loss; "+
				"writes should be rejected", i)
		}

		t.Logf("node[%d]: Propose correctly failed: %v", i, propErr)
	}
	assertKeyAbsentEverywhere(t, cluster, trackers, 1, "should-fail-leaderless", time.Second)

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

	// Verify writes are rejected during quorum loss. An error from Propose is
	// necessary but not sufficient — it only says the caller gave up — so the
	// value's absence from every live state machine is asserted as well.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	cmd := encodeKVPut("during-loss", "should-fail")
	//quicraft:direct-propose the deposed leader IS the subject: it must not commit after quorum loss
	rs, err := cluster.hosts[activeLeaderIdx].Propose(ctx, 1, cmd)
	if err == nil {
		_, resErr := rs.Result()
		rs.Release()
		if resErr == nil {
			cancel()
			t.Fatalf("Propose reported %q as committed during quorum loss (%d of 5 alive)",
				"during-loss", cluster.liveNodes())
		}
		t.Logf("proposal accepted but never committed during quorum loss: %v", resErr)
	} else {
		t.Logf("proposal correctly rejected during quorum loss: %v", err)
	}
	cancel()
	assertKeyAbsentEverywhere(t, cluster, trackers, 1, "during-loss", time.Second)

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
	// The returning node presents the LEAF it had before: the shard's
	// committed identity for this replica is that keypair, and a host with a
	// fresh one would be refused by every peer as an identity mismatch.
	// The node lock serializes this restart with the cluster's stop and
	// close paths, and the host is replaced under hostsMu, as the harness
	// helpers do (#309). The closure releases the node lock on t.Fatalf too.
	recoveredReplicaID := uint64(thirdStop + 1)
	var addr string
	func() {
		lk := cluster.nodeLock(thirdStop)
		lk.Lock()
		defer lk.Unlock()
		if !cluster.stopped[thirdStop].Load() {
			t.Fatalf("node[%d] must be stopped before recovery", thirdStop)
		}
		addr = freePort(t)
		hostCfg := testHostConfig(t, addr, cluster.leaves[thirdStop])
		newHost, hostErr := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
		if hostErr != nil {
			t.Fatalf("NewHost for recovery node failed: %v", hostErr)
		}

		// Build the original members map with the new address for the
		// recovered replica.
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
		scfg.MemberIdentities = cluster.identities()
		if startErr := newHost.StartShard(members, false, createFn, scfg); startErr != nil {
			newHost.Close()
			t.Fatalf("StartShard for recovery node failed: %v", startErr)
		}

		// Replace the stopped node in the cluster tracking.
		cluster.hostsMu.Lock()
		cluster.hosts[thirdStop] = newHost
		cluster.addrs[thirdStop] = addr
		cluster.trackers[thirdStop] = newTracker
		cluster.stopped[thirdStop].Store(false)
		cluster.hostsMu.Unlock()
	}()

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

	// Verify pre-quorum-loss data is intact on all live nodes. verifyAllData
	// counts the nodes it checked and fails on a shortfall; the hand-rolled
	// loop this replaces skipped any node whose tracker was nil, which is
	// exactly the recovered node whose recovery is the point of the test.
	verifyAllData(t, cluster, trackers, 1, expected)

	t.Logf("quorum recovery complete: all %d live nodes have %d entries",
		cluster.liveNodes(), len(expected))
}
