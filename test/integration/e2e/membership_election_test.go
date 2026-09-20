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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ---------------------------------------------------------------------------
// TestE2E_MembershipChange_DuringLeaderElection
// ---------------------------------------------------------------------------

// TestE2E_MembershipChange_DuringLeaderElection kills the leader and
// immediately attempts a membership change (add node). The test verifies
// that the membership change either succeeds after the new leader is
// elected or returns an appropriate error (NotLeader, SystemBusy, etc.),
// and that the cluster reaches a consistent state with no data loss.
func TestE2E_MembershipChange_DuringLeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Write baseline data and ensure full replication before disruption.
	baselineData := proposeNEntries(t, cluster, trackers, shardID, 10, "baseline")
	verifyAllData(t, cluster, trackers, shardID, baselineData)
	t.Logf("baseline: 10 entries replicated to all 3 nodes")

	// Re-query the current leader immediately before killing it. The
	// initial leaderIdx from waitForClusterLeader may be stale if a
	// leadership change occurred during baseline data writes.
	currentLeader := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	cluster.stopNode(currentLeader)
	t.Logf("stopped current leader node[%d], waiting for new leader", currentLeader)

	// Wait for a new leader to be elected.
	newLeaderAfterFailover := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d], now attempting membership change", newLeaderAfterFailover)

	const newReplicaID uint64 = 4

	newAddr := freePort(t)
	hostCfg := testHostConfig(t, newAddr, cluster.ca.Leaf(t))
	newHost, err := quicraft.NewHost(hostCfg)
	if err != nil {
		t.Fatalf("NewHost for new node failed: %v", err)
	}
	defer func() {
		newHost.Close()
	}()

	// Start the shard on the new host in join mode BEFORE requesting
	// membership addition. This ensures the new node is reachable when
	// the leader adds it to the cluster. Without this, the leader adds
	// the 4th member but can't reach it (shard not started), and with
	// CheckQuorum enabled the leader steps down — deadlocking the
	// cluster (4 members, 2 live nodes, quorum=3).
	createFn, newTracker := kvCreateFunc()
	scfg := testShardConfig(shardID, newReplicaID)
	scfg.MemberIdentities = cluster.identities()
	if err := newHost.StartShard(nil, true, createFn, scfg); err != nil {
		t.Fatalf("StartShard(join=true) for replica=%d failed: %v", newReplicaID, err)
	}
	t.Logf("new node started in join mode at %s", newAddr)

	// Now request membership addition, through whichever node is the leader
	// AT THE POINT OF EACH ATTEMPT.
	//
	// The loop this replaces walked every live host with a two-second call
	// each and retried transient errors until failoverTimeout. That reads "no
	// answer" as "did not happen", which is false for a membership change: an
	// add whose response was lost to a second leadership move had already
	// committed, the next attempt was refused CORRECTLY with "already exists
	// in membership", and the loop ran to its deadline reporting a failure
	// the cluster never had (#112, #132). syncAddNode accepts that refusal
	// only after an attempt in this same loop was genuinely submitted AND a
	// linearizable membership read shows replica 4 as a voter at newAddr; an
	// add that never lands still fails, within the same budget.
	syncAddNode(t, cluster, shardID, newReplicaID, newAddr, newHost.NodeIdentity(), failoverTimeout)
	t.Logf("membership change succeeded after leader election")

	// Wait for the cluster to stabilize with 4 members.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	waitForMembershipSize(t, cluster.hosts[newLeaderIdx], shardID, 4, membershipTimeout)

	// And on every replica still running, not just on the one that accepted
	// the change. A membership change proposed during an election is exactly
	// where a replica can be left behind: it may have been mid-campaign, or
	// catching up, when the entry committed.
	waitForMembershipConverged(t, cluster, shardID,
		[]uint64{1, 2, 3, newReplicaID}, nil, membershipTimeout)
	t.Logf("cluster stabilized with 4 members, new leader: node[%d]", newLeaderIdx)

	// Verify the new node catches up on all baseline data.
	waitForNodeData(t, newTracker, shardID, newReplicaID, baselineData, membershipTimeout)
	t.Logf("new node caught up on all %d baseline entries", len(baselineData))

	// Verify no data loss: all surviving original nodes have baseline data.
	verifyAllData(t, cluster, trackers, shardID, baselineData)

	// Write additional entries to confirm the cluster is fully functional.
	postData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-election-add")
	verifyAllData(t, cluster, trackers, shardID, postData)
	waitForNodeData(t, newTracker, shardID, newReplicaID, postData, membershipTimeout)

	t.Logf("cluster consistent after membership change during election: %d entries verified",
		len(baselineData)+len(postData))
}

// ---------------------------------------------------------------------------
// TestE2E_MembershipChange_ConcurrentWithProposals
// ---------------------------------------------------------------------------

// TestE2E_MembershipChange_ConcurrentWithProposals adds a node while
// concurrent proposals are in-flight. It verifies that all proposals
// before the membership change commit, the new node receives the full
// log (or snapshot), and proposals during and after the change succeed.
// All nodes (including the new one) must have consistent state.
func TestE2E_MembershipChange_ConcurrentWithProposals(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Phase 1: Write initial data to ensure the cluster is warmed up.
	initialData := proposeNEntries(t, cluster, trackers, shardID, 10, "initial")
	verifyAllData(t, cluster, trackers, shardID, initialData)
	t.Logf("phase 1: 10 initial entries replicated")

	// Phase 2: Launch concurrent writers while adding a node.
	const (
		numWriters      = 3
		writesPerWriter = 10
		newReplicaID    = 4
	)

	// Track all successfully proposed keys and their values.
	var mu sync.Mutex
	successfulWrites := make(map[string]string)
	var writeErrors atomic.Int64

	var wg sync.WaitGroup
	writerStarted := make(chan struct{})

	// Start concurrent writers.
	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			<-writerStarted
			for i := range writesPerWriter {
				key := fmt.Sprintf("concurrent-w%d-%03d", writerID, i)
				value := fmt.Sprintf("val-w%d-%03d", writerID, i)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				leaderHost := cluster.leaderHost(shardID)
				if leaderHost == nil {
					cancel()
					writeErrors.Add(1)
					continue
				}

				cmd := encodeKVPut(key, value)
				//quicraft:direct-propose leaderHost is resolved at the top of this same loop iteration
				rs, err := leaderHost.Propose(ctx, shardID, cmd)
				if err != nil {
					cancel()
					if isTransientLeadershipError(err) {
						writeErrors.Add(1)
						continue
					}
					t.Errorf("writer[%d] Propose(%q) unexpected error: %v", writerID, key, err)
					writeErrors.Add(1)
					continue
				}

				_, err = rs.Result()
				rs.Release()
				cancel()

				if err != nil {
					writeErrors.Add(1)
					continue
				}

				mu.Lock()
				successfulWrites[key] = value
				mu.Unlock()
			}
		}(w)
	}

	// Signal writers to start and concurrently add a new node.
	close(writerStarted)

	result := addNodeToCluster(t, cluster, shardID, newReplicaID, testShardConfig)
	t.Logf("added node[%d] (replica=%d) at %s while writers active", result.idx, newReplicaID, result.addr)

	// Wait for all writers to complete.
	wg.Wait()

	mu.Lock()
	numSuccessful := len(successfulWrites)
	mu.Unlock()
	t.Logf("concurrent writers completed: %d successful, %d transient errors",
		numSuccessful, writeErrors.Load())

	// A membership addition does not disturb leadership, so nearly every write
	// should land; measured at HEAD it is 30 of 30. Without a floor the whole
	// concurrent phase can contribute ZERO writes and the test still passes,
	// because every later assertion is over the set of writes that happened to
	// succeed. Two thirds leaves room for an unlucky election without
	// admitting a run that wrote nothing.
	const concurrentWriteFloor = numWriters * writesPerWriter * 2 / 3
	if numSuccessful < concurrentWriteFloor {
		t.Fatalf("only %d of %d concurrent writes committed during the membership change (floor %d, %d transient errors)",
			numSuccessful, numWriters*writesPerWriter, concurrentWriteFloor, writeErrors.Load())
	}

	// Wait for membership to reach 4.
	leaderIdx = cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	waitForMembershipSize(t, cluster.hosts[leaderIdx], shardID, 4, membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID,
		[]uint64{1, 2, 3, newReplicaID}, nil, membershipTimeout)

	// Phase 3: Write additional entries after the membership change.
	postData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-add")
	t.Logf("phase 3: 5 post-add entries proposed")

	// Merge all expected data.
	allExpected := make(map[string]string, len(initialData)+numSuccessful+len(postData))
	for k, v := range initialData {
		allExpected[k] = v
	}
	mu.Lock()
	for k, v := range successfulWrites {
		allExpected[k] = v
	}
	mu.Unlock()
	for k, v := range postData {
		allExpected[k] = v
	}

	// Verify all data on the original nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	// Verify the new node catches up on all data.
	waitForNodeData(t, result.tracker, shardID, newReplicaID, allExpected, membershipTimeout)

	t.Logf("all %d entries verified on all 4 nodes (including new node)", len(allExpected))
}

// ---------------------------------------------------------------------------
// TestE2E_MultiShardFailover_UnderLoad
// ---------------------------------------------------------------------------

// TestE2E_MultiShardFailover_UnderLoad starts a 3-node cluster with 3
// shards, runs concurrent proposals on all shards, then kills the leader
// host. It verifies that all shards elect new leaders, all in-flight
// proposals either complete or return errors, and after recovery all
// shards are functional and consistent.
//
// # What the write count asserts
//
// Each shard has one sequential writer with a budget of writesPerShard. The
// kill is armed only once EVERY shard has committed a write and is parked, so
// at the instant the host is stopped all three shards have the same amount of
// work left and are actively proposing. From there the property is exact: a
// leader-host kill costs a shard the ONE proposal that was in flight when the
// host went away, and nothing else -- every later write goes to the shard's
// new leader and must commit.
//
// That is what the per-shard floor of writesPerShard-maxLostPerShard checks. It
// is a statement about the cluster's recovery, and it holds identically on a
// fast box and a saturated one.
//
// It replaces an aggregate floor of 7-of-30 gated on a 50ms timer, which was a
// statement about the RUNNER: the writers had whatever fitted into 50ms to
// commit in, and after the kill every remaining iteration was spent instantly
// on a nil leader without a proposal being attempted. A slow runner scored
// 6-of-30 and the gate went red with nothing wrong (#107).
//
// A failed proposal is still permanent here. Nothing is re-sent, so the floor
// cannot be met by retrying past a cluster that has not recovered.
func TestE2E_MultiShardFailover_UnderLoad(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const numShards = 3
	shardIDs := [numShards]uint64{1, 2, 3}

	// Start all shards. Each shard gets its own set of trackers.
	shardTrackers := make([][]*smTracker, numShards)
	for i, sid := range shardIDs {
		shardTrackers[i] = cluster.startShard(sid)
	}

	// Wait for all shards to have leaders and write warmup data.
	for _, sid := range shardIDs {
		cluster.waitForClusterLeader(sid, leaderElectionTimeout)
	}
	t.Logf("all %d shards have leaders", numShards)

	// Write baseline data to each shard.
	shardBaseline := make([]map[string]string, numShards)
	for i, sid := range shardIDs {
		prefix := fmt.Sprintf("shard%d-baseline", sid)
		shardBaseline[i] = proposeNEntries(t, cluster, shardTrackers[i], sid, 5, prefix)
	}
	t.Logf("baseline data written to all shards")

	// Find the node that is leader for the most shards (or any leader).
	// We want to kill a node that disrupts at least one shard's leadership.
	leaderNode := -1
	for _, sid := range shardIDs {
		idx := cluster.findLeader(sid)
		if idx >= 0 {
			leaderNode = idx
			break
		}
	}
	if leaderNode < 0 {
		t.Fatal("no leader found on any shard")
	}

	// Launch concurrent writers on all shards.
	const writesPerShard = 10

	// maxLostPerShard is the whole cost a leader-host kill is allowed to have.
	// Each writer is SEQUENTIAL, so at most one proposal per shard can be
	// outstanding at the instant the host goes away; every write after it is
	// submitted to the shard's new leader and has to commit. A shard that lost
	// more than one write did not recover cleanly, and that is a statement
	// about the cluster rather than about how fast the runner is.
	const maxLostPerShard = 1

	type shardWriteResult struct {
		shardIdx int
		shardID  uint64
		success  map[string]string
		errors   int64
		// firstErr is the first proposal error the shard saw, kept so a
		// failure names the cause instead of only the count.
		firstErr error
		// leaderless is set when the shard never produced a leader within
		// failoverTimeout, so the writer stopped early.
		leaderless bool
	}

	resultCh := make(chan shardWriteResult, numShards)
	writerStarted := make(chan struct{})
	// One send per shard, the moment that shard commits its first write. Every
	// writer then parks on killArmed, so no writer can run its whole budget out
	// before the others have started.
	firstCommits := make(chan uint64, numShards)
	killArmed := make(chan struct{})
	// One send per shard, the moment that shard has SUBMITTED its next proposal
	// and is waiting for it to commit. The kill is fired on this signal, so
	// every shard has a proposal genuinely outstanding when the host goes away
	// -- which is what "under load" has to mean for the count below to be about
	// the failover at all.
	inFlight := make(chan uint64, numShards)

	for i, sid := range shardIDs {
		go func(shardIdx int, shardID uint64) {
			<-writerStarted
			success := make(map[string]string)
			var errCount int64
			var firstErr error
			armed, leaderless, awaitingKill := false, false, false
			// Announce that this shard's kill-window proposal has been
			// submitted. Called on the error path too: main must not block
			// waiting for a proposal that was refused on submission.
			announceInFlight := func() {
				if awaitingKill {
					awaitingKill = false
					inFlight <- shardID
				}
			}
			for w := range writesPerShard {
				key := fmt.Sprintf("shard%d-load-%03d", shardID, w)
				value := fmt.Sprintf("val-shard%d-%03d", shardID, w)

				// Resolve the leader at the point of proposal, and WAIT for one
				// while the shard is mid-election. Waiting is not a retry: it
				// refuses to score a write that was never attempted. Spending an
				// iteration on a nil leader is what turned this test's count into
				// "how many writes fit in the pre-kill window on this machine"
				// (#107) -- during the election every remaining iteration was
				// consumed in microseconds without a proposal being made.
				leaderHost := cluster.awaitLeaderHost(shardID, failoverTimeout)
				if leaderHost == nil {
					// The shard never re-elected. Nothing later in this loop can
					// succeed either, so stop rather than burn the budget again.
					leaderless = true
					break
				}

				// ONE attempt, and no retry. A proposal that WAS attempted and
				// then refused is a real loss and stays counted -- that is the
				// cost of the failover this test measures, and re-sending it
				// would make the test pass without the cluster recovering.
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				cmd := encodeKVPut(key, value)
				//quicraft:direct-propose leaderHost is resolved at the top of this same loop iteration
				rs, propErr := leaderHost.Propose(ctx, shardID, cmd)
				if propErr != nil {
					cancel()
					errCount++
					if firstErr == nil {
						firstErr = propErr
					}
					announceInFlight()
					continue
				}
				// Submitted. The kill fires from here, while Result is pending.
				announceInFlight()

				_, resErr := rs.Result()
				rs.Release()
				cancel()
				if resErr != nil {
					errCount++
					if firstErr == nil {
						firstErr = resErr
					}
					continue
				}
				success[key] = value

				if !armed {
					armed = true
					firstCommits <- shardID
					<-killArmed
					awaitingKill = true
				}
			}
			// The loop can end (or break) with the announcement still owed;
			// main would otherwise wait out its guard for a signal that is
			// never coming.
			announceInFlight()
			resultCh <- shardWriteResult{
				shardIdx:   shardIdx,
				shardID:    shardID,
				success:    success,
				errors:     errCount,
				firstErr:   firstErr,
				leaderless: leaderless,
			}
		}(i, sid)
	}

	// Gate the kill on PROGRESS, not on a clock. The 50ms timer this replaces
	// was the entire pre-kill budget, so the test scored how much work fitted
	// into a fixed window on the runner rather than what the cluster did with
	// it (#107).
	//
	// Two barriers, because "under load" needs both halves: every shard has
	// committed a write (so the cluster is warm and the writers are level), and
	// every shard then has a proposal actually outstanding at the kill instant.
	close(writerStarted)
	armGuard := time.NewTimer(failoverTimeout)
	defer armGuard.Stop()
	for range numShards {
		select {
		case sid := <-firstCommits:
			t.Logf("shard %d committed its first write under load", sid)
		case <-armGuard.C:
			// Release the parked writers so they finish and exit rather than
			// leaking on a channel nothing will ever close.
			close(killArmed)
			t.Fatalf("timed out after %v waiting for all %d shards to commit a "+
				"write on a healthy cluster, before the leader kill",
				failoverTimeout, numShards)
		}
	}
	close(killArmed)
	for range numShards {
		select {
		case sid := <-inFlight:
			t.Logf("shard %d has a proposal in flight", sid)
		case <-armGuard.C:
			t.Fatalf("timed out after %v waiting for all %d shards to put a "+
				"proposal in flight for the leader kill", failoverTimeout, numShards)
		}
	}

	t.Logf("stopping leader host node[%d] while %d shards are under load", leaderNode, numShards)
	cluster.stopNode(leaderNode)

	// Collect all writer results.
	shardResults := make([]shardWriteResult, numShards)
	for range numShards {
		r := <-resultCh
		shardResults[r.shardIdx] = r
	}

	totalSuccess := 0
	for _, r := range shardResults {
		t.Logf("shard %d: %d of %d writes committed, %d errors (first: %v), leaderless=%v",
			r.shardID, len(r.success), writesPerShard, r.errors, r.firstErr, r.leaderless)
		totalSuccess += len(r.success)
	}
	for _, r := range shardResults {
		if r.leaderless {
			t.Errorf("shard %d never elected a leader within %v after the kill, so it "+
				"stopped after %d of %d writes", r.shardID, failoverTimeout,
				len(r.success), writesPerShard)
		}
		// A shard that lost more than the single in-flight proposal did not
		// survive the kill. This is per-shard on purpose: an aggregate hides a
		// shard that stalled behind two that did not, and the consistency check
		// below verifies nothing for the keys a shard never wrote.
		if len(r.success) < writesPerShard-maxLostPerShard {
			t.Errorf("shard %d committed only %d of its %d writes across the leader "+
				"kill; at most %d may be lost in flight (%d errors, first: %v)",
				r.shardID, len(r.success), writesPerShard, maxLostPerShard,
				r.errors, r.firstErr)
		}
	}
	if t.Failed() {
		t.Fatalf("multi-shard failover lost more than the in-flight window: %d of %d "+
			"writes committed across %d shards", totalSuccess, numShards*writesPerShard, numShards)
	}

	// Wait for all shards to elect new leaders.
	for _, sid := range shardIDs {
		newLeader := cluster.waitForClusterLeader(sid, failoverTimeout)
		if cluster.stopped[newLeader].Load() {
			t.Fatalf("shard %d elected stopped node[%d] as leader", sid, newLeader)
		}
		t.Logf("shard %d: new leader at node[%d]", sid, newLeader)
	}

	// Verify all shards are functional: propose and verify on each.
	for i, sid := range shardIDs {
		key := fmt.Sprintf("shard%d-recovery", sid)
		proposeAndWait(t, cluster, shardTrackers[i], sid, key, "recovered")

		// Verify on all surviving nodes. A live node whose state machine never
		// registered is a startup failure, not a node to pass over: skipping
		// it removes the only broken replica from the assertion.
		for nodeIdx := range cluster.hosts {
			if cluster.stopped[nodeIdx].Load() {
				continue
			}
			nodeSM := resolveStateMachine(cluster, shardTrackers[i], sid, nodeIdx)
			if nodeSM == nil {
				t.Fatalf("shard %d node[%d] is live but has no state machine", sid, nodeIdx)
			}
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("shard %d node[%d] has recovery key", sid, nodeIdx),
				func() bool {
					val, ok := nodeSM.Get(key)
					return ok && val == "recovered"
				},
			)
		}
	}

	// Verify consistency: all successfully written data is present on
	// surviving nodes.
	for i, sid := range shardIDs {
		allExpected := make(map[string]string, len(shardBaseline[i])+len(shardResults[i].success)+1)
		for k, v := range shardBaseline[i] {
			allExpected[k] = v
		}
		for k, v := range shardResults[i].success {
			allExpected[k] = v
		}
		recoveryKey := fmt.Sprintf("shard%d-recovery", sid)
		allExpected[recoveryKey] = "recovered"

		verified := 0
		live := 0
		for nodeIdx := range cluster.hosts {
			if cluster.stopped[nodeIdx].Load() {
				continue
			}
			live++
			nodeSM := resolveStateMachine(cluster, shardTrackers[i], sid, nodeIdx)
			if nodeSM == nil {
				t.Fatalf("shard %d node[%d] is live but has no state machine", sid, nodeIdx)
			}
			for key, val := range allExpected {
				waitForCondition(t, membershipTimeout,
					fmt.Sprintf("shard %d node[%d] key=%q", sid, nodeIdx, key),
					func() bool {
						got, ok := nodeSM.Get(key)
						return ok && got == val
					},
				)
			}
			verified++
		}
		if verified < live {
			t.Fatalf("shard %d: verified %d nodes but %d are live", sid, verified, live)
		}
	}

	t.Logf("multi-shard failover complete: all %d shards recovered and consistent", numShards)
}

// ---------------------------------------------------------------------------
// TestE2E_LeaderTransfer_DuringConcurrentWrites
// ---------------------------------------------------------------------------

// TestE2E_LeaderTransfer_DuringConcurrentWrites starts concurrent writers,
// then initiates leadership transfer. It verifies the transfer completes
// successfully, all in-flight proposals complete or fail gracefully, and
// the new leader serves subsequent proposals.
func TestE2E_LeaderTransfer_DuringConcurrentWrites(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1

	trackers := cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	t.Logf("initial leader: node[%d]", leaderIdx)

	// Warm up and establish all connections.
	warmupData := proposeNEntries(t, cluster, trackers, shardID, 5, "warmup")
	verifyAllData(t, cluster, trackers, shardID, warmupData)
	t.Logf("warmup complete: 5 entries replicated")

	// Find the transfer target (a follower).
	targetIdx := cluster.findFollower(shardID)
	if targetIdx < 0 {
		t.Fatal("no follower found for transfer target")
	}
	targetReplicaID := uint64(targetIdx + 1)
	oldLeaderIdx := leaderIdx
	t.Logf("transfer target: node[%d] (replica=%d)", targetIdx, targetReplicaID)

	// Launch concurrent writers.
	const (
		numWriters      = 3
		writesPerWriter = 15
	)

	var mu sync.Mutex
	successfulWrites := make(map[string]string)
	var writeErrors atomic.Int64
	var wg sync.WaitGroup
	writerStarted := make(chan struct{})

	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			<-writerStarted
			for i := range writesPerWriter {
				key := fmt.Sprintf("transfer-w%d-%03d", writerID, i)
				value := fmt.Sprintf("val-w%d-%03d", writerID, i)

				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				leaderHost := cluster.leaderHost(shardID)
				if leaderHost == nil {
					cancel()
					writeErrors.Add(1)
					// Wait briefly for a leader to appear during transfer.
					noLeaderTimer := time.NewTimer(20 * time.Millisecond)
					<-noLeaderTimer.C
					noLeaderTimer.Stop()
					continue
				}

				cmd := encodeKVPut(key, value)
				//quicraft:direct-propose leaderHost is resolved at the top of this same loop iteration
				rs, propErr := leaderHost.Propose(ctx, shardID, cmd)
				if propErr != nil {
					cancel()
					// NotLeader and SystemBusy are expected during transfer.
					writeErrors.Add(1)
					continue
				}

				_, resErr := rs.Result()
				rs.Release()
				cancel()

				if resErr != nil {
					writeErrors.Add(1)
					continue
				}

				mu.Lock()
				successfulWrites[key] = value
				mu.Unlock()
			}
		}(w)
	}

	// Start writers, then initiate the leadership transfer.
	close(writerStarted)
	// Allow proposals to be in flight before triggering the transfer.
	flowTimer := time.NewTimer(30 * time.Millisecond)
	<-flowTimer.C
	flowTimer.Stop()

	transferCtx, transferCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer transferCancel()

	transferErr := cluster.hosts[oldLeaderIdx].RequestLeaderTransfer(
		transferCtx, shardID, targetReplicaID)
	if transferErr != nil {
		// If the leader already changed due to timing, that is acceptable.
		if !isTransientLeadershipError(transferErr) {
			t.Fatalf("RequestLeaderTransfer failed unexpectedly: %v", transferErr)
		}
		t.Logf("RequestLeaderTransfer returned transient error (acceptable): %v", transferErr)
	} else {
		t.Logf("RequestLeaderTransfer submitted successfully")
	}

	// Wait for all writers to finish.
	wg.Wait()

	mu.Lock()
	numSuccessful := len(successfulWrites)
	mu.Unlock()
	t.Logf("writers completed: %d successful, %d transient errors",
		numSuccessful, writeErrors.Load())

	// The transfer blocks proposals for its whole duration, so most writes
	// legitimately fail here; measured at HEAD 6-9 of 45 commit. The floor
	// exists only to rule out a run where the writers committed NOTHING, which
	// would leave every later consistency assertion ranging over an empty set.
	const transferWriteFloor = 3
	if numSuccessful < transferWriteFloor {
		t.Fatalf("only %d of %d writes committed across the leadership transfer (floor %d, %d transient errors)",
			numSuccessful, numWriters*writesPerWriter, transferWriteFloor, writeErrors.Load())
	}

	// Leadership must actually MOVE TO THE REQUESTED TARGET. This used to
	// accept "any stable leader", which is satisfied by the old leader simply
	// keeping the job -- the exact outcome a broken transfer produces, and the
	// outcome observed at HEAD when the sample is taken too early. A transfer
	// racing an in-flight election is legitimately refused, so the request is
	// re-issued until the cluster is able to honour it or the budget expires.
	transferDeadline := time.Now().Add(failoverTimeout)
	landed := false
	for time.Now().Before(transferDeadline) {
		curIdx := cluster.findLeader(shardID)
		if curIdx == targetIdx {
			landed = true
			break
		}
		if curIdx >= 0 {
			retryCtx, retryCancel := context.WithTimeout(context.Background(), defaultTimeout)
			retryErr := cluster.hosts[curIdx].RequestLeaderTransfer(retryCtx, shardID, targetReplicaID)
			retryCancel()
			if retryErr != nil && !isTransientLeadershipError(retryErr) {
				t.Fatalf("RequestLeaderTransfer to replica %d failed unexpectedly: %v",
					targetReplicaID, retryErr)
			}
		}
		retryTimer := time.NewTimer(100 * time.Millisecond)
		<-retryTimer.C
		retryTimer.Stop()
	}
	if !landed {
		t.Fatalf("leadership never transferred to the requested target node[%d] (replica=%d) "+
			"within %v; leader is node[%d] (was node[%d])",
			targetIdx, targetReplicaID, failoverTimeout, cluster.findLeader(shardID), oldLeaderIdx)
	}
	newLeaderIdx := targetIdx
	t.Logf("leader after transfer: node[%d] (was node[%d], target was node[%d])",
		newLeaderIdx, oldLeaderIdx, targetIdx)

	// Verify the new leader can serve proposals.
	postData := proposeNEntries(t, cluster, trackers, shardID, 5, "post-transfer")
	t.Logf("post-transfer: 5 entries proposed via new leader")

	// Merge all expected data.
	allExpected := make(map[string]string, len(warmupData)+numSuccessful+len(postData))
	for k, v := range warmupData {
		allExpected[k] = v
	}
	mu.Lock()
	for k, v := range successfulWrites {
		allExpected[k] = v
	}
	mu.Unlock()
	for k, v := range postData {
		allExpected[k] = v
	}

	// Verify consistency across all nodes.
	verifyAllData(t, cluster, trackers, shardID, allExpected)

	t.Logf("leader transfer during concurrent writes complete: %d entries consistent across all nodes",
		len(allExpected))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// isTransientLeadershipError returns true if the error is a transient
// leadership error that is expected during elections and transfers.
func isTransientLeadershipError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, quicraft.ErrNotLeader) {
		return true
	}
	if errors.Is(err, quicraft.ErrSystemBusy) {
		return true
	}
	if errors.Is(err, quicraft.ErrTimeout) {
		return true
	}
	if errors.Is(err, quicraft.ErrCanceled) {
		return true
	}
	if errors.Is(err, quicraft.ErrShardNotReady) {
		return true
	}
	// Leadership transfer in progress is transient — it clears after
	// the transfer completes or the election timeout expires. Matched by
	// sentinel since #146 re-exported it; it was a substring match on the
	// error text while the engine's error was internal and unmatchable.
	if errors.Is(err, quicraft.ErrTransferInProgress) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return false
}

// Compile-time assertion: ensure testShardConfig matches the signature
// expected by addNodeToCluster's cfgFn parameter.
var _ func(uint64, uint64) config.Config = testShardConfig
