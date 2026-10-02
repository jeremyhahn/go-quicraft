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
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// barrierSnapshotConfig is the shard config every barrier test uses.
//
// SnapshotEntries has to be set explicitly: it has NO default (declared in
// pkg/config/config.go and never assigned in SetDefaults), so a shard started
// with testShardConfig keeps SnapshotEntries == 0, never takes a snapshot, and
// therefore never asks the barrier to encrypt anything. Every barrier test that
// starts a shard without this is asserting on an at-rest pipeline that was
// never run.
func barrierSnapshotConfig(shardID, replicaID uint64) config.Config {
	return testShardConfigWithSnapshot(shardID, replicaID, 15, 5)
}

// barrierCanary is embedded in every value a barrier test writes. It is what
// the on-disk assertion greps for: a distinctive token that must NOT survive
// into a snapshot artifact, because the barrier is supposed to have sealed it.
const barrierCanary = "CANARY-2f8b1d-PLAINTEXT"

// assertBarrierSealedEverything is the shared at-rest assertion. For every live
// node it requires that the node's barrier actually sealed snapshot frames, and
// that the plaintext canary is absent from every snapshot artifact that node
// wrote under its NodeHostDir.
//
// Both halves are needed and neither implies the other. The seal count proves
// the encrypt path RAN; the byte search proves what it produced is not readable
// off the disk. A round trip through the engine proves neither, because it
// reads back whatever it wrote either way.
func assertBarrierSealedEverything(t *testing.T, cluster *barrierTestCluster, canaries []string) {
	t.Helper()

	checked := 0
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		if i >= len(cluster.barriers) {
			t.Fatalf("node[%d] is live but has no barrier recorded", i)
		}
		b := cluster.barriers[i]
		if b.IsSealed() {
			t.Fatalf("barrier[%d] is sealed; expected unsealed for active encryption", i)
		}
		label := fmt.Sprintf("node[%d]", i)
		dir := cluster.nodeHostDir(i)

		// The artifact has to exist before the seal count can be meaningful,
		// and snapshots are written asynchronously, so wait for the file first.
		assertSnapshotsSealed(t, label, dir, canaries)

		if seals := b.snapshotFrameSeals(); seals == 0 {
			t.Fatalf("%s wrote a snapshot artifact but its barrier sealed 0 frames: "+
				"the at-rest encrypt path was never invoked", label)
		} else {
			t.Logf("%s barrier sealed %d snapshot frame(s)", label, seals)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("assertBarrierSealedEverything checked no nodes")
	}
	t.Logf("at-rest encryption verified on %d node(s)", checked)
}

// TestE2E_Barrier_ProposeAndRead verifies that a 3-node barrier-encrypted
// cluster can propose entries, replicate them, and serve linearizable reads,
// and that the data those entries carry is ENCRYPTED where it lands on disk.
//
// The at-rest half is the reason this test exists. Asserting only
// IsSealed() == false — an in-memory flag no production path touches — on a
// shard whose SnapshotEntries is 0 takes no snapshot and never asks the
// barrier to encrypt a byte, so a total plaintext downgrade would pass. It
// takes a real snapshot and asserts both that the barrier sealed
// frames and that the plaintext canary is absent from the artifact.
func TestE2E_Barrier_ProposeAndRead(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShardWithConfig(1, barrierSnapshotConfig)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4) to commit a
	// no-op at the current term before reads can be linearized.
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose entries whose values carry the canary. With SnapshotEntries=15
	// this also crosses the automatic snapshot threshold on every replica.
	expected := make(map[string]string, 30)
	for i := range 30 {
		key := fmt.Sprintf("barrier-rw-%03d", i)
		value := fmt.Sprintf("val-%s-barrier-rw-%03d", barrierCanary, i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all entries replicated to all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 30 entries replicated to 3 barrier-encrypted nodes")

	// Perform a linearizable read for one of the keys. The subject is the
	// value the barrier-encrypted cluster returns, not the replica that
	// serves it, so the leader is resolved at the point of the read rather
	// than reused from the election above: thirty proposals and a
	// verifyAllData walk have run since then, any of which can span an
	// election.
	targetKey := "barrier-rw-005"
	expectedValue := expected[targetKey]
	result := proposer.Read(t, cluster.leaderHost, 1, encodeKVGet(targetKey), defaultTimeout)

	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != expectedValue {
		t.Fatalf("SyncRead(%q) = %q, want %q", targetKey, val, expectedValue)
	}
	t.Logf("SyncRead(%q) = %q (correct)", targetKey, val)

	// The snapshot below is deliberately still addressed to one replica: it
	// forces the at-rest write this test then greps for, so it must happen on
	// a host whose data directory the assertion knows.
	leaderHost := cluster.hosts[leaderIdx]

	// Force a snapshot on the leader and WAIT for it, so the at-rest assertion
	// does not depend on the automatic trigger having already fired there.
	snapshotIndex := requestSnapshotSync(t, leaderHost, 1)
	t.Logf("explicit snapshot on leader node[%d] completed at index %d", leaderIdx, snapshotIndex)

	assertBarrierSealedEverything(t, cluster, []string{barrierCanary, expectedValue})
}

// TestE2E_Barrier_Replication verifies that a 3-node barrier-encrypted
// cluster replicates data identically across all nodes despite each node
// having an independent barrier with a distinct passphrase, and that each
// node's own barrier sealed its own snapshot artifacts.
//
// Independent per-node keys are the interesting part: the decrypted state must
// match across replicas while the CIPHERTEXT on each node is that node's own.
func TestE2E_Barrier_Replication(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShardWithConfig(1, barrierSnapshotConfig)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 30 entries to exercise replication under encryption. The values
	// carry the canary so the on-disk assertion has something to look for.
	expected := make(map[string]string, 30)
	for i := range 30 {
		key := fmt.Sprintf("barrier-repl-%03d", i)
		value := fmt.Sprintf("val-%s-barrier-repl-%03d", barrierCanary, i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all entries on all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 3 barrier-encrypted nodes")

	// Collect state machine snapshots from all live nodes and compare.
	// Despite independent barriers on each host, the decrypted state
	// machine data must be identical across all replicas.
	snapshots := make([]map[string]string, 0, 3)
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		nodeSM := resolveStateMachine(cluster.testCluster, trackers, 1, i)
		if nodeSM == nil {
			t.Fatalf("node[%d] has no state machine", i)
		}
		snap := nodeSM.Snapshot()
		snapshots = append(snapshots, snap)
	}

	if len(snapshots) < 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snapshots))
	}

	// Compare every snapshot against the first to ensure consistency.
	reference := snapshots[0]
	for i := 1; i < len(snapshots); i++ {
		if len(snapshots[i]) != len(reference) {
			t.Fatalf("node[%d] has %d keys, node[0] has %d keys",
				i, len(snapshots[i]), len(reference))
		}
		for key, refVal := range reference {
			got, exists := snapshots[i][key]
			if !exists {
				t.Fatalf("node[%d] missing key %q present on node[0]", i, key)
			}
			if got != refVal {
				t.Fatalf("node[%d] key %q = %q, node[0] has %q", i, key, got, refVal)
			}
		}
	}
	t.Logf("all 3 barrier-encrypted nodes have identical state machine data (%d keys)", len(reference))

	// Force a snapshot on the leader and wait for it, then require that every
	// node's own barrier sealed frames and that no artifact leaks the canary.
	snapshotIndex := requestSnapshotSync(t, cluster.hosts[leaderIdx], 1)
	t.Logf("explicit snapshot on leader node[%d] completed at index %d", leaderIdx, snapshotIndex)

	assertBarrierSealedEverything(t, cluster,
		[]string{barrierCanary, expected["barrier-repl-007"]})
}

// TestE2E_Barrier_SnapshotRestore verifies that automatic and explicit
// snapshots work correctly in a barrier-encrypted cluster. Data written
// before and after snapshots must be fully recoverable.
func TestE2E_Barrier_SnapshotRestore(t *testing.T) {
	cluster := newBarrierTestCluster(t, 3)
	defer cluster.close()

	// Configure aggressive snapshot settings: every 15 entries with
	// 5 entries of compaction overhead.
	trackers := cluster.startShardWithConfig(1, barrierSnapshotConfig)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 30 entries. With SnapshotEntries=15, this triggers at
	// least one automatic snapshot with barrier encryption.
	expected := make(map[string]string, 30)
	for i := range 30 {
		key := fmt.Sprintf("barrier-snap-%03d", i)
		value := fmt.Sprintf("val-%s-barrier-snap-%03d", barrierCanary, i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("30 entries proposed and verified, auto-snapshot should have triggered")

	// Request an explicit snapshot from the leader and WAIT for it. This
	// exercises the full snapshot pipeline: save -> encrypt -> persist ->
	// acknowledge.
	leaderHost := cluster.hosts[leaderIdx]
	snapshotIndex := requestSnapshotSync(t, leaderHost, 1)
	t.Logf("explicit snapshot completed at index %d", snapshotIndex)

	// Propose 10 more entries after the snapshot. This ensures that
	// entries after the snapshot point are correctly applied on top
	// of any future snapshot restore.
	for i := range 10 {
		key := fmt.Sprintf("barrier-post-%03d", i)
		value := fmt.Sprintf("val-%s-barrier-post-%03d", barrierCanary, i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 40 entries (30 pre-snapshot + 10 post-snapshot)
	// are present and correct on all 3 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 40 entries verified on 3 barrier-encrypted nodes after snapshot")

	// The snapshot the pipeline just produced must be ciphertext on disk.
	assertBarrierSealedEverything(t, cluster,
		[]string{barrierCanary, expected["barrier-snap-007"]})
}

// TestE2E_Barrier_FiveNode verifies that a 5-node barrier-encrypted cluster
// handles proposals, replication, leader failover, and continued writes.
// This exercises the barrier under a larger quorum (3 of 5).
func TestE2E_Barrier_FiveNode(t *testing.T) {
	cluster := newBarrierTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShardWithConfig(1, barrierSnapshotConfig)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("5-node barrier cluster leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster.testCluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster.testCluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 20 entries through the barrier-encrypted 5-node cluster. With
	// SnapshotEntries=15 this crosses the automatic snapshot threshold on
	// every replica, so each node's own barrier has real work to do.
	expected := make(map[string]string, 20)
	for i := range 20 {
		key := fmt.Sprintf("barrier5n-%03d", i)
		value := fmt.Sprintf("val-%s-barrier5n-%03d", barrierCanary, i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 20 entries on all 5 nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("20 entries verified on all 5 barrier-encrypted nodes")

	// Snapshot before the failover so the pre-failover state is on disk under
	// the original leader's own barrier too.
	preFailoverIndex := requestSnapshotSync(t, cluster.hosts[leaderIdx], 1)
	t.Logf("pre-failover snapshot on node[%d] completed at index %d", leaderIdx, preFailoverIndex)

	// Kill the leader and wait for a new leader to be elected.
	oldLeaderIdx := cluster.findLeader(1)
	if oldLeaderIdx < 0 {
		t.Fatal("no leader found before failover")
	}
	t.Logf("stopping leader node[%d]", oldLeaderIdx)
	cluster.stopNode(oldLeaderIdx)

	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the stopped node")
	}
	t.Logf("new leader elected at node[%d] after failover", newLeaderIdx)

	// Propose 10 more entries through the new leader.
	for i := range 10 {
		key := fmt.Sprintf("barrier5n-post-%03d", i)
		value := fmt.Sprintf("val-%s-barrier5n-post-%03d", barrierCanary, i)
		proposeAndWait(t, cluster.testCluster, trackers, 1, key, value)
		expected[key] = value
	}

	// Verify all 30 entries on the 4 surviving nodes.
	verifyAllData(t, cluster.testCluster, trackers, 1, expected)
	t.Logf("all 30 entries verified on 4 surviving barrier-encrypted nodes after failover")

	// Snapshot through the NEW leader so the post-failover state is written
	// under its barrier, then require ciphertext on every surviving node.
	postFailoverIndex := requestSnapshotSync(t, cluster.hosts[newLeaderIdx], 1)
	t.Logf("post-failover snapshot on node[%d] completed at index %d", newLeaderIdx, postFailoverIndex)
	if postFailoverIndex <= preFailoverIndex {
		t.Fatalf("post-failover snapshot index %d did not advance past the pre-failover index %d",
			postFailoverIndex, preFailoverIndex)
	}

	assertBarrierSealedEverything(t, cluster,
		[]string{barrierCanary, expected["barrier5n-007"], expected["barrier5n-post-003"]})
}
