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
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// TestE2E_QuorumLossRunbook walks the quorum-loss runbook in
// docs/multi-group-host/engineering.md step by step through the public API,
// and proves the shard serves linearizable reads and writes afterwards.
//
// Seven replicas lose four, so the three survivors cannot elect. The runbook
// then: picks the most advanced survivor by ShardInfo.LastApplied, exports a
// snapshot from it while its shard is loaded, stops every survivor, imports
// the artifact with the survivors as the complete new membership, restarts
// the survivors and adds a replacement.
//
// The exporter's own import is refused with *ImportSnapshotOutOfDateError, as
// the runbook documents: it already records the artifact's index. The other
// two survivors hold the rewritten membership and form its quorum on their
// own, so the shard recovers without depending on how the exporter resumes.
func TestE2E_QuorumLossRunbook(t *testing.T) {
	const (
		shardID  = uint64(1)
		numNodes = 7
		// replacementID is a replica ID the shard has never used.
		replacementID = uint64(numNodes + 1)
	)
	survivorIdx := []int{0, 1, 2}

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	trackers := cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	expected := make(map[string]string)
	for i := range 5 {
		key := fmt.Sprintf("pre-loss-%03d", i)
		value := fmt.Sprintf("pre-val-%03d", i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	verifyAllData(t, cluster, trackers, shardID, expected)

	// Lose quorum: four of seven replicas go, and the three left cannot
	// elect a leader among themselves.
	for idx := len(survivorIdx); idx < numNodes; idx++ {
		cluster.stopNode(idx)
	}
	waitForCondition(t, failoverTimeout, "no leader after quorum loss", func() bool {
		return cluster.findLeader(shardID) < 0
	})

	// Step 1: the most up-to-date survivor, by ShardInfo.LastApplied, with
	// the survivors' shards still started.
	exporter := -1
	var bestApplied uint64
	for _, idx := range survivorIdx {
		info, err := cluster.hosts[idx].ShardInfo(shardID)
		if err != nil {
			t.Fatalf("ShardInfo on survivor node[%d]: %v", idx, err)
		}
		if exporter < 0 || info.LastApplied > bestApplied {
			exporter, bestApplied = idx, info.LastApplied
		}
	}
	exportHost := cluster.hosts[exporter]
	t.Logf("exporter is node[%d] (replica=%d) at applied index %d", exporter, exporter+1, bestApplied)

	// Step 2: export a snapshot from it while its shard is loaded.
	// ExportSnapshot exports the latest snapshot recorded in LogDB, so one
	// is taken first; a snapshot is local and needs no quorum.
	snapIndex := requestSnapshotSync(t, exportHost, shardID)
	var artifact bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	err := exportHost.ExportSnapshot(ctx, shardID, &artifact)
	cancel()
	if err != nil {
		t.Fatalf("ExportSnapshot on node[%d]: %v", exporter, err)
	}

	// Step 3: stop every surviving replica. ImportSnapshot refuses a
	// loaded shard.
	for _, idx := range survivorIdx {
		if err := cluster.hosts[idx].StopShard(shardID); err != nil {
			t.Fatalf("StopShard on survivor node[%d]: %v", idx, err)
		}
	}

	// Step 4: import on every survivor, naming only the survivors and their
	// identities.
	allIDs := cluster.identities()
	survivors := make(map[uint64]string, len(survivorIdx))
	survivorIDs := make(map[uint64]string, len(survivorIdx))
	for _, idx := range survivorIdx {
		replicaID := uint64(idx + 1)
		survivors[replicaID] = cluster.addrs[idx]
		survivorIDs[replicaID] = allIDs[replicaID]
	}
	for _, idx := range survivorIdx {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		err := cluster.hosts[idx].ImportSnapshot(ctx, shardID, bytes.NewReader(artifact.Bytes()),
			quicraft.WithImportMembership(survivors),
			quicraft.WithImportMemberIdentities(survivorIDs))
		cancel()
		if idx != exporter {
			if err != nil {
				t.Fatalf("ImportSnapshot on survivor node[%d]: %v", idx, err)
			}
			continue
		}
		// The exporter already records the artifact's index.
		var outOfDate *quicraft.ImportSnapshotOutOfDateError
		if !errors.As(err, &outOfDate) {
			t.Fatalf("ImportSnapshot on the exporter node[%d] = %v, want *ImportSnapshotOutOfDateError",
				idx, err)
		}
		if outOfDate.Index != snapIndex || outOfDate.Recorded < snapIndex {
			t.Fatalf("exporter refusal reports artifact index %d, recorded %d; want artifact %d, recorded >= %d",
				outOfDate.Index, outOfDate.Recorded, snapIndex, snapIndex)
		}
	}

	// Step 5: restart the survivors with the survivors as members and
	// their identities in MemberIdentities.
	for _, idx := range survivorIdx {
		createFn, tracker := kvCreateFunc()
		scfg := testShardConfig(shardID, uint64(idx+1))
		scfg.MemberIdentities = survivorIDs
		if err := cluster.hosts[idx].StartShard(survivors, false, createFn, scfg); err != nil {
			t.Fatalf("StartShard on survivor node[%d]: %v", idx, err)
		}
		cluster.hostsMu.Lock()
		trackers[idx] = tracker
		cluster.hostsMu.Unlock()
	}

	leaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("leader after the runbook: node[%d]", leaderIdx)
	for _, idx := range survivorIdx {
		if idx == exporter {
			continue
		}
		waitForMembershipSize(t, cluster.hosts[idx], shardID, len(survivorIdx), membershipTimeout)
	}

	// A value written before quorum loss is served by a linearizable read,
	// and the shard takes a new write and serves it back.
	assertLinearizableRead(t, cluster, shardID, "pre-loss-000", expected["pre-loss-000"])
	proposeAndWait(t, cluster, trackers, shardID, "post-recovery", "restored")
	expected["post-recovery"] = "restored"
	assertLinearizableRead(t, cluster, shardID, "post-recovery", "restored")

	// Step 6: add a replacement normally once the shard has a leader.
	added := addNodeToCluster(t, cluster, shardID, replacementID, testShardConfig)
	t.Logf("replacement replica=%d joined at node[%d]", replacementID, added.idx)
	proposeAndWait(t, cluster, cluster.trackers, shardID, "post-replacement", "grown")
	expected["post-replacement"] = "grown"
	assertLinearizableRead(t, cluster, shardID, "post-replacement", "grown")

	verifyAllData(t, cluster, cluster.trackers, shardID, expected)
}

// assertLinearizableRead reads key through the shard's current leader and
// fails unless it returns want.
func assertLinearizableRead(t *testing.T, cluster *testCluster, shardID uint64, key, want string) {
	t.Helper()
	result := proposer.Read(t, cluster.leaderHost, shardID, encodeKVGet(key), defaultTimeout)
	got, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead(%q) returned type %T, want string", key, result)
	}
	if got != want {
		t.Fatalf("SyncRead(%q) = %q, want %q", key, got, want)
	}
}
