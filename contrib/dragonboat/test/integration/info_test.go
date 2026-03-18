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

package integration_test

import (
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
)

// TestIntegration_DB_GetNodeHostInfo_Complete verifies that GetNodeHostInfo
// returns comprehensive host status including NodeHostID, RaftAddress,
// ShardInfoList, and LogInfo.
func TestIntegration_DB_GetNodeHostInfo_Complete(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Get info with full log info (SkipLogInfo=false).
	leaderHost := cluster.hosts[leaderIdx]
	info := leaderHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: false})
	if info == nil {
		t.Fatal("GetNodeHostInfo returned nil")
	}

	// Verify NodeHostID.
	if info.NodeHostID == "" {
		t.Fatal("NodeHostID should be non-empty")
	}
	t.Logf("NodeHostID = %q", info.NodeHostID)

	// Verify RaftAddress.
	if info.RaftAddress == "" {
		t.Fatal("RaftAddress should be non-empty")
	}
	if info.RaftAddress != cluster.addrs[leaderIdx] {
		t.Fatalf("RaftAddress = %q, want %q", info.RaftAddress, cluster.addrs[leaderIdx])
	}
	t.Logf("RaftAddress = %q", info.RaftAddress)

	// Verify ShardInfoList contains our shard.
	if len(info.ShardInfoList) == 0 {
		t.Fatal("ShardInfoList should not be empty")
	}

	var foundShard bool
	for _, si := range info.ShardInfoList {
		if si.ShardID == 1 {
			foundShard = true
			// Verify shard info fields.
			if si.ReplicaID == 0 {
				t.Fatal("ShardInfo.ReplicaID should be non-zero")
			}
			if si.Term == 0 {
				t.Fatal("ShardInfo.Term should be non-zero after election")
			}
			if si.Nodes == nil || len(si.Nodes) == 0 {
				t.Fatal("ShardInfo.Nodes should be populated")
			}
			t.Logf("ShardInfo: ShardID=%d, ReplicaID=%d, LeaderID=%d, Term=%d, IsLeader=%v, Nodes=%d",
				si.ShardID, si.ReplicaID, si.LeaderID, si.Term, si.IsLeader, len(si.Nodes))
			break
		}
	}
	if !foundShard {
		t.Fatal("shard 1 not found in ShardInfoList")
	}

	// Verify LogInfo is populated when SkipLogInfo=false.
	if info.LogInfo == nil {
		t.Fatal("LogInfo should be non-nil when SkipLogInfo=false")
	}
	if len(info.LogInfo) == 0 {
		t.Fatal("LogInfo should not be empty")
	}
	t.Logf("LogInfo has %d entries", len(info.LogInfo))

	// Get info with SkipLogInfo=true.
	infoSkip := leaderHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
	if infoSkip == nil {
		t.Fatal("GetNodeHostInfo with SkipLogInfo=true returned nil")
	}
	if infoSkip.LogInfo != nil {
		t.Fatalf("LogInfo should be nil when SkipLogInfo=true, got %d entries", len(infoSkip.LogInfo))
	}
	t.Logf("SkipLogInfo=true: LogInfo correctly nil")
}

// TestIntegration_DB_GetLeaderID_FourValues verifies that GetLeaderID
// returns the dragonboat four-value signature: (leaderID, term, valid, error).
func TestIntegration_DB_GetLeaderID_FourValues(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	leaderID, term, valid, err := leaderHost.GetLeaderID(1)

	// Verify error is nil.
	if err != nil {
		t.Fatalf("GetLeaderID failed: %v", err)
	}

	// Verify valid is true.
	if !valid {
		t.Fatal("GetLeaderID returned valid=false, expected true")
	}

	// Verify leader is the expected replica.
	expectedLeaderReplicaID := uint64(leaderIdx + 1)
	if leaderID != expectedLeaderReplicaID {
		t.Fatalf("leaderID = %d, want %d", leaderID, expectedLeaderReplicaID)
	}

	// Verify term is positive.
	if term == 0 {
		t.Fatal("term should be > 0 after election")
	}

	t.Logf("GetLeaderID: leaderID=%d, term=%d, valid=%v, err=%v",
		leaderID, term, valid, err)
}

// TestIntegration_DB_NodeHost_ID_And_RaftAddress verifies that the NodeHost
// ID() and RaftAddress() methods return correct non-empty values.
func TestIntegration_DB_NodeHost_ID_And_RaftAddress(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	for i, nh := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		// Verify ID() returns non-empty.
		id := nh.ID()
		if id == "" {
			t.Fatalf("node[%d] ID() returned empty string", i)
		}

		// Verify RaftAddress() returns the configured address.
		addr := nh.RaftAddress()
		if addr == "" {
			t.Fatalf("node[%d] RaftAddress() returned empty string", i)
		}
		if addr != cluster.addrs[i] {
			t.Fatalf("node[%d] RaftAddress() = %q, want %q", i, addr, cluster.addrs[i])
		}

		t.Logf("node[%d]: ID=%q, RaftAddress=%q", i, id, addr)
	}
}
