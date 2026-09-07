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
	"errors"
	"testing"
	"time"

	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
)

// TestIntegration_DB_NAReadLocalNode verifies that NAReadLocalNode returns
// ErrNotImplemented since the QuicRaft adapter does not currently support
// the IExtended/NALookup interface. This test ensures the method exists,
// is callable, and returns the correct sentinel error rather than panicking
// or returning an unexpected error.
func TestIntegration_DB_NAReadLocalNode(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose data so there is state to potentially read.
	proposeAndWait(t, cluster, trackers, 1, "naread-key", "naread-value")
	waitForReplication(t, cluster, trackers, 1, "naread-key", "naread-value", defaultTimeout)

	// First, get a valid ReadIndex RequestState so we have a real rs to pass.
	// The ReadIndex itself is leader-bound and would be refused after an
	// election, which has nothing to do with what this test asserts, so it
	// resolves the leader instead of pinning leaderIdx. NAReadLocalNode is
	// then called on the replica that ACCEPTED the request: it is a local
	// read against that node's own state machine, so the rs and the host it
	// is presented to must be the same one.
	ri := cluster.readIndexFromLeader(t, 1, defaultTimeout)
	defer ri.RS.Release()

	// Call NAReadLocalNode which should return ErrNotImplemented.
	_, naErr := ri.Host.NAReadLocalNode(ri.RS, encodeKVGet("naread-key"))
	if naErr == nil {
		t.Fatal("NAReadLocalNode should return an error (ErrNotImplemented)")
	}
	if !errors.Is(naErr, dbSM.ErrNotImplemented) {
		t.Fatalf("NAReadLocalNode returned %v, want %v", naErr, dbSM.ErrNotImplemented)
	}
	t.Logf("NAReadLocalNode correctly returned ErrNotImplemented: %v", naErr)
}

// TestIntegration_DB_NAReadLocalNode_AllNodes verifies that NAReadLocalNode
// returns ErrNotImplemented on all nodes in the cluster (leader and followers),
// not just the leader. This ensures consistent behavior across the cluster.
func TestIntegration_DB_NAReadLocalNode_AllNodes(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Verify NAReadLocalNode returns ErrNotImplemented on every live node.
	for i, nh := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}

		//quicraft:direct-read every live node IS the subject: the assertion is that
		// NAReadLocalNode returns ErrNotImplemented on each of them
		rs, rsErr := nh.ReadIndex(1, 2*time.Second)
		if rsErr != nil {
			t.Fatalf("ReadIndex on node[%d] failed: %v", i, rsErr)
		}

		_, naErr := nh.NAReadLocalNode(rs, encodeKVGet("warmup"))
		rs.Release()

		if naErr == nil {
			t.Fatalf("NAReadLocalNode on node[%d] should return error", i)
		}
		if !errors.Is(naErr, dbSM.ErrNotImplemented) {
			t.Fatalf("NAReadLocalNode on node[%d] returned %v, want %v", i, naErr, dbSM.ErrNotImplemented)
		}
		t.Logf("node[%d] NAReadLocalNode correctly returned ErrNotImplemented", i)
	}
}
