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

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
)

// awaitReadIndex waits for rs to complete. NAReadLocalNode, like
// ReadLocalNode, serves only a completed ReadIndex. The caller releases rs.
func awaitReadIndex(t *testing.T, rs *dragonboat.RequestState, timeout time.Duration) {
	t.Helper()
	select {
	case r := <-rs.ResultC():
		if !r.Completed() {
			t.Fatalf("ReadIndex did not complete: %+v", r)
		}
	case <-time.After(timeout):
		t.Fatalf("ReadIndex did not resolve within %v", timeout)
	}
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
		awaitReadIndex(t, rs, defaultTimeout)

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
