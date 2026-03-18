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
	"context"
	"fmt"
	"testing"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_ConcurrentStateMachine verifies that a 3-node cluster
// using StartConcurrentReplica with the IConcurrentStateMachine interface
// can propose entries and replicate them. The concurrent SM receives batch
// updates and supports concurrent reads during writes.
func TestIntegration_DB_ConcurrentStateMachine(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	concTrackers := cluster.startConcurrentShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("concurrent SM cluster leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	leaderHost := cluster.hosts[leaderIdx]
	session := dbClient.NewNoOPSession(1, nil)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	warmupCmd := encodeKVPut("warmup", "ok")
	_, err := leaderHost.SyncPropose(ctx, session, warmupCmd)
	if err != nil {
		t.Fatalf("warmup SyncPropose failed: %v", err)
	}

	// Wait for warmup to replicate to all concurrent SMs.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || concTrackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		sm := concTrackers[i].Get(1, replicaID)
		if sm == nil {
			continue
		}
		waitForCondition(t, defaultTimeout, fmt.Sprintf("node[%d] warmup", i), func() bool {
			val, ok := sm.Get("warmup")
			return ok && val == "ok"
		})
	}

	// Propose 10 entries through the concurrent SM.
	expected := make(map[string]string, 10)
	for i := range 10 {
		key := fmt.Sprintf("conc-%03d", i)
		value := fmt.Sprintf("val-conc-%03d", i)
		expected[key] = value

		cmd := encodeKVPut(key, value)
		propCtx, propCancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, propErr := leaderHost.SyncPropose(propCtx, session, cmd)
		propCancel()
		if propErr != nil {
			t.Fatalf("SyncPropose(%q) failed: %v", key, propErr)
		}
	}

	// Verify all entries replicated to all nodes' concurrent SMs.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || concTrackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		sm := concTrackers[i].Get(1, replicaID)
		if sm == nil {
			continue
		}
		for key, val := range expected {
			waitForCondition(t, defaultTimeout, fmt.Sprintf("node[%d] key=%q", i, key), func() bool {
				got, ok := sm.Get(key)
				return ok && got == val
			})
		}
	}

	// Verify via SyncRead through the adapter.
	for key, expectedVal := range expected {
		readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
		result, readErr := leaderHost.SyncRead(readCtx, 1, encodeKVGet(key))
		readCancel()
		if readErr != nil {
			t.Fatalf("SyncRead(%q) failed: %v", key, readErr)
		}
		val, ok := result.(string)
		if !ok {
			t.Fatalf("SyncRead(%q) returned type %T, want string", key, result)
		}
		if val != expectedVal {
			t.Fatalf("SyncRead(%q) = %q, want %q", key, val, expectedVal)
		}
	}

	t.Logf("concurrent SM: 10 entries verified on all 3 nodes")
}

// TestIntegration_DB_OnDiskStateMachine verifies that a 3-node cluster
// using StartOnDiskReplica with the IOnDiskStateMachine interface can
// propose entries and replicate them. The on-disk SM uses Open/Sync and
// receives batch updates.
func TestIntegration_DB_OnDiskStateMachine(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	diskTrackers := cluster.startOnDiskShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("on-disk SM cluster leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	leaderHost := cluster.hosts[leaderIdx]
	session := dbClient.NewNoOPSession(1, nil)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	warmupCmd := encodeKVPut("warmup", "ok")
	_, err := leaderHost.SyncPropose(ctx, session, warmupCmd)
	if err != nil {
		t.Fatalf("warmup SyncPropose failed: %v", err)
	}

	// Wait for warmup to replicate to all on-disk SMs.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || diskTrackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		sm := diskTrackers[i].Get(1, replicaID)
		if sm == nil {
			continue
		}
		waitForCondition(t, defaultTimeout, fmt.Sprintf("node[%d] warmup", i), func() bool {
			val, ok := sm.Get("warmup")
			return ok && val == "ok"
		})
	}

	// Propose 10 entries through the on-disk SM.
	expected := make(map[string]string, 10)
	for i := range 10 {
		key := fmt.Sprintf("disk-%03d", i)
		value := fmt.Sprintf("val-disk-%03d", i)
		expected[key] = value

		cmd := encodeKVPut(key, value)
		propCtx, propCancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, propErr := leaderHost.SyncPropose(propCtx, session, cmd)
		propCancel()
		if propErr != nil {
			t.Fatalf("SyncPropose(%q) failed: %v", key, propErr)
		}
	}

	// Verify all entries replicated to all nodes' on-disk SMs.
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || diskTrackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		sm := diskTrackers[i].Get(1, replicaID)
		if sm == nil {
			continue
		}
		for key, val := range expected {
			waitForCondition(t, defaultTimeout, fmt.Sprintf("node[%d] key=%q", i, key), func() bool {
				got, ok := sm.Get(key)
				return ok && got == val
			})
		}
	}

	// Verify via SyncRead through the adapter.
	for key, expectedVal := range expected {
		readCtx, readCancel := context.WithTimeout(context.Background(), defaultTimeout)
		result, readErr := leaderHost.SyncRead(readCtx, 1, encodeKVGet(key))
		readCancel()
		if readErr != nil {
			t.Fatalf("SyncRead(%q) failed: %v", key, readErr)
		}
		val, ok := result.(string)
		if !ok {
			t.Fatalf("SyncRead(%q) returned type %T, want string", key, result)
		}
		if val != expectedVal {
			t.Fatalf("SyncRead(%q) = %q, want %q", key, val, expectedVal)
		}
	}

	t.Logf("on-disk SM: 10 entries verified on all 3 nodes")
}
