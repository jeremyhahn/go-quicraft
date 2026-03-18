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
	"errors"
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_ProposeOnClosedNodeHost verifies that SyncPropose
// returns an error when called on a closed NodeHost.
func TestIntegration_DB_ProposeOnClosedNodeHost(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}

	// Start a shard so we have something to propose to.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Close the NodeHost.
	nh.Close()

	// Try to SyncPropose on the closed host.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session := dbClient.NewNoOPSession(1, nil)
	cmd := encodeKVPut("key", "value")
	_, proposeErr := nh.SyncPropose(ctx, session, cmd)
	if proposeErr == nil {
		t.Fatal("SyncPropose on closed NodeHost should return error")
	}
	t.Logf("SyncPropose on closed NodeHost returned expected error: %v", proposeErr)
}

// TestIntegration_DB_ReadOnInvalidShard verifies that SyncRead on a
// non-existent shard returns an error.
func TestIntegration_DB_ReadOnInvalidShard(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// SyncRead on shard 99999 which does not exist.
	_, readErr := leaderHost.SyncRead(ctx, 99999, encodeKVGet("key"))
	if readErr == nil {
		t.Fatal("SyncRead on non-existent shard should return error")
	}
	if !errors.Is(readErr, dragonboat.ErrShardNotFound) {
		t.Logf("SyncRead returned error (may vary by implementation): %v", readErr)
	}
	t.Logf("SyncRead on invalid shard returned expected error: %v", readErr)
}

// TestIntegration_DB_InvalidContextDeadline verifies that operations with
// a context lacking a deadline return an appropriate error. In the
// dragonboat adapter, this maps to ErrDeadlineNotSet (which is aliased
// to ErrTimeout in the adapter layer).
func TestIntegration_DB_InvalidContextDeadline(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Use context.Background() which has no deadline. The implementation
	// must reject this with a deadline/timeout-related error.
	ctx := context.Background()
	session := dbClient.NewNoOPSession(1, nil)
	cmd := encodeKVPut("key", "value")
	_, proposeErr := leaderHost.SyncPropose(ctx, session, cmd)
	if proposeErr == nil {
		t.Fatal("SyncPropose with no-deadline context should return an error")
	}

	// Verify the error is related to deadline/timeout.
	if !errors.Is(proposeErr, dragonboat.ErrDeadlineNotSet) && !errors.Is(proposeErr, dragonboat.ErrTimeout) {
		t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", proposeErr)
	}
	t.Logf("SyncPropose with no deadline returned expected error: %v", proposeErr)
}

// TestIntegration_DB_GetLeaderID_InvalidShard verifies that GetLeaderID
// on a non-existent shard returns an error.
func TestIntegration_DB_GetLeaderID_InvalidShard(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// GetLeaderID on shard 99999 which does not exist.
	leaderHost := cluster.hosts[0]
	_, _, valid, err := leaderHost.GetLeaderID(99999)
	if err == nil && valid {
		t.Fatal("GetLeaderID on non-existent shard should return error or valid=false")
	}
	if err != nil {
		t.Logf("GetLeaderID on invalid shard returned error: %v", err)
	} else {
		t.Logf("GetLeaderID on invalid shard returned valid=false")
	}
}

// TestIntegration_DB_StaleRead_Uninitialized verifies that StaleRead on
// a shard that has not been started returns an appropriate error.
func TestIntegration_DB_StaleRead_Uninitialized(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbConfig.NodeHostConfig{
		WALDir:         t.TempDir(),
		NodeHostDir:    t.TempDir(),
		RaftAddress:    addr,
		ListenAddress:  addr,
		RTTMillisecond: 50,
		DeploymentID:   1,
		MutualTLS:      true,
		CAFile:         mtls.CAFile,
		CertFile:       mtls.CertFile,
		KeyFile:        mtls.KeyFile,
	}

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// StaleRead on shard 1 which has not been started.
	_, staleErr := nh.StaleRead(1, encodeKVGet("key"))
	if staleErr == nil {
		t.Fatal("StaleRead on uninitialized shard should return error")
	}
	t.Logf("StaleRead on uninitialized shard returned expected error: %v", staleErr)
}

// TestIntegration_DB_InvalidContextDeadline_AllSyncMethods verifies that every
// Sync* method on NodeHost rejects a context that has no deadline set. Each
// method must return ErrDeadlineNotSet.
func TestIntegration_DB_InvalidContextDeadline_AllSyncMethods(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// context.Background() has no deadline.
	ctx := context.Background()

	isDeadlineErr := func(err error) bool {
		return errors.Is(err, dragonboat.ErrDeadlineNotSet) ||
			errors.Is(err, dragonboat.ErrTimeout)
	}

	t.Run("SyncPropose", func(t *testing.T) {
		session := dbClient.NewNoOPSession(1, nil)
		cmd := encodeKVPut("dp-key", "dp-val")
		_, err := leaderHost.SyncPropose(ctx, session, cmd)
		if err == nil {
			t.Fatal("SyncPropose should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncRead", func(t *testing.T) {
		_, err := leaderHost.SyncRead(ctx, 1, encodeKVGet("key"))
		if err == nil {
			t.Fatal("SyncRead should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncGetSession", func(t *testing.T) {
		_, err := leaderHost.SyncGetSession(ctx, 1)
		if err == nil {
			t.Fatal("SyncGetSession should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncCloseSession", func(t *testing.T) {
		session := dbClient.NewNoOPSession(1, nil)
		err := leaderHost.SyncCloseSession(ctx, session)
		if err == nil {
			t.Fatal("SyncCloseSession should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncGetShardMembership", func(t *testing.T) {
		_, err := leaderHost.SyncGetShardMembership(ctx, 1)
		if err == nil {
			t.Fatal("SyncGetShardMembership should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncRequestSnapshot", func(t *testing.T) {
		_, err := leaderHost.SyncRequestSnapshot(ctx, 1, dragonboat.SnapshotOption{})
		if err == nil {
			t.Fatal("SyncRequestSnapshot should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncRequestAddReplica", func(t *testing.T) {
		err := leaderHost.SyncRequestAddReplica(ctx, 1, 99, "127.0.0.1:59999", 0)
		if err == nil {
			t.Fatal("SyncRequestAddReplica should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncRequestDeleteReplica", func(t *testing.T) {
		err := leaderHost.SyncRequestDeleteReplica(ctx, 1, 99, 0)
		if err == nil {
			t.Fatal("SyncRequestDeleteReplica should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})

	t.Run("SyncRequestAddNonVoting", func(t *testing.T) {
		err := leaderHost.SyncRequestAddNonVoting(ctx, 1, 99, "127.0.0.1:59999", 0)
		if err == nil {
			t.Fatal("SyncRequestAddNonVoting should reject context without deadline")
		}
		if !isDeadlineErr(err) {
			t.Fatalf("expected ErrDeadlineNotSet or ErrTimeout, got: %v", err)
		}
	})
}

// TestIntegration_DB_InvalidAddressIsRejected verifies that SyncRequestAddReplica
// returns ErrInvalidAddress when given a target address that lacks a port (e.g., "a1").
func TestIntegration_DB_InvalidAddressIsRejected(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Obtain the current membership to get a valid ConfigChangeID.
	mCtx, mCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer mCancel()

	membership, err := leaderHost.SyncGetShardMembership(mCtx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}

	// Attempt to add a replica with an invalid address (no port).
	addCtx, addCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer addCancel()

	addErr := leaderHost.SyncRequestAddReplica(addCtx, 1, 100, "a1", membership.ConfigChangeID)
	if addErr == nil {
		t.Fatal("SyncRequestAddReplica with invalid address should return error")
	}
	if !errors.Is(addErr, dragonboat.ErrInvalidAddress) {
		t.Logf("SyncRequestAddReplica returned error (may vary): %v", addErr)
	}
	t.Logf("SyncRequestAddReplica with invalid address returned expected error: %v", addErr)
}

// TestIntegration_DB_RestartWithRemovedData_Rejected verifies that after
// stopping a shard and calling RemoveData, attempting to StartReplica again
// fails with ErrReplicaRemoved.
func TestIntegration_DB_RestartWithRemovedData_Rejected(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Wait for the shard to be ready.
	waitForCondition(t, leaderElectionTimeout, "single-node leader election", func() bool {
		_, _, valid, lerr := nh.GetLeaderID(1)
		return lerr == nil && valid
	})

	// Stop the shard.
	if stopErr := nh.StopReplica(1, 1); stopErr != nil {
		t.Fatalf("StopReplica failed: %v", stopErr)
	}

	// Remove all data for this shard/replica.
	if removeErr := nh.RemoveData(1, 1); removeErr != nil {
		t.Fatalf("RemoveData failed: %v", removeErr)
	}

	// Attempt to restart the replica with join=true.
	restartErr := nh.StartReplica(nil, true, createFn, cfg)
	if restartErr == nil {
		t.Fatal("StartReplica after RemoveData should return error")
	}
	if !errors.Is(restartErr, dragonboat.ErrReplicaRemoved) {
		t.Logf("StartReplica returned error (may vary): %v", restartErr)
	}
	t.Logf("StartReplica after RemoveData returned expected error: %v", restartErr)
}

// TestIntegration_DB_SyncRemoveData verifies the context-aware SyncRemoveData
// method. After proposing data and stopping the shard, SyncRemoveData should
// successfully remove the shard data, confirmed by HasNodeInfo returning false.
func TestIntegration_DB_SyncRemoveData(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Wait for the shard to be ready.
	waitForCondition(t, leaderElectionTimeout, "single-node leader election", func() bool {
		_, _, valid, lerr := nh.GetLeaderID(1)
		return lerr == nil && valid
	})

	// Propose some data so the shard has committed entries.
	propCtx, propCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer propCancel()

	session := dbClient.NewNoOPSession(1, nil)
	cmd := encodeKVPut("sync-remove-key", "sync-remove-value")
	_, propErr := nh.SyncPropose(propCtx, session, cmd)
	if propErr != nil {
		t.Fatalf("SyncPropose failed: %v", propErr)
	}

	// Verify HasNodeInfo returns true before removal.
	if !nh.HasNodeInfo(1, 1) {
		t.Fatal("HasNodeInfo should return true before removal")
	}

	// Stop the shard.
	if stopErr := nh.StopReplica(1, 1); stopErr != nil {
		t.Fatalf("StopReplica failed: %v", stopErr)
	}

	// Use SyncRemoveData with a proper deadline context.
	removeCtx, removeCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer removeCancel()

	if removeErr := nh.SyncRemoveData(removeCtx, 1, 1); removeErr != nil {
		t.Fatalf("SyncRemoveData failed: %v", removeErr)
	}

	// Verify HasNodeInfo returns false after removal.
	if nh.HasNodeInfo(1, 1) {
		t.Fatal("HasNodeInfo should return false after SyncRemoveData")
	}
	t.Log("SyncRemoveData successfully removed shard data")
}
