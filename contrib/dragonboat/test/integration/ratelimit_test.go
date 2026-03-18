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

// TestIntegration_DB_ProposalsWorkWhenRateLimited verifies that proposals
// succeed when the per-shard MaxInMemLogSize is set but the limit is not
// yet reached. Matches dragonboat's TestProposalsCanBeMadeWhenRateLimited.
func TestIntegration_DB_ProposalsWorkWhenRateLimited(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Configure with a generous MaxInMemLogSize (10MB) so proposals succeed.
	rateLimitCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbConfig.Config{
			ShardID:         shardID,
			ReplicaID:       replicaID,
			ElectionRTT:     10,
			HeartbeatRTT:    1,
			CheckQuorum:     true,
			MaxInMemLogSize: 10 * 1024 * 1024, // 10MB
		}
	}
	trackers := cluster.startShardWithConfig(1, rateLimitCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose 20 entries — should all succeed under generous limit.
	expected := proposeNEntries(t, cluster, trackers, 1, 20, "ratelimit")
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("20 proposals succeeded with MaxInMemLogSize=10MB")
}

// TestIntegration_DB_RateLimitCanBeTriggered verifies that when
// MaxInMemLogSize is set extremely low, proposals eventually return
// ErrSystemBusy. Matches dragonboat's TestRateLimitCanBeTriggered.
//
// The test uses async Propose (not SyncPropose) to fire proposals
// without waiting for each to complete. SyncPropose on a single-node
// cluster applies entries nearly instantly, so the in-memory log never
// accumulates enough to trigger rate limiting. Async proposals build
// up backpressure in the proposal pipeline, allowing the rate limiter
// to detect that the in-memory log size exceeds MaxInMemLogSize.
func TestIntegration_DB_RateLimitCanBeTriggered(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Configure with an extremely small MaxInMemLogSize (1 byte).
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbConfig.Config{
		ShardID:         1,
		ReplicaID:       1,
		ElectionRTT:     10,
		HeartbeatRTT:    1,
		CheckQuorum:     true,
		MaxInMemLogSize: 1, // 1 byte -- extremely aggressive
	}
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Wait for leader election.
	waitForCondition(t, leaderElectionTimeout, "single-node leader", func() bool {
		_, _, valid, lErr := nh.GetLeaderID(1)
		return lErr == nil && valid
	})

	// Fire async proposals rapidly to build backpressure. With
	// MaxInMemLogSize=1, the in-memory log should exceed the threshold
	// after a single entry, causing subsequent proposals to be rejected
	// with ErrSystemBusy.
	session := dbClient.NewNoOPSession(1, nil)
	payload := make([]byte, 512)
	for i := range len(payload) {
		payload[i] = byte(i % 256)
	}
	cmd := encodeKVPut("rl-key", string(payload))

	var gotBusy bool
	const maxAttempts = 500
	for i := range maxAttempts {
		// Use async Propose so proposals queue up without waiting for
		// each to be applied.
		rs, propErr := nh.Propose(session, cmd, defaultTimeout)
		if propErr != nil {
			if errors.Is(propErr, dragonboat.ErrSystemBusy) {
				gotBusy = true
				t.Logf("ErrSystemBusy triggered at iteration %d (from Propose)", i)
				break
			}
			// Other errors (e.g., timeout, not ready) may occur -- log and
			// continue to give the rate limiter time to react.
			t.Logf("proposal %d returned: %v", i, propErr)
			continue
		}

		// Also check the result channel for dropped proposals.
		select {
		case result := <-rs.ResultC():
			if result.Dropped() {
				gotBusy = true
				t.Logf("ErrSystemBusy triggered at iteration %d (proposal dropped)", i)
			}
			rs.Release()
		default:
			// Result not ready yet -- that's expected for async proposals.
			// Release will happen when the result is eventually consumed or
			// the host shuts down.
			rs.Release()
		}
		if gotBusy {
			break
		}
	}

	if !gotBusy {
		// If async proposals didn't trigger it, try one final synchronous
		// proposal as a fallback check.
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		_, syncErr := nh.SyncPropose(ctx, session, cmd)
		if syncErr != nil && errors.Is(syncErr, dragonboat.ErrSystemBusy) {
			gotBusy = true
			t.Logf("ErrSystemBusy triggered on final sync proposal")
		}
	}

	if !gotBusy {
		t.Fatal("rate limit was never triggered despite many proposals with MaxInMemLogSize=1; rate limiting is broken")
	}
}

// TestIntegration_DB_UpdateResultIsReturned verifies that SyncPropose
// returns the result value set by the state machine's Update method.
// Matches dragonboat's TestUpdateResultIsReturnedToCaller.
func TestIntegration_DB_UpdateResultIsReturned(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session := dbClient.NewNoOPSession(1, nil)

	// The KV SM's Update returns Result{Value: len(value)}.
	testCases := []struct {
		key, value string
		wantLen    uint64
	}{
		{"k1", "short", 5},
		{"k2", "a-longer-value-string", 21},
		{"k3", "", 0},
	}

	for _, tc := range testCases {
		cmd := encodeKVPut(tc.key, tc.value)
		result, err := leaderHost.SyncPropose(ctx, session, cmd)
		if err != nil {
			t.Fatalf("SyncPropose(%q) failed: %v", tc.key, err)
		}
		if result.Value != tc.wantLen {
			t.Errorf("SyncPropose(%q) result.Value = %d, want %d",
				tc.key, result.Value, tc.wantLen)
		}
	}
	t.Logf("update result values verified for %d proposals", len(testCases))
}
