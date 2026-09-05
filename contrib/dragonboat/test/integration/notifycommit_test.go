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
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_NotifyCommit_ProposalCommitted verifies that when
// NotifyCommit is enabled on the NodeHost, an async Propose delivers a
// Committed notification followed by a Completed notification, both on
// the ResultC aggregate channel. This validates the two-phase
// notification pipeline: WAL-durable commit then state machine apply.
func TestIntegration_DB_NotifyCommit_ProposalCommitted(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)
	nhConfig.NotifyCommit = true

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

	// Wait for leader election on the single-node shard.
	waitForCondition(t, leaderElectionTimeout, "single-node leader", func() bool {
		leaderID, _, valid, leaderErr := nh.GetLeaderID(1)
		return leaderErr == nil && valid && leaderID == 1
	})
	t.Logf("single-node leader elected")

	// Submit an async proposal with NotifyCommit enabled.
	session := dbClient.NewNoOPSession(1, nil)
	cmd := encodeKVPut("nc-key", "nc-val")
	//quicraft:direct-propose standalone single-voter shard: nh is the only replica
	// in the configuration, so leadership has nowhere to move, and the
	// NotifyCommit wiring on THIS host is the subject.
	rs, proposeErr := nh.Propose(session, cmd, defaultTimeout)
	if proposeErr != nil {
		t.Fatalf("Propose failed: %v", proposeErr)
	}
	defer rs.Release()

	// ResultC must be non-nil when NotifyCommit is enabled, and its first
	// delivery must be the committed notification (WAL-durable).
	resultCh := rs.ResultC()
	if resultCh == nil {
		t.Fatal("ResultC() returned nil; expected non-nil channel when NotifyCommit is enabled")
	}

	select {
	case committedResult := <-resultCh:
		if !committedResult.Committed() {
			t.Fatalf("expected Committed() == true, got code %v", committedResult)
		}
		t.Logf("received committed notification: Committed()=%v", committedResult.Committed())
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for committed notification on ResultC")
	}

	// Wait for the completed notification (state machine applied).
	select {
	case completedResult := <-resultCh:
		if !completedResult.Completed() {
			t.Fatalf("expected Completed() == true, got code %v", completedResult)
		}
		smResult := completedResult.GetResult()
		expectedLen := uint64(len("nc-val"))
		if smResult.Value != expectedLen {
			t.Errorf("result value = %d, want %d", smResult.Value, expectedLen)
		}
		t.Logf("received completed notification: Completed()=%v, result.Value=%d",
			completedResult.Completed(), smResult.Value)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for completed notification on ResultC")
	}
}

// TestIntegration_DB_NotifyCommit_Disabled verifies that when NotifyCommit
// is NOT enabled (default), ResultC is simply the applied channel — no
// separate committed notification is ever delivered — and proposals still
// complete normally. This ensures the disabled path does not produce a
// spurious commit notification or block the apply pipeline.
func TestIntegration_DB_NotifyCommit_Disabled(t *testing.T) {
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, mtls)
	// NotifyCommit defaults to false; do not set it.

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

	// Wait for leader election on the single-node shard.
	waitForCondition(t, leaderElectionTimeout, "single-node leader", func() bool {
		leaderID, _, valid, leaderErr := nh.GetLeaderID(1)
		return leaderErr == nil && valid && leaderID == 1
	})
	t.Logf("single-node leader elected")

	// Submit an async proposal without NotifyCommit.
	session := dbClient.NewNoOPSession(1, nil)
	cmd := encodeKVPut("disabled-key", "disabled-val")
	//quicraft:direct-propose standalone single-voter shard: nh is the only replica
	// in the configuration, and the NotifyCommit-disabled wiring on THIS host is
	// the subject.
	rs, proposeErr := nh.Propose(session, cmd, defaultTimeout)
	if proposeErr != nil {
		t.Fatalf("Propose failed: %v", proposeErr)
	}
	defer rs.Release()

	// When NotifyCommit is disabled, ResultC must be exactly the applied
	// channel — no separate committed notification is ever delivered.
	if rs.ResultC() != rs.AppliedC() {
		t.Fatal("ResultC() must equal AppliedC() when NotifyCommit is disabled")
	}

	// Wait for the proposal to complete via ResultC.
	select {
	case completedResult := <-rs.ResultC():
		if !completedResult.Completed() {
			t.Fatalf("expected Completed() == true, got code %v", completedResult)
		}
		smResult := completedResult.GetResult()
		expectedLen := uint64(len("disabled-val"))
		if smResult.Value != expectedLen {
			t.Errorf("result value = %d, want %d", smResult.Value, expectedLen)
		}
		t.Logf("received completed notification: Completed()=%v, result.Value=%d",
			completedResult.Completed(), smResult.Value)
	case <-time.After(defaultTimeout):
		t.Fatal("timed out waiting for completed notification on ResultC")
	}
}

// compile-time assertion that dbConfig.NodeHostConfig has a NotifyCommit field.
var _ = dbConfig.NodeHostConfig{NotifyCommit: true}
