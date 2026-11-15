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

package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestNodeHost_RemoveCompact_Sequence pins the documented StopShard,
// RemoveData, RequestCompaction sequence: a wrong replica ID on a loaded
// shard returns ErrShardNotFound; the sequence succeeds once the shard is
// unloaded with an already-complete SysOpState; the removed replica cannot
// restart, on this NodeHost or on one reopened over the same directories.
func TestNodeHost_RemoveCompact_Sequence(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)
	nhConfig := testNodeHostConfig(t.TempDir())

	nh1, err := NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(), quicraft.WithoutEncryption(),
	)
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			nh1.Close()
		}
	})
	startTestShard(t, nh1, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session := nh1.GetNoOPSession(shardID)
	for i := 0; i < 20; i++ {
		if _, err := nh1.SyncPropose(ctx, session, []byte("remove-compact")); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}
	if _, err := nh1.SyncRequestSnapshot(ctx, shardID, DefaultSnapshotOption); err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}

	// (a) A replica ID that does not match the loaded replica.
	sysOp, err := nh1.RequestCompaction(shardID, replicaID+1)
	if !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("RequestCompaction(wrong replica) = %v, want ErrShardNotFound", err)
	}
	if sysOp != nil {
		t.Fatalf("RequestCompaction(wrong replica) returned a non-nil SysOpState")
	}

	// (b) StopShard, RemoveData, RequestCompaction in sequence succeed.
	if err := nh1.StopShard(shardID); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if err := nh1.RemoveData(shardID, replicaID); err != nil {
		t.Fatalf("RemoveData: %v", err)
	}
	sysOp, err = nh1.RequestCompaction(shardID, replicaID)
	if err != nil {
		t.Fatalf("RequestCompaction after RemoveData: %v", err)
	}
	if sysOp == nil {
		t.Fatal("RequestCompaction after RemoveData returned a nil SysOpState")
	}
	select {
	case <-sysOp.CompletedC():
	default:
		t.Error("RequestCompaction after RemoveData: CompletedC not already closed")
	}
	sysOp.Release()

	// (c) The removed replica cannot restart on this NodeHost.
	cfg := dbConfig.Config{
		ShardID: shardID, ReplicaID: replicaID,
		ElectionRTT: 5, HeartbeatRTT: 1, CheckQuorum: true,
	}
	members := map[uint64]Target{replicaID: nh1.RaftAddress()}
	if err := nh1.StartReplica(members, false, testSMFactory(), cfg); !errors.Is(err, ErrReplicaRemoved) {
		t.Fatalf("StartReplica after RemoveData = %v, want ErrReplicaRemoved", err)
	}
	if nh1.HasNodeInfo(shardID, replicaID) {
		t.Error("HasNodeInfo after RemoveData = true, want false")
	}
	nh1.Close()
	closed = true

	// (d) A NodeHost reopened on the same directories still refuses it.
	nh2 := newTestNodeHostWithConfig(t, nhConfig)
	members = map[uint64]Target{replicaID: nh2.RaftAddress()}
	if err := nh2.StartReplica(members, false, testSMFactory(), cfg); !errors.Is(err, ErrReplicaRemoved) {
		t.Fatalf("StartReplica on reopened NodeHost = %v, want ErrReplicaRemoved", err)
	}
	if nh2.HasNodeInfo(shardID, replicaID) {
		t.Error("HasNodeInfo on reopened NodeHost = true, want false")
	}
}
