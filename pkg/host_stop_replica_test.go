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

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestStopReplica_StopsLikeStopShardAndRestartsWithState pins the documented
// StopReplica contract (docs/multi-group-host/api.md, Replica management;
// docs/membership-changes/api.md, local destructive replica operations) on a
// real Host with a real WAL:
//
//   - a replica ID other than the loaded one returns *ReplicaNotFoundError and
//     leaves the shard running;
//   - the loaded replica ID stops the shard exactly like StopShard, keeps the
//     replica's bootstrap record (HasNodeInfo), and a second stop returns
//     ErrShardNotFound;
//   - StartShard of the same replica on the same Host recovers its state from
//     the log;
//   - a closed Host returns ErrClosed.
func TestStopReplica_StopsLikeStopShardAndRestartsWithState(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = h.Close()
		}
	}()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, hashingCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	propose := func(label string, i int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("set:%s%d=%d", label, i, i)))
		return err
	}
	for i := 0; i < 20; i++ {
		if err := propose("k", i); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}

	// (a) A wrong replica ID is refused and the shard keeps serving.
	err = h.StopReplica(1, 2)
	var rnf *ReplicaNotFoundError
	if !errors.As(err, &rnf) {
		t.Fatalf("StopReplica(1, 2): got %v (%T), want *ReplicaNotFoundError", err, err)
	}
	if rnf.ShardID != 1 || rnf.ReplicaID != 2 {
		t.Fatalf("StopReplica(1, 2): error names shard %d replica %d, want shard 1 replica 2",
			rnf.ShardID, rnf.ReplicaID)
	}
	if !errors.Is(err, ErrReplicaNotFound) {
		t.Fatalf("StopReplica(1, 2): %v does not match ErrReplicaNotFound", err)
	}
	if err := propose("after-wrong", 0); err != nil {
		t.Fatalf("SyncPropose after StopReplica with a wrong replica ID: %v; "+
			"the refused stop must leave the shard running", err)
	}
	info, err := h.ShardInfo(1)
	if err != nil {
		t.Fatalf("ShardInfo(1) after a refused StopReplica: %v", err)
	}
	if info.ShardID != 1 || info.ReplicaID != 1 {
		t.Fatalf("ShardInfo(1) after a refused StopReplica: shard %d replica %d, want shard 1 replica 1",
			info.ShardID, info.ReplicaID)
	}

	// The last acknowledged write's state, to compare after the restart.
	wantHash, wantIdx, ok, err := h.StateMachineHash(1)
	if err != nil || !ok {
		t.Fatalf("StateMachineHash before stop: ok=%v err=%v", ok, err)
	}
	if wantIdx == 0 {
		t.Fatal("StateMachineHash before stop reported applied index 0 after 21 writes")
	}

	// (b) The loaded replica ID stops the shard like StopShard.
	if err := h.StopReplica(1, 1); err != nil {
		t.Fatalf("StopReplica(1, 1): %v", err)
	}
	if err := propose("after-stop", 0); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("SyncPropose after StopReplica: got %v, want ErrShardNotFound", err)
	}
	if err := h.StopReplica(1, 1); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("second StopReplica(1, 1): got %v, want ErrShardNotFound", err)
	}
	if !h.HasNodeInfo(1, 1) {
		t.Fatal("HasNodeInfo(1, 1) is false after StopReplica: stopping must not " +
			"remove the replica's bootstrap record")
	}

	// (c) Restarting the same replica on the same Host recovers its state.
	if err := h.StartShard(members, false, hashingCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard after StopReplica: %v", err)
	}
	waitForLeader(t, h, 1)
	gotHash, gotIdx, ok, err := h.StateMachineHash(1)
	if err != nil || !ok {
		t.Fatalf("StateMachineHash after restart: ok=%v err=%v", ok, err)
	}
	if gotIdx < wantIdx {
		t.Fatalf("applied index after restart %d < %d, the last acknowledged write's index",
			gotIdx, wantIdx)
	}
	if gotHash != wantHash {
		t.Fatalf("state hash after restart %d != %d before the stop: the restarted "+
			"replica did not recover its state", gotHash, wantHash)
	}
	if err := propose("after-restart", 0); err != nil {
		t.Fatalf("SyncPropose after restart: %v", err)
	}

	// (d) A closed Host returns ErrClosed.
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
	if err := h.StopReplica(1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("StopReplica after Close: got %v, want ErrClosed", err)
	}
}
