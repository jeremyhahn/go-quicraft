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
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestStartShard_RefusesAnElectionTimeoutThatOverflows verifies StartShard
// refuses a shard whose ElectionRTT, in this host's RTTMillisecond ticks, is
// not representable as a time.Duration. Each value passes its own bound, but
// the product wrapped negative and left a LeaseRead replica unable to measure
// the end of its post-restart vote hold-down, so it refused every vote and
// campaign for good.
func TestStartShard_RefusesAnElectionTimeoutThatOverflows(t *testing.T) {
	cfg := testHostConfig(t)
	cfg.RTTMillisecond = 60_000
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	scfg := testShardConfig(1, 1)
	scfg.ElectionRTT = 200_000_000 // * 60s overflows int64 nanoseconds
	err = h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg)
	var verr *config.ValidationError
	if !errors.As(err, &verr) || verr.Field != "ElectionRTT" {
		t.Fatalf("StartShard = %v, want a *config.ValidationError on ElectionRTT", err)
	}
}

// TestStartShard_LeaseReadRecordsAndReleasesTheVoteWindow verifies, through
// the engine and the LogDB, that a LeaseRead replica persists the vote window
// it promises, and that after a restart into a configuration that promises
// nothing it honours the recorded window and then lowers the record.
func TestStartShard_LeaseReadRecordsAndReleasesTheVoteWindow(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	members := map[uint64]string{1: "127.0.0.1:5000"}

	scfg := testShardConfig(1, 1)
	scfg.LeaseRead = true
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard(LeaseRead): %v", err)
	}
	waitForLeader(t, h, 1)
	want := uint64(time.Duration(scfg.ElectionRTT*cfg.RTTMillisecond) * time.Millisecond)
	voteWindow := func() uint64 {
		st, _, err := h.logdb.ReadState(1, 1)
		if err != nil {
			t.Fatalf("ReadState: %v", err)
		}
		return st.VoteWindow
	}
	if !waitUntil(2*time.Second, func() bool { return voteWindow() == want }) {
		t.Fatalf("persisted vote window = %d, want the promised %d", voteWindow(), want)
	}

	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	scfg.LeaseRead = false
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard(without LeaseRead): %v", err)
	}
	waitForLeader(t, h, 1)
	if !waitUntil(2*time.Second, func() bool { return voteWindow() == 0 }) {
		t.Fatalf("persisted vote window = %d after the owed window passed, want 0", voteWindow())
	}
}

// TestRecoverLogReader_RecoversTheOwedVoteWindow verifies the restart path
// hands raft the vote window the replica recorded, with the rest of its hard
// state. Dropping it here would bring every replica back owing nothing, and a
// replica restarted into a configuration that promises less would vote while
// a leader still counts its pre-restart acknowledgement.
func TestRecoverLogReader_RecoversTheOwedVoteWindow(t *testing.T) {
	db := memdb.New()
	saved := logdb.State{Term: 3, Vote: 1, Commit: 0, VoteWindow: uint64(2 * time.Second)}
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, State: saved}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	lr := logdb.NewLogReader(1, 1, db)
	if _, err := h.recoverLogReader(lr, 1, 1); err != nil {
		t.Fatalf("recoverLogReader: %v", err)
	}
	if st, _ := lr.NodeState(); st.VoteWindow != saved.VoteWindow {
		t.Fatalf("recovered vote window = %d, want the recorded %d", st.VoteWindow, saved.VoteWindow)
	}
}

// TestRemoveData_RetiresTheReplicaID verifies RemoveData retires the replica
// ID on this host: StartShard refuses it from then on, because the removed
// data held its term, vote and owed vote window, and a replica started again
// without them could vote twice in one term or inside a lease its old ack
// backs. The shard can be rejoined under a new replica ID.
func TestRemoveData_RetiresTheReplicaID(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if err := h.RemoveData(1, 1); err != nil {
		t.Fatalf("RemoveData: %v", err)
	}

	err = h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	var retired *ReplicaRetiredError
	if !errors.As(err, &retired) || !errors.Is(err, ErrReplicaRetired) {
		t.Fatalf("StartShard of the removed replica ID = %v, want *ReplicaRetiredError", err)
	}
	if retired.ShardID != 1 || retired.ReplicaID != 1 {
		t.Fatalf("ReplicaRetiredError names shard %d replica %d, want 1/1", retired.ShardID, retired.ReplicaID)
	}

	if err := h.StartShard(map[uint64]string{2: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(1, 2)); err != nil {
		t.Fatalf("StartShard under a new replica ID: %v", err)
	}
}

// TestRemoveData_DuringAStartIsRefused verifies RemoveData cannot interleave
// with a start of the same shard: while a StartShard holds the shard's import
// lock — past its retirement check, before its node is published — RemoveData
// is refused as transient and retires nothing. Retiring the ID then would let
// the start run a replica whose term, vote and owed vote window are gone.
func TestRemoveData_DuringAStartIsRefused(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	// Stand in for a StartShard mid-way: the claim and the lock it holds.
	h.startingShards.Store(uint64(1), struct{}{})
	release, idle := h.tryAcquireImportLock(1)
	if !idle {
		t.Fatal("fixture: import lock already held")
	}
	err = h.RemoveData(1, 1)
	release()
	h.startingShards.Delete(uint64(1))
	if !errors.Is(err, ErrShardNotReady) {
		t.Fatalf("RemoveData during a start = %v, want ErrShardNotReady", err)
	}
	if retired, _ := h.logdb.NodeRemoved(1, 1); retired {
		t.Fatal("a refused RemoveData retired the replica anyway")
	}
}

// TestRemoveData_RacingStartShardNeverBothSucceed runs StartShard and
// RemoveData for the same replica concurrently: at most one may succeed. Both
// succeeding means the replica was retired and started anyway.
func TestRemoveData_RacingStartShardNeverBothSucceed(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	for shard := uint64(1); shard <= 50; shard++ {
		var startErr, removeErr error
		done := make(chan struct{})
		go func() {
			defer close(done)
			startErr = h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(shard, 1))
		}()
		removeErr = h.RemoveData(shard, 1)
		<-done
		if startErr == nil && removeErr == nil {
			t.Fatalf("shard %d: StartShard and RemoveData of replica 1 both succeeded", shard)
		}
		if startErr == nil {
			if err := h.StopShard(shard); err != nil {
				t.Fatalf("StopShard(%d): %v", shard, err)
			}
		}
	}
}
