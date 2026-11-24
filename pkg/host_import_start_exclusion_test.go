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
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// These pin the mutual exclusion between ImportSnapshot and StartShard. An
// import checked that the shard was stopped BEFORE taking the shard's import
// lock, and StartShard only TRIED that lock for its staging sweep and carried
// on without it — so a start could build a live node from the old log while an
// import recorded a snapshot and compacted that log underneath it.

// TestStartShard_RefusedWhileAnImportIsInProgress: the start holds the import
// lock for its whole length, so while an import holds it the start is refused
// with the transient ErrImportInProgress and loads nothing.
func TestStartShard_RefusedWhileAnImportIsInProgress(t *testing.T) {
	f := newImportFixture(t)
	release, err := f.host.acquireImportLock(context.Background(), f.shardID)
	if err != nil {
		t.Fatalf("acquireImportLock: %v", err)
	}
	defer release()

	members := map[uint64]string{f.replicaID: "127.0.0.1:5000"}
	err = f.host.StartShard(members, false, testCreateFunc, testShardConfig(f.shardID, f.replicaID))
	if !errors.Is(err, ErrImportInProgress) {
		t.Fatalf("StartShard during an import = %v, want ErrImportInProgress", err)
	}
	if _, loaded := f.host.nodes.Load(f.shardID); loaded {
		t.Fatal("a start refused for an import in progress loaded the shard anyway")
	}
}

// TestStartShard_ConcurrentDuplicateIsNotReady: the start holds the import
// lock for its whole length, so a second concurrent start of the same shard
// must not be told an import is in progress. It is told the transient
// ErrShardNotReady — not ErrShardAlreadyExists, since the first start may yet
// fail and a caller reading "exists" as "running" would never start the shard.
func TestStartShard_ConcurrentDuplicateIsNotReady(t *testing.T) {
	f := newImportFixture(t)
	f.host.startingShards.Store(f.shardID, struct{}{}) // a start owns the shard
	defer f.host.startingShards.Delete(f.shardID)

	members := map[uint64]string{f.replicaID: "127.0.0.1:5000"}
	err := f.host.StartShard(members, false, testCreateFunc, testShardConfig(f.shardID, f.replicaID))
	if !errors.Is(err, ErrShardNotReady) {
		t.Fatalf("a concurrent duplicate start = %v, want the transient ErrShardNotReady", err)
	}
	if errors.Is(err, ErrImportInProgress) || errors.Is(err, ErrShardAlreadyExists) {
		t.Fatalf("a start in progress was misreported as %v", err)
	}
}

// TestStartShard_DuplicateOfAPublishedButUnfinishedStartIsNotReady: the
// first start publishes its node before its bootstrap save, and a failed save
// withdraws it. A second start in that window found the node and was told
// ErrShardAlreadyExists — a caller reading that as "running" would never
// retry, and the shard would stay stopped. While the first start holds its
// claim, the answer is the transient ErrShardNotReady.
func TestStartShard_DuplicateOfAPublishedButUnfinishedStartIsNotReady(t *testing.T) {
	f := newImportFixture(t)
	f.host.startingShards.Store(f.shardID, struct{}{})
	f.host.nodes.Store(f.shardID, &nodeState{replicaID: f.replicaID})
	defer f.host.startingShards.Delete(f.shardID)
	defer f.host.nodes.Delete(f.shardID)

	members := map[uint64]string{f.replicaID: "127.0.0.1:5000"}
	err := f.host.StartShard(members, false, testCreateFunc, testShardConfig(f.shardID, f.replicaID))
	if !errors.Is(err, ErrShardNotReady) {
		t.Fatalf("a start racing a published-but-unfinished start = %v, want ErrShardNotReady", err)
	}

	// Once that start has finished (its claim released), the node is running.
	f.host.startingShards.Delete(f.shardID)
	err = f.host.StartShard(members, false, testCreateFunc, testShardConfig(f.shardID, f.replicaID))
	if !errors.Is(err, ErrShardAlreadyExists) {
		t.Fatalf("a start of a running shard = %v, want ErrShardAlreadyExists", err)
	}
}

// TestImportSnapshot_RefusesAShardWithSeveralReplicas: the replica an import
// writes under was "the first" LogDB record for the shard, and ListNodeInfo is
// built from a map — so a shard restarted under a new replica without removing
// the old one's data imported under either, at random, and the locked
// re-check then failed at random too. The ambiguity is refused, every time.
func TestImportSnapshot_RefusesAShardWithSeveralReplicas(t *testing.T) {
	f := newImportFixture(t)
	f.logdb.mu.Lock()
	f.logdb.nodeInfos = append(f.logdb.nodeInfos, logdb.NodeInfo{ShardID: f.shardID, ReplicaID: f.replicaID + 1})
	f.logdb.mu.Unlock()

	for range 5 {
		artifact := buildSnapshotArtifact(t, 90, 7, 0, proto.Membership{}, []byte("restore"))
		err := f.host.ImportSnapshot(context.Background(), f.shardID, bytes.NewReader(artifact), WithImportDataDir(""))
		if !errors.Is(err, ErrInvalidOperation) {
			t.Fatalf("import into a shard with two replicas on this host = %v, want ErrInvalidOperation", err)
		}
	}
	f.assertNoImportedSnapshot(t)
}

// TestImportSnapshot_RechecksTheShardIsStoppedUnderTheLock: an import that
// passed its stopped-shard preflight and then waited for the lock must not
// proceed over a shard that was started in between.
func TestImportSnapshot_RechecksTheShardIsStoppedUnderTheLock(t *testing.T) {
	f := newImportFixture(t)
	release, err := f.host.acquireImportLock(context.Background(), f.shardID)
	if err != nil {
		t.Fatalf("acquireImportLock: %v", err)
	}

	artifact := buildSnapshotArtifact(t, 90, 7, 0, proto.Membership{}, []byte("restore"))
	done := make(chan error, 1)
	go func() {
		done <- f.host.ImportSnapshot(context.Background(), f.shardID, bytes.NewReader(artifact), WithImportDataDir(""))
	}()
	// Wait for the import to pass its preflight and park on the lock.
	waitForShardLockRefs(t, &f.host.importLocks, f.shardID, 2)
	select {
	case err := <-done:
		release()
		t.Fatalf("the import returned (%v) instead of waiting for the lock", err)
	default:
	}

	// The shard starts while the import waits: registered as running, as a
	// completed StartShard leaves it.
	f.host.nodes.Store(f.shardID, &nodeState{replicaID: f.replicaID})
	defer f.host.nodes.Delete(f.shardID)
	release()

	select {
	case err := <-done:
		if !errors.Is(err, ErrInvalidOperation) {
			t.Fatalf("the import over a started shard = %v, want ErrInvalidOperation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the import never returned")
	}
	f.assertNoImportedSnapshot(t)
}
