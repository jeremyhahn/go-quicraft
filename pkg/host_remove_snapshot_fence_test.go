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
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestRemoveData_RefusedUntilStoppingReplicaUnloads holds the engine node's
// write lease, as a snapshot save does while it publishes, and stops the
// shard. StopShard has claimed teardown (ns.stopped is set) but is held in
// UnloadNode behind the lease. A RemoveData in that window must be refused:
// the lease holder can still publish into the snapshot directory, and a
// removal that deleted the directory and reported success would leave the
// published snapshot behind. Once StopShard has fully unloaded the replica,
// RemoveData succeeds and nothing is left in the directory.
func TestRemoveData_RefusedUntilStoppingReplicaUnloads(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithLogDB(memdb.New()), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer func() { _ = h.Close() }()

	create := func(_, _ uint64) interface{} { return &mockStateMachine{} }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	v, ok := h.nodes.Load(uint64(1))
	if !ok {
		t.Fatal("shard not published")
	}
	ns := v.(*nodeState)
	node := h.engine.GetNode(1)
	if node == nil {
		t.Fatal("engine holds no node for the started shard")
	}
	snapDir, _, err := h.removeDataSnapshotDir(1, 1)
	if err != nil {
		t.Fatalf("removeDataSnapshotDir: %v", err)
	}

	// The snapshot writer: holds the lease UnloadNode waits on.
	if !node.AcquireWriteLease() {
		t.Fatal("write lease refused on a running node")
	}
	leased := true
	defer func() {
		if leased {
			node.ReleaseWriteLease()
		}
	}()

	stopErr := make(chan error, 1)
	go func() { stopErr <- h.StopShard(1) }()

	// Gate on StopShard's claim: ns.stopped is set before UnloadNode, which
	// cannot return while the lease is held.
	deadline := time.Now().Add(30 * time.Second)
	for !ns.stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("StopShard did not claim teardown")
		}
		runtime.Gosched()
	}

	if err := h.RemoveData(1, 1); !errors.Is(err, ErrShardNotStopped) {
		t.Fatalf("RemoveData while the stopping replica's snapshot writer runs = %v, want ErrShardNotStopped", err)
	}

	// The writer publishes, then releases the lease; the unload completes.
	published := filepath.Join(snapDir, "snapshot-published")
	if err := os.MkdirAll(published, 0o750); err != nil {
		t.Fatalf("publish: %v", err)
	}
	leased = false
	node.ReleaseWriteLease()
	select {
	case err := <-stopErr:
		if err != nil {
			t.Fatalf("StopShard: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("StopShard did not return after the lease was released")
	}

	if err := h.RemoveData(1, 1); err != nil {
		t.Fatalf("RemoveData after the replica unloaded: %v", err)
	}
	if _, err := os.Stat(published); !os.IsNotExist(err) {
		t.Fatalf("snapshot published by the stopping replica survived a successful RemoveData (stat err %v)", err)
	}
}
