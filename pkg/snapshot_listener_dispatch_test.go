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
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// TestSnapshotCallbacks_DispatchUserListeners: the snapshot callbacks are
// invoked INLINE on a snapshot-pool worker, and a recovery reports while
// holding its node's write lease, which UnloadNode waits out. The host used to
// call the user's EventListener from them directly, so a listener doing the
// natural thing — StopShard on a failed recovery — waited on its own lease
// forever. The fan-out is dispatched: the callback returns while the listener
// is still running.
func TestSnapshotCallbacks_DispatchUserListeners(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:        t.TempDir(),
		NodeHostDir:   t.TempDir(),
		RaftAddress:   "127.0.0.1:5000",
		ListenAddress: "127.0.0.1:5000",
		DeploymentID:  1,
	}
	cfg.SetDefaults()
	eng := engine.NewEngine(cfg, memdb.New(), &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	entered := make(chan string, 4)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	block := func(name string) {
		entered <- name
		<-release
	}
	cfg.EventListener = &config.EventListener{
		OnShardFailed:  func(config.ShardFailedInfo) { block("OnShardFailed") },
		OnLogCompacted: func(config.EventShardInfo) { block("OnLogCompacted") },
	}
	h := &Host{cfg: cfg, engine: eng}
	h.nodes.Store(uint64(7), &nodeState{replicaID: 1, pendingSnapshots: map[uint64]*RequestState{}})
	cb := &hostEngineCallback{h: h}

	for _, tt := range []struct {
		name string
		call func()
	}{
		{"OnShardFailed", func() { cb.OnSnapshotCompleted(7, 0, errors.New("recovery failed")) }},
		{"OnLogCompacted", func() {
			cb.OnLogCompacted(7, 1, engine.LogCompaction{Index: 100, Reader: true})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			returned := make(chan struct{})
			go func() {
				tt.call()
				close(returned)
			}()
			select {
			case <-returned:
			case <-time.After(5 * time.Second):
				t.Fatal("the callback ran the user listener inline: a listener calling " +
					"StopShard would deadlock the snapshot worker against its own lease")
			}
			select {
			case got := <-entered:
				if got != tt.name {
					t.Fatalf("listener %s ran, want %s", got, tt.name)
				}
				release <- struct{}{} // the dispatcher is FIFO: free it for the next case
			case <-time.After(5 * time.Second):
				t.Fatalf("%s was never delivered", tt.name)
			}
		})
	}
}

// flushListeners waits until every EventListener notification the callback's
// host has dispatched so far has been delivered. The dispatcher is FIFO, so a
// sentinel queued behind them runs only after they have; the channel close
// also orders the test's reads after the listener's writes. With no engine the
// host delivers inline and there is nothing to wait for.
func flushListeners(t *testing.T, cb *hostEngineCallback) {
	t.Helper()
	if cb.h.engine == nil {
		return
	}
	done := make(chan struct{})
	if !cb.h.engine.DispatchListener(func() { close(done) }) {
		t.Fatal("listener queue full: the flush sentinel was dropped")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("listener dispatcher did not drain within 5s")
	}
}
