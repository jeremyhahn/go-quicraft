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

package engine

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// newUnreadableLogPeer builds a raft.Peer whose log claims committed entries
// 1..3 that the LogReader cannot serve: the reader has no LogDB and no entry
// cache, so the committed-entry read inside GetUpdate fails with
// logdb.ErrUnavailable. It is the shape of a LogDB that lost or cannot read
// entries its hard state says are committed.
func newUnreadableLogPeer(t *testing.T, shardID, replicaID uint64) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	lr := logdb.NewLogReader(shardID, replicaID, nil)
	if err := lr.ApplySnapshot(proto.Snapshot{
		Membership: proto.Membership{Addresses: map[uint64]string{replicaID: "localhost:0"}},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if err := lr.SetRange(1, 3); err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	lr.SetState(proto.State{Term: 1, Commit: 3})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if _, err := peer.GetUpdate(true, 0); !errors.Is(err, logdb.ErrUnavailable) {
		t.Fatalf("fixture: GetUpdate error = %v, want logdb.ErrUnavailable", err)
	}
	return peer, lr
}

// TestStepWorker_GetUpdateErrorLatchesShard: a GetUpdate error leaves the Peer
// in a state the engine cannot reason about — the committed entries it owes the
// state machine cannot be read — so the shard fails closed exactly like an
// apply or recovery failure. The latch trips, OnShardFailed fires once (engine
// callback and EventListener alike) with the log's own error, later cycles skip
// the shard instead of reporting again, and another shard on the same step
// worker keeps running.
func TestStepWorker_GetUpdateErrorLatchesShard(t *testing.T) {
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, newTestSender(), ws, commitC, cb, stopC, nil)

	var listenerFired atomic.Int64
	var listenerErr atomic.Value
	listener := &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			listenerFired.Add(1)
			listenerErr.Store(info.Err)
		},
	}

	badPeer, badLR := newUnreadableLogPeer(t, 1, 1)
	badCfg := config.Config{ShardID: 1, ReplicaID: 1}
	badCfg.SetDefaults()
	bad := NewNode(badPeer, nil, badLR, badCfg, nil, "", listener)
	sw.loadNode(bad)

	goodPeer, goodLR := newTestPeer(t, 2, 1)
	goodCfg := config.Config{ShardID: 2, ReplicaID: 1}
	goodCfg.SetDefaults()
	good := NewNode(goodPeer, newTestApplier(newTestSM()), goodLR, goodCfg, nil, "", nil)
	sw.loadNode(good)
	tickPeerToLeader(goodPeer)

	for range 3 {
		ws.Notify(1)
		sw.processReady()
	}

	if !bad.IsFailed() {
		t.Fatal("a GetUpdate error did not trip the shard's failure latch")
	}
	calls := cb.getShardFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("engine OnShardFailed called %d times over three cycles, want 1", len(calls))
	}
	if calls[0].shardID != 1 || !errors.Is(calls[0].err, logdb.ErrUnavailable) {
		t.Fatalf("engine OnShardFailed(%d, %v), want (1, logdb.ErrUnavailable)", calls[0].shardID, calls[0].err)
	}
	if n := listenerFired.Load(); n != 1 {
		t.Fatalf("EventListener.OnShardFailed fired %d times, want 1", n)
	}
	if err, _ := listenerErr.Load().(error); !errors.Is(err, logdb.ErrUnavailable) {
		t.Fatalf("EventListener.OnShardFailed err = %v, want logdb.ErrUnavailable", err)
	}

	if good.IsFailed() {
		t.Fatal("the healthy shard on the same step worker was failed")
	}
	if _, err := goodPeer.Propose([]proto.Entry{{Cmd: []byte("still serving")}}); err != nil {
		t.Fatalf("propose on the healthy shard: %v", err)
	}
	ws.Notify(2)
	sw.processReady()
	select {
	case item := <-commitC:
		if item.node != good {
			t.Fatalf("commit item for shard %d, want the healthy shard 2", item.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("the healthy shard produced no update after its neighbour failed")
	}
}
