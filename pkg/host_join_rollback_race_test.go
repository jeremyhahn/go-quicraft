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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
)

// errGatedBootstrapSave is the failure failingBootstrapGateLogDB returns.
var errGatedBootstrapSave = errors.New("gated bootstrap save failed")

// failingBootstrapGateLogDB holds SaveBootstrap until the test releases it,
// then fails the save without writing anything.
type failingBootstrapGateLogDB struct {
	logdb.LogDB
	entered chan struct{}
	release chan struct{}
}

func (db *failingBootstrapGateLogDB) SaveBootstrap(uint64, uint64, logdb.Bootstrap) error {
	close(db.entered)
	<-db.release
	return errGatedBootstrapSave
}

// closeCountingStateMachine counts the calls to its Close.
type closeCountingStateMachine struct {
	mockStateMachine
	closes *atomic.Int32
}

func (m *closeCountingStateMachine) Close(_ context.Context) error {
	m.closes.Add(1)
	return nil
}

// TestStartShard_JoinBootstrapFailure_StopShardOwnsTeardown holds a joining
// StartShard inside a SaveBootstrap that will fail, after its nodeState is
// published. A StopShard in that window claims teardown: it removes the node,
// decrements activeNodes and closes the state machine. The failed save must
// then leave teardown to its owner: no second decrement and no second Close.
func TestStartShard_JoinBootstrapFailure_StopShardOwnsTeardown(t *testing.T) {
	cfg := testHostConfig(t)
	db := &failingBootstrapGateLogDB{
		LogDB:   memdb.New(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer func() { _ = h.Close() }()

	var closes atomic.Int32
	create := func(_, _ uint64) interface{} {
		return &closeCountingStateMachine{closes: &closes}
	}

	startErr := make(chan error, 1)
	go func() {
		startErr <- h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, true,
			create, testShardConfig(1, 1))
	}()
	released := false
	defer func() {
		if !released {
			close(db.release)
		}
	}()

	select {
	case <-db.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("StartShard did not reach SaveBootstrap")
	}

	// The nodeState is published; StopShard claims and completes teardown.
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard of the published joining shard: %v", err)
	}
	if n := closes.Load(); n != 1 {
		t.Fatalf("StopShard closed the state machine %d times, want 1", n)
	}

	released = true
	close(db.release)
	select {
	case err := <-startErr:
		if err == nil {
			t.Fatal("StartShard succeeded with a failed bootstrap save")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("StartShard did not return")
	}

	if n := closes.Load(); n != 1 {
		t.Fatalf("state machine closed %d times, want 1: the failed save rolled back a teardown StopShard owned", n)
	}
	if n := h.activeNodes.Load(); n != 0 {
		t.Fatalf("activeNodes = %d after the failed join, want 0", n)
	}
	if n := h.ShardCount(); n != 0 {
		t.Fatalf("ShardCount = %d after the failed join, want 0", n)
	}
	if _, loaded := h.nodes.Load(uint64(1)); loaded {
		t.Fatal("nodeState left behind by the failed join")
	}
	if h.isShardJoining(1) {
		t.Fatal("join window left open by the failed join")
	}
}
