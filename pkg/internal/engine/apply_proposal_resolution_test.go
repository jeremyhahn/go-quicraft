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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestApplyWorker_DiscardItemFailsProposals asserts the engine.Callback
// contract on the discard path: a committed batch dropped because the shard
// was unloaded (or the engine is stopping) never reaches a state machine, so
// nothing else can resolve its proposals. Without the callback every proposal
// in the batch stays registered until its context deadline, and a
// deadline-less caller blocks forever.
func TestApplyWorker_DiscardItemFailsProposals(t *testing.T) {
	applyC := make(chan *applyItem, 1)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	cfg := config.Config{ShardID: 9, ReplicaID: 2}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)
	node.commitPending.Store(true)

	item := applyPool.Get().(*applyItem)
	item.node = node
	item.update = proto.Update{
		ShardID:   9,
		ReplicaID: 2,
		CommittedEntries: []proto.Entry{
			{Index: 11, Term: 3, Cmd: []byte("a")},
			{Index: 12, Term: 3, Cmd: []byte("b")},
		},
	}

	aw.discardItem(item)

	calls := cb.getProposalFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("OnProposalFailed called %d times, want 1", len(calls))
	}
	if calls[0].shardID != 9 {
		t.Errorf("OnProposalFailed shardID = %d, want 9", calls[0].shardID)
	}
	if len(calls[0].entries) != 2 {
		t.Errorf("OnProposalFailed reported %d entries, want 2", len(calls[0].entries))
	}
	if !errors.Is(calls[0].err, errApplyItemDiscarded) {
		t.Errorf("OnProposalFailed err = %v, want errApplyItemDiscarded", calls[0].err)
	}
	var applyErr *ApplyError
	if !errors.As(calls[0].err, &applyErr) {
		t.Fatalf("OnProposalFailed err = %T, want *ApplyError", calls[0].err)
	}
	if applyErr.ShardID != 9 || applyErr.ReplicaID != 2 {
		t.Errorf("ApplyError identity = shard %d replica %d, want 9/2",
			applyErr.ShardID, applyErr.ReplicaID)
	}
	if node.commitPending.Load() {
		t.Error("discardItem left commitPending set")
	}
}

// TestApplyWorker_SMPanicFailsProposals asserts the panic recovery resolves
// the batch's proposals too. The panic aborts the apply, so no OnApplied ever
// follows: a caller with a deadline-less context would otherwise block until
// StopShard/Close and the host's pendingOps would never drop, making every
// Drain time out.
func TestApplyWorker_SMPanicFailsProposals(t *testing.T) {
	applyC := make(chan *applyItem, 64)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	var errorReported atomic.Value
	listener := &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) { errorReported.Store(info.Err) },
	}
	cfg := config.Config{ShardID: 3, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, &panicApplier{panicValue: "boom"}, nil, cfg, nil, "", listener)

	go aw.run()

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.node = node
	item.update = proto.Update{
		ShardID:          3,
		ReplicaID:        1,
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("trigger-panic")}},
	}
	applyC <- item

	deadline := time.After(5 * time.Second)
	for errorReported.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for panic recovery")
		default:
		}
		time.Sleep(time.Millisecond)
	}

	calls := cb.getProposalFailedCalls()
	if len(calls) != 1 {
		t.Fatalf("OnProposalFailed called %d times after an SM panic, want 1", len(calls))
	}
	if len(calls[0].entries) != 1 {
		t.Errorf("OnProposalFailed reported %d entries, want 1", len(calls[0].entries))
	}
	var smPanic *SMPanicError
	if !errors.As(calls[0].err, &smPanic) {
		t.Fatalf("OnProposalFailed err = %T (%v), want *SMPanicError", calls[0].err, calls[0].err)
	}
	if smPanic.ShardID != 3 || smPanic.ReplicaID != 1 {
		t.Errorf("SMPanicError identity = shard %d replica %d, want 3/1",
			smPanic.ShardID, smPanic.ReplicaID)
	}
}
