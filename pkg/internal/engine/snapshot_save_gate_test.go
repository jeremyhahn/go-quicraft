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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// gateProbeSM is an Applier that flags any Apply that executes while a snapshot
// is in its consistent-capture window. In the real pipeline that window reads
// shared in-memory state (the session manager, which is not safe for concurrent
// use) — an overlapping Apply is exactly the C2 bug: a fatal "concurrent map
// iteration and map write" that crashes the whole host.
type gateProbeSM struct {
	capturing  atomic.Bool
	violations atomic.Int64
	applies    atomic.Int64
}

func (s *gateProbeSM) Apply(entries []proto.Entry, results []sm.Result) error {
	if s.capturing.Load() {
		s.violations.Add(1)
	}
	// Touch a little shared state to widen any overlap with the capture window.
	for i := range entries {
		results[i] = sm.Result{}
		runtime.Gosched()
	}
	s.applies.Add(1)
	return nil
}

// TestSavingGateQuiescesApplies is the C2 regression test. It drives real
// applies through the apply worker while a second goroutine repeatedly runs the
// exact gate sequence snapshotPool.saveSnapshot uses around the consistent
// capture — SetSaving(true) -> WaitForApplyDrain -> capture window ->
// SetSaving(false). The apply worker waits out the saving gate, and the drain
// guarantees no apply is mid-flight when the capture begins, so no Apply may
// execute during the capture window. A non-zero violation count means the gate
// failed to quiesce applies — the regression that crashes the host.
func TestSavingGateQuiescesApplies(t *testing.T) {
	applyC := make(chan *applyItem, 256)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	probe := &gateProbeSM{}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, probe, nil, cfg, 10, nil, "", nil)

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Saver: run the C2 gate sequence repeatedly. During the capture window no
	// Apply may run.
	const saves = 250
	saverDone := make(chan struct{})
	go func() {
		defer close(saverDone)
		for i := 0; i < saves; i++ {
			node.SetSaving(true)
			if !node.WaitForApplyDrain(stopC) {
				node.SetSaving(false)
				return
			}
			// Consistent-capture window: applies must be quiesced here.
			probe.capturing.Store(true)
			for j := 0; j < 8; j++ {
				runtime.Gosched()
			}
			probe.capturing.Store(false)
			// Release the gate (mirrors the afterCapture callback) so applies
			// resume, then yield so the applier makes progress before the next
			// save.
			node.SetSaving(false)
			runtime.Gosched()
		}
	}()

	// Applier: feed committed entries continuously while saves run. Applies
	// that arrive during a capture window block until the gate clears; the
	// invariant under test is that no Apply overlaps a capture window.
	idx := uint64(0)
	feeding := true
	for feeding {
		select {
		case <-saverDone:
			feeding = false
		default:
			idx++
			node.commitPending.Store(true)
			item := applyPool.Get().(*applyItem)
			item.update = proto.Update{
				ShardID:          1,
				CommittedEntries: []proto.Entry{{Term: 1, Index: idx, Cmd: []byte("c")}},
			}
			item.node = node
			applyC <- item
			// Pace the feeder to the single per-shard apply goroutine so the
			// dispatch channel does not sit permanently full (avoids backpressure
			// log spam) while still interleaving applies with the saves.
			runtime.Gosched()
		}
	}
	<-saverDone

	close(stopC)
	<-runDone
	wg.Wait()

	if v := probe.violations.Load(); v != 0 {
		t.Fatalf("Apply ran during the snapshot capture window %d times (of %d applies): "+
			"the saving gate failed to quiesce applies — this is the C2 concurrent-map crash",
			v, probe.applies.Load())
	}
	if probe.applies.Load() == 0 {
		t.Fatal("no applies executed; test did not exercise the apply path")
	}
}

// TestSavingGateAndDrainConsistency is a focused check that WaitForApplyDrain
// observes an apply that passed the gate just before SetSaving, and that a
// snapshot index/term captured after the drain reflects that apply.
func TestSavingGateAndDrainConsistency(t *testing.T) {
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	probe := &gateProbeSM{}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, probe, nil, cfg, 10, nil, "", nil)

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Apply a batch whose last entry is (index=7, term=3).
	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{
		ShardID: 1,
		CommittedEntries: []proto.Entry{
			{Term: 2, Index: 6, Cmd: []byte("a")},
			{Term: 3, Index: 7, Cmd: []byte("b")},
		},
	}
	item.node = node
	applyC <- item

	waitFor(t, 5*time.Second, time.Millisecond, "apply did not advance lastApplied",
		func() bool { return node.lastApplied.Load() >= 7 })

	// Drain and capture the (index, term) pair as saveSnapshot does.
	node.SetSaving(true)
	if !node.WaitForApplyDrain(stopC) {
		t.Fatal("WaitForApplyDrain returned early")
	}
	gotIdx := node.lastApplied.Load()
	gotTerm := node.lastAppliedTerm.Load()
	node.SetSaving(false)

	close(stopC)
	<-runDone
	wg.Wait()

	if gotIdx != 7 || gotTerm != 3 {
		t.Fatalf("post-drain snapshot point = (index=%d, term=%d), want (7, 3): "+
			"the apply-tracked (lastApplied, lastAppliedTerm) pair is inconsistent", gotIdx, gotTerm)
	}
}

// countingSM records every applied entry index so a test can verify that
// no committed entry is lost across save-gate windows.
type countingSM struct {
	mu      sync.Mutex
	applied []uint64
}

func (s *countingSM) Apply(entries []proto.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		s.applied = append(s.applied, entries[i].Index)
		results[i] = sm.Result{}
	}
	return nil
}

// TestSavingGateDoesNotDropEntries verifies that a batch delivered to the
// apply worker while a snapshot-save capture holds the gate is applied
// after the gate clears, never discarded. Raft's processed marker has
// already advanced past a delivered batch (Peer.Commit), so a discarded
// batch would never be re-delivered and the state machine would silently
// diverge from its peers.
func TestSavingGateDoesNotDropEntries(t *testing.T) {
	applyC := make(chan *applyItem, 256)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})

	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, nil, nil, NewWorkSignal(1), stopC, nil)

	rec := &countingSM{}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, rec, nil, cfg, 10, nil, "", nil)

	runDone := make(chan struct{})
	go func() {
		aw.run()
		close(runDone)
	}()

	// Close the gate, deliver a batch while it is held, keep it closed
	// long enough that a drop-style gate would have returned, then open it.
	node.SetSaving(true)

	const total = 32
	for i := uint64(1); i <= total; i++ {
		node.commitPending.Store(true)
		item := applyPool.Get().(*applyItem)
		item.update = proto.Update{
			ShardID:          1,
			CommittedEntries: []proto.Entry{{Term: 1, Index: i, Cmd: []byte("c")}},
		}
		item.node = node
		applyC <- item
	}
	time.Sleep(5 * time.Millisecond)
	node.SetSaving(false)

	// Every delivered entry must eventually reach the state machine.
	waitFor(t, 5*time.Second, time.Millisecond, "gated entries were not applied",
		func() bool { return node.lastApplied.Load() >= total })

	close(stopC)
	<-runDone
	wg.Wait()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.applied) != total {
		t.Fatalf("applied %d entries, want %d: entries delivered during a "+
			"save-gate window were dropped", len(rec.applied), total)
	}
	for i, idx := range rec.applied {
		if idx != uint64(i+1) {
			t.Fatalf("apply order violated at position %d: got index %d, want %d",
				i, idx, i+1)
		}
	}
}
