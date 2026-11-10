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
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestShardFailure_ReportedOnceUnderConcurrentLatches: every path that trips
// the failure latch — a recovery exhausting its attempts (failShard), a
// SaveState failure on the commit worker, a Peer.Commit failure, an apply
// failure, the host's snapshot receive and routing mirror (Engine.FailShard) —
// may race another. Exactly one wins the latch, and the winner performs both
// reports: one EventListener.OnShardFailed and one Callback.OnShardFailed per
// incarnation, whatever the interleaving and whichever path wins.
func TestShardFailure_ReportedOnceUnderConcurrentLatches(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for round := range 200 {
		var listenerFailed atomic.Int64
		listener := &config.EventListener{
			OnShardFailed: func(config.ShardFailedInfo) { listenerFailed.Add(1) },
		}
		node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", listener)
		cb := newTestCallback()
		// The engine the host fails shards through (Engine.FailShard).
		eng := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, cb, nil)
		cause := errors.New("injected")
		update := proto.Update{ShardID: 1, ReplicaID: 1}
		latches := []func(){
			func() { failShard(node, cb, cause, nil) },
			func() {
				w := &commitWorker{callback: cb, batch: []proto.Update{update}, batchNodes: []*Node{node}}
				w.failBatchSaveState(cause)
			},
			func() { failCommit(node, cb, update, cause) },
			func() { (&applyWorker{callback: cb}).failApply(node, nil, cause) },
			// The snapshot receive's failed LogDB record (the host's
			// messagehandler) and the routing mirror's corrupt entry.
			func() { eng.FailShard(node, cause) },
		}
		var start, done sync.WaitGroup
		start.Add(1)
		for _, latch := range latches {
			done.Add(1)
			go func() {
				defer done.Done()
				start.Wait()
				latch()
			}()
		}
		start.Done()
		done.Wait()

		if !node.IsFailed() {
			t.Fatalf("round %d: node not failed", round)
		}
		if got := listenerFailed.Load(); got != 1 {
			t.Fatalf("round %d: EventListener.OnShardFailed fired %d times, want 1", round, got)
		}
		// Whichever path wins the latch performs both reports: the engine
		// callback is what drains the host's pending reads.
		if got := cb.shardFailedCount.Load(); got != 1 {
			t.Fatalf("round %d: Callback.OnShardFailed fired %d times, want exactly 1", round, got)
		}
	}
}

// orderRecordingCallback records, for each proposal failure and shard-failure
// report, whether the node's latch had already tripped when it ran.
type orderRecordingCallback struct {
	*testCallback
	node   *Node
	mu     sync.Mutex
	events []string
}

func (c *orderRecordingCallback) record(event string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.node.IsFailed() {
		event += " before the latch"
	}
	c.events = append(c.events, event)
}

func (c *orderRecordingCallback) OnProposalFailed(shardID uint64, entries []proto.Entry, err error) {
	c.record("proposals failed")
	c.testCallback.OnProposalFailed(shardID, entries, err)
}

func (c *orderRecordingCallback) OnShardFailed(shardID uint64, err error) {
	c.record("shard failed")
	c.testCallback.OnShardFailed(shardID, err)
}

// TestShardFailure_LatchTripsBeforeTheBatchIsFailed: a latching path trips the
// latch FIRST, then fails its own batch's proposals, then reports. Failing a
// proposal wakes its caller, and a caller that retried before the latch
// tripped would pass the host's IsFailed admission check and register a
// proposal the halted shard never answers.
func TestShardFailure_LatchTripsBeforeTheBatchIsFailed(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	cause := errors.New("injected")
	entries := []proto.Entry{{Index: 1, Term: 1, Key: 7}}
	update := proto.Update{ShardID: 1, ReplicaID: 1, EntriesToSave: entries}
	for name, latch := range map[string]func(node *Node, cb Callback){
		"SaveState failure": func(node *Node, cb Callback) {
			w := &commitWorker{callback: cb, batch: []proto.Update{update}, batchNodes: []*Node{node}}
			w.failBatchSaveState(cause)
		},
		"Peer.Commit failure": func(node *Node, cb Callback) { failCommit(node, cb, update, cause) },
		"apply failure":       func(node *Node, cb Callback) { (&applyWorker{callback: cb}).failApply(node, entries, cause) },
		"apply dispatcher panic": func(node *Node, cb Callback) {
			item := &applyItem{node: node, update: proto.Update{ShardID: 1, ReplicaID: 1, CommittedEntries: entries}}
			(&applyWorker{callback: cb}).failInFlightItem(item)
		},
	} {
		t.Run(name, func(t *testing.T) {
			node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", nil)
			cb := &orderRecordingCallback{testCallback: newTestCallback(), node: node}
			latch(node, cb)
			cb.mu.Lock()
			got := append([]string(nil), cb.events...)
			cb.mu.Unlock()
			want := []string{"proposals failed", "shard failed"}
			if !slices.Equal(got, want) {
				t.Fatalf("events = %q, want %q: the latch must trip before the batch's "+
					"proposals are failed, and the reports follow them", got, want)
			}
		})
	}
}

// panickingReportCallback panics from OnShardFailed when panics is set.
type panickingReportCallback struct {
	*testCallback
	panics bool
}

func (c *panickingReportCallback) OnShardFailed(shardID uint64, err error) {
	c.testCallback.OnShardFailed(shardID, err)
	if c.panics {
		panic("engine callback panicked")
	}
}

// TestShardFailure_ReportsSurviveAPanickingBatch: once a path wins the latch,
// both reports are made even when failing its batch panics (a state machine
// error whose Is/As panics inside the host's OnProposalFailed, say), and the
// panic still reaches the worker's own recovery. The recovery's second
// failShard loses the latch, so had the first skipped its reports nothing
// would ever drain the shard's pending requests.
func TestShardFailure_ReportsSurviveAPanickingBatch(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	var listenerFailed atomic.Int64
	listener := &config.EventListener{
		OnShardFailed: func(config.ShardFailedInfo) { listenerFailed.Add(1) },
	}
	node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", listener)
	cb := newTestCallback()

	recovered := func() (value any) {
		defer func() { value = recover() }()
		failShard(node, cb, errors.New("batch failure"), func() { panic("batch panicked") })
		return nil
	}()
	if recovered != "batch panicked" {
		t.Fatalf("recovered %v, want the batch's own panic re-raised to the worker", recovered)
	}
	// The worker's recovery fails the shard again; it loses the latch.
	failShard(node, cb, errors.New("worker recovery"), nil)

	if got := listenerFailed.Load(); got != 1 {
		t.Fatalf("EventListener.OnShardFailed fired %d times, want 1", got)
	}
	if got := cb.shardFailedCount.Load(); got != 1 {
		t.Fatalf("Callback.OnShardFailed fired %d times, want 1: it is what drains the "+
			"shard's remaining requests", got)
	}
}

// TestShardFailure_OneReportCannotSuppressTheOther: a panic in one report does
// not stop the other, and still reaches the caller once both have run.
func TestShardFailure_OneReportCannotSuppressTheOther(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	t.Run("a panicking listener", func(t *testing.T) {
		listener := &config.EventListener{
			OnShardFailed: func(config.ShardFailedInfo) { panic("listener panicked") },
		}
		node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", listener)
		cb := newTestCallback()
		recovered := func() (value any) {
			defer func() { value = recover() }()
			failShard(node, cb, errors.New("failure"), nil)
			return nil
		}()
		if recovered != "listener panicked" {
			t.Fatalf("recovered %v, want the listener's panic re-raised", recovered)
		}
		if got := cb.shardFailedCount.Load(); got != 1 {
			t.Fatalf("Callback.OnShardFailed fired %d times after the listener panicked, want 1", got)
		}
	})
	t.Run("a panicking engine callback", func(t *testing.T) {
		var listenerFailed atomic.Int64
		listener := &config.EventListener{
			OnShardFailed: func(config.ShardFailedInfo) { listenerFailed.Add(1) },
		}
		node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", listener)
		cb := &panickingReportCallback{testCallback: newTestCallback(), panics: true}
		recovered := func() (value any) {
			defer func() { value = recover() }()
			failShard(node, cb, errors.New("failure"), nil)
			return nil
		}()
		if recovered != "engine callback panicked" {
			t.Fatalf("recovered %v, want the callback's panic re-raised", recovered)
		}
		if got := listenerFailed.Load(); got != 1 {
			t.Fatalf("EventListener.OnShardFailed fired %d times, want 1", got)
		}
	})
}

// TestShardFailure_GoexitInAReportCannotSuppressTheOther: recover returns nil
// for runtime.Goexit, so a report that Goexits is invisible to a recover-based
// guard. Each report is its own deferred call, so the other still runs — the
// engine callback, with which the host drains the shard's pending requests,
// included — as the goroutine exits.
func TestShardFailure_GoexitInAReportCannotSuppressTheOther(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	t.Run("the listener Goexits", func(t *testing.T) {
		listener := &config.EventListener{
			OnShardFailed: func(config.ShardFailedInfo) { runtime.Goexit() },
		}
		node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", listener)
		cb := newTestCallback()
		done := make(chan struct{})
		go func() {
			defer close(done)
			failShard(node, cb, errors.New("failure"), nil)
		}()
		<-done
		if got := cb.shardFailedCount.Load(); got != 1 {
			t.Fatalf("Callback.OnShardFailed fired %d times after the listener Goexited, want 1", got)
		}
	})
	t.Run("the batch Goexits", func(t *testing.T) {
		var listenerFailed atomic.Int64
		listener := &config.EventListener{
			OnShardFailed: func(config.ShardFailedInfo) { listenerFailed.Add(1) },
		}
		node := NewNode(nil, nil, nil, config.Config{ShardID: 1, ReplicaID: 1}, nil, "", listener)
		cb := newTestCallback()
		done := make(chan struct{})
		go func() {
			defer close(done)
			failShard(node, cb, errors.New("failure"), func() { runtime.Goexit() })
		}()
		<-done
		if listenerFailed.Load() != 1 || cb.shardFailedCount.Load() != 1 {
			t.Fatalf("after the batch Goexited: listener=%d callback=%d, want 1 and 1",
				listenerFailed.Load(), cb.shardFailedCount.Load())
		}
	})
}

// abnormalExits are the two ways user code can leave a worker goroutine
// without returning: a panic and runtime.Goexit.
var abnormalExits = map[string]func(){
	"panic":  func() { panic("user code panicked") },
	"Goexit": runtime.Goexit,
}

// runToAbnormalEnd runs fn on its own goroutine, as a worker runs user code,
// and returns once it has ended however it ended; a panic is recovered.
func runToAbnormalEnd(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		fn()
	}()
	<-done
}

// hookMetrics runs hook from the named metric, standing in for a
// user-supplied metrics collector.
type hookMetrics struct {
	*testRaftMetrics
	piggybackHook, batchSizeHook func()
}

func (m *hookMetrics) IncPiggybackCommit(shardID uint64) {
	if m.piggybackHook != nil {
		m.piggybackHook()
	}
	m.testRaftMetrics.IncPiggybackCommit(shardID)
}

func (m *hookMetrics) ObserveProposalBatchSize(shardID uint64, n int) {
	if m.batchSizeHook != nil {
		m.batchSizeHook()
	}
	m.testRaftMetrics.ObserveProposalBatchSize(shardID, n)
}

// hookCallback runs hook from the named callback.
type hookCallback struct {
	*testCallback
	committedHook, batchedHook, readyHook func()
}

func (c *hookCallback) OnCommitted(shardID uint64, entries []proto.Entry) {
	if c.committedHook != nil {
		c.committedHook()
	}
	c.testCallback.OnCommitted(shardID, entries)
}

func (c *hookCallback) OnReadIndexBatched(shardID, batchKey uint64, aliasKeys []uint64) {
	if c.batchedHook != nil {
		c.batchedHook()
	}
	c.testCallback.OnReadIndexBatched(shardID, batchKey, aliasKeys)
}

func (c *hookCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	if c.readyHook != nil {
		c.readyHook()
	}
	c.testCallback.OnReadyToRead(shardID, readyToRead)
}

// TestCriticalWindow_PiggybackCommitToApply: once the piggyback's Peer.Commit
// has advanced raft's processed marker, its committed entries exist only in
// the update on its way to the apply worker. User code that exits abnormally
// in between (a metrics collector, the OnCommitted callback) must not lose
// them silently: either the entries still reach the apply worker, or the
// shard is failed and their proposals are failed with it.
func TestCriticalWindow_PiggybackCommitToApply(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for exitName, exit := range abnormalExits {
		for site, install := range map[string]func(m *hookMetrics, cb *hookCallback, exit func()){
			"metrics":     func(m *hookMetrics, _ *hookCallback, exit func()) { m.piggybackHook = exit },
			"OnCommitted": func(_ *hookMetrics, cb *hookCallback, exit func()) { cb.committedHook = exit },
		} {
			t.Run(site+"/"+exitName, func(t *testing.T) {
				applyC := make(chan *applyItem, 16)
				metrics := &hookMetrics{testRaftMetrics: newTestRaftMetrics()}
				cb := &hookCallback{testCallback: newTestCallback()}
				install(metrics, cb, exit)
				cw := newCommitWorker(0, newTestLogDB(), newTestSender(), make(chan *commitItem, 16), applyC,
					&sync.Pool{New: func() interface{} { return &applyItem{} }}, NewWorkSignal(1), cb,
					make(chan struct{}), metrics)
				cw.noRetryDelay = true

				peer, lr := newTestPeer(t, 1, 1)
				tickPeerToLeader(peer)
				nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
				nodeCfg.SetDefaults()
				node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
				node.SetNotifyCommit(true)
				node.commitPending.Store(true)
				initial, err := peer.GetUpdate(true, 0)
				if err != nil {
					t.Fatalf("GetUpdate: %v", err)
				}
				if err := peer.Commit(initial); err != nil {
					t.Fatalf("Commit(initial): %v", err)
				}
				piggyback := proto.Update{
					ShardID:          1,
					ReplicaID:        1,
					State:            proto.State{Term: 1, Commit: 1},
					CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Key: 5, Cmd: []byte("committed")}},
				}

				runToAbnormalEnd(func() { cw.executePiggyback(node, piggyback) })
				cw.recoverAbnormalExit(errors.New("abnormal exit"), NewWorkSignal(1))

				delivered := len(applyC) == 1
				failed := node.IsFailed() && cb.proposalFailedCount.Load() > 0
				if !delivered && !failed {
					t.Fatalf("committed entries vanished: delivered=%v failed=%v proposalsFailed=%d",
						delivered, node.IsFailed(), cb.proposalFailedCount.Load())
				}
			})
		}
	}
}

// TestCriticalWindow_ProposalBatchToPeerHandle: a proposal batch the step
// worker has taken from the queue exists nowhere else until Peer.Handle
// appends it. User code that exits abnormally in between (the batch-size
// metric, the OnReadIndexBatched callback on the commit-pending path) must
// not leave it lost while reported handed off: either it reached raft, or it
// is not marked taken and its proposals are refused definitely.
func TestCriticalWindow_ProposalBatchToPeerHandle(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for exitName, exit := range abnormalExits {
		for site, run := range map[string]func(t *testing.T, exit func()) (*stepWorker, *Node, *raft.Peer, *testCallback, *uint64){
			"batch-size metric": func(t *testing.T, exit func()) (*stepWorker, *Node, *raft.Peer, *testCallback, *uint64) {
				metrics := &hookMetrics{testRaftMetrics: newTestRaftMetrics(), batchSizeHook: exit}
				cb := newTestCallback()
				sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1),
					make(chan *commitItem, 16), cb, make(chan struct{}), metrics)
				peer, lr := newTestPeer(t, 1, 1)
				tickPeerToLeader(peer)
				nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
				nodeCfg.SetDefaults()
				node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
				sw.loadNode(node)
				var at uint64
				node.DeliverTrackedProposal([]proto.Entry{{Key: 5, Cmd: []byte("x")}}, &at)
				runToAbnormalEnd(func() { sw.drainAndHandleProposals(1, node) })
				return sw, node, peer, cb, &at
			},
			"OnReadIndexBatched on the commit-pending path": func(t *testing.T, exit func()) (*stepWorker, *Node, *raft.Peer, *testCallback, *uint64) {
				cb := &hookCallback{testCallback: newTestCallback(), batchedHook: exit}
				sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1),
					make(chan *commitItem, 16), cb, make(chan struct{}), nil)
				peer, lr := newTestPeer(t, 1, 1)
				tickPeerToLeader(peer)
				nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
				nodeCfg.SetDefaults()
				node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
				sw.loadNode(node)
				node.commitPending.Store(true)
				node.DeliverReadIndex(11)
				node.DeliverReadIndex(12)
				var at uint64
				node.DeliverTrackedProposal([]proto.Entry{{Key: 5, Cmd: []byte("x")}}, &at)
				runToAbnormalEnd(func() { sw.processCommitPendingInbox(1, node) })
				return sw, node, peer, cb.testCallback, &at
			},
		} {
			t.Run(site+"/"+exitName, func(t *testing.T) {
				sw, node, peer, cb, at := run(t, exit)
				sw.recoverAbnormalExit(errors.New("abnormal exit"))

				taken, _ := node.ProposalTaken(at)
				reached := false
				if update, err := peer.GetUpdate(true, 0); err == nil {
					for _, e := range update.EntriesToSave {
						if e.Key == 5 {
							reached = true
						}
					}
				}
				switch {
				case taken && !reached:
					t.Fatal("the batch is reported handed off but never reached raft")
				case !taken && !reached && cb.proposalRejectedCount.Load() == 0:
					t.Fatal("a batch that never reached raft was neither requeued nor refused")
				}
			})
		}
	}
}

// TestCriticalWindow_ReadKeysAndStates: ReadIndex keys the step worker has
// drained, and read states it has taken from raft, exist nowhere else until
// they are handed to raft or delivered. An abnormal exit in between fails
// them, rather than leaving their callers to the deadline sweep.
func TestCriticalWindow_ReadKeysAndStates(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for exitName, exit := range abnormalExits {
		t.Run("drained keys/"+exitName, func(t *testing.T) {
			cb := &hookCallback{testCallback: newTestCallback(), batchedHook: exit}
			sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1),
				make(chan *commitItem, 16), cb, make(chan struct{}), nil)
			peer, lr := newTestPeer(t, 1, 1)
			tickPeerToLeader(peer)
			nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
			nodeCfg.SetDefaults()
			node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
			sw.loadNode(node)
			node.DeliverReadIndex(11)
			node.DeliverReadIndex(12)
			runToAbnormalEnd(func() { sw.drainAndHandleReadIndex(1, node) })
			sw.recoverAbnormalExit(errors.New("abnormal exit"))
			if cb.readIndexFailedCount.Load() == 0 {
				t.Fatal("drained ReadIndex keys that never reached raft were not failed")
			}
		})
		t.Run("read states taken from raft/"+exitName, func(t *testing.T) {
			cb := &hookCallback{testCallback: newTestCallback()}
			ws := NewWorkSignal(1)
			commitC := make(chan *commitItem, 16)
			sw := newStepWorker(0, time.Millisecond, newTestSender(), ws, commitC, cb, make(chan struct{}), nil)
			peer, lr := newTestPeer(t, 1, 1)
			nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
			nodeCfg.SetDefaults()
			node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
			sw.loadNode(node)
			tickPeerToLeader(peer)
			// Persist the election update, as the commit worker would.
			ws.Notify(1)
			sw.processReady()
			select {
			case item := <-commitC:
				if err := peer.Commit(item.update); err != nil {
					t.Fatalf("Commit: %v", err)
				}
				node.commitPending.Store(false)
			default:
			}
			// The confirmation is taken from raft by GetUpdate and delivered
			// by OnReadyToRead, which exits abnormally.
			cb.readyHook = exit
			node.DeliverReadIndex(21)
			ws.Notify(1)
			runToAbnormalEnd(sw.processReady)
			if cb.readyCount.Load() != 0 {
				t.Fatal("fixture: OnReadyToRead completed despite the hook")
			}
			sw.recoverAbnormalExit(errors.New("abnormal exit"))
			if cb.readIndexFailedCount.Load() == 0 {
				t.Fatal("read states taken from raft and never delivered were not failed")
			}
		})
	}
}

// TestCriticalWindow_CommitBatchBeforeSaveState: GetUpdate claims a batch's
// hard state durable as it extracts it, and only the commit worker's
// SaveState makes the claim true. User code that exits abnormally before
// SaveState completes (a user LogDB, its retry path) must not leave the claim
// standing: the next update would be classified ReadOnly and a vote grant that
// never reached disk would be treated as persisted.
func TestCriticalWindow_CommitBatchBeforeSaveState(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for exitName, exit := range abnormalExits {
		t.Run(exitName, func(t *testing.T) {
			ldb := newTestLogDB()
			ldb.setSaveError(errors.New("disk error"))
			cw := newCommitWorker(0, ldb, newTestSender(), make(chan *commitItem, 16), make(chan *applyItem, 16),
				&sync.Pool{New: func() interface{} { return &applyItem{} }}, NewWorkSignal(1), newTestCallback(),
				make(chan struct{}), nil)
			cw.noRetryDelay = true
			cw.retryHookForTest = func(int) { exit() }

			peer, lr := newTestVoterPeer(t, 1, 1, 3)
			nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
			nodeCfg.SetDefaults()
			node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
			if err := peer.Handle(proto.Message{Type: proto.RequestVote, From: 2, To: 1, Term: 5}); err != nil {
				t.Fatalf("Handle(RequestVote): %v", err)
			}
			update, err := peer.GetUpdate(true, 0)
			if err != nil || update.ReadOnly || update.State.Vote != 2 {
				t.Fatalf("fixture: update %+v err %v, want the vote grant to persist", update.State, err)
			}
			node.commitPending.Store(true)
			cw.batch = append(cw.batch[:0], update)
			cw.batchNodes = append(cw.batchNodes[:0], node)
			cw.batchUnpersisted = true

			runToAbnormalEnd(cw.processBatch)
			cw.recoverAbnormalExit(errors.New("abnormal exit"), NewWorkSignal(1))

			next, err := peer.GetUpdate(true, 0)
			if err != nil {
				t.Fatalf("GetUpdate: %v", err)
			}
			if next.ReadOnly || next.State.Vote != 2 {
				t.Fatalf("after the lost SaveState the next update is ReadOnly=%v vote=%d: the "+
					"vote grant would never reach the WAL", next.ReadOnly, next.State.Vote)
			}
			if node.commitPending.Load() {
				t.Fatal("the batch's shard was left wedged")
			}
		})
	}
}

// exitingSender runs exit from Send once armed.
type exitingSender struct {
	*testSender
	armed bool
	exit  func()
}

func (s *exitingSender) Send(msgs []proto.Message) {
	if s.armed {
		s.exit()
	}
	s.testSender.Send(msgs)
}

// TestCriticalWindow_MemoryModeCommitToApply: in memory mode the step worker
// commits an update itself, advancing raft's processed marker, and then hands
// its committed entries to applyC. An abnormal exit in between — here in the
// ordered-message send — must not lose them silently: the shard is failed and
// their proposals are failed with it.
func TestCriticalWindow_MemoryModeCommitToApply(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for exitName, exit := range abnormalExits {
		t.Run(exitName, func(t *testing.T) {
			sender := &exitingSender{testSender: newTestSender(), exit: exit}
			cb := newTestCallback()
			sw := newStepWorker(0, time.Millisecond, sender, NewWorkSignal(1), nil, cb, make(chan struct{}), nil)
			applyC := make(chan *applyItem, 1)
			sw.memoryMode = true
			sw.applyC = applyC
			sw.applyPool = &sync.Pool{New: func() interface{} { return &applyItem{} }}

			peer, lr := newTestPeer(t, 1, 1)
			tickPeerToLeader(peer)
			nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
			nodeCfg.SetDefaults()
			node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
			initial, err := peer.GetUpdate(true, 0)
			if err != nil {
				t.Fatalf("GetUpdate: %v", err)
			}
			if err := peer.Commit(initial); err != nil {
				t.Fatalf("Commit(initial): %v", err)
			}
			update := proto.Update{
				ShardID:          1,
				ReplicaID:        1,
				State:            proto.State{Term: 1, Commit: 1},
				Messages:         []proto.Message{{Type: proto.RequestVoteResp, ShardID: 1, From: 1, To: 2, Term: 1}},
				CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Key: 5, Cmd: []byte("committed")}},
			}
			sender.armed = true

			runToAbnormalEnd(func() { sw.commitMemoryMode(node, update) })
			sw.recoverAbnormalExit(errors.New("abnormal exit"))

			delivered := len(applyC) == 1
			failed := node.IsFailed() && cb.proposalFailedCount.Load() > 0
			if !delivered && !failed {
				t.Fatalf("committed entries vanished: delivered=%v failed=%v", delivered, node.IsFailed())
			}
		})
	}
}

// commitBatchSizeHook runs hook from ObserveCommitBatchSize.
type commitBatchSizeHook struct {
	*testRaftMetrics
	hook func()
}

func (m *commitBatchSizeHook) ObserveCommitBatchSize(n int) {
	m.hook()
	m.testRaftMetrics.ObserveCommitBatchSize(n)
}

// newCommittedNode returns a leader node whose next update can commit entry
// 1, and that update, as a piggyback-style committed batch for shard id.
func newCommittedNode(t *testing.T, id uint64) (*Node, proto.Update) {
	t.Helper()
	peer, lr := newTestPeer(t, id, 1)
	tickPeerToLeader(peer)
	nodeCfg := config.Config{ShardID: id, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
	initial, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if err := peer.Commit(initial); err != nil {
		t.Fatalf("Commit(initial): %v", err)
	}
	return node, proto.Update{
		ShardID:          id,
		ReplicaID:        1,
		State:            proto.State{Term: 1, Commit: 1},
		CommittedEntries: []proto.Entry{{Index: 1, Term: 1, Key: id, Cmd: []byte("committed")}},
	}
}

// TestCriticalWindow_HandedOffBatchItemsAreRetired: once an item of a commit
// batch reaches the apply worker, its commitPending belongs to the apply
// worker. An abnormal exit later in the batch — a later item, or a metric
// recorded after the hand-off — must not clear it: the step worker would
// produce a second update while the first is still being applied, and the
// shard's reusable commit item would be enqueued twice.
func TestCriticalWindow_HandedOffBatchItemsAreRetired(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	for exitName, exit := range abnormalExits {
		t.Run(exitName, func(t *testing.T) {
			applyC := make(chan *applyItem, 16)
			metrics := &commitBatchSizeHook{testRaftMetrics: newTestRaftMetrics(), hook: exit}
			cw := newCommitWorker(0, newTestLogDB(), newTestSender(), make(chan *commitItem, 16), applyC,
				&sync.Pool{New: func() interface{} { return &applyItem{} }}, NewWorkSignal(1), newTestCallback(),
				make(chan struct{}), metrics)
			cw.noRetryDelay = true
			a, ua := newCommittedNode(t, 1)
			b, ub := newCommittedNode(t, 2)
			for _, n := range []*Node{a, b} {
				n.commitPending.Store(true)
			}
			cw.batch = append(cw.batch[:0], ua, ub)
			cw.batchNodes = append(cw.batchNodes[:0], a, b)
			cw.batchUnpersisted = true

			runToAbnormalEnd(cw.processBatch)
			cw.recoverAbnormalExit(errors.New("abnormal exit"), NewWorkSignal(1))

			if got := len(applyC); got != 2 {
				t.Fatalf("fixture: %d items reached the apply worker, want both", got)
			}
			if !a.commitPending.Load() || !b.commitPending.Load() {
				t.Fatalf("commitPending cleared for items the apply worker owns: a=%v b=%v",
					a.commitPending.Load(), b.commitPending.Load())
			}
		})
	}
}

// TestCriticalWindow_HaltedShardRefusesItsDrainedBatchAsShardFailed: a batch
// drained on the commit-pending path whose hand-off is refused because the
// shard halted in between is answered with the definite ErrShardFailed, the
// same answer the host's halt drain gives. ErrDropped is reserved for a batch
// lost to user code on a live shard.
func TestCriticalWindow_HaltedShardRefusesItsDrainedBatchAsShardFailed(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	cb := &hookCallback{testCallback: newTestCallback()}
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1),
		make(chan *commitItem, 16), cb, make(chan struct{}), nil)
	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	node := NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
	sw.loadNode(node)
	node.commitPending.Store(true)
	// The shard halts between the drain and the hand-off.
	cb.batchedHook = func() { failShard(node, nil, errors.New("halted"), nil) }
	node.DeliverReadIndex(11)
	node.DeliverReadIndex(12)
	var at uint64
	node.DeliverTrackedProposal([]proto.Entry{{Key: 5, Cmd: []byte("x")}}, &at)
	sw.processCommitPendingInbox(1, node)

	cb.mu.Lock()
	calls := append([]proposalFailedCall(nil), cb.proposalRejectedCalls...)
	cb.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("OnProposalRejected fired %d times, want 1", len(calls))
	}
	if !errors.Is(calls[0].err, ErrShardFailed) {
		t.Fatalf("refusal = %v, want ErrShardFailed for a halted shard", calls[0].err)
	}
}
