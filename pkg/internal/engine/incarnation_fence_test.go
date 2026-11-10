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
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// gatedLogDB blocks every SaveState until release is closed, announcing each
// entry on entered, so a test can hold a batch mid-persist.
type gatedLogDB struct {
	*testLogDB
	entered chan struct{}
	release chan struct{}
}

func (g *gatedLogDB) SaveState(updates []logdb.Update) error {
	g.entered <- struct{}{}
	<-g.release
	return g.testLogDB.SaveState(updates)
}

func newFenceTestNode(shardID uint64) *Node {
	cfg := config.Config{ShardID: shardID, ReplicaID: 1}
	cfg.SetDefaults()
	return NewNode(nil, nil, nil, cfg, nil, "", nil)
}

// TestCommitWorker_UnloadFenceIsAnsweredWhileForwardIsBackpressured: the
// commit worker's forward to the apply tier blocks while the apply dispatcher
// is backpressured on ANOTHER shard's busy state machine. UnloadNode waits on
// the commit worker's unload fence, so a forward that did not answer
// membership made StopShard of one shard wait out an unrelated shard's
// Apply — and hang outright if that Apply was the one calling StopShard.
func TestCommitWorker_UnloadFenceIsAnsweredWhileForwardIsBackpressured(t *testing.T) {
	applyC := make(chan *applyItem, 1)
	applyC <- &applyItem{} // the dispatcher is not draining: full
	stopC := make(chan struct{})
	defer close(stopC)
	cw := newCommitWorker(0, nil, nil, make(chan *commitItem, 1), applyC,
		&sync.Pool{New: func() interface{} { return &applyItem{} }}, NewWorkSignal(1), nil, stopC, nil)

	delivered := make(chan bool, 1)
	blocked := &applyItem{update: proto.Update{ShardID: 2}}
	go func() { delivered <- cw.forwardToApply(blocked) }()

	fence := cw.requestUnload(1)
	select {
	case <-fence:
	case <-time.After(5 * time.Second):
		t.Fatal("the unload fence was not answered while the forward was blocked: " +
			"StopShard would wait on another shard's apply")
	}
	if !cw.unloaded.has(1) {
		t.Fatal("the fence closed without the unload being applied")
	}

	select {
	case ok := <-delivered:
		t.Fatalf("the forward returned (%v) while the apply tier was still full", ok)
	default:
	}
	<-applyC // the dispatcher drains
	if ok := <-delivered; !ok {
		t.Fatal("the forward gave up instead of delivering once the apply tier drained")
	}
	if got := <-applyC; got != blocked {
		t.Fatal("the delivered item is not the one that was blocked")
	}
}

// TestCommitWorker_UnloadAppliedMidBatchSkipsTheRestOfTheShard pins what makes
// answering the fence during a blocked forward safe: an unload applied while
// processBatch is blocked forwarding one shard's item must still keep every
// later item of the unloaded shard in the same batch from being committed or
// forwarded — the per-item re-checks, not the between-batches timing, are
// what the fence rests on.
func TestCommitWorker_UnloadAppliedMidBatchSkipsTheRestOfTheShard(t *testing.T) {
	ldb := newTestLogDB()
	applyC := make(chan *applyItem, 1)
	stopC := make(chan struct{})
	defer close(stopC)
	cw := newCommitWorker(0, ldb, nil, make(chan *commitItem, 1), applyC,
		&sync.Pool{New: func() any { return &applyItem{} }}, NewWorkSignal(64), nil, stopC, nil)
	cw.noRetryDelay = true

	follower := func(shardID uint64) (*Node, proto.Update) {
		peer, lr := newPiggybackFollower(t, ldb, shardID, 1)
		if err := peer.Handle(proto.Message{
			Type: proto.Replicate, ShardID: shardID, From: 2, To: 1, Term: 1, Commit: 1,
			Entries: []proto.Entry{{Index: 1, Term: 1, Cmd: []byte("x")}},
		}); err != nil {
			t.Fatalf("Handle(Replicate): %v", err)
		}
		u, err := peer.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate: %v", err)
		}
		cfg := config.Config{ShardID: shardID, ReplicaID: 1}
		cfg.SetDefaults()
		node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
		node.commitPending.Store(true)
		return node, u
	}
	busy, busyUpdate := follower(1)
	stopping, stoppingUpdate := follower(2)
	cw.batch = append(cw.batch[:0], busyUpdate, stoppingUpdate)
	cw.batchNodes = []*Node{busy, stopping}

	applyC <- &applyItem{} // the apply tier is backpressured
	done := make(chan struct{})
	go func() {
		cw.processBatch()
		close(done)
	}()

	// UnloadNode's order: mark, stop, then the fence. The fence can only be
	// answered while processBatch is blocked forwarding the busy shard's item.
	stopping.unloaded.Store(true)
	stopping.Stop()
	select {
	case <-cw.requestUnload(2):
	case <-time.After(5 * time.Second):
		t.Fatal("the unload fence was not answered while the batch's forward was blocked")
	}

	<-applyC // the dispatcher drains
	var forwarded []uint64
	for {
		select {
		case ai := <-applyC:
			forwarded = append(forwarded, ai.update.ShardID)
			continue
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("processBatch did not finish once the apply tier drained")
		}
		break
	}
	for len(applyC) > 0 {
		forwarded = append(forwarded, (<-applyC).update.ShardID)
	}
	if len(forwarded) != 1 || forwarded[0] != 1 {
		t.Fatalf("forwarded shards %v, want only the busy shard [1]: the unloaded shard's "+
			"item later in the batch was committed after its fence closed", forwarded)
	}
	if stopping.commitPending.Load() {
		t.Fatal("the skipped item left commitPending set")
	}
}

// TestCommitWorker_UnloadFenceWaitsForInFlightSaveState pins the unload fence.
// Were UnloadNode to hand the commit worker an asynchronous unload and return,
// a batch holding the stopped incarnation's update could reach SaveState
// after a new incarnation of the same shard had loaded LogDB: its HardState
// (a vote) overwriting the new one's, or its adopted snapshot recorded against
// an artifact a later receive had already reclaimed.
func TestCommitWorker_UnloadFenceWaitsForInFlightSaveState(t *testing.T) {
	ldb := &gatedLogDB{
		testLogDB: newTestLogDB(),
		entered:   make(chan struct{}, 1),
		release:   make(chan struct{}),
	}
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cw.run()
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(ldb.release) }) }
	defer func() {
		release()
		close(stopC)
		wg.Wait()
	}()

	node := newFenceTestNode(1)
	node.commitPending.Store(true)
	commitC <- &commitItem{
		update: proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 2, Vote: 3}},
		node:   node,
	}
	select {
	case <-ldb.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the batch never reached SaveState")
	}

	// UnloadNode's sequence: the node is marked unloaded and stops, then the
	// fence is requested.
	node.unloaded.Store(true)
	node.Stop()
	done := cw.requestUnload(1)
	select {
	case <-done:
		t.Fatal("the unload fence closed while the stopped incarnation's " +
			"SaveState was still running: a new incarnation could load LogDB " +
			"and then have the old HardState written over it")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the unload fence never closed after the batch finished")
	}
	if node.commitPending.Load() {
		t.Fatal("commitPending left set on the stopped incarnation")
	}
}

// TestCommitWorker_AdmitDropsStoppedIncarnation is the other half of the
// fence: an update still queued in commitC when its incarnation stopped is
// dropped before SaveState, and its ordered messages — the ones promising
// durability, like a vote grant — never merge into a new incarnation's.
func TestCommitWorker_AdmitDropsStoppedIncarnation(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, make(chan struct{}), nil)

	old := newFenceTestNode(1)
	old.commitPending.Store(true)
	old.unloaded.Store(true)
	old.Stop()
	fresh := newFenceTestNode(1)
	fresh.commitPending.Store(true)

	cw.admit(&commitItem{
		update:      proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 2, Vote: 3}},
		node:        old,
		orderedMsgs: []proto.Message{{Type: proto.RequestVoteResp, ShardID: 1, To: 3}},
	})
	cw.admit(&commitItem{
		update:      proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 2}},
		node:        fresh,
		orderedMsgs: []proto.Message{{Type: proto.HeartbeatResp, ShardID: 1, To: 2}},
	})

	if len(cw.batch) != 1 || cw.batchNodes[0] != fresh {
		t.Fatalf("batch holds %d updates, want only the running incarnation's", len(cw.batch))
	}
	if old.commitPending.Load() {
		t.Fatal("commitPending left set on the dropped incarnation")
	}
	msgs := cw.orderedMsgsBuf[1]
	if len(msgs) != 1 || msgs[0].Type != proto.HeartbeatResp {
		t.Fatalf("ordered messages = %+v, want only the running incarnation's "+
			"HeartbeatResp: the stopped one's vote grant was never made durable", msgs)
	}
}

// TestStepWorker_AnswerDroppedInstalls pins that a local install dropped by a
// recovered step-worker panic is answered rather than left waiting. It also
// pins that the recovery never reads the peer on a poisoned reading and fails a
// node whose install step was interrupted.
// A recovered step-worker panic loses the rest of the drained inbox batch, and
// a local install in it has a receive waiting on its verdict; unanswered, that
// receive would wait until the shard stopped, holding a transport receive slot
// the whole time.
func TestStepWorker_AnswerDroppedInstalls(t *testing.T) {
	newNode := func(t *testing.T, committed uint64) *Node {
		t.Helper()
		ldb := newTestLogDB()
		lr := logdb.NewLogReader(1, 1, ldb)
		if err := lr.ApplySnapshot(proto.Snapshot{
			Index: committed, Term: 1,
			Membership: proto.Membership{Addresses: map[uint64]string{1: "a1", 2: "a2"}},
		}); err != nil {
			t.Fatalf("ApplySnapshot: %v", err)
		}
		cfg := config.Config{ShardID: 1, ReplicaID: 1}
		cfg.SetDefaults()
		peer, err := raft.NewPeer(cfg, lr, nil)
		if err != nil {
			t.Fatalf("NewPeer: %v", err)
		}
		return NewNode(peer, nil, nil, cfg, nil, "", nil)
	}
	install := func(index uint64) proto.Message {
		return proto.Message{
			Type: proto.InstallSnapshot, From: 2, To: 1, ShardID: 1,
			Origin: proto.OriginLocal, Snapshot: proto.Snapshot{Index: index, Term: 1},
		}
	}
	verdictOf := func(t *testing.T, c <-chan InstallVerdict) InstallOutcome {
		t.Helper()
		select {
		case v := <-c:
			return v.Outcome
		default:
			t.Fatal("no verdict: the waiting receive would block until the shard stops")
			return 0
		}
	}

	tests := []struct {
		name            string
		committed       uint64
		committedBefore uint64
		wantInHand      InstallOutcome
		wantFailed      bool
	}{
		{"interrupted before adoption fails the shard and keeps the artifact", 10, 10, InstallAbandoned, true},
		{"adopted before the panic is reported adopted", 700, 10, InstallAdopted, false},
		{"a poisoned pre-step reading is never taken for adoption", 700, math.MaxUint64, InstallAbandoned, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newNode(t, tt.committed)
			sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, nil, make(chan struct{}), nil)
			inHand := node.AwaitInstallVerdict(500)
			unstepped := node.AwaitInstallVerdict(600)

			heartbeat := proto.Message{Type: proto.Heartbeat, From: 2, To: 1, ShardID: 1}
			sw.inboxBuf = []proto.Message{heartbeat, install(500), heartbeat, install(600)}
			sw.inboxNode, sw.inboxStep, sw.inboxCommittedBefore = node, 1, tt.committedBefore

			sw.answerDroppedInstalls(errors.New("injected step panic"))

			if got := verdictOf(t, inHand); got != tt.wantInHand {
				t.Errorf("install in raft's hands answered %v, want %v", got, tt.wantInHand)
			}
			if got := node.IsFailed(); got != tt.wantFailed {
				t.Errorf("node failed = %v, want %v: an interrupted install leaves raft's "+
					"state unknown, and a node left running would let a same-index retry "+
					"reclaim an artifact raft may record", got, tt.wantFailed)
			}
			if got := verdictOf(t, unstepped); got != InstallSuperseded {
				t.Errorf("unstepped install answered %v, want InstallSuperseded", got)
			}
			if sw.inboxNode != nil {
				t.Error("inboxNode left set: a later panic would answer a stale batch")
			}
		})
	}
}

// TestStepWorker_InboxDrainClearsInboxNode pins that a completed drain leaves
// nothing for the panic recovery to answer.
func TestStepWorker_InboxDrainClearsInboxNode(t *testing.T) {
	node := newFenceTestNode(1)
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, nil, make(chan struct{}), nil)
	sw.drainAndHandleInbox(1, node, false)
	if sw.inboxNode != nil {
		t.Fatal("inboxNode still set after a completed drain")
	}
}

// TestCommitWorker_AdmitPersistsDuringEngineShutdown pins the shutdown half of
// the admission rule. Engine.stop stops every node in step 1 but closes the
// commit tier's own stopC only in step 3, so gating the drop on stopC dropped
// every update the step tier handed over in between — the updates the
// shutdown drain exists to persist. The drop is gated on the node having been
// UNLOADED instead, which Engine.Stop never marks.
func TestCommitWorker_AdmitPersistsDuringEngineShutdown(t *testing.T) {
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	cw := newCommitWorker(0, newTestLogDB(), nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, make(chan struct{}), nil)
	node := newFenceTestNode(1)
	node.commitPending.Store(true)
	node.Stop() // Engine.Stop step 1; the worker's own stopC is still open
	cw.admit(&commitItem{
		update:      proto.Update{ShardID: 1, ReplicaID: 1, State: proto.State{Term: 2, Vote: 3}},
		node:        node,
		orderedMsgs: []proto.Message{{Type: proto.RequestVoteResp, ShardID: 1, To: 3}},
	})
	if len(cw.batch) != 1 {
		t.Fatal("an update handed over during engine shutdown was dropped rather than persisted")
	}
	if len(cw.orderedMsgsBuf[1]) != 1 {
		t.Fatal("the update's ordered messages were dropped with it")
	}
}

// TestNode_WriteLeaseFencesUnload pins the lease a snapshot receive holds while
// it writes on an incarnation's behalf: UnloadNode's wait does not return
// while one is held, and none is granted once the node has stopped.
func TestNode_WriteLeaseFencesUnload(t *testing.T) {
	node := newFenceTestNode(1)
	if !node.AcquireWriteLease() {
		t.Fatal("a running node refused a write lease")
	}

	fenced := make(chan struct{})
	go func() {
		node.Stop()
		node.awaitWriteLeases()
		close(fenced)
	}()
	waitFor(t, 5*time.Second, time.Millisecond, "node never stopped", node.IsStopped)
	select {
	case <-fenced:
		t.Fatal("the unload fence passed while a receive still held a write lease: " +
			"its LogDB record or log-floor advance could land under a new incarnation")
	case <-time.After(50 * time.Millisecond):
	}

	node.ReleaseWriteLease()
	select {
	case <-fenced:
	case <-time.After(5 * time.Second):
		t.Fatal("the unload fence never passed after the lease was released")
	}
	if node.AcquireWriteLease() {
		node.ReleaseWriteLease()
		t.Fatal("a stopped node granted a write lease")
	}
}

// TestSnapshotPool_StoppedNodeRecoveryIsAbandoned: a recovery queued on the
// incarnation that adopted an install can find that node stopped. Its failure
// is the unload, not a failed restore, and reporting it the way a failed
// restore is reported — Fail, OnShardFailed, OnSnapshotCompleted, all resolved
// by shard ID — would land on the incarnation that replaced it.
func TestSnapshotPool_StoppedNodeRecoveryIsAbandoned(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	cb := &testCallback{}
	p := newSnapshotPool(2, ldb, nil, cb, nil, stopC, nil)
	p.start(&wg)

	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)
	ss := &failingRecoverSnapshotter{
		snapshotTestSnapshotter: newSnapshotTestSnapshotter(0),
		err:                     errors.New("state machine closed by the unload"),
	}
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	installSnapshotIntoLog(t, node, peer, 100, 7)
	node.Stop()

	if err := p.requestSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}
	if res := awaitSnapshotResult(t, p, "recovery"); res.err == nil {
		t.Fatal("the recovery on a stopped node reported success")
	}
	if node.IsFailed() {
		t.Error("a recovery abandoned by the unload failed the node")
	}
	if n := len(cb.getShardFailedCalls()); n != 0 {
		t.Errorf("OnShardFailed fired %d times for the unload; it resolves by shard ID "+
			"and would fail the incarnation that replaced this one", n)
	}
	if n := len(cb.getSnapshotCalls()); n != 0 {
		t.Errorf("OnSnapshotCompleted fired %d times for the unload; it would complete "+
			"the next incarnation's pending snapshot requests with this error", n)
	}
	if node.IsRecovering() {
		t.Error("the recovering gate is still raised")
	}
}

// TestApplyWorker_UnloadDuringShutdownWaitsForTheJoin: an unload arriving once
// the apply tier is stopping must not be acknowledged at once, because the ack
// is what lets the host close the shard's state machine — while the stopping
// dispatcher's drain could still be inside that state machine's Apply. It
// completes only once the dispatcher has joined every per-shard goroutine.
func TestApplyWorker_UnloadDuringShutdownWaitsForTheJoin(t *testing.T) {
	stopC := make(chan struct{})
	aw := newApplyWorker(0, make(chan *applyItem, 1), &sync.Pool{New: func() interface{} { return &applyItem{} }},
		1, &sync.WaitGroup{}, nil, nil, NewWorkSignal(1), stopC, nil)
	close(stopC)

	done := aw.requestUnload(7)
	select {
	case <-done:
		t.Fatal("an unload during shutdown was acknowledged before the per-shard " +
			"goroutines were joined: the host could close a state machine still in Apply")
	default:
	}
	aw.shutdownIfStopping()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the unload was never acknowledged after the join")
	}
}

// TestEngine_ApplyLoopJoinsWhenStoppedDuringRestart: the apply dispatcher
// panics while the engine runs, and the shutdown lands during its restart
// delay. The loop must still run the dispatcher's stop path: otherwise the
// per-shard goroutines are never joined and joinedC never closes, and an
// UnloadNode racing that shutdown waits on it forever.
func TestEngine_ApplyLoopJoinsWhenStoppedDuringRestart(t *testing.T) {
	_, restore := captureLogOutput(t)
	defer restore()

	e := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	applyC := make(chan *applyItem, 16)
	stopC := make(chan struct{})
	aw := newApplyWorker(0, applyC, &sync.Pool{New: func() interface{} { return &applyItem{} }},
		4, &sync.WaitGroup{}, nil, nil, NewWorkSignal(1), stopC, nil)
	aw.shardChans = nil // the next dispatch panics on the nil map

	done := make(chan struct{})
	go func() {
		e.runApplyWorkerLoop(aw)
		close(done)
	}()
	applyC <- &applyItem{update: proto.Update{ShardID: 1, CommittedEntries: []proto.Entry{{Index: 1}}}}
	waitFor(t, 5*time.Second, time.Millisecond, "the dispatcher never took the item",
		func() bool { return len(applyC) == 0 })
	close(stopC)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the apply loop never exited after a shutdown during its restart delay")
	}
	select {
	case <-aw.joinedC:
	default:
		t.Fatal("joinedC left open: an UnloadNode racing the shutdown would wait forever")
	}
}

// TestApplyWorker_IncarnationGuardKeepsTheShutdownDrain: the dispatcher's
// incarnation guard read the node's stopC, which Engine.Stop closes for every
// node long before the apply tier exits — so the shutdown drain discarded
// committed, durable items and failed their proposals. The guard now reads
// whether the node was UNLOADED: an unloaded incarnation's item is still
// discarded, and a node merely stopped by the shutdown is still applied.
func TestApplyWorker_IncarnationGuardKeepsTheShutdownDrain(t *testing.T) {
	cb := &testCallback{}
	aw := newApplyWorker(0, make(chan *applyItem, 1), &sync.Pool{New: func() interface{} { return &applyItem{} }},
		1, &sync.WaitGroup{}, cb, nil, NewWorkSignal(1), make(chan struct{}), nil)
	shardCh := make(chan *applyItem, 4)
	aw.shardChans[1] = shardCh // stands in for the per-shard goroutine

	shutdownStopped := newFenceTestNode(1)
	shutdownStopped.Stop()
	aw.dispatch(&applyItem{node: shutdownStopped, update: proto.Update{ShardID: 1, CommittedEntries: []proto.Entry{{Index: 5, Key: 9}}}})
	if len(shardCh) != 1 {
		t.Fatal("a committed item of a node stopped by the engine shutdown was discarded, not applied")
	}
	if n := len(cb.getProposalFailedCalls()); n != 0 {
		t.Fatalf("the shutdown drain failed %d proposals that were committed and durable", n)
	}

	unloaded := newFenceTestNode(1)
	unloaded.unloaded.Store(true)
	unloaded.Stop()
	aw.dispatch(&applyItem{node: unloaded, update: proto.Update{ShardID: 1, CommittedEntries: []proto.Entry{{Index: 6, Key: 10}}}})
	if len(shardCh) != 1 {
		t.Fatal("an unloaded incarnation's item reached the shard's apply channel")
	}
	if n := len(cb.getProposalFailedCalls()); n != 1 {
		t.Fatalf("OnProposalFailed fired %d times for the unloaded incarnation's item, want 1", n)
	}
}

// TestApplyWorker_BackpressuredShutdownDeliversRatherThanDiscards: a
// dispatcher blocked on a FULL shard channel must not abandon the committed
// item the moment the engine begins stopping, which would fail proposals whose
// entries are committed and durable. The shard goroutine keeps consuming during
// shutdown, so the item is delivered; it is discarded only when that
// goroutine has exited and can read nothing more.
func TestApplyWorker_BackpressuredShutdownDeliversRatherThanDiscards(t *testing.T) {
	newWorker := func(t *testing.T) (*applyWorker, *testCallback, chan *applyItem, chan struct{}, chan struct{}) {
		t.Helper()
		cb := &testCallback{}
		stopC := make(chan struct{})
		aw := newApplyWorker(0, make(chan *applyItem, 1), &sync.Pool{New: func() interface{} { return &applyItem{} }},
			1, &sync.WaitGroup{}, cb, nil, NewWorkSignal(1), stopC, nil)
		shardCh := make(chan *applyItem, 1)
		shardCh <- &applyItem{} // full
		exited := make(chan struct{})
		aw.shardChans[1] = shardCh
		aw.shardExits[1] = exited
		return aw, cb, shardCh, stopC, exited
	}
	item := func() *applyItem {
		return &applyItem{node: newFenceTestNode(1), update: proto.Update{ShardID: 1, CommittedEntries: []proto.Entry{{Index: 7, Key: 3}}}}
	}

	t.Run("engine stopping: delivered once the shard reads", func(t *testing.T) {
		aw, cb, shardCh, stopC, _ := newWorker(t)
		done := make(chan struct{})
		go func() {
			aw.dispatch(item())
			close(done)
		}()
		close(stopC)
		select {
		case <-done:
			t.Fatal("the dispatcher gave up on a committed item because the engine is stopping")
		case <-time.After(50 * time.Millisecond):
		}
		<-shardCh // the live shard goroutine reads
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the committed item was never delivered")
		}
		if n := len(cb.getProposalFailedCalls()); n != 0 {
			t.Fatalf("the shutdown drain failed %d committed proposals", n)
		}
		if got := <-shardCh; got == nil || got.update.ShardID != 1 {
			t.Fatal("the delivered item is not the committed one")
		}
	})

	t.Run("shard goroutine exited: discarded", func(t *testing.T) {
		aw, cb, _, _, exited := newWorker(t)
		close(exited)
		aw.dispatch(item())
		if n := len(cb.getProposalFailedCalls()); n != 1 {
			t.Fatalf("OnProposalFailed fired %d times for an item no goroutine can apply, want 1", n)
		}
	})
}

// TestStepWorker_AnsweredInstallNeverFailsTheNode: a panic AFTER the install's
// step returned — in a log call, or the classification itself — leaves raft's
// state known and the verdict already delivered; the recovery must not fail a
// healthy node over it. Nor a stopped one, whose failure would be reported by
// shard ID to the incarnation that follows.
func TestStepWorker_AnsweredInstallNeverFailsTheNode(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*Node)
	}{
		{"verdict already delivered", func(n *Node) {
			n.signalInstallVerdict(500, InstallVerdict{Outcome: InstallDeclined})
		}},
		{"node stopped", func(n *Node) { n.Stop() }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := newFenceTestNode(1)
			node.AwaitInstallVerdict(500)
			tt.setup(node)
			sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, nil, make(chan struct{}), nil)
			sw.inboxBuf = []proto.Message{{Type: proto.InstallSnapshot, Origin: proto.OriginLocal, ShardID: 1, Snapshot: proto.Snapshot{Index: 500}}}
			sw.inboxNode, sw.inboxStep, sw.inboxCommittedBefore = node, 0, 10

			sw.answerDroppedInstalls(errors.New("panic after the step"))
			if node.IsFailed() {
				t.Fatal("the panic recovery failed a node whose install was already settled")
			}
		})
	}
}

// TestSnapshotPool_SuccessfulRecoveryOnStoppedNodeReportsNothing: a restore
// that SUCCEEDS on a node stopped under it restored a state machine discarded
// with the node, and OnSnapshotCompleted — resolved by shard ID — would
// complete the next incarnation's pending snapshot requests with its result.
func TestSnapshotPool_SuccessfulRecoveryOnStoppedNodeReportsNothing(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	cb := &testCallback{}
	p := newSnapshotPool(2, ldb, nil, cb, nil, stopC, nil)
	p.start(&wg)

	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
	installSnapshotIntoLog(t, node, peer, 100, 7)
	node.Stop()

	if err := p.requestSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}
	awaitSnapshotResult(t, p, "recovery")
	if n := len(cb.getSnapshotCalls()); n != 0 {
		t.Fatalf("OnSnapshotCompleted fired %d times for a stopped node's recovery", n)
	}
	if node.IsRecovering() {
		t.Error("the recovering gate is still raised")
	}
}

// blockingCapture holds the state machine's capture open until release is
// closed, announcing on entered that it has begun: a save caught mid-capture
// by an UnloadNode.
type blockingCapture struct {
	*snapshotTestSnapshotter
	entered chan struct{}
	release chan struct{}
}

func (s *blockingCapture) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

func (s *blockingCapture) SaveSnapshotGated(ctx context.Context, w io.Writer, stopper <-chan struct{}, afterCapture func()) error {
	close(s.entered)
	<-s.release
	return s.snapshotTestSnapshotter.SaveSnapshotGated(ctx, w, stopper, afterCapture)
}

// TestSnapshotPool_SaveIsFencedByUnload: a local save stages its artifact and
// publishes it onto the canonical snapshot-<index> directory — shared with a
// snapshot receive at the same index — under the node's write lease. The
// unload must not wait for the capture (user code: a state machine ignoring
// the stopper, or waiting on its own shard, would hang StopShard), and a save
// whose node stopped publishes, records and leaves nothing.
func TestSnapshotPool_SaveIsFencedByUnload(t *testing.T) {
	newPool := func(t *testing.T) (*snapshotPool, *snapshotTestLogDB) {
		t.Helper()
		stopC := make(chan struct{})
		t.Cleanup(func() { close(stopC) })
		ldb := newSnapshotTestLogDB()
		p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
		var wg sync.WaitGroup
		p.start(&wg)
		return p, ldb
	}
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}

	t.Run("unload does not wait for the capture, and the save publishes nothing", func(t *testing.T) {
		p, ldb := newPool(t)
		ss := &blockingCapture{
			snapshotTestSnapshotter: newSnapshotTestSnapshotter(100),
			entered:                 make(chan struct{}),
			release:                 make(chan struct{}),
		}
		dir := t.TempDir()
		node := NewNode(nil, nil, nil, cfg, ss, dir, nil)
		if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node, save: true}); err != nil {
			t.Fatalf("requestSnapshot: %v", err)
		}
		<-ss.entered

		fenced := make(chan struct{})
		go func() {
			node.Stop()
			node.awaitWriteLeases()
			close(fenced)
		}()
		select {
		case <-fenced:
		case <-time.After(5 * time.Second):
			close(ss.release)
			t.Fatal("the unload waited for the state machine's capture: a state machine " +
				"that ignores the stopper, or waits on its own shard, would hang StopShard")
		}
		close(ss.release)
		if res := awaitSnapshotResult(t, p, "snapshot"); res.err == nil {
			t.Fatal("a save whose node stopped during its capture reported success")
		}
		if snap, err := ldb.GetSnapshot(10, 1); err == nil {
			t.Fatalf("a stopped node's save recorded snapshot %d", snap.Index)
		}
		canonical := snapshotSaveDir(dir, 10, 1, 100)
		if _, err := os.Stat(canonical); !os.IsNotExist(err) {
			t.Fatalf("a stopped node's save published %s (stat err %v)", canonical, err)
		}
		if stages, _ := filepath.Glob(filepath.Join(filepath.Dir(canonical), SaveStageDirPrefix+"*")); len(stages) != 0 {
			t.Fatalf("the unpublished save left its staging directory behind: %v", stages)
		}
	})

	t.Run("a save on a stopped node writes nothing", func(t *testing.T) {
		p, ldb := newPool(t)
		dir := t.TempDir()
		node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(100), dir, nil)
		node.Stop()
		if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node, save: true}); err != nil {
			t.Fatalf("requestSnapshot: %v", err)
		}
		if res := awaitSnapshotResult(t, p, "snapshot"); res.err == nil {
			t.Fatal("a save on a stopped node reported success")
		}
		if snap, err := ldb.GetSnapshot(10, 1); err == nil {
			t.Fatalf("a stopped node's save recorded snapshot %d", snap.Index)
		}
		if _, err := os.Stat(snapshotSaveDir(dir, 10, 1, 100)); !os.IsNotExist(err) {
			t.Fatalf("a stopped node's save created its snapshot directory (stat err %v)", err)
		}
	})
}

// TestSweepStaleSaveStageDirs_ReclaimsOnlySaveStages: a save's staging
// directory is invisible to GC and the purge-floor scan by construction, so a
// crash's orphan is reclaimed only by this sweep at StartShard — and the sweep
// must touch nothing else in the snapshot tree.
func TestSweepStaleSaveStageDirs_ReclaimsOnlySaveStages(t *testing.T) {
	dir := t.TempDir()
	base := snapshotBaseDir(dir, 3, 1)
	keep := []string{
		filepath.Base(snapshotSaveDir(dir, 3, 1, 40)),
		RecvStageDirPrefix + "live",
		ImportStageDirPrefix + "live",
	}
	orphan := SaveStageDirPrefix + "crashed"
	for _, name := range append(append([]string{}, keep...), orphan) {
		if err := os.MkdirAll(filepath.Join(base, name), 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}

	SweepStaleSaveStageDirs(dir, 3, 1)

	if _, err := os.Stat(filepath.Join(base, orphan)); !os.IsNotExist(err) {
		t.Fatalf("the crashed save's staging directory survived the sweep (stat err %v)", err)
	}
	for _, name := range keep {
		if _, err := os.Stat(filepath.Join(base, name)); err != nil {
			t.Fatalf("the save sweep removed %s, which it does not own: %v", name, err)
		}
	}
}

// TestSnapshotPool_PublishedSaveHasTheDocumentedMode: a stage is published by
// rename, which keeps its mode, and os.MkdirTemp creates 0700 — so a staged
// save published a snapshot directory the documented 0750 promises the
// operator's group can traverse, and it could not.
func TestSnapshotPool_PublishedSaveHasTheDocumentedMode(t *testing.T) {
	f := newResaveFixture(t, 90)
	if _, err := f.pool.saveSnapshot(f.req); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}
	fi, err := os.Stat(f.saveDirFor(90))
	if err != nil {
		t.Fatalf("stat the published snapshot: %v", err)
	}
	// The contract is "0750 under the umask, like every other snapshot
	// directory", so the expectation is a directory made the same way under
	// the same umask, not a literal 0750 a restrictive umask would break.
	ref := filepath.Join(t.TempDir(), "reference")
	if err := os.Mkdir(ref, 0o750); err != nil {
		t.Fatalf("mkdir reference: %v", err)
	}
	want, err := os.Stat(ref)
	if err != nil {
		t.Fatalf("stat reference: %v", err)
	}
	if got := fi.Mode().Perm(); got != want.Mode().Perm() {
		t.Fatalf("published snapshot directory mode = %o, want %o (0750 under the umask)", got, want.Mode().Perm())
	}
	if fi.Mode().Perm() == 0o700 && want.Mode().Perm() != 0o700 {
		t.Fatal("published snapshot directory kept os.MkdirTemp's 0700")
	}
}

// TestGCSnapshotDirs_SparesAHeldDirectory: the GC spared only the directory
// the record NOW names, so under retention pressure it deleted a directory
// still in use and failed the shard. Two holds keep a directory out of the
// removal window:
//
//   - a recovery chooses the record's directory and reads it by path for its
//     whole length, while a receive or save recording a newer snapshot runs
//     the GC; the pinned directory (Node.PinSnapshotDir) is spared;
//   - a receive publishes its canonical directory and claims it until raft's
//     verdict, during which it is unrecorded. Unrecorded directories at higher
//     indexes — receives a stop abandoned, or a crash left — pushed it into
//     the removal window, and raft adopting it then recorded a snapshot whose
//     directory was gone; the claimed directory (Node.ClaimSnapshotDir) is
//     spared.
func TestGCSnapshotDirs_SparesAHeldDirectory(t *testing.T) {
	const shardID, replicaID = uint64(4), uint64(1)
	for _, tt := range []struct {
		name     string
		recorded bool // the GC is told the newest directory is the recorded one
		hold     func(n *Node, oldest string)
		release  func(n *Node, oldest string)
	}{
		{
			name:     "a recovery is reading it",
			recorded: true,
			hold:     func(n *Node, oldest string) { n.PinSnapshotDir(oldest) }, // the recovery's choice
			release:  func(n *Node, oldest string) { n.UnpinSnapshotDir(oldest) },
		},
		{
			name:    "a receive has claimed it",
			hold:    func(n *Node, _ string) { n.ClaimSnapshotDir(100) }, // published, awaiting raft's verdict
			release: func(n *Node, _ string) { n.ReleaseSnapshotDir(100) },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			node := NewNode(nil, nil, nil, config.Config{ShardID: shardID, ReplicaID: replicaID}, nil, base, nil)
			var dirs []string
			for i := uint64(1); i <= 5; i++ { // two more than the GC retains
				dir := node.SnapshotSaveDir(i * 100)
				if err := os.MkdirAll(dir, 0o750); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				dirs = append(dirs, dir)
			}
			node.LockSnapshotDir()
			tt.hold(node, dirs[0])
			spare := node.GCSparedSnapshotDirs()
			node.UnlockSnapshotDir()

			recorded := ""
			if tt.recorded {
				recorded = dirs[4]
			}
			if err := GCSnapshotDirs(base, shardID, replicaID, recorded, spare...); err != nil {
				t.Fatalf("GCSnapshotDirs: %v", err)
			}
			if _, err := os.Stat(dirs[0]); err != nil {
				t.Fatalf("GC deleted the held directory: %v", err)
			}
			if _, err := os.Stat(dirs[1]); !os.IsNotExist(err) {
				t.Fatalf("GC spared an unheld directory beyond retention (stat err %v)", err)
			}

			node.LockSnapshotDir()
			tt.release(node, dirs[0])
			if got := node.GCSparedSnapshotDirs(); len(got) != 0 {
				t.Fatalf("holds left after the release: %v", got)
			}
			node.UnlockSnapshotDir()
		})
	}
}

// pinCheckingSnapshotter records, from inside RecoverFromSnapshot, whether the
// directory being recovered is pinned — the pin must span the read.
type pinCheckingSnapshotter struct {
	*snapshotTestSnapshotter
	node   *Node
	dir    string
	pinned bool
	called bool
}

func (s *pinCheckingSnapshotter) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	s.called = true
	s.node.LockSnapshotDir()
	s.pinned = s.node.IsSnapshotDirPinned(s.dir)
	s.node.UnlockSnapshotDir()
	return s.snapshotTestSnapshotter.RecoverFromSnapshot(ctx, r, stopper)
}

// TestRecoverSnapshot_PinsItsDirectoryWhileReading: the GC honours pins
// (above); this pins that recoverSnapshot actually TAKES one for the
// directory it reads, holds it across the state machine's read, and releases
// it after.
func TestRecoverSnapshot_PinsItsDirectoryWhileReading(t *testing.T) {
	const shardID, replicaID uint64 = 12, 1
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	seedRecoverableSnapshot(t, ldb, shardID, replicaID, 100, 3, 100)
	rec, err := ldb.GetSnapshot(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	ss := &pinCheckingSnapshotter{snapshotTestSnapshotter: newSnapshotTestSnapshotter(0), dir: rec.Filepath}
	node, peer := newRecoveryTestNode(t, shardID, replicaID, ss)
	ss.node = node
	installSnapshotIntoLog(t, node, peer, 100, 3)

	p := newSnapshotPool(1, ldb, nil, nil, nil, stopC, nil)
	if err := p.recoverSnapshot(snapshotRequest{shardID: shardID, replicaID: replicaID, node: node}); err != nil {
		t.Fatalf("recoverSnapshot: %v", err)
	}
	if !ss.called {
		t.Fatal("the state machine was never asked to recover; the probe proves nothing")
	}
	if !ss.pinned {
		t.Fatal("the recovery read its directory unpinned: a concurrent GC could delete it underneath")
	}
	node.LockSnapshotDir()
	defer node.UnlockSnapshotDir()
	if node.IsSnapshotDirPinned(rec.Filepath) {
		t.Fatal("the recovery left its directory pinned")
	}
}

// TestEngine_StoppedApplyWorkerIsNotRerunAfterItsJoin: a dispatcher whose
// panic unwound with stopC already closed ran its deferred stop path — joined
// every per-shard goroutine and closed joinedC. Running it again could start a
// per-shard goroutine after joinedC had promised there were none, and a
// StopShard waiting on joinedC would then close a state machine that
// goroutine could still apply to.
func TestEngine_StoppedApplyWorkerIsNotRerunAfterItsJoin(t *testing.T) {
	e := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	applyC := make(chan *applyItem, 1)
	stopC := make(chan struct{})
	aw := newApplyWorker(0, applyC, &sync.Pool{New: func() interface{} { return &applyItem{} }},
		4, &sync.WaitGroup{}, nil, nil, NewWorkSignal(1), stopC, nil)
	close(stopC)
	aw.shutdownIfStopping() // the panicked run's deferred stop path
	applyC <- &applyItem{node: newFenceTestNode(1), update: proto.Update{ShardID: 1}}

	e.finishStoppedApplyWorker(aw)
	if len(applyC) != 1 || len(aw.shardChans) != 0 {
		t.Fatal("the dispatcher ran again after its join: it may start per-shard " +
			"goroutines that joinedC has already promised do not exist")
	}
}

// fenceProbeCallback checks, inside a report made by shard ID, that an unload
// of the reporting node cannot complete until the report has returned — the
// property that keeps the report from reaching a next incarnation.
type fenceProbeCallback struct {
	testCallback
	node      *Node
	fenceHeld chan bool
	// fenced closes once the probe's unload fence has passed; a test waits on
	// it to prove the report released the lease rather than leaking it.
	fenced chan struct{}
}

func (c *fenceProbeCallback) OnShardFailed(shardID uint64, err error) {
	c.testCallback.OnShardFailed(shardID, err)
	fenced := c.fenced
	go func() {
		c.node.Stop()
		c.node.awaitWriteLeases()
		close(fenced)
	}()
	select {
	case <-fenced:
		c.fenceHeld <- false
	case <-time.After(50 * time.Millisecond):
		c.fenceHeld <- true
	}
}

// TestStepWorker_PanicFailureReportIsFencedByUnload: the panic recovery's
// failure report must be fenced by the unload; a report that merely followed a
// stop check it did not hold would let an unload landing in between route
// OnShardFailed — resolved by shard ID — to the incarnation that followed.
func TestStepWorker_PanicFailureReportIsFencedByUnload(t *testing.T) {
	node := newFenceTestNode(1)
	cb := &fenceProbeCallback{node: node, fenceHeld: make(chan bool, 1), fenced: make(chan struct{})}
	node.AwaitInstallVerdict(500)
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), nil, cb, make(chan struct{}), nil)
	sw.inboxBuf = []proto.Message{{Type: proto.InstallSnapshot, Origin: proto.OriginLocal, ShardID: 1, Snapshot: proto.Snapshot{Index: 500}}}
	sw.inboxNode, sw.inboxStep, sw.inboxCommittedBefore = node, 0, math.MaxUint64

	sw.answerDroppedInstalls(errors.New("panic inside the install"))
	select {
	case held := <-cb.fenceHeld:
		if !held {
			t.Fatal("an unload completed while the failure report was still being made")
		}
		select {
		case <-cb.fenced:
		case <-time.After(5 * time.Second):
			t.Fatal("the unload fence never passed after the report returned: the lease leaked")
		}
	default:
		t.Fatal("the interrupted install was not reported")
	}
}

// TestSnapshotPool_RecoveryReportIsFencedByUnload: likewise for a failed
// recovery — its node checked as running, then reported by shard ID.
func TestSnapshotPool_RecoveryReportIsFencedByUnload(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)
	ss := &failingRecoverSnapshotter{
		snapshotTestSnapshotter: newSnapshotTestSnapshotter(0),
		err:                     errors.New("user state machine refused the snapshot"),
	}
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	installSnapshotIntoLog(t, node, peer, 100, 7)
	cb := &fenceProbeCallback{node: node, fenceHeld: make(chan bool, 1), fenced: make(chan struct{})}
	p := newSnapshotPool(2, ldb, nil, cb, nil, stopC, nil)
	p.start(&wg)

	if err := p.requestSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}
	awaitSnapshotResult(t, p, "recovery")
	select {
	case held := <-cb.fenceHeld:
		if !held {
			t.Fatal("an unload completed while the recovery failure was still being reported")
		}
		select {
		case <-cb.fenced:
		case <-time.After(5 * time.Second):
			t.Fatal("the unload fence never passed after the report returned: the lease leaked")
		}
	default:
		t.Fatal("the failed recovery was not reported")
	}
}

// TestNode_TryAcquireWriteLeaseNeverWaitsBehindAnUnload: the step worker's
// panic recovery takes the lease, and a step worker owns a whole partition of
// shards. An unload already waiting for a long-running holder (a snapshot
// save) must make it refuse at once rather than queue behind that holder.
func TestNode_TryAcquireWriteLeaseNeverWaitsBehindAnUnload(t *testing.T) {
	node := newFenceTestNode(1)
	if !node.AcquireWriteLease() { // the long-running holder
		t.Fatal("a running node refused a write lease")
	}
	fenced := make(chan struct{})
	go func() {
		node.Stop()
		node.awaitWriteLeases()
		close(fenced)
	}()
	waitFor(t, 5*time.Second, time.Millisecond, "node never stopped", node.IsStopped)

	tried := make(chan bool, 1)
	go func() { tried <- node.TryAcquireWriteLease() }()
	select {
	case got := <-tried:
		if got {
			t.Fatal("a stopping node granted a write lease")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TryAcquireWriteLease blocked behind the unload waiting on another holder")
	}
	node.ReleaseWriteLease()
	<-fenced
}

// TestSnapshotPool_SaveReportIsFencedByUnload: a save's completion — handed to
// the shard's pending snapshot requests by shard ID — was reported after the
// save had released its lease, so an unload and a new incarnation's request
// could land in between and receive this save's result. It is reported under
// the node's write lease, as a recovery's is.
func TestSnapshotPool_SaveReportIsFencedByUnload(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(100), t.TempDir(), nil)
	cb := &completionProbeCallback{node: node, fenceHeld: make(chan bool, 1), fenced: make(chan struct{})}
	p := newSnapshotPool(2, newSnapshotTestLogDB(), nil, cb, nil, stopC, nil)
	var wg sync.WaitGroup
	p.start(&wg)

	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node, save: true}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}
	awaitSnapshotResult(t, p, "snapshot")
	select {
	case held := <-cb.fenceHeld:
		if !held {
			t.Fatal("an unload completed while the save's completion was still being reported")
		}
	default:
		t.Fatal("the save's completion was not reported")
	}
	select {
	case <-cb.fenced:
	case <-time.After(5 * time.Second):
		t.Fatal("the unload fence never passed after the report returned: the lease leaked")
	}
}

// completionProbeCallback is fenceProbeCallback for OnSnapshotCompleted.
type completionProbeCallback struct {
	testCallback
	node      *Node
	fenceHeld chan bool
	fenced    chan struct{}
}

func (c *completionProbeCallback) OnSnapshotCompleted(shardID, index uint64, op SnapshotOperation, err error) {
	c.testCallback.OnSnapshotCompleted(shardID, index, op, err)
	fenced := c.fenced
	go func() {
		c.node.Stop()
		c.node.awaitWriteLeases()
		close(fenced)
	}()
	select {
	case <-fenced:
		c.fenceHeld <- false
	case <-time.After(50 * time.Millisecond):
		c.fenceHeld <- true
	}
}
