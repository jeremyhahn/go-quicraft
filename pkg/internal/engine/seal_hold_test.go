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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// errTestSealed stands in for the barrier's sealed refusal.
var errTestSealed = errors.New("test: barrier sealed")

// sealingLogDB refuses, as sealed, any SaveState carrying sealedShard while
// sealed is set — the shape of a multiplexing LogDB whose shards have their
// own barriers. Every other write goes through.
type sealingLogDB struct {
	*testLogDB
	sealedShard uint64
	sealed      atomic.Bool
	refused     atomic.Int64
}

func (db *sealingLogDB) SaveState(updates []logdb.Update) error {
	if db.sealed.Load() {
		for _, u := range updates {
			if u.ShardID == db.sealedShard {
				db.refused.Add(1)
				return fmt.Errorf("wal: %w", errTestSealed)
			}
		}
	}
	return db.testLogDB.SaveState(updates)
}

func (db *sealingLogDB) savedShards() map[uint64]int {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make(map[uint64]int)
	for _, u := range db.saved {
		out[u.ShardID]++
	}
	return out
}

type sealHoldFixture struct {
	cw      *commitWorker
	ldb     *sealingLogDB
	cb      *testCallback
	ws      *WorkSignal
	stopC   chan struct{}
	unsealC chan struct{}
}

func newSealHoldFixture(t *testing.T, sealedShard uint64) *sealHoldFixture {
	t.Helper()
	f := &sealHoldFixture{
		ldb:     &sealingLogDB{testLogDB: newTestLogDB(), sealedShard: sealedShard},
		cb:      newTestCallback(),
		ws:      NewWorkSignal(1),
		stopC:   make(chan struct{}),
		unsealC: make(chan struct{}),
	}
	f.ldb.sealed.Store(true)
	f.cw = newCommitWorker(0, f.ldb, newTestSender(), make(chan *commitItem, 16), make(chan *applyItem, 16),
		&sync.Pool{New: func() interface{} { return &applyItem{} }}, f.ws, f.cb, f.stopC, nil)
	f.cw.noRetryDelay = true
	f.cw.sealHold = SealHold{
		IsSealed:  func(err error) bool { return errors.Is(err, errTestSealed) },
		UnsealedC: func(uint64) <-chan struct{} { return f.unsealC },
	}
	t.Cleanup(func() {
		select {
		case <-f.stopC:
		default:
			close(f.stopC)
		}
	})
	return f
}

// newLeaderNode returns a single-voter leader for shardID and the update
// carrying its election (hard state and the leader's first entry).
func newLeaderNode(t *testing.T, shardID uint64) (*Node, proto.Update) {
	t.Helper()
	peer, lr := newTestPeer(t, shardID, 1)
	tickPeerToLeader(peer)
	cfg := config.Config{ShardID: shardID, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	u, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate(shard %d): %v", shardID, err)
	}
	if u.ReadOnly || len(u.EntriesToSave) == 0 {
		t.Fatalf("fixture: shard %d update ReadOnly=%v entries=%d, want the election to persist",
			shardID, u.ReadOnly, len(u.EntriesToSave))
	}
	node.commitPending.Store(true)
	return node, u
}

func (f *sealHoldFixture) submit(nodes []*Node, updates []proto.Update) {
	f.cw.batch = append(f.cw.batch[:0], updates...)
	f.cw.batchNodes = append(f.cw.batchNodes[:0], nodes...)
	f.cw.batchUnpersisted = true
}

// awaitWake waits for a held shard's waiter to fire and releases it as the
// run loop would.
func (f *sealHoldFixture) awaitWake(t *testing.T) {
	t.Helper()
	select {
	case <-f.cw.sealWakeC:
	case <-time.After(5 * time.Second):
		t.Fatal("held shard's waiter never woke the worker")
	}
	f.cw.releaseWoken()
}

func entryIDs(es []proto.Entry) [][2]uint64 {
	out := make([][2]uint64, len(es))
	for i, e := range es {
		out[i] = [2]uint64{e.Index, e.Term}
	}
	return out
}

// A sealed refusal on a shared commit worker parks only the sealed shard: the
// other shard in the same batch persists and commits, the sealed one is not
// failed, and on unseal it re-derives the identical update and persists it.
func TestSealHold_SharedWorker_OnlySealedShardHeld(t *testing.T) {
	f := newSealHoldFixture(t, 1)
	nodeA, uA := newLeaderNode(t, 1)
	nodeB, uB := newLeaderNode(t, 2)
	wantA := entryIDs(uA.EntriesToSave)

	f.submit([]*Node{nodeA, nodeB}, []proto.Update{uA, uB})
	f.cw.processBatch()

	if n := f.cb.shardFailedCount.Load(); n != 0 {
		t.Fatalf("a sealed refusal failed %d shard(s); it must park the shard", n)
	}
	if nodeA.IsFailed() {
		t.Fatal("sealed shard A latched failed")
	}
	if !nodeA.commitPending.Load() || !nodeA.sealHeld.Load() {
		t.Fatalf("shard A commitPending=%v sealHeld=%v, want it held", nodeA.commitPending.Load(), nodeA.sealHeld.Load())
	}
	if nodeB.sealHeld.Load() || nodeB.IsFailed() {
		t.Fatal("unsealed shard B was held or failed with A")
	}
	saved := f.ldb.savedShards()
	if saved[2] == 0 {
		t.Fatal("shard B's update was not persisted while A was sealed")
	}
	if saved[1] != 0 {
		t.Fatal("shard A's refused update was recorded as persisted")
	}
	if len(f.cw.held) != 1 || f.cw.held[0].node != nodeA {
		t.Fatalf("held=%d after the batch, want shard A alone", len(f.cw.held))
	}
	for i, n := range f.cw.batchNodes {
		if n != nil {
			t.Fatalf("batchNodes[%d] still references a node after the batch settled", i)
		}
	}

	// Still sealed: the waiter must not fire.
	select {
	case <-f.cw.sealWakeC:
		t.Fatal("held shard woke before the unseal")
	case <-time.After(50 * time.Millisecond):
	}

	f.ldb.sealed.Store(false)
	close(f.unsealC)
	f.awaitWake(t)
	if nodeA.sealHeld.Load() || nodeA.commitPending.Load() || len(f.cw.held) != 0 {
		t.Fatal("unseal did not release shard A")
	}

	// Raft was rewound, not advanced: the retried update is the same write.
	again, err := nodeA.peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate after release: %v", err)
	}
	if again.ReadOnly {
		t.Fatal("re-derived update is ReadOnly: the refused write would never reach the WAL")
	}
	if got := entryIDs(again.EntriesToSave); fmt.Sprint(got) != fmt.Sprint(wantA) {
		t.Fatalf("re-derived entries %v, want the refused write's %v", got, wantA)
	}
	if again.State.Term != uA.State.Term || again.State.Vote != uA.State.Vote {
		t.Fatalf("re-derived hard state %+v, want %+v", again.State, uA.State)
	}
	nodeA.commitPending.Store(true)
	f.submit([]*Node{nodeA}, []proto.Update{again})
	f.cw.processBatch()
	if f.ldb.savedShards()[1] == 0 || nodeA.IsFailed() || nodeA.sealHeld.Load() {
		t.Fatal("the retried write did not persist after the unseal")
	}
}

// A lone sealed update is held the same way, and a barrier with no unseal
// notification falls back to polling.
func TestSealHold_SingleItem_PollFallback(t *testing.T) {
	f := newSealHoldFixture(t, 1)
	f.cw.sealHold.UnsealedC = nil
	nodeA, uA := newLeaderNode(t, 1)

	f.submit([]*Node{nodeA}, []proto.Update{uA})
	f.cw.processBatch()
	if nodeA.IsFailed() || !nodeA.sealHeld.Load() || !nodeA.commitPending.Load() {
		t.Fatal("lone sealed update was not held")
	}
	if f.ldb.refused.Load() != 1 {
		t.Fatalf("sealed write attempted %d times, want 1 (no backoff retries)", f.ldb.refused.Load())
	}
	select {
	case <-f.cw.sealWakeC:
		f.cw.releaseWoken()
	case <-time.After(sealHoldPollInterval + 5*time.Second):
		t.Fatal("poll fallback never woke the worker")
	}
	if nodeA.sealHeld.Load() || nodeA.commitPending.Load() {
		t.Fatal("poll wake did not release the shard")
	}
}

// Only the exact sealed refusal is held: any other SaveState failure still
// latches the shard, and with no hold installed a sealed refusal does too.
func TestSealHold_OtherFailuresStillLatch(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	t.Run("disk fault", func(t *testing.T) {
		f := newSealHoldFixture(t, 99)
		f.ldb.saveErr = errors.New("fsync: input/output error")
		nodeA, uA := newLeaderNode(t, 1)
		f.submit([]*Node{nodeA}, []proto.Update{uA})
		f.cw.processBatch()
		if nodeA.sealHeld.Load() || len(f.cw.held) != 0 {
			t.Fatal("a disk fault took the sealed hold")
		}
		if f.cb.shardFailedCount.Load() != 1 {
			t.Fatalf("disk fault failed %d shards, want 1", f.cb.shardFailedCount.Load())
		}
	})
	t.Run("no hold installed", func(t *testing.T) {
		f := newSealHoldFixture(t, 1)
		f.cw.sealHold = SealHold{}
		nodeA, uA := newLeaderNode(t, 1)
		f.submit([]*Node{nodeA}, []proto.Update{uA})
		f.cw.processBatch()
		if nodeA.sealHeld.Load() || f.cb.shardFailedCount.Load() != 1 {
			t.Fatal("without a SealHold a sealed refusal must fail the shard as before")
		}
	})
	t.Run("mixed batch", func(t *testing.T) {
		// A sealed refusal of the batch, then a disk fault on one shard's
		// own retry: that shard fails, the sealed one is held.
		f := newSealHoldFixture(t, 1)
		nodeA, uA := newLeaderNode(t, 1)
		nodeB, uB := newLeaderNode(t, 2)
		f.ldb.saveErr = errors.New("disk error")
		f.submit([]*Node{nodeA, nodeB}, []proto.Update{uA, uB})
		f.cw.processBatch()
		if !nodeA.sealHeld.Load() || nodeA.IsFailed() {
			t.Fatal("sealed shard A not held")
		}
		if nodeB.sealHeld.Load() || f.cb.shardFailedCount.Load() != 1 {
			t.Fatalf("disk-faulted shard B: held=%v failed=%d, want failed once",
				nodeB.sealHeld.Load(), f.cb.shardFailedCount.Load())
		}
	})
}

// Stopping the shard or the worker wakes a held shard's waiter, and unloading
// the shard releases it, so neither stop nor UnloadNode waits on an unseal.
func TestSealHold_StopAndUnloadRelease(t *testing.T) {
	t.Run("worker stop", func(t *testing.T) {
		f := newSealHoldFixture(t, 1)
		nodeA, uA := newLeaderNode(t, 1)
		f.submit([]*Node{nodeA}, []proto.Update{uA})
		f.cw.processBatch()
		close(f.stopC)
		f.awaitWake(t)
		if nodeA.sealHeld.Load() {
			t.Fatal("worker stop left the shard held")
		}
	})
	t.Run("shard stop", func(t *testing.T) {
		f := newSealHoldFixture(t, 1)
		nodeA, uA := newLeaderNode(t, 1)
		f.submit([]*Node{nodeA}, []proto.Update{uA})
		f.cw.processBatch()
		nodeA.Stop()
		f.awaitWake(t)
		if nodeA.sealHeld.Load() {
			t.Fatal("shard stop left the shard held")
		}
	})
	t.Run("unload", func(t *testing.T) {
		f := newSealHoldFixture(t, 1)
		nodeA, uA := newLeaderNode(t, 1)
		f.submit([]*Node{nodeA}, []proto.Update{uA})
		f.cw.processBatch()
		f.cw.releaseUnloaded(1)
		if nodeA.sealHeld.Load() || nodeA.commitPending.Load() || len(f.cw.held) != 0 {
			t.Fatal("unload left the shard held")
		}
	})
}

// Engine stop joins every seal-hold waiter and releases every held shard
// before the commit tier returns: no waiter outlives the engine and no node is
// left marked sealHeld.
func TestSealHold_EngineStopJoinsWaiters(t *testing.T) {
	f := newSealHoldFixture(t, 1)
	nodeA, uA := newLeaderNode(t, 1)
	nodeA.commitPending.Store(true) // as the step worker does on handoff
	f.submit([]*Node{nodeA}, []proto.Update{uA})
	f.cw.processBatch()
	if len(f.cw.held) != 1 || !nodeA.sealHeld.Load() {
		t.Fatal("sealed refusal did not hold the shard")
	}
	held := f.cw.held[0]

	close(f.stopC)
	done := make(chan struct{})
	go func() {
		f.cw.run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("commit worker did not return on stop with a shard held")
	}
	// woken is the waiter's last write before it returns; run joined it.
	if !held.woken.Load() {
		t.Fatal("run returned before the held shard's waiter")
	}
	if nodeA.sealHeld.Load() || nodeA.commitPending.Load() || len(f.cw.held) != 0 {
		t.Fatal("engine stop left the shard held")
	}
}

// A held node is frozen: the step worker does not tick it, so raft cannot
// move (campaign, advance its term) while its last write is unpersisted.
func TestSealHold_StepWorkerDoesNotTickHeldNode(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), make(chan *commitItem, 16), nil, stopC, nil)
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	node.commitPending.Store(true)
	node.sealHeld.Store(true)
	sw.loadNode(node)
	for range 30 {
		sw.tick()
	}
	if peer.IsLeader() {
		t.Fatal("a held node was ticked into an election")
	}
}

// The piggyback path's hard-state write is refused by the sealed barrier:
// the shard is held, no ordered response leaves and nothing is applied, and
// after the unseal the re-derived update carries the same hard state, the
// same committed entries and the withheld vote, and persists.
func TestSealHold_PiggybackSealedRefusal(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	cw, applyC := newPiggybackCommitWorker(t, ldb, sender)

	peer, lr := newPiggybackFollower(t, ldb, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)

	// Entries 1-3 durable, then committed with nothing unsaved, and a vote
	// granted at term 2: the piggyback carries committed entries, the hard
	// state term 2 vote 3, and an ORDERED RequestVoteResp.
	replicateTo(t, peer, 0, 0, 0, []proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	})
	saveCycle, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate (cycle 1): %v", err)
	}
	node.commitPending.Store(true)
	cw.batch = append(cw.batch[:0], saveCycle)
	cw.batchNodes = []*Node{node}
	cw.processBatch()
	node.commitPending.Store(true)
	replicateTo(t, peer, 3, 1, 3, nil)
	if err := peer.Handle(proto.Message{
		Type: proto.RequestVote, ShardID: 1, From: 3, To: 1,
		Term: 2, LogIndex: 3, LogTerm: 1, Hint: 3,
	}); err != nil {
		t.Fatalf("Handle(RequestVote): %v", err)
	}
	piggyback, err := peer.GetPiggybackUpdate(node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetPiggybackUpdate: %v", err)
	}
	if len(piggyback.CommittedEntries) == 0 || piggyback.State.Vote != 3 || piggyback.State.Term != 2 {
		t.Fatalf("fixture: piggyback committed=%d state=%+v, want entries and term 2 vote 3",
			len(piggyback.CommittedEntries), piggyback.State)
	}
	wantCommitted := entryIDs(piggyback.CommittedEntries)
	wantState := piggyback.State
	// The sealed branch's ordered half is not empty: the hard-state-backed
	// VoteResp is in it, so holdSealed must requeue it, not drop it.
	var orderedVote bool
	for _, m := range piggyback.Messages {
		orderedVote = orderedVote || (m.Type == proto.RequestVoteResp && !proto.IsFreeOrderMessage(m.Type))
	}
	if !orderedVote {
		t.Fatal("fixture: the piggyback carries no ordered RequestVoteResp")
	}

	sealing := &sealingLogDB{testLogDB: ldb, sealedShard: 1}
	sealing.sealed.Store(true)
	cw.logdb = sealing
	unsealC := make(chan struct{})
	cw.sealHold = SealHold{
		IsSealed:  func(err error) bool { return errors.Is(err, errTestSealed) },
		UnsealedC: func(uint64) <-chan struct{} { return unsealC },
	}
	savedBefore := sealing.savedShards()[1]
	sentBefore := len(sender.sentMessages())

	if !cw.executePiggyback(node, piggyback) {
		t.Fatal("executePiggyback reported shutdown")
	}
	if node.IsFailed() || !node.sealHeld.Load() || !node.commitPending.Load() {
		t.Fatalf("piggyback refusal: failed=%v sealHeld=%v commitPending=%v, want held",
			node.IsFailed(), node.sealHeld.Load(), node.commitPending.Load())
	}
	if sealing.refused.Load() != 1 {
		t.Fatalf("sealed piggyback write attempted %d times, want 1", sealing.refused.Load())
	}
	if got := sealing.savedShards()[1]; got != savedBefore {
		t.Fatal("the refused piggyback hard state was recorded as persisted")
	}
	for _, m := range sender.sentMessages()[sentBefore:] {
		if !proto.IsFreeOrderMessage(m.Type) {
			t.Fatalf("ordered %v left the replica before its hard state was durable", m.Type)
		}
	}
	select {
	case ai := <-applyC:
		t.Fatalf("refused piggyback forwarded %d entries to apply", len(ai.update.CommittedEntries))
	default:
	}

	sealing.sealed.Store(false)
	close(unsealC)
	select {
	case <-cw.sealWakeC:
	case <-time.After(5 * time.Second):
		t.Fatal("held shard's waiter never woke the worker")
	}
	cw.releaseWoken()
	if node.sealHeld.Load() || node.commitPending.Load() {
		t.Fatal("unseal did not release the shard")
	}

	again, err := peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate after release: %v", err)
	}
	if again.ReadOnly || again.State != wantState {
		t.Fatalf("re-derived update ReadOnly=%v state=%+v, want the refused hard state %+v",
			again.ReadOnly, again.State, wantState)
	}
	if got := entryIDs(again.CommittedEntries); fmt.Sprint(got) != fmt.Sprint(wantCommitted) {
		t.Fatalf("re-derived committed entries %v, want %v", got, wantCommitted)
	}
	var voteResp bool
	for _, m := range again.Messages {
		voteResp = voteResp || (m.Type == proto.RequestVoteResp && !m.Reject)
	}
	if !voteResp {
		t.Fatal("the vote withheld by the refused write was not requeued")
	}
	node.commitPending.Store(true)
	cw.batch = append(cw.batch[:0], again)
	cw.batchNodes = append(cw.batchNodes[:0], node)
	cw.batchUnpersisted = true
	cw.processBatch()
	if sealing.savedShards()[1] == savedBefore || node.IsFailed() {
		t.Fatal("the retried write did not persist after the unseal")
	}
	select {
	case ai := <-applyC:
		if got := entryIDs(ai.update.CommittedEntries); fmt.Sprint(got) != fmt.Sprint(wantCommitted) {
			t.Fatalf("applied %v after the unseal, want %v", got, wantCommitted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("committed entries never reached apply after the unseal")
	}
}

// leaseEngine wires a step worker and a commit worker over one LeaseRead
// leader, moving each commit item across by hand so the test owns ordering.
type leaseEngine struct {
	sw      *stepWorker
	cw      *commitWorker
	node    *Node
	commitC chan *commitItem
	applyC  chan *applyItem
	ws      *WorkSignal
	sender  *testSender
	cb      *testCallback
	ldb     *sealingLogDB
	unsealC chan struct{}
}

// pump runs one step cycle and, if it produced a commit item, one commit
// cycle, standing in for the apply worker's commitPending release.
func (e *leaseEngine) pump(t *testing.T) {
	t.Helper()
	e.ws.Notify(e.node.shardID)
	e.sw.processReady()
	select {
	case item := <-e.commitC:
		e.cw.batch, e.cw.batchNodes = e.cw.batch[:0], e.cw.batchNodes[:0]
		e.cw.admit(item)
		e.cw.batchUnpersisted = true
		e.cw.processBatch()
	default:
	}
	for drained := false; !drained; {
		select {
		case ai := <-e.applyC:
			ai.update.ReleaseCommittedEntries()
		default:
			drained = true
		}
	}
	if !e.node.sealHeld.Load() {
		e.node.commitPending.Store(false)
	}
}

// lastHeartbeat returns the lease round and read context of the newest
// heartbeat the leader sent to replica 2.
func (e *leaseEngine) lastHeartbeat(t *testing.T) (round, ctx uint64) {
	t.Helper()
	msgs := e.sender.sentMessages()
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Type == proto.Heartbeat && msgs[i].To == 2 {
			return msgs[i].LogIndex, msgs[i].Hint
		}
	}
	t.Fatal("the leader sent no heartbeat")
	return 0, 0
}

// ack delivers HeartbeatResp from both followers for a lease round, each
// promising a vote window longer than any lease.
func (e *leaseEngine) ack(round, ctx uint64) {
	term, _, _, _, _, _ := e.node.peer.DiagRaftState()
	for _, from := range []uint64{2, 3} {
		e.node.Deliver(proto.Message{
			Type: proto.HeartbeatResp, From: from, To: 1, Term: term,
			LogIndex: round, LogTerm: uint64(time.Hour), Hint: ctx,
		})
	}
}

func (e *leaseEngine) served(key uint64) bool {
	e.cb.mu.Lock()
	defer e.cb.mu.Unlock()
	for _, c := range e.cb.readyToReadKeys {
		for _, r := range c.readyToRead {
			if r.Key == key {
				return true
			}
		}
	}
	return false
}

func newLeaseEngine(t *testing.T) *leaseEngine {
	t.Helper()
	const tick = 100 * time.Millisecond
	e := &leaseEngine{
		commitC: make(chan *commitItem, 16),
		applyC:  make(chan *applyItem, 16),
		ws:      NewWorkSignal(1),
		sender:  newTestSender(),
		cb:      newTestCallback(),
		ldb:     &sealingLogDB{testLogDB: newTestLogDB(), sealedShard: 1},
		unsealC: make(chan struct{}),
	}
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	e.sw = newStepWorker(0, tick, e.sender, e.ws, e.commitC, e.cb, stopC, nil)
	e.cw = newCommitWorker(0, e.ldb, e.sender, make(chan *commitItem, 16), e.applyC,
		&sync.Pool{New: func() any { return &applyItem{} }}, e.ws, e.cb, stopC, nil)
	e.cw.noRetryDelay = true
	e.cw.sealHold = SealHold{
		IsSealed:  func(err error) bool { return errors.Is(err, errTestSealed) },
		UnsealedC: func(uint64) <-chan struct{} { return e.unsealC },
	}

	lr := logdb.NewLogReader(1, 1, e.ldb)
	if err := lr.ApplySnapshot(proto.Snapshot{Membership: proto.Membership{Addresses: map[uint64]string{
		1: "localhost:10001", 2: "localhost:10002", 3: "localhost:10003",
	}}}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	cfg := config.Config{ShardID: 1, ReplicaID: 1, PreVote: config.BoolPtr(false), LeaseRead: true}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	peer.SetTickInterval(tick)
	// A LeaseRead replica holds even its own vote down for one election
	// span of real time after it starts; campaign once that has passed.
	waitFor(t, 10*time.Second, 5*time.Millisecond, "fixture: the peer never campaigned", func() bool {
		if term, _, _, _, _, _ := peer.DiagRaftState(); term != 0 {
			return true
		}
		peer.Tick()
		return false
	})
	_ = peer.DrainMessages()
	term, _, _, _, _, _ := peer.DiagRaftState()
	for _, from := range []uint64{2, 3} {
		_ = peer.Handle(proto.Message{Type: proto.RequestVoteResp, From: from, Term: term})
	}
	if !peer.IsLeader() {
		t.Fatal("fixture: peer must be leader")
	}
	_ = peer.DrainMessages()
	_, _, _, last, _, _ := peer.DiagRaftState()
	for _, from := range []uint64{2, 3} {
		_ = peer.Handle(proto.Message{Type: proto.ReplicateResp, From: from, LogIndex: last, Term: term})
	}
	_ = peer.DrainMessages()

	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	e.node = NewNode(peer, newTestApplier(newTestSM()), lr, nodeCfg, nil, "", nil)
	e.sw.loadNode(e.node)
	e.node.commitPending.Store(true)
	e.pump(t) // persists the election and the no-op, which commits it
	return e
}

// A leader held on a sealed refusal serves no LeaseRead from the lease it
// held before the hold: not while held, not after the unseal, and not on a
// late ack of a pre-hold heartbeat round. Only acks of a round sent after
// the release renew it. Driven through the step and commit workers.
func TestSealHold_LeaseNotServedAcrossHold(t *testing.T) {
	_, restore := captureLogOutput(t)
	t.Cleanup(restore)
	e := newLeaseEngine(t)

	// Establish the lease: a ReadIndex confirmed by a heartbeat quorum.
	e.node.DeliverReadIndex(100)
	e.pump(t)
	preHoldRound, ctx := e.lastHeartbeat(t)
	if preHoldRound == 0 {
		t.Fatal("fixture: heartbeat carried no lease round; LeaseRead is off")
	}
	e.ack(preHoldRound, ctx)
	e.pump(t)
	if !e.served(100) {
		t.Fatal("fixture: the quorum-confirmed read was not served")
	}
	// Positive control: with the lease held, a read is served at once.
	e.node.DeliverReadIndex(101)
	e.pump(t)
	if !e.served(101) {
		t.Fatal("fixture: the lease did not serve a read; the checks below would prove nothing")
	}

	// A proposal's write is refused: the leader is held.
	e.ldb.sealed.Store(true)
	if !e.node.DeliverProposal([]proto.Entry{{Cmd: []byte("x")}}) {
		t.Fatal("fixture: proposal queue full")
	}
	e.pump(t)
	if !e.node.sealHeld.Load() || e.node.IsFailed() {
		t.Fatal("fixture: the sealed refusal did not hold the leader")
	}

	// While held: no read is served.
	e.node.DeliverReadIndex(200)
	e.pump(t)
	if e.served(200) {
		t.Fatal("a held leader served a read")
	}

	// Unseal and release; the retried write persists.
	e.ldb.sealed.Store(false)
	close(e.unsealC)
	select {
	case <-e.cw.sealWakeC:
	case <-time.After(5 * time.Second):
		t.Fatal("held shard's waiter never woke the worker")
	}
	e.cw.releaseWoken()
	e.pump(t)
	e.pump(t)
	if e.node.sealHeld.Load() || e.node.IsFailed() {
		t.Fatal("the leader was not released")
	}
	if e.served(200) {
		t.Fatal("after the hold a read was served from the pre-hold lease")
	}

	// A late ack of the pre-hold round must not renew the lease.
	e.ack(preHoldRound, 0)
	e.pump(t)
	e.node.DeliverReadIndex(300)
	e.pump(t)
	if e.served(300) {
		t.Fatal("a late ack of a heartbeat round sent before the hold renewed the lease")
	}

	// Acks of a round sent after the release renew it.
	freshRound, _ := e.lastHeartbeat(t)
	if freshRound <= preHoldRound {
		t.Fatalf("heartbeat round after the hold = %d, want > %d", freshRound, preHoldRound)
	}
	e.ack(freshRound, 0)
	e.pump(t)
	e.node.DeliverReadIndex(400)
	e.pump(t)
	if !e.served(400) {
		t.Fatal("acks of a fresh heartbeat round did not renew the lease")
	}
}
