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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// pauseApplier records the index of every applied entry that carries a
// command, and can hold an Apply call open until released.
type pauseApplier struct {
	mu      sync.Mutex
	indices []uint64

	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newPauseApplier() *pauseApplier {
	return &pauseApplier{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (a *pauseApplier) Apply(entries []proto.Entry, results []sm.Result) error {
	if a.block.Load() {
		select {
		case a.entered <- struct{}{}:
		default:
		}
		<-a.release
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			a.indices = append(a.indices, entries[i].Index)
		}
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

func (a *pauseApplier) applied() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.indices...)
}

func (a *pauseApplier) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.indices)
}

// pauseCluster is a 3-replica testCluster whose replicas apply through
// pauseAppliers.
func pauseCluster(t *testing.T) (*testCluster, map[uint64]*pauseApplier) {
	t.Helper()
	appliers := map[uint64]*pauseApplier{1: newPauseApplier(), 2: newPauseApplier(), 3: newPauseApplier()}
	c := newTestCluster(t, 1, 3, withApplier(func(id uint64) Applier { return appliers[id] }))
	return c, appliers
}

// proposeOn proposes n commands on replica id. Keys continue from *key.
func proposeOn(t *testing.T, c *testCluster, id uint64, n int, key *uint64) {
	t.Helper()
	r := c.replicas[id]
	for range n {
		*key++
		if !r.node.DeliverProposal([]proto.Entry{{Key: *key, Cmd: []byte("pause")}}) {
			t.Fatalf("replica %d refused proposal %d", id, *key)
		}
		r.engine.NotifyWork(c.shardID)
	}
}

// waitApplied waits for a to have applied at least n commands.
func waitApplied(t *testing.T, a *pauseApplier, n int, what string) {
	t.Helper()
	waitFor(t, 15*time.Second, 5*time.Millisecond, fmt.Sprintf("%s: applied fewer than %d commands", what, n),
		func() bool { return a.count() >= n })
}

// waitDiscarded waits until the leader has sent paused replica id several
// messages since the call and its inbox is empty again: step cycles have run
// over the paused node with the cluster's traffic, and discarded it.
func waitDiscarded(t *testing.T, c *testCluster, id uint64, what string) {
	t.Helper()
	var sent atomic.Int64
	c.router.setDrop(func(m proto.Message) bool {
		if m.To == id {
			sent.Add(1)
		}
		return false
	})
	defer c.router.setDrop(nil)
	waitFor(t, 15*time.Second, 5*time.Millisecond, what+": paused replica never received and discarded traffic",
		func() bool { return sent.Load() >= 3 && c.replicas[id].node.DiagState().InboxLen == 0 })
}

func assertLogOrder(t *testing.T, indices []uint64) {
	t.Helper()
	for i := 1; i < len(indices); i++ {
		if indices[i] <= indices[i-1] {
			t.Fatalf("applied out of log order at %d: %v", i, indices)
		}
	}
}

func pauseCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func otherReplicas(c *testCluster, skip uint64) []uint64 {
	var ids []uint64
	for id := range c.replicas {
		if id != skip {
			ids = append(ids, id)
		}
	}
	return ids
}

// A paused follower applies nothing while the rest of the shard commits,
// then applies every entry in log order once resumed.
func TestPause_FollowerAppliesNothingUntilResume(t *testing.T) {
	c, appliers := pauseCluster(t)
	leader := c.waitForLeader(t, 15*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	others := otherReplicas(c, leader)
	follower, witness := others[0], others[1]
	var key uint64

	proposeOn(t, c, leader, 5, &key)
	waitApplied(t, appliers[follower], 5, "follower before the pause")

	f := c.replicas[follower]
	if err := f.engine.PauseNode(pauseCtx(t, 10*time.Second), f.node); err != nil {
		t.Fatalf("PauseNode: %v", err)
	}
	if !f.node.IsPaused() {
		t.Fatal("IsPaused false after PauseNode")
	}
	before := appliers[follower].count()
	appliedBefore := f.node.lastApplied.Load()

	proposeOn(t, c, leader, 20, &key)
	waitApplied(t, appliers[witness], 25, "unpaused follower")
	waitDiscarded(t, c, follower, "paused follower")
	if got := appliers[follower].count(); got != before {
		t.Fatalf("paused follower applied %d commands while paused", got-before)
	}
	if got := f.node.lastApplied.Load(); got != appliedBefore {
		t.Fatalf("paused follower lastApplied moved %d -> %d", appliedBefore, got)
	}

	f.engine.ResumeNode(f.node)
	if f.node.IsPaused() {
		t.Fatal("IsPaused true after ResumeNode")
	}
	waitApplied(t, appliers[follower], 25, "resumed follower")
	got := appliers[follower].applied()
	assertLogOrder(t, got)
	want := appliers[witness].applied()
	for i := range want[:25] {
		if got[i] != want[i] {
			t.Fatalf("resumed follower applied %v, want %v", got, want)
		}
	}
}

// A paused leader stops heartbeating, so the other voters elect a leader of
// their own and keep committing; resumed, it rejoins as a follower and
// catches up.
func TestPause_LeaderStopsAndRejoinsAsFollower(t *testing.T) {
	c, appliers := pauseCluster(t)
	leader := c.waitForLeader(t, 15*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	var key uint64
	proposeOn(t, c, leader, 3, &key)
	waitApplied(t, appliers[leader], 3, "leader before the pause")

	l := c.replicas[leader]
	if err := l.engine.PauseNode(pauseCtx(t, 10*time.Second), l.node); err != nil {
		t.Fatalf("PauseNode: %v", err)
	}
	before := appliers[leader].count()

	newLeader := c.waitForLeader(t, 15*time.Second, leader)
	if newLeader == 0 {
		t.Fatal("no new leader elected while the old one was paused")
	}
	proposeOn(t, c, newLeader, 10, &key)
	waitApplied(t, appliers[newLeader], 13, "new leader")
	if got := appliers[leader].count(); got != before {
		t.Fatalf("paused leader applied %d commands while paused", got-before)
	}

	l.engine.ResumeNode(l.node)
	waitApplied(t, appliers[leader], 13, "resumed old leader")
	assertLogOrder(t, appliers[leader].applied())
	if l.node.Peer().IsLeader() && c.waitForLeader(t, time.Second, leader) != 0 {
		t.Fatal("two leaders after the resume")
	}
}

// PauseNode waits for an apply already running; on ctx expiry it returns
// ctx's error and the node stays paused. The batch already handed to the
// apply goroutine completes, and nothing more is applied until the resume.
func TestPause_WaitsForInFlightApply(t *testing.T) {
	c, appliers := pauseCluster(t)
	leader := c.waitForLeader(t, 15*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	follower := otherReplicas(c, leader)[0]
	fa := appliers[follower]
	f := c.replicas[follower]
	var key uint64

	fa.block.Store(true)
	proposeOn(t, c, leader, 1, &key)
	select {
	case <-fa.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("follower apply never started")
	}

	err := f.engine.PauseNode(pauseCtx(t, 100*time.Millisecond), f.node)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PauseNode with an apply in flight = %v, want context.DeadlineExceeded", err)
	}
	if !f.node.IsPaused() {
		t.Fatal("a pause whose ctx expired must leave the node paused")
	}

	fa.block.Store(false)
	close(fa.release)
	if err := f.engine.PauseNode(pauseCtx(t, 10*time.Second), f.node); err != nil {
		t.Fatalf("second PauseNode: %v", err)
	}
	before := fa.count()
	proposeOn(t, c, leader, 5, &key)
	waitApplied(t, appliers[leader], 6, "leader")
	waitDiscarded(t, c, follower, "paused follower")
	if got := fa.count(); got != before {
		t.Fatalf("paused follower applied %d commands while paused", got-before)
	}
	f.engine.ResumeNode(f.node)
	waitApplied(t, fa, 6, "resumed follower")
	assertLogOrder(t, fa.applied())
}

// PauseNode and ResumeNode are idempotent.
func TestPause_Idempotent(t *testing.T) {
	c, appliers := pauseCluster(t)
	leader := c.waitForLeader(t, 15*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	follower := otherReplicas(c, leader)[0]
	f := c.replicas[follower]

	f.engine.ResumeNode(f.node) // not paused: a no-op
	for i := range 3 {
		if err := f.engine.PauseNode(pauseCtx(t, 10*time.Second), f.node); err != nil {
			t.Fatalf("PauseNode #%d: %v", i, err)
		}
	}
	for range 3 {
		f.engine.ResumeNode(f.node)
	}
	if f.node.IsPaused() {
		t.Fatal("paused after ResumeNode")
	}
	var key uint64
	proposeOn(t, c, leader, 3, &key)
	waitApplied(t, appliers[follower], 3, "follower after pause/resume cycles")
}

// A snapshot request taken off the queue for a paused shard is refused
// without running: a recovery is re-armed without counting an attempt — any
// number of refusals never reaches the shard-failure limit — an automatic
// save is remembered for the resume, and a requested save is answered
// ErrShardPaused. Each releases the shard's slot.
func TestPause_SnapshotPoolRefusesPausedRequests(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	cb := newTestCallback()
	pool := newSnapshotPool(1, newTestLogDB(), nil, cb, NewWorkSignal(1), stopC, nil)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, t.TempDir(), nil)
	node.pause()

	submit := func(req snapshotRequest) {
		t.Helper()
		node.snapshotting.Store(true)
		pool.inFlight.Store(uint64(1), node)
		pool.handleRequest(req)
		if pool.holdsSlot(node) {
			t.Fatal("refused request left the slot claimed")
		}
		if node.snapshotting.Load() || node.IsRecovering() || node.IsSaving() {
			t.Fatal("refused request left a snapshot flag set")
		}
		select {
		case res := <-pool.CompletedC():
			t.Fatalf("refused request produced a completion: %+v", res)
		default:
		}
	}

	for range 2 * maxRecoveryAttempts {
		submit(snapshotRequest{shardID: 1, replicaID: 1, node: node})
		if !node.redrive.armed.Load() {
			t.Fatal("refused recovery was not re-armed")
		}
	}
	node.redrive.mu.Lock()
	rejected := node.redrive.rejected
	node.redrive.mu.Unlock()
	if rejected != 0 {
		t.Fatalf("refused recoveries counted %d rejections, want 0", rejected)
	}
	if node.IsFailed() || cb.shardFailedCount.Load() != 0 {
		t.Fatal("refused recoveries failed the shard")
	}

	submit(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: true})
	if !node.pausedSaveDropped.Load() {
		t.Fatal("refused automatic save was not remembered for the resume")
	}

	submit(snapshotRequest{shardID: 1, replicaID: 1, node: node, save: true, requestKey: 9})
	cb.mu.Lock()
	calls := append([]requestCall(nil), cb.requestCalls...)
	cb.mu.Unlock()
	if len(calls) != 1 || calls[0].requestKey != 9 || !errors.Is(calls[0].err, ErrShardPaused) {
		t.Fatalf("requested save completions = %+v, want one ErrShardPaused for key 9", calls)
	}
}

// A snapshot transfer registered before the pause holds PauseNode until it
// ends; once paused, no transfer is admitted.
func TestPause_WaitsForSnapshotTransfer(t *testing.T) {
	e := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if !node.BeginSnapshotTransfer() {
		t.Fatal("transfer refused on an unpaused node")
	}
	err := e.PauseNode(pauseCtx(t, 50*time.Millisecond), node)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PauseNode with a transfer in flight = %v, want context.DeadlineExceeded", err)
	}
	node.EndSnapshotTransfer()
	if err := e.PauseNode(pauseCtx(t, 5*time.Second), node); err != nil {
		t.Fatalf("PauseNode after the transfer ended: %v", err)
	}
	if node.BeginSnapshotTransfer() {
		t.Fatal("transfer admitted on a paused node")
	}
	if n := node.snapshotTransfers.Load(); n != 0 {
		t.Fatalf("refused transfer left %d registrations", n)
	}
	e.ResumeNode(node)
	if !node.BeginSnapshotTransfer() {
		t.Fatal("transfer refused after the resume")
	}
	node.EndSnapshotTransfer()
}

// TestPause_AwaitEndsWhenResumed: BeginPause marks the node at once, and an
// AwaitPause still waiting for in-flight work returns nil once ResumeNode
// undoes the pause, rather than waiting for a running node to settle.
func TestPause_AwaitEndsWhenResumed(t *testing.T) {
	e := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if !node.BeginSnapshotTransfer() {
		t.Fatal("transfer refused on an unpaused node")
	}
	defer node.EndSnapshotTransfer()
	e.BeginPause(node)
	if !node.IsPaused() {
		t.Fatal("BeginPause did not mark the node paused")
	}
	done := make(chan error, 1)
	go func() { done <- e.AwaitPause(pauseCtx(t, 5*time.Second), node) }()
	select {
	case err := <-done:
		t.Fatalf("AwaitPause returned %v with a transfer in flight", err)
	case <-time.After(20 * time.Millisecond):
	}
	e.ResumeNode(node)
	if err := <-done; err != nil {
		t.Fatalf("AwaitPause after ResumeNode = %v, want nil", err)
	}
}

// An AwaitPause whose ctx is already done on entry returns ctx.Err(), even
// when the pause has nothing in flight and no step-worker iteration is in
// progress: ctx's error is decided before either is looked at, never raced
// against them (main run 52823).
func TestPause_AwaitDoneCtxReturnsCtxErr(t *testing.T) {
	e := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)

	e.BeginPause(node)
	if err := e.AwaitPause(pauseCtx(t, 5*time.Second), node); err != nil {
		t.Fatalf("AwaitPause with nothing in flight = %v, want nil", err)
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	for i := range 100 {
		if err := e.AwaitPause(done, node); !errors.Is(err, context.Canceled) {
			t.Fatalf("AwaitPause(done ctx) #%d on a settled pause = %v, want context.Canceled", i, err)
		}
	}
	if !node.IsPaused() {
		t.Fatal("AwaitPause(done ctx) left the node unpaused")
	}
}

// ResumeNode re-requests an automatic snapshot the pause refused if it is
// still due.
func TestPause_ResumeRequestsDroppedSave(t *testing.T) {
	e := NewEngine(newTestHostConfig(), newTestLogDB(), newTestSender(), nil, nil, nil)
	cfg := config.Config{ShardID: 1, ReplicaID: 1, SnapshotEntries: 10}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, t.TempDir(), nil)
	node.lastApplied.Store(50)
	if err := e.PauseNode(pauseCtx(t, 5*time.Second), node); err != nil {
		t.Fatalf("PauseNode: %v", err)
	}
	node.pausedSaveDropped.Store(true)
	e.ResumeNode(node)
	if node.pausedSaveDropped.Load() {
		t.Fatal("dropped-save marker survived the resume")
	}
	if !node.snapshotting.Load() || !e.snapshots.holdsSlot(node) {
		t.Fatal("the due save was not requested again on resume")
	}
}
