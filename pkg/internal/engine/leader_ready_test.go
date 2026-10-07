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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// readyFixture is one step worker owning one replica of a three-voter shard,
// driven by hand on the test goroutine: every tick, processReady and commit
// happens exactly when the test says so, so the event sequence it asserts is
// deterministic and needs no sleeps.
type readyFixture struct {
	t       *testing.T
	sw      *stepWorker
	cb      *testCallback
	peer    *raft.Peer
	node    *Node
	commitC chan *commitItem
	// lastSaved is the last log index persist made durable.
	lastSaved uint64
}

func newReadyFixture(t *testing.T) *readyFixture {
	t.Helper()
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), commitC, cb, stopC, nil)
	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)
	return &readyFixture{t: t, sw: sw, cb: cb, peer: peer, node: node, commitC: commitC}
}

// persist plays the commit worker for every update the step worker queued:
// the entries become durable, which is half of what the leader's commit needs.
func (f *readyFixture) persist() {
	f.t.Helper()
	for {
		select {
		case item := <-f.commitC:
			if err := f.peer.Commit(item.update); err != nil {
				f.t.Fatalf("Commit: %v", err)
			}
			if n := len(item.update.EntriesToSave); n > 0 {
				f.lastSaved = item.update.EntriesToSave[n-1].Index
			}
			f.node.commitPending.Store(false)
		default:
			return
		}
	}
}

// ackFollower delivers replica 2's acknowledgement of everything persisted so
// far: the other half of the commit.
func (f *readyFixture) ackFollower() {
	f.t.Helper()
	_, term := f.peer.LeaderAndTerm()
	if err := f.peer.Handle(proto.Message{
		Type: proto.ReplicateResp, From: 2, To: 1, Term: term, LogIndex: f.lastSaved,
	}); err != nil {
		f.t.Fatalf("ReplicateResp: %v", err)
	}
}

// cycle runs the step worker once: one processReady, then one tick.
func (f *readyFixture) cycle() {
	f.sw.processReady()
	f.sw.tick()
}

// readIndexNotReady reports whether a ReadIndex stepped on the peer now is
// refused with ErrReadIndexNotReady.
func (f *readyFixture) readIndexNotReady(key uint64) bool {
	return errors.Is(f.peer.ReadIndex(key), raft.ErrReadIndexNotReady)
}

func describe(events []leaderEvent) string {
	s := ""
	for _, e := range events {
		kind := "updated"
		if e.ready {
			kind = "ready"
		}
		s += fmt.Sprintf("[%s leader=%d term=%d] ", kind, e.leaderID, e.term)
	}
	return s
}

// assertEvents checks the exact delivered sequence of leadership events.
func (f *readyFixture) assertEvents(want ...leaderEvent) {
	f.t.Helper()
	got := f.cb.getLeaderEvents()
	if len(got) != len(want) {
		f.t.Fatalf("leadership events:\n got  %s\n want %s", describe(got), describe(want))
	}
	for i := range want {
		if got[i].ready != want[i].ready || got[i].leaderID != want[i].leaderID ||
			got[i].term != want[i].term {
			f.t.Fatalf("leadership events:\n got  %s\n want %s", describe(got), describe(want))
		}
		if got[i].ready && got[i].replicaID != 1 {
			f.t.Fatalf("OnLeaderReady reported replica %d, want 1", got[i].replicaID)
		}
	}
}

func updated(leaderID, term uint64) leaderEvent {
	return leaderEvent{leaderCall: leaderCall{shardID: 1, leaderID: leaderID, term: term}}
}

func ready(leaderID, term uint64) leaderEvent {
	return leaderEvent{leaderCall: leaderCall{shardID: 1, leaderID: leaderID, term: term}, ready: true}
}

// reelect makes replica 1, a follower of replica 2, time out and win the next
// election with replica 2's votes. driveToLeader stops at any known leader, so
// it cannot start from a follower.
func reelect(t *testing.T, peer *raft.Peer) {
	t.Helper()
	for range 200 {
		if peer.IsLeader() {
			return
		}
		peer.Tick()
		term := peer.Term()
		if err := peer.Handle(proto.Message{
			Type: proto.RequestPreVoteResp, From: 2, To: 1, Term: term + 1,
		}); err != nil {
			t.Fatalf("RequestPreVoteResp: %v", err)
		}
		if err := peer.Handle(proto.Message{
			Type: proto.RequestVoteResp, From: 2, To: 1, Term: peer.Term(),
		}); err != nil {
			t.Fatalf("RequestVoteResp: %v", err)
		}
	}
	t.Fatalf("replica 1 was not re-elected (leaderID=%d, term=%d)", peer.LeaderID(), peer.Term())
}

// OnLeaderReady fires once per term, after the leader's first commit of the
// term and not before, and ReadIndex agrees with it at every step.
func TestStepWorker_LeaderReady_OncePerTermAfterFirstCommit(t *testing.T) {
	f := newReadyFixture(t)
	driveToLeader(t, f.peer, 1, 2)
	_, term := f.peer.LeaderAndTerm()

	// Elected: OnLeaderUpdated, but the no-op is neither durable nor acked.
	f.cycle()
	f.assertEvents(updated(1, term))
	if !f.readIndexNotReady(1) {
		t.Fatal("fixture: ReadIndex served before the no-op committed")
	}

	// Durable locally but not acknowledged: still not committed.
	f.persist()
	f.cycle()
	f.assertEvents(updated(1, term))
	if !f.readIndexNotReady(2) {
		t.Fatal("fixture: ReadIndex served before the no-op committed")
	}

	// The acknowledgement commits the no-op: the next cycle announces it.
	f.ackFollower()
	f.cycle()
	f.assertEvents(updated(1, term), ready(1, term))
	if f.readIndexNotReady(3) {
		t.Fatal("OnLeaderReady fired but ReadIndex still answers ErrReadIndexNotReady")
	}

	// Further commits and cycles in the same term announce nothing more.
	f.persist()
	for range 3 {
		f.cycle()
		f.persist()
	}
	f.assertEvents(updated(1, term), ready(1, term))
	if got := f.cb.leaderReadyCount.Load(); got != 1 {
		t.Fatalf("OnLeaderReady fired %d times in one term, want 1", got)
	}
}

// A same-leader re-election at a higher term ends the old term's readiness
// with an OnLeaderUpdated and announces the new term's only after its own
// first commit — the old term's commits say nothing about the new term.
func TestStepWorker_LeaderReady_SameLeaderReelectedAtHigherTerm(t *testing.T) {
	f := newReadyFixture(t)
	driveToLeader(t, f.peer, 1, 2)
	_, t1 := f.peer.LeaderAndTerm()
	f.cycle()
	f.persist()
	f.ackFollower()
	f.cycle()
	f.assertEvents(updated(1, t1), ready(1, t1))

	// Deposed by replica 2 leading a higher term, then re-elected.
	if err := f.peer.Handle(proto.Message{
		Type: proto.Heartbeat, From: 2, To: 1, Term: t1 + 1,
	}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	f.cycle()
	f.persist()
	if f.peer.IsLeader() {
		t.Fatal("fixture: a higher-term heartbeat did not depose the leader")
	}
	// Replica 2 leads t1+1 but this replica holds no commit of that term, so
	// it is not told replica 2 is read-ready.
	f.assertEvents(updated(1, t1), ready(1, t1), updated(2, t1+1))
	reelect(t, f.peer)
	_, t2 := f.peer.LeaderAndTerm()
	if t2 <= t1 {
		t.Fatalf("fixture: re-election did not raise the term (%d -> %d)", t1, t2)
	}
	f.cycle()
	if !f.readIndexNotReady(1) {
		t.Fatal("fixture: ReadIndex served before the new term's no-op committed")
	}
	got := f.cb.getLeaderEvents()
	last := got[len(got)-1]
	if last.ready || last.leaderID != 1 || last.term != t2 {
		t.Fatalf("re-election not announced as OnLeaderUpdated(1, %d): %s", t2, describe(got))
	}

	f.persist()
	f.ackFollower()
	f.cycle()
	got = f.cb.getLeaderEvents()
	if n := len(got); n < 2 || !got[n-1].ready || got[n-1].term != t2 ||
		got[n-2].ready || got[n-2].term != t2 {
		t.Fatalf("want OnLeaderUpdated(1,%d) immediately followed by OnLeaderReady(1,%d): %s",
			t2, t2, describe(got))
	}
	if f.readIndexNotReady(2) {
		t.Fatal("OnLeaderReady fired but ReadIndex still answers ErrReadIndexNotReady")
	}
	// Exactly one readiness per term across the whole history.
	seen := map[uint64]int{}
	for _, e := range got {
		if e.ready {
			seen[e.term]++
		}
	}
	if seen[t1] != 1 || seen[t2] != 1 || len(seen) != 2 {
		t.Fatalf("readiness per term = %v, want exactly once for %d and %d", seen, t1, t2)
	}
}

// A refused readiness dispatch is retried, never dropped, and never overtakes
// the leader change it belongs to. The dispatcher's queue is driven by hand:
// it is "running" but nothing consumes it until the test drains it.
func TestStepWorker_LeaderReady_RetriedBehindItsLeaderChange(t *testing.T) {
	f := newReadyFixture(t)
	d := newListenerDispatcher()
	d.running.Store(true)
	f.sw.listeners = d
	fill := func() {
		for len(d.q) < cap(d.q) {
			d.q <- func() {}
		}
	}
	discard := func() {
		for len(d.q) > 0 {
			<-d.q
		}
	}

	// The queue is full from the election on: the leader change and, once the
	// no-op commits, its readiness are both refused. Neither is recorded, and
	// readiness is not announced ahead of the leader change it belongs to.
	fill()
	driveToLeader(t, f.peer, 1, 2)
	_, term := f.peer.LeaderAndTerm()
	f.cycle()
	f.persist()
	f.ackFollower()
	f.cycle()
	if _, _, ready := f.peer.LeaderTermReady(); !ready {
		t.Fatal("fixture: the no-op did not commit")
	}
	if _, ok := f.sw.prevLeaders[1]; ok {
		t.Fatal("leader change recorded although its dispatch was refused")
	}
	if _, ok := f.sw.prevReadyTerms[1]; ok {
		t.Fatal("readiness recorded although its dispatch was refused")
	}
	discard()
	f.assertEvents()

	// Room again: the leader change, then its readiness, in that order.
	f.sw.publishLeaderChange(1, f.node)
	d.drain()
	f.assertEvents(updated(1, term), ready(1, term))

	// The leader change is accepted but its readiness is refused: readiness
	// alone is retried later.
	f2 := newReadyFixture(t)
	d2 := newListenerDispatcher()
	d2.running.Store(true)
	f2.sw.listeners = d2
	driveToLeader(t, f2.peer, 1, 2)
	_, term2 := f2.peer.LeaderAndTerm()
	f2.cycle()
	d2.drain()
	f2.persist()
	f2.ackFollower()
	for len(d2.q) < cap(d2.q) {
		d2.q <- func() {}
	}
	f2.sw.publishLeaderChange(1, f2.node)
	for len(d2.q) > 0 {
		<-d2.q
	}
	f2.assertEvents(updated(1, term2))
	f2.sw.publishLeaderChange(1, f2.node)
	d2.drain()
	f2.assertEvents(updated(1, term2), ready(1, term2))
}

// Unloading the replica forgets what was announced, so a reloaded replica
// announces its leader and readiness again even in the same term: a listener
// that reset its view on OnNodeUnloaded is told the state afresh.
func TestStepWorker_LeaderReady_ReannouncedAfterReload(t *testing.T) {
	f := newReadyFixture(t)
	driveToLeader(t, f.peer, 1, 2)
	_, term := f.peer.LeaderAndTerm()
	f.cycle()
	f.persist()
	f.ackFollower()
	f.cycle()
	f.assertEvents(updated(1, term), ready(1, term))

	f.sw.unloadNode(1)
	f.sw.loadNode(f.node)
	f.sw.tick()
	f.assertEvents(updated(1, term), ready(1, term), updated(1, term), ready(1, term))
}
