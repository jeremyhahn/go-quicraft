package raft

import (
	"errors"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// forwardTestBindings is a stand-in for Peer.boundConfigChanges on a bare raft.
type forwardTestBindings struct {
	mu   sync.Mutex
	keys map[uint64]struct{}
}

func (b *forwardTestBindings) bind(key uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keys[key] = struct{}{}
}

func (b *forwardTestBindings) take(key uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.keys[key]
	delete(b.keys, key)
	return ok
}

func (b *forwardTestBindings) bound(key uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.keys[key]
	return ok
}

func forwardTestConfigChange(t *testing.T, key, replicaID uint64) proto.Message {
	t.Helper()
	cc := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: replicaID, Address: "127.0.0.1:7000"}
	return proto.Message{
		Type:    proto.Propose,
		Origin:  proto.OriginLocal,
		Entries: []proto.Entry{{Type: proto.EntryConfigChange, Key: key, Cmd: marshalConfigChange(t, cc)}},
	}
}

// TestRaft_BoundConfigChangeNotForwarded: the binding of a bound config change
// (index 0 with MustMatchConfigChangeID) lives only on the replica that
// proposed it, and the encoded entry carries index 0. Once leadership has
// moved, forwarding the entry to the new leader would commit it as an
// unbound add, so the former leader refuses it locally with
// ErrStaleConfigChange, forwards nothing and forgets the binding. An unbound
// config change is still forwarded.
func TestRaft_BoundConfigChangeNotForwarded(t *testing.T) {
	members := map[uint64]string{1: "a1", 2: "a2", 3: "a3"}
	r1 := newTestRaftWithMembers(1, members)
	r2 := newTestRaftWithMembers(2, members)
	b := &forwardTestBindings{keys: map[uint64]struct{}{}}
	r1.takeBoundConfigChange = b.take

	// The add is bound and queued while replica 1 leads.
	electLeader(r1)
	const boundKey, unboundKey = 101, 102
	b.bind(boundKey)

	// Leadership moves to replica 2 before the worker takes the entry.
	r1.becomeFollower(r1.term+1, 2)
	electLeader(r2)
	r1.msgs = nil
	lastBefore := r2.log.lastIndex()

	err := r1.step(forwardTestConfigChange(t, boundKey, 50))
	if !errors.Is(err, ErrStaleConfigChange) {
		t.Fatalf("bound config change on a non-leader = %v, want ErrStaleConfigChange", err)
	}
	if fwd := collectMessages(r1, proto.Propose); len(fwd) != 0 {
		t.Fatalf("bound config change forwarded to the new leader: %d Propose messages", len(fwd))
	}
	if b.bound(boundKey) {
		t.Fatal("refused bound config change left its binding behind")
	}
	if got := r2.log.lastIndex(); got != lastBefore {
		t.Fatalf("new leader log grew from %d to %d", lastBefore, got)
	}

	// An unbound config change forwards as before, and the new leader
	// appends it.
	if err := r1.step(forwardTestConfigChange(t, unboundKey, 51)); err != nil {
		t.Fatalf("unbound config change on a follower: %v", err)
	}
	fwd := collectMessages(r1, proto.Propose)
	if len(fwd) != 1 || fwd[0].To != 2 {
		t.Fatalf("unbound config change: %d Propose messages, want 1 to replica 2", len(fwd))
	}
	fwd[0].From = 1
	fwd[0].Term = r2.term
	fwd[0].Origin = 0
	if err := r2.step(fwd[0]); err != nil {
		t.Fatalf("new leader refused the forwarded unbound change: %v", err)
	}
	if got := r2.log.lastIndex(); got != lastBefore+1 {
		t.Fatalf("new leader log at %d after the forwarded change, want %d", got, lastBefore+1)
	}
}
