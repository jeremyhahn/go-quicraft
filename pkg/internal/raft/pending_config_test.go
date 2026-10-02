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

package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// addNodeEntry builds an EntryConfigChange that admits replicaID with identity
// at index/term.
func addNodeEntry(t *testing.T, index, term, replicaID uint64, identity string) proto.Entry {
	t.Helper()
	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: replicaID, Address: "addr", NodeIdentity: identity}
	cmd := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(cmd); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	return proto.Entry{Index: index, Term: term, Type: proto.EntryConfigChange, Cmd: cmd}
}

// TestPendingConfig_FollowsTheLog pins the pending configuration:
// an identity-bearing add enters it when the entry is appended, leaves it when
// a conflicting append truncates the entry, is cleared by a snapshot restore,
// and is ignored by the reader once the applied configuration has moved past
// its index.
func TestPendingConfig_FollowsTheLog(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{Term: 1}, threeNodeMembership())
	l := p.r.log

	if _, ok := p.PendingMemberIdentity(4); ok {
		t.Fatal("a fresh peer reports a pending member")
	}
	p.PublishPendingConfig()
	if p.pendingSnap.Load() != nil {
		t.Fatal("PublishPendingConfig allocated a view with nothing pending")
	}

	// Appended: pending, bound to the identity the entry carries.
	l.append([]proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal},
		addNodeEntry(t, 2, 1, 4, "spki:4"),
		{Index: 3, Term: 1, Type: proto.EntryNormal},
	})
	p.PublishPendingConfig()
	if got, ok := p.PendingMemberIdentity(4); !ok || got != "spki:4" {
		t.Fatalf("pending identity for 4 = (%q, %v), want (spki:4, true)", got, ok)
	}
	if _, ok := p.PendingMemberIdentity(5); ok {
		t.Fatal("a replica no entry admits is reported pending")
	}
	// An add without an identity admits nothing on this path.
	l.append([]proto.Entry{addNodeEntry(t, 4, 1, 6, "")})
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(6); ok {
		t.Fatal("an identity-less AddNode was reported as a pending identity")
	}

	// A republish with nothing changed keeps the same view.
	before := p.pendingSnap.Load()
	p.PublishPendingConfig()
	if p.pendingSnap.Load() != before {
		t.Fatal("PublishPendingConfig rebuilt the view although nothing moved")
	}

	// Truncated by a conflicting append: the entry at 2 is replaced, so 4 is
	// no longer admitted by this log; the replacement admits 7 instead.
	if _, ok, _, err := l.tryAppend(1, 1, []proto.Entry{addNodeEntry(t, 2, 2, 7, "spki:7")}); err != nil || !ok {
		t.Fatalf("tryAppend: ok=%v err=%v", ok, err)
	}
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(4); ok {
		t.Fatal("a truncated AddNode is still reported as pending")
	}
	if got, ok := p.PendingMemberIdentity(7); !ok || got != "spki:7" {
		t.Fatalf("pending identity for 7 = (%q, %v), want (spki:7, true)", got, ok)
	}

	// A later entry for a replica that is truncated leaves an earlier
	// surviving entry for the same replica in force: entry 3 promotes 7 and
	// is cut, entry 2 still admits it.
	l.append([]proto.Entry{addNodeEntry(t, 3, 2, 7, "spki:7")})
	if _, ok, _, err := l.tryAppend(2, 2, []proto.Entry{{Index: 3, Term: 3, Type: proto.EntryNormal}}); err != nil || !ok {
		t.Fatalf("tryAppend: ok=%v err=%v", ok, err)
	}
	p.PublishPendingConfig()
	if got, ok := p.PendingMemberIdentity(7); !ok || got != "spki:7" {
		t.Fatalf("pending identity for 7 after its later entry was cut = (%q, %v), want the surviving entry's", got, ok)
	}

	// Applied: once the committed configuration has moved past the entry's
	// index the reader ignores it (the committed binding takes over), and the
	// publisher forgets it, so the map is bounded by the pending window.
	p.membership.configChangeID = 2
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(7); ok {
		t.Fatal("an add at or below the applied configuration is still reported as pending")
	}
	if _, kept := l.pendingAdds[2]; kept {
		t.Fatal("a folded entry's add was kept; the map would grow with the shard's whole history")
	}

	// Restored from a snapshot: nothing is pending.
	if _, ok, _, err := l.tryAppend(2, 2, []proto.Entry{addNodeEntry(t, 3, 4, 8, "spki:8")}); err != nil || !ok {
		t.Fatalf("tryAppend: ok=%v err=%v", ok, err)
	}
	p.membership.configChangeID = 0
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(8); !ok {
		t.Fatal("fixture: 8 should be pending before the restore")
	}
	l.restore(proto.Snapshot{Index: 10, Term: 2, Membership: threeNodeMembership()})
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(8); ok {
		t.Fatal("a snapshot restore left a pending add behind")
	}
}

// TestPendingConfig_SeededFromTheRecoveredUncommittedTail pins the restart
// case: an AddNode that was appended but not committed when the replica went
// down is still pending after the restart, so the member it names can still
// be admitted — and so is a COMMITTED one the applied configuration does not
// yet reflect, since the replay that folds it is paced on the state machine
// and the gate must be as wide after the restart as before it. Once the
// replay folds it, the next publish prunes it.
func TestPendingConfig_SeededFromTheRecoveredUncommittedTail(t *testing.T) {
	cfg := testConfig()
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal},
		addNodeEntry(t, 2, 1, 4, "spki:4"), // committed, not yet folded: seeded
		{Index: 3, Term: 1, Type: proto.EntryNormal},
		addNodeEntry(t, 4, 1, 5, "spki:5"), // uncommitted: seeded
	}
	p := newTestPeerWithLog(t, cfg, entries, proto.State{Term: 1, Commit: 3}, threeNodeMembership())
	p.PublishPendingConfig()
	if got, ok := p.PendingMemberIdentity(5); !ok || got != "spki:5" {
		t.Fatalf("pending identity for 5 after restart = (%q, %v), want (spki:5, true): "+
			"the uncommitted tail was not classified", got, ok)
	}
	if got, ok := p.PendingMemberIdentity(4); !ok || got != "spki:4" {
		t.Fatalf("pending identity for 4 after restart = (%q, %v), want (spki:4, true): "+
			"a committed AddNode the replay has not folded admits its member, as it did before the restart", got, ok)
	}
	// Folded through index 2: the committed add is pruned, the uncommitted
	// one stays.
	p.membership.configChangeID = 2
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(4); ok {
		t.Fatal("a folded AddNode stayed pending")
	}
	if _, ok := p.PendingMemberIdentity(5); !ok {
		t.Fatal("the uncommitted AddNode was pruned with the folded one")
	}
}

// TestPendingConfig_SeedBudgetLeavesTheCommittedRemainderToTheReplay pins the
// budget: a committed tail past pendingSeedBudget is not read at
// construction — StartShard would block on the whole log of a shard that
// never snapshots — while the uncommitted tail above it is still seeded.
func TestPendingConfig_SeedBudgetLeavesTheCommittedRemainderToTheReplay(t *testing.T) {
	saved := pendingSeedBudget
	pendingSeedBudget = 4096
	defer func() { pendingSeedBudget = saved }()

	cfg := testConfig()
	var entries []proto.Entry
	for i := uint64(1); i <= 200; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1, Type: proto.EntryNormal, Cmd: make([]byte, 100)})
	}
	entries = append(entries, addNodeEntry(t, 201, 1, 4, "spki:4")) // committed, past the budget
	entries = append(entries, addNodeEntry(t, 202, 1, 5, "spki:5")) // uncommitted
	p := newTestPeerWithLog(t, cfg, entries, proto.State{Term: 1, Commit: 201}, threeNodeMembership())
	p.PublishPendingConfig()
	if _, ok := p.PendingMemberIdentity(4); ok {
		t.Fatal("a committed AddNode past the seed budget was read at construction")
	}
	if got, ok := p.PendingMemberIdentity(5); !ok || got != "spki:5" {
		t.Fatalf("pending identity for 5 = (%q, %v), want (spki:5, true): the uncommitted tail "+
			"above the budget was not seeded", got, ok)
	}
}

// TestPendingConfig_InitialMembershipEntryAdmitsItsMembers pins that a
// replicated bootstrap entry — what a joiner started with a partial
// configuration receives from the leader — admits every identity it names
// until it is applied.
func TestPendingConfig_InitialMembershipEntryAdmitsItsMembers(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{Term: 1}, proto.Membership{})
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		Identities: map[uint64]string{1: "spki:1", 2: "spki:2", 3: "spki:3"},
	}
	entry, err := BuildInitialMembershipEntry(ms)
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	p.r.log.append([]proto.Entry{entry})
	p.PublishPendingConfig()
	for id, want := range ms.Identities {
		if got, ok := p.PendingMemberIdentity(id); !ok || got != want {
			t.Fatalf("pending identity for %d = (%q, %v), want (%q, true)", id, got, ok, want)
		}
	}

	// An identity the entry carries for an ID no active role holds admits
	// nobody: it would authorize that keypair under an arbitrary replica ID
	// until the entry applied.
	stray := proto.Membership{
		Addresses:  map[uint64]string{1: "a1"},
		Identities: map[uint64]string{1: "spki:1", 42: "spki:42"},
	}
	q := newTestPeer(t, cfg, proto.State{Term: 1}, proto.Membership{})
	strayEntry, err := BuildInitialMembershipEntry(stray)
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	q.r.log.append([]proto.Entry{strayEntry})
	q.PublishPendingConfig()
	if _, ok := q.PendingMemberIdentity(42); ok {
		t.Fatal("a bootstrap entry admitted an identity for a replica no role holds")
	}
	if _, ok := q.PendingMemberIdentity(1); !ok {
		t.Fatal("the bootstrap entry's real member was not admitted")
	}
}

// TestPendingConfig_SeedReadsTheWholeTail pins that the seed is not fooled by
// the reader's per-read slice cap (about 35k entries): an uncommitted tail
// longer than one read still has its last AddNode classified, because the
// seed reads on from the last index each read returned.
func TestPendingConfig_SeedReadsTheWholeTail(t *testing.T) {
	cfg := testConfig()
	const tail = 40_000
	entries := make([]proto.Entry, 0, tail+1)
	entries = append(entries, proto.Entry{Index: 1, Term: 1, Type: proto.EntryNormal})
	for i := uint64(2); i <= tail; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1, Type: proto.EntryNormal})
	}
	entries = append(entries, addNodeEntry(t, tail+1, 1, 5, "spki:5"))
	// The real reader, whose per-read cap is the thing under test; the
	// package's test reader does not cap.
	lr := logdb.NewLogReader(cfg.ShardID, cfg.ReplicaID, nil)
	if err := lr.ApplySnapshot(proto.Snapshot{Membership: threeNodeMembership()}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if err := lr.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	lr.SetState(proto.State{Term: 1, Commit: 1})
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	p.PublishPendingConfig()
	if got, ok := p.PendingMemberIdentity(5); !ok || got != "spki:5" {
		t.Fatalf("pending identity for 5 after restart = (%q, %v), want (spki:5, true): "+
			"the AddNode past the reader's slice cap was not seeded", got, ok)
	}
}

// removeNodeEntry builds a committed-shaped RemoveNode entry at index.
func removeNodeEntry(t *testing.T, index, term, replicaID uint64) proto.Entry {
	t.Helper()
	cc := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: replicaID}
	cmd := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(cmd); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	return proto.Entry{Index: index, Term: term, Type: proto.EntryConfigChange, Cmd: cmd}
}

// TestPendingConfig_RemovalsAreFoldedInLogOrder pins the pending tombstone:
// adds and removals are resolved in log order, so a
// removal above an add cancels the add and stands as a pending removal by ID
// and by the identity the add bound; a removal of a COMMITTED member stands
// with the committed identity; and an add above a removal supersedes it. A
// restart that seeds its committed-but-unfolded stretch must not re-admit a
// member the shard removed for as long as the replay takes to reach it.
func TestPendingConfig_RemovalsAreFoldedInLogOrder(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	ms.Identities = map[uint64]string{1: "spki:1", 2: "spki:2", 3: "spki:3"}
	entries := []proto.Entry{
		addNodeEntry(t, 1, 1, 5, "spki:5"),
		removeNodeEntry(t, 2, 1, 5), // cancels the add; tombstones spki:5
		removeNodeEntry(t, 3, 1, 2), // a committed member: tombstones spki:2
		removeNodeEntry(t, 4, 1, 6),
		addNodeEntry(t, 5, 1, 6, "spki:6"), // re-added above its removal
	}
	p := newTestPeerWithLog(t, cfg, entries, proto.State{Term: 1, Commit: 5}, ms)
	p.PublishPendingConfig()

	if _, ok := p.PendingMemberIdentity(5); ok {
		t.Fatal("an add cancelled by a later removal still admits its member")
	}
	if !p.PendingRemoval(5, "") || !p.PendingRemoval(0, "spki:5") {
		t.Fatal("the removal above the add is not a pending tombstone by ID and by the identity the add bound")
	}
	if !p.PendingRemoval(2, "") || !p.PendingRemoval(0, "spki:2") {
		t.Fatal("the removal of a committed member is not a pending tombstone by ID and by its committed identity")
	}
	if p.PendingRemoval(6, "") || p.PendingRemoval(0, "spki:6") {
		t.Fatal("an add above a removal did not supersede it")
	}
	if got, ok := p.PendingMemberIdentity(6); !ok || got != "spki:6" {
		t.Fatalf("pending identity for the re-added replica = (%q, %v), want (spki:6, true)", got, ok)
	}
	if p.PendingRemoval(1, "spki:1") {
		t.Fatal("a member nothing removes is reported as pending removal")
	}

	// Folded through the removals: they are the committed tombstones' business now.
	p.membership.configChangeID = 4
	p.PublishPendingConfig()
	if p.PendingRemoval(2, "spki:2") || p.PendingRemoval(5, "spki:5") {
		t.Fatal("a folded removal stayed pending")
	}
	if got, ok := p.PendingMemberIdentity(6); !ok || got != "spki:6" {
		t.Fatalf("the add above the fold = (%q, %v), want (spki:6, true)", got, ok)
	}
}

// TestPendingConfig_UncommittedRemovalDoesNotRevoke pins the liveness half:
// a RemoveNode that is appended but not committed revokes nobody. In a
// two-voter shard the leader that appended RemoveNode(B) would otherwise
// refuse B's very ack of it, the entry could never commit, and the shard
// would stay leaderless. The removal binds the gate from the moment it is
// committed — which the commit index alone announces, so the published
// view is keyed on it too.
func TestPendingConfig_UncommittedRemovalDoesNotRevoke(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	ms.Identities = map[uint64]string{1: "spki:1", 2: "spki:2", 3: "spki:3"}
	entries := []proto.Entry{
		addNodeEntry(t, 1, 1, 5, "spki:5"),
		removeNodeEntry(t, 2, 1, 5),
		removeNodeEntry(t, 3, 1, 2),
	}
	p := newTestPeerWithLog(t, cfg, entries, proto.State{Term: 1, Commit: 1}, ms)
	p.PublishPendingConfig()
	if p.PendingRemoval(2, "spki:2") || p.PendingRemoval(5, "spki:5") {
		t.Fatal("an uncommitted removal revoked a member")
	}
	if got, ok := p.PendingMemberIdentity(5); !ok || got != "spki:5" {
		t.Fatalf("an add below an uncommitted removal = (%q, %v), want (spki:5, true): the removal may yet be truncated", got, ok)
	}
	// Committed: the same entries now bind, with no log mutation in between.
	p.r.log.commitTo(3)
	p.PublishPendingConfig()
	if !p.PendingRemoval(2, "spki:2") || !p.PendingRemoval(5, "spki:5") {
		t.Fatal("a removal that became committed without a log mutation is not a pending tombstone: the view was not re-resolved on the commit index")
	}
	if _, ok := p.PendingMemberIdentity(5); ok {
		t.Fatal("an add below a now-committed removal still admits its member")
	}
}
