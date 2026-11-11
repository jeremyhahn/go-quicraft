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

// Regression tests for memdb: snapshot metadata persisted through
// SaveState, deep-copied Cmd payloads, and gap detection in EntryRange/IterateEntries (LogDB contract).

package memdb

import (
	"bytes"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSaveStateIgnoresStaleSnapshot verifies the staleness check mirrors
// SaveSnapshot/waldb: an older snapshot in a later Update must not regress
// the stored snapshot or its membership.
func TestSaveStateIgnoresStaleSnapshot(t *testing.T) {
	db := New()
	newer := logdb.Snapshot{
		Index:      10,
		Term:       3,
		Membership: logdb.Membership{ConfigChangeID: 7},
	}
	stale := logdb.Snapshot{
		Index:      5,
		Term:       2,
		Membership: logdb.Membership{ConfigChangeID: 3},
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:    logdb.State{Term: 1, Commit: 1},
		Snapshot: newer,
	}}); err != nil {
		t.Fatalf("SaveState newer: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Snapshot: stale}}); err != nil {
		t.Fatalf("SaveState stale: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 10 {
		t.Fatalf("GetSnapshot.Index = %d, want 10 (stale snapshot must not regress)", got.Index)
	}
	_, membership, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if membership.ConfigChangeID != 7 {
		t.Fatalf("membership.ConfigChangeID = %d, want 7 (stale snapshot must not regress membership)",
			membership.ConfigChangeID)
	}
}

// TestSaveStateDeepCopiesCmd verifies the stored log never aliases the
// caller's Cmd buffers: Host.Propose builds entries over application-owned
// buffers, and reuse after SaveState must not mutate the persisted log
// (replica divergence). Both merge paths are covered: the first append to
// an empty node and a follow-up append to existing entries.
func TestSaveStateDeepCopiesCmd(t *testing.T) {
	db := New()

	buf1 := []byte("first-payload")
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: buf1}},
	}}); err != nil {
		t.Fatalf("SaveState 1: %v", err)
	}

	buf2 := []byte("second-payload")
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 2, Term: 1, Cmd: buf2}},
	}}); err != nil {
		t.Fatalf("SaveState 2: %v", err)
	}

	// Caller reuses its buffers.
	copy(buf1, "XXXXX")
	copy(buf2, "YYYYY")

	got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 3, 0)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if !bytes.Equal(got[0].Cmd, []byte("first-payload")) {
		t.Fatalf("entry 1 Cmd = %q, want %q (caller buffer aliased)", got[0].Cmd, "first-payload")
	}
	if !bytes.Equal(got[1].Cmd, []byte("second-payload")) {
		t.Fatalf("entry 2 Cmd = %q, want %q (caller buffer aliased)", got[1].Cmd, "second-payload")
	}
}

// TestEntryRangeRejectsGap verifies EntryRange reports ErrLogGapDetected
// when the stored entries are not contiguous, instead of a bare count that
// would hide the hole from LogReader.SetRange.
func TestEntryRangeRejectsGap(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	// Non-contiguous append creates a hole (a persistence-pipeline bug).
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 10, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState gap: %v", err)
	}

	if _, _, err := db.EntryRange(1, 1); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("EntryRange = %v, want ErrLogGapDetected", err)
	}
}

// TestIterateEntriesRejectsInteriorHole verifies a hole inside the
// requested range fails with ErrLogGapDetected instead of returning fewer
// entries with a nil error.
func TestIterateEntriesRejectsInteriorHole(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 10, Term: 1}},
	}}); err != nil {
		t.Fatalf("SaveState gap: %v", err)
	}

	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 11, 0); !errors.Is(err, logdb.ErrLogGapDetected) {
		t.Fatalf("IterateEntries = %v, want ErrLogGapDetected", err)
	}
}

// TestIterateEntriesReportsCompactedAfterFullCompaction is the regression test
// for the missing persistent compaction floor. RemoveEntriesTo sliced the
// entries and set them to nil when empty, recording no floor, so IterateEntries
// fell through to the empty-slice branch and returned ErrUnavailable. waldb
// keeps a durable compactedTo and checks it FIRST, returning ErrCompacted even
// with an empty index. The divergence matters to raft: ErrCompacted switches
// the leader to InstallSnapshot, ErrUnavailable makes it retry forever.
func TestIterateEntriesReportsCompactedAfterFullCompaction(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Full compaction: every entry is removed and the slice goes nil.
	if err := db.RemoveEntriesTo(1, 1, 3); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	for _, low := range []uint64{1, 2, 3} {
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, low, low+1, 0); !errors.Is(err, logdb.ErrCompacted) {
			t.Errorf("IterateEntries(low=%d) error = %v, want ErrCompacted", low, err)
		}
	}
	// Above the floor there is simply nothing yet.
	if _, _, err := db.IterateEntries(nil, 0, 1, 1, 4, 5, 0); !errors.Is(err, logdb.ErrUnavailable) {
		t.Errorf("IterateEntries(low=4) error = %v, want ErrUnavailable", err)
	}
}

// TestSaveStateDeepCopiesSnapshotMembership verifies the four membership maps
// carried by an Update's snapshot are cloned, matching the no-aliasing
// contract already honoured for Entry.Cmd. Retaining the caller's maps would
// let a later mutation rewrite the persisted membership.
func TestSaveStateDeepCopiesSnapshotMembership(t *testing.T) {
	db := New()
	defer db.Close()

	m := logdb.Membership{
		ConfigChangeID: 7,
		Addresses:      map[uint64]string{1: "a1"},
		Observers:      map[uint64]string{2: "o2"},
		Witnesses:      map[uint64]string{3: "w3"},
		Removed:        map[uint64]bool{4: true},
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:    logdb.State{Term: 2, Vote: 1, Commit: 5},
		Snapshot: logdb.Snapshot{Index: 5, Term: 2, Membership: m},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Mutate the caller's maps after the call.
	m.Addresses[1] = "MUTATED"
	m.Addresses[99] = "injected"
	m.Observers[2] = "MUTATED"
	m.Witnesses[3] = "MUTATED"
	m.Removed[4] = false
	m.Removed[98] = true

	_, stored, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if stored.Addresses[1] != "a1" || len(stored.Addresses) != 1 {
		t.Errorf("Addresses aliased the caller's map: %v", stored.Addresses)
	}
	if stored.Observers[2] != "o2" {
		t.Errorf("Observers aliased the caller's map: %v", stored.Observers)
	}
	if stored.Witnesses[3] != "w3" {
		t.Errorf("Witnesses aliased the caller's map: %v", stored.Witnesses)
	}
	if !stored.Removed[4] || len(stored.Removed) != 1 {
		t.Errorf("Removed aliased the caller's map: %v", stored.Removed)
	}

	ss, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Membership.Addresses[1] != "a1" {
		t.Errorf("snapshot membership aliased the caller's map: %v", ss.Membership.Addresses)
	}
}

// TestCopyMembershipPreservesNilMaps asserts a round-trip does not manufacture
// empty maps where the caller supplied none.
func TestCopyMembershipPreservesNilMaps(t *testing.T) {
	got := copyMembership(logdb.Membership{ConfigChangeID: 3})
	if got.ConfigChangeID != 3 {
		t.Fatalf("ConfigChangeID = %d, want 3", got.ConfigChangeID)
	}
	if got.Addresses != nil || got.Observers != nil || got.Witnesses != nil || got.Removed != nil {
		t.Fatalf("nil maps were replaced by empty maps: %+v", got)
	}
}

// TestSaveSnapshotUpdatesMembership is a regression test. SaveSnapshot
// stored the snapshot but left nd.membership untouched and kept the caller's
// maps, so ReadState reported an EMPTY membership where waldb reports the
// snapshot's real one, and a later caller mutation rewrote the stored copy.
func TestSaveSnapshotUpdatesMembership(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State: logdb.State{Term: 2, Vote: 1, Commit: 5},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	m := logdb.Membership{
		ConfigChangeID: 9,
		Addresses:      map[uint64]string{1: "a1"},
		Observers:      map[uint64]string{2: "o2"},
		Witnesses:      map[uint64]string{3: "w3"},
		Removed:        map[uint64]bool{4: true},
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 2, Membership: m}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Mutate the caller's maps after the call.
	m.Addresses[1] = "MUTATED"
	m.Removed[4] = false

	_, stored, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if stored.ConfigChangeID != 9 {
		t.Fatalf("ReadState membership = %+v, want the snapshot's membership", stored)
	}
	if stored.Addresses[1] != "a1" || len(stored.Addresses) != 1 {
		t.Errorf("Addresses aliased the caller's map: %v", stored.Addresses)
	}
	if !stored.Removed[4] {
		t.Errorf("Removed aliased the caller's map: %v", stored.Removed)
	}

	ss, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Membership.Addresses[1] != "a1" {
		t.Errorf("snapshot membership aliased the caller's map: %v", ss.Membership.Addresses)
	}
}

// TestSaveBootstrapDoesNotAliasCallerAddresses closes the last aliasing hole in
// this package.
//
// Every other write path deep-copies what the caller hands it — Entry.Cmd in
// appendCopied, all six Membership maps in copyMembership — because the caller
// owns those buffers and reuses them. SaveBootstrap took the Addresses map by
// reference, so a caller that recycled its map (or simply kept editing it while
// bringing more shards up) rewrote the stored INITIAL MEMBERSHIP after the fact:
// the address set a recovering replica dials, and the identity set it authorizes
// joins against.
func TestSaveBootstrapDoesNotAliasCallerAddresses(t *testing.T) {
	db := New()

	addrs := map[uint64]string{1: "node1:9000", 2: "node2:9000"}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{Addresses: addrs, Type: 7}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	// The caller reuses its own map, exactly as a host bringing up the next
	// shard would.
	addrs[1] = "attacker:9000"
	addrs[3] = "node3:9000"
	delete(addrs, 2)

	got, ok, err := db.GetBootstrap(1, 1)
	if err != nil || !ok {
		t.Fatalf("GetBootstrap: ok=%v err=%v", ok, err)
	}
	want := map[uint64]string{1: "node1:9000", 2: "node2:9000"}
	if len(got.Addresses) != len(want) {
		t.Fatalf("stored bootstrap addresses = %v, want %v: the store aliased the "+
			"caller's map, so mutating it after the call rewrote the persisted "+
			"initial membership", got.Addresses, want)
	}
	for id, addr := range want {
		if got.Addresses[id] != addr {
			t.Fatalf("stored bootstrap address for replica %d = %q, want %q: the "+
				"store aliased the caller's map and the persisted initial "+
				"membership now names a replica the caller never bootstrapped with",
				id, got.Addresses[id], addr)
		}
	}
}

// TestMergeEntriesReleasesTruncatedPayloads pins the memory behaviour of a
// leader overwrite that SHORTENS the log.
//
// mergeEntries reuses the existing backing array, so replacing a long
// uncommitted suffix with a short one leaves the difference sitting past len —
// each abandoned element still holding a live pointer to its Cmd payload.
// Nothing ever reads those elements again, which is exactly why the retention
// is invisible: the log looks correct at every index. Under repeated leader
// churn it is unbounded.
func TestMergeEntriesReleasesTruncatedPayloads(t *testing.T) {
	db := New()

	const total = 64
	entries := make([]logdb.Entry, 0, total)
	for i := uint64(1); i <= total; i++ {
		entries = append(entries, logdb.Entry{Index: i, Term: 1, Cmd: make([]byte, 512)})
	}
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1, Entries: entries}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	nd := db.getNode(1, 1)
	if nd == nil {
		t.Fatal("node missing after SaveState")
	}

	// A new leader overwrites from index 2, replacing 63 entries with one.
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 2, Term: 2, Cmd: make([]byte, 512)}}}}); err != nil {
		t.Fatalf("SaveState (overwrite): %v", err)
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()
	live := len(nd.entries)
	if live != 2 {
		t.Fatalf("log holds %d entries after the overwrite, want 2", live)
	}
	tail := nd.entries[live:cap(nd.entries)]
	var retained int
	for i := range tail {
		if tail[i].Cmd != nil || tail[i].Index != 0 {
			retained++
		}
	}
	if retained != 0 {
		t.Fatalf("%d of the %d entries dropped by the overwrite are still reachable "+
			"from the backing array past len(entries)=%d (cap=%d), each pinning its "+
			"Cmd payload: the log is correct at every index while the memory it "+
			"discarded is never released, and every further overwrite adds another "+
			"orphaned tail", retained, len(tail), live, cap(nd.entries))
	}
}

// TestSaveSnapshotRejectsIndexZero pins the fresh-node hole in the staleness
// rule.
//
// Index 0 is the empty log: a snapshot there covers nothing and can never be
// newer than anything. The staleness check rejects it whenever a snapshot is
// already stored, but on a node that has none there was nothing to compare
// against, so it was accepted — and GetSnapshot then reported a snapshot
// existing at no index, which a caller reads as "this replica has a snapshot to
// recover from". The SaveState path has always required Index > 0; this is the
// same rule on the other door.
func TestSaveSnapshotRejectsIndexZero(t *testing.T) {
	db := New()

	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 0, Term: 4}); !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(Index=0) on a fresh node = %v, want ErrSnapshotOutOfDate", err)
	}
	if _, err := db.GetSnapshot(1, 1); !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Fatalf("GetSnapshot after a rejected index-0 snapshot = %v, want "+
			"ErrNoSnapshot: a phantom snapshot at index 0 was stored, and a caller "+
			"reads that as a recovery point this replica does not have", err)
	}

	// A real snapshot still lands, and index 0 stays rejected afterwards.
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 10, Term: 4}); err != nil {
		t.Fatalf("SaveSnapshot(Index=10): %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 0}); !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
		t.Fatalf("SaveSnapshot(Index=0) over a stored snapshot = %v, want ErrSnapshotOutOfDate", err)
	}
}

// TestRemoveNodeDataWaitsForAnInFlightBatch pins that a removal never splits a
// SaveState batch. If a batch for replicas A and B were applied one update at
// a time with nothing ordering it against RemoveNodeData, removing B between
// the two would store A and then refuse B: SaveState would return an error for
// a batch it had half applied, and a caller told the batch failed would retry
// or abandon writes that were in fact kept. RemoveNodeData waits for every
// in-flight write (removalMu), so the batch is applied whole and the
// removal follows it.
//
// The window is HELD OPEN rather than raced for: the test holds B's node lock,
// so the batch applies A and parks on B. The fixture asserts both the batch
// and the removal really are blocked there, so a change that closed the
// window some other way cannot make this vacuous.
// waitForRemovalLock waits until cond holds, failing the test with what
// after five seconds.
func waitForRemovalLock(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		runtime.Gosched()
	}
}

// removalReadHeld reports whether a write holds removalMu for reading.
func (m *MemDB) removalReadHeld() bool {
	if m.removalMu.TryLock() {
		m.removalMu.Unlock()
		return false
	}
	return true
}

// removalWriterQueued reports whether RemoveNodeData or Close is waiting for
// removalMu: a queued writer makes TryRLock fail.
func (m *MemDB) removalWriterQueued() bool {
	if m.removalMu.TryRLock() {
		m.removalMu.RUnlock()
		return false
	}
	return true
}

func TestRemoveNodeDataWaitsForAnInFlightBatch(t *testing.T) {
	db := New()
	b := db.getOrCreateNode(2, 1)
	b.mu.Lock()

	saved := make(chan error, 1)
	go func() {
		saved <- db.SaveState([]logdb.Update{
			{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 3, Vote: 1}},
			{ShardID: 2, ReplicaID: 1, State: logdb.State{Term: 3, Vote: 1}},
		})
	}()
	removed := make(chan error, 1)
	waitForRemovalLock(t, "the batch holds the removal lock", db.removalReadHeld)
	go func() { removed <- db.RemoveNodeData(2, 1) }()
	waitForRemovalLock(t, "RemoveNodeData waits for the removal lock", db.removalWriterQueued)
	select {
	case err := <-saved:
		b.mu.Unlock()
		t.Fatalf("SaveState completed (%v) while B's lock was held: the batch never reached B", err)
	case err := <-removed:
		b.mu.Unlock()
		t.Fatalf("RemoveNodeData completed (%v) while a batch naming its node was in flight", err)
	default:
	}
	b.mu.Unlock()

	if err := <-saved; err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := <-removed; err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
	if st, _, err := db.ReadState(1, 1); err != nil || st.Term != 3 {
		t.Fatalf("A after the batch = (%+v, %v), want term 3", st, err)
	}
	if _, _, err := db.ReadState(2, 1); !errors.Is(err, logdb.ErrNoState) {
		t.Fatalf("B after its removal = %v, want ErrNoState", err)
	}
	if retired, _ := db.NodeRemoved(2, 1); !retired {
		t.Fatal("B not retired by RemoveNodeData")
	}
}

// TestCloseWaitsForAnInFlightBatch pins that Close never splits a SaveState
// batch either. If Close released the node map while holding only the map
// lock, a batch in flight across it would store part of itself in the
// discarded map and the rest in the new one, and return nil. Close waits for
// every in-flight write, and a write that starts after it is refused.
func TestCloseWaitsForAnInFlightBatch(t *testing.T) {
	db := New()
	b := db.getOrCreateNode(2, 1)
	b.mu.Lock()

	saved := make(chan error, 1)
	go func() {
		saved <- db.SaveState([]logdb.Update{
			{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 3, Vote: 1}},
			{ShardID: 2, ReplicaID: 1, State: logdb.State{Term: 3, Vote: 1}},
		})
	}()
	waitForRemovalLock(t, "the batch holds the removal lock", db.removalReadHeld)
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	waitForRemovalLock(t, "Close waits for the removal lock", db.removalWriterQueued)
	select {
	case err := <-saved:
		b.mu.Unlock()
		t.Fatalf("SaveState completed (%v) while B's lock was held: the batch never reached B", err)
	case err := <-closed:
		b.mu.Unlock()
		t.Fatalf("Close completed (%v) while a batch was in flight", err)
	default:
	}
	b.mu.Unlock()

	if err := <-saved; err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.SaveState([]logdb.Update{{ShardID: 3, ReplicaID: 1, State: logdb.State{Term: 1}}}); !errors.Is(err, logdb.ErrClosed) {
		t.Fatalf("SaveState after Close = %v, want ErrClosed", err)
	}
	db.mu.RLock()
	n := len(db.nodes)
	db.mu.RUnlock()
	if n != 0 {
		t.Fatalf("Close left %d nodes in the map: a write in flight across it repopulated the released data", n)
	}
}

// TestCloseReleasesNodeData pins Close's only real job in an in-memory store.
//
// Every byte this backend holds is on the heap. A Close that just flipped a flag
// kept the entire Raft log of every shard alive for as long as the caller held
// the handle, which in a host that closes one store and opens another is the
// whole log twice.
func TestCloseReleasesNodeData(t *testing.T) {
	db := New()
	if err := db.SaveState([]logdb.Update{{ShardID: 1, ReplicaID: 1,
		Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: make([]byte, 4096)}}}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if db.getNode(1, 1) == nil {
		t.Fatal("node missing before Close")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if nd := db.getNode(1, 1); nd != nil {
		t.Fatalf("Close retained node data (%d entries): closing an in-memory "+
			"store that releases nothing keeps every shard's whole log alive for "+
			"the lifetime of the handle", len(nd.entries))
	}

	// Close stays idempotent and the store stays closed.
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, _, err := db.EntryRange(1, 1); !errors.Is(err, logdb.ErrClosed) {
		t.Fatalf("EntryRange after Close = %v, want ErrClosed", err)
	}
}
