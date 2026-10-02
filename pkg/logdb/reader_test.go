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

package logdb

import (
	"errors"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// mustSetRange is a test helper that calls SetRange and fails on error.
func mustSetRange(t *testing.T, lr *LogReader, firstIndex, length uint64) {
	t.Helper()
	if err := lr.SetRange(firstIndex, length); err != nil {
		t.Fatalf("unexpected error from SetRange(%d, %d): %v", firstIndex, length, err)
	}
}

// mustAppend is a test helper that calls Append and fails on error.
func mustAppend(t *testing.T, lr *LogReader, entries []proto.Entry) {
	t.Helper()
	if err := lr.Append(entries); err != nil {
		t.Fatalf("unexpected error from Append: %v", err)
	}
}

// memLogDB is an in-memory LogDB implementation for testing LogReader.
// It stores entries and state per (shardID, replicaID) pair. All methods
// are thread-safe via a mutex, matching the LogDB concurrency contract.
type memLogDB struct {
	mu        sync.Mutex
	entries   map[nodeKey][]Entry
	states    map[nodeKey]State
	members   map[nodeKey]Membership
	snapshots map[nodeKey]Snapshot
	bootstrap map[nodeKey]Bootstrap
	closed    bool
}

type nodeKey struct {
	shardID   uint64
	replicaID uint64
}

func newMemLogDB() *memLogDB {
	return &memLogDB{
		entries:   make(map[nodeKey][]Entry),
		states:    make(map[nodeKey]State),
		members:   make(map[nodeKey]Membership),
		snapshots: make(map[nodeKey]Snapshot),
		bootstrap: make(map[nodeKey]Bootstrap),
	}
}

func (m *memLogDB) Name() string { return "mem" }

func (m *memLogDB) SaveState(updates []Update) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	for _, u := range updates {
		key := nodeKey{u.ShardID, u.ReplicaID}
		m.states[key] = u.State
		if len(u.Entries) > 0 {
			existing := m.entries[key]
			// Find insertion point: if new entries overlap existing, truncate.
			if len(existing) > 0 && len(u.Entries) > 0 {
				firstNew := u.Entries[0].Index
				cutIdx := -1
				for i, e := range existing {
					if e.Index >= firstNew {
						cutIdx = i
						break
					}
				}
				if cutIdx >= 0 {
					existing = existing[:cutIdx]
				}
			}
			existing = append(existing, u.Entries...)
			m.entries[key] = existing
		}
	}
	return nil
}

func (m *memLogDB) IterateEntries(
	ents []Entry, size uint64,
	shardID, replicaID, low, high, maxSize uint64,
) ([]Entry, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, 0, ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	all := m.entries[key]

	for _, e := range all {
		if e.Index >= low && e.Index < high {
			entSize := uint64(16 + len(e.Cmd)) // approximate size
			if maxSize > 0 && size+entSize > maxSize && len(ents) > 0 {
				// Already have at least one entry, stop adding.
				break
			}
			ents = append(ents, e)
			size += entSize
		}
	}
	return ents, size, nil
}

func (m *memLogDB) ReadState(shardID, replicaID uint64) (State, Membership, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return State{}, Membership{}, ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	s, ok := m.states[key]
	if !ok {
		return State{}, Membership{}, ErrNoState
	}
	mem := m.members[key]
	return s, mem, nil
}

func (m *memLogDB) EntryRange(shardID, replicaID uint64) (uint64, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, 0, ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	all := m.entries[key]
	if len(all) == 0 {
		return 0, 0, nil
	}
	return all[0].Index, uint64(len(all)), nil
}

func (m *memLogDB) RemoveEntriesTo(shardID, replicaID, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	all := m.entries[key]
	var kept []Entry
	for _, e := range all {
		if e.Index > index {
			kept = append(kept, e)
		}
	}
	m.entries[key] = kept
	return nil
}

func (m *memLogDB) Compact(shardID, replicaID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return nil
}

func (m *memLogDB) SaveSnapshot(shardID, replicaID uint64, snapshot Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	m.snapshots[key] = snapshot
	return nil
}

func (m *memLogDB) GetSnapshot(shardID, replicaID uint64) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Snapshot{}, ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	s, ok := m.snapshots[key]
	if !ok {
		return Snapshot{}, ErrNoSnapshot
	}
	return s, nil
}

func (m *memLogDB) RemoveNodeData(shardID, replicaID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	delete(m.entries, key)
	delete(m.states, key)
	delete(m.members, key)
	delete(m.snapshots, key)
	delete(m.bootstrap, key)
	return nil
}

func (m *memLogDB) NodeRemoved(_, _ uint64) (bool, error) { return false, nil }

func (m *memLogDB) ListNodeInfo() ([]NodeInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	seen := make(map[nodeKey]struct{})
	var result []NodeInfo
	for k := range m.entries {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			result = append(result, NodeInfo{ShardID: k.shardID, ReplicaID: k.replicaID})
		}
	}
	return result, nil
}

func (m *memLogDB) SaveBootstrap(shardID, replicaID uint64, bootstrap Bootstrap) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	m.bootstrap[key] = bootstrap
	return nil
}

func (m *memLogDB) GetBootstrap(shardID, replicaID uint64) (Bootstrap, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Bootstrap{}, false, ErrClosed
	}
	key := nodeKey{shardID, replicaID}
	b, ok := m.bootstrap[key]
	return b, ok, nil
}

func (m *memLogDB) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// addEntries is a test helper that writes entries directly into the memLogDB
// for the given shard/replica.
func (m *memLogDB) addEntries(shardID, replicaID uint64, entries []Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := nodeKey{shardID, replicaID}
	m.entries[key] = append(m.entries[key], entries...)
}

// --- Compile-time interface check ---

var _ LogDB = (*memLogDB)(nil)

// --- Helper functions ---

// makeEntries creates a slice of logdb.Entry with sequential indices
// starting at firstIndex, all with the given term.
func makeEntries(firstIndex, count, term uint64) []Entry {
	entries := make([]Entry, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = Entry{
			Index: firstIndex + i,
			Term:  term,
			Cmd:   []byte("test-cmd"),
		}
	}
	return entries
}

// makeProtoEntries creates a slice of proto.Entry with sequential indices.
func makeProtoEntries(firstIndex, count, term uint64) []proto.Entry {
	entries := make([]proto.Entry, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = proto.Entry{
			Index: firstIndex + i,
			Term:  term,
			Cmd:   []byte("test-cmd"),
		}
	}
	return entries
}

// --- NewLogReader tests ---

func TestLogReaderNewLogReaderInitializesCorrectly(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 2, db)

	if lr.shardID != 1 {
		t.Errorf("shardID = %d, want 1", lr.shardID)
	}
	if lr.replicaID != 2 {
		t.Errorf("replicaID = %d, want 2", lr.replicaID)
	}
	if lr.length != 1 {
		t.Errorf("length = %d, want 1", lr.length)
	}
	if lr.markerIndex != 0 {
		t.Errorf("markerIndex = %d, want 0", lr.markerIndex)
	}
	if lr.markerTerm != 0 {
		t.Errorf("markerTerm = %d, want 0", lr.markerTerm)
	}
}

func TestLogReaderNewLogReaderWithZeroIDs(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(0, 0, db)

	if lr.shardID != 0 {
		t.Errorf("shardID = %d, want 0", lr.shardID)
	}
	if lr.replicaID != 0 {
		t.Errorf("replicaID = %d, want 0", lr.replicaID)
	}
	if lr.length != 1 {
		t.Errorf("length = %d, want 1", lr.length)
	}
}

// --- NodeState tests ---

func TestLogReaderNodeStateReturnsDefaultState(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	state, membership := lr.NodeState()
	if state.Term != 0 || state.Vote != 0 || state.Commit != 0 {
		t.Errorf("initial state should be zero, got %+v", state)
	}
	if membership.ConfigChangeID != 0 {
		t.Errorf("initial membership should be zero, got %+v", membership)
	}
}

func TestLogReaderNodeStateReturnsUpdatedState(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	lr.SetState(proto.State{Term: 5, Vote: 2, Commit: 10})

	state, _ := lr.NodeState()
	if state.Term != 5 {
		t.Errorf("state.Term = %d, want 5", state.Term)
	}
	if state.Vote != 2 {
		t.Errorf("state.Vote = %d, want 2", state.Vote)
	}
	if state.Commit != 10 {
		t.Errorf("state.Commit = %d, want 10", state.Commit)
	}
}

// --- SetState tests ---

func TestLogReaderSetStateUpdatesState(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	lr.SetState(proto.State{Term: 3, Vote: 1, Commit: 42})
	state, _ := lr.NodeState()

	if state.Term != 3 {
		t.Errorf("state.Term = %d, want 3", state.Term)
	}
	if state.Vote != 1 {
		t.Errorf("state.Vote = %d, want 1", state.Vote)
	}
	if state.Commit != 42 {
		t.Errorf("state.Commit = %d, want 42", state.Commit)
	}
}

func TestLogReaderSetStateOverwritesPreviousState(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	lr.SetState(proto.State{Term: 1, Vote: 1, Commit: 1})
	lr.SetState(proto.State{Term: 10, Vote: 5, Commit: 100})

	state, _ := lr.NodeState()
	if state.Term != 10 {
		t.Errorf("state.Term = %d, want 10", state.Term)
	}
	if state.Vote != 5 {
		t.Errorf("state.Vote = %d, want 5", state.Vote)
	}
	if state.Commit != 100 {
		t.Errorf("state.Commit = %d, want 100", state.Commit)
	}
}

// --- SetRange tests ---

func TestLogReaderSetRangeExtendsRange(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Initially: markerIndex=0, length=1, so firstIndex=1, lastIndex=0.
	// Setting range at index 1, length 5 means entries [1..5] available.
	mustSetRange(t, lr, 1, 5)

	lr.mu.RLock()
	first := lr.firstIndex()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if first != 1 {
		t.Errorf("firstIndex = %d, want 1", first)
	}
	if last != 5 {
		t.Errorf("lastIndex = %d, want 5", last)
	}
}

func TestLogReaderSetRangeWithZeroLengthIsNoop(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	mustSetRange(t, lr, 1, 5)
	lr.mu.RLock()
	lengthBefore := lr.length
	lr.mu.RUnlock()

	mustSetRange(t, lr, 10, 0)

	lr.mu.RLock()
	lengthAfter := lr.length
	lr.mu.RUnlock()

	if lengthBefore != lengthAfter {
		t.Errorf("length changed from %d to %d after zero-length SetRange",
			lengthBefore, lengthAfter)
	}
}

func TestLogReaderSetRangeContiguousAppend(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// First batch: entries [1..3].
	mustSetRange(t, lr, 1, 3)
	// Contiguous append: entries [4..6].
	mustSetRange(t, lr, 4, 3)

	lr.mu.RLock()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if last != 6 {
		t.Errorf("lastIndex = %d, want 6", last)
	}
}

func TestLogReaderSetRangeOverlappingEntries(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// First batch: entries [1..5].
	mustSetRange(t, lr, 1, 5)
	// Overlapping: entries [3..7], extends the range.
	mustSetRange(t, lr, 3, 5)

	lr.mu.RLock()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if last != 7 {
		t.Errorf("lastIndex = %d, want 7", last)
	}
}

func TestLogReaderSetRangeOldEntriesIgnored(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	mustSetRange(t, lr, 1, 10) // entries [1..10]

	lr.mu.RLock()
	lastBefore := lr.lastIndex()
	lr.mu.RUnlock()

	// Set range that ends before our first index: should be ignored.
	// After SetRange(1,10), markerIndex=0, firstIndex=1.
	// This is all already covered, so it should be a no-op or handled.
	mustSetRange(t, lr, 1, 3) // entries [1..3], all already covered

	lr.mu.RLock()
	lastAfter := lr.lastIndex()
	lr.mu.RUnlock()

	// The range should not shrink -- but SetRange with overlap could truncate.
	// Looking at the logic: offset = 1-0 = 1, lr.length (11) > offset (1),
	// so lr.length = 1 + 3 = 4. This would actually shrink the range.
	// This matches dragonboat behavior where SetRange can truncate.
	// The design says "extends or replaces the tail of the existing range."
	// So lastAfter should be 3.
	if lastAfter != 3 {
		t.Errorf("lastIndex = %d, want 3 (SetRange replaces tail), was %d before",
			lastAfter, lastBefore)
	}
}

func TestLogReaderSetRangeGapReturnsError(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	mustSetRange(t, lr, 1, 3) // entries [1..3]

	// Gap: entries [1..3] then [10..12] with nothing in between.
	err := lr.SetRange(10, 3)
	if err == nil {
		t.Fatal("expected error on gap in SetRange, got nil")
	}
	if !errors.Is(err, ErrLogGapDetected) {
		t.Errorf("expected ErrLogGapDetected, got %v", err)
	}
}

func TestLogReaderSetRangeGapFromAppendReturnsError(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	mustSetRange(t, lr, 1, 3) // entries [1..3]

	// Append entries with a gap (index 10 when last is 3).
	gapEntries := makeProtoEntries(10, 3, 1)
	err := lr.Append(gapEntries)
	if err == nil {
		t.Fatal("expected error on gap in Append, got nil")
	}
	if !errors.Is(err, ErrLogGapDetected) {
		t.Errorf("expected ErrLogGapDetected, got %v", err)
	}
}

// --- Append tests ---

func TestLogReaderAppendUpdatesRange(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	entries := makeProtoEntries(1, 5, 1)
	mustAppend(t, lr, entries)

	lr.mu.RLock()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if last != 5 {
		t.Errorf("lastIndex = %d, want 5", last)
	}
}

func TestLogReaderAppendEmptySliceIsNoop(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	mustSetRange(t, lr, 1, 3)
	lr.mu.RLock()
	lengthBefore := lr.length
	lr.mu.RUnlock()

	mustAppend(t, lr, nil)
	mustAppend(t, lr, []proto.Entry{})

	lr.mu.RLock()
	lengthAfter := lr.length
	lr.mu.RUnlock()

	if lengthBefore != lengthAfter {
		t.Errorf("length changed from %d to %d after empty Append",
			lengthBefore, lengthAfter)
	}
}

// --- GetRange tests ---

func TestLogReaderGetRangeReturnsEntries(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Store entries in LogDB and update range.
	dbEntries := makeEntries(1, 5, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 5)

	got, err := lr.GetRange(1, 6, 0)
	if err != nil {
		t.Fatalf("GetRange() returned error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetRange() returned %d entries, want 5", len(got))
	}
	for i, e := range got {
		wantIdx := uint64(i + 1)
		if e.Index != wantIdx {
			t.Errorf("entry[%d].Index = %d, want %d", i, e.Index, wantIdx)
		}
		if e.Term != 1 {
			t.Errorf("entry[%d].Term = %d, want 1", i, e.Term)
		}
	}
}

func TestLogReaderGetRangeCompactedError(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// markerIndex=0, so requesting index 0 should return ErrCompacted.
	dbEntries := makeEntries(1, 5, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 5)

	_, err := lr.GetRange(0, 3, 0)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("GetRange(0, 3) error = %v, want ErrCompacted", err)
	}
}

func TestLogReaderGetRangeUnavailableError(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 5, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 5)

	// lastIndex=5, so high=10 exceeds the range.
	_, err := lr.GetRange(1, 10, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("GetRange(1, 10) error = %v, want ErrUnavailable", err)
	}
}

func TestLogReaderGetRangeLowGreaterThanHigh(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)
	mustSetRange(t, lr, 1, 5)

	_, err := lr.GetRange(5, 2, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("GetRange(5, 2) error = %v, want ErrUnavailable", err)
	}
}

func TestLogReaderGetRangeSubset(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 2)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// Request subset [3, 7).
	got, err := lr.GetRange(3, 7, 0)
	if err != nil {
		t.Fatalf("GetRange(3, 7) returned error: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("GetRange(3, 7) returned %d entries, want 4", len(got))
	}
	if got[0].Index != 3 {
		t.Errorf("first entry Index = %d, want 3", got[0].Index)
	}
	if got[3].Index != 6 {
		t.Errorf("last entry Index = %d, want 6", got[3].Index)
	}
}

// --- Term tests ---

func TestLogReaderTermAtMarkerIndex(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// markerIndex=0, markerTerm=0, so Term(0) should return 0.
	term, err := lr.Term(0)
	if err != nil {
		t.Fatalf("Term(0) returned error: %v", err)
	}
	if term != 0 {
		t.Errorf("Term(0) = %d, want 0", term)
	}
}

func TestLogReaderTermFromLogDB(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 5, 3)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 5)

	term, err := lr.Term(3)
	if err != nil {
		t.Fatalf("Term(3) returned error: %v", err)
	}
	if term != 3 {
		t.Errorf("Term(3) = %d, want 3", term)
	}
}

func TestLogReaderTermCompactedError(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// Compact up to index 5.
	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	// Term(3) should be compacted.
	_, err = lr.Term(3)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("Term(3) error = %v, want ErrCompacted", err)
	}
}

func TestLogReaderTermUnavailableError(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)
	mustSetRange(t, lr, 1, 5)

	// lastIndex=5, so index 10 is beyond range.
	_, err := lr.Term(10)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("Term(10) error = %v, want ErrUnavailable", err)
	}
}

func TestLogReaderTermAtMarkerAfterCompaction(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 5)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	err := lr.Compact(7)
	if err != nil {
		t.Fatalf("Compact(7) returned error: %v", err)
	}

	// Term(7) should return the cached markerTerm.
	term, err := lr.Term(7)
	if err != nil {
		t.Fatalf("Term(7) returned error: %v", err)
	}
	if term != 5 {
		t.Errorf("Term(7) = %d, want 5", term)
	}
}

// --- CreateSnapshot tests ---

func TestLogReaderCreateSnapshotUpdatesMembership(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	ss := proto.Snapshot{
		Index: 10,
		Term:  3,
		Membership: proto.Membership{
			ConfigChangeID: 5,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2"},
		},
	}

	err := lr.CreateSnapshot(ss)
	if err != nil {
		t.Fatalf("CreateSnapshot() returned error: %v", err)
	}

	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 5 {
		t.Errorf("membership.ConfigChangeID = %d, want 5", membership.ConfigChangeID)
	}
	if len(membership.Addresses) != 2 {
		t.Errorf("membership.Addresses length = %d, want 2", len(membership.Addresses))
	}
}

func TestLogReaderCreateSnapshotDoesNotChangeRange(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	mustSetRange(t, lr, 1, 10)

	lr.mu.RLock()
	lastBefore := lr.lastIndex()
	markerBefore := lr.markerIndex
	lr.mu.RUnlock()

	ss := proto.Snapshot{Index: 5, Term: 2}
	err := lr.CreateSnapshot(ss)
	if err != nil {
		t.Fatalf("CreateSnapshot() returned error: %v", err)
	}

	lr.mu.RLock()
	lastAfter := lr.lastIndex()
	markerAfter := lr.markerIndex
	lr.mu.RUnlock()

	if lastAfter != lastBefore {
		t.Errorf("lastIndex changed from %d to %d after CreateSnapshot", lastBefore, lastAfter)
	}
	if markerAfter != markerBefore {
		t.Errorf("markerIndex changed from %d to %d after CreateSnapshot", markerBefore, markerAfter)
	}
}

// --- ApplySnapshot tests ---

func TestLogReaderApplySnapshotResetsRange(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	mustSetRange(t, lr, 1, 10)

	ss := proto.Snapshot{
		Index: 50,
		Term:  7,
		Membership: proto.Membership{
			ConfigChangeID: 10,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}

	err := lr.ApplySnapshot(ss)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	lr.mu.RLock()
	marker := lr.markerIndex
	markerTerm := lr.markerTerm
	length := lr.length
	first := lr.firstIndex()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if marker != 50 {
		t.Errorf("markerIndex = %d, want 50", marker)
	}
	if markerTerm != 7 {
		t.Errorf("markerTerm = %d, want 7", markerTerm)
	}
	if length != 1 {
		t.Errorf("length = %d, want 1", length)
	}
	if first != 51 {
		t.Errorf("firstIndex = %d, want 51", first)
	}
	if last != 50 {
		t.Errorf("lastIndex = %d, want 50", last)
	}

	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 10 {
		t.Errorf("membership.ConfigChangeID = %d, want 10", membership.ConfigChangeID)
	}
}

func TestLogReaderApplySnapshotOutOfDateError(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// First, set some state so the reader has existing context.
	lr.SetState(proto.State{Term: 1, Vote: 1, Commit: 1})

	ss1 := proto.Snapshot{
		Index: 50,
		Term:  5,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss1)
	if err != nil {
		t.Fatalf("ApplySnapshot(ss1) returned error: %v", err)
	}

	// Applying an older snapshot should return ErrSnapshotOutOfDate.
	ss2 := proto.Snapshot{
		Index: 30,
		Term:  3,
		Membership: proto.Membership{
			ConfigChangeID: 2,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err = lr.ApplySnapshot(ss2)
	if !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Errorf("ApplySnapshot(ss2) error = %v, want ErrSnapshotOutOfDate", err)
	}

	// Verify the range was not modified by the failed apply.
	lr.mu.RLock()
	marker := lr.markerIndex
	lr.mu.RUnlock()

	if marker != 50 {
		t.Errorf("markerIndex = %d, want 50 (unchanged after failed apply)", marker)
	}
}

// --- Compact tests ---

func TestLogReaderCompactAdvancesMarker(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 4)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	lr.mu.RLock()
	marker := lr.markerIndex
	markerTerm := lr.markerTerm
	first := lr.firstIndex()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if marker != 5 {
		t.Errorf("markerIndex = %d, want 5", marker)
	}
	if markerTerm != 4 {
		t.Errorf("markerTerm = %d, want 4", markerTerm)
	}
	if first != 6 {
		t.Errorf("firstIndex = %d, want 6", first)
	}
	if last != 10 {
		t.Errorf("lastIndex = %d, want 10", last)
	}
}

func TestLogReaderCompactBelowMarkerReturnsError(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	// Compact below current marker should return ErrCompacted.
	err = lr.Compact(3)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("Compact(3) error = %v, want ErrCompacted", err)
	}
}

func TestLogReaderCompactBeyondLastIndexReturnsError(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)
	mustSetRange(t, lr, 1, 5) // lastIndex=5

	err := lr.Compact(10)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("Compact(10) error = %v, want ErrUnavailable", err)
	}
}

func TestLogReaderCompactToLastIndex(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 5, 2)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 5)

	// Compact to lastIndex (5) should leave length=1.
	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	lr.mu.RLock()
	marker := lr.markerIndex
	length := lr.length
	lr.mu.RUnlock()

	if marker != 5 {
		t.Errorf("markerIndex = %d, want 5", marker)
	}
	if length != 1 {
		t.Errorf("length = %d, want 1", length)
	}
}

func TestLogReaderCompactProgressivelyWorks(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 20, 3)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 20)

	// Compact progressively.
	for _, idx := range []uint64{5, 10, 15, 20} {
		err := lr.Compact(idx)
		if err != nil {
			t.Fatalf("Compact(%d) returned error: %v", idx, err)
		}

		lr.mu.RLock()
		marker := lr.markerIndex
		lr.mu.RUnlock()

		if marker != idx {
			t.Errorf("after Compact(%d), markerIndex = %d", idx, marker)
		}
	}
}

// --- GetRange after Compact tests ---

func TestLogReaderGetRangeAfterCompact(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	// Requesting compacted entries should fail.
	_, err = lr.GetRange(3, 7, 0)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("GetRange(3, 7) after compact error = %v, want ErrCompacted", err)
	}

	// Requesting available entries should succeed.
	got, err := lr.GetRange(6, 11, 0)
	if err != nil {
		t.Fatalf("GetRange(6, 11) after compact returned error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetRange(6, 11) returned %d entries, want 5", len(got))
	}
	if got[0].Index != 6 {
		t.Errorf("first entry Index = %d, want 6", got[0].Index)
	}
}

// --- ApplySnapshot then SetRange workflow ---

func TestLogReaderApplySnapshotThenSetRange(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Apply snapshot at index 100.
	ss := proto.Snapshot{
		Index: 100,
		Term:  10,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	// Now add entries starting after the snapshot.
	dbEntries := makeEntries(101, 5, 10)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 101, 5)

	lr.mu.RLock()
	first := lr.firstIndex()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if first != 101 {
		t.Errorf("firstIndex = %d, want 101", first)
	}
	if last != 105 {
		t.Errorf("lastIndex = %d, want 105", last)
	}

	// Verify we can read the entries.
	got, err := lr.GetRange(101, 106, 0)
	if err != nil {
		t.Fatalf("GetRange(101, 106) returned error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetRange(101, 106) returned %d entries, want 5", len(got))
	}

	// Verify compacted entries return ErrCompacted.
	_, err = lr.GetRange(50, 60, 0)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("GetRange(50, 60) error = %v, want ErrCompacted", err)
	}
}

// --- Concurrent access tests ---

func TestLogReaderConcurrentReaders(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 100, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 100)
	lr.SetState(proto.State{Term: 1, Vote: 1, Commit: 50})

	var wg sync.WaitGroup
	const readers = 10
	const iterations = 100

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Read term.
				term, err := lr.Term(50)
				if err != nil {
					t.Errorf("concurrent Term(50) error: %v", err)
					return
				}
				if term != 1 {
					t.Errorf("concurrent Term(50) = %d, want 1", term)
					return
				}

				// Read state.
				state, _ := lr.NodeState()
				if state.Term != 1 {
					t.Errorf("concurrent NodeState().Term = %d, want 1", state.Term)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestLogReaderConcurrentReadersAndWriter(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Pre-populate with initial entries.
	initialEntries := makeEntries(1, 50, 1)
	db.addEntries(shardID, replicaID, initialEntries)
	mustSetRange(t, lr, 1, 50)

	var wg sync.WaitGroup

	// Writer goroutine: extends the range.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(51); i <= 150; i++ {
			newEntries := makeEntries(i, 1, 2)
			db.addEntries(shardID, replicaID, newEntries)
			mustSetRange(t, lr, i, 1)
			lr.SetState(proto.State{Term: 2, Vote: 1, Commit: i})
		}
	}()

	// Multiple reader goroutines.
	const readers = 5
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				// These should not panic or return unexpected errors.
				// Term at markerIndex (0) is always valid.
				_, err := lr.Term(0)
				if err != nil {
					t.Errorf("concurrent Term(0) error: %v", err)
					return
				}

				lr.NodeState()
			}
		}()
	}

	wg.Wait()
}

func TestLogReaderConcurrentCompactAndRead(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 100, 5)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 100)

	var wg sync.WaitGroup

	// Reader goroutines: read Term and handle compaction gracefully.
	// Because compaction runs concurrently, any index may become
	// compacted between our read of markerIndex and our call to Term.
	// Both nil and ErrCompacted are acceptable outcomes.
	const readers = 5
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lr.mu.RLock()
				idx := lr.markerIndex
				lr.mu.RUnlock()
				_, err := lr.Term(idx)
				if err != nil && !errors.Is(err, ErrCompacted) {
					t.Errorf("concurrent Term(%d) unexpected error: %v", idx, err)
					return
				}
			}
		}()
	}

	// Compactor goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= 90; i += 10 {
			err := lr.Compact(i)
			if err != nil {
				// May get ErrCompacted if another compact already advanced.
				if !errors.Is(err, ErrCompacted) {
					t.Errorf("Compact(%d) error: %v", i, err)
					return
				}
			}
		}
	}()

	wg.Wait()
}

// --- Full lifecycle test ---

func TestLogReaderFullLifecycle(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Phase 1: Initial entries.
	entries1 := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, entries1)
	mustSetRange(t, lr, 1, 10)
	lr.SetState(proto.State{Term: 1, Vote: 1, Commit: 10})

	// Verify range.
	got, err := lr.GetRange(1, 11, 0)
	if err != nil {
		t.Fatalf("Phase 1: GetRange error: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("Phase 1: got %d entries, want 10", len(got))
	}

	// Phase 2: More entries in a new term.
	entries2 := makeEntries(11, 10, 2)
	db.addEntries(shardID, replicaID, entries2)
	mustSetRange(t, lr, 11, 10)
	lr.SetState(proto.State{Term: 2, Vote: 3, Commit: 20})

	term, err := lr.Term(15)
	if err != nil {
		t.Fatalf("Phase 2: Term(15) error: %v", err)
	}
	if term != 2 {
		t.Errorf("Phase 2: Term(15) = %d, want 2", term)
	}

	// Phase 3: Snapshot at index 15.
	ss := proto.Snapshot{
		Index: 15,
		Term:  2,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	err = lr.CreateSnapshot(ss)
	if err != nil {
		t.Fatalf("Phase 3: CreateSnapshot error: %v", err)
	}

	_, membership := lr.NodeState()
	if len(membership.Addresses) != 3 {
		t.Errorf("Phase 3: membership.Addresses count = %d, want 3",
			len(membership.Addresses))
	}

	// Phase 4: Compact up to index 12.
	err = lr.Compact(12)
	if err != nil {
		t.Fatalf("Phase 4: Compact(12) error: %v", err)
	}

	_, err = lr.Term(5)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("Phase 4: Term(5) error = %v, want ErrCompacted", err)
	}

	term, err = lr.Term(12)
	if err != nil {
		t.Fatalf("Phase 4: Term(12) error: %v", err)
	}
	// markerTerm should be the term at index 12.
	if term != 2 {
		t.Errorf("Phase 4: Term(12) = %d, want 2", term)
	}

	// Phase 5: Add more entries and compact further.
	entries3 := makeEntries(21, 5, 3)
	db.addEntries(shardID, replicaID, entries3)
	mustSetRange(t, lr, 21, 5)

	lr.mu.RLock()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if last != 25 {
		t.Errorf("Phase 5: lastIndex = %d, want 25", last)
	}

	// Phase 6: Verify state.
	state, _ := lr.NodeState()
	if state.Term != 2 {
		t.Errorf("Phase 6: state.Term = %d, want 2", state.Term)
	}
	if state.Commit != 20 {
		t.Errorf("Phase 6: state.Commit = %d, want 20", state.Commit)
	}
}

// --- Edge case: SetRange with entries ending before firstIndex ---

func TestLogReaderSetRangeEntriesBeforeFirstIndex(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Apply snapshot at index 100 (markerIndex=100, firstIndex=101).
	ss := proto.Snapshot{
		Index: 100,
		Term:  5,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	// SetRange with entries [50..55]: all before firstIndex=101.
	// These should be silently ignored.
	mustSetRange(t, lr, 50, 6) // entries [50..55]

	lr.mu.RLock()
	length := lr.length
	lr.mu.RUnlock()

	// Length should still be 1 (just the marker).
	if length != 1 {
		t.Errorf("length = %d, want 1 after SetRange with old entries", length)
	}
}

// --- Edge case: Term at exact boundary after ApplySnapshot ---

func TestLogReaderTermAtSnapshotBoundary(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	ss := proto.Snapshot{
		Index: 50,
		Term:  7,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	// Term at markerIndex (50) should return cached markerTerm (7).
	term, err := lr.Term(50)
	if err != nil {
		t.Fatalf("Term(50) returned error: %v", err)
	}
	if term != 7 {
		t.Errorf("Term(50) = %d, want 7", term)
	}

	// Term at index 49 should be compacted.
	_, err = lr.Term(49)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("Term(49) error = %v, want ErrCompacted", err)
	}

	// Term at index 51 should be unavailable (no entries yet).
	_, err = lr.Term(51)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("Term(51) error = %v, want ErrUnavailable", err)
	}
}

// --- Edge case: Compact at current markerIndex ---

func TestLogReaderCompactAtCurrentMarkerIsNoop(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	lr.mu.RLock()
	lengthBefore := lr.length
	lr.mu.RUnlock()

	// Compacting at the current marker (5) should be a no-op.
	err = lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) at current marker returned error: %v", err)
	}

	lr.mu.RLock()
	lengthAfter := lr.length
	lr.mu.RUnlock()

	if lengthBefore != lengthAfter {
		t.Errorf("length changed from %d to %d after compact at marker",
			lengthBefore, lengthAfter)
	}
}

// --- Multiple snapshots test ---

func TestLogReaderMultipleSnapshotsProgressively(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Apply progressively newer snapshots.
	for i := uint64(10); i <= 50; i += 10 {
		ss := proto.Snapshot{
			Index: i,
			Term:  i / 10,
			Membership: proto.Membership{
				ConfigChangeID: i,
				Addresses:      map[uint64]string{1: "addr1"},
			},
		}
		err := lr.ApplySnapshot(ss)
		if err != nil {
			t.Fatalf("ApplySnapshot(index=%d) returned error: %v", i, err)
		}

		lr.mu.RLock()
		marker := lr.markerIndex
		markerTerm := lr.markerTerm
		lr.mu.RUnlock()

		if marker != i {
			t.Errorf("after snapshot at %d: markerIndex = %d", i, marker)
		}
		if markerTerm != i/10 {
			t.Errorf("after snapshot at %d: markerTerm = %d, want %d", i, markerTerm, i/10)
		}
	}
}

// --- Interface satisfaction test ---

func TestLogReaderSatisfiesRaftLogReaderInterface(t *testing.T) {
	// This test verifies that LogReader has all methods matching the
	// raft.LogReader interface. Since raft.LogReader is in an internal
	// package, we verify by calling each method that the interface requires.
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// GetRange(low, high, maxSize uint64) ([]proto.Entry, error)
	_, err := lr.GetRange(0, 0, 0)
	_ = err

	// Term(index uint64) (uint64, error)
	_, err = lr.Term(0)
	_ = err

	// SetRange(firstIndex, length uint64)
	mustSetRange(t, lr, 1, 0)

	// SetState(state proto.State)
	lr.SetState(proto.State{})

	// Append(entries []proto.Entry)
	mustAppend(t, lr, nil)

	// NodeState() (proto.State, proto.Membership)
	_, _ = lr.NodeState()

	// CreateSnapshot(ss proto.Snapshot) error
	_ = lr.CreateSnapshot(proto.Snapshot{})

	// ApplySnapshot(ss proto.Snapshot) error
	_ = lr.ApplySnapshot(proto.Snapshot{})

	// Compact(index uint64) error
	_ = lr.Compact(0)
}

// --- GetRange with maxSize test ---

func TestLogReaderGetRangeRespectsMaxSize(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// Request with a small maxSize that should limit returned entries.
	// Each entry has ~24 bytes (16 header + 8 cmd "test-cmd").
	got, err := lr.GetRange(1, 11, 20)
	if err != nil {
		t.Fatalf("GetRange with maxSize returned error: %v", err)
	}

	// With maxSize=20, we should get at least 1 entry but fewer than 10.
	if len(got) == 0 {
		t.Error("GetRange with maxSize returned 0 entries, want at least 1")
	}
	if len(got) > 5 {
		t.Errorf("GetRange with maxSize=20 returned %d entries, expected fewer", len(got))
	}
}

func TestLogReaderGetRangeMaxSizeZeroReturnsAllEntries(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// maxSize=0 means unlimited: all entries should be returned.
	got, err := lr.GetRange(1, 11, 0)
	if err != nil {
		t.Fatalf("GetRange with maxSize=0 returned error: %v", err)
	}
	if len(got) != 10 {
		t.Errorf("GetRange with maxSize=0 returned %d entries, want 10", len(got))
	}
}

// --- Append with contiguous entries test ---

func TestLogReaderAppendMultipleBatches(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Append three batches contiguously.
	mustAppend(t, lr, makeProtoEntries(1, 3, 1))
	mustAppend(t, lr, makeProtoEntries(4, 3, 1))
	mustAppend(t, lr, makeProtoEntries(7, 3, 1))

	lr.mu.RLock()
	first := lr.firstIndex()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if first != 1 {
		t.Errorf("firstIndex = %d, want 1", first)
	}
	if last != 9 {
		t.Errorf("lastIndex = %d, want 9", last)
	}
}

// --- CreateSnapshot then ApplySnapshot test ---

func TestLogReaderCreateSnapshotThenApplySnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)
	mustSetRange(t, lr, 1, 20)

	// Create snapshot (does not change range).
	ss := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			ConfigChangeID: 5,
			Addresses:      map[uint64]string{1: "a1"},
		},
	}
	err := lr.CreateSnapshot(ss)
	if err != nil {
		t.Fatalf("CreateSnapshot() returned error: %v", err)
	}

	lr.mu.RLock()
	lastAfterCreate := lr.lastIndex()
	lr.mu.RUnlock()

	if lastAfterCreate != 20 {
		t.Errorf("lastIndex after CreateSnapshot = %d, want 20", lastAfterCreate)
	}

	// Apply snapshot (resets range).
	ss2 := proto.Snapshot{
		Index: 15,
		Term:  3,
		Membership: proto.Membership{
			ConfigChangeID: 8,
			Addresses:      map[uint64]string{1: "a1", 2: "a2"},
		},
	}
	err = lr.ApplySnapshot(ss2)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	lr.mu.RLock()
	marker := lr.markerIndex
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if marker != 15 {
		t.Errorf("markerIndex = %d, want 15", marker)
	}
	if last != 15 {
		t.Errorf("lastIndex = %d, want 15 (only marker, no entries)", last)
	}

	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 8 {
		t.Errorf("membership.ConfigChangeID = %d, want 8", membership.ConfigChangeID)
	}
}

// --- GetRange with single entry ---

func TestLogReaderGetRangeSingleEntry(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 5, 3)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 5)

	got, err := lr.GetRange(3, 4, 0)
	if err != nil {
		t.Fatalf("GetRange(3, 4) returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("GetRange(3, 4) returned %d entries, want 1", len(got))
	}
	if got[0].Index != 3 {
		t.Errorf("entry.Index = %d, want 3", got[0].Index)
	}
	if got[0].Term != 3 {
		t.Errorf("entry.Term = %d, want 3", got[0].Term)
	}
}

// --- Compact followed by SetRange ---

func TestLogReaderCompactThenSetRange(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// Compact up to 5.
	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	// Add more entries [11..15].
	moreEntries := makeEntries(11, 5, 2)
	db.addEntries(shardID, replicaID, moreEntries)
	mustSetRange(t, lr, 11, 5)

	lr.mu.RLock()
	first := lr.firstIndex()
	last := lr.lastIndex()
	lr.mu.RUnlock()

	if first != 6 {
		t.Errorf("firstIndex = %d, want 6", first)
	}
	if last != 15 {
		t.Errorf("lastIndex = %d, want 15", last)
	}
}

// --- Additional edge case tests for branch coverage ---

func TestLogReaderGetRangeMaxSizeTrimsExcess(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Create entries with large commands to trigger the size trimming path.
	var entries []Entry
	for i := uint64(1); i <= 10; i++ {
		entries = append(entries, Entry{
			Index: i,
			Term:  1,
			Cmd:   make([]byte, 100), // 100 bytes each
		})
	}
	db.addEntries(shardID, replicaID, entries)
	mustSetRange(t, lr, 1, 10)

	// Request with maxSize that allows a few entries but not all.
	// Each entry is ~116 bytes (16 header + 100 cmd).
	// Setting maxSize=250 should allow about 2 entries.
	got, err := lr.GetRange(1, 11, 250)
	if err != nil {
		t.Fatalf("GetRange with maxSize=250 returned error: %v", err)
	}
	if len(got) == 0 {
		t.Error("GetRange with maxSize=250 returned 0 entries, want at least 1")
	}
	if len(got) >= 10 {
		t.Errorf("GetRange with maxSize=250 returned %d entries, expected fewer than 10", len(got))
	}
}

func TestLogReaderSetRangeLastBeforeFirst(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Apply snapshot at index 100 to set a high firstIndex.
	ss := proto.Snapshot{
		Index: 100,
		Term:  5,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	// firstIndex is now 101. SetRange with entries [50..52]:
	// last = 50+3-1 = 52, which is < first=101, so it should be ignored.
	lr.mu.RLock()
	lengthBefore := lr.length
	lr.mu.RUnlock()

	mustSetRange(t, lr, 50, 3) // entries [50..52], all before firstIndex=101

	lr.mu.RLock()
	lengthAfter := lr.length
	lr.mu.RUnlock()

	if lengthBefore != lengthAfter {
		t.Errorf("length changed from %d to %d after SetRange with old entries",
			lengthBefore, lengthAfter)
	}
}

func TestLogReaderGetRangeLogDBError(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	mustSetRange(t, lr, 1, 5)

	// Close the DB to trigger an error on IterateEntries.
	closeErr := db.Close()
	if closeErr != nil {
		t.Fatalf("Close() returned error: %v", closeErr)
	}

	_, err := lr.GetRange(1, 6, 0)
	if err == nil {
		t.Error("GetRange on closed LogDB returned nil error, want error")
	}
	if !errors.Is(err, ErrClosed) {
		t.Errorf("GetRange on closed LogDB error = %v, want ErrClosed", err)
	}
}

func TestLogReaderTermLogDBError(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	mustSetRange(t, lr, 1, 5)

	// Close the DB to trigger an error on IterateEntries via Term.
	closeErr := db.Close()
	if closeErr != nil {
		t.Fatalf("Close() returned error: %v", closeErr)
	}

	_, err := lr.Term(3)
	if err == nil {
		t.Error("Term on closed LogDB returned nil error, want error")
	}
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Term on closed LogDB error = %v, want ErrClosed", err)
	}
}

func TestLogReaderCompactLogDBErrorPropagation(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Add entries to LogDB and set range.
	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// Close the DB so that Compact's internal Term lookup fails.
	closeErr := db.Close()
	if closeErr != nil {
		t.Fatalf("Close() returned error: %v", closeErr)
	}

	// Compact to index 5 should fail because termLocked can't read the term.
	err := lr.Compact(5)
	if err == nil {
		t.Error("Compact on closed LogDB returned nil error, want error")
	}
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Compact on closed LogDB error = %v, want ErrClosed", err)
	}

	// Verify markerIndex was NOT advanced (Compact failed before update).
	lr.mu.RLock()
	marker := lr.markerIndex
	lr.mu.RUnlock()

	if marker != 0 {
		t.Errorf("markerIndex = %d, want 0 (should not advance on error)", marker)
	}
}

// TestLogReaderTermWhenLogDBReturnsEmpty is a regression test: an empty
// LogDB result for an in-range index must surface as ErrUnavailable, never
// as term 0 — Raft's "no term" sentinel — which would make followers reject
// appends forever (as prevLogTerm) or grant votes to shorter logs (as the
// local last-log term).
func TestLogReaderTermWhenLogDBReturnsEmpty(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Set range to [1..5] but DO NOT add entries to LogDB.
	// This simulates a scenario where LogDB has no matching entries.
	mustSetRange(t, lr, 1, 5)

	// Term(3) will call getRangeLocked which calls IterateEntries,
	// which returns an empty slice (no entries in LogDB).
	term, err := lr.Term(3)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(3) = (%d, %v), want ErrUnavailable for missing entries", term, err)
	}
	if term != 0 {
		t.Errorf("Term(3) = %d, want 0 alongside the error", term)
	}
}

// wrongIndexLogDB wraps memLogDB but returns an entry whose Index does not
// match the requested range start, simulating a buggy or gapped backend.
type wrongIndexLogDB struct {
	*memLogDB
}

func (w *wrongIndexLogDB) IterateEntries(
	ents []Entry, size uint64,
	shardID, replicaID, low, high, maxSize uint64,
) ([]Entry, uint64, error) {
	return append(ents, Entry{Index: low + 1, Term: 7}), size, nil
}

// TestLogReaderTermRejectsMismatchedIndex verifies the defensive guard in
// termLocked: a backend result whose index does not match the request must
// surface as ErrUnavailable, never as another entry's term.
func TestLogReaderTermRejectsMismatchedIndex(t *testing.T) {
	lr := NewLogReader(1, 1, &wrongIndexLogDB{newMemLogDB()})
	mustSetRange(t, lr, 1, 5)

	if _, err := lr.Term(3); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(3) = %v, want ErrUnavailable for mismatched entry index", err)
	}
}

func TestLogReaderCreateSnapshotOnEmptyReader(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// CreateSnapshot on a freshly created reader should succeed
	// since there's no existing snapshot to compare against.
	ss := proto.Snapshot{
		Index: 0,
		Term:  0,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.CreateSnapshot(ss)
	if err != nil {
		t.Errorf("CreateSnapshot on empty reader returned error: %v", err)
	}
}

// --- termBuf and entryCache tests ---

func TestLogReaderAppendPopulatesTermBuf(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	entries := makeProtoEntries(1, 5, 3)
	mustAppend(t, lr, entries)

	// Verify termBuf is populated for each appended entry.
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	if lr.termBuf == nil {
		t.Fatal("termBuf is nil after Append, want non-nil")
	}
	for _, e := range entries {
		term, ok := lr.termBuf.lookup(e.Index)
		if !ok {
			t.Errorf("termBuf missing entry for index %d", e.Index)
			continue
		}
		if term != e.Term {
			t.Errorf("termBuf[%d] = %d, want %d", e.Index, term, e.Term)
		}
	}
}

func TestLogReaderTermUsesTermBufBeforeLogDB(t *testing.T) {
	// Create LogReader with nil LogDB. Append entries so termBuf is populated.
	// Term() should succeed from the cache without any LogDB backing.
	lr := NewLogReader(1, 1, nil)

	entries := makeProtoEntries(1, 5, 7)
	mustAppend(t, lr, entries)

	for _, e := range entries {
		term, err := lr.Term(e.Index)
		if err != nil {
			t.Fatalf("Term(%d) returned error: %v", e.Index, err)
		}
		if term != e.Term {
			t.Errorf("Term(%d) = %d, want %d", e.Index, term, e.Term)
		}
	}
}

func TestLogReaderAppendPopulatesEntryCacheWhenNilLogDB(t *testing.T) {
	// Create LogReader with nil LogDB. Append entries and verify GetRange
	// succeeds from the entry cache.
	lr := NewLogReader(1, 1, nil)

	entries := makeProtoEntries(1, 5, 2)
	mustAppend(t, lr, entries)

	got, err := lr.GetRange(1, 6, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 6) returned error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetRange(1, 6) returned %d entries, want 5", len(got))
	}
	for i, e := range got {
		wantIdx := uint64(i + 1)
		if e.Index != wantIdx {
			t.Errorf("entry[%d].Index = %d, want %d", i, e.Index, wantIdx)
		}
		if e.Term != 2 {
			t.Errorf("entry[%d].Term = %d, want 2", i, e.Term)
		}
	}
}

func TestLogReaderGetRangeWithNilLogDBNoCache(t *testing.T) {
	// Create LogReader with nil LogDB, set range but don't Append.
	// GetRange should return ErrUnavailable because entryCache is nil.
	lr := NewLogReader(1, 1, nil)

	// Manually set the range so that bounds checks pass.
	mustSetRange(t, lr, 1, 5)

	_, err := lr.GetRange(1, 6, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("GetRange with nil LogDB and no cache error = %v, want ErrUnavailable", err)
	}
}

func TestLogReaderCompactCleansTermBufAndEntryCache(t *testing.T) {
	// Create LogReader with nil LogDB. Append entries, Compact, verify
	// old entries are removed from both caches but newer entries remain.
	lr := NewLogReader(1, 1, nil)

	entries := makeProtoEntries(1, 10, 4)
	mustAppend(t, lr, entries)

	// Compact up to index 5.
	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	lr.mu.RLock()
	defer lr.mu.RUnlock()

	// Entries 1-5 should be removed from termBuf.
	for i := uint64(1); i <= 5; i++ {
		if _, ok := lr.termBuf.lookup(i); ok {
			t.Errorf("termBuf still contains compacted index %d", i)
		}
	}

	// Entries 6-10 should still exist in termBuf.
	for i := uint64(6); i <= 10; i++ {
		term, ok := lr.termBuf.lookup(i)
		if !ok {
			t.Errorf("termBuf missing entry for index %d after compact", i)
			continue
		}
		if term != 4 {
			t.Errorf("termBuf[%d] = %d, want 4", i, term)
		}
	}

	// Entries 1-5 should be removed from entryCache.
	for i := uint64(1); i <= 5; i++ {
		if _, ok := lr.entryCache[i]; ok {
			t.Errorf("entryCache still contains compacted index %d", i)
		}
	}

	// Entries 6-10 should still exist in entryCache.
	for i := uint64(6); i <= 10; i++ {
		if _, ok := lr.entryCache[i]; !ok {
			t.Errorf("entryCache missing entry for index %d after compact", i)
		}
	}
}

func TestLogReaderApplySnapshotClearsCaches(t *testing.T) {
	// Create LogReader with nil LogDB, populate caches, apply snapshot,
	// verify both termBuf and entryCache are cleared.
	lr := NewLogReader(1, 1, nil)

	entries := makeProtoEntries(1, 10, 3)
	mustAppend(t, lr, entries)

	// Verify caches are populated.
	lr.mu.RLock()
	if lr.termBuf == nil || lr.termBuf.len() == 0 {
		t.Fatal("termBuf empty before ApplySnapshot")
	}
	if len(lr.entryCache) == 0 {
		t.Fatal("entryCache empty before ApplySnapshot")
	}
	lr.mu.RUnlock()

	ss := proto.Snapshot{
		Index: 50,
		Term:  7,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss)
	if err != nil {
		t.Fatalf("ApplySnapshot() returned error: %v", err)
	}

	lr.mu.RLock()
	defer lr.mu.RUnlock()

	if lr.termBuf.len() != 0 {
		t.Errorf("termBuf has %d entries after ApplySnapshot, want 0", lr.termBuf.len())
	}
	if len(lr.entryCache) != 0 {
		t.Errorf("entryCache has %d entries after ApplySnapshot, want 0", len(lr.entryCache))
	}
}

func TestLogReaderNilLogDBGetRangePartialCache(t *testing.T) {
	// Create LogReader with nil LogDB, Append only entries 1-3, set range
	// to 1-5, verify GetRange for the full range returns ErrUnavailable.
	lr := NewLogReader(1, 1, nil)

	// Append entries 1-3 (this also sets range to cover 1-3).
	entries := makeProtoEntries(1, 3, 2)
	mustAppend(t, lr, entries)

	// Manually extend the range to include indices 4-5 (no cached entries).
	mustSetRange(t, lr, 4, 2)

	// Requesting the full range [1, 6) should fail because indices 4 and 5
	// are not in the entry cache.
	_, err := lr.GetRange(1, 6, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("GetRange(1, 6) with partial cache error = %v, want ErrUnavailable", err)
	}

	// Requesting only cached range [1, 4) should succeed.
	got, err := lr.GetRange(1, 4, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 4) with full cache returned error: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("GetRange(1, 4) returned %d entries, want 3", len(got))
	}
}

func TestLogReaderTermBufWithRealLogDB(t *testing.T) {
	// Create LogReader with a real mock LogDB. Append entries via Append()
	// (which populates termBuf) but do NOT add entries to the LogDB directly.
	// Term() should return the term from termBuf, proving the cache takes
	// priority over LogDB (which would return 0 since it has no entries).
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	entries := makeProtoEntries(1, 5, 9)
	mustAppend(t, lr, entries)

	// LogDB has no entries for this shard/replica. Without the termBuf,
	// Term() would fall through to getRangeLocked -> IterateEntries which
	// would return an empty slice, yielding term 0.
	for _, e := range entries {
		term, err := lr.Term(e.Index)
		if err != nil {
			t.Fatalf("Term(%d) returned error: %v", e.Index, err)
		}
		if term != 9 {
			t.Errorf("Term(%d) = %d, want 9 (from termBuf, not LogDB)", e.Index, term)
		}
	}
}

func TestLogReaderEntryCacheNotPopulatedWithRealLogDB(t *testing.T) {
	// Create LogReader with a real mock LogDB. Append entries. Verify that
	// entryCache is nil because entries should only be cached when LogDB is nil.
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	entries := makeProtoEntries(1, 5, 2)
	mustAppend(t, lr, entries)

	lr.mu.RLock()
	defer lr.mu.RUnlock()

	if lr.entryCache != nil {
		t.Errorf("entryCache = %v, want nil when LogDB is non-nil", lr.entryCache)
	}

	// termBuf should still be populated (it is always populated).
	if lr.termBuf == nil {
		t.Error("termBuf is nil, want non-nil even with real LogDB")
	}
	if lr.termBuf.len() != 5 {
		t.Errorf("termBuf has %d entries, want 5", lr.termBuf.len())
	}
}

// --- SetMembership tests ---

func TestLogReaderSetMembershipUpdatesMembership(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	m := proto.Membership{
		ConfigChangeID: 42,
		Addresses:      map[uint64]string{1: "addr1", 2: "addr2"},
	}
	lr.SetMembership(m)

	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 42 {
		t.Errorf("membership.ConfigChangeID = %d, want 42", membership.ConfigChangeID)
	}
	if len(membership.Addresses) != 2 {
		t.Errorf("membership.Addresses length = %d, want 2", len(membership.Addresses))
	}
}

func TestLogReaderSetMembershipOverwritesPrevious(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	m1 := proto.Membership{
		ConfigChangeID: 1,
		Addresses:      map[uint64]string{1: "addr1"},
	}
	lr.SetMembership(m1)

	m2 := proto.Membership{
		ConfigChangeID: 99,
		Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	}
	lr.SetMembership(m2)

	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 99 {
		t.Errorf("membership.ConfigChangeID = %d, want 99", membership.ConfigChangeID)
	}
	if len(membership.Addresses) != 3 {
		t.Errorf("membership.Addresses length = %d, want 3", len(membership.Addresses))
	}
}

// --- Additional nil-LogDB edge case tests ---

func TestLogReaderNilLogDBAppendMultipleBatchesThenGetRange(t *testing.T) {
	// Verify that multiple Append calls on a nil-LogDB reader accumulate
	// entries correctly in the entryCache and serve via GetRange.
	lr := NewLogReader(1, 1, nil)

	mustAppend(t, lr, makeProtoEntries(1, 3, 1))
	mustAppend(t, lr, makeProtoEntries(4, 3, 2))
	mustAppend(t, lr, makeProtoEntries(7, 4, 3))

	got, err := lr.GetRange(1, 11, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 11) returned error: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("GetRange(1, 11) returned %d entries, want 10", len(got))
	}

	// Verify terms change across batches.
	if got[0].Term != 1 {
		t.Errorf("entry[0].Term = %d, want 1", got[0].Term)
	}
	if got[3].Term != 2 {
		t.Errorf("entry[3].Term = %d, want 2", got[3].Term)
	}
	if got[6].Term != 3 {
		t.Errorf("entry[6].Term = %d, want 3", got[6].Term)
	}
}

func TestLogReaderNilLogDBCompactThenGetRange(t *testing.T) {
	// Full lifecycle with nil LogDB: Append, Compact, verify remaining
	// entries are still accessible via GetRange.
	lr := NewLogReader(1, 1, nil)

	entries := makeProtoEntries(1, 10, 5)
	mustAppend(t, lr, entries)

	err := lr.Compact(5)
	if err != nil {
		t.Fatalf("Compact(5) returned error: %v", err)
	}

	// Compacted entries should return ErrCompacted.
	_, err = lr.GetRange(3, 6, 0)
	if !errors.Is(err, ErrCompacted) {
		t.Errorf("GetRange(3, 6) after compact error = %v, want ErrCompacted", err)
	}

	// Remaining entries [6, 11) should be accessible.
	got, err := lr.GetRange(6, 11, 0)
	if err != nil {
		t.Fatalf("GetRange(6, 11) after compact returned error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetRange(6, 11) returned %d entries, want 5", len(got))
	}
	if got[0].Index != 6 {
		t.Errorf("first entry Index = %d, want 6", got[0].Index)
	}
	if got[4].Index != 10 {
		t.Errorf("last entry Index = %d, want 10", got[4].Index)
	}
}

func TestLogReaderApplySnapshotSameIndexAfterState(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Set initial state and snapshot.
	lr.SetState(proto.State{Term: 1, Vote: 1, Commit: 10})
	ss1 := proto.Snapshot{
		Index: 10,
		Term:  1,
		Membership: proto.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err := lr.ApplySnapshot(ss1)
	if err != nil {
		t.Fatalf("ApplySnapshot(ss1) returned error: %v", err)
	}

	// Applying a snapshot at the same index should return ErrSnapshotOutOfDate
	// because ss2.Index (10) <= markerIndex (10).
	ss2 := proto.Snapshot{
		Index: 10,
		Term:  1,
		Membership: proto.Membership{
			ConfigChangeID: 2,
			Addresses:      map[uint64]string{1: "addr1"},
		},
	}
	err = lr.ApplySnapshot(ss2)
	if !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Errorf("ApplySnapshot(same index) error = %v, want ErrSnapshotOutOfDate", err)
	}
}

// --- Buffer reuse tests ---

func TestLogReaderGetRangeReusesProtoEntryBuffer(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 10, 1)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 10)

	// First call: allocates buffer.
	got1, err := lr.GetRange(1, 6, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 6) returned error: %v", err)
	}
	if len(got1) != 5 {
		t.Fatalf("GetRange(1, 6) returned %d entries, want 5", len(got1))
	}

	// Capture buffer capacity after first call.
	lr.mu.Lock()
	protoCap := cap(lr.protoEntryBuf)
	logdbCap := cap(lr.logdbEntryBuf)
	lr.mu.Unlock()

	if protoCap == 0 {
		t.Fatal("protoEntryBuf capacity is 0 after first GetRange, want > 0")
	}
	if logdbCap == 0 {
		t.Fatal("logdbEntryBuf capacity is 0 after first GetRange, want > 0")
	}

	// Second call with a smaller range: should reuse the existing buffer
	// capacity without reallocating.
	got2, err := lr.GetRange(1, 4, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 4) returned error: %v", err)
	}
	if len(got2) != 3 {
		t.Fatalf("GetRange(1, 4) returned %d entries, want 3", len(got2))
	}

	// Verify capacity was preserved (not shrunk).
	lr.mu.Lock()
	protoCapAfter := cap(lr.protoEntryBuf)
	logdbCapAfter := cap(lr.logdbEntryBuf)
	lr.mu.Unlock()

	if protoCapAfter < protoCap {
		t.Errorf("protoEntryBuf capacity shrunk from %d to %d", protoCap, protoCapAfter)
	}
	if logdbCapAfter < logdbCap {
		t.Errorf("logdbEntryBuf capacity shrunk from %d to %d", logdbCap, logdbCapAfter)
	}
}

func TestLogReaderGetRangeBufferGrowsForLargerRange(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	dbEntries := makeEntries(1, 20, 2)
	db.addEntries(shardID, replicaID, dbEntries)
	mustSetRange(t, lr, 1, 20)

	// First call: small range.
	_, err := lr.GetRange(1, 4, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 4) returned error: %v", err)
	}

	lr.mu.Lock()
	smallCap := cap(lr.protoEntryBuf)
	lr.mu.Unlock()

	// Second call: larger range that exceeds current capacity.
	got, err := lr.GetRange(1, 16, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 16) returned error: %v", err)
	}
	if len(got) != 15 {
		t.Fatalf("GetRange(1, 16) returned %d entries, want 15", len(got))
	}

	lr.mu.Lock()
	largeCap := cap(lr.protoEntryBuf)
	lr.mu.Unlock()

	if largeCap < 15 {
		t.Errorf("protoEntryBuf capacity = %d, want >= 15", largeCap)
	}
	if largeCap < smallCap {
		t.Errorf("protoEntryBuf capacity shrunk from %d to %d after larger range",
			smallCap, largeCap)
	}
}

func TestLogReaderGetRangeBufferContentCorrectAfterReuse(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)

	// Create entries with distinct terms per index for verification.
	var entries []Entry
	for i := uint64(1); i <= 10; i++ {
		entries = append(entries, Entry{
			Index: i,
			Term:  i * 10,
			Cmd:   []byte("cmd"),
		})
	}
	db.addEntries(shardID, replicaID, entries)
	mustSetRange(t, lr, 1, 10)

	// First call: range [1, 6).
	got1, err := lr.GetRange(1, 6, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 6) returned error: %v", err)
	}
	for i, e := range got1 {
		wantTerm := uint64(i+1) * 10
		if e.Term != wantTerm {
			t.Errorf("call1 entry[%d].Term = %d, want %d", i, e.Term, wantTerm)
		}
	}

	// Second call: range [6, 11). Buffer should have correct NEW content,
	// not stale data from the first call.
	got2, err := lr.GetRange(6, 11, 0)
	if err != nil {
		t.Fatalf("GetRange(6, 11) returned error: %v", err)
	}
	for i, e := range got2 {
		wantTerm := uint64(i+6) * 10
		if e.Term != wantTerm {
			t.Errorf("call2 entry[%d].Term = %d, want %d", i, e.Term, wantTerm)
		}
	}
}

// --- logdbEntriesToProtoReuse tests ---

func TestLogReaderLogdbEntriesToProtoReuseConvertsCorrectly(t *testing.T) {
	input := []Entry{
		{Index: 1, Term: 1, Type: 0, Cmd: []byte("cmd1")},
		{Index: 2, Term: 1, Type: 1, Cmd: []byte("cmd2")},
		{Index: 3, Term: 2, Type: 0, Cmd: nil},
	}

	var buf []proto.Entry
	buf = logdbEntriesToProtoReuse(buf, input)

	if len(buf) != 3 {
		t.Fatalf("logdbEntriesToProtoReuse returned %d entries, want 3", len(buf))
	}
	if buf[0].Index != 1 || buf[0].Term != 1 || string(buf[0].Cmd) != "cmd1" {
		t.Errorf("entry[0] = %+v, want Index=1 Term=1 Cmd=cmd1", buf[0])
	}
	if buf[1].Type != 1 {
		t.Errorf("entry[1].Type = %d, want 1", buf[1].Type)
	}
	if buf[2].Cmd != nil {
		t.Errorf("entry[2].Cmd = %v, want nil", buf[2].Cmd)
	}
}

func TestLogReaderLogdbEntriesToProtoReuseEmptyInput(t *testing.T) {
	buf := make([]proto.Entry, 5)
	result := logdbEntriesToProtoReuse(buf, nil)
	if len(result) != 0 {
		t.Errorf("logdbEntriesToProtoReuse(buf, nil) len = %d, want 0", len(result))
	}
	// Capacity should be preserved.
	if cap(result) != 5 {
		t.Errorf("logdbEntriesToProtoReuse(buf, nil) cap = %d, want 5", cap(result))
	}

	result = logdbEntriesToProtoReuse(buf, []Entry{})
	if len(result) != 0 {
		t.Errorf("logdbEntriesToProtoReuse(buf, []Entry{}) len = %d, want 0", len(result))
	}
}

func TestLogReaderLogdbEntriesToProtoReusePreservesCapacity(t *testing.T) {
	// Pre-allocate a buffer with capacity 10.
	buf := make([]proto.Entry, 0, 10)

	// Convert 3 entries: should reuse capacity, not reallocate.
	input := []Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	}
	result := logdbEntriesToProtoReuse(buf, input)

	if len(result) != 3 {
		t.Fatalf("result len = %d, want 3", len(result))
	}
	if cap(result) != 10 {
		t.Errorf("result cap = %d, want 10 (should reuse original capacity)", cap(result))
	}
}

func TestLogReaderLogdbEntriesToProtoReuseGrowsWhenNeeded(t *testing.T) {
	// Start with a small buffer.
	buf := make([]proto.Entry, 0, 2)

	// Convert 5 entries: should allocate a new buffer.
	input := makeEntries(1, 5, 1)
	result := logdbEntriesToProtoReuse(buf, input)

	if len(result) != 5 {
		t.Fatalf("result len = %d, want 5", len(result))
	}
	if cap(result) < 5 {
		t.Errorf("result cap = %d, want >= 5", cap(result))
	}
}

func TestLogReaderLogdbEntriesToProtoReuseNilBuffer(t *testing.T) {
	// Starting with a nil buffer should work (allocates fresh).
	input := makeEntries(1, 3, 2)
	result := logdbEntriesToProtoReuse(nil, input)

	if len(result) != 3 {
		t.Fatalf("result len = %d, want 3", len(result))
	}
	if result[0].Index != 1 || result[0].Term != 2 {
		t.Errorf("entry[0] = %+v, want Index=1 Term=2", result[0])
	}
}

// --- termBuf bounding tests ---

func TestLogReaderTermBufBoundsAfterManyAppends(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// Append enough entries to exceed maxTermCacheEntries.
	// The ring buffer auto-evicts oldest entries when full.
	batchSize := uint64(10_000)
	totalEntries := uint64(maxTermCacheEntries) + batchSize

	for i := uint64(1); i <= totalEntries; i += batchSize {
		count := batchSize
		if i+count-1 > totalEntries {
			count = totalEntries - i + 1
		}
		entries := makeProtoEntries(i, count, 1)
		mustAppend(t, lr, entries)
	}

	lr.mu.Lock()
	bufSize := lr.termBuf.len()
	lr.mu.Unlock()

	// Ring buffer should never exceed its capacity.
	if bufSize > maxTermCacheEntries {
		t.Errorf("termBuf size = %d, want <= %d", bufSize, maxTermCacheEntries)
	}
	// It should be exactly at capacity since we added more than capacity.
	if bufSize != maxTermCacheEntries {
		t.Errorf("termBuf size = %d, want exactly %d (full ring)", bufSize, maxTermCacheEntries)
	}
}

func TestLogReaderTermBufEvictsOldestOnOverflow(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// Append entries and compact some to advance markerIndex.
	entries := makeProtoEntries(1, 100, 1)
	mustAppend(t, lr, entries)

	err := lr.Compact(50)
	if err != nil {
		t.Fatalf("Compact(50) returned error: %v", err)
	}

	// Now append a very large batch that overflows the ring buffer.
	bigBatch := makeProtoEntries(101, uint64(maxTermCacheEntries), 2)
	mustAppend(t, lr, bigBatch)

	lr.mu.Lock()
	defer lr.mu.Unlock()

	// Ring buffer should be bounded.
	if lr.termBuf.len() > maxTermCacheEntries {
		t.Errorf("termBuf size = %d, want <= %d", lr.termBuf.len(), maxTermCacheEntries)
	}

	// The most recent entries should be in the buffer.
	lastIdx := uint64(100) + uint64(maxTermCacheEntries)
	if _, ok := lr.termBuf.lookup(lastIdx); !ok {
		t.Errorf("termBuf missing most recent entry at index %d", lastIdx)
	}
}

func TestLogReaderTermBufRetainsRecentEntries(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// Append maxTermCacheEntries + 50_000 entries.
	total := uint64(maxTermCacheEntries + 50_000)
	batchSize := uint64(10_000)
	for i := uint64(1); i <= total; i += batchSize {
		count := batchSize
		if i+count-1 > total {
			count = total - i + 1
		}
		mustAppend(t, lr, makeProtoEntries(i, count, 3))
	}

	lr.mu.Lock()
	bufSize := lr.termBuf.len()
	// Verify the most recent entries are still in the buffer.
	_, hasLast := lr.termBuf.lookup(total)
	_, hasSecondToLast := lr.termBuf.lookup(total - 1)
	// Oldest entries should have been evicted.
	_, hasFirst := lr.termBuf.lookup(1)
	lr.mu.Unlock()

	if bufSize > maxTermCacheEntries {
		t.Errorf("termBuf size = %d, want <= %d", bufSize, maxTermCacheEntries)
	}
	if !hasLast {
		t.Errorf("termBuf missing most recent entry at index %d", total)
	}
	if !hasSecondToLast {
		t.Errorf("termBuf missing entry at index %d", total-1)
	}
	if hasFirst {
		t.Error("termBuf should have evicted oldest entry at index 1")
	}
}

func TestLogReaderTermBufBelowLimitNoEviction(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// Append fewer entries than the limit. No eviction should occur.
	entries := makeProtoEntries(1, 100, 1)
	mustAppend(t, lr, entries)

	lr.mu.Lock()
	bufSize := lr.termBuf.len()
	lr.mu.Unlock()

	if bufSize != 100 {
		t.Errorf("termBuf size = %d, want 100 (no eviction expected)", bufSize)
	}
}

func TestLogReaderTermBufTrimToDirectly(t *testing.T) {
	// Test the ring buffer trimTo operation directly, simulating
	// what Compact does to remove compacted entries.
	rb := newTermRingBuffer(maxTermCacheEntries)

	// Fill ring buffer with entries 1..200_000. Since capacity is
	// 100_000, entries 1..100_000 will be auto-evicted, leaving
	// entries 100_001..200_000.
	for i := uint64(1); i <= 200_000; i++ {
		rb.append(i, i/1000)
	}

	if rb.len() != maxTermCacheEntries {
		t.Fatalf("ring buffer len = %d, want %d", rb.len(), maxTermCacheEntries)
	}

	// Simulate compaction up to 150_000 by trimming.
	rb.trimTo(150_000)

	// Should have entries 150_001..200_000 = 50_000 entries remaining.
	if rb.len() != 50_000 {
		t.Errorf("ring buffer len after trimTo(150_000) = %d, want 50_000", rb.len())
	}

	// Verify entries above cutoff are retained.
	_, has200k := rb.lookup(200_000)
	_, has150001 := rb.lookup(150_001)
	_, has150k := rb.lookup(150_000)
	_, has100k := rb.lookup(100_000)

	if !has200k {
		t.Error("ring buffer missing entry at index 200_000 after trim")
	}
	if !has150001 {
		t.Error("ring buffer missing entry at index 150_001 after trim")
	}
	if has150k {
		t.Error("ring buffer still contains trimmed entry at index 150_000")
	}
	if has100k {
		t.Error("ring buffer still contains evicted entry at index 100_000")
	}
}

// --- termRingBuffer unit tests ---

func TestTermRingBufferAppendAndLookup(t *testing.T) {
	rb := newTermRingBuffer(10)

	// Append 5 entries.
	for i := uint64(1); i <= 5; i++ {
		rb.append(i, i*10)
	}
	if rb.len() != 5 {
		t.Fatalf("len = %d, want 5", rb.len())
	}

	// Lookup each entry.
	for i := uint64(1); i <= 5; i++ {
		term, ok := rb.lookup(i)
		if !ok {
			t.Errorf("lookup(%d) returned false, want true", i)
			continue
		}
		if term != i*10 {
			t.Errorf("lookup(%d) = %d, want %d", i, term, i*10)
		}
	}

	// Lookup non-existent index.
	_, ok := rb.lookup(99)
	if ok {
		t.Error("lookup(99) returned true for non-existent index")
	}
}

func TestTermRingBufferLookupMiss(t *testing.T) {
	rb := newTermRingBuffer(10)

	// Empty buffer.
	_, ok := rb.lookup(1)
	if ok {
		t.Error("lookup on empty buffer returned true")
	}

	// Add entries 2, 4, 6, 8 (gaps).
	rb.append(2, 20)
	rb.append(4, 40)
	rb.append(6, 60)
	rb.append(8, 80)

	// Lookup existing entries.
	term, ok := rb.lookup(4)
	if !ok || term != 40 {
		t.Errorf("lookup(4) = (%d, %v), want (40, true)", term, ok)
	}

	// Lookup index in gap.
	_, ok = rb.lookup(3)
	if ok {
		t.Error("lookup(3) returned true for non-existent gap index")
	}

	// Lookup below range.
	_, ok = rb.lookup(1)
	if ok {
		t.Error("lookup(1) returned true for index below range")
	}

	// Lookup above range.
	_, ok = rb.lookup(10)
	if ok {
		t.Error("lookup(10) returned true for index above range")
	}
}

func TestTermRingBufferCapacityEviction(t *testing.T) {
	rb := newTermRingBuffer(5)

	// Fill to capacity.
	for i := uint64(1); i <= 5; i++ {
		rb.append(i, i*10)
	}
	if rb.len() != 5 {
		t.Fatalf("len = %d, want 5 (at capacity)", rb.len())
	}

	// Append one more, evicting index 1.
	rb.append(6, 60)
	if rb.len() != 5 {
		t.Errorf("len = %d, want 5 (still at capacity)", rb.len())
	}

	// Index 1 should be evicted.
	_, ok := rb.lookup(1)
	if ok {
		t.Error("lookup(1) returned true after eviction")
	}

	// Index 6 should be present.
	term, ok := rb.lookup(6)
	if !ok || term != 60 {
		t.Errorf("lookup(6) = (%d, %v), want (60, true)", term, ok)
	}

	// Indices 2-5 should still be present.
	for i := uint64(2); i <= 5; i++ {
		term, ok := rb.lookup(i)
		if !ok {
			t.Errorf("lookup(%d) returned false, want true", i)
			continue
		}
		if term != i*10 {
			t.Errorf("lookup(%d) = %d, want %d", i, term, i*10)
		}
	}
}

func TestTermRingBufferTrimTo(t *testing.T) {
	rb := newTermRingBuffer(10)

	for i := uint64(1); i <= 8; i++ {
		rb.append(i, i*100)
	}

	// Trim entries with index <= 5.
	rb.trimTo(5)
	if rb.len() != 3 {
		t.Fatalf("len after trimTo(5) = %d, want 3", rb.len())
	}

	// Indices 1-5 should be gone.
	for i := uint64(1); i <= 5; i++ {
		if _, ok := rb.lookup(i); ok {
			t.Errorf("lookup(%d) returned true after trimTo(5)", i)
		}
	}

	// Indices 6-8 should remain.
	for i := uint64(6); i <= 8; i++ {
		term, ok := rb.lookup(i)
		if !ok {
			t.Errorf("lookup(%d) returned false, want true", i)
			continue
		}
		if term != i*100 {
			t.Errorf("lookup(%d) = %d, want %d", i, term, i*100)
		}
	}
}

func TestTermRingBufferTrimToAll(t *testing.T) {
	rb := newTermRingBuffer(10)

	for i := uint64(1); i <= 5; i++ {
		rb.append(i, i)
	}

	// Trim everything.
	rb.trimTo(100)
	if rb.len() != 0 {
		t.Errorf("len after trimTo(100) = %d, want 0", rb.len())
	}
}

func TestTermRingBufferTrimToEmpty(t *testing.T) {
	rb := newTermRingBuffer(10)

	// Trimming an empty buffer should not panic.
	rb.trimTo(100)
	if rb.len() != 0 {
		t.Errorf("len after trimTo on empty = %d, want 0", rb.len())
	}
}

func TestTermRingBufferTrimToBelowRange(t *testing.T) {
	rb := newTermRingBuffer(10)

	for i := uint64(10); i <= 15; i++ {
		rb.append(i, i)
	}

	// Trim to index below the range. Nothing should be removed.
	rb.trimTo(5)
	if rb.len() != 6 {
		t.Errorf("len after trimTo(5) = %d, want 6 (nothing removed)", rb.len())
	}
}

func TestTermRingBufferClear(t *testing.T) {
	rb := newTermRingBuffer(10)

	for i := uint64(1); i <= 7; i++ {
		rb.append(i, i)
	}

	rb.clear()
	if rb.len() != 0 {
		t.Errorf("len after clear = %d, want 0", rb.len())
	}

	// Lookup should fail.
	_, ok := rb.lookup(1)
	if ok {
		t.Error("lookup after clear returned true")
	}

	// Should be able to reuse after clear.
	rb.append(100, 200)
	term, ok := rb.lookup(100)
	if !ok || term != 200 {
		t.Errorf("lookup(100) after clear+append = (%d, %v), want (200, true)", term, ok)
	}
}

func TestTermRingBufferWraparound(t *testing.T) {
	rb := newTermRingBuffer(4)

	// Fill to capacity: [1, 2, 3, 4]
	for i := uint64(1); i <= 4; i++ {
		rb.append(i, i*10)
	}

	// Append 3 more, causing wraparound: head advances 3 times.
	// Final state: [5, 6, 7] with head wrapped.
	rb.append(5, 50)
	rb.append(6, 60)
	rb.append(7, 70)

	if rb.len() != 4 {
		t.Fatalf("len = %d, want 4", rb.len())
	}

	// Indices 1-3 should be evicted.
	for i := uint64(1); i <= 3; i++ {
		if _, ok := rb.lookup(i); ok {
			t.Errorf("lookup(%d) returned true after eviction", i)
		}
	}

	// Indices 4-7 should be present.
	for i := uint64(4); i <= 7; i++ {
		term, ok := rb.lookup(i)
		if !ok {
			t.Errorf("lookup(%d) returned false", i)
			continue
		}
		if term != i*10 {
			t.Errorf("lookup(%d) = %d, want %d", i, term, i*10)
		}
	}

	// Trim through the wraparound point.
	rb.trimTo(5)
	if rb.len() != 2 {
		t.Fatalf("len after trimTo(5) = %d, want 2", rb.len())
	}

	term, ok := rb.lookup(6)
	if !ok || term != 60 {
		t.Errorf("lookup(6) after trim = (%d, %v), want (60, true)", term, ok)
	}
	term, ok = rb.lookup(7)
	if !ok || term != 70 {
		t.Errorf("lookup(7) after trim = (%d, %v), want (70, true)", term, ok)
	}
}

func TestTermRingBufferBinarySearchEdgeCases(t *testing.T) {
	rb := newTermRingBuffer(100)

	// Single element.
	rb.append(42, 420)
	term, ok := rb.lookup(42)
	if !ok || term != 420 {
		t.Errorf("single element lookup(42) = (%d, %v), want (420, true)", term, ok)
	}
	_, ok = rb.lookup(41)
	if ok {
		t.Error("lookup(41) returned true on single-element buffer")
	}
	_, ok = rb.lookup(43)
	if ok {
		t.Error("lookup(43) returned true on single-element buffer")
	}

	// First and last element in a larger buffer.
	rb.clear()
	for i := uint64(10); i <= 50; i++ {
		rb.append(i, i*2)
	}
	term, ok = rb.lookup(10)
	if !ok || term != 20 {
		t.Errorf("lookup(10) first element = (%d, %v), want (20, true)", term, ok)
	}
	term, ok = rb.lookup(50)
	if !ok || term != 100 {
		t.Errorf("lookup(50) last element = (%d, %v), want (100, true)", term, ok)
	}
}

// --- IndexRange tests ---

func TestLogReaderIndexRangeEmptyReader(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	first, last := lr.IndexRange()
	if first != 0 || last != 0 {
		t.Errorf("IndexRange() = (%d, %d), want (0, 0) for empty reader", first, last)
	}
}

func TestLogReaderIndexRangeAfterSetRange(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Set range for entries [1..5].
	mustSetRange(t, lr, 1, 5)

	first, last := lr.IndexRange()
	if first != 1 || last != 5 {
		t.Errorf("IndexRange() = (%d, %d), want (1, 5)", first, last)
	}
}

func TestLogReaderIndexRangeAfterAppend(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	entries := makeProtoEntries(1, 10, 1)
	mustAppend(t, lr, entries)

	first, last := lr.IndexRange()
	if first != 1 || last != 10 {
		t.Errorf("IndexRange() = (%d, %d), want (1, 10)", first, last)
	}
}

func TestLogReaderIndexRangeAfterCompact(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Add entries [1..10].
	entries := makeProtoEntries(1, 10, 1)
	mustAppend(t, lr, entries)

	// Compact up to index 5.
	if err := lr.Compact(5); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	first, last := lr.IndexRange()
	if first != 6 || last != 10 {
		t.Errorf("IndexRange() = (%d, %d), want (6, 10) after compaction", first, last)
	}
}

func TestLogReaderIndexRangeAfterApplySnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Apply a snapshot at index 100.
	ss := proto.Snapshot{
		Index: 100,
		Term:  5,
		Membership: proto.Membership{
			ConfigChangeID: 1,
		},
	}
	if err := lr.ApplySnapshot(ss); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	// After snapshot, first available entry is 101 (markerIndex+1),
	// but length is 1 (marker only), so last is 100.
	first, last := lr.IndexRange()
	if first != 101 || last != 100 {
		t.Errorf("IndexRange() = (%d, %d), want (101, 100) after snapshot", first, last)
	}
}

func TestLogReaderIndexRangeWithEntriesAfterSnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Apply a snapshot at index 100.
	ss := proto.Snapshot{
		Index: 100,
		Term:  5,
	}
	if err := lr.ApplySnapshot(ss); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	// Add entries [101..105].
	entries := makeProtoEntries(101, 5, 5)
	mustAppend(t, lr, entries)

	first, last := lr.IndexRange()
	if first != 101 || last != 105 {
		t.Errorf("IndexRange() = (%d, %d), want (101, 105)", first, last)
	}
}

// --- Snapshot tests ---

func TestLogReaderSnapshotEmptyReader(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	ss := lr.Snapshot()
	if ss.Index != 0 || ss.Term != 0 {
		t.Errorf("Snapshot() = {Index: %d, Term: %d}, want zero-value", ss.Index, ss.Term)
	}
}

func TestLogReaderSnapshotAfterCreateSnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// First populate some state so CreateSnapshot accepts the snapshot.
	lr.SetState(proto.State{Commit: 10})
	lr.SetMembership(proto.Membership{ConfigChangeID: 1})

	// Add entries [1..5].
	mustSetRange(t, lr, 1, 5)

	// Create a snapshot at index 5.
	ss := proto.Snapshot{
		Index: 5,
		Term:  2,
		Membership: proto.Membership{
			ConfigChangeID: 2,
		},
	}
	if err := lr.CreateSnapshot(ss); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	got := lr.Snapshot()
	if got.Index != 5 {
		t.Errorf("Snapshot().Index = %d, want 5", got.Index)
	}
	if got.Term != 2 {
		t.Errorf("Snapshot().Term = %d, want 2", got.Term)
	}
	if got.Membership.ConfigChangeID != 2 {
		t.Errorf("Snapshot().Membership.ConfigChangeID = %d, want 2", got.Membership.ConfigChangeID)
	}
}

func TestLogReaderSnapshotAfterApplySnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	ss := proto.Snapshot{
		Index: 50,
		Term:  3,
		Membership: proto.Membership{
			ConfigChangeID: 42,
			Addresses:      map[uint64]string{1: "node1", 2: "node2"},
		},
	}
	if err := lr.ApplySnapshot(ss); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	got := lr.Snapshot()
	if got.Index != 50 {
		t.Errorf("Snapshot().Index = %d, want 50", got.Index)
	}
	if got.Term != 3 {
		t.Errorf("Snapshot().Term = %d, want 3", got.Term)
	}
	if got.Membership.ConfigChangeID != 42 {
		t.Errorf("Snapshot().Membership.ConfigChangeID = %d, want 42", got.Membership.ConfigChangeID)
	}
	if len(got.Membership.Addresses) != 2 {
		t.Errorf("len(Addresses) = %d, want 2", len(got.Membership.Addresses))
	}
}

func TestLogReaderSnapshotUpdatesAfterNewerSnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// First snapshot at index 10.
	ss1 := proto.Snapshot{Index: 10, Term: 1}
	if err := lr.ApplySnapshot(ss1); err != nil {
		t.Fatalf("ApplySnapshot(1): %v", err)
	}

	// Second (newer) snapshot at index 20.
	ss2 := proto.Snapshot{Index: 20, Term: 2}
	if err := lr.ApplySnapshot(ss2); err != nil {
		t.Fatalf("ApplySnapshot(2): %v", err)
	}

	got := lr.Snapshot()
	if got.Index != 20 {
		t.Errorf("Snapshot().Index = %d, want 20", got.Index)
	}
	if got.Term != 2 {
		t.Errorf("Snapshot().Term = %d, want 2", got.Term)
	}
}

func TestLogReaderSnapshotRejectsOlderSnapshot(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	// Set up state so that snapshot rejection logic is triggered.
	// The setSnapshot only rejects older snapshots if ConfigChangeID != 0 or Commit != 0.
	lr.SetState(proto.State{Commit: 10})
	lr.SetMembership(proto.Membership{ConfigChangeID: 1})

	// Apply initial snapshot at index 50 (with membership so state exists).
	ss1 := proto.Snapshot{
		Index: 50,
		Term:  5,
		Membership: proto.Membership{
			ConfigChangeID: 2,
		},
	}
	if err := lr.ApplySnapshot(ss1); err != nil {
		t.Fatalf("ApplySnapshot(1): %v", err)
	}

	// Try to apply an older snapshot at index 30.
	ss2 := proto.Snapshot{Index: 30, Term: 3}
	err := lr.ApplySnapshot(ss2)
	if !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Errorf("ApplySnapshot(older) = %v, want ErrSnapshotOutOfDate", err)
	}

	// Snapshot should still be the original.
	got := lr.Snapshot()
	if got.Index != 50 {
		t.Errorf("Snapshot().Index = %d, want 50 (unchanged)", got.Index)
	}
}

func TestLogReaderSnapshotIsSafeForConcurrentRead(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)

	ss := proto.Snapshot{Index: 100, Term: 10}
	if err := lr.ApplySnapshot(ss); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	// Concurrent reads should be safe (uses RLock internally).
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			got := lr.Snapshot()
			if got.Index != 100 {
				t.Errorf("concurrent Snapshot().Index = %d, want 100", got.Index)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestLogReaderTermCacheInvalidatedOnTruncatingAppend covers the follower
// conflict-truncation path: entries 1..10 at term 5 are replaced from index 6
// onward by entries 6..8 at term 6. Before the termBuf trimFrom fix, the ring
// still held the superseded (7,5)/(8,5)/(9,5)/(10,5) pairs and termLocked —
// which consults termBuf ahead of the range check — served them, corrupting
// matchTerm, the conflict hint, and (via Compact) markerTerm.
func TestLogReaderTermCacheInvalidatedOnTruncatingAppend(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	first := make([]proto.Entry, 0, 10)
	for i := uint64(1); i <= 10; i++ {
		first = append(first, proto.Entry{Index: i, Term: 5})
	}
	mustAppend(t, lr, first)

	second := []proto.Entry{
		{Index: 6, Term: 6},
		{Index: 7, Term: 6},
		{Index: 8, Term: 6},
	}
	mustAppend(t, lr, second)

	lo, hi := lr.IndexRange()
	if lo != 1 || hi != 8 {
		t.Fatalf("IndexRange() = (%d, %d), want (1, 8)", lo, hi)
	}

	for idx := uint64(1); idx <= 5; idx++ {
		term, err := lr.Term(idx)
		if err != nil {
			t.Fatalf("Term(%d): unexpected error %v", idx, err)
		}
		if term != 5 {
			t.Errorf("Term(%d) = %d, want 5", idx, term)
		}
	}
	for idx := uint64(6); idx <= 8; idx++ {
		term, err := lr.Term(idx)
		if err != nil {
			t.Fatalf("Term(%d): unexpected error %v", idx, err)
		}
		if term != 6 {
			t.Errorf("Term(%d) = %d, want 6 (truncated entry must not serve the stale term)", idx, term)
		}
	}
	for _, idx := range []uint64{9, 10} {
		term, err := lr.Term(idx)
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("Term(%d) = (%d, %v), want ErrUnavailable (entry no longer exists)", idx, term, err)
		}
	}
}

// TestLogReaderCompactAfterTruncatingAppendUsesNewTerm pins the downstream
// consequence of the stale term cache: Compact reads termLocked(index) to set
// markerTerm, which is later served as prevLogTerm and as the last-log term for
// isUpToDate. A stale term there is an Election Safety hazard.
func TestLogReaderCompactAfterTruncatingAppendUsesNewTerm(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	first := make([]proto.Entry, 0, 10)
	for i := uint64(1); i <= 10; i++ {
		first = append(first, proto.Entry{Index: i, Term: 5})
	}
	mustAppend(t, lr, first)
	mustAppend(t, lr, []proto.Entry{
		{Index: 6, Term: 6},
		{Index: 7, Term: 6},
		{Index: 8, Term: 6},
	})

	if err := lr.Compact(7); err != nil {
		t.Fatalf("Compact(7): %v", err)
	}
	term, err := lr.Term(7)
	if err != nil {
		t.Fatalf("Term(7) after Compact: %v", err)
	}
	if term != 6 {
		t.Errorf("markerTerm after Compact(7) = %d, want 6", term)
	}
	if lo, hi := lr.IndexRange(); lo != 8 || hi != 8 {
		t.Errorf("IndexRange() after Compact(7) = (%d, %d), want (8, 8)", lo, hi)
	}
}

// TestLogReaderTruncatingAppendPurgesEntryCache verifies the entry cache is
// truncated alongside the term cache so GetRange cannot serve a superseded
// entry body for a re-written index.
func TestLogReaderTruncatingAppendPurgesEntryCache(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	mustAppend(t, lr, []proto.Entry{
		{Index: 1, Term: 5, Cmd: []byte("old1")},
		{Index: 2, Term: 5, Cmd: []byte("old2")},
		{Index: 3, Term: 5, Cmd: []byte("old3")},
	})
	mustAppend(t, lr, []proto.Entry{
		{Index: 2, Term: 6, Cmd: []byte("new2")},
	})

	ents, err := lr.GetRange(1, 3, 0)
	if err != nil {
		t.Fatalf("GetRange(1, 3): %v", err)
	}
	if len(ents) != 2 {
		t.Fatalf("GetRange(1, 3) returned %d entries, want 2", len(ents))
	}
	if string(ents[1].Cmd) != "new2" || ents[1].Term != 6 {
		t.Errorf("entry 2 = %q term %d, want %q term 6", ents[1].Cmd, ents[1].Term, "new2")
	}
	if _, err := lr.GetRange(3, 4, 0); !errors.Is(err, ErrUnavailable) {
		t.Errorf("GetRange(3, 4) error = %v, want ErrUnavailable", err)
	}
	if _, ok := lr.entryCache[3]; ok {
		t.Error("entryCache still holds the truncated entry at index 3")
	}
}

// TestTermRingBufferTrimFrom exercises trimFrom directly across the
// below-range, mid-range, above-range and empty-buffer cases.
func TestTermRingBufferTrimFrom(t *testing.T) {
	r := newTermRingBuffer(16)
	for i := uint64(1); i <= 8; i++ {
		r.append(i, i*10)
	}

	r.trimFrom(20) // Above range: no-op.
	if r.len() != 8 {
		t.Fatalf("len after trimFrom(20) = %d, want 8", r.len())
	}

	r.trimFrom(5)
	if r.len() != 4 {
		t.Fatalf("len after trimFrom(5) = %d, want 4", r.len())
	}
	if _, ok := r.lookup(5); ok {
		t.Error("lookup(5) succeeded after trimFrom(5)")
	}
	if term, ok := r.lookup(4); !ok || term != 40 {
		t.Errorf("lookup(4) = (%d, %v), want (40, true)", term, ok)
	}

	r.trimFrom(1) // Below range: removes everything.
	if r.len() != 0 {
		t.Fatalf("len after trimFrom(1) = %d, want 0", r.len())
	}
	r.trimFrom(1) // Empty buffer: no-op, must not panic.
}

// TestTermRingBufferAppendRepairsNonMonotonicIndex asserts that appending an
// index at or below the tail repairs the ring (drops the superseded suffix)
// instead of corrupting the monotonic order that lookup binary-searches.
func TestTermRingBufferAppendRepairsNonMonotonicIndex(t *testing.T) {
	r := newTermRingBuffer(16)
	for i := uint64(1); i <= 10; i++ {
		r.append(i, 5)
	}

	r.append(6, 6)
	if r.len() != 6 {
		t.Fatalf("len after repairing append(6) = %d, want 6", r.len())
	}
	if term, ok := r.lookup(6); !ok || term != 6 {
		t.Errorf("lookup(6) = (%d, %v), want (6, true)", term, ok)
	}
	for _, idx := range []uint64{7, 8, 9, 10} {
		if term, ok := r.lookup(idx); ok {
			t.Errorf("lookup(%d) = (%d, true), want miss after repair", idx, term)
		}
	}

	// Re-appending the same index repeatedly must stay stable.
	r.append(6, 7)
	if r.len() != 6 {
		t.Fatalf("len after second append(6) = %d, want 6", r.len())
	}
	if term, ok := r.lookup(6); !ok || term != 7 {
		t.Errorf("lookup(6) = (%d, %v), want (7, true)", term, ok)
	}
}

// TestTermRingBufferAppendRepairAfterWraparound exercises the repairing
// append on a wrapped ring, where head is non-zero.
func TestTermRingBufferAppendRepairAfterWraparound(t *testing.T) {
	r := newTermRingBuffer(4)
	for i := uint64(1); i <= 6; i++ {
		r.append(i, 5) // Wraps; retains 3..6.
	}
	if r.head == 0 {
		t.Fatalf("expected a wrapped ring, head = 0")
	}

	r.append(5, 9)
	if r.len() != 3 {
		t.Fatalf("len after repairing append(5) = %d, want 3", r.len())
	}
	if term, ok := r.lookup(5); !ok || term != 9 {
		t.Errorf("lookup(5) = (%d, %v), want (9, true)", term, ok)
	}
	if _, ok := r.lookup(6); ok {
		t.Error("lookup(6) succeeded after repairing append(5)")
	}
	if term, ok := r.lookup(3); !ok || term != 5 {
		t.Errorf("lookup(3) = (%d, %v), want (5, true)", term, ok)
	}
}

// TestCreateSnapshot_RejectsRegressionAboveTheMarker pins the guard that the
// marker check cannot cover.
//
// Compaction deliberately retains CompactionOverhead entries BELOW the
// snapshot, so markerIndex normally trails the recorded snapshot index. That
// leaves a live band, markerIndex < ss.Index < snapshot.Index, in which a stale
// snapshot passes the marker check. Accepting it does not merely record the
// wrong index: setSnapshot also overwrites lr.membership from the snapshot, so
// the reader's quorum set rolls back to an older configuration — a removed
// replica reappears as a voter, and quorum is computed over a set the cluster
// has moved past.
//
// Deleting the guard left the whole pkg/logdb suite green.
func TestCreateSnapshot_RejectsRegressionAboveTheMarker(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// Recover into a state with a marker well below the recorded snapshot,
	// which is exactly what compaction with overhead produces.
	newer := proto.Snapshot{
		Index:      100,
		Term:       5,
		Membership: proto.Membership{ConfigChangeID: 9, Addresses: map[uint64]string{1: "a", 2: "b"}},
	}
	if err := lr.ApplySnapshot(newer); err != nil {
		t.Fatalf("ApplySnapshot(100): %v", err)
	}
	// Move the marker back below the snapshot, modelling the retained overhead.
	lr.mu.Lock()
	lr.markerIndex = 90
	lr.markerTerm = 5
	lr.mu.Unlock()

	stale := proto.Snapshot{
		Index:      95, // above the marker, below the recorded snapshot
		Term:       4,
		Membership: proto.Membership{ConfigChangeID: 3, Addresses: map[uint64]string{1: "a"}},
	}
	if err := lr.CreateSnapshot(stale); !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Fatalf("CreateSnapshot(95) over a recorded snapshot at 100 = %v, want "+
			"ErrSnapshotOutOfDate: it sits ABOVE the compaction marker (90), so the "+
			"marker check passes it, and accepting it rolls the recorded snapshot "+
			"back and overwrites the membership the reader computes quorum over", err)
	}
	if got := lr.Snapshot(); got.Index != 100 {
		t.Fatalf("recorded snapshot regressed to index %d, want 100", got.Index)
	}
	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 9 || len(membership.Addresses) != 2 {
		t.Fatalf("membership regressed to %+v: a stale snapshot overwrote the "+
			"quorum set with an older configuration", membership)
	}
}

// TestSetSnapshot_GuardsDoNotDependOnUnrelatedState pins that neither
// staleness guard is gated on a zero-value proxy for "this reader has been
// used".
//
// A proxy such as `membership.ConfigChangeID != 0 || state.Commit != 0` reads
// zero for a replica that recovered from a snapshot and has committed nothing
// of its own since — both values are legal there — and would switch BOTH guards
// off for exactly that replica, so the one state in which a stale snapshot is
// most likely to arrive would be the one state in which nothing rejected it.
func TestSetSnapshot_GuardsDoNotDependOnUnrelatedState(t *testing.T) {
	lr := NewLogReader(1, 1, nil)

	// A snapshot whose membership carries no config change, on a reader that
	// has committed nothing: both halves of such a proxy read zero.
	recorded := proto.Snapshot{
		Index:      50,
		Term:       3,
		Membership: proto.Membership{Addresses: map[uint64]string{1: "a", 2: "b"}},
	}
	if err := lr.ApplySnapshot(recorded); err != nil {
		t.Fatalf("ApplySnapshot(50): %v", err)
	}
	_, membership := lr.NodeState()
	if membership.ConfigChangeID != 0 {
		t.Fatalf("fixture: ConfigChangeID = %d, want 0 so a zero-value proxy reads zero",
			membership.ConfigChangeID)
	}
	if st, _ := lr.NodeState(); st.Commit != 0 {
		t.Fatalf("fixture: Commit = %d, want 0 so a zero-value proxy reads zero", st.Commit)
	}

	for _, ss := range []proto.Snapshot{
		{Index: 10, Term: 2, Membership: proto.Membership{Addresses: map[uint64]string{1: "a"}}},
		{Index: 50, Term: 3, Membership: proto.Membership{Addresses: map[uint64]string{1: "a"}}},
	} {
		if err := lr.ApplySnapshot(ss); !errors.Is(err, ErrSnapshotOutOfDate) {
			t.Fatalf("ApplySnapshot(%d) over a recorded snapshot at 50 = %v, want "+
				"ErrSnapshotOutOfDate: the reader's ConfigChangeID and Commit are "+
				"both zero, which is legal after a snapshot recovery, and must not "+
				"disable any staleness check on this path", ss.Index, err)
		}
		if err := lr.CreateSnapshot(ss); !errors.Is(err, ErrSnapshotOutOfDate) {
			t.Fatalf("CreateSnapshot(%d) over a recorded snapshot at 50 = %v, want "+
				"ErrSnapshotOutOfDate", ss.Index, err)
		}
	}
	if got := lr.Snapshot(); got.Index != 50 {
		t.Fatalf("recorded snapshot is now at index %d, want 50", got.Index)
	}
	_, membership = lr.NodeState()
	if len(membership.Addresses) != 2 {
		t.Fatalf("membership was overwritten by a stale snapshot: %+v", membership)
	}

	// The legitimate advance still works.
	if err := lr.ApplySnapshot(proto.Snapshot{Index: 60, Term: 4,
		Membership: proto.Membership{Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"}}}); err != nil {
		t.Fatalf("ApplySnapshot(60): %v", err)
	}
	if got := lr.Snapshot(); got.Index != 60 {
		t.Fatalf("recorded snapshot = %d after a legitimate advance, want 60", got.Index)
	}
}

// rangeRecordingLogDB records every (low, high) IterateEntries is asked for and
// serves nothing. It exists so a test can assert what the LogReader asked the
// backend, not merely what it returned: the two range guards below are reached
// indirectly whatever they are set to, so the returned error alone cannot tell
// a working guard from a broken one.
type rangeRecordingLogDB struct {
	LogDB
	calls [][2]uint64
}

func (r *rangeRecordingLogDB) IterateEntries(entries []Entry, size uint64,
	_ uint64, _ uint64, low uint64, high uint64, _ uint64) ([]Entry, uint64, error) {
	r.calls = append(r.calls, [2]uint64{low, high})
	return entries, size, ErrUnavailable
}

// TestLogReaderRangeBoundariesAreExact pins the two range comparisons in
// getRangeLockedInto that no test could fail on: widening either by one index
// left the pkg/logdb and pkg/internal/raft suites green.
//
// Neither can be pinned on the returned error, and that is the whole difficulty.
// Both guards are backstopped by something further down that happens to return
// the same error for the same input, so a widened guard still LOOKS right:
// an inverted range underflows `high-low`, is silently rewritten to a huge
// range, and then fails on a cache miss; an over-long range fails when the
// backend has nothing at the missing index. What actually changes is the range
// the reader ASKS FOR, so that is what is asserted here.
func TestLogReaderRangeBoundariesAreExact(t *testing.T) {
	const marker, count = uint64(10), uint64(5) // available: [11,15]

	newRecording := func(t *testing.T) (*LogReader, *rangeRecordingLogDB) {
		t.Helper()
		rec := &rangeRecordingLogDB{}
		lr := NewLogReader(1, 1, rec)
		if err := lr.ApplySnapshot(proto.Snapshot{Index: marker, Term: 1}); err != nil {
			t.Fatalf("ApplySnapshot: %v", err)
		}
		if err := lr.SetRange(marker+1, count); err != nil {
			t.Fatalf("SetRange: %v", err)
		}
		if first, last := lr.IndexRange(); first != marker+1 || last != marker+count {
			t.Fatalf("fixture: range [%d,%d], want [%d,%d]",
				first, last, marker+1, marker+count)
		}
		rec.calls = nil
		return lr, rec
	}

	t.Run("inverted_range_never_reaches_the_backend", func(t *testing.T) {
		lr, rec := newRecording(t)
		// low == high is an empty but well-formed range; low > high is inverted.
		// Without the guard `high - low` underflows, the cap rewrites high to
		// low+maxEntries, and the reader asks the backend for a range of
		// billions of entries starting in the middle of the log.
		if _, err := lr.GetRange(14, 13, 0); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("GetRange(14,13) = %v, want ErrUnavailable", err)
		}
		if len(rec.calls) != 0 {
			t.Fatalf("an INVERTED range was passed through to the backend as %v: "+
				"`high - low` underflowed and the cap rewrote high, so the reader "+
				"asked for a slab of the log rather than refusing a caller bug",
				rec.calls)
		}
	})

	t.Run("high_bound_is_last_plus_one", func(t *testing.T) {
		lr, rec := newRecording(t)
		// high is exclusive, so lastIndex+1 is the largest legal value.
		if _, err := lr.GetRange(11, 16, 0); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("GetRange(11,16) = %v; the fixture backend serves nothing, so "+
				"ErrUnavailable is expected — but it must come from the BACKEND", err)
		}
		if len(rec.calls) != 1 || rec.calls[0] != [2]uint64{11, 16} {
			t.Fatalf("the whole available range [11,16) was not passed to the "+
				"backend; calls = %v", rec.calls)
		}

		lr, rec = newRecording(t)
		if _, err := lr.GetRange(11, 17, 0); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("GetRange(11,17) (one past the end) = %v, want ErrUnavailable", err)
		}
		if len(rec.calls) != 0 {
			t.Fatalf("a range running PAST the last available index was passed to "+
				"the backend as %v: the reader asked for entries this replica does "+
				"not have, and whatever the backend returns for them would be served "+
				"to raft as log content", rec.calls)
		}
	})
}

// TestLogReaderCompactRefusesOutOfRangeIndexes pins Compact's outcome at both
// ends of its range.
//
// Its two explicit guards (`index < markerIndex` and `index > lastIndex()`) are
// REDUNDANT and cannot be pinned individually: widening either one leaves the
// behaviour unchanged, because the termLocked call immediately below repeats the
// same bounds through getRangeLockedInto and returns the same error for the same
// input. That is a genuine defence-in-depth, not a coverage gap, and this test
// therefore asserts the OUTCOME both paths must produce rather than which line
// produced it — including that a refused Compact leaves the range untouched,
// which matters because the update it guards (`lr.length -= index - markerIndex`)
// underflows on unsigned arithmetic.
func TestLogReaderCompactRefusesOutOfRangeIndexes(t *testing.T) {
	const marker, count = uint64(10), uint64(5) // available: [11,15]

	build := func(t *testing.T) *LogReader {
		t.Helper()
		lr := NewLogReader(1, 1, nil)
		if err := lr.ApplySnapshot(proto.Snapshot{Index: marker, Term: 1}); err != nil {
			t.Fatalf("ApplySnapshot: %v", err)
		}
		ents := make([]proto.Entry, 0, count)
		for i := uint64(1); i <= count; i++ {
			ents = append(ents, proto.Entry{Index: marker + i, Term: 1})
		}
		if err := lr.Append(ents); err != nil {
			t.Fatalf("Append: %v", err)
		}
		return lr
	}

	t.Run("below_the_marker", func(t *testing.T) {
		lr := build(t)
		if err := lr.Compact(marker); err != nil {
			t.Fatalf("Compact(%d) (at the marker) = %v, want nil", marker, err)
		}
		lr = build(t)
		if err := lr.Compact(marker - 1); !errors.Is(err, ErrCompacted) {
			t.Fatalf("Compact(%d) (below the marker) = %v, want ErrCompacted",
				marker-1, err)
		}
		if first, last := lr.IndexRange(); first != marker+1 || last != marker+count {
			t.Fatalf("a refused Compact changed the range to [%d,%d]: the marker "+
				"moved backwards and `length -= index - markerIndex` underflowed",
				first, last)
		}
	})

	t.Run("above_the_last_index", func(t *testing.T) {
		lr := build(t)
		last := marker + count
		if err := lr.Compact(last); err != nil {
			t.Fatalf("Compact(%d) (exactly the last index) = %v, want nil", last, err)
		}
		lr = build(t)
		if err := lr.Compact(last + 1); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Compact(%d) (one past the last index) = %v, want ErrUnavailable",
				last+1, err)
		}
		if first, l := lr.IndexRange(); first != marker+1 || l != last {
			t.Fatalf("a refused Compact changed the range to [%d,%d]", first, l)
		}
	})
}

// failingSaveStateLogDB refuses every SaveState and serves nothing, so a test
// can observe what the reader registered in memory when the durable write did
// not happen.
type failingSaveStateLogDB struct {
	LogDB
	err   error
	calls int
}

func (f *failingSaveStateLogDB) SaveState([]Update) error {
	f.calls++
	return f.err
}

func (f *failingSaveStateLogDB) IterateEntries(entries []Entry, size uint64,
	_ uint64, _ uint64, _ uint64, _ uint64, _ uint64) ([]Entry, uint64, error) {
	return entries, size, ErrUnavailable
}

// TestPersistBootstrap_RegistersNothingWhenTheDurableWriteFails pins the
// ordering PersistBootstrap's contract rests on: the durable write happens
// FIRST, and the in-memory registration only if it succeeded.
//
// Reversing the two — registering the entries and caching their terms before
// calling SaveState — left pkg/logdb, pkg/internal/raft and pkg/internal/engine
// all green, so the ordering was free to flip. It must not, and the reason is
// what the reader becomes if it does: a failed bootstrap write leaves the reader
// reporting a range the LogDB does not hold and answering Term() for an index
// that was never persisted, while GetRange for the same index goes to storage
// and fails. The two views of the log disagree, and the disagreement is exactly
// the window in which raft would use a term for an entry that does not exist.
//
// This is the general invariant in its one testable instance: the reader's
// caches may LAG storage, never lead it.
func TestPersistBootstrap_RegistersNothingWhenTheDurableWriteFails(t *testing.T) {
	injected := errors.New("reader_test: bootstrap write refused")
	db := &failingSaveStateLogDB{err: injected}
	lr := NewLogReader(1, 1, db)

	beforeFirst, beforeLast := lr.IndexRange()

	err := lr.PersistBootstrap(
		[]proto.Entry{{Index: 1, Term: 3, Type: 1, Cmd: []byte("initial-membership")}},
		proto.State{Term: 3, Vote: 1, Commit: 1},
	)
	if !errors.Is(err, injected) {
		t.Fatalf("PersistBootstrap = %v, want the injected write error", err)
	}
	if db.calls != 1 {
		t.Fatalf("SaveState was called %d times, want exactly 1", db.calls)
	}

	if first, last := lr.IndexRange(); first != beforeFirst || last != beforeLast {
		t.Fatalf("the reader registered a range [%d,%d] (was [%d,%d]) after the "+
			"durable write FAILED: it now reports entries the LogDB does not hold, "+
			"and the in-memory view of the log leads storage instead of lagging it",
			first, last, beforeFirst, beforeLast)
	}
	if term, err := lr.Term(1); err == nil {
		t.Fatalf("Term(1) returned %d after the bootstrap write failed; want an "+
			"error. The term cache was populated for an entry that was never "+
			"persisted, so Term() answers for an index GetRange cannot serve",
			term)
	}
	if st, _ := lr.NodeState(); st.Term != 0 || st.Commit != 0 {
		t.Fatalf("hard state advanced to %+v after the bootstrap write failed", st)
	}

	// The same call succeeds once the write does, so the guard is an ordering
	// requirement and not a refusal to bootstrap.
	db.err = nil
	if err := lr.PersistBootstrap(
		[]proto.Entry{{Index: 1, Term: 3, Type: 1, Cmd: []byte("initial-membership")}},
		proto.State{Term: 3, Vote: 1, Commit: 1},
	); err != nil {
		t.Fatalf("PersistBootstrap after the write succeeds: %v", err)
	}
	if first, last := lr.IndexRange(); first != 1 || last != 1 {
		t.Fatalf("range after a successful bootstrap = [%d,%d], want [1,1]", first, last)
	}
	if term, err := lr.Term(1); err != nil || term != 3 {
		t.Fatalf("Term(1) after a successful bootstrap = (%d, %v), want (3, nil)", term, err)
	}
}

// TestLogReaderGetRangeCopyIsPrivate pins the contract Host.QueryRaftLog
// relies on to read the log from an arbitrary goroutine: every returned Cmd is
// the caller's own copy, so neither a later GetRange (which reuses the step
// worker's buffers) nor a caller mutating its result can reach the other.
func TestLogReaderGetRangeCopyIsPrivate(t *testing.T) {
	db := newMemLogDB()
	const shardID, replicaID uint64 = 1, 1
	lr := NewLogReader(shardID, replicaID, db)
	db.addEntries(shardID, replicaID, makeEntries(1, 4, 1))
	mustSetRange(t, lr, 1, 4)

	copied, err := lr.GetRangeCopy(1, 5, 0)
	if err != nil {
		t.Fatalf("GetRangeCopy: %v", err)
	}
	if len(copied) != 4 {
		t.Fatalf("GetRangeCopy returned %d entries, want 4", len(copied))
	}
	// A step-worker read through the reused buffers must not disturb the copy.
	shared, err := lr.GetRange(1, 5, 0)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	for i := range shared {
		for j := range shared[i].Cmd {
			shared[i].Cmd[j] = 'X'
		}
	}
	for i, e := range copied {
		if string(e.Cmd) != "test-cmd" {
			t.Fatalf("copy[%d].Cmd = %q after the shared buffer was overwritten: the copy aliases it", i, e.Cmd)
		}
	}
	// And mutating the copy must not reach the reader's own state.
	copied[0].Cmd[0] = 'Z'
	again, err := lr.GetRangeCopy(1, 2, 0)
	if err != nil {
		t.Fatalf("GetRangeCopy: %v", err)
	}
	if again[0].Cmd[0] == 'Z' {
		t.Fatal("mutating one GetRangeCopy result changed the next one")
	}
}

// TestLogReaderGetRangeCopyPropagatesRangeErrors covers the error path: a
// range the reader does not hold returns the error and no entries.
func TestLogReaderGetRangeCopyPropagatesRangeErrors(t *testing.T) {
	lr := NewLogReader(1, 1, newMemLogDB())
	if got, err := lr.GetRangeCopy(10, 20, 0); err == nil || got != nil {
		t.Fatalf("GetRangeCopy over an absent range = (%v, %v), want (nil, error)", got, err)
	}
}

// TestLogReaderPersistBootstrapRegistersNothingOnWriteFailure pins the
// durable-first ordering: when the LogDB refuses the bootstrap write, the
// reader must not report a range the LogDB does not hold.
func TestLogReaderPersistBootstrapRegistersNothingOnWriteFailure(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	first, last := lr.IndexRange()
	err := lr.PersistBootstrap([]proto.Entry{{Index: 1, Term: 1}}, proto.State{Term: 1, Commit: 1})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("PersistBootstrap on a closed LogDB = %v, want ErrClosed", err)
	}
	if f, l := lr.IndexRange(); f != first || l != last {
		t.Fatalf("IndexRange moved from (%d, %d) to (%d, %d) after a failed durable write", first, last, f, l)
	}
	if st, _ := lr.NodeState(); st.Term != 0 || st.Commit != 0 {
		t.Fatalf("NodeState = %+v after a failed durable write, want the zero state", st)
	}
}

// TestLogReaderPersistBootstrapEmptyIsNoop covers the empty-input guard.
func TestLogReaderPersistBootstrapEmptyIsNoop(t *testing.T) {
	db := newMemLogDB()
	lr := NewLogReader(1, 1, db)
	if err := lr.PersistBootstrap(nil, proto.State{Term: 9}); err != nil {
		t.Fatalf("PersistBootstrap(nil) = %v, want nil", err)
	}
	if st, _ := lr.NodeState(); st.Term != 0 {
		t.Fatalf("NodeState.Term = %d after an empty bootstrap, want 0", st.Term)
	}
}

// TestLogReaderCompactToReportsWhetherTheMarkerMoved pins CompactTo's answer:
// moving the marker is true, compacting at the current marker is a nil no-op
// that reports false (Compact's own contract), and below it is ErrCompacted.
func TestLogReaderCompactToReportsWhetherTheMarkerMoved(t *testing.T) {
	lr := NewLogReader(1, 1, newMemLogDB())
	if err := lr.Append(makeProtoEntries(1, 10, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if moved, err := lr.CompactTo(5); err != nil || !moved {
		t.Fatalf("CompactTo(5) = (%v, %v), want (true, nil)", moved, err)
	}
	if moved, err := lr.CompactTo(5); err != nil || moved {
		t.Fatalf("CompactTo(5) at the marker = (%v, %v), want (false, nil)", moved, err)
	}
	if moved, err := lr.CompactTo(3); !errors.Is(err, ErrCompacted) || moved {
		t.Fatalf("CompactTo(3) below the marker = (%v, %v), want (false, ErrCompacted)", moved, err)
	}
}

// TestLogReaderRefreshSnapshotFileSize: a rebuilt artifact at the recorded
// index cannot go through CreateSnapshot, which refuses a non-advancing
// index, so the reader's declared size is refreshed in place — for exactly
// the recorded index and no other.
func TestLogReaderRefreshSnapshotFileSize(t *testing.T) {
	lr := NewLogReader(1, 1, newMemLogDB())
	if err := lr.CreateSnapshot(proto.Snapshot{Index: 40, Term: 2, Filepath: "/snap/40", FileSize: 0}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := lr.RefreshSnapshotFileSize(40, 4096); err != nil {
		t.Fatalf("RefreshSnapshotFileSize at the recorded index: %v", err)
	}
	if got := lr.Snapshot().FileSize; got != 4096 {
		t.Fatalf("FileSize = %d, want 4096", got)
	}
	for _, index := range []uint64{0, 39, 41} {
		if err := lr.RefreshSnapshotFileSize(index, 1); !errors.Is(err, ErrSnapshotOutOfDate) {
			t.Fatalf("RefreshSnapshotFileSize(%d) = %v, want ErrSnapshotOutOfDate", index, err)
		}
	}
	if got := lr.Snapshot().FileSize; got != 4096 {
		t.Fatalf("a refused refresh changed FileSize to %d", got)
	}
}
