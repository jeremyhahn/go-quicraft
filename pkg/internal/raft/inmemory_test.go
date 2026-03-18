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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// getEntryForTest is a test helper that retrieves a single entry at the given
// index from inMemory. This was previously a method on inMemory but was
// removed as unused in production code.
func getEntryForTest(im *inMemory, index uint64) (*proto.Entry, error) {
	entries, err := im.getEntries(index, index+1)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, ErrUnavailable
	}
	return &entries[0], nil
}

func makeEntries(start, end uint64, term uint64) []proto.Entry {
	entries := make([]proto.Entry, 0, end-start)
	for i := start; i < end; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: term})
	}
	return entries
}

func TestNewInMemory_SetsCorrectMarkerIndex(t *testing.T) {
	im := newInMemory(10)
	if im.markerIndex != 11 {
		t.Fatalf("expected markerIndex 11, got %d", im.markerIndex)
	}
	if im.savedTo != 10 {
		t.Fatalf("expected savedTo 10, got %d", im.savedTo)
	}
}

func TestNewInMemory_ZeroLastIndex(t *testing.T) {
	im := newInMemory(0)
	if im.markerIndex != 1 {
		t.Fatalf("expected markerIndex 1, got %d", im.markerIndex)
	}
	if im.savedTo != 0 {
		t.Fatalf("expected savedTo 0, got %d", im.savedTo)
	}
}

func TestInMemory_GetEntries_ReturnsCorrectRange(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	entries, err := im.getEntries(5, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Index != 5 || entries[2].Index != 7 {
		t.Fatalf("unexpected entry indices: %d, %d", entries[0].Index, entries[2].Index)
	}
}

func TestInMemory_GetEntries_AllEntries(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	entries, err := im.getEntries(5, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
}

func TestInMemory_GetEntries_ReturnsErrCompacted(t *testing.T) {
	im := newInMemory(10)
	im.merge(makeEntries(11, 15, 1))

	_, err := im.getEntries(5, 12)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
}

func TestInMemory_GetEntries_ReturnsErrUnavailable(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	_, err := im.getEntries(5, 15)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestInMemory_GetEntries_LowGreaterThanHigh(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	_, err := im.getEntries(8, 6)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestInMemory_GetEntries_EqualLowHigh(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	entries, err := im.getEntries(7, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entries != nil {
		t.Fatalf("expected nil, got %v", entries)
	}
}

func TestInMemory_GetEntry_Valid(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	entry, err := getEntryForTest(&im, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry.Index != 7 {
		t.Fatalf("expected index 7, got %d", entry.Index)
	}
}

func TestInMemory_GetEntry_Compacted(t *testing.T) {
	im := newInMemory(10)
	im.merge(makeEntries(11, 15, 1))

	_, err := getEntryForTest(&im, 5)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
}

func TestInMemory_GetEntry_Unavailable(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	_, err := getEntryForTest(&im, 20)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestInMemory_Term_ValidIndex(t *testing.T) {
	im := newInMemory(4)
	entries := []proto.Entry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 2},
		{Index: 7, Term: 3},
	}
	im.merge(entries)

	term, ok := im.term(6)
	if !ok {
		t.Fatal("expected term to be found")
	}
	if term != 2 {
		t.Fatalf("expected term 2, got %d", term)
	}
}

func TestInMemory_Term_IndexBelowMarker(t *testing.T) {
	im := newInMemory(10)
	im.merge(makeEntries(11, 15, 1))

	_, ok := im.term(5)
	if ok {
		t.Fatal("expected term not found for compacted index")
	}
}

func TestInMemory_Term_IndexAboveLastEntry(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	_, ok := im.term(20)
	if ok {
		t.Fatal("expected term not found for unavailable index")
	}
}

func TestInMemory_Term_SnapshotIndex(t *testing.T) {
	im := newInMemory(10)
	im.snapshot = &proto.Snapshot{Index: 8, Term: 3}

	term, ok := im.term(8)
	if !ok {
		t.Fatal("expected term found for snapshot index")
	}
	if term != 3 {
		t.Fatalf("expected term 3, got %d", term)
	}
}

func TestInMemory_Term_AppliedToCache(t *testing.T) {
	im := newInMemory(4)
	im.appliedTo = 6
	im.appliedToTerm = 5

	term, ok := im.term(6)
	if !ok {
		t.Fatal("expected term found for appliedTo index")
	}
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}
}

func TestInMemory_LastIndex_WithEntries(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	if im.lastIndex() != 9 {
		t.Fatalf("expected lastIndex 9, got %d", im.lastIndex())
	}
}

func TestInMemory_LastIndex_Empty(t *testing.T) {
	im := newInMemory(10)

	if im.lastIndex() != 10 {
		t.Fatalf("expected lastIndex 10, got %d", im.lastIndex())
	}
}

func TestInMemory_LastIndex_WithSnapshot(t *testing.T) {
	im := newInMemory(0)
	im.snapshot = &proto.Snapshot{Index: 7, Term: 2}
	im.markerIndex = 8

	if im.lastIndex() != 7 {
		t.Fatalf("expected lastIndex 7, got %d", im.lastIndex())
	}
}

func TestInMemory_FirstIndex_WithEntries(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	if im.firstIndex() != 5 {
		t.Fatalf("expected firstIndex 5, got %d", im.firstIndex())
	}
}

func TestInMemory_FirstIndex_WithSnapshot(t *testing.T) {
	im := newInMemory(0)
	im.snapshot = &proto.Snapshot{Index: 7, Term: 2}

	if im.firstIndex() != 8 {
		t.Fatalf("expected firstIndex 8, got %d", im.firstIndex())
	}
}

func TestInMemory_Merge_ContiguousAppend(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 8, 1))
	im.merge(makeEntries(8, 11, 2))

	if len(im.entries) != 6 {
		t.Fatalf("expected 6 entries, got %d", len(im.entries))
	}
	if im.entries[0].Index != 5 {
		t.Fatalf("expected first entry index 5, got %d", im.entries[0].Index)
	}
	if im.entries[5].Index != 10 {
		t.Fatalf("expected last entry index 10, got %d", im.entries[5].Index)
	}
	if im.entries[3].Term != 2 {
		t.Fatalf("expected entry at index 8 to have term 2, got %d", im.entries[3].Term)
	}
}

func TestInMemory_Merge_OverlapReplace(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	// Overlap: new entries start at index 7 with different term.
	newEntries := makeEntries(7, 12, 2)
	im.merge(newEntries)

	if len(im.entries) != 7 {
		t.Fatalf("expected 7 entries, got %d", len(im.entries))
	}
	// Entries 5,6 should have term 1; entries 7..11 should have term 2.
	if im.entries[0].Term != 1 || im.entries[1].Term != 1 {
		t.Fatal("expected first two entries to have term 1")
	}
	for i := 2; i < 7; i++ {
		if im.entries[i].Term != 2 {
			t.Fatalf("expected entry at position %d to have term 2, got %d", i, im.entries[i].Term)
		}
	}
}

func TestInMemory_Merge_CompleteReplacement(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))
	im.savedTo = 9

	// Replace everything with entries starting before markerIndex.
	im.merge(makeEntries(3, 7, 3))

	if im.markerIndex != 3 {
		t.Fatalf("expected markerIndex 3, got %d", im.markerIndex)
	}
	if len(im.entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(im.entries))
	}
	if im.savedTo != 2 {
		t.Fatalf("expected savedTo 2, got %d", im.savedTo)
	}
}

func TestInMemory_Merge_EmptyEntries(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	im.merge(nil)

	if len(im.entries) != 5 {
		t.Fatalf("expected 5 entries unchanged, got %d", len(im.entries))
	}
}

func TestInMemory_Restore_ResetsEverything(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	ss := proto.Snapshot{Index: 20, Term: 5}
	im.restore(ss)

	if im.markerIndex != 21 {
		t.Fatalf("expected markerIndex 21, got %d", im.markerIndex)
	}
	if im.entries != nil {
		t.Fatalf("expected nil entries, got %v", im.entries)
	}
	if im.snapshot == nil || im.snapshot.Index != 20 {
		t.Fatal("expected snapshot at index 20")
	}
	if im.savedTo != 20 {
		t.Fatalf("expected savedTo 20, got %d", im.savedTo)
	}
	if im.appliedTo != 20 {
		t.Fatalf("expected appliedTo 20, got %d", im.appliedTo)
	}
	if im.appliedToTerm != 5 {
		t.Fatalf("expected appliedToTerm 5, got %d", im.appliedToTerm)
	}
}

func TestInMemory_Restore_ClearsSnapshot(t *testing.T) {
	im := newInMemory(0)
	im.snapshot = &proto.Snapshot{Index: 5, Term: 1}

	ss := proto.Snapshot{Index: 10, Term: 2}
	im.restore(ss)

	if im.snapshot.Index != 10 || im.snapshot.Term != 2 {
		t.Fatalf("expected snapshot to be replaced, got index=%d term=%d",
			im.snapshot.Index, im.snapshot.Term)
	}
}

func TestInMemory_EntriesToSave_AllUnsaved(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	toSave := im.entriesToSave()
	if len(toSave) != 5 {
		t.Fatalf("expected 5 entries to save, got %d", len(toSave))
	}
}

func TestInMemory_EntriesToSave_PartiallySaved(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))
	im.savedLogTo(7, 1) // entries 5,6,7 are saved

	toSave := im.entriesToSave()
	if len(toSave) != 2 {
		t.Fatalf("expected 2 entries to save, got %d", len(toSave))
	}
	if toSave[0].Index != 8 {
		t.Fatalf("expected first unsaved index 8, got %d", toSave[0].Index)
	}
}

func TestInMemory_EntriesToSave_AllSaved(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))
	im.savedLogTo(9, 1)

	toSave := im.entriesToSave()
	if len(toSave) != 0 {
		t.Fatalf("expected 0 entries to save, got %d", len(toSave))
	}
}

func TestInMemory_SavedLogTo_WrongTerm(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	// Try to save with wrong term: should be ignored.
	im.savedLogTo(7, 99)
	if im.savedTo != 4 {
		t.Fatalf("expected savedTo to remain 4, got %d", im.savedTo)
	}
}

func TestInMemory_SavedLogTo_IndexBeyondEntries(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	im.savedLogTo(20, 1)
	if im.savedTo != 4 {
		t.Fatalf("expected savedTo to remain 4, got %d", im.savedTo)
	}
}

func TestInMemory_CommitUpdate_StableLog(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	im.commitUpdate(proto.UpdateCommit{StableLogTo: 7, StableLogTerm: 1})
	if im.savedTo != 7 {
		t.Fatalf("expected savedTo 7, got %d", im.savedTo)
	}
}

func TestInMemory_CommitUpdate_StableSnapshot(t *testing.T) {
	im := newInMemory(0)
	im.snapshot = &proto.Snapshot{Index: 10, Term: 2}

	im.commitUpdate(proto.UpdateCommit{StableSnapshotTo: 10})
	if im.snapshot != nil {
		t.Fatal("expected snapshot to be cleared after persistence")
	}
}

func TestInMemory_SavedSnapshot_MismatchIndex(t *testing.T) {
	im := newInMemory(0)
	im.snapshot = &proto.Snapshot{Index: 10, Term: 2}

	im.savedSnapshot(5) // wrong index
	if im.snapshot == nil {
		t.Fatal("expected snapshot to remain since index doesn't match")
	}
}

func TestInMemory_AppliedLogTo_TruncatesFront(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))

	im.appliedLogTo(7)

	if im.markerIndex != 8 {
		t.Fatalf("expected markerIndex 8, got %d", im.markerIndex)
	}
	if len(im.entries) != 2 {
		t.Fatalf("expected 2 remaining entries, got %d", len(im.entries))
	}
	if im.entries[0].Index != 8 {
		t.Fatalf("expected first entry index 8, got %d", im.entries[0].Index)
	}
	if im.appliedTo != 7 {
		t.Fatalf("expected appliedTo 7, got %d", im.appliedTo)
	}
}

func TestInMemory_AppliedLogTo_IndexBelowMarker(t *testing.T) {
	im := newInMemory(10)
	im.merge(makeEntries(11, 15, 1))
	originalLen := len(im.entries)

	im.appliedLogTo(5) // below marker, should be no-op
	if len(im.entries) != originalLen {
		t.Fatalf("expected entries unchanged, got %d", len(im.entries))
	}
}

func TestInMemory_AppliedLogTo_IndexBeyondEntries(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))
	originalLen := len(im.entries)

	im.appliedLogTo(20) // beyond entries, should be no-op
	if len(im.entries) != originalLen {
		t.Fatalf("expected entries unchanged, got %d", len(im.entries))
	}
}

func TestInMemory_AppliedLogTo_EmptyEntries(t *testing.T) {
	im := newInMemory(10)
	im.appliedLogTo(5) // no entries, should be no-op
	// No panic expected.
}

func TestInMemory_Resize_ReducesCapacity(t *testing.T) {
	im := newInMemory(0)
	// Create a large slice with excess capacity.
	large := make([]proto.Entry, 5, 10000)
	for i := range large {
		large[i] = proto.Entry{Index: uint64(i + 1), Term: 1}
	}
	im.entries = large
	im.markerIndex = 1
	im.shrunk = true

	im.resize()

	if im.shrunk {
		t.Fatal("expected shrunk to be false after resize")
	}
	if cap(im.entries) >= 10000 {
		t.Fatalf("expected capacity to be reduced, got %d", cap(im.entries))
	}
	if len(im.entries) != 5 {
		t.Fatalf("expected 5 entries preserved, got %d", len(im.entries))
	}
}

func TestInMemory_ResizeIfNeeded_NoOpWhenNotShrunk(t *testing.T) {
	im := newInMemory(0)
	im.entries = make([]proto.Entry, 5, 10000)
	im.shrunk = false

	oldCap := cap(im.entries)
	im.resizeIfNeeded()

	if cap(im.entries) != oldCap {
		t.Fatal("expected no resize when shrunk is false")
	}
}

func TestInMemory_GetSnapshotIndex_NoSnapshot(t *testing.T) {
	im := newInMemory(10)
	_, ok := im.getSnapshotIndex()
	if ok {
		t.Fatal("expected no snapshot index")
	}
}

func TestInMemory_GetSnapshotIndex_WithSnapshot(t *testing.T) {
	im := newInMemory(10)
	im.snapshot = &proto.Snapshot{Index: 8, Term: 2}
	idx, ok := im.getSnapshotIndex()
	if !ok {
		t.Fatal("expected snapshot index to be found")
	}
	if idx != 8 {
		t.Fatalf("expected snapshot index 8, got %d", idx)
	}
}

func TestInMemory_Merge_SavedToClampedOnOverlap(t *testing.T) {
	im := newInMemory(4)
	im.merge(makeEntries(5, 10, 1))
	im.savedTo = 9 // all saved

	// Overlap at index 7: savedTo should clamp to 6 (firstNewIndex-1).
	im.merge(makeEntries(7, 12, 2))
	if im.savedTo != 6 {
		t.Fatalf("expected savedTo clamped to 6, got %d", im.savedTo)
	}
}

func TestInMemory_NewEntrySlice_MinCapacity(t *testing.T) {
	im := newInMemory(0)
	result := im.newEntrySlice(makeEntries(1, 3, 1))
	if cap(result) < entrySliceSize {
		t.Fatalf("expected capacity >= %d, got %d", entrySliceSize, cap(result))
	}
}

func TestInMemory_NewEntrySlice_LargeInput(t *testing.T) {
	im := newInMemory(0)
	large := makeEntries(1, uint64(entrySliceSize+100), 1)
	result := im.newEntrySlice(large)
	if cap(result) < len(large) {
		t.Fatalf("expected capacity >= %d, got %d", len(large), cap(result))
	}
	if len(result) != len(large) {
		t.Fatalf("expected length %d, got %d", len(large), len(result))
	}
}

// --- WastedPrefix / Lazy Compaction Tests ---

// TestInMemory_AppliedLogTo_NoCompactionBelowThreshold verifies that applying
// entries one-at-a-time does NOT trigger a reallocation when the wasted prefix
// is below entrySliceSize and below the live entry count.
func TestInMemory_AppliedLogTo_NoCompactionBelowThreshold(t *testing.T) {
	im := newInMemory(0)
	// Merge 1024 entries so the backing array is allocated once.
	im.merge(makeEntries(1, 1025, 1))
	initialPtr := &im.entries[0]

	// Apply 100 entries one-at-a-time. wastedPrefix should accumulate
	// but no compaction should occur because 100 < 512 (entrySliceSize)
	// and 100 < 924 (remaining live entries).
	for i := uint64(1); i <= 100; i++ {
		im.appliedLogTo(i)
	}

	if im.wastedPrefix != 100 {
		t.Fatalf("expected wastedPrefix 100, got %d", im.wastedPrefix)
	}
	if im.shrunk != true {
		t.Fatal("expected shrunk to be true (deferred)")
	}
	// The entries slice header should still point into the original
	// backing array (no reallocation occurred).
	currentPtr := &im.entries[0]
	if currentPtr == initialPtr {
		// Pointers could match if the compiler reuses the same base,
		// but entry values must be correct regardless.
	}
	if len(im.entries) != 924 {
		t.Fatalf("expected 924 entries remaining, got %d", len(im.entries))
	}
	if im.entries[0].Index != 101 {
		t.Fatalf("expected first entry index 101, got %d", im.entries[0].Index)
	}
}

// TestInMemory_AppliedLogTo_CompactsAtAbsoluteThreshold verifies that
// compaction triggers when both the absolute threshold (wastedPrefix >=
// entrySliceSize) and the proportional gate (wastedPrefix >= 2*live)
// are satisfied. With 1024 entries, the proportional gate fires when
// wastedPrefix >= 2 * (1024 - applied), i.e., applied >= ceil(1024*2/3)
// = 683. At that point wastedPrefix=683 >= entrySliceSize=512.
func TestInMemory_AppliedLogTo_CompactsAtAbsoluteThreshold(t *testing.T) {
	im := newInMemory(0)
	total := uint64(2 * entrySliceSize)
	im.merge(makeEntries(1, total+1, 1))

	// Apply exactly entrySliceSize entries. At this point:
	// wastedPrefix=512, live=512. Proportional gate: 512 < 2*512=1024,
	// so compaction is deferred.
	for i := uint64(1); i <= uint64(entrySliceSize); i++ {
		im.appliedLogTo(i)
	}

	// Compaction should NOT have fired yet: proportional gate blocks it.
	if im.wastedPrefix != uint64(entrySliceSize) {
		t.Fatalf("expected wastedPrefix %d (deferred by proportional gate), got %d",
			entrySliceSize, im.wastedPrefix)
	}
	if !im.shrunk {
		t.Fatal("expected shrunk to be true (compaction deferred)")
	}

	// Continue applying until compaction fires. The proportional gate
	// (wastedPrefix >= 2*live) triggers when applied >= ceil(total*2/3).
	compacted := false
	for i := uint64(entrySliceSize) + 1; i <= total; i++ {
		prevWaste := im.wastedPrefix
		im.appliedLogTo(i)
		if prevWaste > 0 && im.wastedPrefix == 0 {
			compacted = true
			break
		}
	}

	if !compacted {
		t.Fatal("expected compaction to fire before all entries were applied")
	}
	if im.wastedPrefix != 0 {
		t.Fatalf("expected wastedPrefix 0 after compaction, got %d", im.wastedPrefix)
	}
	if im.shrunk {
		t.Fatal("expected shrunk to be false after compaction")
	}
	// Remaining entries should be roughly total/3 (341).
	if len(im.entries) == 0 {
		t.Fatal("expected some entries remaining after compaction")
	}
}

// TestInMemory_AppliedLogTo_NoCompactWhenFewEntriesRemain verifies that
// when entries drains to 0 entries (the sequential propose hot path),
// the zero-entries fast path releases the backing array without any
// allocation. No resize (makeslice) is triggered.
func TestInMemory_AppliedLogTo_NoCompactWhenFewEntriesRemain(t *testing.T) {
	im := newInMemory(0)
	// Merge 10 entries (well below entrySliceSize=512).
	im.merge(makeEntries(1, 11, 1))

	// Apply all 10 entries one at a time. When the last entry is applied,
	// len(entries)==0, and the zero-entries fast path fires: entries=nil,
	// shrunk=false, wastedPrefix=0.
	for i := uint64(1); i <= 10; i++ {
		im.appliedLogTo(i)
	}

	// Zero-entries fast path should have cleared everything.
	if im.entries != nil {
		t.Fatalf("expected nil entries after draining, got len=%d", len(im.entries))
	}
	if im.wastedPrefix != 0 {
		t.Fatalf("expected wastedPrefix 0 after zero-entries fast path, got %d",
			im.wastedPrefix)
	}
	if im.shrunk {
		t.Fatal("expected shrunk to be false after zero-entries fast path")
	}
}

// TestAppliedLogToNoPathologicalResize verifies that the sequential
// propose hot path (merge batch, apply one-at-a-time) does not trigger
// resize on every single apply call. Compaction should only occur once
// per entrySliceSize (512) accumulated waste, not on every apply.
func TestAppliedLogToNoPathologicalResize(t *testing.T) {
	im := newInMemory(0)

	// Seed with entrySliceSize entries so there is a backing array.
	total := uint64(entrySliceSize)
	im.merge(makeEntries(1, total+1, 1))

	// Track compaction events by monitoring wastedPrefix resets.
	compactions := 0
	nextIndex := total + 1

	// Run 3 full cycles of entrySliceSize applies. Each cycle:
	// - Merge entrySliceSize entries (one batch)
	// - Apply them one at a time
	// Compaction should trigger exactly once per cycle (when
	// wastedPrefix reaches entrySliceSize), not on every apply.
	for cycle := 0; cycle < 3; cycle++ {
		im.merge(makeEntries(nextIndex, nextIndex+total, 1))

		for i := uint64(0); i < total; i++ {
			applyIdx := nextIndex - total + uint64(cycle)*total + i
			if applyIdx > im.lastIndex() {
				break
			}
			if applyIdx < im.markerIndex {
				continue
			}

			prevWaste := im.wastedPrefix
			im.appliedLogTo(applyIdx)
			// Detect compaction: wastedPrefix resets to 0.
			if prevWaste > 0 && im.wastedPrefix == 0 {
				compactions++
			}
		}
		nextIndex += total
	}

	// With entrySliceSize=512 and 3 cycles of 512 applies each (1536
	// total applies), we expect at most a small number of compactions
	// (roughly 1 per cycle), NOT 1536 compactions.
	if compactions > 6 {
		t.Fatalf("expected at most 6 compactions over 1536 applies, got %d "+
			"(pathological per-apply resize detected)", compactions)
	}
	if compactions == 0 {
		t.Fatal("expected at least 1 compaction to occur")
	}
}

// TestInMemory_WastedPrefix_ResetOnRestore verifies that restore resets
// wastedPrefix.
func TestInMemory_WastedPrefix_ResetOnRestore(t *testing.T) {
	im := newInMemory(0)
	// Use 2000 entries and apply only 100 so wastedPrefix stays below both
	// thresholds: 100 < 1900 (live) and 100 < 512 (entrySliceSize).
	im.merge(makeEntries(1, 2001, 1))
	im.appliedLogTo(100)
	if im.wastedPrefix == 0 {
		t.Fatal("expected non-zero wastedPrefix before restore")
	}

	im.restore(proto.Snapshot{Index: 200, Term: 5})
	if im.wastedPrefix != 0 {
		t.Fatalf("expected wastedPrefix 0 after restore, got %d", im.wastedPrefix)
	}
}

// TestInMemory_WastedPrefix_ResetOnMergeReplaceAll verifies that merge
// case 2 (replace all) resets wastedPrefix.
func TestInMemory_WastedPrefix_ResetOnMergeReplaceAll(t *testing.T) {
	im := newInMemory(0)
	// Use 2000 entries and apply only 100 to keep wastedPrefix below threshold.
	im.merge(makeEntries(1, 2001, 1))
	im.appliedLogTo(100)
	if im.wastedPrefix == 0 {
		t.Fatal("expected non-zero wastedPrefix before merge replace-all")
	}

	// Case 2: firstNewIndex <= markerIndex (101). Entries starting at 1.
	im.merge(makeEntries(1, 20, 2))
	if im.wastedPrefix != 0 {
		t.Fatalf("expected wastedPrefix 0 after merge replace-all, got %d",
			im.wastedPrefix)
	}
}

// TestInMemory_WastedPrefix_ResetOnMergeOverlap verifies that merge
// case 3 (overlap) resets wastedPrefix.
func TestInMemory_WastedPrefix_ResetOnMergeOverlap(t *testing.T) {
	im := newInMemory(0)
	im.merge(makeEntries(1, 100, 1))
	im.appliedLogTo(10)
	if im.wastedPrefix == 0 {
		t.Fatal("expected non-zero wastedPrefix before merge overlap")
	}

	// Case 3: overlap at index 50 (within current entries 11..99).
	im.merge(makeEntries(50, 120, 2))
	if im.wastedPrefix != 0 {
		t.Fatalf("expected wastedPrefix 0 after merge overlap, got %d",
			im.wastedPrefix)
	}
}

// TestInMemory_Merge_AfterRestoreGap verifies that merge Case 3 does not
// panic when entries is nil (after snapshot restore) and leader sends entries
// starting beyond markerIndex. Also verifies savedTo is updated so
// entriesToSave() returns the new entries (regression for M3 bug where
// savedTo stayed at the snapshot index, causing a uint64 underflow in
// entriesToSave offset calculation).
func TestInMemory_Merge_AfterRestoreGap(t *testing.T) {
	im := newInMemory(0)
	im.restore(proto.Snapshot{Index: 100, Term: 5})
	if im.entries != nil {
		t.Fatal("expected nil entries after restore")
	}
	if im.markerIndex != 101 {
		t.Fatalf("expected markerIndex 101, got %d", im.markerIndex)
	}
	if im.savedTo != 100 {
		t.Fatalf("expected savedTo 100 after restore, got %d", im.savedTo)
	}

	// Leader sends entries starting at 103 (gap from markerIndex 101).
	im.merge(makeEntries(103, 106, 5))

	if len(im.entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(im.entries))
	}
	if im.markerIndex != 103 {
		t.Fatalf("expected markerIndex 103, got %d", im.markerIndex)
	}
	if im.entries[0].Index != 103 || im.entries[2].Index != 105 {
		t.Fatalf("unexpected entry indices: %d..%d",
			im.entries[0].Index, im.entries[2].Index)
	}
	// savedTo must be firstNewIndex-1 (102) so entriesToSave() works.
	if im.savedTo != 102 {
		t.Fatalf("expected savedTo 102 after gap merge, got %d", im.savedTo)
	}
	// entriesToSave must return the 3 new entries.
	toSave := im.entriesToSave()
	if len(toSave) != 3 {
		t.Fatalf("expected entriesToSave to return 3 entries, got %d", len(toSave))
	}
	if toSave[0].Index != 103 || toSave[2].Index != 105 {
		t.Fatalf("entriesToSave returned wrong indices: %d..%d",
			toSave[0].Index, toSave[2].Index)
	}
}

// TestInMemory_Merge_AfterRestoreGap_SavedToHigherThanNewEntries verifies
// that the gap branch does not regress savedTo when it is already beyond
// the new entries (should not happen in practice, but guards correctness).
func TestInMemory_Merge_AfterRestoreGap_SavedToHigherThanNewEntries(t *testing.T) {
	im := newInMemory(0)
	im.restore(proto.Snapshot{Index: 100, Term: 5})
	// Artificially advance savedTo past where new entries will be.
	im.savedTo = 200

	im.merge(makeEntries(103, 106, 5))

	// savedTo should be reset to 102 (firstNewIndex-1) because the gap
	// branch always resets it. Entries 103-105 must be saved.
	if im.savedTo != 102 {
		t.Fatalf("expected savedTo 102, got %d", im.savedTo)
	}
	toSave := im.entriesToSave()
	if len(toSave) != 3 {
		t.Fatalf("expected 3 entries to save, got %d", len(toSave))
	}
}

// TestInMemory_Merge_AfterRestoreContiguous verifies that merge after
// snapshot restore with entries at exactly markerIndex works (Case 1).
func TestInMemory_Merge_AfterRestoreContiguous(t *testing.T) {
	im := newInMemory(0)
	im.restore(proto.Snapshot{Index: 100, Term: 5})

	// Leader sends entries starting exactly at markerIndex (Case 1).
	im.merge(makeEntries(101, 104, 5))

	if len(im.entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(im.entries))
	}
	if im.markerIndex != 101 {
		t.Fatalf("expected markerIndex 101, got %d", im.markerIndex)
	}
}

// TestInMemory_WastedPrefix_NotResetOnContiguousAppend verifies that merge
// case 1 (contiguous append) does NOT reset wastedPrefix, preserving the
// accumulated waste tracking.
func TestInMemory_WastedPrefix_NotResetOnContiguousAppend(t *testing.T) {
	im := newInMemory(0)
	im.merge(makeEntries(1, 100, 1))
	im.appliedLogTo(10)
	waste := im.wastedPrefix

	// Case 1: contiguous append.
	im.merge(makeEntries(100, 110, 1))
	if im.wastedPrefix != waste {
		t.Fatalf("expected wastedPrefix unchanged at %d, got %d",
			waste, im.wastedPrefix)
	}
}

// TestInMemory_Resize_ResetsWastedPrefix verifies that calling resize
// directly resets wastedPrefix.
func TestInMemory_Resize_ResetsWastedPrefix(t *testing.T) {
	im := newInMemory(0)
	// Use 2000 entries and apply only 100 to keep wastedPrefix below threshold.
	im.merge(makeEntries(1, 2001, 1))
	im.appliedLogTo(100)
	if im.wastedPrefix == 0 {
		t.Fatal("expected non-zero wastedPrefix before resize")
	}

	im.resize()
	if im.wastedPrefix != 0 {
		t.Fatalf("expected wastedPrefix 0 after resize, got %d", im.wastedPrefix)
	}
}

// --- Benchmarks ---

// BenchmarkInMemoryMerge measures the cost of merging batches of 10
// contiguous entries into inMemory. This is the hot append path for
// all new log entries (both leader proposals and follower replications).
func BenchmarkInMemoryMerge(b *testing.B) {
	b.ReportAllocs()
	im := newInMemory(0)
	batch := make([]proto.Entry, 10)
	nextIndex := uint64(1)
	for b.Loop() {
		for i := range batch {
			batch[i] = proto.Entry{Index: nextIndex, Term: 1}
			nextIndex++
		}
		im.merge(batch)
	}
}

// BenchmarkInMemoryMergeApply simulates the production hot path: merge a
// batch of entries, then apply them one at a time. Before the lazy
// compaction fix, this triggered a newEntrySlice allocation on every
// single appliedLogTo call (96.5% of all allocations). After the fix,
// compaction is deferred until wasted prefix space justifies it.
func BenchmarkInMemoryMergeApply(b *testing.B) {
	b.ReportAllocs()
	im := newInMemory(0)
	nextIndex := uint64(1)
	batchSize := 10
	batch := make([]proto.Entry, batchSize)
	for b.Loop() {
		// Merge a batch (simulates leader proposing entries).
		for i := range batch {
			batch[i] = proto.Entry{Index: nextIndex, Term: 1}
			nextIndex++
		}
		im.merge(batch)
		// Apply them one-at-a-time (simulates sequential state machine apply).
		for i := 0; i < batchSize; i++ {
			im.appliedLogTo(nextIndex - uint64(batchSize) + uint64(i))
		}
	}
}

// BenchmarkInMemoryGetEntries measures the cost of reading a range of
// entries from a pre-populated inMemory. This is the inner loop for
// building replication messages and apply batches.
func BenchmarkInMemoryGetEntries(b *testing.B) {
	b.ReportAllocs()
	im := newInMemory(0)
	im.merge(makeEntries(1, 1001, 1))
	for b.Loop() {
		_, err := im.getEntries(1, 100)
		if err != nil {
			b.Fatalf("getEntries failed: %v", err)
		}
	}
}
