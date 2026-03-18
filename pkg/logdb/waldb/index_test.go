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

package waldb

import (
	"sync"
	"testing"
)

// TestNewIndexIsEmpty verifies that a newly created index has zero length.
func TestNewIndexIsEmpty(t *testing.T) {
	idx := newIndex()
	if idx.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", idx.Len())
	}
}

// TestNewIndexFirstAndLastReturnFalse verifies First and Last on empty index.
func TestNewIndexFirstAndLastReturnFalse(t *testing.T) {
	idx := newIndex()
	if _, ok := idx.First(); ok {
		t.Fatal("First: got ok=true on empty index, want false")
	}
	if _, ok := idx.Last(); ok {
		t.Fatal("Last: got ok=true on empty index, want false")
	}
}

// TestAppendSingleEntry verifies appending a single entry.
func TestAppendSingleEntry(t *testing.T) {
	idx := newIndex()
	e := indexEntry{Index: 1, Term: 1, SegmentID: 0, Offset: 0, Size: 64}
	idx.Append(e)

	if idx.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", idx.Len())
	}
	got, ok := idx.Lookup(1)
	if !ok {
		t.Fatal("Lookup(1): not found")
	}
	if got.Index != 1 || got.Term != 1 {
		t.Fatalf("Lookup(1): got %+v, want Index=1 Term=1", got)
	}
}

// TestAppendEmptyIsNoOp verifies appending zero entries is a no-op.
func TestAppendEmptyIsNoOp(t *testing.T) {
	idx := newIndex()
	idx.Append()
	if idx.Len() != 0 {
		t.Fatalf("Len after empty Append: got %d, want 0", idx.Len())
	}
}

// TestAppendMultipleEntries verifies appending several entries in order.
func TestAppendMultipleEntries(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1, SegmentID: 0, Offset: int64(i * 64), Size: 64})
	}

	if idx.Len() != 10 {
		t.Fatalf("Len: got %d, want 10", idx.Len())
	}

	first, ok := idx.First()
	if !ok || first.Index != 1 {
		t.Fatalf("First: got %+v, ok=%v, want Index=1", first, ok)
	}
	last, ok := idx.Last()
	if !ok || last.Index != 10 {
		t.Fatalf("Last: got %+v, ok=%v, want Index=10", last, ok)
	}
}

// TestAppendBatchEntries verifies appending multiple entries in a single call.
func TestAppendBatchEntries(t *testing.T) {
	idx := newIndex()
	entries := []indexEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	idx.Append(entries...)

	if idx.Len() != 3 {
		t.Fatalf("Len: got %d, want 3", idx.Len())
	}
}

// TestAppendOverwriteTruncates verifies that appending an entry with an
// index that already exists truncates entries from that point onward.
func TestAppendOverwriteTruncates(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 5; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	// Overwrite from index 3 with a new term.
	idx.Append(indexEntry{Index: 3, Term: 2})

	if idx.Len() != 3 {
		t.Fatalf("Len after overwrite: got %d, want 3", idx.Len())
	}

	// Index 3 should have term 2.
	e, ok := idx.Lookup(3)
	if !ok || e.Term != 2 {
		t.Fatalf("Lookup(3) after overwrite: got %+v ok=%v, want Term=2", e, ok)
	}

	// Index 4 and 5 should no longer exist.
	if _, ok := idx.Lookup(4); ok {
		t.Fatal("Lookup(4): should not exist after truncation")
	}
	if _, ok := idx.Lookup(5); ok {
		t.Fatal("Lookup(5): should not exist after truncation")
	}
}

// TestAppendOverwriteAtFirst verifies overwrite at the very first index.
func TestAppendOverwriteAtFirst(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1})
	idx.Append(indexEntry{Index: 2, Term: 1})
	idx.Append(indexEntry{Index: 3, Term: 1})

	// Overwrite from the beginning.
	idx.Append(indexEntry{Index: 1, Term: 5})
	if idx.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", idx.Len())
	}
	e, ok := idx.Lookup(1)
	if !ok || e.Term != 5 {
		t.Fatalf("Lookup(1): got %+v ok=%v, want Term=5", e, ok)
	}
}

// TestLookupExisting verifies lookup of an existing entry by index.
func TestLookupExisting(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 10, Term: 3, SegmentID: 2, Offset: 100, Size: 50})

	e, ok := idx.Lookup(10)
	if !ok {
		t.Fatal("Lookup(10): not found")
	}
	if e.Index != 10 || e.Term != 3 || e.SegmentID != 2 || e.Offset != 100 || e.Size != 50 {
		t.Fatalf("Lookup(10): got %+v", e)
	}
}

// TestLookupNonExistent verifies that lookup returns false for missing entries.
func TestLookupNonExistent(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1})
	idx.Append(indexEntry{Index: 3, Term: 1})

	if _, ok := idx.Lookup(2); ok {
		t.Fatal("Lookup(2): found non-existent entry")
	}
	if _, ok := idx.Lookup(0); ok {
		t.Fatal("Lookup(0): found non-existent entry")
	}
	if _, ok := idx.Lookup(100); ok {
		t.Fatal("Lookup(100): found non-existent entry")
	}
}

// TestLookupOnEmptyIndex verifies that lookup on an empty index returns false.
func TestLookupOnEmptyIndex(t *testing.T) {
	idx := newIndex()
	if _, ok := idx.Lookup(1); ok {
		t.Fatal("Lookup(1): found entry in empty index")
	}
}

// TestRangeFullSet verifies Range returns all entries when range covers
// the entire index.
func TestRangeFullSet(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 5; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	result := idx.Range(1, 6)
	if len(result) != 5 {
		t.Fatalf("Range(1,6): got %d entries, want 5", len(result))
	}
	for i, e := range result {
		if e.Index != uint64(i+1) {
			t.Fatalf("Range(1,6)[%d]: got Index=%d, want %d", i, e.Index, i+1)
		}
	}
}

// TestRangeSubset verifies Range returns only entries in [low, high).
func TestRangeSubset(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	result := idx.Range(3, 7)
	if len(result) != 4 {
		t.Fatalf("Range(3,7): got %d entries, want 4", len(result))
	}
	for i, e := range result {
		expected := uint64(i + 3)
		if e.Index != expected {
			t.Fatalf("Range(3,7)[%d]: got Index=%d, want %d", i, e.Index, expected)
		}
	}
}

// TestRangeEmptyResult verifies Range returns nil for non-overlapping ranges.
func TestRangeEmptyResult(t *testing.T) {
	idx := newIndex()
	for i := uint64(5); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	if result := idx.Range(1, 5); result != nil {
		t.Fatalf("Range(1,5): got %d entries, want nil", len(result))
	}
	if result := idx.Range(11, 20); result != nil {
		t.Fatalf("Range(11,20): got %d entries, want nil", len(result))
	}
}

// TestRangeInvalidBounds verifies Range returns nil when low >= high.
func TestRangeInvalidBounds(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1})

	if result := idx.Range(5, 3); result != nil {
		t.Fatalf("Range(5,3): got %d entries, want nil", len(result))
	}
	if result := idx.Range(1, 1); result != nil {
		t.Fatalf("Range(1,1): got %d entries, want nil", len(result))
	}
}

// TestRangeOnEmptyIndex verifies Range on empty index returns nil.
func TestRangeOnEmptyIndex(t *testing.T) {
	idx := newIndex()
	if result := idx.Range(1, 10); result != nil {
		t.Fatalf("Range(1,10) on empty: got %d entries, want nil", len(result))
	}
}

// TestRangeReturnsCopy verifies that the returned slice is an independent
// copy that does not alias internal state. Mutating the result must not
// affect the index.
func TestRangeReturnsCopy(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1})
	idx.Append(indexEntry{Index: 2, Term: 1})
	idx.Append(indexEntry{Index: 3, Term: 1})

	result := idx.Range(1, 4)
	if len(result) != 3 {
		t.Fatalf("Range(1,4): got %d entries, want 3", len(result))
	}

	// Mutating the result must NOT affect the internal state.
	result[0].Term = 999
	e, ok := idx.Lookup(1)
	if !ok {
		t.Fatal("Lookup(1): not found")
	}
	if e.Term != 1 {
		t.Fatalf("Lookup(1) after mutating Range result: Term=%d, want 1 (should be a copy)", e.Term)
	}
}

// TestRangeIndependentCopies verifies that two Range calls on overlapping
// regions return independent copies.
func TestRangeIndependentCopies(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1, SegmentID: i})
	}

	result := idx.Range(3, 8)
	if len(result) != 5 {
		t.Fatalf("Range(3,8): got %d entries, want 5", len(result))
	}

	// Verify entries match expected values.
	for i, e := range result {
		expected := uint64(i + 3)
		if e.Index != expected || e.SegmentID != expected {
			t.Fatalf("Range(3,8)[%d]: got Index=%d SegmentID=%d, want %d",
				i, e.Index, e.SegmentID, expected)
		}
	}

	// A second Range on overlapping region returns an independent copy.
	result2 := idx.Range(3, 6)
	if len(result2) != 3 {
		t.Fatalf("Range(3,6): got %d entries, want 3", len(result2))
	}

	// Mutate via result2; result should be unaffected (independent copies).
	result2[0].Term = 42
	if result[0].Term != 1 {
		t.Fatalf("overlapping results should be independent: got Term=%d, want 1", result[0].Term)
	}
}

// TestCompactRemovesEntriesUpTo verifies Compact removes all entries <= upTo.
func TestCompactRemovesEntriesUpTo(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	idx.Compact(5)

	if idx.Len() != 5 {
		t.Fatalf("Len after Compact(5): got %d, want 5", idx.Len())
	}

	// Entries 1-5 should be gone.
	for i := uint64(1); i <= 5; i++ {
		if _, ok := idx.Lookup(i); ok {
			t.Fatalf("Lookup(%d): found entry after compaction", i)
		}
	}

	// Entries 6-10 should remain.
	for i := uint64(6); i <= 10; i++ {
		if _, ok := idx.Lookup(i); !ok {
			t.Fatalf("Lookup(%d): not found after compaction", i)
		}
	}
}

// TestCompactAll verifies compacting all entries leaves an empty index.
func TestCompactAll(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 5; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	idx.Compact(10) // Compact past the last entry.

	if idx.Len() != 0 {
		t.Fatalf("Len after full compaction: got %d, want 0", idx.Len())
	}
}

// TestCompactNone verifies compacting to 0 when first entry is 1 is a no-op.
func TestCompactNone(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 5; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	idx.Compact(0)

	if idx.Len() != 5 {
		t.Fatalf("Len after Compact(0): got %d, want 5", idx.Len())
	}
}

// TestCompactOnEmptyIndex verifies Compact on empty index does not panic.
func TestCompactOnEmptyIndex(t *testing.T) {
	idx := newIndex()
	idx.Compact(100) // Should not panic.
	if idx.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", idx.Len())
	}
}

// TestCompactThenAppend verifies new entries can be appended after compaction.
func TestCompactThenAppend(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 5; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	idx.Compact(3)
	idx.Append(indexEntry{Index: 6, Term: 2})
	idx.Append(indexEntry{Index: 7, Term: 2})

	if idx.Len() != 4 { // 4,5,6,7
		t.Fatalf("Len: got %d, want 4", idx.Len())
	}

	e, ok := idx.Lookup(6)
	if !ok || e.Term != 2 {
		t.Fatalf("Lookup(6): got %+v ok=%v", e, ok)
	}
}

// TestAllSegmentIDs verifies AllSegmentIDs returns all referenced segments.
func TestAllSegmentIDs(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1, SegmentID: 5})
	idx.Append(indexEntry{Index: 2, Term: 1, SegmentID: 10})
	idx.Append(indexEntry{Index: 3, Term: 1, SegmentID: 5})

	segs := idx.AllSegmentIDs()
	if len(segs) != 2 {
		t.Fatalf("AllSegmentIDs: got %d, want 2", len(segs))
	}
	if !segs[5] || !segs[10] {
		t.Fatalf("AllSegmentIDs: expected 5 and 10, got %v", segs)
	}
}

// TestAllSegmentIDsEmptyIndex verifies AllSegmentIDs on empty index.
func TestAllSegmentIDsEmptyIndex(t *testing.T) {
	idx := newIndex()
	segs := idx.AllSegmentIDs()
	if len(segs) != 0 {
		t.Fatalf("AllSegmentIDs on empty: got %d, want 0", len(segs))
	}
}

// TestConcurrentAppendAndLookup verifies thread safety of concurrent reads
// and writes to the index.
func TestConcurrentAppendAndLookup(t *testing.T) {
	idx := newIndex()
	count := 1000
	var wg sync.WaitGroup

	// Writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= uint64(count); i++ {
			idx.Append(indexEntry{Index: i, Term: 1})
		}
	}()

	// Multiple reader goroutines.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(1); i <= uint64(count); i++ {
				idx.Lookup(i) // May or may not find, that is fine.
			}
		}()
	}

	wg.Wait()

	// All entries should be present after all writers finish.
	if idx.Len() != count {
		t.Fatalf("Len: got %d, want %d", idx.Len(), count)
	}
}

// TestConcurrentRangeAndCompact verifies thread safety of concurrent Range
// and Compact operations.
func TestConcurrentRangeAndCompact(t *testing.T) {
	idx := newIndex()
	count := 500
	for i := uint64(1); i <= uint64(count); i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	var wg sync.WaitGroup

	// Compactor goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= uint64(count/2); i++ {
			idx.Compact(i)
		}
	}()

	// Range reader goroutines.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				idx.Range(1, uint64(count)+1)
			}
		}()
	}

	wg.Wait()

	// After compaction up to count/2, remaining entries should be from
	// count/2+1 to count.
	expected := count - count/2
	if idx.Len() != expected {
		t.Fatalf("Len after concurrent compact: got %d, want %d", idx.Len(), expected)
	}
}

// TestFirstAndLast verifies First and Last return correct entries.
func TestFirstAndLast(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 5, Term: 1})
	idx.Append(indexEntry{Index: 10, Term: 2})
	idx.Append(indexEntry{Index: 15, Term: 3})

	first, ok := idx.First()
	if !ok || first.Index != 5 {
		t.Fatalf("First: got %+v ok=%v, want Index=5", first, ok)
	}

	last, ok := idx.Last()
	if !ok || last.Index != 15 || last.Term != 3 {
		t.Fatalf("Last: got %+v ok=%v, want Index=15 Term=3", last, ok)
	}
}

// TestFirstAndLastAfterCompact verifies First/Last after partial compaction.
func TestFirstAndLastAfterCompact(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	idx.Compact(7)

	first, ok := idx.First()
	if !ok || first.Index != 8 {
		t.Fatalf("First after Compact(7): got %+v ok=%v, want Index=8", first, ok)
	}

	last, ok := idx.Last()
	if !ok || last.Index != 10 {
		t.Fatalf("Last after Compact(7): got %+v ok=%v, want Index=10", last, ok)
	}
}
