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

// TestEmptyIndex checks every accessor of a new index, and that an empty
// Append and a Compact leave it empty.
func TestEmptyIndex(t *testing.T) {
	idx := newIndex()
	if idx.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", idx.Len())
	}
	if _, ok := idx.First(); ok {
		t.Fatal("First: got ok=true on empty index, want false")
	}
	if _, ok := idx.Last(); ok {
		t.Fatal("Last: got ok=true on empty index, want false")
	}
	if result := idx.Range(1, 10); result != nil {
		t.Fatalf("Range(1,10) on empty: got %d entries, want nil", len(result))
	}
	if segs := idx.AllSegmentIDs(); len(segs) != 0 {
		t.Fatalf("AllSegmentIDs on empty: got %d, want 0", len(segs))
	}
	idx.Append()
	idx.Compact(100)
	if idx.Len() != 0 {
		t.Fatalf("Len after empty Append and Compact: got %d, want 0", idx.Len())
	}
}

// TestAppendMultipleEntries checks a batch Append followed by single Appends
// grows the index in order, with First and Last at the ends.
func TestAppendMultipleEntries(t *testing.T) {
	idx := newIndex()
	idx.Append(
		indexEntry{Index: 1, Term: 1},
		indexEntry{Index: 2, Term: 2},
		indexEntry{Index: 3, Term: 3},
	)
	if idx.Len() != 3 {
		t.Fatalf("Len after batch Append: got %d, want 3", idx.Len())
	}
	for i := uint64(4); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: i, SegmentID: 0, Offset: int64(i * 64), Size: 64})
	}

	if idx.Len() != 10 {
		t.Fatalf("Len: got %d, want 10", idx.Len())
	}

	first, ok := idx.First()
	if !ok || first.Index != 1 || first.Term != 1 {
		t.Fatalf("First: got %+v, ok=%v, want Index=1 Term=1", first, ok)
	}
	last, ok := idx.Last()
	if !ok || last.Index != 10 || last.Term != 10 {
		t.Fatalf("Last: got %+v, ok=%v, want Index=10 Term=10", last, ok)
	}
}

// TestAppendOverwriteTruncates checks an Append at or below the last index
// replaces that entry and drops every entry after it.
func TestAppendOverwriteTruncates(t *testing.T) {
	tests := []struct {
		name    string
		at      uint64
		wantLen int
	}{
		{"middle", 3, 3},
		{"first", 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := newIndex()
			for i := uint64(1); i <= 5; i++ {
				idx.Append(indexEntry{Index: i, Term: 1})
			}

			idx.Append(indexEntry{Index: tc.at, Term: 2})

			if idx.Len() != tc.wantLen {
				t.Fatalf("Len after overwrite: got %d, want %d", idx.Len(), tc.wantLen)
			}
			e, ok := idx.Lookup(tc.at)
			if !ok || e.Term != 2 {
				t.Fatalf("Lookup(%d) after overwrite: got %+v ok=%v, want Term=2", tc.at, e, ok)
			}
			for i := tc.at + 1; i <= 5; i++ {
				if _, ok := idx.Lookup(i); ok {
					t.Fatalf("Lookup(%d): should not exist after truncation", i)
				}
			}
		})
	}
}

// TestLookupExisting checks Lookup returns every field of the stored entry.
func TestLookupExisting(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 10, Term: 3, SegmentID: 2, Offset: 100, Size: 50})

	if idx.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", idx.Len())
	}
	e, ok := idx.Lookup(10)
	if !ok {
		t.Fatal("Lookup(10): not found")
	}
	if e.Index != 10 || e.Term != 3 || e.SegmentID != 2 || e.Offset != 100 || e.Size != 50 {
		t.Fatalf("Lookup(10): got %+v", e)
	}
}

// TestLookupNonExistent checks Lookup misses a gap, and indexes below and
// above the stored range.
func TestLookupNonExistent(t *testing.T) {
	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1})
	idx.Append(indexEntry{Index: 3, Term: 1})

	for _, i := range []uint64{2, 0, 100} {
		if _, ok := idx.Lookup(i); ok {
			t.Fatalf("Lookup(%d): found non-existent entry", i)
		}
	}
}

// TestRange checks Range returns the entries in [low, high), and nil for a
// window outside the stored range or an empty or inverted window.
func TestRange(t *testing.T) {
	idx := newIndex()
	for i := uint64(5); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}
	tests := []struct {
		name      string
		low, high uint64
		want      []uint64
	}{
		{"subset", 6, 9, []uint64{6, 7, 8}},
		{"below first", 1, 5, nil},
		{"above last", 11, 20, nil},
		{"inverted", 9, 6, nil},
		{"empty window", 6, 6, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := idx.Range(tc.low, tc.high)
			if tc.want == nil {
				if result != nil {
					t.Fatalf("Range(%d,%d): got %d entries, want nil", tc.low, tc.high, len(result))
				}
				return
			}
			if len(result) != len(tc.want) {
				t.Fatalf("Range(%d,%d): got %d entries, want %d", tc.low, tc.high, len(result), len(tc.want))
			}
			for i, e := range result {
				if e.Index != tc.want[i] {
					t.Fatalf("Range(%d,%d)[%d]: got Index=%d, want %d", tc.low, tc.high, i, e.Index, tc.want[i])
				}
			}
		})
	}
}

// TestRangeReturnsCopy verifies that mutating the slice returned by Range
// does not affect the index's internal state.
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

// TestCompact checks Compact(upTo) drops every entry at or below upTo and
// keeps the rest, with First and Last at the ends of what remains.
func TestCompact(t *testing.T) {
	tests := []struct {
		name string
		upTo uint64
	}{
		{"none", 0},
		{"partial", 7},
		{"past last", 15},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := newIndex()
			for i := uint64(1); i <= 10; i++ {
				idx.Append(indexEntry{Index: i, Term: 1})
			}

			idx.Compact(tc.upTo)

			lo := min(tc.upTo, 10) + 1 // first index that remains
			if want := int(11 - lo); idx.Len() != want {
				t.Fatalf("Len after Compact(%d): got %d, want %d", tc.upTo, idx.Len(), want)
			}
			for i := uint64(1); i <= 10; i++ {
				if _, ok := idx.Lookup(i); ok != (i >= lo) {
					t.Fatalf("Lookup(%d) after Compact(%d): found=%v, want %v", i, tc.upTo, ok, i >= lo)
				}
			}
			first, ok := idx.First()
			if lo > 10 {
				if ok {
					t.Fatalf("First after full Compact: got %+v, want none", first)
				}
				return
			}
			if !ok || first.Index != lo {
				t.Fatalf("First after Compact(%d): got %+v ok=%v, want Index=%d", tc.upTo, first, ok, lo)
			}
			if last, ok := idx.Last(); !ok || last.Index != 10 {
				t.Fatalf("Last after Compact(%d): got %+v ok=%v, want Index=10", tc.upTo, last, ok)
			}
		})
	}
}

// TestCompactThenAppend checks an Append after a Compact extends what remains.
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

// TestAllSegmentIDs checks AllSegmentIDs returns each distinct segment once.
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
