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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// requireIndexBounds fails t unless idx.Bounds reports first.Index,
// last.Index and count equal to the wanted values. A zero wantCount means
// the index must be empty and both entries zero-valued.
func requireIndexBounds(ctx context.Context, t *testing.T, idx *index, wantFirst, wantLast uint64, wantCount int) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before Bounds check: %v", err)
	}
	first, last, count := idx.Bounds()
	if wantCount == 0 {
		if first != (indexEntry{}) || last != (indexEntry{}) || count != 0 {
			t.Fatalf("Bounds: got %+v, %+v, %d, want zero entries and 0", first, last, count)
		}
		return
	}
	if first.Index != wantFirst || last.Index != wantLast || count != wantCount {
		t.Fatalf("Bounds: got first=%d last=%d count=%d, want %d, %d, %d",
			first.Index, last.Index, count, wantFirst, wantLast, wantCount)
	}
}

// TestIndexBoundsEmpty checks a new index reports two zero-valued entries
// and a count of 0.
func TestIndexBoundsEmpty(t *testing.T) {
	requireIndexBounds(t.Context(), t, newIndex(), 0, 0, 0)
}

// TestIndexBoundsMatchesFirstLastLen checks Bounds agrees field for field
// with First, Last and Len after appending Index 1..10.
func TestIndexBoundsMatchesFirstLastLen(t *testing.T) {
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: i, SegmentID: i % 3, Offset: int64(i * 64), Size: 64})
	}
	first, last, count := idx.Bounds()
	wantFirst, ok := idx.First()
	if !ok {
		t.Fatal("First: got ok=false on a non-empty index")
	}
	wantLast, ok := idx.Last()
	if !ok {
		t.Fatal("Last: got ok=false on a non-empty index")
	}
	if first != wantFirst {
		t.Fatalf("Bounds first: got %+v, First %+v", first, wantFirst)
	}
	if last != wantLast {
		t.Fatalf("Bounds last: got %+v, Last %+v", last, wantLast)
	}
	if count != idx.Len() {
		t.Fatalf("Bounds count: got %d, Len %d", count, idx.Len())
	}
	requireIndexBounds(t.Context(), t, idx, 1, 10, 10)
}

// TestIndexBoundsAfterOverwriteAndCompact checks Bounds after an overwrite
// truncates the tail, after a Compact below the tail, and after a Compact of
// every remaining entry.
func TestIndexBoundsAfterOverwriteAndCompact(t *testing.T) {
	ctx := t.Context()
	idx := newIndex()
	for i := uint64(1); i <= 10; i++ {
		idx.Append(indexEntry{Index: i, Term: 1})
	}

	idx.Append(indexEntry{Index: 6, Term: 2})
	requireIndexBounds(ctx, t, idx, 1, 6, 6)
	if _, last, _ := idx.Bounds(); last.Term != 2 {
		t.Fatalf("Bounds last after overwrite: got Term=%d, want 2", last.Term)
	}

	idx.Compact(3)
	requireIndexBounds(ctx, t, idx, 4, 6, 3)

	idx.Compact(6)
	requireIndexBounds(ctx, t, idx, 0, 0, 0)
}

// TestIndexBoundsConcurrentAppendCompact checks every Bounds result taken
// while one goroutine appends contiguous indexes and another compacts below
// the tail describes one contiguous range: first <= last and
// last-first+1 == count.
func TestIndexBoundsConcurrentAppendCompact(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	idx := newIndex()
	idx.Append(indexEntry{Index: 1, Term: 1})

	// started is a start barrier: sampling begins only once both workers
	// run. appends and compactions count each mutation that changed the
	// index, so the test can prove both happened while it sampled.
	var started, wg sync.WaitGroup
	var appends, compactions atomic.Uint64
	started.Add(2)
	wg.Add(2)
	go func() { // appender
		defer wg.Done()
		started.Done()
		for i := uint64(2); ctx.Err() == nil; i++ {
			idx.Append(indexEntry{Index: i, Term: 1})
			appends.Add(1)
		}
	}()
	go func() { // compactor, always below the tail
		defer wg.Done()
		started.Done()
		for ctx.Err() == nil {
			last, ok := idx.Last()
			if !ok || last.Index <= 1 {
				continue
			}
			// Only the compactor moves First, so an upTo at or above it
			// removes at least one entry.
			if first, ok := idx.First(); ok && first.Index <= last.Index-1 {
				idx.Compact(last.Index - 1)
				compactions.Add(1)
			}
		}
	}()
	started.Wait()

	const (
		minReads    = 10000
		minProgress = 100
		maxSampling = 10 * time.Second
	)
	appendsBefore, compactionsBefore := appends.Load(), compactions.Load()
	deadline := time.Now().Add(maxSampling)
	var reads, bad, nonEmpty int
	var badFirst, badLast uint64
	var badCount int
	enough := func() bool {
		return reads >= minReads &&
			appends.Load()-appendsBefore >= minProgress &&
			compactions.Load()-compactionsBefore >= minProgress
	}
	for !enough() && time.Now().Before(deadline) {
		reads++
		first, last, count := idx.Bounds()
		if count == 0 {
			continue
		}
		nonEmpty++
		if first.Index > last.Index || last.Index-first.Index+1 != uint64(count) {
			if bad == 0 {
				badFirst, badLast, badCount = first.Index, last.Index, count
			}
			bad++
		}
	}
	appendsDuring := appends.Load() - appendsBefore
	compactionsDuring := compactions.Load() - compactionsBefore
	cancel()
	wg.Wait()

	if bad != 0 {
		t.Fatalf("%d of %d non-empty Bounds results inconsistent; first: first=%d last=%d count=%d",
			bad, nonEmpty, badFirst, badLast, badCount)
	}
	if nonEmpty == 0 {
		t.Fatal("every Bounds result was empty; the index should never empty under append and compact-below-tail")
	}
	if appendsDuring < minProgress || compactionsDuring < minProgress {
		t.Fatalf("mutations during %d Bounds reads: %d appends, %d compactions; want at least %d of each",
			reads, appendsDuring, compactionsDuring, minProgress)
	}
}
