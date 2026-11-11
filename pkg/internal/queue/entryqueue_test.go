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

package queue

import (
	"sync"
	"testing"
)

func TestEntryQueue_AddRejectsWhenFull(t *testing.T) {
	q := NewEntryQueue[int](4, 5)
	if !q.Add([]int{1, 2, 3}) {
		t.Fatal("first Add should succeed")
	}
	// Adding 3 more would exceed maxLen of 5 (3 + 3 = 6 > 5).
	if q.Add([]int{4, 5, 6}) {
		t.Fatal("Add should reject when batch would exceed maxLen")
	}
	if !q.Add([]int{4, 5}) {
		t.Fatal("Add should succeed when items fit exactly at maxLen")
	}
	if q.Add([]int{6}) {
		t.Fatal("Add should reject when queue is at capacity")
	}
	// Only the accepted batches are present.
	items := q.Swap(nil)
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
}

func TestEntryQueue_EmptyTracksEveryTransition(t *testing.T) {
	q := NewEntryQueue[int](4, 3)
	check := func(step string, want bool) {
		t.Helper()
		if got := q.Empty(); got != want {
			t.Fatalf("%s: Empty() = %v, want %v (Len %d)", step, got, want, q.Len())
		}
	}
	check("new", true)
	if !q.Add(nil) {
		t.Fatal("Add of nil refused")
	}
	check("add nil", true)
	if !q.Add([]int{}) {
		t.Fatal("Add of empty slice refused")
	}
	check("add empty slice", true)
	q.Add([]int{1, 2})
	check("add", false)
	if q.Add([]int{3, 4}) {
		t.Fatal("overflowing Add accepted")
	}
	check("rejected add", false)
	q.Swap(nil)
	check("swap", true)
	q.Swap(nil)
	check("swap of empty", true)
	q.AddOne(5)
	check("add one", false)
	buf, id := q.SwapBatch(make([]int, 0, 4))
	if len(buf) != 1 || id == 0 {
		t.Fatalf("SwapBatch = %v, %d", buf, id)
	}
	check("swap batch", true)
	q.AddOne(6)
	check("add one again", false)
	q.Close()
	check("close", true)
	if q.Add([]int{7}) || q.AddOne(8) {
		t.Fatal("add after close accepted")
	}
	check("add after close", true)
}

func TestEntryQueue_EmptyAgreesWithSoleConsumer(t *testing.T) {
	const producers, perProducer = 3, 2000
	q := NewEntryQueue[int](8, producers*perProducer)
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perProducer {
				if i%2 == 0 {
					q.AddOne(p*perProducer + i)
				} else {
					q.Add([]int{p*perProducer + i})
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	// Only this goroutine takes, so a queue that reported non-empty cannot
	// be emptied before the Swap that follows.
	seen := make(map[int]bool, producers*perProducer)
	take := func(items []int) {
		for _, v := range items {
			if seen[v] {
				t.Fatalf("item %d taken twice", v)
			}
			seen[v] = true
		}
	}
	var buf []int
	for finished := false; !finished; {
		select {
		case <-done:
			finished = true
		default:
		}
		if q.Empty() {
			continue
		}
		buf = q.Swap(buf[:0])
		if len(buf) == 0 {
			t.Fatal("Empty() reported false but Swap took nothing")
		}
		take(buf)
	}
	take(q.Swap(buf[:0]))
	if !q.Empty() || q.Len() != 0 {
		t.Fatalf("drained queue: Empty() = %v, Len() = %d", q.Empty(), q.Len())
	}
	// Every item, from Add and AddOne alike, is taken exactly once.
	for i := range producers * perProducer {
		if !seen[i] {
			t.Fatalf("missing item %d", i)
		}
	}
}

// --- AddOne tests ---

func TestEntryQueue_AddOneFailsWhenFull(t *testing.T) {
	q := NewEntryQueue[int](4, 3)
	// Fill the queue to maxLen.
	q.Add([]int{1, 2, 3})
	if q.Len() != 3 {
		t.Fatalf("expected Len() 3, got %d", q.Len())
	}
	// Try to add one more item.
	ok := q.AddOne(4)
	if ok {
		t.Fatal("AddOne should reject when queue is at capacity")
	}
	// Verify original items are intact.
	items := q.Swap(nil)
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0] != 1 || items[1] != 2 || items[2] != 3 {
		t.Fatalf("items = %v, want [1 2 3]", items)
	}
}

func TestEntryQueue_AddOneAndAddMixed(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	ok := q.AddOne(1)
	if !ok {
		t.Fatal("AddOne should succeed")
	}
	ok = q.Add([]int{2, 3})
	if !ok {
		t.Fatal("Add should succeed")
	}
	ok = q.AddOne(4)
	if !ok {
		t.Fatal("second AddOne should succeed")
	}
	items := q.Swap(nil)
	if len(items) != 4 {
		t.Fatalf("expected 4 items, got %d", len(items))
	}
	if items[0] != 1 || items[1] != 2 || items[2] != 3 || items[3] != 4 {
		t.Fatalf("items = %v, want [1 2 3 4]", items)
	}

	// Passing the previous result back makes its backing array the new
	// write buffer; the items added into it come back intact and in order.
	q.Add([]int{40, 50})
	if second := q.Swap(items[:0]); len(second) != 2 || second[0] != 40 || second[1] != 50 {
		t.Fatalf("swap with a caller buffer = %v, want [40 50]", second)
	}
	q.Add([]int{60, 70})
	if third := q.Swap(nil); len(third) != 2 || third[0] != 60 || third[1] != 70 {
		t.Fatalf("swap of the recycled buffer = %v, want [60 70]", third)
	}
}

// TestEntryQueue_TakenTracksTheSwapThatTookTheItems: AddTracked records where
// items entered the queue, and Taken reports them taken exactly once a Swap
// has taken them; a never-added item is never taken.
func TestEntryQueue_TakenTracksTheSwapThatTookTheItems(t *testing.T) {
	q := NewEntryQueue[int](4, 16)
	var first, second, never uint64
	if !q.AddTracked([]int{1}, &first) || first == 0 {
		t.Fatalf("AddTracked recorded %d, want a non-zero batch id", first)
	}
	if taken, closed := q.Taken(&first); taken || closed {
		t.Fatalf("queued item: taken=%v closed=%v, want neither", taken, closed)
	}
	_, id := q.SwapBatch(nil)
	if id != first {
		t.Fatalf("SwapBatch took batch %d, want %d", id, first)
	}
	if taken, _ := q.Taken(&first); taken {
		t.Fatal("an item swapped out but not yet handed off is reported taken")
	}
	if !q.HandOff(id) {
		t.Fatal("HandOff refused on an open queue")
	}
	if taken, _ := q.Taken(&first); !taken {
		t.Fatal("an item whose batch was handed off is not reported taken")
	}
	if !q.AddTracked([]int{2}, &second) {
		t.Fatal("AddTracked refused")
	}
	if taken, _ := q.Taken(&second); taken {
		t.Fatal("an item added after the hand-off is reported taken")
	}
	if taken, _ := q.Taken(&never); taken {
		t.Fatal("an item never added is reported taken")
	}
}

// TestEntryQueue_CloseIsFinal: after Close nothing is added or taken, so an
// item still queued is never taken and Taken's answer is final.
func TestEntryQueue_CloseIsFinal(t *testing.T) {
	q := NewEntryQueue[int](4, 16)
	var queued, late uint64
	q.AddTracked([]int{1}, &queued)
	q.Close()
	if got := q.Swap(nil); len(got) != 0 {
		t.Fatalf("Swap after Close took %v, want nothing", got)
	}
	taken, closed := q.Taken(&queued)
	if taken || !closed {
		t.Fatalf("queued item after Close: taken=%v closed=%v, want untaken and closed", taken, closed)
	}
	if q.AddTracked([]int{2}, &late) || q.Add([]int{3}) || q.AddOne(4) {
		t.Fatal("an add after Close was accepted")
	}
	if late != 0 {
		t.Fatalf("a refused AddTracked recorded %d, want it untouched", late)
	}
}

// TestEntryQueue_HandOffAfterCloseIsRefused: a batch taken before Close but
// not yet handed off can no longer be handed off, so Taken's final answer for
// it — untaken — stays true.
func TestEntryQueue_HandOffAfterCloseIsRefused(t *testing.T) {
	q := NewEntryQueue[int](4, 16)
	var at uint64
	q.AddTracked([]int{1}, &at)
	_, id := q.SwapBatch(nil)
	q.Close()
	if q.HandOff(id) {
		t.Fatal("HandOff succeeded on a closed queue")
	}
	if taken, closed := q.Taken(&at); taken || !closed {
		t.Fatalf("taken=%v closed=%v, want final and untaken", taken, closed)
	}
}

// --- Benchmarks ---

func BenchmarkEntryQueue_Add(b *testing.B) {
	q := NewEntryQueue[int](64, b.N+1)
	item := []int{42}
	b.ResetTimer()
	for range b.N {
		q.Add(item)
	}
}

func BenchmarkEntryQueue_AddSwap(b *testing.B) {
	q := NewEntryQueue[int](64, 1024)
	item := []int{42}
	var buf []int
	b.ResetTimer()
	for range b.N {
		q.Add(item)
		if q.Len() >= 64 {
			buf = q.Swap(buf[:0])
		}
	}
}

func BenchmarkEntryQueue_Contended(b *testing.B) {
	q := NewEntryQueue[int](64, b.N*8+1)
	item := []int{42}
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Add(item)
		}
	})
}

// BenchmarkEntryQueue_LenZero and BenchmarkEntryQueue_Empty compare the two
// ways to ask whether an idle queue holds anything; the step worker asks once
// per shard per tick.
func BenchmarkEntryQueue_LenZero(b *testing.B) {
	q := NewEntryQueue[int](64, 1024)
	empty := false
	for b.Loop() {
		empty = q.Len() == 0
	}
	if !empty {
		b.Fatal("idle queue reported non-empty")
	}
}

func BenchmarkEntryQueue_Empty(b *testing.B) {
	q := NewEntryQueue[int](64, 1024)
	empty := false
	for b.Loop() {
		empty = q.Empty()
	}
	if !empty {
		b.Fatal("idle queue reported non-empty")
	}
}

func BenchmarkChannel_Contended(b *testing.B) {
	ch := make(chan int, 256)
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			select {
			case ch <- 42:
			default:
				// Drain some to make room.
				select {
				case <-ch:
				default:
				}
			}
		}
	})
}
