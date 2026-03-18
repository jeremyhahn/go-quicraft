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

func TestEntryQueue_AddAndSwap(t *testing.T) {
	q := NewEntryQueue[int](8, 256)
	ok := q.Add([]int{1, 2, 3})
	if !ok {
		t.Fatal("Add should succeed when queue is empty")
	}
	items := q.Swap(nil)
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0] != 1 || items[1] != 2 || items[2] != 3 {
		t.Fatalf("items = %v, want [1 2 3]", items)
	}
}

func TestEntryQueue_AddRejectsWhenFull(t *testing.T) {
	q := NewEntryQueue[int](4, 4)
	ok := q.Add([]int{1, 2, 3, 4})
	if !ok {
		t.Fatal("Add should succeed when items fit exactly at maxLen")
	}
	ok = q.Add([]int{5})
	if ok {
		t.Fatal("Add should reject when queue is at capacity")
	}
	// Verify original items are still intact.
	items := q.Swap(nil)
	if len(items) != 4 {
		t.Fatalf("expected 4 items, got %d", len(items))
	}
}

func TestEntryQueue_AddRejectsPartialOverflow(t *testing.T) {
	q := NewEntryQueue[int](4, 5)
	ok := q.Add([]int{1, 2, 3})
	if !ok {
		t.Fatal("first Add should succeed")
	}
	// Adding 3 more would exceed maxLen of 5 (3 + 3 = 6 > 5).
	ok = q.Add([]int{4, 5, 6})
	if ok {
		t.Fatal("Add should reject when batch would exceed maxLen")
	}
	// Only the first 3 should be present.
	items := q.Swap(nil)
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
}

func TestEntryQueue_SwapEmpty(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	buf := make([]int, 0, 8)
	items := q.Swap(buf)
	if len(items) != 0 {
		t.Fatalf("expected empty result, got %d items", len(items))
	}
}

func TestEntryQueue_SwapReusesBuf(t *testing.T) {
	q := NewEntryQueue[int](4, 256)

	// First cycle: add items, swap with nil.
	q.Add([]int{10, 20, 30})
	first := q.Swap(nil)
	if len(first) != 3 {
		t.Fatalf("expected 3 items, got %d", len(first))
	}

	// Second cycle: add more items, swap passing the previous result.
	// The queue should reuse first's backing array as the new write buffer.
	q.Add([]int{40, 50})
	second := q.Swap(first[:0])
	if len(second) != 2 {
		t.Fatalf("expected 2 items, got %d", len(second))
	}
	if second[0] != 40 || second[1] != 50 {
		t.Fatalf("items = %v, want [40 50]", second)
	}
}

func TestEntryQueue_SwapClearsQueue(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	q.Add([]int{1, 2})
	_ = q.Swap(nil)

	// Second swap should return empty.
	items := q.Swap(nil)
	if len(items) != 0 {
		t.Fatalf("expected empty after second swap, got %d items", len(items))
	}
}

func TestEntryQueue_MultipleAddsThenSwap(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	for i := range 5 {
		ok := q.Add([]int{i})
		if !ok {
			t.Fatalf("Add(%d) should succeed", i)
		}
	}
	items := q.Swap(nil)
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
	for i, v := range items {
		if v != i {
			t.Fatalf("items[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestEntryQueue_Len(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	if q.Len() != 0 {
		t.Fatalf("expected Len() 0, got %d", q.Len())
	}
	q.Add([]int{1, 2, 3})
	if q.Len() != 3 {
		t.Fatalf("expected Len() 3, got %d", q.Len())
	}
	q.Swap(nil)
	if q.Len() != 0 {
		t.Fatalf("expected Len() 0 after swap, got %d", q.Len())
	}
}

func TestEntryQueue_GrowsBeyondInitialCapacity(t *testing.T) {
	q := NewEntryQueue[int](2, 1000)
	batch := make([]int, 100)
	for i := range batch {
		batch[i] = i
	}
	ok := q.Add(batch)
	if !ok {
		t.Fatal("Add should succeed within maxLen")
	}
	items := q.Swap(nil)
	if len(items) != 100 {
		t.Fatalf("expected 100 items, got %d", len(items))
	}
}

func TestEntryQueue_ConcurrentProducers(t *testing.T) {
	const numProducers = 8
	const itemsPerProducer = 100
	q := NewEntryQueue[int](64, numProducers*itemsPerProducer)

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for p := range numProducers {
		go func(id int) {
			defer wg.Done()
			for i := range itemsPerProducer {
				ok := q.Add([]int{id*itemsPerProducer + i})
				if !ok {
					// Under maxLen, all should succeed.
					t.Errorf("producer %d: Add(%d) rejected", id, i)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	items := q.Swap(nil)
	if len(items) != numProducers*itemsPerProducer {
		t.Fatalf("expected %d items, got %d", numProducers*itemsPerProducer, len(items))
	}

	// Verify all items are present (order may vary due to concurrency).
	seen := make(map[int]bool, len(items))
	for _, v := range items {
		seen[v] = true
	}
	for i := range numProducers * itemsPerProducer {
		if !seen[i] {
			t.Fatalf("missing item %d", i)
		}
	}
}

func TestEntryQueue_ConcurrentProducersSingleConsumer(t *testing.T) {
	const numProducers = 4
	const itemsPerProducer = 250
	q := NewEntryQueue[int](64, numProducers*itemsPerProducer)

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for p := range numProducers {
		go func(id int) {
			defer wg.Done()
			for i := range itemsPerProducer {
				q.Add([]int{id*itemsPerProducer + i})
			}
		}(p)
	}

	// Wait for all producers to complete.
	wg.Wait()

	// Consumer drains in multiple swap cycles.
	var collected []int
	var buf []int
	for {
		buf = q.Swap(buf[:0])
		if len(buf) == 0 {
			break
		}
		collected = append(collected, buf...)
	}

	if len(collected) != numProducers*itemsPerProducer {
		t.Fatalf("expected %d total items, got %d",
			numProducers*itemsPerProducer, len(collected))
	}
}

func TestEntryQueue_AddEmptySlice(t *testing.T) {
	q := NewEntryQueue[int](4, 10)
	ok := q.Add([]int{})
	if !ok {
		t.Fatal("Add of empty slice should succeed")
	}
	if q.Len() != 0 {
		t.Fatalf("Len should be 0 after adding empty slice, got %d", q.Len())
	}
}

func TestEntryQueue_AddNilSlice(t *testing.T) {
	q := NewEntryQueue[int](4, 10)
	ok := q.Add(nil)
	if !ok {
		t.Fatal("Add of nil slice should succeed")
	}
	if q.Len() != 0 {
		t.Fatalf("Len should be 0 after adding nil, got %d", q.Len())
	}
}

func TestEntryQueue_MaxLenZero(t *testing.T) {
	q := NewEntryQueue[int](0, 0)
	ok := q.Add([]int{1})
	if ok {
		t.Fatal("Add should reject with maxLen=0")
	}
	items := q.Swap(nil)
	if len(items) != 0 {
		t.Fatalf("expected empty, got %d items", len(items))
	}
}

func TestEntryQueue_AddExactlyMaxLen(t *testing.T) {
	q := NewEntryQueue[int](4, 3)
	ok := q.Add([]int{1, 2, 3})
	if !ok {
		t.Fatal("Add of exactly maxLen items should succeed")
	}
	ok = q.Add([]int{4})
	if ok {
		t.Fatal("Add should reject when at maxLen")
	}
}

func TestEntryQueue_SwapAlternatingCycles(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	var buf []int

	for cycle := range 10 {
		ok := q.Add([]int{cycle})
		if !ok {
			t.Fatalf("cycle %d: Add failed", cycle)
		}
		buf = q.Swap(buf[:0])
		if len(buf) != 1 {
			t.Fatalf("cycle %d: expected 1 item, got %d", cycle, len(buf))
		}
		if buf[0] != cycle {
			t.Fatalf("cycle %d: item = %d, want %d", cycle, buf[0], cycle)
		}
	}
}

// --- AddOne tests ---

func TestEntryQueue_AddOneSucceedsWhenNotFull(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	ok := q.AddOne(42)
	if !ok {
		t.Fatal("AddOne should succeed when queue is empty")
	}
	if q.Len() != 1 {
		t.Fatalf("expected Len() 1, got %d", q.Len())
	}
	items := q.Swap(nil)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0] != 42 {
		t.Fatalf("item = %d, want 42", items[0])
	}
}

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

func TestEntryQueue_AddOneAtMaxCapacity(t *testing.T) {
	q := NewEntryQueue[int](4, 1)
	ok := q.AddOne(10)
	if !ok {
		t.Fatal("AddOne should succeed when queue has space")
	}
	if q.Len() != 1 {
		t.Fatalf("expected Len() 1, got %d", q.Len())
	}
	// Second item should be rejected.
	ok = q.AddOne(20)
	if ok {
		t.Fatal("AddOne should reject when at maxLen=1")
	}
	if q.Len() != 1 {
		t.Fatalf("expected Len() 1, got %d", q.Len())
	}
}

func TestEntryQueue_AddOneMultipleThenSwap(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	for i := range 10 {
		ok := q.AddOne(i)
		if !ok {
			t.Fatalf("AddOne(%d) should succeed", i)
		}
	}
	if q.Len() != 10 {
		t.Fatalf("expected Len() 10, got %d", q.Len())
	}
	items := q.Swap(nil)
	if len(items) != 10 {
		t.Fatalf("expected 10 items, got %d", len(items))
	}
	for i, v := range items {
		if v != i {
			t.Fatalf("items[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestEntryQueue_AddOneWithDifferentTypes(t *testing.T) {
	// Test with string type.
	q := NewEntryQueue[string](4, 256)
	ok := q.AddOne("hello")
	if !ok {
		t.Fatal("AddOne should succeed for string type")
	}
	ok = q.AddOne("world")
	if !ok {
		t.Fatal("second AddOne should succeed")
	}
	items := q.Swap(nil)
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0] != "hello" || items[1] != "world" {
		t.Fatalf("items = %v, want [hello world]", items)
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
}

func TestEntryQueue_AddOneConcurrentProducers(t *testing.T) {
	const numProducers = 8
	const itemsPerProducer = 100
	q := NewEntryQueue[int](64, numProducers*itemsPerProducer)

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for p := range numProducers {
		go func(id int) {
			defer wg.Done()
			for i := range itemsPerProducer {
				ok := q.AddOne(id*itemsPerProducer + i)
				if !ok {
					// Under maxLen, all should succeed.
					t.Errorf("producer %d: AddOne(%d) rejected", id, i)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	items := q.Swap(nil)
	if len(items) != numProducers*itemsPerProducer {
		t.Fatalf("expected %d items, got %d", numProducers*itemsPerProducer, len(items))
	}

	// Verify all items are present.
	seen := make(map[int]bool, len(items))
	for _, v := range items {
		seen[v] = true
	}
	for i := range numProducers * itemsPerProducer {
		if !seen[i] {
			t.Fatalf("missing item %d", i)
		}
	}
}

func TestEntryQueue_AddOneMixedConcurrency(t *testing.T) {
	const numProducers = 4
	const itemsPerProducer = 100
	q := NewEntryQueue[int](64, numProducers*itemsPerProducer)

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for p := range numProducers {
		go func(id int) {
			defer wg.Done()
			for i := range itemsPerProducer {
				// Alternate between Add and AddOne.
				if i%2 == 0 {
					q.Add([]int{id*itemsPerProducer + i})
				} else {
					q.AddOne(id*itemsPerProducer + i)
				}
			}
		}(p)
	}
	wg.Wait()

	var collected []int
	var buf []int
	for {
		buf = q.Swap(buf[:0])
		if len(buf) == 0 {
			break
		}
		collected = append(collected, buf...)
	}

	if len(collected) != numProducers*itemsPerProducer {
		t.Fatalf("expected %d total items, got %d",
			numProducers*itemsPerProducer, len(collected))
	}

	// Verify all items are present.
	seen := make(map[int]bool, len(collected))
	for _, v := range collected {
		seen[v] = true
	}
	for i := range numProducers * itemsPerProducer {
		if !seen[i] {
			t.Fatalf("missing item %d", i)
		}
	}
}

func TestEntryQueue_AddOneFillToExactCapacity(t *testing.T) {
	q := NewEntryQueue[int](4, 5)
	// Add exactly 5 items using AddOne.
	for i := range 5 {
		ok := q.AddOne(i)
		if !ok {
			t.Fatalf("AddOne(%d) should succeed, at Len=%d", i, q.Len())
		}
	}
	if q.Len() != 5 {
		t.Fatalf("expected Len() 5, got %d", q.Len())
	}
	// Sixth item should be rejected.
	ok := q.AddOne(5)
	if ok {
		t.Fatal("AddOne should reject when at exact maxLen")
	}
	items := q.Swap(nil)
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
	for i, v := range items {
		if v != i {
			t.Fatalf("items[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestEntryQueue_AddOneAfterSwap(t *testing.T) {
	q := NewEntryQueue[int](4, 256)
	ok := q.AddOne(1)
	if !ok {
		t.Fatal("first AddOne should succeed")
	}
	_ = q.Swap(nil)
	if q.Len() != 0 {
		t.Fatalf("Len should be 0 after swap, got %d", q.Len())
	}
	// Add after swap should succeed.
	ok = q.AddOne(2)
	if !ok {
		t.Fatal("AddOne after swap should succeed")
	}
	items := q.Swap(nil)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0] != 2 {
		t.Fatalf("item = %d, want 2", items[0])
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
