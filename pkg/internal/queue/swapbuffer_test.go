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

func TestSwapBuffer_AddAndSwap(t *testing.T) {
	sb := NewSwapBuffer[int](8, 0)
	sb.Add(42)
	items := sb.Swap()
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0] != 42 {
		t.Fatalf("expected 42, got %d", items[0])
	}
}

func TestSwapBuffer_SwapEmpty(t *testing.T) {
	sb := NewSwapBuffer[string](4, 0)
	items := sb.Swap()
	if items != nil {
		t.Fatalf("expected nil from empty swap, got %v", items)
	}
}

func TestSwapBuffer_MultipleAddThenSwap(t *testing.T) {
	sb := NewSwapBuffer[int](4, 0)
	for i := range 5 {
		sb.Add(i)
	}
	items := sb.Swap()
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
	for i, v := range items {
		if v != i {
			t.Fatalf("items[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestSwapBuffer_SwapClearsBuffer(t *testing.T) {
	sb := NewSwapBuffer[int](4, 0)
	sb.Add(1)
	sb.Add(2)
	first := sb.Swap()
	if len(first) != 2 {
		t.Fatalf("first swap: expected 2 items, got %d", len(first))
	}
	second := sb.Swap()
	if second != nil {
		t.Fatalf("second swap: expected nil, got %v", second)
	}
}

func TestSwapBuffer_ConcurrentProducerConsumer(t *testing.T) {
	sb := NewSwapBuffer[int](64, 0)
	const n = 1000
	var wg sync.WaitGroup

	// Single producer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range n {
			sb.Add(i)
		}
	}()

	// Wait for producer to finish, then drain.
	wg.Wait()

	var collected []int
	for {
		items := sb.Swap()
		if items == nil {
			break
		}
		collected = append(collected, items...)
	}

	if len(collected) != n {
		t.Fatalf("expected %d total items, got %d", n, len(collected))
	}
}

func TestSwapBuffer_Len(t *testing.T) {
	sb := NewSwapBuffer[int](4, 0)
	if sb.Len() != 0 {
		t.Fatalf("expected Len() 0, got %d", sb.Len())
	}
	sb.Add(1)
	sb.Add(2)
	sb.Add(3)
	if sb.Len() != 3 {
		t.Fatalf("expected Len() 3, got %d", sb.Len())
	}
	sb.Swap()
	if sb.Len() != 0 {
		t.Fatalf("expected Len() 0 after swap, got %d", sb.Len())
	}
}

func TestSwapBuffer_GrowsBeyondInitialCapacity(t *testing.T) {
	sb := NewSwapBuffer[int](2, 0)
	for i := range 100 {
		sb.Add(i)
	}
	items := sb.Swap()
	if len(items) != 100 {
		t.Fatalf("expected 100 items, got %d", len(items))
	}
}

func TestSwapBuffer_BufferReuse(t *testing.T) {
	sb := NewSwapBuffer[int](8, 0)

	// First cycle: populate and swap.
	for i := range 5 {
		sb.Add(i)
	}
	first := sb.Swap()
	if len(first) != 5 {
		t.Fatalf("first swap: expected 5 items, got %d", len(first))
	}

	// Second cycle: populate and swap. The write buffer should reuse
	// the backing array from the first swap's result.
	for i := range 3 {
		sb.Add(i + 100)
	}
	second := sb.Swap()
	if len(second) != 3 {
		t.Fatalf("second swap: expected 3 items, got %d", len(second))
	}
	for i, v := range second {
		if v != i+100 {
			t.Fatalf("second[%d] = %d, want %d", i, v, i+100)
		}
	}

	// Third cycle: verify data integrity after multiple reuses.
	sb.Add(999)
	third := sb.Swap()
	if len(third) != 1 || third[0] != 999 {
		t.Fatalf("third swap: expected [999], got %v", third)
	}
}

func TestSwapBuffer_BufferReuse_NoDataLeak(t *testing.T) {
	// Ensure recycled buffers do not leak data from prior cycles.
	sb := NewSwapBuffer[int](4, 0)

	sb.Add(1)
	sb.Add(2)
	sb.Add(3)
	sb.Swap() // first: returns [1,2,3]

	sb.Add(10)
	second := sb.Swap()
	if len(second) != 1 {
		t.Fatalf("expected 1 item, got %d", len(second))
	}
	if second[0] != 10 {
		t.Fatalf("expected 10, got %d", second[0])
	}
}

func TestSwapBuffer_BackpressureAtCapacity(t *testing.T) {
	sb := NewSwapBuffer[int](4, 3) // maxLen = 3
	if ok := sb.Add(1); !ok {
		t.Fatal("first Add should succeed")
	}
	if ok := sb.Add(2); !ok {
		t.Fatal("second Add should succeed")
	}
	if ok := sb.Add(3); !ok {
		t.Fatal("third Add should succeed")
	}
	// At capacity, next Add should return false.
	if ok := sb.Add(4); ok {
		t.Fatal("Add at capacity should return false")
	}
	// Len should be 3, not 4.
	if sb.Len() != 3 {
		t.Fatalf("expected Len() 3, got %d", sb.Len())
	}
}

func TestSwapBuffer_SwapDrainsAllowsNewAdds(t *testing.T) {
	sb := NewSwapBuffer[int](4, 2) // maxLen = 2
	sb.Add(1)
	sb.Add(2)

	// At capacity.
	if ok := sb.Add(3); ok {
		t.Fatal("Add at capacity should return false")
	}

	// Swap drains the write buffer.
	items := sb.Swap()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	// Now Add should succeed again.
	if ok := sb.Add(10); !ok {
		t.Fatal("Add after Swap should succeed")
	}
	if ok := sb.Add(20); !ok {
		t.Fatal("second Add after Swap should succeed")
	}

	items = sb.Swap()
	if len(items) != 2 || items[0] != 10 || items[1] != 20 {
		t.Fatalf("expected [10, 20], got %v", items)
	}
}

func TestSwapBuffer_UnboundedWhenMaxLenZero(t *testing.T) {
	sb := NewSwapBuffer[int](2, 0) // maxLen = 0, unbounded
	for i := range 1000 {
		if ok := sb.Add(i); !ok {
			t.Fatalf("Add %d should succeed when maxLen=0", i)
		}
	}
	items := sb.Swap()
	if len(items) != 1000 {
		t.Fatalf("expected 1000 items, got %d", len(items))
	}
}

func BenchmarkSwapBuffer_AddAndSwap(b *testing.B) {
	sb := NewSwapBuffer[int](64, 0)
	b.ResetTimer()
	for i := range b.N {
		sb.Add(i)
		if i%64 == 63 {
			sb.Swap()
		}
	}
}

func BenchmarkSwapBuffer_SteadyState(b *testing.B) {
	sb := NewSwapBuffer[int](64, 0)
	// Prime the buffer to establish steady-state recycling.
	for i := range 64 {
		sb.Add(i)
	}
	sb.Swap()
	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		sb.Add(i)
		if i%64 == 63 {
			sb.Swap()
		}
	}
}
