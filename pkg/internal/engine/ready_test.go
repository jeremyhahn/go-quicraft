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

package engine

import (
	"sync"
	"testing"
)

func TestReadyMap_SetAndSwap(t *testing.T) {
	rm := NewReadyMap()
	rm.Set(7)
	rm.Set(13)
	rm.Set(42)

	got := rm.Swap()
	if len(got) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(got))
	}
	for _, id := range []uint64{7, 13, 42} {
		if _, ok := got[id]; !ok {
			t.Fatalf("expected shard %d in result", id)
		}
	}
}

func TestReadyMap_SwapEmpty(t *testing.T) {
	rm := NewReadyMap()
	got := rm.Swap()
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(got))
	}
}

func TestReadyMap_DuplicateSetDeduplicated(t *testing.T) {
	rm := NewReadyMap()
	rm.Set(42)
	rm.Set(42)
	rm.Set(42)

	got := rm.Swap()
	if len(got) != 1 {
		t.Fatalf("expected 1 shard (deduplicated), got %d", len(got))
	}
	if _, ok := got[42]; !ok {
		t.Fatal("expected shard 42 in result")
	}
}

func TestReadyMap_SetAcrossDifferentBuckets(t *testing.T) {
	rm := NewReadyMap()

	// These shard IDs map to different buckets: id % 64.
	shards := []uint64{0, 1, 63, 64, 127, 128, 1000, 9999}
	for _, id := range shards {
		rm.Set(id)
	}

	got := rm.Swap()
	if len(got) != len(shards) {
		t.Fatalf("expected %d shards, got %d", len(shards), len(got))
	}
	for _, id := range shards {
		if _, ok := got[id]; !ok {
			t.Fatalf("expected shard %d in result", id)
		}
	}
}

func TestReadyMap_SwapResetsActiveSet(t *testing.T) {
	rm := NewReadyMap()
	rm.Set(10)
	rm.Set(20)

	first := rm.Swap()
	if len(first) != 2 {
		t.Fatalf("first swap: expected 2 shards, got %d", len(first))
	}

	second := rm.Swap()
	if len(second) != 0 {
		t.Fatalf("second swap: expected 0 shards, got %d", len(second))
	}
}

func TestReadyMap_ConcurrentSetAndSwap(t *testing.T) {
	rm := NewReadyMap()
	const numWriters = 100
	const shardsPerWriter = 50
	var wg sync.WaitGroup

	// Launch writers that set unique shard IDs.
	wg.Add(numWriters)
	for w := range numWriters {
		go func(base int) {
			defer wg.Done()
			for i := range shardsPerWriter {
				rm.Set(uint64(base*shardsPerWriter + i))
			}
		}(w)
	}
	wg.Wait()

	got := rm.Swap()
	expected := numWriters * shardsPerWriter
	if len(got) != expected {
		t.Fatalf("expected %d shards, got %d", expected, len(got))
	}
}

func TestReadyMap_HighShardCount(t *testing.T) {
	rm := NewReadyMap()
	const n = 10_000

	for i := range uint64(n) {
		rm.Set(i)
	}

	got := rm.Swap()
	if len(got) != n {
		t.Fatalf("expected %d shards, got %d", n, len(got))
	}

	// Verify all shard IDs are present.
	for i := range uint64(n) {
		if _, ok := got[i]; !ok {
			t.Fatalf("missing shard %d in result", i)
		}
	}
}

func TestReadyMap_DoubleBufferSwapAlternates(t *testing.T) {
	rm := NewReadyMap()

	// First cycle: set and swap.
	rm.Set(1)
	first := rm.Swap()
	if _, ok := first[1]; !ok {
		t.Fatal("first swap: expected shard 1")
	}

	// Second cycle: set different shards and swap.
	rm.Set(2)
	rm.Set(3)
	second := rm.Swap()
	if len(second) != 2 {
		t.Fatalf("second swap: expected 2 shards, got %d", len(second))
	}
	if _, ok := second[2]; !ok {
		t.Fatal("second swap: expected shard 2")
	}
	if _, ok := second[3]; !ok {
		t.Fatal("second swap: expected shard 3")
	}

	// Third cycle: no sets, should be empty.
	third := rm.Swap()
	if len(third) != 0 {
		t.Fatalf("third swap: expected 0 shards, got %d", len(third))
	}
}

func TestReadyMap_SetAfterSwapGoesToNewActive(t *testing.T) {
	rm := NewReadyMap()

	rm.Set(100)
	got := rm.Swap()
	if _, ok := got[100]; !ok {
		t.Fatal("expected shard 100 in first swap")
	}

	// Set new shard after swap; it should appear in next swap.
	rm.Set(200)
	got2 := rm.Swap()
	if _, ok := got2[200]; !ok {
		t.Fatal("expected shard 200 in second swap")
	}
	if _, ok := got2[100]; ok {
		t.Fatal("shard 100 should not appear in second swap")
	}
}

func TestReadyMap_ConcurrentSetWithSwap(t *testing.T) {
	rm := NewReadyMap()
	const iterations = 100
	const writersPerIteration = 10
	const shardsPerWriter = 100

	// Run concurrent writers while swapping periodically.
	// All shards must eventually appear in some swap result.
	collected := make(map[uint64]struct{})
	var mu sync.Mutex

	var wg sync.WaitGroup

	// Continuous writers.
	wg.Add(writersPerIteration)
	for w := range writersPerIteration {
		go func(base int) {
			defer wg.Done()
			for i := range shardsPerWriter {
				rm.Set(uint64(base*shardsPerWriter + i))
			}
		}(w)
	}

	wg.Wait()

	// Drain everything via swap.
	for range iterations {
		result := rm.Swap()
		mu.Lock()
		for id := range result {
			collected[id] = struct{}{}
		}
		mu.Unlock()
	}

	expected := writersPerIteration * shardsPerWriter
	if len(collected) != expected {
		t.Fatalf("expected %d unique shards, got %d", expected, len(collected))
	}
}

func BenchmarkReadyMap_Set(b *testing.B) {
	rm := NewReadyMap()
	b.ResetTimer()
	for i := range b.N {
		rm.Set(uint64(i))
	}
}

func BenchmarkReadyMap_SetAndSwap(b *testing.B) {
	rm := NewReadyMap()
	b.ResetTimer()
	for i := range b.N {
		rm.Set(uint64(i % 1000))
		if i%100 == 99 {
			rm.Swap()
		}
	}
}

func BenchmarkReadyMap_ConcurrentSet(b *testing.B) {
	rm := NewReadyMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := uint64(0)
		for pb.Next() {
			rm.Set(id)
			id++
		}
	})
}

// BenchmarkReadyMap_SwapEmpty measures Swap() performance when the map
// is empty. This is the critical hot path that was previously O(64 locks)
// and is now O(1).
func BenchmarkReadyMap_SwapEmpty(b *testing.B) {
	rm := NewReadyMap()
	b.ResetTimer()
	for range b.N {
		rm.Swap()
	}
}

// BenchmarkReadyMap_SwapWithData measures Swap() with some data present.
func BenchmarkReadyMap_SwapWithData(b *testing.B) {
	rm := NewReadyMap()
	// Pre-populate with some shards.
	for i := range 100 {
		rm.Set(uint64(i))
	}
	b.ResetTimer()
	for range b.N {
		// Re-add data for next iteration.
		for i := range 100 {
			rm.Set(uint64(i))
		}
		rm.Swap()
	}
}
