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

package sm

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
)

// lruOrder returns the cache's keys from most to least recently used,
// walking the list without promoting any entry or touching the hit/miss
// counters. It fails the test when the forward walk, the backward walk and
// the map disagree.
func lruOrder(ctx context.Context, t *testing.T, c *lruCache) []string {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	var fwd []string
	var prev *lruEntry
	for e := c.head; e != nil; e = e.next {
		if e.prev != prev {
			t.Fatalf("entry %q: prev link broken", e.key)
		}
		fwd = append(fwd, e.key)
		prev = e
		if len(fwd) > len(c.items)+1 {
			t.Fatalf("list longer than map (%d entries): cycle?", len(c.items))
		}
	}
	if c.tail != prev {
		t.Fatalf("tail does not match last list entry")
	}
	var back []string
	for e := c.tail; e != nil; e = e.prev {
		back = append(back, e.key)
		if len(back) > len(c.items)+1 {
			t.Fatalf("backward walk longer than map: cycle?")
		}
	}
	slices.Reverse(back)
	if !slices.Equal(fwd, back) {
		t.Fatalf("forward walk %v != backward walk %v", fwd, back)
	}
	if len(fwd) != len(c.items) {
		t.Fatalf("list has %d entries, map has %d", len(fwd), len(c.items))
	}
	for _, k := range fwd {
		if e, ok := c.items[k]; !ok || e.key != k {
			t.Fatalf("list key %q not in map", k)
		}
	}
	return fwd
}

// assertLRUOrder fails the test unless the cache's recency order, most
// recent first, equals want.
func assertLRUOrder(ctx context.Context, t *testing.T, c *lruCache, want ...string) {
	t.Helper()
	got := lruOrder(ctx, t, c)
	if want == nil {
		want = []string{}
	}
	if got == nil {
		got = []string{}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("recency order = %v, want %v", got, want)
	}
}

// assertLRUStats fails the test unless Stats returns the given counts.
func assertLRUStats(ctx context.Context, t *testing.T, c *lruCache, hits, misses uint64) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	h, m := c.Stats()
	if h != hits || m != misses {
		t.Fatalf("Stats() = (%d, %d), want (%d, %d)", h, m, hits, misses)
	}
}

// fillLRU puts each key with a value equal to the key, in order.
func fillLRU(ctx context.Context, t *testing.T, c *lruCache, keys ...string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	for _, k := range keys {
		c.Put(k, []byte(k))
	}
}

func TestLRUCacheDeleteClearStats_DeleteMiddleKeepsOrder(t *testing.T) {
	ctx := context.Background()
	c := newLRUCache(4)
	fillLRU(ctx, t, c, "a", "b", "c", "d")
	assertLRUOrder(ctx, t, c, "d", "c", "b", "a")

	c.Delete("b")
	if c.Len() != 3 {
		t.Fatalf("Len after Delete = %d, want 3", c.Len())
	}
	assertLRUOrder(ctx, t, c, "d", "c", "a")

	// Room for one more without eviction, then each Put evicts the LRU.
	fillLRU(ctx, t, c, "e")
	assertLRUOrder(ctx, t, c, "e", "d", "c", "a")
	fillLRU(ctx, t, c, "f")
	assertLRUOrder(ctx, t, c, "f", "e", "d", "c")
	fillLRU(ctx, t, c, "g")
	assertLRUOrder(ctx, t, c, "g", "f", "e", "d")
	assertLRUStats(ctx, t, c, 0, 0)

	if v, ok := c.Get("b"); ok || v != nil {
		t.Fatalf("Get(deleted b) = (%q, %v), want (nil, false)", v, ok)
	}
}

func TestLRUCacheDeleteClearStats_DeleteMissingChangesNothing(t *testing.T) {
	ctx := context.Background()
	c := newLRUCache(3)
	fillLRU(ctx, t, c, "a", "b", "c")
	c.Delete("missing")
	if c.Len() != 3 {
		t.Fatalf("Len after Delete(missing) = %d, want 3", c.Len())
	}
	assertLRUOrder(ctx, t, c, "c", "b", "a")
	assertLRUStats(ctx, t, c, 0, 0)

	// Deleting twice is a no-op the second time.
	c.Delete("b")
	c.Delete("b")
	assertLRUOrder(ctx, t, c, "c", "a")

	// Delete on an empty cache.
	e := newLRUCache(2)
	e.Delete("x")
	assertLRUOrder(ctx, t, e)
	if e.Len() != 0 {
		t.Fatalf("Len of empty cache after Delete = %d", e.Len())
	}
}

func TestLRUCacheDeleteClearStats_DeleteHeadAndTail(t *testing.T) {
	ctx := context.Background()
	c := newLRUCache(4)
	fillLRU(ctx, t, c, "a", "b", "c", "d")

	c.Delete("d") // head (MRU)
	assertLRUOrder(ctx, t, c, "c", "b", "a")
	c.Delete("a") // tail (LRU)
	assertLRUOrder(ctx, t, c, "c", "b")

	fillLRU(ctx, t, c, "e", "f")
	assertLRUOrder(ctx, t, c, "f", "e", "c", "b")
	fillLRU(ctx, t, c, "g")
	assertLRUOrder(ctx, t, c, "g", "f", "e", "c")
	fillLRU(ctx, t, c, "h")
	assertLRUOrder(ctx, t, c, "h", "g", "f", "e")

	// Delete down to the last entry, which is head and tail at once.
	c.Delete("h")
	c.Delete("e")
	c.Delete("f")
	assertLRUOrder(ctx, t, c, "g")
	c.Delete("g")
	assertLRUOrder(ctx, t, c)
	fillLRU(ctx, t, c, "1", "2", "3", "4", "5")
	assertLRUOrder(ctx, t, c, "5", "4", "3", "2")

	// Nil Put deletes the head and the tail the same way.
	c.Put("5", nil)
	c.Put("2", nil)
	assertLRUOrder(ctx, t, c, "4", "3")
}

func TestLRUCacheDeleteClearStats_ClearEmptiesCache(t *testing.T) {
	ctx := context.Background()
	c := newLRUCache(3)
	fillLRU(ctx, t, c, "a", "b", "c")
	c.Clear()
	if c.Len() != 0 {
		t.Fatalf("Len after Clear = %d, want 0", c.Len())
	}
	assertLRUOrder(ctx, t, c)
	for _, k := range []string{"a", "b", "c"} {
		if v, ok := c.Get(k); ok || v != nil {
			t.Fatalf("Get(%q) after Clear = (%q, %v), want miss", k, v, ok)
		}
	}
	assertLRUStats(ctx, t, c, 0, 3)

	// Later Puts behave as on a new cache.
	fillLRU(ctx, t, c, "x", "y", "z")
	assertLRUOrder(ctx, t, c, "z", "y", "x")
	fillLRU(ctx, t, c, "w")
	assertLRUOrder(ctx, t, c, "w", "z", "y")
	if v, ok := c.Get("y"); !ok || string(v) != "y" {
		t.Fatalf("Get(y) = (%q, %v), want (y, true)", v, ok)
	}
	assertLRUOrder(ctx, t, c, "y", "w", "z")
	fillLRU(ctx, t, c, "v")
	assertLRUOrder(ctx, t, c, "v", "y", "w")

	// Clear on an empty cache and twice in a row is safe.
	c.Clear()
	c.Clear()
	assertLRUOrder(ctx, t, c)
	c.Delete("v")
	assertLRUOrder(ctx, t, c)
}

func TestLRUCacheDeleteClearStats_StatsCountEachGetOnce(t *testing.T) {
	ctx := context.Background()
	c := newLRUCache(2)
	assertLRUStats(ctx, t, c, 0, 0)

	c.Get("a")
	assertLRUStats(ctx, t, c, 0, 1)
	fillLRU(ctx, t, c, "a")
	assertLRUStats(ctx, t, c, 0, 1)
	c.Get("a")
	assertLRUStats(ctx, t, c, 1, 1)
	c.Get("a")
	c.Get("b")
	assertLRUStats(ctx, t, c, 2, 2)

	// Put, Delete, nil Put and eviction do not count.
	fillLRU(ctx, t, c, "b", "c")
	c.Delete("c")
	c.Put("b", nil)
	c.Delete("missing")
	assertLRUStats(ctx, t, c, 2, 2)

	// An evicted key is a miss.
	fillLRU(ctx, t, c, "p", "q", "r")
	c.Get("p")
	c.Get("q")
	assertLRUStats(ctx, t, c, 3, 3)

	// Clear leaves the counters as they were.
	c.Clear()
	assertLRUStats(ctx, t, c, 3, 3)
	c.Get("q")
	assertLRUStats(ctx, t, c, 3, 4)
}

func TestLRUCacheDeleteClearStats_ConcurrentKeepsCapacity(t *testing.T) {
	ctx := context.Background()
	const capacity = 8
	const workers = 8
	const ops = 2000
	c := newLRUCache(capacity)

	var gets sync.Map // worker -> number of Gets it made
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			n := 0
			for i := range ops {
				k := fmt.Sprintf("k%d", (w*7+i)%(capacity*3))
				switch i % 7 {
				case 0, 1, 2:
					c.Put(k, []byte(k))
				case 3, 4:
					if v, ok := c.Get(k); ok && string(v) != k {
						t.Errorf("Get(%q) = %q", k, v)
					}
					n++
				case 5:
					c.Delete(k)
				case 6:
					if w == 0 && i%70 == 6 {
						c.Clear()
					} else {
						c.Put(k, nil)
					}
				}
				if l := c.Len(); l > capacity {
					t.Errorf("Len = %d > capacity %d", l, capacity)
					return
				}
			}
			gets.Store(w, n)
		}(w)
	}
	wg.Wait()

	order := lruOrder(ctx, t, c)
	if len(order) > capacity {
		t.Fatalf("final Len = %d > capacity %d", len(order), capacity)
	}
	var total uint64
	gets.Range(func(_, v any) bool {
		total += uint64(v.(int))
		return true
	})
	h, m := c.Stats()
	if h+m != total {
		t.Fatalf("hits+misses = %d, want %d Gets", h+m, total)
	}
}
