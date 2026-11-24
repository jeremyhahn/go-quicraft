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
	"sync"
	"sync/atomic"
)

// lruEntry is a node in the doubly-linked list maintained by lruCache.
type lruEntry struct {
	key        string
	value      []byte
	prev, next *lruEntry
}

// lruCache is a concurrency-safe LRU cache using a map and doubly-linked list.
// The head is the most recently used entry and the tail is the least recently
// used. The RWMutex allows concurrent reads (Get) while serializing writes
// (Put/Delete/Clear). Atomic hit/miss counters avoid lock contention on the
// read path for metrics.
type lruCache struct {
	maxEntries int
	mu         sync.RWMutex
	items      map[string]*lruEntry
	head, tail *lruEntry
	hits       atomic.Uint64
	misses     atomic.Uint64
}

// newLRUCache creates an LRU cache with the given capacity.
func newLRUCache(maxEntries int) *lruCache {
	return &lruCache{
		maxEntries: maxEntries,
		items:      make(map[string]*lruEntry, maxEntries),
	}
}

// Get retrieves a value from the cache. Returns the value and true on hit,
// or nil and false on miss. On hit the entry is promoted to head (MRU).
// Promotion requires a write lock upgrade, so Get acquires a full lock
// rather than a read lock to avoid a TOCTOU race between the map lookup
// and the list manipulation.
func (c *lruCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	entry, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return nil, false
	}
	c.moveToHead(entry)
	val := entry.value
	c.mu.Unlock()
	c.hits.Add(1)
	return val, true
}

// Put inserts or updates a key-value pair. If value is nil the key is
// removed from the cache (delete semantics). When the cache exceeds
// maxEntries the least recently used entry is evicted.
func (c *lruCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Nil value means delete.
	if value == nil {
		if entry, ok := c.items[key]; ok {
			c.removeEntry(entry)
			delete(c.items, key)
		}
		return
	}

	// Update existing entry.
	if entry, ok := c.items[key]; ok {
		entry.value = value
		c.moveToHead(entry)
		return
	}

	// Insert new entry.
	entry := &lruEntry{key: key, value: value}
	c.items[key] = entry
	c.pushHead(entry)

	// Evict LRU if over capacity.
	if len(c.items) > c.maxEntries {
		c.evictTail()
	}
}

// Delete removes a key from the cache.
func (c *lruCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return
	}
	c.removeEntry(entry)
	delete(c.items, key)
}

// Clear removes all entries from the cache.
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*lruEntry, c.maxEntries)
	c.head = nil
	c.tail = nil
}

// Len returns the current number of entries.
func (c *lruCache) Len() int {
	c.mu.RLock()
	n := len(c.items)
	c.mu.RUnlock()
	return n
}

// Stats returns the cumulative hit and miss counts.
func (c *lruCache) Stats() (hits, misses uint64) {
	return c.hits.Load(), c.misses.Load()
}

// moveToHead moves an existing entry to the head of the list.
// Caller must hold c.mu (write lock).
func (c *lruCache) moveToHead(entry *lruEntry) {
	if c.head == entry {
		return
	}
	c.removeEntry(entry)
	c.pushHead(entry)
}

// pushHead inserts an entry at the head of the list.
// Caller must hold c.mu (write lock).
func (c *lruCache) pushHead(entry *lruEntry) {
	entry.prev = nil
	entry.next = c.head
	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry
	if c.tail == nil {
		c.tail = entry
	}
}

// removeEntry unlinks an entry from the doubly-linked list.
// Caller must hold c.mu (write lock).
func (c *lruCache) removeEntry(entry *lruEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
	entry.prev = nil
	entry.next = nil
}

// evictTail removes the tail (LRU) entry from the cache.
// Caller must hold c.mu (write lock).
func (c *lruCache) evictTail() {
	if c.tail == nil {
		return
	}
	evicted := c.tail
	c.removeEntry(evicted)
	delete(c.items, evicted.key)
}
