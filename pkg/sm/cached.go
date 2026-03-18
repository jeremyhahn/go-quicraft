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
	"io"
)

// EntryKeyFunc extracts a cache key and value from a committed entry's Cmd.
// Return ("", nil) to skip caching the entry (e.g., for non-cacheable ops).
// The key is a string for map compatibility; the value is the raw bytes to
// cache. Returning a non-empty key with a nil value signals a cache delete.
type EntryKeyFunc func(cmd []byte) (key string, value []byte)

// CacheConfig configures the write-through LRU cache for a
// CachedDiskStateMachine.
type CacheConfig struct {
	// MaxEntries is the maximum number of entries in the LRU cache.
	// When exceeded, the least recently used entry is evicted.
	// Must be > 0.
	MaxEntries int

	// ExtractKV extracts cache key/value from entry Cmd bytes.
	// Required -- without this the cache cannot know what to store.
	ExtractKV EntryKeyFunc
}

// validate checks that the CacheConfig is valid.
func (cfg *CacheConfig) validate() error {
	if cfg.MaxEntries <= 0 {
		return &CacheMaxEntriesError{Value: cfg.MaxEntries}
	}
	if cfg.ExtractKV == nil {
		return &InvalidCacheConfigError{Reason: "ExtractKV must not be nil"}
	}
	return nil
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine wraps a DiskStateMachine with a write-through LRU
// cache. In-memory StateMachine and ConcurrentStateMachine do not benefit
// from caching because their state is already resident in memory. Disk state
// machines, however, incur I/O on every Lookup, making a cache layer valuable.
// ---------------------------------------------------------------------------

// CachedDiskStateMachine wraps a DiskStateMachine with a write-through
// LRU cache. The cache sits in front of disk reads, converting expensive
// disk I/O into memory lookups on cache hit.
type CachedDiskStateMachine struct {
	inner     DiskStateMachine
	cache     *lruCache
	extractKV EntryKeyFunc
}

// NewCachedDiskStateMachine creates a CachedDiskStateMachine wrapping the
// given DiskStateMachine.
func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &CachedDiskStateMachine{
		inner:     inner,
		cache:     newLRUCache(cfg.MaxEntries),
		extractKV: cfg.ExtractKV,
	}, nil
}

// Open delegates to the inner SM and clears the cache. The cache starts
// empty after Open because the inner SM's prior state is unknown until
// entries are applied or lookups populate it.
func (c *CachedDiskStateMachine) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
	lastApplied, err := c.inner.Open(ctx, initialDir, stopper)
	if err != nil {
		return 0, err
	}
	c.cache.Clear()
	return lastApplied, nil
}

// Update applies entries to the inner SM, then updates the cache.
func (c *CachedDiskStateMachine) Update(ctx context.Context, entries []Entry, results []Result) error {
	if err := c.inner.Update(ctx, entries, results); err != nil {
		return err
	}
	for _, e := range entries {
		key, value := c.extractKV(e.Cmd)
		if key != "" {
			c.cache.Put(key, value)
		}
	}
	return nil
}

// Lookup checks the cache first, then falls back to the inner SM on miss.
func (c *CachedDiskStateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	if key := extractQueryKey(query); key != "" {
		if val, hit := c.cache.Get(key); hit {
			return val, nil
		}
	}
	return c.inner.Lookup(ctx, query)
}

// BytesLookup provides the NALookup interface for zero-allocation cached reads.
func (c *CachedDiskStateMachine) BytesLookup(key []byte) ([]byte, error) {
	if val, hit := c.cache.Get(string(key)); hit {
		return val, nil
	}
	if na, ok := c.inner.(NALookup); ok {
		return na.BytesLookup(key)
	}
	result, err := c.inner.Lookup(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	if b, ok := result.([]byte); ok {
		return b, nil
	}
	return nil, nil
}

// GetHash delegates to the inner SM if it implements Hasher.
func (c *CachedDiskStateMachine) GetHash() (uint64, error) {
	if h, ok := c.inner.(Hasher); ok {
		return h.GetHash()
	}
	return 0, &InvalidCacheConfigError{Reason: "inner DiskStateMachine does not implement Hasher"}
}

// Sync delegates to the inner SM.
func (c *CachedDiskStateMachine) Sync() error {
	return c.inner.Sync()
}

// PrepareSnapshot delegates to the inner SM.
func (c *CachedDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	return c.inner.PrepareSnapshot()
}

// SaveSnapshot delegates to the inner SM.
func (c *CachedDiskStateMachine) SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	return c.inner.SaveSnapshot(ctx, snapshotCtx, w, stopper)
}

// RecoverFromSnapshot restores the inner SM state and clears the cache.
func (c *CachedDiskStateMachine) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	if err := c.inner.RecoverFromSnapshot(ctx, r, stopper); err != nil {
		return err
	}
	c.cache.Clear()
	return nil
}

// Close releases resources of both the cache and the inner SM.
func (c *CachedDiskStateMachine) Close(ctx context.Context) error {
	c.cache.Clear()
	return c.inner.Close(ctx)
}

// CacheStats returns cumulative cache hit and miss counts.
func (c *CachedDiskStateMachine) CacheStats() (hits, misses uint64) {
	return c.cache.Stats()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// extractQueryKey attempts to extract a string cache key from a query value.
// Supports string and []byte query types. Returns "" if the query type is
// not cacheable (e.g., struct-based queries for list operations).
func extractQueryKey(query interface{}) string {
	switch q := query.(type) {
	case string:
		return q
	case []byte:
		return string(q)
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// Compile-time interface assertions
// ---------------------------------------------------------------------------

var (
	_ DiskStateMachine = (*CachedDiskStateMachine)(nil)
	_ NALookup         = (*CachedDiskStateMachine)(nil)
	_ Hasher           = (*CachedDiskStateMachine)(nil)
)
