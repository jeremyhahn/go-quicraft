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

// EntryAppliedFunc reports whether a committed entry actually took effect on
// the inner state machine, given the command bytes and the Result the state
// machine produced for it. Returning false suppresses caching for that entry.
//
// It takes the command as well as the Result because a result code alone is
// often ambiguous: a conditional write that was declined and an unconditional
// write that happened to return the same code are indistinguishable from the
// Result. The command says which operation was asked for.
type EntryAppliedFunc func(cmd []byte, result Result) bool

// CacheConfig configures the write-through LRU cache for a
// CachedDiskStateMachine.
//
// # Conditional writes
//
// The cache is populated from the value ExtractKV pulls out of the COMMAND,
// not from the store. For an unconditional write those agree. For a
// CONDITIONAL write — compare-and-set, put-if-absent, delete-if-match — they
// do not: the command carries a value the store may have declined to take,
// and caching it would let a later Lookup return a value the store never held.
//
// A state machine with conditional operations MUST therefore set EntryApplied.
// Nothing here can infer the answer, because the outcome encoding is the state
// machine's own (`kv.OpPutIfAbsent`, for instance, reports its verdict as
// `kv.ResultStored` vs `kv.ResultNotStored`, both successful applications).
type CacheConfig struct {
	// MaxEntries is the maximum number of entries in the LRU cache.
	// When exceeded, the least recently used entry is evicted.
	// Must be > 0.
	MaxEntries int

	// ExtractKV extracts cache key/value from entry Cmd bytes.
	// Required -- without this the cache cannot know what to store.
	ExtractKV EntryKeyFunc

	// EntryApplied reports whether an entry took effect, and is consulted
	// before the extracted value is cached. Optional: nil caches every entry
	// whose result is not the reserved ResultInvalidCommand, which is correct
	// only when every operation ExtractKV names is unconditional.
	//
	// ResultInvalidCommand is excluded whether or not this is set. It is a
	// reserved sentinel meaning the payload could not be decoded and the store
	// was left unchanged, so caching such an entry is never correct and a
	// predicate cannot opt back into it.
	EntryApplied EntryAppliedFunc
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
	inner        DiskStateMachine
	cache        *lruCache
	extractKV    EntryKeyFunc
	entryApplied EntryAppliedFunc
}

// NewCachedDiskStateMachine creates a CachedDiskStateMachine wrapping the
// given DiskStateMachine.
func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &CachedDiskStateMachine{
		inner:        inner,
		cache:        newLRUCache(cfg.MaxEntries),
		extractKV:    cfg.ExtractKV,
		entryApplied: cfg.EntryApplied,
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

// Update invalidates the affected cache keys, applies entries to the inner
// SM, then repopulates the cache.
//
// The invalidation MUST happen before the inner write: if the cache were
// only repopulated afterwards, a Lookup racing with Update could return the
// stale pre-write value from the cache while the write is already durable
// in the inner SM. Deleting first degrades that window to a cache miss that
// falls through to the (already or not-yet updated, but never stale-cached)
// inner SM.
func (c *CachedDiskStateMachine) Update(ctx context.Context, entries []Entry, results []Result) error {
	// Extract once; the (key, value) pairs alias e.Cmd, which stays valid
	// until this Update returns (the engine recycles Cmd buffers only after
	// that, see ZeroCopyEntryCmd).
	type kvPair struct {
		key   string
		value []byte
		idx   int // index into entries/results for this pair
	}
	pairs := make([]kvPair, 0, len(entries))
	for i := range entries {
		key, value := c.extractKV(entries[i].Cmd)
		if key != "" {
			pairs = append(pairs, kvPair{key: key, value: value, idx: i})
			c.cache.Delete(key)
		}
	}

	if err := c.inner.Update(ctx, entries, results); err != nil {
		// The affected keys stay invalidated; subsequent lookups fall
		// through to the inner SM, which is the source of truth.
		return err
	}

	for _, p := range pairs {
		// Do not cache the new value for an entry that did not take effect --
		// a rejected payload, or a conditional write the store declined. In
		// either case the store state was left unchanged, so caching the
		// would-be value would let Lookup return a value the store never held.
		// The key was already invalidated above, so a later Lookup correctly
		// falls through to the inner SM.
		if !c.applied(entries[p.idx].Cmd, results[p.idx]) {
			continue
		}
		value := p.value
		// Copy the value before caching. extractKV may return a sub-slice
		// of e.Cmd, which under ZeroCopyEntryCmd aliases an internal buffer
		// the engine recycles after Update returns. Caching the alias would
		// let a later write corrupt previously-cached reads. A nil value is
		// preserved as nil (it signals a delete to the cache).
		if value != nil {
			cached := make([]byte, len(value))
			copy(cached, value)
			value = cached
		}
		c.cache.Put(p.key, value)
	}
	return nil
}

// applied reports whether an entry took effect and its extracted value may
// therefore be cached. ResultInvalidCommand is refused unconditionally: it is
// the reserved sentinel for a payload that could not be decoded, so the store
// was left unchanged no matter what a configured predicate believes.
func (c *CachedDiskStateMachine) applied(cmd []byte, result Result) bool {
	if result.Value == ResultInvalidCommand {
		return false
	}
	if c.entryApplied == nil {
		return true
	}
	return c.entryApplied(cmd, result)
}

// Lookup checks the cache first, then falls back to the inner SM on miss.
// Cache hits return an owned copy: handing out the live cache buffer would
// let a caller that mutates or pools the returned slice corrupt the cached
// value for every future reader.
func (c *CachedDiskStateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	if key := extractQueryKey(query); key != "" {
		if val, hit := c.cache.Get(key); hit {
			return copyValue(val), nil
		}
	}
	return c.inner.Lookup(ctx, query)
}

// BytesLookup provides the NALookup interface for cached reads. Cache hits
// return an owned copy (see Lookup).
func (c *CachedDiskStateMachine) BytesLookup(key []byte) ([]byte, error) {
	if val, hit := c.cache.Get(string(key)); hit {
		return copyValue(val), nil
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
	// The inner state machine answered with something this byte-oriented path
	// cannot represent. Returning (nil, nil) reported that as "supported, and
	// the value is empty", which is indistinguishable from a genuine empty
	// value and from a missing key — so a caller reading through the
	// no-allocation path silently saw nothing where the ordinary Lookup would
	// have handed back a real result. Report the type instead: it is a
	// programming error in the state machine, and naming what came back is the
	// difference between a one-line fix and an unexplained empty read.
	return nil, unsupportedLookupResult(result)
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

// copyValue returns an owned copy of a cached value, preserving nil.
func copyValue(val []byte) []byte {
	if val == nil {
		return nil
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp
}

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
