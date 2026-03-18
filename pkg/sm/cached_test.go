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
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// Test helpers: inner SM that tracks call counts
// ---------------------------------------------------------------------------

// trackingDiskStateMachine wraps testOnDiskStateMachine with tracking.
type trackingDiskStateMachine struct {
	testOnDiskStateMachine
	lookupCount atomic.Int64
	openCalled  bool
	syncCalled  bool
}

func newTrackingDiskStateMachine() *trackingDiskStateMachine {
	return &trackingDiskStateMachine{
		testOnDiskStateMachine: testOnDiskStateMachine{
			testConcurrentStateMachine: testConcurrentStateMachine{
				testStateMachine: testStateMachine{state: make(map[string][]byte)},
			},
		},
	}
}

func (t *trackingDiskStateMachine) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
	t.openCalled = true
	return t.testOnDiskStateMachine.Open(ctx, initialDir, stopper)
}

func (t *trackingDiskStateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	t.lookupCount.Add(1)
	return t.testOnDiskStateMachine.Lookup(ctx, query)
}

func (t *trackingDiskStateMachine) Sync() error {
	t.syncCalled = true
	return t.testOnDiskStateMachine.Sync()
}

// simpleExtractKV is a test EntryKeyFunc: key = Cmd as string, value = Cmd.
func simpleExtractKV(cmd []byte) (string, []byte) {
	if len(cmd) == 0 {
		return "", nil
	}
	return string(cmd), cmd
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine tests
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_OpenDelegates(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()
	stopper := make(chan struct{})

	lastApplied, err := cached.Open(ctx, "/tmp/cached-disk", stopper)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if lastApplied != 0 {
		t.Errorf("lastApplied = %d, want 0", lastApplied)
	}
	if !inner.openCalled {
		t.Error("inner Open not called")
	}
}

func TestCachedDiskStateMachine_SyncDelegates(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	if err := cached.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if !inner.syncCalled {
		t.Error("inner Sync not called")
	}
}

func TestCachedDiskStateMachine_CacheHitSkipsInnerLookup(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	entries := []Entry{{Index: 1, Cmd: []byte("disk-key")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	inner.lookupCount.Store(0)
	val, err := cached.Lookup(ctx, "disk-key")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if string(val.([]byte)) != "disk-key" {
		t.Errorf("Lookup = %q, want %q", val, "disk-key")
	}
	if inner.lookupCount.Load() != 0 {
		t.Error("expected cache hit for disk SM")
	}
}

func TestCachedDiskStateMachine_RecoverClearsCache(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()
	stopper := make(chan struct{})

	entries := []Entry{{Index: 1, Cmd: []byte("disk-stale")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	snapCtx, err := cached.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot failed: %v", err)
	}
	var buf bytes.Buffer
	if err := cached.SaveSnapshot(ctx, snapCtx, &buf, stopper); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	if err := cached.RecoverFromSnapshot(ctx, &buf, stopper); err != nil {
		t.Fatalf("RecoverFromSnapshot failed: %v", err)
	}

	inner.lookupCount.Store(0)
	if _, err := cached.Lookup(ctx, "disk-stale"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if inner.lookupCount.Load() != 1 {
		t.Error("cache should be cleared after RecoverFromSnapshot")
	}
}

func TestCachedDiskStateMachine_FullLifecycle(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()
	stopper := make(chan struct{})

	// Open
	if _, err := cached.Open(ctx, "/tmp/cached-lifecycle", stopper); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Update
	entries := []Entry{{Index: 1, Cmd: []byte("lifecycle-key")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Sync
	if err := cached.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Lookup (cache hit)
	inner.lookupCount.Store(0)
	val, err := cached.Lookup(ctx, "lifecycle-key")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if string(val.([]byte)) != "lifecycle-key" {
		t.Errorf("Lookup = %q, want %q", val, "lifecycle-key")
	}
	if inner.lookupCount.Load() != 0 {
		t.Error("expected cache hit in lifecycle test")
	}

	// Close
	if err := cached.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestCachedDiskStateMachine_InvalidConfig(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	_, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: -5,
		ExtractKV:  simpleExtractKV,
	})
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestCachedDiskStateMachine_OpenClearsCache(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()
	stopper := make(chan struct{})

	// Manually put something in cache.
	cached.cache.Put("pre-open", []byte("stale"))

	// Open should clear the cache.
	if _, err := cached.Open(ctx, "/tmp/open-clear", stopper); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if cached.cache.Len() != 0 {
		t.Errorf("cache length = %d after Open, want 0", cached.cache.Len())
	}
}

// ---------------------------------------------------------------------------
// LRU cache unit tests
// ---------------------------------------------------------------------------

func TestLRUCache_PutAndGet(t *testing.T) {
	c := newLRUCache(10)
	c.Put("k1", []byte("v1"))
	c.Put("k2", []byte("v2"))

	val, hit := c.Get("k1")
	if !hit {
		t.Fatal("expected hit for k1")
	}
	if string(val) != "v1" {
		t.Errorf("Get(k1) = %q, want %q", val, "v1")
	}

	val, hit = c.Get("k2")
	if !hit {
		t.Fatal("expected hit for k2")
	}
	if string(val) != "v2" {
		t.Errorf("Get(k2) = %q, want %q", val, "v2")
	}
}

func TestLRUCache_GetMiss(t *testing.T) {
	c := newLRUCache(10)
	val, hit := c.Get("nonexistent")
	if hit {
		t.Error("expected miss for nonexistent key")
	}
	if val != nil {
		t.Errorf("Get = %v, want nil", val)
	}
}

func TestLRUCache_EvictionOrder(t *testing.T) {
	c := newLRUCache(3)
	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	c.Put("c", []byte("3"))

	// Access "a" to make it MRU.
	c.Get("a")

	// Insert "d" -- should evict "b" (LRU).
	c.Put("d", []byte("4"))

	if _, hit := c.Get("b"); hit {
		t.Error("expected 'b' to be evicted")
	}
	if _, hit := c.Get("a"); !hit {
		t.Error("expected 'a' to survive (was accessed)")
	}
	if _, hit := c.Get("c"); !hit {
		t.Error("expected 'c' to survive")
	}
	if _, hit := c.Get("d"); !hit {
		t.Error("expected 'd' to exist")
	}
}

func TestLRUCache_UpdateExistingKey(t *testing.T) {
	c := newLRUCache(10)
	c.Put("k", []byte("old"))
	c.Put("k", []byte("new"))

	val, hit := c.Get("k")
	if !hit {
		t.Fatal("expected hit")
	}
	if string(val) != "new" {
		t.Errorf("Get(k) = %q, want %q", val, "new")
	}
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
}

func TestLRUCache_PutNilValueDeletes(t *testing.T) {
	c := newLRUCache(10)
	c.Put("k", []byte("value"))
	c.Put("k", nil)

	if _, hit := c.Get("k"); hit {
		t.Error("expected miss after nil-value put")
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0", c.Len())
	}
}

func TestLRUCache_Delete(t *testing.T) {
	c := newLRUCache(10)
	c.Put("k", []byte("value"))
	c.Delete("k")

	if _, hit := c.Get("k"); hit {
		t.Error("expected miss after Delete")
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0", c.Len())
	}
}

func TestLRUCache_DeleteNonexistent(t *testing.T) {
	c := newLRUCache(10)
	c.Delete("nonexistent") // should not panic
}

func TestLRUCache_Clear(t *testing.T) {
	c := newLRUCache(10)
	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	c.Clear()

	if c.Len() != 0 {
		t.Errorf("Len after Clear = %d, want 0", c.Len())
	}
	if _, hit := c.Get("a"); hit {
		t.Error("expected miss after Clear")
	}
}

func TestLRUCache_Stats(t *testing.T) {
	c := newLRUCache(10)
	c.Put("k", []byte("v"))

	c.Get("k")           // hit
	c.Get("nonexistent") // miss

	hits, misses := c.Stats()
	if hits != 1 {
		t.Errorf("hits = %d, want 1", hits)
	}
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
}

func TestLRUCache_SingleEntry(t *testing.T) {
	c := newLRUCache(1)
	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))

	if _, hit := c.Get("a"); hit {
		t.Error("expected 'a' evicted")
	}
	val, hit := c.Get("b")
	if !hit {
		t.Fatal("expected hit for 'b'")
	}
	if string(val) != "2" {
		t.Errorf("Get(b) = %q, want %q", val, "2")
	}
}

func TestLRUCache_ConcurrentAccess(t *testing.T) {
	c := newLRUCache(1000)
	var wg sync.WaitGroup
	n := 100

	// Concurrent writers.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := "key-" + strconv.Itoa(idx)
			c.Put(key, []byte(key))
		}(i)
	}

	// Concurrent readers.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := "key-" + strconv.Itoa(idx)
			c.Get(key)
		}(i)
	}

	wg.Wait()
	// No panic or race condition is the success condition.
}

// ---------------------------------------------------------------------------
// Error type tests
// ---------------------------------------------------------------------------

func TestInvalidCacheConfigError_Message(t *testing.T) {
	err := &InvalidCacheConfigError{Reason: "test reason"}
	expected := "sm: invalid cache config: test reason"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

func TestCacheMaxEntriesError_Message(t *testing.T) {
	err := &CacheMaxEntriesError{Value: -1}
	expected := "sm: cache max entries must be > 0, got -1"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

func TestCacheMaxEntriesError_ZeroValue(t *testing.T) {
	err := &CacheMaxEntriesError{Value: 0}
	expected := "sm: cache max entries must be > 0, got 0"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

// ---------------------------------------------------------------------------
// extractQueryKey tests
// ---------------------------------------------------------------------------

func TestExtractQueryKey_String(t *testing.T) {
	key := extractQueryKey("hello")
	if key != "hello" {
		t.Errorf("extractQueryKey(string) = %q, want %q", key, "hello")
	}
}

func TestExtractQueryKey_Bytes(t *testing.T) {
	key := extractQueryKey([]byte("hello"))
	if key != "hello" {
		t.Errorf("extractQueryKey([]byte) = %q, want %q", key, "hello")
	}
}

func TestExtractQueryKey_Other(t *testing.T) {
	key := extractQueryKey(42)
	if key != "" {
		t.Errorf("extractQueryKey(int) = %q, want empty", key)
	}
}

func TestExtractQueryKey_EmptyString(t *testing.T) {
	key := extractQueryKey("")
	if key != "" {
		t.Errorf("extractQueryKey(empty string) = %q, want empty", key)
	}
}

// ---------------------------------------------------------------------------
// CacheConfig validate tests
// ---------------------------------------------------------------------------

func TestCacheConfig_ValidConfig(t *testing.T) {
	cfg := CacheConfig{MaxEntries: 100, ExtractKV: simpleExtractKV}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() failed: %v", err)
	}
}

func TestCacheConfig_ZeroMaxEntries(t *testing.T) {
	cfg := CacheConfig{MaxEntries: 0, ExtractKV: simpleExtractKV}
	err := cfg.validate()
	if err == nil {
		t.Fatal("expected error for MaxEntries=0")
	}
}

func TestCacheConfig_NilExtractKV(t *testing.T) {
	cfg := CacheConfig{MaxEntries: 100, ExtractKV: nil}
	err := cfg.validate()
	if err == nil {
		t.Fatal("expected error for nil ExtractKV")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine BytesLookup and GetHash
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_BytesLookupCacheHit(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	entries := []Entry{{Index: 1, Cmd: []byte("disk-na")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	val, err := cached.BytesLookup([]byte("disk-na"))
	if err != nil {
		t.Fatalf("BytesLookup failed: %v", err)
	}
	if string(val) != "disk-na" {
		t.Errorf("BytesLookup = %q, want %q", val, "disk-na")
	}
}

func TestCachedDiskStateMachine_GetHash(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	_, err = cached.GetHash()
	if err == nil {
		t.Error("expected error when inner doesn't implement Hasher")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine BytesLookup fallback paths
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_BytesLookupCacheMissFallsToNALookup(t *testing.T) {
	// Use inner with NALookup to test the NALookup delegation path.
	inner := newDiskWithNALookup()
	inner.state["disk-miss"] = []byte("disk-value")

	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	val, err := cached.BytesLookup([]byte("disk-miss"))
	if err != nil {
		t.Fatalf("BytesLookup failed: %v", err)
	}
	if string(val) != "disk-value" {
		t.Errorf("BytesLookup = %q, want %q", val, "disk-value")
	}
	if inner.bytesLookupCount.Load() != 1 {
		t.Errorf("NALookup count = %d, want 1", inner.bytesLookupCount.Load())
	}
}

func TestCachedDiskStateMachine_BytesLookupMissReturnsNil(t *testing.T) {
	// Use inner with NALookup that returns nil for missing keys.
	inner := newDiskWithNALookup()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	val, err := cached.BytesLookup([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("BytesLookup failed: %v", err)
	}
	if val != nil {
		t.Errorf("BytesLookup = %v, want nil", val)
	}
}

func TestCachedDiskStateMachine_CacheStats(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	entries := []Entry{{Index: 1, Cmd: []byte("stats-key")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Cache hit.
	if _, err := cached.Lookup(ctx, "stats-key"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	// Cache miss.
	if _, err := cached.Lookup(ctx, "missing"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	hits, misses := cached.CacheStats()
	if hits != 1 {
		t.Errorf("hits = %d, want 1", hits)
	}
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine Update error path
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_UpdateErrorDoesNotPopulateCache(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	// Empty Cmd triggers error in inner Update.
	entries := []Entry{{Index: 1, Cmd: []byte{}}}
	results := make([]Result, 1)
	err = cached.Update(ctx, entries, results)
	if err == nil {
		t.Fatal("expected Update error for empty Cmd")
	}

	if cached.cache.Len() != 0 {
		t.Error("cache should be empty after failed Update")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine RecoverFromSnapshot error path
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_RecoverFromSnapshotError(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()
	stopper := make(chan struct{})

	// Populate cache.
	entries := []Entry{{Index: 1, Cmd: []byte("pre-recover")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Feed truncated data: a uint32 key length claiming 100 bytes, but no key data.
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(100)); err != nil {
		t.Fatalf("binary.Write failed: %v", err)
	}

	err = cached.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected RecoverFromSnapshot error for truncated data")
	}

	// Cache should NOT be cleared on error (recovery failed, old state preserved).
	if cached.cache.Len() != 1 {
		t.Errorf("cache should be preserved when RecoverFromSnapshot fails, got len=%d", cached.cache.Len())
	}
}

// ---------------------------------------------------------------------------
// Open error path for CachedDiskStateMachine
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_OpenError(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopper := make(chan struct{})

	_, err = cached.Open(ctx, "/tmp/err-open", stopper)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine with NALookup inner
// ---------------------------------------------------------------------------

// diskWithNALookup wraps trackingDiskStateMachine and adds NALookup.
type diskWithNALookup struct {
	trackingDiskStateMachine
	bytesLookupCount atomic.Int64
}

func newDiskWithNALookup() *diskWithNALookup {
	return &diskWithNALookup{
		trackingDiskStateMachine: trackingDiskStateMachine{
			testOnDiskStateMachine: testOnDiskStateMachine{
				testConcurrentStateMachine: testConcurrentStateMachine{
					testStateMachine: testStateMachine{state: make(map[string][]byte)},
				},
			},
		},
	}
}

func (d *diskWithNALookup) BytesLookup(key []byte) ([]byte, error) {
	d.bytesLookupCount.Add(1)
	val, exists := d.state[string(key)]
	if !exists {
		return nil, nil
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

var _ NALookup = (*diskWithNALookup)(nil)

func TestCachedDiskStateMachine_BytesLookupDelegatesToNALookup(t *testing.T) {
	inner := newDiskWithNALookup()
	inner.state["na-disk"] = []byte("na-disk-val")

	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	val, err := cached.BytesLookup([]byte("na-disk"))
	if err != nil {
		t.Fatalf("BytesLookup failed: %v", err)
	}
	if string(val) != "na-disk-val" {
		t.Errorf("BytesLookup = %q, want %q", val, "na-disk-val")
	}
	if inner.bytesLookupCount.Load() != 1 {
		t.Errorf("NALookup count = %d, want 1", inner.bytesLookupCount.Load())
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine with Hasher inner
// ---------------------------------------------------------------------------

type diskWithHasher struct {
	trackingDiskStateMachine
	hashValue uint64
}

func newDiskWithHasher() *diskWithHasher {
	return &diskWithHasher{
		trackingDiskStateMachine: trackingDiskStateMachine{
			testOnDiskStateMachine: testOnDiskStateMachine{
				testConcurrentStateMachine: testConcurrentStateMachine{
					testStateMachine: testStateMachine{state: make(map[string][]byte)},
				},
			},
		},
		hashValue: 99999,
	}
}

func (d *diskWithHasher) GetHash() (uint64, error) {
	return d.hashValue, nil
}

var _ Hasher = (*diskWithHasher)(nil)

func TestCachedDiskStateMachine_GetHashWithHasherInner(t *testing.T) {
	inner := newDiskWithHasher()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	hash, err := cached.GetHash()
	if err != nil {
		t.Fatalf("GetHash failed: %v", err)
	}
	if hash != 99999 {
		t.Errorf("GetHash = %d, want 99999", hash)
	}
}

// ---------------------------------------------------------------------------
// BytesLookup fallback to Lookup (no NALookup) for Disk
// ---------------------------------------------------------------------------

// diskBytesLookupFallback does NOT implement NALookup but accepts []byte in Lookup.
type diskBytesLookupFallback struct {
	testOnDiskStateMachine
	lookupCount atomic.Int64
}

func newDiskBytesLookupFallback() *diskBytesLookupFallback {
	return &diskBytesLookupFallback{
		testOnDiskStateMachine: testOnDiskStateMachine{
			testConcurrentStateMachine: testConcurrentStateMachine{
				testStateMachine: testStateMachine{state: make(map[string][]byte)},
			},
		},
	}
}

func (d *diskBytesLookupFallback) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	d.lookupCount.Add(1)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var key string
	switch q := query.(type) {
	case string:
		key = q
	case []byte:
		key = string(q)
	default:
		return nil, errTestFatal
	}
	val, exists := d.state[key]
	if !exists {
		return nil, nil
	}
	return val, nil
}

func TestCachedDiskStateMachine_BytesLookupFallbackToLookup(t *testing.T) {
	inner := newDiskBytesLookupFallback()
	inner.state["disk-fb"] = []byte("disk-fb-val")

	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	val, err := cached.BytesLookup([]byte("disk-fb"))
	if err != nil {
		t.Fatalf("BytesLookup failed: %v", err)
	}
	if string(val) != "disk-fb-val" {
		t.Errorf("BytesLookup = %q, want %q", val, "disk-fb-val")
	}
	if inner.lookupCount.Load() != 1 {
		t.Errorf("Lookup fallback count = %d, want 1", inner.lookupCount.Load())
	}
}

func TestCachedDiskStateMachine_BytesLookupFallbackMiss(t *testing.T) {
	inner := newDiskBytesLookupFallback()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}

	val, err := cached.BytesLookup([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("BytesLookup failed: %v", err)
	}
	if val != nil {
		t.Errorf("BytesLookup = %v, want nil", val)
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine LRU eviction
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_LRUEviction(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 2,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	// Insert 3 entries into a cache of size 2; "a" should be evicted.
	entries := []Entry{
		{Index: 1, Cmd: []byte("a")},
		{Index: 2, Cmd: []byte("b")},
		{Index: 3, Cmd: []byte("c")},
	}
	results := make([]Result, 3)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	inner.lookupCount.Store(0)

	// "b" and "c" should be cache hits.
	for _, key := range []string{"b", "c"} {
		if _, err := cached.Lookup(ctx, key); err != nil {
			t.Fatalf("Lookup(%s) failed: %v", key, err)
		}
	}
	if inner.lookupCount.Load() != 0 {
		t.Errorf("inner Lookup called %d times for cached keys, want 0", inner.lookupCount.Load())
	}

	// "a" should be a cache miss (evicted), falling through to inner.
	if _, err := cached.Lookup(ctx, "a"); err != nil {
		t.Fatalf("Lookup(a) failed: %v", err)
	}
	if inner.lookupCount.Load() != 1 {
		t.Errorf("inner Lookup called %d times after eviction, want 1", inner.lookupCount.Load())
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine delete via nil value
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_NilValueDeletesFromCache(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	callCount := 0
	extractKV := func(cmd []byte) (string, []byte) {
		callCount++
		if callCount == 1 {
			return string(cmd), cmd // first call: insert
		}
		return string(cmd), nil // second call: delete
	}
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  extractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	// Insert.
	entries := []Entry{{Index: 1, Cmd: []byte("key")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update (insert) failed: %v", err)
	}

	// Verify cached.
	inner.lookupCount.Store(0)
	if _, err := cached.Lookup(ctx, "key"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if inner.lookupCount.Load() != 0 {
		t.Error("expected cache hit after insert")
	}

	// Delete via nil value.
	entries2 := []Entry{{Index: 2, Cmd: []byte("key")}}
	results2 := make([]Result, 1)
	if err := cached.Update(ctx, entries2, results2); err != nil {
		t.Fatalf("Update (delete) failed: %v", err)
	}

	// Verify evicted.
	inner.lookupCount.Store(0)
	if _, err := cached.Lookup(ctx, "key"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if inner.lookupCount.Load() != 1 {
		t.Error("expected cache miss after nil-value delete")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine Close
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_Close(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	// Populate cache.
	entries := []Entry{{Index: 1, Cmd: []byte("close-key")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if err := cached.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !inner.closed {
		t.Error("inner SM not closed")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine cache miss falls through
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_CacheMissFallsThrough(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	// Directly populate inner state (bypassing cache).
	inner.state["direct-key"] = []byte("direct-value")

	val, err := cached.Lookup(ctx, "direct-key")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if string(val.([]byte)) != "direct-value" {
		t.Errorf("Lookup = %q, want %q", val, "direct-value")
	}
	if inner.lookupCount.Load() != 1 {
		t.Errorf("inner Lookup called %d times, want 1 (cache miss)", inner.lookupCount.Load())
	}

	_, misses := cached.CacheStats()
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine Lookup with []byte query
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_LookupBytesQuery(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	entries := []Entry{{Index: 1, Cmd: []byte("bytes-key")}}
	results := make([]Result, 1)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	inner.lookupCount.Store(0)

	// Query with []byte instead of string.
	val, err := cached.Lookup(ctx, []byte("bytes-key"))
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if string(val.([]byte)) != "bytes-key" {
		t.Errorf("Lookup = %q, want %q", val, "bytes-key")
	}
	if inner.lookupCount.Load() != 0 {
		t.Error("expected cache hit for []byte query")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine ExtractKV skip entries
// ---------------------------------------------------------------------------

func TestCachedDiskStateMachine_ExtractKVSkipsEntries(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	// ExtractKV returns ("", nil) for entries starting with "skip-".
	skipExtractKV := func(cmd []byte) (string, []byte) {
		s := string(cmd)
		if len(s) > 5 && s[:5] == "skip-" {
			return "", nil
		}
		return s, cmd
	}
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  skipExtractKV,
	})
	if err != nil {
		t.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	entries := []Entry{
		{Index: 1, Cmd: []byte("cached-key")},
		{Index: 2, Cmd: []byte("skip-key")},
	}
	results := make([]Result, 2)
	if err := cached.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	inner.lookupCount.Store(0)

	// "cached-key" should be a hit.
	if _, err := cached.Lookup(ctx, "cached-key"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if inner.lookupCount.Load() != 0 {
		t.Error("expected cache hit for cached-key")
	}

	// "skip-key" should be a miss (not cached).
	if _, err := cached.Lookup(ctx, "skip-key"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if inner.lookupCount.Load() != 1 {
		t.Error("expected cache miss for skip-key")
	}
}

// ---------------------------------------------------------------------------
// CachedDiskStateMachine config validation via constructor
// ---------------------------------------------------------------------------

func TestNewCachedDiskStateMachine_NilExtractKV(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	_, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 100,
		ExtractKV:  nil,
	})
	if err == nil {
		t.Fatal("expected error for nil ExtractKV")
	}
}

func TestNewCachedDiskStateMachine_ZeroMaxEntries(t *testing.T) {
	inner := newTrackingDiskStateMachine()
	_, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 0,
		ExtractKV:  simpleExtractKV,
	})
	if err == nil {
		t.Fatal("expected error for MaxEntries=0")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkCachedDiskLookup(b *testing.B) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 10000,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		b.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	// Pre-populate cache.
	entries := make([]Entry, 1000)
	results := make([]Result, 1000)
	for i := range entries {
		entries[i] = Entry{Index: uint64(i + 1), Cmd: []byte("key-" + strconv.Itoa(i))}
	}
	if err := cached.Update(ctx, entries, results); err != nil {
		b.Fatalf("Update failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i%1000)
		_, _ = cached.Lookup(ctx, key)
	}
}

func BenchmarkCachedDiskBytesLookup(b *testing.B) {
	inner := newTrackingDiskStateMachine()
	cached, err := NewCachedDiskStateMachine(inner, CacheConfig{
		MaxEntries: 10000,
		ExtractKV:  simpleExtractKV,
	})
	if err != nil {
		b.Fatalf("NewCachedDiskStateMachine failed: %v", err)
	}
	ctx := context.Background()

	entries := make([]Entry, 1000)
	results := make([]Result, 1000)
	for i := range entries {
		entries[i] = Entry{Index: uint64(i + 1), Cmd: []byte("key-" + strconv.Itoa(i))}
	}
	if err := cached.Update(ctx, entries, results); err != nil {
		b.Fatalf("Update failed: %v", err)
	}

	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte("key-" + strconv.Itoa(i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = cached.BytesLookup(keys[i%1000])
	}
}

func BenchmarkLRUPut(b *testing.B) {
	c := newLRUCache(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i%10000)
		c.Put(key, []byte(key))
	}
}

func BenchmarkLRUGet(b *testing.B) {
	c := newLRUCache(10000)
	// Pre-populate.
	for i := 0; i < 10000; i++ {
		key := "key-" + strconv.Itoa(i)
		c.Put(key, []byte(key))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i%10000)
		c.Get(key)
	}
}
