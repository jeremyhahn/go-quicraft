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

package seal

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
)

// The barrier never calls StorageBackend — it is a shape an embedder satisfies
// by duck typing (go-qrdb's storage.Storage). That means nothing in this
// module would notice a signature drifting away from what consumers implement,
// so the contract is pinned here instead: referenceStore is a minimal
// conforming implementation, and the tests below execute the semantics the
// interface documents rather than restating them in prose.

// referenceStore is a single-process StorageBackend. It serializes the
// check-then-write and the compare-then-delete under one mutex, which is
// exactly what the interface asks of a single-process consistency domain.
type referenceStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newReferenceStore() *referenceStore {
	return &referenceStore{m: make(map[string][]byte)}
}

func (s *referenceStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (s *referenceStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = append([]byte(nil), data...)
	return nil
}

func (s *referenceStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

func (s *referenceStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *referenceStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[key]
	return ok, nil
}

func (s *referenceStore) Scan(ctx context.Context, prefix string, fn func(string, []byte) error) error {
	keys, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	// Snapshot under List, then call fn OUTSIDE the lock: fn is caller code and
	// may re-enter the store.
	for _, k := range keys {
		v, err := s.Get(ctx, k)
		if err != nil {
			return err
		}
		if v == nil {
			continue // removed between the snapshot and the read
		}
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *referenceStore) PutIfAbsent(_ context.Context, key string, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[key]; exists {
		return false, nil
	}
	s.m[key] = append([]byte(nil), data...)
	return true, nil
}

func (s *referenceStore) CompareAndDelete(_ context.Context, key string, expected []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[key]
	if !ok {
		return false, nil
	}
	if !bytes.Equal(cur, expected) {
		return false, nil
	}
	delete(s.m, key)
	return true, nil
}

func (s *referenceStore) Close() error { return nil }

// Signature drift in either direction fails the build here rather than in a
// consumer that cannot change this interface.
var _ StorageBackend = (*referenceStore)(nil)

func TestCompareAndDeleteReportsRefusalAsAnOutcomeNotAnError(t *testing.T) {
	ctx := context.Background()
	s := newReferenceStore()

	// Absent key: refused, and explicitly NOT an error — a replicated backend
	// that errored here would stall consensus over a deterministic
	// consequence of valid input.
	deleted, err := s.CompareAndDelete(ctx, "k", []byte("A"))
	if err != nil {
		t.Fatalf("absent key must not be an error, got %v", err)
	}
	if deleted {
		t.Fatal("absent key must report deleted=false")
	}

	if err := s.Put(ctx, "k", []byte("A")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Value differs: refused, still not an error, and the store is unchanged.
	deleted, err = s.CompareAndDelete(ctx, "k", []byte("B"))
	if err != nil {
		t.Fatalf("mismatched guard must not be an error, got %v", err)
	}
	if deleted {
		t.Fatal("mismatched guard must report deleted=false")
	}
	if ok, _ := s.Exists(ctx, "k"); !ok {
		t.Fatal("a refused delete must leave the store unchanged")
	}

	// Guard matches: removed, reported as deleted=true.
	deleted, err = s.CompareAndDelete(ctx, "k", []byte("A"))
	if err != nil {
		t.Fatalf("matching guard: %v", err)
	}
	if !deleted {
		t.Fatal("matching guard must report deleted=true")
	}
	if ok, _ := s.Exists(ctx, "k"); ok {
		t.Fatal("key must be gone after a successful CompareAndDelete")
	}
}

// This is the defect the method exists to close (go-qrdb#41). A stale releaser
// that read holder A, and never observed B at all, must not be able to remove
// B's claim after B legitimately acquired the key.
func TestCompareAndDeleteStopsAStaleReleaserFromRemovingTheNextHolder(t *testing.T) {
	ctx := context.Background()
	s := newReferenceStore()

	if stored, err := s.PutIfAbsent(ctx, "id", []byte("A")); err != nil || !stored {
		t.Fatalf("A should claim the key: stored=%v err=%v", stored, err)
	}

	// Two releasers both observe holder A.
	observedByReleaser1 := []byte("A")
	observedByReleaser2 := []byte("A")

	// Releaser 1 releases.
	if deleted, err := s.CompareAndDelete(ctx, "id", observedByReleaser1); err != nil || !deleted {
		t.Fatalf("releaser 1 should release A: deleted=%v err=%v", deleted, err)
	}

	// B claims the now-absent key, under the guarantee PutIfAbsent advertises.
	if stored, err := s.PutIfAbsent(ctx, "id", []byte("B")); err != nil || !stored {
		t.Fatalf("B should claim the released key: stored=%v err=%v", stored, err)
	}

	// Releaser 2 now runs, still holding its stale view of A. With a plain
	// Delete this removes B's value. Guarded, it must refuse.
	deleted, err := s.CompareAndDelete(ctx, "id", observedByReleaser2)
	if err != nil {
		t.Fatalf("stale releaser: %v", err)
	}
	if deleted {
		t.Fatal("a stale releaser must NOT remove the next holder's value")
	}

	got, err := s.Get(ctx, "id")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("B")) {
		t.Fatalf("B's claim must survive the stale release, got %q", got)
	}
}

// The guarantee is only as good as its serialization: concurrent releasers all
// holding the same observed value must produce exactly one winner, the same
// property PutIfAbsent has on the acquire side.
func TestCompareAndDeleteHasExactlyOneWinnerUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	s := newReferenceStore()
	if err := s.Put(ctx, "id", []byte("A")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	const releasers = 16
	var wg sync.WaitGroup
	results := make([]bool, releasers)
	start := make(chan struct{})
	for i := range releasers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			deleted, err := s.CompareAndDelete(ctx, "id", []byte("A"))
			if err != nil {
				t.Errorf("releaser %d: %v", i, err)
			}
			results[i] = deleted
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for _, ok := range results {
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one releaser to win, got %d", winners)
	}
}

// referenceStore is only useful as a contract fixture if it is honest about
// the whole interface, not just the two methods under test here.
func TestReferenceStoreSatisfiesTheRestOfTheShape(t *testing.T) {
	ctx := context.Background()
	s := newReferenceStore()
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		if err := s.Put(ctx, k, []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	got, err := s.List(ctx, "a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0] != "a/1" || got[1] != "a/2" {
		t.Fatalf("List must return the prefix match in order, got %v", got)
	}

	seen := map[string]string{}
	if err := s.Scan(ctx, "a/", func(k string, v []byte) error {
		seen[k] = string(v)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(seen) != 2 || seen["a/1"] != "a/1" || seen["a/2"] != "a/2" {
		t.Fatalf("Scan must visit each prefix match with its value, got %v", seen)
	}

	// Scan must surface the callback's error rather than swallowing it.
	sentinel := errors.New("stop")
	if err := s.Scan(ctx, "a/", func(string, []byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("Scan must propagate the callback error, got %v", err)
	}

	// Mutating a returned buffer must not reach into the store.
	v, _ := s.Get(ctx, "b/1")
	v[0] = 'X'
	again, _ := s.Get(ctx, "b/1")
	if !bytes.Equal(again, []byte("b/1")) {
		t.Fatalf("Get must not alias stored bytes, store now holds %q", again)
	}
}
