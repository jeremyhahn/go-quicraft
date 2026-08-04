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

package kv

import (
	"context"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// applyPut applies a single Put through Update on either store type.
func applyPut(t *testing.T, update func(context.Context, []sm.Entry, []sm.Result) error, key, value string) {
	t.Helper()
	entries := []sm.Entry{{Index: 1, Cmd: mustEncodePut([]byte(key), []byte(value))}}
	results := make([]sm.Result, 1)
	if err := update(context.Background(), entries, results); err != nil {
		t.Fatalf("Update(%q=%q) = %v", key, value, err)
	}
}

// TestGetHash_BoundaryAmbiguity is the regression test for the missing
// length prefixes: without them {"user":"1"} and {"use":"r1"} concatenate
// to the same byte stream and hash identically, so genuine divergence
// between replicas would go undetected. The two states MUST hash
// differently on both store implementations.
func TestGetHash_BoundaryAmbiguity(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		a, b := NewMemoryStore(), NewMemoryStore()
		applyPut(t, a.Update, "user", "1")
		applyPut(t, b.Update, "use", "r1")
		ha, err := a.GetHash()
		if err != nil {
			t.Fatalf("GetHash a = %v", err)
		}
		hb, err := b.GetHash()
		if err != nil {
			t.Fatalf("GetHash b = %v", err)
		}
		if ha == hb {
			t.Fatalf("divergent states hash identically (%d): key/value boundaries are ambiguous", ha)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		a, b := NewConcurrentStore(), NewConcurrentStore()
		applyPut(t, a.Update, "user", "1")
		applyPut(t, b.Update, "use", "r1")
		ha, err := a.GetHash()
		if err != nil {
			t.Fatalf("GetHash a = %v", err)
		}
		hb, err := b.GetHash()
		if err != nil {
			t.Fatalf("GetHash b = %v", err)
		}
		if ha == hb {
			t.Fatalf("divergent states hash identically (%d): key/value boundaries are ambiguous", ha)
		}
	})
}

// TestGetHash_CrossStoreEquality asserts that MemoryStore and
// ConcurrentStore produce the same hash for the same logical contents:
// both hash sorted keys through the shared hashLenPrefixed helper, so a
// mixed deployment can compare hashes across implementations.
func TestGetHash_CrossStoreEquality(t *testing.T) {
	ms, cs := NewMemoryStore(), NewConcurrentStore()
	for _, kv := range [][2]string{{"alpha", "1"}, {"beta", "22"}, {"gamma", ""}} {
		applyPut(t, ms.Update, kv[0], kv[1])
		applyPut(t, cs.Update, kv[0], kv[1])
	}
	hm, err := ms.GetHash()
	if err != nil {
		t.Fatalf("MemoryStore.GetHash = %v", err)
	}
	hc, err := cs.GetHash()
	if err != nil {
		t.Fatalf("ConcurrentStore.GetHash = %v", err)
	}
	if hm != hc {
		t.Fatalf("MemoryStore hash %d != ConcurrentStore hash %d for identical contents", hm, hc)
	}
}

// TestConcurrentStore_GetHash_AtomicUnderWrites exercises GetHash racing
// with Update. GetHash holds all bucket read locks for the whole
// computation, so every observed hash must equal the hash of a state that
// existed atomically: here each Update batch overwrites both keys to the
// same generation value, so a hash mixing two generations (the old
// two-phase bug) would match neither pre-state nor post-state. The race
// detector additionally validates the locking discipline.
func TestConcurrentStore_GetHash_AtomicUnderWrites(t *testing.T) {
	cs := NewConcurrentStore()
	applyPut(t, cs.Update, "k1", "gen-0")
	applyPut(t, cs.Update, "k2", "gen-0")

	// Precompute the hash of every consistent generation.
	valid := make(map[uint64]bool)
	ref := NewConcurrentStore()
	for gen := 0; gen < 10; gen++ {
		v := "gen-" + string(rune('0'+gen))
		applyPut(t, ref.Update, "k1", v)
		applyPut(t, ref.Update, "k2", v)
		h, err := ref.GetHash()
		if err != nil {
			t.Fatalf("ref GetHash = %v", err)
		}
		valid[h] = true
	}

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for gen := 1; gen < 10; gen++ {
			v := "gen-" + string(rune('0'+gen))
			entries := []sm.Entry{
				{Index: uint64(gen), Cmd: mustEncodePut([]byte("k1"), []byte(v))},
				{Index: uint64(gen), Cmd: mustEncodePut([]byte("k2"), []byte(v))},
			}
			results := make([]sm.Result, 2)
			if err := cs.Update(context.Background(), entries, results); err != nil {
				t.Errorf("Update = %v", err)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				h, err := cs.GetHash()
				if err != nil {
					t.Errorf("GetHash = %v", err)
					return
				}
				if !valid[h] {
					t.Errorf("GetHash observed hash %d of a state that never existed atomically", h)
					return
				}
				select {
				case <-writerDone:
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
}
