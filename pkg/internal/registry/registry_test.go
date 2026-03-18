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

package registry

import (
	"errors"
	"sync"
	"testing"
)

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}
	if r.Count() != 0 {
		t.Fatalf("expected 0 entries, got %d", r.Count())
	}
}

func TestRegisterAndResolve(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 1, "10.0.0.1:5000")

	addr, err := r.Resolve(1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "10.0.0.1:5000" {
		t.Fatalf("expected 10.0.0.1:5000, got %s", addr)
	}
}

func TestResolveNonExistentNode(t *testing.T) {
	r := NewRegistry()

	_, err := r.Resolve(99, 99)
	if err == nil {
		t.Fatal("expected error for non-existent node")
	}

	var notFound *NodeNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected NodeNotFoundError, got %T", err)
	}
	if notFound.ShardID != 99 || notFound.ReplicaID != 99 {
		t.Fatalf("unexpected error fields: shard=%d, replica=%d",
			notFound.ShardID, notFound.ReplicaID)
	}

	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatal("expected errors.Is match with ErrNodeNotFound")
	}
}

func TestRegisterOverwritesExistingAddress(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 1, "10.0.0.1:5000")
	r.Register(1, 1, "10.0.0.2:6000")

	addr, err := r.Resolve(1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "10.0.0.2:6000" {
		t.Fatalf("expected 10.0.0.2:6000, got %s", addr)
	}
}

func TestRemoveAndResolve(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 1, "10.0.0.1:5000")
	r.Remove(1, 1)

	_, err := r.Resolve(1, 1)
	if err == nil {
		t.Fatal("expected error after removal")
	}
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected NodeNotFoundError, got %T", err)
	}
}

func TestRemoveNonExistent(t *testing.T) {
	r := NewRegistry()
	// Remove on non-existent node should not panic.
	r.Remove(42, 42)
	if r.Count() != 0 {
		t.Fatalf("expected 0 entries, got %d", r.Count())
	}
}

func TestCount(t *testing.T) {
	r := NewRegistry()
	if r.Count() != 0 {
		t.Fatalf("expected 0 entries, got %d", r.Count())
	}

	r.Register(1, 1, "10.0.0.1:5000")
	r.Register(2, 1, "10.0.0.2:5000")
	r.Register(3, 1, "10.0.0.3:5000")
	if r.Count() != 3 {
		t.Fatalf("expected 3 entries, got %d", r.Count())
	}

	r.Remove(2, 1)
	if r.Count() != 2 {
		t.Fatalf("expected 2 entries, got %d", r.Count())
	}
}

func TestCountAfterOverwrite(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 1, "10.0.0.1:5000")
	r.Register(1, 1, "10.0.0.1:6000")
	if r.Count() != 1 {
		t.Fatalf("expected 1 entry after overwrite, got %d", r.Count())
	}
}

func TestMultipleNodes(t *testing.T) {
	r := NewRegistry()
	nodes := []struct {
		shardID   uint64
		replicaID uint64
		address   string
	}{
		{1, 1, "10.0.0.1:5000"},
		{1, 2, "10.0.0.2:5000"},
		{2, 1, "10.0.0.3:5000"},
		{2, 2, "10.0.0.4:5000"},
		{3, 1, "10.0.0.5:5000"},
	}

	for _, n := range nodes {
		r.Register(n.shardID, n.replicaID, n.address)
	}

	if r.Count() != len(nodes) {
		t.Fatalf("expected %d entries, got %d", len(nodes), r.Count())
	}

	for _, n := range nodes {
		addr, err := r.Resolve(n.shardID, n.replicaID)
		if err != nil {
			t.Fatalf("unexpected error for (%d, %d): %v",
				n.shardID, n.replicaID, err)
		}
		if addr != n.address {
			t.Fatalf("expected %s for (%d, %d), got %s",
				n.address, n.shardID, n.replicaID, addr)
		}
	}
}

func TestConcurrentRegisterResolveRemove(t *testing.T) {
	r := NewRegistry()
	const goroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	// Concurrent registers.
	for g := 0; g < goroutines; g++ {
		go func(id uint64) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				r.Register(id, uint64(i%10), "10.0.0.1:5000")
			}
		}(uint64(g))
	}

	// Concurrent resolves.
	for g := 0; g < goroutines; g++ {
		go func(id uint64) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				r.Resolve(id, uint64(i%10)) //nolint:errcheck
			}
		}(uint64(g))
	}

	// Concurrent removes.
	for g := 0; g < goroutines; g++ {
		go func(id uint64) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				r.Remove(id, uint64(i%10))
			}
		}(uint64(g))
	}

	wg.Wait()
}

func TestNodeInfoAsMapKey(t *testing.T) {
	// Verify that different (shardID, replicaID) pairs produce distinct keys.
	r := NewRegistry()
	r.Register(1, 2, "addr-1-2")
	r.Register(2, 1, "addr-2-1")

	addr12, err := r.Resolve(1, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr12 != "addr-1-2" {
		t.Fatalf("expected addr-1-2, got %s", addr12)
	}

	addr21, err := r.Resolve(2, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr21 != "addr-2-1" {
		t.Fatalf("expected addr-2-1, got %s", addr21)
	}
}

func TestNodeNotFoundErrorMessage(t *testing.T) {
	err := &NodeNotFoundError{ShardID: 5, ReplicaID: 10}
	expected := "registry: node not found (shard=5, replica=10)"
	if err.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, err.Error())
	}
}

func TestNodeNotFoundErrorIs(t *testing.T) {
	err := &NodeNotFoundError{ShardID: 1, ReplicaID: 2}
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatal("expected errors.Is match with ErrNodeNotFound")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkRegistryResolve(b *testing.B) {
	b.ReportAllocs()
	r := NewRegistry()
	for i := uint64(0); i < 100; i++ {
		r.Register(i/10, i%10, "10.0.0.1:5000")
	}
	// Resolve an entry known to exist.
	for b.Loop() {
		r.Resolve(5, 5) //nolint:errcheck
	}
}

func BenchmarkRegistryRegister(b *testing.B) {
	b.ReportAllocs()
	r := NewRegistry()
	for b.Loop() {
		r.Register(1, 1, "10.0.0.1:5000")
	}
}

func BenchmarkRegistryResolve_Concurrent(b *testing.B) {
	b.ReportAllocs()
	r := NewRegistry()
	for i := uint64(0); i < 100; i++ {
		r.Register(i/10, i%10, "10.0.0.1:5000")
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Resolve(5, 5) //nolint:errcheck
		}
	})
}
