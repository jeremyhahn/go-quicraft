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
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

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
	if r.Count() != 1 {
		t.Fatalf("expected 1 entry after overwrite, got %d", r.Count())
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
				r.Resolve(id, uint64(i%10))
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

func TestNodeNotFoundErrorMessage(t *testing.T) {
	err := &NodeNotFoundError{ShardID: 5, ReplicaID: 10}
	expected := "registry: node not found (shard=5, replica=10)"
	if err.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, err.Error())
	}
}

// ---------------------------------------------------------------------------
// AddressInUse
// ---------------------------------------------------------------------------

// TestAddressInUsePresentAndAbsent verifies the predicate matches on the
// address value regardless of which (shard, replica) holds it, and that an
// unregistered address (and the empty string) is reported absent.
func TestAddressInUsePresentAndAbsent(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 1, "10.0.0.1:5000")
	r.Register(2, 7, "10.0.0.2:5000")

	if !r.AddressInUse("10.0.0.1:5000") {
		t.Fatal("registered address reported not in use")
	}
	if !r.AddressInUse("10.0.0.2:5000") {
		t.Fatal("registered address on a different shard reported not in use")
	}
	if r.AddressInUse("10.0.0.3:5000") {
		t.Fatal("unregistered address reported in use")
	}
	if r.AddressInUse("") {
		t.Fatal("empty address reported in use")
	}
}

// TestAddressInUseAfterRemove verifies an address shared by several members
// stays in use until the LAST member mapping to it is removed. This is the
// property a caller relies on before tearing down the address-keyed connection
// that all of those members multiplex over.
func TestAddressInUseAfterRemove(t *testing.T) {
	r := NewRegistry()
	const addr = "10.0.0.9:5000"
	r.Register(1, 3, addr)
	r.Register(2, 3, addr)

	r.Remove(1, 3)
	if !r.AddressInUse(addr) {
		t.Fatal("address reported free while another shard still maps to it")
	}

	r.Remove(2, 3)
	if r.AddressInUse(addr) {
		t.Fatal("address reported in use after its last member was removed")
	}
	if r.Count() != 0 {
		t.Fatalf("expected count 0 after removals, got %d", r.Count())
	}
}

// TestAddressInUseAfterCompareAndRemove verifies the predicate observes the
// rollback path (speculative pre-register removed by CompareAndRemove).
func TestAddressInUseAfterCompareAndRemove(t *testing.T) {
	r := NewRegistry()
	const addr = "10.0.0.4:5000"
	r.Register(3, 4, addr)

	if !r.CompareAndRemove(3, 4, addr) {
		t.Fatal("CompareAndRemove failed on a matching entry")
	}
	if r.AddressInUse(addr) {
		t.Fatal("address reported in use after CompareAndRemove")
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
		r.Resolve(5, 5)
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
			r.Resolve(5, 5)
		}
	})
}

// TestRegisterIfAbsentStoresOnlyWhenAbsent pins the establish-but-never-replace
// contract address learning depends on: the first call stores and reports it,
// a later call with a DIFFERENT address keeps the original and says so.
func TestRegisterIfAbsentStoresOnlyWhenAbsent(t *testing.T) {
	r := NewRegistry()
	got, stored := r.RegisterIfAbsent(1, 2, "a:1")
	if !stored || got != "a:1" {
		t.Fatalf("first RegisterIfAbsent = (%q, %v), want (\"a:1\", true)", got, stored)
	}
	got, stored = r.RegisterIfAbsent(1, 2, "b:2")
	if stored || got != "a:1" {
		t.Fatalf("second RegisterIfAbsent = (%q, %v), want the kept (\"a:1\", false)", got, stored)
	}
	if addr, err := r.Resolve(1, 2); err != nil || addr != "a:1" {
		t.Fatalf("Resolve = (%q, %v), want a:1: the established mapping was replaced", addr, err)
	}
	if c := r.Count(); c != 1 {
		t.Fatalf("Count = %d, want 1", c)
	}
}

// TestRegisterIfAbsentConcurrentHasOneWinner is the race the function exists
// to close: many concurrent first-contacts for one (shard, replica), each
// with its own address. Exactly one may store, and every caller must be told
// the winner's address, never its own losing one.
func TestRegisterIfAbsentConcurrentHasOneWinner(t *testing.T) {
	r := NewRegistry()
	const racers = 64
	var (
		wg      sync.WaitGroup
		winners atomic.Int64
		results [racers]string
	)
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			got, stored := r.RegisterIfAbsent(7, 9, fmt.Sprintf("racer-%d:1", i))
			if stored {
				winners.Add(1)
			}
			results[i] = got
		}(i)
	}
	close(start)
	wg.Wait()
	if w := winners.Load(); w != 1 {
		t.Fatalf("%d callers stored an address, want exactly 1", w)
	}
	final, err := r.Resolve(7, 9)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for i, got := range results {
		if got != final {
			t.Fatalf("racer %d was told %q, but the registry holds %q", i, got, final)
		}
	}
	if c := r.Count(); c != 1 {
		t.Fatalf("Count = %d, want 1", c)
	}
}

// TestCompareAndRegisterSwapsOnlyOnMatch pins the rollback-restore contract: a
// speculative mapping is restored to its prior value only while it still holds
// the speculative value, and a mapping a later change already claimed wins.
func TestCompareAndRegisterSwapsOnlyOnMatch(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 2, "speculative:1")
	if !r.CompareAndRegister(1, 2, "speculative:1", "prior:1") {
		t.Fatal("CompareAndRegister did not swap a matching entry")
	}
	if addr, _ := r.Resolve(1, 2); addr != "prior:1" {
		t.Fatalf("after swap Resolve = %q, want prior:1", addr)
	}
	// A later change claimed the entry: the late rollback must lose.
	r.Register(1, 2, "committed:1")
	if r.CompareAndRegister(1, 2, "speculative:1", "prior:1") {
		t.Fatal("CompareAndRegister swapped an entry that no longer held the expected value")
	}
	if addr, _ := r.Resolve(1, 2); addr != "committed:1" {
		t.Fatalf("late rollback overwrote the committed mapping: Resolve = %q", addr)
	}
	// An absent entry is never created by a compare.
	if r.CompareAndRegister(3, 4, "", "x:1") {
		t.Fatal("CompareAndRegister created an entry that did not exist")
	}
	if c := r.Count(); c != 1 {
		t.Fatalf("Count = %d, want 1: CompareAndRegister must not change it", c)
	}
}

// TestCompareAndRemoveKeepsAMappingThatMoved is CompareAndRemove's losing
// branch: a rollback must not delete a mapping the apply path overwrote.
func TestCompareAndRemoveKeepsAMappingThatMoved(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 2, "applied:1")
	if r.CompareAndRemove(1, 2, "speculative:1") {
		t.Fatal("CompareAndRemove deleted a mapping that no longer held the expected value")
	}
	if addr, err := r.Resolve(1, 2); err != nil || addr != "applied:1" {
		t.Fatalf("Resolve = (%q, %v), want the applied mapping intact", addr, err)
	}
	if c := r.Count(); c != 1 {
		t.Fatalf("Count = %d, want 1", c)
	}
}

// TestReplicasAt returns every replica routed to an address, and none for an
// address nothing is routed to.
func TestReplicasAt(t *testing.T) {
	r := NewRegistry()
	r.Register(1, 1, "10.0.0.1:5000")
	r.Register(2, 3, "10.0.0.1:5000") // a co-located replica of another shard
	r.Register(1, 2, "10.0.0.2:5000")

	got := r.ReplicasAt("10.0.0.1:5000")
	slices.SortFunc(got, func(a, b NodeInfo) int { return cmp.Compare(a.ShardID, b.ShardID) })
	want := []NodeInfo{{ShardID: 1, ReplicaID: 1}, {ShardID: 2, ReplicaID: 3}}
	if !slices.Equal(got, want) {
		t.Fatalf("ReplicasAt = %v, want %v", got, want)
	}
	if got := r.ReplicasAt("10.0.0.9:5000"); len(got) != 0 {
		t.Fatalf("ReplicasAt of an unused address = %v, want none", got)
	}
}

func TestAddresses(t *testing.T) {
	r := NewRegistry()
	if got := r.Addresses(); len(got) != 0 {
		t.Fatalf("Addresses of an empty registry = %v, want none", got)
	}
	r.Register(1, 1, "10.0.0.1:5000")
	r.Register(2, 3, "10.0.0.1:5000") // co-located: one address, two replicas
	r.Register(1, 2, "10.0.0.2:5000")

	got := r.Addresses()
	slices.Sort(got)
	want := []string{"10.0.0.1:5000", "10.0.0.2:5000"}
	if !slices.Equal(got, want) {
		t.Fatalf("Addresses = %v, want %v (each address once)", got, want)
	}

	r.Remove(1, 2)
	if got := r.Addresses(); !slices.Equal(got, []string{"10.0.0.1:5000"}) {
		t.Fatalf("Addresses after Remove = %v, want [10.0.0.1:5000]", got)
	}
}
