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

// This file deliberately carries NO build tag, unlike the suites that drive the
// injector. The injector's own logic must be tested in an ordinary build,
// because a defect in it does not produce a failure — it produces a partition
// test that passes against a perfectly healthy cluster. That is the exact class
// of vacuous gate this package was written to eliminate, so leaving the
// eliminator itself untested would be self-defeating.
package netfault

import (
	"sync"
	"testing"
)

// reset returns the injector to its pristine state. Every test starts from it,
// because the state is process-global by design.
func reset(t *testing.T) {
	t.Helper()
	Heal()
	t.Cleanup(Heal)
}

// TestDropIsFalseWhenNothingIsInjected pins the production-shaped case: with no
// faults installed, nothing is dropped. If this were ever true by accident the
// transport would silently discard traffic in the tagged build.
func TestDropIsFalseWhenNothingIsInjected(t *testing.T) {
	reset(t)
	if Drop(1, 2, 3) {
		t.Fatal("Drop reported true with no faults installed")
	}
	if got := BlockedCount(); got != 0 {
		t.Fatalf("BlockedCount() = %d with no faults installed, want 0", got)
	}
}

// TestBlockLinkIsDirectional is the property asymmetric-partition tests rest on.
// Blocking A->B must leave B->A intact; if BlockLink were symmetric, every
// "asymmetric" test would silently be testing a full partition instead.
func TestBlockLinkIsDirectional(t *testing.T) {
	reset(t)
	BlockLink(2, 3)

	if !Drop(1, 2, 3) {
		t.Error("the blocked edge 2->3 was not dropped")
	}
	if Drop(1, 3, 2) {
		t.Error("blocking 2->3 also dropped 3->2; the cut is symmetric, so no " +
			"asymmetric partition can be expressed")
	}
	if Drop(1, 2, 4) {
		t.Error("blocking 2->3 also dropped an unrelated edge 2->4")
	}
	if got := BlockedCount(); got != 1 {
		t.Errorf("BlockedCount() = %d after one BlockLink, want 1", got)
	}

	HealLink(2, 3)
	if Drop(1, 2, 3) {
		t.Error("HealLink did not restore the edge")
	}
}

// TestIsolateCutsBothDirections pins the primitive the deposed-leader tests use.
// An isolated replica must be unable to send OR receive; if only one direction
// were cut it would still hear a quorum, or still be heard by one, and would not
// be isolated at all.
func TestIsolateCutsBothDirections(t *testing.T) {
	reset(t)
	Isolate(3)

	if !Drop(1, 3, 1) {
		t.Error("an isolated replica could still SEND")
	}
	if !Drop(1, 1, 3) {
		t.Error("an isolated replica could still RECEIVE")
	}
	if Drop(1, 1, 2) {
		t.Error("isolating 3 also dropped traffic between 1 and 2")
	}
	if got := BlockedCount(); got != 1 {
		t.Errorf("BlockedCount() = %d after one Isolate, want 1", got)
	}

	Rejoin(3)
	if Drop(1, 3, 1) || Drop(1, 1, 3) {
		t.Error("Rejoin did not restore the isolated replica")
	}
}

// TestPartitionCutsAcrossAndNotWithin is what makes a split-brain test mean
// anything: each side must remain internally connected so it can elect among
// itself, while nothing crosses the cut. A partition that also severed traffic
// WITHIN a side would prevent either side from electing, and a
// "minority cannot elect" test would pass for the wrong reason.
func TestPartitionCutsAcrossAndNotWithin(t *testing.T) {
	reset(t)
	left, right := []uint64{1, 2}, []uint64{3, 4, 5}
	Partition(left, right)

	for _, a := range left {
		for _, b := range right {
			if !Drop(1, a, b) {
				t.Errorf("edge %d->%d crosses the cut but was not dropped", a, b)
			}
			if !Drop(1, b, a) {
				t.Errorf("edge %d->%d crosses the cut but was not dropped", b, a)
			}
		}
	}
	if Drop(1, 1, 2) || Drop(1, 2, 1) {
		t.Error("traffic WITHIN the left side was dropped; that side cannot elect")
	}
	if Drop(1, 3, 4) || Drop(1, 4, 5) {
		t.Error("traffic WITHIN the right side was dropped; that side cannot elect")
	}

	// 2 x 3 pairs, both directions.
	if got, want := BlockedCount(), 12; got != want {
		t.Errorf("BlockedCount() = %d, want %d", got, want)
	}
}

// TestHealRemovesEverything pins the cleanup every test registers with
// t.Cleanup. The state is process-global, so a fault left standing leaks into
// whatever runs next — and it would leak as a test failing for a reason that has
// nothing to do with what it asserts.
func TestHealRemovesEverything(t *testing.T) {
	reset(t)
	BlockLink(1, 2)
	Isolate(4)
	Partition([]uint64{1}, []uint64{5})
	if BlockedCount() == 0 {
		t.Fatal("fixture installed no faults")
	}

	Heal()

	if got := BlockedCount(); got != 0 {
		t.Fatalf("BlockedCount() = %d after Heal, want 0", got)
	}
	for _, e := range [][2]uint64{{1, 2}, {4, 1}, {1, 4}, {1, 5}, {5, 1}} {
		if Drop(1, e[0], e[1]) {
			t.Errorf("edge %d->%d still dropped after Heal", e[0], e[1])
		}
	}
}

// TestMutatorsAreSafeUnderConcurrentReads exercises the copy-on-write
// publication. The send path calls Drop from every sender goroutine while a test
// flips a partition, so a mutator that edited a published map in place would be
// a data race in the suite that is supposed to be finding races.
func TestMutatorsAreSafeUnderConcurrentReads(t *testing.T) {
	reset(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = Drop(1, 2, 3)
					_ = BlockedCount()
				}
			}
		}()
	}

	for i := uint64(0); i < 200; i++ {
		BlockLink(i, i+1)
		Isolate(i)
		HealLink(i, i+1)
		Rejoin(i)
	}
	close(stop)
	wg.Wait()

	if got := BlockedCount(); got != 0 {
		t.Fatalf("BlockedCount() = %d after paired install/remove, want 0", got)
	}
}

// TestShardIDIsNotConsulted pins that cuts are node-level, which is what a
// network partition actually is. If the shard were part of the key, a partition
// installed by one test would not affect a shard it did not name, and a
// multi-shard host would keep replicating across a cut that was supposed to be
// total.
func TestShardIDIsNotConsulted(t *testing.T) {
	reset(t)
	BlockLink(1, 2)
	for _, shard := range []uint64{0, 1, 7, 1 << 40} {
		if !Drop(shard, 1, 2) {
			t.Errorf("edge 1->2 was not dropped for shard %d; the cut is "+
				"per-shard rather than per-node", shard)
		}
	}
}
