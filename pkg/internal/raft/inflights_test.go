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

package raft

import (
	"errors"
	"testing"
)

// mustAdd is a test helper that calls add and fails the test on error.
func mustAdd(t *testing.T, in *inflights, index uint64) {
	t.Helper()
	if err := in.add(index); err != nil {
		t.Fatalf("unexpected error from add(%d): %v", index, err)
	}
}

func TestNewInflightsZeroCapacity(t *testing.T) {
	in := newInflights(0)
	if in.size != 0 {
		t.Errorf("expected size 0, got %d", in.size)
	}
	if !in.full() {
		t.Errorf("expected zero-capacity inflights to be full")
	}
}

func TestInflightsAddAllocatesBufferLazily(t *testing.T) {
	in := newInflights(5)
	if in.buffer != nil {
		t.Errorf("expected nil buffer before first add")
	}
	mustAdd(t, in, 100)
	if in.buffer == nil {
		t.Errorf("expected buffer to be allocated after first add")
	}
	if len(in.buffer) != 5 {
		t.Errorf("expected buffer length 5, got %d", len(in.buffer))
	}
}

func TestInflightsAddReturnsErrInflightsFullWhenFull(t *testing.T) {
	in := newInflights(2)
	mustAdd(t, in, 100)
	mustAdd(t, in, 101)
	err := in.add(102)
	if err == nil {
		t.Fatalf("expected error when adding to full inflights, got nil")
	}
	if !errors.Is(err, ErrInflightsFull) {
		t.Errorf("expected ErrInflightsFull, got %v", err)
	}
}

func TestInflightsFullReturnsTrueAtCapacity(t *testing.T) {
	in := newInflights(3)
	if in.full() {
		t.Errorf("expected not full when empty")
	}
	mustAdd(t, in, 100)
	if in.full() {
		t.Errorf("expected not full with 1 of 3")
	}
	mustAdd(t, in, 101)
	if in.full() {
		t.Errorf("expected not full with 2 of 3")
	}
	mustAdd(t, in, 102)
	if !in.full() {
		t.Errorf("expected full with 3 of 3")
	}
}

func TestInflightsFreeToNoopWhenEmpty(t *testing.T) {
	in := newInflights(5)
	in.freeTo(100) // should not panic
	if in.Count() != 0 {
		t.Errorf("expected count 0 on empty inflights, got %d", in.Count())
	}
}

func TestInflightsFreeToWithIndexNotInBuffer(t *testing.T) {
	in := newInflights(10)
	mustAdd(t, in, 5)
	mustAdd(t, in, 10)
	mustAdd(t, in, 15)
	// Index 3 is less than all buffer entries -- should free nothing
	// because no entries have index <= 3... wait, 5 > 3, so nothing freed.
	// Actually the scan checks buffer[pos] <= index. 5 > 3, 10 > 3, 15 > 3.
	// So nothing freed.
	in.freeTo(3)
	if in.Count() != 3 {
		t.Errorf("expected count 3 (no entries freed for index 3), got %d", in.Count())
	}
}

func TestInflightsFreeToWithIndexBetweenEntries(t *testing.T) {
	in := newInflights(10)
	mustAdd(t, in, 5)
	mustAdd(t, in, 10)
	mustAdd(t, in, 15)
	// Free to index 12 should free entries with index <= 12 (5 and 10)
	in.freeTo(12)
	if in.Count() != 1 {
		t.Errorf("expected count 1, got %d", in.Count())
	}
}

func TestInflightsFreeFirstOneFreesOldest(t *testing.T) {
	in := newInflights(5)
	mustAdd(t, in, 100)
	mustAdd(t, in, 101)
	mustAdd(t, in, 102)
	in.freeFirstOne()
	if in.Count() != 2 {
		t.Errorf("expected count 2, got %d", in.Count())
	}
	// The start should have advanced
	if in.start != 1 {
		t.Errorf("expected start 1, got %d", in.start)
	}
}

func TestInflightsFreeFirstOneNoopWhenEmpty(t *testing.T) {
	in := newInflights(5)
	in.freeFirstOne() // should not panic
	if in.Count() != 0 {
		t.Errorf("expected count 0, got %d", in.Count())
	}
}

func TestInflightsCircularBufferWrapsCorrectly(t *testing.T) {
	in := newInflights(5)
	// Fill buffer
	for i := uint64(0); i < 5; i++ {
		mustAdd(t, in, i)
	}
	// Free first 3
	in.freeTo(2)
	if in.Count() != 2 {
		t.Errorf("expected count 2, got %d", in.Count())
	}
	if in.start != 3 {
		t.Errorf("expected start 3, got %d", in.start)
	}
	// Add 3 more (should wrap around)
	mustAdd(t, in, 5)
	mustAdd(t, in, 6)
	mustAdd(t, in, 7)
	if in.Count() != 5 {
		t.Errorf("expected count 5, got %d", in.Count())
	}
	if !in.full() {
		t.Errorf("expected full after wrapping")
	}
	// Verify the wrapped entries are correct by freeing them
	in.freeTo(4)
	if in.Count() != 3 {
		t.Errorf("expected count 3 after freeing wrapped entries, got %d", in.Count())
	}
	in.freeTo(7)
	if in.Count() != 0 {
		t.Errorf("expected count 0, got %d", in.Count())
	}
}

func TestInflightsCircularBufferStartWraps(t *testing.T) {
	in := newInflights(3)
	// Fill and free to move start to the end
	mustAdd(t, in, 1)
	mustAdd(t, in, 2)
	mustAdd(t, in, 3)
	in.freeTo(3) // start should wrap back to 0
	if in.start != 0 {
		t.Errorf("expected start 0 after full cycle, got %d", in.start)
	}
	if in.Count() != 0 {
		t.Errorf("expected count 0, got %d", in.Count())
	}
}

func TestInflightsResetClearsEverything(t *testing.T) {
	in := newInflights(5)
	mustAdd(t, in, 10)
	mustAdd(t, in, 20)
	mustAdd(t, in, 30)
	in.reset()
	if in.Count() != 0 {
		t.Errorf("expected count 0 after reset, got %d", in.Count())
	}
	if in.start != 0 {
		t.Errorf("expected start 0 after reset, got %d", in.start)
	}
	if in.full() {
		t.Errorf("expected not full after reset")
	}
}

func TestInflightsResetRetainsBuffer(t *testing.T) {
	in := newInflights(5)
	mustAdd(t, in, 10)
	in.reset()
	if in.buffer == nil {
		t.Errorf("expected buffer to be retained after reset")
	}
	if len(in.buffer) != 5 {
		t.Errorf("expected buffer length 5, got %d", len(in.buffer))
	}
}

func TestInflightsCountReturnsCorrectValue(t *testing.T) {
	in := newInflights(10)
	if in.Count() != 0 {
		t.Errorf("expected 0, got %d", in.Count())
	}
	mustAdd(t, in, 1)
	if in.Count() != 1 {
		t.Errorf("expected 1, got %d", in.Count())
	}
	mustAdd(t, in, 2)
	mustAdd(t, in, 3)
	if in.Count() != 3 {
		t.Errorf("expected 3, got %d", in.Count())
	}
	in.freeFirstOne()
	if in.Count() != 2 {
		t.Errorf("expected 2, got %d", in.Count())
	}
}

func TestInflightsFreeFirstOneWrapsStart(t *testing.T) {
	in := newInflights(3)
	mustAdd(t, in, 1)
	mustAdd(t, in, 2)
	mustAdd(t, in, 3)
	// Free first two via freeFirstOne to position start at 2
	in.freeFirstOne()
	in.freeFirstOne()
	// Add two more to wrap
	mustAdd(t, in, 4)
	mustAdd(t, in, 5)
	// Now start=2, count=3, entries at positions 2,0,1
	in.freeFirstOne() // frees position 2, start becomes 0
	if in.start != 0 {
		t.Errorf("expected start 0 after wrap, got %d", in.start)
	}
	if in.Count() != 2 {
		t.Errorf("expected count 2, got %d", in.Count())
	}
}
