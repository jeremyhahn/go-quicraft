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

package proto

import "testing"

func TestEntrySlicePool_GetReturnsEmptySlice(t *testing.T) {
	sp := GetEntrySlice()
	defer PutEntrySlice(sp)
	if len(*sp) != 0 {
		t.Fatalf("expected length 0, got %d", len(*sp))
	}
	if cap(*sp) < defaultEntrySliceCap {
		t.Fatalf("expected capacity >= %d, got %d", defaultEntrySliceCap, cap(*sp))
	}
}

func TestEntrySlicePool_ClearsEntriesOnReturn(t *testing.T) {
	sp := GetEntrySlice()
	*sp = append(*sp,
		Entry{Term: 5, Index: 100, Cmd: []byte("payload1")},
		Entry{Term: 5, Index: 101, Cmd: []byte("payload2")},
		Entry{Term: 5, Index: 102, Cmd: []byte("payload3")},
	)

	// Capture the slice before putting it back.
	slice := *sp

	// Return to pool -- this should zero all entries.
	PutEntrySlice(sp)

	// Verify that the backing array entries were zeroed.
	// Access the original slice header (length was 3 before reset).
	fullSlice := slice[:3]
	for i, e := range fullSlice {
		if e.Term != 0 || e.Index != 0 || e.Cmd != nil {
			t.Fatalf("entry %d not zeroed after PutEntrySlice: Term=%d Index=%d Cmd=%v",
				i, e.Term, e.Index, e.Cmd)
		}
	}
}

func TestEntrySlicePool_PutNilIsNoop(t *testing.T) {
	// Must not panic.
	PutEntrySlice(nil)
}

func TestEntrySlicePool_GrowthPreserved(t *testing.T) {
	sp := GetEntrySlice()

	// Grow beyond default capacity.
	for i := range 64 {
		*sp = append(*sp, Entry{Term: 1, Index: uint64(i + 1)})
	}
	if cap(*sp) < 64 {
		t.Fatalf("expected capacity >= 64 after growth, got %d", cap(*sp))
	}
	grownCap := cap(*sp)

	PutEntrySlice(sp)

	// Get again and verify the grown capacity is preserved.
	sp2 := GetEntrySlice()
	defer PutEntrySlice(sp2)
	if len(*sp2) != 0 {
		t.Fatalf("expected length 0, got %d", len(*sp2))
	}
	// If we got the same pooled item, capacity should be preserved.
	// sync.Pool is best-effort, so only check if capacity >= default.
	backingPtr := &(*sp)[:cap(*sp)][0]
	backingPtr2 := &(*sp2)[:cap(*sp2)][0]
	if backingPtr == backingPtr2 && cap(*sp2) != grownCap {
		t.Fatalf("expected grown capacity %d preserved, got %d", grownCap, cap(*sp2))
	}
}

func BenchmarkEntrySlicePool_GetPut(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sp := GetEntrySlice()
		*sp = append(*sp, Entry{Term: 1, Index: 1, Cmd: []byte("bench")})
		PutEntrySlice(sp)
	}
}

func BenchmarkEntrySlicePool_GetPutBatch16(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sp := GetEntrySlice()
		for i := range 16 {
			*sp = append(*sp, Entry{Term: 1, Index: uint64(i + 1), Cmd: []byte("bench")})
		}
		PutEntrySlice(sp)
	}
}
