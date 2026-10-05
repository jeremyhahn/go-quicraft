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

import (
	"fmt"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// Decode memory amplification
//
// The transport reserves a frame's WIRE LENGTH against its aggregate
// message-receive budget and then hands the bytes to MessageBatch.UnmarshalFrom,
// whose result is materially larger than the bytes it came from. Nothing
// measured that gap, so the budget bounded bytes READ while the heap held bytes
// DECODED, and the two differed by nearly an order of magnitude for the frames
// an attacker would choose.
//
// This is the measurement. MaxDecodeMemoryFactor is the contract it enforces:
// what a receiver must multiply its reservation by for the budget to describe
// memory rather than I/O.
// ---------------------------------------------------------------------------

// amplifyingFrames returns the densest MessageBatch encodings this format
// admits, each paired with the shape it exercises. They are chosen for
// allocation per wire byte, not for realism: every one of them is a frame a peer
// can send.
func amplifyingFrames(t *testing.T) map[string][]byte {
	t.Helper()

	// A membership whose five string maps each hold n zero-length values. A map
	// entry is 12 bytes on the wire ([key:8][valueLen:4]) and about 57 in a Go
	// map, which is the densest per-byte allocation the format has.
	denseMembership := func(n int) Membership {
		mk := func() map[uint64]string {
			m := make(map[uint64]string, n)
			for i := 0; i < n; i++ {
				m[uint64(i+1)] = ""
			}
			return m
		}
		return Membership{
			Addresses:         mk(),
			Observers:         mk(),
			Witnesses:         mk(),
			Identities:        mk(),
			RemovedIdentities: mk(),
		}
	}

	frames := map[string]MessageBatch{}

	// Minimal messages: 100 wire bytes each become a 320-byte Message in the
	// decoded slice. This is the floor every batch pays.
	minimal := MessageBatch{BinVer: WireVersion, Requests: make([]Message, 1000)}
	for i := range minimal.Requests {
		minimal.Requests[i] = Message{Type: Heartbeat}
	}
	frames["minimal-messages"] = minimal

	// One message, membership maps just past a map growth boundary (7*128).
	// This is the shape that measured 9.8x while getMap clamped its size hint:
	// the map reached its final size by doubling, so the decode allocated every
	// intermediate table as well as the last one.
	frames["membership-dense"] = MessageBatch{BinVer: WireVersion, Requests: []Message{{
		Type:     InstallSnapshot,
		Snapshot: Snapshot{Index: 1, Term: 1, Membership: denseMembership(897)},
	}}}

	// Many messages, each carrying a membership with ONE entry per map. A
	// one-entry Go map still owns a whole 8-slot group, so this maximises
	// per-entry overhead rather than per-entry count. It is the current worst
	// case.
	single := MessageBatch{BinVer: WireVersion, Requests: make([]Message, 200)}
	for i := range single.Requests {
		single.Requests[i] = Message{
			Type:     InstallSnapshot,
			Snapshot: Snapshot{Index: 1, Term: 1, Membership: denseMembership(1)},
		}
	}
	frames["single-entry-memberships"] = single

	// Messages whose snapshots carry no membership at all: six map headers per
	// message against 24 wire bytes of empty count prefixes.
	empties := MessageBatch{BinVer: WireVersion, Requests: make([]Message, 200)}
	for i := range empties.Requests {
		empties.Requests[i] = Message{
			Type:     InstallSnapshot,
			Snapshot: Snapshot{Index: 1, Term: 1},
		}
	}
	frames["empty-memberships"] = empties

	// Entry-dense: entries alias the source buffer, so this is the cheapest
	// shape and the control for the others.
	entryDense := Message{Type: Replicate, Entries: make([]Entry, 5000)}
	for i := range entryDense.Entries {
		entryDense.Entries[i] = Entry{Term: 1, Index: uint64(i + 1), Type: EntryNormal}
	}
	frames["entry-dense"] = MessageBatch{BinVer: WireVersion, Requests: []Message{entryDense}}

	out := make(map[string][]byte, len(frames))
	for name, mb := range frames {
		buf := make([]byte, mb.Size())
		n, err := mb.MarshalTo(buf)
		if err != nil {
			t.Fatalf("%s: MarshalTo: %v", name, err)
		}
		out[name] = buf[:n]
	}
	return out
}

// TestDecodeMemoryAmplification is the enforcement of MaxDecodeMemoryFactor.
//
// It fails in exactly the case that matters: a decoder change that makes the
// decoded form cost more per wire byte than a receiver was told to expect. A
// clamped map size hint is the worked example — it measures 9.8x on the
// membership-dense frame, so this test refuses it, and that frame is one of
// the cases below.
func TestDecodeMemoryAmplification(t *testing.T) {
	for name, buf := range amplifyingFrames(t) {
		t.Run(name, func(t *testing.T) {
			// A fresh receiver per iteration: reusing one would amortise the
			// slice and map allocations that are the whole subject.
			perOp := allocBytesPerOp(t, 200, func() {
				var mb MessageBatch
				if _, err := mb.UnmarshalFrom(buf); err != nil {
					t.Fatalf("UnmarshalFrom: %v", err)
				}
			})
			ratio := float64(perOp) / float64(len(buf))
			t.Logf("wire=%d bytes, allocated=%d bytes, amplification=%.2fx",
				len(buf), perOp, ratio)
			if ratio > MaxDecodeMemoryFactor {
				t.Fatalf("decode allocated %d bytes for a %d-byte frame (%.2fx), "+
					"exceeding the stated MaxDecodeMemoryFactor of %d.\n"+
					"A receiver reserves the WIRE length against its message-receive "+
					"budget, so every byte above the stated factor is heap the budget "+
					"does not account for and an operator cannot size against.",
					perOp, len(buf), ratio, MaxDecodeMemoryFactor)
			}
		})
	}
}

// TestGetMapSizesExactly pins the sizing that halves the amplification above:
// the map is sized from the (already cross-checked) wire count in one
// allocation, not clamped to a fixed hint and grown by rehashing.
//
// It asserts the OBSERVABLE consequence rather than the hint itself, because
// the hint is not observable: a map built by doubling allocates its
// intermediate tables too, so the decode's total allocation is roughly twice
// the final map's. The threshold sits between the two — comfortably above one
// exactly-sized table and comfortably below a table plus its growth history.
func TestGetMapSizesExactly(t *testing.T) {
	// Just past 7*128 slots: the worst point for a map that grows into its
	// size, and unremarkable for one that is sized up front.
	const count = 897
	buf := make([]byte, 4+count*mapEntryMinBytes)
	putUint32(buf, count)
	for i := 0; i < count; i++ {
		off := 4 + i*mapEntryMinBytes
		putUint64(buf[off:], uint64(i+1))
		putUint32(buf[off+8:], 0)
	}

	perOp := allocBytesPerOp(t, 500, func() {
		m, _, err := getMap(buf)
		if err != nil || len(m) != count {
			t.Fatalf("getMap: err=%v len=%d", err, len(m))
		}
	})
	// One exactly-sized table is at most count*mapAllocBytesPerEntry; growing
	// into the same size costs that again for the tables it discards.
	limit := uint64(count) * mapAllocBytesPerEntry
	t.Logf("allocated %d bytes for %d entries (%d wire bytes), single-table bound %d",
		perOp, count, len(buf), limit)
	if perOp > limit {
		t.Fatalf("getMap allocated %d bytes for %d entries, above the %d-byte bound "+
			"for a single exactly-sized table: the map is being grown into its "+
			"size rather than allocated at it, and every superseded table stays "+
			"live until the next GC", perOp, count, limit)
	}
}

// TestMapAllocBytesPerEntryIsAnUpperBound keeps the constant honest. It is
// documentation of the Go runtime's map layout, used to derive the bound in
// TestGetMapSizesExactly, and nothing in the language pins it.
func TestMapAllocBytesPerEntryIsAnUpperBound(t *testing.T) {
	for _, count := range []int{1, 7, 8, 9, 56, 57, 112, 113, 896, 897, 4096} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			buf := make([]byte, 4+count*mapEntryMinBytes)
			putUint32(buf, uint32(count))
			for i := 0; i < count; i++ {
				off := 4 + i*mapEntryMinBytes
				putUint64(buf[off:], uint64(i+1))
				putUint32(buf[off+8:], 0)
			}
			perOp := allocBytesPerOp(t, 500, func() {
				if _, _, err := getMap(buf); err != nil {
					t.Fatalf("getMap: %v", err)
				}
			})
			// A one-entry map pays a fixed map header on top of its single
			// group, so the per-entry figure is only meaningful once the header
			// is amortised; below that, assert the absolute floor instead.
			if count < 8 {
				if perOp > 8*mapAllocBytesPerEntry+256 {
					t.Fatalf("small map allocated %d bytes for %d entries", perOp, count)
				}
				return
			}
			if perEntry := perOp / uint64(count); perEntry > mapAllocBytesPerEntry {
				t.Fatalf("map allocated %d bytes per entry at count=%d, above the "+
					"documented bound of %d; MaxDecodeMemoryFactor is derived from "+
					"this figure", perEntry, count, mapAllocBytesPerEntry)
			}
		})
	}
}

// TestDecodeChargeSaturates pins the saturating arithmetic behind every
// receive-budget reservation. A charge that wrapped negative would reserve
// negative budget, which is unlimited headroom, so overflow must clamp to
// MaxInt64, not wrap.
func TestDecodeChargeSaturates(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int64
		want int64
	}{
		{"zero", 0, 0},
		{"negative", -5, 0},
		{"ordinary", 1024, 1024 * MaxDecodeMemoryFactor},
		{"largest exact", math.MaxInt64 / MaxDecodeMemoryFactor,
			(math.MaxInt64 / MaxDecodeMemoryFactor) * MaxDecodeMemoryFactor},
		{"would overflow", math.MaxInt64/MaxDecodeMemoryFactor + 1, math.MaxInt64},
		{"max", math.MaxInt64, math.MaxInt64},
	} {
		got := DecodeCharge(tc.n)
		if got != tc.want {
			t.Errorf("DecodeCharge(%s = %d) = %d, want %d", tc.name, tc.n, got, tc.want)
		}
		if got < 0 {
			t.Errorf("DecodeCharge(%s) = %d: a negative charge is unlimited budget", tc.name, got)
		}
	}
}

// TestFrameDecodeCharge pins the per-frame charge as the sum of the two
// reservations the receiver takes: the wire length, then the positive
// decompression delta, each at DecodeCharge. A frame that decodes to no more
// than its wire length pays for its wire length alone.
func TestFrameDecodeCharge(t *testing.T) {
	const mib = int64(1 << 20)
	for _, tc := range []struct {
		name          string
		wire, decoded int64
		want          int64
	}{
		{"uncompressed", 16 * mib, 0, DecodeCharge(16 * mib)},
		{"decodes smaller than wire", 16 * mib, 8 * mib, DecodeCharge(16 * mib)},
		{"compressed to max", 1024, 16 * mib, DecodeCharge(1024) + DecodeCharge(16*mib-1024)},
		{"wire and decoded at max", 16 * mib, 16 * mib, DecodeCharge(16 * mib)},
		{"decoded above wire limit", 16 * mib, 32 * mib, DecodeCharge(32 * mib)},
		{"saturates", math.MaxInt64 / 2, math.MaxInt64, math.MaxInt64},
	} {
		got := FrameDecodeCharge(tc.wire, tc.decoded)
		if got != tc.want {
			t.Errorf("FrameDecodeCharge(%s: %d, %d) = %d, want %d", tc.name, tc.wire, tc.decoded, got, tc.want)
		}
		if got < 0 {
			t.Errorf("FrameDecodeCharge(%s) = %d: a negative charge is unlimited budget", tc.name, got)
		}
	}
}
