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
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// The type fields, and the one field a message can lose in silence
//
// writeside_limits_test.go covers the LENGTH limits each encoder must mirror.
// These are the remaining asymmetries: the two enum ranges the decoders check
// and the encoders did not, the reserved bits of Entry.Type that neither end
// checked, and the embedded Snapshot that Message.MarshalTo discarded without
// saying so.
//
// The write-side map limits themselves (ErrAddressTooLong,
// ErrTooManyMembershipEntries) are pinned on Membership in
// writeside_limits_test.go, which reaches the same checkWritableMap/putMap/getMap
// helpers on the durable path.
// ---------------------------------------------------------------------------

// TestEntryMarshalRefusesTypesUnmarshalRefuses drives the durable round trip.
// An entry is committed to the raft log BEFORE any decoder sees it, so an
// encoder that emits a type its own decoder rejects writes an entry that every
// replica — including the one that wrote it — faults on for the life of the
// shard.
func TestEntryMarshalRefusesTypesUnmarshalRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  uint64
	}{
		{"unknown enum value", 5},
		{"unknown enum value with compression flag", 5 | EntryCompressFlag},
		{"unknown enum value at the top of the mask", EntryTypeMask},
		// The reserved bits. The masked type is a perfectly good EntryNormal,
		// so every IsConfigChange/IsMetadataEntry predicate in the engine reads
		// these as ordinary entries while 59 bits of sender-chosen data ride
		// along in a replicated field.
		{"reserved bit immediately above the compression flag", EntryNormal | 0x20},
		{"reserved high bit", EntryConfigChange | 1<<63},
		{"every reserved bit", EntryNormal | entryTypeReservedMask},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := Entry{Term: 1, Index: 1, Type: tc.typ, Cmd: []byte("x")}
			buf := make([]byte, e.Size())
			if _, err := e.MarshalTo(buf); !errors.Is(err, ErrInvalidEntryType) {
				t.Fatalf("MarshalTo(Type=%#x): got %v, want ErrInvalidEntryType", tc.typ, err)
			}

			// The read side must refuse the same bytes, or the write-side check
			// is stricter than the format rather than a mirror of it. Encode by
			// hand, since MarshalTo now declines.
			raw := marshalEntryUnchecked(e)
			var decoded Entry
			if _, err := decoded.UnmarshalFrom(raw); !errors.Is(err, ErrInvalidEntryType) {
				t.Fatalf("UnmarshalFrom(Type=%#x): got %v, want ErrInvalidEntryType.\n"+
					"The encoder refuses this type precisely because the decoder does; "+
					"if the decoder admits it, the two ends disagree about the format",
					tc.typ, err)
			}
		})
	}
}

// TestEntryMarshalAcceptsEveryLegalType is the boundary control. Without it the
// test above passes on an encoder that refuses everything.
func TestEntryMarshalAcceptsEveryLegalType(t *testing.T) {
	for _, base := range []uint64{
		EntryNormal, EntryConfigChange, EntryEncodedEntry, EntryMetadata,
		EntryInitialMembership,
	} {
		for _, typ := range []uint64{base, base | EntryCompressFlag} {
			e := Entry{Term: 1, Index: 1, Type: typ, Cmd: []byte("payload")}
			buf := make([]byte, e.Size())
			n, err := e.MarshalTo(buf)
			if err != nil {
				t.Fatalf("MarshalTo(Type=%#x): %v", typ, err)
			}
			var decoded Entry
			if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
				t.Fatalf("UnmarshalFrom(Type=%#x): %v", typ, err)
			}
			if decoded.Type != typ {
				t.Fatalf("Type did not round-trip: got %#x, want %#x", decoded.Type, typ)
			}
		}
	}
}

// TestMessageMarshalRefusesTypesUnmarshalRefuses covers the second enum range.
// Message.Type selects the handler at the receiver, so an out-of-range value is
// not cosmetic: UnmarshalFrom rejects the whole FRAME, taking every other
// message batched with it, and the sender sees only a dead connection.
func TestMessageMarshalRefusesTypesUnmarshalRefuses(t *testing.T) {
	for _, typ := range []uint64{RateLimit + 1, 100, 1 << 63} {
		m := Message{Type: typ, From: 1, To: 2}
		buf := make([]byte, m.Size())
		if _, err := m.MarshalTo(buf); !errors.Is(err, ErrInvalidMessageType) {
			t.Fatalf("MarshalTo(Type=%d): got %v, want ErrInvalidMessageType", typ, err)
		}
	}

	// Boundary control: the highest legal type round-trips.
	m := Message{Type: RateLimit, From: 1, To: 2}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo(Type=RateLimit): %v", err)
	}
	var decoded Message
	if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("UnmarshalFrom(Type=RateLimit): %v", err)
	}
	if decoded.Type != RateLimit {
		t.Fatalf("Type: got %d, want %d", decoded.Type, RateLimit)
	}
}

// TestMessageBatchMarshalRejectsInvalidNestedTypes asserts the refusal
// propagates out of the enclosing types rather than being swallowed by a batch
// that writes what it can.
func TestMessageBatchMarshalRejectsInvalidNestedTypes(t *testing.T) {
	t.Run("message type", func(t *testing.T) {
		mb := MessageBatch{BinVer: WireVersion, Requests: []Message{
			{Type: Heartbeat}, {Type: RateLimit + 1},
		}}
		buf := make([]byte, mb.Size())
		if _, err := mb.MarshalTo(buf); !errors.Is(err, ErrInvalidMessageType) {
			t.Fatalf("MarshalTo: got %v, want ErrInvalidMessageType", err)
		}
	})

	t.Run("entry type", func(t *testing.T) {
		mb := MessageBatch{BinVer: WireVersion, Requests: []Message{{
			Type:    Replicate,
			Entries: []Entry{{Term: 1, Index: 1, Type: EntryNormal | 0x20}},
		}}}
		buf := make([]byte, mb.Size())
		if _, err := mb.MarshalTo(buf); !errors.Is(err, ErrInvalidEntryType) {
			t.Fatalf("MarshalTo: got %v, want ErrInvalidEntryType", err)
		}
	})
}

// TestMessageMarshalRefusesPositionlessSnapshot pins that a message cannot
// drop the field without a word.
//
// The presence flag is Snapshot.IsEmpty, which asks only about Index and Term.
// A snapshot carrying a filepath, a size and a whole membership but no log
// position therefore encoded as ABSENT: MarshalTo returned success, the frame
// was well-formed, and the receiver got an InstallSnapshot with nothing in it.
// No error anywhere on either side.
func TestMessageMarshalRefusesPositionlessSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		snap Snapshot
	}{
		{"filepath only", Snapshot{Filepath: "/snap/0.dat"}},
		{"file size only", Snapshot{FileSize: 4096}},
		{"shard and replica only", Snapshot{ShardID: 1, ReplicaID: 2}},
		{"membership only", Snapshot{
			Membership: Membership{Addresses: map[uint64]string{1: "node-1:5000"}},
		}},
		{"epoch only", Snapshot{Epoch: 7}},
		{"on-disk index only", Snapshot{OnDiskIndex: 99}},
		{"compression only", Snapshot{Compression: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Message{Type: InstallSnapshot, From: 1, To: 2, Snapshot: tc.snap}
			buf := make([]byte, m.Size())
			if _, err := m.MarshalTo(buf); !errors.Is(err, ErrSnapshotWithoutPosition) {
				t.Fatalf("MarshalTo: got %v, want ErrSnapshotWithoutPosition.\n"+
					"Encoding succeeded, which means the snapshot was discarded whole "+
					"and the peer received an InstallSnapshot carrying nothing", err)
			}
		})
	}
}

// TestMessageMarshalAcceptsGenuinelyAbsentSnapshot is the control that keeps the
// check from becoming "every message must carry a snapshot". The overwhelming
// majority of messages have a zero Snapshot and must encode exactly as before.
func TestMessageMarshalAcceptsGenuinelyAbsentSnapshot(t *testing.T) {
	t.Run("zero snapshot", func(t *testing.T) {
		m := Message{Type: Heartbeat, From: 1, To: 2, Term: 3}
		buf := make([]byte, m.Size())
		n, err := m.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		if n != HeartbeatMessageSize {
			t.Fatalf("wrote %d bytes, want %d", n, HeartbeatMessageSize)
		}
		var decoded Message
		if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
			t.Fatalf("UnmarshalFrom: %v", err)
		}
		if !decoded.Snapshot.IsEmpty() {
			t.Fatal("decoded a snapshot from a message that carried none")
		}
	})

	t.Run("reset snapshot", func(t *testing.T) {
		// A pooled message whose snapshot was populated and then Reset must be
		// indistinguishable from one that never had it.
		m := Message{Type: Heartbeat, Snapshot: Snapshot{
			Index: 5, Term: 2, Filepath: "/snap/5.dat",
			Membership: Membership{Addresses: map[uint64]string{1: "a"}},
		}}
		m.Snapshot.Reset()
		buf := make([]byte, m.Size())
		if _, err := m.MarshalTo(buf); err != nil {
			t.Fatalf("MarshalTo after Snapshot.Reset: %v", err)
		}
	})

	t.Run("position present", func(t *testing.T) {
		// Either half of the position is enough to make the snapshot real.
		for _, snap := range []Snapshot{
			{Index: 1, Filepath: "/snap/1.dat"},
			{Term: 1, Filepath: "/snap/1.dat"},
			{Index: 10, Term: 3},
		} {
			m := Message{Type: InstallSnapshot, Snapshot: snap}
			buf := make([]byte, m.Size())
			n, err := m.MarshalTo(buf)
			if err != nil {
				t.Fatalf("MarshalTo(%+v): %v", snap, err)
			}
			var decoded Message
			if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
				t.Fatalf("UnmarshalFrom(%+v): %v", snap, err)
			}
			if decoded.Snapshot.Filepath != snap.Filepath ||
				decoded.Snapshot.Index != snap.Index {
				t.Fatalf("snapshot did not round-trip: got %+v, want %+v",
					decoded.Snapshot, snap)
			}
		}
	})
}

// TestGetBytesAndGetStringApplyAZeroLimit pins the semantics of a zero limit.
//
// A zero limit admits only an empty value. Skipping the bound when maxLen is
// zero would reward forgetting to pass a field's limit with an unbounded
// length prefix straight off the wire.
func TestGetBytesAndGetStringApplyAZeroLimit(t *testing.T) {
	buf := make([]byte, 4+16)
	putBytes(buf, make([]byte, 16))

	if _, _, err := getBytes(buf, 0, ErrChunkDataTooLong); !errors.Is(err, ErrChunkDataTooLong) {
		t.Fatalf("getBytes(maxLen=0) over a 16-byte payload: got %v, want "+
			"ErrChunkDataTooLong; a zero limit must not read as unlimited", err)
	}
	if _, _, err := getString(buf, 0, ErrAddressTooLong); !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("getString(maxLen=0) over a 16-byte payload: got %v, want "+
			"ErrAddressTooLong; a zero limit must not read as unlimited", err)
	}

	// An empty value is still admissible at a zero limit: the bound is on the
	// length, and zero bytes are within it.
	empty := make([]byte, 4)
	if _, n, err := getBytes(empty, 0, ErrChunkDataTooLong); err != nil || n != 4 {
		t.Fatalf("getBytes(maxLen=0) over an empty payload: n=%d err=%v", n, err)
	}
	if s, n, err := getString(empty, 0, ErrAddressTooLong); err != nil || n != 4 || s != "" {
		t.Fatalf("getString(maxLen=0) over an empty payload: s=%q n=%d err=%v", s, n, err)
	}
}

// TestLengthPrefixPastMaxInt32IsRefusedBeforeTheLimit pins the ORDER of the two
// guards, which is the only part of the 32-bit wrap defence a 64-bit test can
// observe.
//
// The guard itself protects `4 + int(dataLen)` from wrapping negative where int
// is 32 bits. On a 64-bit host that conversion is harmless and both orderings
// end in a refusal, so no test can tell them apart from the outcome alone —
// which is exactly how getBytes came to have the guard sequenced BEHIND its
// limit check, where a caller whose own limit exceeds MaxInt32 (none today, and
// nothing stops one) skips it entirely. What is observable is the sentinel: with
// the guard first an over-MaxInt32 prefix reports ErrBufferTooSmall whatever the
// limit is, which is what getString has always done and what getBytes now
// matches.
func TestLengthPrefixPastMaxInt32IsRefusedBeforeTheLimit(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 1<<31) // MaxInt32 + 1

	if _, _, err := getBytes(buf, 10, ErrChunkDataTooLong); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBytes: got %v, want ErrBufferTooSmall; the MaxInt32 guard must "+
			"precede the limit check so it cannot be shadowed by it", err)
	}
	if _, _, err := getString(buf, 10, ErrAddressTooLong); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getString: got %v, want ErrBufferTooSmall", err)
	}
	// And with a limit above MaxInt32 — the case the guard exists for — the
	// refusal is the same rather than a narrowing conversion.
	if _, _, err := getBytes(buf, ^uint32(0), ErrChunkDataTooLong); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBytes(maxLen past MaxInt32): got %v, want ErrBufferTooSmall", err)
	}
}

// marshalEntryUnchecked encodes an Entry with the write-side type check
// BYPASSED, so the read-side half of the round-trip assertion can be driven. It mirrors
// marshalMembershipUnchecked in writeside_limits_test.go.
func marshalEntryUnchecked(e Entry) []byte {
	buf := make([]byte, e.Size())
	putUint64(buf[0:], e.Term)
	putUint64(buf[8:], e.Index)
	putUint64(buf[16:], e.Type)
	putUint64(buf[24:], e.Key)
	putUint64(buf[32:], e.ClientID)
	putUint64(buf[40:], e.SeriesID)
	putUint64(buf[48:], e.RespondedTo)
	putUint64(buf[56:], e.OwnerID)
	putUint32(buf[64:], uint32(len(e.Cmd)))
	copy(buf[entryHeaderSize:], e.Cmd)
	return buf
}
