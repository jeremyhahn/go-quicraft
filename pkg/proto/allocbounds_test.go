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
	"runtime"
	"testing"
)

// allocBytesPerOp runs f n times and reports the average bytes allocated per
// call. Used to assert that a decode driven by a wire count allocates in
// proportion to the bytes actually supplied, not to the claimed count.
func allocBytesPerOp(t *testing.T, n int, f func()) uint64 {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(n)
}

// craftedBatchHeader builds the smallest well-formed MessageBatch prefix with
// an empty SourceAddress and the given request count, and no request bytes.
func craftedBatchHeader(reqCount uint32) []byte {
	buf := make([]byte, messageBatchFixedSize+4+4)
	putUint64(buf[0:], WireVersion)
	putUint32(buf[messageBatchFixedSize:], 0) // SourceAddress length
	putUint32(buf[messageBatchFixedSize+4:], reqCount)
	return buf
}

// TestMessageBatch_UnmarshalFrom_CountExceedsBuffer asserts that a request
// count the remaining buffer cannot possibly hold is rejected before the
// []Message is allocated. Before the length cross-check, this 32-byte payload
// allocated ~2.88 MB (10000 * 288-byte Message) and then returned
// ErrBufferTooSmall — a ~90,000x amplification over the frame length the
// transport's receive-memory budget charges.
func TestMessageBatch_UnmarshalFrom_CountExceedsBuffer(t *testing.T) {
	buf := craftedBatchHeader(MaxMessagesPerBatch)
	if len(buf) != 40 {
		t.Fatalf("crafted header = %d bytes, want 40", len(buf))
	}
	var mb MessageBatch
	if _, err := mb.UnmarshalFrom(buf); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom = %v, want ErrBufferTooSmall", err)
	}
	if len(mb.Requests) != 0 {
		t.Fatalf("Requests populated on error: len=%d", len(mb.Requests))
	}

	got := allocBytesPerOp(t, 2000, func() {
		var b MessageBatch
		_, _ = b.UnmarshalFrom(buf)
	})
	if got > 1024 {
		t.Fatalf("allocated %d bytes for a %d-byte payload; want <= 1024", got, len(buf))
	}
}

// TestMessageBatch_UnmarshalFrom_CountBoundaryAccepted asserts the
// cross-check does not reject counts the buffer could legitimately hold.
func TestMessageBatch_UnmarshalFrom_CountBoundaryAccepted(t *testing.T) {
	mb := MessageBatch{
		BinVer:   WireVersion,
		Requests: []Message{{Type: Heartbeat, ShardID: 1}, {Type: Heartbeat, ShardID: 2}},
	}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var got MessageBatch
	if _, err := got.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if len(got.Requests) != 2 {
		t.Fatalf("Requests = %d, want 2", len(got.Requests))
	}
}

// TestMessage_UnmarshalFrom_EntryCountExceedsBuffer asserts that an entry
// count the remaining buffer cannot hold is rejected before the []Entry is
// allocated (10000 * 80-byte Entry was ~800 KB from a 92-byte payload).
func TestMessage_UnmarshalFrom_EntryCountExceedsBuffer(t *testing.T) {
	buf := make([]byte, messageFixedSize+4)
	putUint64(buf[0:], Replicate)
	putUint32(buf[messageFixedSize:], MaxEntriesPerMessage)

	var m Message
	if _, err := m.UnmarshalFrom(buf); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom = %v, want ErrBufferTooSmall", err)
	}
	if len(m.Entries) != 0 {
		t.Fatalf("Entries populated on error: len=%d", len(m.Entries))
	}

	got := allocBytesPerOp(t, 2000, func() {
		var msg Message
		_, _ = msg.UnmarshalFrom(buf)
	})
	if got > 1024 {
		t.Fatalf("allocated %d bytes for a %d-byte payload; want <= 1024", got, len(buf))
	}
}

// craftedMembership builds a Membership fixed region with the current
// version byte and the given Addresses map count, supplying no map entry
// bytes.
func craftedMembership(count uint32) []byte {
	buf := make([]byte, membershipFixedSize+4)
	buf[0] = membershipFormatVersion
	putUint32(buf[membershipFixedSize:], count)
	return buf
}

// TestMembership_UnmarshalFrom_MapCountExceedsBuffer asserts that a map count
// the remaining buffer cannot hold is rejected before make(map...). Before the
// cross-check this 29-byte payload allocated ~437 KB.
func TestMembership_UnmarshalFrom_MapCountExceedsBuffer(t *testing.T) {
	buf := craftedMembership(MaxMembershipEntries)
	if len(buf) != membershipFixedSize+4 {
		t.Fatalf("crafted membership = %d bytes, want %d",
			len(buf), membershipFixedSize+4)
	}
	var m Membership
	if _, err := m.UnmarshalFrom(buf); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom = %v, want ErrBufferTooSmall", err)
	}

	got := allocBytesPerOp(t, 2000, func() {
		var mm Membership
		_, _ = mm.UnmarshalFrom(buf)
	})
	if got > 1024 {
		t.Fatalf("allocated %d bytes for a %d-byte payload; want <= 1024", got, len(buf))
	}
}

// TestGetBoolMap_CountExceedsBuffer asserts the bool-map decoder applies the
// same cross-check (16 bytes per entry).
func TestGetBoolMap_CountExceedsBuffer(t *testing.T) {
	buf := make([]byte, 4+8) // count prefix plus a partial entry
	putUint32(buf, MaxMembershipEntries)
	if _, _, err := getBoolMap(buf); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBoolMap = %v, want ErrBufferTooSmall", err)
	}

	got := allocBytesPerOp(t, 2000, func() {
		_, _, _ = getBoolMap(buf)
	})
	if got > 512 {
		t.Fatalf("allocated %d bytes for a %d-byte payload; want <= 512", got, len(buf))
	}
}

// TestGetMap_LargeSatisfiableCount asserts a large count the buffer genuinely
// satisfies decodes in full. Each entry is the 12-byte minimum
// ([key:8][valueLen:4=0]), so the count passes the length cross-check and every
// entry is really there — this is the case the cross-check must NOT reject, and
// the case getMap now sizes its map from in one allocation (see
// TestGetMapSizesExactly).
func TestGetMap_LargeSatisfiableCount(t *testing.T) {
	const count = 4096
	buf := make([]byte, 4+count*mapEntryMinBytes)
	putUint32(buf, count)
	for i := 0; i < count; i++ {
		off := 4 + i*mapEntryMinBytes
		putUint64(buf[off:], uint64(i))
		putUint32(buf[off+8:], 0)
	}
	m, n, err := getMap(buf)
	if err != nil {
		t.Fatalf("getMap: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("consumed %d bytes, want %d", n, len(buf))
	}
	if len(m) != count {
		t.Fatalf("map has %d entries, want %d", len(m), count)
	}
}

// TestGetMap_RejectsDuplicateKeys asserts the decoder rejects a non-canonical
// encoding it would never produce (putMap emits each key once, sorted).
func TestGetMap_RejectsDuplicateKeys(t *testing.T) {
	buf := make([]byte, 4+2*mapEntryMinBytes)
	putUint32(buf, 2)
	putUint64(buf[4:], 7)
	putUint32(buf[12:], 0)
	putUint64(buf[16:], 7)
	putUint32(buf[24:], 0)
	if _, _, err := getMap(buf); !errors.Is(err, ErrDuplicateMapKey) {
		t.Fatalf("getMap = %v, want ErrDuplicateMapKey", err)
	}
}

// TestGetBoolMap_RejectsDuplicateKeys mirrors TestGetMap_RejectsDuplicateKeys
// for the bool-map decoder.
func TestGetBoolMap_RejectsDuplicateKeys(t *testing.T) {
	buf := make([]byte, 4+2*boolMapEntryBytes)
	putUint32(buf, 2)
	putUint64(buf[4:], 9)
	putBool(buf[12:], true)
	putUint64(buf[20:], 9)
	putBool(buf[28:], false)
	if _, _, err := getBoolMap(buf); !errors.Is(err, ErrDuplicateMapKey) {
		t.Fatalf("getBoolMap = %v, want ErrDuplicateMapKey", err)
	}
}

// TestMessageBatch_Reset_DropsOversizedRequests asserts a batch grown past
// maxRetainedRequests releases the slice, so a pooled batch cannot retain
// attacker-influenced capacity for the process lifetime.
func TestMessageBatch_Reset_DropsOversizedRequests(t *testing.T) {
	t.Run("oversized capacity dropped", func(t *testing.T) {
		mb := MessageBatch{Requests: make([]Message, maxRetainedRequests+1)}
		mb.Reset()
		if cap(mb.Requests) != 0 {
			t.Fatalf("cap = %d after Reset, want 0", cap(mb.Requests))
		}
	})

	t.Run("capacity within threshold retained", func(t *testing.T) {
		mb := MessageBatch{Requests: make([]Message, 0, maxRetainedRequests)}
		mb.Reset()
		if cap(mb.Requests) != maxRetainedRequests {
			t.Fatalf("cap = %d after Reset, want %d", cap(mb.Requests), maxRetainedRequests)
		}
		if len(mb.Requests) != 0 {
			t.Fatalf("len = %d after Reset, want 0", len(mb.Requests))
		}
	})
}

// TestMessage_Reset_DropsOversizedEntries asserts a message grown past
// maxRetainedEntries releases the slice, so a message held inside a pooled
// batch cannot retain attacker-influenced capacity for the process lifetime.
func TestMessage_Reset_DropsOversizedEntries(t *testing.T) {
	t.Run("oversized capacity dropped", func(t *testing.T) {
		m := Message{Entries: make([]Entry, maxRetainedEntries+1)}
		m.Reset()
		if cap(m.Entries) != 0 {
			t.Fatalf("cap = %d after Reset, want 0", cap(m.Entries))
		}
	})

	t.Run("capacity within threshold retained", func(t *testing.T) {
		m := Message{Entries: make([]Entry, 0, maxRetainedEntries)}
		m.Reset()
		if cap(m.Entries) != maxRetainedEntries {
			t.Fatalf("cap = %d after Reset, want %d", cap(m.Entries), maxRetainedEntries)
		}
		if len(m.Entries) != 0 {
			t.Fatalf("len = %d after Reset, want 0", len(m.Entries))
		}
	})
}
