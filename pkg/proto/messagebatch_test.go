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
	"math"
	"reflect"
	"testing"
)

func TestMessageBatchMarshalUnmarshalRoundTrip(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  12345,
		SourceAddress: "10.0.0.1:5000",
		Requests: []Message{
			{
				Type:    Replicate,
				From:    1,
				To:      2,
				ShardID: 100,
				Term:    5,
				Entries: []Entry{
					{Term: 5, Index: 100, Type: EntryNormal, Cmd: []byte("cmd1")},
				},
			},
			{
				Type: HeartbeatResp,
				From: 2,
				To:   1,
				Term: 5,
			},
		},
	}
	buf := make([]byte, mb.Size())
	n, err := mb.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != mb.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, mb.Size())
	}

	var mb2 MessageBatch
	consumed, err := mb2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertMessageBatchEqual(t, &mb, &mb2)
}

func TestMessageBatchMarshalUnmarshalEmpty(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  0,
		SourceAddress: "",
	}
	buf := make([]byte, mb.Size())
	mb.MarshalTo(buf)
	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if len(mb2.Requests) != 0 {
		t.Fatalf("Requests: got len %d, want 0", len(mb2.Requests))
	}
}

func TestMessageBatchMarshalUnmarshalMultipleMessages(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "sender:5000",
		Requests:      make([]Message, 10),
	}
	for i := range mb.Requests {
		mb.Requests[i] = Message{
			Type:    Heartbeat,
			From:    1,
			To:      uint64(i + 2),
			ShardID: uint64(100 + i),
			Term:    5,
		}
	}
	buf := make([]byte, mb.Size())
	mb.MarshalTo(buf)
	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if len(mb2.Requests) != 10 {
		t.Fatalf("Requests: got %d, want 10", len(mb2.Requests))
	}
	assertMessageBatchEqual(t, &mb, &mb2)
}

func TestMessageBatchUnsupportedVersion(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "x",
	}
	buf := make([]byte, mb.Size())
	mb.MarshalTo(buf)
	// Corrupt version to future version
	putUint64(buf[0:], WireVersion+1)
	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrUnsupportedVersion", err)
	}
}

func TestWireWindowIsAClosedSet(t *testing.T) {
	w := DefaultWireWindow()
	if w.Current != WireVersion || w.Previous != PreviousWireVersion {
		t.Fatalf("DefaultWireWindow() = %+v", w)
	}
	if !AcceptsWireVersion(WireVersion) {
		t.Fatal("this build must accept its own WireVersion")
	}
	for _, v := range []uint64{0, WireVersion + 1, WireVersion + 2, ^uint64(0)} {
		if AcceptsWireVersion(v) {
			t.Fatalf("AcceptsWireVersion(%d) = true outside the set", v)
		}
	}
	// A two-member window accepts exactly its members.
	next := WireWindow{Current: 3, Previous: 2}
	for v, want := range map[uint64]bool{1: false, 2: true, 3: true, 4: false} {
		if got := next.Accepts(v); got != want {
			t.Fatalf("WireWindow{3,2}.Accepts(%d) = %v, want %v", v, got, want)
		}
	}
}

func TestMessageBatchUnmarshalFromWindow(t *testing.T) {
	w := WireWindow{Current: WireVersion + 1, Previous: WireVersion}
	for _, v := range []uint64{WireVersion, WireVersion + 1, WireVersion + 2} {
		mb := MessageBatch{BinVer: v, DeploymentID: 7, MaxBinVer: WireVersion + 1, SourceAddress: "a"}
		buf := make([]byte, mb.Size())
		if _, err := mb.MarshalTo(buf); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		var got MessageBatch
		_, err := got.UnmarshalFromWindow(buf, w)
		if w.Accepts(v) {
			if err != nil {
				t.Fatalf("BinVer %d in window: %v", v, err)
			}
			if got.MaxBinVer != WireVersion+1 || got.BinVer != v {
				t.Fatalf("round trip lost versions: %+v", got)
			}
		} else if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("BinVer %d outside window: err = %v, want ErrUnsupportedVersion", v, err)
		}
		// This build's own window refuses the next version.
		if _, err := got.UnmarshalFrom(buf); (err == nil) != AcceptsWireVersion(v) {
			t.Fatalf("UnmarshalFrom(BinVer %d) err = %v", v, err)
		}
	}
}

func TestMessageBatchTooManyMessages(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		SourceAddress: "x",
	}
	buf := make([]byte, mb.Size())
	mb.MarshalTo(buf)
	// Find the requests count field and corrupt it
	offset := messageBatchFixedSize + 4 + 1 // after BinVer + DeploymentID + sourceAddress("x")
	putUint32(buf[offset:], uint32(MaxMessagesPerBatch+1))
	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrTooManyMessages) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrTooManyMessages", err)
	}
}

func TestMessageBatchBufferTooSmallMarshal(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		SourceAddress: "x",
	}
	buf := make([]byte, mb.Size()-1)
	_, err := mb.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMessageBatchBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, messageBatchFixedSize-1)
	var mb MessageBatch
	_, err := mb.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMessageBatchReset(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "addr",
		Requests:      []Message{{Type: Heartbeat}},
	}
	mb.Reset()
	if mb.BinVer != 0 || mb.DeploymentID != 0 || mb.SourceAddress != "" {
		t.Fatal("Reset did not clear scalar fields")
	}
	if len(mb.Requests) != 0 {
		t.Fatalf("Reset: len(Requests) = %d, want 0", len(mb.Requests))
	}
	if cap(mb.Requests) == 0 {
		t.Fatal("Reset: cap(Requests) = 0, want preserved capacity")
	}
}

func TestMessageBatchRequestsCountTruncated(t *testing.T) {
	// Buffer has BinVer, DeploymentID, and source address but no requests count
	mb := MessageBatch{
		BinVer:        WireVersion,
		SourceAddress: "x",
	}
	buf := make([]byte, mb.Size())
	mb.MarshalTo(buf)
	// Truncate just before requests count
	truncated := messageBatchFixedSize + 4 + 1 // source address = "x" (4 len + 1 char)
	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf[:truncated])
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMessageBatchUnmarshalFromReusesCapacity(t *testing.T) {
	// Build a batch with 5 messages, marshal it.
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  42,
		SourceAddress: "10.0.0.1:5000",
		Requests:      make([]Message, 5),
	}
	for i := range mb.Requests {
		mb.Requests[i] = Message{
			Type:    Heartbeat,
			From:    1,
			To:      uint64(i + 2),
			ShardID: uint64(100 + i),
			Term:    3,
		}
	}
	buf5 := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf5); err != nil {
		t.Fatalf("MarshalTo 5: %v", err)
	}

	// First unmarshal: allocates the slice.
	var dst MessageBatch
	if _, err := dst.UnmarshalFrom(buf5); err != nil {
		t.Fatalf("UnmarshalFrom 5: %v", err)
	}
	if len(dst.Requests) != 5 {
		t.Fatalf("len(Requests) = %d, want 5", len(dst.Requests))
	}
	capAfterFirst := cap(dst.Requests)

	// Build a smaller batch with 3 messages.
	mb.Requests = mb.Requests[:3]
	buf3 := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf3); err != nil {
		t.Fatalf("MarshalTo 3: %v", err)
	}

	// Reset preserves capacity.
	dst.Reset()
	if cap(dst.Requests) != capAfterFirst {
		t.Fatalf("Reset lost capacity: cap = %d, want %d", cap(dst.Requests), capAfterFirst)
	}

	// Second unmarshal: reuses existing capacity (3 <= 5).
	if _, err := dst.UnmarshalFrom(buf3); err != nil {
		t.Fatalf("UnmarshalFrom 3: %v", err)
	}
	if len(dst.Requests) != 3 {
		t.Fatalf("len(Requests) = %d, want 3", len(dst.Requests))
	}
	if cap(dst.Requests) != capAfterFirst {
		t.Fatalf("capacity changed: got %d, want %d (reuse failed)", cap(dst.Requests), capAfterFirst)
	}

	// Verify correctness of the 3-message unmarshal.
	assertMessageBatchEqual(t, &mb, &dst)
}

func TestMessageBatchUnmarshalFromGrowsWhenNeeded(t *testing.T) {
	// Build a batch with 2 messages.
	mb2 := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "node:9000",
		Requests: []Message{
			{Type: Heartbeat, From: 1, To: 2, Term: 1},
			{Type: HeartbeatResp, From: 2, To: 1, Term: 1},
		},
	}
	buf2 := make([]byte, mb2.Size())
	if _, err := mb2.MarshalTo(buf2); err != nil {
		t.Fatalf("MarshalTo 2: %v", err)
	}

	// First unmarshal: allocates for 2.
	var dst MessageBatch
	if _, err := dst.UnmarshalFrom(buf2); err != nil {
		t.Fatalf("UnmarshalFrom 2: %v", err)
	}
	capAfterFirst := cap(dst.Requests)
	if capAfterFirst < 2 {
		t.Fatalf("cap(Requests) = %d, want >= 2", capAfterFirst)
	}

	// Build a larger batch with 8 messages.
	mb8 := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "node:9000",
		Requests:      make([]Message, 8),
	}
	for i := range mb8.Requests {
		mb8.Requests[i] = Message{Type: Heartbeat, From: 1, To: uint64(i + 2), Term: 1}
	}
	buf8 := make([]byte, mb8.Size())
	if _, err := mb8.MarshalTo(buf8); err != nil {
		t.Fatalf("MarshalTo 8: %v", err)
	}

	// Reset and unmarshal larger batch: must grow.
	dst.Reset()
	if _, err := dst.UnmarshalFrom(buf8); err != nil {
		t.Fatalf("UnmarshalFrom 8: %v", err)
	}
	if len(dst.Requests) != 8 {
		t.Fatalf("len(Requests) = %d, want 8", len(dst.Requests))
	}
	if cap(dst.Requests) < 8 {
		t.Fatalf("cap(Requests) = %d, want >= 8", cap(dst.Requests))
	}
	assertMessageBatchEqual(t, &mb8, &dst)
}

func TestMessageBatchUnmarshalFromEmptyPreservesCapacity(t *testing.T) {
	// Unmarshal a non-empty batch first.
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "x",
		Requests:      []Message{{Type: Heartbeat, From: 1, To: 2, Term: 1}},
	}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var dst MessageBatch
	if _, err := dst.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	capAfterFirst := cap(dst.Requests)

	// Now unmarshal an empty batch.
	empty := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  2,
		SourceAddress: "y",
	}
	emptyBuf := make([]byte, empty.Size())
	if _, err := empty.MarshalTo(emptyBuf); err != nil {
		t.Fatalf("MarshalTo empty: %v", err)
	}

	if _, err := dst.UnmarshalFrom(emptyBuf); err != nil {
		t.Fatalf("UnmarshalFrom empty: %v", err)
	}
	if len(dst.Requests) != 0 {
		t.Fatalf("len(Requests) = %d, want 0", len(dst.Requests))
	}
	if cap(dst.Requests) != capAfterFirst {
		t.Fatalf("empty unmarshal lost capacity: got %d, want %d", cap(dst.Requests), capAfterFirst)
	}
}

func TestMessageBatchUnmarshalFromBatchSizeLimitExceeded(t *testing.T) {
	// Craft a batch with messages carrying large Entry.Cmd payloads that
	// exceed MaxBatchSize (64MB) in aggregate. This verifies the cumulative
	// size check rejects oversized batches.
	//
	// Each message has one entry with a 7MB Cmd payload. With 10 such
	// messages (10 * ~7MB = ~70MB), the cumulative offset exceeds the
	// 64MB MaxBatchSize limit. The decoder must reject this.
	const cmdSize = 7 * 1024 * 1024 // 7MB per entry
	const msgCount = 10             // 10 * 7MB = 70MB > 64MB limit

	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "x",
		Requests:      make([]Message, msgCount),
	}
	for i := range mb.Requests {
		mb.Requests[i] = Message{
			Type:    Replicate,
			From:    1,
			To:      2,
			ShardID: 1,
			Term:    1,
			Entries: []Entry{
				{
					Term:  1,
					Index: uint64(i + 1),
					Type:  EntryNormal,
					Cmd:   make([]byte, cmdSize),
				},
			},
		}
	}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBatchSizeLimitExceeded) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBatchSizeLimitExceeded", err)
	}
}

func TestMessageBatchUnmarshalFromBatchSizeBoundary(t *testing.T) {
	// Verify that a batch just under MaxBatchSize succeeds, while a batch
	// that crosses the limit is rejected. Uses multiple messages with
	// entries that individually stay under MaxEntrySize (8MB) but
	// collectively approach MaxBatchSize (64MB).
	//
	// Strategy: use 8 messages, each with one 7MB entry = 56MB total
	// payload. With wire overhead this stays under 64MB. Then add one
	// more message to push it over.
	const cmdSize = 7 * 1024 * 1024 // 7MB per entry, under MaxEntrySize (8MB)
	const safeCount = 8             // 8 * ~7MB = ~56MB < 64MB

	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "x",
		Requests:      make([]Message, safeCount),
	}
	for i := range mb.Requests {
		mb.Requests[i] = Message{
			Type:    Replicate,
			From:    1,
			To:      2,
			ShardID: 1,
			Term:    1,
			Entries: []Entry{
				{
					Term:  1,
					Index: uint64(i + 1),
					Type:  EntryNormal,
					Cmd:   make([]byte, cmdSize),
				},
			},
		}
	}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if mb.Size() >= int(MaxBatchSize) {
		t.Fatalf("test premise invalid: %d messages already >= MaxBatchSize (%d)",
			safeCount, MaxBatchSize)
	}

	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom (under limit): unexpected error: %v", err)
	}

	// Add 2 more messages to push total over 64MB.
	overCount := safeCount + 2
	mbOver := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "x",
		Requests:      make([]Message, overCount),
	}
	for i := range mbOver.Requests {
		mbOver.Requests[i] = Message{
			Type:    Replicate,
			From:    1,
			To:      2,
			ShardID: 1,
			Term:    1,
			Entries: []Entry{
				{
					Term:  1,
					Index: uint64(i + 1),
					Type:  EntryNormal,
					Cmd:   make([]byte, cmdSize),
				},
			},
		}
	}
	bufOver := make([]byte, mbOver.Size())
	if _, err := mbOver.MarshalTo(bufOver); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var mb3 MessageBatch
	_, err = mb3.UnmarshalFrom(bufOver)
	if !errors.Is(err, ErrBatchSizeLimitExceeded) {
		t.Fatalf("UnmarshalFrom (over limit): got %v, want ErrBatchSizeLimitExceeded", err)
	}
}

// TestExceedsMaxBatchSizeDoesNotTruncate drives the PRODUCTION comparison at an
// offset UnmarshalFrom cannot be driven to.
//
// Asserting uint32(1<<32+1) == 1 and uint64(1<<32+1) > MaxBatchSize instead
// would check facts about Go's arithmetic, true no matter what this package
// does, so such a test could not fail. The hazard is a truncating cast inside
// the decoder's cumulative-size gate, and reaching it
// through UnmarshalFrom needs a 4 GB frame no test can allocate; calling the
// gate directly is what makes it testable at all.
func TestExceedsMaxBatchSizeDoesNotTruncate(t *testing.T) {
	if math.MaxInt < math.MaxInt64 {
		t.Skip("int is 32-bit on this platform; an offset past 4 GB is unrepresentable")
	}
	// Written through a variable so the shift is not a constant expression,
	// which would fail to compile on a 32-bit platform before the skip runs.
	// Shifted through a uint64 so vet's shift check passes on a 32-bit int
	// as well; the skip above already guarantees the conversion is lossless.
	one := uint64(1)
	fourGB := int(one << 32)

	for _, tc := range []struct {
		name   string
		offset int
		want   bool
	}{
		{"empty", 0, false},
		{"just under the limit", int(MaxBatchSize) - 1, false},
		{"exactly the limit", int(MaxBatchSize), false},
		{"one byte over", int(MaxBatchSize) + 1, true},
		// The truncation case. uint32(4GB+1) is 1, which is under MaxBatchSize, so a
		// decoder that narrows the offset waves this through and keeps parsing.
		{"4GB+1 truncates to 1 under a uint32 cast", fourGB + 1, true},
		{"4GB+64MB truncates to 64MB under a uint32 cast", fourGB + int(MaxBatchSize), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := exceedsMaxBatchSize(tc.offset); got != tc.want {
				t.Fatalf("exceedsMaxBatchSize(%d) = %v, want %v; an offset of %d "+
					"narrows to %d as a uint32, which is how a batch decodes past "+
					"MaxBatchSize without the gate noticing",
					tc.offset, got, tc.want, tc.offset, uint32(tc.offset))
			}
		})
	}
}

// TestMessageBatchUnmarshalEnforcesMaxBatchSize is the call-site half: the gate
// above is only worth having if the decoder actually consults it. This drives a
// real frame past MaxBatchSize, which costs a 64 MB buffer and is the smallest
// input that reaches the check.
//
// It cannot catch the truncating cast (a 64 MB offset does not overflow a
// uint32) and does not try to; it catches the gate being removed, weakened, or
// moved out of the request loop.
func TestMessageBatchUnmarshalEnforcesMaxBatchSize(t *testing.T) {
	// One message carrying one maximum-size entry. Cmd bytes are left zero:
	// the decoder aliases them, so the frame is the only large allocation.
	const msgSize = messageFixedSize + 4 + entryHeaderSize + int(MaxEntrySize) + 8
	// Enough messages to carry the cumulative offset past the limit.
	count := int(MaxBatchSize)/msgSize + 1
	headerSize := messageBatchFixedSize + 4 + 4

	buf := make([]byte, headerSize+count*msgSize)
	putUint64(buf[0:], WireVersion)
	putUint32(buf[messageBatchFixedSize:], 0) // SourceAddress length
	putUint32(buf[messageBatchFixedSize+4:], uint32(count))
	for i := 0; i < count; i++ {
		off := headerSize + i*msgSize
		putUint64(buf[off:], Replicate)
		putUint32(buf[off+messageFixedSize:], 1) // one entry
		entry := off + messageFixedSize + 4
		putUint64(buf[entry:], 1)   // Term
		putUint64(buf[entry+8:], 1) // Index
		putUint64(buf[entry+16:], EntryNormal)
		putUint32(buf[entry+64:], MaxEntrySize)
		// HasSnapshot is already zero.
	}

	var mb MessageBatch
	if _, err := mb.UnmarshalFrom(buf); !errors.Is(err, ErrBatchSizeLimitExceeded) {
		t.Fatalf("UnmarshalFrom(%d-byte batch, limit %d): got %v, want "+
			"ErrBatchSizeLimitExceeded", len(buf), MaxBatchSize, err)
	}

	// Control: the same bytes one message short must decode cleanly, so the
	// test above is failing on the limit and not on the frame's shape.
	putUint32(buf[messageBatchFixedSize+4:], uint32(count-1))
	var ok MessageBatch
	if _, err := ok.UnmarshalFrom(buf[:headerSize+(count-1)*msgSize]); err != nil {
		t.Fatalf("UnmarshalFrom(batch just under the limit): %v", err)
	}
	if len(ok.Requests) != count-1 {
		t.Fatalf("Requests = %d, want %d", len(ok.Requests), count-1)
	}
}

func TestMessageBatchUnmarshalFromCraftedOffsetBeyondBuffer(t *testing.T) {
	// Craft a buffer where the message count claims more messages than the
	// buffer actually contains. The decoder must return ErrBufferTooSmall
	// (from individual Message.UnmarshalFrom) rather than panicking with
	// an index-out-of-range on buf[offset:].
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  1,
		SourceAddress: "x",
		Requests: []Message{
			{Type: Heartbeat, From: 1, To: 2, Term: 1},
		},
	}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	// Corrupt the requests count to claim 5 messages when only 1 exists.
	reqCountOffset := messageBatchFixedSize + 4 + 1 // after addr "x"
	putUint32(buf[reqCountOffset:], 5)

	var mb2 MessageBatch
	_, err := mb2.UnmarshalFrom(buf)
	if err == nil {
		t.Fatal("UnmarshalFrom: expected error for corrupted message count, got nil")
	}
	// Should get ErrBufferTooSmall when trying to read message 2+
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMessageBatchUnmarshalFromMaxBatchSizeIsUint64(t *testing.T) {
	// Verify the MaxBatchSize constant is typed as uint64. This prevents
	// accidental regression to uint32 which would reintroduce the offset
	// truncation vulnerability. The reflected kind is the assertion: a
	// `var check uint64 = MaxBatchSize` declaration says the same thing, but
	// reads as a redundant type annotation rather than as the check it is.
	if got := reflect.TypeOf(MaxBatchSize); got.Kind() != reflect.Uint64 {
		t.Fatalf("MaxBatchSize is %v (kind %v), want a uint64", got, got.Kind())
	}
	if MaxBatchSize != 64*1024*1024 {
		t.Fatalf("MaxBatchSize: got %d, want %d", MaxBatchSize, uint64(64*1024*1024))
	}
}

func BenchmarkMessageBatchUnmarshalFrom(b *testing.B) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		DeploymentID:  42,
		SourceAddress: "10.0.0.1:5000",
		Requests:      make([]Message, 10),
	}
	for i := range mb.Requests {
		mb.Requests[i] = Message{
			Type:    Heartbeat,
			From:    1,
			To:      uint64(i + 2),
			ShardID: uint64(100 + i),
			Term:    5,
		}
	}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		b.Fatalf("MarshalTo: %v", err)
	}

	var dst MessageBatch
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		dst.Reset()
		if _, err := dst.UnmarshalFrom(buf); err != nil {
			b.Fatalf("UnmarshalFrom: %v", err)
		}
	}
}

func assertMessageBatchEqual(t *testing.T, want, got *MessageBatch) {
	t.Helper()
	if got.BinVer != want.BinVer {
		t.Fatalf("BinVer: got %d, want %d", got.BinVer, want.BinVer)
	}
	if got.DeploymentID != want.DeploymentID {
		t.Fatalf("DeploymentID: got %d, want %d", got.DeploymentID, want.DeploymentID)
	}
	if got.SourceAddress != want.SourceAddress {
		t.Fatalf("SourceAddress: got %q, want %q", got.SourceAddress, want.SourceAddress)
	}
	if len(got.Requests) != len(want.Requests) {
		t.Fatalf("Requests: got %d, want %d", len(got.Requests), len(want.Requests))
	}
	for i := range want.Requests {
		assertMessageEqual(t, &want.Requests[i], &got.Requests[i])
	}
}
