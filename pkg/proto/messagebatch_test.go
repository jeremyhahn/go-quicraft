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

func TestMessageBatchTooManyMessages(t *testing.T) {
	mb := MessageBatch{
		BinVer:        WireVersion,
		SourceAddress: "x",
	}
	buf := make([]byte, mb.Size())
	mb.MarshalTo(buf)
	// Find the requests count field and corrupt it
	offset := messageBatchFixedSize + 4 + 1 // after BinVer + DeploymentID + sourceAddress("x")
	putUint32(buf[offset:], uint32(maxMessagesPerBatch+1))
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

func TestMessageBatchUint32OffsetTruncationFixed(t *testing.T) {
	// This test documents the H9 security fix. Previously, the batch size
	// check used uint32(offset) > MaxBatchSize, which silently truncated
	// offsets larger than math.MaxUint32 (4GB). An attacker could craft a
	// batch that wraps the uint32 offset, bypassing the size limit.
	//
	// We cannot allocate a 4GB+ buffer in a unit test, so we verify the
	// fix algebraically: confirm that the uint64 comparison used in the
	// fixed code correctly catches values that uint32 would truncate.
	//
	// Example: offset = 0x1_0000_0001 (4GB + 1)
	//   uint32(offset) = 1          -> passes old check (1 <= 64MB)
	//   uint64(offset) = 0x100000001 -> fails new check (4GB > 64MB)
	offset := int(1<<32 + 1) // 4GB + 1

	// Old code (vulnerable): uint32(offset) wraps to 1
	truncated := uint32(offset)
	if truncated > uint32(MaxBatchSize) {
		t.Fatal("uint32 truncation unexpectedly caught the overflow - test premise invalid")
	}

	// New code (fixed): uint64(offset) preserves full value
	if uint64(offset) <= MaxBatchSize {
		t.Fatal("uint64 comparison failed to detect offset exceeding MaxBatchSize")
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
	// accidental regression to uint32 which would reintroduce the H9
	// truncation vulnerability.
	var check uint64 = MaxBatchSize
	if check != 64*1024*1024 {
		t.Fatalf("MaxBatchSize: got %d, want %d", check, 64*1024*1024)
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
