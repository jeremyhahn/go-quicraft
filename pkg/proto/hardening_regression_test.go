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
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

// TestGetMapRejectsValueLengthAboveMaxInt32 pins the 32-bit wrap guard that
// getBytes and getString already carry. A valLen above math.MaxInt32 becomes
// NEGATIVE when converted to int on a 32-bit platform, which passes both bound
// checks and panics on the slice expression. The guard must reject the length
// prefix itself, before any int conversion, so the verdict is identical on
// every word size.
func TestGetMapRejectsValueLengthAboveMaxInt32(t *testing.T) {
	// [count:4=1][key:8][valLen:4=0x80000000] and no value bytes.
	buf := make([]byte, 4+8+4)
	binary.LittleEndian.PutUint32(buf[0:], 1)
	binary.LittleEndian.PutUint64(buf[4:], 7)
	binary.LittleEndian.PutUint32(buf[12:], math.MaxInt32+1)

	_, _, err := getMap(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getMap with valLen %d: expected ErrBufferTooSmall from the pre-conversion guard, got %v",
			uint32(math.MaxInt32)+1, err)
	}
}

// TestGetBoolMapRejectsCountBeyondBuffer pins the sibling decoder's bound: its
// entries are fixed width, so there is no length prefix to wrap, and a count
// larger than the buffer can hold must be rejected before allocation.
func TestGetBoolMapRejectsCountBeyondBuffer(t *testing.T) {
	buf := make([]byte, 4+8)
	binary.LittleEndian.PutUint32(buf[0:], 4)
	if _, _, err := getBoolMap(buf); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBoolMap with truncated entries: expected ErrBufferTooSmall, got %v", err)
	}
}

// TestMessageBatchResetReleasesOversizedEntries pins that Reset propagates to
// the messages it retains. Without the per-message Reset the Entries capacity
// bound (maxRetainedEntries) is unreachable through the pooled batch, so one
// crafted frame pins len(Requests) * cap(Entries) * sizeof(Entry) bytes in the
// pool for the process lifetime.
func TestMessageBatchResetReleasesOversizedEntries(t *testing.T) {
	mb := &MessageBatch{
		Requests: make([]Message, 1, 2),
	}
	mb.Requests[0].Entries = make([]Entry, 0, maxRetainedEntries+1)
	mb.Requests[0].Term = 9
	mb.Requests[0].Snapshot.Index = 11

	mb.Reset()

	retained := mb.Requests[:cap(mb.Requests)][0]
	if cap(retained.Entries) > maxRetainedEntries {
		t.Fatalf("retained Entries capacity %d exceeds maxRetainedEntries %d: Reset did not reset the message",
			cap(retained.Entries), maxRetainedEntries)
	}
	if retained.Term != 0 || retained.Snapshot.Index != 0 {
		t.Fatalf("retained message not cleared: Term=%d Snapshot.Index=%d", retained.Term, retained.Snapshot.Index)
	}
}
