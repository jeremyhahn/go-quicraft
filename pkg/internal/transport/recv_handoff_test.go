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

package transport

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// liveConn establishes a real loopback QUIC connection and returns the
// sender-side *quic.Conn. The connection's context is live (not cancelled), so
// it is a valid inboundConn argument for handoffToSnapSem's select. The caller
// runs under the transportPair cleanup, so no extra teardown is needed.
func liveConn(t *testing.T) (*QUICTransport, *quic.Conn) {
	t.Helper()
	t1, t2, _, _, cleanup := transportPair(t, 42)
	t.Cleanup(cleanup)
	conn, err := t1.getConnection(t2.Addr().String())
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	return t1, conn
}

// TestHandoffToSnapSem_NormalPath verifies the normal snapshot handoff: the
// caller enters holding one streamSem slot, and across the handoff exactly the
// streamSem slot is released and exactly one snapSem slot is acquired. Neither
// semaphore is leaked: streamSem ends empty, snapSem ends with one slot held by
// the caller.
func TestHandoffToSnapSem_NormalPath(t *testing.T) {
	tr, conn := liveConn(t)

	streamSem := make(chan struct{}, 1)
	snapSem := make(chan struct{}, 1)

	// Caller holds one streamSem slot on entry, as handleConnection guarantees.
	streamSem <- struct{}{}

	acquired := tr.handoffToSnapSem(streamSem, snapSem, conn)
	if !acquired {
		t.Fatal("normal path: handoff should acquire a snapSem slot")
	}
	if len(streamSem) != 0 {
		t.Fatalf("normal path: streamSem must be released exactly once, len=%d", len(streamSem))
	}
	if len(snapSem) != 1 {
		t.Fatalf("normal path: snapSem must hold exactly one slot, len=%d", len(snapSem))
	}

	// The caller owns the snapSem slot and releases it after the transfer.
	<-snapSem
	if len(snapSem) != 0 {
		t.Fatalf("normal path: snapSem must be empty after release, len=%d", len(snapSem))
	}
}

// TestHandoffToSnapSem_SnapSemFull verifies the drop path: when the snapshot
// budget is exhausted, the handoff still releases the held streamSem slot,
// returns false (so the caller drops the snapshot), and does not acquire or
// leak a snapSem slot. The pre-existing snapSem occupancy is left untouched.
func TestHandoffToSnapSem_SnapSemFull(t *testing.T) {
	tr, conn := liveConn(t)

	streamSem := make(chan struct{}, 1)
	snapSem := make(chan struct{}, 1)

	// Caller holds one streamSem slot on entry.
	streamSem <- struct{}{}
	// Snapshot budget is already fully consumed by another in-flight transfer.
	snapSem <- struct{}{}

	acquired := tr.handoffToSnapSem(streamSem, snapSem, conn)
	if acquired {
		t.Fatal("snapSem-full path: handoff must not acquire a slot when budget is exhausted")
	}
	if len(streamSem) != 0 {
		t.Fatalf("snapSem-full path: streamSem must still be released, len=%d", len(streamSem))
	}
	// snapSem occupancy is unchanged: the one pre-existing slot, no extra
	// acquire, no spurious release.
	if len(snapSem) != 1 {
		t.Fatalf("snapSem-full path: snapSem occupancy must be unchanged at 1, len=%d", len(snapSem))
	}
}

// TestHandoffToSnapSem_ConnContextCancelled verifies that when the connection
// is closing, the handoff releases streamSem, returns false, and acquires no
// snapSem slot. This exercises the inboundConn.Context().Done() branch of the
// select deterministically by closing the connection before the handoff.
func TestHandoffToSnapSem_ConnContextCancelled(t *testing.T) {
	tr, conn := liveConn(t)

	// Fill snapSem so the non-blocking send cannot succeed; this forces the
	// select to choose between the cancelled-context case and default. With the
	// context cancelled, the cancellation case is taken.
	streamSem := make(chan struct{}, 1)
	snapSem := make(chan struct{}, 1)
	streamSem <- struct{}{}
	snapSem <- struct{}{}

	conn.CloseWithError(0, "test: connection closing during handoff")
	// Wait for the context to actually cancel so the branch is deterministic.
	waitForChan(t, conn.Context().Done(), 5*time.Second,
		"connection context not cancelled after CloseWithError")

	acquired := tr.handoffToSnapSem(streamSem, snapSem, conn)
	if acquired {
		t.Fatal("cancelled path: handoff must not acquire a slot when connection is closing")
	}
	if len(streamSem) != 0 {
		t.Fatalf("cancelled path: streamSem must be released, len=%d", len(streamSem))
	}
	if len(snapSem) != 1 {
		t.Fatalf("cancelled path: snapSem occupancy must be unchanged at 1, len=%d", len(snapSem))
	}
}

// TestDataStreamNeverUsesSnapSem verifies invariant (c): a normal
// (non-snapshot) data stream only ever consumes the data-stream budget and
// never enters the snapshot handoff. It drives a real data message end to end
// through handleConnection/handleStream over a live loopback connection and
// asserts the receiver's snapshot handler is never invoked, which is the
// externally observable proof that snapSem (touched only by handoffToSnapSem,
// reachable only under FlagSnapshot) was never used.
func TestDataStreamNeverUsesSnapSem(t *testing.T) {
	t1, _, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	// Send several normal data frames so the receive path opens data streams.
	for i := 0; i < 5; i++ {
		t1.Send([]proto.Message{
			{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: uint64(i + 1),
				Entries: []proto.Entry{{Index: uint64(i + 1), Term: uint64(i + 1), Cmd: []byte("data")}}},
		})
		handler2.waitBatch(t, 10*time.Second)
	}

	// A data stream must never be dispatched to the snapshot handler. If the
	// snapshot handoff had ever been taken for a data frame, HandleSnapshot
	// would have fired.
	if got := handler2.snapshotCount.Load(); got != 0 {
		t.Fatalf("data streams must not invoke the snapshot path, snapshotCount=%d", got)
	}
	if snaps := handler2.getSnapshots(); len(snaps) != 0 {
		t.Fatalf("data streams must not produce snapshots, got %d", len(snaps))
	}
}
