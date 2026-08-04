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
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestMsgRecvBudget_ReserveReleaseBounds is the H2 regression test for the
// aggregate message-receive memory budget: reservations succeed only up to the
// cap, a rejected reservation restores the budget exactly, and releases free
// capacity for subsequent frames.
func TestMsgRecvBudget_ReserveReleaseBounds(t *testing.T) {
	tr := &QUICTransport{}
	tr.msgRecvBudget.Store(1000)

	if !tr.reserveMsgRecvBudget(600) {
		t.Fatal("first reserve(600) should succeed")
	}
	if !tr.reserveMsgRecvBudget(400) {
		t.Fatal("reserve(400) up to the cap should succeed")
	}
	// Budget exhausted (0 remaining): any further reservation must fail and
	// leave the budget unchanged (the rejection restores its deduction).
	if tr.reserveMsgRecvBudget(1) {
		t.Fatal("reserve(1) past the cap must fail")
	}
	if got := tr.msgRecvBudget.Load(); got != 0 {
		t.Fatalf("budget after rejected reserve = %d, want 0 (rejection must restore)", got)
	}
	// Release frees capacity for the next frame.
	tr.releaseMsgRecvBudget(400)
	if !tr.reserveMsgRecvBudget(400) {
		t.Fatal("reserve(400) after release should succeed")
	}
	tr.releaseMsgRecvBudget(600)
	tr.releaseMsgRecvBudget(400)
	if got := tr.msgRecvBudget.Load(); got != 1000 {
		t.Fatalf("budget fully released = %d, want 1000", got)
	}
}

// TestMsgRecvBudget_ConcurrentNeverOverCommits verifies that under concurrent
// reservation attempts the budget never grants more than its cap — the property
// that bounds the OOM DoS — and is fully restored after all reservations are
// released.
func TestMsgRecvBudget_ConcurrentNeverOverCommits(t *testing.T) {
	const (
		slots   = 40
		each    = 4096
		workers = 200
	)
	tr := &QUICTransport{}
	tr.msgRecvBudget.Store(slots * each)

	var attempted sync.WaitGroup
	attempted.Add(workers)
	var done sync.WaitGroup
	done.Add(workers)
	var granted atomic.Int64
	release := make(chan struct{})

	for i := 0; i < workers; i++ {
		go func() {
			defer done.Done()
			ok := tr.reserveMsgRecvBudget(each)
			attempted.Done()
			if ok {
				granted.Add(1)
				<-release // hold the reservation so concurrent grants are visible
				tr.releaseMsgRecvBudget(each)
			}
		}()
	}

	// Once every worker has attempted its reservation, the grant count is stable
	// and must not exceed the budget.
	attempted.Wait()
	if g := granted.Load(); g > slots {
		t.Fatalf("granted %d reservations of %d bytes, exceeds budget of %d slots (over-commit)", g, each, slots)
	}

	close(release)
	done.Wait()
	if got := tr.msgRecvBudget.Load(); got != slots*each {
		t.Fatalf("budget after all releases = %d, want %d (leak)", got, slots*each)
	}
}

// TestMsgRecvBudget_CompressedDeltaCharged is the H9 regression test for the
// compressed receive path: the aggregate budget must be charged with the
// DECLARED DECOMPRESSED length, not just the compressed frame length. Snappy
// achieves >1000:1 ratios, so charging only the compressed length would let a
// peer pin MaxDecompressedSize per in-flight frame while the budget barely
// moves. A frame whose declared decompressed size exceeds the remaining budget
// must be rejected, and the budget must return to its baseline afterward.
func TestMsgRecvBudget_CompressedDeltaCharged(t *testing.T) {
	const budget = 4096
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress:           ":0",
		DeploymentID:            42,
		MaxMessageReceiveMemory: budget,
		MTLSConfig:              testMTLSConfig(t),
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// 64KB of zeros compresses to a few dozen bytes: the compressed frame
	// length fits comfortably within the budget while the declared
	// decompressed size exceeds the whole budget many times over.
	raw := make([]byte, 64*1024)
	compressed := snappyEncode(nil, raw)
	if int64(len(compressed)) >= budget {
		t.Fatalf("test setup: compressed length %d must be below budget %d", len(compressed), budget)
	}

	err = tr.handleMessageFrame(nil, bytes.NewReader(compressed), uint32(len(compressed)), FlagCompressed)
	if !errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatalf("expected ErrMessageRecvMemoryBudgetExceeded for oversized declared length, got %v", err)
	}
	if got := tr.msgRecvBudget.Load(); got != budget {
		t.Fatalf("budget after rejected compressed frame = %d, want %d (reserve/release imbalance)", got, budget)
	}
}

// TestMsgRecvBudget_CompressedDeltaReleasedOnSuccess verifies the success path
// of the H9 fix: a compressed frame whose declared decompressed size fits the
// budget is processed normally, and both the compressed-length reservation and
// the decompression delta are released afterward, restoring the baseline.
func TestMsgRecvBudget_CompressedDeltaReleasedOnSuccess(t *testing.T) {
	const budget = 64 * 1024
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress:           ":0",
		DeploymentID:            42,
		MaxMessageReceiveMemory: budget,
		MTLSConfig:              testMTLSConfig(t),
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	// A batch with a highly compressible 8KB command: declared decompressed
	// length exceeds the compressed length (positive delta) but fits the
	// budget, so the frame must be accepted.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{
			{
				Type:    proto.Replicate,
				ShardID: 1,
				From:    2,
				To:      1,
				Term:    5,
				Entries: []proto.Entry{{Index: 1, Term: 5, Cmd: make([]byte, 8*1024)}},
			},
		},
	}
	rawBuf := make([]byte, mb.Size())
	n, err := mb.MarshalTo(rawBuf)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	compressed := snappyEncode(nil, rawBuf[:n])
	if len(compressed) >= n {
		t.Fatalf("test setup: compressed length %d must be below declared length %d", len(compressed), n)
	}

	err = tr.handleMessageFrame(nil, bytes.NewReader(compressed), uint32(len(compressed)), FlagCompressed)
	if err != nil {
		t.Fatalf("expected in-budget compressed frame to succeed, got %v", err)
	}
	if got := handler.batchCount.Load(); got != 1 {
		t.Fatalf("handler received %d batches, want 1", got)
	}
	if got := tr.msgRecvBudget.Load(); got != budget {
		t.Fatalf("budget after successful compressed frame = %d, want %d (reserve/release imbalance)", got, budget)
	}
}
