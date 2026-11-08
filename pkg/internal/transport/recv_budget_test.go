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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestMsgRecvBudget_ReserveReleaseBounds is the regression test for the
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

// TestMsgRecvBudget_CompressedDeltaReleasedOnSuccess verifies the success path
// of the fix: a compressed frame whose declared decompressed size fits the
// budget is processed normally, and both the compressed-length reservation and
// the decompression delta are released afterward, restoring the baseline.
func TestMsgRecvBudget_CompressedDeltaReleasedOnSuccess(t *testing.T) {
	// Scaled by the decode factor deliberately. The budget charges what a frame
	// can occupy while being DECODED, not its wire length, so "a frame that
	// fits" is a frame whose decode fits. Writing the multiplier out rather
	// than hard-coding a larger number keeps this fixture correct if the factor
	// ever changes, instead of silently becoming an out-of-budget test that
	// asserts the in-budget path.
	const budget = 64 * 1024 * proto.MaxDecodeMemoryFactor
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	// Config rejects a budget below one maximum-size frame, so the small
	// budget this case needs is modelled as the aggregate budget other frames
	// have already drawn down to: only what remains is set.
	tr.msgRecvBudget.Store(budget)

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

// wantMaxFrameCharge is the most one frame can reserve against the receive
// budgets: a MaxFrameSize frame at the decode factor, or a smaller compressed
// one whose declared decompressed size reaches MaxDecompressedSize.
func wantMaxFrameCharge(t *testing.T) int64 {
	t.Helper()
	var def Config
	def.SetDefaults()
	return proto.FrameDecodeCharge(int64(MaxFrameSize), int64(def.MaxDecompressedSize))
}

// TestConfigMessageReceiveMemoryDefaultsToFrameCharges pins the default
// aggregate budget to the frame it exists to admit: the configured number of
// maximum-size frames, each charged exactly as handleMessageFrame charges it.
func TestConfigMessageReceiveMemoryDefaultsToFrameCharges(t *testing.T) {
	var c Config
	c.SetDefaults()
	want := int64(config.DefaultMessageReceiveBudgetFrames) * wantMaxFrameCharge(t)
	if c.MaxMessageReceiveMemory != want {
		t.Fatalf("default MaxMessageReceiveMemory = %d, want %d (%d maximum-frame charges)",
			c.MaxMessageReceiveMemory, want, config.DefaultMessageReceiveBudgetFrames)
	}
}

// TestConfigValidateRejectsBudgetBelowOneFrame verifies that a budget that
// cannot hold one maximum-size frame is a startup error naming the minimum,
// rather than a transport that drops every peer's largest legal frame.
func TestConfigValidateRejectsBudgetBelowOneFrame(t *testing.T) {
	minimum := wantMaxFrameCharge(t)
	c := Config{ListenAddress: ":0", MaxMessageReceiveMemory: minimum - 1, MTLSConfig: testMTLSConfig(t)}
	c.SetDefaults()
	err := c.Validate()
	var tooSmall *MaxMessageReceiveMemoryTooSmallError
	if !errors.As(err, &tooSmall) {
		t.Fatalf("Validate() = %v, want *MaxMessageReceiveMemoryTooSmallError", err)
	}
	if tooSmall.Configured != minimum-1 || tooSmall.Minimum != minimum {
		t.Fatalf("error = %+v, want Configured=%d Minimum=%d", tooSmall, minimum-1, minimum)
	}
	if !errors.Is(err, ErrInvalidMaxMessageReceiveMemory) {
		t.Fatalf("Validate() = %v, want it to match ErrInvalidMaxMessageReceiveMemory", err)
	}

	c.MaxMessageReceiveMemory = minimum
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() with exactly one maximum-frame charge = %v, want nil", err)
	}
}

// firstReadReader signals once its first Read is called. handleMessageFrame
// takes every reservation before it reads a frame body, so the first Read
// marks the point at which a frame has either been admitted (and holds its
// reservations) or refused (and is draining).
type firstReadReader struct {
	r    io.Reader
	once sync.Once
	read chan struct{}
}

func (f *firstReadReader) Read(p []byte) (int, error) {
	f.once.Do(func() { close(f.read) })
	return f.r.Read(p)
}

// TestMsgRecvBudget_MaxFramePeerDoesNotStallOthers is the regression test for
// one peer wedging the whole receive path under the DEFAULT configuration.
//
// Peer A opens as many streams as the default aggregate budget has maximum-size
// frames, declares a maximum-size frame on each, and withholds every body.
// Before the budget was derived from the frame charge, and while the per-peer
// sub-budget was charged in wire bytes against an aggregate charged at the
// decode factor, two such frames reserved the entire 256 MB default and every
// other peer's frames were dropped until A's read deadline. A refused frame
// also kept its aggregate reservation while its body drained, so the per-peer
// cap could not help even when it fired.
//
// Now the per-peer cap admits A's first maximum-size frame, refuses the rest
// without holding anything, and peer B's ordinary frame is processed.
func TestMsgRecvBudget_MaxFramePeerDoesNotStallOthers(t *testing.T) {
	tr, peer, handler, _, cleanup := transportPair(t, 42)
	defer cleanup()
	connA, err := tr.getConnection(peer.Addr().String(), "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	var def Config
	def.SetDefaults()
	budget := tr.cfg.MaxMessageReceiveMemory
	if budget != def.MaxMessageReceiveMemory {
		t.Fatalf("test setup: transport budget %d is not the default %d", budget, def.MaxMessageReceiveMemory)
	}
	frameCharge := proto.DecodeCharge(int64(MaxFrameSize))
	attempts := int(budget / frameCharge)
	if attempts < 2 {
		t.Fatalf("test setup: default budget %d holds %d maximum-size frames, want >= 2", budget, attempts)
	}

	writers := make([]*io.PipeWriter, attempts)
	results := make(chan error, attempts)
	for i := range attempts {
		pr, pw := io.Pipe()
		writers[i] = pw
		fr := &firstReadReader{r: pr, read: make(chan struct{})}
		go func() {
			results <- tr.handleMessageFrame(connA, fr, MaxFrameSize, FlagNone)
		}()
		select {
		case <-fr.read:
		case <-time.After(10 * time.Second):
			t.Fatalf("attempt %d never reached its body read", i)
		}
	}
	defer func() {
		for _, pw := range writers {
			_ = pw.CloseWithError(io.ErrUnexpectedEOF)
		}
		for range attempts {
			<-results
		}
		if got := tr.msgRecvBudget.Load(); got != budget {
			t.Errorf("aggregate budget after peer A's frames ended = %d, want %d (leak)", got, budget)
		}
		if v, ok := tr.connMsgBudget.Load(msgBudgetKey(connA)); ok {
			if got := v.(*peerMsgBudget).reserved.Load(); got != 0 {
				t.Errorf("peer A's reservation after its frames ended = %d, want 0 (leak)", got)
			}
		}
	}()

	if got, want := tr.recvBudgetDrops.Load(), uint64(attempts-1); got != want {
		t.Fatalf("peer A had %d of %d maximum-size frames refused, want %d: the per-peer cap "+
			"must admit exactly one at the default budget", got, attempts, want)
	}

	// Peer B: an ordinary replication frame, from a connection that holds none
	// of the budget.
	mb := proto.MessageBatch{
		BinVer:       proto.WireVersion,
		DeploymentID: 42,
		Requests: []proto.Message{{
			Type: proto.Replicate, ShardID: 1, From: 2, To: 1, Term: 5,
			Entries: []proto.Entry{{Index: 1, Term: 5, Cmd: []byte("peer-b")}},
		}},
	}
	payload := make([]byte, mb.Size())
	n, err := mb.MarshalTo(payload)
	if err != nil {
		t.Fatalf("MarshalTo failed: %v", err)
	}
	if err := tr.handleMessageFrame(nil, bytes.NewReader(payload[:n]), uint32(n), FlagNone); err != nil {
		t.Fatalf("peer B's frame = %v while peer A held maximum-size frames, want nil: "+
			"one peer stalled every other peer's receive path", err)
	}
	if got := handler.batchCount.Load(); got != 1 {
		t.Fatalf("handler received %d batches, want peer B's 1", got)
	}
}

// TestMsgRecvBudget_RefusedFrameHoldsNothingWhileDraining pins the other half
// of the reservation order: the per-peer sub-budget is reserved first, so a
// frame the aggregate then refuses must hand its per-peer reservation back
// BEFORE draining its body. A peer whose bodies never arrive would otherwise
// hold its sub-budget through every drain.
func TestMsgRecvBudget_RefusedFrameHoldsNothingWhileDraining(t *testing.T) {
	tr, conn := liveConn(t)
	tr.msgRecvBudget.Store(0) // aggregate exhausted by other peers

	pr, pw := io.Pipe()
	fr := &firstReadReader{r: pr, read: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		result <- tr.handleMessageFrame(conn, fr, 4096, FlagNone)
	}()
	select {
	case <-fr.read:
	case <-time.After(10 * time.Second):
		t.Fatal("refused frame never reached its body drain")
	}

	if v, ok := tr.connMsgBudget.Load(msgBudgetKey(conn)); ok {
		if got := v.(*peerMsgBudget).reserved.Load(); got != 0 {
			t.Fatalf("per-peer reservation while the refused frame drains = %d, want 0", got)
		}
	}

	if _, err := pw.Write(make([]byte, 4096)); err != nil {
		t.Fatalf("writing the frame body: %v", err)
	}
	if err := <-result; !errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatalf("handleMessageFrame = %v, want ErrMessageRecvMemoryBudgetExceeded", err)
	}
	if got := tr.msgRecvBudget.Load(); got != 0 {
		t.Fatalf("aggregate budget after the refusal = %d, want 0 (reserve/release imbalance)", got)
	}
}
