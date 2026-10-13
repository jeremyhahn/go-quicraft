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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// gatedHandler holds every HandleMessage call until release is closed, and
// records the term of each message it is handed. Holding the first call keeps
// the receiving stream's read loop parked, so every frame written after it is
// sitting unread in the receiver's stream buffer: exactly the state a vote
// request is in when the reply that triggers an epoch eviction overtakes it.
type gatedHandler struct {
	*testHandler // every other MessageHandler method

	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu    sync.Mutex
	terms []uint64
	got   chan struct{}
}

func newGatedHandler() *gatedHandler {
	return &gatedHandler{
		testHandler: newTestHandler(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
		got:         make(chan struct{}, 16),
	}
}

func (h *gatedHandler) HandleMessage(batch proto.MessageBatch, _ PeerAuth) error {
	h.once.Do(func() { close(h.entered) })
	<-h.release
	h.mu.Lock()
	for i := range batch.Requests {
		h.terms = append(h.terms, batch.Requests[i].Term)
	}
	h.mu.Unlock()
	h.got <- struct{}{}
	return nil
}

func (h *gatedHandler) received() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.terms...)
}

// TestEvictStaleOutboundOnRecv_LiveOutboundDrainsWrittenVote pins #309: an
// epoch-driven eviction must not destroy messages already written to a
// LIVE outbound.
//
// A restarted candidate dials a peer it has never heard from, so the outbound
// is stamped with epoch 0, and writes a RequestVote on it. The peer's reply
// (on the peer's own connection, carrying its epoch) then reaches the
// candidate before the vote request has been read on the far side. The stamp
// cannot be attributed to that epoch, so the outbound is evicted; closing it
// on the spot discarded the vote request, and a candidate does not resend a
// vote request within a term, so each loss cost a whole election term.
//
// The eviction must take the outbound out of the pool at once (no new traffic
// may use it) and still deliver what was already written to it.
func TestEvictStaleOutboundOnRecv_LiveOutboundDrainsWrittenVote(t *testing.T) {
	h2 := newGatedHandler()
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), h2)
	defer cleanup()

	target := t2.Addr().String()
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}

	vote := func(term uint64) []proto.Message {
		return []proto.Message{{Type: proto.RequestVote, ShardID: 1, From: 1, To: 2, Term: term}}
	}

	// The first vote parks the receiver's read loop for this stream.
	t1.sendBatch(target, "", vote(1))
	select {
	case <-h2.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("first RequestVote never reached the receiver")
	}
	// The second is written (sendBatch returns after stream.Write) but cannot
	// be read on the far side until the handler is released.
	t1.sendBatch(target, "", vote(2))

	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}
	if inbound == outbound {
		t.Fatal("outbound and inbound must be distinct connections")
	}
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)

	if n := t1.ConnectionCount(); n != 0 {
		t.Fatalf("evicted outbound is still pooled (%d connections): new "+
			"traffic would keep using a connection of unproven provenance", n)
	}

	close(h2.release)
	deadline := time.After(10 * time.Second)
	for {
		if terms := h2.received(); len(terms) == 2 {
			return
		}
		select {
		case <-h2.got:
		case <-deadline:
			t.Fatalf("RequestVote written to a live outbound before its "+
				"eviction was never delivered (received terms %v); the "+
				"candidate does not resend it within the term", h2.received())
		}
	}
}

// TestEvictStaleOutboundOnRecv_DrainingOutboundIsClosed pins the bound on
// the drain: an evicted outbound that nobody reads (a black hole to a dead
// incarnation) is still closed, once the grace has passed.
func TestEvictStaleOutboundOnRecv_DrainingOutboundIsClosed(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}
	evictedAt := time.Now()
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)

	select {
	case <-outbound.Context().Done():
	case <-time.After(staleOutboundDrainGrace + 30*time.Second):
		t.Fatal("evicted outbound was never closed: a black-holed connection " +
			"would linger until MaxIdleTimeout")
	}
	if waited := time.Since(evictedAt); waited < staleOutboundDrainGrace/2 {
		t.Fatalf("evicted outbound closed after %v, before the drain grace "+
			"(%v): frames already written to it are discarded", waited,
			staleOutboundDrainGrace)
	}
	t1.connMu.RLock()
	draining := len(t1.draining)
	t1.connMu.RUnlock()
	if draining != 0 {
		t.Fatalf("closed outbound still tracked as draining (%d)", draining)
	}
}

// TestEvictStaleOutboundOnRecv_StopClosesDrainingOutbound pins shutdown: an
// outbound still inside its drain grace is closed by Stop, not left to a
// timer that outlives the transport.
func TestEvictStaleOutboundOnRecv_StopClosesDrainingOutbound(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)

	stopStart := time.Now()
	if err := t1.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	select {
	case <-outbound.Context().Done():
	default:
		t.Fatal("Stop returned with an evicted outbound still open")
	}
	if took := time.Since(stopStart); took >= staleOutboundDrainGrace {
		t.Fatalf("Stop waited out the drain grace (%v) instead of closing "+
			"the draining outbound", took)
	}
	t1.connMu.RLock()
	draining := len(t1.draining)
	t1.connMu.RUnlock()
	if draining != 0 {
		t.Fatalf("Stop left %d outbounds tracked as draining", draining)
	}
}

// drainedOutbound dials t2 from t1, evicts the outbound as stale, and returns
// it once it is parked in t1.draining under t2's address.
func drainedOutbound(t *testing.T, t1, t2 *QUICTransport) (string, *quic.Conn) {
	t.Helper()
	target := t2.Addr().String()
	outbound, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection failed: %v", err)
	}
	inbound, err := t2.getConnection(t1.Addr().String(), "")
	if err != nil {
		t.Fatalf("t2.getConnection failed: %v", err)
	}
	t1.evictStaleOutboundOnRecv(target, t2.bootEpoch, inbound)
	t1.connMu.RLock()
	to, parked := t1.draining[outbound]
	t1.connMu.RUnlock()
	if !parked || to != target {
		t.Fatalf("evicted outbound not draining under %q (parked=%v, target=%q)",
			target, parked, to)
	}
	return target, outbound
}

// removedPeerClose reports whether conn was closed by CloseTarget's
// removed-peer teardown, not by closeAfterDrain's stale-eviction close.
func removedPeerClose(conn *quic.Conn) bool {
	var appErr *quic.ApplicationError
	if !errors.As(context.Cause(conn.Context()), &appErr) {
		return false
	}
	return !appErr.Remote && appErr.ErrorCode == 0 &&
		appErr.ErrorMessage == "peer removed from membership"
}

// A peer removed from membership while our outbound to it is inside its drain
// grace must lose that connection at once: CloseTarget promises immediate
// reclamation, and the grace timer would otherwise hold it open for up to
// staleOutboundDrainGrace.
func TestCloseTarget_ClosesDrainingOutboundAtOnce(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target, outbound := drainedOutbound(t, t1, t2)
	start := time.Now()
	if !t1.CloseTarget(target) {
		t.Fatal("CloseTarget reported nothing reclaimed with an outbound draining")
	}
	select {
	case <-outbound.Context().Done():
	default:
		t.Fatal("CloseTarget returned with the target's draining outbound still open")
	}
	if took := time.Since(start); took >= staleOutboundDrainGrace/2 {
		t.Fatalf("CloseTarget took %v, not well inside the drain grace (%v)",
			took, staleOutboundDrainGrace)
	}
	t1.connMu.RLock()
	draining := len(t1.draining)
	t1.connMu.RUnlock()
	if draining != 0 {
		t.Fatalf("closed outbound still tracked as draining (%d)", draining)
	}
}

// The draining entry is the close obligation, so CloseTarget takes it and the
// drain goroutine, when its grace expires, finds nothing to close: the
// connection's close cause stays CloseTarget's.
func TestCloseTarget_DrainingOutboundClosedExactlyOnce(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target, outbound := drainedOutbound(t, t1, t2)
	if !t1.CloseTarget(target) {
		t.Fatal("CloseTarget reported nothing reclaimed with an outbound draining")
	}
	if !removedPeerClose(outbound) {
		t.Fatalf("draining outbound not closed by CloseTarget: cause %v",
			context.Cause(outbound.Context()))
	}
	// Outlast the grace so the drain goroutine has run its ownership check.
	time.Sleep(staleOutboundDrainGrace + 250*time.Millisecond)
	if !removedPeerClose(outbound) {
		t.Fatalf("close cause changed after the drain grace: %v",
			context.Cause(outbound.Context()))
	}
	t1.connMu.RLock()
	draining := len(t1.draining)
	t1.connMu.RUnlock()
	if draining != 0 {
		t.Fatalf("draining set not empty after the grace (%d)", draining)
	}
}

// CloseTarget for one address must not reach an outbound draining to another.
func TestCloseTarget_LeavesOtherTargetsDrainingOutbound(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target, outbound := drainedOutbound(t, t1, t2)
	if t1.CloseTarget("127.0.0.1:1") {
		t.Fatal("CloseTarget on an unrelated address reported something reclaimed")
	}
	select {
	case <-outbound.Context().Done():
		t.Fatalf("CloseTarget on an unrelated address closed %s's draining outbound",
			target)
	default:
	}
	t1.connMu.RLock()
	to, parked := t1.draining[outbound]
	t1.connMu.RUnlock()
	if !parked || to != target {
		t.Fatalf("draining entry disturbed (parked=%v, target=%q)", parked, to)
	}
}
