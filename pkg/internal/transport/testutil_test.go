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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// pollUntil polls condition() every interval until it returns true or timeout
// elapses. Reports a fatal error with msg on timeout.
func pollUntil(t *testing.T, timeout, interval time.Duration, msg string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal(msg)
		}
	}
}

// assertNever verifies condition() stays false for the entire duration,
// sampling every interval. Reports a fatal error if condition() becomes true.
func assertNever(t *testing.T, duration, interval time.Duration, msg string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if condition() {
				t.Fatal(msg)
			}
		case <-deadline.C:
			// Passed: condition never became true.
			return
		}
	}
}

// countDropsWithReason returns how many message drops carrying reason the
// recorder has observed.
func countDropsWithReason(d *dropRecorder, reason string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, s := range d.messageDrops {
		if s.reason == reason {
			n++
		}
	}
	return n
}

// countRequests returns the total number of messages the handler has observed
// across every batch it received.
func countRequests(h *testHandler) int {
	total := 0
	for _, b := range h.getBatches() {
		total += len(b.Requests)
	}
	return total
}

// waitConnDown blocks until tr's cached connection to target reports itself
// torn down. The signal is the connection's own context, not the size of
// tr.conns: quic-go cancels the context on close, but the map entry survives
// until a later send or the cleanup sweep evicts it, so polling for an empty
// pool waits for a different event that may never happen within the test.
//
// It is usable for a connection closed on EITHER side. It was once only usable
// for a local close: a stopped transport closed its listener, its own outbound
// connections and its socket but never the connections it had ACCEPTED, so no
// CONNECTION_CLOSE reached the sender and its context stayed live for the whole
// idle timeout. Stop now announces itself on the inbound half too (#78), so a
// peer's Stop is a condition a test can wait on instead of guessing at with a
// fixed sleep.
func waitConnDown(t *testing.T, tr *QUICTransport, target string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, timeout, 2*time.Millisecond,
		"peer connection to "+target+" was never observed closed", func() bool {
			tr.connMu.RLock()
			conn, ok := tr.conns[target]
			tr.connMu.RUnlock()
			return !ok || conn.Context().Err() != nil
		})
}

// waitForChan waits for a channel to be closed or for timeout.
func waitForChan(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal(msg)
	}
}

// ---------------------------------------------------------------------------
// First contact: what a cold Send is allowed to take
// ---------------------------------------------------------------------------

// firstContactBound is the longest a single Send to a target with no live
// connection can legitimately take, and the point past which waiting is
// pointless.
//
// A cold Send passes through three bounds the transport applies to ITSELF:
// the dial (dialTimeout, covering the QUIC handshake), the stream open
// (streamOpenTimeout) and the frame write (sendWriteDeadline). The moment any
// of them expires the batch is DROPPED, by contract — "Raft retransmission
// will resend it" — so no wait, however long, delivers that particular Send.
// The bound is their sum, read from the transport's own constants rather than
// restated, so it follows a change to any of them.
//
// #142: the wait at every first-contact site was a 10s literal, BELOW this
// bound. Under CI saturation (load 150-200 on 24 cores, -race and coverage
// instrumentation, 24 packages in parallel) a cold handshake was traced at
// 4.3s against the 5s dial deadline and delivery at 5.35s, with the whole
// process descheduled for seconds at a time; the batcher itself flushes on
// dequeue with no timer. A literal below the transport's own ceiling could
// neither outlast a slow-but-legal handshake nor survive an abandoned one.
const firstContactBound = dialTimeout + streamOpenTimeout + sendWriteDeadline

// firstContactAttempts is how many cold Sends sendFirstContact makes before
// failing: one the transport is allowed to abandon at firstContactBound, and
// one more. Two consecutive abandoned dials is a broken transport, not a busy
// box.
const firstContactAttempts = 2

// firstContactError reports a cold Send that no attempt delivered.
type firstContactError struct {
	attempts int
	wait     time.Duration
}

func (e *firstContactError) Error() string {
	return fmt.Sprintf("transport test: no batch delivered by %d cold sends of %v each",
		e.attempts, e.wait)
}

// sendFirstContact delivers msgs to a target the sender has no connection to
// yet and returns once h has seen a batch. After each firstContactBound with
// nothing delivered it sends again, as Raft does on its next tick, so a cold
// dial the transport abandons under saturation does not strand the test on a
// message nothing would ever retransmit.
//
// h may therefore see MORE than one batch, and a later waitBatch on h may be
// satisfied by the duplicate. Use it only for the first contact of a test that
// asserts on batch content rather than batch count.
func sendFirstContact(t testing.TB, tr *QUICTransport, h *testHandler, msgs []proto.Message) {
	t.Helper()
	if err := sendUntilDelivered(tr, h, msgs, firstContactAttempts, firstContactBound); err != nil {
		t.Fatal(err)
	}
}

// sendUntilDelivered is sendFirstContact with the attempt count and the
// per-attempt wait injected, so that its re-send and give-up behaviour can be
// tested at a scale that does not take firstContactAttempts*firstContactBound
// to fail.
func sendUntilDelivered(tr *QUICTransport, h *testHandler, msgs []proto.Message, attempts int, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			timer.Reset(wait)
		}
		tr.Send(msgs)
		select {
		case <-h.batchNotify:
			return nil
		case <-timer.C:
		}
	}
	return &firstContactError{attempts: attempts, wait: wait}
}

// dropRecorder implements the transport Metrics interface, recording only the
// two drop counters so tests can assert the reason bucket a call site emits.
type dropRecorder struct {
	noopTransportMetrics
	mu               sync.Mutex
	snapshotsRejects []dropSample
	messageDrops     []dropSample
}

// dropSample is one recorded drop: the shard, the target replica (0 for
// snapshot rejections), and the reason label.
type dropSample struct {
	shardID uint64
	to      uint64
	reason  string
}

func (d *dropRecorder) IncSnapshotsRejected(shardID uint64, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.snapshotsRejects = append(d.snapshotsRejects, dropSample{shardID: shardID, reason: reason})
}

func (d *dropRecorder) IncMessagesDropped(shardID, to uint64, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.messageDrops = append(d.messageDrops, dropSample{shardID: shardID, to: to, reason: reason})
}

func (d *dropRecorder) rejects() []dropSample {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]dropSample(nil), d.snapshotsRejects...)
}

func (d *dropRecorder) drops() []dropSample {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]dropSample(nil), d.messageDrops...)
}

// firstDropWithReason returns the first recorded message drop carrying reason,
// and whether one was recorded at all. Polling for a specific reason this way
// avoids copying the whole drop log on every poll.
func (d *dropRecorder) firstDropWithReason(reason string) (dropSample, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.messageDrops {
		if s.reason == reason {
			return s, true
		}
	}
	return dropSample{}, false
}
