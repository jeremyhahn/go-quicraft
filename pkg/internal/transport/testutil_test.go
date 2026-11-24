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

// waitConnDown blocks until tr's cached connection to target is torn down, by its context; it works for a close from either side.
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

// firstContactBound is the longest a cold Send can take: the sum of the transport's dial, stream-open and write bounds.
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

// sendFirstContact sends msgs to an unconnected target, resending after each firstContactBound until h sees a batch; h may see duplicates.
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
