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
	"log/slog"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
)

// User code on the transport's own goroutines
//
// The transport runs user code at three kinds of point: a slog handler (any
// log call), an EventListener callback or revocation notification, and a
// MetricsCollector. None of it may stand between a state advance — a
// connection accepted, a slot or budget taken, a connection found dead or
// revoked, a stream released — and the cleanup or hand-off that settles it,
// because a panic or runtime.Goexit there skips the settlement. The rule is
// the engine's:
//
//   - settle first: everything structural is done, or owned by a defer that
//     user code cannot skip, before any user code runs;
//   - log last and contained: a diagnostic on a path that still has work to do
//     goes through supervise.LogContained, off the calling goroutine;
//   - isolate callbacks: an EventListener callback or revocation notification
//     runs through deliverEvent (supervise.RunEach), synchronously but on a
//     helper goroutine, so its abnormal exit can end neither the transport
//     goroutine that produced it nor the callbacks after it.
//
// Metrics on the per-message paths are observed after the hand-off they
// describe, never inside a window, and are not isolated: a helper goroutine
// per observation would cost more than the observation.

// deliverEvent runs each of calls, in order and synchronously, isolated from
// one another and from the calling goroutine (supervise.RunEach). A nil call
// is skipped, and when every call is nil nothing runs at all.
func deliverEvent(calls ...func()) {
	live := calls[:0:0]
	for _, call := range calls {
		if call != nil {
			live = append(live, call)
		}
	}
	supervise.RunEach(len(live), func(i int) { live[i]() })
}

// snapshotEvent returns the call delivering info to the listener callback
// pick selects, or nil when no such callback is installed.
func (t *QUICTransport) snapshotEvent(pick func(*config.EventListener) func(config.SnapshotInfo), info config.SnapshotInfo) func() {
	if t.eventListener == nil {
		return nil
	}
	cb := pick(t.eventListener)
	if cb == nil {
		return nil
	}
	return func() { cb(info) }
}

// reportSnapshotSent reports an outbound snapshot that is entirely on the
// wire: its diagnostic, contained, then OnSnapshotSent and the
// IncSnapshotsSent metric, each isolated. Nothing it runs can change the
// send's result, which its caller has already recorded as a success.
func (t *QUICTransport) reportSnapshotSent(info config.SnapshotInfo, chunks uint64, bytes int64, elapsed time.Duration) {
	supervise.LogContained(func() {
		slog.Info("snapshot sent",
			"shard", info.ShardID,
			"replica", info.ReplicaID,
			"index", info.Index,
			"chunks", chunks)
	})
	var observe func()
	if m := t.metrics; m != nil {
		observe = func() { m.IncSnapshotsSent(info.ShardID, bytes, elapsed) }
	}
	deliverEvent(t.snapshotEvent(onSnapshotSent, info), observe)
}

// The EventListener callbacks snapshotEvent selects.
func onSnapshotSendStarted(l *config.EventListener) func(config.SnapshotInfo) {
	return l.OnSnapshotSendStarted
}
func onSnapshotSent(l *config.EventListener) func(config.SnapshotInfo) { return l.OnSnapshotSent }
func onSnapshotSendAborted(l *config.EventListener) func(config.SnapshotInfo) {
	return l.OnSnapshotSendAborted
}
