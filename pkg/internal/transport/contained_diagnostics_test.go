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
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
)

// blockingLogHandler is a slog handler — user code, through slog.SetDefault —
// that never returns until release closes, and signals entered on its first
// record.
type blockingLogHandler struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (h blockingLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h blockingLogHandler) Handle(context.Context, slog.Record) error {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
	return nil
}
func (h blockingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h blockingLogHandler) WithGroup(string) slog.Handler      { return h }

// goroutineSlack absorbs goroutines of earlier tests' transports that are
// still winding down while a test counts its own.
const goroutineSlack = 8

// TestRejectSnapshotStream_BlockingLogHandlerIsBounded: an authenticated peer
// that keeps sending snapshot streams the receiver rejects, against a slog
// handler that blocks, costs a bounded number of goroutines — not one per
// rejection — and the diagnostics the handler could not take are counted as
// dropped.
func TestRejectSnapshotStream_BlockingLogHandlerIsBounded(t *testing.T) {
	sr := newSnapshotReceiver(4, 1<<20, 0, 0, newTestHandler())
	baseline := runtime.NumGoroutine()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	prev := slog.Default()
	slog.SetDefault(slog.New(blockingLogHandler{entered: entered, release: release}))
	// The handler is replaced before it is released, so the records still
	// queued drain into the previous one, not into a later test's.
	t.Cleanup(func() {
		slog.SetDefault(prev)
		close(release)
	})

	droppedBefore := supervise.DiagnosticsDropped()
	const rejections = 4 * supervise.DiagnosticQueueCapacity
	hdr := &SnapshotHeader{ShardID: 1, ReplicaID: 2, Index: 3}
	for range rejections {
		sr.rejectSnapshotStream(snapshotRejectMalformed, "invalid header", hdr, ErrStreamClosed)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking handler never received a rejection diagnostic")
	}

	if growth := runtime.NumGoroutine() - baseline; growth > goroutineSlack {
		t.Fatalf("%d rejections behind a blocked slog handler left %d more goroutines, want at most %d",
			rejections, growth, goroutineSlack)
	}
	if dropped, want := supervise.DiagnosticsDropped()-droppedBefore, uint64(rejections-supervise.DiagnosticQueueCapacity-1); dropped < want {
		t.Fatalf("DiagnosticsDropped grew by %d, want at least %d", dropped, want)
	}
}
