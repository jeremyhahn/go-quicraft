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

package supervise

import (
	"context"
	"log/slog"
	"math"
	"runtime"
	"testing"
	"time"
)

// maxWorkerGoroutines is the most goroutines the contained-diagnostics worker
// may add while it runs: the worker itself, and the replacement Supervise
// starts when a handler ends it with runtime.Goexit.
const maxWorkerGoroutines = 2

// signalHandler is a slog handler that runs hostile on records whose message
// is "hostile", signals the records "first" and "second" on seen, and discards
// every other record.
type signalHandler struct {
	hostile func()
	seen    chan<- string
}

func (h signalHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h signalHandler) Handle(_ context.Context, r slog.Record) error {
	switch r.Message {
	case "hostile":
		h.hostile()
	case "first", "second":
		h.seen <- r.Message
	}
	return nil
}
func (h signalHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h signalHandler) WithGroup(string) slog.Handler      { return h }

// awaitIdle fails the test unless, within five seconds, the
// contained-diagnostics worker has emptied its queue and exited, and the
// goroutine count has fallen to at most want: the worker left nothing running
// behind it.
func awaitIdle(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for contained.running.Load() || len(contained.queue) > 0 || runtime.NumGoroutine() > want {
		select {
		case <-deadline:
			t.Fatalf("worker running %v with %d records queued and %d goroutines, want idle with at most %d",
				contained.running.Load(), len(contained.queue), runtime.NumGoroutine(), want)
		case <-tick.C:
		}
	}
}

// TestLogContained_BlockingHandlerIsBounded: a slog handler that never returns
// holds one record and the contained-diagnostics worker, and nothing more.
// Every caller still returns at once, the records beyond the queue's capacity
// are dropped and counted, no goroutine is started per record, and the worker
// drains its queue and exits once the handler is released.
func TestLogContained_BlockingHandlerIsBounded(t *testing.T) {
	assertBlockingHandlerBounded(t)
}

// TestLogContained_BoundHoldsWithDiagnosticsInFlight starts the bound check
// while an earlier test's diagnostics are still on the worker: one held in a
// slow handler and one queued, as RunEach and RunResilient leave them. The
// check must not count them against the bound (seen at -count=20).
func TestLogContained_BoundHoldsWithDiagnosticsInFlight(t *testing.T) {
	prev := slog.Default()
	inHandler := make(chan struct{}, 1)
	slog.SetDefault(slog.New(hostileHandler{act: func() {
		select {
		case inHandler <- struct{}{}:
		default:
		}
		time.Sleep(200 * time.Millisecond)
	}}))
	LogContained(func() { slog.Error("in flight") })
	await(t, inHandler, "the in-flight diagnostic never reached its handler")
	LogContained(func() { slog.Error("queued") })
	slog.SetDefault(prev)
	assertBlockingHandlerBounded(t)
}

// assertBlockingHandlerBounded checks that a blocked slog handler neither
// blocks LogContained's callers nor grows goroutines, and drops exactly the
// records the queue cannot hold.
func assertBlockingHandlerBounded(t *testing.T) {
	t.Helper()
	// The bound is counted from an empty queue and an idle worker: records
	// an earlier test left on the shared worker would take queue slots and
	// show as drops of this check's own records.
	awaitIdle(t, math.MaxInt)
	baseline := runtime.NumGoroutine()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	prev := slog.Default()
	slog.SetDefault(slog.New(hostileHandler{act: func() {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}}))
	// The handler is replaced before it is released, so the records still
	// queued drain into the previous one, not into the next test's.
	t.Cleanup(func() {
		slog.SetDefault(prev)
		close(release)
		awaitIdle(t, baseline)
	})

	droppedBefore := DiagnosticsDropped()
	const records = 4 * DiagnosticQueueCapacity
	submitted := make(chan struct{})
	go func() {
		defer close(submitted)
		for range records {
			LogContained(func() { slog.Error("diagnostic") })
		}
	}()
	await(t, submitted, "LogContained blocked its caller behind a blocked slog handler")
	await(t, entered, "the blocked handler never received a record")

	if growth := runtime.NumGoroutine() - baseline; growth > maxWorkerGoroutines {
		t.Fatalf("%d records behind a blocked handler left %d goroutines running, want at most %d",
			records, growth, maxWorkerGoroutines)
	}
	// The worker holds one record in the handler and the queue holds
	// DiagnosticQueueCapacity; the worker may not have taken its first record
	// before the last submission, in which case one more is dropped.
	dropped := DiagnosticsDropped() - droppedBefore
	if lo, hi := uint64(records-DiagnosticQueueCapacity-1), uint64(records-DiagnosticQueueCapacity); dropped < lo || dropped > hi {
		t.Fatalf("DiagnosticsDropped grew by %d, want %d..%d", dropped, lo, hi)
	}
}

// TestLogContained_WorkerSurvivesHostileHandler: a slog handler that panics or
// calls runtime.Goexit on a record ends neither the worker nor the records
// queued behind it — a panic is recovered in place and a Goexit restarts the
// worker through Supervise — and the worker still exits once its queue is
// empty, leaving no goroutine behind.
func TestLogContained_WorkerSurvivesHostileHandler(t *testing.T) {
	for exitName, exit := range hostileExits {
		t.Run(exitName, func(t *testing.T) {
			baseline := runtime.NumGoroutine()
			seen := make(chan string, 4)
			prev := slog.Default()
			slog.SetDefault(slog.New(signalHandler{hostile: exit, seen: seen}))
			t.Cleanup(func() { slog.SetDefault(prev) })

			for _, msg := range []string{"hostile", "first", "hostile", "second"} {
				LogContained(func() { slog.Error(msg) })
			}
			for _, want := range []string{"first", "second"} {
				select {
				case got := <-seen:
					if got != want {
						t.Fatalf("record %q emitted, want %q: the worker reordered its queue", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("record %q never emitted: a %s in the handler ended the worker", want, exitName)
				}
			}
			awaitIdle(t, baseline)
		})
	}
}
