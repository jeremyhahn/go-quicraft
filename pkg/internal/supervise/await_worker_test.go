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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// boundedWait is the longest any condition in this file is waited for; a
// correct worker or timer meets each one in milliseconds.
const boundedWait = 10 * time.Second

// awaitRestartResult runs AwaitRestart on stopC in its own goroutine, bounded
// by ctx, and reports its result and how long the call itself took.
func awaitRestartResult(ctx context.Context, t *testing.T, stopC <-chan struct{}) (bool, time.Duration) {
	t.Helper()
	type result struct {
		ok      bool
		elapsed time.Duration
	}
	resC := make(chan result, 1)
	go func() {
		start := time.Now()
		ok := AwaitRestart(stopC)
		resC <- result{ok: ok, elapsed: time.Since(start)}
	}()
	select {
	case r := <-resC:
		return r.ok, r.elapsed
	case <-ctx.Done():
		t.Fatalf("AwaitRestart did not return within %v", boundedWait)
		return false, 0
	}
}

// waitUntil polls cond until it holds or ctx ends, failing the test then.
func waitUntil(ctx context.Context, t *testing.T, what string, cond func() bool) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for %s", boundedWait, what)
		}
	}
}

// awaitClosed waits for c to close, bounded by ctx.
func awaitClosed(ctx context.Context, t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-ctx.Done():
		t.Fatalf("timed out after %v waiting for %s", boundedWait, what)
	}
}

func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), boundedWait)
	t.Cleanup(cancel)
	return ctx
}

func TestAwaitRestart_ClosedStopReturnsFalseAtOnce(t *testing.T) {
	ctx := boundedContext(t)
	stopC := make(chan struct{})
	close(stopC)
	ok, elapsed := awaitRestartResult(ctx, t, stopC)
	if ok {
		t.Fatal("AwaitRestart with a closed stopC returned true, want false")
	}
	if elapsed >= RestartDelay/2 {
		t.Fatalf("AwaitRestart with a closed stopC took %v, want well under RestartDelay %v", elapsed, RestartDelay)
	}
}

func TestAwaitRestart_OpenStopWaitsRestartDelay(t *testing.T) {
	ctx := boundedContext(t)
	ok, elapsed := awaitRestartResult(ctx, t, make(chan struct{}))
	if !ok {
		t.Fatal("AwaitRestart with an open stopC returned false, want true")
	}
	if elapsed < RestartDelay {
		t.Fatalf("AwaitRestart returned true after %v, before RestartDelay %v", elapsed, RestartDelay)
	}
}

func TestAwaitRestart_StopDuringWaitReturnsFalse(t *testing.T) {
	ctx := boundedContext(t)
	stopC := make(chan struct{})
	closer := time.AfterFunc(RestartDelay/4, func() { close(stopC) })
	defer closer.Stop()
	ok, elapsed := awaitRestartResult(ctx, t, stopC)
	if ok {
		t.Fatal("AwaitRestart with stopC closed during the wait returned true, want false")
	}
	if elapsed >= RestartDelay {
		t.Fatalf("AwaitRestart with stopC closed during the wait took %v, want under RestartDelay %v", elapsed, RestartDelay)
	}
}

// awaitWorkerIdle waits until w has given up its worker goroutine with an
// empty queue.
func awaitWorkerIdle(ctx context.Context, t *testing.T, w *diagnosticWorker) {
	t.Helper()
	waitUntil(ctx, t, "the worker to go idle", func() bool {
		return !w.running.Load() && len(w.queue) == 0
	})
}

func TestDiagnosticWorker_DropsPastCapacityAndRunsInOrder(t *testing.T) {
	ctx := boundedContext(t)
	const capacity, extra = 4, 3
	w := newDiagnosticWorker(capacity)

	var mu sync.Mutex
	var order []int
	record := func(i int) func() {
		return func() {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		}
	}
	ran := func() []int {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), order...)
	}

	started, release := make(chan struct{}), make(chan struct{})
	w.submit(func() {
		close(started)
		<-release
		record(0)()
	})
	// The blocking record is out of the queue and running: the queue is empty
	// and holds exactly capacity more.
	awaitClosed(ctx, t, started, "the blocking record to start")

	for i := 1; i <= capacity+extra; i++ {
		w.submit(record(i))
	}
	if got := w.dropped.Load(); got != extra {
		t.Fatalf("dropped = %d after %d submits past capacity %d, want %d", got, extra, capacity, extra)
	}
	if got := ran(); len(got) != 0 {
		t.Fatalf("records ran while the first record blocked: %v", got)
	}

	close(release)
	waitUntil(ctx, t, "the kept records to run", func() bool { return len(ran()) == capacity+1 })
	awaitWorkerIdle(ctx, t, w)

	got := ran()
	if len(got) != capacity+1 {
		t.Fatalf("ran %v, want records 0..%d", got, capacity)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("ran %v, want records 0..%d in submission order", got, capacity)
		}
	}
	if d := w.dropped.Load(); d != extra {
		t.Fatalf("dropped = %d after the drain, want %d", d, extra)
	}
}

func TestDiagnosticWorker_SurvivesPanickingRecord(t *testing.T) {
	ctx := boundedContext(t)
	w := newDiagnosticWorker(4)

	started, release, next := make(chan struct{}), make(chan struct{}), make(chan struct{})
	w.submit(func() {
		close(started)
		<-release
		panic("hostile diagnostic record")
	})
	awaitClosed(ctx, t, started, "the panicking record to start")
	// Queued behind the panicking record, so the same drain must reach it.
	w.submit(func() { close(next) })
	close(release)

	awaitClosed(ctx, t, next, "the record after the panicking one to run")
	awaitWorkerIdle(ctx, t, w)
	if d := w.dropped.Load(); d != 0 {
		t.Fatalf("dropped = %d, want 0", d)
	}
}

func TestDiagnosticWorker_ConcurrentSubmitRunsEachOnce(t *testing.T) {
	ctx := boundedContext(t)
	const goroutines, perGoroutine = 32, 16
	const total = goroutines * perGoroutine
	w := newDiagnosticWorker(total)

	var counts [total]atomic.Int32
	var ranTotal atomic.Int32
	var wg sync.WaitGroup
	startC := make(chan struct{})
	for g := range goroutines {
		wg.Go(func() {
			<-startC
			for j := range perGoroutine {
				id := g*perGoroutine + j
				w.submit(func() {
					counts[id].Add(1)
					ranTotal.Add(1)
				})
			}
		})
	}
	close(startC)
	wg.Wait()

	waitUntil(ctx, t, "every submitted record to run", func() bool { return ranTotal.Load() >= total })
	awaitWorkerIdle(ctx, t, w)

	if d := w.dropped.Load(); d != 0 {
		t.Fatalf("dropped = %d with capacity for every record, want 0", d)
	}
	if n := ranTotal.Load(); n != total {
		t.Fatalf("ran %d records, want %d", n, total)
	}
	for id := range counts {
		if c := counts[id].Load(); c != 1 {
			t.Fatalf("record %d ran %d times, want exactly once", id, c)
		}
	}
}
