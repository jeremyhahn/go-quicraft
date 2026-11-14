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

package dragonboat

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// concurrentTestTimeout bounds every concurrent phase so a deadlock in
// ResultC or Release fails the test instead of hanging the package.
const concurrentTestTimeout = 10 * time.Second

// newConcurrentInner returns a fresh pooled quicraft RequestState, built
// the same way as the single-goroutine tests in requeststate_test.go.
func newConcurrentInner(_ context.Context, t *testing.T) *quicraft.RequestState {
	t.Helper()
	inner, ok := quicraft.NewRequestStatePool().Get().(*quicraft.RequestState)
	if !ok || inner == nil {
		t.Fatal("pool did not return a *quicraft.RequestState")
	}
	return inner
}

// runConcurrently starts every fn in its own goroutine, releases them
// together from a shared barrier, and waits for all of them to return
// before ctx expires. A panic in any fn is reported as a test failure.
func runConcurrently(ctx context.Context, t *testing.T, fns ...func()) {
	t.Helper()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var panics atomic.Int64
	for _, fn := range fns {
		wg.Add(1)
		go func(fn func()) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
					t.Errorf("concurrent call panicked: %v", r)
				}
			}()
			<-start
			fn()
		}(fn)
	}
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("concurrent calls did not finish: %v", ctx.Err())
	}
	if n := panics.Load(); n != 0 {
		t.Fatalf("%d concurrent calls panicked", n)
	}
}

// TestRequestStateConcurrent_ResultCSameChannel checks that 32 goroutines
// calling ResultC on a NotifyCommit RequestState all receive the one lazily
// built aggregate channel, and that AppliedC is still the apply channel.
func TestRequestStateConcurrent_ResultCSameChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	rs := WrapRequestState(newConcurrentInner(ctx, t), true)
	defer rs.Release()

	const callers = 32
	got := make([]chan RequestResult, callers)
	fns := make([]func(), callers)
	for i := range fns {
		fns[i] = func() { got[i] = rs.ResultC() }
	}
	runConcurrently(ctx, t, fns...)

	first := got[0]
	if first == nil {
		t.Fatal("ResultC returned nil")
	}
	for i, ch := range got {
		if ch != first {
			t.Fatalf("caller %d got a different ResultC channel", i)
		}
	}
	if again := rs.ResultC(); again != first {
		t.Fatal("a later ResultC call returned a different channel")
	}
	if rs.AppliedC() != rs.CompletedC {
		t.Fatal("AppliedC is no longer the apply channel")
	}
	if rs.AppliedC() == first {
		t.Fatal("ResultC with NotifyCommit must be the aggregate, not the apply channel")
	}
	if c := cap(first); c != 2 {
		t.Fatalf("aggregate ResultC cap = %d, want 2", c)
	}
}

// TestRequestStateConcurrent_ReleaseAndResultC races 8 Release calls
// against 8 ResultC calls on a RequestState carrying a cancel func: no
// panic, the cancel func runs exactly once, every ResultC caller gets the
// same channel, and every bridge goroutine has exited once Release returns.
func TestRequestStateConcurrent_ReleaseAndResultC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	runtime.GC()
	baseline := runtime.NumGoroutine()

	var cancels atomic.Int64
	opCtx, opCancel := context.WithCancel(ctx)
	defer opCancel()
	countingCancel := func() {
		cancels.Add(1)
		opCancel()
	}
	rs := WrapRequestStateWithCancel(newConcurrentInner(ctx, t), countingCancel, true)

	const each = 8
	got := make([]chan RequestResult, each)
	fns := make([]func(), 0, 2*each)
	for i := 0; i < each; i++ {
		fns = append(fns, rs.Release)
		fns = append(fns, func() { got[i] = rs.ResultC() })
	}
	// runConcurrently returns only after every call has returned, including
	// the one Release that won and waited for the bridge goroutines.
	runConcurrently(ctx, t, fns...)

	if n := cancels.Load(); n != 1 {
		t.Fatalf("cancel func ran %d times, want exactly 1", n)
	}
	if opCtx.Err() == nil {
		t.Fatal("operation context not canceled after Release")
	}
	for i, ch := range got {
		if ch == nil {
			t.Fatalf("ResultC caller %d got nil", i)
		}
		if ch != got[0] {
			t.Fatalf("ResultC caller %d got a different channel", i)
		}
	}

	after := runtime.NumGoroutine()
	if leaked := after - baseline; leaked > 5 {
		t.Fatalf("goroutines not back to baseline: baseline=%d after=%d leaked=%d", baseline, after, leaked)
	}
}

// TestRequestStateConcurrent_ResultCAfterRelease checks that ResultC on a
// released NotifyCommit RequestState still returns a usable non-nil
// channel and spawns no bridge goroutine.
func TestRequestStateConcurrent_ResultCAfterRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	rs := WrapRequestState(newConcurrentInner(ctx, t), true)
	rs.Release()

	runtime.GC()
	before := runtime.NumGoroutine()

	const callers = 8
	got := make([]chan RequestResult, callers)
	fns := make([]func(), callers)
	for i := range fns {
		fns[i] = func() { got[i] = rs.ResultC() }
	}
	runConcurrently(ctx, t, fns...)

	after := runtime.NumGoroutine()
	if after > before {
		t.Fatalf("ResultC after Release started goroutines: before=%d after=%d", before, after)
	}
	for i, ch := range got {
		if ch == nil {
			t.Fatalf("ResultC caller %d got nil after Release", i)
		}
		if ch != got[0] {
			t.Fatalf("ResultC caller %d got a different channel after Release", i)
		}
	}
	// Release is a no-op the second time and must not block.
	rs.Release()
}

// TestRequestStateConcurrent_NoNotifyCommitIsAppliedC checks that with
// NotifyCommit disabled every concurrent ResultC caller receives the apply
// channel itself.
func TestRequestStateConcurrent_NoNotifyCommitIsAppliedC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	rs := WrapRequestState(newConcurrentInner(ctx, t), false)
	defer rs.Release()

	const callers = 32
	got := make([]chan RequestResult, callers)
	fns := make([]func(), callers)
	for i := range fns {
		fns[i] = func() { got[i] = rs.ResultC() }
	}
	runConcurrently(ctx, t, fns...)

	applied := rs.AppliedC()
	if applied == nil {
		t.Fatal("AppliedC returned nil")
	}
	for i, ch := range got {
		if ch != applied {
			t.Fatalf("caller %d: ResultC without NotifyCommit is not AppliedC", i)
		}
	}
	rs.mu.Lock()
	aggr := rs.aggrC
	rs.mu.Unlock()
	if aggr != nil {
		t.Fatal("ResultC without NotifyCommit built an aggregate channel")
	}
}
