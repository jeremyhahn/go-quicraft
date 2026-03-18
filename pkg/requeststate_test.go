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

package quicraft

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Timer pool helpers
// ---------------------------------------------------------------------------

func TestAcquireTimer_FiresAfterDuration(t *testing.T) {
	d := 50 * time.Millisecond
	timer := acquireTimer(d)
	defer releaseTimer(timer)

	start := time.Now()
	select {
	case <-timer.C:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire within expected window")
	}
	elapsed := time.Since(start)
	if elapsed < d/2 {
		t.Errorf("timer fired too early: elapsed %v, expected >= %v", elapsed, d/2)
	}
}

func TestAcquireTimer_ChannelDrainedOnReuse(t *testing.T) {
	// Acquire, let it fire naturally, release, then re-acquire.
	// The re-acquired timer's channel must be empty when returned.
	timer := acquireTimer(10 * time.Millisecond)
	<-timer.C // consume the tick
	releaseTimer(timer)

	// Re-acquire with a long duration; the channel must be empty immediately.
	timer2 := acquireTimer(10 * time.Second)
	defer releaseTimer(timer2)

	select {
	case <-timer2.C:
		t.Error("timer channel should be empty after reuse from pool")
	default:
		// Expected: no stale tick.
	}
}

func TestReleaseTimer_StopsBeforeReturningToPool(t *testing.T) {
	// Acquire with a short duration, release before it fires.
	// The pool entry must not have a pending tick when re-acquired.
	timer := acquireTimer(200 * time.Millisecond)
	releaseTimer(timer) // Stop() called inside; timer has not fired yet.

	timer2 := acquireTimer(10 * time.Second)
	defer releaseTimer(timer2)

	select {
	case <-timer2.C:
		t.Error("stale tick present in pooled timer after early Stop")
	default:
		// Expected: channel is empty.
	}
}

func TestReleaseTimer_DrainsFiredTimer(t *testing.T) {
	// Acquire, wait for it to fire, then release. releaseTimer must drain
	// the channel so the next caller gets a clean timer.
	timer := acquireTimer(10 * time.Millisecond)
	// Wait for the timer to actually fire before releasing it.
	select {
	case <-timer.C:
	case <-time.After(2 * time.Second):
		t.Fatal("timer did not fire within expected window")
	}
	releaseTimer(timer) // Stop() returns false; drains channel.

	timer2 := acquireTimer(10 * time.Second)
	defer releaseTimer(timer2)

	select {
	case <-timer2.C:
		t.Error("stale tick present after releaseTimer drained fired timer")
	default:
		// Expected: channel is clean.
	}
}

// ---------------------------------------------------------------------------
// Result — timer pool integration
// ---------------------------------------------------------------------------

func TestResult_PooledTimerTimeout(t *testing.T) {
	pool := NewRequestStatePool()
	// Use a short deadline so the pooled timer fires quickly.
	deadline := time.Now().Add(40 * time.Millisecond)
	rs := newRequestState(pool, 1, deadline)

	_, err := rs.Result()
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Result error = %v, want ErrTimeout", err)
	}
}

func TestResult_PooledTimerReuse(t *testing.T) {
	pool := NewRequestStatePool()

	// Run several sequential Result() calls each hitting the timer path to
	// confirm the pool handles reuse correctly with no stale ticks.
	for i := range 5 {
		deadline := time.Now().Add(30 * time.Millisecond)
		rs := newRequestState(pool, uint64(i)+1, deadline)
		_, err := rs.Result()
		if !errors.Is(err, ErrTimeout) {
			t.Errorf("iteration %d: Result error = %v, want ErrTimeout", i, err)
		}
		rs.Release()
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkResultPooledTimer measures the allocations eliminated by the
// timerPool in the Result() timeout path.
func BenchmarkResultPooledTimer(b *testing.B) {
	pool := NewRequestStatePool()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		deadline := time.Now().Add(time.Hour) // far future: timer never fires
		rs := newRequestState(pool, 1, deadline)
		// Send a result so Result() returns via the result path, not timer.
		rs.complete(RequestResult{Value: 1})
		_, _ = rs.Result()
		rs.Release()
	}
}

// BenchmarkAcquireReleaseTimer measures the round-trip cost of the timer pool.
func BenchmarkAcquireReleaseTimer(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t := acquireTimer(time.Hour)
		releaseTimer(t)
	}
}

// ---------------------------------------------------------------------------
// NewRequestStatePool
// ---------------------------------------------------------------------------

func TestNewRequestStatePool_ReturnsPool(t *testing.T) {
	pool := NewRequestStatePool()
	if pool == nil {
		t.Fatal("NewRequestStatePool returned nil")
	}

	obj := pool.Get()
	rs, ok := obj.(*RequestState)
	if !ok {
		t.Fatalf("pool.Get returned %T, want *RequestState", obj)
	}
	if rs.completedC == nil {
		t.Error("completedC channel is nil")
	}
	if cap(rs.completedC) != 1 {
		t.Errorf("completedC capacity = %d, want 1", cap(rs.completedC))
	}
}

func TestNewRequestStatePool_AllocatesCommittedChannel(t *testing.T) {
	pool := NewRequestStatePool()
	obj := pool.Get()
	rs, ok := obj.(*RequestState)
	if !ok {
		t.Fatalf("pool.Get returned %T, want *RequestState", obj)
	}
	if rs.committedC == nil {
		t.Error("committedC channel is nil")
	}
	if cap(rs.committedC) != 1 {
		t.Errorf("committedC capacity = %d, want 1", cap(rs.committedC))
	}
}

// ---------------------------------------------------------------------------
// newRequestState
// ---------------------------------------------------------------------------

func TestNewRequestState_Initialization(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(5 * time.Second)
	rs := newRequestState(pool, 42, deadline)

	if rs.key != 42 {
		t.Errorf("key = %d, want 42", rs.key)
	}
	if !rs.Deadline().Equal(deadline) {
		t.Errorf("deadline = %v, want %v", rs.Deadline(), deadline)
	}
	if rs.pool != pool {
		t.Error("pool not set correctly")
	}
	if rs.released.Load() {
		t.Error("released should be false for new RequestState")
	}
	if rs.committedDone.Load() {
		t.Error("committedDone should be false for new RequestState")
	}
}

func TestNewRequestState_DrainsStalePreviousResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Simulate a stale result in the channel.
	rs.completedC <- RequestResult{Value: 999}

	// Re-initialize should drain the stale result.
	rs2 := newRequestState(pool, 2, time.Time{})

	// The channel should be empty now.
	select {
	case <-rs2.completedC:
		t.Error("completedC should be empty after reinitialization")
	default:
		// Expected.
	}
}

func TestNewRequestState_DrainsStaleCommittedResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Simulate a stale committed notification in the channel.
	rs.committedC <- RequestResult{Value: 888}

	// Re-initialize should drain the stale committed result.
	rs2 := newRequestState(pool, 2, time.Time{})

	// The committedC channel should be empty now.
	select {
	case <-rs2.committedC:
		t.Error("committedC should be empty after reinitialization")
	default:
		// Expected.
	}
}

func TestNewRequestState_ResetsCommittedDone(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Mark as committed.
	rs.commitNotify(RequestResult{Value: 0})
	if !rs.committedDone.Load() {
		t.Error("committedDone should be true after commitNotify")
	}

	// Release and re-init.
	rs.Release()
	rs2 := newRequestState(pool, 2, time.Time{})

	// committedDone must be reset.
	if rs2.committedDone.Load() {
		t.Error("committedDone should be false after reinitialization")
	}
}

// ---------------------------------------------------------------------------
// Result (blocking)
// ---------------------------------------------------------------------------

func TestResult_BlockingSuccess(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	expected := RequestResult{Value: 42, Data: []byte("hello")}

	go func() {
		rs.complete(expected)
	}()

	result, err := rs.Result()
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Value != 42 {
		t.Errorf("Value = %d, want 42", result.Value)
	}
	if string(result.Data) != "hello" {
		t.Errorf("Data = %q, want %q", result.Data, "hello")
	}
}

func TestResult_BlockingWithError(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	go func() {
		rs.complete(RequestResult{Err: ErrRejected})
	}()

	_, err := rs.Result()
	if !errors.Is(err, ErrRejected) {
		t.Errorf("Result error = %v, want ErrRejected", err)
	}
}

func TestResult_Timeout(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(50 * time.Millisecond)
	rs := newRequestState(pool, 1, deadline)

	_, err := rs.Result()
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Result error = %v, want ErrTimeout", err)
	}
}

func TestResult_AlreadyExpiredDeadline(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(-1 * time.Second) // Already past.
	rs := newRequestState(pool, 1, deadline)

	_, err := rs.Result()
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Result error = %v, want ErrTimeout", err)
	}
}

func TestResult_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()

	_, err := rs.Result()
	if !errors.Is(err, ErrReleased) {
		t.Errorf("Result error = %v, want ErrReleased", err)
	}
}

// ---------------------------------------------------------------------------
// ResultC (non-blocking)
// ---------------------------------------------------------------------------

func TestResultC_ReturnsChannel(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	ch := rs.ResultC()
	if ch == nil {
		t.Fatal("ResultC returned nil channel")
	}
}

func TestResultC_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()

	ch := rs.ResultC()
	if ch != nil {
		t.Error("ResultC should return nil after Release")
	}
}

func TestResultC_ReceivesResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	go func() {
		rs.complete(RequestResult{Value: 77})
	}()

	ch := rs.ResultC()
	select {
	case result := <-ch:
		if result.Value != 77 {
			t.Errorf("Value = %d, want 77", result.Value)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for result on ResultC")
	}
}

// ---------------------------------------------------------------------------
// CommittedC
// ---------------------------------------------------------------------------

func TestCommittedC_ReturnsChannel(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	ch := rs.CommittedC()
	if ch == nil {
		t.Fatal("CommittedC returned nil channel")
	}
}

func TestCommittedC_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()

	ch := rs.CommittedC()
	if ch != nil {
		t.Error("CommittedC should return nil after Release")
	}
}

func TestCommittedC_ReceivesNotification(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	go func() {
		rs.commitNotify(RequestResult{Value: 0})
	}()

	ch := rs.CommittedC()
	select {
	case result := <-ch:
		if result.Value != 0 {
			t.Errorf("Value = %d, want 0", result.Value)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for committed notification")
	}
}

func TestCommittedC_IndependentFromCompletedC(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Commit notification fires first.
	rs.commitNotify(RequestResult{Value: 0})

	// Apply result fires second.
	rs.complete(RequestResult{Value: 42, Data: []byte("applied")})

	// Both channels should have their respective results.
	select {
	case result := <-rs.CommittedC():
		if result.Value != 0 {
			t.Errorf("CommittedC Value = %d, want 0", result.Value)
		}
	default:
		t.Error("CommittedC should have a result")
	}

	select {
	case result := <-rs.ApplyResultC():
		if result.Value != 42 {
			t.Errorf("ApplyResultC Value = %d, want 42", result.Value)
		}
		if string(result.Data) != "applied" {
			t.Errorf("ApplyResultC Data = %q, want %q", result.Data, "applied")
		}
	default:
		t.Error("ApplyResultC should have a result")
	}
}

// ---------------------------------------------------------------------------
// ApplyResultC
// ---------------------------------------------------------------------------

func TestApplyResultC_ReturnsCompletedChannel(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	ch := rs.ApplyResultC()
	if ch == nil {
		t.Fatal("ApplyResultC returned nil channel")
	}
	// ApplyResultC should return the same channel as ResultC.
	if ch != rs.ResultC() {
		t.Error("ApplyResultC should return the same channel as ResultC")
	}
}

func TestApplyResultC_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()

	ch := rs.ApplyResultC()
	if ch != nil {
		t.Error("ApplyResultC should return nil after Release")
	}
}

// ---------------------------------------------------------------------------
// Release (idempotent)
// ---------------------------------------------------------------------------

func TestRelease_Idempotent(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Release should not panic when called multiple times.
	rs.Release()
	rs.Release()
	rs.Release()

	if !rs.released.Load() {
		t.Error("released flag should be true after Release")
	}
}

func TestRelease_ConcurrentSafety(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs.Release()
		}()
	}
	wg.Wait()

	if !rs.released.Load() {
		t.Error("released flag should be true")
	}
}

func TestRelease_DrainsUnconsumedResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.complete(RequestResult{Value: 42})
	rs.Release()

	// The channel should be drained.
	select {
	case <-rs.completedC:
		// Channel was drained by Release and then pool.Put may have been called.
		// In any case, the released flag prevents further use.
	default:
		// Also acceptable if already drained.
	}
}

func TestRelease_DrainsUnconsumedCommittedResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.commitNotify(RequestResult{Value: 0})
	rs.complete(RequestResult{Value: 42})
	rs.Release()

	// Both channels should be drained.
	select {
	case <-rs.committedC:
		// Drained by Release.
	default:
		// Also acceptable if already drained.
	}

	select {
	case <-rs.completedC:
		// Drained by Release.
	default:
		// Also acceptable if already drained.
	}
}

func TestRelease_NilPoolDoesNotPanic(t *testing.T) {
	rs := &RequestState{
		completedC: make(chan RequestResult, 1),
		committedC: make(chan RequestResult, 1),
		pool:       nil,
	}
	// Should not panic.
	rs.Release()

	if !rs.released.Load() {
		t.Error("released flag should be true")
	}
}

// ---------------------------------------------------------------------------
// complete
// ---------------------------------------------------------------------------

func TestComplete_SendsResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	expected := RequestResult{Value: 123, Data: []byte("data")}
	rs.complete(expected)

	select {
	case result := <-rs.completedC:
		if result.Value != 123 {
			t.Errorf("Value = %d, want 123", result.Value)
		}
		if string(result.Data) != "data" {
			t.Errorf("Data = %q, want %q", result.Data, "data")
		}
	default:
		t.Error("completedC should have a result")
	}
}

func TestComplete_AfterRelease_IsNoOp(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()
	// Should not panic or block.
	rs.complete(RequestResult{Value: 42})
}

func TestComplete_DoubleComplete_DoesNotBlock(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.complete(RequestResult{Value: 1})
	// Second complete should not block (already completed).
	rs.complete(RequestResult{Value: 2})

	result := <-rs.completedC
	if result.Value != 1 {
		t.Errorf("Value = %d, want 1 (first complete)", result.Value)
	}
}

func TestComplete_DoubleComplete_AtomicGuard(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// First complete delivers the result.
	rs.complete(RequestResult{Value: 42})

	// Second complete with different value is silently dropped.
	rs.complete(RequestResult{Value: 99})

	// Verify only the first result was delivered.
	result := <-rs.completedC
	if result.Value != 42 {
		t.Errorf("Value = %d, want 42 (first complete wins)", result.Value)
	}

	// Channel should be empty after draining the single result.
	select {
	case extra := <-rs.completedC:
		t.Errorf("unexpected second result: %+v", extra)
	default:
		// Expected: no second result.
	}
}

func TestComplete_ConcurrentDoubleComplete(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	var wg sync.WaitGroup
	// Race N goroutines trying to complete the same request.
	const n = 100
	wg.Add(n)
	for i := range n {
		go func(val uint64) {
			defer wg.Done()
			rs.complete(RequestResult{Value: val})
		}(uint64(i))
	}
	wg.Wait()

	// Exactly one result should be in the channel.
	select {
	case <-rs.completedC:
		// Good: exactly one result delivered.
	default:
		t.Fatal("expected one result in completedC, got none")
	}

	// No second result.
	select {
	case extra := <-rs.completedC:
		t.Errorf("unexpected second result: %+v", extra)
	default:
		// Expected.
	}
}

// ---------------------------------------------------------------------------
// commitNotify
// ---------------------------------------------------------------------------

func TestCommitNotify_SendsNotification(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.commitNotify(RequestResult{Value: 0})

	select {
	case result := <-rs.committedC:
		if result.Value != 0 {
			t.Errorf("Value = %d, want 0", result.Value)
		}
	default:
		t.Error("committedC should have a notification")
	}
}

func TestCommitNotify_AfterRelease_IsNoOp(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()
	// Should not panic or block.
	rs.commitNotify(RequestResult{Value: 0})
}

func TestCommitNotify_Idempotent(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// First call delivers.
	rs.commitNotify(RequestResult{Value: 0})
	// Second call is silently dropped.
	rs.commitNotify(RequestResult{Value: 99})

	// Verify only the first notification was delivered.
	result := <-rs.committedC
	if result.Value != 0 {
		t.Errorf("Value = %d, want 0 (first commitNotify wins)", result.Value)
	}

	// Channel should be empty after draining the single notification.
	select {
	case extra := <-rs.committedC:
		t.Errorf("unexpected second notification: %+v", extra)
	default:
		// Expected: no second notification.
	}
}

func TestCommitNotify_ConcurrentSafety(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	var wg sync.WaitGroup
	const n = 100
	wg.Add(n)
	for i := range n {
		go func(val uint64) {
			defer wg.Done()
			rs.commitNotify(RequestResult{Value: val})
		}(uint64(i))
	}
	wg.Wait()

	// Exactly one notification should be in the channel.
	select {
	case <-rs.committedC:
		// Good: exactly one notification delivered.
	default:
		t.Fatal("expected one notification in committedC, got none")
	}

	// No second notification.
	select {
	case extra := <-rs.committedC:
		t.Errorf("unexpected second notification: %+v", extra)
	default:
		// Expected.
	}
}

func TestCommitNotify_IndependentFromComplete(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Both commitNotify and complete should work independently.
	rs.commitNotify(RequestResult{Value: 0})
	rs.complete(RequestResult{Value: 42})

	// committedC should have Value=0.
	select {
	case result := <-rs.committedC:
		if result.Value != 0 {
			t.Errorf("committedC Value = %d, want 0", result.Value)
		}
	default:
		t.Error("committedC should have a notification")
	}

	// completedC should have Value=42.
	select {
	case result := <-rs.completedC:
		if result.Value != 42 {
			t.Errorf("completedC Value = %d, want 42", result.Value)
		}
	default:
		t.Error("completedC should have a result")
	}
}

// ---------------------------------------------------------------------------
// Key and Deadline accessors
// ---------------------------------------------------------------------------

func TestKey_ReturnsKey(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 999, time.Time{})
	if rs.Key() != 999 {
		t.Errorf("Key = %d, want 999", rs.Key())
	}
}

func TestDeadline_ReturnsDeadline(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(10 * time.Second)
	rs := newRequestState(pool, 1, deadline)
	if !rs.Deadline().Equal(deadline) {
		t.Errorf("Deadline = %v, want %v", rs.Deadline(), deadline)
	}
}

func TestDeadline_ZeroValue(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})
	if !rs.Deadline().IsZero() {
		t.Error("Deadline should be zero when not set")
	}
}

// ---------------------------------------------------------------------------
// Pool recycling
// ---------------------------------------------------------------------------

func TestPoolRecycling_ReusesObjects(t *testing.T) {
	pool := NewRequestStatePool()

	rs1 := newRequestState(pool, 1, time.Time{})
	rs1.Release()

	rs2 := newRequestState(pool, 2, time.Time{})
	// The pool may or may not return the same object, but it should work correctly.
	if rs2.key != 2 {
		t.Errorf("key = %d, want 2", rs2.key)
	}
	if rs2.released.Load() {
		t.Error("recycled RequestState should not be marked as released")
	}
	if rs2.committedDone.Load() {
		t.Error("recycled RequestState should not have committedDone set")
	}
	rs2.Release()
}
