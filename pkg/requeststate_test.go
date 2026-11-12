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

func TestResult_PooledTimerReuse(t *testing.T) {
	pool := NewRequestStatePool()

	// Run several sequential Result() calls each hitting the timer path to
	// confirm the pool handles reuse correctly with no stale ticks.
	for i := range 5 {
		deadline := time.Now().Add(30 * time.Millisecond)
		rs := newRequestState(pool, uint64(i)+1, deadline, true)
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
		rs := newRequestState(pool, 1, deadline, true)
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
	rs := newRequestState(pool, 42, deadline, true)

	if rs.Key() != 42 {
		t.Errorf("Key = %d, want 42", rs.Key())
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

// ---------------------------------------------------------------------------
// Result (blocking)
// ---------------------------------------------------------------------------

func TestResult_BlockingSuccess(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

	go func() {
		rs.complete(RequestResult{Err: ErrRejected})
	}()

	_, err := rs.Result()
	if !errors.Is(err, ErrRejected) {
		t.Errorf("Result error = %v, want ErrRejected", err)
	}
}

func TestResult_AlreadyExpiredDeadline(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(-1 * time.Second) // Already past.
	rs := newRequestState(pool, 1, deadline, true)

	_, err := rs.Result()
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Result error = %v, want ErrTimeout", err)
	}
}

func TestResult_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

	rs.Release()

	_, err := rs.Result()
	if !errors.Is(err, ErrReleased) {
		t.Errorf("Result error = %v, want ErrReleased", err)
	}
}

// ---------------------------------------------------------------------------
// ResultC (non-blocking)
// ---------------------------------------------------------------------------

func TestResultC_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

	rs.Release()

	ch := rs.ResultC()
	if ch != nil {
		t.Error("ResultC should return nil after Release")
	}
}

func TestResultC_ReceivesResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

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

func TestCommittedC_AfterRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

	rs.Release()

	ch := rs.CommittedC()
	if ch != nil {
		t.Error("CommittedC should return nil after Release")
	}
}

func TestCommittedC_ReceivesNotification(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

	rs.Release()

	ch := rs.ApplyResultC()
	if ch != nil {
		t.Error("ApplyResultC should return nil after Release")
	}
}

// ---------------------------------------------------------------------------
// Release (idempotent)
// ---------------------------------------------------------------------------

func TestRelease_ConcurrentSafety(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

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

func TestComplete_DoubleComplete_AtomicGuard(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

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

func TestCommitNotify_AfterRelease_IsNoOp(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

	rs.Release()
	// Should not panic or block.
	rs.commitNotify(RequestResult{Value: 0})
}

func TestCommitNotify_Idempotent(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

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
	rs := newRequestState(pool, 1, time.Time{}, true)

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

// ---------------------------------------------------------------------------
// Reference-count gated pool return (Bug E)
// ---------------------------------------------------------------------------

// TestRefcount_ReleaseBeforeReleaseRefDefersRecycle verifies that a caller's
// Release does NOT return the object to the pool while a pending-map
// reference is still outstanding. The object is recycled only when the final
// releaseRef runs after Release.
func TestRefcount_ReleaseBeforeReleaseRefDefersRecycle(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

	// Simulate registration in a pending map.
	rs.acquireRef()

	// Caller releases (e.g., Sync* timed out and returned) while still
	// registered. This must NOT recycle: the map still references rs.
	rs.Release()
	if rs.recycled.Load() {
		t.Fatal("rs recycled while a pending-map reference is still outstanding")
	}

	// The late completion path removes its reference; now rs may recycle.
	rs.releaseRef()
	if !rs.recycled.Load() {
		t.Fatal("rs not recycled after final releaseRef following Release")
	}
}

// TestRefcount_ReleaseRefBeforeReleaseDefersRecycle verifies the opposite
// ordering: the pending-map reference is removed first (the request completed
// normally) but the caller has not yet released. Recycle must wait for
// Release.
func TestRefcount_ReleaseRefBeforeReleaseDefersRecycle(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)
	rs.acquireRef()

	rs.releaseRef()
	if rs.recycled.Load() {
		t.Fatal("rs recycled before the caller released it")
	}

	rs.Release()
	if !rs.recycled.Load() {
		t.Fatal("rs not recycled after Release with no outstanding references")
	}
}

// pinnedStatePool returns a pool that always hands back the same
// RequestState storage, together with that storage. sync.Pool is free to
// drop a Put object (per-P caches, GC victim eviction), so a test that
// hopes the production pool returns the object it just recycled is
// nondeterministic. Pinning the storage makes the recycle-then-reuse path
// run on every execution while leaving the production reuse semantics
// (newRequestState's field reset and channel drain) exactly as they are:
// the storage itself is built by the production pool's New func.
func pinnedStatePool() (*sync.Pool, *RequestState) {
	storage := NewRequestStatePool().Get().(*RequestState)
	return &sync.Pool{New: func() interface{} { return storage }}, storage
}

// TestRefcount_NoCrossDeliveryAfterRecycle reproduces the use-after-Release
// hazard: request A times out and is released, its RequestState is recycled
// and reused for request B, then A's late completion fires. B's result
// channel must be untouched.
func TestRefcount_NoCrossDeliveryAfterRecycle(t *testing.T) {
	pool, storage := pinnedStatePool()

	// Request A registers and, before the caller gives up, a result and an
	// early commit notification land in its channels. Nobody consumes them:
	// this is the timed-out Sync* caller that walked away. The recycled
	// storage therefore carries A's stale payload into B's hands.
	rsA := newRequestState(pool, 100, time.Time{}, true)
	rsA.acquireRef()
	rsA.commitNotify(RequestResult{Value: 1})
	rsA.complete(RequestResult{Value: 100})
	rsA.Release()

	// A's late completion removes its reference, recycling rsA into the pool.
	rsA.releaseRef()
	if !rsA.recycled.Load() {
		t.Fatal("rsA not recycled after timeout + late releaseRef")
	}

	// Request B obtains its RequestState from the pool, which hands back the
	// recycled rsA storage.
	rsB := newRequestState(pool, 200, time.Time{}, true)
	if rsB != storage {
		t.Fatal("pool did not hand back the recycled storage")
	}
	rsB.acquireRef()

	// B is a different logical request even though it shares A's storage:
	// every per-request flag must have been reset by newRequestState.
	if rsB.key != 200 {
		t.Fatalf("recycled rsB.key = %d, want 200", rsB.key)
	}
	if rsB.released.Load() {
		t.Fatal("recycled rsB is still marked released")
	}
	if rsB.recycled.Load() {
		t.Fatal("recycled rsB is still marked recycled")
	}
	if rsB.completed.Load() {
		t.Fatal("recycled rsB still carries A's completed flag")
	}
	if rsB.committedDone.Load() {
		t.Fatal("recycled rsB still carries A's committedDone flag")
	}

	// The critical invariant: A's undelivered result must not surface as B's.
	// Both channels must be empty until B completes on its own.
	select {
	case got := <-rsB.ResultC():
		t.Fatalf("rsB result channel delivered A's stale result %+v", got)
	default:
	}
	select {
	case got := <-rsB.CommittedC():
		t.Fatalf("rsB committed channel delivered A's stale notification %+v", got)
	default:
	}

	// Complete B legitimately and confirm B receives its own result on both
	// channels: a leftover completed/committedDone flag would swallow them.
	rsB.commitNotify(RequestResult{Value: 2})
	select {
	case got := <-rsB.CommittedC():
		if got.Value != 2 {
			t.Fatalf("rsB committed result = %d, want 2", got.Value)
		}
	default:
		t.Fatal("rsB did not receive its own commit notification")
	}
	rsB.complete(RequestResult{Value: 200})
	select {
	case got := <-rsB.ResultC():
		if got.Value != 200 {
			t.Fatalf("rsB result = %d, want 200", got.Value)
		}
	default:
		t.Fatal("rsB did not receive its own completion")
	}
	rsB.releaseRef()
	rsB.Release()
	if !rsB.recycled.Load() {
		t.Fatal("rsB not recycled after its own release")
	}
}

// TestRefcount_MultipleRefs verifies the object survives until ALL references
// are released, regardless of caller Release ordering.
func TestRefcount_MultipleRefs(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)

	rs.acquireRef()
	rs.acquireRef()
	rs.Release()
	if rs.recycled.Load() {
		t.Fatal("recycled with two outstanding refs")
	}
	rs.releaseRef()
	if rs.recycled.Load() {
		t.Fatal("recycled with one outstanding ref remaining")
	}
	rs.releaseRef()
	if !rs.recycled.Load() {
		t.Fatal("not recycled after all refs released and caller released")
	}
}

// TestRefcount_NoRefRecyclesOnRelease verifies a RequestState that is never
// registered in a pending map recycles immediately on Release.
func TestRefcount_NoRefRecyclesOnRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)
	rs.Release()
	if !rs.recycled.Load() {
		t.Fatal("unregistered rs should recycle immediately on Release")
	}
}

// TestTryAcquireRef_FailsAtZero verifies tryAcquireRef never increments from
// zero holders: an object whose caller has released and whose pending-map
// references are all dropped has been recycled (or is racing recycling), and
// pinning it would be unsafe. While any holder remains, including only the
// caller's own reference, tryAcquireRef succeeds.
func TestTryAcquireRef_FailsAtZero(t *testing.T) {
	pool := NewRequestStatePool()

	// Reach zero holders through the real release path: the map's
	// reference, then the caller's Release, then the map's releaseRef.
	rs := newRequestState(pool, 1, time.Time{}, true)
	rs.acquireRef()
	rs.Release()
	rs.releaseRef()
	if !rs.recycled.Load() {
		t.Fatal("RequestState not recycled after its last holder dropped")
	}
	if rs.tryAcquireRef() {
		t.Fatal("tryAcquireRef succeeded with zero holders")
	}
	if got := rs.holders.Load(); got != 0 {
		t.Fatalf("holders = %d after failed tryAcquireRef, want 0", got)
	}
	if got := rs.outstanding.Load(); got != 0 {
		t.Fatalf("outstanding = %d after failed tryAcquireRef, want 0", got)
	}

	// Only the caller's reference held: tryAcquireRef pins the object and
	// a paired releaseRef leaves it live.
	live := newRequestState(NewRequestStatePool(), 2, time.Time{}, true)
	if !live.tryAcquireRef() {
		t.Fatal("tryAcquireRef failed while the caller's reference is held")
	}
	live.releaseRef()
	if live.recycled.Load() {
		t.Fatal("paired releaseRef recycled an object the caller still holds")
	}
	live.Release()
}

// TestTryAcquireRef_PinsAgainstRecycle verifies a successful tryAcquireRef
// (taken while a map reference pins the object) keeps the object out of the
// pool until its paired releaseRef, even when the map reference and the
// caller both release in the meantime.
func TestTryAcquireRef_PinsAgainstRecycle(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{}, true)
	rs.acquireRef() // registration-time map reference

	if !rs.tryAcquireRef() {
		t.Fatal("tryAcquireRef failed while a map reference was outstanding")
	}

	// The claiming path removes the map reference and the caller releases.
	rs.releaseRef()
	rs.Release()
	if rs.recycled.Load() {
		t.Fatal("recycled while a tryAcquireRef reference was still held")
	}

	// Dropping the pinned reference performs the final recycle.
	rs.releaseRef()
	if !rs.recycled.Load() {
		t.Fatal("not recycled after the last reference was released")
	}
}

// TestTryAcquireRef_ConcurrentWithRelease races tryAcquireRef against the
// claiming path's releaseRef and the caller's Release. Invariants checked:
// every successful tryAcquireRef is balanced by releaseRef, the counter
// never goes negative, and exactly one recycle happens. Run under -race.
func TestTryAcquireRef_ConcurrentWithRelease(t *testing.T) {
	for i := 0; i < 2000; i++ {
		pool := &sync.Pool{New: func() interface{} {
			return &RequestState{
				completedC: make(chan RequestResult, 1),
				committedC: make(chan RequestResult, 1),
			}
		}}
		rs := newRequestState(pool, 1, time.Time{}, true)
		rs.acquireRef()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if rs.tryAcquireRef() {
				rs.releaseRef()
			}
		}()
		go func() {
			defer wg.Done()
			rs.releaseRef() // claiming path drops the map reference
		}()
		rs.Release()
		wg.Wait()

		if got := rs.outstanding.Load(); got != 0 {
			t.Fatalf("iteration %d: outstanding = %d, want 0", i, got)
		}
		if !rs.recycled.Load() {
			t.Fatalf("iteration %d: object was not recycled", i)
		}
	}
}
