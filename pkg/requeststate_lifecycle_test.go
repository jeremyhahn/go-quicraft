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

// ===========================================================================
// PendingConfigChange lifecycle
// ===========================================================================

// TestRequestState_PendingConfigChange_CreateAndVerify verifies that a
// RequestState created for a config change operation has correct initial
// state: key set, deadline set, not released, not completed.
func TestRequestState_PendingConfigChange_CreateAndVerify(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(5 * time.Second)
	rs := newRequestState(pool, 100, deadline)

	if rs.Key() != 100 {
		t.Errorf("Key = %d, want 100", rs.Key())
	}
	if !rs.Deadline().Equal(deadline) {
		t.Errorf("Deadline mismatch: got %v, want %v", rs.Deadline(), deadline)
	}
	if rs.released.Load() {
		t.Error("newly created RequestState should not be released")
	}
	if rs.completed.Load() {
		t.Error("newly created RequestState should not be completed")
	}
	if rs.committedDone.Load() {
		t.Error("newly created RequestState should not have committedDone set")
	}

	ch := rs.ResultC()
	if ch == nil {
		t.Error("ResultC should return non-nil channel for active request")
	}
	select {
	case <-ch:
		t.Error("channel should be empty initially")
	default:
		// Expected.
	}
	rs.Release()
}

// TestRequestState_PendingConfigChange_BusyWhenPending verifies that
// when a config change request is already pending (ResultC not yet consumed),
// a second request can detect the busy state by checking the channel.
func TestRequestState_PendingConfigChange_BusyWhenPending(t *testing.T) {
	pool := NewRequestStatePool()
	rs1 := newRequestState(pool, 1, time.Now().Add(10*time.Second))

	// rs1 is pending (no result delivered yet). A caller checking
	// whether a config change is pending should find the channel empty.
	select {
	case <-rs1.ResultC():
		t.Fatal("pending config change should not have a result yet")
	default:
		// Busy: config change is pending.
	}

	// Complete the first one.
	rs1.complete(RequestResult{Value: 1})

	// Now the channel should have a result.
	select {
	case result := <-rs1.ResultC():
		if result.Value != 1 {
			t.Errorf("Value = %d, want 1", result.Value)
		}
	default:
		t.Error("completed config change should have a result")
	}
	rs1.Release()
}

// TestRequestState_PendingConfigChange_Timeout verifies that a config
// change request that exceeds its deadline returns ErrTimeout.
func TestRequestState_PendingConfigChange_Timeout(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(50 * time.Millisecond)
	rs := newRequestState(pool, 1, deadline)

	_, err := rs.Result()
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
	rs.Release()
}

// TestRequestState_PendingConfigChange_CompletionNotification verifies
// that completing a config change delivers the result through both
// ResultC and Result().
func TestRequestState_PendingConfigChange_CompletionNotification(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	expected := RequestResult{Value: 42, Data: []byte("config-applied")}

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
	if string(result.Data) != "config-applied" {
		t.Errorf("Data = %q, want %q", result.Data, "config-applied")
	}
	rs.Release()
}

// TestRequestState_PendingConfigChange_DropCancel verifies that releasing
// a pending config change (simulating a drop/cancel) makes ResultC return
// nil and Result return ErrReleased.
func TestRequestState_PendingConfigChange_DropCancel(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Now().Add(10*time.Second))

	// Drop/cancel the pending request.
	rs.Release()

	if rs.ResultC() != nil {
		t.Error("ResultC should return nil after Release")
	}
	_, err := rs.Result()
	if !errors.Is(err, ErrReleased) {
		t.Errorf("expected ErrReleased after drop, got %v", err)
	}
}

// ===========================================================================
// PendingReadIndex lifecycle
// ===========================================================================

// TestRequestState_PendingReadIndex_CreateAndVerify verifies that a
// RequestState created for a read index operation has correct initial state.
func TestRequestState_PendingReadIndex_CreateAndVerify(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(3 * time.Second)
	rs := newRequestState(pool, 200, deadline)

	if rs.Key() != 200 {
		t.Errorf("Key = %d, want 200", rs.Key())
	}
	if rs.released.Load() {
		t.Error("newly created read index RequestState should not be released")
	}
	if rs.completed.Load() {
		t.Error("newly created read index RequestState should not be completed")
	}
	rs.Release()
}

// TestRequestState_PendingReadIndex_WhenPending verifies that checking
// a pending read index detects the busy state through channel inspection.
func TestRequestState_PendingReadIndex_WhenPending(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Now().Add(10*time.Second))

	// Read index is pending.
	select {
	case <-rs.ResultC():
		t.Fatal("pending read index should not have a result")
	default:
		// Busy.
	}

	rs.complete(RequestResult{Value: 500})

	select {
	case result := <-rs.ResultC():
		if result.Value != 500 {
			t.Errorf("Value = %d, want 500", result.Value)
		}
	default:
		t.Error("completed read index should have a result")
	}
	rs.Release()
}

// TestRequestState_PendingReadIndex_CompletionWithCommittedIndex verifies
// that a read index result carries the committed index from the leader.
func TestRequestState_PendingReadIndex_CompletionWithCommittedIndex(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Simulate leader confirming the read index at committed=1000.
	go func() {
		rs.complete(RequestResult{Value: 1000})
	}()

	result, err := rs.Result()
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Value != 1000 {
		t.Errorf("committed index = %d, want 1000", result.Value)
	}
	rs.Release()
}

// TestRequestState_PendingReadIndex_Timeout verifies that a read index
// request that exceeds its deadline returns ErrTimeout.
func TestRequestState_PendingReadIndex_Timeout(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(50 * time.Millisecond)
	rs := newRequestState(pool, 1, deadline)

	_, err := rs.Result()
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout for read index, got %v", err)
	}
	rs.Release()
}

// TestRequestState_BatchReadIndex_Timeout verifies that multiple read
// index requests created as a batch all respect their individual deadlines.
func TestRequestState_BatchReadIndex_Timeout(t *testing.T) {
	pool := NewRequestStatePool()
	deadline := time.Now().Add(50 * time.Millisecond)

	const batchSize = 5
	states := make([]*RequestState, batchSize)
	for i := range states {
		states[i] = newRequestState(pool, uint64(i+1), deadline)
	}

	// All should timeout.
	for i, rs := range states {
		_, err := rs.Result()
		if !errors.Is(err, ErrTimeout) {
			t.Errorf("batch[%d]: expected ErrTimeout, got %v", i, err)
		}
		rs.Release()
	}
}

// ===========================================================================
// PendingSnapshot edge cases
// ===========================================================================

// TestRequestState_SnapshotGCDuringPending verifies that releasing a
// pending snapshot request (simulating GC) correctly cleans up state.
func TestRequestState_SnapshotGCDuringPending(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 300, time.Now().Add(30*time.Second))

	// Snapshot is pending. Simulate GC by releasing before completion.
	rs.Release()

	if !rs.released.Load() {
		t.Error("released flag should be true after Release")
	}

	// Complete after release should be a no-op.
	rs.complete(RequestResult{Value: 42})

	// Channel should not have a result since release drained it.
	select {
	case result := <-rs.completedC:
		t.Errorf("unexpected result after release+complete: %+v", result)
	default:
		// Expected.
	}
}

// TestRequestState_SnapshotApplyCompletion verifies that completing a
// snapshot apply delivers the result correctly to the caller.
func TestRequestState_SnapshotApplyCompletion(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Simulate snapshot apply completion.
	go func() {
		rs.complete(RequestResult{Value: 999, Data: []byte("snapshot-done")})
	}()

	result, err := rs.Result()
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Value != 999 {
		t.Errorf("Value = %d, want 999", result.Value)
	}
	if string(result.Data) != "snapshot-done" {
		t.Errorf("Data = %q, want %q", result.Data, "snapshot-done")
	}
	rs.Release()
}

// TestRequestState_SnapshotIdentityByKey verifies that multiple snapshot
// requests can be distinguished by their unique keys.
func TestRequestState_SnapshotIdentityByKey(t *testing.T) {
	pool := NewRequestStatePool()

	rs1 := newRequestState(pool, 100, time.Time{})
	rs2 := newRequestState(pool, 200, time.Time{})
	rs3 := newRequestState(pool, 300, time.Time{})

	if rs1.Key() == rs2.Key() || rs2.Key() == rs3.Key() || rs1.Key() == rs3.Key() {
		t.Error("snapshot requests should have distinct keys")
	}

	rs1.complete(RequestResult{Value: 1})
	rs2.complete(RequestResult{Value: 2})
	rs3.complete(RequestResult{Value: 3})

	check := func(rs *RequestState, expected uint64) {
		t.Helper()
		select {
		case result := <-rs.ResultC():
			if result.Value != expected {
				t.Errorf("key=%d: Value = %d, want %d", rs.Key(), result.Value, expected)
			}
		default:
			t.Errorf("key=%d: no result in channel", rs.Key())
		}
	}

	check(rs1, 1)
	check(rs2, 2)
	check(rs3, 3)

	rs1.Release()
	rs2.Release()
	rs3.Release()
}

// ===========================================================================
// Release and ready-to-release semantics
// ===========================================================================

// TestRequestState_ReleaseAndReadyStates verifies the full lifecycle from
// creation through completion to release, checking state at each stage.
func TestRequestState_ReleaseAndReadyStates(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	// Stage 1: Active state.
	if rs.released.Load() {
		t.Fatal("stage 1: should not be released")
	}
	if rs.completed.Load() {
		t.Fatal("stage 1: should not be completed")
	}
	if rs.ResultC() == nil {
		t.Fatal("stage 1: ResultC should return non-nil")
	}

	// Stage 2: Completed but not released.
	rs.complete(RequestResult{Value: 42})
	if rs.released.Load() {
		t.Fatal("stage 2: should not be released after complete")
	}
	if !rs.completed.Load() {
		t.Fatal("stage 2: should be completed")
	}

	// Consume the result.
	<-rs.ResultC()

	// Stage 3: Release.
	rs.Release()
	if !rs.released.Load() {
		t.Fatal("stage 3: should be released")
	}
	if rs.ResultC() != nil {
		t.Fatal("stage 3: ResultC should return nil after release")
	}
	if rs.CommittedC() != nil {
		t.Fatal("stage 3: CommittedC should return nil after release")
	}
	if rs.ApplyResultC() != nil {
		t.Fatal("stage 3: ApplyResultC should return nil after release")
	}
}

// TestRequestState_DoubleReleaseSafety verifies that releasing twice is
// safe and idempotent, not panicking or corrupting state.
func TestRequestState_DoubleReleaseSafety(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.complete(RequestResult{Value: 1})
	<-rs.ResultC()

	// First release.
	rs.Release()
	if !rs.released.Load() {
		t.Fatal("should be released after first Release")
	}

	// Second release should not panic.
	rs.Release()
	if !rs.released.Load() {
		t.Fatal("should still be released after second Release")
	}

	// Result and ResultC after double release.
	_, err := rs.Result()
	if !errors.Is(err, ErrReleased) {
		t.Errorf("expected ErrReleased after double release, got %v", err)
	}
	if rs.ResultC() != nil {
		t.Error("ResultC should return nil after double release")
	}
}

// TestRequestState_DoubleReleaseConcurrent verifies that concurrent
// double-release from multiple goroutines is safe.
func TestRequestState_DoubleReleaseConcurrent(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})
	rs.complete(RequestResult{Value: 1})

	var wg sync.WaitGroup
	const goroutines = 50
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			rs.Release()
		}()
	}
	wg.Wait()

	if !rs.released.Load() {
		t.Error("should be released after concurrent releases")
	}
}

// TestRequestState_CompleteAfterDoubleRelease verifies that completing
// a request after double release is a safe no-op.
func TestRequestState_CompleteAfterDoubleRelease(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.Release()
	rs.Release()

	// Complete should be no-op.
	rs.complete(RequestResult{Value: 999})

	// CommitNotify should also be no-op.
	rs.commitNotify(RequestResult{Value: 0})
}

// ===========================================================================
// CommitNotify + Complete interaction
// ===========================================================================

// TestRequestState_CommitThenComplete_BothChannelsWork verifies that
// commitNotify and complete can be called sequentially, delivering
// independent results to their respective channels.
func TestRequestState_CommitThenComplete_BothChannelsWork(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	rs.commitNotify(RequestResult{Value: 0})
	rs.complete(RequestResult{Value: 42, Data: []byte("applied")})

	// Commit channel.
	select {
	case result := <-rs.CommittedC():
		if result.Value != 0 {
			t.Errorf("CommittedC Value = %d, want 0", result.Value)
		}
	default:
		t.Error("CommittedC should have a notification")
	}

	// Apply channel.
	select {
	case result := <-rs.ApplyResultC():
		if result.Value != 42 {
			t.Errorf("ApplyResultC Value = %d, want 42", result.Value)
		}
	default:
		t.Error("ApplyResultC should have a result")
	}
	rs.Release()
}

// TestRequestState_PoolRecycleFullLifecycle verifies a full
// create-complete-release-recycle-create-complete-release cycle
// ensuring recycled RequestState objects work correctly.
func TestRequestState_PoolRecycleFullLifecycle(t *testing.T) {
	pool := NewRequestStatePool()

	// First lifecycle.
	rs1 := newRequestState(pool, 1, time.Now().Add(5*time.Second))
	rs1.commitNotify(RequestResult{Value: 0})
	rs1.complete(RequestResult{Value: 10})
	<-rs1.CommittedC()
	<-rs1.ResultC()
	rs1.Release()

	// Second lifecycle with recycled object.
	rs2 := newRequestState(pool, 2, time.Now().Add(5*time.Second))
	if rs2.Key() != 2 {
		t.Errorf("recycled key = %d, want 2", rs2.Key())
	}
	if rs2.released.Load() {
		t.Error("recycled RequestState should not be released")
	}
	if rs2.completed.Load() {
		t.Error("recycled RequestState should not be completed")
	}
	if rs2.committedDone.Load() {
		t.Error("recycled RequestState should not have committedDone set")
	}

	// Channels should be empty.
	select {
	case <-rs2.completedC:
		t.Error("recycled completedC should be empty")
	default:
	}
	select {
	case <-rs2.committedC:
		t.Error("recycled committedC should be empty")
	default:
	}

	// Complete the recycled request.
	rs2.complete(RequestResult{Value: 20})
	result := <-rs2.ResultC()
	if result.Value != 20 {
		t.Errorf("recycled result Value = %d, want 20", result.Value)
	}
	rs2.Release()
}

// TestRequestState_ZeroDeadline_BlocksIndefinitely verifies that a request
// with zero deadline blocks until completion (no timeout).
func TestRequestState_ZeroDeadline_BlocksIndefinitely(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	if !rs.Deadline().IsZero() {
		t.Fatalf("deadline should be zero, got %v", rs.Deadline())
	}

	done := make(chan RequestResult, 1)
	go func() {
		result, _ := rs.Result()
		done <- result
	}()

	// Should not have received yet.
	select {
	case <-done:
		t.Fatal("should block with zero deadline")
	case <-time.After(50 * time.Millisecond):
		// Expected.
	}

	// Complete it.
	rs.complete(RequestResult{Value: 77})

	select {
	case result := <-done:
		if result.Value != 77 {
			t.Errorf("Value = %d, want 77", result.Value)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for result")
	}
	rs.Release()
}

// TestRequestState_CompleteWithError_PropagatesToResult verifies that
// completing with an error result propagates through Result().
func TestRequestState_CompleteWithError_PropagatesToResult(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 1, time.Time{})

	go func() {
		rs.complete(RequestResult{Err: ErrAborted})
	}()

	_, err := rs.Result()
	if !errors.Is(err, ErrAborted) {
		t.Errorf("expected ErrAborted, got %v", err)
	}
	rs.Release()
}
