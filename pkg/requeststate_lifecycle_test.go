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
	"testing"
	"time"
)

// TestRequestState_SnapshotGCDuringPending verifies that releasing a
// pending snapshot request (simulating GC) correctly cleans up state.
func TestRequestState_SnapshotGCDuringPending(t *testing.T) {
	pool := NewRequestStatePool()
	rs := newRequestState(pool, 300, time.Now().Add(30*time.Second), true)

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

// TestRequestState_PoolRecycleFullLifecycle verifies a full
// create-complete-release-recycle-create-complete-release cycle on one
// object. The pinned pool hands the same object back, so every field
// newRequestState resets is observed on a genuinely recycled object: a
// plain sync.Pool may return a fresh one, and drops Puts under -race.
func TestRequestState_PoolRecycleFullLifecycle(t *testing.T) {
	pool, storage := pinnedStatePool()

	// First lifecycle, released with both results unconsumed.
	rs1 := newRequestState(pool, 1, time.Now().Add(5*time.Second), true)
	rs1.commitNotify(RequestResult{Value: 0})
	rs1.complete(RequestResult{Value: 10})
	rs1.Release()
	if !rs1.recycled.Load() {
		t.Fatal("first lifecycle: not recycled on Release")
	}
	// A stale delivery left in the channels of the recycled object must
	// not reach the next owner.
	storage.completedC <- RequestResult{Value: 999}
	storage.committedC <- RequestResult{Value: 888}

	// Second lifecycle with the recycled object.
	rs2 := newRequestState(pool, 2, time.Time{}, true)
	if rs2 != storage {
		t.Fatal("pinned pool did not hand back the recycled object")
	}
	if rs2.Key() != 2 {
		t.Errorf("recycled key = %d, want 2", rs2.Key())
	}
	if !rs2.Deadline().IsZero() {
		t.Errorf("recycled deadline = %v, want zero", rs2.Deadline())
	}
	if rs2.released.Load() || rs2.recycled.Load() || rs2.completed.Load() || rs2.committedDone.Load() {
		t.Errorf("recycled flags not reset: released=%v recycled=%v completed=%v committedDone=%v",
			rs2.released.Load(), rs2.recycled.Load(), rs2.completed.Load(), rs2.committedDone.Load())
	}
	if got := rs2.outstanding.Load(); got != 0 {
		t.Errorf("recycled outstanding = %d, want 0", got)
	}

	// Channels should be empty.
	select {
	case r := <-rs2.completedC:
		t.Errorf("recycled completedC holds a stale result: %+v", r)
	default:
	}
	select {
	case r := <-rs2.committedC:
		t.Errorf("recycled committedC holds a stale result: %+v", r)
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
	rs := newRequestState(pool, 1, time.Time{}, true)

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
