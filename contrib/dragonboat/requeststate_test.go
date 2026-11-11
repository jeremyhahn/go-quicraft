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
	"errors"
	"runtime"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

func TestRequestResult_RequestOutOfRange(t *testing.T) {
	// Dragonboat signals an out-of-range log query with a dedicated result
	// code carrying the available range, not with an error value.
	r := RequestResult{
		code:     RequestOutOfRange,
		logRange: LogRange{FirstIndex: 4, LastIndex: 9},
	}
	if !r.RequestOutOfRange() {
		t.Error("expected RequestOutOfRange() to be true")
	}
	entries, lr := r.RaftLogs()
	if entries != nil {
		t.Errorf("expected nil entries, got %v", entries)
	}
	if lr.FirstIndex != 4 || lr.LastIndex != 9 {
		t.Errorf("LogRange = %+v, want {4 9}", lr)
	}
}

func TestRequestResult_RaftLogs_WithEntries(t *testing.T) {
	entries := []Entry{
		{Index: 5, Term: 1, Cmd: []byte("a")},
		{Index: 6, Term: 1, Cmd: []byte("b")},
		{Index: 7, Term: 2, Cmd: []byte("c")},
	}
	r := RequestResult{
		code:     RequestCompleted,
		entries:  entries,
		logRange: LogRange{FirstIndex: 5, LastIndex: 8},
	}
	gotEntries, lr := r.RaftLogs()
	if len(gotEntries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(gotEntries))
	}
	if lr.FirstIndex != 5 {
		t.Errorf("FirstIndex = %d, want 5", lr.FirstIndex)
	}
	if lr.LastIndex != 8 {
		t.Errorf("LastIndex = %d, want 8", lr.LastIndex)
	}
}

func TestRequestResultCode_String(t *testing.T) {
	tests := []struct {
		code RequestResultCode
		want string
	}{
		{RequestTimeout, "RequestTimeout"},
		{RequestCompleted, "RequestCompleted"},
		{RequestTerminated, "RequestTerminated"},
		{RequestRejected, "RequestRejected"},
		{RequestDropped, "RequestDropped"},
		{RequestAborted, "RequestAborted"},
		{RequestCommitted, "RequestCommitted"},
		{RequestOutOfRange, "RequestOutOfRange"},
		{RequestResultCode(99), "RequestResultCode(99)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := tc.code.String()
			if got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslateResultCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want RequestResultCode
	}{
		{name: "nil_error", err: nil, want: RequestCompleted},
		{name: "timeout", err: quicraft.ErrTimeout, want: RequestTimeout},
		{name: "rejected", err: quicraft.ErrRejected, want: RequestRejected},
		{name: "dropped", err: quicraft.ErrDropped, want: RequestDropped},
		{name: "aborted", err: quicraft.ErrAborted, want: RequestAborted},
		{name: "closed", err: quicraft.ErrClosed, want: RequestTerminated},
		{name: "stopped", err: quicraft.ErrStopped, want: RequestTerminated},
		{name: "canceled", err: quicraft.ErrCanceled, want: RequestAborted},
		{name: "session_expired", err: quicraft.ErrSessionExpired, want: RequestRejected},
		{name: "session_not_found", err: quicraft.ErrSessionNotFound, want: RequestRejected},
		{name: "session_already_exists", err: quicraft.ErrSessionAlreadyExists, want: RequestRejected},
		{name: "not_leader", err: quicraft.ErrNotLeader, want: RequestDropped},
		{name: "readindex_not_ready", err: quicraft.ErrReadIndexNotReady, want: RequestDropped},
		{name: "shard_not_ready", err: quicraft.ErrShardNotReady, want: RequestDropped},
		{name: "disk_full", err: quicraft.ErrDiskFull, want: RequestDropped},
		{name: "busy", err: quicraft.ErrBusy, want: RequestDropped},
		{name: "system_busy", err: quicraft.ErrSystemBusy, want: RequestDropped},
		{name: "draining", err: quicraft.ErrDraining, want: RequestDropped},
		{name: "shard_paused", err: quicraft.ErrShardPaused, want: RequestDropped},
		{name: "unknown", err: errors.New("no mapping"), want: RequestAborted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translateResultCode(tc.err)
			if got != tc.want {
				t.Errorf("translateResultCode(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

func TestTranslateResult(t *testing.T) {
	qr := quicraft.RequestResult{Value: 55, Data: []byte("data"), Err: nil}
	r := translateResult(qr)
	if !r.Completed() {
		t.Error("expected Completed() for nil error")
	}
	result := r.GetResult()
	if result.Value != 55 {
		t.Errorf("Value = %d, want 55", result.Value)
	}
	if string(result.Data) != "data" {
		t.Errorf("Data = %q, want %q", string(result.Data), "data")
	}
	// A completed snapshot request surfaces the quicraft result Value as
	// the snapshot index, matching dragonboat's behavior of returning the
	// request result's value.
	if r.SnapshotIndex() != 55 {
		t.Errorf("SnapshotIndex() = %d, want 55", r.SnapshotIndex())
	}

	r = translateResult(quicraft.RequestResult{Err: quicraft.ErrTimeout})
	if !r.Timeout() {
		t.Error("expected Timeout() for ErrTimeout")
	}
	if r.Completed() {
		t.Error("expected Completed() to be false for timeout")
	}
}

func TestRequestState_Release_NilInner(t *testing.T) {
	rs := &RequestState{}
	// Should not panic.
	rs.Release()
}

func TestRequestState_ShardID(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestStateForShard(inner, 42, false)
	if rs.ShardID() != 42 {
		t.Errorf("ShardID() = %d, want 42", rs.ShardID())
	}
	rs.Release()
}

func TestRequestState_makeAdaptedChannel_NilInner(t *testing.T) {
	rs := &RequestState{inner: nil}
	ch := rs.makeAdaptedChannel()
	if ch != nil {
		t.Error("makeAdaptedChannel(nil inner) should return nil")
	}
}

func TestRequestResult_AllCodes(t *testing.T) {
	codes := []struct {
		code       RequestResultCode
		timeout    bool
		completed  bool
		rejected   bool
		term       bool
		dropped    bool
		aborted    bool
		committed  bool
		outOfRange bool
	}{
		{RequestTimeout, true, false, false, false, false, false, false, false},
		{RequestCompleted, false, true, false, false, false, false, true, false},
		{RequestTerminated, false, false, false, true, false, false, false, false},
		{RequestRejected, false, false, true, false, false, false, false, false},
		{RequestDropped, false, false, false, false, true, false, false, false},
		{RequestAborted, false, false, false, false, false, true, false, false},
		{RequestCommitted, false, false, false, false, false, false, true, false},
		{RequestOutOfRange, false, false, false, false, false, false, false, true},
	}
	for _, tc := range codes {
		r := RequestResult{code: tc.code}
		if r.Timeout() != tc.timeout {
			t.Errorf("code %d: Timeout() = %v, want %v", tc.code, r.Timeout(), tc.timeout)
		}
		if r.Completed() != tc.completed {
			t.Errorf("code %d: Completed() = %v, want %v", tc.code, r.Completed(), tc.completed)
		}
		if r.Rejected() != tc.rejected {
			t.Errorf("code %d: Rejected() = %v, want %v", tc.code, r.Rejected(), tc.rejected)
		}
		if r.Terminated() != tc.term {
			t.Errorf("code %d: Terminated() = %v, want %v", tc.code, r.Terminated(), tc.term)
		}
		if r.Dropped() != tc.dropped {
			t.Errorf("code %d: Dropped() = %v, want %v", tc.code, r.Dropped(), tc.dropped)
		}
		if r.Aborted() != tc.aborted {
			t.Errorf("code %d: Aborted() = %v, want %v", tc.code, r.Aborted(), tc.aborted)
		}
		if r.Committed() != tc.committed {
			t.Errorf("code %d: Committed() = %v, want %v", tc.code, r.Committed(), tc.committed)
		}
		if r.RequestOutOfRange() != tc.outOfRange {
			t.Errorf("code %d: RequestOutOfRange() = %v, want %v", tc.code, r.RequestOutOfRange(), tc.outOfRange)
		}
	}
}

// ---------------------------------------------------------------------------
// RequestState ShardID on non-shard wrapper
// ---------------------------------------------------------------------------

func TestRequestState_ShardID_ZeroDefault(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner, false)
	if rs.ShardID() != 0 {
		t.Errorf("ShardID() = %d, want 0 for non-shard wrapper", rs.ShardID())
	}
	rs.Release()
}

// ---------------------------------------------------------------------------
// WrapSysOpState
// ---------------------------------------------------------------------------

func TestWrapSysOpState_Release(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	// Release BEFORE wrapping, so the wrapper's goroutine is certain to see a
	// released state (ResultC == nil) and complete at once. Releasing after
	// wrapping raced that goroutine: when it read ResultC first it held a live
	// channel a released, never-completed request never closes, and the test
	// timed out under load. In production a SysOpState releases its own inner
	// state through sysOp.Release, which also stops that goroutine.
	inner.Release()
	sysOp := WrapSysOpState(inner)

	// Wait for completion.
	select {
	case <-sysOp.CompletedC():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	// Release should not panic.
	sysOp.Release()
}

// ---------------------------------------------------------------------------
// AppliedC on wrapped (non-resolved) RequestState
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// WrapRequestStateWithCancel
// ---------------------------------------------------------------------------

func TestWrapRequestStateWithCancel_CancelOnRelease(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	canceled := false
	cancel := func() { canceled = true }

	rs := WrapRequestStateWithCancel(inner, cancel, false)
	if canceled {
		t.Error("cancel should not be called before Release")
	}

	rs.Release()
	if !canceled {
		t.Error("cancel should be called on Release")
	}
}

func TestWrapRequestStateForShardWithCancel_CancelNotCalledPrematurely(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	canceled := false
	cancel := func() { canceled = true }

	rs := WrapRequestStateForShardWithCancel(inner, 42, cancel, false)
	if rs.ShardID() != 42 {
		t.Errorf("ShardID() = %d, want 42", rs.ShardID())
	}

	// Access channels before release -- cancel should not fire.
	ch := rs.ResultC()
	if ch == nil {
		t.Error("expected non-nil ResultC channel")
	}
	if canceled {
		t.Error("cancel should not be called before Release")
	}

	rs.Release()
	if !canceled {
		t.Error("cancel should be called on Release")
	}
}

func TestWrapRequestStateWithCancel_DoubleRelease(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	callCount := 0
	cancel := func() { callCount++ }

	rs := WrapRequestStateWithCancel(inner, cancel, false)
	rs.Release()
	// A second Release is absorbed by the adapter: the inner object is pooled
	// and may already belong to another request, so nothing — not even the
	// context cancel, which the first call already ran — is repeated.
	rs.Release()
	if callCount != 1 {
		t.Errorf("cancel called %d times, expected 1 (the second Release is a no-op)", callCount)
	}
}

// TestSysOpState_DoubleReleaseReachesInnerOnce pins the same rule for the
// system-operation wrapper: only the first Release reaches the inner
// RequestState, which a second call could otherwise find recycled.
func TestSysOpState_DoubleReleaseReachesInnerOnce(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	s := &SysOpState{inner: inner}
	s.Release()
	s.Release()
	if !s.released.Load() {
		t.Fatal("released not recorded")
	}
}

// ---------------------------------------------------------------------------
// Goroutine leak prevention tests
// ---------------------------------------------------------------------------

func TestSysOpState_ReleaseBeforeCompletion_NoGoroutineLeak(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	// Create multiple SysOpStates whose inner channels will never
	// receive a value, then Release them immediately.
	const count = 50
	states := make([]*SysOpState, count)
	for i := range states {
		pool := quicraft.NewRequestStatePool()
		inner := pool.Get().(*quicraft.RequestState)
		states[i] = WrapSysOpState(inner)
	}

	// Release all before any result is delivered. Release waits for the
	// wrapper goroutine, so no grace sleep is needed before counting.
	for _, s := range states {
		s.Release()
	}

	after := runtime.NumGoroutine()
	leaked := after - baseline
	if leaked > 5 {
		t.Errorf("SysOpState goroutine leak: baseline=%d, after=%d, leaked=%d", baseline, after, leaked)
	}
}

func TestSysOpState_DoubleRelease_NoPanic(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	sysOp := WrapSysOpState(inner)

	// Double release should not panic (doneOnce protects close).
	sysOp.Release()
	sysOp.Release()
}

func TestRequestState_ReleaseBeforeResult_CommittedChannel_NoLeak(t *testing.T) {
	// Verify the ResultC aggregate bridge goroutine also exits on Release.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const count = 50
	states := make([]*RequestState, count)
	for i := range states {
		pool := quicraft.NewRequestStatePool()
		inner := pool.Get().(*quicraft.RequestState)
		states[i] = WrapRequestState(inner, true)
		// Touch ResultC so the aggregate bridge goroutine is actually
		// created; it is spawned lazily on first use.
		if states[i].ResultC() == nil {
			t.Fatalf("expected non-nil ResultC for state %d", i)
		}
	}

	// Release waits for its bridges, so the goroutine count is back at the
	// baseline the moment the loop finishes; no grace sleep needed.
	for _, rs := range states {
		rs.Release()
	}

	after := runtime.NumGoroutine()
	leaked := after - baseline
	if leaked > 5 {
		t.Errorf("committed channel goroutine leak: baseline=%d, after=%d, leaked=%d", baseline, after, leaked)
	}
}

// ---------------------------------------------------------------------------
// Resolved RequestState multiple independent reads
// ---------------------------------------------------------------------------

func TestResolvedRequestState_MultipleReads(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 88, Data: []byte("multi")},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	// ResultC, AppliedC and CompletedC must all be the SAME channel, each
	// call returning the same instance. Handing out a fresh pre-loaded
	// channel per call makes the idiomatic dragonboat receive loop spin
	// forever on the same value.
	if rs.ResultC() != rs.AppliedC() || rs.ResultC() != rs.CompletedC {
		t.Fatal("resolved RequestState must hand out one shared channel")
	}
	if first, second := rs.ResultC(), rs.ResultC(); first != second {
		t.Fatal("ResultC must return the same channel on every call")
	}

	select {
	case r := <-rs.ResultC():
		if r.GetResult().Value != 88 {
			t.Errorf("ResultC Value = %d, want 88", r.GetResult().Value)
		}
		if string(r.GetResult().Data) != "multi" {
			t.Errorf("ResultC Data = %q, want %q", string(r.GetResult().Data), "multi")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from ResultC")
	}

	// The single buffered value is now consumed, matching a real request.
	select {
	case <-rs.AppliedC():
		t.Fatal("resolved RequestState delivered a second value")
	default:
	}

	// Release after consuming the result must not panic.
	rs.Release()
}

func BenchmarkTranslateResultCode(b *testing.B) {
	// Benchmark with a known error (first entry) to measure the fast path.
	b.Run("KnownError_First", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateResultCode(quicraft.ErrTimeout)
		}
	})

	// Benchmark with a known error near the end of the table.
	b.Run("KnownError_Last", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateResultCode(quicraft.ErrCanceled)
		}
	})

	// Benchmark with nil (early return).
	b.Run("NilError", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateResultCode(nil)
		}
	})

	// Benchmark with an unknown error (full table scan, no match).
	b.Run("UnknownError", func(b *testing.B) {
		unknown := errors.New("unknown")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = translateResultCode(unknown)
		}
	})
}
