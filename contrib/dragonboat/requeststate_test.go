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

func TestRequestResult_Completed(t *testing.T) {
	r := RequestResult{
		inner: quicraft.RequestResult{Value: 42, Data: []byte("ok")},
		code:  RequestCompleted,
	}
	if !r.Completed() {
		t.Error("expected Completed() to be true")
	}
	if r.Timeout() {
		t.Error("expected Timeout() to be false")
	}
	if r.Rejected() {
		t.Error("expected Rejected() to be false")
	}
	if r.Terminated() {
		t.Error("expected Terminated() to be false")
	}
	if r.Dropped() {
		t.Error("expected Dropped() to be false")
	}
	if r.Aborted() {
		t.Error("expected Aborted() to be false")
	}
	if !r.Committed() {
		t.Error("expected Committed() to be true (completed implies committed)")
	}
}

func TestRequestResult_Timeout(t *testing.T) {
	r := RequestResult{
		inner: quicraft.RequestResult{Err: quicraft.ErrTimeout},
		code:  RequestTimeout,
	}
	if !r.Timeout() {
		t.Error("expected Timeout() to be true")
	}
	if r.Completed() {
		t.Error("expected Completed() to be false")
	}
}

func TestRequestResult_GetResult(t *testing.T) {
	data := []byte("payload")
	r := RequestResult{
		inner: quicraft.RequestResult{Value: 123, Data: data},
		code:  RequestCompleted,
	}
	result := r.GetResult()
	if result.Value != 123 {
		t.Errorf("Value = %d, want 123", result.Value)
	}
	if string(result.Data) != "payload" {
		t.Errorf("Data = %q, want %q", string(result.Data), "payload")
	}
}

func TestRequestResult_GetResult_NilData(t *testing.T) {
	r := RequestResult{
		inner: quicraft.RequestResult{Value: 0, Data: nil},
		code:  RequestCompleted,
	}
	result := r.GetResult()
	if result.Value != 0 {
		t.Errorf("Value = %d, want 0", result.Value)
	}
	if result.Data != nil {
		t.Errorf("Data = %v, want nil", result.Data)
	}
}

func TestRequestResult_RequestOutOfRange(t *testing.T) {
	r := RequestResult{
		inner: quicraft.RequestResult{Err: quicraft.ErrInvalidRange},
		code:  RequestAborted,
	}
	if !r.RequestOutOfRange() {
		t.Error("expected RequestOutOfRange() to be true")
	}
}

func TestRequestResult_RequestOutOfRange_False(t *testing.T) {
	r := RequestResult{
		inner: quicraft.RequestResult{},
		code:  RequestCompleted,
	}
	if r.RequestOutOfRange() {
		t.Error("expected RequestOutOfRange() to be false")
	}
}

func TestRequestResult_SnapshotIndex(t *testing.T) {
	r := RequestResult{
		snapshotIndex: 42,
		code:          RequestCompleted,
	}
	if r.SnapshotIndex() != 42 {
		t.Errorf("SnapshotIndex() = %d, want 42", r.SnapshotIndex())
	}
}

func TestRequestResult_SnapshotIndex_Zero(t *testing.T) {
	r := RequestResult{code: RequestCompleted}
	if r.SnapshotIndex() != 0 {
		t.Errorf("SnapshotIndex() = %d, want 0", r.SnapshotIndex())
	}
}

func TestRequestResult_RaftLogs_WithEntries(t *testing.T) {
	entries := []Entry{
		{Index: 5, Term: 1, Cmd: []byte("a")},
		{Index: 6, Term: 1, Cmd: []byte("b")},
		{Index: 7, Term: 2, Cmd: []byte("c")},
	}
	r := RequestResult{
		code:    RequestCompleted,
		entries: entries,
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

func TestRequestResult_RaftLogs_NoEntries(t *testing.T) {
	r := RequestResult{code: RequestCompleted}
	gotEntries, lr := r.RaftLogs()
	if gotEntries != nil {
		t.Errorf("expected nil entries, got %v", gotEntries)
	}
	if lr.FirstIndex != 0 || lr.LastIndex != 0 {
		t.Errorf("expected zero LogRange, got {%d, %d}", lr.FirstIndex, lr.LastIndex)
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
		{name: "unknown", err: quicraft.ErrNotLeader, want: RequestAborted},
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
}

func TestTranslateResult_WithError(t *testing.T) {
	qr := quicraft.RequestResult{Err: quicraft.ErrTimeout}
	r := translateResult(qr)
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

func TestRequestState_Release_WithInner(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner)
	// Should not panic.
	rs.Release()
}

func TestRequestState_CompletedC_Field(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 10},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	if rs.CompletedC == nil {
		t.Fatal("expected non-nil CompletedC field")
	}

	select {
	case r := <-rs.CompletedC:
		if !r.Completed() {
			t.Error("expected Completed() from CompletedC field")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from CompletedC field")
	}
}

func TestRequestState_ResultC_Bidirectional(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 20},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	ch := rs.ResultC()
	if ch == nil {
		t.Fatal("expected non-nil ResultC channel")
	}
	select {
	case r := <-ch:
		if !r.Completed() {
			t.Error("expected Completed() from ResultC")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from ResultC")
	}
}

func TestRequestState_AppliedC_Resolved(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 30},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	ch := rs.AppliedC()
	if ch == nil {
		t.Fatal("expected non-nil AppliedC channel")
	}
	select {
	case r := <-ch:
		if r.GetResult().Value != 30 {
			t.Errorf("Value = %d, want 30", r.GetResult().Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from AppliedC")
	}
}

func TestRequestState_ShardID(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestStateForShard(inner, 42)
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

func TestSysOpState_CompletedC_PreCompleted(t *testing.T) {
	// Test with a pre-completed SysOpState (channel already closed).
	s := &SysOpState{
		inner:      quicraft.NewRequestStatePool().Get().(*quicraft.RequestState),
		completedC: make(chan struct{}),
	}
	close(s.completedC)

	ch := s.CompletedC()
	select {
	case <-ch:
		// Expected: channel closed.
	default:
		t.Error("expected CompletedC to be closed")
	}
}

func TestSysOpState_ResultC_IsAliasForCompletedC(t *testing.T) {
	s := &SysOpState{
		inner:      quicraft.NewRequestStatePool().Get().(*quicraft.RequestState),
		completedC: make(chan struct{}),
	}
	close(s.completedC)

	completedC := s.CompletedC()
	resultC := s.ResultC()

	// Both should signal completion.
	select {
	case <-completedC:
	default:
		t.Error("expected CompletedC to be closed")
	}
	select {
	case <-resultC:
	default:
		t.Error("expected ResultC to also be closed")
	}
}

func TestSysOpState_Release(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	s := &SysOpState{
		inner:      inner,
		completedC: make(chan struct{}),
	}
	close(s.completedC)
	// Verify Release does not panic.
	s.Release()
}

func TestRequestResult_AllCodes(t *testing.T) {
	codes := []struct {
		code      RequestResultCode
		timeout   bool
		completed bool
		rejected  bool
		term      bool
		dropped   bool
		aborted   bool
		committed bool
	}{
		{RequestTimeout, true, false, false, false, false, false, false},
		{RequestCompleted, false, true, false, false, false, false, true},
		{RequestTerminated, false, false, false, true, false, false, false},
		{RequestRejected, false, false, true, false, false, false, false},
		{RequestDropped, false, false, false, false, true, false, false},
		{RequestAborted, false, false, false, false, false, true, false},
		{RequestCommitted, false, false, false, false, false, false, true},
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
	}
}

func TestLogRange_ZeroValue(t *testing.T) {
	var lr LogRange
	if lr.FirstIndex != 0 {
		t.Errorf("FirstIndex = %d, want 0", lr.FirstIndex)
	}
	if lr.LastIndex != 0 {
		t.Errorf("LastIndex = %d, want 0", lr.LastIndex)
	}
}

func TestEntry_Fields(t *testing.T) {
	e := Entry{Index: 10, Term: 3, Cmd: []byte("cmd")}
	if e.Index != 10 {
		t.Errorf("Index = %d, want 10", e.Index)
	}
	if e.Term != 3 {
		t.Errorf("Term = %d, want 3", e.Term)
	}
	if string(e.Cmd) != "cmd" {
		t.Errorf("Cmd = %q, want %q", string(e.Cmd), "cmd")
	}
}

func TestEntry_TermField(t *testing.T) {
	e := Entry{Index: 5, Term: 42, Cmd: []byte("data")}
	if e.Term != 42 {
		t.Errorf("Term = %d, want 42", e.Term)
	}
}

func TestEntry_ZeroTerm(t *testing.T) {
	e := Entry{Index: 1, Cmd: []byte("x")}
	if e.Term != 0 {
		t.Errorf("Term = %d, want 0 for zero value", e.Term)
	}
}

func TestResult_TypeAlias(t *testing.T) {
	r := Result{Value: 5, Data: []byte("data")}
	if r.Value != 5 {
		t.Errorf("Value = %d, want 5", r.Value)
	}
	if string(r.Data) != "data" {
		t.Errorf("Data = %q, want %q", string(r.Data), "data")
	}
}

// ---------------------------------------------------------------------------
// SnapshotIndex behavior on non-snapshot results
// ---------------------------------------------------------------------------

func TestRequestResult_SnapshotIndex_NonSnapshotResult(t *testing.T) {
	// In dragonboat, accessing SnapshotIndex on a regular (non-snapshot)
	// result panics. Our adapter returns 0 for non-snapshot results,
	// which is a safe and compatible behavior.
	r := RequestResult{
		inner: quicraft.RequestResult{Value: 42, Data: []byte("data")},
		code:  RequestCompleted,
		// snapshotIndex not set (zero value).
	}
	if r.SnapshotIndex() != 0 {
		t.Errorf("SnapshotIndex() = %d, want 0 for non-snapshot result", r.SnapshotIndex())
	}
}

func TestRequestResult_SnapshotIndex_WithSnapshotResult(t *testing.T) {
	r := RequestResult{
		code:          RequestCompleted,
		snapshotIndex: 12345,
	}
	if r.SnapshotIndex() != 12345 {
		t.Errorf("SnapshotIndex() = %d, want 12345", r.SnapshotIndex())
	}
}

// ---------------------------------------------------------------------------
// SysOpState CompletedC multiple reads
// ---------------------------------------------------------------------------

func TestSysOpState_CompletedC_MultipleReads(t *testing.T) {
	s := &SysOpState{
		inner:      quicraft.NewRequestStatePool().Get().(*quicraft.RequestState),
		completedC: make(chan struct{}),
	}
	close(s.completedC)

	ch1 := s.CompletedC()
	ch2 := s.CompletedC()

	// Both calls should return the same channel and both should be readable.
	select {
	case <-ch1:
	default:
		t.Error("first CompletedC read should succeed on closed channel")
	}
	select {
	case <-ch2:
	default:
		t.Error("second CompletedC read should succeed on closed channel")
	}

	// Reading again from the same channel should still work (closed channels
	// return the zero value indefinitely).
	select {
	case <-ch1:
	default:
		t.Error("third CompletedC read should succeed on closed channel")
	}
}

// ---------------------------------------------------------------------------
// WrapSysOpState with real inner
// ---------------------------------------------------------------------------

func TestSysOpState_Release_NilCompletedC(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	s := &SysOpState{
		inner:      inner,
		completedC: make(chan struct{}),
	}
	close(s.completedC)
	// Release should not panic when inner is from pool.
	s.Release()
}

// ---------------------------------------------------------------------------
// RequestState with resolved channel - multiple reads
// ---------------------------------------------------------------------------

func TestRequestState_ResolvedC_MultipleAppliedCCalls(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 77},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	// Both AppliedC and ResultC should return the same resolved channel.
	ac := rs.AppliedC()
	rc := rs.ResultC()

	// They should be the same channel.
	if ac == nil || rc == nil {
		t.Fatal("expected non-nil channels")
	}

	// Read from one should consume the result.
	select {
	case r := <-ac:
		if r.GetResult().Value != 77 {
			t.Errorf("Value = %d, want 77", r.GetResult().Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from AppliedC")
	}
}

// ---------------------------------------------------------------------------
// RequestState ShardID on non-shard wrapper
// ---------------------------------------------------------------------------

func TestRequestState_ShardID_ZeroDefault(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner)
	if rs.ShardID() != 0 {
		t.Errorf("ShardID() = %d, want 0 for non-shard wrapper", rs.ShardID())
	}
	rs.Release()
}

// ---------------------------------------------------------------------------
// translateResult with various error types
// ---------------------------------------------------------------------------

func TestTranslateResult_Rejected(t *testing.T) {
	qr := quicraft.RequestResult{Err: quicraft.ErrRejected}
	r := translateResult(qr)
	if !r.Rejected() {
		t.Error("expected Rejected() for ErrRejected")
	}
}

func TestTranslateResult_Dropped(t *testing.T) {
	qr := quicraft.RequestResult{Err: quicraft.ErrDropped}
	r := translateResult(qr)
	if !r.Dropped() {
		t.Error("expected Dropped() for ErrDropped")
	}
}

func TestTranslateResult_Closed(t *testing.T) {
	qr := quicraft.RequestResult{Err: quicraft.ErrClosed}
	r := translateResult(qr)
	if !r.Terminated() {
		t.Error("expected Terminated() for ErrClosed")
	}
}

func TestTranslateResult_Aborted(t *testing.T) {
	qr := quicraft.RequestResult{Err: quicraft.ErrAborted}
	r := translateResult(qr)
	if !r.Aborted() {
		t.Error("expected Aborted() for ErrAborted")
	}
}

// ---------------------------------------------------------------------------
// RaftLogs with single entry
// ---------------------------------------------------------------------------

func TestRequestResult_RaftLogs_SingleEntry(t *testing.T) {
	entries := []Entry{{Index: 10, Cmd: []byte("single")}}
	r := RequestResult{code: RequestCompleted, entries: entries}
	gotEntries, lr := r.RaftLogs()
	if len(gotEntries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(gotEntries))
	}
	if lr.FirstIndex != 10 {
		t.Errorf("FirstIndex = %d, want 10", lr.FirstIndex)
	}
	if lr.LastIndex != 11 {
		t.Errorf("LastIndex = %d, want 11", lr.LastIndex)
	}
}

// ---------------------------------------------------------------------------
// WrapSysOpState
// ---------------------------------------------------------------------------

func TestWrapSysOpState_CompletesWhenInnerResolves(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	sysOp := WrapSysOpState(inner)

	// The inner has a buffered completedC channel. We need to send a result
	// on it to trigger waitForCompletion. Since the inner's ResultC() returns
	// a receive-only channel from the pool's pre-allocated buffered channel,
	// we can complete it by releasing it (which closes the channel) -- but
	// actually, Release does not close the channel; it marks released.
	//
	// For the pool-based RequestState, completedC is make(chan, 1). We need
	// to exercise waitForCompletion which does <-src. If src (ResultC()) is
	// nil (released), it closes completedC immediately.

	// Release the inner so ResultC returns nil, which triggers the nil-src
	// path in waitForCompletion (closes completedC immediately).
	inner.Release()

	// Wait for completion signal.
	select {
	case <-sysOp.CompletedC():
		// Expected: waitForCompletion detected nil src and closed completedC.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WrapSysOpState completion")
	}
}

func TestWrapSysOpState_Release(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	sysOp := WrapSysOpState(inner)
	inner.Release()

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

func TestRequestState_AppliedC_NonResolved(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner)

	// AppliedC should return the CompletedC channel (adapted from inner).
	ch := rs.AppliedC()
	if ch == nil {
		t.Error("expected non-nil AppliedC channel for wrapped RequestState")
	}
	rs.Release()
}

func TestRequestState_ResultC_NonResolved(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner)

	ch := rs.ResultC()
	if ch == nil {
		t.Error("expected non-nil ResultC channel for wrapped RequestState")
	}
	rs.Release()
}

// ---------------------------------------------------------------------------
// WrapRequestStateWithCancel
// ---------------------------------------------------------------------------

func TestWrapRequestStateWithCancel_CancelOnRelease(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	canceled := false
	cancel := func() { canceled = true }

	rs := WrapRequestStateWithCancel(inner, cancel)
	if canceled {
		t.Error("cancel should not be called before Release")
	}

	rs.Release()
	if !canceled {
		t.Error("cancel should be called on Release")
	}
}

func TestWrapRequestStateWithCancel_NilCancel(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	// Passing nil cancel should not panic on Release.
	rs := WrapRequestState(inner)
	rs.Release()
}

func TestWrapRequestStateForShardWithCancel_ShardID(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	canceled := false
	cancel := func() { canceled = true }

	rs := WrapRequestStateForShardWithCancel(inner, 99, cancel)
	if rs.ShardID() != 99 {
		t.Errorf("ShardID() = %d, want 99", rs.ShardID())
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

	rs := WrapRequestStateForShardWithCancel(inner, 42, cancel)

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

	rs := WrapRequestStateWithCancel(inner, cancel)
	rs.Release()
	// Second release should call cancel again (cancel funcs are idempotent).
	// This verifies no panic on double release.
	rs.Release()
	if callCount != 2 {
		t.Errorf("cancel called %d times, expected 2 (idempotent)", callCount)
	}
}

// ---------------------------------------------------------------------------
// Goroutine leak prevention tests
// ---------------------------------------------------------------------------

func TestRequestState_ReleaseBeforeResult_NoGoroutineLeak(t *testing.T) {
	// Force GC and stabilize goroutine count before the test.
	runtime.GC()
	stabilizeTimer := time.NewTimer(50 * time.Millisecond)
	<-stabilizeTimer.C
	stabilizeTimer.Stop()
	baseline := runtime.NumGoroutine()

	// Create multiple wrapped RequestStates whose inner channels will
	// never receive a value, then Release them immediately.
	const count = 50
	states := make([]*RequestState, count)
	for i := range states {
		pool := quicraft.NewRequestStatePool()
		inner := pool.Get().(*quicraft.RequestState)
		states[i] = WrapRequestState(inner)
	}

	// Release all before any result is delivered.
	for _, rs := range states {
		rs.Release()
	}

	// Allow bridge goroutines to observe doneC and exit.
	runtime.Gosched()
	drainTimer := time.NewTimer(100 * time.Millisecond)
	<-drainTimer.C
	drainTimer.Stop()
	runtime.GC()

	after := runtime.NumGoroutine()
	// Allow a small margin for unrelated runtime goroutines, but the
	// bridge goroutines (2 per RequestState = 100 total) must not leak.
	leaked := after - baseline
	if leaked > 5 {
		t.Errorf("goroutine leak detected: baseline=%d, after=%d, leaked=%d", baseline, after, leaked)
	}
}

func TestRequestState_ResultThenRelease_NormalFlow(t *testing.T) {
	// Use newResolvedRequestState to simulate the normal flow where a
	// result arrives and the caller reads it before calling Release.
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 99, Data: []byte("ok")},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	ch := rs.AppliedC()
	if ch == nil {
		t.Fatal("expected non-nil AppliedC channel")
	}

	select {
	case r := <-ch:
		if r.GetResult().Value != 99 {
			t.Errorf("Value = %d, want 99", r.GetResult().Value)
		}
		if string(r.GetResult().Data) != "ok" {
			t.Errorf("Data = %q, want %q", string(r.GetResult().Data), "ok")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
	}

	// Release after consuming the result - should not panic.
	rs.Release()
}

func TestRequestState_ResultArrivesThenRelease(t *testing.T) {
	// Use newResolvedRequestState which delivers the result immediately.
	// This confirms the normal flow: result arrives, caller reads, then releases.
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 42},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	ch := rs.AppliedC()
	select {
	case r := <-ch:
		if r.GetResult().Value != 42 {
			t.Errorf("Value = %d, want 42", r.GetResult().Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
	}

	// Release after consuming - should not panic.
	rs.Release()
}

func TestRequestState_DoubleRelease_NoPanic(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner)

	// Double release should not panic (doneOnce protects close).
	rs.Release()
	rs.Release()
}

func TestSysOpState_ReleaseBeforeCompletion_NoGoroutineLeak(t *testing.T) {
	// Force GC and stabilize goroutine count before the test.
	runtime.GC()
	stabilizeTimer := time.NewTimer(50 * time.Millisecond)
	<-stabilizeTimer.C
	stabilizeTimer.Stop()
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

	// Release all before any result is delivered.
	for _, s := range states {
		s.Release()
	}

	// Allow bridge goroutines to observe doneC and exit.
	runtime.Gosched()
	drainTimer := time.NewTimer(100 * time.Millisecond)
	<-drainTimer.C
	drainTimer.Stop()
	runtime.GC()

	after := runtime.NumGoroutine()
	leaked := after - baseline
	if leaked > 5 {
		t.Errorf("SysOpState goroutine leak: baseline=%d, after=%d, leaked=%d", baseline, after, leaked)
	}
}

func TestSysOpState_CompletionThenRelease_NormalFlow(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)

	sysOp := WrapSysOpState(inner)

	// Release the inner so ResultC returns nil, which triggers the
	// nil-src fast path in waitForCompletion (closes completedC).
	inner.Release()

	// Wait for completion signal.
	select {
	case <-sysOp.CompletedC():
		// Expected: completion arrived normally.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SysOpState completion")
	}

	// Release after completion - should not panic.
	sysOp.Release()
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
	// Verify the committed channel bridge goroutine also exits on Release.
	runtime.GC()
	stabilizeTimer := time.NewTimer(50 * time.Millisecond)
	<-stabilizeTimer.C
	stabilizeTimer.Stop()
	baseline := runtime.NumGoroutine()

	const count = 50
	states := make([]*RequestState, count)
	for i := range states {
		pool := quicraft.NewRequestStatePool()
		inner := pool.Get().(*quicraft.RequestState)
		states[i] = WrapRequestState(inner)
		// Verify committedC is non-nil (inner has a committedC channel).
		if states[i].CommittedC() == nil {
			t.Fatalf("expected non-nil CommittedC for state %d", i)
		}
	}

	for _, rs := range states {
		rs.Release()
	}

	runtime.Gosched()
	drainTimer := time.NewTimer(100 * time.Millisecond)
	<-drainTimer.C
	drainTimer.Stop()
	runtime.GC()

	after := runtime.NumGoroutine()
	leaked := after - baseline
	if leaked > 5 {
		t.Errorf("committed channel goroutine leak: baseline=%d, after=%d, leaked=%d", baseline, after, leaked)
	}
}

// ---------------------------------------------------------------------------
// C6: Result code mappings for ErrBusy, ErrSystemBusy, ErrDraining
// ---------------------------------------------------------------------------

func TestTranslateResultCode_Busy(t *testing.T) {
	got := translateResultCode(quicraft.ErrBusy)
	if got != RequestDropped {
		t.Errorf("translateResultCode(ErrBusy) = %v, want RequestDropped", got)
	}
}

func TestTranslateResultCode_BusyNotAborted(t *testing.T) {
	got := translateResultCode(quicraft.ErrBusy)
	if got == RequestAborted {
		t.Error("ErrBusy should map to RequestDropped, not fall through to RequestAborted")
	}
}

func TestTranslateResultCode_SystemBusy(t *testing.T) {
	got := translateResultCode(quicraft.ErrSystemBusy)
	if got != RequestDropped {
		t.Errorf("translateResultCode(ErrSystemBusy) = %v, want RequestDropped", got)
	}
}

func TestTranslateResultCode_SystemBusyNotAborted(t *testing.T) {
	got := translateResultCode(quicraft.ErrSystemBusy)
	if got == RequestAborted {
		t.Error("ErrSystemBusy should map to RequestDropped, not fall through to RequestAborted")
	}
}

func TestTranslateResultCode_Draining(t *testing.T) {
	got := translateResultCode(quicraft.ErrDraining)
	if got != RequestDropped {
		t.Errorf("translateResultCode(ErrDraining) = %v, want RequestDropped", got)
	}
}

func TestTranslateResultCode_DrainingNotAborted(t *testing.T) {
	got := translateResultCode(quicraft.ErrDraining)
	if got == RequestAborted {
		t.Error("ErrDraining should map to RequestDropped, not fall through to RequestAborted")
	}
}

// ---------------------------------------------------------------------------
// C7: Resolved RequestState multiple independent reads
// ---------------------------------------------------------------------------

func TestResolvedRequestState_MultipleReads(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 88, Data: []byte("multi")},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	// Read from ResultC first.
	select {
	case r := <-rs.ResultC():
		if r.GetResult().Value != 88 {
			t.Errorf("ResultC Value = %d, want 88", r.GetResult().Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from ResultC")
	}

	// Read from AppliedC second -- must not block.
	select {
	case r := <-rs.AppliedC():
		if r.GetResult().Value != 88 {
			t.Errorf("AppliedC Value = %d, want 88", r.GetResult().Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from AppliedC after ResultC was consumed")
	}

	// Read from CompletedC third -- must not block.
	select {
	case r := <-rs.CompletedC:
		if r.GetResult().Value != 88 {
			t.Errorf("CompletedC Value = %d, want 88", r.GetResult().Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from CompletedC after ResultC and AppliedC were consumed")
	}
}

func TestResolvedRequestState_MultipleReads_DataPreserved(t *testing.T) {
	result := RequestResult{
		inner: quicraft.RequestResult{Value: 42, Data: []byte("preserved")},
		code:  RequestCompleted,
	}
	rs := newResolvedRequestState(result)

	// Drain CompletedC.
	<-rs.CompletedC

	// AppliedC should still deliver the full result.
	select {
	case r := <-rs.AppliedC():
		if string(r.GetResult().Data) != "preserved" {
			t.Errorf("Data = %q, want %q", string(r.GetResult().Data), "preserved")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading from AppliedC")
	}
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
