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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Result is a type alias for the statemachine Result type, matching
// dragonboat's convention where sm.Result is re-exported at the top level.
type Result = dbSM.Result

// Entry represents a Raft log entry in the dragonboat adapter layer.
// This is a simplified view of the proto.Entry used by QueryRaftLog
// results and the RaftLogs() method.
type Entry struct {
	// Index is the Raft log index of this entry.
	Index uint64

	// Term is the Raft term when this entry was proposed.
	Term uint64

	// Type identifies the entry type (e.g., normal, config change, metadata).
	// This corresponds to proto.Entry.Type and allows callers to filter
	// entries by kind when processing QueryRaftLog results.
	Type uint64

	// Cmd is the proposal payload.
	Cmd []byte
}

// LogRange describes a contiguous range of Raft log entries.
type LogRange struct {
	// FirstIndex is the first log index in the range (inclusive).
	FirstIndex uint64

	// LastIndex is the last log index in the range (exclusive).
	LastIndex uint64
}

// RequestResultCode identifies the outcome type of an asynchronous request.
type RequestResultCode int

const (
	// RequestTimeout indicates the request exceeded its deadline.
	RequestTimeout RequestResultCode = iota

	// RequestCompleted indicates the request was applied by the state machine.
	RequestCompleted

	// RequestTerminated indicates the request was terminated due to
	// host shutdown or shard closure.
	RequestTerminated

	// RequestRejected indicates the request was rejected, for example
	// when a config change is already in progress.
	RequestRejected

	// RequestDropped indicates the request was dropped due to queue
	// overflow or shutdown in progress.
	RequestDropped

	// RequestAborted indicates the request was aborted due to an
	// internal state transition such as leadership change.
	RequestAborted

	// RequestCommitted indicates the request was committed to the WAL
	// but not yet applied by the state machine.
	RequestCommitted

	// RequestOutOfRange indicates a QueryRaftLog request whose requested
	// index range lies outside the log range currently available. The
	// available range is reported by RequestResult.RaftLogs.
	RequestOutOfRange
)

// requestResultCodeNames maps each RequestResultCode to its string name.
var requestResultCodeNames = map[RequestResultCode]string{
	RequestTimeout:    "RequestTimeout",
	RequestCompleted:  "RequestCompleted",
	RequestTerminated: "RequestTerminated",
	RequestRejected:   "RequestRejected",
	RequestDropped:    "RequestDropped",
	RequestAborted:    "RequestAborted",
	RequestCommitted:  "RequestCommitted",
	RequestOutOfRange: "RequestOutOfRange",
}

// String returns the human-readable name of the RequestResultCode.
func (c RequestResultCode) String() string {
	if name, ok := requestResultCodeNames[c]; ok {
		return name
	}
	return fmt.Sprintf("RequestResultCode(%d)", int(c))
}

// RequestResult wraps a quicraft RequestResult with dragonboat-compatible
// query methods for checking the outcome type.
type RequestResult struct {
	inner quicraft.RequestResult
	code  RequestResultCode

	// entries holds raft log entries for QueryRaftLog results. Since
	// quicraft's QueryRaftLog is synchronous and returns entries directly,
	// the adapter carries them here for the RaftLogs() method.
	entries []Entry

	// logRange is the RaftLogs range: the returned entries, or the available range for RequestOutOfRange.
	logRange LogRange
}

// Timeout returns true if the request exceeded its deadline.
func (r RequestResult) Timeout() bool { return r.code == RequestTimeout }

// Completed returns true if the request was applied by the state machine.
func (r RequestResult) Completed() bool { return r.code == RequestCompleted }

// Rejected returns true if the request was rejected.
func (r RequestResult) Rejected() bool { return r.code == RequestRejected }

// Terminated returns true if the request was terminated.
func (r RequestResult) Terminated() bool { return r.code == RequestTerminated }

// Dropped returns true if the request was dropped.
func (r RequestResult) Dropped() bool { return r.code == RequestDropped }

// Aborted returns true if the request was aborted.
func (r RequestResult) Aborted() bool { return r.code == RequestAborted }

// Committed returns true if the request was committed or completed, as in dragonboat.
func (r RequestResult) Committed() bool {
	return r.code == RequestCommitted || r.code == RequestCompleted
}

// GetResult returns the application-defined result from the state machine.
// Only meaningful when Completed() is true.
func (r RequestResult) GetResult() Result {
	return Result{Value: r.inner.Value, Data: r.inner.Data}
}

// RequestOutOfRange returns true if the requested range was out of bounds.
// This applies to QueryRaftLog requests where the requested index range
// lies outside the log range currently available.
func (r RequestResult) RequestOutOfRange() bool {
	return r.code == RequestOutOfRange
}

// SnapshotIndex returns the snapshot log index carried in the result value; it is meaningful only for snapshot results.
func (r RequestResult) SnapshotIndex() uint64 {
	return r.inner.Value
}

// RaftLogs returns the raft log entries and the log range from a
// QueryRaftLog result. For a RequestOutOfRange result the entries are nil
// and the range is the one currently available in the log.
func (r RequestResult) RaftLogs() ([]Entry, LogRange) {
	if len(r.entries) == 0 {
		return nil, r.logRange
	}
	return r.entries, r.logRange
}

// resultCodeMapping pairs a quicraft error with its dragonboat-compatible
// RequestResultCode.
type resultCodeMapping struct {
	target error
	code   RequestResultCode
}

// resultCodeMappings is the package-level translation table used by
// translateResultCode. Order matters: the first match wins.
var resultCodeMappings = [...]resultCodeMapping{
	// Session-identity failures map to RequestRejected, as dragonboat documents Rejected().
	{quicraft.ErrSessionExpired, RequestRejected},
	{quicraft.ErrSessionNotFound, RequestRejected},
	{quicraft.ErrSessionAlreadyExists, RequestRejected},
	// Leader-unavailable and not-ready failures map to RequestDropped, which callers retry.
	{quicraft.ErrNotLeader, RequestDropped},
	{quicraft.ErrReadIndexNotReady, RequestDropped},
	{quicraft.ErrShardNotReady, RequestDropped},
	{quicraft.ErrDiskFull, RequestDropped},
	{quicraft.ErrShardPaused, RequestDropped},
	{quicraft.ErrTimeout, RequestTimeout},
	{quicraft.ErrRejected, RequestRejected},
	{quicraft.ErrDropped, RequestDropped},
	{quicraft.ErrBusy, RequestDropped},
	{quicraft.ErrSystemBusy, RequestDropped},
	{quicraft.ErrDraining, RequestDropped},
	{quicraft.ErrAborted, RequestAborted},
	{quicraft.ErrClosed, RequestTerminated},
	{quicraft.ErrStopped, RequestTerminated},
	{quicraft.ErrCanceled, RequestAborted},
}

// translateResultCode maps a quicraft RequestResult error to a
// dragonboat-compatible RequestResultCode.
func translateResultCode(err error) RequestResultCode {
	if err == nil {
		return RequestCompleted
	}

	for _, m := range resultCodeMappings {
		if errors.Is(err, m.target) {
			return m.code
		}
	}

	// Default: treat unknown errors as aborted.
	return RequestAborted
}

// translateResult converts a quicraft RequestResult into a dragonboat
// RequestResult with the appropriate result code.
func translateResult(qr quicraft.RequestResult) RequestResult {
	return RequestResult{
		inner: qr,
		code:  translateResultCode(qr.Err),
	}
}

// RequestState wraps quicraft's RequestState for dragonboat-compatible
// asynchronous result consumption.
type RequestState struct {
	inner *quicraft.RequestState

	// CompletedC is a deprecated public field maintained for dragonboat
	// v4 API compatibility. It carries the same result as AppliedC and
	// ResultC. Users should prefer AppliedC() or ResultC() methods.
	CompletedC chan RequestResult

	// notifyCommit records the host's NotifyCommit setting. It selects
	// dragonboat's two-value ResultC contract.
	notifyCommit bool

	// mu guards the lazily created aggrC bridge together with the released
	// flag, so it is never spawned after Release has begun waiting on wg.
	mu sync.Mutex

	// released is set by Release under mu. Once set, no further bridge
	// goroutine is spawned.
	released bool

	// aggrC is the lazy cap-2 ResultC channel under NotifyCommit: commit, then apply; it reads inner.CommittedC directly so no notification is dropped.
	aggrC chan RequestResult

	// shardID records the shard this request belongs to. This is used
	// by ReadLocalNode to query the correct shard after ReadIndex
	// resolves. Set by WrapRequestStateForShard.
	shardID uint64

	// resolved holds the result of a synchronous op; AppliedC and ResultC return the one preloaded resolvedC.
	resolved *RequestResult

	// resolvedC is the single channel handed out for a pre-resolved
	// RequestState. It holds exactly one buffered result, matching the
	// one-value contract of a real request's channel.
	resolvedC chan RequestResult

	// cancelFunc is the async method's context cancel, called by Release.
	cancelFunc context.CancelFunc

	// doneC is closed by Release to signal bridge goroutines to exit.
	// This prevents goroutine leaks when Release is called before the
	// inner result channel delivers a value.
	doneC chan struct{}

	// doneOnce ensures doneC is closed exactly once across multiple
	// Release calls.
	doneOnce sync.Once

	// wg tracks this RequestState's goroutines; Release waits on it before releasing the pooled inner RequestState.
	wg sync.WaitGroup

	// latched is the delivered result, read by ReadLocalNode because the caller has already drained ResultC.
	latched atomic.Pointer[RequestResult]
}

// wrapRequestState builds the wrapper; notifyCommit selects the ResultC contract, and only the apply bridge starts here.
func wrapRequestState(inner *quicraft.RequestState, shardID uint64, cancel context.CancelFunc, notifyCommit bool) *RequestState {
	rs := &RequestState{
		inner:        inner,
		shardID:      shardID,
		cancelFunc:   cancel,
		notifyCommit: notifyCommit,
		doneC:        make(chan struct{}),
	}
	rs.CompletedC = rs.makeAdaptedChannel()
	return rs
}

// WrapRequestState wraps a quicraft RequestState; notifyCommit must match the owning host's NotifyCommit setting.
func WrapRequestState(inner *quicraft.RequestState, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, 0, nil, notifyCommit)
}

// WrapRequestStateForShard wraps a quicraft RequestState with an
// explicit shardID. Used by ReadIndex so that ReadLocalNode can
// determine which shard to query.
func WrapRequestStateForShard(inner *quicraft.RequestState, shardID uint64, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, shardID, nil, notifyCommit)
}

// WrapRequestStateWithCancel wraps a quicraft RequestState whose context cancel runs on Release.
func WrapRequestStateWithCancel(inner *quicraft.RequestState, cancel context.CancelFunc, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, 0, cancel, notifyCommit)
}

// WrapRequestStateForShardWithCancel wraps a quicraft RequestState with its shardID and a context cancel run on Release.
func WrapRequestStateForShardWithCancel(inner *quicraft.RequestState, shardID uint64, cancel context.CancelFunc, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, shardID, cancel, notifyCommit)
}

// ShardID returns the shard this request belongs to.
func (rs *RequestState) ShardID() uint64 {
	return rs.shardID
}

// newResolvedRequestState returns a RequestState that already holds result, for synchronous quicraft APIs.
func newResolvedRequestState(result RequestResult) *RequestState {
	rs := &RequestState{doneC: make(chan struct{})}
	rs.resolved = &result
	// One channel, built once and shared by CompletedC/AppliedC/ResultC.
	// Handing out a fresh pre-loaded channel per call would make the
	// idiomatic dragonboat receive loop spin forever on the same value.
	rs.resolvedC = make(chan RequestResult, 1)
	rs.resolvedC <- result
	rs.CompletedC = rs.resolvedC
	return rs
}

// AppliedC returns a channel that receives one RequestResult when the request is applied.
func (rs *RequestState) AppliedC() chan RequestResult {
	return rs.CompletedC
}

// ResultC returns the apply channel, or with NotifyCommit a cap-2 channel delivering commit then apply.
func (rs *RequestState) ResultC() chan RequestResult {
	if rs.resolved != nil || !rs.notifyCommit {
		return rs.CompletedC
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.aggrC != nil {
		return rs.aggrC
	}
	rs.aggrC = make(chan RequestResult, 2)
	if rs.inner == nil || rs.released {
		return rs.aggrC
	}
	// Read inner.CommittedC directly: a relaying goroutine could drop the commit notification.
	committed := rs.inner.CommittedC()
	if committed == nil {
		return rs.aggrC
	}

	applied := rs.CompletedC
	out := rs.aggrC
	done := rs.doneC
	rs.wg.Add(1)
	go func() {
		defer rs.wg.Done()
		select {
		case qcn, ok := <-committed:
			if !ok {
				return
			}
			out <- RequestResult{inner: qcn, code: RequestCommitted}
			select {
			case cc, ok := <-applied:
				if ok {
					out <- cc
				}
			case <-done:
			}
		case cc, ok := <-applied:
			if !ok {
				return
			}
			// Commit happens before apply, so a committed value is already buffered; forward it to keep commit-then-apply order.
			select {
			case qcn := <-committed:
				out <- RequestResult{inner: qcn, code: RequestCommitted}
			default:
			}
			out <- cc
		case <-done:
		}
	}()
	return out
}

// Release cancels the context, waits for the bridges, then releases the pooled inner RequestState once; repeat calls are no-ops.
func (rs *RequestState) Release() {
	rs.mu.Lock()
	if rs.released {
		rs.mu.Unlock()
		return
	}
	rs.released = true
	rs.mu.Unlock()
	if rs.doneC != nil {
		rs.doneOnce.Do(func() { close(rs.doneC) })
	}
	if rs.cancelFunc != nil {
		rs.cancelFunc()
	}
	rs.wg.Wait()
	if rs.inner != nil {
		rs.inner.Release()
	}
}

// makeAdaptedChannel bridges the inner result channel to a translated adapter channel on a goroutine tracked by rs.wg.
func (rs *RequestState) makeAdaptedChannel() chan RequestResult {
	if rs.inner == nil {
		return nil
	}
	src := rs.inner.ResultC()
	if src == nil {
		return nil
	}
	out := make(chan RequestResult, 1)
	rs.wg.Add(1)
	go func() {
		defer rs.wg.Done()
		select {
		case qr, ok := <-src:
			if !ok {
				return
			}
			r := translateResult(qr)
			rs.latched.Store(&r)
			out <- r
		case <-rs.doneC:
			return
		}
	}()
	return out
}

// completedResult returns the request's delivered result, if any. It
// covers both the pre-resolved synchronous adaptation and the async
// bridge path, and never blocks.
func (rs *RequestState) completedResult() (RequestResult, bool) {
	if rs.resolved != nil {
		return *rs.resolved, true
	}
	if p := rs.latched.Load(); p != nil {
		return *p, true
	}
	return RequestResult{}, false
}

// newAsyncOpRequestState runs op on a goroutine and delivers its result; op must honour ctx, which Release cancels before waiting.
func newAsyncOpRequestState(cancel context.CancelFunc, op func() RequestResult) *RequestState {
	rs := &RequestState{cancelFunc: cancel, doneC: make(chan struct{})}
	out := make(chan RequestResult, 1)
	rs.CompletedC = out
	rs.wg.Add(1)
	go func() {
		defer rs.wg.Done()
		r := op()
		rs.latched.Store(&r)
		out <- r
	}()
	return rs
}

// SysOpState wraps quicraft's RequestState for system operations, signalling completion by closing a channel.
type SysOpState struct {
	inner *quicraft.RequestState

	// released elects the one Release call that reaches inner.
	released atomic.Bool

	// completedC is closed when the system operation finishes.
	completedC chan struct{}

	// doneC is closed by Release to signal the bridge goroutine to exit.
	// This prevents goroutine leaks when Release is called before the
	// inner result channel delivers a value.
	doneC chan struct{}

	// doneOnce ensures doneC is closed exactly once across multiple
	// Release calls.
	doneOnce sync.Once

	// wg tracks the bridge goroutine; Release waits on it before releasing the inner RequestState.
	wg sync.WaitGroup
}

// WrapSysOpState wraps a quicraft RequestState for system operation
// dragonboat API compatibility. It spawns a goroutine to wait for
// the inner result and close the completion channel.
func WrapSysOpState(inner *quicraft.RequestState) *SysOpState {
	s := &SysOpState{
		inner:      inner,
		completedC: make(chan struct{}),
		doneC:      make(chan struct{}),
	}
	s.wg.Add(1)
	go s.waitForCompletion()
	return s
}

// waitForCompletion closes completedC when the inner result arrives, or returns when Release closes doneC.
func (s *SysOpState) waitForCompletion() {
	defer s.wg.Done()
	src := s.inner.ResultC()
	if src == nil {
		close(s.completedC)
		return
	}
	select {
	case <-src:
		close(s.completedC)
	case <-s.doneC:
		return
	}
}

// CompletedC returns a receive-only channel that is closed when the
// system operation completes. Dragonboat v4 returns <-chan struct{}.
func (s *SysOpState) CompletedC() <-chan struct{} {
	return s.completedC
}

// ResultC returns a receive-only channel that is closed when the
// system operation completes. This is an alias for CompletedC.
func (s *SysOpState) ResultC() <-chan struct{} {
	return s.completedC
}

// Release signals and waits for the bridge, then releases the inner RequestState once.
func (s *SysOpState) Release() {
	if !s.released.CompareAndSwap(false, true) {
		return
	}
	if s.doneC != nil {
		s.doneOnce.Do(func() { close(s.doneC) })
	}
	s.wg.Wait()
	if s.inner != nil {
		s.inner.Release()
	}
}
