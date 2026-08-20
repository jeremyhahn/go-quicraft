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

	// logRange is the range reported by RaftLogs. For a completed query it
	// covers the returned entries; for a RequestOutOfRange result it is the
	// range currently available in the log, which is what dragonboat
	// delivers so that paginating callers can re-anchor.
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

// Committed returns true if the request was committed to the WAL.
// Per dragonboat semantics, this includes both committed and fully
// completed (applied) results, since a completed request was also
// committed.
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

// SnapshotIndex returns the log index of the snapshot when the result
// is from a snapshot request. Quicraft delivers the snapshot index in
// the result's Value field (see Host.SyncRequestSnapshot), matching
// dragonboat, which reads the request result's value. The returned
// value is only meaningful for snapshot-related results; dragonboat
// panics on misuse, while this adapter returns the raw result value.
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
	// Session failures. Dragonboat documents Rejected() as "the client
	// session is not registered or has been evicted on the server side",
	// so every session-identity failure maps to RequestRejected and never
	// to the generic RequestAborted.
	{quicraft.ErrSessionExpired, RequestRejected},
	{quicraft.ErrSessionNotFound, RequestRejected},
	{quicraft.ErrSessionAlreadyExists, RequestRejected},
	// Leader-unavailable / shard-not-ready failures. Dragonboat documents
	// Dropped() as "the leader is unavailable or not ready yet ... can
	// usually be retried", which is exactly what these report; mapping them
	// to RequestAborted would turn every election and rolling restart into
	// a hard failure for callers that only retry on Dropped().
	{quicraft.ErrNotLeader, RequestDropped},
	{quicraft.ErrReadIndexNotReady, RequestDropped},
	{quicraft.ErrShardNotReady, RequestDropped},
	{quicraft.ErrDiskFull, RequestDropped},
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

	// aggrC is the lazily created cap-2 aggregate channel returned by
	// ResultC when notifyCommit is set. It delivers the commit
	// notification followed by the apply result, matching dragonboat's
	// RequestState.ResultC. Its goroutine reads rs.inner.CommittedC()
	// directly (the true source, written to synchronously by quicraft's
	// own commit path) rather than through a separate translating bridge:
	// an earlier version raced a bridge's OUTPUT here instead of the
	// source, and the extra asynchronous hop between the two — the bridge
	// goroutine not yet having been scheduled to relay a value that was
	// already sitting in the source channel — is what dropped the commit
	// notification (#13).
	aggrC chan RequestResult

	// shardID records the shard this request belongs to. This is used
	// by ReadLocalNode to query the correct shard after ReadIndex
	// resolves. Set by WrapRequestStateForShard.
	shardID uint64

	// resolved holds the pre-resolved result for synchronous operations
	// adapted to the async RequestState API. When non-nil, AppliedC and
	// ResultC return resolvedC, the single pre-loaded channel built at
	// construction time. Returning a fresh channel per call would make the
	// idiomatic dragonboat receive loop spin forever.
	resolved *RequestResult

	// resolvedC is the single channel handed out for a pre-resolved
	// RequestState. It holds exactly one buffered result, matching the
	// one-value contract of a real request's channel.
	resolvedC chan RequestResult

	// cancelFunc holds the context cancel function from async methods
	// that create a context.WithTimeout. It is called during Release()
	// to ensure the context is cleaned up after the async operation
	// completes or the caller is done with the RequestState.
	cancelFunc context.CancelFunc

	// doneC is closed by Release to signal bridge goroutines to exit.
	// This prevents goroutine leaks when Release is called before the
	// inner result channel delivers a value.
	doneC chan struct{}

	// doneOnce ensures doneC is closed exactly once across multiple
	// Release calls.
	doneOnce sync.Once

	// wg tracks every goroutine spawned on behalf of this RequestState
	// (the channel bridges and the async-op runner). Release waits on it
	// after signaling doneC and BEFORE releasing the inner quicraft
	// RequestState: inner.Release may recycle the pooled object, and a
	// bridge goroutine still selecting on the recycled result channel
	// would otherwise race the pool and could steal a successor
	// request's result.
	wg sync.WaitGroup

	// latched records the delivered result, stored by the bridge
	// goroutine immediately before it sends on the adapter channel.
	// ReadLocalNode consults this instead of receiving from ResultC:
	// per the dragonboat contract the caller has already drained the
	// channel, so a second receive would block forever.
	latched atomic.Pointer[RequestResult]
}

// wrapRequestState is the single internal constructor behind the exported
// Wrap* helpers. The notifyCommit flag threads the host's NotifyCommit
// setting onto the wrapper: when false, ResultC() returns the apply-only
// channel, matching dragonboat.
//
// Only the apply bridge is spawned here. It cannot be deferred because
// the deprecated CompletedC field is read directly by callers; the
// ResultC aggregate is created on first use.
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

// WrapRequestState wraps a quicraft RequestState for dragonboat API
// compatibility. notifyCommit must carry the owning host's NotifyCommit
// setting: quicraft always allocates an inner committed channel, so the
// wrapper cannot infer it, and a wrong value changes the ResultC contract.
func WrapRequestState(inner *quicraft.RequestState, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, 0, nil, notifyCommit)
}

// WrapRequestStateForShard wraps a quicraft RequestState with an
// explicit shardID. Used by ReadIndex so that ReadLocalNode can
// determine which shard to query.
func WrapRequestStateForShard(inner *quicraft.RequestState, shardID uint64, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, shardID, nil, notifyCommit)
}

// WrapRequestStateWithCancel wraps a quicraft RequestState with an
// associated context cancel function. The cancel function is invoked
// when Release() is called, ensuring the context is not canceled
// prematurely while the async operation is still in flight.
func WrapRequestStateWithCancel(inner *quicraft.RequestState, cancel context.CancelFunc, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, 0, cancel, notifyCommit)
}

// WrapRequestStateForShardWithCancel wraps a quicraft RequestState with
// an explicit shardID and an associated context cancel function. Used by
// ReadIndex so that ReadLocalNode can determine which shard to query,
// while ensuring the context outlives the async operation.
func WrapRequestStateForShardWithCancel(inner *quicraft.RequestState, shardID uint64, cancel context.CancelFunc, notifyCommit bool) *RequestState {
	return wrapRequestState(inner, shardID, cancel, notifyCommit)
}

// ShardID returns the shard this request belongs to.
func (rs *RequestState) ShardID() uint64 {
	return rs.shardID
}

// newResolvedRequestState creates a pre-resolved RequestState that
// immediately delivers the given result on its channels. This is used
// for adapting synchronous quicraft APIs (like QueryRaftLog) to
// dragonboat's asynchronous RequestState return convention.
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

// AppliedC returns a bidirectional channel that receives when the request
// is applied by the state machine. The returned channel receives exactly
// one RequestResult value. Dragonboat returns chan (bidirectional), not
// <-chan (receive-only).
func (rs *RequestState) AppliedC() chan RequestResult {
	return rs.CompletedC
}

// ResultC returns a bidirectional channel that receives the result.
//
// When NotifyCommit is disabled on the host the returned channel is the
// apply-only channel, identical to AppliedC. When NotifyCommit is enabled
// the returned channel is a cap-2 aggregate delivering up to two values in
// dragonboat's order: the commit notification first, then the apply
// result. Dragonboat returns chan (bidirectional), not <-chan.
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
	// Read rs.inner.CommittedC() directly -- the true source, written to
	// synchronously by quicraft's own commit path -- rather than through a
	// separate translating bridge. See the aggrC field doc for why: an
	// earlier version raced a bridge's translated OUTPUT here instead of
	// the source, and the extra asynchronous hop between the two (the
	// bridge goroutine not yet scheduled to relay a value already sitting
	// in the source channel) is what dropped the commit notification
	// (#13). This mirrors dragonboat's own RequestState.ResultC
	// (request.go), which races r.committedC/r.CompletedC directly for
	// the same reason -- they are the fields the raft engine itself
	// writes into, with nothing relaying between the writer and this
	// select.
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
			// The apply result won the race, or the request failed before
			// it was ever committed. rs.inner's own commitNotify always
			// happens-before its complete for the same entry (raft
			// commits before it applies), and both write directly into
			// rs.inner's own fields -- so if this request actually
			// committed, the value is already sitting in `committed`'s
			// buffer by the time `applied` fires, needing no wait. Forward
			// it if present so the caller still observes dragonboat's
			// committed-then-applied order.
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

// Release returns the RequestState to the pool and cancels any associated
// context. After Release, the channels returned by AppliedC and ResultC
// should not be used.
//
// Release waits for every bridge goroutine to exit BEFORE releasing the
// inner quicraft RequestState. inner.Release may recycle the pooled inner
// object immediately; a bridge goroutine still selecting on the recycled
// result channel would race the pool and could steal the result of a new
// request that was handed the recycled object.
func (rs *RequestState) Release() {
	rs.mu.Lock()
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

// makeAdaptedChannel creates and returns a buffered channel that receives
// the translated result from the inner quicraft RequestState. A goroutine
// is spawned to bridge the inner channel to the adapter channel; it is
// registered on rs.wg so Release can wait for it to exit before the inner
// RequestState is recycled.
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

// newAsyncOpRequestState runs op in its own goroutine and delivers its
// result through the RequestState channels. This adapts quicraft
// operations that have no asynchronous variant (such as session
// registration via Host.RegisterSession) to dragonboat's non-blocking
// RequestState return convention. The op must honor the context whose
// cancel function is passed here: Release cancels the context and then
// waits for op to return.
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

// SysOpState wraps quicraft's RequestState for system operations such as
// membership changes and snapshot requests. In dragonboat v4, SysOpState
// signals completion via a closed struct{} channel, not a RequestResult
// channel.
type SysOpState struct {
	inner *quicraft.RequestState

	// completedC is closed when the system operation finishes.
	completedC chan struct{}

	// doneC is closed by Release to signal the bridge goroutine to exit.
	// This prevents goroutine leaks when Release is called before the
	// inner result channel delivers a value.
	doneC chan struct{}

	// doneOnce ensures doneC is closed exactly once across multiple
	// Release calls.
	doneOnce sync.Once

	// wg tracks the bridge goroutine. Release waits on it after
	// signaling doneC and before releasing the inner RequestState, so
	// the bridge can never race the pool recycling the inner object
	// and steal a successor request's result.
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

// waitForCompletion reads from the inner quicraft result channel and
// closes the completedC channel to signal that the operation is done.
// The goroutine also monitors doneC so it can exit promptly when
// Release is called before the inner result arrives.
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

// Release returns the RequestState to the pool and signals the bridge
// goroutine to exit. After Release, the channels returned by CompletedC
// and ResultC should not be used. The bridge goroutine is waited for
// before the inner RequestState is released, so it can never observe the
// recycled inner object (see RequestState.Release for the hazard).
func (s *SysOpState) Release() {
	if s.doneC != nil {
		s.doneOnce.Do(func() { close(s.doneC) })
	}
	s.wg.Wait()
	if s.inner != nil {
		s.inner.Release()
	}
}
