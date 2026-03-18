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

	// snapshotIndex holds the snapshot index for snapshot-related results.
	snapshotIndex uint64

	// entries holds raft log entries for QueryRaftLog results. Since
	// quicraft's QueryRaftLog is synchronous and returns entries directly,
	// the adapter carries them here for the RaftLogs() method.
	entries []Entry
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
// exceeds the available log entries.
func (r RequestResult) RequestOutOfRange() bool {
	return errors.Is(r.inner.Err, quicraft.ErrInvalidRange)
}

// SnapshotIndex returns the log index of the snapshot when the result
// is from a snapshot request. Returns 0 for non-snapshot results.
func (r RequestResult) SnapshotIndex() uint64 {
	return r.snapshotIndex
}

// RaftLogs returns the raft log entries and the log range from a
// QueryRaftLog result. Returns nil entries and a zero LogRange when
// no entries are available.
func (r RequestResult) RaftLogs() ([]Entry, LogRange) {
	if len(r.entries) == 0 {
		return nil, LogRange{}
	}
	lr := LogRange{
		FirstIndex: r.entries[0].Index,
		LastIndex:  r.entries[len(r.entries)-1].Index + 1,
	}
	return r.entries, lr
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

	// committedC delivers the early WAL-durable notification when
	// NotifyCommit is enabled on the host. Only signaled after the
	// entry is durably committed to the WAL but before the state
	// machine applies it.
	committedC chan RequestResult

	// shardID records the shard this request belongs to. This is used
	// by ReadLocalNode to query the correct shard after ReadIndex
	// resolves. Set by WrapRequestStateForShard.
	shardID uint64

	// resolved holds the pre-resolved result for synchronous operations
	// adapted to the async RequestState API. When non-nil, AppliedC/ResultC
	// return a fresh buffered channel containing the result on each call,
	// allowing multiple independent reads.
	resolved *RequestResult

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
}

// WrapRequestState wraps a quicraft RequestState for dragonboat API
// compatibility.
func WrapRequestState(inner *quicraft.RequestState) *RequestState {
	rs := &RequestState{inner: inner, doneC: make(chan struct{})}
	rs.CompletedC = rs.makeAdaptedChannel()
	rs.committedC = rs.makeAdaptedCommittedChannel()
	return rs
}

// WrapRequestStateForShard wraps a quicraft RequestState with an
// explicit shardID. Used by ReadIndex so that ReadLocalNode can
// determine which shard to query.
func WrapRequestStateForShard(inner *quicraft.RequestState, shardID uint64) *RequestState {
	rs := &RequestState{inner: inner, shardID: shardID, doneC: make(chan struct{})}
	rs.CompletedC = rs.makeAdaptedChannel()
	rs.committedC = rs.makeAdaptedCommittedChannel()
	return rs
}

// WrapRequestStateWithCancel wraps a quicraft RequestState with an
// associated context cancel function. The cancel function is invoked
// when Release() is called, ensuring the context is not canceled
// prematurely while the async operation is still in flight.
func WrapRequestStateWithCancel(inner *quicraft.RequestState, cancel context.CancelFunc) *RequestState {
	rs := &RequestState{inner: inner, cancelFunc: cancel, doneC: make(chan struct{})}
	rs.CompletedC = rs.makeAdaptedChannel()
	rs.committedC = rs.makeAdaptedCommittedChannel()
	return rs
}

// WrapRequestStateForShardWithCancel wraps a quicraft RequestState with
// an explicit shardID and an associated context cancel function. Used by
// ReadIndex so that ReadLocalNode can determine which shard to query,
// while ensuring the context outlives the async operation.
func WrapRequestStateForShardWithCancel(inner *quicraft.RequestState, shardID uint64, cancel context.CancelFunc) *RequestState {
	rs := &RequestState{inner: inner, shardID: shardID, cancelFunc: cancel, doneC: make(chan struct{})}
	rs.CompletedC = rs.makeAdaptedChannel()
	rs.committedC = rs.makeAdaptedCommittedChannel()
	return rs
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
	// Store the result so that AppliedC/ResultC/CompletedC can each
	// return a fresh buffered channel, allowing multiple independent reads.
	rs.resolved = &result
	rs.CompletedC = rs.makeResolvedChannel()
	return rs
}

// makeResolvedChannel creates a new single-buffered channel pre-loaded
// with the resolved result. Each call returns a distinct channel so that
// multiple readers (CompletedC, AppliedC, ResultC) can each receive the
// value independently.
func (rs *RequestState) makeResolvedChannel() chan RequestResult {
	ch := make(chan RequestResult, 1)
	ch <- *rs.resolved
	return ch
}

// AppliedC returns a bidirectional channel that receives when the request
// is applied by the state machine. The returned channel receives exactly
// one RequestResult value. Dragonboat returns chan (bidirectional), not
// <-chan (receive-only).
func (rs *RequestState) AppliedC() chan RequestResult {
	if rs.resolved != nil {
		return rs.makeResolvedChannel()
	}
	return rs.CompletedC
}

// ResultC returns a bidirectional channel that receives the result.
// This is an alias for AppliedC in quicraft's model where ResultC
// delivers the apply result. Dragonboat returns chan (bidirectional),
// not <-chan (receive-only).
func (rs *RequestState) ResultC() chan RequestResult {
	if rs.resolved != nil {
		return rs.makeResolvedChannel()
	}
	return rs.CompletedC
}

// CommittedC returns a channel that receives an early WAL-durable
// notification when NotifyCommit is enabled on the host. The result
// carries RequestCommitted as its code. When NotifyCommit is disabled,
// this returns nil. Dragonboat returns chan (bidirectional).
func (rs *RequestState) CommittedC() chan RequestResult {
	return rs.committedC
}

// Release returns the RequestState to the pool and cancels any associated
// context. After Release, the channels returned by AppliedC and ResultC
// should not be used.
func (rs *RequestState) Release() {
	if rs.doneC != nil {
		rs.doneOnce.Do(func() { close(rs.doneC) })
	}
	if rs.cancelFunc != nil {
		rs.cancelFunc()
	}
	if rs.inner != nil {
		rs.inner.Release()
	}
}

// makeAdaptedChannel creates and returns a buffered channel that receives
// the translated result from the inner quicraft RequestState. A goroutine
// is spawned to bridge the inner channel to the adapter channel.
func (rs *RequestState) makeAdaptedChannel() chan RequestResult {
	if rs.inner == nil {
		return nil
	}
	src := rs.inner.ResultC()
	if src == nil {
		return nil
	}
	out := make(chan RequestResult, 1)
	go func() {
		select {
		case qr, ok := <-src:
			if !ok {
				return
			}
			out <- translateResult(qr)
		case <-rs.doneC:
			return
		}
	}()
	return out
}

// makeAdaptedCommittedChannel creates a buffered channel that bridges
// quicraft's CommittedC to the adapter layer. Returns nil when the
// inner RequestState has no committed channel (NotifyCommit disabled).
func (rs *RequestState) makeAdaptedCommittedChannel() chan RequestResult {
	if rs.inner == nil {
		return nil
	}
	src := rs.inner.CommittedC()
	if src == nil {
		return nil
	}
	out := make(chan RequestResult, 1)
	go func() {
		select {
		case qr, ok := <-src:
			if !ok {
				return
			}
			out <- RequestResult{
				inner: qr,
				code:  RequestCommitted,
			}
		case <-rs.doneC:
			return
		}
	}()
	return out
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
	go s.waitForCompletion()
	return s
}

// waitForCompletion reads from the inner quicraft result channel and
// closes the completedC channel to signal that the operation is done.
// The goroutine also monitors doneC so it can exit promptly when
// Release is called before the inner result arrives.
func (s *SysOpState) waitForCompletion() {
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
// and ResultC should not be used.
func (s *SysOpState) Release() {
	if s.doneC != nil {
		s.doneOnce.Do(func() { close(s.doneC) })
	}
	if s.inner != nil {
		s.inner.Release()
	}
}
