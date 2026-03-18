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
	"sync"
	"sync/atomic"
	"time"
)

// timerPool holds recycled time.Timer instances to eliminate per-call
// allocations in the Result() hot path. Each call to Result() with a
// deadline borrows a timer, resets it, and returns it after use.
//
// Correct usage per the sync.Pool contract: always call Stop() and drain
// the channel before Reset() when reusing a timer from the pool.
var timerPool = sync.Pool{
	New: func() interface{} {
		// Create with a far-future expiry; the caller resets before use.
		t := time.NewTimer(1<<63 - 1)
		t.Stop()
		return t
	},
}

// acquireTimer gets a timer from the pool and resets it to fire after d.
// The returned timer must be returned via releaseTimer after use.
func acquireTimer(d time.Duration) *time.Timer {
	t := timerPool.Get().(*time.Timer)
	// Drain any stale tick that may have fired before the previous Stop.
	select {
	case <-t.C:
	default:
	}
	t.Reset(d)
	return t
}

// releaseTimer stops the timer and returns it to the pool. The channel
// is drained only when Stop returns false (meaning the timer fired before
// we stopped it) to keep the channel empty for the next acquireTimer.
func releaseTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

// RequestResult holds the outcome of an asynchronous operation submitted
// through Propose, ReadIndex, or RequestSnapshot. Value and Data carry the
// application-defined result from the state machine. Err is non-nil when
// the request was rejected, timed out, or canceled.
type RequestResult struct {
	// Value is the application-defined result code from sm.Result.
	Value uint64

	// Data is the optional application-defined result data from sm.Result.
	Data []byte

	// Err is non-nil when the request failed (timeout, canceled, rejected, etc.).
	Err error
}

// RequestState tracks a single asynchronous request through the Raft pipeline.
// Callers obtain a RequestState from Propose, ReadIndex, or RequestSnapshot
// and must either call Result (blocking) or select on ResultC (non-blocking).
// After consuming the result, call Release to return the object to the pool.
//
// When NotifyCommit is enabled on the host, callers can select on CommittedC()
// for early WAL-durable notification (~1-2ms) or ApplyResultC() for the full
// state machine apply result (~10-30ms).
//
// RequestState is safe for use by a single goroutine. Do not share across
// goroutines without external synchronization.
type RequestState struct {
	// key is the unique identifier for this request within the pending map.
	key uint64

	// deadlineNano stores the deadline as unix nanoseconds for atomic access.
	// This prevents data races when the sweeper reads deadline while another
	// goroutine is initializing a recycled RequestState from the pool.
	deadlineNano atomic.Int64

	// createdAt records when the request was submitted, for measuring
	// end-to-end proposal latency. Zero when metrics are disabled.
	createdAt time.Time

	// completedC delivers the full apply result exactly once.
	completedC chan RequestResult

	// committedC delivers the early WAL-durable notification exactly once.
	// Only used when NotifyCommit is enabled on the host. The result
	// carries Value=0 and nil Data since the state machine has not yet
	// applied the entry.
	committedC chan RequestResult

	// pool is the sync.Pool to return this object to on Release.
	pool *sync.Pool

	// released prevents double-Release and use-after-Release.
	released atomic.Bool

	// completed prevents double-complete. The first complete() wins;
	// subsequent calls are no-ops. This guards against races where
	// multiple engine paths (e.g., apply + leadership change) try to
	// resolve the same request.
	completed atomic.Bool

	// committedDone prevents double-commitNotify. The first call wins;
	// subsequent calls are no-ops.
	committedDone atomic.Bool
}

// NewRequestStatePool creates a sync.Pool that produces RequestState objects
// with pre-allocated completedC and committedC channels.
func NewRequestStatePool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return &RequestState{
				completedC: make(chan RequestResult, 1),
				committedC: make(chan RequestResult, 1),
			}
		},
	}
}

// newRequestState obtains a RequestState from the pool and initializes it
// for a new request.
func newRequestState(pool *sync.Pool, key uint64, deadline time.Time) *RequestState {
	rs := pool.Get().(*RequestState)
	rs.key = key
	if deadline.IsZero() {
		rs.deadlineNano.Store(0)
	} else {
		rs.deadlineNano.Store(deadline.UnixNano())
	}
	rs.createdAt = time.Now()
	rs.pool = pool
	rs.released.Store(false)
	rs.completed.Store(false)
	rs.committedDone.Store(false)
	// Drain any stale result from a previous use.
	select {
	case <-rs.completedC:
	default:
	}
	select {
	case <-rs.committedC:
	default:
	}
	return rs
}

// Result blocks until the request completes or the deadline is reached.
// Returns ErrReleased if the RequestState has already been released.
// Returns ErrTimeout if the deadline expires before completion.
//
// For high-throughput paths, prefer ResultC() with a context-based select
// to multiplex the deadline externally. When a deadline is present, Result
// borrows a timer from timerPool (zero heap allocation on the steady path).
func (rs *RequestState) Result() (RequestResult, error) {
	if rs.released.Load() {
		return RequestResult{}, ErrReleased
	}

	deadline := rs.Deadline()
	if deadline.IsZero() {
		// No deadline: block indefinitely.
		result := <-rs.completedC
		return result, result.Err
	}

	timeout := time.Until(deadline)
	if timeout <= 0 {
		// Already expired.
		return RequestResult{}, ErrTimeout
	}

	timer := acquireTimer(timeout)
	defer releaseTimer(timer)

	select {
	case result := <-rs.completedC:
		return result, result.Err
	case <-timer.C:
		return RequestResult{}, ErrTimeout
	}
}

// ResultC returns a read-only channel that receives the result when the
// request completes. Returns nil if the RequestState has been released.
// The channel receives exactly one value.
func (rs *RequestState) ResultC() <-chan RequestResult {
	if rs.released.Load() {
		return nil
	}
	return rs.completedC
}

// CommittedC returns a read-only channel that receives an early notification
// after the entry has been persisted to the local WAL (~1-2ms). The result
// carries Value=0 and nil Data because the state machine has not yet applied
// the entry. Returns nil if the RequestState has been released.
//
// CommittedC is only signaled when NotifyCommit is enabled on the host.
// When NotifyCommit is disabled, this channel never receives a value.
func (rs *RequestState) CommittedC() <-chan RequestResult {
	if rs.released.Load() {
		return nil
	}
	return rs.committedC
}

// ApplyResultC returns a read-only channel that receives the full state
// machine apply result. This is identical to ResultC() and always returns
// the completedC channel. Provided for clarity when both CommittedC() and
// the apply result are needed in the same select.
func (rs *RequestState) ApplyResultC() <-chan RequestResult {
	if rs.released.Load() {
		return nil
	}
	return rs.completedC
}

// Release returns the RequestState to its pool. Release is idempotent;
// subsequent calls are safe no-ops. After Release, Result returns
// ErrReleased and ResultC returns nil.
func (rs *RequestState) Release() {
	if !rs.released.CompareAndSwap(false, true) {
		return
	}
	// Drain any unconsumed result to reset channel state.
	select {
	case <-rs.completedC:
	default:
	}
	select {
	case <-rs.committedC:
	default:
	}
	if rs.pool != nil {
		rs.pool.Put(rs)
	}
}

// complete sends the result to the completedC channel. This is called
// internally by the engine when the request resolves. Only the first
// call delivers the result; subsequent calls are no-ops. This prevents
// races where multiple engine paths (e.g., apply + leadership change)
// try to resolve the same request concurrently.
func (rs *RequestState) complete(result RequestResult) {
	if rs.released.Load() {
		return
	}
	if !rs.completed.CompareAndSwap(false, true) {
		return // already completed
	}
	select {
	case rs.completedC <- result:
	default:
	}
}

// commitNotify sends an early WAL-durable notification to the committedC
// channel. This is called by the commit worker callback after entries are
// persisted to LogDB but before state machine apply. Only the first call
// delivers the notification; subsequent calls are no-ops.
func (rs *RequestState) commitNotify(result RequestResult) {
	if rs.released.Load() {
		return
	}
	if !rs.committedDone.CompareAndSwap(false, true) {
		return // already notified
	}
	select {
	case rs.committedC <- result:
	default:
	}
}

// Key returns the unique request key. Used by the request manager.
func (rs *RequestState) Key() uint64 {
	return rs.key
}

// Deadline returns the request deadline. Used by the timeout sweeper.
// This method is safe for concurrent access via atomic load.
func (rs *RequestState) Deadline() time.Time {
	ns := rs.deadlineNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
