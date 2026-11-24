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
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// timerPool recycles time.Timer instances for Result.
var timerPool = sync.Pool{
	New: func() interface{} {
		// Create with a far-future expiry; the caller resets before use.
		t := time.NewTimer(1<<63 - 1)
		t.Stop()
		return t
	},
}

// Stages at which tryRecycle calls testHookTryRecycle: on entry, and just before it returns the request to the pool.
const (
	recycleStageEnter = iota
	recycleStageDecide
)

// testHookTryRecycle, when set by a test, is called by tryRecycle at each
// recycleStage. Tests use it to interleave a Release and a reuse of the
// object with a recycle in progress. Nil outside tests.
var testHookTryRecycle func(rs *RequestState, stage int)

// acquireTimer takes a timer from timerPool set to fire after d; return it with releaseTimer.
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

// RequestResult holds the outcome of a Propose, ReadIndex or RequestSnapshot; Err is non-nil on failure.
type RequestResult struct {
	// Value is the application-defined result code from sm.Result.
	Value uint64

	// Data is the optional application-defined result data from sm.Result.
	Data []byte

	// Err is non-nil when the request failed (timeout, canceled, rejected, etc.).
	Err error
}

// RequestState tracks one asynchronous request; call Result or select on ResultC, then Release exactly once.
type RequestState struct {
	// key is the unique identifier for this request within the pending map.
	key uint64

	// deadlineNano stores the deadline as unix nanoseconds for atomic access.
	// This prevents data races when the sweeper reads deadline while another
	// goroutine is initializing a recycled RequestState from the pool.
	deadlineNano atomic.Int64

	// createdAt is the submission time for latency metrics; zero when trackLatency is false.
	createdAt time.Time

	// completedC delivers the full apply result exactly once.
	completedC chan RequestResult

	// committedC delivers the WAL-durable notification once, when NotifyCommit is enabled; Value is 0 and Data nil.
	committedC chan RequestResult

	// pool is the sync.Pool to return this object to on Release.
	pool *sync.Pool

	// released prevents double-Release and use-after-Release.
	released atomic.Bool

	// outstanding counts pending-map references; bookkeeping only, holders decides recycling.
	outstanding atomic.Int64

	// holders counts every reference keeping the object out of the pool: the caller's until Release, plus each pending-map reference.
	holders atomic.Int64

	// recycled records the return to the pool. The zero transition of
	// holders already elects one recycler; the CAS is a second guard.
	recycled atomic.Bool

	// completed makes the first complete win; later calls are no-ops.
	completed atomic.Bool

	// committedDone prevents double-commitNotify. The first call wins;
	// subsequent calls are no-ops.
	committedDone atomic.Bool

	// session and sessionSeries name the session series this request holds in flight; nil for non-session requests.
	session       atomic.Pointer[Session]
	sessionSeries atomic.Uint64

	// proposal is the pending entry the request is registered under as a proposal, nil otherwise; lostWait uses it to classify a failed wait.
	proposal atomic.Pointer[pendingProposal]
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

// newRequestState takes a RequestState from the pool for a new request; trackLatency stamps createdAt.
func newRequestState(pool *sync.Pool, key uint64, deadline time.Time, trackLatency bool) *RequestState {
	rs := pool.Get().(*RequestState)
	rs.key = key
	if deadline.IsZero() {
		rs.deadlineNano.Store(0)
	} else {
		rs.deadlineNano.Store(deadline.UnixNano())
	}
	if trackLatency {
		rs.createdAt = time.Now()
	} else {
		rs.createdAt = time.Time{}
	}
	rs.pool = pool
	rs.released.Store(false)
	rs.recycled.Store(false)
	rs.outstanding.Store(0)
	// The caller's reference. Set after outstanding is reset: until it is
	// nonzero no tryAcquireRef can succeed on this object, so no stale
	// increment is lost to the reset.
	rs.holders.Store(1)
	rs.completed.Store(false)
	rs.committedDone.Store(false)
	rs.session.Store(nil)
	rs.sessionSeries.Store(0)
	rs.proposal.Store(nil)
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

// Result blocks until the request completes or the deadline passes, returning ErrReleased after Release or ErrTimeout on expiry.
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
		return RequestResult{}, rs.lostWait(ErrTimeout)
	}

	timer := acquireTimer(timeout)
	defer releaseTimer(timer)

	select {
	case result := <-rs.completedC:
		return result, result.Err
	case <-timer.C:
		return RequestResult{}, rs.lostWait(ErrTimeout)
	}
}

// lostWait classifies err, a failure of the caller's own wait, as maybe-applied when the proposal may have reached raft.
func (rs *RequestState) lostWait(err error) error {
	if pp := rs.proposal.Load(); pp != nil && proposalMayReachRaft(pp) {
		return proposalOutcomeUnknown(err)
	}
	return err
}

// awaitResult waits for rs or ctx, reporting a context end as ErrTimeout or ErrCanceled classified by lostWait.
func awaitResult(ctx context.Context, rs *RequestState) (RequestResult, error) {
	select {
	case result := <-rs.ResultC():
		return result, result.Err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return RequestResult{}, rs.lostWait(ErrTimeout)
		}
		return RequestResult{}, rs.lostWait(ErrCanceled)
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

// CommittedC returns a channel notified once the entry is in the local WAL, before apply; Value is 0 and Data nil.
func (rs *RequestState) CommittedC() <-chan RequestResult {
	if rs.released.Load() {
		return nil
	}
	return rs.committedC
}

// ApplyResultC returns the apply result channel, the same as ResultC.
func (rs *RequestState) ApplyResultC() <-chan RequestResult {
	if rs.released.Load() {
		return nil
	}
	return rs.completedC
}

// Release drops the caller's reference and must be called exactly once; afterwards Result returns ErrReleased and ResultC nil, and the object is pooled once no pending map holds it.
func (rs *RequestState) Release() {
	// The released CAS elects the one Release that drops the caller's reference.
	if !rs.released.CompareAndSwap(false, true) {
		return
	}
	rs.releaseSession()
	rs.dropHolder()
}

// bindSession records that this request holds series in flight on session.
// Called once, by proposeEntry, before registration.
func (rs *RequestState) bindSession(session *Session, series uint64) {
	rs.sessionSeries.Store(series)
	rs.session.Store(session)
}

// releaseSession releases the session reservation this request holds, if it
// still holds one. Safe to call from both the completion and the caller
// goroutine: the pointer swap elects exactly one releaser.
func (rs *RequestState) releaseSession() {
	if s := rs.session.Swap(nil); s != nil {
		s.endProposal(rs.sessionSeries.Load())
	}
}

// acquireRef records a pending-map reference; pair it with releaseRef when the entry is removed.
func (rs *RequestState) acquireRef() {
	rs.outstanding.Add(1)
	rs.holders.Add(1)
}

// releaseRef records that a pending map no longer holds this RequestState.
func (rs *RequestState) releaseRef() {
	rs.outstanding.Add(-1)
	rs.dropHolder()
}

// dropHolder drops one reference from holders and, if it was the last,
// recycles the object. Only the goroutine that takes holders to zero
// recycles, and it makes that decision from its own decrement alone.
func (rs *RequestState) dropHolder() {
	if rs.holders.Add(-1) == 0 {
		rs.tryRecycle()
	}
}

// tryAcquireRef takes an extra reference only while another is outstanding, returning false otherwise.
func (rs *RequestState) tryAcquireRef() bool {
	for {
		n := rs.holders.Load()
		if n <= 0 {
			return false
		}
		if rs.holders.CompareAndSwap(n, n+1) {
			rs.outstanding.Add(1)
			return true
		}
	}
}

// tryRecycle returns the object to its pool; only dropHolder calls it, holding the last reference.
func (rs *RequestState) tryRecycle() {
	if h := testHookTryRecycle; h != nil {
		h(rs, recycleStageEnter)
	}
	if !rs.released.Load() {
		return
	}
	if h := testHookTryRecycle; h != nil {
		h(rs, recycleStageDecide)
	}
	if !rs.recycled.CompareAndSwap(false, true) {
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

// complete delivers the result to completedC; only the first call does.
func (rs *RequestState) complete(result RequestResult) {
	if rs.released.Load() {
		return
	}
	if !rs.completed.CompareAndSwap(false, true) {
		return // already completed
	}
	// A failed session proposal releases its series before the caller sees the error, so a retry is admitted.
	if result.Err != nil {
		rs.releaseSession()
	}
	select {
	case rs.completedC <- result:
	default:
	}
}

// commitNotify sends the WAL-durable notification to committedC; only the first call does.
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
