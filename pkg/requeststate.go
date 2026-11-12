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
// Stages at which tryRecycle calls testHookTryRecycle: on entry, and once
// it has decided the request may be recycled, just before it claims the
// return to the pool.
const (
	recycleStageEnter = iota
	recycleStageDecide
)

// testHookTryRecycle, when set by a test, is called by tryRecycle at each
// recycleStage. Tests use it to interleave a Release and a reuse of the
// object with a recycle in progress. Nil outside tests.
var testHookTryRecycle func(rs *RequestState, stage int)

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
	// end-to-end proposal latency. Zero when the request was created with
	// trackLatency false (no metrics collector configured), in which case
	// no latency is observed for it.
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

	// outstanding counts the references held by pending maps. Registering
	// the RequestState in a pending map is +1 (acquireRef); removing it is
	// -1 (releaseRef). It is bookkeeping only: the recycle decision is made
	// on holders.
	outstanding atomic.Int64

	// holders counts every reference that keeps the object out of the pool:
	// the caller's own, until its first Release, plus each pending-map
	// reference. newRequestState sets it to 1; acquireRef and tryAcquireRef
	// add 1; releaseRef and the first Release subtract 1. Exactly one
	// goroutine takes it to zero — once zero it is never raised again in
	// this use (tryAcquireRef refuses zero) — and that goroutine, holding the
	// last reference, recycles the object. Nothing else reads it to decide.
	// A decision made from separate loads of released and outstanding
	// instead let a releaser that had seen the pending count reach zero
	// before the caller released finish its check late, across the caller's
	// recycle and the next newRequestState's reset, and return the live
	// object to the pool a second time: a pending read then completed a
	// RequestState another request was using (#831). The object is only
	// returned to the pool when the caller has released it AND no pending-map
	// references remain, so a Sync* caller that times out and releases while
	// the request is still registered leaves it to the late
	// OnApplied/sweep, which recycles it then, never delivering into a
	// recycled (foreign) request.
	holders atomic.Int64

	// recycled records the return to the pool. The zero transition of
	// holders already elects one recycler; the CAS is a second guard.
	recycled atomic.Bool

	// completed prevents double-complete. The first complete() wins;
	// subsequent calls are no-ops. This guards against races where
	// multiple engine paths (e.g., apply + leadership change) try to
	// resolve the same request.
	completed atomic.Bool

	// committedDone prevents double-commitNotify. The first call wins;
	// subsequent calls are no-ops.
	committedDone atomic.Bool

	// session is the Session whose series this request holds in flight, nil
	// for every request that is not a session-managed proposal. sessionSeries
	// is that series. Whichever of complete-with-error or Release runs first
	// swaps the pointer out and releases the reservation; a successful
	// completion leaves it for Session.ProposalCompleted, which releases the
	// same series, so the later Release finds either nil or a series the
	// session has already moved past. Set by proposeEntry BEFORE the request
	// is registered — a completion can run the moment it is, and one that
	// found no binding would leave the reservation held — and reset by
	// newRequestState.
	session       atomic.Pointer[Session]
	sessionSeries atomic.Uint64

	// proposal is the pending entry under which the request is registered as
	// a proposal — a client proposal, config change or session operation —
	// and nil for any other request. A failure of the caller's own wait for
	// it is classified from that entry's hand-off marker by lostWait, by the
	// same rule the drains use (proposalMayReachRaft). Reset by
	// newRequestState.
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

// newRequestState obtains a RequestState from the pool and initializes it
// for a new request. trackLatency stamps createdAt for the end-to-end
// latency histograms; it must be false when no metrics collector is
// configured so the clock read is kept off the proposal hot path entirely.
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

// lostWait classifies err, a failure of the caller's own wait for this
// request (its deadline passed, or its context ended) rather than a result the
// request completed with. A proposal that may have reached raft — taken from
// its node's queue, or still queued while the queue is open — may still
// commit, so for one the failure is reported maybe-applied
// (proposalOutcomeUnknown); a proposal provably never taken from a closed
// queue, and any other request, gets err as given.
func (rs *RequestState) lostWait(err error) error {
	if pp := rs.proposal.Load(); pp != nil && proposalMayReachRaft(pp) {
		return proposalOutcomeUnknown(err)
	}
	return err
}

// awaitResult waits for rs to complete or ctx to end, whichever is first. A
// context that ends first is reported as ErrTimeout (deadline) or ErrCanceled,
// classified by lostWait. It is the one wait every synchronous proposal path
// uses.
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

// Release marks the RequestState as released by the caller. It is called
// EXACTLY ONCE, by the caller that holds the RequestState. The object is
// pooled, so a second call is a use-after-free whether it comes later or
// concurrently: once the first call has recycled the object and a new
// request has been built on it, nothing can tell a stale second call from
// that request's own, and a caller descheduled before its first instruction
// is exactly that stale call. A second call that finds released already set
// touches nothing else.
// After Release, Result returns ErrReleased and ResultC returns nil.
//
// The object is NOT returned to the pool until both the caller has
// released it AND every pending-map reference has been removed (see
// acquireRef/releaseRef). This prevents the use-after-Release hazard
// where a Sync* caller times out and releases while the request is
// still registered in a pending map; recycling early would hand the
// same object to a new request and deliver a stale/foreign result into
// it. The late OnApplied/sweep removes the final reference and recycles
// the object then.
func (rs *RequestState) Release() {
	// The released CAS elects the one Release that drops the caller's
	// reference; any later Release touches nothing else. The caller's
	// reference is still held across releaseSession, so the last
	// pending-map releaseRef cannot recycle the object out from under it.
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

// acquireRef records that a pending map now holds a reference to this
// RequestState. Must be paired with releaseRef when the entry is removed
// from the map. Called at registration time (Propose/ReadIndex/
// RequestSnapshot) before the object is exposed to completion callbacks.
func (rs *RequestState) acquireRef() {
	rs.outstanding.Add(1)
	rs.holders.Add(1)
}

// releaseRef records that a pending map no longer holds a reference to
// this RequestState. Called at every map-removal site (OnApplied,
// OnReadyToRead, OnReadIndexFailed, OnProposalFailed, OnSnapshotCompleted,
// failPendingRequests, the sweep, and the registration-error rollbacks).
// When the caller has already released and this was the last reference,
// the object is returned to the pool.
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

// tryAcquireRef acquires an additional reference only while at least one
// other reference is outstanding, returning false otherwise. Unlike
// acquireRef (which is only safe at registration time, before the object
// is published to a pending map), tryAcquireRef is safe on an object that
// may be racing recycling: it never increments from zero, and a nonzero
// holders count means a pending-map reference or the caller's own still
// pins the object, so it cannot
// reach tryRecycle's pool.Put — or newRequestState's counter reset — while
// the new reference is held. An unconditional Add(1) here could instead
// land on an object already recycled into the pool; if newRequestState
// then reset the counter to zero before the paired releaseRef ran, the
// counts would unbalance and the object could be recycled while still
// registered. A successful tryAcquireRef must be paired with releaseRef.
// Callers that need the reference to belong to a specific request must
// re-verify their pending-map entry after acquiring (see OnCommitted):
// the count may belong to a recycled successor, in which case the
// verify fails and the balanced acquire/release pair is harmless.
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

// tryRecycle returns the object to its pool. It is called only by
// dropHolder, from the goroutine that took holders to zero and so holds the
// last reference: no other goroutine can release, reset or reuse the object
// while it runs, and every load below sees this use's state. The released
// check only matters for an object not made by newRequestState (holders
// starts at zero), which is never recycled before its caller's Release.
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
	// A failed session proposal releases its series BEFORE the caller can
	// see the error, because the documented recovery is to retry with the
	// same session, and that retry must be admitted. A successful one keeps
	// the reservation until ProposalCompleted advances the series.
	if result.Err != nil {
		rs.releaseSession()
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
