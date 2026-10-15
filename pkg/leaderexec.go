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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// LeaderHandler executes a leader-executed request (see Host.ExecuteOnLeader)
// on the shard's leader and returns its reply. It is never appended to the
// Raft log, so it must not change replicated state; it serves state the
// leader holds in memory for the term it leads, such as lease deadlines.
//
// term is the leadership term the request is executed under: the handler
// runs only while this replica leads the shard in that term and the leader
// has committed an entry of it, so every entry committed in an earlier term
// is committed here too (its apply may still be in flight). ctx ends with the
// caller's deadline, or when the host closes; a handler must honour it.
//
// An error is returned to the caller as a *LeaderHandlerError carrying its
// text. Handlers for different requests run concurrently.
type LeaderHandler func(ctx context.Context, term uint64, request []byte) ([]byte, error)

const (
	// leaderExecMaxInflight bounds the requests a host executes for remote
	// callers at once. A request beyond it is refused with ErrBusy rather than
	// queued, so a burst cannot pin an unbounded number of goroutines on a
	// leader.
	leaderExecMaxInflight = 1024

	// leaderExecMaxFailureText bounds the handler error text sent back to a
	// remote caller.
	leaderExecMaxFailureText = 1024
)

// Status of one attempt, carried in LeaderExecResp.LogIndex when Reject is
// set. leaderExecOK is the unrejected reply.
const (
	leaderExecOK uint64 = iota
	// leaderExecNotLeader: refused BEFORE execution. The replica did not lead
	// the shard in the request's term, or left it while waiting to be ready,
	// or its shard has stopped or failed.
	leaderExecNotLeader
	// leaderExecDeposed: the handler ran, and the replica no longer led the
	// shard in the request's term, or the shard had stopped or failed, when
	// it returned. The reply is withheld.
	leaderExecDeposed
	// leaderExecNoHandler: refused before execution by the ready leader of
	// the request's term; no handler registered.
	leaderExecNoHandler
	// leaderExecHandlerFailed: the handler ran and failed; the payload is
	// its error text.
	leaderExecHandlerFailed
	// leaderExecBusy: refused before execution; leaderExecMaxInflight
	// requests are already executing.
	leaderExecBusy
	// leaderExecPaused: refused before execution by the leader of the
	// request's term; its shard is paused (PauseShard).
	leaderExecPaused
	// leaderExecSealed: refused before execution by the leader of the
	// request's term; its shard's barrier is sealed.
	leaderExecSealed
)

// leaderExecOutcome is the answer to one attempt: its status and the reply
// (or the handler's error text).
type leaderExecOutcome struct {
	status  uint64
	payload []byte
}

// leaderExecVerdict is what ExecuteOnLeader does with an attempt that did
// not return a reply. fail, when set, ends the request with its error. spend
// marks the attempt's term as one it may have executed under, so no later
// attempt may use it. Neither means the request was refused before it ran:
// it is retried once the leadership view moves.
type leaderExecVerdict struct {
	spend bool
	fail  func(shardID, leaderID, term uint64, payload []byte) error
}

// leaderExecVerdicts maps every refusal status to its verdict. A remote
// status outside it is normalised to leaderExecDeposed on receipt.
var leaderExecVerdicts = map[uint64]leaderExecVerdict{
	leaderExecNotLeader: {},
	leaderExecDeposed:   {spend: true},
	leaderExecNoHandler: {fail: func(_, _, _ uint64, _ []byte) error {
		return ErrLeaderHandlerNotRegistered
	}},
	leaderExecHandlerFailed: {fail: func(shardID, leaderID, term uint64, payload []byte) error {
		return &LeaderHandlerError{ShardID: shardID, LeaderID: leaderID, Term: term, Message: string(payload)}
	}},
	leaderExecBusy: {fail: func(_, _, _ uint64, _ []byte) error {
		return ErrBusy
	}},
	leaderExecPaused: {fail: func(_, _, _ uint64, _ []byte) error {
		return ErrShardPaused
	}},
	leaderExecSealed: {fail: func(_, _, _ uint64, _ []byte) error {
		return ErrBarrierSealed
	}},
}

// leaderExecReceivers routes the two leader-executed request message types,
// once the receive gate has bound their sender, to the host instead of raft.
var leaderExecReceivers = map[uint64]func(*Host, *proto.Message){
	proto.LeaderExec:     (*Host).serveLeaderExec,
	proto.LeaderExecResp: (*Host).completeLeaderExec,
}

// leaderExecCall is one attempt this host has in flight to a remote leader.
// A response is accepted only from the replica it was sent to, for the shard
// and term it was sent under.
type leaderExecCall struct {
	shardID uint64
	target  uint64
	term    uint64
	done    chan leaderExecOutcome // buffered 1; written once, by the claimant
}

// leaderExecRouter is the host's leader-executed request state.
type leaderExecRouter struct {
	handlers sync.Map // shard ID -> LeaderHandler
	calls    sync.Map // request ID -> *leaderExecCall

	// slots is the semaphore over requests executing for remote callers. close
	// fills it, which is how it waits for the executions in flight.
	slots  chan struct{}
	closed atomic.Bool

	// local counts the in-process executions still running, which an
	// abandoned attempt leaves behind; close waits for them too. closed is
	// set under localMu, which orders every Add before close's Wait.
	localMu sync.Mutex
	local   sync.WaitGroup

	// ctx ends when the host closes: it bounds every execution and wakes
	// every waiting caller.
	ctx    context.Context
	cancel context.CancelFunc
}

func newLeaderExecRouter() *leaderExecRouter {
	ctx, cancel := context.WithCancel(context.Background())
	return &leaderExecRouter{
		slots:  make(chan struct{}, leaderExecMaxInflight),
		ctx:    ctx,
		cancel: cancel,
	}
}

// handler returns the shard's registered handler.
func (r *leaderExecRouter) handler(shardID uint64) (LeaderHandler, bool) {
	v, ok := r.handlers.Load(shardID)
	if !ok {
		return nil, false
	}
	fn, ok := v.(LeaderHandler)
	return fn, ok
}

// acquire claims an execution slot without blocking.
func (r *leaderExecRouter) acquire() bool {
	select {
	case r.slots <- struct{}{}:
	default:
		return false
	}
	if r.closed.Load() {
		<-r.slots
		return false
	}
	return true
}

func (r *leaderExecRouter) release() { <-r.slots }

// startLocal registers an in-process execution, which must call
// r.local.Done when it returns. It reports false once the host is closing.
func (r *leaderExecRouter) startLocal() bool {
	r.localMu.Lock()
	defer r.localMu.Unlock()
	if r.closed.Load() {
		return false
	}
	r.local.Add(1)
	return true
}

// register files call under a fresh request ID.
func (r *leaderExecRouter) register(call *leaderExecCall) uint64 {
	for {
		id := nextRequestKey()
		if _, loaded := r.calls.LoadOrStore(id, call); !loaded {
			return id
		}
	}
}

// bind derives a context that ends with ctx or with the host.
func (r *leaderExecRouter) bind(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// await blocks until changed closes. It returns the caller's context error,
// mapped, or ErrStopped when the host closes first.
func (r *leaderExecRouter) await(ctx context.Context, changed <-chan struct{}) error {
	select {
	case <-changed:
		return nil
	case <-ctx.Done():
		return mapContextError(ctx.Err())
	case <-r.ctx.Done():
		return ErrStopped
	}
}

// close ends every leader-executed request and waits for the executions in
// flight, for remote callers and in process. Safe on a nil router and
// idempotent.
func (r *leaderExecRouter) close() {
	if r == nil {
		return
	}
	// closed is set under localMu, so every startLocal either made its Add
	// before the Wait below or sees closed.
	r.localMu.Lock()
	first := r.closed.CompareAndSwap(false, true)
	r.localMu.Unlock()
	if !first {
		return
	}
	r.cancel()
	for range cap(r.slots) {
		r.slots <- struct{}{}
	}
	r.local.Wait()
}

// RegisterLeaderHandler registers the handler ExecuteOnLeader requests for
// shardID are executed by when this replica leads the shard, replacing any
// handler registered before. It may be called before StartShard, and the
// registration outlives StopShard. Register it on every replica that can
// lead the shard: a leader without one answers ErrLeaderHandlerNotRegistered.
func (h *Host) RegisterLeaderHandler(shardID uint64, handler LeaderHandler) error {
	if h.closed.Load() {
		return ErrClosed
	}
	if handler == nil {
		return ErrInvalidOperation
	}
	h.leaderExec.handlers.Store(shardID, handler)
	return nil
}

// UnregisterLeaderHandler removes the shard's handler. Requests already
// executing finish; later ones are answered ErrLeaderHandlerNotRegistered.
func (h *Host) UnregisterLeaderHandler(shardID uint64) {
	h.leaderExec.handlers.Delete(shardID)
}

// ExecuteOnLeader has the shard's current leader execute request with its
// LeaderHandler and returns the reply. The request is NOT appended to the
// Raft log. Called on the leader it runs in-process; called on any other
// replica it is forwarded over the host's QUIC transport, through the same
// receive gate as a forwarded proposal (mTLS, then the sender bound to the
// replica it claims), to the replica this host knows as leader.
//
// Guarantees:
//
//   - A request is executed only by the replica that leads the shard in the
//     term it was sent under, and only once that leader has committed an
//     entry of the term (the leader's OnLeaderReady). A replica that does not
//     lead in that term refuses it without executing it.
//   - A reply is returned only if this replica has seen no later term than
//     the one it was executed under, and the executor still led that term,
//     with the shard loaded, when the handler returned. A reply from a
//     deposed leader is rejected and the request is retried on the new
//     leader, under the new term; so is any refusal or handler failure
//     answered under a term this replica has seen superseded.
//   - A request is never executed twice under one term: an attempt is sent
//     once, and a term an attempt may have executed under is never used
//     again. Across terms a request may execute more than once (once per
//     term at most), so a handler must be idempotent across a leader change.
//
// It waits, within ctx, for a leader to be known and for leadership to
// settle. When ctx ends first it returns a *LeaderUnavailableError whose
// cause is ErrNoLeader or ErrLeadershipChanged; when ctx ends while an
// attempt is in flight to a leader the view still names, ErrTimeout or
// ErrCanceled. A failing handler is reported as a *LeaderHandlerError, a
// leader without a handler as ErrLeaderHandlerNotRegistered, and a leader
// already executing leaderExecMaxInflight remote requests as ErrBusy, and a
// leader whose shard is paused or sealed as ErrShardPaused or
// ErrBarrierSealed, refused before it ran, as this replica's own are. The
// request is held to the shard's proposal payload limit (EntryTooLargeError),
// and so is the reply. ErrStopped means the shard or the host stopped while
// the request waited, and ErrShardFailed that this replica's failure latch
// tripped. A leader whose shard stops or fails refuses or withholds as a
// deposed one does, so a caller elsewhere retries on the next leader.
//
// Called on the leader, the handler runs on its own goroutine and the caller
// watches the leadership view meanwhile, as it does for a forwarded attempt:
// if leadership moves first, the attempt is abandoned, its term spent, and
// the request retried on the new leader.
//
// Leadership is the executing replica's own view: a leader cut off from its
// quorum keeps executing until it steps down (Config.CheckQuorum bounds that
// to an election timeout), exactly as a leader-local read does.
func (h *Host) ExecuteOnLeader(ctx context.Context, shardID uint64, request []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, mapContextError(err)
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}
	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}
	if ns.paused.Load() {
		return nil, ErrShardPaused
	}
	if ns.barrierSealed() {
		return nil, ErrBarrierSealed
	}
	if limit := effectiveMaxPayload(ns.cfg.MaxProposalPayloadSize, ns.cfg.MaxEntrySize); uint64(len(request)) > limit {
		return nil, &EntryTooLargeError{Size: uint64(len(request)), MaxSize: limit}
	}
	return h.executeOnLeader(ctx, ns, request)
}

// executeOnLeader is ExecuteOnLeader past its preflight: one attempt per
// leadership view, until a leader answers or ctx ends.
//
// spent is the highest term an attempt may have executed under. An attempt
// is made only under a term above it, which is what makes "never twice under
// one term" structural: the executor checks the term, and no term is offered
// to it twice once it might have been used.
func (h *Host) executeOnLeader(ctx context.Context, ns *nodeState, request []byte) ([]byte, error) {
	r := h.leaderExec
	var spent uint64
	for {
		changed := ns.leaderChanged()
		if err := h.leaderExecShardDown(ns); err != nil {
			return nil, err
		}
		leaderID, term, _ := ns.leaderAndTerm()
		if leaderID == 0 || term <= spent {
			if err := r.await(ctx, changed); err != nil {
				return nil, h.leaderUnavailable(ns, err)
			}
			continue
		}

		out, latest, err := h.attemptOnLeader(ctx, ns, leaderID, term, request, changed)
		if err != nil {
			return nil, err
		}
		// Every answer, reply or refusal, is the leader's for term. If this
		// replica has already seen a later term, the leader was deposed:
		// discard the answer whatever it says and retry under the new term.
		// The term is spent, since the answer may follow an execution.
		if _, viewTerm, _ := ns.leaderAndTerm(); viewTerm > term {
			spent = term
			continue
		}
		if out.status == leaderExecOK {
			return out.payload, nil
		}
		verdict := leaderExecVerdicts[out.status]
		if verdict.fail != nil {
			return nil, verdict.fail(ns.shardID, leaderID, term, out.payload)
		}
		if verdict.spend {
			spent = term
			continue
		}
		// Refused before execution: try again once the view moves.
		if err := r.await(ctx, latest); err != nil {
			return nil, h.leaderUnavailable(ns, err)
		}
	}
}

// leaderExecShardDown reports why this replica of the shard can no longer
// take part in a leader-executed request: ErrStopped once StopShard has run,
// ErrShardFailed once the engine node's failure latch has tripped. Neither
// clears the cached leadership view, so the view alone would keep naming a
// replica that will never step again. nil while the replica is live.
func (h *Host) leaderExecShardDown(ns *nodeState) error {
	if ns.stopped.Load() {
		return ErrStopped
	}
	if n := h.engine.GetNode(ns.shardID); n != nil && n.IsFailed() {
		return ErrShardFailed
	}
	return nil
}

// leaderExecRaftLeads reports whether this replica's Raft leads the shard in
// term right now. The nodeState view is what the listener dispatcher last
// delivered, and it delivers asynchronously: Raft may already have stepped
// down, on a higher term or on CheckQuorum, while the view still says it
// leads. The peer's own leadership mirror is published on the step worker
// after every step and tick, under the seqlock the engine reads to detect
// leadership changes in the first place, so it is safe to read here and never
// trails the Raft state machine by more than the step in progress. No
// node, or a node without Raft, does not lead.
func (h *Host) leaderExecRaftLeads(ns *nodeState, term uint64) bool {
	n := h.engine.GetNode(ns.shardID)
	if n == nil {
		return false
	}
	leaderID, current := n.LeaderAndTerm()
	return leaderID == ns.replicaID && current == term
}

// leaderUnavailable converts the error a wait for the leadership view ended
// with into ExecuteOnLeader's answer: the context's error, with the cause
// read from the view, or ErrStopped unchanged.
func (h *Host) leaderUnavailable(ns *nodeState, err error) error {
	if err == ErrStopped {
		return err
	}
	cause := ErrLeadershipChanged
	if leaderID, _, _ := ns.leaderAndTerm(); leaderID == 0 {
		cause = ErrNoLeader
	}
	return &LeaderUnavailableError{ShardID: ns.shardID, Cause: cause, Err: err}
}

// attemptOnLeader makes one attempt under (leaderID, term): in-process when
// this replica is the leader, forwarded otherwise. It returns the outcome and
// the newest leadership-change channel it holds.
func (h *Host) attemptOnLeader(ctx context.Context, ns *nodeState, leaderID, term uint64,
	request []byte, changed <-chan struct{}) (leaderExecOutcome, <-chan struct{}, error) {

	if leaderID == ns.replicaID {
		return h.executeInProcess(ctx, ns, term, request, changed)
	}
	return h.forwardToLeader(ctx, ns, leaderID, term, request, changed)
}

// executeInProcess runs one attempt on this replica, the leader, on its own
// goroutine, and waits for it exactly as forwardToLeader waits for a remote
// leader: if the view leaves (this replica, term) first, the attempt is
// abandoned as leaderExecDeposed and its term spent, so a handler blocked on
// a deposed leader does not hold the request there. The abandoned execution
// runs on, bounded by ctx and the host, on its own copy of the request, and
// its outcome is discarded.
func (h *Host) executeInProcess(ctx context.Context, ns *nodeState, term uint64,
	request []byte, changed <-chan struct{}) (leaderExecOutcome, <-chan struct{}, error) {

	r := h.leaderExec
	if !r.startLocal() {
		return leaderExecOutcome{}, changed, ErrStopped
	}
	// The execution can outlive this attempt, so it gets its own copy of the
	// request, as the receive path makes for a forwarded one: the caller may
	// reuse its buffer once it returns, and a handler that edits its input
	// must not edit the payload a later attempt carries.
	request = append([]byte(nil), request...)
	done := make(chan leaderExecOutcome, 1)
	execCtx, cancel := r.bind(ctx)
	go func() {
		defer r.local.Done()
		defer cancel()
		done <- h.executeLeaderRequest(execCtx, ns, term, request)
	}()
	return h.awaitAttempt(ctx, ns, ns.replicaID, term, done, changed)
}

// awaitAttempt waits for the outcome of the attempt under (leaderID, term).
// If the view leaves that pair, or this replica goes down, before the
// outcome arrives, whether the request executed is unknown, so the attempt is
// reported as leaderExecDeposed: its term is spent and the request moves to
// the new leader.
func (h *Host) awaitAttempt(ctx context.Context, ns *nodeState, leaderID, term uint64,
	done <-chan leaderExecOutcome, changed <-chan struct{}) (leaderExecOutcome, <-chan struct{}, error) {

	r := h.leaderExec
	for {
		select {
		case out := <-done:
			return out, changed, nil
		case <-changed:
			changed = ns.leaderChanged()
			viewLeader, viewTerm, _ := ns.leaderAndTerm()
			if viewLeader == leaderID && viewTerm == term && h.leaderExecShardDown(ns) == nil {
				continue
			}
			return leaderExecOutcome{status: leaderExecDeposed}, changed, nil
		case <-ctx.Done():
			return leaderExecOutcome{}, changed, mapContextError(ctx.Err())
		case <-r.ctx.Done():
			return leaderExecOutcome{}, changed, ErrStopped
		}
	}
}

// forwardToLeader sends one LeaderExec to leaderID and waits for its answer
// (awaitAttempt). The attempt is sent ONCE. A late answer, for an attempt
// abandoned when the view moved, finds no call registered and is dropped.
func (h *Host) forwardToLeader(ctx context.Context, ns *nodeState, leaderID, term uint64,
	request []byte, changed <-chan struct{}) (leaderExecOutcome, <-chan struct{}, error) {

	r := h.leaderExec
	call := &leaderExecCall{
		shardID: ns.shardID,
		target:  leaderID,
		term:    term,
		done:    make(chan leaderExecOutcome, 1),
	}
	id := r.register(call)
	defer r.calls.CompareAndDelete(id, call)

	var budget uint64
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			budget = uint64(remaining)
		}
	}
	h.sender.Send([]proto.Message{{
		Type:     proto.LeaderExec,
		ShardID:  ns.shardID,
		From:     ns.replicaID,
		To:       leaderID,
		Term:     term,
		Hint:     id,
		LogIndex: budget,
		Entries:  []proto.Entry{{Cmd: request}},
	}})
	return h.awaitAttempt(ctx, ns, leaderID, term, call.done, changed)
}

// executeLeaderRequest is the executor: it runs the shard's handler for a
// request sent under term, if and only if this replica leads the shard in
// that term, and reports whether it still did when the handler returned.
// Leading is the cached view AND Raft's own state (leaderExecRaftLeads) AND
// the replica still live (leaderExecShardDown). The view alone can lag a
// step-down Raft has already taken. Neither StopShard nor the engine's failure
// latch clears the view, and a failed node steps no further Raft, so it is
// deposed without hearing of it.
// A replica refusing for either reason answers as one that does not lead,
// so a remote caller retries on the leader the others elect; a local caller
// finds its own replica down and returns ErrStopped or ErrShardFailed.
func (h *Host) executeLeaderRequest(ctx context.Context, ns *nodeState, term uint64, request []byte) leaderExecOutcome {
	// Wait for the term to be ready: a new leader has not yet committed an
	// entry of its term, so entries an earlier leader committed may not be
	// known committed here. It does within a heartbeat or two.
	for {
		changed := ns.leaderChanged()
		_, current, isLeader, ready := ns.leaderTermReady()
		if h.leaderExecShardDown(ns) != nil || !isLeader || current != term || !h.leaderExecRaftLeads(ns, term) {
			return leaderExecOutcome{status: leaderExecNotLeader}
		}
		// A paused leader's frozen Raft still reports it leads, and the
		// remote path never passes ExecuteOnLeader's preflight: the
		// executor refuses as every client entry point does.
		if ns.paused.Load() {
			return leaderExecOutcome{status: leaderExecPaused}
		}
		if ns.barrierSealed() {
			return leaderExecOutcome{status: leaderExecSealed}
		}
		if ready {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return leaderExecOutcome{status: leaderExecNotLeader}
		}
	}
	// Only the leader of the request's term reports a missing handler: from
	// any other replica the caller would take the configuration error for
	// the leader's, and it is permanent.
	handler, ok := h.leaderExec.handler(ns.shardID)
	if !ok {
		return leaderExecOutcome{status: leaderExecNoHandler}
	}

	reply, failure, failed := callLeaderHandler(ctx, handler, term, request)

	if _, current, isLeader, _ := ns.leaderTermReady(); h.leaderExecShardDown(ns) != nil ||
		!isLeader || current != term || !h.leaderExecRaftLeads(ns, term) {
		return leaderExecOutcome{status: leaderExecDeposed}
	}
	if failed {
		return leaderExecOutcome{status: leaderExecHandlerFailed, payload: []byte(failure)}
	}
	if limit := effectiveMaxPayload(ns.cfg.MaxProposalPayloadSize, ns.cfg.MaxEntrySize); uint64(len(reply)) > limit {
		failure = (&EntryTooLargeError{Size: uint64(len(reply)), MaxSize: limit}).Error()
		return leaderExecOutcome{status: leaderExecHandlerFailed, payload: []byte(failure)}
	}
	return leaderExecOutcome{status: leaderExecOK, payload: reply}
}

// callLeaderHandler runs handler, converting an error or a panic into its
// text, bounded to leaderExecMaxFailureText. A remote caller's request must
// not be able to take the leader's process down with it.
func callLeaderHandler(ctx context.Context, handler LeaderHandler, term uint64,
	request []byte) (reply []byte, failure string, failed bool) {

	defer func() {
		if p := recover(); p != nil {
			reply, failure, failed = nil, truncateFailure(fmt.Sprintf("panic: %v", p)), true
		}
	}()
	reply, err := handler(ctx, term, request)
	if err != nil {
		return nil, truncateFailure(err.Error()), true
	}
	return reply, "", false
}

func truncateFailure(text string) string {
	if len(text) > leaderExecMaxFailureText {
		return text[:leaderExecMaxFailureText]
	}
	return text
}

// leaderExecBudget is the time an execution for a remote caller may take:
// the caller's remaining budget, or defaultPendingRequestTimeout when it sent
// none or one beyond it.
func leaderExecBudget(nanos uint64) time.Duration {
	budget := time.Duration(nanos)
	if budget <= 0 || budget > defaultPendingRequestTimeout {
		return defaultPendingRequestTimeout
	}
	return budget
}

// serveLeaderExec executes a LeaderExec from a remote caller and answers it.
// The receive gate has bound msg.From to the connection's identity, so the
// answer goes to the replica that really sent it. Execution runs on its own
// goroutine: the transport's receive path is never held by a handler.
func (h *Host) serveLeaderExec(msg *proto.Message) {
	r := h.leaderExec
	if len(msg.Entries) != 1 {
		slog.Debug("dropped malformed LeaderExec",
			"shard", msg.ShardID, "from", msg.From, "entries", len(msg.Entries))
		return
	}
	if r.closed.Load() {
		return
	}
	// The receive gate drops a message for a shard not loaded here.
	ns, err := h.getNode(msg.ShardID)
	if err != nil {
		return
	}
	// The answer comes from this replica's own ID: the gate admits To == 0,
	// and the caller accepts an answer only from the replica it asked.
	caller, term, id := msg.From, msg.Term, msg.Hint
	reply := func(out leaderExecOutcome) {
		h.replyLeaderExec(ns.shardID, ns.replicaID, caller, term, id, out)
	}
	if !r.acquire() {
		reply(leaderExecOutcome{status: leaderExecBusy})
		return
	}
	request := append([]byte(nil), msg.Entries[0].Cmd...)
	budget := leaderExecBudget(msg.LogIndex)
	go func() {
		defer r.release()
		ctx, cancel := context.WithTimeout(r.ctx, budget)
		out := h.executeLeaderRequest(ctx, ns, term, request)
		cancel()
		reply(out)
	}()
}

// replyLeaderExec sends the answer to a remote attempt.
func (h *Host) replyLeaderExec(shardID, from, to, term, id uint64, out leaderExecOutcome) {
	h.sender.Send([]proto.Message{{
		Type:     proto.LeaderExecResp,
		ShardID:  shardID,
		From:     from,
		To:       to,
		Term:     term,
		Hint:     id,
		Reject:   out.status != leaderExecOK,
		LogIndex: out.status,
		Entries:  []proto.Entry{{Cmd: out.payload}},
	}})
}

// completeLeaderExec hands a LeaderExecResp to the attempt waiting for it.
// Only the replica the attempt went to may answer it, for the shard and the
// term it was sent under; anything else is dropped. A status this host does
// not know is taken as leaderExecDeposed: it cannot tell whether the request
// ran, so the term is spent rather than offered again.
func (h *Host) completeLeaderExec(msg *proto.Message) {
	r := h.leaderExec
	v, ok := r.calls.Load(msg.Hint)
	if !ok {
		return
	}
	call, ok := v.(*leaderExecCall)
	if !ok || call.shardID != msg.ShardID || call.target != msg.From ||
		call.term != msg.Term || len(msg.Entries) != 1 {
		slog.Debug("dropped LeaderExecResp that does not answer the attempt it names",
			"shard", msg.ShardID, "from", msg.From, "term", msg.Term)
		return
	}
	if !r.calls.CompareAndDelete(msg.Hint, call) {
		return
	}
	out := leaderExecOutcome{status: leaderExecOK, payload: append([]byte(nil), msg.Entries[0].Cmd...)}
	if msg.Reject {
		out.status = msg.LogIndex
		if _, known := leaderExecVerdicts[out.status]; !known {
			out.status = leaderExecDeposed
		}
	}
	call.done <- out
}
