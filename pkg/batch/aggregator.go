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

package batch

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

const (
	// DefaultMaxBatchSize is the maximum number of proposals collected
	// before a flush is triggered.
	DefaultMaxBatchSize = 1000

	// DefaultFlushInterval is the maximum time between batch flushes.
	// If the batch has not reached MaxBatchSize within this interval,
	// it is flushed on the timer.
	DefaultFlushInterval = 50 * time.Millisecond

	// DefaultQueueSize is the capacity of the proposal channel. When
	// the queue is full, Submit returns a QueueFullError immediately
	// rather than blocking.
	DefaultQueueSize = 10000

	// DefaultProposeTimeout is the ceiling on one batch's SyncPropose. It is
	// generous — a propose that has to wait out an election legitimately takes
	// seconds — but finite: the flush goroutine is single, so an unbounded
	// propose stops every shard's flush and wedges Close (see proposeContext).
	DefaultProposeTimeout = 30 * time.Second

	// DefaultMaxBatchBytes is the maximum encoded size of one flushed batch.
	//
	// It is proto.MaxEntrySize because the encoded batch is proposed as a
	// SINGLE Raft entry command: proto.Entry rejects a Cmd above that at
	// marshal AND unmarshal time, so it is an absolute wire-format ceiling no
	// configuration can raise (Config.MaxEntrySize and
	// Config.MaxProposalPayloadSize can only lower it, and
	// MaxReplicationPayloadBytes — the looser bound on a whole replication
	// batch — is above it). Without a byte cap only the proposal COUNT
	// triggers a flush, so DefaultMaxBatchSize commands of a few KiB each
	// build a batch the host rejects whole as EntryTooLarge; the aggregator
	// has no split-and-retry, and the next batch forms identically, so writes
	// that individually succeed fail forever.
	DefaultMaxBatchBytes uint64 = uint64(proto.MaxEntrySize)
)

// Flush reason labels for the flushReasonTotal metric.
const (
	flushReasonSize  = "size"
	flushReasonBytes = "bytes"
	flushReasonTime  = "time"
	flushReasonClose = "close"
)

// Proposer defines the interface for submitting proposals to Raft.
// This is intentionally minimal: implementors wrap their Host's
// SyncPropose and discard the result value, returning only the error.
type Proposer interface {
	// SyncPropose submits a command to the specified shard and blocks
	// until the proposal is committed or an error occurs.
	SyncPropose(ctx context.Context, shardID uint64, cmd []byte) error
}

// proposal represents a single write operation waiting to be batched.
type proposal struct {
	ctx      context.Context
	shardID  uint64
	data     []byte
	resultCh chan error
}

// Config configures the batch aggregator.
type Config struct {
	// MaxBatchSize is the maximum number of proposals that can be
	// accumulated before a flush is triggered.
	MaxBatchSize int

	// MaxBatchBytes is the maximum ENCODED size of the accumulated batch. A
	// proposal that would push the batch past it triggers a flush first, so
	// the batch handed to the proposer never exceeds it and an oversized
	// proposal ends up alone in its own batch instead of poisoning the
	// unrelated commands batched with it. See DefaultMaxBatchBytes.
	//
	// The cap is applied to the WHOLE accumulated batch rather than per shard.
	// A per-shard subset is never larger than the whole, so the bound holds
	// for every shard's proposal; a multi-shard workload simply flushes
	// slightly sooner than the per-shard arithmetic would require.
	MaxBatchBytes uint64

	// FlushInterval is the maximum duration between batch flushes.
	FlushInterval time.Duration

	// QueueSize is the capacity of the proposal channel.
	QueueSize int

	// ProposeTimeout is the CEILING on a single batch's SyncPropose. A batch
	// is proposed under min(latest live caller deadline, ProposeTimeout): the
	// aggregator never proposes for longer than this, and never cuts a caller
	// short of its own deadline. See proposeContext.
	ProposeTimeout time.Duration
}

// DefaultConfig returns a Config with production-ready default values.
func DefaultConfig() Config {
	return Config{
		MaxBatchSize:   DefaultMaxBatchSize,
		MaxBatchBytes:  DefaultMaxBatchBytes,
		FlushInterval:  DefaultFlushInterval,
		QueueSize:      DefaultQueueSize,
		ProposeTimeout: DefaultProposeTimeout,
	}
}

// Option configures the aggregator via the functional options pattern.
type Option func(*Aggregator)

// WithConfig sets custom batch configuration. Zero-valued fields are
// replaced with defaults to prevent misconfiguration.
func WithConfig(cfg Config) Option {
	return func(a *Aggregator) {
		if cfg.MaxBatchSize > 0 {
			a.cfg.MaxBatchSize = cfg.MaxBatchSize
		}
		if cfg.MaxBatchBytes > 0 {
			a.cfg.MaxBatchBytes = cfg.MaxBatchBytes
		}
		if cfg.FlushInterval > 0 {
			a.cfg.FlushInterval = cfg.FlushInterval
		}
		if cfg.QueueSize > 0 {
			a.cfg.QueueSize = cfg.QueueSize
		}
		if cfg.ProposeTimeout > 0 {
			a.cfg.ProposeTimeout = cfg.ProposeTimeout
		}
	}
}

// WithLogger sets a custom structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(a *Aggregator) {
		if logger != nil {
			a.logger = logger
		}
	}
}

// WithRegistry sets a custom Prometheus registerer for metrics. If nil,
// prometheus.DefaultRegisterer is used.
func WithRegistry(reg prometheus.Registerer) Option {
	return func(a *Aggregator) {
		if reg != nil {
			a.registry = reg
		}
	}
}

// aggregatorMetrics holds all Prometheus metrics for the aggregator.
//
// Cardinality policy: the library targets 10K+ shards, so the per-shard
// "shard" label is carried ONLY by the batchesTotal counter (2 series per
// shard, needed for per-shard flush/failure attribution). The batchSize and
// flushDuration histograms are deliberately unlabeled aggregates — with
// their 11/14 buckets a shard label would multiply into hundreds of
// thousands of time series from this component alone.
type aggregatorMetrics struct {
	batchesTotal     *prometheus.CounterVec
	batchSize        prometheus.Histogram
	flushDuration    prometheus.Histogram
	queueDepth       prometheus.Gauge
	flushReasonTotal *prometheus.CounterVec
}

// Aggregator batches multiple write operations and flushes them
// periodically or when a size threshold is reached. This amortizes
// the per-proposal Raft consensus overhead across many operations,
// significantly improving write throughput.
//
// Delivery semantics: AT-LEAST-ONCE. The aggregator proposes a raw encoded
// batch with no client session, so it has no at-most-once path. A flush that
// fails with FlushError.Indeterminate set (proposer timeout or cancellation)
// may nonetheless commit and apply; retrying such a batch applies every
// command in it a second time. Callers needing exactly-once semantics must
// make their commands idempotent or propose through a session instead.
//
// Thread safety: Submit may be called concurrently from multiple
// goroutines. The aggregator manages its own background flush loop, which
// flushes each shard's batch concurrently so one slow shard cannot stall
// the others.
type Aggregator struct {
	cfg      Config
	proposer Proposer
	logger   *slog.Logger
	registry prometheus.Registerer

	// Batching state - protected by mu. batchBytes is the encoded size of
	// currentBatch, maintained incrementally so the byte cap costs no scan.
	mu           sync.Mutex
	currentBatch []*proposal
	batchBytes   uint64

	// Queue for incoming proposals.
	queue chan *proposal

	// Lifecycle management.
	closed    atomic.Bool
	closeChan chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	// lifetimeCtx is the parent of every propose context. Proposing under a
	// CALLER's context makes one submitter's deadline (or cancellation) govern
	// the whole batch it happens to share, so the aggregator owns the parent
	// and derives a bounded child per flush. lifetimeCancel runs after the
	// flush loop has exited, releasing anything still derived from it.
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc
	// flushDone is closed when flushLoop returns. A Submit blocked on the
	// close path uses it as a terminator: once the loop has exited, any
	// proposal not already flushed will never be flushed, so Submit must not
	// wait on resultCh forever (a caller with a non-cancellable context would
	// otherwise hang).
	flushDone chan struct{}

	// Metrics.
	metrics     *aggregatorMetrics
	metricsOnce sync.Once
}

// NewAggregator creates a batch aggregator that groups proposals and
// flushes them through the given Proposer. It starts a background
// goroutine for the flush loop which is stopped by calling Close.
//
// Returns a NilProposerError if proposer is nil.
func NewAggregator(proposer Proposer, opts ...Option) (*Aggregator, error) {
	if proposer == nil {
		return nil, &NilProposerError{}
	}

	a := &Aggregator{
		cfg:       DefaultConfig(),
		proposer:  proposer,
		logger:    slog.Default(),
		registry:  prometheus.DefaultRegisterer,
		closeChan: make(chan struct{}),
		flushDone: make(chan struct{}),
	}
	a.lifetimeCtx, a.lifetimeCancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(a)
	}

	a.queue = make(chan *proposal, a.cfg.QueueSize)
	a.currentBatch = make([]*proposal, 0, a.cfg.MaxBatchSize)

	a.initMetrics()

	a.wg.Add(1)
	go a.flushLoop()

	return a, nil
}

// Submit enqueues a proposal for batching and blocks until the batch
// containing this proposal is flushed or the context is canceled.
//
// OWNERSHIP: Submit COPIES data. The aggregator holds a proposal until its
// batch flushes — up to FlushInterval later, plus the propose itself — and
// Submit can return before that (its own context expiring is the common case),
// so the caller regains control of data while the aggregator has not yet read
// it. Keeping the caller's slice would let a pooled or reused buffer be
// refilled in the window between the two, and the flush would then encode and
// replicate the buffer's LATER contents: a wrong command committed identically
// on every replica, with no divergence to detect it. Callers that can
// guarantee the buffer is never touched again may use SubmitNoCopy.
//
// Returns:
//   - ClosedError if the aggregator has been closed
//   - QueueFullError if the queue is at capacity
//   - context.Canceled or context.DeadlineExceeded if ctx is done
//   - FlushError if the batch flush fails
//
// A returned FlushError wraps the proposer's error (use errors.Is/errors.As
// to inspect it) and sets Indeterminate when the outcome is unknown rather
// than a definite rejection. Retrying an indeterminate submission can apply
// the batch twice; see the Aggregator doc comment.
func (a *Aggregator) Submit(ctx context.Context, shardID uint64, data []byte) error {
	// append to a nil slice rather than make+copy: it yields nil for an empty
	// command (which encodes identically) and one allocation otherwise.
	return a.submit(ctx, shardID, append([]byte(nil), data...))
}

// SubmitNoCopy is Submit without the defensive copy of data.
//
// OWNERSHIP: the caller PERMANENTLY transfers data to the aggregator. The
// aggregator reads the slice at an unspecified later time — after Submit's
// caller has returned, on the flush goroutine — and the caller must never
// write to, reslice into, reuse, or return data to a pool, whatever
// SubmitNoCopy returned and however it returned. That includes the error
// paths: a QueueFullError means the proposal was never enqueued, but a
// context error means it WAS, and the batch it is in may still be encoded and
// replicated. Treat data as owned by the aggregator forever.
//
// Use it only for a buffer that is allocated per call and then dropped. When
// in doubt use Submit; a wrong command committed on every replica is not
// detectable after the fact.
//
// It returns the same errors as Submit.
func (a *Aggregator) SubmitNoCopy(ctx context.Context, shardID uint64, data []byte) error {
	return a.submit(ctx, shardID, data)
}

// submit enqueues a proposal whose data the aggregator already owns and waits
// for its batch's result. It backs both Submit and SubmitNoCopy, which differ
// only in how that ownership was obtained.
func (a *Aggregator) submit(ctx context.Context, shardID uint64, data []byte) error {
	if a.closed.Load() {
		return &ClosedError{}
	}

	p := &proposal{
		ctx:      ctx,
		shardID:  shardID,
		data:     data,
		resultCh: make(chan error, 1),
	}

	select {
	case a.queue <- p:
		a.metrics.queueDepth.Set(float64(len(a.queue)))
	default:
		return &QueueFullError{QueueSize: a.cfg.QueueSize}
	}

	return a.awaitResult(ctx, p)
}

// awaitResult blocks until p's batch reports a result, the caller's context
// ends, or the aggregator closes, and returns what the caller should see.
func (a *Aggregator) awaitResult(ctx context.Context, p *proposal) error {
	select {
	case err := <-p.resultCh:
		return err
	case <-ctx.Done():
		// The result may have arrived in the same instant the context expired.
		// select picks uniformly among ready cases, so honoring ctx.Done()
		// blind reports a timeout for a batch that is already committed HALF
		// the time — and the caller's only recourse is to retry a batch of up
		// to MaxBatchSize commands with no at-most-once path. Drain the result
		// first: a delivered result is always the better answer.
		select {
		case err := <-p.resultCh:
			return err
		default:
			return ctx.Err()
		}
	case <-a.closeChan:
		// Close raced with this submission. If the proposal was enqueued
		// before flushLoop's close branch drained the queue, that branch
		// flushes it and sends the real result on resultCh — returning a
		// spurious ClosedError would wrongly tell the caller the write
		// failed while the command is in fact replicated, so we prefer the
		// genuine result. But the enqueue may also have landed in the
		// buffered queue AFTER flushLoop already drained and exited, in
		// which case no result will ever arrive; flushDone bounds the wait
		// so a caller with a non-cancellable context cannot hang. On
		// flushDone we make one final non-blocking check (the result may
		// have been delivered just before the loop returned) and otherwise
		// report closed.
		select {
		case err := <-p.resultCh:
			return err
		case <-ctx.Done():
			// Same drain-first rule as above: a result already delivered beats
			// the context error it tied with.
			select {
			case err := <-p.resultCh:
				return err
			default:
				return ctx.Err()
			}
		case <-a.flushDone:
			select {
			case err := <-p.resultCh:
				return err
			default:
				return &ClosedError{}
			}
		}
	}
}

// BatchLen returns the number of proposals in the current unflushed batch.
func (a *Aggregator) BatchLen() int {
	a.mu.Lock()
	n := len(a.currentBatch)
	a.mu.Unlock()
	return n
}

// Close signals the aggregator to stop accepting new proposals, flushes
// any remaining batched proposals, and waits for all goroutines to exit.
// Close is safe to call multiple times; subsequent calls are no-ops.
func (a *Aggregator) Close() error {
	a.closeOnce.Do(func() {
		a.closed.Store(true)
		close(a.closeChan)
	})
	a.wg.Wait()
	// AFTER wg.Wait, not before: the close path's final flush proposes under a
	// child of lifetimeCtx, and cancelling first would fail the very batch
	// Close exists to drain. By here the flush loop has returned and nothing
	// is in flight, so this only releases the parent.
	a.lifetimeCancel()
	return nil
}

// flushLoop is the background goroutine that dequeues proposals, adds
// them to the current batch, and triggers flushes on size threshold or
// timer expiry.
func (a *Aggregator) flushLoop() {
	defer a.wg.Done()
	// Signal Submit's close-path waiters that no further flushing will occur.
	defer close(a.flushDone)

	timer := time.NewTimer(a.cfg.FlushInterval)
	defer timer.Stop()

	for {
		select {
		case p := <-a.queue:
			a.metrics.queueDepth.Set(float64(len(a.queue)))
			a.addToBatch(p)

			if a.BatchLen() >= a.cfg.MaxBatchSize {
				a.flush(flushReasonSize)
				timer.Reset(a.cfg.FlushInterval)
			}

		case <-timer.C:
			a.mu.Lock()
			hasPending := len(a.currentBatch) > 0
			a.mu.Unlock()

			if hasPending {
				a.flush(flushReasonTime)
			}
			timer.Reset(a.cfg.FlushInterval)

		case <-a.closeChan:
			// Drain remaining items from the queue.
			a.drainQueue()

			// Final flush of any accumulated proposals.
			a.mu.Lock()
			hasPending := len(a.currentBatch) > 0
			a.mu.Unlock()

			if hasPending {
				a.flush(flushReasonClose)
			}
			return
		}
	}
}

// addToBatch appends a proposal to the current batch under the lock, first
// flushing the accumulated batch when p would push it past MaxBatchBytes.
//
// Flushing BEFORE the append (rather than after) is what keeps an oversized
// proposal from taking unrelated commands down with it: a proposal that
// exceeds the cap on its own is proposed alone, so the host's EntryTooLarge
// rejection reaches its own caller as a definite error while everything
// batched with it commits normally.
//
// Only flushLoop mutates currentBatch, so releasing the lock between the
// decision and the append cannot lose an interleaved proposal.
func (a *Aggregator) addToBatch(p *proposal) {
	size := proposalBytes(p)

	a.mu.Lock()
	overflow := len(a.currentBatch) > 0 && a.batchBytes+size > a.cfg.MaxBatchBytes
	a.mu.Unlock()

	if overflow {
		a.flush(flushReasonBytes)
	}

	a.mu.Lock()
	a.currentBatch = append(a.currentBatch, p)
	a.batchBytes += size
	a.mu.Unlock()
}

// proposalBytes is a proposal's contribution to the encoded batch: its
// command plus the length prefix EncodeBatch writes in front of it.
func proposalBytes(p *proposal) uint64 {
	return uint64(len(p.data)) + lengthPrefixSize
}

// drainQueue reads all remaining proposals from the queue channel and
// adds them to the current batch. Called during shutdown to ensure no
// proposals are lost. It goes through addToBatch, so a drain large enough to
// exceed MaxBatchBytes still flushes in bounded pieces rather than building
// one unproposable batch at the very moment the caller wants everything
// durable.
func (a *Aggregator) drainQueue() {
	for {
		select {
		case p := <-a.queue:
			a.addToBatch(p)
		default:
			return
		}
	}
}

// flush takes ownership of the current batch under the lock, encodes all
// proposals as a length-prefixed batch, submits the batch to the proposer,
// and distributes the result to all waiting callers.
func (a *Aggregator) flush(reason string) {
	a.mu.Lock()
	batch := a.currentBatch
	a.currentBatch = make([]*proposal, 0, a.cfg.MaxBatchSize)
	a.batchBytes = 0
	a.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	a.metrics.flushReasonTotal.WithLabelValues(reason).Inc()

	// Group proposals by shard for per-shard batch encoding.
	shardBatches := make(map[uint64][]*proposal)
	for _, p := range batch {
		shardBatches[p.shardID] = append(shardBatches[p.shardID], p)
	}

	// Flush every shard concurrently. Flushing them in sequence on the single
	// flushLoop goroutine lets one slow or leaderless shard block every other
	// shard's flush for the full proposal timeout, backing the queue up to
	// QueueSize and producing QueueFullError for unrelated shards.
	if len(shardBatches) == 1 {
		for shardID, shardProposals := range shardBatches {
			a.flushShard(shardID, shardProposals)
		}
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(shardBatches))
	for shardID, shardProposals := range shardBatches {
		go func() {
			defer wg.Done()
			a.flushShard(shardID, shardProposals)
		}()
	}
	wg.Wait()
}

// flushShard encodes and submits a batch of proposals for a single shard,
// then distributes the result to all waiting callers.
func (a *Aggregator) flushShard(shardID uint64, proposals []*proposal) {
	shardLabel := strconv.FormatUint(shardID, 10)
	a.metrics.batchesTotal.WithLabelValues(shardLabel, "attempted").Inc()

	// Drop the proposals whose callers have already given up BEFORE the batch
	// is encoded. Selecting only the context to propose UNDER is not enough:
	// a caller told context.Canceled — a DEFINITE rejection, not an
	// indeterminate outcome — still had its command carried into the encoded
	// payload by whichever proposal in the batch was still live, and every
	// replica applied it. Encode only the survivors.
	live := a.rejectAbandoned(shardID, proposals)
	batchLen := len(live)
	if batchLen == 0 {
		// Nobody is left to receive a result, and proposing anyway would
		// replicate a batch whose outcome no one observes while occupying the
		// flush goroutine for the whole propose.
		a.metrics.batchesTotal.WithLabelValues(shardLabel, "failed").Inc()
		return
	}

	a.metrics.batchSize.Observe(float64(batchLen))

	// Encode the surviving proposal data as a length-prefixed batch.
	cmds := make([][]byte, batchLen)
	for i, p := range live {
		cmds[i] = p.data
	}
	encoded, encErr := EncodeBatch(cmds)
	if encErr != nil {
		// A command the batch format cannot represent. This is a definite
		// rejection: nothing was proposed, and a retry of the same command
		// fails identically.
		a.metrics.batchesTotal.WithLabelValues(shardLabel, "failed").Inc()
		a.logger.Error("batch encode failed",
			slog.Uint64("shard_id", shardID),
			slog.Int("batch_size", batchLen),
			slog.String("error", encErr.Error()),
		)
		flushErr := &FlushError{ShardID: shardID, Err: encErr, Indeterminate: false}
		for _, p := range live {
			p.resultCh <- flushErr
		}
		return
	}

	// A caller may still have gone away between the filter and here; that
	// leaves nobody to receive a result, so the batch is dropped rather than
	// proposed blind.
	ctx, cancel, ok := a.proposeContext(live)
	if !ok {
		a.metrics.batchesTotal.WithLabelValues(shardLabel, "failed").Inc()
		flushErr := &FlushError{ShardID: shardID, Err: context.Canceled, Indeterminate: false}
		for _, p := range live {
			p.resultCh <- flushErr
		}
		return
	}
	defer cancel()

	start := time.Now()
	err := a.proposer.SyncPropose(ctx, shardID, encoded)
	elapsed := time.Since(start)

	a.metrics.flushDuration.Observe(elapsed.Seconds())

	if err != nil {
		// A timeout or cancellation does NOT mean the proposal was rejected:
		// the entry may already be replicated and will apply. Reporting it as
		// a definite failure would tell the caller a committed write failed,
		// and a blind retry would duplicate the whole batch. Flag the outcome
		// as indeterminate so the caller can distinguish the two.
		indeterminate := errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			ctx.Err() != nil
		a.metrics.batchesTotal.WithLabelValues(shardLabel, "failed").Inc()
		a.logger.Warn("batch flush failed",
			slog.Uint64("shard_id", shardID),
			slog.Int("batch_size", batchLen),
			slog.Bool("indeterminate", indeterminate),
			slog.String("error", err.Error()),
		)
		flushErr := &FlushError{ShardID: shardID, Err: err, Indeterminate: indeterminate}
		for _, p := range live {
			p.resultCh <- flushErr
		}
		return
	}

	a.metrics.batchesTotal.WithLabelValues(shardLabel, "success").Inc()

	for _, p := range live {
		p.resultCh <- nil
	}
}

// rejectAbandoned returns the proposals whose callers are still waiting, and
// reports a DEFINITE failure carrying its own context error to each one that
// is not. Filtering in place is safe: the destination index never runs ahead
// of the source, and the per-shard slice is not shared.
func (a *Aggregator) rejectAbandoned(shardID uint64, proposals []*proposal) []*proposal {
	live := proposals[:0]
	for _, p := range proposals {
		if ctxErr := p.ctx.Err(); ctxErr != nil {
			p.resultCh <- &FlushError{ShardID: shardID, Err: ctxErr, Indeterminate: false}
			continue
		}
		live = append(live, p)
	}
	return live
}

// proposeContext returns the context a batch's SyncPropose runs under, a
// cancel func the caller must invoke, and whether the batch should be
// proposed at all.
//
// It reports false when EVERY proposal context is already done. That case used
// to fall back to context.Background(), which is unbounded: Host.SyncPropose
// selects on the result channel and ctx.Done(), so with a Background context
// neither arm can ever fire and the propose blocks forever. It blocks on the
// SINGLE flush goroutine, so the aggregator then stops flushing entirely —
// the queue fills, every later Submit returns QueueFullError, and Close hangs
// on wg.Wait with nothing left to release it. The trigger is worse than
// incidental: all contexts being cancelled at once is what mass caller
// cancellation looks like, which is what quorum loss produces, which is
// precisely when the propose cannot complete. There is also no caller left to
// receive the result, so the batch is dropped rather than proposed blind.
//
// The returned context is NEVER a caller's context, and is never derived from
// one. A batch is shared by callers who know nothing of each other, so
// adopting one of them made an arbitrary submitter — whichever landed in the
// batch first — govern all the rest: a 200ms caller aborted a 30s caller's
// propose after 200ms of its budget (and an at-least-once retry then
// duplicated the WHOLE batch), while a caller supplying an hour-long deadline
// bypassed ProposeTimeout entirely and parked the single flush goroutine for
// the hour, which is precisely the wedge described above.
//
// The parent is instead the aggregator's own lifetime context, and the
// deadline is min(latest live caller deadline, now+ProposeTimeout):
//
//   - the ProposeTimeout ceiling always applies, so no caller can leave the
//     flush goroutine parked past it;
//   - taking the LATEST rather than the earliest caller deadline means no
//     caller is cut short of the budget it asked for — a caller that has
//     already timed out is simply no longer waiting, and the batch it shares
//     is still owed to everyone else.
func (a *Aggregator) proposeContext(proposals []*proposal) (context.Context, context.CancelFunc, bool) {
	deadline := time.Now().Add(a.cfg.ProposeTimeout)
	var latest time.Time
	live, allBounded := false, true

	for _, p := range proposals {
		if p.ctx.Err() != nil {
			continue
		}
		live = true
		d, hasDeadline := p.ctx.Deadline()
		if !hasDeadline {
			// A deadline-less caller waits indefinitely; only ProposeTimeout
			// bounds this batch.
			allBounded = false
			continue
		}
		if d.After(latest) {
			latest = d
		}
	}

	if !live {
		return nil, nil, false
	}
	if allBounded && latest.Before(deadline) {
		deadline = latest
	}
	ctx, cancel := context.WithDeadline(a.lifetimeCtx, deadline)
	return ctx, cancel, true
}

// initMetrics creates and registers Prometheus metrics. Uses sync.Once
// for safe re-registration when running multiple aggregators in tests.
func (a *Aggregator) initMetrics() {
	a.metricsOnce.Do(func() {
		m := &aggregatorMetrics{
			batchesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "batches_total",
				Help:      "Total number of batch flush operations by shard and status.",
			}, []string{"shard", "status"}),

			// Aggregate (no shard label) by design — see the
			// aggregatorMetrics cardinality policy.
			batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "batch_size",
				Help:      "Number of proposals in each flushed batch (all shards).",
				Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
			}),

			// Aggregate (no shard label) by design — see the
			// aggregatorMetrics cardinality policy.
			flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "flush_duration_seconds",
				Help:      "Duration of batch flush operations (proposer SyncPropose call, all shards).",
				Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			}),

			queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "queue_depth",
				Help:      "Current number of proposals waiting in the queue.",
			}),

			flushReasonTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "flush_reason_total",
				Help:      "Total batch flushes by trigger reason.",
			}, []string{"reason"}),
		}

		// Register metrics, tolerating AlreadyRegisteredError for
		// multi-aggregator or test scenarios. On conflict we adopt the
		// ALREADY-REGISTERED collector instead of keeping our fresh,
		// unregistered one — otherwise increments/observations would go to a
		// collector that is never scraped and the batch metrics would silently
		// vanish.
		m.batchesTotal = registerOrExisting(a.registry, m.batchesTotal, a.logger)
		m.batchSize = registerOrExisting(a.registry, m.batchSize, a.logger)
		m.flushDuration = registerOrExisting(a.registry, m.flushDuration, a.logger)
		m.queueDepth = registerOrExisting(a.registry, m.queueDepth, a.logger)
		m.flushReasonTotal = registerOrExisting(a.registry, m.flushReasonTotal, a.logger)

		a.metrics = m
	})
}

// registerOrExisting registers c with reg. If an equivalent collector is
// already registered, it returns that existing collector (so metric updates
// target the scraped instance); otherwise it returns c. Other registration
// errors are logged and c is returned unchanged.
func registerOrExisting[T prometheus.Collector](reg prometheus.Registerer, c T, logger *slog.Logger) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
			// A collector is already registered under this name but is a
			// different concrete type than T — a genuine name/type collision.
			// Returning the fresh collector below means its updates are never
			// scraped, so log it loudly rather than letting it fall through as
			// a generic "registration issue".
			logger.Warn("metric name/type collision: existing collector has a different type, updates will not be scraped",
				slog.String("error", err.Error()))
			return c
		}
		logger.Debug("metric registration issue", slog.String("error", err.Error()))
	}
	return c
}
