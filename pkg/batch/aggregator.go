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

	// DefaultProposeTimeout is the ceiling on one batch's SyncPropose; finite so the single flush goroutine cannot wedge.
	DefaultProposeTimeout = 30 * time.Second

	// DefaultMaxBatchBytes is the maximum encoded batch size: proto.MaxEntrySize, since a batch is proposed as one Raft entry.
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

	// MaxBatchBytes caps the encoded size of the whole accumulated batch; a proposal that would exceed it flushes the batch first.
	MaxBatchBytes uint64

	// FlushInterval is the maximum duration between batch flushes.
	FlushInterval time.Duration

	// QueueSize is the capacity of the proposal channel.
	QueueSize int

	// ProposeTimeout caps a batch's SyncPropose at min(latest live caller deadline, ProposeTimeout).
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

// aggregatorMetrics holds the aggregator's metrics; only batchesTotal carries a shard label, to bound cardinality.
type aggregatorMetrics struct {
	batchesTotal     *prometheus.CounterVec
	batchSize        prometheus.Histogram
	flushDuration    prometheus.Histogram
	queueDepth       prometheus.Gauge
	flushReasonTotal *prometheus.CounterVec
}

// Aggregator batches proposals per shard and flushes them on an interval or size threshold; delivery is at-least-once and Submit is safe for concurrent use.
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
	// lifetimeCtx is the parent of every propose context, so no caller's context governs a shared batch.
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc
	// flushDone is closed when flushLoop returns, so a Submit on the close path never waits on resultCh forever.
	flushDone chan struct{}

	// Metrics.
	metrics     *aggregatorMetrics
	metricsOnce sync.Once
}

// NewAggregator starts a batch aggregator flushing through proposer; it returns a NilProposerError if proposer is nil.
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

// Submit copies data, enqueues it and blocks until its batch flushes or ctx is done; an indeterminate FlushError may still have applied.
func (a *Aggregator) Submit(ctx context.Context, shardID uint64, data []byte) error {
	// append to a nil slice rather than make+copy: it yields nil for an empty
	// command (which encodes identically) and one allocation otherwise.
	return a.submit(ctx, shardID, append([]byte(nil), data...))
}

// SubmitNoCopy is Submit without the copy: the caller must never touch data again, on any return path.
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
		// Prefer a result that arrived with the context expiry: the batch may already be committed.
		select {
		case err := <-p.resultCh:
			return err
		default:
			// The proposal is already enqueued, so a caller-side timeout is indeterminate and must be reported as such.
			return &FlushError{ShardID: p.shardID, Err: ctx.Err(), Indeterminate: true}
		}
	case <-a.closeChan:
		// Close raced with this submission: wait for the real result, bounded by flushDone, then report closed.
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
	// Cancel after wg.Wait so the final flush's propose is not cancelled.
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

// addToBatch flushes before appending when p would exceed MaxBatchBytes, so an oversized proposal is proposed alone.
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

// drainQueue moves the remaining queued proposals into the batch through addToBatch during shutdown.
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

	// Flush shards concurrently so one slow or leaderless shard does not block the others.
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

	// Drop proposals whose callers gave up before encoding, so a definitely rejected command is never replicated.
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
		// A timeout or cancellation may still commit, so the outcome is marked indeterminate.
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

// rejectAbandoned returns the proposals still waiting and fails each abandoned one with its own context error.
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

// proposeContext returns a child of the aggregator's lifetime context with deadline min(latest live caller deadline, now+ProposeTimeout), and false when every caller is gone.
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

		// Adopt an already-registered collector so updates go to the scraped instance.
		m.batchesTotal = registerOrExisting(a.registry, m.batchesTotal, a.logger)
		m.batchSize = registerOrExisting(a.registry, m.batchSize, a.logger)
		m.flushDuration = registerOrExisting(a.registry, m.flushDuration, a.logger)
		m.queueDepth = registerOrExisting(a.registry, m.queueDepth, a.logger)
		m.flushReasonTotal = registerOrExisting(a.registry, m.flushReasonTotal, a.logger)

		a.metrics = m
	})
}

// registerOrExisting registers c with reg, returning the existing collector on AlreadyRegisteredError.
func registerOrExisting[T prometheus.Collector](reg prometheus.Registerer, c T, logger *slog.Logger) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
			// Same name, different collector type: log it, since the fresh collector's updates are never scraped.
			logger.Warn("metric name/type collision: existing collector has a different type, updates will not be scraped",
				slog.String("error", err.Error()))
			return c
		}
		logger.Debug("metric registration issue", slog.String("error", err.Error()))
	}
	return c
}
