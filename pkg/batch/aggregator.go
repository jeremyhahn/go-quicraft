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
)

// Flush reason labels for the flushReasonTotal metric.
const (
	flushReasonSize  = "size"
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

	// FlushInterval is the maximum duration between batch flushes.
	FlushInterval time.Duration

	// QueueSize is the capacity of the proposal channel.
	QueueSize int
}

// DefaultConfig returns a Config with production-ready default values.
func DefaultConfig() Config {
	return Config{
		MaxBatchSize:  DefaultMaxBatchSize,
		FlushInterval: DefaultFlushInterval,
		QueueSize:     DefaultQueueSize,
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
		if cfg.FlushInterval > 0 {
			a.cfg.FlushInterval = cfg.FlushInterval
		}
		if cfg.QueueSize > 0 {
			a.cfg.QueueSize = cfg.QueueSize
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
type aggregatorMetrics struct {
	batchesTotal     *prometheus.CounterVec
	batchSize        *prometheus.HistogramVec
	flushDuration    *prometheus.HistogramVec
	queueDepth       prometheus.Gauge
	flushReasonTotal *prometheus.CounterVec
}

// Aggregator batches multiple write operations and flushes them
// periodically or when a size threshold is reached. This amortizes
// the per-proposal Raft consensus overhead across many operations,
// significantly improving write throughput.
//
// Thread safety: Submit may be called concurrently from multiple
// goroutines. The aggregator manages its own background flush loop.
type Aggregator struct {
	cfg      Config
	proposer Proposer
	logger   *slog.Logger
	registry prometheus.Registerer

	// Batching state - protected by mu.
	mu           sync.Mutex
	currentBatch []*proposal

	// Queue for incoming proposals.
	queue chan *proposal

	// Lifecycle management.
	closed    atomic.Bool
	closeChan chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
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
// Returns:
//   - ClosedError if the aggregator has been closed
//   - QueueFullError if the queue is at capacity
//   - context.Canceled or context.DeadlineExceeded if ctx is done
//   - FlushError if the batch flush fails
func (a *Aggregator) Submit(ctx context.Context, shardID uint64, data []byte) error {
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

	select {
	case err := <-p.resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
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
			return ctx.Err()
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

			if len(a.currentBatch) >= a.cfg.MaxBatchSize {
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

// addToBatch appends a proposal to the current batch under the lock.
func (a *Aggregator) addToBatch(p *proposal) {
	a.mu.Lock()
	a.currentBatch = append(a.currentBatch, p)
	a.mu.Unlock()
}

// drainQueue reads all remaining proposals from the queue channel and
// adds them to the current batch. Called during shutdown to ensure no
// proposals are lost.
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

	for shardID, shardProposals := range shardBatches {
		a.flushShard(shardID, shardProposals)
	}
}

// flushShard encodes and submits a batch of proposals for a single shard,
// then distributes the result to all waiting callers.
func (a *Aggregator) flushShard(shardID uint64, proposals []*proposal) {
	shardLabel := strconv.FormatUint(shardID, 10)
	batchLen := len(proposals)

	a.metrics.batchSize.WithLabelValues(shardLabel).Observe(float64(batchLen))
	a.metrics.batchesTotal.WithLabelValues(shardLabel, "attempted").Inc()

	// Encode all proposal data as a length-prefixed batch.
	cmds := make([][]byte, batchLen)
	for i, p := range proposals {
		cmds[i] = p.data
	}
	encoded := EncodeBatch(cmds)

	// Use the first non-canceled context for the propose call. If all
	// contexts are already canceled, use a background context to avoid
	// losing the batch entirely. The proposer will return quickly if
	// the Raft layer rejects the stale proposal.
	ctx := a.pickContext(proposals)

	start := time.Now()
	err := a.proposer.SyncPropose(ctx, shardID, encoded)
	elapsed := time.Since(start)

	a.metrics.flushDuration.WithLabelValues(shardLabel).Observe(elapsed.Seconds())

	if err != nil {
		a.metrics.batchesTotal.WithLabelValues(shardLabel, "failed").Inc()
		a.logger.Warn("batch flush failed",
			slog.Uint64("shard_id", shardID),
			slog.Int("batch_size", batchLen),
			slog.String("error", err.Error()),
		)
		flushErr := &FlushError{ShardID: shardID, Err: err}
		for _, p := range proposals {
			p.resultCh <- flushErr
		}
		return
	}

	a.metrics.batchesTotal.WithLabelValues(shardLabel, "success").Inc()

	for _, p := range proposals {
		p.resultCh <- nil
	}
}

// pickContext returns the first non-canceled context from the proposal
// list. If all contexts are already done, it returns context.Background()
// to ensure the batch can still be submitted.
func (a *Aggregator) pickContext(proposals []*proposal) context.Context {
	for _, p := range proposals {
		if p.ctx.Err() == nil {
			return p.ctx
		}
	}
	return context.Background()
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

			batchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "batch_size",
				Help:      "Number of proposals in each flushed batch.",
				Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
			}, []string{"shard"}),

			flushDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: "quicraft",
				Subsystem: "batch",
				Name:      "flush_duration_seconds",
				Help:      "Duration of batch flush operations (proposer SyncPropose call).",
				Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			}, []string{"shard"}),

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
