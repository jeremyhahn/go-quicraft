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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// mockProposeCall records a single SyncPropose invocation.
type mockProposeCall struct {
	shardID uint64
	data    []byte
}

// mockProposer is a test double that records all SyncPropose calls and
// optionally returns a configurable error.
type mockProposer struct {
	mu    sync.Mutex
	calls []mockProposeCall
	err   error
}

func (m *mockProposer) SyncPropose(_ context.Context, shardID uint64, cmd []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dataCopy := make([]byte, len(cmd))
	copy(dataCopy, cmd)
	m.calls = append(m.calls, mockProposeCall{shardID: shardID, data: dataCopy})
	return m.err
}

func (m *mockProposer) getCalls() []mockProposeCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]mockProposeCall, len(m.calls))
	copy(cp, m.calls)
	return cp
}

// newTestRegistry returns a fresh Prometheus registry for test isolation.
func newTestRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	return prometheus.NewRegistry()
}

// newTestAggregator creates an aggregator with short flush intervals, a small
// batch size, and an isolated Prometheus registry. Returns the aggregator and
// the mock proposer for assertions.
func newTestAggregator(t *testing.T, opts ...Option) (*Aggregator, *mockProposer) {
	t.Helper()
	mp := &mockProposer{}
	defaultOpts := []Option{
		WithConfig(Config{
			MaxBatchSize:  10,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	}
	allOpts := append(defaultOpts, opts...)
	agg, err := NewAggregator(mp, allOpts...)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	return agg, mp
}

func TestNewAggregator_NilProposer(t *testing.T) {
	t.Parallel()
	_, err := NewAggregator(nil)
	if err == nil {
		t.Fatal("NewAggregator(nil) should return error")
	}

	var npe *NilProposerError
	if !errors.As(err, &npe) {
		t.Fatalf("error should be *NilProposerError, got %T", err)
	}
	if !errors.Is(err, ErrNilProposer) {
		t.Error("error should match ErrNilProposer sentinel")
	}
}

func TestNewAggregator_NilProposerReturnsNilAggregator(t *testing.T) {
	t.Parallel()
	agg, err := NewAggregator(nil)
	if err == nil {
		t.Fatal("expected error for nil proposer")
	}
	if agg != nil {
		t.Error("aggregator should be nil when proposer is nil")
	}
}

func TestNewAggregator_Defaults(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	agg, err := NewAggregator(mp, WithRegistry(newTestRegistry(t)))
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	if agg.cfg.MaxBatchSize != DefaultMaxBatchSize {
		t.Errorf("MaxBatchSize = %d, want %d", agg.cfg.MaxBatchSize, DefaultMaxBatchSize)
	}
	if agg.cfg.FlushInterval != DefaultFlushInterval {
		t.Errorf("FlushInterval = %v, want %v", agg.cfg.FlushInterval, DefaultFlushInterval)
	}
	if agg.cfg.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d, want %d", agg.cfg.QueueSize, DefaultQueueSize)
	}
}

func TestNewAggregator_WithCustomConfig(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	cfg := Config{
		MaxBatchSize:  50,
		FlushInterval: 100 * time.Millisecond,
		QueueSize:     500,
	}
	agg, err := NewAggregator(mp,
		WithConfig(cfg),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	if agg.cfg.MaxBatchSize != 50 {
		t.Errorf("MaxBatchSize = %d, want 50", agg.cfg.MaxBatchSize)
	}
	if agg.cfg.FlushInterval != 100*time.Millisecond {
		t.Errorf("FlushInterval = %v, want 100ms", agg.cfg.FlushInterval)
	}
	if agg.cfg.QueueSize != 500 {
		t.Errorf("QueueSize = %d, want 500", agg.cfg.QueueSize)
	}
}

func TestNewAggregator_WithConfigZeroFieldsUsesDefaults(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	agg, err := NewAggregator(mp,
		WithConfig(Config{}),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	if agg.cfg.MaxBatchSize != DefaultMaxBatchSize {
		t.Errorf("MaxBatchSize = %d, want %d (default)", agg.cfg.MaxBatchSize, DefaultMaxBatchSize)
	}
	if agg.cfg.FlushInterval != DefaultFlushInterval {
		t.Errorf("FlushInterval = %v, want %v (default)", agg.cfg.FlushInterval, DefaultFlushInterval)
	}
	if agg.cfg.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d, want %d (default)", agg.cfg.QueueSize, DefaultQueueSize)
	}
}

func TestNewAggregator_WithLogger(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	logger := slog.Default()
	agg, err := NewAggregator(mp,
		WithLogger(logger),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	if agg.logger != logger {
		t.Error("logger was not set correctly")
	}
}

func TestNewAggregator_WithNilLoggerUsesDefault(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	agg, err := NewAggregator(mp,
		WithLogger(nil),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	if agg.logger == nil {
		t.Error("logger should not be nil when nil is passed to WithLogger")
	}
}

func TestNewAggregator_WithRegistry(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	reg := prometheus.NewRegistry()
	agg, err := NewAggregator(mp, WithRegistry(reg))
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	if agg.registry != reg {
		t.Error("registry was not set correctly")
	}
}

// TestNewAggregator_WithNilRegistryUsesDefault pins WithRegistry's nil guard:
// passing nil must LEAVE the existing registerer in place, because a nil
// prometheus.Registerer is not a usable default — initMetrics would panic on
// it, and the constructor has already installed prometheus.DefaultRegisterer.
//
// The option is exercised directly rather than through NewAggregator. The
// previous shape passed WithRegistry(nil) followed by WithRegistry(a real one)
// and then asserted only that the field was non-nil — which the SECOND call
// guarantees regardless of what the first did. Deleting the nil guard left the
// test passing, so the guard it exists to protect was unverified. Constructing
// through NewAggregator with nil alone is not an option here either: the
// aggregator would then register its collectors on the global default
// registry, which is shared with every other test in the binary.
func TestNewAggregator_WithNilRegistryUsesDefault(t *testing.T) {
	t.Parallel()

	existing := newTestRegistry(t)
	a := &Aggregator{registry: existing}
	WithRegistry(nil)(a)

	if a.registry != prometheus.Registerer(existing) {
		t.Errorf("WithRegistry(nil) replaced the registerer with %v; nil must be "+
			"ignored so the constructor's default survives", a.registry)
	}

	replacement := newTestRegistry(t)
	WithRegistry(replacement)(a)
	if a.registry != prometheus.Registerer(replacement) {
		t.Error("WithRegistry(non-nil) did not install the supplied registerer")
	}
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()

	if cfg.MaxBatchSize != DefaultMaxBatchSize {
		t.Errorf("MaxBatchSize = %d, want %d", cfg.MaxBatchSize, DefaultMaxBatchSize)
	}
	if cfg.FlushInterval != DefaultFlushInterval {
		t.Errorf("FlushInterval = %v, want %v", cfg.FlushInterval, DefaultFlushInterval)
	}
	if cfg.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d, want %d", cfg.QueueSize, DefaultQueueSize)
	}
}

func TestDefaultConfig_NonZero(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()

	if cfg.MaxBatchSize <= 0 {
		t.Errorf("MaxBatchSize = %d, should be positive", cfg.MaxBatchSize)
	}
	if cfg.FlushInterval <= 0 {
		t.Errorf("FlushInterval = %v, should be positive", cfg.FlushInterval)
	}
	if cfg.QueueSize <= 0 {
		t.Errorf("QueueSize = %d, should be positive", cfg.QueueSize)
	}
}

func TestSubmit_Success(t *testing.T) {
	t.Parallel()
	agg, mp := newTestAggregator(t)
	defer agg.Close()

	ctx := context.Background()
	err := agg.Submit(ctx, 1, []byte("test-data"))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Verify proposer was called.
	calls := mp.getCalls()
	if len(calls) == 0 {
		t.Fatal("proposer was not called")
	}

	// The data should be encoded in a batch format. Decode it and verify.
	found := false
	for _, call := range calls {
		if call.shardID != 1 {
			continue
		}
		decoded, decErr := DecodeBatch(call.data)
		if decErr != nil {
			t.Fatalf("DecodeBatch failed: %v", decErr)
		}
		for _, cmd := range decoded {
			if bytes.Equal(cmd, []byte("test-data")) {
				found = true
				break
			}
		}
	}
	if !found {
		t.Error("submitted data not found in proposer calls")
	}
}

func TestSubmit_MultipleItems(t *testing.T) {
	t.Parallel()
	agg, mp := newTestAggregator(t)
	defer agg.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	itemCount := 5

	errs := make([]error, itemCount)
	wg.Add(itemCount)
	for i := range itemCount {
		go func(idx int) {
			defer wg.Done()
			data := []byte{byte(idx)}
			errs[idx] = agg.Submit(ctx, 1, data)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Submit[%d] failed: %v", i, err)
		}
	}

	calls := mp.getCalls()
	if len(calls) == 0 {
		t.Fatal("proposer was never called")
	}
}

func TestSubmit_AfterClose(t *testing.T) {
	t.Parallel()
	agg, _ := newTestAggregator(t)

	if err := agg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err := agg.Submit(context.Background(), 1, []byte("data"))
	if err == nil {
		t.Fatal("Submit after Close should return error")
	}

	var ce *ClosedError
	if !errors.As(err, &ce) {
		t.Fatalf("error should be *ClosedError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrClosed) {
		t.Error("error should match ErrClosed sentinel")
	}
}

func TestSubmit_AfterCloseReturnsSentinel(t *testing.T) {
	t.Parallel()
	agg, _ := newTestAggregator(t)
	agg.Close()

	err := agg.Submit(context.Background(), 1, []byte("data"))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Submit after Close should return ErrClosed, got %v", err)
	}
}

func TestSubmit_CanceledContext(t *testing.T) {
	t.Parallel()

	// Create a proposer that blocks until signaled so the submit waits
	// long enough for the context cancellation to take effect.
	blockingProposer := &blockingMockProposer{
		unblock: make(chan struct{}),
	}

	reg := newTestRegistry(t)
	agg, err := NewAggregator(blockingProposer,
		WithConfig(Config{
			MaxBatchSize:  1,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer func() {
		close(blockingProposer.unblock)
		agg.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	submitErr := agg.Submit(ctx, 1, []byte("data"))
	if submitErr == nil {
		t.Fatal("Submit with canceled context should return error")
	}

	// The error should be context.Canceled or ClosedError (depending on
	// timing), but it must not be nil.
	if !errors.Is(submitErr, context.Canceled) && !errors.Is(submitErr, ErrClosed) {
		t.Errorf("unexpected error: %v", submitErr)
	}
}

// blockingMockProposer blocks SyncPropose until unblock is closed.
type blockingMockProposer struct {
	unblock chan struct{}
}

func (b *blockingMockProposer) SyncPropose(_ context.Context, _ uint64, _ []byte) error {
	<-b.unblock
	return nil
}

// enteredBlockingProposer blocks every SyncPropose until unblock is closed and
// announces the first entry, so a test can wait for the flush goroutine to be
// provably parked instead of guessing at a duration.
type enteredBlockingProposer struct {
	unblock chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (b *enteredBlockingProposer) SyncPropose(_ context.Context, _ uint64, _ []byte) error {
	b.once.Do(func() { close(b.entered) })
	<-b.unblock
	return nil
}

// TestSubmit_QueueFull pins backpressure: once the queue is at capacity, Submit
// must report QueueFullError IMMEDIATELY rather than blocking a caller behind a
// flush that is going nowhere.
//
// The previous shape of this test could not fail. It looped up to 100 times
// hoping to observe the condition and, when it never did, called t.Log — so a
// build in which Submit blocked forever instead of rejecting, or returned some
// unrelated error, passed exactly as cleanly as a correct one. There was no
// t.Error or t.Fatal reachable from any aggregator behaviour in the whole body.
//
// The condition is now constructed deterministically: MaxBatchSize 1 sends the
// first proposal straight into a flush that blocks in the proposer, which parks
// the single flush goroutine so nothing drains the queue; the next QueueSize
// submissions fill it exactly; and the one after that must be rejected.
func TestSubmit_QueueFull(t *testing.T) {
	t.Parallel()

	const queueSize = 2

	blocking := &enteredBlockingProposer{
		unblock: make(chan struct{}),
		entered: make(chan struct{}),
	}

	agg, err := NewAggregator(blocking,
		WithConfig(Config{
			// One proposal per flush, so the very first submission parks the
			// flush goroutine inside the proposer.
			MaxBatchSize: 1,
			// Long enough that the timer plays no part.
			FlushInterval:  time.Hour,
			QueueSize:      queueSize,
			ProposeTimeout: time.Hour,
		}),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer func() {
		close(blocking.unblock)
		_ = agg.Close()
	}()

	// The first proposal reaches the proposer and parks the flush goroutine.
	go func() { _ = agg.Submit(context.Background(), 1, []byte("parks-the-loop")) }()
	select {
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first proposal never reached the proposer")
	}

	// Fill the queue exactly. Nothing drains it while the flush goroutine is
	// parked, so each of these stays enqueued and its caller blocks.
	for i := range queueSize {
		go func() { _ = agg.Submit(context.Background(), 1, []byte{byte(i)}) }()
	}
	waitForCondition(t, 10*time.Second, "the queue never reached capacity", func() bool {
		return len(agg.queue) == queueSize
	})

	// One more must be rejected outright, not blocked and not silently accepted.
	done := make(chan error, 1)
	go func() { done <- agg.Submit(context.Background(), 1, []byte("overflow")) }()
	select {
	case submitErr := <-done:
		var qfe *QueueFullError
		if !errors.As(submitErr, &qfe) {
			t.Fatalf("Submit into a full queue = %T (%v), want *QueueFullError",
				submitErr, submitErr)
		}
		if qfe.QueueSize != queueSize {
			t.Errorf("QueueFullError.QueueSize = %d, want %d", qfe.QueueSize, queueSize)
		}
		if !errors.Is(submitErr, ErrQueueFull) {
			t.Error("error should match the ErrQueueFull sentinel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Submit blocked on a full queue instead of reporting QueueFullError")
	}
}

func TestSubmit_SizeBasedFlush(t *testing.T) {
	t.Parallel()
	const batchSize = 3

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  batchSize,
			FlushInterval: 10 * time.Second, // Long interval to ensure size trigger.
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(batchSize)
	for i := range batchSize {
		go func(idx int) {
			defer wg.Done()
			agg.Submit(ctx, 1, []byte{byte(idx)})
		}(i)
	}
	wg.Wait()

	calls := mp.getCalls()
	if len(calls) == 0 {
		t.Fatal("size-based flush did not trigger proposer call")
	}

	// Verify all items were delivered.
	totalDecoded := 0
	for _, call := range calls {
		decoded, decErr := DecodeBatch(call.data)
		if decErr != nil {
			t.Fatalf("DecodeBatch failed: %v", decErr)
		}
		totalDecoded += len(decoded)
	}
	if totalDecoded < batchSize {
		t.Errorf("decoded %d items from proposer, want at least %d", totalDecoded, batchSize)
	}
}

// TestSubmit_TimeBasedFlush pins that a batch below MaxBatchSize is flushed by
// the interval timer, and that the flush is ATTRIBUTED to the timer.
//
// The previous assertion — proposer calls are non-empty — could not fail:
// Submit returns nil only once a flush has delivered that proposal's result,
// and a flush that delivered a result has by definition called the proposer.
// The interesting half, that the TIMER is what triggered it, went unchecked, so
// the flushReasonTime label had no coverage at all and a flush misreported as
// "size" or "close" would have gone unnoticed by every test in the package.
func TestSubmit_TimeBasedFlush(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			// Neither the size threshold nor Close may fire: the batch holds
			// one proposal, and the assertions run before Close.
			MaxBatchSize:  1000,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	ctx := context.Background()
	submitErr := agg.Submit(ctx, 1, []byte("timer-flush"))
	if submitErr != nil {
		t.Fatalf("Submit failed: %v", submitErr)
	}

	if got := flushReasonCount(t, reg, flushReasonTime); got < 1 {
		t.Errorf("flush_reason_total{reason=%q} = %v, want at least 1: the batch "+
			"was flushed, but not by the interval timer", flushReasonTime, got)
	}
	for _, other := range []string{flushReasonSize, flushReasonBytes, flushReasonClose} {
		if got := flushReasonCount(t, reg, other); got != 0 {
			t.Errorf("flush_reason_total{reason=%q} = %v, want 0: only the timer "+
				"can flush a one-proposal batch under this configuration", other, got)
		}
	}
}

// flushReasonCount reads the quicraft_batch_flush_reason_total counter for one
// reason label out of reg, returning 0 when the label has not been observed.
func flushReasonCount(t *testing.T, reg *prometheus.Registry, reason string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "quicraft_batch_flush_reason_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" && lp.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestSubmit_ProposerError(t *testing.T) {
	t.Parallel()

	proposerErr := errors.New("raft unavailable")
	mp := &mockProposer{err: proposerErr}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	submitErr := agg.Submit(context.Background(), 1, []byte("will-fail"))
	if submitErr == nil {
		t.Fatal("Submit should propagate proposer error")
	}

	var fe *FlushError
	if !errors.As(submitErr, &fe) {
		t.Fatalf("error should be *FlushError, got %T: %v", submitErr, submitErr)
	}
	if fe.ShardID != 1 {
		t.Errorf("FlushError.ShardID = %d, want 1", fe.ShardID)
	}
	if !errors.Is(submitErr, ErrFlush) {
		t.Error("error should match ErrFlush sentinel")
	}
	// The original proposer error should be unwrappable.
	if !errors.Is(submitErr, proposerErr) {
		t.Error("error should wrap the original proposer error")
	}
}

func TestSubmit_ProposerErrorMessage(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{err: errors.New("connection reset")}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	submitErr := agg.Submit(context.Background(), 42, []byte("data"))
	if submitErr == nil {
		t.Fatal("Submit should return error when proposer fails")
	}

	var fe *FlushError
	if !errors.As(submitErr, &fe) {
		t.Fatalf("error should be *FlushError, got %T: %v", submitErr, submitErr)
	}
	if fe.ShardID != 42 {
		t.Errorf("ShardID = %d, want 42", fe.ShardID)
	}
}

func TestSubmit_MultipleShards(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  10,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	shards := []uint64{1, 2, 3}

	wg.Add(len(shards))
	for _, shardID := range shards {
		go func(sid uint64) {
			defer wg.Done()
			agg.Submit(ctx, sid, []byte{byte(sid)})
		}(shardID)
	}
	wg.Wait()

	calls := mp.getCalls()
	if len(calls) == 0 {
		t.Fatal("proposer was never called for multi-shard submissions")
	}

	// Every submitted shard must have been proposed, and its own command must
	// be in its own batch. The previous assertion here was `len(seenShards) ==
	// 0`, which the `len(calls) == 0` check above already rules out — every
	// call carries a shard ID, so a non-empty call list can never produce an
	// empty shard set. It could not fail, and in particular it could not catch
	// a flush that merged or dropped a shard's proposals.
	seen := make(map[uint64][]byte)
	for _, call := range calls {
		decoded, decErr := DecodeBatch(call.data)
		if decErr != nil {
			t.Fatalf("DecodeBatch for shard %d: %v", call.shardID, decErr)
		}
		for _, cmd := range decoded {
			seen[call.shardID] = append(seen[call.shardID], cmd...)
		}
	}
	for _, shardID := range shards {
		got, ok := seen[shardID]
		if !ok {
			t.Errorf("shard %d was submitted but never proposed", shardID)
			continue
		}
		if want := []byte{byte(shardID)}; !bytes.Equal(got, want) {
			t.Errorf("shard %d was proposed %q, want %q: a shard's batch must "+
				"carry that shard's commands and no others", shardID, got, want)
		}
	}
	for shardID := range seen {
		if !slices.Contains(shards, shardID) {
			t.Errorf("the proposer saw shard %d, which was never submitted", shardID)
		}
	}
}

func TestClose_FlushesRemaining(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1000,             // Large batch so items stay pending.
			FlushInterval: 10 * time.Second, // Long interval so timer does not fire.
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}

	// Submit in a goroutine (Submit blocks until flush completes).
	submitted := make(chan error, 1)
	go func() {
		submitted <- agg.Submit(context.Background(), 1, []byte("pending-on-close"))
	}()

	// Poll until the flush loop has dequeued the item into its
	// internal batch. We check BatchLen which exposes the current
	// batch size under the mutex.
	pollDeadline := time.NewTimer(5 * time.Second)
	defer pollDeadline.Stop()
	pollTicker := time.NewTicker(time.Millisecond)
	defer pollTicker.Stop()
	for agg.BatchLen() == 0 {
		select {
		case <-pollTicker.C:
		case <-pollDeadline.C:
			t.Fatal("timed out waiting for item to enter batch")
		}
	}

	// Close should flush the pending batch.
	if closeErr := agg.Close(); closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	// Wait for the submit goroutine to complete.
	<-submitted

	calls := mp.getCalls()
	if len(calls) == 0 {
		t.Fatal("Close did not flush remaining items")
	}
}

// countingProposer records how many distinct commands it received across
// all batches and always succeeds. It is used to confirm that a proposal
// reported as failed (ClosedError) was not in fact flushed to Raft.
type countingProposer struct {
	mu       sync.Mutex
	commands int
}

func (c *countingProposer) SyncPropose(_ context.Context, _ uint64, cmd []byte) error {
	decoded, err := DecodeBatch(cmd)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.commands += len(decoded)
	c.mu.Unlock()
	return nil
}

func (c *countingProposer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commands
}

// TestSubmit_CloseRaceReportsRealResult exercises the shutdown race where
// Close() fires while a caller is blocked waiting for its proposal's result.
// The proposal has already been enqueued, so the flush loop's close branch
// (drainQueue then flush) still proposes it and sends the genuine result on
// resultCh. The caller must observe that result (nil here), never a spurious
// ClosedError that would falsely report a write the cluster actually applied.
func TestSubmit_CloseRaceReportsRealResult(t *testing.T) {
	t.Parallel()

	// Many iterations to reliably interleave Submit's second select with
	// Close closing closeChan.
	const iterations = 200
	for i := 0; i < iterations; i++ {
		cp := &countingProposer{}
		reg := newTestRegistry(t)
		agg, err := NewAggregator(cp,
			WithConfig(Config{
				// Large batch and long timer so neither size nor timer
				// flushes the proposal: only the close path will flush it,
				// maximizing the closeChan race window in Submit.
				MaxBatchSize:  1000,
				FlushInterval: 10 * time.Second,
				QueueSize:     100,
			}),
			WithRegistry(reg),
		)
		if err != nil {
			t.Fatalf("NewAggregator failed: %v", err)
		}

		resultCh := make(chan error, 1)
		go func() {
			resultCh <- agg.Submit(context.Background(), 1, []byte("close-race"))
		}()

		// Yield so the proposal is enqueued and the goroutine is parked in
		// Submit's second select before we close.
		runtime.Gosched()

		if closeErr := agg.Close(); closeErr != nil {
			t.Fatalf("Close failed: %v", closeErr)
		}

		submitErr := <-resultCh

		var ce *ClosedError
		if errors.As(submitErr, &ce) {
			// A ClosedError is only acceptable if the proposal was never
			// flushed (e.g. Close won before the proposal was enqueued).
			if cp.count() != 0 {
				t.Fatalf("iter %d: Submit returned ClosedError but proposal was flushed (proposer saw %d commands)", i, cp.count())
			}
			continue
		}

		// Any non-ClosedError result means the proposal was processed: it
		// must have succeeded and reached the proposer exactly once.
		if submitErr != nil {
			t.Fatalf("iter %d: unexpected Submit error: %v", i, submitErr)
		}
		if cp.count() != 1 {
			t.Fatalf("iter %d: Submit returned success but proposer saw %d commands, want 1", i, cp.count())
		}
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	agg, _ := newTestAggregator(t)

	for i := range 5 {
		if err := agg.Close(); err != nil {
			t.Errorf("Close() call %d returned error: %v", i, err)
		}
	}
}

func TestClose_IdempotentNoError(t *testing.T) {
	t.Parallel()
	agg, _ := newTestAggregator(t)

	err1 := agg.Close()
	err2 := agg.Close()

	if err1 != nil {
		t.Errorf("first Close returned error: %v", err1)
	}
	if err2 != nil {
		t.Errorf("second Close returned error: %v", err2)
	}
}

func TestConcurrentSubmit(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  5,
			FlushInterval: 2 * time.Millisecond,
			QueueSize:     1000,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	const goroutines = 20
	const submitsPerGoroutine = 10
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, goroutines*submitsPerGoroutine)

	for g := range goroutines {
		go func(gIdx int) {
			defer wg.Done()
			for s := range submitsPerGoroutine {
				data := []byte{byte(gIdx), byte(s)}
				if submitErr := agg.Submit(ctx, uint64(gIdx%3+1), data); submitErr != nil {
					errCh <- submitErr
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	for submitErr := range errCh {
		t.Errorf("concurrent Submit failed: %v", submitErr)
	}

	calls := mp.getCalls()
	totalDecoded := 0
	for _, call := range calls {
		decoded, decErr := DecodeBatch(call.data)
		if decErr != nil {
			t.Fatalf("DecodeBatch failed: %v", decErr)
		}
		totalDecoded += len(decoded)
	}

	expected := goroutines * submitsPerGoroutine
	if totalDecoded != expected {
		t.Errorf("total decoded items = %d, want %d", totalDecoded, expected)
	}
}

// TestConcurrentSubmitAndClose races Submit against Close and pins the property
// that makes the race safe: a Submit that reports SUCCESS must have had its
// command proposed, and a Submit that reports failure must not have.
//
// It previously ended at "the test passes if there are no panics or data
// races" — every Submit's return value was discarded, so nothing but the race
// detector could fail it. A build that reported success for a command it never
// proposed (or ClosedError for one it did) passed silently, and that is exactly
// the class of bug this shutdown race produces: the caller of a batched write
// has no other signal, and the aggregator has no at-most-once path to repair a
// wrong answer with.
func TestConcurrentSubmitAndClose(t *testing.T) {
	t.Parallel()

	cp := &countingProposer{}
	agg, err := NewAggregator(cp,
		WithConfig(Config{
			MaxBatchSize:  5,
			FlushInterval: 2 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}

	const goroutines = 10
	ctx := context.Background()

	results := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			// Errors are expected: Close races with Submit.
			results[g] = agg.Submit(ctx, 1, []byte("race"))
		}()
	}

	// Yield to give goroutines a chance to submit before closing.
	runtime.Gosched()
	if closeErr := agg.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	wg.Wait()

	// Close has returned, so the flush loop has exited and nothing further can
	// be proposed: the proposer's count is final and comparable.
	succeeded := 0
	for g, submitErr := range results {
		switch {
		case submitErr == nil:
			succeeded++
		case errors.Is(submitErr, ErrClosed), errors.Is(submitErr, ErrFlush):
			// The two legitimate refusals: the aggregator was already closed,
			// or the batch failed.
		default:
			t.Errorf("goroutine %d: Submit = %T (%v); a submission racing Close "+
				"must report nil, ErrClosed or ErrFlush and nothing else",
				g, submitErr, submitErr)
		}
	}

	if got := cp.count(); got != succeeded {
		t.Errorf("%d Submit calls reported success but the proposer saw %d commands: "+
			"a batched write was acknowledged without being proposed, or a proposed "+
			"one was reported as failed", succeeded, got)
	}
}

func TestSubmit_DataIntegrity(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1,
			FlushInterval: 5 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	original := []byte("important-raft-command-payload")
	submitErr := agg.Submit(context.Background(), 7, original)
	if submitErr != nil {
		t.Fatalf("Submit failed: %v", submitErr)
	}

	calls := mp.getCalls()
	found := false
	for _, call := range calls {
		if call.shardID != 7 {
			continue
		}
		decoded, decErr := DecodeBatch(call.data)
		if decErr != nil {
			t.Fatalf("DecodeBatch failed: %v", decErr)
		}
		for _, cmd := range decoded {
			if bytes.Equal(cmd, original) {
				found = true
				break
			}
		}
	}
	if !found {
		t.Error("original data not found in proposer calls after submit")
	}
}

func TestProposeContext_AllCanceled(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1000,
			FlushInterval: 10 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel1()
	cancel2()

	proposals := []*proposal{
		{ctx: ctx1, shardID: 1, data: []byte("a"), resultCh: make(chan error, 1)},
		{ctx: ctx2, shardID: 1, data: []byte("b"), resultCh: make(chan error, 1)},
	}

	// Every caller has already given up, so there is nobody left to receive a
	// result. The batch must be skipped rather than proposed on an unbounded
	// context, which is what wedged the single flush goroutine.
	ctx, cancel, ok := agg.proposeContext(proposals)
	if ok {
		cancel()
		t.Fatal("proposeContext with all contexts canceled should report ok=false")
	}
	if ctx != nil || cancel != nil {
		t.Error("proposeContext must return a nil context and cancel when ok=false")
	}
}

func TestProposeContext_FirstNonCanceled(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1000,
			FlushInterval: 10 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	liveCtx := context.Background()

	proposals := []*proposal{
		{ctx: canceledCtx, shardID: 1, data: []byte("a"), resultCh: make(chan error, 1)},
		{ctx: liveCtx, shardID: 1, data: []byte("b"), resultCh: make(chan error, 1)},
	}

	ctx, cancel, ok := agg.proposeContext(proposals)
	if !ok {
		t.Fatal("proposeContext should select the first non-canceled context")
	}
	defer cancel()
	if ctx.Err() != nil {
		t.Error("proposeContext should return a live context")
	}
	// liveCtx carries no deadline of its own, so the aggregator must supply
	// one: an unbounded propose on the single flush goroutine stalls every
	// other shard and hangs Close.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		t.Error("proposeContext must bound a deadline-less caller context")
	}
}

func TestConstants(t *testing.T) {
	t.Parallel()

	if DefaultMaxBatchSize != 1000 {
		t.Errorf("DefaultMaxBatchSize = %d, want 1000", DefaultMaxBatchSize)
	}
	if DefaultFlushInterval != 50*time.Millisecond {
		t.Errorf("DefaultFlushInterval = %v, want 50ms", DefaultFlushInterval)
	}
	if DefaultQueueSize != 10000 {
		t.Errorf("DefaultQueueSize = %d, want 10000", DefaultQueueSize)
	}
}

func TestFlushReasonConstants(t *testing.T) {
	t.Parallel()

	if flushReasonSize != "size" {
		t.Errorf("flushReasonSize = %q, want %q", flushReasonSize, "size")
	}
	if flushReasonTime != "time" {
		t.Errorf("flushReasonTime = %q, want %q", flushReasonTime, "time")
	}
	if flushReasonClose != "close" {
		t.Errorf("flushReasonClose = %q, want %q", flushReasonClose, "close")
	}
}

// TestMetrics_HistogramsHaveNoShardLabel is the regression test for the
// metrics cardinality policy: with 10K+ shards a per-shard label on the
// 11/14-bucket histograms would explode into hundreds of thousands of time
// series. The histograms must be unlabeled aggregates; only the
// low-cardinality batchesTotal counter keeps the shard label.
func TestMetrics_HistogramsHaveNoShardLabel(t *testing.T) {
	mp := &mockProposer{}
	reg := prometheus.NewRegistry()
	agg, err := NewAggregator(mp, WithRegistry(reg))
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer agg.Close()

	if err := agg.Submit(context.Background(), 42, []byte("cmd")); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	found := map[string]bool{}
	for _, mf := range families {
		name := mf.GetName()
		switch name {
		case "quicraft_batch_batch_size", "quicraft_batch_flush_duration_seconds":
			found[name] = true
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "shard" {
						t.Errorf("%s carries a per-shard label; histograms must be aggregate-only", name)
					}
				}
			}
		case "quicraft_batch_batches_total":
			found[name] = true
			hasShard := false
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "shard" {
						hasShard = true
					}
				}
			}
			if !hasShard {
				t.Error("quicraft_batch_batches_total lost its shard label")
			}
		}
	}
	for _, name := range []string{
		"quicraft_batch_batch_size",
		"quicraft_batch_flush_duration_seconds",
		"quicraft_batch_batches_total",
	} {
		if !found[name] {
			t.Errorf("metric %s not found in registry", name)
		}
	}
}
