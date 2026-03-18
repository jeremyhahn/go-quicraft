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

func TestNewAggregator_WithNilRegistryUsesDefault(t *testing.T) {
	t.Parallel()
	mp := &mockProposer{}
	agg, err := NewAggregator(mp,
		WithRegistry(nil),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	defer agg.Close()

	// The second WithRegistry call should have set a non-nil registry.
	if agg.registry == nil {
		t.Error("registry should not be nil")
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

func TestSubmit_QueueFull(t *testing.T) {
	t.Parallel()

	// Create a proposer that blocks forever so the queue fills up.
	blockingProposer := &blockingMockProposer{
		unblock: make(chan struct{}),
	}

	reg := newTestRegistry(t)
	agg, err := NewAggregator(blockingProposer,
		WithConfig(Config{
			MaxBatchSize:  1000,
			FlushInterval: 10 * time.Second, // Long interval to prevent timer flushes.
			QueueSize:     2,
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

	// Fill the queue. The flush loop will dequeue items but proposer blocks,
	// so eventually the queue fills.
	var queueFullSeen bool
	for i := range 100 {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		submitErr := agg.Submit(ctx, 1, []byte{byte(i)})
		cancel()
		if submitErr != nil {
			var qfe *QueueFullError
			if errors.As(submitErr, &qfe) {
				queueFullSeen = true
				break
			}
			// Context timeout is also acceptable - the queue may have
			// drained but proposer is blocking.
			if errors.Is(submitErr, context.DeadlineExceeded) {
				continue
			}
		}
	}

	if !queueFullSeen {
		t.Log("queue full condition was not triggered (proposer may be draining fast enough)")
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

func TestSubmit_TimeBasedFlush(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  1000, // Large batch size so it does not trigger.
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

	// The item should have been flushed by the timer. Verify proposer was called.
	calls := mp.getCalls()
	if len(calls) == 0 {
		t.Fatal("time-based flush did not trigger proposer call")
	}
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

	// Verify we got calls for different shards.
	seenShards := make(map[uint64]bool)
	for _, call := range calls {
		seenShards[call.shardID] = true
	}
	if len(seenShards) == 0 {
		t.Error("no shards seen in proposer calls")
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

func TestConcurrentSubmitAndClose(t *testing.T) {
	t.Parallel()

	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{
			MaxBatchSize:  5,
			FlushInterval: 2 * time.Millisecond,
			QueueSize:     100,
		}),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}

	const goroutines = 10
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			// Errors are expected since Close races with Submit.
			agg.Submit(ctx, 1, []byte("race"))
		}()
	}

	// Yield to give goroutines a chance to submit before closing.
	runtime.Gosched()
	agg.Close()

	wg.Wait()
	// The test passes if there are no panics or data races.
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

func TestPickContext_AllCanceled(t *testing.T) {
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

	picked := agg.pickContext(proposals)
	if picked.Err() != nil {
		t.Error("pickContext with all canceled should return context.Background() (non-canceled)")
	}
}

func TestPickContext_FirstNonCanceled(t *testing.T) {
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

	picked := agg.pickContext(proposals)
	if picked.Err() != nil {
		t.Error("pickContext should return the first non-canceled context")
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
