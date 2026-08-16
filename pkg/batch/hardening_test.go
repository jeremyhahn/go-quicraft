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
	"sync"
	"testing"
	"time"
)

// rendezvousProposer answers a SyncPropose only once EVERY expected shard has
// entered one. A sequential flush can never satisfy that — the first shard
// would have to return before the second is ever called — so a batch whose
// proposes all complete is PROOF the shards were flushed concurrently, and a
// batch that times out is proof they were not.
//
// The rendezvous replaces a "slow shard plus a wall-clock wait" shape, which
// was a coin flip against the code it pinned. Two things were left to chance
// there: whether the two shards landed in the same batch at all, and — because
// flush ranges over a map, whose iteration order Go randomizes — whether the
// SLOW shard happened to be picked first. With the concurrency removed the old
// test still passed 5 runs in 20 (measured), because half the time the fast
// shard was simply flushed first and never had to wait for anything.
type rendezvousProposer struct {
	want int

	mu      sync.Mutex
	entered map[uint64]bool

	allIn chan struct{}
	once  sync.Once
}

func newRendezvousProposer(want int) *rendezvousProposer {
	return &rendezvousProposer{
		want:    want,
		entered: make(map[uint64]bool),
		allIn:   make(chan struct{}),
	}
}

func (p *rendezvousProposer) SyncPropose(ctx context.Context, shardID uint64, _ []byte) error {
	p.mu.Lock()
	p.entered[shardID] = true
	n := len(p.entered)
	p.mu.Unlock()

	if n >= p.want {
		p.once.Do(func() { close(p.allIn) })
	}
	select {
	case <-p.allIn:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestFlush_SlowShardDoesNotBlockOtherShards is the M7(a) regression test.
// flush used to call flushShard sequentially on the single flushLoop
// goroutine, so one slow or leaderless shard blocked every other shard's
// flush for the full proposal timeout, backing the queue up to QueueSize and
// producing QueueFullError for entirely unrelated shards.
//
// The batch is filled one proposal at a time so both shards provably share a
// single flush, and the proposer only returns once both are inside it. Reverting
// flush to a sequential loop fails this test on every run and in either map
// order: whichever shard is flushed first waits out ProposeTimeout for a peer
// that cannot be called until it returns.
func TestFlush_SlowShardDoesNotBlockOtherShards(t *testing.T) {
	p := newRendezvousProposer(2)
	agg, err := NewAggregator(p,
		WithConfig(Config{
			// The second proposal fills the batch and triggers the flush; an
			// hour-long interval keeps the timer from splitting the two shards
			// into separate batches, which would leave nothing to prove.
			MaxBatchSize:  2,
			FlushInterval: time.Hour,
			QueueSize:     10,
			// Bounds the failing case: under a sequential flush the first
			// shard's propose waits for a shard that will never be reached, and
			// only this ceiling releases it.
			ProposeTimeout: 2 * time.Second,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	first := make(chan error, 1)
	go func() { first <- agg.Submit(context.Background(), 1, []byte("shard-1")) }()

	// Shard 1's proposal must be in the batch before shard 2's is submitted, or
	// the two could be flushed separately and never test anything.
	waitForBatchLen(t, agg, 1)

	second := make(chan error, 1)
	go func() { second <- agg.Submit(context.Background(), 2, []byte("shard-2")) }()

	for _, c := range []struct {
		name string
		ch   chan error
	}{{"shard 1", first}, {"shard 2", second}} {
		select {
		case submitErr := <-c.ch:
			if submitErr != nil {
				t.Fatalf("%s: Submit = %v; the proposer answers only once BOTH "+
					"shards are inside SyncPropose, so this batch's shards were "+
					"flushed one after the other on the single flush goroutine",
					c.name, submitErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never got a result", c.name)
		}
	}
}

// timeoutProposer always reports a context deadline.
type timeoutProposer struct{}

func (timeoutProposer) SyncPropose(context.Context, uint64, []byte) error {
	return context.DeadlineExceeded
}

// rejectingProposer always reports a definite rejection.
type rejectingProposer struct{ err error }

func (r rejectingProposer) SyncPropose(context.Context, uint64, []byte) error {
	return r.err
}

// TestSubmit_TimeoutIsReportedIndeterminate is the M7(b) regression test. A
// proposer timeout or cancellation does NOT mean the proposal was rejected:
// the entry may already be replicated and will apply. Reporting a hard
// failure told the caller a committed write failed, and a blind retry
// duplicated the whole batch.
func TestSubmit_TimeoutIsReportedIndeterminate(t *testing.T) {
	agg, err := NewAggregator(timeoutProposer{},
		WithConfig(Config{MaxBatchSize: 1, FlushInterval: 5 * time.Millisecond, QueueSize: 10}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	submitErr := agg.Submit(context.Background(), 1, []byte("write"))
	if submitErr == nil {
		t.Fatal("Submit returned nil for a timed-out proposal")
	}

	var flushErr *FlushError
	if !errors.As(submitErr, &flushErr) {
		t.Fatalf("error = %T (%v), want *FlushError", submitErr, submitErr)
	}
	if !flushErr.Indeterminate {
		t.Error("a proposer timeout must be reported as indeterminate, not as a definite failure")
	}
	// The underlying proposer error must stay matchable through the wrapper.
	if !errors.Is(submitErr, context.DeadlineExceeded) {
		t.Error("the proposer's error is not reachable through errors.Is")
	}
	if !errors.Is(submitErr, ErrFlush) {
		t.Error("errors.Is(err, ErrFlush) = false, want true")
	}
}

// TestSubmit_DefiniteRejectionIsNotIndeterminate pins the other side of the
// M7(b) distinction: an ordinary rejection is a definite failure and is safe
// to surface as one.
func TestSubmit_DefiniteRejectionIsNotIndeterminate(t *testing.T) {
	rejected := errors.New("quicraft: not leader")
	agg, err := NewAggregator(rejectingProposer{err: rejected},
		WithConfig(Config{MaxBatchSize: 1, FlushInterval: 5 * time.Millisecond, QueueSize: 10}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	submitErr := agg.Submit(context.Background(), 1, []byte("write"))
	var flushErr *FlushError
	if !errors.As(submitErr, &flushErr) {
		t.Fatalf("error = %T (%v), want *FlushError", submitErr, submitErr)
	}
	if flushErr.Indeterminate {
		t.Error("a definite rejection must not be reported as indeterminate")
	}
	if !errors.Is(submitErr, rejected) {
		t.Error("the proposer's error is not reachable through errors.Is")
	}
}

// ctxBoundProposer blocks until its context is done, exactly as Host.SyncPropose
// does while a proposal is outstanding: it selects on the result channel and on
// ctx.Done(), so a context that can never fire is a permanent block. It records
// whether it was entered at all.
type ctxBoundProposer struct {
	entered chan struct{}
	once    sync.Once
}

func (p *ctxBoundProposer) SyncPropose(ctx context.Context, _ uint64, _ []byte) error {
	p.once.Do(func() { close(p.entered) })
	<-ctx.Done()
	return ctx.Err()
}

// TestFlush_AllCallersCanceledSkipsPropose is the regression test for the
// context.Background() fallback in pickContext. When every proposal context is
// already cancelled — which is what mass caller cancellation under quorum loss
// looks like — the batch used to be proposed on context.Background(). Nothing
// can cancel that, so SyncPropose blocked forever ON THE SINGLE FLUSH
// GOROUTINE: no later batch flushed, the queue filled to QueueSize, every
// subsequent Submit returned QueueFullError, and Close hung on wg.Wait.
func TestFlush_AllCallersCanceledSkipsPropose(t *testing.T) {
	p := &ctxBoundProposer{entered: make(chan struct{})}
	// An hour-long flush interval keeps the background loop's timer out of the
	// way so this test drives flush directly and deterministically.
	agg, err := NewAggregator(p,
		WithConfig(Config{MaxBatchSize: 100, FlushInterval: time.Hour, QueueSize: 100}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel1()
	cancel2()

	p1 := &proposal{ctx: ctx1, shardID: 1, data: []byte("a"), resultCh: make(chan error, 1)}
	p2 := &proposal{ctx: ctx2, shardID: 1, data: []byte("b"), resultCh: make(chan error, 1)}
	agg.mu.Lock()
	agg.currentBatch = append(agg.currentBatch, p1, p2)
	agg.mu.Unlock()

	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		agg.flush(flushReasonTime)
	}()
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("flush blocked: a batch with no live caller was proposed on an uncancellable context")
	}

	select {
	case <-p.entered:
		t.Error("a batch whose callers have all gone away must not be proposed")
	default:
	}

	for i, pr := range []*proposal{p1, p2} {
		select {
		case res := <-pr.resultCh:
			var flushErr *FlushError
			if !errors.As(res, &flushErr) {
				t.Fatalf("proposal %d: result = %T (%v), want *FlushError", i, res, res)
			}
			if flushErr.Indeterminate {
				t.Errorf("proposal %d: a batch that was never proposed is a definite failure", i)
			}
		default:
			t.Errorf("proposal %d: no result delivered", i)
		}
	}

	// Close must still return: the flush goroutine was never wedged.
	closed := make(chan error, 1)
	go func() { closed <- agg.Close() }()
	select {
	case closeErr := <-closed:
		if closeErr != nil {
			t.Fatalf("Close: %v", closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung on wg.Wait behind a wedged flush goroutine")
	}
}

// TestFlush_DeadlinelessCallerContextIsBounded pins the other half of the fix.
// A caller that submits with a context carrying no deadline (context.Background
// is the common case) must not be able to park the single flush goroutine
// indefinitely: the aggregator supplies ProposeTimeout as the ceiling.
func TestFlush_DeadlinelessCallerContextIsBounded(t *testing.T) {
	p := &ctxBoundProposer{entered: make(chan struct{})}
	agg, err := NewAggregator(p,
		WithConfig(Config{
			MaxBatchSize:   1,
			FlushInterval:  5 * time.Millisecond,
			QueueSize:      10,
			ProposeTimeout: 100 * time.Millisecond,
		}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	done := make(chan error, 1)
	go func() { done <- agg.Submit(context.Background(), 1, []byte("write")) }()

	select {
	case submitErr := <-done:
		if !errors.Is(submitErr, context.DeadlineExceeded) {
			t.Fatalf("Submit error = %v, want context.DeadlineExceeded", submitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a deadline-less caller context left the propose unbounded")
	}
}
