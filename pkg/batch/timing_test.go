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

// The branches here are otherwise reached only when a caller's context, Close
// and the flush loop happen to interleave in one particular order. Each test
// fixes that order instead of waiting for the scheduler to produce it.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// closedChan returns a channel that is already closed.
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// doneOnSecondLookCtx is a context that ends between two looks at it: its
// first Done returns a channel that is never ready, and every later one a
// closed channel. It stands for a caller whose context expires after
// awaitResult has chosen the close branch but before it waits there.
type doneOnSecondLookCtx struct {
	context.Context
	looks int
}

func (c *doneOnSecondLookCtx) Done() <-chan struct{} {
	c.looks++
	if c.looks == 1 {
		return nil
	}
	return closedChan()
}

func (c *doneOnSecondLookCtx) Err() error {
	if c.looks < 2 {
		return nil
	}
	return context.Canceled
}

// errOnSecondLookCtx is a context that is live when first asked and cancelled
// from the second question on: a caller who gives up after flushShard has
// filtered out the abandoned proposals but before it proposes.
type errOnSecondLookCtx struct {
	context.Context
	looks int
}

func (c *errOnSecondLookCtx) Err() error {
	c.looks++
	if c.looks == 1 {
		return nil
	}
	return context.Canceled
}

// closingAggregator is an Aggregator whose Close has begun, with flushDone
// as given; awaitResult reads nothing else.
func closingAggregator(flushDone chan struct{}) *Aggregator {
	return &Aggregator{closeChan: closedChan(), flushDone: flushDone}
}

func TestAwaitResult_CloseThenFlushLoopExitReportsClosed(t *testing.T) {
	// The proposal reached the queue after the flush loop drained it, so no
	// result will come: once the loop has exited, the caller is told closed.
	a := closingAggregator(closedChan())
	p := &proposal{ctx: context.Background(), shardID: 1, resultCh: make(chan error, 1)}

	err := a.awaitResult(context.Background(), p)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("awaitResult = %v, want ErrClosed", err)
	}
}

func TestAwaitResult_CloseThenContextEndReportsContextError(t *testing.T) {
	// Close began, and the caller's context ended while it waited for the
	// flush loop to finish. No result was delivered, so the context's error
	// is the answer.
	a := closingAggregator(make(chan struct{}))
	ctx := &doneOnSecondLookCtx{Context: context.Background()}
	p := &proposal{ctx: ctx, shardID: 1, resultCh: make(chan error, 1)}

	err := a.awaitResult(ctx, p)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitResult = %v, want context.Canceled", err)
	}
	if ctx.looks != 2 {
		t.Fatalf("context looked at %d times, want 2: the close branch was not the one taken", ctx.looks)
	}
}

func TestFlushShard_CallerGoneBeforeProposeIsNotProposed(t *testing.T) {
	mp := &mockProposer{}
	agg, err := NewAggregator(mp,
		WithConfig(Config{MaxBatchSize: 10, FlushInterval: time.Hour, QueueSize: 10}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	ctx := &errOnSecondLookCtx{Context: context.Background()}
	p := &proposal{ctx: ctx, shardID: 1, data: []byte("x"), resultCh: make(chan error, 1)}

	agg.flushShard(1, []*proposal{p})

	if ctx.looks != 2 {
		t.Fatalf("context looked at %d times, want 2: once by the filter, once before proposing", ctx.looks)
	}
	got := <-p.resultCh
	var fe *FlushError
	if !errors.As(got, &fe) {
		t.Fatalf("result = %v, want *FlushError", got)
	}
	if !errors.Is(fe.Err, context.Canceled) || fe.Indeterminate {
		t.Fatalf("result = %+v, want a definite context.Canceled", fe)
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if len(mp.calls) != 0 {
		t.Fatalf("proposer called %d times for a batch nobody waits on", len(mp.calls))
	}
}

func TestFlushShard_UnencodableCommandIsDefinitelyRejected(t *testing.T) {
	if math.MaxInt < math.MaxInt64 {
		t.Skip("int is 32-bit on this platform; a MaxInt32+1 slice is unrepresentable")
	}
	mp := &mockProposer{}
	agg, err := NewAggregator(mp,
		WithConfig(Config{MaxBatchSize: 10, FlushInterval: time.Hour, QueueSize: 10}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	// Never written to, so this costs address space rather than memory.
	n := math.MaxInt32
	ok := &proposal{ctx: context.Background(), shardID: 1, data: []byte("ok"), resultCh: make(chan error, 1)}
	big := &proposal{ctx: context.Background(), shardID: 1, data: make([]byte, n+1), resultCh: make(chan error, 1)}

	agg.flushShard(1, []*proposal{ok, big})

	for i, p := range []*proposal{ok, big} {
		got := <-p.resultCh
		var fe *FlushError
		if !errors.As(got, &fe) {
			t.Fatalf("proposal %d: result = %v, want *FlushError", i, got)
		}
		if !errors.Is(fe.Err, ErrEncode) || fe.Indeterminate {
			t.Fatalf("proposal %d: result = %+v, want a definite ErrEncode", i, fe)
		}
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if len(mp.calls) != 0 {
		t.Fatalf("proposer called %d times for a batch that could not be encoded", len(mp.calls))
	}
}

func TestFlush_EmptyBatchIsNotCounted(t *testing.T) {
	mp := &mockProposer{}
	reg := newTestRegistry(t)
	agg, err := NewAggregator(mp,
		WithConfig(Config{MaxBatchSize: 10, FlushInterval: time.Hour, QueueSize: 10}),
		WithLogger(slog.Default()),
		WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() { _ = agg.Close() }()

	agg.flush(flushReasonTime)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "quicraft_batch_flush_reason_total" {
			t.Fatalf("an empty batch was counted as a flush: %v", f)
		}
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if len(mp.calls) != 0 {
		t.Fatalf("proposer called %d times for an empty batch", len(mp.calls))
	}
}

func TestRegisterOrExisting(t *testing.T) {
	opts := prometheus.CounterOpts{Namespace: "quicraft", Subsystem: "batch", Name: "probe_total", Help: "probe"}
	newLogger := func() (*slog.Logger, *bytes.Buffer) {
		var buf bytes.Buffer
		return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
	}

	t.Run("adopts the registered collector", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger, _ := newLogger()
		first := registerOrExisting(reg, prometheus.NewCounter(opts), logger)
		second := registerOrExisting(reg, prometheus.NewCounter(opts), logger)
		if first != second {
			t.Fatal("a second registration did not adopt the collector already registered")
		}
	})

	t.Run("type collision keeps the fresh collector and warns", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger, buf := newLogger()
		// A Gauge satisfies the Counter interface but not the reverse, so the
		// registered Counter cannot be adopted as the Gauge asked for.
		reg.MustRegister(prometheus.NewCounter(opts))
		fresh := prometheus.NewGauge(prometheus.GaugeOpts(opts))
		if got := registerOrExisting(reg, fresh, logger); got != fresh {
			t.Fatal("a collision with a different collector type did not return the fresh collector")
		}
		if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "collision") {
			t.Fatalf("log = %q, want a collision warning", out)
		}
	})

	t.Run("other registration error keeps the fresh collector", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger, buf := newLogger()
		reg.MustRegister(prometheus.NewCounterVec(opts, []string{"shard"}))
		fresh := prometheus.NewCounter(opts)
		if got := registerOrExisting(reg, fresh, logger); got != fresh {
			t.Fatal("a failed registration did not return the fresh collector")
		}
		if out := buf.String(); !strings.Contains(out, "level=DEBUG") || !strings.Contains(out, "registration issue") {
			t.Fatalf("log = %q, want a registration-issue debug line", out)
		}
	})
}
