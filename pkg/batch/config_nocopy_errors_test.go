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
	"sync"
	"testing"
	"time"
)

// submitFunc is the shared signature of Submit and SubmitNoCopy.
type submitFunc func(ctx context.Context, shardID uint64, data []byte) error

// submitVariant names one of the two submit entry points of an aggregator.
type submitVariant struct {
	name string
	of   func(a *Aggregator) submitFunc
}

// submitVariants lists SubmitNoCopy first: the documented claim is that it
// returns the same errors as Submit, so every error test runs both.
var submitVariants = []submitVariant{
	{name: "SubmitNoCopy", of: func(a *Aggregator) submitFunc { return a.SubmitNoCopy }},
	{name: "Submit", of: func(a *Aggregator) submitFunc { return a.Submit }},
}

// newConfiguredAggregator builds an aggregator over p with cfg applied
// through WithConfig and a private metrics registry, closed at cleanup.
func newConfiguredAggregator(t *testing.T, p Proposer, cfg Config) *Aggregator {
	t.Helper()
	agg, err := NewAggregator(p, WithConfig(cfg), WithRegistry(newTestRegistry(t)))
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}
	t.Cleanup(func() { _ = agg.Close() })
	return agg
}

// submitAsync runs submit in a goroutine and returns the channel that
// receives its result.
func submitAsync(ctx context.Context, t *testing.T, submit submitFunc, data []byte) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- submit(ctx, 1, data) }()
	return done
}

// awaitSubmit waits, bounded, for an asynchronous submit's result.
func awaitSubmit(ctx context.Context, t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatalf("submit did not return: %v", ctx.Err())
		return nil
	}
}

func TestDefaultConfig_HoldsDocumentedDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxBatchSize != DefaultMaxBatchSize {
		t.Errorf("MaxBatchSize = %d, want DefaultMaxBatchSize %d", cfg.MaxBatchSize, DefaultMaxBatchSize)
	}
	if cfg.MaxBatchBytes != DefaultMaxBatchBytes {
		t.Errorf("MaxBatchBytes = %d, want DefaultMaxBatchBytes %d", cfg.MaxBatchBytes, DefaultMaxBatchBytes)
	}
	if cfg.FlushInterval != DefaultFlushInterval {
		t.Errorf("FlushInterval = %v, want DefaultFlushInterval %v", cfg.FlushInterval, DefaultFlushInterval)
	}
	if cfg.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d, want DefaultQueueSize %d", cfg.QueueSize, DefaultQueueSize)
	}
	if cfg.ProposeTimeout != DefaultProposeTimeout {
		t.Errorf("ProposeTimeout = %v, want DefaultProposeTimeout %v", cfg.ProposeTimeout, DefaultProposeTimeout)
	}
	if cfg != (Config{
		MaxBatchSize:   DefaultMaxBatchSize,
		MaxBatchBytes:  DefaultMaxBatchBytes,
		FlushInterval:  DefaultFlushInterval,
		QueueSize:      DefaultQueueSize,
		ProposeTimeout: DefaultProposeTimeout,
	}) {
		t.Errorf("DefaultConfig() = %+v carries a field beyond the Default* constants", cfg)
	}
}

func TestWithConfig_ZeroFieldsKeepDefaults(t *testing.T) {
	t.Run("all zero", func(t *testing.T) {
		agg := newConfiguredAggregator(t, &mockProposer{}, Config{})
		if agg.cfg != DefaultConfig() {
			t.Errorf("config = %+v, want DefaultConfig() %+v", agg.cfg, DefaultConfig())
		}
		if got := cap(agg.queue); got != DefaultQueueSize {
			t.Errorf("queue capacity = %d, want DefaultQueueSize %d", got, DefaultQueueSize)
		}
	})

	cases := []struct {
		name string
		set  Config
		want func(c *Config)
	}{
		{"MaxBatchSize", Config{MaxBatchSize: 7}, func(c *Config) { c.MaxBatchSize = 7 }},
		{"MaxBatchBytes", Config{MaxBatchBytes: 4096}, func(c *Config) { c.MaxBatchBytes = 4096 }},
		{"FlushInterval", Config{FlushInterval: 3 * time.Millisecond}, func(c *Config) { c.FlushInterval = 3 * time.Millisecond }},
		{"QueueSize", Config{QueueSize: 3}, func(c *Config) { c.QueueSize = 3 }},
		{"ProposeTimeout", Config{ProposeTimeout: 2 * time.Second}, func(c *Config) { c.ProposeTimeout = 2 * time.Second }},
	}
	for _, tc := range cases {
		t.Run("only "+tc.name, func(t *testing.T) {
			agg := newConfiguredAggregator(t, &mockProposer{}, tc.set)
			want := DefaultConfig()
			tc.want(&want)
			if agg.cfg != want {
				t.Errorf("config = %+v, want %+v (only %s changed)", agg.cfg, want, tc.name)
			}
			if got := cap(agg.queue); got != want.QueueSize {
				t.Errorf("queue capacity = %d, want %d", got, want.QueueSize)
			}
		})
	}

	t.Run("negative fields keep defaults", func(t *testing.T) {
		agg := newConfiguredAggregator(t, &mockProposer{}, Config{
			MaxBatchSize:   -1,
			FlushInterval:  -time.Millisecond,
			QueueSize:      -1,
			ProposeTimeout: -time.Second,
		})
		if agg.cfg != DefaultConfig() {
			t.Errorf("config = %+v, want DefaultConfig() %+v", agg.cfg, DefaultConfig())
		}
	})
}

func TestSubmitNoCopy_ClosedError(t *testing.T) {
	for _, v := range submitVariants {
		t.Run(v.name, func(t *testing.T) {
			agg := newConfiguredAggregator(t, &mockProposer{}, Config{FlushInterval: time.Millisecond})
			if err := agg.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			err := v.of(agg)(context.Background(), 1, []byte("x"))
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("%s after Close = %v, want an error matching ErrClosed", v.name, err)
			}
			var ce *ClosedError
			if !errors.As(err, &ce) {
				t.Fatalf("%s after Close = %T, want *ClosedError", v.name, err)
			}
		})
	}
}

func TestSubmitNoCopy_QueueFullError(t *testing.T) {
	for _, v := range submitVariants {
		t.Run(v.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			bp := &enteredBlockingProposer{unblock: make(chan struct{}), entered: make(chan struct{})}
			agg := newConfiguredAggregator(t, bp, Config{
				MaxBatchSize:  1,
				QueueSize:     1,
				FlushInterval: time.Hour,
			})
			// Cleanups run last-registered first, so a failing test releases
			// the proposer before the aggregator's Close waits on it.
			release := sync.OnceFunc(func() { close(bp.unblock) })
			t.Cleanup(release)
			submit := v.of(agg)

			// The first proposal fills a batch of one, so the flush loop is
			// held inside the blocked proposer.
			first := submitAsync(ctx, t, submit, []byte("first"))
			select {
			case <-bp.entered:
			case <-ctx.Done():
				t.Fatal("the proposer was never called for the first proposal")
			}
			// The second proposal occupies the only queue slot.
			second := submitAsync(ctx, t, submit, []byte("second"))
			waitForCondition(t, 5*time.Second, "the second proposal never reached the queue", func() bool {
				return len(agg.queue) == 1
			})

			err := submit(ctx, 1, []byte("third"))
			var qf *QueueFullError
			if !errors.As(err, &qf) {
				t.Fatalf("%s at capacity = %v (%T), want *QueueFullError", v.name, err, err)
			}
			if !errors.Is(err, ErrQueueFull) {
				t.Errorf("%s at capacity = %v, want an error matching ErrQueueFull", v.name, err)
			}
			if qf.QueueSize != 1 {
				t.Errorf("QueueFullError.QueueSize = %d, want 1", qf.QueueSize)
			}

			release()
			if err := awaitSubmit(ctx, t, first); err != nil {
				t.Errorf("first submit = %v, want nil", err)
			}
			if err := awaitSubmit(ctx, t, second); err != nil {
				t.Errorf("second submit = %v, want nil", err)
			}
		})
	}
}

func TestSubmitNoCopy_ContextErrors(t *testing.T) {
	for _, v := range submitVariants {
		t.Run(v.name+"/canceled", func(t *testing.T) {
			agg := newConfiguredAggregator(t, &mockProposer{}, Config{FlushInterval: time.Millisecond})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := v.of(agg)(ctx, 1, []byte("x"))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s with a cancelled context = %v, want an error matching context.Canceled", v.name, err)
			}
		})
		t.Run(v.name+"/deadline", func(t *testing.T) {
			agg := newConfiguredAggregator(t, &mockProposer{}, Config{FlushInterval: time.Millisecond})
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			err := v.of(agg)(ctx, 1, []byte("x"))
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s with an expired deadline = %v, want an error matching context.DeadlineExceeded", v.name, err)
			}
		})
	}
}

func TestSubmitNoCopy_FlushErrorWrapsProposerError(t *testing.T) {
	errProposer := errors.New("proposer refused the batch")
	for _, v := range submitVariants {
		t.Run(v.name, func(t *testing.T) {
			agg := newConfiguredAggregator(t, rejectingProposer{err: errProposer}, Config{
				MaxBatchSize:  1,
				FlushInterval: time.Millisecond,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := v.of(agg)(ctx, 9, []byte("x"))
			var fe *FlushError
			if !errors.As(err, &fe) {
				t.Fatalf("%s with a failing proposer = %v (%T), want *FlushError", v.name, err, err)
			}
			if !errors.Is(err, errProposer) {
				t.Errorf("%s error %v does not match the proposer's error", v.name, err)
			}
			if !errors.Is(err, ErrFlush) {
				t.Errorf("%s error %v does not match ErrFlush", v.name, err)
			}
			if fe.ShardID != 9 {
				t.Errorf("FlushError.ShardID = %d, want 9", fe.ShardID)
			}
			if fe.Indeterminate {
				t.Errorf("FlushError.Indeterminate = true for a plain proposer rejection, want false")
			}
		})
	}
}
