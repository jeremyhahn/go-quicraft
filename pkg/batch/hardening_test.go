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

// perShardProposer blocks proposals for one designated shard until it is
// released, and answers every other shard immediately.
type perShardProposer struct {
	slowShard uint64
	release   chan struct{}

	mu   sync.Mutex
	seen map[uint64]int
}

func (p *perShardProposer) SyncPropose(_ context.Context, shardID uint64, _ []byte) error {
	if shardID == p.slowShard {
		<-p.release
	}
	p.mu.Lock()
	if p.seen == nil {
		p.seen = make(map[uint64]int)
	}
	p.seen[shardID]++
	p.mu.Unlock()
	return nil
}

// TestFlush_SlowShardDoesNotBlockOtherShards is the M7(a) regression test.
// flush used to call flushShard sequentially on the single flushLoop
// goroutine, so one slow or leaderless shard blocked every other shard's
// flush for the full proposal timeout, backing the queue up to QueueSize and
// producing QueueFullError for entirely unrelated shards.
func TestFlush_SlowShardDoesNotBlockOtherShards(t *testing.T) {
	p := &perShardProposer{slowShard: 1, release: make(chan struct{})}
	agg, err := NewAggregator(p,
		WithConfig(Config{MaxBatchSize: 100, FlushInterval: 5 * time.Millisecond, QueueSize: 100}),
		WithLogger(slog.Default()),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	defer func() {
		close(p.release)
		_ = agg.Close()
	}()

	fast := make(chan error, 1)
	go func() { fast <- agg.Submit(context.Background(), 2, []byte("fast")) }()
	go func() { _ = agg.Submit(context.Background(), 1, []byte("slow")) }()

	select {
	case submitErr := <-fast:
		if submitErr != nil {
			t.Fatalf("fast shard Submit: %v", submitErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the fast shard's flush was blocked behind the slow shard")
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
