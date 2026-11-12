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
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/batch"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

const batchAggregatorShard = uint64(921)

// batchCountingStateMachine decodes every entry with batch.DecodeBatch, as
// docs/write-batching/user.md tells a state machine behind an Aggregator to,
// and counts the batch entries it applied and how often each command was
// applied.
type batchCountingStateMachine struct {
	mu      sync.Mutex
	entries int
	applied map[string]int
}

func newBatchCountingStateMachine() *batchCountingStateMachine {
	return &batchCountingStateMachine{applied: make(map[string]int)}
}

func (s *batchCountingStateMachine) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range entries {
		cmds, err := batch.DecodeBatch(e.Cmd)
		if err != nil {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		if len(cmds) > 0 {
			s.entries++
		}
		for _, cmd := range cmds {
			s.applied[string(cmd)]++
		}
		results[i] = sm.Result{Value: uint64(len(cmds))}
	}
	return nil
}

func (s *batchCountingStateMachine) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

func (s *batchCountingStateMachine) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (s *batchCountingStateMachine) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *batchCountingStateMachine) Close(context.Context) error { return nil }

// counts returns the number of batch entries applied and a copy of the
// per-command counts.
func (s *batchCountingStateMachine) counts() (int, map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.applied))
	for k, v := range s.applied {
		out[k] = v
	}
	return s.entries, out
}

var _ sm.StateMachine = (*batchCountingStateMachine)(nil)

// newBatchAggregatorHost starts a single-voter transportless Host with a real
// WAL, runs batchAggregatorShard on it under a batchCountingStateMachine, and
// returns both once the shard has a leader.
func newBatchAggregatorHost(t *testing.T) (*Host, *batchCountingStateMachine) {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	machine := newBatchCountingStateMachine()
	create := func(_, _ uint64) interface{} { return machine }
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, create, testShardConfig(batchAggregatorShard, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, batchAggregatorShard)
	return h, machine
}

// newBatchAggregator builds an Aggregator over h through batch.HostProposer,
// the wiring docs/write-batching/user.md documents.
func newBatchAggregator(t *testing.T, h *Host) *batch.Aggregator {
	t.Helper()
	cfg := batch.DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	agg, err := batch.NewAggregator(batch.HostProposer{Host: h}, batch.WithConfig(cfg))
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	t.Cleanup(func() { agg.Close() })
	return agg
}

// TestBatchAggregator_ConcurrentSubmitsAppliedExactlyOnceInFewerEntries: 64
// goroutines each Submit 50 distinct commands. Every Submit returns nil only
// after its batch is applied, so the state machine holds every command
// exactly once when the last one returns, and batching put them in fewer
// entries than commands.
func TestBatchAggregator_ConcurrentSubmitsAppliedExactlyOnceInFewerEntries(t *testing.T) {
	const (
		goroutines = 64
		perG       = 50
	)
	h, machine := newBatchAggregatorHost(t)
	agg := newBatchAggregator(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var (
		mu     sync.Mutex
		failed = make(map[string]error)
		wg     sync.WaitGroup
	)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perG {
				key := fmt.Sprintf("g%02d-k%02d", g, i)
				if err := agg.Submit(ctx, batchAggregatorShard, []byte(key)); err != nil {
					mu.Lock()
					failed[key] = err
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	for key, err := range failed {
		t.Errorf("Submit(%s) = %v, want nil", key, err)
	}

	entries, applied := machine.counts()
	for g := range goroutines {
		for i := range perG {
			key := fmt.Sprintf("g%02d-k%02d", g, i)
			if n := applied[key]; n != 1 {
				t.Errorf("command %s applied %d times, want 1", key, n)
			}
		}
	}
	if len(applied) != goroutines*perG {
		t.Errorf("state machine holds %d distinct commands, want %d", len(applied), goroutines*perG)
	}
	if entries >= goroutines*perG {
		t.Errorf("applied %d entries for %d commands, want fewer entries than commands", entries, goroutines*perG)
	}
}

// TestBatchAggregator_SubmitNoCopyAppliedUnchanged: a buffer handed to
// SubmitNoCopy and never touched again is applied byte for byte.
func TestBatchAggregator_SubmitNoCopyAppliedUnchanged(t *testing.T) {
	h, machine := newBatchAggregatorHost(t)
	agg := newBatchAggregator(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const want = "no-copy-command-\x00\xff-921"
	buf := make([]byte, len(want))
	copy(buf, want)
	if err := agg.SubmitNoCopy(ctx, batchAggregatorShard, buf); err != nil {
		t.Fatalf("SubmitNoCopy = %v, want nil", err)
	}

	_, applied := machine.counts()
	if n := applied[want]; n != 1 {
		t.Fatalf("SubmitNoCopy command applied %d times, want 1 (applied: %v)", n, applied)
	}
}

// TestBatchAggregator_CancelledSubmitNeverApplied: a Submit whose context is
// already cancelled returns an error matching context.Canceled, and its
// command is never applied, while a later live Submit still is.
func TestBatchAggregator_CancelledSubmitNeverApplied(t *testing.T) {
	h, machine := newBatchAggregatorHost(t)
	agg := newBatchAggregator(t, h)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	const dropped = "cancelled-command"
	err := agg.Submit(cancelled, batchAggregatorShard, []byte(dropped))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit under a cancelled context = %v, want an error matching context.Canceled", err)
	}

	// A live Submit after it: once this returns, any batch the cancelled
	// command could have joined has been flushed.
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	const live = "live-command"
	if err := agg.Submit(ctx, batchAggregatorShard, []byte(live)); err != nil {
		t.Fatalf("live Submit = %v, want nil", err)
	}
	if err := agg.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}

	_, applied := machine.counts()
	if n := applied[dropped]; n != 0 {
		t.Errorf("cancelled command applied %d times, want 0", n)
	}
	if n := applied[live]; n != 1 {
		t.Errorf("live command applied %d times, want 1", n)
	}
}

// TestBatchAggregator_SubmitAfterCloseFails: after Close nothing is left
// unflushed, Submit fails with ErrClosed and applies nothing, and a second
// Close returns nil.
func TestBatchAggregator_SubmitAfterCloseFails(t *testing.T) {
	h, machine := newBatchAggregatorHost(t)
	agg := newBatchAggregator(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := agg.Submit(ctx, batchAggregatorShard, []byte("before-close")); err != nil {
		t.Fatalf("Submit before Close = %v, want nil", err)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if n := agg.BatchLen(); n != 0 {
		t.Errorf("BatchLen after Close = %d, want 0", n)
	}

	const late = "after-close"
	err := agg.Submit(ctx, batchAggregatorShard, []byte(late))
	if err == nil {
		t.Fatal("Submit after Close = nil, want an error")
	}
	if !errors.Is(err, batch.ErrClosed) {
		t.Errorf("Submit after Close = %v, want an error matching batch.ErrClosed", err)
	}
	var ce *batch.ClosedError
	if !errors.As(err, &ce) {
		t.Errorf("Submit after Close = %T, want *batch.ClosedError", err)
	}
	if err := agg.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}

	_, applied := machine.counts()
	if n := applied[late]; n != 0 {
		t.Errorf("command submitted after Close applied %d times, want 0", n)
	}
	if n := applied["before-close"]; n != 1 {
		t.Errorf("command submitted before Close applied %d times, want 1", n)
	}
}

// TestBatchAggregator_UnknownShardReturnsShardNotFound: a Submit to a shard
// the Host does not run surfaces the Host's ErrShardNotFound.
func TestBatchAggregator_UnknownShardReturnsShardNotFound(t *testing.T) {
	h, machine := newBatchAggregatorHost(t)
	agg := newBatchAggregator(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const absent = batchAggregatorShard + 1000
	err := agg.Submit(ctx, absent, []byte("to-absent-shard"))
	if !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("Submit to absent shard = %v, want an error matching ErrShardNotFound", err)
	}

	_, applied := machine.counts()
	if len(applied) != 0 {
		t.Errorf("state machine applied %v, want nothing", applied)
	}
}
