package quicraft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// applyWorkersProbe is shared by every shard's state machine in the
// MaxApplyWorkers test. It records how many Update calls run at once across
// the Host, the highest such count, how often one shard's Update calls
// overlapped, and how many times each proposal payload was applied.
type applyWorkersProbe struct {
	release  chan struct{}
	running  atomic.Int64
	peak     atomic.Int64
	overlaps atomic.Int64

	mu      sync.Mutex
	applied map[string]int
}

func (p *applyWorkersProbe) enter() {
	n := p.running.Add(1)
	for {
		cur := p.peak.Load()
		if n <= cur || p.peak.CompareAndSwap(cur, n) {
			return
		}
	}
}

func (p *applyWorkersProbe) appliedCounts() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.applied))
	for k, v := range p.applied {
		out[k] = v
	}
	return out
}

// applyWorkersSM blocks every Update that carries a proposal payload until
// the probe's release channel is closed. Updates with only empty entries
// (leader no-ops) are counted but do not block.
type applyWorkersSM struct {
	mockStateMachine
	probe     *applyWorkersProbe
	inFlight  atomic.Int32
	shardPeak atomic.Int32
}

func (s *applyWorkersSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	if n > 1 {
		s.probe.overlaps.Add(1)
	}
	if n > s.shardPeak.Load() {
		s.shardPeak.Store(n)
	}
	s.probe.enter()
	defer s.probe.running.Add(-1)

	hasCmd := false
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			hasCmd = true
			break
		}
	}
	if hasCmd {
		select {
		case <-s.probe.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.probe.mu.Lock()
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			s.probe.applied[string(entries[i].Cmd)]++
		}
		results[i] = sm.Result{Value: entries[i].Index}
	}
	s.probe.mu.Unlock()
	return nil
}

// waitForApplyWorkersCondition polls cond until it holds or ctx ends.
func waitForApplyWorkersCondition(ctx context.Context, t *testing.T, what string, cond func() bool) {
	t.Helper()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(time.Millisecond):
		}
	}
}

// TestHost_ApplyWorkers_BoundConcurrentUpdatesAcrossShards checks the
// documented MaxApplyWorkers bound at the Host level. The engine gives each
// apply worker its own MaxApplyWorkers slots, so with NumWorkers 1 the bound
// across the Host is exactly MaxApplyWorkers (2 here). Eight shards each have
// one proposal outstanding while their Updates are held:
//
//	(a) the number of Updates running at once reaches 2 and never exceeds it;
//	(b) no two Updates for one shard overlap;
//	(c) after release every proposal completes and is applied exactly once;
//	(d) Close returns in bounded time with no Update still running.
func TestHost_ApplyWorkers_BoundConcurrentUpdatesAcrossShards(t *testing.T) {
	const (
		numShards       = 8
		maxApplyWorkers = 2
	)

	cfg := testHostConfig(t)
	cfg.NumWorkers = 1
	cfg.MaxApplyWorkers = maxApplyWorkers

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}

	probe := &applyWorkersProbe{
		release: make(chan struct{}),
		applied: make(map[string]int),
	}
	release := sync.OnceFunc(func() { close(probe.release) })
	closed := false
	defer func() {
		release()
		if !closed {
			_ = h.Close()
		}
	}()

	sms := make([]*applyWorkersSM, numShards+1)
	var smsMu sync.Mutex
	createFn := func(shardID, _ uint64) interface{} {
		s := &applyWorkersSM{probe: probe}
		smsMu.Lock()
		sms[shardID] = s
		smsMu.Unlock()
		return s
	}

	members := map[uint64]string{1: "127.0.0.1:5000"}
	for shardID := uint64(1); shardID <= numShards; shardID++ {
		if err := h.StartShard(members, false, createFn, testShardConfig(shardID, 1)); err != nil {
			t.Fatalf("StartShard(%d) failed: %v", shardID, err)
		}
	}
	for shardID := uint64(1); shardID <= numShards; shardID++ {
		waitForLeader(t, h, shardID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	payload := func(shardID uint64) string { return fmt.Sprintf("shard-%d", shardID) }
	errs := make(chan error, numShards)
	for shardID := uint64(1); shardID <= numShards; shardID++ {
		go func(shardID uint64) {
			_, perr := h.SyncPropose(ctx, shardID, []byte(payload(shardID)))
			if perr != nil {
				perr = fmt.Errorf("shard %d: %w", shardID, perr)
			}
			errs <- perr
		}(shardID)
	}

	// (a) With a proposal outstanding on all eight shards, the held Updates
	// fill exactly the two slots and stay there: the other six wait.
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	waitForApplyWorkersCondition(waitCtx, t, "2 Updates running at once", func() bool {
		return probe.running.Load() == maxApplyWorkers
	})
	waitCancel()
	time.Sleep(200 * time.Millisecond)
	if got := probe.running.Load(); got != maxApplyWorkers {
		t.Fatalf("Updates running while held = %d, want %d", got, maxApplyWorkers)
	}
	if got := probe.peak.Load(); got != maxApplyWorkers {
		t.Fatalf("peak concurrent Updates while held = %d, want %d", got, maxApplyWorkers)
	}
	if got := len(probe.appliedCounts()); got != 0 {
		t.Fatalf("proposals applied while held = %d, want 0", got)
	}

	// (c) Release: every proposal completes.
	release()
	for range numShards {
		select {
		case perr := <-errs:
			if perr != nil {
				t.Fatalf("SyncPropose failed: %v", perr)
			}
		case <-ctx.Done():
			t.Fatalf("proposals did not complete after release: %v", ctx.Err())
		}
	}

	// (a) The bound held for the whole run.
	if got := probe.peak.Load(); got != maxApplyWorkers {
		t.Fatalf("peak concurrent Updates = %d, want %d", got, maxApplyWorkers)
	}

	// (b) No shard ever had two Updates running at once.
	if got := probe.overlaps.Load(); got != 0 {
		t.Fatalf("overlapping Updates within one shard = %d, want 0", got)
	}
	smsMu.Lock()
	for shardID := uint64(1); shardID <= numShards; shardID++ {
		if s := sms[shardID]; s == nil {
			t.Errorf("shard %d: state machine was never created", shardID)
		} else if p := s.shardPeak.Load(); p != 1 {
			t.Errorf("shard %d: peak concurrent Updates = %d, want 1", shardID, p)
		}
	}
	smsMu.Unlock()

	// (c) Each proposal was applied exactly once.
	counts := probe.appliedCounts()
	if len(counts) != numShards {
		t.Fatalf("distinct proposals applied = %d, want %d (%v)", len(counts), numShards, counts)
	}
	for shardID := uint64(1); shardID <= numShards; shardID++ {
		if c := counts[payload(shardID)]; c != 1 {
			t.Errorf("shard %d: proposal applied %d times, want 1", shardID, c)
		}
	}

	// (d) Close returns in bounded time and leaves no Update running.
	closeErr := make(chan error, 1)
	go func() { closeErr <- h.Close() }()
	select {
	case cerr := <-closeErr:
		closed = true
		if cerr != nil {
			t.Fatalf("Close failed: %v", cerr)
		}
	case <-time.After(10 * time.Second):
		closed = true // do not wait on a hung Close again in the defer
		t.Fatal("Close did not return within 10s")
	}
	if got := probe.running.Load(); got != 0 {
		t.Fatalf("Updates running after Close = %d, want 0", got)
	}
}
