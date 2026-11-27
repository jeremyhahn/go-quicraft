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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// lifecycleCall is one collector call recorded by lifecycleCollector.
type lifecycleCall struct {
	method  string
	shardID uint64
}

// lifecycleCollector records, in order, the shard lifecycle calls and the
// per-shard latency samples a Host makes on its MetricsCollector. Every
// other method is the embedded no-op.
type lifecycleCollector struct {
	NoOpMetricsCollector
	mu    sync.Mutex
	calls []lifecycleCall
}

func (c *lifecycleCollector) record(method string, shardID uint64) {
	c.mu.Lock()
	c.calls = append(c.calls, lifecycleCall{method: method, shardID: shardID})
	c.mu.Unlock()
}

func (c *lifecycleCollector) RegisterShard(shardID uint64) {
	c.record("RegisterShard", shardID)
}

func (c *lifecycleCollector) UnregisterShard(shardID uint64) {
	c.record("UnregisterShard", shardID)
}

func (c *lifecycleCollector) ObserveProposalLatency(shardID uint64, _ time.Duration) {
	c.record("ObserveProposalLatency", shardID)
}

func (c *lifecycleCollector) ObserveCommitLatency(shardID uint64, _ time.Duration) {
	c.record("ObserveCommitLatency", shardID)
}

func (c *lifecycleCollector) ObserveApplyLatency(shardID uint64, _ time.Duration) {
	c.record("ObserveApplyLatency", shardID)
}

func (c *lifecycleCollector) snapshot() []lifecycleCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]lifecycleCall(nil), c.calls...)
}

// indices returns the positions of every call to method for shardID.
func lifecycleIndices(calls []lifecycleCall, method string, shardID uint64) []int {
	var out []int
	for i, c := range calls {
		if c.method == method && c.shardID == shardID {
			out = append(out, i)
		}
	}
	return out
}

func isLatencySample(method string) bool {
	switch method {
	case "ObserveProposalLatency", "ObserveCommitLatency", "ObserveApplyLatency":
		return true
	}
	return false
}

func newMetricsLifecycleHost(t *testing.T, c MetricsCollector) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption(), WithMetrics(c))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func startLifecycleShard(t *testing.T, h *Host, shardID uint64) {
	t.Helper()
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("StartShard(%d): %v", shardID, err)
	}
	waitForLeader(t, h, shardID)
}

// waitForLifecycleCall polls rc's snapshot until it holds a call to method
// for shardID, and returns that snapshot. It fails the test when ctx ends
// first.
func waitForLifecycleCall(ctx context.Context, t *testing.T, rc *lifecycleCollector, method string, shardID uint64) []lifecycleCall {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		calls := rc.snapshot()
		if len(lifecycleIndices(calls, method, shardID)) > 0 {
			return calls
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no %s(%d) recorded before deadline: %v", method, shardID, ctx.Err())
			return nil
		case <-tick.C:
		}
	}
}

// TestStartShard_StopShardBeforeRegister_NoLeak checks that a StopShard which
// claims teardown after StartShard's TOCTOU check but before its metrics and
// registry registration leaves no per-shard metric series and no member
// address behind, that StartShard reports ErrClosed, and that a restart of the
// same shard then works.
func TestStartShard_StopShardBeforeRegister_NoLeak(t *testing.T) {
	rc := &lifecycleCollector{}
	h := newMetricsLifecycleHost(t, rc)
	const shardID = 77
	members := map[uint64]string{1: "127.0.0.1:5000"}

	var stopErr error
	testHookStartShardBeforeRegister = func(id uint64) {
		testHookStartShardBeforeRegister = nil
		stopErr = h.StopShard(id)
	}
	t.Cleanup(func() { testHookStartShardBeforeRegister = nil })

	err := h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1))
	if stopErr != nil {
		t.Fatalf("StopShard from hook: %v", stopErr)
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("StartShard = %v, want ErrClosed", err)
	}

	calls := rc.snapshot()
	regs := lifecycleIndices(calls, "RegisterShard", shardID)
	unregs := lifecycleIndices(calls, "UnregisterShard", shardID)
	if len(regs) > 0 && (len(unregs) == 0 || regs[len(regs)-1] > unregs[len(unregs)-1]) {
		t.Fatalf("RegisterShard(%d) left after the last UnregisterShard: %v", shardID, calls)
	}
	for replicaID := range members {
		if addr, rerr := h.registry.Resolve(shardID, replicaID); rerr == nil {
			t.Fatalf("replica %d still resolves to %q after StopShard", replicaID, addr)
		}
	}
	if _, ok := h.nodes.Load(uint64(shardID)); ok {
		t.Fatalf("h.nodes still holds shard %d", shardID)
	}

	startLifecycleShard(t, h, shardID)
	for replicaID, want := range members {
		addr, rerr := h.registry.Resolve(shardID, replicaID)
		if rerr != nil || addr != want {
			t.Fatalf("restart: Resolve(%d, %d) = %q, %v; want %q", shardID, replicaID, addr, rerr, want)
		}
	}
}

// shardInfoCollector calls back into the host from RegisterShard, as an
// embedder's collector may to label its series.
type shardInfoCollector struct {
	NoOpMetricsCollector
	host  atomic.Pointer[Host]
	calls atomic.Int32
}

func (c *shardInfoCollector) RegisterShard(shardID uint64) {
	if h := c.host.Load(); h != nil {
		_, _ = h.ShardInfo(shardID)
		c.calls.Add(1)
	}
}

// TestStartShard_CollectorCallsShardInfo_NoDeadlock verifies StartShard does
// not hold a nodeState lock that ShardInfo needs while it runs the embedder's
// RegisterShard.
func TestStartShard_CollectorCallsShardInfo_NoDeadlock(t *testing.T) {
	c := &shardInfoCollector{}
	h := newMetricsLifecycleHost(t, c)
	c.host.Store(h)
	const shardID = 78
	members := map[uint64]string{1: "127.0.0.1:5000"}

	done := make(chan error, 1)
	go func() {
		done <- h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StartShard: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StartShard did not return within 10s: RegisterShard calling ShardInfo deadlocked")
	}
	if c.calls.Load() == 0 {
		t.Fatal("collector RegisterShard was not called")
	}
}

// pausingCollector records lifecycle calls like lifecycleCollector, and its
// first RegisterShard for shardID signals entered and then blocks until
// release is closed.
type pausingCollector struct {
	lifecycleCollector
	shardID uint64
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *pausingCollector) RegisterShard(shardID uint64) {
	if shardID == c.shardID {
		c.once.Do(func() {
			close(c.entered)
			<-c.release
		})
	}
	c.lifecycleCollector.RegisterShard(shardID)
}

// TestStartShard_StopShardWaitsForInFlightRegister checks that a StopShard
// which claims teardown while StartShard is inside the collector's
// RegisterShard waits on teardownShard's lifecycleMu barrier until that
// registration completes, and that it then removes both the metric series and
// the member addresses the registration added.
func TestStartShard_StopShardWaitsForInFlightRegister(t *testing.T) {
	const shardID = 79
	pc := &pausingCollector{
		shardID: shardID,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	h := newMetricsLifecycleHost(t, pc)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	var releaseOnce sync.Once
	releaseRegister := func() { releaseOnce.Do(func() { close(pc.release) }) }
	t.Cleanup(releaseRegister)

	startDone := make(chan error, 1)
	go func() {
		startDone <- h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1))
	}()
	select {
	case <-pc.entered:
	case err := <-startDone:
		t.Fatalf("StartShard returned %v before RegisterShard(%d) was called", err, shardID)
	case <-time.After(10 * time.Second):
		t.Fatalf("RegisterShard(%d) not called within 10s", shardID)
	}
	v, ok := h.nodes.Load(uint64(shardID))
	if !ok {
		t.Fatalf("h.nodes does not hold shard %d while RegisterShard runs", shardID)
	}
	ns := v.(*nodeState)

	stopDone := make(chan error, 1)
	go func() { stopDone <- h.StopShard(shardID) }()

	// Wait until StopShard has claimed teardown, so it is past its stopped
	// CAS and heading for the barrier, then give it time to (wrongly) finish.
	deadline := time.Now().Add(10 * time.Second)
	for !ns.stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("StopShard did not claim teardown within 10s")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-stopDone:
		t.Fatalf("StopShard returned %v while RegisterShard(%d) was in flight; teardown must wait on lifecycleMu", err, shardID)
	case <-time.After(200 * time.Millisecond):
	}

	releaseRegister()
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("StartShard: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StartShard did not return within 10s after RegisterShard was released")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopShard: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopShard did not return within 10s after RegisterShard was released")
	}

	calls := pc.snapshot()
	regs := lifecycleIndices(calls, "RegisterShard", shardID)
	unregs := lifecycleIndices(calls, "UnregisterShard", shardID)
	if len(regs) != 1 {
		t.Fatalf("RegisterShard(%d) calls = %d, want 1: %v", shardID, len(regs), calls)
	}
	if len(unregs) == 0 || regs[len(regs)-1] > unregs[len(unregs)-1] {
		t.Fatalf("RegisterShard(%d) left after the last UnregisterShard: %v", shardID, calls)
	}
	for replicaID := range members {
		if addr, rerr := h.registry.Resolve(shardID, replicaID); rerr == nil {
			t.Fatalf("replica %d still resolves to %q after StopShard", replicaID, addr)
		}
	}
	if _, ok := h.nodes.Load(uint64(shardID)); ok {
		t.Fatalf("h.nodes still holds shard %d", shardID)
	}
}

// TestHostMetricsShardLifecycle_RecordingCollector checks that a Host calls
// RegisterShard on StartShard before any per-shard sample, UnregisterShard on
// StopShard for that shard only, and RegisterShard again on a restart.
func TestHostMetricsShardLifecycle_RecordingCollector(t *testing.T) {
	rc := &lifecycleCollector{}
	h := newMetricsLifecycleHost(t, rc)

	// (a) RegisterShard(1) once, with no per-shard sample for shard 1 before it.
	startLifecycleShard(t, h, 1)
	calls := rc.snapshot()
	regs := lifecycleIndices(calls, "RegisterShard", 1)
	if len(regs) != 1 {
		t.Fatalf("(a) RegisterShard(1) calls = %d, want 1", len(regs))
	}
	for i := 0; i < regs[0]; i++ {
		if isLatencySample(calls[i].method) && calls[i].shardID == 1 {
			t.Fatalf("(a) %s(1) at %d recorded before RegisterShard(1) at %d", calls[i].method, i, regs[0])
		}
	}

	// (b) A SyncPropose yields an ObserveProposalLatency(1) after RegisterShard(1).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.SyncPropose(ctx, 1, []byte("lifecycle")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	// SyncPropose returns once the result is delivered; the apply goroutine
	// records the latency sample after that, so wait for it.
	calls = waitForLifecycleCall(ctx, t, rc, "ObserveProposalLatency", 1)
	props := lifecycleIndices(calls, "ObserveProposalLatency", 1)
	if len(props) == 0 {
		t.Fatal("(b) no ObserveProposalLatency(1) after SyncPropose")
	}
	if props[0] < regs[0] {
		t.Fatalf("(b) ObserveProposalLatency(1) at %d precedes RegisterShard(1) at %d", props[0], regs[0])
	}

	// (c) StopShard(1) gives one UnregisterShard(1); shard 2 is untouched.
	startLifecycleShard(t, h, 2)
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard(1): %v", err)
	}
	calls = rc.snapshot()
	if n := len(lifecycleIndices(calls, "UnregisterShard", 1)); n != 1 {
		t.Fatalf("(c) UnregisterShard(1) calls = %d, want 1", n)
	}
	if n := len(lifecycleIndices(calls, "UnregisterShard", 2)); n != 0 {
		t.Fatalf("(c) UnregisterShard(2) calls = %d, want 0", n)
	}
	if n := len(lifecycleIndices(calls, "RegisterShard", 2)); n != 1 {
		t.Fatalf("(c) RegisterShard(2) calls = %d, want 1", n)
	}

	// (d) Restarting shard 1 gives a second RegisterShard(1).
	startLifecycleShard(t, h, 1)
	calls = rc.snapshot()
	regs = lifecycleIndices(calls, "RegisterShard", 1)
	if len(regs) != 2 {
		t.Fatalf("(d) RegisterShard(1) calls after restart = %d, want 2", len(regs))
	}
	unregs := lifecycleIndices(calls, "UnregisterShard", 1)
	if regs[1] < unregs[0] {
		t.Fatalf("(d) second RegisterShard(1) at %d precedes UnregisterShard(1) at %d", regs[1], unregs[0])
	}
}

// TestHostMetricsShardLifecycle_PrometheusDropsSeries checks that a per-shard
// Prometheus collector passed to a Host carries the shard's series while it
// runs and drops them when the Host stops the shard.
func TestHostMetricsShardLifecycle_PrometheusDropsSeries(t *testing.T) {
	const name = "quicraft_raft_elections_total"
	pc := NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: true})
	h := newMetricsLifecycleHost(t, pc)

	startLifecycleShard(t, h, 1)
	if !shardSeriesExists(t, pc.Registry, name, "1") {
		t.Fatalf("(e) %s{shard_id=\"1\"} missing after StartShard", name)
	}
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard(1): %v", err)
	}
	if shardSeriesExists(t, pc.Registry, name, "1") {
		t.Fatalf("(e) %s{shard_id=\"1\"} still present after StopShard", name)
	}
}
