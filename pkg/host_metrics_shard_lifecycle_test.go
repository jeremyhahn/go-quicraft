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
	"sync"
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
	calls = rc.snapshot()
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
