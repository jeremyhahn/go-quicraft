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
	"runtime"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

// benchHostConfig creates a minimal HostConfig for benchmarks using b.TempDir.
func benchHostConfig(b *testing.B) config.HostConfig {
	b.Helper()
	return config.HostConfig{
		WALDir:                b.TempDir(),
		NodeHostDir:           b.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		RTTMillisecond:        1,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
}

// benchShardConfig creates a default shard config for benchmarks.
func benchShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	c.SetDefaults()
	return c
}

// benchWaitForLeader polls until the single-node raft peer self-elects
// and syncs the Host's cached leader state. Calls b.Fatal on timeout.
func benchWaitForLeader(b *testing.B, h *Host, shardID uint64) {
	b.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		engNode := h.engine.GetNode(shardID)
		if engNode != nil && engNode.Peer() != nil && engNode.Peer().IsLeader() {
			h.updateNodeLeader(shardID, engNode.Peer().LeaderID(), 0)
			return
		}
		runtime.Gosched()
	}
	b.Fatalf("timed out waiting for shard %d to elect leader", shardID)
}

// benchNewHost creates a Host with in-memory LogDB and no transport.
// The Host is cleaned up via b.Cleanup.
func benchNewHost(b *testing.B) *Host {
	b.Helper()
	cfg := benchHostConfig(b)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
	if err != nil {
		b.Fatalf("NewHost failed: %v", err)
	}
	b.Cleanup(func() { h.Close() })
	return h
}

// benchStartKVShard starts a single-node shard with the KV MemoryStore SM,
// waits for leader election, and pre-populates the given key-value pair
// through SyncPropose so it is available for reads.
func benchStartKVShard(b *testing.B, h *Host, shardID uint64, key, value []byte) {
	b.Helper()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := benchShardConfig(shardID, 1)
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
		b.Fatalf("StartShard failed: %v", err)
	}

	benchWaitForLeader(b, h, shardID)

	// Pre-populate the key if provided.
	if key != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd, err := kv.EncodePut(key, value)
		if err != nil {
			b.Fatalf("EncodePut failed: %v", err)
		}
		if _, err := h.SyncPropose(ctx, shardID, cmd); err != nil {
			b.Fatalf("SyncPropose (seed) failed: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// BenchmarkGetNoOPSession
// ---------------------------------------------------------------------------

// BenchmarkGetNoOPSession measures the cost of allocating a no-op session.
// GetNoOPSession creates a lightweight Session struct with a fixed clientID
// of 0. This is a pure allocation benchmark with no engine interaction.
func BenchmarkGetNoOPSession(b *testing.B) {
	h := benchNewHost(b)

	const shardID uint64 = 1
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := benchShardConfig(shardID, 1)
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
		b.Fatalf("StartShard failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = h.GetNoOPSession(shardID)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkGetLeaderID
// ---------------------------------------------------------------------------

// BenchmarkGetLeaderID measures the cost of reading the cached leader ID.
// After election, GetLeaderID performs an atomic load and a map lookup,
// expected to complete in the nanosecond range.
func BenchmarkGetLeaderID(b *testing.B) {
	h := benchNewHost(b)

	const shardID uint64 = 1
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := benchShardConfig(shardID, 1)
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
		b.Fatalf("StartShard failed: %v", err)
	}

	benchWaitForLeader(b, h, shardID)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = h.GetLeaderID(shardID)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkStaleRead
// ---------------------------------------------------------------------------

// BenchmarkStaleRead measures the fastest read path: a direct SM Lookup
// with no consensus. The benchmark pre-populates a key, then measures
// repeated StaleRead calls. This exercises the Host node lookup and
// RSM Lookup path.
func BenchmarkStaleRead(b *testing.B) {
	h := benchNewHost(b)

	const shardID uint64 = 1
	key := []byte("bench-key")
	value := []byte("bench-value")
	benchStartKVShard(b, h, shardID, key, value)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = h.StaleRead(ctx, shardID, key)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkSyncPropose
// ---------------------------------------------------------------------------

// BenchmarkSyncPropose measures the full Raft proposal pipeline for a
// single-node cluster: propose -> step -> commit -> apply -> callback.
// Uses a small KV Put payload to minimize serialization overhead and
// focus on the engine round-trip latency.
func BenchmarkSyncPropose(b *testing.B) {
	h := benchNewHost(b)

	const shardID uint64 = 1
	benchStartKVShard(b, h, shardID, nil, nil)

	cmd, err := kv.EncodePut([]byte("k"), []byte("v"))
	if err != nil {
		b.Fatalf("EncodePut failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = h.SyncPropose(ctx, shardID, cmd)
		cancel()
	}
}

// ---------------------------------------------------------------------------
// BenchmarkSyncRead
// ---------------------------------------------------------------------------

// BenchmarkSyncRead measures the linearizable read path: ReadIndex
// confirmation via heartbeat quorum (single-node self-ack), wait for
// SM catch-up, then Lookup. This exercises the full ReadIndex pipeline.
func BenchmarkSyncRead(b *testing.B) {
	h := benchNewHost(b)

	const shardID uint64 = 1
	key := []byte("bench-key")
	value := []byte("bench-value")
	benchStartKVShard(b, h, shardID, key, value)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = h.SyncRead(ctx, shardID, key)
		cancel()
	}
}
