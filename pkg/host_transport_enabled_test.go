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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// testTransportHostConfig returns a HostConfig with the QUIC transport ENABLED
// — the production default — bound to an ephemeral loopback port and using a
// throwaway mTLS identity.
func testTransportHostConfig(t *testing.T) config.HostConfig {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.ListenAddress = "127.0.0.1:0"
	cfg.RaftAddress = "127.0.0.1:0"
	mtls := testMTLSConfig(t)
	cfg.TransportConfig.MTLSConfig = &config.MTLSConfig{
		CACert: mtls.CACert,
		Cert:   mtls.Cert,
		Key:    mtls.Key,
	}
	return cfg
}

// assertTransportHostServesShard proves the host is fully wired by driving a
// shard through it: elect, commit a proposal, and serve a linearizable read.
// Any missing wiring in NewHost's transport path (sender -> engine, message
// handler -> engine/host) shows up as a failure here rather than as a silent
// nil field.
func assertTransportHostServesShard(t *testing.T, h *Host) {
	t.Helper()
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.SyncPropose(ctx, 1, []byte("transport-enabled-write")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	got, err := h.SyncRead(ctx, 1, "transport-enabled-read")
	if err != nil {
		t.Fatalf("SyncRead: %v", err)
	}
	if got != "transport-enabled-read" {
		t.Fatalf("SyncRead = %v, want %q", got, "transport-enabled-read")
	}
}

// TestNewHost_TransportEnabledWithBarrierAndMetrics exercises the default
// production wiring that every other test in this package opts out of: a real
// QUIC transport, a host-wide at-rest barrier, and a metrics collector. It
// asserts the transport is created and started, and that a shard on this host
// can elect, commit, and serve reads through the fully wired pipeline.
func TestNewHost_TransportEnabledWithBarrierAndMetrics(t *testing.T) {
	cfg := testTransportHostConfig(t)
	barrier := createTestBarrier(t)

	h, err := NewHost(cfg,
		WithMemoryLogDB(),
		WithBarrier(barrier),
		WithMetrics(NoOpMetricsCollector{}),
	)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if h.transport == nil {
		t.Fatal("transport was not created although mTLS is configured and the transport is enabled")
	}
	ts, ok := h.sender.(*transportSender)
	if !ok {
		t.Fatalf("sender = %T, want *transportSender when the transport is enabled", h.sender)
	}
	if ts.engine == nil {
		t.Fatal("transportSender.engine was never wired; snapshot status notifications would be dropped")
	}
	if ts.barrier != Barrier(barrier) {
		t.Fatal("transportSender did not receive the host-wide barrier; snapshots could not be decrypted for send")
	}

	assertTransportHostServesShard(t, h)
}

// TestNewHost_TransportEnabledWithShardBarriers covers the per-shard barrier
// wiring on the transport path: the resolver must be installed on both the
// engine (snapshot encryption) and the transport (in-flight epoch pinning),
// and a shard the resolver does not map must fall back to the host-wide
// barrier rather than silently running unencrypted.
func TestNewHost_TransportEnabledWithShardBarriers(t *testing.T) {
	cfg := testTransportHostConfig(t)
	shardBarrier := createTestBarrier(t)
	hostBarrier := createTestBarrier(t)

	// The resolver is consulted from engine and transport goroutines, so the
	// counter must be atomic.
	var resolved atomic.Int64
	h, err := NewHost(cfg,
		WithMemoryLogDB(),
		WithBarrier(hostBarrier),
		WithShardBarriers(func(shardID uint64) Barrier {
			resolved.Add(1)
			if shardID == 1 {
				return shardBarrier
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if h.transport == nil {
		t.Fatal("transport was not created although mTLS is configured")
	}

	// barrierForShard is the host-side view of the same routing the engine
	// and transport resolvers use.
	got, err := h.barrierForShard(1)
	if err != nil {
		t.Fatalf("barrierForShard(1): %v", err)
	}
	if got != Barrier(shardBarrier) {
		t.Fatal("shard 1 did not resolve to its per-shard barrier")
	}
	got, err = h.barrierForShard(2)
	if err != nil {
		t.Fatalf("barrierForShard(2): %v", err)
	}
	if got != Barrier(hostBarrier) {
		t.Fatal("an unmapped shard did not fall back to the host-wide barrier")
	}

	assertTransportHostServesShard(t, h)

	if resolved.Load() == 0 {
		t.Fatal("the per-shard barrier resolver was never consulted")
	}
}

// TestNewHost_KernelAssignedPortIsAdvertised verifies that a host configured
// with port 0 reports, and advertises to peers, the port the kernel bound, so
// a test can create a host first and read its address afterwards (#386).
func TestNewHost_KernelAssignedPortIsAdvertised(t *testing.T) {
	cfg := testTransportHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	addr := h.RaftAddress()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("RaftAddress %q: %v", addr, err)
	}
	if host != "127.0.0.1" || port == "0" || port == "" {
		t.Fatalf("RaftAddress = %q, want 127.0.0.1 with the bound port", addr)
	}
	if bound := h.transport.Addr().String(); bound != addr {
		t.Fatalf("RaftAddress = %q, transport bound %q", addr, bound)
	}
	if got := h.transport.AdvertisedAddr(); got != addr {
		t.Fatalf("transport advertises %q, host reports %q", got, addr)
	}
	if got := h.NodeHostInfo(NodeHostInfoOption{}).RaftAddress; got != addr {
		t.Fatalf("NodeHostInfo.RaftAddress = %q, want %q", got, addr)
	}
}

// TestNewHost_FixedPortIsKept verifies a RaftAddress naming a port is
// advertised unchanged, even when the listener binds a kernel-assigned port.
func TestNewHost_FixedPortIsKept(t *testing.T) {
	cfg := testTransportHostConfig(t)
	cfg.RaftAddress = "node-a.example:4001"
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if got := h.RaftAddress(); got != "node-a.example:4001" {
		t.Fatalf("RaftAddress = %q, want the configured address", got)
	}
}
