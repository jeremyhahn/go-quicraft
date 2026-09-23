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

package quicraft_perf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/performance/benchutil"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const (
	// quicraftShardID is the shard ID used for single-node benchmarks.
	quicraftShardID uint64 = 1

	// quicraftReplicaID is the replica ID used for single-node benchmarks.
	quicraftReplicaID uint64 = 1

	// quicraftAddr is the address used for the single-node benchmark host.
	quicraftAddr = "127.0.0.1:0"

	// quicraftRTT is the RTT in milliseconds for benchmarks.
	quicraftRTT uint64 = 1

	// quicraftElectionWait is the maximum time to wait for leader election.
	quicraftElectionWait = 10 * time.Second

	// quicraftClusterElectionWait is the maximum time to wait for leader
	// election in multi-node clusters with real QUIC transport. Set to
	// 60s to accommodate Docker and CI environments where scheduling
	// latency can delay the first election round.
	quicraftClusterElectionWait = 60 * time.Second

	// commitLatencyWarmup is the number of unmeasured proposals run before
	// the commit-latency measurement loop, so the first timed proposal does
	// not include one-time pipeline initialization. Shared across all engines.
	commitLatencyWarmup = 100
)

// BenchCluster holds a multi-node QuicRaft cluster for benchmarks.
type BenchCluster struct {
	Hosts     []*quicraft.Host
	Addrs     []string
	LeaderIdx int
}

// Close shuts down all hosts in the cluster.
func (c *BenchCluster) Close() {
	for _, h := range c.Hosts {
		h.Close()
	}
	// Brief pause to let QUIC listeners and UDP sockets fully release.
	// Without this, the next benchmark's cluster setup may hit port conflicts
	// or stale connection state from the previous cluster.
	drainTimer := time.NewTimer(500 * time.Millisecond)
	<-drainTimer.C
	drainTimer.Stop()
}

// Leader returns the host that was elected leader during setup.
func (c *BenchCluster) Leader() *quicraft.Host {
	return c.Hosts[c.LeaderIdx]
}

// reservePorts returns n loopback addresses for hosts that do not exist yet.
//
// It used to bind 127.0.0.1:0 for each and hold the socket open until the
// caller was ready, closing them "immediately before" the transport bound the
// same addresses. That NARROWED the race rather than removing it: the addresses
// still came from the kernel's ephemeral range, which is the same pool the
// kernel draws from for the local end of every outbound connection, so a
// transport still running from the previous benchmark could be handed one in
// the close-then-bind window.
//
// test/netport removes the class instead: it allocates from a window OUTSIDE
// the ephemeral range, which the kernel never auto-assigns, so there is no
// window to be tight about and no socket for the caller to release (#163).
func reservePorts(b *testing.B, n int) []string {
	b.Helper()
	addrs := make([]string, n)
	for i := range n {
		addrs[i] = netport.MustReserveUDP(b.Fatalf)
	}
	return addrs
}

// SetupQuicRaftCluster creates a multi-node QuicRaft cluster with real
// QUIC transport and auto-generated TLS certificates. It starts one shard
// across all nodes, waits for leader election, and performs a warmup
// proposal to establish all QUIC connections and TLS handshakes.
func SetupQuicRaftCluster(b *testing.B, numNodes int) *BenchCluster {
	b.Helper()

	addrs := reservePorts(b, numNodes)

	// Build the initial members map: replicaID (1-indexed) -> address.
	members := make(map[uint64]string, numNodes)
	for i, addr := range addrs {
		members[uint64(i+1)] = addr
	}

	// One CA for the cluster so the nodes verify each other, one leaf per
	// node: a host with a transport is admitted by its node identity, which
	// is its leaf's key, so the leaves must differ and the founding map must
	// name each one (Config.MemberIdentities).
	ca := testpki.NewCA(b)

	// Create hosts with real QUIC transport (no WithoutTransport).
	hosts := make([]*quicraft.Host, numNodes)
	for i := 0; i < numNodes; i++ {
		dir := b.TempDir()
		mtls := ca.Leaf(b)
		hostCfg := config.HostConfig{
			WALDir:                dir + "/wal",
			NodeHostDir:           dir + "/data",
			RaftAddress:           addrs[i],
			ListenAddress:         addrs[i],
			RTTMillisecond:        quicraftRTT,
			DeploymentID:          1,
			AllowZeroDeploymentID: false,
			NumWorkers:            4,
			MaxApplyWorkers:       4,
			TransportConfig: config.TransportConfig{
				MTLSConfig:         mtls,
				DisableCompression: true,
			},
		}

		h, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
		if err != nil {
			// Clean up already-created hosts on failure.
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			b.Fatalf("quicraft.NewHost[%d] failed: %v", i, err)
		}
		hosts[i] = h
	}

	// Start the shard on all hosts.
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	identities := make(map[uint64]string, numNodes)
	for i, h := range hosts {
		identities[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range hosts {
		shardCfg := config.Config{
			ShardID:          quicraftShardID,
			ReplicaID:        uint64(i + 1),
			MemberIdentities: identities,
		}
		shardCfg.SetDefaults()

		if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
			for _, hh := range hosts {
				hh.Close()
			}
			b.Fatalf("StartShard[%d] failed: %v", i, err)
		}
	}

	// Wait for leader election.
	leaderIdx, err := waitForClusterLeader(hosts, quicraftShardID, quicraftClusterElectionWait)
	if err != nil {
		for _, h := range hosts {
			h.Close()
		}
		b.Fatalf("cluster leader election timed out: %v", err)
	}

	// Verify the cluster is ready to accept proposals with retry-tolerant
	// warmup. This establishes all peer QUIC connections and ensures the
	// leader can process proposals before the benchmark timer starts.
	verifyClusterReady(b, hosts, leaderIdx, quicraftShardID)

	return &BenchCluster{
		Hosts:     hosts,
		Addrs:     addrs,
		LeaderIdx: leaderIdx,
	}
}

// waitForClusterLeader polls all hosts until one reports itself as leader
// for the given shard, or the timeout expires. On timeout, it returns
// diagnostic information about each node's last observed state.
func waitForClusterLeader(hosts []*quicraft.Host, shardID uint64, timeout time.Duration) (int, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var lastErrors []string
	for {
		lastErrors = lastErrors[:0]
		for i, h := range hosts {
			info, err := h.GetShardInfo(shardID)
			if err == nil && info.IsLeader {
				return i, nil
			}
			if err != nil {
				lastErrors = append(lastErrors, fmt.Sprintf("node %d: %v", i, err))
			} else {
				lastErrors = append(lastErrors, fmt.Sprintf("node %d: leader=%d isLeader=%v", i, info.LeaderID, info.IsLeader))
			}
		}
		select {
		case <-deadline.C:
			return -1, fmt.Errorf("no leader elected within %v; last state: %v", timeout, lastErrors)
		case <-ticker.C:
		}
	}
}

// warmupPropose attempts a single SyncPropose with the shared warmup retry
// budget (benchutil.WarmupAttempts attempts, benchutil.WarmupBackoff between
// attempts). Returns an error only if all attempts are exhausted.
func warmupPropose(ctx context.Context, h *quicraft.Host, shardID uint64, cmd []byte, label string) error {
	err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
		_, perr := h.SyncPropose(ctx, shardID, cmd)
		return perr
	})
	if err != nil {
		return fmt.Errorf("warmup %s failed after %d attempts: %w", label, benchutil.WarmupAttempts, err)
	}
	return nil
}

// verifyClusterReady performs post-election readiness verification by
// running a series of proposals with retry tolerance. This ensures the
// leader can actually process proposals and all peer QUIC connections
// are fully established before the benchmark timer starts.
func verifyClusterReady(b *testing.B, hosts []*quicraft.Host, leaderIdx int, shardID uint64) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		warmupCmd := encodeKVCmd([]byte(fmt.Sprintf("warmup-%d", i)), []byte("ok"))
		label := fmt.Sprintf("proposal %d on node %d", i, leaderIdx)
		if err := warmupPropose(ctx, hosts[leaderIdx], shardID, warmupCmd, label); err != nil {
			for _, h := range hosts {
				h.Close()
			}
			b.Fatalf("cluster readiness verification failed: %v", err)
		}
	}

	// Warm up ReadIndex path so the first benchmark read does not
	// include one-time initialization overhead.
	if _, err := hosts[leaderIdx].SyncRead(ctx, shardID, []byte("warmup-0")); err != nil {
		// SyncRead may return a "key not found" type error from the SM
		// lookup, which is fine. Only fail on infrastructure errors that
		// indicate the ReadIndex path itself is broken. Log and continue.
		b.Logf("warmup SyncRead (non-fatal): %v", err)
	}

	// Brief pause to let the pipeline settle after warmup. This allows
	// in-flight heartbeats and acks to complete so the first benchmark
	// iteration does not include pipeline drain latency.
	settleTimer := time.NewTimer(50 * time.Millisecond)
	<-settleTimer.C
	settleTimer.Stop()
}

// BenchQuicRaftElectionCluster benchmarks leader election time for a
// multi-node cluster with real QUIC transport. Host infrastructure (TLS
// cert generation, QUIC transport, worker pools) is created once outside
// the timed loop. Each iteration starts shards on all hosts, waits for
// leader election, records the elapsed time, and stops shards.
func BenchQuicRaftElectionCluster(b *testing.B, numNodes int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	addrs := reservePorts(b, numNodes)

	members := make(map[uint64]string, numNodes)
	for j, addr := range addrs {
		members[uint64(j+1)] = addr
	}

	// Generate mTLS material once for the entire cluster so all nodes
	// share the same CA and can verify each other.
	ca := testpki.NewCA(b)

	// Create hosts once outside the benchmark loop. This is transport
	// infrastructure setup (TLS, QUIC, workers) and should not be
	// included in election time measurement.
	hosts := make([]*quicraft.Host, numNodes)
	for j := 0; j < numNodes; j++ {
		dir := b.TempDir()
		mtls := ca.Leaf(b)
		hostCfg := config.HostConfig{
			WALDir:                dir + "/wal",
			NodeHostDir:           dir + "/data",
			RaftAddress:           addrs[j],
			ListenAddress:         addrs[j],
			RTTMillisecond:        quicraftRTT,
			DeploymentID:          1,
			AllowZeroDeploymentID: false,
			NumWorkers:            4,
			MaxApplyWorkers:       4,
			TransportConfig: config.TransportConfig{
				MTLSConfig:         mtls,
				DisableCompression: true,
			},
		}

		h, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
		if err != nil {
			for k := 0; k < j; k++ {
				hosts[k].Close()
			}
			b.Fatalf("NewHost[%d] failed: %v", j, err)
		}
		hosts[j] = h
	}
	defer func() {
		for _, h := range hosts {
			h.Close()
		}
	}()

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})
	identities := make(map[uint64]string, numNodes)
	for j, h := range hosts {
		identities[uint64(j+1)] = h.NodeIdentity()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()

		// Start shard on all hosts.
		for j, h := range hosts {
			shardCfg := config.Config{
				ShardID:          quicraftShardID,
				ReplicaID:        uint64(j + 1),
				MemberIdentities: identities,
			}
			shardCfg.SetDefaults()

			if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
				b.Fatalf("StartShard[%d] failed at iteration %d: %v", j, i, err)
			}
		}

		// Wait for leader.
		if _, err := waitForClusterLeader(hosts, quicraftShardID, quicraftClusterElectionWait); err != nil {
			b.Fatalf("cluster leader election timed out at iteration %d: %v", i, err)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		// Stop shards on all hosts before the next iteration.
		for j, h := range hosts {
			if err := h.StopShard(quicraftShardID); err != nil {
				b.Fatalf("StopShard[%d] failed at iteration %d: %v", j, i, err)
			}
		}
	}
}

// BenchQuicRaftProposeCluster benchmarks sequential SyncPropose throughput
// on a multi-node cluster with real QUIC transport. Per-iteration errors
// are logged and counted rather than fatal, matching the concurrent variant.
func BenchQuicRaftProposeCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	leader := cluster.Leader()
	key := []byte("bench-key")
	cmd := makeKVCmd(key)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(cmd)))

	var errCount atomic.Int64
	for i := 0; i < b.N; i++ {
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
		start := time.Now()
		_, err := leader.SyncPropose(proposeCtx, quicraftShardID, cmd)
		elapsed := time.Since(start)
		proposeCancel()
		if err != nil {
			errCount.Add(1)
			b.Logf("SyncPropose failed at iteration %d on leader node %d: %v", i, cluster.LeaderIdx, err)
			continue
		}
		rec.Record(elapsed)
	}
	if n := errCount.Load(); n > 0 {
		b.Logf("total propose errors: %d/%d", n, b.N)
	}
}

// BenchQuicRaftConcurrentProposeFixedCluster benchmarks concurrent proposals
// with a fixed number of goroutines on a multi-node cluster. Per-goroutine
// errors are logged with node context and counted rather than fatal.
func BenchQuicRaftConcurrentProposeFixedCluster(b *testing.B, cluster *BenchCluster, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	leader := cluster.Leader()
	opsPerGoroutine := b.N / numGoroutines
	if opsPerGoroutine < 1 {
		opsPerGoroutine = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	var errCount atomic.Int64
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			key := []byte(fmt.Sprintf("key-%d", id))
			cmd := makeKVCmd(key)

			for j := 0; j < opsPerGoroutine; j++ {
				proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
				start := time.Now()
				_, err := leader.SyncPropose(proposeCtx, quicraftShardID, cmd)
				elapsed := time.Since(start)
				proposeCancel()
				if err != nil {
					errCount.Add(1)
					continue
				}
				rec.Record(elapsed)
			}
		}(g)
	}

	wg.Wait()
	if n := errCount.Load(); n > 0 {
		b.Logf("total concurrent propose errors: %d/%d ops (%d goroutines)", n, b.N, numGoroutines)
	}
}

// BenchQuicRaftCommitLatencyCluster benchmarks per-proposal commit latency
// on a multi-node cluster with real QUIC transport.
func BenchQuicRaftCommitLatencyCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	leader := cluster.Leader()
	key := []byte("commit-key")
	cmd := makeKVCmd(key)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Warm up the pipeline (same count and retry budget as all engines)
	// before measuring. Transient errors are retried so a single startup
	// flake cannot discard the whole suite.
	for w := 0; w < commitLatencyWarmup; w++ {
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			warmCtx, warmCancel := context.WithTimeout(ctx, 10*time.Second)
			defer warmCancel()
			_, perr := leader.SyncPropose(warmCtx, quicraftShardID, cmd)
			return perr
		})
		if err != nil {
			b.Fatalf("warmup propose failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var errCount atomic.Int64
	for i := 0; i < b.N; i++ {
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
		start := time.Now()
		_, err := leader.SyncPropose(proposeCtx, quicraftShardID, cmd)
		elapsed := time.Since(start)
		proposeCancel()
		if err != nil {
			errCount.Add(1)
			b.Logf("SyncPropose failed at iteration %d: %v", i, err)
			continue
		}
		rec.Record(elapsed)
	}
	if n := errCount.Load(); n > 0 {
		b.Logf("total commit latency errors: %d/%d", n, b.N)
	}
}

// BenchQuicRaftSyncReadCluster benchmarks linearizable read throughput
// on a multi-node cluster with real QUIC transport.
func BenchQuicRaftSyncReadCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	leader := cluster.Leader()

	// Seed data.
	key := []byte("read-key")
	cmd := makeKVCmd(key)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := leader.SyncPropose(seedCtx, quicraftShardID, cmd); err != nil {
		seedCancel()
		b.Fatalf("seed propose failed: %v", err)
	}
	seedCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	b.ResetTimer()
	b.ReportAllocs()

	var errCount atomic.Int64
	for i := 0; i < b.N; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		start := time.Now()
		_, err := leader.SyncRead(readCtx, quicraftShardID, key)
		elapsed := time.Since(start)
		readCancel()
		if err != nil {
			errCount.Add(1)
			b.Logf("SyncRead failed at iteration %d: %v", i, err)
			continue
		}
		rec.Record(elapsed)
	}
	if n := errCount.Load(); n > 0 {
		b.Logf("total sync read errors: %d/%d", n, b.N)
	}
}

// BenchQuicRaftStaleReadCluster benchmarks stale (non-linearizable) read
// throughput on a multi-node cluster with real QUIC transport.
func BenchQuicRaftStaleReadCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	leader := cluster.Leader()

	// Seed data.
	key := []byte("stale-key")
	cmd := makeKVCmd(key)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := leader.SyncPropose(seedCtx, quicraftShardID, cmd); err != nil {
		seedCancel()
		b.Fatalf("seed propose failed: %v", err)
	}
	seedCancel()

	b.ResetTimer()
	b.ReportAllocs()

	var errCount atomic.Int64
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := leader.StaleRead(ctx, quicraftShardID, key)
		elapsed := time.Since(start)
		if err != nil {
			errCount.Add(1)
			b.Logf("StaleRead failed at iteration %d: %v", i, err)
			continue
		}
		rec.Record(elapsed)
	}
	if n := errCount.Load(); n > 0 {
		b.Logf("total stale read errors: %d/%d", n, b.N)
	}
}

// SetupQuicRaftHost creates a single-node QuicRaft host with one shard
// running the KV state machine. It returns the host and a cleanup
// function that must be called when the benchmark completes.
func SetupQuicRaftHost(b *testing.B) (*quicraft.Host, func()) {
	b.Helper()

	dir := b.TempDir()

	hostCfg := config.HostConfig{
		WALDir:                dir + "/wal",
		NodeHostDir:           dir + "/data",
		RaftAddress:           quicraftAddr,
		ListenAddress:         quicraftAddr,
		RTTMillisecond:        quicraftRTT,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            4,
		MaxApplyWorkers:       4,
		TransportConfig: config.TransportConfig{
			MTLSConfig:         testpki.NewCA(b).Leaf(b),
			DisableCompression: true,
		},
	}

	h, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
	if err != nil {
		b.Fatalf("quicraft.NewHost failed: %v", err)
	}

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	shardCfg := config.Config{
		ShardID:          quicraftShardID,
		ReplicaID:        quicraftReplicaID,
		MemberIdentities: map[uint64]string{quicraftReplicaID: h.NodeIdentity()},
	}
	shardCfg.SetDefaults()

	members := map[uint64]string{
		quicraftReplicaID: quicraftAddr,
	}

	if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
		h.Close()
		b.Fatalf("StartShard failed: %v", err)
	}

	// Wait for leader election.
	if err := waitForQuicRaftLeader(h, quicraftShardID, quicraftElectionWait); err != nil {
		h.Close()
		b.Fatalf("leader election timed out: %v", err)
	}

	cleanup := func() {
		h.Close()
	}
	return h, cleanup
}

// SetupQuicRaftHostMemory creates a single-node QuicRaft host with one shard
// running the KV state machine using an in-memory LogDB. It returns the host
// and a cleanup function that must be called when the benchmark completes.
func SetupQuicRaftHostMemory(b *testing.B) (*quicraft.Host, func()) {
	b.Helper()
	dir := b.TempDir()

	hostCfg := config.HostConfig{
		WALDir:                dir + "/wal",
		NodeHostDir:           dir + "/data",
		RaftAddress:           quicraftAddr,
		ListenAddress:         quicraftAddr,
		RTTMillisecond:        quicraftRTT,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            4,
		MaxApplyWorkers:       4,
		TransportConfig: config.TransportConfig{
			MTLSConfig:         testpki.NewCA(b).Leaf(b),
			DisableCompression: true,
		},
	}

	h, err := quicraft.NewHost(hostCfg, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())
	if err != nil {
		b.Fatalf("quicraft.NewHost (memory) failed: %v", err)
	}

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	shardCfg := config.Config{
		ShardID:          quicraftShardID,
		ReplicaID:        quicraftReplicaID,
		MemberIdentities: map[uint64]string{quicraftReplicaID: h.NodeIdentity()},
	}
	shardCfg.SetDefaults()

	members := map[uint64]string{
		quicraftReplicaID: quicraftAddr,
	}

	if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
		h.Close()
		b.Fatalf("StartShard (memory) failed: %v", err)
	}

	if err := waitForQuicRaftLeader(h, quicraftShardID, quicraftElectionWait); err != nil {
		h.Close()
		b.Fatalf("leader election (memory) timed out: %v", err)
	}

	cleanup := func() {
		h.Close()
	}
	return h, cleanup
}

// waitForQuicRaftLeader polls until a leader is elected or the timeout expires.
func waitForQuicRaftLeader(h *quicraft.Host, shardID uint64, timeout time.Duration) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		leaderID, ok, err := h.GetLeaderID(shardID)
		if err == nil && ok && leaderID > 0 {
			return nil
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("no leader elected within %v", timeout)
		case <-ticker.C:
		}
	}
}

// BenchQuicRaftPropose benchmarks sequential SyncPropose throughput.
func BenchQuicRaftPropose(b *testing.B, h *quicraft.Host, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()
	key := []byte("bench-key")
	cmd := makeKVCmd(key)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(cmd)))

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := h.SyncPropose(ctx, quicraftShardID, cmd)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("SyncPropose failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchQuicRaftConcurrentProposeFixed benchmarks concurrent proposals with
// a fixed number of goroutines rather than using b.RunParallel, so the
// concurrency level is identical across all benchmarked engines.
func BenchQuicRaftConcurrentProposeFixed(b *testing.B, h *quicraft.Host, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	opsPerGoroutine := b.N / numGoroutines
	if opsPerGoroutine < 1 {
		opsPerGoroutine = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			key := []byte(fmt.Sprintf("key-%d", id))
			cmd := makeKVCmd(key)

			for j := 0; j < opsPerGoroutine; j++ {
				start := time.Now()
				_, err := h.SyncPropose(ctx, quicraftShardID, cmd)
				elapsed := time.Since(start)
				if err != nil {
					continue
				}
				rec.Record(elapsed)
			}
		}(g)
	}

	wg.Wait()
}

// BenchQuicRaftSyncRead benchmarks linearizable read throughput.
func BenchQuicRaftSyncRead(b *testing.B, h *quicraft.Host, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()

	// Seed data.
	key := []byte("read-key")
	cmd := makeKVCmd(key)
	if _, err := h.SyncPropose(ctx, quicraftShardID, cmd); err != nil {
		b.Fatalf("seed propose failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := h.SyncRead(ctx, quicraftShardID, key)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("SyncRead failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchQuicRaftStaleRead benchmarks stale (non-linearizable) read throughput.
func BenchQuicRaftStaleRead(b *testing.B, h *quicraft.Host, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()

	// Seed data.
	key := []byte("stale-key")
	cmd := makeKVCmd(key)
	if _, err := h.SyncPropose(ctx, quicraftShardID, cmd); err != nil {
		b.Fatalf("seed propose failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := h.StaleRead(ctx, quicraftShardID, key)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("StaleRead failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchQuicRaftElection benchmarks the Raft election algorithm time,
// excluding Host infrastructure setup (TLS cert generation, QUIC transport,
// worker pool creation). The Host is created once outside the timed loop;
// only StartShard + waitForLeader is measured per iteration.
func BenchQuicRaftElection(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	dir := b.TempDir()
	hostCfg := config.HostConfig{
		WALDir:                dir + "/wal",
		NodeHostDir:           dir + "/data",
		RaftAddress:           quicraftAddr,
		ListenAddress:         quicraftAddr,
		RTTMillisecond:        quicraftRTT,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            4,
		MaxApplyWorkers:       4,
		TransportConfig: config.TransportConfig{
			MTLSConfig:         testpki.NewCA(b).Leaf(b),
			DisableCompression: true,
		},
	}

	h, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
	if err != nil {
		b.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	members := map[uint64]string{
		quicraftReplicaID: quicraftAddr,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		shardCfg := config.Config{
			ShardID:          quicraftShardID,
			ReplicaID:        quicraftReplicaID,
			MemberIdentities: map[uint64]string{quicraftReplicaID: h.NodeIdentity()},
		}
		shardCfg.SetDefaults()

		start := time.Now()

		if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
			b.Fatalf("StartShard failed at iteration %d: %v", i, err)
		}

		if err := waitForQuicRaftLeader(h, quicraftShardID, quicraftElectionWait); err != nil {
			b.Fatalf("leader election timed out at iteration %d: %v", i, err)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		if err := h.StopShard(quicraftShardID); err != nil {
			b.Fatalf("StopShard failed at iteration %d: %v", i, err)
		}
	}
}

// BenchQuicRaftElectionMemory benchmarks the Raft election algorithm time
// using an in-memory LogDB, excluding Host infrastructure setup (TLS cert
// generation, QUIC transport, worker pool creation). The Host is created
// once outside the timed loop; only StartShard + waitForLeader is measured
// per iteration.
func BenchQuicRaftElectionMemory(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	dir := b.TempDir()
	hostCfg := config.HostConfig{
		WALDir:                dir + "/wal",
		NodeHostDir:           dir + "/data",
		RaftAddress:           quicraftAddr,
		ListenAddress:         quicraftAddr,
		RTTMillisecond:        quicraftRTT,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            4,
		MaxApplyWorkers:       4,
		TransportConfig: config.TransportConfig{
			MTLSConfig:         testpki.NewCA(b).Leaf(b),
			DisableCompression: true,
		},
	}

	h, err := quicraft.NewHost(hostCfg, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())
	if err != nil {
		b.Fatalf("NewHost (memory) failed: %v", err)
	}
	defer h.Close()

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	members := map[uint64]string{
		quicraftReplicaID: quicraftAddr,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		shardCfg := config.Config{
			ShardID:          quicraftShardID,
			ReplicaID:        quicraftReplicaID,
			MemberIdentities: map[uint64]string{quicraftReplicaID: h.NodeIdentity()},
		}
		shardCfg.SetDefaults()

		start := time.Now()

		if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
			b.Fatalf("StartShard (memory) failed at iteration %d: %v", i, err)
		}

		if err := waitForQuicRaftLeader(h, quicraftShardID, quicraftElectionWait); err != nil {
			b.Fatalf("leader election (memory) timed out at iteration %d: %v", i, err)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		if err := h.StopShard(quicraftShardID); err != nil {
			b.Fatalf("StopShard (memory) failed at iteration %d: %v", i, err)
		}
	}
}

// BenchQuicRaftCommitLatency benchmarks per-proposal commit latency.
// Unlike BenchQuicRaftPropose, this does not set b.SetBytes because it
// measures latency rather than throughput.
func BenchQuicRaftCommitLatency(b *testing.B, h *quicraft.Host, rec *perfresult.LatencyRecorder) {
	b.Helper()
	ctx := context.Background()
	key := []byte("commit-key")
	cmd := makeKVCmd(key)

	// Warm up the pipeline so the first measured proposal excludes one-time
	// initialization, matching the other engines' commit-latency benchmarks.
	for w := 0; w < commitLatencyWarmup; w++ {
		if err := warmupPropose(ctx, h, quicraftShardID, cmd, "commit latency"); err != nil {
			b.Fatalf("warmup propose failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := h.SyncPropose(ctx, quicraftShardID, cmd)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("SyncPropose failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// SetupQuicRaftClusterLeaseRead creates a multi-node QuicRaft cluster
// identical to SetupQuicRaftCluster but with LeaseRead enabled. This
// shows the incremental benefit of lease-based reads over the standard
// heartbeat-based ReadIndex used by other systems.
func SetupQuicRaftClusterLeaseRead(b *testing.B, numNodes int) *BenchCluster {
	b.Helper()

	addrs := reservePorts(b, numNodes)

	members := make(map[uint64]string, numNodes)
	for i, addr := range addrs {
		members[uint64(i+1)] = addr
	}

	// One CA for the cluster, one leaf (identity) per node.
	ca := testpki.NewCA(b)

	hosts := make([]*quicraft.Host, numNodes)
	for i := 0; i < numNodes; i++ {
		dir := b.TempDir()
		mtls := ca.Leaf(b)
		hostCfg := config.HostConfig{
			WALDir:                dir + "/wal",
			NodeHostDir:           dir + "/data",
			RaftAddress:           addrs[i],
			ListenAddress:         addrs[i],
			RTTMillisecond:        quicraftRTT,
			DeploymentID:          1,
			AllowZeroDeploymentID: false,
			NumWorkers:            4,
			MaxApplyWorkers:       4,
			TransportConfig: config.TransportConfig{
				MTLSConfig:         mtls,
				DisableCompression: true,
			},
		}

		h, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			b.Fatalf("quicraft.NewHost[%d] failed: %v", i, err)
		}
		hosts[i] = h
	}

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	identities := make(map[uint64]string, numNodes)
	for i, h := range hosts {
		identities[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range hosts {
		shardCfg := config.Config{
			ShardID:          quicraftShardID,
			ReplicaID:        uint64(i + 1),
			LeaseRead:        true,
			MemberIdentities: identities,
		}
		shardCfg.SetDefaults()

		if err := h.StartShard(members, false, createFn, shardCfg); err != nil {
			for _, hh := range hosts {
				hh.Close()
			}
			b.Fatalf("StartShard[%d] failed: %v", i, err)
		}
	}

	leaderIdx, err := waitForClusterLeader(hosts, quicraftShardID, quicraftClusterElectionWait)
	if err != nil {
		for _, h := range hosts {
			h.Close()
		}
		b.Fatalf("cluster leader election timed out: %v", err)
	}

	// Verify the cluster is ready to accept proposals with retry-tolerant
	// warmup. This establishes all peer QUIC connections and ensures the
	// leader can process proposals before the benchmark timer starts.
	verifyClusterReady(b, hosts, leaderIdx, quicraftShardID)

	return &BenchCluster{
		Hosts:     hosts,
		Addrs:     addrs,
		LeaderIdx: leaderIdx,
	}
}
