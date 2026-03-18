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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
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

	// warmupMaxRetries is the maximum number of retry attempts for each
	// warmup proposal during cluster setup. Transient errors (e.g. QUIC
	// stream not yet established) are retried with backoff.
	warmupMaxRetries = 3

	// warmupRetryBackoff is the delay between warmup proposal retries.
	warmupRetryBackoff = 500 * time.Millisecond
)

// benchMTLSConfig generates an ephemeral ECDSA P-256 CA and leaf certificate
// pair suitable for localhost benchmarks. The returned MTLSConfig contains
// PEM-encoded CA cert, node cert, and private key. All nodes in a cluster
// must share the same CA to verify each other, so generate this once and
// pass it to all HostConfig structs in the cluster.
func benchMTLSConfig(b *testing.B) *config.MTLSConfig {
	b.Helper()

	// Generate CA key pair.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatalf("generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft Benchmark"},
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		b.Fatalf("create CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		b.Fatalf("parse CA cert: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate leaf key and cert signed by CA.
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatalf("generate leaf key: %v", err)
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft Benchmark"},
			CommonName:   "localhost",
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:   time.Now().Add(-1 * time.Minute),
		NotAfter:    time.Now().Add(1 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		BasicConstraintsValid: true,
	}

	leafCertDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		b.Fatalf("create leaf cert: %v", err)
	}

	leafCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCertDER})
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		b.Fatalf("marshal leaf key: %v", err)
	}
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})

	return &config.MTLSConfig{
		CACert: caCertPEM,
		Cert:   leafCertPEM,
		Key:    leafKeyPEM,
	}
}

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

// reservePorts allocates free UDP ports by binding sockets and keeping them
// open until the caller is ready to use them. This avoids the TOCTOU race
// condition where a released port gets reassigned by the OS before the
// application can rebind it. The caller must close all returned sockets
// immediately before binding the same addresses.
func reservePorts(b *testing.B, n int) (addrs []string, sockets []net.PacketConn) {
	b.Helper()
	addrs = make([]string, n)
	sockets = make([]net.PacketConn, n)
	for i := 0; i < n; i++ {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			// Clean up already-opened sockets on failure.
			for j := 0; j < i; j++ {
				sockets[j].Close()
			}
			b.Fatalf("failed to reserve port %d: %v", i, err)
		}
		addrs[i] = pc.LocalAddr().String()
		sockets[i] = pc
	}
	return addrs, sockets
}

// SetupQuicRaftCluster creates a multi-node QuicRaft cluster with real
// QUIC transport and auto-generated TLS certificates. It starts one shard
// across all nodes, waits for leader election, and performs a warmup
// proposal to establish all QUIC connections and TLS handshakes.
func SetupQuicRaftCluster(b *testing.B, numNodes int) *BenchCluster {
	b.Helper()

	// Reserve UDP ports by keeping sockets open until we're ready to bind.
	// This prevents TOCTOU races where released ports get reassigned.
	addrs, sockets := reservePorts(b, numNodes)

	// Build the initial members map: replicaID (1-indexed) -> address.
	members := make(map[uint64]string, numNodes)
	for i, addr := range addrs {
		members[uint64(i+1)] = addr
	}

	// Release reserved sockets immediately before host creation so the
	// QUIC transport can bind to the same ports. The tight release-bind
	// window minimizes the TOCTOU race window.
	for _, s := range sockets {
		s.Close()
	}

	// Generate mTLS material once for the entire cluster so all nodes
	// share the same CA and can verify each other.
	mtls := benchMTLSConfig(b)

	// Create hosts with real QUIC transport (no WithoutTransport).
	hosts := make([]*quicraft.Host, numNodes)
	for i := 0; i < numNodes; i++ {
		dir := b.TempDir()
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
				Enable0RTT:         true,
			},
		}

		h, err := quicraft.NewHost(hostCfg)
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

	for i, h := range hosts {
		shardCfg := config.Config{
			ShardID:   quicraftShardID,
			ReplicaID: uint64(i + 1),
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

// warmupPropose attempts a single SyncPropose with retry logic. It retries
// up to warmupMaxRetries times with warmupRetryBackoff between attempts.
// Returns an error only if all retries are exhausted.
func warmupPropose(ctx context.Context, h *quicraft.Host, shardID uint64, cmd []byte, label string) error {
	var lastErr error
	for attempt := 0; attempt <= warmupMaxRetries; attempt++ {
		if attempt > 0 {
			retryTimer := time.NewTimer(warmupRetryBackoff)
			<-retryTimer.C
			retryTimer.Stop()
		}
		if _, err := h.SyncPropose(ctx, shardID, cmd); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("warmup %s failed after %d retries: %w", label, warmupMaxRetries+1, lastErr)
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

	// Reserve ports with sockets held open to prevent TOCTOU races.
	addrs, sockets := reservePorts(b, numNodes)

	members := make(map[uint64]string, numNodes)
	for j, addr := range addrs {
		members[uint64(j+1)] = addr
	}

	// Release sockets immediately before host creation.
	for _, s := range sockets {
		s.Close()
	}

	// Generate mTLS material once for the entire cluster so all nodes
	// share the same CA and can verify each other.
	mtls := benchMTLSConfig(b)

	// Create hosts once outside the benchmark loop. This is transport
	// infrastructure setup (TLS, QUIC, workers) and should not be
	// included in election time measurement.
	hosts := make([]*quicraft.Host, numNodes)
	for j := 0; j < numNodes; j++ {
		dir := b.TempDir()
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
				Enable0RTT:         true,
			},
		}

		h, err := quicraft.NewHost(hostCfg)
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()

		// Start shard on all hosts.
		for j, h := range hosts {
			shardCfg := config.Config{
				ShardID:   quicraftShardID,
				ReplicaID: uint64(j + 1),
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
	value := []byte("bench-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

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
	value := []byte("bench-value-0123456789abcdef")
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
			cmd := encodeKVCmd(key, value)

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
	value := []byte("commit-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

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
	value := []byte("read-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
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
	value := []byte("stale-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
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
			MTLSConfig:         benchMTLSConfig(b),
			DisableCompression: true,
			Enable0RTT:         true,
		},
	}

	h, err := quicraft.NewHost(hostCfg)
	if err != nil {
		b.Fatalf("quicraft.NewHost failed: %v", err)
	}

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	shardCfg := config.Config{
		ShardID:   quicraftShardID,
		ReplicaID: quicraftReplicaID,
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
			MTLSConfig:         benchMTLSConfig(b),
			DisableCompression: true,
			Enable0RTT:         true,
		},
	}

	h, err := quicraft.NewHost(hostCfg, quicraft.WithMemoryLogDB())
	if err != nil {
		b.Fatalf("quicraft.NewHost (memory) failed: %v", err)
	}

	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newQuicraftKVSM()
	})

	shardCfg := config.Config{
		ShardID:   quicraftShardID,
		ReplicaID: quicraftReplicaID,
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
	value := []byte("bench-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

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

// BenchQuicRaftConcurrentPropose benchmarks concurrent SyncPropose throughput.
func BenchQuicRaftConcurrentPropose(b *testing.B, h *quicraft.Host, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	value := []byte("bench-value-0123456789abcdef")

	b.ResetTimer()
	b.ReportAllocs()

	var ops atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		seq := ops.Add(1)
		key := []byte(fmt.Sprintf("key-%d", seq))
		cmd := encodeKVCmd(key, value)
		b.SetBytes(int64(len(cmd)))

		for pb.Next() {
			start := time.Now()
			_, err := h.SyncPropose(ctx, quicraftShardID, cmd)
			elapsed := time.Since(start)
			if err != nil {
				continue
			}
			rec.Record(elapsed)
		}
	})
}

// BenchQuicRaftConcurrentProposeFixed benchmarks concurrent proposals with
// a fixed number of goroutines rather than using b.RunParallel.
func BenchQuicRaftConcurrentProposeFixed(b *testing.B, h *quicraft.Host, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	value := []byte("bench-value-0123456789abcdef")
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
			cmd := encodeKVCmd(key, value)

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
	value := []byte("read-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
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
	value := []byte("stale-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
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
			MTLSConfig:         benchMTLSConfig(b),
			DisableCompression: true,
			Enable0RTT:         true,
		},
	}

	h, err := quicraft.NewHost(hostCfg)
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
			ShardID:   quicraftShardID,
			ReplicaID: quicraftReplicaID,
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
			MTLSConfig:         benchMTLSConfig(b),
			DisableCompression: true,
			Enable0RTT:         true,
		},
	}

	h, err := quicraft.NewHost(hostCfg, quicraft.WithMemoryLogDB())
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
			ShardID:   quicraftShardID,
			ReplicaID: quicraftReplicaID,
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
	value := []byte("commit-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

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

	addrs, sockets := reservePorts(b, numNodes)

	members := make(map[uint64]string, numNodes)
	for i, addr := range addrs {
		members[uint64(i+1)] = addr
	}

	for _, s := range sockets {
		s.Close()
	}

	// Generate mTLS material once for the entire cluster so all nodes
	// share the same CA and can verify each other.
	mtls := benchMTLSConfig(b)

	hosts := make([]*quicraft.Host, numNodes)
	for i := 0; i < numNodes; i++ {
		dir := b.TempDir()
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
				Enable0RTT:         true,
			},
		}

		h, err := quicraft.NewHost(hostCfg)
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

	for i, h := range hosts {
		shardCfg := config.Config{
			ShardID:   quicraftShardID,
			ReplicaID: uint64(i + 1),
			LeaseRead: true,
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
