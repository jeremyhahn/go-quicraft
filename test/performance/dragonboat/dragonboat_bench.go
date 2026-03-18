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

package dragonboat_perf

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
	"github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

// LeaderElectionTimeoutError is returned when no leader is elected within
// the specified timeout duration.
type LeaderElectionTimeoutError struct {
	Timeout time.Duration
}

// Error implements the error interface.
func (e *LeaderElectionTimeoutError) Error() string {
	return "no leader elected within " + e.Timeout.String()
}

const (
	// dbShardID is the shard ID used for Dragonboat single-node benchmarks.
	dbShardID uint64 = 1

	// dbReplicaID is the replica ID for the single-node benchmark.
	dbReplicaID uint64 = 1

	// dbAddr is the address for the single-node Dragonboat host.
	// Dragonboat does not support port 0 (auto-assign), so we use a
	// fixed port. The election benchmark creates multiple hosts
	// sequentially so reuse is safe.
	dbAddr = "127.0.0.1:63100"

	// dbRTT is the RTT in milliseconds for Dragonboat benchmarks.
	dbRTT uint64 = 1

	// dbElectionWait is the maximum time to wait for leader election.
	dbElectionWait = 10 * time.Second
)

// SetupDragonboatHost creates a single-node Dragonboat NodeHost with one
// shard running the KV state machine. It returns the host and a cleanup
// function that must be called when the benchmark completes.
func SetupDragonboatHost(b *testing.B) (*dragonboat.NodeHost, func()) {
	b.Helper()

	dir := b.TempDir()

	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0755); err != nil {
		b.Fatalf("create TLS dir: %v", err)
	}
	certs := generateTLSCerts(b, tlsDir, 1)

	nhCfg := dbconfig.NodeHostConfig{
		WALDir:         dir + "/wal",
		NodeHostDir:    dir + "/data",
		RaftAddress:    dbAddr,
		RTTMillisecond: dbRTT,
		MutualTLS:      true,
		CAFile:         certs[0].CAFile,
		CertFile:       certs[0].CertFile,
		KeyFile:        certs[0].KeyFile,
	}

	nh, err := dragonboat.NewNodeHost(nhCfg)
	if err != nil {
		b.Fatalf("dragonboat.NewNodeHost failed: %v", err)
	}

	raftCfg := dbconfig.Config{
		ReplicaID:    dbReplicaID,
		ShardID:      dbShardID,
		ElectionRTT:  10,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}

	members := map[uint64]dragonboat.Target{
		dbReplicaID: dbAddr,
	}

	createFn := func(shardID, replicaID uint64) dbsm.IStateMachine {
		return newDragonboatKVSM()
	}

	if err := nh.StartReplica(members, false, createFn, raftCfg); err != nil {
		nh.Close()
		b.Fatalf("StartReplica failed: %v", err)
	}

	// Wait for leader election.
	if err := waitForDragonboatLeader(nh, dbShardID, dbElectionWait); err != nil {
		nh.Close()
		b.Fatalf("leader election timed out: %v", err)
	}

	cleanup := func() {
		nh.Close()
	}
	return nh, cleanup
}

// waitForDragonboatLeader polls until a leader is elected or timeout expires.
func waitForDragonboatLeader(nh *dragonboat.NodeHost, shardID uint64, timeout time.Duration) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		leaderID, _, ok, err := nh.GetLeaderID(shardID)
		if err == nil && ok && leaderID > 0 {
			return nil
		}
		select {
		case <-deadline.C:
			return &LeaderElectionTimeoutError{Timeout: timeout}
		case <-ticker.C:
		}
	}
}

// BenchDragonboatPropose benchmarks sequential SyncPropose throughput.
func BenchDragonboatPropose(b *testing.B, nh *dragonboat.NodeHost, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session := nh.GetNoOPSession(dbShardID)
	key := []byte("bench-key")
	value := []byte("bench-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(cmd)))

	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := nh.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("SyncPropose failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchDragonboatConcurrentPropose benchmarks concurrent SyncPropose throughput.
func BenchDragonboatConcurrentPropose(b *testing.B, nh *dragonboat.NodeHost, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	value := []byte("bench-value-0123456789abcdef")

	b.ResetTimer()
	b.ReportAllocs()

	var ops atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		session := nh.GetNoOPSession(dbShardID)
		seq := ops.Add(1)
		key := []byte(fmt.Sprintf("key-%d", seq))
		cmd := encodeKVCmd(key, value)
		b.SetBytes(int64(len(cmd)))

		for pb.Next() {
			start := time.Now()
			proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := nh.SyncPropose(proposeCtx, session, cmd)
			proposeCancel()
			elapsed := time.Since(start)
			if err != nil {
				b.Errorf("SyncPropose failed: %v", err)
				return
			}
			rec.Record(elapsed)
		}
	})
}

// BenchDragonboatSyncRead benchmarks linearizable read throughput.
func BenchDragonboatSyncRead(b *testing.B, nh *dragonboat.NodeHost, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()

	// Seed data.
	session := nh.GetNoOPSession(dbShardID)
	key := []byte("read-key")
	value := []byte("read-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
	seedCtx, seedCancel := context.WithTimeout(ctx, 5*time.Second)
	if _, err := nh.SyncPropose(seedCtx, session, cmd); err != nil {
		seedCancel()
		b.Fatalf("seed propose failed: %v", err)
	}
	seedCancel()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := nh.SyncRead(readCtx, dbShardID, key)
		readCancel()
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("SyncRead failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchDragonboatStaleRead benchmarks stale (non-linearizable) read throughput.
func BenchDragonboatStaleRead(b *testing.B, nh *dragonboat.NodeHost, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()

	// Seed data.
	session := nh.GetNoOPSession(dbShardID)
	key := []byte("stale-key")
	value := []byte("stale-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
	seedCtx, seedCancel := context.WithTimeout(ctx, 5*time.Second)
	if _, err := nh.SyncPropose(seedCtx, session, cmd); err != nil {
		seedCancel()
		b.Fatalf("seed propose failed: %v", err)
	}
	seedCancel()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := nh.StaleRead(dbShardID, key)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("StaleRead failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchDragonboatCommitLatency benchmarks per-proposal commit latency.
func BenchDragonboatCommitLatency(b *testing.B, nh *dragonboat.NodeHost, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session := nh.GetNoOPSession(dbShardID)
	key := []byte("commit-key")
	value := []byte("commit-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := nh.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("SyncPropose failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// BenchDragonboatElection benchmarks the Raft election algorithm time,
// excluding NodeHost infrastructure setup (TLS cert generation, gRPC
// transport initialization). The NodeHost is created once outside the
// timed loop; only StartReplica + waitForLeader is measured per iteration.
// Each iteration uses a unique clusterID to avoid on-disk state conflicts.
func BenchDragonboatElection(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	dir := b.TempDir()

	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0755); err != nil {
		b.Fatalf("create TLS dir: %v", err)
	}
	certs := generateTLSCerts(b, tlsDir, 1)

	nhCfg := dbconfig.NodeHostConfig{
		WALDir:         dir + "/wal",
		NodeHostDir:    dir + "/data",
		RaftAddress:    dbAddr,
		RTTMillisecond: dbRTT,
		MutualTLS:      true,
		CAFile:         certs[0].CAFile,
		CertFile:       certs[0].CertFile,
		KeyFile:        certs[0].KeyFile,
	}

	nh, err := dragonboat.NewNodeHost(nhCfg)
	if err != nil {
		b.Fatalf("NewNodeHost failed: %v", err)
	}
	defer nh.Close()

	members := map[uint64]dragonboat.Target{
		dbReplicaID: dbAddr,
	}

	createFn := func(shardID, replicaID uint64) dbsm.IStateMachine {
		return newDragonboatKVSM()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iterShardID := dbShardID + uint64(i)

		raftCfg := dbconfig.Config{
			ReplicaID:    dbReplicaID,
			ShardID:      iterShardID,
			ElectionRTT:  10,
			HeartbeatRTT: 1,
			CheckQuorum:  true,
		}

		start := time.Now()

		if err := nh.StartReplica(members, false, createFn, raftCfg); err != nil {
			b.Fatalf("StartReplica failed at iteration %d: %v", i, err)
		}

		if err := waitForDragonboatLeader(nh, iterShardID, dbElectionWait); err != nil {
			b.Fatalf("leader election timed out at iteration %d: %v", i, err)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		// Warmup proposal with retry to ensure everything is settled.
		session := nh.GetNoOPSession(iterShardID)
		warmupCmd := encodeKVCmd([]byte("warmup"), []byte("ok"))
		for attempt := 0; attempt < 10; attempt++ {
			warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, warmupErr := nh.SyncPropose(warmupCtx, session, warmupCmd)
			warmupCancel()
			if warmupErr == nil {
				break
			}
			warmupTimer := time.NewTimer(100 * time.Millisecond)
			<-warmupTimer.C
			warmupTimer.Stop()
		}

		if err := nh.StopShard(iterShardID); err != nil {
			b.Fatalf("StopShard failed at iteration %d: %v", i, err)
		}
	}
}

// BenchDragonboatConcurrentProposeFixed benchmarks concurrent proposals with
// a fixed number of goroutines rather than using b.RunParallel.
func BenchDragonboatConcurrentProposeFixed(b *testing.B, nh *dragonboat.NodeHost, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

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
			ctx := context.Background()
			session := nh.GetNoOPSession(dbShardID)
			key := []byte(fmt.Sprintf("key-%d", id))
			cmd := encodeKVCmd(key, value)

			for j := 0; j < opsPerGoroutine; j++ {
				start := time.Now()
				proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := nh.SyncPropose(proposeCtx, session, cmd)
				proposeCancel()
				elapsed := time.Since(start)
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
		b.Logf("concurrent propose: %d errors out of %d ops", n, b.N)
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Cluster Setup
// ---------------------------------------------------------------------------

// BenchCluster holds a multi-node Dragonboat cluster for benchmarks.
type BenchCluster struct {
	Hosts     []*dragonboat.NodeHost
	Addrs     []string
	LeaderIdx int
}

// Close shuts down all NodeHost instances in the cluster.
func (c *BenchCluster) Close() {
	for _, nh := range c.Hosts {
		nh.Close()
	}
}

// Leader returns the NodeHost that is currently the Raft leader.
func (c *BenchCluster) Leader() *dragonboat.NodeHost {
	return c.Hosts[c.LeaderIdx]
}

// freeTCPPort finds a free TCP port on localhost for Dragonboat transport.
func freeTCPPort(b *testing.B) string {
	b.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// SetupDragonboatCluster creates a multi-node Dragonboat cluster with
// MutualTLS enabled. It generates ephemeral ECDSA certificates, starts
// numNodes NodeHost instances, forms a Raft group, waits for leader
// election, and performs a warmup proposal to ensure all transport
// connections are fully established.
func SetupDragonboatCluster(b *testing.B, numNodes int) *BenchCluster {
	b.Helper()

	dir := b.TempDir()
	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0755); err != nil {
		b.Fatalf("create TLS dir: %v", err)
	}

	certs := generateTLSCerts(b, tlsDir, numNodes)

	// Allocate ports for all nodes before starting any of them.
	addrs := make([]string, numNodes)
	for i := range numNodes {
		addrs[i] = freeTCPPort(b)
	}

	hosts := make([]*dragonboat.NodeHost, numNodes)
	for i := range numNodes {
		nodeDir := filepath.Join(dir, fmt.Sprintf("node-%d", i))

		nhCfg := dbconfig.NodeHostConfig{
			WALDir:         nodeDir + "/wal",
			NodeHostDir:    nodeDir + "/data",
			RaftAddress:    addrs[i],
			RTTMillisecond: dbRTT,
			MutualTLS:      true,
			CAFile:         certs[i].CAFile,
			CertFile:       certs[i].CertFile,
			KeyFile:        certs[i].KeyFile,
		}

		nh, err := dragonboat.NewNodeHost(nhCfg)
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			b.Fatalf("NewNodeHost[%d] failed: %v", i, err)
		}
		hosts[i] = nh
	}

	// Build members map: nodeID = i+1, target = address.
	members := make(map[uint64]dragonboat.Target, numNodes)
	for i, addr := range addrs {
		members[uint64(i+1)] = addr
	}

	// Start replicas on all hosts.
	for i, nh := range hosts {
		raftCfg := dbconfig.Config{
			ReplicaID:    uint64(i + 1),
			ShardID:      dbShardID,
			ElectionRTT:  10,
			HeartbeatRTT: 1,
			CheckQuorum:  true,
		}
		createFn := func(shardID, replicaID uint64) dbsm.IStateMachine {
			return newDragonboatKVSM()
		}
		if err := nh.StartReplica(members, false, createFn, raftCfg); err != nil {
			for _, h := range hosts {
				h.Close()
			}
			b.Fatalf("StartReplica[%d] failed: %v", i, err)
		}
	}

	// Wait for a leader to be elected and identified.
	leaderIdx, err := waitForDragonboatClusterLeader(hosts, dbShardID, 30*time.Second)
	if err != nil {
		for _, h := range hosts {
			h.Close()
		}
		b.Fatalf("leader election failed: %v", err)
	}

	// Warmup proposal with retry to ensure all TLS connections are fully
	// established before timing begins.
	session := hosts[leaderIdx].GetNoOPSession(dbShardID)
	warmupCmd := encodeKVCmd([]byte("warmup"), []byte("ok"))
	for attempt := 0; attempt < 10; attempt++ {
		warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, warmupErr := hosts[leaderIdx].SyncPropose(warmupCtx, session, warmupCmd)
		warmupCancel()
		if warmupErr == nil {
			break
		}
		warmupTimer := time.NewTimer(100 * time.Millisecond)
		<-warmupTimer.C
		warmupTimer.Stop()
	}

	return &BenchCluster{
		Hosts:     hosts,
		Addrs:     addrs,
		LeaderIdx: leaderIdx,
	}
}

// waitForDragonboatClusterLeader polls all hosts until one reports itself
// as the leader. Returns the host index of the leader.
func waitForDragonboatClusterLeader(hosts []*dragonboat.NodeHost, shardID uint64, timeout time.Duration) (int, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		for i, nh := range hosts {
			leaderID, _, ok, err := nh.GetLeaderID(shardID)
			if err == nil && ok && leaderID == uint64(i+1) {
				return i, nil
			}
		}
		select {
		case <-deadline.C:
			return -1, &LeaderElectionTimeoutError{Timeout: timeout}
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Cluster Benchmark Functions
// ---------------------------------------------------------------------------

// BenchDragonboatElectionCluster benchmarks leader election time for a
// multi-node cluster with MutualTLS. NodeHost infrastructure (TLS, gRPC
// transport) is created once outside the timed loop. Each iteration starts
// replicas on all hosts with a unique clusterID, waits for leader election,
// records the elapsed time, and stops the shard on all hosts.
func BenchDragonboatElectionCluster(b *testing.B, numNodes int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	dir := b.TempDir()
	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0755); err != nil {
		b.Fatalf("create TLS dir: %v", err)
	}

	certs := generateTLSCerts(b, tlsDir, numNodes)

	addrs := make([]string, numNodes)
	for j := range numNodes {
		addrs[j] = freeTCPPort(b)
	}

	// Create all NodeHosts once outside the benchmark loop.
	hosts := make([]*dragonboat.NodeHost, numNodes)
	for j := range numNodes {
		nodeDir := filepath.Join(dir, fmt.Sprintf("node-%d", j))
		nhCfg := dbconfig.NodeHostConfig{
			WALDir:         nodeDir + "/wal",
			NodeHostDir:    nodeDir + "/data",
			RaftAddress:    addrs[j],
			RTTMillisecond: dbRTT,
			MutualTLS:      true,
			CAFile:         certs[j].CAFile,
			CertFile:       certs[j].CertFile,
			KeyFile:        certs[j].KeyFile,
		}

		nh, err := dragonboat.NewNodeHost(nhCfg)
		if err != nil {
			for k := 0; k < j; k++ {
				hosts[k].Close()
			}
			b.Fatalf("NewNodeHost[%d] failed: %v", j, err)
		}
		hosts[j] = nh
	}
	defer func() {
		for _, h := range hosts {
			h.Close()
		}
	}()

	members := make(map[uint64]dragonboat.Target, numNodes)
	for j, addr := range addrs {
		members[uint64(j+1)] = addr
	}

	createFn := func(shardID, replicaID uint64) dbsm.IStateMachine {
		return newDragonboatKVSM()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iterShardID := dbShardID + uint64(i)

		start := time.Now()

		// Start replicas on all hosts with this iteration's shard ID.
		for j, nh := range hosts {
			raftCfg := dbconfig.Config{
				ReplicaID:    uint64(j + 1),
				ShardID:      iterShardID,
				ElectionRTT:  10,
				HeartbeatRTT: 1,
				CheckQuorum:  true,
			}
			if err := nh.StartReplica(members, false, createFn, raftCfg); err != nil {
				b.Fatalf("StartReplica[%d] failed at iteration %d: %v", j, i, err)
			}
		}

		// Wait for a leader to be elected.
		leaderIdx, err := waitForDragonboatClusterLeader(hosts, iterShardID, 30*time.Second)
		if err != nil {
			b.Fatalf("leader election failed at iteration %d: %v", i, err)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		// Warmup proposal with retry to ensure transport connections settle.
		session := hosts[leaderIdx].GetNoOPSession(iterShardID)
		warmupCmd := encodeKVCmd([]byte("warmup"), []byte("ok"))
		for attempt := 0; attempt < 10; attempt++ {
			warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, warmupErr := hosts[leaderIdx].SyncPropose(warmupCtx, session, warmupCmd)
			warmupCancel()
			if warmupErr == nil {
				break
			}
			warmupTimer := time.NewTimer(100 * time.Millisecond)
			<-warmupTimer.C
			warmupTimer.Stop()
		}

		// Stop the shard on all hosts before the next iteration.
		for j, nh := range hosts {
			if err := nh.StopShard(iterShardID); err != nil {
				b.Fatalf("StopShard[%d] failed at iteration %d: %v", j, i, err)
			}
		}
	}
}

// BenchDragonboatProposeCluster benchmarks sequential SyncPropose
// throughput through a multi-node cluster leader.
func BenchDragonboatProposeCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	leader := cluster.Leader()
	session := leader.GetNoOPSession(dbShardID)
	key := []byte("bench-key")
	value := []byte("bench-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(cmd)))

	var errCount atomic.Int64
	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := leader.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			errCount.Add(1)
			b.Logf("SyncPropose failed at iteration %d: %v", i, err)
			continue
		}
		rec.Record(elapsed)
	}
	if n := errCount.Load(); n > 0 {
		b.Logf("total propose errors: %d/%d", n, b.N)
	}
}

// BenchDragonboatConcurrentProposeFixedCluster benchmarks concurrent
// proposals with a fixed number of goroutines through the cluster leader.
func BenchDragonboatConcurrentProposeFixedCluster(b *testing.B, cluster *BenchCluster, numGoroutines int, rec *perfresult.LatencyRecorder) {
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
			ctx := context.Background()
			session := leader.GetNoOPSession(dbShardID)
			key := []byte(fmt.Sprintf("key-%d", id))
			cmd := encodeKVCmd(key, value)

			for j := 0; j < opsPerGoroutine; j++ {
				start := time.Now()
				proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
				_, err := leader.SyncPropose(proposeCtx, session, cmd)
				proposeCancel()
				elapsed := time.Since(start)
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
		b.Logf("concurrent propose cluster: %d errors out of %d ops", n, b.N)
	}
}

// BenchDragonboatCommitLatencyCluster benchmarks per-proposal commit
// latency through the cluster leader.
func BenchDragonboatCommitLatencyCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	leader := cluster.Leader()
	session := leader.GetNoOPSession(dbShardID)
	key := []byte("commit-key")
	value := []byte("commit-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)

	b.ResetTimer()
	b.ReportAllocs()

	var errCount atomic.Int64
	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := leader.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
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

// BenchDragonboatSyncReadCluster benchmarks linearizable read throughput
// through the cluster leader.
func BenchDragonboatSyncReadCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()
	leader := cluster.Leader()

	// Seed data.
	session := leader.GetNoOPSession(dbShardID)
	key := []byte("read-key")
	value := []byte("read-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
	seedCtx, seedCancel := context.WithTimeout(ctx, 10*time.Second)
	if _, err := leader.SyncPropose(seedCtx, session, cmd); err != nil {
		seedCancel()
		b.Fatalf("seed propose failed: %v", err)
	}
	seedCancel()

	b.ResetTimer()
	b.ReportAllocs()

	var errCount atomic.Int64
	for i := 0; i < b.N; i++ {
		start := time.Now()
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := leader.SyncRead(readCtx, dbShardID, key)
		readCancel()
		elapsed := time.Since(start)
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

// BenchDragonboatStaleReadCluster benchmarks stale (non-linearizable)
// read throughput through the cluster leader.
func BenchDragonboatStaleReadCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()
	leader := cluster.Leader()

	// Seed data.
	session := leader.GetNoOPSession(dbShardID)
	key := []byte("stale-key")
	value := []byte("stale-value-0123456789abcdef")
	cmd := encodeKVCmd(key, value)
	seedCtx, seedCancel := context.WithTimeout(ctx, 10*time.Second)
	if _, err := leader.SyncPropose(seedCtx, session, cmd); err != nil {
		seedCancel()
		b.Fatalf("seed propose failed: %v", err)
	}
	seedCancel()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := leader.StaleRead(dbShardID, key)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("StaleRead failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}
