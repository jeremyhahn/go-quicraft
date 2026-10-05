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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/dragonboat/v4"
	dbconfig "github.com/jeremyhahn/dragonboat/v4/config"
	"github.com/jeremyhahn/dragonboat/v4/plugin/tan"
	dbsm "github.com/jeremyhahn/dragonboat/v4/statemachine"
	"github.com/jeremyhahn/go-quicraft/test/performance/benchutil"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
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

	// dbRTT is the RTT in milliseconds for Dragonboat benchmarks.
	dbRTT uint64 = 1

	// dbElectionWait is the maximum time to wait for leader election.
	dbElectionWait = 10 * time.Second

	// commitLatencyWarmup is the number of unmeasured proposals run before
	// the commit-latency measurement loop, so the first timed proposal does
	// not include one-time pipeline initialization. Shared across all engines.
	commitLatencyWarmup = 100

	// setupWarmupProposals is the number of unmeasured proposals a setup
	// helper makes, followed by one linearizable read, before handing the
	// shard to a benchmark. Every engine's setup does the same, so no engine
	// starts its timer on a colder pipeline than another.
	setupWarmupProposals = 10

	// leaderPollInterval is how often the leader waits poll GetLeaderID. It
	// bounds how late an election benchmark can notice a leader, so every
	// engine's harness polls at the same 1 ms.
	leaderPollInterval = time.Millisecond

	// clusterLeaderTimeout bounds how long a multi-node setup or election
	// iteration waits for a leader before failing.
	clusterLeaderTimeout = 30 * time.Second
)

// raftConfig returns the Raft configuration every benchmark replica uses. The
// timing is the cross-engine common configuration: a 1 ms tick (dbRTT), an
// election timeout of 10 ticks that Dragonboat randomizes over [10, 20), a
// 1-tick heartbeat, CheckQuorum, and PreVote. PreVote is off by default in
// Dragonboat and on in QuicRaft, so it is set explicitly here.
func raftConfig(shardID, replicaID uint64) dbconfig.Config {
	return dbconfig.Config{
		ReplicaID:    replicaID,
		ShardID:      shardID,
		ElectionRTT:  10,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
		PreVote:      true,
	}
}

// warmupShard makes setupWarmupProposals proposals and then one linearizable
// read through nh, each on the shared warmup retry budget. It establishes the
// transport connections and exercises the propose and ReadIndex paths so the
// first timed operation carries no one-time initialization.
func warmupShard(nh *dragonboat.NodeHost, shardID uint64) error {
	session := nh.GetNoOPSession(shardID)
	for i := 0; i < setupWarmupProposals; i++ {
		cmd := encodeKVCmd([]byte(fmt.Sprintf("warmup-%d", i)), []byte("ok"))
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := nh.SyncPropose(ctx, session, cmd)
			return err
		})
		if err != nil {
			return fmt.Errorf("warmup proposal %d failed after %d attempts: %w",
				i, benchutil.WarmupAttempts, err)
		}
	}
	err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nh.SyncRead(ctx, shardID, []byte("warmup-0"))
		return err
	})
	if err != nil {
		return fmt.Errorf("warmup read failed after %d attempts: %w", benchutil.WarmupAttempts, err)
	}
	return nil
}

// SetupDragonboatHost creates a single-node Dragonboat NodeHost with one
// shard running the KV state machine, waits for leader election and runs
// warmupShard. It returns the host and a cleanup function that must be
// called when the benchmark completes.
func SetupDragonboatHost(b *testing.B) (*dragonboat.NodeHost, func()) {
	b.Helper()

	dir := b.TempDir()

	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0755); err != nil {
		b.Fatalf("create TLS dir: %v", err)
	}
	certs := generateTLSCerts(b, tlsDir, 1)

	addr := freeTCPPort(b)

	nhCfg := nodeHostConfig(dir, addr, certs[0])

	nh, err := dragonboat.NewNodeHost(nhCfg)
	if err != nil {
		b.Fatalf("dragonboat.NewNodeHost failed: %v", err)
	}

	raftCfg := raftConfig(dbShardID, dbReplicaID)

	members := map[uint64]dragonboat.Target{
		dbReplicaID: addr,
	}

	createFn := func(_, _ uint64) dbsm.IStateMachine {
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

	if err := warmupShard(nh, dbShardID); err != nil {
		nh.Close()
		b.Fatalf("single-node setup: %v", err)
	}

	cleanup := func() {
		nh.Close()
	}
	return nh, cleanup
}

// waitForDragonboatLeader polls until a leader is elected or timeout expires.
func waitForDragonboatLeader(nh *dragonboat.NodeHost, shardID uint64, timeout time.Duration) error {
	ticker := time.NewTicker(leaderPollInterval)
	defer ticker.Stop()
	deadline := time.NewTimer(clusterLeaderTimeout)
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

	// The outer context must comfortably exceed the longest benchtime: it
	// parents every per-op context, so an outer deadline shorter than the
	// measured loop (e.g. 30s at BENCHTIME=30s) expires mid-run and fails
	// every remaining proposal. Matches the cluster variants.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	session := nh.GetNoOPSession(dbShardID)
	key := []byte("bench-key")
	cmd := makeKVCmd(key)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(cmd)))

	var completed, errored int
	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := nh.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			// A transient refusal is Raft weather, not a failed benchmark;
			// requireFloor decides whether there were too many.
			if !tolerable(err) {
				b.Fatalf("iteration %d: permanent error, not Raft weather: %v", i, err)
			}
			errored++
			b.Logf("SyncPropose failed at iteration %d: %v", i, err)
			continue
		}
		completed++
		rec.Record(elapsed)
	}
	requireFloor(b, "proposals", completed, errored)
}

// BenchDragonboatSyncRead benchmarks linearizable read throughput.
func BenchDragonboatSyncRead(b *testing.B, nh *dragonboat.NodeHost, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()

	// Seed data.
	session := nh.GetNoOPSession(dbShardID)
	key := []byte("read-key")
	cmd := makeKVCmd(key)
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
	cmd := makeKVCmd(key)
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

	// 10-minute outer context for the same reason as BenchDragonboatPropose:
	// it must outlive the measured loop at the longest benchtime.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	session := nh.GetNoOPSession(dbShardID)
	key := []byte("commit-key")
	cmd := makeKVCmd(key)

	// Warm up the pipeline (same count and retry budget as all engines)
	// before measuring. Transient errors ("shard not ready") are retried
	// so a single startup flake cannot discard the whole suite.
	for w := 0; w < commitLatencyWarmup; w++ {
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			warmCtx, warmCancel := context.WithTimeout(ctx, 5*time.Second)
			defer warmCancel()
			_, perr := nh.SyncPropose(warmCtx, session, cmd)
			return perr
		})
		if err != nil {
			b.Fatalf("warmup propose failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var completed, errored int
	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := nh.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			// A transient refusal is Raft weather, not a failed benchmark;
			// requireFloor decides whether there were too many.
			if !tolerable(err) {
				b.Fatalf("iteration %d: permanent error, not Raft weather: %v", i, err)
			}
			errored++
			b.Logf("SyncPropose failed at iteration %d: %v", i, err)
			continue
		}
		completed++
		rec.Record(elapsed)
	}
	requireFloor(b, "proposals", completed, errored)
}

// BenchDragonboatElection benchmarks the Raft election algorithm time,
// excluding NodeHost infrastructure setup (TLS cert generation, gRPC
// transport initialization). The NodeHost is created once outside the
// timed loop; only StartReplica + waitForLeader is measured per iteration.
// primeElectionRing bootstraps every ring shard ID before the timer starts, so
// each timed iteration restarts an existing group (see electionShardRing).
func BenchDragonboatElection(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	dir := b.TempDir()

	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0755); err != nil {
		b.Fatalf("create TLS dir: %v", err)
	}
	certs := generateTLSCerts(b, tlsDir, 1)

	addr := freeTCPPort(b)

	nhCfg := nodeHostConfig(dir, addr, certs[0])

	nh, err := dragonboat.NewNodeHost(nhCfg)
	if err != nil {
		b.Fatalf("NewNodeHost failed: %v", err)
	}
	defer nh.Close()

	members := map[uint64]dragonboat.Target{
		dbReplicaID: addr,
	}

	createFn := func(_, _ uint64) dbsm.IStateMachine {
		return newDragonboatKVSM()
	}

	primeElectionRing(b, []*dragonboat.NodeHost{nh}, members, createFn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iterShardID := electionShardID(i)

		raftCfg := raftConfig(iterShardID, dbReplicaID)

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

	opsPerGoroutine := b.N / numGoroutines
	if opsPerGoroutine < 1 {
		opsPerGoroutine = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	var okCount, errCount atomic.Int64
	var permanent atomic.Pointer[error]
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			session := nh.GetNoOPSession(dbShardID)
			key := []byte(fmt.Sprintf("key-%d", id))
			cmd := makeKVCmd(key)

			for j := 0; j < opsPerGoroutine; j++ {
				start := time.Now()
				proposeCtx, proposeCancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := nh.SyncPropose(proposeCtx, session, cmd)
				proposeCancel()
				elapsed := time.Since(start)
				if err != nil {
					if !tolerable(err) {
						// A goroutine must not touch b. Keep the FIRST
						// permanent error; the parent fails on it after Wait.
						permanent.CompareAndSwap(nil, &err)
						return
					}
					errCount.Add(1)
					continue
				}
				okCount.Add(1)
				rec.Record(elapsed)
			}
		}(g)
	}

	wg.Wait()

	if p := permanent.Load(); p != nil {
		b.Fatalf("permanent error, not Raft weather: %v", *p)
	}

	// Counted AND checked: a loop that tolerates every error and logs the
	// total without checking it lets a run in which every proposal failed
	// report no samples and pass clean.
	requireFloor(b, "proposals", int(okCount.Load()), int(errCount.Load()))
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

// seed writes key through the cluster's CURRENT leader before a read
// benchmark times reads of it, on the warmup retry budget, re-resolving the
// leader before every attempt. The cluster was warm a moment ago, but a
// 3-node shard on a large host can lose its leader between the warmup and
// the first benchmark, and dragonboat does not forward: a proposal
// addressed to the deposed leader sits for its whole 10s and reports
// "timeout", which would take the run down with it. The retry treats that
// the way the propose loops treat every other transient, and LeaderIdx is
// updated so the loop that follows reads from the leader the seed actually
// landed on.
func (c *BenchCluster) seed(b *testing.B, key []byte) {
	b.Helper()
	cmd := makeKVCmd(key)
	err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
		idx, err := waitForDragonboatClusterLeader(c.Hosts, dbShardID)
		if err != nil {
			return err
		}
		c.LeaderIdx = idx
		leader := c.Hosts[idx]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = leader.SyncPropose(ctx, leader.GetNoOPSession(dbShardID), cmd)
		return err
	})
	if err != nil {
		b.Fatalf("seed propose failed after %d attempts: %v", benchutil.WarmupAttempts, err)
	}
}

// freeTCPPort reserves a loopback address for a Dragonboat transport listener.
// Dragonboat binds by address rather than by inherited listener, so the port
// has to be chosen before the NodeHost exists; benchutil.ReserveTCPPort picks
// one from outside the kernel's ephemeral range so no outbound connection can
// be assigned it in the gap. See benchutil/port.go for the failure this
// replaced.
func freeTCPPort(b testing.TB) string {
	b.Helper()
	return benchutil.MustReserveTCPPort(b.Fatalf)
}

// nodeHostConfig returns the NodeHostConfig every benchmark NodeHost uses:
// WAL and data directories under dir, mutual TLS from cert, and Tan as the
// LogDB. Dragonboat falls back to its sharded Pebble LogDB when no factory is
// set, so the factory is explicit to benchmark Dragonboat's own log store.
func nodeHostConfig(dir, addr string, cert tlsCertBundle) dbconfig.NodeHostConfig {
	return dbconfig.NodeHostConfig{
		WALDir:         dir + "/wal",
		NodeHostDir:    dir + "/data",
		RaftAddress:    addr,
		RTTMillisecond: dbRTT,
		MutualTLS:      true,
		CAFile:         cert.CAFile,
		CertFile:       cert.CertFile,
		KeyFile:        cert.KeyFile,
		Expert:         dbconfig.ExpertConfig{LogDBFactory: tan.Factory},
	}
}

// electionShardRing is how many shard IDs the election benchmarks rotate
// through. Tan gives every shard/replica pair its own database with a 64 MiB
// preallocated log file that neither StopShard nor RemoveData deletes, so a new
// shard ID per iteration fills the disk (about 300 MiB per 5-node iteration).
// Reusing one ID is not possible either: StopShard unloads the node
// asynchronously and StartReplica refuses the ID until it has. A ring bounds the
// disk to electionShardRing databases per replica and returns to an ID only
// after many iterations, long after its unload finished. An iteration that
// reuses an ID restarts the replicas from their log and runs a fresh election,
// as the QuicRaft harness does with its single shard ID.
const electionShardRing = 16

// electionShardID returns the shard ID for election benchmark iteration i.
func electionShardID(i int) uint64 {
	return dbShardID + uint64(i%electionShardRing)
}

// primeElectionRing runs one untimed election cycle for every ring shard ID:
// start the replica on every host, wait for a leader, stop the shard. It runs
// before the election benchmarks reset their timer, so Tan's per-shard database
// creation (including its 64 MiB log preallocation) and the first-ever
// bootstrap of each group stay out of the measurement, and every timed
// iteration restarts an existing group, as the QuicRaft harness restarts its
// one shard ID.
//
// StopShard returns before the engine has unloaded the node, and StartReplica
// refuses the ID with ErrShardAlreadyExist until it has. Every ID here is fresh,
// so that cannot happen while priming; the start is still retried on that error
// rather than assumed. The timed loop does not retry: the ring order puts
// electionShardRing-1 other cycles between a stop and the next start of the
// same ID, and a start that still collides fails the run loudly instead of
// folding a wait into the measurement.
func primeElectionRing(b *testing.B, hosts []*dragonboat.NodeHost,
	members map[uint64]dragonboat.Target, createFn dbsm.CreateStateMachineFunc) {
	b.Helper()
	for k := 0; k < electionShardRing; k++ {
		shardID := electionShardID(k)
		for j, nh := range hosts {
			cfg := raftConfig(shardID, uint64(j+1))
			deadline := time.Now().Add(dbElectionWait)
			for {
				err := nh.StartReplica(members, false, createFn, cfg)
				if err == nil {
					break
				}
				if !errors.Is(err, dragonboat.ErrShardAlreadyExist) || time.Now().After(deadline) {
					b.Fatalf("priming shard %d: StartReplica[%d]: %v", shardID, j, err)
				}
				time.Sleep(leaderPollInterval)
			}
		}
		if _, err := waitForDragonboatClusterLeader(hosts, shardID); err != nil {
			b.Fatalf("priming shard %d: %v", shardID, err)
		}
		for j, nh := range hosts {
			if err := nh.StopShard(shardID); err != nil {
				b.Fatalf("priming shard %d: StopShard[%d]: %v", shardID, j, err)
			}
		}
	}
}

// SetupDragonboatCluster creates a multi-node Dragonboat cluster with
// MutualTLS enabled. It generates ephemeral ECDSA certificates, starts
// numNodes NodeHost instances, forms a Raft group, waits for leader
// election, and runs warmupShard (10 proposals and a linearizable read) so
// all transport connections are fully established.
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

		nhCfg := nodeHostConfig(nodeDir, addrs[i], certs[i])

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
		raftCfg := raftConfig(dbShardID, uint64(i+1))
		createFn := func(_, _ uint64) dbsm.IStateMachine {
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
	leaderIdx, err := waitForDragonboatClusterLeader(hosts, dbShardID)
	if err != nil {
		for _, h := range hosts {
			h.Close()
		}
		b.Fatalf("leader election failed: %v", err)
	}

	// Warm up so all TLS connections are fully established before timing
	// begins. The retry budget is benchutil's, shared with every other engine,
	// and a warmup that never succeeds is fatal HERE. Swallowing it would hand
	// an unready cluster to the caller, to surface later as a misleading
	// downstream error such as "seed propose failed: request dropped as the
	// shard is not ready" instead of naming the setup that had already failed.
	if err := warmupShard(hosts[leaderIdx], dbShardID); err != nil {
		for _, h := range hosts {
			h.Close()
		}
		b.Fatalf("%d-node cluster setup: %v", numNodes, err)
	}

	return &BenchCluster{
		Hosts:     hosts,
		Addrs:     addrs,
		LeaderIdx: leaderIdx,
	}
}

// waitForDragonboatClusterLeader polls all hosts until one reports itself
// as the leader, for at most clusterLeaderTimeout. Returns the host index of
// the leader.
func waitForDragonboatClusterLeader(hosts []*dragonboat.NodeHost, shardID uint64) (int, error) {
	ticker := time.NewTicker(leaderPollInterval)
	defer ticker.Stop()
	deadline := time.NewTimer(clusterLeaderTimeout)
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
			return -1, &LeaderElectionTimeoutError{Timeout: clusterLeaderTimeout}
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Cluster Benchmark Functions
// ---------------------------------------------------------------------------

// BenchDragonboatElectionCluster benchmarks leader election time for a
// multi-node cluster with MutualTLS. NodeHost infrastructure (TLS, gRPC
// transport) is created once outside the timed loop, and primeElectionRing
// bootstraps every ring shard ID before the timer starts. Each iteration
// restarts replicas on all hosts with the next ring shard ID, waits for leader
// election, records the elapsed time, and stops the shard on all hosts.
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
		nhCfg := nodeHostConfig(nodeDir, addrs[j], certs[j])

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

	createFn := func(_, _ uint64) dbsm.IStateMachine {
		return newDragonboatKVSM()
	}

	primeElectionRing(b, hosts, members, createFn)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iterShardID := electionShardID(i)

		start := time.Now()

		// Start replicas on all hosts with this iteration's shard ID.
		for j, nh := range hosts {
			raftCfg := raftConfig(iterShardID, uint64(j+1))
			if err := nh.StartReplica(members, false, createFn, raftCfg); err != nil {
				b.Fatalf("StartReplica[%d] failed at iteration %d: %v", j, i, err)
			}
		}

		// Wait for a leader to be elected.
		leaderIdx, err := waitForDragonboatClusterLeader(hosts, iterShardID)
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
	cmd := makeKVCmd(key)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(cmd)))

	var completed, errored int
	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := leader.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			if !tolerable(err) {
				b.Fatalf("iteration %d: permanent error, not Raft weather: %v", i, err)
			}
			errored++
			b.Logf("SyncPropose failed at iteration %d: %v", i, err)
			continue
		}
		completed++
		rec.Record(elapsed)
	}
	// This loop tolerates errors and counts them, and the count must be
	// checked, or a run in which EVERY proposal failed reports zero samples
	// and passes clean.
	requireFloor(b, "proposals", completed, errored)
}

// BenchDragonboatConcurrentProposeFixedCluster benchmarks concurrent
// proposals with a fixed number of goroutines through the cluster leader.
func BenchDragonboatConcurrentProposeFixedCluster(b *testing.B, cluster *BenchCluster, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	leader := cluster.Leader()
	opsPerGoroutine := b.N / numGoroutines
	if opsPerGoroutine < 1 {
		opsPerGoroutine = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	var okCount, errCount atomic.Int64
	var permanent atomic.Pointer[error]
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			session := leader.GetNoOPSession(dbShardID)
			key := []byte(fmt.Sprintf("key-%d", id))
			cmd := makeKVCmd(key)

			for j := 0; j < opsPerGoroutine; j++ {
				start := time.Now()
				proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
				_, err := leader.SyncPropose(proposeCtx, session, cmd)
				proposeCancel()
				elapsed := time.Since(start)
				if err != nil {
					if !tolerable(err) {
						// A goroutine must not touch b. Keep the FIRST
						// permanent error; the parent fails on it after Wait.
						permanent.CompareAndSwap(nil, &err)
						return
					}
					errCount.Add(1)
					continue
				}
				okCount.Add(1)
				rec.Record(elapsed)
			}
		}(g)
	}

	wg.Wait()

	if p := permanent.Load(); p != nil {
		b.Fatalf("permanent error, not Raft weather: %v", *p)
	}

	requireFloor(b, "proposals", int(okCount.Load()), int(errCount.Load()))
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
	cmd := makeKVCmd(key)

	// Warm up the pipeline (same count and retry budget as all engines)
	// before measuring. Transient errors ("shard not ready") are retried
	// so a single startup flake cannot discard the whole suite.
	for w := 0; w < commitLatencyWarmup; w++ {
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			warmCtx, warmCancel := context.WithTimeout(ctx, 10*time.Second)
			defer warmCancel()
			_, perr := leader.SyncPropose(warmCtx, session, cmd)
			return perr
		})
		if err != nil {
			b.Fatalf("warmup propose failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var completed, errored int
	for i := 0; i < b.N; i++ {
		start := time.Now()
		proposeCtx, proposeCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := leader.SyncPropose(proposeCtx, session, cmd)
		proposeCancel()
		elapsed := time.Since(start)
		if err != nil {
			if !tolerable(err) {
				b.Fatalf("iteration %d: permanent error, not Raft weather: %v", i, err)
			}
			errored++
			b.Logf("SyncPropose failed at iteration %d: %v", i, err)
			continue
		}
		completed++
		rec.Record(elapsed)
	}
	// Counted AND checked: tolerating and logging is not enough unless
	// something reads the count.
	requireFloor(b, "proposals", completed, errored)
}

// BenchDragonboatSyncReadCluster benchmarks linearizable read throughput
// through the cluster leader.
func BenchDragonboatSyncReadCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	ctx := context.Background()

	// Seed data, through whichever host leads NOW.
	key := []byte("read-key")
	cluster.seed(b, key)
	leader := cluster.Leader()

	b.ResetTimer()
	b.ReportAllocs()

	var completed, errored int
	for i := 0; i < b.N; i++ {
		start := time.Now()
		readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := leader.SyncRead(readCtx, dbShardID, key)
		readCancel()
		elapsed := time.Since(start)
		if err != nil {
			if !tolerable(err) {
				b.Fatalf("iteration %d: permanent error, not Raft weather: %v", i, err)
			}
			errored++
			b.Logf("SyncRead failed at iteration %d: %v", i, err)
			continue
		}
		completed++
		rec.Record(elapsed)
	}
	// A linearizable read is leader-bound exactly as a proposal is and fails
	// on the same transients, so it takes the same floor.
	requireFloor(b, "reads", completed, errored)
}

// BenchDragonboatStaleReadCluster benchmarks stale (non-linearizable)
// read throughput through the cluster leader.
func BenchDragonboatStaleReadCluster(b *testing.B, cluster *BenchCluster, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// Seed data, through whichever host leads NOW.
	key := []byte("stale-key")
	cluster.seed(b, key)
	leader := cluster.Leader()

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

// ---------------------------------------------------------------------------
// Propose-loop error tolerance
// ---------------------------------------------------------------------------

// maxProposeErrorRate is the share of a propose loop's iterations that may fail
// before the loop's recorded latencies stop being worth publishing.
//
// The number is measured, not chosen for roundness. The Dragonboat baseline was
// run twice on 2026-09-10 against a host also serving CI:
//
//	benchtime=30s, load ~14-24 on 20 cores   21 of 24 proposals failed  (87%)
//	benchtime=5s,  load ~3.4 on 20 cores     24 of 399 failed           (6%)
//
// The quiet run's 6% is ordinary Raft weather -- a leader election completing,
// a pipeline draining -- and its surviving samples describe the engine. The
// loaded run's 87% describes the box. A threshold between them lets the first
// publish and refuses the second, and it does so on the RUN'S OWN evidence
// rather than on a guess about the host.
const maxProposeErrorRate = 0.20

// minProposeSample is the number of attempts below which the floor withholds
// judgement instead of applying a rate.
//
// Go's benchmark runner invokes every benchmark first with b.N=1, and only
// launches the larger calibrated runs if that invocation returns without
// failing. At b.N=1 the sequential loops make ONE proposal and the concurrent
// helper makes eight (one per goroutine), so a single transient refusal is a
// 100% or 12.5% error rate -- and a rate-based floor would call b.Fatal on the
// calibration probe and abort the whole benchmark before it measured anything.
// That is the exact failure this file was changed to remove, arriving by a
// different door.
//
// A rate is not meaningful over a handful of attempts anyway. Below this many,
// the floor applies only the zero-completions check (a probe that could not
// propose at all IS a real failure and should stop calibration) and lets the
// calibrated run, with thousands of attempts, be the one that is judged.
const minProposeSample = 100

// tolerable reports whether err is transient Raft weather a benchmark loop may
// count and continue past, as opposed to a real result that must stop the run.
//
// Counting EVERY error would let a permanent failure -- ErrPayloadTooBig,
// ErrRejected, ErrInvalidSession, a state-machine failure -- at 5% be
// published as a clean run with a note. The distinction is the one test/proposer already draws and
// documents: a retry loop may absorb only the conditions that clear on their
// own, because those are the only ones re-trying can change the answer to.
// Everything else is the engine answering, and the benchmark must report it.
//
// dragonboat.IsTempError is the library's own classification -- "worth
// retrying with the exact same input" -- and it covers the two transient
// failures actually observed: ErrShardNotReady and ErrTimeout.
func tolerable(err error) bool { return dragonboat.IsTempError(err) }

// requireFloor fails the benchmark unless enough of its proposals
// completed for the recorded latencies to mean anything.
//
// # Why this exists at all
//
// Calling b.Fatalf on the FIRST error, under a 5-second per-proposal deadline,
// would let one transient "request dropped as the shard is not ready" destroy
// a whole run, so the Dragonboat baseline could not be measured at all on a
// host carrying ordinary CI load. Those are precisely the conditions a
// benchmark must not treat as failures.
//
// # Why a floor and not merely tolerance
//
// Tolerance alone replaces a loud failure with a silent one. The concurrent
// loop already counted its errors and logged them -- and would have reported
// ZERO successful proposals as a clean pass, because nothing checked. A
// benchmark that publishes percentiles over no samples is worse than one that
// crashes, since the number reaches a comparison table.
//
// So both halves are here: a transient error does not abort the run, and a
// run that failed to measure the thing it exists to measure fails loudly and
// says which of the two it was. This is the shape linearizability's
// minSuccessfulReads floor already uses in the integration suites.
func requireFloor(b *testing.B, op string, completed, errored int) {
	b.Helper()
	note, fatal := proposeFloorVerdict(op, completed, errored)
	if note == "" {
		return
	}
	if fatal {
		b.Fatal(note)
	}
	b.Log(note)
}

// proposeFloorVerdict is requireFloor's decision, separated from the
// *testing.B it reports through so the thresholds can be tested at their
// boundaries rather than only exercised by a 30-minute benchmark run.
//
// It returns the message to report and whether it is fatal; an empty message
// means the loop was clean and there is nothing to say.
func proposeFloorVerdict(op string, completed, errored int) (string, bool) {
	attempted := completed + errored
	if attempted == 0 {
		return "", false // b.N was 0; nothing was asked of the engine.
	}
	if completed == 0 {
		return fmt.Sprintf("every one of %d %s failed: there are no samples "+
			"to report, so this benchmark measured nothing", attempted, op), true
	}
	rate := float64(errored) / float64(attempted)
	if attempted < minProposeSample {
		// A calibration probe, or a run too short to rate. Something
		// completed, so calibration may continue; the calibrated run is
		// where the rate is judged. Still say what happened.
		if errored > 0 {
			return fmt.Sprintf("total %s errors: %d/%d (%.1f%%) -- below the "+
				"%d-attempt sample floor, not judged", op, errored, attempted, 100*rate,
				minProposeSample), false
		}
		return "", false
	}
	if rate > maxProposeErrorRate {
		return fmt.Sprintf("%d of %d %s failed (%.1f%%, limit %.0f%%): the "+
			"surviving samples describe a contended host rather than the engine, so "+
			"publishing them would put a measurement of this machine's load into a "+
			"comparison table. Re-run on a quiet host.",
			errored, attempted, op, 100*rate, 100*maxProposeErrorRate), true
	}
	if errored > 0 {
		return fmt.Sprintf("total %s errors: %d/%d (%.1f%%)",
			op, errored, attempted, 100*rate), false
	}
	return "", false
}
