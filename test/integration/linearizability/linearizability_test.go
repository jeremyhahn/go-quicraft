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

//go:build integration

package linearizability_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ---------------------------------------------------------------------------
// Porcupine KV Model
// ---------------------------------------------------------------------------

// kvInput describes an operation submitted to the KV store.
type kvInput struct {
	Op    string // "put" or "get"
	Key   string
	Value string // for put only
}

// kvOutput describes the result of a KV store operation.
type kvOutput struct {
	Value string // for get only
	Ok    bool   // always true for successful put
}

// kvModel returns a porcupine.Model for a linearizable KV store.
// The state is map[string]string representing the current KV contents.
// Operations are partitioned by key for parallel checking.
func kvModel() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			partitions := make(map[string][]porcupine.Operation)
			for _, op := range history {
				inp := op.Input.(kvInput)
				partitions[inp.Key] = append(partitions[inp.Key], op)
			}
			result := make([][]porcupine.Operation, 0, len(partitions))
			// Sort keys for deterministic partition ordering.
			keys := make([]string, 0, len(partitions))
			for k := range partitions {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				result = append(result, partitions[k])
			}
			return result
		},
		Init: func() interface{} {
			// Per-partition state is a single string value (empty = missing).
			return ""
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			inp := input.(kvInput)
			out := output.(kvOutput)
			val := state.(string)

			switch inp.Op {
			case "put":
				// A put always succeeds and updates the value.
				if !out.Ok {
					return false, val
				}
				return true, inp.Value
			case "get":
				// A get must return the current value. Empty string
				// means key not found, which is also the initial state.
				if out.Value == val {
					return true, val
				}
				return false, val
			default:
				return false, val
			}
		},
		Equal: func(state1, state2 interface{}) bool {
			return state1.(string) == state2.(string)
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			inp := input.(kvInput)
			out := output.(kvOutput)
			switch inp.Op {
			case "put":
				return fmt.Sprintf("Put(%s, %s) -> ok=%v", inp.Key, inp.Value, out.Ok)
			case "get":
				return fmt.Sprintf("Get(%s) -> %q", inp.Key, out.Value)
			default:
				return fmt.Sprintf("Unknown(%v) -> %v", inp, out)
			}
		},
		DescribeState: func(state interface{}) string {
			val := state.(string)
			if val == "" {
				return "<empty>"
			}
			return fmt.Sprintf("%q", val)
		},
	}
}

// ---------------------------------------------------------------------------
// Operation Recorder
// ---------------------------------------------------------------------------

// opRecorder safely collects porcupine.Operation entries from concurrent
// goroutines. Each operation is assigned a monotonic timestamp pair to
// establish a real-time ordering for the linearizability checker.
type opRecorder struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

// record appends an operation to the history.
func (r *opRecorder) record(clientID int, input kvInput, output kvOutput, callTime, returnTime int64) {
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    input,
		Output:   output,
		Call:     callTime,
		Return:   returnTime,
	})
	r.mu.Unlock()
}

// history returns a copy of the recorded operations.
func (r *opRecorder) history() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]porcupine.Operation, len(r.ops))
	copy(result, r.ops)
	return result
}

// ---------------------------------------------------------------------------
// Typed Errors
// ---------------------------------------------------------------------------

// LinearizabilityViolationError is returned when porcupine detects a
// linearizability violation in the recorded operation history.
type LinearizabilityViolationError struct {
	Result   porcupine.CheckResult
	NumOps   int
	Duration time.Duration
}

// Error returns a description of the linearizability violation.
func (e *LinearizabilityViolationError) Error() string {
	return fmt.Sprintf(
		"linearizability violation: result=%s, ops=%d, duration=%v",
		e.Result, e.NumOps, e.Duration,
	)
}

// ---------------------------------------------------------------------------
// Retry Helpers
// ---------------------------------------------------------------------------

// isRetryableError returns true if the error is transient and the operation
// should be retried on a different node or after a short delay.
func isRetryableError(err error) bool {
	return errors.Is(err, quicraft.ErrTimeout) ||
		errors.Is(err, quicraft.ErrCanceled) ||
		errors.Is(err, quicraft.ErrNotLeader) ||
		errors.Is(err, quicraft.ErrShardNotReady) ||
		errors.Is(err, quicraft.ErrBusy) ||
		errors.Is(err, quicraft.ErrDropped) ||
		errors.Is(err, quicraft.ErrAborted) ||
		errors.Is(err, quicraft.ErrClosed) ||
		errors.Is(err, quicraft.ErrDraining) ||
		errors.Is(err, quicraft.ErrReadIndexNotReady) ||
		errors.Is(err, quicraft.ErrRejected)
}

// findAnyLiveHost returns a live host from the cluster, preferring the leader.
// Falls back to any non-stopped host if the leader is unknown.
func findAnyLiveHost(cluster *testCluster, shardID uint64) *quicraft.Host {
	leaderIdx := cluster.findLeader(shardID)
	if leaderIdx >= 0 {
		cluster.hostsMu.RLock()
		h := cluster.hosts[leaderIdx]
		cluster.hostsMu.RUnlock()
		return h
	}
	cluster.hostsMu.RLock()
	defer cluster.hostsMu.RUnlock()
	for i, h := range cluster.hosts {
		if !cluster.stopped[i].Load() {
			return h
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Client Worker
// ---------------------------------------------------------------------------

// clientWorker runs Put and Get operations against the cluster for the given
// duration. Operations alternate between Put and Get on a shared key space.
// All operations (including failed ones that are retried) are recorded in
// the opRecorder with accurate timestamps for linearizability checking.
func clientWorker(
	cluster *testCluster,
	shardID uint64,
	clientID int,
	numKeys int,
	duration time.Duration,
	recorder *opRecorder,
	opsCount *atomic.Int64,
	done <-chan struct{},
) {
	deadline := time.Now().Add(duration)
	opIdx := 0

	for time.Now().Before(deadline) {
		select {
		case <-done:
			return
		default:
		}

		key := fmt.Sprintf("key-%d", opIdx%numKeys)
		host := findAnyLiveHost(cluster, shardID)
		if host == nil {
			noHostTimer := time.NewTimer(10 * time.Millisecond)
			<-noHostTimer.C
			noHostTimer.Stop()
			continue
		}

		if opIdx%2 == 0 {
			// Put operation.
			value := fmt.Sprintf("v-%d-%d", clientID, opIdx)
			input := kvInput{Op: "put", Key: key, Value: value}

			callTime := time.Now().UnixNano()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := host.SyncPropose(ctx, shardID, encodeKVPut(key, value))
			returnTime := time.Now().UnixNano()
			cancel()

			if err != nil {
				if isRetryableError(err) {
					retryTimer := time.NewTimer(5 * time.Millisecond)
					<-retryTimer.C
					retryTimer.Stop()
					continue
				}
				// Non-retryable error: skip this operation.
				continue
			}

			recorder.record(clientID, input, kvOutput{Ok: true}, callTime, returnTime)
			opsCount.Add(1)
		} else {
			// Get operation via SyncRead (linearizable).
			input := kvInput{Op: "get", Key: key}

			callTime := time.Now().UnixNano()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := host.SyncRead(ctx, shardID, encodeKVGet(key))
			returnTime := time.Now().UnixNano()
			cancel()

			if err != nil {
				if isRetryableError(err) {
					retryTimer := time.NewTimer(5 * time.Millisecond)
					<-retryTimer.C
					retryTimer.Stop()
					continue
				}
				continue
			}

			val := ""
			if result != nil {
				val, _ = result.(string)
			}

			recorder.record(clientID, input, kvOutput{Value: val}, callTime, returnTime)
			opsCount.Add(1)
		}

		opIdx++
	}
}

// ---------------------------------------------------------------------------
// Session Client Worker
// ---------------------------------------------------------------------------

// sessionClientWorker is like clientWorker but uses session-managed proposals
// (ProposeWithSession) for at-most-once delivery semantics. Each client gets
// its own session. Reads still use SyncRead (linearizable).
func sessionClientWorker(
	cluster *testCluster,
	shardID uint64,
	clientID int,
	numKeys int,
	duration time.Duration,
	recorder *opRecorder,
	opsCount *atomic.Int64,
	done <-chan struct{},
) {
	deadline := time.Now().Add(duration)

	// Get a session from any live host.
	var session *quicraft.Session
	sessionTicker := time.NewTicker(10 * time.Millisecond)
	sessionDeadline := time.NewTimer(time.Until(deadline))
	sessionObtained := false
	for !sessionObtained {
		host := findAnyLiveHost(cluster, shardID)
		if host == nil {
			select {
			case <-sessionDeadline.C:
				sessionTicker.Stop()
				sessionDeadline.Stop()
				return
			case <-sessionTicker.C:
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		session, err = host.GetNewSession(ctx, shardID)
		cancel()
		if err != nil {
			if isRetryableError(err) {
				select {
				case <-sessionDeadline.C:
					sessionTicker.Stop()
					sessionDeadline.Stop()
					return
				case <-sessionTicker.C:
				}
				continue
			}
			sessionTicker.Stop()
			sessionDeadline.Stop()
			return // non-retryable error getting session
		}
		sessionObtained = true
	}
	sessionTicker.Stop()
	sessionDeadline.Stop()
	if session == nil {
		return
	}

	opIdx := 0
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return
		default:
		}

		key := fmt.Sprintf("key-%d", opIdx%numKeys)
		host := findAnyLiveHost(cluster, shardID)
		if host == nil {
			noHostTimer := time.NewTimer(10 * time.Millisecond)
			<-noHostTimer.C
			noHostTimer.Stop()
			continue
		}

		if opIdx%2 == 0 {
			// Put via session-managed proposal.
			value := fmt.Sprintf("v-%d-%d", clientID, opIdx)
			input := kvInput{Op: "put", Key: key, Value: value}

			callTime := time.Now().UnixNano()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := host.SyncProposeWithSession(ctx, session, encodeKVPut(key, value))
			returnTime := time.Now().UnixNano()
			cancel()

			if err != nil {
				if isRetryableError(err) {
					retryTimer := time.NewTimer(5 * time.Millisecond)
					<-retryTimer.C
					retryTimer.Stop()
					continue
				}
				continue
			}

			recorder.record(clientID, input, kvOutput{Ok: true}, callTime, returnTime)
			opsCount.Add(1)
		} else {
			// Get via linearizable read (same as normal clientWorker).
			input := kvInput{Op: "get", Key: key}

			callTime := time.Now().UnixNano()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := host.SyncRead(ctx, shardID, encodeKVGet(key))
			returnTime := time.Now().UnixNano()
			cancel()

			if err != nil {
				if isRetryableError(err) {
					retryTimer := time.NewTimer(5 * time.Millisecond)
					<-retryTimer.C
					retryTimer.Stop()
					continue
				}
				continue
			}

			val := ""
			if result != nil {
				val, _ = result.(string)
			}

			recorder.record(clientID, input, kvOutput{Value: val}, callTime, returnTime)
			opsCount.Add(1)
		}

		opIdx++
	}
}

// ---------------------------------------------------------------------------
// Linearizability Check Helper
// ---------------------------------------------------------------------------

// checkLinearizability runs porcupine on the recorded history and fails the
// test if the result is not Ok. Logs detailed diagnostics on failure.
func checkLinearizability(t *testing.T, recorder *opRecorder, totalOps int64) {
	t.Helper()

	ops := recorder.history()
	t.Logf("checking linearizability: %d operations recorded, %d total completed", len(ops), totalOps)

	if len(ops) == 0 {
		t.Fatal("no operations recorded; cannot verify linearizability")
	}

	model := kvModel()
	checkStart := time.Now()
	result := porcupine.CheckOperationsTimeout(model, ops, 30*time.Second)
	checkDuration := time.Since(checkStart)

	t.Logf("porcupine result: %s (checked %d ops in %v)", result, len(ops), checkDuration)

	if result != porcupine.Ok {
		// Log a sample of operations for debugging.
		sampleSize := len(ops)
		if sampleSize > 20 {
			sampleSize = 20
		}
		var sb strings.Builder
		for i := 0; i < sampleSize; i++ {
			op := ops[i]
			sb.WriteString(fmt.Sprintf("  client=%d input=%v output=%v call=%d return=%d\n",
				op.ClientId, op.Input, op.Output, op.Call, op.Return))
		}
		t.Logf("first %d operations:\n%s", sampleSize, sb.String())
		t.Fatalf("linearizability check failed: result=%s, ops=%d, duration=%v",
			result, len(ops), checkDuration)
	}
}

// ---------------------------------------------------------------------------
// Warmup Helper
// ---------------------------------------------------------------------------

// warmupCluster proposes a few entries to ensure QUIC connections are
// established and the leader has committed an entry in its current term
// (required for ReadIndex per PhD 6.4).
func warmupCluster(t *testing.T, cluster *testCluster, shardID uint64) {
	t.Helper()

	leaderIdx := cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatal("no leader found for warmup")
	}
	leader := cluster.hosts[leaderIdx]

	for i := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		key := fmt.Sprintf("warmup-%d", i)
		_, err := leader.SyncPropose(ctx, shardID, encodeKVPut(key, "ok"))
		cancel()
		if err != nil {
			t.Fatalf("warmup proposal %d failed: %v", i, err)
		}
	}

	// Verify linearizable read works after warmup.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	result, err := leader.SyncRead(ctx, shardID, encodeKVGet("warmup-0"))
	if err != nil {
		t.Fatalf("warmup read failed: %v", err)
	}
	val, ok := result.(string)
	if !ok || val != "ok" {
		t.Fatalf("warmup read returned unexpected value: %v", result)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestLinearizability_3Node_Normal verifies that 8 concurrent clients
// performing Put and Get operations on a 3-node QuicRaft cluster produce
// a linearizable history. This is the core correctness property:
// every read must return the value of the most recent write that
// precedes it in real-time ordering.
func TestLinearizability_3Node_Normal(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_LeaderFailover verifies that linearizability is
// maintained across a leader failover. Clients run continuously while the
// current leader is killed, forcing a new election. Operations that fail
// during failover are retried, and the full history must still be
// linearizable from an external observer's perspective.
func TestLinearizability_3Node_LeaderFailover(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	// Launch clients that run for the full test duration (5s).
	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, 5*time.Second, recorder, &totalOps, done)
		}(i)
	}

	// After 2 seconds of normal operation, kill the leader.
	chaosTimer := time.NewTimer(2 * time.Second)
	<-chaosTimer.C
	chaosTimer.Stop()
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	// Wait for a new leader to be elected from surviving nodes.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d]", newLeaderIdx)

	// Let clients continue for the remaining duration (handled by their
	// own deadline in clientWorker).
	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_5Node_Normal verifies linearizability on a 5-node
// cluster with 8 concurrent clients. The larger cluster provides higher
// fault tolerance (tolerates 2 failures) and exercises the Raft quorum
// logic with more participants.
func TestLinearizability_5Node_Normal(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_5Node_MultipleLeaderFailovers stresses repeated leader
// elections and log reconciliation. Two successive leader kills force back-to-back
// elections while clients continuously issue Put/Get operations. A 5-node
// cluster is used because surviving 2 leader kills requires at least 3
// remaining nodes to form a majority quorum.
func TestLinearizability_5Node_MultipleLeaderFailovers(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 15 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	// First leader kill at 3s.
	firstKillTimer := time.NewTimer(3 * time.Second)
	<-firstKillTimer.C
	firstKillTimer.Stop()
	t.Logf("stopping first leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d]", newLeaderIdx)

	// Second leader kill at 8s.
	secondKillTimer := time.NewTimer(5 * time.Second)
	<-secondKillTimer.C
	secondKillTimer.Stop()
	t.Logf("stopping second leader node[%d]", newLeaderIdx)
	cluster.stopNode(newLeaderIdx)

	finalLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("final leader elected: node[%d]", finalLeaderIdx)

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_5Node_MinorityDown verifies that a 5-node cluster
// continues to serve linearizable operations when 2 follower nodes are
// stopped. The remaining 3 nodes form a majority quorum and must maintain
// correctness under degraded conditions.
func TestLinearizability_5Node_MinorityDown(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 8 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	// At 2s, stop 2 follower nodes (not the leader).
	minorityTimer := time.NewTimer(2 * time.Second)
	<-minorityTimer.C
	minorityTimer.Stop()
	stoppedCount := 0
	for i := range numNodes {
		if i == leaderIdx {
			continue
		}
		if stoppedCount >= 2 {
			break
		}
		t.Logf("stopping follower node[%d]", i)
		cluster.stopNode(i)
		stoppedCount++
	}

	// Confirm leader is still alive with degraded quorum.
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_SingleKeyContention tests serialization correctness
// under maximum write contention. All 8 clients target the same key, creating
// the worst-case scenario for Raft log ordering. The linearizability check
// verifies that all reads observe a consistent sequence of values.
func TestLinearizability_3Node_SingleKeyContention(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 1 // all clients target the same key
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_FollowerRestart verifies that WAL recovery and
// log catchup maintain linearizability. A follower is stopped while clients
// continue writing, then restarted using the same WAL/NodeHost directories.
// The restarted node recovers from its write-ahead log and catches up with
// the cluster. All operations must remain linearizable throughout.
func TestLinearizability_3Node_FollowerRestart(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 12 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	// Pick a follower to stop/restart (not the leader).
	followerIdx := -1
	for i := range numNodes {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	// At 2s, stop the follower.
	stopTimer := time.NewTimer(2 * time.Second)
	<-stopTimer.C
	stopTimer.Stop()
	t.Logf("stopping follower node[%d]", followerIdx)
	cluster.stopNode(followerIdx)

	// At 6s, restart the follower (WAL recovery + log catchup).
	restartTimer := time.NewTimer(4 * time.Second)
	<-restartTimer.C
	restartTimer.Stop()
	t.Logf("restarting follower node[%d]", followerIdx)
	cluster.restartNode(followerIdx, shardID)

	// Wait for the restarted node to rejoin and catch up.
	waitForCondition(t, failoverTimeout, "restarted follower catches up", func() bool {
		if cluster.stopped[followerIdx].Load() {
			return false
		}
		_, err := cluster.hosts[followerIdx].GetShardInfo(shardID)
		return err == nil
	})
	t.Logf("follower node[%d] has rejoined the cluster", followerIdx)

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_5Node_RollingRestart simulates a rolling deployment by
// stopping and restarting follower nodes one at a time. At no point is more
// than one node down, so the 5-node cluster always maintains a 4-node
// majority quorum. Each restarted node recovers from its WAL and catches
// up while clients continue issuing operations. The full operation history
// must be linearizable.
func TestLinearizability_5Node_RollingRestart(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 20 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	// Collect follower indices (skip the leader).
	followers := make([]int, 0, numNodes-1)
	for i := range numNodes {
		if i != leaderIdx {
			followers = append(followers, i)
		}
	}

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	// Rolling restart: stop follower[0] at 3s, restart at 6s.
	rollingTimer0 := time.NewTimer(3 * time.Second)
	<-rollingTimer0.C
	rollingTimer0.Stop()
	t.Logf("stopping follower node[%d]", followers[0])
	cluster.stopNode(followers[0])

	restartTimer0 := time.NewTimer(3 * time.Second)
	<-restartTimer0.C
	restartTimer0.Stop()
	t.Logf("restarting follower node[%d]", followers[0])
	cluster.restartNode(followers[0], shardID)
	waitForCondition(t, failoverTimeout, "follower[0] catches up", func() bool {
		_, err := cluster.hosts[followers[0]].GetShardInfo(shardID)
		return err == nil
	})
	t.Logf("follower node[%d] has rejoined", followers[0])

	// Rolling restart: stop follower[1] at 9s, restart at 12s.
	rollingTimer1 := time.NewTimer(3 * time.Second)
	<-rollingTimer1.C
	rollingTimer1.Stop()
	t.Logf("stopping follower node[%d]", followers[1])
	cluster.stopNode(followers[1])

	restartTimer1 := time.NewTimer(3 * time.Second)
	<-restartTimer1.C
	restartTimer1.Stop()
	t.Logf("restarting follower node[%d]", followers[1])
	cluster.restartNode(followers[1], shardID)
	waitForCondition(t, failoverTimeout, "follower[1] catches up", func() bool {
		_, err := cluster.hosts[followers[1]].GetShardInfo(shardID)
		return err == nil
	})
	t.Logf("follower node[%d] has rejoined", followers[1])

	// Rolling restart: stop follower[2] at 15s, restart at 18s.
	rollingTimer2 := time.NewTimer(3 * time.Second)
	<-rollingTimer2.C
	rollingTimer2.Stop()
	t.Logf("stopping follower node[%d]", followers[2])
	cluster.stopNode(followers[2])

	restartTimer2 := time.NewTimer(3 * time.Second)
	<-restartTimer2.C
	restartTimer2.Stop()
	t.Logf("restarting follower node[%d]", followers[2])
	cluster.restartNode(followers[2], shardID)
	waitForCondition(t, failoverTimeout, "follower[2] catches up", func() bool {
		_, err := cluster.hosts[followers[2]].GetShardInfo(shardID)
		return err == nil
	})
	t.Logf("follower node[%d] has rejoined", followers[2])

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_SnapshotDuringLoad verifies that automatic
// snapshots and log compaction do not violate linearizability. With
// SnapshotEntries set very low (50), multiple snapshots fire during the
// test, exercising the snapshot creation, log truncation, and recovery
// code paths under concurrent client load.
func TestLinearizability_3Node_SnapshotDuringLoad(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 10 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	// Use aggressive snapshot settings to trigger frequent compaction.
	cluster.startShardWithConfig(shardID, func(sid, rid uint64) config.Config {
		c := config.Config{
			ShardID:            sid,
			ReplicaID:          rid,
			SnapshotEntries:    50,
			CompactionOverhead: 10,
		}
		c.SetDefaults()
		return c
	})
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_LeaderRestart verifies that restarting a former
// leader does not violate linearizability. The old leader may have
// uncommitted entries from its leadership term that must be discarded when
// it rejoins as a follower. The restarted node recovers from its WAL and
// adopts the new leader's log.
func TestLinearizability_3Node_LeaderRestart(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 12 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	// At 2s, stop the leader.
	stopLeaderTimer := time.NewTimer(2 * time.Second)
	<-stopLeaderTimer.C
	stopLeaderTimer.Stop()
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	// Wait for a new leader from the remaining 2 nodes.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	t.Logf("new leader elected: node[%d]", newLeaderIdx)

	// At 6s, restart the old leader (becomes follower).
	restartLeaderTimer := time.NewTimer(4 * time.Second)
	<-restartLeaderTimer.C
	restartLeaderTimer.Stop()
	t.Logf("restarting old leader node[%d]", leaderIdx)
	cluster.restartNode(leaderIdx, shardID)

	waitForCondition(t, failoverTimeout, "old leader rejoins as follower", func() bool {
		if cluster.stopped[leaderIdx].Load() {
			return false
		}
		_, err := cluster.hosts[leaderIdx].GetShardInfo(shardID)
		return err == nil
	})
	t.Logf("old leader node[%d] has rejoined the cluster", leaderIdx)

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_5Node_MembershipAddRemove verifies that dynamic
// membership changes (adding and removing nodes) do not violate
// linearizability. A 6th node is added to a running 5-node cluster, then
// a follower is removed. Raft joint consensus ensures safety during
// configuration transitions while clients continuously read and write.
func TestLinearizability_5Node_MembershipAddRemove(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 15 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	// At 3s, add a 6th node.
	addNodeTimer := time.NewTimer(3 * time.Second)
	<-addNodeTimer.C
	addNodeTimer.Stop()
	newIdx := cluster.addNode(shardID)
	t.Logf("added node[%d] to cluster", newIdx)

	// Wait for the new node to catch up.
	waitForCondition(t, failoverTimeout, "new node catches up", func() bool {
		cluster.hostsMu.RLock()
		h := cluster.hosts[newIdx]
		cluster.hostsMu.RUnlock()
		_, err := h.GetShardInfo(shardID)
		return err == nil
	})
	t.Logf("node[%d] has joined and caught up", newIdx)

	// At 8s, remove a follower (not the leader, not the newly added node).
	removeNodeTimer := time.NewTimer(5 * time.Second)
	<-removeNodeTimer.C
	removeNodeTimer.Stop()
	removeIdx := -1
	for i := range numNodes {
		if i != leaderIdx {
			removeIdx = i
			break
		}
	}
	t.Logf("removing follower node[%d]", removeIdx)
	cluster.removeNode(removeIdx, shardID)

	// Confirm the cluster still has a leader after removal.
	cluster.waitForClusterLeader(shardID, failoverTimeout)

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_SessionProposal verifies that session-managed
// proposals (ProposeWithSession) produce a linearizable history. Sessions
// provide at-most-once delivery semantics, ensuring duplicate proposals
// are deduplicated by the state machine. This test exercises the session
// registration, proposal, and series ID advancement paths.
func TestLinearizability_3Node_SessionProposal(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 4
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			sessionClientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_3Node_LeaseRead verifies that lease-based reads
// (which skip the ReadIndex heartbeat round-trip) produce a linearizable
// history. LeaseRead relies on the leader's lease not having expired,
// making it faster but requiring clock assumptions. This test ensures the
// optimization does not sacrifice correctness.
func TestLinearizability_3Node_LeaseRead(t *testing.T) {
	const (
		numNodes   = 3
		numClients = 8
		numKeys    = 10
		shardID    = 1
		testDur    = 5 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	// Enable LeaseRead for all nodes.
	cluster.startShardWithConfig(shardID, func(sid, rid uint64) config.Config {
		c := config.Config{
			ShardID:   sid,
			ReplicaID: rid,
			LeaseRead: true,
		}
		c.SetDefaults()
		return c
	})
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}

// TestLinearizability_5Node_HighThroughput is a sustained high-throughput
// stress test with 16 concurrent clients on a 5-node cluster. The longer
// duration and higher client count exercise internal batching, pipeline
// buffering, and rate limiting paths that shorter tests may not trigger.
func TestLinearizability_5Node_HighThroughput(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 16
		numKeys    = 20
		shardID    = 1
		testDur    = 30 * time.Second
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	recorder := &opRecorder{}
	var totalOps atomic.Int64
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clientWorker(cluster, shardID, clientID, numKeys, testDur, recorder, &totalOps, done)
		}(i)
	}

	wg.Wait()
	close(done)

	checkLinearizability(t, recorder, totalOps.Load())
}
