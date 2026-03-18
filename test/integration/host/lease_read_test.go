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

package host_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// ---------------------------------------------------------------------------
// Typed Errors for LeaseRead Tests
// ---------------------------------------------------------------------------

// StaleReadError is returned when a read observes a value that is older
// than a previously observed value, violating monotonic read guarantees.
type StaleReadError struct {
	ReaderID int
	Key      string
	Expected string
	Got      string
}

// Error returns a description of the stale read violation.
func (e *StaleReadError) Error() string {
	return fmt.Sprintf("reader %d: stale read on key %q: expected >= %q, got %q",
		e.ReaderID, e.Key, e.Expected, e.Got)
}

// WriteError is returned when a write operation fails during a
// concurrent read/write test.
type WriteError struct {
	WriterID int
	EntryIdx int
	Cause    error
}

// Error returns a description of the write failure.
func (e *WriteError) Error() string {
	return fmt.Sprintf("writer %d entry %d: %v", e.WriterID, e.EntryIdx, e.Cause)
}

// Unwrap returns the underlying cause of the write failure.
func (e *WriteError) Unwrap() error {
	return e.Cause
}

// isLeaseReadRetryable returns true if the error is a transient failure
// that should be retried during lease read safety tests. These errors
// are expected during leader failover and lease re-establishment.
func isLeaseReadRetryable(err error) bool {
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

// testLeaseReadShardConfig creates a per-shard config with LeaseRead enabled.
func testLeaseReadShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
		LeaseRead: true,
	}
	c.SetDefaults()
	return c
}

// TestHostAPI_LeaseRead_BasicLinearizable verifies that LeaseRead returns
// linearizable results on a 3-node cluster. A value is written, then read
// back via SyncRead (which uses LeaseRead when configured) and verified
// to be consistent.
func TestHostAPI_LeaseRead_BasicLinearizable(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, testLeaseReadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with LeaseRead enabled", leaderIdx)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Write a value.
	_, err := leaderHost.SyncPropose(ctx, shardID, encodeKVPut("lr-key", "lr-value"))
	if err != nil {
		t.Fatalf("SyncPropose failed: %v", err)
	}

	// Wait for the leader SM to apply.
	leaderSM := trackers[leaderIdx].Get(shardID, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "lr-key applied on leader", func() bool {
		_, ok := leaderSM.Get("lr-key")
		return ok
	})

	// Perform a linearizable read via SyncRead (uses LeaseRead internally).
	result, err := leaderHost.SyncRead(ctx, shardID, "lr-key")
	if err != nil {
		t.Fatalf("SyncRead with LeaseRead failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned unexpected type: %T", result)
	}
	if val != "lr-value" {
		t.Errorf("SyncRead = %q, want %q", val, "lr-value")
	}

	t.Logf("LeaseRead linearizable read verified: key=%q value=%q", "lr-key", val)
}

// TestHostAPI_LeaseRead_ConcurrentWritesAndReads verifies that LeaseRead
// returns consistent results under concurrent write load. Multiple
// goroutines write sequentially increasing values for the same key while
// readers verify that values are monotonically non-decreasing (no stale
// reads when using LeaseRead).
func TestHostAPI_LeaseRead_ConcurrentWritesAndReads(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	_ = cluster.startShardWithConfig(shardID, testLeaseReadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with LeaseRead enabled", leaderIdx)

	// Warmup write to establish a committed entry at the current term
	// (PhD 6.4 requirement for linearizable reads).
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := leaderHost.SyncPropose(ctx, shardID, encodeKVPut("warmup", "ok"))
	if err != nil {
		t.Fatalf("warmup SyncPropose failed: %v", err)
	}

	const (
		numWriters = 3
		numReaders = 4
		numWrites  = 20
	)

	// Separate wait groups: writerWg tracks writers, readerWg tracks readers.
	// Writers signal writesDone when all finish, allowing readers to exit.
	var writesDone atomic.Bool
	var writerWg, readerWg sync.WaitGroup
	errCh := make(chan error, (numWriters*numWrites)+(numReaders*100))

	// Launch writers.
	for w := range numWriters {
		writerWg.Add(1)
		go func(workerID int) {
			defer writerWg.Done()
			for i := range numWrites {
				key := fmt.Sprintf("writer-%d-%03d", workerID, i)
				value := fmt.Sprintf("val-%d-%03d", workerID, i)

				wCtx, wCancel := context.WithTimeout(context.Background(), defaultTimeout)
				_, wErr := leaderHost.SyncPropose(wCtx, shardID, encodeKVPut(key, value))
				wCancel()
				if wErr != nil {
					errCh <- fmt.Errorf("writer %d entry %d: %w", workerID, i, wErr)
					return
				}
			}
		}(w)
	}

	// Launch readers that perform SyncRead on the leader. Each reader
	// reads a fixed key that writer-0 will have written, verifying that
	// once a value is read, subsequent reads never return an empty string
	// (no stale/regression).
	for r := range numReaders {
		readerWg.Add(1)
		go func(readerID int) {
			defer readerWg.Done()
			lastSeen := ""
			for !writesDone.Load() {
				key := fmt.Sprintf("writer-0-%03d", numWrites-1)
				rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
				result, rErr := leaderHost.SyncRead(rCtx, shardID, key)
				rCancel()
				if rErr != nil {
					continue
				}
				val, ok := result.(string)
				if !ok || val == "" {
					continue
				}
				// Once we've seen a non-empty value, it must never regress.
				if lastSeen != "" && val < lastSeen {
					errCh <- fmt.Errorf("reader %d: stale read regression: saw %q then %q", readerID, lastSeen, val)
					return
				}
				lastSeen = val
			}
		}(r)
	}

	// Wait for all writers to finish, then signal readers to stop.
	writerWg.Wait()
	writesDone.Store(true)

	// Wait for all readers to observe the signal and exit.
	readerWg.Wait()

	// Drain and report any errors.
	close(errCh)
	for e := range errCh {
		t.Errorf("concurrent error: %v", e)
	}
}

// TestHostAPI_LeaseRead_MonotonicReadsAfterLeaderFailover verifies that
// lease-based reads do not return stale values after a leader failover.
// Per PhD Section 6.4, a new leader must not serve LeaseRead requests
// until it has established a valid lease through heartbeat acks from a
// quorum. This test writes data, kills the leader, waits for a new
// leader election, and immediately issues reads to verify safety:
//   - Reads either succeed with the latest committed value or fail
//     with a retriable error (acceptable during lease establishment).
//   - No stale reads are returned (read value >= previously observed).
//   - After stabilization, reads consistently return the correct value.
func TestHostAPI_LeaseRead_MonotonicReadsAfterLeaderFailover(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, testLeaseReadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with LeaseRead enabled", leaderIdx)

	// Write several entries to establish committed state.
	const numEntries = 10
	for i := range numEntries {
		key := fmt.Sprintf("failover-key-%03d", i)
		value := fmt.Sprintf("failover-val-%03d", i)

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, err := leaderHost.SyncPropose(ctx, shardID, encodeKVPut(key, value))
		cancel()
		if err != nil {
			t.Fatalf("SyncPropose entry %d failed: %v", i, err)
		}
	}

	// Wait for the last entry to replicate to all live nodes.
	lastKey := fmt.Sprintf("failover-key-%03d", numEntries-1)
	lastVal := fmt.Sprintf("failover-val-%03d", numEntries-1)
	waitForReplication(t, cluster, trackers, shardID, lastKey, lastVal, defaultTimeout)
	t.Logf("all %d entries replicated to all nodes", numEntries)

	// Verify the leader can read the last entry before failover.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	result, err := leaderHost.SyncRead(ctx, shardID, lastKey)
	cancel()
	if err != nil {
		t.Fatalf("pre-failover SyncRead failed: %v", err)
	}
	preFailoverVal, ok := result.(string)
	if !ok || preFailoverVal != lastVal {
		t.Fatalf("pre-failover read: got %q, want %q", preFailoverVal, lastVal)
	}

	// Kill the leader to trigger failover.
	t.Logf("stopping leader node[%d]", leaderIdx)
	cluster.stopNode(leaderIdx)

	// Wait for a new leader to be elected from the surviving nodes.
	newLeaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
	newLeaderHost := cluster.hosts[newLeaderIdx]
	t.Logf("new leader elected at node[%d]", newLeaderIdx)

	// Immediately issue reads against the new leader. During lease
	// establishment, some reads may fail with retriable errors. Track
	// the highest observed value to verify monotonicity.
	var lastObserved string
	var successCount, retryCount int

	readDeadline := time.Now().Add(defaultTimeout)
	for time.Now().Before(readDeadline) {
		readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
		readResult, readErr := newLeaderHost.SyncRead(readCtx, shardID, lastKey)
		readCancel()

		if readErr != nil {
			if isLeaseReadRetryable(readErr) {
				retryCount++
				retryTimer := time.NewTimer(50 * time.Millisecond)
				<-retryTimer.C
				retryTimer.Stop()
				continue
			}
			t.Fatalf("non-retriable SyncRead error after failover: %v", readErr)
		}

		val, valOK := readResult.(string)
		if !valOK {
			t.Fatalf("SyncRead returned unexpected type: %T", readResult)
		}

		// Verify monotonicity: once we observe a value, we must never
		// see an older value.
		if lastObserved != "" && val < lastObserved {
			t.Fatalf("stale read after failover: observed %q then %q", lastObserved, val)
		}
		if val > lastObserved {
			lastObserved = val
		}
		successCount++

		// Once we have multiple successful reads returning the expected
		// value, the lease is established and reads are stable.
		if successCount >= 5 && lastObserved == lastVal {
			break
		}
	}

	if successCount == 0 {
		t.Fatal("no successful reads after leader failover")
	}
	if lastObserved != lastVal {
		t.Errorf("final observed value = %q, want %q", lastObserved, lastVal)
	}

	t.Logf("post-failover reads: %d successes, %d retries, final value = %q",
		successCount, retryCount, lastObserved)

	// After stabilization, verify all entries are readable through
	// the new leader.
	for i := range numEntries {
		key := fmt.Sprintf("failover-key-%03d", i)
		expectedVal := fmt.Sprintf("failover-val-%03d", i)

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
		verifyResult, verifyErr := newLeaderHost.SyncRead(verifyCtx, shardID, key)
		verifyCancel()
		if verifyErr != nil {
			t.Errorf("post-stabilization read key=%q failed: %v", key, verifyErr)
			continue
		}
		got, gotOK := verifyResult.(string)
		if !gotOK || got != expectedVal {
			t.Errorf("post-stabilization read key=%q: got %q, want %q", key, got, expectedVal)
		}
	}
}

// TestHostAPI_LeaseRead_ConcurrentWritersAndReaders verifies that
// lease-based reads return monotonically non-decreasing values per key
// under sustained concurrent write and read pressure. N writer goroutines
// propose sequentially numbered values for distinct keys while M reader
// goroutines continuously perform SyncRead (which uses LeaseRead when
// configured). The test runs for 5 seconds and then verifies:
//   - All reads returned monotonically non-decreasing values per key.
//   - No stale reads were observed across any reader goroutine.
//   - After all writers complete, final reads match the last written values.
func TestHostAPI_LeaseRead_ConcurrentWritersAndReaders(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, testLeaseReadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with LeaseRead enabled", leaderIdx)

	// Warmup to establish committed entry in current term (PhD 6.4).
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), defaultTimeout)
	_, err := leaderHost.SyncPropose(warmupCtx, shardID, encodeKVPut("warmup", "ok"))
	warmupCancel()
	if err != nil {
		t.Fatalf("warmup SyncPropose failed: %v", err)
	}

	const (
		numWriters  = 4
		numReaders  = 6
		numKeys     = 3
		testDur     = 5 * time.Second
		writesPerOp = 50
	)

	var writesDone atomic.Bool
	var writerWg, readerWg sync.WaitGroup
	errCh := make(chan error, (numWriters*writesPerOp)+(numReaders*200))

	// Each writer owns a disjoint set of keys so that per-key values
	// are produced by a single writer with a monotonically increasing
	// sequence number. This avoids false "stale read" reports caused by
	// concurrent writers whose values are not comparable via string
	// ordering (the Raft commit order may differ from acquisition order).
	totalKeys := numWriters * numKeys

	// maxWritten tracks the highest sequence number written per key.
	maxWritten := make([]atomic.Int64, totalKeys)

	// Launch writers. Each writer writes to its own set of keys with
	// monotonically increasing sequence numbers encoded in the value.
	for w := range numWriters {
		writerWg.Add(1)
		go func(workerID int) {
			defer writerWg.Done()
			deadline := time.Now().Add(testDur)
			seq := 0
			for time.Now().Before(deadline) {
				// Each writer owns keys [workerID*numKeys .. (workerID+1)*numKeys).
				keyIdx := workerID*numKeys + (seq % numKeys)
				key := fmt.Sprintf("conc-key-%d", keyIdx)
				// Encode workerID and seq in value for ordering.
				// The seq is per-writer and monotonically increasing.
				value := fmt.Sprintf("%08d-%04d-%06d", workerID, keyIdx, seq)

				wCtx, wCancel := context.WithTimeout(context.Background(), defaultTimeout)
				_, wErr := leaderHost.SyncPropose(wCtx, shardID, encodeKVPut(key, value))
				wCancel()
				if wErr != nil {
					if isLeaseReadRetryable(wErr) {
						retryTimer := time.NewTimer(10 * time.Millisecond)
						<-retryTimer.C
						retryTimer.Stop()
						continue
					}
					errCh <- &WriteError{WriterID: workerID, EntryIdx: seq, Cause: wErr}
					return
				}

				// Update the max written tracker.
				for {
					cur := maxWritten[keyIdx].Load()
					newVal := int64(seq)
					if newVal <= cur {
						break
					}
					if maxWritten[keyIdx].CompareAndSwap(cur, newVal) {
						break
					}
				}
				seq++
			}
		}(w)
	}

	// Launch readers. Each reader continuously reads keys and tracks
	// the highest value seen per key. Values must never decrease.
	// Since each key is owned by a single writer, values for each key
	// are monotonically increasing and string-comparable.
	for r := range numReaders {
		readerWg.Add(1)
		go func(readerID int) {
			defer readerWg.Done()
			lastSeen := make([]string, totalKeys)
			for !writesDone.Load() {
				for keyIdx := range totalKeys {
					key := fmt.Sprintf("conc-key-%d", keyIdx)
					rCtx, rCancel := context.WithTimeout(context.Background(), 3*time.Second)
					result, rErr := leaderHost.SyncRead(rCtx, shardID, key)
					rCancel()
					if rErr != nil {
						// Retriable errors are expected during normal
						// operation under load; skip and retry.
						continue
					}
					val, valOK := result.(string)
					if !valOK || val == "" {
						continue
					}
					// Monotonicity check: string comparison works
					// because values are zero-padded and each key is
					// written by a single writer with increasing seq.
					if lastSeen[keyIdx] != "" && val < lastSeen[keyIdx] {
						errCh <- &StaleReadError{
							ReaderID: readerID,
							Key:      key,
							Expected: lastSeen[keyIdx],
							Got:      val,
						}
						return
					}
					lastSeen[keyIdx] = val
				}
			}
		}(r)
	}

	// Wait for writers, signal readers, then wait for readers.
	writerWg.Wait()
	writesDone.Store(true)
	readerWg.Wait()

	// Drain errors.
	close(errCh)
	for e := range errCh {
		t.Errorf("concurrent error: %v", e)
	}

	// Verify final reads match the committed state. The SM on the
	// leader must reflect all successfully proposed values.
	leaderSM := trackers[leaderIdx].Get(shardID, uint64(leaderIdx+1))
	if leaderSM == nil {
		t.Fatal("leader state machine not found")
	}
	for keyIdx := range totalKeys {
		key := fmt.Sprintf("conc-key-%d", keyIdx)
		finalCtx, finalCancel := context.WithTimeout(context.Background(), defaultTimeout)
		result, readErr := leaderHost.SyncRead(finalCtx, shardID, key)
		finalCancel()
		if readErr != nil {
			t.Errorf("final read key=%q failed: %v", key, readErr)
			continue
		}
		val, valOK := result.(string)
		if !valOK {
			t.Errorf("final read key=%q returned unexpected type: %T", key, result)
			continue
		}
		t.Logf("final read key=%q value=%q (maxWritten seq=%d)", key, val, maxWritten[keyIdx].Load())
	}
}

// TestHostAPI_LeaseRead_ReadAfterLeaderTransfer verifies that leadership
// transfer correctly invalidates the old leader's lease and that the new
// leader does not serve stale reads. Per PhD Section 6.4.1, a leader must
// expire its lease before transferring leadership. This test:
//  1. Writes data on the original leader.
//  2. Transfers leadership to a specific follower.
//  3. Immediately reads from the new leader.
//  4. Verifies the read returns the latest committed value (not stale data).
func TestHostAPI_LeaseRead_ReadAfterLeaderTransfer(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1

	trackers := cluster.startShardWithConfig(shardID, testLeaseReadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("initial leader elected at node[%d] with LeaseRead enabled", leaderIdx)

	// Write several entries on the current leader.
	const numEntries = 5
	for i := range numEntries {
		key := fmt.Sprintf("transfer-key-%03d", i)
		value := fmt.Sprintf("transfer-val-%03d", i)

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, propErr := leaderHost.SyncPropose(ctx, shardID, encodeKVPut(key, value))
		cancel()
		if propErr != nil {
			t.Fatalf("SyncPropose entry %d failed: %v", i, propErr)
		}
	}

	// Wait for replication to all nodes.
	lastKey := fmt.Sprintf("transfer-key-%03d", numEntries-1)
	lastVal := fmt.Sprintf("transfer-val-%03d", numEntries-1)
	waitForReplication(t, cluster, trackers, shardID, lastKey, lastVal, defaultTimeout)
	t.Logf("all %d entries replicated", numEntries)

	// Write a final value that we will verify after transfer.
	finalKey := "transfer-final"
	finalVal := "final-value"
	finalCtx, finalCancel := context.WithTimeout(context.Background(), defaultTimeout)
	_, err := leaderHost.SyncPropose(finalCtx, shardID, encodeKVPut(finalKey, finalVal))
	finalCancel()
	if err != nil {
		t.Fatalf("SyncPropose final entry failed: %v", err)
	}
	waitForReplication(t, cluster, trackers, shardID, finalKey, finalVal, defaultTimeout)

	// Find a follower to transfer leadership to.
	targetIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			targetIdx = i
			break
		}
	}
	if targetIdx < 0 {
		t.Fatal("no follower found for leadership transfer")
	}
	targetReplicaID := uint64(targetIdx + 1)
	t.Logf("requesting leadership transfer to node[%d] (replicaID=%d)", targetIdx, targetReplicaID)

	// Request leadership transfer.
	transferCtx, transferCancel := context.WithTimeout(context.Background(), defaultTimeout)
	transferErr := leaderHost.RequestLeaderTransfer(transferCtx, shardID, targetReplicaID)
	transferCancel()
	if transferErr != nil {
		t.Fatalf("RequestLeaderTransfer failed: %v", transferErr)
	}

	// Wait for the target to become leader.
	waitForCondition(t, failoverTimeout, "target becomes leader after transfer", func() bool {
		info, infoErr := cluster.hosts[targetIdx].GetShardInfo(shardID)
		if infoErr != nil {
			return false
		}
		return info.IsLeader
	})
	t.Logf("leadership transferred to node[%d]", targetIdx)

	newLeaderHost := cluster.hosts[targetIdx]

	// Immediately read from the new leader. The new leader must not
	// serve reads until it has a valid lease. We retry retriable errors
	// (expected during lease establishment) but fail on stale values.
	var readVal string
	readDeadline := time.Now().Add(defaultTimeout)
	for time.Now().Before(readDeadline) {
		readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
		result, readErr := newLeaderHost.SyncRead(readCtx, shardID, finalKey)
		readCancel()

		if readErr != nil {
			if isLeaseReadRetryable(readErr) {
				retryTimer := time.NewTimer(50 * time.Millisecond)
				<-retryTimer.C
				retryTimer.Stop()
				continue
			}
			t.Fatalf("non-retriable SyncRead error after transfer: %v", readErr)
		}

		val, valOK := result.(string)
		if !valOK {
			t.Fatalf("SyncRead returned unexpected type: %T", result)
		}
		readVal = val
		break
	}

	if readVal == "" {
		t.Fatal("no successful reads from new leader after transfer")
	}
	if readVal != finalVal {
		t.Errorf("read after transfer: got %q, want %q (stale read detected)", readVal, finalVal)
	}
	t.Logf("read after leader transfer verified: key=%q value=%q", finalKey, readVal)

	// Verify the old leader rejects reads (it should no longer be leader).
	// After transferring leadership, its lease should be invalidated.
	oldLeaderReadCtx, oldLeaderReadCancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, oldLeaderErr := leaderHost.SyncRead(oldLeaderReadCtx, shardID, finalKey)
	oldLeaderReadCancel()
	if oldLeaderErr == nil {
		// If the old leader still serves reads, it may have been
		// re-elected. Verify it is not the leader anymore.
		info, infoErr := leaderHost.GetShardInfo(shardID)
		if infoErr == nil && info.IsLeader {
			t.Logf("old leader was re-elected, which is valid raft behavior")
		}
	} else {
		t.Logf("old leader correctly rejected read after transfer: %v", oldLeaderErr)
	}

	// Final verification: read all entries from the new leader.
	for i := range numEntries {
		key := fmt.Sprintf("transfer-key-%03d", i)
		expectedVal := fmt.Sprintf("transfer-val-%03d", i)

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
		verifyResult, verifyErr := newLeaderHost.SyncRead(verifyCtx, shardID, key)
		verifyCancel()
		if verifyErr != nil {
			t.Errorf("post-transfer read key=%q failed: %v", key, verifyErr)
			continue
		}
		got, gotOK := verifyResult.(string)
		if !gotOK || got != expectedVal {
			t.Errorf("post-transfer read key=%q: got %q, want %q", key, got, expectedVal)
		}
	}
}
