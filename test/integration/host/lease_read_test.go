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
	"strconv"
	"strings"
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

	// Write a value through whoever leads now. The index above is kept for the
	// state machine assertion below, which is satisfied by replication and does
	// not care which replica proposed.
	proposeToLeader(t, cluster, shardID, encodeKVPut("lr-key", "lr-value"), defaultTimeout)

	// Wait for the leader SM to apply.
	leaderSM := trackers[leaderIdx].Get(shardID, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "lr-key applied on leader", func() bool {
		_, ok := leaderSM.Get("lr-key")
		return ok
	})

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

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

	trackers := cluster.startShardWithConfig(shardID, testLeaseReadShardConfig)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]
	t.Logf("leader elected at node[%d] with LeaseRead enabled", leaderIdx)

	// Warmup write to establish a committed entry at the current term
	// (PhD 6.4 requirement for linearizable reads).
	proposeToLeader(t, cluster, shardID, encodeKVPut("warmup", "ok"), defaultTimeout)

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

	// pollKey is the key the readers poll: the last key writer 0 writes.
	pollKey := fmt.Sprintf("writer-0-%03d", numWrites-1)
	pollVal := fmt.Sprintf("val-0-%03d", numWrites-1)

	// pollKeyWritten is set once writer 0's SyncPropose for pollKey has
	// returned successfully, which means the entry is committed and applied.
	// From that instant a linearizable read of pollKey MUST return its value.
	var pollKeyWritten atomic.Bool

	// written records every value a writer committed, for the final check.
	var writtenMu sync.Mutex
	written := make(map[string]string, numWriters*numWrites)

	// Launch writers.
	for w := range numWriters {
		writerWg.Add(1)
		go func(workerID int) {
			defer writerWg.Done()
			for i := range numWrites {
				key := fmt.Sprintf("writer-%d-%03d", workerID, i)
				value := fmt.Sprintf("val-%d-%03d", workerID, i)

				// tryProposeToLeader, not the t-fataling form: this runs on a
				// non-test goroutine, where t.Fatal would only stop this
				// goroutine and let the test continue. Errors go to errCh.
				_, wErr := tryProposeToLeader(cluster, shardID, encodeKVPut(key, value), defaultTimeout)
				if wErr != nil {
					errCh <- fmt.Errorf("writer %d entry %d: %w", workerID, i, wErr)
					return
				}
				writtenMu.Lock()
				written[key] = value
				writtenMu.Unlock()
				if key == pollKey {
					pollKeyWritten.Store(true)
				}
			}
		}(w)
	}

	// Launch readers that perform SyncRead on the leader. Each reader reads a
	// fixed key that writer-0 will have written, verifying that a read issued
	// after that key was committed never comes back empty and that values
	// never regress.
	for r := range numReaders {
		readerWg.Add(1)
		go func(readerID int) {
			defer readerWg.Done()
			lastSeen := ""
			for !writesDone.Load() {
				// Sampled BEFORE the read: if the key was already committed
				// when this read STARTED, a linearizable read must observe
				// it, so an empty value is a stale read and not a race.
				committedBeforeRead := pollKeyWritten.Load()

				rCtx, rCancel := context.WithTimeout(context.Background(), defaultTimeout)
				result, rErr := leaderHost.SyncRead(rCtx, shardID, pollKey)
				rCancel()
				if rErr != nil {
					continue
				}
				val, ok := result.(string)
				if !ok {
					errCh <- fmt.Errorf("reader %d: SyncRead(%q) returned %T, want string",
						readerID, pollKey, result)
					return
				}
				if val == "" {
					// An empty value is what a MISSING key returns. Skipping
					// it as noise — the previous behaviour — discarded
					// exactly the stale read this test exists to detect.
					if committedBeforeRead {
						errCh <- fmt.Errorf(
							"reader %d: stale read on key %q: the key was committed before "+
								"this read started, but the read returned an empty value "+
								"(what a missing key returns)", readerID, pollKey)
						return
					}
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

	// Every write that was accepted must be present with its own value on
	// every node: the reader loop alone asserts nothing about the writes that
	// were not polled.
	writtenMu.Lock()
	finalWritten := make(map[string]string, len(written))
	for k, v := range written {
		finalWritten[k] = v
	}
	writtenMu.Unlock()

	if len(finalWritten) != numWriters*numWrites {
		t.Fatalf("only %d of %d writes were accepted", len(finalWritten), numWriters*numWrites)
	}
	if got := finalWritten[pollKey]; got != pollVal {
		t.Fatalf("poll key %q recorded value %q, want %q", pollKey, got, pollVal)
	}
	verifyAllData(t, cluster, trackers, shardID, finalWritten)
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

		proposeToLeader(t, cluster, shardID, encodeKVPut(key, value), defaultTimeout)
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
	proposeToLeader(t, cluster, shardID, encodeKVPut("warmup", "ok"), defaultTimeout)

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

	// keyWritten records that a key has at least one COMMITTED value. It is
	// what turns an empty read into a provable stale read: before the first
	// write a missing key legitimately reads back as "".
	keyWritten := make([]atomic.Bool, totalKeys)

	// totalWrites counts committed proposals across all writers.
	var totalWrites atomic.Int64

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

				_, wErr := tryProposeToLeader(cluster, shardID, encodeKVPut(key, value), defaultTimeout)
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

				totalWrites.Add(1)

				// Update the max written tracker before publishing the key as
				// written, so a reader that sees keyWritten also sees a
				// maxWritten it can compare the read value against.
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
				keyWritten[keyIdx].Store(true)
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
					// Sampled BEFORE the read: a key already committed when
					// this read STARTS must be visible to a linearizable
					// read, so an empty result is a stale read rather than a
					// race with the writer.
					committedBeforeRead := keyWritten[keyIdx].Load()

					rCtx, rCancel := context.WithTimeout(context.Background(), 3*time.Second)
					result, rErr := leaderHost.SyncRead(rCtx, shardID, key)
					rCancel()
					if rErr != nil {
						// Retriable errors are expected during normal
						// operation under load; skip and retry.
						continue
					}
					val, valOK := result.(string)
					if !valOK {
						errCh <- fmt.Errorf("reader %d: SyncRead(%q) returned %T, want string",
							readerID, key, result)
						return
					}
					if val == "" {
						// An empty value is exactly what a MISSING key
						// returns. Discarding it as noise — the previous
						// behaviour — threw away the stale read this test
						// exists to detect.
						if committedBeforeRead {
							errCh <- &StaleReadError{
								ReaderID: readerID,
								Key:      key,
								Expected: fmt.Sprintf("a committed value (seq >= %d)", maxWritten[keyIdx].Load()),
								Got:      "",
							}
							return
						}
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

	// The writers must actually have written something. A time-boxed writer
	// loop that produced no committed proposals would otherwise leave every
	// read below trivially satisfiable.
	if got := totalWrites.Load(); got < int64(totalKeys) {
		t.Fatalf("writers committed only %d proposals in %v, want at least %d "+
			"(one per key)", got, testDur, totalKeys)
	}
	for keyIdx := range totalKeys {
		if !keyWritten[keyIdx].Load() {
			t.Fatalf("key conc-key-%d was never written, so nothing below verifies it", keyIdx)
		}
	}

	// Verify final reads match the committed state. Every key must read back
	// the value its own writer produced, at a sequence number at least as
	// high as the last one that writer saw committed. The previous version
	// only logged the value, so a read returning an empty string, a foreign
	// writer's value, or a rolled-back sequence number all passed.
	//
	// The read is retried against the CURRENT leader: sustained write load can
	// move leadership, and a read that fails because this host is no longer
	// leader says nothing about the value.
	leaderSM := trackers[leaderIdx].Get(shardID, uint64(leaderIdx+1))
	if leaderSM == nil {
		t.Fatal("leader state machine not found")
	}
	for keyIdx := range totalKeys {
		key := fmt.Sprintf("conc-key-%d", keyIdx)
		wantWriter := uint64(keyIdx / numKeys)
		wantSeq := uint64(maxWritten[keyIdx].Load())

		var lastVal string
		var lastErr error
		ok := waitForConditionNoFatal(failoverTimeout, func() bool {
			readerIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
			finalCtx, finalCancel := context.WithTimeout(context.Background(), defaultTimeout)
			result, readErr := cluster.hosts[readerIdx].SyncRead(finalCtx, shardID, key)
			finalCancel()
			if readErr != nil {
				lastErr = readErr
				return false
			}
			val, valOK := result.(string)
			if !valOK {
				lastErr = fmt.Errorf("SyncRead returned %T, want string", result)
				return false
			}
			lastVal = val
			gotWriter, gotKeyIdx, gotSeq, parseErr := parseConcValue(val)
			if parseErr != nil {
				lastErr = parseErr
				return false
			}
			if gotWriter != wantWriter || gotKeyIdx != uint64(keyIdx) {
				lastErr = fmt.Errorf("value %q belongs to writer %d key %d, want writer %d key %d",
					val, gotWriter, gotKeyIdx, wantWriter, keyIdx)
				return false
			}
			if gotSeq < wantSeq {
				lastErr = fmt.Errorf("value %q has seq %d, want >= %d (the last committed write)",
					val, gotSeq, wantSeq)
				return false
			}
			lastErr = nil
			return true
		})
		if !ok {
			t.Errorf("final read key=%q: got %q, last error: %v", key, lastVal, lastErr)
			continue
		}
		t.Logf("final read key=%q value=%q (maxWritten seq=%d)", key, lastVal, wantSeq)
	}
}

// parseConcValue decodes a value written by the concurrent lease-read writers:
// "%08d-%04d-%06d" of writer id, key index and sequence number.
func parseConcValue(val string) (writerID, keyIdx, seq uint64, err error) {
	parts := strings.Split(val, "-")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("value %q is not a writer-key-seq triple", val)
	}
	fields := [3]uint64{}
	for i, p := range parts {
		v, parseErr := strconv.ParseUint(p, 10, 64)
		if parseErr != nil {
			return 0, 0, 0, fmt.Errorf("value %q has an unparsable field %q: %w", val, p, parseErr)
		}
		fields[i] = v
	}
	return fields[0], fields[1], fields[2], nil
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

		proposeToLeader(t, cluster, shardID, encodeKVPut(key, value), defaultTimeout)
	}

	// Wait for replication to all nodes.
	lastKey := fmt.Sprintf("transfer-key-%03d", numEntries-1)
	lastVal := fmt.Sprintf("transfer-val-%03d", numEntries-1)
	waitForReplication(t, cluster, trackers, shardID, lastKey, lastVal, defaultTimeout)
	t.Logf("all %d entries replicated", numEntries)

	// Write a final value that we will verify after transfer.
	finalKey := "transfer-final"
	finalVal := "final-value"
	proposeToLeader(t, cluster, shardID, encodeKVPut(finalKey, finalVal), defaultTimeout)
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

	// Request leadership transfer and require that it lands on the REQUESTED
	// target rather than on whoever happens to lead next.
	transferLeadershipTo(t, cluster, shardID, targetIdx, failoverTimeout)

	targetInfo, targetErr := cluster.hosts[targetIdx].GetShardInfo(shardID)
	if targetErr != nil {
		t.Fatalf("GetShardInfo on transfer target node[%d] failed: %v", targetIdx, targetErr)
	}
	if !targetInfo.IsLeader {
		t.Fatalf("transfer target node[%d] is not the leader after transfer", targetIdx)
	}
	if targetInfo.ReplicaID != targetReplicaID {
		t.Fatalf("transfer target node[%d] ReplicaID = %d, want %d",
			targetIdx, targetInfo.ReplicaID, targetReplicaID)
	}
	t.Logf("leadership transferred to node[%d] (replica=%d)", targetIdx, targetReplicaID)

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
