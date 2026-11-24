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

package raft_test

import (
	"fmt"
	"sync"
	"testing"
)

// stressWrite is one key/value pair whose proposal COMMITTED AND APPLIED on
// the leader. Only these are asserted on afterwards: a proposal that was
// merely accepted into the pipeline proves nothing about replication.
type stressWrite struct {
	key   string
	value string
}

// TestStress_MultiNode_ConcurrentProposals verifies that concurrent
// proposals from multiple goroutines to a multi-node cluster leader
// are all replicated without loss.
//
// Two things make this test non-vacuous. First, the success floor is
// proportional to the configured workload rather than "at least one": a run
// in which 49 of 50 writes failed is a broken cluster, not a passing stress
// test. Second, replication is verified by looking up every committed
// key/value pair on every replica, not by comparing map sizes — a replica
// holding a completely different set of keys has the same Len().
func TestStress_MultiNode_ConcurrentProposals(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	const (
		numGoroutines         = 5
		proposalsPerGoroutine = 10
	)
	totalProposals := numGoroutines * proposalsPerGoroutine

	// Allow at most 10% of the workload to fail. The cluster is not being
	// faulted here, so the expected result is that every proposal commits;
	// the slack only absorbs an isolated transient rejection.
	minSuccess := totalProposals - totalProposals/10

	var (
		mu        sync.Mutex
		committed []stressWrite
		failures  []string
	)

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for p := 0; p < proposalsPerGoroutine; p++ {
				key := fmt.Sprintf("multi-g%d-p%d", goroutineID, p)
				value := fmt.Sprintf("mval-%d-%d", goroutineID, p)

				// tryProposeToLeader waits for the apply result, so a bare
				// accepted-but-never-committed proposal is not counted as a
				// success. It also re-resolves the leader per attempt: pinning
				// the index sampled before the goroutines started sent every
				// write to a deposed replica after any re-election, which
				// failed the 90% floor below on a cluster that was never
				// faulted.
				//
				// Recorded, not t.Fatal'd: this runs off the test goroutine,
				// where t.Fatal only calls runtime.Goexit on this goroutine and
				// lets the test continue with partial results. Failures are
				// reported by the test goroutine after wg.Wait below.
				_, propErr := tryProposeToLeader(cluster, 1, encodeKVPut(key, value), defaultTimeout)
				if propErr != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("%s: propose: %v", key, propErr))
					mu.Unlock()
					continue
				}

				mu.Lock()
				committed = append(committed, stressWrite{key: key, value: value})
				mu.Unlock()
			}
		}(g)
	}

	wg.Wait()

	mu.Lock()
	committedWrites := append([]stressWrite(nil), committed...)
	observedFailures := append([]string(nil), failures...)
	mu.Unlock()

	for _, f := range observedFailures {
		t.Logf("concurrent write failed: %s", f)
	}
	t.Logf("multi-node concurrent: %d/%d proposals committed",
		len(committedWrites), totalProposals)

	if len(committedWrites) == 0 {
		t.Fatal("no proposals succeeded in multi-node concurrent stress")
	}
	if len(committedWrites) < minSuccess {
		t.Fatalf("only %d/%d concurrent proposals committed, want at least %d",
			len(committedWrites), totalProposals, minSuccess)
	}

	// Every committed write must reach every replica with its exact value.
	for i := range cluster.hosts {
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(1, replicaID)
		if nodeSM == nil {
			t.Fatalf("node[%d] state machine not found", i)
		}

		waitForCondition(t, defaultTimeout*4,
			fmt.Sprintf("node[%d] replicated all %d committed writes", i, len(committedWrites)),
			func() bool {
				for _, w := range committedWrites {
					got, ok := nodeSM.Get(w.key)
					if !ok || got != w.value {
						return false
					}
				}
				return true
			},
		)

		// Re-assert individually so a divergence names the offending key.
		for _, w := range committedWrites {
			assertSMHasExact(t, nodeSM, fmt.Sprintf("node[%d]", i), w.key, w.value)
		}
	}
}
