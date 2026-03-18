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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestStress_MultiNode_ConcurrentProposals verifies that concurrent
// proposals from multiple goroutines to a multi-node cluster leader
// are all replicated without loss.
func TestStress_MultiNode_ConcurrentProposals(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	numGoroutines := 5
	proposalsPerGoroutine := 10

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for p := 0; p < proposalsPerGoroutine; p++ {
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				key := fmt.Sprintf("multi-g%d-p%d", goroutineID, p)
				value := fmt.Sprintf("mval-%d-%d", goroutineID, p)
				cmd := encodeKVPut(key, value)

				rs, propErr := leaderHost.Propose(ctx, 1, cmd)
				cancel()
				if propErr != nil {
					continue
				}
				rs.Release()
				successCount.Add(1)
			}
		}(g)
	}

	wg.Wait()

	t.Logf("multi-node concurrent: %d/%d proposals succeeded",
		successCount.Load(), int64(numGoroutines*proposalsPerGoroutine))

	if successCount.Load() == 0 {
		t.Fatal("no proposals succeeded in multi-node concurrent stress")
	}

	// Verify replication to all nodes.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "leader SM apply all", func() bool {
		return int64(leaderSM.Len()) >= successCount.Load()
	})

	// Check follower replication.
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		followerSM := trackers[i].Get(1, uint64(i+1))
		if followerSM == nil {
			continue
		}
		waitForCondition(t, defaultTimeout*4,
			fmt.Sprintf("follower[%d] full replication", i),
			func() bool {
				return int64(followerSM.Len()) >= successCount.Load()
			},
		)
	}
}
