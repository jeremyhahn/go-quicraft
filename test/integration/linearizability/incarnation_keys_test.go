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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// TestRestart_ProposalKeysDoNotCollideAcrossIncarnations: a restarted follower's new proposals are never completed by replayed entries from its previous incarnation.
func TestRestart_ProposalKeysDoNotCollideAcrossIncarnations(t *testing.T) {
	const (
		numNodes = 3
		shardID  = 1
		padding  = 20000
		keyed    = 64
	)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()
	cluster.startShard(shardID)
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)

	followerIdx := (leaderIdx + 1) % numNodes
	cluster.hostsMu.RLock()
	follower := cluster.hosts[followerIdx]
	cluster.hostsMu.RUnlock()

	// Pad through whichever replica leads at each proposal, budgeted failoverTimeout; a retried put may apply twice.
	for i := range padding {
		proposeToLeader(t, cluster, shardID, encodeKVPut(fmt.Sprintf("pad-%d", i%256), "p"), failoverTimeout)
	}

	// A gate entry before the keyed ones parks the follower's replay until the new proposals are registered.
	const gateKey = "gate"
	proposeToLeader(t, cluster, shardID, encodeKVPut(gateKey, "closed"), failoverTimeout)

	// Keyed proposals are pinned to the follower, retrying it with quicraft.Temporary.
	for i := range keyed {
		if _, err := proposer.TryWith(proposer.Pinned(follower), shardID,
			encodeKVPut(fmt.Sprintf("k-%d", i), "old"), failoverTimeout, quicraft.Temporary); err != nil {
			t.Fatalf("keyed %d through follower: %v", i, err)
		}
	}

	// The follower's LastApplied bounds every pre-restart entry.
	logEnd := uint64(0)
	if info, err := follower.ShardInfo(shardID); err == nil {
		logEnd = info.LastApplied
	}
	if logEnd == 0 {
		t.Fatal("could not read the follower's applied index before the restart")
	}

	// Restart the follower with the gate armed for its replica alone.
	parked, release := gateApply(t, gateKey, shardID, uint64(followerIdx+1))
	// Deferred after cluster.close so it runs first; Close waits for the parked Update.
	defer release()
	cluster.stopNode(followerIdx)
	cluster.restartNode(followerIdx, shardID)
	cluster.hostsMu.RLock()
	follower = cluster.hosts[followerIdx]
	cluster.hostsMu.RUnlock()
	select {
	case <-parked:
	case <-time.After(failoverTimeout):
		t.Fatalf("the restarted follower's replay did not reach the gate entry within %v", failoverTimeout)
	}

	const newValue = "new-value" // 9 bytes; "old" is 3
	var (
		mu        sync.Mutex
		falseAcks []string
		accepted  int
		firstAt   uint64 = ^uint64(0)
		inFlight  atomic.Int64
		wg        sync.WaitGroup
		stop      = make(chan struct{}) // closed to stop the proposers early
	)
	// Once every proposal is in flight, all were registered during the replay; release the gate.
	for i := range keyed {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			// Retry refusals for failoverTimeout plus one proposeTimeout; a closed stop ends the retries.
			deadline := time.Now().Add(failoverTimeout + proposeTimeout)
			for time.Now().Before(deadline) {
				select {
				case <-stop:
					return
				default:
				}
				var appliedAtSubmit uint64
				if info, err := follower.ShardInfo(shardID); err == nil {
					appliedAtSubmit = info.LastApplied
				}
				ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
				inFlight.Add(1)
				//quicraft:direct-propose the restarted follower is the subject
				res, err := follower.SyncPropose(ctx, shardID, encodeKVPut(key, newValue))
				inFlight.Add(-1)
				cancel()
				if err != nil {
					pause(time.Millisecond, stop)
					continue
				}
				mu.Lock()
				accepted++
				if appliedAtSubmit < firstAt {
					firstAt = appliedAtSubmit
				}
				mu.Unlock()
				if res.Value != uint64(len(newValue)) {
					mu.Lock()
					falseAcks = append(falseAcks, fmt.Sprintf("%s: acknowledged with result length %d, wrote %d (follower applied=%d at submit) -- another entry's result", key, res.Value, len(newValue), appliedAtSubmit))
					mu.Unlock()
					return
				}
				// Check the acknowledgement with a linearizable read through the current leader.
				got, readErr := proposer.TryRead(cluster.leaderHost, shardID, encodeKVGet(key), readTimeout)
				if readErr != nil {
					t.Errorf("read after ack of %s: %v", key, readErr)
					return
				}
				if got != newValue {
					mu.Lock()
					falseAcks = append(falseAcks, fmt.Sprintf("%s: acknowledged (follower applied=%d at submit), but a linearizable read returned %q", key, appliedAtSubmit, got))
					mu.Unlock()
				}
				return
			}
		}(i)
	}
	// Wait until every proposer is inside SyncPropose at once -- a refusal
	// returns immediately, so a proposer that stays inside for a while has
	// been accepted -- then let the replay through.
	allIn := time.NewTimer(failoverTimeout)
	defer allIn.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for stable := 0; stable < 10; {
		select {
		case <-tick.C:
			if inFlight.Load() == keyed {
				stable++
			} else {
				stable = 0
			}
		case <-allIn.C:
			close(stop)
			release()
			wg.Wait()
			t.Fatalf("only %d of %d proposals were in flight at once within %v; the collision window was never exercised", inFlight.Load(), keyed, failoverTimeout)
		}
	}
	release()
	wg.Wait()
	t.Logf("restarted follower: %d of %d proposals accepted; the earliest was submitted with the replay parked at applied=%d of a log that ended at %d before the restart", accepted, keyed, firstAt, logEnd)
	// The test means nothing unless a reused key met the replay: at least one
	// proposal must have been accepted while the follower's applied marker
	// was still inside the pre-restart log. If the replay had finished first,
	// every old (R, k) entry was already applied before the new (R, k) was
	// registered, and a pass proves nothing about the collision.
	if accepted == 0 {
		t.Fatal("no proposal was accepted by the restarted follower; the collision window was never exercised")
	}
	if firstAt >= logEnd {
		t.Fatalf("a proposal was accepted after the replay had passed the pre-restart log end (%d >= %d), which the gate should have made impossible", firstAt, logEnd)
	}
	if len(falseAcks) > 0 {
		t.Fatalf("%d of %d proposals through the restarted replica were acknowledged for an entry that was not theirs:\n  %s",
			len(falseAcks), keyed, strings.Join(falseAcks, "\n  "))
	}
}
