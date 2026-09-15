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
)

// TestRestart_ProposalKeysDoNotCollideAcrossIncarnations is the mechanism
// behind #186, isolated.
//
// A proposal is matched back to its caller by entry.Key. The key used to be
// (replicaID, per-host sequence), and the sequence lived in the Host, so a
// restarted replica -- a NEW Host for the SAME replica ID -- minted the same
// keys its previous incarnation minted, while the raft log still carried the
// previous incarnation's entries under those keys. A restart with no
// snapshot replays the whole log through OnApplied, and the moment a
// replayed old entry with key (R, k) meets a fresh proposal registered under
// (R, k), the fresh proposal is completed with the OLD entry's result: the
// caller is told its write landed while its entry is still in flight, and
// a linearizable read issued on the strength of that acknowledgement
// returns the value from before it. That is the soak's Illegal history.
//
// Keys are random 64-bit draws now and every pending proposal carries a
// floor (see pendingProposal), so this test is the regression net: it
// built the collision out of the old counter, and it must stay quiet
// however keys are minted.
//
// The shape here made the collision certain rather than lucky. Padding
// entries go first so the replay takes long enough for the new proposals to
// register; then the follower proposes `keyed` entries, so the old (R, 1..N)
// keys sit late in the log; then the follower is restarted and the same
// number of proposals are issued through it immediately. Old and new values
// have DIFFERENT lengths, so a result carrying the old length is proof of
// which entry completed the caller, independent of any read.
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
	leader, follower := cluster.hosts[leaderIdx], cluster.hosts[followerIdx]
	cluster.hostsMu.RUnlock()

	// Padding through the leader: no follower-minted keys in this stretch.
	for i := range padding {
		ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
		//quicraft:direct-propose the leader is resolved once above; this is bulk padding
		_, err := leader.SyncPropose(ctx, shardID, encodeKVPut(fmt.Sprintf("pad-%d", i%256), "p"))
		cancel()
		if err != nil {
			t.Fatalf("padding %d: %v", i, err)
		}
	}

	// A gate entry goes in just before the keyed ones. When the follower
	// restarts, its replay parks at this entry -- with the old (R, 1..keyed)
	// entries still ahead of it -- until the new proposals are registered.
	// That is what makes the collision a certainty rather than a race
	// against the machine's speed (#187: on the CI runner the replay of
	// 20,000 entries finished before the first proposal was accepted).
	const gateKey = "gate"
	{
		ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
		//quicraft:direct-propose the leader is resolved once above; this is the gate entry
		_, err := leader.SyncPropose(ctx, shardID, encodeKVPut(gateKey, "closed"))
		cancel()
		if err != nil {
			t.Fatalf("gate entry: %v", err)
		}
	}

	// The follower's first incarnation mints (R, 1..keyed) for these.
	for i := range keyed {
		ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
		//quicraft:direct-propose the follower is the subject: its keys must land in the log
		_, err := follower.SyncPropose(ctx, shardID, encodeKVPut(fmt.Sprintf("k-%d", i), "old"))
		cancel()
		if err != nil {
			t.Fatalf("keyed %d through follower: %v", i, err)
		}
	}

	// Where the log ends before the restart: every pre-restart entry, the
	// (R, 1..keyed) ones included, lies at or below this index.
	logEnd := uint64(0)
	if info, err := leader.GetShardInfo(shardID); err == nil {
		logEnd = info.LastApplied
	}
	if logEnd == 0 {
		t.Fatal("could not read the leader's applied index before the restart")
	}

	// Restart the follower: a new Host, the same replica ID, and a full-log
	// replay ahead of it that will park at the gate entry.
	parked, release := gateApply(t, gateKey)
	// Deferred AFTER cluster.close was, so it runs BEFORE it: Host.Close
	// waits for the parked Update, and a failure while parked would
	// otherwise wedge the shutdown ahead of the t.Cleanup that also releases.
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
	// A restarted follower refuses proposals until it knows the leader; it
	// learns that from the leader's first AppendEntries, which the parked
	// apply does not delay. Every proposer retries the honest refusals; an
	// accepted proposal then sits pending -- the follower cannot apply it
	// while parked -- so once all of them are in flight at once, all of
	// them were registered during the replay. The gate is released then.
	for i := range keyed {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			deadline := time.Now().Add(proposeTimeout)
			for time.Now().Before(deadline) {
				var appliedAtSubmit uint64
				if info, err := follower.GetShardInfo(shardID); err == nil {
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
				ctx, cancel = context.WithTimeout(context.Background(), readTimeout)
				//quicraft:direct-read on the leader, to check the acknowledgement rather than the follower
				got, readErr := leader.SyncRead(ctx, shardID, encodeKVGet(key))
				cancel()
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
