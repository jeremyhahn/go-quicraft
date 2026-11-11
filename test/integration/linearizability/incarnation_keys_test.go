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

// TestRestart_ProposalKeysDoNotCollideAcrossIncarnations isolates the
// cross-incarnation key collision.
//
// A proposal is matched back to its caller by entry.Key. Were the key
// (replicaID, per-host sequence) with the sequence living in the Host, a
// restarted replica -- a NEW Host for the SAME replica ID -- would mint the
// same keys its previous incarnation minted, while the raft log still carried the
// previous incarnation's entries under those keys. A restart with no
// snapshot replays the whole log through OnApplied, and the moment a
// replayed old entry with key (R, k) meets a fresh proposal registered under
// (R, k), the fresh proposal is completed with the OLD entry's result: the
// caller is told its write landed while its entry is still in flight, and
// a linearizable read issued on the strength of that acknowledgement
// returns the value from before it. That is the soak's Illegal history.
//
// Keys are random 64-bit draws and every pending proposal carries a floor
// (see pendingProposal), so this test is the regression net: it builds the
// conditions for that collision, and it must stay quiet however keys are
// minted.
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
	follower := cluster.hosts[followerIdx]
	cluster.hostsMu.RUnlock()

	// Padding through whichever replica leads AT EACH PROPOSAL, never the
	// leader observed above: 20,000 sequential writes span many election
	// timeouts, and on a loaded runner leadership moves inside that stretch.
	// A pinned leader then answers "not leader, leader unknown" and the test
	// failed on an election the cluster handled correctly (CI run 18403).
	// Who leads does not matter to the shape; that the entries are NOT
	// follower-minted does, and a leader proposing on its own replica mints
	// its own keys. If the new leader is the follower, its padding keys are
	// minted under a different incarnation's draws and collide with nothing
	// the assertion reads. A retried padding put may apply twice
	// (quicraft.MaybeApplied); it is a last-write-wins put and only lengthens
	// the replay.
	//
	// Each proposal before the restart is budgeted failoverTimeout, not
	// proposeTimeout: these phases only build the log's shape, and over
	// 20,000 writes a loaded runner can lose its leader to a run of split
	// votes that outlasts 5s (batch soak on a loaded host, #309: terms 7-13 in 4s
	// with all three replicas up, then "leader unknown"). An election is
	// granted failoverTimeout everywhere else in this harness; the keyed
	// collision after the restart is what this test asserts, and it keeps
	// its own budgets.
	for i := range padding {
		proposeToLeader(t, cluster, shardID, encodeKVPut(fmt.Sprintf("pad-%d", i%256), "p"), failoverTimeout)
	}

	// A gate entry goes in just before the keyed ones. When the follower
	// restarts, its replay parks at this entry -- with the old (R, 1..keyed)
	// entries still ahead of it -- until the new proposals are registered.
	// That is what makes the collision a certainty rather than a race
	// against the machine's speed (on a CI runner the replay of 20,000
	// entries can finish before the first proposal is accepted).
	// Re-resolved for the same reason as the padding. A retry that applies
	// the gate put twice is harmless: the replay parks at the FIRST one, which
	// still lies ahead of every keyed entry, and release disarms the gate
	// before the second is applied.
	const gateKey = "gate"
	proposeToLeader(t, cluster, shardID, encodeKVPut(gateKey, "closed"), failoverTimeout)

	// The follower's first incarnation mints (R, 1..keyed) for these, so they
	// are PINNED to it: the follower is the subject, and re-resolving would put
	// some other replica's keys in the log. Pinned with quicraft.Temporary is
	// the package's policy for that caller -- wait and ask the SAME replica
	// again, which clears the not-leader window an election opens while the
	// follower learns the successor it forwards to.
	for i := range keyed {
		if _, err := proposer.TryWith(proposer.Pinned(follower), shardID,
			encodeKVPut(fmt.Sprintf("k-%d", i), "old"), failoverTimeout, quicraft.Temporary); err != nil {
			t.Fatalf("keyed %d through follower: %v", i, err)
		}
	}

	// Where the log ends before the restart: every pre-restart entry, the
	// (R, 1..keyed) ones included, lies at or below this index.
	// Read from the follower itself: TryWith returned only after each
	// keyed entry was APPLIED there, so its LastApplied covers them all, and
	// it needs no leader to answer.
	logEnd := uint64(0)
	if info, err := follower.ShardInfo(shardID); err == nil {
		logEnd = info.LastApplied
	}
	if logEnd == 0 {
		t.Fatal("could not read the follower's applied index before the restart")
	}

	// Restart the follower: a new Host, the same replica ID, and a full-log
	// replay ahead of it that will park at the gate entry.
	// The gate is armed for the follower's replica alone. Every replica of
	// this cluster runs in this process, and a peer still behind on the
	// gate entry would otherwise park in the follower's place and close
	// parked before the replay is anywhere near the gate (#309).
	parked, release := gateApply(t, gateKey, shardID, uint64(followerIdx+1))
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
			// Retry refusals for as long as the in-flight wait below can
			// still succeed (failoverTimeout), plus one proposeTimeout for a
			// proposal still pending when the gate is released -- not for
			// proposeTimeout alone: a restarted follower that learns the
			// leader more than 5s after the proposers start would otherwise
			// see every proposer gone, and the wait fail with "only 0 of 64".
			// Each SyncPropose keeps its own proposeTimeout context, and a
			// closed stop ends the retries at once.
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
				// A linearizable read through whichever replica leads NOW, to
				// check the acknowledgement independently of the follower that
				// gave it. Not the leader observed before the restart: a
				// 20,000-entry replay lies between the two, and a deposed
				// leader that has not learned its successor refuses the read.
				// An ack arrives only after the gate is released (the parked
				// follower cannot apply), so even a follower that has since
				// won the shard is a correct reader here.
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
