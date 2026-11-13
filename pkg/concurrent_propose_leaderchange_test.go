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

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"
)

// TestConcurrentPropose_LeaderChangeHitsEveryOutcomeClass forces each
// outcome class the concurrent-propose workload loop files a SyncPropose
// under (proposeOutcomes.record) at a known point, instead of waiting for a
// leader change to happen by chance:
//   - refused: replica 1 runs the shard alone, so no leader can be elected
//     and Host.Propose refuses before submission;
//   - acked: proposals at the elected leader;
//   - maybe: the leader's apply is held at a gate and proposals queue
//     behind it (a host whose apply is held replicates no new entries);
//     both followers are stopped, the leader steps down on CheckQuorum,
//     and cancelling the held calls fails them MaybeApplied.
//
// The cluster is then drained and checked with the same bounds as the
// workload test: the gate key, whose apply was reached, is applied exactly
// once on every replica, and every other held proposal at most once, the
// same number of times on every replica.
func TestConcurrentPropose_LeaderChangeHitsEveryOutcomeClass(t *testing.T) {
	if testing.Short() {
		t.Skip("three-host cluster over loopback QUIC")
	}
	c := newConcurrentProposeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out := newProposeOutcomes()

	c.startShard(t, 0)
	_, err := c.hosts[0].SyncPropose(ctx, concurrentProposeShard, []byte("refused-00"))
	if !out.record("refused-00", err) {
		t.Fatalf("SyncPropose with no leader elected = %v, want a not-leader refusal before submission", err)
	}
	c.startShard(t, 1)
	c.startShard(t, 2)
	leader := c.waitLeader(t)
	info, err := leader.ShardInfo(concurrentProposeShard)
	if err != nil {
		t.Fatalf("ShardInfo: %v", err)
	}
	leaderID := info.ReplicaID
	// The gate holds the LEADER's apply. A host whose apply is held keeps
	// heartbeating but replicates no new entry and never gets a TimeoutNow
	// out, so neither an election nor a transfer moves leadership; losing
	// the quorum does. The held calls wait at the old leader and can only
	// learn that their outcome is unknown.
	proposer := leader

	for i := range 4 {
		key := fmt.Sprintf("acked-%02d", i)
		_, err := leader.SyncPropose(ctx, concurrentProposeShard, []byte(key))
		if out.record(key, err); err != nil {
			t.Fatalf("SyncPropose(%s) at a stable leader: %v", key, err)
		}
	}

	const held = 8
	heldCtx, cancelHeld := context.WithCancel(ctx)
	defer cancelHeld()
	results := make(chan concurrentProposeResult, held+1)
	propose := func(key string) {
		go func() {
			_, err := proposer.SyncPropose(heldCtx, concurrentProposeShard, []byte(key))
			results <- concurrentProposeResult{key, err}
		}()
	}
	c.gate.arm(leaderID)
	propose(concurrentProposeGateKey)
	select {
	case <-c.gate.reached:
	case <-ctx.Done():
		t.Fatalf("replica %d never reached the gated apply", leaderID)
	}
	heldKeys := []string{concurrentProposeGateKey}
	for i := range held {
		key := fmt.Sprintf("held-%02d", i)
		heldKeys = append(heldKeys, key)
		propose(key)
	}
	// Stop both followers: the leader's heartbeats go unanswered and
	// CheckQuorum makes it step down with the held proposals still pending.
	for id := uint64(1); id <= 3; id++ {
		if id == leaderID {
			continue
		}
		c.closeHost(t, c.hosts[id-1])
	}
	if !waitUntil(30*time.Second, func() bool {
		info, err := leader.ShardInfo(concurrentProposeShard)
		return err == nil && !info.IsLeader
	}) {
		t.Fatalf("replica %d never stepped down after losing its quorum", leaderID)
	}
	// The held calls are collected under the live test ctx, not a cancelled
	// one: a cancel would make every held call return a MaybeApplied
	// ErrCanceled whatever the leader-loss path does. OnLeaderUpdated must
	// fail each of them with a MaybeApplied *NotLeaderError on its own.
	// cancelHeld stays only as the deferred cleanup.
	pending := make(map[string]bool, len(heldKeys))
	for _, key := range heldKeys {
		pending[key] = true
	}
	for range heldKeys {
		select {
		case r := <-results:
			delete(pending, r.key)
			if out.record(r.key, r.err) {
				t.Errorf("held proposal %s refused before submission (%v), want MaybeApplied", r.key, r.err)
			} else if !MaybeApplied(r.err) || !errors.Is(r.err, ErrNotLeader) {
				t.Errorf("held proposal %s = %v, want a MaybeApplied ErrNotLeader from the leader-loss path", r.key, r.err)
			}
		case <-ctx.Done():
			stuck := make([]string, 0, len(pending))
			for key := range pending {
				stuck = append(stuck, key)
			}
			sort.Strings(stuck)
			t.Fatalf("held proposals %v never returned after replica %d stepped down: the leader-loss path did not fail them", stuck, leaderID)
		}
	}
	c.gate.open()

	for key, err := range out.failed {
		t.Errorf("SyncPropose(%s): %v", key, err)
	}
	if len(out.acked) == 0 || len(out.maybe) == 0 || len(out.refused) == 0 {
		t.Fatalf("outcome classes hit: acked=%d maybe=%d refused=%d, want every class at least once",
			len(out.acked), len(out.maybe), len(out.refused))
	}

	barrierKey, barriers, want := concurrentProposeDrain(ctx, t, c)
	concurrentProposeCheckPreRestart(t, c, out)
	at, hashes := concurrentProposeConverge(ctx, t, c.hosts, want)
	concurrentProposeCheck(t, c, out, barrierKey, barriers)
	concurrentProposeSameCount(t, c, heldKeys)
	// The gate key reached the old leader's apply, so it was committed.
	for id := uint64(1); id <= 3; id++ {
		if n := c.machine(id).snapshot()[concurrentProposeGateKey]; n != 1 {
			t.Errorf("replica %d applied committed gate key %d times, want 1", id, n)
		}
	}
	if hashes[0] != hashes[1] || hashes[0] != hashes[2] {
		t.Fatalf("replica state hashes differ at index %d: %v", at, hashes)
	}
}

// concurrentProposeResult is one SyncPropose call's key and outcome.
type concurrentProposeResult struct {
	key string
	err error
}

// concurrentProposeSameCount fails the test unless each key is applied at
// most once, the same number of times on every replica.
func concurrentProposeSameCount(t *testing.T, c *concurrentProposeCluster, keys []string) {
	t.Helper()
	for _, key := range keys {
		var counts [3]int
		for id := uint64(1); id <= 3; id++ {
			counts[id-1] = c.machine(id).snapshot()[key]
		}
		if counts[0] > 1 || counts[0] != counts[1] || counts[0] != counts[2] {
			t.Errorf("key %s applied %v times on replicas 1-3, want the same count, at most 1, on every replica", key, counts)
		}
	}
}

// TestConcurrentPropose_ProposalToStoppedFormerLeader is the regression for
// a proposal forwarded to a leader that is gone by the time the replicas are
// compared (#928 review, finding 1). No test hook delays a single forwarded
// message (the only hook is testHookTryRecycle, pkg/requeststate.go; HostOptions
// inject no transport), so the leader's host is stopped instead: a follower
// that still names it forwards the proposals to it, the survivors elect a new
// leader, and the drain's restart brings the stopped host back from its WAL.
// Each orphaned proposal is then applied at most once, the same number of
// times on every replica.
func TestConcurrentPropose_ProposalToStoppedFormerLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("three-host cluster over loopback QUIC")
	}
	c := newConcurrentProposeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out := newProposeOutcomes()
	for i := range 3 {
		c.startShard(t, i)
	}
	leader := c.waitLeader(t)
	info, err := leader.ShardInfo(concurrentProposeShard)
	if err != nil {
		t.Fatalf("ShardInfo: %v", err)
	}
	leaderID := info.ReplicaID
	follower := leaderID%3 + 1
	proposer := c.hosts[follower-1]
	if !waitUntil(10*time.Second, func() bool {
		info, err := proposer.ShardInfo(concurrentProposeShard)
		return err == nil && info.LeaderID == leaderID
	}) {
		t.Fatalf("replica %d never learned leader %d", follower, leaderID)
	}
	// A proposal through the follower proves it forwards to the leader.
	if _, err := proposer.SyncPropose(ctx, concurrentProposeShard, []byte("acked-00")); err != nil {
		t.Fatalf("SyncPropose(acked-00) through a follower: %v", err)
	}
	out.record("acked-00", nil)

	c.closeHost(t, leader)
	const orphans = 8
	orphanCtx, cancelOrphans := context.WithCancel(ctx)
	defer cancelOrphans()
	results := make(chan concurrentProposeResult, orphans)
	keys := make([]string, 0, orphans)
	for i := range orphans {
		key := fmt.Sprintf("orphan-%02d", i)
		keys = append(keys, key)
		go func() {
			_, err := proposer.SyncPropose(orphanCtx, concurrentProposeShard, []byte(key))
			results <- concurrentProposeResult{key, err}
		}()
	}
	if !waitUntil(30*time.Second, func() bool {
		for id := uint64(1); id <= 3; id++ {
			if id == leaderID {
				continue
			}
			info, err := c.hosts[id-1].ShardInfo(concurrentProposeShard)
			if err == nil && info.IsLeader {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("the survivors never elected a leader after replica %d stopped", leaderID)
	}
	cancelOrphans()
	for range keys {
		select {
		case r := <-results:
			out.record(r.key, r.err)
		case <-ctx.Done():
			t.Fatal("a proposal forwarded to the stopped leader never returned")
		}
	}
	for key, err := range out.failed {
		t.Errorf("SyncPropose(%s): %v", key, err)
	}

	barrierKey, barriers, want := concurrentProposeDrain(ctx, t, c)
	concurrentProposeCheckPreRestart(t, c, out)
	at, hashes := concurrentProposeConverge(ctx, t, c.hosts, want)
	concurrentProposeCheck(t, c, out, barrierKey, barriers)
	concurrentProposeSameCount(t, c, keys)
	if hashes[0] != hashes[1] || hashes[0] != hashes[2] {
		t.Fatalf("replica state hashes differ at index %d: %v", at, hashes)
	}
}
