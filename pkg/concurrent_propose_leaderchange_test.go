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
//   - maybe: the leader's apply is held at a gate, proposals behind it
//     commit (a follower applies them) but cannot complete, and a
//     RequestLeaderTransfer moves leadership away, which fails every
//     handed-off pending proposal MaybeApplied.
//
// The cluster is then drained and checked with the same bounds as the
// workload test, and every held proposal, whose commit was observed, must be
// applied exactly once on every replica.
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
	// The gate sits on a FOLLOWER, which also proposes the held keys, and
	// leadership moves between the other two replicas. A leader whose apply
	// is held never gets its TimeoutNow to the target: the transfer times out
	// and the held calls only return at the test's deadline.
	follower := leaderID%3 + 1
	target := follower%3 + 1
	proposer := c.hosts[follower-1]

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
	c.gate.arm(follower)
	propose(concurrentProposeGateKey)
	select {
	case <-c.gate.reached:
	case <-ctx.Done():
		t.Fatalf("replica %d never reached the gated apply", follower)
	}
	heldKeys := []string{concurrentProposeGateKey}
	for i := range held {
		key := fmt.Sprintf("held-%02d", i)
		heldKeys = append(heldKeys, key)
		propose(key)
	}
	if !waitUntil(20*time.Second, func() bool {
		for _, id := range []uint64{leaderID, target} {
			applied := c.machine(id).snapshot()
			for _, key := range heldKeys {
				if applied[key] != 1 {
					return false
				}
			}
		}
		return true
	}) {
		t.Fatalf("replicas %d and %d never applied the proposals held at replica %d", leaderID, target, follower)
	}
	concurrentProposeTransfer(ctx, t, leader, c.hosts[target-1], target)
	// Every held key is committed, but the proposer's apply is still held:
	// its callers can only learn that the outcome is unknown.
	cancelHeld()
	for range heldKeys {
		select {
		case r := <-results:
			if out.record(r.key, r.err) {
				t.Errorf("held proposal %s refused before submission (%v), want MaybeApplied", r.key, r.err)
			} else if !MaybeApplied(r.err) {
				t.Errorf("held proposal %s = %v, want MaybeApplied", r.key, r.err)
			}
		case <-ctx.Done():
			t.Fatal("a proposal held at the gate never returned after the leader transfer")
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
	at, hashes := concurrentProposeConverge(ctx, t, c.hosts, want)
	concurrentProposeCheck(t, c, out, barrierKey, barriers)
	for id := uint64(1); id <= 3; id++ {
		applied := c.machine(id).snapshot()
		for _, key := range heldKeys {
			if n := applied[key]; n != 1 {
				t.Errorf("replica %d applied committed held key %s %d times, want 1", id, key, n)
			}
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

// concurrentProposeTransfer moves leadership from `from` to replica target
// (hosted by to), retrying a transfer that times out, until ctx ends.
func concurrentProposeTransfer(ctx context.Context, t *testing.T, from, to *Host, target uint64) {
	t.Helper()
	for {
		err := from.RequestLeaderTransfer(ctx, concurrentProposeShard, target)
		if err != nil && !errors.Is(err, ErrTransferInProgress) {
			t.Fatalf("RequestLeaderTransfer(-> %d): %v", target, err)
		}
		if waitUntil(5*time.Second, func() bool {
			info, err := to.ShardInfo(concurrentProposeShard)
			return err == nil && info.IsLeader
		}) {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("replica %d never took leadership: %v", target, ctx.Err())
		}
	}
}
