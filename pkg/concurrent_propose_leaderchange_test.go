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
	target := leaderID%3 + 1

	for i := range 4 {
		key := fmt.Sprintf("acked-%02d", i)
		_, err := leader.SyncPropose(ctx, concurrentProposeShard, []byte(key))
		if out.record(key, err); err != nil {
			t.Fatalf("SyncPropose(%s) at a stable leader: %v", key, err)
		}
	}

	type outcome struct {
		key string
		err error
	}
	const held = 8
	results := make(chan outcome, held+1)
	propose := func(key string) {
		go func() {
			_, err := leader.SyncPropose(ctx, concurrentProposeShard, []byte(key))
			results <- outcome{key, err}
		}()
	}
	c.gate.arm(leaderID)
	propose(concurrentProposeGateKey)
	select {
	case <-c.gate.reached:
	case <-ctx.Done():
		t.Fatal("the leader never reached the gated apply")
	}
	heldKeys := []string{concurrentProposeGateKey}
	for i := range held {
		key := fmt.Sprintf("held-%02d", i)
		heldKeys = append(heldKeys, key)
		propose(key)
	}
	if !waitUntil(20*time.Second, func() bool {
		applied := c.machine(target).snapshot()
		for _, key := range heldKeys {
			if applied[key] != 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("replica %d never applied the proposals held at leader %d", target, leaderID)
	}
	if err := leader.RequestLeaderTransfer(ctx, concurrentProposeShard, target); err != nil {
		t.Fatalf("RequestLeaderTransfer(%d -> %d): %v", leaderID, target, err)
	}
	for range heldKeys {
		select {
		case r := <-results:
			if out.record(r.key, r.err) {
				t.Errorf("held proposal %s refused before submission (%v), want MaybeApplied", r.key, r.err)
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
