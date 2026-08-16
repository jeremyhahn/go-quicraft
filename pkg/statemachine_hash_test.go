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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// hashingStateMachine is a mockStateMachine that also implements sm.Hasher,
// hashing an ORDER-INDEPENDENT digest of its applied entries. Order
// independence is the property that matters: a hash that depends on iteration
// order reports divergence that does not exist, which is worse than having no
// detector at all.
type hashingStateMachine struct {
	mockStateMachine
	sum uint64
}

func (h *hashingStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		// Commutative and associative, so the digest cannot depend on the
		// order entries happen to be visited in.
		h.sum += entries[i].Index * 1000003
		for _, b := range entries[i].Cmd {
			h.sum += uint64(b)
		}
	}
	return h.mockStateMachine.Update(ctx, entries, results)
}

func (h *hashingStateMachine) GetHash() (uint64, error) { return h.sum, nil }

func hashingCreateFunc(shardID, replicaID uint64) interface{} {
	return &hashingStateMachine{}
}

// TestStateMachineHash_DetectsDivergentState pins the facility that detects
// the one failure Raft cannot: a state machine that is not a deterministic
// function of the log.
//
// GetHash existed at every layer — sm.Hasher, rsm.StateMachine.GetHash, all
// three wrappers, and both reference stores implement it order-independently —
// but NO public API exposed it and nothing in the engine called it. The
// mechanism that would catch replica divergence was dead code.
func TestStateMachineHash_DetectsDivergentState(t *testing.T) {
	h := newTestHost(t)

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if err := h.StartShard(members, false, hashingCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx := context.Background()
	if _, err := h.SyncPropose(ctx, 1, []byte("set:alpha=1")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	hash1, idx1, ok, err := h.StateMachineHash(1)
	if err != nil {
		t.Fatalf("StateMachineHash: %v", err)
	}
	if !ok {
		t.Fatal("the state machine implements sm.Hasher but StateMachineHash " +
			"reported ok=false: the detector is not wired through")
	}
	if idx1 == 0 {
		t.Fatal("the reported applied index is 0 after a committed write: " +
			"a comparison across replicas would be meaningless without it")
	}

	// The same state must hash the same way. An order-DEPENDENT hash would
	// report divergence that does not exist, which is worse than no detector.
	hash2, idx2, _, err := h.StateMachineHash(1)
	if err != nil {
		t.Fatalf("StateMachineHash (second): %v", err)
	}
	if hash1 != hash2 || idx1 != idx2 {
		t.Fatalf("two reads of unchanged state disagreed: (%d,%d) vs (%d,%d). "+
			"An order-dependent hash reports divergence that does not exist",
			hash1, idx1, hash2, idx2)
	}

	// Applying more entries must move both the hash and the index, or the
	// detector cannot distinguish states at all.
	if _, err := h.SyncPropose(ctx, 1, []byte("set:beta=2")); err != nil {
		t.Fatalf("SyncPropose (second): %v", err)
	}
	hash3, idx3, _, err := h.StateMachineHash(1)
	if err != nil {
		t.Fatalf("StateMachineHash (third): %v", err)
	}
	if idx3 <= idx1 {
		t.Fatalf("applied index did not advance: %d -> %d", idx1, idx3)
	}
	if hash3 == hash1 {
		t.Fatal("the hash did not change after applying a new entry: it " +
			"cannot distinguish two different states, so it cannot detect " +
			"divergence either")
	}
}

// TestStateMachineHash_ClosedHostAndUnknownShard pins the error contract: a
// diagnostic must not panic on a torn-down host.
func TestStateMachineHash_ClosedHostAndUnknownShard(t *testing.T) {
	h := newTestHost(t)
	if _, _, _, err := h.StateMachineHash(999); err == nil {
		t.Error("StateMachineHash on an unknown shard returned nil error")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, _, err := h.StateMachineHash(1); err == nil {
		t.Error("StateMachineHash after Close returned nil error")
	}
}
