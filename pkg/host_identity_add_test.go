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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestHostIdentityAdd pins docs/membership-changes/api.md (Capability: add a
// voter; Capability: add an observer or a witness; Errors) for the
// WithIdentity add calls: each commits the new replica's identity in
// Membership.Identities, refuses an identity an active member holds with
// ErrDuplicateMemberIdentity, refuses an identity the shard removed (under
// any replica ID) with ErrRemovedMemberIdentity, and AddNode cannot promote
// a witness (ErrNodeAlreadyExists). Every subtest runs on its own
// single-voter Host so an added member never leaves a later change waiting
// on a quorum it cannot reach. A witness votes and never starts here, so
// membership is read with the local ShardMembership (a linearizable read
// would wait on it), and the removed identity is an observer's: removing a
// voting witness would wait on the same missing quorum.
func TestHostIdentityAdd(t *testing.T) {
	const witnessAddr = "127.0.0.1:6002"

	// addWitness2 commits witness 2 bound to testIdentity(2) and returns the
	// membership that results.
	addWitness2 := func(t *testing.T, ctx context.Context, h *Host) *Membership {
		t.Helper()
		if err := h.SyncRequestAddWitnessWithIdentity(ctx, 1, 2, witnessAddr, testIdentity(2), 0); err != nil {
			t.Fatalf("SyncRequestAddWitnessWithIdentity(2): %v", err)
		}
		m, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		return m
	}

	// startShard is startSingleVoterShard with RequireFullReplicaMajority
	// off, so the single voter may take a witness without the change being
	// refused for leaving full replicas short of a quorum.
	startShard := func(t *testing.T) *Host {
		t.Helper()
		h := newTestHost(t)
		scfg := testShardConfig(1, 1)
		scfg.CheckQuorum = config.BoolPtr(false)
		scfg.Witness.RequireFullReplicaMajority = config.BoolPtr(false)
		if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard failed: %v", err)
		}
		waitForLeader(t, h, 1)
		return h
	}

	newCtx := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	t.Run("witness add commits the identity", func(t *testing.T) {
		h := startShard(t)
		m := addWitness2(t, newCtx(t), h)
		if got, ok := m.Witnesses[2]; !ok || got != witnessAddr {
			t.Fatalf("Witnesses[2] = %q (present %v), want %q", got, ok, witnessAddr)
		}
		if got := m.Identities[2]; got != testIdentity(2) {
			t.Fatalf("Identities[2] = %q, want %q", got, testIdentity(2))
		}
	})

	t.Run("witness add with an active member's identity is refused", func(t *testing.T) {
		h := startShard(t)
		ctx := newCtx(t)
		addWitness2(t, ctx, h)
		rs, err := h.RequestAddWitnessWithIdentity(ctx, 1, 3, "127.0.0.1:6003", testIdentity(2), 0)
		err = awaitConfigChange(ctx, rs, err)
		if !errors.Is(err, ErrDuplicateMemberIdentity) {
			t.Fatalf("RequestAddWitnessWithIdentity(3, testIdentity(2)) = %v, want ErrDuplicateMemberIdentity", err)
		}
		m, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		if _, ok := m.Witnesses[3]; ok {
			t.Fatal("the refused witness 3 is in Witnesses")
		}
	})

	t.Run("add with a removed member's identity is refused", func(t *testing.T) {
		h := startShard(t)
		ctx := newCtx(t)
		if err := h.SyncRequestAddObserverWithIdentity(ctx, 1, 2, witnessAddr, testIdentity(2), 0); err != nil {
			t.Fatalf("SyncRequestAddObserverWithIdentity(2): %v", err)
		}
		added, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		if err := h.SyncRequestRemoveNode(ctx, 1, 2, added.ConfigChangeID); err != nil {
			t.Fatalf("SyncRequestRemoveNode(2): %v", err)
		}
		m, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		if got := m.RemovedIdentities[2]; got != testIdentity(2) {
			t.Fatalf("RemovedIdentities[2] = %q, want %q", got, testIdentity(2))
		}
		err = h.SyncRequestAddWitnessWithIdentity(ctx, 1, 4, "127.0.0.1:6004", testIdentity(2), m.ConfigChangeID)
		if !errors.Is(err, ErrRemovedMemberIdentity) {
			t.Fatalf("SyncRequestAddWitnessWithIdentity(4, testIdentity(2)) = %v, want ErrRemovedMemberIdentity", err)
		}
	})

	t.Run("AddNode cannot promote a witness", func(t *testing.T) {
		h := startShard(t)
		ctx := newCtx(t)
		added := addWitness2(t, ctx, h)
		err := h.SyncRequestAddNodeWithIdentity(ctx, 1, 2, witnessAddr, testIdentity(2), added.ConfigChangeID)
		if !errors.Is(err, ErrNodeAlreadyExists) {
			t.Fatalf("SyncRequestAddNodeWithIdentity(witness 2) = %v, want ErrNodeAlreadyExists", err)
		}
		m, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		if _, ok := m.Nodes[2]; ok {
			t.Fatal("witness 2 was promoted into Nodes")
		}
	})

	t.Run("voter add commits the identity", func(t *testing.T) {
		h := startShard(t)
		ctx := newCtx(t)
		if err := h.SyncRequestAddNodeWithIdentity(ctx, 1, 5, "127.0.0.1:6005", testIdentity(5), 0); err != nil {
			t.Fatalf("SyncRequestAddNodeWithIdentity(5): %v", err)
		}
		// Replica 5 never starts, so a linearizable read would wait on a
		// quorum of two; the committed membership is read locally.
		m, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		if got, ok := m.Nodes[5]; !ok || got != "127.0.0.1:6005" {
			t.Fatalf("Nodes[5] = %q (present %v), want 127.0.0.1:6005", got, ok)
		}
		if got := m.Identities[5]; got != testIdentity(5) {
			t.Fatalf("Identities[5] = %q, want %q", got, testIdentity(5))
		}
	})
}
