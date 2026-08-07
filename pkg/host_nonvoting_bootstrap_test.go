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

// ---------------------------------------------------------------------------
// M2: a witness/observer bootstrapped through the members map is a full voter
// ---------------------------------------------------------------------------

// TestStartShard_RejectsWitnessOrObserverInMembers pins the fail-closed half of
// the fix.
//
// The members map seeds proto.Membership.Addresses — the FULL VOTER set — on
// every replica that lists it, and membership is replicated state, so a replica
// cannot hold a role for itself that its peers do not hold for it. A witness
// bootstrapped this way was a full voter in every replica's membership: the
// leader replicated FULL entry payloads to it, it counted as a full replica for
// RequireFullReplicaMajority and toward every quorum, and the only witness-like
// behaviour left was this host's local read gate refusing the reads the
// operator asked the replica to serve. raft.checkSelfRole then latched the
// disagreement permanently (no campaigning, no reads) — a true positive nobody
// could act on, because the shape was created at bootstrap.
//
// The shard must not exist afterwards: a half-started shard would leave the
// operator with the broken configuration the rejection exists to prevent.
func TestStartShard_RejectsWitnessOrObserverInMembers(t *testing.T) {
	tests := []struct {
		name     string
		wantRole string
		mutate   func(*config.Config)
	}{
		{
			name:     "witness",
			wantRole: "witness",
			mutate:   func(c *config.Config) { c.IsWitness = true },
		},
		{
			name:     "observer",
			wantRole: "observer",
			mutate:   func(c *config.Config) { c.IsObserver = true },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHost(t)

			scfg := testShardConfig(1, 2)
			tt.mutate(&scfg)
			members := map[uint64]string{
				1: "127.0.0.1:5000",
				2: "127.0.0.1:5001", // this replica: the bad shape
			}

			err := h.StartShard(members, false, testCreateFunc, scfg)
			if err == nil {
				t.Fatal("StartShard accepted a non-voting replica in the initial " +
					"members map; the replica is a FULL VOTER in the replicated " +
					"membership of every peer")
			}
			var nvErr *NonVotingRoleInMembersError
			if !errors.As(err, &nvErr) {
				t.Fatalf("StartShard error = %v (%T), want *NonVotingRoleInMembersError", err, err)
			}
			if !errors.Is(err, ErrNonVotingRoleInMembers) {
				t.Errorf("error does not match sentinel ErrNonVotingRoleInMembers: %v", err)
			}
			if nvErr.ShardID != 1 || nvErr.ReplicaID != 2 || nvErr.Role != tt.wantRole {
				t.Errorf("error = %+v, want ShardID 1, ReplicaID 2, Role %q", nvErr, tt.wantRole)
			}

			// The rejection must be complete: no shard, no engine node.
			if _, loaded := h.nodes.Load(uint64(1)); loaded {
				t.Error("shard 1 was created despite the rejected configuration")
			}
			if h.engine.GetNode(1) != nil {
				t.Error("engine node for shard 1 was created despite the rejected configuration")
			}

			// The same replica IS startable through the supported flow: it
			// joins with join=true and the leader adds it with a config change.
			if joinErr := h.StartShard(nil, true, testCreateFunc, scfg); joinErr != nil {
				t.Fatalf("StartShard(join=true) for a %s failed: %v", tt.wantRole, joinErr)
			}
		})
	}
}

// TestStartShard_WitnessEntersMembershipThroughConfigChange pins the other half:
// the supported flow puts the witness where the DATA PATH reads it from.
//
// r.witnesses — the map broadcastAppend consults to choose sendWitnessAppend
// (metadata-only entries) over sendAppend (full payloads) — is populated from
// Membership.Witnesses and from nothing else. A witness that is not in that map
// receives full payloads no matter what its local Config says, which is exactly
// what the rejected bootstrap shape produced. This asserts the committed config
// change lands the replica in Membership.Witnesses and NOT in Membership.Nodes
// (the voter set the members map seeds).
//
// pkg/internal/raft.TestWitnessDataPath_FollowsReplicatedMembership pins the
// payload-stripping consequence of that placement directly.
func TestStartShard_WitnessEntersMembershipThroughConfigChange(t *testing.T) {
	h := newTestHost(t)

	scfg := testShardConfig(1, 1)
	// A single-replica shard plus one witness is {1 full + 1 witness}, which
	// the default RequireFullReplicaMajority policy rejects (full 1 < quorum 2).
	// The policy is not what this test is about, so seed it off explicitly.
	scfg.Witness.RequireFullReplicaMajority = config.BoolPtr(false)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"},
		false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	before, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}
	if err := h.SyncRequestAddWitness(ctx, 1, 2, "127.0.0.1:5001", before.ConfigChangeID); err != nil {
		t.Fatalf("SyncRequestAddWitness failed: %v", err)
	}

	// Read the applied membership directly rather than through another
	// linearizable fence: the add succeeded, so the config change is committed
	// and applied, and the shard's voting set is now {1 full + 1 witness} with
	// the witness unreachable — CheckQuorum steps the sole leader down, which
	// would make a second ReadIndex fail for reasons this test is not about.
	after, err := h.GetShardMembership(1)
	if err != nil {
		t.Fatalf("GetShardMembership after the add failed: %v", err)
	}
	if _, ok := after.Witnesses[2]; !ok {
		t.Fatalf("replica 2 is not in Membership.Witnesses after the config change: %+v", after)
	}
	if _, ok := after.Nodes[2]; ok {
		t.Fatalf("replica 2 is in Membership.Nodes (the full voter set the members "+
			"map seeds), so the leader would replicate full entry payloads to it: %+v", after)
	}
}
