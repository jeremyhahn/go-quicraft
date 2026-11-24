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
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// A witness/observer bootstrapped through the members map is a full voter
// ---------------------------------------------------------------------------

// TestStartShard_RejectsWitnessOrObserverInMembers pins that bootstrap refuses a witness or observer in members and leaves no shard behind.
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

// The refusal above must not apply to a restart.

// TestStartShard_NonVotingRestartWithRecoveredMembership pins that a witness or observer added by config change restarts with join=false, without the joining marker.
func TestStartShard_NonVotingRestartWithRecoveredMembership(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{name: "witness", mutate: func(c *config.Config) { c.IsWitness = true }},
		{name: "observer", mutate: func(c *config.Config) { c.IsObserver = true }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const shardID, replicaID uint64 = 61, 2
			const voterAddr, selfAddr = "127.0.0.1:5000", "127.0.0.1:5001"
			payload := []byte("witness restart state")

			cfg := testHostConfig(t)
			db := memdb.New()
			h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost failed: %v", err)
			}
			defer h.Close()

			// Install a snapshot whose replicated membership carries the role, through the public import path.
			if bsErr := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
				Addresses: map[uint64]string{1: voterAddr},
			}); bsErr != nil {
				t.Fatalf("SaveBootstrap failed: %v", bsErr)
			}
			memb := proto.Membership{
				ConfigChangeID: 4,
				Addresses:      map[uint64]string{1: voterAddr},
			}
			scfg := testShardConfig(shardID, replicaID)
			tt.mutate(&scfg)
			scfg.Witness.RequireFullReplicaMajority = config.BoolPtr(false)
			if scfg.IsWitness {
				memb.Witnesses = map[uint64]string{replicaID: selfAddr}
			} else {
				memb.Observers = map[uint64]string{replicaID: selfAddr}
			}
			artifact := buildSnapshotArtifact(t, 900, 5, 0, memb, snapshotEnvelope(payload))
			if impErr := h.ImportSnapshot(context.Background(), shardID,
				bytes.NewReader(artifact)); impErr != nil {
				t.Fatalf("seeding the recovered snapshot: %v", impErr)
			}

			// The restart the operator actually performs: the same members map
			// as every other replica, join=false.
			members := map[uint64]string{1: voterAddr, replicaID: selfAddr}
			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := h.StartShard(members, false, create, scfg); startErr != nil {
				t.Fatalf("restarting a %s with a recovered membership failed: %v; "+
					"there is then NO working restart path for this replica",
					tt.name, startErr)
			}
			defer func() { _ = h.StopShard(shardID) }()

			if !memSM.recoverCalled.Load() {
				t.Fatal("the recovered snapshot was not applied, so this test is not " +
					"exercising a restart")
			}

			// Consequence 1: the shard must NOT be in its join bootstrap window.
			if h.IsShardJoining(shardID) {
				t.Error("the restarted replica is marked joining; the marker is cleared " +
					"only by an InstallSnapshot adopt, which a caught-up witness never " +
					"receives, so it would never clear")
			}

			// Consequence 2: per-shard message authorization must be ON. A
			// joining peer answers IsMember true for every replica ID.
			node := h.engine.GetNode(shardID)
			if node == nil {
				t.Fatal("engine node missing after the restart")
			}
			if node.IsMember(999) {
				t.Error("IsMember(999) is true: per-shard message authorization is " +
					"disabled for the life of the process, and the handler's " +
					"SourceAddress binding is skipped with it")
			}
			if !node.IsMember(1) {
				t.Error("IsMember(1) is false: the recovered voter is not recognised")
			}

			// The role came from replicated state, not local config.
			after, memErr := h.ShardMembership(shardID)
			if memErr != nil {
				t.Fatalf("ShardMembership failed: %v", memErr)
			}
			roleMap := after.Witnesses
			if scfg.IsObserver {
				roleMap = after.Observers
			}
			if _, ok := roleMap[replicaID]; !ok {
				t.Fatalf("replica %d is not in the recovered role map: %+v", replicaID, after)
			}
			if _, ok := after.Nodes[replicaID]; ok {
				t.Fatalf("the members map made the replica a full voter after all: %+v", after)
			}
		})
	}
}

// TestStartShard_NonVotingRestartWithoutASnapshot covers a caught-up, never-snapshotted witness or observer: persisted state allows the restart and the committed config change restores its role.
func TestStartShard_NonVotingRestartWithoutASnapshot(t *testing.T) {
	tests := []struct {
		name    string
		ccType  uint64
		mutate  func(*config.Config)
		roleMap func(*Membership) map[uint64]string
	}{
		{
			name:    "witness",
			ccType:  proto.AddWitness,
			mutate:  func(c *config.Config) { c.IsWitness = true },
			roleMap: func(m *Membership) map[uint64]string { return m.Witnesses },
		},
		{
			name:    "observer",
			ccType:  proto.AddNonVoting,
			mutate:  func(c *config.Config) { c.IsObserver = true },
			roleMap: func(m *Membership) map[uint64]string { return m.Observers },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const shardID, replicaID uint64 = 62, 2
			const voterAddr, selfAddr = "127.0.0.1:5000", "127.0.0.1:5001"

			cfg := testHostConfig(t)
			db := memdb.New()
			h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost failed: %v", err)
			}
			defer h.Close()

			// A caught-up, never-snapshotted replica: bootstrap record, hard state, entry 1 (founding config) and entry 2 (the committed role change).
			if bsErr := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
				Addresses: map[uint64]string{1: voterAddr},
			}); bsErr != nil {
				t.Fatalf("SaveBootstrap failed: %v", bsErr)
			}
			cc := proto.ConfigChange{
				Type:      tt.ccType,
				ReplicaID: replicaID,
				Address:   selfAddr,
			}
			ccCmd := make([]byte, cc.Size())
			if _, mErr := cc.MarshalTo(ccCmd); mErr != nil {
				t.Fatalf("marshalling the config change: %v", mErr)
			}
			if stErr := db.SaveState([]logdb.Update{{
				ShardID:   shardID,
				ReplicaID: replicaID,
				State:     logdb.State{Term: 7, Vote: 1, Commit: 2},
				Entries: []logdb.Entry{
					bootstrapLogEntry(t, 7, map[uint64]string{1: voterAddr}),
					{Index: 2, Term: 7, Type: proto.EntryConfigChange, Cmd: ccCmd},
				},
			}}); stErr != nil {
				t.Fatalf("SaveState failed: %v", stErr)
			}
			if _, _, stateErr := db.ReadState(shardID, replicaID); stateErr != nil {
				t.Fatalf("seeding the hard state: %v", stateErr)
			}
			if _, snapErr := db.GetSnapshot(shardID, replicaID); !errors.Is(snapErr, logdb.ErrNoSnapshot) {
				t.Fatalf("the fixture recorded a snapshot (%v); this test must cover the "+
					"UNSNAPSHOTTED restart, which is the case the gate missed", snapErr)
			}

			scfg := testShardConfig(shardID, replicaID)
			tt.mutate(&scfg)
			scfg.Witness.RequireFullReplicaMajority = config.BoolPtr(false)

			// The restart the operator actually performs: the same members map
			// as every other replica, join=false.
			members := map[uint64]string{1: voterAddr, replicaID: selfAddr}
			if startErr := h.StartShard(members, false, testCreateFunc, scfg); startErr != nil {
				t.Fatalf("restarting a caught-up but unsnapshotted %s failed: %v; there "+
					"is then NO working restart path for this replica", tt.name, startErr)
			}
			defer func() { _ = h.StopShard(shardID) }()

			// The consequence, not the error code: join=true was the only form
			// that returned nil before, and it set a marker nothing clears.
			if h.IsShardJoining(shardID) {
				t.Error("the restarted replica is marked joining; the marker is cleared " +
					"only by an InstallSnapshot adopt, which a caught-up replica never " +
					"receives, so it would never clear")
			}
			node := h.engine.GetNode(shardID)
			if node == nil {
				t.Fatal("engine node missing after the restart")
			}
			if node.IsMember(999) {
				t.Error("IsMember(999) is true: per-shard message authorization is " +
					"disabled for the life of the process, and the handler's " +
					"SourceAddress binding is skipped with it")
			}

			// The recovered log is what proves this was a restart and not a
			// fresh bootstrap that happened to be accepted.
			if _, _, idxErr := db.EntryRange(shardID, replicaID); idxErr != nil {
				t.Fatalf("EntryRange after the restart: %v", idxErr)
			}

			// The membership, which is the whole point. The committed entry is
			// re-applied through the ordinary commit path (recovered
			// HardState.Commit is 2, the processed marker is 0), so poll for
			// it rather than assuming it landed before StartShard returned.
			var after *Membership
			if !waitUntil(10*time.Second, func() bool {
				var memErr error
				after, memErr = h.ShardMembership(shardID)
				if memErr != nil {
					t.Fatalf("ShardMembership failed: %v", memErr)
				}
				return after.ConfigChangeID == 2
			}) {
				t.Fatalf("the committed %s config change at index 2 was never applied: %+v; "+
					"the replica was seeded into the FULL VOTER set by the members map "+
					"and nothing repaired it", tt.name, after)
			}

			if _, ok := after.Nodes[replicaID]; ok {
				t.Fatalf("the members map made the %s a full voter after all: %+v; the "+
					"leader replicates full entry payloads to it, it counts toward every "+
					"quorum, and a snapshot would make this durable", tt.name, after)
			}
			if _, ok := tt.roleMap(after)[replicaID]; !ok {
				t.Fatalf("replica %d is not in the %s role map: %+v", replicaID, tt.name, after)
			}
			if _, ok := after.Nodes[1]; !ok {
				t.Fatalf("the bootstrap voter 1 is missing from the voting set: %+v", after)
			}
			if len(after.Nodes) != 1 {
				t.Fatalf("voting set = %+v, want exactly the one full replica", after.Nodes)
			}
		})
	}
}

// TestStartShard_WitnessEntersMembershipThroughConfigChange pins that the config change puts the witness in Membership.Witnesses, not Nodes.
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
	after, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership after the add failed: %v", err)
	}
	if _, ok := after.Witnesses[2]; !ok {
		t.Fatalf("replica 2 is not in Membership.Witnesses after the config change: %+v", after)
	}
	if _, ok := after.Nodes[2]; ok {
		t.Fatalf("replica 2 is in Membership.Nodes (the full voter set the members "+
			"map seeds), so the leader would replicate full entry payloads to it: %+v", after)
	}
}
