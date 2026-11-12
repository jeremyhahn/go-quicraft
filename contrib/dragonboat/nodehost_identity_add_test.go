// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// identityAddMembership reads the shard's membership through the adapter's
// SyncGetShardMembership and the underlying host's, so a test checks both the
// dragonboat role and the identity the shard committed for each member.
func identityAddMembership(t *testing.T, ctx context.Context, nh *NodeHost, shardID uint64) (*Membership, *quicraft.Membership) {
	t.Helper()
	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	qm, err := nh.host.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("host SyncGetShardMembership: %v", err)
	}
	return m, qm
}

// identityAddLocalMembership waits until the replica's local membership
// satisfies ok and returns it. The last add on a single-host shard makes the
// new member a voter that is not running, so the leader loses quorum and a
// linearizable SyncGetShardMembership cannot answer; the committed change is
// still in the local membership, which is what this reads.
func identityAddLocalMembership(t *testing.T, nh *NodeHost, shardID uint64, ok func(*Membership, *quicraft.Membership) bool) (*Membership, *quicraft.Membership) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		qm, err := nh.host.ShardMembership(shardID)
		if err == nil {
			m := fromQuicraftMembership(qm)
			if ok(m, qm) || time.Now().After(deadline) {
				return m, qm
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("host ShardMembership: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func identityAddMembers(m *Membership) int {
	return len(m.Nodes) + len(m.NonVotings) + len(m.Witnesses)
}

// TestNodeHost_IdentityAdd drives the three Sync*WithIdentity add calls as
// contrib/dragonboat/README.md documents them: each adds the replica in its
// role with the identity the shard commits, an address that fails
// IsValidAddress returns ErrInvalidAddress and changes nothing, and a
// duplicate identity is refused with an error that still matches
// quicraft.ErrDuplicateMemberIdentity.
//
// A witness votes, so adding one to a single-voter shard whose other replicas
// are not running leaves the leader without quorum, as adding a voter does.
// Each of those adds is therefore the last on its shard: the witness gets a
// shard of its own, and the voter closes the main shard.
func TestNodeHost_IdentityAdd(t *testing.T) {
	nh := newTestNodeHost(t)
	const (
		shardID        = uint64(907)
		witnessShardID = uint64(908)
		id2            = "identity-replica-2"
		id3            = "identity-replica-3"
		id5            = "identity-replica-5"
	)
	startTestShard(t, nh, shardID, 1)
	startTestShard(t, nh, witnessShardID, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("invalid address refused", func(t *testing.T) {
		before, _ := identityAddMembership(t, ctx, nh, shardID)
		const bad = "not a valid address"
		calls := map[string]func() error{
			"replica": func() error {
				return nh.SyncRequestAddReplicaWithIdentity(ctx, shardID, 9, bad, "identity-bad-voter", 0)
			},
			"non-voting": func() error {
				return nh.SyncRequestAddNonVotingWithIdentity(ctx, shardID, 9, bad, "identity-bad-nonvoting", 0)
			},
			"witness": func() error {
				return nh.SyncRequestAddWitnessWithIdentity(ctx, shardID, 9, bad, "identity-bad-witness", 0)
			},
		}
		for name, call := range calls {
			if err := call(); !errors.Is(err, ErrInvalidAddress) {
				t.Errorf("%s: err = %v, want ErrInvalidAddress", name, err)
			}
		}
		after, qm := identityAddMembership(t, ctx, nh, shardID)
		if identityAddMembers(after) != identityAddMembers(before) || after.ConfigChangeID != before.ConfigChangeID {
			t.Fatalf("membership changed by refused adds: before %+v, after %+v", before, after)
		}
		if _, ok := qm.Identities[9]; ok {
			t.Fatalf("refused add bound an identity to replica 9: %v", qm.Identities)
		}
	})

	t.Run("non-voting added with identity", func(t *testing.T) {
		if err := nh.SyncRequestAddNonVotingWithIdentity(ctx, shardID, 2, "localhost:63002", id2, 0); err != nil {
			t.Fatalf("SyncRequestAddNonVotingWithIdentity: %v", err)
		}
		m, qm := identityAddMembership(t, ctx, nh, shardID)
		if _, ok := m.NonVotings[2]; !ok {
			t.Fatalf("replica 2 not in NonVotings: %+v", m)
		}
		if got := qm.Identities[2]; got != id2 {
			t.Fatalf("Identities[2] = %q, want %q", got, id2)
		}
	})

	t.Run("duplicate identity refused", func(t *testing.T) {
		before, _ := identityAddMembership(t, ctx, nh, shardID)
		err := nh.SyncRequestAddNonVotingWithIdentity(ctx, shardID, 4, "localhost:63004", id2, 0)
		if !errors.Is(err, quicraft.ErrDuplicateMemberIdentity) {
			t.Fatalf("err = %v, want quicraft.ErrDuplicateMemberIdentity", err)
		}
		after, _ := identityAddMembership(t, ctx, nh, shardID)
		if _, ok := after.NonVotings[4]; ok {
			t.Fatalf("replica 4 added despite a duplicate identity: %+v", after)
		}
		if identityAddMembers(after) != identityAddMembers(before) {
			t.Fatalf("membership changed by refused add: before %+v, after %+v", before, after)
		}
	})

	t.Run("witness added with identity", func(t *testing.T) {
		if err := nh.SyncRequestAddWitnessWithIdentity(ctx, witnessShardID, 3, "localhost:63003", id3, 0); err != nil {
			t.Fatalf("SyncRequestAddWitnessWithIdentity: %v", err)
		}
		m, qm := identityAddLocalMembership(t, nh, witnessShardID, func(m *Membership, _ *quicraft.Membership) bool {
			_, ok := m.Witnesses[3]
			return ok
		})
		if _, ok := m.Witnesses[3]; !ok {
			t.Fatalf("replica 3 not in Witnesses: %+v", m)
		}
		if got := qm.Identities[3]; got != id3 {
			t.Fatalf("Identities[3] = %q, want %q", got, id3)
		}
	})

	t.Run("voter added with identity", func(t *testing.T) {
		if err := nh.SyncRequestAddReplicaWithIdentity(ctx, shardID, 5, "localhost:63005", id5, 0); err != nil {
			t.Fatalf("SyncRequestAddReplicaWithIdentity: %v", err)
		}
		m, qm := identityAddLocalMembership(t, nh, shardID, func(m *Membership, _ *quicraft.Membership) bool {
			_, ok := m.Nodes[5]
			return ok
		})
		if _, ok := m.Nodes[5]; !ok {
			t.Fatalf("replica 5 not in Nodes: %+v", m)
		}
		if got := qm.Identities[5]; got != id5 {
			t.Fatalf("Identities[5] = %q, want %q", got, id5)
		}
	})
}
