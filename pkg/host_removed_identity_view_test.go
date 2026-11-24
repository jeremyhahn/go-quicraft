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

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// The removed-identity deny-list on the public Membership view
//
// A host classifying a joiner needs two things from the public API: a typed
// refusal it can match (ErrRemovedMemberIdentity, pinned with the other
// membership aliases in errors_membership_alias_test.go) and the deny-list
// itself, so it can answer before proposing. These tests pin the second.
// ---------------------------------------------------------------------------

// TestMembership_RemovedIdentitiesSurfaceThroughTheHostAPI drives a committed
// removal through consensus and checks the public view names the identity the
// shard now refuses, and that the add it predicts really is refused.
func TestMembership_RemovedIdentitiesSurfaceThroughTheHostAPI(t *testing.T) {
	h := startSingleVoterShard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	identity := testIdentity(10)
	if err := h.SyncRequestAddObserverWithIdentity(ctx, 1, 10, "127.0.0.1:6000", identity, 0); err != nil {
		t.Fatalf("SyncRequestAddObserverWithIdentity: %v", err)
	}
	added, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership after add: %v", err)
	}
	if len(added.RemovedIdentities) != 0 {
		t.Fatalf("RemovedIdentities before any removal = %v, want empty", added.RemovedIdentities)
	}
	if err := h.SyncRequestRemoveNode(ctx, 1, 10, added.ConfigChangeID); err != nil {
		t.Fatalf("SyncRequestRemoveNode: %v", err)
	}

	for name, read := range map[string]func() (*Membership, error){
		"SyncGetShardMembership": func() (*Membership, error) { return h.SyncGetShardMembership(ctx, 1) },
		"ShardMembership":        func() (*Membership, error) { return h.ShardMembership(1) },
	} {
		m, err := read()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := m.RemovedIdentities[10]; got != identity {
			t.Fatalf("%s RemovedIdentities[10] = %q, want %q: a host cannot "+
				"classify a returning keypair before proposing it", name, got, identity)
		}
	}

	_, err = h.RequestAddNodeWithIdentity(ctx, 1, 11, "127.0.0.1:6001", identity, 0)
	if !errors.Is(err, ErrRemovedMemberIdentity) {
		t.Fatalf("re-adding a removed identity under a new replica ID = %v, want ErrRemovedMemberIdentity", err)
	}
}

// TestProtoMembershipToPublic_RemovedIdentitiesCopied pins the conversion: the
// map is carried over, and copied rather than aliased, like every other map
// on the public view.
func TestProtoMembershipToPublic_RemovedIdentitiesCopied(t *testing.T) {
	pm := &proto.Membership{
		RemovedIdentities: map[uint64]string{7: "spki:seven", 8: "spki:eight"},
	}

	m := protoMembershipToPublic(pm)
	pm.RemovedIdentities[7] = "mutated"
	delete(pm.RemovedIdentities, 8)

	if len(m.RemovedIdentities) != 2 || m.RemovedIdentities[7] != "spki:seven" ||
		m.RemovedIdentities[8] != "spki:eight" {
		t.Fatalf("RemovedIdentities = %v, want an unaliased copy of both entries", m.RemovedIdentities)
	}

	empty := protoMembershipToPublic(&proto.Membership{})
	if empty.RemovedIdentities == nil || len(empty.RemovedIdentities) != 0 {
		t.Fatalf("RemovedIdentities on an empty membership = %v, want an empty non-nil map",
			empty.RemovedIdentities)
	}
	if NewMembership().RemovedIdentities == nil {
		t.Fatal("NewMembership left RemovedIdentities nil")
	}
}

// TestShardInfo_IsObserverFollowsTheCommittedMembership: ShardInfo.IsObserver
// reports the EFFECTIVE role. A single-voter shard's only replica is a voter
// whatever its Config said, because Config.IsObserver is only the initial
// hint; the hint is what a shard without a published peer reports.
func TestShardInfo_IsObserverFollowsTheCommittedMembership(t *testing.T) {
	h := startSingleVoterShard(t)

	info, err := h.ShardInfo(1)
	if err != nil {
		t.Fatalf("ShardInfo: %v", err)
	}
	if info.IsObserver {
		t.Fatal("ShardInfo reports the shard's only voter as an observer")
	}

	// The published membership lists replica 1 as a voter, so it overrides a
	// Config that (as on a replica started as an observer and since promoted)
	// still says observer. A probe nodeState carries that Config without
	// writing to the live one the engine reads.
	promoted := &nodeState{shardID: 1, replicaID: 1}
	promoted.cfg.IsObserver = true
	if h.shardIsObserver(promoted) {
		t.Fatal("shardIsObserver reported the configured hint over the committed voter role")
	}

	// A shard the engine does not hold falls back to the hint.
	orphan := &nodeState{shardID: 999}
	orphan.cfg.IsObserver = true
	if !h.shardIsObserver(orphan) {
		t.Fatal("shardIsObserver ignored the hint for a shard with no published peer")
	}
}
