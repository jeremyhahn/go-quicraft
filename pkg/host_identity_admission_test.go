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
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Mandatory identities at admission (#197 A)
//
// A member is authorized on the receive path by its committed node identity
// and by nothing else, so every path that ADMITS a member on a host with a
// transport must require the identity: the founding map, the join anchor,
// every later add, and a snapshot import. A host with no transport has no
// wire boundary and keeps the old ergonomics.
// ---------------------------------------------------------------------------

// TestStartShard_TransportHostRequiresFounderIdentities pins the founding
// map: every member must be named in Config.MemberIdentities, and the refusal
// names the replica that is not.
func TestStartShard_TransportHostRequiresFounderIdentities(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "node-c.raft.test:5000",
	}

	// No identities at all.
	err = h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	var inv *InvalidMemberError
	if !errors.As(err, &inv) {
		t.Fatalf("StartShard with no MemberIdentities on a transport host = %v, want *InvalidMemberError", err)
	}
	if _, loaded := h.nodes.Load(uint64(1)); loaded {
		t.Fatal("the shard was created despite the refusal")
	}

	// One member left out: the refusal names it.
	scfg := testShardConfigWithIdentities(h, 1, 1, members)
	delete(scfg.MemberIdentities, 3)
	err = h.StartShard(members, false, testCreateFunc, scfg)
	if !errors.As(err, &inv) || inv.ReplicaID != 3 {
		t.Fatalf("StartShard with replica 3 unnamed = %v, want *InvalidMemberError naming replica 3", err)
	}

	// This host's own entry must be this host's identity: a typo there is
	// committed at index 1 and every peer then refuses this replica.
	wrongSelf := testShardConfigWithIdentities(h, 1, 1, members)
	wrongSelf.MemberIdentities[1] = testIdentity(1)
	if err := h.StartShard(members, false, testCreateFunc, wrongSelf); !errors.As(err, &inv) || inv.ReplicaID != 1 {
		t.Fatalf("StartShard with a wrong identity for this host = %v, want *InvalidMemberError naming replica 1", err)
	}

	// Replica ID 0 is reserved and never an anchor.
	zero := testShardConfigWithIdentities(h, 1, 1, members)
	zero.MemberIdentities[0] = testIdentity(0)
	if err := h.StartShard(members, false, testCreateFunc, zero); !errors.As(err, &inv) {
		t.Fatalf("StartShard with an identity for replica 0 = %v, want *InvalidMemberError", err)
	}

	// Every member named: admitted.
	if err := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members)); err != nil {
		t.Fatalf("StartShard with every member named: %v", err)
	}
}

// TestStartShard_RefusesDuplicateIdentitiesAndKeepsAnchorEntriesOutOfBootstrap
// pins the two shapes the second-round review found the founding map could
// still commit: one identity bound to two members (one keypair, two votes —
// the forgery identities exist to defeat, and the shape a later add refuses),
// and an identity for a replica the members map does not hold. The second is
// not refused — it is the operator anchor, and a restarted replica that missed
// an AddNode is vouched for exactly this way — but it must not be written into
// the bootstrap entry for a replica no role holds.
func TestStartShard_RefusesDuplicateIdentitiesAndKeepsAnchorEntriesOutOfBootstrap(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	members := map[uint64]string{1: "node-a.raft.test:5000", 2: "node-b.raft.test:5000"}

	dup := testShardConfigWithIdentities(h, 1, 1, members)
	dup.MemberIdentities[2] = dup.MemberIdentities[1]
	var inv *InvalidMemberError
	if err := h.StartShard(members, false, testCreateFunc, dup); !errors.As(err, &inv) || inv.ReplicaID != 2 {
		t.Fatalf("StartShard with one identity on two members = %v, want *InvalidMemberError naming replica 2", err)
	}

	// An entry beyond the members map is the operator anchor: admitted as
	// trust for the receive gate, kept out of the bootstrap entry.
	anchored := testShardConfigWithIdentities(h, 1, 1, members)
	anchored.MemberIdentities[9] = testIdentity(9)
	if err := h.StartShard(members, false, testCreateFunc, anchored); err != nil {
		t.Fatalf("StartShard with an anchor entry for a non-member = %v, want nil", err)
	}
	if got, ok := h.anchorIdentity(1, 9); !ok || got != testIdentity(9) {
		t.Fatalf("anchorIdentity(1, 9) = (%q, %v), want the anchored identity", got, ok)
	}
	// A two-member shard on one host never elects (replica 2 is not running),
	// so the committed configuration is read from the peer, not through a
	// linearizable membership read.
	ms := h.engine.GetNode(1).Peer().GetMembership()
	if _, committed := ms.Identities[9]; committed {
		t.Fatal("an anchor entry for a replica no role holds was committed into the bootstrap entry")
	}
	for id := range members {
		if ms.Identities[id] != anchored.MemberIdentities[id] {
			t.Fatalf("member %d's identity was not committed: %v", id, ms.Identities)
		}
	}
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if _, ok := h.anchorIdentity(1, 9); ok {
		t.Fatal("the anchor outlived the shard")
	}

	// A joiner's anchor names replicas it does not know: extra entries are the point.
	anchor := testShardConfig(1, 3)
	anchor.MemberIdentities = map[uint64]string{1: testIdentity(1), 2: testIdentity(2)}
	if err := h.StartShard(nil, true, testCreateFunc, anchor); err != nil {
		t.Fatalf("StartShard(join=true) with an anchor over unknown replicas: %v", err)
	}
}

// TestStartShard_TransportHostRequiresJoinAnchor pins the joiner: with no
// committed configuration, Config.MemberIdentities is the only thing that can
// say who may hand it one, so an empty anchor is refused — and so is one that
// names only the joiner itself, since its own entry vouches for no sender.
func TestStartShard_TransportHostRequiresJoinAnchor(t *testing.T) {
	cfg := sanHostConfig(t, "node-b.raft.test:5000", "node-b.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	err = h.StartShard(nil, true, testCreateFunc, testShardConfig(1, 2))
	var inv *InvalidMemberError
	if !errors.As(err, &inv) {
		t.Fatalf("StartShard(join=true) with no anchor on a transport host = %v, want *InvalidMemberError", err)
	}
	selfOnly := testShardConfig(1, 2)
	selfOnly.MemberIdentities = map[uint64]string{2: h.NodeIdentity()}
	if err := h.StartShard(nil, true, testCreateFunc, selfOnly); !errors.As(err, &inv) {
		t.Fatalf("StartShard(join=true) with an anchor naming only the joiner = %v, want *InvalidMemberError", err)
	}
	if h.HasNodeInfo(1, 2) {
		t.Fatal("a refused join left the shard loaded")
	}
	// A members map naming only the joiner is no anchor either: it seeds
	// nobody who could deliver the bootstrap snapshot.
	if err := h.StartShard(map[uint64]string{2: "node-b.raft.test:5000"}, true, testCreateFunc, selfOnly); !errors.As(err, &inv) {
		t.Fatalf("StartShard(join=true) with a members map and anchor naming only the joiner = %v, want *InvalidMemberError", err)
	}
	// One that names another replica, identified, does.
	seeded := testShardConfig(1, 2)
	seeded.MemberIdentities = map[uint64]string{1: testIdentity(1), 2: h.NodeIdentity()}
	if err := h.StartShard(map[uint64]string{1: "node-a.raft.test:5000", 2: "node-b.raft.test:5000"}, true, testCreateFunc, seeded); err != nil {
		t.Fatalf("StartShard(join=true) with a members map naming another replica = %v, want nil", err)
	}
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}

	scfg := testShardConfig(1, 2)
	scfg.MemberIdentities = map[uint64]string{1: testIdentity(1)}
	if err := h.StartShard(nil, true, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard(join=true) with an anchor: %v", err)
	}
	if got, ok := h.anchorIdentity(1, 1); !ok || got != testIdentity(1) {
		t.Fatalf("anchorIdentity(1, 1) = (%q, %v), want the anchored identity", got, ok)
	}
	if _, ok := h.anchorIdentity(1, 7); ok {
		t.Fatal("anchorIdentity named a replica the anchor does not hold")
	}
}

// TestStartShard_EstablishedReplicaRestartsWithJoinAndNoAnchor pins that the
// anchor is a FIRST-start requirement: a replica that already holds a durable
// configuration may be restarted with join=true and no MemberIdentities — a
// supported convention (Peer.MarkJoining) — because it recovers what a joiner
// would have needed the anchor for.
func TestStartShard_EstablishedReplicaRestartsWithJoinAndNoAnchor(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	cfg.WALDir, cfg.NodeHostDir = t.TempDir(), t.TempDir()
	h, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	seed := map[uint64]string{1: "node-a.raft.test:5000"}
	if err := h.StartShard(seed, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, seed)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost (restart): %v", err)
	}
	defer restarted.Close()
	if err := restarted.StartShard(nil, true, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard(join=true, no anchor) on an established replica = %v, want nil: "+
			"its configuration is durable and it needs no bootstrap", err)
	}
	if _, ok := restarted.engine.GetNode(1).MemberBinding(1); !ok {
		t.Fatal("the restarted replica did not recover its committed configuration")
	}
}

// TestRequestAddNode_TransportHostRequiresIdentity pins the later add: the
// identity-less forms are refused with ErrMemberIdentityRequired, promptly,
// at the API, and the WithIdentity form is admitted.
func TestRequestAddNode_TransportHostRequiresIdentity(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	seed := map[uint64]string{1: "node-a.raft.test:5000"}
	if err := h.StartShard(seed, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, seed)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for name, add := range map[string]func() (*RequestState, error){
		"RequestAddNode": func() (*RequestState, error) {
			return h.RequestAddNode(ctx, 1, 2, "node-b.raft.test:5000", 0)
		},
		"RequestAddObserver": func() (*RequestState, error) {
			return h.RequestAddObserver(ctx, 1, 3, "node-c.raft.test:5000", 0)
		},
		"RequestAddWitness": func() (*RequestState, error) {
			return h.RequestAddWitness(ctx, 1, 4, "node-d.raft.test:5000", 0)
		},
		"RequestAddNodeWithIdentity(empty)": func() (*RequestState, error) {
			return h.RequestAddNodeWithIdentity(ctx, 1, 5, "node-e.raft.test:5000", "", 0)
		},
	} {
		t.Run(name, func(t *testing.T) {
			rs, err := add()
			if rs != nil {
				rs.Release()
			}
			if !errors.Is(err, ErrMemberIdentityRequired) {
				t.Fatalf("%s without an identity = %v, want ErrMemberIdentityRequired", name, err)
			}
		})
	}

	rs, err := h.RequestAddNodeWithIdentity(ctx, 1, 2, "node-b.raft.test:5000", testIdentity(2), 0)
	if err != nil {
		t.Fatalf("RequestAddNodeWithIdentity: %v", err)
	}
	rs.Release()
}

// TestStartShard_NoTransportHostDoesNotRequireIdentities pins the other side:
// a host without a transport has no wire boundary, so its shards keep the
// old ergonomics — no identities at bootstrap, identity-less adds.
func TestStartShard_NoTransportHostDoesNotRequireIdentities(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard without identities on a transportless host: %v", err)
	}
	waitForLeader(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rs, err := h.RequestAddNode(ctx, 1, 2, "127.0.0.1:5001", 0)
	if err != nil {
		t.Fatalf("RequestAddNode without an identity on a transportless host: %v", err)
	}
	rs.Release()
}

// TestImportMembershipFor_RequiresIdentitiesBehindATransport pins the import
// door: the configuration a shard restarts into must hold an identity for
// every active member when the host has a transport, whether that
// configuration is the artifact's own or a WithImportMembership rewrite, and
// WithImportMemberIdentities is how a rewrite supplies them.
func TestImportMembershipFor_RequiresIdentitiesBehindATransport(t *testing.T) {
	artifact := artifactMembership()

	// The artifact's own membership has no identities: refused behind a
	// transport, accepted without one.
	if _, err := importMembershipFor(5, 3, artifact, nil, true); err == nil {
		t.Fatal("an artifact whose members carry no identities was accepted behind a transport")
	} else {
		var inv *ImportMembershipInvalidError
		if !errors.As(err, &inv) {
			t.Fatalf("error = %T (%v), want *ImportMembershipInvalidError", err, err)
		}
	}
	if _, err := importMembershipFor(5, 3, artifact, nil, false); err != nil {
		t.Fatalf("an identity-less artifact was refused on a transportless host: %v", err)
	}

	// Identities supplied WITHOUT a rewrite complete the artifact's own
	// membership, which is how an artifact exported by a build that did not
	// require identities is imported behind a transport.
	all := map[uint64]string{}
	for id := range artifact.Addresses {
		all[id] = testIdentity(id)
	}
	for id := range artifact.Observers {
		all[id] = testIdentity(id)
	}
	completed, err := importMembershipFor(5, 3, artifact, []ImportOption{WithImportMemberIdentities(all)}, true)
	if err != nil {
		t.Fatalf("an identity-less artifact completed by WithImportMemberIdentities alone = %v, want nil", err)
	}
	if len(completed.Addresses) != len(artifact.Addresses) || completed.Identities[9] != testIdentity(9) {
		t.Fatalf("the artifact's membership was not kept, or the identities not applied: %+v", completed)
	}
	// An identity for a replica the artifact holds no role for is the map
	// keyed by intended new IDs with WithImportMembership forgotten: refused,
	// not persisted as a binding to a non-member.
	stray := map[uint64]string{}
	for id, identity := range all {
		stray[id] = identity
	}
	stray[42] = testIdentity(42)
	stray[41] = testIdentity(41)
	_, err = importMembershipFor(5, 3, artifact, []ImportOption{WithImportMemberIdentities(stray)}, true)
	var strayErr *ImportMembershipInvalidError
	if !errors.As(err, &strayErr) {
		t.Fatalf("an identity for a non-member = %v, want *ImportMembershipInvalidError", err)
	}
	if !strings.Contains(strayErr.Reason, "replica 41,") {
		t.Fatalf("the refusal names %q, want the lowest stray replica (41)", strayErr.Reason)
	}
	// The same for a rewrite: identities keyed by the OLD replica IDs are
	// refused, not dropped into a misleading "no identity" refusal.
	_, err = importMembershipFor(5, 3, artifact, []ImportOption{
		WithImportMembership(map[uint64]string{3: "10.0.0.3:9000", 4: "10.0.0.4:9000"}),
		WithImportMemberIdentities(map[uint64]string{3: testIdentity(3), 4: testIdentity(4), 5: testIdentity(5)}),
	}, true)
	if !errors.As(err, &strayErr) || !strings.Contains(strayErr.Reason, "replica 5,") {
		t.Fatalf("a rewrite with an identity for a replica it does not name = %v, want *ImportMembershipInvalidError naming replica 5", err)
	}

	// A rewrite keeps the artifact's identity for a survivor and takes the
	// supplied one for a survivor the artifact does not name.
	artifact.Identities = map[uint64]string{3: testIdentity(3)}
	survivors := map[uint64]string{3: "10.0.0.3:9000", 4: "10.0.0.4:9000"}
	_, err = importMembershipFor(5, 3, artifact, []ImportOption{WithImportMembership(survivors)}, true)
	var inv *ImportMembershipInvalidError
	if !errors.As(err, &inv) {
		t.Fatalf("a rewrite leaving replica 4 without an identity = %v, want *ImportMembershipInvalidError", err)
	}
	got, err := importMembershipFor(5, 3, artifact, []ImportOption{
		WithImportMembership(survivors),
		WithImportMemberIdentities(map[uint64]string{4: testIdentity(4)}),
	}, true)
	if err != nil {
		t.Fatalf("a rewrite with every survivor identified: %v", err)
	}
	if got.Identities[3] != testIdentity(3) || got.Identities[4] != testIdentity(4) {
		t.Fatalf("recovered identities = %v, want the artifact's for 3 and the supplied one for 4", got.Identities)
	}
	if _, missing := firstMemberWithoutIdentity(got); missing {
		t.Fatal("firstMemberWithoutIdentity reports a gap in a complete membership")
	}
	if id, missing := firstMemberWithoutIdentity(proto.Membership{
		Addresses:  map[uint64]string{1: "a", 2: "b"},
		Observers:  map[uint64]string{9: "c"},
		Identities: map[uint64]string{1: "x", 9: "y"},
	}); !missing || id != 2 {
		t.Fatalf("firstMemberWithoutIdentity = (%d, %v), want (2, true)", id, missing)
	}

	// One keypair bound to two survivors is refused whatever the posture: the
	// holder would cast two votes.
	_, err = importMembershipFor(5, 3, artifact, []ImportOption{
		WithImportMembership(survivors),
		WithImportMemberIdentities(map[uint64]string{4: testIdentity(3)}),
	}, false)
	if !errors.As(err, &inv) {
		t.Fatalf("a rewrite binding one identity to two survivors = %v, want *ImportMembershipInvalidError", err)
	}

	// An identity the shard removed is not re-admitted by import, on either
	// path, whatever the posture: the add path refuses it
	// (ErrRemovedMemberIdentity) and the import door is not a way around it.
	artifact.Removed = map[uint64]bool{7: true}
	artifact.RemovedIdentities = map[uint64]string{7: testIdentity(7)}
	_, err = importMembershipFor(5, 3, artifact, []ImportOption{
		WithImportMembership(survivors),
		WithImportMemberIdentities(map[uint64]string{4: testIdentity(7)}),
	}, false)
	if !errors.As(err, &inv) {
		t.Fatalf("a rewrite giving a survivor a removed replica's identity = %v, want *ImportMembershipInvalidError", err)
	}
	artifact.Identities = map[uint64]string{3: testIdentity(3), 4: testIdentity(7)}
	if _, err := importMembershipFor(5, 3, artifact, nil, false); !errors.As(err, &inv) {
		t.Fatalf("an artifact whose own member holds a removed identity = %v, want *ImportMembershipInvalidError", err)
	}
	if member, removed := identityRemovedByArtifact(proto.Membership{
		Addresses:  map[uint64]string{1: "a", 2: "b"},
		Identities: map[uint64]string{1: "x", 2: "y"},
	}, map[uint64]string{8: "y", 9: "z"}); member != 2 || removed != 8 {
		t.Fatalf("identityRemovedByArtifact = (%d, %d), want (2, 8)", member, removed)
	}
}

// TestStartShard_RestartHeldToRecoveredIdentities pins checkRecoveredIdentities:
// an established replica restarts against the bindings the shard committed —
// its own keypair must be the committed one, and an operator anchor may not
// contradict a committed binding or bind a committed member's identity to a
// second replica ID (which the anchor would otherwise admit as a sender).
func TestStartShard_RestartHeldToRecoveredIdentities(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	cfg.WALDir, cfg.NodeHostDir = t.TempDir(), t.TempDir()
	h, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	seed := map[uint64]string{1: "node-a.raft.test:5000"}
	if err := h.StartShard(seed, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, seed)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	committed := h.NodeIdentity()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var inv *InvalidMemberError
	restart := func(t *testing.T, hostCfg config.HostConfig, anchor map[uint64]string) error {
		t.Helper()
		restarted, err := NewHost(hostCfg)
		if err != nil {
			t.Fatalf("NewHost (restart): %v", err)
		}
		defer restarted.Close()
		scfg := testShardConfig(1, 1)
		scfg.MemberIdentities = anchor
		return restarted.StartShard(nil, true, testCreateFunc, scfg)
	}

	// A new keypair under the same replica ID: refused, with the reason.
	rekeyed := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	rekeyed.WALDir, rekeyed.NodeHostDir = cfg.WALDir, cfg.NodeHostDir
	if err := restart(t, rekeyed, nil); !errors.As(err, &inv) || inv.ReplicaID != 1 {
		t.Fatalf("restart with a changed keypair = %v, want *InvalidMemberError for replica 1: "+
			"every peer would refuse the replica and nothing local would say why", err)
	}

	// An anchor contradicting the committed binding for a member.
	if err := restart(t, cfg, map[uint64]string{1: testIdentity(1)}); !errors.As(err, &inv) {
		t.Fatalf("restart with an anchor naming a different identity for a committed member = %v, want *InvalidMemberError", err)
	}
	// An anchor binding the committed member's identity to a second replica ID.
	if err := restart(t, cfg, map[uint64]string{99: committed}); !errors.As(err, &inv) || inv.ReplicaID != 99 {
		t.Fatalf("restart with an anchor binding replica 1's identity to replica 99 = %v, want *InvalidMemberError for 99", err)
	}
	// A consistent anchor — the committed binding restated, plus a replica
	// the configuration does not know — is admitted.
	if err := restart(t, cfg, map[uint64]string{1: committed, 7: testIdentity(7)}); err != nil {
		t.Fatalf("restart with a consistent anchor = %v, want nil", err)
	}

	// The founders' coverage rule is a bootstrap rule. On a restart the
	// members map is ignored, so a stale map naming a replica the identities
	// no longer cover must not be refused — that is every remaining
	// founder's map after a RemoveNode, on its next rolling restart.
	restarted, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost (restart): %v", err)
	}
	stale := map[uint64]string{1: "node-a.raft.test:5000", 2: "node-b.raft.test:5000"}
	scfg := testShardConfig(1, 1)
	scfg.MemberIdentities = map[uint64]string{1: committed}
	if err := restarted.StartShard(stale, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("restart with a members map the identities do not cover = %v, want nil: "+
			"the map is ignored on a restart and must not be held to the bootstrap rule", err)
	}
	waitForLeader(t, restarted, 1)
	// Commit a removal so the recovered configuration carries a tombstoned
	// identity, then restart with the founding map still naming it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := restarted.SyncRequestAddObserverWithIdentity(ctx, 1, 2, "node-b.raft.test:5000", testIdentity(2), 0); err != nil {
		t.Fatalf("SyncRequestAddObserverWithIdentity: %v", err)
	}
	if err := restarted.SyncRequestDeleteNode(ctx, 1, 2, 0); err != nil {
		t.Fatalf("SyncRequestDeleteNode: %v", err)
	}
	// A snapshot, so the restart recovers the tombstone rather than the
	// founding entry alone (checkRecoveredIdentities sees what recovery
	// hands StartShard).
	if _, err := restarted.SyncRequestSnapshot(ctx, 1, SnapshotOption{}); err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := restart(t, cfg, map[uint64]string{1: committed, 2: testIdentity(2)}); err != nil {
		t.Fatalf("restart with an anchor naming a since-removed identity = %v, want nil: the "+
			"entry is inert behind the removal deny-list, and refusing it would fail every "+
			"founder's next restart after a routine removal", err)
	}
}

// TestHandleSnapshot_RefusesABindingThisHostDoesNotPresent pins the snapshot
// path's own identity check: the snapshot REPLACES a joiner's committed
// membership, so a binding in it for this replica that is not the identity
// this host presents (the AddNode that admitted it carried a mistyped one) is
// refused before anything is persisted, instead of being adopted and having
// every peer refuse the joiner's acks for the life of the shard.
func TestHandleSnapshot_RefusesABindingThisHostDoesNotPresent(t *testing.T) {
	cfg := sanHostConfig(t, "node-b.raft.test:5000", "node-b.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	const leaderAddr = "node-a.raft.test:5000"
	leaderID := testIdentity(1)
	scfg := testShardConfig(1, 2)
	scfg.MemberIdentities = map[uint64]string{1: leaderID}
	if err := h.StartShard(nil, true, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard(join=true): %v", err)
	}
	handler := &hostMessageHandler{engine: h.engine, host: h}

	chunks := func(self string) []proto.SnapshotChunk {
		return []proto.SnapshotChunk{{
			ShardID: 1, ReplicaID: 2, From: 1, Index: 10, Term: 3, ChunkID: 0, ChunkCount: 1,
			Data: []byte("x"),
			Membership: proto.Membership{
				ConfigChangeID: 9,
				Addresses:      map[uint64]string{1: leaderAddr, 2: "node-b.raft.test:5000"},
				Identities:     map[uint64]string{1: leaderID, 2: self},
			},
		}}
	}
	err = handler.HandleSnapshot(chunks(testIdentity(2)), certCoveringAs(leaderID, leaderAddr))
	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("a snapshot binding this replica to a foreign identity = %v, want *UnauthorizedMessageError", err)
	}
	if h.engine.GetNode(1).Peer().IsMember(1) && !h.engine.GetNode(1).IsJoining() {
		t.Fatal("the refused snapshot was adopted")
	}
	// The binding this host presents is not what the check refuses.
	err = handler.HandleSnapshot(chunks(h.NodeIdentity()), certCoveringAs(leaderID, leaderAddr))
	if errors.As(err, &authErr) {
		t.Fatalf("a snapshot binding this replica to its own identity was refused as unauthorized: %v", err)
	}
}
