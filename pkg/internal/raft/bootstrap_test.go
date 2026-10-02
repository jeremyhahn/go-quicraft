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

package raft

import (
	"bytes"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestBuildInitialMembershipEntry_Deterministic is the load-bearing property:
// two replicas building the bootstrap entry from the SAME configuration must
// produce byte-identical entries, or the entry at index 1 differs between
// replicas and Log Matching is violated on first contact. The maps are
// populated in different literal orders to make map-iteration nondeterminism,
// if any leaked in, observable.
func TestBuildInitialMembershipEntry_Deterministic(t *testing.T) {
	a := proto.Membership{
		Addresses:                  map[uint64]string{3: "c", 1: "a", 2: "b"},
		Observers:                  map[uint64]string{5: "e", 4: "d"},
		Witnesses:                  map[uint64]string{7: "g", 6: "f"},
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
	}
	b := proto.Membership{
		Addresses:                  map[uint64]string{1: "a", 2: "b", 3: "c"},
		Observers:                  map[uint64]string{4: "d", 5: "e"},
		Witnesses:                  map[uint64]string{6: "f", 7: "g"},
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
	}

	ea, err := BuildInitialMembershipEntry(a)
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	eb, err := BuildInitialMembershipEntry(b)
	if err != nil {
		t.Fatalf("build b: %v", err)
	}

	if ea.Index != BootstrapMembershipIndex || ea.Term != bootstrapMembershipTerm {
		t.Fatalf("entry landed at index %d term %d, want index %d term %d",
			ea.Index, ea.Term, BootstrapMembershipIndex, bootstrapMembershipTerm)
	}
	if ea.Type&proto.EntryTypeMask != proto.EntryInitialMembership {
		t.Fatalf("entry type = %d, want EntryInitialMembership", ea.Type&proto.EntryTypeMask)
	}
	if !bytes.Equal(ea.Cmd, eb.Cmd) {
		t.Fatal("two replicas built DIFFERENT bootstrap entries from the same " +
			"configuration: the entry at index 1 would differ between replicas and " +
			"Log Matching is violated the moment they compare logs")
	}
}

// TestBuildInitialMembershipEntry_WitnessPolicyIsPartOfTheInput pins the
// determinism CONTRACT: the witness policy (MaxWitnesses /
// RequireFullReplicaMajority) is part of the bootstrap input, not only the
// address map. Two founders with an IDENTICAL members map but DIFFERENT local
// witness policy produce byte-DIFFERENT entries at the same (index=1, term=1),
// which AppendEntries (matching on index+term only) would never reconcile. This
// test documents that every founding member must be configured with an identical
// witness policy, exactly as it must pass an identical member list.
func TestBuildInitialMembershipEntry_WitnessPolicyIsPartOfTheInput(t *testing.T) {
	base := map[uint64]string{1: "a", 2: "b", 3: "c"}

	strict, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: base, MaxWitnesses: 1, RequireFullReplicaMajority: true,
	})
	if err != nil {
		t.Fatalf("build strict: %v", err)
	}
	loose, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: base, MaxWitnesses: 2, RequireFullReplicaMajority: false,
	})
	if err != nil {
		t.Fatalf("build loose: %v", err)
	}

	if strict.Index != loose.Index || strict.Term != loose.Term {
		t.Fatalf("fixture: entries must share (index,term); got (%d,%d) vs (%d,%d)",
			strict.Index, strict.Term, loose.Index, loose.Term)
	}
	if bytes.Equal(strict.Cmd, loose.Cmd) {
		t.Fatal("differing witness policy produced IDENTICAL bootstrap bytes: the " +
			"policy would silently NOT be part of the committed configuration, and a " +
			"cluster could form with each founder using its own policy undetected")
	}
	// The corollary: SAME policy + same members => identical, which is the safe
	// path founders must stay on.
	same, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: base, MaxWitnesses: 1, RequireFullReplicaMajority: true,
	})
	if err != nil {
		t.Fatalf("build same: %v", err)
	}
	if !bytes.Equal(strict.Cmd, same.Cmd) {
		t.Fatal("identical input produced different bytes: determinism is broken")
	}
}

// TestInitialMembershipRoundTrip confirms the entry decodes back to the
// configuration it was built from, with ConfigChangeID pinned to the entry's
// own index.
func TestInitialMembershipRoundTrip(t *testing.T) {
	src := proto.Membership{
		Addresses:                  map[uint64]string{1: "a", 2: "b", 3: "c"},
		Witnesses:                  map[uint64]string{4: "d"},
		MaxWitnesses:               1,
		RequireFullReplicaMajority: true,
	}
	e, err := BuildInitialMembershipEntry(src)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !e.IsInitialMembership() {
		t.Fatal("IsInitialMembership() is false for a bootstrap entry")
	}

	got, err := initialMembershipFromEntry(e)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ConfigChangeID != 0 {
		t.Fatalf("ConfigChangeID = %d, want 0 (the initial configuration predates "+
			"any config change; stamping it would break a compare-and-swap that read 0)",
			got.ConfigChangeID)
	}
	if len(got.Addresses) != 3 || got.Addresses[2] != "b" {
		t.Fatalf("voters not round-tripped: %v", got.Addresses)
	}
	if len(got.Witnesses) != 1 || got.Witnesses[4] != "d" {
		t.Fatalf("witnesses not round-tripped: %v", got.Witnesses)
	}
	if got.MaxWitnesses != 1 || !got.RequireFullReplicaMajority {
		t.Fatalf("witness policy not round-tripped: max=%d requireFull=%v",
			got.MaxWitnesses, got.RequireFullReplicaMajority)
	}
}

// TestJoiningClearsWhenBootstrapEntryApplied is the Stage-4 property: a joiner
// that started with empty membership ends its joining window the moment it
// applies the bootstrap membership entry the leader replicates to it. Before
// config-in-log the window could only be closed by a snapshot, so a joiner
// caught up by replication alone stayed joining permanently.
//
// The window gates CAMPAIGNING (a replica holding an incomplete configuration
// must not self-elect on a quorum of one) and the configuration test in
// handleVoteRequest. It is no longer an authorization bypass: per-shard
// authorization is a removed-tombstone deny-list that needs no bypass. See
// Peer.IsRemoved.
func TestJoiningClearsWhenBootstrapEntryApplied(t *testing.T) {
	// Replica 4 joins a {1,2,3} cluster: it starts knowing no members.
	p := newTestPeer(t, testConfigWithID(1, 4), proto.State{}, proto.Membership{})
	p.MarkJoining()
	if !p.joining.Load() {
		t.Fatal("fixture: joining flag must be set")
	}

	// The joiner is following leader 1 (a founding member). The bypass clears
	// only once it knows THIS leader is a member.
	p.r.leaderID = 1

	// The leader replicates the committed bootstrap entry (index 1) carrying the
	// full initial configuration. Applying it at commit time is what
	// processConfigChanges does on the joiner.
	entry, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
	})
	if err != nil {
		t.Fatalf("build bootstrap entry: %v", err)
	}
	p.processConfigChanges([]proto.Entry{entry})

	if p.joining.Load() {
		t.Fatal("joining was NOT cleared after applying the bootstrap membership " +
			"entry: the IsMember bypass would persist for the life of the process, " +
			"exactly the permanent-bypass bug config-in-log removes")
	}
	if p.IsMember(999) {
		t.Fatal("IsMember still authorizes an unknown replica after the joining " +
			"window ended: per-shard authorization is still disabled")
	}
	for _, id := range []uint64{1, 2, 3} {
		if !p.IsMember(id) {
			t.Fatalf("member %d not recognized after reconstructing membership from "+
				"the bootstrap entry", id)
		}
	}
}

// TestJoiningDoesNotClearBeforeLeaderIsKnown pins that a joiner following a leader that was ADDED AFTER the initial bootstrap must NOT
// clear the auth bypass merely from learning the founding membership. If
// replication delivers the bootstrap entry in an earlier batch than that
// leader's AddNode, clearing on "knows any member" would then reject the current
// leader's next append and strand the joiner forever.
func TestJoiningDoesNotClearBeforeLeaderIsKnown(t *testing.T) {
	// Replica 6 joins a cluster whose founding members are {1,2,3}. The CURRENT
	// leader is replica 5 — added after bootstrap, so NOT in the index-1 entry.
	p := newTestPeer(t, testConfigWithID(1, 6), proto.State{}, proto.Membership{})
	p.MarkJoining()
	p.r.leaderID = 5 // following leader 5, which the bootstrap entry does not name

	// Batch 1: only the bootstrap entry (founders {1,2,3}), split from leader 5's
	// AddNode by the replication byte limit.
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
	})
	if err != nil {
		t.Fatalf("build bootstrap entry: %v", err)
	}
	p.processConfigChanges([]proto.Entry{bootstrap})

	if !p.joining.Load() {
		t.Fatal("the joiner left the joining window after learning only the FOUNDING " +
			"members — but the current leader 5 was added after bootstrap and is not " +
			"among them, so its configuration is still incomplete and the vote guard " +
			"in handleVoteRequest would deny the real members it must vote for")
	}
	// Leader 5 can always reach the joiner regardless: authorization is a
	// deny-list, and leader 5 carries no removal tombstone.
	if p.IsRemoved(5) {
		t.Fatal("the joiner refused leader 5")
	}

	// Batch 2: leader 5's AddNode arrives. Now the joiner knows the leader.
	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 5, Address: "e"}
	ccBuf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(ccBuf); err != nil {
		t.Fatalf("marshal AddNode: %v", err)
	}
	p.processConfigChanges([]proto.Entry{{
		Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: ccBuf,
	}})

	if p.joining.Load() {
		t.Fatal("the joiner did NOT clear the bypass after learning the leader 5 is a " +
			"member: the bypass would now persist for the process lifetime")
	}
	if p.IsMember(999) {
		t.Fatal("IsMember still authorizes an unknown replica after the bypass ended")
	}
}

// TestJoiningClearsOnRestartReplayWithoutLeader is the residual-fix regression:
// on the RESTART path, replayConfigChangesTo (SetProcessedIndex) runs BEFORE the
// first Tick, so leaderID is noLeader and the leader-based clear cannot fire. A
// replica restarted with join=true whose durable log already holds the full
// membership must still clear the bypass — via the "self is a member" signal —
// rather than keep per-shard authorization disabled until the next snapshot.
func TestJoiningClearsOnRestartReplayWithoutLeader(t *testing.T) {
	// Replica 4 comes up joining, with NO leader contact yet (leaderID == noLeader,
	// the pre-first-Tick restart-replay state).
	p := newTestPeer(t, testConfigWithID(1, 4), proto.State{}, proto.Membership{})
	p.MarkJoining()
	if p.r.leaderID != noLeader {
		t.Fatalf("fixture: expected no leader, got %d", p.r.leaderID)
	}

	// Replay reconstructs the full committed membership from the durable log: the
	// bootstrap founders {1,2,3} and this replica's own AddNode(4).
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
	})
	if err != nil {
		t.Fatalf("build bootstrap entry: %v", err)
	}
	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "d"}
	ccBuf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(ccBuf); err != nil {
		t.Fatalf("marshal AddNode: %v", err)
	}
	p.processConfigChanges([]proto.Entry{
		bootstrap,
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: ccBuf},
	})

	if p.joining.Load() {
		t.Fatal("a join=true replica that replayed its FULL membership on restart " +
			"(with no leader yet) did not clear the auth bypass: IsMember stays disabled " +
			"for the process until a snapshot or new config change — a lingering per-shard " +
			"authorization bypass")
	}
	if p.IsMember(999) {
		t.Fatal("IsMember still authorizes an unknown replica after restart-replay")
	}
}

// TestJoiningClearsWhenReplayRemovesSelf is the third terminal state of the
// joining window: a join=true replica that replays a committed RemoveNode for
// ITSELF before any leader is known is neither a member (so the self signal is
// false) nor following a leader (so the leader signal is false). Without an
// explicit tombstone signal the auth bypass would persist for the whole process
// — and because clearJoiningIfKnown only runs on membership changes, no later
// heartbeat would ever clear it.
func TestJoiningClearsWhenReplayRemovesSelf(t *testing.T) {
	// Replica 4 comes up joining with no leader contact (restart-replay state).
	p := newTestPeer(t, testConfigWithID(1, 4), proto.State{}, proto.Membership{})
	p.MarkJoining()
	if p.r.leaderID != noLeader {
		t.Fatalf("fixture: expected no leader, got %d", p.r.leaderID)
	}

	// Replay: the founding membership, this replica's AddNode, then the committed
	// RemoveNode that took it back out of the configuration.
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
	})
	if err != nil {
		t.Fatalf("build bootstrap entry: %v", err)
	}
	ccEntry := func(index uint64, t2 uint64, cc proto.ConfigChange) proto.Entry {
		t.Helper()
		buf := make([]byte, cc.Size())
		if _, err := cc.MarshalTo(buf); err != nil {
			t.Fatalf("marshal config change: %v", err)
		}
		return proto.Entry{Index: index, Term: t2, Type: proto.EntryConfigChange, Cmd: buf}
	}
	p.processConfigChanges([]proto.Entry{
		bootstrap,
		ccEntry(2, 1, proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "d"}),
		ccEntry(3, 1, proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 4}),
	})

	if p.membership.isMember(4) {
		t.Fatal("fixture: replica 4 should have been removed by the replayed RemoveNode")
	}
	if p.joining.Load() {
		t.Fatal("a join=true replica that replayed its OWN committed removal did not " +
			"clear the auth bypass: it is not a member and has no leader, so no later " +
			"membership change or heartbeat will ever clear it — IsMember authorizes " +
			"every sender for the life of the process")
	}
	if p.IsMember(999) {
		t.Fatal("IsMember still authorizes an unknown replica after self-removal replay")
	}
}

// configChangeEntry marshals cc into an EntryConfigChange log entry at index.
func configChangeEntry(t *testing.T, index uint64, cc proto.ConfigChange) proto.Entry {
	t.Helper()
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		t.Fatalf("marshal config change: %v", err)
	}
	return proto.Entry{Index: index, Term: 1, Type: proto.EntryConfigChange, Cmd: buf}
}

// TestJoiningDoesNotClearOnSelfOnlyMembership covers the other end of the same
// hazard as TestJoiningDoesNotClearBeforeLeaderIsKnown: a joiner that applies an
// AddNode for ITSELF without yet holding the entry that names the rest of the
// shard. Self-membership alone is not knowledge of the cluster. Clearing on it
// would leave the replica with a configuration of exactly {self}, which
// authorizes nobody — so it would reject the leader's next append and could
// never learn more. Permanent, and self-inflicted.
func TestJoiningDoesNotClearOnSelfOnlyMembership(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 6), proto.State{}, proto.Membership{})
	p.MarkJoining()
	// No leader known yet — this replica has not been contacted by one.
	p.r.leaderID = noLeader

	// The only membership-bearing entry this replica has is its own addition.
	p.processConfigChanges([]proto.Entry{configChangeEntry(t, 10, proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 6,
		Address:   "f",
	})})

	if !p.membership.isMember(6) {
		t.Fatal("precondition: the AddNode for self should have applied")
	}
	if p.membership.hasMemberOtherThan(6) {
		t.Fatal("precondition: the configuration should name only replica 6")
	}
	if !p.joining.Load() {
		t.Fatal("the joiner cleared the auth bypass on a configuration naming ONLY " +
			"itself: it can now authorize nobody, so it will reject the leader's next " +
			"append and can never learn the rest of the membership")
	}

	// Once a second member is named, the configuration can authorize somebody
	// and the bypass correctly ends.
	p.processConfigChanges([]proto.Entry{configChangeEntry(t, 11, proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 2,
		Address:   "b",
	})})
	if p.joining.Load() {
		t.Fatal("the bypass should end once the configuration names a member besides self")
	}
	if p.IsMember(999) {
		t.Fatal("bypass still active: a non-member was authorized")
	}
}

// TestMarkJoiningClearsWhenMembershipIsAlreadyKnown pins the restart hole.
// join=true is a STARTUP FLAG, not durable state: an operator restarting an
// established replica keeps it set. NewPeer has already seeded the membership
// from the persisted NodeState, so this replica knows the whole shard — but if
// MarkJoining only ever set the flag, nothing would reconsider it. In a
// quiescent shard (no config change, no snapshot) the bypass would then
// authorize every replica ID for the entire lifetime of the process.
func TestMarkJoiningClearsWhenMembershipIsAlreadyKnown(t *testing.T) {
	known := proto.Membership{
		Addresses:      map[uint64]string{1: "a", 2: "b", 3: "c"},
		ConfigChangeID: 7,
	}
	p := newTestPeer(t, testConfigWithID(1, 3), proto.State{}, known)

	p.MarkJoining()

	if p.joining.Load() {
		t.Fatal("a restarted replica with a complete persisted membership is still " +
			"marked joining: nothing else would reconsider the flag in a quiescent " +
			"shard, so per-shard auth is bypassed permanently")
	}
	if p.IsMember(999) {
		t.Fatal("auth bypass survived the restart: a non-member was authorized")
	}
	if !p.IsMember(2) {
		t.Fatal("a real member was rejected after the restart")
	}
}

// TestInitialMembershipOutsideBootstrapIndexIsRefused pins the index check on
// the founding configuration.
//
// Applying an EntryInitialMembership REPLACES the membership wholesale rather
// than amending it, so one accepted at any index above bootstrap would discard
// every config change committed since — rolling the voting set back to the
// founding members, a configuration no live replica shares, while this replica
// went on computing quorum over it.
func TestInitialMembershipOutsideBootstrapIndexIsRefused(t *testing.T) {
	// An established shard: founded as {1,2,3}, replicas 4 and 5 added since.
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, proto.Membership{
		Addresses:      map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"},
		ConfigChangeID: 40,
	})

	founding, err := BuildInitialMembershipEntry(proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
	})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}

	// The same entry, but replicated at a later index.
	stray := founding
	stray.Index = 500
	if applied, err := p.applyMembershipEntry(stray); applied || err != nil {
		t.Fatalf("an initial-membership entry above the bootstrap index was applied "+
			"(applied=%v err=%v): the refusal is structural, not a decode failure, "+
			"so it must be a silent no-op rather than a shard fault", applied, err)
	}
	for _, id := range []uint64{4, 5} {
		if !p.membership.isMember(id) {
			t.Fatalf("replica %d was dropped from the configuration by a stray "+
				"initial-membership entry: this replica now computes quorum over "+
				"the FOUNDING set, which no live replica shares (membership=%v)",
				id, p.membership.addresses)
		}
	}

	// At the bootstrap index it is of course still applied.
	if applied, err := p.applyMembershipEntry(founding); !applied || err != nil {
		t.Fatalf("the founding entry at the bootstrap index was refused "+
			"(applied=%v err=%v)", applied, err)
	}
}
