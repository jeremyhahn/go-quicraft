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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// M1: membership.apply's replay guard
// ---------------------------------------------------------------------------
//
// The guard skips a config change whose log index is at or below the current
// configChangeID. It is reached on every restart that recovers a snapshot:
// CompactionOverhead (default 5000) deliberately RETAINS entries below the
// snapshot index, so Peer.replayConfigChangesTo re-offers config changes the
// snapshot membership already accounts for.
//
// Removing the guard is not a harmless double-apply. configChangeID REGRESSES
// to the replayed entry's index, and a replayed UpdateWitnessPolicy reverts the
// shard's replicated policy to a superseded value.

// TestMembershipApply_ReplayGuard_SkipsAlreadyAppliedChanges drives the guard
// directly: three offers at, below, and above the current configChangeID.
func TestMembershipApply_ReplayGuard_SkipsAlreadyAppliedChanges(t *testing.T) {
	m := newMembership(proto.Membership{
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeID: 3,
		MaxWitnesses:   7,
	})

	// BELOW the marker: a retained entry the snapshot membership already
	// covers. Applying it would add a member the configuration removed long
	// ago and would drag configChangeID backwards.
	if err := m.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "a4",
	}, 2); err != nil {
		t.Fatalf("replayed AddNode must be skipped silently, got %v", err)
	}
	if m.isMember(4) {
		t.Fatal("replayed AddNode(4) at index 2 was applied on top of " +
			"configChangeID=3: the snapshot membership already accounts for " +
			"every change at or below 3, so this replica now holds a member " +
			"the rest of the shard does not and computes quorum over the wrong " +
			"voting set")
	}
	if m.configChangeID != 3 {
		t.Fatalf("configChangeID REGRESSED from 3 to %d: the marker is "+
			"replicated state and must advance monotonically, or every later "+
			"replay guard decision is taken against a stale baseline",
			m.configChangeID)
	}

	// EQUAL to the marker: the boundary case. This is the entry the snapshot
	// membership was built from, retained below the compaction floor.
	if err := m.apply(proto.ConfigChange{
		Type:                       proto.UpdateWitnessPolicy,
		MaxWitnesses:               1,
		RequireFullReplicaMajority: true,
	}, 3); err != nil {
		t.Fatalf("replayed UpdateWitnessPolicy must be skipped silently, got %v", err)
	}
	if m.maxWitnesses != 7 {
		t.Fatalf("a replayed UpdateWitnessPolicy at index 3 REVERTED the "+
			"replicated witness policy from MaxWitnesses=7 to %d. The guard is "+
			"the only thing that makes a non-idempotent config change safe to "+
			"re-offer, and re-offering is the normal restart path",
			m.maxWitnesses)
	}
	if m.requireFullReplicaMajority {
		t.Fatal("a replayed UpdateWitnessPolicy reverted " +
			"RequireFullReplicaMajority to a superseded value")
	}
	if m.configChangeID != 3 {
		t.Fatalf("configChangeID moved to %d on a skipped replay", m.configChangeID)
	}

	// ABOVE the marker: genuinely new, must still apply. Without this the test
	// would also pass against an apply() that skips everything.
	if err := m.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "a4",
	}, 4); err != nil {
		t.Fatalf("AddNode above the marker: %v", err)
	}
	if !m.isVoter(4) {
		t.Fatal("a config change above configChangeID was not applied")
	}
	if m.configChangeID != 4 {
		t.Fatalf("expected configChangeID 4, got %d", m.configChangeID)
	}
}

// TestPeerReplay_GuardStopsSnapshotCoveredEntriesFromReverting reaches the
// guard the way production does: a restart that recovered a snapshot whose
// membership is at configChangeID 5, over a log that still HOLDS the
// superseded policy change at index 2 (CompactionOverhead retains it).
func TestPeerReplay_GuardStopsSnapshotCoveredEntriesFromReverting(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t,
			proto.ConfigChange{Type: proto.UpdateWitnessPolicy, MaxWitnesses: 1})},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
	}
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 6})
	// The recovered snapshot membership already reflects the LATER policy
	// change (MaxWitnesses 7) that superseded index 2.
	lr.membership = proto.Membership{
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeID: 5,
		MaxWitnesses:   7,
	}

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}

	p.SetProcessedIndex(6)

	if p.membership.maxWitnesses != 7 {
		t.Fatalf("restart replay reverted the replicated witness policy to "+
			"MaxWitnesses=%d: the retained entry at index 2 is already covered "+
			"by the recovered snapshot membership (configChangeID=5) and must "+
			"be skipped", p.membership.maxWitnesses)
	}
	if p.membership.configChangeID != 5 {
		t.Fatalf("restart replay regressed configChangeID from 5 to %d",
			p.membership.configChangeID)
	}
	if got := p.GetMembership().MaxWitnesses; got != 7 {
		t.Fatalf("published membership snapshot carries MaxWitnesses=%d", got)
	}
	if p.r.maxWitnesses != 7 {
		t.Fatalf("the raft-layer propose gate mirrors MaxWitnesses=%d",
			p.r.maxWitnesses)
	}
}

// ---------------------------------------------------------------------------
// P4: SetProcessedIndex must fold the SM-skipped window's config changes
// ---------------------------------------------------------------------------

// An on-disk state machine reports a durable lastApplied ahead of the
// snapshot index, so the engine skips re-applying that window. Commit-time
// membership semantics mean those entries never reach processConfigChanges,
// which is the ONLY other place membership is applied. If SetProcessedIndex
// does not fold them in here, nothing ever does, and this replica comes up
// computing quorum over a stale voting set — exactly what the function's own
// doc comment warns about.
//
// The two pre-existing tests only assert the NEGATIVE (an UNCOMMITTED change
// must not be folded), so deleting the replay call broke no test at all.
func TestSetProcessedIndex_FoldsCommittedConfigChangeInSkippedWindow(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		// COMMITTED AddNode(4): inside the window the state machine already
		// applied and the engine will therefore never re-deliver.
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t,
			proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"})},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 5})
	lr.membership = threeNodeMembership()

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	// Fixture check: replica 4 is not a member until the replay folds it in.
	if p.membership.isVoter(4) {
		t.Fatal("fixture: replica 4 must not be in the recovered membership")
	}
	quorumBefore := quorum(p.r.remotes, p.r.witnesses)
	if quorumBefore != 2 {
		t.Fatalf("fixture: expected a 3-voter quorum of 2, got %d", quorumBefore)
	}

	applied := p.SetProcessedIndex(5)
	if applied != 5 {
		t.Fatalf("expected the processed marker to settle at 5, got %d", applied)
	}

	if !p.membership.isVoter(4) {
		t.Fatal("the committed AddNode(4) at index 3 was NOT folded into the " +
			"membership. The state machine's durable marker made the engine " +
			"skip indices 1..5, so entry 3 never reaches CommittedEntries and " +
			"never reaches processConfigChanges — SetProcessedIndex is the last " +
			"chance to apply it. This replica now believes the shard has 3 " +
			"voters while it has 4, and commits on a quorum of 2 that the real " +
			"configuration does not consider a majority")
	}
	if p.membership.configChangeID != 3 {
		t.Fatalf("expected configChangeID 3 after folding the replay, got %d",
			p.membership.configChangeID)
	}
	if _, ok := p.r.remotes[4]; !ok {
		t.Fatal("replica 4 has no remote: the leader would never replicate to " +
			"it and its acks would not be tallied")
	}
	if got := quorum(p.r.remotes, p.r.witnesses); got != 3 {
		t.Fatalf("quorum is computed over a stale voting set: got %d, want 3 "+
			"(4 voters)", got)
	}
	// The published snapshot backs IsMember on the transport recv path, so the
	// fold has to be visible there too or replica 4's messages are refused.
	if !p.IsMember(4) {
		t.Fatal("replica 4 is absent from the published membership snapshot: " +
			"the transport would reject its messages as unauthorized")
	}
}
