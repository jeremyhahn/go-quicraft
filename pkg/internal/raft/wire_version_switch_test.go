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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// nextWindow is the window of a build one wire version ahead of this one.
func nextWindow() proto.WireWindow {
	return proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion}
}

func wireSwitch(v uint64) proto.ConfigChange {
	return proto.ConfigChange{Type: proto.UpdateWireVersion, WireVersion: v}
}

// TestUpdateWireVersion_AppliesAtOneIndexAndNeverReverts pins the switch's
// three properties at the membership layer: it takes a zero ReplicaID, it
// moves the committed version forward at the entry's index, and a lower value
// or a replay at or below that index changes nothing.
func TestUpdateWireVersion_AppliesAtOneIndexAndNeverReverts(t *testing.T) {
	m := newMembership(proto.Membership{Addresses: map[uint64]string{1: "a1"}})
	m.wire = nextWindow()
	next := proto.WireVersion + 1

	if err := m.validate(wireSwitch(next)); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := m.apply(wireSwitch(next), 7); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if m.wireVersion != next || m.configChangeID != 7 {
		t.Fatalf("after apply: wireVersion=%d configChangeID=%d, want %d and 7",
			m.wireVersion, m.configChangeID, next)
	}

	// A replay at or below the applied index is skipped, whatever it says.
	if err := m.apply(wireSwitch(proto.WireVersion), 7); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if m.wireVersion != next {
		t.Fatalf("replay reverted the switch to %d", m.wireVersion)
	}

	// A fresh entry with a lower value is refused, at validate and at apply.
	if err := m.validate(wireSwitch(proto.WireVersion)); !errors.Is(err, proto.ErrUnsupportedVersion) {
		t.Fatalf("validate downgrade: got %v, want ErrUnsupportedVersion", err)
	}
	if err := m.apply(wireSwitch(proto.WireVersion), 8); !errors.Is(err, proto.ErrUnsupportedVersion) {
		t.Fatalf("apply downgrade: got %v, want ErrUnsupportedVersion", err)
	}
	if m.wireVersion != next || m.configChangeID != 7 {
		t.Fatal("a refused downgrade changed membership state")
	}

	// The value survives the snapshot round trip every restart and
	// InstallSnapshot goes through.
	if got := newMembership(m.snapshot().toProto()).wireVersion; got != next {
		t.Fatalf("snapshot round trip: wireVersion=%d, want %d", got, next)
	}
}

// TestUpdateWireVersion_RefusedWhenMembershipChangedSinceCheck pins that a
// switch names the membership its proposer checked: a config change that
// applies between that check and the proposal reaching the step worker gets
// the switch refused before append, on the Propose path the host uses as
// well as ProposeConfigChange, and a switch naming the current membership
// still commits.
func TestUpdateWireVersion_RefusedWhenMembershipChangedSinceCheck(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	if err := p.SetWireWindow(nextWindow()); err != nil {
		t.Fatalf("SetWireWindow: %v", err)
	}
	electSingleNodeLeader(t, p)
	runUpdateCycles(t, p)
	next := proto.WireVersion + 1

	checked := p.GetMembership()
	add := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: 2, Address: "addr2"}
	if err := p.ProposeConfigChange(add); err != nil {
		t.Fatalf("propose add: %v", err)
	}
	runUpdateCycles(t, p)
	current := p.GetMembership()
	if _, ok := current.Observers[2]; !ok || current.ConfigChangeID == checked.ConfigChangeID {
		t.Fatalf("add did not apply: observers=%v configChangeID=%d", current.Observers, current.ConfigChangeID)
	}

	stale := wireSwitch(next)
	stale.ConfigChangeID = checked.ConfigChangeID
	if err := p.ProposeConfigChange(stale); !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("ProposeConfigChange stale switch: got %v, want ErrPendingConfigChange", err)
	}
	last := p.r.log.lastIndex()
	entry := proto.Entry{Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t, stale)}
	if _, err := p.Propose([]proto.Entry{entry}); !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("Propose stale switch: got %v, want ErrPendingConfigChange", err)
	}
	if got := p.r.log.lastIndex(); got != last {
		t.Fatalf("a refused switch reached the log: lastIndex %d, want %d", got, last)
	}
	runUpdateCycles(t, p)
	if got := p.WireVersion(); got != proto.PreviousWireVersion {
		t.Fatalf("WireVersion after a refused switch: %d, want %d", got, proto.PreviousWireVersion)
	}

	fresh := wireSwitch(next)
	fresh.ConfigChangeID = current.ConfigChangeID
	if err := p.ProposeConfigChange(fresh); err != nil {
		t.Fatalf("propose current switch: %v", err)
	}
	runUpdateCycles(t, p)
	if got := p.WireVersion(); got != next {
		t.Fatalf("WireVersion after the switch: %d, want %d", got, next)
	}
}

// TestUpdateWireVersion_OutsideWindowIsRefused pins that this build refuses a
// committed version it does not read, and that the refusal is fatal to the
// shard on the commit path rather than a logged skip.
func TestUpdateWireVersion_OutsideWindowIsRefused(t *testing.T) {
	for _, v := range []uint64{proto.WireVersion + 1, proto.WireVersion + 2, ^uint64(0)} {
		m := newMembership(proto.Membership{Addresses: map[uint64]string{1: "a1"}})
		if err := m.validate(wireSwitch(v)); !errors.Is(err, proto.ErrUnsupportedVersion) {
			t.Fatalf("validate %d: got %v, want ErrUnsupportedVersion", v, err)
		}
		if err := m.apply(wireSwitch(v), 5); !errors.Is(err, proto.ErrUnsupportedVersion) {
			t.Fatalf("apply %d: got %v, want ErrUnsupportedVersion", v, err)
		}
	}

	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	entry := proto.Entry{
		Index: 2, Term: 1, Type: proto.EntryConfigChange,
		Cmd: marshalConfigChange(t, wireSwitch(proto.WireVersion+1)),
	}
	if _, err := p.applyMembershipEntry(entry); !errors.Is(err, proto.ErrUnsupportedVersion) {
		t.Fatalf("applyMembershipEntry: got %v, want ErrUnsupportedVersion (fatal)", err)
	}
	if got := p.WireVersion(); got != proto.PreviousWireVersion {
		t.Fatalf("WireVersion after a refused switch: %d, want %d", got, proto.PreviousWireVersion)
	}
}

// TestUpdateWireVersion_SnapshotOutsideWindowIsRefused pins that an
// InstallSnapshot whose membership is switched past this build's window is
// refused before it reaches the log.
func TestUpdateWireVersion_SnapshotOutsideWindowIsRefused(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	msg := proto.Message{Type: proto.InstallSnapshot, From: 2, To: 1, Term: 5}
	msg.Snapshot.Index = 10
	msg.Snapshot.Term = 5
	msg.Snapshot.Membership.Addresses = map[uint64]string{1: "a1", 2: "a2"}
	msg.Snapshot.Membership.WireVersion = proto.WireVersion + 1
	if err := p.Handle(msg); !errors.Is(err, proto.ErrUnsupportedVersion) {
		t.Fatalf("Handle: got %v, want ErrUnsupportedVersion", err)
	}
	if p.r.log.pendingSnapshot() != nil {
		t.Fatal("a refused snapshot reached the log")
	}

	// The next build's window accepts the same snapshot's version.
	if err := p.SetWireWindow(nextWindow()); err != nil {
		t.Fatalf("SetWireWindow: %v", err)
	}
	if err := p.Handle(msg); errors.Is(err, proto.ErrUnsupportedVersion) {
		t.Fatalf("Handle under the next window: %v", err)
	}
}
