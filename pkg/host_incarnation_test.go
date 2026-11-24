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
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestPendingProposal_OwnsPlacesAnEntryByIndex pins the floor: the one check in
// proposal matching that is exact rather than probabilistic. A key repeats
// across hosts or incarnations only by a 2^-64 accident, and when it does, an
// entry at or below the committed index recorded at registration is still
// refused.
func TestPendingProposal_OwnsPlacesAnEntryByIndex(t *testing.T) {
	pp := &pendingProposal{floor: 100}
	if !pp.owns(proto.Entry{Index: 101, Key: 7}) {
		t.Fatal("an entry above the floor was not owned")
	}
	if pp.owns(proto.Entry{Index: 100, Key: 7}) {
		t.Fatal("an entry AT the floor was committed before the proposal registered; it cannot be the proposal")
	}
	if pp.owns(proto.Entry{Index: 3, Key: 7}) {
		t.Fatal("a replayed entry far below the floor was owned")
	}
	// The zero value has floor 0 and owns every entry, which is what every
	// test that builds a pendingProposal literal relies on.
	if !(&pendingProposal{}).owns(proto.Entry{Index: 1, Key: 7}) {
		t.Fatal("a zero-value proposal must own any entry")
	}
}

// TestPropose_RegistersTheFloor pins that the production registration path
// records the floor. A pendingProposal literal in a test can leave it zero;
// Propose must not, or the check is silently absent exactly where it matters.
func TestPropose_RegistersTheFloor(t *testing.T) {
	h, machine, _ := newLatchableHost(t)
	// Park the state machine so the proposal stays pending long enough to be
	// inspected; the poison entry is what Update parks on.
	release := parkPoison(t, machine)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	poison, err := h.Propose(ctx, 1, poisonCmd)
	if err != nil {
		t.Fatalf("Propose(poison): %v", err)
	}
	defer poison.Release()
	<-machine.poisonHeld

	rs, err := h.Propose(ctx, 1, []byte("held behind the poison"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()

	ns, err := h.getNode(1)
	if err != nil {
		t.Fatalf("getNode: %v", err)
	}
	v, ok := ns.pendingProposals.Load(rs.key)
	if !ok {
		t.Fatalf("proposal %d is not pending", rs.key)
	}
	pp := v.(*pendingProposal)
	// The floor is the committed index at registration; a single-node shard
	// that has already applied entries has committed at least one, and the
	// poison entry is committed by now (it is parked in APPLY, not commit).
	if pp.floor == 0 {
		t.Fatal("Propose registered floor 0 on a shard that has committed entries")
	}
	engNode := h.engine.GetNode(1)
	if committed := engNode.CommittedIndex(); pp.floor > committed {
		t.Fatalf("floor %d is above the committed index %d", pp.floor, committed)
	}
	if pp.owns(proto.Entry{Index: pp.floor, Key: rs.key}) {
		t.Fatal("an entry at the floor was owned")
	}
	if !pp.owns(proto.Entry{Index: pp.floor + 1, Key: rs.key}) {
		t.Fatal("an entry above the floor was not owned")
	}
}
