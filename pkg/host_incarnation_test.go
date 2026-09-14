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

// TestPendingProposal_OwnsPlacesAnEntryByIndexAndPayload pins the two
// discriminators #186 added to proposal matching, in isolation. A key match
// used to be the whole test; a previous incarnation of the same replica mints
// the same keys, so a key match alone completed a caller with another
// entry's result.
func TestPendingProposal_OwnsPlacesAnEntryByIndexAndPayload(t *testing.T) {
	cmd := []byte("put k v-new")
	pp := &pendingProposal{
		floor:    100,
		cmdKnown: true,
		cmdLen:   len(cmd),
		cmdHash:  hashCmd(cmd),
	}
	own := proto.Entry{Index: 101, Key: 7, Cmd: cmd}
	if !pp.owns(own) {
		t.Fatal("the proposal's own entry -- above the floor, same payload -- was not owned")
	}
	if !pp.mayOwn(own) {
		t.Fatal("the proposal's own entry was not placed above the floor")
	}

	replayed := proto.Entry{Index: 100, Key: 7, Cmd: cmd}
	if pp.mayOwn(replayed) || pp.owns(replayed) {
		t.Fatal("an entry AT the floor was committed before the proposal registered; it cannot be the proposal")
	}
	replayed.Index = 3
	if pp.owns(replayed) {
		t.Fatal("a replayed entry far below the floor was owned")
	}

	uncommitted := proto.Entry{Index: 101, Key: 7, Cmd: []byte("put k v-old")}
	if !pp.mayOwn(uncommitted) {
		t.Fatal("by index alone an uncommitted old entry above the floor is admissible; that is what the payload is for")
	}
	if pp.owns(uncommitted) {
		t.Fatal("an entry above the floor carrying another payload was owned: the crash-loop collision")
	}
	shorter := proto.Entry{Index: 101, Key: 7, Cmd: cmd[:len(cmd)-1]}
	if pp.owns(shorter) {
		t.Fatal("a payload of a different length was owned")
	}

	// The commit path sees the entry as the log holds it: uncompressed, the
	// full check; compressed, index alone, because the recorded identity is
	// of the bytes that were proposed.
	if pp.ownsCommitted(uncommitted) {
		t.Fatal("the commit path owned an uncompressed entry with another payload")
	}
	compressed := proto.Entry{Index: 101, Key: 7, Cmd: []byte("snappy bytes"), Type: proto.EntryNormal | proto.EntryCompressFlag}
	if !pp.ownsCommitted(compressed) {
		t.Fatal("the commit path must place a compressed entry by index alone")
	}
	compressed.Index = 100
	if pp.ownsCommitted(compressed) {
		t.Fatal("the floor must hold for a compressed entry on the commit path")
	}

	// The zero value carries no payload identity and is placed by index alone,
	// which is what every test that builds a pendingProposal literal relies on.
	bare := &pendingProposal{floor: 100}
	if !bare.owns(uncommitted) {
		t.Fatal("a proposal with no recorded payload must be placed by index alone")
	}
	if bare.owns(replayed) {
		t.Fatal("the floor must hold even without a recorded payload")
	}
}

// TestPropose_RegistersTheFloorAndThePayload pins that the production
// registration path records what owns needs. A pendingProposal literal in a
// test can leave them zero; Propose must not, or the discriminators are
// silently absent exactly where they matter.
func TestPropose_RegistersTheFloorAndThePayload(t *testing.T) {
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

	cmd := []byte("held behind the poison")
	rs, err := h.Propose(ctx, 1, cmd)
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
	if !pp.cmdKnown {
		t.Fatal("Propose registered no payload identity")
	}
	if pp.cmdLen != len(cmd) || pp.cmdHash != hashCmd(cmd) {
		t.Fatalf("payload identity (%d, %d) does not match the command (%d, %d)",
			pp.cmdLen, pp.cmdHash, len(cmd), hashCmd(cmd))
	}
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
	// An entry that carries this proposal's key but a payload it never had
	// is refused by the recorded identity; the proposal's own shape is owned.
	if pp.owns(proto.Entry{Index: pp.floor + 1, Key: rs.key, Cmd: []byte("other")}) {
		t.Fatal("a foreign payload under this proposal's key was owned")
	}
	if !pp.owns(proto.Entry{Index: pp.floor + 1, Key: rs.key, Cmd: cmd}) {
		t.Fatal("the proposal's own payload was not owned")
	}
}
