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

// ---------------------------------------------------------------------------
// The startup fold must REPORT a corrupt committed config entry
//
// Three paths fold committed config changes, and they disagreed about what an
// UNDECODABLE one means:
//
//   - the commit pipeline (processConfigChanges) faults the shard;
//   - recovery (ReconstructMembership) aborts StartShard;
//   - the startup fold (SetProcessedIndex) logged an ERROR, armed the staleness
//     watermark and started the shard anyway.
//
// The third is the one that failed OPEN. Refusing to campaign is a LIVENESS
// answer to a SAFETY fact: the replica came up on a configuration it knows is
// unreconstructable and then served reads and acked the leader from it. Its
// caller took only the clamped uint64, so it could not do better; widening the
// signature is what lets Host.StartShard reach the same verdict as the other
// two.
// ---------------------------------------------------------------------------

// corruptTailFixture builds a replica whose committed log holds an
// EntryConfigChange at index 31 that cannot be decoded, above a readable
// 3-voter bootstrap entry. It is staleTailFixture with the tail entry
// corrupted, so the two differ in exactly the fact under test.
func corruptTailFixture(t *testing.T) *Peer {
	t.Helper()
	base := map[uint64]string{1: "a1", 2: "a2", 3: "a3"}
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{Addresses: base})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	entries := []proto.Entry{bootstrap}
	for i := uint64(2); i <= 30; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1, Cmd: []byte("x")})
	}
	// A config-change entry whose Cmd is not a marshaled ConfigChange. Non-empty,
	// because an empty Cmd is the documented silent no-op rather than corruption.
	entries = append(entries, proto.Entry{
		Index: 31, Term: 1, Type: proto.EntryConfigChange,
		Cmd: []byte{0xFF, 0xFF, 0xFF},
	})
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 31})
	lr.membership = proto.Membership{Addresses: base}

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	return p
}

// TestSetProcessedIndex_ReportsACorruptCommittedConfigEntry is the regression.
// The error must reach the caller; the watermark must ALSO still be armed, so a
// caller that ignores the error keeps the old fail-closed campaigning behaviour
// rather than getting nothing.
func TestSetProcessedIndex_ReportsACorruptCommittedConfigEntry(t *testing.T) {
	quietLogs(t)

	p := corruptTailFixture(t)
	applied, err := p.SetProcessedIndex(31)
	if !errors.Is(err, ErrCorruptConfigEntry) {
		t.Fatalf("SetProcessedIndex: got err %v, want ErrCorruptConfigEntry.\n"+
			"The commit pipeline faults the shard for these bytes and recovery "+
			"aborts StartShard for them; the startup fold swallowing them is the "+
			"one path that failed open, and the replica then served reads from a "+
			"configuration it knows it cannot reconstruct", err)
	}
	if applied != 31 {
		t.Fatalf("applied = %d, want 31: the clamped index must still be reported "+
			"so the caller's markers agree with raft's", applied)
	}
	if p.staleConfigThrough == 0 {
		t.Fatal("the staleness watermark was cleared over an unreadable window: a " +
			"caller that ignores the error must still get the campaign suppression")
	}
	if p.membership.isVoter(4) {
		t.Fatal("fixture: the corrupt entry must not have changed the membership")
	}
	// Recorded for the Host to log on its own logger (Peer.UnfoldedConfig).
	if c := p.UnfoldedConfig(); !c.Recorded() || c.Index != 31 ||
		c.StaleThrough != p.staleConfigThrough || !errors.Is(c.Err, ErrCorruptConfigEntry) {
		t.Fatalf("UnfoldedConfig = %+v, want a recorded fold to 31 carrying ErrCorruptConfigEntry", c)
	}
}

// TestSetProcessedIndex_CleanFoldReportsNoError is the POSITIVE CONTROL, and it
// is what stops the assertion above from being satisfied by a method that
// returns an error unconditionally. A readable tail must fold, clear the
// watermark, and report success.
func TestSetProcessedIndex_CleanFoldReportsNoError(t *testing.T) {
	quietLogs(t)

	p, _ := staleTailFixture(t)
	applied, err := p.SetProcessedIndex(31)
	if err != nil {
		t.Fatalf("SetProcessedIndex over a readable tail: %v; a shard whose log is "+
			"intact must start", err)
	}
	if applied != 31 {
		t.Fatalf("applied = %d, want 31", applied)
	}
	if p.staleConfigThrough != 0 {
		t.Fatalf("staleConfigThrough = %d after a complete fold", p.staleConfigThrough)
	}
	if !p.membership.isVoter(4) {
		t.Fatal("the readable tail was not folded")
	}
	if c := p.UnfoldedConfig(); c.Recorded() {
		t.Fatalf("UnfoldedConfig = %+v after a complete fold, want none", c)
	}
}

// TestSetProcessedIndex_UnreadableButNotCorruptIsNotAnError pins the
// distinction the return value has to make. A window this replica could not READ
// is not a window it can never read: the commit pipeline or
// resolveStaleStartupConfig may cover it later, so it arms the watermark and
// reports no error. Only "these committed bytes will never decode" is terminal.
//
// Without this the fix would turn every transient log-read failure at startup
// into a refusal to start the shard, which is a far worse availability
// regression than the safety hole it closes.
func TestSetProcessedIndex_UnreadableButNotCorruptIsNotAnError(t *testing.T) {
	quietLogs(t)

	p, lr := staleTailFixture(t)
	lr.failing = true
	applied, err := p.SetProcessedIndex(31)
	if err != nil {
		t.Fatalf("SetProcessedIndex over an UNREADABLE (not corrupt) tail returned "+
			"%v; a transient read failure must arm the watermark and let the shard "+
			"start, or every such failure becomes a refusal to load the shard", err)
	}
	if applied != 31 {
		t.Fatalf("applied = %d, want 31", applied)
	}
	if p.staleConfigThrough == 0 {
		t.Fatal("the watermark was cleared over a window that was never read")
	}
}
