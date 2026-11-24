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
	"slices"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// "the state machine applied it" is not "the membership folded it"
//
// SetProcessedIndex advanced p.r.log.processed UNCONDITIONALLY and then called
// replayConfigChangesTo, DISCARDING the report of whether the fold reached the
// end. The fold's lower bound was that same processed marker, so one unreadable
// chunk produced a window that could never be revisited:
//
//	SetProcessedIndex(31) -> processed = 31, fold of [1,31] fails at 20
//	resolveStaleStartupConfig -> scans [processed+1, staleConfigThrough]
//	                          -> [32, 31] -> low > index -> "resolved"
//	                          -> staleConfigThrough = 0, campaign allowed
//
// The replica then campaigns against the configuration its BASE described,
// having skipped every config change in the window — soliciting votes from
// replicas the shard removed, and computing quorum over the wrong denominator.
//
// The fix is to stop conflating the two facts: configFoldedThrough records what
// the FOLD covered, and only a fold that really offered the entries to
// applyMembershipEntry advances it.

// staleTailFixture builds a replica that recovered with a 3-voter BASE while its
// committed log says 4 voters: the bootstrap entry at index 1 names {1,2,3}, and
// a committed AddNode at index 31 adds replica 4. lr.failing decides whether the
// tail can be read.
func staleTailFixture(t *testing.T) (*Peer, *failingReadLogReader) {
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
	entries = append(entries, proto.Entry{
		Index: 31, Term: 1, Type: proto.EntryConfigChange,
		Cmd: marshalConfigChange(t, proto.ConfigChange{
			Type: proto.AddNode, ReplicaID: 4, Address: "a4"}),
	})
	inner := newTestLogReader(entries, proto.State{Term: 1, Commit: 31})
	inner.membership = proto.Membership{Addresses: map[uint64]string{
		1: "a1", 2: "a2", 3: "a3"}}
	lr := &failingReadLogReader{testLogReader: inner}

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if p.staleConfigThrough != 31 {
		t.Fatalf("fixture: staleConfigThrough = %d, want 31", p.staleConfigThrough)
	}
	return p, lr
}

// TestSetProcessedIndex_UnfoldedWindowStillSuppressesTheCampaign is the
// regression. The state machine reports lastApplied 31, so SetProcessedIndex
// pushes processed to 31 — but the log cannot be read, so nothing is folded. The
// replica must NOT then decide its configuration is current.
//
// It also pins the distinction the return value has to make. A window this
// replica could not READ is not a window it can never read: the commit pipeline
// or resolveStaleStartupConfig may cover it later, so it arms the watermark and
// reports no error. Only "these committed bytes will never decode" is terminal
// (TestSetProcessedIndex_ReportsACorruptCommittedConfigEntry); otherwise every
// transient log-read failure at startup would refuse to start the shard.
func TestSetProcessedIndex_UnfoldedWindowStillSuppressesTheCampaign(t *testing.T) {
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

	if p.membership.isVoter(4) {
		t.Fatal("fixture: the unreadable fold must not have added replica 4")
	}
	if p.staleConfigThrough == 0 {
		t.Fatal("SetProcessedIndex cleared the staleness watermark after a fold " +
			"that read nothing: the replica now believes its configuration is " +
			"current when it is the recovered BASE")
	}

	if targets := tickToCampaign(t, p); len(targets) != 0 {
		t.Fatalf("the replica campaigned and solicited %v while the committed "+
			"configuration tail was never folded: the voting set it computed "+
			"quorum over is the superseded 3-voter base, and the real "+
			"configuration has 4 voters", targets)
	}
	if p.r.state != follower {
		t.Fatalf("state %v: an unfolded configuration tail must not start an "+
			"election", p.r.state)
	}
}

// TestSetProcessedIndex_FoldedWindowStillAllowsTheCampaign is the POSITIVE
// CONTROL. With a readable log the identical sequence must fold the tail, clear
// the watermark, and campaign against the RESOLVED 4-voter set — otherwise the
// gate is just a permanent election block. It is also the positive control of
// TestSetProcessedIndex_ReportsACorruptCommittedConfigEntry: a readable tail
// reports no error and records no unfolded configuration, so that assertion is
// not satisfied by a method that returns an error unconditionally.
func TestSetProcessedIndex_FoldedWindowStillAllowsTheCampaign(t *testing.T) {
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
	if c := p.UnfoldedConfig(); c.Recorded() {
		t.Fatalf("UnfoldedConfig = %+v after a complete fold, want none", c)
	}

	if !p.membership.isVoter(4) {
		t.Fatal("a readable fold did not add replica 4: SetProcessedIndex must " +
			"still fold the window the state machine skipped")
	}
	if p.staleConfigThrough != 0 {
		t.Fatalf("staleConfigThrough = %d after a complete fold: the watermark "+
			"latched and this replica can never campaign", p.staleConfigThrough)
	}
	targets := tickToCampaign(t, p)
	if !slices.Equal(targets, []uint64{2, 3, 4}) {
		t.Fatalf("solicited %v, want [2 3 4]: the campaign must run against the "+
			"resolved voting set", targets)
	}
}

// TestSetProcessedIndex_RecoveryAfterTheReadFaultClears proves the suppression
// is a retry rather than a latch, on the SetProcessedIndex path specifically.
// Nothing else happens: no leader appears, no entry is applied, no caller
// intervenes. The next election timeout after the log becomes readable must
// campaign, against the resolved set.
func TestSetProcessedIndex_RecoveryAfterTheReadFaultClears(t *testing.T) {
	quietLogs(t)

	p, lr := staleTailFixture(t)
	lr.failing = true
	p.SetProcessedIndex(31)
	if targets := tickToCampaign(t, p); len(targets) != 0 {
		t.Fatalf("campaigned while unresolved, soliciting %v", targets)
	}

	lr.failing = false
	targets := tickToCampaign(t, p)
	if !slices.Equal(targets, []uint64{2, 3, 4}) {
		t.Fatalf("solicited %v, want [2 3 4]: the replica stayed suppressed after "+
			"the read fault cleared (watermark %d, folded_through %d), so the gate "+
			"latched instead of retrying",
			targets, p.staleConfigThrough, p.configFoldedThrough)
	}
}

// TestResolveStaleStartupConfig_ResumesFromTheFoldMarkerNotProcessed pins the
// lower bound directly. The processed marker is at 31 and the fold marker is at
// 30, and the on-demand resolution must read from 31 — the entry it has NOT
// folded — rather than concluding from processed that there is nothing left.
func TestResolveStaleStartupConfig_ResumesFromTheFoldMarkerNotProcessed(t *testing.T) {
	quietLogs(t)

	p, lr := staleTailFixture(t)
	// The commit pipeline folded 1..30 for free; index 31 is still outstanding,
	// while the state machine's marker has been pushed all the way to 31.
	if err := p.processConfigChanges(entriesUpTo(t, p, 30)); err != nil {
		t.Fatalf("processConfigChanges: %v", err)
	}
	p.r.log.processed = 31
	p.staleConfigThrough = 31

	lr.reads = 0
	lr.readLows = nil
	if !p.resolveStaleStartupConfig() {
		t.Fatal("resolution failed on a readable log")
	}
	if !slices.Equal(lr.readLows, []uint64{31}) {
		t.Fatalf("the fold read from %v, want [31]: resuming from the PROCESSED "+
			"marker would have read nothing at all and declared the configuration "+
			"resolved over an entry it never folded", lr.readLows)
	}
	if !p.membership.isVoter(4) {
		t.Fatal("the residual config change at index 31 was not folded")
	}
}

// entriesUpTo returns the fixture's committed entries 1..index, for driving the
// commit path directly.
func entriesUpTo(t *testing.T, p *Peer, index uint64) []proto.Entry {
	t.Helper()
	entries, err := p.r.log.entries(1, index+1, 0)
	if err != nil {
		t.Fatalf("log.entries: %v", err)
	}
	return entries
}
