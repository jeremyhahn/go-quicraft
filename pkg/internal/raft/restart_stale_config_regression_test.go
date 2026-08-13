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
	"slices"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// A restarting replica must not campaign against its BASE configuration
// ---------------------------------------------------------------------------
//
// Membership is config-in-log. A restart recovers a BASE configuration — the
// snapshot record's membership, or the founding EntryInitialMembership at index
// 1, which is the single entry Host.reconstructShardMembership reads — and
// learns every change above it by re-applying committed entries through the
// ordinary pipeline. The replica is live and TICKING throughout that replay, so
// its election timer can fire while its voting set is still the superseded one.
//
// Host.StartShard reaches this state on the most ordinary restart there is: an
// in-memory state machine on a shard that has never snapshotted. It calls
// peer.SetProcessedIndex — the only other place the tail is folded — only when
// recoveredIndex > 0, which requires either a durable on-disk marker or a
// recovered snapshot record. Neither exists here, so nothing folds the tail
// before the first Tick.
//
// The fixtures below construct exactly that peer: a committed log whose index-1
// bootstrap entry names the base voters, config changes above it, and a
// recovered membership holding only the base.

// staleRestartPeer builds a peer in the post-restart, pre-replay state: the
// recovered membership is base, the committed log is base plus tail.
func staleRestartPeer(t *testing.T, self uint64, base map[uint64]string,
	tail []proto.ConfigChange) *Peer {
	t.Helper()

	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{Addresses: base})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	entries := []proto.Entry{bootstrap}
	for i, cc := range tail {
		entries = append(entries, proto.Entry{
			Index: uint64(i) + 2,
			Term:  1,
			Type:  proto.EntryConfigChange,
			Cmd:   marshalConfigChange(t, cc),
		})
	}
	commit := entries[len(entries)-1].Index

	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: commit})
	// Exactly what recovery hands the peer: the BASE only. Host reads index 1
	// and stops; the deltas are supposed to arrive later, by replay.
	lr.membership = proto.Membership{
		Addresses: maps(base),
	}

	p, err := NewPeer(testConfigWithID(1, self), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	return p
}

// maps copies an address map so a fixture and the peer never alias one.
func maps(m map[uint64]string) map[uint64]string {
	out := make(map[uint64]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// tickToCampaign ticks until the peer leaves the follower state, and returns
// the replicas it solicited votes from. The randomized election timeout is
// electionTimeout + rand[0, electionTimeout), so 3x the timeout is ample.
func tickToCampaign(t *testing.T, p *Peer) []uint64 {
	t.Helper()
	for range 3 * p.r.electionTimeout {
		p.Tick()
		if p.r.state != follower {
			break
		}
	}
	var targets []uint64
	for _, m := range p.DrainMessages() {
		if m.Type == proto.RequestVote || m.Type == proto.RequestPreVote {
			targets = append(targets, m.To)
		}
	}
	slices.Sort(targets)
	return targets
}

// TestRestartStaleConfig_GrownConfigurationIsNotCampaignedAgainst is the
// SAFETY case: the committed tail ADDED voters, so the base quorum is a
// MINORITY of the real configuration. A replica that campaigns against the
// base wins on 2 votes out of a 5-voter shard — in a term the real
// configuration can elect a different leader in, from the disjoint remainder.
func TestRestartStaleConfig_GrownConfigurationIsNotCampaignedAgainst(t *testing.T) {
	quietLogs(t)

	p := staleRestartPeer(t, 1,
		map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		[]proto.ConfigChange{
			{Type: proto.AddNode, ReplicaID: 4, Address: "a4"},
			{Type: proto.AddNode, ReplicaID: 5, Address: "a5"},
		})

	// Fixture check: the peer really is holding the stale base right now. If
	// this ever stops being true the rest of the test proves nothing.
	if got := quorum(p.r.remotes, p.r.witnesses); got != 2 {
		t.Fatalf("fixture: expected the recovered BASE quorum of 2, got %d "+
			"(voters %v)", got, p.GetMembership().Addresses)
	}

	targets := tickToCampaign(t, p)

	if p.r.state == follower {
		t.Fatal("the replica never campaigned: resolving the configuration " +
			"must DELAY nothing on a readable log, it must fold the tail and " +
			"proceed")
	}
	if !slices.Equal(targets, []uint64{2, 3, 4, 5}) {
		t.Fatalf("campaigned against the stale BASE voting set: solicited votes "+
			"from %v, want [2 3 4 5]. Replicas 4 and 5 are voters by committed "+
			"EntryConfigChange, so a candidate that never asks them is counting "+
			"quorum over a configuration the shard has left", targets)
	}
	if got := quorum(p.r.remotes, p.r.witnesses); got != 3 {
		t.Fatalf("quorum is %d, want 3: with 5 committed voters a candidate that "+
			"needs only %d votes can be elected by a MINORITY, and the disjoint "+
			"remainder can elect a second leader in the same term", got, got)
	}

	// The decisive observable: two grants — a majority of the BASE — must not
	// be enough. This is what the bug produced and what the fix must prevent.
	term := p.r.term
	for _, from := range []uint64{2, 3} {
		if err := p.Handle(proto.Message{
			Type: preVoteRespType(p), From: from, To: 1,
			ShardID: 1, Term: term + preVoteTermBump(p), Origin: proto.OriginRemote,
		}); err != nil {
			t.Fatalf("vote response from %d: %v", from, err)
		}
	}
	if p.IsLeader() {
		t.Fatal("elected leader on 2 grants out of a 5-voter committed " +
			"configuration: Election Safety is violated, because {4,5} plus " +
			"either of {2,3} is also a majority and can elect a different " +
			"leader in this same term")
	}
}

// preVoteRespType and preVoteTermBump adapt a grant to whichever round the peer
// is in. PreVote is on by default (config.SetDefaults), and a pre-candidate
// solicits at term+1.
func preVoteRespType(p *Peer) uint64 {
	if p.r.state == preCandidate {
		return proto.RequestPreVoteResp
	}
	return proto.RequestVoteResp
}

func preVoteTermBump(p *Peer) uint64 {
	if p.r.state == preCandidate {
		return 1
	}
	return 0
}

// TestRestartStaleConfig_ShrunkConfigurationIsNotCampaignedAgainst is the
// LIVENESS case: the committed tail REMOVED voters. A replica campaigning
// against the base needs 3 of 5 votes while only 3 replicas still exist, and
// two of the replicas it solicits are gone — so a shard with a perfectly
// healthy quorum cannot elect.
func TestRestartStaleConfig_ShrunkConfigurationIsNotCampaignedAgainst(t *testing.T) {
	quietLogs(t)

	p := staleRestartPeer(t, 1,
		map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4", 5: "a5"},
		[]proto.ConfigChange{
			{Type: proto.RemoveNode, ReplicaID: 4},
			{Type: proto.RemoveNode, ReplicaID: 5},
		})

	if got := quorum(p.r.remotes, p.r.witnesses); got != 3 {
		t.Fatalf("fixture: expected the recovered BASE quorum of 3, got %d", got)
	}

	targets := tickToCampaign(t, p)

	if p.r.state == follower {
		t.Fatal("the replica never campaigned")
	}
	if !slices.Equal(targets, []uint64{2, 3}) {
		t.Fatalf("solicited votes from %v, want [2 3]: replicas 4 and 5 were "+
			"removed by committed config changes and will never answer, so a "+
			"candidate still counting them holds the shard leaderless with a "+
			"healthy quorum up", targets)
	}
	if got := quorum(p.r.remotes, p.r.witnesses); got != 2 {
		t.Fatalf("quorum is %d, want 2: computing a 5-voter majority over a "+
			"3-voter configuration is unreachable by construction", got)
	}
}

// TestRestartStaleConfig_SelfRemovedInTailDoesNotCampaign covers the gate
// ORDER. The base names this replica as a voter; the committed tail removes it.
// Resolving the configuration has to happen BEFORE the selfRemoved decision, or
// that decision is taken against a configuration the shard has left and a
// removed replica campaigns (Raft thesis 4.2.2).
func TestRestartStaleConfig_SelfRemovedInTailDoesNotCampaign(t *testing.T) {
	quietLogs(t)

	p := staleRestartPeer(t, 4,
		map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4"},
		[]proto.ConfigChange{
			{Type: proto.RemoveNode, ReplicaID: 4},
		})

	if p.r.selfRemoved {
		t.Fatal("fixture: the BASE configuration must still contain replica 4, " +
			"otherwise the pre-existing gate covers this and the ordering is " +
			"never exercised")
	}

	targets := tickToCampaign(t, p)

	if len(targets) != 0 {
		t.Fatalf("a replica the committed configuration REMOVED solicited votes "+
			"from %v. The surviving configuration computes quorum without it, so "+
			"a term it wins is a term the real configuration can also elect in",
			targets)
	}
	if p.r.state != follower {
		t.Fatalf("removed replica reached state %v instead of staying a follower",
			p.r.state)
	}
	if !p.r.selfRemoved {
		t.Fatal("the committed RemoveNode for self was never folded in: the " +
			"campaign gate resolved nothing")
	}
	if p.r.leaderID != noLeader {
		t.Fatalf("leaderID is still %d on a replica that refused to campaign: "+
			"the shared leader-lease test stays true and this replica denies "+
			"PreVotes and drops higher-term RequestVotes", p.r.leaderID)
	}
}

// TestRestartStaleConfig_TimeoutNowResolvesBeforeCampaigning covers the OTHER
// campaign entry point. TimeoutNow bypasses startElection and bypasses PreVote,
// so a stale voting set there is decided in a single round.
func TestRestartStaleConfig_TimeoutNowResolvesBeforeCampaigning(t *testing.T) {
	quietLogs(t)

	p := staleRestartPeer(t, 1,
		map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		[]proto.ConfigChange{
			{Type: proto.AddNode, ReplicaID: 4, Address: "a4"},
			{Type: proto.AddNode, ReplicaID: 5, Address: "a5"},
		})

	if err := p.Handle(proto.Message{
		Type: proto.TimeoutNow, From: 2, To: 1, ShardID: 1,
		Term: p.r.term, Origin: proto.OriginRemote,
	}); err != nil {
		t.Fatalf("TimeoutNow: %v", err)
	}

	var targets []uint64
	for _, m := range p.DrainMessages() {
		if m.Type == proto.RequestVote {
			targets = append(targets, m.To)
		}
	}
	slices.Sort(targets)

	if p.r.state != candidate {
		t.Fatalf("TimeoutNow did not start an election: state %v", p.r.state)
	}
	if !slices.Equal(targets, []uint64{2, 3, 4, 5}) {
		t.Fatalf("TimeoutNow campaigned against the stale BASE set: solicited "+
			"%v, want [2 3 4 5]", targets)
	}
	if got := quorum(p.r.remotes, p.r.witnesses); got != 3 {
		t.Fatalf("quorum %d, want 3", got)
	}
}

// ---------------------------------------------------------------------------
// The gate must never suppress an election it has no reason to
// ---------------------------------------------------------------------------
//
// A previous attempt at this bug gated campaigning on a replay FLOOR and was
// reverted because it suppressed elections outright. The tests below are the
// ones that attempt would have failed: each is a replica that has every right
// to campaign, and each carries a non-zero recovered commit index, which is the
// condition that arms the watermark.

// TestRestartStaleConfig_SingleMemberRestartStillElectsItself is the sharpest
// of them. A one-voter shard has nobody to hear from, so if the gate depends on
// ANYTHING external — an apply pipeline, a peer, a caller invoking
// SetProcessedIndex — the shard is dead for the life of the process.
func TestRestartStaleConfig_SingleMemberRestartStillElectsItself(t *testing.T) {
	quietLogs(t)

	bootstrap, err := BuildInitialMembershipEntry(
		proto.Membership{Addresses: map[uint64]string{1: "a1"}})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	entries := []proto.Entry{bootstrap}
	for i := uint64(2); i <= 40; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1, Cmd: []byte("x")})
	}
	lr := newTestLogReader(entries, proto.State{Term: 1, Commit: 40})
	lr.membership = proto.Membership{Addresses: map[uint64]string{1: "a1"}}

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	// Nothing here advances the processed marker: no engine, no state machine,
	// no Commit. The gate has to resolve itself from the local log alone.
	if p.staleConfigThrough == 0 {
		t.Fatal("fixture: the watermark must be ARMED (commit 40 above a base " +
			"at ConfigChangeID 0), or this test does not exercise the gate")
	}

	for range 3 * p.r.electionTimeout {
		p.Tick()
		if p.IsLeader() {
			break
		}
	}
	if !p.IsLeader() {
		t.Fatalf("a single-member shard could not elect itself after a restart "+
			"(state %v, processed %d, committed %d, watermark %d). Nothing else "+
			"can ever elect it: gating a campaign on apply progress, or on a "+
			"caller that may never run, takes the shard down permanently",
			p.r.state, p.r.log.processed, p.r.log.committed, p.staleConfigThrough)
	}
}

// TestRestartStaleConfig_RestartWithNoConfigTailStillCampaigns is the common
// case: a long committed tail of ordinary data entries and no membership change
// anywhere in it. Resolution must complete and the replica must campaign
// normally, without waiting for the state machine to catch up.
func TestRestartStaleConfig_RestartWithNoConfigTailStillCampaigns(t *testing.T) {
	quietLogs(t)

	p := staleRestartPeer(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"}, nil)
	// Pad the committed log with plain data entries above the bootstrap entry.
	lr := p.logReader.(*testLogReader)
	for i := uint64(2); i <= 50; i++ {
		lr.entries = append(lr.entries, proto.Entry{Index: i, Term: 1, Cmd: []byte("x")})
	}
	lr.state.Commit = 50
	p2, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	if p2.staleConfigThrough == 0 {
		t.Fatal("fixture: the watermark must be armed")
	}

	targets := tickToCampaign(t, p2)
	if p2.r.state == follower {
		t.Fatalf("a replica whose committed tail holds no config change at all "+
			"was kept from campaigning (watermark %d, processed %d)",
			p2.staleConfigThrough, p2.r.log.processed)
	}
	if !slices.Equal(targets, []uint64{2, 3}) {
		t.Fatalf("solicited %v, want [2 3]", targets)
	}
}

// TestRestartStaleConfig_CommitPipelineResolvesWithoutReadingBack asserts the
// free path: when the ordinary commit pipeline delivers past the watermark, the
// configuration is resolved by that alone. This is what keeps a restart into a
// healthy shard from ever paying for the on-demand read.
func TestRestartStaleConfig_CommitPipelineResolvesWithoutReadingBack(t *testing.T) {
	quietLogs(t)

	p := staleRestartPeer(t, 1,
		map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		[]proto.ConfigChange{{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}})
	if p.staleConfigThrough != 2 {
		t.Fatalf("fixture: watermark %d, want 2", p.staleConfigThrough)
	}

	lr := p.logReader.(*testLogReader)
	p.processConfigChanges(lr.entries)

	if p.staleConfigThrough != 0 {
		t.Fatalf("the commit pipeline delivered every entry through index %d "+
			"and the watermark is still %d: every later election would re-read "+
			"the whole committed tail off disk",
			lr.entries[len(lr.entries)-1].Index, p.staleConfigThrough)
	}
	if !p.membership.isVoter(4) {
		t.Fatal("fixture: the commit path must have folded AddNode(4)")
	}
}

// TestRestartStaleConfig_BareRaftIsUngated pins the property that keeps the
// adversarial harness and every state-machine unit test out of the gate's way.
// A raft built without a Peer owns no membership, has no recovered base to be
// stale against and no tail to fold, so the correct answer there is "no gate" —
// not "assume stale", which is the shape that suppresses elections outright.
func TestRestartStaleConfig_BareRaftIsUngated(t *testing.T) {
	quietLogs(t)

	r, _ := newAdversarialRaft(t)
	if r.resolveStaleConfig != nil {
		t.Fatal("a bare raft must not carry a resolver: only NewPeer installs " +
			"one, alongside the watermark it reads")
	}
	if !r.configCurrentForCampaign() {
		t.Fatal("a bare raft reported its configuration stale, which would " +
			"suppress every election in the fuzz harness and in every raft-level " +
			"test")
	}
	electSelf(r)
	if r.state != leader {
		t.Fatalf("a bare raft could not elect itself: state %v", r.state)
	}
}

// ---------------------------------------------------------------------------
// An unreadable log suppresses one timeout, not the replica
// ---------------------------------------------------------------------------

// failingReadLogReader fails GetRange while failing is set, so the config tail
// cannot be folded. It models a transient LogDB read fault.
type failingReadLogReader struct {
	*testLogReader
	failing bool
	reads   int
	// readLows records the low bound of every GetRange, which is how a test
	// sees WHICH part of the log a fold actually pulled off disk.
	readLows []uint64
}

func (lr *failingReadLogReader) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	lr.reads++
	lr.readLows = append(lr.readLows, low)
	if lr.failing {
		return nil, errors.New("logdb: simulated read fault")
	}
	return lr.testLogReader.GetRange(low, high, maxSize)
}

// TestRestartStaleConfig_UnreadableTailSuppressesOnlyWhileUnreadable proves the
// refusal is a retry, not a latch. An unread window may hold config changes, so
// campaigning through it is exactly the bug; but the replica must campaign the
// moment the log becomes readable again, with no other event required.
func TestRestartStaleConfig_UnreadableTailSuppressesOnlyWhileUnreadable(t *testing.T) {
	quietLogs(t)

	base := map[uint64]string{1: "a1", 2: "a2", 3: "a3"}
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{Addresses: base})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	entries := []proto.Entry{bootstrap, {
		Index: 2, Term: 1, Type: proto.EntryConfigChange,
		Cmd: marshalConfigChange(t, proto.ConfigChange{
			Type: proto.AddNode, ReplicaID: 4, Address: "a4"}),
	}}
	inner := newTestLogReader(entries, proto.State{Term: 1, Commit: 2})
	inner.membership = proto.Membership{Addresses: maps(base)}
	lr := &failingReadLogReader{testLogReader: inner, failing: true}

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}

	if targets := tickToCampaign(t, p); len(targets) != 0 {
		t.Fatalf("solicited votes from %v while the committed configuration "+
			"tail was unreadable: an unread window may add or remove voters, so "+
			"the voting set the campaign ran against is unverified", targets)
	}
	if p.r.state != follower {
		t.Fatalf("state %v: an unresolvable configuration must not start an "+
			"election", p.r.state)
	}
	if p.r.leaderID != noLeader {
		t.Fatalf("leaderID %d survived a refused campaign", p.r.leaderID)
	}
	if lr.reads == 0 {
		t.Fatal("fixture: the fold never attempted a read")
	}

	// The fault clears. Nothing else changes: no leader appears, no entry is
	// applied, no caller intervenes. The very next election timeout must
	// campaign, against the RESOLVED configuration.
	lr.failing = false
	targets := tickToCampaign(t, p)
	if p.r.state == follower {
		t.Fatalf("the replica stayed suppressed after the read fault cleared "+
			"(watermark %d): the gate latched instead of retrying",
			p.staleConfigThrough)
	}
	if !slices.Equal(targets, []uint64{2, 3, 4}) {
		t.Fatalf("solicited %v, want [2 3 4]: the recovered tail added replica "+
			"4 and the campaign must run against the resolved set", targets)
	}
	if p.staleConfigThrough != 0 {
		t.Fatalf("watermark %d after a successful fold", p.staleConfigThrough)
	}
}

// TestRestartStaleConfig_ResolutionStartsAbovePlainProcessedMarker pins the
// lower bound the on-demand fold uses. Everything at or below the processed
// marker has already been offered to applyMembershipEntry, so re-reading it
// buys nothing — and re-reading the whole committed log on the step worker is
// the cost that got the previous full-log reconstruction removed.
func TestRestartStaleConfig_ResolutionStartsAbovePlainProcessedMarker(t *testing.T) {
	quietLogs(t)

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
	inner.membership = proto.Membership{Addresses: maps(base)}
	lr := &failingReadLogReader{testLogReader: inner}

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	// The commit pipeline has folded 1..30 already; only index 31 is left.
	p.processConfigChanges(entries[:30])
	p.r.log.processed = 30
	p.staleConfigThrough = 31

	lr.reads = 0
	lr.readLows = nil
	if !p.resolveStaleStartupConfig() {
		t.Fatal("resolution failed on a readable log")
	}
	if !p.membership.isVoter(4) {
		t.Fatal("the residual config change at index 31 was not folded")
	}
	if !slices.Equal(lr.readLows, []uint64{31}) {
		t.Fatalf("the fold read from %v, want [31]: indices 1..30 were already "+
			"offered to applyMembershipEntry by the commit pipeline, so pulling "+
			"them off disk again buys nothing. Re-reading the whole committed "+
			"tail synchronously is the cost that got the full-log membership "+
			"reconstruction removed, and this fold runs on the step worker",
			lr.readLows)
	}
}
