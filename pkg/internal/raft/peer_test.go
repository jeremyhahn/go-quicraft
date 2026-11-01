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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// mustHandle is a test helper that steps a message and fails on error.
func mustHandle(t *testing.T, p *Peer, msg proto.Message) {
	t.Helper()
	if err := p.Handle(msg); err != nil {
		t.Fatalf("unexpected error from Handle(%s): %v",
			proto.MessageTypeName(msg.Type), err)
	}
}

// mustCommit is a test helper that calls Commit and fails on error.
func mustCommit(t *testing.T, p *Peer, update proto.Update) {
	t.Helper()
	if err := p.Commit(update); err != nil {
		t.Fatalf("unexpected error from Commit: %v", err)
	}
}

// runUpdateCycles drives the GetUpdate→Commit contract until the peer
// reports no pending update, mirroring the engine's step/commit worker
// loop. Commit fires the persistence-edge commit re-evaluation, so a
// proposal takes one cycle to persist its entries and a second cycle to
// surface them as committed entries. Returns every committed entry
// observed across the cycles so callers can assert on what reached the
// apply stage.
func runUpdateCycles(t *testing.T, p *Peer) []proto.Entry {
	t.Helper()
	var committed []proto.Entry
	for range 10 {
		if !p.HasUpdate(0) {
			return committed
		}
		update, err := p.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("failed to get update: %v", err)
		}
		committed = append(committed, update.CommittedEntries...)
		mustCommit(t, p, update)
	}
	t.Fatal("peer did not settle within 10 update cycles")
	return nil
}

// testConfig returns a valid Config for testing.
func testConfig() config.Config {
	cfg := config.Config{
		ShardID:   1,
		ReplicaID: 1,
	}
	cfg.SetDefaults()
	return cfg
}

// testConfigWithID returns a valid Config with the given shard and replica IDs.
func testConfigWithID(shardID, replicaID uint64) config.Config {
	cfg := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	cfg.SetDefaults()
	return cfg
}

// newTestPeer creates a Peer with a test log reader and initial state.
func newTestPeer(t *testing.T, cfg config.Config, state proto.State, ms proto.Membership) *Peer {
	t.Helper()
	lr := newTestLogReader(nil, state)
	lr.membership = ms
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}
	setTestLeaseClock(p.r)
	return p
}

// singleNodeMembership returns a membership with a single voting member.
func singleNodeMembership() proto.Membership {
	return proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
		},
	}
}

// threeNodeMembership returns a membership with three voting members.
func threeNodeMembership() proto.Membership {
	return proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
			3: "addr3",
		},
	}
}

// electSingleNodeLeader ticks a single-node peer until it becomes leader.
// It returns the peer in leader state. The peer must be configured as a
// single-node cluster (only itself in membership).
func electSingleNodeLeader(t *testing.T, p *Peer) {
	t.Helper()
	// A single-node cluster elects itself after the election timeout.
	// The randomized timeout is electionTimeout + rand [0, electionTimeout).
	// With ElectionRTT=10, worst case is 19 ticks. Tick 30 to be safe.
	for range 30 {
		p.Tick()
		if p.IsLeader() {
			return
		}
	}
	t.Fatal("single-node peer did not elect itself as leader")
}

// newTestLeaderPeer creates a single-node Peer that has self-elected
// as leader through the full raft election protocol.
func newTestLeaderPeer(t *testing.T) *Peer {
	t.Helper()
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)
	electSingleNodeLeader(t, p)

	// Drain the no-op entry and election messages from the initial
	// leader election so tests start with a clean state. The first
	// Commit persists the no-op, which commits it at the persistence
	// edge; further cycles apply it and settle the peer.
	runUpdateCycles(t, p)
	return p
}

// newTestMultiNodeLeaderPeer creates a 3-node Peer (replica 1) that has
// self-elected as leader. Used for tests that need remote tracking.
func newTestMultiNodeLeaderPeer(t *testing.T) *Peer {
	t.Helper()
	cfg := testConfig()
	// Disable PreVote so the candidate directly increments term.
	cfg.PreVote = config.BoolPtr(false)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past the election timeout to trigger an election.
	for range 30 {
		p.Tick()
	}

	// The candidate needs votes from peers. Simulate vote grants.
	// After ticking, raft will have sent RequestVote messages. We
	// need to step VoteResp messages from a quorum.
	p.Handle(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: p.r.term,
	})
	p.Handle(proto.Message{
		Type: proto.RequestVoteResp,
		From: 3,
		Term: p.r.term,
	})

	if !p.IsLeader() {
		t.Fatal("3-node peer did not become leader after vote grants")
	}

	// Commit the no-op entry from becomeLeader by simulating a
	// follower acknowledgement. This is required so that
	// hasCommittedEntryAtCurrentTerm() returns true for ReadIndex.
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: p.r.log.lastIndex(),
	})

	// Drain initial state. Commit persists the no-op, completing the
	// durable quorum with the follower ack above; further cycles apply
	// the no-op and settle the peer.
	runUpdateCycles(t, p)
	return p
}

// --- NewPeer ---

func TestPeer_NewPeer_Success(t *testing.T) {
	cfg := testConfig()
	state := proto.State{Term: 5, Vote: 2, Commit: 0}
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, state, ms)

	if p.shardID != cfg.ShardID {
		t.Fatalf("expected shardID %d, got %d", cfg.ShardID, p.shardID)
	}
	if p.replicaID != cfg.ReplicaID {
		t.Fatalf("expected replicaID %d, got %d", cfg.ReplicaID, p.replicaID)
	}
	// The raft state machine restores term from persisted state.
	if p.r.term != 5 {
		t.Fatalf("expected term 5, got %d", p.r.term)
	}
	// The persisted vote is restored too, and is NOT reset. becomeFollower
	// clears the vote only on term ADVANCEMENT (Raft Figure 2), and newRaft
	// calls it with the term it just restored, so the term does not change and
	// the vote survives. Preserving it is what stops a replica that restarts
	// inside a term from voting a second time in that term. See
	// persisted_vote_regression_test.go for the double-vote scenario.
	if p.r.vote != 2 {
		t.Fatalf("expected persisted vote 2 to be restored, got %d", p.r.vote)
	}
}

func TestPeer_NewPeer_InvalidConfig(t *testing.T) {
	cfg := config.Config{} // missing required fields
	lr := newTestLogReader(nil, proto.State{})
	_, err := NewPeer(cfg, lr, nil)
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestPeer_NewPeer_InitializesRaftStateMachine(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Verify the raft state machine is initialized as follower.
	if p.r.state != follower {
		t.Fatalf("expected follower state, got %s", p.r.state)
	}
	if p.r.shardID != cfg.ShardID {
		t.Fatalf("expected raft shardID %d, got %d", cfg.ShardID, p.r.shardID)
	}
	if p.r.replicaID != cfg.ReplicaID {
		t.Fatalf("expected raft replicaID %d, got %d", cfg.ReplicaID, p.r.replicaID)
	}
}

// --- Tick ---

func TestPeer_Tick_TriggersElection_SingleNode(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Initially not leader.
	if p.IsLeader() {
		t.Fatal("expected not to be leader initially")
	}

	// Tick past the election timeout. Single-node cluster self-elects.
	electSingleNodeLeader(t, p)

	if !p.IsLeader() {
		t.Fatal("expected to be leader after election timeout")
	}
	if p.LeaderID() != cfg.ReplicaID {
		t.Fatalf("expected leaderID %d, got %d", cfg.ReplicaID, p.LeaderID())
	}
}

func TestPeer_Tick_StartsAtZeroElectionTick(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// The raft state machine starts with electionTick=0.
	if p.r.electionTick != 0 {
		t.Fatalf("expected initial electionTick 0, got %d", p.r.electionTick)
	}
}

func TestPeer_Tick_AdvancesElectionTick(t *testing.T) {
	cfg := testConfig()
	// Use a 3-node cluster so election doesn't complete in one tick.
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	p.Tick()
	if p.r.electionTick < 1 {
		t.Fatalf("expected electionTick >= 1 after tick, got %d", p.r.electionTick)
	}
}

// --- Handle ---

func TestPeer_Handle_Replicate_Success(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Seed the log with an initial entry so tryAppend can match.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1, // prevLogIndex
		LogTerm:  1, // prevLogTerm
		Commit:   0,
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Cmd: []byte("hello")},
		},
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have response message(s) in raft's outbound buffer.
	if len(p.r.msgs) == 0 {
		t.Fatal("expected at least 1 message")
	}

	// Find the ReplicateResp message.
	var found bool
	for _, m := range p.r.msgs {
		if m.Type == proto.ReplicateResp && !m.Reject {
			found = true
			if m.LogIndex != 2 {
				t.Fatalf("expected LogIndex 2, got %d", m.LogIndex)
			}
		}
	}
	if !found {
		t.Fatal("expected non-reject ReplicateResp message")
	}
}

func TestPeer_Handle_Replicate_Reject(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Seed with entry at index 1, term 1.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	// Send a message with wrong prevLogTerm.
	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  99, // does not match
		Entries: []proto.Entry{
			{Index: 2, Term: 1},
		},
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find reject response.
	var found bool
	for _, m := range p.r.msgs {
		if m.Type == proto.ReplicateResp && m.Reject {
			found = true
		}
	}
	if !found {
		t.Fatal("expected reject ReplicateResp")
	}
}

func TestPeer_Handle_Replicate_AdvancesCommit(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Seed log with entries.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})

	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 3,
		LogTerm:  1,
		Commit:   3,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.r.log.committed != 3 {
		t.Fatalf("expected committed 3, got %d", p.r.log.committed)
	}
}

func TestPeer_Handle_Heartbeat_Success(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Seed log so commit can advance.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})

	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    1,
		Commit:  2,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find HeartbeatResp.
	var found bool
	for _, m := range p.r.msgs {
		if m.Type == proto.HeartbeatResp {
			found = true
		}
	}
	if !found {
		t.Fatal("expected HeartbeatResp message")
	}
	if p.r.log.committed != 2 {
		t.Fatalf("expected committed 2, got %d", p.r.log.committed)
	}
}

func TestPeer_Handle_Heartbeat_CommitBeyondLog(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Log has entries up to index 2.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})

	// Heartbeat with commit beyond log; should clamp to lastIndex.
	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    1,
		Commit:  100,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Commit should be clamped to lastIndex (2).
	if p.r.log.committed != 2 {
		t.Fatalf("expected committed 2 (clamped), got %d", p.r.log.committed)
	}
}

func TestPeer_Handle_HigherTerm_ResetsState(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{Term: 1, Vote: 1}, singleNodeMembership())

	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    5,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.r.term != 5 {
		t.Fatalf("expected term 5, got %d", p.r.term)
	}
	// becomeFollower resets vote to 0.
	if p.r.vote != 0 {
		t.Fatalf("expected vote 0 after term change, got %d", p.r.vote)
	}
}

func TestPeer_Handle_TracksLeaderFromReplicate(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})

	msg := proto.Message{
		Type:     proto.Replicate,
		From:     5,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// syncLeaderID copies raft's leaderID to the atomic field.
	if p.LeaderID() != 5 {
		t.Fatalf("expected leaderID 5, got %d", p.LeaderID())
	}
}

func TestPeer_Handle_TracksLeaderFromHeartbeat(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	msg := proto.Message{
		Type:    proto.Heartbeat,
		From:    7,
		To:      1,
		ShardID: 1,
		Term:    1,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.LeaderID() != 7 {
		t.Fatalf("expected leaderID 7, got %d", p.LeaderID())
	}
}

func TestPeer_Handle_InstallSnapshot_Success(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	snap := proto.Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     10,
		Term:      2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{
				1: "addr1",
				2: "addr2",
			},
		},
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     2,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.r.log.committed != 10 {
		t.Fatalf("expected committed 10, got %d", p.r.log.committed)
	}
}

func TestPeer_Handle_InstallSnapshot_BehindCommit(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Advance committed to 20.
	p.r.log.inmem.merge(makeEntries(1, 21, 1))
	p.r.log.committed = 20

	snap := proto.Snapshot{
		Index: 5,
		Term:  1,
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should reject and respond with our committed index.
	var found bool
	for _, m := range p.r.msgs {
		if m.Type == proto.ReplicateResp && m.LogIndex == 20 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected ReplicateResp with LogIndex=20 for rejected snapshot")
	}
}

func TestPeer_Handle_InstallSnapshot_EmptySnapshot(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    1,
		// Empty snapshot (Index=0, Term=0).
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPeer_Handle_RequestVote_Processed(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	msg := proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 0,
		LogTerm:  0,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The raft state machine should have produced a RequestVoteResp.
	var found bool
	for _, m := range p.r.msgs {
		if m.Type == proto.RequestVoteResp {
			found = true
		}
	}
	if !found {
		t.Fatal("expected RequestVoteResp message from raft")
	}
}

// --- Propose ---

func TestPeer_Propose_Success(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Record the lastIndex before proposing.
	lastBefore := p.r.log.lastIndex()

	entries := []proto.Entry{
		{Cmd: []byte("cmd1")},
		{Cmd: []byte("cmd2")},
	}
	lastIndex, err := p.Propose(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Returned lastIndex should match the log's lastIndex.
	if lastIndex != p.r.log.lastIndex() {
		t.Fatalf("expected lastIndex %d, got %d", p.r.log.lastIndex(), lastIndex)
	}

	// Two new entries should have been appended.
	lastAfter := p.r.log.lastIndex()
	if lastAfter != lastBefore+2 {
		t.Fatalf("expected lastIndex %d, got %d", lastBefore+2, lastAfter)
	}

	// Verify the entries have the leader's term.
	e, err := getEntryForTest(&p.r.log.inmem, lastBefore+1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Term != p.r.term {
		t.Fatalf("expected term %d, got %d", p.r.term, e.Term)
	}
}

func TestPeer_Propose_NotLeader(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)
	// Peer is a follower with no known leader.

	entries := []proto.Entry{{Cmd: []byte("cmd1")}}
	_, err := p.Propose(entries)
	if err == nil {
		t.Fatal("expected error for non-leader propose")
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %T", err)
	}
}

func TestPeer_Propose_EmptyEntries(t *testing.T) {
	p := newTestLeaderPeer(t)

	lastIndex, err := p.Propose(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lastIndex != 0 {
		t.Fatalf("expected lastIndex 0 for empty propose, got %d", lastIndex)
	}
}

func TestPeer_Propose_SingleNodeAutoCommits(t *testing.T) {
	p := newTestLeaderPeer(t)

	entries := []proto.Entry{
		{Cmd: []byte("cmd1")},
	}
	_, err := p.Propose(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// In a single-node cluster, quorum == 1 and the leader's own match
	// is its durable index, so the entry commits at the persistence
	// edge: Commit acknowledges local durability and re-runs tryCommit.
	runUpdateCycles(t, p)
	if p.r.log.committed != p.r.log.lastIndex() {
		t.Fatalf("expected committed == lastIndex (%d), got committed=%d",
			p.r.log.lastIndex(), p.r.log.committed)
	}
}

// --- ProposeConfigChange ---

func TestPeer_ProposeConfigChange_Success(t *testing.T) {
	p := newTestLeaderPeer(t)

	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Membership takes effect at commit time; drive the update cycles so
	// the single-node leader persists and commits the entry.
	runUpdateCycles(t, p)
	if !p.membership.isVoter(4) {
		t.Fatal("expected replica 4 to be voter after config change committed")
	}
}

func TestPeer_ProposeConfigChange_NotLeader(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc)
	if err == nil {
		t.Fatal("expected error for non-leader config change")
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %T", err)
	}
}

func TestPeer_ProposeConfigChange_InvalidChange(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Try adding a node that already exists.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 1,
		Address:   "addr1",
	}
	err := p.ProposeConfigChange(cc)
	if !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("expected ErrNodeAlreadyExists, got %v", err)
	}
}

func TestPeer_ProposeConfigChange_Serialization(t *testing.T) {
	p := newTestLeaderPeer(t)

	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the entry Cmd can be deserialized back to the config change.
	lastIdx := p.r.log.lastIndex()
	e, err := getEntryForTest(&p.r.log.inmem, lastIdx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var decoded proto.ConfigChange
	_, unmarshalErr := decoded.UnmarshalFrom(e.Cmd)
	if unmarshalErr != nil {
		t.Fatalf("failed to unmarshal config change: %v", unmarshalErr)
	}
	if decoded.ReplicaID != 4 {
		t.Fatalf("expected ReplicaID 4, got %d", decoded.ReplicaID)
	}
	if decoded.Address != "addr4" {
		t.Fatalf("expected address addr4, got %s", decoded.Address)
	}
}

// --- ReadIndex ---

func TestPeer_ReadIndex_Success_SingleNode(t *testing.T) {
	p := newTestLeaderPeer(t)

	err := p.ReadIndex(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// For a single-node cluster, the read index should be immediately
	// available in readStates.
	if len(p.r.readStates) != 1 {
		t.Fatalf("expected 1 read state, got %d", len(p.r.readStates))
	}
	if p.r.readStates[0].Key != 42 {
		t.Fatalf("expected key 42, got %d", p.r.readStates[0].Key)
	}
}

func TestPeer_ReadIndex_NotLeader(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	err := p.ReadIndex(42)
	if err == nil {
		t.Fatal("expected error for non-leader read index")
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %T", err)
	}
}

// --- RequestLeaderTransfer ---

func TestPeer_RequestLeaderTransfer_ProducesMessages(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	p.RequestLeaderTransfer(2)

	// The raft state machine should have produced messages related
	// to the transfer (either TimeoutNow if caught up, or Replicate
	// to fast-track catch-up).
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after leader transfer")
	}
}

func TestPeer_RequestLeaderTransfer_NotLeader(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	p.RequestLeaderTransfer(2)

	// Should be a no-op since we're not the leader.
	if len(p.r.msgs) != 0 {
		t.Fatalf("expected no messages from non-leader, got %d", len(p.r.msgs))
	}
}

func TestPeer_RequestLeaderTransfer_ToSelf(t *testing.T) {
	p := newTestLeaderPeer(t)

	p.RequestLeaderTransfer(1) // self

	if len(p.r.msgs) != 0 {
		t.Fatalf("expected no messages for self-transfer, got %d", len(p.r.msgs))
	}
}

// --- HasUpdate ---

func TestPeer_HasUpdate_StateChanged(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	// Manually bump term to simulate state change.
	p.r.term = 5

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true when state changed")
	}
}

func TestPeer_HasUpdate_NoChanges(t *testing.T) {
	p := newTestLeaderPeer(t)

	// After draining in newTestLeaderPeer, HasUpdate should be false.
	if p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be false with no changes")
	}
}

func TestPeer_HasUpdate_PendingMessages(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.msgs = append(p.r.msgs, proto.Message{Type: proto.Heartbeat})

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true with pending messages")
	}
}

func TestPeer_HasUpdate_CommittedEntries(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})
	p.r.log.committed = 1

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true with committed entries to apply")
	}
}

func TestPeer_HasUpdate_ReadStates(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.readStates = append(p.r.readStates, proto.ReadyToRead{Key: 1, Index: 1})

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true with read states")
	}
}

func TestPeer_HasUpdate_PendingSnapshot(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.hasPendingSnapshot = true

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true with pending snapshot")
	}
}

func TestPeer_HasUpdate_EntriesToSave(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	// Add entries that haven't been saved yet.
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true with entries to save")
	}
}

// TestPeer_RemoteProgress_ReportsWhichFollowerIsBehind pins the structured
// answer to "which replica is lagging, and why".
//
// Before this existed the information was reachable only by parsing the
// DiagRemotes debug string, which made it available to a human reading a log
// line and to nothing else — not to ShardInfo, not to an operator dashboard,
// not to a pre-upgrade check asking whether it is safe to take another replica
// down.
func TestPeer_RemoteProgress_ReportsWhichFollowerIsBehind(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, threeNodeMembership())
	p.r.becomeCandidate()
	p.r.becomeLeader()

	// A follower cannot be its own leader, so only 2 and 3 are tracked.
	// Advance replica 2 and leave replica 3 behind.
	p.r.remotes[2].match = 40
	p.r.remotes[2].next = 41
	p.r.remotes[3].match = 5
	p.r.remotes[3].next = 6

	progress := p.RemoteProgress()
	if len(progress) != 2 {
		t.Fatalf("RemoteProgress() returned %d entries, want 2 (replicas 2 and 3): %+v",
			len(progress), progress)
	}

	// Deterministic order matters: map iteration would reshuffle this on every
	// call, which makes successive samples impossible to diff.
	if progress[0].ReplicaID != 2 || progress[1].ReplicaID != 3 {
		t.Fatalf("RemoteProgress() is not ordered by replica ID: got %d then %d",
			progress[0].ReplicaID, progress[1].ReplicaID)
	}

	if progress[0].Match != 40 || progress[1].Match != 5 {
		t.Errorf("match indexes not reported: got r2=%d r3=%d, want 40 and 5",
			progress[0].Match, progress[1].Match)
	}
	if progress[1].Match >= progress[0].Match {
		t.Errorf("the lagging replica is not distinguishable from the caught-up "+
			"one: r2 match=%d, r3 match=%d", progress[0].Match, progress[1].Match)
	}
	for i := range progress {
		if progress[i].State == "" || progress[i].State == "unknown" {
			t.Errorf("replica %d has no replication state, so a follower stuck "+
				"in probe cannot be told from one merely behind",
				progress[i].ReplicaID)
		}
	}
}

// TestPeer_RemoteProgress_ReportsObserversAndWitnessesWithRole pins that the
// snapshot covers every remote the leader tracks, not just voters, and tags
// each with its role so voter-only callers can filter. An embedder promoting a
// learner needs the observer's match index to know it has caught up.
func TestPeer_RemoteProgress_ReportsObserversAndWitnessesWithRole(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, threeNodeMembership())
	p.r.becomeCandidate()
	p.r.becomeLeader()

	p.r.observers[4] = newRemote(1, maxInflightMsgs)
	p.r.observers[4].match = 33
	p.r.witnesses[5] = newRemote(1, maxInflightMsgs)
	p.r.witnesses[5].match = 22
	p.r.remotes[2].match = 11

	progress := p.RemoteProgress()
	if len(progress) != 4 {
		t.Fatalf("RemoteProgress() returned %d entries, want 4 (voters 2,3, observer 4, witness 5): %+v",
			len(progress), progress)
	}
	want := map[uint64]struct {
		role  Role
		match uint64
	}{
		2: {RoleVoter, 11},
		3: {RoleVoter, 0},
		4: {RoleObserver, 33},
		5: {RoleWitness, 22},
	}
	for i := range progress {
		w, ok := want[progress[i].ReplicaID]
		if !ok {
			t.Fatalf("unexpected replica %d in %+v", progress[i].ReplicaID, progress)
		}
		if progress[i].Role != w.role || progress[i].Match != w.match {
			t.Errorf("replica %d: role=%q match=%d, want role=%q match=%d",
				progress[i].ReplicaID, progress[i].Role, progress[i].Match, w.role, w.match)
		}
		if progress[i].IsObserver != (w.role == RoleObserver) ||
			progress[i].IsWitness != (w.role == RoleWitness) {
			t.Errorf("replica %d: IsObserver/IsWitness disagree with role %q",
				progress[i].ReplicaID, w.role)
		}
	}
	if progress[0].ReplicaID != 2 || progress[3].ReplicaID != 5 {
		t.Errorf("not ordered by replica ID: %+v", progress)
	}
}

// TestPeer_RemoteProgress_NilOnFollower pins that a non-leader reports nothing
// rather than a stale or zero-valued view: only a leader tracks remote
// progress, and an empty slice would read as "all followers at index 0".
func TestPeer_RemoteProgress_NilOnFollower(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, threeNodeMembership())
	if got := p.RemoteProgress(); got != nil {
		t.Fatalf("RemoteProgress() on a follower = %+v, want nil", got)
	}
	if s := p.DiagRemotes(); s != "not leader" {
		t.Errorf("DiagRemotes() on a follower = %q, want %q", s, "not leader")
	}
}

// --- GetUpdate ---

// TestPeer_LostUpdate_ForcesHardStateBackThroughTheWAL pins the recovery from
// an update that was extracted but never persisted.
//
// GetUpdate advances prevState when it hands an update out, not when the engine
// writes it. That is an optimistic claim, and when it is wrong — the step
// worker panics between extraction and SaveState — prevState asserts a term and
// vote are on disk that never got there. Because ReadOnly is computed from
// exactly that diff, the NEXT update is then classified ReadOnly and skips the
// WAL too, so the hard state is never written at all: the node answers
// RequestVote from memory, restarts with no record of the grant, and can grant
// the same term to a second candidate.
//
// GetPiggybackUpdate was already fixed for this by not advancing prevState (see
// NotePiggybackPersisted); the main extraction path was not.
func TestPeer_LostUpdate_ForcesHardStateBackThroughTheWAL(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, threeNodeMembership())

	// Grant a vote, so there is hard state whose durability matters.
	if err := p.Handle(proto.Message{
		Type: proto.RequestVote, From: 2, To: 1, Term: 5, LogIndex: 0, LogTerm: 0,
	}); err != nil {
		t.Fatalf("Handle(RequestVote): %v", err)
	}

	lost, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if lost.ReadOnly {
		t.Fatal("the update carrying the vote grant was ReadOnly, so it would " +
			"never have been persisted in the first place")
	}
	if lost.State.Term != 5 || lost.State.Vote != 2 {
		t.Fatalf("expected the grant in the update, got term=%d vote=%d",
			lost.State.Term, lost.State.Vote)
	}

	// The engine never persists `lost` — a panic destroyed it. This is the
	// engine telling the peer so.
	p.InvalidatePersistedState()

	// The shard must be woken: on an idle follower that has just granted a
	// vote, nothing else is going to move term, vote or commit.
	if !p.hasStateChanged() {
		t.Error("hasStateChanged() = false after a lost update, so HasUpdate " +
			"never wakes the step worker and the grant is never written")
	}

	next, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate (second): %v", err)
	}
	if next.ReadOnly {
		t.Fatal("the update after a lost one is ReadOnly, so SaveState is " +
			"skipped and the vote grant never reaches the WAL")
	}
	if next.State.Term != 5 || next.State.Vote != 2 {
		t.Fatalf("the re-persisted state lost the grant: term=%d vote=%d",
			next.State.Term, next.State.Vote)
	}

	// The demand is satisfied by that update, not sticky forever.
	if p.stateUnpersisted {
		t.Error("stateUnpersisted survived the update that persisted the state")
	}
}

func TestPeer_GetUpdate_CollectsEntriesToSave(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.EntriesToSave) != 2 {
		t.Fatalf("expected 2 entries to save, got %d", len(update.EntriesToSave))
	}
	if update.FirstIndex != 1 {
		t.Fatalf("expected FirstIndex 1, got %d", update.FirstIndex)
	}
	if update.LastIndex != 2 {
		t.Fatalf("expected LastIndex 2, got %d", update.LastIndex)
	}
}

func TestPeer_GetUpdate_CollectsCommittedEntries(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	p.r.log.committed = 2

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.CommittedEntries) != 2 {
		t.Fatalf("expected 2 committed entries, got %d", len(update.CommittedEntries))
	}
}

func TestPeer_GetUpdate_SkipsCommittedWhenFalse(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})
	p.r.log.committed = 1

	update, err := p.GetUpdate(false, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.CommittedEntries) != 0 {
		t.Fatalf("expected 0 committed entries, got %d", len(update.CommittedEntries))
	}
}

func TestPeer_GetUpdate_CollectsMessages(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.msgs = append(p.r.msgs, proto.Message{Type: proto.Heartbeat})

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(update.Messages))
	}
	// Messages should be drained from raft.
	if len(p.r.msgs) != 0 {
		t.Fatalf("expected raft msgs to be drained, got %d", len(p.r.msgs))
	}
}

func TestPeer_GetUpdate_CollectsReadStates(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.readStates = append(p.r.readStates, proto.ReadyToRead{Key: 42, Index: 5})

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.ReadyToRead) != 1 {
		t.Fatalf("expected 1 read state, got %d", len(update.ReadyToRead))
	}
	if update.ReadyToRead[0].Key != 42 {
		t.Fatalf("expected key 42, got %d", update.ReadyToRead[0].Key)
	}
	// Read states should be drained.
	if p.r.readStates != nil {
		t.Fatal("expected readStates to be nil after GetUpdate")
	}
}

func TestPeer_GetUpdate_UpdatesState(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.term = 5
	p.r.vote = 2

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if update.State.Term != 5 {
		t.Fatalf("expected term 5, got %d", update.State.Term)
	}
	if update.State.Vote != 2 {
		t.Fatalf("expected vote 2, got %d", update.State.Vote)
	}
}

func TestPeer_GetUpdate_ResetsPrevState(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.term = 5

	_, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.prevState.Term != 5 {
		t.Fatalf("expected prevState.Term 5, got %d", p.prevState.Term)
	}
}

func TestPeer_GetUpdate_SetsShardAndReplica(t *testing.T) {
	cfg := testConfigWithID(42, 7)
	ms := proto.Membership{
		Addresses: map[uint64]string{7: "addr7"},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if update.ShardID != 42 {
		t.Fatalf("expected ShardID 42, got %d", update.ShardID)
	}
	if update.ReplicaID != 7 {
		t.Fatalf("expected ReplicaID 7, got %d", update.ReplicaID)
	}
}

func TestPeer_GetUpdate_DoesNotAdvanceProcessed(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	p.r.log.committed = 2

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// GetUpdate must NOT advance processed. The processed marker is
	// advanced by Commit to ensure entries are not lost if the
	// downstream send fails.
	if p.r.log.processed != 0 {
		t.Fatalf("expected processed 0 after GetUpdate, got %d", p.r.log.processed)
	}

	// Commit advances processed.
	mustCommit(t, p, update)
	if p.r.log.processed != 2 {
		t.Fatalf("expected processed 2 after Commit, got %d", p.r.log.processed)
	}
}

func TestPeer_GetUpdate_CollectsSnapshot(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 2}
	p.hasPendingSnapshot = true

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if update.Snapshot.Index != 10 {
		t.Fatalf("expected snapshot index 10, got %d", update.Snapshot.Index)
	}
	if update.Snapshot.Term != 2 {
		t.Fatalf("expected snapshot term 2, got %d", update.Snapshot.Term)
	}
	if p.hasPendingSnapshot {
		t.Fatal("expected hasPendingSnapshot to be false after GetUpdate")
	}
}

func TestPeer_GetUpdate_Empty(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.EntriesToSave) != 0 {
		t.Fatalf("expected no entries to save, got %d", len(update.EntriesToSave))
	}
	if len(update.CommittedEntries) != 0 {
		t.Fatalf("expected no committed entries, got %d", len(update.CommittedEntries))
	}
	if len(update.Messages) != 0 {
		t.Fatalf("expected no messages, got %d", len(update.Messages))
	}
}

// --- Commit ---

func TestPeer_Commit_UpdatesSavedMarkers(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})

	update := proto.Update{
		EntriesToSave: []proto.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
		},
	}
	mustCommit(t, p, update)

	if p.r.log.inmem.savedTo != 2 {
		t.Fatalf("expected savedTo 2, got %d", p.r.log.inmem.savedTo)
	}
}

func TestPeer_Commit_NoEntries(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	update := proto.Update{}
	mustCommit(t, p, update) // should not panic

	if p.r.log.inmem.savedTo != 0 {
		t.Fatalf("expected savedTo 0, got %d", p.r.log.inmem.savedTo)
	}
}

func TestPeer_Commit_WithSnapshot(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.snapshot = &proto.Snapshot{Index: 10, Term: 2}

	update := proto.Update{
		Snapshot: proto.Snapshot{Index: 10, Term: 2},
	}
	mustCommit(t, p, update)

	// Snapshot should be cleared after commit.
	if p.r.log.inmem.snapshot != nil {
		t.Fatal("expected snapshot to be nil after commit")
	}
}

func TestPeer_Commit_WithCommittedEntries(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	p.r.log.committed = 3
	p.r.log.processed = 2

	// LastApplied reflects what the apply worker confirmed in the
	// previous cycle. CommittedEntries are being committed NOW;
	// LastApplied is what was applied BEFORE. appliedLogTo truncates
	// inmem up to LastApplied, not CommittedEntries[last].Index.
	update := proto.Update{
		CommittedEntries: []proto.Entry{
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
		LastApplied: 1,
	}
	mustCommit(t, p, update)

	// appliedLogTo(1) truncated entry@1, so appliedTo = 1.
	if p.r.log.inmem.appliedTo != 1 {
		t.Fatalf("expected appliedTo 1, got %d", p.r.log.inmem.appliedTo)
	}
	// markerIndex should advance past the truncated entry.
	if p.r.log.inmem.markerIndex != 2 {
		t.Fatalf("expected markerIndex 2, got %d", p.r.log.inmem.markerIndex)
	}
	// Entries 2 and 3 should remain in inmem.
	if len(p.r.log.inmem.entries) != 2 {
		t.Fatalf("expected 2 remaining entries, got %d", len(p.r.log.inmem.entries))
	}
}

func TestPeer_Commit_WithReadyToRead(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	update := proto.Update{
		ReadyToRead: []proto.ReadyToRead{
			{Key: 1, Index: 5},
			{Key: 2, Index: 6},
		},
	}
	mustCommit(t, p, update)
	// Should not panic; ReadyToRead count is tracked.
}

func TestPeer_Commit_LastAppliedUsesUpdateFieldNotCommittedEntries(t *testing.T) {
	// Verify that uc.LastApplied is set from update.LastApplied (what the
	// apply worker confirmed in a previous cycle), NOT from
	// CommittedEntries[last].Index. Using the wrong value caused marker
	// index mismatches during rapid node create/destroy.
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	p.r.log.committed = 3
	p.r.log.processed = 0

	// Simulate first cycle: nothing applied yet (lastApplied=0).
	// CommittedEntries=[1,2,3], but LastApplied=0.
	update := proto.Update{
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
		EntriesToSave: []proto.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
		LastApplied: 0,
	}
	mustCommit(t, p, update)

	// With LastApplied=0, appliedLogTo should NOT be called, so all
	// entries remain in inmem (only truncated in the next cycle when
	// the apply worker reports lastApplied > 0).
	if p.r.log.inmem.markerIndex != 1 {
		t.Fatalf("expected markerIndex 1 (no truncation), got %d",
			p.r.log.inmem.markerIndex)
	}
	if len(p.r.log.inmem.entries) != 3 {
		t.Fatalf("expected 3 entries in inmem, got %d",
			len(p.r.log.inmem.entries))
	}
	// processed should be advanced by appliedTo().
	if p.r.log.processed != 3 {
		t.Fatalf("expected processed 3, got %d", p.r.log.processed)
	}
}

func TestPeer_Commit_LastAppliedTruncatesOnlyAppliedEntries(t *testing.T) {
	// When LastApplied > 0 from a previous cycle, only entries up to
	// LastApplied should be truncated from inmem.
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
	})
	p.r.log.committed = 4
	p.r.log.processed = 2 // Processed up to 2 from a prior cycle.

	// This cycle commits entries 3-4, and the apply worker confirmed
	// up to index 2 from the previous cycle.
	update := proto.Update{
		CommittedEntries: []proto.Entry{
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
		},
		EntriesToSave: []proto.Entry{
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
		},
		LastApplied: 2, // Apply worker confirmed up to 2.
	}
	mustCommit(t, p, update)

	// appliedLogTo(2) should truncate entries 1,2 but keep 3,4.
	if p.r.log.inmem.markerIndex != 3 {
		t.Fatalf("expected markerIndex 3, got %d",
			p.r.log.inmem.markerIndex)
	}
	if len(p.r.log.inmem.entries) != 2 {
		t.Fatalf("expected 2 remaining entries, got %d",
			len(p.r.log.inmem.entries))
	}
	if p.r.log.inmem.entries[0].Index != 3 {
		t.Fatalf("expected first remaining entry at index 3, got %d",
			p.r.log.inmem.entries[0].Index)
	}
	if p.r.log.inmem.appliedTo != 2 {
		t.Fatalf("expected appliedTo 2, got %d",
			p.r.log.inmem.appliedTo)
	}
}

// TestPeer_Commit_MultipleConfigChangesInBatch verifies that Commit
// processes multiple config change entries in a single committed batch
// without panicking. This scenario occurs when a follower catches up
// via log replication and receives entries from multiple leader rounds
// in one batch (e.g., a RemoveNode followed by an AddNode that were
// committed separately on the leader).
func TestPeer_Commit_MultipleConfigChangesInBatch(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange},
		{Index: 3, Term: 1},
		{Index: 4, Term: 2, Type: proto.EntryConfigChange},
	})
	p.r.log.committed = 4
	p.r.log.processed = 0

	// Set pendingConfigChange to true to verify it gets cleared.
	p.r.pendingConfigChange = true

	update := proto.Update{
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1, Type: proto.EntryConfigChange},
			{Index: 3, Term: 1},
			{Index: 4, Term: 2, Type: proto.EntryConfigChange},
		},
	}
	mustCommit(t, p, update)

	// pendingConfigChange must be cleared after processing both
	// config change entries.
	if p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be false after committing batch with multiple config changes")
	}
}

// TestPeer_Commit_MultipleConfigChangesInBatch_InvalidInput verifies
// that a batch with zero committed entries does not affect the
// pendingConfigChange flag.
func TestPeer_Commit_MultipleConfigChangesInBatch_NoEntries(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.pendingConfigChange = true

	update := proto.Update{
		CommittedEntries: nil,
	}
	mustCommit(t, p, update)

	// pendingConfigChange should remain true since no config change
	// entries were processed.
	if !p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to remain true when no entries are committed")
	}
}

// --- LeaderID / IsLeader / IsFollower / IsObserver / IsWitness ---

func TestPeer_LeaderID_InitialZero(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	if p.LeaderID() != 0 {
		t.Fatalf("expected initial leaderID 0, got %d", p.LeaderID())
	}
}

func TestPeer_IsFollower_True(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	if !p.IsFollower() {
		t.Fatal("expected IsFollower to be true")
	}
}

func TestPeer_IsFollower_FalseWhenLeader(t *testing.T) {
	p := newTestLeaderPeer(t)

	if p.IsFollower() {
		t.Fatal("expected IsFollower to be false when leader")
	}
}

func TestPeer_IsFollower_FalseWhenObserver(t *testing.T) {
	cfg := testConfig()
	cfg.IsObserver = true
	p := newTestPeer(t, cfg, proto.State{}, selfObserverMembership())

	if p.IsFollower() {
		t.Fatal("expected IsFollower to be false when observer")
	}
}

func TestPeer_IsObserver_True(t *testing.T) {
	cfg := testConfig()
	cfg.IsObserver = true
	p := newTestPeer(t, cfg, proto.State{}, selfObserverMembership())

	if !p.IsObserver() {
		t.Fatal("expected IsObserver to be true")
	}
}

func TestPeer_IsObserver_False(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	if p.IsObserver() {
		t.Fatal("expected IsObserver to be false")
	}
}

func TestPeer_IsWitness_True(t *testing.T) {
	cfg := testConfig()
	cfg.IsWitness = true
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	if !p.IsWitness() {
		t.Fatal("expected IsWitness to be true")
	}
}

func TestPeer_IsWitness_False(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	if p.IsWitness() {
		t.Fatal("expected IsWitness to be false")
	}
}

// --- GetMembership ---

func TestPeer_GetMembership_ReturnsCopy(t *testing.T) {
	ms := proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
	}
	p := newTestPeer(t, testConfig(), proto.State{}, ms)

	result := p.GetMembership()
	result.Addresses[1] = "mutated"

	// Original should be unchanged.
	if p.membership.addresses[1] != "addr1" {
		t.Fatal("GetMembership should return a deep copy")
	}
}

func TestPeer_GetMembership_ReflectsMutations(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)
	electSingleNodeLeader(t, p)
	// Drain initial state.
	if p.HasUpdate(0) {
		update, err := p.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("failed to get update: %v", err)
		}
		mustCommit(t, p, update)
	}

	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Membership takes effect at commit time; drive the update cycles so
	// the single-node leader persists and commits the entry.
	runUpdateCycles(t, p)

	result := p.GetMembership()
	if _, ok := result.Addresses[4]; !ok {
		t.Fatal("expected replica 4 in membership after config change committed")
	}
}

// --- hasStateChanged ---

func TestPeer_HasStateChanged_TermChanged(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.term = 5

	if !p.hasStateChanged() {
		t.Fatal("expected state changed when term differs")
	}
}

func TestPeer_HasStateChanged_VoteChanged(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.vote = 2

	if !p.hasStateChanged() {
		t.Fatal("expected state changed when vote differs")
	}
}

func TestPeer_HasStateChanged_CommitChanged(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})
	p.r.log.committed = 1

	if !p.hasStateChanged() {
		t.Fatal("expected state changed when commit differs")
	}
}

func TestPeer_HasStateChanged_NoChange(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())
	// After newTestPeer, prevState matches raft's hard state.

	if p.hasStateChanged() {
		t.Fatal("expected no state change")
	}
}

// --- NotLeaderError ---

func TestPeer_NotLeaderError_WithLeader(t *testing.T) {
	e := &NotLeaderError{LeaderID: 5, LeaderAddress: "addr5"}
	expected := "raft: not leader, current leader: 5 (addr5)"
	if e.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, e.Error())
	}
}

func TestPeer_NotLeaderError_NoLeader(t *testing.T) {
	e := &NotLeaderError{LeaderID: 0}
	expected := "raft: not leader, no leader currently known"
	if e.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, e.Error())
	}
}

// --- Handle ReplicateResp / HeartbeatResp ---

func TestPeer_Handle_ReplicateResp(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	msg := proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 5,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPeer_Handle_HeartbeatResp(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	msg := proto.Message{
		Type:    proto.HeartbeatResp,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    1,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Full Lifecycle ---

func TestPeer_FullLifecycle_ProposeGetUpdateCommit(t *testing.T) {
	p := newTestLeaderPeer(t)
	leaderTerm := p.r.term

	// Step 1: Propose entries.
	entries := []proto.Entry{
		{Cmd: []byte("cmd1")},
		{Cmd: []byte("cmd2")},
	}
	_, err := p.Propose(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Step 2: HasUpdate should be true.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after propose")
	}

	// Step 3: GetUpdate collects the changes.
	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.EntriesToSave) < 2 {
		t.Fatalf("expected at least 2 entries to save, got %d", len(update.EntriesToSave))
	}
	if update.State.Term != leaderTerm {
		t.Fatalf("expected term %d, got %d", leaderTerm, update.State.Term)
	}

	// Step 4: Commit acknowledges persistence. This is the persistence
	// edge: the leader's own match advances to its durable index, which
	// completes the single-node quorum and commits the entries.
	mustCommit(t, p, update)

	// Step 5: The persistence-edge commit produces committed entries to
	// apply, so HasUpdate must report a pending update.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after the persistence-edge commit")
	}

	// Step 6: The follow-up update carries the proposed entries as
	// committed entries.
	update, err = p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.CommittedEntries) < 2 {
		t.Fatalf("expected at least 2 committed entries, got %d", len(update.CommittedEntries))
	}
	mustCommit(t, p, update)

	// Step 7: With the entries persisted and applied, no update remains.
	if p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be false after commit")
	}
}

func TestPeer_FullLifecycle_HandleReplicateGetUpdateCommit(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Seed log with initial entry for tryAppend.
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})

	// Step 1: Handle a Replicate with new entries and commit.
	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
		Commit:   3,
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Cmd: []byte("a")},
			{Index: 3, Term: 1, Cmd: []byte("b")},
		},
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Step 2: Check HasUpdate.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true")
	}

	// Step 3: GetUpdate.
	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.Messages) == 0 {
		t.Fatal("expected at least 1 message")
	}
	if len(update.CommittedEntries) < 2 {
		t.Fatalf("expected at least 2 committed entries, got %d", len(update.CommittedEntries))
	}

	// Step 4: Commit.
	mustCommit(t, p, update)
}

func TestPeer_FullLifecycle_ElectAndPropose(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick until we become leader.
	electSingleNodeLeader(t, p)

	// Drain election state.
	runUpdateCycles(t, p)

	// Now propose.
	entries := []proto.Entry{{Cmd: []byte("hello")}}
	_, err := p.Propose(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have update.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after propose")
	}

	// A single-node proposal commits at the persistence edge: the first
	// cycle persists the entry, the second surfaces it as committed.
	committed := runUpdateCycles(t, p)

	var foundCmd bool
	for _, e := range committed {
		if string(e.Cmd) == "hello" {
			foundCmd = true
		}
	}
	if !foundCmd {
		t.Fatal("expected proposed entry in committed entries")
	}
}

// --- Leader heartbeat production ---

func TestPeer_Leader_ProducesHeartbeats(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Tick past the heartbeat timeout. With HeartbeatRTT=1, one tick
	// should produce heartbeats.
	p.Tick()

	// Check for heartbeat messages.
	var heartbeatCount int
	for _, m := range p.r.msgs {
		if m.Type == proto.Heartbeat {
			heartbeatCount++
		}
	}
	if heartbeatCount == 0 {
		t.Fatal("expected heartbeat messages from leader after tick")
	}
}

func TestPeer_Leader_ProducesReplicateOnPropose(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// After becoming leader, the remotes are in probe state with
	// probeSent=true (from the initial broadcastAppend). Simulate
	// successful ReplicateResp from both peers to transition them
	// to replicate state and unblock further sends.
	noopIdx := p.r.log.lastIndex()
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: noopIdx,
	})
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     3,
		Term:     p.r.term,
		LogIndex: noopIdx,
	})
	// Drain any messages from the response handling.
	p.r.drainMessages()

	entries := []proto.Entry{{Cmd: []byte("cmd1")}}
	_, err := p.Propose(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have Replicate messages to peers.
	var replicateCount int
	for _, m := range p.r.msgs {
		if m.Type == proto.Replicate {
			replicateCount++
		}
	}
	if replicateCount == 0 {
		t.Fatal("expected Replicate messages to peers after propose")
	}
}

// --- syncLeaderID ---

func TestPeer_SyncLeaderID_ReflectsRaftState(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Initially leaderID is 0 (follower, no known leader).
	if p.LeaderID() != 0 {
		t.Fatalf("expected initial leaderID 0, got %d", p.LeaderID())
	}

	// Become leader.
	electSingleNodeLeader(t, p)

	// syncLeaderID should have been called by Tick.
	if p.LeaderID() != cfg.ReplicaID {
		t.Fatalf("expected leaderID %d after election, got %d",
			cfg.ReplicaID, p.LeaderID())
	}
}

func TestPeer_SyncLeaderID_UpdatedOnHandle(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Handle a heartbeat from node 5 to set leader.
	msg := proto.Message{
		Type: proto.Heartbeat,
		From: 5,
		Term: 1,
	}
	p.Handle(msg)

	if p.LeaderID() != 5 {
		t.Fatalf("expected leaderID 5, got %d", p.LeaderID())
	}
}

// --- ReadIndex through full protocol ---

func TestPeer_ReadIndex_MultiNode_ProducesHeartbeats(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	err := p.ReadIndex(99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	p.FlushReadIndex()

	// For a multi-node cluster, the leader should have sent heartbeats
	// to confirm leadership.
	var heartbeatCount int
	for _, m := range p.r.msgs {
		if m.Type == proto.Heartbeat {
			heartbeatCount++
		}
	}
	if heartbeatCount == 0 {
		t.Fatal("expected heartbeat messages for read index confirmation")
	}
}

// --- Tick produces messages ---

func TestPeer_Tick_FollowerStartsElection(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past the election timeout.
	for range 25 {
		p.Tick()
	}

	// Should have sent RequestVote messages.
	var voteReqCount int
	for _, m := range p.r.msgs {
		if m.Type == proto.RequestVote {
			voteReqCount++
		}
	}
	if voteReqCount == 0 {
		t.Fatal("expected RequestVote messages after election timeout")
	}
}

func TestPeer_Tick_FollowerStartsPreVoteElection(t *testing.T) {
	cfg := testConfig()
	// PreVote is true by default from SetDefaults.
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past the election timeout.
	for range 25 {
		p.Tick()
	}

	// Should have sent RequestPreVote messages.
	var preVoteCount int
	for _, m := range p.r.msgs {
		if m.Type == proto.RequestPreVote {
			preVoteCount++
		}
	}
	if preVoteCount == 0 {
		t.Fatal("expected RequestPreVote messages after election timeout")
	}
}

// --- Multi-node election through Peer ---

func TestPeer_MultiNodeElection_VoteGrantsLeadToLeadership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past election timeout to start election.
	for range 25 {
		p.Tick()
	}

	// Simulate vote grants from peers.
	p.Handle(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: p.r.term,
	})

	// With vote from self and node 2, that is a quorum (2 out of 3).
	if !p.IsLeader() {
		t.Fatal("expected to be leader after receiving quorum of votes")
	}
}

func TestPeer_MultiNodeElection_VoteRejectionsLeadToFollower(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past election timeout.
	for range 25 {
		p.Tick()
	}

	// Both peers reject.
	p.Handle(proto.Message{
		Type:   proto.RequestVoteResp,
		From:   2,
		Term:   p.r.term,
		Reject: true,
	})
	p.Handle(proto.Message{
		Type:   proto.RequestVoteResp,
		From:   3,
		Term:   p.r.term,
		Reject: true,
	})

	// Should revert to follower.
	if p.IsLeader() {
		t.Fatal("expected to be follower after vote rejections")
	}
}

// --- OrderedConfigChange via Peer.ProposeConfigChange ---

func TestPeer_ProposeConfigChange_RejectsWhilePending(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// First config change: add node 4. This sets pendingConfigChange=true
	// in the raft state machine until the entry is committed.
	cc1 := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc1)
	if err != nil {
		t.Fatalf("first config change should succeed: %v", err)
	}

	// Verify pendingConfigChange is true.
	if !p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be true after first proposal")
	}

	// Second config change while first is still pending (not yet committed).
	cc2 := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 5,
		Address:   "addr5",
	}
	err = p.ProposeConfigChange(cc2)
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("expected ErrPendingConfigChange, got %v", err)
	}
}

func TestPeer_ProposeConfigChange_AllowsAfterCommit(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Propose first config change: add node 4.
	cc1 := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc1)
	if err != nil {
		t.Fatalf("first config change should succeed: %v", err)
	}

	// Membership takes effect at commit time, so the AddNode(4) entry
	// commits under the OLD config {1,2,3} (quorum 2). One follower ack
	// plus the leader's durable match suffices; ack both for good measure.
	lastIdx := p.r.log.lastIndex()
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: lastIdx,
	})
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     3,
		Term:     p.r.term,
		LogIndex: lastIdx,
	})

	// Run the update cycles: the first persists the config change entry
	// (whose durable leader match completes the quorum with the two acks
	// above), and a follow-up cycle surfaces it as committed, which
	// clears pendingConfigChange.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after config change acks")
	}
	committed := runUpdateCycles(t, p)

	// Verify the config change entry reached the committed entries.
	var foundCC bool
	for _, e := range committed {
		if e.Type&proto.EntryTypeMask == proto.EntryConfigChange {
			foundCC = true
		}
	}
	if !foundCC {
		t.Fatal("expected config change entry in committed entries")
	}

	// pendingConfigChange should now be cleared.
	if p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be false after commit")
	}

	// Second config change should now succeed.
	cc2 := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 5,
		Address:   "addr5",
	}
	err = p.ProposeConfigChange(cc2)
	if err != nil {
		t.Fatalf("second config change after commit should succeed: %v", err)
	}

	// Commit the second change: membership {1,2,3,4} has quorum 3, so the
	// leader's durable match plus acks from replicas 2 and 3 suffice.
	lastIdx2 := p.r.log.lastIndex()
	for _, from := range []uint64{2, 3} {
		p.Handle(proto.Message{
			Type:     proto.ReplicateResp,
			From:     from,
			Term:     p.r.term,
			LogIndex: lastIdx2,
		})
	}
	runUpdateCycles(t, p)

	if !p.membership.isVoter(5) {
		t.Fatal("expected replica 5 to be voter after second config change committed")
	}
}

// --- Follower config change application (commit-time) ---

func TestPeer_Handle_FollowerAppliesConfigChangeOnCommit(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Seed the log with an initial entry so the follower has a matching
	// prevLogIndex for the Replicate message.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	// Construct a config change entry that adds node 4.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	buf := make([]byte, cc.Size())
	_, err := cc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("failed to marshal config change: %v", err)
	}

	// Send a Replicate message from the leader containing the config
	// change entry with the leader's commit index covering it. Config
	// changes take effect at COMMIT time, applied via the update cycle
	// (Peer.Commit -> processConfigChanges).
	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1, // prevLogIndex
		LogTerm:  1, // prevLogTerm
		Commit:   2,
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: buf},
		},
	}
	err = p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Not yet applied: membership mutates only when the committed entry
	// flows through Peer.Commit.
	if _, ok := p.GetMembership().Addresses[4]; ok {
		t.Fatal("membership must not change before the update cycle commits the entry")
	}
	runUpdateCycles(t, p)

	// Verify the peer's membership reflects the new node.
	membership := p.GetMembership()
	addr, ok := membership.Addresses[4]
	if !ok {
		t.Fatal("expected replica 4 to be in membership after follower commits config change")
	}
	if addr != "addr4" {
		t.Fatalf("expected address addr4, got %s", addr)
	}
}

func TestPeer_Handle_RejectedReplicateDoesNotApplyConfigChange(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Seed log with entry at index 1 term 1.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	// Construct a config change entry that adds node 4.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	buf := make([]byte, cc.Size())
	_, err := cc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("failed to marshal config change: %v", err)
	}

	// Send a Replicate with a WRONG prevLogTerm so the append is rejected.
	// The entries contain a config change that should NOT be applied.
	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  99, // mismatch: actual term at index 1 is 1
		Commit:   0,
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: buf},
		},
	}
	err = p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify a reject response was sent.
	var rejected bool
	for _, m := range p.r.msgs {
		if m.Type == proto.ReplicateResp && m.Reject {
			rejected = true
		}
	}
	if !rejected {
		t.Fatal("expected a reject ReplicateResp")
	}

	// Membership must NOT contain node 4, since the entries were rejected.
	membership := p.GetMembership()
	if _, ok := membership.Addresses[4]; ok {
		t.Fatal("replica 4 should NOT be in membership after rejected append")
	}
}

func TestPeer_Handle_FollowerUpdatesRemotesOnConfigChange(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Seed log.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	// Construct a config change entry adding node 4 as a voter.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	buf := make([]byte, cc.Size())
	_, err := cc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("failed to marshal config change: %v", err)
	}

	msg := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
		Commit:   2,
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: buf},
		},
	}
	err = p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	runUpdateCycles(t, p)

	// Verify that the membership's internal state includes node 4 as a voter.
	if !p.membership.isVoter(4) {
		t.Fatal("expected replica 4 to be a voter in membership after config change committed")
	}

	// Also test adding an observer via a second Replicate.
	ccObs := proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 5,
		Address:   "addr5",
	}
	bufObs := make([]byte, ccObs.Size())
	_, err = ccObs.MarshalTo(bufObs)
	if err != nil {
		t.Fatalf("failed to marshal observer config change: %v", err)
	}

	msg2 := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 2,
		LogTerm:  1,
		Commit:   3,
		Entries: []proto.Entry{
			{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: bufObs},
		},
	}
	err = p.Handle(msg2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	runUpdateCycles(t, p)

	if !p.membership.isObserver(5) {
		t.Fatal("expected replica 5 to be an observer after config change committed")
	}
}

// --- Leader step-down on self-removal ---

func TestPeer_LeaderStepsDown_WhenRemovedFromConfig(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	if !p.IsLeader() {
		t.Fatal("precondition: peer must be leader")
	}

	// Propose removing self (replica 1) from the cluster.
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: p.replicaID,
	}
	err := p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("self-removal config change should succeed: %v", err)
	}

	// Membership takes effect at commit time: the leader is still a
	// member (and still functioning) until the removal entry commits.
	if !p.membership.isMember(p.replicaID) {
		t.Fatal("expected leader to remain in membership until the removal commits")
	}

	// Simulate follower acknowledgement to commit the entry.
	lastIdx := p.r.log.lastIndex()
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: lastIdx,
	})

	// Run the update cycles: the first persists the config change entry
	// (its durable leader match completes the quorum with the ack above),
	// and a follow-up cycle surfaces it as committed. The Commit that
	// carries the committed config change calls checkLeaderSelfRemoval,
	// which steps the leader down since it is no longer in membership.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true")
	}
	committed := runUpdateCycles(t, p)

	// Verify the config change reached the committed entries.
	var foundCC bool
	for _, e := range committed {
		if e.Type&proto.EntryTypeMask == proto.EntryConfigChange {
			foundCC = true
		}
	}
	if !foundCC {
		t.Fatal("expected config change entry in committed entries")
	}

	// After commit, the leader must have stepped down.
	if p.IsLeader() {
		t.Fatal("expected leader to step down after self-removal is committed")
	}
	if p.r.state == leader {
		t.Fatal("expected raft state to be follower after self-removal commit")
	}
}

func TestPeer_LeaderContinues_WhenOtherNodeRemoved(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	if !p.IsLeader() {
		t.Fatal("precondition: peer must be leader")
	}

	leaderID := p.replicaID

	// Propose removing node 3 (not the leader).
	cc := proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 3,
	}
	err := p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("remove-other config change should succeed: %v", err)
	}

	// Membership takes effect at commit time: node 3 is still a member
	// until the removal entry commits.
	if !p.membership.isMember(3) {
		t.Fatal("expected node 3 to remain in membership until the removal commits")
	}

	// Simulate follower acknowledgement and drive the update cycles so
	// the entry persists, commits, and the membership change applies.
	lastIdx := p.r.log.lastIndex()
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: lastIdx,
	})
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true")
	}
	runUpdateCycles(t, p)

	// Node 3 is removed once the entry committed.
	if p.membership.isMember(3) {
		t.Fatal("expected node 3 to be removed after the removal committed")
	}

	// Leader must remain leader since it was not the removed node.
	if !p.IsLeader() {
		t.Fatal("expected leader to remain leader after removing another node")
	}
	if p.LeaderID() != leaderID {
		t.Fatalf("expected leaderID %d, got %d", leaderID, p.LeaderID())
	}
	if p.r.state != leader {
		t.Fatal("expected raft state to remain leader")
	}
}

// --- Snapshot membership update via Peer.Handle ---

func TestPeer_Handle_SnapshotUpdatesMembership(t *testing.T) {
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Verify initial membership has only node 1.
	initialMembership := p.GetMembership()
	if len(initialMembership.Addresses) != 1 {
		t.Fatalf("expected 1 initial member, got %d", len(initialMembership.Addresses))
	}

	// Construct a snapshot with expanded membership including nodes 1, 2, 3.
	newMembershipProto := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
			3: "addr3",
		},
		Observers: map[uint64]string{
			10: "obs10",
		},
	}
	snap := proto.Snapshot{
		ShardID:    1,
		ReplicaID:  1,
		Index:      50,
		Term:       5,
		Membership: newMembershipProto,
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     5,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the peer's membership now reflects the snapshot's membership.
	updatedMembership := p.GetMembership()

	if len(updatedMembership.Addresses) != 3 {
		t.Fatalf("expected 3 voting members after snapshot, got %d", len(updatedMembership.Addresses))
	}
	for _, id := range []uint64{1, 2, 3} {
		if _, ok := updatedMembership.Addresses[id]; !ok {
			t.Fatalf("expected replica %d in membership after snapshot", id)
		}
	}

	// Verify observers from the snapshot are also reflected.
	if len(updatedMembership.Observers) != 1 {
		t.Fatalf("expected 1 observer after snapshot, got %d", len(updatedMembership.Observers))
	}
	if addr, ok := updatedMembership.Observers[10]; !ok || addr != "obs10" {
		t.Fatalf("expected observer 10 with address obs10, got %v", updatedMembership.Observers)
	}
}

func TestPeer_Handle_SnapshotUpdatesMembership_ReducedCluster(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Verify initial membership has 3 nodes.
	initialMembership := p.GetMembership()
	if len(initialMembership.Addresses) != 3 {
		t.Fatalf("expected 3 initial members, got %d", len(initialMembership.Addresses))
	}

	// Install a snapshot that reduces the cluster to 2 nodes (node 3 removed).
	snap := proto.Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     100,
		Term:      10,
		Membership: proto.Membership{
			Addresses: map[uint64]string{
				1: "addr1",
				2: "addr2",
			},
			Removed: map[uint64]bool{
				3: true,
			},
		},
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     10,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify membership reflects the reduced cluster.
	updatedMembership := p.GetMembership()
	if len(updatedMembership.Addresses) != 2 {
		t.Fatalf("expected 2 voting members after snapshot, got %d", len(updatedMembership.Addresses))
	}
	if _, ok := updatedMembership.Addresses[3]; ok {
		t.Fatal("expected replica 3 to NOT be in membership after snapshot")
	}
	if !updatedMembership.Removed[3] {
		t.Fatal("expected replica 3 to be in removed set after snapshot")
	}
}

// --- GetUpdate ReadOnly flag ---

func TestGetUpdate_ReadOnly_PureReadIndex(t *testing.T) {
	// A single-node leader that has committed its no-op entry. When a
	// ReadIndex request is made, the resulting Update should be marked
	// ReadOnly because there are no entries to save, no state changes,
	// no committed entries to apply, and no snapshot.
	p := newTestLeaderPeer(t)

	// Verify precondition: no pending state.
	if p.HasUpdate(0) {
		t.Fatal("precondition: expected no pending updates after drain")
	}

	// Issue a ReadIndex request. For a single-node cluster, the leader
	// responds immediately by appending to readStates.
	err := p.ReadIndex(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// HasUpdate should be true due to the read state.
	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after ReadIndex")
	}

	// GetUpdate should produce a read-only update.
	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify ReadOnly is true.
	if !update.ReadOnly {
		t.Fatal("expected update.ReadOnly to be true for pure ReadIndex update")
	}

	// Verify ReadyToRead contains the read index request.
	if len(update.ReadyToRead) != 1 {
		t.Fatalf("expected 1 ReadyToRead entry, got %d", len(update.ReadyToRead))
	}
	if update.ReadyToRead[0].Key != 42 {
		t.Fatalf("expected ReadyToRead key 42, got %d", update.ReadyToRead[0].Key)
	}

	// Verify no entries or state changes are present.
	if len(update.EntriesToSave) != 0 {
		t.Fatalf("expected 0 entries to save, got %d", len(update.EntriesToSave))
	}
	if len(update.CommittedEntries) != 0 {
		t.Fatalf("expected 0 committed entries, got %d", len(update.CommittedEntries))
	}
	if update.Snapshot.Index != 0 {
		t.Fatalf("expected no snapshot, got index %d", update.Snapshot.Index)
	}
}

func TestGetUpdate_NotReadOnly_WithEntries(t *testing.T) {
	// When a leader proposes entries, the resulting update has entries
	// to save and committed entries to apply, so ReadOnly must be false.
	p := newTestLeaderPeer(t)

	entries := []proto.Entry{
		{Cmd: []byte("cmd1")},
	}
	_, err := p.Propose(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after propose")
	}

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ReadOnly must be false because there are entries to save.
	if update.ReadOnly {
		t.Fatal("expected update.ReadOnly to be false when entries are present")
	}

	// Verify entries are actually present in the update.
	if len(update.EntriesToSave) == 0 {
		t.Fatal("expected entries to save in the update")
	}
}

func TestGetUpdate_NotReadOnly_StateChange(t *testing.T) {
	// When the peer's hard state changes (e.g., term increases from a
	// higher-term message), the resulting update must not be read-only.
	cfg := testConfig()
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// The peer starts as a follower at term 0. Receive a heartbeat from
	// a leader at term 5. This causes a term change (state change).
	msg := proto.Message{
		Type: proto.Heartbeat,
		From: 2,
		Term: 5,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after term change")
	}

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ReadOnly must be false because the hard state changed (term).
	if update.ReadOnly {
		t.Fatal("expected update.ReadOnly to be false when state changed")
	}

	// Verify the state change is reflected.
	if update.State.Term != 5 {
		t.Fatalf("expected term 5 in update state, got %d", update.State.Term)
	}
}

// --- Benchmarks ---

// benchLeaderPeer creates a single-node Peer that has self-elected as
// leader and drained all initial state. It uses b.Fatal for errors
// instead of t.Fatal. This is the benchmark equivalent of newTestLeaderPeer.
func benchLeaderPeer(b *testing.B) *Peer {
	b.Helper()
	cfg := testConfig()
	ms := singleNodeMembership()
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = ms
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		b.Fatalf("failed to create peer: %v", err)
	}
	// Tick until self-elected as leader.
	for range 30 {
		p.Tick()
		if p.IsLeader() {
			break
		}
	}
	if !p.IsLeader() {
		b.Fatal("single-node peer did not self-elect as leader")
	}
	// Drain the no-op entry and election messages.
	if p.HasUpdate(0) {
		update, err := p.GetUpdate(true, 0)
		if err != nil {
			b.Fatalf("failed to get update: %v", err)
		}
		if err := p.Commit(update); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}
	}
	return p
}

// BenchmarkPeerTick measures the cost of Tick() on a single-node leader.
// This is the hot path called at RTT intervals by the engine step worker.
func BenchmarkPeerTick(b *testing.B) {
	b.ReportAllocs()
	p := benchLeaderPeer(b)
	for b.Loop() {
		p.Tick()
	}
}

// BenchmarkPeerHasUpdate measures the cost of HasUpdate on a leader in
// steady state with no pending work. This is called every step iteration.
func BenchmarkPeerHasUpdate(b *testing.B) {
	b.ReportAllocs()
	p := benchLeaderPeer(b)
	for b.Loop() {
		p.HasUpdate(0)
	}
}

// --- IsMember ---

func TestPeer_IsMember_Voter(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// All voting members should be recognized.
	for id := range ms.Addresses {
		if !p.IsMember(id) {
			t.Errorf("expected replica %d to be a member (voter)", id)
		}
	}

	// Unknown replica should not be a member.
	if p.IsMember(999) {
		t.Error("expected replica 999 to NOT be a member")
	}
}

func TestPeer_IsMember_Observer(t *testing.T) {
	cfg := testConfig()
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
		},
		Observers: map[uint64]string{
			10: "obs-addr",
		},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)

	if !p.IsMember(10) {
		t.Error("expected observer replica 10 to be a member")
	}
	if !p.IsMember(1) {
		t.Error("expected voter replica 1 to be a member")
	}
	if p.IsMember(99) {
		t.Error("expected replica 99 to NOT be a member")
	}
}

func TestPeer_IsMember_Witness(t *testing.T) {
	cfg := testConfig()
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
		},
		Witnesses: map[uint64]string{
			20: "wit-addr",
		},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)

	if !p.IsMember(20) {
		t.Error("expected witness replica 20 to be a member")
	}
	if !p.IsMember(1) {
		t.Error("expected voter replica 1 to be a member")
	}
	if p.IsMember(99) {
		t.Error("expected replica 99 to NOT be a member")
	}
}

func TestPeer_IsMember_RemovedNotMember(t *testing.T) {
	// Use the multi-node leader helper to get a proper 3-node leader.
	p := newTestMultiNodeLeaderPeer(t)

	// Initially member.
	if !p.IsMember(2) {
		t.Fatal("expected replica 2 to be a member before removal")
	}

	// Remove replica 2 via config change.
	err := p.ProposeConfigChange(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 2,
	})
	if err != nil {
		t.Fatalf("ProposeConfigChange failed: %v", err)
	}

	// Membership takes effect at commit time: ack from replica 3 and
	// drive the update cycles so the removal commits.
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     3,
		Term:     p.r.term,
		LogIndex: p.r.log.lastIndex(),
	})
	runUpdateCycles(t, p)

	// After the removal commits, replica 2 should not be a member.
	if p.IsMember(2) {
		t.Error("expected replica 2 to NOT be a member after removal committed")
	}
}

// TestPeer_Authorization_IsADenyListNotAnAllowList pins the per-shard
// authorization predicate.
//
// A replica's configuration is replicated state that LAGS BY DESIGN, so it
// cannot serve as an allow-list: a replica that has not yet applied the config
// change adding the current leader would reject that leader's Replicate,
// Heartbeat and InstallSnapshot — including the very messages that would bring
// it up to date — and wedge itself out of the shard permanently.
//
// The removed set is monotone: entries are only ever added by a committed config
// change, so a stale replica's failure mode is not yet knowing about a removal,
// which the next committed entry repairs. That is the only membership-derived
// fact safe to enforce on the receive path. Same model as etcd
// (EtcdServer.Process rejects only "message from removed member") and
// hashicorp/raft (no sender-membership test on appendEntries at all).
func TestPeer_Authorization_IsADenyListNotAnAllowList(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, proto.Membership{
		Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"},
		Removed:   map[uint64]bool{9: true},
	})

	// A replica this configuration has never heard of is NOT refused. This is
	// the lagging-follower case: it may be a member added after our last
	// applied config change, and it may be the current leader.
	if p.IsRemoved(42) {
		t.Fatal("a replica absent from our configuration was refused: a replica " +
			"added while we were behind — possibly the current leader — would be " +
			"unable to reach us, and so could never bring us up to date")
	}

	// A committed removal IS refused.
	if !p.IsRemoved(9) {
		t.Fatal("a replica with a committed Removed tombstone was authorized")
	}

	// Ordinary members are of course fine.
	for _, id := range []uint64{1, 2, 3} {
		if p.IsRemoved(id) {
			t.Fatalf("member %d was refused", id)
		}
	}

	// IsMember remains available as a view of replicated state, and must NOT be
	// conflated with the authorization decision above.
	if p.IsMember(42) {
		t.Fatal("IsMember reported an unknown replica as a member")
	}
	if !p.IsMember(2) {
		t.Fatal("IsMember failed to report a real member")
	}
}

func TestPeer_IsMember_EmptyMembershipRejectsUnknown(t *testing.T) {
	cfg := testConfigWithID(1, 1)
	// A non-joining peer with empty membership should reject unknown
	// replicas. This covers observers/witnesses with no voters.
	p := newTestPeer(t, cfg, proto.State{}, proto.Membership{})

	if p.IsMember(42) {
		t.Fatal("expected IsMember(42) = false for non-joining empty membership")
	}
}

func TestPeer_IsMember_ConcurrentReadDuringConfigChange(t *testing.T) {
	// Verify that concurrent IsMember reads do not race with config
	// change writes. The atomic snapshot approach means readers always
	// see a consistent snapshot, and the race detector should not fire.
	cfg := testConfigWithID(1, 1)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)
	// Become leader so we can propose config changes.
	p.r.becomeCandidate()
	p.r.becomeLeader()
	p.syncLeaderID()

	done := make(chan struct{})
	// Start concurrent readers.
	for i := 0; i < 8; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					p.IsMember(1)
					p.IsMember(2)
					p.IsMember(3)
					p.IsMember(999)
					_ = p.GetMembership()
				}
			}
		}()
	}

	// Simulate config changes via processConfigChanges (the write path).
	for i := uint64(10); i < 20; i++ {
		cc := proto.ConfigChange{
			Type:      proto.AddNode,
			ReplicaID: i,
			Address:   "addr",
		}
		buf := make([]byte, cc.Size())
		_, _ = cc.MarshalTo(buf)
		entries := []proto.Entry{{
			// Distinct, increasing index per change: membership.apply
			// skips logIndex <= configChangeID as replay.
			Index: p.r.log.lastIndex() + 1 + (i - 10),
			Term:  p.r.term,
			Type:  proto.EntryConfigChange,
			Cmd:   buf,
		}}
		p.processConfigChanges(entries)
	}

	close(done)

	// Verify the final state is consistent.
	for i := uint64(10); i < 20; i++ {
		if !p.IsMember(i) {
			t.Fatalf("expected replica %d to be member after config changes", i)
		}
	}
}

func TestPeer_GetMembership_ReadsFromAtomicSnapshot(t *testing.T) {
	cfg := testConfigWithID(1, 1)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Initial snapshot should reflect the membership.
	m := p.GetMembership()
	if len(m.Addresses) != 3 {
		t.Fatalf("expected 3 addresses, got %d", len(m.Addresses))
	}

	// Mutating the returned proto should not affect the peer's snapshot.
	m.Addresses[999] = "mutated"
	m2 := p.GetMembership()
	if _, ok := m2.Addresses[999]; ok {
		t.Fatal("GetMembership should return a deep copy")
	}
}

func TestPeer_IsMember_SnapshotRestore(t *testing.T) {
	// Verify that IsMember reads from the new snapshot after an
	// InstallSnapshot message replaces the membership.
	cfg := testConfigWithID(1, 1)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	if !p.IsMember(1) {
		t.Fatal("expected replica 1 to be member before snapshot")
	}
	if p.IsMember(42) {
		t.Fatal("expected replica 42 to not be member before snapshot")
	}

	// Simulate a snapshot restore with a different membership.
	newMs := proto.Membership{
		Addresses: map[uint64]string{
			42: "addr42",
			43: "addr43",
		},
	}
	snap := proto.Snapshot{
		Index:      100,
		Term:       5,
		Membership: newMs,
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		Snapshot: snap,
	}
	_ = p.Handle(msg)

	// After snapshot restore, membership should reflect the new state.
	if !p.IsMember(42) {
		t.Fatal("expected replica 42 to be member after snapshot restore")
	}
	if !p.IsMember(43) {
		t.Fatal("expected replica 43 to be member after snapshot restore")
	}
	// Old members should no longer be members (unless snapshot includes them).
	if p.IsMember(1) {
		t.Fatal("expected replica 1 to not be member after snapshot restore with different membership")
	}
}

// BenchmarkPeerGetUpdate measures the cost of a full Propose + GetUpdate +
// Commit cycle on a single-node leader. Each iteration proposes one entry,
// collects the update, and commits it.
func BenchmarkPeerGetUpdate(b *testing.B) {
	b.ReportAllocs()
	p := benchLeaderPeer(b)
	entries := []proto.Entry{{Cmd: []byte("bench-cmd")}}
	for b.Loop() {
		_, err := p.Propose(entries)
		if err != nil {
			b.Fatalf("propose failed: %v", err)
		}
		update, err := p.GetUpdate(true, 0)
		if err != nil {
			b.Fatalf("GetUpdate failed: %v", err)
		}
		if err := p.Commit(update); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// AppliedToTerm returns cached term even after compaction
// ---------------------------------------------------------------------------

func TestPeer_AppliedToTerm_AfterSnapshotRestore(t *testing.T) {
	// After a snapshot restore, AppliedToTerm should return the snapshot
	// term, not 0. This verifies the snapshot pool can always get a valid
	// term for snapshot metadata without depending on LogReader.
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Simulate snapshot restore: this sets appliedToTerm via inMemory.restore.
	snap := proto.Snapshot{
		Index: 100,
		Term:  7,
		Membership: proto.Membership{
			Addresses: map[uint64]string{1: "addr1"},
		},
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     7,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	term := p.AppliedToTerm()
	if term != 7 {
		t.Fatalf("expected AppliedToTerm 7 after snapshot restore, got %d", term)
	}
}

func TestPeer_AppliedToTerm_ZeroBeforeAnyApply(t *testing.T) {
	// A freshly created peer that has not applied any entries should
	// return 0 from AppliedToTerm.
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	term := p.AppliedToTerm()
	if term != 0 {
		t.Fatalf("expected AppliedToTerm 0 for fresh peer, got %d", term)
	}
}

// ---------------------------------------------------------------------------
// handleSnapshot retains membership when entries are retained
// ---------------------------------------------------------------------------

// TestPeer_Handle_InstallSnapshot_MatchAdoptsNewerMembership verifies the
// commit-time-membership requirement that a matching-log snapshot whose
// membership is strictly newer IS adopted. The matching branch advances the
// processed marker to the snapshot index, so config changes below it are
// never emitted as CommittedEntries and never reach processConfigChanges —
// without adoption this replica would compute quorum over a stale voting set.
func TestPeer_Handle_InstallSnapshot_MatchAdoptsNewerMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	initialMs := proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	}
	p := newTestPeer(t, cfg, proto.State{}, initialMs)
	p.r.becomeFollower(2, 2)

	for i := uint64(1); i <= 12; i++ {
		p.r.log.inmem.merge([]proto.Entry{{Index: i, Term: 2}})
	}

	// Snapshot at index 10 matches the log, and its membership carries a
	// witness added by a config change at index 8 (ConfigChangeID 8 > 0).
	snap := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			ConfigChangeID: 8,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
			Witnesses:      map[uint64]string{4: "addr4"},
		},
	}
	if err := p.Handle(proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     2,
		Snapshot: snap,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !p.membership.isWitness(4) {
		t.Fatal("expected witness 4 from the strictly newer snapshot membership")
	}
	if p.membership.configChangeID != 8 {
		t.Fatalf("expected configChangeID 8, got %d", p.membership.configChangeID)
	}
	// Remote tracking must follow the adopted membership.
	if _, ok := p.r.witnesses[4]; !ok {
		t.Fatal("expected raft witness remote for replica 4 after adoption")
	}
}

func TestPeer_Handle_InstallSnapshot_MatchRetainsMembership(t *testing.T) {
	// When a snapshot matches (same index+term in the log) and its
	// membership is NOT strictly newer (equal ConfigChangeID here), the
	// local membership wins: an equal ID means the same configuration, and
	// a locally-higher ID means config changes were already applied above
	// the snapshot index. A strictly newer snapshot IS adopted — see
	// TestPeer_Handle_InstallSnapshot_MatchAdoptsNewerMembership.
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	initialMs := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
			3: "addr3",
		},
	}
	p := newTestPeer(t, cfg, proto.State{}, initialMs)
	p.r.becomeFollower(2, 2)

	// Build a contiguous log from index 1..12, all at term 2.
	for i := uint64(1); i <= 12; i++ {
		p.r.log.inmem.merge([]proto.Entry{{Index: i, Term: 2}})
	}

	// Install a snapshot at index 10 with term 2 (matches) but with
	// a DIFFERENT membership (only 2 nodes). Since the log matches,
	// entries 11-12 are retained and membership should NOT be overwritten.
	snap := proto.Snapshot{
		Index: 10,
		Term:  2,
		Membership: proto.Membership{
			Addresses: map[uint64]string{
				1: "addr1",
				2: "addr2",
				// Node 3 is missing from snapshot membership.
			},
		},
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     2,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The log should still have entries past the snapshot.
	if p.r.log.lastIndex() < 12 {
		t.Fatalf("expected lastIndex >= 12, got %d", p.r.log.lastIndex())
	}

	// The peer's membership should retain the original 3-node membership,
	// NOT the snapshot's 2-node membership.
	m := p.GetMembership()
	if len(m.Addresses) != 3 {
		t.Fatalf("expected 3 members (original retained), got %d: %v",
			len(m.Addresses), m.Addresses)
	}
	if _, ok := m.Addresses[3]; !ok {
		t.Fatal("expected node 3 to still be in membership (retained entries may have config changes)")
	}
}

func TestPeer_Handle_InstallSnapshot_MismatchOverwritesMembership(t *testing.T) {
	// When a snapshot does NOT match (different term), the log is fully
	// replaced. The peer's membership must be overwritten with the
	// snapshot's membership.
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	initialMs := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
			3: "addr3",
		},
	}
	p := newTestPeer(t, cfg, proto.State{}, initialMs)
	p.r.becomeFollower(3, 2)

	// Build a contiguous log from index 1..12, all at term 2.
	for i := uint64(1); i <= 12; i++ {
		p.r.log.inmem.merge([]proto.Entry{{Index: i, Term: 2}})
	}

	// Install a snapshot at index 10 with term 3 (does NOT match term 2).
	snap := proto.Snapshot{
		Index: 10,
		Term:  3,
		Membership: proto.Membership{
			Addresses: map[uint64]string{
				1: "addr1",
				4: "addr4",
			},
		},
	}
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     3,
		Snapshot: snap,
	}
	err := p.Handle(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The log should be replaced. lastIndex should be the snapshot index.
	if p.r.log.lastIndex() != 10 {
		t.Fatalf("expected lastIndex 10 after mismatch restore, got %d",
			p.r.log.lastIndex())
	}

	// Membership must be overwritten with the snapshot's membership.
	m := p.GetMembership()
	if len(m.Addresses) != 2 {
		t.Fatalf("expected 2 members from snapshot, got %d: %v",
			len(m.Addresses), m.Addresses)
	}
	if _, ok := m.Addresses[4]; !ok {
		t.Fatal("expected node 4 from snapshot membership")
	}
	if _, ok := m.Addresses[3]; ok {
		t.Fatal("node 3 should not be in membership after mismatch restore")
	}
}

// --- processConfigChanges logging ---

// captureSlog installs a text handler writing to a buffer, returning the
// buffer and a cleanup function that restores the previous logger.
func captureSlog(level slog.Level) (*bytes.Buffer, func()) {
	var buf bytes.Buffer
	prev := slog.Default()
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
	return &buf, func() { slog.SetDefault(prev) }
}

func TestPeer_processConfigChanges_LogsUnmarshalError(t *testing.T) {
	buf, cleanup := captureSlog(slog.LevelDebug)
	defer cleanup()

	cfg := testConfigWithID(1, 1)
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Create an entry with corrupt config change data.
	entries := []proto.Entry{
		{
			Index: 2,
			Term:  1,
			Type:  proto.EntryConfigChange,
			Cmd:   []byte{0xFF, 0xFF}, // invalid binary, will fail UnmarshalFrom
		},
	}
	p.processConfigChanges(entries)

	logOutput := buf.String()
	if !bytes.Contains([]byte(logOutput), []byte("config change unmarshal failed")) {
		t.Errorf("expected 'config change unmarshal failed' in log, got:\n%s", logOutput)
	}
	if !bytes.Contains([]byte(logOutput), []byte("shard_id=1")) {
		t.Errorf("expected structured shard_id in log, got:\n%s", logOutput)
	}
	if !bytes.Contains([]byte(logOutput), []byte("level=ERROR")) {
		t.Errorf("expected ERROR level for unmarshal failure, got:\n%s", logOutput)
	}
}

func TestPeer_processConfigChanges_LogsApplyError(t *testing.T) {
	buf, cleanup := captureSlog(slog.LevelDebug)
	defer cleanup()

	cfg := testConfigWithID(1, 1)
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// First, apply a valid config change at index 2.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 5,
		Address:   "addr5",
	}
	ccBuf := make([]byte, cc.Size())
	_, _ = cc.MarshalTo(ccBuf)

	entries := []proto.Entry{
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: ccBuf},
	}
	p.processConfigChanges(entries)

	if !p.IsMember(5) {
		t.Fatal("replica 5 should be a member after first apply")
	}

	// Remove replica 5, then re-add it at a higher index. The replay guard
	// (logIndex <= configChangeID) does not cover either entry, so the
	// Removed-tombstone refusal is surfaced and logged. A tombstone is
	// replicated state every replica evaluates identically, which is what
	// makes refusing it safe — unlike a duplicate add, which apply converges
	// on rather than skipping (see membership.go).
	rm := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 5}
	rmBuf := make([]byte, rm.Size())
	_, _ = rm.MarshalTo(rmBuf)
	p.processConfigChanges([]proto.Entry{
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: rmBuf},
	})
	if p.IsMember(5) {
		t.Fatal("replica 5 should not be a member after the removal")
	}

	entries[0].Index = 4
	buf.Reset()
	p.processConfigChanges(entries)

	logOutput := buf.String()
	if !bytes.Contains([]byte(logOutput), []byte("config change apply skipped")) {
		t.Errorf("expected 'config change apply skipped' in log, got:\n%s", logOutput)
	}
	// The apply-skip is now logged at WARN: the Host membership API
	// pre-validates against current membership before proposing, so any skip
	// on the leader's own freshly proposed entry is unexpected and should be
	// visible to operators rather than buried at DEBUG.
	if !bytes.Contains([]byte(logOutput), []byte("level=WARN")) {
		t.Errorf("expected WARN level for apply skip, got:\n%s", logOutput)
	}
}

// TestPeer_processConfigChanges_UndecodableEntryFaultsTheShard pins the runtime
// decode site as fail-CLOSED.
//
// Logging and skipping an undecodable committed config entry, and carrying on
// with the entries after it, is not a tolerant no-op — a config change that commits and is then dropped on one
// replica leaves that replica computing quorum over a membership the rest of the
// shard does not hold, and a peer that catches up by SNAPSHOT instead of by log
// replay gets the change, so the two genuinely diverge. Recovery already refuses
// these bytes (ReconstructMembership / bootstrap.go); the runtime failing open
// while recovery fails closed over the same input would be the inconsistency.
func TestPeer_processConfigChanges_UndecodableEntryFaultsTheShard(t *testing.T) {
	cfg := testConfigWithID(1, 1)
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// A valid change AFTER the corrupt one, so the test can also show the scan
	// stops rather than folding past a membership it cannot reconstruct.
	ccBuf := marshalConfigChange(t, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 7, Address: "addr7",
	})
	entries := []proto.Entry{
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte{0xFF}},
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: ccBuf},
	}
	err := p.processConfigChanges(entries)
	if !errors.Is(err, ErrCorruptConfigEntry) {
		t.Fatalf("processConfigChanges returned %v, want ErrCorruptConfigEntry: an "+
			"undecodable COMMITTED config entry must fault the shard, not be skipped",
			err)
	}
	if p.IsMember(7) {
		t.Fatal("the scan folded a config change ABOVE the entry it could not " +
			"decode: every change after the corrupt one is defined relative to a " +
			"membership this replica cannot build")
	}
}

// TestPeer_processConfigChanges_DeterministicRefusalIsStillIgnored is the
// control. The fault above must key on the DECODE, not on "membership.apply
// returned an error": an apply refusal is evaluated identically by every replica
// (a Removed tombstone re-add here), so it is agreement rather than corruption,
// and faulting on it would kill shards over an outcome the whole cluster shares.
func TestPeer_processConfigChanges_DeterministicRefusalIsStillIgnored(t *testing.T) {
	cfg := testConfigWithID(1, 1)
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	add7 := marshalConfigChange(t, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 7, Address: "addr7",
	})
	remove7 := marshalConfigChange(t, proto.ConfigChange{
		Type: proto.RemoveNode, ReplicaID: 7,
	})
	add9 := marshalConfigChange(t, proto.ConfigChange{
		Type: proto.AddNode, ReplicaID: 9, Address: "addr9",
	})

	if err := p.processConfigChanges([]proto.Entry{
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: add7},
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: remove7},
		// Refused: replica 7 carries a Removed tombstone now.
		{Index: 4, Term: 1, Type: proto.EntryConfigChange, Cmd: add7},
		{Index: 5, Term: 1, Type: proto.EntryConfigChange, Cmd: add9},
	}); err != nil {
		t.Fatalf("processConfigChanges returned %v for a deterministic apply "+
			"refusal: only an undecodable payload may fault the shard", err)
	}
	if p.IsMember(7) {
		t.Fatal("the Removed tombstone was overridden by a re-add")
	}
	if !p.IsMember(9) {
		t.Fatal("the scan stopped at a deterministic refusal instead of continuing")
	}
}

func TestPeer_processConfigChanges_SkipsEmptyCmd(t *testing.T) {
	cfg := testConfigWithID(1, 1)
	ms := singleNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// An empty Cmd config change entry is skipped silently.
	entries := []proto.Entry{
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: nil},
	}
	p.processConfigChanges(entries)

	// Membership should be unchanged (only replica 1 from initial setup).
	m := p.GetMembership()
	if len(m.Addresses) != 1 {
		t.Fatalf("expected 1 member, got %d", len(m.Addresses))
	}
}

// testConfigChangeCmd returns a VALID marshaled ConfigChange payload, for tests
// whose subject is the propose-time bookkeeping (pendingConfigChange, batch
// rejection, entry-type masking) rather than the change itself.
//
// It exists because handleLeaderPropose REJECTS a config-change payload that
// does not decode (raft.validateProposedConfigChange), so placeholder bytes like
// []byte("cc1") cannot stand in for one. Passing such a payload through would
// let a wire-forwarded Propose put a membership change nobody had validated
// into the log of every replica.
func testConfigChangeCmd(replicaID uint64) []byte {
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: replicaID,
		Address:   fmt.Sprintf("cc-addr-%d", replicaID),
	}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		panic(err)
	}
	return buf
}

// marshalConfigChange is a test helper that marshals a ConfigChange to bytes.
func marshalConfigChange(t *testing.T, cc proto.ConfigChange) []byte {
	t.Helper()
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		t.Fatalf("failed to marshal config change: %v", err)
	}
	return buf
}

// --- Config Change Rollback on Log Truncation (MC1) ---

// TestPeer_Handle_ConfigChangeRollbackOnTruncation verifies that when a
// new leader overwrites log entries via conflict resolution, config
// changes from the truncated entries are rolled back. Per PhD thesis
// 4.1: "a log entry for a configuration change can be removed (if
// leadership changes); in this case, a server must be prepared to fall
// back to the previous configuration in its log."
// TestPeer_Handle_UncommittedConfigChangeNotApplied verifies commit-time
// membership semantics on a follower: a config change entry that is
// appended but not committed never mutates membership or remote tracking,
// so log truncation that overwrites it requires no rollback. When a
// replacement config change commits, membership reflects only the
// replacement.
func TestPeer_Handle_UncommittedConfigChangeNotApplied(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Seed the log with an initial entry at index 1, term 1.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	// Follower receives AddNode(4) at index 2 from leader L1, NOT committed.
	addNode4 := marshalConfigChange(t, proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	})
	msg1 := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
		Commit:   0,
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: addNode4},
		},
	}
	if err := p.Handle(msg1); err != nil {
		t.Fatalf("unexpected error on first Replicate: %v", err)
	}

	// Uncommitted: membership and remote tracking must be untouched.
	if p.membership.isVoter(4) {
		t.Fatal("uncommitted config change must not mutate membership")
	}
	if p.IsMember(4) {
		t.Fatal("expected IsMember(4) to be false while the change is uncommitted")
	}
	if _, ok := p.r.remotes[4]; ok {
		t.Fatal("uncommitted config change must not create a remote")
	}

	// New leader L2 (term 2) overwrites index 2 with AddNode(6) and
	// commits it. The truncated AddNode(4) was never applied, so no
	// rollback is involved.
	addNode6 := marshalConfigChange(t, proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 6,
		Address:   "addr6",
	})
	msg2 := proto.Message{
		Type:     proto.Replicate,
		From:     3,
		To:       1,
		ShardID:  1,
		Term:     2,
		LogIndex: 1,
		LogTerm:  1,
		Commit:   2,
		Entries: []proto.Entry{
			{Index: 2, Term: 2, Type: proto.EntryConfigChange, Cmd: addNode6},
		},
	}
	if err := p.Handle(msg2); err != nil {
		t.Fatalf("unexpected error on conflicting Replicate: %v", err)
	}
	runUpdateCycles(t, p)

	// Only the committed replacement is applied.
	if p.membership.isVoter(4) {
		t.Fatal("truncated config change must not be in membership")
	}
	if !p.membership.isVoter(6) {
		t.Fatal("expected committed replacement AddNode(6) to be applied")
	}
	if _, ok := p.r.remotes[6]; !ok {
		t.Fatal("expected remote for replica 6 after its addition committed")
	}
	m := p.GetMembership()
	if len(m.Addresses) != 4 {
		t.Fatalf("expected 4 voters (1,2,3,6), got %d", len(m.Addresses))
	}
}

// TestPeer_Handle_CommittedConfigChangeSurvivesTruncation verifies that a
// COMMITTED config change is applied to membership and survives later log
// conflicts, while an uncommitted one in the same batch is never applied.
func TestPeer_Handle_CommittedConfigChangeSurvivesTruncation(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Seed log with initial entry.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
	})

	// Follower receives AddNode(4) at index 2 (committed) and AddNode(5)
	// at index 3 (uncommitted).
	addNode4 := marshalConfigChange(t, proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	})
	addNode5 := marshalConfigChange(t, proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 5,
		Address:   "addr5",
	})
	msg1 := proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     1,
		LogIndex: 1,
		LogTerm:  1,
		Commit:   2, // commit up to index 2 (AddNode(4) only)
		Entries: []proto.Entry{
			{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: addNode4},
			{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: addNode5},
		},
	}
	if err := p.Handle(msg1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	runUpdateCycles(t, p)

	// Committed AddNode(4) applied; uncommitted AddNode(5) not applied.
	if !p.membership.isVoter(4) {
		t.Fatal("expected committed AddNode(4) to be applied")
	}
	if p.membership.isVoter(5) {
		t.Fatal("uncommitted AddNode(5) must not be applied")
	}

	// New leader overwrites uncommitted index 3 with a normal entry.
	// The committed AddNode(4) must survive.
	msg2 := proto.Message{
		Type:     proto.Replicate,
		From:     3,
		To:       1,
		ShardID:  1,
		Term:     2,
		LogIndex: 2,
		LogTerm:  1,
		Commit:   2,
		Entries: []proto.Entry{
			{Index: 3, Term: 2, Type: proto.EntryNormal, Cmd: []byte("data")},
		},
	}
	if err := p.Handle(msg2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	runUpdateCycles(t, p)

	if !p.membership.isVoter(4) {
		t.Fatal("expected committed AddNode(4) to survive truncation of later entries")
	}
	if p.membership.isVoter(5) {
		t.Fatal("truncated AddNode(5) must not be in membership")
	}
	m := p.GetMembership()
	if len(m.Addresses) != 4 {
		t.Fatalf("expected 4 voters (1,2,3,4), got %d", len(m.Addresses))
	}
}

// --- GetPiggybackUpdate ---

func TestPeer_GetPiggybackUpdate_ReturnsCommittedEntries(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Merge 3 entries into the in-memory log.
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	})
	p.r.log.committed = 3
	// Mark the entries durable. GetPiggybackUpdate clamps the applied range to
	// the durable marker (savedTo); without this the piggyback would (correctly)
	// withhold the not-yet-fsynced tail and return nothing.
	p.r.log.inmem.savedTo = 3
	// processed starts at 0 from newRaftLog, so hasEntriesToApply = true.

	update, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.CommittedEntries) != 3 {
		t.Fatalf("expected 3 committed entries, got %d", len(update.CommittedEntries))
	}
	for i, e := range update.CommittedEntries {
		expectedIdx := uint64(i + 1)
		if e.Index != expectedIdx {
			t.Fatalf("entry %d: expected index %d, got %d", i, expectedIdx, e.Index)
		}
		if e.Term != 1 {
			t.Fatalf("entry %d: expected term 1, got %d", i, e.Term)
		}
	}
	if update.ShardID != cfg.ShardID {
		t.Fatalf("expected ShardID %d, got %d", cfg.ShardID, update.ShardID)
	}
	if update.ReplicaID != cfg.ReplicaID {
		t.Fatalf("expected ReplicaID %d, got %d", cfg.ReplicaID, update.ReplicaID)
	}
}

func TestPeer_GetPiggybackUpdate_EmptyWhenNoEntries(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{}, singleNodeMembership())

	// No entries merged; committed == processed == 0.
	update, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if update.CommittedEntries != nil {
		t.Fatalf("expected nil CommittedEntries, got %d entries", len(update.CommittedEntries))
	}
	if update.ShardID != 0 {
		t.Fatalf("expected zero ShardID in empty update, got %d", update.ShardID)
	}
}

func TestPeer_GetPiggybackUpdate_DrainsMessages(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Merge 1 entry and set committed so there are entries to apply.
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})
	p.r.log.committed = 1
	p.r.log.inmem.savedTo = 1 // durable, so the piggyback applies it

	// Add 2 outbound messages.
	p.r.msgs = append(p.r.msgs, proto.Message{Type: proto.Heartbeat, From: 1})
	p.r.msgs = append(p.r.msgs, proto.Message{Type: proto.HeartbeatResp, From: 1})

	update, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(update.Messages) != 2 {
		t.Fatalf("expected 2 messages in update, got %d", len(update.Messages))
	}
	// Messages must have been drained from r.msgs.
	if len(p.r.msgs) != 0 {
		t.Fatalf("expected r.msgs to be empty after drain, got %d", len(p.r.msgs))
	}
}

// TestPeer_GetPiggybackUpdate_DefersPrevState pins the ownership of the
// prevState advance: extraction does not perform it, the persist
// acknowledgement does. The old name (UpdatesPrevState) described the
// behaviour that made a failed piggyback SaveState claim an un-fsynced
// term/vote as durable.
func TestPeer_GetPiggybackUpdate_DefersPrevState(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Set raft state that differs from the zero prevState.
	p.r.term = 5
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 5}})
	p.r.log.committed = 1
	p.r.log.inmem.savedTo = 1   // durable, so the piggyback produces an update
	p.prevState = proto.State{} // zero

	if !p.hasStateChanged() {
		t.Fatal("expected hasStateChanged=true before GetPiggybackUpdate")
	}

	update, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Extracting the update must NOT advance prevState. The hard state it
	// carries has not been fsynced yet, and treating it as recorded here
	// would drop it from the next cycle if the persist fails -- the whole
	// reason the advance moved to NotePiggybackPersisted.
	if !p.hasStateChanged() {
		t.Fatal("expected hasStateChanged=true until the update is persisted")
	}

	// Acknowledging the persist is what records it.
	p.NotePiggybackPersisted(update.State)
	if p.hasStateChanged() {
		t.Fatal("expected hasStateChanged=false after NotePiggybackPersisted")
	}
}

func TestPeer_GetPiggybackUpdate_DoesNotTouchEntriesToSave(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Merge 3 entries (all unsaved since savedTo=0 < index 1..3).
	p.r.log.inmem.merge([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})
	p.r.log.committed = 2 // only 2 committed, but all 3 unsaved

	_, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// entriesToSave must still return all 3 unsaved entries.
	toSave := p.r.log.entriesToSave()
	if len(toSave) != 3 {
		t.Fatalf("expected 3 entries to save, got %d", len(toSave))
	}
}

func TestPeer_GetPiggybackUpdate_DoesNotTouchReadStates(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Merge 1 entry and set committed.
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})
	p.r.log.committed = 1
	p.r.log.inmem.savedTo = 1 // durable, so the piggyback produces an update

	// Add a ReadyToRead state.
	p.r.readStates = append(p.r.readStates, proto.ReadyToRead{Key: 42, Index: 1})

	_, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// readStates must remain untouched.
	if len(p.r.readStates) != 1 {
		t.Fatalf("expected 1 readState, got %d", len(p.r.readStates))
	}
	if p.r.readStates[0].Key != 42 {
		t.Fatalf("expected readState key 42, got %d", p.r.readStates[0].Key)
	}
}

func TestPeer_GetPiggybackUpdate_DoesNotTouchSnapshot(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	// Set a pending snapshot flag but no entries to apply.
	p.hasPendingSnapshot = true

	update, err := p.GetPiggybackUpdate(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With no entries to apply, update should be empty.
	if update.CommittedEntries != nil {
		t.Fatalf("expected nil CommittedEntries, got %d", len(update.CommittedEntries))
	}
	if update.ShardID != 0 {
		t.Fatalf("expected zero ShardID in empty update, got %d", update.ShardID)
	}

	// hasPendingSnapshot must remain true.
	if !p.hasPendingSnapshot {
		t.Fatal("expected hasPendingSnapshot to remain true")
	}
}

// ---------------------------------------------------------------------------
// SetProcessedIndex tests
// ---------------------------------------------------------------------------

// contiguousEntries returns entries [1, last] all at term 1.
func contiguousEntries(last uint64) []proto.Entry {
	entries := make([]proto.Entry, 0, last)
	for i := uint64(1); i <= last; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1})
	}
	return entries
}

// newTestPeerWithLog builds a peer over a LogReader that actually HOLDS the
// entries the recovered hard state claims. A hard state whose Commit runs
// past the last recovered entry is a corrupt/incomplete recovery and is
// clamped by newRaftLog, so a fixture that omits the entries does not test
// what it appears to.
func newTestPeerWithLog(t *testing.T, cfg config.Config, entries []proto.Entry, state proto.State, ms proto.Membership) *Peer {
	t.Helper()
	lr := newTestLogReader(entries, state)
	lr.membership = ms
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}
	return p
}

func TestPeer_SetProcessedIndex_SetsLogProcessedMarker(t *testing.T) {
	cfg := testConfig()
	// The marker is clamped to the recovered commit index, and the recovered
	// commit index is itself clamped to what the log actually holds, so the
	// log must really carry entries 1..100.
	p := newTestPeerWithLog(t, cfg, contiguousEntries(100),
		proto.State{Term: 1, Commit: 100}, singleNodeMembership())

	if p.r.log.committed != 100 {
		t.Fatalf("fixture: expected committed 100, got %d", p.r.log.committed)
	}
	// Initial processed should be 0.
	if p.r.log.processed != 0 {
		t.Fatalf("expected initial processed 0, got %d", p.r.log.processed)
	}

	// Set processed index to a higher value.
	p.SetProcessedIndex(100)

	if p.r.log.processed != 100 {
		t.Fatalf("expected processed 100, got %d", p.r.log.processed)
	}
}

func TestPeer_SetProcessedIndex_ZeroValue(t *testing.T) {
	cfg := testConfig()
	p := newTestPeerWithLog(t, cfg, contiguousEntries(50),
		proto.State{Term: 1, Commit: 50}, singleNodeMembership())

	// Set to a non-zero value first.
	p.SetProcessedIndex(50)
	if p.r.log.processed != 50 {
		t.Fatalf("expected processed 50, got %d", p.r.log.processed)
	}

	// Setting to 0 should work.
	p.SetProcessedIndex(0)

	if p.r.log.processed != 0 {
		t.Fatalf("expected processed 0 after reset, got %d", p.r.log.processed)
	}
}

// TestPeer_SetProcessedIndex_RecordsAppliedClamp: an applied index ahead of the
// recovered commit index is clamped and recorded for the Host to log on its
// own logger (Peer.AppliedClamp); an index within it records nothing.
func TestPeer_SetProcessedIndex_RecordsAppliedClamp(t *testing.T) {
	cfg := testConfig()
	p := newTestPeerWithLog(t, cfg, contiguousEntries(50),
		proto.State{Term: 1, Commit: 50}, singleNodeMembership())

	if _, err := p.SetProcessedIndex(40); err != nil {
		t.Fatalf("SetProcessedIndex(40): %v", err)
	}
	if c := p.AppliedClamp(); c.Applied() {
		t.Fatalf("AppliedClamp = %+v for an index within the commit index, want none", c)
	}

	applied, err := p.SetProcessedIndex(70)
	if err != nil {
		t.Fatalf("SetProcessedIndex(70): %v", err)
	}
	if applied != 50 {
		t.Fatalf("applied = %d, want 50 (clamped to the recovered commit index)", applied)
	}
	want := AppliedClamp{SMApplied: 70, Committed: 50}
	if c := p.AppliedClamp(); c != want || !c.Applied() {
		t.Fatalf("AppliedClamp = %+v, want %+v", c, want)
	}
}

// ---------------------------------------------------------------------------
// SetOnCampaign tests
// ---------------------------------------------------------------------------

func TestPeer_SetOnCampaign_CallbackInvoked(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false) // Disable PreVote to trigger onCampaign directly.
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	var callbackShardID, callbackReplicaID, callbackTerm uint64
	callbackCalled := false

	p.SetOnCampaign(func(shardID, replicaID, term uint64) {
		callbackCalled = true
		callbackShardID = shardID
		callbackReplicaID = replicaID
		callbackTerm = term
	})

	// Tick past election timeout to trigger campaign.
	for range 30 {
		p.Tick()
		if callbackCalled {
			break
		}
	}

	if !callbackCalled {
		t.Fatal("expected onCampaign callback to be invoked")
	}
	if callbackShardID != cfg.ShardID {
		t.Errorf("expected shardID %d, got %d", cfg.ShardID, callbackShardID)
	}
	if callbackReplicaID != cfg.ReplicaID {
		t.Errorf("expected replicaID %d, got %d", cfg.ReplicaID, callbackReplicaID)
	}
	// The term passed to onCampaign is the term at the time startElection
	// is called, before the term is incremented by becomeCandidate.
	// It may be 0 if the peer started with term 0.
	_ = callbackTerm // Term is valid regardless of value.
}

func TestPeer_SetOnCampaign_NilCallbackSafe(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Explicitly set callback to nil.
	p.SetOnCampaign(nil)

	// Tick past election timeout. Should not panic.
	for range 30 {
		p.Tick()
	}
	// Test passes if no panic occurred.
}

// ---------------------------------------------------------------------------
// DrainMessages tests
// ---------------------------------------------------------------------------

func TestPeer_DrainMessages_ExtractsAllMessages(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Manually populate the raft message buffer.
	p.r.msgs = append(p.r.msgs,
		proto.Message{Type: proto.Heartbeat, To: 2},
		proto.Message{Type: proto.Replicate, To: 3},
		proto.Message{Type: proto.RequestVoteResp, To: 2},
	)

	msgs := p.DrainMessages()

	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	// After drain, raft's msg buffer should be empty.
	if len(p.r.msgs) != 0 {
		t.Fatalf("expected raft msgs to be drained, got %d", len(p.r.msgs))
	}
}

func TestPeer_DrainMessages_EmptyBufferReturnsNil(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Drain any residual messages from leader election.
	_ = p.DrainMessages()

	// Drain again from empty buffer.
	msgs := p.DrainMessages()

	if msgs != nil {
		t.Fatalf("expected nil from empty drain, got %d messages", len(msgs))
	}
}

func TestPeer_DrainMessages_DoesNotAffectReadStates(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Add a ReadIndex to create a read state.
	err := p.ReadIndex(55)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}

	// Verify readStates exist before drain.
	if len(p.r.readStates) == 0 {
		t.Fatal("expected readStates to exist before drain")
	}

	_ = p.DrainMessages()

	// readStates should remain after draining messages.
	if len(p.r.readStates) == 0 {
		t.Fatal("expected readStates to remain after DrainMessages")
	}
}

// ---------------------------------------------------------------------------
// DrainFreeOrderMessages tests
// ---------------------------------------------------------------------------

func TestPeer_DrainFreeOrderMessages_ExtractsOnlyFreeOrderTypes(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Populate with a mix of free-order and ordered messages.
	p.r.msgs = append(p.r.msgs,
		proto.Message{Type: proto.Heartbeat, To: 2},       // free-order
		proto.Message{Type: proto.ReadIndexResp, To: 2},   // ordered
		proto.Message{Type: proto.Replicate, To: 3},       // free-order
		proto.Message{Type: proto.ReplicateResp, To: 3},   // ordered
		proto.Message{Type: proto.RequestVoteResp, To: 2}, // ordered
		proto.Message{Type: proto.Ping, To: 3},            // free-order
	)

	freeOrderMsgs := p.DrainFreeOrderMessages()

	// Should extract Heartbeat, Replicate, Ping (3 messages).
	if len(freeOrderMsgs) != 3 {
		t.Fatalf("expected 3 free-order messages, got %d", len(freeOrderMsgs))
	}

	// Verify types of extracted messages.
	for _, m := range freeOrderMsgs {
		if !proto.IsFreeOrderMessage(m.Type) {
			t.Errorf("unexpected non-free-order message type: %d", m.Type)
		}
	}

	// Ordered messages should remain in the raft buffer.
	if len(p.r.msgs) != 3 {
		t.Fatalf("expected 3 ordered messages to remain, got %d", len(p.r.msgs))
	}

	// Verify remaining messages are ordered types.
	for _, m := range p.r.msgs {
		if proto.IsFreeOrderMessage(m.Type) {
			t.Errorf("unexpected free-order message in remaining: %d", m.Type)
		}
	}
}

func TestPeer_DrainFreeOrderMessages_EmptyBufferReturnsNil(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Drain any residual messages.
	_ = p.DrainMessages()

	// Drain free-order messages from empty buffer.
	msgs := p.DrainFreeOrderMessages()

	if msgs != nil {
		t.Fatalf("expected nil from empty drain, got %d messages", len(msgs))
	}
}

func TestPeer_DrainFreeOrderMessages_AllOrderedReturnsNil(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Populate with only ordered messages. HeartbeatResp is left out: it
	// is free-order here, the hard state being unchanged (#390).
	p.r.msgs = append(p.r.msgs,
		proto.Message{Type: proto.ReadIndexResp, To: 2},
		proto.Message{Type: proto.ReplicateResp, To: 3},
		proto.Message{Type: proto.RequestVoteResp, To: 2},
	)

	originalLen := len(p.r.msgs)
	freeOrderMsgs := p.DrainFreeOrderMessages()

	if freeOrderMsgs != nil {
		t.Fatalf("expected nil when no free-order messages, got %d", len(freeOrderMsgs))
	}

	// All messages should remain.
	if len(p.r.msgs) != originalLen {
		t.Fatalf("expected %d messages to remain, got %d", originalLen, len(p.r.msgs))
	}
}

func TestPeer_DrainFreeOrderMessages_AllFreeOrderDrainsAll(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Populate with only free-order messages.
	p.r.msgs = append(p.r.msgs,
		proto.Message{Type: proto.Heartbeat, To: 2},
		proto.Message{Type: proto.Replicate, To: 3},
		proto.Message{Type: proto.Ping, To: 2},
	)

	freeOrderMsgs := p.DrainFreeOrderMessages()

	if len(freeOrderMsgs) != 3 {
		t.Fatalf("expected 3 free-order messages, got %d", len(freeOrderMsgs))
	}

	// Buffer should be empty now.
	if len(p.r.msgs) != 0 {
		t.Fatalf("expected 0 messages to remain, got %d", len(p.r.msgs))
	}
}

// ---------------------------------------------------------------------------
// DrainReadStatesOnly tests
// ---------------------------------------------------------------------------

func TestPeer_DrainReadStatesOnly_ExtractsReadStates(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Add multiple read states.
	p.r.readStates = append(p.r.readStates,
		proto.ReadyToRead{Key: 1, Index: 10},
		proto.ReadyToRead{Key: 2, Index: 20},
	)

	readStates := p.DrainReadStatesOnly()

	if len(readStates) != 2 {
		t.Fatalf("expected 2 readStates, got %d", len(readStates))
	}
	if readStates[0].Key != 1 || readStates[1].Key != 2 {
		t.Errorf("unexpected readState keys: %d, %d", readStates[0].Key, readStates[1].Key)
	}

	// After drain, raft's readStates should be nil.
	if p.r.readStates != nil {
		t.Fatalf("expected raft readStates to be nil, got %d", len(p.r.readStates))
	}
}

func TestPeer_DrainReadStatesOnly_EmptyReturnsNil(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Drain any residual readStates.
	_ = p.DrainReadStatesOnly()

	// Drain again from empty.
	readStates := p.DrainReadStatesOnly()

	if readStates != nil {
		t.Fatalf("expected nil from empty drain, got %d", len(readStates))
	}
}

func TestPeer_DrainReadStatesOnly_DoesNotTouchMessages(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Add messages and readStates.
	p.r.msgs = append(p.r.msgs,
		proto.Message{Type: proto.Heartbeat, To: 2},
		proto.Message{Type: proto.ReplicateResp, To: 3},
	)
	p.r.readStates = append(p.r.readStates,
		proto.ReadyToRead{Key: 42, Index: 100},
	)

	readStates := p.DrainReadStatesOnly()

	// ReadStates should be extracted.
	if len(readStates) != 1 {
		t.Fatalf("expected 1 readState, got %d", len(readStates))
	}

	// Messages should remain untouched.
	if len(p.r.msgs) != 2 {
		t.Fatalf("expected 2 messages to remain, got %d", len(p.r.msgs))
	}
}

// ---------------------------------------------------------------------------
// DiagRaftState tests
// ---------------------------------------------------------------------------

func TestPeer_DiagRaftState_LeaderState(t *testing.T) {
	p := newTestLeaderPeer(t)

	term, committed, processed, lastIdx, msgs, state := p.DiagRaftState()

	if term == 0 {
		t.Error("expected non-zero term for leader")
	}
	if state == "" {
		t.Error("expected non-empty state string")
	}
	if !bytes.Contains([]byte(state), []byte("leader")) {
		t.Errorf("expected state to contain 'leader', got %s", state)
	}
	// Should include heartbeat tick info.
	if !bytes.Contains([]byte(state), []byte("hb=")) {
		t.Errorf("expected state to contain 'hb=' for leader, got %s", state)
	}
	// Should include election tick info.
	if !bytes.Contains([]byte(state), []byte("et=")) {
		t.Errorf("expected state to contain 'et=' for leader, got %s", state)
	}

	// Verify numeric fields are reasonable.
	_ = committed
	_ = processed
	_ = lastIdx
	_ = msgs
}

func TestPeer_DiagRaftState_FollowerState(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	_, _, _, _, _, state := p.DiagRaftState()

	if !bytes.Contains([]byte(state), []byte("follower")) {
		t.Errorf("expected state to contain 'follower', got %s", state)
	}
	if !bytes.Contains([]byte(state), []byte("et=")) {
		t.Errorf("expected state to contain 'et=' for follower, got %s", state)
	}
}

func TestPeer_DiagRaftState_CandidateState(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past election timeout to become candidate.
	for range 200 {
		p.Tick()
		if p.r.state == candidate {
			break
		}
	}

	if p.r.state != candidate {
		t.Fatal("peer did not become candidate after 200 ticks")
	}

	_, _, _, _, _, state := p.DiagRaftState()

	if state != "candidate" {
		t.Errorf("expected state 'candidate', got %s", state)
	}
}

func TestPeer_DiagRaftState_PreCandidateState(t *testing.T) {
	cfg := testConfig()
	// PreVote is enabled by default after SetDefaults.
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Tick past election timeout to become preCandidate.
	for range 200 {
		p.Tick()
		if p.r.state == preCandidate {
			break
		}
	}

	if p.r.state != preCandidate {
		t.Fatal("peer did not become preCandidate after 200 ticks")
	}

	_, _, _, _, _, state := p.DiagRaftState()

	if state != "preCandidate" {
		t.Errorf("expected state 'preCandidate', got %s", state)
	}
}

func TestPeer_DiagRaftState_ReturnsCorrectMetrics(t *testing.T) {
	p := newTestLeaderPeer(t)

	// Propose entries to modify the log state.
	_, err := p.Propose([]proto.Entry{{Cmd: []byte("test")}})
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	term, committed, processed, lastIdx, msgs, _ := p.DiagRaftState()

	// Term should match the leader's current term.
	if term != p.r.term {
		t.Errorf("expected term %d, got %d", p.r.term, term)
	}

	// Committed should match log's committed index.
	if committed != p.r.log.committed {
		t.Errorf("expected committed %d, got %d", p.r.log.committed, committed)
	}

	// Processed should match log's processed index.
	if processed != p.r.log.processed {
		t.Errorf("expected processed %d, got %d", p.r.log.processed, processed)
	}

	// LastIdx should match log's lastIndex.
	if lastIdx != p.r.log.lastIndex() {
		t.Errorf("expected lastIdx %d, got %d", p.r.log.lastIndex(), lastIdx)
	}

	// Msgs should match the pending message count.
	if msgs != len(p.r.msgs) {
		t.Errorf("expected msgs %d, got %d", len(p.r.msgs), msgs)
	}
}

// ---------------------------------------------------------------------------
// DiagRemotes tests
// ---------------------------------------------------------------------------

func TestPeer_DiagRemotes_LeaderReturnsRemoteInfo(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	remotesStr := p.DiagRemotes()

	if remotesStr == "not leader" {
		t.Fatal("expected remote info, got 'not leader'")
	}

	// Should contain info for remotes 2 and 3.
	if !bytes.Contains([]byte(remotesStr), []byte("r2[")) {
		t.Errorf("expected info for remote 2, got: %s", remotesStr)
	}
	if !bytes.Contains([]byte(remotesStr), []byte("r3[")) {
		t.Errorf("expected info for remote 3, got: %s", remotesStr)
	}

	// Should contain match, next, state fields.
	if !bytes.Contains([]byte(remotesStr), []byte("match=")) {
		t.Errorf("expected 'match=' in output, got: %s", remotesStr)
	}
	if !bytes.Contains([]byte(remotesStr), []byte("next=")) {
		t.Errorf("expected 'next=' in output, got: %s", remotesStr)
	}
	if !bytes.Contains([]byte(remotesStr), []byte("state=")) {
		t.Errorf("expected 'state=' in output, got: %s", remotesStr)
	}
}

func TestPeer_DiagRemotes_NotLeaderReturnsMessage(t *testing.T) {
	cfg := testConfig()
	ms := threeNodeMembership()
	p := newTestPeer(t, cfg, proto.State{}, ms)

	remotesStr := p.DiagRemotes()

	if remotesStr != "not leader" {
		t.Errorf("expected 'not leader', got: %s", remotesStr)
	}
}

func TestPeer_DiagRemotes_SingleNodeLeaderNoRemotes(t *testing.T) {
	p := newTestLeaderPeer(t)

	remotesStr := p.DiagRemotes()

	// Single-node leader has no remotes, so output should be empty.
	if remotesStr != "" {
		t.Errorf("expected empty string for single-node leader, got: %s", remotesStr)
	}
}

func TestPeer_DiagRemotes_ContainsRemoteState(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	remotesStr := p.DiagRemotes()

	// Remote state should be one of: probe, replicate, snapshot.
	validStates := []string{"probe", "replicate", "snapshot"}
	foundValidState := false
	for _, s := range validStates {
		if bytes.Contains([]byte(remotesStr), []byte("state="+s)) {
			foundValidState = true
			break
		}
	}
	if !foundValidState {
		t.Errorf("expected valid state in output, got: %s", remotesStr)
	}

	// Should contain paused status.
	if !bytes.Contains([]byte(remotesStr), []byte("paused=")) {
		t.Errorf("expected 'paused=' in output, got: %s", remotesStr)
	}

	// Should contain active status.
	if !bytes.Contains([]byte(remotesStr), []byte("active=")) {
		t.Errorf("expected 'active=' in output, got: %s", remotesStr)
	}

	// Should contain inflights count.
	if !bytes.Contains([]byte(remotesStr), []byte("inflights=")) {
		t.Errorf("expected 'inflights=' in output, got: %s", remotesStr)
	}
}

// ---------------------------------------------------------------------------
// RateLimited tests
// ---------------------------------------------------------------------------

func TestPeer_RateLimited_FalseWhenDisabled(t *testing.T) {
	// Default config has MaxInMemLogSize = 0, which disables rate limiting.
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	if p.RateLimited() {
		t.Error("expected RateLimited to be false when rate limiting is disabled")
	}
}

func TestPeer_RateLimited_TrueWhenOverLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxInMemLogSize = 100 // Enable rate limiting with small limit.
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = singleNodeMembership()
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	// Initially not rate limited.
	if p.RateLimited() {
		t.Error("expected not rate limited initially")
	}

	// Increase the in-memory size beyond the limit.
	if p.r.rl != nil {
		p.r.rl.Increase(200)
	}

	if !p.RateLimited() {
		t.Error("expected RateLimited to be true when over limit")
	}
}

// ---------------------------------------------------------------------------
// RateLimiter tests
// ---------------------------------------------------------------------------

func TestPeer_RateLimiter_ReturnsNilWhenDisabled(t *testing.T) {
	cfg := testConfig()
	// MaxInMemLogSize = 0 by default, rate limiting disabled.
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	rl := p.RateLimiter()

	if rl != nil {
		t.Error("expected RateLimiter to return nil when rate limiting is disabled")
	}
}

func TestPeer_RateLimiter_ReturnsInstanceWhenEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.MaxInMemLogSize = 1000 // Enable rate limiting.
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = singleNodeMembership()
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	rl := p.RateLimiter()

	if rl == nil {
		t.Fatal("expected RateLimiter to return non-nil instance")
	}

	// Verify the returned rate limiter is the same as the internal one.
	if rl != p.r.rl {
		t.Error("expected RateLimiter to return the same instance as internal rl")
	}
}

// --- Raft Specification Compliance Tests ---

// TestPeer_GetUpdate_IncludesStateBeforeMessages verifies Figure 2's
// requirement that state changes are persisted before responding to RPCs.
// When a higher-term heartbeat arrives, the Update must include the new
// hard state AND the update must NOT be marked ReadOnly, forcing the
// engine to persist state before sending any outbound messages.
func TestPeer_GetUpdate_IncludesStateBeforeMessages(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{Term: 1, Vote: 1}, singleNodeMembership())

	// Receive a heartbeat from a higher term. This triggers a term
	// transition and produces a HeartbeatResp message.
	err := p.Handle(proto.Message{
		Type:    proto.Heartbeat,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify internal state changed to new term.
	if p.r.term != 5 {
		t.Fatalf("expected term 5 after handling higher-term heartbeat, got %d", p.r.term)
	}

	if !p.HasUpdate(0) {
		t.Fatal("expected HasUpdate to be true after term change")
	}

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("failed to get update: %v", err)
	}

	// Verify the Update includes the new hard state reflecting the term change.
	if update.State.Term != 5 {
		t.Fatalf("expected Update.State.Term to be 5, got %d", update.State.Term)
	}

	// The vote must be cleared on term advancement (per becomeFollower).
	if update.State.Vote != 0 {
		t.Fatalf("expected Update.State.Vote to be 0 after term change, got %d", update.State.Vote)
	}

	// The Update must NOT be ReadOnly because the state changed. This forces
	// the engine to persist the new hard state to stable storage before
	// sending the HeartbeatResp, satisfying Figure 2's requirement:
	// "Updated on stable storage before responding to RPCs".
	if update.ReadOnly {
		t.Fatal("expected Update.ReadOnly to be false when hard state changed; " +
			"state must be persisted before sending messages per Raft Figure 2")
	}

	// There should be outbound messages (HeartbeatResp at minimum).
	if len(update.Messages) == 0 {
		t.Fatal("expected outbound messages in the update after handling heartbeat")
	}

	// ReadyToRead should be nil since no read index was requested.
	if len(update.ReadyToRead) != 0 {
		t.Fatalf("expected no ReadyToRead entries, got %d", len(update.ReadyToRead))
	}
}

// TestPeer_NewPeer_VolatileStateReinitialized verifies that volatile state
// is correctly initialized on peer creation even when persisted state is
// restored. Per Raft Figure 2: commitIndex and lastApplied are volatile
// and reinitialized on boot, while currentTerm and votedFor are persisted.
// leaderID is also volatile (unknown after restart).
func TestPeer_NewPeer_VolatileStateReinitialized(t *testing.T) {
	cfg := testConfig()
	persistedState := proto.State{Term: 5, Vote: 2, Commit: 10}
	ms := singleNodeMembership()

	// Create a log reader with persisted entries so committed=10 is valid.
	entries := make([]proto.Entry, 10)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: 5}
	}
	lr := newTestLogReader(entries, persistedState)
	lr.membership = ms

	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	// Persisted state: term and vote are restored from stable storage.
	// newRaft sets r.term from state first, then calls becomeFollower with the
	// SAME term, and becomeFollower clears the vote only on term advancement
	// (Raft Figure 2), so the vote is NOT cleared. Both halves are asserted:
	// asserting only the term is what let a mutation of the vote-recovery line
	// pass the whole suite.
	if p.r.term != 5 {
		t.Fatalf("expected persisted term 5 to be restored, got %d", p.r.term)
	}
	if p.r.vote != 2 {
		t.Fatalf("expected persisted vote 2 to be restored, got %d: a forgotten "+
			"votedFor lets this replica vote twice in term 5", p.r.vote)
	}

	// The raft state machine should start as follower regardless of
	// persisted state. A node does not persist its role; it always
	// starts as follower after restart.
	if p.r.state != follower {
		t.Fatalf("expected raft to start as follower, got state %d", p.r.state)
	}

	// leaderID is volatile and must NOT be restored from any persisted
	// state. After restart, the node does not know who the current leader
	// is until it receives a message from the leader.
	if p.LeaderID() != noLeader {
		t.Fatalf("expected volatile leaderID to be noLeader (0) after restart, got %d", p.LeaderID())
	}

	// log.processed starts at snapshot index (0 here since no snapshot)
	// or firstIndex-1. With entries [1..10] and no snapshot, processed
	// should be firstIndex-1 = 0. This means all committed entries will
	// be re-applied after restart, which is correct behavior.
	if p.r.log.processed != 0 {
		t.Fatalf("expected log.processed to be 0 (no snapshot, firstIndex-1), got %d", p.r.log.processed)
	}

	// Committed index IS restored from persisted state (it's in the
	// State on disk). Verify it was restored.
	if p.r.log.committed != 10 {
		t.Fatalf("expected committed index 10 from persisted state, got %d", p.r.log.committed)
	}

	// transferTarget must be noNode (volatile, not persisted).
	if p.r.transferTarget != noNode {
		t.Fatalf("expected transferTarget to be noNode after restart, got %d", p.r.transferTarget)
	}

	// pendingConfigChange must be false on fresh start.
	// Note: becomeLeader rescans the log for uncommitted config changes,
	// but as a follower it starts clean.
	if p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be false on fresh start as follower")
	}
}

// TestPeer_RequestLeaderTransfer_BlockedDuringPendingConfigChange verifies
// the interaction between leader transfer and pending config changes.
// Per the Raft implementation, leader transfer checks are independent of
// config change state at the Peer level. The raft state machine does not
// block leader transfers due to pending config changes; these are separate
// protocol mechanisms. This test documents that behavior.
func TestPeer_RequestLeaderTransfer_BlockedDuringPendingConfigChange(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Propose a config change: add node 4. This sets pendingConfigChange=true.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err := p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("config change should succeed: %v", err)
	}

	// Verify pendingConfigChange is set.
	if !p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to be true after proposal")
	}

	// Drain messages from the config change proposal so they don't
	// interfere with leader transfer message detection.
	if p.HasUpdate(0) {
		update, err := p.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("failed to get update: %v", err)
		}
		// Do NOT commit here - we want pendingConfigChange to remain true.
		// Just drain messages by reading but not committing the config change.
		_ = update
	}

	// Attempt leader transfer while config change is pending.
	// The raft state machine does NOT block leader transfer due to
	// pending config changes; they are independent mechanisms.
	err = p.RequestLeaderTransfer(2)
	if err != nil {
		t.Fatalf("leader transfer should not be blocked by pending config change: %v", err)
	}

	// Verify the transfer target was set, confirming the transfer proceeded.
	if p.r.transferTarget != 2 {
		t.Fatalf("expected transferTarget to be 2 after transfer request, got %d", p.r.transferTarget)
	}

	// Now verify that the pending config change is still set - the leader
	// transfer should not have cleared it.
	if !p.r.pendingConfigChange {
		t.Fatal("expected pendingConfigChange to remain true during leader transfer")
	}

	// Since both mechanisms are active simultaneously, verify that new
	// proposals are blocked due to the active transfer (not config change).
	_, propErr := p.Propose([]proto.Entry{{Cmd: []byte("data")}})
	if !errors.Is(propErr, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress for proposals during transfer, got %v", propErr)
	}
}

// TestPeer_ProposeConfigChange_BlockedDuringLeaderTransfer verifies that
// config change proposals are rejected while a leadership transfer is in
// progress. The raft state machine's handleLeaderPropose rejects ALL
// proposals (including config changes) with ErrTransferInProgress when
// transferTarget is set. This prevents config mutations during the handoff
// window where the new leader may not have the config change in its log.
func TestPeer_ProposeConfigChange_BlockedDuringLeaderTransfer(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Start a leader transfer to node 2.
	err := p.RequestLeaderTransfer(2)
	if err != nil {
		t.Fatalf("leader transfer should succeed: %v", err)
	}

	// Verify the transfer is in progress.
	if p.r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", p.r.transferTarget)
	}

	// Attempt a config change while transfer is in progress.
	// ProposeConfigChange checks IsLeader first, then calls r.step which
	// goes through handleLeaderPropose. handleLeaderPropose rejects all
	// proposals with ErrTransferInProgress.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "addr4",
	}
	err = p.ProposeConfigChange(cc)
	if !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress during leader transfer, got %v", err)
	}

	// Verify the transfer target is unchanged (still node 2).
	if p.r.transferTarget != 2 {
		t.Fatalf("expected transferTarget to remain 2, got %d", p.r.transferTarget)
	}

	// Verify membership was NOT modified by the rejected config change.
	if p.membership.isVoter(4) {
		t.Fatal("expected replica 4 NOT to be a voter after rejected config change")
	}

	// Now let the transfer timeout by ticking past the election timeout.
	for range p.r.electionTimeout + 1 {
		p.Tick()
	}

	// After timeout, transfer should be cleared.
	if p.r.transferTarget != noNode {
		t.Fatalf("expected transferTarget to be noNode after timeout, got %d", p.r.transferTarget)
	}

	// Config change should now succeed since transfer is no longer active.
	err = p.ProposeConfigChange(cc)
	if err != nil {
		t.Fatalf("config change should succeed after transfer timeout: %v", err)
	}

	// Membership takes effect at commit time: ack the appended entry
	// from a follower and drive the update cycles so it commits.
	p.Handle(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     p.r.term,
		LogIndex: p.r.log.lastIndex(),
	})
	runUpdateCycles(t, p)

	// Verify the config change was applied.
	if !p.membership.isVoter(4) {
		t.Fatal("expected replica 4 to be a voter after successful config change")
	}
}

// ---------------------------------------------------------------------------
// syncRaftRemotes clears transferTarget when target removed
// ---------------------------------------------------------------------------

func TestPeer_SyncRaftRemotes_ClearsTransferTargetOnRemoval(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Start a leader transfer to node 2.
	err := p.RequestLeaderTransfer(2)
	if err != nil {
		t.Fatalf("leader transfer should succeed: %v", err)
	}
	if p.r.transferTarget != 2 {
		t.Fatalf("expected transferTarget 2, got %d", p.r.transferTarget)
	}

	// Remove the transfer target (node 2) via config change.
	// ProposeConfigChange applies the membership change immediately,
	// which triggers syncRaftRemotes. This should clear the transfer
	// state since the target is no longer in the cluster.
	//
	// We need to clear the transfer state first to allow the proposal,
	// because handleLeaderPropose rejects during active transfer. So
	// instead, directly manipulate the membership and call syncRaftRemotes
	// to test the fix in isolation.
	p.membership.apply(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 2,
	}, p.r.log.lastIndex()+1)
	p.syncRaftRemotes()

	// Transfer state must be cleared.
	if p.r.transferTarget != noNode {
		t.Fatalf("expected transferTarget to be cleared after removing target, got %d",
			p.r.transferTarget)
	}
	if p.r.transferElapsed != 0 {
		t.Fatalf("expected transferElapsed to be 0 after clearing, got %d",
			p.r.transferElapsed)
	}

	// Remote for node 2 must be removed.
	if _, ok := p.r.remotes[2]; ok {
		t.Fatal("expected remote for node 2 to be removed")
	}
}

// TestPeer_SyncRaftRemotes_PreservesRemoteOnObserverPromotion verifies that
// promoting an observer to voter moves its existing remote struct into the
// voter set instead of creating a fresh one. The observer's remote carries
// live replication progress — possibly a snapshot transfer or its ack wait —
// and recreating it at next=lastIndex+1 would immediately probe a
// still-restoring node into a redundant full snapshot re-send.
func TestPeer_SyncRaftRemotes_PreservesRemoteOnObserverPromotion(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Add node 9 as an observer and put its remote mid-snapshot-wait.
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNonVoting,
		ReplicaID: 9,
		Address:   "addr9",
	}, p.r.log.lastIndex()+1); err != nil {
		t.Fatalf("add observer: %v", err)
	}
	p.syncRaftRemotes()
	obs, ok := p.r.observers[9]
	if !ok {
		t.Fatal("expected observer remote for node 9")
	}
	obs.match = 7
	obs.becomeSnapshot(40, 3)
	obs.snapshotDone(5)

	// Promote observer 9 to voter.
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 9,
		Address:   "addr9",
	}, p.r.log.lastIndex()+2); err != nil {
		t.Fatalf("promote observer: %v", err)
	}
	p.syncRaftRemotes()

	promoted, ok := p.r.remotes[9]
	if !ok {
		t.Fatal("expected voter remote for node 9 after promotion")
	}
	if _, still := p.r.observers[9]; still {
		t.Fatal("observer entry must be removed after promotion")
	}
	if promoted != obs {
		t.Fatal("promotion must preserve the existing remote struct, not recreate it")
	}
	if promoted.state != remoteSnapshot || promoted.snapshotIndex != 40 ||
		promoted.snapshotSeq != 3 || promoted.snapshotWaitTicks != 5 || promoted.match != 7 {
		t.Fatalf("promotion must preserve replication state, got %s seq=%d wait=%d match=%d",
			promoted, promoted.snapshotSeq, promoted.snapshotWaitTicks, promoted.match)
	}
}

func TestPeer_SyncRaftRemotes_ProposalsSucceedAfterTransferTargetRemoved(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Start a leader transfer to node 3.
	err := p.RequestLeaderTransfer(3)
	if err != nil {
		t.Fatalf("leader transfer should succeed: %v", err)
	}

	// Proposals must be blocked during transfer.
	_, propErr := p.Propose([]proto.Entry{{Cmd: []byte("data")}})
	if !errors.Is(propErr, ErrTransferInProgress) {
		t.Fatalf("expected ErrTransferInProgress, got %v", propErr)
	}

	// Remove the transfer target from membership and sync remotes.
	p.membership.apply(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 3,
	}, p.r.log.lastIndex()+1)
	p.syncRaftRemotes()

	// Transfer state must be cleared, allowing proposals.
	if p.r.transferTarget != noNode {
		t.Fatalf("expected transferTarget cleared, got %d", p.r.transferTarget)
	}

	// Proposals must now succeed.
	_, err = p.Propose([]proto.Entry{{Cmd: []byte("data")}})
	if err != nil {
		t.Fatalf("proposal should succeed after transfer target removed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// syncRaftRemotes includes witnesses in remoteLastAck
// ---------------------------------------------------------------------------

func TestPeer_SyncRaftRemotes_TracksWitnessLeaseAcks(t *testing.T) {
	// Create a 2-voter + 1-witness cluster with lease reads enabled.
	cfg := testConfigWithID(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.LeaseRead = true
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
		},
		Witnesses: map[uint64]string{
			3: "addr3",
		},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)
	passVoteHoldDown(p.r)

	// Tick past election timeout to trigger election.
	for range 30 {
		p.Tick()
	}

	// Grant votes from remote and witness to become leader.
	mustHandle(t, p, proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: p.r.term,
	})
	mustHandle(t, p, proto.Message{
		Type: proto.RequestVoteResp,
		From: 3,
		Term: p.r.term,
	})

	if !p.IsLeader() {
		t.Fatal("expected peer to become leader")
	}

	// The leader should have initialized remoteLastAck for lease tracking.
	if p.r.remoteLastAck == nil {
		t.Fatal("expected remoteLastAck to be initialized for lease reads")
	}

	// A witness ack counts toward the lease exactly like a voter ack:
	// witnesses vote in elections, so they belong to the same quorum.
	round := sendLeaseRound(p.r)
	for _, from := range []uint64{2, 3} {
		mustHandle(t, p, proto.Message{
			Type:     proto.HeartbeatResp,
			From:     from,
			Term:     p.r.term,
			LogIndex: round,
			LogTerm:  testLeaseWindow,
		})
		if _, ok := p.r.remoteLastAck[from]; !ok {
			t.Fatalf("expected replica %d in remoteLastAck after its heartbeat ack", from)
		}
	}

	// A witness added during leadership must NOT be seeded with an ack:
	// any seeded time is indistinguishable from a real ack and would let
	// a member that never responded contribute to the lease.
	p.membership.witnesses[4] = "addr4"
	p.syncRaftRemotes()
	if _, ok := p.r.remoteLastAck[4]; ok {
		t.Fatal("newly added witness 4 was seeded with an ack")
	}

	// It enters the map on its first real ack.
	mustHandle(t, p, proto.Message{
		Type:     proto.HeartbeatResp,
		From:     4,
		Term:     p.r.term,
		LogIndex: round,
		LogTerm:  testLeaseWindow,
	})
	if _, ok := p.r.remoteLastAck[4]; !ok {
		t.Fatal("expected witness 4 in remoteLastAck after its first ack")
	}

	// Remove witness 4 from membership and the raft witnesses map, then sync.
	delete(p.membership.witnesses, 4)
	delete(p.r.witnesses, 4)
	p.syncRaftRemotes()

	// The removed witness should be cleaned from remoteLastAck.
	if _, ok := p.r.remoteLastAck[4]; ok {
		t.Fatal("expected removed witness 4 to be deleted from remoteLastAck")
	}

	// The remaining witness 3 should still be present.
	if _, ok := p.r.remoteLastAck[3]; !ok {
		t.Fatal("expected witness 3 to remain in remoteLastAck after removing witness 4")
	}
}

func TestPeer_SyncRaftRemotes_WitnessAckNotDeletedByCleanup(t *testing.T) {
	// Verify that a witness heartbeat ack added by handleHeartbeatResp is
	// NOT deleted by syncRaftRemotes cleanup.
	cfg := testConfigWithID(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.LeaseRead = true
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
		},
		Witnesses: map[uint64]string{
			3: "addr3",
		},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)
	passVoteHoldDown(p.r)

	// Become leader.
	for range 30 {
		p.Tick()
	}
	p.Handle(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		Term: p.r.term,
	})
	p.Handle(proto.Message{
		Type: proto.RequestVoteResp,
		From: 3,
		Term: p.r.term,
	})

	if !p.IsLeader() {
		t.Fatal("expected peer to become leader")
	}

	// Tick a few times so the leader broadcasts a heartbeat round and
	// electionTick advances beyond 0.
	p.Tick()
	p.Tick()

	// Simulate a heartbeat response from the witness, which sets the ack.
	p.Handle(proto.Message{
		Type:     proto.HeartbeatResp,
		From:     3,
		Term:     p.r.term,
		LogIndex: p.r.leaseRound,
		LogTerm:  testLeaseWindow,
	})

	// Verify the ack was recorded.
	if _, ok := p.r.remoteLastAck[3]; !ok {
		t.Fatal("expected witness 3 ack in remoteLastAck after heartbeat response")
	}
	ackTick := p.r.remoteLastAck[3]
	if ackTick.IsZero() {
		t.Fatal("expected the ack to be dated by its round's send time")
	}

	// Trigger syncRaftRemotes (e.g., from a config change).
	p.syncRaftRemotes()

	// The witness ack must survive the cleanup.
	if _, ok := p.r.remoteLastAck[3]; !ok {
		t.Fatal("witness 3 ack was incorrectly deleted by syncRaftRemotes cleanup")
	}

	// The ack value should be preserved, not reset to 0.
	if !p.r.remoteLastAck[3].Equal(ackTick) {
		t.Fatalf("expected witness 3 ack time %v to be preserved, got %v",
			ackTick, p.r.remoteLastAck[3])
	}
}

// TestPeer_InvalidateLease_EndsTheLeaseUntilAFreshRound is the call the
// engine makes when it parks a shard whose log write a sealed barrier refused
// (#296): the parked leader stops heartbeating and may be deposed while it
// waits, so on release it must not serve a LeaseRead from acks it collected
// before — not even from a late ack for a round it sent before the hold.
func TestPeer_InvalidateLease_EndsTheLeaseUntilAFreshRound(t *testing.T) {
	cfg := testConfigWithID(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.LeaseRead = true
	ms := proto.Membership{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)
	passVoteHoldDown(p.r)
	for range 30 {
		p.Tick()
	}
	for _, from := range []uint64{2, 3} {
		mustHandle(t, p, proto.Message{Type: proto.RequestVoteResp, From: from, Term: p.r.term})
	}
	if !p.IsLeader() {
		t.Fatal("expected peer to become leader")
	}
	ack := func(round uint64) {
		t.Helper()
		for _, from := range []uint64{2, 3} {
			mustHandle(t, p, proto.Message{
				Type: proto.HeartbeatResp, From: from, Term: p.r.term,
				LogIndex: round, LogTerm: testLeaseWindow,
			})
		}
	}

	held := sendLeaseRound(p.r)
	ack(held)
	if !p.r.hasValidLease() {
		t.Fatal("fixture: a quorum of fresh acks should establish the lease")
	}

	p.InvalidateLease()
	if p.r.hasValidLease() {
		t.Fatal("lease still valid after InvalidateLease")
	}
	ack(held)
	if p.r.hasValidLease() {
		t.Fatal("a late ack for a round sent before the hold rebuilt the lease")
	}

	fresh := sendLeaseRound(p.r)
	if fresh <= held {
		t.Fatalf("round after InvalidateLease = %d, want > %d", fresh, held)
	}
	ack(fresh)
	if !p.r.hasValidLease() {
		t.Fatal("a quorum of acks for a fresh round should renew the lease")
	}
}

// --- ValidateConfigChange ---

// TestPeer_ValidateConfigChange_DuplicateAdd verifies that validating an
// AddNode for a replica that is already a voting member returns
// ErrNodeAlreadyExists without mutating state.
func TestPeer_ValidateConfigChange_DuplicateAdd(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 1, Address: "addr1"}
	if err := p.ValidateConfigChange(cc); !errors.Is(err, ErrNodeAlreadyExists) {
		t.Fatalf("ValidateConfigChange(duplicate) = %v, want ErrNodeAlreadyExists", err)
	}
}

// TestPeer_ValidateConfigChange_RemoveLastVoter verifies that validating the
// removal of the last full replica returns ErrRemoveLastFullReplica.
func TestPeer_ValidateConfigChange_RemoveLastVoter(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	cc := proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 1}
	if err := p.ValidateConfigChange(cc); !errors.Is(err, ErrRemoveLastFullReplica) {
		t.Fatalf("ValidateConfigChange(remove last voter) = %v, want ErrRemoveLastFullReplica", err)
	}
}

// TestPeer_ValidateConfigChange_ExceedsCap builds a membership at the
// 64-voting-member cap and verifies that ValidateConfigChange rejects both a
// 65th full replica and a 65th witness with ErrTooManyVotingMembers — the
// check the Host membership API relies on to avoid the rebuildReplicaToBit
// invariant panic. An observer add (which does not affect quorum) is still
// accepted at the cap.
func TestPeer_ValidateConfigChange_ExceedsCap(t *testing.T) {
	cfg := testConfig()
	// Distinct addresses per member: one address may name only one member of a
	// shard (see membership.addressHolder), so a fixture that reuses one string
	// would be rejected for THAT reason and prove nothing about the cap.
	addrs := make(map[uint64]string, maxVotingMembers)
	for id := uint64(1); id <= maxVotingMembers; id++ {
		addrs[id] = fmt.Sprintf("addr%d", id)
	}
	p := newTestPeer(t, cfg, proto.State{}, proto.Membership{Addresses: addrs})

	addNode := proto.ConfigChange{Type: proto.AddNode, ReplicaID: maxVotingMembers + 1, Address: "addr-new-voter"}
	if err := p.ValidateConfigChange(addNode); !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("ValidateConfigChange(AddNode over cap) = %v, want ErrTooManyVotingMembers", err)
	}

	addWitness := proto.ConfigChange{Type: proto.AddWitness, ReplicaID: maxVotingMembers + 2, Address: "addr-new-witness"}
	if err := p.ValidateConfigChange(addWitness); !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("ValidateConfigChange(AddWitness over cap) = %v, want ErrTooManyVotingMembers", err)
	}

	addObserver := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: maxVotingMembers + 3, Address: "addr-new-observer"}
	if err := p.ValidateConfigChange(addObserver); err != nil {
		t.Fatalf("ValidateConfigChange(AddNonVoting over cap) = %v, want nil", err)
	}
}

// TestPeer_ValidateConfigChange_Valid verifies that a legitimate config
// change passes validation.
func TestPeer_ValidateConfigChange_Valid(t *testing.T) {
	cfg := testConfig()
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())

	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 2, Address: "addr2"}
	if err := p.ValidateConfigChange(cc); err != nil {
		t.Fatalf("ValidateConfigChange(valid add) = %v, want nil", err)
	}
}

// TestLeaderAndTermIsAConsistentPair pins that the leader and term are readable
// together as a combination that actually held.
//
// They are two independent atomic words. A reader loading them separately can
// straddle a syncLeaderID and observe a (leaderID, term) pair that never
// existed, which reported through OnLeaderUpdated tells a listener that a
// replica led in a term it never led. The seqlock on syncLeaderID is what makes
// the pair atomic.
func TestLeaderAndTermIsAConsistentPair(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, threeNodeMembership())

	// Every leadership change advances BOTH fields together: leader N holds
	// term N here, so any mismatch is a torn read.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			p.r.leaderID = i
			p.r.term = i
			p.syncLeaderID()
		}
	}()

	deadline := time.Now().Add(300 * time.Millisecond)
	var reads int
	for time.Now().Before(deadline) {
		leaderID, term := p.LeaderAndTerm()
		reads++
		if leaderID != term {
			close(stop)
			wg.Wait()
			t.Fatalf("torn read: leader=%d with term=%d, a pair that never "+
				"co-existed (every update sets both to the same value)",
				leaderID, term)
		}
	}
	close(stop)
	wg.Wait()
	if reads == 0 {
		t.Fatal("fixture: no reads were performed")
	}
	t.Logf("%d consistent pair reads under a concurrent writer", reads)
}
