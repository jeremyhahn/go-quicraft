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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// atTerm places a freshly constructed node at a realistic post-bootstrap
// term. Term 0 belongs to a node that has never seen an election; no peer
// can produce a protocol message at that term (every send() stamps the
// sender's term, and a sender must reach term 1 before it can lead or
// campaign), so tests that inject peer messages must start above it.
func atTerm(r *raft, term uint64) *raft {
	r.becomeFollower(term, noLeader)
	return r
}

// newRegressionRaft builds a raft node over the given voting membership with
// PreVote disabled so a single RequestVoteResp decides the election.
func newRegressionRaft(t *testing.T, replicaID uint64, members map[uint64]string) *raft {
	t.Helper()
	cfg := raftTestCfgPreVote(1, replicaID, false)
	r, err := newRaft(cfg, newTestLogReaderWithMembership(members))
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}
	return r
}

// ---------------------------------------------------------------------------
// Vote tally must be taken over the configuration voters
// ---------------------------------------------------------------------------

// A vote response from a replica that is not in the current configuration
// must not count toward the election quorum. Counting it lets a candidate
// win with a single configuration vote (its own) and produces two leaders
// in the same term.
func TestHandleVoteResp_IgnoresNonMemberGrant(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
	r.becomeCandidate()
	r.broadcastVote()

	// Replica 9 is not a voter, observer, or witness of this shard.
	if err := r.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 9,
		Term: r.term,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}

	if _, ok := r.votes[9]; ok {
		t.Fatal("vote from non-member was recorded in the vote map")
	}
	if r.state == leader {
		t.Fatal("became leader on a non-member vote plus self-vote")
	}
}

// A vote response queued before the sender was removed from the
// configuration must not count once the voting set (and therefore the
// quorum) has shrunk.
func TestHandleVoteResp_IgnoresRemovedVoterGrant(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4"})
	r.becomeCandidate()
	r.broadcastVote()

	// RemoveNode(4) commits mid-election: quorum drops from 3 to 2 while
	// replica 4's granted response is still in flight.
	delete(r.remotes, 4)

	if err := r.step(proto.Message{
		Type: proto.RequestVoteResp,
		From: 4,
		Term: r.term,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}

	if r.state == leader {
		t.Fatal("became leader on a departed replica's vote plus self-vote")
	}
}

// tallyVotes must iterate the configuration voters, not the vote map, so a
// stale entry for a departed replica cannot be counted.
func TestTallyVotes_CountsConfigurationVotersOnly(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4"})
	r.becomeCandidate()
	// Stale grant left over from before replica 4 departed.
	r.votes[4] = true
	delete(r.remotes, 4)

	if got := r.tallyVotes(); got == voteWon {
		t.Fatal("tallyVotes counted a vote from outside the configuration")
	}
}

// syncRaftRemotes must purge vote records for replicas that left the
// configuration so a later tally cannot resurrect them.
func TestSyncRaftRemotes_PurgesVotesOfDepartedMembers(t *testing.T) {
	ms := proto.Membership{Addresses: map[uint64]string{
		1: "a1", 2: "a2", 3: "a3", 4: "a4",
	}}
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, ms)
	p.r.becomeCandidate()
	p.r.votes[4] = true

	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 4,
	}, 1); err != nil {
		t.Fatalf("apply RemoveNode: %v", err)
	}
	p.syncRaftRemotes()

	if _, ok := p.r.votes[4]; ok {
		t.Fatal("vote record for departed replica 4 survived syncRaftRemotes")
	}
}

// rebuildRemotesFromMembership (snapshot restore) must purge vote records
// for replicas absent from the restored configuration.
func TestRebuildRemotesFromMembership_PurgesDepartedVotes(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4"})
	r.becomeCandidate()
	r.votes[4] = true

	r.rebuildRemotesFromMembership(proto.Membership{Addresses: map[uint64]string{
		1: "a1", 2: "a2", 3: "a3",
	}})

	if _, ok := r.votes[4]; ok {
		t.Fatal("vote record for replica 4 survived the snapshot rebuild")
	}
}

// ---------------------------------------------------------------------------
// Term==0 is a local-injection escape hatch, not a wire value
// ---------------------------------------------------------------------------

// A Replicate carrying Term 0 bypasses every term check. Accepting it lets
// any authorized shard member advance a follower's commit index over its
// uncommitted tail.
func TestStep_RejectsZeroTermReplicate(t *testing.T) {
	r := newTestRaft(1)
	r.becomeFollower(7, noLeader)
	r.log.append([]proto.Entry{
		{Index: 1, Term: 7},
		{Index: 2, Term: 7},
		{Index: 3, Term: 7},
	})

	err := r.step(proto.Message{
		Type:     proto.Replicate,
		From:     3,
		Term:     0,
		LogIndex: 3,
		LogTerm:  7,
		Commit:   3,
	})
	if !errors.Is(err, ErrZeroTermMessage) {
		t.Fatalf("expected ErrZeroTermMessage, got %v", err)
	}
	if r.log.committed != 0 {
		t.Fatalf("zero-term Replicate advanced committed to %d", r.log.committed)
	}
	if r.leaderID != noLeader {
		t.Fatalf("zero-term Replicate set leaderID to %d", r.leaderID)
	}
}

// A Heartbeat carrying Term 0 must not reset the election timer or install
// a leader.
func TestStep_RejectsZeroTermHeartbeat(t *testing.T) {
	r := newTestRaft(1)
	r.becomeFollower(7, noLeader)
	r.electionTick = 5

	err := r.step(proto.Message{Type: proto.Heartbeat, From: 3, Term: 0})
	if !errors.Is(err, ErrZeroTermMessage) {
		t.Fatalf("expected ErrZeroTermMessage, got %v", err)
	}
	if r.leaderID != noLeader {
		t.Fatalf("zero-term Heartbeat set leaderID to %d", r.leaderID)
	}
	if r.electionTick != 5 {
		t.Fatalf("zero-term Heartbeat reset electionTick to %d", r.electionTick)
	}
}

// A RequestVoteResp carrying Term 0 escapes the stale-term filter and can
// decide an election at any term.
func TestStep_RejectsZeroTermVoteResp(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
	r.becomeCandidate()
	r.broadcastVote()

	err := r.step(proto.Message{Type: proto.RequestVoteResp, From: 2, Term: 0})
	if !errors.Is(err, ErrZeroTermMessage) {
		t.Fatalf("expected ErrZeroTermMessage, got %v", err)
	}
	if r.state == leader {
		t.Fatal("zero-term vote response decided the election")
	}
}

// The locally injected control messages must keep the Term==0 bypass: the
// engine and the message handler build them without a term.
func TestStep_AllowsZeroTermLocalControlMessages(t *testing.T) {
	for _, msgType := range []uint64{
		proto.Propose,
		proto.ReadIndex,
		proto.InstallSnapshot,
		proto.SnapshotStatus,
		proto.Unreachable,
		proto.LeaderTransfer,
	} {
		if !isLocalControlMessage(msgType) {
			t.Fatalf("%s must remain locally injectable with Term 0",
				proto.MessageTypeName(msgType))
		}
	}
	for _, msgType := range []uint64{
		proto.Replicate,
		proto.ReplicateResp,
		proto.Heartbeat,
		proto.HeartbeatResp,
		proto.RequestVote,
		proto.RequestVoteResp,
		proto.RequestPreVote,
		proto.RequestPreVoteResp,
		proto.TimeoutNow,
		proto.Quiesce,
		proto.RateLimit,
		proto.ReadIndexResp,
	} {
		if isLocalControlMessage(msgType) {
			t.Fatalf("%s is produced by send() and must carry a term",
				proto.MessageTypeName(msgType))
		}
	}
}

// ---------------------------------------------------------------------------
// The Term==0 bypass belongs to the LOCAL delivery path, not to the
// message TYPE. Every type in localControlMessages is decodable from the wire
// (proto.Message.UnmarshalFrom accepts any known type), the transport hands
// whole batches to the message handler with no type filter, and the handler
// delivers into the SAME inbox the in-process injectors use. Keying the
// exemption on the type alone therefore exposed all six to the network.
// ---------------------------------------------------------------------------

// A wire message of a local-control type carrying Term 0 must be dropped, not
// stepped. Propose and ReadIndex are legitimately FORWARDED by followers, but
// a forward is produced by send() and always carries the sender's term; a
// term-less one can only be a forgery.
func TestStep_RejectsZeroTermLocalControlFromWire(t *testing.T) {
	for msgType := range localControlMessages {
		name := proto.MessageTypeName(msgType)
		t.Run(name, func(t *testing.T) {
			r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
			r.becomeFollower(7, 2)

			// Origin defaults to proto.OriginRemote — exactly what
			// UnmarshalFrom leaves on a decoded message.
			err := r.step(proto.Message{Type: msgType, From: 3, Term: 0})
			if err == nil {
				t.Fatalf("wire %s with Term 0 was accepted", name)
			}
			if !errors.Is(err, ErrZeroTermMessage) && !errors.Is(err, ErrWireLocalMessage) {
				t.Fatalf("wire %s with Term 0: expected a rejection, got %v", name, err)
			}
		})
	}
}

// The four types that are NEVER exchanged between replicas must be refused
// from the wire at ANY term, including the receiver's own current term.
// Supplying a matching term is free for an attacker (terms are observable in
// every heartbeat), so a term-value heuristic is not a boundary.
func TestStep_RejectsProcessLocalTypesFromWireAtAnyTerm(t *testing.T) {
	for msgType := range processLocalMessages {
		name := proto.MessageTypeName(msgType)
		for _, term := range []uint64{0, 7, 99} {
			t.Run(fmt.Sprintf("%s/term%d", name, term), func(t *testing.T) {
				r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
				r.becomeFollower(7, 2)

				err := r.step(proto.Message{Type: msgType, From: 3, Term: term})
				if !errors.Is(err, ErrWireLocalMessage) {
					t.Fatalf("wire %s at term %d: expected ErrWireLocalMessage, got %v",
						name, term, err)
				}
			})
		}
	}
}

// Every process-local type must also be a local-control type: the two sets
// share the "carries no term" property, and a type in processLocalMessages
// that step still required a term from would be unreachable altogether.
func TestProcessLocalMessagesAreLocalControlMessages(t *testing.T) {
	for msgType := range processLocalMessages {
		if !isLocalControlMessage(msgType) {
			t.Fatalf("%s is process-local but not exempt from the term gate",
				proto.MessageTypeName(msgType))
		}
	}
	// Propose and ReadIndex must NOT be process-local: followers forward both
	// to the leader, and refusing them from the wire would break every
	// non-leader client request and every follower ReadIndex.
	for _, msgType := range []uint64{proto.Propose, proto.ReadIndex} {
		if isProcessLocalMessage(msgType) {
			t.Fatalf("%s is forwarded between replicas and must not be process-local",
				proto.MessageTypeName(msgType))
		}
	}
}

// The forwarding paths must keep working: a follower's Propose and ReadIndex
// reach the leader over the wire with the sender's term.
func TestStep_AcceptsForwardedProposeAndReadIndexFromWire(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
	r.becomeCandidate()
	r.becomeLeader()
	r.log.commitTo(r.log.lastIndex())
	r.log.appliedTo(r.log.lastIndex())
	term := r.term

	if err := r.step(proto.Message{
		Type:    proto.Propose,
		From:    2,
		Term:    term,
		Entries: []proto.Entry{{Cmd: []byte("forwarded")}},
	}); err != nil {
		t.Fatalf("forwarded Propose rejected: %v", err)
	}

	if err := r.step(proto.Message{
		Type: proto.ReadIndex,
		From: 2,
		Term: term,
		Hint: 42,
	}); err != nil {
		t.Fatalf("forwarded ReadIndex rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// A joining node must not be elected on its own one-member view
// ---------------------------------------------------------------------------

// A joining node that caught up by log replay holds only itself in its
// membership. TimeoutNow must not let it campaign: it would win with a
// quorum of one and acknowledge writes the real cluster discards.
func TestJoiningNode_TimeoutNowDoesNotElect(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 1), proto.State{}, singleNodeMembership())
	p.MarkJoining()

	err := p.Handle(proto.Message{
		Type: proto.TimeoutNow,
		From: 2,
		Term: 5,
	})
	if !errors.Is(err, ErrJoiningNodeCannotCampaign) {
		t.Fatalf("expected ErrJoiningNodeCannotCampaign, got %v", err)
	}

	if p.IsLeader() {
		t.Fatal("joining node with a one-member view became leader")
	}
	if p.r.state == candidate {
		t.Fatal("joining node campaigned on a one-member view")
	}
}

// A joining node whose configChangeID already equals the snapshot's (it
// applied its own AddNode from the log) must still adopt the snapshot
// membership: that is the only mechanism that repairs its view and clears
// the joining flag.
func TestJoiningNode_AdoptsSnapshotWithEqualConfigChangeID(t *testing.T) {
	// A joining node starts with empty membership.
	p := newTestPeer(t, testConfigWithID(1, 4), proto.State{}, proto.Membership{})
	p.MarkJoining()

	// The joining node applied its own AddNode at index 7.
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "a4",
	}, 7); err != nil {
		t.Fatalf("apply AddNode: %v", err)
	}
	p.publishMembership()

	// The leader's snapshot carries the full configuration at the SAME
	// ConfigChangeID.
	full := proto.Membership{
		Addresses: map[uint64]string{
			1: "a1", 2: "a2", 3: "a3", 4: "a4",
		},
		ConfigChangeID: p.membership.configChangeID,
	}
	// Matching-log branch: the local log has an entry at the snapshot
	// index with the snapshot's term.
	p.r.log.append([]proto.Entry{{Index: 1, Term: 2}})
	if err := p.Handle(proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From: 1,
		Term: 2,
		Snapshot: proto.Snapshot{
			Index:      1,
			Term:       2,
			Membership: full,
		},
	}); err != nil {
		t.Fatalf("Handle InstallSnapshot: %v", err)
	}

	if len(p.GetMembership().Addresses) != 4 {
		t.Fatalf("joining node did not adopt the snapshot membership: %v",
			p.GetMembership().Addresses)
	}
	if p.joining.Load() {
		t.Fatal("joining flag survived a full-membership snapshot")
	}
}

// ---------------------------------------------------------------------------
// SetProcessedIndex must never outrun the recovered commit index
// ---------------------------------------------------------------------------

// An on-disk state machine whose durable marker outruns the recovered
// HardState.Commit must not fold the uncommitted tail's config changes into
// the membership.
func TestSetProcessedIndex_IgnoresUncommittedConfigChange(t *testing.T) {
	lr := newTestLogReader([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t,
			proto.ConfigChange{Type: proto.AddNode, ReplicaID: 9, Address: "a9"})},
	}, proto.State{Commit: 2})
	lr.membership = singleNodeMembership()

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}

	p.SetProcessedIndex(3)

	if _, ok := p.GetMembership().Addresses[9]; ok {
		t.Fatal("uncommitted AddNode(9) was folded into the membership")
	}
	if p.membership.configChangeID >= 3 {
		t.Fatalf("configChangeID advanced past committed: %d",
			p.membership.configChangeID)
	}
}

// A processed marker above the recovered commit index makes the first
// Peer.Commit trip commitUpdate's LastApplied assertions, turning a
// survivable crash state into a restart loop.
func TestSetProcessedIndex_ClampedSoCommitDoesNotPanic(t *testing.T) {
	lr := newTestLogReader([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}, proto.State{Commit: 2})
	lr.membership = singleNodeMembership()

	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}

	p.SetProcessedIndex(3)
	if p.r.log.processed > p.r.log.committed {
		t.Fatalf("processed %d exceeds committed %d",
			p.r.log.processed, p.r.log.committed)
	}

	if err := p.Commit(proto.Update{LastApplied: p.r.log.processed}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// ---------------------------------------------------------------------------
// A newly added member must earn its lease contribution
// ---------------------------------------------------------------------------

// A member added during leadership must not contribute to hasValidLease
// before its first real heartbeat ack. Any seeded ack time would claim a
// heartbeat round the member never answered.
func TestSyncLeaseAcks_NewMemberCannotGrantLease(t *testing.T) {
	ms := threeNodeMembership()
	cfg := testConfigWithID(1, 1)
	cfg.LeaseRead = true
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Leader with a lease map holding one real ack (replica 2) and no ack
	// from replica 3.
	p.r.state = leader
	p.r.leaderID = 1
	p.r.remoteLastAck = map[uint64]time.Time{2: p.r.now()}
	p.r.electionTick = 1

	// AddNode(4) commits during leadership.
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: 4,
		Address:   "a4",
	}, 1); err != nil {
		t.Fatalf("apply AddNode: %v", err)
	}
	p.syncRaftRemotes()

	if _, ok := p.r.remoteLastAck[4]; ok {
		t.Fatal("newly added replica 4 was seeded with an ack")
	}
	// Voters are now {1,2,3,4}: quorum 3, real acks are self + replica 2.
	if p.r.hasValidLease() {
		t.Fatal("lease granted using a phantom ack from a replica that never acked")
	}
}

// ---------------------------------------------------------------------------
// Quiesce must not freeze the lease or the CheckQuorum step-down
// ---------------------------------------------------------------------------

// A quiesced, partitioned leader must still lose its lease and step down.
func TestQuiescedLeader_StepsDownWithoutQuorum(t *testing.T) {
	members := map[uint64]string{1: "a1", 2: "a2", 3: "a3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.LeaseRead = true
	r, err := newRaft(cfg, newTestLogReaderWithMembership(members))
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	tickIntoQuiesce(t, r)

	// The partition means no remote ever acks again. The leader must give
	// up leadership within a bounded number of quiesce liveness intervals.
	limit := r.quiesce.livenessInterval * 4
	for i := 0; i < limit && r.state == leader; i++ {
		r.tickFunc(r)
	}

	if r.state == leader {
		t.Fatalf("quiesced leader never stepped down after %d ticks", limit)
	}
	if r.hasValidLease() {
		t.Fatal("quiesced leader still holds a read lease")
	}
}

// tickIntoQuiesce drives a leader into quiesce while its followers keep
// acking, so the CheckQuorum step-down never fires during the warm-up.
func tickIntoQuiesce(t *testing.T, r *raft) {
	t.Helper()
	for i := 0; i < r.quiesce.threshold+r.electionTimeout*2 && !r.quiesce.active; i++ {
		grantLease(r)
		r.tickFunc(r)
	}
	if !r.quiesce.active {
		t.Fatal("leader did not enter quiesce")
	}
}

// Leaving quiesce must invalidate the lease: acks recorded before or during
// quiesce are arbitrarily old by the time the leader resumes.
func TestQuiesceExit_ResetsLeaseAcks(t *testing.T) {
	members := map[uint64]string{1: "a1", 2: "a2", 3: "a3"}
	cfg := raftTestCfgQuiesce(1, 1)
	cfg.PreVote = config.BoolPtr(false)
	cfg.LeaseRead = true
	r, err := newRaft(cfg, newTestLogReaderWithMembership(members))
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	electLeader(r)
	tickIntoQuiesce(t, r)

	// Acks taken while quiesced must not survive the wake-up.
	grantLease(r)

	// A client proposal wakes the leader up.
	if err := r.step(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: []proto.Entry{{Type: proto.EntryNormal, Cmd: []byte("wake")}},
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	if r.quiesce.active {
		t.Fatal("quiesce still active after a proposal")
	}
	if r.hasValidLease() {
		t.Fatal("lease survived the quiesce cycle without a fresh ack")
	}
}

// ---------------------------------------------------------------------------
// Low-severity findings
// ---------------------------------------------------------------------------

// Removing the leader-transfer target must restore lease tracking, exactly
// as the transfer-timeout path in tickLeader does. Otherwise every ReadIndex
// degrades to a full heartbeat round-trip for the rest of the term.
func TestSyncRaftRemotes_RestoresLeaseOnTransferTargetRemoval(t *testing.T) {
	ms := threeNodeMembership()
	cfg := testConfigWithID(1, 1)
	cfg.LeaseRead = true
	p := newTestPeer(t, cfg, proto.State{}, ms)

	p.r.state = leader
	p.r.leaderID = 1
	p.r.remoteLastAck = nil // handleLeaderTransfer expires the lease
	p.r.transferTarget = 3

	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.RemoveNode,
		ReplicaID: 3,
	}, 1); err != nil {
		t.Fatalf("apply RemoveNode: %v", err)
	}
	p.syncRaftRemotes()

	if p.r.transferTarget != noNode {
		t.Fatal("transfer target survived removal of the target replica")
	}
	if p.r.remoteLastAck == nil {
		t.Fatal("lease tracking was not restored after the transfer target left")
	}
}

// handleAppendResp must reject a match index beyond the leader's own log.
// rm.update would otherwise raise match above lastIndex and let tryCommit
// advance the commit index past entries that do not exist.
func TestHandleAppendResp_RejectsMatchBeyondLastIndex(t *testing.T) {
	r := newTestRaft(1)
	electLeader(r)
	last := r.log.lastIndex()

	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: last + 100,
	}); err != nil {
		t.Fatalf("step: %v", err)
	}

	if got := r.remotes[2].match; got > last {
		t.Fatalf("match advanced to %d beyond leader lastIndex %d", got, last)
	}
}

// countingLogReader counts GetRange calls so a test can assert that a code
// path does no log reading at all.
type countingLogReader struct {
	*testLogReader
	reads atomic.Int64
}

func (c *countingLogReader) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	c.reads.Add(1)
	return c.testLogReader.GetRange(low, high, maxSize)
}

// hasConfigChangeToApply is answered from an incrementally maintained bound
// (raftLog.confChangeIndex), so it must cover ranges of any width — including
// ones far wider than the 1024-index chunk the previous re-scanning
// implementation read at a time — at the same O(1) cost.
func TestHasConfigChangeToApply_CoversArbitrarilyWideRanges(t *testing.T) {
	r := newTestRaft(1)
	const total = uint64(1224)
	entries := make([]proto.Entry, 0, total)
	for i := uint64(1); i <= total; i++ {
		e := proto.Entry{Index: i, Term: 1}
		if i == total {
			e.Type = proto.EntryConfigChange
		}
		entries = append(entries, e)
	}
	r.log.append(entries)

	if !r.hasConfigChangeToApply() {
		t.Fatalf("config change at index %d was missed", total)
	}

	// Same log without the trailing config change must report absence.
	r2 := newTestRaft(1)
	plain := make([]proto.Entry, 0, total)
	for i := uint64(1); i <= total; i++ {
		plain = append(plain, proto.Entry{Index: i, Term: 1})
	}
	r2.log.append(plain)
	if r2.hasConfigChangeToApply() {
		t.Fatal("reported a pending config change over a log with none")
	}
}

// becomeLeader must not do work proportional to the unapplied log.
//
// A gate that re-read (processed, lastIndex] with no byte bound, synchronously
// on the step worker, would not scale. On a restart processed starts at the
// snapshot index, and Config.SnapshotEntries has no default, so a default
// deployment never compacts: the range is the whole log. 200k unapplied 1KB
// entries is ~200MB copied before the new leader can send its first heartbeat,
// so the election it had just won would time out and the shard would
// election-loop.
//
// A counting LogReader proves the property directly: answering the gate over
// a large unapplied range must not read the log at all.
func TestHasConfigChangeToApply_DoesNotReadTheLog(t *testing.T) {
	entries := make([]proto.Entry, 0, 20000)
	for i := uint64(1); i <= 20000; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1, Cmd: make([]byte, 1024)})
	}
	inner := newTestLogReader(entries, proto.State{Term: 1, Commit: 20000})
	inner.membership = threeNodeMembership()
	counting := &countingLogReader{testLogReader: inner}

	r, err := newRaft(raftTestCfgPreVote(1, 1, false), counting)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if r.log.processed != 0 || r.log.lastIndex() != 20000 {
		t.Fatalf("fixture: processed=%d lastIndex=%d, want 0 and 20000",
			r.log.processed, r.log.lastIndex())
	}

	counting.reads.Store(0)
	r.becomeCandidate()
	r.becomeLeader()

	if got := counting.reads.Load(); got != 0 {
		t.Fatalf("becomeLeader issued %d log range reads over a 20000-entry "+
			"unapplied tail; the config-change gate must be O(1) or the "+
			"first heartbeat is delayed behind the whole log", got)
	}
	// It must still FAIL CLOSED over an unclassified recovered tail.
	if !r.pendingConfigChange {
		t.Fatal("a restarted leader with an unapplied tail did not arm the " +
			"config-change gate: the tail may hold a config change this " +
			"process never classified")
	}
}

// The conservative arming must RELEASE as the recovered tail is applied, or a
// leader elected after a restart would reject membership changes for its whole
// term. This is what the old scan's configChangeScanUnproven re-check existed
// to guarantee; the bound now releases without reading anything, so a LogDB
// read error cannot latch the gate at all.
func TestConfigChangeGate_ConservativeArmingReleasesOnApply(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}
	inner := newTestLogReader(entries, proto.State{Term: 1, Commit: 5})
	inner.membership = threeNodeMembership()

	r, err := newRaft(raftTestCfgPreVote(1, 1, false), inner)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeCandidate()
	r.becomeLeader()
	if !r.pendingConfigChange {
		t.Fatal("becomeLeader did not fail closed over an unclassified recovered tail")
	}

	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}
	ccMsg := func() proto.Message {
		return proto.Message{
			Type:   proto.Propose,
			Origin: proto.OriginLocal,
			Entries: []proto.Entry{{
				Type: proto.EntryConfigChange,
				Cmd:  marshalConfigChange(t, cc),
			}},
		}
	}
	if err := r.step(ccMsg()); !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("config change accepted over an unclassified tail: %v", err)
	}

	// Apply the recovered tail: nothing in it was a config change, and the
	// gate must now open.
	r.log.appliedTo(r.log.committed)
	if r.hasConfigChangeToApply() {
		t.Fatal("the gate stayed armed after the recovered tail was applied")
	}
	if err := r.step(ccMsg()); err != nil {
		t.Fatalf("config change rejected after the recovered tail was applied: %v", err)
	}
}

// A config change that IS in the log must keep the gate armed until its
// membership effect has landed, not merely until it commits. This is the
// safety property the O(1) bound has to preserve: reverting the anchor from
// processed to committed, or dropping the bound entirely, must fail here.
func TestConfigChangeGate_ArmedUntilConfigChangeIsApplied(t *testing.T) {
	r := newTestRaft(1)
	r.becomeCandidate()
	r.becomeLeader()
	ccIndex := r.log.lastIndex() + 1
	r.log.append([]proto.Entry{{
		Index: ccIndex,
		Term:  r.term,
		Type:  proto.EntryConfigChange,
	}})

	// Committed but NOT applied: the membership effect lands in Peer.Commit.
	r.log.commitTo(ccIndex)
	if !r.hasConfigChangeToApply() {
		t.Fatal("a committed but unapplied config change left the gate open")
	}

	r.log.appliedTo(ccIndex)
	if r.hasConfigChangeToApply() {
		t.Fatal("the gate stayed armed after the config change was applied")
	}
}

// A snapshot restore adopts the configuration as of the snapshot index and
// discards everything above it, so it must clear the bound. Leaving it set
// would block membership changes on a replica that caught up by snapshot
// until its log grew past a stale index.
func TestConfigChangeGate_ClearedBySnapshotRestore(t *testing.T) {
	r := newTestRaft(1)
	// The config change sits ABOVE the snapshot index, so it is one of the
	// entries the restore DISCARDS. A bound left behind here outlives every
	// entry it described.
	r.log.append([]proto.Entry{{Index: 20, Term: 1, Type: proto.EntryConfigChange}})
	if !r.hasConfigChangeToApply() {
		t.Fatal("fixture: the appended config change did not arm the gate")
	}

	r.log.restore(proto.Snapshot{Index: 10, Term: 2})
	if r.log.processed != 10 {
		t.Fatalf("fixture: processed = %d after restore, want 10", r.log.processed)
	}
	if r.hasConfigChangeToApply() {
		t.Fatalf("a snapshot restore left the config-change gate armed at index "+
			"%d over a log that now ends at %d: the replica refuses every "+
			"membership change until its log grows past an entry the restore "+
			"threw away", r.log.confChangeIndex, r.log.lastIndex())
	}
}

// ---------------------------------------------------------------------------
// The config change gate must be anchored at the APPLIED frontier
// ---------------------------------------------------------------------------

// newConfigChangeGateRaft builds a leader-elect over a log whose last entry is
// a config change that has COMMITTED but has NOT been applied to membership
// yet — the window between r.log.committed advancing in the step worker and
// Peer.Commit -> processConfigChanges running.
func newConfigChangeGateRaft(t *testing.T, applied uint64) *raft {
	t.Helper()
	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}
	lr := newTestLogReader([]proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal},
		{Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t, cc)},
	}, proto.State{Term: 1, Commit: 2})
	lr.membership = threeNodeMembership()

	r, err := newRaft(raftTestCfgPreVote(1, 2, false), lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if r.log.committed != 2 {
		t.Fatalf("fixture: committed = %d, want 2", r.log.committed)
	}
	if applied > 0 {
		r.log.appliedTo(applied)
	}
	if r.log.processed != applied {
		t.Fatalf("fixture: processed = %d, want %d", r.log.processed, applied)
	}
	return r
}

// TestConfigChangeGate_ArmedByCommittedButUnappliedChange is the
// regression.
//
// Membership does not take effect at commit; it takes effect in Peer.Commit
// -> processConfigChanges, a stage BEHIND r.log.committed. becomeLeader armed
// pendingConfigChange from a scan of (committed, lastIndex] only, so a config
// change that had committed but not yet been folded into the membership was
// invisible to it.
//
// {A,B,C}: A commits AddNode(D) at index 2 and crashes. B has committed=2 but
// has not run Peer.Commit for it, so B.membership is still {A,B,C}. B wins the
// election; the old scan starts at committed+1 = 3 over a log whose last index
// is 2, returns immediately, and leaves pendingConfigChange=false. A client
// then proposes AddNode(E) and it is ACCEPTED. B now computes its commit
// quorum over {A,B,C} (q=2) while the newest configuration in the log is
// {A,B,C,D,E} (q=3): commit quorum {A,B} and election quorum {C,D,E} do not
// intersect, and Leader Completeness fails.
func TestConfigChangeGate_ArmedByCommittedButUnappliedChange(t *testing.T) {
	r := newConfigChangeGateRaft(t, 1)

	// The config change at index 2 is committed but unapplied.
	if !r.hasConfigChangeToApply() {
		t.Fatal("a COMMITTED but UNAPPLIED config change was not detected: " +
			"membership takes effect in Peer.Commit, which lags " +
			"r.log.committed, so a scan anchored at committed reports " +
			"\"nothing pending\" for a change whose membership effect has " +
			"not landed")
	}

	r.becomeCandidate()
	r.becomeLeader()
	if !r.pendingConfigChange {
		t.Fatal("becomeLeader did not arm pendingConfigChange for a " +
			"committed-but-unapplied config change")
	}

	// The second config change must be refused. Accepting it puts two
	// configurations in flight whose quorums need not intersect.
	next := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 5, Address: "a5"}
	err := r.step(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  marshalConfigChange(t, next),
		}},
	})
	if !errors.Is(err, ErrPendingConfigChange) {
		t.Fatalf("second config change accepted (err = %v) while the first "+
			"had not been applied to membership: the leader computes commit "+
			"quorum over the OLD voting set while the log's newest "+
			"configuration is larger, so the commit quorum and the election "+
			"quorum need not intersect", err)
	}
}

// The gate must RELEASE once the change has actually been applied, or a
// leader could never make a second membership change.
func TestConfigChangeGate_ReleasedOnceChangeIsApplied(t *testing.T) {
	r := newConfigChangeGateRaft(t, 2)

	if r.hasConfigChangeToApply() {
		t.Fatal("a fully APPLIED config change still armed the gate: no " +
			"further membership change would ever be accepted")
	}

	r.becomeCandidate()
	r.becomeLeader()
	if r.pendingConfigChange {
		t.Fatal("becomeLeader armed pendingConfigChange over a fully " +
			"applied log")
	}

	next := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 5, Address: "a5"}
	err := r.step(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  marshalConfigChange(t, next),
		}},
	})
	if err != nil {
		t.Fatalf("config change rejected over a fully applied log: %v", err)
	}
}

// ---------------------------------------------------------------------------
// The local witness role must be cross-checked against replicated
// membership
// ---------------------------------------------------------------------------

// witnessMismatchMembership lists replica 1 as a WITNESS alongside two full
// replicas. A node started with IsWitness=false over this configuration is on
// the wrong side of the witness/full-replica split.
func witnessMismatchMembership() proto.Membership {
	return proto.Membership{
		ConfigChangeID: 4,
		Addresses:      map[uint64]string{2: "addr2", 3: "addr3"},
		Witnesses:      map[uint64]string{1: "addr1"},
	}
}

// TestWitnessRoleMismatch_CannotCampaign is the regression.
//
// r.isWitness comes only from local config; nothing validated it against the
// replicated membership. A node the configuration lists as a witness but that
// was started with IsWitness=false passed every witness gate — they all key
// on the local config — and could win an election. Its leader has been
// sending it metadata-only entries (sendAppend strips the payload for
// witnesses), so the state machine it would then answer reads from never saw
// the entry data: linearizability is violated for the whole shard.
//
// dragonboat fails closed on this disagreement by panicking
// (internal/raft/raft.go addNode/addWitness).
func TestWitnessRoleMismatch_CannotCampaign(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsWitness = false // contradicts the replicated membership below

	lr := newTestLogReader(nil, proto.State{})
	lr.membership = witnessMismatchMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if !r.roleMismatch {
		t.Fatal("a replicated membership that lists this replica as a " +
			"WITNESS while it was started with IsWitness=false was not " +
			"detected: every local witness gate keys on the config, so " +
			"nothing else catches it")
	}

	// The election timer must not produce a candidacy.
	for i := 0; i < 10*int(cfg.ElectionRTT); i++ {
		r.tickFunc(r)
	}
	if r.state == candidate || r.state == preCandidate || r.state == leader {
		t.Fatalf("replica campaigned (state=%v) while the configuration "+
			"lists it as a witness: a term it wins puts a state machine "+
			"that only ever received metadata-only entries in charge of "+
			"answering reads", r.state)
	}

	// TimeoutNow is the other campaign entry point and bypasses startElection.
	err = r.step(proto.Message{Type: proto.TimeoutNow, Term: 1, From: 2, To: 1})
	if !errors.Is(err, ErrWitnessRoleMismatch) {
		t.Fatalf("TimeoutNow accepted (err = %v) on a role-mismatched replica", err)
	}
	if r.state == candidate || r.state == leader {
		t.Fatalf("TimeoutNow made a role-mismatched replica a %v", r.state)
	}
}

// A role-mismatched replica must not serve a linearizable read either: the
// leader has been stripping entry payloads for it, so its state machine
// cannot answer one correctly.
func TestWitnessRoleMismatch_RefusesReadIndex(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsWitness = false

	lr := newTestLogReader(nil, proto.State{})
	lr.membership = witnessMismatchMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeFollower(3, 2) // a leader is known, so the read would be forwarded

	err = r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 42})
	if !errors.Is(err, ErrWitnessRoleMismatch) {
		t.Fatalf("ReadIndex accepted (err = %v) on a replica the "+
			"configuration lists as a witness: its state machine never "+
			"received the entry data the read would return", err)
	}
}

// The reverse disagreement — started as a witness, listed as a full voting
// replica — must latch too. Such a node applies nothing and would answer
// reads from an empty state machine.
func TestWitnessRoleMismatch_ConfiguredWitnessListedAsVoter(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsWitness = true

	lr := newTestLogReader(nil, proto.State{})
	lr.membership = threeNodeMembership() // replica 1 is a full voter here

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if !r.roleMismatch {
		t.Fatal("a replica started as a WITNESS but listed as a full voting " +
			"member was not detected")
	}
}

// An AGREEING configuration must not latch anything, or every witness and
// every full replica in the cluster would stop campaigning and stop reading.
func TestWitnessRole_AgreeingConfigurationIsNotLatched(t *testing.T) {
	t.Run("full replica", func(t *testing.T) {
		cfg := raftTestCfgPreVote(1, 1, false)
		cfg.IsWitness = false
		lr := newTestLogReader(nil, proto.State{})
		lr.membership = threeNodeMembership()
		r, err := newRaft(cfg, lr)
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		if r.roleMismatch {
			t.Fatal("a full replica listed as a full replica was latched")
		}
		if err := r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 1}); errors.Is(err, ErrWitnessRoleMismatch) {
			t.Fatal("a correctly configured full replica was refused a read")
		}
	})

	t.Run("witness", func(t *testing.T) {
		cfg := raftTestCfgPreVote(1, 1, false)
		cfg.IsWitness = true
		lr := newTestLogReader(nil, proto.State{})
		lr.membership = witnessMismatchMembership() // lists 1 as a witness
		r, err := newRaft(cfg, lr)
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		if r.roleMismatch {
			t.Fatal("a witness listed as a witness was latched")
		}
	})

	t.Run("not in configuration", func(t *testing.T) {
		// A joining node knows only the existing members. The configuration
		// makes no statement about its role yet, so nothing may be latched.
		cfg := raftTestCfgPreVote(1, 1, false)
		cfg.IsWitness = false
		lr := newTestLogReader(nil, proto.State{})
		lr.membership = proto.Membership{
			Addresses: map[uint64]string{2: "addr2", 3: "addr3"},
		}
		r, err := newRaft(cfg, lr)
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		if r.roleMismatch {
			t.Fatal("a replica absent from the configuration was latched")
		}
	})
}

// TG-16. checkSelfRole's `if !inConfig { return }` early return.
//
// The check compares the LOCAL witness flag against the membership's witness
// list. For a replica the membership does not mention, "not listed as a
// witness" is not a statement that the replica is a full voter — the
// configuration says nothing about it at all. Without the early return, that
// silence reads as "listed as a full replica" and a replica started as a
// WITNESS latches a role mismatch against a configuration that never disagreed
// with it.
//
// That is the normal state of a JOINING witness: it is started with
// IsWitness=true and knows only the existing members until the AddWitness
// naming it commits. Latching there logs an ERROR declaring the replica
// unsafe, and turns its TimeoutNow and read paths into
// ErrWitnessRoleMismatch — a diagnosis of a corrupted replica, produced by the
// one check whose entire job is to distinguish a corrupted replica from a
// healthy one.
//
// The existing "not in configuration" coverage cannot see this: it uses
// IsWitness=false, where listedWitness==false agrees with the local flag and
// the early return changes nothing.
func TestCheckSelfRole_AbsentFromConfigurationMakesNoStatement(t *testing.T) {
	// The configuration lists the two existing members; this replica (1) is
	// joining and is not mentioned anywhere.
	joiningMembership := func() proto.Membership {
		return proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{2: "addr2", 3: "addr3"},
		}
	}

	t.Run("joining witness is not latched", func(t *testing.T) {
		cfg := raftTestCfgPreVote(1, 1, false)
		cfg.IsWitness = true

		lr := newTestLogReader(nil, proto.State{})
		lr.membership = joiningMembership()

		r, err := newRaft(cfg, lr)
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		if r.roleMismatch {
			t.Fatal("a WITNESS the configuration does not mention was latched as " +
				"role-mismatched: absence from the configuration is not a claim " +
				"that this replica is a full voter, and a joining witness is in " +
				"exactly this state until the AddWitness naming it commits")
		}
		if err := r.handleTimeoutNow(proto.Message{Type: proto.TimeoutNow}); errors.Is(err, ErrWitnessRoleMismatch) {
			t.Fatal("a joining witness was refused with ErrWitnessRoleMismatch: the " +
				"latch turned a healthy replica into a diagnosed corrupt one")
		}
	})

	// Control: once the configuration DOES make a statement about this
	// replica, the disagreement must still be caught. Without this the test
	// above would also pass against a checkSelfRole that never latches.
	t.Run("witness listed as a voter is latched", func(t *testing.T) {
		cfg := raftTestCfgPreVote(1, 1, false)
		cfg.IsWitness = true

		lr := newTestLogReader(nil, proto.State{})
		ms := joiningMembership()
		ms.Addresses[1] = "addr1" // now listed, as a FULL voter
		lr.membership = ms

		r, err := newRaft(cfg, lr)
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		if !r.roleMismatch {
			t.Fatal("a replica started as a WITNESS but listed as a full voter was " +
				"not latched")
		}
	})
}

// A leader that only learns of the disagreement from a committed config
// change must step down: it has been answering reads and counting quorums it
// had no right to.
func TestWitnessRoleMismatch_LeaderStepsDownWhenLearned(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsWitness = false
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = threeNodeMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeCandidate()
	r.becomeLeader()
	if r.state != leader {
		t.Fatalf("fixture: expected leader, got %v", r.state)
	}

	// A snapshot delivers a configuration that calls this replica a witness.
	r.rebuildRemotesFromMembership(witnessMismatchMembership())

	if !r.roleMismatch {
		t.Fatal("the disagreement carried by the snapshot membership was not detected")
	}
	if r.state == leader {
		t.Fatal("a leader that learned the configuration calls it a witness " +
			"stayed leader: it keeps answering reads out of a state machine " +
			"the leader path never sent entry data to")
	}
}

// ---------------------------------------------------------------------------
// TG-10 / TG-15: the witness role latch on the paths that were untested
// ---------------------------------------------------------------------------

// TG-15. syncRaftRemotes is the only DYNAMIC detection path for a role
// disagreement: construction (newRaft -> initRemotes) catches a mismatch that
// exists at startup, and snapshot restore catches one that arrives whole, but
// a replica that JOINS as a witness while it was started with IsWitness=false
// only learns about it when the committed AddWitness naming it is applied to
// membership. Nothing else cross-checks the two — every witness gate in the
// host and in raft keys on the LOCAL config — so if this call is missing the
// replica runs a payload-stripped state machine while believing itself a full
// replica, indefinitely.
func TestSyncRaftRemotes_LatchesWitnessRoleMismatchOnCommittedAddWitness(t *testing.T) {
	cfg := testConfigWithID(1, 4)
	cfg.IsWitness = false // contradicts the AddWitness applied below

	// Replica 4 is not yet in the configuration: it is joining.
	ms := proto.Membership{
		ConfigChangeID: 1,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
	}
	p := newTestPeer(t, cfg, proto.State{}, ms)
	if p.r.roleMismatch {
		t.Fatal("fixture: a replica the configuration does not mention must not " +
			"be flagged; it makes no statement about its role")
	}

	// The cluster adds it as a WITNESS and the change commits.
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddWitness,
		ReplicaID: 4,
		Address:   "a4",
	}, 2); err != nil {
		t.Fatalf("apply AddWitness: %v", err)
	}
	p.syncRaftRemotes()

	if !p.r.roleMismatch {
		t.Fatal("a committed AddWitness naming this replica did not latch the " +
			"role mismatch: the leader now strips entry payloads for it while " +
			"every local gate still treats it as a full replica, so a state " +
			"machine that never saw the data can win an election and answer " +
			"linearizable reads out of it")
	}

	// The latch must actually refuse the two operations that turn the bad
	// state machine into an authoritative answer.
	if err := p.r.handleFollowerReadIndex(proto.Message{Type: proto.ReadIndex, Hint: 1}); !errors.Is(err, ErrWitnessRoleMismatch) {
		t.Fatalf("a role-mismatched replica served a read: %v", err)
	}
	p.r.startElection()
	if p.r.state == candidate || p.r.state == preCandidate {
		t.Fatal("a role-mismatched replica campaigned")
	}
}

// The latch must CLEAR when the disagreement is resolved, or a replica whose
// configuration and membership are brought back into agreement stays
// permanently unelectable and unable to serve reads. syncRaftRemotes is the
// clearing path too: Peer.Handle replaces the membership wholesale from a
// snapshot and then calls it.
func TestSyncRaftRemotes_ClearsWitnessRoleMismatchWhenRolesAgree(t *testing.T) {
	cfg := testConfigWithID(1, 4)
	cfg.IsWitness = false

	p := newTestPeer(t, cfg, proto.State{}, proto.Membership{
		ConfigChangeID: 1,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		Witnesses:      map[uint64]string{4: "a4"},
	})
	if !p.r.roleMismatch {
		t.Fatal("fixture: the mismatch must be latched at construction")
	}

	// A snapshot delivers a configuration that lists this replica as the full
	// voting member it was started as.
	p.membership = newMembership(proto.Membership{
		ConfigChangeID: 5,
		Addresses:      map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4"},
	})
	p.publishMembership()
	p.syncRaftRemotes()

	if p.r.roleMismatch {
		t.Fatal("the latch survived a configuration that agrees with the " +
			"local role: the replica can never campaign or serve reads again")
	}
}

// TG-10. The LEADER-side read gate.
//
// The follower gate (handleFollowerReadIndex) is covered; the leader gate is
// the one that matters. A follower refusing a read merely fails the request —
// the client retries against the leader. A LEADER that answers one from a
// payload-stripped state machine returns data the shard never agreed on,
// under the linearizability guarantee, to a client that has no way to tell.
// checkSelfRole steps a mismatched leader down, but a ReadIndex already
// dispatched in the same inbox batch reaches this handler with the latch set
// and the role still leader, which is exactly the window this gate covers.
func TestHandleLeaderReadIndex_RefusesOnWitnessRoleMismatch(t *testing.T) {
	r := newRegressionRaft(t, 1, map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
	r.becomeCandidate()
	r.becomeLeader()
	r.log.commitTo(r.log.lastIndex())
	r.log.appliedTo(r.log.lastIndex())

	// Preconditions: without the latch this read WOULD be answered, so the
	// assertion below cannot pass for an unrelated reason.
	if err := r.handleLeaderReadIndex(proto.Message{Type: proto.ReadIndex, Hint: 7}); err != nil {
		t.Fatalf("fixture: the leader must be able to serve this read: %v", err)
	}
	if len(r.readStates) == 0 && len(r.pendingReadIndex) == 0 {
		t.Fatal("fixture: the read produced neither a read state nor a pending entry")
	}
	r.readStates = nil
	r.pendingReadIndex = r.pendingReadIndex[:0]

	// checkSelfRole has latched the disagreement; the step-down it performs
	// lands after the messages already drained from this inbox batch.
	r.roleMismatch = true

	err := r.handleLeaderReadIndex(proto.Message{Type: proto.ReadIndex, Hint: 8})
	if !errors.Is(err, ErrWitnessRoleMismatch) {
		t.Fatalf("a role-confused LEADER accepted a linearizable read "+
			"(err = %v): the configuration calls it a witness, so the leader "+
			"has been stripping entry payloads for it and its state machine "+
			"never saw the data this read returns", err)
	}
	if len(r.readStates) != 0 || len(r.pendingReadIndex) != 0 {
		t.Fatalf("the refused read still produced read state (%d ready, %d pending)",
			len(r.readStates), len(r.pendingReadIndex))
	}
}

// observerMismatchMembership lists replica 1 as a NON-VOTING observer among
// four real voters. Four is deliberate: with an even voting set the two quorum
// formulas actually differ. The configuration's quorum is quorumOf(4) = 3,
// while a replica that wrongly counts itself computes quorumOf(4+1) = 3 and
// supplies one of those itself — so it needs only 2 real grants where the
// configuration requires 3. That gap is what lets two observers be elected in
// the same term.
func observerMismatchMembership() proto.Membership {
	return proto.Membership{
		ConfigChangeID: 4,
		Addresses: map[uint64]string{
			2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
		},
		Observers: map[uint64]string{1: "addr1"},
	}
}

// TestObserverRole_ListedObserverCannotCampaignWhateverItsConfig is the
// observer counterpart to TestWitnessRoleMismatch_CannotCampaign.
//
// A replica the configuration lists as a non-voting observer, but which was
// started with IsObserver=false, must not campaign. If startElection gated on
// the LOCAL flag, voters would grant it a vote because handleVoteRequest's
// configuration test is isKnownMember, and an unconditional +1 for self in
// numVotingMembers would size quorum over V+1 while the real voting set was V
// and count its own vote toward it. Two such replicas could each collect a
// "majority" from disjoint halves of the real voting set and both be elected
// in the SAME term.
//
// Rather than latching the disagreement (roleMismatch), the observer role
// FOLLOWS the replicated membership:
// config.IsObserver is only the initial hint, so the replica simply IS an
// observer here — it cannot campaign, by either entry point — and nothing is
// latched, because nothing is wrong with it: observers and voters are
// replicated identically, so the replica serves the same reads any observer
// does. This test pins both: no candidacy, and no latch (role adopted).
func TestObserverRole_ListedObserverCannotCampaignWhateverItsConfig(t *testing.T) {
	cfg := raftTestCfgPreVote(1, 1, false)
	cfg.IsObserver = false // the replicated membership below says otherwise

	lr := newTestLogReader(nil, proto.State{})
	lr.membership = observerMismatchMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if !r.isObserver {
		t.Fatal("a replica the replicated membership lists as a non-voting " +
			"OBSERVER did not take that role: startElection gates on it, so " +
			"nothing else stops it campaigning against a quorum it is not in")
	}
	if r.roleMismatch {
		t.Fatal("an observer/voter difference was latched as a role mismatch: " +
			"the observer role follows the membership and must not refuse reads")
	}

	// The election timer must not produce a candidacy.
	for i := 0; i < 10*int(cfg.ElectionRTT); i++ {
		r.tickFunc(r)
	}
	if r.state == candidate || r.state == preCandidate || r.state == leader {
		t.Fatalf("replica campaigned (state=%v) while the configuration "+
			"lists it as a non-voting observer: it counts its own vote "+
			"toward a quorum computed over a voting set it is not in", r.state)
	}

	// TimeoutNow is the other campaign entry point and bypasses startElection.
	if err := r.step(proto.Message{Type: proto.TimeoutNow, Term: 1, From: 2, To: 1}); err != nil {
		t.Fatalf("TimeoutNow on an observer: %v", err)
	}
	if r.state == candidate || r.state == leader {
		t.Fatalf("TimeoutNow made an observer a %v", r.state)
	}
}

// TestObserverCandidate_IsDeniedAVote is the independent second half of the
// fix, enforced by every VOTER rather than by the miscounting candidate.
//
// handleVoteRequest's configuration test is isKnownMember, which is true for
// observers because getRemote searches r.observers. That is correct for
// witnesses — they vote — and wrong for observers, which by definition do not.
// This holds even when the candidate's own role latch is bypassed.
func TestObserverCandidate_IsDeniedAVote(t *testing.T) {
	// This replica (2) is a real voter; replica 1 is a non-voting observer.
	cfg := raftTestCfgPreVote(1, 2, false)
	lr := newTestLogReader(nil, proto.State{})
	lr.membership = observerMismatchMembership()

	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	if _, ok := r.observers[1]; !ok {
		t.Fatal("fixture is wrong: replica 1 must be a known observer on " +
			"this voter, or the test proves nothing")
	}

	r.msgs = r.msgs[:0]
	if err := r.step(proto.Message{
		Type: proto.RequestVote, From: 1, To: 2, Term: r.term + 1,
	}); err != nil {
		t.Fatalf("step(RequestVote): %v", err)
	}

	if len(r.msgs) != 1 {
		t.Fatalf("expected exactly one vote response, got %d", len(r.msgs))
	}
	if !r.msgs[0].Reject {
		t.Fatal("a voter GRANTED a vote to a candidate the configuration " +
			"lists as a non-voting observer: two such candidates can each " +
			"collect a majority from disjoint halves of the real voting set " +
			"and both be elected in the same term")
	}
	if r.vote == 1 {
		t.Fatal("the voter recorded its durable vote for a non-voting " +
			"observer, consuming this term's vote")
	}
}
