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
// F1: vote tally must be taken over the configuration voters
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
// F2: Term==0 is a local-injection escape hatch, not a wire value
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
// F3: a joining node must not be elected on its own one-member view
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
		Type: proto.InstallSnapshot,
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
// F4/F5: SetProcessedIndex must never outrun the recovered commit index
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
// F6: a newly added member must earn its lease contribution
// ---------------------------------------------------------------------------

// A member added during leadership must not contribute to hasValidLease
// before its first real heartbeat ack. Seeding a zero ack tick is
// indistinguishable from an ack recorded at tick 0.
func TestSyncLeaseAcks_NewMemberCannotGrantLease(t *testing.T) {
	ms := threeNodeMembership()
	cfg := testConfigWithID(1, 1)
	cfg.LeaseRead = true
	p := newTestPeer(t, cfg, proto.State{}, ms)

	// Leader with a lease map holding one real ack (replica 2) and no ack
	// from replica 3.
	p.r.state = leader
	p.r.leaderID = 1
	p.r.remoteLastAck = map[uint64]int{2: 1}
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
		t.Fatal("newly added replica 4 was seeded with an ack tick")
	}
	// Voters are now {1,2,3,4}: quorum 3, real acks are self + replica 2.
	if p.r.hasValidLease() {
		t.Fatal("lease granted using a phantom ack from a replica that never acked")
	}
}

// ---------------------------------------------------------------------------
// F7: quiesce must not freeze the lease or the CheckQuorum step-down
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
		Type:    proto.Propose,
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

// hasConfigChangeToApply scans the uncommitted tail in bounded chunks. The
// loop must cover ranges wider than a single chunk.
func TestHasConfigChangeToApply_ScansBeyondOneChunk(t *testing.T) {
	r := newTestRaft(1)
	const total = configChangeScanChunkEntries + 200
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
		t.Fatalf("config change at index %d beyond the first chunk was missed", total)
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

// errInjectedRead is the injected LogReader failure used to exercise the
// fail-closed branch of hasConfigChangeToApply.
var errInjectedRead = errors.New("raft: injected logdb read failure")

// flakyLogReader fails GetRange while fail is true, then behaves normally.
type flakyLogReader struct {
	*testLogReader
	fail bool
}

func (f *flakyLogReader) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	if f.fail {
		return nil, errInjectedRead
	}
	return f.testLogReader.GetRange(low, high, maxSize)
}

// A read error must fail closed, and the latch must be re-evaluated on the
// next call so one transient error does not block membership changes for
// the whole term.
func TestHasConfigChangeToApply_FailsClosedAndRecovers(t *testing.T) {
	inner := newTestLogReader([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}, proto.State{Commit: 1})
	inner.membership = singleNodeMembership()
	lr := &flakyLogReader{testLogReader: inner, fail: true}

	r, err := newRaft(raftTestCfg(1, 1), lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	if !r.hasConfigChangeToApply() {
		t.Fatal("a read failure must fail closed")
	}
	if !r.configChangeScanUnproven {
		t.Fatal("fail-closed scan did not record that absence was unproven")
	}
	if r.configChangeScanFailures == 0 {
		t.Fatal("fail-closed scan was not counted")
	}
	lr.fail = false
	if r.hasConfigChangeToApply() {
		t.Fatal("the fail-closed result was latched across a successful re-scan")
	}
	if r.configChangeScanUnproven {
		t.Fatal("the unproven flag survived a successful scan")
	}
}

// A leader whose becomeLeader scan failed closed must not reject config
// changes for the rest of the term once the log is readable again.
func TestConfigChangeGate_RecoversFromTransientScanFailure(t *testing.T) {
	inner := newTestLogReader([]proto.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}, proto.State{Commit: 1})
	inner.membership = threeNodeMembership()
	lr := &flakyLogReader{testLogReader: inner, fail: true}

	cfg := raftTestCfgPreVote(1, 1, false)
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	r.becomeCandidate()
	r.becomeLeader()
	if !r.pendingConfigChange {
		t.Fatal("becomeLeader did not fail closed on the unreadable tail")
	}

	// The log is readable again and holds no config change.
	lr.fail = false
	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}
	err = r.step(proto.Message{
		Type: proto.Propose,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  marshalConfigChange(t, cc),
		}},
	})
	if err != nil {
		t.Fatalf("config change rejected after the scan recovered: %v", err)
	}
}

// ---------------------------------------------------------------------------
// F2: the config change gate must be anchored at the APPLIED frontier
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

// TestConfigChangeGate_ArmedByCommittedButUnappliedChange is the F2
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
		Type: proto.Propose,
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
		Type: proto.Propose,
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
// F4: the local witness role must be cross-checked against replicated
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

// TestWitnessRoleMismatch_CannotCampaign is the F4 regression.
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

	err = r.step(proto.Message{Type: proto.ReadIndex, Hint: 42})
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
		if err := r.step(proto.Message{Type: proto.ReadIndex, Hint: 1}); errors.Is(err, ErrWitnessRoleMismatch) {
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
