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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// firstMsgOfType returns the first drained message of the given type.
func firstMsgOfType(msgs []proto.Message, t uint64) *proto.Message {
	for i := range msgs {
		if msgs[i].Type == t {
			return &msgs[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Heartbeat commit index must be capped at the peer's known match
// ---------------------------------------------------------------------------

// TestHeartbeatCommitIsCappedAtPeerMatch checks the heartbeat commit is capped at the peer's match index.
func TestHeartbeatCommitIsCappedAtPeerMatch(t *testing.T) {
	leader := newTestRaft(1)
	leader.becomeCandidate()
	leader.becomeLeader()
	leader.drainMessages()

	// The leader has committed through index 10.
	leader.log.append(makeEntries(leader.log.lastIndex()+1, leader.log.lastIndex()+10, leader.term))
	leader.log.commitTo(leader.log.lastIndex())
	committed := leader.log.committed

	// Follower 2 has acknowledged NOTHING (match stays 0): a replica that
	// just restarted, or one the leader has not successfully probed yet.
	if leader.remotes[2].match != 0 {
		t.Fatalf("fixture: expected match 0 for peer 2, got %d", leader.remotes[2].match)
	}

	leader.sendHeartbeatTo(2, 0, 0)
	hb := firstMsgOfType(leader.drainMessages(), proto.Heartbeat)
	if hb == nil {
		t.Fatal("no Heartbeat produced")
	}
	if hb.Commit != 0 {
		t.Fatalf("heartbeat to a peer with match=0 advertised Commit=%d "+
			"(leader committed=%d): the follower commits to "+
			"min(msg.Commit, its OWN lastIndex), so a divergent uncommitted "+
			"tail at those indices gets committed and APPLIED at indices "+
			"this leader will truncate", hb.Commit, committed)
	}

	// Once the peer's match catches up, the commit index may be advertised.
	leader.remotes[2].match = committed
	leader.sendHeartbeatTo(2, 0, 0)
	hb = firstMsgOfType(leader.drainMessages(), proto.Heartbeat)
	if hb == nil {
		t.Fatal("no Heartbeat produced after match advanced")
	}
	if hb.Commit != committed {
		t.Fatalf("heartbeat to a fully caught-up peer advertised Commit=%d, want %d",
			hb.Commit, committed)
	}
}

// TestHeartbeatCommitCap_FollowerDoesNotApplyItsOwnDivergentTail is the
// end-to-end consequence: the follower must not commit its divergent
// uncommitted entries on the strength of a heartbeat alone.
func TestHeartbeatCommitCap_FollowerDoesNotApplyItsOwnDivergentTail(t *testing.T) {
	leader := newTestRaft(1)
	leader.becomeCandidate() // term 1
	leader.becomeLeader()
	leader.drainMessages()
	// Give the leader a log through index 10 and commit it.
	leader.log.append(makeEntries(leader.log.lastIndex()+1, 11, leader.term))
	leader.log.commitTo(10)
	leader.term = 7
	for _, rm := range leader.remotes {
		rm.match = 0
	}

	// Follower 2 holds entries 1..10 from term 5 — a divergent tail the
	// term-7 leader will truncate. Nothing is committed on it.
	follower := newTestRaft(2)
	follower.becomeFollower(7, 1)
	follower.log.append(makeEntries(1, 11, 5))
	if follower.log.committed != 0 {
		t.Fatalf("fixture: follower committed = %d, want 0", follower.log.committed)
	}

	leader.sendHeartbeatTo(2, 0, 0)
	hb := firstMsgOfType(leader.drainMessages(), proto.Heartbeat)
	if hb == nil {
		t.Fatal("no Heartbeat produced")
	}
	hb.Term = leader.term
	hb.From = 1
	hb.To = 2
	if err := follower.step(*hb); err != nil {
		t.Fatalf("step Heartbeat: %v", err)
	}

	if follower.log.committed != 0 {
		t.Fatalf("follower committed to %d on a heartbeat from a leader whose "+
			"match for it is 0: those are its OWN term-5 entries, which the "+
			"term-7 leader truncates — they reach the state machine and are "+
			"then overwritten, violating State Machine Safety",
			follower.log.committed)
	}
}

// Pending ReadIndex records the committed index, never lastIndex.

// TestReadIndexRecordsCommittedNotLastIndex checks the read index is the commit index at request time (PhD 6.4).
func TestReadIndexRecordsCommittedNotLastIndex(t *testing.T) {
	r := newTestRaft(1)
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	// Commit the leader's no-op so ReadIndex is allowed (PhD 6.4 requires a
	// committed entry in the current term).
	r.log.commitTo(r.log.lastIndex())
	for _, rm := range r.remotes {
		rm.match = r.log.committed
	}
	committed := r.log.committed

	// An uncommitted tail: 5 entries beyond the commit index.
	r.log.append(makeEntries(r.log.lastIndex()+1, r.log.lastIndex()+5, r.term))
	if r.log.lastIndex() == committed {
		t.Fatal("fixture: the log must hold an uncommitted tail")
	}

	if err := r.step(proto.Message{Type: proto.ReadIndex, Origin: proto.OriginLocal, Hint: 77}); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(r.pendingReadIndex) != 1 {
		t.Fatalf("expected 1 pending read index, got %d", len(r.pendingReadIndex))
	}
	if got := r.pendingReadIndex[0].index; got != committed {
		t.Fatalf("pending ReadIndex recorded index %d with committed=%d and "+
			"lastIndex=%d: a read served above the commit index can observe "+
			"entries a later leader truncates, which is a dirty read served "+
			"as linearizable", got, committed, r.log.lastIndex())
	}
}

// ---------------------------------------------------------------------------
// Lease acks must be reset at each CheckQuorum window
// ---------------------------------------------------------------------------

// TestLeaseAcksResetEachCheckQuorumWindow checks lease acks reset at each CheckQuorum window, with the clock frozen.
func TestLeaseAcksResetEachCheckQuorumWindow(t *testing.T) {
	cfg := raftTestCfgCheckQuorum(1, 1, true)
	cfg.LeaseRead = true
	lr := newTestLogReaderWithMembership(map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	setTestLeaseClock(r)
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	// A full quorum acks the heartbeat early in this CheckQuorum window.
	r.remoteLastAck[2] = r.now()
	r.remoteLastAck[3] = r.now()
	r.recordActivity()
	for _, rm := range r.remotes {
		rm.setActive()
	}
	if !r.hasValidLease() {
		t.Fatal("fixture: a fresh quorum of acks must produce a valid lease")
	}

	// Run out the CheckQuorum window with NO further acks. The peers stay
	// "active" so the leader does not step down; only the lease must lapse.
	for i := 0; i < r.electionTimeout; i++ {
		for _, rm := range r.remotes {
			rm.setActive()
		}
		r.tickFunc(r)
	}
	if r.state != leader {
		t.Fatalf("fixture: leader stepped down (state=%v); this test is about "+
			"the lease, not CheckQuorum", r.state)
	}

	if r.hasValidLease() {
		t.Fatalf("the read lease survived a whole CheckQuorum window with no "+
			"fresh ack (recorded acks: %v, electionTick=%d): resetLeaseAcks "+
			"did not run at the window boundary", r.remoteLastAck, r.electionTick)
	}
}

// ---------------------------------------------------------------------------
// In-flight ReadIndex acks must be re-based when bit positions move
// ---------------------------------------------------------------------------

// TestRebuildReplicaToBitRebasesInflightReadIndexAcks checks ackBits are re-based when bit positions move.
func TestRebuildReplicaToBitRebasesInflightReadIndexAcks(t *testing.T) {
	r := newTestRaft(1)
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()
	r.log.commitTo(r.log.lastIndex())

	// A pending ReadIndex with self + replica 2 already acked.
	bit2 := r.replicaToBit[2]
	r.pendingReadIndex = append(r.pendingReadIndex, readIndexStatus{
		key:      1,
		index:    r.log.committed,
		ctx:      1,
		ackBits:  1<<r.replicaToBit[r.replicaID] | 1<<bit2,
		ackCount: 2,
	})

	// Membership changes during leadership: replica 4 joins.
	r.remotes[4] = newRemote(r.log.lastIndex()+1, maxInflightMsgs)
	r.rebuildReplicaToBit()

	selfMask := uint64(1) << r.replicaToBit[r.replicaID]
	if got := r.pendingReadIndex[0].ackBits; got != selfMask {
		t.Fatalf("in-flight ReadIndex kept ackBits %#x across a bit-position "+
			"rebuild (want self-only %#x): the recorded bits refer to the OLD "+
			"mapping, so a replica whose position moved acks into a fresh bit "+
			"and is counted TWICE, letting a sub-quorum satisfy the read",
			got, selfMask)
	}
	if got := r.pendingReadIndex[0].ackCount; got != 1 {
		t.Fatalf("in-flight ReadIndex kept ackCount %d across a bit-position "+
			"rebuild, want 1 (self)", got)
	}
	if !r.readIndexBroadcastPending {
		t.Fatal("no fresh confirmation heartbeat round was armed after the " +
			"re-base: responses to the previous round were just discarded and " +
			"regular heartbeats carry ctx=0, which never confirms a ReadIndex")
	}
}

// ---------------------------------------------------------------------------
// The 64 voting member cap
// ---------------------------------------------------------------------------

// TestVotingMemberCapRejectsSixtyFifthVoter checks the propose gate refuses a 65th voter, including on forwarded proposals.
func TestVotingMemberCapRejectsSixtyFifthVoter(t *testing.T) {
	members := make(map[uint64]string, maxVotingMembers)
	for id := uint64(1); id <= maxVotingMembers; id++ {
		members[id] = "addr"
	}
	r := newTestRaftWithMembers(1, members)
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	if len(r.remotes)+1 != maxVotingMembers {
		t.Fatalf("fixture: expected %d voting members, got %d",
			maxVotingMembers, len(r.remotes)+1)
	}

	// A follower-forwarded proposal: it reaches the leader with no prior
	// validation at all.
	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: maxVotingMembers + 1,
		Address:   "addr65",
	}
	err := r.step(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: []proto.Entry{{
			Type: proto.EntryConfigChange,
			Cmd:  marshalConfigChange(t, cc),
		}},
	})
	if !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("a %dth voting member was accepted into the log (err = %v): "+
			"the committed replay path does not re-check, so "+
			"rebuildReplicaToBit's invariant panics on every node that "+
			"becomes leader — permanent shard death",
			maxVotingMembers+1, err)
	}

	// The rejected entry must not have entered the log.
	if r.log.lastIndex() != r.log.committed && r.log.lastIndex() > 1 {
		entries, eerr := r.log.entries(r.log.committed+1, r.log.lastIndex()+1, 0)
		if eerr == nil {
			for i := range entries {
				if entries[i].Type&proto.EntryTypeMask == proto.EntryConfigChange {
					t.Fatal("the over-cap config change entered the log despite the rejection")
				}
			}
		}
	}
}

// TestSnapshotMembershipOverVotingCapIsRejected checks a snapshot whose membership exceeds the ackBits word is rejected.
func TestSnapshotMembershipOverVotingCapIsRejected(t *testing.T) {
	r := newTestRaft(1)
	r.becomeFollower(1, 2)
	remotesBefore := len(r.remotes)

	addrs := make(map[uint64]string, maxVotingMembers+1)
	for id := uint64(1); id <= uint64(maxVotingMembers)+1; id++ {
		addrs[id] = fmt.Sprintf("addr%d", id)
	}

	err := r.step(proto.Message{
		Type:   proto.InstallSnapshot,
		Origin: proto.OriginLocal,
		From:   2,
		To:     1,
		Term:   1,
		Snapshot: proto.Snapshot{
			Index:      10,
			Term:       1,
			Membership: proto.Membership{Addresses: addrs},
		},
	})
	if !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("a snapshot naming %d voting members was accepted (err = %v): "+
			"rebuildRemotesFromMembership installs the whole map, and "+
			"rebuildReplicaToBit then panics the step worker on every "+
			"election this replica wins",
			maxVotingMembers+1, err)
	}

	// Rejection must be total. A partially adopted configuration is worse
	// than the panic: quorum() would be computed over a set no committed
	// entry put there.
	if len(r.remotes) != remotesBefore {
		t.Fatalf("remotes = %d, want %d: the rejected snapshot's membership "+
			"was installed anyway", len(r.remotes), remotesBefore)
	}
	if r.log.committed != 0 {
		t.Fatalf("committed = %d, want 0: the rejected snapshot advanced the "+
			"commit index", r.log.committed)
	}

	// The replica must still be usable. The whole point of dropping the
	// message rather than asserting is that this election does not panic.
	electLeader(r)
	if r.state != leader {
		t.Fatalf("state = %v, want leader: the shard did not recover from the "+
			"rejected snapshot", r.state)
	}
}

// TestSnapshotMembershipAtVotingCapIsAccepted checks a snapshot at the cap is accepted, counting self at bit 0 when unlisted.
func TestSnapshotMembershipAtVotingCapIsAccepted(t *testing.T) {
	t.Run("64 voters including self is accepted", func(t *testing.T) {
		r := newTestRaft(1)
		r.becomeFollower(1, 2)

		addrs := make(map[uint64]string, maxVotingMembers)
		for id := uint64(1); id <= uint64(maxVotingMembers); id++ {
			addrs[id] = fmt.Sprintf("addr%d", id)
		}
		if got := snapshotVotingBitCount(proto.Membership{Addresses: addrs}, 1); got != maxVotingMembers {
			t.Fatalf("snapshotVotingBitCount = %d, want %d", got, maxVotingMembers)
		}

		err := r.step(proto.Message{
			Type:   proto.InstallSnapshot,
			Origin: proto.OriginLocal,
			From:   2,
			To:     1,
			Term:   1,
			Snapshot: proto.Snapshot{
				Index:      10,
				Term:       1,
				Membership: proto.Membership{Addresses: addrs},
			},
		})
		if err != nil {
			t.Fatalf("a snapshot naming exactly %d voting members was rejected "+
				"(err = %v): a shard at the supported maximum cannot receive a "+
				"snapshot, so it can never recover a lagging replica",
				maxVotingMembers, err)
		}
		if r.log.committed != 10 {
			t.Fatalf("committed = %d, want 10: the snapshot was not applied",
				r.log.committed)
		}
	})

	t.Run("64 voters excluding self needs 65 bits and is rejected", func(t *testing.T) {
		r := newTestRaft(1)
		r.becomeFollower(1, 2)

		addrs := make(map[uint64]string, maxVotingMembers)
		for id := uint64(2); id <= uint64(maxVotingMembers)+1; id++ {
			addrs[id] = fmt.Sprintf("addr%d", id)
		}
		if len(addrs) != maxVotingMembers {
			t.Fatalf("fixture: %d addresses, want %d", len(addrs), maxVotingMembers)
		}
		if got := snapshotVotingBitCount(proto.Membership{Addresses: addrs}, 1); got != maxVotingMembers+1 {
			t.Fatalf("snapshotVotingBitCount = %d, want %d: self is seated at "+
				"bit 0 whether or not the configuration lists it",
				got, maxVotingMembers+1)
		}

		err := r.step(proto.Message{
			Type:   proto.InstallSnapshot,
			Origin: proto.OriginLocal,
			From:   2,
			To:     1,
			Term:   1,
			Snapshot: proto.Snapshot{
				Index:      10,
				Term:       1,
				Membership: proto.Membership{Addresses: addrs},
			},
		})
		if !errors.Is(err, ErrTooManyVotingMembers) {
			t.Fatalf("err = %v, want ErrTooManyVotingMembers: %d voters none of "+
				"which is this replica still needs %d bit positions",
				err, maxVotingMembers, maxVotingMembers+1)
		}
	})

	t.Run("witnesses count toward the same word", func(t *testing.T) {
		// Witnesses vote, so rebuildReplicaToBit seats them too. A gate that
		// looked only at Addresses would let a membership of 40 voters and 40
		// witnesses through, which needs 81 bits.
		addrs := make(map[uint64]string, 40)
		for id := uint64(1); id <= 40; id++ {
			addrs[id] = fmt.Sprintf("addr%d", id)
		}
		wits := make(map[uint64]string, 40)
		for id := uint64(41); id <= 80; id++ {
			wits[id] = fmt.Sprintf("wit%d", id)
		}
		m := proto.Membership{Addresses: addrs, Witnesses: wits}
		if got := snapshotVotingBitCount(m, 1); got != 80 {
			t.Fatalf("snapshotVotingBitCount = %d, want 80 (39 remotes + 40 "+
				"witnesses + self)", got)
		}

		r := newTestRaft(1)
		r.becomeFollower(1, 2)
		err := r.step(proto.Message{
			Type:   proto.InstallSnapshot,
			Origin: proto.OriginLocal,
			From:   2,
			To:     1,
			Term:   1,
			Snapshot: proto.Snapshot{
				Index:      10,
				Term:       1,
				Membership: m,
			},
		})
		if !errors.Is(err, ErrTooManyVotingMembers) {
			t.Fatalf("err = %v, want ErrTooManyVotingMembers: witnesses occupy "+
				"ackBits positions too", err)
		}
	})
}

// TestOverCapStoredMembershipRefusesShardStartup checks NewPeer refuses an over-cap stored membership with an error.
func TestOverCapStoredMembershipRefusesShardStartup(t *testing.T) {
	overCap := make(map[uint64]string, maxVotingMembers+1)
	for id := uint64(1); id <= uint64(maxVotingMembers)+1; id++ {
		overCap[id] = fmt.Sprintf("addr%d", id)
	}

	// The refusal has to reach the caller StartShard actually uses, because
	// that is what decides whether the host survives. The constructor's own
	// refusal, and its acceptance of exactly the maximum, are asserted by
	// TestBecomeLeader_RefusesTooManyVotingMembers and
	// TestBecomeLeader_SucceedsWithExactly64VotingMembers.
	peer, err := NewPeer(raftTestCfg(1, 1), newTestLogReaderWithMembership(overCap), nil)
	if !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("NewPeer with a stored %d-voter membership returned %v, want "+
			"ErrTooManyVotingMembers: the replica is built and panics the step "+
			"worker the moment it campaigns, on every restart",
			maxVotingMembers+1, err)
	}
	if peer != nil {
		t.Fatal("NewPeer returned a peer alongside the error: a half-built " +
			"replica with an untrackable voting set must not escape the constructor")
	}
}

// TestCommittedOverCapConfigChangeFailsShardNotWorker checks a committed 65th voter fails the shard, not the step worker.
func TestCommittedOverCapConfigChangeFailsShardNotWorker(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)

	// Grow the configuration to exactly the cap. Every one of these is legal
	// and must be accepted, which also proves the guard is not simply
	// rejecting everything.
	idx := p.r.log.lastIndex()
	for id := uint64(4); id <= uint64(maxVotingMembers); id++ {
		idx++
		if err := p.membership.apply(proto.ConfigChange{
			Type:      proto.AddNode,
			ReplicaID: id,
			Address:   fmt.Sprintf("addr%d", id),
		}, idx); err != nil {
			t.Fatalf("apply AddNode %d: %v", id, err)
		}
		if err := p.syncRaftRemotes(); err != nil {
			t.Fatalf("syncRaftRemotes at %d voting members: %v", id, err)
		}
	}
	if got := p.r.votingBitCount(); got != maxVotingMembers {
		t.Fatalf("fixture: votingBitCount = %d, want %d", got, maxVotingMembers)
	}
	remotesAtCap := len(p.r.remotes)
	bitsAtCap := len(p.r.replicaToBit)

	// The 65th voter as a committed entry: apply does not re-validate.
	idx++
	if err := p.membership.apply(proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: uint64(maxVotingMembers) + 1,
		Address:   "addr65",
	}, idx); err != nil {
		t.Fatalf("apply the over-cap AddNode: %v", err)
	}

	err := p.syncRaftRemotes()
	if !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("syncRaftRemotes with a committed %dth voter returned %v, want "+
			"ErrTooManyVotingMembers: the engine never learns the shard is "+
			"unusable and rebuildReplicaToBit panics the step worker instead",
			maxVotingMembers+1, err)
	}

	// Fail closed: the raft mirror stays on the last configuration that fit.
	if len(p.r.remotes) != remotesAtCap {
		t.Fatalf("remotes = %d, want %d: the oversized configuration was "+
			"mirrored into the raft layer anyway", len(p.r.remotes), remotesAtCap)
	}
	if len(p.r.replicaToBit) != bitsAtCap {
		t.Fatalf("replicaToBit has %d entries, want %d: an ack map was built "+
			"for a voting set that does not fit the word", len(p.r.replicaToBit), bitsAtCap)
	}

	// And the failure must be reportable rather than fatal: the peer is still
	// a live object the engine can mark failed and unload.
	if !p.IsLeader() {
		t.Fatal("the peer stopped being leader: the shard should be failed by " +
			"the engine, not torn down inside raft")
	}
}

// TestOverCapConfigChangeSurfacesThroughPeerCommit checks the over-cap error leaves Peer.Commit on the Replicate path.
func TestOverCapConfigChangeSurfacesThroughPeerCommit(t *testing.T) {
	// A follower already at the supported maximum.
	atCap := proto.Membership{Addresses: make(map[uint64]string, maxVotingMembers)}
	for id := uint64(1); id <= uint64(maxVotingMembers); id++ {
		atCap.Addresses[id] = fmt.Sprintf("addr%d", id)
	}
	p := newTestPeer(t, testConfig(), proto.State{}, atCap)

	// Seed the log so the Replicate below has a matching prevLogIndex.
	p.r.log.inmem.merge([]proto.Entry{{Index: 1, Term: 1}})

	cc := proto.ConfigChange{
		Type:      proto.AddNode,
		ReplicaID: uint64(maxVotingMembers) + 1,
		Address:   "addr65",
	}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		t.Fatalf("marshal config change: %v", err)
	}

	// The leader replicates the over-cap config change and commits it. A
	// follower has no propose gate to refuse it: this is exactly the entry a
	// compromised or older leader can land.
	if err := p.Handle(proto.Message{
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
	}); err != nil {
		t.Fatalf("Handle(Replicate): %v", err)
	}

	// Drive the engine's GetUpdate -> Commit contract until Commit reports the
	// fault. It takes more than one cycle: entries persist first, then surface
	// as committed entries on a later cycle.
	var commitErr error
	for range 10 {
		if !p.HasUpdate(0) {
			break
		}
		update, err := p.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("GetUpdate: %v", err)
		}
		if commitErr = p.Commit(update); commitErr != nil {
			break
		}
	}

	if !errors.Is(commitErr, ErrTooManyVotingMembers) {
		t.Fatalf("Peer.Commit returned %v, want ErrTooManyVotingMembers.\n"+
			"The committed %dth voter never reaches the engine, so failCommit "+
			"never runs: node.failed stays false, OnShardFailed never fires, "+
			"and this replica keeps serving with a raft mirror that is "+
			"permanently behind its own committed membership",
			commitErr, maxVotingMembers+1)
	}
}

// TestBecomeLeaderDeclinesOverCapVotingSet checks leadership is refused over the cap without a panic.
func TestBecomeLeaderDeclinesOverCapVotingSet(t *testing.T) {
	r := newTestRaft(1)
	for id := uint64(4); id <= uint64(maxVotingMembers)+1; id++ {
		r.remotes[id] = newRemote(1, maxInflightMsgs)
	}
	if got := r.votingBitCount(); got <= maxVotingMembers {
		t.Fatalf("fixture: votingBitCount = %d, want > %d", got, maxVotingMembers)
	}

	r.becomeCandidate()
	if err := r.becomeLeader(); !errors.Is(err, ErrTooManyVotingMembers) {
		t.Fatalf("becomeLeader returned %v, want ErrTooManyVotingMembers", err)
	}
	// Declined, not half-applied: becomeLeader must not have written a thing.
	if r.state == leader {
		t.Fatal("the replica became leader with a voting set that does not fit " +
			"the ackBits word")
	}

	// checkElectionResult must turn the refusal into the same state a lost
	// election produces, so the step worker carries on.
	r.becomeCandidate()
	for id := range r.remotes {
		r.votes[id] = true
	}
	r.checkElectionResult()
	if r.state == leader {
		t.Fatal("checkElectionResult seated a leader whose voting set does not fit")
	}
}

// ---------------------------------------------------------------------------
// A joining node's own election timer
// ---------------------------------------------------------------------------

// TestJoiningNodeElectionTimerIsSuppressed checks a joining node's election timer does not campaign.
func TestJoiningNodeElectionTimerIsSuppressed(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	// The configuration a joining node has after applying its own AddNode:
	// itself, and nothing else.
	p := newTestPeer(t, cfg, proto.State{}, singleNodeMembership())
	p.MarkJoining()

	for i := 0; i < 20*int(cfg.ElectionRTT); i++ {
		p.Tick()
	}

	if p.r.state != follower {
		t.Fatalf("a joining node campaigned (state=%v): its membership holds "+
			"only itself, so it wins with a quorum of one and acknowledges "+
			"writes the real cluster discards", p.r.state)
	}
	if p.IsLeader() {
		t.Fatal("a joining node became leader off its own election timer")
	}
	for _, m := range p.DrainMessages() {
		if m.Type == proto.RequestVote || m.Type == proto.RequestPreVote {
			t.Fatalf("a joining node emitted %s: the rising term reaches the "+
				"running cluster through ordinary response messages and "+
				"forces the real leader to step down",
				proto.MessageTypeName(m.Type))
		}
	}

	// Once the snapshot has delivered the full configuration and cleared the
	// joining flag, the election timer must work again — otherwise the guard
	// would be a permanent liveness hole rather than a join-window one.
	p.setJoining(false)
	for i := 0; i < 20*int(cfg.ElectionRTT); i++ {
		p.Tick()
	}
	if p.r.state == follower {
		t.Fatal("the election timer never recovered after the joining flag " +
			"was cleared: the guard would be permanent, not join-scoped")
	}
}
