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

// TestHeartbeatCommitIsCappedAtPeerMatch protects the min() in
// sendHeartbeatTo.
//
// A heartbeat carries the leader's commit index, and handleHeartbeat commits
// to min(msg.Commit, follower.lastIndex()) — the follower's OWN last index,
// with no proof that the entries there came from this leader. A follower that
// holds a divergent uncommitted tail at those indices would therefore commit
// and APPLY its own entries at indices the leader is about to truncate: a
// State Machine Safety violation (two replicas applying different commands at
// the same index).
//
// Capping the advertised commit at the leader's recorded match for that peer
// is what makes the follower's commit provably about entries the leader
// replicated to it.
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

	leader.sendHeartbeatTo(2, 0)
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
	leader.sendHeartbeatTo(2, 0)
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

	leader.sendHeartbeatTo(2, 0)
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

// ---------------------------------------------------------------------------
// Pending ReadIndex must record the COMMITTED index, never lastIndex
// ---------------------------------------------------------------------------

// TestReadIndexRecordsCommittedNotLastIndex protects PhD 6.4: the read index
// handed back to a client must be the leader's COMMIT index at request time.
//
// lastIndex() includes the uncommitted tail. A read served at an uncommitted
// index can observe entries that a later leader truncates — the classic dirty
// read — and the read would be acknowledged as linearizable.
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

	// An UNCOMMITTED tail: 5 entries beyond the commit index.
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

// TestLeaseAcksResetEachCheckQuorumWindow protects resetLeaseAcks() in
// tickLeader.
//
// hasValidLease compares each recorded ack tick against
// electionTick - leaseTimeout. electionTick WRAPS to 0 at the end of every
// CheckQuorum window, so the expiry goes negative and every stale ack tick
// from the previous window looks valid again. The lease would then survive
// roughly two election timeouts with no fresh acknowledgement at all —
// exactly long enough for another replica to win an election and for this one
// to keep serving lease reads as a deposed leader.
func TestLeaseAcksResetEachCheckQuorumWindow(t *testing.T) {
	cfg := raftTestCfgCheckQuorum(1, 1, true)
	cfg.LeaseRead = true
	lr := newTestLogReaderWithMembership(map[uint64]string{1: "a1", 2: "a2", 3: "a3"})
	r, err := newRaft(cfg, lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	// A full quorum acks the heartbeat early in this CheckQuorum window.
	r.remoteLastAck[2] = r.electionTick
	r.remoteLastAck[3] = r.electionTick
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
			"fresh ack (recorded acks: %v, electionTick=%d, leaseTimeout=%d): "+
			"electionTick wrapped to 0, the lease expiry underflowed negative "+
			"and every stale tick looks valid, so a deposed leader keeps "+
			"serving lease reads", r.remoteLastAck, r.electionTick, r.leaseTimeout)
	}
}

// ---------------------------------------------------------------------------
// In-flight ReadIndex acks must be re-based when bit positions move
// ---------------------------------------------------------------------------

// TestRebuildReplicaToBitRebasesInflightReadIndexAcks protects the ackBits
// re-base in rebuildReplicaToBit.
//
// ackBits records WHICH replicas acked a pending ReadIndex, by bit position.
// rebuildReplicaToBit reassigns those positions on every membership change
// during leadership (the maps are unordered, so positions move even for
// replicas that did not change role). A replica whose bit moved can then set
// a SECOND bit for the same ReadIndex, so ackCount reaches quorum on fewer
// distinct replicas than quorum requires — leadership is "confirmed" by a
// sub-quorum and the read can be stale.
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

// TestVotingMemberCapRejectsSixtyFifthVoter protects the cap in
// validateConfigChangePolicy.
//
// ReadIndex ack tracking is a uint64 bitfield, so only bit positions 0..63
// exist. rebuildReplicaToBit asserts the cap and PANICS above it — and the
// committed-replay path deliberately does not re-check (replay must be
// deterministic). A committed 65th voter therefore panics every node the
// moment it becomes leader: the shard can never elect again. This propose
// gate is the last place the entry can be stopped, and it is the ONLY one on
// a follower-forwarded proposal, which bypasses the Host API's validation
// entirely.
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

// ---------------------------------------------------------------------------
// A joining node's own election timer
// ---------------------------------------------------------------------------

// TestJoiningNodeElectionTimerIsSuppressed protects the joining check in
// Peer.Tick.
//
// A joining node's membership is incomplete by construction: the existing
// cluster's members were established by the bootstrap confState, not by
// config change entries, so they are not in the log this node replays. Once
// it has applied its OWN AddNode it holds a configuration of exactly one
// replica — itself — and an election it starts is decided by a quorum of one.
// It would then acknowledge writes the real cluster later discards. Its
// campaign also carries a term the existing cluster has not reached, which
// forces the running leader to step down.
//
// The TimeoutNow half of this guard (Peer.Handle) is covered elsewhere; this
// is the election TIMER half, which is the only path that fires without any
// message arriving at all.
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
