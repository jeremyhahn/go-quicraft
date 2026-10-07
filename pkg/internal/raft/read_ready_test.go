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

// assertReadReady checks the published triple and, on the leader, that it
// agrees with the gate handleLeaderReadIndex applies: ready exactly when a
// ReadIndex is no longer refused with ErrReadIndexNotReady.
func assertReadReady(t *testing.T, p *Peer, wantLeader, wantTerm uint64, want bool, probeKey uint64) {
	t.Helper()
	leaderID, term, ready := p.LeaderTermReady()
	if leaderID != wantLeader || term != wantTerm || ready != want {
		t.Fatalf("LeaderTermReady = (%d, %d, %v), want (%d, %d, %v)",
			leaderID, term, ready, wantLeader, wantTerm, want)
	}
	if !p.IsLeader() {
		return
	}
	err := p.ReadIndex(probeKey)
	if notReady := errors.Is(err, ErrReadIndexNotReady); notReady == ready {
		t.Fatalf("readiness %v disagrees with ReadIndex (err=%v): the event "+
			"would promise a read the leader refuses, or withhold one it serves", ready, err)
	}
}

// newElectedThreeNodeLeader elects replica 1 of three at a fresh term and
// stops there: the no-op is appended but neither acknowledged nor persisted.
func newElectedThreeNodeLeader(t *testing.T) *Peer {
	t.Helper()
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	for range 30 {
		p.Tick()
	}
	mustHandle(t, p, proto.Message{Type: proto.RequestVoteResp, From: 2, Term: p.r.term})
	if !p.IsLeader() {
		t.Fatal("replica 1 did not become leader")
	}
	return p
}

// A leader is read-ready from the first commit of its term, and not a step
// before: not on election, not on a follower's acknowledgement that cannot
// commit yet because the leader's own copy is not durable.
func TestPeer_ReadReady_LeaderAtFirstCommitOfTerm(t *testing.T) {
	p := newElectedThreeNodeLeader(t)
	term := p.r.term
	assertReadReady(t, p, 1, term, false, 1)

	// The follower acknowledges the no-op, but the leader has not persisted
	// it: tryCommit counts the leader's DURABLE index, so nothing commits.
	mustHandle(t, p, proto.Message{
		Type: proto.ReplicateResp, From: 2, Term: term, LogIndex: p.r.log.lastIndex(),
	})
	assertReadReady(t, p, 1, term, false, 2)

	// Persisting it commits it at the persistence edge inside Commit, which
	// must publish readiness itself rather than wait for the next step.
	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	mustCommit(t, p, update)
	if p.r.log.committed == 0 {
		t.Fatal("fixture: the no-op did not commit")
	}
	if _, _, ready := p.LeaderTermReady(); !ready {
		t.Fatal("Commit advanced the commit index into the current term but " +
			"did not publish readiness")
	}
	assertReadReady(t, p, 1, term, true, 3)
}

// Readiness ends with the term. A leader deposed by a higher term is not
// read-ready in the new term, even though its old commit is still there.
func TestPeer_ReadReady_EndsOnTermChange(t *testing.T) {
	p := newTestMultiNodeLeaderPeer(t)
	term := p.r.term
	assertReadReady(t, p, 1, term, true, 1)

	mustHandle(t, p, proto.Message{
		Type: proto.Heartbeat, From: 2, To: 1, Term: term + 1,
	})
	assertReadReady(t, p, 2, term+1, false, 2)
}

// A follower learns the leader's readiness from its own commit index: the
// first committed entry of the current term can only have been committed by
// that term's leader.
func TestPeer_ReadReady_FollowerLearnsFromCommitIndex(t *testing.T) {
	p := newTestPeer(t, testConfigWithID(1, 2), proto.State{}, threeNodeMembership())
	assertReadReady(t, p, 0, 0, false, 0)

	// Leader 1 of term 1 replicates its no-op; nothing is committed yet.
	mustHandle(t, p, proto.Message{
		Type: proto.Replicate, From: 1, To: 2, Term: 1,
		Entries: []proto.Entry{{Index: 1, Term: 1}},
	})
	assertReadReady(t, p, 1, 1, false, 0)

	// The leader's commit index reaches the no-op.
	mustHandle(t, p, proto.Message{
		Type: proto.Heartbeat, From: 1, To: 2, Term: 1, Commit: 1,
	})
	assertReadReady(t, p, 1, 1, true, 0)

	// A new leader at term 2. The follower's commit index still names a
	// term-1 entry, which says nothing about the new leader.
	mustHandle(t, p, proto.Message{
		Type: proto.Replicate, From: 3, To: 2, Term: 2, LogIndex: 1, LogTerm: 1,
		Commit: 1, Entries: []proto.Entry{{Index: 2, Term: 2}},
	})
	assertReadReady(t, p, 3, 2, false, 0)

	mustHandle(t, p, proto.Message{
		Type: proto.Heartbeat, From: 3, To: 2, Term: 2, Commit: 2,
	})
	assertReadReady(t, p, 3, 2, true, 0)
}

// A replica that restarts into a term it already holds commits for is not
// read-ready until it knows that term's leader: readiness is about the leader
// a read would reach, and an unknown leader can be reached by nobody.
func TestPeer_ReadReady_RequiresAKnownLeader(t *testing.T) {
	p := newTestPeerWithLog(t, testConfigWithID(1, 2),
		[]proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}},
		proto.State{Term: 3, Commit: 2}, threeNodeMembership())
	assertReadReady(t, p, 0, 3, false, 0)

	mustHandle(t, p, proto.Message{
		Type: proto.Heartbeat, From: 3, To: 2, Term: 3, Commit: 2,
	})
	assertReadReady(t, p, 3, 3, true, 0)
}
