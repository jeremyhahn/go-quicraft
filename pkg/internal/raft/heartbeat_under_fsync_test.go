// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Tests for heartbeat-under-fsync (#390): while a WAL write is in flight the
// step worker sends only what Peer.DrainFreeOrderMessages releases, and a
// HeartbeatResp is released there only when the step that produced it left
// the hard state (term, vote, owed vote window) at its durable value and
// appended nothing.

var hbMembers = map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}

// newHBFollower is replica 2 of a three-voter shard whose durable state is
// state and whose durable log is entries.
func newHBFollower(t *testing.T, cfg config.Config, entries []proto.Entry, state proto.State) *Peer {
	t.Helper()
	lr := newTestLogReader(entries, state)
	lr.membership = proto.Membership{Addresses: hbMembers}
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	return p
}

func countType(msgs []proto.Message, typ uint64) int {
	n := 0
	for i := range msgs {
		if msgs[i].Type == typ {
			n++
		}
	}
	return n
}

func TestHeartbeatUnderFsync_FreeWhenHardStateUnchanged(t *testing.T) {
	p := newHBFollower(t, raftTestCfg(1, 2), nil, proto.State{Term: 5})
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 5, Hint: 7, LogIndex: 3})
	free := p.DrainFreeOrderMessages()
	if countType(free, proto.HeartbeatResp) != 1 {
		t.Fatalf("drain released %v, want the HeartbeatResp", free)
	}
	resp := free[0]
	if resp.To != 1 || resp.Term != 5 || resp.Hint != 7 || resp.LogIndex != 3 {
		t.Fatalf("HeartbeatResp = %+v, want an echo of ctx 7 and round 3 at term 5", resp)
	}
	if n := countType(p.r.msgs, proto.HeartbeatResp); n != 0 {
		t.Fatalf("%d HeartbeatResp still held as ordered", n)
	}
}

// A heartbeat that only advances the commit index changes nothing the
// HeartbeatResp tells the leader, so the reply stays free-order.
func TestHeartbeatUnderFsync_CommitOnlyChangeStaysFree(t *testing.T) {
	entries := []proto.Entry{{Index: 1, Term: 5}, {Index: 2, Term: 5}}
	p := newHBFollower(t, raftTestCfg(1, 2), entries, proto.State{Term: 5})
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 5, Commit: 2})
	if p.r.log.committed != 2 {
		t.Fatalf("committed = %d, want 2", p.r.log.committed)
	}
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 1 {
		t.Fatalf("drain released %d HeartbeatResp after a commit-only step, want 1", n)
	}
}

func TestHeartbeatUnderFsync_TermChangeStaysOrdered(t *testing.T) {
	p := newHBFollower(t, raftTestCfg(1, 2), nil, proto.State{Term: 5})
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 6})
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 0 {
		t.Fatal("a HeartbeatResp at a term not yet durable was released before the fsync")
	}
	upd, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if upd.State.Term != 6 || countType(upd.Messages, proto.HeartbeatResp) != 1 {
		t.Fatalf("update term %d with %d HeartbeatResp, want the reply ordered behind term 6",
			upd.State.Term, countType(upd.Messages, proto.HeartbeatResp))
	}
}

func TestHeartbeatUnderFsync_VoteInSameStepStaysOrdered(t *testing.T) {
	p := newHBFollower(t, raftTestCfg(1, 2), nil, proto.State{Term: 5})
	mustHandle(t, p, proto.Message{Type: proto.RequestVote, From: 3, To: 2, Term: 5})
	if p.r.vote != 3 {
		t.Fatalf("vote = %d, want 3", p.r.vote)
	}
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 3, To: 2, Term: 5})
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 0 {
		t.Fatal("a HeartbeatResp stepped after an undurable vote was released before the fsync")
	}
	// The vote still undurable, a later heartbeat stays ordered as well.
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 3, To: 2, Term: 5})
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 0 {
		t.Fatal("a HeartbeatResp was released while the vote was still undurable")
	}
	upd, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	mustCommit(t, p, upd)
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 3, To: 2, Term: 5})
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 1 {
		t.Fatal("once the vote is durable the HeartbeatResp should be free-order")
	}
}

func TestHeartbeatUnderFsync_AppendInSameStepStaysOrdered(t *testing.T) {
	p := newHBFollower(t, raftTestCfg(1, 2), nil, proto.State{Term: 5})
	mustHandle(t, p, proto.Message{
		Type: proto.Replicate, From: 1, To: 2, Term: 5,
		Entries: []proto.Entry{{Index: 1, Term: 5}},
	})
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 5, Hint: 1})
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 0 {
		t.Fatal("a HeartbeatResp stepped with an append was released before the fsync")
	}
	// The next step appends nothing: its reply is free, the held one is not.
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 5, Hint: 2})
	free := p.DrainFreeOrderMessages()
	if countType(free, proto.HeartbeatResp) != 1 || free[0].Hint != 2 {
		t.Fatalf("drain released %+v, want only the ctx-2 HeartbeatResp", free)
	}
	held := 0
	for _, m := range p.r.msgs {
		if m.Type == proto.HeartbeatResp {
			held++
			if m.Hint != 1 {
				t.Fatalf("held HeartbeatResp ctx %d, want 1", m.Hint)
			}
		}
	}
	if held != 1 || countType(p.r.msgs, proto.ReplicateResp) != 1 {
		t.Fatalf("ordered set %+v, want the ReplicateResp and the ctx-1 HeartbeatResp", p.r.msgs)
	}
}

// Raising the owed vote window is a hard-state change: a lease ack promising
// the window may not leave before the window is durable (PR #207).
func TestHeartbeatUnderFsync_VoteWindowRaiseStaysOrdered(t *testing.T) {
	p := newHBFollower(t, raftTestCfgLeaseRead(1, 2), nil, proto.State{Term: 5})
	setTestLeaseClock(p.r)
	p.SetTickInterval(testLeaseTick)
	if p.r.voteWindowOwed == p.durableState.VoteWindow {
		t.Fatal("setup: the owed window should be raised and not yet durable")
	}
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 5, LogIndex: 1})
	if n := countType(p.DrainFreeOrderMessages(), proto.HeartbeatResp); n != 0 {
		t.Fatal("a lease ack was released before its vote window was durable")
	}
	upd, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	mustCommit(t, p, upd)
	mustHandle(t, p, proto.Message{Type: proto.Heartbeat, From: 1, To: 2, Term: 5, LogIndex: 2})
	free := p.DrainFreeOrderMessages()
	if countType(free, proto.HeartbeatResp) != 1 {
		t.Fatal("with the window durable the lease ack should be free-order")
	}
	if free[0].LogTerm == 0 || free[0].LogTerm > p.durableState.VoteWindow {
		t.Fatalf("ack promises %d, durable window %d", free[0].LogTerm, p.durableState.VoteWindow)
	}
}

// fsyncStallShard routes three Peers by hand. A stalled replica models the
// step worker with commitPending set: it steps its inbox and ticks but sends
// only what DrainFreeOrderMessages releases, and its in-flight update is
// never committed.
type fsyncStallShard struct {
	t       *testing.T
	peers   map[uint64]*Peer
	stalled map[uint64]bool
	inbox   map[uint64][]proto.Message
}

func newFsyncStallShard(t *testing.T, leaseRead bool) *fsyncStallShard {
	t.Helper()
	s := &fsyncStallShard{t: t, peers: map[uint64]*Peer{}, stalled: map[uint64]bool{}, inbox: map[uint64][]proto.Message{}}
	for id := uint64(1); id <= 3; id++ {
		cfg := raftTestCfg(1, id)
		cfg.LeaseRead = leaseRead
		p, err := NewPeer(cfg, newTestLogReaderWithMembership(hbMembers), nil)
		if err != nil {
			t.Fatalf("NewPeer(%d): %v", id, err)
		}
		setTestLeaseClock(p.r)
		passVoteHoldDown(p.r)
		s.peers[id] = p
	}
	return s
}

// step delivers id's inbox and routes what the replica sends.
func (s *fsyncStallShard) step(id uint64) {
	p := s.peers[id]
	in := s.inbox[id]
	s.inbox[id] = nil
	for _, m := range in {
		mustHandle(s.t, p, m)
	}
	var out []proto.Message
	if s.stalled[id] {
		out = append(out, p.DrainFreeOrderMessages()...)
	} else if p.HasUpdate(0) {
		upd, err := p.GetUpdate(true, 0)
		if err != nil {
			s.t.Fatalf("GetUpdate(%d): %v", id, err)
		}
		mustCommit(s.t, p, upd)
		out = append(out, upd.Messages...)
	}
	for _, m := range out {
		s.inbox[m.To] = append(s.inbox[m.To], m)
	}
}

func (s *fsyncStallShard) route() {
	for range 20 {
		for id := uint64(1); id <= 3; id++ {
			s.step(id)
		}
	}
}

func testFollowerStall(t *testing.T, leaseRead bool) {
	s := newFsyncStallShard(t, leaseRead)
	leader := s.peers[1]
	leader.r.startElection()
	s.route()
	if !leader.IsLeader() {
		t.Fatal("replica 1 did not win the election")
	}
	durable := leader.r.log.lastIndex()

	// Both followers extract an update carrying a new entry and stall in
	// its fsync: the update is out, Commit never comes.
	if _, err := leader.Propose([]proto.Entry{{Cmd: []byte("x")}}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	s.step(1)
	for _, id := range []uint64{2, 3} {
		f := s.peers[id]
		for _, m := range s.inbox[id] {
			mustHandle(t, f, m)
		}
		s.inbox[id] = nil
		if _, err := f.GetUpdate(true, 0); err != nil {
			t.Fatalf("GetUpdate(%d): %v", id, err)
		}
		s.stalled[id] = true
	}

	// A bounded stall several election timeouts long.
	for range 5 * leader.r.electionTimeout {
		for id := uint64(1); id <= 3; id++ {
			s.peers[id].Tick()
			s.step(id)
		}
		if !leader.IsLeader() {
			t.Fatalf("leader deposed by CheckQuorum during the follower fsync stall (term %d)", leader.r.term)
		}
		for _, id := range []uint64{2, 3} {
			if m := leader.r.remotes[id].match; m > durable {
				t.Fatalf("leader matched replica %d at %d, past its durable %d", id, m, durable)
			}
		}
		if c := leader.r.log.committed; c > durable {
			t.Fatalf("leader committed %d on acks for an entry no follower made durable (%d)", c, durable)
		}
	}
}

// The leader keeps leadership under CheckQuorum through a bounded fsync stall
// on a quorum of followers, and nothing in the stall acknowledges an index the
// followers have not made durable.
func TestHeartbeatUnderFsync_LeaderSurvivesQuorumFollowerStall(t *testing.T) {
	t.Run("plain", func(t *testing.T) { testFollowerStall(t, false) })
	t.Run("lease_read", func(t *testing.T) { testFollowerStall(t, true) })
}
