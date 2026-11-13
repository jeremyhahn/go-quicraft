// Copyright 2026 Jeremy Hahn
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
	"context"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// rejoinCase sets up a peer in one role before RejoinAfterPause and states
// what the documentation promises after it.
type rejoinCase struct {
	name string
	// setup puts the peer in the role under test.
	setup func(ctx context.Context, t *testing.T, p *Peer)
	// wantLeader is the leader the peer must report after the rejoin.
	wantLeader uint64
}

// newRejoinPeer returns replica 1 of a three-replica shard, started from
// state, with the peer's update baseline taken so the next GetUpdate shows
// only what changed after setup.
func newRejoinPeer(ctx context.Context, t *testing.T, state proto.State) *Peer {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	return newTestPeer(t, raftTestCfg(1, 1), state, threeNodeMembership())
}

// takeUpdate drains the peer's pending update and returns it.
func takeUpdate(ctx context.Context, t *testing.T, p *Peer) proto.Update {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	u, err := p.GetUpdate(false, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	return u
}

func rejoinCases() []rejoinCase {
	return []rejoinCase{
		{
			name: "follower with leader",
			setup: func(_ context.Context, t *testing.T, p *Peer) {
				t.Helper()
				p.r.becomeFollower(3, 2)
				if p.r.vote != 2 {
					t.Fatalf("setup: vote = %d, want 2", p.r.vote)
				}
			},
			wantLeader: 2,
		},
		{
			name: "leader",
			setup: func(_ context.Context, t *testing.T, p *Peer) {
				t.Helper()
				electLeader(p.r)
				if p.r.state != leader {
					t.Fatalf("setup: state = %v, want leader", p.r.state)
				}
			},
			wantLeader: noLeader,
		},
		{
			name: "candidate",
			setup: func(_ context.Context, t *testing.T, p *Peer) {
				t.Helper()
				p.r.becomeCandidate()
				if p.r.state != candidate || p.r.vote != 1 {
					t.Fatalf("setup: state = %v vote = %d, want candidate voting 1",
						p.r.state, p.r.vote)
				}
			},
			wantLeader: noLeader,
		},
		{
			name: "pre-candidate",
			setup: func(_ context.Context, t *testing.T, p *Peer) {
				t.Helper()
				p.r.becomePreCandidate()
				if p.r.state != preCandidate || p.r.vote != 1 {
					t.Fatalf("setup: state = %v vote = %d, want pre-candidate voting 1",
						p.r.state, p.r.vote)
				}
			},
			wantLeader: noLeader,
		},
	}
}

// startState is the persisted state every case starts from: term 3, and a
// vote already cast in it (for replica 2 when the case is a follower of 2,
// for itself otherwise; becomeCandidate moves to term 4 voting for itself).
func startState(name string) proto.State {
	if name == "follower with leader" {
		return proto.State{Term: 3, Vote: 2}
	}
	return proto.State{Term: 3, Vote: 1}
}

// rejoinSubject builds the peer for c, runs its setup and drains the update
// the setup produced, so the next update reflects the rejoin alone.
func rejoinSubject(ctx context.Context, t *testing.T, c rejoinCase) (*Peer, proto.Update) {
	t.Helper()
	p := newRejoinPeer(ctx, t, startState(c.name))
	c.setup(ctx, t, p)
	return p, takeUpdate(ctx, t, p)
}

// TestPeer_RejoinAfterPause_KeepsTermVoteAndFollowerLeader pins (a), (b) and
// the role half of (c): the replica is a follower at the same term with the
// same vote, a follower keeps its leader, and a leader, candidate or
// pre-candidate has no leader.
func TestPeer_RejoinAfterPause_KeepsTermVoteAndFollowerLeader(t *testing.T) {
	ctx := t.Context()
	for _, c := range rejoinCases() {
		t.Run(c.name, func(t *testing.T) {
			p, _ := rejoinSubject(ctx, t, c)
			term, vote := p.r.term, p.r.vote

			p.RejoinAfterPause()

			if p.r.state != follower {
				t.Fatalf("state = %v, want follower", p.r.state)
			}
			if !p.IsFollower() {
				t.Fatal("IsFollower() = false after rejoin")
			}
			if p.r.term != term {
				t.Fatalf("term = %d, want unchanged %d", p.r.term, term)
			}
			if p.r.vote != vote {
				t.Fatalf("vote = %d, want unchanged %d", p.r.vote, vote)
			}
			if p.r.leaderID != c.wantLeader {
				t.Fatalf("raft leader = %d, want %d", p.r.leaderID, c.wantLeader)
			}
			if got := p.LeaderID(); got != c.wantLeader {
				t.Fatalf("LeaderID() = %d, want %d", got, c.wantLeader)
			}
		})
	}
}

// TestPeer_RejoinAfterPause_RefusesSecondVoteInTerm pins the vote half of
// (c), checked in every role: a RequestVote from another replica at the
// rejoined term, with a log at least as up to date, is refused because the
// vote in that term is already cast, and the vote does not move.
func TestPeer_RejoinAfterPause_RefusesSecondVoteInTerm(t *testing.T) {
	ctx := t.Context()
	for _, c := range rejoinCases() {
		t.Run(c.name, func(t *testing.T) {
			p, _ := rejoinSubject(ctx, t, c)
			vote := p.r.vote
			p.RejoinAfterPause()

			err := p.r.step(proto.Message{
				Type:     proto.RequestVote,
				From:     3,
				To:       1,
				Term:     p.r.term,
				LogTerm:  p.r.term + 10,
				LogIndex: p.r.log.lastIndex() + 10,
			})
			if err != nil {
				t.Fatalf("step RequestVote: %v", err)
			}
			if p.r.vote != vote {
				t.Fatalf("vote = %d after a second request, want %d", p.r.vote, vote)
			}
			var resp *proto.Message
			for i := range p.r.msgs {
				if p.r.msgs[i].Type == proto.RequestVoteResp && p.r.msgs[i].To == 3 {
					resp = &p.r.msgs[i]
				}
			}
			if resp == nil {
				t.Fatal("no RequestVoteResp sent to replica 3")
			}
			if !resp.Reject {
				t.Fatal("RequestVote from replica 3 granted in a term whose vote is cast")
			}
		})
	}
}

// TestPeer_RejoinAfterPause_PersistsNothing pins (d): the GetUpdate after the
// rejoin carries the same HardState as the one before it, so the rejoin
// writes nothing to the log store.
func TestPeer_RejoinAfterPause_PersistsNothing(t *testing.T) {
	ctx := t.Context()
	for _, c := range rejoinCases() {
		t.Run(c.name, func(t *testing.T) {
			p, before := rejoinSubject(ctx, t, c)
			// The engine persists the pre-rejoin update and commits it, so the
			// rejoin is measured against a durable baseline.
			if err := p.Commit(before); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			p.RejoinAfterPause()

			if p.stateUnpersisted {
				t.Fatal("rejoin demanded a hard-state re-persist")
			}
			after := takeUpdate(ctx, t, p)
			if after.State != before.State {
				t.Fatalf("HardState changed by rejoin: before %+v, after %+v",
					before.State, after.State)
			}
			if len(after.EntriesToSave) != 0 {
				t.Fatalf("rejoin produced %d entries to save", len(after.EntriesToSave))
			}
			// GetUpdate always carries the hard state; the engine skips
			// SaveState only when the update is ReadOnly.
			if !after.ReadOnly {
				t.Fatalf("rejoin update is not ReadOnly, the engine would persist it: "+
					"%d committed entries, snapshot index %d",
					len(after.CommittedEntries), after.Snapshot.Index)
			}
		})
	}
}

// TestPeer_RejoinAfterPause_ElectionTimeoutCampaigns pins (e): after the
// rejoin, ticking past the election timeout starts a campaign as usual.
func TestPeer_RejoinAfterPause_ElectionTimeoutCampaigns(t *testing.T) {
	ctx := t.Context()
	for _, c := range rejoinCases() {
		t.Run(c.name, func(t *testing.T) {
			p, _ := rejoinSubject(ctx, t, c)
			// Seed a timer one tick short of expiry. Set directly: ticking a
			// leader or candidate there would change its role first.
			p.r.electionTick = p.r.randomizedElectionTimeout - 1

			p.RejoinAfterPause()
			_ = takeUpdate(ctx, t, p)
			if p.r.electionTick != 0 {
				t.Fatalf("electionTick = %d after rejoin, want 0", p.r.electionTick)
			}

			campaignSent := func() bool {
				for _, m := range p.r.msgs {
					if m.Type == proto.RequestPreVote || m.Type == proto.RequestVote {
						return true
					}
				}
				return false
			}
			armed := p.r.randomizedElectionTimeout
			for i := range armed - 1 {
				p.Tick()
				if campaignSent() {
					t.Fatalf("campaign at tick %d after rejoin, before the armed timeout %d",
						i+1, armed)
				}
			}

			campaigned := false
			for range 3*p.r.electionTimeout - (armed - 1) {
				p.Tick()
				if campaignSent() {
					campaigned = true
					break
				}
			}
			if !campaigned {
				t.Fatalf("no campaign within %d ticks of the rejoin", 3*p.r.electionTimeout)
			}
			if p.r.state != preCandidate && p.r.state != candidate {
				t.Fatalf("state = %v after campaign, want pre-candidate or candidate", p.r.state)
			}
		})
	}
}
