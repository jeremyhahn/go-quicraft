package raft

import (
	mathrand "math/rand"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// electionReviewMembers is the five-voter shard of
// TestSoak_LeaderCloseDoesNotStallWrites.
var electionReviewMembers = map[uint64]string{
	1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4", 5: "addr5",
}

// newElectionReviewRaft builds a replica the way Peer does, through newRaft,
// so its jitter PRNG gets the crypto/rand seed from raft.go:653-657.
func newElectionReviewRaft(t *testing.T, replicaID uint64) *raft {
	t.Helper()
	r, err := newRaft(raftTestCfg(1, replicaID), newTestLogReaderWithMembership(electionReviewMembers))
	if err != nil {
		t.Fatalf("newRaft(%d): %v", replicaID, err)
	}
	return r
}

// TestElectionReview_TimeoutDrawHasElectionRTTSlots pins the timeout draw at
// raft.go:5122: randomizedElectionTimeout is a whole number of ticks in
// [electionTimeout, 2*electionTimeout), so there are exactly electionTimeout
// distinct values. With the default ElectionRTT of 10 and the harness's
// 500ms WAL-sync tick, two followers that draw the same value and were reset
// by the same heartbeat campaign within the same tick. For the four
// survivors of a closed leader in a five-node shard the lowest draw is tied
// about 19% of the time. This test passes on 88b940be: it records the
// behaviour, it does not judge it.
func TestElectionReview_TimeoutDrawHasElectionRTTSlots(t *testing.T) {
	const et = 10
	r := &raft{electionTimeout: et, rng: mathrand.New(mathrand.NewSource(1))}
	seen := map[int]bool{}
	for range 10000 {
		r.resetElectionTimeout()
		v := r.randomizedElectionTimeout
		if v < et || v >= 2*et {
			t.Fatalf("draw %d outside [%d, %d)", v, et, 2*et)
		}
		seen[v] = true
	}
	if len(seen) != et {
		t.Fatalf("draw has %d distinct values, want %d (one per tick)", len(seen), et)
	}

	// Rate at which the earliest of four followers' timers is tied.
	ties := 0
	const trials = 20000
	for range trials {
		minV, minN := 1<<30, 0
		for range 4 {
			r.resetElectionTimeout()
			switch v := r.randomizedElectionTimeout; {
			case v < minV:
				minV, minN = v, 1
			case v == minV:
				minN++
			}
		}
		if minN > 1 {
			ties++
		}
	}
	rate := float64(ties) / trials
	if rate < 0.15 || rate > 0.24 {
		t.Fatalf("tied-earliest-timer rate %.3f, want about 0.19", rate)
	}
}

// TestElectionReview_HostBuiltReplicasDrawIndependently checks the evidence
// from mfrepro1 (the earliest tick shared in 6 of 11 soak runs, against an
// expected 0.19) against the raft layer. Replicas built through newRaft, as
// the host builds them, must have distinct seeds and independent draws:
// equal draws for two replicas about 1/ElectionRTT of the time, and the
// earliest of four survivors tied about 19% of the time, both at
// construction and at the redraw a term change makes (becomeFollower,
// raft.go:1060). It passes on 88b940be: the draws are not correlated, so the
// excess ties in the soak must come from outside the draw (tick phase or
// timer resets; see the lane report).
func TestElectionReview_HostBuiltReplicasDrawIndependently(t *testing.T) {
	const clusters = 2000
	var pairEq, tieInit, tieRedraw, sameSeq int
	for range clusters {
		rs := make([]*raft, 0, 5)
		for id := uint64(1); id <= 5; id++ {
			rs = append(rs, newElectionReviewRaft(t, id))
		}
		if rs[1].randomizedElectionTimeout == rs[2].randomizedElectionTimeout {
			pairEq++
		}
		if earliestTied(rs[1:]) {
			tieInit++
		}
		// The new term the survivors adopt from the first RequestVote redraws
		// every timer (becomeFollower).
		for _, r := range rs[1:] {
			r.becomeFollower(r.term+1, noLeader)
		}
		if earliestTied(rs[1:]) {
			tieRedraw++
		}
		// Seeds: two replicas with the same seed produce the same sequence.
		a, b := rs[1], rs[2]
		same := true
		for range 16 {
			if a.rng.Int63() != b.rng.Int63() {
				same = false
			}
		}
		if same {
			sameSeq++
		}
	}
	pairRate := float64(pairEq) / clusters
	initRate := float64(tieInit) / clusters
	redrawRate := float64(tieRedraw) / clusters
	t.Logf("replica-pair equal-draw rate %.3f (expect 0.100); earliest-of-4 tie rate %.3f at "+
		"construction, %.3f after a term change (expect 0.190); identical PRNG sequences %d of %d",
		pairRate, initRate, redrawRate, sameSeq, clusters)
	if sameSeq != 0 {
		t.Fatalf("%d of %d replica pairs share a jitter PRNG sequence", sameSeq, clusters)
	}
	if pairRate < 0.06 || pairRate > 0.14 {
		t.Fatalf("replica-pair equal-draw rate %.3f, want about 0.10", pairRate)
	}
	for _, rate := range []float64{initRate, redrawRate} {
		if rate < 0.14 || rate > 0.25 {
			t.Fatalf("earliest-of-4 tie rate %.3f, want about 0.19", rate)
		}
	}
}

// earliestTied reports whether the lowest randomizedElectionTimeout among rs
// is shared by two or more replicas.
func earliestTied(rs []*raft) bool {
	minV, minN := 1<<30, 0
	for _, r := range rs {
		switch v := r.randomizedElectionTimeout; {
		case v < minV:
			minV, minN = v, 1
		case v == minV:
			minN++
		}
	}
	return minN > 1
}

// TestElectionReview_HeartbeatsKeepTheDraw pins finding 4: a heartbeat from
// the known leader zeroes electionTick but does not redraw
// randomizedElectionTimeout (only becomeFollower/becomeCandidate/
// becomePreCandidate call resetElectionTimeout, raft.go:1060, 1095, 1117).
// Two followers whose draws tie at the start of a term stay tied for the
// whole term, so every loss of that leader starts their elections on the
// same tick. Passes on 88b940be: it records the behaviour.
func TestElectionReview_HeartbeatsKeepTheDraw(t *testing.T) {
	r := newElectionReviewRaft(t, 2)
	r.becomeFollower(1, 1)
	draw := r.randomizedElectionTimeout
	for i := range 200 {
		for range draw - 1 {
			r.tickFunc(r)
		}
		if err := r.step(proto.Message{Type: proto.Heartbeat, From: 1, Term: 1}); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
		if r.state != follower || r.leaderID != 1 {
			t.Fatalf("heartbeat %d: state %v leader %d, want follower of 1", i, r.state, r.leaderID)
		}
		if r.electionTick != 0 {
			t.Fatalf("heartbeat %d: electionTick %d, want 0", i, r.electionTick)
		}
		if r.randomizedElectionTimeout != draw {
			t.Fatalf("heartbeat %d redrew the timeout: %d, want %d", i, r.randomizedElectionTimeout, draw)
		}
	}
}

// TestElectionReview_ConcurrentPreCandidatesBothPass pins finding 5: when two
// survivors' timers expire on the same tick, each grants the other's PreVote
// (handleVoteRequest, raft.go:2127: becomePreCandidate, raft.go:1107, has
// cleared leaderID and msg.Term > r.term), and the followers whose timers
// have passed electionTimeout grant both. Both pre-candidates reach a
// pre-vote quorum and both become candidates at the same term, so the real
// vote can split 2-2. Passes on 88b940be: standard Raft, as in etcd.
func TestElectionReview_ConcurrentPreCandidatesBothPass(t *testing.T) {
	rs := map[uint64]*raft{}
	for id := uint64(2); id <= 5; id++ {
		r := newElectionReviewRaft(t, id)
		r.becomeFollower(1, 1)
		// Leader 1 is gone: every survivor has gone a full election timeout
		// without a heartbeat.
		r.electionTick = r.electionTimeout
		rs[id] = r
	}
	a, b := rs[2], rs[3]
	a.becomePreCandidate()
	a.broadcastPreVote()
	b.becomePreCandidate()
	b.broadcastPreVote()

	// The requests cross in flight: every voter answers before either
	// pre-candidate counts a grant, as on the shared tick in the soak.
	// ask steps src's RequestPreVote into each listed voter and returns the
	// voter's answer.
	ask := func(src *raft, voters ...uint64) []proto.Message {
		t.Helper()
		var out []proto.Message
		for _, m := range collectMessages(src, proto.RequestPreVote) {
			v, ok := rs[m.To]
			if !ok || !containsID(voters, m.To) {
				continue
			}
			v.msgs = nil
			if err := v.step(m); err != nil {
				t.Fatalf("replica %d stepping PreVote from %d: %v", m.To, m.From, err)
			}
			for _, resp := range collectMessages(v, proto.RequestPreVoteResp) {
				if resp.Reject {
					t.Fatalf("replica %d (state %v) rejected the PreVote of %d", m.To, v.state, m.From)
				}
				out = append(out, resp)
			}
		}
		return out
	}
	// a hears from its rival pre-candidate b and from follower 4; b from a
	// and follower 5. Each has self plus two grants: a quorum of 3 of 5.
	aMsgs, bMsgs := a.msgs, b.msgs
	toA := ask(&raft{msgs: aMsgs}, 3, 4)
	toB := ask(&raft{msgs: bMsgs}, 2, 5)
	for src, resps := range map[*raft][]proto.Message{a: toA, b: toB} {
		if len(resps) != 2 {
			t.Fatalf("replica %d got %d PreVote grants, want 2", src.replicaID, len(resps))
		}
		for _, resp := range resps {
			if err := src.step(resp); err != nil {
				t.Fatalf("replica %d stepping PreVoteResp from %d: %v", src.replicaID, resp.From, err)
			}
		}
	}

	for _, r := range []*raft{a, b} {
		if r.state != candidate {
			t.Fatalf("replica %d: state %v, want candidate", r.replicaID, r.state)
		}
		if r.term != 2 || r.vote != r.replicaID {
			t.Fatalf("replica %d: term %d vote %d, want term 2 self-vote", r.replicaID, r.term, r.vote)
		}
	}
}

func containsID(ids []uint64, id uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
