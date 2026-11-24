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

import "testing"

// TestQuorumOf pins the majority rule itself.
//
// quorumOf is the SINGLE definition from which election tallying
// (checkElectionResult), commit advancement (tryCommit), the ReadIndex quorum
// and the membership validators all derive. Until this test existed it had no
// direct coverage at all: the only occurrence of the identifier in any test
// file was a COMMENT. That mattered — changing it to n/2 (majority to half)
// left 18 of the 19 linearizability tests passing, because producing two
// leaders from a halved quorum needs a split vote or a live partition and the
// integration suite constructs neither.
//
// A one-character change here silently halves the safety margin of the entire
// engine, so it is pinned as a table rather than left to emerge from a
// multi-second cluster test.
func TestQuorumOf(t *testing.T) {
	// Hand-computed majorities: strictly more than half of n.
	cases := []struct {
		n    int
		want int
	}{
		{1, 1},
		{2, 2}, // both members: 1 is not a majority of 2
		{3, 2},
		{4, 3}, // even sizes are where an off-by-one shows up
		{5, 3},
		{6, 4},
		{7, 4},
		{8, 5},
		{9, 5},
		{64, 33}, // the maximum voting set (ReadIndex ackBits is a uint64)
	}
	for _, c := range cases {
		if got := quorumOf(c.n); got != c.want {
			t.Errorf("quorumOf(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

// TestQuorumOf_IsStrictMajority states the property the table encodes, so a
// future edit cannot satisfy the table by coincidence: two disjoint quorums
// must be impossible over the same voting set. That is the whole reason the
// rule is n/2+1 and not n/2 — it is what makes two leaders in one term, and
// two independently-committing halves, unrepresentable.
func TestQuorumOf_IsStrictMajority(t *testing.T) {
	for n := 1; n <= 64; n++ {
		q := quorumOf(n)
		if 2*q <= n {
			t.Fatalf("quorumOf(%d) = %d: two disjoint sets of that size fit "+
				"in %d members, so two candidates could each claim a quorum "+
				"in the same term", n, q, n)
		}
		// It must also be the SMALLEST such value, or the cluster loses
		// availability it is entitled to (a 5-node shard must tolerate 2 down).
		if q > 1 && 2*(q-1) > n {
			t.Fatalf("quorumOf(%d) = %d is larger than the smallest strict "+
				"majority: the shard refuses to make progress with a "+
				"majority available", n, q)
		}
	}
}

// TestNumVotingMembers_CountsSelfAndWitnessesOnly pins the denominator that
// quorumOf is applied to. Observers are NOT voting members; if they ever enter
// this count, a shard sizes its quorum over a set larger than the one that can
// actually vote and can never elect a leader.
func TestNumVotingMembers_CountsSelfAndWitnessesOnly(t *testing.T) {
	remotes := map[uint64]*remote{2: {}, 3: {}}
	witnesses := map[uint64]*remote{4: {}}

	if got := numVotingMembers(remotes, witnesses); got != 4 {
		t.Fatalf("numVotingMembers(2 voters, 1 witness) = %d, want 4 "+
			"(2 remotes + 1 witness + self)", got)
	}
	if got := numVotingMembers(nil, nil); got != 1 {
		t.Fatalf("numVotingMembers(none, none) = %d, want 1 (self)", got)
	}
}
