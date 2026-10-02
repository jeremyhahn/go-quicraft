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

// voteResult represents the outcome of tallying votes.
type voteResult uint8

const (
	// votePending means not enough votes to determine the outcome.
	votePending voteResult = iota
	// voteWon means the candidate has received a quorum of votes.
	voteWon
	// voteLost means the candidate has received a quorum of rejections.
	voteLost
)

// numVotingMembers returns the total number of voting members in the
// cluster, including remotes, witnesses, and the local node (self).
func numVotingMembers(remotes map[uint64]*remote, witnesses map[uint64]*remote) int {
	return len(remotes) + len(witnesses) + 1 // +1 for self
}

// quorumOf returns the majority threshold over a voting set of n members.
// This is the single definition of the majority rule; quorum() and the
// membership validators (validateAddWitness, validateRemoveNode) all derive
// from it so the admission checks and the runtime quorum can never diverge.
func quorumOf(n int) int {
	return n/2 + 1
}

// quorum returns the number of voting members needed for a quorum over the
// full voting set (remotes + witnesses + self). This is the SINGLE quorum
// used for BOTH elections and commit advancement.
//
// Using one unified quorum is a Raft safety requirement: witnesses vote in
// elections, so they MUST also be counted toward the commit quorum, or the
// two quorums are taken over different member sets and need not intersect
// in any node holding a committed entry — violating Leader Completeness.
// Witness metadata (Index/Term, no Cmd) is sufficient to gate commit and
// to enforce the up-to-date election check. The resulting durability
// trade-off (a commit may rest on {leader + witnesses} with its payload on
// one node, unrecoverable if that node suffers permanent storage loss) and
// the WitnessConfig membership-shape bounds are documented canonically in
// docs/raft-consensus/engineering.md Section 6. Matches the dragonboat reference.
func quorum(remotes map[uint64]*remote, witnesses map[uint64]*remote) int {
	return quorumOf(numVotingMembers(remotes, witnesses))
}
