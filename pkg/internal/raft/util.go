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

// electionQuorum returns the number of votes needed to win an election.
// Both remotes and witnesses vote in elections.
func electionQuorum(remotes map[uint64]*remote, witnesses map[uint64]*remote) int {
	return numVotingMembers(remotes, witnesses)/2 + 1
}

// commitQuorum returns the number of data-bearing nodes needed to commit
// an entry. Only remotes and the leader count; witnesses do not store
// log entries and are excluded.
func commitQuorum(remotes map[uint64]*remote) int {
	replicatingMembers := len(remotes) + 1 // remotes + leader
	return replicatingMembers/2 + 1
}
