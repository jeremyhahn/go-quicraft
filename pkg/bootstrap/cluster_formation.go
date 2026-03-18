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

package bootstrap

import (
	"sort"

	"github.com/jeremyhahn/go-quicraft/pkg/discovery"
)

// QuorumSize returns the minimum quorum for n nodes (majority).
func QuorumSize(n int) int {
	return n/2 + 1
}

// HasQuorum checks if the number of discovered peers meets or exceeds
// the quorum requirement for minPeers nodes.
func HasQuorum(peers []discovery.Peer, minPeers int) bool {
	return len(peers) >= QuorumSize(minPeers)
}

// DeterministicLeader returns the peer with the lowest NodeID.
// This ensures all nodes agree on the initial leader without communication.
// The input slice must not be empty.
func DeterministicLeader(peers []discovery.Peer) discovery.Peer {
	if len(peers) == 0 {
		return discovery.Peer{}
	}

	sorted := make([]discovery.Peer, len(peers))
	copy(sorted, peers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NodeID < sorted[j].NodeID
	})
	return sorted[0]
}

// BuildMemberMap creates the members map from discovered peers,
// mapping NodeID to Address.
func BuildMemberMap(peers []discovery.Peer) map[uint64]string {
	members := make(map[uint64]string, len(peers))
	for _, p := range peers {
		members[p.NodeID] = p.Address
	}
	return members
}
