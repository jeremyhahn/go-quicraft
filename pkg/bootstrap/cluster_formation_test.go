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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/discovery"
)

func TestQuorumSize(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 4},
		{7, 4},
		{9, 5},
		{10, 6},
	}

	for _, tt := range tests {
		got := QuorumSize(tt.n)
		if got != tt.want {
			t.Errorf("QuorumSize(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestHasQuorum_True(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
	}

	// 3 peers for minPeers=3 => quorum=2 => 3 >= 2 => true
	if !HasQuorum(peers, 3) {
		t.Error("HasQuorum should be true with 3 peers for minPeers=3")
	}
}

func TestHasQuorum_False(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
	}

	// 1 peer for minPeers=3 => quorum=2 => 1 < 2 => false
	if HasQuorum(peers, 3) {
		t.Error("HasQuorum should be false with 1 peer for minPeers=3")
	}
}

func TestHasQuorum_ExactQuorum(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
	}

	// 2 peers for minPeers=3 => quorum=2 => 2 >= 2 => true
	if !HasQuorum(peers, 3) {
		t.Error("HasQuorum should be true with exactly quorum peers")
	}
}

func TestHasQuorum_EmptyPeers(t *testing.T) {
	if HasQuorum(nil, 3) {
		t.Error("HasQuorum should be false with nil peers")
	}
	if HasQuorum([]discovery.Peer{}, 3) {
		t.Error("HasQuorum should be false with empty peers")
	}
}

func TestHasQuorum_SingleNodeCluster(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
	}
	// minPeers=1 => quorum=1 => 1 >= 1 => true
	if !HasQuorum(peers, 1) {
		t.Error("HasQuorum should be true for single-node cluster")
	}
}

func TestDeterministicLeader_MultipleNodes(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 5, Address: "10.0.0.5:5000"},
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
	}

	leader := DeterministicLeader(peers)
	if leader.NodeID != 1 {
		t.Errorf("DeterministicLeader = NodeID %d, want 1", leader.NodeID)
	}
	if leader.Address != "10.0.0.1:5000" {
		t.Errorf("DeterministicLeader.Address = %q, want %q", leader.Address, "10.0.0.1:5000")
	}
}

func TestDeterministicLeader_SinglePeer(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 42, Address: "10.0.0.42:5000"},
	}

	leader := DeterministicLeader(peers)
	if leader.NodeID != 42 {
		t.Errorf("DeterministicLeader = NodeID %d, want 42", leader.NodeID)
	}
}

func TestDeterministicLeader_EmptyPeers(t *testing.T) {
	leader := DeterministicLeader(nil)
	if leader.NodeID != 0 || leader.Address != "" {
		t.Errorf("DeterministicLeader(nil) = %+v, want zero Peer", leader)
	}
}

func TestDeterministicLeader_DoesNotMutateInput(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 5, Address: "10.0.0.5:5000"},
		{NodeID: 1, Address: "10.0.0.1:5000"},
	}

	DeterministicLeader(peers)

	// Original slice should not be sorted.
	if peers[0].NodeID != 5 {
		t.Error("DeterministicLeader should not mutate the input slice")
	}
}

func TestBuildMemberMap(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
	}

	members := BuildMemberMap(peers)

	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}

	for _, p := range peers {
		addr, ok := members[p.NodeID]
		if !ok {
			t.Errorf("missing member for NodeID %d", p.NodeID)
			continue
		}
		if addr != p.Address {
			t.Errorf("members[%d] = %q, want %q", p.NodeID, addr, p.Address)
		}
	}
}

func TestBuildMemberMap_Empty(t *testing.T) {
	members := BuildMemberMap(nil)
	if len(members) != 0 {
		t.Errorf("expected empty map, got %d entries", len(members))
	}
}

func TestBuildMemberMap_DuplicateNodeID(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 1, Address: "10.0.0.1:5001"},
	}

	members := BuildMemberMap(peers)
	if len(members) != 1 {
		t.Errorf("expected 1 member (deduped), got %d", len(members))
	}
	// Last write wins.
	if members[1] != "10.0.0.1:5001" {
		t.Errorf("members[1] = %q, want %q", members[1], "10.0.0.1:5001")
	}
}

// BenchmarkQuorumSize measures the quorum calculation for various cluster sizes.
func BenchmarkQuorumSize(b *testing.B) {
	b.ReportAllocs()
	sizes := []int{1, 3, 5, 7, 9}
	for b.Loop() {
		for _, n := range sizes {
			_ = QuorumSize(n)
		}
	}
}

// BenchmarkDeterministicLeader measures leader selection with 5 peers.
// This involves copying, sorting, and selecting the minimum NodeID.
func BenchmarkDeterministicLeader(b *testing.B) {
	b.ReportAllocs()
	peers := []discovery.Peer{
		{NodeID: 5, Address: "10.0.0.5:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
		{NodeID: 4, Address: "10.0.0.4:5000"},
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
	}
	for b.Loop() {
		_ = DeterministicLeader(peers)
	}
}

// BenchmarkBuildMemberMap measures map construction from 5 peers.
func BenchmarkBuildMemberMap(b *testing.B) {
	b.ReportAllocs()
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
		{NodeID: 4, Address: "10.0.0.4:5000"},
		{NodeID: 5, Address: "10.0.0.5:5000"},
	}
	for b.Loop() {
		_ = BuildMemberMap(peers)
	}
}

func TestDeterministicLeader_AlreadySorted(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
	}

	leader := DeterministicLeader(peers)
	if leader.NodeID != 1 {
		t.Errorf("DeterministicLeader = NodeID %d, want 1", leader.NodeID)
	}
}

func TestDeterministicLeader_ReverseSorted(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 100, Address: "10.0.0.100:5000"},
		{NodeID: 50, Address: "10.0.0.50:5000"},
		{NodeID: 10, Address: "10.0.0.10:5000"},
	}

	leader := DeterministicLeader(peers)
	if leader.NodeID != 10 {
		t.Errorf("DeterministicLeader = NodeID %d, want 10", leader.NodeID)
	}
}
