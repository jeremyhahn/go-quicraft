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
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/discovery"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// mockDiscovery implements discovery.Method for testing.
type mockDiscovery struct {
	peers []discovery.Peer
	err   error
}

func (m *mockDiscovery) Discover(_ context.Context) ([]discovery.Peer, error) {
	return m.peers, m.err
}

func (m *mockDiscovery) Name() string { return "mock" }
func (m *mockDiscovery) Stop() error  { return nil }

// mockRaftNode implements RaftNode for testing.
type mockRaftNode struct {
	startErr    error
	members     map[uint64]string
	join        bool
	createFn    sm.CreateFunc
	cfg         config.Config
	startCalled bool

	leaderID  uint64
	hasLeader bool
	leaderErr error
}

func (m *mockRaftNode) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error {
	m.startCalled = true
	m.members = members
	m.join = join
	m.createFn = create
	m.cfg = cfg
	return m.startErr
}

func (m *mockRaftNode) GetLeaderID(shardID uint64) (uint64, bool, error) {
	return m.leaderID, m.hasLeader, m.leaderErr
}

func TestBootstrapper_SuccessfulBootstrap(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		ShardID:   100,
		ReplicaID: 1,
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !node.startCalled {
		t.Fatal("StartShard was not called")
	}

	// Verify self was added.
	if _, ok := node.members[1]; !ok {
		t.Error("self (NodeID=1) should be in members map")
	}

	// Verify all peers present.
	if len(node.members) != 3 {
		t.Errorf("expected 3 members, got %d", len(node.members))
	}

	for _, p := range disc.peers {
		addr, ok := node.members[p.NodeID]
		if !ok {
			t.Errorf("missing member NodeID %d", p.NodeID)
		}
		if addr != p.Address {
			t.Errorf("members[%d] = %q, want %q", p.NodeID, addr, p.Address)
		}
	}
}

func TestBootstrapper_DiscoveryError(t *testing.T) {
	discErr := errors.New("network unreachable")
	disc := &mockDiscovery{err: discErr}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var be *Error
	if !errors.As(err, &be) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
	if be.Phase != "discovery" {
		t.Errorf("Phase = %q, want %q", be.Phase, "discovery")
	}

	if node.startCalled {
		t.Error("StartShard should not be called when discovery fails")
	}
}

func TestBootstrapper_InsufficientPeers(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{}, // No peers discovered.
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected QuorumError, got nil")
	}

	var qe *QuorumError
	if !errors.As(err, &qe) {
		t.Errorf("expected QuorumError, got %T: %v", err, err)
	}

	// Self is added, so have=1, need=2 (quorum of 3).
	if qe.Have != 1 {
		t.Errorf("Have = %d, want 1", qe.Have)
	}
	if qe.Need != 2 {
		t.Errorf("Need = %d, want 2", qe.Need)
	}

	if node.startCalled {
		t.Error("StartShard should not be called with insufficient peers")
	}
}

func TestBootstrapper_StartShardError(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	startErr := errors.New("shard already exists")
	node := &mockRaftNode{startErr: startErr}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var be *Error
	if !errors.As(err, &be) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
	if be.Phase != "start-shard" {
		t.Errorf("Phase = %q, want %q", be.Phase, "start-shard")
	}
	if !errors.Is(err, startErr) {
		t.Error("error should wrap the StartShard error")
	}
}

func TestBootstrapper_SelfNotDuplicated(t *testing.T) {
	// Discovery already includes self.
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 1, Address: "10.0.0.1:5000"},
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(node.members) != 3 {
		t.Errorf("expected 3 members (self not duplicated), got %d", len(node.members))
	}
}

func TestBootstrapper_DefaultMinPeers(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	node := &mockRaftNode{}

	// MinPeers=0 should default to 3.
	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
	}, node)

	err := b.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 3 peers total (self + 2 discovered) >= quorum(3)=2, should succeed.
	if !node.startCalled {
		t.Error("StartShard should be called with default MinPeers")
	}
}

func TestBootstrapper_JoinIsFalse(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	b.Bootstrap(context.Background())

	if node.join {
		t.Error("join should be false for bootstrap (not joining existing cluster)")
	}
}

func TestBootstrapper_PassesCreateFnAndConfig(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
		},
	}
	node := &mockRaftNode{}
	createFn := sm.CreateFunc(func(shardID, replicaID uint64) interface{} { return nil })
	shardCfg := config.Config{ElectionRTT: 20}

	b := NewBootstrapper(Config{
		NodeID:      1,
		Address:     "10.0.0.1:5000",
		Discovery:   disc,
		CreateFn:    createFn,
		ShardConfig: shardCfg,
		MinPeers:    2,
	}, node)

	err := b.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if node.cfg.ElectionRTT != 20 {
		t.Errorf("ShardConfig.ElectionRTT = %d, want 20", node.cfg.ElectionRTT)
	}

	if node.createFn == nil {
		t.Error("CreateFn should be passed through")
	}
}

func TestBootstrapper_SingleNodeCluster(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  1,
	}, node)

	err := b.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !node.startCalled {
		t.Error("single-node cluster should succeed")
	}
	if len(node.members) != 1 {
		t.Errorf("expected 1 member, got %d", len(node.members))
	}
}

func TestAddSelfToPeers_NotPresent(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 2, Address: "10.0.0.2:5000"},
	}
	self := discovery.Peer{NodeID: 1, Address: "10.0.0.1:5000"}

	result := addSelfToPeers(peers, self)
	if len(result) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(result))
	}
}

func TestAddSelfToPeers_AlreadyPresent(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
	}
	self := discovery.Peer{NodeID: 1, Address: "10.0.0.1:5000"}

	result := addSelfToPeers(peers, self)
	if len(result) != 2 {
		t.Fatalf("expected 2 peers (no duplicate), got %d", len(result))
	}
}

func TestBootstrapper_InvalidAddressRejected(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "not-a-valid-address"},
		},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  2,
	}, node)

	err := b.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid address, got nil")
	}
	var be *Error
	if !errors.As(err, &be) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if be.Phase != "validation" {
		t.Errorf("Phase = %q, want %q", be.Phase, "validation")
	}
	var iae *InvalidAddressError
	if !errors.As(err, &iae) {
		t.Fatalf("expected *InvalidAddressError in chain, got %T: %v", err, err)
	}
}

func TestBootstrapper_DuplicateAddressRejected(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.2:5000"}, // same address, different NodeID
		},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected error for duplicate address, got nil")
	}
	var dae *DuplicateAddressError
	if !errors.As(err, &dae) {
		t.Fatalf("expected *DuplicateAddressError, got %T: %v", err, err)
	}
	if dae.Address != "10.0.0.2:5000" {
		t.Errorf("DuplicateAddressError.Address = %q, want %q", dae.Address, "10.0.0.2:5000")
	}
}

func TestBootstrapper_ValidAddressesPass(t *testing.T) {
	disc := &mockDiscovery{
		peers: []discovery.Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
		},
	}
	node := &mockRaftNode{}

	b := NewBootstrapper(Config{
		NodeID:    1,
		Address:   "10.0.0.1:5000",
		Discovery: disc,
		MinPeers:  3,
	}, node)

	err := b.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("expected no error for valid addresses, got: %v", err)
	}
}

func TestValidatePeerAddresses_EmptyHost(t *testing.T) {
	peers := []discovery.Peer{
		{NodeID: 1, Address: ":5000"},
	}
	err := validatePeerAddresses(peers)
	if err == nil {
		t.Fatal("expected error for empty host, got nil")
	}
	var iae *InvalidAddressError
	if !errors.As(err, &iae) {
		t.Fatalf("expected *InvalidAddressError, got %T: %v", err, err)
	}
}

func TestInvalidAddressError_Format(t *testing.T) {
	e := &InvalidAddressError{Address: "bad", Reason: "missing port"}
	want := "bootstrap: invalid peer address bad: missing port"
	if e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
}

func TestDuplicateAddressError_Format(t *testing.T) {
	e := &DuplicateAddressError{Address: "10.0.0.1:5000", NodeID1: 1, NodeID2: 2}
	want := "bootstrap: duplicate address 10.0.0.1:5000 for NodeIDs 1 and 2"
	if e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
}

func TestAddSelfToPeers_EmptyPeers(t *testing.T) {
	self := discovery.Peer{NodeID: 1, Address: "10.0.0.1:5000"}

	result := addSelfToPeers(nil, self)
	if len(result) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(result))
	}
	if result[0].NodeID != 1 {
		t.Errorf("NodeID = %d, want 1", result[0].NodeID)
	}
}
