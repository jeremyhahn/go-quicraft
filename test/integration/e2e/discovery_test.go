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

//go:build integration

package e2e_test

import (
	"context"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/bootstrap"
	"github.com/jeremyhahn/go-quicraft/pkg/discovery"
)

// TestE2E_StaticDiscovery_ThreeNodes verifies that StaticDiscovery
// correctly returns all 3 configured peers.
func TestE2E_StaticDiscovery_ThreeNodes(t *testing.T) {
	addrs := make([]string, 3)
	for i := range 3 {
		addrs[i] = freePort(t)
	}

	peers := []discovery.Peer{
		{NodeID: 1, Address: addrs[0]},
		{NodeID: 2, Address: addrs[1]},
		{NodeID: 3, Address: addrs[2]},
	}

	sd := discovery.NewStaticDiscovery(discovery.StaticConfig{
		Peers: peers,
	})

	discovered, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	if len(discovered) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(discovered))
	}

	// Verify each peer is present with correct address.
	peerMap := make(map[uint64]string, len(discovered))
	for _, p := range discovered {
		peerMap[p.NodeID] = p.Address
	}

	for _, expected := range peers {
		addr, ok := peerMap[expected.NodeID]
		if !ok {
			t.Errorf("peer NodeID=%d not found in discovered peers", expected.NodeID)
			continue
		}
		if addr != expected.Address {
			t.Errorf("peer NodeID=%d address = %q, want %q", expected.NodeID, addr, expected.Address)
		}
	}

	// Verify the discovery method name.
	if sd.Name() != "static" {
		t.Errorf("Name() = %q, want %q", sd.Name(), "static")
	}

	// Verify Stop is safe to call.
	if err := sd.Stop(); err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

// TestE2E_Bootstrap_ThreeNodeFormation uses the Bootstrapper with
// StaticDiscovery to form a 3-node cluster, then verifies that leader
// election succeeds and proposals are accepted.
func TestE2E_Bootstrap_ThreeNodeFormation(t *testing.T) {
	const numNodes = 3
	const shardID = 1

	// Allocate addresses for all nodes.
	addrs := make([]string, numNodes)
	for i := range numNodes {
		addrs[i] = freePort(t)
	}

	// Build the peer list for discovery. Each peer maps to its
	// replica ID (1-indexed).
	peers := make([]discovery.Peer, numNodes)
	for i := range numNodes {
		peers[i] = discovery.Peer{
			NodeID:  uint64(i + 1),
			Address: addrs[i],
		}
	}

	// Create hosts with shared mTLS config so peers can verify each other.
	mtls := testMTLSConfig(t)
	hosts := make([]*quicraft.Host, numNodes)
	for i := range numNodes {
		cfg := testHostConfig(t, addrs[i], mtls)
		h, err := quicraft.NewHost(cfg)
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] failed: %v", i, err)
		}
		hosts[i] = h
	}
	defer func() {
		for _, h := range hosts {
			h.Close()
		}
	}()

	// Bootstrap each node using the Bootstrapper with StaticDiscovery.
	trackerMap := make(map[int]*smTracker, numNodes)
	for i := range numNodes {
		replicaID := uint64(i + 1)

		// Each node gets its own discovery that lists all other peers.
		// The bootstrapper will add self to the peer list automatically.
		otherPeers := make([]discovery.Peer, 0, numNodes-1)
		for j := range numNodes {
			if j == i {
				continue
			}
			otherPeers = append(otherPeers, peers[j])
		}

		sd := discovery.NewStaticDiscovery(discovery.StaticConfig{
			Peers: otherPeers,
		})

		createFn, tracker := kvCreateFunc()
		trackerMap[i] = tracker

		shardCfg := testShardConfig(shardID, replicaID)

		bsCfg := bootstrap.Config{
			NodeID:      replicaID,
			Address:     addrs[i],
			ShardID:     shardID,
			ReplicaID:   replicaID,
			Discovery:   sd,
			CreateFn:    createFn,
			ShardConfig: shardCfg,
			MinPeers:    numNodes,
		}

		bs := bootstrap.NewBootstrapper(bsCfg, hosts[i])
		if err := bs.Bootstrap(context.Background()); err != nil {
			t.Fatalf("Bootstrap[%d] failed: %v", i, err)
		}
	}

	// Wait for leader election.
	var leaderIdx int
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	electionDeadline := time.NewTimer(leaderElectionTimeout)
	defer electionDeadline.Stop()
leaderSearch:
	for {
		for i, h := range hosts {
			leaderID, known, err := h.GetLeaderID(shardID)
			if err == nil && known && leaderID > 0 {
				info, infoErr := h.GetShardInfo(shardID)
				if infoErr == nil && info.IsLeader {
					leaderIdx = i
					break leaderSearch
				}
			}
		}
		select {
		case <-electionDeadline.C:
			t.Fatal("timed out waiting for leader election after bootstrap")
		case <-ticker.C:
		}
	}

	t.Logf("bootstrap complete, leader at node[%d]", leaderIdx)

	// Propose a value through the leader.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("bootstrap-key", "bootstrap-value")
	rs, err := hosts[leaderIdx].Propose(ctx, shardID, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	_, err = rs.Result()
	if err != nil {
		t.Fatalf("Result failed: %v", err)
	}
	rs.Release()

	// Verify replication on all nodes.
	for i := range numNodes {
		replicaID := uint64(i + 1)
		sm := trackerMap[i].Get(shardID, replicaID)
		if sm == nil {
			t.Fatalf("node[%d] has no state machine for shard %d", i, shardID)
		}
		waitForCondition(t, defaultTimeout,
			"bootstrap replication",
			func() bool {
				val, ok := sm.Get("bootstrap-key")
				return ok && val == "bootstrap-value"
			},
		)
	}
}
