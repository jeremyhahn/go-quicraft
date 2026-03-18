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
	"net"

	"github.com/jeremyhahn/go-quicraft/pkg/discovery"
)

const defaultMinPeers = 3

// Bootstrapper orchestrates cluster formation: discovery, quorum check,
// member map construction, and shard startup.
type Bootstrapper struct {
	cfg  Config
	node RaftNode
}

// NewBootstrapper creates a Bootstrapper with the given configuration
// and RaftNode implementation.
func NewBootstrapper(cfg Config, node RaftNode) *Bootstrapper {
	if cfg.MinPeers == 0 {
		cfg.MinPeers = defaultMinPeers
	}
	return &Bootstrapper{cfg: cfg, node: node}
}

// Bootstrap performs cluster formation. It discovers peers, validates
// quorum, and starts the shard. The calling node includes itself in
// the member map. The provided context is propagated to the discovery
// method, allowing the caller to cancel discovery during shutdown.
func (b *Bootstrapper) Bootstrap(ctx context.Context) error {
	// 1. Discover peers.
	peers, err := b.cfg.Discovery.Discover(ctx)
	if err != nil {
		return &Error{Phase: "discovery", Err: err}
	}

	// 2. Validate peer addresses and check for duplicates.
	if err := validatePeerAddresses(peers); err != nil {
		return &Error{Phase: "validation", Err: err}
	}

	// 3. Add self to peer list (avoid duplicates).
	self := discovery.Peer{
		NodeID:  b.cfg.NodeID,
		Address: b.cfg.Address,
	}
	peers = addSelfToPeers(peers, self)

	// 4. Check quorum.
	needed := QuorumSize(b.cfg.MinPeers)
	if !HasQuorum(peers, b.cfg.MinPeers) {
		return &QuorumError{
			Have: len(peers),
			Need: needed,
		}
	}

	// 5. Build member map.
	members := BuildMemberMap(peers)

	// 6. Start shard with members.
	if err := b.node.StartShard(members, false, b.cfg.CreateFn, b.cfg.ShardConfig); err != nil {
		return &Error{Phase: "start-shard", Err: err}
	}

	return nil
}

// validatePeerAddresses validates that all peer addresses are valid
// host:port pairs and that no two different NodeIDs share the same address.
func validatePeerAddresses(peers []discovery.Peer) error {
	seen := make(map[string]uint64, len(peers))
	for _, p := range peers {
		host, port, err := net.SplitHostPort(p.Address)
		if err != nil {
			return &InvalidAddressError{
				Address: p.Address,
				Reason:  err.Error(),
			}
		}
		if host == "" {
			return &InvalidAddressError{
				Address: p.Address,
				Reason:  "empty host",
			}
		}
		if port == "" {
			return &InvalidAddressError{
				Address: p.Address,
				Reason:  "empty port",
			}
		}
		if existingID, exists := seen[p.Address]; exists && existingID != p.NodeID {
			return &DuplicateAddressError{
				Address: p.Address,
				NodeID1: existingID,
				NodeID2: p.NodeID,
			}
		}
		seen[p.Address] = p.NodeID
	}
	return nil
}

// addSelfToPeers adds self to peers if not already present.
func addSelfToPeers(peers []discovery.Peer, self discovery.Peer) []discovery.Peer {
	for _, p := range peers {
		if p.NodeID == self.NodeID {
			return peers
		}
	}
	return append(peers, self)
}
