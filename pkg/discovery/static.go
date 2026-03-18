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

package discovery

import "context"

// StaticConfig configures static peer discovery from a fixed peer list.
type StaticConfig struct {
	// Peers is the fixed list of peers to return on discovery.
	Peers []Peer

	// SharedSecret is the optional HMAC-SHA256 key for authenticating
	// cluster membership. When set, static peers must present a matching
	// secret to join. The secret is stored for downstream consumption
	// by the Raft transport layer.
	SharedSecret []byte
}

// StaticDiscovery returns a fixed set of peers provided at construction time.
// This is the simplest discovery method, suitable for deployments where all
// peer addresses are known in advance.
type StaticDiscovery struct {
	peers        []Peer
	sharedSecret []byte
}

// NewStaticDiscovery creates a StaticDiscovery that returns the configured peers.
// The peers and shared secret are deep-copied to prevent external mutation.
func NewStaticDiscovery(cfg StaticConfig) *StaticDiscovery {
	peers := make([]Peer, len(cfg.Peers))
	copy(peers, cfg.Peers)

	var secret []byte
	if len(cfg.SharedSecret) > 0 {
		secret = make([]byte, len(cfg.SharedSecret))
		copy(secret, cfg.SharedSecret)
	}

	return &StaticDiscovery{
		peers:        peers,
		sharedSecret: secret,
	}
}

// Discover returns the configured peer list. Each call returns a fresh copy
// to prevent callers from mutating the internal state. The context parameter
// is accepted for interface compliance but is not used since static discovery
// performs no I/O.
func (s *StaticDiscovery) Discover(_ context.Context) ([]Peer, error) {
	if len(s.peers) == 0 {
		return nil, &NoPeersError{Method: s.Name()}
	}
	result := make([]Peer, len(s.peers))
	copy(result, s.peers)
	return result, nil
}

// Name returns "static".
func (s *StaticDiscovery) Name() string {
	return "static"
}

// Stop is a no-op for static discovery.
func (s *StaticDiscovery) Stop() error {
	return nil
}
