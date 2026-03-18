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

// Package discovery provides peer discovery methods for QuicRaft cluster
// formation. Multiple discovery strategies are supported: static configuration,
// UDP multicast with HMAC authentication, and DNS SRV record lookup.
// The Manager orchestrates multiple methods with retry logic.
package discovery

import (
	"context"
	"time"
)

// Peer represents a discovered Raft peer.
type Peer struct {
	// NodeID is the unique identifier for this peer.
	NodeID uint64

	// Address is the network address of this peer (host:port).
	Address string
}

// Config holds common discovery configuration.
type Config struct {
	// DeploymentID isolates discovery to a specific cluster deployment.
	DeploymentID uint64

	// SharedSecret is the optional HMAC-SHA256 key for authenticating
	// cluster membership. When set, all discovery methods can use it
	// to verify that peers belong to the same cluster.
	SharedSecret []byte
}

// Method is the interface that all discovery implementations must satisfy.
type Method interface {
	// Discover returns a list of discovered peers. The provided context
	// allows the caller to cancel or set a deadline on the discovery
	// operation. Implementations that perform I/O (DNS lookups, HTTP
	// requests, multicast listening) should respect context cancellation.
	// Simple implementations (e.g., static) may accept and ignore the
	// context.
	Discover(ctx context.Context) ([]Peer, error)

	// Name returns the human-readable name of this discovery method.
	Name() string

	// Stop releases resources held by the discovery method.
	Stop() error
}

// ManagerConfig configures the discovery manager.
type ManagerConfig struct {
	// Methods is the ordered list of discovery methods to try.
	// The manager tries them in order and returns the first successful result.
	Methods []Method

	// RetryInterval is the time between discovery retries.
	RetryInterval time.Duration

	// MaxRetries is the maximum number of retry attempts. 0 means no retry.
	MaxRetries int
}
