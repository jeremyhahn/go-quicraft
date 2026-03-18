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

// Package bootstrap orchestrates Raft cluster formation. It combines peer
// discovery with quorum validation and shard initialization to bring a
// new cluster from zero to a running Raft group.
package bootstrap

import (
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/discovery"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// RaftNode is the interface that a Host must satisfy for bootstrap operations.
type RaftNode interface {
	// StartShard starts a Raft shard with the given members.
	StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error

	// GetLeaderID returns the leader ID for the given shard.
	GetLeaderID(shardID uint64) (uint64, bool, error)
}

// Config holds bootstrap configuration.
type Config struct {
	// NodeID is this node's unique identifier.
	NodeID uint64

	// Address is this node's Raft address.
	Address string

	// ShardID is the shard to bootstrap.
	ShardID uint64

	// ReplicaID is this node's replica ID.
	ReplicaID uint64

	// Discovery is the discovery method to use.
	Discovery discovery.Method

	// CreateFn is the state machine factory.
	CreateFn sm.CreateFunc

	// ShardConfig is the shard-level Raft configuration.
	ShardConfig config.Config

	// MinPeers is the minimum number of peers required for quorum.
	// Default: 3.
	MinPeers int
}
