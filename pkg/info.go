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

package quicraft

import (
	"encoding/json"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// ShardHealth describes the operational health of a shard on this host.
type ShardHealth uint8

const (
	// ShardHealthy indicates the shard is operating normally with a
	// known leader and an active apply pipeline.
	ShardHealthy ShardHealth = iota

	// ShardNoLeader indicates no leader is known for this shard,
	// typically during election or network partition.
	ShardNoLeader

	// ShardLagging indicates the shard's apply index is significantly
	// behind the commit index.
	ShardLagging

	// ShardUnloaded indicates the shard is not loaded on this host.
	ShardUnloaded

	// ShardError indicates the shard experienced a fatal error such as
	// a state machine failure or LogDB failure.
	ShardError
)

// String returns a human-readable representation of the ShardHealth value.
func (h ShardHealth) String() string {
	return shardHealthNames[h]
}

// MarshalJSON encodes ShardHealth as its string representation.
func (h ShardHealth) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.String())
}

// UnmarshalJSON decodes a JSON string back into a ShardHealth value.
func (h *ShardHealth) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, ok := shardHealthValues[s]
	if !ok {
		*h = ShardHealthy
		return nil
	}
	*h = v
	return nil
}

// shardHealthValues maps string representations to ShardHealth values.
var shardHealthValues = map[string]ShardHealth{
	"healthy":   ShardHealthy,
	"no-leader": ShardNoLeader,
	"lagging":   ShardLagging,
	"unloaded":  ShardUnloaded,
	"error":     ShardError,
}

// shardHealthNames maps ShardHealth values to their string representations.
var shardHealthNames = map[ShardHealth]string{
	ShardHealthy:  "healthy",
	ShardNoLeader: "no-leader",
	ShardLagging:  "lagging",
	ShardUnloaded: "unloaded",
	ShardError:    "error",
}

// ShardInfo provides a point-in-time snapshot of a shard's status.
// The returned ShardInfo is a copy and is NOT live-updated. Fields like
// LeaderID and Pending reflect the state at call time and may be stale
// by the time the caller inspects them.
type ShardInfo struct {
	// ShardID is the unique identifier for this shard.
	ShardID uint64 `json:"shard_id"`

	// ReplicaID is the local replica identifier for this shard.
	ReplicaID uint64 `json:"replica_id"`

	// LeaderID is the current leader's replicaID. 0 if no leader is known.
	LeaderID uint64 `json:"leader_id"`

	// Term is the current Raft term.
	Term uint64 `json:"term"`

	// IsLeader is true if this replica is the current leader.
	IsLeader bool `json:"is_leader"`

	// IsObserver is true if this replica is a non-voting observer.
	IsObserver bool `json:"is_observer"`

	// IsWitness is true if this replica is a voting witness without a
	// state machine.
	IsWitness bool `json:"is_witness"`

	// StateMachineType identifies the state machine variant:
	// 0 = regular, 1 = concurrent, 2 = on-disk.
	StateMachineType uint64 `json:"state_machine_type"`

	// StateMachineTypeName is the human-readable name for StateMachineType.
	StateMachineTypeName string `json:"state_machine_type_name"`

	// Pending is the number of proposals pending in the shard's queue.
	Pending uint64 `json:"pending"`

	// Nodes maps replicaID to network address for all known members.
	Nodes map[uint64]string `json:"nodes"`

	// Health is the current operational health of the shard.
	Health ShardHealth `json:"health"`

	// LastApplied is the index of the last entry applied to the state machine.
	LastApplied uint64 `json:"last_applied"`

	// ConfigChangeIndex is the log index of the most recently applied
	// membership configuration change entry. 0 if no config change has
	// been applied yet.
	ConfigChangeIndex uint64 `json:"config_change_index"`
}

// HealthStatus describes the overall health of a Host instance. The Status
// field is one of "ok", "degraded", "sealed", or "unhealthy".
type HealthStatus struct {
	// Status is the aggregate health verdict: "ok", "degraded", "sealed",
	// or "unhealthy".
	Status string `json:"status"`

	// BarrierSealed is true when the barrier is configured and sealed.
	BarrierSealed bool `json:"barrier_sealed"`

	// DiskOK is true when no disk full condition is active.
	DiskOK bool `json:"disk_ok"`

	// ShardCount is the number of shards loaded on this host.
	ShardCount int `json:"shard_count"`

	// Shards contains per-shard health information. Omitted when empty.
	Shards []ShardInfo `json:"shards,omitempty"`
}

// NodeHostInfoOption controls what information GetNodeHostInfo returns.
type NodeHostInfoOption struct {
	// SkipLogInfo when true omits per-shard log entry information,
	// making the call faster for health checks that only need shard status.
	SkipLogInfo bool
}

// NodeHostInfo provides a snapshot of the host's state for monitoring
// and debugging. It contains the host's network address, all loaded
// shards' status, and optionally per-shard log entry information.
type NodeHostInfo struct {
	// RaftAddress is this host's Raft network address.
	RaftAddress string `json:"raft_address"`

	// ShardInfoList contains status information for each loaded shard.
	ShardInfoList []ShardInfo `json:"shard_info_list"`

	// LogInfo contains per-shard log entry information. This field is
	// nil when NodeHostInfoOption.SkipLogInfo is true.
	LogInfo []logdb.NodeInfo `json:"log_info,omitempty"`
}
