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

	// ShardRecovering indicates the shard's state machine is being
	// replaced by a received snapshot (RecoverFromSnapshot). The replica
	// keeps participating in Raft — it votes and replicates — but applies
	// nothing and serves no up-to-date local read until recovery
	// completes, which takes as long as the user state machine needs.
	ShardRecovering

	// ShardSnapshotting indicates the shard is capturing a consistent
	// point-in-time for a snapshot save. Applies are quiesced for the
	// duration of the capture.
	ShardSnapshotting
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
//
// An unrecognized string is rejected with *InvalidShardHealthError and leaves
// the receiver untouched. It is NOT decoded to ShardHealthy: that is the zero
// value and the most reassuring state this type has, so defaulting to it would
// report a shard as healthy on the strength of a typo, a truncated payload, or
// a health state a newer binary knows and this one does not.
func (h *ShardHealth) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, ok := shardHealthValues[s]
	if !ok {
		return &InvalidShardHealthError{Value: s}
	}
	*h = v
	return nil
}

// shardHealthValues maps string representations to ShardHealth values.
var shardHealthValues = map[string]ShardHealth{
	"healthy":      ShardHealthy,
	"no-leader":    ShardNoLeader,
	"lagging":      ShardLagging,
	"unloaded":     ShardUnloaded,
	"error":        ShardError,
	"recovering":   ShardRecovering,
	"snapshotting": ShardSnapshotting,
}

// shardHealthNames maps ShardHealth values to their string representations.
var shardHealthNames = map[ShardHealth]string{
	ShardHealthy:      "healthy",
	ShardNoLeader:     "no-leader",
	ShardLagging:      "lagging",
	ShardUnloaded:     "unloaded",
	ShardError:        "error",
	ShardRecovering:   "recovering",
	ShardSnapshotting: "snapshotting",
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

	// Term is the term of the last leadership change this replica
	// published, not its current Raft term. It does not advance while no
	// leader is known, so during split-vote churn it stands still while the
	// replica's real term climbs.
	Term uint64 `json:"term"`

	// IsLeader is true if this replica is the current leader.
	IsLeader bool `json:"is_leader"`

	// IsObserver is true if this replica is currently a non-voting observer.
	// It follows the committed membership, not Config.IsObserver: an observer
	// whose promotion to voter has been applied on this host reports false
	// without a restart.
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

	// Paused is true while the replica is paused by Host.PauseShard and
	// not yet resumed by Host.ResumeShard. A pause is not persisted: a
	// restarted replica reports false.
	Paused bool `json:"paused"`

	// Replicas reports how far each remote member has replicated, as this
	// leader understands it: voters, observers (learners) and witnesses, each
	// tagged with its Role. Callers that want voters only, for quorum or
	// health arithmetic, must filter on ReplicaProgress.Role. It is empty on a follower, which tracks no such state,
	// and empty on the first call for a shard because publication is opt-in
	// and the next tick has not run yet.
	//
	// This is what answers "which replica is behind, and why". Before it
	// existed the same information was reachable only by parsing the
	// DiagRemotes debug string, so it was available to a human reading a log
	// line and to nothing else — not to an operator dashboard, not to an
	// alerting rule, not to a pre-upgrade safety check.
	Replicas []ReplicaProgress `json:"replicas,omitempty"`
}

// ReplicaRole is a remote member's role in a shard, as reported in
// ReplicaProgress.Role. The set is closed: ReplicaRoleVoter,
// ReplicaRoleObserver and ReplicaRoleWitness are the only values.
type ReplicaRole string

const (
	// ReplicaRoleVoter is a full voting member.
	ReplicaRoleVoter ReplicaRole = "voter"
	// ReplicaRoleObserver is a non-voting member (learner). It receives the
	// log but does not count toward any quorum.
	ReplicaRoleObserver ReplicaRole = "observer"
	// ReplicaRoleWitness is a voting member that carries no payload.
	ReplicaRoleWitness ReplicaRole = "witness"
)

// ReplicaProgress is one remote member's replication position from the
// leader's point of view. The member may be a voter, an observer or a witness;
// Role says which, and callers that only care about voters filter on it. Every field is the leader's local belief, which is precisely
// what makes it useful when diagnosing a stall: a follower stuck in probe with
// a stale match index is a different problem from one that is merely behind,
// and one pinned at the inflight limit is flow-controlled rather than slow.
type ReplicaProgress struct {
	// ReplicaID identifies the member.
	ReplicaID uint64 `json:"replica_id"`

	// Role is the member's role: "voter", "observer" or "witness" (the
	// ReplicaRole constants). Only voters and witnesses count toward quorum.
	Role ReplicaRole `json:"role"`

	// Match is the highest log index known to be replicated to it.
	Match uint64 `json:"match"`

	// Next is the next index the leader will send.
	Next uint64 `json:"next"`

	// State is the replication state: "probe", "replicate" or "snapshot".
	State string `json:"state"`

	// Paused is true when the leader is withholding traffic, because the
	// inflight window is full or a probe is awaiting its response.
	Paused bool `json:"paused"`

	// Active is true if the follower has responded within the election
	// timeout. An inactive follower still counts in the configuration but
	// not toward a live quorum — the distinction that decides whether it is
	// safe to take another replica down.
	Active bool `json:"active"`

	// Inflights is the number of unacknowledged messages in the window.
	Inflights int `json:"inflights"`

	// IsWitness reports a voting member that carries no payload.
	IsWitness bool `json:"is_witness"`

	// IsObserver reports a non-voting member.
	IsObserver bool `json:"is_observer"`
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

	// DiagnosticsDropped is the number of library log records dropped since
	// the process started because the handler installed through
	// slog.SetDefault was not taking them: the library emits its diagnostics
	// through one bounded queue, never blocking the goroutine that produced
	// them, and drops a record when that queue is full. A rising count means
	// the handler is blocking or far slower than the process logs. The count
	// is process-wide, as the default handler is, so every Host in a process
	// reports the same value. It does not affect Status.
	DiagnosticsDropped uint64 `json:"diagnostics_dropped"`
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
