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

	// ShardRecovering means the state machine is being replaced by a received snapshot; the replica votes and replicates but applies nothing.
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

// UnmarshalJSON decodes a ShardHealth string, rejecting an unknown one with *InvalidShardHealthError.
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

// ShardInfo is a point-in-time copy of a shard's status.
type ShardInfo struct {
	// ShardID is the unique identifier for this shard.
	ShardID uint64 `json:"shard_id"`

	// ReplicaID is the local replica identifier for this shard.
	ReplicaID uint64 `json:"replica_id"`

	// LeaderID is the current leader's replicaID. 0 if no leader is known.
	LeaderID uint64 `json:"leader_id"`

	// Term is the term of the last published leadership change, not the current Raft term.
	Term uint64 `json:"term"`

	// IsLeader is true if this replica is the current leader.
	IsLeader bool `json:"is_leader"`

	// IsObserver is true if this replica is a non-voting observer in the committed membership.
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

	// Paused is true between Host.PauseShard and Host.ResumeShard; a paused replica reports no leader.
	Paused bool `json:"paused"`

	// Replicas reports each remote member's replication as this leader sees it, tagged with its Role; nil on a follower.
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

// ReplicaProgress is one remote member's replication position as the leader believes it; Role says whether it is a voter, observer or witness.
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

	// Active is true if the follower responded within the election timeout; an inactive follower does not count toward a live quorum.
	Active bool `json:"active"`

	// Inflights is the number of unacknowledged messages in the window.
	Inflights int `json:"inflights"`

	// IsWitness reports a voting member that carries no payload.
	IsWitness bool `json:"is_witness"`

	// IsObserver reports a non-voting member.
	IsObserver bool `json:"is_observer"`
}

// PipelineDiag is a shard's engine pipeline state, returned by
// Host.DiagPipelineState for debugging stalls. Every field is a point-in-time
// read; the fields are not captured atomically together.
type PipelineDiag struct {
	// CommitPending is true while a batch is handed to the commit worker and
	// not yet persisted.
	CommitPending bool `json:"commit_pending"`

	// ProposalQueueLen is the number of proposals queued for the step worker.
	ProposalQueueLen int `json:"proposal_queue_len"`

	// InboxLen is the number of inbound raft messages queued for the step
	// worker.
	InboxLen int `json:"inbox_len"`

	// PendingProposals is the number of proposals the host is still waiting
	// to resolve.
	PendingProposals int `json:"pending_proposals"`
}

// ShardGatesDiag reports the snapshot gates that suspend a shard's state
// machine work, returned by Host.DiagShardGates.
type ShardGatesDiag struct {
	// Recovering is true while the state machine is being replaced by a
	// received snapshot: it applies nothing and extracts no committed
	// entries, though it keeps voting and replicating.
	Recovering bool `json:"recovering"`

	// Saving is true while a snapshot capture has quiesced applies.
	Saving bool `json:"saving"`

	// ApplyInFlight is the drain counter both gates wait on before touching
	// the state machine. A non-zero value with a gate up means the drain has
	// not completed yet.
	ApplyInFlight int64 `json:"apply_in_flight"`
}

// RaftStateDiag is a shard's raft state, returned by Host.DiagRaftState. It is
// read from the snapshot the step worker last published, so it lags the live
// state by up to one tick.
type RaftStateDiag struct {
	// Term is the replica's current term.
	Term uint64 `json:"term"`

	// Committed is the highest log index known to be committed.
	Committed uint64 `json:"committed"`

	// Processed is the highest log index handed to the apply pipeline.
	Processed uint64 `json:"processed"`

	// LastIndex is the index of the last entry in the replica's log.
	LastIndex uint64 `json:"last_index"`

	// PendingMessages is the number of outbound raft messages not yet sent.
	PendingMessages int `json:"pending_messages"`

	// State is the replica's role with its timers, such as "leader(hb=1 et=4)", with " self-removed" when absent from the configuration.
	State string `json:"state"`
}

// RemotesDiag is the leader's view of its remotes, returned by
// Host.DiagRemotes for debugging commit stalls.
type RemotesDiag struct {
	// Text renders each remote as "r<id>[role= match= next= state= paused= active= inflights=]" in replica-ID order, or "not leader".
	Text string `json:"text"`
}

// HealthStatus describes the overall health of a Host instance. The Status
// field is one of "ok", "degraded", "sealed", or "unhealthy".
type HealthStatus struct {
	// Status is the aggregate verdict: "ok", "degraded", "not-ready", "sealed" or "unhealthy"; "not-ready" means a started shard has been leaderless past its election window.
	Status string `json:"status"`

	// NotReadyShards lists, ascending, the shards leaderless past their election window, whatever Status is; omitted when empty.
	NotReadyShards []uint64 `json:"not_ready_shards,omitempty"`

	// BarrierSealed is true when the barrier is configured and sealed.
	BarrierSealed bool `json:"barrier_sealed"`

	// DiskOK is true when no disk full condition is active.
	DiskOK bool `json:"disk_ok"`

	// ShardCount is the number of shards loaded on this host.
	ShardCount int `json:"shard_count"`

	// Shards contains per-shard health information. Omitted when empty.
	Shards []ShardInfo `json:"shards,omitempty"`

	// DiagnosticsDropped is the number of library log records dropped since process start because the slog handler was not taking them.
	DiagnosticsDropped uint64 `json:"diagnostics_dropped"`
}

// NodeHostInfoOption controls what information NodeHostInfo returns.
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
