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

// Package config provides dragonboat-compatible configuration types with
// conversion methods to quicraft equivalents. This enables existing
// dragonboat users to migrate to quicraft by changing imports without
// rewriting their configuration code.
package config

import qcConfig "github.com/jeremyhahn/go-quicraft/pkg/config"

// CompressionType is the type of compression used for Raft entries and
// snapshot data.
type CompressionType = qcConfig.CompressionType

// Compression type constants matching dragonboat's API.
const (
	NoCompression = qcConfig.NoCompression
	Snappy        = qcConfig.Snappy
)

// Config is the per-shard configuration matching dragonboat's config.Config.
// It is converted to quicraft's config.Config via ToQuicraft().
type Config struct {
	// ReplicaID is the unique identifier for this replica within the shard.
	ReplicaID uint64

	// ShardID is the globally unique identifier for the Raft shard.
	ShardID uint64

	// CheckQuorum enables the leader to step down when it cannot reach
	// a quorum within an election timeout.
	CheckQuorum bool

	// PreVote enables the pre-vote protocol extension to prevent
	// disruptive elections from partitioned nodes.
	PreVote bool

	// ElectionRTT is the election timeout measured in multiples of
	// RTTMillisecond. Must be greater than HeartbeatRTT.
	ElectionRTT uint64

	// HeartbeatRTT is the heartbeat interval measured in multiples of
	// RTTMillisecond.
	HeartbeatRTT uint64

	// SnapshotEntries is the number of applied entries between automatic
	// snapshots. Set to 0 to disable automatic snapshots.
	SnapshotEntries uint64

	// CompactionOverhead is the number of entries to retain after log
	// compaction for follower catch-up.
	CompactionOverhead uint64

	// OrderedConfigChange serializes configuration changes so that only
	// one membership change is applied at a time.
	OrderedConfigChange bool

	// MaxInMemLogSize is the maximum in-memory Raft log size in bytes
	// before rate limiting.
	MaxInMemLogSize uint64

	// SnapshotCompressionType selects the compression algorithm for
	// snapshot data.
	SnapshotCompressionType CompressionType

	// EntryCompressionType selects the compression algorithm for Raft
	// entry payloads.
	EntryCompressionType CompressionType

	// DisableAutoCompactions disables automatic log compaction when true.
	// When set, SnapshotEntries is forced to 0 in the quicraft config.
	DisableAutoCompactions bool

	// IsNonVoting marks this replica as a non-voting learner. Maps to
	// quicraft's IsObserver field.
	IsNonVoting bool

	// IsObserver is the deprecated name for IsNonVoting. When either is
	// true, the replica is a non-voting learner.
	IsObserver bool

	// IsWitness marks this replica as a voting member that participates
	// in elections and quorum but does not maintain a state machine.
	IsWitness bool

	// Quiesce enables quiescence for idle shards, reducing CPU usage
	// when there are no pending proposals.
	Quiesce bool

	// WaitReady is accepted for dragonboat API compatibility but is not
	// used by quicraft. It has no effect on behavior.
	WaitReady bool

	// MaxEntrySize is the maximum proposal payload size in bytes.
	MaxEntrySize uint64
}

// Validate checks that the config is valid for use. It performs basic
// sanity checks on field values that would cause failures during
// conversion or shard startup.
func (c Config) Validate() error {
	if c.ShardID == 0 {
		return &ValidationError{Field: "ShardID", Message: "must be > 0"}
	}
	if c.ReplicaID == 0 {
		return &ValidationError{Field: "ReplicaID", Message: "must be > 0"}
	}
	if c.HeartbeatRTT == 0 {
		return &ValidationError{Field: "HeartbeatRTT", Message: "must be > 0"}
	}
	if c.ElectionRTT == 0 {
		return &ValidationError{Field: "ElectionRTT", Message: "must be > 0"}
	}
	if c.ElectionRTT <= 2*c.HeartbeatRTT {
		return &ValidationError{Field: "ElectionRTT", Message: "must be > 2 * HeartbeatRTT"}
	}
	if c.IsNonVoting && c.IsWitness {
		return &ValidationError{Field: "IsNonVoting", Message: "IsNonVoting and IsWitness are mutually exclusive"}
	}
	if c.IsObserver && c.IsWitness {
		return &ValidationError{Field: "IsObserver", Message: "IsObserver and IsWitness are mutually exclusive"}
	}
	if c.IsWitness && c.SnapshotEntries > 0 {
		return &ValidationError{Field: "SnapshotEntries", Message: "must be 0 for witness nodes"}
	}
	if c.SnapshotEntries > 0 && c.CompactionOverhead > 0 && c.SnapshotEntries <= c.CompactionOverhead {
		return &ValidationError{Field: "SnapshotEntries", Message: "must be > CompactionOverhead when non-zero"}
	}
	return nil
}

// ToQuicraft converts the dragonboat-compatible Config to a quicraft
// config.Config. Boolean fields that dragonboat treats as plain bool
// are converted to *bool pointers so that user-specified values are
// respected rather than treated as zero-value defaults.
func (c Config) ToQuicraft() qcConfig.Config {
	observer := c.IsNonVoting || c.IsObserver

	snapshotEntries := c.SnapshotEntries
	if c.DisableAutoCompactions {
		snapshotEntries = 0
	}

	return qcConfig.Config{
		ShardID:                 c.ShardID,
		ReplicaID:               c.ReplicaID,
		IsObserver:              observer,
		IsWitness:               c.IsWitness,
		CheckQuorum:             qcConfig.BoolPtr(c.CheckQuorum),
		PreVote:                 qcConfig.BoolPtr(c.PreVote),
		ElectionRTT:             c.ElectionRTT,
		HeartbeatRTT:            c.HeartbeatRTT,
		SnapshotEntries:         snapshotEntries,
		CompactionOverhead:      c.CompactionOverhead,
		OrderedConfigChange:     qcConfig.BoolPtr(c.OrderedConfigChange),
		MaxInMemLogSize:         c.MaxInMemLogSize,
		SnapshotCompressionType: c.SnapshotCompressionType,
		EntryCompressionType:    c.EntryCompressionType,
		Quiesce:                 c.Quiesce,
		MaxEntrySize:            c.MaxEntrySize,
	}
}
