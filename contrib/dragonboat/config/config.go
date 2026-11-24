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

// Package config provides dragonboat-compatible configuration types that convert to quicraft equivalents.
package config

import (
	"log/slog"
	"sync"

	qcConfig "github.com/jeremyhahn/go-quicraft/pkg/config"
)

// entryNonCmdFieldsSize is the upper bound of a Raft entry's non-Cmd
// fields, matching dragonboat's settings.EntryNonCmdFieldsSize. A
// MaxInMemLogSize below one such entry cannot hold a single proposal.
const entryNonCmdFieldsSize = 16 * 8

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

	// CompactionOverhead is the number of entries kept after compaction; 0 with SnapshotEntries set maps to an explicit quicraft value.
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

	// DisableAutoCompactions approximates dragonboat by raising a zero CompactionOverhead to SnapshotEntries-1; snapshots continue.
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

	// MemberIdentities is passed to quicraft unchanged: initial member identities, or the join anchor when join is true; required with a transport.
	MemberIdentities map[uint64]string

	// Quiesce enables quiescence for idle shards, reducing CPU usage
	// when there are no pending proposals.
	Quiesce bool

	// WaitReady makes Start*Replica block, with a bounded poll, until the replica is initialized.
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
	if c.MaxInMemLogSize > 0 && c.MaxInMemLogSize < entryNonCmdFieldsSize+1 {
		return &ValidationError{Field: "MaxInMemLogSize", Message: "is too small"}
	}
	if !isKnownCompressionType(c.SnapshotCompressionType) {
		return &ValidationError{Field: "SnapshotCompressionType", Message: "unknown compression type"}
	}
	if !isKnownCompressionType(c.EntryCompressionType) {
		return &ValidationError{Field: "EntryCompressionType", Message: "unknown compression type"}
	}
	// Pairs dragonboat accepts are accepted here; ToQuicraft clamps them to quicraft's SnapshotEntries > CompactionOverhead >= 1.
	return nil
}

// isKnownCompressionType reports whether t is one of the compression
// algorithms dragonboat accepts.
func isKnownCompressionType(t CompressionType) bool {
	return t == NoCompression || t == Snappy
}

// autoCompactionsWarnOnce limits the DisableAutoCompactions warning to one
// emission per process; the conversion runs once per shard start.
var autoCompactionsWarnOnce sync.Once

// warnAutoCompactionsNotDisablable reports that quicraft has no switch to
// turn log compaction off, so DisableAutoCompactions cannot be honored
// exactly.
func warnAutoCompactionsNotDisablable() {
	autoCompactionsWarnOnce.Do(func() {
		slog.Warn("dragonboat adapter: DisableAutoCompactions cannot be fully honored; quicraft has no switch to disable log compaction, retention is governed by CompactionOverhead")
	})
}

// ToQuicraft converts the dragonboat-compatible Config to a quicraft config.Config, keeping explicit booleans as *bool.
func (c Config) ToQuicraft() qcConfig.Config {
	observer := c.IsNonVoting || c.IsObserver

	// SnapshotEntries passes through; zero or disabled compaction overhead maps to an explicit quicraft value so snapshots stay on.
	snapshotEntries := c.SnapshotEntries
	compactionOverhead := c.CompactionOverhead
	if snapshotEntries > 0 {
		if compactionOverhead == 0 {
			// Overhead 0 uses quicraft's default retention capped below SnapshotEntries; a configured overhead is kept.
			compactionOverhead = qcConfig.DefaultCompactionOverhead
			if c.DisableAutoCompactions {
				// With no user retention to preserve, approximate
				// "compaction disabled" with the maximum retention
				// quicraft's validation allows.
				compactionOverhead = snapshotEntries - 1
			}
		}
		if c.DisableAutoCompactions {
			warnAutoCompactionsNotDisablable()
		}
		// Quicraft requires SnapshotEntries > CompactionOverhead >= 1.
		// Dragonboat accepts pairs that violate both bounds, so clamp
		// rather than fail the shard start.
		if snapshotEntries < 2 {
			snapshotEntries = 2
		}
		if compactionOverhead >= snapshotEntries {
			compactionOverhead = snapshotEntries - 1
		}
	} else {
		// Dragonboat's SnapshotEntries 0 means snapshots off; SnapshotsDisabled keeps that meaning, which quicraft's zero default would not.
		snapshotEntries = qcConfig.SnapshotsDisabled
	}

	return qcConfig.Config{
		ShardID:    c.ShardID,
		ReplicaID:  c.ReplicaID,
		IsObserver: observer,
		IsWitness:  c.IsWitness,
		// Dragonboat does not restrict witness adds, so RequireFullReplicaMajority is off here.
		Witness: qcConfig.WitnessConfig{
			RequireFullReplicaMajority: qcConfig.BoolPtr(false),
		},
		CheckQuorum:             qcConfig.BoolPtr(c.CheckQuorum),
		PreVote:                 qcConfig.BoolPtr(c.PreVote),
		ElectionRTT:             c.ElectionRTT,
		HeartbeatRTT:            c.HeartbeatRTT,
		SnapshotEntries:         snapshotEntries,
		CompactionOverhead:      compactionOverhead,
		OrderedConfigChange:     qcConfig.BoolPtr(c.OrderedConfigChange),
		MaxInMemLogSize:         c.MaxInMemLogSize,
		SnapshotCompressionType: c.SnapshotCompressionType,
		EntryCompressionType:    c.EntryCompressionType,
		Quiesce:                 c.Quiesce,
		MaxEntrySize:            c.MaxEntrySize,
		MemberIdentities:        c.MemberIdentities,
	}
}
