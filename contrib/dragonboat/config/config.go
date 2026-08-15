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

	// CompactionOverhead is the number of entries to retain after log
	// compaction for follower catch-up. When left 0 with SnapshotEntries
	// set, the conversion picks an explicit quicraft value (see
	// ToQuicraft) because quicraft cannot express zero retention.
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
	// In dragonboat this only suppresses the compaction request issued
	// after each snapshot; snapshotting continues at the SnapshotEntries
	// cadence and CompactionOverhead still governs retention for a manual
	// RequestCompaction. Quicraft has no switch to turn compaction off, so
	// the conversion approximates it by raising CompactionOverhead to
	// SnapshotEntries-1 ONLY when CompactionOverhead is 0; an explicitly
	// configured CompactionOverhead is preserved.
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

	// WaitReady makes Start*Replica block until the replica is initialized,
	// matching dragonboat's startShard. Quicraft's StartShard returns as
	// soon as the node is registered, so the adapter polls the shard's
	// readiness after StartShard returns and gives up after a bounded
	// timeout (see the readiness wait in the dragonboat NodeHost adapter).
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
	// SnapshotEntries == 1 and SnapshotEntries <= CompactionOverhead are
	// accepted by dragonboat, so they are accepted here too. Quicraft
	// requires SnapshotEntries > CompactionOverhead >= 1, so ToQuicraft
	// clamps the pair instead of this rejecting the config.
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

// ToQuicraft converts the dragonboat-compatible Config to a quicraft
// config.Config. Boolean fields that dragonboat treats as plain bool
// are converted to *bool pointers so that user-specified values are
// respected rather than treated as zero-value defaults.
func (c Config) ToQuicraft() qcConfig.Config {
	observer := c.IsNonVoting || c.IsObserver

	// SnapshotEntries passes through unchanged: dragonboat's
	// DisableAutoCompactions only disables log compaction, never
	// snapshotting. Quicraft cannot switch compaction off, and it also
	// cannot express dragonboat's CompactionOverhead=0 ("retain no extra
	// entries") because quicraft's SetDefaults replaces a zero
	// CompactionOverhead with DefaultCompactionOverhead, which its
	// validation then requires to be < SnapshotEntries. Both cases are
	// therefore mapped to an explicit CompactionOverhead here so that
	// every valid dragonboat config starts cleanly and snapshots are
	// never silently disabled.
	snapshotEntries := c.SnapshotEntries
	compactionOverhead := c.CompactionOverhead
	if snapshotEntries > 0 {
		if compactionOverhead == 0 {
			// Dragonboat with CompactionOverhead 0 retains no extra
			// entries after compaction. Use quicraft's default retention,
			// capped below SnapshotEntries so quicraft's
			// SnapshotEntries > CompactionOverhead check passes. Retaining
			// more entries than dragonboat's zero only costs disk space;
			// followers that fall behind further catch up via snapshot.
			//
			// DisableAutoCompactions does NOT override a CompactionOverhead
			// the user configured: dragonboat's flag only suppresses the
			// automatic compaction request, while CompactionOverhead keeps
			// governing retention for a manual RequestCompaction.
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
		// Dragonboat reads SnapshotEntries == 0 as "automatic snapshotting is
		// off; the user will call RequestSnapshot". Quicraft used to read a
		// zero the same way, but no longer does: a zero there now means "I did
		// not choose" and SetDefaults substitutes DefaultSnapshotEntries.
		// Passing the zero through would therefore silently ENABLE snapshotting
		// on a config that explicitly asked for it off — and would do it to
		// every witness, since dragonboat REQUIRES SnapshotEntries == 0 for
		// witnesses (see Validate).
		//
		// SnapshotsDisabled is the sentinel that preserves the caller's
		// meaning. CompactionOverhead is left as the caller set it: with no
		// automatic snapshot there is no automatic compaction for it to bound,
		// and quicraft's SnapshotEntries > CompactionOverhead check does not
		// apply to the sentinel.
		snapshotEntries = qcConfig.SnapshotsDisabled
	}

	return qcConfig.Config{
		ShardID:    c.ShardID,
		ReplicaID:  c.ReplicaID,
		IsObserver: observer,
		IsWitness:  c.IsWitness,
		// Dragonboat has no witness-membership policy: witness adds are
		// unrestricted. Disable quicraft's RequireFullReplicaMajority default
		// so the adapter preserves dragonboat semantics (its unified quorum
		// carries the same durability trade-off; see quicraft
		// config.WitnessConfig).
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
	}
}
