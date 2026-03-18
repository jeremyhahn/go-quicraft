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

package config

import (
	"log/slog"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Default values for Config fields.
const (
	DefaultElectionRTT          uint64 = 10
	DefaultHeartbeatRTT         uint64 = 1
	DefaultCompactionOverhead   uint64 = 5000
	DefaultMaxInMemLogSize      uint64 = 0                // 0 = disabled
	DefaultMaxEntrySize         uint64 = 8 * 1024 * 1024  // 8 MB
	DefaultMaxApplyEntrySize    uint64 = 64 * 1024 * 1024 // 64 MB
	DefaultMaxSnapshotSize      uint64 = 4 * 1024 * 1024 * 1024
	DefaultMaxSessionsPerClient uint64 = 128
	DefaultMaxTotalSessions     uint64 = 16384
)

// BoolPtr returns a pointer to the given bool value. This is the standard
// Go pattern for setting *bool config fields where nil means "use default".
// Exported for use by contrib packages (e.g., dragonboat adapter).
func BoolPtr(v bool) *bool { return &v }

// Config is per-shard Raft configuration. It controls election timing,
// log compaction, entry size limits, session limits, and compression.
// ShardID and ReplicaID are set internally by Host.StartShard() and
// should not be set by users directly.
type Config struct {
	// ShardID is the globally unique identifier for the Raft shard.
	// Set internally by Host.StartShard().
	ShardID uint64

	// ReplicaID is the unique identifier for this replica within the shard.
	// Set internally by Host.StartShard().
	ReplicaID uint64

	// IsObserver marks this replica as a non-voting learner that receives
	// replicated data but does not participate in elections or quorum.
	// Mutually exclusive with IsWitness.
	IsObserver bool

	// IsWitness marks this replica as a voting member that participates
	// in elections and quorum but does not maintain a state machine.
	// Mutually exclusive with IsObserver.
	IsWitness bool

	// CheckQuorum enables the leader to step down if it cannot reach a
	// quorum within an election timeout. Defaults to true when nil.
	// Set to BoolPtr(false) to explicitly disable for benchmarking
	// or compatibility testing. Disabling is UNSAFE for production.
	CheckQuorum *bool

	// PreVote enables the pre-vote protocol extension to prevent disruptive
	// elections from partitioned nodes. Defaults to true when nil.
	// Set to BoolPtr(false) to explicitly disable for benchmarking
	// or compatibility testing. Disabling is UNSAFE for production.
	PreVote *bool

	// ElectionRTT is the election timeout measured in multiples of
	// RTTMillisecond. The recommended minimum is 10. Must be greater
	// than HeartbeatRTT.
	ElectionRTT uint64

	// HeartbeatRTT is the heartbeat interval measured in multiples of
	// RTTMillisecond. Must be at least 1.
	HeartbeatRTT uint64

	// SnapshotEntries is the number of applied entries between automatic
	// snapshots. Set to 0 to disable automatic snapshots (manual-only via
	// Host.RequestSnapshot). When non-zero, must be greater than
	// CompactionOverhead.
	SnapshotEntries uint64

	// CompactionOverhead is the number of entries to retain after log
	// compaction for follower catch-up. Default: 5000.
	CompactionOverhead uint64

	// MaxInMemLogSize is the maximum in-memory Raft log size in bytes
	// before rate limiting. Default: 0 (disabled).
	MaxInMemLogSize uint64

	// MaxProposalPayloadSize is the maximum size of a single proposal payload
	// in bytes. Proposals exceeding this limit are rejected at Propose() with
	// ErrEntryTooLarge. This value must not exceed the wire protocol's
	// proto.MaxEntrySize (8 MB), which is the compile-time maximum enforced
	// during deserialization. Users may set a lower limit to constrain
	// proposal sizes but can never exceed the protocol maximum.
	//
	// Relationship with MaxEntrySize: At Propose() time, the effective limit
	// is min(MaxProposalPayloadSize, MaxEntrySize) when both are non-zero.
	// A proposal is rejected if it exceeds EITHER limit. This ensures that
	// MaxProposalPayloadSize can restrict proposals below MaxEntrySize, but
	// never bypass the transport/storage constraint imposed by MaxEntrySize.
	//
	// Default: proto.MaxEntrySize (8 MB).
	MaxProposalPayloadSize uint64

	// MaxEntrySize is the maximum Raft entry payload size in bytes for
	// transport and storage. Proposals exceeding this are rejected at
	// Propose() with ErrEntryTooLarge.
	//
	// Relationship with MaxProposalPayloadSize: Both limits are enforced at
	// Propose() time. The effective limit is min(MaxProposalPayloadSize,
	// MaxEntrySize) when both are set. Set MaxProposalPayloadSize to impose
	// a tighter user-facing limit while keeping MaxEntrySize at the
	// transport default.
	//
	// Default: 8 MB.
	MaxEntrySize uint64

	// MaxApplyEntrySize is the maximum total bytes of entries in a single
	// SM.Update() call. Default: 64 MB.
	MaxApplyEntrySize uint64

	// MaxSnapshotSize is the maximum snapshot file size in bytes.
	// Default: 4 GB.
	MaxSnapshotSize uint64

	// MaxSessionsPerClient is the maximum number of concurrent sessions per
	// client ID within a shard. Default: 128.
	MaxSessionsPerClient uint64

	// MaxTotalSessions is the maximum total sessions per shard across all
	// clients. Default: 16384.
	MaxTotalSessions uint64

	// SnapshotCompressionType selects the compression algorithm for
	// snapshot data.
	SnapshotCompressionType CompressionType

	// EntryCompressionType selects the compression algorithm for Raft
	// entry payloads.
	EntryCompressionType CompressionType

	// Quiesce enables quiescence for idle shards, reducing CPU usage when
	// there are no pending proposals.
	Quiesce bool

	// OrderedConfigChange serializes configuration changes so that only
	// one membership change is applied at a time. The Raft paper mandates
	// single-membership-change-at-a-time for safety. Defaults to true
	// when nil. Set to BoolPtr(false) to explicitly disable for
	// benchmarking or compatibility testing. Disabling is UNSAFE for
	// production.
	OrderedConfigChange *bool

	// ZeroCopyEntryCmd controls whether Entry.Cmd aliases the internal
	// buffer (zero-copy, zero-alloc). When true, the state machine MUST
	// copy Cmd if retaining it beyond Update(). Default: false (safe copy).
	ZeroCopyEntryCmd bool

	// SessionExpiryEntries controls automatic cleanup of abandoned client
	// sessions. Sessions that have not been active within this many applied
	// log entries are expired during the apply path. This is deterministic
	// across all replicas because it is driven by log index, not wall-clock
	// time. Set to 0 to disable automatic expiry (default). When disabled,
	// sessions are only removed via explicit unregister or when the
	// MaxTotalSessions limit forces rejection of new registrations.
	SessionExpiryEntries uint64

	// LeaseRead enables lease-based linearizable reads. When the leader has
	// received heartbeat acks from a quorum within the lease period (derived
	// from ElectionRTT), ReadIndex requests are served immediately without a
	// heartbeat round-trip. The lease timeout is ElectionRTT - 2 ticks to
	// provide a safety margin: followers will not grant votes to another
	// candidate until their election timeout expires, and the leader's lease
	// expires before that. Requires monotonic clocks (guaranteed by Go's
	// time.Now on supported platforms). Per Raft PhD thesis Section 6.4.
	// Default: false (use heartbeat-based ReadIndex).
	LeaseRead bool
}

// SetDefaults fills zero-valued fields with their documented defaults.
//
// CheckQuorum, PreVote, and OrderedConfigChange default to true when nil.
// These are Raft safety invariants: disabling CheckQuorum permits a stale
// leader to serve reads after a partition heals, disabling PreVote allows
// disruptive elections from partitioned nodes, and disabling
// OrderedConfigChange violates the single-membership-change-at-a-time
// requirement from the Raft paper. Explicitly setting these to
// BoolPtr(false) is respected but will emit a warning during Validate().
func (c *Config) SetDefaults() {
	// Safety-critical defaults: only apply when user has not explicitly set.
	if c.CheckQuorum == nil {
		c.CheckQuorum = BoolPtr(true)
	}
	if c.PreVote == nil {
		c.PreVote = BoolPtr(true)
	}
	if c.OrderedConfigChange == nil {
		c.OrderedConfigChange = BoolPtr(true)
	}

	if c.ElectionRTT == 0 {
		c.ElectionRTT = DefaultElectionRTT
	}
	if c.HeartbeatRTT == 0 {
		c.HeartbeatRTT = DefaultHeartbeatRTT
	}
	if c.CompactionOverhead == 0 {
		c.CompactionOverhead = DefaultCompactionOverhead
	}
	if c.MaxProposalPayloadSize == 0 {
		c.MaxProposalPayloadSize = uint64(proto.MaxEntrySize)
	}
	if c.MaxEntrySize == 0 {
		c.MaxEntrySize = DefaultMaxEntrySize
	}
	if c.MaxApplyEntrySize == 0 {
		c.MaxApplyEntrySize = DefaultMaxApplyEntrySize
	}
	if c.MaxSnapshotSize == 0 {
		c.MaxSnapshotSize = DefaultMaxSnapshotSize
	}
	if c.MaxSessionsPerClient == 0 {
		c.MaxSessionsPerClient = DefaultMaxSessionsPerClient
	}
	if c.MaxTotalSessions == 0 {
		c.MaxTotalSessions = DefaultMaxTotalSessions
	}
}

// Validate checks all cross-field constraints. It returns a
// *ValidationError describing the first violated rule, or nil
// if the configuration is valid.
func (c *Config) Validate() error {
	if err := c.validateIdentity(); err != nil {
		return err
	}
	if err := c.validateTiming(); err != nil {
		return err
	}
	if err := c.validateSizeLimits(); err != nil {
		return err
	}
	if err := c.validateSessions(); err != nil {
		return err
	}
	if err := c.validateCompression(); err != nil {
		return err
	}
	if err := c.validateLeaseRead(); err != nil {
		return err
	}
	c.warnDisabledSafetyFlags()
	return nil
}

func (c *Config) validateIdentity() error {
	if c.ShardID == 0 {
		return newValidationError("ShardID", "must be > 0")
	}
	if c.ReplicaID == 0 {
		return newValidationError("ReplicaID", "must be > 0")
	}
	if c.IsObserver && c.IsWitness {
		return newValidationError("IsObserver", "IsObserver and IsWitness are mutually exclusive")
	}
	return nil
}

func (c *Config) validateTiming() error {
	if c.HeartbeatRTT < 1 {
		return newValidationError("HeartbeatRTT", "must be >= 1")
	}
	if c.ElectionRTT < 5 {
		return newValidationError("ElectionRTT", "must be >= 5 (10 recommended)")
	}
	if c.ElectionRTT <= c.HeartbeatRTT {
		return newValidationError("ElectionRTT", "must be > HeartbeatRTT")
	}
	return nil
}

func (c *Config) validateSizeLimits() error {
	if c.SnapshotEntries > 0 && c.SnapshotEntries <= c.CompactionOverhead {
		return newValidationError("SnapshotEntries", "must be > CompactionOverhead when non-zero")
	}
	if c.MaxProposalPayloadSize == 0 {
		return newValidationError("MaxProposalPayloadSize", "must be > 0")
	}
	if c.MaxProposalPayloadSize > uint64(proto.MaxEntrySize) {
		return &MaxProposalPayloadSizeExceededError{
			Configured:      c.MaxProposalPayloadSize,
			ProtocolMaximum: uint64(proto.MaxEntrySize),
		}
	}
	if c.MaxEntrySize == 0 {
		return newValidationError("MaxEntrySize", "must be > 0")
	}
	if c.MaxApplyEntrySize == 0 {
		return newValidationError("MaxApplyEntrySize", "must be > 0")
	}
	return nil
}

func (c *Config) validateSessions() error {
	if c.MaxSessionsPerClient == 0 {
		return newValidationError("MaxSessionsPerClient", "must be > 0")
	}
	if c.MaxTotalSessions == 0 {
		return newValidationError("MaxTotalSessions", "must be > 0")
	}
	if c.SessionExpiryEntries != 0 && c.SessionExpiryEntries < 100 {
		return &SessionExpiryEntriesTooSmallError{
			Configured: c.SessionExpiryEntries,
			Minimum:    100,
		}
	}
	if c.SessionExpiryEntries != 0 && c.SnapshotEntries != 0 &&
		c.SessionExpiryEntries > c.SnapshotEntries {
		slog.Warn("SessionExpiryEntries exceeds SnapshotEntries: sessions may never expire between snapshots",
			"session_expiry_entries", c.SessionExpiryEntries,
			"snapshot_entries", c.SnapshotEntries,
		)
	}
	return nil
}

func (c *Config) validateCompression() error {
	if !c.SnapshotCompressionType.IsValid() {
		return newValidationError("SnapshotCompressionType", "unrecognized compression type")
	}
	if !c.EntryCompressionType.IsValid() {
		return newValidationError("EntryCompressionType", "unrecognized compression type")
	}
	return nil
}

func (c *Config) validateLeaseRead() error {
	if c.LeaseRead && !c.GetCheckQuorum() {
		return &LeaseReadRequiresCheckQuorumError{}
	}
	if c.LeaseRead && c.ElectionRTT < 5 {
		return newValidationError("ElectionRTT",
			"must be >= 5 when LeaseRead is enabled (lease timeout = ElectionRTT - 2)")
	}
	return nil
}

// warnDisabledSafetyFlags emits warnings when safety booleans are explicitly
// disabled. These are not rejected to support benchmarking and compatibility testing.
func (c *Config) warnDisabledSafetyFlags() {
	if c.CheckQuorum != nil && !*c.CheckQuorum {
		slog.Warn("CheckQuorum disabled: leader will not step down on quorum loss")
	}
	if c.PreVote != nil && !*c.PreVote {
		slog.Warn("PreVote disabled: partitioned nodes may cause disruptive elections")
	}
	if c.OrderedConfigChange != nil && !*c.OrderedConfigChange {
		slog.Warn("OrderedConfigChange disabled: concurrent membership changes violate Raft safety")
	}
}

// GetCheckQuorum returns the effective CheckQuorum value.
// Returns true if CheckQuorum is nil (safe default).
func (c *Config) GetCheckQuorum() bool {
	if c.CheckQuorum == nil {
		return true
	}
	return *c.CheckQuorum
}

// GetPreVote returns the effective PreVote value.
// Returns true if PreVote is nil (safe default).
func (c *Config) GetPreVote() bool {
	if c.PreVote == nil {
		return true
	}
	return *c.PreVote
}

// GetOrderedConfigChange returns the effective OrderedConfigChange value.
// Returns true if OrderedConfigChange is nil (safe default).
func (c *Config) GetOrderedConfigChange() bool {
	if c.OrderedConfigChange == nil {
		return true
	}
	return *c.OrderedConfigChange
}
