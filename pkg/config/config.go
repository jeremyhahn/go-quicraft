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
	"math"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Default values for Config fields.
const (
	DefaultElectionRTT            uint64 = 10
	DefaultHeartbeatRTT           uint64 = 1
	DefaultCompactionOverhead     uint64 = 5000
	DefaultMaxInMemLogSize        uint64 = 0               // 0 = disabled
	DefaultMaxEntrySize           uint64 = 8 * 1024 * 1024 // 8 MB
	DefaultMaxApplyEntrySize      uint64 = MaxReplicationPayloadBytes
	DefaultMaxSnapshotSize        uint64 = 4 * 1024 * 1024 * 1024
	DefaultMaxResponsesPerSession uint64 = 128
	DefaultMaxTotalSessions       uint64 = 16384

	// DefaultMaxSessionsPerOwner caps sessions per declared owner so one owner cannot exhaust MaxTotalSessions; owner-less sessions are exempt.
	DefaultMaxSessionsPerOwner uint64 = 128

	// SessionsPerOwnerUnlimited turns the per-owner session cap off when assigned to Config.MaxSessionsPerOwner.
	SessionsPerOwnerUnlimited uint64 = math.MaxUint64

	// DefaultSessionExpiryEntries is the session inactivity threshold in applied entries, replicated into membership at bootstrap.
	DefaultSessionExpiryEntries uint64 = 100000

	// SessionExpiryDisabled turns session expiry off when assigned to Config.SessionExpiryEntries; dead clients then leak sessions.
	SessionExpiryDisabled uint64 = math.MaxUint64

	// DefaultSnapshotEntries is the applied-entry interval between automatic snapshots when Config.SnapshotEntries is unset.
	DefaultSnapshotEntries uint64 = 100000

	// SnapshotsDisabled turns automatic snapshots off when assigned to Config.SnapshotEntries; the caller must call Host.RequestSnapshot.
	SnapshotsDisabled uint64 = math.MaxUint64
)

// Bounds Validate places on a non-zero Config.MaxClockDrift.
const (
	MaxClockDriftMin float64 = 1e-6
	MaxClockDriftMax float64 = 0.5
)

// MaxReplicationPayloadBytes bounds MaxApplyEntrySize to what the transport can frame; transport asserts the two agree.
const MaxReplicationPayloadBytes uint64 = 16711672

// BoolPtr returns a pointer to the given bool value. This is the standard
// Go pattern for setting *bool config fields where nil means "use default".
// Exported for use by contrib packages (e.g., dragonboat adapter).
func BoolPtr(v bool) *bool { return &v }

// WitnessConfig seeds the shard's replicated witness policy at bootstrap; see docs/raft-consensus/engineering.md section 6.
type WitnessConfig struct {
	// MaxWitnesses caps witnesses in the shard; zero means no explicit cap, negative is invalid.
	MaxWitnesses int

	// RequireFullReplicaMajority rejects config changes leaving full replicas unable to form a quorum; nil means true.
	RequireFullReplicaMajority *bool
}

// GetRequireFullReplicaMajority returns the effective RequireFullReplicaMajority
// policy, defaulting to true (the safe posture) when unset.
func (w WitnessConfig) GetRequireFullReplicaMajority() bool {
	if w.RequireFullReplicaMajority == nil {
		return true
	}
	return *w.RequireFullReplicaMajority
}

// Config is per-shard Raft configuration; ShardID and ReplicaID are set by Host.StartShard.
type Config struct {
	// ShardID is the globally unique identifier for the Raft shard.
	// Set internally by Host.StartShard().
	ShardID uint64

	// ReplicaID is the unique identifier for this replica within the shard.
	// Set internally by Host.StartShard().
	ReplicaID uint64

	// IsObserver is the initial non-voting hint until replicated membership names the replica; add observers with join=true and Host.RequestAddObserver.
	IsObserver bool

	// IsWitness marks a voting replica without a state machine; it must match membership; add with join=true and Host.RequestAddWitness.
	IsWitness bool

	// Witness seeds the shard's replicated witness policy at bootstrap; see WitnessConfig.
	Witness WitnessConfig

	// MemberIdentities maps replica ID to node identity (SPKI SHA-256): the founding seed (identical on every founder) and the operator anchor for the receive gate.
	MemberIdentities map[uint64]string

	// RequireMemberIdentities refuses member admissions without a node identity; the Host sets it when it has a transport.
	RequireMemberIdentities bool

	// CheckQuorum makes the leader step down without quorum contact within an election timeout; nil means true, disabling is unsafe in production.
	CheckQuorum *bool

	// PreVote enables the pre-vote extension against disruptive elections; nil means true, disabling is unsafe in production.
	PreVote *bool

	// ElectionRTT is the election timeout measured in multiples of
	// RTTMillisecond. The recommended minimum is 10. Must be greater
	// than HeartbeatRTT.
	ElectionRTT uint64

	// HeartbeatRTT is the heartbeat interval measured in multiples of
	// RTTMillisecond. Must be at least 1.
	HeartbeatRTT uint64

	// SnapshotEntries is the applied-entry interval between automatic snapshots, above CompactionOverhead; zero selects the default, SnapshotsDisabled turns it off.
	SnapshotEntries uint64

	// CompactionOverhead is the number of entries to retain after log
	// compaction for follower catch-up. Default: 5000.
	CompactionOverhead uint64

	// MaxInMemLogSize is the maximum in-memory Raft log size in bytes
	// before rate limiting. Default: 0 (disabled).
	MaxInMemLogSize uint64

	// MaxProposalPayloadSize caps one proposal payload (ErrEntryTooLarge), at most proto.MaxEntrySize; the effective limit is min with MaxEntrySize. Default: proto.MaxEntrySize.
	MaxProposalPayloadSize uint64

	// MaxEntrySize caps an entry payload for transport, storage and decompression, at most proto.MaxEntrySize (8 MB). Default: 8 MB.
	MaxEntrySize uint64

	// MaxApplyEntrySize caps one outbound Replicate payload, at most MaxReplicationPayloadBytes (the default); it does not bound SM.Update batches.
	MaxApplyEntrySize uint64

	// MaxSnapshotSize is the maximum snapshot file size in bytes.
	// Default: 4 GB.
	MaxSnapshotSize uint64

	// MaxResponsesPerSession is the per-session response-cache depth, not a session count. Default: 128.
	MaxResponsesPerSession uint64

	// MaxTotalSessions is the shard-wide session limit; MaxSessionsPerOwner bounds one principal. Default: 16384.
	MaxTotalSessions uint64

	// MaxSessionsPerOwner caps sessions per declared owner (proto.Entry.OwnerID), seeded into replicated membership. Default: 128.
	MaxSessionsPerOwner uint64

	// SnapshotCompressionType selects the compression algorithm for
	// snapshot data.
	SnapshotCompressionType CompressionType

	// EntryCompressionType selects the compression algorithm for Raft
	// entry payloads.
	EntryCompressionType CompressionType

	// Quiesce enables quiescence for idle shards, reducing CPU usage when
	// there are no pending proposals.
	Quiesce bool

	// OrderedConfigChange has no effect: config changes are always serialised; kept for configuration compatibility.
	OrderedConfigChange *bool

	// ZeroCopyEntryCmd controls whether Entry.Cmd aliases the internal
	// buffer (zero-copy, zero-alloc). When true, the state machine MUST
	// copy Cmd if retaining it beyond Update(). Default: false (safe copy).
	ZeroCopyEntryCmd bool

	// SessionExpiryEntries is the replicated session inactivity threshold in applied entries (minimum 100); founders must agree, mismatches fail with ErrSessionPolicyMismatch.
	SessionExpiryEntries uint64

	// LeaseRead serves ReadIndex from a leader lease (Raft thesis 6.4) bounded by followers' recorded vote windows and MaxClockDrift. Default: false.
	LeaseRead bool

	// MaxClockDrift is the tolerated relative clock-rate difference for LeaseRead (0.01 is 1%); zero selects 1%.
	MaxClockDrift float64

	// DataDir roots this shard's on-disk state-machine data; empty uses NodeHostDir, set must be absolute. The WAL stays in the host LogDB.
	DataDir string
}

// SetDefaults fills zero-valued fields; CheckQuorum, PreVote and OrderedConfigChange default to true when nil.
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
	// An unset snapshot interval takes the default; SnapshotsDisabled asks for manual-only snapshots.
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = DefaultSnapshotEntries
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
	if c.MaxResponsesPerSession == 0 {
		c.MaxResponsesPerSession = DefaultMaxResponsesPerSession
	}
	if c.MaxTotalSessions == 0 {
		c.MaxTotalSessions = DefaultMaxTotalSessions
	}
	// An unset per-owner cap takes the default; SessionsPerOwnerUnlimited removes it.
	if c.MaxSessionsPerOwner == 0 {
		c.MaxSessionsPerOwner = DefaultMaxSessionsPerOwner
	}
	// An unset expiry threshold takes the default; SessionExpiryDisabled turns expiry off.
	if c.SessionExpiryEntries == 0 {
		c.SessionExpiryEntries = DefaultSessionExpiryEntries
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
	if err := c.validateDataDir(); err != nil {
		return err
	}
	c.warnDisabledSafetyFlags()
	return nil
}

// validateDataDir requires a set DataDir to be absolute; empty selects the default under NodeHostDir.
func (c *Config) validateDataDir() error {
	if c.DataDir == "" {
		return nil
	}
	if !filepath.IsAbs(c.DataDir) {
		return newValidationError("DataDir", "must be an absolute path when set")
	}
	return nil
}

func (c *Config) validateIdentity() error {
	if c.ShardID == 0 {
		return newValidationError("ShardID", "must be > 0")
	}
	if c.ReplicaID == 0 {
		return newValidationError("ReplicaID", "must be > 0")
	}
	// The replica ID is packed into the high bits of proposal keys, so it must fit 32 bits.
	if c.ReplicaID > math.MaxUint32 {
		return newValidationError("ReplicaID",
			"must be <= 4294967295 (the proposal key packs the replica ID)")
	}
	if c.IsObserver && c.IsWitness {
		return newValidationError("IsObserver", "IsObserver and IsWitness are mutually exclusive")
	}
	if c.Witness.MaxWitnesses < 0 {
		return newValidationError("Witness.MaxWitnesses", "must be >= 0 (0 means no explicit cap)")
	}
	return nil
}

// ValidateElectionTimeout checks ElectionRTT*rttMillisecond fits a time.Duration; Host.StartShard calls it.
func (c *Config) ValidateElectionTimeout(rttMillisecond uint64) error {
	const nanosPerMilli = 1_000_000
	if rttMillisecond > 0 && c.ElectionRTT > uint64(math.MaxInt64)/nanosPerMilli/rttMillisecond {
		return newValidationError("ElectionRTT",
			"ElectionRTT * RTTMillisecond overflows time.Duration on this host")
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
	// Bound both RTTs to math.MaxInt32 so downstream int conversions stay in range.
	if c.HeartbeatRTT > math.MaxInt32 {
		return newValidationError("HeartbeatRTT",
			"must be <= 2147483647 (raft narrows the tick multiple to int)")
	}
	if c.ElectionRTT > math.MaxInt32 {
		return newValidationError("ElectionRTT",
			"must be <= 2147483647 (raft narrows the tick multiple to int)")
	}
	// Require ElectionRTT at least 2x HeartbeatRTT (10x recommended), compared without overflow.
	if c.ElectionRTT/2 < c.HeartbeatRTT {
		return newValidationError("ElectionRTT",
			"must be >= 2*HeartbeatRTT (10*HeartbeatRTT recommended)")
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
	// MaxEntrySize doubles as the decompression ceiling on the apply path,
	// so an operator-raised value above the protocol maximum would let a
	// small compressed entry expand to an arbitrary size in memory.
	if c.MaxEntrySize > uint64(proto.MaxEntrySize) {
		return &MaxEntrySizeExceededError{
			Configured:      c.MaxEntrySize,
			ProtocolMaximum: uint64(proto.MaxEntrySize),
		}
	}
	if c.MaxApplyEntrySize == 0 {
		return newValidationError("MaxApplyEntrySize", "must be > 0")
	}
	if c.MaxApplyEntrySize > MaxReplicationPayloadBytes {
		return &MaxApplyEntrySizeExceededError{
			Configured:   c.MaxApplyEntrySize,
			FrameMaximum: MaxReplicationPayloadBytes,
		}
	}
	return nil
}

func (c *Config) validateSessions() error {
	if c.MaxResponsesPerSession == 0 {
		return newValidationError("MaxResponsesPerSession", "must be > 0")
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
	// Checked whether or not LeaseRead is on, so a value that would be
	// rejected is not carried silently until the lease is enabled.
	if d := c.MaxClockDrift; d != 0 && (math.IsNaN(d) || d < MaxClockDriftMin || d > MaxClockDriftMax) {
		return newValidationError("MaxClockDrift",
			"must be 0 (the default, 1%) or between MaxClockDriftMin (1e-6) and MaxClockDriftMax (0.5)")
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
		slog.Warn("OrderedConfigChange=false has no effect: membership changes are always serialised")
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
// Returns true if OrderedConfigChange is nil (safe default). The value
// governs nothing: membership changes are always serialised.
func (c *Config) GetOrderedConfigChange() bool {
	if c.OrderedConfigChange == nil {
		return true
	}
	return *c.OrderedConfigChange
}
