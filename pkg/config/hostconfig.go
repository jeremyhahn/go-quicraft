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
	"runtime"
	"testing"
	"time"
)

// isTestBinary reports whether the process is a go test binary; Validate refuses NoSyncWAL otherwise.
var isTestBinary = testing.Testing

// Default values for HostConfig fields.
const (
	DefaultRTTMillisecond       uint64        = 200
	DefaultNumWorkers           uint64        = 0 // 0 = auto = runtime.NumCPU()
	DefaultMaxApplyWorkers      int           = 0 // 0 = auto = runtime.NumCPU()
	DefaultCommitCBufferSize    int           = 64
	DefaultShutdownTimeout      time.Duration = 30 * time.Second
	DefaultMaxTotalInMemLogSize uint64        = 0
)

// LogFormat specifies the output format for structured logging.
type LogFormat string

const (
	// LogFormatText outputs human-readable key=value log lines.
	LogFormatText LogFormat = "text"

	// LogFormatJSON outputs machine-parseable JSON log lines.
	LogFormatJSON LogFormat = "json"
)

// LogLevel specifies the minimum severity level for log output.
type LogLevel string

// Log level constants for structured logging verbosity control.
const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// LogConfig controls structured logging output format and verbosity.
type LogConfig struct {
	// Format selects the log output format. Default: "text".
	Format LogFormat

	// Level sets the minimum log level. Default: "info".
	Level LogLevel
}

// SetDefaults fills zero-valued fields with their documented defaults.
func (lc *LogConfig) SetDefaults() {
	if lc.Format == "" {
		lc.Format = LogFormatText
	}
	if lc.Level == "" {
		lc.Level = LogLevelInfo
	}
}

// Validate checks that Format and Level contain recognized values.
func (lc *LogConfig) Validate() error {
	switch lc.Format {
	case LogFormatText, LogFormatJSON:
	default:
		return &LogValidationError{
			Field:   "Format",
			Message: "must be \"text\" or \"json\"",
			Value:   string(lc.Format),
		}
	}
	switch lc.Level {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
	default:
		return &LogValidationError{
			Field:   "Level",
			Message: "must be \"debug\", \"info\", \"warn\", or \"error\"",
			Value:   string(lc.Level),
		}
	}
	return nil
}

// MetricsConfig sets metric label cardinality at collector construction; the zero value labels only page-worthy signals with shard_id.
type MetricsConfig struct {
	// PerShardMetrics labels every metric with shard_id (about 113 series per shard instead of 6); alerting signals keep shard_id either way.
	PerShardMetrics bool
}

// EventListener holds optional callbacks: leader, shard and snapshot notifications run FIFO on a lock-free dispatcher, the rest synchronously and must not block.
type EventListener struct {
	// Raft events

	// OnLeaderUpdated fires when the engine reports a leader update for a
	// shard. LeaderID is 0 when no leader is known.
	OnLeaderUpdated func(info LeaderInfo)
	// OnLeaderLost fires when this replica was the shard's leader and no
	// longer is.
	OnLeaderLost func(info EventShardInfo)
	// OnLeaderReady fires once the leader of info.Term has committed an entry of that term, so ReadIndex is not refused until the next OnLeaderUpdated.
	OnLeaderReady func(info LeaderInfo)
	// OnCampaignLaunched fires when this replica starts an election.
	OnCampaignLaunched func(info EventShardInfo)

	// OnSnapshotRejected fires when a snapshot request, recovery or inbound transfer is refused or abandoned; info.Err is the cause if known.
	OnSnapshotRejected func(info SnapshotInfo)
	// OnProposalDropped fires when a proposal could not be handed to the
	// shard and was failed with ErrDropped.
	OnProposalDropped func(info EventShardInfo)
	// OnReadIndexDropped fires when a ReadIndex request could not be handed
	// to the shard and was refused with ErrSystemBusy.
	OnReadIndexDropped func(info EventShardInfo)

	// System events

	// OnNodeHostShuttingDown fires once, when Host.Close begins.
	OnNodeHostShuttingDown func()
	// OnNodeReady fires when StartShard has loaded a replica and it is
	// processing.
	OnNodeReady func(info EventShardInfo)
	// OnNodeUnloaded fires when a replica has been stopped and unloaded.
	OnNodeUnloaded func(info EventShardInfo)
	// OnNodeDeleted fires after RemoveData has deleted a replica's data.
	OnNodeDeleted func(info EventShardInfo)
	// OnMembershipChanged fires after an applied entry changed the shard's
	// membership.
	OnMembershipChanged func(info EventShardInfo)
	// OnConnectionEstablished fires when an outbound connection to a peer
	// is established.
	OnConnectionEstablished func(info ConnectionInfo)
	// OnConnectionFailed fires when a connection to a peer fails to
	// establish or is lost.
	OnConnectionFailed func(info ConnectionInfo)
	// OnSnapshotSendStarted fires when an outbound snapshot transfer starts.
	OnSnapshotSendStarted func(info SnapshotInfo)
	// OnSnapshotSent fires when an outbound snapshot transfer completes.
	OnSnapshotSent func(info SnapshotInfo)
	// OnSnapshotSendAborted fires when an outbound snapshot transfer fails.
	OnSnapshotSendAborted func(info SnapshotInfo)
	// OnSnapshotReceived fires when an inbound snapshot transfer has been
	// received in full.
	OnSnapshotReceived func(info SnapshotInfo)
	// OnSnapshotRecovered fires when a replica has restored its state
	// machine from a snapshot.
	OnSnapshotRecovered func(info SnapshotInfo)
	// OnSnapshotCreated fires when a snapshot save completes.
	OnSnapshotCreated func(info SnapshotInfo)
	// OnSnapshotCompacted fires after a successful snapshot save, alongside
	// OnSnapshotCreated.
	OnSnapshotCompacted func(info SnapshotInfo)
	// OnLogCompacted fires when a snapshot save compacted the in-memory log;
	// Index is the index compaction ran through.
	OnLogCompacted func(info EventShardInfo)
	// OnLogDBCompacted fires when the LogDB also removed the compacted
	// entries; it does not fire when that step failed.
	OnLogDBCompacted func(info EventShardInfo)

	// Failure events

	// OnShardFailed fires when a replica's failure latch trips and it stops
	// making progress until it is stopped and restarted.
	OnShardFailed func(info ShardFailedInfo)
	// OnLogDBDiskWarning fires when WAL disk usage crosses the warning
	// threshold of HostConfig.MaxWALDiskSize. It requires MaxWALDiskSize > 0.
	OnLogDBDiskWarning func(info DiskInfo)
	// OnLogDBDiskFull fires once at 90% (proposals refused) or 95% (WAL writes refused) of MaxWALDiskSize, and again only after usage drops below.
	OnLogDBDiskFull func(info DiskInfo)
}

// LeaderInfo describes a leader change event (OnLeaderUpdated) or a leader's
// read readiness (OnLeaderReady). ReplicaID is the local replica reporting it.
type LeaderInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Term      uint64
	LeaderID  uint64
}

// EventShardInfo identifies a shard for events. LeaderID is 0 when no
// leader is known.
type EventShardInfo struct {
	ShardID   uint64
	ReplicaID uint64
	LeaderID  uint64
	Term      uint64
	// Index is the log index a compaction ran through, set for
	// OnLogCompacted and OnLogDBCompacted and 0 for every other event.
	Index uint64
}

// SnapshotInfo describes a snapshot lifecycle event.
type SnapshotInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Index     uint64
	From      uint64
	// Err is the cause of an OnSnapshotRejected event when one is known, nil otherwise.
	Err error
}

// ShardFailedInfo describes a shard failure event.
type ShardFailedInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Err       error
}

// DiskInfo describes a disk usage event for WAL directories.
type DiskInfo struct {
	WALDir    string
	UsedBytes uint64
	MaxBytes  uint64
	// Shards lists, ascending, the shards whose data is in the WAL when the event fires.
	Shards []uint64
}

// ConnectionInfo describes a connection lifecycle event.
type ConnectionInfo struct {
	Address string
	Err     error
}

// HostConfig is per-host configuration controlling directories, network
// addresses, worker counts, resource limits, security, and observability.
type HostConfig struct {
	// WALDir is the directory for write-ahead log data (write-intensive).
	// Must not be empty.
	WALDir string

	// NodeHostDir is the working directory for metadata, snapshots, and
	// host ID. Must not be empty. Use a separate disk from WALDir in
	// production for best I/O performance.
	NodeHostDir string

	// RaftAddress is this node's address (host:port) as seen by other
	// nodes in the cluster.
	RaftAddress string

	// ListenAddress is the local bind address. It may differ from
	// RaftAddress when behind NAT or in containers. Must not be empty.
	ListenAddress string

	// RTTMillisecond is the estimated round-trip time in milliseconds.
	// Used to compute real timeouts: election timeout = ElectionRTT *
	// RTTMillisecond. Default: 200 (typical WAN). LAN: 1-5.
	RTTMillisecond uint64

	// DeploymentID is a plaintext cluster identity token, non-zero unless AllowZeroDeploymentID; use per-cluster CAs for cryptographic isolation.
	DeploymentID uint64

	// NumWorkers is the number of step/commit/apply workers. 0 means
	// auto-detect via runtime.NumCPU(). Snapshot workers use
	// NumWorkers/2 (minimum 2).
	NumWorkers uint64

	// MaxApplyWorkers is the maximum concurrent per-shard apply
	// goroutines. Default: runtime.NumCPU().
	MaxApplyWorkers int

	// CommitCBufferSize is the buffer size for the commit-to-apply
	// channel. Default: 64.
	CommitCBufferSize int

	// ShutdownTimeout bounds Drain only (default 30s); Close still waits for every in-flight SM.Apply.
	ShutdownTimeout time.Duration

	// MaxWALDiskSize is a ceiling on the WALDir filesystem's used bytes: warn at 85%, refuse proposals at 90%, refuse WAL writes at 95%; 0 is unlimited.
	MaxWALDiskSize uint64

	// NoSyncWAL skips fdatasync on WAL writes; Validate refuses it outside a test binary (ErrNoSyncWALOutsideTest).
	NoSyncWAL bool

	// MaxTotalInMemLogSize is the maximum total in-memory log size across
	// all shards. When exceeded, the global rate limiter rejects new
	// proposals with ErrSystemBusy. Default: 0 (disabled).
	MaxTotalInMemLogSize uint64

	// AllowZeroDeploymentID permits DeploymentID=0. Intended for
	// development and testing only.
	AllowZeroDeploymentID bool

	// NotifyCommit makes CommittedC fire after the local WAL persist; use ApplyResultC for read-after-write consistency.
	NotifyCommit bool

	// HostID is the host identifier; empty loads NodeHostDir/host.id or writes a new random 32-hex-character ID there.
	HostID string

	// EventListener holds per-type event callbacks. Nil means no events
	// are dispatched (zero overhead).
	EventListener *EventListener

	// TransportConfig holds transport-layer tuning parameters. A zero
	// value is valid; all fields have documented defaults.
	TransportConfig TransportConfig

	// KeyRotationInterval rotates the barrier epoch on a schedule (0 disables; 24h suggested); requires WithBarrier.
	KeyRotationInterval time.Duration

	// MaxRetainedEpochs is how many old key epochs stay in memory after rotation; 0 retains all.
	MaxRetainedEpochs uint64

	// LogConfig controls structured logging format and verbosity.
	// A zero value is valid; SetDefaults fills format=text, level=info.
	LogConfig LogConfig
}

// SetDefaults fills zero-valued fields with their documented defaults.
func (hc *HostConfig) SetDefaults() {
	if hc.RTTMillisecond == 0 {
		hc.RTTMillisecond = DefaultRTTMillisecond
	}
	if hc.NumWorkers == 0 {
		hc.NumWorkers = uint64(runtime.NumCPU())
	}
	if hc.MaxApplyWorkers == 0 {
		hc.MaxApplyWorkers = runtime.NumCPU()
	}
	if hc.CommitCBufferSize == 0 {
		hc.CommitCBufferSize = DefaultCommitCBufferSize
	}
	if hc.ShutdownTimeout == 0 {
		hc.ShutdownTimeout = DefaultShutdownTimeout
	}
	hc.TransportConfig.SetDefaults()
	hc.LogConfig.SetDefaults()
}

// Validate checks all host configuration constraints. Returns a
// *ValidationError describing the first violated rule, or nil
// if the configuration is valid.
func (hc *HostConfig) Validate() error {
	if hc.WALDir == "" {
		return newValidationError("WALDir", "must not be empty")
	}
	if hc.NodeHostDir == "" {
		return newValidationError("NodeHostDir", "must not be empty")
	}
	if hc.RaftAddress == "" {
		return newValidationError("RaftAddress", "must not be empty")
	}
	if hc.ListenAddress == "" {
		return newValidationError("ListenAddress", "must not be empty")
	}
	if hc.RTTMillisecond == 0 {
		return newValidationError("RTTMillisecond", "must be > 0")
	}
	// Upper bound prevents time.Duration (int64 ns) overflow when the tick
	// interval is computed as RTTMillisecond * time.Millisecond. 60_000 ms
	// (1 minute) is far beyond any sane Raft RTT and leaves enormous headroom.
	if hc.RTTMillisecond > 60_000 {
		return newValidationError("RTTMillisecond", "must be <= 60000 (1 minute)")
	}
	if hc.DeploymentID == 0 && !hc.AllowZeroDeploymentID {
		return newValidationError("DeploymentID",
			"must be > 0 (set AllowZeroDeploymentID for dev/testing)")
	}
	if hc.NumWorkers == 0 {
		return newValidationError("NumWorkers", "must be > 0")
	}
	if hc.MaxApplyWorkers < 1 {
		return newValidationError("MaxApplyWorkers", "must be >= 1")
	}
	if hc.CommitCBufferSize < 1 {
		return newValidationError("CommitCBufferSize", "must be >= 1")
	}
	if hc.ShutdownTimeout <= 0 {
		return newValidationError("ShutdownTimeout", "must be > 0")
	}
	if err := hc.TransportConfig.Validate(); err != nil {
		return err
	}
	if err := hc.LogConfig.Validate(); err != nil {
		return err
	}
	if hc.KeyRotationInterval != 0 && hc.KeyRotationInterval < time.Second {
		return &KeyRotationIntervalTooSmallError{
			Configured: hc.KeyRotationInterval,
			Minimum:    time.Second,
		}
	}
	if hc.KeyRotationInterval > 0 && hc.MaxRetainedEpochs == 0 {
		slog.Warn("KeyRotationInterval is set but MaxRetainedEpochs is 0: all epochs will be retained (unbounded growth)",
			"key_rotation_interval", hc.KeyRotationInterval,
		)
	}
	if hc.NoSyncWAL && !isTestBinary() {
		return ErrNoSyncWALOutsideTest
	}
	hc.warnDisabledSafetyFlags()
	return nil
}

// warnDisabledSafetyFlags logs host settings that trade durability or safety for performance.
func (hc *HostConfig) warnDisabledSafetyFlags() {
	if hc.NoSyncWAL {
		slog.Warn("NoSyncWAL enabled: Raft WAL writes are not fsynced, committed entries can be lost on power loss or OS crash (testing only)")
	}
}
