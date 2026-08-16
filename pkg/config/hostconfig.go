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
	"time"
)

// Default values for HostConfig fields.
const (
	DefaultRTTMillisecond       uint64        = 200
	DefaultNumWorkers           uint64        = 0 // 0 = auto = runtime.NumCPU()
	DefaultMaxApplyWorkers      int           = 0 // 0 = auto = runtime.NumCPU()
	DefaultCommitCBufferSize    int           = 64
	DefaultShutdownTimeout      time.Duration = 30 * time.Second
	DefaultMaxApplyRetries      uint64        = 100
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

// MetricsConfig controls the label cardinality of the metrics a host exports.
//
// It is deliberately NOT a HostConfig field. A Prometheus vector's label set is
// fixed when the vector is constructed, and the collector is built by the
// embedder and handed to the host through WithMetrics — by the time a host sees
// its HostConfig the label sets already exist. The knob therefore belongs to
// collector construction, where it can still take effect; see
// quicraft.NewPrometheusMetricsCollectorWithConfig. A HostConfig field would be
// inert and would imply a control the host does not have.
//
// The zero value is the safe value: metrics aggregate to host level and only
// the shard-scoped signals an operator pages on keep a shard_id label.
type MetricsConfig struct {
	// PerShardMetrics labels the full metric set with shard_id, giving a
	// per-shard breakdown of latency, batch size and snapshot volume.
	//
	// It defaults to false because the cost is far higher than the count of
	// shard-labelled metric NAMES suggests: a Prometheus histogram child is
	// one series per bucket plus _sum and _count, so a shard's handful of
	// latency histograms expand into dozens of series. Measured (see
	// TestPrometheusMetrics_AggregatedModeSeriesBudget), each active shard
	// costs 113 series with PerShardMetrics on and 6 with it off. At the
	// 10,000-shard scale this library is designed for that is 1.13M active
	// series from a single host versus 60k — enough to destabilise a shared
	// Prometheus, a failure that lands on every other tenant of that
	// Prometheus and not only on the operator who enabled it.
	//
	// Disabling it never costs an alert. The signals worth paging on keep
	// their shard_id in BOTH modes: elections (leader change), dropped
	// proposals, transport message and snapshot drops, and every security
	// counter. What aggregates is the volume-and-latency telemetry, whose
	// per-shard breakdown is a debugging luxury rather than an alerting
	// input. Enable it for small deployments or when bisecting a specific
	// shard's latency.
	PerShardMetrics bool
}

// EventListener holds per-type optional event callbacks. Only non-nil
// callbacks are invoked. Callbacks are called synchronously on engine
// worker goroutines and must not block. See the package-level
// documentation for callback guidelines.
type EventListener struct {
	// Raft events
	OnLeaderUpdated    func(info LeaderInfo)
	OnLeaderLost       func(info EventShardInfo)
	OnCampaignLaunched func(info EventShardInfo)

	// OnCampaignSkipped is called when an election is skipped because
	// uncommitted config change entries exist between committed and
	// lastIndex. Elections during membership transitions can violate
	// the overlap guarantee.
	OnCampaignSkipped func(info EventShardInfo)

	OnSnapshotRejected func(info SnapshotInfo)
	OnProposalDropped  func(info EventShardInfo)
	OnReadIndexDropped func(info EventShardInfo)

	// System events
	OnNodeHostShuttingDown  func()
	OnNodeReady             func(info EventShardInfo)
	OnNodeUnloaded          func(info EventShardInfo)
	OnNodeDeleted           func(info EventShardInfo)
	OnMembershipChanged     func(info EventShardInfo)
	OnConnectionEstablished func(info ConnectionInfo)
	OnConnectionFailed      func(info ConnectionInfo)
	OnSnapshotSendStarted   func(info SnapshotInfo)
	OnSnapshotSent          func(info SnapshotInfo)
	OnSnapshotSendAborted   func(info SnapshotInfo)
	OnSnapshotReceived      func(info SnapshotInfo)
	OnSnapshotRecovered     func(info SnapshotInfo)
	OnSnapshotCreated       func(info SnapshotInfo)
	OnSnapshotCompacted     func(info SnapshotInfo)
	OnLogCompacted          func(info EventShardInfo)
	OnLogDBCompacted        func(info EventShardInfo)

	// Failure events
	OnShardFailed      func(info ShardFailedInfo)
	OnLogDBDiskWarning func(info DiskInfo)
	OnLogDBDiskFull    func(info DiskInfo)
}

// LeaderInfo describes a leader change event.
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
}

// SnapshotInfo describes a snapshot lifecycle event.
type SnapshotInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Index     uint64
	From      uint64
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
	Shards    []uint64
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

	// DeploymentID is the cluster identity token. Prevents accidental
	// cross-cluster communication. Must be non-zero unless
	// AllowZeroDeploymentID is set.
	//
	// Security note: DeploymentID provides application-layer cluster
	// isolation only. It is transmitted in plaintext within Raft
	// messages and is not cryptographically bound to TLS certificates.
	// An attacker with network access could forge the ID. For
	// cryptographic isolation between clusters, use separate CA
	// certificates per cluster so that mTLS handshakes reject
	// cross-cluster connections at the TLS layer.
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

	// ShutdownTimeout is the maximum time to wait for graceful shutdown.
	// Default: 30s.
	//
	// It bounds Drain ONLY. Host.Close additionally waits, with NO timeout,
	// for every in-flight SM.Apply to return: the apply worker's shutdown
	// joins its per-shard goroutines, and one parked inside a user state
	// machine's Apply never returns. A state machine that blocks in Apply
	// therefore blocks Close indefinitely, whatever this value is.
	//
	// The wait cannot safely be abandoned — dropping it would let Close return
	// while an apply goroutine still touches state the caller is about to free
	// — so this is a documented limit rather than a bug to configure around.
	// The shards still applying at shutdown are logged before the wait begins.
	ShutdownTimeout time.Duration

	// MaxWALDiskSize is the maximum WAL directory size in bytes. 0 means
	// unlimited.
	MaxWALDiskSize uint64

	// NoSyncWAL disables fdatasync on Raft WAL segment writes. When true,
	// the WAL does not wait for OS-level write synchronisation. This trades
	// durability for performance and is appropriate only for testing
	// environments where overlay2/Docker fdatasync latency causes Raft
	// proposal timeouts. Never use in production.
	NoSyncWAL bool

	// MaxApplyRetries is the maximum consecutive SM.Apply() failures
	// before the circuit breaker trips. Default: 100.
	MaxApplyRetries uint64

	// MaxTotalInMemLogSize is the maximum total in-memory log size across
	// all shards. When exceeded, the global rate limiter rejects new
	// proposals with ErrSystemBusy. Default: 0 (disabled).
	MaxTotalInMemLogSize uint64

	// AllowZeroDeploymentID permits DeploymentID=0. Intended for
	// development and testing only.
	AllowZeroDeploymentID bool

	// NotifyCommit enables WAL-durable early notification. When true,
	// CommittedC() returns the committed channel that fires after local
	// WAL persist (~1-2ms) rather than waiting for full quorum commit
	// and state machine apply (~10-30ms). Callers who need strong
	// read-after-write consistency should use ApplyResultC() instead.
	NotifyCommit bool

	// HostID is the unique host identifier. When empty, a new UUID is
	// generated or loaded from NodeHostDir/host.id.
	HostID string

	// EventListener holds per-type event callbacks. Nil means no events
	// are dispatched (zero overhead).
	EventListener *EventListener

	// TransportConfig holds transport-layer tuning parameters. A zero
	// value is valid; all fields have documented defaults.
	TransportConfig TransportConfig

	// KeyRotationInterval is the interval at which the barrier epoch is
	// rotated on a schedule. When 0 (the default), scheduled rotation is
	// disabled; invocation-threshold rotation (NIST SP 800-38D AEAD
	// budget enforcement) still runs whenever a barrier is configured.
	// A recommended production value is 24 hours. Requires a barrier to
	// be configured via WithBarrier; ignored otherwise.
	KeyRotationInterval time.Duration

	// MaxRetainedEpochs is the maximum number of old encryption key
	// epochs to retain after rotation. Epochs older than
	// currentEpoch - MaxRetainedEpochs are purged from memory.
	// Zero means retain all epochs (no automatic purging).
	// Default: 0 (no purging for backward compatibility).
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
	if hc.MaxApplyRetries == 0 {
		hc.MaxApplyRetries = DefaultMaxApplyRetries
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
	if hc.MaxApplyRetries == 0 {
		return newValidationError("MaxApplyRetries", "must be > 0")
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
	hc.warnDisabledSafetyFlags()
	return nil
}

// warnDisabledSafetyFlags emits warnings for host settings that trade a
// durability or safety guarantee for performance. They are not rejected —
// benchmarking and containerised test environments legitimately need them —
// but a config copied into production must leave a trace in the logs.
func (hc *HostConfig) warnDisabledSafetyFlags() {
	if hc.NoSyncWAL {
		slog.Warn("NoSyncWAL enabled: Raft WAL writes are not fsynced, committed entries can be lost on power loss or OS crash (testing only)")
	}
}
