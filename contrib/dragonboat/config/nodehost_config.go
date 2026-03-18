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
	"net"
	"os"

	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/raftio"
	qcConfig "github.com/jeremyhahn/go-quicraft/pkg/config"
)

// NodeHostConfig is the dragonboat-compatible per-host configuration.
// It is converted to quicraft's HostConfig via ToQuicraft().
type NodeHostConfig struct {
	// NodeHostID is the user-specified unique identity for this NodeHost
	// instance. When empty, the system generates one automatically.
	NodeHostID string

	// DeploymentID is the cluster identity token that prevents
	// accidental cross-cluster communication.
	DeploymentID uint64

	// WALDir is the directory for write-ahead log data.
	WALDir string

	// NodeHostDir is the working directory for metadata and snapshots.
	NodeHostDir string

	// RTTMillisecond is the estimated round-trip time in milliseconds.
	RTTMillisecond uint64

	// RaftAddress is this node's address (host:port) as seen by other
	// nodes in the cluster.
	RaftAddress string

	// ListenAddress is the local bind address. It may differ from
	// RaftAddress when behind NAT or in containers.
	ListenAddress string

	// MutualTLS enables mutual TLS authentication. When true, CAFile,
	// CertFile, and KeyFile must be set to valid file paths.
	MutualTLS bool

	// CAFile is the path to the PEM-encoded CA certificate file.
	// Used only when MutualTLS is true.
	CAFile string

	// CertFile is the path to the PEM-encoded certificate file.
	// Used only when MutualTLS is true.
	CertFile string

	// KeyFile is the path to the PEM-encoded private key file.
	// Used only when MutualTLS is true.
	KeyFile string

	// EnableMetrics enables Prometheus metrics collection. This field
	// is accepted for dragonboat API compatibility but metrics in
	// quicraft are always available via the metrics package.
	EnableMetrics bool

	// MaxSendQueueSize is the maximum outbound message queue size.
	// Accepted for compatibility but mapped to transport config.
	MaxSendQueueSize uint64

	// MaxReceiveQueueSize is the maximum inbound message queue size.
	// Accepted for compatibility but mapped to transport config.
	MaxReceiveQueueSize uint64

	// NotifyCommit enables WAL-durable early notification.
	NotifyCommit bool

	// DefaultNodeRegistryEnabled enables the built-in node registry
	// for automatic node discovery.
	DefaultNodeRegistryEnabled bool

	// RaftEventListener receives Raft protocol event notifications.
	RaftEventListener raftio.IRaftEventListener

	// SystemEventListener receives system-level event notifications.
	SystemEventListener raftio.ISystemEventListener

	// Gossip configures the gossip protocol for node discovery.
	Gossip GossipConfig

	// Expert holds advanced tuning parameters.
	Expert ExpertConfig
}

// ExpertConfig holds advanced tuning parameters for the NodeHost.
type ExpertConfig struct {
	Engine EngineConfig
	LogDB  LogDBConfig
}

// EngineConfig controls the number of worker goroutines for each
// engine processing stage.
type EngineConfig struct {
	// ExecShards is the number of execution worker shards.
	ExecShards uint64

	// CommitShards is the number of commit worker shards.
	CommitShards uint64

	// ApplyShards is the number of apply worker shards.
	ApplyShards uint64

	// SnapshotShards is the number of snapshot worker shards.
	SnapshotShards uint64

	// CloseShards is the number of close worker shards.
	CloseShards uint64
}

// LogDBConfig controls the number of LogDB shards.
type LogDBConfig struct {
	// Shards is the number of LogDB shards for parallel WAL access.
	Shards uint64
}

// IsEmpty returns true when LogDBConfig has no shards configured.
func (cfg *LogDBConfig) IsEmpty() bool {
	return cfg.Shards == 0
}

// MemorySizeMB returns the estimated memory usage in megabytes.
// Returns 0 as a placeholder; actual memory depends on workload.
func (cfg *LogDBConfig) MemorySizeMB() uint64 {
	return 0
}

// GossipConfig configures the gossip protocol for node discovery.
type GossipConfig struct {
	// BindAddress is the local address for gossip protocol communication.
	BindAddress string

	// AdvertiseAddress is the address advertised to other nodes. When
	// empty, BindAddress is used.
	AdvertiseAddress string

	// Seed is a list of seed node addresses for joining the gossip cluster.
	Seed []string

	// Meta is application-specific metadata attached to gossip messages.
	Meta []byte
}

// IsEmpty returns true when no gossip configuration fields are set.
func (g *GossipConfig) IsEmpty() bool {
	return g.BindAddress == "" && g.AdvertiseAddress == "" && len(g.Seed) == 0
}

// Validate checks the gossip configuration. Returns nil when the
// configuration is empty (gossip disabled). Returns an error when
// gossip is partially configured with missing required fields.
func (g *GossipConfig) Validate() error {
	if g.IsEmpty() {
		return nil
	}
	if g.BindAddress == "" {
		return &ValidationError{Field: "GossipConfig.BindAddress", Message: "must not be empty when gossip is configured"}
	}
	return nil
}

// TargetValidator is a function that validates a target address string.
type TargetValidator func(string) bool

// RaftAddressValidator is a function that validates a Raft address string.
type RaftAddressValidator func(string) bool

// LogDBCallback is invoked by the LogDB factory to report shard status.
type LogDBCallback func(LogDBInfo)

// LogDBInfo reports the status of a LogDB shard.
type LogDBInfo struct {
	Shard uint64
	Busy  bool
}

// LogDBFactory creates LogDB instances for NodeHost.
type LogDBFactory interface {
	Create(NodeHostConfig, LogDBCallback, []string, []string) (raftio.ILogDB, error)
	Name() string
}

// LogDBFactoryFunc is a function type matching the LogDB factory Create
// signature. This matches dragonboat v4's deprecated LogDBFactoryFunc.
type LogDBFactoryFunc func(NodeHostConfig, LogDBCallback, []string, []string) (raftio.ILogDB, error)

// RaftRPCFactoryFunc creates Raft RPC transport instances. This matches
// dragonboat v4's deprecated RaftRPCFactoryFunc signature.
type RaftRPCFactoryFunc func(NodeHostConfig, raftio.MessageHandler, raftio.ChunkHandler) raftio.ITransport

// TransportFactory creates transport instances for NodeHost.
type TransportFactory interface {
	Create(NodeHostConfig, raftio.MessageHandler, raftio.ChunkHandler) raftio.ITransport
	Validate(string) bool
}

// NodeRegistryFactory creates node registry instances for NodeHost.
type NodeRegistryFactory interface {
	Create(nhid string, streamConnections uint64, v TargetValidator) (raftio.INodeRegistry, error)
}

// IsValidAddress returns true when addr is in valid host:port format.
func IsValidAddress(addr string) bool {
	_, _, err := net.SplitHostPort(addr)
	return err == nil
}

// GetDefaultEngineConfig returns the default EngineConfig with standard
// worker shard counts.
func GetDefaultEngineConfig() EngineConfig {
	return EngineConfig{
		ExecShards:     16,
		CommitShards:   16,
		ApplyShards:    16,
		SnapshotShards: 48,
		CloseShards:    32,
	}
}

// GetDefaultExpertConfig returns the default ExpertConfig using the
// default engine configuration.
func GetDefaultExpertConfig() ExpertConfig {
	return ExpertConfig{
		Engine: GetDefaultEngineConfig(),
	}
}

// GetDefaultLogDBConfig returns the default LogDBConfig.
func GetDefaultLogDBConfig() LogDBConfig {
	return LogDBConfig{Shards: 16}
}

// GetTinyMemLogDBConfig returns a LogDBConfig suitable for tiny memory
// environments.
func GetTinyMemLogDBConfig() LogDBConfig {
	return LogDBConfig{Shards: 16}
}

// GetSmallMemLogDBConfig returns a LogDBConfig suitable for small memory
// environments.
func GetSmallMemLogDBConfig() LogDBConfig {
	return LogDBConfig{Shards: 16}
}

// GetMediumMemLogDBConfig returns a LogDBConfig suitable for medium
// memory environments.
func GetMediumMemLogDBConfig() LogDBConfig {
	return LogDBConfig{Shards: 16}
}

// GetLargeMemLogDBConfig returns a LogDBConfig suitable for large memory
// environments.
func GetLargeMemLogDBConfig() LogDBConfig {
	return LogDBConfig{Shards: 16}
}

// IsEmpty returns true when all EngineConfig worker shard counts are zero.
func (ec EngineConfig) IsEmpty() bool {
	return ec.ExecShards == 0 && ec.CommitShards == 0 &&
		ec.ApplyShards == 0 && ec.SnapshotShards == 0 &&
		ec.CloseShards == 0
}

// Validate checks the EngineConfig. Currently all configurations are valid.
func (ec EngineConfig) Validate() error {
	return nil
}

// GetDeploymentID returns the configured deployment ID.
func (c *NodeHostConfig) GetDeploymentID() uint64 {
	return c.DeploymentID
}

// GetListenAddress returns the listen address. When ListenAddress is
// empty, RaftAddress is returned as the default listen address.
func (c *NodeHostConfig) GetListenAddress() string {
	if c.ListenAddress != "" {
		return c.ListenAddress
	}
	return c.RaftAddress
}

// Prepare validates the NodeHostConfig and prepares it for use. This
// is an alias for Validate that matches dragonboat's API convention.
func (c *NodeHostConfig) Prepare() error {
	return c.Validate()
}

// NodeRegistryEnabled returns true when the default node registry is
// enabled for automatic node discovery.
func (c *NodeHostConfig) NodeRegistryEnabled() bool {
	return c.DefaultNodeRegistryEnabled
}

// Validate checks that the NodeHostConfig is valid. It performs basic
// sanity checks that would cause failures during conversion or startup.
func (c NodeHostConfig) Validate() error {
	if c.WALDir == "" {
		return &ValidationError{Field: "WALDir", Message: "must not be empty"}
	}
	if c.NodeHostDir == "" {
		return &ValidationError{Field: "NodeHostDir", Message: "must not be empty"}
	}
	if c.RaftAddress == "" {
		return &ValidationError{Field: "RaftAddress", Message: "must not be empty"}
	}
	if c.RTTMillisecond == 0 {
		return &ValidationError{Field: "RTTMillisecond", Message: "must be > 0"}
	}
	if c.MutualTLS {
		if c.CAFile == "" {
			return &ValidationError{Field: "CAFile", Message: "must not be empty when MutualTLS is true"}
		}
		if c.CertFile == "" {
			return &ValidationError{Field: "CertFile", Message: "must not be empty when MutualTLS is true"}
		}
		if c.KeyFile == "" {
			return &ValidationError{Field: "KeyFile", Message: "must not be empty when MutualTLS is true"}
		}
	}
	if err := c.Gossip.Validate(); err != nil {
		return err
	}
	return nil
}

// ToQuicraft converts the dragonboat-compatible NodeHostConfig to a
// quicraft HostConfig. When MutualTLS is true, the CA, certificate,
// and key files are read from disk and populated into the transport
// MTLSConfig. Returns an error if file reading fails.
func (c NodeHostConfig) ToQuicraft() (qcConfig.HostConfig, error) {
	hc := qcConfig.HostConfig{
		WALDir:         c.WALDir,
		NodeHostDir:    c.NodeHostDir,
		RaftAddress:    c.RaftAddress,
		ListenAddress:  c.ListenAddress,
		RTTMillisecond: c.RTTMillisecond,
		DeploymentID:   c.DeploymentID,
		NotifyCommit:   c.NotifyCommit,
	}

	// Map Expert.Engine fields to quicraft worker counts. ExecShards
	// maps to NumWorkers (general engine parallelism) and ApplyShards
	// maps to MaxApplyWorkers (per-shard apply concurrency).
	if c.Expert.Engine.ExecShards > 0 {
		hc.NumWorkers = c.Expert.Engine.ExecShards
	}
	if c.Expert.Engine.ApplyShards > 0 {
		hc.MaxApplyWorkers = int(c.Expert.Engine.ApplyShards)
	}

	// Wire event listeners if either is provided.
	if c.RaftEventListener != nil || c.SystemEventListener != nil {
		hc.EventListener = raftio.ConvertEventListeners(c.RaftEventListener, c.SystemEventListener)
	}

	// Set custom HostID if configured.
	if c.NodeHostID != "" {
		hc.HostID = c.NodeHostID
	}

	if c.MutualTLS {
		caCert, err := os.ReadFile(c.CAFile)
		if err != nil {
			return qcConfig.HostConfig{}, &FileReadError{Path: c.CAFile, Err: err}
		}
		cert, err := os.ReadFile(c.CertFile)
		if err != nil {
			return qcConfig.HostConfig{}, &FileReadError{Path: c.CertFile, Err: err}
		}
		key, err := os.ReadFile(c.KeyFile)
		if err != nil {
			return qcConfig.HostConfig{}, &FileReadError{Path: c.KeyFile, Err: err}
		}
		hc.TransportConfig.MTLSConfig = &qcConfig.MTLSConfig{
			CACert: caCert,
			Cert:   cert,
			Key:    key,
		}
	}

	return hc, nil
}
