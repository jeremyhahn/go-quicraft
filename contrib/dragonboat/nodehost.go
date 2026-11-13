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

package dragonboat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/internal/nalookup"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	qcLogDB "github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	qcSM "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Version Constants
// ---------------------------------------------------------------------------

const (
	// DragonboatMajor is the major version of the dragonboat API being adapted.
	DragonboatMajor = 4

	// DragonboatMinor is the minor version of the dragonboat API being adapted.
	DragonboatMinor = 0

	// DragonboatPatch is the patch version of the dragonboat API being adapted.
	DragonboatPatch = 0

	// DEVVersion indicates whether this is a development version.
	DEVVersion = true
)

// ---------------------------------------------------------------------------
// Default Option Values
// ---------------------------------------------------------------------------

// DefaultNodeHostInfoOption is the zero-value NodeHostInfoOption that
// includes all available information (log info not skipped).
var DefaultNodeHostInfoOption NodeHostInfoOption

// DefaultSnapshotOption is the zero-value SnapshotOption that uses
// shard-level defaults for all settings.
var DefaultSnapshotOption SnapshotOption

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// INodeUser provides direct access to a shard's replica for proposals
// and read index requests, bypassing the NodeHost routing layer.
type INodeUser interface {
	// ShardID returns the shard identifier this node user belongs to.
	ShardID() uint64

	// ReplicaID returns the replica identifier for this node user.
	ReplicaID() uint64

	// Propose submits a proposal through Raft consensus on the shard.
	Propose(s *dbClient.Session, cmd []byte, timeout time.Duration) (*RequestState, error)

	// ReadIndex requests a linearizable read index from the shard.
	ReadIndex(timeout time.Duration) (*RequestState, error)
}

// INodeHostRegistry provides access to the gossip-based node host registry.
// QuicRaft does not support gossip, so this interface exists solely for
// API compatibility with dragonboat v4.
type INodeHostRegistry interface {
	// NumOfShards returns the number of shards known to the registry.
	NumOfShards() int

	// GetMeta returns metadata associated with a node host identifier.
	GetMeta(nhID string) ([]byte, bool)

	// GetShardInfo returns shard information from the gossip registry.
	GetShardInfo(shardID uint64) (ShardView, bool)
}

// ShardView is a placeholder type for gossip-based shard information.
// QuicRaft does not support gossip, so this type exists solely for
// API compatibility with dragonboat v4.
type ShardView struct{}

// ---------------------------------------------------------------------------
// Gossip Info
// ---------------------------------------------------------------------------

// GossipInfo provides information about the gossip protocol status.
// QuicRaft does not support gossip, so all fields will be zero-valued.
type GossipInfo struct {
	// AdvertiseAddress is the address advertised to other nodes via gossip.
	AdvertiseAddress string

	// NumOfKnownNodeHosts is the number of node hosts known through gossip.
	NumOfKnownNodeHosts int

	// Enabled indicates whether gossip is enabled.
	Enabled bool
}

// removedKey uniquely identifies a removed shard/replica pair.
type removedKey struct {
	shardID   uint64
	replicaID uint64
}

// defaultQueryTimeout is the default deadline for local state machine
// queries in ReadLocalNode. Zero means no deadline, matching dragonboat,
// whose ReadLocalNode calls sm.Lookup without one. SetQueryTimeout is a
// quicraft extension that opts into a bound.
const defaultQueryTimeout time.Duration = 0

// defaultWaitReadyTimeout bounds the Start*Replica readiness wait used
// when Config.WaitReady is set. Dragonboat blocks on the node's
// initializedC until the node is initialized or stopped; the adapter
// polls and gives up after this timeout so a shard that never
// initializes cannot wedge the caller forever.
const defaultWaitReadyTimeout = 30 * time.Second

// waitReadyPollInterval is how often the WaitReady loop re-checks shard
// readiness.
const waitReadyPollInterval = 5 * time.Millisecond

// NodeHost wraps a quicraft Host with the dragonboat NodeHost API surface.
// It translates dragonboat types and conventions to quicraft equivalents,
// enabling drop-in migration from dragonboat to quicraft.
type NodeHost struct {
	host   *quicraft.Host
	config dbConfig.NodeHostConfig

	// removed tracks shard/replica pairs that have been permanently
	// removed via RemoveData or SyncRemoveData. Dragonboat rejects
	// attempts to restart a replica whose data was removed.
	//
	// The guard is backed by on-disk marker files under NodeHostDir
	// (see removedMarkerDir): markRemoved persists a marker and
	// NewNodeHost reloads the markers at startup, so the removal
	// decision survives process restarts, matching dragonboat's
	// persistent dir-deleted marker.
	//
	// This map grows monotonically and never evicts entries. This is
	// intentional and matches dragonboat's behavior: once a replica's
	// data is removed, that decision is permanent. The map is bounded
	// by the total number of unique (shardID, replicaID) pairs ever
	// removed, which in practice is small (typically fewer than the
	// total number of shards configured on this host).
	removed sync.Map // map[removedKey]struct{}

	// queryTimeout is the timeout for local state machine queries in
	// ReadLocalNode, stored as nanoseconds for atomic access. Defaults to
	// 0 (no deadline), matching dragonboat's ReadLocalNode, which calls
	// the state machine's Lookup with no deadline at all. Use
	// SetQueryTimeout to opt into a bound.
	queryTimeout atomic.Int64

	// shardConfigs retains the dragonboat Config each shard was started
	// with, keyed by shardID. OrderedConfigChange is only expressible in
	// the dragonboat Config, and quicraft's host-level staleness check
	// never consults it, so the adapter must remember it to reproduce
	// dragonboat's rule that ConfigChangeIndex is ignored unless
	// OrderedConfigChange is true.
	shardConfigs sync.Map // map[uint64]dbConfig.Config
}

// SnapshotOption controls per-request snapshot behavior. This mirrors
// dragonboat's SnapshotOption type and is converted to quicraft's
// SnapshotOption before delegation.
type SnapshotOption struct {
	// ExportPath, when non-empty, causes the snapshot to be written
	// to this directory as an exported snapshot.
	ExportPath string

	// CompactionOverhead overrides the shard-level compaction overhead
	// for this snapshot's log compaction. Only used when
	// OverrideCompactionOverhead is true.
	CompactionOverhead uint64

	// CompactionIndex specifies an exact log index for compaction.
	// Mutually exclusive with CompactionOverhead; setting both
	// causes Validate to return an error.
	//
	// NOTE: quicraft does not support CompactionIndex. Setting a
	// non-zero value causes Validate to return ErrInvalidOption.
	CompactionIndex uint64

	// OverrideCompactionOverhead controls whether CompactionOverhead
	// replaces the shard-level value.
	OverrideCompactionOverhead bool

	// Exported indicates whether this is an export operation. Set
	// automatically when ExportPath is non-empty.
	Exported bool
}

// Validate checks that the SnapshotOption fields are consistent and
// valid. CompactionOverhead and CompactionIndex are mutually exclusive:
// setting both is an error. Additionally, setting CompactionIndex when
// OverrideCompactionOverhead is true is always rejected because quicraft
// does not support index-based compaction.
func (o SnapshotOption) Validate() error {
	if o.CompactionOverhead > 0 && o.CompactionIndex > 0 {
		return &CompactionIndexNotSupportedError{}
	}
	if o.OverrideCompactionOverhead && o.CompactionIndex > 0 {
		return &CompactionIndexNotSupportedError{}
	}
	return nil
}

// checkExportPath reports ErrDirNotExist when an export was requested but
// its destination directory does not exist. Dragonboat performs the same
// check before starting the snapshot request.
func checkExportPath(o SnapshotOption) error {
	if o.ExportPath == "" {
		return nil
	}
	info, err := os.Stat(o.ExportPath)
	if err != nil || !info.IsDir() {
		return ErrDirNotExist
	}
	return nil
}

// toQuicraft converts a dragonboat SnapshotOption to quicraft's
// SnapshotOption type.
func (o SnapshotOption) toQuicraft() quicraft.SnapshotOption {
	return quicraft.SnapshotOption{
		ExportPath:                 o.ExportPath,
		CompactionOverhead:         o.CompactionOverhead,
		OverrideCompactionOverhead: o.OverrideCompactionOverhead,
		Exported:                   o.Exported,
	}
}

// NodeHostInfoOption controls what information GetNodeHostInfo returns.
type NodeHostInfoOption struct {
	// SkipLogInfo when true omits per-shard log entry information.
	SkipLogInfo bool
}

// NodeHostInfo provides a snapshot of the host's state for monitoring
// and debugging.
type NodeHostInfo struct {
	// NodeHostID is the unique identifier for this node host.
	NodeHostID string

	// RaftAddress is this host's Raft network address.
	RaftAddress string

	// Gossip contains gossip protocol status information.
	// Always zero-valued since QuicRaft does not support gossip.
	Gossip GossipInfo

	// ShardInfoList contains status information for each loaded shard.
	ShardInfoList []ShardInfo

	// LogInfo contains per-shard log node information. Nil when
	// NodeHostInfoOption.SkipLogInfo is true.
	LogInfo []LogNodeInfo
}

// LogNodeInfo identifies a shard/replica pair in the log database.
type LogNodeInfo struct {
	ShardID   uint64
	ReplicaID uint64
}

// ShardInfo provides a point-in-time snapshot of a shard's status,
// matching the dragonboat ShardInfo field set.
type ShardInfo struct {
	// ShardID is the unique identifier for this shard.
	ShardID uint64

	// ReplicaID is the local replica identifier for this shard.
	ReplicaID uint64

	// LeaderID is the current leader's replicaID. 0 if unknown.
	LeaderID uint64

	// Term is the current Raft term.
	Term uint64

	// IsLeader is true if this replica is the current leader.
	IsLeader bool

	// IsObserver is true if this replica is a non-voting observer.
	IsObserver bool

	// IsWitness is true if this replica is a voting witness.
	IsWitness bool

	// StateMachineType identifies the state machine variant using
	// dragonboat's numbering (Regular=1, Concurrent=2, OnDisk=3), as
	// defined by the statemachine package's Type constants. Quicraft's
	// internal codes (0/1/2) are translated during conversion.
	StateMachineType uint64

	// Pending reports that details of this shard node are not available
	// because the node has not had anything applied yet, matching
	// dragonboat's ShardInfo.Pending. It is NOT a count.
	Pending bool

	// PendingProposals is the number of proposals currently pending on
	// this replica. This is a quicraft extension; dragonboat has no
	// equivalent field.
	PendingProposals uint64

	// Nodes maps replicaID to network address for all members. Dragonboat
	// names the same field Replicas.
	Nodes map[uint64]string

	// ConfigChangeIndex is the log index of the most recently applied
	// membership configuration change.
	ConfigChangeIndex uint64
}

// ReadonlyLogReader provides read-only access to a shard's Raft log.
type ReadonlyLogReader = quicraft.ReadonlyLogReader

// NodeHostConstructionError is returned when NewNodeHost fails during
// host construction.
type NodeHostConstructionError struct {
	Reason string
	Err    error
}

// Error returns a human-readable description of the construction failure.
func (e *NodeHostConstructionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("dragonboat: failed to create NodeHost: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("dragonboat: failed to create NodeHost: %s", e.Reason)
}

// Unwrap returns the underlying error.
func (e *NodeHostConstructionError) Unwrap() error {
	return e.Err
}

// validateNodeHostConfig runs the dragonboat construction-time config
// checks, mirroring NewNodeHost in dragonboat, which calls Validate then
// Prepare before doing anything else. Skipping them would silently accept
// configurations that cannot work: an empty NodeHostDir resolves the
// removed-replica marker directory relative to the process working
// directory, and a gossip deployment would construct successfully and then
// never resolve its peers.
func validateNodeHostConfig(cfg *dbConfig.NodeHostConfig) error {
	if err := cfg.Validate(); err != nil {
		return &NodeHostConstructionError{Reason: "config validation", Err: err}
	}
	if err := cfg.Prepare(); err != nil {
		return &NodeHostConstructionError{Reason: "config preparation", Err: err}
	}
	return nil
}

// NewNodeHost creates a new NodeHost by converting the dragonboat-
// compatible configuration to quicraft's HostConfig, then constructing
// a quicraft Host with in-memory LogDB and no transport for test
// isolation, or full transport for production use.
//
// Like Dragonboat, the host stores its Raft log and snapshots UNENCRYPTED at
// rest (quicraft.WithoutEncryption). To encrypt, use NewNodeHostWithOptions
// with quicraft.WithBarrier.
func NewNodeHost(nhConfig dbConfig.NodeHostConfig) (*NodeHost, error) {
	if err := validateNodeHostConfig(&nhConfig); err != nil {
		return nil, err
	}

	qcCfg, err := nhConfig.ToQuicraft()
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "config conversion", Err: err}
	}

	// Dragonboat has no at-rest encryption, so the drop-in constructor keeps
	// Dragonboat's behaviour and declares it: the Raft log and snapshots are
	// stored as plaintext. NewNodeHostWithOptions is the way to encrypt, by
	// passing quicraft.WithBarrier.
	host, err := quicraft.NewHost(qcCfg, quicraft.WithoutEncryption())
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "host creation", Err: err}
	}

	// A port-0 RaftAddress is resolved to the bound port by the host.
	nhConfig.RaftAddress = host.RaftAddress()
	nh := &NodeHost{
		host:   host,
		config: nhConfig,
	}
	nh.queryTimeout.Store(int64(defaultQueryTimeout))
	if err := nh.loadRemovedMarkers(); err != nil {
		nh.Close()
		return nil, &NodeHostConstructionError{Reason: "removed-replica marker load", Err: err}
	}
	return nh, nil
}

// NewNodeHostWithOptions creates a new NodeHost with explicit quicraft
// HostOptions. This allows callers to specify custom LogDB backends,
// transport settings, and other advanced configuration.
//
// This method accepts QuicRaft-specific HostOption values, which is an
// extension beyond the standard Dragonboat v4 API. It provides access
// to QuicRaft-native capabilities such as WithMemoryLogDB() and
// WithoutTransport() that have no Dragonboat equivalent.
//
// opts must state the host's at-rest encryption: quicraft.WithBarrier (or
// WithShardBarriers) to encrypt, or quicraft.WithoutEncryption to store
// plaintext. quicraft.NewHost refuses opts that state neither, and the error
// is returned wrapped in a NodeHostConstructionError.
func NewNodeHostWithOptions(nhConfig dbConfig.NodeHostConfig, opts ...quicraft.HostOption) (*NodeHost, error) {
	if err := validateNodeHostConfig(&nhConfig); err != nil {
		return nil, err
	}

	qcCfg, err := nhConfig.ToQuicraft()
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "config conversion", Err: err}
	}

	host, err := quicraft.NewHost(qcCfg, opts...)
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "host creation", Err: err}
	}

	// A port-0 RaftAddress is resolved to the bound port by the host.
	nhConfig.RaftAddress = host.RaftAddress()
	nh := &NodeHost{
		host:   host,
		config: nhConfig,
	}
	nh.queryTimeout.Store(int64(defaultQueryTimeout))
	if err := nh.loadRemovedMarkers(); err != nil {
		nh.Close()
		return nil, &NodeHostConstructionError{Reason: "removed-replica marker load", Err: err}
	}
	return nh, nil
}

// checkContextDeadline mirrors the validation in dragonboat's
// getTimeoutFromContext: the context must carry a deadline and that deadline
// must not already have passed. Checking only for the deadline's presence, as
// the adapter used to, accepted an already-expired context and let the
// operation run to a zero-length timeout instead of reporting the caller's
// bug. Dragonboat also returns the remaining time; quicraft takes the context
// itself, so only the validation is needed.
func checkContextDeadline(ctx context.Context) error {
	d, ok := ctx.Deadline()
	if !ok {
		return ErrDeadlineNotSet
	}
	if !time.Now().Before(d) {
		return ErrInvalidDeadline
	}
	return nil
}

// checkTimeout reports ErrTimeoutTooSmall when the requested timeout is not
// positive or is shorter than a single RTT. Dragonboat converts a timeout to
// ticks with getTimeoutTick (whole milliseconds divided by the RTT) and
// rejects a zero tick count in its propose, read-index, snapshot and
// config-change requests; this is that check. Unlike getTimeoutTick it
// rejects a negative timeout instead of letting it wrap through uint64 into
// an enormous tick count. The comparison is in whole milliseconds, so it
// cannot overflow for any RTT.
func (nh *NodeHost) checkTimeout(timeout time.Duration) error {
	if timeout <= 0 || uint64(timeout.Milliseconds()) < nh.config.RTTMillisecond {
		return ErrTimeoutTooSmall
	}
	return nil
}

// SetQueryTimeout sets the deadline used for local state machine queries
// in ReadLocalNode. This is a quicraft extension: dragonboat has no such
// bound and calls the state machine's Lookup with no deadline, which is
// also this adapter's default. A zero or negative duration restores that
// default. This method is safe for concurrent use.
func (nh *NodeHost) SetQueryTimeout(d time.Duration) {
	if d < 0 {
		d = 0
	}
	nh.queryTimeout.Store(int64(d))
}

// QueryTimeout returns the current deadline for local state machine
// queries, or 0 when queries run without one. Safe for concurrent use.
func (nh *NodeHost) QueryTimeout() time.Duration {
	return time.Duration(nh.queryTimeout.Load())
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Close stops the NodeHost and releases all resources. Dragonboat's Close
// returns nothing; this adapter logs a warning if the underlying quicraft
// host returns an error.
func (nh *NodeHost) Close() {
	if err := nh.host.Close(); err != nil {
		slog.Warn("dragonboat adapter: error closing quicraft host",
			"error", err)
	}
}

// NodeHostConfig returns the dragonboat-compatible configuration used to
// create this NodeHost.
func (nh *NodeHost) NodeHostConfig() dbConfig.NodeHostConfig {
	return nh.config
}

// RaftAddress returns this host's Raft network address.
func (nh *NodeHost) RaftAddress() string {
	return nh.host.RaftAddress()
}

// ID returns the unique host identifier.
func (nh *NodeHost) ID() string {
	return nh.host.ID()
}

// ---------------------------------------------------------------------------
// Replica Management
// ---------------------------------------------------------------------------

// validateStartReplica performs the shared pre-start checks for the three
// Start*Replica methods: the shard configuration must be valid, a joining
// replica must not carry initial members (dragonboat returns
// ErrInvalidShardSettings for that combination), and the replica must not
// have been permanently removed.
func (nh *NodeHost) validateStartReplica(initialMembers map[uint64]Target, join bool, cfg dbConfig.Config) error {
	if err := cfg.Validate(); err != nil {
		return &ShardSettingsError{Err: err}
	}
	// Dragonboat's startShard validates every initial member target before
	// touching the LogDB, so a typo'd address fails fast instead of
	// bootstrapping a shard that can never reach its peers.
	for _, target := range initialMembers {
		if !dbConfig.IsValidAddress(target) {
			return ErrInvalidTarget
		}
	}
	if join && len(initialMembers) > 0 {
		return ErrInvalidShardSettings
	}
	if nh.isRemoved(cfg.ShardID, cfg.ReplicaID) {
		return ErrReplicaRemoved
	}
	return nil
}

// startReplica is the shared body of the three Start*Replica methods. It
// validates, starts the shard, retains the dragonboat Config for later
// OrderedConfigChange decisions, and honors Config.WaitReady.
func (nh *NodeHost) startReplica(initialMembers map[uint64]Target, join bool, create qcSM.CreateFunc, cfg dbConfig.Config) error {
	if err := nh.validateStartReplica(initialMembers, join, cfg); err != nil {
		return err
	}
	if err := translateError(nh.host.StartShard(initialMembers, join, create, cfg.ToQuicraft())); err != nil {
		return err
	}
	nh.shardConfigs.Store(cfg.ShardID, cfg)
	if cfg.WaitReady {
		nh.waitShardReady(cfg.ShardID)
	}
	return nil
}

// waitShardReady polls until the shard reports a known leader or the
// bounded wait elapses. Dragonboat's startShard blocks on the node's
// initializedC when Config.WaitReady is set; quicraft's StartShard returns
// as soon as the node is registered, so bootstrap code that proposes right
// after Start*Replica would otherwise fail as not ready.
func (nh *NodeHost) waitShardReady(shardID uint64) {
	deadline := time.NewTimer(defaultWaitReadyTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(waitReadyPollInterval)
	defer ticker.Stop()
	for {
		if _, _, valid, err := nh.host.LeaderIDAndTerm(shardID); err == nil && valid {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			slog.Warn("dragonboat adapter: WaitReady timed out before the shard became ready",
				"shard_id", shardID, "timeout", defaultWaitReadyTimeout)
			return
		}
	}
}

// StartReplica starts a regular state machine replica on this host.
// The initialMembers map is only used when bootstrapping a new shard.
func (nh *NodeHost) StartReplica(initialMembers map[uint64]Target, join bool, create dbSM.CreateStateMachineFunc, cfg dbConfig.Config) error {
	return nh.startReplica(initialMembers, join, dbSM.WrapCreateFunc(create), cfg)
}

// StartConcurrentReplica starts a concurrent state machine replica on
// this host. Concurrent state machines support batched updates.
func (nh *NodeHost) StartConcurrentReplica(initialMembers map[uint64]Target, join bool, create dbSM.CreateConcurrentStateMachineFunc, cfg dbConfig.Config) error {
	return nh.startReplica(initialMembers, join, dbSM.WrapConcurrentCreateFunc(create), cfg)
}

// StartOnDiskReplica starts an on-disk state machine replica on this host.
// On-disk state machines persist their state externally and only use
// snapshots for recovery.
func (nh *NodeHost) StartOnDiskReplica(initialMembers map[uint64]Target, join bool, create dbSM.CreateOnDiskStateMachineFunc, cfg dbConfig.Config) error {
	return nh.startReplica(initialMembers, join, dbSM.WrapOnDiskCreateFunc(create), cfg)
}

// orderedConfigChange reports whether the shard was started with
// OrderedConfigChange enabled. Unknown shards default to false, matching
// dragonboat's zero-value Config.
func (nh *NodeHost) orderedConfigChange(shardID uint64) bool {
	v, ok := nh.shardConfigs.Load(shardID)
	if !ok {
		return false
	}
	cfg, ok := v.(dbConfig.Config)
	return ok && cfg.OrderedConfigChange
}

// effectiveConfigChangeIndex returns the configChangeIndex to forward to
// quicraft. Dragonboat's Config.OrderedConfigChange documents that
// "when set to false (default), ConfigChangeIndex is ignored for
// membership change requests" (internal/rsm.membership.isUpToDate), while
// quicraft's host-level staleness check rejects any mismatching non-zero
// value regardless. Zeroing the index restores dragonboat's behavior.
func (nh *NodeHost) effectiveConfigChangeIndex(shardID, configChangeIndex uint64) uint64 {
	if !nh.orderedConfigChange(shardID) {
		return 0
	}
	return configChangeIndex
}

// removedMarkerDirName is the directory under NodeHostDir that holds one
// marker file per permanently removed shard/replica pair. The markers make
// the RemoveData guard survive process restarts.
const removedMarkerDirName = "dragonboat-removed"

// removedMarkerDir returns the directory holding removed-replica markers.
func (nh *NodeHost) removedMarkerDir() string {
	return filepath.Join(nh.config.NodeHostDir, removedMarkerDirName)
}

// removedMarkerPath returns the marker file path for a shard/replica pair.
func (nh *NodeHost) removedMarkerPath(shardID, replicaID uint64) string {
	name := "shard-" + strconv.FormatUint(shardID, 10) +
		"-replica-" + strconv.FormatUint(replicaID, 10)
	return filepath.Join(nh.removedMarkerDir(), name)
}

// parseRemovedMarkerName parses a marker file name produced by
// removedMarkerPath back into its shard/replica pair. Returns false for
// names that do not match the marker format.
func parseRemovedMarkerName(name string) (shardID, replicaID uint64, ok bool) {
	rest, found := strings.CutPrefix(name, "shard-")
	if !found {
		return 0, 0, false
	}
	shardStr, replicaStr, found := strings.Cut(rest, "-replica-")
	if !found {
		return 0, 0, false
	}
	shardID, err := strconv.ParseUint(shardStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	replicaID, err = strconv.ParseUint(replicaStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return shardID, replicaID, true
}

// loadRemovedMarkers populates the in-memory removed map from the on-disk
// marker files. Called once during NodeHost construction so that a replica
// removed in a previous process lifetime stays rejected after restart.
func (nh *NodeHost) loadRemovedMarkers() error {
	dir := nh.removedMarkerDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return &RemovalMarkerError{Op: "load", Path: dir, Err: err}
	}
	for _, e := range entries {
		if shardID, replicaID, ok := parseRemovedMarkerName(e.Name()); ok {
			nh.removed.Store(removedKey{shardID, replicaID}, struct{}{})
		}
	}
	return nil
}

// isRemoved checks whether a shard/replica pair was permanently removed.
func (nh *NodeHost) isRemoved(shardID, replicaID uint64) bool {
	_, removed := nh.removed.Load(removedKey{shardID, replicaID})
	return removed
}

// markRemoved records a shard/replica pair as permanently removed, both
// in memory and as a durable marker file so the guard survives restarts.
// The in-memory record is stored first so the in-process guard holds even
// when marker persistence fails; the persistence error is still returned
// so callers learn the guard is not yet durable.
func (nh *NodeHost) markRemoved(shardID, replicaID uint64) error {
	nh.removed.Store(removedKey{shardID, replicaID}, struct{}{})
	return writeRemovalMarker(fsutil.SyncDir, nh.removedMarkerDir(), nh.removedMarkerPath(shardID, replicaID))
}

// writeRemovalMarker durably creates the marker file at path inside the
// marker directory dir, fsyncing directories through syncDir. Every link of
// the chain is made durable: dir's parent (NodeHostDir) so dir's own entry
// survives a crash, the marker file, and then dir so the marker's entry
// survives. The parent is synced even when dir already existed, because a
// previous attempt may have created dir and failed before syncing it.
//
// dir is created with Mkdir, not MkdirAll: quicraft.NewHost has already
// created NodeHostDir and synced its chain, so dir is the only directory
// this function may create. A missing NodeHostDir is an error, not something
// to recreate as a chain of directories nothing would sync.
func writeRemovalMarker(syncDir func(string) error, dir, path string) error {
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return &RemovalMarkerError{Op: "mkdir", Path: dir, Err: err}
	}
	parent := filepath.Dir(dir)
	if err := syncDir(parent); err != nil {
		return &RemovalMarkerError{Op: "sync", Path: parent, Err: err}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return &RemovalMarkerError{Op: "create", Path: path, Err: err}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return &RemovalMarkerError{Op: "sync", Path: path, Err: err}
	}
	if err := f.Close(); err != nil {
		return &RemovalMarkerError{Op: "close", Path: path, Err: err}
	}
	if err := syncDir(dir); err != nil {
		return &RemovalMarkerError{Op: "sync", Path: dir, Err: err}
	}
	return nil
}

// StopShard stops the shard identified by shardID on this host.
func (nh *NodeHost) StopShard(shardID uint64) error {
	return translateError(nh.host.StopShard(shardID))
}

// StopReplica stops the specific replica on the given shard.
func (nh *NodeHost) StopReplica(shardID, replicaID uint64) error {
	return translateError(nh.host.StopReplica(shardID, replicaID))
}

// ---------------------------------------------------------------------------
// Synchronous Data Access
// ---------------------------------------------------------------------------

// SyncPropose makes a synchronous proposal through Raft consensus.
// For no-op sessions, the simpler SyncPropose path is used. For tracked
// sessions, SyncProposeWithSession handles at-most-once semantics.
func (nh *NodeHost) SyncPropose(ctx context.Context, session *dbClient.Session, cmd []byte) (dbSM.Result, error) {
	if err := checkContextDeadline(ctx); err != nil {
		return dbSM.Result{}, err
	}
	if err := nh.requireNoOPSessionOnDisk(session); err != nil {
		return dbSM.Result{}, err
	}
	if session.IsNoOPSession() {
		result, err := nh.host.SyncPropose(ctx, session.ShardID, cmd)
		if err != nil {
			return dbSM.Result{}, translateError(err)
		}
		return dbSM.Result{Value: result.Value, Data: result.Data}, nil
	}

	// Use ProposeWithSession (not SyncProposeWithSession) so that
	// ProposalCompleted is NOT called automatically. Dragonboat's API
	// contract requires the caller to advance the session by calling
	// session.ProposalCompleted() after receiving a successful result.
	// Auto-advancing would break duplicate detection: the session's
	// series ID would already be incremented before the duplicate
	// proposal is re-submitted, bypassing at-most-once enforcement.
	//
	// Release runs before the caller's ProposalCompleted, and Release ends
	// the session's in-flight reservation (quicraft.ErrSessionBusy) — the
	// adapter cannot hold it open across the return without breaking the
	// contract above. The reservation therefore covers only the window in
	// which this call is in flight; a second SyncPropose on the same session
	// issued between this return and ProposalCompleted carries the same
	// series and is answered as a retransmission, which is dragonboat's own
	// rule for a session used before it was advanced.
	rs, err := nh.host.ProposeWithSession(ctx, session.Inner(), cmd)
	if err != nil {
		return dbSM.Result{}, translateError(err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return dbSM.Result{}, translateError(result.Err)
		}
		return dbSM.Result{Value: result.Value, Data: result.Data}, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return dbSM.Result{}, ErrTimeout
		}
		return dbSM.Result{}, ErrCanceled
	}
}

// SyncRead performs a linearizable read via ReadIndex + Lookup.
func (nh *NodeHost) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	if err := checkContextDeadline(ctx); err != nil {
		return nil, err
	}
	result, err := nh.host.SyncRead(ctx, shardID, query)
	if err != nil {
		return nil, translateError(err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Client Sessions
// ---------------------------------------------------------------------------

// GetNoOPSession returns a no-op session for idempotent proposals that
// do not require at-most-once delivery guarantees.
func (nh *NodeHost) GetNoOPSession(shardID uint64) *dbClient.Session {
	return dbClient.NewNoOPSession(shardID, nil)
}

// SyncGetSession registers a new client session through Raft consensus
// and returns it. The session provides at-most-once proposal semantics.
// Returns ErrRejected for shards backed by an on-disk state machine, which
// do not support tracked sessions: they manage their own persistence and
// do not participate in the session tracking machinery (dragonboat
// semantics: ErrRejected). The comparison below is against the UNCONVERTED
// quicraft ShardInfo, so it must use quicraft's numbering
// (proto.OnDiskStateMachine == 2), not the dragonboat-numbered
// statemachine.OnDiskStateMachine (== 3).
func (nh *NodeHost) SyncGetSession(ctx context.Context, shardID uint64) (*dbClient.Session, error) {
	if err := checkContextDeadline(ctx); err != nil {
		return nil, err
	}
	info, infoErr := nh.host.ShardInfo(shardID)
	if infoErr == nil && info.StateMachineType == proto.OnDiskStateMachine {
		return nil, ErrRejected
	}
	session, err := nh.host.NewSession(ctx, shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return dbClient.WrapSession(session), nil
}

// SyncCloseSession closes a client session by proposing unregistration
// through Raft consensus. Blocks until the session is closed or the
// context is canceled.
func (nh *NodeHost) SyncCloseSession(ctx context.Context, cs *dbClient.Session) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}

	rs, err := nh.host.CloseSession(ctx, cs.ShardID, cs.Inner())
	if err != nil {
		return translateError(err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return translateError(result.Err)
		}
		return validateCloseSessionResult(result.Value, cs.ClientID())
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	}
}

// requireNoOPSessionOnDisk enforces dragonboat's rule that replicas backed
// by an IOnDiskStateMachine must use a NoOP session: they manage their own
// persistence and do not participate in session tracking. Dragonboat
// panics with "IOnDiskStateMachine based nodes must use NoOPSession"; a
// library must not panic on caller input, so the adapter reports
// ErrRejected, the same outcome dragonboat gives for a session a shard
// will not honor.
//
// The comparison is against the UNCONVERTED quicraft ShardInfo, so it uses
// quicraft's numbering (proto.OnDiskStateMachine == 2), not the
// dragonboat-numbered statemachine.OnDiskStateMachine (== 3).
//
// A failed shard lookup is returned, translated: without the shard's state
// machine type the rule cannot be checked, and it is the same error the
// caller's proposal would have met, reported before any work starts, as
// dragonboat's getShard check does.
func (nh *NodeHost) requireNoOPSessionOnDisk(session *dbClient.Session) error {
	if session == nil || session.IsNoOPSession() {
		return nil
	}
	info, err := nh.host.ShardInfo(session.ShardID)
	if err != nil {
		return translateError(err)
	}
	if info.StateMachineType == proto.OnDiskStateMachine {
		return ErrRejected
	}
	return nil
}

// validateCloseSessionResult verifies that the result value from a session
// close proposal matches the expected client ID. Dragonboat v4 performs
// this check to detect protocol violations where the wrong session was closed.
func validateCloseSessionResult(resultValue, clientID uint64) error {
	if resultValue != clientID {
		return ErrClientIDMismatch
	}
	return nil
}

// ---------------------------------------------------------------------------
// Asynchronous Operations
// ---------------------------------------------------------------------------

// Propose submits an asynchronous proposal. Returns a RequestState that
// the caller polls for the result.
//
// The context is created with a timeout derived from the timeout parameter.
// The context is NOT canceled when this method returns because the async
// operation may still be in flight. Instead, the cancel function is attached
// to the returned RequestState and invoked when Release() is called.
func (nh *NodeHost) Propose(session *dbClient.Session, cmd []byte, timeout time.Duration) (*RequestState, error) {
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	if err := nh.requireNoOPSessionOnDisk(session); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	if session.IsNoOPSession() {
		rs, err := nh.host.Propose(ctx, session.ShardID, cmd)
		if err != nil {
			cancel()
			return nil, translateError(err)
		}
		return wrapRequestState(rs, session.ShardID, cancel, nh.config.NotifyCommit), nil
	}

	rs, err := nh.host.ProposeWithSession(ctx, session.Inner(), cmd)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, session.ShardID, cancel, nh.config.NotifyCommit), nil
}

// ProposeSession submits a session proposal (registration or
// unregistration) asynchronously. Returns a RequestState that the caller
// polls for the result. In dragonboat, this is used to register or
// unregister a client session via Raft consensus.
//
// The session's SeriesID determines the operation:
//   - SeriesIDForRegister   → register a new session
//   - SeriesIDForUnregister → close an existing session
//
// Any other series returns ErrInvalidSession, matching dragonboat's
// ValidForSessionOp check: a live in-use session must never be passed
// here, or it would be silently unregistered. Quicraft's own
// Session.ValidForSessionOp does not require the sentinel, so the shim
// enforces the strict dragonboat rule itself.
//
// The register path returns without waiting for consensus: quicraft has
// no asynchronous register primitive (Host.RegisterSession is
// synchronous), so it runs in its own goroutine and its outcome is
// delivered through the returned RequestState. Quicraft's
// RegisterSession prepares the session for normal proposals internally
// on success; the caller's subsequent PrepareForPropose (per the
// dragonboat pattern) is an idempotent no-op, so the series never
// regresses.
func (nh *NodeHost) ProposeSession(session *dbClient.Session, timeout time.Duration) (*RequestState, error) {
	if !session.ValidForSessionOp(session.ShardID) {
		return nil, ErrInvalidSession
	}

	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}

	// Surface shard lookup failures synchronously, matching dragonboat's
	// getShard check before the proposal is started.
	if _, err := nh.host.ShardInfo(session.ShardID); err != nil {
		return nil, translateError(err)
	}
	if err := nh.requireNoOPSessionOnDisk(session); err != nil {
		return nil, err
	}

	inner := session.Inner()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	if inner.SeriesID() == quicraft.SeriesIDForRegister {
		// Register the caller's session through Raft, preserving its
		// original client ID. RegisterSession honors ctx, which Release
		// cancels, so the goroutine cannot outlive the caller's interest.
		return newAsyncOpRequestState(cancel, func() RequestResult {
			registered, err := nh.host.RegisterSession(ctx, inner)
			if err != nil {
				return translateResult(quicraft.RequestResult{Err: err})
			}
			// Atomically swap the inner quicraft session pointer so
			// concurrent readers never observe a partially written
			// struct. ShardID is invariant (same shard), so only the
			// inner pointer needs updating.
			session.SwapInner(registered)
			return RequestResult{
				inner: quicraft.RequestResult{Value: session.ClientID()},
				code:  RequestCompleted,
			}
		}), nil
	}

	// Close/unregister the session.
	rs, err := nh.host.CloseSession(ctx, session.ShardID, inner)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, session.ShardID, cancel, nh.config.NotifyCommit), nil
}

// ReadIndex requests a linearizable read index. The returned RequestState
// resolves when the local state machine has caught up to the confirmed
// read index. The shardID is stored on the wrapper for ReadLocalNode.
func (nh *NodeHost) ReadIndex(shardID uint64, timeout time.Duration) (*RequestState, error) {
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.ReadIndex(ctx, shardID)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, shardID, cancel, nh.config.NotifyCommit), nil
}

// ReadLocalNode queries the local state machine after a ReadIndex has
// completed. Per the dragonboat contract, it is only allowed to be called
// after the caller has received a RequestCompleted notification from the
// RequestState returned by ReadIndex:
//
//	rs, _ := nh.ReadIndex(shardID, timeout)
//	defer rs.Release()
//	s := <-rs.ResultC()
//	if s.Completed() {
//	    val, err := nh.ReadLocalNode(rs, query)
//	}
//
// The completion is latched on the RequestState when it is delivered, so
// this method never receives from ResultC itself — the caller has already
// drained the single buffered value. Calling ReadLocalNode before the
// ReadIndex has completed returns ErrInvalidOperation; calling it after a
// failed ReadIndex returns that failure's error.
func (nh *NodeHost) ReadLocalNode(rs *RequestState, query interface{}) (interface{}, error) {
	return nh.readLocalNode(rs, query)
}

// NAReadLocalNode is the byte-slice variant of ReadLocalNode, as in
// dragonboat v4: the query is passed to the state machine's
// statemachine.IExtended NALookup method and its result is returned as is.
// It returns statemachine.ErrNotImplemented when the state machine does not
// implement IExtended. Like ReadLocalNode, it may only be called after the
// ReadIndex that produced rs has completed, and is served under the same
// read guard and query timeout.
func (nh *NodeHost) NAReadLocalNode(rs *RequestState, query []byte) ([]byte, error) {
	val, err := nh.readLocalNode(rs, nalookup.Query{Key: query})
	if err != nil {
		if errors.Is(err, dbSM.ErrNotImplemented) {
			return nil, dbSM.ErrNotImplemented
		}
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	b, ok := val.([]byte)
	if !ok {
		// The adapters answer a nalookup.Query with NALookup's []byte
		// result only; anything else is a broken invariant, not data.
		return nil, ErrInvalidOperation
	}
	return b, nil
}

// readLocalNode is the local read shared by ReadLocalNode and
// NAReadLocalNode.
func (nh *NodeHost) readLocalNode(rs *RequestState, query interface{}) (interface{}, error) {
	result, ok := rs.completedResult()
	if !ok {
		// The ReadIndex has not resolved (or the caller never waited
		// for it). Serving the query anyway would violate
		// linearizability.
		return nil, ErrInvalidOperation
	}
	if !result.Completed() {
		return nil, translateError(codeToError(result.code))
	}

	// The wrapper carries the shardID from the original ReadIndex call.
	shardID := rs.ShardID()
	// Dragonboat's ReadLocalNode calls sm.Lookup with no deadline at all,
	// so the adapter imposes one only when SetQueryTimeout opted in.
	d := nh.QueryTimeout()
	if d <= 0 {
		val, err := nh.host.QueryLocalNode(context.Background(), shardID, query)
		if err != nil {
			return nil, translateError(err)
		}
		return val, nil
	}
	// The dragonboat Lookup and NALookup take no context, so the deadline
	// cannot reach the user's state machine: run the query in its own
	// goroutine and stop waiting for it when the deadline passes. A lookup
	// that outlives the deadline finishes in the background and its result
	// is discarded.
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	type lookupResult struct {
		val interface{}
		err error
	}
	done := make(chan lookupResult, 1)
	go func() {
		val, err := nh.host.QueryLocalNode(ctx, shardID, query)
		done <- lookupResult{val: val, err: err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return nil, translateError(r.err)
		}
		return r.val, nil
	case <-ctx.Done():
		return nil, translateError(ctx.Err())
	}
}

// StaleRead reads local state machine state directly without any
// consensus check. The result may be arbitrarily stale.
func (nh *NodeHost) StaleRead(shardID uint64, query interface{}) (interface{}, error) {
	ctx := context.Background()
	result, err := nh.host.StaleRead(ctx, shardID, query)
	if err != nil {
		return nil, translateError(err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Membership Management
// ---------------------------------------------------------------------------

// SyncGetShardMembership returns the committed shard membership after
// confirming leadership via ReadIndex.
func (nh *NodeHost) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error) {
	if err := checkContextDeadline(ctx); err != nil {
		return nil, err
	}
	m, err := nh.host.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return fromQuicraftMembership(m), nil
}

// GetShardInfo returns information about the specified shard. The bool
// return value is true when the shard is found, false when it is not
// loaded on this host; a shard that is not loaded also returns a nil
// ShardInfo and ErrShardNotFound. This matches dragonboat's three-value
// return signature for shard info queries.
func (nh *NodeHost) GetShardInfo(shardID uint64) (*ShardInfo, bool, error) {
	info, err := nh.host.ShardInfo(shardID)
	if err != nil {
		return nil, false, translateError(err)
	}
	if info == nil {
		return nil, false, nil
	}
	adapted := fromQuicraftShardInfo(*info)
	return &adapted, true, nil
}

// GetShardMembership returns the shard membership without a linearizable
// consensus check. Unlike SyncGetShardMembership, this reads committed
// membership directly from the local replica's Raft peer state and does
// not perform a ReadIndex round-trip.
func (nh *NodeHost) GetShardMembership(shardID uint64) (*Membership, error) {
	m, err := nh.host.ShardMembership(shardID)
	if err != nil {
		// quicraft answers a shard with no engine node as not ready;
		// dragonboat reports a shard this host does not run as
		// ErrShardNotFound.
		if _, infoErr := nh.host.ShardInfo(shardID); errors.Is(infoErr, quicraft.ErrShardNotFound) {
			return nil, ErrShardNotFound
		}
		return nil, translateError(err)
	}
	return fromQuicraftMembership(m), nil
}

// GetLeaderID returns the leader's replicaID, the current term, whether
// a leader is known, and any error. This matches dragonboat's four-value
// return signature, which Quicraft's Host.LeaderIDAndTerm returns as is
// (Host.LeaderID returns only leaderID, valid and error).
func (nh *NodeHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error) {
	leaderID, term, valid, err := nh.host.LeaderIDAndTerm(shardID)
	if err != nil {
		return 0, 0, false, translateError(err)
	}
	return leaderID, term, valid, nil
}

// SyncRequestAddReplica adds a voting replica to the shard and blocks
// until the membership change is committed.
func (nh *NodeHost) SyncRequestAddReplica(ctx context.Context, shardID, replicaID uint64, target Target, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddNode(ctx, shardID, replicaID, target, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// SyncRequestAddReplicaWithIdentity is SyncRequestAddReplica carrying the node
// identity quicraft commits for the new replica (its NodeHost.NodeIdentity).
// It has no dragonboat counterpart: a NodeHost with a transport refuses the
// identity-less form with quicraft.ErrMemberIdentityRequired, because
// quicraft authorizes a member by its identity and by nothing else.
func (nh *NodeHost) SyncRequestAddReplicaWithIdentity(ctx context.Context, shardID, replicaID uint64, target Target, identity string, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID, target, identity, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// SyncRequestAddNonVotingWithIdentity is SyncRequestAddNonVoting carrying the
// node identity; see SyncRequestAddReplicaWithIdentity.
func (nh *NodeHost) SyncRequestAddNonVotingWithIdentity(ctx context.Context, shardID, replicaID uint64, target Target, identity string, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddObserverWithIdentity(ctx, shardID, replicaID, target, identity, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// SyncRequestAddWitnessWithIdentity is SyncRequestAddWitness carrying the node
// identity; see SyncRequestAddReplicaWithIdentity.
func (nh *NodeHost) SyncRequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, target Target, identity string, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddWitnessWithIdentity(ctx, shardID, replicaID, target, identity, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// NodeIdentity returns this NodeHost's stable node identity — the value
// peers must have committed for it (Config.MemberIdentities at bootstrap, the
// *WithIdentity adds afterwards) before they count its messages. It is empty
// for a NodeHost started WithoutTransport.
func (nh *NodeHost) NodeIdentity() string {
	return nh.host.NodeIdentity()
}

// ReloadMutualTLS re-reads NodeHostConfig's CAFile, CertFile and KeyFile and
// makes them the material every new handshake uses, without a restart. It is
// quicraft's Host.ReloadMTLS over the configured files: the new material is
// validated as at start, must keep this NodeHost's NodeIdentity (re-issue on
// the same keypair), and is swapped only when every check passes. A file
// that cannot be read returns a *config.FileReadError; refused material
// returns a *quicraft.MTLSReloadError. Either way the previous material stays
// in force. Established connections keep their session until they
// reconnect.
func (nh *NodeHost) ReloadMutualTLS() error {
	if !nh.config.MutualTLS {
		return nh.host.ReloadMTLS(nil)
	}
	mtls, err := nh.config.ReadMutualTLS()
	if err != nil {
		return err
	}
	return nh.host.ReloadMTLS(mtls)
}

// SyncRequestAddNonVoting adds a non-voting observer replica and blocks
// until the membership change is committed.
func (nh *NodeHost) SyncRequestAddNonVoting(ctx context.Context, shardID, replicaID uint64, target Target, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddObserver(ctx, shardID, replicaID, target, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// SyncRequestAddWitness adds a voting witness replica and blocks until
// the membership change is committed.
func (nh *NodeHost) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, target Target, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddWitness(ctx, shardID, replicaID, target, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// SyncRequestDeleteReplica removes a replica from the shard and blocks
// until the membership change is committed.
func (nh *NodeHost) SyncRequestDeleteReplica(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error {
	if err := checkContextDeadline(ctx); err != nil {
		return err
	}
	return translateError(nh.host.SyncRequestRemoveNode(ctx, shardID, replicaID, nh.effectiveConfigChangeIndex(shardID, configChangeIndex)))
}

// RequestLeaderTransfer requests leadership transfer to the specified
// target replica. targetReplicaID must name a voting member: 0, an
// observer, a witness and a removed replica are all rejected with
// ErrInvalidTarget, as they are by dragonboat. There is no guarantee the
// transfer completes.
func (nh *NodeHost) RequestLeaderTransfer(shardID, targetReplicaID uint64) error {
	ctx := context.Background()
	return translateError(nh.host.RequestLeaderTransfer(ctx, shardID, targetReplicaID))
}

// ---------------------------------------------------------------------------
// Async Membership Operations
// ---------------------------------------------------------------------------

// RequestAddReplica requests adding a voting replica asynchronously. Unlike upstream
// dragonboat, quicraft pre-validates membership changes: a change that is
// invalid against the current membership (e.g. removing the last full
// replica, or a witness policy violation) is rejected synchronously with a
// nil RequestState and a non-nil error, instead of failing later through
// RequestState.ResultC.
// Returns a RequestState that resolves when the config change is committed.
//
// It commits no node identity for the replica, which a NodeHost with a
// transport refuses (quicraft.ErrMemberIdentityRequired); use
// RequestAddReplicaWithIdentity there. See NodeHost.NodeIdentity.
func (nh *NodeHost) RequestAddReplica(shardID, replicaID uint64, target Target, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	return nh.RequestAddReplicaWithIdentity(shardID, replicaID, target, "", configChangeIndex, timeout)
}

// RequestAddReplicaWithIdentity is RequestAddReplica with the node identity the
// shard should commit for the replica — the value that node reports from
// NodeHost.NodeIdentity. See quicraft.Host.RequestAddNodeWithIdentity.
func (nh *NodeHost) RequestAddReplicaWithIdentity(shardID, replicaID uint64, target Target, identity string, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if !dbConfig.IsValidAddress(target) {
		return nil, ErrInvalidAddress
	}
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestAddNodeWithIdentity(ctx, shardID, replicaID, target, identity, nh.effectiveConfigChangeIndex(shardID, configChangeIndex))
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, shardID, cancel, nh.config.NotifyCommit), nil
}

// RequestAddNonVoting requests adding a non-voting observer asynchronously.
// It commits no node identity, which a NodeHost with a transport refuses; use
// RequestAddNonVotingWithIdentity there.
func (nh *NodeHost) RequestAddNonVoting(shardID, replicaID uint64, target Target, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	return nh.RequestAddNonVotingWithIdentity(shardID, replicaID, target, "", configChangeIndex, timeout)
}

// RequestAddNonVotingWithIdentity is RequestAddNonVoting with the node identity
// the shard should commit for the observer.
func (nh *NodeHost) RequestAddNonVotingWithIdentity(shardID, replicaID uint64, target Target, identity string, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if !dbConfig.IsValidAddress(target) {
		return nil, ErrInvalidAddress
	}
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestAddObserverWithIdentity(ctx, shardID, replicaID, target, identity, nh.effectiveConfigChangeIndex(shardID, configChangeIndex))
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, shardID, cancel, nh.config.NotifyCommit), nil
}

// RequestAddWitness requests adding a voting witness asynchronously. Unlike upstream
// dragonboat, quicraft pre-validates membership changes: a change that is
// invalid against the current membership (e.g. removing the last full
// replica, or a witness policy violation) is rejected synchronously with a
// nil RequestState and a non-nil error, instead of failing later through
// RequestState.ResultC. It commits no node identity, which a NodeHost with a
// transport refuses; use RequestAddWitnessWithIdentity there.
func (nh *NodeHost) RequestAddWitness(shardID, replicaID uint64, target Target, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	return nh.RequestAddWitnessWithIdentity(shardID, replicaID, target, "", configChangeIndex, timeout)
}

// RequestAddWitnessWithIdentity is RequestAddWitness with the node identity the
// shard should commit for the witness.
func (nh *NodeHost) RequestAddWitnessWithIdentity(shardID, replicaID uint64, target Target, identity string, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if !dbConfig.IsValidAddress(target) {
		return nil, ErrInvalidAddress
	}
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestAddWitnessWithIdentity(ctx, shardID, replicaID, target, identity, nh.effectiveConfigChangeIndex(shardID, configChangeIndex))
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, shardID, cancel, nh.config.NotifyCommit), nil
}

// RequestDeleteReplica requests removing a replica asynchronously. Unlike upstream
// dragonboat, quicraft pre-validates membership changes: a change that is
// invalid against the current membership (e.g. removing the last full
// replica, or a witness policy violation) is rejected synchronously with a
// nil RequestState and a non-nil error, instead of failing later through
// RequestState.ResultC.
func (nh *NodeHost) RequestDeleteReplica(shardID, replicaID uint64, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestRemoveNode(ctx, shardID, replicaID, nh.effectiveConfigChangeIndex(shardID, configChangeIndex))
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, shardID, cancel, nh.config.NotifyCommit), nil
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

// RequestCompaction requests log compaction for the specified shard/replica
// pair. Returns a SysOpState that completes immediately since quicraft's
// RequestCompaction is synchronous.
//
// A shard that is not loaded is treated as already retired via RemoveData
// and reclaims what it can, matching dragonboat's behavior for that case.
// Quicraft deliberately does NOT compact such a shard to MaxUint64 the way
// dragonboat does: it cannot distinguish a removed replica from a merely
// stopped one, and a stopped replica's log is live data it needs on
// restart. Segment GC honors the existing compaction markers instead, so
// the canonical StopShard -> RemoveData -> RequestCompaction sequence
// succeeds without risking a healthy replica's log.
func (nh *NodeHost) RequestCompaction(shardID, replicaID uint64) (*SysOpState, error) {
	err := nh.host.RequestCompaction(shardID, replicaID)
	if err != nil {
		// Dragonboat reports a replicaID that does not match the loaded
		// replica as ErrShardNotFound, not as a replica-level error.
		if errors.Is(err, quicraft.ErrReplicaNotFound) {
			return nil, ErrShardNotFound
		}
		return nil, translateError(err)
	}
	// quicraft's RequestCompaction is synchronous; wrap a pre-completed
	// SysOpState for API compatibility.
	completedC := make(chan struct{})
	close(completedC)
	s := &SysOpState{
		completedC: completedC,
	}
	return s, nil
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// SyncRequestSnapshot requests a snapshot synchronously and returns the
// snapshot index on success.
func (nh *NodeHost) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error) {
	if err := checkContextDeadline(ctx); err != nil {
		return 0, err
	}
	if err := opt.Validate(); err != nil {
		return 0, err
	}
	if err := checkExportPath(opt); err != nil {
		return 0, err
	}
	idx, err := nh.host.SyncRequestSnapshot(ctx, shardID, opt.toQuicraft())
	if err != nil {
		return 0, translateError(err)
	}
	return idx, nil
}

// RequestSnapshot requests a snapshot asynchronously. Returns a RequestState
// that resolves when the snapshot completes.
func (nh *NodeHost) RequestSnapshot(shardID uint64, opt SnapshotOption, timeout time.Duration) (*RequestState, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	if err := checkExportPath(opt); err != nil {
		return nil, err
	}
	if err := nh.checkTimeout(timeout); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestSnapshot(ctx, shardID, opt.toQuicraft())
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, shardID, cancel, nh.config.NotifyCommit), nil
}

// ExportSnapshot streams the latest snapshot data for the given shard
// to the provided writer. The shard must have at least one snapshot
// available.
func (nh *NodeHost) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error {
	return translateError(nh.host.ExportSnapshot(ctx, shardID, w))
}

// ImportSnapshot imports a snapshot from the reader for the given shard.
// The shard must be stopped before calling. The snapshot is written to
// disk and recorded so the next StartShard can recover from it.
func (nh *NodeHost) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader) error {
	return translateError(nh.host.ImportSnapshot(ctx, shardID, r))
}

// ---------------------------------------------------------------------------
// Node User / Registry
// ---------------------------------------------------------------------------

// nodeUser implements INodeUser by delegating to the NodeHost methods
// for a specific shard/replica pair.
type nodeUser struct {
	host      *NodeHost
	shardID   uint64
	replicaID uint64
}

// ShardID returns the shard identifier this node user belongs to.
func (u *nodeUser) ShardID() uint64 { return u.shardID }

// ReplicaID returns the replica identifier for this node user.
func (u *nodeUser) ReplicaID() uint64 { return u.replicaID }

// Propose submits a proposal through Raft consensus on the shard. A
// session belonging to a different shard is rejected with
// ErrInvalidSession, matching dragonboat's per-shard session validation.
func (u *nodeUser) Propose(session *dbClient.Session, cmd []byte, timeout time.Duration) (*RequestState, error) {
	if session == nil || session.ShardID != u.shardID {
		return nil, ErrInvalidSession
	}
	return u.host.Propose(session, cmd, timeout)
}

// ReadIndex requests a linearizable read index from the shard.
func (u *nodeUser) ReadIndex(timeout time.Duration) (*RequestState, error) {
	return u.host.ReadIndex(u.shardID, timeout)
}

// GetNodeUser returns an INodeUser for direct access to the shard's replica.
// The shard must be running on this host.
func (nh *NodeHost) GetNodeUser(shardID uint64) (INodeUser, error) {
	info, err := nh.host.ShardInfo(shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return &nodeUser{
		host:      nh,
		shardID:   shardID,
		replicaID: info.ReplicaID,
	}, nil
}

// GetNodeHostRegistry returns the gossip-based node host registry.
// QuicRaft does not support gossip, so this always returns nil, false.
func (nh *NodeHost) GetNodeHostRegistry() (INodeHostRegistry, bool) {
	return nil, false
}

// ---------------------------------------------------------------------------
// Info
// ---------------------------------------------------------------------------

// GetNodeHostInfo returns a snapshot of the host's state for monitoring.
func (nh *NodeHost) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	qcOpt := quicraft.NodeHostInfoOption{
		SkipLogInfo: opt.SkipLogInfo,
	}
	qcInfo := nh.host.NodeHostInfo(qcOpt)
	if qcInfo == nil {
		return &NodeHostInfo{}
	}

	info := &NodeHostInfo{
		NodeHostID:  nh.host.ID(),
		RaftAddress: qcInfo.RaftAddress,
	}

	info.ShardInfoList = make([]ShardInfo, len(qcInfo.ShardInfoList))
	for i, si := range qcInfo.ShardInfoList {
		info.ShardInfoList[i] = fromQuicraftShardInfo(si)
	}

	if qcInfo.LogInfo != nil {
		info.LogInfo = make([]LogNodeInfo, len(qcInfo.LogInfo))
		for i, li := range qcInfo.LogInfo {
			info.LogInfo[i] = LogNodeInfo{
				ShardID:   li.ShardID,
				ReplicaID: li.ReplicaID,
			}
		}
	}

	return info
}

// HasNodeInfo returns true if the LogDB contains data for the given
// shard/replica pair.
func (nh *NodeHost) HasNodeInfo(shardID, replicaID uint64) bool {
	return nh.host.HasNodeInfo(shardID, replicaID)
}

// ---------------------------------------------------------------------------
// Log Access
// ---------------------------------------------------------------------------

// QueryRaftLog returns Raft log entries in the half-open range
// [firstIndex, lastIndex) up to maxSize bytes.
//
// NOTE: Dragonboat returns a RequestState (async) for QueryRaftLog.
// Quicraft's QueryRaftLog is synchronous and returns entries directly.
// This adapter calls the synchronous version and wraps the result in a
// pre-resolved RequestState for API compatibility. Callers can read
// from ResultC() immediately; it will never block.
func (nh *NodeHost) QueryRaftLog(shardID, firstIndex, lastIndex, maxSize uint64) (*RequestState, error) {
	ctx := context.Background()
	protoEntries, err := nh.host.QueryRaftLog(ctx, shardID, firstIndex, lastIndex, maxSize)
	if err != nil {
		// A range outside the entries currently retained is not a caller
		// error in dragonboat: it resolves the RequestState with
		// RequestOutOfRange and the available range so that a paginating
		// reader can re-anchor. Only a malformed range (lastIndex <=
		// firstIndex) is reported synchronously, as dragonboat does.
		if errors.Is(err, qcLogDB.ErrCompacted) || errors.Is(err, qcLogDB.ErrUnavailable) {
			return newResolvedRequestState(RequestResult{
				code:     RequestOutOfRange,
				logRange: nh.availableLogRange(shardID),
			}), nil
		}
		return nil, translateError(err)
	}

	// Convert proto.Entry to adapter Entry.
	entries := make([]Entry, len(protoEntries))
	for i, pe := range protoEntries {
		entries[i] = Entry{
			Index: pe.Index,
			Term:  pe.Term,
			Type:  pe.Type,
			Cmd:   pe.Cmd,
		}
	}

	// Build a pre-resolved RequestState carrying the entries.
	lr := LogRange{}
	if len(entries) > 0 {
		lr = LogRange{
			FirstIndex: entries[0].Index,
			LastIndex:  entries[len(entries)-1].Index + 1,
		}
	}
	result := RequestResult{
		inner: quicraft.RequestResult{
			Value: uint64(len(entries)),
		},
		code:     RequestCompleted,
		entries:  entries,
		logRange: lr,
	}
	return newResolvedRequestState(result), nil
}

// availableLogRange reports the log range currently retained for the
// shard. A lookup failure yields the zero LogRange: the range is
// advisory pagination detail, never the reason the query failed.
func (nh *NodeHost) availableLogRange(shardID uint64) LogRange {
	r, err := nh.host.LogRange(shardID)
	if err != nil {
		return LogRange{}
	}
	return LogRange{FirstIndex: r.FirstIndex, LastIndex: r.LastIndex}
}

// GetLogReader returns a read-only view of the Raft log for the shard.
// Dragonboat reports both a closed LogDB and an unknown shard as
// ErrLogDBNotCreatedOrClosed, so the adapter does the same rather than
// distinguishing them with ErrShardNotFound.
func (nh *NodeHost) GetLogReader(shardID uint64) (ReadonlyLogReader, error) {
	reader, err := nh.host.LogReader(shardID)
	if err != nil {
		return nil, ErrLogDBNotCreatedOrClosed
	}
	return reader, nil
}

// ---------------------------------------------------------------------------
// Data Cleanup
// ---------------------------------------------------------------------------

// SyncRemoveData removes all LogDB data for the given shard/replica pair
// synchronously. The shard must be stopped before calling. On success a
// durable removal marker is persisted so the replica stays rejected after
// a restart; a marker persistence failure is returned as a
// RemovalMarkerError (the data has already been removed at that point).
func (nh *NodeHost) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error {
	if err := translateError(nh.host.SyncRemoveData(ctx, shardID, replicaID)); err != nil {
		return err
	}
	return nh.markRemoved(shardID, replicaID)
}

// RemoveData removes all LogDB data for the given shard/replica pair.
// The shard must be stopped before calling. After removal, the replica
// cannot be restarted on this host: a durable removal marker is persisted
// so the guard survives restarts. A marker persistence failure is returned
// as a RemovalMarkerError (the data has already been removed at that point).
func (nh *NodeHost) RemoveData(shardID, replicaID uint64) error {
	if err := translateError(nh.host.RemoveData(shardID, replicaID)); err != nil {
		return err
	}
	return nh.markRemoved(shardID, replicaID)
}

// ---------------------------------------------------------------------------
// Health Metrics
// ---------------------------------------------------------------------------

// WriteHealthMetrics is not supported and writes nothing to w. It exists for
// dragonboat v4 API compatibility, where it writes dragonboat's process-wide
// metrics. QuicRaft keeps metrics per host, in the quicraft.MetricsCollector
// passed to NewNodeHostWithOptions with quicraft.WithMetrics; read them from
// that collector.
func WriteHealthMetrics(w io.Writer) {
	// Deliberately writes nothing: there is no process-wide metric set to
	// write. See the doc comment.
	_ = w
}

// ---------------------------------------------------------------------------
// Internal Helpers
// ---------------------------------------------------------------------------

// toDragonboatStateMachineType translates a quicraft/proto state machine
// type code (Regular=0, Concurrent=1, OnDisk=2) to dragonboat's numbering
// (Regular=1, Concurrent=2, OnDisk=3) as defined by the statemachine
// package's Type constants. Unknown codes map to 0, which is not a valid
// dragonboat type and therefore cannot be mistaken for any real variant.
func toDragonboatStateMachineType(t uint64) uint64 {
	switch t {
	case proto.RegularStateMachine:
		return uint64(dbSM.RegularStateMachine)
	case proto.ConcurrentStateMachine:
		return uint64(dbSM.ConcurrentStateMachine)
	case proto.OnDiskStateMachine:
		return uint64(dbSM.OnDiskStateMachine)
	}
	return 0
}

// fromQuicraftShardInfo converts a quicraft ShardInfo to the dragonboat-
// compatible format. The state machine type is translated from quicraft's
// numbering to dragonboat's; all other fields carry over verbatim.
func fromQuicraftShardInfo(si quicraft.ShardInfo) ShardInfo {
	nodes := make(map[uint64]string, len(si.Nodes))
	for k, v := range si.Nodes {
		nodes[k] = v
	}

	return ShardInfo{
		ShardID:           si.ShardID,
		ReplicaID:         si.ReplicaID,
		LeaderID:          si.LeaderID,
		Term:              si.Term,
		IsLeader:          si.IsLeader,
		IsObserver:        si.IsObserver,
		IsWitness:         si.IsWitness,
		StateMachineType:  toDragonboatStateMachineType(si.StateMachineType),
		Pending:           si.LastApplied == 0,
		PendingProposals:  si.Pending,
		Nodes:             nodes,
		ConfigChangeIndex: si.ConfigChangeIndex,
	}
}

// codeToError maps a RequestResultCode back to a sentinel error for
// error translation in ReadLocalNode.
// The mapping mirrors dragonboat's getRequestState: Terminated means the
// shard is being shut down (ErrShardClosed), not that the whole host is
// closing, and Dropped means the leader is not ready (ErrShardNotReady),
// not that the system is overloaded.
var codeErrorMap = map[RequestResultCode]error{
	RequestTimeout:    ErrTimeout,
	RequestTerminated: ErrShardClosed,
	RequestRejected:   ErrRejected,
	RequestDropped:    ErrShardNotReady,
	RequestAborted:    ErrAborted,
}

func codeToError(code RequestResultCode) error {
	if err, ok := codeErrorMap[code]; ok {
		return err
	}
	return ErrAborted
}
