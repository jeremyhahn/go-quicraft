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
	"bytes"
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

// defaultQueryTimeout is the ReadLocalNode query deadline; zero means none, as in dragonboat.
const defaultQueryTimeout time.Duration = 0

// maxDetachedLookups bounds timed lookups in flight; a timed read finding no slot gets ErrSystemBusy.
const maxDetachedLookups = 64

// defaultWaitReadyTimeout bounds the Start*Replica readiness wait when Config.WaitReady is set.
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

	// removed holds permanently removed shard/replica pairs, persisted as marker files under NodeHostDir and reloaded at startup.
	removed sync.Map // map[removedKey]struct{}

	// queryTimeout is the ReadLocalNode query deadline in nanoseconds; 0 means none.
	queryTimeout atomic.Int64

	// detachedLookups counts the timed local lookups whose state machine
	// call has not returned yet, bounded by maxDetachedLookups.
	detachedLookups atomic.Int64

	// shardConfigs keeps each shard's dragonboat Config so OrderedConfigChange can be honoured.
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

	// CompactionIndex is not supported by quicraft; a non-zero value fails Validate with ErrInvalidOption.
	CompactionIndex uint64

	// OverrideCompactionOverhead controls whether CompactionOverhead
	// replaces the shard-level value.
	OverrideCompactionOverhead bool

	// Exported indicates whether this is an export operation. Set
	// automatically when ExportPath is non-empty.
	Exported bool
}

// Validate rejects setting both CompactionOverhead and CompactionIndex, and any CompactionIndex.
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

	// StateMachineType uses dragonboat's numbering (Regular=1, Concurrent=2, OnDisk=3), translated from quicraft's 0/1/2.
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

// validateNodeHostConfig runs dragonboat's construction-time Validate and Prepare checks.
func validateNodeHostConfig(cfg *dbConfig.NodeHostConfig) error {
	if err := cfg.Validate(); err != nil {
		return &NodeHostConstructionError{Reason: "config validation", Err: err}
	}
	if err := cfg.Prepare(); err != nil {
		return &NodeHostConstructionError{Reason: "config preparation", Err: err}
	}
	return nil
}

// NewNodeHost builds a quicraft Host from a dragonboat config; storage is unencrypted at rest, use NewNodeHostWithOptions with quicraft.WithBarrier to encrypt.
func NewNodeHost(nhConfig dbConfig.NodeHostConfig) (*NodeHost, error) {
	if err := validateNodeHostConfig(&nhConfig); err != nil {
		return nil, err
	}

	qcCfg, err := nhConfig.ToQuicraft()
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "config conversion", Err: err}
	}

	// The drop-in constructor stores the Raft log and snapshots as plaintext, as dragonboat does.
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

// NewNodeHostWithOptions creates a NodeHost with quicraft HostOptions, which must state at-rest encryption (WithBarrier, WithShardBarriers or WithoutEncryption).
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

// checkContextDeadline requires a context deadline that has not passed, as dragonboat's getTimeoutFromContext does.
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

// checkTimeout returns ErrTimeoutTooSmall for a non-positive timeout or one shorter than an RTT.
func (nh *NodeHost) checkTimeout(timeout time.Duration) error {
	if timeout <= 0 || uint64(timeout.Milliseconds()) < nh.config.RTTMillisecond {
		return ErrTimeoutTooSmall
	}
	return nil
}

// SetQueryTimeout sets the ReadLocalNode query deadline (zero or negative disables it); a timed-out lookup keeps running in the background.
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

// validateStartReplica checks the shard config, that a joining replica has no initial members, and that the replica was not removed.
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

// waitShardReady polls until the shard knows a leader or the bounded wait elapses.
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

// effectiveConfigChangeIndex zeroes the index unless OrderedConfigChange is set, as dragonboat ignores it then.
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

// markRemoved records a removed shard/replica in memory and as a durable marker; a persistence error is returned.
func (nh *NodeHost) markRemoved(shardID, replicaID uint64) error {
	nh.removed.Store(removedKey{shardID, replicaID}, struct{}{})
	return writeRemovalMarker(fsutil.SyncDir, nh.removedMarkerDir(), nh.removedMarkerPath(shardID, replicaID))
}

// writeRemovalMarker creates the marker file in dir and fsyncs the file, dir and dir's parent.
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
	if !session.ValidForProposal(session.ShardID) {
		return dbSM.Result{}, ErrInvalidSession
	}

	// ProposeWithSession leaves ProposalCompleted to the caller, as dragonboat's contract requires.
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

// SyncGetSession registers a client session through Raft; on-disk state machines get ErrRejected (compared in quicraft numbering).
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

// requireNoOPSessionOnDisk returns ErrRejected when an on-disk state machine gets a non-NoOP session (quicraft numbering).
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

// Propose submits an asynchronous proposal; the context's cancel runs on RequestState.Release.
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
	if !session.ValidForProposal(session.ShardID) {
		cancel()
		return nil, ErrInvalidSession
	}

	rs, err := nh.host.ProposeWithSession(ctx, session.Inner(), cmd)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return wrapRequestState(rs, session.ShardID, cancel, nh.config.NotifyCommit), nil
}

// ProposeSession registers or unregisters a session asynchronously, per its SeriesID; any other series returns ErrInvalidSession.
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
			// Swap the inner session pointer atomically; ShardID is unchanged.
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

// ReadLocalNode queries the local state machine after the ReadIndex for rs completed; before that it returns ErrInvalidOperation.
func (nh *NodeHost) ReadLocalNode(rs *RequestState, query interface{}) (interface{}, error) {
	return nh.readLocalNode(rs, query)
}

// NAReadLocalNode is the byte-slice ReadLocalNode via IExtended.NALookup; statemachine.ErrNotImplemented when not implemented.
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
	// Lookup takes no context: run it in a goroutine with its own copy of a []byte query and stop waiting at the deadline.
	query = detachedQuery(query)
	// Each lookup holds a slot until it actually returns, not until its
	// caller stops waiting, so stuck lookups cannot pile up.
	if !nh.acquireDetachedLookup() {
		return nil, ErrSystemBusy
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	type lookupResult struct {
		val      interface{}
		err      error
		panicked bool
		panicVal interface{}
	}
	done := make(chan lookupResult, 1)
	go func() {
		defer nh.detachedLookups.Add(-1)
		var r lookupResult
		// Carry a Lookup panic back to the caller; a Goexit is reported as ErrAborted.
		returned := false
		defer func() {
			if p := recover(); p != nil {
				r = lookupResult{panicked: true, panicVal: p}
			} else if !returned {
				r = lookupResult{err: ErrAborted}
			}
			done <- r
		}()
		r.val, r.err = nh.host.QueryLocalNode(ctx, shardID, query)
		returned = true
	}()
	select {
	case r := <-done:
		if r.panicked {
			panic(r.panicVal)
		}
		if r.err != nil {
			return nil, translateError(r.err)
		}
		return r.val, nil
	case <-ctx.Done():
		return nil, translateError(ctx.Err())
	}
}

// detachedQuery returns a copy of query that a lookup outliving its
// caller's deadline can own: []byte and nalookup.Query keys are cloned,
// any other value is returned unchanged.
func detachedQuery(query interface{}) interface{} {
	switch q := query.(type) {
	case []byte:
		return bytes.Clone(q)
	case nalookup.Query:
		return nalookup.Query{Key: bytes.Clone(q.Key)}
	default:
		return query
	}
}

// acquireDetachedLookup takes one of the maxDetachedLookups slots for a
// timed local lookup, reporting false when all are taken. The lookup's
// goroutine releases the slot when the state machine call returns.
func (nh *NodeHost) acquireDetachedLookup() bool {
	for {
		n := nh.detachedLookups.Load()
		if n >= maxDetachedLookups {
			return false
		}
		if nh.detachedLookups.CompareAndSwap(n, n+1) {
			return true
		}
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

// GetShardInfo returns the shard's info and true, or nil, false and ErrShardNotFound when the shard is not loaded.
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

// GetShardMembership returns the local replica's committed membership without a ReadIndex.
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

// GetLeaderID returns the leader's replicaID, the term, whether a leader is known, and any error.
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

// SyncRequestAddReplicaWithIdentity is SyncRequestAddReplica committing the new replica's node identity.
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

// NodeIdentity returns this NodeHost's node identity; empty without a transport.
func (nh *NodeHost) NodeIdentity() string {
	return nh.host.NodeIdentity()
}

// ReloadMutualTLS re-reads CAFile, CertFile and KeyFile and swaps them in for new handshakes when every check passes.
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

// RequestLeaderTransfer asks to transfer leadership to a voting replica; others get ErrInvalidTarget.
func (nh *NodeHost) RequestLeaderTransfer(shardID, targetReplicaID uint64) error {
	ctx := context.Background()
	return translateError(nh.host.RequestLeaderTransfer(ctx, shardID, targetReplicaID))
}

// ---------------------------------------------------------------------------
// Async Membership Operations
// ---------------------------------------------------------------------------

// RequestAddReplica adds a voting replica asynchronously; invalid changes are rejected synchronously, and a transport host needs RequestAddReplicaWithIdentity.
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

// RequestAddWitness adds a witness asynchronously; invalid changes are rejected synchronously, and a transport host needs RequestAddWitnessWithIdentity.
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

// RequestDeleteReplica removes a replica asynchronously; invalid changes are rejected synchronously.
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

// RequestCompaction compacts synchronously and returns a completed SysOpState; an unloaded shard is treated as removed, without compacting a stopped replica's log.
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

// QueryRaftLog returns entries in [firstIndex, lastIndex) up to maxSize bytes in a pre-resolved RequestState.
func (nh *NodeHost) QueryRaftLog(shardID, firstIndex, lastIndex, maxSize uint64) (*RequestState, error) {
	ctx := context.Background()
	protoEntries, err := nh.host.QueryRaftLog(ctx, shardID, firstIndex, lastIndex, maxSize)
	if err != nil {
		// Out-of-range resolves with RequestOutOfRange and the available range; only lastIndex <= firstIndex is a synchronous error.
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

// GetLogReader returns a read-only view of the shard's log; a closed LogDB and an unknown shard both give ErrLogDBNotCreatedOrClosed.
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

// SyncRemoveData removes a stopped replica's LogDB data and persists a removal marker; a marker failure is a RemovalMarkerError.
func (nh *NodeHost) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error {
	if err := translateError(nh.host.SyncRemoveData(ctx, shardID, replicaID)); err != nil {
		return err
	}
	return nh.markRemoved(shardID, replicaID)
}

// RemoveData removes a stopped replica's LogDB data and persists a removal marker; a marker failure is a RemovalMarkerError.
func (nh *NodeHost) RemoveData(shardID, replicaID uint64) error {
	if err := translateError(nh.host.RemoveData(shardID, replicaID)); err != nil {
		return err
	}
	return nh.markRemoved(shardID, replicaID)
}

// ---------------------------------------------------------------------------
// Health Metrics
// ---------------------------------------------------------------------------

// WriteHealthMetrics writes nothing; metrics are in the quicraft.MetricsCollector passed with quicraft.WithMetrics.
func WriteHealthMetrics(w io.Writer) {
	// Deliberately writes nothing: there is no process-wide metric set to
	// write. See the doc comment.
	_ = w
}

// ---------------------------------------------------------------------------
// Internal Helpers
// ---------------------------------------------------------------------------

// toDragonboatStateMachineType maps quicraft codes 0/1/2 to dragonboat's 1/2/3; unknown codes map to 0.
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

// codeToError maps a RequestResultCode to a sentinel as dragonboat's getRequestState does (Terminated: ErrShardClosed, Dropped: ErrShardNotReady).
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
