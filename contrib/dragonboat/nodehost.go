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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
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

// defaultQueryTimeout is the default timeout for local state machine queries
// in ReadLocalNode when no custom timeout has been configured.
const defaultQueryTimeout = 5 * time.Second

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
	// This map grows monotonically and never evicts entries. This is
	// intentional and matches dragonboat's behavior: once a replica's
	// data is removed, that decision is permanent for the lifetime of
	// the NodeHost. The map is bounded by the total number of unique
	// (shardID, replicaID) pairs ever removed, which in practice is
	// small (typically fewer than the total number of shards configured
	// on this host).
	removed sync.Map // map[removedKey]struct{}

	// queryTimeout is the timeout for local state machine queries in
	// ReadLocalNode, stored as nanoseconds for atomic access.
	// Defaults to defaultQueryTimeout (5s). Use SetQueryTimeout to
	// override.
	queryTimeout atomic.Int64
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

	// StateMachineType identifies the state machine variant.
	StateMachineType uint64

	// Pending is the number of pending proposals.
	Pending uint64

	// Nodes maps replicaID to network address for all members.
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

// NewNodeHost creates a new NodeHost by converting the dragonboat-
// compatible configuration to quicraft's HostConfig, then constructing
// a quicraft Host with in-memory LogDB and no transport for test
// isolation, or full transport for production use.
func NewNodeHost(nhConfig dbConfig.NodeHostConfig) (*NodeHost, error) {
	qcCfg, err := nhConfig.ToQuicraft()
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "config conversion", Err: err}
	}

	host, err := quicraft.NewHost(qcCfg)
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "host creation", Err: err}
	}

	nh := &NodeHost{
		host:   host,
		config: nhConfig,
	}
	nh.queryTimeout.Store(int64(defaultQueryTimeout))
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
func NewNodeHostWithOptions(nhConfig dbConfig.NodeHostConfig, opts ...quicraft.HostOption) (*NodeHost, error) {
	qcCfg, err := nhConfig.ToQuicraft()
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "config conversion", Err: err}
	}

	host, err := quicraft.NewHost(qcCfg, opts...)
	if err != nil {
		return nil, &NodeHostConstructionError{Reason: "host creation", Err: err}
	}

	nh := &NodeHost{
		host:   host,
		config: nhConfig,
	}
	nh.queryTimeout.Store(int64(defaultQueryTimeout))
	return nh, nil
}

// SetQueryTimeout sets the timeout used for local state machine queries
// in ReadLocalNode. The duration must be positive; zero or negative values
// are ignored. This method is safe for concurrent use.
func (nh *NodeHost) SetQueryTimeout(d time.Duration) {
	if d > 0 {
		nh.queryTimeout.Store(int64(d))
	}
}

// QueryTimeout returns the current timeout for local state machine queries.
// This method is safe for concurrent use.
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

// StartReplica starts a regular state machine replica on this host.
// The initialMembers map is only used when bootstrapping a new shard.
func (nh *NodeHost) StartReplica(initialMembers map[uint64]Target, join bool, create dbSM.CreateStateMachineFunc, cfg dbConfig.Config) error {
	if nh.isRemoved(cfg.ShardID, cfg.ReplicaID) {
		return ErrReplicaRemoved
	}
	wrappedCreate := dbSM.WrapCreateFunc(create)
	qcCfg := cfg.ToQuicraft()
	return translateError(nh.host.StartShard(initialMembers, join, wrappedCreate, qcCfg))
}

// StartConcurrentReplica starts a concurrent state machine replica on
// this host. Concurrent state machines support batched updates.
func (nh *NodeHost) StartConcurrentReplica(initialMembers map[uint64]Target, join bool, create dbSM.CreateConcurrentStateMachineFunc, cfg dbConfig.Config) error {
	if nh.isRemoved(cfg.ShardID, cfg.ReplicaID) {
		return ErrReplicaRemoved
	}
	wrappedCreate := dbSM.WrapConcurrentCreateFunc(create)
	qcCfg := cfg.ToQuicraft()
	return translateError(nh.host.StartShard(initialMembers, join, wrappedCreate, qcCfg))
}

// StartOnDiskReplica starts an on-disk state machine replica on this host.
// On-disk state machines persist their state externally and only use
// snapshots for recovery.
func (nh *NodeHost) StartOnDiskReplica(initialMembers map[uint64]Target, join bool, create dbSM.CreateOnDiskStateMachineFunc, cfg dbConfig.Config) error {
	if nh.isRemoved(cfg.ShardID, cfg.ReplicaID) {
		return ErrReplicaRemoved
	}
	wrappedCreate := dbSM.WrapOnDiskCreateFunc(create)
	qcCfg := cfg.ToQuicraft()
	return translateError(nh.host.StartShard(initialMembers, join, wrappedCreate, qcCfg))
}

// isRemoved checks whether a shard/replica pair was permanently removed.
func (nh *NodeHost) isRemoved(shardID, replicaID uint64) bool {
	_, removed := nh.removed.Load(removedKey{shardID, replicaID})
	return removed
}

// markRemoved records a shard/replica pair as permanently removed.
func (nh *NodeHost) markRemoved(shardID, replicaID uint64) {
	nh.removed.Store(removedKey{shardID, replicaID}, struct{}{})
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
	if _, ok := ctx.Deadline(); !ok {
		return dbSM.Result{}, ErrDeadlineNotSet
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
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrDeadlineNotSet
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

// onDiskStateMachineType is the quicraft/proto constant for on-disk SMs.
// Tracked sessions are not supported for on-disk state machines because
// they manage their own persistence and do not participate in the session
// tracking machinery (dragonboat semantics: ErrRejected).
const onDiskStateMachineType uint64 = 2

// SyncGetSession registers a new client session through Raft consensus
// and returns it. The session provides at-most-once proposal semantics.
// Returns ErrRejected for shards backed by an on-disk state machine, which
// do not support tracked sessions.
func (nh *NodeHost) SyncGetSession(ctx context.Context, shardID uint64) (*dbClient.Session, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrDeadlineNotSet
	}
	info, infoErr := nh.host.GetShardInfo(shardID)
	if infoErr == nil && info.StateMachineType == onDiskStateMachineType {
		return nil, ErrRejected
	}
	session, err := nh.host.GetNewSession(ctx, shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return dbClient.WrapSession(session), nil
}

// SyncCloseSession closes a client session by proposing unregistration
// through Raft consensus. Blocks until the session is closed or the
// context is canceled.
func (nh *NodeHost) SyncCloseSession(ctx context.Context, cs *dbClient.Session) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
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
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	if session.IsNoOPSession() {
		rs, err := nh.host.Propose(ctx, session.ShardID, cmd)
		if err != nil {
			cancel()
			return nil, translateError(err)
		}
		return WrapRequestStateWithCancel(rs, cancel), nil
	}

	rs, err := nh.host.ProposeWithSession(ctx, session.Inner(), cmd)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
}

// ProposeSession submits a session proposal (registration or
// unregistration) asynchronously. Returns a RequestState that the caller
// polls for the result. In dragonboat, this is used to register or
// unregister a client session via Raft consensus.
//
// The session's SeriesID determines the operation:
//   - SeriesIDForRegister  → register a new session
//   - SeriesIDForUnregister → close an existing session
//   - Any other value      → close (session already registered)
func (nh *NodeHost) ProposeSession(session *dbClient.Session, timeout time.Duration) (*RequestState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	inner := session.Inner()
	sid := inner.SeriesID()

	if sid == quicraft.SeriesIDForRegister {
		// Register the caller's session through Raft, preserving its
		// original client ID. Unlike the previous GetNewSession call,
		// RegisterSession does not generate a new random client ID.
		registered, err := nh.host.RegisterSession(ctx, inner)
		if err != nil {
			cancel()
			return nil, translateError(err)
		}
		// Atomically swap the inner quicraft session pointer so concurrent
		// readers never observe a partially written struct. ShardID is
		// invariant (same shard), so only the inner pointer needs updating.
		session.SwapInner(registered)
		cancel()
		return newResolvedRequestState(RequestResult{
			inner: quicraft.RequestResult{Value: session.ClientID()},
			code:  RequestCompleted,
		}), nil
	}

	// Close/unregister the session.
	rs, err := nh.host.CloseSession(ctx, session.ShardID, inner)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
}

// ReadIndex requests a linearizable read index. The returned RequestState
// resolves when the local state machine has caught up to the confirmed
// read index. The shardID is stored on the wrapper for ReadLocalNode.
func (nh *NodeHost) ReadIndex(shardID uint64, timeout time.Duration) (*RequestState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.ReadIndex(ctx, shardID)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateForShardWithCancel(rs, shardID, cancel), nil
}

// ReadLocalNode blocks on the RequestState's ResultC (from ReadIndex) until
// the read index resolves, then queries the local state machine. This
// two-step pattern provides linearizable reads via the dragonboat async API.
//
// NOTE: Dragonboat's ReadLocalNode takes a resolved RequestState from
// ReadIndex. In quicraft, ReadIndex + QueryLocalNode are separate calls.
// This adapter performs the local query after the ReadIndex completes.
//
// This method internally blocks on rs.ResultC() and verifies the result is
// Completed() before performing the local query. The caller should NOT drain
// ResultC() before calling ReadLocalNode, as that would cause this method to
// block indefinitely on the already-drained channel.
//
// Usage:
//
//	rs, _ := nh.ReadIndex(shardID, timeout)
//	val, err := nh.ReadLocalNode(rs, query)
func (nh *NodeHost) ReadLocalNode(rs *RequestState, query interface{}) (interface{}, error) {
	// Block on ResultC until the ReadIndex resolves. The channel delivers
	// exactly one value when the read index is confirmed (or fails).
	// Closing the channel without a value indicates the request was
	// abandoned. There is no default case: silently skipping an
	// unresolved ReadIndex would violate linearizability.
	resultC := rs.ResultC()
	if resultC != nil {
		result, ok := <-resultC
		if !ok {
			return nil, ErrClosed
		}
		if !result.Completed() {
			return nil, translateError(codeToError(result.code))
		}
	}

	// The wrapper carries the shardID from the original ReadIndex call.
	shardID := rs.ShardID()
	ctx, cancel := context.WithTimeout(context.Background(), nh.QueryTimeout())
	defer cancel()
	val, err := nh.host.QueryLocalNode(ctx, shardID, query)
	if err != nil {
		return nil, translateError(err)
	}
	return val, nil
}

// NAReadLocalNode performs a non-agreement read on the local node. This
// would call the state machine's NALookup method if the state machine
// implements the IExtended interface. QuicRaft does not currently
// support NALookup, so this returns ErrNotImplemented from the
// statemachine package.
func (nh *NodeHost) NAReadLocalNode(rs *RequestState, query []byte) ([]byte, error) {
	_ = rs    // accepted for dragonboat v4 API compatibility
	_ = query // accepted for dragonboat v4 API compatibility
	return nil, dbSM.ErrNotImplemented
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
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrDeadlineNotSet
	}
	m, err := nh.host.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return fromQuicraftMembership(m), nil
}

// GetShardInfo returns information about the specified shard. The bool
// return value is true when the shard is found, false when it is not
// loaded on this host. This matches dragonboat's three-value return
// signature for shard info queries.
func (nh *NodeHost) GetShardInfo(shardID uint64) (*ShardInfo, bool, error) {
	info, err := nh.host.GetShardInfo(shardID)
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
	m, err := nh.host.GetShardMembership(shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return fromQuicraftMembership(m), nil
}

// GetLeaderID returns the leader's replicaID, the current term, whether
// a leader is known, and any error. This matches dragonboat's four-value
// return signature. Quicraft's GetLeaderID returns (leaderID, valid, error);
// the term is obtained from GetShardInfo.
func (nh *NodeHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error) {
	leaderID, term, valid, err := nh.host.GetLeaderIDAndTerm(shardID)
	if err != nil {
		return 0, 0, false, translateError(err)
	}
	return leaderID, term, valid, nil
}

// SyncRequestAddReplica adds a voting replica to the shard and blocks
// until the membership change is committed.
func (nh *NodeHost) SyncRequestAddReplica(ctx context.Context, shardID, replicaID uint64, target Target, configChangeIndex uint64) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddNode(ctx, shardID, replicaID, target, configChangeIndex))
}

// SyncRequestAddNonVoting adds a non-voting observer replica and blocks
// until the membership change is committed.
func (nh *NodeHost) SyncRequestAddNonVoting(ctx context.Context, shardID, replicaID uint64, target Target, configChangeIndex uint64) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddObserver(ctx, shardID, replicaID, target, configChangeIndex))
}

// SyncRequestAddWitness adds a voting witness replica and blocks until
// the membership change is committed.
func (nh *NodeHost) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, target Target, configChangeIndex uint64) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
	}
	if !dbConfig.IsValidAddress(target) {
		return ErrInvalidAddress
	}
	return translateError(nh.host.SyncRequestAddWitness(ctx, shardID, replicaID, target, configChangeIndex))
}

// SyncRequestDeleteReplica removes a replica from the shard and blocks
// until the membership change is committed.
func (nh *NodeHost) SyncRequestDeleteReplica(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
	}
	return translateError(nh.host.SyncRequestDeleteNode(ctx, shardID, replicaID, configChangeIndex))
}

// RequestLeaderTransfer requests leadership transfer to the specified
// target replica. Pass 0 to transfer to the most up-to-date follower.
func (nh *NodeHost) RequestLeaderTransfer(shardID, targetReplicaID uint64) error {
	ctx := context.Background()
	return translateError(nh.host.RequestLeaderTransfer(ctx, shardID, targetReplicaID))
}

// ---------------------------------------------------------------------------
// Async Membership Operations
// ---------------------------------------------------------------------------

// RequestAddReplica requests adding a voting replica asynchronously.
// Returns a RequestState that resolves when the config change is committed.
func (nh *NodeHost) RequestAddReplica(shardID, replicaID uint64, target Target, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if !dbConfig.IsValidAddress(target) {
		return nil, ErrInvalidAddress
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestAddNode(ctx, shardID, replicaID, target, configChangeIndex)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
}

// RequestAddNonVoting requests adding a non-voting observer asynchronously.
func (nh *NodeHost) RequestAddNonVoting(shardID, replicaID uint64, target Target, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if !dbConfig.IsValidAddress(target) {
		return nil, ErrInvalidAddress
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestAddObserver(ctx, shardID, replicaID, target, configChangeIndex)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
}

// RequestAddWitness requests adding a voting witness asynchronously.
func (nh *NodeHost) RequestAddWitness(shardID, replicaID uint64, target Target, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if !dbConfig.IsValidAddress(target) {
		return nil, ErrInvalidAddress
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestAddWitness(ctx, shardID, replicaID, target, configChangeIndex)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
}

// RequestDeleteReplica requests removing a replica asynchronously.
func (nh *NodeHost) RequestDeleteReplica(shardID, replicaID uint64, configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestRemoveNode(ctx, shardID, replicaID, configChangeIndex)
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

// RequestCompaction requests log compaction for the specified shard/replica
// pair. Returns a SysOpState that completes immediately since quicraft's
// RequestCompaction is synchronous.
func (nh *NodeHost) RequestCompaction(shardID, replicaID uint64) (*SysOpState, error) {
	err := nh.host.RequestCompaction(shardID, replicaID)
	if err != nil {
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
	if _, ok := ctx.Deadline(); !ok {
		return 0, ErrDeadlineNotSet
	}
	if err := opt.Validate(); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rs, err := nh.host.RequestSnapshot(ctx, shardID, opt.toQuicraft())
	if err != nil {
		cancel()
		return nil, translateError(err)
	}
	return WrapRequestStateWithCancel(rs, cancel), nil
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

// Propose submits a proposal through Raft consensus on the shard.
func (u *nodeUser) Propose(session *dbClient.Session, cmd []byte, timeout time.Duration) (*RequestState, error) {
	return u.host.Propose(session, cmd, timeout)
}

// ReadIndex requests a linearizable read index from the shard.
func (u *nodeUser) ReadIndex(timeout time.Duration) (*RequestState, error) {
	return u.host.ReadIndex(u.shardID, timeout)
}

// GetNodeUser returns an INodeUser for direct access to the shard's replica.
// The shard must be running on this host.
func (nh *NodeHost) GetNodeUser(shardID uint64) (INodeUser, error) {
	info, err := nh.host.GetShardInfo(shardID)
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
	qcInfo := nh.host.GetNodeHostInfo(qcOpt)
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
	result := RequestResult{
		inner: quicraft.RequestResult{
			Value: uint64(len(entries)),
		},
		code:    RequestCompleted,
		entries: entries,
	}
	return newResolvedRequestState(result), nil
}

// GetLogReader returns a read-only view of the Raft log for the shard.
func (nh *NodeHost) GetLogReader(shardID uint64) (ReadonlyLogReader, error) {
	reader, err := nh.host.GetLogReader(shardID)
	if err != nil {
		return nil, translateError(err)
	}
	return reader, nil
}

// ---------------------------------------------------------------------------
// Data Cleanup
// ---------------------------------------------------------------------------

// SyncRemoveData removes all LogDB data for the given shard/replica pair
// synchronously. The shard must be stopped before calling.
func (nh *NodeHost) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error {
	if err := translateError(nh.host.SyncRemoveData(ctx, shardID, replicaID)); err != nil {
		return err
	}
	nh.markRemoved(shardID, replicaID)
	return nil
}

// RemoveData removes all LogDB data for the given shard/replica pair.
// The shard must be stopped before calling. After removal, the replica
// cannot be restarted on this host.
func (nh *NodeHost) RemoveData(shardID, replicaID uint64) error {
	if err := translateError(nh.host.RemoveData(shardID, replicaID)); err != nil {
		return err
	}
	nh.markRemoved(shardID, replicaID)
	return nil
}

// ---------------------------------------------------------------------------
// Health Metrics
// ---------------------------------------------------------------------------

// WriteHealthMetrics writes health metrics to the provided writer.
// This is a no-op in QuicRaft; it exists for dragonboat v4 API
// compatibility.
func WriteHealthMetrics(w io.Writer) {
	_ = w // accepted for dragonboat v4 API compatibility
}

// ---------------------------------------------------------------------------
// Internal Helpers
// ---------------------------------------------------------------------------

// fromQuicraftShardInfo converts a quicraft ShardInfo to the dragonboat-
// compatible format.
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
		StateMachineType:  si.StateMachineType,
		Pending:           si.Pending,
		Nodes:             nodes,
		ConfigChangeIndex: si.ConfigChangeIndex,
	}
}

// codeToError maps a RequestResultCode back to a sentinel error for
// error translation in ReadLocalNode.
var codeErrorMap = map[RequestResultCode]error{
	RequestTimeout:    ErrTimeout,
	RequestTerminated: ErrClosed,
	RequestRejected:   ErrRejected,
	RequestDropped:    ErrSystemBusy,
	RequestAborted:    ErrAborted,
}

func codeToError(code RequestResultCode) error {
	if err, ok := codeErrorMap[code]; ok {
		return err
	}
	return ErrAborted
}
