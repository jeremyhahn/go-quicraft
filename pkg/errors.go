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

// Package quicraft provides a multi-group Raft consensus library with QUIC transport.
package quicraft

import (
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// Sentinel errors are pre-allocated for zero-alloc hot path returns and
// use with errors.Is(). Each error uses the "quicraft:" prefix for clear
// provenance in error chains.
var (
	// ErrBusy is returned when the per-shard in-memory log exceeds
	// MaxInMemLogSize and new proposals are rate-limited.
	ErrBusy = errors.New("quicraft: system busy")

	// ErrClosed is returned when an operation is attempted on a closed host.
	ErrClosed = errors.New("quicraft: host closed")

	// ErrCanceled is returned when a request is canceled by the caller
	// via context cancellation.
	ErrCanceled = errors.New("quicraft: request canceled")

	// ErrTimeout is returned when a request exceeds its deadline.
	ErrTimeout = errors.New("quicraft: timeout")

	// ErrRejected is returned when a proposal is rejected, for example
	// when a config change is already in progress.
	ErrRejected = errors.New("quicraft: request rejected")

	// ErrAborted is returned when an operation is aborted due to
	// an internal state transition such as leadership change.
	ErrAborted = errors.New("quicraft: operation aborted")

	// ErrShardBarriersPlaintextWAL is returned by NewHost when
	// WithShardBarriers is configured but the resulting WAL would hold
	// plaintext: the default WAL takes a single host-wide barrier, so
	// per-shard barriers alone leave every Raft log entry -- and therefore
	// every user command -- unencrypted on disk while snapshots ARE
	// encrypted. Silently accepting that hands an operator who configured
	// encryption a cleartext command log whose spot-check looks correct.
	// Supply WithLogDB with a multiplexing LogDB for genuine per-shard WAL
	// keys, or WithBarrier to at least encrypt the WAL host-wide.
	//
	// The refusal is decided by what the LogDB IS, never by whether one was
	// supplied. WithLogDB is not itself a mitigation: an unencrypted
	// waldb.Open(dir) handed to WithLogDB produces byte-for-byte the state
	// this sentinel exists to prevent. The LogDB must report
	// logdb.AtRestBarrierEncrypted or logdb.AtRestNonDurable, and a LogDB that
	// does not implement logdb.AtRestProtectedLogDB at all declares nothing and
	// is therefore treated as unprotected.
	ErrShardBarriersPlaintextWAL = errors.New(
		"quicraft: WithShardBarriers requires a LogDB that is encrypted at rest " +
			"(logdb.AtRestProtectedLogDB reporting barrier_encrypted or non_durable) " +
			"or WithBarrier (host-wide WAL key); this LogDB would store plaintext")

	// ErrShardBarrierUnresolved is returned when WithShardBarriers is
	// configured, the resolver returns nil for a shard, and there is no
	// host-wide barrier to fall back to.
	//
	// A resolver typed func(shardID uint64) Barrier cannot distinguish "this
	// shard is deliberately plaintext" from "I do not know this shard yet",
	// which is the normal outcome of a map lookup for an async-provisioned
	// tenant or a remapped shard ID. Reading that nil as "plaintext is fine"
	// downgrades exactly one tenant while every other tenant's spot-check still
	// shows encryption working, so the miss fails CLOSED instead.
	//
	// It is NOT returned when WithBarrier supplied a host-wide barrier: a nil
	// resolver result then means the documented fallback to that barrier, which
	// still encrypts.
	//
	// Re-exported from the engine so the host paths (receive, import, export,
	// StartShard) and the engine paths (snapshot save, recover, DEK purge) fail
	// with ONE error identity for one condition.
	ErrShardBarrierUnresolved = engine.ErrShardBarrierUnresolved

	// ErrShardBarrierLogDBMismatch is returned when a shard's resolved
	// per-shard barrier is NOT the barrier the supplied LogDB stores that
	// shard's records under.
	//
	// It closes the second half of the per-shard axis: a LogDB can be encrypted
	// at rest (satisfying ErrShardBarriersPlaintextWAL) and still hold this
	// shard's log under a DIFFERENT tenant's key, which is precisely the key
	// isolation WithShardBarriers exists to provide. The LogDB answers by
	// barrier identity through logdb.PerBarrierEpochAwareLogDB; a LogDB that
	// cannot answer cannot prove isolation and is refused.
	ErrShardBarrierLogDBMismatch = errors.New(
		"quicraft: the supplied LogDB does not store this shard's records under " +
			"the barrier WithShardBarriers resolved for it")

	// ErrDropped is returned when a proposal is dropped by the engine
	// due to queue overflow or shutdown in progress.
	ErrDropped = errors.New("quicraft: proposal dropped")

	// ErrShardNotFound is returned when the requested shard does not
	// exist on this host.
	ErrShardNotFound = errors.New("quicraft: shard not found")

	// ErrShardNotStarted is returned when a shard has been registered
	// but has not yet completed initialization.
	ErrShardNotStarted = errors.New("quicraft: shard not started")

	// ErrShardNotReady is returned when the shard is still initializing
	// and cannot serve requests yet.
	ErrShardNotReady = errors.New("quicraft: shard not ready")

	// ErrShardAlreadyExists is returned when StartShard is called for
	// a shard that is already running on this host.
	ErrShardAlreadyExists = errors.New("quicraft: shard already exists")

	// ErrShardFailed is returned when the state machine circuit breaker
	// has tripped due to repeated apply failures.
	ErrShardFailed = errors.New("quicraft: shard failed (SM circuit breaker tripped)")

	// ErrInvalidConfig is returned when a configuration value fails
	// validation during StartShard or NewHost.
	ErrInvalidConfig = errors.New("quicraft: invalid configuration")

	// ErrInvalidOperation is returned when an operation is not valid
	// for the current state, such as proposing to a witness replica.
	ErrInvalidOperation = errors.New("quicraft: invalid operation")

	// ErrNotLeader is returned when a proposal or read-index request
	// targets a node that is not the current leader.
	ErrNotLeader = errors.New("quicraft: not leader")

	// ErrReleased is returned when Result() or ResultC() is called on
	// a RequestState that has already been released back to the pool.
	ErrReleased = errors.New("quicraft: request state released")

	// ErrLogDBWrite is returned when a write to the log database fails.
	ErrLogDBWrite = errors.New("quicraft: logdb write failed")

	// ErrRecoveredTermUnknown is returned when StartShard cannot resolve the
	// Raft term of the index an on-disk state machine recovered to. Seeding
	// term 0 would stamp it on the next snapshot record, and a follower that
	// installs that snapshot reports lastTerm() == 0 — which makes isUpToDate
	// grant a vote to EVERY candidate, including one missing entries this
	// replica holds committed. That is a Leader Completeness violation, so an
	// unresolvable term must fail the shard start rather than seed a sentinel.
	ErrRecoveredTermUnknown = errors.New("quicraft: recovered applied index has no resolvable raft term")

	// ErrShutdownTimeout is returned when Host.Close() exceeds the
	// configured ShutdownTimeout waiting for graceful shutdown.
	ErrShutdownTimeout = errors.New("quicraft: shutdown timeout exceeded")

	// ErrSystemBusy is returned when the global in-memory log size
	// across all shards exceeds MaxTotalInMemLogSize. It is a *BusyError
	// so that errors.Is(err, ErrBusy) returns true for both per-shard
	// and global busy conditions.
	ErrSystemBusy error = &BusyError{message: "quicraft: system busy, global rate limit exceeded"}

	// ErrRotateInProgress is returned when a key rotation is requested
	// while another rotation is already in progress.
	ErrRotateInProgress = errors.New("quicraft: key rotation already in progress")

	// ErrDecompressionBomb is returned when a decompressed payload
	// exceeds the configured maximum size, preventing zip-bomb attacks.
	ErrDecompressionBomb = errors.New("quicraft: decompressed size exceeds limit")

	// ErrSessionLimitExceeded is returned when the per-client or total
	// session limit for a shard has been reached.
	ErrSessionLimitExceeded = errors.New("quicraft: session limit exceeded")

	// ErrTooManySessions is the legacy alias for ErrSessionLimitExceeded,
	// kept for backward compatibility. It is the same pointer value so
	// that errors.Is(ErrTooManySessions, ErrSessionLimitExceeded) is true.
	ErrTooManySessions = ErrSessionLimitExceeded

	// ErrBarrierSealed is returned when an encrypt/decrypt operation is
	// attempted while the barrier is sealed. Re-exported from seal package.
	ErrBarrierSealed = seal.ErrBarrierSealed

	// ErrBarrierNotInit is returned when the barrier has not been
	// initialized with a sealing strategy. Re-exported from seal package.
	ErrBarrierNotInit = seal.ErrBarrierNotInit

	// ErrDiskFull is returned when the WAL directory has reached its
	// configured maximum size.
	ErrDiskFull = errors.New("quicraft: WAL disk full")

	// ErrDecryptionFailed is returned when a decryption operation fails,
	// typically due to a corrupted ciphertext or incorrect key. Re-exported from seal package.
	ErrDecryptionFailed = seal.ErrDecryptionFailed

	// ErrSnapshotRecoveryFailed is returned when recovering from a
	// snapshot fails due to corruption or incompatibility.
	ErrSnapshotRecoveryFailed = errors.New("quicraft: snapshot recovery failed")

	// ErrEntryTooLarge is the sentinel form of EntryTooLargeError, used
	// for errors.Is() checks without needing the structured fields.
	ErrEntryTooLarge = errors.New("quicraft: entry too large")

	// ErrSessionNotFound is returned when an operation references a
	// client session that does not exist.
	ErrSessionNotFound = errors.New("quicraft: session not found")

	// ErrSessionExpired is returned when a session-managed proposal
	// references a clientID whose session is no longer registered.
	// This occurs after session expiry or snapshot restore that did
	// not include the session (Raft PhD Figure 6.1, step 3:
	// "Reply SESSION_EXPIRED if no record of clientId or if response
	// for client's sequenceNum already discarded").
	ErrSessionExpired = errors.New("quicraft: session expired")

	// ErrSessionAlreadyExists is returned when registering a client
	// session that is already registered.
	ErrSessionAlreadyExists = errors.New("quicraft: session already exists")

	// ErrResponseLimitExceeded is returned when a session's cached
	// response count exceeds the configured maximum per session.
	ErrResponseLimitExceeded = errors.New("quicraft: response limit exceeded")

	// ErrInvalidSMType is returned when a CreateFunc returns an object
	// that does not implement any known state machine interface.
	ErrInvalidSMType = errors.New("quicraft: invalid state machine type")

	// ErrSnapshotNotFound is returned when a requested snapshot does
	// not exist on disk.
	ErrSnapshotNotFound = errors.New("quicraft: snapshot not found")

	// ErrSnapshotInProgress is returned when a snapshot is already
	// in progress for the shard.
	ErrSnapshotInProgress = errors.New("quicraft: snapshot already in progress")

	// ErrStopped is returned when an operation is interrupted because
	// the stopper channel was closed.
	ErrStopped = errors.New("quicraft: operation stopped")

	// ErrReadIndexNotReady is returned when a ReadIndex request cannot
	// be served because the leader has not yet committed an entry in
	// its current term (PhD thesis 6.4).
	ErrReadIndexNotReady = errors.New("quicraft: read index not ready")

	// ErrDraining is returned when a new proposal or read-index request
	// is submitted to a host that has entered drain mode. In-flight
	// operations are allowed to complete, but no new work is accepted.
	ErrDraining = errors.New("quicraft: host is draining")

	// ErrInvalidSession is returned when a session is not valid for the
	// requested operation, e.g. wrong shard ID or a no-op session used
	// for a session operation.
	ErrInvalidSession = errors.New("quicraft: invalid session")

	// ErrSnapshotSizeMismatch is returned when the snapshot file size
	// on disk does not match the expected FileSize from the message.
	ErrSnapshotSizeMismatch = errors.New("quicraft: snapshot file size mismatch")

	// ErrSnapshotCorrupt is returned when an encrypted snapshot frame has a
	// corrupt/oversized length prefix during decryption (send/export path).
	ErrSnapshotCorrupt = errors.New("quicraft: snapshot frame corrupt")

	// ErrShardNotStopped is returned when RemoveData is called for a
	// shard that is still loaded on this host.
	ErrShardNotStopped = errors.New("quicraft: shard not stopped")

	// ErrStaleConfigChange is returned when a config change request
	// carries a ConfigChangeID that does not match the shard's current
	// configuration version. Callers should re-read the membership via
	// SyncGetShardMembership and retry with the fresh ConfigChangeID.
	ErrStaleConfigChange = errors.New("quicraft: stale config change")

	// ErrReplicaNotFound is returned when an operation references a
	// replica ID that does not match the replica running on this host.
	ErrReplicaNotFound = errors.New("quicraft: replica not found")

	// ErrInvalidRange is returned when a log query specifies an invalid
	// index range, such as firstIndex >= lastIndex.
	ErrInvalidRange = errors.New("quicraft: invalid range")

	// ErrInvalidMember is the sentinel form of InvalidMemberError, returned
	// when a member in the initial members map has a zero replica ID or an
	// empty address.
	ErrInvalidMember = errors.New("quicraft: invalid member")

	// ErrSnapshotTooLarge is the sentinel form of SnapshotTooLargeError,
	// returned when an imported snapshot artifact exceeds the configured
	// MaxSnapshotSize.
	ErrSnapshotTooLarge = errors.New("quicraft: snapshot too large")
)

// Membership validation sentinels surfaced by the membership-change API
// (RequestAddWitness, RequestRemoveNode, and their Sync variants). These
// alias the internal raft sentinels so external callers can match the
// rejections with errors.Is instead of string comparison.
var (
	// ErrTooManyWitnesses is returned when a witness add would exceed the
	// shard's WitnessConfig.MaxWitnesses cap.
	ErrTooManyWitnesses = raft.ErrTooManyWitnesses

	// ErrBreaksFullReplicaMajority is returned when a config change —
	// adding a witness or removing a full replica — would leave the full
	// replicas unable to form a quorum, violating the
	// WitnessConfig.RequireFullReplicaMajority policy.
	ErrBreaksFullReplicaMajority = raft.ErrBreaksFullReplicaMajority

	// ErrRemoveLastFullReplica is returned when a config change would
	// remove the shard's only full replica; witnesses cannot become
	// leader, so the shard would be permanently headless.
	ErrRemoveLastFullReplica = raft.ErrRemoveLastFullReplica
)

// NotLeaderError is returned when a proposal targets a non-leader node.
// It carries the current leader's identity so clients can redirect
// immediately without a separate GetShardInfo() call.
type NotLeaderError struct {
	ShardID       uint64
	LeaderID      uint64 // 0 if no known leader
	LeaderAddress string // empty if no known leader
}

// Error returns a human-readable description of the not-leader condition.
func (e *NotLeaderError) Error() string {
	if e.LeaderID == 0 {
		return fmt.Sprintf("quicraft: not leader for shard %d, leader unknown", e.ShardID)
	}
	return fmt.Sprintf(
		"quicraft: not leader for shard %d, leader is replica %d at %s",
		e.ShardID, e.LeaderID, e.LeaderAddress,
	)
}

// Is reports whether target matches the sentinel ErrNotLeader error.
// This enables errors.Is(notLeaderErr, ErrNotLeader) to return true.
func (e *NotLeaderError) Is(target error) bool {
	return target == ErrNotLeader
}

// EntryTooLargeError is returned when a proposal exceeds the configured
// MaxEntrySize. It includes the actual size and the limit for diagnostics.
type EntryTooLargeError struct {
	Size    uint64
	MaxSize uint64
}

// Error returns a human-readable description including size details.
func (e *EntryTooLargeError) Error() string {
	return fmt.Sprintf(
		"quicraft: entry too large (%d bytes, max %d bytes)",
		e.Size, e.MaxSize,
	)
}

// Is reports whether target matches the sentinel ErrEntryTooLarge error.
func (e *EntryTooLargeError) Is(target error) bool {
	return target == ErrEntryTooLarge
}

// SnapshotTooLargeError is returned when an imported snapshot artifact
// exceeds the configured MaxSnapshotSize. MaxSize is the configured limit;
// the artifact is rejected as soon as the limit is exceeded, so its full
// size is not known and is not reported.
type SnapshotTooLargeError struct {
	ShardID uint64
	MaxSize uint64
}

// Error returns a human-readable description including the limit.
func (e *SnapshotTooLargeError) Error() string {
	return fmt.Sprintf(
		"quicraft: snapshot for shard %d exceeds the maximum snapshot size of %d bytes",
		e.ShardID, e.MaxSize,
	)
}

// Is reports whether target matches the sentinel ErrSnapshotTooLarge error.
func (e *SnapshotTooLargeError) Is(target error) bool {
	return target == ErrSnapshotTooLarge
}

// BusyError is returned when the in-memory log exceeds MaxInMemLogSize
// for a specific shard, or when the global rate limiter triggers. It
// carries diagnostic information about the current and maximum sizes.
// The optional message field allows global-level busy errors (e.g.,
// ErrSystemBusy) to carry a distinct message while still satisfying
// errors.Is(err, ErrBusy).
type BusyError struct {
	ShardID     uint64
	CurrentSize uint64
	MaxSize     uint64
	message     string // override message; if empty, default format is used
}

// Error returns a human-readable description including shard and size details.
// If a custom message was set (e.g., for the global rate limiter sentinel),
// it is returned verbatim.
func (e *BusyError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf(
		"quicraft: system busy for shard %d (current %d bytes, max %d bytes)",
		e.ShardID, e.CurrentSize, e.MaxSize,
	)
}

// Is reports whether target matches the sentinel ErrBusy error.
func (e *BusyError) Is(target error) bool {
	return target == ErrBusy
}

// SnapshotSendError is returned when a snapshot send operation fails.
// It carries the shard ID, target replica, and the failing operation.
type SnapshotSendError struct {
	ShardID   uint64
	ReplicaID uint64
	Op        string
	Err       error
}

// Error returns a human-readable description of the snapshot send failure.
func (e *SnapshotSendError) Error() string {
	return fmt.Sprintf(
		"quicraft: snapshot send %s failed for shard %d replica %d: %v",
		e.Op, e.ShardID, e.ReplicaID, e.Err,
	)
}

// Unwrap returns the underlying error.
func (e *SnapshotSendError) Unwrap() error {
	return e.Err
}

// SnapshotReceiveError is returned when a snapshot receive operation fails.
// It carries the shard ID, replica ID, and the failing operation.
type SnapshotReceiveError struct {
	ShardID   uint64
	ReplicaID uint64
	Op        string
	Err       error
}

// Error returns a human-readable description of the snapshot receive failure.
func (e *SnapshotReceiveError) Error() string {
	return fmt.Sprintf(
		"quicraft: snapshot receive %s failed for shard %d replica %d: %v",
		e.Op, e.ShardID, e.ReplicaID, e.Err,
	)
}

// Unwrap returns the underlying error.
func (e *SnapshotReceiveError) Unwrap() error {
	return e.Err
}

// UnauthorizedMessageError is returned when a message is received from
// a replica that is not a member of the target shard. This prevents
// compromised nodes from injecting messages into shards they do not
// belong to, enforcing per-shard message authorization at the transport
// boundary.
type UnauthorizedMessageError struct {
	ShardID uint64
	From    uint64
	MsgType uint64
}

// Error returns a human-readable description of the unauthorized message.
func (e *UnauthorizedMessageError) Error() string {
	return fmt.Sprintf(
		"quicraft: unauthorized message from replica %d to shard %d (type=%d)",
		e.From, e.ShardID, e.MsgType,
	)
}

// SessionError is returned when a session operation fails. It carries
// the client ID and operation name for diagnostics.
type SessionError struct {
	ClientID uint64
	Op       string
	Err      error
}

// Error returns a human-readable description of the session error.
func (e *SessionError) Error() string {
	return fmt.Sprintf(
		"quicraft: session %s failed for client %d: %v",
		e.Op, e.ClientID, e.Err,
	)
}

// Unwrap returns the underlying error.
func (e *SessionError) Unwrap() error {
	return e.Err
}

// SessionExpiredError is returned when a session-managed proposal references
// a clientID whose session is no longer registered. It carries the ClientID
// and ShardID for diagnostics and client-side retry logic.
type SessionExpiredError struct {
	ShardID  uint64
	ClientID uint64
}

// Error returns a human-readable description of the expired session.
func (e *SessionExpiredError) Error() string {
	return fmt.Sprintf(
		"quicraft: session expired for client %d on shard %d",
		e.ClientID, e.ShardID,
	)
}

// Is reports whether target matches the sentinel ErrSessionExpired error.
// This enables errors.Is(sessionExpiredErr, ErrSessionExpired) to return true.
func (e *SessionExpiredError) Is(target error) bool {
	return target == ErrSessionExpired
}

// ShardNotStoppedError is returned when RemoveData is called for a shard/replica
// pair that is still loaded on this host. The shard must be stopped first.
type ShardNotStoppedError struct {
	ShardID   uint64
	ReplicaID uint64
}

// Error returns a human-readable description.
func (e *ShardNotStoppedError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d is still running, stop it first",
		e.ShardID, e.ReplicaID,
	)
}

// Is reports whether target matches the sentinel ErrShardNotStopped error.
func (e *ShardNotStoppedError) Is(target error) bool {
	return target == ErrShardNotStopped
}

// ReplicaNotFoundError is returned when an operation targets a replica ID
// that does not match the replica running on this host for the given shard.
type ReplicaNotFoundError struct {
	ShardID   uint64
	ReplicaID uint64
}

// Error returns a human-readable description.
func (e *ReplicaNotFoundError) Error() string {
	return fmt.Sprintf(
		"quicraft: replica %d not found on shard %d on this host",
		e.ReplicaID, e.ShardID,
	)
}

// Is reports whether target matches the sentinel ErrReplicaNotFound error.
func (e *ReplicaNotFoundError) Is(target error) bool {
	return target == ErrReplicaNotFound
}

// StaleConfigChangeError is returned when a config change request carries
// a ConfigChangeID that does not match the shard's current configuration
// version. This prevents stale config changes from being proposed when
// two clients race to modify the membership.
type StaleConfigChangeError struct {
	ShardID  uint64
	Expected uint64
	Got      uint64
}

// Error returns a human-readable description.
func (e *StaleConfigChangeError) Error() string {
	return fmt.Sprintf(
		"quicraft: stale config change on shard %d: expected ConfigChangeID %d, got %d",
		e.ShardID, e.Expected, e.Got,
	)
}

// Is reports whether target matches the sentinel ErrStaleConfigChange error.
func (e *StaleConfigChangeError) Is(target error) bool {
	return target == ErrStaleConfigChange
}

// LoopPanicError is returned when a background loop (key rotation,
// pending sweep) recovers from a panic. It captures the loop name,
// the recovered value, and the stack trace for diagnostics.
type LoopPanicError struct {
	LoopName string
	Value    any
	Stack    []byte
}

// Error returns a human-readable description of the loop panic.
func (e *LoopPanicError) Error() string {
	return fmt.Sprintf(
		"quicraft: %s loop panicked: %v",
		e.LoopName, e.Value,
	)
}

// EmptyMembersError is returned when StartShard is called with join=false
// and an empty members map. A non-joining shard requires at least one
// initial member to form a quorum.
type EmptyMembersError struct {
	ShardID uint64
}

// Error returns a human-readable description.
func (e *EmptyMembersError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d cannot start with empty members (join=false)",
		e.ShardID,
	)
}

// SelfNotInMembersError is returned when StartShard is called with
// join=false and the configured ReplicaID is not present in the
// initial members map. The local replica must be a member of the
// initial cluster configuration.
type SelfNotInMembersError struct {
	ShardID   uint64
	ReplicaID uint64
}

// Error returns a human-readable description.
func (e *SelfNotInMembersError) Error() string {
	return fmt.Sprintf(
		"quicraft: replica %d not found in initial members for shard %d",
		e.ReplicaID, e.ShardID,
	)
}

// InvalidMemberError is returned when a member in the initial members
// map has an invalid replicaID (zero) or an empty address.
type InvalidMemberError struct {
	ShardID   uint64
	ReplicaID uint64
	Address   string
	Reason    string
}

// Error returns a human-readable description.
func (e *InvalidMemberError) Error() string {
	return fmt.Sprintf(
		"quicraft: invalid member in shard %d: replica %d address %q: %s",
		e.ShardID, e.ReplicaID, e.Address, e.Reason,
	)
}

// Is reports whether target matches the sentinel ErrInvalidMember error.
func (e *InvalidMemberError) Is(target error) bool {
	return target == ErrInvalidMember
}

// ErrNonVotingRoleInMembers is the sentinel for *NonVotingRoleInMembersError.
var ErrNonVotingRoleInMembers = errors.New(
	"quicraft: a witness or observer cannot be bootstrapped through the initial members map")

// NonVotingRoleInMembersError is returned by Host.StartShard when the local
// replica is configured as a witness (Config.IsWitness) or an observer
// (Config.IsObserver) and also appears in the initial members map.
//
// The members map seeds proto.Membership.Addresses — the FULL VOTER set — and
// membership is REPLICATED state: every peer seeds the same replica into its
// own Addresses from its own copy of that map, so one replica cannot
// unilaterally hold a different role for itself than its peers hold for it.
// A witness bootstrapped this way is therefore a full voter everywhere:
//
//   - the leader replicates FULL entry payloads to it (sendAppend), never the
//     metadata-only entries sendWitnessAppend produces for
//     Membership.Witnesses, so it consumes the disk of a full replica;
//   - it counts as a full replica for the replicated
//     RequireFullReplicaMajority policy, inflating the shard's apparent
//     durability;
//   - the only behaviour that still looks like a witness is this host's local
//     propose/read gate, which refuses exactly the reads the operator expects
//     the replica to serve;
//   - and the raft layer permanently latches the disagreement (roleMismatch),
//     after which the replica can neither campaign nor serve reads.
//
// An observer bootstrapped this way is likewise a full VOTER: it is counted in
// every quorum and it grants votes, while the local Config.IsObserver gate only
// stops it from campaigning. The operator asks for a non-voting learner and
// silently gets a voter that never stands for election — the voting set is
// larger than the operator believes, so the shard tolerates fewer failures than
// intended.
//
// The supported flow is the two-step one etcd and dragonboat use for learners:
// bootstrap the full voters, then start the witness/observer with join=true and
// add it with Host.RequestAddWitness / Host.RequestAddObserver, which writes it
// into Membership.Witnesses / Membership.Observers through a committed config
// change every replica applies identically.
type NonVotingRoleInMembersError struct {
	ShardID   uint64
	ReplicaID uint64
	// Role is the non-voting role the local Config declared: "witness" or
	// "observer". Config.Validate already rejects both at once.
	Role string
}

// Error returns a human-readable description.
func (e *NonVotingRoleInMembersError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d is configured as a %s but is listed in the initial "+
			"members map, which seeds the full voting membership; bootstrap the voters "+
			"first, then start this replica with join=true and add it through a config change",
		e.ShardID, e.ReplicaID, e.Role,
	)
}

// Is reports whether target matches the sentinel ErrNonVotingRoleInMembers.
func (e *NonVotingRoleInMembersError) Is(target error) bool {
	return target == ErrNonVotingRoleInMembers
}

// TypeAssertionError is returned when a type assertion fails on data
// retrieved from sync.Map or other interface{} sources. This indicates
// an internal programming error where the stored type does not match
// the expected type.
type TypeAssertionError struct {
	Context      string // description of where the assertion occurred
	ExpectedType string // the type that was expected
	ActualValue  any    // the actual value retrieved (may be nil)
}

// Error returns a human-readable description of the type assertion failure.
func (e *TypeAssertionError) Error() string {
	return fmt.Sprintf(
		"quicraft: type assertion failed in %s: expected %s, got %T",
		e.Context, e.ExpectedType, e.ActualValue,
	)
}

// ErrImportLayoutUnknown is the sentinel for
// *ImportLayoutUnknownError.
var ErrImportLayoutUnknown = errors.New("quicraft: snapshot import cannot determine the shard's on-disk layout")

// ImportLayoutUnknownError is returned by Host.ImportSnapshot when the
// shard's snapshot-tree root can be neither discovered nor asserted.
//
// StartShard records Config.DataDir in the shard's LogDB bootstrap record, so
// the layout is normally discovered. A shard started with join=true records no
// bootstrap, and a record written before the field existed carries no DataDir;
// in those cases the import must be told explicitly with WithImportDataDir
// (pass "" to assert the shard has no DataDir).
//
// It fails closed rather than assuming NodeHostDir because a wrong assumption
// is silent and destructive: the artifact lands outside the tree that snapshot
// garbage collection and the DEK purge-floor scan enumerate, so the next
// PurgeEpochsBefore destroys the key it needs and the restore is unrecoverable.
type ImportLayoutUnknownError struct {
	ShardID   uint64
	ReplicaID uint64
}

// Error returns a human-readable description of the unknown layout.
func (e *ImportLayoutUnknownError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d has no recorded DataDir; pass WithImportDataDir "+
			"to state the shard's Config.DataDir (or WithImportDataDir(\"\") if it has none)",
		e.ShardID, e.ReplicaID,
	)
}

// Is reports whether target matches the sentinel ErrImportLayoutUnknown.
func (e *ImportLayoutUnknownError) Is(target error) bool {
	return target == ErrImportLayoutUnknown
}

// ErrImportDataDirMismatch is the sentinel for *ImportDataDirMismatchError.
var ErrImportDataDirMismatch = errors.New("quicraft: snapshot import DataDir disagrees with the shard's recorded DataDir")

// ImportDataDirMismatchError is returned by Host.ImportSnapshot when the
// caller's WithImportDataDir contradicts the DataDir StartShard recorded for
// the shard.
//
// One of the two is wrong and the import cannot tell which, so it refuses
// instead of choosing. Choosing wrongly writes the artifact into a tree the
// shard will not scan, which leaks it and lets the next PurgeEpochsBefore
// destroy its DEK.
type ImportDataDirMismatchError struct {
	ShardID   uint64
	ReplicaID uint64
	Declared  string
	Recorded  string
}

// Error returns a human-readable description of the disagreement.
func (e *ImportDataDirMismatchError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d was started with DataDir %q but the import declares %q",
		e.ShardID, e.ReplicaID, e.Recorded, e.Declared,
	)
}

// Is reports whether target matches the sentinel ErrImportDataDirMismatch.
func (e *ImportDataDirMismatchError) Is(target error) bool {
	return target == ErrImportDataDirMismatch
}

// ImportSnapshotOutOfDateError is returned by Host.ImportSnapshot when the
// artifact's snapshot index is not NEWER than the snapshot the shard already
// has recorded in LogDB.
//
// The check runs BEFORE the import touches the shard's snapshot tree. Every
// production LogDB (memdb, waldb) refuses such a save with
// logdb.ErrSnapshotOutOfDate, so leaving the check to the final SaveSnapshot
// meant the artifact had already been written — over the LIVE snapshot
// directory, whose name is the same index — by the time the refusal arrived.
// The operator saw an error and believed nothing had happened while the LogDB
// record described a file it no longer matched: a wrong FileSize ships a short
// snapshot that erases the receiving follower's state, and a wrong Epoch makes
// StartShard fail closed on an undecryptable snapshot.
//
// It matches logdb.ErrSnapshotOutOfDate under errors.Is so a caller sees one
// error identity whichever layer refused.
type ImportSnapshotOutOfDateError struct {
	ShardID   uint64
	ReplicaID uint64
	// Index is the artifact's snapshot index.
	Index uint64
	// Recorded is the index of the snapshot already recorded in LogDB.
	Recorded uint64
}

// Error returns a human-readable description of the stale import.
func (e *ImportSnapshotOutOfDateError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d already has snapshot index %d recorded; "+
			"importing index %d would not advance it",
		e.ShardID, e.ReplicaID, e.Recorded, e.Index,
	)
}

// Is reports whether target matches logdb.ErrSnapshotOutOfDate, the sentinel
// every LogDB backend returns for the same condition.
func (e *ImportSnapshotOutOfDateError) Is(target error) bool {
	return target == logdb.ErrSnapshotOutOfDate
}

// ErrImportSnapshotDirExists is the sentinel for *ImportSnapshotDirExistsError.
var ErrImportSnapshotDirExists = errors.New("quicraft: snapshot import destination directory already exists")

// ImportSnapshotDirExistsError is returned by Host.ImportSnapshot when the
// canonical directory for the artifact's index is already present on disk.
//
// Snapshot directories are named by the snapshot's own index so the tree ORDERS
// correctly, which means an import can collide with a directory it did not
// create — an operator retry of the same artifact, a disaster-recovery artifact
// exported from a peer at an index this replica also reached, or the remains of
// an import that died before it finished. The import refuses instead of
// reusing or removing it: the directory may be the shard's live snapshot, and
// both writing into it and deleting it destroy data the LogDB still references.
//
// Clearing it is an operator decision. An import NEVER deletes a directory it
// did not create.
type ImportSnapshotDirExistsError struct {
	ShardID   uint64
	ReplicaID uint64
	Index     uint64
	Dir       string
}

// Error returns a human-readable description of the collision.
func (e *ImportSnapshotDirExistsError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d snapshot directory %q for index %d already exists; "+
			"the import will not overwrite a directory it did not create",
		e.ShardID, e.ReplicaID, e.Dir, e.Index,
	)
}

// Is reports whether target matches the sentinel ErrImportSnapshotDirExists.
func (e *ImportSnapshotDirExistsError) Is(target error) bool {
	return target == ErrImportSnapshotDirExists
}

// temporaryErrors is the complete set of sentinels Temporary reports true for.
//
// Membership is decided per sentinel, never by pattern: an error belongs here
// only when re-submitting the IDENTICAL request, unchanged, can succeed once a
// transient condition clears without any operator or caller intervention.
// Every other sentinel in this package is permanent by construction — see
// Temporary's doc comment for why the default falls that way.
//
// Sentinels deliberately excluded, with the reason, because each one looks
// retryable at a glance:
//
//   - ErrCanceled: the caller cancelled. Nothing on this host will change; a
//     retry is the caller's own decision, not a property of the error. A
//     deadline that elapses server-side surfaces as ErrTimeout, which IS
//     temporary.
//   - ErrDraining: drain is a one-way transition into shutdown. Retrying this
//     host never succeeds; the request must go to a different host.
//   - ErrStaleConfigChange: the request carries a ConfigChangeID that is now
//     stale, so the identical request fails forever. The caller must re-read
//     membership via SyncGetShardMembership and build a NEW request.
//   - ErrSessionLimitExceeded: the shard is at its session cap. Whether that
//     ever clears depends on Config.SessionExpiryEntries and on clients
//     closing sessions; a retry loop against a client that leaks sessions
//     spins forever.
//   - ErrBarrierSealed: clearing it requires an operator unseal ceremony.
//   - ErrShardBarrierUnresolved: a resolver miss can mean "tenant not
//     provisioned yet" (clears) or "shard is misconfigured" (never clears),
//     and the error cannot tell the two apart, so it fails fast.
//   - ErrDiskFull, ErrLogDBWrite: storage faults that need operator action.
//     Retrying writes into a full or failing volume makes the incident worse.
//   - ErrShardFailed: the state-machine circuit breaker latched after repeated
//     apply failures and does not reset on its own.
var temporaryErrors = [...]error{
	// Rate limiting and backpressure. ErrSystemBusy is a *BusyError that
	// matches ErrBusy, so the global limiter is covered by the same entry.
	ErrBusy,
	// Deadline elapsed; the shard may simply have been slow.
	ErrTimeout,
	// Interrupted by an internal state transition such as a leadership
	// change. The next attempt runs against the new state.
	ErrAborted,
	// Queue overflow. If the drop was caused by shutdown instead, the retry
	// gets the permanent ErrClosed and the caller stops.
	ErrDropped,
	// Shard is loaded but still initializing; it becomes ready on its own.
	ErrShardNotStarted,
	ErrShardNotReady,
	// Wrong replica. NotLeader reports where to go instead.
	ErrNotLeader,
	// The leader has not yet committed an entry in its current term
	// (Raft PhD 6.4). It will, within one heartbeat interval.
	ErrReadIndexNotReady,
	// Another rotation or snapshot holds the single-flight slot.
	ErrRotateInProgress,
	ErrSnapshotInProgress,
}

// Temporary reports whether err represents a transient condition that may
// succeed on retry. A nil error is not temporary.
//
// Temporary is true only for the conditions that clear on their own: rate
// limiting (ErrBusy, ErrSystemBusy), an elapsed deadline (ErrTimeout), an
// interrupted operation (ErrAborted, ErrDropped), a shard that is still
// coming up (ErrShardNotStarted, ErrShardNotReady), a request that reached
// the wrong replica (ErrNotLeader), a leader that has not yet committed in
// its term (ErrReadIndexNotReady), and a single-flight slot that is already
// taken (ErrRotateInProgress, ErrSnapshotInProgress).
//
// EVERY other error is reported as permanent, including errors this package
// did not produce. That default is deliberate: retrying a permanent failure
// forever is worse than failing fast, because it converts one bad request
// into sustained load and hides the real fault. A caller that needs a
// different answer for its own error type should test that type first and
// fall back to Temporary.
//
// Wrapped errors classify correctly: Temporary uses errors.Is, so an error
// wrapped with %w, or a typed error whose Is method matches a sentinel
// (*BusyError, *NotLeaderError), is recognised.
func Temporary(err error) bool {
	if err == nil {
		return false
	}
	for _, t := range temporaryErrors {
		if errors.Is(err, t) {
			return true
		}
	}
	return false
}

// NotLeader reports whether err indicates this replica is not the leader,
// and returns the known leader's replica ID when the error carries one.
//
// ok is true for any not-leader error, whether it is the bare ErrNotLeader
// sentinel or a *NotLeaderError. replicaID is the leader this replica
// believes is current, or 0 when the leader is unknown — an election in
// progress, or a bare sentinel that carries no identity. Callers must
// therefore treat (0, true) as "retry, but discover the leader yourself"
// rather than as an address to redirect to.
//
// A nil error returns (0, false).
//
// Every not-leader error is also Temporary, so a retry loop can gate on
// Temporary and use NotLeader only to pick a target.
func NotLeader(err error) (replicaID uint64, ok bool) {
	if err == nil {
		return 0, false
	}
	var notLeader *NotLeaderError
	if errors.As(err, &notLeader) {
		return notLeader.LeaderID, true
	}
	if errors.Is(err, ErrNotLeader) {
		return 0, true
	}
	return 0, false
}
