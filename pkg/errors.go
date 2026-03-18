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
