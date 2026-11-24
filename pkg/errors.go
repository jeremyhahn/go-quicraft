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

	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/rsm"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// Data-directory ownership errors, returned by NewHost before it touches the
// log or the transport.
var (
	// ErrDirLocked is returned, wrapped with the lock path, when NodeHostDir or WALDir is already held; it equals fsutil.ErrDirLocked.
	ErrDirLocked = fsutil.ErrDirLocked

	// ErrHostIdentityMismatch is returned when NodeHostDir records a different DeploymentID from the configured one.
	ErrHostIdentityMismatch = errors.New("quicraft: NodeHostDir belongs to a different deployment")
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

	// ErrAtRestEncryptionUnspecified is returned by NewHost when neither a barrier nor WithoutEncryption is configured.
	ErrAtRestEncryptionUnspecified = errors.New(
		"quicraft: at-rest encryption is unspecified: configure WithBarrier " +
			"or WithShardBarriers, or declare WithoutEncryption")

	// ErrAtRestEncryptionConflict is returned by NewHost when WithoutEncryption is combined with WithBarrier.
	ErrAtRestEncryptionConflict = errors.New(
		"quicraft: WithoutEncryption cannot be combined with WithBarrier")

	// ErrBarrierPlaintextLogDB is returned by NewHost when WithBarrier is combined with a supplied LogDB that does not report barrier-encrypted, non-durable or host-routed through that barrier.
	ErrBarrierPlaintextLogDB = errors.New(
		"quicraft: WithBarrier with a supplied LogDB requires that LogDB to be " +
			"encrypted at rest (logdb.AtRestProtectedLogDB reporting barrier_encrypted " +
			"or non_durable) or host_routed through the WithBarrier barrier itself; " +
			"this LogDB would store plaintext")

	// ErrShardBarriersPlaintextWAL is returned by NewHost when WithShardBarriers would leave the WAL in plaintext; supply an encrypting LogDB or WithBarrier.
	ErrShardBarriersPlaintextWAL = errors.New(
		"quicraft: WithShardBarriers requires a LogDB that is encrypted at rest " +
			"(logdb.AtRestProtectedLogDB reporting barrier_encrypted or non_durable, " +
			"or host_routed under WithoutEncryption) or WithBarrier (host-wide WAL " +
			"key); this LogDB would store plaintext")

	// ErrShardBarrierUnresolved is returned when the shard barrier resolver returns nil and no host-wide route (WithBarrier or WithoutEncryption) is declared.
	ErrShardBarrierUnresolved = engine.ErrShardBarrierUnresolved

	// ErrShardBarrierLogDBMismatch is returned when a shard's resolved barrier is not the one the LogDB stores its records under.
	ErrShardBarrierLogDBMismatch = errors.New(
		"quicraft: the supplied LogDB does not store this shard's records under " +
			"the barrier WithShardBarriers resolved for it")

	// ErrDropped is returned when the engine drops a request before it reached Raft; it is Temporary and never MaybeApplied.
	ErrDropped = engine.ErrDropped

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

	// ErrShardFailed is returned when the shard's failure latch has tripped and it is offline until reloaded or recovered.
	ErrShardFailed = engine.ErrShardFailed

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

	// ErrRecoveredTermUnknown is returned when StartShard cannot resolve the term of an on-disk state machine's recovered index.
	ErrRecoveredTermUnknown = errors.New("quicraft: recovered applied index has no resolvable raft term")

	// ErrShutdownTimeout is returned when Host.Close() exceeds the
	// configured ShutdownTimeout waiting for graceful shutdown.
	ErrShutdownTimeout = errors.New("quicraft: shutdown timeout exceeded")

	// ErrSystemBusy is a *BusyError returned when the global in-memory log size exceeds MaxTotalInMemLogSize.
	ErrSystemBusy error = &BusyError{message: "quicraft: system busy, global rate limit exceeded"}

	// ErrDecompressionBomb is returned when a decompressed payload exceeds the configured maximum; committed, it fails the batch.
	ErrDecompressionBomb = raft.ErrDecompressionBomb

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

	// ErrShardPaused is returned by every proposal, read, session,
	// membership, snapshot and leader-transfer entry point of a shard that
	// Host.PauseShard paused, until Host.ResumeShard. It is Temporary.
	ErrShardPaused = engine.ErrShardPaused

	// ErrBarrierNotInit is returned when the barrier has not been
	// initialized with a sealing strategy. Re-exported from seal package.
	ErrBarrierNotInit = seal.ErrBarrierNotInit

	// ErrDiskFull is returned when the WAL directory has reached its
	// configured maximum size.
	ErrDiskFull = errors.New("quicraft: WAL disk full")

	// ErrDecryptionFailed is returned when a decryption operation fails,
	// typically due to a corrupted ciphertext or incorrect key. Re-exported from seal package.
	ErrDecryptionFailed = seal.ErrDecryptionFailed

	// ErrSnapshotRecoveryFailed is returned, as *SnapshotFailedError, when snapshot recovery fails.
	ErrSnapshotRecoveryFailed = errors.New("quicraft: snapshot recovery failed")

	// ErrEntryTooLarge is the sentinel form of EntryTooLargeError, used
	// for errors.Is() checks without needing the structured fields.
	ErrEntryTooLarge = errors.New("quicraft: entry too large")

	// ErrSessionNotFound is returned when an operation references a
	// client session that does not exist.
	ErrSessionNotFound = errors.New("quicraft: session not found")

	// ErrSessionExpired is returned when a session-managed proposal references an unregistered clientID (Raft thesis figure 6.1).
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

	// ErrImportInProgress is returned by StartShard while an ImportSnapshot for the shard is running.
	ErrImportInProgress = errors.New("quicraft: snapshot import in progress for shard")

	// ErrStopped is returned when the shard or engine stopped underneath an operation; ErrClosed means it never started.
	ErrStopped = errors.New("quicraft: operation stopped")

	// ErrReadIndexNotReady is returned when a ReadIndex request cannot
	// be served because the leader has not yet committed an entry in
	// its current term (PhD thesis 6.4).
	ErrReadIndexNotReady = errors.New("quicraft: read index not ready")

	// ErrNoLeader is the cause a LeaderUnavailableError carries when
	// ExecuteOnLeader's context ended while the shard had no known leader.
	ErrNoLeader = errors.New("quicraft: shard has no leader")

	// ErrLeadershipChanged is the LeaderUnavailableError cause when ExecuteOnLeader timed out during a leadership change.
	ErrLeadershipChanged = errors.New("quicraft: leadership changed during a leader-executed request")

	// ErrLeaderHandlerNotRegistered is returned by ExecuteOnLeader when the
	// shard's leader has no LeaderHandler registered for the shard (see
	// Host.RegisterLeaderHandler). The request was not executed.
	ErrLeaderHandlerNotRegistered = errors.New("quicraft: no leader handler registered for shard")

	// ErrDraining is returned when a new proposal or read-index request
	// is submitted to a host that has entered drain mode. In-flight
	// operations are allowed to complete, but no new work is accepted.
	ErrDraining = errors.New("quicraft: host is draining")

	// ErrInvalidSession is returned when a session is not valid for the
	// requested operation, e.g. wrong shard ID, or a no-op session passed
	// to ProposeWithSession or to a session register/unregister operation.
	ErrInvalidSession = errors.New("quicraft: invalid session")

	// ErrSessionBusy is returned when a session already has a proposal in flight; resubmit once it completes.
	ErrSessionBusy = errors.New("quicraft: session has a proposal in flight")

	// ErrSnapshotSizeMismatch is returned when a snapshot file's size differs from the message's or the LogDB record's FileSize.
	ErrSnapshotSizeMismatch = engine.ErrSnapshotSizeMismatch

	// ErrSnapshotCorrupt is returned when snapshot data or a frame does not match its recorded length and hash.
	ErrSnapshotCorrupt = engine.ErrSnapshotCorrupt

	// ErrSnapshotDestinationRecorded is returned when a received snapshot would replace the live snapshot directory LogDB records.
	ErrSnapshotDestinationRecorded = errors.New(
		"quicraft: received snapshot's destination is the snapshot LogDB currently records")

	// ErrShardNotStopped is returned when RemoveData is called for a
	// shard that is still loaded on this host.
	ErrShardNotStopped = errors.New("quicraft: shard not stopped")

	// ErrReplicaRetired is returned when StartShard targets a replica whose
	// data RemoveData removed on this host. See ReplicaRetiredError.
	ErrReplicaRetired = errors.New("quicraft: replica ID retired")

	// ErrStaleConfigChange is returned when ConfigChangeID is stale; re-read with SyncGetShardMembership and retry.
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

// Membership validation sentinels: aliases of the internal raft values, matchable with errors.Is.
var (
	// ErrTooManyWitnesses is returned when a witness add would exceed the
	// shard's WitnessConfig.MaxWitnesses cap.
	ErrTooManyWitnesses = raft.ErrTooManyWitnesses

	// ErrBreaksFullReplicaMajority is returned when a config change would break WitnessConfig.RequireFullReplicaMajority.
	ErrBreaksFullReplicaMajority = raft.ErrBreaksFullReplicaMajority

	// ErrRemoveLastFullReplica is returned when a config change would
	// remove the shard's only full replica; witnesses cannot become
	// leader, so the shard would be permanently headless.
	ErrRemoveLastFullReplica = raft.ErrRemoveLastFullReplica

	// ErrNodeNotFound is returned when a removal targets a replica that is
	// not in the shard's current membership. The removal is refused rather
	// than committed as a no-op, so the caller learns its view is stale.
	ErrNodeNotFound = raft.ErrNodeNotFound

	// ErrNodeAlreadyExists is returned when an add targets a replica that is
	// already a member of the shard in the role being requested.
	ErrNodeAlreadyExists = raft.ErrNodeAlreadyExists

	// ErrNodeRemoved is returned when an add targets a removed replica ID; IDs are never recycled.
	ErrNodeRemoved = raft.ErrNodeRemoved

	// ErrRemoveLastVoter is returned when a config change would remove the
	// shard's last voting member, leaving it unable to elect a leader.
	ErrRemoveLastVoter = raft.ErrRemoveLastVoter

	// ErrTooManyVotingMembers is returned when an add would exceed 64 voting members.
	ErrTooManyVotingMembers = raft.ErrTooManyVotingMembers

	// ErrZeroReplicaID is returned when a config change names replica ID 0,
	// which is the reserved "no replica" value and can never be a member.
	ErrZeroReplicaID = raft.ErrZeroReplicaID

	// ErrEmptyAddress is returned when an add carries an empty address. A
	// member with no address is unreachable and cannot be replicated to.
	ErrEmptyAddress = raft.ErrEmptyAddress

	// ErrRemovedMemberIdentity is returned when an add would bind a node identity the shard removed; use a new keypair.
	ErrRemovedMemberIdentity = raft.ErrRemovedMemberIdentity

	// ErrSessionPolicyMismatch is returned when this node's session settings disagree with the shard's committed session policy.
	ErrSessionPolicyMismatch = rsm.ErrSessionPolicyMismatch

	// ErrDuplicateMemberAddress is returned when an add names an address another active member holds.
	ErrDuplicateMemberAddress = raft.ErrDuplicateMemberAddress

	// ErrDuplicateMemberIdentity is returned when an add would bind a node identity another active member holds.
	ErrDuplicateMemberIdentity = raft.ErrDuplicateMemberIdentity

	// ErrMemberIdentityRequired is returned when a member would be added without a node identity on a host with a transport.
	ErrMemberIdentityRequired = raft.ErrMemberIdentityRequired

	// ErrTransferInProgress is returned while a leadership transfer is in flight; it is Temporary and never MaybeApplied.
	ErrTransferInProgress = raft.ErrTransferInProgress
)

// ErrSharedPeerCertificate is returned when this host's certificate also covers another member's host; re-issue certificates per node.
var ErrSharedPeerCertificate = transport.ErrSharedPeerCertificate

// SharedPeerCertificateError is the error behind ErrSharedPeerCertificate, an alias of the transport type.
type SharedPeerCertificateError = transport.SharedPeerCertificateError

// ErrMTLSReload is returned by Host.ReloadMTLS when the new certificate
// material was refused and the previous material stays in force. The
// concrete error is a *MTLSReloadError wrapping the check that failed.
var ErrMTLSReload = transport.ErrMTLSReload

// MTLSReloadError is the error behind ErrMTLSReload; Err is the failed check.
type MTLSReloadError = transport.MTLSReloadError

// ErrNodeIdentityChanged is the cause inside a *MTLSReloadError when the
// reloaded leaf carries a different public key, which would change this
// host's NodeIdentity. The concrete error is a *NodeIdentityChangedError.
var ErrNodeIdentityChanged = transport.ErrNodeIdentityChanged

// NodeIdentityChangedError is the error behind ErrNodeIdentityChanged,
// naming the identity in force and the refused one.
type NodeIdentityChangedError = transport.NodeIdentityChangedError

// The remaining causes a *MTLSReloadError can carry, for errors.Is matching.
var (
	// ErrIncompleteMTLSConfig: the material is missing CACert, or both the
	// pre-built Certificate and the Cert/Key pair.
	ErrIncompleteMTLSConfig = transport.ErrIncompleteMTLSConfig

	// ErrInvalidCACert: the CA bundle yields no certificate for the pool.
	ErrInvalidCACert = transport.ErrInvalidCACert

	// ErrNoLocalCertificate: the host has no transport, so it holds no
	// certificate to replace.
	ErrNoLocalCertificate = transport.ErrNoLocalCertificate
)

// Internal engine, raft and transport errors that reach callers, aliased to the same values so errors.Is and errors.As match.
var (
	// ErrApplyGap is the *ApplyError cause when a committed batch does not follow the last applied entry.
	ErrApplyGap = engine.ErrApplyGap

	// ErrSnapshotMetaCorrupt is matched by *SnapshotMetaInvalidError for a snapshot.meta that cannot vouch for its data.
	ErrSnapshotMetaCorrupt = engine.ErrSnapshotMetaCorrupt

	// ErrScratchNotUnlinked is the sentinel of *ScratchUnlinkError: the decryption scratch file could not be unlinked.
	ErrScratchNotUnlinked = engine.ErrScratchNotUnlinked

	// ErrUnsupportedSnapshotCompression is returned when a snapshot names a compression this binary cannot decompress.
	ErrUnsupportedSnapshotCompression = engine.ErrUnsupportedSnapshotCompression

	// ErrCorruptConfigEntry is returned when a committed config change or bootstrap membership entry does not decode.
	ErrCorruptConfigEntry = raft.ErrCorruptConfigEntry

	// ErrEntropyUnavailable is the cause inside a *HostInitError with Field
	// "peer" from StartShard when crypto/rand could not seed the Raft peer's
	// election-timeout randomization.
	ErrEntropyUnavailable = raft.ErrEntropyUnavailable

	// ErrCACertificateParse is the sentinel form of *CACertificateParseError,
	// the cause inside a *HostInitError with Field "transport" from NewHost.
	ErrCACertificateParse = transport.ErrCACertificateParse

	// ErrInvalidCertKeyPair is the NewHost transport cause when the node's TLS leaf cannot be parsed.
	ErrInvalidCertKeyPair = transport.ErrInvalidCertKeyPair
)

// ApplyError reports a committed batch the state machine could not apply; Err is the cause and the shard is failed.
type ApplyError = engine.ApplyError

// SMPanicError reports a state machine that panicked while applying; it fails the shard and matches ErrShardFailed.
type SMPanicError = engine.SMPanicError

// SnapshotSavePanicError reports a panicking SaveSnapshot; the shard is not failed.
type SnapshotSavePanicError = engine.SnapshotSavePanicError

// SaveStateError reports a batch whose Raft state could not be persisted after every retry; the shard is failed.
type SaveStateError = engine.SaveStateError

// CommitError reports a persisted batch the Raft peer could not commit; the shard is failed.
type CommitError = engine.CommitError

// WorkerPanicError reports a step worker that panicked while a snapshot
// install it had in hand was in progress, leaving raft's state unknown. The
// shard owning that install is failed with it as config.ShardFailedInfo.Err.
type WorkerPanicError = engine.WorkerPanicError

// SnapshotError reports a failed snapshot save or recovery; only a recovery that reached RecoverFromSnapshot fails the shard.
type SnapshotError = engine.SnapshotError

// SnapshotRecoveryBehindError reports a recovery abandoned because the log never reached the snapshot index; it is rejected, not fatal.
type SnapshotRecoveryBehindError = engine.SnapshotRecoveryBehindError

// SnapshotRecoveryPanicError reports a recovery that panicked before RecoverFromSnapshot; it is rejected and re-driven.
type SnapshotRecoveryPanicError = engine.SnapshotRecoveryPanicError

// SnapshotRecoveryExhaustedError reports a shard failed after every snapshot recovery attempt was rejected.
type SnapshotRecoveryExhaustedError = engine.SnapshotRecoveryExhaustedError

// SnapshotEncryptError reports a snapshot frame that could not be encrypted or decrypted; Op names the step.
type SnapshotEncryptError = engine.SnapshotEncryptError

// SnapshotMetaInvalidError reports a snapshot.meta that cannot vouch for its data file; it matches ErrSnapshotCorrupt and ErrSnapshotMetaCorrupt.
type SnapshotMetaInvalidError = engine.SnapshotMetaInvalidError

// ScratchUnlinkError is the error behind ErrScratchNotUnlinked. Path is the
// scratch file and Err the refused unlink.
type ScratchUnlinkError = engine.ScratchUnlinkError

// DecompressError is the cause inside a *ApplyError when a committed entry's
// snappy payload does not decode. Err is the decoder's error.
type DecompressError = raft.DecompressError

// CACertificateParseError is the error behind ErrCACertificateParse, for an unusable CA bundle with revocation configured.
type CACertificateParseError = transport.CACertificateParseError

// NotLeaderError is returned when a proposal targets a non-leader node.
// It carries the current leader's identity so clients can redirect
// immediately without a separate ShardInfo() call.
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

// LeaderUnavailableError is returned by ExecuteOnLeader when its context ended without a leader answering.
type LeaderUnavailableError struct {
	ShardID uint64
	Cause   error
	Err     error
}

// Error returns a human-readable description naming the cause and how the
// request's context ended.
func (e *LeaderUnavailableError) Error() string {
	return fmt.Sprintf("quicraft: shard %d: %v: %v", e.ShardID, e.Cause, e.Err)
}

// Unwrap returns the cause and the context error, so errors.Is matches
// ErrNoLeader or ErrLeadershipChanged, and ErrTimeout or ErrCanceled.
func (e *LeaderUnavailableError) Unwrap() []error {
	return []error{e.Cause, e.Err}
}

// LeaderHandlerError is returned by ExecuteOnLeader when the leader's handler failed; the request was executed.
type LeaderHandlerError struct {
	ShardID  uint64
	LeaderID uint64
	Term     uint64
	Message  string
}

// Error returns the handler's error text with the shard, leader and term it
// was executed under.
func (e *LeaderHandlerError) Error() string {
	return fmt.Sprintf("quicraft: leader handler for shard %d failed on replica %d in term %d: %s",
		e.ShardID, e.LeaderID, e.Term, e.Message)
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

// SnapshotTooLargeError is returned when an imported snapshot exceeds MaxSnapshotSize.
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

// BusyError is returned when a shard's in-memory log exceeds MaxInMemLogSize or the global limiter triggers; it matches ErrBusy.
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

// SnapshotChunkError is the validate cause when delivered chunks do not form one snapshot; Reason names the failed rule.
type SnapshotChunkError struct {
	ChunkID    uint64
	ChunkCount uint64
	Reason     string
}

// Error returns a human-readable description of the chunk-set defect.
func (e *SnapshotChunkError) Error() string {
	return fmt.Sprintf("quicraft: snapshot chunk %d of %d: %s", e.ChunkID, e.ChunkCount, e.Reason)
}

// SnapshotSizeMismatchError is the validate cause when reassembled chunks do not add up to the declared FileSize.
type SnapshotSizeMismatchError struct {
	Declared uint64
	Actual   uint64
}

// Error returns a human-readable description of the size mismatch.
func (e *SnapshotSizeMismatchError) Error() string {
	return fmt.Sprintf("quicraft: snapshot reassembled size %d does not match declared FileSize %d", e.Actual, e.Declared)
}

// SnapshotFailedError reports a snapshot operation the engine could not complete; recovery failures match ErrSnapshotRecoveryFailed, the cause stays reachable.
type SnapshotFailedError struct {
	ShardID   uint64
	ReplicaID uint64
	// Recovery is true when the failed operation was installing an artifact
	// into the state machine, false when it was writing one out.
	Recovery bool
	// Err is the underlying engine failure.
	Err error
}

// Error returns a human-readable description of the snapshot failure.
func (e *SnapshotFailedError) Error() string {
	op := "save"
	if e.Recovery {
		op = "recovery"
	}
	return fmt.Sprintf(
		"quicraft: snapshot %s failed for shard %d replica %d: %v",
		op, e.ShardID, e.ReplicaID, e.Err,
	)
}

// Unwrap returns the underlying engine failure.
func (e *SnapshotFailedError) Unwrap() error { return e.Err }

// Is reports whether target matches ErrSnapshotRecoveryFailed. Only a recovery
// failure matches; a save failure has no sentinel of its own.
func (e *SnapshotFailedError) Is(target error) bool {
	return e.Recovery && target == ErrSnapshotRecoveryFailed
}

// CorruptConfigEntryError reports a committed config entry this binary cannot decode; it fails the shard.
type CorruptConfigEntryError struct {
	// ShardID is the shard whose committed entry failed to decode.
	ShardID uint64
	// Index is the log index of that entry.
	Index uint64
	// Kind names which payload it was — "config change" or "bootstrap
	// membership" — because the two have different remedies.
	Kind string
	// Err is the decode failure from pkg/proto.
	Err error
}

// Error returns a human-readable description of the corrupt committed entry.
func (e *CorruptConfigEntryError) Error() string {
	return fmt.Sprintf(
		"quicraft: committed %s entry at index %d of shard %d does not decode: %v",
		e.Kind, e.Index, e.ShardID, e.Err,
	)
}

// Unwrap returns the underlying decode error.
func (e *CorruptConfigEntryError) Unwrap() error {
	return e.Err
}

// UnauthorizedMessageError is returned when a message comes from a replica that is not a member of the target shard.
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

// SessionExpiredError is returned when a session-managed proposal references an unregistered clientID.
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

// ReplicaRetiredError is returned when StartShard targets a replica whose data RemoveData removed; rejoin under a new replica ID.
type ReplicaRetiredError struct {
	ShardID   uint64
	ReplicaID uint64
}

// Error returns a human-readable description.
func (e *ReplicaRetiredError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d was retired by RemoveData; rejoin under a new replica ID",
		e.ShardID, e.ReplicaID,
	)
}

// Is reports whether target matches the sentinel ErrReplicaRetired error.
func (e *ReplicaRetiredError) Is(target error) bool {
	return target == ErrReplicaRetired
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

// StaleConfigChangeError is returned when a config change carries a ConfigChangeID that does not match the current configuration.
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

// SelfNotInMembersError is returned when StartShard with join=false is given members that omit the local ReplicaID.
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

// NonVotingRoleInMembersError is returned by StartShard when a witness or observer appears in the initial members map; start it with join=true and add it instead.
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

// TypeAssertionError is returned when a stored value has an unexpected type, an internal programming error.
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

// ImportLayoutUnknownError is returned by ImportSnapshot when the shard's DataDir is neither recorded nor given with WithImportDataDir.
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

// ErrImportMembershipInvalid is the sentinel for *ImportMembershipInvalidError.
var ErrImportMembershipInvalid = errors.New("quicraft: snapshot import membership override is not usable")

// ImportMembershipInvalidError is returned by ImportSnapshot when the WithImportMembership override cannot be used.
type ImportMembershipInvalidError struct {
	ShardID   uint64
	ReplicaID uint64
	// Reason states which rule the supplied membership broke.
	Reason string
}

// Error returns a human-readable description of the unusable membership.
func (e *ImportMembershipInvalidError) Error() string {
	return fmt.Sprintf(
		"quicraft: shard %d replica %d: WithImportMembership rejected: %s",
		e.ShardID, e.ReplicaID, e.Reason,
	)
}

// Is reports whether target matches the sentinel ErrImportMembershipInvalid.
func (e *ImportMembershipInvalidError) Is(target error) bool {
	return target == ErrImportMembershipInvalid
}

// ErrImportDataDirMismatch is the sentinel for *ImportDataDirMismatchError.
var ErrImportDataDirMismatch = errors.New("quicraft: snapshot import DataDir disagrees with the shard's recorded DataDir")

// ImportDataDirMismatchError is returned by ImportSnapshot when WithImportDataDir contradicts the recorded DataDir.
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

// ImportSnapshotOutOfDateError is returned by ImportSnapshot, before any write, when the artifact is not newer than the recorded snapshot; it matches logdb.ErrSnapshotOutOfDate.
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

// ImportSnapshotDirExistsError is returned by ImportSnapshot when the artifact's canonical directory already exists; an import never deletes a directory it did not create.
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

// ErrInvalidShardHealth is the sentinel for *InvalidShardHealthError.
var ErrInvalidShardHealth = errors.New("quicraft: unrecognized shard health value")

// InvalidShardHealthError is returned by ShardHealth.UnmarshalJSON for an unknown value, rather than defaulting to healthy.
type InvalidShardHealthError struct {
	// Value is the string that was decoded.
	Value string
}

// Error returns a human-readable description of the unrecognized value.
func (e *InvalidShardHealthError) Error() string {
	return fmt.Sprintf("quicraft: unrecognized shard health value %q", e.Value)
}

// Is reports whether target matches the sentinel ErrInvalidShardHealth.
func (e *InvalidShardHealthError) Is(target error) bool {
	return target == ErrInvalidShardHealth
}

// temporaryErrors is the complete set of sentinels Temporary reports true for: only those where the identical request can succeed once a transient condition clears.
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
	// A leader-executed request found no leader, or leadership moving; an
	// election settles both without any caller action.
	ErrNoLeader,
	ErrLeadershipChanged,
	// Another snapshot holds the shard's single-flight slot, or every
	// snapshot worker in the pool is occupied.
	ErrSnapshotInProgress,
	// An ImportSnapshot for the shard is in progress; StartShard succeeds once
	// it returns.
	ErrImportInProgress,
	// A leadership transfer is in flight. It completes or times out without
	// any caller or operator action, and the identical request then succeeds.
	ErrTransferInProgress,
	// The session's in-flight proposal completes, fails or is released on
	// its own, and the identical proposal then reserves the series.
	ErrSessionBusy,
	// The shard is paused for a runtime seal (Host.PauseShard). A pause is
	// an operator window that ResumeShard ends after the unseal, never a
	// terminal state, so the identical request succeeds after it.
	ErrShardPaused,
}

// Temporary reports whether err is a transient condition that may succeed on retry; every other error, including foreign ones, is permanent.
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

// MaybeAppliedError wraps an error whose request had already been handed to Raft, so the entry may still commit and apply.
type MaybeAppliedError struct{ Err error }

// Error returns the wrapped error's message, marked as already submitted.
func (e *MaybeAppliedError) Error() string {
	return e.Err.Error() + " (already submitted to raft; it may still commit)"
}

// Unwrap returns the wrapped error.
func (e *MaybeAppliedError) Unwrap() error { return e.Err }

// MaybeApplied reports whether err's request reached Raft before failing; check it before retrying, and use a session for non-idempotent operations.
func MaybeApplied(err error) bool {
	if err == nil {
		return false
	}
	var m *MaybeAppliedError
	return errors.As(err, &m)
}

// NotLeader reports whether err is a not-leader error and the known leader's replica ID (0 if unknown); check MaybeApplied first.
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
