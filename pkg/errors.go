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
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
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

	// ErrAtRestEncryptionUnspecified is returned by NewHost when the host
	// neither configures at-rest encryption (WithBarrier or
	// WithShardBarriers) nor declares WithoutEncryption. Without it, the
	// omission of an option would store every command and snapshot as
	// plaintext with nothing to say so.
	ErrAtRestEncryptionUnspecified = errors.New(
		"quicraft: at-rest encryption is unspecified: configure WithBarrier " +
			"or WithShardBarriers, or declare WithoutEncryption")

	// ErrAtRestEncryptionConflict is returned by NewHost when
	// WithoutEncryption is combined with WithBarrier: the two declare opposite
	// host-wide routes. WithShardBarriers is a separate, per-shard axis and
	// combines with either.
	ErrAtRestEncryptionConflict = errors.New(
		"quicraft: WithoutEncryption cannot be combined with WithBarrier")

	// ErrBarrierPlaintextLogDB is returned by NewHost when WithBarrier is
	// configured together with a caller-supplied LogDB that does not encrypt
	// at rest. The host-wide barrier is wired only into the DEFAULT WAL, so a
	// supplied waldb.Open(dir) would store every proposed command in plaintext
	// while snapshots were encrypted — the state a stated encryption choice
	// must never reach. The LogDB must report logdb.AtRestBarrierEncrypted or
	// logdb.AtRestNonDurable, or logdb.AtRestHostRouted with its host-wide route
	// written through the WithBarrier barrier itself (by identity), which is
	// how the default WAL is judged, plaintext mode included. One that declares
	// nothing is treated as unprotected.
	ErrBarrierPlaintextLogDB = errors.New(
		"quicraft: WithBarrier with a supplied LogDB requires that LogDB to be " +
			"encrypted at rest (logdb.AtRestProtectedLogDB reporting barrier_encrypted " +
			"or non_durable) or host_routed through the WithBarrier barrier itself; " +
			"this LogDB would store plaintext")

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
	//
	// Under WithoutEncryption a LogDB reporting logdb.AtRestHostRouted is also
	// accepted: its only plaintext route is the host-wide one that
	// WithoutEncryption declared, and each barriered shard's dedicated route is
	// held to its barrier at StartShard (ErrShardBarrierLogDBMismatch).
	// WithoutEncryption does not accept the default WAL or a plain waldb, which
	// would hold the barriered shards' logs in plaintext too.
	ErrShardBarriersPlaintextWAL = errors.New(
		"quicraft: WithShardBarriers requires a LogDB that is encrypted at rest " +
			"(logdb.AtRestProtectedLogDB reporting barrier_encrypted or non_durable, " +
			"or host_routed under WithoutEncryption) or WithBarrier (host-wide WAL " +
			"key); this LogDB would store plaintext")

	// ErrShardBarrierUnresolved is returned when WithShardBarriers is
	// configured, the resolver returns nil for a shard, and there is no
	// declared host-wide route to fall back to: neither WithBarrier nor
	// WithoutEncryption.
	//
	// A resolver typed func(shardID uint64) Barrier cannot distinguish "this
	// shard is deliberately plaintext" from "I do not know this shard yet",
	// which is the normal outcome of a map lookup for an async-provisioned
	// tenant or a remapped shard ID. Reading that nil as "plaintext is fine"
	// downgrades exactly one tenant while every other tenant's spot-check still
	// shows encryption working, so the miss fails CLOSED instead.
	//
	// It is NOT returned when WithBarrier supplied a host-wide barrier: a nil
	// resolver result then means the documented fallback to that barrier. Nor
	// is it returned under WithoutEncryption, where the nil means the plaintext
	// host-wide route the host declared; the declaration is what makes the nil
	// a decision rather than a miss.
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

	// ErrDropped is returned when a request is dropped by the engine before
	// it reached Raft: a proposal queue overflow or shutdown in progress, or
	// a proposal, ReadIndex or read result an engine worker took out of a
	// shard and could not hand on because user code it ran (a metrics
	// collector, a callback, a logger) panicked or called runtime.Goexit. It
	// is Temporary, and never MaybeApplied: nothing reached the log.
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

	// ErrShardFailed is returned when the shard's failure latch has tripped
	// and the shard is offline until it is reloaded or recovered from a
	// snapshot. Apply is fail-fast: the FIRST failure latches, and there is
	// no retry count to configure. An SM.Update that returns an error instead
	// reaches the caller as a *ApplyError wrapping the state machine's own
	// error, which is the more specific answer.
	//
	// It is the engine's own sentinel, so the *SMPanicError a panicking state
	// machine produces in config.ShardFailedInfo.Err matches it.
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

	// ErrDecompressionBomb is returned when a decompressed payload
	// exceeds the configured maximum size, preventing zip-bomb attacks. A
	// committed entry that declares such an expansion fails its batch: the
	// batch's proposals receive this sentinel and config.ShardFailedInfo.Err is
	// a *ApplyError wrapping it. It is the raft package's own sentinel, so
	// both match.
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
	// snapshot fails due to corruption or incompatibility. It is produced as
	// *SnapshotFailedError with Recovery set, so the underlying engine cause
	// stays reachable through Unwrap.
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

	// ErrImportInProgress is returned by StartShard when an ImportSnapshot
	// for the same shard is in progress. An import records a snapshot and
	// compacts the log beneath it, so a node started over the shard's durable
	// state mid-import would run on a log removed underneath it. The identical
	// StartShard succeeds once the import returns.
	ErrImportInProgress = errors.New("quicraft: snapshot import in progress for shard")

	// ErrStopped is returned when an operation is interrupted because the
	// shard or the engine stopped underneath it — a snapshot save or recovery
	// abandoned because the node's stop channel closed. It is distinct from
	// ErrClosed, which means the request never started.
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
	// requested operation, e.g. wrong shard ID, or a no-op session passed
	// to ProposeWithSession or to a session register/unregister operation.
	ErrInvalidSession = errors.New("quicraft: invalid session")

	// ErrSessionBusy is returned by ProposeWithSession and
	// SyncProposeWithSession when the session already has a proposal in
	// flight. A session carries ONE series number at a time, and the state
	// machine treats every entry stamped with the same (ClientID, SeriesID)
	// as a retransmission of one command: it applies the first and answers
	// the rest with its result. Two concurrent proposals on one session
	// would therefore both report success while only one of them ran, so
	// the second is refused; it may be resubmitted once the first has
	// completed (ProposalCompleted, an error result, or Release).
	ErrSessionBusy = errors.New("quicraft: session has a proposal in flight")

	// ErrSnapshotSizeMismatch is returned when the snapshot file size
	// on disk does not match the expected FileSize from the message, or the
	// FileSize the snapshot's LogDB record claims. The second is the cause
	// inside a *HostInitError with Field "snapshot_integrity" from StartShard;
	// a runtime recovery that meets it is rejected rather than failing the shard
	// (see SnapshotError). It is the engine's own sentinel, so both match.
	ErrSnapshotSizeMismatch = engine.ErrSnapshotSizeMismatch

	// ErrSnapshotCorrupt is returned when an encrypted snapshot frame has a
	// corrupt/oversized length prefix during decryption (send/export path),
	// or when a snapshot's data does not match the length and hash recorded
	// in its snapshot.meta as the send or export path reads it. The same
	// checks on recovery make it the cause inside a *HostInitError with Field
	// "snapshot_integrity" or "snapshot_decrypt" from StartShard; a runtime
	// recovery that meets it is rejected rather than failing the shard (see
	// SnapshotError). It is the engine's own sentinel, so every one of those
	// matches it.
	ErrSnapshotCorrupt = engine.ErrSnapshotCorrupt

	// ErrSnapshotDestinationRecorded is returned when a received snapshot's
	// canonical directory is the directory LogDB currently records as this
	// replica's live snapshot.
	//
	// A receive publishes by renaming its staging directory onto the canonical
	// name, which means clearing whatever is already there. Doing that to the
	// LIVE artifact leaves the LogDB record pointing at nothing: the shard
	// cannot start, and every leader re-send is answered by the dedup branch
	// from the record that now describes a deleted directory — a permanent
	// wedge with no self-heal. The transfer is refused instead, and the leader
	// re-sends after its ack timeout.
	//
	// It is unreachable through the normal path (the dedup branch answers every
	// re-send at or below the recorded index) and exists for the paths that
	// reach the publish without a usable LogDB answer.
	ErrSnapshotDestinationRecorded = errors.New(
		"quicraft: received snapshot's destination is the snapshot LogDB currently records")

	// ErrShardNotStopped is returned when RemoveData is called for a
	// shard that is still loaded on this host.
	ErrShardNotStopped = errors.New("quicraft: shard not stopped")

	// ErrReplicaRetired is returned when StartShard targets a replica whose
	// data RemoveData removed on this host. See ReplicaRetiredError.
	ErrReplicaRetired = errors.New("quicraft: replica ID retired")

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
// (RequestAddNode, RequestAddObserver, RequestAddWitness, RequestRemoveNode,
// RequestDeleteNode, their *WithIdentity forms and their Sync variants). These
// alias the internal raft sentinels so external callers can match the
// rejections with errors.Is instead of string comparison.
//
// The alias is the SAME error value the raft package returns, not a copy with
// the same text, so errors.Is matches through the wrapping the config-change
// funnel applies. That identity is the whole point: pkg/internal/raft is
// unimportable from outside this module, so before these aliases existed a
// downstream consumer could only string-match a rejection it is expected to
// handle programmatically.
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

	// ErrNodeNotFound is returned when a removal targets a replica that is
	// not in the shard's current membership. The removal is refused rather
	// than committed as a no-op, so the caller learns its view is stale.
	ErrNodeNotFound = raft.ErrNodeNotFound

	// ErrNodeAlreadyExists is returned when an add targets a replica that is
	// already a member of the shard in the role being requested.
	ErrNodeAlreadyExists = raft.ErrNodeAlreadyExists

	// ErrNodeRemoved is returned when an add targets a replica ID that this
	// shard previously removed. Replica IDs are never recycled: re-adding one
	// would let a node that still holds the old identity's log rejoin.
	ErrNodeRemoved = raft.ErrNodeRemoved

	// ErrRemoveLastVoter is returned when a config change would remove the
	// shard's last voting member, leaving it unable to elect a leader.
	ErrRemoveLastVoter = raft.ErrRemoveLastVoter

	// ErrTooManyVotingMembers is returned when an add would take the shard
	// past 64 voting members. ReadIndex quorum tracking is a uint64 bitfield,
	// so a 65th voter would alias another voter's bit and break the quorum
	// count.
	ErrTooManyVotingMembers = raft.ErrTooManyVotingMembers

	// ErrZeroReplicaID is returned when a config change names replica ID 0,
	// which is the reserved "no replica" value and can never be a member.
	ErrZeroReplicaID = raft.ErrZeroReplicaID

	// ErrEmptyAddress is returned when an add carries an empty address. A
	// member with no address is unreachable and cannot be replicated to.
	ErrEmptyAddress = raft.ErrEmptyAddress

	// ErrRemovedMemberIdentity is returned when a membership add would bind a
	// node identity this shard has already committed a removal for, under any
	// replica ID. The removal tombstone is on the IDENTITY, not on the replica
	// ID it was bound to, and is never evicted (Membership.RemovedIdentities),
	// so a keypair whose replica was removed is banned from the shard
	// permanently: every replica refuses its traffic whatever ID it presents.
	// The add is refused at propose time rather than committed as a member no
	// replica will ever hear from. The remedy is a NEW keypair; recycling the
	// removed machine's address under a new replica ID remains legal.
	ErrRemovedMemberIdentity = raft.ErrRemovedMemberIdentity

	// ErrDuplicateMemberAddress is returned when an add (RequestAddNode,
	// RequestAddObserver, RequestAddWitness, their *WithIdentity forms and
	// their Sync variants) names a new replica at an address another ACTIVE
	// member of the shard already holds. Routing is per address: this host
	// dials a member at its committed address and learns, replaces and evicts
	// routes by address, so two replica IDs at one address would share one
	// route and the transport's address-scoped checks could not tell them
	// apart. Promoting an observer at its
	// own address is not a duplicate. Remove the current holder first, or use
	// a different address.
	ErrDuplicateMemberAddress = raft.ErrDuplicateMemberAddress

	// ErrDuplicateMemberIdentity is returned when an add (the same methods as
	// ErrDuplicateMemberAddress) would bind a node identity another ACTIVE
	// member of the shard already holds. One identity is one keypair, and the
	// keypair is what every replica authenticates a sender by, so binding it to
	// a second replica ID would give its holder two votes. Unlike addresses
	// there is no co-location exception: two replicas on one machine still need
	// distinct keypairs.
	ErrDuplicateMemberIdentity = raft.ErrDuplicateMemberIdentity

	// ErrMemberIdentityRequired is returned by RequestAddNode,
	// RequestAddObserver and RequestAddWitness — the identity-less forms — on
	// a host with a transport, and by their WithIdentity forms given an empty
	// identity. A member is authorized by its committed node identity and by
	// nothing else, so admitting one without would seat a voter that can never
	// be heard from. See config.Config.RequireMemberIdentities.
	ErrMemberIdentityRequired = raft.ErrMemberIdentityRequired

	// ErrTransferInProgress is returned when a proposal, config change or
	// linearizable read arrives while a leadership transfer is in flight on
	// this shard. The replica IS the leader; it is deliberately not accepting
	// work it may be about to hand off.
	//
	// It is re-exported for the reason ErrPendingConfigChange was mapped: an
	// internal sentinel lives in an internal package, so no caller can match it
	// with errors.Is and every consumer falls through to its own catch-all,
	// reporting a retryable rejection as a fatal error, or matches it with
	// `strings.Contains(err.Error(), "transfer in progress")` — string
	// matching on error text, the shape to avoid.
	//
	// It is Temporary: the transfer completes or times out on its own, and the
	// IDENTICAL request then succeeds. It is NOT MaybeApplied — raft returns it
	// from handleLeaderPropose and handleReadIndex BEFORE the entry enters the
	// log, so nothing was submitted and nothing can commit later.
	ErrTransferInProgress = raft.ErrTransferInProgress
)

// ErrSharedPeerCertificate is returned by StartShard when this host's own TLS
// leaf certificate also covers the host of another member in the initial
// members map, and by the membership-add API (RequestAddNode,
// RequestAddObserver, RequestAddWitness, their *WithIdentity forms and their
// Sync variants) when it covers the host of the replica being added. The
// concrete error is a *SharedPeerCertificateError naming the covered hosts.
//
// The transport authorizes a sender by checking that the address it claims is
// named in its verified certificate, which only means something while every
// node's certificate names that node alone. A wildcard SAN, or one certificate
// carrying every node's SAN, lets any single replica speak for every peer, so
// the shard refuses to start (or the add is refused) until certificates are
// re-issued per node. Replicas on the same machine are exempt: a certificate
// is matched on host alone and cannot tell them apart.
//
// It is the same value the internal transport package returns, so errors.Is
// matches through any wrapping.
var ErrSharedPeerCertificate = transport.ErrSharedPeerCertificate

// SharedPeerCertificateError is the error behind ErrSharedPeerCertificate.
// SelfAddress is this host's advertised Raft address and CoveredPeers the
// other members' hosts its certificate also covers, sorted. It is an alias of
// the transport type, so errors.As into it matches the error StartShard and
// the membership-add API return.
type SharedPeerCertificateError = transport.SharedPeerCertificateError

// Errors built inside the engine, raft and transport packages that reach a
// caller through one of three channels: the result of a proposal whose batch
// failed to apply or persist, config.ShardFailedInfo.Err, and the cause inside
// a *HostInitError from NewHost or StartShard. Each is an alias of the SAME
// value or type the internal package builds, never a copy with the same text,
// so errors.Is and errors.As match through every layer of wrapping without any
// translation. Errors of those packages that cannot reach a caller stay
// internal.
//
// The sentinels ErrShardFailed, ErrSnapshotCorrupt, ErrSnapshotSizeMismatch
// and ErrDecompressionBomb, declared with the others above, are aliases of the
// same kind: the internal error and the public sentinel are one value.
var (
	// ErrApplyGap is the cause inside a *ApplyError when a committed batch
	// does not start at the entry after the last one applied. The missing
	// range can never be re-offered, so the shard is failed rather than left
	// with a hole in its state machine. It reaches the batch's proposals and
	// config.ShardFailedInfo.Err.
	ErrApplyGap = engine.ErrApplyGap

	// ErrSnapshotMetaCorrupt is matched, together with ErrSnapshotCorrupt, by
	// a *SnapshotMetaInvalidError: a recorded snapshot whose snapshot.meta is
	// missing, unreadable, foreign or lacks its integrity fields. It is the
	// cause inside a *HostInitError with Field "snapshot_integrity" from
	// StartShard; a runtime recovery that meets it is rejected rather than failing the shard
	// (see SnapshotError).
	ErrSnapshotMetaCorrupt = engine.ErrSnapshotMetaCorrupt

	// ErrScratchNotUnlinked is the sentinel form of *ScratchUnlinkError: the
	// scratch file an encrypted snapshot is decrypted into could not be
	// unlinked the moment it was created, so no plaintext was written to it.
	// It is the cause inside a *HostInitError with Field "snapshot_decrypt"
	// from StartShard; a runtime recovery that meets it is rejected rather than failing the shard
	// (see SnapshotError).
	ErrScratchNotUnlinked = engine.ErrScratchNotUnlinked

	// ErrUnsupportedSnapshotCompression is returned when a snapshot record
	// names a compression algorithm this binary cannot decompress. It is the
	// cause inside a *HostInitError with Field "snapshot_compression" from
	// StartShard; a runtime recovery that meets it is rejected rather than failing the shard
	// (see SnapshotError).
	ErrUnsupportedSnapshotCompression = engine.ErrUnsupportedSnapshotCompression

	// ErrCorruptConfigEntry is returned when a COMMITTED config change or
	// bootstrap membership entry does not decode. Every peer can read those
	// bytes and this replica cannot, so there is no local repair. It is the
	// cause inside a *HostInitError with Field "config_replay" or
	// "membership_reconstruct" from StartShard, and inside the *CommitError in
	// config.ShardFailedInfo.Err when the commit pipeline meets the entry.
	ErrCorruptConfigEntry = raft.ErrCorruptConfigEntry

	// ErrEntropyUnavailable is the cause inside a *HostInitError with Field
	// "peer" from StartShard when crypto/rand could not seed the Raft peer's
	// election-timeout randomization.
	ErrEntropyUnavailable = raft.ErrEntropyUnavailable

	// ErrCACertificateParse is the sentinel form of *CACertificateParseError,
	// the cause inside a *HostInitError with Field "transport" from NewHost.
	ErrCACertificateParse = transport.ErrCACertificateParse

	// ErrInvalidCertKeyPair is the cause inside a *HostInitError with Field
	// "transport" from NewHost when the node's TLS leaf cannot be parsed: a
	// pre-built TransportConfig.MTLSConfig.Certificate whose first DER block
	// is not an X.509 certificate.
	ErrInvalidCertKeyPair = transport.ErrInvalidCertKeyPair
)

// ApplyError reports a committed batch the state machine could not apply:
// SM.Update returned an error, an entry failed to decompress, or the batch did
// not continue from the last applied entry (ErrApplyGap). Err is the cause —
// the state machine's own error, a *DecompressError, ErrDecompressionBomb or
// ErrApplyGap. The shard is failed: every proposal in the batch receives the
// *ApplyError, and it is config.ShardFailedInfo.Err. It does not match
// ErrShardFailed; Err is the more specific answer.
type ApplyError = engine.ApplyError

// SMPanicError reports a state machine that panicked while applying a batch.
// Value is the recovered panic value and Stack the goroutine stack. The shard
// is failed: it is config.ShardFailedInfo.Err and matches ErrShardFailed. The
// batch's proposals receive ErrShardFailed itself.
type SMPanicError = engine.SMPanicError

// SnapshotSavePanicError reports a state machine whose SaveSnapshot panicked.
// Value is the recovered panic value and Stack the goroutine stack. The shard
// is NOT failed — a save only reads the state machine — so it does not match
// ErrShardFailed. RequestSnapshot's caller receives it as the Err of a
// *SnapshotFailedError, like any other failed save.
type SnapshotSavePanicError = engine.SnapshotSavePanicError

// SaveStateError reports a batch whose Raft state and entries could not be
// persisted to the LogDB after every retry. Err is the LogDB error. The shard
// is failed: every proposal in the batch receives the *SaveStateError, and it
// is config.ShardFailedInfo.Err.
type SaveStateError = engine.SaveStateError

// CommitError reports a batch the Raft peer could not commit after it was
// persisted: the log reader refused the entries or snapshot, or a committed
// config entry did not decode (ErrCorruptConfigEntry). Err is the cause. The
// shard is failed: every proposal in the batch receives the *CommitError, and
// it is config.ShardFailedInfo.Err.
type CommitError = engine.CommitError

// WorkerPanicError reports a step worker that panicked while a snapshot
// install it had in hand was in progress, leaving raft's state unknown. The
// shard owning that install is failed with it as config.ShardFailedInfo.Err.
type WorkerPanicError = engine.WorkerPanicError

// SnapshotError reports a snapshot save (Save true) or recovery (Save false)
// that failed inside the engine's snapshot pool. Err is the cause, which may
// be any of the snapshot sentinels and types in this file. It is
// config.ShardFailedInfo.Err for a recovery that failed once the state
// machine's RecoverFromSnapshot was invoked, which fails the shard. A recovery
// that fails before that (reading the snapshot record, verifying, decrypting or
// decompressing the artifact, resolving the barrier) has not touched the state
// machine: it is rejected, logged at ERROR and reported through
// OnSnapshotRejected with its cause as config.SnapshotInfo.Err, the shard keeps
// running, and the engine re-drives the recovery; the last rejected attempt
// fails the shard with a *SnapshotRecoveryExhaustedError. A failed save does not fail
// the shard and fires no OnShardFailed: it is logged, and a SyncRequestSnapshot
// caller receives it as a *SnapshotFailedError.
type SnapshotError = engine.SnapshotError

// SnapshotRecoveryBehindError reports a snapshot recovery abandoned because
// the Raft log never reached the snapshot's index within the wait budget. The
// state machine was not touched, so the recovery is rejected rather than
// failing the shard (see SnapshotError).
type SnapshotRecoveryBehindError = engine.SnapshotRecoveryBehindError

// SnapshotRecoveryPanicError reports a snapshot recovery that panicked before
// the state machine's RecoverFromSnapshot was invoked. Value is the recovered
// panic value and Stack the goroutine stack. The state machine was not
// touched, so it does not match ErrShardFailed: the recovery is rejected and
// re-driven. It is config.SnapshotInfo.Err of that OnSnapshotRejected, and the
// cause inside a *SnapshotRecoveryExhaustedError when it was the last attempt.
type SnapshotRecoveryPanicError = engine.SnapshotRecoveryPanicError

// SnapshotRecoveryExhaustedError reports a shard failed because every attempt
// to recover a snapshot its Raft log adopted was rejected before the state
// machine restore began: the engine re-drives a rejected recovery with backoff
// and fails the shard after the last attempt of one continuous episode of
// holding entries, whatever snapshot each attempt was made against. Attempts is
// how many were made, SnapshotIndex the snapshot the last one was made
// against, and Err its cause, reachable with errors.Is and errors.As. It is
// config.ShardFailedInfo.Err.
type SnapshotRecoveryExhaustedError = engine.SnapshotRecoveryExhaustedError

// SnapshotEncryptError reports a snapshot frame that could not be encrypted
// or decrypted; Op names the step and Err the cause (ErrSnapshotCorrupt for a
// damaged artifact). It is the cause inside a *HostInitError with Field
// "snapshot_decrypt" from StartShard and inside the *SnapshotFailedError a
// failed save returns to its requester; a runtime recovery that meets it is
// rejected rather than failing the shard (see SnapshotError).
type SnapshotEncryptError = engine.SnapshotEncryptError

// SnapshotMetaInvalidError reports a recorded snapshot whose snapshot.meta
// cannot vouch for the data file beside it. Dir is the snapshot directory and
// Err the underlying cause, if any. It matches ErrSnapshotCorrupt and
// ErrSnapshotMetaCorrupt; see ErrSnapshotMetaCorrupt for where it is returned.
type SnapshotMetaInvalidError = engine.SnapshotMetaInvalidError

// ScratchUnlinkError is the error behind ErrScratchNotUnlinked. Path is the
// scratch file and Err the refused unlink.
type ScratchUnlinkError = engine.ScratchUnlinkError

// DecompressError is the cause inside a *ApplyError when a committed entry's
// snappy payload does not decode. Err is the decoder's error.
type DecompressError = raft.DecompressError

// CACertificateParseError is the error behind ErrCACertificateParse, returned
// by NewHost (inside a *HostInitError with Field "transport") when
// TransportConfig.RevocationConfig is set and the CA bundle has a block that
// does not parse, no CERTIFICATE block, or no certificate able to act as an
// issuer. Reason names the failure and Err the parse error, if any.
type CACertificateParseError = transport.CACertificateParseError

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

// SnapshotChunkError is the cause inside a SnapshotReceiveError with Op
// "validate" when the chunk set a sender delivered is not one snapshot:
// a chunk whose identity fields disagree with chunk 0, a ChunkCount that
// disagrees with what arrived, an ID out of range or repeated, or an ID
// missing from the set. ChunkID is the offending chunk (or the missing one)
// and ChunkCount the number received; Reason says which rule failed.
type SnapshotChunkError struct {
	ChunkID    uint64
	ChunkCount uint64
	Reason     string
}

// Error returns a human-readable description of the chunk-set defect.
func (e *SnapshotChunkError) Error() string {
	return fmt.Sprintf("quicraft: snapshot chunk %d of %d: %s", e.ChunkID, e.ChunkCount, e.Reason)
}

// SnapshotSizeMismatchError is the cause inside a SnapshotReceiveError with
// Op "validate" when the bytes reassembled from a snapshot's chunks do not
// add up to the FileSize the first chunk declared: a chunk was truncated in
// transit, or the declared size was forged. Either way the artifact is not
// used.
type SnapshotSizeMismatchError struct {
	Declared uint64
	Actual   uint64
}

// Error returns a human-readable description of the size mismatch.
func (e *SnapshotSizeMismatchError) Error() string {
	return fmt.Sprintf("quicraft: snapshot reassembled size %d does not match declared FileSize %d", e.Actual, e.Declared)
}

// SnapshotFailedError reports a snapshot operation the engine's snapshot pool
// could not complete. It is the public face of the engine-internal snapshot
// error: RequestSnapshot's RequestState carries it rather than the internal
// error verbatim, so a caller of SyncRequestSnapshot has an errors.Is or
// errors.As it can write against the failure instead of string matching.
//
// Only the RECOVERY direction claims a sentinel. Installing an artifact into
// the state machine is what ErrSnapshotRecoveryFailed describes, so a recovery
// failure matches it; a SAVE failure has no sentinel and deliberately claims
// none, because inventing one would say more about the failure than the host
// knows. Both directions keep the underlying cause reachable through Unwrap,
// which is what preserves the barrier sentinels (ErrShardBarrierUnresolved,
// ErrSnapshotBarrierSealed) that the engine already fails with by public
// identity.
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

// CorruptConfigEntryError reports a COMMITTED config-carrying entry whose
// payload this binary cannot decode, detected by the Host's routing mirror of
// the membership fold. It is terminal: the shard's failure latch is tripped
// and the error is delivered through EventListener.OnShardFailed.
//
// The bytes are replicated state every peer can read and this replica cannot,
// so there is no local repair. It is the host-side counterpart of
// raft.ErrCorruptConfigEntry, and recovery has always treated the same input the
// same way (ReconstructMembership returns it and StartShard aborts).
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

// ReplicaRetiredError is returned when StartShard targets a replica whose
// data Host.RemoveData removed on this host (ImportSnapshot, which imports into
// a replica that has data on this host, finds none and returns
// ErrShardNotFound, or this error should a record for it remain). RemoveData retires
// the replica ID: Raft never lets a removed replica return under the same ID,
// because its term and vote are gone with its data and it could vote twice in
// one term — and a vote it had promised a leader's read lease is gone with
// them. Rejoin the shard under a new replica ID.
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
// An observer bootstrapped this way is likewise a full VOTER, on this replica
// as much as on every other: Config.IsObserver is only an initial hint and
// yields to a membership that names the replica, so it votes, campaigns and is
// counted in every quorum. The operator asks for a non-voting learner and
// silently gets a voter — the voting set is larger than the operator believes,
// so the shard tolerates fewer failures than intended.
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

// ErrImportMembershipInvalid is the sentinel for *ImportMembershipInvalidError.
var ErrImportMembershipInvalid = errors.New("quicraft: snapshot import membership override is not usable")

// ImportMembershipInvalidError is returned by Host.ImportSnapshot when the
// membership passed to WithImportMembership cannot be used.
//
// The override is the one import input that can silently destroy committed
// data — it rewrites the shard's configuration, discarding whatever the
// excluded replicas held. Every plausible operator mistake (an address left
// off, the local replica omitted, a map copied stale from a runbook) therefore
// gets refused by name instead of resolved, because a configuration accepted by
// accident is not detectable afterwards: the shard simply comes up missing
// writes it once acknowledged.
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
// would mean the artifact had already been written — over the LIVE snapshot
// directory, whose name is the same index — by the time the refusal arrived.
// The operator would see an error and believe nothing had happened while the
// LogDB record described a file it no longer matched: a wrong FileSize ships a short
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

// ErrInvalidShardHealth is the sentinel for *InvalidShardHealthError.
var ErrInvalidShardHealth = errors.New("quicraft: unrecognized shard health value")

// InvalidShardHealthError is returned by ShardHealth.UnmarshalJSON when the
// JSON string names no ShardHealth value.
//
// It fails rather than defaulting because ShardHealthy is the zero value and
// therefore the reassuring one: decoding an unrecognized string to it turns a
// typo, a truncated field, or a payload written by a newer binary that knows a
// health state this one does not into a report of a perfectly healthy shard.
// A health signal must never degrade towards "fine" — the failure mode is a
// monitor that stays green through the incident it exists to catch.
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
//   - ErrShardFailed: the state-machine failure latch tripped on the first
//     apply failure and does not reset on its own.
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
// taken (ErrSnapshotInProgress).
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

// MaybeAppliedError wraps an error whose request had ALREADY been submitted to
// Raft when it failed. The entry may still commit and apply even though this
// caller received an error.
//
// It exists because Temporary() answers "is it reasonable to retry?" and says
// nothing about "did it happen?" — and for a consensus library those are
// different questions with different consequences.
//
// Every failure delivered to a proposal, config change or session operation
// after it was handed to Raft is wrapped in it: leadership moved, the caller's
// own wait ended (its context's deadline or cancellation in a Sync* call, or
// the request's deadline in RequestState.Result), the host's deadline sweep
// expired it, the shard was stopped, closed or halted while it was pending, the
// leader could not persist it (it may already be replicated), or its committed
// entry failed to commit or apply (a reload replays it). Retrying an
// UNSESSIONED proposal after any of these can apply the command twice. A
// proposal is handed to Raft when the step worker takes it from its shard's
// proposal queue; a leadership change fails only proposals already taken. A
// failure that provably happened before the hand-off is never wrapped: an
// admission refusal, a full proposal queue, a proposal still queued when its
// shard halted or stopped, and a batch Raft refused before appending it. Reads
// append nothing and are never wrapped.
//
// Wrapping preserves every existing errors.Is / errors.As / Temporary /
// NotLeader result, so a caller that does not know about this type behaves
// exactly as before.
type MaybeAppliedError struct{ Err error }

// Error returns the wrapped error's message, marked as already submitted.
func (e *MaybeAppliedError) Error() string {
	return e.Err.Error() + " (already submitted to raft; it may still commit)"
}

// Unwrap returns the wrapped error.
func (e *MaybeAppliedError) Unwrap() error { return e.Err }

// MaybeApplied reports whether err's request had been delivered to Raft before
// it failed, so the entry may still commit and apply.
//
// Check this BEFORE acting on Temporary. A blind retry of an unsessioned
// proposal when this returns true can double-apply the command; use a client
// session (Host.GetNewSession / SyncProposeWithSession) if the operation is
// not idempotent, and the session layer will deduplicate the retry.
//
// A nil error returns false.
func MaybeApplied(err error) bool {
	if err == nil {
		return false
	}
	var m *MaybeAppliedError
	return errors.As(err, &m)
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
//
// CHECK MaybeApplied FIRST. A not-leader error is produced at two very
// different points: as a pre-flight refusal (nothing was submitted — safe to
// retry anywhere) and when leadership moves out from under a proposal that is
// ALREADY in the log. The two are structurally identical to a caller, so the
// second is wrapped in *MaybeAppliedError. Retrying that one without a client
// session can apply the command twice, because an unsessioned proposal carries
// ClientID 0 and nothing deduplicates it.
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
