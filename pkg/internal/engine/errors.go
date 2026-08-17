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

package engine

import (
	"errors"
	"fmt"
)

// Engine-specific sentinel errors. These are independent of the quicraft
// package to avoid import cycles (quicraft imports engine, engine must
// not import quicraft). The quicraft package defines equivalent errors
// for the public API; these are the engine-internal variants.
var (
	// ErrShardFailed is the identity of a shard whose failure latch has
	// tripped. It is never returned directly: *SMPanicError matches it through
	// its own Is method, which is how the condition reaches a caller.
	ErrShardFailed = errors.New("engine: shard failed (SM circuit breaker tripped)")

	// ErrStopped is returned when an operation is interrupted because
	// the stopper channel was closed.
	ErrStopped = errors.New("engine: operation stopped")

	// ErrSnapshotCorrupt is returned when a snapshot data file has an
	// invalid frame structure during decryption.
	ErrSnapshotCorrupt = errors.New("engine: snapshot data corrupt")

	// ErrSnapshotTooLarge is returned when a snapshot data file exceeds
	// the configured Config.MaxSnapshotSize limit.
	ErrSnapshotTooLarge = errors.New("engine: snapshot exceeds MaxSnapshotSize")

	// ErrSnapshotMetaCorrupt is returned by ParseSnapshotMeta when a
	// snapshot.meta image is neither a versioned record nor one of the
	// recognised pre-magic layouts. It means "these bytes say nothing about
	// the artifact beside them", NOT "the artifact is unencrypted": the DEK
	// purge floor must treat the directory as unevaluable.
	ErrSnapshotMetaCorrupt = errors.New("engine: snapshot metadata record is corrupt or foreign")

	// ErrSnapshotEncryptedBarrierUnavailable is returned when a snapshot
	// recorded with Epoch > 0 cannot be read because the barrier required to
	// decrypt it is absent or sealed. Recovery fails so the leader re-sends
	// rather than handing ciphertext to RecoverFromSnapshot.
	//
	// This sentinel is declared here — not in the quicraft package — because
	// both the runtime recovery path (snapshotPool.recoverSnapshot) and the
	// startup path (Host.StartShard) must fail with the SAME error identity,
	// and quicraft imports engine. The quicraft package re-exports it, which
	// is why the message keeps the public "quicraft:" prefix.
	ErrSnapshotEncryptedBarrierUnavailable = errors.New(
		"quicraft: snapshot is encrypted but barrier is unavailable for decryption")

	// ErrSnapshotBarrierSealed is returned when a snapshot must be written
	// (saved locally or re-encrypted after a receive) while the shard's
	// barrier is CONFIGURED but SEALED. It is the write-side counterpart of
	// ErrSnapshotEncryptedBarrierUnavailable and exists to keep the two
	// barrier states distinct: a nil barrier means encryption is DISABLED for
	// the shard (plaintext is the configured outcome), whereas a sealed
	// barrier means encryption is temporarily UNAVAILABLE. Producing a
	// plaintext artifact in the second case would durably downgrade a shard
	// whose data is required to be encrypted at rest.
	//
	// Declared here, like ErrSnapshotEncryptedBarrierUnavailable, so the
	// engine save path and the quicraft receive path fail with the SAME error
	// identity; the quicraft package re-exports it, hence the public prefix.
	ErrSnapshotBarrierSealed = errors.New(
		"quicraft: snapshot encryption unavailable: barrier is sealed")

	// ErrShardBarrierUnresolved is returned when per-shard barriers are
	// configured, the resolver returns nil for a shard, and there is no
	// process-wide barrier to fall back to. Writing the shard's snapshot in
	// plaintext would downgrade exactly one tenant in a deployment that asked
	// for per-shard encryption, so the operation fails closed instead.
	//
	// Declared here, like the other snapshot barrier sentinels, so the engine
	// save/recover paths and the quicraft host paths fail with the SAME error
	// identity; the quicraft package re-exports it, hence the public prefix.
	ErrShardBarrierUnresolved = errors.New(
		"quicraft: per-shard barriers resolved no barrier for this shard and no " +
			"host-wide barrier is configured; refusing to write it in plaintext")

	// ErrUnsupportedSnapshotCompression is returned when a snapshot record
	// declares a compression algorithm this binary cannot decompress. The
	// artifact is unreadable, so recovery fails instead of feeding the state
	// machine bytes in an unknown encoding.
	ErrUnsupportedSnapshotCompression = errors.New(
		"quicraft: snapshot declares an unsupported compression algorithm")

	// ErrSnapshotIndexNotAdvanced is returned when a snapshot EXPORT is
	// requested at an index the durable snapshot already covers. The save is a
	// no-op — re-saving would truncate the artifact LogDB records — but an
	// export caller asked for a directory that the no-op cannot produce, so the
	// request fails instead of reporting a success with nothing exported.
	ErrSnapshotIndexNotAdvanced = errors.New(
		"engine: snapshot index has not advanced since the last snapshot")

	// ErrSnapshotSizeMismatch is returned when a snapshot data file's size on
	// disk does not match the FileSize its LogDB record claims. It is the
	// sentinel form of *SnapshotSizeMismatchError.
	ErrSnapshotSizeMismatch = errors.New(
		"engine: snapshot file size does not match the size recorded for it")

	// ErrSnapshotMembershipAhead is returned when a snapshot save is asked to
	// record a membership that folded a config change ABOVE the snapshot's own
	// index. It is the sentinel form of *SnapshotMembershipAheadError.
	ErrSnapshotMembershipAhead = errors.New(
		"engine: snapshot membership is ahead of the snapshot index")

	// ErrApplyGap is returned when a committed batch does not start at
	// lastApplied+1. The missing range can never be re-offered (Raft's
	// processed marker already advanced past it), so the shard is failed
	// instead of applying a batch that would leave a hole in the state
	// machine.
	ErrApplyGap = errors.New("engine: committed entry gap detected before apply")
)

// SaveStateError is returned when a LogDB SaveState operation fails after
// all retry attempts have been exhausted.
type SaveStateError struct {
	ShardID   uint64
	ReplicaID uint64
	Err       error
}

// Error returns a human-readable description of the SaveState failure.
func (e *SaveStateError) Error() string {
	return fmt.Sprintf("engine: SaveState failed for shard %d replica %d: %s",
		e.ShardID, e.ReplicaID, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *SaveStateError) Unwrap() error {
	return e.Err
}

// ApplyError is returned when state machine Apply fails for a shard.
type ApplyError struct {
	ShardID   uint64
	ReplicaID uint64
	Err       error
}

// Error returns a human-readable description of the Apply failure.
func (e *ApplyError) Error() string {
	return fmt.Sprintf("engine: SM.Apply failed for shard %d replica %d: %s",
		e.ShardID, e.ReplicaID, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *ApplyError) Unwrap() error {
	return e.Err
}

// SnapshotDuplicateError is returned when a snapshot request is submitted
// for a shard that already has an in-flight snapshot operation.
type SnapshotDuplicateError struct {
	ShardID uint64
}

// Error returns a human-readable description of the duplicate snapshot.
func (e *SnapshotDuplicateError) Error() string {
	return fmt.Sprintf("engine: snapshot already in-flight for shard %d", e.ShardID)
}

// SnapshotPoolBusyError is returned when a snapshot request cannot be
// enqueued because the snapshot pool's request channel is full. Unlike
// SnapshotDuplicateError, this does not indicate a duplicate request —
// it means all snapshot workers are occupied and the request should be
// retried after a backoff.
type SnapshotPoolBusyError struct {
	ShardID uint64
}

// Error returns a human-readable description of the busy pool.
func (e *SnapshotPoolBusyError) Error() string {
	return fmt.Sprintf("engine: snapshot pool busy for shard %d, retry later", e.ShardID)
}

// SnapshotError is returned when a snapshot save or recover operation fails.
type SnapshotError struct {
	ShardID   uint64
	ReplicaID uint64
	Save      bool
	Err       error
}

// Error returns a human-readable description of the snapshot failure.
func (e *SnapshotError) Error() string {
	op := "recover"
	if e.Save {
		op = "save"
	}
	return fmt.Sprintf("engine: snapshot %s failed for shard %d replica %d: %s",
		op, e.ShardID, e.ReplicaID, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *SnapshotError) Unwrap() error {
	return e.Err
}

// CommitError is returned when a Peer.Commit operation fails, typically
// due to a log gap detected in the LogReader during entry range updates.
type CommitError struct {
	ShardID   uint64
	ReplicaID uint64
	Err       error
}

// Error returns a human-readable description of the commit failure.
func (e *CommitError) Error() string {
	return fmt.Sprintf("engine: Commit failed for shard %d replica %d: %s",
		e.ShardID, e.ReplicaID, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *CommitError) Unwrap() error {
	return e.Err
}

// SnapshotRecoveryBehindError is returned when a state machine recovery is
// requested for a snapshot index the Raft log has not yet committed AND
// processed. Advancing the node's apply markers to that index would make the
// next Update carry a LastApplied greater than the raft log's
// committed/processed markers, which the commit path asserts against.
//
// The condition is TRANSIENT by construction: the InstallSnapshot that moves
// the markers is queued on the node inbox and installed by the step worker,
// which runs concurrently with the recovery request. The state machine is
// left untouched, so the caller may retry once the markers catch up.
type SnapshotRecoveryBehindError struct {
	ShardID       uint64
	ReplicaID     uint64
	SnapshotIndex uint64
	Committed     uint64
	Processed     uint64
}

// Error returns a human-readable description of the recovery mismatch.
func (e *SnapshotRecoveryBehindError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot recovery for shard %d replica %d at index %d is ahead of the raft log (committed %d, processed %d)",
		e.ShardID, e.ReplicaID, e.SnapshotIndex, e.Committed, e.Processed)
}

// Retryable reports that the recovery may be re-requested unchanged. It is
// always true: the snapshot is durable and the state machine was not touched,
// so the same request succeeds as soon as the step worker installs the
// snapshot into the raft log.
func (e *SnapshotRecoveryBehindError) Retryable() bool { return true }

// SnapshotRecoveryStaleError is returned when a state machine recovery is
// requested for a snapshot that is NOT strictly ahead of the state machine's
// apply frontier. Recovering it would REPLACE the state machine with older
// state and REGRESS lastApplied, discarding every entry applied between the
// snapshot index and the current frontier; the next committed batch would then
// start past lastApplied+1 and fail the shard with ErrApplyGap.
//
// It is reachable whenever a leader ships an InstallSnapshot below the
// follower's real progress (a leader change or rejection walk-down that drives
// remote.next backwards). Raft itself rejects such a snapshot, so the engine
// must refuse it too rather than recover behind raft's back.
type SnapshotRecoveryStaleError struct {
	ShardID       uint64
	ReplicaID     uint64
	SnapshotIndex uint64
	LastApplied   uint64
}

// Error returns a human-readable description of the stale recovery.
func (e *SnapshotRecoveryStaleError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot recovery for shard %d replica %d at index %d is not ahead of applied index %d",
		e.ShardID, e.ReplicaID, e.SnapshotIndex, e.LastApplied)
}

// SnapshotExportError is returned when exporting a snapshot to the
// user-specified ExportPath fails.
type SnapshotExportError struct {
	ExportPath string
	Err        error
}

// Error returns a human-readable description of the export failure.
func (e *SnapshotExportError) Error() string {
	return fmt.Sprintf("engine: snapshot export to %s failed: %s", e.ExportPath, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *SnapshotExportError) Unwrap() error {
	return e.Err
}

// SnapshotEncryptError is returned when snapshot encryption or decryption
// fails during save or recover.
type SnapshotEncryptError struct {
	Op  string
	Err error
}

// Error returns a human-readable description of the encryption failure.
func (e *SnapshotEncryptError) Error() string {
	return fmt.Sprintf("engine: snapshot %s failed: %s", e.Op, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *SnapshotEncryptError) Unwrap() error {
	return e.Err
}

// SnapshotTooLargeError is returned when a snapshot data file exceeds
// the configured Config.MaxSnapshotSize limit. It includes the actual
// file size and the configured limit for diagnostic purposes.
type SnapshotTooLargeError struct {
	ShardID   uint64
	ReplicaID uint64
	Size      uint64
	Limit     uint64
}

// Error returns a human-readable description of the size violation.
func (e *SnapshotTooLargeError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot for shard %d replica %d is %d bytes, exceeds limit %d",
		e.ShardID, e.ReplicaID, e.Size, e.Limit)
}

// Is reports whether target matches the sentinel ErrSnapshotTooLarge error.
func (e *SnapshotTooLargeError) Is(target error) bool {
	return target == ErrSnapshotTooLarge
}

// SnapshotMembershipAheadError reports a snapshot save whose captured
// membership had already folded a config change committed ABOVE the snapshot's
// index.
//
// The snapshot's index is the APPLY frontier and its membership is folded at
// COMMIT time, and commit runs ahead of apply, so the two can disagree.
// Recording the pair anyway produces a snapshot claiming a configuration that
// did not exist at its own index: a follower installing it adopts the later
// membership, sets its fold marker to the snapshot's index, and re-folds every
// config change in between — while computing quorums, until it catches up, from
// a voting set the log at that index does not justify.
//
// It is TRANSIENT by construction: it lasts only until the apply frontier
// passes the config change, which returning this error permits by releasing the
// save's apply gate. The next snapshot trigger takes it.
type SnapshotMembershipAheadError struct {
	ShardID       uint64
	ReplicaID     uint64
	SnapshotIndex uint64
	// ConfigChangeID is the log index of the last config change the captured
	// membership folded.
	ConfigChangeID uint64
}

// Error returns a human-readable description of the frontier disagreement.
func (e *SnapshotMembershipAheadError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot for shard %d replica %d at index %d would record a membership "+
			"whose last config change is at index %d, above the snapshot",
		e.ShardID, e.ReplicaID, e.SnapshotIndex, e.ConfigChangeID)
}

// Is reports whether target matches the sentinel ErrSnapshotMembershipAhead.
func (e *SnapshotMembershipAheadError) Is(target error) bool {
	return target == ErrSnapshotMembershipAhead
}

// Retryable reports that the save may be re-requested unchanged. It is always
// true: nothing was written, and the apply frontier advances past the config
// change as soon as this error releases the save's apply gate.
func (e *SnapshotMembershipAheadError) Retryable() bool { return true }

// SnapshotSizeMismatchError reports a snapshot data file whose size on disk
// disagrees with the FileSize its LogDB record claims. It carries both sizes,
// because the direction is diagnostic: a SHORTER file is a truncation (a full
// volume, a torn copy, a partial restore) and a LONGER one means the record
// describes a different artifact than the directory holds.
type SnapshotSizeMismatchError struct {
	Path     string
	Recorded uint64
	OnDisk   uint64
}

// Error returns a human-readable description of the size disagreement.
func (e *SnapshotSizeMismatchError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot %s is %d bytes on disk but its record claims %d",
		e.Path, e.OnDisk, e.Recorded)
}

// Is reports whether target matches the sentinel ErrSnapshotSizeMismatch.
func (e *SnapshotSizeMismatchError) Is(target error) bool {
	return target == ErrSnapshotSizeMismatch
}

// SMPanicError is returned when a user state machine panics during Apply
// or a related operation. The shard is marked as failed and becomes
// unavailable, but the host process continues running other shards.
type SMPanicError struct {
	ShardID   uint64
	ReplicaID uint64
	Value     interface{}
	Stack     []byte
}

// Error returns a human-readable description of the state machine panic.
func (e *SMPanicError) Error() string {
	return fmt.Sprintf(
		"engine: state machine panic for shard %d replica %d: %v",
		e.ShardID, e.ReplicaID, e.Value)
}

// Is reports whether target matches the sentinel ErrShardFailed error.
// A panicked state machine is a permanently failed shard.
func (e *SMPanicError) Is(target error) bool {
	return target == ErrShardFailed
}

// NodeNotLoadedError is returned when an operation targets a shard that
// is not loaded in the engine.
type NodeNotLoadedError struct {
	ShardID uint64
}

// Error returns a human-readable description.
func (e *NodeNotLoadedError) Error() string {
	return fmt.Sprintf("engine: node not loaded for shard %d", e.ShardID)
}

// MessageHandleError is returned when Peer.Handle fails to process an
// incoming Raft message. This error is non-fatal and logged at warn
// level for observability. It typically indicates a message from a
// stale term, an unknown sender, or a corrupted message.
type MessageHandleError struct {
	ShardID   uint64
	ReplicaID uint64
	MsgType   uint64
	From      uint64
	Err       error
}

// Error returns a human-readable description of the message handling failure.
func (e *MessageHandleError) Error() string {
	return fmt.Sprintf("engine: Handle failed for shard %d replica %d (msgType=%d from=%d): %s",
		e.ShardID, e.ReplicaID, e.MsgType, e.From, e.Err.Error())
}

// Unwrap returns the underlying error.
func (e *MessageHandleError) Unwrap() error {
	return e.Err
}

// WorkerPanicError is returned when a step or commit worker goroutine
// panics. The worker ID and type identify which goroutine failed. The
// stack trace captures the panic origin for debugging. Unlike SM panics
// (SMPanicError), worker panics affect all shards in the worker partition,
// not just a single shard.
type WorkerPanicError struct {
	WorkerID   int
	WorkerType string // "step" or "commit"
	Value      interface{}
	Stack      []byte
}

// Error returns a human-readable description of the worker panic.
func (e *WorkerPanicError) Error() string {
	return fmt.Sprintf(
		"engine: %s worker %d panicked: %v",
		e.WorkerType, e.WorkerID, e.Value)
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
		"engine: type assertion failed in %s: expected %s, got %T",
		e.Context, e.ExpectedType, e.ActualValue,
	)
}
