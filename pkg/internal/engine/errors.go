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

// Engine-internal sentinel errors; the quicraft package defines the public equivalents.
var (
	// ErrShardFailed identifies a shard whose failure latch tripped; matched through *SMPanicError, re-exported publicly.
	ErrShardFailed = errors.New("quicraft: shard failed (SM circuit breaker tripped)")

	// ErrDropped reports a request a worker could not hand on; re-exported as quicraft.ErrDropped.
	ErrDropped = errors.New("quicraft: proposal dropped")

	// ErrShardPaused reports work refused because the replica is paused; re-exported publicly.
	ErrShardPaused = errors.New("quicraft: shard paused")

	// ErrStopped is returned when an operation is interrupted because
	// the stopper channel was closed.
	ErrStopped = errors.New("engine: operation stopped")

	// ErrSnapshotCorrupt reports an invalid snapshot frame structure; re-exported publicly.
	ErrSnapshotCorrupt = errors.New("quicraft: snapshot frame corrupt")

	// ErrSnapshotTooLarge is returned when a snapshot data file exceeds
	// the configured Config.MaxSnapshotSize limit.
	ErrSnapshotTooLarge = errors.New("engine: snapshot exceeds MaxSnapshotSize")

	// ErrSnapshotFrameWriterClosed reports a write after the trailing frame: a caller bug.
	ErrSnapshotFrameWriterClosed = errors.New("engine: snapshot frame writer is closed")

	// ErrSnapshotMetaCorrupt reports an unreadable snapshot.meta; the purge floor treats the directory as unevaluable.
	ErrSnapshotMetaCorrupt = errors.New("engine: snapshot metadata record is corrupt or foreign")

	// ErrSnapshotEncryptedBarrierUnavailable reports an encrypted snapshot whose barrier is absent or sealed; re-exported publicly.
	ErrSnapshotEncryptedBarrierUnavailable = errors.New(
		"quicraft: snapshot is encrypted but barrier is unavailable for decryption")

	// ErrSnapshotBarrierSealed reports a snapshot write while the configured barrier is sealed; re-exported publicly.
	ErrSnapshotBarrierSealed = errors.New(
		"quicraft: snapshot encryption unavailable: barrier is sealed")

	// ErrShardBarrierUnresolved reports a shard with no per-shard or process-wide barrier; re-exported publicly.
	ErrShardBarrierUnresolved = errors.New(
		"quicraft: per-shard barriers resolved no barrier for this shard and no " +
			"host-wide barrier is configured; refusing to write it in plaintext")

	// ErrUnsupportedSnapshotCompression reports a snapshot compression this binary cannot decompress.
	ErrUnsupportedSnapshotCompression = errors.New(
		"quicraft: snapshot declares an unsupported compression algorithm")

	// ErrSnapshotIndexNotAdvanced reports an export requested at an index the durable snapshot already covers.
	ErrSnapshotIndexNotAdvanced = errors.New(
		"engine: snapshot index has not advanced since the last snapshot")

	// ErrSnapshotSizeMismatch reports a data file whose size differs from its LogDB record; re-exported publicly.
	ErrSnapshotSizeMismatch = errors.New("quicraft: snapshot file size mismatch")

	// ErrSnapshotMembershipAhead reports a save whose membership folded a change above the snapshot index.
	ErrSnapshotMembershipAhead = errors.New(
		"engine: snapshot membership is ahead of the snapshot index")

	// ErrScratchNotUnlinked reports a decrypt scratch file that could not be unlinked on creation; the operation fails and is retried.
	ErrScratchNotUnlinked = errors.New(
		"engine: snapshot decrypt scratch could not be unlinked")

	// ErrApplyGap reports a committed batch not starting at lastApplied+1; the shard is failed.
	ErrApplyGap = errors.New("engine: committed entry gap detected before apply")

	// errBarrierWithoutIdentity reports a barrier with no pointer identity; the per-barrier purge floor declines on it.
	errBarrierWithoutIdentity = errors.New("engine: barrier has no pointer identity")
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

// SnapshotPoolBusyError reports a full snapshot request channel; retry after a backoff.
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

// SnapshotRecoveryBehindError reports a recovery requested above the raft log's committed and processed markers; transient and retryable.
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

// Retryable reports that the recovery may be re-requested unchanged; always true.
func (e *SnapshotRecoveryBehindError) Retryable() bool { return true }

// SnapshotRecoveryStaleError reports a recovery not ahead of the apply frontier; refused before restore, the shard keeps running.
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

// recoveryRejectedError marks a recovery that failed before RecoverFromSnapshot; it wraps the step's error.
type recoveryRejectedError struct {
	err error
}

// Error returns the wrapped error's description.
func (e *recoveryRejectedError) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error.
func (e *recoveryRejectedError) Unwrap() error {
	return e.err
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

// SnapshotMembershipAheadError reports a save whose membership folded a change above the snapshot index; transient until apply passes it.
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

// SnapshotSizeMismatchError reports a data file whose size differs from its LogDB record, with both sizes.
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

// ScratchUnlinkError reports a decrypt scratch file whose immediate unlink
// failed. Nothing was written into it; Err is the refused unlink.
type ScratchUnlinkError struct {
	Path string
	Err  error
}

// Error returns a human-readable description of the refused unlink.
func (e *ScratchUnlinkError) Error() string {
	return fmt.Sprintf("engine: snapshot decrypt scratch %s could not be unlinked: %s",
		e.Path, e.Err.Error())
}

// Unwrap returns the underlying unlink error.
func (e *ScratchUnlinkError) Unwrap() error {
	return e.Err
}

// Is reports whether target matches the sentinel ErrScratchNotUnlinked.
func (e *ScratchUnlinkError) Is(target error) bool {
	return target == ErrScratchNotUnlinked
}

// SnapshotMetaInvalidError reports a snapshot.meta that cannot vouch for its data file; it matches ErrSnapshotCorrupt and ErrSnapshotMetaCorrupt.
type SnapshotMetaInvalidError struct {
	Dir string
	Err error
}

// Error returns a human-readable description of the unusable record.
func (e *SnapshotMetaInvalidError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf(
			"engine: snapshot metadata in %s carries no integrity fields", e.Dir)
	}
	return fmt.Sprintf(
		"engine: snapshot metadata in %s cannot verify the artifact: %v", e.Dir, e.Err)
}

// Is reports whether target matches ErrSnapshotCorrupt or
// ErrSnapshotMetaCorrupt.
func (e *SnapshotMetaInvalidError) Is(target error) bool {
	return target == ErrSnapshotCorrupt || target == ErrSnapshotMetaCorrupt
}

// Unwrap returns the underlying read or parse error, if any.
func (e *SnapshotMetaInvalidError) Unwrap() error { return e.Err }

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

// SnapshotSavePanicError reports a panicking SaveSnapshot; the shard is not failed.
type SnapshotSavePanicError struct {
	ShardID   uint64
	ReplicaID uint64
	Value     interface{}
	Stack     []byte
}

// Error returns a human-readable description of the save panic.
func (e *SnapshotSavePanicError) Error() string {
	return fmt.Sprintf(
		"engine: state machine SaveSnapshot panicked for shard %d replica %d: %v",
		e.ShardID, e.ReplicaID, e.Value)
}

// SnapshotRecoveryPanicError reports a recovery panic before RecoverFromSnapshot; the recovery is rejected, the shard is not failed.
type SnapshotRecoveryPanicError struct {
	ShardID   uint64
	ReplicaID uint64
	Value     interface{}
	Stack     []byte
}

// Error returns a human-readable description of the recovery panic.
func (e *SnapshotRecoveryPanicError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot recovery panicked before the state machine restore for shard %d replica %d: %v",
		e.ShardID, e.ReplicaID, e.Value)
}

// SnapshotRecoveryExhaustedError reports a shard failed after every recovery attempt for an adopted snapshot was rejected.
type SnapshotRecoveryExhaustedError struct {
	ShardID       uint64
	ReplicaID     uint64
	SnapshotIndex uint64
	Attempts      int
	Err           error
}

// Error returns a human-readable description of the exhausted recovery.
func (e *SnapshotRecoveryExhaustedError) Error() string {
	return fmt.Sprintf(
		"engine: snapshot recovery for shard %d replica %d rejected %d times, the last at index %d: %v",
		e.ShardID, e.ReplicaID, e.Attempts, e.SnapshotIndex, e.Err)
}

// Unwrap returns the last attempt's cause.
func (e *SnapshotRecoveryExhaustedError) Unwrap() error {
	return e.Err
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

// MessageHandleError reports a Raft message Peer.Handle could not process; non-fatal.
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

// WorkerPanicError reports a panicking step or commit worker, which affects every shard in its partition.
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

// TypeAssertionError reports an internal type assertion failure on stored data.
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
