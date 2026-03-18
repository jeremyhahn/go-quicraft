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
	// ErrShardFailed is returned when the state machine circuit breaker
	// has tripped due to repeated apply failures.
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

// CircuitBreakerError is returned when the apply circuit breaker trips
// for a shard after MaxApplyRetries consecutive failures.
type CircuitBreakerError struct {
	ShardID          uint64
	ReplicaID        uint64
	ConsecutiveFails uint64
	LastErr          error
}

// Error returns a human-readable description of the circuit breaker trip.
func (e *CircuitBreakerError) Error() string {
	return fmt.Sprintf(
		"engine: circuit breaker tripped for shard %d replica %d after %d consecutive failures: %s",
		e.ShardID, e.ReplicaID, e.ConsecutiveFails, e.LastErr.Error())
}

// Unwrap returns the underlying error.
func (e *CircuitBreakerError) Unwrap() error {
	return e.LastErr
}

// Is reports whether target matches the sentinel ErrShardFailed error.
func (e *CircuitBreakerError) Is(target error) bool {
	return target == ErrShardFailed
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
