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

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Dragonboat-compatible error sentinels mapped to quicraft equivalents.
// These allow existing dragonboat error-checking code (errors.Is) to work
// transparently with quicraft's error values.
//
// Errors fall into two categories:
//   - Direct aliases: 1:1 mapping to a quicraft sentinel (same pointer).
//   - Distinct sentinels: errors that dragonboat v4 distinguishes but
//     quicraft merges into a single value. Each gets its own errors.New
//     so that errors.Is can differentiate them.
var (
	// Direct quicraft aliases (1:1 mappings).
	ErrClosed             = quicraft.ErrClosed
	ErrShardNotFound      = quicraft.ErrShardNotFound
	ErrShardAlreadyExist  = quicraft.ErrShardAlreadyExists // singular vs plural
	ErrShardNotStopped    = quicraft.ErrShardNotStopped
	ErrSystemBusy         = quicraft.ErrSystemBusy
	ErrTimeout            = quicraft.ErrTimeout
	ErrCanceled           = quicraft.ErrCanceled
	ErrRejected           = quicraft.ErrRejected
	ErrAborted            = quicraft.ErrAborted
	ErrInvalidSession     = quicraft.ErrInvalidSession
	ErrPayloadTooBig      = quicraft.ErrEntryTooLarge // name mapping
	ErrShardNotReady      = quicraft.ErrShardNotReady
	ErrInvalidOperation   = quicraft.ErrInvalidOperation
	ErrInvalidRange       = quicraft.ErrInvalidRange
	ErrReplicaNotFound    = quicraft.ErrReplicaNotFound
	ErrStaleConfigChange  = quicraft.ErrStaleConfigChange
	ErrSnapshotNotFound   = quicraft.ErrSnapshotNotFound
	ErrSnapshotInProgress = quicraft.ErrSnapshotInProgress

	// Distinct dragonboat sentinels (no single quicraft equivalent).
	// Each must be its own value so that errors.Is can distinguish them.

	// ErrReplicaRemoved indicates the replica's data was permanently
	// removed via RemoveData/SyncRemoveData; the replica can never be
	// restarted on this host. It is distinct from ErrReplicaNotFound
	// (an unrelated lookup miss), so repair loops can safely re-add a
	// replica that merely was not found.
	ErrReplicaRemoved = errors.New("dragonboat: replica permanently removed")

	ErrShardClosed         = errors.New("dragonboat: shard closed")
	ErrShardNotInitialized = errors.New("dragonboat: shard not initialized")

	// ErrShardNotBootstrapped is declared for API compatibility so that
	// migrating code referencing it still compiles. Quicraft has no
	// partial-bootstrap state to report, so this adapter never returns it;
	// a restart of a never-bootstrapped replica surfaces as
	// ErrShardNotInitialized.
	ErrShardNotBootstrapped = errors.New("dragonboat: shard not bootstrapped")

	ErrInvalidShardSettings = errors.New("dragonboat: invalid shard settings")
	ErrInvalidAddress       = errors.New("dragonboat: invalid address")
	ErrDirNotExist          = errors.New("dragonboat: directory does not exist")
	ErrInvalidOption        = errors.New("dragonboat: invalid option")
	ErrInvalidTarget        = errors.New("dragonboat: invalid target")

	// Deadline/timeout programming errors. These are NOT temporary:
	// retrying without fixing the caller's context is pointless.
	ErrDeadlineNotSet  = errors.New("dragonboat: context deadline not set")
	ErrInvalidDeadline = errors.New("dragonboat: invalid deadline")
	ErrTimeoutTooSmall = errors.New("dragonboat: timeout too small")

	// ErrClientIDMismatch is returned when the result of a session
	// close proposal does not match the expected client ID. This
	// indicates a protocol violation where the wrong session was closed.
	ErrClientIDMismatch = errors.New("dragonboat: session close result client ID mismatch")

	// ErrLogDBNotCreatedOrClosed is returned when the LogDB has not been
	// created yet or has already been closed.
	ErrLogDBNotCreatedOrClosed = errors.New("logdb is not created yet or closed already")

	// ErrNoSnapshot is declared for API compatibility so that migrating
	// code referencing it still compiles. Quicraft reports a missing
	// snapshot as ErrSnapshotNotFound, which is what this adapter returns;
	// ErrNoSnapshot is never produced.
	ErrNoSnapshot = errors.New("no snapshot available")

	// ErrNotLeader is a quicraft extension with no dragonboat equivalent.
	// Dragonboat's proposal, read and config-change entry points have no
	// leader check: they accept the request and let it resolve as
	// Dropped(). This adapter therefore never returns ErrNotLeader from a
	// NodeHost method; quicraft's not-leader failures are translated to
	// ErrShardNotReady, which is dragonboat's documented "unknown leader is
	// the most common cause" error. The sentinel is retained only so that
	// code holding a quicraft error can still match it.
	ErrNotLeader = quicraft.ErrNotLeader
)

// IsTempError reports whether the error is temporary and the operation
// should be retried. Temporary errors include system busy, shard not
// ready, timeout, shard closed, shard not initialized, host closed,
// and aborted conditions.
//
// Note: ErrDeadlineNotSet, ErrInvalidDeadline, and ErrTimeoutTooSmall are
// programming errors (broken caller context), NOT temporary conditions.
// Retrying them without fixing the caller is pointless.
func IsTempError(err error) bool {
	return errors.Is(err, ErrSystemBusy) ||
		errors.Is(err, ErrShardNotReady) ||
		errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrShardClosed) ||
		errors.Is(err, ErrShardNotInitialized) ||
		errors.Is(err, ErrClosed) ||
		errors.Is(err, ErrAborted) ||
		// Quicraft extension: a raw not-leader error can still reach a
		// caller that bypassed the adapter, and an election is temporary.
		errors.Is(err, ErrNotLeader)
}

// AdapterError wraps a quicraft or quicraft-internal error that has no
// dragonboat equivalent. Leaking the raw internal error would expose types
// from packages a migrating application cannot import; the wrapper keeps
// the original reachable through errors.Is/errors.As while marking it as
// adapter-translated.
type AdapterError struct {
	Err error
}

// Error returns a human-readable description of the untranslated error.
func (e *AdapterError) Error() string {
	return "dragonboat: untranslated quicraft error: " + e.Err.Error()
}

// Unwrap returns the underlying quicraft error.
func (e *AdapterError) Unwrap() error {
	return e.Err
}

// CompactionIndexNotSupportedError is returned when SnapshotOption.CompactionIndex
// is set to a non-zero value. QuicRaft does not support CompactionIndex; use
// CompactionOverhead instead.
type CompactionIndexNotSupportedError struct{}

// Error returns a human-readable description of the unsupported option.
func (e *CompactionIndexNotSupportedError) Error() string {
	return "dragonboat: CompactionIndex is not supported by QuicRaft; use CompactionOverhead instead"
}

// Is reports whether target matches the sentinel ErrInvalidOption error.
// This preserves backward compatibility for callers using errors.Is(err, ErrInvalidOption).
func (e *CompactionIndexNotSupportedError) Is(target error) bool {
	return target == ErrInvalidOption
}

// ShardSettingsError wraps a shard configuration validation failure from
// the Start*Replica methods. It matches ErrInvalidShardSettings via
// errors.Is while preserving the underlying validation detail.
type ShardSettingsError struct {
	Err error
}

// Error returns a human-readable description of the invalid settings.
func (e *ShardSettingsError) Error() string {
	return "dragonboat: invalid shard settings: " + e.Err.Error()
}

// Unwrap returns the underlying validation error.
func (e *ShardSettingsError) Unwrap() error {
	return e.Err
}

// Is reports whether target matches the ErrInvalidShardSettings sentinel,
// preserving dragonboat's error identity for callers using errors.Is.
func (e *ShardSettingsError) Is(target error) bool {
	return target == ErrInvalidShardSettings
}

// RemovalMarkerError wraps a filesystem failure while persisting or
// loading the on-disk removed-replica markers that back ErrReplicaRemoved
// across restarts.
type RemovalMarkerError struct {
	Op   string
	Path string
	Err  error
}

// Error returns a human-readable description of the marker failure.
func (e *RemovalMarkerError) Error() string {
	return "dragonboat: removed-replica marker " + e.Op + " failed: " + e.Path + ": " + e.Err.Error()
}

// Unwrap returns the underlying filesystem error.
func (e *RemovalMarkerError) Unwrap() error {
	return e.Err
}

// errorMapping pairs a quicraft error with its dragonboat-compatible sentinel.
type errorMapping struct {
	quicraftErr   error
	dragonboatErr error
}

// errorMappings is the package-level translation table used by translateError.
// Order matters: more specific errors are checked first so that wrapped errors
// match the tightest dragonboat sentinel.
var errorMappings = []errorMapping{
	{quicraft.ErrClosed, ErrClosed},
	{quicraft.ErrStopped, ErrShardClosed},
	{quicraft.ErrShardNotFound, ErrShardNotFound},
	{quicraft.ErrShardAlreadyExists, ErrShardAlreadyExist},
	{quicraft.ErrShardNotStopped, ErrShardNotStopped},
	{quicraft.ErrBusy, ErrSystemBusy},
	{quicraft.ErrDraining, ErrSystemBusy},
	// Dropped means the leader is unavailable or not ready. Dragonboat's
	// getRequestState maps Dropped() to ErrShardNotReady, which callers
	// treat as an election wait rather than as backpressure.
	{quicraft.ErrDropped, ErrShardNotReady},
	{quicraft.ErrSystemBusy, ErrSystemBusy},
	{quicraft.ErrTimeout, ErrTimeout},
	{quicraft.ErrCanceled, ErrCanceled},
	// Context failures reach the adapter from quicraft's ctx-aware paths.
	// Untranslated they would leak stdlib errors that no errors.Is(err,
	// ErrTimeout) check can match.
	{context.DeadlineExceeded, ErrTimeout},
	{context.Canceled, ErrCanceled},
	{quicraft.ErrRejected, ErrRejected},
	{quicraft.ErrAborted, ErrAborted},
	{quicraft.ErrInvalidSession, ErrInvalidSession},
	{quicraft.ErrEntryTooLarge, ErrPayloadTooBig},
	{quicraft.ErrShardNotReady, ErrShardNotReady},
	{quicraft.ErrShardNotStarted, ErrShardNotInitialized},
	{quicraft.ErrInvalidConfig, ErrInvalidShardSettings},
	{quicraft.ErrInvalidOperation, ErrInvalidOperation},
	{quicraft.ErrInvalidRange, ErrInvalidRange},
	{quicraft.ErrReplicaNotFound, ErrReplicaNotFound},
	// Dragonboat has no not-leader error: its propose/read/config-change
	// entry points accept the request and let it resolve as Dropped().
	// ErrShardNotReady is the documented equivalent ("unknown leader is the
	// most common cause") and, unlike ErrNotLeader, satisfies IsTempError
	// in a migrated retry loop.
	{quicraft.ErrNotLeader, ErrShardNotReady},
	{quicraft.ErrStaleConfigChange, ErrStaleConfigChange},
	{quicraft.ErrSnapshotNotFound, ErrSnapshotNotFound},
	{quicraft.ErrSnapshotInProgress, ErrSnapshotInProgress},
	// Session errors: map to dragonboat session/rejection sentinels.
	{quicraft.ErrSessionNotFound, ErrInvalidSession},
	{quicraft.ErrSessionExpired, ErrRejected},
	{quicraft.ErrSessionAlreadyExists, ErrRejected},
	// ReadIndex not ready: leader hasn't committed in current term yet.
	{quicraft.ErrReadIndexNotReady, ErrShardNotReady},
	// Shard failed (SM circuit breaker): map to ErrShardClosed.
	{quicraft.ErrShardFailed, ErrShardClosed},
	// Session limit exceeded: map to ErrSystemBusy (dragonboat has no
	// separate session limit error; overloaded sessions are a busy signal).
	{quicraft.ErrSessionLimitExceeded, ErrSystemBusy},
	// LogDB write failure: transient I/O error, map to ErrSystemBusy.
	{quicraft.ErrLogDBWrite, ErrSystemBusy},
	// Disk full: resource exhaustion, map to ErrSystemBusy.
	{quicraft.ErrDiskFull, ErrSystemBusy},
	// Snapshot recovery failed: map to ErrAborted.
	{quicraft.ErrSnapshotRecoveryFailed, ErrAborted},
	// Snapshot size mismatch: map to ErrAborted.
	{quicraft.ErrSnapshotSizeMismatch, ErrAborted},
	// Response limit exceeded: map to ErrPayloadTooBig.
	{quicraft.ErrResponseLimitExceeded, ErrPayloadTooBig},
	// Invalid SM type: map to ErrInvalidShardSettings.
	{quicraft.ErrInvalidSMType, ErrInvalidShardSettings},
}

// translateError converts a quicraft error to the closest dragonboat-
// compatible sentinel. Errors that already carry a dragonboat identity
// (the adapter's own typed errors) pass through unchanged; anything with
// no mapping is wrapped in AdapterError rather than leaking a quicraft or
// quicraft-internal type to a migrating caller.
func translateError(err error) error {
	if err == nil {
		return nil
	}

	// InvalidMemberError has no Is method, so it cannot be matched through
	// the errors.Is table. Dragonboat rejects an invalid leader-transfer or
	// membership target with ErrInvalidTarget.
	var invalidMember *quicraft.InvalidMemberError
	if errors.As(err, &invalidMember) {
		return ErrInvalidTarget
	}

	for _, m := range errorMappings {
		if errors.Is(err, m.quicraftErr) {
			return m.dragonboatErr
		}
	}

	if isAdapterError(err) {
		return err
	}

	return &AdapterError{Err: err}
}

// isAdapterError reports whether err already carries a dragonboat identity
// produced by this package, in which case translateError must not re-wrap
// it. The check is by sentinel rather than by concrete type so that future
// adapter error types are covered as long as they match a sentinel.
func isAdapterError(err error) bool {
	for _, target := range adapterSentinels {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// adapterSentinels lists the dragonboat sentinels this package produces
// directly (not via errorMappings), so that translateError recognizes an
// already-translated error.
var adapterSentinels = []error{
	ErrReplicaRemoved,
	ErrShardClosed,
	ErrShardNotInitialized,
	ErrShardNotBootstrapped,
	ErrInvalidShardSettings,
	ErrInvalidAddress,
	ErrDirNotExist,
	ErrInvalidOption,
	ErrInvalidTarget,
	ErrDeadlineNotSet,
	ErrInvalidDeadline,
	ErrTimeoutTooSmall,
	ErrClientIDMismatch,
	ErrLogDBNotCreatedOrClosed,
	ErrNoSnapshot,
}
