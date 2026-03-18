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
	ErrReplicaRemoved     = quicraft.ErrReplicaNotFound
	ErrNotLeader          = quicraft.ErrNotLeader
	ErrStaleConfigChange  = quicraft.ErrStaleConfigChange
	ErrSnapshotNotFound   = quicraft.ErrSnapshotNotFound
	ErrSnapshotInProgress = quicraft.ErrSnapshotInProgress

	// Distinct dragonboat sentinels (no single quicraft equivalent).
	// Each must be its own value so that errors.Is can distinguish them.
	ErrShardClosed          = errors.New("dragonboat: shard closed")
	ErrShardNotInitialized  = errors.New("dragonboat: shard not initialized")
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

	// ErrNoSnapshot is returned when no snapshot is available for the
	// requested shard.
	ErrNoSnapshot = errors.New("no snapshot available")
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
		errors.Is(err, ErrAborted)
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
	{quicraft.ErrDropped, ErrSystemBusy},
	{quicraft.ErrSystemBusy, ErrSystemBusy},
	{quicraft.ErrTimeout, ErrTimeout},
	{quicraft.ErrCanceled, ErrCanceled},
	{quicraft.ErrRejected, ErrRejected},
	{quicraft.ErrAborted, ErrAborted},
	{quicraft.ErrInvalidSession, ErrInvalidSession},
	{quicraft.ErrEntryTooLarge, ErrPayloadTooBig},
	{quicraft.ErrShardNotReady, ErrShardNotReady},
	{quicraft.ErrShardNotStarted, ErrShardNotInitialized},
	{quicraft.ErrInvalidConfig, ErrInvalidShardSettings},
	{quicraft.ErrInvalidOperation, ErrInvalidOperation},
	{quicraft.ErrInvalidRange, ErrInvalidRange},
	{quicraft.ErrReplicaNotFound, ErrReplicaRemoved},
	{quicraft.ErrNotLeader, ErrNotLeader},
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
// compatible sentinel using errors.Is chains. When no specific mapping
// exists, the original error is returned unchanged.
func translateError(err error) error {
	if err == nil {
		return nil
	}

	for _, m := range errorMappings {
		if errors.Is(err, m.quicraftErr) {
			return m.dragonboatErr
		}
	}

	return err
}
