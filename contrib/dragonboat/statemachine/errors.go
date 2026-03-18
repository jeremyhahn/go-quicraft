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

package statemachine

import "errors"

// Sentinel errors matching dragonboat v4's statemachine package.
var (
	// ErrSnapshotStopped is returned when a snapshot operation is stopped.
	ErrSnapshotStopped = errors.New("snapshot stopped")

	// ErrSnapshotAborted is returned when a snapshot operation is aborted.
	ErrSnapshotAborted = errors.New("snapshot aborted")

	// ErrSnapshotStreaming is returned when streaming a snapshot fails.
	ErrSnapshotStreaming = errors.New("failed to stream the snapshot")

	// ErrOpenStopped is returned when the Open method did not complete.
	ErrOpenStopped = errors.New("open method did not complete")

	// ErrNotImplemented is returned when a requested feature is not implemented.
	ErrNotImplemented = errors.New("requested feature not implemented")
)

// AdapterUpdateError wraps an error returned by the inner state machine's
// Update method, preserving the entry index where the failure occurred.
type AdapterUpdateError struct {
	// Index is the Raft log index of the entry that caused the error.
	Index uint64

	// Cause is the underlying error from the inner state machine.
	Cause error
}

func (e *AdapterUpdateError) Error() string {
	return "statemachine adapter: update failed at index: " + uitoa(e.Index) + ": " + e.Cause.Error()
}

func (e *AdapterUpdateError) Unwrap() error {
	return e.Cause
}

// AdapterLookupError wraps an error returned by the inner state machine's
// Lookup method.
type AdapterLookupError struct {
	Cause error
}

func (e *AdapterLookupError) Error() string {
	return "statemachine adapter: lookup failed: " + e.Cause.Error()
}

func (e *AdapterLookupError) Unwrap() error {
	return e.Cause
}

// AdapterSnapshotSaveError wraps an error returned during snapshot save.
type AdapterSnapshotSaveError struct {
	Cause error
}

func (e *AdapterSnapshotSaveError) Error() string {
	return "statemachine adapter: snapshot save failed: " + e.Cause.Error()
}

func (e *AdapterSnapshotSaveError) Unwrap() error {
	return e.Cause
}

// AdapterSnapshotRecoverError wraps an error returned during snapshot recovery.
type AdapterSnapshotRecoverError struct {
	Cause error
}

func (e *AdapterSnapshotRecoverError) Error() string {
	return "statemachine adapter: snapshot recover failed: " + e.Cause.Error()
}

func (e *AdapterSnapshotRecoverError) Unwrap() error {
	return e.Cause
}

// AdapterCloseError wraps an error returned by the inner state machine's
// Close method.
type AdapterCloseError struct {
	Cause error
}

func (e *AdapterCloseError) Error() string {
	return "statemachine adapter: close failed: " + e.Cause.Error()
}

func (e *AdapterCloseError) Unwrap() error {
	return e.Cause
}

// AdapterOpenError wraps an error returned by the on-disk state machine's
// Open method.
type AdapterOpenError struct {
	Cause error
}

func (e *AdapterOpenError) Error() string {
	return "statemachine adapter: open failed: " + e.Cause.Error()
}

func (e *AdapterOpenError) Unwrap() error {
	return e.Cause
}

// AdapterSyncError wraps an error returned by the on-disk state machine's
// Sync method.
type AdapterSyncError struct {
	Cause error
}

func (e *AdapterSyncError) Error() string {
	return "statemachine adapter: sync failed: " + e.Cause.Error()
}

func (e *AdapterSyncError) Unwrap() error {
	return e.Cause
}

// AdapterPrepareSnapshotError wraps an error returned by PrepareSnapshot.
type AdapterPrepareSnapshotError struct {
	Cause error
}

func (e *AdapterPrepareSnapshotError) Error() string {
	return "statemachine adapter: prepare snapshot failed: " + e.Cause.Error()
}

func (e *AdapterPrepareSnapshotError) Unwrap() error {
	return e.Cause
}

// uitoa converts a uint64 to its decimal string representation without
// importing strconv, keeping the error package dependency-free.
func uitoa(val uint64) string {
	if val == 0 {
		return "0"
	}
	var buf [20]byte // max uint64 is 20 digits
	i := len(buf)
	for val > 0 {
		i--
		buf[i] = byte('0' + val%10)
		val /= 10
	}
	return string(buf[i:])
}
