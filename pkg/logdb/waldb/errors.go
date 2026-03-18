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

package waldb

import "errors"

// Sentinel errors for WAL operations. Pre-allocated for zero-alloc returns
// and use with errors.Is().
var (
	// ErrCorruptedRecord is returned when a WAL record fails CRC verification
	// or has an otherwise invalid format.
	ErrCorruptedRecord = errors.New("waldb: corrupted record")

	// ErrSegmentFull is returned when a write exceeds the segment's maximum
	// file size.
	ErrSegmentFull = errors.New("waldb: segment full")

	// ErrClosed is returned when an operation is attempted on a closed segment.
	ErrClosed = errors.New("waldb: segment closed")

	// ErrInvalidBlockSize is returned when the configured block size is not a
	// power of two or is smaller than the minimum required to hold a record header.
	ErrInvalidBlockSize = errors.New("waldb: invalid block size")

	// ErrBufferTooSmall is returned when a provided buffer is too small to hold
	// a record header plus any data.
	ErrBufferTooSmall = errors.New("waldb: buffer too small")

	// ErrInvalidRecordType is returned when a record header contains an
	// unrecognized type byte.
	ErrInvalidRecordType = errors.New("waldb: invalid record type")

	// ErrEmptyWALDir is returned when Options.WALDir is empty.
	ErrEmptyWALDir = errors.New("waldb: WAL directory must not be empty")

	// ErrInvalidMaxFileSize is returned when Options.MaxFileSize is not positive.
	ErrInvalidMaxFileSize = errors.New("waldb: max file size must be positive")

	// ErrInvalidMaxBatchSize is returned when Options.MaxBatchSize is not positive.
	ErrInvalidMaxBatchSize = errors.New("waldb: max batch size must be positive")

	// ErrDiskFull is returned when the WAL directory has reached or
	// exceeded the configured maximum disk usage threshold.
	ErrDiskFull = errors.New("waldb: disk full")
)

// WALWriteError represents a compound failure during WAL write with recovery
// attempts. It wraps the original write error along with any errors from
// truncation and segment switch recovery steps.
type WALWriteError struct {
	// WriteErr is the original error from the WAL write attempt.
	WriteErr error
	// TruncateErr is the error from attempting to truncate the segment
	// back to the last known-good offset. Nil if truncation succeeded.
	TruncateErr error
	// SwitchErr is the error from attempting to switch to a new segment
	// after truncation failed. Nil if the switch succeeded.
	SwitchErr error
}

// Error implements the error interface, returning a human-readable description
// of the compound failure.
func (e *WALWriteError) Error() string {
	msg := "waldb: WAL write failed: " + e.WriteErr.Error()
	if e.TruncateErr != nil {
		msg += "; truncate failed: " + e.TruncateErr.Error()
	}
	if e.SwitchErr != nil {
		msg += "; segment switch failed: " + e.SwitchErr.Error()
	}
	return msg
}

// Unwrap returns the underlying write error for use with errors.Is/errors.As.
func (e *WALWriteError) Unwrap() error {
	return e.WriteErr
}

// WALEncryptError is returned when encrypting a WAL record fails.
type WALEncryptError struct {
	Err error
}

// Error implements the error interface.
func (e *WALEncryptError) Error() string {
	return "waldb: WAL record encryption failed: " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *WALEncryptError) Unwrap() error {
	return e.Err
}

// WALDecryptError is returned when decrypting a WAL record fails during
// recovery. This is a fatal error because silently skipping undecryptable
// records would cause undetected data loss.
type WALDecryptError struct {
	SegmentID uint64
	Err       error
}

// Error implements the error interface.
func (e *WALDecryptError) Error() string {
	return "waldb: WAL record decryption failed for segment " + itoa(int(e.SegmentID)) + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *WALDecryptError) Unwrap() error {
	return e.Err
}

// DirSyncError is returned when fsyncing a directory fails. This typically
// indicates a filesystem or I/O error that could result in metadata loss
// (e.g., new file directory entries not persisted after a crash).
type DirSyncError struct {
	Dir string
	Err error
}

// Error implements the error interface.
func (e *DirSyncError) Error() string {
	return "waldb: failed to sync directory " + e.Dir + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *DirSyncError) Unwrap() error {
	return e.Err
}

// SegmentDiscardError is returned when discarding unflushed block buffer
// data from a segment fails. This occurs during partial batch write
// rollback when a truncate or seek operation fails.
type SegmentDiscardError struct {
	SegmentID uint64
	Err       error
}

// Error implements the error interface.
func (e *SegmentDiscardError) Error() string {
	return "waldb: failed to discard pending data for segment " + itoa(int(e.SegmentID)) + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *SegmentDiscardError) Unwrap() error {
	return e.Err
}

// BatchWriteError is returned when a batch write fails and includes any
// errors from rolling back dirty shards. The WriteErr is the original
// failure; DiscardErrors lists per-shard rollback failures.
type BatchWriteError struct {
	WriteErr      error
	DiscardErrors []error
}

// Error implements the error interface.
func (e *BatchWriteError) Error() string {
	msg := "waldb: batch write failed: " + e.WriteErr.Error()
	for _, de := range e.DiscardErrors {
		msg += "; discard failed: " + de.Error()
	}
	return msg
}

// Unwrap returns the original write error.
func (e *BatchWriteError) Unwrap() error {
	return e.WriteErr
}

// DiskUsageError is returned when the platform disk usage query fails.
type DiskUsageError struct {
	Dir string
	Err error
}

// Error implements the error interface.
func (e *DiskUsageError) Error() string {
	return "waldb: failed to query disk usage for " + e.Dir + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *DiskUsageError) Unwrap() error {
	return e.Err
}

// WriteBootstrapBatchModeError is returned when writeBootstrap is called in
// batchMode. Bootstrap writes must use non-batch mode (with inline fsync) to
// ensure the in-memory update is safe. This error indicates a programming
// mistake in the call path.
type WriteBootstrapBatchModeError struct{}

// Error implements the error interface.
func (e *WriteBootstrapBatchModeError) Error() string {
	return "waldb: writeBootstrap called in batchMode; bootstrap writes require inline fsync"
}
