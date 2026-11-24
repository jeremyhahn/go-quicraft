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

	// ErrRecordTooLarge is returned when one record's framed size exceeds the segment size; the caller must split it.
	ErrRecordTooLarge = errors.New("waldb: record exceeds maximum segment size")

	// ErrClosed is returned when an operation is attempted on a closed segment.
	ErrClosed = errors.New("waldb: segment closed")

	// ErrBarrierContextUnsupported is returned by Open when the configured barrier cannot bind caller context into its AAD (walContextBarrier).
	ErrBarrierContextUnsupported = errors.New(
		"waldb: barrier does not support context-bound encryption")

	// ErrBarrierContextBindingUnreportable is returned by Open for a context-binding barrier without ContextBindingResolved. See docs/compatibility/architecture.md.
	ErrBarrierContextBindingUnreportable = errors.New(
		"waldb: barrier is missing ContextBindingResolved() bool, required since the " +
			"WAL must distinguish a barrier that cannot bind context from one that is " +
			"merely still sealed; add the method (return true whenever the barrier is " +
			"unsealed and its encryptor is installed)")

	// ErrBarrierContextUnresolved is returned by Open when the barrier is still sealed, so its context-binding capability is unknown; unseal it before opening the WAL.
	ErrBarrierContextUnresolved = errors.New(
		"waldb: barrier is not unsealed, so its context-binding capability cannot be resolved; unseal before opening the WAL")

	// ErrBarrierContextBindingChanged is returned when a barrier latched at open as unable to bind context later reports that it can; restart against the intended strategy.
	ErrBarrierContextBindingChanged = errors.New(
		"waldb: barrier context-binding capability changed after open; restart required")

	// ErrLandingSegmentOverflow is returned when a record pre-fitted to its segment overflows it: a broken segment.CanFit/segment.Write invariant.
	ErrLandingSegmentOverflow = errors.New(
		"waldb: record overflowed its pre-resolved landing segment")

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

	// ErrInvalidMaxFileSize is returned when Options.MaxFileSize is not
	// positive or exceeds the 4 GiB - 1 a chunk's generation-start offset can
	// name (see record.go).
	ErrInvalidMaxFileSize = errors.New("waldb: max file size must be positive and at most 4 GiB - 1")

	// ErrSegmentTooSmallForEntry is returned when a segment spans fewer than two blocks and can hold nothing.
	ErrSegmentTooSmallForEntry = errors.New(
		"waldb: max file size must span at least two blocks")

	// ErrDiskFull is returned when the WAL directory has reached or
	// exceeded the configured maximum disk usage threshold.
	ErrDiskFull = errors.New("waldb: disk full")

	// ErrCmdTooLarge is returned when an entry's Cmd exceeds maxCmdLen; fatal during replay (wrapped in RecoveryError).
	ErrCmdTooLarge = errors.New("waldb: entry Cmd exceeds maximum entry size")

	// ErrMembershipTooLarge is returned when a membership or bootstrap address map exceeds maxMembershipMapEntries, which replay would reject.
	ErrMembershipTooLarge = errors.New("waldb: membership exceeds maximum entry count")

	// ErrTruncatedEntryRecord is returned when replay finds an entry batch record ending mid-entry; recovery fails.
	ErrTruncatedEntryRecord = errors.New("waldb: entry batch record truncated mid-entry")

	// ErrTruncatedStateRecord is returned when replay finds a hard-state record of the wrong length; fatal, since skipping it could regress Term/Vote.
	ErrTruncatedStateRecord = errors.New("waldb: state record malformed")

	// ErrTruncatedSnapshotRecord is returned when replay cannot parse a snapshot record; fatal.
	ErrTruncatedSnapshotRecord = errors.New("waldb: snapshot record malformed")

	// ErrTruncatedBootstrapRecord is returned when WAL replay cannot parse a
	// bootstrap record. Fatal: the record already passed its chunk CRC, so a
	// parse failure is corruption or a format skew.
	ErrTruncatedBootstrapRecord = errors.New("waldb: bootstrap record malformed")

	// ErrTruncatedCompactionRecord is returned when replay finds a compaction marker of the wrong length; fatal.
	ErrTruncatedCompactionRecord = errors.New("waldb: compaction record malformed")

	// ErrUnknownPayloadType is returned when replay finds a payload type this package does not write, such as a keyed WAL opened without a barrier; fatal.
	ErrUnknownPayloadType = errors.New("waldb: unknown WAL record payload type")

	// ErrDBPoisoned is returned by every write after an fsync failure fail-stopped the database; only a restart and crash recovery are safe.
	ErrDBPoisoned = errors.New(
		"waldb: database poisoned after fsync failure; restart required for crash recovery")
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

// SegmentReadError is returned when reading a segment block fails during recovery; it carries the file offset and is wrapped in a RecoveryError.
type SegmentReadError struct {
	Offset int64
	Err    error
}

// Error implements the error interface.
func (e *SegmentReadError) Error() string {
	return "waldb: read failed at offset " + itoa(int(e.Offset)) + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *SegmentReadError) Unwrap() error {
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

// Unwrap returns the write error and every rollback failure, so ErrDBPoisoned is visible to errors.Is.
func (e *BatchWriteError) Unwrap() []error {
	errs := make([]error, 0, 1+len(e.DiscardErrors))
	errs = append(errs, e.WriteErr)
	errs = append(errs, e.DiscardErrors...)
	return errs
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

// WriteBootstrapBatchModeError is returned when writeBootstrap is called in batchMode, a programming error.
type WriteBootstrapBatchModeError struct{}

// Error implements the error interface.
func (e *WriteBootstrapBatchModeError) Error() string {
	return "waldb: writeBootstrap called in batchMode; bootstrap writes require inline fsync"
}

// IsCleanRefusal reports whether err from SaveState is a refusal caused by cause that left the log unchanged, so the updates can be retried; an error carrying a disk fault is never clean.
func IsCleanRefusal(err, cause error) bool {
	if !errors.Is(err, cause) || errors.Is(err, ErrDBPoisoned) {
		return false
	}
	var batchErr *BatchWriteError
	var walErr *WALWriteError
	return !errors.As(err, &batchErr) && !errors.As(err, &walErr)
}
