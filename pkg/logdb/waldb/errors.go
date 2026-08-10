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

	// ErrRecordTooLarge is returned when a single record's framed size exceeds
	// the segment size limit, so it can never fit in any segment. Unlike
	// ErrSegmentFull this is permanent: rotating to a fresh segment will not
	// help, and the caller must split the record into smaller writes.
	ErrRecordTooLarge = errors.New("waldb: record exceeds maximum segment size")

	// ErrClosed is returned when an operation is attempted on a closed segment.
	ErrClosed = errors.New("waldb: segment closed")

	// ErrBarrierContextUnsupported is returned by Open when a barrier is
	// configured but cannot bind caller context into its AEAD AAD (see
	// walContextBarrier). Every encrypted WAL record is positionally bound, so
	// there is nothing to fall back to: accepting such a barrier would write
	// records an at-rest attacker could reorder or splice undetected.
	ErrBarrierContextUnsupported = errors.New(
		"waldb: barrier does not support context-bound encryption")

	// ErrLandingSegmentOverflow is returned when a record that was pre-fitted
	// to its landing segment nevertheless overflows it. It signals a broken
	// invariant between segment.CanFit and segment.Write, never a condition a
	// caller can provoke: the alternative — moving a positionally sealed record
	// to a different segment — would make it permanently undecryptable.
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

	// ErrInvalidMaxFileSize is returned when Options.MaxFileSize is not positive.
	ErrInvalidMaxFileSize = errors.New("waldb: max file size must be positive")

	// ErrSegmentTooSmallForEntry is returned when a segment spans fewer than
	// two blocks, leaving no usable record payload at all. A large BATCH is
	// split across records and a lone oversized entry fails at the write with
	// ErrRecordTooLarge, so this rejects only the geometry where nothing can
	// ever be written.
	ErrSegmentTooSmallForEntry = errors.New(
		"waldb: max file size must span at least two blocks")

	// ErrInvalidMaxBatchSize is returned when Options.MaxBatchSize is not positive.
	ErrInvalidMaxBatchSize = errors.New("waldb: max batch size must be positive")

	// ErrDiskFull is returned when the WAL directory has reached or
	// exceeded the configured maximum disk usage threshold.
	ErrDiskFull = errors.New("waldb: disk full")

	// ErrCmdTooLarge is returned when a single entry's Cmd exceeds maxCmdLen.
	// The write path enforces it so the WAL never contains an entry that
	// recovery would reject, keeping the two sides of the format symmetric;
	// during replay it is fatal (wrapped in RecoveryError) because a stored
	// oversized length can only be corruption.
	ErrCmdTooLarge = errors.New("waldb: entry Cmd exceeds maximum entry size")

	// ErrMembershipTooLarge is returned when a snapshot's membership maps or a
	// bootstrap's address map declare more than maxMembershipMapEntries
	// entries. Replay rejects such a record as corruption and fails the whole
	// recovery, so the write path must refuse it: a record that is durable but
	// unreadable makes the node permanently unopenable. Same symmetry rule as
	// ErrCmdTooLarge.
	ErrMembershipTooLarge = errors.New("waldb: membership exceeds maximum entry count")

	// ErrBootstrapDataDirProvenance is returned by the migration copy path when
	// a bootstrap record carries a DataDir while claiming the field was never
	// recorded. No writer or replay can produce that pair — replay fills DataDir
	// only when the record carried the field — so it is a caller fabricating
	// provenance. The copy fails closed rather than choose between dropping the
	// DataDir (whose loss lets the next PurgeEpochsBefore destroy the DEK a
	// snapshot written under it needs) and stamping the field as fact.
	ErrBootstrapDataDirProvenance = errors.New(
		"waldb: bootstrap record declares a DataDir but reports it as never recorded")

	// ErrTruncatedEntryRecord is returned when WAL replay finds an entry
	// batch record that ends mid-entry. The batch was written as a single
	// CRC-protected record, so a mid-record decode failure is corruption
	// that CRC did not explain; skipping the remainder would leave a hole
	// in the replayed log that reads would serve as a silently shortened
	// range (Log Matching violation), so recovery fails instead.
	ErrTruncatedEntryRecord = errors.New("waldb: entry batch record truncated mid-entry")

	// ErrTruncatedStateRecord is returned when WAL replay finds a hard-state
	// record whose length does not match the fixed on-disk layout. It is
	// fatal: skipping the record lets an OLDER state record win
	// last-writer-wins replay, regressing Term/Vote so the node can vote
	// twice in one term (Election Safety violation).
	ErrTruncatedStateRecord = errors.New("waldb: state record malformed")

	// ErrTruncatedSnapshotRecord is returned when WAL replay cannot parse a
	// snapshot record: a short header, a filepath length past the record end,
	// a missing or malformed membership section, or trailing bytes. It is
	// fatal because a skipped snapshot record boots the node with empty
	// membership and no snapshot metadata.
	ErrTruncatedSnapshotRecord = errors.New("waldb: snapshot record malformed")

	// ErrTruncatedBootstrapRecord is returned when WAL replay cannot parse a
	// bootstrap record. Fatal: the record already passed its chunk CRC, so a
	// parse failure is corruption or a format skew.
	ErrTruncatedBootstrapRecord = errors.New("waldb: bootstrap record malformed")

	// ErrTruncatedCompactionRecord is returned when WAL replay finds a
	// compaction marker whose length does not match the fixed on-disk layout.
	// Fatal: a lost marker resets compactedTo to 0, so reads below the true
	// floor return empty instead of ErrCompacted.
	ErrTruncatedCompactionRecord = errors.New("waldb: compaction record malformed")

	// ErrTruncatedRemoveNodeRecord is returned when WAL replay finds a
	// remove-node tombstone whose length does not match the fixed on-disk
	// layout. Fatal: a skipped tombstone resurrects a removed replica.
	ErrTruncatedRemoveNodeRecord = errors.New("waldb: remove-node record malformed")

	// ErrUnknownPayloadType is returned when WAL replay finds a record whose
	// payload type byte is not written by this package. Payload types are
	// CRC-protected, so an unknown type is corruption or a configuration
	// skew — most importantly a KEYED WAL opened with barrier==nil, where
	// every ciphertext record's leading epoch byte decodes as type 0x00.
	// Skipping such records booted the node as an empty replica with no
	// memory of prior votes, so replay fails-stop instead.
	ErrUnknownPayloadType = errors.New("waldb: unknown WAL record payload type")

	// ErrDBPoisoned is returned by every write operation after an fsync
	// failure has fail-stopped the database. Once fdatasync fails, the
	// kernel may mark dirty pages clean without persisting them
	// (fsyncgate), so retrying writes has undefined durability semantics
	// and ghost records could surface at recovery. The only safe path is
	// a process restart followed by WAL crash recovery.
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

// Unwrap returns the original write error followed by every rollback
// failure, so errors.Is/errors.As traverse the full chain — in particular,
// a rollback that fail-stopped the DB surfaces ErrDBPoisoned to callers of
// SaveState via one of the DiscardErrors.
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

// WriteBootstrapBatchModeError is returned when writeBootstrap is called in
// batchMode. Bootstrap writes must use non-batch mode (with inline fsync) to
// ensure the in-memory update is safe. This error indicates a programming
// mistake in the call path.
type WriteBootstrapBatchModeError struct{}

// Error implements the error interface.
func (e *WriteBootstrapBatchModeError) Error() string {
	return "waldb: writeBootstrap called in batchMode; bootstrap writes require inline fsync"
}
