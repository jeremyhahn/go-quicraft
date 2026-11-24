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

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

// segment is a WAL segment file of fixed-size blocks; records split across blocks as First/Middle/Last chunks, and fileOffset always stays block-aligned.
type segment struct {
	f           File
	id          uint64
	dir         string
	blockSize   int
	blockBuf    []byte // pre-allocated block buffer
	blockOffset int    // current write position within the current block
	fileOffset  int64  // block-aligned start offset of the current block
	// flushed is the open block's flush watermark: a flush writes only blockBuf[flushed:blockOffset], so acknowledged bytes are written once. flushed <= blockOffset.
	flushed     int
	maxFileSize int64
	noSync      bool
	// recordSeq counts logical records in the segment and is rewound by DiscardPending; it is the record ordinal in the context-AAD.
	recordSeq atomic.Uint64
	// durabilitySeq is the number of completed Syncs, stamped into every chunk; DiscardPending never rewinds it. Guarded by the shard's mu.
	durabilitySeq uint32
	// unsynced reports a Write since the last completed Sync; without one, Sync opens no generation and appends no mark.
	unsynced bool
	closed   bool
	// stoppedAtCorruption is set when ReadAll halts at a corrupt or torn record; recovery then replays no higher segment.
	stoppedAtCorruption bool
	// ackedDataAfterCorruption is set when records above a corruption carry a lower generation than the segment's highest; recovery must not truncate (ErrMidSegmentCorruption).
	ackedDataAfterCorruption bool
	// sequenceRegression is set when a record above the damage has a generation below the clean prefix's; see ErrRecordSequenceRegression.
	sequenceRegression bool
	// genStart is the file offset where the current durability generation began; moved only by Sync, never rewound.
	genStart uint32
	// acknowledgedDataLost is set when survivors above a corruption carry a genStart above the corruption offset; recovery fails with ErrAcknowledgedDataLost.
	acknowledgedDataLost bool
}

// errSegmentCloseSync marks a Close whose closing fdatasync failed; rotateSegment poisons the DB on it.
var errSegmentCloseSync = errors.New("waldb: segment close fdatasync failed")

// segmentFilename returns the canonical filename for a given segment ID.
// Format: %06d.log (e.g., "000001.log").
func segmentFilename(id uint64) string {
	return fmt.Sprintf("%06d.log", id)
}

// createSegment creates a WAL segment pre-allocated to maxFileSize; preallocFailed reports a best-effort PreAllocate failure.
func createSegment(fs FS, dir string, id uint64, blockSize int, maxFileSize int64) (seg *segment, preallocFailed bool, err error) {
	name := filepath.Join(dir, segmentFilename(id))
	f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, false, err
	}

	// Pre-allocate disk space. Errors are non-fatal; writes will fail with
	// a clear error if disk is truly full.
	if pa, ok := f.(FilePreAllocator); ok {
		if perr := pa.PreAllocate(maxFileSize); perr != nil {
			preallocFailed = true
			slog.Warn("waldb: fallocate failed during segment creation; "+
				"ENOSPC pre-reservation defeated, disk may be near full",
				"segment_id", id,
				"dir", dir,
				"max_file_size", maxFileSize,
				"error", perr,
			)
		}
	}

	return &segment{
		f:           f,
		id:          id,
		dir:         dir,
		blockSize:   blockSize,
		blockBuf:    make([]byte, blockSize),
		blockOffset: 0,
		fileOffset:  0,
		maxFileSize: maxFileSize,
	}, preallocFailed, nil
}

// Write appends one logical record, split across blocks as needed, and returns its 1-based sequence number.
func (s *segment) Write(data []byte) (uint64, error) {
	if err := s.CanFit(len(data)); err != nil {
		return 0, err
	}

	remaining := data
	isFirst := true

	for len(remaining) > 0 {
		avail := s.blockSize - s.blockOffset

		// If only the header fits (or less), pad this block and move to the next.
		if avail < recordHeaderSize+1 {
			if err := s.padAndFlushBlock(); err != nil {
				return 0, err
			}
			avail = s.blockSize
		}

		// Cap a chunk at the space left and at maxRecordDataLen (2-byte length field); bytesNeeded applies the same cap.
		maxPayload := avail - recordHeaderSize
		if maxPayload > maxRecordDataLen {
			maxPayload = maxRecordDataLen
		}
		if maxPayload > len(remaining) {
			maxPayload = len(remaining)
		}

		chunk := remaining[:maxPayload]
		remaining = remaining[maxPayload:]

		var chunkType byte
		switch {
		case isFirst && len(remaining) == 0:
			chunkType = RecordFull
		case isFirst:
			chunkType = RecordFirst
		case len(remaining) == 0:
			chunkType = RecordLast
		default:
			chunkType = RecordMiddle
		}

		// Every chunk of a record carries the same generation; no Sync lands between them.
		n, err := encodeRecord(s.blockBuf[s.blockOffset:], chunkType, s.durabilitySeq, s.genStart, chunk)
		if err != nil {
			return 0, err
		}
		s.blockOffset += n
		isFirst = false
	}

	s.unsynced = true
	seq := s.recordSeq.Add(1)
	return seq, nil
}

// CanFit reports whether a record of dataLen bytes fits now: nil, ErrSegmentFull, ErrRecordTooLarge or ErrClosed. Write uses the same predicate.
func (s *segment) CanFit(dataLen int) error {
	if s.closed {
		return ErrClosed
	}
	// Refuse a segment whose durability sequence would wrap; ErrSegmentFull rotates.
	if s.durabilitySeq == maxDurabilitySeq {
		return ErrSegmentFull
	}
	// Calculate the total bytes this record will consume (headers + data).
	// Account for unflushed data in the block buffer (blockOffset).
	needed := s.bytesNeeded(dataLen)
	// A record too large for an empty segment, with its sync mark reserve, gets permanent ErrRecordTooLarge; one that fits a fresh segment gets ErrSegmentFull.
	if int64(s.bytesNeededFrom(0, dataLen))+syncMarkReserve > s.maxFileSize {
		return ErrRecordTooLarge
	}
	if s.fileOffset+int64(s.blockOffset)+int64(needed)+syncMarkReserve > s.maxFileSize {
		return ErrSegmentFull
	}
	return nil
}

// RecordCount returns the number of logical records in the segment, the next record's ordinal.
func (s *segment) RecordCount() uint64 {
	return s.recordSeq.Load()
}

// bytesNeeded calculates the total bytes a record of dataLen bytes will consume
// including all chunk headers and any block padding needed for block
// transitions.
func (s *segment) bytesNeeded(dataLen int) int {
	return s.bytesNeededFrom(s.blockOffset, dataLen)
}

// bytesNeededFrom is bytesNeeded for a record that starts at block offset
// blockOff rather than at the segment's current one. From offset 0 it is the
// framed size the record takes in an empty segment.
func (s *segment) bytesNeededFrom(blockOff, dataLen int) int {
	total := 0
	remaining := dataLen

	for remaining > 0 {
		avail := s.blockSize - blockOff
		if avail < recordHeaderSize+1 {
			// Must pad and advance to next block.
			total += avail
			blockOff = 0
			avail = s.blockSize
		}

		maxPayload := avail - recordHeaderSize
		if maxPayload > maxRecordDataLen {
			maxPayload = maxRecordDataLen
		}
		if maxPayload > remaining {
			maxPayload = remaining
		}

		total += recordHeaderSize + maxPayload
		remaining -= maxPayload
		blockOff += recordHeaderSize + maxPayload
	}
	return total
}

// padAndFlushBlock zero-fills the block, writes its unflushed bytes in one call and advances fileOffset by one block; the only path that does.
func (s *segment) padAndFlushBlock() error {
	// Zero-fill remaining space in the current block.
	for i := s.blockOffset; i < s.blockSize; i++ {
		s.blockBuf[i] = 0
	}

	// Write the unflushed tail at its exact file position. WriteAt does not
	// move the file's implicit position, so this never disturbs the grid.
	if _, err := s.f.WriteAt(s.blockBuf[s.flushed:s.blockSize], s.fileOffset+int64(s.flushed)); err != nil {
		return err
	}
	s.fileOffset += int64(s.blockSize)
	s.blockOffset = 0
	s.flushed = 0
	return nil
}

// flushBlock writes the open block's unflushed bytes at their file position without advancing fileOffset.
func (s *segment) flushBlock() error {
	if s.blockOffset == s.flushed {
		return nil
	}

	if _, err := s.f.WriteAt(s.blockBuf[s.flushed:s.blockOffset], s.fileOffset+int64(s.flushed)); err != nil {
		return err
	}
	s.flushed = s.blockOffset
	return nil
}

// Sync flushes and fdatasyncs the segment, then closes a durability generation if anything was written; under noSync nothing is durable.
func (s *segment) Sync() error {
	if s.closed {
		return ErrClosed
	}
	if err := s.flushBlock(); err != nil {
		return err
	}
	if s.noSync {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	if !s.unsynced {
		return nil
	}
	s.unsynced = false
	s.durabilitySeq++
	// Everything below this offset is now acknowledged; the next generation
	// begins here. flushBlock above made blockOffset the flushed watermark, and
	// CanFit keeps fileOffset+blockOffset within maxSegmentFileSize.
	s.genStart = uint32(s.fileOffset + int64(s.blockOffset))
	return s.writeSyncMark()
}

// writeSyncMark appends the RecordSyncMark chunk that opens the generation a completed Sync began; the mark itself is not synced.
func (s *segment) writeSyncMark() error {
	if s.blockSize-s.blockOffset < recordHeaderSize {
		if err := s.padAndFlushBlock(); err != nil {
			return err
		}
	}
	n, err := encodeRecord(s.blockBuf[s.blockOffset:], RecordSyncMark, s.durabilitySeq, s.genStart, nil)
	if err != nil {
		return err
	}
	s.blockOffset += n
	return s.flushBlock()
}

// ReadAll returns the segment's valid records in order and the clean-prefix end; the durability generations above any damage decide truncate or fail closed.
//
//nolint:gocyclo // WAL recovery walks blocks, chunk types and padding while distinguishing a legal torn tail from corruption; that distinction is the whole function, and splitting it separates the hole detection from the stride arithmetic it depends on
func (s *segment) ReadAll() ([][]byte, int64, error) {
	if s.closed {
		return nil, 0, ErrClosed
	}

	fi, err := s.f.Stat()
	if err != nil {
		return nil, 0, err
	}
	fileSize := fi.Size()

	var records [][]byte
	var partial []byte // accumulates chunks for multi-chunk records
	offset := int64(0)
	blockBuf := make([]byte, s.blockSize)

	// holeOffset is the first zero region legal only at the tail; data after it makes it a hole, and generations decide whether to truncate there.
	holeOffset := int64(-1)

	// corruptOffset is where the clean prefix ends; the scan continues to gather generations.
	corruptOffset := int64(-1)

	// prefixMaxSeq is the highest durability generation seen in the clean
	// prefix, and ev accumulates the generations found above the damage. The
	// two together decide truncate-versus-fail-closed; see gatherEvidence.
	var prefixMaxSeq uint32
	var ev durabilityEvidence

	for offset < fileSize {
		readSize := int64(s.blockSize)
		if offset+readSize > fileSize {
			readSize = fileSize - offset
		}

		n, err := s.f.ReadAt(blockBuf[:readSize], offset)
		if err != nil {
			// A real I/O error, even on a short read, is not end-of-data.
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return records, offset, &RecoveryError{
					SegmentID: s.id,
					Err:       &SegmentReadError{Offset: offset, Err: err},
				}
			}
			// A clean EOF (or short read at the tail of a pre-allocated file)
			// is the normal way to end the scan. With n > 0 the bytes that did
			// come back are the file's genuine tail and are parsed below.
			if n == 0 {
				break
			}
		}

		block := blockBuf[:n]

		// Post-corruption sweep: gather generations only, ending early once they settle the verdict.
		if corruptOffset >= 0 {
			if s.gatherEvidence(block, offset, 0, prefixMaxSeq, &ev, len(records)) {
				return records, corruptOffset, nil
			}
			offset += int64(s.blockSize)
			continue
		}

		pos := 0

		for pos < len(block) {
			// Check if remaining bytes in block are all zeros (normal EOF padding).
			if block[pos] == 0 {
				allZero := true
				for j := pos; j < len(block); j++ {
					if block[j] != 0 {
						allZero = false
						break
					}
				}
				if allZero {
					// A wholly-zero block or over-long zero tail is legal only at the end; data after it makes it a hole.
					if pos == 0 || len(block)-pos > recordHeaderSize {
						if holeOffset < 0 {
							holeOffset = offset + int64(pos)
						}
					}
					break // End of records in this block.
				}
			}

			// A non-zero sub-header remainder cannot come from the writer, so it falls through to decodeRecord as corruption.

			// Data after a zero hole: the clean prefix ends at the hole, and the generations above decide whether to truncate.
			if holeOffset >= 0 {
				s.stoppedAtCorruption = true
				corruptOffset = holeOffset
				slog.Warn("waldb: stopping WAL recovery at zero-filled hole",
					"segment_id", s.id,
					"dir", s.dir,
					"hole_offset", holeOffset,
					"data_offset", offset+int64(pos),
					"prefix_generation", prefixMaxSeq,
					"recovered_records", len(records),
				)

				// Sweep the rest of this block after the hole before striding on.
				if s.gatherEvidence(block, offset, pos, prefixMaxSeq, &ev, len(records)) {
					return records, corruptOffset, nil
				}
				break
			}

			data, recType, durSeq, _, consumed, err := decodeRecord(block[pos:])
			if err != nil {
				s.stoppedAtCorruption = true
				corruptOffset = offset + int64(pos)
				// Corrupt or torn record: the clean prefix is final; the sweep continues to decide truncate or fail-stop.
				slog.Warn("waldb: stopping WAL recovery at first corrupt record",
					"segment_id", s.id,
					"dir", s.dir,
					"offset", corruptOffset,
					"prefix_generation", prefixMaxSeq,
					"recovered_records", len(records),
					"error", err,
				)

				// Sweep the rest of this block before striding on.
				if s.gatherEvidence(block, offset, pos+1, prefixMaxSeq, &ev, len(records)) {
					return records, corruptOffset, nil
				}
				break
			}

			// Track the clean prefix's generation. It only ever rises, but
			// taking the max rather than the last value keeps the invariant
			// local instead of assuming it.
			if durSeq > prefixMaxSeq {
				prefixMaxSeq = durSeq
			}

			switch recType {
			case RecordFull:
				// Complete record in a single chunk.
				rec := make([]byte, len(data))
				copy(rec, data)
				records = append(records, rec)
				partial = nil

			case RecordFirst:
				// Start of a multi-chunk record.
				partial = make([]byte, 0, len(data)*4)
				partial = append(partial, data...)

			case RecordMiddle:
				if partial != nil {
					partial = append(partial, data...)
				}
				// If partial is nil, we lost the First chunk; discard.

			case RecordLast:
				if partial != nil {
					partial = append(partial, data...)
					rec := make([]byte, len(partial))
					copy(rec, partial)
					records = append(records, rec)
					partial = nil
				}

			case RecordSyncMark:
				// A sync mark is evidence only; partial is left alone.
			}

			pos += consumed
		}

		offset += int64(s.blockSize)
	}

	// No evidence against truncation: report the clean-prefix end, or fail closed when acknowledged bytes lie in the damage.
	if corruptOffset >= 0 {
		if ev.found && int64(ev.genStart) > corruptOffset {
			// The survivors' generation began above the corruption, so acknowledged bytes were destroyed: fail with acknowledgedDataLost.
			s.acknowledgedDataLost = true
			slog.Error("waldb: corruption begins BELOW the acknowledged end that the "+
				"records surviving above it name; the fdatasync that opened their "+
				"generation had acknowledged bytes the damage destroyed — refusing "+
				"to open rather than come up shorter than the log this node promised",
				"segment_id", s.id,
				"dir", s.dir,
				"corrupt_offset", corruptOffset,
				"acknowledged_end", ev.genStart,
				"prefix_generation", prefixMaxSeq,
				"surviving_generation", ev.minSeq,
				"clean_prefix_records", len(records),
			)
			return records, corruptOffset, nil
		}
		if ev.found {
			slog.Warn("waldb: corruption is followed only by records from one unsynced "+
				"generation whose start lies at or below the damage; nothing above the "+
				"damage was ever acknowledged — treating it as an interrupted write and "+
				"truncating to the clean prefix",
				"segment_id", s.id,
				"dir", s.dir,
				"corrupt_offset", corruptOffset,
				"generation", ev.minSeq,
				"generation_start", ev.genStart,
				"prefix_generation", prefixMaxSeq,
				"clean_prefix_records", len(records),
			)
		}
		return records, corruptOffset, nil
	}

	return records, fileSize, nil
}

// durabilityEvidence accumulates the min and max generations of chunks decoding above a corruption.
type durabilityEvidence struct {
	found  bool
	minSeq uint32
	maxSeq uint32
	// genStart is the highest generation start the survivors carry.
	genStart uint32
}

// observe folds one surviving chunk's generation and generation start into
// the evidence.
func (e *durabilityEvidence) observe(seq, genStart uint32) {
	if !e.found {
		e.found, e.minSeq, e.maxSeq, e.genStart = true, seq, seq, genStart
		return
	}
	if seq < e.minSeq {
		e.minSeq = seq
	}
	if seq > e.maxSeq {
		e.maxSeq = seq
	}
	if genStart > e.genStart {
		e.genStart = genStart
	}
}

// gatherEvidence folds the generation of every chunk decoding in one block at or above a corruption into ev, probing every offset, and reports whether truncation is ruled out.
func (s *segment) gatherEvidence(block []byte, blockStart int64, from int, prefixMaxSeq uint32, ev *durabilityEvidence, cleanRecords int) bool {
	if from < 0 {
		from = 0
	}
	for at := from; at+recordHeaderSize <= len(block); {
		_, _, durSeq, genStart, consumed, err := decodeRecord(block[at:])
		if err != nil {
			at++
			continue
		}
		ev.observe(durSeq, genStart)

		// A generation below the clean prefix's cannot come from this writer.
		if ev.minSeq < prefixMaxSeq {
			s.sequenceRegression = true
			slog.Error("waldb: WAL record above a corruption carries a durability "+
				"generation LOWER than the clean prefix; no writer of this segment "+
				"could have produced it",
				"segment_id", s.id,
				"dir", s.dir,
				"offset", blockStart+int64(at),
				"generation", durSeq,
				"prefix_generation", prefixMaxSeq,
				"clean_prefix_records", cleanRecords,
			)
			return true
		}

		// Two different generations above the damage mean an fdatasync
		// completed between them, which acknowledged everything written before
		// it — including records the truncation would destroy.
		if ev.maxSeq > ev.minSeq {
			s.ackedDataAfterCorruption = true
			slog.Warn("waldb: WAL records above a corruption span a completed "+
				"fdatasync, so they were acknowledged; this is not an interrupted "+
				"write and must not be truncated",
				"segment_id", s.id,
				"dir", s.dir,
				"offset", blockStart+int64(at),
				"generation_low", ev.minSeq,
				"generation_high", ev.maxSeq,
				"clean_prefix_records", cleanRecords,
			)
			return true
		}
		at += consumed
	}
	return false
}

// Close flushes, fdatasyncs, truncates and closes the segment; a sync failure returns errSegmentCloseSync. Later calls return ErrClosed.
func (s *segment) Close() error {
	if s.closed {
		return ErrClosed
	}
	s.closed = true

	// Flush any remaining data in the block buffer.
	if err := s.flushBlock(); err != nil {
		if closeErr := s.f.Close(); closeErr != nil {
			slog.Debug("waldb: file close failed after flush error",
				"segment_id", s.id,
				"dir", s.dir,
				"error", closeErr,
			)
		}
		return err
	}
	if !s.noSync {
		if err := s.f.Sync(); err != nil {
			if closeErr := s.f.Close(); closeErr != nil {
				slog.Debug("waldb: file close failed after sync error",
					"segment_id", s.id,
					"dir", s.dir,
					"error", closeErr,
				)
			}
			return fmt.Errorf("%w: %w", errSegmentCloseSync, err)
		}
	}

	// Truncate to the written size, including the open block.
	if err := s.f.Truncate(s.Size()); err != nil {
		if closeErr := s.f.Close(); closeErr != nil {
			slog.Debug("waldb: file close failed after truncate error",
				"segment_id", s.id,
				"dir", s.dir,
				"error", closeErr,
			)
		}
		return err
	}

	return s.f.Close()
}

// DiscardPending rolls the segment back to the checkpoint fileOffset, blockOffset and record count captured before the batch.
func (s *segment) DiscardPending(cleanFileOffset int64, cleanBlockOffset int, cleanRecordCount uint64) error {
	if s.closed {
		return ErrClosed
	}

	// Truncate at the batch-start durable boundary.
	cleanEnd := cleanFileOffset + int64(cleanBlockOffset)
	if s.Size() > cleanEnd {
		if err := s.f.Truncate(cleanEnd); err != nil {
			return &SegmentDiscardError{
				SegmentID: s.id,
				Err:       err,
			}
		}
		// fsync the truncation so a crash right after rollback cannot resurrect
		// the discarded batch bytes (which would replay "rolled back" records).
		if !s.noSync {
			if err := s.f.Sync(); err != nil {
				return &SegmentDiscardError{SegmentID: s.id, Err: err}
			}
		}
		// Re-take the fallocate reservation the truncate dropped; best-effort.
		if err := s.PreAllocate(); err != nil {
			slog.Debug("waldb: re-preallocate after discard failed",
				"segment_id", s.id, "dir", s.dir, "error", err)
		}
	}

	// Reload the open block's pre-batch content from disk.
	if cleanBlockOffset > 0 {
		if _, err := s.f.ReadAt(s.blockBuf[:cleanBlockOffset], cleanFileOffset); err != nil {
			return &SegmentDiscardError{SegmentID: s.id, Err: err}
		}
	}
	// Zero the rest of the block so stale bytes never leak into a future flush.
	for i := cleanBlockOffset; i < len(s.blockBuf); i++ {
		s.blockBuf[i] = 0
	}

	s.fileOffset = cleanFileOffset
	s.blockOffset = cleanBlockOffset
	// The restored prefix is on disk, so the flush watermark rewinds to cleanBlockOffset.
	s.flushed = cleanBlockOffset
	s.recordSeq.Store(cleanRecordCount)
	// Nothing below the checkpoint awaits a Sync, so clear unsynced.
	s.unsynced = false
	return nil
}

// PreAllocate re-reserves the segment's disk space up to maxFileSize, best-effort.
func (s *segment) PreAllocate() error {
	if s.closed {
		return ErrClosed
	}
	if pa, ok := s.f.(FilePreAllocator); ok {
		return pa.PreAllocate(s.maxFileSize)
	}
	return nil
}

// Size returns the logical size of the segment, including data buffered
// but not yet flushed to disk.
func (s *segment) Size() int64 {
	return s.fileOffset + int64(s.blockOffset)
}

// ID returns the segment's monotonically increasing identifier.
func (s *segment) ID() uint64 {
	return s.id
}
