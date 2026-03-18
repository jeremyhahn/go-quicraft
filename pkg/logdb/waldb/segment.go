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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

// segment represents a single WAL segment file divided into fixed-size blocks.
// Records are written as chunks within blocks. When a record does not fit in
// the remaining block space, it is split across multiple blocks using the
// RecordFirst/RecordMiddle/RecordLast chunk types.
type segment struct {
	f           File
	id          uint64
	dir         string
	blockSize   int
	blockBuf    []byte // pre-allocated block buffer
	blockOffset int    // current write position within the current block
	fileOffset  int64  // current byte offset within the file
	maxFileSize int64
	noSync      bool
	recordSeq   atomic.Uint32
	closed      bool
}

// segmentFilename returns the canonical filename for a given segment ID.
// Format: %06d.log (e.g., "000001.log").
func segmentFilename(id uint64) string {
	return fmt.Sprintf("%06d.log", id)
}

// createSegment creates a new WAL segment file. The file is
// pre-allocated to maxFileSize using fallocate (Linux) or ftruncate (other
// platforms) to avoid filesystem metadata updates on each append.
func createSegment(fs FS, dir string, id uint64, blockSize int, maxFileSize int64) (*segment, error) {
	name := filepath.Join(dir, segmentFilename(id))
	f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	// Pre-allocate disk space. Errors are non-fatal; writes will fail with
	// a clear error if disk is truly full.
	if pa, ok := f.(FilePreAllocator); ok {
		if err := pa.PreAllocate(maxFileSize); err != nil {
			slog.Debug("waldb: fallocate failed during segment creation",
				"segment_id", id,
				"dir", dir,
				"max_file_size", maxFileSize,
				"error", err,
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
	}, nil
}

// Write writes a complete logical record to the segment. Large records are
// automatically split across blocks using First/Middle/Last chunk types.
// Returns the monotonically increasing record sequence number assigned.
func (s *segment) Write(data []byte) (uint32, error) {
	if s.closed {
		return 0, ErrClosed
	}

	// Calculate the total bytes this record will consume (headers + data).
	// Account for unflushed data in the block buffer (blockOffset).
	needed := s.bytesNeeded(data)
	logicalSize := s.fileOffset + int64(s.blockOffset) + int64(needed)
	if logicalSize > s.maxFileSize {
		return 0, ErrSegmentFull
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

		// Maximum payload that fits in the remaining block space.
		maxPayload := avail - recordHeaderSize
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

		n, err := encodeRecord(s.blockBuf[s.blockOffset:], chunkType, chunk)
		if err != nil {
			return 0, err
		}
		s.blockOffset += n
		isFirst = false
	}

	seq := s.recordSeq.Add(1)
	return seq, nil
}

// bytesNeeded calculates the total bytes a record will consume including all
// chunk headers and any block padding needed for block transitions.
func (s *segment) bytesNeeded(data []byte) int {
	total := 0
	remaining := len(data)
	blockOff := s.blockOffset

	for remaining > 0 {
		avail := s.blockSize - blockOff
		if avail < recordHeaderSize+1 {
			// Must pad and advance to next block.
			total += avail
			blockOff = 0
			avail = s.blockSize
		}

		maxPayload := avail - recordHeaderSize
		if maxPayload > remaining {
			maxPayload = remaining
		}

		total += recordHeaderSize + maxPayload
		remaining -= maxPayload
		blockOff += recordHeaderSize + maxPayload
	}
	return total
}

// padAndFlushBlock zero-fills the remainder of the current block and writes
// the entire block (data + padding) to disk, then resets the block offset.
func (s *segment) padAndFlushBlock() error {
	// Zero-fill remaining space in the current block.
	for i := s.blockOffset; i < s.blockSize; i++ {
		s.blockBuf[i] = 0
	}

	// Write the complete block including previously buffered data.
	_, err := s.f.Write(s.blockBuf[:s.blockSize])
	if err != nil {
		return err
	}
	s.fileOffset += int64(s.blockSize)
	s.blockOffset = 0
	return nil
}

// flushBlock writes any pending data in the block buffer to disk.
func (s *segment) flushBlock() error {
	if s.blockOffset == 0 {
		return nil
	}

	_, err := s.f.Write(s.blockBuf[:s.blockOffset])
	if err != nil {
		return err
	}
	s.fileOffset += int64(s.blockOffset)
	s.blockOffset = 0
	return nil
}

// Sync flushes any buffered data and forces it to durable storage.
// Uses fdatasync on Linux for better performance when only data (not metadata)
// has changed.
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
	return s.f.Sync()
}

// ReadAll reads all valid records from the segment sequentially. This is the
// primary recovery path. Records are returned in write order. Scanning stops
// at the first zero-filled or corrupted block (normal EOF for pre-allocated
// files).
func (s *segment) ReadAll() ([][]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}

	fi, err := s.f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()

	var records [][]byte
	var partial []byte // accumulates chunks for multi-chunk records
	offset := int64(0)
	blockBuf := make([]byte, s.blockSize)

	for offset < fileSize {
		readSize := int64(s.blockSize)
		if offset+readSize > fileSize {
			readSize = fileSize - offset
		}

		n, err := s.f.ReadAt(blockBuf[:readSize], offset)
		if err != nil && n == 0 {
			break
		}

		block := blockBuf[:n]
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
					break // End of records in this block.
				}
			}

			// Not enough room for a header means we are in zero padding.
			if len(block)-pos < recordHeaderSize {
				break
			}

			data, recType, consumed, err := decodeRecord(block[pos:])
			if err != nil {
				// Corrupted record. If we have an incomplete multi-chunk record
				// in progress, discard it. Stop scanning this block.
				partial = nil
				// Try to skip to next block boundary for recovery.
				goto nextBlock
			}

			switch recType {
			case RecordFull, RecordEpoch:
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
			}

			pos += consumed
		}

	nextBlock:
		offset += int64(s.blockSize)
	}

	return records, nil
}

// Close flushes any pending block data and closes the underlying file.
// After Close, all operations return ErrClosed.
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

	// Truncate the file to the actual written size to reclaim pre-allocated
	// space beyond what was written.
	if err := s.f.Truncate(s.fileOffset); err != nil {
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

// DiscardPending truncates the segment back to the given file offset and
// resets the block buffer. This discards any data written (but not fsynced)
// since the caller's saved checkpoint. It is used to roll back partial
// batch writes when a later write in the batch fails.
//
// The caller must provide the fileOffset and blockOffset captured before
// the batch began. The segment file is truncated to cleanFileOffset, the
// write position is seeked back, and the block buffer is zeroed.
func (s *segment) DiscardPending(cleanFileOffset int64, cleanBlockOffset int) error {
	if s.closed {
		return ErrClosed
	}

	// Truncate the file to discard any data written via padAndFlushBlock
	// or flushBlock since the clean checkpoint.
	if s.fileOffset > cleanFileOffset {
		if err := s.f.Truncate(cleanFileOffset); err != nil {
			return &SegmentDiscardError{
				SegmentID: s.id,
				Err:       err,
			}
		}
		// Seek the file write position back to match.
		if _, err := s.f.Seek(cleanFileOffset, 0); err != nil {
			return &SegmentDiscardError{
				SegmentID: s.id,
				Err:       err,
			}
		}
	}

	// Zero out the block buffer to prevent stale data from leaking into
	// future writes.
	for i := range s.blockBuf {
		s.blockBuf[i] = 0
	}

	s.fileOffset = cleanFileOffset
	s.blockOffset = cleanBlockOffset
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
