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

// segment represents a single WAL segment file divided into fixed-size blocks.
// Records are written as chunks within blocks. When a record does not fit in
// the remaining block space, it is split across multiple blocks using the
// RecordFirst/RecordMiddle/RecordLast chunk types.
// Block-alignment invariant: fileOffset is ALWAYS a multiple of blockSize and
// marks the start of the current (in-progress) block. blockBuf[0:blockOffset]
// holds that block's live content. A partial (mid-block) Sync writes the open
// block's NEW bytes at fileOffset+flushed via WriteAt WITHOUT advancing
// fileOffset, so the block stays open and the next write continues filling it;
// only a full block (padAndFlushBlock) advances fileOffset by blockSize.
// ReadAll relies on every block beginning at a multiple of blockSize, so
// flushing must never move fileOffset off the grid.
type segment struct {
	f           File
	id          uint64
	dir         string
	blockSize   int
	blockBuf    []byte // pre-allocated block buffer
	blockOffset int    // current write position within the current block
	fileOffset  int64  // block-aligned start offset of the current block
	// flushed is the number of leading bytes of the OPEN block that have
	// already been handed to the file (blockBuf[:flushed] is on disk at
	// fileOffset). It is the append watermark inside the block: a flush writes
	// only blockBuf[flushed:blockOffset], never the bytes below it.
	//
	// This is a durability requirement, not an optimization. Re-writing the
	// whole open block on every Sync puts already-fsynced bytes — a HardState
	// carrying a vote grant, say — back in flight inside the NEXT batch's
	// write, so a power loss during that rewrite can destroy a record the
	// caller was already told was durable. Writing only the new suffix means an
	// acknowledged byte is written exactly once and is never at risk again.
	//
	// It also avoids a quadratic write amplification: rewriting the whole block
	// would make filling one block over n syncs write ~n^2*recSize/2 bytes
	// (~11 MiB to persist a 32 KiB block of 48-byte records) on the WAL hot
	// path, per shard.
	//
	// flushed <= blockOffset always: Write only grows blockOffset,
	// padAndFlushBlock resets both to 0 in lockstep with advancing fileOffset,
	// and DiscardPending restores both to the checkpoint's block offset.
	flushed     int
	maxFileSize int64
	noSync      bool
	// recordSeq counts the LOGICAL records (one per Write call, regardless of
	// how many chunks the record is split into) committed to this segment. It
	// is ROLLBACK-AWARE: DiscardPending rewinds it exactly as it rewinds
	// fileOffset/blockOffset, so it always reflects records actually present in
	// the file. That is what makes recordSeq usable as the record ordinal in
	// the positional context-AAD (see walRecordContext): the write side's
	// pre-write value equals the index segment.ReadAll returns the record at
	// during recovery. Without the rewind, one rolled-back batch would
	// desynchronize the two sides permanently and make the whole segment tail
	// undecryptable at startup.
	recordSeq atomic.Uint64
	// durabilitySeq is the number of Syncs this segment has COMPLETED. It is
	// stamped into every chunk written (see encodeRecord) and is the evidence
	// recovery uses to tell an interrupted write from damage to acknowledged
	// data. It is guarded by the owning shard's mu, exactly like blockOffset and
	// fileOffset, and is deliberately NOT rewound by DiscardPending: a Sync that
	// completed is a fact about the medium, and rolling the counter back would
	// let a later batch reuse a generation whose records were already durable —
	// which is precisely the proof the counter exists to carry.
	durabilitySeq uint32
	// unsynced reports that a Write has landed since the last completed
	// Sync. A Sync with nothing new to acknowledge still issues its fdatasync
	// but neither opens a generation nor appends a sync mark: there is
	// nothing for the mark to witness, CanFit reserved room for exactly one
	// mark per acknowledged write (syncMarkReserve), and rotateSegment's
	// closing Sync ordinarily follows the batch's own Sync directly — an
	// unconditional mark there would land past the reserve and, at
	// maxDurabilitySeq, wrap the counter CanFit guards.
	unsynced bool
	closed   bool
	// stoppedAtCorruption is set by ReadAll when it halts at a corrupt/torn
	// record (as opposed to a clean zero-filled EOF). Recovery uses it to stop
	// replaying any HIGHER-numbered segment: a WAL is valid only as a prefix,
	// so nothing after the first corruption may be resurrected.
	stoppedAtCorruption bool
	// ackedDataAfterCorruption is set by ReadAll when the records it finds
	// ABOVE a corruption prove, by their durability sequences, that they were
	// already acknowledged: at least one of them carries a generation LOWER
	// than the segment's highest, which can only happen if a Sync completed
	// after it was written. Recovery must therefore NOT truncate here — see
	// ErrMidSegmentCorruption — because truncating to the clean prefix would
	// silently destroy committed entries and roll term/vote backwards.
	//
	// Decodable records above the damage are NOT sufficient on their own.
	// They are exactly what an ordinary torn multi-block write leaves behind,
	// and treating them as proof would make a routine power loss unbootable. What
	// makes them proof is their GENERATION; see the durability-sequence
	// commentary in record.go.
	ackedDataAfterCorruption bool
	// sequenceRegression is set by ReadAll when a record above the damage
	// carries a durability sequence BELOW one already seen in the clean prefix.
	// The writer's counter never decreases, so no segment it produced can look
	// like this: these bytes are neither this segment's torn tail nor its
	// acknowledged data, but something a WAL writer did not write — a
	// misdirected write from elsewhere on the device, a partially restored
	// file, or tampering. Recovery fails closed on its own sentinel rather than
	// guessing which. See ErrRecordSequenceRegression.
	sequenceRegression bool
	// genStart is the file offset at which the CURRENT durability generation
	// began: the acknowledged end when the fdatasync that closed the previous
	// generation returned (fileOffset+blockOffset at that moment), 0 for a
	// fresh segment. It is stamped into every chunk alongside durabilitySeq
	// and moves only in Sync, for the same reason durabilitySeq does — it
	// records a fact about the medium, so DiscardPending must not rewind it.
	// See record.go for what recovery does with it.
	genStart uint32
	// acknowledgedDataLost is set by ReadAll when the chunks surviving above
	// a corruption carry a generation start ABOVE the corruption offset: the
	// generation they belong to began past the damage, so the fdatasync that
	// closed the previous generation acknowledged bytes the damage destroyed.
	// This is the exact form of the verdict the sequence alone could only
	// approximate (it could say a completed fdatasync lay inside the damage,
	// not whether any acknowledged byte did). Recovery fails closed on it —
	// see ErrAcknowledgedDataLost — because the node would otherwise come up
	// holding a strictly shorter log than the one it promised its peers.
	//
	// The ordinary interrupted write that loses the in-flight batch's leading
	// page is NOT this: the page reverts to its durable content, so the hole
	// begins at or above the acknowledged end and the survivors' genStart is
	// at or below the corruption offset. That case truncates, as it always
	// did, and now provably rather than on the argument that a crash cannot
	// destroy fsynced pages.
	acknowledgedDataLost bool
}

// errSegmentCloseSync marks a Close whose closing fdatasync failed. The
// records were acknowledged by earlier Syncs; what may not have reached the
// medium is the final sync mark, and after a failed fdatasync nothing about
// the file can be established (fsyncgate), so rotateSegment poisons the DB
// on it exactly as it does for a failed Sync.
var errSegmentCloseSync = errors.New("waldb: segment close fdatasync failed")

// segmentFilename returns the canonical filename for a given segment ID.
// Format: %06d.log (e.g., "000001.log").
func segmentFilename(id uint64) string {
	return fmt.Sprintf("%06d.log", id)
}

// createSegment creates a new WAL segment file. The file is
// pre-allocated to maxFileSize using fallocate (Linux) or ftruncate (other
// platforms) to avoid filesystem metadata updates on each append.
//
// preallocFailed reports whether the best-effort PreAllocate failed. A
// fallocate failure is non-fatal (writes proceed and fail later if the
// disk is truly full), but it defeats the ENOSPC-avoidance design, so the
// caller surfaces it at Warn level and forces a disk-usage check.
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

// Write writes a complete logical record to the segment. Large records are
// automatically split across blocks using First/Middle/Last chunk types.
// Returns the monotonically increasing record sequence number assigned, which
// is 1-based (the record's 0-based ordinal is RecordCount() sampled BEFORE the
// call; that ordinal, not this sequence number, is what the context-AAD binds).
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

		// Maximum payload that fits in the remaining block space, capped at
		// maxRecordDataLen — the record's length field is 2 bytes, so a single
		// chunk can never carry more than that regardless of block size. Without
		// this cap, a block size >= 64KiB+header would size a chunk past the
		// length-field limit and encodeRecord would fail. bytesNeeded applies
		// the identical cap so the two stay in sync.
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

		// Every chunk of a record carries the SAME generation. The record is
		// one logical write and no Sync can land between its chunks — Write
		// runs under the shard lock and never syncs — so a multi-block record
		// is either wholly inside the in-flight generation or wholly below it.
		// That is what makes a torn multi-block write a clean suffix in
		// generation terms rather than a straddle ReadAll would have to guess at.
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

// CanFit reports whether a logical record of dataLen bytes can still be
// appended to this segment at its current position, WITHOUT writing anything.
// It returns nil when the record fits, ErrSegmentFull when it does not (rotate
// and retry), ErrRecordTooLarge when its framed size could not fit in any
// segment (permanent; the caller must split the record) and ErrClosed on a
// closed segment.
//
// Write applies exactly this predicate, so a caller that pre-checks with CanFit
// and gets nil is guaranteed the following Write will not be refused. That is
// what lets the encrypted write path resolve its LANDING segment — and with it
// the record ordinal that goes into the context-AAD — BEFORE sealing the
// record. See walShard.reserveLanding.
//
// bytesNeeded is monotonic non-decreasing in dataLen, so a caller that passes a
// conservative UPPER bound on the eventual framed length (a plaintext length
// plus the barrier's maximum envelope expansion) can only ever rotate early,
// never overflow the segment.
func (s *segment) CanFit(dataLen int) error {
	if s.closed {
		return ErrClosed
	}
	// A wrapped durability sequence would present an OLD generation as the
	// newest one and invert the acknowledgement proof recovery draws from it.
	// Refuse the segment instead; ErrSegmentFull rotates to a fresh one, whose
	// counter starts at zero. Unreachable in practice — see maxDurabilitySeq —
	// but the counter is load-bearing, so it is enforced rather than assumed.
	if s.durabilitySeq == maxDurabilitySeq {
		return ErrSegmentFull
	}
	// Calculate the total bytes this record will consume (headers + data).
	// Account for unflushed data in the block buffer (blockOffset).
	needed := s.bytesNeeded(dataLen)
	// A record whose framed size (data + per-block headers + padding) exceeds
	// the segment size limit can NEVER fit, even in a fresh segment. Return a
	// distinct permanent error instead of ErrSegmentFull so the caller does not
	// rotate to a new segment and retry forever (which would exhaust the
	// SaveState retry budget and fail the node); the error tells the caller the
	// record itself must be split.
	// The record itself must leave room for the sync mark the Sync that
	// acknowledges it will append (syncMarkReserve), or the mark could land
	// past maxFileSize. Both bounds include it.
	if int64(needed)+syncMarkReserve > s.maxFileSize {
		return ErrRecordTooLarge
	}
	if s.fileOffset+int64(s.blockOffset)+int64(needed)+syncMarkReserve > s.maxFileSize {
		return ErrSegmentFull
	}
	return nil
}

// RecordCount returns the number of logical records committed to this segment.
// It is both the ordinal the NEXT record will be written at and the index
// segment.ReadAll returns that record at during recovery. DiscardPending
// rewinds it with the write offsets, so it never counts a rolled-back record.
func (s *segment) RecordCount() uint64 {
	return s.recordSeq.Load()
}

// bytesNeeded calculates the total bytes a record of dataLen bytes will consume
// including all chunk headers and any block padding needed for block
// transitions.
func (s *segment) bytesNeeded(dataLen int) int {
	total := 0
	remaining := dataLen
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

// padAndFlushBlock zero-fills the remainder of the current block, writes
// everything not yet flushed (the block's new bytes plus the padding), then
// seals the block by advancing fileOffset by one block and resetting the block
// offset. This is the only path that advances fileOffset, preserving block-grid
// alignment.
//
// The write starts at the flush watermark, not at the block start, so bytes an
// earlier Sync already made durable are not put back in flight. The tail is
// written in ONE call ending exactly at the block boundary, so the block is
// complete on disk the moment this returns and fileOffset can move.
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

// flushBlock writes the in-progress block's NOT-YET-FLUSHED bytes to disk at
// their exact file position WITHOUT advancing fileOffset or resetting
// blockOffset: the block remains open so subsequent writes continue filling it
// and a later flush appends the newly added bytes after these. This is what
// makes a mid-block Sync safe — fileOffset never leaves the block grid that
// ReadAll strides over during recovery.
//
// Block-grid alignment is unaffected by writing a sub-range: the target offset
// is fileOffset+flushed with flushed < blockSize, so every byte still lands
// inside the block that starts at fileOffset, and fileOffset itself is moved
// ONLY by padAndFlushBlock (by exactly one blockSize, after the block has been
// completed on disk). ReadAll therefore keeps finding a chunk header at every
// multiple of blockSize.
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

// Sync flushes any buffered data and forces it to durable storage.
// Uses fdatasync on Linux for better performance when only data (not metadata)
// has changed.
//
// A successful Sync closes a durability generation: every record written so far
// is now acknowledged, and the next record is stamped with a higher sequence so
// recovery can tell the two apart. The counter is advanced only AFTER the
// fdatasync returns, because that return is the whole proof — bumping it first
// would mark the next batch as "separated by a completed sync" from records
// that a failed sync leaves in limbo. A Sync that acknowledges nothing new
// (no Write since the last completed one) closes no generation and appends
// no mark; see unsynced.
//
// Under noSync (tests only) no fdatasync is issued and NOTHING in the segment is
// durable, so there is no generation to close and the counter does not move.
// Every record such a segment holds carries generation 0, which recovery reads
// exactly as the truth it is: no Sync ever completed here, so no record in it
// was ever acknowledged, and truncating anywhere inside it destroys nothing that
// was. A test that needs recovery to see acknowledged data must therefore let
// the syncs run; see docs/log-storage/engineering.md ("Durability guarantees").
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

// writeSyncMark appends the RecordSyncMark chunk that opens the generation a
// completed Sync just began, and writes it through to the file. It is the
// witness that lets the generation BELOW it — the one the fdatasync just
// acknowledged — be told apart from a torn tail when nothing else is ever
// written after it; see record.go. The mark is not itself synced: if it is
// lost, recovery is exactly where it was without it.
//
// A mark starts only where a full header fits, so the padding it can force is
// shorter than a header and ReadAll reads it as block padding, never as a
// hole. CanFit's syncMarkReserve guarantees the room below maxFileSize.
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

// ReadAll reads all valid records from the segment sequentially. This is the
// primary recovery path. Records are returned in write order. Scanning stops
// at the first zero-filled or corrupted block (normal EOF for pre-allocated
// files).
//
// The second return value is the clean-prefix end: the file offset of the
// first hole or corrupt chunk when scanning stopped early (stoppedAtCorruption
// is then set), otherwise the full file size. Recovery truncates the segment
// there so the corrupt tail is repaired on disk instead of re-triggering
// prefix truncation — and the destruction of every later segment — on every
// subsequent restart.
//
// Damage is NOT accepted as a torn tail on the strength of being the first
// failure, and this holds for BOTH shapes it can take — a chunk whose CRC does
// not verify, and a zero-filled hole with data above it. Scanning continues over
// the remaining blocks and collects the DURABILITY GENERATION of every chunk
// that still decodes above the damage. What those generations say decides the
// outcome:
//
//   - They span more than one generation, or one of them is lower than a
//     generation already seen in the clean prefix. Either way a Sync completed
//     after some record above the damage was written, so that record was
//     acknowledged; truncating would destroy committed data. Fail closed
//     (ackedDataAfterCorruption, or sequenceRegression for the impossible
//     backwards case).
//   - They are all the SAME generation, and it is the segment's highest. Then
//     nothing above the damage has a completed Sync behind it: this is the one
//     batch that was still in flight, an ordinary interrupted write. Truncate to
//     the clean prefix, exactly as a torn tail always did. A generation that
//     WAS acknowledged never presents this way for long: the Sync that closed
//     it appended a sync mark of the next generation (writeSyncMark), and a
//     surviving mark is the higher generation the first case needs.
//   - Nothing decodes above the damage at all. The classic torn tail; truncate.
//
// The second case is the one this scheme exists for, and it is NOT rare: a batch
// spanning several blocks issues one WriteAt per block before a single
// fdatasync, so writeback may persist any subset of those pages. Losing an
// interior or leading page of block N while block N+1 survives leaves precisely
// "decodable records above a CRC failure", and reading that as corruption of
// acknowledged data would refuse to start, forever, after a routine power loss.
// See record.go for why the generation proves what it does.
//
// The zero-hole rule below is held to exactly the same standard. Truncating on
// sight, without consulting the generations, would let a zeroed sector under
// acknowledged data — a reallocated block, a partially restored file, a
// filesystem hole — silently destroy committed entries and roll the commit
// index backwards, while the IDENTICAL damage expressed as a flipped CRC byte at
// the same offset refuses to open. The two classifiers share one verdict.
//
// A crash cannot produce a hole under acknowledged data in the first place:
// writes are strictly ascending in file offset, so a block lying below a later
// generation was covered by that generation's fdatasync and its pages cannot
// revert to zeros. A hole whose survivors span a completed fdatasync is
// therefore medium damage or tampering, never an interrupted write, and the only
// safe answer is the one the CRC path already gives.
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

	// holeOffset remembers the file offset of the first zero region that
	// can only legitimately occur at the segment's tail: a wholly-zero
	// block, or a zero run longer than the maximum legal intra-block
	// padding (padAndFlushBlock pads at most recordHeaderSize bytes; only
	// the FINAL open block may have a longer zero tail). If any later
	// block contains data, that zero region was a write hole — a crash
	// lost block N while block N+1 reached the medium (fdatasync does not
	// order inter-block writes), or the medium lost a sector that had
	// already been acknowledged. Records after the hole must NOT be
	// resurrected either way: a WAL is valid only as a prefix, and
	// replaying post-hole entries would create a Raft log gap violating
	// Log Matching. Which of the two it is decides only whether the
	// segment may be TRUNCATED at the hole, and the durability generations
	// above it are what answer that.
	holeOffset := int64(-1)

	// corruptOffset remembers the file offset where the clean prefix ends: the
	// first chunk that failed to decode, or the zero hole that turned out to
	// have data above it. Recording it does not end the scan — the blocks after
	// it are still swept, gathering the durability generations that say whether
	// the damage sits below acknowledged data. Everything from corruptOffset on
	// is untrusted either way — no record past it is ever appended to records.
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
			// A real I/O error (EIO, permission, a failing sector) must NOT be
			// silently treated as end-of-data: combined with prefix-only
			// validity it would masquerade as data loss and drop all higher
			// segments. That holds however many bytes came back — a SHORT read
			// carrying an error is still an error, and processing its prefix
			// would then stride a whole block forward and replay the following
			// blocks with neither a hole nor a corruption recorded.
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

		// Post-corruption sweep: the clean prefix is already fixed, so this
		// block is examined for one thing only — the durability generations of
		// whatever still decodes in it. The sweep ends early the moment those
		// generations settle the question; otherwise it runs to end of file,
		// because "every surviving record belongs to the final in-flight batch"
		// is a statement about the WHOLE remainder and cannot be concluded from
		// a prefix of it.
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
					// A wholly-zero block, or a zero tail longer than the
					// largest legal block padding, is only valid as the
					// final region of the segment. Remember it; if data
					// follows in a later block this was a hole.
					if pos == 0 || len(block)-pos > recordHeaderSize {
						if holeOffset < 0 {
							holeOffset = offset + int64(pos)
						}
					}
					break // End of records in this block.
				}
			}

			// A sub-header remainder is NOT treated as padding, and the writer
			// is what settles that.
			//
			// segment.Write starts a chunk only when at least recordHeaderSize+1
			// bytes are free (writeSyncMark, whose chunk is exactly a header,
			// when recordHeaderSize are), and a chunk occupies every byte it
			// claims, so the walk can only ever arrive here with
			// 1..recordHeaderSize-1 bytes left when the writer padded them —
			// and padAndFlushBlock pads with ZEROS. An all-zero remainder is therefore the legal case, and it
			// has already been taken by the branch above, which breaks. Reaching
			// this point means the remainder is short AND contains a non-zero
			// byte, which is a shape no writer of this segment can produce.
			//
			// It is left to fall through into decodeRecord, which reports
			// ErrCorruptedRecord for a buffer shorter than a header, so the
			// damage takes exactly the same route as a failed CRC: the clean
			// prefix ends here and the durability generations above decide
			// whether truncating is safe. Skipping it instead — which a `break`
			// did — silently discarded the evidence and, when the short
			// remainder sat in an INTERIOR block, went on to replay the records
			// in every block above it, manufacturing precisely the Raft log gap
			// the prefix rule exists to prevent.

			// Data after a zero hole. The clean prefix ends AT THE HOLE and
			// nothing above it is ever replayed — a WAL is valid only as a
			// prefix, and resurrecting post-hole entries would create a Raft log
			// gap violating Log Matching. Whether the segment may be TRUNCATED
			// there is a different question, and it is answered exactly as it is
			// for a failed CRC: by the durability generations of whatever still
			// decodes above the damage. Record the hole as the corruption point
			// and let the post-corruption sweep gather that evidence instead of
			// truncating on sight.
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

				// Sweep the REST OF THIS BLOCK, starting at the data that
				// follows the hole, before striding on. Same blind spot the CRC
				// path closes: when the hole sits in the last block that holds
				// data there are no later blocks to probe, and the evidence that
				// decides the verdict would never be gathered at all.
				if s.gatherEvidence(block, offset, pos, prefixMaxSeq, &ev, len(records)) {
					return records, corruptOffset, nil
				}
				break
			}

			data, recType, durSeq, _, consumed, err := decodeRecord(block[pos:])
			if err != nil {
				s.stoppedAtCorruption = true
				corruptOffset = offset + int64(pos)
				// Corrupted or torn record. A WAL is only valid as a prefix:
				// skipping the bad record and resuming at the next block could
				// recover entries/state that sit AFTER a gap, silently
				// violating Raft log-matching. The clean prefix collected so
				// far is final from here on. Any in-progress multi-chunk record
				// (partial) is simply abandoned — no need to clear it.
				//
				// The scan does NOT end here, though: whether this is an
				// interrupted write (truncate) or damage below acknowledged
				// data (fail-stop) is decided by the durability generations
				// further down, so the outer loop keeps striding blocks in the
				// post-corruption sweep above.
				slog.Warn("waldb: stopping WAL recovery at first corrupt record",
					"segment_id", s.id,
					"dir", s.dir,
					"offset", corruptOffset,
					"prefix_generation", prefixMaxSeq,
					"recovered_records", len(records),
					"error", err,
				)

				// Sweep the REST OF THIS BLOCK before striding on. Without this
				// the test has a blind spot exactly where it matters most: when
				// the damage is in the last block that holds data there are no
				// later blocks to probe, no evidence is gathered at all, and
				// recovery calls a fixed prefix boundary the segment's tail
				// without ever looking at what sits above it in that block.
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
				// Evidence only: its generation has already raised
				// prefixMaxSeq above, which is the whole of its testimony.
				// It carries no record and never lands inside one — Write
				// emits every chunk of a record in one call and never syncs
				// between them — so partial is left alone.
			}

			pos += consumed
		}

		offset += int64(s.blockSize)
	}

	// The sweep reached end of file without the generations ever settling the
	// question against truncation: either nothing decoded above the damage at
	// all, or everything that did belongs to the single generation that was
	// still in flight. Both are interrupted writes — PROVIDED the damage begins
	// at or above the acknowledged end, which the survivors' generation start
	// now states exactly. Report the clean-prefix end so recovery can repair the
	// segment there, or fail closed when acknowledged bytes lie in the damage.
	if corruptOffset >= 0 {
		if ev.found && int64(ev.genStart) > corruptOffset {
			// The survivors' generation began ABOVE the corruption offset, so
			// the fdatasync that opened it acknowledged every byte below
			// genStart — including the bytes from corruptOffset up to it, which
			// are destroyed. A crash cannot produce this (a lost writeback
			// leaves an fsynced page at its persisted content, so an
			// interrupted write's damage begins at or above the acknowledged
			// end); this is media damage or tampering under acknowledged data,
			// and truncating would silently shorten the log this node promised
			// its peers. The verdict is the same as ackedDataAfterCorruption's
			// and carries its own name so an operator can tell the two apart.
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

// durabilityEvidence accumulates the durability generations of the chunks that
// still decode ABOVE a corruption. Only the extremes matter: whether they span
// a completed fdatasync (maxSeq > minSeq) and whether any of them precedes the
// clean prefix (minSeq below it).
type durabilityEvidence struct {
	found  bool
	minSeq uint32
	maxSeq uint32
	// genStart is the generation start carried by the survivors, the highest
	// seen. Every chunk of one generation carries the same value, and a
	// higher generation's is never lower, so the maximum is the acknowledged
	// end the survivors testify to; taking it — rather than the first seen —
	// makes a CRC-collision false positive err toward failing closed.
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

// gatherEvidence walks one block lying at or above a corruption and folds the
// durability generation of every chunk that still decodes in it into ev. It
// reports whether the evidence is now CONCLUSIVE against truncating, having
// already set the flag that says why.
//
// It probes every byte offset rather than only the block start, resynchronizing
// on the length of each chunk it does decode. After a decode failure there IS no
// known next boundary — the length field that would have given it is part of
// what failed — and the block start alone is a blind spot exactly where the
// damage most often is. This is not the speculative resynchronization that
// replay refuses: nothing found here is ever replayed, and every candidate
// answers with its own CRC over its own declared length, so a false positive
// needs a CRC-32C collision against arbitrary bytes rather than a lucky guess.
// A false positive costs a fail-stop where a truncation would have done; a false
// negative costs acknowledged records, silently. The asymmetry is the whole
// justification for probing this widely.
//
// Zero padding cannot produce a false positive: a zeroed header decodes to
// record type 0, which isValidRecordType rejects before the CRC is computed.
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

		// A generation BELOW the clean prefix's cannot come from this writer:
		// the counter only ever rises within a segment. Something else put
		// these bytes here. (A CRC-32C collision on a probed offset could also
		// land here, at roughly 2^-32 per probe; it fails the open, which is the
		// safe direction of a mistake this rare.)
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

// Close flushes any pending block data, makes the file durable, truncates it
// to its written size and closes it. After Close, all operations return
// ErrClosed.
//
// The fdatasync is what makes the segment's LAST sync mark durable. Sync
// writes the mark after its own fdatasync returns, so the mark of the final
// generation is the one unsynced thing a closed segment would otherwise
// carry — and a non-final segment must carry nothing unsynced, because
// recovery refuses damage there (ErrNonFinalSegmentCorruption) on exactly
// that guarantee. A mark torn by a power loss inside its writeback window
// would fail its CRC and turn an ordinary crash into a refused open. One
// fdatasync per segment lifetime buys that back; under noSync there is no
// durability to speak of and none is issued. A failure is reported as
// errSegmentCloseSync so the caller can fail-stop on it as it does for Sync.
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

	// Truncate the file to the actual written size to reclaim pre-allocated
	// space beyond what was written. Size() accounts for the open block
	// (fileOffset is only the block-aligned start; the live block tail lives
	// in blockOffset).
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

// DiscardPending truncates the segment back to the given file offset and
// resets the block buffer. This discards any data written (but not fsynced)
// since the caller's saved checkpoint. It is used to roll back partial
// batch writes when a later write in the batch fails.
//
// The caller must provide the fileOffset, blockOffset and record count
// captured before the batch began. The segment file is truncated to
// cleanFileOffset, the write position is seeked back, and the block buffer is
// zeroed.
//
// cleanRecordCount rewinds the logical-record ordinal in lockstep with the
// offsets. This is not bookkeeping: the ordinal is bound into every encrypted
// record's context-AAD, so an ordinal left ahead of the file contents would
// make every record written after the first rolled-back batch decrypt against
// the wrong context and fail authentication at the next recovery — silently
// losing the whole segment tail.
func (s *segment) DiscardPending(cleanFileOffset int64, cleanBlockOffset int, cleanRecordCount uint64) error {
	if s.closed {
		return ErrClosed
	}

	// The durable boundary at batch start is the block-aligned start plus the
	// open-block tail. Truncate exactly there: this removes any blocks the
	// failed batch sealed beyond the checkpoint AND any batch bytes appended
	// into the open block, while preserving the pre-batch open-block content
	// that the previous successful Sync already made durable.
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
		// The truncation dropped the segment's fallocate reservation, so
		// re-take it: without this every write after a rollback pays a file
		// extension and the ENOSPC pre-reservation the design relies on is
		// gone. Best-effort, exactly as in createSegment — a failed
		// reservation costs performance, never correctness.
		if err := s.PreAllocate(); err != nil {
			slog.Debug("waldb: re-preallocate after discard failed",
				"segment_id", s.id, "dir", s.dir, "error", err)
		}
	}

	// Restore the open block's pre-batch content. If the failed batch sealed a
	// block (padAndFlushBlock) the in-memory blockBuf was reused for a later
	// block and no longer holds the checkpoint block, so reload the pre-batch
	// tail from disk where the previous successful Sync left it. (After the
	// truncate above, [cleanFileOffset : cleanEnd] is exactly that tail.)
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
	// The restored open-block prefix was just read back FROM DISK (and the
	// checkpoint was taken after a flushBlock, so those bytes were durable),
	// which makes blockBuf[:cleanBlockOffset] byte-for-byte what the file
	// already holds. The flush watermark therefore rewinds to exactly
	// cleanBlockOffset: re-writing that prefix would put acknowledged bytes
	// back in flight, and leaving the watermark higher would skip bytes the
	// next flush must still write.
	s.flushed = cleanBlockOffset
	s.recordSeq.Store(cleanRecordCount)
	// The checkpoint is a position the previous batch's Sync had already
	// acknowledged (it is captured before a batch begins, above that Sync's
	// mark), so nothing below it is waiting on a Sync; the rolled-back bytes
	// were the only unsynced ones. Leaving the flag set would let the next
	// idle Sync open a generation for nothing and append a mark CanFit
	// reserved no room for.
	s.unsynced = false
	return nil
}

// PreAllocate re-reserves disk space for the segment up to its configured
// maxFileSize. Used after a rollback truncates the file to zero so subsequent
// writes do not pay repeated file extensions. Mirrors createSegment: the
// reservation is best-effort and only available when the file backend
// implements FilePreAllocator.
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
