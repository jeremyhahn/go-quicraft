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
	"log/slog"
	"path/filepath"
	"sort"
)

// Operator-authorized repair of a WAL that recovery refuses to open.
//
// Recovery fails closed on ErrMidSegmentCorruption, ErrNonFinalSegmentCorruption,
// ErrAcknowledgedDataLost and ErrRecordSequenceRegression. The first three mean
// the same thing: acknowledged records lie in or above the damage — proved by a
// durability generation above them, by the segment not being the highest-
// numbered one, or by the survivors' generation start sitting above the damage
// — so prefix-truncating would silently destroy committed entries and roll
// term/vote backwards. The fourth means the bytes above the damage carry a
// generation this writer cannot have produced, so recovery cannot classify them
// at all. That posture is right, and nothing here changes it — Open still
// refuses, every time, on its own.
//
// What it cannot be is the ONLY exit. An ordinary power loss does not arrive
// here: a single fdatasync covers as many WriteAt calls as the batch had
// blocks, writeback can persist an arbitrary subset of those pages, and losing
// an interior page leaves decodable records above the damage — but every
// surviving record carries the in-flight batch's generation, so recovery
// truncates and boots (see segment.ReadAll).
//
// What does arrive here is damage below acknowledged data: bit rot, a
// misdirected write, a lying fsync, tampering. Rare, and not explicable as an
// interrupted write, but not impossible. "Restore from a peer" answers it for a replicated
// shard and answers nothing at all for a single-node deployment, which would be
// left with a node that will not start and no procedure that makes it start.
//
// Repair is that procedure. It is deliberately NOT reachable from Open, from an
// Option, or from any configuration a node could carry into production by
// accident: it is a separate call an operator makes by hand, naming the exact
// shard, segment and offset from the error, having decided that losing the
// records above that offset is preferable to not booting. Every guard below
// exists to make sure the truncation that happens is the one that was
// authorized and no other.
//
// The lost records are lost. This is data destruction with a signature on it,
// not a recovery.

// ErrRepairShardOutOfRange is returned when a RepairTarget names a shard index
// outside [0, DefaultShardCount).
var ErrRepairShardOutOfRange = errors.New("waldb: repair target names a shard index that does not exist")

// ErrRepairNoCorruption is returned when the named segment scans cleanly to its
// end. There is nothing to repair, and truncating a healthy segment on an
// operator's say-so would destroy data for no reason — most likely the target
// was copied from a stale error, or a previous repair already ran.
var ErrRepairNoCorruption = errors.New("waldb: the named WAL segment is not corrupt; nothing to repair")

// ErrRepairOffsetMismatch is returned when TruncateAt is not the clean-prefix
// end the scan itself computes.
//
// This is the interlock that makes the API safe to expose: an operator cannot
// pick an offset, only CONFIRM the one the WAL named. A mistyped or stale
// offset is refused rather than applied, so the failure mode is a second look
// at the error message instead of a segment cut at an arbitrary byte — which
// would leave a half record at the end and, if the cut landed above the real
// clean end, silently readmit the corrupt region on the next open.
var ErrRepairOffsetMismatch = errors.New("waldb: repair offset is not the clean-prefix end of the named segment")

// RepairTarget names exactly one truncation an operator authorizes. Every field
// is copied from the *SegmentCorruptionError that Open's RecoveryError wraps:
// ShardIndex and SegmentID carry over by name, and TruncateAt is its CleanEnd.
type RepairTarget struct {
	// ShardIndex is the WAL shard holding the segment, in [0, DefaultShardCount).
	ShardIndex int

	// SegmentID is the corrupt segment's numeric ID (its %06d.log name).
	SegmentID uint64

	// TruncateAt is the clean-prefix end in bytes. It must equal the offset the
	// scan computes; it is a confirmation, not a free parameter.
	TruncateAt int64
}

// RepairResult reports what a Repair actually did, so the operator can record
// it and reconcile the node against its peers afterwards.
type RepairResult struct {
	// RecordsKept is the number of records the surviving clean prefix holds.
	RecordsKept int

	// BytesDiscarded is how much of the named segment was cut away.
	BytesDiscarded int64

	// SegmentsDeleted lists the higher-numbered segments removed from the same
	// shard. They sit above a truncation point, so they are unreachable by the
	// prefix-validity rule whatever they contain.
	SegmentsDeleted []uint64
}

// Repair truncates one corrupt WAL segment to its clean prefix and deletes the
// segments above it, making a WAL that recovery refuses to open openable again
// at the cost of every record above TruncateAt.
//
// It is the documented exit from ErrMidSegmentCorruption,
// ErrNonFinalSegmentCorruption, ErrAcknowledgedDataLost and
// ErrRecordSequenceRegression. It must be run with the database CLOSED — no
// process may hold the directory — and the node it belongs to should be treated
// as having lost its most recent writes until it has caught up from its peers.
// On a single-node deployment there is nothing to catch up from and the loss is
// final.
//
// The opts are the same options the DB is opened with; only the filesystem and
// the block size are consulted, and the block size MUST match the one the
// directory was written with or the scan reports a clean end computed on the
// wrong grid. No barrier is needed: the scan works at the CRC framing level and
// never decrypts a record, so a sealed or unavailable barrier does not stand
// between an operator and a bootable node.
func Repair(dir string, target RepairTarget, opts ...Option) (RepairResult, error) {
	o := defaultDBOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if target.ShardIndex < 0 || target.ShardIndex >= DefaultShardCount {
		return RepairResult{}, ErrRepairShardOutOfRange
	}
	// Hold the repair to the same geometry rule Open enforces. A repair run
	// under the wrong block size scans a grid the segment was not written on,
	// so the clean end it computes is meaningless — and this is the one code
	// path whose entire job is to truncate at that number. The offset interlock
	// would catch it as a mismatch, but reporting the real cause beats making
	// an operator wonder why the offset from the error is being rejected.
	if err := checkRepairGeometry(o, dir); err != nil {
		return RepairResult{}, err
	}

	shard := repairShard(o, filepath.Join(dir, shardDirName(target.ShardIndex)), target.ShardIndex)

	records, cleanEnd, size, err := scanSegmentForRepair(shard, target.SegmentID)
	if err != nil {
		return RepairResult{}, err
	}
	if cleanEnd != target.TruncateAt {
		return RepairResult{}, &RepairOffsetMismatchError{
			ShardIndex: target.ShardIndex,
			SegmentID:  target.SegmentID,
			Requested:  target.TruncateAt,
			CleanEnd:   cleanEnd,
		}
	}

	orphans, err := shard.segmentIDsAbove(target.SegmentID)
	if err != nil {
		return RepairResult{}, err
	}

	slog.Error("waldb: OPERATOR-AUTHORIZED WAL REPAIR; records above the clean "+
		"prefix are being destroyed and cannot be recovered from this node",
		"dir", shard.dir,
		"shard_index", target.ShardIndex,
		"segment_id", target.SegmentID,
		"truncate_at", cleanEnd,
		"bytes_discarded", size-cleanEnd,
		"records_kept", len(records),
		"segments_deleted", orphans,
	)

	// High to low, for the same reason recovery repairs in that order: the
	// segments above the truncation point go first and their unlinks are made
	// durable (discardSegmentsAbove fsyncs the directory and reports a failure
	// to do so), and only then is the named segment cut. A crash between the two
	// leaves the corrupt segment still corrupt, so a re-run of the repair finds
	// the same clean end and does the same thing. Cutting first would leave a
	// segment that ends at a clean record boundary with CRC-valid segments still
	// sitting above it, and the next recovery would replay straight through both
	// — readmitting exactly the records the operator authorized destroying, with
	// a hole where the cut was. That is the double-vote case syncDirFS exists
	// for, arrived at from the one code path an operator drives by hand.
	if err := shard.discardSegmentsAbove(orphans); err != nil {
		return RepairResult{}, err
	}
	if err := shard.truncateCorruptSegment(target.SegmentID, cleanEnd); err != nil {
		return RepairResult{}, &RecoveryError{SegmentID: target.SegmentID, Err: err}
	}

	slog.Warn("waldb: WAL repair complete; this node has lost its most recent "+
		"writes and must be reconciled against its peers before it is trusted",
		"dir", shard.dir,
		"shard_index", target.ShardIndex,
		"records_kept", len(records),
	)

	return RepairResult{
		RecordsKept:     len(records),
		BytesDiscarded:  size - cleanEnd,
		SegmentsDeleted: orphans,
	}, nil
}

// checkRepairGeometry refuses a repair configured with a block size the
// directory was not written with, reusing the marker Open consults.
func checkRepairGeometry(o dbOptions, dir string) error {
	recorded, present, err := readWALFormat(o.fs, dir)
	if err != nil {
		return err
	}
	if present && recorded != o.blockSize {
		return &WALGeometryMismatchError{
			Dir:        dir,
			Recorded:   recorded,
			Configured: o.blockSize,
		}
	}
	return nil
}

// repairShard builds the minimum walShard the repair primitives need.
//
// truncateCorruptSegment and discardSegmentsAbove are the SAME code recovery
// uses for a legal torn tail, and reusing them is the point: an operator repair
// must leave the directory in a state recovery would itself have produced, down
// to the fsync of the file and of the shard directory. Only the fields those
// two touch are populated; the shard is never published to a DB, never written
// to, and does not outlive the call.
func repairShard(o dbOptions, shardDir string, idx int) *walShard {
	return &walShard{
		fs:            o.fs,
		dir:           shardDir,
		opts:          o,
		shardIdx:      uint64(idx),
		knownSegments: make(map[uint64]bool),
		metaSegments:  make(map[nodeKey]metaSegments),
	}
}

// scanSegmentForRepair reads the named segment at the framing level and returns
// its clean prefix, the offset that prefix ends at, and the file's current
// size. It refuses a segment that is not actually corrupt.
func scanSegmentForRepair(s *walShard, segID uint64) (records [][]byte, cleanEnd, size int64, err error) {
	name := filepath.Join(s.dir, segmentFilename(segID))
	f, err := s.fs.Open(name)
	if err != nil {
		return nil, 0, 0, &RecoveryError{SegmentID: segID, Err: err}
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("waldb: closing a segment after a repair scan failed",
				"dir", s.dir, "segment_id", segID, "error", closeErr)
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, 0, &RecoveryError{SegmentID: segID, Err: err}
	}

	readSeg := &segment{
		f:         f,
		id:        segID,
		dir:       s.dir,
		blockSize: s.opts.blockSize,
		blockBuf:  make([]byte, s.opts.blockSize),
	}
	records, cleanEnd, err = readSeg.ReadAll()
	if err != nil {
		return nil, 0, 0, err
	}
	if !readSeg.stoppedAtCorruption {
		return nil, 0, 0, ErrRepairNoCorruption
	}
	return records, cleanEnd, fi.Size(), nil
}

// segmentIDsAbove lists the segment IDs in this shard's directory that are
// strictly greater than segID, in ascending order. They are the segments a
// truncation at segID makes unreachable.
func (s *walShard) segmentIDsAbove(segID uint64) ([]uint64, error) {
	entries, err := s.fs.ReadDir(s.dir)
	if err != nil {
		return nil, &DBOpenError{Dir: s.dir, Err: err}
	}
	var above []uint64
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		id, ok := parseSegmentFilename(de.Name())
		if !ok || id <= segID {
			continue
		}
		above = append(above, id)
		s.knownSegments[id] = true
	}
	sort.Slice(above, func(i, j int) bool { return above[i] < above[j] })
	return above, nil
}

// RepairOffsetMismatchError reports a repair whose TruncateAt is not the clean
// prefix end, showing both offsets so the operator can see which one to use. It
// unwraps to ErrRepairOffsetMismatch.
type RepairOffsetMismatchError struct {
	ShardIndex int
	SegmentID  uint64
	Requested  int64
	CleanEnd   int64
}

// Error implements the error interface.
func (e *RepairOffsetMismatchError) Error() string {
	return "waldb: repair of shard " + itoa(e.ShardIndex) + " segment " +
		itoa(int(e.SegmentID)) + " requested truncation at " + itoa(int(e.Requested)) +
		" but the clean prefix ends at " + itoa(int(e.CleanEnd)) + "; " +
		ErrRepairOffsetMismatch.Error()
}

// Unwrap returns the sentinel so errors.Is(err, ErrRepairOffsetMismatch) holds.
func (e *RepairOffsetMismatchError) Unwrap() error {
	return ErrRepairOffsetMismatch
}
