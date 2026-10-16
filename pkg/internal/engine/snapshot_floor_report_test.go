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

package engine

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The floor scan's diagnostics must be bounded
// ---------------------------------------------------------------------------

// TestSnapshotFloorScan_BoundsAccumulationAndRateLimitsTheReport pins the cost
// of the scan's operator diagnostics without weakening the property they exist
// for.
//
// The scan deliberately does NOT short-circuit: the verdict is settled by the
// first unevaluable directory, but it keeps going so one log record can name
// every blocking directory rather than the operator fixing them one scan at a
// time. What was unbounded was the ACCUMULATION: one struct and one path string
// per blocking directory, on a host that can hold thousands of shards, allocated
// on EVERY snapshot save (purgeOldEpochs runs the scan after each one) — to
// print at most maxReportedFloorBlockers of them. The warning itself was emitted
// on every save too, describing a condition that only changes when directories
// roll off or are repaired.
//
// So: the COUNT is complete, the retained sample is bounded, and the record is
// rate-limited.
func TestSnapshotFloorScan_BoundsAccumulationAndRateLimitsTheReport(t *testing.T) {
	baseDir := t.TempDir()

	// Far more blocking directories than a single record can name, spread over
	// two shards so a scan that stops at the first blocked SHARD is caught too.
	const blockingPerShard = 40
	for i := 0; i < blockingPerShard; i++ {
		writeLegacyShardSnapshot(t, baseDir, 1, 1, uint64(100+i), legacyMetaSizeNoEpoch)
		writeLegacyShardSnapshot(t, baseDir, 2, 1, uint64(100+i), legacyMetaSizeNoEpoch)
	}
	// One evaluable encrypted directory per shard, to prove the scan still
	// folds in what it CAN evaluate while blocked.
	writeShardSnapshot(t, baseDir, 1, 1, 900, 6)
	writeShardSnapshot(t, baseDir, 2, 1, 900, 4)

	var scan snapshotFloorScan
	scan.addNode(snapshotDirNode(baseDir, 1, 1))
	scan.addNode(snapshotDirNode(baseDir, 2, 1))

	if !scan.blocked() {
		t.Fatal("scan is not blocked although directories with an indeterminate epoch exist")
	}
	// No short-circuit: every blocking directory was visited and counted.
	if scan.blockedCount != 2*blockingPerShard {
		t.Errorf("scan counted %d blocking directories, want %d: the scan stopped early, "+
			"so the operator log understates the work still to do",
			scan.blockedCount, 2*blockingPerShard)
	}
	// ...and the evaluable ones were still folded in.
	if scan.minEpoch != 4 {
		t.Errorf("scan minEpoch = %d, want 4: evaluable directories must still be folded in",
			scan.minEpoch)
	}
	// Bounded retention.
	if len(scan.blockers) > maxReportedFloorBlockers {
		t.Errorf("scan retained %d blocker records, want at most %d: this allocation "+
			"happens on every snapshot save, for every shard, forever",
			len(scan.blockers), maxReportedFloorBlockers)
	}
	if len(scan.blockers) != maxReportedFloorBlockers {
		t.Errorf("scan retained %d blocker records, want a full sample of %d",
			len(scan.blockers), maxReportedFloorBlockers)
	}

	// The record must carry the COMPLETE count even though the sample is a
	// slice of it, and must be emitted at most once per interval.
	buf, restore := captureWarnOutput(t)
	defer restore()
	lastFloorReport.Store(0)

	scan.report("global")
	first := buf.String()
	if !strings.Contains(first, "epoch purge suppressed") {
		t.Fatalf("no suppression warning was emitted: %q", first)
	}
	if !strings.Contains(first, "blocking_directories="+strconv.Itoa(2*blockingPerShard)) {
		t.Errorf("the warning does not report the complete blocking count: %q", first)
	}

	buf.Reset()
	for i := 0; i < 100; i++ {
		scan.report("global")
	}
	if buf.Len() != 0 {
		t.Errorf("%d further reports within the rate-limit interval emitted %q; the scan "+
			"runs after every snapshot save, so this is one record per save saying "+
			"the same thing", 100, buf.String())
	}

	// The limiter must not silence the condition permanently: once the interval
	// has elapsed the operator hears about it again.
	lastFloorReport.Store(0)
	scan.report("global")
	if buf.Len() == 0 {
		t.Error("no warning after the rate-limit interval elapsed: the suppression " +
			"condition would go unreported for the life of the process")
	}
}

// captureWarnOutput installs a Warn-level slog handler backed by a buffer.
// captureLogOutput filters at Error, which drops the record under test.
func captureWarnOutput(t *testing.T) (*logCapture, func()) {
	t.Helper()
	buf := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	return buf, func() { slog.SetDefault(prev) }
}

// ---------------------------------------------------------------------------
// A declared body length must be compared unsigned
// ---------------------------------------------------------------------------

// TestParseSnapshotMeta_HugeDeclaredBodyLength covers a record that declares an
// absurd body length.
//
// The declared length is a uint32 read from a file and is used to SLICE the
// body. Converting it to int before the bounds check is safe only where int is
// 64 bits: on a 32-bit GOARCH a declared length of 2^31 or more becomes
// NEGATIVE, "claims more body than the file holds" is then false, and
// body[:bodyLen] panics — inside the DEK purge-floor scan, which walks every
// snapshot directory on the host and whose entire input is bytes on disk.
//
// A test that only falsifies the length on a SHORT record cannot see that: on
// amd64 the signed comparison gives the same answer as the unsigned one, so
// such a test passes with the bug present and only a 386 build can distinguish
// them. This one asserts the property that IS architecture-independent —
// the declared length is capped BEFORE it is narrowed, so a value that could
// truncate to a negative int is unrepresentable here on any GOARCH — and the
// oversized_declaration_with_a_body_to_match case makes the cap load-bearing on
// a 64-bit build: the file really does hold the bytes it declares, so every
// length-versus-available comparison, signed or unsigned, accepts it and only
// the cap rejects it.
func TestParseSnapshotMeta_HugeDeclaredBodyLength(t *testing.T) {
	t.Run("declared beyond the file", func(t *testing.T) {
		for _, declared := range []uint32{1 << 31, 1<<32 - 1, 0x8000_0038, maxSnapshotMetaBodyLen + 1} {
			t.Run(strconv.FormatUint(uint64(declared), 16), func(t *testing.T) {
				// A well-formed header and a real body, with only the declared
				// length falsified — so nothing but the length check can reject it.
				rec := MarshalSnapshotMeta(SnapshotMeta{
					Index: 900, Term: 5, ShardID: 1, ReplicaID: 1, CreatedAt: 1, Epoch: 3,
				})
				binary.LittleEndian.PutUint32(rec[snapshotMetaBodyLenOff:], declared)

				meta, err := ParseSnapshotMeta(rec)
				if !errors.Is(err, ErrSnapshotMetaCorrupt) {
					t.Fatalf("ParseSnapshotMeta(bodyLen=%#x) = (%+v, %v), want ErrSnapshotMetaCorrupt; "+
						"a length that truncates to a negative int walks straight past the "+
						"bounds check and panics on the slice", declared, meta, err)
				}
			})
		}
	})

	// The falsifiable half. The record CARRIES the body it declares, so the
	// "claims more body than the file holds" guard — in either arithmetic — is
	// false and the parse proceeds unless the declared length is capped.
	t.Run("oversized declaration with a body to match", func(t *testing.T) {
		const declared = maxSnapshotMetaBodyLen + 8
		rec := make([]byte, snapshotMetaHeaderSize+declared)
		copy(rec, MarshalSnapshotMeta(SnapshotMeta{
			Index: 900, Term: 5, ShardID: 1, ReplicaID: 1, CreatedAt: 1, Epoch: 3,
		}))
		binary.LittleEndian.PutUint32(rec[snapshotMetaBodyLenOff:], declared)

		meta, err := ParseSnapshotMeta(rec)
		if !errors.Is(err, ErrSnapshotMetaCorrupt) {
			t.Fatalf("ParseSnapshotMeta(bodyLen=%d over a %d-byte body) = (%+v, %v), want "+
				"ErrSnapshotMetaCorrupt; an unbounded declared length is one a 32-bit "+
				"build narrows to a negative int, and the epoch it yields is fed to the "+
				"DEK purge floor as fact", declared, len(rec)-snapshotMetaHeaderSize, meta, err)
		}
	})

	// The cap must not reject a record this format can actually produce,
	// including one written by a NEWER version with more body fields.
	t.Run("a body at the cap is still accepted", func(t *testing.T) {
		const declared = maxSnapshotMetaBodyLen
		rec := make([]byte, snapshotMetaHeaderSize+declared)
		copy(rec, MarshalSnapshotMeta(SnapshotMeta{
			Index: 900, Term: 5, ShardID: 1, ReplicaID: 1, CreatedAt: 1, Epoch: 3,
		}))
		binary.LittleEndian.PutUint32(rec[snapshotMetaBodyLenOff:], declared)

		meta, err := ParseSnapshotMeta(rec)
		if err != nil {
			t.Fatalf("ParseSnapshotMeta(bodyLen=%d) = %v, want a parsed record", declared, err)
		}
		if meta.Index != 900 || meta.Epoch != 3 || !meta.EpochRecorded {
			t.Fatalf("parsed record = %+v, want Index 900 and Epoch 3 recorded", meta)
		}
	})
}
