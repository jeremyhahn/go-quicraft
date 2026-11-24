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

// TestSnapshotFloorScan_BoundsAccumulationAndRateLimitsTheReport: the blocker count is complete, the sample bounded and the record rate-limited.
func TestSnapshotFloorScan_BoundsAccumulationAndRateLimitsTheReport(t *testing.T) {
	baseDir := t.TempDir()

	// Far more blocking directories than a single record can name, spread over
	// two shards so a scan that stops at the first blocked SHARD is caught too.
	const blockingPerShard = 40
	for i := 0; i < blockingPerShard; i++ {
		writeEpochlessShardSnapshot(t, baseDir, 1, 1, uint64(100+i))
		writeEpochlessShardSnapshot(t, baseDir, 2, 1, uint64(100+i))
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

	// The record carries the complete count and is emitted at most once per interval.
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

// TestParseSnapshotMeta_HugeDeclaredBodyLength pins that the declared body length is capped before it is narrowed to int, on any GOARCH.
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

	// The record carries the body it declares, so only the cap rejects it.
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

	// The cap must accept any record the format can produce, including newer versions'.
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
