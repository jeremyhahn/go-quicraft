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
	"bytes"
	"encoding/binary"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// M2: the floor scan's diagnostics must be bounded
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
func captureWarnOutput(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	return &buf, func() { slog.SetDefault(prev) }
}

// ---------------------------------------------------------------------------
// L1: a declared body length must be compared unsigned
// ---------------------------------------------------------------------------

// TestParseSnapshotMeta_HugeDeclaredBodyLength covers a record that declares a
// body of 2^31 bytes or more.
//
// The declared length is a uint32 read from a file. Converting it to int before
// the bounds check is safe only where int is 64 bits: on a 32-bit GOARCH such a
// value becomes NEGATIVE, "claims more body than the file holds" is then false,
// and body[:bodyLen] panics — inside the DEK purge-floor scan, which walks every
// snapshot directory on the host and whose entire input is bytes on disk. The
// comparison is therefore made in unsigned arithmetic, as waldb's replayBootstrap
// deliberately does for the same reason.
//
// The assertion holds on every GOARCH: an unsigned comparison rejects these
// records as truncated on a 64-bit build too, so a regression to a signed
// comparison changes the 64-bit result as well.
func TestParseSnapshotMeta_HugeDeclaredBodyLength(t *testing.T) {
	for _, declared := range []uint32{1 << 31, 1<<32 - 1, 0x8000_0038} {
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
}
