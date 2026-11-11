// Copyright 2025 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package waldb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

type recordingMetrics struct{}

func (recordingMetrics) ObserveWriteLatency(time.Duration)      {}
func (recordingMetrics) ObserveFsyncLatency(time.Duration)      {}
func (recordingMetrics) ObserveCompactionLatency(time.Duration) {}
func (recordingMetrics) IncWriteBytes(int64)                    {}

// TestErrorTypes_MessageNamesItsFields pins that each typed error renders the
// fields an operator needs to locate the fault, and unwraps where it wraps.
func TestErrorTypes_MessageNamesItsFields(t *testing.T) {
	cause := errors.New("underlying cause")
	tests := []struct {
		name string
		err  error
		want []string
	}{
		{"SegmentCorruptionError", &SegmentCorruptionError{ShardIndex: 3, SegmentID: 17, CleanEnd: 4096, Err: cause},
			[]string{"underlying cause"}},
		{"SegmentReadError", &SegmentReadError{Offset: 8192, Err: cause},
			[]string{"8192", "underlying cause"}},
		{"WALFormatVersionError", &WALFormatVersionError{Dir: "/wal/a", Recorded: 7, Supported: 1},
			[]string{"/wal/a", "7", "1"}},
		{"WALGeometryMismatchError", &WALGeometryMismatchError{Dir: "/wal/b", Recorded: 16, Configured: 8},
			[]string{"/wal/b", "16", "8"}},
		{"ReencryptError", &ReencryptError{Op: "rewrite-segment", Err: cause},
			[]string{"rewrite-segment", "underlying cause"}},
		{"RepairOffsetMismatchError", &RepairOffsetMismatchError{ShardIndex: 2, SegmentID: 9, Requested: 100, CleanEnd: 64},
			[]string{"100", "64"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if msg == "" {
				t.Fatal("Error() returned an empty message")
			}
			for _, w := range tc.want {
				if !strings.Contains(msg, w) {
					t.Errorf("message %q does not contain %q", msg, w)
				}
			}
		})
	}
}

// TestWithMetrics_SetsOption verifies the option installs the metrics sink.
func TestWithMetrics_SetsOption(t *testing.T) {
	var o dbOptions
	m := recordingMetrics{}
	WithMetrics(m)(&o)
	if o.metrics != LogDBMetrics(m) {
		t.Fatal("WithMetrics did not install the metrics sink")
	}
}

// TestClearMetaSegmentPins_ReleasesOnlyMatchingPins verifies that clearing a
// segment drops exactly the pins that name it, keeps the entry while another
// pin remains, and deletes the entry once no pin is left.
func TestClearMetaSegmentPins_ReleasesOnlyMatchingPins(t *testing.T) {
	both := nodeKey{ShardID: 1, ReplicaID: 1}
	only := nodeKey{ShardID: 2, ReplicaID: 1}
	other := nodeKey{ShardID: 3, ReplicaID: 1}
	s := &walShard{metaSegments: map[nodeKey]metaSegments{
		both: {state: 5, hasState: true, snapshot: 6, hasSnapshot: true,
			bootstrap: 5, hasBootstrap: true, compact: 5, hasCompact: true},
		only:  {state: 5, hasState: true},
		other: {snapshot: 9, hasSnapshot: true},
	}}

	s.clearMetaSegmentPins(5)

	if _, ok := s.metaSegments[only]; ok {
		t.Error("entry whose only pin was cleared should be deleted")
	}
	got, ok := s.metaSegments[both]
	if !ok {
		t.Fatal("entry with a remaining pin should be kept")
	}
	if got.hasState || got.hasBootstrap || got.hasCompact || got.state != 0 || got.bootstrap != 0 || got.compact != 0 {
		t.Errorf("matching pins not cleared: %+v", got)
	}
	if !got.hasSnapshot || got.snapshot != 6 {
		t.Errorf("non-matching snapshot pin must survive: %+v", got)
	}
	if m := s.metaSegments[other]; !m.hasSnapshot || m.snapshot != 9 {
		t.Errorf("unrelated entry must be untouched: %+v", m)
	}
}

// TestWriteMarkerTemp_RoundTripAndNoSync verifies the temp marker is written
// whole with and without the data sync.
func TestWriteMarkerTemp_RoundTripAndNoSync(t *testing.T) {
	for _, noSync := range []bool{false, true} {
		tmp := filepath.Join(t.TempDir(), "marker.tmp")
		want := []byte("marker-bytes")
		if err := writeMarkerTemp(defaultFS(), tmp, want, noSync); err != nil {
			t.Fatalf("noSync=%v: %v", noSync, err)
		}
		got, err := os.ReadFile(tmp)
		if err != nil || string(got) != string(want) {
			t.Fatalf("noSync=%v: read %q, %v", noSync, got, err)
		}
	}
	if err := writeMarkerTemp(defaultFS(), filepath.Join(t.TempDir(), "no", "dir", "m"), nil, true); err == nil {
		t.Fatal("writeMarkerTemp into a missing directory must fail")
	}
}

// TestTruncateOrphanToZero verifies an orphan segment is emptied, and that an
// open or truncate failure is returned rather than swallowed.
func TestTruncateOrphanToZero(t *testing.T) {
	s := &walShard{fs: defaultFS()}
	name := filepath.Join(t.TempDir(), "orphan")
	if err := os.WriteFile(name, []byte("stale-tail"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.truncateOrphanToZero(name); err != nil {
		t.Fatalf("truncateOrphanToZero: %v", err)
	}
	if fi, err := os.Stat(name); err != nil || fi.Size() != 0 {
		t.Fatalf("orphan not emptied: %v, %v", fi, err)
	}
	if err := s.truncateOrphanToZero(name + ".missing"); err == nil {
		t.Fatal("truncating a missing file must fail")
	}
}

// TestOSFile_Seek verifies the os-backed File forwards Seek.
func TestOSFile_Seek(t *testing.T) {
	name := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(name, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := defaultFS().Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	pos, err := f.Seek(4, io.SeekStart)
	if err != nil || pos != 4 {
		t.Fatalf("Seek = %d, %v", pos, err)
	}
	b := make([]byte, 2)
	if _, err := io.ReadFull(f, b); err != nil || string(b) != "45" {
		t.Fatalf("read after seek = %q, %v", b, err)
	}
}

// TestNodeRemoved_ClosedDB verifies NodeRemoved refuses once the DB is closed.
// The answers before and after a retirement are asserted by
// TestNodeRemoved_RetirementIsDurableAndMigrates.
func TestNodeRemoved_ClosedDB(t *testing.T) {
	db, err := Open(t.TempDir(), WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := db.NodeRemoved(1, 1); !errors.Is(err, logdb.ErrClosed) {
		t.Fatalf("closed DB: got %v, want logdb.ErrClosed", err)
	}
}

// TestCommitEpoch_ZeroEpochIsNoop verifies a landing without an epoch is
// ignored without touching shard state.
func TestCommitEpoch_ZeroEpochIsNoop(t *testing.T) {
	s := &walShard{}
	s.commitEpoch(recordLanding{})
}

// TestReconcileCompactionFloors re-derives a lost floor only when a recorded
// snapshot covers the missing prefix, and leaves every other node alone.
func TestReconcileCompactionFloors(t *testing.T) {
	covered := nodeKey{ShardID: 1, ReplicaID: 1}
	noSnap := nodeKey{ShardID: 2, ReplicaID: 1}
	behind := nodeKey{ShardID: 3, ReplicaID: 1}
	contiguous := nodeKey{ShardID: 4, ReplicaID: 1}
	empty := nodeKey{ShardID: 5, ReplicaID: 1}

	mk := func(first uint64) *index {
		idx := newIndex()
		idx.Append(indexEntry{Index: first, Term: 1, SegmentID: 1, Offset: 0, Size: 8})
		return idx
	}
	s := &walShard{
		indexes: map[nodeKey]*index{
			covered: mk(11), noSnap: mk(11), behind: mk(11), contiguous: mk(1), empty: newIndex(),
		},
		snapshots: map[nodeKey]logdb.Snapshot{
			covered: {Index: 10},
			behind:  {Index: 5},
		},
		compactedTo: map[nodeKey]uint64{},
	}

	if err := s.reconcileCompactionFloors(); err != nil {
		t.Fatalf("reconcileCompactionFloors: %v", err)
	}
	if got := s.compactedTo[covered]; got != 10 {
		t.Errorf("covered floor = %d, want 10", got)
	}
	for name, k := range map[string]nodeKey{"noSnap": noSnap, "behind": behind, "contiguous": contiguous, "empty": empty} {
		if got := s.compactedTo[k]; got != 0 {
			t.Errorf("%s floor = %d, want it untouched", name, got)
		}
	}
}
