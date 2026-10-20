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
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// floorTestDB opens a DB whose segments rotate every few entries, so a
// compaction unlinks whole segments.
func floorTestDB(t *testing.T, dir string, opts ...Option) *DB {
	t.Helper()
	db, err := Open(dir, append([]Option{
		WithBlockSize(1024), WithMaxFileSize(4096), WithNoSync(true),
	}, opts...)...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// countSegments returns the number of segment files in dir.
func countSegments(t *testing.T, dir string) int {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	n := 0
	for _, de := range des {
		if _, ok := parseSegmentFilename(de.Name()); ok {
			n++
		}
	}
	return n
}

// saveFloorTestEntries writes entries [first, last] one record each.
func saveFloorTestEntries(t *testing.T, db *DB, shardID, replicaID, first, last uint64) {
	t.Helper()
	for i := first; i <= last; i++ {
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{
			{Index: i, Term: 1, Type: 1, Cmd: make([]byte, 200)},
		}); err != nil {
			t.Fatalf("SaveEntries %d: %v", i, err)
		}
	}
}

// assertFloorReads checks both directions of the floor contract: every index
// at or below floor reads ErrCompacted, and every index above it up to last is
// served.
func assertFloorReads(t *testing.T, db *DB, shardID, replicaID, floor, last uint64) {
	t.Helper()
	for low := uint64(1); low <= floor; low++ {
		_, _, err := db.IterateEntries(nil, 0, shardID, replicaID, low, last+1, 0)
		if !errors.Is(err, logdb.ErrCompacted) {
			t.Fatalf("read from %d (floor %d) = %v, want ErrCompacted", low, floor, err)
		}
	}
	for low := floor + 1; low <= last; low++ {
		got, _, err := db.IterateEntries(nil, 0, shardID, replicaID, low, last+1, 0)
		if err != nil {
			t.Fatalf("read from %d (floor %d) = %v, want the entries served", low, floor, err)
		}
		if uint64(len(got)) != last-low+1 || got[0].Index != low {
			t.Fatalf("read from %d returned %d entries starting at %d, want %d from %d",
				low, len(got), got[0].Index, last-low+1, low)
		}
	}
}

// TestRecovery_TornFloorMarkerWithoutSnapshotReadsCompacted is the lost-floor
// fault: a shard is compacted, the compacted segments are unlinked, the block
// holding the compaction marker is torn, and no snapshot covers the missing
// prefix. Reads below the real floor must report ErrCompacted, which routes
// raft to InstallSnapshot, never ErrLogGapDetected.
func TestRecovery_TornFloorMarkerWithoutSnapshotReadsCompacted(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)
	const floor, last = uint64(36), uint64(48)

	db := floorTestDB(t, dir)
	saveFloorTestEntries(t, db, shardID, replicaID, 1, last)
	shard := db.getShard(shardID)
	before := countSegments(t, shard.dir)

	// The marker's position, so the block holding it can be torn below.
	shard.mu.Lock()
	markerSeg := shard.activeSegmentID
	markerOff := shard.activeSegment.fileOffset + int64(shard.activeSegment.blockOffset)
	shard.mu.Unlock()

	if err := db.RemoveEntriesTo(shardID, replicaID, floor); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := db.Compact(shardID, replicaID); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if after := countSegments(t, shard.dir); after >= before {
		t.Fatalf("Compact unlinked nothing (%d segments before, %d after)", before, after)
	}
	shardDir := shard.dir
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tearBlockFrom(t, filepath.Join(shardDir, segmentFilename(markerSeg)), markerOff, 1024)

	db2 := floorTestDB(t, dir)
	defer func() { _ = db2.Close() }()
	got, err := db2.CompactedTo(shardID, replicaID)
	if err != nil {
		t.Fatalf("CompactedTo: %v", err)
	}
	if got != floor {
		t.Fatalf("recovered floor = %d, want %d", got, floor)
	}
	assertFloorReads(t, db2, shardID, replicaID, floor, last)
}

// tearBlockFrom zeroes a segment from off to the end of its block and cuts the
// file there: the block holding the record at off is torn and nothing written
// after it survives.
func tearBlockFrom(t *testing.T, path string, off int64, blockSize int64) {
	t.Helper()
	end := (off/blockSize + 1) * blockSize
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(make([]byte, end-off), off); err != nil {
		t.Fatalf("tear block: %v", err)
	}
	if err := f.Truncate(end); err != nil {
		t.Fatalf("truncate segment: %v", err)
	}
}

// BenchmarkDBAppendEntries measures the single-entry append path.
// renameFailFS fails every Rename onto the COMPACTED file: a crash at the
// floor publication, before its rename lands.
type renameFailFS struct {
	FS
}

func (f *renameFailFS) Rename(oldpath, newpath string) error {
	if filepath.Base(newpath) == floorsFilename {
		return errors.New("simulated crash before the floor file rename")
	}
	return f.FS.Rename(oldpath, newpath)
}

// floorFileFloors reads the COMPACTED file of dir, failing the test when it
// does not parse.
func floorFileFloors(t *testing.T, dir string) map[nodeKey]uint64 {
	t.Helper()
	floors, err := loadCompactionFloors(defaultFS(), dir)
	if err != nil {
		t.Fatalf("loadCompactionFloors: %v", err)
	}
	return floors
}

// TestCompactionFloors_CrashAfterPublishBeforeUnlink is the first crash
// order: the floor file is durable, the unlink never happens. The segments
// still hold every entry, and the marker's block is torn as well, so the floor
// file is the only surviving copy of the floor. Recovery must restore the
// floor and stop serving the entries it covers, though they are on disk.
func TestCompactionFloors_CrashAfterPublishBeforeUnlink(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)
	const floor, last = uint64(36), uint64(48)
	ffs := &faultFS{FS: defaultFS()}

	db := floorTestDB(t, dir, WithFS(ffs))
	saveFloorTestEntries(t, db, shardID, replicaID, 1, last)
	shard := db.getShard(shardID)
	before := countSegments(t, shard.dir)
	shard.mu.Lock()
	markerSeg := shard.activeSegmentID
	markerOff := shard.activeSegment.fileOffset + int64(shard.activeSegment.blockOffset)
	shard.mu.Unlock()
	if err := db.RemoveEntriesTo(shardID, replicaID, floor); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	ffs.mu.Lock()
	ffs.failAll, ffs.failRemove = true, errors.New("simulated crash before the unlink")
	ffs.mu.Unlock()
	if err := db.Compact(shardID, replicaID); err == nil {
		t.Fatal("Compact succeeded with every unlink failing")
	}
	ffs.mu.Lock()
	ffs.failAll, ffs.failRemove = false, nil
	ffs.mu.Unlock()

	shardDir := shard.dir
	if got := floorFileFloors(t, shardDir)[nodeKey{shardID, replicaID}]; got != floor {
		t.Fatalf("floor file holds %d, want %d published before the unlink", got, floor)
	}
	if after := countSegments(t, shardDir); after != before {
		t.Fatalf("segments %d -> %d: an unlink happened", before, after)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	tearBlockFrom(t, filepath.Join(shardDir, segmentFilename(markerSeg)), markerOff, 1024)

	db2 := floorTestDB(t, dir)
	defer func() { _ = db2.Close() }()
	if got, err := db2.CompactedTo(shardID, replicaID); err != nil || got != floor {
		t.Fatalf("recovered floor = %d, %v; want %d", got, err, floor)
	}
	first, count, err := db2.EntryRange(shardID, replicaID)
	if err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	if first != floor+1 || count != last-floor {
		t.Fatalf("EntryRange = (%d, %d), want (%d, %d): entries the floor covers are still indexed",
			first, count, floor+1, last-floor)
	}
	assertFloorReads(t, db2, shardID, replicaID, floor, last)
}

// TestCompactionFloors_CrashAtPublishUnlinksNothing is the other crash
// order: the process dies at the floor publication, before its rename. No
// segment may be unlinked by then, so recovery finds every entry and the
// marker, and the floor is exactly the marker's.
func TestCompactionFloors_CrashAtPublishUnlinksNothing(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)
	const floor, last = uint64(36), uint64(48)

	db := floorTestDB(t, dir, WithFS(&renameFailFS{FS: defaultFS()}))
	saveFloorTestEntries(t, db, shardID, replicaID, 1, last)
	shard := db.getShard(shardID)
	before := countSegments(t, shard.dir)
	if err := db.RemoveEntriesTo(shardID, replicaID, floor); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := db.Compact(shardID, replicaID); err == nil {
		t.Fatal("Compact succeeded although the floor file could not be published")
	}
	shardDir := shard.dir
	if after := countSegments(t, shardDir); after != before {
		t.Fatalf("segments %d -> %d: a segment was unlinked before its floor was durable",
			before, after)
	}
	if _, err := os.Stat(filepath.Join(shardDir, floorsFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("floor file present after a failed publication: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2 := floorTestDB(t, dir)
	defer func() { _ = db2.Close() }()
	if got, err := db2.CompactedTo(shardID, replicaID); err != nil || got != floor {
		t.Fatalf("recovered floor = %d, %v; want %d", got, err, floor)
	}
	assertFloorReads(t, db2, shardID, replicaID, floor, last)
}

// TestCompactionFloors_RecoveryTakesTheHigherFloor pins the max rule in the
// direction where the file is the stale copy: a floor raised after the last
// publication survives as its marker, and the lower published floor must not
// pull it down.
func TestCompactionFloors_RecoveryTakesTheHigherFloor(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)
	const published, floor, last = uint64(20), uint64(36), uint64(48)

	db := floorTestDB(t, dir)
	saveFloorTestEntries(t, db, shardID, replicaID, 1, last)
	if err := db.RemoveEntriesTo(shardID, replicaID, published); err != nil {
		t.Fatalf("RemoveEntriesTo %d: %v", published, err)
	}
	if err := db.Compact(shardID, replicaID); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := db.RemoveEntriesTo(shardID, replicaID, floor); err != nil {
		t.Fatalf("RemoveEntriesTo %d: %v", floor, err)
	}
	shardDir := db.getShard(shardID).dir
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := floorFileFloors(t, shardDir)[nodeKey{shardID, replicaID}]; got != published {
		t.Fatalf("floor file holds %d, want the stale %d", got, published)
	}

	db2 := floorTestDB(t, dir)
	defer func() { _ = db2.Close() }()
	if got, err := db2.CompactedTo(shardID, replicaID); err != nil || got != floor {
		t.Fatalf("recovered floor = %d, %v; want %d", got, err, floor)
	}
	assertFloorReads(t, db2, shardID, replicaID, floor, last)
}

// TestCompactionFloors_UncoveredPrefixIsNotReportedCompacted pins the other
// safety direction: a log that starts high with no floor and no snapshot
// covering its prefix is not compacted, and nothing below its first entry may
// be reported as ErrCompacted.
func TestCompactionFloors_UncoveredPrefixIsNotReportedCompacted(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)

	db := floorTestDB(t, dir)
	saveFloorTestEntries(t, db, shardID, replicaID, 10, 20)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2 := floorTestDB(t, dir)
	defer func() { _ = db2.Close() }()
	if got, err := db2.CompactedTo(shardID, replicaID); err != nil || got != 0 {
		t.Fatalf("floor = %d, %v; want 0: nothing was compacted", got, err)
	}
	for low := uint64(1); low < 10; low++ {
		_, _, err := db2.IterateEntries(nil, 0, shardID, replicaID, low, 21, 0)
		if errors.Is(err, logdb.ErrCompacted) {
			t.Fatalf("read from %d reports ErrCompacted with no floor covering it", low)
		}
	}
}

// TestCompactionFloors_MalformedFileRefusesOpen fails closed: a floor file
// that does not parse leaves the floors it held unknowable.
func TestCompactionFloors_MalformedFileRefusesOpen(t *testing.T) {
	dir := t.TempDir()
	const shardID, replicaID = uint64(1), uint64(1)

	db := floorTestDB(t, dir)
	saveFloorTestEntries(t, db, shardID, replicaID, 1, 48)
	if err := db.RemoveEntriesTo(shardID, replicaID, 36); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := db.Compact(shardID, replicaID); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	path := filepath.Join(db.getShard(shardID).dir, floorsFilename)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	buf[len(buf)-5] ^= 0xff // a floor byte: the checksum no longer matches
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	db2, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(4096), WithNoSync(true))
	if err == nil {
		_ = db2.Close()
		t.Fatal("Open succeeded over a malformed floor file")
	}
	if !errors.Is(err, ErrCompactionFloorsMalformed) {
		t.Fatalf("Open = %v, want ErrCompactionFloorsMalformed", err)
	}
}

// TestCompactionFloors_Codec round-trips the file image and rejects every
// malformed shape.
func TestCompactionFloors_Codec(t *testing.T) {
	floors := map[nodeKey]uint64{{2, 1}: 7, {1, 3}: 1 << 40, {1, 1}: 0}
	buf := marshalCompactionFloors(floors)
	got, err := parseCompactionFloors(buf)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != len(floors) {
		t.Fatalf("parsed %d floors, want %d", len(got), len(floors))
	}
	for k, v := range floors {
		if got[k] != v {
			t.Fatalf("floor %v = %d, want %d", k, got[k], v)
		}
	}
	if empty, err := parseCompactionFloors(marshalCompactionFloors(nil)); err != nil || len(empty) != 0 {
		t.Fatalf("empty image = %v, %v", empty, err)
	}
	bad := map[string][]byte{
		"short":     buf[:floorsHeaderSize],
		"magic":     append([]byte("XXXXXXXX"), buf[8:]...),
		"truncated": buf[:len(buf)-1],
		"count":     append(append([]byte{}, buf[:8]...), append([]byte{9, 0, 0, 0, 0, 0, 0, 0}, buf[16:]...)...),
	}
	for name, b := range bad {
		if _, err := parseCompactionFloors(b); !errors.Is(err, ErrCompactionFloorsMalformed) {
			t.Errorf("%s: parse = %v, want ErrCompactionFloorsMalformed", name, err)
		}
	}
}

func BenchmarkDBAppendEntries(b *testing.B) {
	b.ReportAllocs()
	db := openBenchDB(b)
	entry := []logdb.Entry{{Term: 1, Type: 1, Cmd: make([]byte, 128)}}
	idx := uint64(1)
	for b.Loop() {
		entry[0].Index = idx
		idx++
		if err := db.SaveEntries(1, 1, entry); err != nil {
			b.Fatalf("SaveEntries: %v", err)
		}
	}
}

// BenchmarkDBCompact measures a compaction cycle: append a batch that rotates
// segments, raise the floor over it, and reclaim the dead segments.
func BenchmarkDBCompact(b *testing.B) {
	b.ReportAllocs()
	db, err := Open(b.TempDir(), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	entries := makeBenchEntries(1, 64, 1, 1024)
	update := []logdb.Update{{ShardID: 1, ReplicaID: 1, Entries: entries}}
	idx := uint64(1)
	for b.Loop() {
		for i := range entries {
			entries[i].Index = idx + uint64(i)
		}
		idx += uint64(len(entries))
		if err := db.SaveState(update); err != nil {
			b.Fatalf("SaveState: %v", err)
		}
		if err := db.RemoveEntriesTo(1, 1, idx-1); err != nil {
			b.Fatalf("RemoveEntriesTo: %v", err)
		}
		if err := db.Compact(1, 1); err != nil {
			b.Fatalf("Compact: %v", err)
		}
	}
}
