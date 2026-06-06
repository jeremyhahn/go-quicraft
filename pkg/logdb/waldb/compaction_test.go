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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestGarbageCollectRemovesObsoleteSegments verifies that GarbageCollect
// deletes segment files that are not in the live set.
func TestGarbageCollectRemovesObsoleteSegments(t *testing.T) {
	dir := t.TempDir()

	// Create segment files.
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		seg, err := createSegment(defaultFS(), dir, id, 1024, 1024*1024)
		if err != nil {
			t.Fatalf("createSegment(%d): %v", id, err)
		}
		seg.Close()
	}

	comp := newCompactor(defaultFS(), dir, 1024)
	liveSegments := map[uint64]bool{3: true, 4: true, 5: true}

	deleted, err := comp.GarbageCollect(liveSegments, 5, 5)
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}

	// Segments 1 and 2 should be deleted (not in live set, < maxLiveID).
	if deleted != 2 {
		t.Fatalf("GarbageCollect: deleted %d, want 2", deleted)
	}

	// Verify files on disk.
	for _, id := range []uint64{1, 2} {
		path := filepath.Join(dir, segmentFilename(id))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("segment %d should be deleted, but exists", id)
		}
	}
	for _, id := range []uint64{3, 4, 5} {
		path := filepath.Join(dir, segmentFilename(id))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("segment %d should exist: %v", id, err)
		}
	}
}

// TestGarbageCollectNeverDeletesActiveSegment verifies that the active segment
// (>= maxLiveID) is never deleted even if not in the live set.
func TestGarbageCollectNeverDeletesActiveSegment(t *testing.T) {
	dir := t.TempDir()

	// Create segments 1-3.
	for _, id := range []uint64{1, 2, 3} {
		seg, err := createSegment(defaultFS(), dir, id, 1024, 1024*1024)
		if err != nil {
			t.Fatalf("createSegment(%d): %v", id, err)
		}
		seg.Close()
	}

	comp := newCompactor(defaultFS(), dir, 1024)
	// Empty live set but maxLiveID = 3 protects segment 3.
	liveSegments := map[uint64]bool{}

	deleted, err := comp.GarbageCollect(liveSegments, 3, 3)
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}

	// Segments 1 and 2 deleted; segment 3 protected.
	if deleted != 2 {
		t.Fatalf("GarbageCollect: deleted %d, want 2", deleted)
	}

	path := filepath.Join(dir, segmentFilename(3))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("active segment 3 should not be deleted: %v", err)
	}
}

// TestGarbageCollectNoSegmentsToDelete verifies no files are deleted when
// all segments are live.
func TestGarbageCollectNoSegmentsToDelete(t *testing.T) {
	dir := t.TempDir()

	for _, id := range []uint64{1, 2, 3} {
		seg, err := createSegment(defaultFS(), dir, id, 1024, 1024*1024)
		if err != nil {
			t.Fatalf("createSegment(%d): %v", id, err)
		}
		seg.Close()
	}

	comp := newCompactor(defaultFS(), dir, 1024)
	liveSegments := map[uint64]bool{1: true, 2: true, 3: true}

	deleted, err := comp.GarbageCollect(liveSegments, 4, 4)
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("GarbageCollect: deleted %d, want 0", deleted)
	}
}

// TestGarbageCollectEmptyDirectory verifies no error on empty directory.
func TestGarbageCollectEmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	comp := newCompactor(defaultFS(), dir, 1024)
	deleted, err := comp.GarbageCollect(map[uint64]bool{}, 1, 1)
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("GarbageCollect: deleted %d, want 0", deleted)
	}
}

// TestGarbageCollectInvalidDirectory verifies error on non-existent directory.
func TestGarbageCollectInvalidDirectory(t *testing.T) {
	comp := newCompactor(defaultFS(), "/nonexistent/path", 1024)
	_, err := comp.GarbageCollect(map[uint64]bool{}, 1, 1)
	if err == nil {
		t.Fatal("GarbageCollect: expected error for invalid directory")
	}

	var readDirErr *CompactionReadDirError
	if !errors.As(err, &readDirErr) {
		t.Fatalf("GarbageCollect: expected CompactionReadDirError, got %T", err)
	}
}

// TestGarbageCollectIgnoresNonSegmentFiles verifies that non-.log files
// in the directory are not touched.
func TestGarbageCollectIgnoresNonSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a segment and a non-segment file.
	seg, err := createSegment(defaultFS(), dir, 1, 1024, 1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	seg.Close()

	otherFile := filepath.Join(dir, "MANIFEST")
	if err := os.WriteFile(otherFile, []byte("manifest"), 0600); err != nil {
		t.Fatalf("write MANIFEST: %v", err)
	}

	comp := newCompactor(defaultFS(), dir, 1024)
	deleted, err := comp.GarbageCollect(map[uint64]bool{}, 2, 2)
	if err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("GarbageCollect: deleted %d, want 1", deleted)
	}

	// MANIFEST should still exist.
	if _, err := os.Stat(otherFile); err != nil {
		t.Fatalf("MANIFEST should not be deleted: %v", err)
	}
}

// TestCompactPreservesBootstrapSegment verifies that Compact does not delete a
// segment whose entries have all been compacted away but which still holds the
// sole durable copy of a node's Bootstrap record. The bootstrap must survive a
// Compact followed by a reopen/recovery.
func TestCompactPreservesBootstrapSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write the bootstrap record FIRST so it lands in the early segment.
	bs := logdb.Bootstrap{
		Addresses: map[uint64]string{1: "node1:9000", 2: "node2:9000"},
		Join:      false,
		Type:      3,
	}
	if err := db.SaveBootstrap(1, 1, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	// Roll the early entries into later segments by forcing several rotations.
	// The bootstrap record stays in the early segment while the live entries
	// move on, leaving that segment holding only soon-to-be-compacted entries
	// plus the bootstrap.
	big := make([]byte, 16*1024)
	for i := uint64(1); i <= 30; i++ {
		e := logdb.Entry{Index: i, Term: 1, Cmd: big}
		if err := db.SaveEntries(1, 1, []logdb.Entry{e}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	// Compact the early entries away, then run segment GC. Only the bootstrap
	// pin should keep the early segment (and its bootstrap record) alive.
	if err := db.RemoveEntriesTo(1, 1, 10); err != nil {
		t.Fatalf("RemoveEntriesTo(10): %v", err)
	}
	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the bootstrap survived recovery.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer db2.Close()

	got, ok, err := db2.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap after Compact+reopen: %v", err)
	}
	if !ok {
		t.Fatal("GetBootstrap: bootstrap record was lost after Compact+reopen")
	}
	if got.Type != 3 || got.Join {
		t.Fatalf("GetBootstrap: got Type=%d Join=%v, want Type=3 Join=false", got.Type, got.Join)
	}
	if len(got.Addresses) != 2 || got.Addresses[1] != "node1:9000" || got.Addresses[2] != "node2:9000" {
		t.Fatalf("GetBootstrap: Addresses=%v, want 2 entries node1:9000/node2:9000", got.Addresses)
	}
}

// TestRemoveNodeDataPreservesSharedSegment verifies that RemoveNodeData on one
// node does not delete a segment that holds the surviving node's State and
// Snapshot records. Two replicas of the same shard share a WAL shard (and thus
// its segment files): removing one replica must keep the other replica's meta
// segments pinned so its State/Snapshot still recover after a reopen.
func TestRemoveNodeDataPreservesSharedSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Replica 2's State and Snapshot land in the early segment FIRST so both
	// share the early segment files with replica 1's about-to-be-compacted
	// entries.
	if err := db.SaveNodeState(1, 2, logdb.State{Term: 9, Vote: 2, Commit: 11}); err != nil {
		t.Fatalf("SaveNodeState(replica 2): %v", err)
	}
	survivorSnap := logdb.Snapshot{
		Index:       5,
		Term:        3,
		Filepath:    "/data/snapshot-5",
		FileSize:    2048,
		OnDiskIndex: 5,
		Membership: logdb.Membership{
			ConfigChangeID: 13,
			Addresses:      map[uint64]string{1: "n1:9000", 2: "n2:9000"},
			Observers:      map[uint64]string{},
			Witnesses:      map[uint64]string{},
			Removed:        map[uint64]bool{},
		},
	}
	if err := db.SaveSnapshot(1, 2, survivorSnap); err != nil {
		t.Fatalf("SaveSnapshot(replica 2): %v", err)
	}

	// Replica 1 writes many entries, rolling new segments. Its entries occupy
	// the early shared segment and later ones; replica 2's meta records remain
	// in the early segment with no live index entry of their own.
	big := make([]byte, 16*1024)
	for i := uint64(1); i <= 30; i++ {
		e := logdb.Entry{Index: i, Term: 1, Cmd: big}
		if err := db.SaveEntries(1, 1, []logdb.Entry{e}); err != nil {
			t.Fatalf("SaveEntries(replica 1, %d): %v", i, err)
		}
	}

	// Remove replica 1. This triggers GC; replica 2's State/Snapshot segment
	// must stay pinned and therefore survive.
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData(replica 1): %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer db2.Close()

	// Replica 1's data must be gone.
	if _, _, err := db2.ReadState(1, 1); !errors.Is(err, logdb.ErrNoState) {
		t.Fatalf("ReadState(replica 1) after remove: got err=%v, want ErrNoState", err)
	}

	// Replica 2's State and Snapshot must survive.
	state, _, err := db2.ReadState(1, 2)
	if err != nil {
		t.Fatalf("ReadState(replica 2) after remove+reopen: %v", err)
	}
	if state.Term != 9 || state.Vote != 2 || state.Commit != 11 {
		t.Fatalf("ReadState(replica 2): got %+v, want Term=9 Vote=2 Commit=11", state)
	}

	gotSnap, err := db2.GetSnapshot(1, 2)
	if err != nil {
		t.Fatalf("GetSnapshot(replica 2) after remove+reopen: %v", err)
	}
	if gotSnap.Index != 5 || gotSnap.Term != 3 {
		t.Fatalf("GetSnapshot(replica 2): got Index=%d Term=%d, want Index=5 Term=3", gotSnap.Index, gotSnap.Term)
	}
	if gotSnap.Membership.ConfigChangeID != 13 || len(gotSnap.Membership.Addresses) != 2 {
		t.Fatalf("GetSnapshot(replica 2) membership=%+v, want ConfigChangeID=13 with 2 addresses", gotSnap.Membership)
	}
}

// TestCompactRespectsCommittedHWM verifies the R6 guard: a Compact must never
// delete a segment that is durably written but not yet reflected in the
// in-memory index or meta pins. SaveState releases shard.mu between its durable
// WAL write (Phase 1) and its in-memory commit (Phase 3); a concurrent Compact
// running in that window would otherwise GC the just-written segment.
//
// The window is reproduced deterministically (no goroutines) by writing a
// record directly through the shard — which is durable and may rotate to a new
// segment — WITHOUT advancing the commit high-water mark, then invoking Compact
// and confirming the new segment and its record survive a reopen.
func TestCompactRespectsCommittedHWM(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Establish several committed segments and a committed HWM via normal
	// SaveState. These entries get fully indexed (Phase 3 ran), so the HWM
	// tracks the highest committed segment.
	big := make([]byte, 16*1024)
	for i := uint64(1); i <= 12; i++ {
		e := logdb.Entry{Index: i, Term: 1, Cmd: big}
		if err := db.SaveEntries(1, 1, []logdb.Entry{e}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	shard := db.getShard(1)

	// Simulate the Phase-1/Phase-3 window: write a state record durably WITHOUT
	// advancing the HWM and WITHOUT updating the in-memory index/pins. The
	// record must sit ALONE in a fresh segment (no live index entry, no pin) so
	// only the HWM guard can protect it, so we rotate before AND after the
	// write.
	shard.mu.Lock()
	hwmBefore := shard.committedSegmentHWM
	// Rotate first so the state record lands in a brand-new, otherwise-empty
	// segment that is unreferenced by any committed entry index.
	if err := shard.rotateSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("rotateSegment(pre): %v", err)
	}
	uncommittedSegID := shard.activeSegmentID
	buf := shard.getMarshalBuf(1 + stateRecordSize)
	buf[0] = payloadTypeState
	binary.LittleEndian.PutUint64(buf[1:], 1)
	binary.LittleEndian.PutUint64(buf[9:], 1)
	binary.LittleEndian.PutUint64(buf[17:], 7)  // Term
	binary.LittleEndian.PutUint64(buf[25:], 1)  // Vote
	binary.LittleEndian.PutUint64(buf[33:], 12) // Commit
	if err := shard.writeRecord(buf[:1+stateRecordSize]); err != nil {
		shard.mu.Unlock()
		t.Fatalf("writeRecord(state): %v", err)
	}
	if err := shard.syncSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("syncSegment: %v", err)
	}
	// Rotate again so the active segment advances past the uncommitted-record
	// segment. The record's segment now has an ID strictly greater than the
	// committed HWM and strictly less than the active segment ID — exactly the
	// case the HWM guard must protect.
	if err := shard.rotateSegment(); err != nil {
		shard.mu.Unlock()
		t.Fatalf("rotateSegment(post): %v", err)
	}
	if uncommittedSegID <= hwmBefore {
		shard.mu.Unlock()
		t.Fatalf("setup: uncommitted segment %d must exceed HWM %d", uncommittedSegID, hwmBefore)
	}
	if uncommittedSegID >= shard.activeSegmentID {
		shard.mu.Unlock()
		t.Fatalf("setup: uncommitted segment %d must be below active %d", uncommittedSegID, shard.activeSegmentID)
	}
	shard.mu.Unlock()

	// Compact must NOT delete the uncommitted segment: it is below the active
	// segment ID and absent from liveSegments, so only the HWM guard protects
	// it.
	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	segPath := filepath.Join(shard.dir, segmentFilename(uncommittedSegID))
	if _, err := os.Stat(segPath); err != nil {
		t.Fatalf("uncommitted segment %d was deleted by Compact (HWM guard failed): %v", uncommittedSegID, err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The durably-written state must recover after reopen, proving the segment
	// (and its record) survived.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer db2.Close()

	state, _, err := db2.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState after Compact+reopen: %v", err)
	}
	if state.Term != 7 || state.Vote != 1 || state.Commit != 12 {
		t.Fatalf("ReadState: got %+v, want Term=7 Vote=1 Commit=12", state)
	}
}

// TestParseSegmentFilename verifies parsing of segment filenames.
func TestParseSegmentFilename(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantID uint64
		wantOK bool
	}{
		{"standard", "000001.log", 1, true},
		{"zero padded", "000042.log", 42, true},
		{"zero", "000000.log", 0, true},
		{"large id", "999999.log", 999999, true},
		{"no padding", "1.log", 1, true},
		{"not a log file", "000001.idx", 0, false},
		{"no extension", "000001", 0, false},
		{"empty name", "", 0, false},
		{"just .log", ".log", 0, false},
		{"non-numeric", "abcdef.log", 0, false},
		{"mixed", "00a001.log", 0, false},
		{"directory", "shard-0", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, ok := parseSegmentFilename(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("parseSegmentFilename(%q): ok=%v, want %v", tt.input, ok, tt.wantOK)
			}
			if ok && id != tt.wantID {
				t.Fatalf("parseSegmentFilename(%q): id=%d, want %d", tt.input, id, tt.wantID)
			}
		})
	}
}

// TestParseSegmentFilenameInvalid verifies various invalid filenames.
func TestParseSegmentFilenameInvalid(t *testing.T) {
	invalidNames := []string{"", "x", "abc", ".log", "test.txt", "000001.idx"}
	for _, name := range invalidNames {
		if _, ok := parseSegmentFilename(name); ok {
			t.Fatalf("parseSegmentFilename(%q): expected false, got true", name)
		}
	}
}

// TestCompactionReadDirError verifies the error type and message.
func TestCompactionReadDirError(t *testing.T) {
	inner := os.ErrNotExist
	e := &CompactionReadDirError{Dir: "/test", Err: inner}

	msg := e.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}

	if e.Unwrap() != inner {
		t.Fatalf("Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// TestCompactionDeleteError verifies the error type and message.
func TestCompactionDeleteError(t *testing.T) {
	inner := os.ErrPermission
	e := &CompactionDeleteError{Path: "/test/000001.log", Err: inner}

	msg := e.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}

	if e.Unwrap() != inner {
		t.Fatalf("Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// TestCompactPreservesSnapshotSegment verifies that Compact does not delete a
// segment whose entries have all been compacted away but which still holds the
// sole durable copy of a node's Snapshot record. The snapshot (and its
// membership) must survive a Compact followed by a reopen/recovery.
func TestCompactPreservesSnapshotSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write a snapshot record FIRST so it lands in the early segment, along
	// with a small batch of entries (1-5). The snapshot carries membership so
	// recovery loss would be observable.
	ss := logdb.Snapshot{
		Index:       5,
		Term:        2,
		Filepath:    "/data/snapshot-5",
		FileSize:    4096,
		OnDiskIndex: 5,
		Membership: logdb.Membership{
			ConfigChangeID: 7,
			Addresses:      map[uint64]string{1: "node1:9000", 2: "node2:9000"},
			Observers:      map[uint64]string{},
			Witnesses:      map[uint64]string{},
			Removed:        map[uint64]bool{},
		},
	}
	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Write large entries to force several segment rotations. The snapshot
	// record stays in segment 0, while the entries spread across later
	// segments. With a 64KiB max file size and 16KiB entries, ~3 entries land
	// per segment, so entries 1-10 occupy roughly segments 0-2.
	big := make([]byte, 16*1024)
	for i := uint64(1); i <= 30; i++ {
		e := logdb.Entry{Index: i, Term: 1, Cmd: big}
		if err := db.SaveEntries(1, 1, []logdb.Entry{e}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	// Compact entries 1-10 away. Segment 0 now has no live index entries; only
	// the snapshot pin should keep it (and therefore the snapshot) alive.
	if err := db.RemoveEntriesTo(1, 1, 10); err != nil {
		t.Fatalf("RemoveEntriesTo(10): %v", err)
	}
	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the snapshot (with membership) survived recovery.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer db2.Close()

	got, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after Compact+reopen: %v", err)
	}
	if got.Index != 5 || got.Term != 2 {
		t.Fatalf("GetSnapshot: got Index=%d Term=%d, want Index=5 Term=2", got.Index, got.Term)
	}
	if got.Membership.ConfigChangeID != 7 {
		t.Fatalf("GetSnapshot membership ConfigChangeID=%d, want 7", got.Membership.ConfigChangeID)
	}
	if len(got.Membership.Addresses) != 2 || got.Membership.Addresses[1] != "node1:9000" {
		t.Fatalf("GetSnapshot membership Addresses=%v, want 2 entries with node1:9000", got.Membership.Addresses)
	}
}

// TestCompactPreservesStateSegment verifies that Compact does not delete a
// segment whose entries have all been compacted away but which still holds the
// sole durable copy of a node's hard State record. The state must survive a
// Compact followed by a reopen/recovery.
func TestCompactPreservesStateSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write the state FIRST so its record lands in segment 0.
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 4, Vote: 1, Commit: 5}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	// Force several rotations so the live entries occupy later segments,
	// leaving segment 0 holding only the (soon compacted) early entries and
	// the state record.
	big := make([]byte, 16*1024)
	for i := uint64(1); i <= 30; i++ {
		e := logdb.Entry{Index: i, Term: 1, Cmd: big}
		if err := db.SaveEntries(1, 1, []logdb.Entry{e}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	if err := db.RemoveEntriesTo(1, 1, 10); err != nil {
		t.Fatalf("RemoveEntriesTo(10): %v", err)
	}
	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	defer db2.Close()

	state, _, err := db2.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState after Compact+reopen: %v", err)
	}
	if state.Term != 4 || state.Vote != 1 || state.Commit != 5 {
		t.Fatalf("ReadState: got %+v, want Term=4 Vote=1 Commit=5", state)
	}
}
