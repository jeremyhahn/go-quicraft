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

	deleted, err := comp.GarbageCollect(liveSegments, 5)
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

	deleted, err := comp.GarbageCollect(liveSegments, 3)
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

	deleted, err := comp.GarbageCollect(liveSegments, 4)
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
	deleted, err := comp.GarbageCollect(map[uint64]bool{}, 1)
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
	_, err := comp.GarbageCollect(map[uint64]bool{}, 1)
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
	deleted, err := comp.GarbageCollect(map[uint64]bool{}, 2)
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
