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
	"bytes"
	"testing"
)

// reopenReadOnly opens an existing segment file read-only exactly as
// replaySegment does (no O_TRUNC) and reads all records back.
func reopenReadOnly(t *testing.T, dir string, id uint64, blockSize int) [][]byte {
	t.Helper()
	f, err := defaultFS().Open(dir + "/" + segmentFilename(id))
	if err != nil {
		t.Fatalf("open %d: %v", id, err)
	}
	defer f.Close()
	rs := &segment{f: f, id: id, dir: dir, blockSize: blockSize, blockBuf: make([]byte, blockSize)}
	recs, err := rs.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return recs
}

// TestSegment_MidBlockSyncRecovery is the regression test for C1: a mid-block
// Sync must not move fileOffset off the block grid, so every record written
// across many interleaved syncs is recoverable. Before the fix this lost
// roughly half the records.
func TestSegment_MidBlockSyncRecovery(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 32 * 1024
	seg, err := createSegment(defaultFS(), dir, 1, blockSize, 64*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}

	var want [][]byte
	write := func(p []byte) {
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
		cp := make([]byte, len(p))
		copy(cp, p)
		want = append(want, cp)
	}

	// Small record then a mid-block sync (the trigger for the old bug).
	write(bytes.Repeat([]byte{0xAB}, 5000))
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// Many records straddling the reader's block grid, with periodic syncs.
	for i := 0; i < 60; i++ {
		write(bytes.Repeat([]byte{byte(i)}, 900))
		if i%7 == 0 {
			if err := seg.Sync(); err != nil {
				t.Fatalf("Sync loop: %v", err)
			}
		}
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("final Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := reopenReadOnly(t, dir, 1, blockSize)
	if len(got) != len(want) {
		t.Fatalf("recovery loss: wrote %d, recovered %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d mismatch: len want=%d got=%d", i, len(want[i]), len(got[i]))
		}
	}
}

// TestSegment_MidBlockSyncRecoveryWithoutClose verifies recovery from a
// crashed (never closed) segment whose final block is partial: the
// preallocated zero tail must terminate the scan cleanly with no loss.
func TestSegment_MidBlockSyncRecoveryWithoutClose(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4 * 1024
	seg, err := createSegment(defaultFS(), dir, 2, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	var want [][]byte
	for i := 0; i < 200; i++ {
		p := bytes.Repeat([]byte{byte(i % 251)}, 100+i)
		if _, err := seg.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
		cp := make([]byte, len(p))
		copy(cp, p)
		want = append(want, cp)
		if i%5 == 0 {
			if err := seg.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
		}
	}
	if err := seg.Sync(); err != nil { // durable, but no Close (simulates crash)
		t.Fatalf("Sync: %v", err)
	}

	got := reopenReadOnly(t, dir, 2, blockSize)
	if len(got) != len(want) {
		t.Fatalf("recovery loss after crash: wrote %d, recovered %d", len(want), len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d mismatch", i)
		}
	}
}

// TestSegment_StopsAtFirstCorruption is the regression test for C2: recovery
// must return the clean prefix and stop at the first corrupt record rather
// than skipping ahead and recovering a non-prefix.
func TestSegment_StopsAtFirstCorruption(t *testing.T) {
	dir := t.TempDir()
	const blockSize = 4 * 1024
	seg, err := createSegment(defaultFS(), dir, 3, blockSize, 8*1024*1024)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	// Write enough records to span several blocks.
	const n = 120
	for i := 0; i < n; i++ {
		if _, err := seg.Write(bytes.Repeat([]byte{byte(i)}, 200)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clean := reopenReadOnly(t, dir, 3, blockSize)
	if len(clean) != n {
		t.Fatalf("baseline: wrote %d, recovered %d", n, len(clean))
	}

	// Corrupt a byte in the middle of the file (inside block 2's records).
	rw, err := defaultFS().OpenFile(dir+"/"+segmentFilename(3), 2 /*O_RDWR*/, 0600)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	// Flip a byte well past the first block so a clean prefix exists.
	corruptAt := int64(blockSize + 512)
	bad := []byte{0xFF}
	if _, err := rw.WriteAt(bad, corruptAt); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}
	_ = rw.Sync()
	_ = rw.Close()

	got := reopenReadOnly(t, dir, 3, blockSize)
	// Recovery must stop at the corruption: strictly fewer than the full set,
	// and a non-empty clean prefix that matches the originals.
	if len(got) >= n {
		t.Fatalf("expected recovery to stop early, got all %d records", len(got))
	}
	if len(got) == 0 {
		t.Fatalf("expected a non-empty clean prefix before the corruption")
	}
	for i := range got {
		if !bytes.Equal(got[i], clean[i]) {
			t.Fatalf("prefix record %d diverged from original; recovered a non-prefix", i)
		}
	}
}
