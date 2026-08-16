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
	"os"
	"path/filepath"
	"testing"
)

// writeSnapshotDir builds a snapshot directory with data and a meta record
// carrying integrity fields computed over that data.
func writeSnapshotDir(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, snapshotDataFile)
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	sum, size, err := hashSnapshotData(dataPath)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	meta := MarshalSnapshotMeta(SnapshotMeta{
		Index: 10, Term: 2, ShardID: 1, ReplicaID: 1,
		DataLen: size, DataHash: sum,
	})
	if err := os.WriteFile(filepath.Join(dir, snapshotMetadataFile), meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	return dir
}

// TestVerifySnapshotArtifact_DetectsSilentCorruption is the check a
// no-barrier deployment has instead of AEAD.
//
// A single flipped byte inside a serialized value leaves the file LENGTH
// unchanged, so no structural check trips: the corrupt value parses, recovers
// into the state machine, and is served to clients. The replica then holds a
// different value for that key than its peers at the same log index, with
// every Raft invariant intact and no error raised anywhere.
func TestVerifySnapshotArtifact_DetectsSilentCorruption(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog, twice over")
	dir := writeSnapshotDir(t, data)
	dataPath := filepath.Join(dir, snapshotDataFile)

	if err := VerifySnapshotArtifact(dir, dataPath); err != nil {
		t.Fatalf("an intact artifact was rejected: %v", err)
	}

	// Flip ONE bit, preserving the length.
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)/2] ^= 0x01
	if err := os.WriteFile(dataPath, corrupt, 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	err := VerifySnapshotArtifact(dir, dataPath)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("VerifySnapshotArtifact on a single flipped bit returned %v, "+
			"want ErrSnapshotCorrupt. Without this the corrupt value is "+
			"recovered into the state machine and served, diverging this "+
			"replica from its peers permanently", err)
	}
}

// TestVerifySnapshotArtifact_DetectsTruncation covers the other half: a
// trailing frame lost to a torn write reads as a clean EOF, because no frame
// count is recorded anywhere.
func TestVerifySnapshotArtifact_DetectsTruncation(t *testing.T) {
	data := []byte("frame-one|frame-two|frame-three")
	dir := writeSnapshotDir(t, data)
	dataPath := filepath.Join(dir, snapshotDataFile)

	if err := os.WriteFile(dataPath, data[:len(data)-10], 0o600); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := VerifySnapshotArtifact(dir, dataPath); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("a truncated artifact returned %v, want ErrSnapshotCorrupt", err)
	}
}

// TestVerifySnapshotArtifact_AcceptsRecordsWithoutIntegrityFields pins the
// upgrade path: an artifact written by a binary that predates these fields has
// a VALID meta record without them, and must still recover. Refusing it would
// make an upgrade unable to read the snapshots it inherited.
func TestVerifySnapshotArtifact_AcceptsRecordsWithoutIntegrityFields(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, snapshotDataFile)
	if err := os.WriteFile(dataPath, []byte("legacy artifact"), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	// A record with the integrity fields left zero — DataHashRecorded false.
	meta := MarshalSnapshotMeta(SnapshotMeta{Index: 10, Term: 2, ShardID: 1, ReplicaID: 1})
	// Truncate the body to the pre-integrity length so the parser reports the
	// fields as absent rather than zero-but-present.
	meta = meta[:snapshotMetaHeaderSize+metaBodyCompressionOff+8]
	binary.LittleEndian.PutUint32(meta[snapshotMetaBodyLenOff:], uint32(metaBodyCompressionOff+8))
	if err := os.WriteFile(filepath.Join(dir, snapshotMetadataFile), meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	if err := VerifySnapshotArtifact(dir, dataPath); err != nil {
		t.Fatalf("an artifact predating the integrity fields was refused: %v", err)
	}
}
