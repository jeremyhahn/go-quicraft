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

package proto

import (
	"bytes"
	"errors"
	"testing"
)

func TestSnapshotFileMarshalUnmarshalRoundTrip(t *testing.T) {
	sf := SnapshotFile{
		Filepath: "/var/lib/quicraft/external.dat",
		FileSize: 1024 * 1024,
		FileID:   7,
		Metadata: []byte("app-specific-metadata"),
	}
	buf := make([]byte, sf.Size())
	n, err := sf.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != sf.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, sf.Size())
	}

	var sf2 SnapshotFile
	consumed, err := sf2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertSnapshotFileEqual(t, &sf, &sf2)
}

func TestSnapshotFileMarshalUnmarshalEmpty(t *testing.T) {
	sf := SnapshotFile{}
	buf := make([]byte, sf.Size())
	sf.MarshalTo(buf)
	var sf2 SnapshotFile
	_, err := sf2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if sf2.Filepath != "" || sf2.FileSize != 0 || sf2.FileID != 0 || sf2.Metadata != nil {
		t.Fatal("Empty SnapshotFile round-trip produced non-zero values")
	}
}

func TestSnapshotFileBufferTooSmallMarshal(t *testing.T) {
	sf := SnapshotFile{Filepath: "x"}
	buf := make([]byte, sf.Size()-1)
	_, err := sf.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestSnapshotFileBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, 2)
	var sf SnapshotFile
	_, err := sf.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestSnapshotFileReset(t *testing.T) {
	sf := SnapshotFile{
		Filepath: "/path",
		FileSize: 100,
		FileID:   1,
		Metadata: []byte("meta"),
	}
	sf.Reset()
	if sf.Filepath != "" || sf.FileSize != 0 || sf.FileID != 0 || sf.Metadata != nil {
		t.Fatal("Reset did not clear all fields")
	}
}

func assertSnapshotFileEqual(t *testing.T, want, got *SnapshotFile) {
	t.Helper()
	if got.Filepath != want.Filepath {
		t.Fatalf("Filepath: got %q, want %q", got.Filepath, want.Filepath)
	}
	if got.FileSize != want.FileSize {
		t.Fatalf("FileSize: got %d, want %d", got.FileSize, want.FileSize)
	}
	if got.FileID != want.FileID {
		t.Fatalf("FileID: got %d, want %d", got.FileID, want.FileID)
	}
	if !bytes.Equal(got.Metadata, want.Metadata) {
		t.Fatalf("Metadata: got %v, want %v", got.Metadata, want.Metadata)
	}
}
