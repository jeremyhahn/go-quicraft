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

func TestSnapshotChunkMarshalUnmarshalRoundTrip(t *testing.T) {
	sc := SnapshotChunk{
		ShardID:        100,
		ReplicaID:      2,
		From:           1,
		Index:          500,
		Term:           10,
		ChunkID:        0,
		ChunkCount:     5,
		ChunkSize:      1024,
		FileSize:       5120,
		Filepath:       "/snap/100/chunk-0.dat",
		FileChunkID:    0,
		FileChunkCount: 5,
		HasFileInfo:    true,
		FileInfo: SnapshotFile{
			Filepath: "/external/data.db",
			FileSize: 2048,
			FileID:   1,
			Metadata: []byte("db-file"),
		},
		Membership: Membership{
			ConfigChangeID: 42,
			Addresses:      map[uint64]string{1: "node1:5000", 2: "node2:5000"},
			Observers:      map[uint64]string{},
			Witnesses:      map[uint64]string{},
			Removed:        map[uint64]bool{},
		},
		OnDiskIndex:  499,
		DeploymentID: 12345,
		BinVer:       WireVersion,
		Epoch:        5,
		Data:         []byte("chunk-payload-data"),
	}
	buf := make([]byte, sc.Size())
	n, err := sc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != sc.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, sc.Size())
	}

	var sc2 SnapshotChunk
	consumed, err := sc2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertSnapshotChunkEqual(t, &sc, &sc2)
}

func TestSnapshotChunkMarshalUnmarshalNoFileInfo(t *testing.T) {
	sc := SnapshotChunk{
		ShardID:        1,
		ReplicaID:      1,
		From:           1,
		Index:          10,
		Term:           1,
		ChunkID:        0,
		ChunkCount:     1,
		ChunkSize:      100,
		FileSize:       100,
		Filepath:       "snap.dat",
		FileChunkID:    0,
		FileChunkCount: 1,
		HasFileInfo:    false,
		Membership: Membership{
			Addresses: map[uint64]string{1: "a"},
			Observers: map[uint64]string{},
			Witnesses: map[uint64]string{},
			Removed:   map[uint64]bool{},
		},
		OnDiskIndex:  9,
		DeploymentID: 1,
		BinVer:       WireVersion,
		Data:         []byte{0x01, 0x02, 0x03},
	}
	buf := make([]byte, sc.Size())
	sc.MarshalTo(buf)
	var sc2 SnapshotChunk
	_, err := sc2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertSnapshotChunkEqual(t, &sc, &sc2)
}

func TestSnapshotChunkMarshalUnmarshalEmptyData(t *testing.T) {
	sc := SnapshotChunk{
		ShardID:    1,
		ReplicaID:  1,
		From:       1,
		Index:      10,
		Term:       1,
		ChunkCount: 1,
		Filepath:   "s",
		Membership: Membership{
			Addresses: map[uint64]string{},
			Observers: map[uint64]string{},
			Witnesses: map[uint64]string{},
			Removed:   map[uint64]bool{},
		},
		BinVer: WireVersion,
	}
	buf := make([]byte, sc.Size())
	sc.MarshalTo(buf)
	var sc2 SnapshotChunk
	_, err := sc2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if sc2.Data != nil {
		t.Fatalf("Data: got %v, want nil", sc2.Data)
	}
}

func TestSnapshotChunkBufferTooSmallMarshal(t *testing.T) {
	sc := SnapshotChunk{
		Filepath: "x",
		Membership: Membership{
			Addresses: map[uint64]string{},
			Observers: map[uint64]string{},
			Witnesses: map[uint64]string{},
			Removed:   map[uint64]bool{},
		},
	}
	buf := make([]byte, sc.Size()-1)
	_, err := sc.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestSnapshotChunkBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, 32)
	var sc SnapshotChunk
	_, err := sc.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestSnapshotChunkReset(t *testing.T) {
	sc := SnapshotChunk{
		ShardID:        100,
		ReplicaID:      2,
		From:           1,
		Index:          500,
		Term:           10,
		ChunkID:        1,
		ChunkCount:     5,
		ChunkSize:      1024,
		FileSize:       5120,
		Filepath:       "/path",
		FileChunkID:    1,
		FileChunkCount: 5,
		HasFileInfo:    true,
		FileInfo:       SnapshotFile{Filepath: "f"},
		OnDiskIndex:    499,
		DeploymentID:   12345,
		BinVer:         WireVersion,
		Epoch:          9,
		Data:           []byte("data"),
	}
	sc.Reset()
	if sc.ShardID != 0 || sc.ReplicaID != 0 || sc.From != 0 ||
		sc.Index != 0 || sc.Term != 0 || sc.ChunkID != 0 ||
		sc.ChunkCount != 0 || sc.ChunkSize != 0 || sc.FileSize != 0 ||
		sc.Filepath != "" || sc.FileChunkID != 0 || sc.FileChunkCount != 0 ||
		sc.HasFileInfo || sc.OnDiskIndex != 0 || sc.DeploymentID != 0 ||
		sc.BinVer != 0 || sc.Epoch != 0 || sc.Data != nil {
		t.Fatal("Reset did not clear all fields")
	}
}

func assertSnapshotChunkEqual(t *testing.T, want, got *SnapshotChunk) {
	t.Helper()
	if got.ShardID != want.ShardID {
		t.Fatalf("ShardID: got %d, want %d", got.ShardID, want.ShardID)
	}
	if got.ReplicaID != want.ReplicaID {
		t.Fatalf("ReplicaID: got %d, want %d", got.ReplicaID, want.ReplicaID)
	}
	if got.From != want.From {
		t.Fatalf("From: got %d, want %d", got.From, want.From)
	}
	if got.Index != want.Index {
		t.Fatalf("Index: got %d, want %d", got.Index, want.Index)
	}
	if got.Term != want.Term {
		t.Fatalf("Term: got %d, want %d", got.Term, want.Term)
	}
	if got.ChunkID != want.ChunkID {
		t.Fatalf("ChunkID: got %d, want %d", got.ChunkID, want.ChunkID)
	}
	if got.ChunkCount != want.ChunkCount {
		t.Fatalf("ChunkCount: got %d, want %d", got.ChunkCount, want.ChunkCount)
	}
	if got.ChunkSize != want.ChunkSize {
		t.Fatalf("ChunkSize: got %d, want %d", got.ChunkSize, want.ChunkSize)
	}
	if got.FileSize != want.FileSize {
		t.Fatalf("FileSize: got %d, want %d", got.FileSize, want.FileSize)
	}
	if got.Filepath != want.Filepath {
		t.Fatalf("Filepath: got %q, want %q", got.Filepath, want.Filepath)
	}
	if got.FileChunkID != want.FileChunkID {
		t.Fatalf("FileChunkID: got %d, want %d", got.FileChunkID, want.FileChunkID)
	}
	if got.FileChunkCount != want.FileChunkCount {
		t.Fatalf("FileChunkCount: got %d, want %d", got.FileChunkCount, want.FileChunkCount)
	}
	if got.HasFileInfo != want.HasFileInfo {
		t.Fatalf("HasFileInfo: got %v, want %v", got.HasFileInfo, want.HasFileInfo)
	}
	if want.HasFileInfo {
		assertSnapshotFileEqual(t, &want.FileInfo, &got.FileInfo)
	}
	assertMembershipEqual(t, &want.Membership, &got.Membership)
	if got.OnDiskIndex != want.OnDiskIndex {
		t.Fatalf("OnDiskIndex: got %d, want %d", got.OnDiskIndex, want.OnDiskIndex)
	}
	if got.DeploymentID != want.DeploymentID {
		t.Fatalf("DeploymentID: got %d, want %d", got.DeploymentID, want.DeploymentID)
	}
	if got.BinVer != want.BinVer {
		t.Fatalf("BinVer: got %d, want %d", got.BinVer, want.BinVer)
	}
	if got.Epoch != want.Epoch {
		t.Fatalf("Epoch: got %d, want %d", got.Epoch, want.Epoch)
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("Data: got %v, want %v", got.Data, want.Data)
	}
}
