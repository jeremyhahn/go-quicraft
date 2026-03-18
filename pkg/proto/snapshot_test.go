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

func TestSnapshotMarshalUnmarshalRoundTrip(t *testing.T) {
	s := Snapshot{
		ShardID:   1,
		ReplicaID: 2,
		Index:     100,
		Term:      5,
		Membership: Membership{
			ConfigChangeID: 10,
			Addresses:      map[uint64]string{1: "node-1:5000", 2: "node-2:5000"},
			Observers:      map[uint64]string{},
			Witnesses:      map[uint64]string{},
			Removed:        map[uint64]bool{},
		},
		Filepath:    "/snapshots/shard-1/snap-100.dat",
		FileSize:    1024 * 1024,
		Checksum:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
		Type:        RegularStateMachine,
		Imported:    true,
		OnDiskIndex: 99,
		Witness:     false,
		Epoch:       7,
	}
	buf := make([]byte, s.Size())
	n, err := s.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != s.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, s.Size())
	}

	var s2 Snapshot
	consumed, err := s2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertSnapshotEqual(t, &s, &s2)
}

func TestSnapshotMarshalUnmarshalAllBoolsTrue(t *testing.T) {
	s := Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     50,
		Term:      3,
		Membership: Membership{
			Addresses: map[uint64]string{1: "a"},
			Observers: map[uint64]string{},
			Witnesses: map[uint64]string{},
			Removed:   map[uint64]bool{},
		},
		Filepath:    "snap.dat",
		FileSize:    100,
		Checksum:    nil,
		Type:        OnDiskStateMachine,
		Imported:    true,
		OnDiskIndex: 49,
		Witness:     true,
	}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)
	var s2 Snapshot
	s2.UnmarshalFrom(buf)
	assertSnapshotEqual(t, &s, &s2)
}

func TestSnapshotIsEmpty(t *testing.T) {
	tests := []struct {
		name string
		snap Snapshot
		want bool
	}{
		{"zero", Snapshot{}, true},
		{"index set", Snapshot{Index: 1}, false},
		{"term set", Snapshot{Term: 1}, false},
		{"both set", Snapshot{Index: 1, Term: 1}, false},
		{"shardID set but index/term zero", Snapshot{ShardID: 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.snap.IsEmpty(); got != tt.want {
				t.Fatalf("IsEmpty: got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnapshotBufferTooSmallMarshal(t *testing.T) {
	s := Snapshot{Index: 1, Term: 1, Membership: Membership{
		Addresses: map[uint64]string{}, Observers: map[uint64]string{},
		Witnesses: map[uint64]string{}, Removed: map[uint64]bool{},
	}}
	buf := make([]byte, s.Size()-1)
	_, err := s.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestSnapshotBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, 16) // too short
	var s Snapshot
	_, err := s.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestSnapshotReset(t *testing.T) {
	s := Snapshot{
		ShardID:     1,
		ReplicaID:   2,
		Index:       100,
		Term:        5,
		Filepath:    "/path",
		FileSize:    1024,
		Checksum:    []byte{1, 2, 3},
		Type:        1,
		Imported:    true,
		OnDiskIndex: 99,
		Witness:     true,
		Epoch:       3,
	}
	s.Reset()
	if !s.IsEmpty() {
		t.Fatal("Reset snapshot should be empty")
	}
	if s.ShardID != 0 || s.ReplicaID != 0 || s.Filepath != "" ||
		s.FileSize != 0 || s.Checksum != nil || s.Type != 0 ||
		s.Imported || s.OnDiskIndex != 0 || s.Witness || s.Epoch != 0 {
		t.Fatal("Reset did not clear all fields")
	}
}

func TestSnapshotWithEmptyMembership(t *testing.T) {
	s := Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     10,
		Term:      1,
		Membership: Membership{
			Addresses: map[uint64]string{},
			Observers: map[uint64]string{},
			Witnesses: map[uint64]string{},
			Removed:   map[uint64]bool{},
		},
		Filepath: "test.snap",
	}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)
	var s2 Snapshot
	_, err := s2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertSnapshotEqual(t, &s, &s2)
}

func TestSnapshotWithLargeMembership(t *testing.T) {
	addrs := make(map[uint64]string)
	for i := uint64(1); i <= 50; i++ {
		addrs[i] = "10.0.0.1:5000"
	}
	s := Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     1000,
		Term:      10,
		Membership: Membership{
			ConfigChangeID: 999,
			Addresses:      addrs,
			Observers:      map[uint64]string{},
			Witnesses:      map[uint64]string{},
			Removed:        map[uint64]bool{100: true, 200: true},
		},
		Filepath: "/var/lib/quicraft/shard-1/snapshot-1000",
		FileSize: 10 * 1024 * 1024,
		Checksum: make([]byte, 32),
	}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)
	var s2 Snapshot
	_, err := s2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertSnapshotEqual(t, &s, &s2)
}

func TestSnapshotUnmarshalOversizedFilepath(t *testing.T) {
	// Build a valid snapshot, then manually craft a buffer with an
	// oversized filepath length prefix.
	s := Snapshot{
		ShardID:   1,
		ReplicaID: 1,
		Index:     1,
		Term:      1,
		Membership: Membership{
			Addresses: map[uint64]string{},
			Observers: map[uint64]string{},
			Witnesses: map[uint64]string{},
			Removed:   map[uint64]bool{},
		},
		Filepath: "ok.snap",
	}
	buf := make([]byte, s.Size())
	s.MarshalTo(buf)

	// Unmarshal the valid one to find where the Membership ends.
	var probe Snapshot
	_, err := probe.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("valid unmarshal failed: %v", err)
	}

	// Build a crafted buffer with filepath length > MaxFilepathLen.
	// Header (32) + empty membership + oversized filepath prefix.
	emptyMembership := Membership{
		Addresses: map[uint64]string{},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]bool{},
	}
	msize := emptyMembership.Size()
	oversizedLen := MaxFilepathLen + 1
	crafted := make([]byte, 32+msize+4+oversizedLen+8+4+64+5*8)
	putUint64(crafted[0:], 1)  // ShardID
	putUint64(crafted[8:], 1)  // ReplicaID
	putUint64(crafted[16:], 1) // Index
	putUint64(crafted[24:], 1) // Term
	emptyMembership.MarshalTo(crafted[32:])
	offset := 32 + msize
	putUint32(crafted[offset:], uint32(oversizedLen))

	var s2 Snapshot
	_, err = s2.UnmarshalFrom(crafted)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("expected ErrAddressTooLong for oversized filepath, got %v", err)
	}
}

func TestSnapshotUnmarshalNormalFieldsSucceed(t *testing.T) {
	// Verify normal-sized fields unmarshal correctly (regression test
	// for field length limits).
	s := Snapshot{
		ShardID:   42,
		ReplicaID: 7,
		Index:     500,
		Term:      10,
		Membership: Membership{
			ConfigChangeID: 5,
			Addresses:      map[uint64]string{1: "node1:5000"},
			Observers:      map[uint64]string{},
			Witnesses:      map[uint64]string{},
			Removed:        map[uint64]bool{},
		},
		Filepath: "/var/lib/quicraft/snapshot.dat",
		FileSize: 1024,
		Checksum: []byte{0x01, 0x02, 0x03, 0x04},
		Epoch:    3,
	}
	buf := make([]byte, s.Size())
	n, err := s.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var s2 Snapshot
	consumed, err := s2.UnmarshalFrom(buf[:n])
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("consumed %d, want %d", consumed, n)
	}
	assertSnapshotEqual(t, &s, &s2)
}

func assertSnapshotEqual(t *testing.T, want, got *Snapshot) {
	t.Helper()
	if got.ShardID != want.ShardID {
		t.Fatalf("ShardID: got %d, want %d", got.ShardID, want.ShardID)
	}
	if got.ReplicaID != want.ReplicaID {
		t.Fatalf("ReplicaID: got %d, want %d", got.ReplicaID, want.ReplicaID)
	}
	if got.Index != want.Index {
		t.Fatalf("Index: got %d, want %d", got.Index, want.Index)
	}
	if got.Term != want.Term {
		t.Fatalf("Term: got %d, want %d", got.Term, want.Term)
	}
	assertMembershipEqual(t, &want.Membership, &got.Membership)
	if got.Filepath != want.Filepath {
		t.Fatalf("Filepath: got %q, want %q", got.Filepath, want.Filepath)
	}
	if got.FileSize != want.FileSize {
		t.Fatalf("FileSize: got %d, want %d", got.FileSize, want.FileSize)
	}
	if !bytes.Equal(got.Checksum, want.Checksum) {
		t.Fatalf("Checksum: got %v, want %v", got.Checksum, want.Checksum)
	}
	if got.Type != want.Type {
		t.Fatalf("Type: got %d, want %d", got.Type, want.Type)
	}
	if got.Imported != want.Imported {
		t.Fatalf("Imported: got %v, want %v", got.Imported, want.Imported)
	}
	if got.OnDiskIndex != want.OnDiskIndex {
		t.Fatalf("OnDiskIndex: got %d, want %d", got.OnDiskIndex, want.OnDiskIndex)
	}
	if got.Witness != want.Witness {
		t.Fatalf("Witness: got %v, want %v", got.Witness, want.Witness)
	}
	if got.Epoch != want.Epoch {
		t.Fatalf("Epoch: got %d, want %d", got.Epoch, want.Epoch)
	}
}
