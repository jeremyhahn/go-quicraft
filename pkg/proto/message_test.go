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
	"errors"
	"testing"
)

func TestMessageMarshalUnmarshalRoundTrip(t *testing.T) {
	m := Message{
		Type:      Replicate,
		From:      1,
		To:        2,
		ShardID:   100,
		ReplicaID: 1,
		Term:      5,
		LogTerm:   4,
		LogIndex:  99,
		Commit:    98,
		Reject:    false,
		Hint:      0,
	}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != m.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, m.Size())
	}

	var m2 Message
	consumed, err := m2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertMessageEqual(t, &m, &m2)
}

func TestMessageMarshalUnmarshalWithReject(t *testing.T) {
	m := Message{
		Type:   ReplicateResp,
		From:   2,
		To:     1,
		Term:   5,
		Reject: true,
		Hint:   95,
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Message
	m2.UnmarshalFrom(buf)
	if !m2.Reject {
		t.Fatal("Reject: got false, want true")
	}
	if m2.Hint != 95 {
		t.Fatalf("Hint: got %d, want 95", m2.Hint)
	}
}

func TestMessageMarshalUnmarshalWithEntries(t *testing.T) {
	m := Message{
		Type:    Replicate,
		From:    1,
		To:      2,
		ShardID: 100,
		Term:    5,
		Entries: []Entry{
			{Term: 5, Index: 100, Type: EntryNormal, Cmd: []byte("cmd1")},
			{Term: 5, Index: 101, Type: EntryNormal, Cmd: []byte("cmd2")},
			{Term: 5, Index: 102, Type: EntryConfigChange, Cmd: []byte("cc")},
		},
	}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var m2 Message
	consumed, err := m2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("consumed %d, want %d", consumed, n)
	}
	if len(m2.Entries) != 3 {
		t.Fatalf("Entries: got %d, want 3", len(m2.Entries))
	}
	for i := range m.Entries {
		assertEntryEqual(t, &m.Entries[i], &m2.Entries[i])
	}
}

func TestMessageMarshalUnmarshalWithSnapshot(t *testing.T) {
	m := Message{
		Type:    InstallSnapshot,
		From:    1,
		To:      2,
		ShardID: 100,
		Term:    5,
		Snapshot: Snapshot{
			ShardID:   100,
			ReplicaID: 1,
			Index:     50,
			Term:      4,
			Membership: Membership{
				ConfigChangeID: 10,
				Addresses:      map[uint64]string{1: "node-1:5000"},
				Observers:      map[uint64]string{},
				Witnesses:      map[uint64]string{},
				Removed:        map[uint64]bool{},
			},
			Filepath: "/snap/50.dat",
			FileSize: 1024,
		},
	}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var m2 Message
	consumed, err := m2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("consumed %d, want %d", consumed, n)
	}
	assertSnapshotEqual(t, &m.Snapshot, &m2.Snapshot)
}

func TestMessageMarshalUnmarshalWithEntriesAndSnapshot(t *testing.T) {
	m := Message{
		Type:    InstallSnapshot,
		From:    1,
		To:      2,
		ShardID: 100,
		Term:    5,
		Entries: []Entry{
			{Term: 5, Index: 100, Type: EntryNormal, Cmd: []byte("data")},
		},
		Snapshot: Snapshot{
			ShardID:   100,
			ReplicaID: 1,
			Index:     50,
			Term:      4,
			Membership: Membership{
				Addresses: map[uint64]string{1: "a"},
				Observers: map[uint64]string{},
				Witnesses: map[uint64]string{},
				Removed:   map[uint64]bool{},
			},
			Filepath: "snap",
		},
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Message
	m2.UnmarshalFrom(buf)
	if len(m2.Entries) != 1 {
		t.Fatalf("Entries: got %d, want 1", len(m2.Entries))
	}
	if m2.Snapshot.IsEmpty() {
		t.Fatal("Snapshot should not be empty")
	}
}

func TestMessageMarshalUnmarshalNoEntries(t *testing.T) {
	m := Message{
		Type: Heartbeat,
		From: 1,
		To:   2,
		Term: 5,
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Message
	m2.UnmarshalFrom(buf)
	if m2.Entries != nil {
		t.Fatalf("Entries: got %v, want nil", m2.Entries)
	}
}

func TestMessageMarshalUnmarshalAllMessageTypes(t *testing.T) {
	types := []uint64{
		LocalTick, Election, LeaderHeartbeat, ConfigChangeEvent, NoOP,
		Ping, Pong, Propose, SnapshotStatus, Unreachable, CheckQuorum,
		BatchedReadIndex, Replicate, ReplicateResp, RequestVote,
		RequestVoteResp, InstallSnapshot, Heartbeat, HeartbeatResp,
		ReadIndex, ReadIndexResp, Quiesce, SnapshotReceived,
		LeaderTransfer, TimeoutNow, RequestPreVote, RequestPreVoteResp,
		RateLimit,
	}
	for _, msgType := range types {
		m := Message{Type: msgType, From: 1, To: 2, Term: 1}
		buf := make([]byte, m.Size())
		m.MarshalTo(buf)
		var m2 Message
		_, err := m2.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("Message type %d: UnmarshalFrom: %v", msgType, err)
		}
		if m2.Type != msgType {
			t.Fatalf("Message type: got %d, want %d", m2.Type, msgType)
		}
	}
}

func TestMessageInvalidType(t *testing.T) {
	m := Message{Type: Replicate, From: 1, To: 2, Term: 1}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	// Corrupt type to invalid value
	putUint64(buf[0:], RateLimit+1)
	var m2 Message
	_, err := m2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrInvalidMessageType) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrInvalidMessageType", err)
	}
}

func TestMessageTooManyEntries(t *testing.T) {
	// Create a valid message header, then corrupt entries count
	m := Message{Type: Replicate, From: 1, To: 2, Term: 1}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	// Set entry count to maxMessagesPerBatch + 1
	putUint32(buf[messageFixedSize:], uint32(maxMessagesPerBatch+1))
	var m2 Message
	_, err := m2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrTooManyMessages) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrTooManyMessages", err)
	}
}

func TestMessageBufferTooSmallMarshal(t *testing.T) {
	m := Message{Type: Replicate, From: 1, To: 2, Term: 1}
	buf := make([]byte, m.Size()-1)
	_, err := m.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMessageBufferTooSmallUnmarshal(t *testing.T) {
	buf := make([]byte, messageFixedSize) // missing entries count
	var m Message
	_, err := m.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMessageReset(t *testing.T) {
	m := Message{
		Type:      Replicate,
		From:      1,
		To:        2,
		ShardID:   100,
		ReplicaID: 1,
		Term:      5,
		LogTerm:   4,
		LogIndex:  99,
		Commit:    98,
		Reject:    true,
		Hint:      50,
		Entries:   []Entry{{Term: 1}},
		Snapshot:  Snapshot{Index: 1, Term: 1},
	}
	m.Reset()
	if m.Type != 0 || m.From != 0 || m.To != 0 || m.ShardID != 0 ||
		m.ReplicaID != 0 || m.Term != 0 || m.LogTerm != 0 || m.LogIndex != 0 ||
		m.Commit != 0 || m.Reject || m.Hint != 0 ||
		len(m.Entries) != 0 || !m.Snapshot.IsEmpty() {
		t.Fatal("Reset did not clear all fields")
	}
}

func TestMessageSize(t *testing.T) {
	// No entries, empty snapshot
	m := Message{Type: Heartbeat}
	// 88 (fixed) + 4 (entries count) + 8 (HasSnapshot flag)
	expectedSize := messageFixedSize + 4 + 8
	if m.Size() != expectedSize {
		t.Fatalf("Size: got %d, want %d", m.Size(), expectedSize)
	}
}

func assertMessageEqual(t *testing.T, want, got *Message) {
	t.Helper()
	if got.Type != want.Type {
		t.Fatalf("Type: got %d, want %d", got.Type, want.Type)
	}
	if got.From != want.From {
		t.Fatalf("From: got %d, want %d", got.From, want.From)
	}
	if got.To != want.To {
		t.Fatalf("To: got %d, want %d", got.To, want.To)
	}
	if got.ShardID != want.ShardID {
		t.Fatalf("ShardID: got %d, want %d", got.ShardID, want.ShardID)
	}
	if got.ReplicaID != want.ReplicaID {
		t.Fatalf("ReplicaID: got %d, want %d", got.ReplicaID, want.ReplicaID)
	}
	if got.Term != want.Term {
		t.Fatalf("Term: got %d, want %d", got.Term, want.Term)
	}
	if got.LogTerm != want.LogTerm {
		t.Fatalf("LogTerm: got %d, want %d", got.LogTerm, want.LogTerm)
	}
	if got.LogIndex != want.LogIndex {
		t.Fatalf("LogIndex: got %d, want %d", got.LogIndex, want.LogIndex)
	}
	if got.Commit != want.Commit {
		t.Fatalf("Commit: got %d, want %d", got.Commit, want.Commit)
	}
	if got.Reject != want.Reject {
		t.Fatalf("Reject: got %v, want %v", got.Reject, want.Reject)
	}
	if got.Hint != want.Hint {
		t.Fatalf("Hint: got %d, want %d", got.Hint, want.Hint)
	}
}

func BenchmarkMessageMarshalTo(b *testing.B) {
	cmd := make([]byte, 256)
	m := Message{
		Type:    Replicate,
		From:    1,
		To:      2,
		ShardID: 100,
		Term:    5,
		Entries: []Entry{
			{Term: 5, Index: 100, Type: EntryNormal, Cmd: cmd},
			{Term: 5, Index: 101, Type: EntryNormal, Cmd: cmd},
		},
	}
	buf := make([]byte, m.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.MarshalTo(buf)
	}
}

func BenchmarkMessageUnmarshalFrom(b *testing.B) {
	cmd := make([]byte, 256)
	m := Message{
		Type:    Replicate,
		From:    1,
		To:      2,
		ShardID: 100,
		Term:    5,
		Entries: []Entry{
			{Term: 5, Index: 100, Type: EntryNormal, Cmd: cmd},
			{Term: 5, Index: 101, Type: EntryNormal, Cmd: cmd},
		},
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Message
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m2.UnmarshalFrom(buf)
	}
}
