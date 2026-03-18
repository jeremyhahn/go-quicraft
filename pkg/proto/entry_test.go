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

func TestEntryMarshalUnmarshalRoundTrip(t *testing.T) {
	e := Entry{
		Term:        5,
		Index:       100,
		Type:        EntryNormal,
		Key:         42,
		ClientID:    1001,
		SeriesID:    7,
		RespondedTo: 6,
		Cmd:         []byte("set key=value"),
	}
	buf := make([]byte, e.Size())
	n, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != e.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, e.Size())
	}

	var e2 Entry
	consumed, err := e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertEntryEqual(t, &e, &e2)
}

func TestEntryMarshalUnmarshalConfigChange(t *testing.T) {
	e := Entry{
		Term:  10,
		Index: 200,
		Type:  EntryConfigChange,
		Cmd:   []byte("add-node 5"),
	}
	buf := make([]byte, e.Size())
	_, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var e2 Entry
	_, err = e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertEntryEqual(t, &e, &e2)
}

func TestEntryMarshalUnmarshalEncodedEntry(t *testing.T) {
	e := Entry{
		Term:  3,
		Index: 50,
		Type:  EntryEncodedEntry,
		Cmd:   []byte{0x01, 0x02, 0x03},
	}
	buf := make([]byte, e.Size())
	_, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var e2 Entry
	_, err = e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertEntryEqual(t, &e, &e2)
}

func TestEntryMarshalUnmarshalWithCompression(t *testing.T) {
	e := Entry{
		Term:  5,
		Index: 100,
		Type:  EntryNormal | EntryCompressFlag,
		Cmd:   []byte("compressed data"),
	}
	buf := make([]byte, e.Size())
	_, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var e2 Entry
	_, err = e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertEntryEqual(t, &e, &e2)
}

func TestEntryMarshalUnmarshalEmptyCmd(t *testing.T) {
	e := Entry{
		Term:  1,
		Index: 1,
		Type:  EntryNormal,
		Cmd:   nil,
	}
	buf := make([]byte, e.Size())
	n, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != entryHeaderSize {
		t.Fatalf("Size with nil cmd: got %d, want %d", n, entryHeaderSize)
	}
	var e2 Entry
	_, err = e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if e2.Cmd != nil {
		t.Fatalf("Cmd: got %v, want nil", e2.Cmd)
	}
}

func TestEntryMarshalUnmarshalLargeCmd(t *testing.T) {
	cmd := make([]byte, 4096)
	for i := range cmd {
		cmd[i] = byte(i % 256)
	}
	e := Entry{
		Term:  1,
		Index: 1,
		Type:  EntryNormal,
		Cmd:   cmd,
	}
	buf := make([]byte, e.Size())
	_, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var e2 Entry
	_, err = e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if !bytes.Equal(e2.Cmd, cmd) {
		t.Fatal("Large cmd data mismatch after round-trip")
	}
}

func TestEntrySize(t *testing.T) {
	tests := []struct {
		name string
		cmd  []byte
		want int
	}{
		{"nil cmd", nil, entryHeaderSize},
		{"empty cmd", []byte{}, entryHeaderSize},
		{"small cmd", []byte("abc"), entryHeaderSize + 3},
		{"256 bytes", make([]byte, 256), entryHeaderSize + 256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := Entry{Cmd: tt.cmd}
			if got := e.Size(); got != tt.want {
				t.Fatalf("Size: got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEntryBufferTooSmallMarshal(t *testing.T) {
	e := Entry{Cmd: []byte("data")}
	buf := make([]byte, e.Size()-1)
	_, err := e.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestEntryBufferTooSmallUnmarshalHeader(t *testing.T) {
	buf := make([]byte, entryHeaderSize-1)
	var e Entry
	_, err := e.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestEntryBufferTooSmallUnmarshalCmd(t *testing.T) {
	// Header says cmd is 100 bytes but buffer only has header
	e := Entry{Cmd: make([]byte, 100)}
	buf := make([]byte, e.Size())
	e.MarshalTo(buf)
	// Truncate the buffer
	var e2 Entry
	_, err := e2.UnmarshalFrom(buf[:entryHeaderSize+10])
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestEntryInvalidType(t *testing.T) {
	e := Entry{
		Term:  1,
		Index: 1,
		Type:  EntryNormal,
		Cmd:   nil,
	}
	buf := make([]byte, e.Size())
	e.MarshalTo(buf)
	// Corrupt the type field to an invalid value
	putUint64(buf[16:], 0x07) // 7 is not a valid entry type
	var e2 Entry
	_, err := e2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrInvalidEntryType) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrInvalidEntryType", err)
	}
}

func TestEntryCmdSizeExceedsMax(t *testing.T) {
	// Create a buffer with a header claiming cmd size > MaxEntrySize
	buf := make([]byte, entryHeaderSize)
	putUint64(buf[0:], 1)  // Term
	putUint64(buf[8:], 1)  // Index
	putUint64(buf[16:], 0) // Type = EntryNormal
	putUint64(buf[24:], 0) // Key
	putUint64(buf[32:], 0) // ClientID
	putUint64(buf[40:], 0) // SeriesID
	putUint64(buf[48:], 0) // RespondedTo
	putUint32(buf[56:], MaxEntrySize+1)
	var e Entry
	_, err := e.UnmarshalFrom(buf)
	if !errors.Is(err, ErrEntrySizeLimitExceeded) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrEntrySizeLimitExceeded", err)
	}
}

func TestEntryZeroCopyCmd(t *testing.T) {
	e := Entry{
		Term:  1,
		Index: 1,
		Type:  EntryNormal,
		Cmd:   []byte("original"),
	}
	buf := make([]byte, e.Size())
	e.MarshalTo(buf)
	var e2 Entry
	e2.UnmarshalFrom(buf)
	// Verify zero-copy: Cmd should alias buf
	if &e2.Cmd[0] != &buf[60] {
		t.Fatal("Cmd is not aliased into source buffer (not zero-copy)")
	}
}

func TestEntryReset(t *testing.T) {
	e := Entry{
		Term:        5,
		Index:       100,
		Type:        EntryConfigChange,
		Key:         42,
		ClientID:    1001,
		SeriesID:    7,
		RespondedTo: 6,
		Cmd:         []byte("data"),
	}
	e.Reset()
	if e.Term != 0 || e.Index != 0 || e.Type != 0 || e.Key != 0 ||
		e.ClientID != 0 || e.SeriesID != 0 || e.RespondedTo != 0 || e.Cmd != nil {
		t.Fatal("Reset did not clear all fields")
	}
}

func TestEntryMultipleInBuffer(t *testing.T) {
	entries := []Entry{
		{Term: 1, Index: 1, Type: EntryNormal, Cmd: []byte("cmd1")},
		{Term: 1, Index: 2, Type: EntryNormal, Cmd: []byte("cmd2-longer")},
		{Term: 2, Index: 3, Type: EntryConfigChange, Cmd: []byte("cc")},
	}
	totalSize := 0
	for i := range entries {
		totalSize += entries[i].Size()
	}
	buf := make([]byte, totalSize)
	offset := 0
	for i := range entries {
		n, err := entries[i].MarshalTo(buf[offset:])
		if err != nil {
			t.Fatalf("MarshalTo entry %d: %v", i, err)
		}
		offset += n
	}

	offset = 0
	for i := range entries {
		var e Entry
		n, err := e.UnmarshalFrom(buf[offset:])
		if err != nil {
			t.Fatalf("UnmarshalFrom entry %d: %v", i, err)
		}
		offset += n
		assertEntryEqual(t, &entries[i], &e)
	}
}

func assertEntryEqual(t *testing.T, want, got *Entry) {
	t.Helper()
	if got.Term != want.Term {
		t.Fatalf("Term: got %d, want %d", got.Term, want.Term)
	}
	if got.Index != want.Index {
		t.Fatalf("Index: got %d, want %d", got.Index, want.Index)
	}
	if got.Type != want.Type {
		t.Fatalf("Type: got %d, want %d", got.Type, want.Type)
	}
	if got.Key != want.Key {
		t.Fatalf("Key: got %d, want %d", got.Key, want.Key)
	}
	if got.ClientID != want.ClientID {
		t.Fatalf("ClientID: got %d, want %d", got.ClientID, want.ClientID)
	}
	if got.SeriesID != want.SeriesID {
		t.Fatalf("SeriesID: got %d, want %d", got.SeriesID, want.SeriesID)
	}
	if got.RespondedTo != want.RespondedTo {
		t.Fatalf("RespondedTo: got %d, want %d", got.RespondedTo, want.RespondedTo)
	}
	if !bytes.Equal(got.Cmd, want.Cmd) {
		t.Fatalf("Cmd: got %q, want %q", got.Cmd, want.Cmd)
	}
}

func BenchmarkEntryMarshalTo(b *testing.B) {
	cmd := make([]byte, 256)
	for i := range cmd {
		cmd[i] = byte(i)
	}
	e := Entry{
		Term:        5,
		Index:       100,
		Type:        EntryNormal,
		Key:         42,
		ClientID:    1001,
		SeriesID:    7,
		RespondedTo: 6,
		Cmd:         cmd,
	}
	buf := make([]byte, e.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		e.MarshalTo(buf)
	}
}

// ---------------------------------------------------------------------------
// Entry helper methods
// ---------------------------------------------------------------------------

func TestIsConfigChange(t *testing.T) {
	cc := Entry{Type: EntryConfigChange}
	if !cc.IsConfigChange() {
		t.Error("EntryConfigChange should return true")
	}
	normal := Entry{Type: EntryNormal}
	if normal.IsConfigChange() {
		t.Error("EntryNormal should return false")
	}
	encoded := Entry{Type: EntryEncodedEntry}
	if encoded.IsConfigChange() {
		t.Error("EntryEncodedEntry should return false")
	}
	// With compression flag
	compressed := Entry{Type: EntryConfigChange | EntryCompressFlag}
	if !compressed.IsConfigChange() {
		t.Error("compressed config change should return true")
	}
}

func TestIsSessionManaged(t *testing.T) {
	managed := Entry{ClientID: 42}
	if !managed.IsSessionManaged() {
		t.Error("ClientID=42 should be session managed")
	}
	unmanaged := Entry{ClientID: NotSessionManagedClientID}
	if unmanaged.IsSessionManaged() {
		t.Error("ClientID=0 should not be session managed")
	}
}

func TestIsNewSessionRequest(t *testing.T) {
	valid := Entry{
		ClientID: 42,
		SeriesID: SessionSeriesIDForRegister,
		Type:     EntryNormal,
	}
	if !valid.IsNewSessionRequest() {
		t.Error("valid register entry should return true")
	}

	// With Cmd: should be false
	withCmd := valid
	withCmd.Cmd = []byte("data")
	if withCmd.IsNewSessionRequest() {
		t.Error("entry with Cmd should return false")
	}

	// Config change type: should be false
	configChange := valid
	configChange.Type = EntryConfigChange
	if configChange.IsNewSessionRequest() {
		t.Error("config change should return false")
	}

	// Wrong SeriesID: should be false
	wrongSeries := valid
	wrongSeries.SeriesID = 5
	if wrongSeries.IsNewSessionRequest() {
		t.Error("wrong SeriesID should return false")
	}

	// Unmanaged client: should be false
	noClient := valid
	noClient.ClientID = 0
	if noClient.IsNewSessionRequest() {
		t.Error("ClientID=0 should return false")
	}
}

func TestIsEndOfSessionRequest(t *testing.T) {
	valid := Entry{
		ClientID: 42,
		SeriesID: SessionSeriesIDForUnregister,
		Type:     EntryNormal,
	}
	if !valid.IsEndOfSessionRequest() {
		t.Error("valid unregister entry should return true")
	}

	// With Cmd: should be false
	withCmd := valid
	withCmd.Cmd = []byte("data")
	if withCmd.IsEndOfSessionRequest() {
		t.Error("entry with Cmd should return false")
	}

	// Config change type: should be false
	configChange := valid
	configChange.Type = EntryConfigChange
	if configChange.IsEndOfSessionRequest() {
		t.Error("config change should return false")
	}

	// Wrong SeriesID: should be false
	wrongSeries := valid
	wrongSeries.SeriesID = 5
	if wrongSeries.IsEndOfSessionRequest() {
		t.Error("wrong SeriesID should return false")
	}

	// Register series ID should NOT match unregister
	registerSeries := valid
	registerSeries.SeriesID = SessionSeriesIDForRegister
	if registerSeries.IsEndOfSessionRequest() {
		t.Error("register SeriesID should not match unregister")
	}
}

func TestIsEmpty(t *testing.T) {
	empty := Entry{}
	if !empty.IsEmpty() {
		t.Error("entry with nil Cmd should be empty")
	}
	withCmd := Entry{Cmd: []byte("data")}
	if withCmd.IsEmpty() {
		t.Error("entry with Cmd should not be empty")
	}
	emptySlice := Entry{Cmd: []byte{}}
	if !emptySlice.IsEmpty() {
		t.Error("entry with empty slice Cmd should be empty")
	}
}

func TestIsMetadataEntry(t *testing.T) {
	meta := Entry{Type: EntryMetadata}
	if !meta.IsMetadataEntry() {
		t.Error("EntryMetadata should return true")
	}
	normal := Entry{Type: EntryNormal}
	if normal.IsMetadataEntry() {
		t.Error("EntryNormal should return false")
	}
	cc := Entry{Type: EntryConfigChange}
	if cc.IsMetadataEntry() {
		t.Error("EntryConfigChange should return false")
	}
	encoded := Entry{Type: EntryEncodedEntry}
	if encoded.IsMetadataEntry() {
		t.Error("EntryEncodedEntry should return false")
	}
	// With compression flag: should still detect EntryMetadata.
	compressed := Entry{Type: EntryMetadata | EntryCompressFlag}
	if !compressed.IsMetadataEntry() {
		t.Error("compressed metadata entry should return true")
	}
}

func TestIsMetadataEntry_NotOtherTypes(t *testing.T) {
	// Verify mutual exclusivity with other entry type predicates.
	meta := Entry{Type: EntryMetadata}
	if meta.IsConfigChange() {
		t.Error("metadata entry should not be a config change")
	}
	// IsSessionManaged checks ClientID, not Type, so metadata entries
	// with clientID=0 are not session-managed.
	if meta.IsSessionManaged() {
		t.Error("metadata entry with zero ClientID should not be session-managed")
	}
}

func TestEntryMarshalUnmarshalMetadata(t *testing.T) {
	e := Entry{
		Term:  7,
		Index: 300,
		Type:  EntryMetadata,
		// Metadata entries have no Cmd, Key, ClientID, SeriesID, RespondedTo.
	}
	buf := make([]byte, e.Size())
	n, err := e.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != entryHeaderSize {
		t.Fatalf("expected %d bytes for metadata entry, got %d", entryHeaderSize, n)
	}
	var e2 Entry
	consumed, err := e2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	if e2.Term != 7 {
		t.Fatalf("Term: got %d, want 7", e2.Term)
	}
	if e2.Index != 300 {
		t.Fatalf("Index: got %d, want 300", e2.Index)
	}
	if e2.Type != EntryMetadata {
		t.Fatalf("Type: got %d, want %d", e2.Type, EntryMetadata)
	}
	if e2.Cmd != nil {
		t.Fatalf("Cmd: got %v, want nil", e2.Cmd)
	}
}

func TestSessionSentinelValues_AreDistinct(t *testing.T) {
	if SessionSeriesIDForRegister == SessionSeriesIDForUnregister {
		t.Fatal("register and unregister sentinel values must differ")
	}
	if SessionSeriesIDForRegister == 0 {
		t.Fatal("register sentinel must not be zero")
	}
	if SessionSeriesIDForUnregister == 0 {
		t.Fatal("unregister sentinel must not be zero")
	}
}

func BenchmarkEntryUnmarshalFrom(b *testing.B) {
	cmd := make([]byte, 256)
	e := Entry{
		Term:  5,
		Index: 100,
		Type:  EntryNormal,
		Cmd:   cmd,
	}
	buf := make([]byte, e.Size())
	e.MarshalTo(buf)
	var e2 Entry
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		e2.UnmarshalFrom(buf)
	}
}
