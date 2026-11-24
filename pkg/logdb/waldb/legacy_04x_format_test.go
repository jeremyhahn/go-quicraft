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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Detection of 0.4.x on-disk data, which 0.5.x cannot read, by retired payload type bytes.

// retiredEntryHeaderSize is the entry record header 0.4.x wrote: the current
// 84-byte header minus OwnerID, which 0.5.x inserted at +72.
const retiredEntryHeaderSize = entryRecordHeaderSize - 8 // 76 bytes

// retiredEntryRecord builds a 0.4.x single-entry record (type 0x07, 76-byte header) whose command would misparse cleanly under the current layout.
func retiredEntryRecord() []byte {
	cmd := make([]byte, 24)
	copy(cmd, "ABCD")
	binary.LittleEndian.PutUint32(cmd[4:], uint32(len(cmd)-8))
	copy(cmd[8:], "PAYLOAD-PAYLOAD!")

	rec := make([]byte, 1+retiredEntryHeaderSize+len(cmd))
	// The literal retired byte, not a constant, so the test fails if a constant moves back to it.
	rec[0] = 0x07
	binary.LittleEndian.PutUint64(rec[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(rec[9:], 2)   // replicaID
	binary.LittleEndian.PutUint64(rec[17:], 7)  // index
	binary.LittleEndian.PutUint64(rec[25:], 3)  // term
	binary.LittleEndian.PutUint64(rec[33:], 0)  // type
	binary.LittleEndian.PutUint64(rec[41:], 11) // Key
	binary.LittleEndian.PutUint64(rec[49:], 22) // ClientID
	binary.LittleEndian.PutUint64(rec[57:], 33) // SeriesID
	binary.LittleEndian.PutUint64(rec[65:], 44) // RespondedTo
	binary.LittleEndian.PutUint32(rec[73:], uint32(len(cmd)))
	copy(rec[1+retiredEntryHeaderSize:], cmd)
	return rec
}

// newReplayShard returns a walShard with the maps replayRecord writes into.
func newReplayShard() *walShard {
	return &walShard{
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
	}
}

// TestRetiredSnapshotPayloadTypeRejected checks a 0.4.x snapshot record is refused as an unknown payload type.
func TestRetiredSnapshotPayloadTypeRejected(t *testing.T) {
	// Each retired snapshot layout; 0x0a has a three-field session policy, 0x0c four fields without WireVersion.
	for _, retired := range []struct {
		typ      byte
		tailSize int
	}{
		{0x0a, membershipSessionPolicySize - 16}, // 24 bytes
		{0x0c, membershipSessionPolicySize - 8},  // 32 bytes
	} {
		payload := make([]byte,
			1+snapshotRecordBaseSize+membershipFixedSize+6*4+retired.tailSize)
		// The literal retired byte; see retiredEntryRecord.
		payload[0] = retired.typ
		binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
		binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
		binary.LittleEndian.PutUint64(payload[17:], 10) // snapshot index
		binary.LittleEndian.PutUint64(payload[25:], 2)  // snapshot term
		payload[1+snapshotRecordBaseSize] = membershipSectionVersion

		t.Run(fmt.Sprintf("Open fails on a segment with a retired %#02x snapshot record", retired.typ), func(t *testing.T) {
			dir := t.TempDir()
			writeRawShardRecord(t, dir, 1, payload)

			_, err := Open(dir, WithNoSync(true), WithBlockSize(1024),
				WithMaxFileSize(64*1024))
			if !errors.Is(err, ErrUnknownPayloadType) {
				t.Fatalf("Open = %v, want ErrUnknownPayloadType", err)
			}
			if errors.Is(err, ErrTruncatedSnapshotRecord) {
				t.Fatal("a legacy snapshot record was reported as TRUNCATED; the record " +
					"is intact and the remedy is to wipe and re-sync, not to investigate " +
					"corruption")
			}
		})
	}
}

// TestRetiredStatePayloadTypeRejected checks a 40-byte 0x02 hard-state record without VoteWindow is refused as unknown.
func TestRetiredStatePayloadTypeRejected(t *testing.T) {
	const legacyStateRecordSize = stateRecordSize - 8 // 40 bytes
	payload := make([]byte, 1+legacyStateRecordSize)
	// The literal retired byte; see retiredEntryRecord.
	payload[0] = 0x02
	binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 5)  // term
	binary.LittleEndian.PutUint64(payload[25:], 2)  // vote
	binary.LittleEndian.PutUint64(payload[33:], 10) // commit

	t.Run("Open fails on a segment with a retired state record", func(t *testing.T) {
		dir := t.TempDir()
		writeRawShardRecord(t, dir, 1, payload)

		_, err := Open(dir, WithNoSync(true), WithBlockSize(1024),
			WithMaxFileSize(64*1024))
		if !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("Open = %v, want ErrUnknownPayloadType", err)
		}
		if errors.Is(err, ErrTruncatedStateRecord) {
			t.Fatal("a legacy state record was reported as TRUNCATED; the record " +
				"is intact and the remedy is to wipe and re-sync")
		}
	})
}

// TestA04xDirectoryIsRefusedByRecordNotByVersion checks a 0.4.x directory passes the version marker and is refused by the record payload types.
func TestA04xDirectoryIsRefusedByRecordNotByVersion(t *testing.T) {
	const legacyWALFormatVersion uint32 = 1

	if walFormatVersion != legacyWALFormatVersion {
		t.Fatalf("walFormatVersion = %d: 0.4.x recorded %d, so the marker gate now "+
			"catches a 0.4.x directory before its records are read. Detection has "+
			"moved; re-point this test at whichever gate is now load-bearing.",
			walFormatVersion, legacyWALFormatVersion)
	}

	dir := t.TempDir()
	seedWAL(t, dir, 1024)

	// The marker byte for byte as 0.4.x wrote it: right magic, right geometry,
	// version 1 -- which is also what this build writes.
	buf := make([]byte, walFormatSize)
	copy(buf[0:8], walFormatMagic)
	binary.LittleEndian.PutUint32(buf[8:12], legacyWALFormatVersion)
	binary.LittleEndian.PutUint32(buf[12:16], 1024)
	if err := os.WriteFile(filepath.Join(dir, walFormatFilename), buf, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	// The marker alone must NOT refuse it. A directory carrying only 0.4.x's
	// marker and no legacy record is indistinguishable from one of ours.
	db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open on a 0.4.x marker alone = %v, want success: the version is "+
			"pinned at 1 on both sides, so the marker carries no discriminator", err)
	}
	if cerr := db.Close(); cerr != nil {
		t.Fatalf("Close = %v", cerr)
	}

	// Put a 0.4.x entry record in a segment and the record gate must refuse it.
	legacyDir := t.TempDir()
	writeRawShardRecord(t, legacyDir, 1, retiredEntryRecord())
	if _, err := Open(legacyDir, WithNoSync(true), WithBlockSize(1024),
		WithMaxFileSize(64*1024)); !errors.Is(err, ErrUnknownPayloadType) {
		t.Fatalf("Open on a directory holding an old entry record = %v, want "+
			"ErrUnknownPayloadType: with the marker pinned at 1 this is the only "+
			"gate, and without it the 76-byte header parses as 84 and misparses", err)
	}
}

// TestPayloadTypesAreDistinct checks every live and retired payload type byte is distinct.
func TestPayloadTypesAreDistinct(t *testing.T) {
	// Live constants plus every retired byte as a literal.
	types := map[string]byte{
		"payloadTypeState":     payloadTypeState,
		"payloadTypeBootstrap": payloadTypeBootstrap,
		"payloadTypeCompact":   payloadTypeCompact,
		"payloadTypeEntries":   payloadTypeEntries,
		"payloadTypeSnapshot":  payloadTypeSnapshot,
		"retired 0x01":         0x01,
		"retired 0x02":         0x02,
		"retired 0x03":         0x03,
		"retired 0x06":         0x06,
		"retired 0x07":         0x07,
		"retired 0x08":         0x08,
		"retired 0x09":         0x09,
		"retired 0x0a":         0x0a,
		"retired 0x0c":         0x0c,
	}
	seen := make(map[byte]string, len(types))
	for name, b := range types {
		if other, dup := seen[b]; dup {
			t.Fatalf("%s and %s are both %#x: one on-disk byte cannot name two "+
				"record layouts, which is exactly how a 0.4.x record came to be "+
				"read as a 0.5.x one", name, other, b)
		}
		seen[b] = name
		// 0x00 is what a keyed WAL decodes to without a barrier; it stays unallocated.
		if b == 0x00 {
			t.Fatalf("%s is 0x00, which is reserved as the no-barrier misdecode "+
				"sentinel", name)
		}
	}
}

// TestEntryRecordIsWrittenWithCurrentPayloadType checks the entry payload type byte on disk.
func TestEntryRecordIsWrittenWithCurrentPayloadType(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cmd := []byte("hello")
	if err := db.SaveEntries(1, 1, []logdb.Entry{{
		Index: 1, Term: 1, Type: 0,
		Key: 11, ClientID: 22, SeriesID: 33, RespondedTo: 44, OwnerID: 55,
		Cmd: cmd,
	}}); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// findRawShardRecord fails the test if no record carries the type, which is
	// the assertion: a writer still stamping the superseded byte leaves none.
	rec := findRawShardRecord(t, dir, 1, payloadTypeEntries)
	if want := 1 + entryRecordHeaderSize + len(cmd); len(rec) != want {
		t.Fatalf("record length = %d, want %d; the payload type and the header "+
			"size must name the same layout", len(rec), want)
	}
	if got := binary.LittleEndian.Uint64(rec[1+72:]); got != 55 {
		t.Fatalf("OwnerID at +72 = %d, want 55", got)
	}
	if got := binary.LittleEndian.Uint32(rec[1+80:]); got != uint32(len(cmd)) {
		t.Fatalf("CmdLen at +80 = %d, want %d", got, len(cmd))
	}
}

// TestEntryAndSnapshotRoundTripAcrossReopen checks current records round-trip across a reopen.
func TestEntryAndSnapshotRoundTripAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := []logdb.Entry{
		{Index: 1, Term: 2, Type: 0, Key: 11, ClientID: 22, SeriesID: 33,
			RespondedTo: 44, OwnerID: 55, Cmd: []byte("first")},
		{Index: 2, Term: 2, Type: 0, Key: 12, ClientID: 23, SeriesID: 34,
			RespondedTo: 45, OwnerID: 56, Cmd: []byte("second")},
	}
	if err := db.SaveEntries(1, 1, want); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	wantSnap := logdb.Snapshot{
		Index: 1, Term: 2, Filepath: "snap-1.dat", FileSize: 4096, OnDiskIndex: 1,
		Membership: logdb.Membership{
			ConfigChangeID:         7,
			Addresses:              map[uint64]string{1: "quic://a:1"},
			SessionExpiryEntries:   100,
			MaxTotalSessions:       200,
			MaxResponsesPerSession: 300,
			MaxSessionsPerOwner:    400,
			WireVersion:            500,
		},
	}
	if err := db.SaveSnapshot(1, 1, wantSnap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	got, _, err := db2.IterateEntries(nil, 0, 1, 1, 1, 3, 0)
	if err != nil {
		t.Fatalf("IterateEntries: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("replayed %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Index != w.Index || g.Term != w.Term || g.Type != w.Type ||
			g.Key != w.Key || g.ClientID != w.ClientID || g.SeriesID != w.SeriesID ||
			g.RespondedTo != w.RespondedTo || g.OwnerID != w.OwnerID ||
			string(g.Cmd) != string(w.Cmd) {
			t.Fatalf("entry %d = %+v, want %+v", i, g, w)
		}
	}

	gotSnap, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if gotSnap.Index != wantSnap.Index || gotSnap.Term != wantSnap.Term ||
		gotSnap.Filepath != wantSnap.Filepath ||
		gotSnap.Membership.ConfigChangeID != wantSnap.Membership.ConfigChangeID ||
		gotSnap.Membership.Addresses[1] != wantSnap.Membership.Addresses[1] ||
		gotSnap.Membership.SessionExpiryEntries != wantSnap.Membership.SessionExpiryEntries ||
		gotSnap.Membership.MaxTotalSessions != wantSnap.Membership.MaxTotalSessions ||
		gotSnap.Membership.MaxResponsesPerSession != wantSnap.Membership.MaxResponsesPerSession ||
		gotSnap.Membership.MaxSessionsPerOwner != wantSnap.Membership.MaxSessionsPerOwner ||
		gotSnap.Membership.WireVersion != wantSnap.Membership.WireVersion {
		t.Fatalf("snapshot = %+v, want %+v", gotSnap, wantSnap)
	}
}

// TestPayloadTypesAreThePublishedBytes checks each live payload type equals its published literal byte.
func TestPayloadTypesAreThePublishedBytes(t *testing.T) {
	published := []struct {
		name string
		got  byte
		want byte
	}{
		// 0x02 was the 40-byte state record; the record gained VoteWindow and
		// moved to 0x0d, retired below. WALs written before are refused with
		// ErrUnknownPayloadType and re-synced from peers (pre-v1 rule).
		{"payloadTypeState", payloadTypeState, 0x0d},
		{"payloadTypeBootstrap", payloadTypeBootstrap, 0x04},
		{"payloadTypeCompact", payloadTypeCompact, 0x05},
		// 0x06 was the RemoveNode tombstone, retired above: a removal is kept
		// in the retired-replica registry beside the WAL (retired.go).
		{"payloadTypeEntries", payloadTypeEntries, 0x0b},
		// 0x0c was the snapshot layout without the committed WireVersion,
		// retired above; the current layout moved to 0x0e.
		{"payloadTypeSnapshot", payloadTypeSnapshot, 0x0e},
	}
	for _, p := range published {
		if p.got != p.want {
			t.Fatalf("%s = %#x, want %#x. This byte is on disk in every WAL a "+
				"released build wrote; moving it makes those directories "+
				"unreadable with ErrUnknownPayloadType and the shard cannot "+
				"start. Change the LAYOUT in place instead, and if the byte "+
				"genuinely must move, retire the old one in "+
				"TestPayloadTypesAreDistinct and state the migration.",
				p.name, p.got, p.want)
		}
	}
}
