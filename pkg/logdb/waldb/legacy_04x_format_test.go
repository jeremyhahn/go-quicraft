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

// This file covers the detection of 0.4.x on-disk data, which 0.5.x cannot
// read. The break itself was deliberate; going undetected was not. Detection is
// per-record: the format version marker is pinned, so the payload type byte is
// what separates a readable record from one written under a layout whose fields
// have since moved. A retired byte is left unallocated and falls through to
// ErrUnknownPayloadType rather than being misparsed.

// retiredEntryHeaderSize is the entry record header 0.4.x wrote: the current
// 84-byte header minus OwnerID, which 0.5.x inserted at +72.
const retiredEntryHeaderSize = entryRecordHeaderSize - 8 // 76 bytes

// retiredEntryRecord builds a single-entry record exactly as 0.4.x wrote it:
// payload type 0x07, a 76-byte header, then the command.
//
// The command is chosen so the record MISPARSES SILENTLY rather than being
// caught as corruption. The current decoder takes CmdLen from +80, which lands
// four bytes into a retired-format record's command; with those bytes naming len(cmd)-8 the
// misparse consumes the record exactly, so every length check passes and the
// entry is stored. That is the case the payload-type gate exists for — a
// record that fails a length check was never the dangerous one.
func retiredEntryRecord() []byte {
	cmd := make([]byte, 24)
	copy(cmd, "ABCD")
	binary.LittleEndian.PutUint32(cmd[4:], uint32(len(cmd)-8))
	copy(cmd[8:], "PAYLOAD-PAYLOAD!")

	rec := make([]byte, 1+retiredEntryHeaderSize+len(cmd))
	// The LITERAL retired byte, deliberately not a named constant.
	// A fixture built from the constant follows it wherever it is moved, so it
	// would keep passing if the current format were pointed back at 0x07 --
	// which is precisely the collision this file exists to prevent. Pinning the
	// on-disk value is what makes the test able to fail.
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

// TestRetiredEntryPayloadTypeRejected pins the entry gate.
//
// While 0x07 still named the current entry format, this fixture replayed with a
// nil error and stored an entry whose OwnerID was the record's own CmdLen
// concatenated with the first four bytes of its command, and whose command was
// the last sixteen bytes of the real one. OwnerID is the session-admission
// grouping key every replica must agree on, and the command goes to the state
// machine, so the accepted-and-wrong outcome is worse than any rejection.
func TestRetiredEntryPayloadTypeRejected(t *testing.T) {
	t.Run("replayRecord returns ErrUnknownPayloadType", func(t *testing.T) {
		s := newReplayShard()
		_, _, err := s.replayRecord(retiredEntryRecord(), 0)
		if !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("replayRecord(0x07) err = %v, want ErrUnknownPayloadType", err)
		}
		if len(s.entries) != 0 {
			t.Fatalf("a refused legacy record stored %d node entry maps; nothing "+
				"from a format this build cannot read may reach the log", len(s.entries))
		}
	})

	t.Run("Open fails on a segment with a retired entry record", func(t *testing.T) {
		dir := t.TempDir()
		writeRawShardRecord(t, dir, 1, retiredEntryRecord())

		if _, err := Open(dir, WithNoSync(true), WithBlockSize(1024),
			WithMaxFileSize(64*1024)); !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("Open = %v, want ErrUnknownPayloadType", err)
		}
	})
}

// TestRetiredSnapshotPayloadTypeRejected pins the snapshot gate.
//
// The snapshot record carried the same unallocated change: 0.5.x grew the
// membership section's session policy tail from three fields to four without
// moving payloadTypeSnapshot, so a 0.4.x record's RemovedIdentities map begins
// eight bytes before the current decoder looks for it. Before the payload type
// was allocated this fixture reached replaySnapshot and came back
// ErrTruncatedSnapshotRecord — a corruption diagnosis, sending an operator to
// look for a damaged disk when the answer is a wiped data directory.
func TestRetiredSnapshotPayloadTypeRejected(t *testing.T) {
	// A retired-layout snapshot record: base header, zero fpLen, membership section
	// with its version byte, the fixed region, six empty map counts and a
	// THREE-field session policy tail.
	const legacySessionPolicySize = membershipSessionPolicySize - 8 // 24 bytes
	payload := make([]byte,
		1+snapshotRecordBaseSize+membershipFixedSize+6*4+legacySessionPolicySize)
	// The LITERAL retired byte -- see the note in retiredEntryRecord for why a
	// fixture built from a constant cannot fail.
	payload[0] = 0x0a
	binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 10) // snapshot index
	binary.LittleEndian.PutUint64(payload[25:], 2)  // snapshot term
	payload[1+snapshotRecordBaseSize] = membershipSectionVersion

	t.Run("replayRecord returns ErrUnknownPayloadType", func(t *testing.T) {
		s := newReplayShard()
		_, _, err := s.replayRecord(payload, 0)
		if !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("replayRecord(0x0a) err = %v, want ErrUnknownPayloadType", err)
		}
		if errors.Is(err, ErrTruncatedSnapshotRecord) {
			t.Fatal("a legacy snapshot record was reported as TRUNCATED; the record " +
				"is intact and the remedy is to wipe and re-sync, not to investigate " +
				"corruption")
		}
		if len(s.snapshots) != 0 {
			t.Fatalf("a refused legacy record stored %d snapshots", len(s.snapshots))
		}
	})

	t.Run("Open fails on a segment with a retired snapshot record", func(t *testing.T) {
		dir := t.TempDir()
		writeRawShardRecord(t, dir, 1, payload)

		if _, err := Open(dir, WithNoSync(true), WithBlockSize(1024),
			WithMaxFileSize(64*1024)); !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("Open = %v, want ErrUnknownPayloadType", err)
		}
	})
}

// TestRetiredStatePayloadTypeRejected pins that a hard-state record in the
// 40-byte layout released builds wrote under 0x02 — Term, Vote and Commit,
// without the VoteWindow a leader's read lease may be relying on — is refused
// as an unknown record rather than reported as a truncated one. A current
// replica recovering from it would come up owing no vote window, which is the
// safety property the field exists to carry across a restart, so it must not
// be read at all: the remedy is to wipe the directory and re-sync.
func TestRetiredStatePayloadTypeRejected(t *testing.T) {
	const legacyStateRecordSize = stateRecordSize - 8 // 40 bytes
	payload := make([]byte, 1+legacyStateRecordSize)
	// The LITERAL retired byte -- see the note in retiredEntryRecord.
	payload[0] = 0x02
	binary.LittleEndian.PutUint64(payload[1:], 1)   // shardID
	binary.LittleEndian.PutUint64(payload[9:], 1)   // replicaID
	binary.LittleEndian.PutUint64(payload[17:], 5)  // term
	binary.LittleEndian.PutUint64(payload[25:], 2)  // vote
	binary.LittleEndian.PutUint64(payload[33:], 10) // commit

	t.Run("replayRecord returns ErrUnknownPayloadType", func(t *testing.T) {
		s := newReplayShard()
		_, _, err := s.replayRecord(payload, 0)
		if !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("replayRecord(0x02) err = %v, want ErrUnknownPayloadType", err)
		}
		if errors.Is(err, ErrTruncatedStateRecord) {
			t.Fatal("a legacy state record was reported as TRUNCATED; the record " +
				"is intact and the remedy is to wipe and re-sync")
		}
		if len(s.states) != 0 {
			t.Fatalf("a refused legacy record stored %d states", len(s.states))
		}
	})

	t.Run("Open fails on a segment with a retired state record", func(t *testing.T) {
		dir := t.TempDir()
		writeRawShardRecord(t, dir, 1, payload)

		if _, err := Open(dir, WithNoSync(true), WithBlockSize(1024),
			WithMaxFileSize(64*1024)); !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("Open = %v, want ErrUnknownPayloadType", err)
		}
	})
}

// TestA04xDirectoryIsRefusedByRecordNotByVersion pins WHERE detection lives.
//
// walFormatVersion is deliberately held at 1 for the whole pre-release period,
// and 0.4.x recorded 1 as well, so the marker gate cannot tell the two apart —
// a 0.4.x directory passes it. That is intentional, not an oversight, and it
// makes the record-level payload types the ONLY thing standing between a 0.4.x
// directory and a silent misparse. This test asserts both halves, because the
// first half is what makes the second load-bearing: if someone bumps the
// constant later, the marker gate starts catching this fixture first and the
// record gate stops being exercised by it.
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

// TestPayloadTypesAreDistinct pins the invariant whose violation caused all of
// this: 0.5.x changed the entry and snapshot layouts and REUSED the payload
// types 0.4.x had written, so the two generations were indistinguishable on
// disk. A byte serves as a format discriminator only while it names one format,
// and a superseded byte therefore stays reserved forever rather than being
// recycled. Sweeping every constant is what makes this an invariant rather than
// an assertion about the pair that happened to collide.
func TestPayloadTypesAreDistinct(t *testing.T) {
	// Live constants, plus every retired byte as a LITERAL. The retired values
	// are not named in the source precisely so they cannot be used, so this is
	// the one place they are written down -- and writing them as literals is
	// what lets this test catch a live constant being pointed back at one.
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
	}
	seen := make(map[byte]string, len(types))
	for name, b := range types {
		if other, dup := seen[b]; dup {
			t.Fatalf("%s and %s are both %#x: one on-disk byte cannot name two "+
				"record layouts, which is exactly how a 0.4.x record came to be "+
				"read as a 0.5.x one", name, other, b)
		}
		seen[b] = name
		// 0x00 is what every record decodes to when a KEYED WAL is opened with
		// no barrier, and replayRecord's default case leans on that. Allocating
		// it would turn a fail-stop into a plausible record.
		if b == 0x00 {
			t.Fatalf("%s is 0x00, which is reserved as the no-barrier misdecode "+
				"sentinel", name)
		}
	}
}

// TestEntryRecordIsWrittenWithCurrentPayloadType pins the WRITE side of the
// entry rename, against the bytes on disk.
//
// Pointing the reader at a new byte while the writer kept stamping the old one
// would make every newly written WAL unreadable by the build that wrote it, and
// nothing shows it until a restart — so the assertion is on the segment, not on
// the constants. The snapshot write site has the same assertion already, in
// TestSnapshotRecordCarriesMembershipVersionByte.
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

// TestEntryAndSnapshotRoundTripAcrossReopen is the positive control for the
// whole file: the new payload bytes are written, read back, and every field
// survives a close and a replay.
//
// Without it the gates above would pass just as well against a build that
// refused everything, including its own records.
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
			ConfigChangeID:       7,
			Addresses:            map[uint64]string{1: "quic://a:1"},
			SessionExpiryEntries: 100,
			MaxTotalSessions:     200,
			MaxSessionsPerClient: 300,
			MaxSessionsPerOwner:  400,
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
		gotSnap.Membership.MaxSessionsPerClient != wantSnap.Membership.MaxSessionsPerClient ||
		gotSnap.Membership.MaxSessionsPerOwner != wantSnap.Membership.MaxSessionsPerOwner {
		t.Fatalf("snapshot = %+v, want %+v", gotSnap, wantSnap)
	}
}

// TestPayloadTypesAreThePublishedBytes pins each live payload type to the
// literal byte 0.5.1-alpha wrote, which is the half TestPayloadTypesAreDistinct
// cannot cover.
//
// Distinctness only refuses a constant pointed back at a RETIRED byte. Moving
// one onto a fresh unallocated byte — 0x0d, say — keeps every constant distinct,
// keeps writer and reader agreeing with each other, and passes
// TestEntryRecordIsWrittenWithCurrentPayloadType, because that test looks the
// record up BY the constant. What it breaks is the thing no in-process test
// touches: a WAL written by a released build is refused by the next one with
// ErrUnknownPayloadType, and the shard cannot start.
//
// The expected values are written as literals rather than derived from the
// constants for the reason the retired list is: a fixture built from the
// constant follows it wherever it moves and can never fail.
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
		{"payloadTypeSnapshot", payloadTypeSnapshot, 0x0c},
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
