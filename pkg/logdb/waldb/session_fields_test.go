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

// TestEntrySessionFieldsSurviveReopen verifies that the session fields
// (Key, ClientID, SeriesID, RespondedTo) survive a full WAL round-trip
// including process restart (Close + Open + replay). Losing them would
// silently break at-most-once delivery after crash recovery.
func TestEntrySessionFieldsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := []logdb.Entry{
		{
			Index:       1,
			Term:        2,
			Type:        0,
			Key:         0xAABBCCDD11223344,
			ClientID:    42,
			SeriesID:    7,
			RespondedTo: 6,
			Cmd:         []byte("session cmd"),
		},
		{
			Index:       2,
			Term:        2,
			Type:        0,
			Key:         99,
			ClientID:    0, // not session-managed
			SeriesID:    0,
			RespondedTo: 0,
			Cmd:         []byte("plain cmd"),
		},
	}
	if err := db.SaveEntries(3, 1, want); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Verify in-memory reads first.
	checkEntries := func(stage string, db *DB) {
		t.Helper()
		got, err := db.GetEntries(3, 1, 1, 3, 0)
		if err != nil {
			t.Fatalf("%s GetEntries: %v", stage, err)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: got %d entries, want %d", stage, len(got), len(want))
		}
		for i := range want {
			w, g := want[i], got[i]
			if g.Key != w.Key || g.ClientID != w.ClientID ||
				g.SeriesID != w.SeriesID || g.RespondedTo != w.RespondedTo {
				t.Fatalf("%s entry %d: session fields = (%d,%d,%d,%d), want (%d,%d,%d,%d)",
					stage, i, g.Key, g.ClientID, g.SeriesID, g.RespondedTo,
					w.Key, w.ClientID, w.SeriesID, w.RespondedTo)
			}
			if string(g.Cmd) != string(w.Cmd) {
				t.Fatalf("%s entry %d: cmd = %q, want %q", stage, i, g.Cmd, w.Cmd)
			}
		}
	}
	checkEntries("pre-restart", db)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: entries must be rebuilt from WAL replay with all fields.
	db2, err := Open(dir, WithNoSync(true), WithBlockSize(4096), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	checkEntries("post-restart", db2)
}

// TestLegacyEntryFormatRejected verifies that a WAL containing an entry
// record in a retired layout, under the retired payload type, fails recovery
// loudly instead of
// being misparsed or silently skipped.
func TestLegacyEntryFormatRejected(t *testing.T) {
	t.Run("Open fails on a segment with a retired-layout record", func(t *testing.T) {
		dir := t.TempDir()

		// Build a valid segment file in shard-0 containing one retired-layout record.
		shardDir := filepath.Join(dir, shardDirName(0))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		// Record the on-disk format the way a real first Open does, so the
		// legacy PAYLOAD type is what fails the open rather than the missing
		// marker. The two are different compatibility ladders: the marker
		// covers the chunk framing, the payload byte covers what is inside it.
		writeTestWALFormat(t, dir, 4096)
		seg, _, err := createSegment(defaultFS(), shardDir, 1, 4096, 64*1024)
		if err != nil {
			t.Fatalf("createSegment: %v", err)
		}
		payload := make([]byte, 1+44)
		payload[0] = 0x01                             // retired entry payload type, pinned as a literal
		binary.LittleEndian.PutUint64(payload[1:], 0) // shardID 0 -> shard-0
		binary.LittleEndian.PutUint64(payload[9:], 1) // replicaID
		if _, err := seg.Write(payload); err != nil {
			t.Fatalf("segment Write: %v", err)
		}
		if err := seg.Sync(); err != nil {
			t.Fatalf("segment Sync: %v", err)
		}
		if err := seg.Close(); err != nil {
			t.Fatalf("segment Close: %v", err)
		}

		_, err = Open(dir, WithNoSync(true), WithBlockSize(4096), WithMaxFileSize(64*1024))
		if err == nil {
			t.Fatal("Open succeeded on a WAL with legacy entry records, want error")
		}
		if !errors.Is(err, ErrUnknownPayloadType) {
			t.Fatalf("Open err = %v, want ErrUnknownPayloadType in chain", err)
		}
	})
}
