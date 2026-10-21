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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/golden"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// The golden WAL inputs. Every map holds at most one entry: the bootstrap
// and membership encoders walk Go maps in iteration order, so a second entry
// would make the fixture depend on the runtime's map seed rather than on the
// layout.
var goldenKey = nodeKey{ShardID: 1, ReplicaID: 1}

func goldenWALEntries() []logdb.Entry {
	return []logdb.Entry{
		{Index: 1, Term: 1, Type: 0, Key: 11, ClientID: 22, SeriesID: 33, RespondedTo: 32, OwnerID: 55, Cmd: []byte("golden")},
		{Index: 2, Term: 1, Type: 1, Key: 12, Cmd: []byte{0xde, 0xad}},
	}
}

func goldenWALState() logdb.State {
	return logdb.State{Term: 7, Vote: 2, Commit: 40, VoteWindow: 1500000000}
}

func goldenWALSnapshot() logdb.Snapshot {
	return logdb.Snapshot{
		Index: 40, Term: 6, Filepath: "snapshot-0000000000000028", FileSize: 4096,
		OnDiskIndex: 39, Epoch: 9, Compression: 1,
		Membership: logdb.Membership{
			ConfigChangeID: 3, MaxWitnesses: 1, RequireFullReplicaMajority: true,
			SessionExpiryEntries: 100, MaxTotalSessions: 200, MaxResponsesPerSession: 3, MaxSessionsPerOwner: 4,
			Addresses:         map[uint64]string{1: "a:1"},
			Observers:         map[uint64]string{2: "b:2"},
			Witnesses:         map[uint64]string{3: "c:3"},
			Removed:           map[uint64]bool{4: true},
			Identities:        map[uint64]string{1: "node-a"},
			RemovedIdentities: map[uint64]string{4: "node-d"},
			WireVersion:       1,
		},
	}
}

func goldenWALBootstrap() logdb.Bootstrap {
	return logdb.Bootstrap{
		Addresses: map[uint64]string{1: "a:1"},
		Join:      true, Type: 2,
		DataDir: "/var/lib/quicraft/1", DataDirRecorded: true,
	}
}

// goldenWALRecord writes through the real DB write path and returns the raw
// record of the given payload type, so the fixture is what the writer emits.
func goldenWALRecord(t *testing.T, payloadType byte, write func(*DB) error) []byte {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := write(db); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return findRawShardRecord(t, dir, 1, payloadType)
}

func saveGoldenEntries(entries []logdb.Entry) func(*DB) error {
	return func(db *DB) error { return db.SaveEntries(goldenKey.ShardID, goldenKey.ReplicaID, entries) }
}

func saveGoldenState(st logdb.State) func(*DB) error {
	return func(db *DB) error { return db.SaveNodeState(goldenKey.ShardID, goldenKey.ReplicaID, st) }
}

func saveGoldenSnapshot(s logdb.Snapshot) func(*DB) error {
	return func(db *DB) error { return db.SaveSnapshot(goldenKey.ShardID, goldenKey.ReplicaID, s) }
}

func saveGoldenBootstrap(b logdb.Bootstrap) func(*DB) error {
	return func(db *DB) error { return db.SaveBootstrap(goldenKey.ShardID, goldenKey.ReplicaID, b) }
}

func saveGoldenCompaction(index uint64) func(*DB) error {
	return func(db *DB) error {
		if err := db.SaveEntries(goldenKey.ShardID, goldenKey.ReplicaID, goldenWALEntries()); err != nil {
			return err
		}
		return db.RemoveEntriesTo(goldenKey.ShardID, goldenKey.ReplicaID, index)
	}
}

// replayGolden decodes a record fixture through the replay path recovery uses.
func replayGolden(t *testing.T, name string) *walShard {
	t.Helper()
	s := newReplayShard()
	skipped, reason, err := s.replayRecord(golden.Load(t, name), 1)
	if err != nil {
		t.Fatalf("replayRecord %s: %v", name, err)
	}
	if skipped != 0 {
		t.Fatalf("replayRecord %s skipped %d (%s)", name, skipped, reason)
	}
	return s
}

func checkReencoded(t *testing.T, name string, got []byte) {
	t.Helper()
	if raw := golden.Load(t, name); !bytes.Equal(got, raw) {
		t.Fatalf("re-encoding the decoded %s changed its bytes:\n got  %x\n want %x", name, got, raw)
	}
}

// TestGoldenWALFormat pins the WAL-format marker file.
func TestGoldenWALFormat(t *testing.T) {
	dir := t.TempDir()
	if err := writeWALFormat(defaultFS(), dir, 1024, true); err != nil {
		t.Fatalf("writeWALFormat: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, walFormatFilename))
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	golden.Check(t, "walformat.bin", raw)

	dec := t.TempDir()
	if err := os.WriteFile(filepath.Join(dec, walFormatFilename), golden.Load(t, "walformat.bin"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	blockSize, present, err := readWALFormat(defaultFS(), dec)
	if err != nil || !present || blockSize != 1024 {
		t.Fatalf("readWALFormat = (%d, %v, %v), want (1024, true, nil)", blockSize, present, err)
	}
}

// TestGoldenChunk pins the 15-byte chunk header and its CRC.
func TestGoldenChunk(t *testing.T) {
	payload := []byte("golden chunk payload")
	buf := make([]byte, recordHeaderSize+len(payload))
	n, err := encodeRecord(buf, RecordFull, 0x01020304, 0x05060708, payload)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	golden.Check(t, "chunk.bin", buf[:n])

	raw := golden.Load(t, "chunk.bin")
	data, typ, durSeq, genStart, n, err := decodeRecord(raw)
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}
	if n != len(raw) || typ != RecordFull || durSeq != 0x01020304 || genStart != 0x05060708 || !bytes.Equal(data, payload) {
		t.Fatalf("decodeRecord = (%q, %d, %#x, %#x, %d), want (%q, %d, 0x1020304, 0x5060708, %d)",
			data, typ, durSeq, genStart, n, payload, RecordFull, len(raw))
	}
	out := make([]byte, len(raw))
	if _, err := encodeRecord(out, typ, durSeq, genStart, data); err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	checkReencoded(t, "chunk.bin", out)
}

// TestGoldenRecordEntries pins the entries record (payload type 0x0b).
func TestGoldenRecordEntries(t *testing.T) {
	in := goldenWALEntries()
	golden.Check(t, "record-entries.bin", goldenWALRecord(t, payloadTypeEntries, saveGoldenEntries(in)))

	s := replayGolden(t, "record-entries.bin")
	got := make([]logdb.Entry, 0, len(in))
	for _, e := range in {
		d, ok := s.entries[goldenKey][e.Index]
		if !ok {
			t.Fatalf("entry %d not replayed", e.Index)
		}
		got = append(got, d)
	}
	if len(s.entries[goldenKey]) != len(in) || !reflect.DeepEqual(got, in) {
		t.Fatalf("replayed entries = %+v, want %+v", s.entries[goldenKey], in)
	}
	checkReencoded(t, "record-entries.bin", goldenWALRecord(t, payloadTypeEntries, saveGoldenEntries(got)))
}

// TestGoldenRecordState pins the hard-state record (payload type 0x0d).
func TestGoldenRecordState(t *testing.T) {
	in := goldenWALState()
	golden.Check(t, "record-state.bin", goldenWALRecord(t, payloadTypeState, saveGoldenState(in)))

	got, ok := replayGolden(t, "record-state.bin").states[goldenKey]
	if !ok || got != in {
		t.Fatalf("replayed state = %+v (present %v), want %+v", got, ok, in)
	}
	checkReencoded(t, "record-state.bin", goldenWALRecord(t, payloadTypeState, saveGoldenState(got)))
}

// TestGoldenRecordSnapshot pins the snapshot record (payload type 0x0e) and
// its membership section.
func TestGoldenRecordSnapshot(t *testing.T) {
	in := goldenWALSnapshot()
	golden.Check(t, "record-snapshot.bin", goldenWALRecord(t, payloadTypeSnapshot, saveGoldenSnapshot(in)))

	got, ok := replayGolden(t, "record-snapshot.bin").snapshots[goldenKey]
	if !ok || !reflect.DeepEqual(got, in) {
		t.Fatalf("replayed snapshot = %+v (present %v), want %+v", got, ok, in)
	}
	checkReencoded(t, "record-snapshot.bin", goldenWALRecord(t, payloadTypeSnapshot, saveGoldenSnapshot(got)))
}

// TestGoldenRecordBootstrap pins the bootstrap record (payload type 0x04).
func TestGoldenRecordBootstrap(t *testing.T) {
	in := goldenWALBootstrap()
	golden.Check(t, "record-bootstrap.bin", goldenWALRecord(t, payloadTypeBootstrap, saveGoldenBootstrap(in)))

	got, ok := replayGolden(t, "record-bootstrap.bin").bootstraps[goldenKey]
	if !ok || !reflect.DeepEqual(got, in) {
		t.Fatalf("replayed bootstrap = %+v (present %v), want %+v", got, ok, in)
	}
	checkReencoded(t, "record-bootstrap.bin", goldenWALRecord(t, payloadTypeBootstrap, saveGoldenBootstrap(got)))
}

// TestGoldenRecordCompact pins the compaction record (payload type 0x05).
func TestGoldenRecordCompact(t *testing.T) {
	const in uint64 = 1
	golden.Check(t, "record-compact.bin", goldenWALRecord(t, payloadTypeCompact, saveGoldenCompaction(in)))

	got, ok := replayGolden(t, "record-compact.bin").compactedTo[goldenKey]
	if !ok || got != in {
		t.Fatalf("replayed compaction = %d (present %v), want %d", got, ok, in)
	}
	checkReencoded(t, "record-compact.bin", goldenWALRecord(t, payloadTypeCompact, saveGoldenCompaction(got)))
}

// TestGoldenRetiredRegistry pins the RETIRED registry file.
func TestGoldenRetiredRegistry(t *testing.T) {
	in := []nodeKey{{ShardID: 1, ReplicaID: 2}, {ShardID: 3, ReplicaID: 4}}
	golden.Check(t, "retired.bin", marshalRetiredRegistry(append([]nodeKey(nil), in...)))

	got, err := parseRetiredRegistry(golden.Load(t, "retired.bin"))
	if err != nil {
		t.Fatalf("parseRetiredRegistry: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("parsed registry = %+v, want %+v", got, in)
	}
	checkReencoded(t, "retired.bin", marshalRetiredRegistry(got))
}

// TestGoldenCompactionFloors pins the COMPACTED floor file.
func TestGoldenCompactionFloors(t *testing.T) {
	in := map[nodeKey]uint64{{ShardID: 1, ReplicaID: 2}: 5, {ShardID: 3, ReplicaID: 4}: 9}
	golden.Check(t, "floors.bin", marshalCompactionFloors(in))

	got, err := parseCompactionFloors(golden.Load(t, "floors.bin"))
	if err != nil {
		t.Fatalf("parseCompactionFloors: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("parsed floors = %+v, want %+v", got, in)
	}
	checkReencoded(t, "floors.bin", marshalCompactionFloors(got))
}
