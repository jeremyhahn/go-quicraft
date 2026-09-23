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
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// mustParseSnapshotMeta decodes a snapshot.meta image or fails the test. Tests
// assert through the shared decoder rather than re-deriving field offsets, so a
// layout change cannot leave a test passing against bytes the production
// purge-floor scan can no longer read.
func mustParseSnapshotMeta(t *testing.T, data []byte) SnapshotMeta {
	t.Helper()
	meta, err := ParseSnapshotMeta(data)
	if err != nil {
		t.Fatalf("ParseSnapshotMeta(%d bytes) failed: %v", len(data), err)
	}
	return meta
}

// legacyMetaRecord builds a pre-magic snapshot.meta image of the given size,
// exactly as the builds that predate the self-describing format wrote it.
func legacyMetaRecord(size int, index, term, shardID, replicaID, createdAt, epoch uint64,
	compression config.CompressionType) []byte {
	buf := make([]byte, size)
	put := func(off int, v uint64) {
		if off+8 <= size {
			binary.LittleEndian.PutUint64(buf[off:], v)
		}
	}
	put(0, index)
	put(8, term)
	put(16, shardID)
	put(24, replicaID)
	put(32, createdAt)
	put(40, epoch)
	put(48, uint64(compression))
	return buf
}

// TestSnapshotMeta_RoundTrip pins the record this binary writes: it is
// self-describing (magic + version + body length) and every field survives a
// round trip.
func TestSnapshotMeta_RoundTrip(t *testing.T) {
	in := SnapshotMeta{
		Index:       900,
		Term:        7,
		ShardID:     12,
		ReplicaID:   3,
		CreatedAt:   1700000000,
		Epoch:       5,
		Compression: config.Snappy,
	}
	data := MarshalSnapshotMeta(in)

	if len(data) != snapshotMetaSize {
		t.Fatalf("record size = %d, want %d", len(data), snapshotMetaSize)
	}
	if !bytes.HasPrefix(data, []byte(snapshotMetaMagic)) {
		t.Fatalf("record does not start with the magic %q: %x", snapshotMetaMagic, data[:8])
	}
	if got := binary.LittleEndian.Uint16(data[snapshotMetaVersionOff:]); got != snapshotMetaVersion {
		t.Fatalf("record version = %d, want %d", got, snapshotMetaVersion)
	}
	if got := binary.LittleEndian.Uint32(data[snapshotMetaBodyLenOff:]); got != snapshotMetaBodySize {
		t.Fatalf("record body length = %d, want %d", got, snapshotMetaBodySize)
	}

	out := mustParseSnapshotMeta(t, data)
	if out.Index != in.Index || out.Term != in.Term || out.ShardID != in.ShardID ||
		out.ReplicaID != in.ReplicaID || out.CreatedAt != in.CreatedAt ||
		out.Epoch != in.Epoch || out.Compression != in.Compression {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
	if out.Version != snapshotMetaVersion {
		t.Fatalf("parsed version = %d, want %d", out.Version, snapshotMetaVersion)
	}
	if !out.EpochRecorded || !out.CompressionRecorded {
		t.Fatalf("parsed record does not report its epoch/compression as recorded: %+v", out)
	}
}

// TestSnapshotMeta_LegacyCompatibility is the migration contract: every record
// an earlier build could have written must still be readable, and a record that
// predates the Epoch field must report the epoch as ABSENT rather than as 0.
//
// Absent and 0 are opposite in consequence. 0 means "this artifact is
// plaintext, it pins no DEK"; absent means "this artifact may be ciphertext at
// an epoch nothing on disk names". Reading the second as the first is what
// would let PurgeEpochsBefore destroy a live DEK.
func TestSnapshotMeta_LegacyCompatibility(t *testing.T) {
	tests := []struct {
		name            string
		size            int
		epoch           uint64
		compression     config.CompressionType
		wantEpoch       uint64
		wantEpochField  bool
		wantCompression bool
	}{
		{
			// Written by the snapshot RECEIVE and IMPORT paths before the epoch
			// was recorded on disk. Those builds re-encrypted the artifact under
			// the receiver's own barrier, so the directory is frequently
			// ciphertext at an unnamed epoch.
			name: "40_byte_receive_path_record", size: legacyMetaSizeNoEpoch,
		},
		{
			name: "48_byte_save_path_record", size: legacyMetaSizeEpoch,
			epoch: 4, wantEpoch: 4, wantEpochField: true,
		},
		{
			name: "56_byte_save_path_record", size: legacyMetaSizeCompression,
			epoch: 6, compression: config.Snappy, wantEpoch: 6,
			wantEpochField: true, wantCompression: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := legacyMetaRecord(tt.size, 900, 7, 12, 3, 1700000000, tt.epoch, tt.compression)
			meta := mustParseSnapshotMeta(t, data)

			if meta.Version != 0 {
				t.Errorf("legacy record parsed as version %d, want 0", meta.Version)
			}
			if meta.Index != 900 || meta.Term != 7 || meta.ShardID != 12 || meta.ReplicaID != 3 {
				t.Errorf("legacy identity fields = %+v, want index=900 term=7 shard=12 replica=3", meta)
			}
			if meta.EpochRecorded != tt.wantEpochField {
				t.Fatalf("EpochRecorded = %v, want %v", meta.EpochRecorded, tt.wantEpochField)
			}
			if meta.Epoch != tt.wantEpoch {
				t.Errorf("Epoch = %d, want %d", meta.Epoch, tt.wantEpoch)
			}
			if meta.CompressionRecorded != tt.wantCompression {
				t.Errorf("CompressionRecorded = %v, want %v", meta.CompressionRecorded, tt.wantCompression)
			}
		})
	}
}

// TestSnapshotMeta_VersionSkew proves the property the magic/version prefix was
// added for: a reader can tell a field that is ABSENT (a different format
// version) from bytes that are CORRUPT. Without the prefix these were the same
// observation — "the file is shorter than I expect" — which is why appending a
// field silently turned every older directory into an unevaluable one.
func TestSnapshotMeta_VersionSkew(t *testing.T) {
	// A record written by a NEWER version that appended an 8th body field.
	// Fields are append-only, so every field this binary knows is still at its
	// own offset and must be read normally.
	t.Run("newer_version_with_appended_field", func(t *testing.T) {
		body := make([]byte, snapshotMetaBodySize+8)
		binary.LittleEndian.PutUint64(body[metaBodyIndexOff:], 900)
		binary.LittleEndian.PutUint64(body[metaBodyEpochOff:], 11)
		binary.LittleEndian.PutUint64(body[metaBodyCompressionOff:], uint64(config.Snappy))
		binary.LittleEndian.PutUint64(body[snapshotMetaBodySize:], 0xFEED) // the future field
		data := versionedMetaRecord(snapshotMetaVersion+1, body)

		meta := mustParseSnapshotMeta(t, data)
		if !meta.EpochRecorded || meta.Epoch != 11 {
			t.Fatalf("epoch from a newer record = (%d, recorded=%v), want (11, true)",
				meta.Epoch, meta.EpochRecorded)
		}
		if meta.Compression != config.Snappy {
			t.Errorf("compression from a newer record = %d, want %d", meta.Compression, config.Snappy)
		}
	})

	// A record written by an OLDER version of the versioned format, whose body
	// stops before the Epoch field. It is RECOGNISED — not corrupt — and
	// reports the epoch as absent.
	t.Run("older_version_without_epoch", func(t *testing.T) {
		body := make([]byte, metaBodyCreatedAtOff+8)
		binary.LittleEndian.PutUint64(body[metaBodyIndexOff:], 900)
		data := versionedMetaRecord(1, body)

		meta := mustParseSnapshotMeta(t, data)
		if meta.EpochRecorded {
			t.Fatal("an older versioned record reported an epoch it does not carry")
		}
		if meta.Index != 900 {
			t.Errorf("index = %d, want 900", meta.Index)
		}
	})
}

// versionedMetaRecord frames body as a versioned snapshot.meta record.
func versionedMetaRecord(version uint16, body []byte) []byte {
	buf := make([]byte, snapshotMetaHeaderSize+len(body))
	copy(buf, snapshotMetaMagic)
	binary.LittleEndian.PutUint16(buf[snapshotMetaVersionOff:], version)
	binary.LittleEndian.PutUint32(buf[snapshotMetaBodyLenOff:], uint32(len(body)))
	copy(buf[snapshotMetaHeaderSize:], body)
	return buf
}

// TestSnapshotMeta_CorruptRejected verifies that bytes which are neither a
// versioned record nor one of the recognised pre-magic layouts are REJECTED.
// Parsing them positionally would hand a fabricated epoch to the purge floor,
// which is worse than refusing to purge.
func TestSnapshotMeta_CorruptRejected(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "torn_legacy_write", data: make([]byte, 44)},
		{name: "legacy_size_plus_one", data: make([]byte, legacyMetaSizeCompression+1)},
		{name: "truncated_versioned_header", data: []byte(snapshotMetaMagic)[:4]},
		{
			name: "versioned_body_shorter_than_declared",
			data: func() []byte {
				d := MarshalSnapshotMeta(SnapshotMeta{Index: 1, Epoch: 3})
				return d[:len(d)-8] // body length still claims the full body
			}(),
		},
		{
			name: "versioned_body_below_identity_fields",
			data: versionedMetaRecord(snapshotMetaVersion, make([]byte, 8)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseSnapshotMeta(tt.data); !errors.Is(err, ErrSnapshotMetaCorrupt) {
				t.Fatalf("ParseSnapshotMeta(%d bytes) error = %v, want ErrSnapshotMetaCorrupt",
					len(tt.data), err)
			}
		})
	}
}

// TestReadSnapshotEpoch_Statuses pins the tri-state contract the purge floor
// depends on. The two failing statuses have identical consequences (do not
// purge) and differ only in what they tell an operator, but neither may ever
// be reported as "known epoch 0".
func TestReadSnapshotEpoch_Statuses(t *testing.T) {
	tests := []struct {
		name       string
		write      func(t *testing.T, dir string)
		wantEpoch  uint64
		wantStatus snapshotEpochStatus
	}{
		{
			name: "current_record",
			write: func(t *testing.T, dir string) {
				t.Helper()
				if err := writeSnapshotMetadata(dir, 1, 1, 1, 1, 9, config.NoCompression); err != nil {
					t.Fatalf("writeSnapshotMetadata: %v", err)
				}
			},
			wantEpoch: 9, wantStatus: snapshotEpochKnown,
		},
		{
			name: "current_record_plaintext_artifact",
			write: func(t *testing.T, dir string) {
				t.Helper()
				if err := writeSnapshotMetadata(dir, 1, 1, 1, 1, 0, config.NoCompression); err != nil {
					t.Fatalf("writeSnapshotMetadata: %v", err)
				}
			},
			wantEpoch: 0, wantStatus: snapshotEpochKnown,
		},
		{
			name: "legacy_record_carrying_epoch",
			write: func(t *testing.T, dir string) {
				t.Helper()
				writeMetaFile(t, dir, legacyMetaRecord(legacyMetaSizeEpoch, 1, 1, 1, 1, 1, 4, 0))
			},
			wantEpoch: 4, wantStatus: snapshotEpochKnown,
		},
		{
			name: "legacy_record_without_epoch",
			write: func(t *testing.T, dir string) {
				t.Helper()
				writeMetaFile(t, dir, legacyMetaRecord(legacyMetaSizeNoEpoch, 1, 1, 1, 1, 1, 0, 0))
			},
			wantEpoch: 0, wantStatus: snapshotEpochLegacy,
		},
		{
			name: "corrupt_record",
			write: func(t *testing.T, dir string) {
				t.Helper()
				writeMetaFile(t, dir, make([]byte, 44))
			},
			wantEpoch: 0, wantStatus: snapshotEpochUnknown,
		},
		{
			// No record AND no artifact: the directory was garbage-collected
			// between enumeration and this read, so there is nothing to protect.
			name:      "missing_record_no_artifact",
			write:     func(t *testing.T, _ string) { t.Helper() },
			wantEpoch: 0, wantStatus: snapshotEpochKnown,
		},
		{
			// No record but the ARTIFACT is still there — a crash between the
			// two writes. Its epoch is unnameable, which can only mean "do not
			// purge"; it can never mean "imposes no floor".
			name: "missing_record_artifact_present",
			write: func(t *testing.T, dir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, snapshotDataFile),
					[]byte("ciphertext"), 0o600); err != nil {
					t.Fatalf("write artifact: %v", err)
				}
			},
			wantEpoch: 0, wantStatus: snapshotEpochUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.write(t, dir)
			epoch, status := readSnapshotEpoch(dir)
			if epoch != tt.wantEpoch || status != tt.wantStatus {
				t.Fatalf("readSnapshotEpoch = (%d, %v), want (%d, %v)",
					epoch, status, tt.wantEpoch, tt.wantStatus)
			}
		})
	}
}

// writeMetaFile writes raw bytes as a snapshot directory's metadata file.
func writeMetaFile(t *testing.T, dir string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, snapshotMetadataFile), data, 0o600); err != nil {
		t.Fatalf("write metadata file: %v", err)
	}
}

// writeLegacyShardSnapshot creates a snapshot directory whose metadata is a
// pre-magic record of the given size.
func writeLegacyShardSnapshot(t *testing.T, baseDir string, shardID, replicaID, index uint64, size int) {
	t.Helper()
	dir := snapshotSaveDir(baseDir, shardID, replicaID, index)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeMetaFile(t, dir, legacyMetaRecord(size, index, 1, shardID, replicaID, 1, 0, 0))
}

// TestSnapshotFloorScan_ReportsEveryBlockingDirectory pins the diagnostic half
// of the fix. The scan's VERDICT is settled by the first directory it cannot
// evaluate, but it must still visit the rest: returning early meant the
// operator log named one directory, and the next scan named the next, so a host
// with several legacy directories never learned what was actually holding key
// purging off.
func TestSnapshotFloorScan_ReportsEveryBlockingDirectory(t *testing.T) {
	baseDir := t.TempDir()

	// Shard 1 retains TWO unevaluable directories (so a scan that stops at the
	// first one within a shard is caught) and one good one; shard 2 is fully
	// evaluable (so a scan that stops at the first BLOCKED shard is caught
	// too, by the minimum). Both good directories must still be folded in.
	writeLegacyShardSnapshot(t, baseDir, 1, 1, 100, legacyMetaSizeNoEpoch)
	writeLegacyShardSnapshot(t, baseDir, 1, 1, 150, legacyMetaSizeNoEpoch)
	writeShardSnapshot(t, baseDir, 1, 1, 200, 6)
	writeShardSnapshot(t, baseDir, 2, 1, 200, 4)

	var scan snapshotFloorScan
	scan.addNode(snapshotDirNode(baseDir, 1, 1))
	scan.addNode(snapshotDirNode(baseDir, 2, 1))

	if !scan.blocked() {
		t.Fatal("scan is not blocked although two directories have an indeterminate epoch")
	}
	if len(scan.blockers) != 2 {
		t.Fatalf("scan recorded %d blocking directories, want 2: the scan stopped at the "+
			"first one, so the operator log can only ever name one of them", len(scan.blockers))
	}
	for _, b := range scan.blockers {
		if b.reason != snapshotEpochLegacy.String() {
			t.Errorf("blocker %s reason = %q, want %q", b.path, b.reason, snapshotEpochLegacy)
		}
		if b.shardID == 0 || b.replicaID == 0 {
			t.Errorf("blocker %s is not attributed to a shard/replica: %+v", b.path, b)
		}
	}
	if scan.minEpoch != 4 {
		t.Errorf("scan minEpoch = %d, want 4: evaluable directories must still be folded in",
			scan.minEpoch)
	}
}

// TestGlobalSafePurgeFloor_LegacyDirectoryFailsSafe verifies the outcome half:
// a directory whose epoch is unknown suppresses purging entirely. It may NOT be
// skipped as "imposes no floor" — it can hold ciphertext at any epoch, and
// purging below the other directories' minimum would destroy its DEK.
func TestGlobalSafePurgeFloor_LegacyDirectoryFailsSafe(t *testing.T) {
	baseDir := t.TempDir()
	barrier := newTestBarrier(9)
	ldb := newSnapshotTestLogDB()

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1))

	// An evaluable encrypted snapshot at epoch 8 alongside a legacy directory
	// whose epoch nothing on disk names.
	writeShardSnapshot(t, baseDir, 1, 1, 200, 8)
	writeLegacyShardSnapshot(t, baseDir, 1, 1, 100, legacyMetaSizeNoEpoch)

	if floor, ok := p.globalSafePurgeFloor(); ok {
		t.Fatalf("globalSafePurgeFloor = (%d, true) with an indeterminate directory present; "+
			"purging below it would destroy the DEK its artifact needs", floor)
	}
	p.purgeOldEpochs("", 1, 1)
	if barrier.PurgeCallCount() != 0 {
		t.Fatalf("purge ran %d times despite an indeterminate snapshot directory",
			barrier.PurgeCallCount())
	}
}

// TestPerBarrierPurgeFloor_LegacyDirectoryScopedToItsBarrier pins how far the
// blast radius of an unevaluable directory reaches. It suppresses purging for
// the barrier that could have sealed it — and for no other. A legacy directory
// belonging to tenant A must not stop tenant B's DEKs from being reclaimed.
//
// (In single-barrier mode there is no smaller scope: every shard shares one
// keyring, so suppression is host-wide by construction. See purgeOldEpochs.)
func TestPerBarrierPurgeFloor_LegacyDirectoryScopedToItsBarrier(t *testing.T) {
	baseDir := t.TempDir()
	bA := newTenantBarrier(1, 9)
	bB := newTenantBarrier(2, 9)

	ldb := newPerBarrierTestLogDB()
	ldb.floors[bA] = 5
	ldb.floors[bB] = 5

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	p.shardBarriers = func(shardID uint64) SnapshotBarrier {
		if shardID == 1 {
			return bA
		}
		return bB
	}
	wireSnapshotNodes(p, snapshotDirNode(baseDir, 1, 1), snapshotDirNode(baseDir, 2, 1))

	// Shard 1 (barrier A) holds a legacy directory; shard 2 (barrier B) is
	// fully evaluable.
	writeLegacyShardSnapshot(t, baseDir, 1, 1, 100, legacyMetaSizeNoEpoch)
	writeShardSnapshot(t, baseDir, 2, 1, 100, 7)

	if floor, ok := p.perBarrierSafePurgeFloor(bA); ok {
		t.Fatalf("barrier A floor = (%d, true) despite its own indeterminate directory", floor)
	}
	floorB, okB := p.perBarrierSafePurgeFloor(bB)
	if !okB {
		t.Fatal("barrier B has no indeterminate directory of its own, yet its floor is " +
			"unusable: one tenant's legacy directory is suppressing another tenant's purge")
	}
	if floorB != 5 {
		t.Fatalf("barrier B floor = %d, want 5 (min of its WAL floor 5 and snapshot epoch 7)", floorB)
	}

	p.purgeOldEpochs("", 1, 1)
	if bA.PurgeCallCount() != 0 {
		t.Fatalf("barrier A purged %d times despite an indeterminate directory",
			bA.PurgeCallCount())
	}
	p.purgeOldEpochs("", 2, 1)
	if bB.PurgeCallCount() == 0 {
		t.Fatal("barrier B never purged: an unrelated barrier's legacy directory disabled it")
	}
}

// TestGlobalSafePurgeFloor_CountsAStoppedShardInItsOwnBase is the Codex review
// finding #2, in the PRODUCTION layout: every shard has its own snapshot base
// (…/snapshots/shard-N), so a stopped shard's artifacts sit under a base no
// loaded node owns. The floor used to scan loaded bases only and came out at
// the loaded shard's epoch 8 — purging epoch 7, which the stopped shard's
// recorded snapshot needs, and making its restart fail closed on decrypt
// forever. The LogDB's record of that snapshot must hold the floor at 7.
func TestGlobalSafePurgeFloor_CountsAStoppedShardInItsOwnBase(t *testing.T) {
	root := t.TempDir()
	baseStopped := filepath.Join(root, "snapshots", "shard-1")
	baseLoaded := filepath.Join(root, "snapshots", "shard-2")
	barrier := newTestBarrier(9)
	ldb := newEpochAwareTestLogDB(8) // the live WAL is at epoch 8

	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, ldb, barrier, nil, nil, stopC, nil)

	// Shard 1 was stopped with an epoch-7 snapshot on disk and on record.
	writeShardSnapshot(t, baseStopped, 1, 1, 100, 7)
	if err := ldb.SaveSnapshot(1, 1, logdb.Snapshot{Index: 100, Epoch: 7}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	// Shard 2 is loaded, with an epoch-8 snapshot in its own base.
	writeShardSnapshot(t, baseLoaded, 2, 1, 200, 8)
	wireSnapshotNodes(p, snapshotDirNode(baseLoaded, 2, 1))
	ldb.nodeInfos = []logdb.NodeInfo{{ShardID: 1, ReplicaID: 1}, {ShardID: 2, ReplicaID: 1}}

	floor, ok := p.globalSafePurgeFloor()
	if !ok || floor != 7 {
		t.Fatalf("globalSafePurgeFloor = (%d, %v), want (7, true): the stopped shard's "+
			"recorded epoch-7 snapshot must hold the floor", floor, ok)
	}
}

// failingListLogDB fails ListNodeInfo: the LogDB cannot say which shards
// exist, so a stopped shard cannot be accounted.
type failingListLogDB struct{ *epochAwareTestLogDB }

func (failingListLogDB) ListNodeInfo() ([]logdb.NodeInfo, error) {
	return nil, errors.New("injected ListNodeInfo failure")
}

// TestGlobalSafePurgeFloor_DeclinesWhenShardsCannotBeEnumerated is the
// fail-safe half: an unknown shard set cannot be proven free of a stopped
// shard's snapshot, so nothing is purged.
func TestGlobalSafePurgeFloor_DeclinesWhenShardsCannotBeEnumerated(t *testing.T) {
	baseLoaded := filepath.Join(t.TempDir(), "snapshots", "shard-2")
	stopC := make(chan struct{})
	defer close(stopC)
	p := newSnapshotPool(2, failingListLogDB{newEpochAwareTestLogDB(8)}, newTestBarrier(9), nil, nil, stopC, nil)
	writeShardSnapshot(t, baseLoaded, 2, 1, 200, 8)
	wireSnapshotNodes(p, snapshotDirNode(baseLoaded, 2, 1))

	if floor, ok := p.globalSafePurgeFloor(); ok {
		t.Fatalf("globalSafePurgeFloor = (%d, true) although the LogDB could not enumerate "+
			"its shards; a stopped shard's DEK could be purged", floor)
	}
}
