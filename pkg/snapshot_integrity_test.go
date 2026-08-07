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

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// FileSize integrity: a snapshot record's FileSize must describe the file at
// its Filepath. A zero FileSize beside a non-empty Filepath is what makes a
// later leader stream a metadata-only (EMPTY) snapshot, which the receiving
// follower recovers from and then marks every index <= snapshotIndex applied:
// silent, permanent state loss.
// ---------------------------------------------------------------------------

// onDiskSnapshotSize returns the size of snapshot.dat inside a snapshot
// directory.
func onDiskSnapshotSize(t *testing.T, dir string) uint64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("stat snapshot.dat: %v", err)
	}
	return uint64(fi.Size())
}

// TestHandleSnapshot_PersistsOnDiskFileSize verifies the receive path records
// the size of the artifact it just wrote. The encrypted case is the one that
// matters most: the on-disk file is LARGER than the plaintext wire bytes, so
// recording either 0 or the wire total makes FileSize disagree with the file
// the send path will later read.
func TestHandleSnapshot_PersistsOnDiskFileSize(t *testing.T) {
	cases := []struct {
		name      string
		encrypted bool
	}{
		{name: "plaintext"},
		{name: "encrypted", encrypted: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng, h, handler := newMedlowHandlerFixture(t)
			if tc.encrypted {
				h.barrier = createTestBarrier(t)
			}

			const shardID, localReplicaID, fromReplicaID = uint64(61), uint64(1), uint64(2)
			const snapIndex = uint64(910)
			authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

			payload := bytes.Repeat([]byte("snapshot-body"), 64)
			chunks := []proto.SnapshotChunk{{
				ShardID: shardID, ReplicaID: localReplicaID, From: fromReplicaID,
				Index: snapIndex, Term: 6, ChunkID: 0, ChunkCount: 1, Data: payload,
			}}
			if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
				t.Fatalf("HandleSnapshot: %v", err)
			}

			ss, err := h.logdb.GetSnapshot(shardID, localReplicaID)
			if err != nil {
				t.Fatalf("GetSnapshot: %v", err)
			}
			want := onDiskSnapshotSize(t, ss.Filepath)
			if want == 0 {
				t.Fatal("snapshot.dat is empty; the test cannot distinguish a missing FileSize")
			}
			if ss.FileSize != want {
				t.Fatalf("persisted FileSize = %d, want %d (the actual snapshot.dat size); "+
					"a zero/short FileSize makes this replica stream an empty snapshot as leader",
					ss.FileSize, want)
			}
		})
	}
}

// TestHandleSnapshot_DedupReDeliveryCannotSendEmptySnapshot is the end-to-end
// regression for the critical chain. The first delivery's InstallSnapshot is
// refused (a persistently full inbox), so the artifacts persist but raft never
// installed them; the leader's re-send then takes the DEDUP branch, which used
// to rebuild the install from the persisted record's FileSize — 0, because the
// receive path never set it — while pointing at a non-empty file. The
// resulting LogReader snapshot ({Filepath: set, FileSize: 0}) made this node
// stream a single metadata-only chunk once it became leader, erasing the
// receiving follower's state.
func TestHandleSnapshot_DedupReDeliveryCannotSendEmptySnapshot(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	h.barrier = createTestBarrier(t)

	const shardID, localReplicaID, fromReplicaID = uint64(62), uint64(1), uint64(2)
	const snapIndex = uint64(1200)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	payload := bytes.Repeat([]byte("dedup-regression-body"), 128)
	chunks := []proto.SnapshotChunk{{
		ShardID: shardID, ReplicaID: localReplicaID, From: fromReplicaID,
		Index: snapIndex, Term: 9, ChunkID: 0, ChunkCount: 1, Data: payload,
	}}

	// First delivery: the install is refused, so the receive fails while the
	// artifacts and the LogDB record survive for the leader's retry.
	refusing := &refusingInstallDeliverer{engineDeliverer: eng}
	first := &hostMessageHandler{engine: refusing, host: h}
	if err := first.HandleSnapshot(chunks, authorizeAllSenders); err == nil {
		t.Fatal("first HandleSnapshot reported success despite a refused install")
	}

	// Leader re-send: the dedup branch answers it from the persisted snapshot.
	capEng := &capturingEngineDeliverer{engineDeliverer: eng}
	second := &hostMessageHandler{engine: capEng, host: h}
	if err := second.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("dedup re-delivery failed: %v", err)
	}

	install, ok := capEng.firstInstallSnapshot()
	if !ok {
		t.Fatal("dedup branch delivered no InstallSnapshot")
	}
	if install.Snapshot.FileSize == 0 {
		t.Fatal("re-delivered InstallSnapshot carries FileSize 0 beside a persisted artifact; " +
			"raft would advertise an empty snapshot and this node would stream nothing as leader")
	}
	if filepath.Base(install.Snapshot.Filepath) != "snapshot.dat" {
		t.Fatalf("re-delivered Filepath = %q, want the snapshot.dat file (not its directory)",
			install.Snapshot.Filepath)
	}
	wantSize := onDiskSnapshotSize(t, filepath.Dir(install.Snapshot.Filepath))
	if install.Snapshot.FileSize != wantSize {
		t.Fatalf("re-delivered FileSize = %d, want %d (the on-disk size)",
			install.Snapshot.FileSize, wantSize)
	}

	// The node must not be able to send an empty snapshot from that record:
	// streaming it yields the whole artifact, not a metadata-only chunk.
	var streamed uint64
	err := forEachSnapshotChunk(h.snapshotFsys(), install, func(c proto.SnapshotChunk) error {
		streamed += uint64(len(c.Data))
		return nil
	})
	if err != nil {
		t.Fatalf("forEachSnapshotChunk on the re-delivered snapshot: %v", err)
	}
	if streamed != wantSize {
		t.Fatalf("streamed %d bytes, want %d; this node would ship an incomplete snapshot",
			streamed, wantSize)
	}
}

// TestForEachSnapshotChunk_DeclaredZeroSizeWithDataFails pins finding 2: a
// declared size of zero beside a NON-EMPTY file is a metadata/artifact
// disagreement, not an empty snapshot. Shipping a metadata-only chunk for it
// is what amplifies a bad FileSize into follower state erasure, so the send
// must fail with ErrSnapshotSizeMismatch instead.
func TestForEachSnapshotChunk_DeclaredZeroSizeWithDataFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, []byte("real snapshot bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    2,
		To:      1,
		Snapshot: proto.Snapshot{
			Index:    50,
			Term:     3,
			Filepath: path,
			FileSize: 0,
		},
	}

	var emitted int
	err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(proto.SnapshotChunk) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatalf("forEachSnapshotChunk emitted %d chunk(s) for a zero declared size beside a "+
			"non-empty file; the receiving follower would recover from an EMPTY snapshot", emitted)
	}
	if !errors.Is(err, ErrSnapshotSizeMismatch) {
		t.Fatalf("error = %v, want ErrSnapshotSizeMismatch", err)
	}
	if emitted != 0 {
		t.Fatalf("emitted %d chunk(s) before failing; nothing may reach the wire", emitted)
	}
}

// TestForEachSnapshotChunk_GenuinelyEmptyFile is the negative of the test
// above: when the declared size and the file AGREE at zero there is nothing to
// stream, and the metadata-only chunk is correct.
func TestForEachSnapshotChunk_GenuinelyEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	msg := proto.Message{
		Type:     proto.InstallSnapshot,
		ShardID:  1,
		From:     2,
		To:       1,
		Snapshot: proto.Snapshot{Index: 50, Term: 3, Filepath: path, FileSize: 0},
	}

	var chunks []proto.SnapshotChunk
	if err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(c proto.SnapshotChunk) error {
		chunks = append(chunks, c)
		return nil
	}); err != nil {
		t.Fatalf("forEachSnapshotChunk: %v", err)
	}
	if len(chunks) != 1 || chunks[0].ChunkCount != 1 || len(chunks[0].Data) != 0 {
		t.Fatalf("chunks = %+v, want a single metadata-only chunk", chunks)
	}
}

// ---------------------------------------------------------------------------
// Compression is a property of the ARTIFACT
// ---------------------------------------------------------------------------

// TestExportImportSnapshot_CrossCompressionConfig pins finding 4 across hosts.
// The exported artifact is decrypted but NEVER decompressed, so the header
// must carry how it is encoded. Deriving that from the destination's own
// SnapshotCompressionType — as both recovery paths used to — feeds the state
// machine Snappy frames (or, in reverse, tries to decompress plain bytes) on
// every restore between hosts configured differently.
func TestExportImportSnapshot_CrossCompressionConfig(t *testing.T) {
	cases := []struct {
		name string
		src  snapshotVariant
		dst  config.CompressionType
	}{
		{
			name: "snappy_source_uncompressed_destination",
			src:  snapshotVariant{name: "plaintext_snappy", snappy: true},
			dst:  config.NoCompression,
		},
		{
			name: "uncompressed_source_snappy_destination",
			src:  snapshotVariant{name: "plaintext_uncompressed"},
			dst:  config.Snappy,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte("cross-config export/import payload")

			srcDB := newMockLogDB()
			src, err := NewHost(testHostConfig(t), WithLogDB(srcDB), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost(src): %v", err)
			}
			defer src.Close()
			if startErr := src.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				testCreateFunc, tc.src.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard(src): %v", startErr)
			}
			seedLocalSnapshot(t, srcDB, tc.src, nil, 1, 1, 2100, 7, payload)

			var exported bytes.Buffer
			if expErr := src.ExportSnapshot(context.Background(), 1, &exported); expErr != nil {
				t.Fatalf("ExportSnapshot: %v", expErr)
			}

			dstDB := newMockLogDB()
			dstDB.nodeInfos = []logdb.NodeInfo{{ShardID: 1, ReplicaID: 1}}
			dst, err := NewHost(testHostConfig(t), WithLogDB(dstDB), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost(dst): %v", err)
			}
			defer dst.Close()

			if impErr := dst.ImportSnapshot(context.Background(), 1, bytes.NewReader(exported.Bytes()), WithImportDataDir("")); impErr != nil {
				t.Fatalf("ImportSnapshot: %v", impErr)
			}
			imported, getErr := dstDB.GetSnapshot(1, 1)
			if getErr != nil {
				t.Fatalf("GetSnapshot(dst): %v", getErr)
			}
			if imported.Compression != tc.src.compression() {
				t.Fatalf("imported Compression = %v, want %v (the artifact's own encoding)",
					imported.Compression, tc.src.compression())
			}

			// The destination shard is configured with the OPPOSITE setting.
			dstCfg := testShardConfig(1, 1)
			dstCfg.SnapshotCompressionType = tc.dst
			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := dst.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				create, dstCfg); startErr != nil {
				t.Fatalf("StartShard(dst) after a cross-config import: %v", startErr)
			}
			if !bytes.Equal(memSM.recovered, payload) {
				t.Fatalf("restored state machine recovered %q, want %q", memSM.recovered, payload)
			}
		})
	}
}

// TestSnapshotArtifactHeader_RejectsLegacyVersion verifies an artifact
// produced before the Compression field is rejected LOUDLY rather than
// assumed uncompressed.
func TestSnapshotArtifactHeader_RejectsLegacyVersion(t *testing.T) {
	hdr, err := marshalSnapshotArtifactHeader(snapshotArtifactHeader{Index: 5, Term: 2})
	if err != nil {
		t.Fatalf("marshalSnapshotArtifactHeader: %v", err)
	}
	legacy := append([]byte(nil), hdr...)
	legacy[8] = byte(snapshotArtifactVersion - 1)

	_, err = readSnapshotArtifactHeader(bytes.NewReader(legacy))
	var artErr *SnapshotArtifactError
	if !errors.As(err, &artErr) {
		t.Fatalf("error = %T (%v), want *SnapshotArtifactError", err, err)
	}
}

// TestSnapshotArtifactHeader_RejectsUnknownCompression verifies an artifact
// declaring an algorithm this binary cannot decompress is rejected instead of
// read as plain bytes.
func TestSnapshotArtifactHeader_RejectsUnknownCompression(t *testing.T) {
	hdr, err := marshalSnapshotArtifactHeader(snapshotArtifactHeader{
		Index:       5,
		Term:        2,
		Compression: config.CompressionType(0xBEEF),
	})
	if err != nil {
		t.Fatalf("marshalSnapshotArtifactHeader: %v", err)
	}
	_, err = readSnapshotArtifactHeader(bytes.NewReader(hdr))
	var artErr *SnapshotArtifactError
	if !errors.As(err, &artErr) {
		t.Fatalf("error = %T (%v), want *SnapshotArtifactError", err, err)
	}
}
