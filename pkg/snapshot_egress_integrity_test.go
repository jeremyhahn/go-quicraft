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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Egress integrity: a snapshot leaves the node only as the bytes its
// snapshot.meta recorded. Recovery checked that record; the transport send and
// ExportSnapshot did not, so a same-size bit flip in a retained plaintext
// artifact was streamed to a lagging follower, which stamped the damaged bytes
// with a fresh hash of its own and discarded the log the snapshot covers.
// ---------------------------------------------------------------------------

// writeRecordedSnapshot writes data as dir/snapshot.dat and a snapshot.meta
// beside it recording the length and hash of those bytes, as the save path
// does.
func writeRecordedSnapshot(t *testing.T, dir string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write snapshot.dat: %v", err)
	}
	stampSnapshotMeta(t, dir, 0)
	return path
}

// stampSnapshotMeta records the current length and hash of dir/snapshot.dat in
// dir/snapshot.meta, then XORs the recorded hash with hashSkew — a non-zero
// skew makes the record disagree with bytes that are themselves intact.
func stampSnapshotMeta(t *testing.T, dir string, hashSkew uint64) {
	t.Helper()
	data, err := os.Open(filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("open snapshot.dat: %v", err)
	}
	defer func() { _ = data.Close() }()
	sum, size, err := engine.HashSnapshotReader(data)
	if err != nil {
		t.Fatalf("hash snapshot.dat: %v", err)
	}
	meta := engine.MarshalSnapshotMeta(engine.SnapshotMeta{
		Index: 100, Term: 5, ShardID: 1, ReplicaID: 1,
		DataLen: size, DataHash: sum ^ hashSkew,
	})
	if err := os.WriteFile(filepath.Join(dir, "snapshot.meta"), meta, 0o600); err != nil {
		t.Fatalf("write snapshot.meta: %v", err)
	}
}

// flipSnapshotByte corrupts one byte of the file at path in place, leaving its
// size unchanged — the damage no size check can see.
func flipSnapshotByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	defer func() { _ = f.Close() }()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatalf("read byte %d: %v", off, err)
	}
	b[0] ^= 0x40
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatalf("write byte %d: %v", off, err)
	}
}

// egressInstallMsg is the InstallSnapshot the leader would stream for the
// artifact at path, addressed so the medlow handler fixture admits it.
func egressInstallMsg(t *testing.T, path string, shardID, to, from uint64) proto.Message {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return proto.Message{
		Type: proto.InstallSnapshot, ShardID: shardID, From: from, To: to,
		Snapshot: proto.Snapshot{
			Index: 1300, Term: 9, Filepath: path, FileSize: uint64(info.Size()),
			Membership: snapshotPathMembership(to, from),
		},
	}
}

// TestSnapshotSend_CorruptPlaintextWithholdsFinalChunk is the regression for
// the defect. The artifact is corrupted after it was recorded, by one byte and
// at the same size. The stream must fail with ErrSnapshotCorrupt having
// emitted every chunk BUT the last, and the chunks that did go out must not be
// installable: the receiver requires the full declared chunk count.
func TestSnapshotSend_CorruptPlaintextWithholdsFinalChunk(t *testing.T) {
	cases := []struct {
		name string
		size int
		off  int64
	}{
		// Damage in a chunk already on the wire when the check runs: only
		// the final chunk can still be withheld.
		{name: "multi_chunk", size: 2*snapshotChunkSize + 123, off: 17},
		// A one-chunk artifact: nothing at all may be emitted.
		{name: "single_chunk", size: 4096, off: 4000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng, h, handler := newMedlowHandlerFixture(t)
			const shardID, localReplicaID, fromReplicaID = uint64(71), uint64(1), uint64(2)
			authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

			path := writeRecordedSnapshot(t, t.TempDir(), bytes.Repeat([]byte("sm-state"), tc.size/8+1)[:tc.size])
			flipSnapshotByte(t, path, tc.off)
			msg := egressInstallMsg(t, path, shardID, localReplicaID, fromReplicaID)
			wantChunks := (msg.Snapshot.FileSize-1)/snapshotChunkSize + 1

			var emitted []proto.SnapshotChunk
			err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(c proto.SnapshotChunk) error {
				emitted = append(emitted, c)
				return nil
			})
			if !errors.Is(err, ErrSnapshotCorrupt) {
				t.Fatalf("streaming a corrupted artifact: err = %v, want ErrSnapshotCorrupt", err)
			}
			if uint64(len(emitted)) != wantChunks-1 {
				t.Fatalf("emitted %d of %d chunks; the final chunk must be withheld and only it",
					len(emitted), wantChunks)
			}

			if len(emitted) > 0 {
				if hsErr := handler.HandleSnapshot(emitted, authorizeAllSenders); hsErr == nil {
					t.Fatal("the receiver accepted a transfer missing its final chunk")
				}
			}
			if ss, getErr := h.logdb.GetSnapshot(shardID, localReplicaID); getErr == nil && ss.Index != 0 {
				t.Fatalf("the receiver recorded snapshot %d from a failed send", ss.Index)
			}
		})
	}
}

// TestSnapshotSend_IntactPlaintextIsDeliveredWhole is the positive control:
// an artifact that matches its record streams in full and installs.
func TestSnapshotSend_IntactPlaintextIsDeliveredWhole(t *testing.T) {
	eng, h, handler := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(72), uint64(1), uint64(2)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	payload := bytes.Repeat([]byte("intact-state"), (2*snapshotChunkSize+500)/12)
	path := writeRecordedSnapshot(t, t.TempDir(), payload)
	msg := egressInstallMsg(t, path, shardID, localReplicaID, fromReplicaID)

	var emitted []proto.SnapshotChunk
	var got []byte
	if err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(c proto.SnapshotChunk) error {
		emitted = append(emitted, c)
		got = append(got, c.Data...)
		return nil
	}); err != nil {
		t.Fatalf("streaming an intact artifact: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("streamed %d bytes, want the %d-byte artifact", len(got), len(payload))
	}
	if err := handler.HandleSnapshot(emitted, authorizeAllSenders); err != nil {
		t.Fatalf("receiver refused an intact transfer: %v", err)
	}
	ss, err := h.logdb.GetSnapshot(shardID, localReplicaID)
	if err != nil || ss.Index != msg.Snapshot.Index {
		t.Fatalf("receiver record = %+v, %v; want snapshot %d installed", ss, err, msg.Snapshot.Index)
	}
}

// TestSnapshotSend_MetadataOnlyStillSends pins that a witness's snapshot —
// no data file, so nothing recorded to verify — is still shipped as its single
// metadata-only chunk.
func TestSnapshotSend_MetadataOnlyStillSends(t *testing.T) {
	msg := proto.Message{
		Type: proto.InstallSnapshot, ShardID: 1, From: 2, To: 1,
		Snapshot: proto.Snapshot{Index: 50, Term: 3},
	}
	var chunks []proto.SnapshotChunk
	if err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(c proto.SnapshotChunk) error {
		chunks = append(chunks, c)
		return nil
	}); err != nil {
		t.Fatalf("metadata-only send: %v", err)
	}
	if len(chunks) != 1 || chunks[0].ChunkCount != 1 || len(chunks[0].Data) != 0 {
		t.Fatalf("chunks = %+v, want a single metadata-only chunk", chunks)
	}
}

// TestDoSendSnapshot_EncryptedArtifactHeldToItsRecord drives the real send
// with an encrypted artifact whose frames authenticate but whose record does
// not match the bytes on disk. The send must refuse it after the decrypt and
// before streaming anything: the refusal is logged as an integrity failure,
// and the transport — which would log its own failure — is never reached.
func TestDoSendSnapshot_EncryptedArtifactHeldToItsRecord(t *testing.T) {
	cases := []struct {
		name       string
		skew       uint64
		wantRefuse bool
	}{
		{name: "record_matches", skew: 0, wantRefuse: false},
		{name: "record_disagrees", skew: 1, wantRefuse: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "snapshot.dat")
			b := createTestBarrier(t)
			writeEncryptedSnapshotFileFor(t, b, path, []byte("sealed state machine"), 1, 1, 100)
			stampSnapshotMeta(t, dir, tc.skew)

			f := newSnapshotSendFixture(t, b)
			var logs bytes.Buffer
			f.sender.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			msg := testInstallSnapshotMsg(t, path, b.CurrentEpoch(), 91)
			msg.ReplicaID = 1
			sendSnapshotSync(f.sender, msg)

			// Either way the leader is told the transfer was rejected: here
			// by the check, otherwise by the unresolvable test transport.
			f.expectSnapshotStatus(t, true, 91)
			refused := strings.Contains(logs.String(), "fails its recorded integrity check")
			reachedTransport := strings.Contains(logs.String(), "snapshot send failed")
			if refused != tc.wantRefuse || reachedTransport == tc.wantRefuse {
				t.Fatalf("refused by the check = %v, reached the transport = %v; want refused = %v\n%s",
					refused, reachedTransport, tc.wantRefuse, logs.String())
			}
			f.assertNoTempFiles(t, dir)
		})
	}
}

// TestExportSnapshot_CorruptPlaintextFails verifies ExportSnapshot checks
// the artifact it reads against its record: a same-size corruption fails the
// export with ErrSnapshotCorrupt, and — the artifact fitting in one copy
// buffer — nothing at all reaches the writer, header included. The intact
// artifact exports as before.
func TestExportSnapshot_CorruptPlaintextFails(t *testing.T) {
	cases := []struct {
		name    string
		size    int
		corrupt bool
	}{
		{name: "intact", size: 64 << 10},
		{name: "corrupt_small", size: 64 << 10, corrupt: true},
		{name: "intact_multi_buffer", size: 3*copyWithContextChunkSize + 7},
		{name: "corrupt_multi_buffer", size: 3*copyWithContextChunkSize + 7, corrupt: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newMockLogDB()
			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				testCreateFunc, testShardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard: %v", startErr)
			}

			dir := t.TempDir()
			payload := bytes.Repeat([]byte("exported-state"), tc.size/14+1)[:tc.size]
			path := writeRecordedSnapshot(t, dir, payload)
			if tc.corrupt {
				flipSnapshotByte(t, path, 3)
			}
			if saveErr := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 500, Term: 3, Filepath: dir}); saveErr != nil {
				t.Fatalf("SaveSnapshot: %v", saveErr)
			}

			var out bytes.Buffer
			expErr := h.ExportSnapshot(context.Background(), 1, &out)
			if !tc.corrupt {
				if expErr != nil {
					t.Fatalf("ExportSnapshot of an intact artifact: %v", expErr)
				}
				stripArtifactTrailer(t, &out)
				if !bytes.Equal(out.Bytes(), payload) {
					t.Fatalf("exported %d data bytes, want the %d-byte artifact", out.Len(), len(payload))
				}
				return
			}
			if !errors.Is(expErr, ErrSnapshotCorrupt) {
				t.Fatalf("ExportSnapshot of a corrupted artifact: err = %v, want ErrSnapshotCorrupt", expErr)
			}
			if tc.size <= copyWithContextChunkSize {
				if out.Len() != 0 {
					t.Fatalf("a failed export wrote %d bytes; nothing may reach the writer", out.Len())
				}
				return
			}
			// A larger artifact has streamed its leading buffers when the
			// check runs; its tail is withheld, so the writer never holds
			// the whole artifact.
			if out.Len() >= len(payload) {
				t.Fatalf("a failed export wrote %d bytes of a %d-byte artifact; its tail must be withheld",
					out.Len(), len(payload))
			}
		})
	}
}

// TestExportSnapshot_EncryptedArtifactHeldToItsRecord is the encrypted
// export's counterpart: frames that authenticate do not excuse a record that
// disagrees with the bytes on disk. The export refuses what a recovery of the
// same artifact would refuse.
func TestExportSnapshot_EncryptedArtifactHeldToItsRecord(t *testing.T) {
	for _, skew := range []uint64{0, 1} {
		t.Run(map[uint64]string{0: "record_matches", 1: "record_disagrees"}[skew], func(t *testing.T) {
			db := newMockLogDB()
			barrier := createTestBarrier(t)
			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithBarrier(barrier))
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				testCreateFunc, testShardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard: %v", startErr)
			}

			dir := t.TempDir()
			plaintext := []byte("sealed state machine for export")
			writeEncryptedSnapshotFileFor(t, barrier, filepath.Join(dir, "snapshot.dat"), plaintext, 1, 1, 700)
			stampSnapshotMeta(t, dir, skew)
			if saveErr := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 700, Filepath: dir, Epoch: barrier.CurrentEpoch()}); saveErr != nil {
				t.Fatalf("SaveSnapshot: %v", saveErr)
			}

			var out bytes.Buffer
			expErr := h.ExportSnapshot(context.Background(), 1, &out)
			if skew == 0 {
				if expErr != nil {
					t.Fatalf("ExportSnapshot of an intact encrypted artifact: %v", expErr)
				}
				return
			}
			if !errors.Is(expErr, ErrSnapshotCorrupt) {
				t.Fatalf("ExportSnapshot against a disagreeing record: err = %v, want ErrSnapshotCorrupt", expErr)
			}
		})
	}
}
