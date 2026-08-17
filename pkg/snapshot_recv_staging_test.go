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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// seedCanonicalSnapshot writes a complete artifact into the canonical
// snapshot-<index> directory through the fixture's filesystem, WITHOUT
// recording it in LogDB — the shape a transfer that wrote its files and then
// failed leaves behind, and the shape a same-index re-send routinely meets.
func seedCanonicalSnapshot(t *testing.T, f *recvFixture, index uint64, content []byte) string {
	t.Helper()
	dir := f.snapDir(index)
	if err := f.fs.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("seed MkdirAll: %v", err)
	}
	path := filepath.Join(dir, "snapshot.dat")
	fh, err := f.fs.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("seed OpenFile: %v", err)
	}
	if _, err := fh.Write(content); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	return path
}

// recvStageDirs lists the receive staging directories currently under the
// shard/replica base.
func recvStageDirs(f *recvFixture, index uint64) []string {
	var out []string
	for _, name := range f.fs.entries(filepath.Dir(f.snapDir(index))) {
		if engine.IsSnapshotRecvStage(name) {
			out = append(out, name)
		}
	}
	return out
}

// TestSnapshotReceive_FailedTransferDoesNotDestroyTheExistingArtifact is the
// regression guard for the destructive receive.
//
// The receive used to open snapshot.dat in the CANONICAL snapshot-<index>
// directory with O_CREATE|O_TRUNC, before a single received byte had been
// authenticated, and its failure defer then called RemoveAll on that same
// directory. Same-index re-sends are routine and supported — msg.Hint carries a
// transfer sequence precisely to disambiguate them — so a re-send that failed
// anywhere in the write truncated and then DELETED a complete artifact the
// receive had not created.
//
// The fix is the discipline ImportSnapshot already uses: stage in a directory
// this receive created, publish with one rename, and never remove a directory
// it merely found.
func TestSnapshotReceive_FailedTransferDoesNotDestroyTheExistingArtifact(t *testing.T) {
	f := newRecvFixture(t, createTestBarrier(t))
	const index, term = uint64(1700), uint64(4)
	preexisting := []byte("a COMPLETE snapshot this transfer did not create")
	dataPath := seedCanonicalSnapshot(t, f, index, preexisting)

	// The transfer fails while writing its own artifact.
	f.fs.failOnce(opWrite, "snapshot.dat", errInjected)

	err := f.handler.HandleSnapshot(
		f.chunks(index, term, bytes.Repeat([]byte("incoming"), 32)), authorizeAllSenders)
	if err == nil {
		t.Fatal("HandleSnapshot reported success although the write failed")
	}

	got, exists := f.fs.liveBytes(dataPath)
	if !exists {
		t.Fatal("the pre-existing snapshot.dat is GONE after a failed receive: the " +
			"receive removed a directory it did not create, so a shard whose LogDB " +
			"still names that directory can no longer start")
	}
	if !bytes.Equal(got, preexisting) {
		t.Fatalf("the pre-existing snapshot.dat = %q, want %q: the receive truncated "+
			"an artifact it did not create before authenticating a byte of the "+
			"incoming one", got, preexisting)
	}
	if leaked := recvStageDirs(f, index); len(leaked) != 0 {
		t.Errorf("the failed receive left staging directories behind: %v", leaked)
	}
}

// TestSnapshotReceive_PublishesAtomicallyOverAnUnrecordedDirectory pins the
// other half: a re-send whose destination already exists must still SUCCEED,
// and the canonical name must never be visible holding a partial artifact.
//
// Refusing the collision (which is what ImportSnapshot does) would be wrong
// here: a leader re-sending after its ack timeout is normal operation, and a
// receive that refuses it leaves the follower permanently behind.
func TestSnapshotReceive_PublishesAtomicallyOverAnUnrecordedDirectory(t *testing.T) {
	f := newRecvFixture(t, createTestBarrier(t))
	const index, term = uint64(1701), uint64(4)
	dataPath := seedCanonicalSnapshot(t, f, index,
		[]byte("a stale artifact from an earlier failed transfer"))

	payload := bytes.Repeat([]byte("the-real-snapshot-payload"), 16)
	if err := f.handler.HandleSnapshot(
		f.chunks(index, term, payload), authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot over an unrecorded directory failed: %v. A leader "+
			"re-send is normal operation; refusing it leaves the follower behind", err)
	}

	rec, err := f.ldb.GetSnapshot(f.shardID, f.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if rec.Index != index {
		t.Fatalf("LogDB records snapshot index %d, want %d", rec.Index, index)
	}
	if rec.Filepath != f.snapDir(index) {
		t.Fatalf("LogDB records Filepath %q, want the canonical %q",
			rec.Filepath, f.snapDir(index))
	}

	// The published artifact is the one just received, and it is the size the
	// record claims.
	body, exists := f.fs.liveBytes(dataPath)
	if !exists {
		t.Fatal("no snapshot.dat at the canonical path after a successful receive")
	}
	if uint64(len(body)) != rec.FileSize {
		t.Fatalf("snapshot.dat is %d bytes but LogDB records FileSize %d",
			len(body), rec.FileSize)
	}
	if bytes.Contains(body, []byte("stale artifact")) {
		t.Fatal("the canonical path still holds the stale artifact")
	}

	// No staging directory survives a success either.
	if leaked := recvStageDirs(f, index); len(leaked) != 0 {
		t.Errorf("the successful receive left staging directories behind: %v", leaked)
	}
}

// TestSnapshotReceive_RefusesToPublishOverTheRecordedSnapshot pins the
// fail-closed guard on the publish.
//
// The publish clears its destination before renaming. Doing that to the
// artifact LogDB currently RECORDS would leave the record pointing at nothing:
// the shard cannot start, and every leader re-send is answered by the dedup
// branch out of a record that names a deleted directory — a permanent wedge
// with no self-heal. The dedup branch means this is normally unreachable, so
// the guard is reached here by recording a snapshot whose Filepath is the
// destination while the recorded INDEX is below it, which is the disagreement
// a lost or rolled-back LogDB record produces.
func TestSnapshotReceive_RefusesToPublishOverTheRecordedSnapshot(t *testing.T) {
	f := newRecvFixture(t, createTestBarrier(t))
	const index, term = uint64(1702), uint64(4)
	preexisting := []byte("the artifact LogDB currently points at")
	dataPath := seedCanonicalSnapshot(t, f, index, preexisting)

	// LogDB names the destination directory but at a LOWER index, so the dedup
	// branch (existing.Index >= snapshotIndex) does not fire.
	if err := f.ldb.SaveSnapshot(f.shardID, f.replicaID, logdb.Snapshot{
		Index:    index - 1,
		Term:     term,
		Filepath: f.snapDir(index),
		FileSize: uint64(len(preexisting)),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	err := f.handler.HandleSnapshot(
		f.chunks(index, term, bytes.Repeat([]byte("incoming"), 32)), authorizeAllSenders)
	if !errors.Is(err, ErrSnapshotDestinationRecorded) {
		t.Fatalf("HandleSnapshot = %v, want ErrSnapshotDestinationRecorded", err)
	}

	got, exists := f.fs.liveBytes(dataPath)
	if !exists || !bytes.Equal(got, preexisting) {
		t.Fatal("the receive destroyed the artifact LogDB records; the shard can no " +
			"longer start and every re-send is swallowed by the dedup branch")
	}
	if leaked := recvStageDirs(f, index); len(leaked) != 0 {
		t.Errorf("the refused receive left staging directories behind: %v", leaked)
	}
}

// ---------------------------------------------------------------------------
// The LogReader snapshot must describe the artifact, not just locate it
// ---------------------------------------------------------------------------

// TestRecoverLogReader_CarriesTheArtifactDescription pins that the snapshot
// StartShard hands the raft LogReader carries the DURABLE record's full
// description of the artifact.
//
// The LogReader's copy is what the SEND path reads when this replica becomes
// leader and ships the snapshot on (forEachSnapshotChunk reads snap.OnDiskIndex
// and snap.Epoch straight out of it), and it is what the commit pipeline
// converts back into a LogDB record on the receiving side. A field dropped in
// this one literal is therefore a field the FOLLOWER records wrong: a snappy
// artifact announced as uncompressed is fed to its state machine as garbage.
func TestRecoverLogReader_CarriesTheArtifactDescription(t *testing.T) {
	const shardID, replicaID = uint64(41), uint64(1)

	// A real artifact on disk, so the recovery records a non-zero FileSize and
	// the assertions below are not measuring an absent file.
	snapDir := t.TempDir()
	body := []byte("an artifact whose description must survive the recovery")
	if err := os.WriteFile(filepath.Join(snapDir, "snapshot.dat"), body, 0o600); err != nil {
		t.Fatalf("write snapshot.dat: %v", err)
	}

	h := newTestHost(t)
	rec := logdb.Snapshot{
		Index:       500,
		Term:        9,
		Filepath:    snapDir,
		FileSize:    uint64(len(body)),
		OnDiskIndex: 77,
		Epoch:       3,
		Compression: config.Snappy,
		Membership: logdb.Membership{
			ConfigChangeID: 44,
			Addresses:      map[uint64]string{replicaID: "127.0.0.1:5000"},
		},
	}
	if err := h.logdb.SaveSnapshot(shardID, replicaID, rec); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// The premise: a compression that is not the zero value, or the assertion
	// below cannot tell a carried field from a dropped one.
	if config.Snappy == config.NoCompression {
		t.Fatal("config.Snappy equals NoCompression; this test would be vacuous")
	}

	lr := logdb.NewLogReader(shardID, replicaID, h.logdb)
	if _, err := h.recoverLogReader(lr, shardID, replicaID); err != nil {
		t.Fatalf("recoverLogReader: %v", err)
	}

	got := lr.Snapshot()
	if got.Index != rec.Index || got.Term != rec.Term {
		t.Fatalf("LogReader snapshot is at (%d, %d), want (%d, %d); the recovery "+
			"did not install it and the rest of this test asserts nothing",
			got.Index, got.Term, rec.Index, rec.Term)
	}
	if got.Compression != uint64(rec.Compression) {
		t.Errorf("Compression = %d, want %d: this replica would ship the snapshot "+
			"announcing an algorithm it was not written with, and the receiving "+
			"follower recovers the bytes as garbage", got.Compression, rec.Compression)
	}
	if got.OnDiskIndex != rec.OnDiskIndex {
		t.Errorf("OnDiskIndex = %d, want %d: the on-disk state machine cursor the "+
			"record carries is dropped on the way to the send path",
			got.OnDiskIndex, rec.OnDiskIndex)
	}
	if got.Epoch != rec.Epoch {
		t.Errorf("Epoch = %d, want %d", got.Epoch, rec.Epoch)
	}
	if got.FileSize != rec.FileSize {
		t.Errorf("FileSize = %d, want %d", got.FileSize, rec.FileSize)
	}
}
