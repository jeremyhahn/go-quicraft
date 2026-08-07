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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/snappy"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Snapshot artifact matrix
//
// snapshotPool.saveSnapshot builds its writer chain as
//
//	state machine -> [snappy.NewBufferedWriter] -> file -> [encrypt in place]
//
// so every reader chain that consumes a saved snapshot must mirror it in
// reverse: decrypt the file, then wrap the plaintext in snappy.NewReader.
// A recovery path that skips either step feeds the state machine ciphertext or
// compressed bytes — silent state corruption on restart.
//
// The epoch-0 + uncompressed cell is the ONE configuration in which BOTH
// omissions are invisible, and it used to be the only configuration recovery
// was tested in: that blind spot shipped two data-integrity bugs. Recovery
// tests therefore run as subtests over the whole matrix so a regression names
// the exact configuration that broke.
// ---------------------------------------------------------------------------

// snapshotVariant is one cell of the {plaintext, encrypted} × {uncompressed,
// snappy} matrix an at-rest snapshot artifact can be written in.
type snapshotVariant struct {
	name      string
	encrypted bool
	snappy    bool
}

// snapshotVariants is the full artifact matrix. Recovery tests that are
// meaningful in every configuration iterate over it.
var snapshotVariants = []snapshotVariant{
	{name: "plaintext_uncompressed"},
	{name: "plaintext_snappy", snappy: true},
	{name: "encrypted_uncompressed", encrypted: true},
	{name: "encrypted_snappy", encrypted: true, snappy: true},
}

// encryptedSnapshotVariants returns the matrix cells whose artifact is
// encrypted at rest. Tests about decryption itself — barrier resolution,
// fail-closed behaviour, ciphertext corruption, artifact canonicality — have
// no plaintext counterpart, but they still must cover both compression cells
// because decompression sits inside decryption.
func encryptedSnapshotVariants() []snapshotVariant {
	out := make([]snapshotVariant, 0, len(snapshotVariants))
	for _, v := range snapshotVariants {
		if v.encrypted {
			out = append(out, v)
		}
	}
	return out
}

// compression is the algorithm the variant's artifact is written through. It
// is what the SNAPSHOT RECORD must carry: recovery builds its reader chain
// from the artifact's recorded compression, never from the shard config.
func (v snapshotVariant) compression() config.CompressionType {
	if v.snappy {
		return config.Snappy
	}
	return config.NoCompression
}

// shardConfig returns a shard config whose compression setting matches the
// variant. It governs the SAVE side only; recovery reads the artifact's own
// recorded compression.
func (v snapshotVariant) shardConfig(shardID, replicaID uint64) config.Config {
	c := testShardConfig(shardID, replicaID)
	c.SnapshotCompressionType = v.compression()
	return c
}

// fileBytes returns the bytes snapshot.dat holds BEFORE at-rest encryption:
// the rsm envelope around payload, Snappy-framed when the variant compresses.
// It is also exactly what a decrypting reader (ExportSnapshot) must emit.
func (v snapshotVariant) fileBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	envelope := snapshotEnvelope(payload)
	if !v.snappy {
		return envelope
	}
	var buf bytes.Buffer
	w := snappy.NewBufferedWriter(&buf)
	if _, err := w.Write(envelope); err != nil {
		t.Fatalf("snappy write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("snappy close: %v", err)
	}
	return buf.Bytes()
}

// writeSnapshotArtifact writes snapshot.dat into dir exactly as saveSnapshot
// would for this variant and returns the epoch the LogDB record must carry
// (0 when the artifact is plaintext).
func (v snapshotVariant) writeSnapshotArtifact(t *testing.T, dir string, barrier Barrier, payload []byte) uint64 {
	t.Helper()
	path := filepath.Join(dir, "snapshot.dat")
	data := v.fileBytes(t, payload)
	if !v.encrypted {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		return 0
	}
	if barrier == nil {
		t.Fatal("encrypted snapshot variant requires a barrier")
	}
	writeEncryptedSnapshotFile(t, barrier, path, data)
	return barrier.CurrentEpoch()
}

// seedLocalSnapshot writes a snapshot.dat holding the given user payload into a
// fresh directory and records the matching LogDB snapshot record, exactly as
// saveSnapshot would for the variant (epoch recorded only when encrypted).
// It returns the snapshot directory.
func seedLocalSnapshot(t *testing.T, db *mockLogDB, v snapshotVariant, barrier Barrier,
	shardID, replicaID, index, term uint64, payload []byte) string {
	t.Helper()
	dir := t.TempDir()
	epoch := v.writeSnapshotArtifact(t, dir, barrier, payload)
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:       index,
		Term:        term,
		Filepath:    dir,
		Epoch:       epoch,
		Compression: v.compression(),
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return dir
}

// recordingMemSM is an in-memory state machine that captures the bytes
// RecoverFromSnapshot received and rejects any payload that is not the
// plaintext it saved. The rejection is what turns "ciphertext (or compressed
// bytes) was fed to the user state machine" into a visible StartShard failure
// instead of silent state corruption.
type recordingMemSM struct {
	mockStateMachine
	want          []byte
	recovered     []byte
	recoverCalled atomic.Bool
}

var errRecoveredNotPlaintext = errors.New("quicraft_test: state machine received non-plaintext snapshot data")

func (m *recordingMemSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	m.recoverCalled.Store(true)
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.recovered = data
	if !bytes.Equal(data, m.want) {
		return errRecoveredNotPlaintext
	}
	return nil
}

// snapshotEnvelope wraps a user payload in the rsm snapshot envelope: a
// 4-byte little-endian session count of 0 followed by the user payload.
func snapshotEnvelope(payload []byte) []byte {
	return append([]byte{0, 0, 0, 0}, payload...)
}

// assertNoDecryptScratch fails if the snapshot directory holds anything other
// than the canonical snapshot.dat: the decrypt scratch sibling must be removed
// on every path.
func assertNoDecryptScratch(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != "snapshot.dat" {
			t.Errorf("stray file %q left in snapshot directory", e.Name())
		}
	}
}

// TestStartShard_LocalSnapshotRecovery_InMemorySM is the regression test for
// the restart-recovery reader chain. It runs the full artifact matrix:
//
//   - encrypted cells pin the decryption gap (snapshots are encrypted at rest
//     and StartShard fed the raw snapshot.dat to the state machine);
//   - snappy cells pin the decompression gap (the save path wraps the writer in
//     snappy.NewBufferedWriter, so the reader must be wrapped symmetrically);
//   - plaintext cells pin the negative: a barrier is always configured here, so
//     the RECORDED EPOCH — not the barrier's presence — decides whether
//     decryption runs, and an epoch-0 artifact is recovered verbatim.
func TestStartShard_LocalSnapshotRecovery_InMemorySM(t *testing.T) {
	for _, v := range snapshotVariants {
		t.Run(v.name, func(t *testing.T) {
			barrier := createTestBarrier(t)
			db := newMockLogDB()
			payload := []byte("in-memory state machine snapshot payload")
			dir := seedLocalSnapshot(t, db, v, barrier, 1, 1, 40, 3, payload)

			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithBarrier(barrier), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
				v.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard failed on a %s local snapshot: %v", v.name, startErr)
			}

			if !memSM.recoverCalled.Load() {
				t.Fatal("RecoverFromSnapshot was not called for an in-memory SM with a local snapshot")
			}
			if !bytes.Equal(memSM.recovered, payload) {
				t.Fatalf("state machine recovered %q, want %q (reader chain does not mirror the writer chain)",
					memSM.recovered, payload)
			}
			if got := testLoadNode(h, 1).lastApplied.Load(); got != 40 {
				t.Errorf("ns.lastApplied = %d, want 40", got)
			}
			assertNoDecryptScratch(t, dir)
		})
	}
}

// TestStartShard_LocalSnapshotRecovery_OnDiskBootstrap covers the on-disk
// bootstrap case (Open() returns 0 with a LogDB snapshot record present): the
// reconstructed database must be built from decrypted, decompressed bytes.
func TestStartShard_LocalSnapshotRecovery_OnDiskBootstrap(t *testing.T) {
	for _, v := range snapshotVariants {
		t.Run(v.name, func(t *testing.T) {
			barrier := createTestBarrier(t)
			db := newMockLogDB()
			payload := []byte("on-disk bootstrap snapshot payload")
			dir := seedLocalSnapshot(t, db, v, barrier, 1, 1, 80, 3, payload)

			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithBarrier(barrier), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			diskSM := &recordingOnDiskSM{openLastApplied: 0}
			create := func(_, _ uint64) interface{} { return diskSM }
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
				v.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard failed on a %s local snapshot: %v", v.name, startErr)
			}

			if !diskSM.recoverCalled.Load() {
				t.Fatal("RecoverFromSnapshot was not called on the on-disk bootstrap path")
			}
			if !bytes.Equal(diskSM.recoveredData, payload) {
				t.Fatalf("on-disk SM recovered %q, want %q (reader chain does not mirror the writer chain)",
					diskSM.recoveredData, payload)
			}
			assertNoDecryptScratch(t, dir)
		})
	}
}

// TestStartShard_LocalSnapshotRecovery_PerShardBarrier verifies the per-shard
// barrier is the one used for recovery. The host-wide barrier holds different
// key material, so recovering through it would fail AEAD authentication.
// Only the encrypted cells are meaningful: with no epoch recorded no barrier
// is consulted at all.
func TestStartShard_LocalSnapshotRecovery_PerShardBarrier(t *testing.T) {
	for _, v := range encryptedSnapshotVariants() {
		t.Run(v.name, func(t *testing.T) {
			hostBarrier := createTestBarrier(t)
			shardBarrier := createTestBarrier(t)
			db := newMockLogDB()
			payload := []byte("per-shard barrier snapshot payload")
			dir := seedLocalSnapshot(t, db, v, shardBarrier, 1, 1, 12, 2, payload)

			h, err := NewHost(testHostConfig(t),
				WithLogDB(db),
				WithBarrier(hostBarrier),
				WithShardBarriers(func(shardID uint64) Barrier {
					if shardID == 1 {
						return shardBarrier
					}
					return nil
				}),
				WithoutTransport(),
			)
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
				v.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard failed with a per-shard barrier: %v", startErr)
			}
			if !bytes.Equal(memSM.recovered, payload) {
				t.Fatalf("state machine recovered %q, want %q", memSM.recovered, payload)
			}
			assertNoDecryptScratch(t, dir)
		})
	}
}

// TestStartShard_LocalSnapshotRecovery_FailsClosedWithoutBarrier verifies the
// fail-closed contract: a snapshot recorded with Epoch > 0 that cannot be
// decrypted must abort StartShard with
// ErrSnapshotEncryptedBarrierUnavailable rather than recover the state machine
// from ciphertext. Compression must not change the verdict — the barrier gate
// runs before any reader is built.
func TestStartShard_LocalSnapshotRecovery_FailsClosedWithoutBarrier(t *testing.T) {
	encryptBarrier := createTestBarrier(t)
	payload := []byte("undecryptable snapshot payload")

	cases := []struct {
		name    string
		barrier func(t *testing.T) Barrier
	}{
		{name: "no barrier configured"},
		{name: "sealed barrier", barrier: func(t *testing.T) Barrier { return createTestBarrier(t) }},
	}
	for _, v := range encryptedSnapshotVariants() {
		for _, tc := range cases {
			t.Run(v.name+"/"+tc.name, func(t *testing.T) {
				db := newMockLogDB()
				dir := seedLocalSnapshot(t, db, v, encryptBarrier, 1, 1, 40, 3, payload)

				opts := []HostOption{WithLogDB(db), WithoutTransport()}
				if tc.barrier != nil {
					b := tc.barrier(t)
					if sealErr := b.Seal(); sealErr != nil {
						t.Fatalf("Seal: %v", sealErr)
					}
					opts = append(opts, WithBarrier(b))
				}
				h, err := NewHost(testHostConfig(t), opts...)
				if err != nil {
					t.Fatalf("NewHost: %v", err)
				}
				defer h.Close()

				memSM := &recordingMemSM{want: payload}
				create := func(_, _ uint64) interface{} { return memSM }
				startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
					v.shardConfig(1, 1))
				if !errors.Is(startErr, ErrSnapshotEncryptedBarrierUnavailable) {
					t.Fatalf("StartShard error = %v, want ErrSnapshotEncryptedBarrierUnavailable", startErr)
				}
				var initErr *HostInitError
				if !errors.As(startErr, &initErr) || initErr.Field != "snapshot_decrypt" {
					t.Fatalf("StartShard error = %v, want HostInitError{Field: snapshot_decrypt}", startErr)
				}
				if memSM.recoverCalled.Load() {
					t.Fatal("RecoverFromSnapshot was called with an undecryptable snapshot")
				}
				if h.ShardCount() != 0 {
					t.Fatalf("ShardCount = %d, want 0 after a failed StartShard", h.ShardCount())
				}
				assertNoDecryptScratch(t, dir)
			})
		}
	}
}

// TestStartShard_LocalSnapshotRecovery_LeavesCanonicalArtifact verifies that
// recovery never rewrites snapshot.dat: it stays byte-identical ciphertext, so
// a crash or a later restart can retry decryption. A plaintext snapshot.dat
// paired with a LogDB record that still says Epoch > 0 would be permanently
// unrecoverable.
func TestStartShard_LocalSnapshotRecovery_LeavesCanonicalArtifact(t *testing.T) {
	for _, v := range encryptedSnapshotVariants() {
		t.Run(v.name, func(t *testing.T) {
			barrier := createTestBarrier(t)
			db := newMockLogDB()
			payload := bytes.Repeat([]byte("canonical-artifact-payload;"), 4096)
			dir := seedLocalSnapshot(t, db, v, barrier, 1, 1, 40, 3, payload)
			dataPath := filepath.Join(dir, "snapshot.dat")
			before, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}

			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithBarrier(barrier), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
				v.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard failed: %v", startErr)
			}
			if !bytes.Equal(memSM.recovered, payload) {
				t.Fatal("state machine did not recover the plaintext payload")
			}

			after, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("snapshot.dat changed during recovery; the canonical encrypted artifact must be untouched")
			}
			if bytes.Contains(after, payload) {
				t.Fatal("snapshot.dat contains plaintext after recovery")
			}
			// Even for the compressed cell the artifact must remain
			// unreadable without the barrier: Snappy framing is not
			// confidentiality, so assert the compressed plaintext the writer
			// chain produced is absent too.
			if plain := v.fileBytes(t, payload); bytes.Contains(after, plain) {
				t.Fatal("snapshot.dat contains the unencrypted (compressed) artifact after recovery")
			}
			assertNoDecryptScratch(t, dir)
		})
	}
}

// TestStartShard_LocalSnapshotRecovery_CorruptCiphertextFailsStart verifies a
// snapshot whose frames cannot be authenticated aborts StartShard instead of
// recovering partial or garbage state.
func TestStartShard_LocalSnapshotRecovery_CorruptCiphertextFailsStart(t *testing.T) {
	for _, v := range encryptedSnapshotVariants() {
		t.Run(v.name, func(t *testing.T) {
			barrier := createTestBarrier(t)
			db := newMockLogDB()
			payload := []byte("corrupt ciphertext payload")
			dir := seedLocalSnapshot(t, db, v, barrier, 1, 1, 40, 3, payload)

			dataPath := filepath.Join(dir, "snapshot.dat")
			raw, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			// Flip a bit inside the first frame's ciphertext body so AEAD fails.
			raw[len(raw)-1] ^= 0xFF
			if wErr := os.WriteFile(dataPath, raw, 0o600); wErr != nil {
				t.Fatalf("WriteFile: %v", wErr)
			}

			h, err := NewHost(testHostConfig(t), WithLogDB(db), WithBarrier(barrier), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			defer h.Close()

			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
				v.shardConfig(1, 1))
			if startErr == nil {
				t.Fatal("StartShard succeeded with an unauthenticatable snapshot")
			}
			var initErr *HostInitError
			if !errors.As(startErr, &initErr) || initErr.Field != "snapshot_decrypt" {
				t.Fatalf("StartShard error = %v, want HostInitError{Field: snapshot_decrypt}", startErr)
			}
			if memSM.recoverCalled.Load() {
				t.Fatal("RecoverFromSnapshot was called with undecryptable data")
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("ReadDir: %v", err)
			}
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), ".decrypt-") {
					t.Errorf("decrypt scratch file %q left behind after a failed decrypt", e.Name())
				}
			}
		})
	}
}

// TestExportImportSnapshot_RoundTripRecoversPlaintext closes the loop across
// hosts: export decrypts the at-rest artifact, import persists it with Epoch 0,
// and the destination's StartShard must still hand the state machine the
// original payload. Running the matrix pins both halves of the reader chain —
// export owns the decrypt step, StartShard owns the decompress step, and only
// the combination proves an operator's disaster-recovery restore of an
// encrypted, compressed shard actually reconstructs state.
func TestExportImportSnapshot_RoundTripRecoversPlaintext(t *testing.T) {
	for _, v := range snapshotVariants {
		t.Run(v.name, func(t *testing.T) {
			payload := []byte("export/import round-trip snapshot payload")

			srcBarrier := createTestBarrier(t)
			srcDB := newMockLogDB()
			src, err := NewHost(testHostConfig(t), WithLogDB(srcDB), WithBarrier(srcBarrier), WithoutTransport())
			if err != nil {
				t.Fatalf("NewHost(src): %v", err)
			}
			defer src.Close()

			// ExportSnapshot resolves the replica through a RUNNING shard, so
			// start first and record the artifact afterwards.
			if startErr := src.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				testCreateFunc, v.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard(src): %v", startErr)
			}
			seedLocalSnapshot(t, srcDB, v, srcBarrier, 1, 1, 1500, 4, payload)

			var exported bytes.Buffer
			if expErr := src.ExportSnapshot(context.Background(), 1, &exported); expErr != nil {
				t.Fatalf("ExportSnapshot: %v", expErr)
			}
			// The artifact body must be the writer chain's pre-encryption
			// bytes: decrypted, still Snappy-framed when the shard compresses.
			body := bytes.NewReader(exported.Bytes())
			if _, hdrErr := readSnapshotArtifactHeader(body); hdrErr != nil {
				t.Fatalf("readSnapshotArtifactHeader: %v", hdrErr)
			}
			data := make([]byte, body.Len())
			if _, readErr := io.ReadFull(body, data); readErr != nil {
				t.Fatalf("reading artifact data: %v", readErr)
			}
			if want := v.fileBytes(t, payload); !bytes.Equal(data, want) {
				t.Fatalf("exported artifact data mismatch: got %d bytes, want the decrypted %d-byte artifact",
					len(data), len(want))
			}

			// Restore onto a second host that holds no barrier at all: the
			// imported artifact is plaintext (Epoch 0) by construction.
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
			if imported.Epoch != 0 {
				t.Fatalf("imported snapshot Epoch = %d, want 0 (plaintext)", imported.Epoch)
			}

			memSM := &recordingMemSM{want: payload}
			create := func(_, _ uint64) interface{} { return memSM }
			if startErr := dst.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create,
				v.shardConfig(1, 1)); startErr != nil {
				t.Fatalf("StartShard(dst) after import: %v", startErr)
			}
			if !memSM.recoverCalled.Load() {
				t.Fatal("RecoverFromSnapshot was not called after ImportSnapshot")
			}
			if !bytes.Equal(memSM.recovered, payload) {
				t.Fatalf("restored state machine recovered %q, want %q", memSM.recovered, payload)
			}
			if got := testLoadNode(dst, 1).lastApplied.Load(); got != 1500 {
				t.Errorf("ns.lastApplied = %d, want 1500 (the exported snapshot index)", got)
			}
		})
	}
}

var _ sm.StateMachine = (*recordingMemSM)(nil)
