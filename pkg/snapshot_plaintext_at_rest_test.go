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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// No NAMED plaintext at rest
//
// With a barrier configured, a snapshot's plaintext must never be written into
// a file that has a name. A named plaintext file — fsynced or not — is exactly
// what a power loss or SIGKILL leaves behind as a complete, readable copy of
// the state machine, and the sweeps that reclaim scratch names run only when
// the shard starts again. Recovery therefore decrypts into an anonymous
// scratch (unlinked before the first plaintext byte), and an import seals the
// payload as it streams in.
// ---------------------------------------------------------------------------

// namedWriteSpyFS is the real filesystem that records, per path, every byte
// written into a file WHILE THAT FILE HAD A NAME. Bytes written into an
// already-unlinked file are not recorded: nothing after a crash can read them.
type namedWriteSpyFS struct {
	osSnapshotFS
	armed *atomic.Bool
	mu    *sync.Mutex
	named map[string][]byte
}

func newNamedWriteSpyFS() namedWriteSpyFS {
	return namedWriteSpyFS{armed: new(atomic.Bool), mu: new(sync.Mutex), named: map[string][]byte{}}
}

func (s namedWriteSpyFS) OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error) {
	f, err := s.osSnapshotFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &namedWriteSpyFile{snapshotFile: f, spy: s}, nil
}

func (s namedWriteSpyFS) CreateTemp(dir, pattern string) (snapshotFile, error) {
	f, err := s.osSnapshotFS.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return &namedWriteSpyFile{snapshotFile: f, spy: s}, nil
}

// namedFilesHolding returns every path whose named writes contain needle.
func (s namedWriteSpyFS) namedFilesHolding(needle []byte) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for name, data := range s.named {
		if bytes.Contains(data, needle) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// namedPaths returns every path the spy saw written while named.
func (s namedWriteSpyFS) namedPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.named))
	for name := range s.named {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

var _ snapshotFS = namedWriteSpyFS{}

type namedWriteSpyFile struct {
	snapshotFile
	spy namedWriteSpyFS
}

func (f *namedWriteSpyFile) Write(b []byte) (int, error) {
	if f.spy.armed.Load() {
		if _, err := os.Stat(f.Name()); err == nil {
			f.spy.mu.Lock()
			f.spy.named[f.Name()] = append(f.spy.named[f.Name()], b...)
			f.spy.mu.Unlock()
		}
	}
	return f.snapshotFile.Write(b)
}

// diskScanningSM is an in-memory state machine that, the moment recovery hands
// it the snapshot, scans the host's whole data tree for a file holding the
// plaintext payload: the state a crash at that instant would leave behind.
type diskScanningSM struct {
	mockStateMachine
	payload   []byte
	roots     []string
	exposed   []string
	recovered []byte
	called    atomic.Bool
}

func (m *diskScanningSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	m.called.Store(true)
	for _, root := range m.roots {
		m.exposed = append(m.exposed, filesHolding(root, m.payload)...)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.recovered = data
	return nil
}

// filesHolding returns every regular file under root whose content holds
// needle.
func filesHolding(root string, needle []byte) []string {
	var found []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable entry is not a finding
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if data, readErr := os.ReadFile(path); readErr == nil && bytes.Contains(data, needle) {
			found = append(found, path)
		}
		return nil
	})
	return found
}

// atRestPayload is a distinctive payload spanning several snapshot frames.
func atRestPayload() []byte {
	return bytes.Repeat([]byte("state-machine-plaintext-must-never-be-named-at-rest|"), 4096)
}

// TestStartShard_LocalSnapshotRecovery_DecryptScratchIsUnnamed: StartShard's
// recovery of an encrypted local snapshot writes its plaintext only into a
// scratch that has already been unlinked, and while the state machine reads
// it no file in the host's data tree — nor beside the recorded artifact —
// holds the plaintext.
func TestStartShard_LocalSnapshotRecovery_DecryptScratchIsUnnamed(t *testing.T) {
	v := snapshotVariant{name: "encrypted_uncompressed", encrypted: true}
	barrier := createTestBarrier(t)
	db := newMockLogDB()
	payload := atRestPayload()
	dir := seedLocalSnapshot(t, db, v, barrier, 1, 1, 40, 3, payload)

	cfg := testHostConfig(t)
	spy := newNamedWriteSpyFS()
	h, err := NewHost(cfg, WithLogDB(db), WithBarrier(barrier), WithoutTransport(), withSnapshotFS(spy))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	memSM := &diskScanningSM{payload: payload, roots: []string{cfg.NodeHostDir, dir}}
	create := func(_, _ uint64) interface{} { return memSM }
	spy.armed.Store(true)
	if startErr := h.StartShard(quiescentMembers(), false, create, v.shardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	spy.armed.Store(false)

	if !memSM.called.Load() {
		t.Fatal("RecoverFromSnapshot was not called")
	}
	if !bytes.Equal(memSM.recovered, payload) {
		t.Fatalf("state machine recovered %d bytes, want the %d-byte payload", len(memSM.recovered), len(payload))
	}
	if named := spy.namedFilesHolding(payload[:4096]); len(named) != 0 {
		t.Fatalf("the recovery decrypt wrote plaintext into files that still had a name: %v", named)
	}
	if len(memSM.exposed) != 0 {
		t.Fatalf("while RecoverFromSnapshot ran, named files held the plaintext: %v", memSM.exposed)
	}
	assertNoDecryptScratch(t, dir)
}

// TestStartShard_LocalSnapshotRecovery_FailedScratchUnlinkFailsStart: when
// the recovery's decrypt scratch cannot be unlinked, StartShard fails before
// a byte of plaintext is written — no named file ever holds it, and the state
// machine is handed nothing — rather than decrypt into a file a crash would
// leave readable.
func TestStartShard_LocalSnapshotRecovery_FailedScratchUnlinkFailsStart(t *testing.T) {
	v := snapshotVariant{name: "encrypted_uncompressed", encrypted: true}
	barrier := createTestBarrier(t)
	db := newMockLogDB()
	payload := atRestPayload()
	dir := seedLocalSnapshot(t, db, v, barrier, 1, 1, 40, 3, payload)

	cfg := testHostConfig(t)
	spy := unlinkRefusingSpyFS{namedWriteSpyFS: newNamedWriteSpyFS(), refused: new(atomic.Bool)}
	h, err := NewHost(cfg, WithLogDB(db), WithBarrier(barrier), WithoutTransport(), withSnapshotFS(spy))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	memSM := &diskScanningSM{payload: payload, roots: []string{cfg.NodeHostDir, dir}}
	create := func(_, _ uint64) interface{} { return memSM }
	spy.armed.Store(true)
	startErr := h.StartShard(quiescentMembers(), false, create, v.shardConfig(1, 1))
	spy.armed.Store(false)

	if !errors.Is(startErr, engine.ErrScratchNotUnlinked) {
		t.Fatalf("StartShard with a refused scratch unlink = %v, want ErrScratchNotUnlinked", startErr)
	}
	if memSM.called.Load() {
		t.Fatal("the state machine was handed a snapshot decrypted into a scratch that kept its name")
	}
	if named := spy.namedPaths(); len(named) != 0 {
		t.Fatalf("the recovery wrote into files that still had a name: %v", named)
	}
	for _, root := range memSM.roots {
		if exposed := filesHolding(root, payload[:4096]); len(exposed) != 0 {
			t.Fatalf("the failed recovery left the plaintext in named files: %v", exposed)
		}
	}
	assertNoDecryptScratch(t, dir)
}

// TestImportSnapshot_EncryptedImportNeverWritesNamedPlaintext: with a barrier
// configured, an import writes its staged snapshot.dat as ciphertext from the
// first byte — no file the import creates ever receives the artifact's
// plaintext — and the imported snapshot still recovers the original state.
func TestImportSnapshot_EncryptedImportNeverWritesNamedPlaintext(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: 1, ReplicaID: 1}}
	barrier := createTestBarrier(t)
	spy := newNamedWriteSpyFS()
	h, err := NewHost(cfg, WithLogDB(db), WithBarrier(barrier), WithoutTransport(), withSnapshotFS(spy))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	payload := atRestPayload()
	artifact := buildSnapshotArtifact(t, 1500, 4, 0, proto.Membership{}, snapshotEnvelope(payload))

	spy.armed.Store(true)
	if impErr := h.ImportSnapshot(context.Background(), 1, bytes.NewReader(artifact),
		WithImportDataDir("")); impErr != nil {
		t.Fatalf("ImportSnapshot: %v", impErr)
	}
	spy.armed.Store(false)

	imported, getErr := db.GetSnapshot(1, 1)
	if getErr != nil {
		t.Fatalf("GetSnapshot: %v", getErr)
	}
	if imported.Epoch == 0 {
		t.Fatal("imported snapshot Epoch = 0 with a barrier configured; it was not encrypted at rest")
	}
	dataPath := filepath.Join(imported.Filepath, "snapshot.dat")
	if len(spy.namedPaths()) == 0 {
		t.Fatal("fixture: the spy saw no write, so it observed nothing")
	}
	if named := spy.namedFilesHolding(payload[:4096]); len(named) != 0 {
		t.Fatalf("the import wrote plaintext into named files: %v", named)
	}
	if exposed := filesHolding(cfg.NodeHostDir, payload[:4096]); len(exposed) != 0 {
		t.Fatalf("plaintext is at rest after the import: %v", exposed)
	}

	// The recorded size and metadata describe the file as stored: ciphertext.
	fi, statErr := os.Stat(dataPath)
	if statErr != nil {
		t.Fatalf("stat imported artifact: %v", statErr)
	}
	if imported.FileSize != uint64(fi.Size()) {
		t.Fatalf("recorded FileSize %d, want the on-disk (encrypted) size %d", imported.FileSize, fi.Size())
	}

	// And the import restores the state: StartShard decrypts and recovers it.
	memSM := &diskScanningSM{payload: payload, roots: []string{cfg.NodeHostDir}}
	create := func(_, _ uint64) interface{} { return memSM }
	if startErr := h.StartShard(quiescentMembers(), false, create, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard after import: %v", startErr)
	}
	if !bytes.Equal(memSM.recovered, payload) {
		t.Fatalf("state machine recovered %d bytes, want the %d-byte imported payload",
			len(memSM.recovered), len(payload))
	}
	if len(memSM.exposed) != 0 {
		t.Fatalf("while RecoverFromSnapshot ran, named files held the plaintext: %v", memSM.exposed)
	}
}
