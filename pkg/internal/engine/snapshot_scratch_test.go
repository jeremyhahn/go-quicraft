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
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// namedPlaintextProbe collects every moment a decrypt scratch file was written
// to while it still had a name on disk.
type namedPlaintextProbe struct {
	mu         sync.Mutex
	created    int
	violations []string
}

func (p *namedPlaintextProbe) violation(name string) {
	p.mu.Lock()
	p.violations = append(p.violations, name)
	p.mu.Unlock()
}

func (p *namedPlaintextProbe) report() (int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.created, append([]string(nil), p.violations...)
}

// probeScratchFS is the real filesystem whose scratch files check, on every
// Write, that their name is already gone. A Write into a file that can still
// be opened by name is exactly the plaintext-at-rest window a crash turns into
// a readable copy of the state machine.
type probeScratchFS struct {
	osScratchFS
	probe *namedPlaintextProbe
}

func (fsys probeScratchFS) CreateTemp(dir, pattern string) (SnapshotFile, error) {
	f, err := fsys.osScratchFS.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	fsys.probe.mu.Lock()
	fsys.probe.created++
	fsys.probe.mu.Unlock()
	return &probeScratchFile{SnapshotFile: f, probe: fsys.probe}, nil
}

type probeScratchFile struct {
	SnapshotFile
	probe *namedPlaintextProbe
}

func (f *probeScratchFile) Write(b []byte) (int, error) {
	if _, err := os.Stat(f.Name()); !errors.Is(err, fs.ErrNotExist) {
		f.probe.violation(f.Name())
	}
	return f.SnapshotFile.Write(b)
}

// plaintextScanningSnapshotter saves a distinctive payload and, when asked to
// recover, first scans the whole snapshot tree for any file holding that
// payload in the clear — the state a crash at that instant would leave behind.
type plaintextScanningSnapshotter struct {
	payload   []byte
	root      string
	exposed   []string
	recovered []byte
	applied   atomic.Uint64
}

func (s *plaintextScanningSnapshotter) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write(s.payload)
	return err
}

func (s *plaintextScanningSnapshotter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	s.exposed = filesContaining(s.root, s.payload)
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.recovered = data
	return nil
}

func (s *plaintextScanningSnapshotter) LastApplied() uint64 { return s.applied.Load() }

// filesContaining returns every regular file under root whose content holds
// needle.
func filesContaining(root string, needle []byte) []string {
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

// TestRecoverSnapshot_DecryptsIntoAnUnnamedScratch: recovering an encrypted
// snapshot must never put a NAMED plaintext copy of the state machine on disk.
// The scratch it decrypts into is unlinked before the first plaintext byte is
// written, and while RecoverFromSnapshot runs no file anywhere in the snapshot
// tree holds the plaintext. A named scratch — removed only after recovery —
// is a complete, readable, fsynced copy of the state machine that a power loss
// or SIGKILL leaves at rest.
func TestRecoverSnapshot_DecryptsIntoAnUnnamedScratch(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, newTestBarrier(5), nil, nil, stopC, nil)
	probe := &namedPlaintextProbe{}
	p.scratchFS = probeScratchFS{probe: probe}
	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	// Larger than one frame, so the decrypt writes the scratch more than once.
	payload := bytes.Repeat([]byte("recovery-plaintext-must-never-have-a-name|"), 4096)

	saver := &plaintextScanningSnapshotter{payload: payload}
	saver.applied.Store(100)
	saveNode := NewNode(nil, nil, nil, cfg, saver, tmpDir, nil)
	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: saveNode, save: true}); err != nil {
		t.Fatalf("save request: %v", err)
	}
	if res := awaitSnapshotResult(t, p, "save"); res.err != nil {
		t.Fatalf("save: %v", res.err)
	}
	if exposed := filesContaining(tmpDir, payload); len(exposed) != 0 {
		t.Fatalf("fixture: the saved artifact holds the plaintext: %v", exposed)
	}

	recoverer := &plaintextScanningSnapshotter{payload: payload, root: tmpDir}
	recoverNode := NewNode(nil, nil, nil, cfg, recoverer, tmpDir, nil)
	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: recoverNode}); err != nil {
		t.Fatalf("recover request: %v", err)
	}
	if res := awaitSnapshotResult(t, p, "recover"); res.err != nil {
		t.Fatalf("recover: %v", res.err)
	}

	if !bytes.Equal(recoverer.recovered, payload) {
		t.Fatalf("recovered %d bytes, want the %d-byte saved payload", len(recoverer.recovered), len(payload))
	}
	created, violations := probe.report()
	if created != 1 {
		t.Fatalf("recovery created %d scratch files through the pool's scratch filesystem, want 1", created)
	}
	if len(violations) != 0 {
		t.Fatalf("plaintext was written into a scratch file that still had a name: %v", violations)
	}
	if len(recoverer.exposed) != 0 {
		t.Fatalf("while RecoverFromSnapshot ran, named files held the plaintext: %v", recoverer.exposed)
	}
	if left, _ := filepath.Glob(filepath.Join(recoverNode.ReplicaSnapshotDir(), RecoverDecryptTempPattern)); len(left) != 0 {
		t.Fatalf("recovery left scratch names behind: %v", left)
	}
}

// errScratchRemove models a scratch file that cannot be unlinked.
var errScratchRemove = errors.New("test fs: remove refused")

// unlinkRefusingProbeFS is probeScratchFS whose first Remove — the scratch's
// immediate unlink — is refused.
type unlinkRefusingProbeFS struct {
	probeScratchFS
	refused *atomic.Bool
}

func (fsys unlinkRefusingProbeFS) Remove(name string) error {
	if fsys.refused.CompareAndSwap(false, true) {
		return errScratchRemove
	}
	return fsys.probeScratchFS.Remove(name)
}

// TestRecoverSnapshot_FailedScratchUnlinkFailsRecovery: when the decrypt
// scratch cannot be unlinked, recovery fails before a byte of plaintext is
// written anywhere — no named file ever holds it, and the state machine is not
// handed anything — and the retried recovery, once the unlink succeeds,
// recovers the snapshot whole.
func TestRecoverSnapshot_FailedScratchUnlinkFailsRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, newTestBarrier(5), nil, nil, stopC, nil)
	probe := &namedPlaintextProbe{}
	p.scratchFS = unlinkRefusingProbeFS{probeScratchFS: probeScratchFS{probe: probe}, refused: new(atomic.Bool)}
	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	payload := bytes.Repeat([]byte("a-refused-unlink-must-never-expose-plaintext|"), 4096)

	saver := &plaintextScanningSnapshotter{payload: payload}
	saver.applied.Store(100)
	saveNode := NewNode(nil, nil, nil, cfg, saver, tmpDir, nil)
	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: saveNode, save: true}); err != nil {
		t.Fatalf("save request: %v", err)
	}
	if res := awaitSnapshotResult(t, p, "save"); res.err != nil {
		t.Fatalf("save: %v", res.err)
	}

	recoverer := &plaintextScanningSnapshotter{payload: payload, root: tmpDir}
	recoverNode := NewNode(nil, nil, nil, cfg, recoverer, tmpDir, nil)
	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: recoverNode}); err != nil {
		t.Fatalf("recover request: %v", err)
	}
	res := awaitSnapshotResult(t, p, "recover")
	if !errors.Is(res.err, ErrScratchNotUnlinked) {
		t.Fatalf("recovery with a refused scratch unlink = %v, want ErrScratchNotUnlinked", res.err)
	}
	if recoverer.recovered != nil {
		t.Fatal("the state machine was handed a snapshot decrypted into a scratch that kept its name")
	}
	if _, violations := probe.report(); len(violations) != 0 {
		t.Fatalf("plaintext was written into a scratch file that still had a name: %v", violations)
	}
	if exposed := filesContaining(tmpDir, payload); len(exposed) != 0 {
		t.Fatalf("the failed recovery left the plaintext in named files: %v", exposed)
	}
	if left, _ := filepath.Glob(filepath.Join(recoverNode.ReplicaSnapshotDir(), RecoverDecryptTempPattern)); len(left) != 0 {
		t.Fatalf("the failed recovery left scratch names behind: %v", left)
	}

	// The unlink now succeeds: the retried recovery reads the snapshot whole.
	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: recoverNode}); err != nil {
		t.Fatalf("retried recover request: %v", err)
	}
	if res := awaitSnapshotResult(t, p, "retried recover"); res.err != nil {
		t.Fatalf("retried recover: %v", res.err)
	}
	if !bytes.Equal(recoverer.recovered, payload) {
		t.Fatalf("retried recovery read %d bytes, want the %d-byte payload", len(recoverer.recovered), len(payload))
	}
}

// removeFailScratchFS refuses Remove: every call, or only the first.
type removeFailScratchFS struct {
	osScratchFS
	onlyFirst bool
	calls     *atomic.Int32
}

func (fsys removeFailScratchFS) Remove(name string) error {
	if n := fsys.calls.Add(1); !fsys.onlyFirst || n == 1 {
		return errScratchRemove
	}
	return fsys.osScratchFS.Remove(name)
}

// TestCreateAnonymousScratch_FailedUnlinkFails: a scratch whose immediate
// unlink fails is never handed out — plaintext decrypted into it would keep a
// name until Close, and a SIGKILL or power loss before then leaves it at rest.
// The call fails with ErrScratchNotUnlinked, carrying the refused unlink. The
// name is removed once more, best effort: reclaimed, it leaves nothing and is
// not counted; surviving that too, it is EMPTY and counted on
// StaleDecryptSweepFailures for the sweep.
func TestCreateAnonymousScratch_FailedUnlinkFails(t *testing.T) {
	t.Run("name_reclaimed", func(t *testing.T) {
		dir := t.TempDir()
		before := StaleDecryptSweepFailures()
		f, err := CreateAnonymousScratch(removeFailScratchFS{onlyFirst: true, calls: new(atomic.Int32)},
			dir, RecoverDecryptTempPattern)
		if f != nil {
			t.Fatal("a scratch whose unlink failed was handed out")
		}
		if !errors.Is(err, ErrScratchNotUnlinked) || !errors.Is(err, errScratchRemove) {
			t.Fatalf("CreateAnonymousScratch = %v, want ErrScratchNotUnlinked wrapping the refused unlink", err)
		}
		var unlinkErr *ScratchUnlinkError
		if !errors.As(err, &unlinkErr) || filepath.Dir(unlinkErr.Path) != dir {
			t.Fatalf("CreateAnonymousScratch = %v, want *ScratchUnlinkError naming a scratch in %s", err, dir)
		}
		if left, _ := filepath.Glob(filepath.Join(dir, RecoverDecryptTempPattern)); len(left) != 0 {
			t.Fatalf("the refused scratch kept its name: %v", left)
		}
		if got := StaleDecryptSweepFailures(); got != before {
			t.Fatalf("a reclaimed scratch was counted as unreclaimed: %d -> %d", before, got)
		}
	})

	t.Run("name_survives", func(t *testing.T) {
		dir := t.TempDir()
		before := StaleDecryptSweepFailures()
		f, err := CreateAnonymousScratch(removeFailScratchFS{calls: new(atomic.Int32)},
			dir, RecoverDecryptTempPattern)
		if f != nil {
			t.Fatal("a scratch whose unlink failed was handed out")
		}
		if !errors.Is(err, ErrScratchNotUnlinked) {
			t.Fatalf("CreateAnonymousScratch = %v, want ErrScratchNotUnlinked", err)
		}
		left, _ := filepath.Glob(filepath.Join(dir, RecoverDecryptTempPattern))
		if len(left) != 1 {
			t.Fatalf("found %d scratch names, want the 1 whose removal was refused twice", len(left))
		}
		if info, statErr := os.Stat(left[0]); statErr != nil || info.Size() != 0 {
			t.Fatalf("the surviving scratch name must be empty (info=%v, err=%v)", info, statErr)
		}
		if got := StaleDecryptSweepFailures(); got == before {
			t.Fatal("a scratch name that survived the best-effort removal was not counted")
		}
	})

	t.Run("unlinked_at_once", func(t *testing.T) {
		dir := t.TempDir()
		f, err := CreateAnonymousScratch(osScratchFS{}, dir, RecoverDecryptTempPattern)
		if err != nil {
			t.Fatalf("CreateAnonymousScratch: %v", err)
		}
		defer func() { _ = f.Close() }()
		if _, statErr := os.Stat(f.Name()); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("the scratch still has a name after CreateAnonymousScratch (stat err=%v)", statErr)
		}
		if _, err := f.Write([]byte("readable through the handle")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			t.Fatalf("Seek: %v", err)
		}
		got, err := io.ReadAll(f)
		if err != nil || string(got) != "readable through the handle" {
			t.Fatalf("read back %q, %v", got, err)
		}
	})
}
