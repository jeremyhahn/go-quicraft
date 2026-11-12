package quicraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// rekeyRunbookSM records every proposed value in order. Lookup returns a copy
// of the list; the snapshot is the list joined by newlines. recoveries counts
// RecoverFromSnapshot calls across every instance the test creates.
type rekeyRunbookSM struct {
	mockStateMachine
	mu         sync.Mutex
	values     []string
	recoveries *atomic.Int64
}

func (s *rekeyRunbookSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		s.values = append(s.values, string(entries[i].Cmd))
		results[i] = sm.Result{Value: uint64(len(s.values))}
	}
	return nil
}

func (s *rekeyRunbookSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.values...), nil
}

func (s *rekeyRunbookSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := io.WriteString(w, strings.Join(s.values, "\n"))
	return err
}

func (s *rekeyRunbookSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = nil
	if len(data) > 0 {
		s.values = strings.Split(string(data), "\n")
	}
	s.recoveries.Add(1)
	return nil
}

// newRekeyRunbookBarrier builds a real barrier, as the shard-barrier posture
// tests do, and initialises it: Initialize draws a fresh root key and salt,
// so two calls give two barriers with independent root keys.
func newRekeyRunbookBarrier(ctx context.Context, t *testing.T, passphrase string) (*seal.Barrier, seal.SealingStrategy) {
	t.Helper()
	_ = seal.AllowWeakKDFParams()
	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = func(*seal.SealedRootKey) error { return nil }
	b := seal.MustNewBarrier(cfg)
	s, err := newTestSealStrategy([]byte(passphrase))
	if err != nil {
		t.Fatalf("newTestSealStrategy: %v", err)
	}
	if err := b.Initialize(ctx, s, seal.Credentials{}); err != nil {
		t.Fatalf("barrier Initialize: %v", err)
	}
	return b, s
}

// unsealRekeyRunbookBarrier unseals b again: Host.Close seals its barrier,
// and the runbook needs both barriers unsealed with the host stopped.
func unsealRekeyRunbookBarrier(ctx context.Context, t *testing.T, b *seal.Barrier, s seal.SealingStrategy) {
	t.Helper()
	if err := b.Unseal(ctx, s, seal.Credentials{}); err != nil {
		t.Fatalf("barrier Unseal: %v", err)
	}
}

func newRekeyRunbookHost(t *testing.T, walDir, nodeDir string, b *seal.Barrier) *Host {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.WALDir, cfg.NodeHostDir = walDir, nodeDir
	h, err := NewHost(cfg, WithoutTransport(), WithBarrier(b))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	return h
}

// syncReadRetry reads the shard's state, retrying while a freshly elected
// leader has not yet committed an entry in its term.
func syncReadRetry(ctx context.Context, t *testing.T, h *Host, shardID uint64) []string {
	t.Helper()
	for {
		got, err := h.SyncRead(ctx, shardID, nil)
		if errors.Is(err, ErrReadIndexNotReady) && ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("SyncRead: %v", err)
		}
		values, ok := got.([]string)
		if !ok {
			t.Fatalf("SyncRead returned %T, want []string", got)
		}
		return values
	}
}

// copyTreeExceptSnapshotData copies every directory and regular file under
// src to dst except snapshot data files, which the runbook re-encrypts with
// ReencryptSnapshotFile instead of copying.
func copyTreeExceptSnapshotData(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		if !d.Type().IsRegular() || d.Name() == "snapshot.dat" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
}

// assertReencryptLeftSource reports each file Reencrypt or
// ReencryptSnapshotFile removed, changed or created in a source directory,
// which the runbook requires them to leave untouched.
func assertReencryptLeftSource(t *testing.T, name string, before, after map[string]string) {
	t.Helper()
	for p, v := range before {
		w, ok := after[p]
		switch {
		case !ok:
			t.Errorf("%s: %s removed by Reencrypt", name, p)
		case w != v:
			t.Errorf("%s: %s changed by Reencrypt", name, p)
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s: %s created by Reencrypt", name, p)
		}
	}
}

// TestRekeyRunbook_NewBarrierServesOldBarrierRefuses runs the documented
// root-key compromise runbook (docs/seal-barrier/user.md, "Respond to a key
// compromise") against a real Host: data written under barrier A, copied
// with waldb.Reencrypt and ReencryptSnapshotFile under barrier B, is served
// by a Host on B and is unreadable under A, and the original directories are
// left as they were.
func TestRekeyRunbook_NewBarrierServesOldBarrierRefuses(t *testing.T) {
	const (
		shardID, replicaID uint64 = 1, 1
		beforeSnapshot            = 6
		afterSnapshot             = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	barrierA, strategyA := newRekeyRunbookBarrier(ctx, t, "rekey-runbook-old-root")
	barrierB, strategyB := newRekeyRunbookBarrier(ctx, t, "rekey-runbook-new-root")

	root := t.TempDir()
	walDir, nodeDir := filepath.Join(root, "wal"), filepath.Join(root, "node")
	newWAL, newNode := filepath.Join(root, "wal.new"), filepath.Join(root, "node.new")
	oldWAL, oldNode := filepath.Join(root, "wal.old"), filepath.Join(root, "node.old")
	members := map[uint64]string{replicaID: "127.0.0.1:5000"}
	var recoveries atomic.Int64
	create := func(_, _ uint64) interface{} { return &rekeyRunbookSM{recoveries: &recoveries} }

	// (a) A Host on barrier A proposes, snapshots, proposes more, closes.
	var want []string
	h := newRekeyRunbookHost(t, walDir, nodeDir, barrierA)
	if err := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard on barrier A: %v", err)
	}
	waitForLeader(t, h, shardID)
	propose := func(n int) {
		for i := 0; i < n; i++ {
			v := fmt.Sprintf("rekey-value-%02d", len(want))
			if _, err := h.SyncPropose(ctx, shardID, []byte(v)); err != nil {
				mustCloseHost(t, h)
				t.Fatalf("SyncPropose %q: %v", v, err)
			}
			want = append(want, v)
		}
	}
	propose(beforeSnapshot)
	snapIndex, err := h.SyncRequestSnapshot(ctx, shardID,
		SnapshotOption{CompactionOverhead: 0, OverrideCompactionOverhead: true})
	if err != nil {
		mustCloseHost(t, h)
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	propose(afterSnapshot)
	mustCloseHost(t, h)
	unsealRekeyRunbookBarrier(ctx, t, barrierA, strategyA)

	walBefore, nodeBefore := dirFingerprint(t, walDir), dirFingerprint(t, nodeDir)

	// (b) Reencrypt the WAL into new directories under barrier B; the
	// snapshot function re-encrypts each snapshot data file into the new
	// node directory at the same relative path.
	copyTreeExceptSnapshotData(t, nodeDir, newNode)
	var snapCalls int
	snapFn := func(sid, rid uint64, snap logdb.Snapshot) (uint64, error) {
		snapCalls++
		rel, relErr := filepath.Rel(nodeDir, snap.Filepath)
		if relErr != nil || strings.HasPrefix(rel, "..") {
			return 0, fmt.Errorf("snapshot %s is outside the node directory", snap.Filepath)
		}
		return ReencryptSnapshotFile(filepath.Join(snap.Filepath, "snapshot.dat"),
			filepath.Join(newNode, rel, "snapshot.dat"), sid, rid, snap.Index, barrierA, barrierB)
	}
	count, err := waldb.Reencrypt(walDir, barrierA, newWAL, barrierB, snapFn)
	if err != nil {
		t.Fatalf("waldb.Reencrypt: %v", err)
	}
	if count == 0 {
		t.Fatal("waldb.Reencrypt migrated 0 nodes, want > 0")
	}
	if snapCalls == 0 {
		t.Fatal("waldb.Reencrypt never called the snapshot function")
	}
	assertReencryptLeftSource(t, "WALDir after Reencrypt", walBefore, dirFingerprint(t, walDir))
	assertReencryptLeftSource(t, "NodeHostDir after Reencrypt", nodeBefore, dirFingerprint(t, nodeDir))

	// Swap the destination into place, keeping the originals aside.
	for _, mv := range [][2]string{{walDir, oldWAL}, {nodeDir, oldNode}, {newWAL, walDir}, {newNode, nodeDir}} {
		if err := os.Rename(mv[0], mv[1]); err != nil {
			t.Fatalf("swap %s -> %s: %v", mv[0], mv[1], err)
		}
	}

	// (c) A Host on barrier B recovers the shard from the snapshot and the
	// entries after it.
	h = newRekeyRunbookHost(t, walDir, nodeDir, barrierB)
	if err := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard on barrier B: %v", err)
	}
	waitForLeader(t, h, shardID)
	got := syncReadRetry(ctx, t, h, shardID)
	if err := h.StopShard(shardID); err != nil {
		t.Errorf("StopShard: %v", err)
	}
	mustCloseHost(t, h)
	unsealRekeyRunbookBarrier(ctx, t, barrierB, strategyB)
	if recoveries.Load() == 0 {
		t.Errorf("Host on barrier B never recovered from the snapshot at index %d", snapIndex)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Host on barrier B read %q, want %q", got, want)
	}

	// (d) The new WAL does not open under barrier A, the new snapshot does
	// not decrypt under it, and the originals are unchanged.
	if db, openErr := waldb.Open(walDir, waldb.WithBarrier(barrierA)); openErr == nil {
		_ = db.Close()
		t.Error("new WAL opened under barrier A, want an error")
	}
	newDB, err := waldb.Open(walDir, waldb.WithBarrier(barrierB))
	if err != nil {
		t.Fatalf("new WAL under barrier B: %v", err)
	}
	ss, err := newDB.GetSnapshot(shardID, replicaID)
	_ = newDB.Close()
	if err != nil {
		t.Fatalf("GetSnapshot from new WAL: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(ss.Filepath, "snapshot.dat"))
	if err != nil {
		t.Fatalf("read new snapshot: %v", err)
	}
	if _, decErr := decryptSnapshotFrames(barrierA, shardID, replicaID, ss.Index,
		bytes.NewReader(raw), io.Discard); decErr == nil {
		t.Error("new snapshot decrypted under barrier A, want an error")
	}
	assertReencryptLeftSource(t, "original WALDir", walBefore, dirFingerprint(t, oldWAL))
	assertReencryptLeftSource(t, "original NodeHostDir", nodeBefore, dirFingerprint(t, oldNode))
}
