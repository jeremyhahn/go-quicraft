package quicraft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// countingIdentitySM counts applied entries and carries the count through
// snapshots, so a restarted shard reports the state it recovered.
type countingIdentitySM struct {
	mockStateMachine
	mu    sync.Mutex
	count uint64
}

func (s *countingIdentitySM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		s.count++
		results[i] = sm.Result{Value: s.count}
	}
	return nil
}

func (s *countingIdentitySM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count, nil
}

func (s *countingIdentitySM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.count)
	_, err := w.Write(buf[:])
	return err
}

func (s *countingIdentitySM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count = binary.BigEndian.Uint64(buf[:])
	return nil
}

// dirFingerprint records every entry under root by relative path: its mode
// and, for regular files, its contents.
func dirFingerprint(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		v := info.Mode().String()
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			v += fmt.Sprintf(" %d %x", len(data), data)
		}
		out[rel] = v
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func assertSameFingerprint(t *testing.T, name string, before, after map[string]string) {
	t.Helper()
	for p, v := range before {
		w, ok := after[p]
		switch {
		case !ok:
			t.Errorf("%s: %s removed by the refused NewHost", name, p)
		case w != v:
			t.Errorf("%s: %s changed by the refused NewHost", name, p)
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s: %s created by the refused NewHost", name, p)
		}
	}
}

// TestNewHost_IdentityRestartRefusedLeavesDirectories starts a Host, applies
// entries, closes it, and restarts on the same directories under another
// DeploymentID: the restart is refused with ErrHostIdentityMismatch and
// leaves both directories unchanged; a restart under the original identity
// recovers the applied state. pkg/host_dirlock_test.go covers the refusal on
// directories that hold no shard state.
func TestNewHost_IdentityRestartRefusedLeavesDirectories(t *testing.T) {
	const (
		shardID, replicaID uint64 = 1, 1
		originalID         uint64 = 41
		otherID            uint64 = 42
		entries                   = 5
	)
	walDir, nodeDir := t.TempDir(), t.TempDir()
	members := map[uint64]string{replicaID: "127.0.0.1:5000"}
	create := func(_, _ uint64) interface{} { return &countingIdentitySM{} }

	h, err := newDirLockTestHost(t, walDir, nodeDir, originalID)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < entries; i++ {
		if _, err := h.SyncPropose(ctx, shardID, []byte(fmt.Sprintf("entry-%d", i))); err != nil {
			mustCloseHost(t, h)
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}
	mustCloseHost(t, h)

	walBefore, nodeBefore := dirFingerprint(t, walDir), dirFingerprint(t, nodeDir)

	other, err := newDirLockTestHost(t, walDir, nodeDir, otherID)
	if err == nil {
		mustCloseHost(t, other)
		t.Fatal("NewHost under another DeploymentID succeeded, want ErrHostIdentityMismatch")
	}
	if !errors.Is(err, ErrHostIdentityMismatch) {
		t.Fatalf("NewHost under another DeploymentID = %v, want ErrHostIdentityMismatch", err)
	}
	assertSameFingerprint(t, "WALDir", walBefore, dirFingerprint(t, walDir))
	assertSameFingerprint(t, "NodeHostDir", nodeBefore, dirFingerprint(t, nodeDir))

	h, err = newDirLockTestHost(t, walDir, nodeDir, originalID)
	if err != nil {
		t.Fatalf("NewHost under the original DeploymentID: %v", err)
	}
	defer mustCloseHost(t, h)
	if err := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard after restart: %v", err)
	}
	defer func() { _ = h.StopShard(shardID) }()
	waitForLeader(t, h, shardID)
	// A restarted leader serves reads once it commits an entry in its term;
	// until then SyncRead returns the retryable ErrReadIndexNotReady.
	var got interface{}
	for {
		got, err = h.SyncRead(ctx, shardID, nil)
		if !errors.Is(err, ErrReadIndexNotReady) || ctx.Err() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("SyncRead after restart: %v", err)
	}
	if n, ok := got.(uint64); !ok || n != entries {
		t.Fatalf("recovered state = %v, want %d applied entries", got, entries)
	}
}
