package quicraft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// newEmergencySealHost starts a Host with a real seal.Barrier, one shard
// built by create, and returns it with its host directory.
func newEmergencySealHost(t *testing.T, create func(uint64, uint64) interface{}) (*Host, string) {
	t.Helper()
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithBarrier(createTestBarrier(t)))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	return h, cfg.NodeHostDir
}

// TestEmergencySeal_LiveHost_RefusesKeyUsingOps: after EmergencySeal on a
// live Host the barrier reports sealed, HealthCheck says "sealed", and every
// key-using call returns ErrBarrierSealed.
func TestEmergencySeal_LiveHost_RefusesKeyUsingOps(t *testing.T) {
	h, _ := newEmergencySealHost(t, testCreateFunc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		if _, err := h.SyncPropose(ctx, 1, []byte("before-seal")); err != nil {
			t.Fatalf("SyncPropose before seal: %v", err)
		}
	}
	if h.IsSealed() {
		t.Fatal("IsSealed before EmergencySeal = true")
	}
	if got := h.HealthCheck().Status; got != "ok" {
		t.Fatalf("HealthCheck().Status before seal = %q, want ok", got)
	}

	if err := h.EmergencySeal(); err != nil {
		t.Fatalf("EmergencySeal: %v", err)
	}
	if !h.IsSealed() {
		t.Error("IsSealed after EmergencySeal = false")
	}
	hs := h.HealthCheck()
	if hs.Status != "sealed" || !hs.BarrierSealed {
		t.Errorf("HealthCheck after seal: Status %q BarrierSealed %v, want sealed/true", hs.Status, hs.BarrierSealed)
	}

	if _, err := h.SyncPropose(ctx, 1, []byte("after-seal")); !errors.Is(err, ErrBarrierSealed) {
		t.Errorf("SyncPropose after seal: err = %v, want ErrBarrierSealed", err)
	}
	if _, err := h.SyncRead(ctx, 1, "q"); !errors.Is(err, ErrBarrierSealed) {
		t.Errorf("SyncRead after seal: err = %v, want ErrBarrierSealed", err)
	}
	if _, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{}); !errors.Is(err, ErrBarrierSealed) {
		t.Errorf("SyncRequestSnapshot after seal: err = %v, want ErrBarrierSealed", err)
	}
}

// blockingSnapshotSM holds SaveSnapshot until released, so a test can seal
// the barrier while the save is in flight.
type blockingSnapshotSM struct {
	mockStateMachine
	entered chan struct{}
	release chan struct{}
}

func (m *blockingSnapshotSM) SaveSnapshot(_ context.Context, w io.Writer, stopper <-chan struct{}) error {
	close(m.entered)
	select {
	case <-m.release:
	case <-stopper:
		return errors.New("snapshot save stopped")
	}
	_, err := w.Write(bytes.Repeat([]byte("emergency-seal-payload"), 256))
	return err
}

// TestEmergencySeal_DuringSnapshotSave: a seal that lands while a snapshot
// save is in progress fails that save with ErrBarrierSealed and leaves no
// partial snapshot directory behind.
func TestEmergencySeal_DuringSnapshotSave(t *testing.T) {
	sm := &blockingSnapshotSM{entered: make(chan struct{}), release: make(chan struct{})}
	h, dir := newEmergencySealHost(t, func(uint64, uint64) interface{} { return sm })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.SyncPropose(ctx, 1, []byte("before-seal")); err != nil {
		t.Fatalf("SyncPropose before seal: %v", err)
	}

	saveErr := make(chan error, 1)
	go func() {
		_, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
		saveErr <- err
	}()
	select {
	case <-sm.entered:
	case <-ctx.Done():
		t.Fatal("SaveSnapshot never started")
	}

	if err := h.EmergencySeal(); err != nil {
		t.Fatalf("EmergencySeal: %v", err)
	}
	close(sm.release)

	select {
	case err := <-saveErr:
		if !errors.Is(err, ErrBarrierSealed) {
			t.Errorf("snapshot save sealed mid-flight: err = %v, want ErrBarrierSealed", err)
		}
	case <-ctx.Done():
		t.Fatal("snapshot save did not finish after the seal")
	}

	walkErr := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if isSaveArtifact(d.Name()) {
			t.Errorf("partial snapshot left on disk after sealed save: %s", p)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk host dir: %v", walkErr)
	}

	// The replica's own snapshot directory is where a local save stages and
	// publishes; it must hold no save artifact after the sealed failure.
	replicaDir := engine.ReplicaSnapshotDir(shardSnapshotBaseDir(dir, testShardConfig(1, 1).DataDir, 1), 1, 1)
	entries, err := os.ReadDir(replicaDir)
	if err != nil {
		t.Fatalf("read replica snapshot dir %s: %v", replicaDir, err)
	}
	for _, e := range entries {
		if isSaveArtifact(e.Name()) {
			t.Errorf("save artifact left in the replica snapshot dir after sealed save: %s",
				filepath.Join(replicaDir, e.Name()))
		}
	}
}

// isSaveArtifact reports whether a directory entry name is anything a local
// snapshot save writes before or at publication: a published snapshot-<index>
// directory, a save staging directory (engine.IsSnapshotSaveStage), the staged
// snapshot.dat, or a *.tmp file.
func isSaveArtifact(name string) bool {
	return strings.HasPrefix(name, "snapshot-") ||
		strings.HasSuffix(name, ".tmp") ||
		engine.IsSnapshotSaveStage(name) ||
		name == "snapshot.dat"
}
