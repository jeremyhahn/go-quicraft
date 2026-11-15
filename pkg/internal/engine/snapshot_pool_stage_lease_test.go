package engine

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestSnapshotPool_SaveStagingIsFencedByUnload holds a save at the point
// just before it creates its staging parent, unloads the node (Stop, then
// the write-lease fence UnloadNode and RemoveData rely on), and then lets
// the save continue. The save must be refused with ErrStopped and must
// create nothing under the node's snapshot directory: a parent created after
// the fence would reappear inside a replica directory RemoveData has just
// removed, or is removing.
func TestSnapshotPool_SaveStagingIsFencedByUnload(t *testing.T) {
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
	dir := t.TempDir()
	node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(100), dir, nil)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func(n *Node) {
		if n != node {
			return
		}
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	saveStageHook.Store(&hook)
	t.Cleanup(func() { saveStageHook.Store(nil) })

	if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node, save: true}); err != nil {
		t.Fatalf("requestSnapshot: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the save never reached its staging step")
	}

	fenced := make(chan struct{})
	go func() {
		node.Stop()
		node.awaitWriteLeases()
		close(fenced)
	}()
	select {
	case <-fenced:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the unload fence waited for a save that holds no lease yet")
	}
	close(release)

	res := awaitSnapshotResult(t, p, "snapshot")
	if !errors.Is(res.err, ErrStopped) {
		t.Fatalf("a save whose node unloaded before its staging step reported %v, want ErrStopped", res.err)
	}
	if snap, err := ldb.GetSnapshot(10, 1); err == nil {
		t.Fatalf("a stopped node's save recorded snapshot %d", snap.Index)
	}
	canonical := snapshotSaveDir(dir, 10, 1, 100)
	for _, path := range []string{
		filepath.Join(dir, "shard-10"),
		filepath.Dir(canonical),
		canonical,
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("a save refused at the unload fence created %s (stat err %v)", path, err)
		}
	}
}

// exitingDiagnosticHandler is a slog.Handler that leaves its goroutine
// abnormally, through exit, on any record whose message contains match, and
// discards every other record.
type exitingDiagnosticHandler struct {
	match   string
	reached chan struct{}
	once    *sync.Once
	exit    func()
}

func (h exitingDiagnosticHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h exitingDiagnosticHandler) Handle(_ context.Context, r slog.Record) error {
	if !strings.Contains(r.Message, h.match) {
		return nil
	}
	h.once.Do(func() { close(h.reached) })
	h.exit()
	return nil
}

func (h exitingDiagnosticHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h exitingDiagnosticHandler) WithGroup(string) slog.Handler { return h }

// TestSnapshotPool_SaveStageDiagnosticExitReleasesLease: a save whose stage
// data file cannot be created, and whose stage cannot then be removed, logs
// that failure. A user slog.Handler that panics or calls runtime.Goexit on
// that diagnostic must not leave the save's short write lease held, or the
// unload waits for it forever.
func TestSnapshotPool_SaveStageDiagnosticExitReleasesLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit func()
	}{
		{"panic", func() { panic("injected diagnostic panic") }},
		{"goexit", runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopC := make(chan struct{})
			t.Cleanup(func() { close(stopC) })
			ldb := newSnapshotTestLogDB()
			p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
			var wg sync.WaitGroup
			p.start(&wg)

			cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
			node := NewNode(nil, nil, nil, cfg, newSnapshotTestSnapshotter(100), t.TempDir(), nil)

			prevOpen, prevRemove := saveStageOpenData, saveStageRemoveAll
			saveStageOpenData = func(string) (*os.File, error) {
				return nil, errors.New("injected data file create failure")
			}
			saveStageRemoveAll = func(string) error {
				return errors.New("injected stage removal failure")
			}
			t.Cleanup(func() { saveStageOpenData, saveStageRemoveAll = prevOpen, prevRemove })

			reached := make(chan struct{})
			prevLog := slog.Default()
			slog.SetDefault(slog.New(exitingDiagnosticHandler{
				match:   "failed to remove a save staging directory",
				reached: reached,
				once:    &sync.Once{},
				exit:    tc.exit,
			}))
			t.Cleanup(func() { slog.SetDefault(prevLog) })

			if err := p.requestSnapshot(snapshotRequest{shardID: 10, replicaID: 1, node: node, save: true}); err != nil {
				t.Fatalf("requestSnapshot: %v", err)
			}
			select {
			case <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("the save never logged its stage cleanup failure")
			}

			fenced := make(chan struct{})
			go func() {
				node.Stop()
				node.awaitWriteLeases()
				close(fenced)
			}()
			select {
			case <-fenced:
			case <-time.After(10 * time.Second):
				t.Fatal("the unload waited forever on a lease the abnormal diagnostic exit leaked")
			}
		})
	}
}
