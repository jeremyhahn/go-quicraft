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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// midSaveStep bounds every wait in the mid-save tests, so a hang fails the
// test at the step that hung instead of at the package timeout.
const midSaveStep = 15 * time.Second

// midSaveGate holds the first SaveSnapshot that reaches it until release is
// closed. It is shared by every incarnation of the shard's state machine, so
// once released, later saves (after the restart) run straight through.
type midSaveGate struct {
	entered       chan struct{} // closed when the first save has begun
	release       chan struct{} // closed by the test to unblock that save
	returned      chan struct{} // closed when that save has returned
	stopperClosed chan struct{} // closed when a blocked save's stopper closes
	enterOnce     sync.Once
	returnOnce    sync.Once
	stopperOnce   sync.Once
	releaseOnce   sync.Once
}

func newMidSaveGate() *midSaveGate {
	return &midSaveGate{
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
		returned:      make(chan struct{}),
		stopperClosed: make(chan struct{}),
	}
}

// releaseSave unblocks the gated save. It is idempotent, so every cleanup
// path may call it.
func (g *midSaveGate) releaseSave() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// midSaveState is the state machine's whole state: how many entries it has
// applied and a hash chained over their payloads in apply order.
type midSaveState struct {
	count uint64
	sum   [sha256.Size]byte
}

func (s midSaveState) apply(cmd []byte) midSaveState {
	h := sha256.New()
	h.Write(s.sum[:])
	h.Write(cmd)
	var next midSaveState
	next.count = s.count + 1
	copy(next.sum[:], h.Sum(nil))
	return next
}

// midSaveSM is a hashing state machine whose SaveSnapshot blocks on the gate.
// It deliberately ignores the stopper: the worst case a library must survive.
type midSaveSM struct {
	gate  *midSaveGate
	mu    sync.Mutex
	state midSaveState
}

func (m *midSaveSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range entries {
		m.state = m.state.apply(e.Cmd)
		results[i] = sm.Result{Value: e.Index}
	}
	return nil
}

func (m *midSaveSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}

func (m *midSaveSM) SaveSnapshot(_ context.Context, w io.Writer, stopper <-chan struct{}) error {
	m.mu.Lock()
	st := m.state
	m.mu.Unlock()

	first := false
	m.gate.enterOnce.Do(func() { first = true; close(m.gate.entered) })
	if first {
		defer m.gate.returnOnce.Do(func() { close(m.gate.returned) })
	}
	// Record that the stopper closed, but keep waiting for the release: the
	// save's own return still ignores the stopper.
	select {
	case <-m.gate.release:
	case <-stopper:
		m.gate.stopperOnce.Do(func() { close(m.gate.stopperClosed) })
		<-m.gate.release
	}

	var buf [8 + sha256.Size]byte
	binary.BigEndian.PutUint64(buf[:8], st.count)
	copy(buf[8:], st.sum[:])
	_, err := w.Write(buf[:])
	return err
}

func (m *midSaveSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8 + sha256.Size]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.count = binary.BigEndian.Uint64(buf[:8])
	copy(m.state.sum[:], buf[8:])
	return nil
}

func (m *midSaveSM) Close(_ context.Context) error { return nil }

// midSaveFixture is one shard on a Host with a persistent WAL, plus the
// directories a restart reopens.
type midSaveFixture struct {
	cfg      config.HostConfig
	gate     *midSaveGate
	expected midSaveState
}

func newMidSaveFixture(t *testing.T) *midSaveFixture {
	t.Helper()
	cfg := testHostConfig(t)
	f := &midSaveFixture{cfg: cfg, gate: newMidSaveGate()}
	// A failed step must never leave the save blocked: engine.Stop joins
	// the snapshot worker, so an unreleased save would hang Close.
	t.Cleanup(f.gate.releaseSave)
	return f
}

// closeHost releases the gated save and closes h, failing the test if Close
// does not return within midSaveStep. It releases the save itself, so the
// order of cleanups does not matter.
func (f *midSaveFixture) closeHost(t *testing.T, h *Host) {
	t.Helper()
	f.gate.releaseSave()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.Close()
	}()
	select {
	case <-done:
	case <-time.After(midSaveStep):
		t.Errorf("Host.Close did not return within %v", midSaveStep)
	}
}

func (f *midSaveFixture) openHost(t *testing.T) *Host {
	t.Helper()
	h, err := NewHost(f.cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	return h
}

func (f *midSaveFixture) startShard(ctx context.Context, t *testing.T, h *Host) {
	t.Helper()
	create := func(_, _ uint64) interface{} { return &midSaveSM{gate: f.gate} }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	f.awaitLeader(ctx, t, h)
}

// awaitLeader waits, bounded by midSaveStep and ctx, until the host's
// leadership view reports this replica as leader of shard 1. The view is
// updated asynchronously by the engine's event dispatcher, so the raft peer
// may already serve a ReadIndex while Propose's pre-flight check still sees
// no leader; waiting on ShardInfo makes the first proposal admissible.
func (f *midSaveFixture) awaitLeader(ctx context.Context, t *testing.T, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, midSaveStep)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		info, err := h.ShardInfo(1)
		if err == nil && info.IsLeader {
			return
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("shard 1 did not report a local leader within %v (last ShardInfo error: %v)",
				midSaveStep, lastErr)
			return
		case <-ticker.C:
		}
	}
}

// readState reads the state machine's state, retrying until the shard can
// serve a linearizable read or the step deadline passes.
func (f *midSaveFixture) readState(t *testing.T, h *Host) midSaveState {
	t.Helper()
	deadline := time.Now().Add(midSaveStep)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		v, err := h.SyncRead(ctx, 1, nil)
		cancel()
		if err == nil {
			st, ok := v.(midSaveState)
			if !ok {
				t.Fatalf("SyncRead returned %T, want midSaveState", v)
			}
			return st
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("shard did not serve a read within %v: %v", midSaveStep, lastErr)
	return midSaveState{}
}

func (f *midSaveFixture) propose(ctx context.Context, t *testing.T, h *Host, n int) {
	t.Helper()
	for i := range n {
		cmd := []byte(fmt.Sprintf("entry-%d", f.expected.count))
		if err := f.proposeOne(ctx, h, cmd); err != nil {
			t.Fatalf("SyncPropose #%d: %v", i, err)
		}
		f.expected = f.expected.apply(cmd)
	}
}

// proposeOne submits cmd, retrying only a NotLeaderError that is not
// MaybeApplied until midSaveStep elapses. That error is a refusal before
// admission, so a retry cannot apply the command twice. Any other error, including a timeout (the proposal may
// have committed), is returned at once; on the deadline the last error is.
func (f *midSaveFixture) proposeOne(ctx context.Context, h *Host, cmd []byte) error {
	ctx, cancel := context.WithTimeout(ctx, midSaveStep)
	defer cancel()
	for {
		_, err := h.SyncPropose(ctx, 1, cmd)
		if err == nil || !errors.Is(err, ErrNotLeader) || MaybeApplied(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// beginBlockedSave requests a snapshot and waits until the state machine is
// inside SaveSnapshot. It returns the request so the caller can see it end.
func (f *midSaveFixture) beginBlockedSave(t *testing.T, h *Host) *RequestState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), midSaveStep)
	defer cancel()
	rs, err := h.RequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot: %v", err)
	}
	awaitMidSave(t, f.gate.entered, "the save to begin")
	return rs
}

// stopDuringSave runs stop while the save is blocked, waits until stop has
// closed the save's stopper, unblocks the save, and waits for both stop and
// the save to return. Stop may or may not wait for the save; either is
// allowed, but neither may hang, and the save is released only once it has
// been told to stop, so the cleanup checked is that of an interrupted save.
func (f *midSaveFixture) stopDuringSave(t *testing.T, what string, stop func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- stop() }()
	select {
	case <-f.gate.stopperClosed:
	case <-time.After(midSaveStep):
		t.Fatalf("%s did not close the blocked save's stopper within %v", what, midSaveStep)
	}
	f.gate.releaseSave()
	var stopErr error
	select {
	case stopErr = <-done:
	case <-time.After(midSaveStep):
		t.Fatalf("%s did not return within %v of the save being released", what, midSaveStep)
	}
	awaitMidSave(t, f.gate.returned, "the blocked save to return")
	return stopErr
}

func awaitMidSave(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(midSaveStep):
		t.Fatalf("timed out after %v waiting for %s", midSaveStep, what)
	}
}

func awaitRequestEnd(t *testing.T, rs *RequestState) {
	t.Helper()
	select {
	case <-rs.ResultC():
	case <-time.After(midSaveStep):
		t.Fatalf("the snapshot request of a stopped shard did not complete within %v", midSaveStep)
	}
}

// assertNoPartialSnapshot fails if the snapshot tree holds a save or receive
// staging directory, a scratch temp file, or a snapshot directory missing
// its data or metadata file.
func assertNoPartialSnapshot(t *testing.T, nodeHostDir string) {
	t.Helper()
	for _, p := range partialSnapshotArtifacts(t, nodeHostDir) {
		t.Error(p)
	}
}

// awaitNoPartialSnapshot polls until no partial snapshot artifact remains
// under nodeHostDir, failing with the last listing after the deadline. A
// stopped shard's request completes before its pool worker's save returns,
// and the worker's deferred stage removal runs only then, so the check that
// follows StopShard must wait for that worker rather than race it.
func awaitNoPartialSnapshot(t *testing.T, nodeHostDir string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		problems := partialSnapshotArtifacts(t, nodeHostDir)
		if len(problems) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, p := range problems {
				t.Errorf("after %v: %s", within, p)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// partialSnapshotArtifacts lists every staging, receive, import or scratch
// artifact, every .tmp file and every snapshot directory missing its data or
// metadata file under nodeHostDir's snapshot root.
func partialSnapshotArtifacts(t *testing.T, nodeHostDir string) []string {
	t.Helper()
	var problems []string
	root := filepath.Join(nodeHostDir, "snapshots")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		name := d.Name()
		if engine.IsSnapshotSaveStage(name) || engine.IsSnapshotRecvStage(name) ||
			engine.IsSnapshotImportStage(name) || engine.IsSnapshotScratchTemp(name) ||
			strings.HasSuffix(name, ".tmp") {
			problems = append(problems, "partial snapshot artifact left behind: "+path)
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() && strings.HasPrefix(name, "snapshot-") {
			for _, f := range []string{"snapshot.dat", "snapshot.meta"} {
				if _, statErr := os.Stat(filepath.Join(path, f)); statErr != nil {
					problems = append(problems, fmt.Sprintf("snapshot directory %s has no %s: %v", path, f, statErr))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return problems
}

// assertRestartedCleanly checks the restarted shard: no partial snapshot,
// the state equals the applied entries, and a new snapshot succeeds.
func (f *midSaveFixture) assertRestartedCleanly(t *testing.T, h *Host) {
	t.Helper()
	assertNoPartialSnapshot(t, f.cfg.NodeHostDir)
	if got := f.readState(t, h); got != f.expected {
		t.Fatalf("restarted state = (count %d, hash %x), want (count %d, hash %x)",
			got.count, got.sum, f.expected.count, f.expected.sum)
	}
	f.propose(context.Background(), t, h, 2)
	ctx, cancel := context.WithTimeout(context.Background(), midSaveStep)
	defer cancel()
	if _, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{}); err != nil {
		t.Fatalf("SyncRequestSnapshot after the restart: %v", err)
	}
	assertNoPartialSnapshot(t, f.cfg.NodeHostDir)
	if got := f.readState(t, h); got != f.expected {
		t.Fatalf("state after the post-restart snapshot = (count %d), want (count %d)",
			got.count, f.expected.count)
	}
}

// TestMidSave_StopShardLeavesNoPartialSnapshot: StopShard while the state
// machine is inside SaveSnapshot, release the save, StartShard again on the
// same Host and directories.
func TestMidSave_StopShardLeavesNoPartialSnapshot(t *testing.T) {
	f := newMidSaveFixture(t)
	h := f.openHost(t)
	defer f.closeHost(t, h)
	f.startShard(context.Background(), t, h)
	f.readState(t, h)
	f.propose(context.Background(), t, h, 5)

	rs := f.beginBlockedSave(t, h)
	if err := f.stopDuringSave(t, "StopShard", func() error { return h.StopShard(1) }); err != nil {
		t.Fatalf("StopShard during a save: %v", err)
	}
	awaitRequestEnd(t, rs)
	awaitNoPartialSnapshot(t, f.cfg.NodeHostDir, 5*time.Second)

	f.startShard(context.Background(), t, h)
	f.assertRestartedCleanly(t, h)
}

// TestMidSave_HostCloseLeavesNoPartialSnapshot: Host.Close while the state
// machine is inside SaveSnapshot, release the save, open a new Host on the
// same directories and start the shard.
func TestMidSave_HostCloseLeavesNoPartialSnapshot(t *testing.T) {
	f := newMidSaveFixture(t)
	h := f.openHost(t)
	// Bounds h's cleanup if a step fails before or during the Close under
	// test; on success this second Close returns at once.
	defer f.closeHost(t, h)
	f.startShard(context.Background(), t, h)
	f.readState(t, h)
	f.propose(context.Background(), t, h, 5)

	rs := f.beginBlockedSave(t, h)
	if err := f.stopDuringSave(t, "Host.Close", h.Close); err != nil {
		// Close reports a drain that abandoned in-flight work; the snapshot
		// request is such work, so an error here is not itself a failure.
		t.Logf("Host.Close during a save: %v", err)
	}
	awaitRequestEnd(t, rs)
	assertNoPartialSnapshot(t, f.cfg.NodeHostDir)

	h2 := f.openHost(t)
	defer f.closeHost(t, h2)
	f.startShard(context.Background(), t, h2)
	f.assertRestartedCleanly(t, h2)
}
