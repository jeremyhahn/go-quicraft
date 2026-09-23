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
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Shutdown is the last durability signal the embedding application ever gets.
// For an on-disk state machine — go-qrdb's Pebble store — the Close it runs is
// the store's final flush-and-close, so a failed flush that shutdown only LOGS
// reports a clean stop for a shard that lost writes. StopShard and Host.Close
// therefore surface it instead.

// errCloseSM fails Close with a fixed error, modelling a backing store whose
// final flush failed.
var errCloseSM = errors.New("test: state machine flush failed on close")

type closeFailingSM struct{}

func (*closeFailingSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

func (*closeFailingSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

func (*closeFailingSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (*closeFailingSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (*closeFailingSM) Close(_ context.Context) error { return errCloseSM }

var _ sm.StateMachine = (*closeFailingSM)(nil)

func closeFailingCreateFunc(uint64, uint64) interface{} { return &closeFailingSM{} }

// TestStopShard_ReportsStateMachineCloseFailure requires StopShard to return the
// user state machine's Close error rather than swallowing it into a log line.
func TestStopShard_ReportsStateMachineCloseFailure(t *testing.T) {
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		closeFailingCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}

	err := h.StopShard(1)
	if !errors.Is(err, errCloseSM) {
		t.Fatalf("StopShard = %v, want the state machine's Close error (%v). A "+
			"stop that reports success over a failed final flush is the last thing "+
			"the application hears about data it believes is durable", err, errCloseSM)
	}

	// The teardown must still have completed: an error return means "this went
	// wrong", not "the shard is still loaded".
	if _, loaded := h.nodes.Load(uint64(1)); loaded {
		t.Fatal("the shard is still in h.nodes after StopShard returned; the close " +
			"error must not abort the teardown")
	}
	if got := h.ShardCount(); got != 0 {
		t.Fatalf("ShardCount = %d after StopShard, want 0", got)
	}
}

// TestHostClose_JoinsStateMachineCloseFailure requires Host.Close to carry the
// same error out. Close previously returned only the LogDB's error, so a failed
// state machine flush was invisible to the caller.
func TestHostClose_JoinsStateMachineCloseFailure(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		closeFailingCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}

	closeErr := h.Close()
	if !errors.Is(closeErr, errCloseSM) {
		t.Fatalf("Host.Close = %v, want it to carry the state machine's Close error "+
			"(%v). A nil return means every shard's store flushed, and this one did "+
			"not", closeErr, errCloseSM)
	}
}

// TestHostClose_ReportsIncompleteDrain pins that a Drain which timed out
// reaches the caller. Close proceeds with shutdown either way — that is the
// point of the timeout — but a timed-out drain ABANDONED in-flight work, and a
// nil return says the opposite.
func TestHostClose_ReportsIncompleteDrain(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}

	// One in-flight operation that never completes: Drain waits out
	// ShutdownTimeout and gives up.
	h.addPendingOp()

	closeErr := h.Close()
	if !errors.Is(closeErr, ErrShutdownTimeout) {
		t.Fatalf("Host.Close with an operation that outlives the drain = %v, want it "+
			"to carry ErrShutdownTimeout. Returning nil tells the application its "+
			"shutdown was clean when work it accepted was dropped", closeErr)
	}
}

// TestHostDrain_ConcurrentCallersJoinTheSameAttempt pins that a second Drain
// — Close's, in the case that matters — waits for the attempt already in
// progress and reports ITS result, instead of returning nil immediately. The
// second caller used to see draining=true and return success at once, so a
// Close racing an explicit Drain proceeded to tear the engine down under the
// first drain and both reported a clean shutdown after aborting accepted work.
func TestHostDrain_ConcurrentCallersJoinTheSameAttempt(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	// One operation that outlives the whole test: the elected attempt waits
	// out ShutdownTimeout, and every joined caller must wait with it.
	h.addPendingOp()

	const callers = 4
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	results := make(chan outcome, callers)
	start := time.Now()
	for range callers {
		go func() {
			err := h.Drain()
			results <- outcome{err: err, elapsed: time.Since(start)}
		}()
	}
	for i := range callers {
		got := <-results
		if !errors.Is(got.err, ErrShutdownTimeout) {
			t.Fatalf("Drain caller %d = %v, want ErrShutdownTimeout: every caller must "+
				"report the one attempt's outcome", i, got.err)
		}
		// A caller that returned before the attempt could possibly have
		// timed out did not join it. Half the timeout is a generous bound.
		if got.elapsed < cfg.ShutdownTimeout/2 {
			t.Fatalf("Drain caller %d returned after %v, before the attempt's %v "+
				"timeout could elapse: it did not wait for the drain in progress",
				i, got.elapsed, cfg.ShutdownTimeout)
		}
	}

	// A call after the attempt has completed returns the recorded result
	// without waiting again.
	before := time.Now()
	if err := h.Drain(); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Drain after completion = %v, want the recorded ErrShutdownTimeout", err)
	}
	if waited := time.Since(before); waited > cfg.ShutdownTimeout/2 {
		t.Fatalf("Drain after completion waited %v; the recorded result should return at once", waited)
	}
	// Close joins the same attempt and carries its error, as before.
	if err := h.Close(); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Close after a timed-out drain = %v, want ErrShutdownTimeout", err)
	}
}

// TestHostClose_JoinsEveryShardsCloseFailure pins that the collection does not
// stop at the first failure: one shard's failed flush must not hide another's,
// and every shard must still be closed so no on-disk file lock is stranded.
func TestHostClose_JoinsEveryShardsCloseFailure(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	for _, shardID := range []uint64{1, 2, 3} {
		if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
			closeFailingCreateFunc, testShardConfig(shardID, 1)); err != nil {
			t.Fatalf("StartShard(%d): %v", shardID, err)
		}
	}

	closeErr := h.Close()
	if !errors.Is(closeErr, errCloseSM) {
		t.Fatalf("Host.Close = %v, want the shards' Close error (%v)", closeErr, errCloseSM)
	}
	if got := h.ShardCount(); got != 0 {
		t.Fatalf("ShardCount = %d after Close, want 0: a shard whose Close failed "+
			"must still be unloaded, or its file lock strands the next host start", got)
	}
}

// closeFailingImportFS is the real filesystem with the snapshot data file's
// Close failing AFTER its Sync succeeded, which is how a full volume, an
// exceeded quota or a network file system delivers a deferred write error.
type closeFailingImportFS struct {
	osSnapshotFS
}

func (f closeFailingImportFS) OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error) {
	inner, err := f.osSnapshotFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultySnapshotFile{snapshotFile: inner, failClose: true}, nil
}

var _ snapshotFS = closeFailingImportFS{}

// TestWriteImportedSnapshotData_ReportsCloseFailure requires the import's
// post-Sync Close error to be RETURNED.
//
// Sync flushes what the process wrote; Close is where a deferred write failure
// surfaces, and on some platforms it is the only place it is ever delivered.
// Dropping it let the import continue to SaveSnapshot — recording the artifact
// as the shard's durable snapshot — and then to RemoveEntriesTo, which DELETES
// the log entries the snapshot subsumes. The entries are gone and a possibly
// short snapshot is all that is left to replace them.
func TestWriteImportedSnapshotData_ReportsCloseFailure(t *testing.T) {
	h := &Host{}
	dataPath := filepath.Join(t.TempDir(), "snapshot.dat")

	err := h.writeImportedSnapshotData(context.Background(), closeFailingImportFS{}, 1, dataPath,
		strings.NewReader("an imported state machine image"))
	if !errors.Is(err, errTestCloseFailed) {
		t.Fatalf("writeImportedSnapshotData = %v, want the file's Close error. The "+
			"import goes on to record this artifact as the shard's snapshot and to "+
			"delete the log entries it subsumes, so a dropped Close is a possibly "+
			"short snapshot standing in for entries that no longer exist", err)
	}
}
