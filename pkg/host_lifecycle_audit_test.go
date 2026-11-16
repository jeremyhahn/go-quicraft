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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/rsm"
)

// TestAudit_MapLocalReadError_StateMachineClosed checks the StopShard
// interaction of the local read paths: a read that passed the stopped check
// and then found the state machine closed by StopShard reports the host's
// ErrClosed, as the stopped check itself does, not the internal rsm error
// callers cannot match.
func TestAudit_MapLocalReadError_StateMachineClosed(t *testing.T) {
	err := mapLocalReadError(rsm.ErrClosed)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("mapLocalReadError(rsm.ErrClosed) = %v, want errors.Is ErrClosed", err)
	}
}

// TestAudit_MapLocalReadError_ContextAndPassthrough checks the request
// deadline interaction: a raw context error from a read abandoned behind an
// Update carries both the quicraft sentinel and the context error, an error
// already carrying a sentinel is returned unchanged, and nil stays nil.
func TestAudit_MapLocalReadError_ContextAndPassthrough(t *testing.T) {
	if err := mapLocalReadError(nil); err != nil {
		t.Fatalf("mapLocalReadError(nil) = %v, want nil", err)
	}
	for _, tc := range []struct {
		in       error
		sentinel error
	}{
		{context.Canceled, ErrCanceled},
		{context.DeadlineExceeded, ErrTimeout},
	} {
		got := mapLocalReadError(tc.in)
		if !errors.Is(got, tc.sentinel) || !errors.Is(got, tc.in) {
			t.Fatalf("mapLocalReadError(%v) = %v, want errors.Is %v and %v", tc.in, got, tc.sentinel, tc.in)
		}
	}
	already := mapContextError(context.DeadlineExceeded)
	if got := mapLocalReadError(already); got != already {
		t.Fatalf("mapLocalReadError(mapped) = %v, want it unchanged", got)
	}
	other := errors.New("user lookup failed")
	if got := mapLocalReadError(other); got != other {
		t.Fatalf("mapLocalReadError(user error) = %v, want it unchanged", got)
	}
}

// TestAudit_WithDeadlineCause checks the request-deadline mapping of the
// Sync* waits: an ErrTimeout returned once the caller's deadline has passed
// also matches context.DeadlineExceeded (a *MaybeAppliedError keeps its
// type); a timeout without a deadline, or under a context that was
// cancelled rather than timed out, is unchanged.
func TestAudit_WithDeadlineCause(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	got := withDeadlineCause(expired, ErrTimeout)
	if !errors.Is(got, ErrTimeout) || !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("withDeadlineCause(expired, ErrTimeout) = %v, want both ErrTimeout and DeadlineExceeded", got)
	}
	mae := &MaybeAppliedError{Err: ErrTimeout}
	got = withDeadlineCause(expired, mae)
	var gotMAE *MaybeAppliedError
	if !errors.As(got, &gotMAE) || !errors.Is(got, context.DeadlineExceeded) || !errors.Is(got, ErrTimeout) {
		t.Fatalf("withDeadlineCause(expired, MaybeApplied) = %T %v, want *MaybeAppliedError with both causes", got, got)
	}
	if got := withDeadlineCause(context.Background(), ErrTimeout); got != ErrTimeout {
		t.Fatalf("withDeadlineCause(no deadline) = %v, want ErrTimeout unchanged", got)
	}
	cancelled, cancelNow := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	cancelNow()
	if got := withDeadlineCause(cancelled, ErrTimeout); got != ErrTimeout {
		t.Fatalf("withDeadlineCause(cancelled) = %v, want ErrTimeout unchanged", got)
	}
	if got := withDeadlineCause(expired, ErrCanceled); got != ErrCanceled {
		t.Fatalf("withDeadlineCause(expired, ErrCanceled) = %v, want it unchanged", got)
	}
}

// TestAudit_LocalReadParkedBehindUpdateAcrossStopShard checks the StopShard
// interaction end to end: a QueryLocalNode parked behind a held Update when
// StopShard runs returns once the Update is released, with its result or the
// host's ErrClosed, never an internal error and never a hang.
func TestAudit_LocalReadParkedBehindUpdateAcrossStopShard(t *testing.T) {
	h, block, release := startBlockedShard(t)
	block()

	ctx, cancel := context.WithTimeout(context.Background(), contextCancelBound)
	defer cancel()
	readErr := make(chan error, 1)
	go func() {
		_, err := h.QueryLocalNode(ctx, 1, []byte("k"))
		readErr <- err
	}()
	// Let the read pass its stopped check and park behind the Update.
	time.Sleep(50 * time.Millisecond)

	stopErr := make(chan error, 1)
	go func() { stopErr <- h.StopShard(1) }()
	time.Sleep(50 * time.Millisecond)
	release()

	select {
	case err := <-stopErr:
		if err != nil {
			t.Fatalf("StopShard = %v", err)
		}
	case <-time.After(contextCancelBound):
		t.Fatal("StopShard did not return after the Update was released")
	}
	select {
	case err := <-readErr:
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("QueryLocalNode across StopShard = %v, want a result or ErrClosed", err)
		}
	case <-time.After(contextCancelBound):
		t.Fatal("QueryLocalNode parked behind the Update never returned after StopShard")
	}
}

// startStoppedJoiner starts a joined replica (no members, join=true) with the
// given DataDir, stops it, and returns the host after checking that its
// bootstrap record is the join form: Join set, no Addresses, DataDir recorded.
func startStoppedJoiner(t *testing.T, shardID, replicaID uint64, dataDir string) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithLogDB(newMockLogDB()), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	scfg := testShardConfig(shardID, replicaID)
	scfg.DataDir = dataDir
	if err := h.StartShard(nil, true, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard(join) failed: %v", err)
	}
	if err := h.StopShard(shardID); err != nil {
		t.Fatalf("StopShard failed: %v", err)
	}
	bs, ok, err := h.logdb.GetBootstrap(shardID, replicaID)
	if err != nil || !ok {
		t.Fatalf("GetBootstrap = ok %v, err %v; want the join's record", ok, err)
	}
	if !bs.Join || len(bs.Addresses) != 0 || bs.DataDir != dataDir {
		t.Fatalf("bootstrap = Join %v, Addresses %v, DataDir %q; want Join, none, %q",
			bs.Join, bs.Addresses, bs.DataDir, dataDir)
	}
	return h
}

// joinedDataDirs are the two layouts a joiner can record: the NodeHostDir
// tree (empty DataDir) and a custom DataDir.
func joinedDataDirs(t *testing.T) map[string]string {
	return map[string]string{"NodeHostDir": "", "custom DataDir": t.TempDir()}
}

// TestAudit_ResolveImportDataDir_JoinedRecordWithoutAddresses pins that the
// import path takes a joined replica's recorded DataDir as authoritative:
// the record has no Addresses, and none are needed to locate the tree.
func TestAudit_ResolveImportDataDir_JoinedRecordWithoutAddresses(t *testing.T) {
	const shardID, replicaID uint64 = 4431, 3
	for name, dataDir := range joinedDataDirs(t) {
		t.Run(name, func(t *testing.T) {
			h := startStoppedJoiner(t, shardID, replicaID, dataDir)
			got, err := h.resolveImportDataDir(shardID, replicaID, importOptions{})
			if err != nil || got != dataDir {
				t.Fatalf("resolveImportDataDir() = %q, %v; want the recorded %q", got, err, dataDir)
			}
			got, err = h.resolveImportDataDir(shardID, replicaID,
				importOptions{dataDir: dataDir, dataDirSet: true})
			if err != nil || got != dataDir {
				t.Fatalf("resolveImportDataDir(agreeing) = %q, %v; want %q", got, err, dataDir)
			}
			other := t.TempDir()
			_, err = h.resolveImportDataDir(shardID, replicaID,
				importOptions{dataDir: other, dataDirSet: true})
			var mm *ImportDataDirMismatchError
			if !errors.As(err, &mm) || mm.Recorded != dataDir || mm.Declared != other {
				t.Fatalf("resolveImportDataDir(disagreeing) = %v; want mismatch %q vs %q", err, dataDir, other)
			}
		})
	}
}

// TestAudit_RefuseDataDirChange_JoinedRecordWithoutAddresses pins that a
// joined replica's record, which has no Addresses, still guards its DataDir:
// a restart with the same DataDir is admitted and a changed one refused.
func TestAudit_RefuseDataDirChange_JoinedRecordWithoutAddresses(t *testing.T) {
	const shardID, replicaID uint64 = 4432, 3
	for name, dataDir := range joinedDataDirs(t) {
		t.Run(name, func(t *testing.T) {
			h := startStoppedJoiner(t, shardID, replicaID, dataDir)
			if err := h.refuseDataDirChange(shardID, replicaID, dataDir); err != nil {
				t.Fatalf("refuseDataDirChange(same) = %v, want nil", err)
			}
			other := t.TempDir()
			err := h.refuseDataDirChange(shardID, replicaID, other)
			var dc *dataDirChangedError
			if !errors.As(err, &dc) || dc.Recorded != dataDir || dc.Requested != other {
				t.Fatalf("refuseDataDirChange(changed) = %v; want dataDirChangedError %q -> %q", err, dataDir, other)
			}
			scfg := testShardConfig(shardID, replicaID)
			scfg.DataDir = dataDir
			if err := h.StartShard(nil, true, testCreateFunc, scfg); err != nil {
				t.Fatalf("restart with the recorded DataDir = %v, want nil", err)
			}
		})
	}
}
