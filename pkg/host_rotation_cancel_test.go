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
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// lockedLogBuffer is a bytes.Buffer safe for the host's background goroutines
// to log into while a test reads it.
type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// cancelAwareHostBarrier is a testHostBarrier whose Rotate and Purge can be
// made to behave as go-enclave does when the caller's context ends mid-call
// (ErrOpenFailed joined with ctx.Err()), or to fail with an error of their own
// regardless of the context.
type cancelAwareHostBarrier struct {
	*testHostBarrier
	rotateHonoursCtx bool
	rotateFailure    error
	purgeHonoursCtx  bool
	purgeFailure     error
}

func (b *cancelAwareHostBarrier) Rotate(ctx context.Context) (uint64, error) {
	if b.rotateHonoursCtx && ctx.Err() != nil {
		return 0, errors.Join(enclave.ErrOpenFailed, ctx.Err())
	}
	if b.rotateFailure != nil {
		return 0, b.rotateFailure
	}
	return b.testHostBarrier.Rotate(ctx)
}

func (b *cancelAwareHostBarrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error) {
	b.lastPurgeAt.Store(minEpoch)
	b.purgeCount.Add(1)
	if b.purgeHonoursCtx && ctx.Err() != nil {
		return 0, errors.Join(enclave.ErrOpenFailed, ctx.Err())
	}
	if b.purgeFailure != nil {
		return 0, b.purgeFailure
	}
	return 1, nil
}

// newRotationLogHost builds a host over barrier that logs at DEBUG into the
// returned buffer. withFloor starts a shard and retains an encrypted snapshot
// at epoch 2, so the global safe-purge floor is known and a rotation past
// MaxRetainedEpochs reaches PurgeEpochsBefore.
func newRotationLogHost(t *testing.T, barrier Barrier, withFloor bool) (*Host, *lockedLogBuffer) {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.MaxRetainedEpochs = 1
	logs := &lockedLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithBarrier(barrier), WithLogger(logger))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if withFloor {
		members := map[uint64]string{1: "127.0.0.1:5000"}
		if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
			t.Fatalf("StartShard: %v", err)
		}
		writeEncryptedSnapshotMeta(t, cfg.NodeHostDir, 1, 1, 100, 2)
	}
	return h, logs
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestRotateBarrierKey_LifetimeCancelAbandonsQuietly pins the shutdown path: a
// rotation the host's own lifetime cancellation cut short is logged at DEBUG
// and never reported as rotation being disabled.
func TestRotateBarrierKey_LifetimeCancelAbandonsQuietly(t *testing.T) {
	rb := &cancelAwareHostBarrier{testHostBarrier: newTestHostBarrier(5), rotateHonoursCtx: true}
	h, logs := newRotationLogHost(t, rb, false)

	h.rotateBarrierKey(cancelledContext())

	out := logs.String()
	if !strings.Contains(out, "level=DEBUG") || !strings.Contains(out, "key rotation abandoned") {
		t.Fatalf("want a DEBUG abandonment line, got:\n%s", out)
	}
	if strings.Contains(out, "DISABLED") {
		t.Fatalf("a cancelled rotation was reported as a misconfiguration:\n%s", out)
	}
	if rb.purgeCount.Load() != 0 {
		t.Fatal("PurgeEpochsBefore ran after an abandoned rotation")
	}
}

// TestRotateBarrierKey_RealFailureDuringStopStillLogsError pins that the
// shutdown carve-out is keyed to the CAUSE of the error, not to the context's
// state: a missing persister that surfaces while the host is closing is still
// the misconfiguration report, at ERROR.
func TestRotateBarrierKey_RealFailureDuringStopStillLogsError(t *testing.T) {
	rb := &cancelAwareHostBarrier{testHostBarrier: newTestHostBarrier(5), rotateFailure: seal.ErrNoStatePersister}
	h, logs := newRotationLogHost(t, rb, false)

	h.rotateBarrierKey(cancelledContext())

	out := logs.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "DISABLED") {
		t.Fatalf("want the ERROR misconfiguration report, got:\n%s", out)
	}
	if strings.Contains(out, "key rotation abandoned") {
		t.Fatalf("a persister failure was reported as a shutdown abandonment:\n%s", out)
	}
}

// TestRotateBarrierKey_PurgeLifetimeCancelLogsDebug pins the purge that
// follows a completed rotation: cut short by the lifetime cancellation, it is
// logged at DEBUG like the rotation itself, not at ERROR.
func TestRotateBarrierKey_PurgeLifetimeCancelLogsDebug(t *testing.T) {
	rb := &cancelAwareHostBarrier{testHostBarrier: newTestHostBarrier(9), purgeHonoursCtx: true}
	h, logs := newRotationLogHost(t, rb, true)

	h.rotateBarrierKey(cancelledContext())

	if rb.purgeCount.Load() == 0 {
		t.Fatal("PurgeEpochsBefore was not reached")
	}
	out := logs.String()
	if !strings.Contains(out, "epoch purge after rotation abandoned") {
		t.Fatalf("want a DEBUG purge-abandonment line, got:\n%s", out)
	}
	if strings.Contains(out, "epoch purge after rotation failed") {
		t.Fatalf("a cancelled purge was logged as a failure:\n%s", out)
	}
}

// TestRotateBarrierKey_PurgeRealFailureDuringStopLogsError is the purge's
// counterpart of the rotation rule: a real failure during stop logs at ERROR.
func TestRotateBarrierKey_PurgeRealFailureDuringStopLogsError(t *testing.T) {
	persistErr := &seal.StatePersistError{Op: "purge", Epoch: 10, Err: errors.New("disk full")}
	rb := &cancelAwareHostBarrier{testHostBarrier: newTestHostBarrier(9), purgeFailure: persistErr}
	h, logs := newRotationLogHost(t, rb, true)

	h.rotateBarrierKey(cancelledContext())

	out := logs.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "epoch purge after rotation failed") {
		t.Fatalf("want the ERROR purge failure, got:\n%s", out)
	}
	if strings.Contains(out, "epoch purge after rotation abandoned") {
		t.Fatalf("a persister failure was reported as a shutdown abandonment:\n%s", out)
	}
}
