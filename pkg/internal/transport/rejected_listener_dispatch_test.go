// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
)

// newRejectedListenerDispatchTransport builds a QUICTransport (not started)
// whose Config carries listener, and stops it at the end of the test.
func newRejectedListenerDispatchTransport(ctx context.Context, t *testing.T, listener *config.EventListener) *QUICTransport {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before the transport was built: %v", err)
	}
	cfg := Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
		DialTimeout:   testDialTimeout,
		EventListener: listener,
	}
	tr, err := NewQUICTransport(cfg, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	t.Cleanup(func() { _ = tr.Stop() })
	return tr
}

// rejectTransferRejectedListenerDispatch drives one transfer the receiver
// rejects (its stop channel is already closed) and waits for it to return.
func rejectTransferRejectedListenerDispatch(ctx context.Context, t *testing.T, tr *QUICTransport, hdr *SnapshotHeader) {
	t.Helper()
	stopC := make(chan struct{})
	close(stopC)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		tr.snapshotRecv.receiveSnapshot(nil, hdr, "10.0.0.8:5000", PeerAuth{}, stopC)
	}()
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("receiveSnapshot did not return for a rejected transfer")
	}
}

func TestRejectedListenerDispatchQueuesDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var calls atomic.Int32
	var got []config.SnapshotInfo
	var mu sync.Mutex
	tr := newRejectedListenerDispatchTransport(ctx, t, &config.EventListener{
		OnSnapshotRejected: func(info config.SnapshotInfo) {
			mu.Lock()
			got = append(got, info)
			mu.Unlock()
		},
	})
	var queued []func()
	tr.SetListenerDispatcher(func(fn func()) bool {
		calls.Add(1)
		mu.Lock()
		queued = append(queued, fn)
		mu.Unlock()
		return true
	})

	rejectTransferRejectedListenerDispatch(ctx, t, tr, &SnapshotHeader{ShardID: 4, ReplicaID: 2, Index: 11, ChunkCount: 3})

	if n := calls.Load(); n != 1 {
		t.Fatalf("dispatcher called %d times, want 1", n)
	}
	mu.Lock()
	ran, fns := len(got), queued
	mu.Unlock()
	if ran != 0 {
		t.Fatalf("OnSnapshotRejected ran %d times before the queued function ran", ran)
	}
	for _, fn := range fns {
		fn()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("OnSnapshotRejected ran %d times after the queued function ran, want 1", len(got))
	}
	if info := got[0]; info.ShardID != 4 || info.ReplicaID != 2 || info.Index != 11 {
		t.Fatalf("OnSnapshotRejected info = %+v, want shard 4 replica 2 index 11", info)
	}
}

func TestRejectedListenerDispatchRefusedIsDroppedAndLogged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	var ran atomic.Int32
	tr := newRejectedListenerDispatchTransport(ctx, t, &config.EventListener{
		OnSnapshotRejected: func(config.SnapshotInfo) { ran.Add(1) },
	})
	var calls atomic.Int32
	tr.SetListenerDispatcher(func(func()) bool {
		calls.Add(1)
		return false
	})

	rejectTransferRejectedListenerDispatch(ctx, t, tr, &SnapshotHeader{ShardID: 7, ReplicaID: 3, Index: 19, ChunkCount: 3})

	if n := calls.Load(); n != 1 {
		t.Fatalf("dispatcher called %d times, want 1", n)
	}
	pollUntil(t, 5*time.Second, 10*time.Millisecond, "the refused delivery was never logged", func() bool {
		out := logBuf.String()
		return strings.Contains(out, "listener notification dropped") &&
			strings.Contains(out, "event=OnSnapshotRejected") &&
			strings.Contains(out, "shard_id=7")
	})
	assertNever(t, 200*time.Millisecond, 10*time.Millisecond, "OnSnapshotRejected ran for a refused delivery", func() bool {
		return ran.Load() != 0
	})
}

func TestRejectedListenerDispatchNoCallbackDispatchesNothing(t *testing.T) {
	cases := map[string]*config.EventListener{
		"no listener":            nil,
		"nil OnSnapshotRejected": {},
	}
	for name, listener := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			tr := newRejectedListenerDispatchTransport(ctx, t, listener)
			var calls atomic.Int32
			tr.SetListenerDispatcher(func(fn func()) bool {
				calls.Add(1)
				fn()
				return true
			})

			rejectTransferRejectedListenerDispatch(ctx, t, tr, &SnapshotHeader{ShardID: 5, ReplicaID: 1, Index: 3, ChunkCount: 3})

			assertNever(t, 200*time.Millisecond, 10*time.Millisecond, "dispatcher called without an OnSnapshotRejected callback", func() bool {
				return calls.Load() != 0
			})
		})
	}
}
