package quicraft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestReadIndexDropped_ReturnsSystemBusyFiresEventLeavesNothingPending covers
// the OnReadIndexDropped row of the Raft events table in
// docs/multi-group-host/api.md: a ReadIndex the shard's ReadIndex queue cannot
// take is refused with ErrSystemBusy, fires OnReadIndexDropped with the shard's
// EventShardInfo, and rolls back both the pending read and the pending-op
// count, so a later Drain is not held up by it.
func TestReadIndexDropped_ReturnsSystemBusyFiresEventLeavesNothingPending(t *testing.T) {
	var (
		mu     sync.Mutex
		events []config.EventShardInfo
	)
	hostCfg := testHostConfig(t)
	hostCfg.EventListener = &config.EventListener{
		OnReadIndexDropped: func(info config.EventShardInfo) {
			mu.Lock()
			events = append(events, info)
			mu.Unlock()
		},
	}
	h, err := NewHost(hostCfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ns := testLoadNode(h, 1)
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("engine node is nil")
	}

	pendingReadKeys := func() map[uint64]struct{} {
		ns.pendingReadsMu.Lock()
		defer ns.pendingReadsMu.Unlock()
		keys := make(map[uint64]struct{}, len(ns.pendingReads))
		for k := range ns.pendingReads {
			keys[k] = struct{}{}
		}
		return keys
	}
	before := pendingReadKeys()
	opsBefore := h.pendingOps.Load()

	// Fill the ReadIndex queue until it refuses, then read at once. The step
	// worker drains the queue concurrently, so retry fill+read until the read
	// is refused. A read that is accepted instead is a real pending read: it
	// is completed (or times out on its short deadline) and released.
	ctx := context.Background()
	var (
		rs      *RequestState
		readErr error
	)
	deadline := time.Now().Add(5 * time.Second)
	for fillKey := uint64(1 << 62); ; {
		for i := 0; i < 100000; i++ {
			fillKey++
			if !engNode.DeliverReadIndex(fillKey) {
				break
			}
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		rs, readErr = h.ReadIndex(rctx, 1)
		if readErr != nil {
			cancel()
			break
		}
		_, _ = rs.Result()
		rs.Release()
		cancel()
		if time.Now().After(deadline) {
			t.Fatal("could not trigger a ReadIndex drop within 5s")
		}
	}

	if !errors.Is(readErr, ErrSystemBusy) {
		t.Fatalf("ReadIndex error = %v, want ErrSystemBusy", readErr)
	}
	if rs != nil {
		t.Errorf("ReadIndex returned a RequestState %p with ErrSystemBusy, want nil", rs)
	}

	mu.Lock()
	got := append([]config.EventShardInfo(nil), events...)
	mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("OnReadIndexDropped fired %d times, want 1: %+v", len(got), got)
	}
	if got[0].ShardID != 1 || got[0].ReplicaID != 1 {
		t.Errorf("OnReadIndexDropped info = %+v, want ShardID 1 ReplicaID 1", got[0])
	}

	// The refused read's registration is rolled back: no key is pending
	// that was not pending before the call.
	for k := range pendingReadKeys() {
		if _, ok := before[k]; !ok {
			t.Errorf("pendingReads holds key %d after the refused ReadIndex", k)
		}
	}
	if ops := h.pendingOps.Load(); ops != opsBefore {
		t.Errorf("pendingOps = %d after the refused ReadIndex, want %d", ops, opsBefore)
	}

	// No leaked pending op: Drain completes promptly.
	drained := make(chan error, 1)
	go func() { drained <- h.Drain() }()
	select {
	case err := <-drained:
		if err != nil {
			t.Errorf("Drain after the refused ReadIndex = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not complete within 5s after the refused ReadIndex")
	}
}
