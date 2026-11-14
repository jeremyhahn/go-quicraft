// Copyright 2025 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

const (
	overheadShardID uint64 = 1
	// overheadShardK is the shard's Config.CompactionOverhead.
	overheadShardK uint64 = 10
)

// overheadShardConfig is a single-replica shard config with
// CompactionOverhead K and automatic snapshots off, so every snapshot in
// the test is one it requested.
func overheadShardConfig() config.Config {
	c := config.Config{
		ShardID:            overheadShardID,
		ReplicaID:          1,
		CompactionOverhead: overheadShardK,
		SnapshotEntries:    config.SnapshotsDisabled,
	}
	c.SetDefaults()
	return c
}

// startOverheadHost opens a Host with an on-disk LogDB on cfg's
// directories and starts the KV shard on it, waiting for leadership.
func startOverheadHost(ctx context.Context, t *testing.T, cfg config.HostConfig) *Host {
	t.Helper()
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if startErr := h.StartShard(map[uint64]string{1: cfg.RaftAddress}, false,
		kv.NewMemoryCreateFunc(), overheadShardConfig()); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, overheadShardID)
	if ctx.Err() != nil {
		t.Fatalf("context done before the shard started: %v", ctx.Err())
	}
	return h
}

// proposeOverheadPuts applies n KV puts, keyed from base.
func proposeOverheadPuts(ctx context.Context, t *testing.T, h *Host, base, n int) {
	t.Helper()
	for i := range n {
		cmd, err := kv.EncodePut([]byte(fmt.Sprintf("key-%05d", base+i)),
			[]byte(fmt.Sprintf("value-%05d", base+i)))
		if err != nil {
			t.Fatalf("EncodePut: %v", err)
		}
		if _, err := h.SyncPropose(ctx, overheadShardID, cmd); err != nil {
			t.Fatalf("SyncPropose[%d]: %v", base+i, err)
		}
	}
}

// waitFirstIndex polls LogRange until FirstIndex equals want, under a
// bounded deadline (log compaction after a snapshot save is asynchronous).
func waitFirstIndex(ctx context.Context, t *testing.T, h *Host, want uint64, what string) LogRange {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		lr, err := h.LogRange(overheadShardID)
		if err != nil {
			t.Fatalf("%s: LogRange: %v", what, err)
		}
		if lr.FirstIndex == want {
			return lr
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s: LogRange = [%d, %d], want FirstIndex %d", what,
				lr.FirstIndex, lr.LastIndex, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitHashAtLeast polls StateMachineHash until the applied index reaches
// minApplied and returns the hash.
func waitHashAtLeast(ctx context.Context, t *testing.T, h *Host, minApplied uint64, what string) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		hash, applied, ok, err := h.StateMachineHash(overheadShardID)
		if err == nil && ok && applied >= minApplied {
			return hash
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s: StateMachineHash applied %d ok %v err %v, want applied >= %d",
				what, applied, ok, err, minApplied)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// restartOverheadHost closes h, reopens a Host on the same directories and
// checks it recovers the same state machine hash and LogRange.FirstIndex.
func restartOverheadHost(ctx context.Context, t *testing.T, h *Host, cfg config.HostConfig, lr LogRange, what string) *Host {
	t.Helper()
	hash := waitHashAtLeast(ctx, t, h, lr.LastIndex, what+" before restart")
	if err := h.Close(); err != nil {
		t.Fatalf("%s: Close: %v", what, err)
	}
	r := startOverheadHost(ctx, t, cfg)
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, applied, ok, err := r.StateMachineHash(overheadShardID)
		if err == nil && ok && applied >= lr.LastIndex && got == hash {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s: after restart hash %#x applied %d ok %v err %v, want hash %#x at applied >= %d",
				what, got, applied, ok, err, hash, lr.LastIndex)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, err := r.LogRange(overheadShardID)
	if err != nil {
		t.Fatalf("%s: LogRange after restart: %v", what, err)
	}
	// The restarted leader appends one entry in its new term, so LastIndex
	// may grow. On restart the log reader is rebuilt from the snapshot
	// marker, so the retained overhead entries at or below the snapshot
	// index are not reloaded and FirstIndex may move up to the snapshot
	// index plus one; it must never move below the compacted first index.
	if got.FirstIndex < lr.FirstIndex || got.LastIndex < lr.LastIndex {
		t.Fatalf("%s: LogRange after restart = [%d, %d], want FirstIndex >= %d and LastIndex >= %d",
			what, got.FirstIndex, got.LastIndex, lr.FirstIndex, lr.LastIndex)
	}
	return r
}

func TestSnapshotOptionOverhead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg := testHostConfig(t)
	h := startOverheadHost(ctx, t, cfg)
	base := 0

	snapshot := func(opt SnapshotOption, what string) uint64 {
		t.Helper()
		idx, err := h.SyncRequestSnapshot(ctx, overheadShardID, opt)
		if err != nil {
			t.Fatalf("%s: SyncRequestSnapshot: %v", what, err)
		}
		return idx
	}

	// (a) The override with C != K keeps exactly C entries up to S.
	const c uint64 = 3
	proposeOverheadPuts(ctx, t, h, base, 60)
	base += 60
	s := snapshot(SnapshotOption{CompactionOverhead: c, OverrideCompactionOverhead: true}, "(a)")
	if s <= c+overheadShardK {
		t.Fatalf("(a): snapshot index %d is not well above the overheads", s)
	}
	lr := waitFirstIndex(ctx, t, h, s-c+1, "(a) override C=3")
	h = restartOverheadHost(ctx, t, h, cfg, lr, "(a)")

	// (b) Without the override the shard's CompactionOverhead K applies.
	proposeOverheadPuts(ctx, t, h, base, 40)
	base += 40
	s = snapshot(SnapshotOption{}, "(b)")
	lr = waitFirstIndex(ctx, t, h, s-overheadShardK+1, "(b) no override")
	h = restartOverheadHost(ctx, t, h, cfg, lr, "(b)")

	// (c) The override with CompactionOverhead 0 keeps no entries up to S.
	proposeOverheadPuts(ctx, t, h, base, 20)
	base += 20
	s = snapshot(SnapshotOption{CompactionOverhead: 0, OverrideCompactionOverhead: true}, "(c)")
	lr = waitFirstIndex(ctx, t, h, s+1, "(c) override C=0")
	h = restartOverheadHost(ctx, t, h, cfg, lr, "(c)")

	// (d) CompactionOverhead without the override is ignored.
	proposeOverheadPuts(ctx, t, h, base, 40)
	s = snapshot(SnapshotOption{CompactionOverhead: 2}, "(d)")
	lr = waitFirstIndex(ctx, t, h, s-overheadShardK+1, "(d) CompactionOverhead without override")
	_ = restartOverheadHost(ctx, t, h, cfg, lr, "(d)")
}
