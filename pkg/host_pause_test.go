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
	"testing"
	"time"
)

// pauseOnlyCalls are the entry points a pause refuses beyond the client work
// a sealed barrier refuses (sealedAdmitCalls).
func pauseOnlyCalls(h *Host) []sealedAdmitCall {
	return []sealedAdmitCall{
		{"SyncReadBuf", func(ctx context.Context) error {
			_, release, err := h.SyncReadBuf(ctx, 1, []byte("k"))
			if release != nil {
				release()
			}
			return err
		}},
		{"RequestLeaderTransfer", func(ctx context.Context) error {
			return h.RequestLeaderTransfer(ctx, 1, 2)
		}},
		{"RequestCompaction", func(context.Context) error {
			return h.RequestCompaction(1, 1)
		}},
		{"ImportSnapshot", func(ctx context.Context) error {
			return h.ImportSnapshot(ctx, 1, bytes.NewReader(nil))
		}},
		{"RequestRemoveNode", func(ctx context.Context) error {
			_, err := h.RequestRemoveNode(ctx, 1, 2, 0)
			return err
		}},
		{"RequestAddObserver", func(ctx context.Context) error {
			_, err := h.RequestAddObserver(ctx, 1, 3, "127.0.0.1:5002", 0)
			return err
		}},
	}
}

func newPauseTestHost(t *testing.T, shards ...uint64) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	for _, id := range shards {
		cfg := testShardConfig(id, 1)
		cfg.LeaseRead = true
		if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, cfg); err != nil {
			t.Fatalf("StartShard(%d): %v", id, err)
		}
		waitForLeader(t, h, id)
	}
	return h
}

func shardPaused(t *testing.T, h *Host, shardID uint64) bool {
	t.Helper()
	info, err := h.GetShardInfo(shardID)
	if err != nil {
		t.Fatalf("GetShardInfo(%d): %v", shardID, err)
	}
	return info.Paused
}

// Every public entry point refuses a paused shard with ErrShardPaused, which
// is Temporary; the refusals touch nothing, and after ResumeShard the shard
// serves again.
func TestPauseShard_RefusesEveryEntryPoint(t *testing.T) {
	h := newPauseTestHost(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, err := h.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession: %v", err)
	}
	if shardPaused(t, h, 1) {
		t.Fatal("GetShardInfo reports a fresh shard paused")
	}

	if err := h.PauseShard(ctx, 1); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	if !shardPaused(t, h, 1) {
		t.Fatal("GetShardInfo does not report the shard paused")
	}
	for _, c := range append(sealedAdmitCalls(h, session), pauseOnlyCalls(h)...) {
		err := c.call(ctx)
		if !errors.Is(err, ErrShardPaused) {
			t.Errorf("%s while paused: err = %v, want ErrShardPaused", c.name, err)
			continue
		}
		if !Temporary(err) {
			t.Errorf("%s while paused: Temporary(%v) = false", c.name, err)
		}
	}
	if engNode := h.engine.GetNode(1); engNode == nil || engNode.IsFailed() {
		t.Fatal("shard failed while paused")
	}

	if err := h.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	if shardPaused(t, h, 1) {
		t.Fatal("GetShardInfo reports the shard paused after ResumeShard")
	}
	waitForLeader(t, h, 1)
	if _, err := h.SyncPropose(ctx, 1, []byte("after-resume")); err != nil {
		t.Fatalf("SyncPropose after resume: %v", err)
	}
	if _, err := h.SyncRead(ctx, 1, "q"); err != nil {
		t.Fatalf("SyncRead after resume: %v", err)
	}
}

// Both calls are idempotent, and an unknown shard is ErrShardNotFound.
func TestPauseShard_IdempotentAndUnknownShard(t *testing.T) {
	h := newPauseTestHost(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := h.PauseShard(ctx, 99); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("PauseShard(unknown) = %v, want ErrShardNotFound", err)
	}
	if err := h.ResumeShard(ctx, 99); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("ResumeShard(unknown) = %v, want ErrShardNotFound", err)
	}
	if err := h.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard on a running shard: %v", err)
	}
	for i := range 3 {
		if err := h.PauseShard(ctx, 1); err != nil {
			t.Fatalf("PauseShard #%d: %v", i, err)
		}
	}
	for i := range 3 {
		if err := h.ResumeShard(ctx, 1); err != nil {
			t.Fatalf("ResumeShard #%d: %v", i, err)
		}
	}
	waitForLeader(t, h, 1)
	if _, err := h.SyncPropose(ctx, 1, []byte("cmd")); err != nil {
		t.Fatalf("SyncPropose after pause/resume cycles: %v", err)
	}
}

// A PauseShard whose ctx ends returns ctx's error and leaves the shard
// paused — even for a ctx already done on entry.
func TestPauseShard_CtxExpiryLeavesShardPaused(t *testing.T) {
	h := newPauseTestHost(t, 1)
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.PauseShard(done, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("PauseShard(done ctx) = %v, want context.Canceled", err)
	}
	if !shardPaused(t, h, 1) {
		t.Fatal("a PauseShard whose ctx ended must leave the shard paused")
	}
	ctx, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if _, err := h.SyncPropose(ctx, 1, []byte("cmd")); !errors.Is(err, ErrShardPaused) {
		t.Fatalf("SyncPropose = %v, want ErrShardPaused", err)
	}
	if err := h.PauseShard(ctx, 1); err != nil {
		t.Fatalf("PauseShard retry: %v", err)
	}
	if err := h.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	waitForLeader(t, h, 1)
	if _, err := h.SyncPropose(ctx, 1, []byte("cmd")); err != nil {
		t.Fatalf("SyncPropose after resume: %v", err)
	}
}

// A pause affects only its own shard.
func TestPauseShard_OtherShardUnaffected(t *testing.T) {
	h := newPauseTestHost(t, 1, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.PauseShard(ctx, 1); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	if _, err := h.SyncPropose(ctx, 1, []byte("cmd")); !errors.Is(err, ErrShardPaused) {
		t.Fatalf("SyncPropose on the paused shard = %v, want ErrShardPaused", err)
	}
	for range 5 {
		if _, err := h.SyncPropose(ctx, 2, []byte("cmd")); err != nil {
			t.Fatalf("SyncPropose on the other shard: %v", err)
		}
	}
	if _, err := h.SyncRead(ctx, 2, "q"); err != nil {
		t.Fatalf("SyncRead on the other shard: %v", err)
	}
	if shardPaused(t, h, 2) {
		t.Fatal("the other shard reports paused")
	}
}

// startSlowPause starts PauseShard on a leader that cannot hand leadership
// over (it has no peer), so the pause holds its transition for the whole
// transfer window, and returns once the host flag is set.
func startSlowPause(t *testing.T, h *Host, shardID uint64) (*nodeState, <-chan error) {
	t.Helper()
	ns, err := h.getNode(shardID)
	if err != nil {
		t.Fatalf("getNode(%d): %v", shardID, err)
	}
	ns.cfg.ElectionRTT = 2000 // a 2s transfer window at RTTMillisecond 1
	done := make(chan error, 1)
	go func() { done <- h.PauseShard(context.Background(), shardID) }()
	deadline := time.Now().Add(10 * time.Second)
	for !ns.paused.Load() {
		if time.Now().After(deadline) {
			t.Fatal("PauseShard never set the host flag")
		}
		time.Sleep(time.Millisecond)
	}
	return ns, done
}

// A ResumeShard that runs while a PauseShard is between its host flag and
// its engine pause (in the leadership transfer) waits for the pause's
// transition, then undoes it: admission and the engine end in agreement.
func TestPauseShard_ResumeDuringTransferIsSerialized(t *testing.T) {
	h := newPauseTestHost(t, 1)
	ns, paused := startSlowPause(t, h, 1)
	engNode := h.engine.GetNode(1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	if err := <-paused; err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	if ns.paused.Load() != engNode.IsPaused() {
		t.Fatalf("host flag paused=%v, engine node paused=%v: they must agree",
			ns.paused.Load(), engNode.IsPaused())
	}
	if shardPaused(t, h, 1) {
		t.Fatal("the resume that ran last left the shard paused")
	}
	waitForLeader(t, h, 1)
	if _, err := h.SyncPropose(ctx, 1, []byte("cmd")); err != nil {
		t.Fatalf("SyncPropose after the serialized resume: %v", err)
	}
}

// A ResumeShard waiting behind a pause's transition returns its ctx's error
// when ctx ends first, changing nothing; a resume on another shard does not
// wait at all.
func TestPauseShard_ResumeBehindPauseHonoursCtx(t *testing.T) {
	h := newPauseTestHost(t, 1, 2)
	ns, paused := startSlowPause(t, h, 1)
	engNode := h.engine.GetNode(1)

	other, cancelOther := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelOther()
	if err := h.ResumeShard(other, 2); err != nil {
		t.Fatalf("ResumeShard on another shard waited on shard 1's pause: %v", err)
	}

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.ResumeShard(short, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ResumeShard behind the pause = %v, want context.DeadlineExceeded", err)
	}
	if err := <-paused; err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	if !ns.paused.Load() || !engNode.IsPaused() {
		t.Fatalf("host flag paused=%v, engine node paused=%v: want both paused",
			ns.paused.Load(), engNode.IsPaused())
	}
	ctx, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if err := h.ResumeShard(ctx, 1); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	if ns.paused.Load() || engNode.IsPaused() {
		t.Fatal("ResumeShard left a flag set")
	}
}
