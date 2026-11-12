// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// lifecycleShard is the shard the lifecycle tests start on their
// single-replica Host; lifecycleOtherShard is never started.
const (
	lifecycleShard      uint64 = 1
	lifecycleOtherShard uint64 = 2
)

// startLifecycleShard starts lifecycleShard as a single voter on h and waits
// until the Host's own leader view shows it leading a ready term, so
// ExecuteOnLeader runs in process. It does not use waitForLeader, which
// overwrites the cached view with term 0.
func startLifecycleShard(t *testing.T, h *Host) {
	t.Helper()
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(lifecycleShard, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ns, err := h.getNode(lifecycleShard); err == nil {
			if _, term, isLeader, ready := ns.leaderTermReady(); isLeader && ready && term > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for shard %d to lead a ready term", lifecycleShard)
}

// executeLifecycle runs ExecuteOnLeader on lifecycleShard with a bounded
// deadline.
func executeLifecycle(ctx context.Context, t *testing.T, h *Host, request []byte) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return h.ExecuteOnLeader(ctx, lifecycleShard, request)
}

// wantLifecycleReply fails t unless ExecuteOnLeader returns want.
func wantLifecycleReply(ctx context.Context, t *testing.T, h *Host, request, want []byte) {
	t.Helper()
	got, err := executeLifecycle(ctx, t, h, request)
	if err != nil {
		t.Fatalf("ExecuteOnLeader(%q): %v", request, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ExecuteOnLeader(%q) = %q, want %q", request, got, want)
	}
}

// countingHandler returns a handler that counts its calls and replies with
// prefix followed by the request.
func countingHandler(prefix string, calls *atomic.Int64) LeaderHandler {
	return func(_ context.Context, _ uint64, request []byte) ([]byte, error) {
		calls.Add(1)
		return append([]byte(prefix), request...), nil
	}
}

func TestHost_LeaderHandlerLifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("RegisteredBeforeStartShardServes", func(t *testing.T) {
		h := newTestHost(t)
		// A reply carrying bytes a careless copy or string conversion would
		// mangle: it must come back unchanged.
		reply := []byte{0x00, 0xff, 'o', 'k', 0x00, 0x80}
		var calls atomic.Int64
		var gotTerm atomic.Uint64
		if err := h.RegisterLeaderHandler(lifecycleShard, func(_ context.Context, term uint64, request []byte) ([]byte, error) {
			calls.Add(1)
			gotTerm.Store(term)
			if !bytes.Equal(request, []byte("req")) {
				return nil, errors.New("unexpected request")
			}
			return reply, nil
		}); err != nil {
			t.Fatalf("RegisterLeaderHandler before StartShard: %v", err)
		}
		startLifecycleShard(t, h)

		wantLifecycleReply(ctx, t, h, []byte("req"), reply)
		if n := calls.Load(); n != 1 {
			t.Fatalf("handler ran %d times, want 1", n)
		}
		if gotTerm.Load() == 0 {
			t.Fatal("handler saw term 0, want the leader's term")
		}
	})

	t.Run("SurvivesStopShard", func(t *testing.T) {
		h := newTestHost(t)
		var calls atomic.Int64
		if err := h.RegisterLeaderHandler(lifecycleShard, countingHandler("a:", &calls)); err != nil {
			t.Fatalf("RegisterLeaderHandler: %v", err)
		}
		startLifecycleShard(t, h)
		wantLifecycleReply(ctx, t, h, []byte("1"), []byte("a:1"))

		if err := h.StopShard(lifecycleShard); err != nil {
			t.Fatalf("StopShard: %v", err)
		}
		startLifecycleShard(t, h)
		wantLifecycleReply(ctx, t, h, []byte("2"), []byte("a:2"))
		if n := calls.Load(); n != 2 {
			t.Fatalf("handler ran %d times, want 2", n)
		}
	})

	t.Run("SecondRegistrationReplaces", func(t *testing.T) {
		h := newTestHost(t)
		var oldCalls, newCalls atomic.Int64
		if err := h.RegisterLeaderHandler(lifecycleShard, countingHandler("old:", &oldCalls)); err != nil {
			t.Fatalf("RegisterLeaderHandler(old): %v", err)
		}
		startLifecycleShard(t, h)
		wantLifecycleReply(ctx, t, h, []byte("x"), []byte("old:x"))

		if err := h.RegisterLeaderHandler(lifecycleShard, countingHandler("new:", &newCalls)); err != nil {
			t.Fatalf("RegisterLeaderHandler(new): %v", err)
		}
		for i := 0; i < 3; i++ {
			wantLifecycleReply(ctx, t, h, []byte("y"), []byte("new:y"))
		}
		if n := oldCalls.Load(); n != 1 {
			t.Fatalf("replaced handler ran %d times, want 1 (before the replacement only)", n)
		}
		if n := newCalls.Load(); n != 3 {
			t.Fatalf("new handler ran %d times, want 3", n)
		}
	})

	t.Run("UnregisterLetsTheRunningRequestFinish", func(t *testing.T) {
		h := newTestHost(t)
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int64
		if err := h.RegisterLeaderHandler(lifecycleShard, func(ctx context.Context, _ uint64, request []byte) ([]byte, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return append([]byte("done:"), request...), nil
		}); err != nil {
			t.Fatalf("RegisterLeaderHandler: %v", err)
		}
		startLifecycleShard(t, h)

		type result struct {
			reply []byte
			err   error
		}
		res := make(chan result, 1)
		go func() {
			reply, err := executeLifecycle(ctx, t, h, []byte("r"))
			res <- result{reply, err}
		}()

		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("handler never started")
		}
		h.UnregisterLeaderHandler(lifecycleShard)
		select {
		case r := <-res:
			t.Fatalf("request returned before the handler was released: %q, %v", r.reply, r.err)
		default:
		}
		close(release)

		select {
		case r := <-res:
			if r.err != nil {
				t.Fatalf("running request: %v, want its reply", r.err)
			}
			if !bytes.Equal(r.reply, []byte("done:r")) {
				t.Fatalf("running request reply = %q, want %q", r.reply, "done:r")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("running request never returned after the handler was released")
		}

		if _, err := executeLifecycle(ctx, t, h, []byte("next")); !errors.Is(err, ErrLeaderHandlerNotRegistered) {
			t.Fatalf("after UnregisterLeaderHandler: err = %v, want ErrLeaderHandlerNotRegistered", err)
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("unregistered handler ran %d times, want 1", n)
		}
	})

	t.Run("OtherShardHandlerNeverCalled", func(t *testing.T) {
		h := newTestHost(t)
		var otherCalls, ownCalls atomic.Int64
		if err := h.RegisterLeaderHandler(lifecycleOtherShard, countingHandler("other:", &otherCalls)); err != nil {
			t.Fatalf("RegisterLeaderHandler(other shard): %v", err)
		}
		startLifecycleShard(t, h)

		if _, err := executeLifecycle(ctx, t, h, []byte("x")); !errors.Is(err, ErrLeaderHandlerNotRegistered) {
			t.Fatalf("only another shard's handler registered: err = %v, want ErrLeaderHandlerNotRegistered", err)
		}

		if err := h.RegisterLeaderHandler(lifecycleShard, countingHandler("own:", &ownCalls)); err != nil {
			t.Fatalf("RegisterLeaderHandler(own shard): %v", err)
		}
		wantLifecycleReply(ctx, t, h, []byte("y"), []byte("own:y"))
		if n := otherCalls.Load(); n != 0 {
			t.Fatalf("handler for shard %d ran %d times, want 0", lifecycleOtherShard, n)
		}
		if n := ownCalls.Load(); n != 1 {
			t.Fatalf("own handler ran %d times, want 1", n)
		}
	})
}
