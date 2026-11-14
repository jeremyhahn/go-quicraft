// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	sm "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// maybeAppliedHoldCmd is the command whose apply countingHoldSM parks.
var maybeAppliedHoldCmd = []byte("maybe-applied-hold")

// countingHoldSM counts the applies of every command and, while hold is
// non-nil, parks the first apply of maybeAppliedHoldCmd until hold is
// closed; held is closed the moment it parks.
type countingHoldSM struct {
	mu      sync.Mutex
	applies map[string]int
	hold    chan struct{}
	held    chan struct{}
	parked  bool
}

func newCountingHoldSM(hold bool) *countingHoldSM {
	s := &countingHoldSM{applies: make(map[string]int)}
	if hold {
		s.hold = make(chan struct{})
		s.held = make(chan struct{})
	}
	return s
}

func (s *countingHoldSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		cmd := string(entries[i].Cmd)
		if cmd == string(maybeAppliedHoldCmd) && s.hold != nil {
			s.mu.Lock()
			park := !s.parked
			s.parked = true
			s.mu.Unlock()
			if park {
				close(s.held)
				<-s.hold
			}
		}
		s.mu.Lock()
		s.applies[cmd]++
		s.mu.Unlock()
		if i < len(results) {
			results[i] = sm.Result{Value: entries[i].Index}
		}
	}
	return nil
}

func (s *countingHoldSM) appliesOf(cmd []byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applies[string(cmd)]
}

func (s *countingHoldSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return nil, nil
}

func (s *countingHoldSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (s *countingHoldSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (s *countingHoldSM) Close(_ context.Context) error { return nil }

// maybeAppliedPayloadLimit is the shard's proposal payload limit, so an
// oversized payload is cheap to build.
const maybeAppliedPayloadLimit = 4096

// startMaybeAppliedShard starts shard 1 on h with machine and waits for it to
// lead.
func startMaybeAppliedShard(t *testing.T, h *Host, machine *countingHoldSM) {
	t.Helper()
	cfg := testShardConfig(1, 1)
	cfg.MaxProposalPayloadSize = maybeAppliedPayloadLimit
	create := func(shardID, replicaID uint64) interface{} { return machine }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create, cfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
}

// maybeAppliedPropose runs a SyncPropose of cmd with a bounded context.
func maybeAppliedPropose(ctx context.Context, t *testing.T, h *Host, cmd []byte) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := h.SyncPropose(ctx, 1, cmd)
	return err
}

// assertWrappedOnce fails t unless err carries exactly one MaybeAppliedError
// layer: the one errors.As finds unwraps to an error that is not one.
func assertWrappedOnce(t *testing.T, err error) {
	t.Helper()
	var m *MaybeAppliedError
	if !errors.As(err, &m) {
		t.Fatalf("error = %v (%T) carries no *MaybeAppliedError", err, err)
	}
	if m.Err == nil {
		t.Fatalf("error = %v wraps a nil error", err)
	}
	if errors.As(m.Err, new(*MaybeAppliedError)) {
		t.Fatalf("error = %v is wrapped as maybe-applied twice", err)
	}
}

// TestMaybeAppliedHostRefusalsBeforeRaft checks that every refusal that
// happens before the proposal is handed to Raft matches its documented error
// and reports MaybeApplied false.
func TestMaybeAppliedHostRefusalsBeforeRaft(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		refuse func(t *testing.T, h *Host) error
		want   error
	}{
		"paused shard": {
			refuse: func(t *testing.T, h *Host) error {
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				if err := h.PauseShard(pctx, 1); err != nil {
					t.Fatalf("PauseShard: %v", err)
				}
				return maybeAppliedPropose(ctx, t, h, []byte("paused"))
			},
			want: ErrShardPaused,
		},
		"oversized payload": {
			refuse: func(t *testing.T, h *Host) error {
				return maybeAppliedPropose(ctx, t, h, make([]byte, maybeAppliedPayloadLimit+1))
			},
			want: ErrEntryTooLarge,
		},
		"after Drain": {
			refuse: func(t *testing.T, h *Host) error {
				if err := h.Drain(); err != nil {
					t.Fatalf("Drain: %v", err)
				}
				return maybeAppliedPropose(ctx, t, h, []byte("drained"))
			},
			want: ErrDraining,
		},
		"after Close": {
			refuse: func(t *testing.T, h *Host) error {
				if err := h.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				return maybeAppliedPropose(ctx, t, h, []byte("closed"))
			},
			want: ErrClosed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestHost(t)
			machine := newCountingHoldSM(false)
			startMaybeAppliedShard(t, h, machine)
			err := tc.refuse(t, h)
			if !errors.Is(err, tc.want) {
				t.Fatalf("proposal = %v (%T), want %v", err, err, tc.want)
			}
			if MaybeApplied(err) {
				t.Fatalf("proposal refused before raft = %v reports MaybeApplied true", err)
			}
		})
	}
}

// TestMaybeAppliedHostDeadlineAppliesOnce checks that a SyncPropose whose
// deadline passes while its entry is in apply reports ErrTimeout, Temporary
// and MaybeApplied, wrapped once, and that the entry still applies exactly
// once after the apply is released.
func TestMaybeAppliedHostDeadlineAppliesOnce(t *testing.T) {
	h := newTestHost(t)
	machine := newCountingHoldSM(true)
	startMaybeAppliedShard(t, h, machine)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := h.SyncPropose(ctx, 1, maybeAppliedHoldCmd)
	select {
	case <-machine.held:
	default:
		t.Fatalf("SyncPropose returned %v before its entry reached apply", err)
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("SyncPropose past its deadline = %v (%T), want ErrTimeout", err, err)
	}
	if !MaybeApplied(err) {
		t.Fatalf("SyncPropose past its deadline = %v reports MaybeApplied false", err)
	}
	if !Temporary(err) {
		t.Fatalf("SyncPropose past its deadline = %v reports Temporary false", err)
	}
	assertWrappedOnce(t, err)

	close(machine.hold)
	if err := maybeAppliedPropose(context.Background(), t, h, []byte("after-release")); err != nil {
		t.Fatalf("SyncPropose after release: %v", err)
	}
	if n := machine.appliesOf(maybeAppliedHoldCmd); n != 1 {
		t.Fatalf("the timed-out command was applied %d times, want exactly once", n)
	}
}

// TestMaybeAppliedHostStopShardWhilePending checks that a proposal pending
// when StopShard runs fails with MaybeApplied true, wrapped once.
func TestMaybeAppliedHostStopShardWhilePending(t *testing.T) {
	h := newTestHost(t)
	machine := newCountingHoldSM(true)
	startMaybeAppliedShard(t, h, machine)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The held proposal parks the apply goroutine; the pending one is handed
	// to raft and committed behind it, but cannot apply.
	heldRS, err := h.Propose(ctx, 1, maybeAppliedHoldCmd)
	if err != nil {
		t.Fatalf("Propose held: %v", err)
	}
	defer heldRS.Release()
	select {
	case <-machine.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the held proposal never reached apply")
	}
	pendingCmd := []byte("maybe-applied-pending")
	rs, err := h.Propose(ctx, 1, pendingCmd)
	if err != nil {
		t.Fatalf("Propose pending: %v", err)
	}
	defer rs.Release()
	// A single voter commits it at once; give the step worker time to take
	// it from the queue so it is past the hand-off.
	time.Sleep(200 * time.Millisecond)

	stopped := make(chan error, 1)
	go func() { stopped <- h.StopShard(1) }()
	// StopShard waits for the in-flight apply; release it once the stop has
	// begun so the teardown can finish.
	time.Sleep(200 * time.Millisecond)
	close(machine.hold)

	var perr error
	select {
	case res := <-rs.ResultC():
		perr = res.Err
	case <-time.After(10 * time.Second):
		t.Fatal("the proposal pending across StopShard was never completed")
	}
	if err := <-stopped; err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if perr == nil {
		t.Fatalf("the proposal pending across StopShard succeeded (applied %d times); "+
			"want a maybe-applied failure", machine.appliesOf(pendingCmd))
	}
	if !MaybeApplied(perr) {
		t.Fatalf("proposal pending across StopShard = %v (%T) reports MaybeApplied false", perr, perr)
	}
	assertWrappedOnce(t, perr)
}
