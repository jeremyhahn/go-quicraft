// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// busyLimitSM is a state machine whose Update blocks until release is closed,
// so committed entries stay in the in-memory log and the limiters trip. It
// records every applied command.
type busyLimitSM struct {
	release <-chan struct{}
	mu      sync.Mutex
	applied map[string]struct{}
}

func (m *busyLimitSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	release := m.release
	m.mu.Unlock()
	select {
	case <-release:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			m.applied[string(entries[i].Cmd)] = struct{}{}
		}
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

func (m *busyLimitSM) has(cmd string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.applied[cmd]
	return ok
}

func (m *busyLimitSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

func (m *busyLimitSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (m *busyLimitSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (m *busyLimitSM) Close(_ context.Context) error { return nil }

const (
	busyLimitBytes   = 16 * 1024
	busyLimitPayload = 1024
	// busyLimitCallTimeout bounds every Propose call; a call that returns
	// later than this plus busyLimitSlack blocked past its deadline.
	busyLimitCallTimeout = 2 * time.Second
	busyLimitSlack       = 500 * time.Millisecond
)

// busyLimitCmd builds a unique command of busyLimitPayload bytes.
func busyLimitCmd(tag string, i int) []byte {
	b := make([]byte, busyLimitPayload)
	for j := range b {
		b[j] = 'x'
	}
	copy(b, fmt.Sprintf("%s-%06d|", tag, i))
	return b
}

// runBusyLimit starts a single-replica shard on a host built from hcfg with
// the shard config scfg, fills the in-memory log against a blocked state
// machine until a proposal is refused, checks the refusal with check, then
// releases the state machine and asserts recovery and that every
// acknowledged proposal is applied.
func runBusyLimit(t *testing.T, hcfg config.HostConfig, scfg config.Config, check func(error) bool) {
	t.Helper()
	h, err := NewHost(hcfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// Unblock before Close so a parked Update cannot hold shutdown.
	t.Cleanup(func() {
		unblock()
		h.Close()
	})

	// The bootstrap and no-op entries must apply before the leader is ready
	// (waitForLeader waits for lastApplied >= 2), so the state machine starts
	// on an open gate and is switched to the blocking one after election.
	setup := make(chan struct{})
	close(setup)
	fsm := &busyLimitSM{release: setup, applied: make(map[string]struct{})}
	create := func(_, _ uint64) interface{} { return fsm }

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, create, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	fsm.mu.Lock()
	fsm.release = release
	fsm.mu.Unlock()

	type acked struct {
		cmd string
		rs  *RequestState
	}
	var accepted []acked
	propose := func(tag string, i int) (*RequestState, error) {
		ctx, cancel := context.WithTimeout(context.Background(), busyLimitCallTimeout)
		defer cancel()
		start := time.Now()
		rs, perr := h.Propose(ctx, 1, busyLimitCmd(tag, i))
		if d := time.Since(start); d > busyLimitCallTimeout+busyLimitSlack {
			t.Fatalf("Propose %s-%d blocked %v, past its %v deadline", tag, i, d, busyLimitCallTimeout)
		}
		return rs, perr
	}

	var refusal error
	const maxProposals = 10 * busyLimitBytes / busyLimitPayload
	for i := 0; i < maxProposals; i++ {
		rs, perr := propose("fill", i)
		if perr != nil {
			refusal = perr
			break
		}
		accepted = append(accepted, acked{cmd: string(busyLimitCmd("fill", i)), rs: rs})
		// Let the step worker append and the limiter re-evaluate.
		time.Sleep(2 * time.Millisecond)
	}
	if refusal == nil {
		t.Fatalf("%d proposals of %d bytes past a %d-byte limit were all accepted",
			maxProposals, busyLimitPayload, busyLimitBytes)
	}
	if !check(refusal) {
		t.Fatalf("refusal = %v (%T), want an error matching ErrBusy/ErrSystemBusy", refusal, refusal)
	}
	if len(accepted) == 0 {
		t.Fatalf("the first proposal was refused: %v", refusal)
	}

	unblock()

	// Every acknowledged proposal completes and is applied.
	for _, a := range accepted {
		res, rerr := waitBusyLimitResult(a.rs)
		a.rs.Release()
		if rerr != nil || res.Err != nil {
			t.Fatalf("acknowledged proposal failed: err=%v result.Err=%v", rerr, res.Err)
		}
		if !fsm.has(a.cmd) {
			t.Fatalf("acknowledged proposal %.12q was not applied", a.cmd)
		}
	}

	// Once the log drains, proposals succeed again.
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), busyLimitCallTimeout)
		cmd := busyLimitCmd("recover", i)
		_, perr := h.SyncPropose(ctx, 1, cmd)
		cancel()
		if perr == nil {
			if !fsm.has(string(cmd)) {
				t.Fatalf("recovered proposal %d was not applied", i)
			}
			return
		}
		if !check(perr) {
			t.Fatalf("proposal after drain failed with %v, want success or busy", perr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("proposals still refused 10s after the state machine was released: %v", perr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitBusyLimitResult waits for rs with a fresh bound so a released state
// machine has time to apply the backlog.
func waitBusyLimitResult(rs *RequestState) (RequestResult, error) {
	type out struct {
		res RequestResult
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := rs.Result()
		ch <- out{res, err}
	}()
	select {
	case o := <-ch:
		return o.res, o.err
	case <-time.After(10 * time.Second):
		return RequestResult{}, errors.New("result not delivered within 10s")
	}
}

// TestBusyLimit_PerShard: past MaxInMemLogSize a proposal is refused with an
// error matching ErrBusy, and the shard recovers once the log drains.
func TestBusyLimit_PerShard(t *testing.T) {
	scfg := testShardConfig(1, 1)
	scfg.MaxInMemLogSize = busyLimitBytes
	runBusyLimit(t, testHostConfig(t), scfg, func(err error) bool {
		return errors.Is(err, ErrBusy)
	})
}

// TestBusyLimit_Global: past MaxTotalInMemLogSize a proposal is refused with
// ErrSystemBusy (which also matches ErrBusy), and the host recovers once the
// log drains.
func TestBusyLimit_Global(t *testing.T) {
	hcfg := testHostConfig(t)
	hcfg.MaxTotalInMemLogSize = busyLimitBytes
	runBusyLimit(t, hcfg, testShardConfig(1, 1), func(err error) bool {
		return errors.Is(err, ErrSystemBusy) && errors.Is(err, ErrBusy)
	})
}

// TestBusyLimit_PerShardLastBatchReleased proposes one entry larger than
// MaxInMemLogSize on an idle shard. Once it is applied nothing else is
// pending, so the in-memory log must still release it: the per-shard size
// drops to zero and the next proposal is accepted.
func TestBusyLimit_PerShardLastBatchReleased(t *testing.T) {
	scfg := testShardConfig(1, 1)
	scfg.MaxInMemLogSize = busyLimitBytes
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	open := make(chan struct{})
	close(open)
	fsm := &busyLimitSM{release: open, applied: make(map[string]struct{})}
	create := func(_, _ uint64) interface{} { return fsm }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	big := make([]byte, 2*busyLimitBytes)
	copy(big, "big-")
	for i := 4; i < len(big); i++ {
		big[i] = 'y'
	}
	ctx, cancel := context.WithTimeout(context.Background(), busyLimitCallTimeout)
	_, err = h.SyncPropose(ctx, 1, big)
	cancel()
	if err != nil {
		t.Fatalf("SyncPropose of the large entry failed: %v", err)
	}
	if !fsm.has(string(big)) {
		t.Fatal("the large entry was not applied")
	}

	engNode := h.engine.GetNode(1)
	if engNode == nil || engNode.Peer() == nil || engNode.Peer().RateLimiter() == nil {
		t.Fatal("shard 1 has no per-shard rate limiter")
	}
	rl := engNode.Peer().RateLimiter()
	deadline := time.Now().Add(5 * time.Second)
	for rl.Get() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("in-memory log size %d bytes 5s after the last entry applied, want 0", rl.Get())
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel = context.WithTimeout(context.Background(), busyLimitCallTimeout)
	defer cancel()
	for {
		_, err = h.SyncPropose(ctx, 1, busyLimitCmd("after", 0))
		if err == nil {
			return
		}
		if !errors.Is(err, ErrBusy) || ctx.Err() != nil {
			t.Fatalf("proposal after the log drained failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
