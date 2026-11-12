// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// contextCancelBound is how long a Host method may take to return once its
// context is cancelled.
const contextCancelBound = 2 * time.Second

// contextCancelArgs overrides the per-type default arguments
// (contextCancelDefaultArg) for a method whose defaults would not reach the
// code path that waits. Keys are method names; values are the arguments after
// the context, in order, built against the Host under test.
var contextCancelArgs = map[string]func(h *Host) []any{
	"QueryRaftLog": func(*Host) []any {
		return []any{uint64(1), uint64(1), uint64(3), uint64(1 << 20)}
	},
	"RequestLeaderTransfer": func(*Host) []any {
		return []any{uint64(1), uint64(2)}
	},
	// RegisterSession needs a session with a client ID that is not yet
	// registered.
	"RegisterSession": func(*Host) []any {
		return []any{NewSession(1, 42)}
	},
	// CloseSession needs a registered session; where none can be registered
	// (no quorum) an unregistered one is refused before any wait.
	"CloseSession": func(h *Host) []any {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, err := h.NewSession(ctx, 1)
		if err != nil {
			s = NewSession(1, 42)
		}
		return []any{uint64(1), s}
	},
	// ProposeWithSession refuses a NoOP session, so it needs a registered one.
	"ProposeWithSession": func(h *Host) []any {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, err := h.NewSession(ctx, 1)
		if err != nil {
			s = NewSession(1, 43)
		}
		return []any{s, []byte("k")}
	},
	"SyncProposeWithSession": func(h *Host) []any {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, err := h.NewSession(ctx, 1)
		if err != nil {
			s = NewSession(1, 43)
		}
		return []any{s, []byte("k")}
	},
	// SyncRequestRemoveNode needs a member other than the sole voter:
	// replica 4 joins as an observer first.
	"SyncRequestRemoveNode": func(h *Host) []any {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = h.SyncRequestAddObserver(ctx, 1, 4, "127.0.0.1:5004", 0)
		return []any{uint64(1), uint64(4), uint64(0)}
	},
	// A zero policy with no ConfigChangeID check.
	"SyncRequestUpdateWitnessPolicy": func(*Host) []any {
		return []any{uint64(1), nil, uint64(0)}
	},
	"SyncRemoveData": func(*Host) []any {
		// Data removal needs the replica to be stopped first; replica 2 is
		// not running on this host.
		return []any{uint64(1), uint64(2)}
	},
}

// contextCancelMethods lists every exported *Host method whose first
// argument (after the receiver) is a context.Context.
func contextCancelMethods(t *testing.T) []reflect.Method {
	t.Helper()
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	hostType := reflect.TypeOf(&Host{})
	var out []reflect.Method
	for i := 0; i < hostType.NumMethod(); i++ {
		m := hostType.Method(i)
		if m.Type.NumIn() >= 2 && m.Type.In(1) == ctxType {
			out = append(out, m)
		}
	}
	if len(out) == 0 {
		t.Fatal("no *Host method takes a context.Context")
	}
	return out
}

// contextCancelCallArgs builds the arguments after the context for m.
func contextCancelCallArgs(h *Host, m reflect.Method) []reflect.Value {
	ft := m.Type
	var vals []reflect.Value
	if build, ok := contextCancelArgs[m.Name]; ok {
		for i, a := range build(h) {
			in := ft.In(2 + i)
			if a == nil {
				vals = append(vals, reflect.Zero(in))
				continue
			}
			vals = append(vals, reflect.ValueOf(a).Convert(in))
		}
		return vals
	}
	n := ft.NumIn()
	if ft.IsVariadic() {
		n-- // variadic options are left empty
	}
	uints := 0
	for i := 2; i < n; i++ {
		vals = append(vals, contextCancelDefaultArg(h, ft.In(i), &uints))
	}
	return vals
}

// contextCancelDefaultArg returns a valid default for a parameter type. The
// first uint64 is the shard ID (1), the second a replica ID that is not a
// member (4), the rest a config change index (0).
func contextCancelDefaultArg(h *Host, in reflect.Type, uints *int) reflect.Value {
	switch in {
	case reflect.TypeOf(uint64(0)):
		*uints++
		switch *uints {
		case 1:
			return reflect.ValueOf(uint64(1))
		case 2:
			return reflect.ValueOf(uint64(4))
		default:
			return reflect.ValueOf(uint64(0))
		}
	case reflect.TypeOf(""):
		return reflect.ValueOf("127.0.0.1:5004")
	case reflect.TypeOf([]byte(nil)):
		return reflect.ValueOf([]byte("k"))
	case reflect.TypeOf((*Session)(nil)):
		return reflect.ValueOf(h.NoOPSession(1))
	case reflect.TypeOf((*io.Writer)(nil)).Elem():
		return reflect.ValueOf(io.Discard)
	case reflect.TypeOf((*io.Reader)(nil)).Elem():
		return reflect.ValueOf(io.Reader(bytes.NewReader(nil)))
	case reflect.TypeOf((*interface{})(nil)).Elem():
		return reflect.ValueOf([]byte("k"))
	}
	return reflect.Zero(in)
}

// contextCancelCall invokes m with args and returns its error, releasing any
// buffer or request state the method returned.
func contextCancelCall(ctx context.Context, h *Host, m reflect.Method, args []reflect.Value) error {
	in := append([]reflect.Value{reflect.ValueOf(h), reflect.ValueOf(ctx)}, args...)
	out := m.Func.Call(in)
	var err error
	for _, v := range out {
		switch x := v.Interface().(type) {
		case error:
			err = x
		case func():
			if x != nil {
				x()
			}
		case *RequestState:
			if x != nil {
				x.Release()
			}
		}
	}
	return err
}

// isContextCancelErr reports whether err is the caller's cancellation: it
// must match both the documented library error and context.Canceled, so a
// caller can test for either.
func isContextCancelErr(err error) bool {
	return errors.Is(err, context.Canceled) && errors.Is(err, ErrCanceled)
}

// isContextDeadlineErr reports whether err is the caller's expired deadline:
// it must match both ErrTimeout and context.DeadlineExceeded.
func isContextDeadlineErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) && errors.Is(err, ErrTimeout)
}

// startBlockedShard starts shard 1 as the elected single replica, with a state
// machine whose Update blocks once armed. The returned block func arms it and
// proposes one entry, returning once Update holds that entry: from then on any
// admitted work that needs an apply stays pending until the test ends.
func startBlockedShard(t *testing.T) (*Host, func(), func()) {
	t.Helper()
	gate := &contextCancelGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	// Lets a witness join the single full replica, so witness adds are
	// admitted and wait on the apply.
	scfg.Witness.RequireFullReplicaMajority = config.BoolPtr(false)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	create := func(uint64, uint64) interface{} {
		return &contextCancelBlockingSM{gate: gate}
	}
	if err := h.StartShard(members, false, create, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	t.Cleanup(gate.open)
	block := func() {
		t.Helper()
		gate.armed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), contextCancelBound)
		defer cancel()
		rs, err := h.Propose(ctx, 1, []byte("block"))
		if err != nil {
			t.Fatalf("blocking Propose failed: %v", err)
		}
		rs.Release()
		select {
		case <-gate.entered:
		case <-time.After(contextCancelBound):
			t.Fatalf("state machine Update was not entered within %v", contextCancelBound)
		}
	}
	return h, block, gate.open
}

// contextCancelGate holds the state machine's Update once armed, so admitted
// work behind it cannot complete until the gate opens.
type contextCancelGate struct {
	armed       atomic.Bool
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

func (g *contextCancelGate) open() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// contextCancelBlockingSM is a mockStateMachine whose Update blocks on its
// gate once the gate is armed.
type contextCancelBlockingSM struct {
	mockStateMachine
	gate *contextCancelGate
}

func (s *contextCancelBlockingSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	if s.gate.armed.Load() {
		s.gate.enteredOnce.Do(func() { close(s.gate.entered) })
		select {
		case <-s.gate.release:
		case <-ctx.Done():
		}
	}
	return s.mockStateMachine.Update(ctx, entries, results)
}

// contextCancelSettle waits until the goroutine count falls to base, and
// reports the count it last saw.
func contextCancelSettle(base int) (int, bool) {
	deadline := time.Now().Add(contextCancelBound)
	n := runtime.NumGoroutine()
	for n > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n, n <= base
}

// TestHostContextCancel calls every exported *Host method that takes a
// context, once with an already-cancelled context against a shard with a
// leader and once with a context cancelled 50ms into a call against a shard
// with no quorum. Each call must return within contextCancelBound with the
// caller's cancellation (or, in the second case, any error it returned
// before the cancel), and leave no goroutine behind once its Host closes.
// contextCancelImmediate names the context-taking Host methods that cannot
// block on the blocked shard, with the reason. Every other method must still
// be pending 50ms into the call and return the cancellation error once its
// context is cancelled.
var contextCancelImmediate = map[string]string{
	// Asynchronous: each returns a RequestState once the work is queued; the
	// wait belongs to the caller.
	"Propose":                        "returns a RequestState once queued",
	"ProposeWithSession":             "returns a RequestState once queued",
	"ReadIndex":                      "returns a RequestState once queued",
	"CloseSession":                   "returns a RequestState once queued",
	"RequestSnapshot":                "returns a RequestState once queued",
	"RequestAddNode":                 "returns a RequestState once queued",
	"RequestAddNodeWithIdentity":     "returns a RequestState once queued",
	"RequestAddObserver":             "returns a RequestState once queued",
	"RequestAddObserverWithIdentity": "returns a RequestState once queued",
	"RequestAddWitness":              "returns a RequestState once queued",
	"RequestAddWitnessWithIdentity":  "returns a RequestState once queued",
	"RequestRemoveNode":              "returns a RequestState once queued",
	"RequestUpdateWitnessPolicy":     "returns a RequestState once queued",
	// RequestLeaderTransfer only hands the transfer to Raft; a single-replica
	// shard has no target, so it is refused before any wait.
	"RequestLeaderTransfer": "queues the transfer without waiting for it",
	// Local reads of on-disk state that never wait on the shard's apply.
	"QueryRaftLog":   "reads the local Raft log",
	"ExportSnapshot": "copies the saved snapshot; this shard has none",
	// Both act only on a stopped replica and refuse a running one at once.
	"ImportSnapshot": "refuses a running shard",
	"SyncRemoveData": "removes a stopped replica's data; replica 2 never ran here",
	// ResumeShard on a shard that is not paused has nothing to wait for.
	"ResumeShard": "the shard is not paused",
}

// contextCancelUncancellable names methods that block on the blocked shard
// and ignore their context: the in-flight case is skipped with the reason.
// The local reads call the state machine's Lookup, which waits behind the
// Update the fixture holds without selecting on the context.
var contextCancelUncancellable = map[string]string{
	"StaleRead":      "Lookup waits behind a running Update without its context",
	"StaleReadBuf":   "Lookup waits behind a running Update without its context",
	"QueryLocalNode": "Lookup waits behind a running Update without its context",
	"FollowerRead":   "Lookup waits behind a running Update without its context",
}

func TestHostContextCancel(t *testing.T) {
	methods := contextCancelMethods(t)
	known := make(map[string]bool, len(methods))
	for _, m := range methods {
		known[m.Name] = true
	}
	for _, table := range []map[string]string{contextCancelImmediate, contextCancelUncancellable} {
		for name := range table {
			if !known[name] {
				t.Fatalf("%s is listed but is not a context-taking *Host method", name)
			}
		}
	}

	// Warm up package-level goroutines (pools, timers) before any baseline.
	newTestHost(t).Close()

	for _, m := range methods {
		t.Run(m.Name+"/precancelled", func(t *testing.T) {
			base := runtime.NumGoroutine()
			h := startSingleVoterShard(t)
			args := contextCancelCallArgs(h, m)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			done := make(chan error, 1)
			go func() { done <- contextCancelCall(ctx, h, m, args) }()
			select {
			case err := <-done:
				if !isContextCancelErr(err) {
					t.Errorf("%s with a cancelled context returned %v, want ErrCanceled and context.Canceled", m.Name, err)
				}
			case <-time.After(contextCancelBound):
				t.Fatalf("%s with a cancelled context did not return within %v", m.Name, contextCancelBound)
			}
			h.Close()
			if n, ok := contextCancelSettle(base); !ok {
				t.Errorf("%s left goroutines behind: %d, want <= %d", m.Name, n, base)
			}
		})
		t.Run(m.Name+"/expired", func(t *testing.T) {
			base := runtime.NumGoroutine()
			h := startSingleVoterShard(t)
			args := contextCancelCallArgs(h, m)
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- contextCancelCall(ctx, h, m, args) }()
			select {
			case err := <-done:
				if !isContextDeadlineErr(err) {
					t.Errorf("%s with an expired context returned %v, want ErrTimeout and context.DeadlineExceeded", m.Name, err)
				}
			case <-time.After(contextCancelBound):
				t.Fatalf("%s with an expired context did not return within %v", m.Name, contextCancelBound)
			}
			h.Close()
			if n, ok := contextCancelSettle(base); !ok {
				t.Errorf("%s left goroutines behind: %d, want <= %d", m.Name, n, base)
			}
		})
		t.Run(m.Name+"/inflight", func(t *testing.T) {
			if reason, ok := contextCancelUncancellable[m.Name]; ok {
				t.Skipf("%s does not honour a cancel in flight: %s", m.Name, reason)
			}
			base := runtime.NumGoroutine()
			h, block, release := startBlockedShard(t)
			args := contextCancelCallArgs(h, m)
			block()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- contextCancelCall(ctx, h, m, args) }()
			if reason, ok := contextCancelImmediate[m.Name]; ok {
				// Cannot block: it must return without a cancel.
				select {
				case <-done:
				case <-time.After(contextCancelBound):
					t.Fatalf("%s (%s) did not return within %v", m.Name, reason, contextCancelBound)
				}
			} else {
				select {
				case err := <-done:
					t.Fatalf("%s returned %v before its context was cancelled, want it pending on the blocked shard", m.Name, err)
				case <-time.After(50 * time.Millisecond):
				}
				cancel()
				select {
				case err := <-done:
					// A request already handed to Raft returns ErrCanceled
					// inside a MaybeAppliedError, without the context error.
					if !errors.Is(err, ErrCanceled) {
						t.Errorf("%s cancelled in flight returned %v, want ErrCanceled", m.Name, err)
					}
				case <-time.After(contextCancelBound):
					t.Fatalf("%s cancelled in flight did not return within %v", m.Name, contextCancelBound)
				}
			}
			release()
			h.Close()
			if n, ok := contextCancelSettle(base); !ok {
				t.Errorf("%s left goroutines behind: %d, want <= %d", m.Name, n, base)
			}
		})
	}
}
