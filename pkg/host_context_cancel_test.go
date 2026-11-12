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
	"testing"
	"time"
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

// startNoQuorumShard starts shard 1 as replica 1 of three on a host without a
// transport, so no leader is ever elected and every call that needs one waits.
func startNoQuorumShard(t *testing.T) *Host {
	t.Helper()
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000", 2: "127.0.0.1:5001", 3: "127.0.0.1:5002"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	return h
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
func TestHostContextCancel(t *testing.T) {
	methods := contextCancelMethods(t)

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
			base := runtime.NumGoroutine()
			h := startNoQuorumShard(t)
			args := contextCancelCallArgs(h, m)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- contextCancelCall(ctx, h, m, args) }()
			select {
			case <-done:
				// Returned before the cancel: the call does not wait on a
				// shard without quorum, which is fine.
			case <-time.After(50 * time.Millisecond):
				cancel()
				select {
				case err := <-done:
					if !isContextCancelErr(err) {
						t.Errorf("%s cancelled in flight returned %v, want ErrCanceled and context.Canceled", m.Name, err)
					}
				case <-time.After(contextCancelBound):
					t.Fatalf("%s cancelled in flight did not return within %v", m.Name, contextCancelBound)
				}
			}
			h.Close()
			if n, ok := contextCancelSettle(base); !ok {
				t.Errorf("%s left goroutines behind: %d, want <= %d", m.Name, n, base)
			}
		})
	}
}
