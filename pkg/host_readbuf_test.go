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

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// State machines for the pooled-buffer read paths (SyncReadBuf / StaleReadBuf)
// ---------------------------------------------------------------------------

// errTestKeyNotFound is the sentinel a test state machine returns for a
// missing key. It must survive the whole rsm -> Host return path unchanged so
// callers can classify the failure with errors.Is.
var errTestKeyNotFound = errors.New("test sm: key not found")

// errTestLookupFailed is the sentinel a test state machine returns from the
// generic Lookup fallback.
var errTestLookupFailed = errors.New("test sm: lookup failed")

// bufLookupSM implements sm.NALookupInto so the pooled read fast path
// (rsm.NALookupBuf -> supported=true) is exercised end to end. It copies the
// value into the caller-supplied buffer as the NALookupInto contract requires.
type bufLookupSM struct {
	mockStateMachine
	values map[string][]byte
}

// BytesLookup satisfies sm.NALookup, which sm.NALookupInto embeds.
func (m *bufLookupSM) BytesLookup(key []byte) ([]byte, error) {
	v, ok := m.values[string(key)]
	if !ok {
		return nil, errTestKeyNotFound
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// BytesLookupInto copies the value into dst, growing it via append.
func (m *bufLookupSM) BytesLookupInto(key, dst []byte) ([]byte, error) {
	v, ok := m.values[string(key)]
	if !ok {
		return nil, errTestKeyNotFound
	}
	return append(dst[:0], v...), nil
}

var _ sm.NALookupInto = (*bufLookupSM)(nil)

// stringLookupSM has no byte-slice lookup at all, so the pooled path reports
// unsupported and the Host falls back to Lookup — which returns a string, the
// type SyncReadBuf/StaleReadBuf must reject with ErrInvalidOperation.
type stringLookupSM struct {
	mockStateMachine
}

func (m *stringLookupSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return "not-a-byte-slice", nil
}

// errLookupSM fails the generic Lookup fallback so the error is observed on
// the lookupErr2 branch rather than the pooled branch.
type errLookupSM struct {
	mockStateMachine
}

func (m *errLookupSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return nil, errTestLookupFailed
}

// startReadBufShard starts a single-node shard whose state machine is built by
// create, waits for it to elect itself leader, and returns the host.
func startReadBufShard(t *testing.T, create sm.CreateFunc) *Host {
	t.Helper()
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

// ---------------------------------------------------------------------------
// SyncReadBuf
// ---------------------------------------------------------------------------

// TestSyncReadBuf_PooledPathReturnsCopy verifies the linearizable pooled read:
// the value reaches the caller intact, the buffer is a COPY (mutating it must
// not corrupt state machine state, which is the NALookupInto contract), and
// the release func returned alongside it frees the state machine read guard so
// the shard can subsequently stop.
func TestSyncReadBuf_PooledPathReturnsCopy(t *testing.T) {
	want := []byte("pooled-value")
	backing := &bufLookupSM{values: map[string][]byte{"k": want}}
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return backing })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	buf, release, err := h.SyncReadBuf(ctx, 1, []byte("k"))
	if err != nil {
		t.Fatalf("SyncReadBuf: %v", err)
	}
	if release == nil {
		t.Fatal("SyncReadBuf returned a nil release func")
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("SyncReadBuf = %q, want %q", buf, want)
	}

	// The pooled buffer must be the caller's to scribble on: overwriting it
	// must not reach into the state machine's stored value.
	for i := range buf {
		buf[i] = 'X'
	}
	release()

	if !bytes.Equal(backing.values["k"], want) {
		t.Fatalf("state machine value corrupted by caller write: %q", backing.values["k"])
	}

	// The second read proves the SM value survived and that release() handed
	// the pooled buffer back rather than leaving it checked out.
	buf2, release2, err := h.SyncReadBuf(ctx, 1, []byte("k"))
	if err != nil {
		t.Fatalf("second SyncReadBuf: %v", err)
	}
	if !bytes.Equal(buf2, want) {
		t.Fatalf("second SyncReadBuf = %q, want %q", buf2, want)
	}
	release2()

	// release() must also have dropped the rsm read guard; if it did not,
	// StopShard would block forever closing the state machine.
	stopped := make(chan error, 1)
	go func() { stopped <- h.StopShard(1) }()
	select {
	case stopErr := <-stopped:
		if stopErr != nil {
			t.Fatalf("StopShard after release: %v", stopErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopShard blocked: SyncReadBuf release() did not drop the state machine read guard")
	}
}

// TestSyncReadBuf_PooledLookupErrorPropagates verifies that an error from the
// state machine's BytesLookupInto reaches the caller unwrapped (errors.Is
// must classify it) and that the caller still gets a callable release func.
func TestSyncReadBuf_PooledLookupErrorPropagates(t *testing.T) {
	backing := &bufLookupSM{values: map[string][]byte{}}
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return backing })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	buf, release, err := h.SyncReadBuf(ctx, 1, []byte("missing"))
	if !errors.Is(err, errTestKeyNotFound) {
		t.Fatalf("SyncReadBuf error = %v, want errTestKeyNotFound", err)
	}
	if buf != nil {
		t.Fatalf("SyncReadBuf returned %q with an error, want nil", buf)
	}
	if release == nil {
		t.Fatal("SyncReadBuf returned a nil release func on error")
	}
	release() // must be safe on the error path (nopHostRelease)
}

// TestSyncReadBuf_FallbackToLookup verifies the non-pooled fallback: a state
// machine with no byte-slice lookup goes through Lookup, and a []byte result
// is returned directly with the no-op release.
func TestSyncReadBuf_FallbackToLookup(t *testing.T) {
	// mockStateMachine.Lookup echoes the query, so the []byte key comes back
	// as a []byte value through the fallback branch.
	h := startReadBufShard(t, testCreateFunc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := []byte("echo-key")
	buf, release, err := h.SyncReadBuf(ctx, 1, key)
	if err != nil {
		t.Fatalf("SyncReadBuf: %v", err)
	}
	if !bytes.Equal(buf, key) {
		t.Fatalf("SyncReadBuf = %q, want %q", buf, key)
	}
	if release == nil {
		t.Fatal("SyncReadBuf returned a nil release func")
	}
	release()
}

// TestSyncReadBuf_NonByteResultRejected verifies that a state machine which
// answers Lookup with a non-[]byte value is rejected with ErrInvalidOperation
// instead of panicking on a type assertion or returning a zero value.
func TestSyncReadBuf_NonByteResultRejected(t *testing.T) {
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return &stringLookupSM{} })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	buf, release, err := h.SyncReadBuf(ctx, 1, []byte("k"))
	if !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("SyncReadBuf error = %v, want ErrInvalidOperation", err)
	}
	if buf != nil {
		t.Fatalf("SyncReadBuf returned %q, want nil", buf)
	}
	release()
}

// TestSyncReadBuf_FallbackLookupErrorPropagates verifies that an error from
// the generic Lookup fallback reaches the caller.
func TestSyncReadBuf_FallbackLookupErrorPropagates(t *testing.T) {
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return &errLookupSM{} })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	buf, release, err := h.SyncReadBuf(ctx, 1, []byte("k"))
	if !errors.Is(err, errTestLookupFailed) {
		t.Fatalf("SyncReadBuf error = %v, want errTestLookupFailed", err)
	}
	if buf != nil {
		t.Fatalf("SyncReadBuf returned %q, want nil", buf)
	}
	release()
}

// TestSyncReadBuf_ReadIndexRejected verifies that a failure to obtain a read
// index is surfaced verbatim and still yields a callable release func, so a
// caller that unconditionally defers release() cannot panic.
func TestSyncReadBuf_ReadIndexRejected(t *testing.T) {
	t.Run("shard not found", func(t *testing.T) {
		h := newTestHost(t)
		buf, release, err := h.SyncReadBuf(context.Background(), 999, []byte("k"))
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("SyncReadBuf error = %v, want ErrShardNotFound", err)
		}
		if buf != nil {
			t.Fatalf("SyncReadBuf returned %q, want nil", buf)
		}
		if release == nil {
			t.Fatal("SyncReadBuf returned a nil release func")
		}
		release()
	})

	t.Run("host closed", func(t *testing.T) {
		h := newTestHost(t)
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		buf, release, err := h.SyncReadBuf(context.Background(), 1, []byte("k"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("SyncReadBuf error = %v, want ErrClosed", err)
		}
		if buf != nil {
			t.Fatalf("SyncReadBuf returned %q, want nil", buf)
		}
		release()
	})

	t.Run("shard stopped", func(t *testing.T) {
		// A shard marked stopped must be rejected before the state machine
		// is touched. getNode pre-empts SyncReadBuf's own stopped check, so
		// the caller sees ErrShardNotFound, not ErrClosed; the in-method
		// check only fires when StopShard wins the race after getNode.
		h := startReadBufShard(t, testCreateFunc)
		testLoadNode(h, 1).stopped.Store(true)

		buf, release, err := h.SyncReadBuf(context.Background(), 1, []byte("k"))
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("SyncReadBuf error = %v, want ErrShardNotFound", err)
		}
		if buf != nil {
			t.Fatalf("SyncReadBuf returned %q, want nil", buf)
		}
		release()
	})
}

// TestSyncReadBuf_ContextCanceled verifies that a canceled context aborts the
// read with ErrCanceled rather than returning a result read after the caller
// gave up.
func TestSyncReadBuf_ContextCanceled(t *testing.T) {
	backing := &bufLookupSM{values: map[string][]byte{"k": []byte("v")}}
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return backing })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	buf, release, err := h.SyncReadBuf(ctx, 1, []byte("k"))
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("SyncReadBuf error = %v, want ErrCanceled", err)
	}
	if buf != nil {
		t.Fatalf("SyncReadBuf returned %q, want nil", buf)
	}
	release()
}

// ---------------------------------------------------------------------------
// StaleReadBuf
// ---------------------------------------------------------------------------

// TestStaleReadBuf_PooledPathReturnsCopy verifies the stale pooled read serves
// local state with no consensus round trip (no leader required), returns a
// caller-owned copy, and releases the state machine read guard.
func TestStaleReadBuf_PooledPathReturnsCopy(t *testing.T) {
	want := []byte("stale-value")
	backing := &bufLookupSM{values: map[string][]byte{"k": want}}

	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	create := func(uint64, uint64) interface{} { return backing }
	if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}

	buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("k"))
	if err != nil {
		t.Fatalf("StaleReadBuf: %v", err)
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("StaleReadBuf = %q, want %q", buf, want)
	}
	for i := range buf {
		buf[i] = 'X'
	}
	release()

	if !bytes.Equal(backing.values["k"], want) {
		t.Fatalf("state machine value corrupted by caller write: %q", backing.values["k"])
	}

	stopped := make(chan error, 1)
	go func() { stopped <- h.StopShard(1) }()
	select {
	case stopErr := <-stopped:
		if stopErr != nil {
			t.Fatalf("StopShard after release: %v", stopErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopShard blocked: StaleReadBuf release() did not drop the state machine read guard")
	}
}

// TestStaleReadBuf_PooledLookupErrorPropagates verifies the state machine's
// error identity survives the pooled path.
func TestStaleReadBuf_PooledLookupErrorPropagates(t *testing.T) {
	backing := &bufLookupSM{values: map[string][]byte{}}
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	create := func(uint64, uint64) interface{} { return backing }
	if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}

	buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("missing"))
	if !errors.Is(err, errTestKeyNotFound) {
		t.Fatalf("StaleReadBuf error = %v, want errTestKeyNotFound", err)
	}
	if buf != nil {
		t.Fatalf("StaleReadBuf returned %q, want nil", buf)
	}
	release()
}

// TestStaleReadBuf_FallbackAndRejections covers the non-pooled fallback and
// every guard StaleReadBuf applies before touching the state machine.
func TestStaleReadBuf_FallbackAndRejections(t *testing.T) {
	t.Run("fallback returns byte result", func(t *testing.T) {
		h := newTestHost(t)
		members := map[uint64]string{1: "127.0.0.1:5000"}
		if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
			t.Fatalf("StartShard failed: %v", err)
		}
		key := []byte("echo-key")
		buf, release, err := h.StaleReadBuf(context.Background(), 1, key)
		if err != nil {
			t.Fatalf("StaleReadBuf: %v", err)
		}
		if !bytes.Equal(buf, key) {
			t.Fatalf("StaleReadBuf = %q, want %q", buf, key)
		}
		release()
	})

	t.Run("non byte result rejected", func(t *testing.T) {
		h := newTestHost(t)
		members := map[uint64]string{1: "127.0.0.1:5000"}
		create := func(uint64, uint64) interface{} { return &stringLookupSM{} }
		if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
			t.Fatalf("StartShard failed: %v", err)
		}
		buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("k"))
		if !errors.Is(err, ErrInvalidOperation) {
			t.Fatalf("StaleReadBuf error = %v, want ErrInvalidOperation", err)
		}
		if buf != nil {
			t.Fatalf("StaleReadBuf returned %q, want nil", buf)
		}
		release()
	})

	t.Run("fallback lookup error", func(t *testing.T) {
		h := newTestHost(t)
		members := map[uint64]string{1: "127.0.0.1:5000"}
		create := func(uint64, uint64) interface{} { return &errLookupSM{} }
		if err := h.StartShard(members, false, create, testShardConfig(1, 1)); err != nil {
			t.Fatalf("StartShard failed: %v", err)
		}
		buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("k"))
		if !errors.Is(err, errTestLookupFailed) {
			t.Fatalf("StaleReadBuf error = %v, want errTestLookupFailed", err)
		}
		if buf != nil {
			t.Fatalf("StaleReadBuf returned %q, want nil", buf)
		}
		release()
	})

	t.Run("shard not found", func(t *testing.T) {
		h := newTestHost(t)
		buf, release, err := h.StaleReadBuf(context.Background(), 999, []byte("k"))
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("StaleReadBuf error = %v, want ErrShardNotFound", err)
		}
		if buf != nil {
			t.Fatalf("StaleReadBuf returned %q, want nil", buf)
		}
		release()
	})

	t.Run("host closed", func(t *testing.T) {
		h := newTestHost(t)
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("k"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("StaleReadBuf error = %v, want ErrClosed", err)
		}
		if buf != nil {
			t.Fatalf("StaleReadBuf returned %q, want nil", buf)
		}
		release()
	})

	t.Run("shard stopped", func(t *testing.T) {
		h := newTestHost(t)
		members := map[uint64]string{1: "127.0.0.1:5000"}
		if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
			t.Fatalf("StartShard failed: %v", err)
		}
		testLoadNode(h, 1).stopped.Store(true)

		// getNode pre-empts StaleReadBuf's own stopped check, so a stopped
		// shard surfaces as ErrShardNotFound to the caller.
		buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("k"))
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("StaleReadBuf error = %v, want ErrShardNotFound", err)
		}
		if buf != nil {
			t.Fatalf("StaleReadBuf returned %q, want nil", buf)
		}
		release()
	})

	t.Run("state machine not ready", func(t *testing.T) {
		// A nodeState registered before its state machine is constructed
		// must report ErrShardNotReady rather than dereferencing nil.
		h := newTestHost(t)
		addTestNodeState(h, 42, 1)

		buf, release, err := h.StaleReadBuf(context.Background(), 42, []byte("k"))
		if !errors.Is(err, ErrShardNotReady) {
			t.Fatalf("StaleReadBuf error = %v, want ErrShardNotReady", err)
		}
		if buf != nil {
			t.Fatalf("StaleReadBuf returned %q, want nil", buf)
		}
		release()
	})
}
