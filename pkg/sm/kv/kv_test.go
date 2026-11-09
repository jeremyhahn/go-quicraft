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

package kv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// mustEncodePut is a test helper that panics on error. Only use with
// known-valid small keys in test code.
func mustEncodePut(key, value []byte) []byte {
	cmd, err := EncodePut(key, value)
	if err != nil {
		panic("mustEncodePut: " + err.Error())
	}
	return cmd
}

// mustEncodeGet is a test helper that panics on error. Only use with
// known-valid small keys in test code.
func mustEncodeGet(key []byte) []byte {
	cmd, err := EncodeGet(key)
	if err != nil {
		panic("mustEncodeGet: " + err.Error())
	}
	return cmd
}

// mustEncodeDelete is a test helper that panics on error. Only use with
// known-valid small keys in test code.
func mustEncodeDelete(key []byte) []byte {
	cmd, err := EncodeDelete(key)
	if err != nil {
		panic("mustEncodeDelete: " + err.Error())
	}
	return cmd
}

// backend wraps both MemoryStore and ConcurrentStore for table-driven tests.
type backend struct {
	name   string
	update func(ctx context.Context, entries []sm.Entry, results []sm.Result) error
	lookup func(ctx context.Context, query interface{}) (interface{}, error)
	bytes  func(key []byte) ([]byte, error)
	into   func(key, dst []byte) ([]byte, error)
	hash   func() (uint64, error)
	save   func(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	recov  func(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
}

func newMemoryBackend() backend {
	ms := NewMemoryStore()
	return backend{
		name:   "memory",
		update: ms.Update,
		lookup: ms.Lookup,
		bytes:  ms.BytesLookup,
		into:   ms.BytesLookupInto,
		hash:   ms.GetHash,
		save:   ms.SaveSnapshot,
		recov:  ms.RecoverFromSnapshot,
	}
}

func newConcurrentBackend() backend {
	cs := NewConcurrentStore()
	return backend{
		name:   "concurrent",
		update: cs.Update,
		lookup: cs.Lookup,
		bytes:  cs.BytesLookup,
		into:   cs.BytesLookupInto,
		hash:   cs.GetHash,
		save: func(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
			snapCtx, err := cs.PrepareSnapshot()
			if err != nil {
				return err
			}
			return cs.SaveSnapshot(ctx, snapCtx, w, stopper)
		},
		recov: cs.RecoverFromSnapshot,
	}
}

// putAll applies a Put of each key/value pair in one Update.
func putAll(t *testing.T, b backend, kvs ...string) {
	t.Helper()
	entries := make([]sm.Entry, 0, len(kvs)/2)
	for i := 0; i+1 < len(kvs); i += 2 {
		entries = append(entries, sm.Entry{Index: uint64(i/2 + 1), Cmd: mustEncodePut([]byte(kvs[i]), []byte(kvs[i+1]))})
	}
	if err := b.update(context.Background(), entries, make([]sm.Result, len(entries))); err != nil {
		t.Fatalf("Update = %v", err)
	}
}

func backends() []backend {
	return []backend{newMemoryBackend(), newConcurrentBackend()}
}

// ---------------------------------------------------------------------------
// Protocol tests
// ---------------------------------------------------------------------------

func TestProtocol_EncodeRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		encode func() ([]byte, error)
		op     byte
		key    string
		value  []byte
	}{
		{"put", func() ([]byte, error) { return EncodePut([]byte("key"), []byte("value")) }, OpPut, "key", []byte("value")},
		{"put_empty_value", func() ([]byte, error) { return EncodePut([]byte("k"), nil) }, OpPut, "k", nil},
		{"get", func() ([]byte, error) { return EncodeGet([]byte("mykey")) }, OpGet, "mykey", nil},
		{"delete", func() ([]byte, error) { return EncodeDelete([]byte("delkey")) }, OpDelete, "delkey", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := tc.encode()
			if err != nil {
				t.Fatalf("encode = %v", err)
			}
			decoded, err := DecodeCommand(cmd)
			if err != nil {
				t.Fatalf("DecodeCommand = %v", err)
			}
			if decoded.Op != tc.op {
				t.Errorf("Op = %d, want %d", decoded.Op, tc.op)
			}
			if string(decoded.Key) != tc.key {
				t.Errorf("Key = %q, want %q", decoded.Key, tc.key)
			}
			if (tc.value == nil) != (decoded.Value == nil) || !bytes.Equal(decoded.Value, tc.value) {
				t.Errorf("Value = %v, want %v", decoded.Value, tc.value)
			}
		})
	}
}

func TestProtocol_DecodeCommand_Invalid(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"too_short", []byte{0x01}},
		{"unknown_op", []byte{0xFF, 0x00, 0x00}},
		{"zero_op", []byte{0x00, 0x00, 0x00}},
		// op=1, keyLen=255 but only 0 bytes of key data
		{"key_len_exceeds_data", []byte{0x01, 0xFF, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCommand(tc.data)
			var invCmd *InvalidCommandError
			if !errors.As(err, &invCmd) {
				t.Errorf("DecodeCommand = %v, want InvalidCommandError", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// KV tests: run against both backends
// ---------------------------------------------------------------------------

func TestKV_PutAndGet(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			// Put
			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("hello"), []byte("world"))},
			}
			results := make([]sm.Result, 1)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}
			if results[0].Value != 5 { // len("world")
				t.Errorf("result = %d, want 5", results[0].Value)
			}

			// Lookup via interface{}
			val, err := b.lookup(ctx, []byte("hello"))
			if err != nil {
				t.Fatalf("Lookup = %v", err)
			}
			if string(val.([]byte)) != "world" {
				t.Errorf("Lookup = %q, want %q", val, "world")
			}

			// BytesLookup
			bval, err := b.bytes([]byte("hello"))
			if err != nil {
				t.Fatalf("BytesLookup = %v", err)
			}
			if string(bval) != "world" {
				t.Errorf("BytesLookup = %q, want %q", bval, "world")
			}
		})
	}
}

func TestKV_Delete(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			// One batch: two puts, delete one key, then a put after the
			// delete. The delete must remove only its own key.
			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("a"), []byte("1"))},
				{Index: 2, Cmd: mustEncodePut([]byte("b"), []byte("2"))},
				{Index: 3, Cmd: mustEncodeDelete([]byte("a"))},
				{Index: 4, Cmd: mustEncodePut([]byte("c"), []byte("3"))},
			}
			results := make([]sm.Result, len(entries))
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}
			if results[2].Value != 1 {
				t.Errorf("delete result = %d, want 1", results[2].Value)
			}

			// Verify the deleted key is gone
			_, err := b.lookup(ctx, []byte("a"))
			if err == nil {
				t.Fatal("expected KeyNotFoundError after delete")
			}
			var knf *KeyNotFoundError
			if !errors.As(err, &knf) {
				t.Errorf("expected KeyNotFoundError, got %T: %v", err, err)
			}

			// The unrelated key and the later put survive the delete
			for key, want := range map[string]string{"b": "2", "c": "3"} {
				val, err := b.lookup(ctx, []byte(key))
				if err != nil {
					t.Fatalf("Lookup(%q) = %v", key, err)
				}
				if string(val.([]byte)) != want {
					t.Errorf("Lookup(%q) = %q, want %q", key, val, want)
				}
			}
		})
	}
}

func TestKV_GetMissing(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			_, err := b.lookup(context.Background(), []byte("nonexistent"))
			var knf *KeyNotFoundError
			if !errors.As(err, &knf) {
				t.Fatalf("Lookup = %v, want KeyNotFoundError", err)
			}
			if knf.Key != "nonexistent" {
				t.Errorf("Key = %q, want %q", knf.Key, "nonexistent")
			}
			if _, err := b.bytes([]byte("missing")); !errors.As(err, &knf) {
				t.Errorf("BytesLookup = %v, want KeyNotFoundError", err)
			}
		})
	}
}

func TestKV_SnapshotRoundTrip(t *testing.T) {
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b := bFn()
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := make(chan struct{})

			// Insert data
			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("alpha"), []byte("one"))},
				{Index: 2, Cmd: mustEncodePut([]byte("beta"), []byte("two"))},
			}
			results := make([]sm.Result, 2)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			// Save
			var buf bytes.Buffer
			if err := b.save(ctx, &buf, stopper); err != nil {
				t.Fatalf("SaveSnapshot = %v", err)
			}

			// Recover into new backend
			b2 := bFn()
			if err := b2.recov(ctx, &buf, stopper); err != nil {
				t.Fatalf("RecoverFromSnapshot = %v", err)
			}

			// Verify
			for _, pair := range []struct{ key, val string }{
				{"alpha", "one"},
				{"beta", "two"},
			} {
				val, err := b2.lookup(ctx, []byte(pair.key))
				if err != nil {
					t.Fatalf("Lookup(%s) = %v", pair.key, err)
				}
				if string(val.([]byte)) != pair.val {
					t.Errorf("Lookup(%s) = %q, want %q", pair.key, val, pair.val)
				}
			}
		})
	}
}

func TestKV_GetHash_DifferentData(t *testing.T) {
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b1 := bFn()
		b2 := bFn()
		t.Run(b1.name, func(t *testing.T) {
			ctx := context.Background()

			e1 := []sm.Entry{{Index: 1, Cmd: mustEncodePut([]byte("a"), []byte("1"))}}
			r1 := make([]sm.Result, 1)
			if err := b1.update(ctx, e1, r1); err != nil {
				t.Fatalf("Update = %v", err)
			}

			e2 := []sm.Entry{{Index: 1, Cmd: mustEncodePut([]byte("a"), []byte("2"))}}
			r2 := make([]sm.Result, 1)
			if err := b2.update(ctx, e2, r2); err != nil {
				t.Fatalf("Update = %v", err)
			}

			h1, _ := b1.hash()
			h2, _ := b2.hash()
			if h1 == h2 {
				t.Error("expected different hashes for different data")
			}
		})
	}
}

func TestKV_GetHash_OrderIndependent(t *testing.T) {
	// Keys should be sorted before hashing, so insertion order shouldn't matter
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b1 := bFn()
		b2 := bFn()
		t.Run(b1.name, func(t *testing.T) {
			ctx := context.Background()

			// Insert in order: a, b, c
			e1 := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("a"), []byte("1"))},
				{Index: 2, Cmd: mustEncodePut([]byte("b"), []byte("2"))},
				{Index: 3, Cmd: mustEncodePut([]byte("c"), []byte("3"))},
			}
			r1 := make([]sm.Result, 3)
			if err := b1.update(ctx, e1, r1); err != nil {
				t.Fatalf("Update = %v", err)
			}

			// Insert in order: c, a, b
			e2 := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("c"), []byte("3"))},
				{Index: 2, Cmd: mustEncodePut([]byte("a"), []byte("1"))},
				{Index: 3, Cmd: mustEncodePut([]byte("b"), []byte("2"))},
			}
			r2 := make([]sm.Result, 3)
			if err := b2.update(ctx, e2, r2); err != nil {
				t.Fatalf("Update = %v", err)
			}

			h1, err := b1.hash()
			if err != nil {
				t.Fatalf("GetHash b1 = %v", err)
			}
			h2, err := b2.hash()
			if err != nil {
				t.Fatalf("GetHash b2 = %v", err)
			}

			if h1 != h2 {
				t.Errorf("GetHash differs based on insertion order: %d vs %d", h1, h2)
			}
			if h1 == 0 {
				t.Error("hash should be non-zero")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MemoryStore-specific tests
// ---------------------------------------------------------------------------

func TestKV_LookupQueryTypes(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			putAll(t, b, "strkey", "strval")

			val, err := b.lookup(ctx, "strkey")
			if err != nil {
				t.Fatalf("Lookup(string) = %v", err)
			}
			if string(val.([]byte)) != "strval" {
				t.Errorf("Lookup(string) = %q, want %q", val, "strval")
			}

			_, err = b.lookup(ctx, 12345)
			var invCmd *InvalidCommandError
			if !errors.As(err, &invCmd) {
				t.Errorf("Lookup(int) = %v, want InvalidCommandError", err)
			}
		})
	}
}

func TestMemoryStore_GetInUpdate(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodeGet([]byte("anything"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}
	if results[0].Value != 0 {
		t.Errorf("Get in Update result = %d, want 0", results[0].Value)
	}
}

func TestKV_SnapshotStopped(t *testing.T) {
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b := bFn()
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			// Put some data so iteration happens
			putAll(t, b, "k", "v")
			closedStopper := make(chan struct{})
			close(closedStopper)

			var discard bytes.Buffer
			if err := b.save(ctx, &discard, closedStopper); err != context.Canceled {
				t.Errorf("SaveSnapshot = %v, want context.Canceled", err)
			}

			var buf bytes.Buffer
			if err := b.save(ctx, &buf, make(chan struct{})); err != nil {
				t.Fatalf("SaveSnapshot = %v", err)
			}
			if err := bFn().recov(ctx, &buf, closedStopper); err != context.Canceled {
				t.Errorf("RecoverFromSnapshot = %v, want context.Canceled", err)
			}
		})
	}
}

func TestKV_BytesLookupInto(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			putAll(t, b, "k", "hello")

			t.Run("nil dst allocates new buffer", func(t *testing.T) {
				got, err := b.into([]byte("k"), nil)
				if err != nil {
					t.Fatalf("BytesLookupInto = %v", err)
				}
				if string(got) != "hello" {
					t.Errorf("BytesLookupInto = %q, want %q", got, "hello")
				}
			})

			t.Run("preallocated dst is reused", func(t *testing.T) {
				dst := make([]byte, 0, 16)
				got, err := b.into([]byte("k"), dst)
				if err != nil {
					t.Fatalf("BytesLookupInto = %v", err)
				}
				if string(got) != "hello" {
					t.Errorf("BytesLookupInto = %q, want %q", got, "hello")
				}
				// Backing array should be the same pointer when capacity is sufficient.
				if cap(got) != cap(dst) {
					t.Errorf("BytesLookupInto grew unnecessarily: cap(got)=%d, cap(dst)=%d", cap(got), cap(dst))
				}
			})

			t.Run("missing key returns error", func(t *testing.T) {
				got, err := b.into([]byte("missing"), nil)
				var kne *KeyNotFoundError
				if !errors.As(err, &kne) {
					t.Errorf("BytesLookupInto = %v, want *KeyNotFoundError", err)
				}
				if got != nil {
					t.Errorf("BytesLookupInto result = %v, want nil on error", got)
				}
			})
		})
	}
}

// TestKV_LookupReturnsCopy verifies that mutating a value returned by any
// lookup path does not corrupt the store.
func TestKV_LookupReturnsCopy(t *testing.T) {
	ctx := context.Background()
	paths := map[string]func(b backend) ([]byte, error){
		"Lookup": func(b backend) ([]byte, error) {
			v, err := b.lookup(ctx, []byte("key"))
			if err != nil {
				return nil, err
			}
			return v.([]byte), nil
		},
		"BytesLookup":     func(b backend) ([]byte, error) { return b.bytes([]byte("key")) },
		"BytesLookupInto": func(b backend) ([]byte, error) { return b.into([]byte("key"), nil) },
	}
	for _, b := range backends() {
		putAll(t, b, "key", "original")
		for name, lookup := range paths {
			t.Run(b.name+"/"+name, func(t *testing.T) {
				got, err := lookup(b)
				if err != nil {
					t.Fatalf("%s = %v", name, err)
				}
				got[0] = 'X'
				got2, err := lookup(b)
				if err != nil {
					t.Fatalf("%s = %v", name, err)
				}
				if string(got2) != "original" {
					t.Errorf("%s after mutation = %q, want %q", name, got2, "original")
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// ConcurrentStore-specific tests
// ---------------------------------------------------------------------------

func TestConcurrentStore_PrepareSnapshotInvalidCtx(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	var buf bytes.Buffer
	err := cs.SaveSnapshot(ctx, "not a map", &buf, stopper)
	if err == nil {
		t.Fatal("expected error for invalid snapshot context")
	}
}

// ---------------------------------------------------------------------------
// List tests: run against both backends
// ---------------------------------------------------------------------------

func TestKV_ListWithPrefix(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("users/alice"), []byte("a"))},
				{Index: 2, Cmd: mustEncodePut([]byte("users/bob"), []byte("b"))},
				{Index: 3, Cmd: mustEncodePut([]byte("certs/ca"), []byte("c"))},
			}
			results := make([]sm.Result, 3)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			val, err := b.lookup(ctx, ListQuery{Prefix: "users/"})
			if err != nil {
				t.Fatalf("Lookup(ListQuery) = %v", err)
			}
			keys, ok := val.([]string)
			if !ok {
				t.Fatalf("Lookup returned %T, want []string", val)
			}
			if len(keys) != 2 {
				t.Fatalf("len(keys) = %d, want 2", len(keys))
			}
			if keys[0] != "users/alice" || keys[1] != "users/bob" {
				t.Errorf("keys = %v, want [users/alice users/bob]", keys)
			}
		})
	}
}

func TestKV_ListEmptyPrefix(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("b"), []byte("2"))},
				{Index: 2, Cmd: mustEncodePut([]byte("a"), []byte("1"))},
				{Index: 3, Cmd: mustEncodePut([]byte("c"), []byte("3"))},
			}
			results := make([]sm.Result, 3)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			val, err := b.lookup(ctx, ListQuery{Prefix: ""})
			if err != nil {
				t.Fatalf("Lookup(ListQuery) = %v", err)
			}
			keys := val.([]string)
			if len(keys) != 3 {
				t.Fatalf("len(keys) = %d, want 3", len(keys))
			}
			// Must be sorted
			if keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
				t.Errorf("keys = %v, want [a b c]", keys)
			}
		})
	}
}

func TestKV_ListNoMatches(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("foo"), []byte("bar"))},
			}
			results := make([]sm.Result, 1)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			val, err := b.lookup(ctx, ListQuery{Prefix: "nonexistent/"})
			if err != nil {
				t.Fatalf("Lookup(ListQuery) = %v", err)
			}
			keys := val.([]string)
			if keys == nil {
				t.Fatal("keys should not be nil, want empty []string{}")
			}
			if len(keys) != 0 {
				t.Errorf("len(keys) = %d, want 0", len(keys))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Error type tests
// ---------------------------------------------------------------------------

func TestErrors_Message(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&KeyNotFoundError{Key: "missing"}, "kv: key not found: missing"},
		{&InvalidCommandError{Reason: "bad data"}, "kv: invalid command: bad data"},
		{&HashWriteError{Err: &InvalidCommandError{Reason: "test error"}}, "kv: hash write failed: kv: invalid command: test error"},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
	}
}

func TestHashWriteError_Unwrap(t *testing.T) {
	innerErr := &InvalidCommandError{Reason: "wrapped"}
	err := &HashWriteError{Err: innerErr}
	unwrapped := err.Unwrap()
	if unwrapped != innerErr {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, innerErr)
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("errors.As failed to unwrap to InvalidCommandError")
	}
}

// ---------------------------------------------------------------------------
// Factory tests
// ---------------------------------------------------------------------------

func TestNewMemoryCreateFunc(t *testing.T) {
	result := NewMemoryCreateFunc()(1, 1)
	smImpl, ok := result.(sm.StateMachine)
	if !ok {
		t.Fatalf("factory returned %T, want sm.StateMachine", result)
	}
	if _, ok := result.(sm.ConcurrentStateMachine); ok {
		t.Error("MemoryStore should not implement ConcurrentStateMachine")
	}
	if err := smImpl.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestNewConcurrentCreateFunc(t *testing.T) {
	result := NewConcurrentCreateFunc()(1, 1)
	smImpl, ok := result.(sm.ConcurrentStateMachine)
	if !ok {
		t.Fatalf("factory returned %T, want sm.ConcurrentStateMachine", result)
	}
	if _, ok := result.(sm.Hasher); !ok {
		t.Error("ConcurrentStore should implement sm.Hasher")
	}
	if _, ok := result.(sm.NALookup); !ok {
		t.Error("ConcurrentStore should implement sm.NALookup")
	}
	if err := smImpl.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

// ---------------------------------------------------------------------------
// Multiple operations in single Update
// ---------------------------------------------------------------------------

func TestKV_DeleteNonexistentKey(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodeDelete([]byte("ghost"))},
			}
			results := make([]sm.Result, 1)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}
			if results[0].Value != 1 {
				t.Errorf("delete result = %d, want 1", results[0].Value)
			}
		})
	}
}

// errWriter is a writer that fails after a configurable number of bytes.
type errWriter struct {
	limit   int
	written int
}

func (w *errWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		return 0, &InvalidCommandError{Reason: "write error"}
	}
	w.written += len(p)
	return len(p), nil
}

func TestKV_SaveSnapshotWriteError(t *testing.T) {
	limits := map[string]int{
		"before_count": 0, // fails before even writing count
		"mid_stream":   4, // allows count (4 bytes), fails on key length
	}
	for _, b := range backends() {
		putAll(t, b, "k", "v")
		for name, limit := range limits {
			t.Run(b.name+"/"+name, func(t *testing.T) {
				if err := b.save(context.Background(), &errWriter{limit: limit}, make(chan struct{})); err == nil {
					t.Fatal("expected error from broken writer")
				}
			})
		}
	}
}

func TestKV_RecoverFromSnapshotTruncated(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		// count=1 but no entry data
		{"count_only", []byte{0x01, 0x00, 0x00, 0x00}},
		// count=1, keyLen=10, only 2 bytes of key
		{"partial_key", []byte{0x01, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x41, 0x42}},
		// count=1, keyLen=1, key="A", valLen=100, no value
		{"partial_value", []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x41, 0x64, 0x00, 0x00, 0x00}},
	}
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		for _, tc := range cases {
			b := bFn()
			t.Run(b.name+"/"+tc.name, func(t *testing.T) {
				if err := b.recov(context.Background(), bytes.NewReader(tc.data), make(chan struct{})); err == nil {
					t.Fatal("expected error from truncated snapshot")
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Key-too-long error coverage
// ---------------------------------------------------------------------------

func TestProtocol_Encode_KeyTooLong(t *testing.T) {
	key := make([]byte, MaxKeyLength+1)
	encoders := map[string]func() ([]byte, error){
		"put":    func() ([]byte, error) { return EncodePut(key, []byte("val")) },
		"get":    func() ([]byte, error) { return EncodeGet(key) },
		"delete": func() ([]byte, error) { return EncodeDelete(key) },
	}
	for name, encode := range encoders {
		t.Run(name, func(t *testing.T) {
			_, err := encode()
			var ktl *KeyTooLongError
			if !errors.As(err, &ktl) {
				t.Fatalf("encode = %v, want KeyTooLongError", err)
			}
			if ktl.Length != MaxKeyLength+1 {
				t.Errorf("KeyTooLongError.Length = %d, want %d", ktl.Length, MaxKeyLength+1)
			}
		})
	}
}

func TestKeyTooLongError_Message(t *testing.T) {
	err := &KeyTooLongError{Length: 70000}
	msg := err.Error()
	if msg == "" {
		t.Fatal("error message should not be empty")
	}
	// Verify it contains the length and the max.
	if !bytes.Contains([]byte(msg), []byte("70000")) {
		t.Errorf("error message should contain length 70000, got: %s", msg)
	}
	if !bytes.Contains([]byte(msg), []byte("65535")) {
		t.Errorf("error message should contain max length 65535, got: %s", msg)
	}
}

// ---------------------------------------------------------------------------
// ConcurrentStore default case in Update switch
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ConcurrentStore recover partial read errors
// ---------------------------------------------------------------------------

func TestKV_Overwrite(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("key"), []byte("first"))},
				{Index: 2, Cmd: mustEncodePut([]byte("key"), []byte("second"))},
			}
			results := make([]sm.Result, 2)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			val, err := b.lookup(ctx, []byte("key"))
			if err != nil {
				t.Fatalf("Lookup = %v", err)
			}
			if string(val.([]byte)) != "second" {
				t.Errorf("Lookup = %q, want %q", val, "second")
			}
		})
	}
}
