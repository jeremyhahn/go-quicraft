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
	hash   func() (uint64, error)
	save   func(ctx context.Context, buf *bytes.Buffer, stopper <-chan struct{}) error
	recov  func(ctx context.Context, buf *bytes.Buffer, stopper <-chan struct{}) error
	close  func(ctx context.Context) error
}

func newMemoryBackend() backend {
	ms := NewMemoryStore()
	return backend{
		name:   "memory",
		update: ms.Update,
		lookup: ms.Lookup,
		bytes:  ms.BytesLookup,
		hash:   ms.GetHash,
		save: func(ctx context.Context, buf *bytes.Buffer, stopper <-chan struct{}) error {
			return ms.SaveSnapshot(ctx, buf, stopper)
		},
		recov: func(ctx context.Context, buf *bytes.Buffer, stopper <-chan struct{}) error {
			return ms.RecoverFromSnapshot(ctx, buf, stopper)
		},
		close: ms.Close,
	}
}

func newConcurrentBackend() backend {
	cs := NewConcurrentStore()
	return backend{
		name:   "concurrent",
		update: cs.Update,
		lookup: cs.Lookup,
		bytes:  cs.BytesLookup,
		hash:   cs.GetHash,
		save: func(ctx context.Context, buf *bytes.Buffer, stopper <-chan struct{}) error {
			snapCtx, err := cs.PrepareSnapshot()
			if err != nil {
				return err
			}
			return cs.SaveSnapshot(ctx, snapCtx, buf, stopper)
		},
		recov: func(ctx context.Context, buf *bytes.Buffer, stopper <-chan struct{}) error {
			return cs.RecoverFromSnapshot(ctx, buf, stopper)
		},
		close: cs.Close,
	}
}

func backends() []backend {
	return []backend{newMemoryBackend(), newConcurrentBackend()}
}

// ---------------------------------------------------------------------------
// Protocol tests
// ---------------------------------------------------------------------------

func TestProtocol_EncodePut(t *testing.T) {
	cmd, err := EncodePut([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("EncodePut = %v", err)
	}
	decoded, err := DecodeCommand(cmd)
	if err != nil {
		t.Fatalf("DecodeCommand = %v", err)
	}
	if decoded.Op != OpPut {
		t.Errorf("Op = %d, want %d", decoded.Op, OpPut)
	}
	if string(decoded.Key) != "key" {
		t.Errorf("Key = %q, want %q", decoded.Key, "key")
	}
	if string(decoded.Value) != "value" {
		t.Errorf("Value = %q, want %q", decoded.Value, "value")
	}
}

func TestProtocol_EncodeGet(t *testing.T) {
	cmd, err := EncodeGet([]byte("mykey"))
	if err != nil {
		t.Fatalf("EncodeGet = %v", err)
	}
	decoded, err := DecodeCommand(cmd)
	if err != nil {
		t.Fatalf("DecodeCommand = %v", err)
	}
	if decoded.Op != OpGet {
		t.Errorf("Op = %d, want %d", decoded.Op, OpGet)
	}
	if string(decoded.Key) != "mykey" {
		t.Errorf("Key = %q, want %q", decoded.Key, "mykey")
	}
	if decoded.Value != nil {
		t.Errorf("Value = %v, want nil", decoded.Value)
	}
}

func TestProtocol_EncodeDelete(t *testing.T) {
	cmd, err := EncodeDelete([]byte("delkey"))
	if err != nil {
		t.Fatalf("EncodeDelete = %v", err)
	}
	decoded, err := DecodeCommand(cmd)
	if err != nil {
		t.Fatalf("DecodeCommand = %v", err)
	}
	if decoded.Op != OpDelete {
		t.Errorf("Op = %d, want %d", decoded.Op, OpDelete)
	}
	if string(decoded.Key) != "delkey" {
		t.Errorf("Key = %q, want %q", decoded.Key, "delkey")
	}
}

func TestProtocol_DecodeCommand_TooShort(t *testing.T) {
	_, err := DecodeCommand([]byte{0x01})
	if err == nil {
		t.Fatal("expected error for short data")
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("expected InvalidCommandError, got %T", err)
	}
}

func TestProtocol_DecodeCommand_UnknownOp(t *testing.T) {
	data := []byte{0xFF, 0x00, 0x00}
	_, err := DecodeCommand(data)
	if err == nil {
		t.Fatal("expected error for unknown op")
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("expected InvalidCommandError, got %T", err)
	}
}

func TestProtocol_DecodeCommand_KeyLenExceedsData(t *testing.T) {
	// op=1, keyLen=255 but only 0 bytes of key data
	data := []byte{0x01, 0xFF, 0x00}
	_, err := DecodeCommand(data)
	if err == nil {
		t.Fatal("expected error for key length exceeding data")
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("expected InvalidCommandError, got %T", err)
	}
}

func TestProtocol_DecodeCommand_ZeroOp(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00}
	_, err := DecodeCommand(data)
	if err == nil {
		t.Fatal("expected error for zero op")
	}
}

func TestProtocol_EncodePut_EmptyValue(t *testing.T) {
	cmd, err := EncodePut([]byte("k"), nil)
	if err != nil {
		t.Fatalf("EncodePut = %v", err)
	}
	decoded, err := DecodeCommand(cmd)
	if err != nil {
		t.Fatalf("DecodeCommand = %v", err)
	}
	if decoded.Op != OpPut {
		t.Errorf("Op = %d, want %d", decoded.Op, OpPut)
	}
	if string(decoded.Key) != "k" {
		t.Errorf("Key = %q, want %q", decoded.Key, "k")
	}
	if decoded.Value != nil {
		t.Errorf("Value = %v, want nil", decoded.Value)
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

			// Put then delete
			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
				{Index: 2, Cmd: mustEncodeDelete([]byte("k"))},
			}
			results := make([]sm.Result, 2)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}
			if results[1].Value != 1 {
				t.Errorf("delete result = %d, want 1", results[1].Value)
			}

			// Verify key is gone
			_, err := b.lookup(ctx, []byte("k"))
			if err == nil {
				t.Fatal("expected KeyNotFoundError after delete")
			}
			var knf *KeyNotFoundError
			if !errors.As(err, &knf) {
				t.Errorf("expected KeyNotFoundError, got %T: %v", err, err)
			}
		})
	}
}

func TestKV_GetMissing(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			_, err := b.lookup(ctx, []byte("nonexistent"))
			if err == nil {
				t.Fatal("expected error for missing key")
			}
			var knf *KeyNotFoundError
			if !errors.As(err, &knf) {
				t.Errorf("expected KeyNotFoundError, got %T", err)
			}
			if knf.Key != "nonexistent" {
				t.Errorf("Key = %q, want %q", knf.Key, "nonexistent")
			}
		})
	}
}

func TestKV_GetMissing_BytesLookup(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			_, err := b.bytes([]byte("missing"))
			if err == nil {
				t.Fatal("expected error for missing key")
			}
			var knf *KeyNotFoundError
			if !errors.As(err, &knf) {
				t.Errorf("expected KeyNotFoundError, got %T", err)
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

func TestKV_GetHash(t *testing.T) {
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b := bFn()
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			// Insert identical data into two backends
			b2 := bFn()
			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("x"), []byte("1"))},
				{Index: 2, Cmd: mustEncodePut([]byte("y"), []byte("2"))},
			}
			results := make([]sm.Result, 2)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update b1 = %v", err)
			}
			results2 := make([]sm.Result, 2)
			if err := b2.update(ctx, entries, results2); err != nil {
				t.Fatalf("Update b2 = %v", err)
			}

			h1, err := b.hash()
			if err != nil {
				t.Fatalf("GetHash b1 = %v", err)
			}
			h2, err := b2.hash()
			if err != nil {
				t.Fatalf("GetHash b2 = %v", err)
			}
			if h1 != h2 {
				t.Errorf("hashes differ: %d vs %d", h1, h2)
			}
			if h1 == 0 {
				t.Error("hash should be non-zero")
			}
		})
	}
}

func TestKV_GetHash_Empty(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			h, err := b.hash()
			if err != nil {
				t.Fatalf("GetHash = %v", err)
			}
			// Empty store should still produce a valid hash (the FNV offset basis).
			_ = h
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

func TestKV_GetHash_Deterministic(t *testing.T) {
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b := bFn()
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("z"), []byte("last"))},
				{Index: 2, Cmd: mustEncodePut([]byte("a"), []byte("first"))},
				{Index: 3, Cmd: mustEncodePut([]byte("m"), []byte("middle"))},
			}
			results := make([]sm.Result, 3)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			// Call GetHash multiple times - should always return the same value
			h1, err := b.hash()
			if err != nil {
				t.Fatalf("GetHash 1 = %v", err)
			}
			h2, err := b.hash()
			if err != nil {
				t.Fatalf("GetHash 2 = %v", err)
			}
			h3, err := b.hash()
			if err != nil {
				t.Fatalf("GetHash 3 = %v", err)
			}

			if h1 != h2 || h2 != h3 {
				t.Errorf("GetHash not deterministic: %d, %d, %d", h1, h2, h3)
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
		})
	}
}

func TestKV_GetHash_LargeDataset(t *testing.T) {
	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b := bFn()
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			// Insert 100 key-value pairs
			entries := make([]sm.Entry, 100)
			for i := range entries {
				key := []byte("key" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
				val := []byte("value" + string(rune(i)))
				entries[i] = sm.Entry{Index: uint64(i + 1), Cmd: mustEncodePut(key, val)}
			}
			results := make([]sm.Result, 100)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			h, err := b.hash()
			if err != nil {
				t.Fatalf("GetHash = %v", err)
			}
			if h == 0 {
				t.Error("GetHash should be non-zero for non-empty store")
			}
		})
	}
}

func TestKV_InvalidCommand(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name+"/short_data", func(t *testing.T) {
			ctx := context.Background()
			entries := []sm.Entry{{Index: 1, Cmd: []byte{0x01}}}
			results := make([]sm.Result, 1)
			err := b.update(ctx, entries, results)
			if err == nil {
				t.Fatal("expected error for short command")
			}
			var invCmd *InvalidCommandError
			if !errors.As(err, &invCmd) {
				t.Errorf("expected InvalidCommandError, got %T", err)
			}
		})
	}

	for _, bFn := range []func() backend{newMemoryBackend, newConcurrentBackend} {
		b := bFn()
		t.Run(b.name+"/bad_op", func(t *testing.T) {
			ctx := context.Background()
			entries := []sm.Entry{{Index: 1, Cmd: []byte{0xFF, 0x00, 0x00}}}
			results := make([]sm.Result, 1)
			err := b.update(ctx, entries, results)
			if err == nil {
				t.Fatal("expected error for bad op")
			}
			var invCmd *InvalidCommandError
			if !errors.As(err, &invCmd) {
				t.Errorf("expected InvalidCommandError, got %T", err)
			}
		})
	}
}

func TestKV_Close(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			if err := b.close(context.Background()); err != nil {
				t.Fatalf("Close = %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MemoryStore-specific tests
// ---------------------------------------------------------------------------

func TestMemoryStore_LookupStringQuery(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("strkey"), []byte("strval"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	val, err := ms.Lookup(ctx, "strkey")
	if err != nil {
		t.Fatalf("Lookup = %v", err)
	}
	if string(val.([]byte)) != "strval" {
		t.Errorf("Lookup = %q, want %q", val, "strval")
	}
}

func TestMemoryStore_LookupInvalidQuery(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	_, err := ms.Lookup(ctx, 12345)
	if err == nil {
		t.Fatal("expected error for invalid query type")
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("expected InvalidCommandError, got %T", err)
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

func TestMemoryStore_SnapshotStopped(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	// Put some data so iteration happens
	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	// Pre-closed stopper
	stopper := make(chan struct{})
	close(stopper)
	var buf bytes.Buffer
	err := ms.SaveSnapshot(ctx, &buf, stopper)
	if err != context.Canceled {
		t.Errorf("SaveSnapshot = %v, want context.Canceled", err)
	}
}

func TestMemoryStore_RecoverStopped(t *testing.T) {
	ms := NewMemoryStore()
	ms2 := NewMemoryStore()
	ctx := context.Background()

	// Put data and take snapshot
	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	openStopper := make(chan struct{})
	var buf bytes.Buffer
	if err := ms.SaveSnapshot(ctx, &buf, openStopper); err != nil {
		t.Fatalf("SaveSnapshot = %v", err)
	}

	// Try to recover with pre-closed stopper
	closedStopper := make(chan struct{})
	close(closedStopper)
	err := ms2.RecoverFromSnapshot(ctx, &buf, closedStopper)
	if err != context.Canceled {
		t.Errorf("RecoverFromSnapshot = %v, want context.Canceled", err)
	}
}

func TestMemoryStore_LookupReturnsCopy(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("key"), []byte("original"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	// Lookup and mutate the returned value
	val, err := ms.Lookup(ctx, []byte("key"))
	if err != nil {
		t.Fatalf("Lookup = %v", err)
	}
	valBytes := val.([]byte)
	valBytes[0] = 'X' // mutate

	// Verify original is unaffected
	val2, err := ms.Lookup(ctx, []byte("key"))
	if err != nil {
		t.Fatalf("Lookup = %v", err)
	}
	if string(val2.([]byte)) != "original" {
		t.Errorf("Lookup after mutation = %q, want %q", val2, "original")
	}
}

func TestMemoryStore_BytesLookupReturnsCopy(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("key"), []byte("original"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	val, err := ms.BytesLookup([]byte("key"))
	if err != nil {
		t.Fatalf("BytesLookup = %v", err)
	}
	val[0] = 'X'

	val2, err := ms.BytesLookup([]byte("key"))
	if err != nil {
		t.Fatalf("BytesLookup = %v", err)
	}
	if string(val2) != "original" {
		t.Errorf("BytesLookup after mutation = %q, want %q", val2, "original")
	}
}

func TestMemoryStore_BytesLookupInto(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("hello"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	t.Run("nil dst allocates new buffer", func(t *testing.T) {
		got, err := ms.BytesLookupInto([]byte("k"), nil)
		if err != nil {
			t.Fatalf("BytesLookupInto = %v", err)
		}
		if string(got) != "hello" {
			t.Errorf("BytesLookupInto = %q, want %q", got, "hello")
		}
	})

	t.Run("preallocated dst is reused", func(t *testing.T) {
		dst := make([]byte, 0, 16)
		got, err := ms.BytesLookupInto([]byte("k"), dst)
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
		got, err := ms.BytesLookupInto([]byte("missing"), nil)
		if err == nil {
			t.Fatal("BytesLookupInto should return error for missing key")
		}
		var kne *KeyNotFoundError
		if !errors.As(err, &kne) {
			t.Errorf("error = %T, want *KeyNotFoundError", err)
		}
		if got != nil {
			t.Errorf("BytesLookupInto result = %v, want nil on error", got)
		}
	})
}

func TestMemoryStore_BytesLookupInto_IndependentOfInternalState(t *testing.T) {
	// Verifies that mutating the returned slice does not corrupt the store.
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("key"), []byte("value"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	got, err := ms.BytesLookupInto([]byte("key"), nil)
	if err != nil {
		t.Fatalf("BytesLookupInto = %v", err)
	}
	// Mutate the returned slice.
	got[0] = 'X'

	// Second lookup must return the original value.
	got2, err := ms.BytesLookupInto([]byte("key"), nil)
	if err != nil {
		t.Fatalf("BytesLookupInto = %v", err)
	}
	if string(got2) != "value" {
		t.Errorf("after mutation BytesLookupInto = %q, want %q", got2, "value")
	}
}

// ---------------------------------------------------------------------------
// ConcurrentStore-specific tests
// ---------------------------------------------------------------------------

func TestConcurrentStore_LookupStringQuery(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("strkey"), []byte("strval"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	val, err := cs.Lookup(ctx, "strkey")
	if err != nil {
		t.Fatalf("Lookup = %v", err)
	}
	if string(val.([]byte)) != "strval" {
		t.Errorf("Lookup = %q, want %q", val, "strval")
	}
}

func TestConcurrentStore_LookupInvalidQuery(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	_, err := cs.Lookup(ctx, 12345)
	if err == nil {
		t.Fatal("expected error for invalid query type")
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("expected InvalidCommandError, got %T", err)
	}
}

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

func TestConcurrentStore_SnapshotStopped(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	snapCtx, err := cs.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot = %v", err)
	}

	stopper := make(chan struct{})
	close(stopper)
	var buf bytes.Buffer
	err = cs.SaveSnapshot(ctx, snapCtx, &buf, stopper)
	if err != context.Canceled {
		t.Errorf("SaveSnapshot = %v, want context.Canceled", err)
	}
}

func TestConcurrentStore_RecoverStopped(t *testing.T) {
	cs := NewConcurrentStore()
	cs2 := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	snapCtx, err := cs.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot = %v", err)
	}

	openStopper := make(chan struct{})
	var buf bytes.Buffer
	if err := cs.SaveSnapshot(ctx, snapCtx, &buf, openStopper); err != nil {
		t.Fatalf("SaveSnapshot = %v", err)
	}

	closedStopper := make(chan struct{})
	close(closedStopper)
	err = cs2.RecoverFromSnapshot(ctx, &buf, closedStopper)
	if err != context.Canceled {
		t.Errorf("RecoverFromSnapshot = %v, want context.Canceled", err)
	}
}

func TestConcurrentStore_BytesLookupInto(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("hello"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	t.Run("nil dst allocates new buffer", func(t *testing.T) {
		got, err := cs.BytesLookupInto([]byte("k"), nil)
		if err != nil {
			t.Fatalf("BytesLookupInto = %v", err)
		}
		if string(got) != "hello" {
			t.Errorf("BytesLookupInto = %q, want %q", got, "hello")
		}
	})

	t.Run("preallocated dst is reused", func(t *testing.T) {
		dst := make([]byte, 0, 16)
		got, err := cs.BytesLookupInto([]byte("k"), dst)
		if err != nil {
			t.Fatalf("BytesLookupInto = %v", err)
		}
		if string(got) != "hello" {
			t.Errorf("BytesLookupInto = %q, want %q", got, "hello")
		}
		if cap(got) != cap(dst) {
			t.Errorf("BytesLookupInto grew unnecessarily: cap(got)=%d, cap(dst)=%d", cap(got), cap(dst))
		}
	})

	t.Run("missing key returns error", func(t *testing.T) {
		got, err := cs.BytesLookupInto([]byte("missing"), nil)
		if err == nil {
			t.Fatal("BytesLookupInto should return error for missing key")
		}
		var kne *KeyNotFoundError
		if !errors.As(err, &kne) {
			t.Errorf("error = %T, want *KeyNotFoundError", err)
		}
		if got != nil {
			t.Errorf("BytesLookupInto result = %v, want nil on error", got)
		}
	})
}

func TestConcurrentStore_BytesLookupInto_IndependentOfInternalState(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("key"), []byte("value"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	got, err := cs.BytesLookupInto([]byte("key"), nil)
	if err != nil {
		t.Fatalf("BytesLookupInto = %v", err)
	}
	got[0] = 'X'

	got2, err := cs.BytesLookupInto([]byte("key"), nil)
	if err != nil {
		t.Fatalf("BytesLookupInto = %v", err)
	}
	if string(got2) != "value" {
		t.Errorf("after mutation BytesLookupInto = %q, want %q", got2, "value")
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

func TestKV_ListOnEmptyStore(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			val, err := b.lookup(ctx, ListQuery{Prefix: ""})
			if err != nil {
				t.Fatalf("Lookup(ListQuery) = %v", err)
			}
			keys := val.([]string)
			if keys == nil {
				t.Fatal("keys should not be nil")
			}
			if len(keys) != 0 {
				t.Errorf("len(keys) = %d, want 0", len(keys))
			}
		})
	}
}

func TestKV_ListDoesNotBreakExistingLookup(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("key"), []byte("value"))},
			}
			results := make([]sm.Result, 1)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			// []byte lookup still works
			val, err := b.lookup(ctx, []byte("key"))
			if err != nil {
				t.Fatalf("Lookup([]byte) = %v", err)
			}
			if string(val.([]byte)) != "value" {
				t.Errorf("Lookup([]byte) = %q, want %q", val, "value")
			}

			// BytesLookup still works
			bval, err := b.bytes([]byte("key"))
			if err != nil {
				t.Fatalf("BytesLookup = %v", err)
			}
			if string(bval) != "value" {
				t.Errorf("BytesLookup = %q, want %q", bval, "value")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Error type tests
// ---------------------------------------------------------------------------

func TestKeyNotFoundError_Message(t *testing.T) {
	err := &KeyNotFoundError{Key: "missing"}
	expected := "kv: key not found: missing"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

func TestInvalidCommandError_Message(t *testing.T) {
	err := &InvalidCommandError{Reason: "bad data"}
	expected := "kv: invalid command: bad data"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

func TestHashWriteError_Message(t *testing.T) {
	innerErr := &InvalidCommandError{Reason: "test error"}
	err := &HashWriteError{Err: innerErr}
	expected := "kv: hash write failed: kv: invalid command: test error"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
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
	cf := NewMemoryCreateFunc()
	result := cf(1, 1)
	if result == nil {
		t.Fatal("factory returned nil")
	}
	smImpl, ok := result.(sm.StateMachine)
	if !ok {
		t.Fatal("factory did not return sm.StateMachine")
	}
	if err := smImpl.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestNewMemoryCreateFunc_InvalidResult(t *testing.T) {
	cf := NewMemoryCreateFunc()
	result := cf(1, 1)
	_, ok := result.(sm.ConcurrentStateMachine)
	if ok {
		t.Error("MemoryStore should not implement ConcurrentStateMachine")
	}
}

func TestNewConcurrentCreateFunc(t *testing.T) {
	cf := NewConcurrentCreateFunc()
	result := cf(1, 1)
	if result == nil {
		t.Fatal("factory returned nil")
	}
	smImpl, ok := result.(sm.ConcurrentStateMachine)
	if !ok {
		t.Fatal("factory did not return sm.ConcurrentStateMachine")
	}
	if err := smImpl.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestNewConcurrentCreateFunc_HasherAndNALookup(t *testing.T) {
	cf := NewConcurrentCreateFunc()
	result := cf(1, 1)
	if _, ok := result.(sm.Hasher); !ok {
		t.Error("ConcurrentStore should implement sm.Hasher")
	}
	if _, ok := result.(sm.NALookup); !ok {
		t.Error("ConcurrentStore should implement sm.NALookup")
	}
}

// ---------------------------------------------------------------------------
// Multiple operations in single Update
// ---------------------------------------------------------------------------

func TestKV_MultipleOpsInSingleUpdate(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()

			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("a"), []byte("1"))},
				{Index: 2, Cmd: mustEncodePut([]byte("b"), []byte("2"))},
				{Index: 3, Cmd: mustEncodeDelete([]byte("a"))},
				{Index: 4, Cmd: mustEncodePut([]byte("c"), []byte("3"))},
			}
			results := make([]sm.Result, 4)
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v", err)
			}

			// a should be deleted
			_, err := b.lookup(ctx, []byte("a"))
			if err == nil {
				t.Error("expected error for deleted key 'a'")
			}

			// b should exist
			val, err := b.lookup(ctx, []byte("b"))
			if err != nil {
				t.Fatalf("Lookup(b) = %v", err)
			}
			if string(val.([]byte)) != "2" {
				t.Errorf("Lookup(b) = %q, want %q", val, "2")
			}

			// c should exist
			val, err = b.lookup(ctx, []byte("c"))
			if err != nil {
				t.Fatalf("Lookup(c) = %v", err)
			}
			if string(val.([]byte)) != "3" {
				t.Errorf("Lookup(c) = %q, want %q", val, "3")
			}
		})
	}
}

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

func TestMemoryStore_SaveSnapshotWriteError(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	// Writer that fails immediately (before even writing count)
	stopper := make(chan struct{})
	w := &errWriter{limit: 0}
	err := ms.SaveSnapshot(ctx, w, stopper)
	if err == nil {
		t.Fatal("expected error from broken writer")
	}
}

func TestMemoryStore_SaveSnapshotWriteErrorMidStream(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := ms.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	// Allow count (4 bytes) to write but fail on key length
	stopper := make(chan struct{})
	w := &errWriter{limit: 4}
	err := ms.SaveSnapshot(ctx, w, stopper)
	if err == nil {
		t.Fatal("expected error from broken writer mid-stream")
	}
}

func TestMemoryStore_RecoverFromSnapshotReadError(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	// Empty reader
	var buf bytes.Buffer
	err := ms.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from empty reader")
	}
}

func TestConcurrentStore_SaveSnapshotWriteError(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	snapCtx, err := cs.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot = %v", err)
	}

	stopper := make(chan struct{})
	w := &errWriter{limit: 0}
	err = cs.SaveSnapshot(ctx, snapCtx, w, stopper)
	if err == nil {
		t.Fatal("expected error from broken writer")
	}
}

func TestConcurrentStore_SaveSnapshotWriteErrorMidStream(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()

	entries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("k"), []byte("v"))},
	}
	results := make([]sm.Result, 1)
	if err := cs.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update = %v", err)
	}

	snapCtx, err := cs.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot = %v", err)
	}

	stopper := make(chan struct{})
	w := &errWriter{limit: 4} // allow count only
	err = cs.SaveSnapshot(ctx, snapCtx, w, stopper)
	if err == nil {
		t.Fatal("expected error from broken writer mid-stream")
	}
}

func TestConcurrentStore_RecoverFromSnapshotReadError(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	// Empty reader
	var buf bytes.Buffer
	err := cs.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from empty reader")
	}
}

func TestConcurrentStore_RecoverFromSnapshotPartialRead(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	// Write count of 1 but no actual data
	var buf bytes.Buffer
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // count=1
	err := cs.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from truncated reader")
	}
}

// ---------------------------------------------------------------------------
// Key-too-long error coverage
// ---------------------------------------------------------------------------

func TestProtocol_EncodePut_KeyTooLong(t *testing.T) {
	key := make([]byte, MaxKeyLength+1)
	_, err := EncodePut(key, []byte("val"))
	if err == nil {
		t.Fatal("expected error for key exceeding MaxKeyLength")
	}
	var ktl *KeyTooLongError
	if !errors.As(err, &ktl) {
		t.Fatalf("expected KeyTooLongError, got %T", err)
	}
	if ktl.Length != MaxKeyLength+1 {
		t.Errorf("KeyTooLongError.Length = %d, want %d", ktl.Length, MaxKeyLength+1)
	}
}

func TestProtocol_EncodeGet_KeyTooLong(t *testing.T) {
	key := make([]byte, MaxKeyLength+1)
	_, err := EncodeGet(key)
	if err == nil {
		t.Fatal("expected error for key exceeding MaxKeyLength")
	}
	var ktl *KeyTooLongError
	if !errors.As(err, &ktl) {
		t.Fatalf("expected KeyTooLongError, got %T", err)
	}
}

func TestProtocol_EncodeDelete_KeyTooLong(t *testing.T) {
	key := make([]byte, MaxKeyLength+1)
	_, err := EncodeDelete(key)
	if err == nil {
		t.Fatal("expected error for key exceeding MaxKeyLength")
	}
	var ktl *KeyTooLongError
	if !errors.As(err, &ktl) {
		t.Fatalf("expected KeyTooLongError, got %T", err)
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

func TestConcurrentStore_UpdateUnknownOpAfterDecode(t *testing.T) {
	// The ConcurrentStore.Update uses a switch with a default case
	// for unknown ops. The decode step catches unknown ops first, but
	// we can test the error path for an op that passes decode but not
	// the switch. OpGet is handled, so we need a valid but unexpected op
	// code. Since DecodeCommand validates op range, we test via the normal
	// invalid command path.
	cs := NewConcurrentStore()
	ctx := context.Background()

	// Feed an entry with op=0 (below OpPut range), header is valid.
	cmd := []byte{0x00, 0x01, 0x00, 'k'}
	entries := []sm.Entry{{Index: 1, Cmd: cmd}}
	results := make([]sm.Result, 1)
	err := cs.Update(ctx, entries, results)
	if err == nil {
		t.Fatal("expected error for unknown op code")
	}
	var invCmd *InvalidCommandError
	if !errors.As(err, &invCmd) {
		t.Errorf("expected InvalidCommandError, got %T", err)
	}
}

// ---------------------------------------------------------------------------
// ConcurrentStore recover partial read errors
// ---------------------------------------------------------------------------

func TestConcurrentStore_RecoverFromSnapshotPartialKeyRead(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	var buf bytes.Buffer
	// Write count=1, keyLen=10, but only 2 bytes of key data.
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // count=1
	buf.Write([]byte{0x0A, 0x00, 0x00, 0x00}) // keyLen=10
	buf.Write([]byte{0x41, 0x42})             // only 2 bytes of key
	err := cs.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from truncated key data")
	}
}

func TestConcurrentStore_RecoverFromSnapshotPartialValueRead(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	var buf bytes.Buffer
	// count=1, keyLen=1, key="A", valLen=100, but no value data.
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // count=1
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // keyLen=1
	buf.Write([]byte{0x41})                   // key="A"
	buf.Write([]byte{0x64, 0x00, 0x00, 0x00}) // valLen=100
	err := cs.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from truncated value data")
	}
}

func TestMemoryStore_RecoverFromSnapshotPartialKeyRead(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	var buf bytes.Buffer
	// count=1, keyLen=10, only partial key.
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // count=1
	buf.Write([]byte{0x0A, 0x00, 0x00, 0x00}) // keyLen=10
	buf.Write([]byte{0x41, 0x42})             // 2 bytes
	err := ms.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from truncated key data")
	}
}

func TestMemoryStore_RecoverFromSnapshotPartialValueRead(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()
	stopper := make(chan struct{})

	var buf bytes.Buffer
	// count=1, keyLen=1, key="A", valLen=100, no value.
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // count=1
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // keyLen=1
	buf.Write([]byte{0x41})                   // key
	buf.Write([]byte{0x64, 0x00, 0x00, 0x00}) // valLen=100
	err := ms.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Fatal("expected error from truncated value data")
	}
}

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
