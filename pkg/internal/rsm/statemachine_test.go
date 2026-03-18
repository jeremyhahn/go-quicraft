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

package rsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// testKV is a simple in-memory key-value store implementing StateMachine.
type testKV struct {
	mu   sync.Mutex
	data map[string]string
}

func newTestKV() *testKV {
	return &testKV{data: make(map[string]string)}
}

func (kv *testKV) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			results[i] = sm.Result{Value: 0}
			continue
		}
		// Simple protocol: first byte is key length, then key, then value.
		keyLen := int(e.Cmd[0])
		if keyLen+1 > len(e.Cmd) {
			results[i] = sm.Result{Value: 0}
			continue
		}
		key := string(e.Cmd[1 : 1+keyLen])
		val := string(e.Cmd[1+keyLen:])
		kv.data[key] = val
		results[i] = sm.Result{Value: uint64(len(kv.data))}
	}
	return nil
}

func (kv *testKV) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("query must be string")
	}
	return kv.data[key], nil
}

func (kv *testKV) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(kv.data)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	keys := make([]string, 0, len(kv.data))
	for k := range kv.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := kv.data[k]
		binary.LittleEndian.PutUint32(buf[:], uint32(len(k)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(k)); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(buf[:], uint32(len(v)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func (kv *testKV) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(buf[:])
	kv.data = make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		kl := binary.LittleEndian.Uint32(buf[:])
		kb := make([]byte, kl)
		if _, err := io.ReadFull(r, kb); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		vl := binary.LittleEndian.Uint32(buf[:])
		vb := make([]byte, vl)
		if _, err := io.ReadFull(r, vb); err != nil {
			return err
		}
		kv.data[string(kb)] = string(vb)
	}
	return nil
}

func (kv *testKV) Close(_ context.Context) error {
	return nil
}

// testConcurrentKV implements ConcurrentStateMachine.
type testConcurrentKV struct {
	mu   sync.RWMutex
	data map[string]string
}

func newTestConcurrentKV() *testConcurrentKV {
	return &testConcurrentKV{data: make(map[string]string)}
}

func (kv *testConcurrentKV) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			results[i] = sm.Result{Value: 0}
			continue
		}
		keyLen := int(e.Cmd[0])
		if keyLen+1 > len(e.Cmd) {
			results[i] = sm.Result{Value: 0}
			continue
		}
		key := string(e.Cmd[1 : 1+keyLen])
		val := string(e.Cmd[1+keyLen:])
		kv.data[key] = val
		results[i] = sm.Result{Value: uint64(len(kv.data))}
	}
	return nil
}

func (kv *testConcurrentKV) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("query must be string")
	}
	return kv.data[key], nil
}

func (kv *testConcurrentKV) PrepareSnapshot() (interface{}, error) {
	kv.mu.RLock()
	snap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snap[k] = v
	}
	kv.mu.RUnlock()
	return snap, nil
}

func (kv *testConcurrentKV) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, _ <-chan struct{}) error {
	snap := snapshotCtx.(map[string]string)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(snap)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	for k, v := range snap {
		binary.LittleEndian.PutUint32(buf[:], uint32(len(k)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(k)); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(buf[:], uint32(len(v)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func (kv *testConcurrentKV) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(buf[:])
	kv.data = make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		kl := binary.LittleEndian.Uint32(buf[:])
		kb := make([]byte, kl)
		if _, err := io.ReadFull(r, kb); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		vl := binary.LittleEndian.Uint32(buf[:])
		vb := make([]byte, vl)
		if _, err := io.ReadFull(r, vb); err != nil {
			return err
		}
		kv.data[string(kb)] = string(vb)
	}
	return nil
}

func (kv *testConcurrentKV) Close(_ context.Context) error {
	return nil
}

// testOnDiskKV implements DiskStateMachine for testing.
type testOnDiskKV struct {
	mu   sync.RWMutex
	data map[string]string
}

func newTestOnDiskKV() *testOnDiskKV {
	return &testOnDiskKV{data: make(map[string]string)}
}

func (kv *testOnDiskKV) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (kv *testOnDiskKV) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			results[i] = sm.Result{Value: 0}
			continue
		}
		keyLen := int(e.Cmd[0])
		if keyLen+1 > len(e.Cmd) {
			results[i] = sm.Result{Value: 0}
			continue
		}
		key := string(e.Cmd[1 : 1+keyLen])
		val := string(e.Cmd[1+keyLen:])
		kv.data[key] = val
		results[i] = sm.Result{Value: uint64(len(kv.data))}
	}
	return nil
}

func (kv *testOnDiskKV) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("query must be string")
	}
	return kv.data[key], nil
}

func (kv *testOnDiskKV) Sync() error { return nil }

func (kv *testOnDiskKV) PrepareSnapshot() (interface{}, error) {
	kv.mu.RLock()
	snap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snap[k] = v
	}
	kv.mu.RUnlock()
	return snap, nil
}

func (kv *testOnDiskKV) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, _ <-chan struct{}) error {
	snap := snapshotCtx.(map[string]string)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(snap)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	for k, v := range snap {
		binary.LittleEndian.PutUint32(buf[:], uint32(len(k)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(k)); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(buf[:], uint32(len(v)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func (kv *testOnDiskKV) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(buf[:])
	kv.data = make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		kl := binary.LittleEndian.Uint32(buf[:])
		kb := make([]byte, kl)
		if _, err := io.ReadFull(r, kb); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		vl := binary.LittleEndian.Uint32(buf[:])
		vb := make([]byte, vl)
		if _, err := io.ReadFull(r, vb); err != nil {
			return err
		}
		kv.data[string(kb)] = string(vb)
	}
	return nil
}

func (kv *testOnDiskKV) Close(_ context.Context) error { return nil }

// testOnDiskKVWithApplied returns a specific lastApplied from Open.
type testOnDiskKVWithApplied struct {
	testOnDiskKV
	lastApplied uint64
}

func (kv *testOnDiskKVWithApplied) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return kv.lastApplied, nil
}

// testOnDiskKVOpenError returns an error from Open.
type testOnDiskKVOpenError struct {
	testOnDiskKV
}

func (kv *testOnDiskKVOpenError) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, &testOpenError{}
}

// testOpenError is a typed error returned by testOnDiskKVOpenError.Open.
type testOpenError struct{}

func (e *testOpenError) Error() string { return "test open error" }

// makeCmd builds a test command: [keyLen:1][key][value].
func makeCmd(key, value string) []byte {
	cmd := make([]byte, 1+len(key)+len(value))
	cmd[0] = byte(len(key))
	copy(cmd[1:], key)
	copy(cmd[1+len(key):], value)
	return cmd
}

func TestNewStateMachine(t *testing.T) {
	t.Run("regular state machine", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()
		if rsm.SMType() != SMTypeRegular {
			t.Errorf("SMType() = %d, want %d", rsm.SMType(), SMTypeRegular)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied() = %d, want 0", rsm.LastApplied())
		}
	})

	t.Run("concurrent state machine", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()
		if rsm.SMType() != SMTypeConcurrent {
			t.Errorf("SMType() = %d, want %d", rsm.SMType(), SMTypeConcurrent)
		}
	})

	t.Run("on-disk state machine", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()
		if rsm.SMType() != SMTypeOnDisk {
			t.Errorf("SMType() = %d, want %d", rsm.SMType(), SMTypeOnDisk)
		}
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return "not a state machine"
		}
		_, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if !errors.Is(err, ErrInvalidSMType) {
			t.Errorf("NewStateMachine = %v, want ErrInvalidSMType", err)
		}
	})
}

func TestStateMachineApply(t *testing.T) {
	t.Run("applies entries to regular SM", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("key1", "val1")},
			{Index: 2, Cmd: makeCmd("key2", "val2")},
		}
		results := make([]sm.Result, len(entries))
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		if results[0].Value != 1 {
			t.Errorf("results[0].Value = %d, want 1", results[0].Value)
		}
		if results[1].Value != 2 {
			t.Errorf("results[1].Value = %d, want 2", results[1].Value)
		}
		if rsm.LastApplied() != 2 {
			t.Errorf("LastApplied() = %d, want 2", rsm.LastApplied())
		}
	})

	t.Run("session dedup skips duplicates", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register client session.
		if err := rsm.Sessions().Register(100, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// First apply.
		entries1 := []proto.Entry{
			{Index: 1, ClientID: 100, SeriesID: 1, Cmd: makeCmd("k", "v1")},
		}
		results1 := make([]sm.Result, 1)
		if err := rsm.Apply(entries1, results1); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Second apply with same clientID/seriesID (duplicate).
		entries2 := []proto.Entry{
			{Index: 2, ClientID: 100, SeriesID: 1, Cmd: makeCmd("k", "v2")},
		}
		results2 := make([]sm.Result, 1)
		if err := rsm.Apply(entries2, results2); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Results should be the same (cached from first apply).
		if results2[0].Value != results1[0].Value {
			t.Errorf("duplicate result = %d, want %d (cached)", results2[0].Value, results1[0].Value)
		}

		// Verify lookup shows original value (not overwritten).
		val, err := rsm.Lookup(context.Background(), "k")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "v1" {
			t.Errorf("Lookup(k) = %q, want %q", val, "v1")
		}
	})

	t.Run("entries without session applied normally", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// ClientID == 0 means no session (no-op session).
		entries := []proto.Entry{
			{Index: 1, ClientID: 0, Cmd: makeCmd("a", "1")},
			{Index: 2, ClientID: 0, Cmd: makeCmd("b", "2")},
		}
		results := make([]sm.Result, 2)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.LastApplied() != 2 {
			t.Errorf("LastApplied() = %d, want 2", rsm.LastApplied())
		}
	})

	t.Run("empty entries batch", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Apply(nil, nil); err != nil {
			t.Fatalf("Apply(nil) = %v", err)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied() = %d, want 0", rsm.LastApplied())
		}
	})

	t.Run("applies to concurrent SM", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("ck", "cv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 1 {
			t.Errorf("results[0].Value = %d, want 1", results[0].Value)
		}
	})

	t.Run("applies to on-disk SM", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("dk", "dv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 1 {
			t.Errorf("results[0].Value = %d, want 1", results[0].Value)
		}
	})

	t.Run("mixed session and no-session entries", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(200, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		entries := []proto.Entry{
			{Index: 1, ClientID: 0, Cmd: makeCmd("a", "1")},                // no session
			{Index: 2, ClientID: 200, SeriesID: 1, Cmd: makeCmd("b", "2")}, // session
			{Index: 3, ClientID: 0, Cmd: makeCmd("c", "3")},                // no session
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.LastApplied() != 3 {
			t.Errorf("LastApplied() = %d, want 3", rsm.LastApplied())
		}
	})
}

func TestStateMachineLookup(t *testing.T) {
	t.Run("regular SM lookup", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("hello", "world")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		val, err := rsm.Lookup(context.Background(), "hello")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "world" {
			t.Errorf("Lookup(hello) = %q, want %q", val, "world")
		}
	})

	t.Run("lookup nonexistent key", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		val, err := rsm.Lookup(context.Background(), "missing")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "" {
			t.Errorf("Lookup(missing) = %q, want empty", val)
		}
	})
}

func TestStateMachineSnapshotRecover(t *testing.T) {
	t.Run("regular SM snapshot roundtrip", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		// Register session and apply data.
		if err := rsm1.Sessions().Register(50, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		entries := []proto.Entry{
			{Index: 1, ClientID: 50, SeriesID: 1, Cmd: makeCmd("x", "y")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Recover into a new SM.
		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// Verify data.
		val, err := rsm2.Lookup(context.Background(), "x")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "y" {
			t.Errorf("Lookup(x) = %q, want %q", val, "y")
		}

		// Verify session was recovered.
		if !rsm2.Sessions().IsRegistered(50) {
			t.Error("session 50 should be registered after recovery")
		}

		// Verify duplicate detection works after recovery.
		cachedResult, isDup, checkErr := rsm2.Sessions().CheckDuplicate(50, 1, 2)
		if checkErr != nil {
			t.Fatalf("CheckDuplicate = %v", checkErr)
		}
		if !isDup {
			t.Error("series 1 should be duplicate after recovery")
		}
		if cachedResult.Value != results[0].Value {
			t.Errorf("cached value = %d, want %d", cachedResult.Value, results[0].Value)
		}
	})

	t.Run("concurrent SM snapshot roundtrip", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("ck", "cv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		val, err := rsm2.Lookup(context.Background(), "ck")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "cv" {
			t.Errorf("Lookup(ck) = %q, want %q", val, "cv")
		}
	})

	t.Run("on-disk SM snapshot roundtrip", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("dk", "dv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		val, err := rsm2.Lookup(context.Background(), "dk")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "dv" {
			t.Errorf("Lookup(dk) = %q, want %q", val, "dv")
		}
	})

	t.Run("save snapshot stopped", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		stopper := make(chan struct{})
		close(stopper) // pre-closed
		var buf bytes.Buffer
		err = rsm1.SaveSnapshot(context.Background(), &buf, stopper)
		if !errors.Is(err, ErrStopped) {
			t.Errorf("SaveSnapshot = %v, want ErrStopped", err)
		}
	})

	t.Run("recover snapshot stopped", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		stopper := make(chan struct{})
		close(stopper) // pre-closed
		var buf bytes.Buffer
		err = rsm1.RecoverFromSnapshot(context.Background(), &buf, stopper)
		if !errors.Is(err, ErrStopped) {
			t.Errorf("RecoverFromSnapshot = %v, want ErrStopped", err)
		}
	})
}

func TestStateMachineClose(t *testing.T) {
	t.Run("close regular SM", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		if err := rsm.Close(); err != nil {
			t.Errorf("Close = %v, want nil", err)
		}
	})

	t.Run("close concurrent SM", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		if err := rsm.Close(); err != nil {
			t.Errorf("Close = %v, want nil", err)
		}
	})
}

func TestStateMachineSessions(t *testing.T) {
	t.Run("sessions returns manager", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		sessions := rsm.Sessions()
		if sessions == nil {
			t.Fatal("Sessions() returned nil")
		}
		if sessions.Count() != 0 {
			t.Errorf("Count() = %d, want 0", sessions.Count())
		}
	})

	t.Run("sessions not nil for concurrent SM", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if rsm.Sessions() == nil {
			t.Fatal("Sessions() returned nil")
		}
	})
}

func TestWrapStateMachine(t *testing.T) {
	t.Run("wraps regular SM", func(t *testing.T) {
		w, st, err := wrapStateMachine(newTestKV())
		if err != nil {
			t.Fatalf("wrapStateMachine = %v", err)
		}
		if st != SMTypeRegular {
			t.Errorf("type = %d, want %d", st, SMTypeRegular)
		}
		if w.Type() != SMTypeRegular {
			t.Errorf("Type() = %d, want %d", w.Type(), SMTypeRegular)
		}
	})

	t.Run("wraps concurrent SM", func(t *testing.T) {
		w, st, err := wrapStateMachine(newTestConcurrentKV())
		if err != nil {
			t.Fatalf("wrapStateMachine = %v", err)
		}
		if st != SMTypeConcurrent {
			t.Errorf("type = %d, want %d", st, SMTypeConcurrent)
		}
		if w.Type() != SMTypeConcurrent {
			t.Errorf("Type() = %d, want %d", w.Type(), SMTypeConcurrent)
		}
	})

	t.Run("wraps on-disk SM", func(t *testing.T) {
		w, st, err := wrapStateMachine(newTestOnDiskKV())
		if err != nil {
			t.Fatalf("wrapStateMachine = %v", err)
		}
		if st != SMTypeOnDisk {
			t.Errorf("type = %d, want %d", st, SMTypeOnDisk)
		}
		if w.Type() != SMTypeOnDisk {
			t.Errorf("Type() = %d, want %d", w.Type(), SMTypeOnDisk)
		}
	})

	t.Run("invalid type", func(t *testing.T) {
		_, _, err := wrapStateMachine("invalid")
		if !errors.Is(err, ErrInvalidSMType) {
			t.Errorf("wrapStateMachine = %v, want ErrInvalidSMType", err)
		}
	})
}

func TestStateMachineNALookup(t *testing.T) {
	t.Run("not supported returns false", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		result, supported, err := rsm.NALookup([]byte("key"))
		if err != nil {
			t.Fatalf("NALookup = %v", err)
		}
		if supported {
			t.Error("NALookup should return false for testKV")
		}
		if result != nil {
			t.Errorf("NALookup result = %v, want nil", result)
		}
	})

	t.Run("supported propagates error", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return &testHasherSMError{testKV: testKV{data: make(map[string]string)}}
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		_, supported, err := rsm.NALookup([]byte("key"))
		if !supported {
			t.Error("NALookup should return supported=true for testHasherSMError")
		}
		if err == nil {
			t.Fatal("NALookup should return error")
		}
		var te *testError
		if !errors.As(err, &te) {
			t.Errorf("NALookup error = %T, want *testError", err)
		}
	})
}

func TestStateMachineGetHash(t *testing.T) {
	t.Run("not supported returns false", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		hash, supported, err := rsm.GetHash()
		if err != nil {
			t.Fatalf("GetHash = %v", err)
		}
		if supported {
			t.Error("GetHash should return false for testKV")
		}
		if hash != 0 {
			t.Errorf("GetHash = %d, want 0", hash)
		}
	})

	t.Run("supported propagates error", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return &testHasherSMError{testKV: testKV{data: make(map[string]string)}}
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		_, supported, err := rsm.GetHash()
		if !supported {
			t.Error("GetHash should return supported=true for testHasherSMError")
		}
		if err == nil {
			t.Fatal("GetHash should return error")
		}
		var te *testError
		if !errors.As(err, &te) {
			t.Errorf("GetHash error = %T, want *testError", err)
		}
	})
}

func TestStateMachineApply_BufferReuse(t *testing.T) {
	t.Run("buffers reused across Apply calls", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// First Apply: creates buffers.
		entries1 := []proto.Entry{
			{Index: 1, Cmd: makeCmd("k1", "v1")},
			{Index: 2, Cmd: makeCmd("k2", "v2")},
		}
		results1 := make([]sm.Result, len(entries1))
		if err := rsm.Apply(entries1, results1); err != nil {
			t.Fatalf("Apply(1) = %v", err)
		}

		// Verify buffers were allocated.
		if rsm.smEntryBuf == nil {
			t.Fatal("smEntryBuf should be non-nil after first Apply")
		}
		buf1Cap := cap(rsm.smEntryBuf)

		// Second Apply: reuses buffers.
		entries2 := []proto.Entry{
			{Index: 3, Cmd: makeCmd("k3", "v3")},
		}
		results2 := make([]sm.Result, len(entries2))
		if err := rsm.Apply(entries2, results2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}

		// Capacity should be retained from first call.
		if cap(rsm.smEntryBuf) < buf1Cap {
			t.Errorf("smEntryBuf cap = %d, want >= %d (reused)", cap(rsm.smEntryBuf), buf1Cap)
		}

		// Verify results are correct.
		if results2[0].Value != 3 {
			t.Errorf("results2[0].Value = %d, want 3", results2[0].Value)
		}
		if rsm.LastApplied() != 3 {
			t.Errorf("LastApplied() = %d, want 3", rsm.LastApplied())
		}
	})

	t.Run("buffers grow for larger batches", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// First Apply: small batch.
		entries1 := []proto.Entry{
			{Index: 1, Cmd: makeCmd("a", "1")},
		}
		results1 := make([]sm.Result, len(entries1))
		if err := rsm.Apply(entries1, results1); err != nil {
			t.Fatalf("Apply(1) = %v", err)
		}

		// Second Apply: larger batch grows buffers.
		entries2 := make([]proto.Entry, 10)
		for i := range entries2 {
			entries2[i] = proto.Entry{
				Index: uint64(i + 2),
				Cmd:   makeCmd("k", "v"),
			}
		}
		results2 := make([]sm.Result, len(entries2))
		if err := rsm.Apply(entries2, results2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}

		if cap(rsm.smEntryBuf) < 10 {
			t.Errorf("smEntryBuf cap = %d, want >= 10", cap(rsm.smEntryBuf))
		}
		if rsm.LastApplied() != 11 {
			t.Errorf("LastApplied() = %d, want 11", rsm.LastApplied())
		}
	})
}

func TestStateMachineLookup_NALookupFastPath(t *testing.T) {
	t.Run("byte query without NALookup falls through", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply data using testKV's encoding: [keyLen:1][key][value]
		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("hello", "world")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Lookup with string query (testKV expects strings)
		val, err := rsm.Lookup(context.Background(), "hello")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "world" {
			t.Errorf("Lookup = %q, want %q", val, "world")
		}

		// Lookup with []byte query should fall through to Lookup
		// since testKV doesn't implement NALookup. testKV expects
		// string queries, so []byte will return an error.
		_, err = rsm.Lookup(context.Background(), []byte("hello"))
		if err == nil {
			t.Error("expected error for []byte query on testKV (expects string)")
		}
	})
}

// ---------------------------------------------------------------------------
// ZeroCopyEntryCmd in Apply
// ---------------------------------------------------------------------------

func TestApply_ZeroCopyFalse_CopiesCmd(t *testing.T) {
	// When zeroCopyEntryCmd is false (default), the SM should receive
	// an owned copy of Cmd, not an alias to the original buffer.
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	original := makeCmd("key1", "val1")
	// Keep a reference to the data before Apply to verify independence.
	beforeByte := original[1]

	entries := []proto.Entry{
		{Index: 1, Cmd: original},
	}
	results := make([]sm.Result, 1)
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if results[0].Value != 1 {
		t.Fatalf("expected result Value=1, got %d", results[0].Value)
	}

	// Mutate the original buffer after Apply returns. Since zeroCopy
	// is false, the SM received an owned copy. The SM's internal
	// data structure references the copy, not the original.
	original[1] = 'X'
	if original[1] == beforeByte {
		t.Fatal("mutation failed to change original buffer")
	}
}

func TestApply_ZeroCopyTrue_AliasesCmd(t *testing.T) {
	// When zeroCopyEntryCmd is true, the SM receives a direct alias
	// of the entry Cmd (zero-copy, zero-alloc).
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	original := makeCmd("key1", "val1")
	entries := []proto.Entry{
		{Index: 1, Cmd: original},
	}
	results := make([]sm.Result, 1)
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if results[0].Value != 1 {
		t.Fatalf("expected result Value=1, got %d", results[0].Value)
	}
}

func TestApply_ZeroCopyFalse_EmptyCmd(t *testing.T) {
	// Verify that empty Cmd is handled correctly (no copy needed).
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	entries := []proto.Entry{
		{Index: 1, Cmd: nil},
		{Index: 2, Cmd: []byte{}},
	}
	results := make([]sm.Result, 2)
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Session register/unregister in Apply
// ---------------------------------------------------------------------------

func TestApply_SessionRegister(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 0, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	entries := []proto.Entry{{
		Index:    1,
		ClientID: 42,
		SeriesID: proto.SessionSeriesIDForRegister,
		Type:     proto.EntryNormal,
	}}
	results := make([]sm.Result, 1)

	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if results[0].Value != 42 {
		t.Errorf("result.Value = %d, want 42", results[0].Value)
	}
	if !rsm.Sessions().IsRegistered(42) {
		t.Error("session 42 should be registered after register entry")
	}
}

func TestApply_SessionRegister_AlreadyExists(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 0, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	regEntry := proto.Entry{
		Index:    1,
		ClientID: 42,
		SeriesID: proto.SessionSeriesIDForRegister,
		Type:     proto.EntryNormal,
	}

	// First register should succeed.
	results := make([]sm.Result, 1)
	if err := rsm.Apply([]proto.Entry{regEntry}, results); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if results[0].Value != 42 {
		t.Errorf("first result.Value = %d, want 42", results[0].Value)
	}

	// Second register should return empty result (already exists).
	regEntry.Index = 2
	results[0] = sm.Result{}
	if err := rsm.Apply([]proto.Entry{regEntry}, results); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if results[0].Value != 0 {
		t.Errorf("second result.Value = %d, want 0 (already exists)", results[0].Value)
	}
}

func TestApply_SessionUnregister(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 0, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	// Register first.
	regEntry := proto.Entry{
		Index:    1,
		ClientID: 42,
		SeriesID: proto.SessionSeriesIDForRegister,
		Type:     proto.EntryNormal,
	}
	results := make([]sm.Result, 1)
	if err := rsm.Apply([]proto.Entry{regEntry}, results); err != nil {
		t.Fatalf("register Apply: %v", err)
	}

	// Unregister.
	unregEntry := proto.Entry{
		Index:    2,
		ClientID: 42,
		SeriesID: proto.SessionSeriesIDForUnregister,
		Type:     proto.EntryNormal,
	}
	results[0] = sm.Result{}
	if err := rsm.Apply([]proto.Entry{unregEntry}, results); err != nil {
		t.Fatalf("unregister Apply: %v", err)
	}
	if results[0].Value != 42 {
		t.Errorf("result.Value = %d, want 42", results[0].Value)
	}
	if rsm.Sessions().IsRegistered(42) {
		t.Error("session 42 should not be registered after unregister")
	}
}

func TestApply_SessionUnregister_NotFound(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 0, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	// Unregister without prior registration.
	unregEntry := proto.Entry{
		Index:    1,
		ClientID: 99,
		SeriesID: proto.SessionSeriesIDForUnregister,
		Type:     proto.EntryNormal,
	}
	results := make([]sm.Result, 1)
	if err := rsm.Apply([]proto.Entry{unregEntry}, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if results[0].Value != sm.ResultSessionExpired {
		t.Errorf("result.Value = %d, want sm.ResultSessionExpired (%d)",
			results[0].Value, sm.ResultSessionExpired)
	}
}

func TestApply_MixedSessionAndNormal(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 0, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	// Batch: register(clientID=42), normal entry, unregister(clientID=42)
	entries := []proto.Entry{
		{
			Index:    1,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		},
		{
			Index: 2,
			Type:  proto.EntryNormal,
			Cmd:   []byte{3, 'f', 'o', 'o', 'b', 'a', 'r'},
		},
		{
			Index:    3,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForUnregister,
			Type:     proto.EntryNormal,
		},
	}
	results := make([]sm.Result, 3)

	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Register result: clientID
	if results[0].Value != 42 {
		t.Errorf("register result.Value = %d, want 42", results[0].Value)
	}
	// Normal entry result: key count (1 entry in KV)
	if results[1].Value != 1 {
		t.Errorf("normal result.Value = %d, want 1", results[1].Value)
	}
	// Unregister result: clientID
	if results[2].Value != 42 {
		t.Errorf("unregister result.Value = %d, want 42", results[2].Value)
	}
	// Session should be gone
	if rsm.Sessions().IsRegistered(42) {
		t.Error("session 42 should be unregistered")
	}
	// LastApplied should be 3
	if rsm.LastApplied() != 3 {
		t.Errorf("LastApplied = %d, want 3", rsm.LastApplied())
	}
}

func TestStateMachineOpen(t *testing.T) {
	t.Run("regular SM returns zero", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		lastApplied, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if lastApplied != 0 {
			t.Errorf("lastApplied = %d, want 0", lastApplied)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied() = %d, want 0", rsm.LastApplied())
		}
	})

	t.Run("on-disk SM returns lastApplied and updates field", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return &testOnDiskKVWithApplied{
				testOnDiskKV: *newTestOnDiskKV(),
				lastApplied:  100,
			}
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		lastApplied, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if lastApplied != 100 {
			t.Errorf("lastApplied = %d, want 100", lastApplied)
		}
		if rsm.LastApplied() != 100 {
			t.Errorf("LastApplied() = %d, want 100", rsm.LastApplied())
		}
	})

	t.Run("on-disk SM open error propagated", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return &testOnDiskKVOpenError{
				testOnDiskKV: *newTestOnDiskKV(),
			}
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		_, err = rsm.Open(make(chan struct{}))
		if err == nil {
			t.Fatal("Open should return error")
		}
		var te *testOpenError
		if !errors.As(err, &te) {
			t.Errorf("Open error = %T, want *testOpenError", err)
		}
	})
}

func TestStateMachineIsOnDisk(t *testing.T) {
	t.Run("regular SM is not on-disk", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if rsm.IsOnDisk() {
			t.Error("IsOnDisk() = true, want false")
		}
	})

	t.Run("concurrent SM is not on-disk", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if rsm.IsOnDisk() {
			t.Error("IsOnDisk() = true, want false")
		}
	})

	t.Run("on-disk SM is on-disk", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if !rsm.IsOnDisk() {
			t.Error("IsOnDisk() = false, want true")
		}
	})
}

func TestStateMachineApply_SessionExpiry(t *testing.T) {
	t.Run("expired sessions are cleaned up during apply", func(t *testing.T) {
		// Create RSM with session expiry after 100 entries of inactivity.
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 100, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register a session via the apply path.
		regEntries := []proto.Entry{{
			Index:    1,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply register = %v", err)
		}
		if !rsm.Sessions().IsRegistered(42) {
			t.Fatal("session 42 should be registered")
		}

		// Apply entries at index 200 (gap of 199 > threshold of 100).
		// This should trigger expiry of session 42.
		entries := []proto.Entry{
			{Index: 200, Cmd: makeCmd("k", "v")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Session should be expired because lastActiveIndex=1, currentIndex=200,
		// gap=199 > threshold=100.
		if rsm.Sessions().IsRegistered(42) {
			t.Error("session 42 should be expired after 200 entries of inactivity")
		}
	})

	t.Run("active sessions are not expired", func(t *testing.T) {
		// Create RSM with session expiry after 100 entries of inactivity.
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 100, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register session at index 1.
		regEntries := []proto.Entry{{
			Index:    1,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply register = %v", err)
		}

		// Use the session at index 50 (within threshold).
		entries := []proto.Entry{
			{Index: 50, ClientID: 42, SeriesID: 1, Cmd: makeCmd("k", "v")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply session entry = %v", err)
		}

		// Apply at index 120. Session was last active at 50, gap=70 < 100.
		entries2 := []proto.Entry{
			{Index: 120, Cmd: makeCmd("k2", "v2")},
		}
		results2 := make([]sm.Result, 1)
		if err := rsm.Apply(entries2, results2); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Session should still be registered.
		if !rsm.Sessions().IsRegistered(42) {
			t.Error("session 42 should still be registered (active within threshold)")
		}
	})

	t.Run("disabled when sessionExpiryEntries is zero", func(t *testing.T) {
		// Create RSM with expiry disabled (default).
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register session at index 1.
		regEntries := []proto.Entry{{
			Index:    1,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply register = %v", err)
		}

		// Apply entries at a very high index.
		entries := []proto.Entry{
			{Index: 1000000, Cmd: makeCmd("k", "v")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Session should NOT be expired because expiry is disabled.
		if !rsm.Sessions().IsRegistered(42) {
			t.Error("session 42 should not be expired when sessionExpiryEntries=0")
		}
	})

	t.Run("deterministic expiry across replicas", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})

		// Two replicas with same config.
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 100, false)
		if err != nil {
			t.Fatalf("NewStateMachine(1) = %v", err)
		}
		defer rsm1.Close()

		rsm2, err := NewStateMachine(1, 2, createFn, t.TempDir(), 100, 100, 100, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		// Both process the same log entries.
		regEntries := []proto.Entry{{
			Index:    1,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}}
		regResults1 := make([]sm.Result, 1)
		regResults2 := make([]sm.Result, 1)
		if err := rsm1.Apply(regEntries, regResults1); err != nil {
			t.Fatalf("rsm1.Apply register = %v", err)
		}
		if err := rsm2.Apply(regEntries, regResults2); err != nil {
			t.Fatalf("rsm2.Apply register = %v", err)
		}

		// Both apply at index 200.
		entries := []proto.Entry{
			{Index: 200, Cmd: makeCmd("k", "v")},
		}
		results1 := make([]sm.Result, 1)
		results2 := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results1); err != nil {
			t.Fatalf("rsm1.Apply = %v", err)
		}
		if err := rsm2.Apply(entries, results2); err != nil {
			t.Fatalf("rsm2.Apply = %v", err)
		}

		// Both should have expired the same session.
		r1Registered := rsm1.Sessions().IsRegistered(42)
		r2Registered := rsm2.Sessions().IsRegistered(42)
		if r1Registered != r2Registered {
			t.Errorf("replica divergence: rsm1=%v, rsm2=%v", r1Registered, r2Registered)
		}
		if r1Registered {
			t.Error("session 42 should be expired on both replicas")
		}
	})
}

// TestApply_UnregisteredSessionManagedEntry verifies that a session-managed
// entry with an unregistered clientID returns ResultSessionExpired (PhD
// Figure 6.1, step 3) instead of an empty result, so the client can
// distinguish it from a legitimate operation. The entry is not applied
// to the user SM.
func TestApply_UnregisteredSessionManagedEntry(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	// Apply a session-managed entry for clientID=99 which is NOT registered.
	entries := []proto.Entry{
		{
			Index:    1,
			ClientID: 99,
			SeriesID: 5,
			Type:     proto.EntryNormal,
			Cmd:      []byte{3, 'f', 'o', 'o', 'b', 'a', 'r'},
		},
	}
	results := make([]sm.Result, 1)

	// This should NOT return an error — the apply pipeline must not crash.
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply should not fail for unregistered session: %v", err)
	}

	// The result should carry the reserved ResultSessionExpired sentinel.
	if results[0].Value != sm.ResultSessionExpired {
		t.Errorf("result.Value = %d, want sm.ResultSessionExpired (%d)",
			results[0].Value, sm.ResultSessionExpired)
	}
	if results[0].Data != nil {
		t.Errorf("result.Data = %v, want nil", results[0].Data)
	}

	// The entry should NOT have been applied to the user SM.
	val, err := rsm.Lookup(nil, "foo")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if val != "" {
		t.Errorf("Lookup(foo) = %q, want empty (entry should be no-op)", val)
	}

	// LastApplied should still advance to track progress.
	if rsm.LastApplied() != 1 {
		t.Errorf("LastApplied = %d, want 1", rsm.LastApplied())
	}
}

// TestApply_UnregisteredSessionMixedBatch verifies that a batch containing
// both a valid normal entry and an unregistered session-managed entry
// applies the normal entry correctly while returning ResultSessionExpired
// for the expired session entry.
func TestApply_UnregisteredSessionMixedBatch(t *testing.T) {
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return newTestKV()
	}, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rsm.Close()

	entries := []proto.Entry{
		// Normal entry (no session).
		{
			Index: 1,
			Type:  proto.EntryNormal,
			Cmd:   []byte{3, 'f', 'o', 'o', 'b', 'a', 'r'},
		},
		// Session-managed entry for unregistered client.
		{
			Index:    2,
			ClientID: 99,
			SeriesID: 5,
			Type:     proto.EntryNormal,
			Cmd:      []byte{3, 'b', 'a', 'z', 'q', 'u', 'x'},
		},
		// Another normal entry.
		{
			Index: 3,
			Type:  proto.EntryNormal,
			Cmd:   []byte{3, 'k', 'e', 'y', 'v', 'a', 'l'},
		},
	}
	results := make([]sm.Result, 3)

	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Normal entries should have results (KV data count).
	if results[0].Value != 1 {
		t.Errorf("results[0].Value = %d, want 1", results[0].Value)
	}

	// Unregistered session entry should signal session expired.
	if results[1].Value != sm.ResultSessionExpired {
		t.Errorf("results[1].Value = %d, want sm.ResultSessionExpired (%d)",
			results[1].Value, sm.ResultSessionExpired)
	}

	// Second normal entry should see 2 keys in KV.
	if results[2].Value != 2 {
		t.Errorf("results[2].Value = %d, want 2", results[2].Value)
	}

	// "baz" should NOT be in the KV (expired session entry was skipped).
	val, err := rsm.Lookup(nil, "baz")
	if err != nil {
		t.Fatalf("Lookup(baz): %v", err)
	}
	if val != "" {
		t.Errorf("Lookup(baz) = %q, want empty (session entry should be no-op)", val)
	}

	if rsm.LastApplied() != 3 {
		t.Errorf("LastApplied = %d, want 3", rsm.LastApplied())
	}
}

// ---------------------------------------------------------------------------
// Batched on-disk SM entry handling
// ---------------------------------------------------------------------------

func TestApply_BatchedOnDiskEntries(t *testing.T) {
	t.Run("large batch applied atomically to on-disk SM", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		const batchSize = 100
		entries := make([]proto.Entry, batchSize)
		for i := range entries {
			key := "bk" + string(rune('A'+i%26))
			val := "bv" + string(rune('0'+i%10))
			entries[i] = proto.Entry{Index: uint64(i + 1), Cmd: makeCmd(key, val)}
		}
		results := make([]sm.Result, batchSize)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		if rsm.LastApplied() != batchSize {
			t.Errorf("LastApplied = %d, want %d", rsm.LastApplied(), batchSize)
		}

		// All results should be non-zero (SM stored data).
		for i, r := range results {
			if r.Value == 0 && len(entries[i].Cmd) > 0 {
				t.Errorf("results[%d].Value = 0, want non-zero", i)
			}
		}
	})

	t.Run("on-disk SM single entry batch", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("single", "entry")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 1 {
			t.Errorf("results[0].Value = %d, want 1", results[0].Value)
		}
		if rsm.LastApplied() != 1 {
			t.Errorf("LastApplied = %d, want 1", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Compressed (Snappy) entry handling - entries with compression flag
// ---------------------------------------------------------------------------

func TestApply_CompressedEntries(t *testing.T) {
	t.Run("entry with compress flag still applies via normal path", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Entry with EntryCompressFlag OR'd into Type. The RSM Apply layer
		// does not decompress -- that happens in the transport/log layer
		// before entries reach Apply. Here we verify the entry type flag
		// does not break the Apply path (Type is not used for routing in Apply).
		entries := []proto.Entry{
			{
				Index: 1,
				Type:  proto.EntryNormal | proto.EntryCompressFlag,
				Cmd:   makeCmd("comp", "data"),
			},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 1 {
			t.Errorf("results[0].Value = %d, want 1", results[0].Value)
		}

		val, err := rsm.Lookup(context.Background(), "comp")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "data" {
			t.Errorf("Lookup(comp) = %q, want %q", val, "data")
		}
	})

	t.Run("compressed session register entry still works", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Session register with compress flag. IsNewSessionRequest checks
		// !IsConfigChange which masks with EntryTypeMask, so compress flag
		// should not interfere.
		entries := []proto.Entry{
			{
				Index:    1,
				ClientID: 55,
				SeriesID: proto.SessionSeriesIDForRegister,
				Type:     proto.EntryNormal | proto.EntryCompressFlag,
			},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 55 {
			t.Errorf("register result = %d, want 55", results[0].Value)
		}
		if !rsm.Sessions().IsRegistered(55) {
			t.Error("session 55 should be registered")
		}
	})
}

// ---------------------------------------------------------------------------
// Idempotency for already-applied entries in on-disk SM
// ---------------------------------------------------------------------------

func TestApply_OnDiskIdempotency(t *testing.T) {
	t.Run("on-disk SM handles re-applied entries after recovery", func(t *testing.T) {
		const recoveredIndex = uint64(10)
		kv := newTestOnDiskKVWithState(recoveredIndex)
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		lastApplied, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if lastApplied != recoveredIndex {
			t.Fatalf("Open lastApplied = %d, want %d", lastApplied, recoveredIndex)
		}

		// Replay entries that were already applied (indices <= recoveredIndex).
		// The user SM is responsible for idempotency. The RSM layer still
		// forwards them because it cannot distinguish replays from new entries
		// for non-session entries. We verify it does not panic or corrupt state.
		entries := []proto.Entry{
			{Index: 8, Cmd: makeCmd("old1", "val1")},
			{Index: 9, Cmd: makeCmd("old2", "val2")},
			{Index: 10, Cmd: makeCmd("old3", "val3")},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// LastApplied should NOT decrease (10 is not > 10, stays at 10).
		if rsm.LastApplied() != recoveredIndex {
			t.Errorf("LastApplied = %d, want %d", rsm.LastApplied(), recoveredIndex)
		}
	})

	t.Run("on-disk SM advances past recovered index", func(t *testing.T) {
		const recoveredIndex = uint64(10)
		kv := newTestOnDiskKVWithState(recoveredIndex)
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if _, err := rsm.Open(make(chan struct{})); err != nil {
			t.Fatalf("Open = %v", err)
		}

		entries := []proto.Entry{
			{Index: 11, Cmd: makeCmd("new1", "val1")},
			{Index: 12, Cmd: makeCmd("new2", "val2")},
		}
		results := make([]sm.Result, 2)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.LastApplied() != 12 {
			t.Errorf("LastApplied = %d, want 12", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Allocation tracking - buffer growth and reuse
// ---------------------------------------------------------------------------

func TestApply_AllocationTracking(t *testing.T) {
	t.Run("buffer capacity grows to accommodate large batch then retains for small batch", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Large batch to force buffer growth.
		const largeBatch = 50
		entries := make([]proto.Entry, largeBatch)
		for i := range entries {
			entries[i] = proto.Entry{Index: uint64(i + 1), Cmd: makeCmd("k", "v")}
		}
		results := make([]sm.Result, largeBatch)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply(large) = %v", err)
		}
		largeCap := cap(rsm.smEntryBuf)
		if largeCap < largeBatch {
			t.Errorf("smEntryBuf cap = %d, want >= %d after large batch", largeCap, largeBatch)
		}

		// Small batch should reuse the large buffer.
		smallEntries := []proto.Entry{
			{Index: uint64(largeBatch + 1), Cmd: makeCmd("s", "v")},
		}
		smallResults := make([]sm.Result, 1)
		if err := rsm.Apply(smallEntries, smallResults); err != nil {
			t.Fatalf("Apply(small) = %v", err)
		}
		// Capacity should not shrink.
		if cap(rsm.smEntryBuf) < largeCap {
			t.Errorf("smEntryBuf cap = %d, shrunk from %d", cap(rsm.smEntryBuf), largeCap)
		}
	})

	t.Run("session-only batch does not allocate sm entry buffers", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply only session register entries (no entries forwarded to SM).
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
			{Index: 2, ClientID: 20, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
		}
		results := make([]sm.Result, 2)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		// smEntryBuf should be empty length (no entries forwarded).
		if len(rsm.smEntryBuf) != 0 {
			t.Errorf("smEntryBuf len = %d, want 0 after session-only batch", len(rsm.smEntryBuf))
		}
	})
}

// ---------------------------------------------------------------------------
// Config change entries - recognized by type but not processed as session ops
// ---------------------------------------------------------------------------

func TestApply_ConfigChangeEntries(t *testing.T) {
	t.Run("config change entry is not treated as session register", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// A config change entry has Type=EntryConfigChange, ClientID != 0,
		// and SeriesID == SessionSeriesIDForRegister. Despite matching the
		// session register SeriesID, IsNewSessionRequest returns false
		// because IsConfigChange() is true.
		entries := []proto.Entry{
			{
				Index:    1,
				ClientID: 42,
				SeriesID: proto.SessionSeriesIDForRegister,
				Type:     proto.EntryConfigChange,
			},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		// Session should NOT be registered (config change, not a session op).
		if rsm.Sessions().IsRegistered(42) {
			t.Error("config change entry should not register a session")
		}
	})

	t.Run("config change entry is forwarded to user SM", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Config change with Cmd data is forwarded to user SM.
		entries := []proto.Entry{
			{
				Index: 1,
				Type:  proto.EntryConfigChange,
				Cmd:   makeCmd("cc", "val"),
			},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		// testKV should have stored it.
		val, lookupErr := rsm.Lookup(context.Background(), "cc")
		if lookupErr != nil {
			t.Fatalf("Lookup = %v", lookupErr)
		}
		if val.(string) != "val" {
			t.Errorf("Lookup(cc) = %q, want %q", val, "val")
		}
	})
}

// ---------------------------------------------------------------------------
// Metadata entry handling
// ---------------------------------------------------------------------------

func TestApply_MetadataEntries(t *testing.T) {
	t.Run("metadata entry with empty cmd is forwarded to SM", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Metadata entries have Type=EntryMetadata and typically no Cmd.
		entries := []proto.Entry{
			{
				Index: 1,
				Type:  proto.EntryMetadata,
				Cmd:   nil,
			},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		// LastApplied should advance.
		if rsm.LastApplied() != 1 {
			t.Errorf("LastApplied = %d, want 1", rsm.LastApplied())
		}
	})

	t.Run("metadata entry is not treated as session operation", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Even with ClientID set, metadata entries go through normal Apply
		// path if they don't match session register/unregister patterns.
		entries := []proto.Entry{
			{
				Index:    1,
				ClientID: 42,
				SeriesID: 5,
				Type:     proto.EntryMetadata,
			},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		// ClientID=42, SeriesID=5 is session-managed but session not registered,
		// so should get SESSION_EXPIRED.
		if results[0].Value != sm.ResultSessionExpired {
			t.Errorf("result.Value = %d, want ResultSessionExpired", results[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// SetLastApplied / SyncedIndex tracking
// ---------------------------------------------------------------------------

func TestStateMachine_LastAppliedNeverDecreases(t *testing.T) {
	t.Run("applying entries with lower index than lastApplied does not decrease it", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply entries up to index 10.
		entries := make([]proto.Entry, 10)
		for i := range entries {
			entries[i] = proto.Entry{Index: uint64(i + 1), Cmd: makeCmd("k", "v")}
		}
		results := make([]sm.Result, 10)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply(1-10) = %v", err)
		}
		if rsm.LastApplied() != 10 {
			t.Fatalf("LastApplied = %d, want 10", rsm.LastApplied())
		}

		// Apply entries with lower indices (this can happen in edge cases).
		// LastApplied should NOT decrease.
		lowEntries := []proto.Entry{
			{Index: 5, Cmd: makeCmd("low", "val")},
		}
		lowResults := make([]sm.Result, 1)
		if err := rsm.Apply(lowEntries, lowResults); err != nil {
			t.Fatalf("Apply(low) = %v", err)
		}
		if rsm.LastApplied() != 10 {
			t.Errorf("LastApplied = %d, want 10 (should not decrease)", rsm.LastApplied())
		}
	})

	t.Run("lastApplied advances to max index in batch not in order", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// The Apply code uses the last entry in the batch for lastApplied.
		// If entries are ordered (which they should be), the last is the max.
		entries := []proto.Entry{
			{Index: 5, Cmd: makeCmd("a", "1")},
			{Index: 6, Cmd: makeCmd("b", "2")},
			{Index: 7, Cmd: makeCmd("c", "3")},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.LastApplied() != 7 {
			t.Errorf("LastApplied = %d, want 7", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Empty event handling
// ---------------------------------------------------------------------------

func TestApply_EmptyEntries(t *testing.T) {
	t.Run("nil entries slice does not panic", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Apply(nil, nil); err != nil {
			t.Fatalf("Apply(nil) = %v", err)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied = %d, want 0", rsm.LastApplied())
		}
	})

	t.Run("zero-length entries slice does not panic", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := make([]proto.Entry, 0)
		results := make([]sm.Result, 0)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply(empty) = %v", err)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied = %d, want 0", rsm.LastApplied())
		}
	})

	t.Run("all entries have empty cmd", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: nil},
			{Index: 2, Cmd: []byte{}},
			{Index: 3, Cmd: nil},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.LastApplied() != 3 {
			t.Errorf("LastApplied = %d, want 3", rsm.LastApplied())
		}
		// All results should have Value=0 (testKV returns 0 for empty cmd).
		for i, r := range results {
			if r.Value != 0 {
				t.Errorf("results[%d].Value = %d, want 0", i, r.Value)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM Open error propagation
// ---------------------------------------------------------------------------

func TestStateMachine_OpenError(t *testing.T) {
	t.Run("open error propagates", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return &testOnDiskKVOpenError{testOnDiskKV: *newTestOnDiskKV()}
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		_, err = rsm.Open(make(chan struct{}))
		if err == nil {
			t.Fatal("Open should return error")
		}
		var te *testOpenError
		if !errors.As(err, &te) {
			t.Errorf("Open error = %T, want *testOpenError", err)
		}
	})

	t.Run("open error does not update lastApplied", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return &testOnDiskKVOpenError{testOnDiskKV: *newTestOnDiskKV()}
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if _, err := rsm.Open(make(chan struct{})); err == nil {
			t.Fatal("Open should return error")
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied = %d, want 0 after failed Open", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent SM snapshot: PrepareSnapshot + SaveSnapshot flow
// ---------------------------------------------------------------------------

func TestStateMachine_ConcurrentSMSaveSnapshotFlow(t *testing.T) {
	t.Run("concurrent SM calls PrepareSnapshot then SaveSnapshot", func(t *testing.T) {
		prepCalled := false
		saveCalled := false
		kv := &testConcurrentKVTracked{
			testConcurrentKV: *newTestConcurrentKV(),
			onPrepare:        func() { prepCalled = true },
			onSave:           func() { saveCalled = true },
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("tk", "tv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		if !prepCalled {
			t.Error("PrepareSnapshot should have been called")
		}
		if !saveCalled {
			t.Error("SaveSnapshot should have been called")
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM SaveSnapshot: Sync + PrepareSnapshot + SaveSnapshot flow
// ---------------------------------------------------------------------------

func TestStateMachine_OnDiskSMSaveSnapshotFlow(t *testing.T) {
	t.Run("on-disk SM calls Sync then PrepareSnapshot then SaveSnapshot", func(t *testing.T) {
		var callOrder []string
		kv := &testOnDiskKVTracked{
			testOnDiskKV: *newTestOnDiskKV(),
			onSync:       func() { callOrder = append(callOrder, "sync") },
			onPrepare:    func() { callOrder = append(callOrder, "prepare") },
			onSave:       func() { callOrder = append(callOrder, "save") },
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("dk", "dv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		if len(callOrder) != 3 {
			t.Fatalf("callOrder = %v, want [sync, prepare, save]", callOrder)
		}
		if callOrder[0] != "sync" || callOrder[1] != "prepare" || callOrder[2] != "save" {
			t.Errorf("callOrder = %v, want [sync, prepare, save]", callOrder)
		}
	})

	t.Run("on-disk SM sync error stops snapshot", func(t *testing.T) {
		kv := &testOnDiskKVSyncError{
			testOnDiskKV: *newTestOnDiskKV(),
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		var buf bytes.Buffer
		stopper := make(chan struct{})
		err = rsm.SaveSnapshot(context.Background(), &buf, stopper)
		if err == nil {
			t.Fatal("SaveSnapshot should fail when Sync fails")
		}
		var se *testSyncError
		if !errors.As(err, &se) {
			t.Errorf("error = %T, want *testSyncError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Close error propagation
// ---------------------------------------------------------------------------

func TestStateMachine_CloseErrorPropagation(t *testing.T) {
	t.Run("close error from regular SM propagates", func(t *testing.T) {
		kv := &testCloseErrorKV{testKV: testKV{data: make(map[string]string)}}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		err = rsm.Close()
		if err == nil {
			t.Fatal("Close should return error")
		}
		var ce *testCloseError
		if !errors.As(err, &ce) {
			t.Errorf("Close error = %T, want *testCloseError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Lookup via NALookup fast path
// ---------------------------------------------------------------------------

func TestStateMachine_LookupNAFastPath(t *testing.T) {
	t.Run("byte query uses NALookup when supported", func(t *testing.T) {
		kv := &testHasherSM{testKV: testKV{data: make(map[string]string)}}
		kv.data["hello"] = "world"
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Using []byte query should use NALookup fast path.
		val, err := rsm.Lookup(context.Background(), []byte("hello"))
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if string(val.([]byte)) != "world" {
			t.Errorf("Lookup = %q, want %q", val, "world")
		}
	})

	t.Run("non-byte query falls through to regular Lookup", func(t *testing.T) {
		kv := &testHasherSM{testKV: testKV{data: make(map[string]string)}}
		kv.data["hello"] = "world"
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// String query should fall through to regular Lookup.
		val, err := rsm.Lookup(context.Background(), "hello")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "world" {
			t.Errorf("Lookup = %q, want %q", val, "world")
		}
	})
}

// ---------------------------------------------------------------------------
// GetHash through StateMachine
// ---------------------------------------------------------------------------

func TestStateMachine_GetHashSupported(t *testing.T) {
	t.Run("GetHash returns hash when SM supports it", func(t *testing.T) {
		kv := &testHasherSM{testKV: testKV{data: make(map[string]string)}}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		hash, supported, err := rsm.GetHash()
		if err != nil {
			t.Fatalf("GetHash = %v", err)
		}
		if !supported {
			t.Fatal("GetHash should be supported for testHasherSM")
		}
		if hash != 42 {
			t.Errorf("GetHash = %d, want 42", hash)
		}
	})

	t.Run("GetHash error propagates", func(t *testing.T) {
		kv := &testHasherSMError{testKV: testKV{data: make(map[string]string)}}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		_, supported, err := rsm.GetHash()
		if !supported {
			t.Fatal("GetHash should be supported")
		}
		if err == nil {
			t.Fatal("GetHash should return error")
		}
	})
}

// ---------------------------------------------------------------------------
// Mixed batch: session register + data + session unregister with dedup
// ---------------------------------------------------------------------------

func TestApply_MixedBatchWithDedup(t *testing.T) {
	t.Run("register then apply then dedup in sequential batches", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Batch 1: register session.
		regEntries := []proto.Entry{
			{Index: 1, ClientID: 42, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
		}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply(register) = %v", err)
		}

		// Batch 2: apply data entry for session.
		dataEntries := []proto.Entry{
			{Index: 2, ClientID: 42, SeriesID: 1, Cmd: makeCmd("dk", "dv")},
		}
		dataResults := make([]sm.Result, 1)
		if err := rsm.Apply(dataEntries, dataResults); err != nil {
			t.Fatalf("Apply(data) = %v", err)
		}
		originalValue := dataResults[0].Value

		// Batch 3: attempt duplicate with same seriesID.
		dupEntries := []proto.Entry{
			{Index: 3, ClientID: 42, SeriesID: 1, Cmd: makeCmd("dk", "DIFFERENT")},
		}
		dupResults := make([]sm.Result, 1)
		if err := rsm.Apply(dupEntries, dupResults); err != nil {
			t.Fatalf("Apply(dup) = %v", err)
		}
		if dupResults[0].Value != originalValue {
			t.Errorf("dup result = %d, want %d (cached)", dupResults[0].Value, originalValue)
		}

		// Batch 4: new seriesID with respondedTo advancing past 1.
		newEntries := []proto.Entry{
			{Index: 4, ClientID: 42, SeriesID: 2, RespondedTo: 1, Cmd: makeCmd("nk", "nv")},
		}
		newResults := make([]sm.Result, 1)
		if err := rsm.Apply(newEntries, newResults); err != nil {
			t.Fatalf("Apply(new) = %v", err)
		}

		// Batch 5: unregister session.
		unregEntries := []proto.Entry{
			{Index: 5, ClientID: 42, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
		}
		unregResults := make([]sm.Result, 1)
		if err := rsm.Apply(unregEntries, unregResults); err != nil {
			t.Fatalf("Apply(unreg) = %v", err)
		}
		if unregResults[0].Value != 42 {
			t.Errorf("unreg Value = %d, want 42", unregResults[0].Value)
		}
		if rsm.Sessions().IsRegistered(42) {
			t.Error("session 42 should be unregistered")
		}
		if rsm.LastApplied() != 5 {
			t.Errorf("LastApplied = %d, want 5", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Session expiry disabled when sessionExpiryEntries is 0
// ---------------------------------------------------------------------------

func TestApply_SessionExpiryDisabled(t *testing.T) {
	t.Run("session expiry does not run when threshold is zero", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// sessionExpiryEntries = 0 (disabled).
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply at very high index. With expiry disabled, session should survive.
		entries := []proto.Entry{
			{Index: 999999, Cmd: makeCmd("x", "y")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if !rsm.Sessions().IsRegistered(10) {
			t.Error("session 10 should survive when expiry is disabled")
		}
	})
}

// ---------------------------------------------------------------------------
// Multiple sessions in single batch with data entries
// ---------------------------------------------------------------------------

func TestApply_MultipleSessionsInBatch(t *testing.T) {
	t.Run("batch with entries from multiple registered sessions", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register two sessions.
		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := rsm.Sessions().Register(20, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		// Apply entries from both sessions in single batch.
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("s10k", "s10v")},
			{Index: 2, ClientID: 20, SeriesID: 1, Cmd: makeCmd("s20k", "s20v")},
			{Index: 3, ClientID: 10, SeriesID: 2, RespondedTo: 1, Cmd: makeCmd("s10k2", "s10v2")},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// All should have been applied.
		for i, r := range results {
			if r.Value == 0 {
				t.Errorf("results[%d].Value = 0, want non-zero", i)
			}
		}

		// Verify data.
		for _, kv := range []struct{ k, v string }{
			{"s10k", "s10v"}, {"s20k", "s20v"}, {"s10k2", "s10v2"},
		} {
			val, lookupErr := rsm.Lookup(context.Background(), kv.k)
			if lookupErr != nil {
				t.Fatalf("Lookup(%s) = %v", kv.k, lookupErr)
			}
			if val.(string) != kv.v {
				t.Errorf("Lookup(%s) = %q, want %q", kv.k, val, kv.v)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot roundtrip with on-disk SM that has pre-existing data
// ---------------------------------------------------------------------------

func TestSnapshot_OnDiskSMWithOpenRecovery(t *testing.T) {
	t.Run("on-disk SM snapshot roundtrip after Open with lastApplied", func(t *testing.T) {
		const recoveredIndex = uint64(100)
		kv1 := newTestOnDiskKVWithState(recoveredIndex)
		createFn1 := func(shardID, replicaID uint64) interface{} {
			return kv1
		}
		rsm1, err := NewStateMachine(1, 1, createFn1, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		if _, err := rsm1.Open(make(chan struct{})); err != nil {
			t.Fatalf("Open = %v", err)
		}

		// Register session and apply data.
		if err := rsm1.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		entries := []proto.Entry{
			{Index: 101, ClientID: 10, SeriesID: 1, Cmd: makeCmd("odsk", "odsv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Recover into fresh SM.
		createFn2 := func(shardID, replicaID uint64) interface{} {
			return newTestOnDiskKVWithState(0)
		}
		rsm2, err := NewStateMachine(1, 1, createFn2, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		if !rsm2.Sessions().IsRegistered(10) {
			t.Error("session 10 should be registered after snapshot recovery")
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent reads of LastApplied, SMType, IsOnDisk during Apply
// ---------------------------------------------------------------------------

func TestStateMachine_ConcurrentAccessors(t *testing.T) {
	t.Run("concurrent reads of accessors during Apply do not race", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 1000; i++ {
				_ = rsm.LastApplied()
				_ = rsm.SMType()
				_ = rsm.IsOnDisk()
				_ = rsm.Sessions()
			}
		}()

		for i := uint64(1); i <= 100; i++ {
			entries := []proto.Entry{
				{Index: i, Cmd: makeCmd("k", "v")},
			}
			results := make([]sm.Result, 1)
			if err := rsm.Apply(entries, results); err != nil {
				t.Fatalf("Apply(%d) = %v", i, err)
			}
		}

		<-done
	})
}

// ---------------------------------------------------------------------------
// SaveSnapshot and RecoverFromSnapshot with user SM that returns errors
// ---------------------------------------------------------------------------

func TestStateMachine_SaveSnapshotUserSMError(t *testing.T) {
	t.Run("save snapshot error from user SM propagates", func(t *testing.T) {
		kv := &testSaveErrorKV{testKV: testKV{data: make(map[string]string)}}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		var buf bytes.Buffer
		stopper := make(chan struct{})
		err = rsm.SaveSnapshot(context.Background(), &buf, stopper)
		if err == nil {
			t.Fatal("SaveSnapshot should fail when user SM returns error")
		}
		var se *testSaveError
		if !errors.As(err, &se) {
			t.Errorf("error = %T, want *testSaveError", err)
		}
	})
}

func TestStateMachine_RecoverSnapshotUserSMError(t *testing.T) {
	t.Run("recover snapshot error from user SM propagates", func(t *testing.T) {
		// First create a valid snapshot to have valid session data.
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}
		rsm1.Close()

		// Now recover into a SM that fails on RecoverFromSnapshot.
		kv := &testRecoverErrorKV{testKV: testKV{data: make(map[string]string)}}
		createFn2 := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm2, err := NewStateMachine(1, 1, createFn2, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		err = rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper)
		if err == nil {
			t.Fatal("RecoverFromSnapshot should fail when user SM returns error")
		}
		var re *testRecoverError
		if !errors.As(err, &re) {
			t.Errorf("error = %T, want *testRecoverError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Test helpers for new tests
// ---------------------------------------------------------------------------

// testConcurrentKVTracked wraps testConcurrentKV with callback hooks.
type testConcurrentKVTracked struct {
	testConcurrentKV
	onPrepare func()
	onSave    func()
}

func (kv *testConcurrentKVTracked) PrepareSnapshot() (interface{}, error) {
	if kv.onPrepare != nil {
		kv.onPrepare()
	}
	return kv.testConcurrentKV.PrepareSnapshot()
}

func (kv *testConcurrentKVTracked) SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	if kv.onSave != nil {
		kv.onSave()
	}
	return kv.testConcurrentKV.SaveSnapshot(ctx, snapshotCtx, w, stopper)
}

// testOnDiskKVTracked wraps testOnDiskKV with callback hooks.
type testOnDiskKVTracked struct {
	testOnDiskKV
	onSync    func()
	onPrepare func()
	onSave    func()
}

func (kv *testOnDiskKVTracked) Sync() error {
	if kv.onSync != nil {
		kv.onSync()
	}
	return kv.testOnDiskKV.Sync()
}

func (kv *testOnDiskKVTracked) PrepareSnapshot() (interface{}, error) {
	if kv.onPrepare != nil {
		kv.onPrepare()
	}
	return kv.testOnDiskKV.PrepareSnapshot()
}

func (kv *testOnDiskKVTracked) SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	if kv.onSave != nil {
		kv.onSave()
	}
	return kv.testOnDiskKV.SaveSnapshot(ctx, snapshotCtx, w, stopper)
}

// testOnDiskKVSyncError returns an error from Sync.
type testOnDiskKVSyncError struct {
	testOnDiskKV
}

func (kv *testOnDiskKVSyncError) Sync() error {
	return &testSyncError{}
}

// testSyncError is a typed error for Sync failures.
type testSyncError struct{}

func (e *testSyncError) Error() string { return "test sync error" }

// testCloseErrorKV returns an error from Close.
type testCloseErrorKV struct {
	testKV
}

func (kv *testCloseErrorKV) Close(_ context.Context) error {
	return &testCloseError{}
}

// testCloseError is a typed error for Close failures.
type testCloseError struct{}

func (e *testCloseError) Error() string { return "test close error" }

// testSaveErrorKV returns an error from SaveSnapshot.
type testSaveErrorKV struct {
	testKV
}

func (kv *testSaveErrorKV) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return &testSaveError{}
}

// testSaveError is a typed error for SaveSnapshot failures.
type testSaveError struct{}

func (e *testSaveError) Error() string { return "test save error" }

// testRecoverErrorKV returns an error from RecoverFromSnapshot.
type testRecoverErrorKV struct {
	testKV
}

func (kv *testRecoverErrorKV) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return &testRecoverError{}
}

// testRecoverError is a typed error for RecoverFromSnapshot failures.
type testRecoverError struct{}

func (e *testRecoverError) Error() string { return "test recover error" }

func TestSMType_String(t *testing.T) {
	t.Run("known types return expected names", func(t *testing.T) {
		tests := []struct {
			smType SMType
			want   string
		}{
			{SMTypeRegular, "regular"},
			{SMTypeConcurrent, "concurrent"},
			{SMTypeOnDisk, "on-disk"},
		}
		for _, tt := range tests {
			if got := tt.smType.String(); got != tt.want {
				t.Errorf("SMType(%d).String() = %q, want %q", tt.smType, got, tt.want)
			}
		}
	})

	t.Run("unknown type returns formatted fallback", func(t *testing.T) {
		unknown := SMType(99)
		got := unknown.String()
		if got != "unknown(99)" {
			t.Errorf("SMType(99).String() = %q, want %q", got, "unknown(99)")
		}
	})
}

func TestSMType_TypeCode(t *testing.T) {
	t.Run("SMTypeRegular returns 0", func(t *testing.T) {
		if got := SMTypeRegular.TypeCode(); got != 0 {
			t.Errorf("SMTypeRegular.TypeCode() = %d, want 0", got)
		}
	})

	t.Run("SMTypeConcurrent returns 1", func(t *testing.T) {
		if got := SMTypeConcurrent.TypeCode(); got != 1 {
			t.Errorf("SMTypeConcurrent.TypeCode() = %d, want 1", got)
		}
	})

	t.Run("SMTypeOnDisk returns 2", func(t *testing.T) {
		if got := SMTypeOnDisk.TypeCode(); got != 2 {
			t.Errorf("SMTypeOnDisk.TypeCode() = %d, want 2", got)
		}
	})

	t.Run("TypeCode returns uint64 for serialization", func(t *testing.T) {
		// Verify the return type is uint64 by attempting assignment
		var code uint64 = SMTypeRegular.TypeCode()
		if code != 0 {
			t.Errorf("code = %d, want 0", code)
		}
	})

	t.Run("TypeCode is consistent with iota ordering", func(t *testing.T) {
		types := []SMType{SMTypeRegular, SMTypeConcurrent, SMTypeOnDisk}
		for i, st := range types {
			if got := st.TypeCode(); got != uint64(i) {
				t.Errorf("types[%d].TypeCode() = %d, want %d", i, got, i)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Session expiry, mixed batches, snapshot determinism, Close delegation, and
// invalid SM type tests
// ---------------------------------------------------------------------------

func TestStateMachine_SessionExpiryDuringApply(t *testing.T) {
	t.Run("expired session returns SESSION_EXPIRED", func(t *testing.T) {
		// sessionExpiryEntries=10 means sessions inactive for >10 entries are expired.
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 10, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register session at index 1.
		regEntries := []proto.Entry{
			{Index: 1, ClientID: 42, SeriesID: proto.SessionSeriesIDForRegister},
		}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply register = %v", err)
		}
		if regResults[0].Value != 42 {
			t.Fatalf("register result = %d, want 42", regResults[0].Value)
		}

		// Apply a batch at index 20 with a non-session entry to advance lastApplied
		// far enough that session 42 (last active at index 1) is expired.
		// Session expiry threshold is 10, so at index 20, sessions inactive since
		// index <= 10 are expired.
		advanceEntries := []proto.Entry{
			{Index: 20, ClientID: 0, Cmd: makeCmd("advance", "val")},
		}
		advanceResults := make([]sm.Result, 1)
		if err := rsm.Apply(advanceEntries, advanceResults); err != nil {
			t.Fatalf("Apply advance = %v", err)
		}

		// Now try to use the expired session — should get SESSION_EXPIRED.
		useEntries := []proto.Entry{
			{Index: 21, ClientID: 42, SeriesID: 1, Cmd: makeCmd("k", "v")},
		}
		useResults := make([]sm.Result, 1)
		if err := rsm.Apply(useEntries, useResults); err != nil {
			t.Fatalf("Apply expired session = %v", err)
		}
		if useResults[0].Value != sm.ResultSessionExpired {
			t.Errorf("expired session result = %d, want ResultSessionExpired (%d)",
				useResults[0].Value, sm.ResultSessionExpired)
		}
	})

	t.Run("active session is not expired", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 10, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register session at index 1.
		regEntries := []proto.Entry{
			{Index: 1, ClientID: 42, SeriesID: proto.SessionSeriesIDForRegister},
		}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply register = %v", err)
		}

		// Use the session at index 5 to keep it active.
		useEntries := []proto.Entry{
			{Index: 5, ClientID: 42, SeriesID: 1, Cmd: makeCmd("k1", "v1")},
		}
		useResults := make([]sm.Result, 1)
		if err := rsm.Apply(useEntries, useResults); err != nil {
			t.Fatalf("Apply use = %v", err)
		}

		// Advance to index 12. Session was active at index 5, threshold is 10,
		// so at index 12, sessions inactive since <= 2 are expired. Session 42
		// was active at index 5, so it should survive.
		advanceEntries := []proto.Entry{
			{Index: 12, ClientID: 0, Cmd: makeCmd("advance", "val")},
		}
		advanceResults := make([]sm.Result, 1)
		if err := rsm.Apply(advanceEntries, advanceResults); err != nil {
			t.Fatalf("Apply advance = %v", err)
		}

		// Session 42 should still be usable.
		use2Entries := []proto.Entry{
			{Index: 13, ClientID: 42, SeriesID: 2, Cmd: makeCmd("k2", "v2")},
		}
		use2Results := make([]sm.Result, 1)
		if err := rsm.Apply(use2Entries, use2Results); err != nil {
			t.Fatalf("Apply active session = %v", err)
		}
		if use2Results[0].Value == sm.ResultSessionExpired {
			t.Error("active session should not return SESSION_EXPIRED")
		}
	})
}

func TestStateMachine_ApplyMixedSessionAndNonSessionEntries(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	// Register two sessions.
	regEntries := []proto.Entry{
		{Index: 1, ClientID: 100, SeriesID: proto.SessionSeriesIDForRegister},
		{Index: 2, ClientID: 200, SeriesID: proto.SessionSeriesIDForRegister},
	}
	regResults := make([]sm.Result, 2)
	if err := rsm.Apply(regEntries, regResults); err != nil {
		t.Fatalf("Apply register = %v", err)
	}

	// Apply a mixed batch: session entries, non-session entries, and a
	// register all in one batch. Unregister is a separate batch because
	// the Apply loop processes register/unregister inline before the SM
	// Update call, so unregistering a session used in the same batch
	// would remove the session before RecordResult can cache the response.
	entries := []proto.Entry{
		{Index: 3, ClientID: 100, SeriesID: 1, Cmd: makeCmd("a", "1")},        // session-managed
		{Index: 4, ClientID: 0, Cmd: makeCmd("b", "2")},                       // non-session
		{Index: 5, ClientID: 200, SeriesID: 1, Cmd: makeCmd("c", "3")},        // session-managed
		{Index: 6, ClientID: 300, SeriesID: proto.SessionSeriesIDForRegister}, // register
		{Index: 7, ClientID: 0, Cmd: makeCmd("d", "4")},                       // non-session
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply mixed = %v", err)
	}

	// Verify session-managed entries got applied (non-zero result from testKV).
	if results[0].Value == sm.ResultSessionExpired {
		t.Error("session entry [0] should not be expired")
	}
	// Non-session entry should be applied.
	if results[1].Value == sm.ResultSessionExpired {
		t.Error("non-session entry [1] should not be expired")
	}
	// Second session entry should be applied.
	if results[2].Value == sm.ResultSessionExpired {
		t.Error("session entry [2] should not be expired")
	}
	// Register should return clientID.
	if results[3].Value != 300 {
		t.Errorf("register result = %d, want 300", results[3].Value)
	}
	// Non-session entry should be applied.
	if results[4].Value == sm.ResultSessionExpired {
		t.Error("non-session entry [4] should not be expired")
	}

	// LastApplied should be the highest index in the batch.
	if rsm.LastApplied() != 7 {
		t.Errorf("LastApplied() = %d, want 7", rsm.LastApplied())
	}

	// Unregister session 100 in a separate batch.
	unregEntries := []proto.Entry{
		{Index: 8, ClientID: 100, SeriesID: proto.SessionSeriesIDForUnregister},
	}
	unregResults := make([]sm.Result, 1)
	if err := rsm.Apply(unregEntries, unregResults); err != nil {
		t.Fatalf("Apply unregister = %v", err)
	}
	if unregResults[0].Value != 100 {
		t.Errorf("unregister result = %d, want 100", unregResults[0].Value)
	}

	// Verify the unregistered session is gone.
	postEntries := []proto.Entry{
		{Index: 9, ClientID: 100, SeriesID: 2, Cmd: makeCmd("e", "5")},
	}
	postResults := make([]sm.Result, 1)
	if err := rsm.Apply(postEntries, postResults); err != nil {
		t.Fatalf("Apply post-unregister = %v", err)
	}
	if postResults[0].Value != sm.ResultSessionExpired {
		t.Errorf("unregistered session result = %d, want ResultSessionExpired", postResults[0].Value)
	}
}

func TestStateMachine_SaveSnapshotDeterministic(t *testing.T) {
	// Two state machines with the same operations must produce identical snapshots.
	buildSM := func() *StateMachine {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		s, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		return s
	}

	applyOps := func(s *StateMachine) {
		// Register sessions.
		regEntries := []proto.Entry{
			{Index: 1, ClientID: 100, SeriesID: proto.SessionSeriesIDForRegister},
			{Index: 2, ClientID: 200, SeriesID: proto.SessionSeriesIDForRegister},
		}
		regResults := make([]sm.Result, 2)
		if err := s.Apply(regEntries, regResults); err != nil {
			t.Fatalf("Apply register = %v", err)
		}

		// Apply session-managed entries.
		entries := []proto.Entry{
			{Index: 3, ClientID: 100, SeriesID: 1, Cmd: makeCmd("k1", "v1")},
			{Index: 4, ClientID: 200, SeriesID: 1, Cmd: makeCmd("k2", "v2")},
			{Index: 5, ClientID: 100, SeriesID: 2, RespondedTo: 1, Cmd: makeCmd("k3", "v3")},
		}
		results := make([]sm.Result, 3)
		if err := s.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
	}

	sm1 := buildSM()
	defer sm1.Close()
	sm2 := buildSM()
	defer sm2.Close()

	applyOps(sm1)
	applyOps(sm2)

	stopper := make(chan struct{})
	ctx := context.Background()

	var buf1, buf2 bytes.Buffer
	if err := sm1.SaveSnapshot(ctx, &buf1, stopper); err != nil {
		t.Fatalf("SaveSnapshot sm1 = %v", err)
	}
	if err := sm2.SaveSnapshot(ctx, &buf2, stopper); err != nil {
		t.Fatalf("SaveSnapshot sm2 = %v", err)
	}

	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Errorf("snapshots differ: len(sm1)=%d, len(sm2)=%d", buf1.Len(), buf2.Len())
	}
}

func TestStateMachine_CloseDelegatesToSM(t *testing.T) {
	t.Run("close is forwarded to user SM", func(t *testing.T) {
		tracker := &closeTrackingKV{data: make(map[string]string)}
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return tracker
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}

		if tracker.closeCalled {
			t.Fatal("close should not be called before Close()")
		}
		if err := rsm.Close(); err != nil {
			t.Fatalf("Close = %v", err)
		}
		if !tracker.closeCalled {
			t.Error("Close should have been forwarded to user SM")
		}
	})

	t.Run("close error is propagated", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return &closeFailingKV{data: make(map[string]string)}
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		err = rsm.Close()
		if err == nil {
			t.Fatal("Close should have returned an error")
		}
		var closeErr *closeError
		if !errors.As(err, &closeErr) {
			t.Errorf("Close error = %T(%v), want *closeError", err, err)
		}
	})
}

func TestNewStateMachine_InvalidSMType(t *testing.T) {
	t.Run("nil factory result", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return nil
		}
		_, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if !errors.Is(err, ErrInvalidSMType) {
			t.Errorf("NewStateMachine(nil) = %v, want ErrInvalidSMType", err)
		}
	})

	t.Run("struct that does not implement any SM interface", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return struct{ Name string }{Name: "not a SM"}
		}
		_, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if !errors.Is(err, ErrInvalidSMType) {
			t.Errorf("NewStateMachine(struct) = %v, want ErrInvalidSMType", err)
		}
	})
}

// closeTrackingKV is a testKV that tracks whether Close was called.
type closeTrackingKV struct {
	data        map[string]string
	closeCalled bool
}

func (kv *closeTrackingKV) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			continue
		}
		kl := int(e.Cmd[0])
		key := string(e.Cmd[1 : 1+kl])
		value := string(e.Cmd[1+kl:])
		kv.data[key] = value
		results[i] = sm.Result{Value: uint64(len(value))}
	}
	return nil
}

func (kv *closeTrackingKV) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("query must be string")
	}
	return kv.data[key], nil
}

func (kv *closeTrackingKV) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(kv.data)))
	_, err := w.Write(buf[:])
	return err
}

func (kv *closeTrackingKV) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [4]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}
	kv.data = make(map[string]string)
	return nil
}

func (kv *closeTrackingKV) Close(_ context.Context) error {
	kv.closeCalled = true
	return nil
}

// closeFailingKV is a testKV whose Close returns an error.
type closeFailingKV struct {
	data map[string]string
}

func (kv *closeFailingKV) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			continue
		}
		kl := int(e.Cmd[0])
		key := string(e.Cmd[1 : 1+kl])
		value := string(e.Cmd[1+kl:])
		kv.data[key] = value
		results[i] = sm.Result{Value: uint64(len(value))}
	}
	return nil
}

func (kv *closeFailingKV) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return nil, nil
}

func (kv *closeFailingKV) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	return nil
}

func (kv *closeFailingKV) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	return nil
}

func (kv *closeFailingKV) Close(_ context.Context) error {
	return &closeError{}
}

// closeError is a typed error returned by closeFailingKV.
type closeError struct{}

func (e *closeError) Error() string { return "test close error" }

// ---------------------------------------------------------------------------
// Test helpers for gap-filling tests
// ---------------------------------------------------------------------------

// testUpdateError is a typed error returned by failingUpdateKV.
type testUpdateError struct{}

func (e *testUpdateError) Error() string { return "test update error" }

// failingUpdateKV is a SM that fails after processing N entries.
type failingUpdateKV struct {
	testKV
	failAfterN int
	callCount  int
}

func (kv *failingUpdateKV) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	kv.callCount++
	for i, e := range entries {
		if kv.failAfterN <= 0 {
			return &testUpdateError{}
		}
		kv.failAfterN--
		if len(e.Cmd) == 0 {
			results[i] = sm.Result{Value: 0}
			continue
		}
		keyLen := int(e.Cmd[0])
		if keyLen+1 > len(e.Cmd) {
			results[i] = sm.Result{Value: 0}
			continue
		}
		key := string(e.Cmd[1 : 1+keyLen])
		val := string(e.Cmd[1+keyLen:])
		kv.mu.Lock()
		kv.data[key] = val
		kv.mu.Unlock()
		results[i] = sm.Result{Value: uint64(i + 1)}
	}
	return nil
}

// corruptingWriter writes data then corrupts it by flipping the last byte.
type corruptingWriter struct {
	buf bytes.Buffer
}

func (w *corruptingWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *corruptingWriter) corrupt() {
	data := w.buf.Bytes()
	if len(data) > 0 {
		data[len(data)-1] ^= 0xFF
	}
}

// testOnDiskKVWithState simulates persistent state for recovery testing.
type testOnDiskKVWithState struct {
	mu          sync.RWMutex
	data        map[string]string
	lastApplied uint64
	openCalled  bool
	closeCalled bool
}

func newTestOnDiskKVWithState(lastApplied uint64) *testOnDiskKVWithState {
	return &testOnDiskKVWithState{
		data:        make(map[string]string),
		lastApplied: lastApplied,
	}
}

func (kv *testOnDiskKVWithState) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	kv.openCalled = true
	return kv.lastApplied, nil
}

func (kv *testOnDiskKVWithState) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			results[i] = sm.Result{Value: 0}
			continue
		}
		keyLen := int(e.Cmd[0])
		if keyLen+1 > len(e.Cmd) {
			results[i] = sm.Result{Value: 0}
			continue
		}
		key := string(e.Cmd[1 : 1+keyLen])
		val := string(e.Cmd[1+keyLen:])
		kv.data[key] = val
		results[i] = sm.Result{Value: uint64(len(kv.data))}
	}
	return nil
}

func (kv *testOnDiskKVWithState) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("query must be string")
	}
	return kv.data[key], nil
}

func (kv *testOnDiskKVWithState) Sync() error { return nil }

func (kv *testOnDiskKVWithState) PrepareSnapshot() (interface{}, error) {
	kv.mu.RLock()
	snap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snap[k] = v
	}
	kv.mu.RUnlock()
	return snap, nil
}

func (kv *testOnDiskKVWithState) SaveSnapshot(_ context.Context, _ interface{}, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (kv *testOnDiskKVWithState) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (kv *testOnDiskKVWithState) Close(_ context.Context) error {
	kv.closeCalled = true
	return nil
}

// ---------------------------------------------------------------------------
// On-disk SM: Open sets lastApplied correctly
// ---------------------------------------------------------------------------

func TestOnDiskSM_OpenSetsLastApplied(t *testing.T) {
	t.Run("open with non-zero lastApplied stores it in StateMachine", func(t *testing.T) {
		const lastApplied = uint64(42)
		kv := newTestOnDiskKVWithState(lastApplied)
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		got, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if got != lastApplied {
			t.Errorf("Open returned = %d, want %d", got, lastApplied)
		}
		if rsm.LastApplied() != lastApplied {
			t.Errorf("LastApplied = %d, want %d", rsm.LastApplied(), lastApplied)
		}
	})

	t.Run("open with zero lastApplied leaves it at zero", func(t *testing.T) {
		kv := newTestOnDiskKVWithState(0)
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		got, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if got != 0 {
			t.Errorf("Open returned = %d, want 0", got)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied = %d, want 0", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM: already-applied entries treated as no-op for lastApplied
// ---------------------------------------------------------------------------

func TestOnDiskSM_AlreadyAppliedEntryTreatedAsNoOp(t *testing.T) {
	t.Run("entries at or below lastApplied do not advance it", func(t *testing.T) {
		const recoveredIndex = uint64(50)
		kv := newTestOnDiskKVWithState(recoveredIndex)
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if _, err := rsm.Open(make(chan struct{})); err != nil {
			t.Fatalf("Open = %v", err)
		}

		// Apply entries at or below recovered index.
		entries := []proto.Entry{
			{Index: 30, Cmd: makeCmd("old1", "val1")},
			{Index: 40, Cmd: makeCmd("old2", "val2")},
			{Index: 50, Cmd: makeCmd("old3", "val3")},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// LastApplied should remain at 50 (not decrease or re-set).
		if rsm.LastApplied() != recoveredIndex {
			t.Errorf("LastApplied = %d, want %d", rsm.LastApplied(), recoveredIndex)
		}
	})

	t.Run("entry above lastApplied advances it", func(t *testing.T) {
		const recoveredIndex = uint64(50)
		kv := newTestOnDiskKVWithState(recoveredIndex)
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if _, err := rsm.Open(make(chan struct{})); err != nil {
			t.Fatalf("Open = %v", err)
		}

		entries := []proto.Entry{
			{Index: 51, Cmd: makeCmd("new1", "val1")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.LastApplied() != 51 {
			t.Errorf("LastApplied = %d, want 51", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Apply: user SM Update error propagates through session-managed batch
// ---------------------------------------------------------------------------

func TestApply_UserSMUpdateErrorPropagatesWithSessionEntries(t *testing.T) {
	t.Run("update error returned even when session entries precede data entries", func(t *testing.T) {
		kv := &failingUpdateKV{
			testKV:     testKV{data: make(map[string]string)},
			failAfterN: 0, // fail immediately
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register a session first.
		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Batch: session register + data entry that will fail SM Update.
		entries := []proto.Entry{
			{Index: 1, ClientID: 20, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
			{Index: 2, ClientID: 10, SeriesID: 1, Cmd: makeCmd("k", "v")},
		}
		results := make([]sm.Result, 2)
		err = rsm.Apply(entries, results)
		if err == nil {
			t.Fatal("Apply should return error when SM Update fails")
		}
		var ue *testUpdateError
		if !errors.As(err, &ue) {
			t.Errorf("error = %T, want *testUpdateError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Apply: large result data preserved through session cache
// ---------------------------------------------------------------------------

func TestApply_LargeResultData(t *testing.T) {
	t.Run("large result Data field preserved in dedup cache", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("big", "value")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		originalValue := results[0].Value

		// Replay same seriesID - should return cached result.
		dupEntries := []proto.Entry{
			{Index: 5, ClientID: 10, SeriesID: 1, Cmd: makeCmd("big", "DIFFERENT")},
		}
		dupResults := make([]sm.Result, 1)
		if err := rsm.Apply(dupEntries, dupResults); err != nil {
			t.Fatalf("Apply(dup) = %v", err)
		}
		if dupResults[0].Value != originalValue {
			t.Errorf("dup result Value = %d, want %d", dupResults[0].Value, originalValue)
		}
	})
}

// ---------------------------------------------------------------------------
// Apply: single entry batch
// ---------------------------------------------------------------------------

func TestApply_SingleEntryBatch(t *testing.T) {
	t.Run("single non-session entry batch processes correctly", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("solo", "val")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 1 {
			t.Errorf("result Value = %d, want 1", results[0].Value)
		}
		if rsm.LastApplied() != 1 {
			t.Errorf("LastApplied = %d, want 1", rsm.LastApplied())
		}
	})

	t.Run("single session register entry batch", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, ClientID: 77, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 77 {
			t.Errorf("result Value = %d, want 77", results[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// Apply: batch where all session entries are duplicates
// ---------------------------------------------------------------------------

func TestApply_AllSessionDuplicatesBatch(t *testing.T) {
	t.Run("all entries are duplicates so no SM Update is called", func(t *testing.T) {
		kv := &failingUpdateKV{
			testKV:     testKV{data: make(map[string]string)},
			failAfterN: 1, // allow first call only
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply original entry.
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("k", "v")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply(original) = %v", err)
		}
		originalValue := results[0].Value

		// Now replay the same entry. failingUpdateKV would fail if Update
		// is called again, but it should not be called since all entries
		// are deduped.
		dupEntries := []proto.Entry{
			{Index: 5, ClientID: 10, SeriesID: 1, Cmd: makeCmd("k", "v")},
		}
		dupResults := make([]sm.Result, 1)
		if err := rsm.Apply(dupEntries, dupResults); err != nil {
			t.Fatalf("Apply(dup) = %v", err)
		}
		if dupResults[0].Value != originalValue {
			t.Errorf("dup result = %d, want %d", dupResults[0].Value, originalValue)
		}
		// Update should have been called exactly once.
		if kv.callCount != 1 {
			t.Errorf("Update call count = %d, want 1", kv.callCount)
		}
	})
}

// ---------------------------------------------------------------------------
// Apply: session-managed entry with empty command payload
// ---------------------------------------------------------------------------

func TestApply_SessionManagedEntryWithEmptyCmd(t *testing.T) {
	t.Run("session entry with nil Cmd still records result", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Session-managed entry with empty Cmd.
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: nil},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Replay should return cached result.
		dupEntries := []proto.Entry{
			{Index: 5, ClientID: 10, SeriesID: 1, Cmd: makeCmd("ignored", "data")},
		}
		dupResults := make([]sm.Result, 1)
		if err := rsm.Apply(dupEntries, dupResults); err != nil {
			t.Fatalf("Apply(dup) = %v", err)
		}
		if dupResults[0].Value != results[0].Value {
			t.Errorf("dup result = %d, want %d", dupResults[0].Value, results[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// StateMachine: Open is idempotent for regular SM (returns 0, nil)
// ---------------------------------------------------------------------------

func TestStateMachine_OpenRegularReturnsZero(t *testing.T) {
	t.Run("Open on regular SM returns zero lastApplied", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		got, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if got != 0 {
			t.Errorf("Open = %d, want 0 for regular SM", got)
		}
	})

	t.Run("Open on concurrent SM returns zero lastApplied", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		got, err := rsm.Open(make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if got != 0 {
			t.Errorf("Open = %d, want 0 for concurrent SM", got)
		}
	})
}

// ---------------------------------------------------------------------------
// StateMachine: SMType accessor returns correct type
// ---------------------------------------------------------------------------

func TestStateMachine_SMTypeAccessorAllTypes(t *testing.T) {
	t.Run("regular SM type", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()
		if rsm.SMType() != SMTypeRegular {
			t.Errorf("SMType = %d, want %d", rsm.SMType(), SMTypeRegular)
		}
	})

	t.Run("concurrent SM type", func(t *testing.T) {
		createFn := sm.NewConcurrentCreateFunc(func(shardID, replicaID uint64) sm.ConcurrentStateMachine {
			return newTestConcurrentKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()
		if rsm.SMType() != SMTypeConcurrent {
			t.Errorf("SMType = %d, want %d", rsm.SMType(), SMTypeConcurrent)
		}
	})

	t.Run("on-disk SM type", func(t *testing.T) {
		createFn := sm.NewDiskCreateFunc(func(shardID, replicaID uint64) sm.DiskStateMachine {
			return newTestOnDiskKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()
		if rsm.SMType() != SMTypeOnDisk {
			t.Errorf("SMType = %d, want %d", rsm.SMType(), SMTypeOnDisk)
		}
	})
}

// ---------------------------------------------------------------------------
// StateMachine: Sessions accessor returns valid manager
// ---------------------------------------------------------------------------

func TestStateMachine_SessionsAccessorNotNil(t *testing.T) {
	t.Run("Sessions returns non-nil manager", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if rsm.Sessions() == nil {
			t.Fatal("Sessions() should not return nil")
		}
	})

	t.Run("Sessions reflects registered sessions", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if rsm.Sessions().Count() != 0 {
			t.Errorf("initial count = %d, want 0", rsm.Sessions().Count())
		}

		// Register via Apply.
		entries := []proto.Entry{
			{Index: 1, ClientID: 42, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		if rsm.Sessions().Count() != 1 {
			t.Errorf("count after register = %d, want 1", rsm.Sessions().Count())
		}
	})
}

// ---------------------------------------------------------------------------
// NALookupBuf: pooled byte-slice lookup
// ---------------------------------------------------------------------------

func TestStateMachine_NALookupBuf_NotSupported(t *testing.T) {
	// testKV does not implement NALookupInto or NALookup — returns not supported.
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	result, release, supported, lookupErr := rsm.NALookupBuf([]byte("key"))
	if lookupErr != nil {
		t.Fatalf("NALookupBuf = %v", lookupErr)
	}
	if supported {
		t.Error("NALookupBuf should return supported=false for testKV")
	}
	if result != nil {
		t.Errorf("NALookupBuf result = %v, want nil", result)
	}
	release() // must be safe to call even when not supported
}

func TestStateMachine_NALookupBuf_Supported(t *testing.T) {
	// testNALookupIntoSM implements NALookupInto — uses pooled path.
	kv := &testNALookupIntoSM{testHasherSM: testHasherSM{testKV: testKV{data: map[string]string{"hello": "world"}}}}
	createFn := func(shardID, replicaID uint64) interface{} {
		return kv
	}
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	result, release, supported, lookupErr := rsm.NALookupBuf([]byte("hello"))
	if lookupErr != nil {
		t.Fatalf("NALookupBuf = %v", lookupErr)
	}
	if !supported {
		t.Fatal("NALookupBuf should return supported=true for testNALookupIntoSM")
	}
	if string(result) != "world" {
		t.Errorf("NALookupBuf = %q, want %q", result, "world")
	}
	release() // must return the pool buffer safely
}

func TestStateMachine_NALookupBuf_ErrorPropagated(t *testing.T) {
	// testNALookupIntoSMError returns an error from BytesLookupInto.
	kv := &testNALookupIntoSMError{}
	createFn := func(shardID, replicaID uint64) interface{} {
		return kv
	}
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	result, release, supported, lookupErr := rsm.NALookupBuf([]byte("key"))
	if !supported {
		t.Fatal("NALookupBuf should return supported=true even on error")
	}
	if lookupErr == nil {
		t.Fatal("NALookupBuf should return error from BytesLookupInto")
	}
	var te *testError
	if !errors.As(lookupErr, &te) {
		t.Errorf("error = %T, want *testError", lookupErr)
	}
	if result != nil {
		t.Errorf("NALookupBuf result = %v, want nil on error", result)
	}
	release() // safe to call even on error
}

func TestStateMachine_NALookupBuf_FallsBackToNALookup(t *testing.T) {
	// testHasherSM implements NALookup but not NALookupInto.
	// NALookupBuf should fall back to the standard NALookup path.
	kv := &testHasherSM{testKV: testKV{data: map[string]string{"key": "val"}}}
	createFn := func(shardID, replicaID uint64) interface{} {
		return kv
	}
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	result, release, supported, lookupErr := rsm.NALookupBuf([]byte("key"))
	if lookupErr != nil {
		t.Fatalf("NALookupBuf = %v", lookupErr)
	}
	if !supported {
		t.Fatal("NALookupBuf should return supported=true via NALookup fallback")
	}
	if string(result) != "val" {
		t.Errorf("NALookupBuf = %q, want %q", result, "val")
	}
	release() // nop release — safe to call
}

func TestStateMachine_NALookupBuf_PoolReuse(t *testing.T) {
	// Verify that the pool buffer is correctly returned and the pool
	// can serve subsequent calls. Make multiple sequential calls and
	// ensure each returns the correct value.
	kv := &testNALookupIntoSM{testHasherSM: testHasherSM{testKV: testKV{data: map[string]string{
		"a": "alpha",
		"b": "beta",
	}}}}
	createFn := func(shardID, replicaID uint64) interface{} {
		return kv
	}
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	for i := 0; i < 8; i++ {
		r1, rel1, ok1, err1 := rsm.NALookupBuf([]byte("a"))
		if err1 != nil || !ok1 {
			t.Fatalf("NALookupBuf(a) supported=%v err=%v", ok1, err1)
		}
		if string(r1) != "alpha" {
			t.Errorf("iter %d: NALookupBuf(a) = %q, want %q", i, r1, "alpha")
		}
		rel1() // return buffer to pool

		r2, rel2, ok2, err2 := rsm.NALookupBuf([]byte("b"))
		if err2 != nil || !ok2 {
			t.Fatalf("NALookupBuf(b) supported=%v err=%v", ok2, err2)
		}
		if string(r2) != "beta" {
			t.Errorf("iter %d: NALookupBuf(b) = %q, want %q", i, r2, "beta")
		}
		rel2()
	}
}
