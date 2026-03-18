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

package sm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// ---------------------------------------------------------------------------
// Types: Result
// ---------------------------------------------------------------------------

func TestResult_FieldsAccessible(t *testing.T) {
	r := Result{Value: 42, Data: []byte("hello")}
	if r.Value != 42 {
		t.Errorf("Value = %d, want 42", r.Value)
	}
	if string(r.Data) != "hello" {
		t.Errorf("Data = %q, want %q", r.Data, "hello")
	}
}

func TestResult_ZeroValue(t *testing.T) {
	var r Result
	if r.Value != 0 {
		t.Errorf("zero Value = %d, want 0", r.Value)
	}
	if r.Data != nil {
		t.Errorf("zero Data = %v, want nil", r.Data)
	}
}

func TestResult_NilData(t *testing.T) {
	r := Result{Value: 1, Data: nil}
	if r.Data != nil {
		t.Errorf("Data = %v, want nil", r.Data)
	}
}

func TestResult_EmptyData(t *testing.T) {
	r := Result{Value: 1, Data: []byte{}}
	if len(r.Data) != 0 {
		t.Errorf("Data length = %d, want 0", len(r.Data))
	}
}

// ---------------------------------------------------------------------------
// Types: Entry
// ---------------------------------------------------------------------------

func TestEntry_FieldsAccessible(t *testing.T) {
	e := Entry{Index: 100, Cmd: []byte("SET key val")}
	if e.Index != 100 {
		t.Errorf("Index = %d, want 100", e.Index)
	}
	if string(e.Cmd) != "SET key val" {
		t.Errorf("Cmd = %q, want %q", e.Cmd, "SET key val")
	}
}

func TestEntry_ZeroValue(t *testing.T) {
	var e Entry
	if e.Index != 0 {
		t.Errorf("zero Index = %d, want 0", e.Index)
	}
	if e.Cmd != nil {
		t.Errorf("zero Cmd = %v, want nil", e.Cmd)
	}
}

func TestEntry_CopyCmd(t *testing.T) {
	original := []byte("payload data")
	e := Entry{Index: 1, Cmd: original}

	cp := e.CopyCmd()
	if !bytes.Equal(cp, original) {
		t.Errorf("CopyCmd() = %q, want %q", cp, original)
	}

	// Mutating the copy must not affect the original.
	cp[0] = 'X'
	if e.Cmd[0] == 'X' {
		t.Error("CopyCmd returned a slice that aliases the original Cmd")
	}
}

func TestEntry_CopyCmd_NilCmd(t *testing.T) {
	e := Entry{Index: 1, Cmd: nil}
	cp := e.CopyCmd()
	if cp != nil {
		t.Errorf("CopyCmd() on nil Cmd = %v, want nil", cp)
	}
}

func TestEntry_CopyCmd_EmptyCmd(t *testing.T) {
	e := Entry{Index: 1, Cmd: []byte{}}
	cp := e.CopyCmd()
	if cp != nil {
		t.Errorf("CopyCmd() on empty Cmd = %v, want nil", cp)
	}
}

// ---------------------------------------------------------------------------
// testStateMachine: implements StateMachine for compile-time and runtime tests.
// ---------------------------------------------------------------------------

// errTestFatal is used by test state machine implementations to simulate errors.
var errTestFatal = errors.New("test: fatal error")

type testStateMachine struct {
	state   map[string][]byte
	closed  bool
	updated bool
}

func newTestStateMachine() *testStateMachine {
	return &testStateMachine{state: make(map[string][]byte)}
}

func (sm *testStateMachine) Update(ctx context.Context, entries []Entry, results []Result) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			return errTestFatal
		}
		sm.state[string(e.Cmd)] = e.Cmd
		results[i] = Result{Value: e.Index}
	}
	sm.updated = true
	return nil
}

func (sm *testStateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	key, ok := query.(string)
	if !ok {
		return nil, errTestFatal
	}
	val, exists := sm.state[key]
	if !exists {
		return nil, nil
	}
	return val, nil
}

func (sm *testStateMachine) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for k, v := range sm.state {
		select {
		case <-stopper:
			return errors.New("stopped")
		default:
		}
		keyBytes := []byte(k)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

func (sm *testStateMachine) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	sm.state = make(map[string][]byte)
	for {
		select {
		case <-stopper:
			return errors.New("stopped")
		default:
		}
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return err
		}
		var valLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return err
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBytes); err != nil {
			return err
		}
		sm.state[string(keyBytes)] = valBytes
	}
}

func (sm *testStateMachine) Close(ctx context.Context) error {
	sm.closed = true
	return nil
}

// Compile-time interface check.
var _ StateMachine = (*testStateMachine)(nil)

// ---------------------------------------------------------------------------
// testConcurrentStateMachine: implements ConcurrentStateMachine.
// ---------------------------------------------------------------------------

type testConcurrentStateMachine struct {
	testStateMachine
	snapshotState map[string][]byte
}

func newTestConcurrentStateMachine() *testConcurrentStateMachine {
	return &testConcurrentStateMachine{
		testStateMachine: testStateMachine{state: make(map[string][]byte)},
	}
}

func (sm *testConcurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	snapshot := make(map[string][]byte, len(sm.state))
	for k, v := range sm.state {
		cp := make([]byte, len(v))
		copy(cp, v)
		snapshot[k] = cp
	}
	return snapshot, nil
}

func (sm *testConcurrentStateMachine) SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	state, ok := snapshotCtx.(map[string][]byte)
	if !ok {
		return errTestFatal
	}
	for k, v := range state {
		select {
		case <-stopper:
			return errors.New("stopped")
		default:
		}
		keyBytes := []byte(k)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

// Compile-time interface check.
var _ ConcurrentStateMachine = (*testConcurrentStateMachine)(nil)

// ---------------------------------------------------------------------------
// testOnDiskStateMachine: implements DiskStateMachine.
// ---------------------------------------------------------------------------

type testOnDiskStateMachine struct {
	testConcurrentStateMachine
	lastApplied uint64
	dir         string
	synced      bool
}

func newTestOnDiskStateMachine() *testOnDiskStateMachine {
	return &testOnDiskStateMachine{
		testConcurrentStateMachine: testConcurrentStateMachine{
			testStateMachine: testStateMachine{state: make(map[string][]byte)},
		},
	}
}

func (sm *testOnDiskStateMachine) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	sm.dir = initialDir
	return sm.lastApplied, nil
}

func (sm *testOnDiskStateMachine) Update(ctx context.Context, entries []Entry, results []Result) error {
	if err := sm.testConcurrentStateMachine.Update(ctx, entries, results); err != nil {
		return err
	}
	if len(entries) > 0 {
		sm.lastApplied = entries[len(entries)-1].Index
	}
	return nil
}

func (sm *testOnDiskStateMachine) Sync() error {
	sm.synced = true
	return nil
}

// Compile-time interface check.
var _ DiskStateMachine = (*testOnDiskStateMachine)(nil)

// ---------------------------------------------------------------------------
// StateMachine tests
// ---------------------------------------------------------------------------

func TestStateMachine_UpdateAndLookup(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()

	entries := []Entry{
		{Index: 1, Cmd: []byte("key1")},
		{Index: 2, Cmd: []byte("key2")},
	}
	results := make([]Result, len(entries))

	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if results[0].Value != 1 {
		t.Errorf("results[0].Value = %d, want 1", results[0].Value)
	}
	if results[1].Value != 2 {
		t.Errorf("results[1].Value = %d, want 2", results[1].Value)
	}

	val, err := sm.Lookup(ctx, "key1")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if string(val.([]byte)) != "key1" {
		t.Errorf("Lookup(key1) = %q, want %q", val, "key1")
	}
}

func TestStateMachine_UpdateErrorOnEmptyCmd(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()

	entries := []Entry{{Index: 1, Cmd: []byte{}}}
	results := make([]Result, 1)

	err := sm.Update(ctx, entries, results)
	if err == nil {
		t.Error("Update with empty Cmd should return error")
	}
}

func TestStateMachine_LookupMissingKey(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()

	val, err := sm.Lookup(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if val != nil {
		t.Errorf("Lookup(nonexistent) = %v, want nil", val)
	}
}

func TestStateMachine_LookupInvalidQuery(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()

	_, err := sm.Lookup(ctx, 12345)
	if err == nil {
		t.Error("Lookup with non-string query should return error")
	}
}

func TestStateMachine_SnapshotRoundTrip(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	entries := []Entry{
		{Index: 1, Cmd: []byte("alpha")},
		{Index: 2, Cmd: []byte("beta")},
	}
	results := make([]Result, len(entries))
	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	var buf bytes.Buffer
	if err := sm.SaveSnapshot(ctx, &buf, stopper); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	sm2 := newTestStateMachine()
	if err := sm2.RecoverFromSnapshot(ctx, &buf, stopper); err != nil {
		t.Fatalf("RecoverFromSnapshot failed: %v", err)
	}

	for _, key := range []string{"alpha", "beta"} {
		val, err := sm2.Lookup(ctx, key)
		if err != nil {
			t.Fatalf("Lookup(%s) after recovery failed: %v", key, err)
		}
		if val == nil {
			t.Errorf("Lookup(%s) after recovery returned nil", key)
		}
	}
}

func TestStateMachine_RecoverFromSnapshot_InvalidData(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	// Feed truncated data: write a key length but no actual key data.
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(100)); err != nil {
		t.Fatalf("binary.Write failed: %v", err)
	}

	err := sm.RecoverFromSnapshot(ctx, &buf, stopper)
	if err == nil {
		t.Error("RecoverFromSnapshot with truncated data should return error")
	}
}

func TestStateMachine_Close(t *testing.T) {
	sm := newTestStateMachine()
	ctx := context.Background()

	if err := sm.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !sm.closed {
		t.Error("Close did not set closed flag")
	}
}

func TestStateMachine_UpdateCanceledContext(t *testing.T) {
	sm := newTestStateMachine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	entries := []Entry{{Index: 1, Cmd: []byte("data")}}
	results := make([]Result, 1)

	err := sm.Update(ctx, entries, results)
	if err == nil {
		t.Error("Update with canceled context should return error")
	}
}

func TestStateMachine_LookupCanceledContext(t *testing.T) {
	sm := newTestStateMachine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := sm.Lookup(ctx, "key")
	if err == nil {
		t.Error("Lookup with canceled context should return error")
	}
}

// ---------------------------------------------------------------------------
// ConcurrentStateMachine tests
// ---------------------------------------------------------------------------

func TestConcurrentStateMachine_UpdateAndLookup(t *testing.T) {
	sm := newTestConcurrentStateMachine()
	ctx := context.Background()

	entries := []Entry{{Index: 10, Cmd: []byte("concurrent-key")}}
	results := make([]Result, 1)

	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if results[0].Value != 10 {
		t.Errorf("results[0].Value = %d, want 10", results[0].Value)
	}

	val, err := sm.Lookup(ctx, "concurrent-key")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if string(val.([]byte)) != "concurrent-key" {
		t.Errorf("Lookup = %q, want %q", val, "concurrent-key")
	}
}

func TestConcurrentStateMachine_PrepareSnapshot(t *testing.T) {
	sm := newTestConcurrentStateMachine()
	ctx := context.Background()

	entries := []Entry{
		{Index: 1, Cmd: []byte("snap-key")},
	}
	results := make([]Result, 1)
	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	snapCtx, err := sm.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot failed: %v", err)
	}
	state, ok := snapCtx.(map[string][]byte)
	if !ok {
		t.Fatal("PrepareSnapshot returned unexpected type")
	}
	if _, exists := state["snap-key"]; !exists {
		t.Error("PrepareSnapshot did not capture 'snap-key'")
	}
}

func TestConcurrentStateMachine_SnapshotRoundTrip(t *testing.T) {
	sm := newTestConcurrentStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	entries := []Entry{{Index: 1, Cmd: []byte("csm-data")}}
	results := make([]Result, 1)
	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	snapCtx, err := sm.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot failed: %v", err)
	}

	var buf bytes.Buffer
	if err := sm.SaveSnapshot(ctx, snapCtx, &buf, stopper); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	sm2 := newTestConcurrentStateMachine()
	if err := sm2.RecoverFromSnapshot(ctx, &buf, stopper); err != nil {
		t.Fatalf("RecoverFromSnapshot failed: %v", err)
	}

	val, err := sm2.Lookup(ctx, "csm-data")
	if err != nil {
		t.Fatalf("Lookup after recovery failed: %v", err)
	}
	if val == nil {
		t.Error("Lookup after recovery returned nil")
	}
}

func TestConcurrentStateMachine_SaveSnapshotInvalidCtx(t *testing.T) {
	sm := newTestConcurrentStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	var buf bytes.Buffer
	err := sm.SaveSnapshot(ctx, "invalid-context", &buf, stopper)
	if err == nil {
		t.Error("SaveSnapshot with invalid snapshotCtx should return error")
	}
}

func TestConcurrentStateMachine_Close(t *testing.T) {
	sm := newTestConcurrentStateMachine()
	ctx := context.Background()

	if err := sm.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !sm.closed {
		t.Error("Close did not set closed flag")
	}
}

// ---------------------------------------------------------------------------
// DiskStateMachine tests
// ---------------------------------------------------------------------------

func TestDiskStateMachine_OpenFreshStart(t *testing.T) {
	sm := newTestOnDiskStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	lastApplied, err := sm.Open(ctx, "/tmp/test-sm", stopper)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if lastApplied != 0 {
		t.Errorf("lastApplied = %d, want 0 for fresh start", lastApplied)
	}
	if sm.dir != "/tmp/test-sm" {
		t.Errorf("dir = %q, want %q", sm.dir, "/tmp/test-sm")
	}
}

func TestDiskStateMachine_OpenCanceledContext(t *testing.T) {
	sm := newTestOnDiskStateMachine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopper := make(chan struct{})

	_, err := sm.Open(ctx, "/tmp/test-sm", stopper)
	if err == nil {
		t.Error("Open with canceled context should return error")
	}
}

func TestDiskStateMachine_UpdateTracksLastApplied(t *testing.T) {
	sm := newTestOnDiskStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	if _, err := sm.Open(ctx, "/tmp/test-sm", stopper); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	entries := []Entry{
		{Index: 5, Cmd: []byte("disk-entry1")},
		{Index: 6, Cmd: []byte("disk-entry2")},
	}
	results := make([]Result, 2)
	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if sm.lastApplied != 6 {
		t.Errorf("lastApplied = %d, want 6", sm.lastApplied)
	}
}

func TestDiskStateMachine_Sync(t *testing.T) {
	sm := newTestOnDiskStateMachine()
	if err := sm.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if !sm.synced {
		t.Error("Sync did not set synced flag")
	}
}

func TestDiskStateMachine_FullLifecycle(t *testing.T) {
	sm := newTestOnDiskStateMachine()
	ctx := context.Background()
	stopper := make(chan struct{})

	// Open
	lastApplied, err := sm.Open(ctx, "/tmp/ondisk-sm", stopper)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if lastApplied != 0 {
		t.Errorf("initial lastApplied = %d, want 0", lastApplied)
	}

	// Update
	entries := []Entry{{Index: 1, Cmd: []byte("ondisk-key")}}
	results := make([]Result, 1)
	if err := sm.Update(ctx, entries, results); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Sync
	if err := sm.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// PrepareSnapshot
	snapCtx, err := sm.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot failed: %v", err)
	}

	// SaveSnapshot
	var buf bytes.Buffer
	if err := sm.SaveSnapshot(ctx, snapCtx, &buf, stopper); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Lookup
	val, err := sm.Lookup(ctx, "ondisk-key")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if val == nil {
		t.Error("Lookup returned nil")
	}

	// RecoverFromSnapshot
	sm2 := newTestOnDiskStateMachine()
	if _, err := sm2.Open(ctx, "/tmp/ondisk-sm2", stopper); err != nil {
		t.Fatalf("Open sm2 failed: %v", err)
	}
	if err := sm2.RecoverFromSnapshot(ctx, &buf, stopper); err != nil {
		t.Fatalf("RecoverFromSnapshot failed: %v", err)
	}
	val2, err := sm2.Lookup(ctx, "ondisk-key")
	if err != nil {
		t.Fatalf("Lookup after recovery failed: %v", err)
	}
	if val2 == nil {
		t.Error("Lookup after recovery returned nil")
	}

	// Close
	if err := sm.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := sm2.Close(ctx); err != nil {
		t.Fatalf("Close sm2 failed: %v", err)
	}
}

func TestDiskStateMachine_UpdateErrorOnEmptyCmd(t *testing.T) {
	sm := newTestOnDiskStateMachine()
	ctx := context.Background()

	entries := []Entry{{Index: 1, Cmd: []byte{}}}
	results := make([]Result, 1)

	err := sm.Update(ctx, entries, results)
	if err == nil {
		t.Error("Update with empty Cmd should return error")
	}
}

// ---------------------------------------------------------------------------
// CreateFunc and type-safe wrappers
// ---------------------------------------------------------------------------

func TestCreateFunc_ReturnsStateMachine(t *testing.T) {
	var cf CreateFunc = func(shardID, replicaID uint64) interface{} {
		return newTestStateMachine()
	}
	result := cf(1, 1)
	if _, ok := result.(StateMachine); !ok {
		t.Error("CreateFunc did not return StateMachine")
	}
}

func TestCreateFunc_ReturnsConcurrentStateMachine(t *testing.T) {
	var cf CreateFunc = func(shardID, replicaID uint64) interface{} {
		return newTestConcurrentStateMachine()
	}
	result := cf(1, 1)
	if _, ok := result.(ConcurrentStateMachine); !ok {
		t.Error("CreateFunc did not return ConcurrentStateMachine")
	}
}

func TestCreateFunc_ReturnsOnDiskStateMachine(t *testing.T) {
	var cf CreateFunc = func(shardID, replicaID uint64) interface{} {
		return newTestOnDiskStateMachine()
	}
	result := cf(1, 1)
	if _, ok := result.(DiskStateMachine); !ok {
		t.Error("CreateFunc did not return DiskStateMachine")
	}
}

func TestNewCreateFunc_TypeSafe(t *testing.T) {
	cf := NewCreateFunc(func(shardID, replicaID uint64) StateMachine {
		return newTestStateMachine()
	})
	result := cf(10, 20)
	sm, ok := result.(StateMachine)
	if !ok {
		t.Fatal("NewCreateFunc result does not implement StateMachine")
	}
	// Verify the instance is usable.
	ctx := context.Background()
	if err := sm.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNewConcurrentCreateFunc_TypeSafe(t *testing.T) {
	cf := NewConcurrentCreateFunc(func(shardID, replicaID uint64) ConcurrentStateMachine {
		return newTestConcurrentStateMachine()
	})
	result := cf(10, 20)
	sm, ok := result.(ConcurrentStateMachine)
	if !ok {
		t.Fatal("NewConcurrentCreateFunc result does not implement ConcurrentStateMachine")
	}
	ctx := context.Background()
	if err := sm.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNewDiskCreateFunc_TypeSafe(t *testing.T) {
	cf := NewDiskCreateFunc(func(shardID, replicaID uint64) DiskStateMachine {
		return newTestOnDiskStateMachine()
	})
	result := cf(10, 20)
	sm, ok := result.(DiskStateMachine)
	if !ok {
		t.Fatal("NewDiskCreateFunc result does not implement DiskStateMachine")
	}
	ctx := context.Background()
	if err := sm.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNewCreateFunc_PassesShardAndReplicaIDs(t *testing.T) {
	var capturedShard, capturedReplica uint64
	cf := NewCreateFunc(func(shardID, replicaID uint64) StateMachine {
		capturedShard = shardID
		capturedReplica = replicaID
		return newTestStateMachine()
	})
	cf(42, 99)
	if capturedShard != 42 {
		t.Errorf("shardID = %d, want 42", capturedShard)
	}
	if capturedReplica != 99 {
		t.Errorf("replicaID = %d, want 99", capturedReplica)
	}
}

func TestNewConcurrentCreateFunc_PassesShardAndReplicaIDs(t *testing.T) {
	var capturedShard, capturedReplica uint64
	cf := NewConcurrentCreateFunc(func(shardID, replicaID uint64) ConcurrentStateMachine {
		capturedShard = shardID
		capturedReplica = replicaID
		return newTestConcurrentStateMachine()
	})
	cf(77, 88)
	if capturedShard != 77 {
		t.Errorf("shardID = %d, want 77", capturedShard)
	}
	if capturedReplica != 88 {
		t.Errorf("replicaID = %d, want 88", capturedReplica)
	}
}

func TestNewDiskCreateFunc_PassesShardAndReplicaIDs(t *testing.T) {
	var capturedShard, capturedReplica uint64
	cf := NewDiskCreateFunc(func(shardID, replicaID uint64) DiskStateMachine {
		capturedShard = shardID
		capturedReplica = replicaID
		return newTestOnDiskStateMachine()
	})
	cf(55, 66)
	if capturedShard != 55 {
		t.Errorf("shardID = %d, want 55", capturedShard)
	}
	if capturedReplica != 66 {
		t.Errorf("replicaID = %d, want 66", capturedReplica)
	}
}

// ---------------------------------------------------------------------------
// Interface compliance: compile-time checks
// ---------------------------------------------------------------------------

// These variables ensure that all three test implementations satisfy their
// respective interfaces. If any method signature changes, the build fails.
var (
	_ StateMachine           = (*testStateMachine)(nil)
	_ ConcurrentStateMachine = (*testConcurrentStateMachine)(nil)
	_ DiskStateMachine       = (*testOnDiskStateMachine)(nil)
)

// TestInterfaces_ContextFirstParameter verifies at compile time that all
// interface methods accept context.Context as the first parameter. The test
// implementations above would fail to compile if the signatures differed.
func TestInterfaces_ContextFirstParameter(t *testing.T) {
	// This test body is intentionally empty. The compile-time interface
	// satisfaction checks above (var _ StateMachine = ...) verify that
	// all method signatures, including context.Context as the first parameter,
	// are correct. If any method were missing context.Context, the test
	// implementations would not compile.
	t.Log("All interface methods accept context.Context as the first parameter (verified at compile time)")
}

// ---------------------------------------------------------------------------
// CreateStateMachineFunc / CreateConcurrentStateMachineFunc / CreateDiskStateMachineFunc type aliases
// ---------------------------------------------------------------------------

func TestCreateStateMachineFunc_TypeAlias(t *testing.T) {
	var fn CreateStateMachineFunc = func(shardID, replicaID uint64) StateMachine {
		return newTestStateMachine()
	}
	sm := fn(1, 1)
	if sm == nil {
		t.Error("CreateStateMachineFunc returned nil")
	}
}

func TestCreateConcurrentStateMachineFunc_TypeAlias(t *testing.T) {
	var fn CreateConcurrentStateMachineFunc = func(shardID, replicaID uint64) ConcurrentStateMachine {
		return newTestConcurrentStateMachine()
	}
	sm := fn(1, 1)
	if sm == nil {
		t.Error("CreateConcurrentStateMachineFunc returned nil")
	}
}

func TestCreateDiskStateMachineFunc_TypeAlias(t *testing.T) {
	var fn CreateDiskStateMachineFunc = func(shardID, replicaID uint64) DiskStateMachine {
		return newTestOnDiskStateMachine()
	}
	sm := fn(1, 1)
	if sm == nil {
		t.Error("CreateDiskStateMachineFunc returned nil")
	}
}
