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

// ---------------------------------------------------------------------------
// Types: Entry
// ---------------------------------------------------------------------------

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
// CreateFunc and type-safe wrappers
// ---------------------------------------------------------------------------

func TestNewCreateFunc_PassesShardAndReplicaIDs(t *testing.T) {
	// Each constructor passes the shard and replica IDs through to the user
	// factory and returns a value of the matching state machine type.
	tests := []struct {
		name string
		// build wraps a factory that records its arguments in the constructor
		// under test.
		build func(record func(shardID, replicaID uint64)) CreateFunc
		// isKind reports whether the created value has the expected type.
		isKind  func(v interface{}) bool
		shardID uint64
		replica uint64
	}{
		{
			name: "NewCreateFunc",
			build: func(record func(uint64, uint64)) CreateFunc {
				return NewCreateFunc(func(shardID, replicaID uint64) StateMachine {
					record(shardID, replicaID)
					return newTestStateMachine()
				})
			},
			isKind:  func(v interface{}) bool { _, ok := v.(StateMachine); return ok },
			shardID: 42, replica: 99,
		},
		{
			name: "NewConcurrentCreateFunc",
			build: func(record func(uint64, uint64)) CreateFunc {
				return NewConcurrentCreateFunc(func(shardID, replicaID uint64) ConcurrentStateMachine {
					record(shardID, replicaID)
					return newTestConcurrentStateMachine()
				})
			},
			isKind:  func(v interface{}) bool { _, ok := v.(ConcurrentStateMachine); return ok },
			shardID: 77, replica: 88,
		},
		{
			name: "NewDiskCreateFunc",
			build: func(record func(uint64, uint64)) CreateFunc {
				return NewDiskCreateFunc(func(shardID, replicaID uint64) DiskStateMachine {
					record(shardID, replicaID)
					return newTestOnDiskStateMachine()
				})
			},
			isKind:  func(v interface{}) bool { _, ok := v.(DiskStateMachine); return ok },
			shardID: 55, replica: 66,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedShard, capturedReplica uint64
			cf := tt.build(func(shardID, replicaID uint64) {
				capturedShard = shardID
				capturedReplica = replicaID
			})
			if !tt.isKind(cf(tt.shardID, tt.replica)) {
				t.Fatalf("%s result has the wrong state machine type", tt.name)
			}
			if capturedShard != tt.shardID {
				t.Errorf("shardID = %d, want %d", capturedShard, tt.shardID)
			}
			if capturedReplica != tt.replica {
				t.Errorf("replicaID = %d, want %d", capturedReplica, tt.replica)
			}
		})
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
