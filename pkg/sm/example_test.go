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

package sm_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// counter is an in-memory sm.StateMachine: every entry adds the first byte
// of its command to a running total. QuicRaft serializes Update and Lookup
// for this interface, so the type needs no locking of its own.
type counter struct {
	total uint64
}

var _ sm.StateMachine = (*counter)(nil)

func (c *counter) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if len(e.Cmd) == 0 {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		c.total += uint64(e.Cmd[0])
		results[i] = sm.Result{Value: c.total}
	}
	return nil
}

func (c *counter) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return c.total, nil
}

func (c *counter) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	return binary.Write(w, binary.LittleEndian, c.total)
}

func (c *counter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	return binary.Read(r, binary.LittleEndian, &c.total)
}

func (c *counter) Close(context.Context) error { return nil }

// ExampleStateMachine applies two entries to an in-memory state machine,
// snapshots it and restores the snapshot into a fresh replica.
func ExampleStateMachine() {
	ctx := context.Background()
	create := sm.NewCreateFunc(func(_, _ uint64) sm.StateMachine { return &counter{} })
	machine := create(1, 1).(sm.StateMachine)

	entries := []sm.Entry{{Index: 1, Cmd: []byte{2}}, {Index: 2, Cmd: []byte{3}}}
	results := make([]sm.Result, len(entries))
	if err := machine.Update(ctx, entries, results); err != nil {
		fmt.Println("update:", err)
		return
	}
	total, _ := machine.Lookup(ctx, nil)
	fmt.Println("results:", results[0].Value, results[1].Value, "total:", total)

	var snap bytes.Buffer
	if err := machine.SaveSnapshot(ctx, &snap, nil); err != nil {
		fmt.Println("save:", err)
		return
	}
	replica := create(1, 2).(sm.StateMachine)
	if err := replica.RecoverFromSnapshot(ctx, &snap, nil); err != nil {
		fmt.Println("recover:", err)
		return
	}
	restored, _ := replica.Lookup(ctx, nil)
	fmt.Println("restored:", restored)
	// Output:
	// results: 2 5 total: 5
	// restored: 5
}

// register is a sm.ConcurrentStateMachine: Lookup may run while Update
// applies, so the state lives in an atomic, and PrepareSnapshot captures a
// point-in-time copy that SaveSnapshot writes while updates continue.
type register struct {
	value atomic.Uint64
}

var _ sm.ConcurrentStateMachine = (*register)(nil)

func (r *register) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if len(e.Cmd) != 8 {
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		v := binary.LittleEndian.Uint64(e.Cmd)
		r.value.Store(v)
		results[i] = sm.Result{Value: v}
	}
	return nil
}

func (r *register) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return r.value.Load(), nil
}

func (r *register) PrepareSnapshot() (interface{}, error) {
	return r.value.Load(), nil
}

func (r *register) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, _ <-chan struct{}) error {
	v, ok := snapshotCtx.(uint64)
	if !ok {
		return fmt.Errorf("unexpected snapshot context %T", snapshotCtx)
	}
	return binary.Write(w, binary.LittleEndian, v)
}

func (r *register) RecoverFromSnapshot(_ context.Context, rd io.Reader, _ <-chan struct{}) error {
	var v uint64
	if err := binary.Read(rd, binary.LittleEndian, &v); err != nil {
		return err
	}
	r.value.Store(v)
	return nil
}

func (r *register) Close(context.Context) error { return nil }

// ExampleConcurrentStateMachine shows that the snapshot holds the state
// captured by PrepareSnapshot, not an update applied after it.
func ExampleConcurrentStateMachine() {
	ctx := context.Background()
	create := sm.NewConcurrentCreateFunc(func(_, _ uint64) sm.ConcurrentStateMachine { return &register{} })
	machine := create(1, 1).(sm.ConcurrentStateMachine)

	put := func(index, v uint64) {
		cmd := binary.LittleEndian.AppendUint64(nil, v)
		results := make([]sm.Result, 1)
		_ = machine.Update(ctx, []sm.Entry{{Index: index, Cmd: cmd}}, results)
	}
	put(1, 7)
	snapCtx, err := machine.PrepareSnapshot()
	if err != nil {
		fmt.Println("prepare:", err)
		return
	}
	put(2, 9)

	var snap bytes.Buffer
	if err := machine.SaveSnapshot(ctx, snapCtx, &snap, nil); err != nil {
		fmt.Println("save:", err)
		return
	}
	current, _ := machine.Lookup(ctx, nil)
	replica := create(1, 2).(sm.ConcurrentStateMachine)
	if err := replica.RecoverFromSnapshot(ctx, &snap, nil); err != nil {
		fmt.Println("recover:", err)
		return
	}
	restored, _ := replica.Lookup(ctx, nil)
	fmt.Println("current:", current, "restored:", restored)
	// Output:
	// current: 9 restored: 7
}

// fileStore is a sm.DiskStateMachine skeleton: it keeps the last applied
// index and a single value in a file under the directory QuicRaft assigns.
// Open reports the persisted index so the engine replays only later entries.
type fileStore struct {
	mu      sync.RWMutex
	path    string
	applied uint64
	value   []byte
}

var _ sm.DiskStateMachine = (*fileStore)(nil)

func (f *fileStore) Open(_ context.Context, initialDir string, _ <-chan struct{}) (uint64, error) {
	f.path = filepath.Join(initialDir, "state")
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := f.decode(data); err != nil {
		return 0, err
	}
	return f.applied, nil
}

func (f *fileStore) decode(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("state file too short: %d bytes", len(data))
	}
	f.applied = binary.LittleEndian.Uint64(data)
	f.value = append([]byte(nil), data[8:]...)
	return nil
}

func (f *fileStore) encode() []byte {
	return append(binary.LittleEndian.AppendUint64(nil, f.applied), f.value...)
}

func (f *fileStore) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, e := range entries {
		// Replay after a restart can resend entries the state already
		// holds: skip them, or the state would roll back to an older index.
		if e.Index <= f.applied {
			results[i] = sm.Result{}
			continue
		}
		f.value = append(f.value[:0], e.Cmd...)
		f.applied = e.Index
		results[i] = sm.Result{Value: uint64(len(e.Cmd))}
	}
	return nil
}

func (f *fileStore) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return string(f.value), nil
}

func (f *fileStore) Sync() error {
	f.mu.RLock()
	data := f.encode()
	f.mu.RUnlock()
	return writeStateFile(f.path, data)
}

// writeStateFile replaces path with data so that a crash leaves either the
// old file or the new one, never a partial file that Open would accept.
func writeStateFile(path string, data []byte) error {
	// Write to a temporary file in the same directory: rename is atomic
	// only within one file system.
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	// Fsync before the rename, or the rename can reach disk before the data.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	// Fsync the directory so the rename itself survives a crash.
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (f *fileStore) PrepareSnapshot() (interface{}, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.encode(), nil
}

func (f *fileStore) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, _ <-chan struct{}) error {
	data, ok := snapshotCtx.([]byte)
	if !ok {
		return fmt.Errorf("unexpected snapshot context %T", snapshotCtx)
	}
	_, err := w.Write(data)
	return err
}

func (f *fileStore) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.decode(data); err != nil {
		return err
	}
	return writeStateFile(f.path, data)
}

func (f *fileStore) Close(context.Context) error { return f.Sync() }

// ExampleDiskStateMachine applies an entry, closes the state machine and
// reopens it from the same directory, which reports the persisted index. It
// then replays entries at and below that index, which change nothing.
func ExampleDiskStateMachine() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "disk-sm-example")
	if err != nil {
		fmt.Println("tempdir:", err)
		return
	}
	defer os.RemoveAll(dir)

	create := sm.NewDiskCreateFunc(func(_, _ uint64) sm.DiskStateMachine { return &fileStore{} })
	machine := create(1, 1).(sm.DiskStateMachine)
	index, err := machine.Open(ctx, dir, nil)
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	fmt.Println("fresh index:", index)

	results := make([]sm.Result, 1)
	if err := machine.Update(ctx, []sm.Entry{{Index: 42, Cmd: []byte("hello")}}, results); err != nil {
		fmt.Println("update:", err)
		return
	}
	if err := machine.Close(ctx); err != nil {
		fmt.Println("close:", err)
		return
	}

	reopened := create(1, 1).(sm.DiskStateMachine)
	index, err = reopened.Open(ctx, dir, nil)
	if err != nil {
		fmt.Println("reopen:", err)
		return
	}
	value, _ := reopened.Lookup(ctx, nil)
	fmt.Println("reopened index:", index, "value:", value)

	// A replay of entries at or below the applied index leaves the state
	// unchanged, and the next Sync persists the same index.
	replay := []sm.Entry{{Index: 41, Cmd: []byte("older")}, {Index: 42, Cmd: []byte("hello")}}
	if err := reopened.Update(ctx, replay, make([]sm.Result, len(replay))); err != nil {
		fmt.Println("replay:", err)
		return
	}
	if err := reopened.Close(ctx); err != nil {
		fmt.Println("close:", err)
		return
	}
	again := create(1, 1).(sm.DiskStateMachine)
	index, err = again.Open(ctx, dir, nil)
	if err != nil {
		fmt.Println("reopen:", err)
		return
	}
	value, _ = again.Lookup(ctx, nil)
	fmt.Println("after replay index:", index, "value:", value)
	if err := again.Close(ctx); err != nil {
		fmt.Println("close:", err)
		return
	}
	// Output:
	// fresh index: 0
	// reopened index: 42 value: hello
	// after replay index: 42 value: hello
}
