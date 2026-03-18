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

package statemachine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	qcSM "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// testUpdateError is a typed error for test state machines.
type testUpdateError struct {
	msg string
}

func (e *testUpdateError) Error() string { return e.msg }

// testLookupError is a typed error for test lookup failures.
type testLookupError struct {
	msg string
}

func (e *testLookupError) Error() string { return e.msg }

// testSnapshotError is a typed error for test snapshot failures.
type testSnapshotError struct {
	msg string
}

func (e *testSnapshotError) Error() string { return e.msg }

// testCloseError is a typed error for test close failures.
type testCloseError struct {
	msg string
}

func (e *testCloseError) Error() string { return e.msg }

// testOpenError is a typed error for test open failures.
type testOpenError struct {
	msg string
}

func (e *testOpenError) Error() string { return e.msg }

// testSyncError is a typed error for test sync failures.
type testSyncError struct {
	msg string
}

func (e *testSyncError) Error() string { return e.msg }

// testPrepareSnapshotError is a typed error for test prepare snapshot failures.
type testPrepareSnapshotError struct {
	msg string
}

func (e *testPrepareSnapshotError) Error() string { return e.msg }

// counterSM is a simple dragonboat IStateMachine that counts updates.
// Each Update increments the counter and returns the new value.
type counterSM struct {
	count    uint64
	closed   bool
	failNext bool
}

func (c *counterSM) Update(entry Entry) (Result, error) {
	if c.failNext {
		return Result{}, &testUpdateError{msg: "forced update error"}
	}
	c.count++
	return Result{Value: c.count, Data: entry.Cmd}, nil
}

func (c *counterSM) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(string); ok && q == "error" {
		return nil, &testLookupError{msg: "forced lookup error"}
	}
	return c.count, nil
}

func (c *counterSM) SaveSnapshot(w io.Writer, _ ISnapshotFileCollection, _ <-chan struct{}) error {
	return binary.Write(w, binary.LittleEndian, c.count)
}

func (c *counterSM) RecoverFromSnapshot(r io.Reader, _ []SnapshotFile, _ <-chan struct{}) error {
	return binary.Read(r, binary.LittleEndian, &c.count)
}

func (c *counterSM) Close() error {
	if c.closed {
		return &testCloseError{msg: "already closed"}
	}
	c.closed = true
	return nil
}

// concurrentCounterSM implements IConcurrentStateMachine for testing.
type concurrentCounterSM struct {
	count              uint64
	closed             bool
	failUpdate         bool
	failPrepare        bool
	failSave           bool
	failRecover        bool
	prepareSnapshotCtx interface{}
}

func (c *concurrentCounterSM) Update(entries []Entry) ([]Entry, error) {
	if c.failUpdate {
		return nil, &testUpdateError{msg: "forced concurrent update error"}
	}
	for i := range entries {
		c.count++
		entries[i].Result = Result{Value: c.count, Data: entries[i].Cmd}
	}
	return entries, nil
}

func (c *concurrentCounterSM) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(string); ok && q == "error" {
		return nil, &testLookupError{msg: "forced lookup error"}
	}
	return c.count, nil
}

func (c *concurrentCounterSM) PrepareSnapshot() (interface{}, error) {
	if c.failPrepare {
		return nil, &testPrepareSnapshotError{msg: "forced prepare error"}
	}
	c.prepareSnapshotCtx = c.count
	return c.count, nil
}

func (c *concurrentCounterSM) SaveSnapshot(ctx interface{}, w io.Writer, _ ISnapshotFileCollection, _ <-chan struct{}) error {
	if c.failSave {
		return &testSnapshotError{msg: "forced save error"}
	}
	count := ctx.(uint64)
	return binary.Write(w, binary.LittleEndian, count)
}

func (c *concurrentCounterSM) RecoverFromSnapshot(r io.Reader, _ []SnapshotFile, _ <-chan struct{}) error {
	if c.failRecover {
		return &testSnapshotError{msg: "forced recover error"}
	}
	return binary.Read(r, binary.LittleEndian, &c.count)
}

func (c *concurrentCounterSM) Close() error {
	if c.closed {
		return &testCloseError{msg: "already closed"}
	}
	c.closed = true
	return nil
}

// diskCounterSM implements IOnDiskStateMachine for testing.
type diskCounterSM struct {
	count       uint64
	opened      bool
	closed      bool
	synced      bool
	failOpen    bool
	failUpdate  bool
	failSync    bool
	failPrepare bool
	failSave    bool
	failRecover bool
}

func (d *diskCounterSM) Open(_ <-chan struct{}) (uint64, error) {
	if d.failOpen {
		return 0, &testOpenError{msg: "forced open error"}
	}
	d.opened = true
	return d.count, nil
}

func (d *diskCounterSM) Update(entries []Entry) ([]Entry, error) {
	if d.failUpdate {
		return nil, &testUpdateError{msg: "forced disk update error"}
	}
	for i := range entries {
		d.count++
		entries[i].Result = Result{Value: d.count, Data: entries[i].Cmd}
	}
	return entries, nil
}

func (d *diskCounterSM) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(string); ok && q == "error" {
		return nil, &testLookupError{msg: "forced lookup error"}
	}
	return d.count, nil
}

func (d *diskCounterSM) Sync() error {
	if d.failSync {
		return &testSyncError{msg: "forced sync error"}
	}
	d.synced = true
	return nil
}

func (d *diskCounterSM) PrepareSnapshot() (interface{}, error) {
	if d.failPrepare {
		return nil, &testPrepareSnapshotError{msg: "forced prepare error"}
	}
	return d.count, nil
}

func (d *diskCounterSM) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
	if d.failSave {
		return &testSnapshotError{msg: "forced save error"}
	}
	count := ctx.(uint64)
	return binary.Write(w, binary.LittleEndian, count)
}

func (d *diskCounterSM) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	if d.failRecover {
		return &testSnapshotError{msg: "forced recover error"}
	}
	return binary.Read(r, binary.LittleEndian, &d.count)
}

func (d *diskCounterSM) Close() error {
	if d.closed {
		return &testCloseError{msg: "already closed"}
	}
	d.closed = true
	return nil
}

// ---------------------------------------------------------------------------
// regularAdapter tests
// ---------------------------------------------------------------------------

func TestRegularAdapter_Update_SingleEntry(t *testing.T) {
	inner := &counterSM{}
	adapter := &regularAdapter{inner: inner}

	entries := []qcSM.Entry{{Index: 1, Cmd: []byte("hello")}}
	results := make([]qcSM.Result, 1)

	err := adapter.Update(context.Background(), entries, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 1 {
		t.Errorf("expected result value 1, got %d", results[0].Value)
	}
	if !bytes.Equal(results[0].Data, []byte("hello")) {
		t.Errorf("expected result data %q, got %q", "hello", results[0].Data)
	}
}

func TestRegularAdapter_Update_BatchEntries(t *testing.T) {
	inner := &counterSM{}
	adapter := &regularAdapter{inner: inner}

	entries := []qcSM.Entry{
		{Index: 1, Cmd: []byte("a")},
		{Index: 2, Cmd: []byte("b")},
		{Index: 3, Cmd: []byte("c")},
	}
	results := make([]qcSM.Result, 3)

	err := adapter.Update(context.Background(), entries, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := range results {
		expected := uint64(i + 1)
		if results[i].Value != expected {
			t.Errorf("results[%d].Value = %d, want %d", i, results[i].Value, expected)
		}
	}
	if !bytes.Equal(results[0].Data, []byte("a")) {
		t.Errorf("results[0].Data = %q, want %q", results[0].Data, "a")
	}
	if !bytes.Equal(results[1].Data, []byte("b")) {
		t.Errorf("results[1].Data = %q, want %q", results[1].Data, "b")
	}
	if !bytes.Equal(results[2].Data, []byte("c")) {
		t.Errorf("results[2].Data = %q, want %q", results[2].Data, "c")
	}
}

func TestRegularAdapter_Update_ErrorPropagation(t *testing.T) {
	inner := &counterSM{failNext: true}
	adapter := &regularAdapter{inner: inner}

	entries := []qcSM.Entry{{Index: 42, Cmd: []byte("fail")}}
	results := make([]qcSM.Result, 1)

	err := adapter.Update(context.Background(), entries, results)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var updateErr *AdapterUpdateError
	if !errors.As(err, &updateErr) {
		t.Fatalf("expected *AdapterUpdateError, got %T", err)
	}
	if updateErr.Index != 42 {
		t.Errorf("expected error index 42, got %d", updateErr.Index)
	}
	var cause *testUpdateError
	if !errors.As(updateErr.Cause, &cause) {
		t.Errorf("expected *testUpdateError cause, got %T", updateErr.Cause)
	}
}

func TestRegularAdapter_Update_EmptyBatch(t *testing.T) {
	inner := &counterSM{}
	adapter := &regularAdapter{inner: inner}

	err := adapter.Update(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if inner.count != 0 {
		t.Errorf("expected count 0 for empty batch, got %d", inner.count)
	}
}

func TestRegularAdapter_Lookup(t *testing.T) {
	inner := &counterSM{count: 42}
	adapter := &regularAdapter{inner: inner}

	result, err := adapter.Lookup(context.Background(), "count")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count, ok := result.(uint64)
	if !ok {
		t.Fatalf("expected uint64 result, got %T", result)
	}
	if count != 42 {
		t.Errorf("expected 42, got %d", count)
	}
}

func TestRegularAdapter_Lookup_ErrorPropagation(t *testing.T) {
	inner := &counterSM{}
	adapter := &regularAdapter{inner: inner}

	_, err := adapter.Lookup(context.Background(), "error")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var lookupErr *AdapterLookupError
	if !errors.As(err, &lookupErr) {
		t.Fatalf("expected *AdapterLookupError, got %T", err)
	}
}

func TestRegularAdapter_SaveSnapshot(t *testing.T) {
	inner := &counterSM{count: 100}
	adapter := &regularAdapter{inner: inner}

	var buf bytes.Buffer
	stopper := make(chan struct{})

	err := adapter.SaveSnapshot(context.Background(), &buf, stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected snapshot data, got empty buffer")
	}

	// Verify the snapshot data is the correct uint64 encoding.
	var restored uint64
	readErr := binary.Read(&buf, binary.LittleEndian, &restored)
	if readErr != nil {
		t.Fatalf("failed to read snapshot data: %v", readErr)
	}
	if restored != 100 {
		t.Errorf("expected snapshot value 100, got %d", restored)
	}
}

func TestRegularAdapter_SaveSnapshot_ErrorPropagation(t *testing.T) {
	// Use a writer that always fails to trigger an error from binary.Write.
	inner := &counterSM{count: 1}
	adapter := &regularAdapter{inner: inner}

	stopper := make(chan struct{})
	err := adapter.SaveSnapshot(context.Background(), &failWriter{}, stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var saveErr *AdapterSnapshotSaveError
	if !errors.As(err, &saveErr) {
		t.Fatalf("expected *AdapterSnapshotSaveError, got %T", err)
	}
}

func TestRegularAdapter_RecoverFromSnapshot(t *testing.T) {
	inner := &counterSM{count: 0}
	adapter := &regularAdapter{inner: inner}

	// Create snapshot data encoding value 77.
	var buf bytes.Buffer
	writeErr := binary.Write(&buf, binary.LittleEndian, uint64(77))
	if writeErr != nil {
		t.Fatalf("failed to write test snapshot: %v", writeErr)
	}

	stopper := make(chan struct{})
	err := adapter.RecoverFromSnapshot(context.Background(), &buf, stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inner.count != 77 {
		t.Errorf("expected count 77 after recovery, got %d", inner.count)
	}
}

func TestRegularAdapter_RecoverFromSnapshot_ErrorPropagation(t *testing.T) {
	inner := &counterSM{}
	adapter := &regularAdapter{inner: inner}

	// Empty reader will cause binary.Read to fail.
	stopper := make(chan struct{})
	err := adapter.RecoverFromSnapshot(context.Background(), &bytes.Buffer{}, stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var recoverErr *AdapterSnapshotRecoverError
	if !errors.As(err, &recoverErr) {
		t.Fatalf("expected *AdapterSnapshotRecoverError, got %T", err)
	}
}

func TestRegularAdapter_Close(t *testing.T) {
	inner := &counterSM{}
	adapter := &regularAdapter{inner: inner}

	err := adapter.Close(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inner.closed {
		t.Error("expected inner state machine to be closed")
	}
}

func TestRegularAdapter_Close_ErrorPropagation(t *testing.T) {
	inner := &counterSM{closed: true}
	adapter := &regularAdapter{inner: inner}

	err := adapter.Close(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var closeErr *AdapterCloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("expected *AdapterCloseError, got %T", err)
	}
}

// ---------------------------------------------------------------------------
// concurrentAdapter tests
// ---------------------------------------------------------------------------

func TestConcurrentAdapter_Update_BatchResults(t *testing.T) {
	inner := &concurrentCounterSM{}
	adapter := &concurrentAdapter{inner: inner}

	entries := []qcSM.Entry{
		{Index: 10, Cmd: []byte("x")},
		{Index: 11, Cmd: []byte("y")},
	}
	results := make([]qcSM.Result, 2)

	err := adapter.Update(context.Background(), entries, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 1 {
		t.Errorf("results[0].Value = %d, want 1", results[0].Value)
	}
	if results[1].Value != 2 {
		t.Errorf("results[1].Value = %d, want 2", results[1].Value)
	}
	if !bytes.Equal(results[0].Data, []byte("x")) {
		t.Errorf("results[0].Data = %q, want %q", results[0].Data, "x")
	}
	if !bytes.Equal(results[1].Data, []byte("y")) {
		t.Errorf("results[1].Data = %q, want %q", results[1].Data, "y")
	}
}

func TestConcurrentAdapter_Update_ErrorPropagation(t *testing.T) {
	inner := &concurrentCounterSM{failUpdate: true}
	adapter := &concurrentAdapter{inner: inner}

	entries := []qcSM.Entry{{Index: 1, Cmd: []byte("fail")}}
	results := make([]qcSM.Result, 1)

	err := adapter.Update(context.Background(), entries, results)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var updateErr *AdapterUpdateError
	if !errors.As(err, &updateErr) {
		t.Fatalf("expected *AdapterUpdateError, got %T", err)
	}
}

func TestConcurrentAdapter_Update_EmptyBatch(t *testing.T) {
	inner := &concurrentCounterSM{}
	adapter := &concurrentAdapter{inner: inner}

	err := adapter.Update(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
}

func TestConcurrentAdapter_Lookup(t *testing.T) {
	inner := &concurrentCounterSM{count: 99}
	adapter := &concurrentAdapter{inner: inner}

	result, err := adapter.Lookup(context.Background(), "count")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count, ok := result.(uint64)
	if !ok {
		t.Fatalf("expected uint64, got %T", result)
	}
	if count != 99 {
		t.Errorf("expected 99, got %d", count)
	}
}

func TestConcurrentAdapter_Lookup_ErrorPropagation(t *testing.T) {
	inner := &concurrentCounterSM{}
	adapter := &concurrentAdapter{inner: inner}

	_, err := adapter.Lookup(context.Background(), "error")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var lookupErr *AdapterLookupError
	if !errors.As(err, &lookupErr) {
		t.Fatalf("expected *AdapterLookupError, got %T", err)
	}
}

func TestConcurrentAdapter_PrepareSnapshot(t *testing.T) {
	inner := &concurrentCounterSM{count: 50}
	adapter := &concurrentAdapter{inner: inner}

	ctx, err := adapter.PrepareSnapshot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	snapCount, ok := ctx.(uint64)
	if !ok {
		t.Fatalf("expected uint64 snapshot context, got %T", ctx)
	}
	if snapCount != 50 {
		t.Errorf("expected snapshot context 50, got %d", snapCount)
	}
}

func TestConcurrentAdapter_PrepareSnapshot_ErrorPropagation(t *testing.T) {
	inner := &concurrentCounterSM{failPrepare: true}
	adapter := &concurrentAdapter{inner: inner}

	_, err := adapter.PrepareSnapshot()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var prepErr *AdapterPrepareSnapshotError
	if !errors.As(err, &prepErr) {
		t.Fatalf("expected *AdapterPrepareSnapshotError, got %T", err)
	}
}

func TestConcurrentAdapter_SaveSnapshot(t *testing.T) {
	inner := &concurrentCounterSM{count: 25}
	adapter := &concurrentAdapter{inner: inner}

	var buf bytes.Buffer
	stopper := make(chan struct{})

	err := adapter.SaveSnapshot(context.Background(), uint64(25), &buf, stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected snapshot data")
	}
}

func TestConcurrentAdapter_SaveSnapshot_ErrorPropagation(t *testing.T) {
	inner := &concurrentCounterSM{failSave: true}
	adapter := &concurrentAdapter{inner: inner}

	var buf bytes.Buffer
	stopper := make(chan struct{})

	err := adapter.SaveSnapshot(context.Background(), uint64(0), &buf, stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var saveErr *AdapterSnapshotSaveError
	if !errors.As(err, &saveErr) {
		t.Fatalf("expected *AdapterSnapshotSaveError, got %T", err)
	}
}

func TestConcurrentAdapter_RecoverFromSnapshot(t *testing.T) {
	inner := &concurrentCounterSM{}
	adapter := &concurrentAdapter{inner: inner}

	var buf bytes.Buffer
	writeErr := binary.Write(&buf, binary.LittleEndian, uint64(88))
	if writeErr != nil {
		t.Fatalf("failed to write test snapshot: %v", writeErr)
	}

	stopper := make(chan struct{})
	err := adapter.RecoverFromSnapshot(context.Background(), &buf, stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inner.count != 88 {
		t.Errorf("expected count 88, got %d", inner.count)
	}
}

func TestConcurrentAdapter_RecoverFromSnapshot_ErrorPropagation(t *testing.T) {
	inner := &concurrentCounterSM{failRecover: true}
	adapter := &concurrentAdapter{inner: inner}

	var buf bytes.Buffer
	writeErr := binary.Write(&buf, binary.LittleEndian, uint64(1))
	if writeErr != nil {
		t.Fatalf("failed to write test data: %v", writeErr)
	}

	stopper := make(chan struct{})
	err := adapter.RecoverFromSnapshot(context.Background(), &buf, stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var recoverErr *AdapterSnapshotRecoverError
	if !errors.As(err, &recoverErr) {
		t.Fatalf("expected *AdapterSnapshotRecoverError, got %T", err)
	}
}

func TestConcurrentAdapter_Close(t *testing.T) {
	inner := &concurrentCounterSM{}
	adapter := &concurrentAdapter{inner: inner}

	err := adapter.Close(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inner.closed {
		t.Error("expected inner state machine to be closed")
	}
}

func TestConcurrentAdapter_Close_ErrorPropagation(t *testing.T) {
	inner := &concurrentCounterSM{closed: true}
	adapter := &concurrentAdapter{inner: inner}

	err := adapter.Close(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var closeErr *AdapterCloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("expected *AdapterCloseError, got %T", err)
	}
}

// ---------------------------------------------------------------------------
// diskAdapter tests
// ---------------------------------------------------------------------------

func TestDiskAdapter_Open(t *testing.T) {
	inner := &diskCounterSM{count: 15}
	adapter := &diskAdapter{inner: inner}

	stopper := make(chan struct{})
	index, err := adapter.Open(context.Background(), "/tmp/test", stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if index != 15 {
		t.Errorf("expected index 15, got %d", index)
	}
	if !inner.opened {
		t.Error("expected inner state machine to be opened")
	}
}

func TestDiskAdapter_Open_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{failOpen: true}
	adapter := &diskAdapter{inner: inner}

	stopper := make(chan struct{})
	_, err := adapter.Open(context.Background(), "/tmp/test", stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var openErr *AdapterOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected *AdapterOpenError, got %T", err)
	}
}

func TestDiskAdapter_Update_BatchResults(t *testing.T) {
	inner := &diskCounterSM{}
	adapter := &diskAdapter{inner: inner}

	entries := []qcSM.Entry{
		{Index: 1, Cmd: []byte("d1")},
		{Index: 2, Cmd: []byte("d2")},
		{Index: 3, Cmd: []byte("d3")},
	}
	results := make([]qcSM.Result, 3)

	err := adapter.Update(context.Background(), entries, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := range results {
		expected := uint64(i + 1)
		if results[i].Value != expected {
			t.Errorf("results[%d].Value = %d, want %d", i, results[i].Value, expected)
		}
	}
}

func TestDiskAdapter_Update_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{failUpdate: true}
	adapter := &diskAdapter{inner: inner}

	entries := []qcSM.Entry{{Index: 1, Cmd: []byte("fail")}}
	results := make([]qcSM.Result, 1)

	err := adapter.Update(context.Background(), entries, results)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var updateErr *AdapterUpdateError
	if !errors.As(err, &updateErr) {
		t.Fatalf("expected *AdapterUpdateError, got %T", err)
	}
}

func TestDiskAdapter_Lookup(t *testing.T) {
	inner := &diskCounterSM{count: 33}
	adapter := &diskAdapter{inner: inner}

	result, err := adapter.Lookup(context.Background(), "count")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count, ok := result.(uint64)
	if !ok {
		t.Fatalf("expected uint64, got %T", result)
	}
	if count != 33 {
		t.Errorf("expected 33, got %d", count)
	}
}

func TestDiskAdapter_Lookup_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{}
	adapter := &diskAdapter{inner: inner}

	_, err := adapter.Lookup(context.Background(), "error")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var lookupErr *AdapterLookupError
	if !errors.As(err, &lookupErr) {
		t.Fatalf("expected *AdapterLookupError, got %T", err)
	}
}

func TestDiskAdapter_Sync(t *testing.T) {
	inner := &diskCounterSM{}
	adapter := &diskAdapter{inner: inner}

	err := adapter.Sync()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inner.synced {
		t.Error("expected inner state machine to be synced")
	}
}

func TestDiskAdapter_Sync_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{failSync: true}
	adapter := &diskAdapter{inner: inner}

	err := adapter.Sync()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var syncErr *AdapterSyncError
	if !errors.As(err, &syncErr) {
		t.Fatalf("expected *AdapterSyncError, got %T", err)
	}
}

func TestDiskAdapter_PrepareSnapshot(t *testing.T) {
	inner := &diskCounterSM{count: 60}
	adapter := &diskAdapter{inner: inner}

	ctx, err := adapter.PrepareSnapshot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	snapCount, ok := ctx.(uint64)
	if !ok {
		t.Fatalf("expected uint64, got %T", ctx)
	}
	if snapCount != 60 {
		t.Errorf("expected 60, got %d", snapCount)
	}
}

func TestDiskAdapter_PrepareSnapshot_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{failPrepare: true}
	adapter := &diskAdapter{inner: inner}

	_, err := adapter.PrepareSnapshot()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var prepErr *AdapterPrepareSnapshotError
	if !errors.As(err, &prepErr) {
		t.Fatalf("expected *AdapterPrepareSnapshotError, got %T", err)
	}
}

func TestDiskAdapter_SaveSnapshot(t *testing.T) {
	inner := &diskCounterSM{count: 45}
	adapter := &diskAdapter{inner: inner}

	var buf bytes.Buffer
	stopper := make(chan struct{})

	err := adapter.SaveSnapshot(context.Background(), uint64(45), &buf, stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected snapshot data")
	}
}

func TestDiskAdapter_SaveSnapshot_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{failSave: true}
	adapter := &diskAdapter{inner: inner}

	var buf bytes.Buffer
	stopper := make(chan struct{})

	err := adapter.SaveSnapshot(context.Background(), uint64(0), &buf, stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var saveErr *AdapterSnapshotSaveError
	if !errors.As(err, &saveErr) {
		t.Fatalf("expected *AdapterSnapshotSaveError, got %T", err)
	}
}

func TestDiskAdapter_RecoverFromSnapshot(t *testing.T) {
	inner := &diskCounterSM{}
	adapter := &diskAdapter{inner: inner}

	var buf bytes.Buffer
	writeErr := binary.Write(&buf, binary.LittleEndian, uint64(200))
	if writeErr != nil {
		t.Fatalf("failed to write test snapshot: %v", writeErr)
	}

	stopper := make(chan struct{})
	err := adapter.RecoverFromSnapshot(context.Background(), &buf, stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inner.count != 200 {
		t.Errorf("expected count 200, got %d", inner.count)
	}
}

func TestDiskAdapter_RecoverFromSnapshot_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{failRecover: true}
	adapter := &diskAdapter{inner: inner}

	var buf bytes.Buffer
	writeErr := binary.Write(&buf, binary.LittleEndian, uint64(1))
	if writeErr != nil {
		t.Fatalf("failed to write test data: %v", writeErr)
	}

	stopper := make(chan struct{})
	err := adapter.RecoverFromSnapshot(context.Background(), &buf, stopper)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var recoverErr *AdapterSnapshotRecoverError
	if !errors.As(err, &recoverErr) {
		t.Fatalf("expected *AdapterSnapshotRecoverError, got %T", err)
	}
}

func TestDiskAdapter_Close(t *testing.T) {
	inner := &diskCounterSM{}
	adapter := &diskAdapter{inner: inner}

	err := adapter.Close(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inner.closed {
		t.Error("expected inner state machine to be closed")
	}
}

func TestDiskAdapter_Close_ErrorPropagation(t *testing.T) {
	inner := &diskCounterSM{closed: true}
	adapter := &diskAdapter{inner: inner}

	err := adapter.Close(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var closeErr *AdapterCloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("expected *AdapterCloseError, got %T", err)
	}
}

// ---------------------------------------------------------------------------
// Factory tests
// ---------------------------------------------------------------------------

func TestWrapCreateFunc(t *testing.T) {
	factory := WrapCreateFunc(func(shardID, replicaID uint64) IStateMachine {
		return &counterSM{}
	})

	result := factory(1, 1)
	sm, ok := result.(qcSM.StateMachine)
	if !ok {
		t.Fatalf("expected qcSM.StateMachine, got %T", result)
	}

	entries := []qcSM.Entry{{Index: 1, Cmd: []byte("test")}}
	results := make([]qcSM.Result, 1)
	err := sm.Update(context.Background(), entries, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 1 {
		t.Errorf("expected result value 1, got %d", results[0].Value)
	}
}

func TestWrapCreateFunc_PassesIDs(t *testing.T) {
	var capturedShard, capturedReplica uint64
	factory := WrapCreateFunc(func(shardID, replicaID uint64) IStateMachine {
		capturedShard = shardID
		capturedReplica = replicaID
		return &counterSM{}
	})

	factory(42, 7)
	if capturedShard != 42 {
		t.Errorf("expected shardID 42, got %d", capturedShard)
	}
	if capturedReplica != 7 {
		t.Errorf("expected replicaID 7, got %d", capturedReplica)
	}
}

func TestWrapConcurrentCreateFunc(t *testing.T) {
	factory := WrapConcurrentCreateFunc(func(shardID, replicaID uint64) IConcurrentStateMachine {
		return &concurrentCounterSM{}
	})

	result := factory(1, 1)
	sm, ok := result.(qcSM.ConcurrentStateMachine)
	if !ok {
		t.Fatalf("expected qcSM.ConcurrentStateMachine, got %T", result)
	}

	entries := []qcSM.Entry{{Index: 1, Cmd: []byte("test")}}
	results := make([]qcSM.Result, 1)
	err := sm.Update(context.Background(), entries, results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 1 {
		t.Errorf("expected result value 1, got %d", results[0].Value)
	}
}

func TestWrapOnDiskCreateFunc(t *testing.T) {
	factory := WrapOnDiskCreateFunc(func(shardID, replicaID uint64) IOnDiskStateMachine {
		return &diskCounterSM{}
	})

	result := factory(1, 1)
	sm, ok := result.(qcSM.DiskStateMachine)
	if !ok {
		t.Fatalf("expected qcSM.DiskStateMachine, got %T", result)
	}

	stopper := make(chan struct{})
	index, err := sm.Open(context.Background(), "/tmp/test", stopper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if index != 0 {
		t.Errorf("expected index 0, got %d", index)
	}
}

// ---------------------------------------------------------------------------
// Error message tests
// ---------------------------------------------------------------------------

func TestAdapterUpdateError_Message(t *testing.T) {
	err := &AdapterUpdateError{Index: 123, Cause: &testUpdateError{msg: "boom"}}
	expected := "statemachine adapter: update failed at index: 123: boom"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

func TestAdapterUpdateError_Unwrap(t *testing.T) {
	cause := &testUpdateError{msg: "root cause"}
	err := &AdapterUpdateError{Index: 1, Cause: cause}
	if !errors.Is(err, cause) {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterLookupError_MessageAndUnwrap(t *testing.T) {
	cause := &testLookupError{msg: "bad query"}
	err := &AdapterLookupError{Cause: cause}
	expected := "statemachine adapter: lookup failed: bad query"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterSnapshotSaveError_MessageAndUnwrap(t *testing.T) {
	cause := &testSnapshotError{msg: "disk full"}
	err := &AdapterSnapshotSaveError{Cause: cause}
	expected := "statemachine adapter: snapshot save failed: disk full"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterSnapshotRecoverError_MessageAndUnwrap(t *testing.T) {
	cause := &testSnapshotError{msg: "corrupt data"}
	err := &AdapterSnapshotRecoverError{Cause: cause}
	expected := "statemachine adapter: snapshot recover failed: corrupt data"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterCloseError_MessageAndUnwrap(t *testing.T) {
	cause := &testCloseError{msg: "resource leak"}
	err := &AdapterCloseError{Cause: cause}
	expected := "statemachine adapter: close failed: resource leak"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterOpenError_MessageAndUnwrap(t *testing.T) {
	cause := &testOpenError{msg: "permission denied"}
	err := &AdapterOpenError{Cause: cause}
	expected := "statemachine adapter: open failed: permission denied"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterSyncError_MessageAndUnwrap(t *testing.T) {
	cause := &testSyncError{msg: "io error"}
	err := &AdapterSyncError{Cause: cause}
	expected := "statemachine adapter: sync failed: io error"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestAdapterPrepareSnapshotError_MessageAndUnwrap(t *testing.T) {
	cause := &testPrepareSnapshotError{msg: "state inconsistent"}
	err := &AdapterPrepareSnapshotError{Cause: cause}
	expected := "statemachine adapter: prepare snapshot failed: state inconsistent"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap did not return the cause")
	}
}

func TestUitoa_Zero(t *testing.T) {
	result := uitoa(0)
	if result != "0" {
		t.Errorf("expected %q, got %q", "0", result)
	}
}

func TestSnapshotFileCollection_NoOp(t *testing.T) {
	fc := &snapshotFileCollection{}
	// Verify AddFile does not panic.
	fc.AddFile(1, "/path", []byte("meta"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// failWriter is an io.Writer that always returns an error.
type failWriter struct{}

type failWriteError struct{}

func (e *failWriteError) Error() string { return "write failed" }

func (w *failWriter) Write(_ []byte) (int, error) {
	return 0, &failWriteError{}
}
