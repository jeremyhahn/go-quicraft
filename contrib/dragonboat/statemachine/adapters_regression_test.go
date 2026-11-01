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
	"errors"
	"io"
	"testing"

	qcSM "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// External snapshot files must fail the snapshot loudly instead of
// being silently discarded (which would be unrecoverable data loss).
// ---------------------------------------------------------------------------

// fileAddingSM is an IStateMachine whose SaveSnapshot registers an
// external file, as dragonboat state machines with bulk out-of-stream
// data do.
type fileAddingSM struct {
	counterSM
}

func (s *fileAddingSM) SaveSnapshot(w io.Writer, files ISnapshotFileCollection, _ <-chan struct{}) error {
	files.AddFile(1, "/tmp/external-data", []byte("meta"))
	_, err := w.Write([]byte("stream"))
	return err
}

// fileAddingConcurrentSM is the IConcurrentStateMachine equivalent.
type fileAddingConcurrentSM struct {
	concurrentCounterSM
}

func (s *fileAddingConcurrentSM) SaveSnapshot(_ interface{}, w io.Writer, files ISnapshotFileCollection, _ <-chan struct{}) error {
	files.AddFile(7, "/tmp/external-data", nil)
	_, err := w.Write([]byte("stream"))
	return err
}

func TestRegularAdapter_SaveSnapshot_ExternalFilesFailLoudly(t *testing.T) {
	a := &regularAdapter{inner: &fileAddingSM{}}
	var buf bytes.Buffer
	err := a.SaveSnapshot(context.Background(), &buf, nil)
	if err == nil {
		t.Fatal("SaveSnapshot with external files must fail: quicraft cannot restore them")
	}
	if !errors.Is(err, ErrSnapshotFilesNotSupported) {
		t.Errorf("error = %v, want errors.Is(..., ErrSnapshotFilesNotSupported)", err)
	}
	var saveErr *AdapterSnapshotSaveError
	if !errors.As(err, &saveErr) {
		t.Errorf("error type = %T, want *AdapterSnapshotSaveError", err)
	}
}

func TestConcurrentAdapter_SaveSnapshot_ExternalFilesFailLoudly(t *testing.T) {
	a := &concurrentAdapter{inner: &fileAddingConcurrentSM{}}
	var buf bytes.Buffer
	err := a.SaveSnapshot(context.Background(), uint64(0), &buf, nil)
	if err == nil {
		t.Fatal("SaveSnapshot with external files must fail: quicraft cannot restore them")
	}
	if !errors.Is(err, ErrSnapshotFilesNotSupported) {
		t.Errorf("error = %v, want errors.Is(..., ErrSnapshotFilesNotSupported)", err)
	}
}

func TestSnapshotFileCollection_RecordsFiles(t *testing.T) {
	fc := &snapshotFileCollection{}
	fc.AddFile(1, "/a", []byte("m1"))
	fc.AddFile(2, "/b", nil)
	if len(fc.files) != 2 {
		t.Fatalf("len(files) = %d, want 2", len(fc.files))
	}
	if fc.files[0].FileID != 1 || fc.files[0].Filepath != "/a" || string(fc.files[0].Metadata) != "m1" {
		t.Errorf("first recorded file = %+v", fc.files[0])
	}
}

// ---------------------------------------------------------------------------
// Update result slices whose length differs from the entry batch
// must be rejected with a typed error instead of panicking (longer) or
// silently zero-filling trailing proposals (shorter).
// ---------------------------------------------------------------------------

// wrongLengthConcurrentSM returns an Update slice of a caller-chosen length.
type wrongLengthConcurrentSM struct {
	concurrentCounterSM
	returned int
}

func (s *wrongLengthConcurrentSM) Update(entries []Entry) ([]Entry, error) {
	out := make([]Entry, s.returned)
	for i := range out {
		out[i] = Entry{Result: Result{Value: uint64(i + 1)}}
	}
	return out, nil
}

// wrongLengthDiskSM is the IOnDiskStateMachine equivalent.
type wrongLengthDiskSM struct {
	diskCounterSM
	returned int
}

func (s *wrongLengthDiskSM) Update(entries []Entry) ([]Entry, error) {
	out := make([]Entry, s.returned)
	for i := range out {
		out[i] = Entry{Result: Result{Value: uint64(i + 1)}}
	}
	return out, nil
}

func assertResultLengthError(t *testing.T, err error, expected, actual int) {
	t.Helper()
	if err == nil {
		t.Fatal("expected AdapterResultLengthError, got nil")
	}
	var lenErr *AdapterResultLengthError
	if !errors.As(err, &lenErr) {
		t.Fatalf("error type = %T (%v), want *AdapterResultLengthError", err, err)
	}
	if lenErr.Expected != expected || lenErr.Actual != actual {
		t.Errorf("AdapterResultLengthError = %+v, want Expected=%d Actual=%d", lenErr, expected, actual)
	}
}

func TestConcurrentAdapter_Update_ShorterResultSliceRejected(t *testing.T) {
	a := &concurrentAdapter{inner: &wrongLengthConcurrentSM{returned: 1}}
	entries := []qcSM.Entry{{Index: 1}, {Index: 2}, {Index: 3}}
	results := make([]qcSM.Result, len(entries))
	err := a.Update(context.Background(), entries, results)
	assertResultLengthError(t, err, 3, 1)
}

func TestConcurrentAdapter_Update_LongerResultSliceRejected(t *testing.T) {
	a := &concurrentAdapter{inner: &wrongLengthConcurrentSM{returned: 5}}
	entries := []qcSM.Entry{{Index: 1}, {Index: 2}}
	results := make([]qcSM.Result, len(entries))
	// Under the previous behavior this panicked inside the apply
	// goroutine when indexing results beyond its length.
	err := a.Update(context.Background(), entries, results)
	assertResultLengthError(t, err, 2, 5)
}

func TestDiskAdapter_Update_ShorterResultSliceRejected(t *testing.T) {
	a := &diskAdapter{inner: &wrongLengthDiskSM{returned: 0}}
	entries := []qcSM.Entry{{Index: 1}, {Index: 2}}
	results := make([]qcSM.Result, len(entries))
	err := a.Update(context.Background(), entries, results)
	assertResultLengthError(t, err, 2, 0)
}

func TestDiskAdapter_Update_LongerResultSliceRejected(t *testing.T) {
	a := &diskAdapter{inner: &wrongLengthDiskSM{returned: 4}}
	entries := []qcSM.Entry{{Index: 1}}
	results := make([]qcSM.Result, len(entries))
	err := a.Update(context.Background(), entries, results)
	assertResultLengthError(t, err, 1, 4)
}

func TestAdapterResultLengthError_Message(t *testing.T) {
	err := &AdapterResultLengthError{Expected: 3, Actual: 1}
	want := "statemachine adapter: update returned 1 results for 3 entries"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}
