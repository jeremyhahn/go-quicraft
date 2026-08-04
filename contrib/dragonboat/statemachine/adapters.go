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
	"context"
	"errors"
	"io"

	qcSM "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// translateSnapshotControlError passes dragonboat's snapshot control-flow
// sentinels through unwrapped. ErrSnapshotStopped and ErrSnapshotAborted
// are not failures: dragonboat treats them as an orderly abort of an
// in-flight save (node shutting down, or the state machine declining).
// Wrapping them in AdapterSnapshotSaveError would report a shutdown as an
// application error.
//
// NOTE: quicraft has no public equivalent sentinel, so an aborted save
// still surfaces to the requester as a generic snapshot failure rather
// than as dragonboat's Terminated result. Preserving the sentinel is what
// the adapter can do without a change in the quicraft engine.
func translateSnapshotControlError(err error) error {
	if errors.Is(err, ErrSnapshotStopped) ||
		errors.Is(err, ErrSnapshotAborted) ||
		errors.Is(err, ErrSnapshotStreaming) {
		return err
	}
	return &AdapterSnapshotSaveError{Cause: err}
}

// translateRecoverControlError passes ErrSnapshotStopped through unwrapped
// for the same reason as translateSnapshotControlError: dragonboat's
// recoverAborted treats it as an orderly abort, not a recovery failure.
func translateRecoverControlError(err error) error {
	if errors.Is(err, ErrSnapshotStopped) {
		return err
	}
	return &AdapterSnapshotRecoverError{Cause: err}
}

// Compile-time interface assertions.
var (
	_ qcSM.StateMachine           = (*regularAdapter)(nil)
	_ qcSM.ConcurrentStateMachine = (*concurrentAdapter)(nil)
	_ qcSM.DiskStateMachine       = (*diskAdapter)(nil)
)

// regularAdapter wraps a dragonboat IStateMachine and implements
// the quicraft sm.StateMachine interface. Each entry in the batch
// is applied individually through the inner state machine's
// single-entry Update method.
type regularAdapter struct {
	inner IStateMachine
}

// Update applies committed entries one at a time through the inner
// IStateMachine.Update, converting each dragonboat Result back into
// the pre-allocated quicraft results slice.
func (a *regularAdapter) Update(_ context.Context, entries []qcSM.Entry, results []qcSM.Result) error {
	for i := range entries {
		dbEntry := Entry{
			Index: entries[i].Index,
			Cmd:   entries[i].Cmd,
		}
		res, err := a.inner.Update(dbEntry)
		if err != nil {
			return &AdapterUpdateError{Index: entries[i].Index, Cause: err}
		}
		results[i] = qcSM.Result{
			Value: res.Value,
			Data:  res.Data,
		}
	}
	return nil
}

// Lookup delegates to the inner IStateMachine.Lookup.
func (a *regularAdapter) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	result, err := a.inner.Lookup(query)
	if err != nil {
		return nil, &AdapterLookupError{Cause: err}
	}
	return result, nil
}

// SaveSnapshot delegates to the inner IStateMachine.SaveSnapshot.
// QuicRaft does not support external snapshot files: when the inner
// state machine adds files to the collection, the snapshot fails with
// ErrSnapshotFilesNotSupported instead of silently producing a snapshot
// that cannot restore the external data.
func (a *regularAdapter) SaveSnapshot(_ context.Context, w io.Writer, stopper <-chan struct{}) error {
	fc := &snapshotFileCollection{}
	if err := a.inner.SaveSnapshot(w, fc, stopper); err != nil {
		return translateSnapshotControlError(err)
	}
	if len(fc.files) > 0 {
		return &AdapterSnapshotSaveError{Cause: ErrSnapshotFilesNotSupported}
	}
	return nil
}

// RecoverFromSnapshot delegates to the inner IStateMachine.RecoverFromSnapshot
// with a nil files slice since quicraft does not support external snapshot files.
func (a *regularAdapter) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	err := a.inner.RecoverFromSnapshot(r, nil, stopper)
	if err != nil {
		return translateRecoverControlError(err)
	}
	return nil
}

// Close delegates to the inner IStateMachine.Close.
func (a *regularAdapter) Close(_ context.Context) error {
	err := a.inner.Close()
	if err != nil {
		return &AdapterCloseError{Cause: err}
	}
	return nil
}

// concurrentAdapter wraps a dragonboat IConcurrentStateMachine and
// implements the quicraft sm.ConcurrentStateMachine interface. Entries
// are batch-converted between the two representations.
type concurrentAdapter struct {
	inner IConcurrentStateMachine
}

// Update converts quicraft entries to dragonboat entries, delegates to
// the inner IConcurrentStateMachine.Update, then copies the in-place
// Entry.Result values back to the pre-allocated quicraft results slice.
func (a *concurrentAdapter) Update(_ context.Context, entries []qcSM.Entry, results []qcSM.Result) error {
	dbEntries := make([]Entry, len(entries))
	for i := range entries {
		dbEntries[i] = Entry{
			Index: entries[i].Index,
			Cmd:   entries[i].Cmd,
		}
	}
	updated, err := a.inner.Update(dbEntries)
	if err != nil {
		return &AdapterUpdateError{Index: 0, Cause: err}
	}
	// The user Update must return exactly one result per entry: a longer
	// slice would panic indexing results, a shorter one would silently
	// zero-fill the trailing proposals' results.
	if len(updated) != len(entries) {
		return &AdapterResultLengthError{Expected: len(entries), Actual: len(updated)}
	}
	for i := range updated {
		results[i] = qcSM.Result{
			Value: updated[i].Result.Value,
			Data:  updated[i].Result.Data,
		}
	}
	return nil
}

// Lookup delegates to the inner IConcurrentStateMachine.Lookup.
func (a *concurrentAdapter) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	result, err := a.inner.Lookup(query)
	if err != nil {
		return nil, &AdapterLookupError{Cause: err}
	}
	return result, nil
}

// PrepareSnapshot delegates to the inner IConcurrentStateMachine.PrepareSnapshot.
func (a *concurrentAdapter) PrepareSnapshot() (interface{}, error) {
	ctx, err := a.inner.PrepareSnapshot()
	if err != nil {
		return nil, &AdapterPrepareSnapshotError{Cause: err}
	}
	return ctx, nil
}

// SaveSnapshot delegates to the inner IConcurrentStateMachine.SaveSnapshot.
// QuicRaft does not support external snapshot files: when the inner
// state machine adds files to the collection, the snapshot fails with
// ErrSnapshotFilesNotSupported instead of silently producing a snapshot
// that cannot restore the external data.
func (a *concurrentAdapter) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	fc := &snapshotFileCollection{}
	if err := a.inner.SaveSnapshot(snapshotCtx, w, fc, stopper); err != nil {
		return translateSnapshotControlError(err)
	}
	if len(fc.files) > 0 {
		return &AdapterSnapshotSaveError{Cause: ErrSnapshotFilesNotSupported}
	}
	return nil
}

// RecoverFromSnapshot delegates to the inner IConcurrentStateMachine.RecoverFromSnapshot
// with a nil files slice.
func (a *concurrentAdapter) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	err := a.inner.RecoverFromSnapshot(r, nil, stopper)
	if err != nil {
		return translateRecoverControlError(err)
	}
	return nil
}

// Close delegates to the inner IConcurrentStateMachine.Close.
func (a *concurrentAdapter) Close(_ context.Context) error {
	err := a.inner.Close()
	if err != nil {
		return &AdapterCloseError{Cause: err}
	}
	return nil
}

// diskAdapter wraps a dragonboat IOnDiskStateMachine and implements
// the quicraft sm.DiskStateMachine interface.
type diskAdapter struct {
	inner IOnDiskStateMachine
}

// Open delegates to the inner IOnDiskStateMachine.Open. The initialDir
// parameter from quicraft is not forwarded because dragonboat's Open
// does not accept a directory argument; on-disk state machines in
// dragonboat manage their own storage paths.
func (a *diskAdapter) Open(_ context.Context, _ string, stopper <-chan struct{}) (uint64, error) {
	index, err := a.inner.Open(stopper)
	if err != nil {
		if errors.Is(err, ErrOpenStopped) {
			// Control flow, not a failure: the node is shutting down
			// before Open completed. Wrapping it would hide the sentinel
			// behind an adapter open failure.
			return 0, err
		}
		return 0, &AdapterOpenError{Cause: err}
	}
	return index, nil
}

// Update converts quicraft entries to dragonboat entries, delegates to
// the inner IOnDiskStateMachine.Update, then copies Entry.Result values
// back to the quicraft results slice.
func (a *diskAdapter) Update(_ context.Context, entries []qcSM.Entry, results []qcSM.Result) error {
	dbEntries := make([]Entry, len(entries))
	for i := range entries {
		dbEntries[i] = Entry{
			Index: entries[i].Index,
			Cmd:   entries[i].Cmd,
		}
	}
	updated, err := a.inner.Update(dbEntries)
	if err != nil {
		return &AdapterUpdateError{Index: 0, Cause: err}
	}
	// The user Update must return exactly one result per entry: a longer
	// slice would panic indexing results, a shorter one would silently
	// zero-fill the trailing proposals' results.
	if len(updated) != len(entries) {
		return &AdapterResultLengthError{Expected: len(entries), Actual: len(updated)}
	}
	for i := range updated {
		results[i] = qcSM.Result{
			Value: updated[i].Result.Value,
			Data:  updated[i].Result.Data,
		}
	}
	return nil
}

// Lookup delegates to the inner IOnDiskStateMachine.Lookup.
func (a *diskAdapter) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	result, err := a.inner.Lookup(query)
	if err != nil {
		return nil, &AdapterLookupError{Cause: err}
	}
	return result, nil
}

// Sync delegates to the inner IOnDiskStateMachine.Sync.
func (a *diskAdapter) Sync() error {
	err := a.inner.Sync()
	if err != nil {
		return &AdapterSyncError{Cause: err}
	}
	return nil
}

// PrepareSnapshot delegates to the inner IOnDiskStateMachine.PrepareSnapshot.
func (a *diskAdapter) PrepareSnapshot() (interface{}, error) {
	ctx, err := a.inner.PrepareSnapshot()
	if err != nil {
		return nil, &AdapterPrepareSnapshotError{Cause: err}
	}
	return ctx, nil
}

// SaveSnapshot delegates to the inner IOnDiskStateMachine.SaveSnapshot.
func (a *diskAdapter) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	err := a.inner.SaveSnapshot(snapshotCtx, w, stopper)
	if err != nil {
		return translateSnapshotControlError(err)
	}
	return nil
}

// RecoverFromSnapshot delegates to the inner IOnDiskStateMachine.RecoverFromSnapshot.
func (a *diskAdapter) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	err := a.inner.RecoverFromSnapshot(r, stopper)
	if err != nil {
		return translateRecoverControlError(err)
	}
	return nil
}

// Close delegates to the inner IOnDiskStateMachine.Close.
func (a *diskAdapter) Close(_ context.Context) error {
	err := a.inner.Close()
	if err != nil {
		return &AdapterCloseError{Cause: err}
	}
	return nil
}
