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
	"context"
	"io"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// smWrapper is the internal interface that all three state machine wrapper
// types implement. The type switch happens once at construction time,
// eliminating interface dispatch on the hot path.
type smWrapper interface {
	Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
	Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	NALookup(key []byte) ([]byte, bool, error)          // returns (result, supported, error)
	NALookupInto(key, dst []byte) ([]byte, bool, error) // returns (result, supported, error)
	GetHash() (uint64, bool, error)                     // returns (hash, supported, error)
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	Close(ctx context.Context) error
	Type() SMType
}

// naLookup performs a byte-slice lookup if the underlying state machine
// implements sm.NALookup. Returns (nil, false, nil) if not supported, or
// (result, true, err) if supported (err may be non-nil).
func naLookup(underlying interface{}, key []byte) ([]byte, bool, error) {
	na, ok := underlying.(sm.NALookup)
	if !ok {
		return nil, false, nil
	}
	result, err := na.BytesLookup(key)
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// naLookupInto performs a byte-slice lookup into a caller-provided buffer
// if the underlying state machine implements sm.NALookupInto. Returns
// (nil, false, nil) if not supported, or (result, true, err) if supported.
// The returned slice aliases dst when the value fits; dst may be grown.
func naLookupInto(underlying interface{}, key, dst []byte) ([]byte, bool, error) {
	na, ok := underlying.(sm.NALookupInto)
	if !ok {
		return nil, false, nil
	}
	result, err := na.BytesLookupInto(key, dst)
	if err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// getHash returns the state machine hash if the underlying SM implements
// sm.Hasher. Returns (0, false, nil) if not supported, or (hash, true, err)
// if supported (err may be non-nil).
func getHash(underlying interface{}) (uint64, bool, error) {
	h, ok := underlying.(sm.Hasher)
	if !ok {
		return 0, false, nil
	}
	hash, err := h.GetHash()
	if err != nil {
		return 0, true, err
	}
	return hash, true, nil
}

// regularSMWrapper wraps a StateMachine with direct method calls.
type regularSMWrapper struct {
	sm sm.StateMachine
}

// Open is a no-op for in-memory state machines. Returns (0, nil).
func (w *regularSMWrapper) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, nil
}

// Update delegates to the wrapped StateMachine.
func (w *regularSMWrapper) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	return w.sm.Update(ctx, entries, results)
}

// Lookup delegates to the wrapped StateMachine.
func (w *regularSMWrapper) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	return w.sm.Lookup(ctx, query)
}

// NALookup performs a byte-slice lookup if the underlying StateMachine
// implements sm.NALookup. Returns (nil, false, nil) if not supported.
func (w *regularSMWrapper) NALookup(key []byte) ([]byte, bool, error) {
	return naLookup(w.sm, key)
}

// NALookupInto performs a byte-slice lookup into dst if the underlying
// StateMachine implements sm.NALookupInto. Returns (nil, false, nil) if
// not supported.
func (w *regularSMWrapper) NALookupInto(key, dst []byte) ([]byte, bool, error) {
	return naLookupInto(w.sm, key, dst)
}

// GetHash returns the state machine hash if the underlying SM implements
// sm.Hasher. Returns (0, false, nil) if not supported.
func (w *regularSMWrapper) GetHash() (uint64, bool, error) {
	return getHash(w.sm)
}

// SaveSnapshot delegates to the wrapped StateMachine.
func (w *regularSMWrapper) SaveSnapshot(ctx context.Context, wr io.Writer, stopper <-chan struct{}) error {
	return w.sm.SaveSnapshot(ctx, wr, stopper)
}

// RecoverFromSnapshot delegates to the wrapped StateMachine.
func (w *regularSMWrapper) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	return w.sm.RecoverFromSnapshot(ctx, r, stopper)
}

// Close delegates to the wrapped StateMachine.
func (w *regularSMWrapper) Close(ctx context.Context) error {
	return w.sm.Close(ctx)
}

// Type returns SMTypeRegular.
func (w *regularSMWrapper) Type() SMType {
	return SMTypeRegular
}

// concurrentSMWrapper wraps a ConcurrentStateMachine with direct method calls.
type concurrentSMWrapper struct {
	sm sm.ConcurrentStateMachine
}

// Open is a no-op for concurrent in-memory state machines. Returns (0, nil).
func (w *concurrentSMWrapper) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, nil
}

// Update delegates to the wrapped ConcurrentStateMachine.
func (w *concurrentSMWrapper) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	return w.sm.Update(ctx, entries, results)
}

// Lookup delegates to the wrapped ConcurrentStateMachine.
func (w *concurrentSMWrapper) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	return w.sm.Lookup(ctx, query)
}

// NALookup performs a byte-slice lookup if the underlying
// ConcurrentStateMachine implements sm.NALookup. Returns (nil, false, nil)
// if not supported.
func (w *concurrentSMWrapper) NALookup(key []byte) ([]byte, bool, error) {
	return naLookup(w.sm, key)
}

// NALookupInto performs a byte-slice lookup into dst if the underlying
// ConcurrentStateMachine implements sm.NALookupInto. Returns (nil, false, nil)
// if not supported.
func (w *concurrentSMWrapper) NALookupInto(key, dst []byte) ([]byte, bool, error) {
	return naLookupInto(w.sm, key, dst)
}

// GetHash returns the state machine hash if the underlying SM implements
// sm.Hasher. Returns (0, false, nil) if not supported.
func (w *concurrentSMWrapper) GetHash() (uint64, bool, error) {
	return getHash(w.sm)
}

// SaveSnapshot calls PrepareSnapshot then SaveSnapshot on the wrapped SM.
func (w *concurrentSMWrapper) SaveSnapshot(ctx context.Context, wr io.Writer, stopper <-chan struct{}) error {
	snapCtx, err := w.sm.PrepareSnapshot()
	if err != nil {
		return err
	}
	return w.sm.SaveSnapshot(ctx, snapCtx, wr, stopper)
}

// RecoverFromSnapshot delegates to the wrapped ConcurrentStateMachine.
func (w *concurrentSMWrapper) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	return w.sm.RecoverFromSnapshot(ctx, r, stopper)
}

// Close delegates to the wrapped ConcurrentStateMachine.
func (w *concurrentSMWrapper) Close(ctx context.Context) error {
	return w.sm.Close(ctx)
}

// Type returns SMTypeConcurrent.
func (w *concurrentSMWrapper) Type() SMType {
	return SMTypeConcurrent
}

// onDiskSMWrapper wraps a DiskStateMachine with direct method calls.
type onDiskSMWrapper struct {
	sm sm.DiskStateMachine
}

// Open delegates to the wrapped DiskStateMachine to initialize persistent
// storage and return the last applied index for crash recovery.
func (w *onDiskSMWrapper) Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error) {
	return w.sm.Open(ctx, initialDir, stopper)
}

// Update delegates to the wrapped DiskStateMachine.
func (w *onDiskSMWrapper) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	return w.sm.Update(ctx, entries, results)
}

// Lookup delegates to the wrapped DiskStateMachine.
func (w *onDiskSMWrapper) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	return w.sm.Lookup(ctx, query)
}

// NALookup performs a byte-slice lookup if the underlying DiskStateMachine
// implements sm.NALookup. Returns (nil, false, nil) if not supported.
func (w *onDiskSMWrapper) NALookup(key []byte) ([]byte, bool, error) {
	return naLookup(w.sm, key)
}

// NALookupInto performs a byte-slice lookup into dst if the underlying
// DiskStateMachine implements sm.NALookupInto. Returns (nil, false, nil)
// if not supported.
func (w *onDiskSMWrapper) NALookupInto(key, dst []byte) ([]byte, bool, error) {
	return naLookupInto(w.sm, key, dst)
}

// GetHash returns the state machine hash if the underlying SM implements
// sm.Hasher. Returns (0, false, nil) if not supported.
func (w *onDiskSMWrapper) GetHash() (uint64, bool, error) {
	return getHash(w.sm)
}

// SaveSnapshot syncs, prepares, and saves the on-disk state machine.
func (w *onDiskSMWrapper) SaveSnapshot(ctx context.Context, wr io.Writer, stopper <-chan struct{}) error {
	if err := w.sm.Sync(); err != nil {
		return err
	}
	snapCtx, err := w.sm.PrepareSnapshot()
	if err != nil {
		return err
	}
	return w.sm.SaveSnapshot(ctx, snapCtx, wr, stopper)
}

// RecoverFromSnapshot delegates to the wrapped DiskStateMachine.
func (w *onDiskSMWrapper) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	return w.sm.RecoverFromSnapshot(ctx, r, stopper)
}

// Close delegates to the wrapped DiskStateMachine.
func (w *onDiskSMWrapper) Close(ctx context.Context) error {
	return w.sm.Close(ctx)
}

// Type returns SMTypeOnDisk.
func (w *onDiskSMWrapper) Type() SMType {
	return SMTypeOnDisk
}
