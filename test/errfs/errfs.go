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

// Package errfs provides a fault-injection filesystem for testing WAL I/O
// error handling. It wraps any waldb.FS implementation and injects a
// configurable error at the Nth file operation across all files opened
// through the ErrorFS instance.
//
// Usage:
//
//	efs := errfs.New(realFS, 5, io.ErrUnexpectedEOF)
//	// The 5th I/O operation (across any file) returns an InjectedError
//	// wrapping io.ErrUnexpectedEOF.
//
//	// Restrict injection to Write operations only:
//	efs := errfs.New(realFS, 3, io.ErrUnexpectedEOF).OnlyOps("Write")
//	// The counter increments for every operation, but the error fires only
//	// when the counter is >= 3 AND the operation is "Write".
//
// All FS-level operations (OpenFile, Open, MkdirAll, ReadDir, Stat, Remove)
// and all File-level operations (Write, Read, ReadAt, Stat, Truncate, Seek,
// Sync, Close, PreAllocate) share a single monotonic counter. When the
// counter reaches injectAt the configured error is returned; subsequent
// operations pass through normally unless Reset is called.
//
// Valid operation names for OnlyOps: "Write", "Read", "ReadAt", "Stat",
// "Truncate", "Seek", "Sync", "Close", "PreAllocate", "OpenFile", "Open",
// "MkdirAll", "ReadDir", "Remove".
package errfs

import (
	"fmt"
	"io/fs"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// InjectedError wraps the configured error with context about which
// operation triggered the injection. It implements the error interface and
// supports errors.Is/errors.As unwrapping to the underlying Err.
type InjectedError struct {
	// Operation is the name of the FS or File method that fired the injection,
	// e.g. "Write", "Sync", "OpenFile".
	Operation string

	// OpCount is the value of the operation counter at the moment the
	// injection fired (i.e. the injectAt value that was reached).
	OpCount int64

	// Err is the error that was supplied to New or Reset.
	Err error
}

// Error implements the error interface.
func (e *InjectedError) Error() string {
	return fmt.Sprintf("errfs: injected error at operation %d (%s): %v",
		e.OpCount, e.Operation, e.Err)
}

// Unwrap returns the underlying configured error, enabling errors.Is and
// errors.As to traverse the chain.
func (e *InjectedError) Unwrap() error {
	return e.Err
}

// ErrorFS wraps a waldb.FS and injects a caller-supplied error at a specific
// operation number. A counter shared across all files opened through this
// instance is incremented before every FS-level and File-level operation.
// When the counter reaches injectAt the operation returns an *InjectedError
// instead of delegating to the underlying implementation.
//
// A value of 0 for injectAt disables injection entirely (pure passthrough).
//
// ErrorFS is safe for concurrent use; the counter and injection-fired flag
// are managed with lock-free atomics.
type ErrorFS struct {
	delegate    waldb.FS
	injectedErr error

	// injectAt holds the 1-based operation number at which to inject the
	// error. 0 means disabled. Stored as int64 for atomic load/store.
	injectAt atomic.Int64

	// counter is the monotonically increasing count of operations executed
	// through this ErrorFS (including all ErrorFiles it has produced).
	counter atomic.Int64

	// fired is set to true the first time the injection fires.
	fired atomic.Bool

	// onlyOps restricts injection to operations whose name matches one of
	// these strings. When nil or empty, all operations are eligible (current
	// behavior).
	onlyOps map[string]struct{}
}

// New creates an ErrorFS that wraps delegate and injects err at the injectAt-th
// operation (1-based). Set injectAt to 0 to create a pure passthrough that
// can be armed later with Reset.
func New(delegate waldb.FS, injectAt int64, err error) *ErrorFS {
	efs := &ErrorFS{
		delegate:    delegate,
		injectedErr: err,
	}
	efs.injectAt.Store(injectAt)
	return efs
}

// OnlyOps restricts error injection to fire only when the operation name
// matches one of the provided names. Valid names: "Write", "Read", "ReadAt",
// "Stat", "Truncate", "Seek", "Sync", "Close", "PreAllocate", "OpenFile",
// "Open", "MkdirAll", "ReadDir", "Remove". Returns the ErrorFS for chaining.
//
// When set, the counter still increments for all operations, but the error
// only fires when the counter reaches injectAt AND the operation matches.
// If the counter reaches injectAt on a non-matching operation, the error is
// deferred to the next matching operation that increments the counter past
// injectAt.
func (efs *ErrorFS) OnlyOps(ops ...string) *ErrorFS {
	m := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		m[op] = struct{}{}
	}
	efs.onlyOps = m
	return efs
}

// ClearOpsFilter removes any operation-name filter set by OnlyOps, restoring
// the default behavior where all operations are eligible for injection.
func (efs *ErrorFS) ClearOpsFilter() {
	efs.onlyOps = nil
}

// Reset atomically reconfigures the injection point and clears both the
// operation counter and the fired flag. The onlyOps filter, if set via
// OnlyOps, is preserved so that a single ErrorFS can be reused across
// multiple test sub-cases without re-configuring the filter.
func (efs *ErrorFS) Reset(injectAt int64, err error) {
	efs.injectedErr = err
	efs.injectAt.Store(injectAt)
	efs.counter.Store(0)
	efs.fired.Store(false)
}

// Count returns the total number of operations executed through this ErrorFS
// and any ErrorFile it has produced.
func (efs *ErrorFS) Count() int64 {
	return efs.counter.Load()
}

// Injected returns true if the injection threshold has been reached at least
// once since the last Reset.
func (efs *ErrorFS) Injected() bool {
	return efs.fired.Load()
}

// tick increments the counter and checks whether injection should fire for
// the named operation. It returns a non-nil *InjectedError at most once per
// Reset cycle (guarded by the fired flag).
//
// When no onlyOps filter is configured the error fires the first time the
// counter reaches exactly injectAt, matching the original behavior.
//
// When onlyOps is set the counter still increments unconditionally, but the
// error is deferred until the counter has reached or exceeded injectAt AND
// the current operation is in the allowed set. This means the error fires on
// the first matching operation at or after the threshold.
func (efs *ErrorFS) tick(op string) error {
	n := efs.counter.Add(1)
	at := efs.injectAt.Load()
	if at <= 0 || efs.fired.Load() {
		return nil
	}

	if len(efs.onlyOps) == 0 {
		// Original behavior: fire at exactly the threshold operation.
		if n != at {
			return nil
		}
	} else {
		// Filtered behavior: fire at >= threshold AND operation matches.
		if n < at {
			return nil
		}
		if _, ok := efs.onlyOps[op]; !ok {
			return nil
		}
	}

	// Use CompareAndSwap to guarantee exactly one goroutine fires the error
	// even under concurrent access.
	if !efs.fired.CompareAndSwap(false, true) {
		return nil
	}
	return &InjectedError{
		Operation: op,
		OpCount:   n,
		Err:       efs.injectedErr,
	}
}

// OpenFile implements waldb.FS. It delegates to the underlying filesystem
// after counting the operation. If injection fires, no file is opened and
// the InjectedError is returned as the error value.
func (efs *ErrorFS) OpenFile(name string, flag int, perm fs.FileMode) (waldb.File, error) {
	if err := efs.tick("OpenFile"); err != nil {
		return nil, err
	}
	f, err := efs.delegate.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &ErrorFile{delegate: f, efs: efs}, nil
}

// Open implements waldb.FS. It delegates to the underlying filesystem after
// counting the operation.
func (efs *ErrorFS) Open(name string) (waldb.File, error) {
	if err := efs.tick("Open"); err != nil {
		return nil, err
	}
	f, err := efs.delegate.Open(name)
	if err != nil {
		return nil, err
	}
	return &ErrorFile{delegate: f, efs: efs}, nil
}

// MkdirAll implements waldb.FS. It delegates to the underlying filesystem
// after counting the operation.
func (efs *ErrorFS) MkdirAll(path string, perm fs.FileMode) error {
	if err := efs.tick("MkdirAll"); err != nil {
		return err
	}
	return efs.delegate.MkdirAll(path, perm)
}

// ReadDir implements waldb.FS. It delegates to the underlying filesystem
// after counting the operation.
func (efs *ErrorFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := efs.tick("ReadDir"); err != nil {
		return nil, err
	}
	return efs.delegate.ReadDir(name)
}

// Stat implements waldb.FS. It delegates to the underlying filesystem after
// counting the operation.
func (efs *ErrorFS) Stat(name string) (fs.FileInfo, error) {
	if err := efs.tick("Stat"); err != nil {
		return nil, err
	}
	return efs.delegate.Stat(name)
}

// Remove implements waldb.FS. It delegates to the underlying filesystem after
// counting the operation.
func (efs *ErrorFS) Remove(name string) error {
	if err := efs.tick("Remove"); err != nil {
		return err
	}
	return efs.delegate.Remove(name)
}

// ErrorFile wraps a waldb.File and routes every I/O operation through the
// parent ErrorFS counter. When the shared counter reaches the configured
// threshold the operation returns an *InjectedError without touching the
// underlying file.
//
// ErrorFile also implements waldb.FilePreAllocator when the wrapped delegate
// supports it; callers can use a type assertion to check for this capability.
type ErrorFile struct {
	delegate waldb.File
	efs      *ErrorFS
}

// Write implements waldb.File.
func (ef *ErrorFile) Write(p []byte) (int, error) {
	if err := ef.efs.tick("Write"); err != nil {
		return 0, err
	}
	return ef.delegate.Write(p)
}

// Read implements waldb.File.
func (ef *ErrorFile) Read(p []byte) (int, error) {
	if err := ef.efs.tick("Read"); err != nil {
		return 0, err
	}
	return ef.delegate.Read(p)
}

// ReadAt implements waldb.File.
func (ef *ErrorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := ef.efs.tick("ReadAt"); err != nil {
		return 0, err
	}
	return ef.delegate.ReadAt(p, off)
}

// Stat implements waldb.File.
func (ef *ErrorFile) Stat() (fs.FileInfo, error) {
	if err := ef.efs.tick("Stat"); err != nil {
		return nil, err
	}
	return ef.delegate.Stat()
}

// Truncate implements waldb.File.
func (ef *ErrorFile) Truncate(size int64) error {
	if err := ef.efs.tick("Truncate"); err != nil {
		return err
	}
	return ef.delegate.Truncate(size)
}

// Seek implements waldb.File.
func (ef *ErrorFile) Seek(offset int64, whence int) (int64, error) {
	if err := ef.efs.tick("Seek"); err != nil {
		return 0, err
	}
	return ef.delegate.Seek(offset, whence)
}

// Sync implements waldb.File.
func (ef *ErrorFile) Sync() error {
	if err := ef.efs.tick("Sync"); err != nil {
		return err
	}
	return ef.delegate.Sync()
}

// Close implements waldb.File.
func (ef *ErrorFile) Close() error {
	if err := ef.efs.tick("Close"); err != nil {
		return err
	}
	return ef.delegate.Close()
}

// PreAllocate implements waldb.FilePreAllocator when the wrapped delegate
// also implements it. Callers that need pre-allocation must type-assert the
// waldb.File returned by Open/OpenFile to waldb.FilePreAllocator.
//
// If the underlying delegate does not implement waldb.FilePreAllocator this
// method is still present on the concrete *ErrorFile type; the waldb package
// guards pre-allocation with a type assertion so the capability is
// transparently absent when the delegate lacks it.
func (ef *ErrorFile) PreAllocate(size int64) error {
	pa, ok := ef.delegate.(waldb.FilePreAllocator)
	if !ok {
		return &ErrPreAllocateUnsupported{Delegate: fmt.Sprintf("%T", ef.delegate)}
	}
	if err := ef.efs.tick("PreAllocate"); err != nil {
		return err
	}
	return pa.PreAllocate(size)
}

// ErrPreAllocateUnsupported is returned by ErrorFile.PreAllocate when the
// wrapped delegate does not implement waldb.FilePreAllocator. This mirrors
// the behaviour of calling PreAllocate on a non-supporting file: the caller
// should guard with a type assertion rather than calling unconditionally.
type ErrPreAllocateUnsupported struct {
	// Delegate is the type name of the wrapped file that lacks the capability.
	Delegate string
}

// Error implements the error interface.
func (e *ErrPreAllocateUnsupported) Error() string {
	return fmt.Sprintf("errfs: delegate %s does not implement FilePreAllocator", e.Delegate)
}

// compile-time interface assertions.
var (
	_ waldb.FS               = (*ErrorFS)(nil)
	_ waldb.File             = (*ErrorFile)(nil)
	_ waldb.FilePreAllocator = (*ErrorFile)(nil)
)
