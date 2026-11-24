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

// Package errfs is a fault-injection waldb.FS that fails the Nth operation across all files, with ShortWrite, LoseUnsyncedData, Times and OnlyOps controls.
package errfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// Operation names accepted by OnlyOps and reported in InjectedError.Operation.
// Path-level and handle-level operations that share a syscall family are named
// apart (Stat vs FileStat, Sync vs SyncMetadata) so a filter can target one
// without arming the other.
const (
	// OpOpenFile is FS.OpenFile.
	OpOpenFile = "OpenFile"
	// OpOpen is FS.Open.
	OpOpen = "Open"
	// OpMkdirAll is FS.MkdirAll.
	OpMkdirAll = "MkdirAll"
	// OpReadDir is FS.ReadDir.
	OpReadDir = "ReadDir"
	// OpStat is FS.Stat — a stat by PATH, not the fstat of an open handle.
	OpStat = "Stat"
	// OpRemove is FS.Remove.
	OpRemove = "Remove"
	// OpRename is FS.Rename, the publication step of the WAL format marker.
	OpRename = "Rename"

	// OpWrite is File.Write.
	OpWrite = "Write"
	// OpWriteAt is File.WriteAt, the only path by which a WAL record reaches
	// the medium (segment.padAndFlushBlock and segment.flushBlock).
	OpWriteAt = "WriteAt"
	// OpRead is File.Read.
	OpRead = "Read"
	// OpReadAt is File.ReadAt.
	OpReadAt = "ReadAt"
	// OpFileStat is File.Stat — the fstat of an open handle.
	OpFileStat = "FileStat"
	// OpTruncate is File.Truncate.
	OpTruncate = "Truncate"
	// OpSeek is File.Seek.
	OpSeek = "Seek"
	// OpSync is File.Sync — fdatasync on the production Linux osFile.
	OpSync = "Sync"
	// OpSyncMetadata is File.SyncMetadata, the full fsync for directories; see waldb.FileMetadataSyncer.
	OpSyncMetadata = "SyncMetadata"
	// OpClose is File.Close.
	OpClose = "Close"
	// OpPreAllocate is File.PreAllocate.
	OpPreAllocate = "PreAllocate"
)

// knownOps is the set OnlyOps validates against.
var knownOps = map[string]struct{}{
	OpOpenFile: {}, OpOpen: {}, OpMkdirAll: {}, OpReadDir: {}, OpStat: {},
	OpRemove: {}, OpRename: {}, OpWrite: {}, OpWriteAt: {}, OpRead: {},
	OpReadAt: {}, OpFileStat: {}, OpTruncate: {}, OpSeek: {}, OpSync: {},
	OpSyncMetadata: {}, OpClose: {}, OpPreAllocate: {},
}

// InjectedError wraps the configured error with context about which
// operation triggered the injection. It implements the error interface and
// supports errors.Is/errors.As unwrapping to the underlying Err.
type InjectedError struct {
	// Operation is the name of the FS or File method that fired the injection,
	// e.g. "WriteAt", "Sync", "OpenFile".
	Operation string

	// OpCount is the value of the operation counter at the moment the
	// injection fired.
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

// UnknownOpError reports a name passed to OnlyOps that is not an Op*
// constant. It is raised as a panic rather than returned: an unrecognised name
// matches nothing, so the injection silently never fires and every assertion
// the test makes about the fault becomes vacuously true.
type UnknownOpError struct {
	// Name is the unrecognised operation name.
	Name string
}

// Error implements the error interface.
func (e *UnknownOpError) Error() string {
	return fmt.Sprintf("errfs: %q is not a known operation name; use the errfs.Op* constants", e.Name)
}

// LossReplayError reports that the LoseUnsyncedData mode could not restore the
// pre-sync image of a file. The modelled fault did not happen, so the test that
// armed it must fail rather than proceed against a filesystem in a state no
// kernel produces.
type LossReplayError struct {
	// Stage names the step that failed: "stat", "restore", "truncate" or
	// "sync".
	Stage string

	// Err is the underlying failure.
	Err error
}

// Error implements the error interface.
func (e *LossReplayError) Error() string {
	return fmt.Sprintf("errfs: replaying unsynced-data loss failed at %s: %v", e.Stage, e.Err)
}

// Unwrap returns the underlying error.
func (e *LossReplayError) Unwrap() error {
	return e.Err
}

// faultMode selects what a firing operation does to the delegate before it
// reports the error.
type faultMode uint8

const (
	// modeError returns the error without touching the delegate.
	modeError faultMode = iota
	// modeShortWrite writes a prefix of the buffer and then returns the error.
	modeShortWrite
	// modeLoseUnsynced discards everything written since the last successful
	// sync and then returns the error.
	modeLoseUnsynced
)

// injectSpec is the complete injection configuration. It is replaced
// wholesale under an atomic pointer so a reconfiguration is never observed
// half-applied by a concurrent operation.
type injectSpec struct {
	at      int64
	err     error
	onlyOps map[string]struct{}
	mode    faultMode
	shortN  int
	times   int64
}

// clone returns a copy the caller may mutate before publishing it.
func (s *injectSpec) clone() *injectSpec {
	c := *s
	return &c
}

// eligible reports whether an operation named op, seen at counter value n, is
// a candidate for injection under this spec.
func (s *injectSpec) eligible(op string, n int64) bool {
	if s.at <= 0 || n < s.at {
		return false
	}
	if len(s.onlyOps) == 0 {
		return true
	}
	_, ok := s.onlyOps[op]
	return ok
}

// fault describes a firing injection: the error to report and how the delegate
// must be disturbed first.
type fault struct {
	err    *InjectedError
	mode   faultMode
	shortN int
}

// ErrorFS wraps a waldb.FS and injects a fault at operation number injectAt (0 disables); safe for concurrent use.
type ErrorFS struct {
	delegate waldb.FS

	// spec holds the current configuration. Never nil after New.
	spec atomic.Pointer[injectSpec]

	// counter is the monotonically increasing count of operations executed
	// through this ErrorFS (including all files it has produced).
	counter atomic.Int64

	// fires counts the injections that have fired since the last Reset.
	fires atomic.Int64
}

// New creates an ErrorFS that wraps delegate and injects err at the injectAt-th
// operation (1-based). Set injectAt to 0 to create a pure passthrough that
// can be armed later with Reset.
func New(delegate waldb.FS, injectAt int64, err error) *ErrorFS {
	efs := &ErrorFS{delegate: delegate}
	// State the mode explicitly rather than rely on the zero value.
	efs.spec.Store(&injectSpec{at: injectAt, err: err, times: 1, mode: modeError})
	return efs
}

// OnlyOps restricts injection to the given Op* operations, deferring a fault to the next match; it panics on an unknown name.
func (efs *ErrorFS) OnlyOps(ops ...string) *ErrorFS {
	m := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		if _, ok := knownOps[op]; !ok {
			panic(&UnknownOpError{Name: op})
		}
		m[op] = struct{}{}
	}
	next := efs.spec.Load().clone()
	next.onlyOps = m
	efs.spec.Store(next)
	return efs
}

// ClearOpsFilter removes any operation-name filter set by OnlyOps, restoring
// the default behavior where all operations are eligible for injection.
func (efs *ErrorFS) ClearOpsFilter() {
	next := efs.spec.Load().clone()
	next.onlyOps = nil
	efs.spec.Store(next)
}

// Times sets how many faults may fire: 1 by default, 0 unlimited.
func (efs *ErrorFS) Times(n int64) *ErrorFS {
	next := efs.spec.Load().clone()
	next.times = n
	efs.spec.Store(next)
	return efs
}

// ShortWrite makes a firing write transfer the first n bytes, clamped, then report the error.
func (efs *ErrorFS) ShortWrite(n int) *ErrorFS {
	next := efs.spec.Load().clone()
	next.mode = modeShortWrite
	next.shortN = n
	efs.spec.Store(next)
	return efs
}

// LoseUnsyncedData makes a firing sync discard everything written since the last successful sync, durably, then report the error.
func (efs *ErrorFS) LoseUnsyncedData() *ErrorFS {
	next := efs.spec.Load().clone()
	next.mode = modeLoseUnsynced
	efs.spec.Store(next)
	return efs
}

// Reset atomically reconfigures the injection point and clears both the
// operation counter and the fire budget. The ops filter, fault mode and
// Times budget are preserved, so a single ErrorFS can be reused across
// multiple test sub-cases without re-configuring them.
func (efs *ErrorFS) Reset(injectAt int64, err error) {
	next := efs.spec.Load().clone()
	next.at = injectAt
	next.err = err
	efs.spec.Store(next)
	efs.counter.Store(0)
	efs.fires.Store(0)
}

// Count returns the total number of operations executed through this ErrorFS
// and any file it has produced.
func (efs *ErrorFS) Count() int64 {
	return efs.counter.Load()
}

// Injected returns true if a fault has fired at least once since the last
// Reset.
func (efs *ErrorFS) Injected() bool {
	return efs.fires.Load() > 0
}

// Fires returns the number of faults that have fired since the last Reset.
func (efs *ErrorFS) Fires() int64 {
	return efs.fires.Load()
}

// tracksLoss reports whether the armed configuration can still fire a
// data-loss fault, which is what makes the per-file undo journal necessary.
func (efs *ErrorFS) tracksLoss() bool {
	spec := efs.spec.Load()
	if spec.mode != modeLoseUnsynced || spec.at <= 0 {
		return false
	}
	return spec.times <= 0 || efs.fires.Load() < spec.times
}

// tick increments the counter and reports the fault the named operation must
// raise, or nil when it passes through. Firing consumes one unit of the
// Times budget; the budget is claimed with CompareAndSwap so concurrent
// operations cannot overrun it.
func (efs *ErrorFS) tick(op string) *fault {
	n := efs.counter.Add(1)
	spec := efs.spec.Load()
	if !spec.eligible(op, n) {
		return nil
	}
	for {
		fired := efs.fires.Load()
		if spec.times > 0 && fired >= spec.times {
			return nil
		}
		if efs.fires.CompareAndSwap(fired, fired+1) {
			break
		}
	}
	return &fault{
		err:    &InjectedError{Operation: op, OpCount: n, Err: spec.err},
		mode:   spec.mode,
		shortN: spec.shortN,
	}
}

// OpenFile implements waldb.FS. It delegates to the underlying filesystem
// after counting the operation. If injection fires, no file is opened and
// the InjectedError is returned as the error value.
func (efs *ErrorFS) OpenFile(name string, flag int, perm fs.FileMode) (waldb.File, error) {
	if f := efs.tick(OpOpenFile); f != nil {
		return nil, f.err
	}
	file, err := efs.delegate.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return efs.wrap(file), nil
}

// Open implements waldb.FS. It delegates to the underlying filesystem after
// counting the operation.
func (efs *ErrorFS) Open(name string) (waldb.File, error) {
	if f := efs.tick(OpOpen); f != nil {
		return nil, f.err
	}
	file, err := efs.delegate.Open(name)
	if err != nil {
		return nil, err
	}
	return efs.wrap(file), nil
}

// MkdirAll implements waldb.FS. It delegates to the underlying filesystem
// after counting the operation.
func (efs *ErrorFS) MkdirAll(path string, perm fs.FileMode) error {
	if f := efs.tick(OpMkdirAll); f != nil {
		return f.err
	}
	return efs.delegate.MkdirAll(path, perm)
}

// ReadDir implements waldb.FS. It delegates to the underlying filesystem
// after counting the operation.
func (efs *ErrorFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f := efs.tick(OpReadDir); f != nil {
		return nil, f.err
	}
	return efs.delegate.ReadDir(name)
}

// Stat implements waldb.FS. It delegates to the underlying filesystem after
// counting the operation.
func (efs *ErrorFS) Stat(name string) (fs.FileInfo, error) {
	if f := efs.tick(OpStat); f != nil {
		return nil, f.err
	}
	return efs.delegate.Stat(name)
}

// Remove implements waldb.FS. It delegates to the underlying filesystem after
// counting the operation.
func (efs *ErrorFS) Remove(name string) error {
	if f := efs.tick(OpRemove); f != nil {
		return f.err
	}
	return efs.delegate.Remove(name)
}

// Rename implements waldb.FS.
func (efs *ErrorFS) Rename(oldpath, newpath string) error {
	if f := efs.tick(OpRename); f != nil {
		return f.err
	}
	return efs.delegate.Rename(oldpath, newpath)
}

// wrap returns a waldb.File with exactly the delegate's optional capabilities.
func (efs *ErrorFS) wrap(file waldb.File) waldb.File {
	ef := &ErrorFile{delegate: file, efs: efs}
	_, preAlloc := file.(waldb.FilePreAllocator)
	_, metaSync := file.(waldb.FileMetadataSyncer)
	switch {
	case preAlloc && metaSync:
		return &preAllocMetaSyncFile{ErrorFile: ef}
	case preAlloc:
		return &preAllocFile{ErrorFile: ef}
	case metaSync:
		return &metaSyncFile{ErrorFile: ef}
	default:
		return ef
	}
}

// undoRecord is the pre-image of one write, kept so LoseUnsyncedData can put
// the file back the way the last successful sync left it. pre holds the bytes
// that occupied [off, off+len(pre)) before the write; a write that extended
// the file past the durable size contributes no pre-image, because the
// truncation back to that size removes it.
type undoRecord struct {
	off int64
	pre []byte
}

// ErrorFile wraps a waldb.File and routes every operation through the ErrorFS counter; capability wrappers embed it.
type ErrorFile struct {
	delegate waldb.File
	efs      *ErrorFS

	// mu guards the undo journal, which is written by the write path and read
	// by the sync path.
	mu sync.Mutex
	// journal holds the pre-images of the writes made since the last
	// successful sync, in write order. Populated only while a data-loss fault
	// is armed.
	journal []undoRecord
	// durableSize is the file size the last successful sync left behind, and
	// the length LoseUnsyncedData truncates back to. Valid only when
	// durableKnown is set.
	durableSize  int64
	durableKnown bool
}

// Write implements waldb.File.
func (ef *ErrorFile) Write(p []byte) (int, error) {
	f := ef.efs.tick(OpWrite)
	if f == nil {
		ef.recordUndoAtCursor(len(p))
		return ef.delegate.Write(p)
	}
	if f.mode != modeShortWrite {
		return 0, f.err
	}
	n := clampShort(f.shortN, len(p))
	ef.recordUndoAtCursor(n)
	written, err := ef.delegate.Write(p[:n])
	if err != nil {
		return written, err
	}
	return written, f.err
}

// WriteAt implements waldb.File.
func (ef *ErrorFile) WriteAt(p []byte, off int64) (int, error) {
	f := ef.efs.tick(OpWriteAt)
	if f == nil {
		ef.recordUndo(off, len(p))
		return ef.delegate.WriteAt(p, off)
	}
	if f.mode != modeShortWrite {
		return 0, f.err
	}
	n := clampShort(f.shortN, len(p))
	ef.recordUndo(off, n)
	written, err := ef.delegate.WriteAt(p[:n], off)
	if err != nil {
		return written, err
	}
	return written, f.err
}

// Read implements waldb.File.
func (ef *ErrorFile) Read(p []byte) (int, error) {
	if f := ef.efs.tick(OpRead); f != nil {
		return 0, f.err
	}
	return ef.delegate.Read(p)
}

// ReadAt implements waldb.File.
func (ef *ErrorFile) ReadAt(p []byte, off int64) (int, error) {
	if f := ef.efs.tick(OpReadAt); f != nil {
		return 0, f.err
	}
	return ef.delegate.ReadAt(p, off)
}

// Stat implements waldb.File. Its operation name is OpFileStat: it is the
// fstat of this handle, not the path stat FS.Stat performs.
func (ef *ErrorFile) Stat() (fs.FileInfo, error) {
	if f := ef.efs.tick(OpFileStat); f != nil {
		return nil, f.err
	}
	return ef.delegate.Stat()
}

// Truncate implements waldb.File.
func (ef *ErrorFile) Truncate(size int64) error {
	if f := ef.efs.tick(OpTruncate); f != nil {
		return f.err
	}
	return ef.delegate.Truncate(size)
}

// Seek implements waldb.File.
func (ef *ErrorFile) Seek(offset int64, whence int) (int64, error) {
	if f := ef.efs.tick(OpSeek); f != nil {
		return 0, f.err
	}
	return ef.delegate.Seek(offset, whence)
}

// Sync implements waldb.File.
func (ef *ErrorFile) Sync() error {
	return ef.sync(OpSync, ef.delegate.Sync)
}

// Close implements waldb.File. The delegate is closed on every path: close(2)
// releases the descriptor even when it reports an error, so a wrapper that
// skipped it would model a state Linux cannot produce and leak the handle for
// the rest of the test.
func (ef *ErrorFile) Close() error {
	f := ef.efs.tick(OpClose)
	err := ef.delegate.Close()
	if f != nil {
		return f.err
	}
	return err
}

// sync runs one flush operation under the named op, applying the data-loss
// mode when the fault fires. doSync performs the real flush.
func (ef *ErrorFile) sync(op string, doSync func() error) error {
	f := ef.efs.tick(op)
	if f == nil {
		if err := doSync(); err != nil {
			return err
		}
		ef.markDurable()
		return nil
	}
	if f.mode != modeLoseUnsynced {
		return f.err
	}
	if err := ef.dropUnsynced(); err != nil {
		return err
	}
	return f.err
}

// preAllocate counts the operation before consulting the delegate.
func (ef *ErrorFile) preAllocate(size int64) error {
	if f := ef.efs.tick(OpPreAllocate); f != nil {
		return f.err
	}
	return ef.delegate.(waldb.FilePreAllocator).PreAllocate(size)
}

// syncMetadata implements the metadata-sync capability for the wrappers whose
// delegate has it.
func (ef *ErrorFile) syncMetadata() error {
	ms := ef.delegate.(waldb.FileMetadataSyncer)
	return ef.sync(OpSyncMetadata, ms.SyncMetadata)
}

// markDurable records the post-sync size and drops the undo journal.
func (ef *ErrorFile) markDurable() {
	if !ef.efs.tracksLoss() {
		return
	}
	ef.mu.Lock()
	defer ef.mu.Unlock()
	ef.journal = nil
	ef.durableKnown = false
}

// recordUndo captures the bytes a write of n bytes at off is about to
// overwrite, so a failed sync can put them back. It is a no-op unless a
// data-loss fault is armed.
func (ef *ErrorFile) recordUndo(off int64, n int) {
	if n <= 0 || !ef.efs.tracksLoss() {
		return
	}
	ef.mu.Lock()
	defer ef.mu.Unlock()
	if !ef.durableKnown {
		fi, err := ef.delegate.Stat()
		if err != nil {
			// The pre-image cannot be captured, so the journal is incomplete
			// and must not be used. dropUnsynced reports this as a replay
			// failure rather than pretending the loss was modelled.
			ef.journal = append(ef.journal, undoRecord{off: -1})
			return
		}
		ef.durableSize = fi.Size()
		ef.durableKnown = true
	}
	// Only the part of the write that lands below the durable size has a
	// pre-image; everything above it disappears with the truncation.
	overlap := ef.durableSize - off
	if overlap <= 0 {
		ef.journal = append(ef.journal, undoRecord{off: off})
		return
	}
	if overlap > int64(n) {
		overlap = int64(n)
	}
	pre := make([]byte, overlap)
	if _, err := ef.delegate.ReadAt(pre, off); err != nil && !errors.Is(err, io.EOF) {
		ef.journal = append(ef.journal, undoRecord{off: -1})
		return
	}
	ef.journal = append(ef.journal, undoRecord{off: off, pre: pre})
}

// recordUndoAtCursor is recordUndo for the position-carrying Write path. The
// cursor is read from the delegate, which is where Write is about to land.
func (ef *ErrorFile) recordUndoAtCursor(n int) {
	if n <= 0 || !ef.efs.tracksLoss() {
		return
	}
	off, err := ef.delegate.Seek(0, io.SeekCurrent)
	if err != nil {
		ef.mu.Lock()
		ef.journal = append(ef.journal, undoRecord{off: -1})
		ef.mu.Unlock()
		return
	}
	ef.recordUndo(off, n)
}

// dropUnsynced puts the file back the way the last successful sync left it and
// makes that image durable, modelling the dirty pages a failed fsync discards.
func (ef *ErrorFile) dropUnsynced() error {
	ef.mu.Lock()
	defer ef.mu.Unlock()

	for i := len(ef.journal) - 1; i >= 0; i-- {
		rec := ef.journal[i]
		if rec.off < 0 {
			return &LossReplayError{Stage: "stat", Err: errUndoIncomplete}
		}
		if len(rec.pre) == 0 {
			continue
		}
		if _, err := ef.delegate.WriteAt(rec.pre, rec.off); err != nil {
			return &LossReplayError{Stage: "restore", Err: err}
		}
	}
	if ef.durableKnown {
		if err := ef.delegate.Truncate(ef.durableSize); err != nil {
			return &LossReplayError{Stage: "truncate", Err: err}
		}
	}
	// The loss has to outlive the process that suffered it, so the restored
	// image is flushed for real. Without this the reverted bytes are
	// themselves unsynced and a later flush could publish either image.
	if err := ef.delegate.Sync(); err != nil {
		return &LossReplayError{Stage: "sync", Err: err}
	}
	ef.journal = nil
	ef.durableKnown = false
	return nil
}

// errUndoIncomplete reports that a pre-image could not be captured, so the
// modelled loss cannot be replayed faithfully.
var errUndoIncomplete = errors.New("errfs: pre-image of an unsynced write was not captured")

// clampShort bounds a configured short-write length to the buffer.
func clampShort(n, size int) int {
	if n < 0 {
		return 0
	}
	if n > size {
		return size
	}
	return n
}

// preAllocFile is the wrapper for a delegate that pre-allocates but syncs
// metadata through Sync.
type preAllocFile struct {
	*ErrorFile
}

// PreAllocate implements waldb.FilePreAllocator.
func (f *preAllocFile) PreAllocate(size int64) error { return f.preAllocate(size) }

// metaSyncFile is the wrapper for a delegate that distinguishes metadata syncs
// but cannot pre-allocate.
type metaSyncFile struct {
	*ErrorFile
}

// SyncMetadata implements waldb.FileMetadataSyncer.
func (f *metaSyncFile) SyncMetadata() error { return f.syncMetadata() }

// preAllocMetaSyncFile is the wrapper for a delegate carrying both optional
// capabilities, which is what the production osFile is.
type preAllocMetaSyncFile struct {
	*ErrorFile
}

// PreAllocate implements waldb.FilePreAllocator.
func (f *preAllocMetaSyncFile) PreAllocate(size int64) error { return f.preAllocate(size) }

// SyncMetadata implements waldb.FileMetadataSyncer.
func (f *preAllocMetaSyncFile) SyncMetadata() error { return f.syncMetadata() }

// compile-time interface assertions.
var (
	_ waldb.FS   = (*ErrorFS)(nil)
	_ waldb.File = (*ErrorFile)(nil)

	_ waldb.File               = (*preAllocFile)(nil)
	_ waldb.FilePreAllocator   = (*preAllocFile)(nil)
	_ waldb.File               = (*metaSyncFile)(nil)
	_ waldb.FileMetadataSyncer = (*metaSyncFile)(nil)
	_ waldb.File               = (*preAllocMetaSyncFile)(nil)
	_ waldb.FilePreAllocator   = (*preAllocMetaSyncFile)(nil)
	_ waldb.FileMetadataSyncer = (*preAllocMetaSyncFile)(nil)
)
