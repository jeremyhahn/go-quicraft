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

package waldb

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// crashFS is an in-memory FS that models POSIX crash semantics rather than
// I/O errors. Every other test double in this package injects an error, which
// can only exercise error HANDLING: a missing fsync produces no failing call
// to intercept, and a SIGKILL leaves write(2) data alive in the page cache, so
// neither can detect a deleted flush. crashFS closes that gap.
//
// The model:
//
//   - Each file carries a LIVE image and a DURABLE image. Write/WriteAt/
//     Truncate/PreAllocate mutate LIVE only. Sync promotes LIVE to DURABLE.
//   - A directory entry (file or subdirectory creation, and file removal)
//     becomes durable only when the PARENT directory is fsynced, matching the
//     production syncDirFS discipline.
//   - Crash discards all LIVE state: every surviving file reverts to its
//     DURABLE image, creations never covered by a parent fsync disappear
//     together with everything beneath them, and removals never covered by a
//     parent fsync are resurrected at their DURABLE content.
//
// The whole image lives in the crashFS value, so reopening a DB over the same
// crashFS after Crash sees exactly the post-crash filesystem.
//
// Faithfulness constraint: this model observes durability only through
// File.Sync. That is sound for waldb because every production flush — segment
// data (segment.Sync, DiscardPending, truncateCorruptSegment,
// truncateClosedSegment, truncateOrphanToZero) and directory entries
// (syncDirFS) — goes through the File interface. osFile.Sync is the only
// caller of the raw fdatasync/fsync syscall wrapper.
type crashFS struct {
	mu    sync.Mutex
	gen   uint64
	nodes map[string]*crashNode
	// tombs holds nodes unlinked from the live image whose removal is not yet
	// durable; Crash resurrects them.
	tombs map[string]*crashNode
	hook  atomic.Pointer[crashHook]
	// wb, when set, replaces the all-or-nothing loss of unsynced bytes with a
	// per-page verdict; see crashWriteback.
	wb atomic.Pointer[crashWriteback]
	// The write log records the byte ranges handed to WriteAt, so a test can
	// find out WHERE an in-flight batch landed and name the pages it wants the
	// crash to swallow. It is off by default and costs nothing when off.
	wlogMu sync.Mutex
	wlogOn bool
	wlog   []crashWrite
}

// crashDefaultPageSize is the writeback granularity used when a test enables
// partial writeback without naming one. Real writeback is per page-cache page.
const crashDefaultPageSize = 4096

// crashWriteback models the fact that the page cache does NOT flush a file's
// dirty pages as a unit.
//
// Without it crashFS loses every unsynced byte of every file, which can only
// ever produce a clean truncation of the tail. That is one shape of power loss,
// and it is the shape the durability-sequence scheme (see record.go) does not
// need: a WAL that simply ends early has no damage under acknowledged data to
// classify. The shape the scheme EXISTS for is the other one — a batch spanning
// several blocks issues one WriteAt per block before a single fdatasync, and
// writeback may persist an arbitrary SUBSET of those pages, leaving decodable
// records sitting above a zero-filled hole. Recovery must read that as an
// interrupted write and truncate, not as damage under acknowledged data and
// refuse to open. Modelling it requires losing page N while keeping page N+1,
// which the whole-file model cannot express at all.
//
// retained reports whether the page's CURRENT content reached the medium. A
// page that did not reverts to the file's durable image, exactly as a real lost
// writeback leaves the previously persisted content in place — which is why an
// acknowledged byte sharing a page with an in-flight one is never destroyed by
// this model, only the in-flight byte is. Pages that never existed in the
// durable image read back as zeros: a hole.
type crashWriteback struct {
	pageSize int
	retained func(path string, page int) bool
}

// crashWrite is one recorded WriteAt.
type crashWrite struct {
	path string
	off  int64
	n    int
}

// crashNode is one filesystem object. Directories carry no content; only
// their entry durability matters.
type crashNode struct {
	dir     bool
	live    []byte
	durable []byte
	// entryDurable reports whether this node's own directory entry has been
	// made durable by an fsync of its parent directory.
	entryDurable bool
}

// crashOp identifies the filesystem operation a crashHook intercepts.
type crashOp int

const (
	crashOpCreate crashOp = iota
	crashOpWriteAt
	crashOpTruncate
	crashOpSync
	crashOpDirSync
	crashOpRemove
	// crashOpWriteAtDone fires AFTER a WriteAt has applied. Every other op
	// fires before, which is what a hook wanting to cut power mid-operation
	// needs; a hook wanting to model writeback flushing what was just written
	// needs the opposite, and firing before would flush the state as it was one
	// write earlier.
	crashOpWriteAtDone
)

// crashHook runs immediately BEFORE the intercepted operation applies, and
// outside the crashFS lock, so a hook may call Crash to cut power in the
// middle of a write path. The operation that fired the hook then fails with
// errCrashedFS, exactly as if it had never reached the medium.
type crashHook struct {
	fn func(op crashOp, path string)
}

// errCrashedFS is returned by any handle obtained before a Crash. A file
// handle cannot survive the power loss it models, so continued use of one
// must fail rather than mutate the post-crash image.
var errCrashedFS = errors.New("waldb: file handle used after simulated crash")

// newCrashFS returns an empty crashFS whose seeded directories (and all their
// ancestors) already exist durably, modelling a mounted filesystem the WAL
// root is created inside.
func newCrashFS(seeded ...string) *crashFS {
	c := &crashFS{
		nodes: make(map[string]*crashNode),
		tombs: make(map[string]*crashNode),
	}
	for _, dir := range seeded {
		path := filepath.Clean(dir)
		for {
			c.nodes[path] = &crashNode{dir: true, entryDurable: true}
			parent := filepath.Dir(path)
			if parent == path {
				break
			}
			path = parent
		}
	}
	return c
}

// setHook installs (or with nil clears) the crash hook.
func (c *crashFS) setHook(fn func(op crashOp, path string)) {
	if fn == nil {
		c.hook.Store(nil)
		return
	}
	c.hook.Store(&crashHook{fn: fn})
}

func (c *crashFS) fire(op crashOp, path string) {
	if h := c.hook.Load(); h != nil {
		h.fn(op, path)
	}
}

// setWriteback installs (or with a nil predicate clears) the per-page
// writeback verdict the next Crash applies. pageSize <= 0 selects
// crashDefaultPageSize.
func (c *crashFS) setWriteback(pageSize int, retained func(path string, page int) bool) {
	if retained == nil {
		c.wb.Store(nil)
		return
	}
	if pageSize <= 0 {
		pageSize = crashDefaultPageSize
	}
	c.wb.Store(&crashWriteback{pageSize: pageSize, retained: retained})
}

// recordWrites turns the WriteAt log on or off, discarding whatever it held.
func (c *crashFS) recordWrites(on bool) {
	c.wlogMu.Lock()
	defer c.wlogMu.Unlock()
	c.wlogOn = on
	c.wlog = nil
}

// writesTo returns the recorded WriteAt ranges issued against path, in order.
func (c *crashFS) writesTo(path string) []crashWrite {
	target := filepath.Clean(path)
	c.wlogMu.Lock()
	defer c.wlogMu.Unlock()
	var out []crashWrite
	for _, w := range c.wlog {
		if w.path == target {
			out = append(out, w)
		}
	}
	return out
}

// Crash discards every byte and every directory entry that was not made
// durable, and invalidates all outstanding handles.
func (c *crashFS) Crash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++

	// Evaluate the durability chain against the pre-crash tree, then delete:
	// a node is lost when its own entry, or any ancestor's entry, never
	// reached stable storage.
	var doomed []string
	for path := range c.nodes {
		if !c.entryChainDurableLocked(path) {
			doomed = append(doomed, path)
		}
	}
	for _, path := range doomed {
		delete(c.nodes, path)
	}
	// The post-crash image becomes BOTH the live and the durable image. A real
	// medium has no second copy: what survived the power loss is what is on the
	// disk, so a LATER crash with no intervening Sync restores exactly this and
	// not some earlier state. Leaving durable behind would let a second crash
	// resurrect content the first one had already overwritten or discarded,
	// which is the model lying in the direction that hides bugs.
	wb := c.wb.Load()
	for path, n := range c.nodes {
		n.durable = postCrashImage(path, n, wb)
		n.live = append([]byte(nil), n.durable...)
	}
	for path, n := range c.tombs {
		if c.entryChainDurableLocked(filepath.Dir(path)) {
			n.live = append([]byte(nil), n.durable...)
			c.nodes[path] = n
		}
	}
	c.tombs = make(map[string]*crashNode)
}

// postCrashImage computes what a file reads back as after power is cut.
//
// With no writeback model installed this is the whole-file rule crashFS has
// always applied: every unsynced byte is gone and the file reverts to its
// durable image. With one installed the verdict is taken page by page, so a
// dirty page that reached the medium survives even when the page below it did
// not — the interleaving a real page-cache flush is free to produce, and the
// one the durability sequence exists to classify.
//
// A page that did not reach the medium keeps whatever the DURABLE image held
// there, which is what a real lost writeback leaves behind. Where the durable
// image does not reach, it reads as zeros, because nothing was ever written
// there: that is the hole a surviving higher page then sits above.
func postCrashImage(path string, n *crashNode, wb *crashWriteback) []byte {
	if n.dir || wb == nil {
		return append([]byte(nil), n.durable...)
	}
	ps := wb.pageSize
	// The post-crash file reaches as far as the durable image did, plus any
	// retained page beyond it.
	size := len(n.durable)
	for page := 0; page*ps < len(n.live); page++ {
		if !wb.retained(path, page) {
			continue
		}
		if end := min((page+1)*ps, len(n.live)); end > size {
			size = end
		}
	}
	img := make([]byte, size)
	copy(img, n.durable)
	for page := 0; page*ps < size; page++ {
		if !wb.retained(path, page) {
			continue
		}
		lo := page * ps
		hi := min(min(lo+ps, size), len(n.live))
		copy(img[lo:hi], n.live[lo:hi])
	}
	return img
}

// entryChainDurableLocked reports whether path and every ancestor inside the
// modelled tree has a durable directory entry.
func (c *crashFS) entryChainDurableLocked(path string) bool {
	for {
		n, ok := c.nodes[path]
		if !ok {
			return true
		}
		if !n.entryDurable {
			return false
		}
		parent := filepath.Dir(path)
		if parent == path {
			return true
		}
		path = parent
	}
}

// Writeback promotes a file's current LIVE image to DURABLE without any fsync,
// modelling the page cache writing dirty pages back on its own schedule.
//
// It is the event crashWriteback cannot express. crashWriteback is a verdict
// applied AT the crash, computed from the live image as it stands then; real
// writeback happens at an arbitrary EARLIER moment, and what it wrote stays on
// the medium even if the file is subsequently truncated. That ordering is
// exactly what makes a rollback's fsync load-bearing: bytes a failed batch
// wrote can reach the disk on their own, so truncating them out of the live
// image is not enough — the truncation itself has to be made durable, or the
// crash brings them back.
func (c *crashFS) Writeback(path string) {
	name := filepath.Clean(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[name]; ok && !n.dir {
		n.durable = append([]byte(nil), n.live...)
	}
}

// generation returns the current crash generation.
func (c *crashFS) generation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// OpenFile implements FS.
func (c *crashFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	path := filepath.Clean(name)
	gen := c.generation()
	if flag&os.O_CREATE != 0 {
		c.fire(crashOpCreate, path)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A hook that crashed mid-operation fences the caller out: the process
	// that issued this call no longer exists.
	if gen != c.gen {
		return nil, errCrashedFS
	}

	n, ok := c.nodes[path]
	if !ok {
		if flag&os.O_CREATE == 0 {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		if _, perr := c.dirLocked(filepath.Dir(path)); perr != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: perr}
		}
		// Re-creating a path retires any pending removal: the new inode
		// carries its own entry durability from here on.
		delete(c.tombs, path)
		n = &crashNode{}
		c.nodes[path] = n
	} else if n.dir {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	} else if flag&os.O_TRUNC != 0 {
		n.live = nil
	}
	return &crashFile{fs: c, node: n, path: path, gen: c.gen}, nil
}

// Open implements FS. Directories open successfully so syncDirFS can fsync
// them.
func (c *crashFS) Open(name string) (File, error) {
	path := filepath.Clean(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[path]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &crashFile{fs: c, node: n, path: path, gen: c.gen}, nil
}

// MkdirAll implements FS.
func (c *crashFS) MkdirAll(path string, perm fs.FileMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.mkdirAllLocked(filepath.Clean(path)); err != nil {
		return &fs.PathError{Op: "mkdir", Path: path, Err: err}
	}
	return nil
}

func (c *crashFS) mkdirAllLocked(path string) error {
	if n, ok := c.nodes[path]; ok {
		if !n.dir {
			return fs.ErrInvalid
		}
		return nil
	}
	if parent := filepath.Dir(path); parent != path {
		if err := c.mkdirAllLocked(parent); err != nil {
			return err
		}
	}
	delete(c.tombs, path)
	c.nodes[path] = &crashNode{dir: true}
	return nil
}

// dirLocked returns the directory node at path.
func (c *crashFS) dirLocked(path string) (*crashNode, error) {
	n, ok := c.nodes[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	if !n.dir {
		return nil, fs.ErrInvalid
	}
	return n, nil
}

// ReadDir implements FS, returning entries sorted by name like os.ReadDir.
func (c *crashFS) ReadDir(name string) ([]fs.DirEntry, error) {
	dir := filepath.Clean(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.dirLocked(dir); err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	var entries []fs.DirEntry
	for path, n := range c.nodes {
		if path == dir || filepath.Dir(path) != dir {
			continue
		}
		entries = append(entries, crashDirEntry{info: crashFileInfo{
			name: filepath.Base(path),
			size: int64(len(n.live)),
			dir:  n.dir,
		}})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

// Stat implements FS.
func (c *crashFS) Stat(name string) (fs.FileInfo, error) {
	path := filepath.Clean(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[path]
	if !ok {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return crashFileInfo{name: filepath.Base(path), size: int64(len(n.live)), dir: n.dir}, nil
}

// Remove implements FS. The unlink is visible immediately but survives a
// crash only once the parent directory has been fsynced.
func (c *crashFS) Remove(name string) error {
	path := filepath.Clean(name)
	gen := c.generation()
	c.fire(crashOpRemove, path)
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return errCrashedFS
	}
	n, ok := c.nodes[path]
	if !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	if n.dir {
		for p := range c.nodes {
			if p != path && filepath.Dir(p) == path {
				return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
			}
		}
	}
	delete(c.nodes, path)
	if n.entryDurable {
		c.tombs[path] = n
	}
	return nil
}

// Rename implements FS. It models rename(2)'s atomicity: the destination name
// refers to the source's node, or to whatever it referred to before, and never
// to anything in between. The moved node's ENTRY durability is reset, because
// the new dirent is exactly as unfsynced as a freshly created one — which is
// what makes a crash between a rename and the parent fsync lose the new name
// and restore the old, the behaviour the WAL format marker is published under.
func (c *crashFS) Rename(oldpath, newpath string) error {
	from, to := filepath.Clean(oldpath), filepath.Clean(newpath)
	gen := c.generation()
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return errCrashedFS
	}
	n, ok := c.nodes[from]
	if !ok {
		return &fs.PathError{Op: "rename", Path: oldpath, Err: fs.ErrNotExist}
	}
	if _, perr := c.dirLocked(filepath.Dir(to)); perr != nil {
		return &fs.PathError{Op: "rename", Path: newpath, Err: perr}
	}
	if prev, exists := c.nodes[to]; exists && prev.entryDurable {
		// The replaced node's own removal is only durable once the directory
		// is fsynced, exactly as for Remove.
		c.tombs[to] = prev
	}
	delete(c.nodes, from)
	delete(c.tombs, to)
	n.entryDurable = false
	c.nodes[to] = n
	return nil
}

// syncDirLocked makes every pending directory entry in dir durable: pending
// creations survive a crash from here on, and pending removals become final.
func (c *crashFS) syncDirLocked(dir string) {
	for path, n := range c.nodes {
		if path != dir && filepath.Dir(path) == dir {
			n.entryDurable = true
		}
	}
	for path := range c.tombs {
		if filepath.Dir(path) == dir {
			delete(c.tombs, path)
		}
	}
}

// crashFile is a handle into a crashFS node. Handles are bound to the
// filesystem generation they were opened in; a Crash bumps the generation and
// every stale handle fails closed.
type crashFile struct {
	fs   *crashFS
	node *crashNode
	path string
	gen  uint64
	pos  int64
}

// Compile-time interface checks.
var (
	_ FS               = (*crashFS)(nil)
	_ File             = (*crashFile)(nil)
	_ FilePreAllocator = (*crashFile)(nil)
)

// staleLocked reports whether this handle predates a Crash.
func (f *crashFile) staleLocked() bool { return f.gen != f.fs.gen }

func (f *crashFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.writeAt(p, off)
	if err == nil {
		f.fs.fire(crashOpWriteAtDone, f.path)
	}
	return n, err
}

// writeAt applies the write; WriteAt wraps it so the completion hook runs
// outside the filesystem lock, like every other hook.
func (f *crashFile) writeAt(p []byte, off int64) (int, error) {
	f.fs.fire(crashOpWriteAt, f.path)
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return 0, errCrashedFS
	}
	if f.node.dir {
		return 0, fs.ErrInvalid
	}
	end := off + int64(len(p))
	if int64(len(f.node.live)) < end {
		grown := make([]byte, end)
		copy(grown, f.node.live)
		f.node.live = grown
	}
	copy(f.node.live[off:end], p)
	f.fs.logWrite(f.path, off, len(p))
	return len(p), nil
}

// logWrite appends one WriteAt range to the log when recording is on.
func (c *crashFS) logWrite(path string, off int64, n int) {
	c.wlogMu.Lock()
	defer c.wlogMu.Unlock()
	if !c.wlogOn {
		return
	}
	c.wlog = append(c.wlog, crashWrite{path: path, off: off, n: n})
}

func (f *crashFile) Write(p []byte) (int, error) {
	n, err := f.WriteAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *crashFile) ReadAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return 0, errCrashedFS
	}
	if f.node.dir {
		return 0, fs.ErrInvalid
	}
	if off >= int64(len(f.node.live)) {
		return 0, io.EOF
	}
	n := copy(p, f.node.live[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *crashFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *crashFile) Seek(offset int64, whence int) (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return 0, errCrashedFS
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.node.live)) + offset
	default:
		return 0, fs.ErrInvalid
	}
	return f.pos, nil
}

func (f *crashFile) Truncate(size int64) error {
	f.fs.fire(crashOpTruncate, f.path)
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return errCrashedFS
	}
	resized := make([]byte, size)
	copy(resized, f.node.live)
	f.node.live = resized
	return nil
}

// PreAllocate models fallocate(2) with mode 0, which grows the file's visible
// size. The reservation is LIVE state like any other write.
func (f *crashFile) PreAllocate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return errCrashedFS
	}
	if int64(len(f.node.live)) >= size {
		return nil
	}
	grown := make([]byte, size)
	copy(grown, f.node.live)
	f.node.live = grown
	return nil
}

func (f *crashFile) Stat() (fs.FileInfo, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return nil, errCrashedFS
	}
	return crashFileInfo{
		name: filepath.Base(f.path),
		size: int64(len(f.node.live)),
		dir:  f.node.dir,
	}, nil
}

// Sync promotes this file's LIVE image to DURABLE. On a directory it instead
// makes the directory's pending entries durable, mirroring syncDirFS.
func (f *crashFile) Sync() error {
	if f.node.dir {
		f.fs.fire(crashOpDirSync, f.path)
	} else {
		f.fs.fire(crashOpSync, f.path)
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return errCrashedFS
	}
	if f.node.dir {
		f.fs.syncDirLocked(f.path)
		return nil
	}
	f.node.durable = append([]byte(nil), f.node.live...)
	return nil
}

func (f *crashFile) Close() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.staleLocked() {
		return errCrashedFS
	}
	return nil
}

// crashFileInfo implements fs.FileInfo with a fixed ModTime so directory
// listings and stats are deterministic.
type crashFileInfo struct {
	name string
	size int64
	dir  bool
}

func (i crashFileInfo) Name() string { return i.name }
func (i crashFileInfo) Size() int64  { return i.size }
func (i crashFileInfo) Mode() fs.FileMode {
	if i.dir {
		return fs.ModeDir | 0o700
	}
	return 0o600
}
func (i crashFileInfo) ModTime() time.Time { return time.Time{} }
func (i crashFileInfo) IsDir() bool        { return i.dir }
func (i crashFileInfo) Sys() any           { return nil }

// crashDirEntry implements fs.DirEntry over crashFileInfo.
type crashDirEntry struct {
	info crashFileInfo
}

func (d crashDirEntry) Name() string               { return d.info.name }
func (d crashDirEntry) IsDir() bool                { return d.info.dir }
func (d crashDirEntry) Type() fs.FileMode          { return d.info.Mode().Type() }
func (d crashDirEntry) Info() (fs.FileInfo, error) { return d.info, nil }
