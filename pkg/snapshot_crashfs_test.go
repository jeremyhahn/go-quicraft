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

package quicraft

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// crashFS is an in-memory snapshotFS that models POSIX crash semantics in
// addition to per-operation error injection. Error injection alone can only
// exercise error HANDLING: a MISSING fsync produces no call to fail, so only a
// durability model can detect one. That is what this double exists for.
//
// The model:
//
//   - Every file carries a LIVE and a DURABLE image. Write mutates LIVE only;
//     Sync promotes LIVE to DURABLE.
//   - A directory ENTRY (a create, a rename's new name, a removal) is pending
//     until the PARENT directory is fsynced, matching the production
//     SyncDir/SyncDirChain discipline.
//   - Crash discards every non-durable effect: files revert to their DURABLE
//     image, entries never covered by a parent fsync disappear along with
//     everything beneath them, removals never covered by a parent fsync are
//     resurrected, and a rename not covered by a parent fsync reverts to the
//     source name.
//   - Handles obtained before a Crash fail afterwards: a file descriptor
//     cannot survive the power loss it models.
//
// It is deliberately NOT waldb's crashFS: that one implements waldb.FS (a
// different, backend-specific interface) and is unexported in its own package.
type crashFS struct {
	mu    sync.Mutex
	gen   uint64
	nodes map[string]*crashNode
	// tombs holds entries unlinked from the live image whose removal is not
	// yet durable; Crash resurrects them.
	tombs   map[string]*crashNode
	tempSeq int

	hookMu sync.Mutex
	// hook runs before each intercepted operation, outside the state lock, so
	// it may call Crash to cut power mid-sequence. A non-nil error it returns
	// fails the operation, which then has no effect.
	hook func(op crashOp, path string) error
}

// crashNode is one filesystem object. Directories hold no content; only their
// entry durability matters.
type crashNode struct {
	dir          bool
	live         []byte
	durable      []byte
	entryDurable bool
}

func (n *crashNode) clone() *crashNode {
	return &crashNode{
		dir:          n.dir,
		live:         append([]byte(nil), n.live...),
		durable:      append([]byte(nil), n.durable...),
		entryDurable: n.entryDurable,
	}
}

// crashOp identifies an intercepted filesystem operation.
type crashOp string

const (
	opOpenFile   crashOp = "openfile"
	opOpen       crashOp = "open"
	opCreateTemp crashOp = "createtemp"
	opMkdirAll   crashOp = "mkdirall"
	opMkdirTemp  crashOp = "mkdirtemp"
	opRename     crashOp = "rename"
	opRemove     crashOp = "remove"
	opRemoveAll  crashOp = "removeall"
	opStat       crashOp = "stat"
	opWrite      crashOp = "write"
	opSync       crashOp = "sync"
	opDirSync    crashOp = "dirsync"
)

// errCrashed is returned by a handle obtained before a Crash.
var errCrashed = errors.New("quicraft_test: handle used after simulated power loss")

// newCrashFS returns a filesystem whose seeded directories, and all their
// ancestors, already exist durably — the mounted volume the data directory
// lives in.
func newCrashFS(seeded ...string) *crashFS {
	c := &crashFS{
		nodes: make(map[string]*crashNode),
		tombs: make(map[string]*crashNode),
	}
	for _, dir := range seeded {
		for p := filepath.Clean(dir); ; {
			c.nodes[p] = &crashNode{dir: true, entryDurable: true}
			parent := filepath.Dir(p)
			if parent == p {
				break
			}
			p = parent
		}
	}
	return c
}

// setHook installs the pre-operation hook, replacing any previous one.
func (c *crashFS) setHook(fn func(op crashOp, path string) error) {
	c.hookMu.Lock()
	c.hook = fn
	c.hookMu.Unlock()
}

// failOnce fails the first occurrence of op on a path containing match with
// err, then lets every later operation through.
func (c *crashFS) failOnce(op crashOp, match string, err error) {
	var once sync.Once
	var fired bool
	var mu sync.Mutex
	c.setHook(func(gotOp crashOp, path string) error {
		if gotOp != op || !strings.Contains(path, match) {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if fired {
			return nil
		}
		once.Do(func() { fired = true })
		return err
	})
}

// crashOn cuts power immediately before the first occurrence of op on a path
// containing match; the triggering operation then fails, as it never reached
// the medium.
func (c *crashFS) crashOn(op crashOp, match string) {
	var mu sync.Mutex
	fired := false
	c.setHook(func(gotOp crashOp, path string) error {
		if gotOp != op || !strings.Contains(path, match) {
			return nil
		}
		mu.Lock()
		if fired {
			mu.Unlock()
			return nil
		}
		fired = true
		mu.Unlock()
		c.Crash()
		return errCrashed
	})
}

func (c *crashFS) intercept(op crashOp, path string) error {
	c.hookMu.Lock()
	hook := c.hook
	c.hookMu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(op, path)
}

// Crash discards all state that was never made durable.
func (c *crashFS) Crash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++

	// A removal that was never covered by a parent fsync did not happen.
	for p, n := range c.tombs {
		if n.entryDurable {
			c.nodes[p] = n
		}
	}
	c.tombs = make(map[string]*crashNode)

	// Unsynced writes are lost.
	for _, n := range c.nodes {
		n.live = append([]byte(nil), n.durable...)
	}

	// Entries never covered by a parent fsync did not happen, and neither did
	// anything beneath them.
	var pending []string
	for p, n := range c.nodes {
		if !n.entryDurable {
			pending = append(pending, p)
		}
	}
	sort.Strings(pending)
	for _, root := range pending {
		prefix := root + string(filepath.Separator)
		for p := range c.nodes {
			if p == root || strings.HasPrefix(p, prefix) {
				delete(c.nodes, p)
			}
		}
	}
}

func notExist(op, path string) error {
	return &fs.PathError{Op: op, Path: path, Err: fs.ErrNotExist}
}

func (c *crashFS) lookupLocked(path string) (*crashNode, bool) {
	n, ok := c.nodes[path]
	return n, ok
}

// OpenFile implements snapshotFS.
func (c *crashFS) OpenFile(name string, flag int, _ fs.FileMode) (snapshotFile, error) {
	name = filepath.Clean(name)
	if err := c.intercept(opOpenFile, name); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(name)
	if !ok {
		if flag&os.O_CREATE == 0 {
			return nil, notExist("open", name)
		}
		parent, pok := c.lookupLocked(filepath.Dir(name))
		if !pok || !parent.dir {
			return nil, notExist("open", name)
		}
		n = &crashNode{}
		c.nodes[name] = n
	}
	if flag&os.O_TRUNC != 0 {
		n.live = nil
	}
	return &crashFile{fs: c, path: name, gen: c.gen, node: n}, nil
}

// Open implements snapshotFS.
func (c *crashFS) Open(name string) (snapshotFile, error) {
	name = filepath.Clean(name)
	if err := c.intercept(opOpen, name); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(name)
	if !ok {
		return nil, notExist("open", name)
	}
	return &crashFile{fs: c, path: name, gen: c.gen, node: n}, nil
}

// CreateTemp implements snapshotFS.
func (c *crashFS) CreateTemp(dir, pattern string) (snapshotFile, error) {
	dir = filepath.Clean(dir)
	if err := c.intercept(opCreateTemp, dir); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	parent, ok := c.lookupLocked(dir)
	if !ok || !parent.dir {
		return nil, notExist("createtemp", dir)
	}
	c.tempSeq++
	name := strings.Replace(pattern, "*", "crashfs"+itoa(c.tempSeq), 1)
	if !strings.Contains(pattern, "*") {
		name = pattern + itoa(c.tempSeq)
	}
	path := filepath.Join(dir, name)
	n := &crashNode{}
	c.nodes[path] = n
	return &crashFile{fs: c, path: path, gen: c.gen, node: n}, nil
}

// MkdirAll implements snapshotFS.
func (c *crashFS) MkdirAll(path string, _ fs.FileMode) error {
	path = filepath.Clean(path)
	if err := c.intercept(opMkdirAll, path); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var missing []string
	for p := path; ; {
		if _, ok := c.lookupLocked(p); ok {
			break
		}
		missing = append(missing, p)
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	for _, p := range missing {
		c.nodes[p] = &crashNode{dir: true}
	}
	return nil
}

// MkdirTemp implements snapshotFS. The created directory is unique by
// construction (the sequence counter never repeats within a run), so a caller
// staging an artifact for a later rename owns everything beneath it.
func (c *crashFS) MkdirTemp(dir, pattern string) (string, error) {
	dir = filepath.Clean(dir)
	if err := c.intercept(opMkdirTemp, dir); err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	parent, ok := c.lookupLocked(dir)
	if !ok || !parent.dir {
		return "", notExist("mkdirtemp", dir)
	}
	c.tempSeq++
	name := strings.Replace(pattern, "*", "crashfs"+itoa(c.tempSeq), 1)
	if !strings.Contains(pattern, "*") {
		name = pattern + itoa(c.tempSeq)
	}
	path := filepath.Join(dir, name)
	c.nodes[path] = &crashNode{dir: true}
	return path, nil
}

// Rename implements snapshotFS. The new name is visible immediately but is
// not durable until the parent directory is fsynced; a crash before that
// reverts to the source name.
func (c *crashFS) Rename(oldpath, newpath string) error {
	oldpath = filepath.Clean(oldpath)
	newpath = filepath.Clean(newpath)
	if err := c.intercept(opRename, newpath); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(oldpath)
	if !ok {
		return notExist("rename", oldpath)
	}
	if existing, ok := c.lookupLocked(newpath); ok {
		c.tombs[newpath] = existing
	}
	// Renaming a DIRECTORY relocates everything beneath it in one step. Moving
	// only the directory node left its children addressable under the old name
	// alone, so a path that staged a whole directory and published it with a
	// single rename looked — to this model — like it had lost every file it
	// wrote, and a durability test could not tell that from a missing fsync.
	oldPrefix := oldpath + string(filepath.Separator)
	newPrefix := newpath + string(filepath.Separator)
	var moved []string
	for p := range c.nodes {
		if strings.HasPrefix(p, oldPrefix) {
			moved = append(moved, p)
		}
	}
	for _, p := range moved {
		child := c.nodes[p]
		delete(c.nodes, p)
		// The old name survives a crash that does not cover the rename, so the
		// child is tombstoned exactly as the directory itself is.
		c.tombs[p] = child.clone()
		c.nodes[newPrefix+strings.TrimPrefix(p, oldPrefix)] = child
	}
	c.tombs[oldpath] = n.clone()
	delete(c.nodes, oldpath)
	n.entryDurable = false
	c.nodes[newpath] = n
	return nil
}

// Remove implements snapshotFS.
func (c *crashFS) Remove(name string) error {
	name = filepath.Clean(name)
	if err := c.intercept(opRemove, name); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(name)
	if !ok {
		return notExist("remove", name)
	}
	delete(c.nodes, name)
	c.tombs[name] = n
	return nil
}

// RemoveAll implements snapshotFS.
func (c *crashFS) RemoveAll(path string) error {
	path = filepath.Clean(path)
	if err := c.intercept(opRemoveAll, path); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := path + string(filepath.Separator)
	for p, n := range c.nodes {
		if p == path || strings.HasPrefix(p, prefix) {
			delete(c.nodes, p)
			c.tombs[p] = n
		}
	}
	return nil
}

// Stat implements snapshotFS.
func (c *crashFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	if err := c.intercept(opStat, name); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(name)
	if !ok {
		return nil, notExist("stat", name)
	}
	return crashFileInfo{name: filepath.Base(name), size: int64(len(n.live)), dir: n.dir}, nil
}

// durableBytes returns a file's post-crash content, reporting whether the
// file would survive a crash at this instant (its own entry and every
// ancestor entry durable).
func (c *crashFS) durableBytes(path string) ([]byte, bool) {
	path = filepath.Clean(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(path)
	if !ok {
		return nil, false
	}
	for p := path; ; {
		an, ok := c.lookupLocked(p)
		if !ok || !an.entryDurable {
			return nil, false
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	return append([]byte(nil), n.durable...), true
}

// liveBytes returns a file's pre-crash content.
func (c *crashFS) liveBytes(path string) ([]byte, bool) {
	path = filepath.Clean(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.lookupLocked(path)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), n.live...), true
}

// exists reports whether path is present in the live image.
func (c *crashFS) exists(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.lookupLocked(filepath.Clean(path))
	return ok
}

// entries lists the live names directly under dir.
func (c *crashFS) entries(dir string) []string {
	dir = filepath.Clean(dir)
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for p := range c.nodes {
		if filepath.Dir(p) == dir && p != dir {
			out = append(out, filepath.Base(p))
		}
	}
	sort.Strings(out)
	return out
}

// crashFile is an open handle. Reads and writes see the LIVE image.
type crashFile struct {
	fs     *crashFS
	path   string
	gen    uint64
	node   *crashNode
	off    int64
	closed bool
}

func (f *crashFile) valid() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.gen != f.fs.gen {
		return errCrashed
	}
	if f.closed {
		return os.ErrClosed
	}
	return nil
}

func (f *crashFile) Write(p []byte) (int, error) {
	if err := f.fs.intercept(opWrite, f.path); err != nil {
		return 0, err
	}
	if err := f.valid(); err != nil {
		return 0, err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	end := f.off + int64(len(p))
	if int64(len(f.node.live)) < end {
		grown := make([]byte, end)
		copy(grown, f.node.live)
		f.node.live = grown
	}
	copy(f.node.live[f.off:end], p)
	f.off = end
	return len(p), nil
}

func (f *crashFile) Read(p []byte) (int, error) {
	if err := f.valid(); err != nil {
		return 0, err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.off >= int64(len(f.node.live)) {
		return 0, io.EOF
	}
	n := copy(p, f.node.live[f.off:])
	f.off += int64(n)
	return n, nil
}

func (f *crashFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.valid(); err != nil {
		return 0, err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	switch whence {
	case io.SeekStart:
		f.off = offset
	case io.SeekCurrent:
		f.off += offset
	case io.SeekEnd:
		f.off = int64(len(f.node.live)) + offset
	}
	if f.off < 0 {
		return 0, errors.New("quicraft_test: negative seek offset")
	}
	return f.off, nil
}

func (f *crashFile) Stat() (fs.FileInfo, error) {
	if err := f.valid(); err != nil {
		return nil, err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	return crashFileInfo{
		name: filepath.Base(f.path),
		size: int64(len(f.node.live)),
		dir:  f.node.dir,
	}, nil
}

func (f *crashFile) Name() string { return f.path }

// Sync promotes this file's LIVE image to DURABLE. On a directory handle it
// instead makes the directory's own entries durable — the production
// SyncDir contract.
func (f *crashFile) Sync() error {
	op := opSync
	if f.node.dir {
		op = opDirSync
	}
	if err := f.fs.intercept(op, f.path); err != nil {
		return err
	}
	if err := f.valid(); err != nil {
		return err
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if !f.node.dir {
		f.node.durable = append([]byte(nil), f.node.live...)
		return nil
	}
	for p, n := range f.fs.nodes {
		if p != f.path && filepath.Dir(p) == f.path {
			n.entryDurable = true
		}
	}
	prefix := f.path + string(filepath.Separator)
	for p := range f.fs.tombs {
		if strings.HasPrefix(p, prefix) {
			delete(f.fs.tombs, p)
		}
	}
	return nil
}

func (f *crashFile) Close() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.gen != f.fs.gen {
		return errCrashed
	}
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return nil
}

type crashFileInfo struct {
	name string
	size int64
	dir  bool
}

func (i crashFileInfo) Name() string { return i.name }
func (i crashFileInfo) Size() int64  { return i.size }
func (i crashFileInfo) Mode() fs.FileMode {
	if i.dir {
		return fs.ModeDir | 0o750
	}
	return 0o600
}
func (i crashFileInfo) ModTime() time.Time { return time.Time{} }
func (i crashFileInfo) IsDir() bool        { return i.dir }
func (i crashFileInfo) Sys() any           { return nil }

// itoa avoids pulling strconv into this file's import set for one call.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// compile-time assertions.
var (
	_ snapshotFS   = (*crashFS)(nil)
	_ snapshotFile = (*crashFile)(nil)
)
