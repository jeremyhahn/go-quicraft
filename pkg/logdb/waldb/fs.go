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
	"io"
	"io/fs"
	"os"

	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
)

// File abstracts WAL segment file I/O; osFile wraps *os.File (fdatasync on Linux, fsync elsewhere), and tests may inject errors.
type File interface {
	// Write writes p at the current offset, as io.Writer does.
	Write(p []byte) (int, error)
	// WriteAt writes p at offset off, as io.WriterAt does.
	WriteAt(p []byte, off int64) (int, error)
	// Read reads into p from the current offset, as io.Reader does.
	Read(p []byte) (int, error)
	// ReadAt reads into p from offset off, as io.ReaderAt does.
	ReadAt(p []byte, off int64) (int, error)
	// Stat returns the file info of the file.
	Stat() (fs.FileInfo, error)
	// Truncate changes the size of the file to size.
	Truncate(size int64) error
	// Seek sets the offset for the next Read or Write, as io.Seeker does.
	Seek(offset int64, whence int) (int64, error)
	// Sync flushes the file's data to stable storage.
	Sync() error
	// Close closes the file.
	Close() error
}

// FileMetadataSyncer is implemented by Files whose Sync omits metadata (Linux fdatasync); directory syncs must use SyncMetadata. It is fsutil.MetadataSyncer.
type FileMetadataSyncer = fsutil.MetadataSyncer

// FilePreAllocator is implemented by Files that can pre-allocate space: fallocate(2) mode 0 on Linux, ftruncate(2) elsewhere; it may change the file size.
type FilePreAllocator interface {
	// PreAllocate reserves disk space for size bytes. It may change the
	// visible file size, and the non-Linux fallback may not reserve
	// physical blocks.
	PreAllocate(size int64) error
}

// walLockFilename is the lock file Open holds in the WAL root.
const walLockFilename = "LOCK"

// DirLocker is implemented by an FS that can lock a directory; Open holds it on the WAL root, so a second Open fails with fsutil.ErrDirLocked. Wrappers should forward it.
type DirLocker interface {
	// LockDir locks dir through the file name inside it, failing
	// immediately with an error wrapping fsutil.ErrDirLocked when it is
	// held. Closing the result releases the lock.
	LockDir(dir, name string) (io.Closer, error)
}

// FS abstracts the WAL's filesystem operations; osFS delegates to os. Rename is required, since the format marker is published by rename (writeWALFormat).
type FS interface {
	// OpenFile opens the named file with the given flags and permissions,
	// as os.OpenFile does.
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	// Open opens the named file for reading, as os.Open does.
	Open(name string) (File, error)
	// MkdirAll creates the directory path and any missing parents, as
	// os.MkdirAll does.
	MkdirAll(path string, perm fs.FileMode) error
	// ReadDir returns the entries of the named directory, as os.ReadDir
	// does.
	ReadDir(name string) ([]fs.DirEntry, error)
	// Stat returns the file info of the named file, as os.Stat does.
	Stat(name string) (fs.FileInfo, error)
	// Remove removes the named file or empty directory, as os.Remove does.
	Remove(name string) error
	// Rename atomically renames oldpath to newpath, as os.Rename does.
	Rename(oldpath, newpath string) error
}

// osFS implements FS using the standard os package.
type osFS struct{}

// OpenFile opens the named file with the specified flags and permissions.
func (osFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

// Open opens the named file for reading.
func (osFS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

// MkdirAll creates the directory path and all parents that do not exist.
func (osFS) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

// ReadDir reads the named directory and returns its directory entries.
func (osFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

// Stat returns file info for the named file.
func (osFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

// Remove removes the named file or empty directory.
func (osFS) Remove(name string) error {
	return os.Remove(name)
}

// Rename atomically replaces newpath with oldpath. Both must live in the same
// directory for the atomicity the marker publication depends on.
func (osFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// LockDir takes the exclusive directory lock through fsutil.LockDir.
func (osFS) LockDir(dir, name string) (io.Closer, error) {
	return fsutil.LockDir(dir, name)
}

// osFile implements File by wrapping *os.File with platform-optimized sync.
type osFile struct {
	f *os.File
}

// Write writes bytes to the file.
func (o *osFile) Write(p []byte) (int, error) { return o.f.Write(p) }

// WriteAt writes bytes to the file at the given offset without changing the
// file's current position. Used by the segment writer to keep blocks aligned
// to the block grid across partial (mid-block) syncs.
func (o *osFile) WriteAt(p []byte, off int64) (int, error) { return o.f.WriteAt(p, off) }

// Read reads bytes from the file.
func (o *osFile) Read(p []byte) (int, error) { return o.f.Read(p) }

// ReadAt reads bytes from the file starting at the given offset.
func (o *osFile) ReadAt(p []byte, off int64) (int, error) { return o.f.ReadAt(p, off) }

// Stat returns the file's FileInfo.
func (o *osFile) Stat() (fs.FileInfo, error) { return o.f.Stat() }

// Truncate changes the size of the file.
func (o *osFile) Truncate(size int64) error { return o.f.Truncate(size) }

// Seek sets the offset for the next read or write.
func (o *osFile) Seek(offset int64, whence int) (int64, error) {
	return o.f.Seek(offset, whence)
}

// Close closes the file.
func (o *osFile) Close() error { return o.f.Close() }

// Sync flushes file data to durable storage using fdatasync on Linux
// (data only, ~10-30% faster) or fsync on other platforms (data + metadata).
// Not sufficient for a directory: see SyncMetadata.
func (o *osFile) Sync() error {
	return fdatasync(o.f)
}

// SyncMetadata flushes data AND metadata with a full fsync. It implements
// FileMetadataSyncer and is what directory syncs use, because the dirent
// changes they exist to make durable are exactly the metadata Sync omits.
func (o *osFile) SyncMetadata() error {
	return o.f.Sync()
}

// PreAllocate pre-allocates disk space using fallocate on Linux or
// ftruncate on other platforms.
func (o *osFile) PreAllocate(size int64) error {
	return fallocateFile(o.f, size)
}

// defaultFS returns the production filesystem implementation.
func defaultFS() FS {
	return osFS{}
}

// DefaultFS returns the production filesystem, for external test packages that wrap it.
func DefaultFS() FS {
	return osFS{}
}
