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
	"io/fs"
	"os"
)

// File abstracts the file I/O operations used by WAL segments. Production
// code uses osFile which wraps *os.File with platform-optimized sync
// (fdatasync on Linux, fsync elsewhere). Test code can provide alternative
// implementations for error injection.
type File interface {
	Write(p []byte) (int, error)
	Read(p []byte) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	Stat() (fs.FileInfo, error)
	Truncate(size int64) error
	Seek(offset int64, whence int) (int64, error)
	Sync() error
	Close() error
}

// FilePreAllocator is optionally implemented by File implementations that
// support pre-allocating disk space without changing the visible file size.
// On Linux this maps to fallocate(2); other platforms fall back to
// ftruncate(2).
type FilePreAllocator interface {
	PreAllocate(size int64) error
}

// FS abstracts filesystem operations used by the WAL database. Production
// code uses osFS which delegates to the os package. Test code can provide
// alternative implementations for error injection and fault simulation.
type FS interface {
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	Open(name string) (File, error)
	MkdirAll(path string, perm fs.FileMode) error
	ReadDir(name string) ([]fs.DirEntry, error)
	Stat(name string) (fs.FileInfo, error)
	Remove(name string) error
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

// osFile implements File by wrapping *os.File with platform-optimized sync.
type osFile struct {
	f *os.File
}

// Write writes bytes to the file.
func (o *osFile) Write(p []byte) (int, error) { return o.f.Write(p) }

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
func (o *osFile) Sync() error {
	return fdatasync(o.f)
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

// DefaultFS returns the production filesystem implementation. It is exported
// for use by external test packages (e.g., error-injection test suites) that
// need to construct an ErrorFS wrapping the real OS filesystem without
// importing os internals directly.
func DefaultFS() FS {
	return osFS{}
}
