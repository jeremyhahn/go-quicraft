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

//go:build linux

package waldb

import (
	"os"
	"syscall"
)

// fdatasync flushes file data to disk without updating file metadata (mtime,
// atime). This is ~10-30% faster than fsync for WAL appends within
// pre-allocated space, because the file size has not changed.
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// fallocateFile reserves disk space for the file so appends within the
// reservation do not extend the file or hit ENOSPC. Mode 0 also grows the
// file's visible size to the requested length (keeping the size unchanged
// would require FALLOC_FL_KEEP_SIZE); segment.Close truncates back to the
// bytes actually written, and recovery treats the zero-filled tail of a
// pre-allocated segment as end-of-records.
func fallocateFile(f *os.File, size int64) error {
	return syscall.Fallocate(int(f.Fd()), 0, 0, size)
}
