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

//go:build !linux

package waldb

import "os"

// fdatasync falls back to fsync on non-Linux platforms (macOS, Windows).
// fsync flushes both data and metadata, which is slightly slower but
// functionally correct on all platforms.
func fdatasync(f *os.File) error {
	return f.Sync()
}

// fallocateFile falls back to ftruncate on platforms without fallocate.
// ftruncate extends the file with zero bytes, which is functionally equivalent
// for pre-allocation but may not reserve disk blocks on all filesystems.
func fallocateFile(f *os.File, size int64) error {
	return f.Truncate(size)
}
