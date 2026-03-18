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

import "syscall"

// platformDiskUsage returns the number of bytes used on the filesystem
// containing dir, using the Linux statfs syscall. This measures
// filesystem-level usage (total - available), not just the WAL
// directory, which provides accurate ENOSPC prevention.
func platformDiskUsage(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, &DiskUsageError{Dir: dir, Err: err}
	}
	// Total bytes = blocks * block size.
	// Available bytes = available blocks * block size.
	// Used = total - available.
	total := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	if available > total {
		// Defensive: some filesystems report weird values.
		return 0, nil
	}
	return total - available, nil
}
