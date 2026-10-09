// Copyright 2025 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package waldb

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestPlatformDiskUsage_RealFilesystem exercises the statfs-backed default
// against a real directory, which every mocked monitor test bypasses.
func TestPlatformDiskUsage_RealFilesystem(t *testing.T) {
	if _, err := platformDiskUsage(t.TempDir()); err != nil {
		t.Fatalf("platformDiskUsage on an existing directory: %v", err)
	}
}

// TestPlatformDiskUsage_MissingDirectory verifies a statfs failure is
// reported as a DiskUsageError naming the directory.
func TestPlatformDiskUsage_MissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	_, err := platformDiskUsage(dir)
	var due *DiskUsageError
	if !errors.As(err, &due) {
		t.Fatalf("want *DiskUsageError, got %v", err)
	}
	if due.Dir != dir {
		t.Errorf("DiskUsageError.Dir = %q, want %q", due.Dir, dir)
	}
}

// TestDiskMonitor_DefaultUsageFunc verifies a monitor built without an
// override measures the real filesystem.
func TestDiskMonitor_DefaultUsageFunc(t *testing.T) {
	dm := NewDiskMonitor(t.TempDir(), 1<<62)
	if _, err := dm.diskUsage(dm.dir); err != nil {
		t.Fatalf("default usage func: %v", err)
	}
}
