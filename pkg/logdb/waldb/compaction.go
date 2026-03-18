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
	"io/fs"
	"path/filepath"
)

// compactor manages background removal of WAL segment files whose entries
// have all been compacted. A segment is safe to delete only when every
// (shardID, replicaID) that wrote to it has compacted past its last entry
// in that segment.
type compactor struct {
	// fs is the filesystem abstraction for directory and file operations.
	fs FS
	// dir is the WAL directory containing segment files.
	dir string
	// blockSize is the WAL block size used when opening segments for
	// verification during garbage collection.
	blockSize int
}

// newCompactor creates a compactor for the given WAL directory.
func newCompactor(fs FS, dir string, blockSize int) *compactor {
	return &compactor{
		fs:        fs,
		dir:       dir,
		blockSize: blockSize,
	}
}

// GarbageCollect removes segment files that are no longer referenced by
// any node index. It compares the set of segment files on disk against
// the set of segment IDs still referenced in the provided liveSegments
// map. Any segment file not present in liveSegments and with an ID
// strictly less than maxLiveID is deleted.
//
// maxLiveID prevents deletion of the active (currently being written to)
// segment which may not yet appear in any index.
func (c *compactor) GarbageCollect(liveSegments map[uint64]bool, maxLiveID uint64) (int, error) {
	dirEntries, err := c.fs.ReadDir(c.dir)
	if err != nil {
		return 0, &CompactionReadDirError{Dir: c.dir, Err: err}
	}

	deleted := 0
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}

		name := de.Name()
		segID, ok := parseSegmentFilename(name)
		if !ok {
			continue
		}

		// Never delete the active segment or any segment >= maxLiveID.
		if segID >= maxLiveID {
			continue
		}

		// If the segment is still referenced by a live index, keep it.
		if liveSegments[segID] {
			continue
		}

		// Safe to delete.
		path := filepath.Join(c.dir, name)
		if err := c.fs.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return deleted, &CompactionDeleteError{Path: path, Err: err}
		}
		deleted++
	}

	return deleted, nil
}

// parseSegmentFilename extracts the segment ID from a filename matching
// the format "%06d.log". Returns the ID and true on success, or 0 and
// false if the filename does not match.
func parseSegmentFilename(name string) (uint64, bool) {
	if len(name) < 5 {
		return 0, false
	}
	// Must end with ".log".
	if name[len(name)-4:] != ".log" {
		return 0, false
	}
	numStr := name[:len(name)-4]
	if len(numStr) == 0 {
		return 0, false
	}

	var id uint64
	for _, ch := range numStr {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		id = id*10 + uint64(ch-'0')
	}
	return id, true
}

// CompactionReadDirError is returned when the compactor fails to read
// the WAL directory during garbage collection.
type CompactionReadDirError struct {
	Dir string
	Err error
}

// Error implements the error interface.
func (e *CompactionReadDirError) Error() string {
	return "waldb: compaction failed to read directory " + e.Dir + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *CompactionReadDirError) Unwrap() error {
	return e.Err
}

// CompactionDeleteError is returned when the compactor fails to delete
// an obsolete segment file.
type CompactionDeleteError struct {
	Path string
	Err  error
}

// Error implements the error interface.
func (e *CompactionDeleteError) Error() string {
	return "waldb: compaction failed to delete " + e.Path + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *CompactionDeleteError) Unwrap() error {
	return e.Err
}
