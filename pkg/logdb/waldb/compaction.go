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
// map. A segment file is deleted only when all of the following hold:
//
//   - its ID is strictly less than maxLiveID (the active segment, which may
//     not yet appear in any index, is never deleted);
//   - its ID is less than or equal to maxCommittedID (the commit high-water
//     mark); and
//   - it is not present in liveSegments.
//
// The maxCommittedID guard keeps GC away from any segment whose records are
// not yet reflected in the in-memory index and meta pins. SaveState writes a
// record durably in its Phase 1 (possibly rotating to a new segment) but
// updates the index and pins only in its Phase 3. It now holds shard.mu across
// both, so a concurrent same-shard Compact cannot run in between; the guard is
// what keeps GC correct regardless of that locking, and it was the fix when
// SaveState released shard.mu between the phases. maxCommittedID advances only
// at the Phase-3 commit point, the writeBootstrap commit and recovery, so a
// durably-written-but-not-yet-indexed segment has an ID above it and is
// preserved.
func (c *compactor) GarbageCollect(liveSegments map[uint64]bool, maxLiveID, maxCommittedID uint64) (int, error) {
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

		// Never delete a segment that has not yet committed its in-memory
		// index/pins. Such a segment is durably written but invisible to
		// liveSegments, so deleting it would lose its records on recovery.
		if segID > maxCommittedID {
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

// segmentFilenameDigits is the canonical zero-padded width of a segment
// filename's numeric part, as produced by segmentFilename ("%06d.log").
const segmentFilenameDigits = 6

// maxSegmentID is the largest representable segment ID.
const maxSegmentID = ^uint64(0)

// parseSegmentFilename extracts the segment ID from a filename in the exact
// form segmentFilename produces: six digits, or more than six digits with no
// leading zero. Returns the ID and true on success.
//
// Only the canonical form is accepted. A non-canonical digit string
// ("0000001.log", "00000000000000000001.log") is a foreign file that would
// otherwise alias a real segment ID — recovery would adopt it, or GC would
// delete it believing an unrelated segment was reclaimed — and an
// arbitrary-length string silently wrapped on uint64 overflow.
func parseSegmentFilename(name string) (uint64, bool) {
	// Must end with ".log".
	if len(name) <= 4 || name[len(name)-4:] != ".log" {
		return 0, false
	}
	numStr := name[:len(name)-4]
	if len(numStr) < segmentFilenameDigits {
		return 0, false
	}
	if len(numStr) > segmentFilenameDigits && numStr[0] == '0' {
		return 0, false
	}

	var id uint64
	for i := 0; i < len(numStr); i++ {
		ch := numStr[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		d := uint64(ch - '0')
		// Reject rather than wrap: a wrapped ID names a different segment.
		if id > (maxSegmentID-d)/10 {
			return 0, false
		}
		id = id*10 + d
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
