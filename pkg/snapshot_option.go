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
	"fmt"
	"path/filepath"
)

// SnapshotOption controls per-request snapshot behavior. A zero-value
// SnapshotOption uses shard-level defaults (equivalent to Dragonboat's
// SnapshotOption{}).
//
// When ExportPath is set, the snapshot is written to the specified
// directory as an exported snapshot (Exported = true on the snapshot
// metadata). When OverrideCompactionOverhead is true, CompactionOverhead
// replaces the shard-level Config.CompactionOverhead for the log
// compaction that follows this snapshot.
type SnapshotOption struct {
	// ExportPath, when non-empty, causes the snapshot to be written to
	// this directory path. The directory must be an absolute path and
	// will be created if it does not exist. When set, the snapshot
	// metadata is marked as exported (Imported = true in proto.Snapshot)
	// and the snapshot is additionally written to the standard snapshot
	// directory for log compaction purposes.
	ExportPath string

	// CompactionOverhead overrides the shard-level
	// Config.CompactionOverhead for this snapshot's log compaction.
	// Only used when OverrideCompactionOverhead is true.
	CompactionOverhead uint64

	// OverrideCompactionOverhead controls whether the CompactionOverhead
	// field is applied. When false, the shard's Config.CompactionOverhead
	// is used.
	OverrideCompactionOverhead bool

	// Exported indicates whether this snapshot is an export operation.
	// This is set automatically when ExportPath is non-empty and should
	// not normally be set by callers directly.
	Exported bool
}

// DefaultSnapshotOption is the zero-value SnapshotOption that uses
// shard-level defaults for all settings. It is equivalent to
// SnapshotOption{}.
var DefaultSnapshotOption = SnapshotOption{}

// Validate checks that the SnapshotOption fields are consistent and
// valid. Returns a *SnapshotOptionError on failure or nil if valid.
func (o *SnapshotOption) Validate() error {
	if o.ExportPath != "" {
		if !filepath.IsAbs(o.ExportPath) {
			return &SnapshotOptionError{
				Field:  "ExportPath",
				Reason: "must be an absolute path",
			}
		}
	}
	return nil
}

// IsExport returns true if this option requests a snapshot export
// (either via ExportPath being set or Exported being explicitly true).
func (o *SnapshotOption) IsExport() bool {
	return o.Exported || o.ExportPath != ""
}

// SnapshotOptionError is returned when a SnapshotOption field fails
// validation. It carries the field name and a reason for the failure.
type SnapshotOptionError struct {
	Field  string
	Reason string
}

// Error returns a human-readable description of the validation failure.
func (e *SnapshotOptionError) Error() string {
	return fmt.Sprintf("quicraft: invalid snapshot option %s: %s", e.Field, e.Reason)
}

// ErrInvalidSnapshotOption is the sentinel error for snapshot option
// validation failures, enabling errors.Is checks.
var ErrInvalidSnapshotOption = &SnapshotOptionError{
	Field:  "",
	Reason: "invalid snapshot option",
}

// Is reports whether target matches the sentinel ErrInvalidSnapshotOption.
func (e *SnapshotOptionError) Is(target error) bool {
	_, ok := target.(*SnapshotOptionError)
	return ok
}
