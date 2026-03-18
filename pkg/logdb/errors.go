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

package logdb

import "errors"

// Sentinel errors for LogDB operations. These are pre-allocated for
// zero-alloc returns and use with errors.Is().
var (
	// ErrNoState is returned by ReadState when no persisted hard state
	// or membership exists for the requested node.
	ErrNoState = errors.New("logdb: no saved state")

	// ErrNoSnapshot is returned by GetSnapshot when no snapshot metadata
	// has been persisted for the requested node.
	ErrNoSnapshot = errors.New("logdb: no saved snapshot")

	// ErrNoBootstrap is returned by GetBootstrap when no bootstrap
	// information has been saved for the requested node.
	ErrNoBootstrap = errors.New("logdb: no bootstrap info")

	// ErrClosed is returned when an operation is attempted on a LogDB
	// instance that has already been closed.
	ErrClosed = errors.New("logdb: database closed")

	// ErrCompacted is returned when a requested log entry has already
	// been removed by compaction.
	ErrCompacted = errors.New("logdb: entry compacted")

	// ErrUnavailable is returned when requested log entries are beyond
	// the available range (index exceeds last known entry).
	ErrUnavailable = errors.New("logdb: entry unavailable")

	// ErrSnapshotOutOfDate is returned when a snapshot is older than
	// or equal to the currently recorded snapshot.
	ErrSnapshotOutOfDate = errors.New("logdb: snapshot out of date")

	// ErrLogGapDetected is returned when SetRange detects a gap between
	// the existing range and the new entries. This indicates a bug in
	// the entry persistence pipeline.
	ErrLogGapDetected = errors.New("logdb: gap detected in log entries")
)
