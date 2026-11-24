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

// Package raft implements the core Raft consensus protocol as a pure state
// machine. It handles leader election, log replication, membership changes,
// snapshotting, read index, and quiesce. The package performs no I/O; the
// engine layer is responsible for persistence and network transport.
package raft

import (
	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// CompressEntry compresses an entry's Cmd payload using Snappy and sets the
// EntryCompressFlag on Entry.Type. No-op if Cmd is empty or nil, or if the
// compressed payload is not smaller than the original (incompressible data).
func CompressEntry(e *proto.Entry) {
	if len(e.Cmd) == 0 {
		return
	}
	compressed := snappy.Encode(nil, e.Cmd)
	// Only use compression if it actually reduces size.
	if len(compressed) < len(e.Cmd) {
		e.Cmd = compressed
		e.Type |= proto.EntryCompressFlag
	}
}

// DecompressEntry decompresses an entry's Cmd payload if the EntryCompressFlag
// is set. Returns a DecompressError if Snappy decoding fails, or
// ErrDecompressionBomb if decompressed size exceeds maxSize (when maxSize > 0).
// The flag is cleared after successful decompression.
func DecompressEntry(e *proto.Entry, maxSize int) error {
	if e.Type&proto.EntryCompressFlag == 0 {
		return nil
	}
	decompressed, err := snappy.Decode(nil, e.Cmd)
	if err != nil {
		return &DecompressError{Err: err}
	}
	if maxSize > 0 && len(decompressed) > maxSize {
		return ErrDecompressionBomb
	}
	e.Cmd = decompressed
	e.Type &^= proto.EntryCompressFlag
	return nil
}

// CompressEntries compresses all entries in the slice using Snappy.
// Only entries with non-empty Cmd payloads that benefit from compression
// are modified.
func CompressEntries(entries []proto.Entry) {
	for i := range entries {
		CompressEntry(&entries[i])
	}
}

// DecompressEntries decompresses all entries in the slice that have the
// EntryCompressFlag set. Returns the first error encountered.
func DecompressEntries(entries []proto.Entry, maxSize int) error {
	for i := range entries {
		if err := DecompressEntry(&entries[i], maxSize); err != nil {
			return err
		}
	}
	return nil
}
