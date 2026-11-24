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
	"cmp"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
)

// The durable compaction floors.
//
// A compaction marker is a WAL record, so it is exactly as durable as the
// block it lands in, while the segment unlink it licenses is irreversible.
// Tear the marker's block after the unlink and replay comes back with a lower
// floor over a prefix that is gone: reads below the real floor then report
// ErrLogGapDetected instead of ErrCompacted, and raft never falls back to
// InstallSnapshot.
//
// Each shard directory therefore keeps a second, independent copy of its
// floors in COMPACTED, published by temp-fsync-rename-dirsync like the
// retired registry, and every segment unlink (Compact, and the reclaim after
// a node is removed) waits for that publication. The ordering is the whole
// argument:
//
//   - The file only ever holds floors that RemoveEntriesTo already made
//     durable as markers, so it never claims more than the WAL acknowledged.
//   - A segment is unlinked only after the file covering its entries is
//     durable, so a crash between the two leaves the segment on disk under a
//     floor that already covers it, and a crash before the rename leaves the
//     old file and every segment.
//   - Recovery takes the higher of the file's floor and the replayed one, so
//     losing either copy alone cannot lower the floor.
//
// A file that exists but does not parse refuses the open: the floors it held
// cannot be reconstructed, and guessing low would fail open.
//
// Layout (little-endian): magic "QRFLOORS" | count u64 | count x
// (shardID u64, replicaID u64, floor u64) sorted by key | CRC32C of all
// preceding bytes.
const (
	floorsFilename     = "COMPACTED"
	floorsTempFilename = "COMPACTED.tmp"
	floorsMagic        = "QRFLOORS"
	floorsHeaderSize   = 8 + 8
	floorsEntrySize    = 8 + 8 + 8
	floorsTrailerSize  = 4
)

// ErrCompactionFloorsMalformed reports a COMPACTED file that does not parse:
// a bad magic, a count that disagrees with the length, or a checksum
// mismatch. The open is refused, since the floors it held are unknown.
var ErrCompactionFloorsMalformed = errors.New("waldb: compaction floor file is malformed")

// loadCompactionFloors reads dir's COMPACTED file. An absent file is an empty
// set: no segment of this directory was ever unlinked below a floor.
func loadCompactionFloors(filesystem FS, dir string) (map[nodeKey]uint64, error) {
	floors := make(map[nodeKey]uint64)
	f, err := filesystem.Open(filepath.Join(dir, floorsFilename))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return floors, nil
		}
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("waldb: closing the compaction floor file failed",
				"dir", dir, "error", closeErr)
		}
	}()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return parseCompactionFloors(buf)
}

// parseCompactionFloors decodes a COMPACTED file image.
func parseCompactionFloors(buf []byte) (map[nodeKey]uint64, error) {
	if len(buf) < floorsHeaderSize+floorsTrailerSize || string(buf[:8]) != floorsMagic {
		return nil, ErrCompactionFloorsMalformed
	}
	count := binary.LittleEndian.Uint64(buf[8:16])
	body := uint64(len(buf) - floorsHeaderSize - floorsTrailerSize)
	if body%floorsEntrySize != 0 || body/floorsEntrySize != count {
		return nil, ErrCompactionFloorsMalformed
	}
	end := len(buf) - floorsTrailerSize
	if crc32.Checksum(buf[:end], crc32Table) != binary.LittleEndian.Uint32(buf[end:]) {
		return nil, ErrCompactionFloorsMalformed
	}
	floors := make(map[nodeKey]uint64, count)
	for off := floorsHeaderSize; off < end; off += floorsEntrySize {
		floors[nodeKey{
			ShardID:   binary.LittleEndian.Uint64(buf[off:]),
			ReplicaID: binary.LittleEndian.Uint64(buf[off+8:]),
		}] = binary.LittleEndian.Uint64(buf[off+16:])
	}
	return floors, nil
}

// marshalCompactionFloors encodes floors as a COMPACTED file image.
func marshalCompactionFloors(floors map[nodeKey]uint64) []byte {
	keys := slices.SortedFunc(maps.Keys(floors), func(a, b nodeKey) int {
		if c := cmp.Compare(a.ShardID, b.ShardID); c != 0 {
			return c
		}
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	buf := make([]byte, floorsHeaderSize+len(keys)*floorsEntrySize+floorsTrailerSize)
	copy(buf[:8], floorsMagic)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(keys)))
	off := floorsHeaderSize
	for _, k := range keys {
		binary.LittleEndian.PutUint64(buf[off:], k.ShardID)
		binary.LittleEndian.PutUint64(buf[off+8:], k.ReplicaID)
		binary.LittleEndian.PutUint64(buf[off+16:], floors[k])
		off += floorsEntrySize
	}
	binary.LittleEndian.PutUint32(buf[off:], crc32.Checksum(buf[:off], crc32Table))
	return buf
}

// publishCompactionFloorsLocked makes the shard's current floors durable in
// COMPACTED before a segment is unlinked. It writes nothing when the file
// already holds them, so a compaction that raised no floor costs no I/O. On
// error nothing may be unlinked: the caller skips garbage collection.
//
// Caller holds s.mu, which every mutator of compactedTo holds as well.
func (s *walShard) publishCompactionFloorsLocked() error {
	if maps.Equal(s.compactedTo, s.durableFloors) {
		return nil
	}
	next := maps.Clone(s.compactedTo)
	tmp := filepath.Join(s.dir, floorsTempFilename)
	if err := writeMarkerTemp(s.fs, tmp, marshalCompactionFloors(next), s.opts.noSync); err != nil {
		removeMarkerTemp(s.fs, tmp)
		return err
	}
	if err := s.fs.Rename(tmp, filepath.Join(s.dir, floorsFilename)); err != nil {
		removeMarkerTemp(s.fs, tmp)
		return err
	}
	if !s.opts.noSync {
		// The rename is durable only once the directory it published into is.
		if err := syncDirFS(s.fs, s.dir); err != nil {
			return err
		}
	}
	s.durableFloors = next
	return nil
}

// applyDurableFloors raises every replayed floor the COMPACTED file holds
// higher, and drops the entries the raised floor covers so none is served.
// A higher durable floor means the marker carrying it was lost (a torn
// block): the file was published only after that marker was acknowledged.
//
// Called from recovery, before any reader exists.
func (s *walShard) applyDurableFloors() {
	for key, durable := range s.durableFloors {
		replayed := s.compactedTo[key]
		if durable <= replayed {
			continue
		}
		slog.Warn("waldb: compaction floor marker lost; restoring the floor "+
			"from the durable floor file",
			"shard_id", key.ShardID,
			"replica_id", key.ReplicaID,
			"replayed_floor", replayed,
			"durable_floor", durable)
		s.compactedTo[key] = durable
		if idx, ok := s.indexes[key]; ok {
			idx.Compact(durable)
		}
		for raftIdx := range s.entries[key] {
			if raftIdx <= durable {
				delete(s.entries[key], raftIdx)
			}
		}
	}
}
