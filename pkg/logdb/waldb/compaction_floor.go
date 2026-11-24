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

// The durable compaction floors: the COMPACTED file records each shard's floor before a segment below it is unlinked.
const (
	floorsFilename     = "COMPACTED"
	floorsTempFilename = "COMPACTED.tmp"
	floorsMagic        = "QRFLOORS"
	floorsHeaderSize   = 8 + 8
	floorsEntrySize    = 8 + 8 + 8
	floorsTrailerSize  = 4
)

// ErrCompactionFloorsMalformed reports a COMPACTED file that does not parse; the open is refused.
var ErrCompactionFloorsMalformed = errors.New("waldb: compaction floor file is malformed")

// loadCompactionFloors reads dir's COMPACTED file; an absent file is an empty set.
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

// publishCompactionFloorsLocked makes the shard's floors durable before a segment is unlinked, writing nothing when unchanged.
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

// applyDurableFloors raises replayed floors to the COMPACTED file's and drops the entries they cover.
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
