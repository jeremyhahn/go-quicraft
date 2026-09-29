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
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
)

// The retired-replica registry: the durable record of every (shardID,
// replicaID) that RemoveNodeData has removed, and so retired for good.
//
// Raft never lets a removed replica ID return: its term, vote and owed lease
// vote window went with its data, and a replica started again without them
// could vote twice in one term. The retirement must therefore outlive every
// record the replica ever wrote, and those records live in WAL segments that
// the shard shares with other Raft groups and that garbage collection reclaims
// only when nothing else pins them. A retirement kept AS a WAL record — the
// old RemoveNode tombstone — had to pin its own segment forever to stay ahead
// of them: a whole segment (64 MiB by default) retained per removed replica.
//
// The registry keeps the retirement beside the WAL instead, in one small file
// in the WAL root: 16 bytes per retired replica. Replay drops every record of
// a retired replica wherever it is found (purgeRetired), so nothing in the
// segments needs pinning, and the removed replica's segments become ordinary
// garbage. The file is rewritten whole on each retirement and published the
// way the format marker is (writeWALFormat): a fsynced temporary file renamed
// over the name, then the directory fsynced, so the name only ever refers to a
// complete registry. Retirements are rare and the file is small; a whole
// rewrite keeps it a single self-checking value with no log to replay.
//
// Layout (little-endian, like the record format):
//
//	magic "QRRETIRE"[8] | count:u64 | count × (shardID:u64 | replicaID:u64) | crc32c:u32
//
// The CRC covers every byte before it. Entries are sorted, so the file is a
// function of the set.
const (
	retiredFilename     = "RETIRED"
	retiredTempFilename = "RETIRED.tmp"
	retiredMagic        = "QRRETIRE"
	retiredHeaderSize   = 8 + 8
	retiredEntrySize    = 8 + 8
	retiredTrailerSize  = 4
)

// ErrRetiredRegistryMalformed is returned by Open when the retired-replica
// registry exists but does not parse: a wrong length, magic or checksum. It
// fails the open rather than being ignored — a registry that cannot be read is
// a set of retirements that cannot be enforced, and opening without it would
// hand retired replica IDs back.
var ErrRetiredRegistryMalformed = errors.New("waldb: retired-replica registry is malformed")

// retiredRegistry is the in-memory copy of the registry file. Reads take mu
// for reading, behind a lock-free empty fast path; a retirement is serialized
// by writeMu, published to disk first and only then to the set, so a reader
// never sees a retirement that is not durable.
type retiredRegistry struct {
	writeMu sync.Mutex
	mu      sync.RWMutex
	set     map[nodeKey]struct{}
	size    atomic.Int64 // len(set), so the common empty case takes no lock
}

// loadRetiredRegistry reads the registry in dir. An absent file is an empty
// registry: a WAL that has never retired a replica has none.
func loadRetiredRegistry(filesystem FS, dir string) (*retiredRegistry, error) {
	r := &retiredRegistry{set: make(map[nodeKey]struct{})}
	f, err := filesystem.Open(filepath.Join(dir, retiredFilename))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return r, nil
		}
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("waldb: closing the retired-replica registry failed",
				"dir", dir, "error", closeErr)
		}
	}()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	keys, err := parseRetiredRegistry(buf)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		r.set[k] = struct{}{}
	}
	r.size.Store(int64(len(r.set)))
	return r, nil
}

// parseRetiredRegistry decodes a registry file's bytes.
func parseRetiredRegistry(buf []byte) ([]nodeKey, error) {
	if len(buf) < retiredHeaderSize+retiredTrailerSize || string(buf[:8]) != retiredMagic {
		return nil, ErrRetiredRegistryMalformed
	}
	count := binary.LittleEndian.Uint64(buf[8:16])
	body := uint64(len(buf) - retiredHeaderSize - retiredTrailerSize)
	if body%retiredEntrySize != 0 || body/retiredEntrySize != count {
		return nil, ErrRetiredRegistryMalformed
	}
	end := len(buf) - retiredTrailerSize
	if crc32.Checksum(buf[:end], crc32Table) != binary.LittleEndian.Uint32(buf[end:]) {
		return nil, ErrRetiredRegistryMalformed
	}
	keys := make([]nodeKey, 0, count)
	for off := retiredHeaderSize; off < end; off += retiredEntrySize {
		keys = append(keys, nodeKey{
			ShardID:   binary.LittleEndian.Uint64(buf[off:]),
			ReplicaID: binary.LittleEndian.Uint64(buf[off+8:]),
		})
	}
	return keys, nil
}

// marshalRetiredRegistry encodes keys, sorted, as a registry file.
func marshalRetiredRegistry(keys []nodeKey) []byte {
	slices.SortFunc(keys, func(a, b nodeKey) int {
		if c := cmp.Compare(a.ShardID, b.ShardID); c != 0 {
			return c
		}
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	buf := make([]byte, retiredHeaderSize+len(keys)*retiredEntrySize+retiredTrailerSize)
	copy(buf[:8], retiredMagic)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(keys)))
	off := retiredHeaderSize
	for _, k := range keys {
		binary.LittleEndian.PutUint64(buf[off:], k.ShardID)
		binary.LittleEndian.PutUint64(buf[off+8:], k.ReplicaID)
		off += retiredEntrySize
	}
	binary.LittleEndian.PutUint32(buf[off:], crc32.Checksum(buf[:off], crc32Table))
	return buf
}

// contains reports whether key has been retired.
func (r *retiredRegistry) contains(key nodeKey) bool {
	if r.size.Load() == 0 {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.set[key]
	return ok
}

// keys returns every retired key.
func (r *retiredRegistry) keys() []nodeKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]nodeKey, 0, len(r.set))
	for k := range r.set {
		out = append(out, k)
	}
	return out
}

// retire durably adds keys to the registry in dir, in one rewrite however
// many there are, so carrying a whole registry across (Reencrypt) costs one
// publication rather than one per key. Keys already retired are skipped;
// retiring nothing new is a no-op.
//
// The new registry is published first. Then, still under writeMu, apply runs
// — RemoveNodeData drops the node's records there — and only after it are the
// keys added to the in-memory set. So NodeRemoved reports a retirement only
// once it is durable AND the node's records are gone from every reader; the
// two can never be observed out of order.
//
// On error, published reports whether the new registry was renamed into
// place. If it was not, nothing has changed. If it was, only the directory
// fsync failed: the retirement may or may not survive a crash, the same
// uncertainty a failed WAL fsync leaves, and the caller must fail-stop the
// database so the next open decides from what is on disk. apply does not run
// on any error.
func (r *retiredRegistry) retire(filesystem FS, dir string, keys []nodeKey, noSync bool, apply func()) (published bool, err error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	var fresh []nodeKey
	seen := make(map[nodeKey]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup || r.contains(k) {
			continue
		}
		seen[k] = struct{}{}
		fresh = append(fresh, k)
	}
	if len(fresh) == 0 {
		return false, nil
	}
	next := append(r.keys(), fresh...)
	tmp := filepath.Join(dir, retiredTempFilename)
	if err := writeMarkerTemp(filesystem, tmp, marshalRetiredRegistry(next), noSync); err != nil {
		removeMarkerTemp(filesystem, tmp)
		return false, err
	}
	if err := filesystem.Rename(tmp, filepath.Join(dir, retiredFilename)); err != nil {
		removeMarkerTemp(filesystem, tmp)
		return false, err
	}
	if !noSync {
		// The rename is durable only once the directory it published into is.
		if err := syncDirFS(filesystem, dir); err != nil {
			return true, err
		}
	}
	if apply != nil {
		apply()
	}
	r.mu.Lock()
	for _, k := range fresh {
		r.set[k] = struct{}{}
	}
	r.size.Store(int64(len(r.set)))
	r.mu.Unlock()
	return true, nil
}
