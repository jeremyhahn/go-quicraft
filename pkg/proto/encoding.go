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

package proto

import (
	"encoding/binary"
	"sort"
)

// putUint64 writes a uint64 value to buf in little-endian byte order.
// buf must be at least 8 bytes.
func putUint64(buf []byte, v uint64) {
	binary.LittleEndian.PutUint64(buf, v)
}

// getUint64 reads a uint64 value from buf in little-endian byte order.
// buf must be at least 8 bytes.
func getUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

// putUint32 writes a uint32 value to buf in little-endian byte order.
// buf must be at least 4 bytes.
func putUint32(buf []byte, v uint32) {
	binary.LittleEndian.PutUint32(buf, v)
}

// getUint32 reads a uint32 value from buf in little-endian byte order.
// buf must be at least 4 bytes.
func getUint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

// putBool writes a bool value as a uint64 (0 or 1) to buf.
// buf must be at least 8 bytes.
func putBool(buf []byte, v bool) {
	if v {
		binary.LittleEndian.PutUint64(buf, 1)
	} else {
		binary.LittleEndian.PutUint64(buf, 0)
	}
}

// getBool reads a uint64 from buf and returns true if non-zero.
// buf must be at least 8 bytes.
func getBool(buf []byte) bool {
	return binary.LittleEndian.Uint64(buf) != 0
}

// putBytes writes a length-prefixed byte slice to buf: [len:4][data:N].
// Returns the total number of bytes written (4 + len(data)).
// buf must be at least 4 + len(data) bytes.
func putBytes(buf []byte, data []byte) int {
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return 4 + len(data)
}

// getBytes reads a length-prefixed byte slice from buf: [len:4][data:N].
// Returns the data slice (aliased into buf for zero-copy), total bytes
// consumed, and any error. Returns nil data for zero-length payloads.
func getBytes(buf []byte, maxLen uint32) ([]byte, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrBufferTooSmall
	}
	dataLen := binary.LittleEndian.Uint32(buf)
	if maxLen > 0 && dataLen > maxLen {
		return nil, 0, ErrEntrySizeLimitExceeded
	}
	total := 4 + int(dataLen)
	if len(buf) < total {
		return nil, 0, ErrBufferTooSmall
	}
	if dataLen == 0 {
		return nil, total, nil
	}
	return buf[4:total], total, nil // zero-copy alias into source buffer
}

// putString writes a length-prefixed string to buf: [len:4][data:N].
// Returns the total number of bytes written (4 + len(s)).
func putString(buf []byte, s string) int {
	binary.LittleEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	return 4 + len(s)
}

// getString reads a length-prefixed string from buf: [len:4][data:N].
// Returns the string, total bytes consumed, and any error. String data
// is copied from buf (strings are immutable in Go).
func getString(buf []byte, maxLen int) (string, int, error) {
	if len(buf) < 4 {
		return "", 0, ErrBufferTooSmall
	}
	strLen := binary.LittleEndian.Uint32(buf)
	if maxLen > 0 && int(strLen) > maxLen {
		return "", 0, ErrAddressTooLong
	}
	total := 4 + int(strLen)
	if len(buf) < total {
		return "", 0, ErrBufferTooSmall
	}
	return string(buf[4:total]), total, nil
}

// mapSize returns the encoded size of a map[uint64]string:
// [count:4] + sum([key:8][valueLen:4][value:N]) for each entry.
func mapSize(m map[uint64]string) int {
	sz := 4 // count prefix
	for _, v := range m {
		sz += 8 + 4 + len(v) // key + valueLen + value
	}
	return sz
}

// sortedKeys returns the keys of a map[uint64]string sorted ascending.
func sortedKeys(m map[uint64]string) []uint64 {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// sortedBoolKeys returns the keys of a map[uint64]bool sorted ascending.
func sortedBoolKeys(m map[uint64]bool) []uint64 {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// putMap writes a map[uint64]string to buf with deterministic key ordering:
// [count:4] then for each entry sorted by key: [key:8][valueLen:4][value:N].
// Returns the total number of bytes written.
func putMap(buf []byte, m map[uint64]string) int {
	binary.LittleEndian.PutUint32(buf, uint32(len(m)))
	offset := 4
	for _, k := range sortedKeys(m) {
		v := m[k]
		binary.LittleEndian.PutUint64(buf[offset:], k)
		offset += 8
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(v)))
		offset += 4
		copy(buf[offset:], v)
		offset += len(v)
	}
	return offset
}

// getMap reads a map[uint64]string from buf:
// [count:4] then for each entry: [key:8][valueLen:4][value:N].
// Returns the map, total bytes consumed, and any error.
func getMap(buf []byte) (map[uint64]string, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrBufferTooSmall
	}
	count := binary.LittleEndian.Uint32(buf)
	if count > maxMembershipEntries {
		return nil, 0, ErrTooManyMembershipEntries
	}
	offset := 4
	m := make(map[uint64]string, count)
	for i := uint32(0); i < count; i++ {
		if len(buf) < offset+8+4 {
			return nil, 0, ErrBufferTooSmall
		}
		key := binary.LittleEndian.Uint64(buf[offset:])
		offset += 8
		valLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += 4
		if int(valLen) > MaxAddressLength {
			return nil, 0, ErrAddressTooLong
		}
		if len(buf) < offset+int(valLen) {
			return nil, 0, ErrBufferTooSmall
		}
		m[key] = string(buf[offset : offset+int(valLen)])
		offset += int(valLen)
	}
	return m, offset, nil
}

// boolMapSize returns the encoded size of a map[uint64]bool:
// [count:4] + count * [key:8][value:8].
func boolMapSize(m map[uint64]bool) int {
	return 4 + len(m)*16
}

// putBoolMap writes a map[uint64]bool to buf with deterministic key ordering:
// [count:4] then for each entry sorted by key: [key:8][value:8].
// Returns the total number of bytes written.
func putBoolMap(buf []byte, m map[uint64]bool) int {
	binary.LittleEndian.PutUint32(buf, uint32(len(m)))
	offset := 4
	for _, k := range sortedBoolKeys(m) {
		binary.LittleEndian.PutUint64(buf[offset:], k)
		offset += 8
		putBool(buf[offset:], m[k])
		offset += 8
	}
	return offset
}

// getBoolMap reads a map[uint64]bool from buf:
// [count:4] then for each entry: [key:8][value:8].
// Returns the map, total bytes consumed, and any error.
func getBoolMap(buf []byte) (map[uint64]bool, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrBufferTooSmall
	}
	count := binary.LittleEndian.Uint32(buf)
	if count > maxMembershipEntries {
		return nil, 0, ErrTooManyMembershipEntries
	}
	offset := 4
	m := make(map[uint64]bool, count)
	for i := uint32(0); i < count; i++ {
		if len(buf) < offset+16 {
			return nil, 0, ErrBufferTooSmall
		}
		key := binary.LittleEndian.Uint64(buf[offset:])
		offset += 8
		m[key] = getBool(buf[offset:])
		offset += 8
	}
	return m, offset, nil
}
