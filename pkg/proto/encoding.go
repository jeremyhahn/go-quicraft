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
	"math"
	"sort"
)

// mapEntryMinBytes is the smallest possible wire size of a map[uint64]string
// entry: [key:8][valueLen:4] with a zero-length value.
const mapEntryMinBytes = 8 + 4

// boolMapEntryBytes is the exact wire size of a map[uint64]bool entry:
// [key:8][value:8].
const boolMapEntryBytes = 8 + 8

// mapAllocBytesPerEntry is the measured upper bound on heap bytes one decoded
// map entry costs, and it is what turns the per-entry WIRE minimums above into
// the memory bound MaxDecodeMemoryFactor states.
//
// A Go map allocates its slots in groups of 8 and grows when a group set is
// 7/8 full, so a map holding N entries owns between N*8/7 and N*16/7 slots
// depending on where N falls against the power-of-two group count. One
// map[uint64]string slot costs 1 control byte + 8 key bytes + 16 string-header
// bytes = 25, putting the slot arrays alone at up to 25*16/7 ~= 57 bytes per
// entry against the 12-byte wire minimum of mapEntryMinBytes; the map's own
// header and table bookkeeping take the MEASURED worst case to ~62 (Go 1.26,
// amd64), and the constant is rounded up from that.
//
// It is documentation and a test anchor, not a runtime check: nothing can
// enforce the Go runtime's map layout from here. TestMapAllocBytesPerEntryIs-
// AnUpperBound measures it and TestDecodeMemoryAmplification measures the ratio
// it feeds, so a toolchain change that moves either one says so.
const mapAllocBytesPerEntry = 64

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
// errOverLimit is returned when the length prefix exceeds maxLen, so each
// call site reports its own sentinel (ErrMetadataTooLong, ErrChunkDataTooLong,
// ...) and errors.Is matching works per field.
//
// maxLen is ALWAYS applied. Treating zero as "unlimited" would be precisely
// backwards for a decoder, because the one way a caller reaches that branch is
// by forgetting to pass its field's limit, and the reward for forgetting would
// be an unbounded length prefix taken straight off the wire. A limit of zero
// means what it says: only an empty payload is admissible. No in-package
// caller passes zero.
func getBytes(buf []byte, maxLen uint32, errOverLimit error) ([]byte, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrBufferTooSmall
	}
	dataLen := binary.LittleEndian.Uint32(buf)
	// Checked FIRST, before any narrowing conversion and before the limit
	// comparison, matching getString: int is 32-bit on 32-bit platforms, where
	// 4+int(dataLen) wraps negative and turns the bounds check below into a
	// slice panic. Ordering it after the maxLen check would leave that wrap
	// reachable for any caller whose own limit exceeds MaxInt32 — no caller
	// does today, which is exactly why the guard must not depend on it.
	if dataLen > math.MaxInt32 {
		return nil, 0, ErrBufferTooSmall
	}
	if dataLen > maxLen {
		return nil, 0, errOverLimit
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
// is copied from buf (strings are immutable in Go). errOverLimit is
// returned when the length prefix exceeds maxLen, so each call site
// reports its own sentinel (ErrAddressTooLong, ErrFilepathTooLong, ...)
// and errors.Is matching works per field.
//
// maxLen is ALWAYS applied; a limit of zero admits only the empty string. See
// getBytes for why a zero limit must not mean "unlimited".
func getString(buf []byte, maxLen int, errOverLimit error) (string, int, error) {
	if len(buf) < 4 {
		return "", 0, ErrBufferTooSmall
	}
	strLen := binary.LittleEndian.Uint32(buf)
	// Checked before int(strLen): on a 32-bit platform int(strLen) is
	// negative for strLen > MaxInt32, which would pass the maxLen check and
	// then panic on the slice below.
	if strLen > math.MaxInt32 {
		return "", 0, ErrBufferTooSmall
	}
	if int(strLen) > maxLen {
		return "", 0, errOverLimit
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
// Returns the map, total bytes consumed, and any error. The count is
// cross-checked against the remaining buffer before allocating, so a short
// buffer with a large count cannot drive an oversized allocation.
//
// # The map is sized from the wire count, and that ALLOCATES LESS
//
// Clamping the capacity hint (to 64, say) on the theory that a count off the
// wire must never size an allocation buys nothing — the cross-check below means
// a count that reaches the make() is one the caller genuinely supplied bytes
// for — and a clamp is not conservative, it is expensive: a map that starts at
// 64 slots and grows to N reallocates its whole table at every doubling, so the
// decode touches roughly twice the memory the final map occupies and every
// superseded table stays live until the next GC.
//
// Measured on a membership-dense frame (five string maps of 897 zero-length
// entries), a hint clamped to 64 allocates 528 KB for a 54 KB frame: 9.8x the
// wire size. Sizing the map once brings the same frame to 5.05x and drops the
// rehashing as a side effect. See
// MaxDecodeMemoryFactor for the bound this buys and the test that holds it.
func getMap(buf []byte) (map[uint64]string, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrBufferTooSmall
	}
	count := binary.LittleEndian.Uint32(buf)
	if count > MaxMembershipEntries {
		return nil, 0, ErrTooManyMembershipEntries
	}
	offset := 4
	if uint64(count)*mapEntryMinBytes > uint64(len(buf)-offset) {
		return nil, 0, ErrBufferTooSmall
	}
	// Sized exactly once, from a count the cross-check above has already proved
	// the buffer can hold; see the doc comment.
	m := make(map[uint64]string, count)
	for i := uint32(0); i < count; i++ {
		if len(buf) < offset+8+4 {
			return nil, 0, ErrBufferTooSmall
		}
		key := binary.LittleEndian.Uint64(buf[offset:])
		offset += 8
		valLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += 4
		// Checked before int(valLen), matching getBytes/getString: on a 32-bit
		// platform int(valLen) is negative for valLen > MaxInt32, which passes
		// both the limit check and the remaining-buffer check below and then
		// panics on the slice expression.
		if valLen > math.MaxInt32 {
			return nil, 0, ErrBufferTooSmall
		}
		if int(valLen) > MaxAddressLength {
			return nil, 0, ErrAddressTooLong
		}
		if len(buf) < offset+int(valLen) {
			return nil, 0, ErrBufferTooSmall
		}
		// putMap emits each key once, sorted ascending. A repeat is a
		// non-canonical encoding this decoder never produces.
		if _, dup := m[key]; dup {
			return nil, 0, ErrDuplicateMapKey
		}
		m[key] = string(buf[offset : offset+int(valLen)])
		offset += int(valLen)
	}
	return m, offset, nil
}

// checkWritableMap mirrors getMap's limits on the WRITE side: the entry count
// against MaxMembershipEntries and every value against MaxAddressLength.
//
// Without it putMap narrows len(m) with uint32(len(m)) against no preceding
// bound, and emits values getMap will refuse — bytes that are durable and
// unreadable. It reports the SAME sentinels getMap does, because the whole
// point is that a value either round-trips or is refused identically at both
// ends.
func checkWritableMap(m map[uint64]string) error {
	if len(m) > MaxMembershipEntries {
		return ErrTooManyMembershipEntries
	}
	for _, v := range m {
		if len(v) > MaxAddressLength {
			return ErrAddressTooLong
		}
	}
	return nil
}

// checkWritableBoolMap mirrors getBoolMap's entry-count limit on the write
// side. A bool map has no per-value length, so the count is the whole check.
func checkWritableBoolMap(m map[uint64]bool) error {
	if len(m) > MaxMembershipEntries {
		return ErrTooManyMembershipEntries
	}
	return nil
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
// Returns the map, total bytes consumed, and any error. The count is
// cross-checked against the remaining buffer before allocating, so a short
// buffer with a large count cannot drive an oversized allocation.
func getBoolMap(buf []byte) (map[uint64]bool, int, error) {
	if len(buf) < 4 {
		return nil, 0, ErrBufferTooSmall
	}
	count := binary.LittleEndian.Uint32(buf)
	if count > MaxMembershipEntries {
		return nil, 0, ErrTooManyMembershipEntries
	}
	offset := 4
	if uint64(count)*boolMapEntryBytes > uint64(len(buf)-offset) {
		return nil, 0, ErrBufferTooSmall
	}
	// Sized exactly once; see getMap.
	m := make(map[uint64]bool, count)
	for i := uint32(0); i < count; i++ {
		if len(buf) < offset+16 {
			return nil, 0, ErrBufferTooSmall
		}
		key := binary.LittleEndian.Uint64(buf[offset:])
		offset += 8
		// putBoolMap emits each key once, sorted ascending. A repeat is a
		// non-canonical encoding this decoder never produces.
		if _, dup := m[key]; dup {
			return nil, 0, ErrDuplicateMapKey
		}
		m[key] = getBool(buf[offset:])
		offset += 8
	}
	return m, offset, nil
}
