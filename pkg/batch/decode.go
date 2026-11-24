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

package batch

import (
	"encoding/binary"
	"math"
)

// lengthPrefixSize is the number of bytes used for each command's length
// prefix in the batch wire format (4 bytes, big-endian uint32).
const lengthPrefixSize = 4

// EncodeBatch joins commands as [len:4 BE][data]...; a command over math.MaxInt32 bytes is an EncodeError, matching what decoding rejects.
func EncodeBatch(cmds [][]byte) ([]byte, error) {
	totalSize := 0
	for _, cmd := range cmds {
		if len(cmd) > math.MaxInt32 {
			return nil, &EncodeError{Reason: "command length exceeds MaxInt32"}
		}
		totalSize += lengthPrefixSize + len(cmd)
	}

	buf := make([]byte, totalSize)
	off := 0
	for _, cmd := range cmds {
		binary.BigEndian.PutUint32(buf[off:off+lengthPrefixSize], uint32(len(cmd)))
		off += lengthPrefixSize
		copy(buf[off:], cmd)
		off += len(cmd)
	}

	return buf, nil
}

// DecodeBatch splits a batch into commands; malformed data is a DecodeError and empty input returns nil.
func DecodeBatch(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Pre-count commands for a single allocation.
	count, err := countCommands(data)
	if err != nil {
		return nil, err
	}

	cmds := make([][]byte, 0, count)
	off := 0
	for off < len(data) {
		if off+lengthPrefixSize > len(data) {
			return nil, &DecodeError{Reason: "truncated length prefix"}
		}
		rawLen := binary.BigEndian.Uint32(data[off : off+lengthPrefixSize])
		// On 32-bit builds int is 32-bit, so a length prefix above MaxInt32
		// converts to a negative int, slips past the off+cmdLen bound check,
		// and panics the slice expression. Reject it while still a uint32.
		if rawLen > math.MaxInt32 {
			return nil, &DecodeError{Reason: "command length exceeds MaxInt32"}
		}
		cmdLen := int(rawLen)
		off += lengthPrefixSize

		// fits compares against the remainder: off+cmdLen overflows int on
		// 32-bit builds for a prefix near MaxInt32.
		if !fits(off, cmdLen, len(data)) {
			return nil, &DecodeError{Reason: "command data exceeds payload boundary"}
		}
		cmds = append(cmds, data[off:off+cmdLen])
		off += cmdLen
	}

	return cmds, nil
}

// countCommands performs a fast scan to count the number of length-prefixed
// commands in the batch payload. This enables a single allocation in
// DecodeBatch. Returns a DecodeError on malformed data.
func countCommands(data []byte) (int, error) {
	count := 0
	off := 0
	for off < len(data) {
		if off+lengthPrefixSize > len(data) {
			return 0, &DecodeError{Reason: "truncated length prefix"}
		}
		rawLen := binary.BigEndian.Uint32(data[off : off+lengthPrefixSize])
		// See DecodeBatch: guard the int conversion against a negative wrap
		// on 32-bit builds before it reaches the bounds check.
		if rawLen > math.MaxInt32 {
			return 0, &DecodeError{Reason: "command length exceeds MaxInt32"}
		}
		cmdLen := int(rawLen)
		off += lengthPrefixSize

		// See DecodeBatch: fits cannot overflow int on 32-bit builds.
		if !fits(off, cmdLen, len(data)) {
			return 0, &DecodeError{Reason: "command data exceeds payload boundary"}
		}
		off += cmdLen
		count++
	}
	return count, nil
}

// fits reports whether n bytes at off lie within total, without forming off+n.
func fits(off, n, total int) bool {
	return off >= 0 && n >= 0 && off <= total && n <= total-off
}
