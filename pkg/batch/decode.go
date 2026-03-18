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

import "encoding/binary"

// lengthPrefixSize is the number of bytes used for each command's length
// prefix in the batch wire format (4 bytes, big-endian uint32).
const lengthPrefixSize = 4

// EncodeBatch combines multiple command payloads into a single batch.
// Format: [cmd1_len:4 BE][cmd1_data][cmd2_len:4 BE][cmd2_data]...
//
// Each command is prefixed with its length as a 4-byte big-endian uint32.
// An empty slice produces a zero-length output. The caller is responsible
// for ensuring individual commands do not exceed math.MaxUint32 bytes.
func EncodeBatch(cmds [][]byte) []byte {
	totalSize := 0
	for _, cmd := range cmds {
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

	return buf
}

// DecodeBatch splits a batch payload into individual commands.
// Returns a DecodeError if the data is malformed (truncated length prefix
// or command data extending beyond the payload boundary).
//
// An empty or nil input returns a nil slice with no error.
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
		cmdLen := int(binary.BigEndian.Uint32(data[off : off+lengthPrefixSize]))
		off += lengthPrefixSize

		if off+cmdLen > len(data) {
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
		cmdLen := int(binary.BigEndian.Uint32(data[off : off+lengthPrefixSize]))
		off += lengthPrefixSize

		if off+cmdLen > len(data) {
			return 0, &DecodeError{Reason: "command data exceeds payload boundary"}
		}
		off += cmdLen
		count++
	}
	return count, nil
}
