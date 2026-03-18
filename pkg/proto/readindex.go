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

// ReadyToRead represents a read index request that has been confirmed
// by the leader. It carries the request key for correlation and the
// committed log index at which the read can be safely served. The wire
// format is exactly 16 bytes:
//
//	[Key:8][Index:8]
type ReadyToRead struct {
	// Key is the client request key for correlating the read response.
	Key uint64
	// Index is the committed log index at which the read is safe.
	Index uint64
}

// readIndexSize is the fixed wire format size: 2 uint64 fields = 16 bytes.
const readIndexSize = 2 * 8 // 16 bytes

// Size returns the marshaled size in bytes. Always returns 16.
func (ri *ReadyToRead) Size() int {
	return readIndexSize
}

// MarshalTo writes the read index into buf and returns bytes written.
// buf must be at least 16 bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (ri *ReadyToRead) MarshalTo(buf []byte) (int, error) {
	if len(buf) < readIndexSize {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], ri.Key)
	putUint64(buf[8:], ri.Index)
	return readIndexSize, nil
}

// UnmarshalFrom reads the read index from buf and returns bytes consumed.
// Returns ErrBufferTooSmall on truncated input.
func (ri *ReadyToRead) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < readIndexSize {
		return 0, ErrBufferTooSmall
	}
	ri.Key = getUint64(buf[0:])
	ri.Index = getUint64(buf[8:])
	return readIndexSize, nil
}

// Reset clears all fields for reuse.
func (ri *ReadyToRead) Reset() {
	ri.Key = 0
	ri.Index = 0
}
