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

// Bootstrap contains the initial cluster configuration used to bootstrap
// a new Raft node. It is persisted to LogDB during shard initialization.
// The wire format is:
//
//	[Addresses map][Join:8][Type:8]
type Bootstrap struct {
	// Addresses maps initial replica IDs to their network addresses.
	Addresses map[uint64]string
	// Join indicates whether this node is joining an existing cluster
	// rather than forming a new one.
	Join bool
	// Type identifies the state machine type for this shard
	// (RegularStateMachine, ConcurrentStateMachine, OnDiskStateMachine).
	Type uint64
}

// Size returns the total marshaled size of the bootstrap in bytes.
func (b *Bootstrap) Size() int {
	return mapSize(b.Addresses) + 8 + 8 // map + Join + Type
}

// MarshalTo writes the bootstrap into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (b *Bootstrap) MarshalTo(buf []byte) (int, error) {
	required := b.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	offset := putMap(buf, b.Addresses)
	putBool(buf[offset:], b.Join)
	offset += 8
	putUint64(buf[offset:], b.Type)
	offset += 8
	return offset, nil
}

// UnmarshalFrom reads the bootstrap from buf and returns bytes consumed.
// Returns ErrBufferTooSmall on truncated input or ErrAddressTooLong
// if any address exceeds MaxAddressLength.
func (b *Bootstrap) UnmarshalFrom(buf []byte) (int, error) {
	addrs, n, err := getMap(buf)
	if err != nil {
		return 0, err
	}
	b.Addresses = addrs
	offset := n
	if len(buf) < offset+16 { // Join (8) + Type (8)
		return 0, ErrBufferTooSmall
	}
	b.Join = getBool(buf[offset:])
	offset += 8
	b.Type = getUint64(buf[offset:])
	offset += 8
	return offset, nil
}

// Reset clears all fields for reuse.
func (b *Bootstrap) Reset() {
	b.Addresses = nil
	b.Join = false
	b.Type = 0
}
