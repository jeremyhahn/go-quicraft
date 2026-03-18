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

// ConfigChange represents a membership change request. It is serialized
// as the Cmd payload of an EntryConfigChange entry. The wire format is:
//
//	[ConfigChangeID:8][Type:8][ReplicaID:8][Address:len+data][Initialize:8]
type ConfigChange struct {
	// ConfigChangeID is the log index that identifies this config change.
	ConfigChangeID uint64
	// Type is the change type: AddNode, RemoveNode, AddNonVoting, or AddWitness.
	Type uint64
	// ReplicaID is the target replica for the membership change.
	ReplicaID uint64
	// Address is the network address for the replica being added.
	// Empty for RemoveNode operations.
	Address string
	// Initialize indicates whether the replica needs initial state.
	Initialize bool
}

// configChangeFixedSize is the wire size of the fixed fields:
// ConfigChangeID(8) + Type(8) + ReplicaID(8) + Initialize(8) = 32 bytes
// plus the 4-byte address string length prefix.
const configChangeFixedSize = 4*8 + 4 // 36 bytes

// Size returns the total marshaled size of the config change in bytes.
func (cc *ConfigChange) Size() int {
	return configChangeFixedSize + len(cc.Address)
}

// MarshalTo writes the config change into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (cc *ConfigChange) MarshalTo(buf []byte) (int, error) {
	required := cc.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], cc.ConfigChangeID)
	putUint64(buf[8:], cc.Type)
	putUint64(buf[16:], cc.ReplicaID)
	offset := 24
	offset += putString(buf[offset:], cc.Address)
	putBool(buf[offset:], cc.Initialize)
	offset += 8
	return offset, nil
}

// UnmarshalFrom reads the config change from buf and returns bytes consumed.
// Returns ErrBufferTooSmall on truncated input or ErrAddressTooLong if
// the address exceeds MaxAddressLength.
func (cc *ConfigChange) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 24+4 { // 3 uint64 fields + address length prefix
		return 0, ErrBufferTooSmall
	}
	cc.ConfigChangeID = getUint64(buf[0:])
	cc.Type = getUint64(buf[8:])
	if cc.Type > maxConfigChangeType {
		return 0, ErrInvalidConfigChangeType
	}
	cc.ReplicaID = getUint64(buf[16:])
	offset := 24
	addr, n, err := getString(buf[offset:], MaxAddressLength)
	if err != nil {
		return 0, err
	}
	cc.Address = addr
	offset += n
	if len(buf) < offset+8 {
		return 0, ErrBufferTooSmall
	}
	cc.Initialize = getBool(buf[offset:])
	offset += 8
	return offset, nil
}

// Reset clears all fields for reuse.
func (cc *ConfigChange) Reset() {
	cc.ConfigChangeID = 0
	cc.Type = 0
	cc.ReplicaID = 0
	cc.Address = ""
	cc.Initialize = false
}
