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

// Membership represents a Raft cluster membership configuration. It tracks
// the addresses of voting members, observers, witnesses, and removed replicas.
// The wire format encodes ConfigChangeID as a fixed uint64 followed by four
// maps (Addresses, Observers, Witnesses, Removed) each with sorted keys for
// deterministic output.
type Membership struct {
	// ConfigChangeID is the log index of the configuration change
	// that produced this membership.
	ConfigChangeID uint64
	// Addresses maps replica IDs to network addresses for full voting members.
	Addresses map[uint64]string
	// Observers maps replica IDs to network addresses for non-voting observers.
	Observers map[uint64]string
	// Witnesses maps replica IDs to network addresses for witness replicas.
	Witnesses map[uint64]string
	// Removed tracks replica IDs that have been removed from the cluster.
	Removed map[uint64]bool
}

// Size returns the total marshaled size of the membership in bytes.
// Format: [ConfigChangeID:8][Addresses map][Observers map][Witnesses map][Removed boolMap]
func (m *Membership) Size() int {
	return 8 + mapSize(m.Addresses) + mapSize(m.Observers) +
		mapSize(m.Witnesses) + boolMapSize(m.Removed)
}

// MarshalTo writes the membership into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (m *Membership) MarshalTo(buf []byte) (int, error) {
	required := m.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], m.ConfigChangeID)
	offset := 8
	offset += putMap(buf[offset:], m.Addresses)
	offset += putMap(buf[offset:], m.Observers)
	offset += putMap(buf[offset:], m.Witnesses)
	offset += putBoolMap(buf[offset:], m.Removed)
	return offset, nil
}

// UnmarshalFrom reads the membership from buf and returns bytes consumed.
// Map counts and address lengths are validated before allocation. Returns
// ErrBufferTooSmall on truncated input, ErrTooManyMembershipEntries if
// any map count exceeds the limit, or ErrAddressTooLong if any address
// string exceeds MaxAddressLength.
func (m *Membership) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 8+4 { // ConfigChangeID + at least first map count
		return 0, ErrBufferTooSmall
	}
	m.ConfigChangeID = getUint64(buf[0:])
	offset := 8
	var n int
	var err error
	m.Addresses, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	m.Observers, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	m.Witnesses, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	m.Removed, n, err = getBoolMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	return offset, nil
}

// Reset clears all fields for reuse.
func (m *Membership) Reset() {
	m.ConfigChangeID = 0
	m.Addresses = nil
	m.Observers = nil
	m.Witnesses = nil
	m.Removed = nil
}
