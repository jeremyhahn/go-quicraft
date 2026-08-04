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

// membershipWireVersion is the layout revision of the Membership section,
// written as its first byte and required to match exactly on decode.
// Membership is always embedded in a larger buffer, so neither a length
// check nor a trailing-byte check can discriminate layouts; the explicit
// byte is the only in-band discriminator.
//
// Layout history:
//
//	v1: [ConfigChangeID:8][4 maps]                      (base 8)
//	v2: [ConfigChangeID:8][MaxWitnesses:8][Policy:8][4 maps] (base 24)
//	v3: [Version:1] prefixed to the v2 fixed fields          (base 25)
//
// The value tracks WireVersion so a single number identifies the format.
const membershipWireVersion uint8 = uint8(WireVersion)

// membershipFixedSize is the wire size of the Membership fixed region:
// Version(1) + ConfigChangeID(8) + MaxWitnesses(8) +
// RequireFullReplicaMajority(8).
const membershipFixedSize = 1 + 8 + 8 + 8 // 25 bytes

// Membership represents a Raft cluster membership configuration. It tracks
// the addresses of voting members, observers, witnesses, and removed
// replicas, plus the shard's replicated witness policy. The wire format
// encodes a version byte and the fixed fields (ConfigChangeID,
// MaxWitnesses, RequireFullReplicaMajority) followed by four maps
// (Addresses, Observers, Witnesses, Removed) each with sorted keys for
// deterministic output.
type Membership struct {
	// ConfigChangeID is the log index of the configuration change
	// that produced this membership.
	ConfigChangeID uint64
	// MaxWitnesses is the shard's replicated witness cap (0 = no explicit
	// cap). Part of membership state so every replica enforces the same
	// policy; changed via an UpdateWitnessPolicy config change.
	MaxWitnesses uint64
	// RequireFullReplicaMajority is the shard's replicated
	// full-replica-majority policy. Seeded at bootstrap from
	// config.WitnessConfig; changed via UpdateWitnessPolicy.
	RequireFullReplicaMajority bool
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
// Format: [Version:1][ConfigChangeID:8][MaxWitnesses:8]
// [RequireFullReplicaMajority:8]
// [Addresses map][Observers map][Witnesses map][Removed boolMap]
func (m *Membership) Size() int {
	return membershipFixedSize + mapSize(m.Addresses) + mapSize(m.Observers) +
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
	buf[0] = membershipWireVersion
	putUint64(buf[1:], m.ConfigChangeID)
	putUint64(buf[9:], m.MaxWitnesses)
	putBool(buf[17:], m.RequireFullReplicaMajority)
	offset := membershipFixedSize
	offset += putMap(buf[offset:], m.Addresses)
	offset += putMap(buf[offset:], m.Observers)
	offset += putMap(buf[offset:], m.Witnesses)
	offset += putBoolMap(buf[offset:], m.Removed)
	return offset, nil
}

// UnmarshalFrom reads the membership from buf and returns bytes consumed.
// Map counts and address lengths are validated before allocation. Returns
// ErrUnsupportedVersion if the leading version byte is not
// membershipWireVersion, ErrBufferTooSmall on truncated input,
// ErrTooManyMembershipEntries if any map count exceeds the limit, or
// ErrAddressTooLong if any address string exceeds MaxAddressLength.
func (m *Membership) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, ErrBufferTooSmall
	}
	// The version byte is the sole layout discriminator, and is checked
	// ahead of the length so a legacy buffer reports the format mismatch
	// rather than an opaque short-buffer error. A v1 or v2 buffer starts
	// with the ConfigChangeID low byte instead, so any value other than the
	// exact current version is rejected before a single field is
	// interpreted. Version 0 — the low byte of every small ConfigChangeID,
	// including the empty v1/v2 membership that no heuristic could tell
	// apart from a v2 record with RequireFullReplicaMajority=false — is
	// never a valid version.
	if buf[0] != membershipWireVersion {
		return 0, ErrUnsupportedVersion
	}
	if len(buf) < membershipFixedSize+4 { // fixed region + first map count
		return 0, ErrBufferTooSmall
	}
	m.ConfigChangeID = getUint64(buf[1:])
	m.MaxWitnesses = getUint64(buf[9:])
	m.RequireFullReplicaMajority = getBool(buf[17:])
	offset := membershipFixedSize
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
	m.MaxWitnesses = 0
	m.RequireFullReplicaMajority = false
	m.Addresses = nil
	m.Observers = nil
	m.Witnesses = nil
	m.Removed = nil
}
