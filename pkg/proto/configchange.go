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

// ConfigChange is a membership change, the Cmd of an EntryConfigChange; one layout, every field required: [ConfigChangeID:8][Type:8][ReplicaID:8][Address][Initialize:8][MaxWitnesses:8][RequireFullReplicaMajority:8][WireVersion:8][NodeIdentity].
type ConfigChange struct {
	// ConfigChangeID is the log index that identifies this config change.
	ConfigChangeID uint64
	// Type is the change type: AddNode, RemoveNode, AddNonVoting,
	// AddWitness, or UpdateWitnessPolicy.
	Type uint64
	// ReplicaID is the target replica for the membership change.
	// Unused (zero) for UpdateWitnessPolicy.
	ReplicaID uint64
	// Address is the network address for the replica being added.
	// Empty for RemoveNode and UpdateWitnessPolicy operations.
	Address string
	// Initialize indicates whether the replica needs initial state.
	Initialize bool
	// MaxWitnesses is the new witness cap for UpdateWitnessPolicy
	// changes (0 = no explicit cap). Ignored for other types.
	MaxWitnesses uint64
	// RequireFullReplicaMajority is the new full-replica-majority policy
	// for UpdateWitnessPolicy changes. Ignored for other types.
	RequireFullReplicaMajority bool
	// WireVersion is the shard's new committed transport version for
	// UpdateWireVersion changes. Zero for every other type.
	WireVersion uint64
	// NodeIdentity is the node identity committed for the added replica, supplied by the operator; empty is encoded as a zero length prefix.
	NodeIdentity string
	// MustMatchConfigChangeID is in-memory only: it binds an add with ConfigChangeID 0 to a current config change ID of 0.
	MustMatchConfigChangeID bool
}

// configChangeFixedSize is the 56 bytes of fixed fields plus the 4-byte address length prefix.
const configChangeFixedSize = 7*8 + 4 // 60 bytes

// configChangeIdentitySize is NodeIdentity's 4-byte length prefix plus its bytes.
func (cc *ConfigChange) configChangeIdentitySize() int {
	return 4 + len(cc.NodeIdentity)
}

// Size returns the total marshaled size of the config change in bytes.
func (cc *ConfigChange) Size() int {
	return configChangeFixedSize + len(cc.Address) + cc.configChangeIdentitySize()
}

// MarshalTo writes the config change into buf (at least Size bytes), applying the read-side bounds: ErrBufferTooSmall, ErrInvalidConfigChangeType, ErrAddressTooLong, ErrNodeIdentityTooLong.
func (cc *ConfigChange) MarshalTo(buf []byte) (int, error) {
	if cc.Type > maxConfigChangeType {
		return 0, ErrInvalidConfigChangeType
	}
	if len(cc.Address) > MaxAddressLength {
		return 0, ErrAddressTooLong
	}
	if len(cc.NodeIdentity) > MaxNodeIdentityLength {
		return 0, ErrNodeIdentityTooLong
	}
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
	putUint64(buf[offset:], cc.MaxWitnesses)
	offset += 8
	putBool(buf[offset:], cc.RequireFullReplicaMajority)
	offset += 8
	putUint64(buf[offset:], cc.WireVersion)
	offset += 8
	offset += putString(buf[offset:], cc.NodeIdentity)
	return offset, nil
}

// UnmarshalFrom reads the config change from buf; a buffer that does not span the whole layout is ErrBufferTooSmall, never a shorter layout.
func (cc *ConfigChange) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 24+4 { // 3 uint64 fields + address length prefix
		return 0, ErrBufferTooSmall
	}
	// Decode into a local and assign only on success, so a failure leaves the receiver unchanged.
	var decoded ConfigChange
	decoded.ConfigChangeID = getUint64(buf[0:])
	decoded.Type = getUint64(buf[8:])
	if decoded.Type > maxConfigChangeType {
		return 0, ErrInvalidConfigChangeType
	}
	decoded.ReplicaID = getUint64(buf[16:])
	offset := 24
	addr, n, err := getString(buf[offset:], MaxAddressLength, ErrAddressTooLong)
	if err != nil {
		return 0, err
	}
	decoded.Address = addr
	offset += n
	// Initialize, MaxWitnesses, RequireFullReplicaMajority and WireVersion
	// are checked as one 32-byte region: they are all mandatory, so there is
	// no boundary between them at which a payload may legally stop.
	if len(buf) < offset+32 {
		return 0, ErrBufferTooSmall
	}
	decoded.Initialize = getBool(buf[offset:])
	offset += 8
	decoded.MaxWitnesses = getUint64(buf[offset:])
	offset += 8
	decoded.RequireFullReplicaMajority = getBool(buf[offset:])
	offset += 8
	decoded.WireVersion = getUint64(buf[offset:])
	offset += 8
	// The trailing identity is read like any other field: getString requires
	// its four-byte length prefix, so a payload that ends here — a truncation,
	// not "no identity" — returns ErrBufferTooSmall.
	identity, n, err := getString(buf[offset:], MaxNodeIdentityLength, ErrNodeIdentityTooLong)
	if err != nil {
		return 0, err
	}
	decoded.NodeIdentity = identity
	offset += n

	*cc = decoded
	return offset, nil
}

// Reset clears all fields for reuse.
func (cc *ConfigChange) Reset() {
	cc.ConfigChangeID = 0
	cc.Type = 0
	cc.ReplicaID = 0
	cc.Address = ""
	cc.Initialize = false
	cc.MaxWitnesses = 0
	cc.RequireFullReplicaMajority = false
	cc.WireVersion = 0
	cc.NodeIdentity = ""
	cc.MustMatchConfigChangeID = false
}
