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
// as the Cmd payload of an EntryConfigChange entry. There is exactly ONE
// layout, and every field in it is mandatory:
//
//	[ConfigChangeID:8][Type:8][ReplicaID:8][Address:len+data][Initialize:8]
//	[MaxWitnesses:8][RequireFullReplicaMajority:8][NodeIdentity:len+data]
//
// The type carries no version byte and needs none. Under the pre-release
// pinning policy (see the package comment) there is one layout to decode, so
// there is nothing for a discriminator to discriminate: a payload that does not
// span the whole layout is refused rather than read as an earlier one. An
// absent NodeIdentity is spelled as a four-byte zero LENGTH PREFIX, not as a
// short buffer — the field is always present, and only its content may be
// empty.
//
// That distinction is the reason the layout is fixed rather than
// length-discriminated. See UnmarshalFrom.
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
	// NodeIdentity is the stable cryptographic node identity to commit for
	// the replica being added — the value quicraft.NodeIdentity derives from
	// that node's leaf certificate. Empty for RemoveNode and
	// UpdateWitnessPolicy, and empty for an add whose caller supplied none.
	//
	// An empty value is a real, encoded state (a zero length prefix), never an
	// absent field. It selects the weaker address-based authorization on the
	// receive side, so it must be something a WRITER chose and not something a
	// truncated buffer produced; see UnmarshalFrom.
	//
	// It travels through consensus alongside Address so the receive-side gate
	// reads a value no inbound message can write. It is supplied by the
	// OPERATOR through the Host API, never captured from a connection: see the
	// "Considered and rejected" note on Host.RequestAddNodeWithIdentity for why
	// learning it from the joiner's own connection is unsound in exactly the
	// threat model identities exist to close.
	NodeIdentity string
}

// configChangeFixedSize is the wire size of the fixed fields:
// ConfigChangeID(8) + Type(8) + ReplicaID(8) + Initialize(8) +
// MaxWitnesses(8) + RequireFullReplicaMajority(8) = 48 bytes
// plus the 4-byte address string length prefix.
const configChangeFixedSize = 6*8 + 4 // 52 bytes

// configChangeIdentitySize is the wire size the trailing NodeIdentity field
// adds: its own 4-byte length prefix plus the bytes. The prefix is
// unconditional, so an empty identity still costs four bytes — that is what
// makes an empty identity distinguishable from a payload that stops early.
func (cc *ConfigChange) configChangeIdentitySize() int {
	return 4 + len(cc.NodeIdentity)
}

// Size returns the total marshaled size of the config change in bytes.
func (cc *ConfigChange) Size() int {
	return configChangeFixedSize + len(cc.Address) + cc.configChangeIdentitySize()
}

// MarshalTo writes the config change into buf and returns bytes written.
// buf must be at least Size() bytes.
//
// Returns ErrBufferTooSmall if buf is too short, ErrInvalidConfigChangeType for
// a type UnmarshalFrom would reject, ErrAddressTooLong if Address exceeds
// MaxAddressLength, or ErrNodeIdentityTooLong if NodeIdentity exceeds
// MaxNodeIdentityLength.
//
// Those three are the READ side's checks applied on write, and they are not
// cosmetic here: a ConfigChange is marshaled into an Entry.Cmd and COMMITTED, so
// a payload this package can emit but not parse is a committed entry every
// replica faults on (peer.applyMembershipEntry returns ErrCorruptConfigEntry and
// the shard is failed) — a self-inflicted cluster-wide outage from an
// over-length operator-supplied address. Unlike Membership these limits are
// per-FIELD, so each keeps its own sentinel: getString is called with the right
// bound and the right error at both ends.
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
	offset += putString(buf[offset:], cc.NodeIdentity)
	return offset, nil
}

// UnmarshalFrom reads the config change from buf and returns bytes consumed.
// Returns ErrBufferTooSmall if buf does not span the whole layout,
// ErrInvalidConfigChangeType for an unknown type, ErrAddressTooLong if the
// address exceeds MaxAddressLength, or ErrNodeIdentityTooLong if the identity
// exceeds MaxNodeIdentityLength.
//
// # Why a short buffer is an error and not a shorter layout
//
// The decoder used to discriminate on LENGTH: a payload ending after
// Initialize was reported as a pre-witness-policy layout, and one ending after
// RequireFullReplicaMajority decoded successfully with an EMPTY NodeIdentity.
// The second of those was a security defect, not just dead compatibility code.
//
// A truncated ConfigChange was byte-indistinguishable from one that genuinely
// carried no identity, and an empty identity is precisely the state that falls
// back to the weaker address-based authorization on the receive side. A 61-byte
// prefix of a valid 78-byte encoding decoded with err == nil and silently
// downgraded the gate the identity exists to raise. Length is not a safe
// discriminator for a field whose absence weakens a check.
//
// Under the pinning policy there is nothing left to discriminate anyway — one
// layout, all of it required — so the length test is now a bound: anything that
// does not span the layout is refused, and an identity-less change is spelled
// with an explicit zero length prefix by MarshalTo.
func (cc *ConfigChange) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 24+4 { // 3 uint64 fields + address length prefix
		return 0, ErrBufferTooSmall
	}
	// Decoded into a LOCAL and assigned only on success, so the method is
	// all-or-nothing.
	//
	// Writing fields into the receiver as they were parsed meant a failure
	// part-way through left the receiver holding the NEW ConfigChangeID, Type,
	// ReplicaID and Address bolted onto the OLD NodeIdentity — a mixture that
	// describes no configuration any writer produced. NodeIdentity is what
	// selects identity-based authorization over the weaker address-based
	// path, so a stale one beside a new target is precisely the shape
	// Membership.UnmarshalFrom was hardened against (see its comment: "a
	// partial decode that kept a STALE Identities map would authorize a
	// replica the decoded configuration never named"). ConfigChange carries
	// the same class of field and did not get the same treatment.
	//
	// Every current caller declares a fresh value and abandons it on error, so
	// nothing is reachable today — which is exactly the argument the
	// Membership decoder rejected, because the invariant belongs in the
	// decoder rather than in the discipline of its callers. This type is also
	// the one a pooling refactor would reach for first: it is decoded once per
	// config entry during replay.
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
	// Initialize, MaxWitnesses and RequireFullReplicaMajority are checked as
	// one 24-byte region: they are all mandatory, so there is no boundary
	// between them at which a payload may legally stop.
	if len(buf) < offset+24 {
		return 0, ErrBufferTooSmall
	}
	decoded.Initialize = getBool(buf[offset:])
	offset += 8
	decoded.MaxWitnesses = getUint64(buf[offset:])
	offset += 8
	decoded.RequireFullReplicaMajority = getBool(buf[offset:])
	offset += 8
	// The trailing identity is read like any other field: getString requires
	// its four-byte length prefix, so a payload that ends here — the shape that
	// used to decode as "no identity" — returns ErrBufferTooSmall.
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
	cc.NodeIdentity = ""
}
