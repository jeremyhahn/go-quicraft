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

// membershipV1 is the only Membership layout: [Version:1][ConfigChangeID:8][MaxWitnesses:8][RequireFullReplicaMajority:8][SessionExpiryEntries:8][MaxTotalSessions:8][MaxResponsesPerSession:8][MaxSessionsPerOwner:8][WireVersion:8][Addresses][Observers][Witnesses][Removed][Identities][RemovedIdentities].
const membershipV1 uint8 = 1

// membershipFormatVersion is the persisted Membership layout version written and read as the section's first byte; it is independent of the transport WireVersion.
const membershipFormatVersion uint8 = membershipV1

// previousMembershipFormatVersion is the previous minor's layout, equal to membershipFormatVersion before any release.
const previousMembershipFormatVersion uint8 = membershipFormatVersion

// acceptsMembershipFormatVersion reports whether v is in the closed read set
// {membershipFormatVersion, previousMembershipFormatVersion}. It is a set,
// not a range, so it never widens when a constant moves.
func acceptsMembershipFormatVersion(v uint8) bool {
	return v == membershipFormatVersion || v == previousMembershipFormatVersion
}

// Compile-time check that WireVersion >= membershipFormatVersion.
const _ uint64 = WireVersion - uint64(membershipFormatVersion)

// Membership fixed-region offsets and sizes.
const (
	// sessionPolicyOffset is where the replicated session policy starts:
	// immediately after Version(1) + ConfigChangeID(8) + MaxWitnesses(8) +
	// RequireFullReplicaMajority(8).
	sessionPolicyOffset = 1 + 8 + 8 + 8 // 25 bytes

	// membershipSessionPolicySize is what the replicated session policy
	// occupies: SessionExpiryEntries(8) + MaxTotalSessions(8) +
	// MaxResponsesPerSession(8) + MaxSessionsPerOwner(8).
	membershipSessionPolicySize = 4 * 8 // 32 bytes

	// wireVersionOffset is where the shard's committed wire version sits:
	// immediately after the session policy.
	wireVersionOffset = sessionPolicyOffset + membershipSessionPolicySize // 57 bytes

	// membershipFixedSize is the wire size of the whole fixed region, and thus
	// the offset the six-map region begins at.
	membershipFixedSize = wireVersionOffset + 8 // 65 bytes
)

// SessionPolicy is the shard's replicated session configuration: ExpiryEntries, MaxTotalSessions, MaxResponsesPerSession and MaxSessionsPerOwner.
type SessionPolicy struct {
	// ExpiryEntries is the session inactivity threshold in applied log
	// entries. Zero means expiry is disabled for the shard — an explicit,
	// replicated choice, distinguishable from "no policy recorded" by IsSet.
	ExpiryEntries uint64
	// MaxTotalSessions is the cap on concurrently registered sessions.
	MaxTotalSessions uint64
	// MaxResponsesPerSession is the response-cache depth per session, mirroring config.Config.MaxResponsesPerSession; the per-owner session cap is MaxSessionsPerOwner.
	MaxResponsesPerSession uint64
	// MaxSessionsPerOwner caps concurrent sessions per Entry.OwnerID; zero disables it, and sessions with OwnerID zero are never counted.
	MaxSessionsPerOwner uint64
}

// IsSet reports whether the shard committed this policy, using MaxTotalSessions, which is never zero when committed.
func (p SessionPolicy) IsSet() bool {
	return p.MaxTotalSessions != 0
}

// Membership is a Raft cluster membership: member, observer, witness and removed maps plus the replicated witness and session policies, encoded as a version byte, fixed fields and six sorted maps.
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
	// SessionExpiryEntries is the replicated session inactivity threshold in applied entries; zero disables expiry.
	SessionExpiryEntries uint64
	// MaxTotalSessions is the shard's replicated cap on registered sessions.
	// Non-zero in every committed policy, which is what makes it the
	// policy-present discriminator.
	MaxTotalSessions uint64
	// MaxResponsesPerSession is the replicated response-cache depth per session.
	MaxResponsesPerSession uint64
	// MaxSessionsPerOwner is the replicated per-owner session cap (zero disables); the session fields are seeded at bootstrap and never changed.
	MaxSessionsPerOwner uint64
	// WireVersion is the committed transport version; zero reads as PreviousWireVersion. Only UpdateWireVersion changes it, never lowering it.
	WireVersion uint64
	// Addresses maps replica IDs to network addresses for full voting members.
	Addresses map[uint64]string
	// Observers maps replica IDs to network addresses for non-voting observers.
	Observers map[uint64]string
	// Witnesses maps replica IDs to network addresses for witness replicas.
	Witnesses map[uint64]string
	// Removed tracks replica IDs that have been removed from the cluster.
	Removed map[uint64]bool
	// Identities maps replica IDs to their committed node identity, derived from the peer's public key; it is the only authorization key, and a missing entry is never a permission.
	Identities map[uint64]string
	// RemovedIdentities maps removed replica IDs to their identity at removal, so the deny-list checks the authenticated identity; entries are never evicted, and its keys are a subset of Removed.
	RemovedIdentities map[uint64]string
}

// SessionPolicy returns the shard's replicated session policy. The result
// reports IsSet false for a configuration that never had one committed — the
// fixed region is always present, so an absent policy is 32 zero bytes.
func (m *Membership) SessionPolicy() SessionPolicy {
	return SessionPolicy{
		ExpiryEntries:          m.SessionExpiryEntries,
		MaxTotalSessions:       m.MaxTotalSessions,
		MaxResponsesPerSession: m.MaxResponsesPerSession,
		MaxSessionsPerOwner:    m.MaxSessionsPerOwner,
	}
}

// SetSessionPolicy records p as the shard's replicated session policy.
func (m *Membership) SetSessionPolicy(p SessionPolicy) {
	m.SessionExpiryEntries = p.ExpiryEntries
	m.MaxTotalSessions = p.MaxTotalSessions
	m.MaxResponsesPerSession = p.MaxResponsesPerSession
	m.MaxSessionsPerOwner = p.MaxSessionsPerOwner
}

// Size returns the marshaled size: [Version:1][ConfigChangeID:8][MaxWitnesses:8][RequireFullReplicaMajority:8][SessionExpiryEntries:8][MaxTotalSessions:8][MaxResponsesPerSession:8][MaxSessionsPerOwner:8][WireVersion:8] then the six maps.
func (m *Membership) Size() int {
	return membershipFixedSize + mapSize(m.Addresses) + mapSize(m.Observers) +
		mapSize(m.Witnesses) + boolMapSize(m.Removed) + mapSize(m.Identities) +
		mapSize(m.RemovedIdentities)
}

// checkWritable refuses a membership UnmarshalFrom would not read back, using the same bounds and sentinels.
func (m *Membership) checkWritable() error {
	for _, sm := range []map[uint64]string{
		m.Addresses, m.Observers, m.Witnesses, m.Identities, m.RemovedIdentities,
	} {
		if err := checkWritableMap(sm); err != nil {
			return err
		}
	}
	return checkWritableBoolMap(m.Removed)
}

// MarshalTo writes the membership into buf (at least Size() bytes) and returns bytes written, or ErrBufferTooSmall, ErrTooManyMembershipEntries or ErrAddressTooLong.
func (m *Membership) MarshalTo(buf []byte) (int, error) {
	if err := m.checkWritable(); err != nil {
		return 0, err
	}
	required := m.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	buf[0] = membershipFormatVersion
	putUint64(buf[1:], m.ConfigChangeID)
	putUint64(buf[9:], m.MaxWitnesses)
	putBool(buf[17:], m.RequireFullReplicaMajority)
	putUint64(buf[sessionPolicyOffset:], m.SessionExpiryEntries)
	putUint64(buf[sessionPolicyOffset+8:], m.MaxTotalSessions)
	putUint64(buf[sessionPolicyOffset+16:], m.MaxResponsesPerSession)
	putUint64(buf[sessionPolicyOffset+24:], m.MaxSessionsPerOwner)
	putUint64(buf[wireVersionOffset:], m.WireVersion)
	offset := membershipFixedSize
	offset += putMap(buf[offset:], m.Addresses)
	offset += putMap(buf[offset:], m.Observers)
	offset += putMap(buf[offset:], m.Witnesses)
	offset += putBoolMap(buf[offset:], m.Removed)
	offset += putMap(buf[offset:], m.Identities)
	offset += putMap(buf[offset:], m.RemovedIdentities)
	return offset, nil
}

// UnmarshalFrom decodes buf all or nothing and returns bytes consumed; a version byte other than membershipFormatVersion returns *UnsupportedMembershipVersionError, else ErrBufferTooSmall, ErrTooManyMembershipEntries or ErrAddressTooLong.
func (m *Membership) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, ErrBufferTooSmall
	}
	// Check the version byte before the length; version 0 is never valid.
	version := buf[0]
	if !acceptsMembershipFormatVersion(version) {
		return 0, &UnsupportedMembershipVersionError{Found: version}
	}
	if len(buf) < membershipFixedSize+4 { // fixed region + first map count
		return 0, ErrBufferTooSmall
	}
	// Decoding into a local is what makes the method all-or-nothing; see the
	// doc comment.
	var decoded Membership
	decoded.ConfigChangeID = getUint64(buf[1:])
	decoded.MaxWitnesses = getUint64(buf[9:])
	decoded.RequireFullReplicaMajority = getBool(buf[17:])
	decoded.SessionExpiryEntries = getUint64(buf[sessionPolicyOffset:])
	decoded.MaxTotalSessions = getUint64(buf[sessionPolicyOffset+8:])
	decoded.MaxResponsesPerSession = getUint64(buf[sessionPolicyOffset+16:])
	decoded.MaxSessionsPerOwner = getUint64(buf[sessionPolicyOffset+24:])
	decoded.WireVersion = getUint64(buf[wireVersionOffset:])
	offset := membershipFixedSize
	var n int
	var err error
	decoded.Addresses, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	decoded.Observers, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	decoded.Witnesses, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	decoded.Removed, n, err = getBoolMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	decoded.Identities, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	decoded.RemovedIdentities, n, err = getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += n
	*m = decoded
	return offset, nil
}

// isZero reports whether the membership carries no change ID, no policy and no map entries.
func (m *Membership) isZero() bool {
	return m.ConfigChangeID == 0 && m.MaxWitnesses == 0 &&
		!m.RequireFullReplicaMajority && m.SessionExpiryEntries == 0 &&
		m.MaxTotalSessions == 0 && m.MaxResponsesPerSession == 0 &&
		m.MaxSessionsPerOwner == 0 && m.WireVersion == 0 &&
		len(m.Addresses) == 0 && len(m.Observers) == 0 && len(m.Witnesses) == 0 &&
		len(m.Removed) == 0 && len(m.Identities) == 0 &&
		len(m.RemovedIdentities) == 0
}

// Reset clears all fields for reuse.
func (m *Membership) Reset() {
	m.ConfigChangeID = 0
	m.MaxWitnesses = 0
	m.RequireFullReplicaMajority = false
	m.SessionExpiryEntries = 0
	m.MaxTotalSessions = 0
	m.MaxResponsesPerSession = 0
	m.MaxSessionsPerOwner = 0
	m.WireVersion = 0
	m.Addresses = nil
	m.Observers = nil
	m.Witnesses = nil
	m.Removed = nil
	m.Identities = nil
	m.RemovedIdentities = nil
}
