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

// Persisted Membership layout revisions. Every revision UnmarshalFrom can
// decode has a named constant here and a matching case in its version
// switch; the constant is the value of the section's leading byte.
//
// Layout history:
//
//	v1: [ConfigChangeID:8][4 maps]                           (base 8)
//	v2: [ConfigChangeID:8][MaxWitnesses:8][Policy:8][4 maps]  (base 24)
//	v3: [Version:1] prefixed to the v2 fixed fields           (base 25)
//	v4: v3 with the session policy appended to the fixed region (base 49)
//	    and a FIFTH map, Identities, appended after Removed
//
// v1 and v2 have no constant because they carry no version byte at all and
// cannot be discriminated in band: an empty v2 section is byte-identical to
// an empty v1 one, and both are indistinguishable from arbitrary
// ConfigChangeID bytes. They are unreadable by construction, which is the
// defect v3 exists to close.
const (
	// membershipV3 prefixes a version byte to the v2 fixed fields, making
	// the layout self-describing.
	membershipV3 uint8 = 3

	// membershipV4 adds two things v3 could not carry: the session policy
	// (SessionExpiryEntries, MaxTotalSessions, MaxSessionsPerClient) in the
	// fixed region, and the Identities map — the stable cryptographic node
	// identity committed membership assigns each replica — after Removed.
	//
	// The two arrived in the same unreleased layout revision deliberately.
	// Shipping the identity map as v4 and the session policy as v5 would cost
	// downstream a second on-disk format break for a version nothing has
	// written outside this development tree.
	membershipV4 uint8 = 4
)

// membershipFormatVersion is the PERSISTED Membership layout revision this
// build writes, stamped as the section's first byte. Membership is always
// embedded in a larger buffer, so neither a length check nor a
// trailing-byte check can discriminate layouts; the explicit byte is the
// only in-band discriminator.
//
// This constant is DELIBERATELY NOT derived from WireVersion, and must
// never become so again. The two version namespaces have different scopes
// and must evolve independently (see the compatibility contract in the
// package comment):
//
//   - WireVersion is TRANSPORT-scoped. It stamps MessageBatch.BinVer and
//     SnapshotChunk.BinVer, both of which are decoded by a live peer and
//     never read back off disk.
//   - membershipFormatVersion is PERSISTED-scoped. The bytes it stamps are
//     written to the WAL, into snapshot records and exported snapshot
//     artifacts, and into the EntryInitialMembership entry at log index 1,
//     where they outlive the process — and the binary — that wrote them.
//
// While the two shared a value, bumping WireVersion for an unrelated
// transport change (a new SnapshotChunk field, say) also changed the
// version byte every replica expects at log index 1, so no node could
// replay its own log: a cluster-wide brick with no migration path. Bump
// this constant only when the Membership BYTES change, and in the same
// change add a decode case for the outgoing layout.
const membershipFormatVersion uint8 = membershipV4

// minSupportedMembershipVersion is the oldest persisted Membership layout
// UnmarshalFrom will decode. Together with membershipFormatVersion it
// defines the supported window [min, current]; a reader accepts a RANGE
// because it must keep decoding the versions already on disk, not only the
// one it writes.
//
// The floor is 3 because v1 and v2 have no in-band discriminator. It exists
// as an explicit constant so that dropping support for an old layout is a
// deliberate one-line decision taken after its migration window, rather
// than an implicit consequence of a decoder that only ever matched the
// version it happened to write.
const minSupportedMembershipVersion uint8 = membershipV3

// Membership fixed-region sizes. Each readable layout owns its own constant
// because v4 GREW the fixed region: a single shared size would silently read
// the v4 session policy out of a v3 section's first map count.
const (
	// membershipFixedSizeV3 is the wire size of the v3 fixed region:
	// Version(1) + ConfigChangeID(8) + MaxWitnesses(8) +
	// RequireFullReplicaMajority(8).
	membershipFixedSizeV3 = 1 + 8 + 8 + 8 // 25 bytes

	// membershipSessionPolicySize is what the replicated session policy adds
	// to the fixed region: SessionExpiryEntries(8) + MaxTotalSessions(8) +
	// MaxSessionsPerClient(8).
	membershipSessionPolicySize = 3 * 8 // 24 bytes

	// membershipFixedSizeV4 is the wire size of the v4 fixed region: the v3
	// fields followed by the session policy, at the offsets sessionPolicyOffset
	// names.
	membershipFixedSizeV4 = membershipFixedSizeV3 + membershipSessionPolicySize // 49 bytes

	// sessionPolicyOffset is where the v4 session policy starts — immediately
	// after the v3 fields, which v4 leaves byte-identical so the shared decode
	// path can read them from either layout.
	sessionPolicyOffset = membershipFixedSizeV3
)

// SessionPolicy is the shard's replicated session configuration: the three
// limits the deterministic apply path reads. They are replicated for the same
// reason the witness policy is — every replica must evaluate them identically
// or the state machines diverge with every Raft invariant intact:
//
//   - ExpiryEntries is the interval of rsm.StateMachine.Apply's expiry
//     checkpoint walk. Two replicas with different intervals expire different
//     sessions at different log indexes.
//   - MaxTotalSessions bounds session.Manager.Register. A replica that refuses
//     a registration its peers admit answers ResultSessionExpired for every
//     later entry of that session while they apply it normally.
//   - MaxSessionsPerClient bounds the per-session response cache
//     (CanRecord/RecordResult), and diverges the same way.
type SessionPolicy struct {
	// ExpiryEntries is the session inactivity threshold in applied log
	// entries. Zero means expiry is disabled for the shard — an explicit,
	// replicated choice, distinguishable from "no policy recorded" by IsSet.
	ExpiryEntries uint64
	// MaxTotalSessions is the cap on concurrently registered sessions.
	MaxTotalSessions uint64
	// MaxSessionsPerClient is the cap on cached responses retained per
	// session (config.Config.MaxSessionsPerClient; the name predates the
	// wiring).
	MaxSessionsPerClient uint64
}

// IsSet reports whether this value is a policy the shard actually committed,
// as opposed to the zero value a v3 section decodes to.
//
// MaxTotalSessions is the discriminator because it can never legitimately be
// zero in a committed policy: config.Validate rejects a zero MaxTotalSessions
// and SetDefaults fills it, so every seeded policy carries a non-zero cap.
// That is what lets ExpiryEntries==0 keep its own meaning — expiry explicitly
// DISABLED cluster-wide — instead of being read as an absent policy and
// silently falling back to whatever the local node happens to be configured
// with, which is the divergence this whole field exists to close.
func (p SessionPolicy) IsSet() bool {
	return p.MaxTotalSessions != 0
}

// Membership represents a Raft cluster membership configuration. It tracks
// the addresses of voting members, observers, witnesses, and removed
// replicas, plus the shard's replicated witness policy and session policy.
// The wire format encodes a version byte and the fixed fields
// (ConfigChangeID, MaxWitnesses, RequireFullReplicaMajority,
// SessionExpiryEntries, MaxTotalSessions, MaxSessionsPerClient) followed by
// four maps (Addresses, Observers, Witnesses, Removed) and the Identities
// map, each with sorted keys for deterministic output.
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
	// SessionExpiryEntries is the shard's replicated session inactivity
	// threshold, in applied log entries. Zero means expiry is disabled — see
	// SessionPolicy.IsSet for how that is told apart from a v3 section that
	// carries no policy at all.
	SessionExpiryEntries uint64
	// MaxTotalSessions is the shard's replicated cap on registered sessions.
	// Non-zero in every committed policy, which is what makes it the
	// policy-present discriminator.
	MaxTotalSessions uint64
	// MaxSessionsPerClient is the shard's replicated cap on responses cached
	// per session.
	//
	// These three are seeded at bootstrap from config.Config and are replicated
	// for the reason SessionPolicy documents: the apply path reads them, so a
	// per-node value diverges the state machine silently. Unlike the witness
	// policy they have no config-change type yet — they are bootstrap-only, see
	// SessionPolicy.
	MaxSessionsPerClient uint64
	// Addresses maps replica IDs to network addresses for full voting members.
	Addresses map[uint64]string
	// Observers maps replica IDs to network addresses for non-voting observers.
	Observers map[uint64]string
	// Witnesses maps replica IDs to network addresses for witness replicas.
	Witnesses map[uint64]string
	// Removed tracks replica IDs that have been removed from the cluster.
	Removed map[uint64]bool
	// Identities maps replica IDs to the stable cryptographic NODE IDENTITY
	// the shard has committed for them — the value quicraft.NodeIdentity
	// derives from a peer's leaf certificate (see the receive-side gate in
	// hostMessageHandler.authorizeInbound).
	//
	// It is the authorization key that the address is not. An address is
	// matched against certificate SANs, so a wildcard or shared-SAN
	// certificate covers every member's address and its holder can assert any
	// member's msg.From; an identity is derived from the peer's PUBLIC KEY, so
	// only the holder of the matching private key can present it. Recording it
	// here — inside the same replicated state, reached through the same
	// consensus path as the address — is what makes it unforgeable by an
	// inbound message.
	//
	// It is SPARSE and may be empty. A shard bootstrapped before identities
	// existed, or by an operator who supplied none, has no entry for its
	// members, and the receive-side gate then falls back to the committed
	// ADDRESS check for those replicas (or refuses them outright under
	// TransportConfig.RequireMemberIdentity). A missing entry is therefore a
	// compatibility state, never a permission.
	Identities map[uint64]string
}

// SessionPolicy returns the shard's replicated session policy. The result
// reports IsSet false for a configuration decoded from a v3 section, which
// carried no policy.
func (m *Membership) SessionPolicy() SessionPolicy {
	return SessionPolicy{
		ExpiryEntries:        m.SessionExpiryEntries,
		MaxTotalSessions:     m.MaxTotalSessions,
		MaxSessionsPerClient: m.MaxSessionsPerClient,
	}
}

// SetSessionPolicy records p as the shard's replicated session policy.
func (m *Membership) SetSessionPolicy(p SessionPolicy) {
	m.SessionExpiryEntries = p.ExpiryEntries
	m.MaxTotalSessions = p.MaxTotalSessions
	m.MaxSessionsPerClient = p.MaxSessionsPerClient
}

// Size returns the total marshaled size of the membership in bytes, in the
// current (membershipFormatVersion) layout.
// Format: [Version:1][ConfigChangeID:8][MaxWitnesses:8]
// [RequireFullReplicaMajority:8][SessionExpiryEntries:8][MaxTotalSessions:8]
// [MaxSessionsPerClient:8]
// [Addresses map][Observers map][Witnesses map][Removed boolMap]
// [Identities map]
func (m *Membership) Size() int {
	return membershipFixedSizeV4 + mapSize(m.Addresses) + mapSize(m.Observers) +
		mapSize(m.Witnesses) + boolMapSize(m.Removed) + mapSize(m.Identities)
}

// MarshalTo writes the membership into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf
// is too short. The encoder only ever writes membershipFormatVersion —
// old layouts are readable, never writable.
func (m *Membership) MarshalTo(buf []byte) (int, error) {
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
	putUint64(buf[sessionPolicyOffset+16:], m.MaxSessionsPerClient)
	offset := membershipFixedSizeV4
	offset += putMap(buf[offset:], m.Addresses)
	offset += putMap(buf[offset:], m.Observers)
	offset += putMap(buf[offset:], m.Witnesses)
	offset += putBoolMap(buf[offset:], m.Removed)
	offset += putMap(buf[offset:], m.Identities)
	return offset, nil
}

// UnmarshalFrom reads the membership from buf and returns bytes consumed.
// The leading version byte selects the decode path: every layout in
// [minSupportedMembershipVersion, membershipFormatVersion] is decoded,
// which is what lets a node keep replaying bytes an older binary wrote.
// Map counts and address lengths are validated before allocation.
//
// Returns an *UnsupportedMembershipVersionError (which matches
// ErrUnsupportedVersion under errors.Is, and names both the version found
// and the supported range) if the version byte falls outside that window,
// ErrBufferTooSmall on truncated input, ErrTooManyMembershipEntries if any
// map count exceeds the limit, or ErrAddressTooLong if any address string
// exceeds MaxAddressLength.
func (m *Membership) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, ErrBufferTooSmall
	}
	// The version byte is the sole layout discriminator, and is checked
	// ahead of the length so a legacy buffer reports the format mismatch
	// rather than an opaque short-buffer error. A v1 or v2 buffer starts
	// with the ConfigChangeID low byte instead, so anything outside the
	// supported window is rejected before a single field is interpreted.
	// Version 0 — the low byte of every small ConfigChangeID, including the
	// empty v1/v2 membership that no heuristic could tell apart from a v2
	// record with RequireFullReplicaMajority=false — is below the floor and
	// is never a valid version.
	version := buf[0]
	if version < minSupportedMembershipVersion || version > membershipFormatVersion {
		return 0, &UnsupportedMembershipVersionError{Found: version}
	}
	// One case per readable layout. The window is [3, 4]: a v3 record written
	// by an older binary — every EntryInitialMembership at log index 1 in an
	// existing cluster, and every snapshot and WAL membership section it ever
	// wrote — still decodes, which is what lets a node replay its own log after
	// the upgrade. It simply decodes with no Identities.
	switch version {
	case membershipV3:
		return m.unmarshalV3(buf)
	case membershipV4:
		return m.unmarshalV4(buf)
	default:
		// Reached only if membershipFormatVersion (or the floor) moved
		// without a matching case. Refuse rather than decode an unknown
		// layout with the v3 reader.
		return 0, &UnsupportedMembershipVersionError{Found: version}
	}
}

// unmarshalV3 decodes the v3 layout — [Version:1][ConfigChangeID:8]
// [MaxWitnesses:8][RequireFullReplicaMajority:8] followed by the Addresses,
// Observers, Witnesses and Removed maps — and returns bytes consumed. The
// caller has already validated buf[0]; buf may be longer than the section.
//
// Identities and the session policy are cleared, not left alone: Membership
// values are reused (see Reset), so a stale identity map surviving a v3 decode
// would silently authorize a replica the decoded configuration never named,
// and a stale session policy would be read as one this shard committed.
func (m *Membership) unmarshalV3(buf []byte) (int, error) {
	offset, err := m.unmarshalCommon(buf, membershipFixedSizeV3)
	if err != nil {
		return 0, err
	}
	m.SetSessionPolicy(SessionPolicy{})
	m.Identities = nil
	return offset, nil
}

// unmarshalV4 decodes the v4 layout — the v3 layout with the session policy
// appended to the fixed region and the Identities map appended after Removed —
// and returns bytes consumed. The caller has already validated buf[0]; buf may
// be longer than the section.
func (m *Membership) unmarshalV4(buf []byte) (int, error) {
	// The session policy sits INSIDE the fixed region, so it is read before
	// the shared decode reaches the maps at their v4 offset. Its own bound
	// check runs first: unmarshalCommon validates the length it was told to
	// expect, which is no help to the three reads below it.
	if len(buf) < membershipFixedSizeV4 {
		return 0, ErrBufferTooSmall
	}
	m.SessionExpiryEntries = getUint64(buf[sessionPolicyOffset:])
	m.MaxTotalSessions = getUint64(buf[sessionPolicyOffset+8:])
	m.MaxSessionsPerClient = getUint64(buf[sessionPolicyOffset+16:])
	offset, err := m.unmarshalCommon(buf, membershipFixedSizeV4)
	if err != nil {
		return 0, err
	}
	identities, n, err := getMap(buf[offset:])
	if err != nil {
		return 0, err
	}
	m.Identities = identities
	return offset + n, nil
}

// unmarshalCommon decodes the region v3 and v4 share — the v3 fixed fields
// plus the Addresses, Observers, Witnesses and Removed maps — and returns the
// offset just past it. Keeping it in one place is what guarantees the two
// layouts cannot drift apart in the region they are supposed to agree on.
//
// fixedSize is the caller's layout's fixed-region size, which is where its map
// region begins: the v3 and v4 fixed fields are byte-identical, but v4's are
// followed by the session policy, so the two layouts share the FIELDS and not
// the offset the maps start at.
func (m *Membership) unmarshalCommon(buf []byte, fixedSize int) (int, error) {
	if len(buf) < fixedSize+4 { // fixed region + first map count
		return 0, ErrBufferTooSmall
	}
	m.ConfigChangeID = getUint64(buf[1:])
	m.MaxWitnesses = getUint64(buf[9:])
	m.RequireFullReplicaMajority = getBool(buf[17:])
	offset := fixedSize
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
	m.SessionExpiryEntries = 0
	m.MaxTotalSessions = 0
	m.MaxSessionsPerClient = 0
	m.Addresses = nil
	m.Observers = nil
	m.Witnesses = nil
	m.Removed = nil
	m.Identities = nil
}
