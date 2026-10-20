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

// membershipV1 is the one persisted Membership layout this build reads or
// writes, and — under the pre-release pinning policy in the package comment —
// the only one that will exist until the first production release.
//
// The v1 layout is:
//
//	[Version:1][ConfigChangeID:8][MaxWitnesses:8][RequireFullReplicaMajority:8]
//	[SessionExpiryEntries:8][MaxTotalSessions:8][MaxResponsesPerSession:8]
//	[MaxSessionsPerOwner:8]
//	[Addresses][Observers][Witnesses][Removed][Identities][RemovedIdentities]
//
// The number is 1 because the project is pre-release, not because the layout
// is the first one ever written: earlier development commits wrote other
// layouts under other numbers, and those bytes are gone with the dev clusters
// that held them. Under the pinning policy a layout change is made IN PLACE at
// version 1 and the consuming repos are updated to match, so this constant
// does not move again until 1.0.
const membershipV1 uint8 = 1

// membershipFormatVersion is the PERSISTED Membership layout revision this
// build writes AND the only one it reads, stamped as the section's first byte.
// Membership is always embedded in a larger buffer, so neither a length check
// nor a trailing-byte check can discriminate layouts; the explicit byte is the
// only in-band discriminator, and it is retained through the pinned period
// precisely so the 1.0 bump has a mechanism to use.
//
// This constant is DELIBERATELY NOT derived from WireVersion, and must
// never become so. The two version namespaces have different scopes and must
// be free to move independently at the first production release (see the
// compatibility contract in the package comment):
//
//   - WireVersion is TRANSPORT-scoped. It stamps MessageBatch.BinVer and
//     SnapshotChunk.BinVer, both of which are decoded by a live peer and
//     never read back off disk.
//   - membershipFormatVersion is PERSISTED-scoped. The bytes it stamps are
//     written to the WAL, into snapshot records and exported snapshot
//     artifacts, and into the EntryInitialMembership entry at log index 1,
//     where they outlive the process — and the binary — that wrote them.
//
// While the two shared a derivation, bumping WireVersion for an unrelated
// transport change (a new SnapshotChunk field, say) also changed the version
// byte every replica expects at log index 1, so no node could replay its own
// log: a cluster-wide brick with no migration path. That is why they are two
// constants even while they hold the same value.
const membershipFormatVersion uint8 = membershipV1

// The wire-version-covers-membership assertion: at COMPILE TIME, the TRANSPORT
// version must be at least the PERSISTED Membership version.
//
// Membership rides the wire inside Snapshot -> Message -> MessageBatch and
// directly inside SnapshotChunk, and both of those gate on
// BinVer == WireVersion by exact equality. A Membership layout bump that left
// WireVersion alone would therefore change the transport bytes while leaving
// the transport discriminator unchanged — two dialects behind one version
// number, which is exactly the ambiguity the version byte exists to remove.
//
// The assertion holds trivially while both are pinned at 1, and that is the
// point of keeping it: the pinning ends at the first production release, and
// this is what stops the two numbers drifting apart at the bump that matters.
//
// The relation is >= and not ==: a transport-only change (a new SnapshotChunk
// field) must be free to move WireVersion without touching a single persisted
// byte, so the transport number is a SUPREMUM over every layout on the wire.
// See the compatibility contract in the package comment.
//
// The expression is a typed-constant subtraction: if WireVersion ever falls
// below membershipFormatVersion the difference is negative, a negative value is
// not representable in uint64, and the package does not compile. Bumping
// Membership without bumping the wire version is a build error instead of a
// silent mismatch. The blank name is deliberate — nothing should ever READ this;
// its only job is to be type-checked.
const _ uint64 = WireVersion - uint64(membershipFormatVersion)

// Membership fixed-region offsets and sizes. There is exactly one layout, so
// there is exactly one set of these. The session policy's offset and its size
// are named separately from the total rather than folded into one number,
// because a future version that GREW the fixed region would have to state its
// own offsets — and the way that goes wrong is a single shared size constant
// silently reading one layout's first map count as another's last fixed field.
const (
	// sessionPolicyOffset is where the replicated session policy starts:
	// immediately after Version(1) + ConfigChangeID(8) + MaxWitnesses(8) +
	// RequireFullReplicaMajority(8).
	sessionPolicyOffset = 1 + 8 + 8 + 8 // 25 bytes

	// membershipSessionPolicySize is what the replicated session policy
	// occupies: SessionExpiryEntries(8) + MaxTotalSessions(8) +
	// MaxResponsesPerSession(8) + MaxSessionsPerOwner(8).
	membershipSessionPolicySize = 4 * 8 // 32 bytes

	// membershipFixedSize is the wire size of the whole fixed region, and thus
	// the offset the six-map region begins at.
	membershipFixedSize = sessionPolicyOffset + membershipSessionPolicySize // 57 bytes
)

// SessionPolicy is the shard's replicated session configuration: the four
// limits the deterministic apply path reads. They are replicated for the same
// reason the witness policy is — every replica must evaluate them identically
// or the state machines diverge with every Raft invariant intact:
//
//   - ExpiryEntries is the interval of rsm.StateMachine.Apply's expiry
//     checkpoint walk. Two replicas with different intervals expire different
//     sessions at different log indexes.
//   - MaxTotalSessions bounds session.Manager.Register shard-wide. A replica
//     that refuses a registration its peers admit answers ResultSessionExpired
//     for every later entry of that session while they apply it normally.
//   - MaxResponsesPerSession bounds the per-session RESPONSE CACHE
//     (CanRecord/RecordResult), and diverges the same way.
//   - MaxSessionsPerOwner bounds session.Manager.Register per OWNER, and is
//     what stops one principal consuming the whole table.
type SessionPolicy struct {
	// ExpiryEntries is the session inactivity threshold in applied log
	// entries. Zero means expiry is disabled for the shard — an explicit,
	// replicated choice, distinguishable from "no policy recorded" by IsSet.
	ExpiryEntries uint64
	// MaxTotalSessions is the cap on concurrently registered sessions.
	MaxTotalSessions uint64
	// MaxResponsesPerSession is the RESPONSE-CACHE DEPTH: the maximum number of
	// cached results one registered session retains for duplicate detection
	// (session.Manager.CanRecord/RecordResult). It is NOT, and has never been,
	// a limit on how many sessions anything may hold.
	//
	// It mirrors config.Config.MaxResponsesPerSession. The per-principal
	// session cap is MaxSessionsPerOwner, a different field with a different
	// meaning and a different failure mode — mixing the two up caps a shard at a
	// response depth or lets one client pin the whole session table, and both
	// have happened here before.
	MaxResponsesPerSession uint64
	// MaxSessionsPerOwner is the cap on how many sessions may be concurrently
	// registered under ONE owner — the principal an embedder declares in
	// Entry.OwnerID.
	//
	// It exists because MaxTotalSessions alone is a shard-wide bound and
	// nothing more: a client that mints session IDs (clientID is eight bytes
	// the caller picks) registers until the table is full and every other
	// tenant on the shard loses at-most-once delivery. That is a denial of
	// service available to anyone with propose rights, and the shard-wide cap
	// is precisely the thing being exhausted, so it cannot also be the defence.
	//
	// ZERO DISABLES IT. Sessions whose Entry.OwnerID is zero — every session
	// registered by an embedder that does not declare owners, which is all of
	// them before this field existed — are not counted against any owner and
	// are never refused by this limit. The cap is opt-in from both ends: the
	// embedder must supply an owner and the shard must commit a non-zero value.
	MaxSessionsPerOwner uint64
}

// IsSet reports whether this value is a policy the shard actually committed,
// as opposed to the zero value a section written without one decodes to.
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
// SessionExpiryEntries, MaxTotalSessions, MaxResponsesPerSession,
// MaxSessionsPerOwner) followed by six maps (Addresses, Observers, Witnesses,
// Removed, Identities, RemovedIdentities), each with sorted keys for
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
	// SessionExpiryEntries is the shard's replicated session inactivity
	// threshold, in applied log entries. Zero means expiry is disabled — see
	// SessionPolicy.IsSet for how that is told apart from a membership that
	// carries no committed policy at all.
	SessionExpiryEntries uint64
	// MaxTotalSessions is the shard's replicated cap on registered sessions.
	// Non-zero in every committed policy, which is what makes it the
	// policy-present discriminator.
	MaxTotalSessions uint64
	// MaxResponsesPerSession is the shard's replicated RESPONSE-CACHE DEPTH — the
	// number of cached results one session retains, not a session count. See
	// SessionPolicy.MaxResponsesPerSession for why the name says otherwise.
	MaxResponsesPerSession uint64
	// MaxSessionsPerOwner is the shard's replicated cap on concurrent sessions
	// per declared owner (Entry.OwnerID); zero disables it. See
	// SessionPolicy.MaxSessionsPerOwner.
	//
	// These four are seeded at bootstrap from config.Config and are replicated
	// for the reason SessionPolicy documents: the apply path reads them, so a
	// per-node value diverges the state machine silently. Unlike the witness
	// policy they have no config-change type yet — they are bootstrap-only, see
	// SessionPolicy.
	MaxSessionsPerOwner uint64
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
	// On any shard bootstrapped behind a transport it is COMPLETE: admission
	// requires an identity for every member (config.Config.RequireMemberIdentities),
	// and the receive-side gate authorizes a member by this map and by nothing
	// else. A replica with no entry has every message refused; the only way
	// to hold such a configuration is data written by a build that did not
	// require identities, or a host without a transport, where nothing
	// crosses the wire. A missing entry is never a permission.
	Identities map[uint64]string
	// RemovedIdentities maps the replica IDs in Removed to the node identity
	// the configuration held for them AT THE MOMENT OF REMOVAL. It is the
	// TOMBSTONE's authorization key, and it is the reason the removal deny-list
	// can be evaluated against something a sender cannot choose.
	//
	// A deny-list answerable only against the CLAIMED msg.From is one a
	// removed replica evades by naming a different replica ID: it still holds
	// its private key, so it still authenticates, and nothing else in
	// replicated state connects the connection it opens to the removal that
	// was committed for it. Retaining the identity here closes that, because the
	// receive path can ask "is the AUTHENTICATED identity on this connection one
	// this shard has removed?" — a question whose answer does not depend on any
	// field the sender fills in.
	//
	// # Retention: entries are NEVER evicted
	//
	// Evicting a tombstone re-opens the hole for exactly that replica, so no
	// eviction policy exists that is not also a re-admission policy; and a
	// time-based or count-based one would re-admit a removed replica at a moment
	// no operator chose and no two replicas would agree on (eviction would have
	// to be deterministic replicated state or the shards diverge). The map is
	// append-only for the life of the shard, exactly like Removed itself.
	//
	// That is safe because it adds NO new growth axis. Its key set is a SUBSET
	// of Removed's — an entry appears only when a removal is committed for a
	// member that had a committed identity — so it can never hold more entries
	// than Removed, which has carried the same unbounded-in-principle,
	// MaxMembershipEntries-in-practice shape since v1. A shard that would
	// overflow this map has already overflowed Removed and become unreadable.
	// Each entry costs 8 + 4 + len(identity) bytes on the wire (~81 for the
	// "spki:" + SHA-256-hex form this build produces), so the 10000-entry cap is
	// under a megabyte — well inside MaxBatchSize and MaxEntrySize.
	//
	// What the cap buys is a LOUD failure rather than a silent one: MarshalTo
	// refuses to write a map beyond it, so an operator who has genuinely removed
	// ten thousand replicas from one shard gets ErrTooManyMembershipEntries on
	// the config change instead of a membership that is durable and unreadable.
	// Forgetting a tombstone to stay under a cap is the one outcome that must
	// not happen quietly.
	//
	// # Residual: address-only shards
	//
	// It is as complete as Identities is: every member admitted behind a
	// transport carries an identity, so every removal records one here. A
	// member removed from a configuration written without identities leaves
	// no entry, and the deny-list for it is the claimed-msg.From check alone;
	// such a configuration is wiped and re-bootstrapped rather than migrated.
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

// Size returns the total marshaled size of the membership in bytes, in the
// current (membershipFormatVersion) layout.
// Format: [Version:1][ConfigChangeID:8][MaxWitnesses:8]
// [RequireFullReplicaMajority:8][SessionExpiryEntries:8][MaxTotalSessions:8]
// [MaxResponsesPerSession:8][MaxSessionsPerOwner:8]
// [Addresses map][Observers map][Witnesses map][Removed boolMap]
// [Identities map][RemovedIdentities map]
func (m *Membership) Size() int {
	return membershipFixedSize + mapSize(m.Addresses) + mapSize(m.Observers) +
		mapSize(m.Witnesses) + boolMapSize(m.Removed) + mapSize(m.Identities) +
		mapSize(m.RemovedIdentities)
}

// checkWritable refuses a membership this package's own UnmarshalFrom would
// not accept back.
//
// A limit enforced on READ only would let MarshalTo produce bytes UnmarshalFrom
// refuses — and for Membership that is reachable through a legal API call with a DURABLE consequence, not merely an
// asymmetry in a codec. Host.StartShard copies the caller's members map
// straight into Addresses, raft.BuildInitialMembershipEntry marshals it
// unchecked, and the result is committed as the EntryInitialMembership at log
// index 1. The next start decodes that entry, getMap refuses the oversized
// value, and the error propagates: StartShard aborts permanently, on data the
// shard itself wrote. "Durable but unreadable" is the failure
// waldb.checkMembershipSize already exists to prevent on its own layout; this
// is the same rule on this one.
//
// The bounds MIRROR the read side exactly, including which sentinel each
// reports, because the point is that the two agree. In particular every string
// map — Identities and RemovedIdentities included — is bounded at
// MaxAddressLength and reports ErrAddressTooLong, because getMap is shared by
// all five and that is the sentinel getMap returns. MaxNodeIdentityLength has
// the same value, so nothing is admitted or refused differently; reporting
// ErrNodeIdentityTooLong here would just mean marshal and unmarshal disagreed
// about the NAME of a refusal they agree on.
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

// MarshalTo writes the membership into buf and returns bytes written.
// buf must be at least Size() bytes. There is exactly one layout, and the
// encoder stamps membershipFormatVersion at buf[0] so the decoder can refuse
// anything that is not it.
//
// Returns ErrBufferTooSmall if buf is too short, ErrTooManyMembershipEntries if
// any map holds more than MaxMembershipEntries entries, or ErrAddressTooLong if
// any map value exceeds MaxAddressLength. The latter two are the read side's
// limits applied on write; see checkWritable for why a codec that can emit what
// it cannot parse is a durability bug here and not a style complaint.
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
	offset := membershipFixedSize
	offset += putMap(buf[offset:], m.Addresses)
	offset += putMap(buf[offset:], m.Observers)
	offset += putMap(buf[offset:], m.Witnesses)
	offset += putBoolMap(buf[offset:], m.Removed)
	offset += putMap(buf[offset:], m.Identities)
	offset += putMap(buf[offset:], m.RemovedIdentities)
	return offset, nil
}

// UnmarshalFrom reads the membership from buf and returns bytes consumed.
// The leading version byte must be membershipFormatVersion EXACTLY: there is
// one layout, and a byte naming any other is refused before a single field is
// interpreted. Map counts and address lengths are validated before allocation.
//
// Exact equality is the same gate MessageBatch and SnapshotChunk apply to
// BinVer, and it is what the pre-release pinning policy makes correct here too:
// no persisted bytes from another layout are meant to exist, because dev
// clusters are wiped rather than migrated (see the package comment). A range
// gate would silently decode a section this build cannot interpret — the
// version byte is the ONLY in-band discriminator, since Membership is always
// embedded in a larger buffer and neither a length check nor a trailing-byte
// check can tell one layout from another.
//
// Returns an *UnsupportedMembershipVersionError (which matches
// ErrUnsupportedVersion under errors.Is, and names both the version found and
// the one supported) if the version byte is anything else, ErrBufferTooSmall on
// truncated input, ErrTooManyMembershipEntries if any map count exceeds the
// limit, or ErrAddressTooLong if any address string exceeds MaxAddressLength.
//
// It is ALL OR NOTHING. Decoding runs into a local value and the receiver is
// assigned only on success, so a failure at any point leaves m exactly as it
// was rather than half-overwritten. That matters for the two authorization
// maps in particular: a partial decode that kept a STALE Identities map would
// authorize a replica the decoded configuration never named, and one that kept
// a stale RemovedIdentities would deny a sender it never tombstoned. Every call
// site abandons the value on error today, so neither is reachable; the
// invariant is a property of the decoder rather than of its callers.
func (m *Membership) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, ErrBufferTooSmall
	}
	// The version byte is checked ahead of the length so a buffer in any other
	// layout reports the format mismatch rather than an opaque short-buffer
	// error. A section written without a version byte starts with the
	// ConfigChangeID low byte instead, so version 0 — the low byte of every
	// small ConfigChangeID — is never a valid version and is rejected here.
	version := buf[0]
	if version != membershipFormatVersion {
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

// isZero reports whether the membership carries nothing: no configuration
// change id, no policy, and not one entry in any of the six maps. It is the
// predicate a Reset (or a never-populated) value satisfies, and it is what lets
// Snapshot tell "no membership" apart from "an empty one", without exporting a
// notion of emptiness this type has no other use for.
func (m *Membership) isZero() bool {
	return m.ConfigChangeID == 0 && m.MaxWitnesses == 0 &&
		!m.RequireFullReplicaMajority && m.SessionExpiryEntries == 0 &&
		m.MaxTotalSessions == 0 && m.MaxResponsesPerSession == 0 &&
		m.MaxSessionsPerOwner == 0 &&
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
	m.Addresses = nil
	m.Observers = nil
	m.Witnesses = nil
	m.Removed = nil
	m.Identities = nil
	m.RemovedIdentities = nil
}
