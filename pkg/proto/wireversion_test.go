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

import (
	"bytes"
	"encoding/hex"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// Tests for the version discriminators this package carries: that both are
// pinned at 1 for the pre-release period, that every gate on them is an EXACT
// match, that they are not derived from one another, and that the layout they
// name is byte-stable.

// TestWireVersionIsOne pins the TRANSPORT wire version at 1.
//
// Every format in this repository is pinned at 1 until the first production
// release: a layout change is made IN PLACE and the consuming repos are
// rebuilt to match (see the compatibility contract in the package comment). So
// this test does NOT mean "the transport format has never changed" — it means
// the number does not move for a pre-release change, and a diff that moves it
// is either the 1.0 release or a mistake.
//
// It deliberately does NOT assert WireVersion == membershipFormatVersion. The
// two namespaces stay independent so that the first post-1.0 transport-only
// bump cannot change the version byte expected at log index 1. What it DOES
// assert is the supremum relation — the transport number must never fall below
// any persisted layout it carries, because MessageBatch and SnapshotChunk gate
// on BinVer by exact equality and Membership rides inside both. That relation
// is also a compile-time constant assertion in membership.go; this is its
// runtime statement, so a reader of the test file sees why the numbers are what
// they are.
func TestWireVersionIsOne(t *testing.T) {
	if WireVersion != 1 {
		t.Fatalf("WireVersion = %d, want 1: every format is pinned at 1 until "+
			"the first production release", WireVersion)
	}
	if WireVersion < uint64(membershipFormatVersion) {
		t.Fatalf("WireVersion %d is below membershipFormatVersion %d: Membership "+
			"rides the wire inside Snapshot->Message->MessageBatch and inside "+
			"SnapshotChunk, both of which gate on BinVer by EXACT equality, so a "+
			"Membership layout change that leaves BinVer alone puts two dialects "+
			"behind one version number", WireVersion, membershipFormatVersion)
	}
}

// TestPersistedMembershipVersionPinned pins the persisted Membership format
// version at 1, and pins that there is exactly ONE of them.
//
// The value describes bytes on disk: the WAL, snapshot records, exported
// artifacts, and the EntryInitialMembership entry at log index 1. Pre-release
// those bytes are disposable — dev clusters are wiped and re-synced rather than
// migrated — which is what makes an in-place layout change legitimate and an
// exact-match decoder correct. From the first production release onward this
// constant may only be raised together with a decode path for the outgoing
// layout and a migration story. Neither may track WireVersion.
func TestPersistedMembershipVersionPinned(t *testing.T) {
	if membershipFormatVersion != 1 {
		t.Fatalf("membershipFormatVersion = %d, want 1", membershipFormatVersion)
	}
	if membershipFormatVersion != membershipV1 {
		t.Fatalf("membershipFormatVersion = %d, want membershipV1 (%d)",
			membershipFormatVersion, membershipV1)
	}
}

// TestMembershipVersionNotDerivedFromWireVersion is a source-level guard: it
// parses membership.go and asserts that neither version constant is defined
// in terms of WireVersion.
//
// A value check cannot catch this — both namespaces are 1 today, so a
// contributor who "simplifies" the constant to uint8(WireVersion) leaves every
// runtime assertion green while arming the defect for 1.0: the first
// transport-only WireVersion bump would then silently change the version byte
// expected at log index 1 and leave a released cluster unable to replay its own
// log.
func TestMembershipVersionNotDerivedFromWireVersion(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "membership.go", nil, 0)
	if err != nil {
		t.Fatalf("parse membership.go: %v", err)
	}
	// membershipV1 is guarded too: membershipFormatVersion is defined in terms
	// of it, so re-deriving IT from WireVersion would reintroduce the coupling
	// one level down.
	guarded := map[string]bool{
		"membershipV1":            false,
		"membershipFormatVersion": false,
	}
	ast.Inspect(f, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if _, watched := guarded[name.Name]; !watched {
				continue
			}
			guarded[name.Name] = true
			if i >= len(spec.Values) {
				continue
			}
			ast.Inspect(spec.Values[i], func(v ast.Node) bool {
				id, ok := v.(*ast.Ident)
				if ok && id.Name == "WireVersion" {
					t.Errorf("%s is defined in terms of WireVersion; the persisted "+
						"Membership format version must stay independent of the "+
						"transport wire version", name.Name)
				}
				return true
			})
		}
		return true
	})
	for name, seen := range guarded {
		if !seen {
			t.Fatalf("constant %s not found in membership.go; update this guard", name)
		}
	}
}

// TestMembershipVersionExactMatch exercises the decode gate: the one supported
// version decodes, EVERY other byte value is refused with the typed error, and
// the message names both the version found and the version supported.
//
// Exact equality is what the pinning policy buys. There is no window to widen
// and no earlier layout to fall back to, so a section stamped with anything
// else was written by a binary built from a different pre-release commit and
// must be refused rather than read at this layout's offsets.
func TestMembershipVersionExactMatch(t *testing.T) {
	m := Membership{
		ConfigChangeID:             9,
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
		Addresses:                  map[uint64]string{1: "n1:9000"},
	}
	buf := make([]byte, m.Size())
	if _, err := m.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var accepted Membership
	if _, err := accepted.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom(version %d): unexpected error %v",
			membershipFormatVersion, err)
	}

	// Every byte value other than the supported one. The neighbours of the
	// supported version and the numbers earlier pre-release commits used are
	// covered by construction rather than by an enumeration that could go stale.
	for v := 0; v <= 0xFF; v++ {
		version := uint8(v)
		if version == membershipFormatVersion {
			continue
		}
		probe := append([]byte{}, buf...)
		probe[0] = version
		var got Membership
		_, err := got.UnmarshalFrom(probe)
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("UnmarshalFrom(version %d): got %v, want ErrUnsupportedVersion",
				version, err)
		}
		var verr *UnsupportedMembershipVersionError
		if !errors.As(err, &verr) {
			t.Fatalf("UnmarshalFrom(version %d): got %T, want *UnsupportedMembershipVersionError",
				version, err)
		}
		if verr.Found != version {
			t.Fatalf("Found = %d, want %d", verr.Found, version)
		}
	}

	// The message must let an operator diagnose the mismatch without reading
	// the source: the value found and the value supported.
	err := (&UnsupportedMembershipVersionError{Found: 7}).Error()
	for _, want := range []string{"7", strconv.Itoa(int(membershipFormatVersion))} {
		if !strings.Contains(err, want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// membershipGoldenFixedHead is the head of the fixed region: ConfigChangeID
// through RequireFullReplicaMajority. It is separate from the session policy
// that follows it so the golden below spells the fixed region out in the two
// pieces the offset constants name.
const membershipGoldenFixedHead = "0807060504030201" + // ConfigChangeID
	"0200000000000000" + // MaxWitnesses
	"0100000000000000" // RequireFullReplicaMajority

// membershipGoldenSessionPolicy is the replicated session policy, which closes
// the fixed region. The values are goldenMembershipSessionPolicy.
const membershipGoldenSessionPolicy = "a086010000000000" + // SessionExpiryEntries = 100000
	"0040000000000000" + // MaxTotalSessions = 16384
	"8000000000000000" + // MaxSessionsPerClient = 128 (response-cache depth)
	"4000000000000000" // MaxSessionsPerOwner = 64

// membershipGoldenMaps is the first four maps of the map region.
const membershipGoldenMaps = "02000000" + // Addresses count
	"0100000000000000" + "03000000" + "613a31" + // 1 -> "a:1"
	"0200000000000000" + "03000000" + "623a32" + // 2 -> "b:2"
	"01000000" + // Observers count
	"0300000000000000" + "03000000" + "633a33" + // 3 -> "c:3"
	"01000000" + // Witnesses count
	"0400000000000000" + "03000000" + "643a34" + // 4 -> "d:4"
	"01000000" + // Removed count
	"0500000000000000" + "0100000000000000" // 5 -> true

// membershipGoldenIdentities is the fifth map: the committed node identity of
// an active member.
const membershipGoldenIdentities = "01000000" + // Identities count
	"0100000000000000" + "07000000" + "73706b693a6161" // 1 -> "spki:aa"

// membershipGoldenRemovedIdentities is the sixth map: one tombstoned replica
// and the identity it held when it was removed.
const membershipGoldenRemovedIdentities = "01000000" + // RemovedIdentities count
	"0500000000000000" + "07000000" + "73706b693a6262" // 5 -> "spki:bb"

// goldenMembershipSessionPolicy is the policy membershipGoldenSessionPolicy
// spells out in bytes.
func goldenMembershipSessionPolicy() SessionPolicy {
	return SessionPolicy{
		ExpiryEntries:        100000,
		MaxTotalSessions:     16384,
		MaxSessionsPerClient: 128,
		MaxSessionsPerOwner:  64,
	}
}

// goldenMembershipFixture is the membership the golden describes, minus the
// session policy and the two identity maps, which the callers add.
func goldenMembershipFixture() Membership {
	return Membership{
		ConfigChangeID:             0x0102030405060708,
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
		Addresses:                  map[uint64]string{2: "b:2", 1: "a:1"},
		Observers:                  map[uint64]string{3: "c:3"},
		Witnesses:                  map[uint64]string{4: "d:4"},
		Removed:                    map[uint64]bool{5: true},
	}
}

// TestMembershipGoldenEncoding asserts the exact bytes this binary writes.
// These bytes go into the WAL, into snapshot records and exported artifacts,
// and into the EntryInitialMembership entry at log index 1.
//
// Pinning the version at 1 does NOT make this test optional — it makes it the
// only thing standing between a layout change and a silent one. The version
// byte no longer moves when the layout does, so a diff that alters these bytes
// produces two mutually unreadable binaries both stamping 1, which is the cost
// the pinning policy accepts and this golden makes visible in review. A change
// here means every consuming repo (go-qrdb, go-xkms, go-quicbaas,
// go-trusted-ca) is rebuilt in lockstep and every dev cluster is wiped.
//
// The golden is spelled as its regions — fixed head, session policy, four maps,
// Identities, RemovedIdentities — so the layout is checked by construction
// rather than asserted in a comment.
func TestMembershipGoldenEncoding(t *testing.T) {
	m := goldenMembershipFixture()
	m.SetSessionPolicy(goldenMembershipSessionPolicy())
	m.Identities = map[uint64]string{1: "spki:aa"}
	m.RemovedIdentities = map[uint64]string{5: "spki:bb"}

	const golden = "01" + // version byte
		membershipGoldenFixedHead +
		membershipGoldenSessionPolicy +
		membershipGoldenMaps +
		membershipGoldenIdentities +
		membershipGoldenRemovedIdentities

	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if got := hex.EncodeToString(buf[:n]); got != golden {
		t.Fatalf("encoding changed:\n got %s\nwant %s", got, golden)
	}

	raw, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	var got Membership
	consumed, err := got.UnmarshalFrom(raw)
	if err != nil {
		t.Fatalf("UnmarshalFrom(golden): %v", err)
	}
	if consumed != len(raw) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(raw))
	}
	assertMembershipEqual(t, &m, &got)
	if got.Identities[1] != "spki:aa" {
		t.Fatalf("Identities lost in round trip: %+v", got.Identities)
	}
	if got.RemovedIdentities[5] != "spki:bb" {
		t.Fatalf("RemovedIdentities lost in round trip: %+v", got.RemovedIdentities)
	}
	if got.MaxWitnesses != m.MaxWitnesses || !got.RequireFullReplicaMajority {
		t.Fatalf("policy fields lost: %+v", got)
	}
	if got.SessionPolicy() != goldenMembershipSessionPolicy() {
		t.Fatalf("session policy lost in round trip: %+v", got.SessionPolicy())
	}
}

// TestMembershipFullRoundTripIsByteIdentical is the equivalence statement for
// the single-version collapse: a membership with all six maps populated, the
// session policy set and RemovedIdentities non-empty must survive
// MarshalTo/UnmarshalFrom field for field, and re-encode to the SAME bytes.
//
// Re-encoding is the half that catches a decoder which "succeeds" while reading
// a field at the wrong offset: the value round-trips through a struct
// comparison only if every offset agreed in both directions.
func TestMembershipFullRoundTripIsByteIdentical(t *testing.T) {
	m := Membership{
		ConfigChangeID:             0xDEADBEEFCAFE,
		MaxWitnesses:               3,
		RequireFullReplicaMajority: true,
		Addresses:                  map[uint64]string{1: "a:1", 2: "b:2", 3: "c:3"},
		Observers:                  map[uint64]string{10: "o:10", 11: "o:11"},
		Witnesses:                  map[uint64]string{20: "w:20"},
		Removed:                    map[uint64]bool{30: true, 31: true},
		Identities:                 map[uint64]string{1: "spki:one", 2: "spki:two"},
		RemovedIdentities:          map[uint64]string{30: "spki:thirty", 31: "spki:thirtyone"},
	}
	m.SetSessionPolicy(SessionPolicy{
		ExpiryEntries: 1234, MaxTotalSessions: 5678, MaxSessionsPerClient: 90,
	})

	first := make([]byte, m.Size())
	n, err := m.MarshalTo(first)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	first = first[:n]

	var got Membership
	consumed, err := got.UnmarshalFrom(first)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("consumed %d bytes, wrote %d", consumed, n)
	}
	assertMembershipEqual(t, &m, &got)
	if got.MaxWitnesses != m.MaxWitnesses ||
		got.RequireFullReplicaMajority != m.RequireFullReplicaMajority {
		t.Fatalf("witness policy lost: %+v", got)
	}

	second := make([]byte, got.Size())
	n2, err := got.MarshalTo(second)
	if err != nil {
		t.Fatalf("re-MarshalTo: %v", err)
	}
	if !bytes.Equal(first, second[:n2]) {
		t.Fatalf("re-encoding differs:\n got %x\nwant %x", second[:n2], first)
	}
}

// TestSnapshotGoldenEncodingUnchanged pins the bytes of the two embedding
// types, so the byte-identical claim covers Membership as it actually ships:
// nested inside a Snapshot (persisted metadata) and a SnapshotChunk (wire).
func TestSnapshotGoldenEncodingUnchanged(t *testing.T) {
	memb := Membership{ConfigChangeID: 7, Addresses: map[uint64]string{1: "a:1"}}

	s := Snapshot{ShardID: 1, ReplicaID: 2, Index: 3, Term: 4, Membership: memb}
	sbuf := make([]byte, s.Size())
	sn, err := s.MarshalTo(sbuf)
	if err != nil {
		t.Fatalf("Snapshot.MarshalTo: %v", err)
	}
	const snapGolden = "0100000000000000" + "0200000000000000" +
		"0300000000000000" + "0400000000000000" + // ShardID, ReplicaID, Index, Term
		"01" + "0700000000000000" + "0000000000000000" + "0000000000000000" + // membership fixed head
		"0000000000000000" + "0000000000000000" + "0000000000000000" +
		"0000000000000000" + // membership fixed (session policy, four uint64)
		"01000000" + "0100000000000000" + "03000000" + "613a31" + // Addresses
		"00000000" + "00000000" + "00000000" + // Observers, Witnesses, Removed
		"00000000" + "00000000" + // Identities, RemovedIdentities
		"00000000" + // Filepath length
		"0000000000000000" + // FileSize
		// Three fields left this layout, shortening the vector by 8 bytes each:
		// Type (no writer anywhere and no reader), Witness (written by raft but
		// never read, and the fact it stated is already carried by the empty
		// Filepath the same branch produces), and Imported (whose only writer
		// set it from "this save is an EXPORT" — the opposite of its documented
		// meaning). Acknowledging the change HERE, deliberately, is exactly what
		// this golden test exists for.
		"0000000000000000" + // OnDiskIndex
		"0000000000000000" + "0000000000000000" // Epoch, Compression
	if got := hex.EncodeToString(sbuf[:sn]); got != snapGolden {
		t.Fatalf("Snapshot encoding changed:\n got %s\nwant %s", got, snapGolden)
	}

	sc := SnapshotChunk{ShardID: 1, BinVer: WireVersion, Membership: memb}
	cbuf := make([]byte, sc.Size())
	cn, err := sc.MarshalTo(cbuf)
	if err != nil {
		t.Fatalf("SnapshotChunk.MarshalTo: %v", err)
	}
	// The membership section sits between HasFileInfo and OnDiskIndex; the
	// version byte must be at that exact offset and unchanged.
	membOffset := 72 + 4 + 8 + 8 + 8
	membBuf := make([]byte, memb.Size())
	if _, err := memb.MarshalTo(membBuf); err != nil {
		t.Fatalf("Membership.MarshalTo: %v", err)
	}
	if got := cbuf[membOffset : membOffset+len(membBuf)]; !bytes.Equal(got, membBuf) {
		t.Fatalf("chunk membership section = %x, want %x", got, membBuf)
	}
	var sc2 SnapshotChunk
	if _, err := sc2.UnmarshalFrom(cbuf[:cn]); err != nil {
		t.Fatalf("SnapshotChunk.UnmarshalFrom: %v", err)
	}
	if sc2.Membership.ConfigChangeID != 7 || sc2.Membership.Addresses[1] != "a:1" {
		t.Fatalf("chunk membership round trip: %+v", sc2.Membership)
	}
}

// TestMessageBatchRejectsWrongWireVersion verifies that a batch stamped with
// anything other than WireVersion is rejected with a clear
// ErrUnsupportedVersion instead of being misparsed. Zero is covered
// specifically: an unstamped batch is all-zero at that offset, and admitting it
// would admit every uninitialized or truncated buffer.
func TestMessageBatchRejectsWrongWireVersion(t *testing.T) {
	mb := MessageBatch{BinVer: WireVersion, SourceAddress: "n1:9000"}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	for _, binVer := range []uint64{0, 2, 3, 4, 5, 255, WireVersion + 100} {
		probe := append([]byte{}, buf...)
		putUint64(probe[0:], binVer)
		var mb2 MessageBatch
		if _, err := mb2.UnmarshalFrom(probe); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("UnmarshalFrom(BinVer=%d): got %v, want ErrUnsupportedVersion",
				binVer, err)
		}
	}

	var mb3 MessageBatch
	if _, err := mb3.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom(BinVer=%d): unexpected error %v", WireVersion, err)
	}
}

// TestSnapshotChunkValidatesBinVer verifies that SnapshotChunk gates BinVer
// on exact equality with WireVersion, mirroring MessageBatch. The gate was
// one-sided (BinVer <= WireVersion), which admitted chunks whose embedded
// Membership uses a different layout, and admitted unstamped chunks that are
// indistinguishable from them.
func TestSnapshotChunkValidatesBinVer(t *testing.T) {
	newChunk := func(binVer uint64) []byte {
		sc := SnapshotChunk{
			ShardID: 1, ReplicaID: 2, Index: 3, Term: 4,
			BinVer:    binVer,
			ChunkSize: uint64(len("payload")),
			Data:      []byte("payload"),
		}
		buf := make([]byte, sc.Size())
		if _, err := sc.MarshalTo(buf); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		return buf
	}

	var sc SnapshotChunk
	for _, binVer := range []uint64{0, 2, 3, 4, 5, 255, WireVersion + 1} {
		if _, err := sc.UnmarshalFrom(newChunk(binVer)); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("UnmarshalFrom(BinVer=%d): got %v, want ErrUnsupportedVersion", binVer, err)
		}
	}
	if _, err := sc.UnmarshalFrom(newChunk(WireVersion)); err != nil {
		t.Fatalf("UnmarshalFrom(BinVer=%d): unexpected error %v", WireVersion, err)
	}
}

// TestConfigChangeRejectsEveryStrictPrefix pins the collapse of ConfigChange to
// ONE fixed layout, and with it the closure of a real defect.
//
// The type used to be LENGTH-discriminated: two of its trailing fields could be
// absent, and the decoder read "the buffer ended" as a shorter layout rather
// than as damage. The dangerous half was the trailing NodeIdentity. A payload
// ending where its length prefix begins decoded with err == nil and an EMPTY
// identity — and an empty identity is exactly the compatibility state that
// falls back to the weaker address-based authorization. So a truncated
// ConfigChange was indistinguishable from a legitimate identity-less one, and
// truncation silently downgraded an authorization check.
//
// ConfigChange carries no version byte and still needs none. Under the pinning
// policy there is one layout and every field in it is mandatory, so there is
// nothing to discriminate: short is short. An identity-less change is spelled
// with an explicit zero length prefix, which is what
// TestConfigChangeWithNoIdentityRoundTrips holds down.
func TestConfigChangeRejectsEveryStrictPrefix(t *testing.T) {
	full := ConfigChange{
		ConfigChangeID:             7,
		Type:                       AddNode,
		ReplicaID:                  42,
		Address:                    "10.0.0.1:9000",
		Initialize:                 true,
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
		NodeIdentity:               "sha256:9f86d081884c7d659a2feaa0c55ad015",
	}
	buf := make([]byte, full.Size())
	n, err := full.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("MarshalTo wrote %d bytes into a %d-byte buffer", n, len(buf))
	}

	// EVERY strict prefix is refused. The sweep is exhaustive rather than
	// sampled because the defect lived at ONE offset — the byte before the
	// identity's length prefix — and a hand-picked set of cut points is exactly
	// what missed it. Two offsets used to decode without error or with the
	// wrong error, and both are inside this loop.
	for cut := 0; cut < len(buf); cut++ {
		var decoded ConfigChange
		consumed, err := decoded.UnmarshalFrom(buf[:cut])
		if err == nil {
			t.Fatalf("a %d-byte prefix of a %d-byte ConfigChange decoded cleanly as "+
				"%+v; a truncated payload must never be readable as a whole one",
				cut, len(buf), decoded)
		}
		if !errors.Is(err, ErrBufferTooSmall) {
			t.Fatalf("UnmarshalFrom(prefix of %d bytes): got %v, want ErrBufferTooSmall",
				cut, err)
		}
		if consumed != 0 {
			t.Fatalf("UnmarshalFrom(prefix of %d bytes) reported %d bytes consumed on "+
				"an error return, want 0", cut, consumed)
		}
	}

	// The cut that mattered, called out on its own so a regression names
	// itself. A payload ending exactly where the identity's length prefix
	// begins used to decode with err == nil and an EMPTY NodeIdentity — and an
	// empty identity is the state that falls back to the weaker address-based
	// authorization, so a truncated ConfigChange silently downgraded the gate
	// the identity exists to raise.
	beforeIdentityPrefix := len(buf) - (4 + len(full.NodeIdentity))
	var downgraded ConfigChange
	if _, err := downgraded.UnmarshalFrom(buf[:beforeIdentityPrefix]); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("a ConfigChange truncated to %d of %d bytes returned %v; it must be "+
			"ErrBufferTooSmall, not a clean decode with an empty identity",
			beforeIdentityPrefix, len(buf), err)
	}

	// The shape that used to be reported as a pre-witness-policy LAYOUT — a
	// payload ending right after Initialize — is now just another truncation.
	// There is one layout, so there is no other layout to name.
	endOfInitialize := 24 + 4 + len(full.Address) + 8
	var legacyShape ConfigChange
	_, legacyErr := legacyShape.UnmarshalFrom(buf[:endOfInitialize])
	if !errors.Is(legacyErr, ErrBufferTooSmall) {
		t.Fatalf("a payload ending after Initialize returned %v, want ErrBufferTooSmall", legacyErr)
	}
	if errors.Is(legacyErr, ErrUnsupportedVersion) {
		t.Fatal("a short ConfigChange is still reported as an unsupported VERSION; " +
			"the type carries no version byte and has exactly one layout, so a short " +
			"payload is a truncation and nothing else")
	}

	// Positive control: the full encoding round-trips unchanged.
	var decoded ConfigChange
	consumed, err := decoded.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom(full encoding): %v", err)
	}
	if consumed != len(buf) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(buf))
	}
	if decoded != full {
		t.Fatalf("round trip = %+v, want %+v", decoded, full)
	}
}

// TestConfigChangeWithNoIdentityRoundTrips is the positive control for the
// truncation sweep: a change that genuinely carries no identity is a legal,
// commonly committed payload and must still decode.
//
// It is what stops the fix from being "reject anything without an identity".
// A shard added without RequestAddNodeWithIdentity commits exactly this, and
// every RemoveNode and UpdateWitnessPolicy does too, so refusing it would leave
// nodes unable to replay their own logs. The distinction the layout now draws
// is between an identity that is EMPTY (a four-byte zero length prefix, chosen
// by the writer) and one that is ABSENT (the buffer stopped), and only the
// second is an error.
func TestConfigChangeWithNoIdentityRoundTrips(t *testing.T) {
	for _, cc := range []ConfigChange{
		{ConfigChangeID: 7, Type: AddNode, ReplicaID: 42, Address: "10.0.0.1:9000", Initialize: true},
		{ConfigChangeID: 8, Type: RemoveNode, ReplicaID: 42},
		{ConfigChangeID: 9, Type: UpdateWitnessPolicy, MaxWitnesses: 3, RequireFullReplicaMajority: true},
	} {
		t.Run(ConfigChangeTypeName(cc.Type), func(t *testing.T) {
			if cc.NodeIdentity != "" {
				t.Fatal("fixture: this case must carry no identity")
			}
			buf := make([]byte, cc.Size())
			n, err := cc.MarshalTo(buf)
			if err != nil {
				t.Fatalf("MarshalTo: %v", err)
			}

			// The encoder spells the absent identity explicitly: four zero
			// bytes of length prefix, not a shorter buffer. That is what keeps
			// it distinguishable from truncation.
			if n != len(buf) {
				t.Fatalf("MarshalTo wrote %d bytes, want %d", n, len(buf))
			}
			if got := getUint32(buf[n-4:]); got != 0 {
				t.Fatalf("trailing identity length prefix = %d, want 0", got)
			}

			var decoded ConfigChange
			consumed, err := decoded.UnmarshalFrom(buf)
			if err != nil {
				t.Fatalf("UnmarshalFrom: %v", err)
			}
			if consumed != len(buf) {
				t.Fatalf("consumed %d bytes, want %d", consumed, len(buf))
			}
			if decoded != cc {
				t.Fatalf("round trip = %+v, want %+v", decoded, cc)
			}
		})
	}
}

// marshalUnversionedMembershipShort encodes a membership section with NO
// version byte at all — [ConfigChangeID:8][Addresses][Observers][Witnesses]
// [Removed] — the shape this format had before it grew a leading
// discriminator.
func marshalUnversionedMembershipShort(ccID uint64, addrs map[uint64]string) []byte {
	buf := make([]byte, 8+mapSize(addrs)+3*4)
	putUint64(buf[0:], ccID)
	offset := 8
	offset += putMap(buf[offset:], addrs)
	offset += putMap(buf[offset:], nil)
	offset += putMap(buf[offset:], nil)
	putBoolMap(buf[offset:], nil)
	return buf
}

// marshalUnversionedMembershipLong encodes the same shape with MaxWitnesses
// and RequireFullReplicaMajority ahead of the maps, and still no version byte.
func marshalUnversionedMembershipLong(ccID, maxWit uint64, policy bool, addrs map[uint64]string) []byte {
	buf := make([]byte, 24+mapSize(addrs)+3*4)
	putUint64(buf[0:], ccID)
	putUint64(buf[8:], maxWit)
	putBool(buf[16:], policy)
	offset := 24
	offset += putMap(buf[offset:], addrs)
	offset += putMap(buf[offset:], nil)
	offset += putMap(buf[offset:], nil)
	putBoolMap(buf[offset:], nil)
	return buf
}

// TestMembershipRejectsUnversionedLayout verifies that a membership section
// carrying no version byte is rejected with ErrUnsupportedVersion.
//
// The EMPTY cases are the point of the version byte. Before it existed, the
// decoder discriminated layouts with a heuristic on bytes 16..24 ("must be a
// bare 0 or 1"), which is undecidable for an empty unversioned section: it
// encodes those bytes as [WitnessCount:4=0][RemovedCount:4=0], byte-identical
// to a section with RequireFullReplicaMajority=false. Such a buffer used to
// parse on and surface as ErrBufferTooSmall / ErrTooManyMembershipEntries — or,
// embedded in a larger buffer, misparse into a plausible membership.
//
// The POPULATED cases use a ConfigChangeID whose low byte is NOT a valid
// version, and that choice is part of the statement rather than a convenience.
// An unversioned section's first byte is the ConfigChangeID's low byte, so a
// section whose ConfigChangeID happened to be congruent to the supported
// version mod 256 would be byte-indistinguishable from a versioned one and
// could not be rejected by any decoder. That is the undecidability the version
// byte exists to end, and it is why the unversioned layouts are unreadable BY
// CONSTRUCTION rather than merely unsupported.
//
// With the version pinned at 1 the colliding set is a single value and no data
// in that layout exists anywhere — the fixture is built by hand here. What it
// pins is what the version byte CAN do (reject a section whose leading byte is
// not the supported one, before a single field is interpreted), not a guarantee
// it cannot make.
func TestMembershipRejectsUnversionedLayout(t *testing.T) {
	addrs := map[uint64]string{1: "10.0.0.1:9000"}
	// 0x140 = 320: low byte 0x40, not the supported version and not a plausible
	// future one either.
	const unversionedCCID uint64 = 0x140
	cases := []struct {
		name string
		buf  []byte
	}{
		{"short populated", marshalUnversionedMembershipShort(unversionedCCID, addrs)},
		{"short empty", marshalUnversionedMembershipShort(0, nil)},
		{"long populated", marshalUnversionedMembershipLong(unversionedCCID, 2, true, addrs)},
		{"long empty", marshalUnversionedMembershipLong(0, 0, false, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m Membership
			if _, err := m.UnmarshalFrom(tc.buf); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("UnmarshalFrom: got %v, want ErrUnsupportedVersion", err)
			}
		})
	}
}

// TestMembershipEmptyUnversionedEmbeddedRejected covers the residual exposure
// the old heuristic could not reach: an empty unversioned membership embedded
// in a larger buffer, where no length or trailing-byte check applies. Both
// shapes are padded with enough trailing bytes to satisfy every downstream
// bounds check, so only the version byte can reject them.
func TestMembershipEmptyUnversionedEmbeddedRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section []byte
	}{
		{"short", marshalUnversionedMembershipShort(0, nil)},
		{"long", marshalUnversionedMembershipLong(0, 0, false, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			embedded := append(append([]byte{}, tc.section...), make([]byte, 256)...)
			var m Membership
			if _, err := m.UnmarshalFrom(embedded); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("UnmarshalFrom(embedded %s): got %v, want ErrUnsupportedVersion", tc.name, err)
			}
		})
	}
}

// TestMembershipVersionByteOnWire pins the version byte's position and value
// for every shape the encoder can produce, and asserts that no other value is
// admitted at that position.
func TestMembershipVersionByteOnWire(t *testing.T) {
	shapes := []Membership{
		{},
		{ConfigChangeID: 7},
		{ConfigChangeID: 7, MaxWitnesses: 3, RequireFullReplicaMajority: true},
		{
			ConfigChangeID:             9,
			MaxWitnesses:               1,
			RequireFullReplicaMajority: false,
			Addresses:                  map[uint64]string{1: "a:1", 2: "b:2"},
			Observers:                  map[uint64]string{3: "c:3"},
			Witnesses:                  map[uint64]string{4: "d:4"},
			Removed:                    map[uint64]bool{5: true},
			Identities:                 map[uint64]string{1: "spki:aa"},
			RemovedIdentities:          map[uint64]string{5: "spki:bb"},
		},
	}
	for i := range shapes {
		m := shapes[i]
		buf := make([]byte, m.Size())
		n, err := m.MarshalTo(buf)
		if err != nil {
			t.Fatalf("shape %d MarshalTo: %v", i, err)
		}
		if n != m.Size() {
			t.Fatalf("shape %d: MarshalTo wrote %d, Size() = %d", i, n, m.Size())
		}
		if buf[0] != membershipFormatVersion {
			t.Fatalf("shape %d: wire byte 0 = %d, want %d", i, buf[0], membershipFormatVersion)
		}
		var got Membership
		n2, err := got.UnmarshalFrom(buf)
		if err != nil {
			t.Fatalf("shape %d UnmarshalFrom: %v", i, err)
		}
		if n2 != n {
			t.Fatalf("shape %d: consumed %d, wrote %d", i, n2, n)
		}
		assertMembershipEqual(t, &m, &got)

		// Any other version byte must be rejected before a field is read. The
		// values below include every number an earlier pre-release commit
		// stamped, plus the two ends of the byte range.
		for _, bad := range []byte{0, 2, 3, 4, 5, 6, 0xFF} {
			corrupt := append([]byte{}, buf...)
			corrupt[0] = bad
			var m2 Membership
			if _, err := m2.UnmarshalFrom(corrupt); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("shape %d version %d: got %v, want ErrUnsupportedVersion", i, bad, err)
			}
		}
	}
}

// TestMembershipRejectsTrailingBytesAtCallSite pins the property that makes
// exact-version gating safe for an EMBEDDED section: UnmarshalFrom reports how
// many bytes it consumed, and the caller — not the decoder — decides whether
// anything is left over. Membership itself must therefore consume exactly its
// own section and no more, whatever follows it.
func TestMembershipRejectsTrailingBytesAtCallSite(t *testing.T) {
	m := goldenMembershipFixture()
	m.SetSessionPolicy(goldenMembershipSessionPolicy())
	m.Identities = map[uint64]string{1: "spki:aa"}
	m.RemovedIdentities = map[uint64]string{5: "spki:bb"}

	exact := make([]byte, m.Size())
	n, err := m.MarshalTo(exact)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	padded := append(append([]byte{}, exact[:n]...), make([]byte, 64)...)
	var got Membership
	consumed, err := got.UnmarshalFrom(padded)
	if err != nil {
		t.Fatalf("UnmarshalFrom(padded): %v", err)
	}
	if consumed != n {
		t.Fatalf("consumed %d bytes of a padded buffer, want %d: an embedded "+
			"section must not run into whatever follows it", consumed, n)
	}
	assertMembershipEqual(t, &m, &got)
}

// TestOverLimitSentinelsPerField verifies that each length-limited field
// reports its own dedicated sentinel (previously getBytes always returned
// ErrEntrySizeLimitExceeded and getString always ErrAddressTooLong, leaving
// ErrMetadataTooLong/ErrChunkDataTooLong/ErrFilepathTooLong
// unreachable).
func TestOverLimitSentinelsPerField(t *testing.T) {
	// SnapshotFile.Filepath over MaxFilepathLen.
	buf := make([]byte, 8)
	putUint32(buf, uint32(MaxFilepathLen+1))
	var sf SnapshotFile
	if _, err := sf.UnmarshalFrom(buf); !errors.Is(err, ErrFilepathTooLong) {
		t.Fatalf("SnapshotFile filepath: got %v, want ErrFilepathTooLong", err)
	}

	// SnapshotFile.Metadata over MaxMetadataLen.
	sfBuf := make([]byte, 4+16+4)
	putString(sfBuf, "")                      // empty filepath
	putUint32(sfBuf[4+16:], MaxMetadataLen+1) // oversized metadata prefix
	if _, err := sf.UnmarshalFrom(sfBuf); !errors.Is(err, ErrMetadataTooLong) {
		t.Fatalf("SnapshotFile metadata: got %v, want ErrMetadataTooLong", err)
	}

	// Snapshot.Filepath over MaxFilepathLen. This replaces the Snapshot.Checksum
	// case: Checksum was removed from the wire format (never computed, never
	// verified — see the Snapshot type doc), so Filepath is now the only
	// length-limited variable field Snapshot owns directly, and it is what pins
	// that Snapshot still reports its own sentinel rather than whatever the
	// shared getString helper last defaulted to.
	var emptyMembership Membership
	msize := emptyMembership.Size()
	snapBuf := make([]byte, 32+msize+4)
	if _, err := emptyMembership.MarshalTo(snapBuf[32:]); err != nil {
		t.Fatalf("Membership.MarshalTo: %v", err)
	}
	putUint32(snapBuf[32+msize:], uint32(MaxFilepathLen+1))
	var s Snapshot
	if _, err := s.UnmarshalFrom(snapBuf); !errors.Is(err, ErrFilepathTooLong) {
		t.Fatalf("Snapshot filepath: got %v, want ErrFilepathTooLong", err)
	}

	// SnapshotChunk.Data over MaxSnapshotChunkDataLen.
	sc := SnapshotChunk{BinVer: WireVersion}
	chunkBuf := make([]byte, sc.Size())
	n, err := sc.MarshalTo(chunkBuf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	putUint32(chunkBuf[n-4:], MaxSnapshotChunkDataLen+1) // oversized data prefix
	var sc2 SnapshotChunk
	if _, err := sc2.UnmarshalFrom(chunkBuf); !errors.Is(err, ErrChunkDataTooLong) {
		t.Fatalf("SnapshotChunk data: got %v, want ErrChunkDataTooLong", err)
	}
}
