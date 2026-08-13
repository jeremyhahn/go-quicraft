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

// Regression tests for the WireVersion 3 bump (Membership version byte, on
// top of the v2 witness-policy fields) and the per-field over-limit
// sentinels.

// TestWireVersionIsThree pins the current TRANSPORT wire version. The v1 and
// v2 layouts have no legacy decode path, so any change here must ship with
// matching encoder/decoder changes across all consumers.
//
// It deliberately does NOT tie WireVersion to membershipFormatVersion: the
// two namespaces are independent (see the package comment), and asserting
// equality would re-impose the coupling that made a transport-only bump
// unreplayable at log index 1.
func TestWireVersionIsThree(t *testing.T) {
	if WireVersion != 3 {
		t.Fatalf("WireVersion = %d, want 3", WireVersion)
	}
}

// TestPersistedMembershipVersionPinned pins the persisted Membership format
// window. These values describe BYTES ALREADY ON DISK in every deployed
// cluster — the WAL, snapshot records, exported artifacts, and the
// EntryInitialMembership entry at log index 1.
//
// membershipFormatVersion may only be raised together with a new decode case
// in Membership.UnmarshalFrom, and minSupportedMembershipVersion may only be
// raised after the migration window that retired the old layout. Neither may
// track WireVersion. If this test fails, the change under review is a
// persisted format change and needs a migration story, not a version bump.
func TestPersistedMembershipVersionPinned(t *testing.T) {
	if membershipFormatVersion != 4 {
		t.Fatalf("membershipFormatVersion = %d, want 4", membershipFormatVersion)
	}
	if minSupportedMembershipVersion != 3 {
		t.Fatalf("minSupportedMembershipVersion = %d, want 3", minSupportedMembershipVersion)
	}
	if minSupportedMembershipVersion > membershipFormatVersion {
		t.Fatalf("empty supported window: min %d > current %d",
			minSupportedMembershipVersion, membershipFormatVersion)
	}
}

// TestMembershipVersionNotDerivedFromWireVersion is a source-level guard: it
// parses membership.go and asserts that neither version constant is defined
// in terms of WireVersion.
//
// A value check cannot catch this — both namespaces are 3 today, so a
// contributor who "simplifies" the constant back to uint8(WireVersion) leaves
// every runtime assertion green while re-arming the original defect: the next
// transport-only WireVersion bump would silently change the version byte
// expected at log index 1 and leave the cluster unable to replay its own log.
func TestMembershipVersionNotDerivedFromWireVersion(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "membership.go", nil, 0)
	if err != nil {
		t.Fatalf("parse membership.go: %v", err)
	}
	// membershipV3 is guarded too: the constants above are defined in terms
	// of it, so re-deriving IT from WireVersion would reintroduce the
	// coupling one level down.
	guarded := map[string]bool{
		"membershipV3":                  false,
		"membershipFormatVersion":       false,
		"minSupportedMembershipVersion": false,
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

// TestMembershipVersionWindow exercises the accept-a-range decode gate: the
// current version decodes, anything below the floor or above the current
// version is refused with the typed error, and the message names both the
// version found and the supported range.
func TestMembershipVersionWindow(t *testing.T) {
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

	// Every version in the supported window decodes. Degenerate today
	// (3..3); the loop is what a future v4 extends.
	for v := minSupportedMembershipVersion; v <= membershipFormatVersion; v++ {
		probe := append([]byte{}, buf...)
		probe[0] = v
		var got Membership
		if _, err := got.UnmarshalFrom(probe); err != nil {
			t.Fatalf("UnmarshalFrom(version %d): unexpected error %v", v, err)
		}
	}

	for _, tc := range []struct {
		name    string
		version uint8
	}{
		{"below floor", minSupportedMembershipVersion - 1},
		{"zero", 0},
		{"above current", membershipFormatVersion + 1},
		{"max", 0xFF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := append([]byte{}, buf...)
			probe[0] = tc.version
			var got Membership
			_, err := got.UnmarshalFrom(probe)
			if !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("UnmarshalFrom(version %d): got %v, want ErrUnsupportedVersion",
					tc.version, err)
			}
			var verr *UnsupportedMembershipVersionError
			if !errors.As(err, &verr) {
				t.Fatalf("UnmarshalFrom(version %d): got %T, want *UnsupportedMembershipVersionError",
					tc.version, err)
			}
			if verr.Found != tc.version {
				t.Fatalf("Found = %d, want %d", verr.Found, tc.version)
			}
			// The message must let an operator diagnose the mismatch
			// without reading the source: found value and window bounds.
			msg := err.Error()
			for _, want := range []string{
				strconv.Itoa(int(tc.version)),
				strconv.Itoa(int(minSupportedMembershipVersion)),
				strconv.Itoa(int(membershipFormatVersion)),
			} {
				if !strings.Contains(msg, want) {
					t.Fatalf("error %q does not name %q", msg, want)
				}
			}
		})
	}
}

// membershipGoldenFixedV3 is the fixed region v3 and v4 share, and v4 leaves
// byte-identical: ConfigChangeID through RequireFullReplicaMajority. v4
// appends the session policy AFTER it, which is why the shared region and the
// map region are separate constants — they are no longer adjacent.
const membershipGoldenFixedV3 = "0807060504030201" + // ConfigChangeID
	"0200000000000000" + // MaxWitnesses
	"0100000000000000" // RequireFullReplicaMajority

// membershipGoldenSessionPolicyV4 is what v4 inserts between the v3 fixed
// fields and the map region: the replicated session policy the apply path
// reads. The values are goldenMembershipSessionPolicy.
const membershipGoldenSessionPolicyV4 = "a086010000000000" + // SessionExpiryEntries = 100000
	"0040000000000000" + // MaxTotalSessions = 16384
	"8000000000000000" // MaxSessionsPerClient = 128

// membershipGoldenMapsV3V4 is the four-map region v3 and v4 share. It is a
// single constant so the two goldens below cannot drift apart in the region
// they are supposed to agree on.
const membershipGoldenMapsV3V4 = "02000000" + // Addresses count
	"0100000000000000" + "03000000" + "613a31" + // 1 -> "a:1"
	"0200000000000000" + "03000000" + "623a32" + // 2 -> "b:2"
	"01000000" + // Observers count
	"0300000000000000" + "03000000" + "633a33" + // 3 -> "c:3"
	"01000000" + // Witnesses count
	"0400000000000000" + "03000000" + "643a34" + // 4 -> "d:4"
	"01000000" + // Removed count
	"0500000000000000" + "0100000000000000" // 5 -> true

// goldenMembershipSessionPolicy is the policy membershipGoldenSessionPolicyV4
// spells out in bytes. It is v4-only: a v3 section has nowhere to put it.
func goldenMembershipSessionPolicy() SessionPolicy {
	return SessionPolicy{
		ExpiryEntries:        100000,
		MaxTotalSessions:     16384,
		MaxSessionsPerClient: 128,
	}
}

// goldenMembershipFixture is the membership both goldens describe, minus the
// v4-only Identities map and session policy.
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

// TestMembershipGoldenEncodingV4 asserts the exact bytes a current binary
// writes. These bytes go into the WAL, into snapshot records and exported
// artifacts, and into the EntryInitialMembership entry at log index 1, where
// they outlive the process that wrote them — so an accidental change here is a
// persisted format change, not a refactor.
//
// The v4 layout is the v3 fixed fields, then the session policy, then the four
// v3 maps, then the Identities map, with the version byte raised. The golden is
// spelled as those pieces so the claim that v4 preserves the v3 fields and map
// region is checked by construction rather than asserted in a comment.
func TestMembershipGoldenEncodingV4(t *testing.T) {
	m := goldenMembershipFixture()
	m.SetSessionPolicy(goldenMembershipSessionPolicy())
	m.Identities = map[uint64]string{1: "spki:aa"}

	const golden = "04" + // version byte
		membershipGoldenFixedV3 +
		membershipGoldenSessionPolicyV4 +
		membershipGoldenMapsV3V4 +
		"01000000" + // Identities count
		"0100000000000000" + "07000000" + "73706b693a6161" // 1 -> "spki:aa"

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
	if got.MaxWitnesses != m.MaxWitnesses || !got.RequireFullReplicaMajority {
		t.Fatalf("policy fields lost: %+v", got)
	}
	if got.SessionPolicy() != goldenMembershipSessionPolicy() {
		t.Fatalf("session policy lost in round trip: %+v", got.SessionPolicy())
	}
}

// TestMembershipV3GoldenStillDecodes is the compatibility half: the exact
// bytes a PRE-IDENTITY binary wrote must still decode, because they are
// already on disk in every deployed cluster — at log index 1, in every
// snapshot record, and in the WAL membership section. A node that could not
// read them could not replay its own log.
//
// The distinguishing assertions are that Identities comes back EMPTY and the
// session policy comes back UNSET, not that the decode merely succeeded. Empty
// identities are what makes the receive-side gate fall back to the
// committed-address check (or refuse, under RequireMemberIdentity); an unset
// session policy is what makes the apply path fall back to local configuration
// for a shard bootstrapped before the policy was replicated. A decoder that
// left stale values in place would hand a v3 configuration state it never
// contained — and for the session policy that state drives apply, so the
// replicas would diverge over bytes that were never on disk.
//
// The v3 fixed region is 25 bytes and v4's is 49, so this also pins that the
// decoder picks its map offset from the VERSION rather than from a single
// shared constant: reading v3's maps at the v4 offset consumes the first map
// count as policy and fails, or worse, succeeds on a crafted buffer.
func TestMembershipV3GoldenStillDecodes(t *testing.T) {
	const goldenV3 = "03" + membershipGoldenFixedV3 + membershipGoldenMapsV3V4

	raw, err := hex.DecodeString(goldenV3)
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	// Pre-populate Identities and the session policy so a decoder that forgets
	// to clear them is caught rather than passing on a zero value that happened
	// to be zero already.
	got := Membership{Identities: map[uint64]string{9: "spki:stale"}}
	got.SetSessionPolicy(SessionPolicy{
		ExpiryEntries:        777,
		MaxTotalSessions:     999,
		MaxSessionsPerClient: 111,
	})
	consumed, err := got.UnmarshalFrom(raw)
	if err != nil {
		t.Fatalf("UnmarshalFrom(v3 golden): %v", err)
	}
	if consumed != len(raw) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(raw))
	}
	want := goldenMembershipFixture()
	assertMembershipEqual(t, &want, &got)
	if len(got.Identities) != 0 {
		t.Fatalf("v3 decode left identities behind: %+v", got.Identities)
	}
	if policy := got.SessionPolicy(); policy.IsSet() {
		t.Fatalf("v3 decode reported a committed session policy %+v; a v3 "+
			"section carries none, and IsSet is what the apply path uses to "+
			"decide whether to trust it over local configuration", policy)
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
		"04" + "0700000000000000" + "0000000000000000" + "0000000000000000" + // membership fixed (v3 fields)
		"0000000000000000" + "0000000000000000" + "0000000000000000" + // membership fixed (session policy)
		"01000000" + "0100000000000000" + "03000000" + "613a31" + // Addresses
		"00000000" + "00000000" + "00000000" + "00000000" + // Observers, Witnesses, Removed, Identities
		"00000000" + // Filepath length
		"0000000000000000" + // FileSize
		"00000000" + // Checksum length
		"0000000000000000" + "0000000000000000" + // Type, Imported
		"0000000000000000" + "0000000000000000" + // OnDiskIndex, Witness
		"0000000000000000" // Epoch
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

// TestMessageBatchRejectsOldWireVersion verifies that a batch stamped with
// the pre-witness-policy wire version 1 is rejected with a clear
// ErrUnsupportedVersion instead of being misparsed.
func TestMessageBatchRejectsOldWireVersion(t *testing.T) {
	mb := MessageBatch{BinVer: WireVersion, SourceAddress: "n1:9000"}
	buf := make([]byte, mb.Size())
	if _, err := mb.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	putUint64(buf[0:], 1) // legacy wire version

	var mb2 MessageBatch
	if _, err := mb2.UnmarshalFrom(buf); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("UnmarshalFrom(v1 batch): got %v, want ErrUnsupportedVersion", err)
	}
}

// TestSnapshotChunkValidatesBinVer verifies that SnapshotChunk gates BinVer
// on exact equality with WireVersion, mirroring MessageBatch. The gate was
// one-sided (BinVer <= WireVersion), which admitted v1/v2 chunks whose
// embedded Membership uses a different layout, and admitted unstamped
// chunks that are indistinguishable from them.
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
	for _, binVer := range []uint64{0, 1, WireVersion - 1, WireVersion + 1} {
		if _, err := sc.UnmarshalFrom(newChunk(binVer)); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("UnmarshalFrom(BinVer=%d): got %v, want ErrUnsupportedVersion", binVer, err)
		}
	}
	if _, err := sc.UnmarshalFrom(newChunk(WireVersion)); err != nil {
		t.Fatalf("UnmarshalFrom(BinVer=%d): unexpected error %v", WireVersion, err)
	}
}

// TestConfigChangeRejectsLegacyLayout verifies that a v1 ConfigChange
// payload — which ends immediately after Initialize, with no witness-policy
// fields — is detected and rejected with ErrUnsupportedVersion rather than
// an opaque ErrBufferTooSmall.
func TestConfigChangeRejectsLegacyLayout(t *testing.T) {
	addr := "10.0.0.1:9000"
	// v1 layout: [ID:8][Type:8][ReplicaID:8][Address:4+len][Initialize:8]
	legacy := make([]byte, 24+4+len(addr)+8)
	putUint64(legacy[0:], 7)
	putUint64(legacy[8:], AddNode)
	putUint64(legacy[16:], 42)
	offset := 24
	offset += putString(legacy[offset:], addr)
	putBool(legacy[offset:], true)

	var cc ConfigChange
	if _, err := cc.UnmarshalFrom(legacy); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("UnmarshalFrom(v1 layout): got %v, want ErrUnsupportedVersion", err)
	}

	// A genuinely truncated payload (cut INSIDE the policy fields) must still
	// surface ErrBufferTooSmall. The cut is 6 bytes, not 4: the current layout
	// ends with a 4-byte NodeIdentity length prefix, and a payload that ends
	// exactly where that prefix begins is the legitimate pre-identity layout,
	// which decodes with an empty identity rather than erroring.
	cc2 := ConfigChange{ConfigChangeID: 7, Type: AddNode, ReplicaID: 42, Address: addr}
	buf := make([]byte, cc2.Size())
	if _, err := cc2.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var cc3 ConfigChange
	if _, err := cc3.UnmarshalFrom(buf[:len(buf)-6]); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom(truncated policy fields): got %v, want ErrBufferTooSmall", err)
	}

	// The pre-identity layout — everything up to and including the policy
	// fields, with no trailing identity — must decode cleanly with an empty
	// NodeIdentity. This is the payload every already-committed
	// EntryConfigChange in a deployed cluster contains, so failing it would
	// make an upgraded node unable to replay its own log.
	var cc4 ConfigChange
	n, err := cc4.UnmarshalFrom(buf[:len(buf)-4])
	if err != nil {
		t.Fatalf("UnmarshalFrom(pre-identity layout): %v", err)
	}
	if n != len(buf)-4 {
		t.Fatalf("consumed %d bytes, want %d", n, len(buf)-4)
	}
	if cc4.NodeIdentity != "" {
		t.Fatalf("NodeIdentity = %q, want empty for a pre-identity payload", cc4.NodeIdentity)
	}
	if cc4.Address != addr || cc4.ReplicaID != 42 {
		t.Fatalf("pre-identity payload decoded wrong: %+v", cc4)
	}
}

// marshalLegacyMembershipV1 encodes the v1 Membership layout:
// [ConfigChangeID:8][Addresses][Observers][Witnesses][Removed].
func marshalLegacyMembershipV1(ccID uint64, addrs map[uint64]string) []byte {
	buf := make([]byte, 8+mapSize(addrs)+3*4)
	putUint64(buf[0:], ccID)
	offset := 8
	offset += putMap(buf[offset:], addrs)
	offset += putMap(buf[offset:], nil)
	offset += putMap(buf[offset:], nil)
	putBoolMap(buf[offset:], nil)
	return buf
}

// marshalLegacyMembershipV2 encodes the v2 Membership layout: the v1 fields
// with MaxWitnesses and RequireFullReplicaMajority appended, and no version
// byte.
func marshalLegacyMembershipV2(ccID, maxWit uint64, policy bool, addrs map[uint64]string) []byte {
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

// TestMembershipRejectsLegacyLayout verifies that pre-v3 Membership
// payloads are rejected with ErrUnsupportedVersion by the leading version
// byte.
//
// The EMPTY cases are the point of the version byte. The previous decoder
// discriminated layouts with a heuristic on bytes 16..24 ("must be a bare
// 0 or 1"), which is undecidable for an empty legacy membership: it encodes
// those bytes as [WitnessCount:4=0][RemovedCount:4=0], byte-identical to a
// membership with RequireFullReplicaMajority=false. Such a buffer used to
// parse on and surface as ErrBufferTooSmall / ErrTooManyMembershipEntries —
// or, embedded in a larger buffer, misparse into a plausible membership.
func TestMembershipRejectsLegacyLayout(t *testing.T) {
	addrs := map[uint64]string{1: "10.0.0.1:9000"}
	cases := []struct {
		name string
		buf  []byte
	}{
		{"v1 populated", marshalLegacyMembershipV1(5, addrs)},
		{"v1 empty", marshalLegacyMembershipV1(0, nil)},
		{"v2 populated", marshalLegacyMembershipV2(5, 2, true, addrs)},
		{"v2 empty", marshalLegacyMembershipV2(0, 0, false, nil)},
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

// TestMembershipEmptyLegacyEmbeddedRejected covers the residual exposure the
// heuristic could not reach: an empty legacy membership embedded in a larger
// buffer, where no length or trailing-byte check applies. Both a v1 and a v2
// section are padded with enough trailing bytes to satisfy every downstream
// bounds check, so only the version byte can reject them.
func TestMembershipEmptyLegacyEmbeddedRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section []byte
	}{
		{"v1", marshalLegacyMembershipV1(0, nil)},
		{"v2", marshalLegacyMembershipV2(0, 0, false, nil)},
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

// TestMembershipVersionByteOnWire pins the version byte's position and
// value, and asserts every legacy layout it must not collide with is
// rejected for every shape the encoder can produce.
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

		// Any other version byte must be rejected before a field is read.
		for _, bad := range []byte{0, 1, 2, membershipFormatVersion + 1, 0xFF} {
			if bad == membershipFormatVersion {
				continue
			}
			corrupt := append([]byte{}, buf...)
			corrupt[0] = bad
			var m2 Membership
			if _, err := m2.UnmarshalFrom(corrupt); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("shape %d version %d: got %v, want ErrUnsupportedVersion", i, bad, err)
			}
		}
	}
}

// TestOverLimitSentinelsPerField verifies that each length-limited field
// reports its own dedicated sentinel (previously getBytes always returned
// ErrEntrySizeLimitExceeded and getString always ErrAddressTooLong, leaving
// ErrChecksumTooLong/ErrMetadataTooLong/ErrChunkDataTooLong/ErrFilepathTooLong
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

	// Snapshot.Checksum over MaxChecksumLen.
	var emptyMembership Membership
	msize := emptyMembership.Size()
	snapBuf := make([]byte, 32+msize+4+8+4)
	emptyMembership.MarshalTo(snapBuf[32:])
	offset := 32 + msize
	offset += putString(snapBuf[offset:], "") // empty filepath
	offset += 8                               // FileSize
	putUint32(snapBuf[offset:], MaxChecksumLen+1)
	var s Snapshot
	if _, err := s.UnmarshalFrom(snapBuf); !errors.Is(err, ErrChecksumTooLong) {
		t.Fatalf("Snapshot checksum: got %v, want ErrChecksumTooLong", err)
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
