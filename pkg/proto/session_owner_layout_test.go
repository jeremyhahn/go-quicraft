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

import "testing"

// TestSessionPolicyLayoutRoundTrip checks MaxSessionsPerOwner round-trips and the size constants match the encoded bytes.
func TestSessionPolicyLayoutRoundTrip(t *testing.T) {
	want := Membership{
		ConfigChangeID:             7,
		MaxWitnesses:               2,
		RequireFullReplicaMajority: true,
		WireVersion:                WireVersion,
		Addresses:                  map[uint64]string{1: "10.0.0.1:5001"},
		Observers:                  map[uint64]string{2: "10.0.0.2:5001"},
		Witnesses:                  map[uint64]string{3: "10.0.0.3:5001"},
		Removed:                    map[uint64]bool{4: true},
		Identities:                 map[uint64]string{1: "spki:aa"},
		RemovedIdentities:          map[uint64]string{4: "spki:dd"},
	}
	// Every value distinct, so a transposition between two policy fields is a
	// failure rather than a coincidence. Transposing MaxTotalSessions with
	// MaxResponsesPerSession has shipped here before.
	want.SetSessionPolicy(SessionPolicy{
		ExpiryEntries:          11111,
		MaxTotalSessions:       22222,
		MaxResponsesPerSession: 33333,
		MaxSessionsPerOwner:    44444,
	})

	buf := make([]byte, want.Size())
	n, err := want.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != want.Size() {
		t.Fatalf("MarshalTo wrote %d bytes but Size() says %d. The two must "+
			"agree exactly: Size() is what every caller allocates and what the "+
			"WAL/snapshot encoders reserve, so a disagreement either truncates "+
			"the section or leaves uninitialised slack the decoder reads as "+
			"field data.", n, want.Size())
	}

	var got Membership
	consumed, err := got.UnmarshalFrom(buf[:n])
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom consumed %d of %d bytes written", consumed, n)
	}

	gp, wp := got.SessionPolicy(), want.SessionPolicy()
	if gp != wp {
		t.Fatalf("session policy round-trip = %+v, want %+v", gp, wp)
	}
	// Named explicitly in case both sides lose the field outside the codec.
	if got.MaxSessionsPerOwner != 44444 {
		t.Fatalf("MaxSessionsPerOwner = %d, want 44444: the field did not "+
			"survive marshal/unmarshal, so a shard's per-owner cap would be "+
			"lost on every path that persists or ships a Membership",
			got.MaxSessionsPerOwner)
	}

	if got.WireVersion != WireVersion {
		t.Fatalf("WireVersion = %d, want %d: the committed wire-version switch "+
			"did not survive marshal/unmarshal", got.WireVersion, WireVersion)
	}

	// The fixed region must be exactly where the constants say, and the maps
	// must start immediately after it. Recomputing the expected size from the
	// field widths rather than reusing membershipFixedSize is deliberate: a
	// test that asserts a constant against itself cannot fail.
	const wantFixed = 1 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 // 65
	if membershipFixedSize != wantFixed {
		t.Fatalf("membershipFixedSize = %d, want %d (Version(1) + "+
			"ConfigChangeID(8) + MaxWitnesses(8) + RequireFullReplicaMajority(8) "+
			"+ the four 8-byte session-policy fields + WireVersion(8))",
			membershipFixedSize, wantFixed)
	}
	if membershipSessionPolicySize != 4*8 {
		t.Fatalf("membershipSessionPolicySize = %d, want %d: the policy is four "+
			"uint64 fields", membershipSessionPolicySize, 4*8)
	}

	// An empty membership is exactly the fixed region plus six empty map count
	// headers, which is the number waldb's minMembershipSize mirrors.
	empty := Membership{}
	if got, want := empty.Size(), wantFixed+6*4; got != want {
		t.Fatalf("empty Membership Size() = %d, want %d", got, want)
	}
}

// TestEntryOwnerIDRoundTrip checks Entry.OwnerID round-trips and entryHeaderSize matches the field widths.
func TestEntryOwnerIDRoundTrip(t *testing.T) {
	const wantHeader = 8*8 + 4 // 68
	if entryHeaderSize != wantHeader {
		t.Fatalf("entryHeaderSize = %d, want %d (Term, Index, Type, Key, "+
			"ClientID, SeriesID, RespondedTo, OwnerID as uint64 + CmdLen as "+
			"uint32)", entryHeaderSize, wantHeader)
	}

	want := Entry{
		Term:        1,
		Index:       2,
		Type:        EntryNormal,
		Key:         3,
		ClientID:    4,
		SeriesID:    5,
		RespondedTo: 6,
		OwnerID:     0xDEADBEEFCAFE,
		Cmd:         []byte("payload"),
	}

	buf := make([]byte, want.Size())
	if want.Size() != wantHeader+len(want.Cmd) {
		t.Fatalf("Size() = %d, want %d", want.Size(), wantHeader+len(want.Cmd))
	}
	n, err := want.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	var got Entry
	consumed, err := got.UnmarshalFrom(buf[:n])
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom consumed %d of %d", consumed, n)
	}
	if got.OwnerID != want.OwnerID {
		t.Fatalf("OwnerID = %#x, want %#x: the owner did not survive the entry "+
			"round-trip, so the per-owner session cap would see every "+
			"registration as ownerless", got.OwnerID, want.OwnerID)
	}
	// The neighbours on both sides of the new field, so an off-by-eight in
	// either direction fails here rather than somewhere unrelated.
	if got.RespondedTo != want.RespondedTo {
		t.Fatalf("RespondedTo = %d, want %d (field before OwnerID)",
			got.RespondedTo, want.RespondedTo)
	}
	if string(got.Cmd) != string(want.Cmd) {
		t.Fatalf("Cmd = %q, want %q (payload follows CmdLen, which follows OwnerID)",
			got.Cmd, want.Cmd)
	}

	// Reset must clear it too, or a pooled entry carries the previous
	// proposal's owner into the next registration.
	got.Reset()
	if got.OwnerID != 0 {
		t.Fatalf("OwnerID = %d after Reset, want 0: entries are pooled, so a "+
			"stale owner would be attributed to whoever reuses the entry",
			got.OwnerID)
	}
}
