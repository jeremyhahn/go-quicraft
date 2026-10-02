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
	"errors"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Every limit must be enforced on WRITE as well as on READ
//
// They were read-only, so MarshalTo emitted values UnmarshalFrom refuses. For a
// transport-only type that is an asymmetry; for Membership and ConfigChange it
// is a DURABILITY bug, because both are committed to the raft log:
//
//   - Membership is the Cmd of the EntryInitialMembership at log index 1.
//     Host.StartShard copies the caller's members map into it unchecked, so an
//     over-length address bootstrapped the shard successfully and then made
//     every subsequent start fail: reconstructShardMembership decodes that
//     entry, getMap refuses the value, and the error propagates out of
//     StartShard. Permanently, on data the shard itself wrote.
//   - ConfigChange is the Cmd of an EntryConfigChange. An unreadable one is a
//     committed entry every replica faults on (ErrCorruptConfigEntry).
//
// waldb.writeEntries and waldb.checkMembershipSize already refuse to write what
// their own replay cannot read, calling it "an entry that is durable but
// unreadable". These are the same rule on this package's formats.
// ---------------------------------------------------------------------------

// TestMembershipMarshalRefusesOversizedAddress is the DURABLE-path assertion:
// it does not stop at the codec, it drives the round trip that a restart
// performs and shows which half breaks.
func TestMembershipMarshalRefusesOversizedAddress(t *testing.T) {
	// The shape Host.StartShard produces: the operator's members map copied
	// verbatim into Addresses.
	oversized := Membership{
		Addresses: map[uint64]string{
			1: "127.0.0.1:5001",
			2: strings.Repeat("x", MaxAddressLength+1),
		},
	}

	buf := make([]byte, oversized.Size())
	_, err := oversized.MarshalTo(buf)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("MarshalTo: got %v, want ErrAddressTooLong.\n"+
			"An encoder that accepts this writes the bytes into the committed "+
			"EntryInitialMembership at log index 1, and the NEXT start of the shard "+
			"cannot decode them — StartShard then fails forever with no remedy but "+
			"deleting the data directory", err)
	}

	// The round trip is what the durable path performs. Encode the same
	// membership by hand — the encoder refuses — and show the decoder rejects it, so the two ends genuinely agree.
	raw := marshalMembershipUnchecked(t, oversized)
	var decoded Membership
	if _, err := decoded.UnmarshalFrom(raw); !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("UnmarshalFrom(hand-encoded oversized membership): got %v, want "+
			"ErrAddressTooLong; the write-side check exists precisely because the "+
			"read side refuses these bytes", err)
	}
}

// TestMembershipMarshalRefusesOversizedMaps covers the count limit on every map
// including the two identity maps, and TestMembershipMarshalRefusesOversized-
// Address covers the per-value length. Between them nothing this encoder can
// emit is refused by its own decoder.
func TestMembershipMarshalRefusesOversizedMaps(t *testing.T) {
	bigStrings := make(map[uint64]string, MaxMembershipEntries+1)
	for i := uint64(0); i <= MaxMembershipEntries; i++ {
		bigStrings[i+1] = "a"
	}
	bigBools := make(map[uint64]bool, MaxMembershipEntries+1)
	for i := uint64(0); i <= MaxMembershipEntries; i++ {
		bigBools[i+1] = true
	}

	for _, tc := range []struct {
		name string
		m    Membership
	}{
		{"Addresses", Membership{Addresses: bigStrings}},
		{"Observers", Membership{Observers: bigStrings}},
		{"Witnesses", Membership{Witnesses: bigStrings}},
		{"Removed", Membership{Removed: bigBools}},
		{"Identities", Membership{Identities: bigStrings}},
		{"RemovedIdentities", Membership{RemovedIdentities: bigStrings}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.m.Size())
			if _, err := tc.m.MarshalTo(buf); !errors.Is(err, ErrTooManyMembershipEntries) {
				t.Fatalf("MarshalTo: got %v, want ErrTooManyMembershipEntries", err)
			}
		})
	}
}

// TestMembershipMarshalAcceptsTheLimitExactly is the boundary control: the
// write-side check must refuse only what the read side refuses, and a map
// holding exactly MaxMembershipEntries entries is legal at both ends. Without
// this the previous test would pass on an encoder that refused everything.
func TestMembershipMarshalAcceptsTheLimitExactly(t *testing.T) {
	atLimit := make(map[uint64]string, MaxMembershipEntries)
	for i := uint64(0); i < MaxMembershipEntries; i++ {
		atLimit[i+1] = "a"
	}
	m := Membership{Addresses: atLimit}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo at exactly MaxMembershipEntries: %v", err)
	}
	var decoded Membership
	if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("UnmarshalFrom at exactly MaxMembershipEntries: %v", err)
	}
	if len(decoded.Addresses) != MaxMembershipEntries {
		t.Fatalf("round trip lost entries: got %d, want %d",
			len(decoded.Addresses), MaxMembershipEntries)
	}

	// The longest legal address round-trips too.
	longest := Membership{Addresses: map[uint64]string{1: strings.Repeat("x", MaxAddressLength)}}
	lbuf := make([]byte, longest.Size())
	ln, err := longest.MarshalTo(lbuf)
	if err != nil {
		t.Fatalf("MarshalTo at exactly MaxAddressLength: %v", err)
	}
	var ldecoded Membership
	if _, err := ldecoded.UnmarshalFrom(lbuf[:ln]); err != nil {
		t.Fatalf("UnmarshalFrom at exactly MaxAddressLength: %v", err)
	}
}

// TestConfigChangeMarshalRefusesOversizedIdentity covers the trailing identity
// field, whose bound is the only one on this type that a caller reaches through
// the Host API (RequestAddNodeWithIdentity).
func TestConfigChangeMarshalRefusesOversizedIdentity(t *testing.T) {
	cc := ConfigChange{
		Type:         AddNode,
		ReplicaID:    2,
		Address:      "127.0.0.1:5002",
		NodeIdentity: strings.Repeat("x", MaxNodeIdentityLength+1),
	}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); !errors.Is(err, ErrNodeIdentityTooLong) {
		t.Fatalf("MarshalTo: got %v, want ErrNodeIdentityTooLong", err)
	}

	// Boundary control: exactly at the limit must still round-trip.
	cc.NodeIdentity = strings.Repeat("x", MaxNodeIdentityLength)
	buf = make([]byte, cc.Size())
	n, err := cc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo at exactly MaxNodeIdentityLength: %v", err)
	}
	var decoded ConfigChange
	if _, err := decoded.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("UnmarshalFrom at exactly MaxNodeIdentityLength: %v", err)
	}
	if decoded.NodeIdentity != cc.NodeIdentity {
		t.Fatal("the longest legal identity did not round-trip")
	}
}

// TestMarshalRefusesWhatUnmarshalRefuses sweeps the remaining five limits the
// reproduction listed, so no type is left able to emit what this package cannot
// parse.
func TestMarshalRefusesWhatUnmarshalRefuses(t *testing.T) {
	t.Run("Entry.Cmd over MaxEntrySize", func(t *testing.T) {
		e := Entry{Index: 1, Term: 1, Type: EntryNormal, Cmd: make([]byte, MaxEntrySize+1)}
		buf := make([]byte, e.Size())
		if _, err := e.MarshalTo(buf); !errors.Is(err, ErrEntrySizeLimitExceeded) {
			t.Fatalf("MarshalTo: got %v, want ErrEntrySizeLimitExceeded", err)
		}
	})

	t.Run("Message.Entries over MaxEntriesPerMessage", func(t *testing.T) {
		m := Message{Type: Replicate, Entries: make([]Entry, MaxEntriesPerMessage+1)}
		for i := range m.Entries {
			m.Entries[i] = Entry{Index: uint64(i + 1), Term: 1, Type: EntryNormal}
		}
		buf := make([]byte, m.Size())
		if _, err := m.MarshalTo(buf); !errors.Is(err, ErrTooManyMessages) {
			t.Fatalf("MarshalTo: got %v, want ErrTooManyMessages", err)
		}
	})

	t.Run("MessageBatch.Requests over MaxMessagesPerBatch", func(t *testing.T) {
		mb := MessageBatch{BinVer: WireVersion, Requests: make([]Message, MaxMessagesPerBatch+1)}
		for i := range mb.Requests {
			mb.Requests[i] = Message{Type: Heartbeat}
		}
		buf := make([]byte, mb.Size())
		if _, err := mb.MarshalTo(buf); !errors.Is(err, ErrTooManyMessages) {
			t.Fatalf("MarshalTo: got %v, want ErrTooManyMessages", err)
		}
	})

	t.Run("SnapshotFile.Metadata over MaxMetadataLen", func(t *testing.T) {
		sf := SnapshotFile{Filepath: "f", Metadata: make([]byte, MaxMetadataLen+1)}
		buf := make([]byte, sf.Size())
		if _, err := sf.MarshalTo(buf); !errors.Is(err, ErrMetadataTooLong) {
			t.Fatalf("MarshalTo: got %v, want ErrMetadataTooLong", err)
		}
	})

	t.Run("Snapshot.Filepath over MaxFilepathLen", func(t *testing.T) {
		s := Snapshot{Index: 1, Term: 1, Filepath: strings.Repeat("p", MaxFilepathLen+1)}
		buf := make([]byte, s.Size())
		if _, err := s.MarshalTo(buf); !errors.Is(err, ErrFilepathTooLong) {
			t.Fatalf("MarshalTo: got %v, want ErrFilepathTooLong", err)
		}
	})
}

// marshalMembershipUnchecked encodes a Membership with the write-side limits
// BYPASSED. It exists so the read-side half of the round-trip assertion can
// be driven even though MarshalTo refuses to produce these bytes.
func marshalMembershipUnchecked(t *testing.T, m Membership) []byte {
	t.Helper()
	buf := make([]byte, m.Size())
	buf[0] = membershipFormatVersion
	putUint64(buf[1:], m.ConfigChangeID)
	putUint64(buf[9:], m.MaxWitnesses)
	putBool(buf[17:], m.RequireFullReplicaMajority)
	putUint64(buf[sessionPolicyOffset:], m.SessionExpiryEntries)
	putUint64(buf[sessionPolicyOffset+8:], m.MaxTotalSessions)
	putUint64(buf[sessionPolicyOffset+16:], m.MaxSessionsPerClient)
	putUint64(buf[sessionPolicyOffset+24:], m.MaxSessionsPerOwner)
	offset := membershipFixedSize
	offset += putMap(buf[offset:], m.Addresses)
	offset += putMap(buf[offset:], m.Observers)
	offset += putMap(buf[offset:], m.Witnesses)
	offset += putBoolMap(buf[offset:], m.Removed)
	offset += putMap(buf[offset:], m.Identities)
	offset += putMap(buf[offset:], m.RemovedIdentities)
	return buf[:offset]
}

// ---------------------------------------------------------------------------
// Membership.UnmarshalFrom must be all-or-nothing
// ---------------------------------------------------------------------------

// TestMembershipUnmarshalLeavesNoPartialState pins that a failed decode leaves
// the receiver untouched.
//
// A decoder that cleared some fields AFTER the field reads and wrote others
// BEFORE them would leave a mixture of old and new on failure: a decode that
// failed early would keep a STALE Identities map — which authorizes a replica the decoded
// configuration never named — and one that failed in the maps left a NEW
// session policy sitting on an OLD configuration, which the apply path reads.
// Decoding into a local and assigning the receiver only on success is what
// removes the whole class.
//
// Latent today, because all three call sites abandon the value on error. That
// is exactly why it needs a test: the invariant is a property of the
// decoder, and nothing else would notice if a future refactor moved it back
// into the callers.
func TestMembershipUnmarshalLeavesNoPartialState(t *testing.T) {
	// The value the receiver holds before the failed decode. Every field is
	// distinctive so any survivor is identifiable.
	pristine := func() Membership {
		m := Membership{
			ConfigChangeID:             42,
			MaxWitnesses:               7,
			RequireFullReplicaMajority: true,
			Addresses:                  map[uint64]string{1: "old-a:1"},
			Observers:                  map[uint64]string{2: "old-b:2"},
			Witnesses:                  map[uint64]string{3: "old-c:3"},
			Removed:                    map[uint64]bool{4: true},
			Identities:                 map[uint64]string{1: "spki:old"},
			RemovedIdentities:          map[uint64]string{4: "spki:old-removed"},
		}
		m.SetSessionPolicy(SessionPolicy{
			ExpiryEntries: 11, MaxTotalSessions: 22, MaxSessionsPerClient: 33,
		})
		return m
	}

	// A well-formed section, truncated at successive points so the decode fails
	// at every stage: inside the fixed region, inside each map.
	full := func() []byte {
		src := Membership{
			ConfigChangeID: 99,
			Addresses:      map[uint64]string{9: "new-a:9"},
			Observers:      map[uint64]string{8: "new-b:8"},
			Witnesses:      map[uint64]string{7: "new-c:7"},
			Removed:        map[uint64]bool{6: true},
			Identities:     map[uint64]string{9: "spki:new"},
		}
		src.RemovedIdentities = map[uint64]string{6: "spki:new-removed"}
		src.SetSessionPolicy(SessionPolicy{
			ExpiryEntries: 1, MaxTotalSessions: 2, MaxSessionsPerClient: 3,
		})
		buf := make([]byte, src.Size())
		n, err := src.MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		return buf[:n]
	}()

	// Every truncation length that still carries the version byte. Each one must
	// either decode cleanly (there is none below full length here) or leave the
	// receiver byte-for-byte as it was.
	for cut := 1; cut < len(full); cut++ {
		want := pristine()
		got := pristine()
		if _, err := got.UnmarshalFrom(full[:cut]); err == nil {
			// No prefix can decode: there is one layout, its last map is the one
			// being cut, and there is no shorter layout for a prefix to satisfy.
			t.Fatalf("UnmarshalFrom(truncated to %d of %d bytes) succeeded", cut, len(full))
		}
		assertMembershipEqual(t, &want, &got)
		if got.ConfigChangeID != want.ConfigChangeID {
			t.Fatalf("cut %d: ConfigChangeID = %d, want %d (the failed decode "+
				"overwrote it)", cut, got.ConfigChangeID, want.ConfigChangeID)
		}
		if got.MaxWitnesses != want.MaxWitnesses ||
			got.RequireFullReplicaMajority != want.RequireFullReplicaMajority {
			t.Fatalf("cut %d: the failed decode overwrote the witness policy: %+v", cut, got)
		}
	}
}

// TestMembershipUnmarshalClearsStaleReceiverFields is the SUCCESS-path half of
// the all-or-nothing rule, and the shape the decoder's comment names: a section
// that carries no identities and no session policy, decoded into a receiver
// that already holds some, must not leave any of them behind.
//
// The fields are absent-as-empty rather than absent-as-missing — the layout
// always writes all six maps and the policy — so the only way a stale value
// survives is a decoder that skips a field instead of overwriting it. That
// distinction is invisible in a fresh-receiver test and load-bearing here: the
// buffer pools in the receive path hand the same Membership back repeatedly.
//
// A surviving Identities map is the dangerous one — it authorizes a replica the
// decoded configuration never named — and a surviving RemovedIdentities map is
// the mirror image, denying a sender the decoded configuration never tombstoned.
func TestMembershipUnmarshalClearsStaleReceiverFields(t *testing.T) {
	src := Membership{
		ConfigChangeID: 5,
		Addresses:      map[uint64]string{1: "a:1"},
	}
	buf := make([]byte, src.Size())
	n, err := src.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	got := Membership{
		Identities:        map[uint64]string{9: "spki:stale-active"},
		RemovedIdentities: map[uint64]string{8: "spki:stale-tombstone"},
	}
	got.SetSessionPolicy(SessionPolicy{
		ExpiryEntries: 77, MaxTotalSessions: 88, MaxSessionsPerClient: 99,
	})
	if _, err := got.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if len(got.Identities) != 0 {
		t.Fatalf("decode left a stale Identities map (%+v): it would authorize "+
			"a replica the decoded configuration never named", got.Identities)
	}
	if len(got.RemovedIdentities) != 0 {
		t.Fatalf("decode left a stale RemovedIdentities map (%+v): it would deny "+
			"a sender the decoded configuration never tombstoned", got.RemovedIdentities)
	}
	if got.SessionPolicy().IsSet() {
		t.Fatalf("decode left a stale session policy (%+v): the apply path "+
			"would expire sessions at indexes the shard never agreed on",
			got.SessionPolicy())
	}
}
