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

func TestMembershipMarshalUnmarshalRoundTrip(t *testing.T) {
	m := Membership{
		ConfigChangeID: 42,
		Addresses: map[uint64]string{
			1: "node-1:5000",
			2: "node-2:5000",
			3: "node-3:5000",
		},
		Observers: map[uint64]string{
			10: "observer-10:5000",
		},
		Witnesses: map[uint64]string{
			20: "witness-20:5000",
		},
		Removed: map[uint64]bool{
			99: true,
		},
	}
	buf := make([]byte, m.Size())
	n, err := m.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != m.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, m.Size())
	}

	var m2 Membership
	consumed, err := m2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertMembershipEqual(t, &m, &m2)
}

func TestMembershipMarshalUnmarshalEmptyMaps(t *testing.T) {
	m := Membership{
		ConfigChangeID: 1,
		Addresses:      map[uint64]string{},
		Observers:      map[uint64]string{},
		Witnesses:      map[uint64]string{},
		Removed:        map[uint64]bool{},
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Membership
	m2.UnmarshalFrom(buf)
	assertMembershipEqual(t, &m, &m2)
}

func TestMembershipMarshalUnmarshalNilMaps(t *testing.T) {
	m := Membership{
		ConfigChangeID: 0,
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Membership
	_, err := m2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if m2.ConfigChangeID != 0 {
		t.Fatalf("ConfigChangeID: got %d, want 0", m2.ConfigChangeID)
	}
	if len(m2.Addresses) != 0 {
		t.Fatalf("Addresses: got %d entries, want 0", len(m2.Addresses))
	}
}

func TestMembershipDeterministicEncoding(t *testing.T) {
	m := Membership{
		ConfigChangeID: 1,
		Addresses: map[uint64]string{
			3: "c",
			1: "a",
			2: "b",
		},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]bool{},
	}
	buf1 := make([]byte, m.Size())
	buf2 := make([]byte, m.Size())
	m.MarshalTo(buf1)
	m.MarshalTo(buf2)
	for i := range buf1 {
		if buf1[i] != buf2[i] {
			t.Fatalf("Non-deterministic encoding at byte %d", i)
		}
	}
}

func TestMembershipBufferTooSmallMarshal(t *testing.T) {
	m := Membership{Addresses: map[uint64]string{1: "x"}}
	buf := make([]byte, m.Size()-1)
	_, err := m.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMembershipBufferTooSmallUnmarshal(t *testing.T) {
	// Correct version byte, then too short for the fixed region plus the
	// first map count.
	buf := make([]byte, 8)
	buf[0] = membershipFormatVersion
	var m Membership
	_, err := m.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

// TestMembershipAddressTooLong pins the address bound at BOTH ends.
//
// The read half is the original assertion. The write half is what closes the
// DURABLE hole: Host.StartShard copies the caller's members map straight into
// Addresses and commits the marshaled result as the EntryInitialMembership at
// log index 1, so an encoder that emitted this produced a bootstrap entry the
// next start could not decode — and StartShard then aborted forever, on data the
// shard itself wrote. The buffer for the read half is built by hand because the
// encoder now refuses to produce it, which is exactly the fix.
func TestMembershipAddressTooLong(t *testing.T) {
	longAddr := strings.Repeat("x", MaxAddressLength+1)
	m := Membership{
		Addresses: map[uint64]string{1: longAddr},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]bool{},
	}
	buf := make([]byte, m.Size())
	if _, err := m.MarshalTo(buf); !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("MarshalTo: got %v, want ErrAddressTooLong: the encoder must not "+
			"produce a membership its own decoder refuses, because this one is "+
			"committed at log index 1", err)
	}

	// Hand-built v5 section: the fixed region, then an Addresses map whose one
	// value is over the limit. Everything after it is unreachable, so it is
	// omitted; the decoder must fail before it gets there.
	raw := make([]byte, membershipFixedSize+4+8+4+len(longAddr))
	raw[0] = membershipFormatVersion
	putMap(raw[membershipFixedSize:], map[uint64]string{1: longAddr})
	var m2 Membership
	_, err := m2.UnmarshalFrom(raw)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrAddressTooLong", err)
	}
}

// TestMembershipPolicyRoundTrip verifies the replicated witness-policy
// fields survive marshal/unmarshal.
func TestMembershipPolicyRoundTrip(t *testing.T) {
	m := Membership{
		ConfigChangeID:             11,
		MaxWitnesses:               4,
		RequireFullReplicaMajority: true,
		Addresses:                  map[uint64]string{1: "a"},
	}
	buf := make([]byte, m.Size())
	if _, err := m.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var m2 Membership
	if _, err := m2.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if m2.MaxWitnesses != 4 || !m2.RequireFullReplicaMajority || m2.ConfigChangeID != 11 {
		t.Fatalf("round trip mismatch: %+v", m2)
	}
}

func TestMembershipTooManyEntries(t *testing.T) {
	// Craft a buffer with Addresses count = MaxMembershipEntries + 1
	buf := make([]byte, membershipFixedSize+4)
	buf[0] = membershipFormatVersion
	putUint64(buf[1:], 0)                                        // ConfigChangeID
	putUint64(buf[9:], 0)                                        // MaxWitnesses
	putBool(buf[17:], false)                                     // RequireFullReplicaMajority
	putUint32(buf[membershipFixedSize:], MaxMembershipEntries+1) // Addresses count
	var m Membership
	_, err := m.UnmarshalFrom(buf)
	if !errors.Is(err, ErrTooManyMembershipEntries) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrTooManyMembershipEntries", err)
	}
}

func TestMembershipReset(t *testing.T) {
	m := Membership{
		ConfigChangeID: 42,
		Addresses:      map[uint64]string{1: "a"},
		Observers:      map[uint64]string{2: "b"},
		Witnesses:      map[uint64]string{3: "c"},
		Removed:        map[uint64]bool{4: true},
	}
	m.Reset()
	if m.ConfigChangeID != 0 {
		t.Fatal("ConfigChangeID not reset")
	}
	if m.Addresses != nil || m.Observers != nil || m.Witnesses != nil || m.Removed != nil {
		t.Fatal("Maps not reset to nil")
	}
}

func TestMembershipLargeCluster(t *testing.T) {
	m := Membership{
		ConfigChangeID: 100,
		Addresses:      make(map[uint64]string),
		Observers:      make(map[uint64]string),
		Witnesses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
	}
	for i := uint64(1); i <= 100; i++ {
		m.Addresses[i] = "node:5000"
	}
	for i := uint64(101); i <= 110; i++ {
		m.Observers[i] = "observer:5000"
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Membership
	_, err := m2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	assertMembershipEqual(t, &m, &m2)
}

func assertMembershipEqual(t *testing.T, want, got *Membership) {
	t.Helper()
	if got.ConfigChangeID != want.ConfigChangeID {
		t.Fatalf("ConfigChangeID: got %d, want %d", got.ConfigChangeID, want.ConfigChangeID)
	}
	// The session policy is compared here, not in the individual tests, so
	// every round-trip in the package pins it. It is read by the apply path:
	// a codec that drops it does not fail, it silently returns the replicas to
	// expiring sessions on per-node configuration.
	if got.SessionPolicy() != want.SessionPolicy() {
		t.Fatalf("SessionPolicy: got %+v, want %+v", got.SessionPolicy(), want.SessionPolicy())
	}
	assertStringMapEqual(t, "Addresses", want.Addresses, got.Addresses)
	assertStringMapEqual(t, "Observers", want.Observers, got.Observers)
	assertStringMapEqual(t, "Witnesses", want.Witnesses, got.Witnesses)
	assertBoolMapEqual(t, "Removed", want.Removed, got.Removed)
	// The two authorization maps are compared here for the same reason the
	// session policy is: a codec that drops either does not fail, it silently
	// changes who this shard authorizes. Identities is the receive-side quorum
	// gate's only input where it is present; RemovedIdentities is the half of
	// the removal deny-list a removed replica cannot evade by renaming itself.
	assertStringMapEqual(t, "Identities", want.Identities, got.Identities)
	assertStringMapEqual(t, "RemovedIdentities", want.RemovedIdentities, got.RemovedIdentities)
}

func assertStringMapEqual(t *testing.T, name string, want, got map[uint64]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d entries, want %d", name, len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s[%d]: got %q, want %q", name, k, got[k], v)
		}
	}
}

func assertBoolMapEqual(t *testing.T, name string, want, got map[uint64]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d entries, want %d", name, len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s[%d]: got %v, want %v", name, k, got[k], v)
		}
	}
}
