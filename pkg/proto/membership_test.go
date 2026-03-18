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
	buf := make([]byte, 8) // too short: needs ConfigChangeID + at least map count
	var m Membership
	_, err := m.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMembershipAddressTooLong(t *testing.T) {
	longAddr := strings.Repeat("x", MaxAddressLength+1)
	m := Membership{
		Addresses: map[uint64]string{1: longAddr},
		Observers: map[uint64]string{},
		Witnesses: map[uint64]string{},
		Removed:   map[uint64]bool{},
	}
	buf := make([]byte, m.Size())
	m.MarshalTo(buf)
	var m2 Membership
	_, err := m2.UnmarshalFrom(buf)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrAddressTooLong", err)
	}
}

func TestMembershipTooManyEntries(t *testing.T) {
	// Craft a buffer with Addresses count = maxMembershipEntries + 1
	buf := make([]byte, 8+4)
	putUint64(buf, 0)                          // ConfigChangeID
	putUint32(buf[8:], maxMembershipEntries+1) // Addresses count
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
	assertStringMapEqual(t, "Addresses", want.Addresses, got.Addresses)
	assertStringMapEqual(t, "Observers", want.Observers, got.Observers)
	assertStringMapEqual(t, "Witnesses", want.Witnesses, got.Witnesses)
	assertBoolMapEqual(t, "Removed", want.Removed, got.Removed)
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
