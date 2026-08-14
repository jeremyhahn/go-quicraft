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

func TestConfigChangeMarshalUnmarshalRoundTrip(t *testing.T) {
	cc := ConfigChange{
		ConfigChangeID: 42,
		Type:           AddNode,
		ReplicaID:      5,
		Address:        "10.0.0.5:5000",
		Initialize:     true,
	}
	buf := make([]byte, cc.Size())
	n, err := cc.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != cc.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, cc.Size())
	}

	var cc2 ConfigChange
	consumed, err := cc2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertConfigChangeEqual(t, &cc, &cc2)
}

func TestConfigChangeMarshalUnmarshalRemoveNode(t *testing.T) {
	cc := ConfigChange{
		ConfigChangeID: 10,
		Type:           RemoveNode,
		ReplicaID:      3,
		Address:        "",
		Initialize:     false,
	}
	buf := make([]byte, cc.Size())
	cc.MarshalTo(buf)
	var cc2 ConfigChange
	cc2.UnmarshalFrom(buf)
	assertConfigChangeEqual(t, &cc, &cc2)
}

func TestConfigChangeMarshalUnmarshalAddNonVoting(t *testing.T) {
	cc := ConfigChange{
		ConfigChangeID: 20,
		Type:           AddNonVoting,
		ReplicaID:      7,
		Address:        "observer:5000",
		Initialize:     true,
	}
	buf := make([]byte, cc.Size())
	cc.MarshalTo(buf)
	var cc2 ConfigChange
	cc2.UnmarshalFrom(buf)
	assertConfigChangeEqual(t, &cc, &cc2)
}

func TestConfigChangeMarshalUnmarshalAddWitness(t *testing.T) {
	cc := ConfigChange{
		ConfigChangeID: 30,
		Type:           AddWitness,
		ReplicaID:      8,
		Address:        "witness:5000",
		Initialize:     false,
	}
	buf := make([]byte, cc.Size())
	cc.MarshalTo(buf)
	var cc2 ConfigChange
	cc2.UnmarshalFrom(buf)
	assertConfigChangeEqual(t, &cc, &cc2)
}

// TestConfigChangeAddressTooLong pins the address bound at BOTH ends.
//
// The read half is the original assertion. The write half is what makes the two
// agree: MarshalTo used to emit this payload happily, and a ConfigChange is
// marshaled into an Entry.Cmd and COMMITTED, so an over-length operator-supplied
// address produced a committed entry every replica then faulted on
// (peer.applyMembershipEntry -> ErrCorruptConfigEntry). The buffer for the read
// half is therefore built by hand rather than by marshaling, because the encoder
// now refuses to produce it — which is the point.
func TestConfigChangeAddressTooLong(t *testing.T) {
	longAddr := strings.Repeat("x", MaxAddressLength+1)
	cc := ConfigChange{
		ConfigChangeID: 1,
		Type:           AddNode,
		ReplicaID:      1,
		Address:        longAddr,
		Initialize:     false,
	}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("MarshalTo: got %v, want ErrAddressTooLong: the encoder must not "+
			"produce a payload its own decoder refuses, because this one is committed",
			err)
	}

	// Hand-built: the fixed prefix, then an oversized address length prefix.
	raw := make([]byte, 24+4+len(longAddr))
	putUint64(raw[0:], 1)
	putUint64(raw[8:], AddNode)
	putUint64(raw[16:], 1)
	putString(raw[24:], longAddr)
	var cc2 ConfigChange
	_, err := cc2.UnmarshalFrom(raw)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrAddressTooLong", err)
	}
}

func TestConfigChangeBufferTooSmallMarshal(t *testing.T) {
	cc := ConfigChange{Address: "x"}
	buf := make([]byte, cc.Size()-1)
	_, err := cc.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestConfigChangeBufferTooSmallUnmarshalHeader(t *testing.T) {
	buf := make([]byte, 20) // too short for 3 uint64 + addr length
	var cc ConfigChange
	_, err := cc.UnmarshalFrom(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestConfigChangeBufferTooSmallUnmarshalInitialize(t *testing.T) {
	cc := ConfigChange{Address: "x"}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	// Cut INTO the witness-policy fields. There is one layout and every field
	// in it is mandatory, so any prefix is a truncation — including the cut at
	// -4, which used to land on the NodeIdentity length prefix and decode
	// cleanly as a shorter layout. TestConfigChangeRejectsEveryStrictPrefix
	// sweeps all of them; these two are the boundaries that moved.
	for _, cut := range []int{4, 6} {
		var cc2 ConfigChange
		_, err := cc2.UnmarshalFrom(buf[:cc.Size()-cut])
		if !errors.Is(err, ErrBufferTooSmall) {
			t.Fatalf("UnmarshalFrom(-%d bytes): got %v, want ErrBufferTooSmall", cut, err)
		}
	}
}

func TestConfigChangeReset(t *testing.T) {
	cc := ConfigChange{
		ConfigChangeID: 42,
		Type:           AddNode,
		ReplicaID:      5,
		Address:        "addr",
		Initialize:     true,
	}
	cc.Reset()
	if cc.ConfigChangeID != 0 || cc.Type != 0 || cc.ReplicaID != 0 ||
		cc.Address != "" || cc.Initialize {
		t.Fatal("Reset did not clear all fields")
	}
}

func TestConfigChangeSize(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want int
	}{
		{"empty address", "", configChangeFixedSize + 4},
		{"short address", "a:1", configChangeFixedSize + 3 + 4},
		{"typical", "192.168.1.100:5000", configChangeFixedSize + 18 + 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := ConfigChange{Address: tt.addr}
			if got := cc.Size(); got != tt.want {
				t.Fatalf("Size: got %d, want %d", got, tt.want)
			}
		})
	}
}

// TestConfigChangeUpdateWitnessPolicyRoundTrip verifies the policy fields
// survive marshal/unmarshal for the UpdateWitnessPolicy change type.
func TestConfigChangeUpdateWitnessPolicyRoundTrip(t *testing.T) {
	cc := ConfigChange{
		ConfigChangeID:             9,
		Type:                       UpdateWitnessPolicy,
		MaxWitnesses:               3,
		RequireFullReplicaMajority: true,
	}
	buf := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	var cc2 ConfigChange
	if _, err := cc2.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if cc2 != cc {
		t.Fatalf("round trip mismatch: got %+v, want %+v", cc2, cc)
	}
}

// TestConfigChangeUnmarshalInvalidType pins the type bound at BOTH ends: the
// encoder refuses an unknown type, and a buffer that carries one anyway — which
// is now only reachable by patching the bytes, i.e. by corruption or tampering,
// exactly the case the decoder check exists for — is refused on read.
func TestConfigChangeUnmarshalInvalidType(t *testing.T) {
	for _, tc := range []struct {
		name    string
		badType uint64
	}{
		{"type beyond UpdateWitnessPolicy is rejected", UpdateWitnessPolicy + 1},
		{"large invalid type is rejected", 999},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := ConfigChange{
				ConfigChangeID: 1,
				Type:           tc.badType,
				ReplicaID:      1,
				Address:        "addr:5000",
			}
			buf := make([]byte, cc.Size())
			if _, err := cc.MarshalTo(buf); !errors.Is(err, ErrInvalidConfigChangeType) {
				t.Fatalf("MarshalTo: got %v, want ErrInvalidConfigChangeType", err)
			}

			// Marshal a VALID type, then patch the type field, so the rest of the
			// payload is well formed and the decoder is judged on the type alone.
			cc.Type = AddNode
			if _, err := cc.MarshalTo(buf); err != nil {
				t.Fatalf("MarshalTo(valid): %v", err)
			}
			putUint64(buf[8:], tc.badType)

			var cc2 ConfigChange
			_, err := cc2.UnmarshalFrom(buf)
			if !errors.Is(err, ErrInvalidConfigChangeType) {
				t.Fatalf("UnmarshalFrom: got %v, want ErrInvalidConfigChangeType", err)
			}
		})
	}
}

func TestConfigChangeUnmarshalValidTypes(t *testing.T) {
	validTypes := []struct {
		name string
		typ  uint64
	}{
		{"AddNode", AddNode},
		{"RemoveNode", RemoveNode},
		{"AddNonVoting", AddNonVoting},
		{"AddWitness", AddWitness},
	}

	for _, tc := range validTypes {
		t.Run(tc.name, func(t *testing.T) {
			cc := ConfigChange{
				ConfigChangeID: 1,
				Type:           tc.typ,
				ReplicaID:      1,
				Address:        "addr:5000",
			}
			buf := make([]byte, cc.Size())
			cc.MarshalTo(buf)

			var cc2 ConfigChange
			_, err := cc2.UnmarshalFrom(buf)
			if err != nil {
				t.Fatalf("UnmarshalFrom: unexpected error for type %s: %v", tc.name, err)
			}
			if cc2.Type != tc.typ {
				t.Fatalf("Type: got %d, want %d", cc2.Type, tc.typ)
			}
		})
	}
}

func assertConfigChangeEqual(t *testing.T, want, got *ConfigChange) {
	t.Helper()
	if got.ConfigChangeID != want.ConfigChangeID {
		t.Fatalf("ConfigChangeID: got %d, want %d", got.ConfigChangeID, want.ConfigChangeID)
	}
	if got.Type != want.Type {
		t.Fatalf("Type: got %d, want %d", got.Type, want.Type)
	}
	if got.ReplicaID != want.ReplicaID {
		t.Fatalf("ReplicaID: got %d, want %d", got.ReplicaID, want.ReplicaID)
	}
	if got.Address != want.Address {
		t.Fatalf("Address: got %q, want %q", got.Address, want.Address)
	}
	if got.Initialize != want.Initialize {
		t.Fatalf("Initialize: got %v, want %v", got.Initialize, want.Initialize)
	}
}
