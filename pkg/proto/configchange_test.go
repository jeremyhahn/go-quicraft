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

// TestConfigChangeAddressTooLong pins the address bound at BOTH ends.
//
// The write half is what makes the two agree: a ConfigChange is marshaled into
// an Entry.Cmd and COMMITTED, so if MarshalTo emitted this payload, an
// over-length operator-supplied address would produce a committed entry every
// replica then faulted on
// (peer.applyMembershipEntry -> ErrCorruptConfigEntry). The buffer for the read
// half is therefore built by hand rather than by marshaling, because the encoder
// refuses to produce it — which is the point.
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

// TestConfigChangeUnmarshalInvalidType pins the type bound at BOTH ends: the
// encoder refuses an unknown type, and a buffer that carries one anyway — which
// is only reachable by patching the bytes, i.e. by corruption or tampering,
// exactly the case the decoder check exists for — is refused on read.
func TestConfigChangeUnmarshalInvalidType(t *testing.T) {
	for _, tc := range []struct {
		name    string
		badType uint64
	}{
		{"type beyond UpdateWireVersion is rejected", UpdateWireVersion + 1},
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

// TestConfigChangeUnmarshalValidTypes decodes the accepted membership types no
// other test round-trips; AddNode, RemoveNode and UpdateWitnessPolicy are
// TestConfigChangeWithNoIdentityRoundTrips.
func TestConfigChangeUnmarshalValidTypes(t *testing.T) {
	validTypes := []struct {
		name string
		typ  uint64
	}{
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
