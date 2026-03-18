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
	cc.MarshalTo(buf)
	var cc2 ConfigChange
	_, err := cc2.UnmarshalFrom(buf)
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
	cc.MarshalTo(buf)
	// Truncate to cut off Initialize field
	var cc2 ConfigChange
	_, err := cc2.UnmarshalFrom(buf[:cc.Size()-4])
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
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
		{"empty address", "", configChangeFixedSize},
		{"short address", "a:1", configChangeFixedSize + 3},
		{"typical", "192.168.1.100:5000", configChangeFixedSize + 18},
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

func TestConfigChangeUnmarshalInvalidType(t *testing.T) {
	t.Run("type beyond AddWitness is rejected", func(t *testing.T) {
		cc := ConfigChange{
			ConfigChangeID: 1,
			Type:           AddWitness + 1, // 4, one beyond the last valid type
			ReplicaID:      1,
			Address:        "addr:5000",
			Initialize:     false,
		}
		buf := make([]byte, cc.Size())
		cc.MarshalTo(buf)

		var cc2 ConfigChange
		_, err := cc2.UnmarshalFrom(buf)
		if !errors.Is(err, ErrInvalidConfigChangeType) {
			t.Fatalf("UnmarshalFrom: got %v, want ErrInvalidConfigChangeType", err)
		}
	})

	t.Run("large invalid type is rejected", func(t *testing.T) {
		cc := ConfigChange{
			ConfigChangeID: 1,
			Type:           999,
			ReplicaID:      1,
			Address:        "addr:5000",
		}
		buf := make([]byte, cc.Size())
		cc.MarshalTo(buf)

		var cc2 ConfigChange
		_, err := cc2.UnmarshalFrom(buf)
		if !errors.Is(err, ErrInvalidConfigChangeType) {
			t.Fatalf("UnmarshalFrom: got %v, want ErrInvalidConfigChangeType", err)
		}
	})
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
