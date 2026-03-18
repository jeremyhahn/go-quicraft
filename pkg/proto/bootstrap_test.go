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
	"testing"
)

func TestBootstrapMarshalUnmarshalRoundTrip(t *testing.T) {
	b := Bootstrap{
		Addresses: map[uint64]string{
			1: "node-1:5000",
			2: "node-2:5000",
			3: "node-3:5000",
		},
		Join: false,
		Type: RegularStateMachine,
	}
	buf := make([]byte, b.Size())
	n, err := b.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	if n != b.Size() {
		t.Fatalf("MarshalTo: wrote %d, want %d", n, b.Size())
	}

	var b2 Bootstrap
	consumed, err := b2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if consumed != n {
		t.Fatalf("UnmarshalFrom: consumed %d, want %d", consumed, n)
	}
	assertBootstrapEqual(t, &b, &b2)
}

func TestBootstrapMarshalUnmarshalJoin(t *testing.T) {
	b := Bootstrap{
		Addresses: map[uint64]string{},
		Join:      true,
		Type:      OnDiskStateMachine,
	}
	buf := make([]byte, b.Size())
	b.MarshalTo(buf)
	var b2 Bootstrap
	b2.UnmarshalFrom(buf)
	assertBootstrapEqual(t, &b, &b2)
}

func TestBootstrapMarshalUnmarshalEmptyAddresses(t *testing.T) {
	b := Bootstrap{
		Addresses: map[uint64]string{},
		Join:      true,
		Type:      ConcurrentStateMachine,
	}
	buf := make([]byte, b.Size())
	b.MarshalTo(buf)
	var b2 Bootstrap
	b2.UnmarshalFrom(buf)
	assertBootstrapEqual(t, &b, &b2)
}

func TestBootstrapMarshalUnmarshalNilAddresses(t *testing.T) {
	b := Bootstrap{
		Type: RegularStateMachine,
	}
	buf := make([]byte, b.Size())
	b.MarshalTo(buf)
	var b2 Bootstrap
	_, err := b2.UnmarshalFrom(buf)
	if err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if len(b2.Addresses) != 0 {
		t.Fatalf("Addresses: got %d entries, want 0", len(b2.Addresses))
	}
	if b2.Join != false {
		t.Fatal("Join: got true, want false")
	}
}

func TestBootstrapBufferTooSmallMarshal(t *testing.T) {
	b := Bootstrap{Addresses: map[uint64]string{1: "x"}}
	buf := make([]byte, b.Size()-1)
	_, err := b.MarshalTo(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("MarshalTo: got %v, want ErrBufferTooSmall", err)
	}
}

func TestBootstrapBufferTooSmallUnmarshal(t *testing.T) {
	b := Bootstrap{
		Addresses: map[uint64]string{},
		Type:      RegularStateMachine,
	}
	buf := make([]byte, b.Size())
	b.MarshalTo(buf)
	// Truncate buffer to cut off Join and Type fields
	var b2 Bootstrap
	_, err := b2.UnmarshalFrom(buf[:4+2])
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("UnmarshalFrom: got %v, want ErrBufferTooSmall", err)
	}
}

func TestBootstrapReset(t *testing.T) {
	b := Bootstrap{
		Addresses: map[uint64]string{1: "a"},
		Join:      true,
		Type:      OnDiskStateMachine,
	}
	b.Reset()
	if b.Addresses != nil || b.Join || b.Type != 0 {
		t.Fatal("Reset did not clear all fields")
	}
}

func assertBootstrapEqual(t *testing.T, want, got *Bootstrap) {
	t.Helper()
	assertStringMapEqual(t, "Addresses", want.Addresses, got.Addresses)
	if got.Join != want.Join {
		t.Fatalf("Join: got %v, want %v", got.Join, want.Join)
	}
	if got.Type != want.Type {
		t.Fatalf("Type: got %d, want %d", got.Type, want.Type)
	}
}
