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

package seal

import (
	"bytes"
	"testing"
)

// TestProtectedBuffer_AliasLifecycle verifies that the seal package type aliases
// for ProtectedBuffer work correctly through the full lifecycle.
func TestProtectedBuffer_AliasLifecycle(t *testing.T) {
	pb := NewProtectedBuffer(32)
	if pb == nil {
		t.Fatal("NewProtectedBuffer returned nil")
	}

	if pb.Size() != 32 {
		t.Fatalf("expected size=32, got %d", pb.Size())
	}

	b := pb.Bytes()
	if len(b) != 32 {
		t.Fatalf("expected Bytes() length=32, got %d", len(b))
	}

	pb.Destroy()
	if pb.Bytes() != nil {
		t.Fatal("Bytes() should return nil after Destroy()")
	}
	if pb.Size() != 0 {
		t.Fatalf("Size() should return 0 after Destroy(), got %d", pb.Size())
	}

	// Double destroy should not panic.
	pb.Destroy()
}

// TestProtectedBuffer_AliasFromBytes verifies the aliased NewProtectedBufferFromBytes
// creates a protected buffer with the correct content.
func TestProtectedBuffer_AliasFromBytes(t *testing.T) {
	original := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}
	source := make([]byte, len(original))
	copy(source, original)

	pb := NewProtectedBufferFromBytes(source)
	defer pb.Destroy()

	got := pb.Bytes()
	if !bytes.Equal(got, original) {
		t.Fatalf("protected buffer content mismatch: got %x, want %x", got, original)
	}
}

// TestProtectedKey_AliasOpenSeal verifies the aliased ProtectedKey works
// through the full encrypt-at-rest cycle.
func TestProtectedKey_AliasOpenSeal(t *testing.T) {
	keyData := []byte("this-is-a-32-byte-encryption-key")
	pk := NewProtectedKey(keyData)
	if pk == nil {
		t.Fatal("NewProtectedKey returned nil")
	}

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open returned unexpected error: %v", err)
	}
	defer buf.Destroy()

	got := buf.Bytes()
	if !bytes.Equal(got, []byte("this-is-a-32-byte-encryption-key")) {
		t.Fatalf("opened key content mismatch: got %x", got)
	}
}

// TestSealProtectedBuffer_AliasRoundTrip verifies the aliased
// SealProtectedBuffer creates a usable ProtectedKey.
func TestSealProtectedBuffer_AliasRoundTrip(t *testing.T) {
	data := []byte("seal-me-into-an-enclave-buffer!!")
	pb := NewProtectedBufferFromBytes(data)

	pk := SealProtectedBuffer(pb)
	if pk == nil {
		t.Fatal("SealProtectedBuffer returned nil")
	}

	// After sealing, the buffer should report nil Bytes and zero Size.
	if pb.Bytes() != nil {
		t.Fatal("buffer Bytes() should return nil after SealProtectedBuffer")
	}
	if pb.Size() != 0 {
		t.Fatal("buffer Size() should return 0 after SealProtectedBuffer")
	}

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open returned unexpected error: %v", err)
	}
	defer buf.Destroy()

	if !bytes.Equal(buf.Bytes(), []byte("seal-me-into-an-enclave-buffer!!")) {
		t.Fatalf("sealed key content mismatch: got %x", buf.Bytes())
	}
}

// TestSealProtectedBuffer_AliasNil verifies SealProtectedBuffer returns nil
// for nil and destroyed buffers.
func TestSealProtectedBuffer_AliasNil(t *testing.T) {
	if SealProtectedBuffer(nil) != nil {
		t.Fatal("SealProtectedBuffer should return nil for nil buffer")
	}

	pb := NewProtectedBuffer(16)
	pb.Destroy()
	if SealProtectedBuffer(pb) != nil {
		t.Fatal("SealProtectedBuffer should return nil for a destroyed buffer")
	}
}

// TestWipeBytes_Alias verifies that the wipeBytes wrapper zeros all bytes.
func TestWipeBytes_Alias(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	wipeBytes(data)

	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte at index %d not zeroed: got %d", i, b)
		}
	}
}

// TestWipeBytes_AliasEmpty verifies wipeBytes handles empty and nil slices
// without panicking.
func TestWipeBytes_AliasEmpty(t *testing.T) {
	wipeBytes(nil)
	wipeBytes([]byte{})
}
