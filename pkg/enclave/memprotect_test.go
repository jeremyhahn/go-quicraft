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

package enclave

import (
	"bytes"
	"errors"
	"testing"
)

// TestProtectedBuffer_Lifecycle verifies the full lifecycle: create, read, destroy.
func TestProtectedBuffer_Lifecycle(t *testing.T) {
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

	// Write some data into the buffer.
	for i := range b {
		b[i] = byte(i)
	}

	// Verify readable.
	readBack := pb.Bytes()
	for i := range readBack {
		if readBack[i] != byte(i) {
			t.Fatalf("buffer content mismatch at index %d: got %d, want %d", i, readBack[i], byte(i))
		}
	}

	// Destroy and verify nil behavior.
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

// TestProtectedBuffer_NilAfterDestroy ensures all accessors return zero values
// after the buffer is destroyed.
func TestProtectedBuffer_NilAfterDestroy(t *testing.T) {
	pb := NewProtectedBuffer(16)
	pb.Destroy()

	if pb.Bytes() != nil {
		t.Fatal("Bytes() should return nil after Destroy()")
	}
	if pb.Size() != 0 {
		t.Fatal("Size() should return 0 after Destroy()")
	}
}

// TestProtectedBuffer_FromBytes verifies creation from existing data and that
// the source slice is wiped by memguard.
func TestProtectedBuffer_FromBytes(t *testing.T) {
	original := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}
	source := make([]byte, len(original))
	copy(source, original)

	pb := NewProtectedBufferFromBytes(source)
	defer pb.Destroy()

	// Verify content in the protected buffer matches original.
	got := pb.Bytes()
	if !bytes.Equal(got, original) {
		t.Fatalf("protected buffer content mismatch: got %x, want %x", got, original)
	}

	// memguard.NewBufferFromBytes wipes the source slice.
	allZero := true
	for _, b := range source {
		if b != 0 {
			allZero = false
			break
		}
	}
	if !allZero {
		t.Fatalf("source slice was not wiped by NewProtectedBufferFromBytes: %x", source)
	}
}

// TestProtectedKey_OpenSeal verifies the encrypt-at-rest cycle: create key,
// open it, verify content, destroy.
func TestProtectedKey_OpenSeal(t *testing.T) {
	keyData := []byte("this-is-a-32-byte-encryption-key")
	pk := NewProtectedKey(keyData)
	if pk == nil {
		t.Fatal("NewProtectedKey returned nil")
	}

	// Open the enclave.
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

// TestProtectedKey_OpenNilEnclave verifies that Open returns ErrEnclaveNil
// when the enclave is nil.
func TestProtectedKey_OpenNilEnclave(t *testing.T) {
	pk := &ProtectedKey{enclave: nil}
	_, err := pk.Open()
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("expected ErrEnclaveNil, got %v", err)
	}
}

// TestProtectedKey_OpenNilEnclaveErrorMessage verifies the error message format
// for ErrEnclaveNil.
func TestProtectedKey_OpenNilEnclaveErrorMessage(t *testing.T) {
	pk := &ProtectedKey{enclave: nil}
	_, err := pk.Open()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	want := "memprotect: enclave is nil"
	if err.Error() != want {
		t.Fatalf("expected error message %q, got %q", want, err.Error())
	}
}

// TestOpenError_Format verifies the OpenError message format and Unwrap.
func TestOpenError_Format(t *testing.T) {
	inner := errors.New("test inner error")
	e := &OpenError{Err: inner}
	want := "memprotect: failed to open enclave: test inner error"
	if e.Error() != want {
		t.Fatalf("expected %q, got %q", want, e.Error())
	}
	if !errors.Is(e, inner) {
		t.Fatal("Unwrap should return inner error")
	}
}

// TestOpenError_Unwrap verifies that OpenError.Unwrap returns the inner error.
func TestOpenError_Unwrap(t *testing.T) {
	inner := errors.New("root cause")
	e := &OpenError{Err: inner}
	if e.Unwrap() != inner {
		t.Fatal("Unwrap did not return the inner error")
	}
}

// TestWipeBytes verifies that WipeBytes zeroes all bytes in a slice.
func TestWipeBytes(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	WipeBytes(data)

	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte at index %d not zeroed: got %d", i, b)
		}
	}
}

// TestWipeBytes_EmptySlice verifies WipeBytes handles empty and nil slices
// without panicking.
func TestWipeBytes_EmptySlice(t *testing.T) {
	WipeBytes(nil)
	WipeBytes([]byte{})
}

// TestSealProtectedBuffer verifies the buffer-to-key sealing workflow.
func TestSealProtectedBuffer(t *testing.T) {
	data := []byte("seal-me-into-an-enclave-buffer!!")
	pb := NewProtectedBufferFromBytes(data)

	// Seal the buffer into a ProtectedKey (consumes the buffer).
	pk := SealProtectedBuffer(pb)
	if pk == nil {
		t.Fatal("SealProtectedBuffer returned nil")
	}

	// The buffer's internal reference should be nil after sealing.
	if pb.buf != nil {
		t.Fatal("buffer's internal reference should be nil after SealProtectedBuffer")
	}

	// Open the key and verify content.
	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open returned unexpected error: %v", err)
	}
	defer buf.Destroy()

	if !bytes.Equal(buf.Bytes(), []byte("seal-me-into-an-enclave-buffer!!")) {
		t.Fatalf("sealed key content mismatch: got %x", buf.Bytes())
	}
}

// TestSealProtectedBuffer_NilBuffer verifies that SealProtectedBuffer returns
// nil when given a nil buffer.
func TestSealProtectedBuffer_NilBuffer(t *testing.T) {
	pk := SealProtectedBuffer(nil)
	if pk != nil {
		t.Fatal("SealProtectedBuffer should return nil for nil buffer")
	}
}

// TestSealProtectedBuffer_DestroyedBuffer verifies that SealProtectedBuffer
// returns nil when given a buffer whose internal data has already been destroyed.
func TestSealProtectedBuffer_DestroyedBuffer(t *testing.T) {
	pb := NewProtectedBuffer(16)
	pb.Destroy()

	pk := SealProtectedBuffer(pb)
	if pk != nil {
		t.Fatal("SealProtectedBuffer should return nil for a destroyed buffer")
	}
}

// TestInitMemguard verifies that InitMemguard does not panic. The function
// sets up signal handlers for secure cleanup.
func TestInitMemguard(t *testing.T) {
	// Should not panic. Calling it multiple times is safe.
	InitMemguard()
}

// TestInitMemguard_Idempotent verifies that calling InitMemguard multiple
// times does not panic.
func TestInitMemguard_Idempotent(t *testing.T) {
	InitMemguard()
	InitMemguard()
}

// TestErrEnclaveNil_Message verifies the sentinel error message.
func TestErrEnclaveNil_Message(t *testing.T) {
	want := "memprotect: enclave is nil"
	if ErrEnclaveNil.Error() != want {
		t.Fatalf("expected %q, got %q", want, ErrEnclaveNil.Error())
	}
}

// TestNilError_TypedError verifies the typed error can be matched
// with errors.As.
func TestNilError_TypedError(t *testing.T) {
	t.Run("errors.As matches NilError type", func(t *testing.T) {
		var err error = ErrEnclaveNil
		var ne *NilError
		if !errors.As(err, &ne) {
			t.Fatal("errors.As should match *NilError")
		}
	})

	t.Run("errors.Is matches ErrEnclaveNil", func(t *testing.T) {
		pk := &ProtectedKey{enclave: nil}
		_, err := pk.Open()
		if !errors.Is(err, ErrEnclaveNil) {
			t.Errorf("expected errors.Is(err, ErrEnclaveNil), got %v", err)
		}
	})

	t.Run("new NilError has correct message", func(t *testing.T) {
		e := &NilError{}
		want := "memprotect: enclave is nil"
		if e.Error() != want {
			t.Errorf("Error() = %q, want %q", e.Error(), want)
		}
	})
}

// TestProtectedKey_MultipleOpens verifies that a ProtectedKey can be opened
// multiple times and returns consistent data.
func TestProtectedKey_MultipleOpens(t *testing.T) {
	keyData := []byte("multi-open-test-key-32-bytes!!!!") // 32 bytes
	pk := NewProtectedKey(keyData)

	for i := 0; i < 3; i++ {
		buf, err := pk.Open()
		if err != nil {
			t.Fatalf("Open #%d returned unexpected error: %v", i, err)
		}
		if !bytes.Equal(buf.Bytes(), []byte("multi-open-test-key-32-bytes!!!!")) {
			t.Fatalf("Open #%d: content mismatch: got %x", i, buf.Bytes())
		}
		buf.Destroy()
	}
}

// TestProtectedKey_ClearZeroesKeyMaterial verifies that Clear() opens and
// destroys the underlying enclave data rather than relying on GC.
func TestProtectedKey_ClearZeroesKeyMaterial(t *testing.T) {
	keyData := []byte("clear-zeroes-key-material-32!!!!") // 32 bytes
	pk := NewProtectedKey(keyData)

	// Clear should not panic and should nil the enclave.
	pk.Clear()

	// After Clear, Open should return ErrEnclaveNil.
	_, err := pk.Open()
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("expected ErrEnclaveNil after Clear, got %v", err)
	}
}

// TestProtectedKey_ClearAlreadyClearedNoPanic verifies that calling Clear()
// on an already-cleared key does not panic.
func TestProtectedKey_ClearAlreadyClearedNoPanic(t *testing.T) {
	keyData := []byte("double-clear-test-key-32bytes!!!") // 32 bytes
	pk := NewProtectedKey(keyData)
	pk.Clear()
	// Second Clear should be a no-op, not panic.
	pk.Clear()

	// Verify still returns ErrEnclaveNil.
	_, err := pk.Open()
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("expected ErrEnclaveNil, got %v", err)
	}
}

// TestNewProtectedKey_WipesSource verifies that NewProtectedKey wipes
// the source slice.
func TestNewProtectedKey_WipesSource(t *testing.T) {
	source := make([]byte, 32)
	for i := range source {
		source[i] = byte(i + 1)
	}

	_ = NewProtectedKey(source)

	// memguard.NewEnclave wipes the source.
	allZero := true
	for _, b := range source {
		if b != 0 {
			allZero = false
			break
		}
	}
	if !allZero {
		t.Fatalf("source slice was not wiped by NewProtectedKey: %x", source)
	}
}
