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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
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
	pk := &ProtectedKey{}
	_, err := pk.Open()
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("expected ErrEnclaveNil, got %v", err)
	}
}

// TestProtectedKey_OpenNilEnclaveErrorMessage verifies the error message format
// for ErrEnclaveNil.
func TestProtectedKey_OpenNilEnclaveErrorMessage(t *testing.T) {
	pk := &ProtectedKey{}
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
	if pb.buf.Load() != nil {
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

// TestNoSignalHandlerInstalled is the regression for the removed InitMemguard.
// That function called memguard.CatchSignal, which calls signal.Reset() with no
// arguments — detaching EVERY handler in the process, including the embedding
// application's — and turns SIGTERM into an immediate core.Exit(1) with no
// engine quiesce, no barrier seal and no WAL flush. It had no callers, and this
// package must not acquire one: a storage library silently seizing the host
// process's signal disposition at import time is not a decision it gets to make.
//
// The test pins the absence behaviourally rather than by grep: it installs its
// own SIGUSR1 handler, exercises this package's whole key lifecycle, and
// requires the handler to still fire afterwards. Anything that reintroduces a
// CatchSignal call anywhere on these paths resets that handler and the delivery
// is lost.
func TestNoSignalHandlerInstalled(t *testing.T) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGUSR1)
	defer signal.Stop(sigC)

	key := []byte("signal-disposition-must-survive!!")
	pk := NewProtectedKey(key)
	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf.Destroy()
	pk.Clear()

	pb := NewProtectedBuffer(32)
	pb.Destroy()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("raising SIGUSR1: %v", err)
	}
	select {
	case got := <-sigC:
		if got != syscall.SIGUSR1 {
			t.Fatalf("received %v, want SIGUSR1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SIGUSR1 was never delivered: something in pkg/enclave reset the process signal handlers " +
			"(memguard.CatchSignal calls signal.Reset() with no arguments)")
	}
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
		pk := &ProtectedKey{}
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

// TestProtectedKey_ClearDropsEnclaveReference verifies that Clear() makes
// the key unopenable. Clear deliberately does NOT open the enclave to
// destroy a decrypted copy (that would briefly re-materialize the plaintext
// without zeroing the enclave's own ciphertext); it only drops the
// reference — see the L16 fix.
func TestProtectedKey_ClearDropsEnclaveReference(t *testing.T) {
	keyData := []byte("clear-zeroes-key-material-32!!!!") // 32 bytes
	pk := NewProtectedKey(keyData)

	// Clear should not panic and should drop the enclave reference.
	pk.Clear()

	// After Clear, Open should return ErrEnclaveNil.
	_, err := pk.Open()
	if !errors.Is(err, ErrEnclaveNil) {
		t.Fatalf("expected ErrEnclaveNil after Clear, got %v", err)
	}
}

// TestProtectedKey_ConcurrentOpenClear is the L16 regression test for the
// unsynchronized enclave field: Clear used to write p.enclave while Open
// read it, a data race that could tear under the race detector. With the
// atomic.Pointer field, concurrent Open and Clear are safe: every Open
// either succeeds against a still-live reference or fails with
// ErrEnclaveNil, and the race detector stays quiet.
func TestProtectedKey_ConcurrentOpenClear(t *testing.T) {
	keyData := []byte("concurrent-open-clear-key-32!!!!") // 32 bytes
	pk := NewProtectedKey(keyData)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				buf, err := pk.Open()
				if err != nil {
					if !errors.Is(err, ErrEnclaveNil) {
						t.Errorf("Open error = %v, want ErrEnclaveNil or success", err)
					}
					return
				}
				buf.Destroy()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		pk.Clear()
	}()
	close(start)
	wg.Wait()

	if _, err := pk.Open(); !errors.Is(err, ErrEnclaveNil) {
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

// TestProtectedBuffer_ConcurrentDestroy is the regression test for the plain
// `buf` field, whose write in Destroy raced every other Destroy. The reference
// now lives in an atomic.Pointer, so exactly one caller wins the swap and
// reaches memguard; the losers are no-ops.
//
// Note the deliberate absence of concurrent Bytes/Size here: memguard's own
// LockedBuffer is not safe for concurrent Destroy/read, so the CONTENTS stay
// single-owner (see the ProtectedBuffer doc comment). The atomic reference
// fixes this type's own field race only.
func TestProtectedBuffer_ConcurrentDestroy(t *testing.T) {
	pb := NewProtectedBuffer(32)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pb.Destroy()
		}()
	}
	wg.Wait()

	if pb.Size() != 0 {
		t.Fatal("Size should be 0 after Destroy")
	}
	if pb.Bytes() != nil {
		t.Fatal("Bytes should be nil after Destroy")
	}
}

// TestSealProtectedBuffer_ConsumedBufferReturnsNil verifies the documented
// single-consumption contract: sealing an already-consumed buffer yields nil
// rather than a second key over the same (now unowned) memory.
func TestSealProtectedBuffer_ConsumedBufferReturnsNil(t *testing.T) {
	pb := NewProtectedBufferFromBytes([]byte("consume-me-once-32-bytes-long!!!"))

	if pk := SealProtectedBuffer(pb); pk == nil {
		t.Fatal("first SealProtectedBuffer returned nil")
	}
	if pk := SealProtectedBuffer(pb); pk != nil {
		t.Fatal("second SealProtectedBuffer should return nil for a consumed buffer")
	}
}

// TestMemoryEnclave_WriteInPlaceThenFreeze covers the allocate → write in
// place → freeze pattern that lets a producer build sensitive output inside
// locked memory instead of assembling it on the ordinary heap and copying it
// in (see shamir.SplitProtected). A fresh enclave must be writable, and frozen
// must mean frozen.
func TestMemoryEnclave_WriteInPlaceThenFreeze(t *testing.T) {
	e := NewMemoryEnclave(8)
	if e == nil {
		t.Fatal("NewMemoryEnclave returned nil")
	}
	defer e.Destroy()

	if !e.buf.Load().IsMutable() {
		t.Fatal("a freshly allocated enclave must be writable, or nothing can be produced in place")
	}
	copy(e.Bytes(), []byte("in-place"))
	if got := string(e.Bytes()); got != "in-place" {
		t.Fatalf("enclave contents = %q, want %q", got, "in-place")
	}

	e.Freeze()
	if e.buf.Load().IsMutable() {
		t.Fatal("enclave is still mutable after Freeze")
	}
	if got := string(e.Bytes()); got != "in-place" {
		t.Fatalf("Freeze altered the contents: %q", got)
	}

	// Destroy must still wipe and release a frozen enclave — memguard melts
	// the pages before wiping them — or freezing would be a memory leak.
	e.Destroy()
	if e.Bytes() != nil || e.Size() != 0 {
		t.Fatal("a frozen enclave survived Destroy")
	}
}

// TestMemoryEnclave_FreezeIsSafeOnDestroyed verifies Freeze is a no-op rather
// than a panic once the enclave is gone: Destroy is single-shot and idempotent,
// and Freeze must not be the one method that breaks that.
func TestMemoryEnclave_FreezeIsSafeOnDestroyed(t *testing.T) {
	e := NewMemoryEnclave(16)
	e.Destroy()
	e.Freeze()
	e.Freeze()
}

// TestMemoryEnclave_FromBytesWipesSourceAndFreezes verifies the constructor's
// two contracts at once.
func TestMemoryEnclave_FromBytesWipesSourceAndFreezes(t *testing.T) {
	source := []byte("root key material, 32 bytes long")
	want := append([]byte(nil), source...)

	e := NewMemoryEnclaveFromBytes(source)
	defer e.Destroy()

	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("enclave contents = %q, want %q", e.Bytes(), want)
	}
	for _, b := range source {
		if b != 0 {
			t.Fatalf("source slice was not wiped by NewMemoryEnclaveFromBytes: %x", source)
		}
	}
	if e.buf.Load().IsMutable() {
		t.Error("NewMemoryEnclaveFromBytes must return a frozen enclave")
	}
}

// TestProtectedBufferAliasesMemoryEnclave is the compatibility guard for the
// rename. ProtectedBuffer is an ALIAS, not a distinct type, so a consumer
// compiled against either name keeps working and the two are interchangeable
// in every signature — which is what makes the rename non-breaking ahead of
// the coordinated wave. If someone later turns the alias into a defined type,
// this stops compiling.
//
// The three //nolint:staticcheck directives below are the point of the test,
// not noise around it. QF1011 reports each explicit type as redundant because
// it is inferable from the right-hand side — which is TRUE, and is precisely
// the property being asserted: it is inferable only while the two names denote
// one type. Letting staticcheck delete the annotations would delete the guard
// and leave three assignments that check nothing.
func TestProtectedBufferAliasesMemoryEnclave(t *testing.T) {
	var e *MemoryEnclave = NewProtectedBuffer(16) //nolint:staticcheck // QF1011: the explicit type IS the assertion
	defer e.Destroy()
	var pb *ProtectedBuffer = e //nolint:staticcheck // QF1011: the explicit type IS the assertion
	if pb.Size() != 16 {
		t.Fatalf("expected size=16 through the deprecated alias, got %d", pb.Size())
	}

	sealed := NewProtectedBufferFromBytes([]byte("sealed through the old name!!!!!"))
	var alias *ProtectedBuffer = sealed //nolint:staticcheck // QF1011: the explicit type IS the assertion
	if key := SealMemoryEnclave(alias); key == nil {
		t.Fatal("SealMemoryEnclave rejected a buffer built by the deprecated constructor")
	}
}

// TestSealMemoryEnclaveAndDeprecatedAliasAgree verifies the deprecated wrapper
// is the same function, not a diverging copy.
func TestSealMemoryEnclaveAndDeprecatedAliasAgree(t *testing.T) {
	for name, seal := range map[string]func(*MemoryEnclave) *ProtectedKey{
		"SealMemoryEnclave":   SealMemoryEnclave,
		"SealProtectedBuffer": SealProtectedBuffer,
	} {
		key := seal(NewMemoryEnclaveFromBytes([]byte("thirty-two bytes of key material")))
		if key == nil {
			t.Fatalf("%s returned nil for a live enclave", name)
		}
		opened, err := key.Open()
		if err != nil {
			t.Fatalf("%s: Open returned unexpected error: %v", name, err)
		}
		if got := string(opened.Bytes()); got != "thirty-two bytes of key material" {
			t.Errorf("%s: opened %q, want the sealed material", name, got)
		}
		opened.Destroy()
		if seal(nil) != nil {
			t.Errorf("%s(nil) must return nil", name)
		}
	}
}
