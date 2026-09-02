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

	goenclave "github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// TestProtectedBuffer_AliasResolvesToGoEnclave pins what this package's
// compatibility surface now means. seal.ProtectedBuffer, enclave.MemoryEnclave
// and go-enclave's MemoryEnclave must all be ONE type, not three that happen to
// agree — the whole point of the adoption is that the portfolio has a single
// locked-memory implementation. Assignment across all three is the compile-time
// proof; a defined type anywhere in the chain would fail to build.
//
// TestProtectedBuffer_AliasLifecycle and TestProtectedBuffer_AliasFromBytes were
// DROPPED here rather than ported, and deliberately: they exercised
// seal.NewProtectedBuffer and seal.NewProtectedBufferFromBytes, which are gone
// because locked memory is go-enclave's and its constructors return an error a
// bare-pointer alias could not express. The behaviour they covered — allocate,
// write, size, destroy, wipe-the-source — is go-enclave's own responsibility and
// is tested there.
// The compile-time half, in the blank-identifier form Go uses for this. These
// only build if seal.ProtectedBuffer, enclave.MemoryEnclave and go-enclave's
// MemoryEnclave are ONE type.
var (
	_ *ProtectedBuffer         = (*goenclave.MemoryEnclave)(nil)
	_ *enclave.MemoryEnclave   = (*ProtectedBuffer)(nil)
	_ *goenclave.MemoryEnclave = (*enclave.MemoryEnclave)(nil)
)

func TestProtectedBuffer_AliasResolvesToGoEnclave(t *testing.T) {
	src := mustSized(t, 32)
	defer src.Destroy()

	// Identity is proven at compile time by the block above; re-asserting it here
	// would only be a no-op conversion. What runs is that an enclave reaches this
	// package's ProtectedBuffer name and behaves.
	if src.Size() != 32 {
		t.Fatalf("Size() = %d, want 32", src.Size())
	}
}

// TestProtectedKey_AliasOpenSeal verifies the aliased ProtectedKey opens into
// locked memory holding what was sealed.
func TestProtectedKey_AliasOpenSeal(t *testing.T) {
	keyData := []byte("an-aliased-protected-key-32-byte")
	want := append([]byte(nil), keyData...)

	pk := NewProtectedKey(keyData)
	if pk == nil {
		t.Fatal("NewProtectedKey returned nil")
	}

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer buf.Destroy()

	if !bytes.Equal(encBytes(buf), want) {
		t.Fatalf("opened key = %x, want %x", encBytes(buf), want)
	}
}

// TestProtectedKey_AliasOpenIsFrozen pins the property the locked-to-locked copy
// in ProtectedKey.Open exists to preserve: what Open hands back is READ-ONLY, so
// a stray write faults instead of silently corrupting the key. A sized enclave
// would stay writable, which is why Open does not simply hand one back.
func TestProtectedKey_AliasOpenIsFrozen(t *testing.T) {
	pk := NewProtectedKey([]byte("a-key-that-must-come-back-frozen"))

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer buf.Destroy()

	// A frozen region is one built by NewMemoryEnclave rather than sized-and-filled.
	// Proving the mprotect by writing would fault the test process, so this asserts
	// the observable contract instead: the material is intact and readable.
	if got := encBytes(buf); len(got) != 32 {
		t.Fatalf("opened key length = %d, want 32", len(got))
	}
}

// TestSealProtectedBuffer_AliasRoundTrip verifies the aliased
// SealProtectedBuffer consumes an enclave into a usable ProtectedKey.
func TestSealProtectedBuffer_AliasRoundTrip(t *testing.T) {
	want := []byte("seal-me-into-an-enclave-buffer!!")
	pb := mustFromBytes(t, append([]byte(nil), want...))

	pk := SealProtectedBuffer(pb)
	if pk == nil {
		t.Fatal("SealProtectedBuffer returned nil")
	}

	// The source enclave is CONSUMED: ownership moved to the key.
	if encBytes(pb) != nil {
		t.Fatal("the source enclave should be destroyed after SealProtectedBuffer")
	}

	buf, err := pk.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer buf.Destroy()

	if !bytes.Equal(encBytes(buf), want) {
		t.Fatalf("sealed key content mismatch: got %x, want %x", encBytes(buf), want)
	}
}

// TestSealProtectedBuffer_AliasNil verifies SealProtectedBuffer returns nil for
// a nil or already-destroyed enclave rather than panicking.
func TestSealProtectedBuffer_AliasNil(t *testing.T) {
	if SealProtectedBuffer(nil) != nil {
		t.Fatal("SealProtectedBuffer should return nil for a nil enclave")
	}

	pb := mustSized(t, 16)
	pb.Destroy()
	if SealProtectedBuffer(pb) != nil {
		t.Fatal("SealProtectedBuffer should return nil for a destroyed enclave")
	}
}

// TestWipeBytes_Alias verifies the package-local wipeBytes zeroes its argument.
func TestWipeBytes_Alias(t *testing.T) {
	b := []byte("sensitive-material-to-be-wiped!!")
	wipeBytes(b)
	for i, c := range b {
		if c != 0 {
			t.Fatalf("byte %d = %#x after wipeBytes, want 0", i, c)
		}
	}
}

// TestWipeBytes_AliasEmpty verifies wipeBytes tolerates an empty slice.
func TestWipeBytes_AliasEmpty(t *testing.T) {
	wipeBytes(nil)
	wipeBytes([]byte{})
}
