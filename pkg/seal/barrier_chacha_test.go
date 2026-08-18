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

//go:build !fips

package seal

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// The barrier's framing has two shapes, and after the migration to
// cipher.NewGCMWithRandomNonce the AES-GCM algorithms exercise only one of
// them: the module generates the nonce and encryptLocked writes none.
// The caller-nonce shape — encryptLocked drawing the nonce from b.randReader
// and writing it into the frame, decryptLocked slicing it back out — is
// reached by the ChaCha20 family and by any injected AEADProvider that returns
// a caller-nonce cipher.AEAD, so it is still live code and must still be
// tested. It cannot be tested in a FIPS build, which compiles no caller-nonce
// AEAD in, hence //go:build !fips.

// chachaBarrier returns an initialized barrier pinned to a caller-nonce AEAD.
func chachaBarrier(t *testing.T, alg SymmetricAlgorithm) (*Barrier, *SoftwareStrategy) {
	t.Helper()
	cfg := testBarrierConfig(1)
	cfg.Algorithm = alg
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize with %s failed: %v", alg, err)
	}
	return b, s
}

// TestBarrier_CallerNonceAEADRoundTrip pins the caller-nonce framing path end
// to end for both nonce widths the wire format supports — 12 bytes and the
// 24-byte extended nonce — including the positional context AAD.
func TestBarrier_CallerNonceAEADRoundTrip(t *testing.T) {
	for _, alg := range []SymmetricAlgorithm{AlgChaCha20Poly1305, AlgXChaCha20Poly1305} {
		t.Run(alg.String(), func(t *testing.T) {
			b, s := chachaBarrier(t, alg)
			defer func() { _ = s.Close() }()

			plaintext := []byte("caller-nonce framing round trip")
			frame, err := b.Encrypt(nil, plaintext)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}

			// The nonce is written into the frame by the barrier, so the frame
			// is header + nonce + ciphertext + tag with the ALGORITHM's nonce
			// width, not the module-nonce zero.
			want := epochSize + algIDSize + alg.NonceSize() + len(plaintext) + aeadTagSize
			if len(frame) != want {
				t.Fatalf("frame is %d bytes, want %d", len(frame), want)
			}
			if got := SymmetricAlgorithm(frame[epochSize]); got != alg {
				t.Fatalf("frame algID = %#x, want %#x", byte(got), byte(alg))
			}

			got, err := b.Decrypt(nil, frame)
			if err != nil {
				t.Fatalf("Decrypt: %v", err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
			}

			// Context AAD over the same framing.
			ctxAAD := []byte("wal/segment/000007")
			bound, err := b.EncryptWithContext(nil, plaintext, ctxAAD)
			if err != nil {
				t.Fatalf("EncryptWithContext: %v", err)
			}
			if _, err := b.DecryptWithContext(nil, bound, ctxAAD); err != nil {
				t.Fatalf("DecryptWithContext with matching context: %v", err)
			}
			if _, err := b.DecryptWithContext(nil, bound, []byte("wal/segment/000008")); err == nil {
				t.Fatal("DecryptWithContext opened a frame under the wrong context")
			}

			// Two seals must differ: the barrier's own nonce source is still
			// drawn per call on this path.
			other, err := b.Encrypt(nil, plaintext)
			if err != nil {
				t.Fatalf("second Encrypt: %v", err)
			}
			if bytes.Equal(frame, other) {
				t.Fatal("two seals of the same plaintext produced identical frames")
			}
		})
	}
}

// TestBarrier_CallerNonceEncryptRandFail pins that the caller-nonce path is
// still fail-closed on a randomness failure. The module-nonce path has no such
// dependency (TestBarrier_EncryptDoesNotDrawTheNonceFromRandReader asserts the
// opposite there), so this is the only place the behaviour still exists.
func TestBarrier_CallerNonceEncryptRandFail(t *testing.T) {
	b, s := chachaBarrier(t, AlgChaCha20Poly1305)
	defer func() { _ = s.Close() }()

	b.randReader = &errReader{remaining: 0, err: io.ErrUnexpectedEOF}

	if _, err := b.Encrypt(nil, []byte("test")); err == nil {
		t.Fatal("Encrypt succeeded with an exhausted random source; a " +
			"caller-supplied nonce must never fall back to a weaker one")
	}
}

// TestBarrier_CallerNonceRejectsMismatchedAlgID pins the algID cross-check on
// the caller-nonce path: a header byte naming a different nonce WIDTH than the
// active cipher would slice a wrong-length nonce and panic inside Open, so it
// must be refused with the generic error first.
func TestBarrier_CallerNonceRejectsMismatchedAlgID(t *testing.T) {
	b, s := chachaBarrier(t, AlgChaCha20Poly1305)
	defer func() { _ = s.Close() }()

	frame, err := b.Encrypt(nil, []byte("algID cross-check"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	// 24-byte nonce class against a 12-byte cipher.
	frame[epochSize] = byte(AlgXChaCha20Poly1305)
	if _, err := b.Decrypt(nil, frame); err == nil {
		t.Fatal("Decrypt accepted a frame whose algID names a different nonce width")
	}
}
