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
	"context"
	"errors"
	"testing"
)

func TestNoneStrategy_Identity(t *testing.T) {
	s := NewNoneStrategy()

	if got := s.ID(); got != StrategyNone {
		t.Fatalf("ID() = %q, want %q", got, StrategyNone)
	}
	if !s.Available() {
		t.Fatal("Available() = false, want true")
	}
	if s.HardwareBacked() {
		t.Fatal("HardwareBacked() = true, want false")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() returned err: %v", err)
	}
}

// TestNoneStrategy_BarrierEncryptor_RoundTrip asserts that the no-encryption
// encrypter round-trips, never aliases the caller's buffer, and prepends
// the 8-byte sentinel-zero epoch prefix so the output is wire-format
// compatible with epoch-aware readers.
func TestNoneStrategy_BarrierEncryptor_RoundTrip(t *testing.T) {
	s := NewNoneStrategy()
	enc, err := s.BarrierEncryptor(context.Background(), nil)
	if err != nil {
		t.Fatalf("BarrierEncryptor: %v", err)
	}
	if enc == nil {
		t.Fatal("BarrierEncryptor returned nil; NoneStrategy must return an encrypter")
	}

	plaintext := []byte("the quick brown fox jumps over the lazy dog")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Leading 8 bytes are the (zero) epoch header that WAL/snapshot
	// readers parse; the payload follows.
	if len(ciphertext) != len(plaintext)+8 {
		t.Fatalf("ciphertext len = %d, want %d (8-byte epoch prefix + payload)", len(ciphertext), len(plaintext)+8)
	}
	for i := 0; i < 8; i++ {
		if ciphertext[i] != 0 {
			t.Fatalf("epoch header byte %d = %d, want 0 (sentinel)", i, ciphertext[i])
		}
	}

	// Encrypt must not alias the input. Mutating plaintext after Encrypt
	// must not change the produced ciphertext payload.
	cipherCopy := append([]byte(nil), ciphertext...)
	for i := range plaintext {
		plaintext[i] ^= 0xFF
	}
	if !bytes.Equal(ciphertext, cipherCopy) {
		t.Fatal("Encrypt aliased the input buffer; mutating plaintext changed ciphertext")
	}
	// Restore plaintext for the round-trip check.
	for i := range plaintext {
		plaintext[i] ^= 0xFF
	}

	recovered, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(recovered, plaintext) {
		t.Fatalf("Decrypt returned %q, want %q", recovered, plaintext)
	}

	// Decrypt must reject a ciphertext shorter than the epoch header.
	if _, err := enc.Decrypt([]byte{1, 2, 3}); err == nil {
		t.Fatal("Decrypt of too-short ciphertext should error")
	}
}

// TestNoneStrategy_BarrierRestartCycle pins that a barrier initialized with
// NoneStrategy unseals again after a simulated process restart (fresh Barrier
// + persisted marker + Unseal). An Unseal that rejected the empty root key with
// ErrInvalidRootKeySize would, after repeated restarts, trip the brute-force
// lockout.
func TestNoneStrategy_BarrierRestartCycle(t *testing.T) {
	ctx := context.Background()
	s := NewNoneStrategy()
	defer s.Close()

	b := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize(None): %v", err)
	}

	ct, err := b.Encrypt(nil, []byte("hello-none"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Persist the marker an operator would store, then simulate a restart.
	sealed := b.SealedRootKeyData()

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal(None) after restart failed: %v", err)
	}
	if b2.IsSealed() {
		t.Fatal("barrier should be unsealed after restart")
	}

	// The post-restart barrier must decrypt data written before the restart.
	pt, err := b2.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after restart: %v", err)
	}
	if string(pt) != "hello-none" {
		t.Fatalf("post-restart decrypt = %q, want %q", pt, "hello-none")
	}
}

// TestNoneStrategy_SealRootKey_MarkerOnly asserts that SealRootKey returns a
// marker SealedRootKey tagged with StrategyNone and never stores key material.
func TestNoneStrategy_SealRootKey_MarkerOnly(t *testing.T) {
	s := NewNoneStrategy()
	rootKey := make([]byte, 32)
	for i := range rootKey {
		rootKey[i] = byte(i)
	}

	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKey: %v", err)
	}
	if sealed == nil {
		t.Fatal("SealRootKey returned nil sealed key")
	}
	if sealed.Strategy != StrategyNone {
		t.Fatalf("sealed.Strategy = %q, want %q", sealed.Strategy, StrategyNone)
	}
	if len(sealed.Ciphertext) != 0 {
		t.Fatalf("sealed.Ciphertext should be empty for NoneStrategy, got %d bytes", len(sealed.Ciphertext))
	}
	if sealed.CreatedAt.IsZero() {
		t.Fatal("sealed.CreatedAt should be set")
	}
}

// TestNoneStrategy_UnsealRootKey asserts that UnsealRootKey returns an empty
// slice for a None marker (no key material is needed because the identity
// encrypter ignores keys) and refuses a nil or another strategy's sealed key.
func TestNoneStrategy_UnsealRootKey(t *testing.T) {
	s := NewNoneStrategy()
	for _, tc := range []struct {
		name    string
		sealed  *SealedRootKey
		wantErr error
	}{
		{"none marker yields an empty key", &SealedRootKey{Strategy: StrategyNone}, nil},
		{"nil sealed key", nil, ErrInvalidSealedKey},
		{"strategy mismatch", &SealedRootKey{Strategy: StrategyPassphrase}, ErrStrategyMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := s.UnsealRootKey(context.Background(), tc.sealed, Credentials{})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("UnsealRootKey err = %v, want %v", err, tc.wantErr)
			}
			if len(key) != 0 {
				t.Fatalf("UnsealRootKey returned %d bytes, want 0", len(key))
			}
		})
	}
}

// TestNoneEncrypterRejectsEncryptedData verifies the no-encryption
// decrypter fails closed on data carrying a non-zero epoch prefix
// (written by a real strategy) instead of returning garbage plaintext.
func TestNoneEncrypterRejectsEncryptedData(t *testing.T) {
	enc := noneEncrypter{}

	// Data with a non-zero epoch prefix is rejected.
	encrypted := make([]byte, noneEpochHeaderSize+4)
	encrypted[7] = 0x01 // epoch 1, big-endian
	copy(encrypted[noneEpochHeaderSize:], []byte("data"))
	if _, err := enc.Decrypt(encrypted); !errors.Is(err, ErrNoneStrategyEncryptedData) {
		t.Fatalf("Decrypt(epoch>0) = %v, want ErrNoneStrategyEncryptedData", err)
	}
}
