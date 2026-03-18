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
	"crypto/rand"
	"errors"
	"testing"
)

func TestDeferredSoftwareStrategy_SealUnsealRoundTrip(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	creds := Credentials{Secret: "my-deferred-passphrase"}

	sealed, err := s.SealRootKey(context.Background(), rootKey, creds)
	if err != nil {
		t.Fatalf("SealRootKey failed: %v", err)
	}

	if sealed.Strategy != StrategySoftware {
		t.Fatalf("expected strategy=%q, got %q", StrategySoftware, sealed.Strategy)
	}
	if len(sealed.Metadata) != metadataKDFVersionLen {
		t.Fatalf("expected %d-byte metadata, got %d bytes", metadataKDFVersionLen, len(sealed.Metadata))
	}

	decrypted, err := s.UnsealRootKey(context.Background(), sealed, creds)
	if err != nil {
		t.Fatalf("UnsealRootKey failed: %v", err)
	}
	defer wipeBytes(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("round-trip failed: decrypted root key does not match original")
	}
}

func TestDeferredSoftwareStrategy_SealEmptyCreds(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	_, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if !errors.Is(err, ErrEmptyCredentials) {
		t.Fatalf("expected ErrEmptyCredentials, got %v", err)
	}
}

func TestDeferredSoftwareStrategy_UnsealEmptyCreds(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	creds := Credentials{Secret: "seal-passphrase"}
	sealed, err := s.SealRootKey(context.Background(), rootKey, creds)
	if err != nil {
		t.Fatalf("SealRootKey failed: %v", err)
	}

	_, err = s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if !errors.Is(err, ErrEmptyCredentials) {
		t.Fatalf("expected ErrEmptyCredentials, got %v", err)
	}
}

func TestDeferredSoftwareStrategy_WrongCreds(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	sealCreds := Credentials{Secret: "correct-passphrase"}
	sealed, err := s.SealRootKey(context.Background(), rootKey, sealCreds)
	if err != nil {
		t.Fatalf("SealRootKey failed: %v", err)
	}

	wrongCreds := Credentials{Secret: "wrong-passphrase"}
	_, err = s.UnsealRootKey(context.Background(), sealed, wrongCreds)
	if err == nil {
		t.Fatal("expected error for wrong credentials, got nil")
	}
	var unsealErr *UnsealError
	if !errors.As(err, &unsealErr) {
		t.Fatalf("expected *UnsealError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed in chain, got %v", err)
	}
}

func TestDeferredSoftwareStrategy_Properties(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	if s.ID() != StrategySoftware {
		t.Fatalf("expected ID=%q, got %q", StrategySoftware, s.ID())
	}
	if !s.Available() {
		t.Fatal("expected Available() to return true")
	}
	if s.HardwareBacked() {
		t.Fatal("expected HardwareBacked() to return false")
	}
	if s.passphrase != nil {
		t.Fatal("expected passphrase to be nil for deferred strategy")
	}
	if !s.deferPassphrase {
		t.Fatal("expected deferPassphrase to be true")
	}
}

func TestDeferredSoftwareStrategy_CloseIdempotent(t *testing.T) {
	s := NewDeferredSoftwareStrategy()

	if err := s.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestDeferredSoftwareStrategy_InvalidRootKeySize(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	creds := Credentials{Secret: "some-passphrase"}

	// Too short.
	_, err := s.SealRootKey(context.Background(), []byte("short"), creds)
	if !errors.Is(err, ErrInvalidRootKeySize) {
		t.Fatalf("expected ErrInvalidRootKeySize for short key, got %v", err)
	}

	// Too long.
	_, err = s.SealRootKey(context.Background(), make([]byte, 64), creds)
	if !errors.Is(err, ErrInvalidRootKeySize) {
		t.Fatalf("expected ErrInvalidRootKeySize for long key, got %v", err)
	}
}

func TestDeferredSoftwareStrategy_UnsealNilSealed(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	creds := Credentials{Secret: "some-passphrase"}
	_, err := s.UnsealRootKey(context.Background(), nil, creds)
	if err == nil {
		t.Fatal("expected error for nil sealed, got nil")
	}
	var unsealErr *UnsealError
	if !errors.As(err, &unsealErr) {
		t.Fatalf("expected *UnsealError, got %T: %v", err, err)
	}
}

func TestDeferredSoftwareStrategy_MultipleSealsDifferentCiphertexts(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	creds := Credentials{Secret: "deferred-multi-seal"}

	sealed1, err := s.SealRootKey(context.Background(), rootKey, creds)
	if err != nil {
		t.Fatalf("first SealRootKey failed: %v", err)
	}

	sealed2, err := s.SealRootKey(context.Background(), rootKey, creds)
	if err != nil {
		t.Fatalf("second SealRootKey failed: %v", err)
	}

	// Different salts should produce different ciphertexts.
	if bytes.Equal(sealed1.Ciphertext, sealed2.Ciphertext) {
		t.Fatal("two seals of the same key produced identical ciphertexts")
	}

	// Both should unseal to the same root key.
	dec1, err := s.UnsealRootKey(context.Background(), sealed1, creds)
	if err != nil {
		t.Fatalf("UnsealRootKey sealed1 failed: %v", err)
	}
	defer wipeBytes(dec1)

	dec2, err := s.UnsealRootKey(context.Background(), sealed2, creds)
	if err != nil {
		t.Fatalf("UnsealRootKey sealed2 failed: %v", err)
	}
	defer wipeBytes(dec2)

	if !bytes.Equal(dec1, dec2) {
		t.Fatal("two unseals produced different root keys")
	}
}

func TestDeferredSoftwareStrategy_BarrierEncryptor(t *testing.T) {
	s := NewDeferredSoftwareStrategy()
	defer s.Close()

	enc, err := s.BarrierEncryptor(context.Background(), make([]byte, 32))
	if enc != nil {
		t.Fatalf("expected nil SymmetricEncrypter, got %v", enc)
	}
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
