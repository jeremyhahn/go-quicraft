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
	"crypto/rand"
	"errors"
	"testing"
)

func TestDeriveKey_Argon2id(t *testing.T) {
	passphrase := []byte("test-passphrase-argon2id")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Time:    1,
		Memory:  64 * 1024,
		Threads: 1,
		KeyLen:  32,
		SaltLen: 32,
	}

	key, err := DeriveKey(passphrase, salt, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("DeriveKey(Argon2id) returned unexpected error: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("expected 32-byte key, got %d bytes", len(key))
	}
}

func TestDeriveKey_Argon2id_Deterministic(t *testing.T) {
	passphrase := []byte("deterministic-test")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Time:    1,
		Memory:  64 * 1024,
		Threads: 1,
		KeyLen:  32,
		SaltLen: 32,
	}

	key1, err := DeriveKey(passphrase, salt, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("first DeriveKey failed: %v", err)
	}

	key2, err := DeriveKey(passphrase, salt, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("second DeriveKey failed: %v", err)
	}

	if !bytes.Equal(key1, key2) {
		t.Fatal("same passphrase and salt should produce identical keys")
	}
}

func TestDeriveKey_PBKDF2SHA256(t *testing.T) {
	passphrase := []byte("test-passphrase-pbkdf2")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Iterations: 1000, // Low iteration count for fast tests.
		KeyLen:     32,
		SaltLen:    32,
	}

	key, err := DeriveKey(passphrase, salt, KDFPbkdf2SHA256, params)
	if err != nil {
		t.Fatalf("DeriveKey(PBKDF2) returned unexpected error: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("expected 32-byte key, got %d bytes", len(key))
	}
}

func TestDeriveKey_PBKDF2SHA256_Deterministic(t *testing.T) {
	passphrase := []byte("deterministic-test-pbkdf2")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Iterations: 1000,
		KeyLen:     32,
		SaltLen:    32,
	}

	key1, err := DeriveKey(passphrase, salt, KDFPbkdf2SHA256, params)
	if err != nil {
		t.Fatalf("first DeriveKey failed: %v", err)
	}

	key2, err := DeriveKey(passphrase, salt, KDFPbkdf2SHA256, params)
	if err != nil {
		t.Fatalf("second DeriveKey failed: %v", err)
	}

	if !bytes.Equal(key1, key2) {
		t.Fatal("same passphrase and salt should produce identical keys")
	}
}

func TestDeriveKey_CrossVersionDifferentResults(t *testing.T) {
	passphrase := []byte("cross-version-test")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	argon2Params := KDFParams{
		Time:    1,
		Memory:  64 * 1024,
		Threads: 1,
		KeyLen:  32,
		SaltLen: 32,
	}

	pbkdf2Params := KDFParams{
		Iterations: 1000,
		KeyLen:     32,
		SaltLen:    32,
	}

	keyArgon2, err := DeriveKey(passphrase, salt, KDFArgon2id, argon2Params)
	if err != nil {
		t.Fatalf("DeriveKey(Argon2id) failed: %v", err)
	}

	keyPBKDF2, err := DeriveKey(passphrase, salt, KDFPbkdf2SHA256, pbkdf2Params)
	if err != nil {
		t.Fatalf("DeriveKey(PBKDF2) failed: %v", err)
	}

	if bytes.Equal(keyArgon2, keyPBKDF2) {
		t.Fatal("different KDF versions with same input should produce different keys")
	}
}

func TestDeriveKey_DifferentSaltsDifferentKeys(t *testing.T) {
	passphrase := []byte("salt-test")
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	if _, err := rand.Read(salt1); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	if _, err := rand.Read(salt2); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Time:    1,
		Memory:  64 * 1024,
		Threads: 1,
		KeyLen:  32,
		SaltLen: 32,
	}

	key1, err := DeriveKey(passphrase, salt1, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("DeriveKey with salt1 failed: %v", err)
	}

	key2, err := DeriveKey(passphrase, salt2, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("DeriveKey with salt2 failed: %v", err)
	}

	if bytes.Equal(key1, key2) {
		t.Fatal("different salts should produce different keys")
	}
}

func TestDeriveKey_EmptyPassphrase(t *testing.T) {
	salt := make([]byte, 32)
	params := DefaultKDFParams()

	_, err := DeriveKey([]byte{}, salt, KDFArgon2id, params)
	if !errors.Is(err, ErrKDFInvalidPassphrase) {
		t.Fatalf("expected ErrKDFInvalidPassphrase, got %v", err)
	}
}

func TestDeriveKey_NilPassphrase(t *testing.T) {
	salt := make([]byte, 32)
	params := DefaultKDFParams()

	_, err := DeriveKey(nil, salt, KDFArgon2id, params)
	if !errors.Is(err, ErrKDFInvalidPassphrase) {
		t.Fatalf("expected ErrKDFInvalidPassphrase, got %v", err)
	}
}

func TestDeriveKey_NilSalt(t *testing.T) {
	passphrase := []byte("test")
	params := DefaultKDFParams()

	_, err := DeriveKey(passphrase, nil, KDFArgon2id, params)
	if !errors.Is(err, ErrKDFInvalidSalt) {
		t.Fatalf("expected ErrKDFInvalidSalt, got %v", err)
	}
}

func TestDeriveKey_ShortSalt(t *testing.T) {
	passphrase := []byte("test")
	salt := make([]byte, 15) // Below minimum 16 bytes.
	params := DefaultKDFParams()

	_, err := DeriveKey(passphrase, salt, KDFArgon2id, params)
	if !errors.Is(err, ErrKDFInvalidSalt) {
		t.Fatalf("expected ErrKDFInvalidSalt, got %v", err)
	}
}

func TestDeriveKey_MinimumSaltLength(t *testing.T) {
	passphrase := []byte("test")
	salt := make([]byte, 16) // Exactly the minimum.
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	params := KDFParams{
		Time:    1,
		Memory:  64 * 1024,
		Threads: 1,
		KeyLen:  32,
		SaltLen: 16,
	}

	key, err := DeriveKey(passphrase, salt, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("DeriveKey with 16-byte salt should succeed: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("expected 32-byte key, got %d bytes", len(key))
	}
}

func TestDeriveKey_InvalidVersion(t *testing.T) {
	passphrase := []byte("test")
	salt := make([]byte, 32)
	params := DefaultKDFParams()

	_, err := DeriveKey(passphrase, salt, KDFVersion(99), params)
	if !errors.Is(err, ErrKDFUnsupportedVersion) {
		t.Fatalf("expected ErrKDFUnsupportedVersion, got %v", err)
	}
}

func TestDeriveKey_InvalidVersionZero(t *testing.T) {
	passphrase := []byte("test")
	salt := make([]byte, 32)
	params := DefaultKDFParams()

	_, err := DeriveKey(passphrase, salt, KDFVersion(0), params)
	if !errors.Is(err, ErrKDFUnsupportedVersion) {
		t.Fatalf("expected ErrKDFUnsupportedVersion, got %v", err)
	}
}

func TestDefaultKDFParams_Values(t *testing.T) {
	params := DefaultKDFParams()
	if params.Time != 4 {
		t.Fatalf("expected Time=4, got %d", params.Time)
	}
	if params.Memory != 128*1024 {
		t.Fatalf("expected Memory=131072, got %d", params.Memory)
	}
	if params.Threads != 4 {
		t.Fatalf("expected Threads=4, got %d", params.Threads)
	}
	if params.KeyLen != 32 {
		t.Fatalf("expected KeyLen=32, got %d", params.KeyLen)
	}
	if params.SaltLen != 32 {
		t.Fatalf("expected SaltLen=32, got %d", params.SaltLen)
	}
}

func TestDefaultFIPSKDFParams_Values(t *testing.T) {
	params := DefaultFIPSKDFParams()
	if params.Iterations != 600_000 {
		t.Fatalf("expected Iterations=600000, got %d", params.Iterations)
	}
	if params.KeyLen != 32 {
		t.Fatalf("expected KeyLen=32, got %d", params.KeyLen)
	}
	if params.SaltLen != 32 {
		t.Fatalf("expected SaltLen=32, got %d", params.SaltLen)
	}
}

func TestDeriveKey_PBKDF2_CustomKeyLen(t *testing.T) {
	passphrase := []byte("custom-key-len")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Iterations: 1000,
		KeyLen:     64, // Non-standard key length.
		SaltLen:    32,
	}

	key, err := DeriveKey(passphrase, salt, KDFPbkdf2SHA256, params)
	if err != nil {
		t.Fatalf("DeriveKey with custom KeyLen failed: %v", err)
	}
	if len(key) != 64 {
		t.Fatalf("expected 64-byte key, got %d bytes", len(key))
	}
}

func TestDeriveKey_Argon2id_CustomKeyLen(t *testing.T) {
	passphrase := []byte("custom-key-len-argon2")
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	params := KDFParams{
		Time:    1,
		Memory:  64 * 1024,
		Threads: 1,
		KeyLen:  64, // Non-standard key length.
		SaltLen: 32,
	}

	key, err := DeriveKey(passphrase, salt, KDFArgon2id, params)
	if err != nil {
		t.Fatalf("DeriveKey with custom KeyLen failed: %v", err)
	}
	if len(key) != 64 {
		t.Fatalf("expected 64-byte key, got %d bytes", len(key))
	}
}
