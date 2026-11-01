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
	"crypto/rand"
	"errors"
	"testing"
)

// Argon2id-specific KDF tests. They live behind //go:build !fips because a FIPS
// build has no Argon2id deriver compiled in at all (see kdf_argon2.go), so
// there is nothing here for it to exercise — the FIPS build's claim about
// Argon2id is that it is ABSENT, which assertArgon2idBaseline pins instead.

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

// TestKDFParamsBelowFloor_Argon2id pins the Argon2id work-factor floors, the
// half of TestKDFParamsBelowFloor that only exists where Argon2id does.
func TestKDFParamsBelowFloor_Argon2id(t *testing.T) {
	cases := []struct {
		name   string
		params KDFParams
		weak   bool
	}{
		{"default", DefaultKDFParams(), false},
		{"trivial memory", KDFParams{Time: 1, Memory: 1, Threads: 1, KeyLen: 32}, true},
		{"trivial time", KDFParams{Time: 1, Memory: 128 * 1024, Threads: 1, KeyLen: 32}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kdfParamsBelowFloor(KDFArgon2id, tc.params); got != tc.weak {
				t.Fatalf("kdfParamsBelowFloor = %v, want %v", got, tc.weak)
			}
		})
	}
}

// TestDeriveKeyRejectsWeakArgon2Params is TestDeriveKeyRejectsWeakParams for
// Argon2id: below-floor work factors must be refused there too, not only for
// the approved KDF.
func TestDeriveKeyRejectsWeakArgon2Params(t *testing.T) {
	prev := allowWeakKDFParams.Swap(false)
	defer allowWeakKDFParams.Store(prev)

	salt := make([]byte, 32)
	_, err := DeriveKey([]byte("passphrase"), salt, KDFArgon2id,
		KDFParams{Time: 1, Memory: 1, Threads: 1, KeyLen: 32})
	if !errors.Is(err, ErrKDFWeakParams) {
		t.Fatalf("DeriveKey(weak) = %v, want ErrKDFWeakParams", err)
	}
}
