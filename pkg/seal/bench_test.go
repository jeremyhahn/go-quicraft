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
	"context"
	"crypto/rand"
	"testing"
)

// benchBarrier creates a barrier initialized with fast Argon2id params.
func benchBarrier(b *testing.B) *Barrier {
	b.Helper()
	barrier := NewBarrier(DefaultBarrierConfig(1))
	s, err := NewSoftwareStrategyWithParams([]byte("bench-passphrase"), 1, 64*1024, 1)
	if err != nil {
		b.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	if err := barrier.Initialize(context.Background(), s, Credentials{}); err != nil {
		b.Fatalf("Initialize failed: %v", err)
	}
	b.Cleanup(func() {
		barrier.Seal()
		s.Close()
	})
	return barrier
}

func BenchmarkBarrier_Encrypt_64B(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 64)
	rand.Read(plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		_, _ = barrier.Encrypt(nil, plaintext)
	}
}

func BenchmarkBarrier_Encrypt_1KB(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 1024)
	rand.Read(plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(1024)
	for b.Loop() {
		_, _ = barrier.Encrypt(nil, plaintext)
	}
}

func BenchmarkBarrier_Encrypt_64KB(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 64*1024)
	rand.Read(plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(64 * 1024)
	for b.Loop() {
		_, _ = barrier.Encrypt(nil, plaintext)
	}
}

func BenchmarkBarrier_Decrypt_64B(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 64)
	rand.Read(plaintext)
	ciphertext, _ := barrier.Encrypt(nil, plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		_, _ = barrier.Decrypt(nil, ciphertext)
	}
}

func BenchmarkBarrier_Decrypt_1KB(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 1024)
	rand.Read(plaintext)
	ciphertext, _ := barrier.Encrypt(nil, plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(1024)
	for b.Loop() {
		_, _ = barrier.Decrypt(nil, ciphertext)
	}
}

func BenchmarkBarrier_Decrypt_64KB(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 64*1024)
	rand.Read(plaintext)
	ciphertext, _ := barrier.Encrypt(nil, plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(64 * 1024)
	for b.Loop() {
		_, _ = barrier.Decrypt(nil, ciphertext)
	}
}

func BenchmarkBarrier_EncryptDecrypt_RoundTrip_64B(b *testing.B) {
	barrier := benchBarrier(b)
	plaintext := make([]byte, 64)
	rand.Read(plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(64)
	for b.Loop() {
		ct, _ := barrier.Encrypt(nil, plaintext)
		_, _ = barrier.Decrypt(nil, ct)
	}
}

func BenchmarkBarrier_Rotate(b *testing.B) {
	barrier := benchBarrier(b)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = barrier.Rotate()
	}
}

func BenchmarkBarrier_IsSealed(b *testing.B) {
	barrier := benchBarrier(b)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = barrier.IsSealed()
	}
}

func BenchmarkSoftwareStrategy_SealUnseal(b *testing.B) {
	s, err := NewSoftwareStrategyWithParams([]byte("bench"), 1, 64*1024, 1)
	if err != nil {
		b.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	rand.Read(rootKey)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		sealed, _ := s.SealRootKey(context.Background(), rootKey, Credentials{})
		_, _ = s.UnsealRootKey(context.Background(), sealed, Credentials{})
	}
}

func BenchmarkDeriveDEK(b *testing.B) {
	rootKey := make([]byte, 32)
	salt := make([]byte, 32)
	rand.Read(rootKey)
	rand.Read(salt)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = deriveDEK(rootKey, salt, 42, 1, 32)
	}
}
