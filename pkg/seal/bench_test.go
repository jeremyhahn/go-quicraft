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
	"io"
	"log/slog"
	"testing"
)

// benchBarrier creates a barrier initialized with fast Argon2id params.
//
// Logging is silenced for the duration. This is not cosmetic: go test
// republishes the test binary's stderr on its own stdout, so a barrier's setup
// logs land BETWEEN a benchmark's name and its result line and corrupt the
// machine-readable output that every A/B comparison of this hot path depends
// on. Nothing on the measured path logs, so no measurement is affected.
func benchBarrier(b *testing.B) *Barrier {
	b.Helper()
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(prevLogger) })

	barrier := MustNewBarrier(DefaultBarrierConfig(1))
	s, err := newTestStrategy([]byte("bench-passphrase"))
	if err != nil {
		b.Fatalf("newTestStrategy failed: %v", err)
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
		_, _ = barrier.Rotate(context.Background())
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
	s, err := newTestStrategy([]byte("bench"))
	if err != nil {
		b.Fatalf("newTestStrategy failed: %v", err)
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
		_, _ = deriveDEK(rootKey, salt, 42, 1, AlgAES256GCM, HKDFV2)
	}
}
