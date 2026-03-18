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
	"time"
)

// --- mockHWEncryptor ---

// mockHWEncryptor implements SymmetricEncrypter for testing hardware mode.
type mockHWEncryptor struct {
	encryptErr error
	decryptErr error
	prefix     []byte // prepended to ciphertext for identification
}

func (m *mockHWEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	if m.encryptErr != nil {
		return nil, m.encryptErr
	}
	out := make([]byte, len(m.prefix)+len(plaintext))
	copy(out, m.prefix)
	copy(out[len(m.prefix):], plaintext)
	return out, nil
}

func (m *mockHWEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if m.decryptErr != nil {
		return nil, m.decryptErr
	}
	if len(ciphertext) < len(m.prefix) {
		return nil, ErrDecryptionFailed
	}
	return ciphertext[len(m.prefix):], nil
}

// --- hwStrategy ---

// hwStrategy is a sealing strategy that provides a hardware encryptor.
type hwStrategy struct {
	hwEnc    SymmetricEncrypter
	hwErr    error
	sealErr  error
	rootKey  []byte
	metadata []byte
}

func (h *hwStrategy) ID() StrategyID       { return "test-hw" }
func (h *hwStrategy) Available() bool      { return true }
func (h *hwStrategy) HardwareBacked() bool { return true }

func (h *hwStrategy) SealRootKey(_ context.Context, rootKey []byte, _ Credentials) (*SealedRootKey, error) {
	if h.sealErr != nil {
		return nil, h.sealErr
	}
	h.rootKey = make([]byte, len(rootKey))
	copy(h.rootKey, rootKey)
	return &SealedRootKey{
		Strategy:        "test-hw",
		Ciphertext:      rootKey,
		HardwarePayload: h.metadata,
	}, nil
}

func (h *hwStrategy) UnsealRootKey(_ context.Context, sealed *SealedRootKey, _ Credentials) ([]byte, error) {
	out := make([]byte, len(sealed.Ciphertext))
	copy(out, sealed.Ciphertext)
	return out, nil
}

func (h *hwStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	if h.hwErr != nil {
		return nil, h.hwErr
	}
	return h.hwEnc, nil
}

func (h *hwStrategy) Close() error { return nil }

// --- Initialize hardware mode ---

func TestBarrier_Initialize_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize (hw) failed: %v", err)
	}

	if !b.IsHardwareMode() {
		t.Fatal("expected hardware mode after Initialize with hw encryptor")
	}
	if b.IsSealed() {
		t.Fatal("barrier should be unsealed after Initialize")
	}
	if b.CurrentEpoch() != 1 {
		t.Fatalf("expected epoch=1, got %d", b.CurrentEpoch())
	}

	// Cipher and currentDEK should NOT be set in hardware mode.
	b.mu.RLock()
	hasCipher := b.cipher != nil
	hasDEK := b.currentDEK != nil
	b.mu.RUnlock()

	if hasCipher {
		t.Fatal("cipher should be nil in hardware mode")
	}
	if hasDEK {
		t.Fatal("currentDEK should be nil in hardware mode")
	}
}

func TestBarrier_Initialize_HardwareMode_EncryptorError(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	hwErr := errors.New("hw init failed")
	s := &hwStrategy{hwEnc: nil, hwErr: hwErr}

	err := b.Initialize(context.Background(), s, Credentials{})
	if !errors.Is(err, hwErr) {
		t.Fatalf("expected hw init error, got %v", err)
	}

	if b.IsInitialized() {
		t.Fatal("barrier should not be initialized after hw error")
	}
}

// --- Unseal hardware mode ---

func TestBarrier_Unseal_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// After seal, hardware mode should be cleared.
	if b.IsHardwareMode() {
		t.Fatal("hardware mode should be cleared after Seal")
	}

	// Unseal with hardware strategy.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal (hw) failed: %v", err)
	}

	if !b.IsHardwareMode() {
		t.Fatal("expected hardware mode after Unseal with hw encryptor")
	}
	if b.IsSealed() {
		t.Fatal("barrier should be unsealed after Unseal")
	}
}

func TestBarrier_Unseal_HardwareMode_EncryptorError(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	// First, initialize with a software strategy.
	swStrategy := testStrategy(t)
	defer swStrategy.Close()

	if err := b.Initialize(context.Background(), swStrategy, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Now try to unseal with a hw strategy that errors on BarrierEncryptor.
	hwErr := errors.New("hw unseal init failed")
	failHW := &hwStrategy{hwEnc: nil, hwErr: hwErr}
	// We need a strategy that can unseal the key but then fails on BarrierEncryptor.
	// Create a wrapper that uses swStrategy for unseal but failHW for BarrierEncryptor.
	wrapped := &hwUnsealErrorStrategy{
		unsealStrategy: swStrategy,
		hwErr:          hwErr,
	}

	err := b.Unseal(context.Background(), wrapped, Credentials{})
	if !errors.Is(err, hwErr) {
		t.Fatalf("expected hw error, got %v", err)
	}
	_ = failHW // satisfy usage
}

// hwUnsealErrorStrategy unseals successfully but then returns an error from BarrierEncryptor.
type hwUnsealErrorStrategy struct {
	unsealStrategy SealingStrategy
	hwErr          error
}

func (h *hwUnsealErrorStrategy) ID() StrategyID       { return "test-hw-err" }
func (h *hwUnsealErrorStrategy) Available() bool      { return true }
func (h *hwUnsealErrorStrategy) HardwareBacked() bool { return true }

func (h *hwUnsealErrorStrategy) SealRootKey(ctx context.Context, rootKey []byte, creds Credentials) (*SealedRootKey, error) {
	return h.unsealStrategy.SealRootKey(ctx, rootKey, creds)
}

func (h *hwUnsealErrorStrategy) UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	return h.unsealStrategy.UnsealRootKey(ctx, sealed, creds)
}

func (h *hwUnsealErrorStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return nil, h.hwErr
}

func (h *hwUnsealErrorStrategy) Close() error { return nil }

// --- Encrypt/Decrypt in hardware mode ---

func TestBarrier_Encrypt_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	plaintext := []byte("hardware encrypted data")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt (hw) failed: %v", err)
	}

	// Verify the hardware encryptor was used (prefix present).
	if !bytes.HasPrefix(ct, []byte("HW:")) {
		t.Fatal("expected HW: prefix from hardware encryptor")
	}

	// Decrypt should round-trip.
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt (hw) failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatalf("round-trip mismatch: got %q, want %q", dec, plaintext)
	}
}

func TestBarrier_Encrypt_HardwareMode_EncryptError(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	encErr := errors.New("encrypt failed")
	enc := &mockHWEncryptor{encryptErr: encErr}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Encrypt(nil, []byte("test"))
	if !errors.Is(err, encErr) {
		t.Fatalf("expected encrypt error, got %v", err)
	}
}

func TestBarrier_Decrypt_HardwareMode_DecryptError(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	decErr := errors.New("decrypt failed")
	enc := &mockHWEncryptor{decryptErr: decErr}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Decrypt(nil, []byte("some ciphertext"))
	if !errors.Is(err, decErr) {
		t.Fatalf("expected decrypt error, got %v", err)
	}
}

// --- EncryptWithEpoch in hardware mode ---

func TestBarrier_EncryptWithEpoch_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	plaintext := []byte("epoch hw test")
	ct, epoch, err := b.EncryptWithEpoch(nil, plaintext)
	if err != nil {
		t.Fatalf("EncryptWithEpoch (hw) failed: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("expected epoch=1, got %d", epoch)
	}
	if !bytes.HasPrefix(ct, []byte("HW:")) {
		t.Fatal("expected HW: prefix from hardware encryptor")
	}
}

func TestBarrier_EncryptWithEpoch_HardwareMode_Error(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	encErr := errors.New("hw encrypt failed")
	enc := &mockHWEncryptor{encryptErr: encErr}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, _, err := b.EncryptWithEpoch(nil, []byte("test"))
	if !errors.Is(err, encErr) {
		t.Fatalf("expected encrypt error, got %v", err)
	}
}

// --- DecryptForEpoch in hardware mode ---

func TestBarrier_DecryptForEpoch_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	plaintext := []byte("epoch hw decrypt test")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// DecryptForEpoch should ignore the epoch parameter in hardware mode
	// and delegate directly to the hardware encryptor.
	dec, err := b.DecryptForEpoch(nil, ct, 999)
	if err != nil {
		t.Fatalf("DecryptForEpoch (hw) failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatalf("round-trip mismatch: got %q, want %q", dec, plaintext)
	}
}

func TestBarrier_DecryptForEpoch_HardwareMode_Error(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	decErr := errors.New("hw decrypt failed")
	enc := &mockHWEncryptor{decryptErr: decErr}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.DecryptForEpoch(nil, []byte("ct"), 1)
	if !errors.Is(err, decErr) {
		t.Fatalf("expected decrypt error, got %v", err)
	}
}

// --- Rotate returns ErrHardwareMode ---

func TestBarrier_Rotate_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.Rotate()
	if !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("expected ErrHardwareMode from Rotate in hardware mode, got %v", err)
	}
}

// --- PurgeEpochsBefore returns ErrHardwareMode ---

func TestBarrier_PurgeEpochsBefore_HardwareMode(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	_, err := b.PurgeEpochsBefore(1)
	if !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("expected ErrHardwareMode from PurgeEpochsBefore in hardware mode, got %v", err)
	}
}

// --- IsHardwareMode ---

func TestBarrier_IsHardwareMode_False(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	if b.IsHardwareMode() {
		t.Fatal("expected IsHardwareMode=false for software strategy")
	}
}

func TestBarrier_IsHardwareMode_True(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !b.IsHardwareMode() {
		t.Fatal("expected IsHardwareMode=true for hardware strategy")
	}
}

func TestBarrier_IsHardwareMode_ClearedAfterSeal(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1))
	enc := &mockHWEncryptor{prefix: []byte("HW:")}
	s := &hwStrategy{hwEnc: enc}

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !b.IsHardwareMode() {
		t.Fatal("expected hardware mode before seal")
	}

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if b.IsHardwareMode() {
		t.Fatal("expected hardware mode cleared after seal")
	}
}

// --- EpochBarrier alias ---

func TestEpochBarrier_IsAlias(t *testing.T) {
	// EpochBarrier is a type alias for Barrier.
	// Creating one via NewEpochBarrier should produce a valid barrier.
	b := NewEpochBarrier(DefaultBarrierConfig(42))
	if b == nil {
		t.Fatal("NewEpochBarrier returned nil")
	}
	if b.config.DeploymentID != 42 {
		t.Fatalf("expected deploymentID=42, got %d", b.config.DeploymentID)
	}

	// Verify it's the same type (type alias, not distinct type).
	var _ *Barrier = b
	var _ *EpochBarrier = b
}

func TestNewEpochBarrier_Functional(t *testing.T) {
	// Full functional test: create via alias, initialize, encrypt, decrypt.
	b := NewEpochBarrier(DefaultBarrierConfig(1))
	s := testStrategy(t)
	defer s.Close()

	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	plaintext := []byte("epoch barrier alias test")
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	dec, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if !bytes.Equal(plaintext, dec) {
		t.Fatal("round-trip failed via EpochBarrier alias")
	}
}

// --- StrategyID type and constants ---

func TestStrategyID_Constants(t *testing.T) {
	tests := []struct {
		name     string
		id       StrategyID
		expected string
	}{
		{"TPM2", StrategyTPM2, "tpm2"},
		{"PKCS11", StrategyPKCS11, "pkcs11"},
		{"AWSKMS", StrategyAWSKMS, "awskms"},
		{"GCPKMS", StrategyGCPKMS, "gcpkms"},
		{"AzureKV", StrategyAzureKV, "azurekv"},
		{"Vault", StrategyVault, "vault"},
		{"Shamir", StrategyShamir, "shamir"},
		{"Software", StrategySoftware, "software"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.id) != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, tt.id)
			}
		})
	}
}

func TestDefaultPreferenceOrder(t *testing.T) {
	if len(DefaultPreferenceOrder) != 8 {
		t.Fatalf("expected 8 strategies in preference order, got %d", len(DefaultPreferenceOrder))
	}

	// First should be TPM2 (hardware preferred).
	if DefaultPreferenceOrder[0] != StrategyTPM2 {
		t.Fatalf("expected first preference=TPM2, got %s", DefaultPreferenceOrder[0])
	}

	// Last should be Software (fallback).
	if DefaultPreferenceOrder[len(DefaultPreferenceOrder)-1] != StrategySoftware {
		t.Fatalf("expected last preference=Software, got %s", DefaultPreferenceOrder[len(DefaultPreferenceOrder)-1])
	}
}

func TestStrategyID_StringConversion(t *testing.T) {
	id := StrategyID("custom")
	if string(id) != "custom" {
		t.Fatalf("expected 'custom', got %q", string(id))
	}
}

// --- SealedRootKey new fields and defensive copy ---

func TestSealedRootKeyData_DefensiveCopyNewFields(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	// Set various new fields on the sealed key.
	b.mu.Lock()
	b.sealed.Version = 2
	b.sealed.KDFVersion = 1
	b.sealed.ShamirThreshold = 3
	b.sealed.ShamirTotal = 5
	b.sealed.Nonce = []byte("test-nonce-12b")
	b.sealed.HardwarePayload = []byte("hw-payload-data")
	b.sealed.CreatedAt = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	b.mu.Unlock()

	cp := b.SealedRootKeyData()
	if cp == nil {
		t.Fatal("expected non-nil SealedRootKeyData")
	}

	// Verify scalar fields.
	if cp.Version != 2 {
		t.Fatalf("expected Version=2, got %d", cp.Version)
	}
	if cp.KDFVersion != 1 {
		t.Fatalf("expected KDFVersion=1, got %d", cp.KDFVersion)
	}
	if cp.ShamirThreshold != 3 {
		t.Fatalf("expected ShamirThreshold=3, got %d", cp.ShamirThreshold)
	}
	if cp.ShamirTotal != 5 {
		t.Fatalf("expected ShamirTotal=5, got %d", cp.ShamirTotal)
	}
	if !cp.CreatedAt.Equal(time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected CreatedAt=2026-01-15, got %v", cp.CreatedAt)
	}

	// Verify byte slices are independent copies.
	if !bytes.Equal(cp.Nonce, []byte("test-nonce-12b")) {
		t.Fatalf("unexpected Nonce: %q", cp.Nonce)
	}
	if !bytes.Equal(cp.HardwarePayload, []byte("hw-payload-data")) {
		t.Fatalf("unexpected HardwarePayload: %q", cp.HardwarePayload)
	}

	// Mutate the copy and verify the original is unchanged.
	cp.Nonce[0] = 0xFF
	cp.HardwarePayload[0] = 0xFF
	cp.Version = 99

	b.mu.RLock()
	if b.sealed.Nonce[0] == 0xFF {
		t.Fatal("Nonce mutation leaked to original")
	}
	if b.sealed.HardwarePayload[0] == 0xFF {
		t.Fatal("HardwarePayload mutation leaked to original")
	}
	if b.sealed.Version == 99 {
		t.Fatal("Version mutation leaked to original")
	}
	b.mu.RUnlock()
}

func TestSealedRootKeyData_NilNonceAndPayload(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer s.Close()

	cp := b.SealedRootKeyData()
	if cp == nil {
		t.Fatal("expected non-nil SealedRootKeyData")
	}

	// Default software strategy doesn't set Nonce or HardwarePayload.
	if cp.Nonce != nil {
		t.Fatalf("expected nil Nonce, got %v", cp.Nonce)
	}
	if cp.HardwarePayload != nil {
		t.Fatalf("expected nil HardwarePayload, got %v", cp.HardwarePayload)
	}
}

// --- Credentials ---

func TestCredentials_ZeroValue(t *testing.T) {
	c := Credentials{}
	if c.Secret != "" {
		t.Fatalf("expected empty secret, got %q", c.Secret)
	}
}

func TestCredentials_WithSecret(t *testing.T) {
	c := Credentials{Secret: "my-secret"}
	if c.Secret != "my-secret" {
		t.Fatalf("expected 'my-secret', got %q", c.Secret)
	}
}
