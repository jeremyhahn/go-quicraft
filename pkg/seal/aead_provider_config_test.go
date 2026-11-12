// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"crypto/cipher"
	"errors"
	"sync"
	"testing"
)

// errProviderRefused is the error recordingAEADProvider returns when told to
// fail.
var errProviderRefused = errors.New("test: AEAD provider refused")

// recordingAEADProvider is an AEADProvider that records every call, delegates
// to SoftwareAEADProvider, and can be told to fail or panic on later calls.
type recordingAEADProvider struct {
	mu       sync.Mutex
	calls    int
	algs     []SymmetricAlgorithm
	keyLens  []int
	failNext bool
	panicNxt bool
	inner    SoftwareAEADProvider
}

var _ AEADProvider = (*recordingAEADProvider)(nil)

func (p *recordingAEADProvider) NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error) {
	p.mu.Lock()
	p.calls++
	p.algs = append(p.algs, alg)
	p.keyLens = append(p.keyLens, len(key))
	fail, pnc := p.failNext, p.panicNxt
	p.failNext, p.panicNxt = false, false
	p.mu.Unlock()
	if pnc {
		panic("test: AEAD provider panicked")
	}
	if fail {
		return nil, errProviderRefused
	}
	return p.inner.NewAEAD(alg, key)
}

func (p *recordingAEADProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// checkCallsSince asserts that every call from index from onward used alg and
// a key of alg's size.
func (p *recordingAEADProvider) checkCallsSince(t *testing.T, from int, alg SymmetricAlgorithm) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := from; i < p.calls; i++ {
		if p.algs[i] != alg {
			t.Errorf("call %d: algorithm = %v, want %v", i, p.algs[i], alg)
		}
		if p.keyLens[i] != alg.KeySize() {
			t.Errorf("call %d: key length = %d, want %d", i, p.keyLens[i], alg.KeySize())
		}
	}
}

func (p *recordingAEADProvider) setFailNext() {
	p.mu.Lock()
	p.failNext = true
	p.mu.Unlock()
}

func (p *recordingAEADProvider) setPanicNext() {
	p.mu.Lock()
	p.panicNxt = true
	p.mu.Unlock()
}

// newProviderBarrier returns an initialized barrier using provider.
func newProviderBarrier(ctx context.Context, t *testing.T, provider AEADProvider) (*Barrier, *SoftwareStrategy) {
	t.Helper()
	cfg := testBarrierConfig(1)
	cfg.AEADProvider = provider
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	t.Cleanup(func() { _ = s.Close() })
	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return b, s
}

// mustRoundTrip encrypts pt on b and returns the ciphertext and its epoch,
// after checking that it decrypts back to pt.
func mustRoundTrip(t *testing.T, b *Barrier, pt []byte) ([]byte, uint64) {
	t.Helper()
	ct, epoch, err := b.EncryptWithEpoch(nil, pt)
	if err != nil {
		t.Fatalf("EncryptWithEpoch: %v", err)
	}
	mustDecrypt(t, b, ct, pt)
	return ct, epoch
}

// mustDecrypt checks that ct decrypts on b to want.
func mustDecrypt(t *testing.T, b *Barrier, ct, want []byte) {
	t.Helper()
	got, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Decrypt = %q, want %q", got, want)
	}
}

// (a) Initialize builds the first epoch's cipher through the provider.
func TestAEADProvider_InitializeUsesProvider(t *testing.T) {
	ctx := context.Background()
	p := &recordingAEADProvider{}
	b, _ := newProviderBarrier(ctx, t, p)
	if p.callCount() < 1 {
		t.Fatalf("provider calls after Initialize = %d, want >= 1", p.callCount())
	}
	p.checkCallsSince(t, 0, b.Config().Algorithm)
	mustRoundTrip(t, b, []byte("initialize payload"))
}

// (b) Rotate builds the new epoch's cipher through the provider; both epochs
// stay readable.
func TestAEADProvider_RotateUsesProvider(t *testing.T) {
	ctx := context.Background()
	p := &recordingAEADProvider{}
	b, _ := newProviderBarrier(ctx, t, p)
	ct1, e1 := mustRoundTrip(t, b, []byte("epoch one"))

	before := p.callCount()
	e2, err := b.Rotate(ctx)
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if e2 != e1+1 {
		t.Fatalf("Rotate epoch = %d, want %d", e2, e1+1)
	}
	if p.callCount() <= before {
		t.Fatalf("provider not called by Rotate (calls %d before, %d after)", before, p.callCount())
	}
	p.checkCallsSince(t, before, b.Config().Algorithm)

	ct2, got := mustRoundTrip(t, b, []byte("epoch two"))
	if got != e2 {
		t.Fatalf("encrypt epoch after Rotate = %d, want %d", got, e2)
	}
	mustDecrypt(t, b, ct1, []byte("epoch one"))
	mustDecrypt(t, b, ct2, []byte("epoch two"))
}

// (c) A provider error on Rotate fails the rotation and leaves the old epoch
// active.
func TestAEADProvider_RotateErrorKeepsEpoch(t *testing.T) {
	ctx := context.Background()
	p := &recordingAEADProvider{}
	b, _ := newProviderBarrier(ctx, t, p)
	ct1, e1 := mustRoundTrip(t, b, []byte("before failed rotate"))

	p.setFailNext()
	if _, err := b.Rotate(ctx); !errors.Is(err, errProviderRefused) {
		t.Fatalf("Rotate error = %v, want errors.Is %v", err, errProviderRefused)
	}
	if got := b.CurrentEpoch(); got != e1 {
		t.Fatalf("CurrentEpoch after failed Rotate = %d, want %d", got, e1)
	}
	if got := b.SealedRootKeyData().Epoch; got != e1 {
		t.Fatalf("sealed epoch after failed Rotate = %d, want %d", got, e1)
	}
	mustDecrypt(t, b, ct1, []byte("before failed rotate"))
	if _, e := mustRoundTrip(t, b, []byte("after failed rotate")); e != e1 {
		t.Fatalf("encrypt epoch after failed Rotate = %d, want %d", e, e1)
	}

	// The provider recovers; a later rotation succeeds.
	if e2, err := b.Rotate(ctx); err != nil || e2 != e1+1 {
		t.Fatalf("Rotate after recovery = (%d, %v), want (%d, nil)", e2, err, e1+1)
	}
}

// rotateRecovering calls Rotate and reports whether it panicked.
func rotateRecovering(ctx context.Context, b *Barrier) (panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	_, err = b.Rotate(ctx)
	return false, err
}

// (d) A provider panic on Rotate, once recovered, leaves the epoch unchanged
// and the barrier usable (the lock is released, the root key still opens).
func TestAEADProvider_RotatePanicKeepsBarrierUsable(t *testing.T) {
	ctx := context.Background()
	p := &recordingAEADProvider{}
	b, _ := newProviderBarrier(ctx, t, p)
	ct1, e1 := mustRoundTrip(t, b, []byte("before panic"))

	p.setPanicNext()
	panicked, err := rotateRecovering(ctx, b)
	if !panicked && err == nil {
		t.Fatal("Rotate with a panicking provider neither panicked nor failed")
	}
	if got := b.CurrentEpoch(); got != e1 {
		t.Fatalf("CurrentEpoch after panicked Rotate = %d, want %d", got, e1)
	}
	mustDecrypt(t, b, ct1, []byte("before panic"))
	if _, e := mustRoundTrip(t, b, []byte("after panic")); e != e1 {
		t.Fatalf("encrypt epoch after panicked Rotate = %d, want %d", e, e1)
	}
	if e2, err := b.Rotate(ctx); err != nil || e2 != e1+1 {
		t.Fatalf("Rotate after panic = (%d, %v), want (%d, nil)", e2, err, e1+1)
	}
}

// (e) Seal then Unseal with the same provider reads data written before the
// seal, and Unseal rebuilds the ciphers through the provider.
func TestAEADProvider_SealUnsealReadsOldData(t *testing.T) {
	ctx := context.Background()
	p := &recordingAEADProvider{}
	b, s := newProviderBarrier(ctx, t, p)
	ct1, _ := mustRoundTrip(t, b, []byte("pre-rotate"))
	if _, err := b.Rotate(ctx); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	ct2, _ := mustRoundTrip(t, b, []byte("post-rotate"))

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	before := p.callCount()
	if err := b.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if p.callCount() <= before {
		t.Fatalf("provider not called by Unseal (calls %d before, %d after)", before, p.callCount())
	}
	p.checkCallsSince(t, 0, b.Config().Algorithm)
	mustDecrypt(t, b, ct1, []byte("pre-rotate"))
	mustDecrypt(t, b, ct2, []byte("post-rotate"))
	mustRoundTrip(t, b, []byte("post-unseal"))
}

// (f) A nil AEADProvider means SoftwareAEADProvider: barriers sharing a root
// key and salt decrypt each other's ciphertext whichever of the two they use.
func TestAEADProvider_NilMeansSoftwareProvider(t *testing.T) {
	ctx := context.Background()
	bNil, s := newProviderBarrier(ctx, t, nil)
	ctNil, _ := mustRoundTrip(t, bNil, []byte("written with nil provider"))

	// A barrier with the explicit software provider loads the same sealed
	// state (root key, salt, epochs).
	cfg := testBarrierConfig(1)
	cfg.AEADProvider = NewSoftwareAEADProvider()
	bSW := MustNewBarrier(cfg)
	if err := bSW.SetSealedRootKey(bNil.SealedRootKeyData()); err != nil {
		t.Fatalf("SetSealedRootKey (software): %v", err)
	}
	if err := bSW.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal (software): %v", err)
	}
	mustDecrypt(t, bSW, ctNil, []byte("written with nil provider"))
	ctSW, _ := mustRoundTrip(t, bSW, []byte("written with software provider"))

	// And back: a nil-provider barrier loading the software barrier's state
	// reads both ciphertexts.
	bBack := MustNewBarrier(testBarrierConfig(1))
	if err := bBack.SetSealedRootKey(bSW.SealedRootKeyData()); err != nil {
		t.Fatalf("SetSealedRootKey (nil): %v", err)
	}
	if err := bBack.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal (nil): %v", err)
	}
	mustDecrypt(t, bBack, ctSW, []byte("written with software provider"))
	mustDecrypt(t, bBack, ctNil, []byte("written with nil provider"))
}

// (g) SoftwareAEADProvider rejects an unknown algorithm and a wrong key size
// with the documented errors.
func TestAEADProvider_SoftwareProviderErrors(t *testing.T) {
	p := NewSoftwareAEADProvider()
	alg := DefaultBarrierConfig(1).Algorithm

	if _, err := p.NewAEAD(SymmetricAlgorithm(250), make([]byte, alg.KeySize())); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Errorf("unknown algorithm: err = %v, want %v", err, ErrUnsupportedAlgorithm)
	}
	if _, err := p.NewAEAD(alg, make([]byte, alg.KeySize()-1)); !errors.Is(err, ErrInvalidKeySize) {
		t.Errorf("short key: err = %v, want %v", err, ErrInvalidKeySize)
	}
	if _, err := p.NewAEAD(alg, make([]byte, alg.KeySize()+1)); !errors.Is(err, ErrInvalidKeySize) {
		t.Errorf("long key: err = %v, want %v", err, ErrInvalidKeySize)
	}
}
