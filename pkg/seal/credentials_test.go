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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// This file is #41's verification. The issue asked for exactly one thing that a
// type change alone does not give you:
//
//	"A test proving the passphrase's caller-side buffer is wiped or never
//	 existed — and a mutation of that wipe turning it red."
//
// The retype of Credentials.Secret from []byte to *enclave.SealedEnclave makes
// the WRONG shape uncompilable, which the compiler proves. What it does not
// prove is that NewCredentials — the only supported way to get from a caller's
// bytes to that type — actually consumes those bytes rather than copying them.
// A constructor that read `enclave.NewSealed(append([]byte(nil), secret...))`
// would type-check, satisfy every other test in this package, and leave the
// passphrase in the caller's slice for the collector to move around: the exact
// gap the issue was filed against, reintroduced one line deeper.
//
// So the wipe is asserted directly, on the caller's own slice, and it is the
// mutation target.

// TestNewCredentials_ConsumesTheCallersPassphrase is the mutation target.
//
// Verified by mutation on 2026-09-03: changing NewCredentials to seal a COPY —
// `enclave.NewSealed(append([]byte(nil), secret...))` — fails here with
//
//	credentials_test.go:64: the caller's passphrase must be wiped by
//	NewCredentials; byte 0 is 0x61 ("a-passphrase-worth-protecting" survived)
//
// and passes again when the copy is removed.
func TestNewCredentials_ConsumesTheCallersPassphrase(t *testing.T) {
	const passphrase = "a-passphrase-worth-protecting"
	secret := []byte(passphrase)

	creds, err := NewCredentials(secret)
	if err != nil {
		t.Fatalf("NewCredentials: %v", err)
	}
	defer creds.Secret.Destroy()

	for i, b := range secret {
		if b != 0 {
			t.Fatalf("the caller's passphrase must be wiped by NewCredentials; "+
				"byte %d is %#x (%q survived)", i, b, passphrase)
		}
	}

	// And the other half of "consumed": the material is really in the credential,
	// so the wipe above is a MOVE rather than a loss. A constructor that wiped and
	// stored nothing would pass the loop and fail here.
	opened, err := creds.Secret.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer opened.Destroy()
	if !opened.EqualTo([]byte(passphrase)) {
		t.Fatal("the credential does not hold the passphrase it consumed")
	}
}

// TestNewCredentials_RefusesEmpty pins the refusal at the boundary the caller
// can act on. go-enclave would refuse a zero-length seal with ErrEmptyEnclave;
// naming ErrInvalidPassphrase here says which of the caller's inputs was wrong
// instead of reporting an allocation fault two layers down.
func TestNewCredentials_RefusesEmpty(t *testing.T) {
	for name, secret := range map[string][]byte{"nil": nil, "empty": {}} {
		creds, err := NewCredentials(secret)
		if !errors.Is(err, ErrInvalidPassphrase) {
			t.Fatalf("NewCredentials(%s) = %v, want ErrInvalidPassphrase", name, err)
		}
		if creds.Secret != nil {
			t.Fatalf("NewCredentials(%s) must not build a credential", name)
		}
	}
}

// TestCredentials_ZeroValueMeansNoneSupplied pins the contract every
// ambient-credential strategy depends on. A nil *SealedEnclave has to keep
// meaning what a nil []byte meant: "the caller supplied none", not "the caller
// supplied an empty passphrase".
func TestCredentials_ZeroValueMeansNoneSupplied(t *testing.T) {
	ctx := context.Background()

	// A passphrase-BOUND strategy ignores credentials entirely and must succeed.
	bound := newPlainStrategy(t)
	defer bound.Close()
	rootKey := make([]byte, rootKeySize)
	for i := range rootKey {
		rootKey[i] = byte(i)
	}
	if _, err := bound.SealRootKey(ctx, rootKey, Credentials{}); err != nil {
		t.Fatalf("a bound strategy must ignore the zero credential: %v", err)
	}

	// A DEFERRED strategy has nowhere else to look and must say so.
	deferred := NewDeferredSoftwareStrategy()
	defer deferred.Close()
	if _, err := deferred.SealRootKey(ctx, rootKey, Credentials{}); !errors.Is(err, ErrEmptyCredentials) {
		t.Fatalf("SealRootKey with the zero credential = %v, want ErrEmptyCredentials", err)
	}
	if _, err := deferred.UnsealRootKey(ctx, &SealedRootKey{}, Credentials{}); !errors.Is(err, ErrEmptyCredentials) {
		t.Fatalf("UnsealRootKey with the zero credential = %v, want ErrEmptyCredentials", err)
	}
}

// TestCredentials_DeferredRoundTripThroughTheByteAPI is the end-to-end half:
// the []byte root-key pair — the surface every strategy in the portfolio
// implements, and the one #41 is about — now carries a protected passphrase and
// still round-trips.
//
// The credential is reused across both calls on purpose. The seal layer opens
// it, derives from it and destroys the opened copy; it never destroys the
// credential, so one credential serves many calls and is released by the
// caller's own Destroy. A strategy that consumed it would fail the unseal here.
func TestCredentials_DeferredRoundTripThroughTheByteAPI(t *testing.T) {
	ctx := context.Background()
	s := NewDeferredSoftwareStrategy()
	s.kdfVersion = testKDFVersion
	s.kdfParams = testKDFParams()
	defer s.Close()

	creds := mustCreds(t, "the-deferred-passphrase")
	defer creds.Secret.Destroy()

	want := make([]byte, rootKeySize)
	for i := range want {
		want[i] = byte(i * 7)
	}
	rootKey := append([]byte(nil), want...)

	sealed, err := s.SealRootKey(ctx, rootKey, creds)
	if err != nil {
		t.Fatalf("SealRootKey: %v", err)
	}

	got, err := s.UnsealRootKey(ctx, sealed, creds)
	if err != nil {
		t.Fatalf("UnsealRootKey: %v", err)
	}
	defer enclave.Wipe(got)
	if string(got) != string(want) {
		t.Fatalf("root key did not round-trip: got %x, want %x", got, want)
	}

	// The wrong passphrase must still fail, or the round trip above proves
	// nothing about the credential being read at all.
	wrong := mustCreds(t, "not-the-deferred-passphrase")
	defer wrong.Secret.Destroy()
	if _, err := s.UnsealRootKey(ctx, sealed, wrong); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("UnsealRootKey with the wrong credential = %v, want ErrDecryptionFailed", err)
	}
}

// TestCredentials_ProtectedAliasIsTheSameType pins the collapse #41 performed.
//
// The point of the issue was not only that the passphrase should be protected;
// it was that ONE interface must not carry TWO credential types, because a
// reader can no longer tell from a signature which one is the real contract.
// ProtectedCredentials survives as an ALIAS so implementations that name it
// still satisfy ProtectedSealingStrategy — a defined type would break every one
// of them for no gain — and these assertions are what keep it an alias.
var (
	_ Credentials          = ProtectedCredentials{}
	_ ProtectedCredentials = Credentials{}
)

func TestCredentials_ProtectedAliasIsTheSameType(t *testing.T) {
	// Identity is proven at compile time above. What runs is that the deprecated
	// constructor is the same function, so a caller on either name gets the same
	// consume-the-source behaviour rather than two that could drift.
	secret := []byte("a-passphrase-through-the-old-name")
	creds, err := NewProtectedCredentials(secret)
	if err != nil {
		t.Fatalf("NewProtectedCredentials: %v", err)
	}
	defer creds.Secret.Destroy()
	for i, b := range secret {
		if b != 0 {
			t.Fatalf("the deprecated constructor must consume its source too; byte %d is %#x", i, b)
		}
	}
}
