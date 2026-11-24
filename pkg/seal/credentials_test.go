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

// These tests assert that NewCredentials consumes the caller's passphrase buffer rather than copying it.

// TestNewCredentials_ConsumesTheCallersPassphrase fails if NewCredentials seals a copy of the passphrase.
func TestNewCredentials_ConsumesTheCallersPassphrase(t *testing.T) {
	const passphrase = "a-passphrase-worth-protecting"
	secret := []byte(passphrase)

	creds, err := NewCredentials(context.Background(), secret)
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

	// The material is in the credential, so the wipe was a move, not a loss.
	opened, err := creds.Secret.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer opened.Destroy()
	if eq, err := opened.EqualTo([]byte(passphrase)); err != nil {
		t.Fatalf("EqualTo: %v", err)
	} else if !eq {
		t.Fatal("the credential does not hold the passphrase it consumed")
	}
}

// TestNewCredentials_RefusesEmpty pins the refusal at the boundary the caller
// can act on. go-enclave would refuse a zero-length seal with ErrEmptyEnclave;
// naming ErrInvalidPassphrase here says which of the caller's inputs was wrong
// instead of reporting an allocation fault two layers down.
func TestNewCredentials_RefusesEmpty(t *testing.T) {
	for name, secret := range map[string][]byte{"nil": nil, "empty": {}} {
		creds, err := NewCredentials(context.Background(), secret)
		if !errors.Is(err, ErrInvalidPassphrase) {
			t.Fatalf("NewCredentials(%s) = %v, want ErrInvalidPassphrase", name, err)
		}
		if creds.Secret != nil {
			t.Fatalf("NewCredentials(%s) must not build a credential", name)
		}
	}
}

// TestCredentials_ZeroValueMeansNoneSupplied pins the contract every
// ambient-credential strategy depends on. A nil enclave.Sealed has to keep
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

// TestCredentials_DeferredRoundTripThroughTheByteAPI: a protected passphrase round-trips through the []byte root-key API, and one credential serves both calls.
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
