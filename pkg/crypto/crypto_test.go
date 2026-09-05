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

package crypto

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// TestBarrierTypeAlias verifies that the crypto.Barrier type alias is
// assignment-compatible with seal.Barrier by using pointer assignments,
// which avoids copying the embedded sync.RWMutex.
func TestBarrierTypeAlias(t *testing.T) {
	var b *Barrier
	var sb *seal.Barrier
	// Pointer assignment between aliased types must compile.
	b = sb
	sb = b
	if b != nil {
		t.Fatal("zero-value pointer should be nil")
	}
	if sb != nil {
		t.Fatal("zero-value pointer should be nil")
	}
}

// TestBarrierConfigTypeAlias verifies that crypto.BarrierConfig is
// assignment-compatible with seal.BarrierConfig.
func TestBarrierConfigTypeAlias(t *testing.T) {
	var bc BarrierConfig
	var sbc seal.BarrierConfig
	bc = sbc
	sbc = bc
	_ = sbc
}

// TestSealingStrategyTypeAlias verifies that crypto.SealingStrategy is
// assignment-compatible with seal.SealingStrategy.
func TestSealingStrategyTypeAlias(t *testing.T) {
	var cs SealingStrategy
	var ss seal.SealingStrategy
	cs = ss
	ss = cs
	_ = ss
}

// TestCredentialsTypeAlias verifies that crypto.Credentials is
// assignment-compatible with seal.Credentials.
func TestCredentialsTypeAlias(t *testing.T) {
	var cc Credentials
	var sc seal.Credentials
	cc = sc
	sc = cc
	_ = sc
}

// TestSealedRootKeyTypeAlias verifies that crypto.SealedRootKey is
// assignment-compatible with seal.SealedRootKey.
func TestSealedRootKeyTypeAlias(t *testing.T) {
	var cr SealedRootKey
	var sr seal.SealedRootKey
	cr = sr
	sr = cr
	_ = sr
}

// TestNewBarrierReexport verifies that the re-exported NewBarrier returns a
// valid Barrier instance, confirming the re-export from pkg/seal is wired
// correctly.
//
// The nil check that stood here went with the var. These re-exports are declared
// functions now (#95), so `NewBarrier == nil` is a vet failure — "comparison of
// function NewBarrier == nil is always false" — and being unreassignable is the
// whole point of the change.
func TestNewBarrierReexport(t *testing.T) {
	cfg := BarrierConfig{
		Algorithm: seal.AlgAES256GCM,
	}
	b := MustNewBarrier(cfg)
	if b == nil {
		t.Fatal("NewBarrier returned nil")
	}
	if !b.IsSealed() {
		t.Fatal("newly created barrier should be sealed")
	}
}

// TestNewBarrierReexport_InvalidConfig verifies that the re-exported
// NewBarrier returns an error — never a panic — for an invalid (zero-value)
// config, and that MustNewBarrier is the form that panics. The re-export is a
// wrapper function, so this also pins that it tracks the real signature rather
// than an older one: a drifted signature in pkg/seal fails to compile here.
func TestNewBarrierReexport_InvalidConfig(t *testing.T) {
	t.Run("NewBarrier returns an error", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("re-exported NewBarrier must not panic, got %v", r)
			}
		}()
		b, err := NewBarrier(BarrierConfig{})
		if err == nil {
			t.Fatal("NewBarrier with an empty config should return an error")
		}
		if b != nil {
			t.Fatal("NewBarrier must return a nil Barrier alongside the error")
		}
	})

	t.Run("MustNewBarrier panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("MustNewBarrier with an empty config should panic")
			}
		}()
		MustNewBarrier(BarrierConfig{})
	})
}

// TestNewCredentialsReexport covers the gap #95 closed: Credentials is
// re-exported above, but #41 retyped its Secret field to an
// *enclave.SealedEnclave, and seal.NewCredentials — which was NOT re-exported —
// became the only supported way to populate one. The shim therefore exposed a
// type its own consumers could not construct.
//
// The delegation must carry NewCredentials's contract intact, including the part
// a caller is most likely to depend on and least likely to notice missing: the
// source slice is CONSUMED.
func TestNewCredentialsReexport(t *testing.T) {
	secret := []byte("a-passphrase-long-enough-to-pass")
	creds, err := NewCredentials(secret)
	if err != nil {
		t.Fatalf("NewCredentials: %v", err)
	}
	if creds.Secret == nil {
		t.Fatal("NewCredentials returned no sealed secret")
	}
	defer creds.Secret.Destroy()

	for i, c := range secret {
		if c != 0 {
			t.Fatalf("byte %d of the caller's slice = %#x: the re-export must "+
				"CONSUME it, as seal.NewCredentials does", i, c)
		}
	}

	// And what it built is a seal.Credentials, not a look-alike: the alias is
	// what makes the shim worth completing rather than replacing.
	sc := creds
	if sc.Secret != creds.Secret {
		t.Fatal("the re-exported constructor did not produce a seal.Credentials")
	}

	if _, err := NewCredentials(nil); !errors.Is(err, seal.ErrInvalidPassphrase) {
		t.Fatalf("NewCredentials(nil) = %v, want seal.ErrInvalidPassphrase", err)
	}
}
