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

// TestNewBarrierReexport verifies that the NewBarrier function variable
// is non-nil and returns a valid Barrier instance. This confirms the
// re-export from pkg/seal is wired correctly.
func TestNewBarrierReexport(t *testing.T) {
	if NewBarrier == nil {
		t.Fatal("NewBarrier should not be nil")
	}
	cfg := BarrierConfig{
		Algorithm: seal.AlgAES256GCM,
	}
	b := NewBarrier(cfg)
	if b == nil {
		t.Fatal("NewBarrier returned nil")
	}
	if !b.IsSealed() {
		t.Fatal("newly created barrier should be sealed")
	}
}

// TestNewBarrierReexport_InvalidConfig verifies that NewBarrier with an
// invalid (zero-value) config panics due to unsupported algorithm,
// confirming that validation is enforced through the re-exported function.
func TestNewBarrierReexport_InvalidConfig(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewBarrier with empty config should panic")
		}
	}()
	NewBarrier(BarrierConfig{})
}
