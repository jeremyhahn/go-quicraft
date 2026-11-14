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
	"errors"
	"strings"
	"testing"
)

// TestNewEpochBarrier_ValidConfig checks that NewEpochBarrier, documented as
// calling NewBarrier, returns a usable barrier carrying the config.
func TestNewEpochBarrier_ValidConfig(t *testing.T) {
	cfg := DefaultBarrierConfig(7)
	b, err := NewEpochBarrier(cfg)
	if err != nil {
		t.Fatalf("NewEpochBarrier() error = %v, want nil", err)
	}
	if b == nil {
		t.Fatal("NewEpochBarrier() returned a nil barrier")
	}
	if b.config.DeploymentID != 7 {
		t.Fatalf("config.DeploymentID = %d, want 7", b.config.DeploymentID)
	}
	ref, err := NewBarrier(DefaultBarrierConfig(7))
	if err != nil {
		t.Fatalf("NewBarrier() error = %v, want nil", err)
	}
	if got, want := b.IsSealed(), ref.IsSealed(); got != want {
		t.Fatalf("NewEpochBarrier().IsSealed() = %v, NewBarrier().IsSealed() = %v", got, want)
	}
}

// TestNewEpochBarrier_UnsupportedAlgorithm checks that NewEpochBarrier
// returns the same refusal NewBarrier returns for an unregistered algorithm.
func TestNewEpochBarrier_UnsupportedAlgorithm(t *testing.T) {
	cfg := DefaultBarrierConfig(7)
	cfg.Algorithm = unknownAlg

	ctors := []struct {
		name string
		fn   func(BarrierConfig) (*Barrier, error)
	}{
		{"NewBarrier", NewBarrier},
		{"NewEpochBarrier", NewEpochBarrier},
	}
	for _, c := range ctors {
		t.Run(c.name, func(t *testing.T) {
			b, err := c.fn(cfg)
			if b != nil {
				t.Fatalf("%s() barrier = %v, want nil", c.name, b)
			}
			if !errors.Is(err, ErrUnsupportedAlgorithm) {
				t.Fatalf("%s() error = %v, want ErrUnsupportedAlgorithm", c.name, err)
			}
		})
	}
}

// TestMustNewEpochBarrier_PanicsOnInvalidConfig checks that
// MustNewEpochBarrier, documented as calling MustNewBarrier, panics with
// MustNewBarrier's message on a config NewBarrier refuses.
func TestMustNewEpochBarrier_PanicsOnInvalidConfig(t *testing.T) {
	cfg := DefaultBarrierConfig(7)
	cfg.Algorithm = unknownAlg

	_, wantErr := NewBarrier(cfg)
	if wantErr == nil {
		t.Fatal("NewBarrier() accepted an unregistered algorithm")
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustNewEpochBarrier() did not panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recovered %T (%v), want string", r, r)
		}
		if !strings.HasPrefix(msg, "seal.MustNewBarrier: ") {
			t.Fatalf("panic = %q, want prefix %q", msg, "seal.MustNewBarrier: ")
		}
		if !strings.Contains(msg, wantErr.Error()) {
			t.Fatalf("panic = %q, want it to contain %q", msg, wantErr.Error())
		}
	}()
	MustNewEpochBarrier(cfg)
}
