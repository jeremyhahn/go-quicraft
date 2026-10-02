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

	"github.com/jeremyhahn/go-enclave"
)

// newBoundStrategies returns one strategy per BOUND constructor, each already
// bound to passphrase.
//
// newTestStrategy is NewSoftwareStrategyWithParams outside FIPS and
// NewSoftwareStrategyFIPS in a -tags fips build, so between the two CI legs
// every bound constructor is covered. NewSoftwareStrategyWithParams cannot be
// named directly here: it selects Argon2id explicitly and therefore fails in a
// FIPS build by design (strategy_software.go), which is pinned separately by
// strategy_params_api_fips_test.go.
func newBoundStrategies(t *testing.T, passphrase []byte) map[string]*SoftwareStrategy {
	t.Helper()
	out := make(map[string]*SoftwareStrategy, 3)
	for name, ctor := range map[string]func() (*SoftwareStrategy, error){
		"NewSoftwareStrategy": func() (*SoftwareStrategy, error) { return NewSoftwareStrategy(context.Background(), passphrase) },
		"NewSoftwareStrategyFIPS": func() (*SoftwareStrategy, error) {
			return NewSoftwareStrategyFIPS(context.Background(), passphrase, 1000)
		},
		"newTestStrategy": func() (*SoftwareStrategy, error) { return newTestStrategy(passphrase) },
	} {
		s, err := ctor()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		t.Cleanup(func() { _ = s.Close() })
		out[name] = s
	}
	return out
}

// TestSoftwareStrategy_DeferredPassphrase_ReportsConstructor pins the accessor
// against every constructor: false for the bound ones, true for the deferred
// one. Both answers are asserted, so an implementation that returns a constant
// fails whichever constant it picks.
func TestSoftwareStrategy_DeferredPassphrase_ReportsConstructor(t *testing.T) {
	for name, s := range newBoundStrategies(t, []byte("correct-horse-battery-staple")) {
		if s.DeferredPassphrase() {
			t.Errorf("%s: DeferredPassphrase() = true, want false — this constructor binds at construction", name)
		}
	}

	d := NewDeferredSoftwareStrategy()
	t.Cleanup(func() { _ = d.Close() })
	if !d.DeferredPassphrase() {
		t.Error("NewDeferredSoftwareStrategy: DeferredPassphrase() = false, want true")
	}
}

// TestSoftwareStrategy_DeferredPassphrase_PredictsCredentialSemantics is the
// test that makes the accessor worth having. The two modes have OPPOSITE
// credential semantics — a bound strategy ignores the credentials handed to
// UnsealRootKey, a deferred one validates against them — and a consumer reads
// the accessor precisely to preserve that behaviour when it replaces a
// strategy.
//
// So the assertion is not "the getter echoes the field" but "the getter
// predicts what unsealing with the WRONG passphrase does". A getter wired to
// the wrong field, or inverted, disagrees with the observed behaviour here even
// while the constructor test above could still be made to pass.
func TestSoftwareStrategy_DeferredPassphrase_PredictsCredentialSemantics(t *testing.T) {
	const correct = "correct-horse-battery-staple"
	const wrong = "wrong-horse-battery-staple"

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	strategies := newBoundStrategies(t, []byte(correct))
	d := NewDeferredSoftwareStrategy()
	t.Cleanup(func() { _ = d.Close() })
	strategies["NewDeferredSoftwareStrategy"] = d

	for name, s := range strategies {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sealed, err := s.SealRootKey(ctx, rootKey, mustCreds(t, string(correct)))
			if err != nil {
				t.Fatalf("SealRootKey: %v", err)
			}

			got, err := s.UnsealRootKey(ctx, sealed, mustCreds(t, string(wrong)))
			if err == nil {
				enclave.Wipe(got)
			}
			acceptedWrongPassphrase := err == nil

			// A bound strategy ignores creds, so the wrong passphrase unseals.
			// A deferred strategy validates them, so it must not.
			if wantAccepted := !s.DeferredPassphrase(); acceptedWrongPassphrase != wantAccepted {
				t.Fatalf("DeferredPassphrase() = %t, but unsealing with the WRONG passphrase %s "+
					"(err = %v) — the accessor does not describe this strategy's credential semantics",
					s.DeferredPassphrase(),
					map[bool]string{true: "SUCCEEDED", false: "FAILED"}[acceptedWrongPassphrase],
					err)
			}
		})
	}
}
