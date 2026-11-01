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

//go:build fips

// This file is deliberately an EXTERNAL test package. Compiling it is half the
// assertion: it consumes pkg/seal exactly as a downstream module does, so if a
// build tag ever moved NewSoftwareStrategyWithParams out of a FIPS build again,
// the FIPS test run would fail to build rather than quietly pass. go-quicraft is
// a Layer 1 dependency; an exported symbol that disappears under -tags fips
// breaks every consumer that merely MENTIONS it, including one whose call is
// already guarded by its own runtime FIPS check.
package seal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// TestFIPSBuildKeepsWithParamsConstructor pins both halves of the contract: the
// symbol is present (this file compiles), and calling it fails closed with the
// same error the absent Argon2id deriver gives everywhere else in a FIPS build.
func TestFIPSBuildKeepsWithParamsConstructor(t *testing.T) {
	s, err := seal.NewSoftwareStrategyWithParams(context.Background(), []byte("a-passphrase-long-enough-to-not-warn"), 4, 128*1024, 4)
	if !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
		t.Fatalf("NewSoftwareStrategyWithParams in a FIPS build = %v, want ErrKDFUnsupportedVersion", err)
	}
	if s != nil {
		t.Fatal("a refused constructor returned a strategy; nothing usable may escape a failed FIPS refusal")
	}
}

// TestFIPSBuildRefusesWithParamsBeforeValidation guards the ORDER of the
// refusal. Work factors are meaningless in a build with no Argon2id, so the
// answer must not depend on them: a caller passing production-grade parameters
// and one passing near-trivial ones get the identical error, and neither is
// told that some parameter choice would have worked.
func TestFIPSBuildRefusesWithParamsBeforeValidation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		time, memory uint32
		threads      uint8
	}{
		{"below every floor", 1, 1, 1},
		{"zero", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := seal.NewSoftwareStrategyWithParams(context.Background(), []byte("a-passphrase-long-enough-to-not-warn"), tc.time, tc.memory, tc.threads); !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
				t.Fatalf("NewSoftwareStrategyWithParams(%d, %d, %d) = %v, want ErrKDFUnsupportedVersion", tc.time, tc.memory, tc.threads, err)
			}
		})
	}
}

// TestFIPSBuildKeepsDeferredWithParamsConstructor is the deferred sibling of
// TestFIPSBuildKeepsWithParamsConstructor, and it exists for the same reason:
// compiling this file is half the assertion. NewDeferredSoftwareStrategyWithParams
// claims in its doc comment to exist in EVERY build including -tags fips, and a
// claim about cross-build availability is only worth making if a FIPS build
// fails to compile when it stops being true.
//
// The in-package test of this constructor cannot make that assertion. It runs in
// the normal binary, toggles GOFIPS140, and accepts either FIPS error — so it
// would still pass if a later refactor put //go:build fips on the constructor and
// removed the symbol from FIPS builds entirely. That would break every consumer
// that merely MENTIONS the call, including one whose use is already guarded by
// its own runtime FIPS check.
func TestFIPSBuildKeepsDeferredWithParamsConstructor(t *testing.T) {
	s, err := seal.NewDeferredSoftwareStrategyWithParams(seal.KDFArgon2id, seal.DefaultKDFParams())
	if !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
		t.Fatalf("NewDeferredSoftwareStrategyWithParams(Argon2id) in a FIPS build = %v, want ErrKDFUnsupportedVersion", err)
	}
	if s != nil {
		t.Fatal("a refused constructor returned a strategy; nothing usable may escape a failed FIPS refusal")
	}
}

// TestFIPSBuildAcceptsDeferredWithParamsPBKDF2 is the half that has no analogue
// among the bound constructors, and it is the one a FIPS deployment depends on.
//
// NewSoftwareStrategyWithParams names Argon2id and can therefore NEVER succeed in
// a FIPS build. This constructor takes the version as an argument, so it can —
// and must, or preserving a tuned work factor across a strategy replacement would
// be impossible in exactly the deployments most likely to have tuned one.
//
// It also pins the pair working end to end under -tags fips: KDFParams() reads
// the factor back, and the result is still deferred.
func TestFIPSBuildAcceptsDeferredWithParamsPBKDF2(t *testing.T) {
	params := seal.DefaultFIPSKDFParams()
	params.Iterations = 750_000 // deliberately not the default, so this proves carriage

	s, err := seal.NewDeferredSoftwareStrategyWithParams(seal.KDFPbkdf2SHA256, params)
	if err != nil {
		t.Fatalf("PBKDF2 must be accepted in a FIPS build: %v", err)
	}
	if s == nil {
		t.Fatal("accepted constructor returned nil")
	}
	defer s.Close()

	if got := s.KDFParams(); got != params {
		t.Fatalf("KDFParams() = %+v, want %+v — the tuned factor was not carried", got, params)
	}
	if s.KDFVersion() != seal.KDFPbkdf2SHA256 {
		t.Fatalf("KDFVersion() = %v, want PBKDF2", s.KDFVersion())
	}
	if !s.DeferredPassphrase() {
		t.Fatal("the strategy is bound; the barrier would stop authenticating its unsealer")
	}
}

// TestFIPSBuildRefusesDeferredWithParamsBeforeValidation guards the refusal ORDER
// for the deferred constructor, exactly as its bound sibling above does: with no
// Argon2id deriver the work factors are meaningless, so no parameter choice may
// change the answer or hint that some other choice would have worked.
func TestFIPSBuildRefusesDeferredWithParamsBeforeValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params seal.KDFParams
	}{
		{"below every floor", seal.KDFParams{Time: 1, Memory: 1, Threads: 1, KeyLen: 1, SaltLen: 1}},
		{"zero", seal.KDFParams{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := seal.NewDeferredSoftwareStrategyWithParams(seal.KDFArgon2id, tc.params); !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
				t.Fatalf("NewDeferredSoftwareStrategyWithParams(Argon2id, %+v) = %v, want ErrKDFUnsupportedVersion", tc.params, err)
			}
		})
	}
}
