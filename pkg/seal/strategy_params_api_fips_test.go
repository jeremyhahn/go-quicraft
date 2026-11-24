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

// External test package: it compiles against pkg/seal as a consumer does, so a symbol missing under -tags fips fails the build.
package seal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// TestFIPSBuildKeepsWithParamsConstructor: the symbol exists in a fips build and fails closed with the same error whatever the work factors.
func TestFIPSBuildKeepsWithParamsConstructor(t *testing.T) {
	for _, tc := range []struct {
		name         string
		time, memory uint32
		threads      uint8
	}{
		{"production grade", 4, 128 * 1024, 4},
		{"below every floor", 1, 1, 1},
		{"zero", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := seal.NewSoftwareStrategyWithParams(context.Background(), []byte("a-passphrase-long-enough-to-not-warn"), tc.time, tc.memory, tc.threads)
			if !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
				t.Fatalf("NewSoftwareStrategyWithParams(%d, %d, %d) in a FIPS build = %v, want ErrKDFUnsupportedVersion", tc.time, tc.memory, tc.threads, err)
			}
			if s != nil {
				t.Fatal("a refused constructor returned a strategy; nothing usable may escape a failed FIPS refusal")
			}
		})
	}
}

// TestFIPSBuildKeepsDeferredWithParamsConstructor: NewDeferredSoftwareStrategyWithParams exists in a fips build and refuses Argon2id with the same error whatever the work factors.
func TestFIPSBuildKeepsDeferredWithParamsConstructor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params seal.KDFParams
	}{
		{"defaults", seal.DefaultKDFParams()},
		{"below every floor", seal.KDFParams{Time: 1, Memory: 1, Threads: 1, KeyLen: 1, SaltLen: 1}},
		{"zero", seal.KDFParams{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := seal.NewDeferredSoftwareStrategyWithParams(seal.KDFArgon2id, tc.params)
			if !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
				t.Fatalf("NewDeferredSoftwareStrategyWithParams(Argon2id, %+v) in a FIPS build = %v, want ErrKDFUnsupportedVersion", tc.params, err)
			}
			if s != nil {
				t.Fatal("a refused constructor returned a strategy; nothing usable may escape a failed FIPS refusal")
			}
		})
	}
}

// TestFIPSBuildAcceptsDeferredWithParamsPBKDF2: under -tags fips the deferred constructor accepts PBKDF2, KDFParams() reads the factor back, and it stays deferred.
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
