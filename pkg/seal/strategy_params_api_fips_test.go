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
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// TestFIPSBuildKeepsWithParamsConstructor pins both halves of the contract: the
// symbol is present (this file compiles), and calling it fails closed with the
// same error the absent Argon2id deriver gives everywhere else in a FIPS build.
func TestFIPSBuildKeepsWithParamsConstructor(t *testing.T) {
	s, err := seal.NewSoftwareStrategyWithParams([]byte("a-passphrase-long-enough-to-not-warn"), 4, 128*1024, 4)
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
		{"production floor", 4, 128 * 1024, 4},
		{"below every floor", 1, 1, 1},
		{"zero", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := seal.NewSoftwareStrategyWithParams([]byte("a-passphrase-long-enough-to-not-warn"), tc.time, tc.memory, tc.threads); !errors.Is(err, seal.ErrKDFUnsupportedVersion) {
				t.Fatalf("NewSoftwareStrategyWithParams(%d, %d, %d) = %v, want ErrKDFUnsupportedVersion", tc.time, tc.memory, tc.threads, err)
			}
		})
	}
}
