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

//go:build linux

package shamir

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/test/lockedmem"
)

// The kernel's view of the mapping proves share values and reconstructed secrets are frozen read-only.

// TestSplitProtected_ShareValuesAreFrozen fails if SplitProtected stops freezing share values.
func TestSplitProtected_ShareValuesAreFrozen(t *testing.T) {
	shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer func() {
		for _, s := range shares {
			s.Destroy()
		}
	}()

	for _, s := range shares {
		var (
			prot lockedmem.Protection
			perr error
		)
		if err := s.WithValue(func(v []byte) error {
			prot, perr = lockedmem.ProtectionOf(v)
			return nil
		}); err != nil {
			t.Fatalf("share %d: WithValue: %v", s.Index, err)
		}
		if perr != nil {
			t.Skipf("this kernel cannot report mapping protections: %v", perr)
		}
		if !prot.ReadOnly() {
			t.Fatalf("share %d's value is mapped %q: it was handed back WRITABLE, "+
				"so a stray write corrupts the share instead of faulting", s.Index, prot)
		}
	}
}

// TestCombineProtected_ReconstructedSecretIsFrozen fails if CombineProtected stops freezing the secret.
func TestCombineProtected_ReconstructedSecretIsFrozen(t *testing.T) {
	shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer func() {
		for _, s := range shares {
			s.Destroy()
		}
	}()

	secret, err := CombineProtected(shares[:2])
	if err != nil {
		t.Fatalf("CombineProtected: %v", err)
	}
	defer secret.Destroy()

	var prot lockedmem.Protection
	if werr := secret.WithBytes(func(view []byte) error {
		prot, err = lockedmem.ProtectionOf(view)
		return nil
	}); werr != nil {
		t.Fatalf("WithBytes: %v", werr)
	}
	if err != nil {
		t.Skipf("this kernel cannot report mapping protections: %v", err)
	}
	if !prot.ReadOnly() {
		t.Fatalf("the reconstructed secret is mapped %q: CombineProtected returned "+
			"a WRITABLE region, so a stray write corrupts the recovered key instead "+
			"of faulting", prot)
	}
}
