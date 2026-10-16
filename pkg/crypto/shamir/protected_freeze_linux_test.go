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

// The two sites that fill a sized enclave and then FREEZE it in place.
//
// Memory.Freeze mprotects the region the material was written into, so
// there is one region and no locked-to-locked copy into a second, from-bytes
// enclave.
//
// The PROPERTY these pin: `WithValue` documents the
// value as read-only, and callers rely on a stray write faulting rather than
// silently corrupting a share or a reconstructed key. A missing Freeze is
// invisible to every functional assertion — the share still decodes, the secret
// still reconstructs — so the kernel's own view of the mapping is the witness.
//
// ProtectionOf reads the ADDRESS only and never dereferences it, which is what
// makes it safe to ask about a region whose protections are the question.

// TestSplitProtected_ShareValuesAreFrozen is a mutation target.
//
// Verified by mutation: deleting the `freeze(value)` call in
// SplitProtected fails here with
//
//	protected_freeze_linux_test.go:66: share 1's value is mapped "rw-p": it was
//	handed back WRITABLE, so a stray write corrupts the share instead of faulting
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

// TestCombineProtected_ReconstructedSecretIsFrozen is the other mutation target.
//
// Verified by mutation: deleting the `freeze(secret)` call in
// CombineProtected fails here with
//
//	protected_freeze_linux_test.go:106: the reconstructed secret is mapped "rw-p":
//	CombineProtected returned a WRITABLE region, so a stray write corrupts the
//	recovered key instead of faulting
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
