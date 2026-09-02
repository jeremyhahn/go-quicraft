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

package shamir

import (
	"bytes"
	"encoding/base64"
	"errors"
	"testing"

	goenclave "github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// errLockRefused stands in for the kernel refusing an mlock.
var errLockRefused = errors.New("mlock refused")

// refuseProtectedMemory mimics go-enclave's failure contract exactly: it WIPES
// the source before reporting the refusal. A stub that skipped the wipe would
// make the test pass against an implementation that leaks the caller's bytes.
func refuseProtectedMemory(data []byte) (*enclave.MemoryEnclave, error) {
	goenclave.Wipe(data)
	return nil, errLockRefused
}

// TestNewProtectedShare_AllocationFailureIsNotValidation pins the ownership
// contract the two error types encode. It is a regression test: this branch used
// to report a locked-memory refusal as a *ValidationError, whose documented
// meaning is "the source is left untouched for the caller to wipe" — while the
// source had in fact already been wiped. A caller distinguishing the two the way
// the docs tell it to would have retried with a buffer of zeroes.
func TestNewProtectedShare_AllocationFailureIsNotValidation(t *testing.T) {
	restore := testOnlyProtectedMemory(refuseProtectedMemory)
	defer restore()

	value := []byte(base64.StdEncoding.EncodeToString([]byte{shareFormatVersion, 1, 0x42}))
	share, err := NewProtectedShare(1, 2, 3, value)

	if share != nil {
		t.Fatal("a refused allocation must not yield a share")
	}

	// The type is the contract.
	var pmErr *ProtectedMemoryError
	if !errors.As(err, &pmErr) {
		t.Fatalf("error = %T (%v), want *ProtectedMemoryError", err, err)
	}
	var vErr *ValidationError
	if errors.As(err, &vErr) {
		t.Fatal("a locked-memory refusal must NOT be reported as a *ValidationError")
	}
	if !errors.Is(err, errLockRefused) {
		t.Fatal("the allocator's own error must remain reachable through Unwrap")
	}

	// And the state that makes the type matter: the value really is gone.
	for i, b := range value {
		if b != 0 {
			t.Fatalf("byte %d = %#x: the source was not wiped, so the documented "+
				"consume-on-allocation-failure contract is wrong", i, b)
		}
	}
}

// TestNewProtectedShare_ValidationFailureLeavesSourceUntouched is the other half
// of the same contract, and the reason the two errors must stay distinct: a
// syntactic rejection happens BEFORE any locked memory is allocated, so the
// caller's buffer survives for it to wipe or retry.
func TestNewProtectedShare_ValidationFailureLeavesSourceUntouched(t *testing.T) {
	value := []byte("not valid base64 at all !!!")
	want := append([]byte(nil), value...)

	share, err := NewProtectedShare(1, 2, 3, value)
	if share != nil {
		t.Fatal("an invalid value must not yield a share")
	}
	var vErr *ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("error = %T (%v), want *ValidationError", err, err)
	}
	if !bytes.Equal(value, want) {
		t.Fatalf("the source was modified on a validation failure: %q", value)
	}
}

// TestSplitProtected_AllocationFailureDestroysPartialShares pins the leak fix on
// the path that can actually reach it. The refusal is injected after the first
// share has been built, so the function must tear down what it already has:
// those shares are locked memory the caller never receives and therefore can
// never Destroy itself.
func TestSplitProtected_AllocationFailureDestroysPartialShares(t *testing.T) {
	var built []*enclave.MemoryEnclave
	calls := 0

	restore := testOnlyProtectedMemory(func(data []byte) (*enclave.MemoryEnclave, error) {
		calls++
		if calls > 1 {
			goenclave.Wipe(data)
			return nil, errLockRefused
		}
		e, err := goenclave.NewMemoryEnclave(data)
		if err == nil {
			built = append(built, e)
		}
		return e, err
	})
	defer restore()

	shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
	if shares != nil {
		t.Fatal("a refused allocation must not yield shares")
	}
	var pmErr *ProtectedMemoryError
	if !errors.As(err, &pmErr) {
		t.Fatalf("error = %T (%v), want *ProtectedMemoryError", err, err)
	}

	if len(built) == 0 {
		t.Fatal("the injection never let a share be built, so this proves nothing")
	}
	for i, e := range built {
		if e.IsAlive() {
			t.Fatalf("share enclave %d survived the failure: SplitProtected leaked "+
				"locked memory the caller can never reach", i)
		}
	}
}

// TestTestOnlyProtectedMemory_RestoresPrevious pins the seam itself. A restore
// that did not restore would leave every later test in the package running
// against a stubbed allocator, which is the kind of failure that looks like a
// bug in whatever test happens to run next.
//
// The panic gate is not exercised: testing.Testing() is true by construction
// inside a test binary, so the production-refusal path is unreachable from here.
// It is the same gate, with the same limit, as testOnlyRandReader's.
func TestTestOnlyProtectedMemory_RestoresPrevious(t *testing.T) {
	restore := testOnlyProtectedMemory(refuseProtectedMemory)
	if _, err := newProtectedMemory([]byte("x")); !errors.Is(err, errLockRefused) {
		t.Fatal("the override did not take effect")
	}

	restore()

	e, err := newProtectedMemory([]byte("restored"))
	if err != nil {
		t.Fatalf("the previous allocator was not restored: %v", err)
	}
	e.Destroy()
}

// TestSplitProtected_BudgetCountsTheEncodingScratch pins MaxShareSetBytes as a
// true upper bound on the locked memory SplitProtected holds at its peak.
//
// It is a regression test for a real gap: the go-enclave port added a reused
// encoding scratch enclave (a share's frozen value is built by copying OUT of
// it, so both are live during the copy) and did not add it to splitLockedBytes.
// These exact dimensions are the boundary that exposed it — priced at 1,047,032
// bytes they were ADMITTED, while the true peak of 1,049,380 is over the
// 1,048,576 cap. Caught by Codex on PR #69.
//
// The parameters are deliberately hard-coded rather than derived: a test that
// recomputed the bound from the same helper it is checking would agree with any
// accounting, including a wrong one.
func TestSplitProtected_BudgetCountsTheEncodingScratch(t *testing.T) {
	const (
		secretLen = 1757
		threshold = 255
		total     = 255

		peakWithScratch    = 1049380 // true peak — must be refused
		peakWithoutScratch = 1047032 // the old accounting — was admitted
	)

	got := splitLockedBytes(secretLen, threshold, total)
	if got != peakWithScratch {
		t.Fatalf("splitLockedBytes(%d,%d,%d) = %d, want %d",
			secretLen, threshold, total, got, peakWithScratch)
	}
	if got <= MaxShareSetBytes {
		t.Fatalf("splitLockedBytes = %d, which fits MaxShareSetBytes (%d): the "+
			"encoding scratch is not counted, so the cap is not an upper bound",
			got, MaxShareSetBytes)
	}

	// The missing term is exactly one encoded value, and this says so, so a
	// future change that drops it fails here with a readable reason.
	encoded := base64.StdEncoding.EncodedLen(rawShareOverhead + secretLen)
	if got-encoded != peakWithoutScratch {
		t.Fatalf("the scratch term is %d, want one encoded value (%d)",
			got-peakWithoutScratch, encoded)
	}
	if peakWithoutScratch > MaxShareSetBytes {
		t.Fatal("the fixture no longer straddles the cap, so it proves nothing")
	}

	// And the refusal must land BEFORE any locked memory is allocated, which is
	// the whole point of pricing the operation up front.
	allocated := false
	restore := testOnlyProtectedMemory(func(data []byte) (*enclave.MemoryEnclave, error) {
		allocated = true
		return goenclave.NewMemoryEnclave(data)
	})
	defer restore()

	shares, err := SplitProtected(make([]byte, secretLen), threshold, total)
	if shares != nil {
		t.Fatal("an over-budget split must not return shares")
	}
	var se *SplitError
	if !errors.As(err, &se) {
		t.Fatalf("error = %T (%v), want *SplitError — an over-budget REQUEST is a "+
			"rejected request, not an exhausted host", err, err)
	}
	if allocated {
		t.Fatal("locked memory was allocated before the budget refused the request")
	}
}
