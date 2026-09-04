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

	"github.com/jeremyhahn/go-enclave"
)

// errLockRefused stands in for the kernel refusing an mlock.
var errLockRefused = errors.New("mlock refused")

// refuseProtectedMemory mimics go-enclave's failure contract exactly: it WIPES
// the source before reporting the refusal. A stub that skipped the wipe would
// make the test pass against an implementation that leaks the caller's bytes.
func refuseProtectedMemory(data []byte) (*enclave.MemoryEnclave, error) {
	enclave.Wipe(data)
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

	// The SIZED seam, not the from-bytes one: a share's value is now encoded
	// straight into locked pages and frozen there, so newProtectedMemory is no
	// longer on this path at all (go-enclave #6 removed the copy through a
	// scratch). Injecting on the old seam would refuse nothing and the test
	// would pass by never exercising the teardown.
	restore := testOnlySizedProtectedMemory(func(n int) (*enclave.MemoryEnclave, error) {
		calls++
		if calls > 1 {
			return nil, errLockRefused
		}
		e, err := enclave.NewMemoryEnclaveSized(n)
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

// TestSplitProtected_BudgetIsATrueUpperBound pins MaxShareSetBytes as a true
// upper bound on the locked memory SplitProtected holds at its peak, and pins
// the accounting change that moved that peak.
//
// HISTORY, because the numbers below only make sense with it. The go-enclave
// port could not freeze a sized enclave after filling it, so each share's frozen
// value was built by copying OUT of a reused writable encoding scratch — both
// regions live during the copy, so the scratch was part of the PEAK. It was
// missing from splitLockedBytes, and the gap was real rather than a rounding
// error: these exact dimensions priced at 1,047,032 were ADMITTED while the true
// peak was 1,049,380, over the 1,048,576 cap (caught by Codex on PR #69).
//
// go-enclave v0.5.0-alpha grew MemoryEnclave.Freeze, so a share value is encoded
// directly into the region it will keep and frozen there. The scratch is gone,
// the term that counted it is gone with it, and 1,047,032 is now BOTH the price
// and the true peak — so the same dimensions are admitted, correctly this time.
//
// The parameters and the expected figures are deliberately hard-coded rather
// than derived: a test that recomputed the bound from the helper it is checking
// would agree with any accounting, including a wrong one.
func TestSplitProtected_BudgetIsATrueUpperBound(t *testing.T) {
	const (
		secretLen = 1757
		threshold = 255
		total     = 255

		peakWithoutScratch = 1047032 // today's accounting AND today's true peak
		peakWithScratch    = 1049380 // what it cost while the scratch existed
	)

	got := splitLockedBytes(secretLen, threshold, total)
	if got != peakWithoutScratch {
		t.Fatalf("splitLockedBytes(%d,%d,%d) = %d, want %d",
			secretLen, threshold, total, got, peakWithoutScratch)
	}
	// One encoded value is the difference, and saying so here means a change that
	// reintroduces a scratch — or drops a term it should keep — fails with a
	// readable reason rather than an unexplained number.
	encoded := base64.StdEncoding.EncodedLen(rawShareOverhead + secretLen)
	if peakWithScratch-got != encoded {
		t.Fatalf("the retired scratch term is %d, want one encoded value (%d)",
			peakWithScratch-got, encoded)
	}
	if got > MaxShareSetBytes {
		t.Fatalf("splitLockedBytes = %d, over MaxShareSetBytes (%d): the fixture no "+
			"longer straddles the old boundary, so it proves nothing", got, MaxShareSetBytes)
	}

	// EVERY REGION THE SEAM HANDS OUT IS STILL ALIVE WHEN THE SPLIT RETURNS.
	// That is what "there is no scratch" means operationally, and it is the half
	// the arithmetic above cannot check: a transient region would be destroyed by
	// the time the caller sees the shares, and would be missing from the price.
	var handed []*enclave.MemoryEnclave
	restore := testOnlySizedProtectedMemory(func(n int) (*enclave.MemoryEnclave, error) {
		e, err := enclave.NewMemoryEnclaveSized(n)
		if err == nil {
			handed = append(handed, e)
		}
		return e, err
	})
	shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
	restore()
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer func() {
		for _, sh := range shares {
			sh.Destroy()
		}
	}()
	if len(handed) != len(shares) {
		t.Fatalf("the sized seam produced %d regions for %d shares: a transient "+
			"region is live at the peak and is not priced by splitLockedBytes",
			len(handed), len(shares))
	}
	for i, e := range handed {
		if !e.IsAlive() {
			t.Fatalf("region %d was destroyed before SplitProtected returned, so it "+
				"was a scratch: it is live at the peak and must be priced", i)
		}
	}
}

// TestSplitProtected_OverBudgetIsRefusedBeforeAllocating pins the other half of
// the bound: a request whose priced footprint exceeds MaxShareSetBytes is a
// rejected REQUEST, refused before a single locked page is taken.
//
// secretLen is the first length at these dimensions whose peak crosses the cap
// (1,048,817 > 1,048,576). It is one step past the fixture above, which is what
// makes the pair meaningful: the same shape, admitted on one side of the line
// and refused on the other.
func TestSplitProtected_OverBudgetIsRefusedBeforeAllocating(t *testing.T) {
	const (
		secretLen = 1760
		threshold = 255
		total     = 255
	)

	if got := splitLockedBytes(secretLen, threshold, total); got <= MaxShareSetBytes {
		t.Fatalf("splitLockedBytes(%d,%d,%d) = %d, which fits MaxShareSetBytes (%d): "+
			"the fixture no longer straddles the cap, so it proves nothing",
			secretLen, threshold, total, got, MaxShareSetBytes)
	}

	allocated := false
	restore := testOnlySizedProtectedMemory(func(n int) (*enclave.MemoryEnclave, error) {
		allocated = true
		return enclave.NewMemoryEnclaveSized(n)
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
