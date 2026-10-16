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
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// errLockRefused stands in for the kernel refusing an mlock.
var errLockRefused = errors.New("mlock refused")

// refuseProtectedMemory mimics go-enclave's failure contract exactly: it WIPES
// the source before reporting the refusal. A stub that skipped the wipe would
// make the test pass against an implementation that leaks the caller's bytes.
func refuseProtectedMemory(data []byte) (enclave.ByteAccess, error) {
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
	var built []enclave.ByteAccess
	calls := 0

	// The SIZED seam, not the from-bytes one: a share's value is encoded
	// straight into locked pages and frozen there, so newProtectedMemory is not
	// on this path at all. Injecting on that seam would refuse nothing and the
	// test would pass by never exercising the teardown.
	restore := testOnlySizedProtectedMemory(func(n int) (enclave.ByteAccess, error) {
		calls++
		if calls > 1 {
			return nil, errLockRefused
		}
		e, err := enclave.NewMemorySized(n)
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

// TestSplitProtected_BudgetCountsEveryLiveRegion pins the arithmetic of
// splitLockedBytes term by term, and pins that every region the seam hands out
// is still alive when the split returns.
//
// It does not claim a "true upper bound": this test counts REGIONS, and a budget
// can count regions correctly while pricing them wrong by a factor of 680. That
// claim needs a measurement of what the kernel charged, and lives in the VmLck
// test in protected_share_lockedmem_linux_test.go.
//
// A share value is encoded directly into the region it will keep and frozen
// there, so there is no reused writable encoding scratch, and splitLockedBytes
// has no term for one. A scratch that a frozen value was copied OUT of would be
// live alongside it during the copy, and therefore part of the PEAK.
//
// Under page rounding these dimensions cost
// 774,144 + 4,096 + 65,536 + 13x90,112 = 2,015,232, and the fixture straddles
// the cap in the direction that matters: it is ADMITTED, and one more
// page-rounded encoded value (90,112) — the price of reintroducing a scratch —
// would carry it to 2,105,344 and a refusal.
//
// The parameters and the expected figures are deliberately hard-coded rather
// than derived: a test that recomputed the bound from the helper it is checking
// would agree with any accounting, including a wrong one.
func TestSplitProtected_BudgetCountsEveryLiveRegion(t *testing.T) {
	// The figures below are derived for a 4 KiB page, exactly as in the tests
	// that follow. Read from the OS, never from lockedBytes: a guard phrased in
	// the helper under test skips itself under the mutation it exists to catch.
	if page := os.Getpagesize(); page != 4096 {
		t.Skipf("hard-coded figures assume a 4 KiB page, host reports %d", page)
	}

	const (
		secretLen = 64511
		threshold = 13
		total     = 13

		peakWithoutScratch = 2015232 // the accounting AND the true peak
		peakWithScratch    = 2105344 // the cost if an encoding scratch were added
	)

	got := splitLockedBytes(secretLen, threshold, total)
	if got != peakWithoutScratch {
		t.Fatalf("splitLockedBytes(%d,%d,%d) = %d, want %d",
			secretLen, threshold, total, got, peakWithoutScratch)
	}
	// One PAGE-ROUNDED encoded value is the difference, and saying so here means a
	// change that reintroduces a scratch — or drops a term it should keep — fails
	// with a readable reason rather than an unexplained number. Page-rounded
	// because that is the unit the region is charged in: a scratch would be a
	// whole enclave, costing a whole number of pages, not 86,020 bytes.
	encoded := lockedBytes(base64.StdEncoding.EncodedLen(rawShareOverhead + secretLen))
	if peakWithScratch-got != encoded {
		t.Fatalf("the retired scratch term is %d, want one page-rounded encoded value (%d)",
			peakWithScratch-got, encoded)
	}
	if got > MaxShareSetBytes {
		t.Fatalf("splitLockedBytes = %d, over MaxShareSetBytes (%d): the fixture no "+
			"longer straddles the old boundary, so it proves nothing", got, MaxShareSetBytes)
	}

	// EVERY REGION THE SEAM HANDS OUT IS ACCOUNTED FOR — which is the half the
	// arithmetic above cannot check, because an allocation missing from
	// splitLockedBytes is still live at the peak and still counts against
	// RLIMIT_MEMLOCK.
	//
	// The seam sees all of them, in allocation order: SplitProtected's three
	// pre-loop buffers — the coefficient pool, the coefficient vector and the
	// raw share payload — then one region per share. A buffer allocated around
	// the seam would be invisible here, and its refusal path untestable.
	//
	// The three ARE transient — each is deferred-Destroy'd — and each is PRICED,
	// as `coefficients + scratch` in splitLockedBytes.
	//
	// So the assertion is the SUM, not the count. Every byte the seam hands out
	// is added up and compared against splitLockedBytes for the same dimensions:
	// an allocation that is unpriced, priced twice, or priced at the wrong size
	// breaks it, and a count alone breaks for none of those. (A headcount plus a
	// comment naming the buffers would prove only that three of something were
	// allocated — the names would live in the comment and nowhere else.)
	const preLoopRegions = 3

	var (
		handed    []enclave.ByteAccess
		handedSum int
	)
	restore := testOnlySizedProtectedMemory(func(n int) (enclave.ByteAccess, error) {
		e, err := enclave.NewMemorySized(n)
		if err == nil {
			handed = append(handed, e)
			handedSum += lockedBytes(n)
		}
		return e, err
	})
	secret := []byte("a-secret-worth-splitting-32-byte")
	shares, err := SplitProtected(secret, 2, 3)
	restore()
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer func() {
		for _, sh := range shares {
			sh.Destroy()
		}
	}()
	if len(handed) != preLoopRegions+len(shares) {
		t.Fatalf("the sized seam produced %d regions for %d shares, want %d + %d: "+
			"an unpriced region is live at the peak, or a priced one no longer "+
			"goes through the seam", len(handed), len(shares), preLoopRegions, len(shares))
	}
	// THE ACCOUNTING PROOF. 32-byte secret, 2-of-3 is six regions -- coefficient
	// pool (32), coefficient vector (2), raw payload (34) and three encoded
	// values (48 each) -- and the kernel charges each one a whole page, so the
	// price is 6 pages, not 212 bytes. The left side applies the same
	// per-region rounding to the sizes the allocator was ACTUALLY asked for;
	// the right side is the helper under test. Both round, neither shares the
	// other's arithmetic, so agreement is a real check rather than a tautology:
	// an allocation that is unpriced, double-priced, or priced at the wrong size
	// still moves one side and not the other.
	if want := splitLockedBytes(len(secret), 2, 3); handedSum != want {
		t.Fatalf("the seam was asked for %d bytes but splitLockedBytes prices %d: "+
			"an allocation is unpriced, double-priced, or priced at the wrong size",
			handedSum, want)
	}
	for i, e := range handed[:preLoopRegions] {
		if e.IsAlive() {
			t.Fatalf("pre-loop region %d outlived SplitProtected: it is priced as "+
				"transient scratch, so holding it past the return understates the "+
				"caller's steady-state locked-memory cost", i)
		}
	}
	for i, e := range handed[preLoopRegions:] {
		if !e.IsAlive() {
			t.Fatalf("share region %d was destroyed before SplitProtected returned, "+
				"so it was a scratch: it is live at the peak and must be priced", i)
		}
	}
}

// TestSplitProtected_PreLoopAllocationRefusalsAreClean covers the refusal paths
// of SplitProtected's three setup allocations. Each can be refused by the
// kernel, and routing them through newSizedProtectedMemory is what makes those
// paths reachable from a test.
//
// The property is that a refusal at ANY of them is a *ProtectedMemoryError
// naming the buffer — not a *SplitError, and not a partially built share set the
// caller has to guess the ownership of.
func TestSplitProtected_PreLoopAllocationRefusalsAreClean(t *testing.T) {
	// One case per pre-loop allocation, identified by the index of the seam call
	// to refuse: 0 the coefficient pool, 1 the coefficient vector, 2 the raw
	// share payload.
	for _, tc := range []struct {
		name    string
		refuseN int
		wantOp  string
	}{
		{"coefficient pool", 0, "locking coefficient memory"},
		{"coefficient vector", 1, "locking coefficient vector"},
		{"share payload", 2, "locking share payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			restore := testOnlySizedProtectedMemory(func(n int) (enclave.ByteAccess, error) {
				defer func() { calls++ }()
				if calls == tc.refuseN {
					return nil, errLockRefused
				}
				return enclave.NewMemorySized(n)
			})
			shares, err := SplitProtected([]byte("a-secret-worth-splitting-32-byte"), 2, 3)
			restore()

			if shares != nil {
				for _, sh := range shares {
					sh.Destroy()
				}
				t.Fatal("SplitProtected returned shares alongside a refused allocation")
			}
			if !errors.Is(err, errLockRefused) {
				t.Fatalf("error = %v, want it to wrap the allocator's refusal", err)
			}
			var pmErr *ProtectedMemoryError
			if !errors.As(err, &pmErr) {
				t.Fatalf("error = %T, want a *ProtectedMemoryError: a refused lock is "+
					"not a caller's validation mistake", err)
			}
			if pmErr.Op != tc.wantOp {
				t.Fatalf("ProtectedMemoryError.Op = %q, want %q", pmErr.Op, tc.wantOp)
			}
		})
	}
}

// TestCombineProtected_AllocationRefusalsAreClean is the same coverage on the
// reconstruction side: both of CombineProtected's regions — the base64 decode
// scratch and the enclave holding the reconstructed secret — go through the
// sized seam.
//
// The secret buffer is the one that matters. It is not a scratch: it is what the
// function RETURNS, so a refusal there must leave nothing for the caller to own
// and must not surface as a *CombineError, which would read as "these shares do
// not reconstruct" over a kernel that would not give out a page.
func TestCombineProtected_AllocationRefusalsAreClean(t *testing.T) {
	secret := []byte("a-secret-worth-splitting-32-byte")
	shares, err := SplitProtected(secret, 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	defer func() {
		for _, sh := range shares {
			sh.Destroy()
		}
	}()

	for _, tc := range []struct {
		name    string
		refuseN int
		wantOp  string
	}{
		{"decode scratch", 0, "locking decode buffer"},
		{"reconstructed secret", 1, "locking secret buffer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			restore := testOnlySizedProtectedMemory(func(n int) (enclave.ByteAccess, error) {
				defer func() { calls++ }()
				if calls == tc.refuseN {
					return nil, errLockRefused
				}
				return enclave.NewMemorySized(n)
			})
			got, err := CombineProtected(shares[:2])
			restore()

			if got != nil {
				got.Destroy()
				t.Fatal("CombineProtected returned a secret alongside a refused allocation")
			}
			if !errors.Is(err, errLockRefused) {
				t.Fatalf("error = %v, want it to wrap the allocator's refusal", err)
			}
			var pmErr *ProtectedMemoryError
			if !errors.As(err, &pmErr) {
				t.Fatalf("error = %T, want a *ProtectedMemoryError: a refused lock is "+
					"not a reconstruction failure", err)
			}
			if pmErr.Op != tc.wantOp {
				t.Fatalf("ProtectedMemoryError.Op = %q, want %q", pmErr.Op, tc.wantOp)
			}
		})
	}
}

// TestSplitProtected_OverBudgetIsRefusedBeforeAllocating pins the other half of
// the bound: a request whose priced footprint exceeds MaxShareSetBytes is a
// rejected REQUEST, refused before a single locked page is taken.
//
// secretLen is the first length at these dimensions whose peak crosses the cap
// under page-rounded accounting. At 3,071 bytes the encoded share does not fit
// one page, so all 255 values take a second one and the peak jumps to
// 2,879,488, over the 2,097,152 cap — while 3,070 is admitted at 1,835,008. The
// pair is meaningful because it is the same shape, admitted on one side of the
// line and refused on the other. TestSplitProtected_AdmissionBoundaryIsPageRounded
// pins both sides and the size of the step; this pins that the refusal costs no
// locked memory.
func TestSplitProtected_OverBudgetIsRefusedBeforeAllocating(t *testing.T) {
	const (
		secretLen = 3071
		threshold = 255
		total     = 255
	)

	if got := splitLockedBytes(secretLen, threshold, total); got <= MaxShareSetBytes {
		t.Fatalf("splitLockedBytes(%d,%d,%d) = %d, which fits MaxShareSetBytes (%d): "+
			"the fixture no longer straddles the cap, so it proves nothing",
			secretLen, threshold, total, got, MaxShareSetBytes)
	}

	allocated := false
	restore := testOnlySizedProtectedMemory(func(n int) (enclave.ByteAccess, error) {
		allocated = true
		return enclave.NewMemorySized(n)
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

// TestSplitLockedBytes_PricesEveryRegionInPages pins the page-rounded accounting
// term by term, against figures worked out by hand rather than recomputed from
// the helper under test.
//
// The regions SplitProtected locks are separate enclaves, and the kernel charges
// each a whole page. Pricing the SUM of the requested sizes and rounding once —
// or not rounding at all — under-counts by nearly a page per share, which for a
// 255-share set is the entire figure: a one-byte secret split 255 ways priced
// that way comes to 1,536 bytes and locks 1,044,480.
//
// The at-rest column is what a caller holds after the split returns (the four
// transient regions are destroyed on the way out), so it is exactly one page per
// share and is what the measured witness in the _linux test compares against.
func TestSplitLockedBytes_PricesEveryRegionInPages(t *testing.T) {
	// The skip guard reads the page size from the OS, NOT from lockedBytes:
	// a guard phrased in terms of the helper under test skips itself the moment
	// that helper stops rounding, which is exactly the mutation these figures
	// exist to catch.
	page := os.Getpagesize()
	if page != 4096 {
		t.Skipf("hard-coded figures assume a 4 KiB page, host reports %d", page)
	}
	if got := lockedBytes(1); got != page {
		t.Fatalf("lockedBytes(1) = %d, want one page (%d): allocations are not "+
			"being priced in the unit the kernel charges", got, page)
	}

	for _, tc := range []struct {
		name                        string
		secretLen, threshold, total int
		peak                        int
		atRest                      int
	}{
		{
			// Priced at 1,536 bytes by byte-summed accounting, which is 680x
			// under what the kernel charges.
			name:      "255 shares of a one-byte secret",
			secretLen: 1, threshold: 255, total: 255,
			peak:   1056768, // 258 pages: 255 values + coefficients + vector + raw payload
			atRest: 1044480, // 255 pages: one per share value
		},
		{
			// The headline case: a full 255-share split of a root key. 259 pages
			// is over 256 (1 MiB), which is why MaxShareSetBytes is 512 pages.
			name:      "255 shares of a 32-byte root key",
			secretLen: 32, threshold: 255, total: 255,
			peak:   1060864,
			atRest: 1044480,
		},
		{
			name:      "5-of-5 of the largest secret the package accepts",
			secretLen: MaxSecretBytes, threshold: 5, total: 5,
			peak:   786432,
			atRest: 450560,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := splitLockedBytes(tc.secretLen, tc.threshold, tc.total)
			if got != tc.peak {
				t.Fatalf("splitLockedBytes(%d,%d,%d) = %d, want %d",
					tc.secretLen, tc.threshold, tc.total, got, tc.peak)
			}
			if got > MaxShareSetBytes {
				t.Fatalf("priced at %d, over MaxShareSetBytes (%d): a case the "+
					"primitive exists to serve must be admitted",
					got, MaxShareSetBytes)
			}
			encoded := base64.StdEncoding.EncodedLen(rawShareOverhead + tc.secretLen)
			if resident := tc.total * lockedBytes(encoded); resident != tc.atRest {
				t.Fatalf("at-rest cost = %d, want %d", resident, tc.atRest)
			}
			if got < tc.atRest {
				t.Fatal("the peak is below what the call leaves resident")
			}
		})
	}
}

// TestSplitProtected_AdmissionBoundaryIsPageRounded pins where the cap now
// falls, and pins that the step is the PAGE and not the byte.
//
// A 3070-byte secret is the largest whose encoded share still fits one page.
// One byte more and every one of the 255 values needs a second page, so the
// footprint jumps by 255 pages at a stroke — from 1,835,008 to 2,879,488 — and
// crosses the cap. Byte-summed accounting sees the same step as 1,279 bytes and
// admits both, which is the whole byte-summing defect stated as a boundary.
//
// The refusal-allocates-nothing half lives in
// TestSplitProtected_OverBudgetIsRefusedBeforeAllocating, which shares this
// fixture; this test owns the arithmetic of both sides and the size of the step.
func TestSplitProtected_AdmissionBoundaryIsPageRounded(t *testing.T) {
	// From the OS, not from lockedBytes — see the note in
	// TestSplitLockedBytes_PricesEveryRegionInPages.
	if page := os.Getpagesize(); page != 4096 {
		t.Skipf("hard-coded figures assume a 4 KiB page, host reports %d", page)
	}
	if got := lockedBytes(1); got != 4096 {
		t.Fatalf("lockedBytes(1) = %d, want one page: allocations are not being "+
			"priced in the unit the kernel charges", got)
	}

	const (
		admitted = 3070
		refused  = 3071

		admittedPrice = 1835008
		refusedPrice  = 2879488
	)

	if got := splitLockedBytes(admitted, maxShares, maxShares); got != admittedPrice {
		t.Fatalf("splitLockedBytes(%d,255,255) = %d, want %d", admitted, got, admittedPrice)
	} else if got > MaxShareSetBytes {
		t.Fatalf("the largest one-page-per-share split (%d bytes) is refused at %d", got, MaxShareSetBytes)
	}
	if got := splitLockedBytes(refused, maxShares, maxShares); got != refusedPrice {
		t.Fatalf("splitLockedBytes(%d,255,255) = %d, want %d", refused, got, refusedPrice)
	} else if got <= MaxShareSetBytes {
		t.Fatalf("a two-page-per-share split prices at %d and fits %d: the cap no "+
			"longer sits at the edge its doc comment claims", got, MaxShareSetBytes)
	}

	// One byte of secret decides it. That the refusal then costs no locked memory
	// is TestSplitProtected_OverBudgetIsRefusedBeforeAllocating's assertion, on
	// this same fixture, rather than a second copy here.
}

// TestUnmarshalShareSet_BoundIsPageRoundedPerShareValue pins the ingestion
// path's bound to the same unit, and the same object, as SplitProtected's.
//
// A share set arriving as JSON is one enclave per element, so 255 small shares
// lock 255 PAGES — around 1 MiB — while their document is about 25 KiB. Bounding
// the raw document length would therefore price an unauthenticated submission
// at a fortieth of what it takes.
//
// The unit is the page; the OBJECT is the share's value, not its element. That
// second half is what keeps this bound and splitLockedBytes' comparable, and
// TestShareSet_RoundTripsAtTheAdmissionBoundary is where it is exercised
// end to end.
func TestUnmarshalShareSet_BoundIsPageRoundedPerShareValue(t *testing.T) {
	// From the OS, not from lockedBytes — see the note in
	// TestSplitLockedBytes_PricesEveryRegionInPages.
	page := os.Getpagesize()
	if page != 4096 {
		t.Skipf("hard-coded figures assume a 4 KiB page, host reports %d", page)
	}
	if got := lockedBytes(1); got != page {
		t.Fatalf("lockedBytes(1) = %d, want one page (%d)", got, page)
	}

	shares, err := SplitProtected(make([]byte, 32), maxShares, maxShares)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	doc, err := json.Marshal(shares)
	for _, s := range shares {
		s.Destroy()
	}
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(doc, &raw); err != nil {
		t.Fatalf("Unmarshal into elements: %v", err)
	}
	plain := 0
	for _, elem := range raw {
		plain += len(elem)
	}
	// From the guard's own function, not a sum recomputed here.
	paged := shareSetLockedBytes(raw)
	if want := maxShares * page; paged != want {
		t.Fatalf("page-rounded set cost = %d, want %d (one page per share)", paged, want)
	}
	if plain*8 > paged {
		t.Fatalf("the fixture no longer separates the two accountings: raw %d vs "+
			"page-rounded %d", plain, paged)
	}

	// The set is legal and must decode: this is the primitive's headline case,
	// and it is the case the re-derived MaxShareSetBytes exists to admit.
	decoded, err := UnmarshalShareSet(doc)
	if err != nil {
		t.Fatalf("UnmarshalShareSet of a 255-share root-key set: %v", err)
	}
	if len(decoded) != maxShares {
		t.Fatalf("decoded %d shares, want %d", len(decoded), maxShares)
	}
	for _, s := range decoded {
		s.Destroy()
	}

	// And the mutation the bound has to survive: a set whose page-rounded cost
	// is over the cap while its RAW length is under it. Each value is sized one
	// token past two pages, so it costs three, and 171 of them are 2,101,248
	// locked bytes against 1,409,040 raw — admitted by raw-length accounting,
	// refused by page-rounded accounting.
	//
	// 171 rather than 255: pricing the VALUE leaves only the rounding slack to
	// separate the two sums, and at 255 elements a value big enough to need a
	// third page already carries the raw sum past the cap on its own, which
	// would prove nothing.
	const (
		elems    = 171
		valueLen = 8196 // 2*4096 + 4, a whole number of base64 quanta
	)
	prefix := `{"index":1,"threshold":2,"total":255,"value":"`
	const suffix = `"}`
	elem := prefix + strings.Repeat("A", valueLen) + suffix
	if got := lockedBytes(valueLen); got != 3*page {
		t.Fatalf("fixture value is %d bytes, which rounds to %d not %d: it no "+
			"longer separates the two accountings", valueLen, got, 3*page)
	}

	over := "[" + strings.Repeat(elem+",", elems-1) + elem + "]"
	rawSum := elems * len(elem)
	if rawSum > MaxShareSetBytes {
		t.Fatalf("fixture is %d raw bytes, which the byte-summed bound would also "+
			"have refused: it proves nothing", rawSum)
	}
	if _, err := UnmarshalShareSet([]byte(over)); err == nil {
		t.Fatalf("a set of %d raw bytes that locks %d was admitted under a "+
			"%d-byte budget", rawSum, elems*lockedBytes(valueLen), MaxShareSetBytes)
	}
}

// TestShareSet_RoundTripsWhateverTheSplitAdmits pins the invariant a ceremony
// depends on: anything SplitProtected admits, UnmarshalShareSet admits.
//
// The two guards price different operations — a split's peak includes the
// coefficients and the two reused scratches, a decode's is one enclave per share
// — so nothing about the two arithmetics forces them to agree. A disagreement
// does not look like a bug from outside; it looks like a legitimate recovery
// being refused, on a set the library itself produced, at a ceremony.
//
// Pricing a decode by the ELEMENT breaks it. A share's element carries ~48
// bytes of JSON header, and when the encoded value lands on an exact page
// multiple that header pushes every element into a further page: 171 shares of
// a 6142-byte secret would split at 1,429,504 and decode at 2,101,248, so the
// split would be admitted and the decode refused. Pricing the VALUE makes the
// decode sum exactly splitLockedBytes' per-share term, which that function then
// adds four positive terms to — so the invariant is structural rather than
// lucky, and this walks the space to say so rather than pinning one point.
//
// # Why the sweep is shaped the way it is
//
// A split costs secretLen x threshold x total byte operations, and `CI / Test`
// runs `go test -race -count=1 -timeout=900s -coverprofile=...`. Race and
// coverage instrumentation together are worth one to two orders of magnitude, so
// the first cut of this sweep — every secret length against threshold=total as
// well as threshold=2 — took ~40s bare and blew the 900s package budget in CI,
// timing out inside a single (3068, 255, 255) split.
//
// The trim is chosen so it cannot cost a witness rather than to hit a number.
// A HIGH threshold makes a split MORE expensive to admit, because
// splitLockedBytes charges pageRound(secretLen*(threshold-1)) for the
// coefficients — so the combinations at risk of being admitted here and refused
// on ingest are the LOW-threshold ones, and those are exactly what the expensive
// half was not contributing. Checked rather than assumed: sweeping
// secretLen 1..13000 against eight share counts and four thresholds finds 417
// combinations an element-priced decode would refuse, 150 of them at
// threshold=2, and NONE at any secret length at or below 64 bytes. So the sweep
// runs every secret length at threshold=2, and threshold=total only for the
// small secrets where the 255-share root-key case lives and where no witness
// exists to lose. Three witnesses survive the trim — (6140, 2, 171),
// (6142, 2, 171) and (9214, 2, 129) — the first being the one the regression was
// found on, and the mutation is verified to still fail there.
//
// The share COUNTS are trimmed on the same principle. A combination's cost is
// dominated by the number of enclaves it maps, not by the GF(256) arithmetic, so
// dropping two of the seven totals halved the run without touching a witness:
// measured under the CI invocation, this test went 152s -> 82s and the package
// 285s -> ~204s against a 900s budget.
func TestShareSet_RoundTripsWhateverTheSplitAdmits(t *testing.T) {
	// Sizes that bracket every page boundary an encoded share can land on,
	// including the exact multiples that produced the regression.
	secretLens := []int{1, 32, 64, 3068, 3070, 3071, 6140, 6142, 6143, 9214, 12286}
	totals := []int{2, 5, 129, 171, maxShares}

	// A split of a large secret at a high threshold is the expensive corner and
	// the one that cannot hold a witness; see the note above.
	const wideThresholdSecretLimit = 64

	tested, admitted := 0, 0
	for _, secretLen := range secretLens {
		for _, total := range totals {
			thresholds := []int{2}
			if secretLen <= wideThresholdSecretLimit && total != 2 {
				thresholds = append(thresholds, total)
			}
			for _, threshold := range thresholds {
				if threshold > total || threshold < 2 {
					continue
				}
				tested++
				if splitLockedBytes(secretLen, threshold, total) > MaxShareSetBytes {
					continue // refused up front; nothing to round-trip
				}

				shares, err := SplitProtected(make([]byte, secretLen), threshold, total)
				if err != nil {
					t.Fatalf("SplitProtected(%d,%d,%d) priced under the cap but "+
						"failed: %v", secretLen, threshold, total, err)
				}
				doc, err := json.Marshal(shares)
				for _, s := range shares {
					s.Destroy()
				}
				if err != nil {
					t.Fatalf("Marshal(%d,%d,%d): %v", secretLen, threshold, total, err)
				}

				decoded, err := UnmarshalShareSet(doc)
				if err != nil {
					t.Fatalf("SplitProtected(%d,%d,%d) was admitted at %d bytes, "+
						"but the set it produced cannot be read back: %v\n"+
						"the split and ingestion bounds disagree, so a ceremony "+
						"that distributes shares as JSON cannot collect them again",
						secretLen, threshold, total,
						splitLockedBytes(secretLen, threshold, total), err)
				}
				if len(decoded) != total {
					t.Fatalf("decoded %d shares, want %d", len(decoded), total)
				}
				for _, s := range decoded {
					s.Destroy()
				}
				admitted++
			}
		}
	}

	// A run in which the split refused everything would pass vacuously.
	if admitted == 0 {
		t.Fatalf("no combination out of %d was admitted, so nothing round-tripped", tested)
	}
	t.Logf("%d of %d combinations admitted and round-tripped", admitted, tested)
}
