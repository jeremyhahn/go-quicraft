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

// refuseProtectedMemory mimics go-enclave's failure contract: it wipes the source before refusing.
func refuseProtectedMemory(data []byte) (enclave.ByteAccess, error) {
	enclave.Wipe(data)
	return nil, errLockRefused
}

// TestNewProtectedShare_AllocationFailureIsNotValidation pins that a locked-memory refusal is not a *ValidationError.
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

// TestSplitProtected_AllocationFailureDestroysPartialShares pins the leak fix on
// the path that can actually reach it. The refusal is injected after the first
// share has been built, so the function must tear down what it already has:
// those shares are locked memory the caller never receives and therefore can
// never Destroy itself.
func TestSplitProtected_AllocationFailureDestroysPartialShares(t *testing.T) {
	var built []enclave.ByteAccess
	calls := 0

	// Inject on the sized seam: share values are encoded straight into locked pages.
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

// TestTestOnlyProtectedMemory_RestoresPrevious pins that the seam restores the previous allocator.
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

// TestSplitProtected_BudgetCountsEveryLiveRegion pins splitLockedBytes term by term against hard-coded figures and counts every live region.
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
	// One page-rounded encoded value is the margin; a reintroduced scratch fails here.
	encoded := lockedBytes(base64.StdEncoding.EncodedLen(rawShareOverhead + secretLen))
	if peakWithScratch-got != encoded {
		t.Fatalf("the retired scratch term is %d, want one page-rounded encoded value (%d)",
			peakWithScratch-got, encoded)
	}
	if got > MaxShareSetBytes {
		t.Fatalf("splitLockedBytes = %d, over MaxShareSetBytes (%d): the fixture no "+
			"longer straddles the old boundary, so it proves nothing", got, MaxShareSetBytes)
	}

	// The sum of every region the seam hands out must equal splitLockedBytes for the same dimensions.
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
	// A 32-byte 2-of-3 split is six page-rounded regions; both sides round independently.
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

// TestSplitProtected_PreLoopAllocationRefusalsAreClean verifies a refused setup allocation is a *ProtectedMemoryError naming the buffer.
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

// TestCombineProtected_AllocationRefusalsAreClean verifies refused decode or secret allocations are *ProtectedMemoryError, not *CombineError.
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

// TestSplitProtected_OverBudgetIsRefusedBeforeAllocating pins that a request priced over MaxShareSetBytes is refused before any page is locked.
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

// TestSplitLockedBytes_PricesEveryRegionInPages pins page-rounded accounting per region against hand-worked figures.
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

// TestSplitProtected_AdmissionBoundaryIsPageRounded pins that 3070 bytes is admitted and 3071 refused, a step of 255 pages.
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

// TestUnmarshalShareSet_BoundIsPageRoundedPerShareValue pins the ingestion bound to page-rounded share values, as in SplitProtected.
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

	// 171 values each one token past two pages: under the cap by raw length, over it page-rounded.
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

// TestShareSet_RoundTripsWhateverTheSplitAdmits pins that UnmarshalShareSet admits anything SplitProtected admits, sweeping every length at threshold 2 within the CI time budget.
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
