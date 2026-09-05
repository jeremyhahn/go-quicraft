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
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

// vmLockedBytes reports this process's currently mlock'd memory, read from
// /proc/self/status.
//
// It is the KERNEL's accounting, not the library's, and that is the entire point
// of the tests below. Every other witness available here — a count of the
// enclaves handed out, a sum of the sizes asked for, splitLockedBytes itself —
// is derived from the same code the accounting under test lives in, so it agrees
// with a wrong answer as readily as a right one. VmLck cannot be fooled by it.
//
// It skips rather than failing where the interface is unavailable, matching how
// pkg/seal's locked-memory tests degrade.
func vmLockedBytes(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skipf("cannot read /proc/self/status: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "VmLck:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.Atoi(fields[1])
		if err != nil {
			break
		}
		return kb * 1024
	}
	t.Skip("VmLck is not reported by this kernel")
	return 0
}

// warmLockedMemory takes the one-time locked allocations out of a later
// measurement: the Go runtime and go-enclave may each lock pages on first use,
// and a first-call cost inside the measured window reads as a bookkeeping gap
// that is not there.
func warmLockedMemory(t *testing.T) {
	t.Helper()
	shares, err := SplitProtected(make([]byte, 32), 2, 3)
	if err != nil {
		t.Skipf("locked memory is unavailable on this host: %v", err)
	}
	for _, s := range shares {
		s.Destroy()
	}
}

// slackBytes is the allowance for locked allocations this process makes for
// reasons unrelated to the call being measured. VmLck is process-wide, and these
// tests share a binary with everything else in the package.
//
// It is 64 KiB — sixteen pages, well above any incidental allocation and two
// orders of magnitude below the ~1 MiB gap that the defect these tests exist for
// produced. It buys robustness without buying the bug back.
const slackBytes = 64 << 10

// TestSplitProtected_BudgetIsATrueUpperBound is the witness that splitLockedBytes
// prices what the KERNEL charges.
//
// This is the test issue #94 exists because of. Its predecessor asserted the
// same property by name and could not detect the gap, because everything it
// measured came from the accounting under test: the price said 1,536 bytes, the
// kernel charged 1,044,480, and nothing in the assertion could tell the
// difference. go-enclave rounds every allocation up to a whole page
// (mapping.go), a 255-share split takes 255 SEPARATE allocations, and the
// per-share rounding — not the secret — is then the whole figure.
//
// So the assertion is against VmLck. It is read either side of a split whose
// parameters the budget ADMITS, which is the case that matters: an admitted call
// is one the guard has promised fits, and the promise is only worth having if
// what the kernel charges is no more than what was priced.
//
// MUTATION CHECK. Drop the page rounding from splitLockedBytes — price the terms
// in requested bytes, as the code did before this issue — and the 255-share case
// prices at 1,536 against a measured 1,044,480, and this fails. Restore it and it
// passes. That is the property; the count of regions handed out is not.
func TestSplitProtected_BudgetIsATrueUpperBound(t *testing.T) {
	warmLockedMemory(t)

	for _, tc := range []struct {
		name                        string
		secretLen, threshold, total int
	}{
		// The issue's own measurement: the per-share page rounding is 100% of
		// the true cost and 0% of the old price.
		{"255 shares of a one-byte secret", 1, maxShares, maxShares},
		// The case the primitive exists for.
		{"255 shares of a 32-byte root key", 32, maxShares, maxShares},
		// The opposite shape — few shares, large secret — where the per-share
		// rounding is a rounding error rather than the whole figure. It is here
		// so the bound is witnessed across the range and not only where the
		// defect was loudest; it does NOT detect the mutation on its own, since
		// a byte-summed price of a large secret still exceeds five pages.
		{"5-of-5 of the largest secret the package accepts", MaxSecretBytes, 5, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			priced := splitLockedBytes(tc.secretLen, tc.threshold, tc.total)
			if priced > MaxShareSetBytes {
				t.Fatalf("priced at %d, over MaxShareSetBytes (%d): this test must "+
					"measure a call the budget ADMITS", priced, MaxShareSetBytes)
			}

			before := vmLockedBytes(t)
			shares, err := SplitProtected(make([]byte, tc.secretLen), tc.threshold, tc.total)
			if err != nil {
				t.Fatalf("SplitProtected: %v", err)
			}
			// Read with the shares still held. The transient regions —
			// coefficients, vector, payload, scratch — are destroyed by the
			// deferred teardown on the way out, so this is the resident part of
			// the peak: one page per returned share, and a lower bound on the
			// peak the price is meant to cover.
			after := vmLockedBytes(t)
			measured := after - before

			if measured == 0 {
				for _, s := range shares {
					s.Destroy()
				}
				t.Skip("VmLck did not move across a successful split: this host is " +
					"not charging locked memory, so there is nothing to witness")
			}
			// Every returned share is its own allocation and every allocation is
			// at least a page, so this is what the kernel must have charged. It
			// also refuses to let the test pass on a measurement that did not
			// happen.
			// os.Getpagesize(), NOT lockedBytes(1): a floor phrased in the helper
			// under test collapses along with it under the very mutation this
			// test exists to survive.
			if want := tc.total * os.Getpagesize(); measured < want {
				t.Fatalf("VmLck moved %d bytes across a %d-share split, below the "+
					"%d bytes %d separate allocations must cost: the measurement "+
					"did not capture the call", measured, tc.total, want, tc.total)
			}

			if measured > priced+slackBytes {
				t.Fatalf("splitLockedBytes(%d,%d,%d) priced %d bytes and the kernel "+
					"charged %d (VmLck %d -> %d, slack %d): the budget is not an "+
					"upper bound, so MaxShareSetBytes cannot be compared to "+
					"RLIMIT_MEMLOCK — see issue #94",
					tc.secretLen, tc.threshold, tc.total, priced, measured,
					before, after, slackBytes)
			}

			for _, s := range shares {
				s.Destroy()
			}
			// And the charge is returned, so the bound describes a peak rather
			// than a permanent cost.
			if leaked := vmLockedBytes(t) - before; leaked > slackBytes {
				t.Fatalf("%d bytes of locked memory survived destroying every share", leaked)
			}
		})
	}
}

// TestUnmarshalShareSet_BudgetIsATrueUpperBound is the same witness on the
// ingestion path, which is the one an unauthenticated submission reaches.
//
// UnmarshalShareSet builds one enclave per element, so a 255-share document of
// small shares locks 255 PAGES while its JSON is around 25 KiB. Bounding the raw
// document length therefore let a submission take about forty times what the
// bound named, on the path whose whole reason for existing is that the submitter
// cannot be authenticated.
//
// MUTATION CHECK. Drop the page rounding from elementLockedBytes and this
// fails: the priced figure drops to ~25 KiB against a measured ~1 MiB. The
// priced figure comes from shareSetLockedBytes, the function admission itself
// called — not from a sum this test recomputes, which would agree with a wrong
// guard as readily as a right one.
func TestUnmarshalShareSet_BudgetIsATrueUpperBound(t *testing.T) {
	warmLockedMemory(t)

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

	// The figure the guard ITSELF computes — shareSetLockedBytes is the single
	// copy of that arithmetic, and admission called this same function. Summing
	// it again here instead would track the correct answer no matter what the
	// guard did, and the mutation below would pass.
	var raw []json.RawMessage
	if err := json.Unmarshal(doc, &raw); err != nil {
		t.Fatalf("Unmarshal into elements: %v", err)
	}
	priced := shareSetLockedBytes(raw)
	if priced > MaxShareSetBytes {
		t.Fatalf("a 255-share root-key set prices at %d, over MaxShareSetBytes "+
			"(%d): the constant no longer admits the case it was derived for",
			priced, MaxShareSetBytes)
	}

	before := vmLockedBytes(t)
	decoded, err := UnmarshalShareSet(doc)
	if err != nil {
		t.Fatalf("UnmarshalShareSet: %v", err)
	}
	measured := vmLockedBytes(t) - before

	if measured == 0 {
		for _, s := range decoded {
			s.Destroy()
		}
		t.Skip("VmLck did not move across a successful decode: this host is not " +
			"charging locked memory, so there is nothing to witness")
	}
	// From the OS, for the reason given in the split test above.
	if want := maxShares * os.Getpagesize(); measured < want {
		t.Fatalf("VmLck moved %d bytes decoding %d shares, below the %d bytes "+
			"%d separate allocations must cost", measured, maxShares, want, maxShares)
	}
	if measured > priced+slackBytes {
		t.Fatalf("UnmarshalShareSet priced the set at %d bytes and the kernel "+
			"charged %d (slack %d): the ingestion bound is not an upper bound on "+
			"the locked memory an unauthenticated submission can take — see "+
			"issue #94", priced, measured, slackBytes)
	}

	for _, s := range decoded {
		s.Destroy()
	}
	if leaked := vmLockedBytes(t) - before; leaked > slackBytes {
		t.Fatalf("%d bytes of locked memory survived destroying every share", leaked)
	}
}
