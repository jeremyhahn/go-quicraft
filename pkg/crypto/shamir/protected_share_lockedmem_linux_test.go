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

// vmLockedBytes reports the kernel's VmLck for this process; it skips where /proc/self/status is unavailable.
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

// slackBytes allows 64 KiB of unrelated process-wide locked allocations.
const slackBytes = 64 << 10

// TestSplitProtected_BudgetIsATrueUpperBound checks the kernel's VmLck charge for an admitted split stays within splitLockedBytes.
func TestSplitProtected_BudgetIsATrueUpperBound(t *testing.T) {
	warmLockedMemory(t)

	for _, tc := range []struct {
		name                        string
		secretLen, threshold, total int
	}{
		// The per-share page rounding is 100% of the true cost and 0% of a
		// byte-summed price.
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
			// Each share is at least one page; use os.Getpagesize, not the helper under test.
			if want := tc.total * os.Getpagesize(); measured < want {
				t.Fatalf("VmLck moved %d bytes across a %d-share split, below the "+
					"%d bytes %d separate allocations must cost: the measurement "+
					"did not capture the call", measured, tc.total, want, tc.total)
			}

			if measured > priced+slackBytes {
				t.Fatalf("splitLockedBytes(%d,%d,%d) priced %d bytes and the kernel "+
					"charged %d (VmLck %d -> %d, slack %d): the budget is not an "+
					"upper bound, so MaxShareSetBytes cannot be compared to "+
					"RLIMIT_MEMLOCK",
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

// TestUnmarshalShareSet_BudgetIsATrueUpperBound checks the same bound on the unauthenticated ingestion path.
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

	// The figure admission itself computed, not a recomputed sum.
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
			"the locked memory an unauthenticated submission can take", priced, measured, slackBytes)
	}

	for _, s := range decoded {
		s.Destroy()
	}
	if leaked := vmLockedBytes(t) - before; leaked > slackBytes {
		t.Fatalf("%d bytes of locked memory survived destroying every share", leaked)
	}
}
