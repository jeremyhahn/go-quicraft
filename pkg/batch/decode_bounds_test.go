package batch

import (
	"math"
	"testing"
)

// TestFits_NoOverflowNearMaxInt pins the decoder's bound check against int
// overflow on every architecture. The old form, off+n > total, wraps to a
// negative sum for off near math.MaxInt and accepts an out-of-range span; the
// regression inputs that drive DecodeBatch can only reach that on a 32-bit
// int, so this calls the helper directly with values that overflow a 64-bit
// int as well.
func TestFits_NoOverflowNearMaxInt(t *testing.T) {
	cases := []struct {
		name          string
		off, n, total int
		want          bool
	}{
		{"off near MaxInt overflows the sum", math.MaxInt - 4, 16, 32, false},
		{"n near MaxInt overflows the sum", 8, math.MaxInt - 4, 32, false},
		{"exact fit", 16, 16, 32, true},
		{"one past the end", 16, 17, 32, false},
		{"empty span at the end", 32, 0, 32, true},
		{"off past the end", 33, 0, 32, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fits(tc.off, tc.n, tc.total); got != tc.want {
				t.Fatalf("fits(%d, %d, %d) = %v, want %v", tc.off, tc.n, tc.total, got, tc.want)
			}
		})
	}
}
