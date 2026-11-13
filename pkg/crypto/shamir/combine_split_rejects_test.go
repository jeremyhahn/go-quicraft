package shamir

import (
	"errors"
	"strings"
	"testing"
	"testing/iotest"
)

// TestCombineSplitRejects_TooShortShare covers parseRawShare's length floor:
// a decoded share must carry [version][x] plus at least one payload byte, and
// a shorter one is refused with a CombineError naming its index, whichever
// position it occupies.
func TestCombineSplitRejects_TooShortShare(t *testing.T) {
	valid := func(x byte) []byte {
		return encodeB64([]byte{shareFormatVersion, x, 0xAA, 0xBB, 0xCC, 0xDD})
	}
	cases := []struct {
		name   string
		values [][]byte
		prefix string
	}{
		{"share0 one byte", [][]byte{encodeB64([]byte{shareFormatVersion}), valid(2)}, "share 0 too short"},
		{"share0 empty", [][]byte{encodeB64(nil), valid(2)}, "share 0 too short"},
		{"share1 header only", [][]byte{valid(1), encodeB64([]byte{shareFormatVersion, 0x02})}, "share 1 too short"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			secret, err := CombineBytes(tc.values, 2)
			if secret != nil {
				t.Fatalf("secret = %x, want nil", secret)
			}
			var ce *CombineError
			if !errors.As(err, &ce) {
				t.Fatalf("err = %v (%T), want *CombineError", err, err)
			}
			if !strings.HasPrefix(ce.Reason, tc.prefix) {
				t.Fatalf("Reason = %q, want prefix %q", ce.Reason, tc.prefix)
			}
		})
	}
}

// TestCombineSplitRejects_PayloadLengthMismatch covers the rule that every
// share's payload length equals share 0's: a longer share 1 is refused with a
// CombineError naming both lengths.
func TestCombineSplitRejects_PayloadLengthMismatch(t *testing.T) {
	share0 := encodeB64([]byte{shareFormatVersion, 0x01, 1, 2, 3, 4})
	share1 := encodeB64([]byte{shareFormatVersion, 0x02, 1, 2, 3, 4, 5, 6})
	secret, err := CombineBytes([][]byte{share0, share1}, 2)
	if secret != nil {
		t.Fatalf("secret = %x, want nil", secret)
	}
	var ce *CombineError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v (%T), want *CombineError", err, err)
	}
	if !strings.Contains(ce.Reason, "length 6") || !strings.Contains(ce.Reason, "length 4") {
		t.Fatalf("Reason = %q, want it to name payload lengths 6 and 4", ce.Reason)
	}
}

// TestCombineSplitRejects_RandomnessFailure covers SplitBytes's coefficient
// read: a failing randomness source yields no shares and a SplitError that
// carries the reader's error.
func TestCombineSplitRejects_RandomnessFailure(t *testing.T) {
	restore := testOnlyRandReader(iotest.ErrReader(errors.New("entropy gone")))
	defer restore()

	values, err := SplitBytes([]byte("secret"), 3, 5)
	if values != nil {
		t.Fatalf("values = %v, want nil", values)
	}
	var se *SplitError
	if !errors.As(err, &se) {
		t.Fatalf("err = %v (%T), want *SplitError", err, err)
	}
	if !strings.HasPrefix(se.Reason, "reading random coefficients") {
		t.Fatalf("Reason = %q, want prefix %q", se.Reason, "reading random coefficients")
	}
	if !strings.Contains(se.Reason, "entropy gone") {
		t.Fatalf("Reason = %q, want it to carry %q", se.Reason, "entropy gone")
	}
}
