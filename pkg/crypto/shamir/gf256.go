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

// Constant-time GF(2^8) arithmetic with the AES polynomial 0x11B: no secret-indexed lookups or secret-dependent branches.

// gfMul multiplies a and b in GF(2^8) in constant time with branchless masks.
func gfMul(a, b byte) byte {
	var r byte
	for i := 0; i < 8; i++ {
		// Add a into the result when the low bit of b is set.
		// -(b & 1) is 0x00 or 0xFF (unsigned negation wraps).
		r ^= a & -(b & 1)
		// Multiply a by x, reducing modulo 0x11B when the high bit
		// overflows. -(a >> 7) is 0x00 or 0xFF.
		carry := -(a >> 7)
		a = (a << 1) ^ (carry & 0x1b)
		b >>= 1
	}
	return r
}

// gfInv returns a^254, the inverse of a, in constant time; gfInv(0) is 0, so callers reject zero first.
func gfInv(a byte) byte {
	sq := gfMul(a, a) // a^2
	r := sq
	for i := 2; i <= 7; i++ {
		sq = gfMul(sq, sq) // a^(2^i)
		r = gfMul(r, sq)
	}
	return r // a^(2+4+...+128) = a^254
}

// gfDiv returns a/b in GF(2^8). b must be non-zero (enforced by callers
// via duplicate/zero x-coordinate rejection); gfDiv(a, 0) returns 0.
func gfDiv(a, b byte) byte {
	return gfMul(a, gfInv(b))
}

// gfPolyEval evaluates the polynomial with the given coefficients at x
// using Horner's method in GF(2^8). coefficients[0] is the constant term
// (the secret byte); higher indices are higher-degree coefficients.
func gfPolyEval(coefficients []byte, x byte) byte {
	var y byte
	for i := len(coefficients) - 1; i >= 0; i-- {
		y = gfMul(y, x) ^ coefficients[i]
	}
	return y
}

// gfLagrangeZero computes the Lagrange basis at x=0 for public, non-zero, distinct x-coordinates.
func gfLagrangeZero(xs []byte) []byte {
	basis := make([]byte, len(xs))
	for j := range xs {
		var l byte = 1
		for m := range xs {
			if m == j {
				continue
			}
			l = gfMul(l, gfDiv(xs[m], xs[j]^xs[m]))
		}
		basis[j] = l
	}
	return basis
}
