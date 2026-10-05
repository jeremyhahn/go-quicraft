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

package proto

import "math"

// DecodeCharge returns the heap a receiver must reserve to admit and decode n
// bytes of frame: n times MaxDecodeMemoryFactor.
//
// The wire length alone is the one number a decode is guaranteed NOT to be
// bounded by, so a receive budget charged in wire bytes describes bytes read
// rather than bytes held. Charging the factor makes such a budget bound decoded
// heap.
//
// The arithmetic saturates rather than wraps. A frame length is bounded well
// below the overflow point, but a reservation that wrapped negative would hand
// out unlimited budget, so it is not left to trust. A non-positive n charges
// nothing.
func DecodeCharge(n int64) int64 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxInt64/MaxDecodeMemoryFactor {
		return math.MaxInt64
	}
	return n * MaxDecodeMemoryFactor
}

// FrameDecodeCharge returns the total a receiver reserves for one frame of
// wireLen bytes whose payload decodes (decompresses) to decodedLen bytes: the
// wire length, plus the amount by which the decoded length exceeds it, each at
// DecodeCharge. A frame that decodes to no more than its wire length is
// charged for its wire length alone.
//
// It is the sum of the two reservations a receiver takes, in the order it can
// take them: the wire length before the body is read, and the decompression
// delta once the declared decoded length is known. Called with the largest
// legal wire length and decoded length, it is the most any single frame can
// reserve, which is the unit a receive budget must be sized in.
//
// The sum saturates at MaxInt64 for the same reason DecodeCharge does.
func FrameDecodeCharge(wireLen, decodedLen int64) int64 {
	wire := DecodeCharge(wireLen)
	delta := DecodeCharge(decodedLen - max(wireLen, 0))
	if wire > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return wire + delta
}
