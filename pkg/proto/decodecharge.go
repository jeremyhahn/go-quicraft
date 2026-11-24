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

// DecodeCharge returns the heap to reserve for decoding n frame bytes, n times MaxDecodeMemoryFactor, saturating; non-positive n charges nothing.
func DecodeCharge(n int64) int64 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxInt64/MaxDecodeMemoryFactor {
		return math.MaxInt64
	}
	return n * MaxDecodeMemoryFactor
}

// FrameDecodeCharge returns the reservation for a frame of wireLen bytes decoding to decodedLen: wire length plus any excess, each at DecodeCharge, saturating at MaxInt64.
func FrameDecodeCharge(wireLen, decodedLen int64) int64 {
	wire := DecodeCharge(wireLen)
	delta := DecodeCharge(decodedLen - max(wireLen, 0))
	if wire > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return wire + delta
}
