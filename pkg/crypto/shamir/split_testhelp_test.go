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
	"testing"
)

// splitValues is SplitBytes that fails the test on error.
func splitValues(t testing.TB, secret []byte, threshold, total int) [][]byte {
	t.Helper()
	values, err := SplitBytes(secret, threshold, total)
	if err != nil {
		t.Fatalf("SplitBytes(%d,%d): %v", threshold, total, err)
	}
	return values
}

// pick returns the values at the given positions, in that order.
func pick(values [][]byte, idx ...int) [][]byte {
	out := make([][]byte, 0, len(idx))
	for _, i := range idx {
		out = append(out, values[i])
	}
	return out
}

// decodeValue returns the raw [version][x][y...] payload of a share value.
func decodeValue(t testing.TB, value []byte) []byte {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(string(value))
	if err != nil {
		t.Fatalf("decoding share value: %v", err)
	}
	return raw
}

// encodeB64 is a test helper mirroring the share value encoding.
func encodeB64(raw []byte) []byte {
	return []byte(base64.StdEncoding.EncodeToString(raw))
}

// splitProtectedShares is SplitProtected that fails the test on error and
// destroys the shares when the test ends.
func splitProtectedShares(t testing.TB, secret []byte, threshold, total int) []*ProtectedShare {
	t.Helper()
	shares, err := SplitProtected(secret, threshold, total)
	if err != nil {
		t.Fatalf("SplitProtected(%d,%d): %v", threshold, total, err)
	}
	t.Cleanup(func() { destroyAll(shares) })
	return shares
}
