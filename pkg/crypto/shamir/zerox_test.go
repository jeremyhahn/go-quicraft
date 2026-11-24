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
	"errors"
	"testing"
)

// TestCombineRejectsZeroXCoordinate pins that a share with x = 0 is rejected; it would dictate the reconstructed secret.
func TestCombineRejectsZeroXCoordinate(t *testing.T) {
	secret := []byte("root-key-material-32-bytes-long!")
	values := splitValues(t, secret, 2, 3)

	// Forge share 0 with x = 0 and an attacker-chosen payload.
	chosen := bytes.Repeat([]byte{0xAA}, len(secret))
	forged := append([]byte{shareFormatVersion, 0x00}, chosen...)
	values[0] = encodeB64(forged)

	got, err := CombineBytes(values[:2], 2)
	if err == nil {
		t.Fatalf("CombineBytes ACCEPTED a zero x-coordinate share and returned %x. "+
			"With x=0 the Lagrange basis collapses to 1 for that share and 0 "+
			"for every other, so the attacker's bytes ARE the reconstructed "+
			"secret", got)
	}
	var combineErr *CombineError
	if !errors.As(err, &combineErr) {
		t.Fatalf("err = %T (%v), want *CombineError", err, err)
	}
}
