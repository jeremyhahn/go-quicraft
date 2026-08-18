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

//go:build fips

package transport

import (
	"crypto/tls"
	"testing"
)

// otherBuildCurves is the list a standard build offers, pinned literally in
// tls_curves_nofips_test.go's assertHandshakeCurves.
var otherBuildCurves = []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384}

// assertHandshakeCurves pins the FIPS build's list: exactly the two approved
// NIST groups, and X25519 ABSENT.
//
// The absence check is the stronger of the two claims and the reason this file
// exists. A list that merely deprioritised X25519 would still offer it, and
// what the connection ends up using would depend on the peer — under
// GODEBUG=fips140=only landing on it fails the handshake outright, so the
// build must not be able to name it.
func assertHandshakeCurves(t *testing.T, role string, curves []tls.CurveID) {
	t.Helper()

	for _, c := range curves {
		if c == tls.X25519 {
			t.Errorf("%s: a FIPS build offered X25519 (%v). It is not an approved "+
				"group, and offering it makes the handshake outcome depend on the "+
				"peer; tls_curves_fips.go must not list it.", role, curves)
		}
	}

	want := []tls.CurveID{tls.CurveP256, tls.CurveP384}
	if len(curves) != len(want) {
		t.Fatalf("%s: curve list is %v, want %v", role, curves, want)
	}
	for i := range want {
		if curves[i] != want[i] {
			t.Fatalf("%s: curve list is %v, want %v", role, curves, want)
		}
	}
}
