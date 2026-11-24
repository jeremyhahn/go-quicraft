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

//go:build !fips

package transport

import (
	"crypto/tls"
	"slices"
	"testing"
)

// otherBuildCurves is the list a `-tags fips` build offers, pinned literally
// in tls_curves_fips_test.go's assertHandshakeCurves.
var otherBuildCurves = []tls.CurveID{tls.SecP256r1MLKEM768, tls.CurveP256, tls.CurveP384}

// selfCurve is the group two standard nodes negotiate, and crossBuildCurve
// the one a standard node and a FIPS node agree on.
const (
	selfCurve       = tls.X25519MLKEM768
	crossBuildCurve = tls.SecP256r1MLKEM768
)

// assertHandshakeCurves pins the standard build's list: X25519MLKEM768 FIRST,
// because a client sends its key share for the first group it names and two
// standard nodes should land on the hybrid in one round trip;
// SecP256r1MLKEM768 PRESENT, because it is the hybrid a FIPS build offers;
// P-256 PRESENT, because it is the classical group shared with a FIPS build
// and a cluster has to be convertible one node at a time.
func assertHandshakeCurves(t *testing.T, role string, curves []tls.CurveID) {
	t.Helper()

	if curves[0] != tls.X25519MLKEM768 {
		t.Errorf("%s: first curve is %v, want X25519MLKEM768. A standard build offers "+
			"the post-quantum hybrid first (RFC 10024).", role, curves[0])
	}

	for _, need := range []tls.CurveID{tls.SecP256r1MLKEM768, tls.CurveP256} {
		if !slices.Contains(curves, need) {
			t.Errorf("%s: curve list %v does not offer %v. It is shared with a FIPS "+
				"build; without the hybrid a mixed cluster falls back to classical "+
				"key exchange, and without P-256 the two builds share nothing with a "+
				"peer that offers no hybrid.", role, curves, need)
		}
	}
}
