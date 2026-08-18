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
	"testing"
)

// otherBuildCurves is the list a `-tags fips` build offers, pinned literally
// in tls_curves_fips_test.go's assertHandshakeCurves.
var otherBuildCurves = []tls.CurveID{tls.CurveP256, tls.CurveP384}

// assertHandshakeCurves pins the standard build's list: X25519 FIRST, because
// the reason the list is pinned at all is to skip ML-KEM and X25519 is the
// cheapest group; P-256 PRESENT, because it is the only group shared with a
// FIPS build and a cluster has to be convertible one node at a time.
func assertHandshakeCurves(t *testing.T, role string, curves []tls.CurveID) {
	t.Helper()

	if curves[0] != tls.X25519 {
		t.Errorf("%s: first curve is %v, want X25519. A standard build offers the "+
			"fastest group first; if X25519 was removed here rather than in "+
			"tls_curves_fips.go, the tag is no longer what decides.", role, curves[0])
	}

	var hasP256 bool
	for _, c := range curves {
		if c == tls.CurveP256 {
			hasP256 = true
		}
	}
	if !hasP256 {
		t.Errorf("%s: curve list %v does not offer P-256. It is the only group a "+
			"FIPS build also offers, so without it the two builds share nothing "+
			"and every cross-build handshake fails with `tls: no supported "+
			"elliptic curves for ECDHE`.", role, curves)
	}
}
