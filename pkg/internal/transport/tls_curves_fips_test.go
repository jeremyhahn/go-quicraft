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
	"context"
	"crypto/fips140"
	"crypto/tls"
	"testing"
)

// otherBuildCurves is the list a standard build offers, pinned literally in
// tls_curves_nofips_test.go's assertHandshakeCurves.
var otherBuildCurves = []tls.CurveID{
	tls.X25519MLKEM768,
	tls.SecP256r1MLKEM768,
	tls.X25519,
	tls.CurveP256,
	tls.CurveP384,
}

// selfCurve is the group two FIPS nodes negotiate, and crossBuildCurve the
// one a FIPS node and a standard node agree on.
const (
	selfCurve       = tls.SecP256r1MLKEM768
	crossBuildCurve = tls.SecP256r1MLKEM768
)

// assertHandshakeCurves pins the FIPS build's list: the SecP256r1MLKEM768
// hybrid, then the two approved NIST groups, and no group with an X25519 half.
//
// The absence check is the stronger of the two claims and the reason this file
// exists. A list that merely deprioritised X25519 would still offer it, and
// what the connection ends up using would depend on the peer — under
// GODEBUG=fips140=only landing on it fails the handshake outright, so the
// build must not be able to name it. X25519MLKEM768 is refused here too: the
// module accepts it, but its classical half is the group this list keeps out.
func assertHandshakeCurves(t *testing.T, role string, curves []tls.CurveID) {
	t.Helper()

	for _, c := range curves {
		if c == tls.X25519 || c == tls.X25519MLKEM768 {
			t.Errorf("%s: a FIPS build offered %v (%v). X25519 is not an approved "+
				"group, and offering it makes the handshake outcome depend on the "+
				"peer; tls_curves_fips.go must not list it.", role, c, curves)
		}
	}

	want := []tls.CurveID{tls.SecP256r1MLKEM768, tls.CurveP256, tls.CurveP384}
	if len(curves) != len(want) {
		t.Fatalf("%s: curve list is %v, want %v", role, curves, want)
	}
	for i := range want {
		if curves[i] != want[i] {
			t.Fatalf("%s: curve list is %v, want %v", role, curves, want)
		}
	}
}

// TestFIPSOnlyNegotiatesHybrid is the evidence that SecP256r1MLKEM768 may lead
// the FIPS list: a handshake offering it and nothing else completes, over
// QUIC, inside the Go Cryptographic Module. make verify-fips runs this
// package under GODEBUG=fips140=only, where a group the module does not
// approve fails with `tls: no supported key exchange methods`.
func TestFIPSOnlyNegotiatesHybrid(t *testing.T) {
	t.Logf("crypto/fips140: enabled=%v version=%q", fips140.Enabled(), fips140.Version())

	only := []tls.CurveID{tls.SecP256r1MLKEM768}
	cl := listenWithCurves(t, only, only)
	ctx, cancel := context.WithTimeout(context.Background(), handshakeDeadline)
	defer cancel()
	conn, err := cl.dial(ctx)
	if err != nil {
		t.Fatalf("SecP256r1MLKEM768-only handshake: %v", err)
	}
	defer func() { _ = conn.CloseWithError(0, "") }()
	if state := conn.ConnectionState().TLS; state.CurveID != tls.SecP256r1MLKEM768 {
		t.Fatalf("negotiated %v, want SecP256r1MLKEM768", state.CurveID)
	}
}
