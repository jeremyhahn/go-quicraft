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

import "crypto/tls"

// handshakeCurves is the key-exchange group list for both roles of the
// intra-cluster TLS 1.3 handshake in a `-tags fips` build: the two
// FIPS 140-3 approved ECDH groups, in that order.
//
// X25519 is ABSENT rather than deprioritised, and that is the whole point of
// building this list behind a tag. It is not an approved group (SP 800-186
// lists the NIST curves; SP 800-56A Rev. 3 is the approved key-agreement
// scheme), so a process running with GODEBUG=fips140=only fails every
// handshake that lands on it with `tls: no supported elliptic curves for
// ECDHE`. Leaving it in the list and trusting negotiation to avoid it would
// make the outcome depend on what the peer offers; removing it at compile time
// means no configuration, environment or peer can reach it, which is the same
// absence-not-refusal shape pkg/seal uses for Argon2id and ChaCha20.
//
// P-384 is offered alongside P-256 rather than instead of it: P-256 is the one
// group this list shares with the standard build (tls_curves_nofips.go), and
// dropping it would leave a FIPS node unable to handshake with a standard one
// at all.
func handshakeCurves() []tls.CurveID {
	return []tls.CurveID{tls.CurveP256, tls.CurveP384}
}
