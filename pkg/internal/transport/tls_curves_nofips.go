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

import "crypto/tls"

// handshakeCurves is the key-exchange group list for both roles of the
// intra-cluster TLS 1.3 handshake in a standard build.
//
// X25519MLKEM768 comes first: the hybrid of draft-ietf-tls-ecdhe-mlkem-05
// (pinned; docs/references/), X25519 combined with the FIPS 203 ML-KEM-768
// KEM. Every byte of replicated user data crosses these connections, and a
// recorded classical handshake is one a future quantum adversary can decrypt;
// the hybrid stays at least as strong as X25519 if ML-KEM falls. Two standard
// nodes always land on it in one round trip, because the client sends its key
// share for the first group it names.
//
// SecP256r1MLKEM768 is second because it is the hybrid a `-tags fips` build
// offers (both halves approved). A standard node meeting a FIPS node agrees on
// it instead of falling back to classical P-256, so converting a cluster to
// FIPS one node at a time never drops the post-quantum half.
//
// X25519, P-256 and P-384 follow, unchanged in order, so a peer that offers no
// hybrid still gets the fastest classical group it supports. They must not be
// trimmed: P-256 is the classical group shared with a FIPS build, and a list
// sharing nothing with the other build fails every cross-build handshake with
// `tls: no supported elliptic curves for ECDHE`.
//
// See tls_curves_fips.go for the approved-only list, and
// docs/quic-transport/architecture.md for the negotiation matrix.
func handshakeCurves() []tls.CurveID {
	return []tls.CurveID{
		tls.X25519MLKEM768,
		tls.SecP256r1MLKEM768,
		tls.X25519,
		tls.CurveP256,
		tls.CurveP384,
	}
}
