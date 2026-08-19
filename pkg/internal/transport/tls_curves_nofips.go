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
// X25519 comes first and keeps the reason it was pinned: naming any group
// explicitly suppresses ML-KEM hybrid negotiation and its ~5% handshake cost,
// which intra-cluster traffic between endpoints we control does not need, and
// X25519 is the fastest ECDH curve at 128-bit security.
//
// The NIST curves after it are NOT decoration and must not be trimmed back to
// an X25519-only list. This build offers a group a FIPS build cannot
// (fips140=only rejects X25519), and the FIPS build offers only the two below,
// so a list of one on either side gives the two builds an empty intersection
// and `tls: no supported elliptic curves for ECDHE` on every cross-build
// handshake. P-256 is the group both lists share, and sharing one is what
// makes a cluster upgradable one node at a time instead of all at once.
//
// See tls_curves_fips.go for the approved-only list, and
// docs/quic-transport/architecture.md for the negotiation matrix.
func handshakeCurves() []tls.CurveID {
	return []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384}
}
