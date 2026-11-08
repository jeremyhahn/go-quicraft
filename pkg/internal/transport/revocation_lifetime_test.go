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

package transport

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// transportPairWithRevocation builds a connected pair and attaches a checker
// to the DIALING transport.
//
// The checker is attached directly rather than through Config.RevocationConfig
// because that path requires a CRL file or an OCSP responder, and neither is
// needed here: the unit under test is revokeCheckConnections, and
// AddRevokedSerial is the checker's own admin entry point. Going through the
// config would test config validation, not connection eviction.
//
// The checker opts into soft-fail for the same reason. revokeCheckConnections
// treats a checker ERROR as grounds to close, so an enforcing checker with no
// source would tear the connection down for the missing source and the test
// would pass without the revocation having done anything.
func transportPairWithRevocation(t testing.TB) (*QUICTransport, *QUICTransport, func()) {
	t.Helper()
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler())
	t1.revocationChecker = revocation.NewChecker(revocation.Config{AllowSoftFail: true})
	return t1, t2, cleanup
}

// TestRevocation_EvictsAnAlreadyEstablishedConnection pins revocation's
// LIFETIME, as distinct from its logic.
//
// VerifyConnection runs only at handshake. Before revokeCheckConnections
// existed, a certificate revoked AFTER admission kept its session for as long
// as the process lived: quic-go's keep-alive (10s) sits under MaxIdleTimeout
// (30s) so the connection never idle-closes, cleanStaleConnections only reaps
// connections whose context is already dead, and CloseTarget reaches only the
// outbound half. Revocation stopped a compromised node RECONNECTING and did
// nothing about the connection it already held.
//
// The test establishes a real connection, revokes the peer's serial, runs the
// re-check the cleanup ticker runs, and requires the connection to be torn
// down.
func TestRevocation_EvictsAnAlreadyEstablishedConnection(t *testing.T) {
	t1, t2, cleanup := transportPairWithRevocation(t)
	defer cleanup()

	target := t2.Addr().String()
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection: %v", err)
	}
	select {
	case <-conn.Context().Done():
		t.Fatal("test setup: connection died before the revocation was published")
	default:
	}

	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		t.Fatal("test setup: no peer certificate on an mTLS connection")
	}
	serial := certs[0].SerialNumber.Text(16)

	// Publish the revocation, then run exactly what the cleanup ticker runs.
	t1.revocationChecker.AddRevokedSerial(serial)
	t1.revokeCheckConnections()

	select {
	case <-conn.Context().Done():
		return // torn down, as required
	case <-time.After(5 * time.Second):
	}
	t.Fatal("a connection whose peer certificate was revoked is still live: " +
		"revocation only stops the peer RECONNECTING, so a compromised node " +
		"keeps its session for the lifetime of the process")
}
