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
	"crypto/fips140"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/ocsp"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// caDrop is a transport whose CA bundle is rolled from oldCA to newCA while
// it holds an outbound session to a peer whose leaf only oldCA signed.
type caDrop struct {
	tr       *QUICTransport
	oldCA    *reloadCA
	newCA    *reloadCA
	conn     *quic.Conn
	peerLeaf *x509.Certificate
}

const caDropPeerSerial = 501

// newCADrop establishes the old-CA session under rev, then reloads the
// transport onto a bundle holding newCA alone. prepare runs after the CAs
// exist and before the transport is built, so a test can point rev at
// sources signed by them.
func newCADrop(t *testing.T, rev *revocation.Config, prepare func(oldCA, newCA *reloadCA)) *caDrop {
	t.Helper()
	d := &caDrop{oldCA: newReloadCA(t, "old"), newCA: newReloadCA(t, "new")}
	if prepare != nil {
		prepare(d.oldCA, d.newCA)
	}
	key := newReloadKey(t)
	d.tr = newReloadTransport(t, mtlsFor(t, d.oldCA, key, 500, d.oldCA), rev)
	if err := d.tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = d.tr.Stop() })
	peer := newReloadTransport(t, mtlsFor(t, d.oldCA, newReloadKey(t), caDropPeerSerial, d.oldCA, d.newCA), nil)
	if err := peer.Start(); err != nil {
		t.Fatalf("peer Start: %v", err)
	}
	t.Cleanup(func() { _ = peer.Stop() })

	conn, err := d.tr.getConnection(peer.Addr().String(), "")
	if err != nil {
		t.Fatalf("getConnection under the old CA: %v", err)
	}
	d.conn = conn
	d.peerLeaf = conn.ConnectionState().TLS.PeerCertificates[0]

	if err := d.tr.ReloadMTLS(mtlsFor(t, d.newCA, key, 502, d.newCA), "", nil); err != nil {
		t.Fatalf("ReloadMTLS onto the new CA only: %v", err)
	}
	return d
}

// closedWithin reports whether conn is torn down within d.
func closedWithin(conn *quic.Conn, d time.Duration) bool {
	select {
	case <-conn.Context().Done():
		return true
	case <-time.After(d):
		return false
	}
}

// assertDroppedCARefused checks that the dropped CA admits no new handshake,
// in either direction, although the checker still holds it for the session.
func (d *caDrop) assertDroppedCARefused(t *testing.T) {
	t.Helper()
	oldPeer := peerPair(t, mtlsFor(t, d.oldCA, newReloadKey(t), 510, d.oldCA, d.newCA))
	if _, _, err := handshake(t, d.tr.listenerTLS(), oldPeer.client); err == nil {
		t.Fatal("inbound: a new handshake under the dropped CA was accepted")
	}
	if _, _, err := handshake(t, oldPeer.server, d.tr.currentTLS().client); err == nil {
		t.Fatal("outbound: a new handshake under the dropped CA was accepted")
	}
	if _, err := d.tr.revocationChecker.IsRevoked(d.peerLeaf); err == nil {
		t.Fatal("IsRevoked (the handshake path) resolved an issuer only an old session uses")
	}
}

// TestReloadMTLS_CADropKeepsSessionUnderEnforcedOCSP is the session the
// reload promises to preserve: OCSP is enforced, the responder answers, and
// the only thing that changed is that the bundle no longer holds the CA the
// peer's leaf was issued by. The sweep must not close it for want of an
// issuer to build the request with.
func TestReloadMTLS_CADropKeepsSessionUnderEnforcedOCSP(t *testing.T) {
	// An OCSP request's CertID is SHA-1 (RFC 6960 section 4.1.1), which
	// GODEBUG=fips140=only refuses at use. ModeOCSP is outside the
	// approved-only recipe; see the FIPS 140-3 exemption record in
	// docs/quic-transport/architecture.md. Every other run executes this test.
	if fips140.Enforced() {
		t.Skip("ModeOCSP builds a SHA-1 CertID, which fips140=only refuses (FIPS 140-3 exemption record)")
	}
	cas := map[int64]*reloadCA{}
	responder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req, err := ocsp.ParseRequest(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ca, ok := cas[req.SerialNumber.Int64()]
		if !ok {
			http.Error(w, "unknown serial", http.StatusNotFound)
			return
		}
		resp, err := ocsp.CreateResponse(ca.cert, ca.cert, ocsp.Response{
			Status:       ocsp.Good,
			SerialNumber: req.SerialNumber,
			ThisUpdate:   time.Now().Add(-time.Minute),
			NextUpdate:   time.Now().Add(time.Hour),
		}, ca.key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(resp)
	}))
	defer responder.Close()

	rev := &revocation.Config{Mode: revocation.ModeOCSP, OCSPResponderURL: responder.URL}
	d := newCADrop(t, rev, func(oldCA, _ *reloadCA) { cas[caDropPeerSerial] = oldCA })

	if revoked, err := d.tr.revocationChecker.IsRevokedEstablished(d.peerLeaf); revoked || err != nil {
		t.Fatalf("IsRevokedEstablished(old-CA session) = %v, %v; want not revoked", revoked, err)
	}
	// Twice: the first sweep's prune must keep the issuer the session needs.
	d.tr.revokeCheckConnections()
	d.tr.revokeCheckConnections()
	if closedWithin(d.conn, 300*time.Millisecond) {
		t.Fatal("the sweep closed a good old-CA session after the CA was dropped from the bundle")
	}
	d.assertDroppedCARefused(t)
}

// TestReloadMTLS_CADropStillRevokesUnderIssuerScopedCRL is the other half:
// the old session stays checkable, so a revocation the dropped CA publishes
// for it still lands. Before the fix its scope fell back to issuer-agnostic,
// missing every issuer-scoped CRL entry, and the old CA's CRL no longer
// verified at all.
func TestReloadMTLS_CADropStillRevokesUnderIssuerScopedCRL(t *testing.T) {
	crlPath := filepath.Join(t.TempDir(), "old.crl")
	writeCRL := func(ca *reloadCA, number int64, revoked ...int64) {
		t.Helper()
		entries := make([]x509.RevocationListEntry, 0, len(revoked))
		for _, serial := range revoked {
			entries = append(entries, x509.RevocationListEntry{
				SerialNumber:   big.NewInt(serial),
				RevocationTime: time.Now().Add(-time.Minute),
			})
		}
		der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
			Number:                    big.NewInt(number),
			ThisUpdate:                time.Now().Add(-time.Minute),
			NextUpdate:                time.Now().Add(time.Hour),
			RevokedCertificateEntries: entries,
		}, ca.cert, ca.key)
		if err != nil {
			t.Fatalf("CreateRevocationList: %v", err)
		}
		if err := os.WriteFile(crlPath, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), 0o600); err != nil {
			t.Fatalf("write CRL: %v", err)
		}
	}

	var oldCA *reloadCA
	rev := &revocation.Config{Mode: revocation.ModeCRL, CRLPaths: []string{crlPath}}
	d := newCADrop(t, rev, func(o, _ *reloadCA) {
		oldCA = o
		writeCRL(oldCA, 1)
	})

	// A sweep before the revocation keeps the session.
	d.tr.revokeCheckConnections()
	if closedWithin(d.conn, 200*time.Millisecond) {
		t.Fatal("the sweep closed an unrevoked old-CA session after the CA drop")
	}

	writeCRL(oldCA, 2, caDropPeerSerial)
	if err := d.tr.revocationChecker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs with the dropped CA's CRL: %v", err)
	}
	if revoked, err := d.tr.revocationChecker.IsRevokedEstablished(d.peerLeaf); !revoked || err != nil {
		t.Fatalf("IsRevokedEstablished(revoked old-CA leaf) = %v, %v; want revoked", revoked, err)
	}
	d.tr.revokeCheckConnections()
	if !closedWithin(d.conn, 5*time.Second) {
		t.Fatal("an old-CA session the dropped CA revoked is still live")
	}
	d.assertDroppedCARefusedCRL(t)
}

// assertDroppedCARefusedCRL is assertDroppedCARefused for CRL mode, where the
// handshake-path IsRevoked of an issuer-less leaf answers from the
// issuer-agnostic scope instead of failing: the TLS chain check is what
// refuses it.
func (d *caDrop) assertDroppedCARefusedCRL(t *testing.T) {
	t.Helper()
	oldPeer := peerPair(t, mtlsFor(t, d.oldCA, newReloadKey(t), 511, d.oldCA, d.newCA))
	if _, _, err := handshake(t, d.tr.listenerTLS(), oldPeer.client); err == nil {
		t.Fatal("inbound: a new handshake under the dropped CA was accepted")
	}
	if _, _, err := handshake(t, oldPeer.server, d.tr.currentTLS().client); err == nil {
		t.Fatal("outbound: a new handshake under the dropped CA was accepted")
	}
}
