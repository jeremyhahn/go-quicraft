package transport

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// writeReloadCRL writes a PEM CRL signed by ca to path, listing revoked.
func writeReloadCRL(t *testing.T, path string, ca *reloadCA, number int64, revoked ...int64) {
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
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CRL: %v", err)
	}
}

// newCARollover is newCADrop for a rollover: the transport first trusts both
// CAs, as it does mid-rollover, so a CRL each one signed loads from the start,
// and then reloads onto the new CA alone. It also returns the TLS pair the
// transport held before that reload.
func newCARollover(t *testing.T, rev *revocation.Config, prepare func(oldCA, newCA *reloadCA)) (*caDrop, *tlsPair) {
	t.Helper()
	d := &caDrop{oldCA: newReloadCA(t, "old"), newCA: newReloadCA(t, "new")}
	prepare(d.oldCA, d.newCA)
	key := newReloadKey(t)
	d.tr = newReloadTransport(t, mtlsFor(t, d.oldCA, key, 500, d.oldCA, d.newCA), rev)
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

	before := d.tr.currentTLS()
	if err := d.tr.ReloadMTLS(mtlsFor(t, d.newCA, key, 502, d.newCA), "", nil); err != nil {
		t.Fatalf("ReloadMTLS onto the new CA only: %v", err)
	}
	return d, before
}

// TestReloadMTLS_CADropKeepsIssuerOfConfiguredCRL pins that the issuer a CA
// rollover dropped stays retained while a configured CRL file it signed is
// still configured. Closing the last old-CA session used to prune it, so the
// next ReloadCRLs could not verify the old CRL, marked the whole revocation
// set stale and, under enforcement, refused new-CA handshakes while the sweep
// closed healthy new-CA sessions.
func TestReloadMTLS_CADropKeepsIssuerOfConfiguredCRL(t *testing.T) {
	dir := t.TempDir()
	oldCRL, newCRL := filepath.Join(dir, "old.crl"), filepath.Join(dir, "new.crl")
	rev := &revocation.Config{Mode: revocation.ModeCRL, CRLPaths: []string{oldCRL, newCRL}}
	d, _ := newCARollover(t, rev, func(o, n *reloadCA) {
		writeReloadCRL(t, oldCRL, o, 1)
		writeReloadCRL(t, newCRL, n, 1)
	})

	newPeer := newReloadTransport(t, mtlsFor(t, d.newCA, newReloadKey(t), 520, d.newCA), nil)
	if err := newPeer.Start(); err != nil {
		t.Fatalf("new-CA peer Start: %v", err)
	}
	t.Cleanup(func() { _ = newPeer.Stop() })
	newConn, err := d.tr.getConnection(newPeer.Addr().String(), "")
	if err != nil {
		t.Fatalf("getConnection under the new CA: %v", err)
	}

	// The last old-CA session ends; the sweep after it releases every
	// retained issuer no live session needs.
	d.tr.removeAndCloseConnection(d.conn.RemoteAddr().String(), "test")
	d.tr.revokeCheckConnections()

	if err := d.tr.revocationChecker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs with the old CA's CRL still configured: %v", err)
	}
	d.tr.revokeCheckConnections()
	if closedWithin(newConn, 200*time.Millisecond) {
		t.Fatal("the sweep closed a healthy new-CA session after the CRL reload")
	}
	newcomer := peerPair(t, mtlsFor(t, d.newCA, newReloadKey(t), 521, d.newCA))
	if _, _, err := handshake(t, d.tr.listenerTLS(), newcomer.client); err != nil {
		t.Fatalf("inbound new-CA handshake after the CRL reload: %v", err)
	}
	if _, _, err := handshake(t, newcomer.server, d.tr.currentTLS().client); err != nil {
		t.Fatalf("outbound new-CA handshake after the CRL reload: %v", err)
	}
}

// TestReloadMTLS_HandshakeStraddlingCADropStillRevoked pins that a handshake
// checks revocation against the issuer set of the TLS config it runs under.
// One that took the old-CA config before a reload dropped that CA, and
// reached VerifyConnection after it, resolved its old-CA leaf in the new
// shared issuer set, found no issuer, fell back to the issuer-agnostic scope
// and missed the old CA's issuer-scoped CRL entry: a revoked certificate
// authenticated.
func TestReloadMTLS_HandshakeStraddlingCADropStillRevoked(t *testing.T) {
	const revokedSerial = 530
	dir := t.TempDir()
	oldCRL, newCRL := filepath.Join(dir, "old.crl"), filepath.Join(dir, "new.crl")
	rev := &revocation.Config{Mode: revocation.ModeCRL, CRLPaths: []string{oldCRL, newCRL}}
	var revokedPeer *tlsPair
	_, before := newCARollover(t, rev, func(o, n *reloadCA) {
		writeReloadCRL(t, oldCRL, o, 1, revokedSerial)
		writeReloadCRL(t, newCRL, n, 1)
		revokedPeer = peerPair(t, mtlsFor(t, o, newReloadKey(t), revokedSerial, o, n))
	})

	// Each handshake runs under the config the transport held before the
	// drop, as one that took it (GetConfigForClient, or currentTLS on dial)
	// just ahead of the reload does, and reaches VerifyConnection after it.
	if _, _, err := handshake(t, before.server, revokedPeer.client); err == nil {
		t.Fatal("inbound: a revoked old-CA leaf authenticated in a handshake straddling the CA drop")
	}
	if _, _, err := handshake(t, revokedPeer.server, before.client); err == nil {
		t.Fatal("outbound: a revoked old-CA leaf authenticated in a handshake straddling the CA drop")
	}
}
