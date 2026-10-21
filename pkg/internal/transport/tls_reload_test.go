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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// reloadCA is a throwaway issuing CA for the reload tests.
type reloadCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newReloadCA(t testing.TB, name string) *reloadCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &reloadCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func newReloadKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	return key
}

// issue signs a loopback leaf for key with the given serial; extraIPs widen
// its SANs.
func (ca *reloadCA) issue(t testing.TB, key *ecdsa.PrivateKey, serial int64, extraIPs ...net.IP) (certPEM, keyPEM []byte) {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  append([]net.IP{net.IPv4(127, 0, 0, 1)}, extraIPs...),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// mtlsFor builds MTLSConfig material trusting the bundle of cas, with a leaf
// for key issued by signer.
func mtlsFor(t testing.TB, signer *reloadCA, key *ecdsa.PrivateKey, serial int64, cas ...*reloadCA) *MTLSConfig {
	t.Helper()
	certPEM, keyPEM := signer.issue(t, key, serial)
	var bundle []byte
	for _, ca := range cas {
		bundle = append(bundle, ca.pem...)
	}
	return &MTLSConfig{CACert: bundle, Cert: certPEM, Key: keyPEM}
}

// peerPair is the TLS pair of a peer that is not under test.
func peerPair(t testing.TB, mtls *MTLSConfig) *tlsPair {
	t.Helper()
	pair, err := buildMTLSTLS(mtls, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS(peer): %v", err)
	}
	return pair
}

// newReloadTransport constructs, without starting, the transport under test.
func newReloadTransport(t testing.TB, mtls *MTLSConfig, rev *revocation.Config) *QUICTransport {
	t.Helper()
	tr, err := NewQUICTransport(Config{
		ListenAddress:    "127.0.0.1:0",
		DeploymentID:     42,
		MTLSConfig:       mtls,
		RevocationConfig: rev,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	return tr
}

// handshake runs one QUIC handshake against a listener serving serverCfg,
// dialed with clientCfg (as the transport's dial path prepares it). It
// returns the leaf each side saw from the other, or the error that refused
// the connection on either side.
func handshake(t testing.TB, serverCfg, clientCfg *tls.Config) (serverLeaf, clientLeaf *x509.Certificate, err error) {
	t.Helper()
	ln, err := quic.ListenAddr("127.0.0.1:0", serverCfg, &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return dialListener(ln.Addr().String(), ln, clientCfg)
}

type acceptResult struct {
	conn *quic.Conn
	err  error
}

func dialListener(addr string, ln *quic.Listener, clientCfg *tls.Config) (serverLeaf, clientLeaf *x509.Certificate, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	accepted := make(chan acceptResult, 1)
	if ln != nil {
		go func() {
			c, aerr := ln.Accept(ctx)
			accepted <- acceptResult{conn: c, err: aerr}
		}()
	}
	cc := clientCfg.Clone()
	cc.ServerName = "127.0.0.1"
	conn, err := quic.DialAddr(ctx, addr, cc, &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = conn.CloseWithError(0, "") }()
	state := conn.ConnectionState().TLS
	if state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != tlsALPN {
		return nil, nil, errors.New("handshake did not negotiate TLS 1.3 with the quicraft ALPN")
	}
	serverLeaf = state.PeerCertificates[0]
	if ln == nil {
		return serverLeaf, nil, nil
	}
	select {
	case res := <-accepted:
		if res.err != nil {
			return nil, nil, res.err
		}
		defer func() { _ = res.conn.CloseWithError(0, "") }()
		return serverLeaf, res.conn.ConnectionState().TLS.PeerCertificates[0], nil
	case <-conn.Context().Done():
		// The server refused this client's certificate after the client
		// finished its half of the handshake.
		return nil, nil, context.Cause(conn.Context())
	}
}

// TestReloadMTLS_LeafReloadPresentsNewLeaf verifies that after a leaf reload
// the next handshake presents the new leaf in both roles, without a ticket
// from the earlier session resuming the old one.
func TestReloadMTLS_LeafReloadPresentsNewLeaf(t *testing.T) {
	ca := newReloadCA(t, "ca")
	key := newReloadKey(t)
	tr := newReloadTransport(t, mtlsFor(t, ca, key, 100, ca), nil)
	peer := peerPair(t, mtlsFor(t, ca, newReloadKey(t), 7, ca))
	listener := tr.listenerTLS()
	identity := tr.LocalNodeIdentity()

	got, _, err := handshake(t, listener, peer.client)
	if err != nil {
		t.Fatalf("handshake before reload: %v", err)
	}
	if got.SerialNumber.Int64() != 100 {
		t.Fatalf("before reload the server presented serial %d, want 100", got.SerialNumber.Int64())
	}

	if err := tr.ReloadMTLS(mtlsFor(t, ca, key, 101, ca), "", nil); err != nil {
		t.Fatalf("ReloadMTLS: %v", err)
	}
	if tr.LocalNodeIdentity() != identity {
		t.Fatal("a same-key reload changed the node identity")
	}

	// The same listener config and the same client session cache: the
	// listener's GetConfigForClient must hand out the new leaf, and the
	// earlier session must not resume the old one.
	got, _, err = handshake(t, listener, peer.client)
	if err != nil {
		t.Fatalf("inbound handshake after reload: %v", err)
	}
	if got.SerialNumber.Int64() != 101 {
		t.Fatalf("inbound: after reload the server presented serial %d, want 101", got.SerialNumber.Int64())
	}

	_, seen, err := handshake(t, peer.server, tr.currentTLS().client)
	if err != nil {
		t.Fatalf("outbound handshake after reload: %v", err)
	}
	if seen.SerialNumber.Int64() != 101 {
		t.Fatalf("outbound: after reload the client presented serial %d, want 101", seen.SerialNumber.Int64())
	}
}

// TestReloadMTLS_TicketsResumeOnlyWithinOnePair verifies the session-ticket
// hooks: without a reload a reconnect resumes (the 1-RTT reconnect is kept),
// and after one it does not.
func TestReloadMTLS_TicketsResumeOnlyWithinOnePair(t *testing.T) {
	ca := newReloadCA(t, "ca")
	key := newReloadKey(t)
	tr := newReloadTransport(t, mtlsFor(t, ca, key, 600, ca), nil)
	peer := peerPair(t, mtlsFor(t, ca, newReloadKey(t), 601, ca))
	ln, err := quic.ListenAddr("127.0.0.1:0", tr.listenerTLS(), &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	resumed := func() bool {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			if c, aerr := ln.Accept(ctx); aerr == nil {
				<-c.Context().Done()
			}
		}()
		cc := peer.client.Clone()
		cc.ServerName = "127.0.0.1"
		conn, derr := quic.DialAddr(ctx, ln.Addr().String(), cc, &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
		if derr != nil {
			t.Fatalf("dial: %v", derr)
		}
		// The ticket arrives after the handshake; give it a moment.
		time.Sleep(50 * time.Millisecond)
		did := conn.ConnectionState().TLS.DidResume
		_ = conn.CloseWithError(0, "")
		return did
	}

	_ = resumed()
	if !resumed() {
		t.Fatal("a reconnect without a reload did not resume")
	}
	if err := tr.ReloadMTLS(mtlsFor(t, ca, key, 602, ca), "", nil); err != nil {
		t.Fatalf("ReloadMTLS: %v", err)
	}
	if resumed() {
		t.Fatal("a ticket issued before the reload resumed after it")
	}
}

// TestReloadMTLS_CAPoolReload verifies a CA-pool reload in both directions:
// once the old CA is dropped, a peer whose leaf only the old CA signed is
// refused inbound and outbound, and a peer signed by the new CA is accepted.
func TestReloadMTLS_CAPoolReload(t *testing.T) {
	oldCA := newReloadCA(t, "old")
	newCA := newReloadCA(t, "new")
	key := newReloadKey(t)
	tr := newReloadTransport(t, mtlsFor(t, oldCA, key, 200, oldCA), nil)
	listener := tr.listenerTLS()

	// Both peers trust both CAs, so only the transport under test decides.
	oldPeer := peerPair(t, mtlsFor(t, oldCA, newReloadKey(t), 201, oldCA, newCA))
	newPeer := peerPair(t, mtlsFor(t, newCA, newReloadKey(t), 202, oldCA, newCA))

	if _, _, err := handshake(t, listener, oldPeer.client); err != nil {
		t.Fatalf("old-CA peer refused before the reload: %v", err)
	}
	if _, _, err := handshake(t, listener, newPeer.client); err == nil {
		t.Fatal("new-CA peer accepted before the new CA was trusted")
	}

	// New pool only, leaf re-issued by the new CA on the same key.
	if err := tr.ReloadMTLS(mtlsFor(t, newCA, key, 203, newCA), "", nil); err != nil {
		t.Fatalf("ReloadMTLS: %v", err)
	}

	if _, _, err := handshake(t, listener, oldPeer.client); err == nil {
		t.Fatal("inbound: a peer signed only by the dropped CA was accepted")
	}
	if _, _, err := handshake(t, listener, newPeer.client); err != nil {
		t.Fatalf("inbound: new-CA peer refused after the reload: %v", err)
	}
	if _, _, err := handshake(t, oldPeer.server, tr.currentTLS().client); err == nil {
		t.Fatal("outbound: a server signed only by the dropped CA was accepted")
	}
	if _, _, err := handshake(t, newPeer.server, tr.currentTLS().client); err != nil {
		t.Fatalf("outbound: new-CA server refused after the reload: %v", err)
	}
}

// TestReloadMTLS_InvalidKeepsOld verifies that every refused reload returns
// a *MTLSReloadError naming its cause and leaves the material in force
// untouched.
func TestReloadMTLS_InvalidKeepsOld(t *testing.T) {
	ca := newReloadCA(t, "ca")
	key := newReloadKey(t)
	good := mtlsFor(t, ca, key, 300, ca)
	tr := newReloadTransport(t, good, nil)
	before := tr.currentTLS()

	otherKeyCert, otherKey := ca.issue(t, newReloadKey(t), 301)
	coveringCert, coveringKey := ca.issue(t, key, 303, net.IPv4(10, 9, 9, 2))
	sharedErr := &SharedPeerCertificateError{}

	tests := []struct {
		name  string
		mtls  *MTLSConfig
		peers []string
		want  error
	}{
		{"nil material", nil, nil, ErrMTLSConfigRequired},
		{"missing CA", &MTLSConfig{Cert: good.Cert, Key: good.Key}, nil, ErrIncompleteMTLSConfig},
		{"missing key", &MTLSConfig{CACert: good.CACert, Cert: good.Cert}, nil, ErrIncompleteMTLSConfig},
		{"garbage certificate", &MTLSConfig{CACert: good.CACert, Cert: []byte("junk"), Key: good.Key}, nil, ErrInvalidCertKeyPair},
		{"key does not match", &MTLSConfig{CACert: good.CACert, Cert: good.Cert, Key: otherKey}, nil, ErrInvalidCertKeyPair},
		{"empty CA pool", &MTLSConfig{CACert: []byte("no pem here"), Cert: good.Cert, Key: good.Key}, nil, ErrInvalidCACert},
		{"new keypair", &MTLSConfig{CACert: good.CACert, Cert: otherKeyCert, Key: otherKey}, nil, ErrNodeIdentityChanged},
		{"covers a peer", &MTLSConfig{CACert: good.CACert, Cert: coveringCert, Key: coveringKey},
			[]string{"10.9.9.2:63001"}, ErrSharedPeerCertificate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tr.ReloadMTLS(tc.mtls, "10.9.9.1:63001", tc.peers)
			var reloadErr *MTLSReloadError
			if !errors.As(err, &reloadErr) || !errors.Is(err, ErrMTLSReload) {
				t.Fatalf("ReloadMTLS = %v, want a *MTLSReloadError", err)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("ReloadMTLS = %v, want cause %v", err, tc.want)
			}
			if tr.currentTLS() != before {
				t.Fatal("a refused reload replaced the material in force")
			}
		})
	}
	if err := tr.ReloadMTLS(&MTLSConfig{CACert: good.CACert, Cert: coveringCert, Key: coveringKey},
		"10.9.9.1:63001", []string{"10.9.9.2:63001"}); !errors.As(err, &sharedErr) {
		t.Fatalf("covering reload = %v, want a *SharedPeerCertificateError inside", err)
	}

	// The old leaf is still what a handshake presents.
	peer := peerPair(t, mtlsFor(t, ca, newReloadKey(t), 304, ca))
	got, _, err := handshake(t, tr.listenerTLS(), peer.client)
	if err != nil {
		t.Fatalf("handshake after refused reloads: %v", err)
	}
	if got.SerialNumber.Int64() != 300 {
		t.Fatalf("after refused reloads the server presented serial %d, want 300", got.SerialNumber.Int64())
	}
}

// TestReloadMTLS_RevocationIssuers verifies that with revocation enabled a
// reload is refused when the bundle holds no issuer, as at start, and
// accepted when it does.
func TestReloadMTLS_RevocationIssuers(t *testing.T) {
	oldCA := newReloadCA(t, "old")
	newCA := newReloadCA(t, "new")
	key := newReloadKey(t)
	rev := &revocation.Config{OCSPResponderURL: "http://ocsp.example.com"}
	tr := newReloadTransport(t, mtlsFor(t, oldCA, key, 400, oldCA), rev)

	leafOnly, _ := newCA.issue(t, newReloadKey(t), 401)
	bad := mtlsFor(t, newCA, key, 402, newCA)
	bad.CACert = leafOnly
	if err := tr.ReloadMTLS(bad, "", nil); !errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("ReloadMTLS with an issuer-less bundle = %v, want ErrCACertificateParse", err)
	}
	if err := tr.ReloadMTLS(mtlsFor(t, newCA, key, 403, oldCA, newCA), "", nil); err != nil {
		t.Fatalf("ReloadMTLS with revocation enabled: %v", err)
	}
}

// TestReloadMTLS_ConcurrentWithHandshakes runs reloads against inbound and
// outbound handshakes. Under -race it proves the swap is safe; every
// handshake must succeed and present one of the two leaves.
func TestReloadMTLS_ConcurrentWithHandshakes(t *testing.T) {
	ca := newReloadCA(t, "ca")
	key := newReloadKey(t)
	materials := []*MTLSConfig{mtlsFor(t, ca, key, 500, ca), mtlsFor(t, ca, key, 501, ca)}
	tr := newReloadTransport(t, materials[0], nil)
	peer := peerPair(t, mtlsFor(t, ca, newReloadKey(t), 502, ca))

	ln, err := quic.ListenAddr("127.0.0.1:0", tr.listenerTLS(), &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, aerr := ln.Accept(context.Background())
			if aerr != nil {
				return
			}
			_ = c.CloseWithError(0, "")
		}
	}()
	peerLn, err := quic.ListenAddr("127.0.0.1:0", peer.server, &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("listen peer: %v", err)
	}
	defer func() { _ = peerLn.Close() }()
	go func() {
		for {
			c, aerr := peerLn.Accept(context.Background())
			if aerr != nil {
				return
			}
			_ = c.CloseWithError(0, "")
		}
	}()

	stop := make(chan struct{})
	var reloads sync.WaitGroup
	for r := 0; r < 2; r++ {
		reloads.Add(1)
		go func(r int) {
			defer reloads.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if rerr := tr.ReloadMTLS(materials[(i+r)%2], "", nil); rerr != nil {
					t.Errorf("ReloadMTLS: %v", rerr)
					return
				}
			}
		}(r)
	}

	var handshakes sync.WaitGroup
	for h := 0; h < 4; h++ {
		handshakes.Add(1)
		go func(h int) {
			defer handshakes.Done()
			for i := 0; i < 5; i++ {
				var (
					leaf *x509.Certificate
					herr error
				)
				if h%2 == 0 {
					leaf, _, herr = dialListener(ln.Addr().String(), nil, peer.client)
				} else {
					_, herr = quicDialOnly(peerLn.Addr().String(), tr.currentTLS().client)
					leaf = nil
				}
				if herr != nil {
					t.Errorf("handshake during reloads: %v", herr)
					return
				}
				if leaf != nil {
					if s := leaf.SerialNumber.Int64(); s != 500 && s != 501 {
						t.Errorf("handshake presented serial %d, want 500 or 501", s)
						return
					}
				}
			}
		}(h)
	}
	handshakes.Wait()
	close(stop)
	reloads.Wait()
}

// quicDialOnly dials addr with clientCfg and reports whether the client side
// of the handshake completed.
func quicDialOnly(addr string, clientCfg *tls.Config) (*x509.Certificate, error) {
	leaf, _, err := dialListener(addr, nil, clientCfg)
	return leaf, err
}
