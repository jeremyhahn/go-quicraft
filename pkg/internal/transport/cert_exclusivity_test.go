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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/quic-go/quic-go"
)

// sharedSANMTLSConfig builds an mTLS identity whose LEAF carries the given
// DNS SANs in addition to loopback. This is the shape of a cluster issued one
// wildcard certificate, or one certificate carrying every node's SAN: both are
// routine operational practice and both validate perfectly against the CA.
func sharedSANMTLSConfig(t *testing.T, dnsNames ...string) *MTLSConfig {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Shared SAN Test CA"}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{Organization: []string{"Shared SAN Test Node"}},
		DNSNames:              dnsNames,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	return &MTLSConfig{
		CACert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		Cert:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		Key:    pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER}),
	}
}

// leafOf parses the leaf certificate out of an MTLSConfig.
func leafOf(t *testing.T, mtls *MTLSConfig) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(mtls.Cert)
	if block == nil {
		t.Fatal("PEM decode of leaf certificate failed")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}
	return leaf
}

// TestInboundAuthorizedFor_WildcardCertCoversEveryPeer characterises the ACTUAL
// strength boundary of the in-band authorization gate, which the existing tests
// do not: they only ever present a certificate that does NOT cover the claimed
// address (TestEvictStaleOutboundOnRecv_UnauthorizedAddressIgnored uses a
// TEST-NET-1 host), so they prove the gate rejects what it obviously must while
// saying nothing about what it accepts.
//
// inboundAuthorizedFor asks only whether the claimed host is PRESENT in the
// sender's certificate. Under one wildcard certificate the answer is yes for
// EVERY peer address, so the peer authenticated on this connection is
// authorized to speak for a third party. That is the collapse the startup
// exclusivity check (TestVerifyPeerAddressExclusivity_*) exists to prevent.
func TestInboundAuthorizedFor_WildcardCertCoversEveryPeer(t *testing.T) {
	// One wildcard identity shared by the whole cluster, plus loopback so the
	// test can complete a real handshake over 127.0.0.1.
	mtls := sharedSANMTLSConfig(t, "*.raft.test")

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	conn, dialErr := t1.dialConnection(t2.Addr().String(), "")
	if dialErr != nil {
		t.Fatalf("dialConnection failed: %v", dialErr)
	}

	// A THIRD party's address. The peer on this connection has no relationship
	// to it whatsoever, yet the gate authorizes it.
	const victim = "node-victim.raft.test:5000"
	if !t1.inboundAuthorizedFor(conn, victim) {
		t.Fatalf("expected the wildcard certificate to authorize %q "+
			"(the gate checks SAN presence, not exclusivity); if this now "+
			"fails the gate has been strengthened and this test must be "+
			"updated to match", victim)
	}

	// Same answer through the cached form, which is what the receive path and
	// the eviction path actually call.
	if !t1.inboundAuthorizedForCached(conn, victim) {
		t.Fatalf("cached gate disagreed with inboundAuthorizedFor for %q", victim)
	}
}

// TestVerifyPeerAddressExclusivity_WildcardRejected proves the startup check
// fails CLOSED for the exact deployment the gate above cannot defend: one
// wildcard certificate covering every member.
func TestVerifyPeerAddressExclusivity_WildcardRejected(t *testing.T) {
	mtls := sharedSANMTLSConfig(t, "*.raft.test")
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	err = tr.VerifyPeerAddressExclusivity("node-a.raft.test:5000", []string{
		"node-b.raft.test:5000",
		"node-c.raft.test:5000",
	})
	if err == nil {
		t.Fatal("wildcard certificate covering every peer was accepted; " +
			"one compromised replica can speak for every node")
	}
	if !errors.Is(err, ErrSharedPeerCertificate) {
		t.Fatalf("error = %v, want ErrSharedPeerCertificate", err)
	}
	var shared *SharedPeerCertificateError
	if !errors.As(err, &shared) {
		t.Fatalf("error %v is not a *SharedPeerCertificateError", err)
	}
	if len(shared.CoveredPeers) != 2 ||
		shared.CoveredPeers[0] != "node-b.raft.test" ||
		shared.CoveredPeers[1] != "node-c.raft.test" {
		t.Fatalf("CoveredPeers = %v, want [node-b.raft.test node-c.raft.test]",
			shared.CoveredPeers)
	}
}

// TestVerifyPeerAddressExclusivity_SharedMultiSANRejected covers the other
// routine form of the same mistake: not a wildcard, but one certificate that
// enumerates every node's SAN.
func TestVerifyPeerAddressExclusivity_SharedMultiSANRejected(t *testing.T) {
	mtls := sharedSANMTLSConfig(t, "node-a.raft.test", "node-b.raft.test", "node-c.raft.test")
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	err = tr.VerifyPeerAddressExclusivity("node-a.raft.test:5000",
		[]string{"node-b.raft.test:5000"})
	if !errors.Is(err, ErrSharedPeerCertificate) {
		t.Fatalf("error = %v, want ErrSharedPeerCertificate", err)
	}
}

// TestVerifyPeerAddressExclusivity_PerNodeCertAccepted proves the correct
// deployment — one certificate per node, naming only that node — passes.
func TestVerifyPeerAddressExclusivity_PerNodeCertAccepted(t *testing.T) {
	mtls := sharedSANMTLSConfig(t, "node-a.raft.test")
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}

	if err := tr.VerifyPeerAddressExclusivity("node-a.raft.test:5000", []string{
		"node-b.raft.test:5000",
		"node-c.raft.test:5000",
	}); err != nil {
		t.Fatalf("per-node certificate rejected: %v", err)
	}
}

// TestVerifyCertificateExclusivity_Table pins the remaining rules of the check.
func TestVerifyCertificateExclusivity_Table(t *testing.T) {
	wildcard := leafOf(t, sharedSANMTLSConfig(t, "*.raft.test"))
	perNode := leafOf(t, sharedSANMTLSConfig(t, "node-a.raft.test"))

	tests := []struct {
		name     string
		leaf     *x509.Certificate
		self     string
		peers    []string
		wantFail bool
	}{
		{
			name: "co-located peers on our own host are not flagged",
			// A certificate is matched on HOST alone, so replicas separated
			// only by port cannot be told apart by any certificate policy.
			// Flagging them would fail every single-machine deployment while
			// telling the operator nothing actionable.
			leaf:     wildcard,
			self:     "127.0.0.1:5000",
			peers:    []string{"127.0.0.1:5001", "127.0.0.1:5002"},
			wantFail: false,
		},
		{
			name:     "loopback SAN still flags a distinct covered host",
			leaf:     wildcard,
			self:     "127.0.0.1:5000",
			peers:    []string{"127.0.0.1:5001", "node-b.raft.test:5000"},
			wantFail: true,
		},
		{
			name:     "no peers is vacuously fine",
			leaf:     wildcard,
			self:     "node-a.raft.test:5000",
			peers:    nil,
			wantFail: false,
		},
		{
			name:     "bare host without a port is still checked",
			leaf:     wildcard,
			self:     "node-a.raft.test",
			peers:    []string{"node-b.raft.test"},
			wantFail: true,
		},
		{
			name:     "per-node cert with an uncovered peer passes",
			leaf:     perNode,
			self:     "node-a.raft.test:5000",
			peers:    []string{"node-b.raft.test:5000"},
			wantFail: false,
		},
		{
			name: "per-node cert is not flagged by its own address repeated",
			leaf: perNode,
			self: "node-a.raft.test:5000",
			// Same host, different port: still self.
			peers:    []string{"node-a.raft.test:6000"},
			wantFail: false,
		},

		// The SKIP used a raw, case-sensitive string compare while the
		// MATCH used x509.VerifyHostname, which lower-cases names, tolerates
		// the trailing-dot FQDN form and canonicalises IP literals. Every row
		// below is a legitimate deployment that the mismatch REJECTED at
		// startup, with no configuration override available.
		{
			name: "co-located peer written in a different case",
			leaf: perNode,
			self: "node-a.raft.test:5000",
			// VerifyHostname matches this against the SAN; the skip did not.
			peers:    []string{"NODE-A.RAFT.TEST:5001"},
			wantFail: false,
		},
		{
			name:     "co-located peer written as an absolute FQDN",
			leaf:     perNode,
			self:     "node-a.raft.test:5000",
			peers:    []string{"node-a.raft.test.:5001"},
			wantFail: false,
		},
		{
			name: "this node's OWN entry listed by IP while RaftAddress is a hostname",
			// The members map routinely lists nodes by IP while RaftAddress is
			// a name. The raw compare made the node flag ITSELF and refuse to
			// start.
			leaf:     perNode,
			self:     "node-a.raft.test:5000",
			peers:    []string{"127.0.0.1:5000"},
			wantFail: false,
		},
		{
			name: "standard single-box deployment across IPv6 and IPv4 loopback",
			// A loopback address is reachable only from this machine, so a
			// member listed at one is co-located by construction.
			leaf:     perNode,
			self:     "[::1]:5000",
			peers:    []string{"127.0.0.1:5001", "[::1]:5002"},
			wantFail: false,
		},
		{
			name:     "equivalent IPv6 spellings of our own host",
			leaf:     perNode,
			self:     "[0:0:0:0:0:0:0:1]:5000",
			peers:    []string{"[::1]:5001"},
			wantFail: false,
		},
		{
			name: "normalisation does not weaken the check: a covered remote host still fails",
			// The whole point of the check. Case-folding a peer name must not
			// turn a genuinely remote covered host into a skip.
			leaf:     wildcard,
			self:     "node-a.raft.test:5000",
			peers:    []string{"NODE-B.RAFT.TEST:5000"},
			wantFail: true,
		},
		{
			name:     "normalisation does not weaken the check: FQDN form of a remote host still fails",
			leaf:     wildcard,
			self:     "node-a.raft.test:5000",
			peers:    []string{"node-b.raft.test.:5000"},
			wantFail: true,
		},
		{
			name: "a non-loopback IP peer is still checked",
			// Only LOOPBACK implies co-location. Any other IP the certificate
			// covers is a real exclusivity failure and must not be skipped
			// just because it is an IP literal. The test leaf carries :: in
			// its IPAddresses SANs.
			leaf:     perNode,
			self:     "node-a.raft.test:5000",
			peers:    []string{"[::]:5000"},
			wantFail: true,
		},
		{
			name: "every loopback form in 127.0.0.0/8 is co-located",
			// 127.0.0.2 is as unreachable from another machine as 127.0.0.1.
			leaf:     perNode,
			self:     "node-a.raft.test:5000",
			peers:    []string{"127.0.0.2:5000"},
			wantFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyCertificateExclusivity(tt.leaf, tt.self, tt.peers)
			if tt.wantFail && err == nil {
				t.Fatal("expected a SharedPeerCertificateError, got nil")
			}
			if !tt.wantFail && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestVerifyPeerAddressExclusivity_NoLeafFailsClosed proves the check refuses
// to answer "fine" when it cannot see this node's own certificate.
func TestVerifyPeerAddressExclusivity_NoLeafFailsClosed(t *testing.T) {
	tr := &QUICTransport{tlsPair: &tlsPair{}}
	if err := tr.VerifyPeerAddressExclusivity("node-a.raft.test:5000",
		[]string{"node-b.raft.test:5000"}); !errors.Is(err, ErrNoLocalCertificate) {
		t.Fatalf("error = %v, want ErrNoLocalCertificate", err)
	}
}

// TestBuildMTLSTLS_PopulatesLeaf verifies the transport keeps its own parsed
// leaf, which is what makes the startup check possible at all.
func TestBuildMTLSTLS_PopulatesLeaf(t *testing.T) {
	mtls := sharedSANMTLSConfig(t, "node-a.raft.test")
	pair, err := buildMTLSTLS(mtls, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}
	if pair.leaf == nil {
		t.Fatal("tlsPair.leaf is nil; the startup exclusivity check cannot run")
	}
	if pair.leaf.VerifyHostname("node-a.raft.test") != nil {
		t.Fatal("parsed leaf does not carry the expected SAN")
	}
}

// TestEvictStaleOutboundOnRecv_EvictionBudget proves one authenticated peer
// cannot force an unbounded number of connection teardowns.
//
// Past the supersededEpochRing hysteresis, a peer stamping a fresh SourceEpoch
// on every batch could otherwise force a full QUIC handshake plus stream-pool
// teardown PER BATCH — arbitrary attacker-driven work, aimed at whichever address the
// certificate covers. The budget caps that at evictionBudget per window; beyond
// it the hint is ignored and recovery degrades to the QUIC MaxIdleTimeout path.
func TestEvictStaleOutboundOnRecv_EvictionBudget(t *testing.T) {
	mtls := testMTLSConfig(t)

	t1, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(1) failed: %v", err)
	}
	if err := t1.Start(); err != nil {
		t.Fatalf("t1.Start failed: %v", err)
	}
	defer t1.Stop()

	t2, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(2) failed: %v", err)
	}
	if err := t2.Start(); err != nil {
		t.Fatalf("t2.Start failed: %v", err)
	}
	defer t2.Stop()

	target := t2.Addr().String()

	// The inbound the "peer" speaks on. Loopback IS covered by the shared test
	// certificate, so every claim below is authorized: the budget is the ONLY
	// thing standing between the peer and unbounded teardown work.
	inbound, dialErr := t1.dialConnection(target, "")
	if dialErr != nil {
		t.Fatalf("dialConnection(inbound) failed: %v", dialErr)
	}

	// A distinct live outbound per attempt: eviction closes the cached
	// connection, and a dead outbound takes the unconditional-eviction path
	// rather than the budgeted one.
	attempts := evictionBudget + 1
	cached := make([]*quic.Conn, 0, attempts)
	for i := 0; i < attempts; i++ {
		// dialConnection reuses a live cached entry, so drop it first to force
		// a genuinely new connection object.
		t1.connMu.Lock()
		delete(t1.conns, target)
		t1.connMu.Unlock()

		conn, err := t1.dialConnection(target, "")
		if err != nil {
			t.Fatalf("dialConnection(cached %d) failed: %v", i, err)
		}
		if conn == inbound {
			t.Fatalf("cached conn %d is the inbound conn; the same-connection "+
				"fast path would short-circuit the test", i)
		}
		cached = append(cached, conn)
	}

	for i, conn := range cached {
		t1.connMu.Lock()
		t1.conns[target] = conn
		t1.connEpoch[target] = 1 // never equals the claimed epochs below
		t1.connMu.Unlock()

		// A fresh, never-superseded epoch per batch: exactly the abusive
		// pattern the budget bounds.
		t1.evictStaleOutboundOnRecv(target, uint64(1000+i), inbound)

		t1.connMu.RLock()
		_, stillCached := t1.conns[target]
		t1.connMu.RUnlock()

		if i < evictionBudget {
			if stillCached {
				t.Fatalf("attempt %d within the budget did not evict; a genuine "+
					"peer restart must still be handled promptly", i+1)
			}
			continue
		}
		if !stillCached {
			t.Fatalf("attempt %d exceeded the budget of %d evictions per %s but "+
				"still tore down the connection: one peer can force a QUIC "+
				"handshake per batch", i+1, evictionBudget, evictionWindow)
		}
	}
}

// TG-26. The eviction budget must be keyed PER PEER ADDRESS.
//
// The budget bounds attacker-driven teardown work, but its OTHER job is
// isolation: a peer stuck in a crash loop — or one deliberately manufacturing
// epochs — exhausts its own budget and nothing else. A single global counter
// would satisfy every existing assertion about the cap while letting one
// flapping peer consume the budget every other peer needs to recover from its
// own genuine restart, converting a one-node fault into a cluster-wide
// reconnection stall. That is precisely the cascade the epoch mechanism exists
// to prevent, re-introduced by its own defence.
func TestAllowEviction_BudgetIsPerPeerAddress(t *testing.T) {
	tr := &QUICTransport{peerEpoch: make(map[string]*peerIncarnation)}
	now := time.Now()

	const flapping = "10.0.0.1:5000"
	const healthy = "10.0.0.2:5000"

	for i := 0; i < evictionBudget; i++ {
		if !tr.allowEviction(flapping, now) {
			t.Fatalf("eviction %d of %d refused for the flapping peer: a genuine "+
				"restart must be handled within the budget", i+1, evictionBudget)
		}
	}
	if tr.allowEviction(flapping, now) {
		t.Fatalf("the flapping peer exceeded %d evictions per %s and was still "+
			"granted one", evictionBudget, evictionWindow)
	}

	// A DIFFERENT peer restarts once, legitimately. It must get its own full
	// budget: the first peer's abuse is not charged to it.
	for i := 0; i < evictionBudget; i++ {
		if !tr.allowEviction(healthy, now) {
			t.Fatalf("eviction %d of %d refused for a second, healthy peer: one "+
				"flapping peer has consumed the recovery budget of every other "+
				"peer, so a real restart elsewhere cannot be acted on until the "+
				"%s window rolls over", i+1, evictionBudget, evictionWindow)
		}
	}
	if tr.allowEviction(healthy, now) {
		t.Fatal("the second peer's own budget was not enforced either")
	}

	// The window is also per peer: rolling the flapping peer's window forward
	// must restore its budget without touching the other's.
	later := now.Add(evictionWindow)
	if !tr.allowEviction(flapping, later) {
		t.Fatal("the flapping peer's budget did not refresh after its window elapsed")
	}
	if tr.allowEviction(healthy, now) {
		t.Fatal("rolling one peer's window refreshed another peer's budget")
	}
}

// TestPeerCertificateExclusivity_RefusesWildcardPeer is the RECEIVE-side half
// of the promiscuous-certificate control, and it is the half that is an attack
// control at all.
//
// The startup checks (TestVerifyPeerAddressExclusivity_* and the two in Host)
// inspect the LOCAL node's own certificate against addresses an operator handed
// the local API. A malicious joiner never calls them — it simply connects. Only
// a check on what ARRIVES can refuse it, and this is that check.
//
// What makes the case concrete: the peer presents a *.raft.example.com
// certificate, which covers this node's own host. A certificate that covers
// THIS member covers every member, so its holder can satisfy the
// committed-address binding for any replica of any shard and have forged
// quorum-bearing responses counted. The connection is refused before it can
// open a single stream.
func TestPeerCertificateExclusivity_RefusesWildcardPeer(t *testing.T) {
	wildcard := leafOf(t, sharedSANMTLSConfig(t, "*.raft.example.com"))
	perNode := leafOf(t, sharedSANMTLSConfig(t, "node-b.raft.example.com"))

	const selfAddr = "node-a.raft.example.com:5001"
	const peerAddr = "10.20.30.40:41000"

	err := peerCertificateExclusivity(wildcard, selfAddr, peerAddr)
	if !errors.Is(err, ErrPromiscuousPeerCertificate) {
		t.Fatalf("peerCertificateExclusivity(wildcard) = %v, want ErrPromiscuousPeerCertificate: "+
			"a certificate that covers this node's host covers every member's host, "+
			"so its holder can assert any replica's msg.From", err)
	}
	var promiscuous *PromiscuousPeerCertificateError
	if !errors.As(err, &promiscuous) {
		t.Fatalf("error is not a *PromiscuousPeerCertificateError: %T", err)
	}
	if promiscuous.SelfHost != "node-a.raft.example.com" {
		t.Fatalf("SelfHost = %q, want node-a.raft.example.com", promiscuous.SelfHost)
	}

	// The correctly-issued peer certificate — per-node SANs, covering only its
	// OWN host — is admitted. Without this the test would pass against a
	// production hunk that refused every connection.
	if err := peerCertificateExclusivity(perNode, selfAddr, peerAddr); err != nil {
		t.Fatalf("peerCertificateExclusivity(per-node cert) = %v, want nil: "+
			"a certificate scoped to its own host is exactly the required deployment", err)
	}
}

// TestPeerCertificateExclusivity_ExemptsCoLocatedPeers pins the exemption the
// refusal must keep, using the SAME predicate the startup check uses
// (isCoLocatedCertHost). A certificate is matched on host alone, so a peer
// sharing this machine cannot be issued one that excludes this node's host.
// Refusing it would break every single-machine and every loopback deployment —
// including the test suite — while telling the operator nothing they could act
// on.
func TestPeerCertificateExclusivity_ExemptsCoLocatedPeers(t *testing.T) {
	wildcard := leafOf(t, sharedSANMTLSConfig(t, "*.raft.example.com"))

	tests := []struct {
		name     string
		selfAddr string
		peerAddr string
	}{
		{"peer on the same host", "node-a.raft.example.com:5001", "node-a.raft.example.com:5002"},
		{"peer on loopback", "node-a.raft.example.com:5001", "127.0.0.1:41000"},
		{"self on loopback", "127.0.0.1:5001", "127.0.0.1:41000"},
		{"no self address configured", "", "10.20.30.40:41000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := peerCertificateExclusivity(wildcard, tt.selfAddr, tt.peerAddr); err != nil {
				t.Fatalf("peerCertificateExclusivity = %v, want nil: co-located peers "+
					"cannot be separated by any certificate policy", err)
			}
		})
	}
}

// TestNodeIdentityFromCertificate_StableAcrossRenewal pins the property that
// made SPKI the identity rather than a certificate fingerprint or serial: two
// certificates issued over the SAME keypair — a routine renewal — yield the
// SAME identity, so a renewal does not require a config change to re-commit it.
// Two DIFFERENT keypairs yield different identities, which is what makes the
// value an identity at all.
func TestNodeIdentityFromCertificate_StableAcrossRenewal(t *testing.T) {
	original := sharedSANMTLSConfig(t, "node-a.raft.example.com")
	leaf := leafOf(t, original)

	renewed := renewLeafOnSameKey(t, original)

	identity := NodeIdentityFromCertificate(leaf)
	if identity == "" {
		t.Fatal("NodeIdentityFromCertificate returned empty for a valid leaf")
	}
	if !strings.HasPrefix(identity, "spki:") {
		t.Fatalf("identity %q lacks the spki: scheme tag", identity)
	}
	if got := NodeIdentityFromCertificate(renewed); got != identity {
		t.Fatalf("renewal on the same keypair changed the identity:\n old %s\n new %s\n"+
			"an identity that changes on renewal needs a config change per rotation, "+
			"which guarantees the strict mode is turned off", identity, got)
	}

	other := leafOf(t, sharedSANMTLSConfig(t, "node-a.raft.example.com"))
	if NodeIdentityFromCertificate(other) == identity {
		t.Fatal("a distinct keypair produced the same identity; the value identifies nothing")
	}
	if NodeIdentityFromCertificate(nil) != "" {
		t.Fatal("a nil certificate must yield no identity, never a usable one")
	}
}

// renewLeafOnSameKey issues a second certificate over the private key in mtls,
// with a different serial and validity window — the shape of a routine
// certificate renewal.
func renewLeafOnSameKey(t *testing.T, mtls *MTLSConfig) *x509.Certificate {
	t.Helper()

	keyBlock, _ := pem.Decode(mtls.Key)
	if keyBlock == nil {
		t.Fatal("PEM decode of leaf key failed")
	}
	leafKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse leaf key: %v", err)
	}
	caBlock, _ := pem.Decode(mtls.CACert)
	if caBlock == nil {
		t.Fatal("PEM decode of CA certificate failed")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	// The renewal is self-signed by the leaf key: the test only reads the
	// SubjectPublicKeyInfo, and issuing under the original CA key would need
	// that key, which the fixture does not retain.
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(99),
		Subject:               caCert.Subject,
		DNSNames:              []string{"node-a.raft.example.com"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(2 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &leafKey.PublicKey, leafKey)
	if err != nil {
		t.Fatalf("create renewed cert: %v", err)
	}
	renewed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse renewed cert: %v", err)
	}
	return renewed
}

// TestUnnameableCertSelfHost_Table pins which advertised hosts leave the
// promiscuous-certificate refusal with no question to ask.
//
// The distinction matters in both directions. Classifying a real host as
// unnameable would silently disable a live security control; classifying a
// loopback host as unnameable would confuse "this deployment is exempt" (a
// documented property of co-located replicas) with "this control cannot run",
// which is what the operator warning is about.
func TestUnnameableCertSelfHost_Table(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{"", true},
		{"0.0.0.0", true},
		{"::", true},
		{"127.0.0.1", false},
		{"::1", false},
		{"node-a.raft.example.com", false},
		{"10.20.30.40", false},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := unnameableCertSelfHost(tt.host); got != tt.want {
				t.Fatalf("unnameableCertSelfHost(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}

// TestPeerCertificateExclusivity_InertOnWildcardBind MEASURES the premise
// behind the operator warning: on a wildcard bind with no AdvertiseAddress, the
// promiscuous-certificate refusal admits the very certificate it exists to
// refuse.
//
// This is not an assertion that the behaviour is desirable — failing closed
// here would take down every wildcard-bound deployment on upgrade, which is an
// operator's decision. It pins the fact so the warning
// (reportPromiscuousCertCoverage) cannot be deleted as redundant, and so that a
// later change to fail closed shows up here as a deliberate edit rather than as
// a silent behaviour change.
func TestPeerCertificateExclusivity_InertOnWildcardBind(t *testing.T) {
	wildcard := leafOf(t, sharedSANMTLSConfig(t, "*.raft.example.com"))
	const peerAddr = "10.20.30.40:41000"

	// CONTROL: with a real advertised host the same certificate IS refused.
	if err := peerCertificateExclusivity(wildcard, "node-a.raft.example.com:5001", peerAddr); err == nil {
		t.Fatal("the control case admitted a wildcard certificate; the " +
			"measurement below would say nothing about the wildcard BIND")
	}

	for _, selfAddr := range []string{"0.0.0.0:5001", "[::]:5001", ":5001"} {
		t.Run(selfAddr, func(t *testing.T) {
			if err := peerCertificateExclusivity(wildcard, selfAddr, peerAddr); err != nil {
				t.Fatalf("peerCertificateExclusivity(self=%q) = %v; if this now "+
					"refuses, the control has been changed to fail closed and "+
					"reportPromiscuousCertCoverage's WARN-only posture must be "+
					"revisited with it", selfAddr, err)
			}
		})
	}
}

// TestReportPromiscuousCertCoverage_FiresOnlyWhenInert pins the trigger for the
// operator warning: it must fire exactly when the refusal cannot ask its
// question, and stay silent otherwise — a warning on every healthy node is a
// warning nobody reads.
func TestReportPromiscuousCertCoverage_FiresOnlyWhenInert(t *testing.T) {
	tests := []struct {
		name             string
		listenAddress    string
		advertiseAddress string
		wantInert        bool
	}{
		{"wildcard bind, no advertise address", "0.0.0.0:0", "", true},
		{"wildcard bind rescued by advertise address", "0.0.0.0:0", "node-a.raft.example.com:5001", false},
		{"loopback bind", "127.0.0.1:0", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := NewQUICTransport(Config{
				ListenAddress:    tt.listenAddress,
				AdvertiseAddress: tt.advertiseAddress,
				DeploymentID:     42,
				MTLSConfig:       testMTLSConfig(t),
			}, newTestHandler(), registry.NewRegistry())
			if err != nil {
				t.Fatalf("NewQUICTransport: %v", err)
			}
			if err := tr.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer tr.Stop()

			selfHost := canonicalCertHost(certAuthHost(tr.localAddr()))
			if got := unnameableCertSelfHost(selfHost); got != tt.wantInert {
				t.Fatalf("resolved advertised host %q is unnameable=%v, want %v "+
					"(listen=%q advertise=%q)", selfHost, got, tt.wantInert,
					tt.listenAddress, tt.advertiseAddress)
			}
		})
	}
}
