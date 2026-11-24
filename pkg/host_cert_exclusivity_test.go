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

package quicraft

import (
	"context"
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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
)

// sanHostConfig returns a transport-enabled HostConfig whose mTLS leaf carries
// the given DNS SANs (plus loopback, so the listener still works) and which
// advertises raftAddress as this node's own address.
func sanHostConfig(t *testing.T, raftAddress string, dnsNames ...string) config.HostConfig {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "quicraft-san-test"},
		DNSNames:              dnsNames,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cfg := testHostConfig(t)
	cfg.ListenAddress = "127.0.0.1:0"
	cfg.RaftAddress = raftAddress
	cfg.TransportConfig.MTLSConfig = &config.MTLSConfig{
		CACert: certPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}
	return cfg
}

// TestStartShard_RejectsCertificateCoveringPeers verifies a certificate covering other members' hosts is refused.
func TestStartShard_RejectsCertificateCoveringPeers(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "node-c.raft.test:5000",
	}
	startErr := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members))
	if startErr == nil {
		t.Fatal("StartShard accepted a wildcard certificate covering every peer; " +
			"a single compromised replica can speak for every node")
	}
	// Match through the public re-exports.
	if !errors.Is(startErr, ErrSharedPeerCertificate) {
		t.Fatalf("StartShard error = %v, want ErrSharedPeerCertificate", startErr)
	}
	var shared *SharedPeerCertificateError
	if !errors.As(startErr, &shared) {
		t.Fatalf("StartShard error = %v (%T), want *SharedPeerCertificateError", startErr, startErr)
	}
	if len(shared.CoveredPeers) != 2 {
		t.Fatalf("CoveredPeers = %v, want the two other members' hosts", shared.CoveredPeers)
	}
	// The shard must not have been created by the rejected call.
	if _, loaded := h.nodes.Load(uint64(1)); loaded {
		t.Fatal("shard was created despite the certificate rejection")
	}
}

// TestStartShard_AcceptsPerNodeCertificate proves the correct deployment — one
// certificate per node, naming only that node — is unaffected.
func TestStartShard_AcceptsPerNodeCertificate(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "node-c.raft.test:5000",
	}
	if startErr := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members)); startErr != nil {
		t.Fatalf("StartShard rejected a correctly issued per-node certificate: %v", startErr)
	}
}

// TestStartShard_ColocatedReplicasNotRejected verifies replicas on one host differing only by port are accepted.
func TestStartShard_ColocatedReplicasNotRejected(t *testing.T) {
	cfg := sanHostConfig(t, "127.0.0.1:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "127.0.0.1:5000",
		2: "127.0.0.1:5001",
		3: "127.0.0.1:5002",
	}
	if startErr := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members)); startErr != nil {
		t.Fatalf("StartShard rejected co-located replicas: %v", startErr)
	}
}

// A shard grown by join must also be checked when members are added.
func TestRequestAddNode_RejectsCertificateCoveringTheNewPeer(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// The seed node of a grow-by-join cluster: itself only. StartShard's
	// check is vacuous here, which is the point.
	members := map[uint64]string{1: "node-a.raft.test:5000"}
	if startErr := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members)); startErr != nil {
		t.Fatalf("StartShard rejected the seed configuration: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, addErr := h.RequestAddNodeWithIdentity(ctx, 1, 2, "node-b.raft.test:5000", testIdentity(2), 0)
	if addErr == nil {
		t.Fatal("RequestAddNode accepted a peer this node's own wildcard " +
			"certificate covers: every in-band transport authorization " +
			"(SourceAddress clearing, peerEpoch/eviction, snapshot-sender " +
			"binding) now succeeds for that address from any replica")
	}
	if !errors.Is(addErr, ErrSharedPeerCertificate) {
		t.Fatalf("RequestAddNode error = %v, want ErrSharedPeerCertificate", addErr)
	}
	var shared *SharedPeerCertificateError
	if !errors.As(addErr, &shared) {
		t.Fatalf("RequestAddNode error = %v (%T), want *SharedPeerCertificateError", addErr, addErr)
	}

	// The membership must be unchanged: the rejection happens before the
	// config change is proposed.
	ms, msErr := h.SyncGetShardMembership(ctx, 1)
	if msErr != nil {
		t.Fatalf("SyncGetShardMembership: %v", msErr)
	}
	if _, added := ms.Nodes[2]; added {
		t.Fatal("the rejected replica was added to the membership anyway")
	}
}

// The same gate must not fire for a correctly issued per-node certificate, or
// growing a cluster by join would be impossible.
func TestRequestAddNode_AcceptsUncoveredPeerUnderPerNodeCertificate(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	seed := map[uint64]string{1: "node-a.raft.test:5000"}
	if startErr := h.StartShard(seed, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, seed)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rs, addErr := h.RequestAddNodeWithIdentity(ctx, 1, 2, "node-b.raft.test:5000", testIdentity(2), 0)
	if addErr != nil {
		t.Fatalf("RequestAddNode rejected an uncovered peer under a per-node "+
			"certificate: %v", addErr)
	}
	rs.Release()
}

// A co-located replica added later must not be flagged either, for the same
// reason StartShard does not flag one: the certificate cannot distinguish it.
func TestRequestAddNode_ColocatedPeerNotRejected(t *testing.T) {
	cfg := sanHostConfig(t, "127.0.0.1:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	seed := map[uint64]string{1: "127.0.0.1:5000"}
	if startErr := h.StartShard(seed, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, seed)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rs, addErr := h.RequestAddNodeWithIdentity(ctx, 1, 2, "127.0.0.1:5001", testIdentity(2), 0)
	if addErr != nil {
		t.Fatalf("RequestAddNode rejected a co-located replica: %v", addErr)
	}
	rs.Release()
}

// ---------------------------------------------------------------------------
// One address may name only one member of a shard
// ---------------------------------------------------------------------------

// TestStartShard_RejectsDuplicateMemberAddress verifies two replicas at one address are refused.
func TestStartShard_RejectsDuplicateMemberAddress(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "node-b.raft.test:5000", // the same host, seeded twice
	}
	startErr := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members))
	if startErr == nil {
		t.Fatal("StartShard accepted two replica IDs at one address: the single host " +
			"behind it authenticates as both, so it holds two votes and two match " +
			"indices in a quorum that believes they are independent")
	}
	if !errors.Is(startErr, ErrInvalidMember) {
		t.Fatalf("StartShard error = %v, want ErrInvalidMember", startErr)
	}
}

// TestStartShard_RejectsSpellingVariantDuplicateAddress verifies case variants of one address are refused.
func TestStartShard_RejectsSpellingVariantDuplicateAddress(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "NODE-B.RAFT.TEST:5000", // the same endpoint, spelled differently
	}
	startErr := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members))
	if startErr == nil {
		t.Fatal("StartShard accepted two spellings of one address: the certificate " +
			"cannot tell the two replica IDs apart, so the single host behind them " +
			"holds two votes and two match indices")
	}
	if !errors.Is(startErr, ErrInvalidMember) {
		t.Fatalf("StartShard error = %v, want ErrInvalidMember", startErr)
	}
}

// TestHostNodeIdentity_ReportsThisNodesOwnIdentity verifies NodeIdentity matches what peers see on its connections.
func TestHostNodeIdentity_ReportsThisNodesOwnIdentity(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	identity := h.NodeIdentity()
	if identity == "" {
		t.Fatal("NodeIdentity is empty for a host with a transport; an operator has " +
			"nothing to commit and the strict mode can never be satisfied")
	}
	if !strings.HasPrefix(identity, "spki:") {
		t.Fatalf("NodeIdentity = %q, want an spki:-tagged value", identity)
	}

	// Derived from this host's own leaf certificate.
	leaf := leafOfPEM(t, cfg.TransportConfig.MTLSConfig.Cert)
	if want := transport.NodeIdentityFromCertificate(leaf); identity != want {
		t.Fatalf("NodeIdentity = %q, but this host's leaf yields %q: a peer that "+
			"committed the reported value would reject every message this node sends",
			identity, want)
	}

	// A host with no transport has no certificate and therefore no identity.
	// It runs beside h, so it takes its own NodeHostDir: h holds the lock on
	// the first.
	cfg.NodeHostDir = t.TempDir()
	noTransport, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(WithoutTransport) failed: %v", err)
	}
	defer noTransport.Close()
	if got := noTransport.NodeIdentity(); got != "" {
		t.Fatalf("NodeIdentity on a transportless host = %q, want empty", got)
	}
}

// leafOfPEM parses the end-entity certificate out of a PEM block.
func leafOfPEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("PEM decode of leaf certificate failed")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}
	return leaf
}
