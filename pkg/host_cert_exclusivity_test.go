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

// TestStartShard_RejectsCertificateCoveringPeers is the regression guard for the
// in-band authorization collapse under a shared/wildcard cluster certificate.
//
// Three transport controls reduce to "is the claimed host present in the
// sender's verified certificate": clearing a spoofed SourceAddress, deciding who
// may mutate peerEpoch and evict a live outbound, and binding a snapshot sender
// to a shard member. Presence is only an authorization when SANs are EXCLUSIVE
// per node; under one wildcard certificate every check succeeds for every peer,
// so one compromised replica can claim any peer's address with a fresh
// SourceEpoch per batch and drive every node to evict its healthy outbound to a
// third-party victim.
//
// StartShard must therefore refuse to run at all. It fails CLOSED rather than
// warning: the weakness is invisible from the running cluster, the operator has
// no signal to act on, and the only remediation (re-issued certificates) has to
// happen before the cluster carries data.
func TestStartShard_RejectsCertificateCoveringPeers(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "node-c.raft.test:5000",
	}
	startErr := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	if startErr == nil {
		t.Fatal("StartShard accepted a wildcard certificate covering every peer; " +
			"a single compromised replica can speak for every node")
	}
	if !errors.Is(startErr, transport.ErrSharedPeerCertificate) {
		t.Fatalf("StartShard error = %v, want transport.ErrSharedPeerCertificate", startErr)
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

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "node-a.raft.test:5000",
		2: "node-b.raft.test:5000",
		3: "node-c.raft.test:5000",
	}
	if startErr := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard rejected a correctly issued per-node certificate: %v", startErr)
	}
}

// TestStartShard_ColocatedReplicasNotRejected pins the deliberate carve-out: a
// certificate is matched on HOST alone, so replicas separated only by port
// cannot be distinguished by any certificate policy. Rejecting them would break
// every single-machine deployment (and this package's own test suite) while
// telling the operator nothing they could act on.
func TestStartShard_ColocatedReplicasNotRejected(t *testing.T) {
	cfg := sanHostConfig(t, "127.0.0.1:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{
		1: "127.0.0.1:5000",
		2: "127.0.0.1:5001",
		3: "127.0.0.1:5002",
	}
	if startErr := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard rejected co-located replicas: %v", startErr)
	}
}

// M1. The grow-by-join flow never evaluated the precondition at all.
//
// StartShard checks the INITIAL members map. A cluster grown by join starts
// its seed node with members={self} — the peer list is empty after
// self-filtering, so the check returns nil — and every joiner with members={},
// which returns nil on the len()==0 guard. Under a wildcard certificate the
// deployment the check exists to refuse therefore comes up entirely clean, and
// the addresses that DO arrive later, through RequestAddNode, were never
// looked at.
func TestRequestAddNode_RejectsCertificateCoveringTheNewPeer(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "*.raft.test")

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// The seed node of a grow-by-join cluster: itself only. StartShard's
	// check is vacuous here, which is the point.
	members := map[uint64]string{1: "node-a.raft.test:5000"}
	if startErr := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard rejected the seed configuration: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, addErr := h.RequestAddNode(ctx, 1, 2, "node-b.raft.test:5000", 0)
	if addErr == nil {
		t.Fatal("RequestAddNode accepted a peer this node's own wildcard " +
			"certificate covers: every in-band transport authorization " +
			"(SourceAddress clearing, peerEpoch/eviction, snapshot-sender " +
			"binding) now succeeds for that address from any replica")
	}
	if !errors.Is(addErr, transport.ErrSharedPeerCertificate) {
		t.Fatalf("RequestAddNode error = %v, want transport.ErrSharedPeerCertificate", addErr)
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

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if startErr := h.StartShard(map[uint64]string{1: "node-a.raft.test:5000"},
		false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rs, addErr := h.RequestAddNode(ctx, 1, 2, "node-b.raft.test:5000", 0)
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

	h, err := NewHost(cfg, WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rs, addErr := h.RequestAddNode(ctx, 1, 2, "127.0.0.1:5001", 0)
	if addErr != nil {
		t.Fatalf("RequestAddNode rejected a co-located replica: %v", addErr)
	}
	rs.Release()
}
