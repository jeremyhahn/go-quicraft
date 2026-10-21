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
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// selfSignedMTLS issues a self-signed node certificate on key for dnsNames,
// so a test can reload a node onto different SANs under the same identity.
func selfSignedMTLS(t *testing.T, key *ecdsa.PrivateKey, serial int64, dnsNames ...string) *config.MTLSConfig {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "quicraft-reload-race-test"},
		DNSNames:              dnsNames,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
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
	return &config.MTLSConfig{
		CACert: certPEM,
		Cert:   certPEM,
		Key:    pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// TestHostReloadMTLS_SerializedWithPeerAdmission races a reload onto a
// certificate that covers a new peer's host against the membership add that
// admits that peer. Exactly one may win. Before the two were serialized, each
// checked SAN exclusivity against the other's stale state, both passed, and
// the node ended up holding a certificate that covers an admitted peer.
func TestHostReloadMTLS_SerializedWithPeerAdmission(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	const self = "node-a.raft.test:5000"
	base := selfSignedMTLS(t, key, 1, "node-a.raft.test")
	cfg := sanHostConfig(t, self, "node-a.raft.test")
	cfg.TransportConfig.MTLSConfig = base

	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()
	members := map[uint64]string{1: self}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfigWithIdentities(h, 1, 1, members)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	const iterations = 40
	reloadsWon, addsWon := 0, 0
	for i := range iterations {
		host := fmt.Sprintf("node-b%d.raft.test", i)
		addr := host + ":5000"
		replicaID := uint64(100 + i)
		covering := selfSignedMTLS(t, key, int64(2+i), "node-a.raft.test", host)

		var wg sync.WaitGroup
		var reloadErr, addErr error
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reloadErr = h.ReloadMTLS(covering)
		}()
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			addErr = h.SyncRequestAddObserverWithIdentity(ctx, 1, replicaID, addr, testIdentity(replicaID), 0)
		}()
		close(start)
		wg.Wait()

		switch {
		case reloadErr == nil && addErr == nil:
			t.Fatalf("iteration %d: the reload of a certificate covering %s and the admission of a peer there both succeeded", i, host)
		case reloadErr == nil:
			if !errors.Is(addErr, ErrSharedPeerCertificate) {
				t.Fatalf("iteration %d: add after a covering reload = %v, want ErrSharedPeerCertificate", i, addErr)
			}
			reloadsWon++
		case addErr == nil:
			if !errors.Is(reloadErr, ErrSharedPeerCertificate) {
				t.Fatalf("iteration %d: reload after the add = %v, want ErrSharedPeerCertificate", i, reloadErr)
			}
			addsWon++
		default:
			t.Fatalf("iteration %d: both failed: reload %v, add %v", i, reloadErr, addErr)
		}

		// The invariant itself: the active certificate covers no admitted peer.
		var peers []string
		for _, a := range h.registry.Addresses() {
			if a != "" && a != self {
				peers = append(peers, a)
			}
		}
		if err := h.transport.VerifyPeerAddressExclusivity(self, peers); err != nil {
			t.Fatalf("iteration %d: the active certificate covers an admitted peer: %v", i, err)
		}
		if reloadErr == nil {
			if err := h.ReloadMTLS(base); err != nil {
				t.Fatalf("iteration %d: reload back to the per-node certificate: %v", i, err)
			}
		}
	}
	t.Logf("reloads won %d, adds won %d of %d", reloadsWon, addsWon, iterations)
}

// TestAdmitMemberAddresses_ReleaseIsIdempotentAndCounted pins the admitting
// set's bookkeeping: two admissions of one address hold it until both
// release, and a second release of the same admission is a no-op.
func TestAdmitMemberAddresses_ReleaseIsIdempotentAndCounted(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	peer := map[uint64]string{2: "node-b.raft.test:5000"}
	first, err := h.admitMemberAddresses(peer)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	second, err := h.admitMemberAddresses(peer)
	if err != nil {
		t.Fatalf("admit again: %v", err)
	}
	first()
	first()
	if n := h.admittingPeers["node-b.raft.test:5000"]; n != 1 {
		t.Fatalf("admitting count after one release (twice) = %d, want 1", n)
	}
	second()
	if len(h.admittingPeers) != 0 {
		t.Fatalf("admitting set after every release = %v, want empty", h.admittingPeers)
	}

	// Self only: nothing to check, nothing recorded.
	release, err := h.admitMemberAddresses(map[uint64]string{1: "node-a.raft.test:5000"})
	if err != nil {
		t.Fatalf("admit self: %v", err)
	}
	release()
}
