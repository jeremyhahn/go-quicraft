// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

const (
	crlShard         = 955
	crlRTTMillis     = 20
	crlCheckInterval = 100 * time.Millisecond
	crlProposals     = 8
)

// crlCA is a test CA whose key stays in this file, so the test can sign CRLs
// as well as the leaf certificates the Hosts present.
type crlCA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	certPE []byte
}

func newCRLCA(t *testing.T) *crlCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"QuicRaft CRL Test"}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &crlCA{cert: cert, key: key, certPE: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// leaf issues a leaf certificate with the given serial for a Host on the
// loopback address.
func (ca *crlCA) leaf(t *testing.T, serial *big.Int) *config.MTLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{Organization: []string{"QuicRaft CRL Test"}, CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	return &config.MTLSConfig{
		CACert: ca.certPE,
		Cert:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		Key:    pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// writeCRL writes a PEM CRL signed by the CA that lists the revoked serials.
// The file is replaced by rename so a reload never reads a partial write.
func (ca *crlCA) writeCRL(t *testing.T, path string, number int64, revoked ...*big.Int) {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, serial := range revoked {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   serial,
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
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CRL: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename CRL: %v", err)
	}
}

// revokedRecorder records every serial one Host's OnCertRevoked reports.
type revokedRecorder struct {
	mu      sync.Mutex
	serials map[string]int
}

func (r *revokedRecorder) record(serial string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serials[serial]++
}

func (r *revokedRecorder) count(serial string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.serials[serial]
}

func (r *revokedRecorder) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.serials))
	for k, v := range r.serials {
		out[k] = v
	}
	return out
}

// crlAwait polls cond until it holds or ctx ends, and fails the test with
// what when ctx ends first.
func crlAwait(ctx context.Context, t *testing.T, what string, cond func() bool) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting: %s", what)
		case <-tick.C:
		}
	}
}

// crlApplied returns the applied index and state hash of one replica, or
// zeros when it has applied nothing yet.
func crlApplied(h *Host) (index, hash uint64) {
	hash, index, ok, err := h.StateMachineHash(crlShard)
	if err != nil || !ok {
		return 0, 0
	}
	return index, hash
}

// crlLeader returns the leader one replica reports, or 0.
func crlLeader(h *Host) uint64 {
	id, _, ready, err := h.LeaderIDAndTerm(crlShard)
	if err != nil || !ready {
		return 0
	}
	return id
}

// crlPropose commits n puts through whichever of the first two Hosts leads.
func crlPropose(ctx context.Context, t *testing.T, hosts []*Host, n int) {
	t.Helper()
	for i := 0; i < n; {
		if ctx.Err() != nil {
			t.Fatalf("committed %d of %d proposals before the deadline", i, n)
		}
		target := hosts[0]
		if id := crlLeader(hosts[0]); id == 2 {
			target = hosts[1]
		}
		key := fmt.Sprintf("crl-%d", i)
		cmd, err := kv.EncodePut([]byte(key), []byte(key))
		if err != nil {
			t.Fatalf("EncodePut: %v", err)
		}
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = target.SyncPropose(pctx, crlShard, cmd)
		cancel()
		if err != nil {
			continue
		}
		i++
	}
}

// TestHost_CRLRevocation_RefusesRevokedPeerAndAdmitsItAfterReload checks the
// documented RevocationConfig behaviour through three Hosts: a peer whose
// certificate the CRL lists is refused at handshake and its serial reported
// to OnCertRevoked in lowercase hex; once the CRL file is rewritten without
// it, the next CheckInterval reload admits it and its replica catches up.
func TestHost_CRLRevocation_RefusesRevokedPeerAndAdmitsItAfterReload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ca := newCRLCA(t)
	// Serials with hex letters prove the reported form is lowercase.
	serials := []*big.Int{big.NewInt(0xA1B2C3D4E5), big.NewInt(0xB1C2D3E4F5), big.NewInt(0xC1D2E3F4A5)}
	crlPath := filepath.Join(t.TempDir(), "peers.crl")
	ca.writeCRL(t, crlPath, 1, serials[2])

	hosts := make([]*Host, 0, 3)
	recorders := make([]*revokedRecorder, 3)
	t.Cleanup(func() {
		for _, h := range hosts {
			h.Close()
		}
	})
	members := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	for i := range 3 {
		rec := &revokedRecorder{serials: make(map[string]int)}
		recorders[i] = rec
		h, err := NewHost(config.HostConfig{
			WALDir:         t.TempDir(),
			NodeHostDir:    t.TempDir(),
			RaftAddress:    "127.0.0.1:0",
			ListenAddress:  "127.0.0.1:0",
			RTTMillisecond: crlRTTMillis,
			DeploymentID:   1,
			NoSyncWAL:      true,
			TransportConfig: config.TransportConfig{
				MTLSConfig: ca.leaf(t, serials[i]),
				RevocationConfig: &config.RevocationConfig{
					Mode:          "crl",
					CRLPaths:      []string{crlPath},
					CheckInterval: crlCheckInterval,
					OnCertRevoked: rec.record,
				},
			},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		hosts = append(hosts, h)
		members[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range hosts {
		scfg := testShardConfig(crlShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}

	revokedHex := fmt.Sprintf("%x", serials[2])

	// (a) The first two Hosts elect a leader between them and commit.
	crlAwait(ctx, t, "a leader among replicas one and two", func() bool {
		id := crlLeader(hosts[0])
		return (id == 1 || id == 2) && crlLeader(hosts[1]) == id
	})
	crlPropose(ctx, t, hosts[:2], crlProposals)
	var committed, committedHash uint64
	crlAwait(ctx, t, "replicas one and two applying the same state", func() bool {
		i0, h0 := crlApplied(hosts[0])
		i1, h1 := crlApplied(hosts[1])
		committed, committedHash = i0, h0
		return i0 > 0 && i0 == i1 && h0 == h1
	})
	crlAwait(ctx, t, "OnCertRevoked on Host one or two reporting "+revokedHex, func() bool {
		return recorders[0].count(revokedHex) > 0 || recorders[1].count(revokedHex) > 0
	})
	if idx, _ := crlApplied(hosts[2]); idx >= committed {
		t.Fatalf("revoked replica three applied index %d, want below the committed %d", idx, committed)
	}

	// (b) An empty CRL revokes nothing: replica three catches up.
	ca.writeCRL(t, crlPath, 2)
	crlAwait(ctx, t, "replica three catching up after the CRL no longer lists it", func() bool {
		idx, hash := crlApplied(hosts[2])
		i0, h0 := crlApplied(hosts[0])
		return idx >= committed && idx == i0 && hash == h0
	})
	if idx, hash := crlApplied(hosts[2]); idx == committed && hash != committedHash {
		t.Fatalf("replica three state at index %d differs from the committed state", idx)
	}

	// (c) OnCertRevoked never reports the first or second Host.
	for i := range 2 {
		good := fmt.Sprintf("%x", serials[i])
		for j, rec := range recorders {
			if n := rec.count(good); n != 0 {
				t.Errorf("Host %d OnCertRevoked reported Host %d serial %s %d times (all: %v)", j+1, i+1, good, n, rec.snapshot())
			}
		}
	}
}
