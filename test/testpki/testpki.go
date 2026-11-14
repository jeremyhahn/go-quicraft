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

// Package testpki issues the ephemeral certificates the integration suites
// run their clusters under: one CA per cluster, one leaf — one KEYPAIR — per
// host.
//
// The per-host keypair is not a nicety. A member is authorized on the receive
// path by its committed node identity, the SHA-256 of its certificate's
// SubjectPublicKeyInfo, and by nothing else (config.Config.RequireMemberIdentities).
// Hosts that share a leaf share an identity, so every one of them authenticates
// as every other: the promiscuous-certificate shape identities exist to defeat,
// and one the membership validator refuses to commit twice
// (raft.ErrDuplicateMemberIdentity). This package is the one generator every
// suite shares, so no suite can hand every host the same leaf.
package testpki

import (
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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// CA is an ephemeral ECDSA P-256 certificate authority for one test cluster.
type CA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	certPE []byte
	serial int64
	// crlNumber is the CRL number of the last list CRL issued.
	crlNumber int64
}

// NewCA generates a fresh CA. Every leaf it issues verifies against it and
// against nothing else, so two clusters in one test binary cannot talk to each
// other by accident.
func NewCA(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testpki: generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"QuicRaft Test"}},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("testpki: create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testpki: parse CA cert: %v", err)
	}
	return &CA{
		cert:   cert,
		key:    key,
		certPE: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		serial: 1,
	}
}

// Leaf issues a host certificate on a FRESH keypair, covering localhost and
// every loopback address, and returns it as the MTLSConfig a Host is started
// with. Each call is a distinct node identity; a host that restarts must be
// given the same MTLSConfig again to keep the identity the shard committed
// for it.
func (ca *CA) Leaf(t testing.TB) *config.MTLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testpki: generate leaf key: %v", err)
	}
	return ca.sign(t, key)
}

// Reissue signs a new leaf for the key of leaf, issued by this CA and trusting
// only this CA. The node identity (the SHA-256 of the public key) is
// unchanged, which is what a CA rollover reload requires.
func (ca *CA) Reissue(t testing.TB, leaf *config.MTLSConfig) *config.MTLSConfig {
	t.Helper()
	block, _ := pem.Decode(leaf.Key)
	if block == nil {
		t.Fatal("testpki: reissue: leaf key is not PEM")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("testpki: reissue: parse leaf key: %v", err)
	}
	return ca.sign(t, key)
}

// PEM returns the CA certificate, PEM-encoded, for building trust bundles.
func (ca *CA) PEM() []byte {
	return ca.certPE
}

func (ca *CA) sign(t testing.TB, key *ecdsa.PrivateKey) *config.MTLSConfig {
	t.Helper()
	ca.serial++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(ca.serial),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft Test"},
			CommonName:   "localhost",
		},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("testpki: create leaf cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("testpki: marshal leaf key: %v", err)
	}
	return &config.MTLSConfig{
		CACert: ca.certPE,
		Cert:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		Key:    pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// Leaves issues n distinct leaves.
func (ca *CA) Leaves(t testing.TB, n int) []*config.MTLSConfig {
	t.Helper()
	out := make([]*config.MTLSConfig, n)
	for i := range out {
		out[i] = ca.Leaf(t)
	}
	return out
}

// CRL returns a PEM-encoded ("X509 CRL") certificate revocation list signed
// by ca that revokes the leaf certificate of each of revoked. With no
// arguments it is an empty, valid CRL. Every call carries a fresh, increasing
// CRL number, and the list is valid from a minute ago for an hour, so a
// checker that refuses a stale CRL accepts it.
func (ca *CA) CRL(t testing.TB, revoked ...*config.MTLSConfig) []byte {
	t.Helper()
	now := time.Now()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for i, m := range revoked {
		block, _ := pem.Decode(m.Cert)
		if block == nil {
			t.Fatalf("testpki: revoked[%d]: no PEM certificate", i)
		}
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("testpki: revoked[%d]: parse leaf: %v", i, err)
		}
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   leaf.SerialNumber,
			RevocationTime: now.Add(-1 * time.Minute),
		})
	}
	ca.crlNumber++
	tmpl := &x509.RevocationList{
		RevokedCertificateEntries: entries,
		Number:                    big.NewInt(ca.crlNumber),
		ThisUpdate:                now.Add(-1 * time.Minute),
		NextUpdate:                now.Add(1 * time.Hour),
	}
	der, err := x509.CreateRevocationList(rand.Reader, tmpl, ca.cert, ca.key)
	if err != nil {
		t.Fatalf("testpki: create CRL: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der})
}

// Files is a leaf written to disk, for adapters that take their mTLS
// material as file paths.
type Files struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// LeafFiles issues a leaf (see Leaf) and writes it, with the CA certificate,
// under dir. Each call is a distinct node identity.
func (ca *CA) LeafFiles(t testing.TB, dir string) Files {
	t.Helper()
	leaf := ca.Leaf(t)
	ca.serial++
	prefix := filepath.Join(dir, fmt.Sprintf("leaf-%d", ca.serial))
	f := Files{
		CAFile:   filepath.Join(dir, "ca.pem"),
		CertFile: prefix + "-cert.pem",
		KeyFile:  prefix + "-key.pem",
	}
	for _, w := range []struct {
		path string
		data []byte
	}{{f.CAFile, leaf.CACert}, {f.CertFile, leaf.Cert}, {f.KeyFile, leaf.Key}} {
		if err := os.WriteFile(w.path, w.data, 0o600); err != nil {
			t.Fatalf("testpki: write %s: %v", w.path, err)
		}
	}
	return f
}
