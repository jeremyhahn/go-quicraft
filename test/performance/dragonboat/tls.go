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

package dragonboat_perf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
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
)

// tlsCertBundle holds paths to generated TLS certificate files for a
// single node in a benchmark cluster.
type tlsCertBundle struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// generateTLSCerts generates a self-signed ECDSA P-256 CA and numNodes
// node certificates written to PEM files in dir. Each node cert includes
// 127.0.0.1 SAN for localhost Dragonboat transport and both server/client
// extended key usage for MutualTLS.
func generateTLSCerts(b *testing.B, dir string, numNodes int) []tlsCertBundle {
	b.Helper()

	// Generate CA key pair.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		b.Fatalf("generate CA key: %v", err)
	}

	caSerial, _ := crand.Int(crand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "bench-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	caCertDER, err := x509.CreateCertificate(crand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		b.Fatalf("create CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		b.Fatalf("parse CA cert: %v", err)
	}

	caFile := filepath.Join(dir, "ca.pem")
	writePEM(b, caFile, "CERTIFICATE", caCertDER)

	bundles := make([]tlsCertBundle, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeKey, keyErr := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
		if keyErr != nil {
			b.Fatalf("generate node %d key: %v", i, keyErr)
		}

		serial, _ := crand.Int(crand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		template := &x509.Certificate{
			SerialNumber: serial,
			Subject:      pkix.Name{CommonName: fmt.Sprintf("bench-node-%d", i)},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{
				x509.ExtKeyUsageServerAuth,
				x509.ExtKeyUsageClientAuth,
			},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			DNSNames:    []string{"localhost"},
		}

		certDER, certErr := x509.CreateCertificate(crand.Reader, template, caCert, &nodeKey.PublicKey, caKey)
		if certErr != nil {
			b.Fatalf("create node %d cert: %v", i, certErr)
		}

		certFile := filepath.Join(dir, fmt.Sprintf("node-%d-cert.pem", i))
		writePEM(b, certFile, "CERTIFICATE", certDER)

		keyDER, marshalErr := x509.MarshalECPrivateKey(nodeKey)
		if marshalErr != nil {
			b.Fatalf("marshal node %d key: %v", i, marshalErr)
		}
		keyFile := filepath.Join(dir, fmt.Sprintf("node-%d-key.pem", i))
		writePEM(b, keyFile, "EC PRIVATE KEY", keyDER)

		bundles[i] = tlsCertBundle{
			CAFile:   caFile,
			CertFile: certFile,
			KeyFile:  keyFile,
		}
	}

	return bundles
}

// writePEM encodes the given DER data as a PEM block and writes it to path.
func writePEM(b *testing.B, path, blockType string, data []byte) {
	b.Helper()
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}); err != nil {
		b.Fatalf("write PEM %s: %v", path, err)
	}
}
