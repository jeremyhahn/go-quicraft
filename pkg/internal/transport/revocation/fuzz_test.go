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

package revocation

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// crlFuzzSeeds builds the shapes the checker tests load: one CRL, several
// CRLs in one file, a CRL beside a non-CRL block, and a CRL block whose body
// is not DER.
func crlFuzzSeeds(f *testing.F) [][]byte {
	f.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		f.Fatalf("generate CA key: %v", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fuzz CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		f.Fatalf("create CA certificate: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		f.Fatalf("parse CA certificate: %v", err)
	}
	crl := func(number int64, serials ...int64) []byte {
		entries := make([]x509.RevocationListEntry, 0, len(serials))
		for _, s := range serials {
			entries = append(entries, x509.RevocationListEntry{
				SerialNumber:   big.NewInt(s),
				RevocationTime: now.Add(-time.Minute),
			})
		}
		raw, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
			Number:                    big.NewInt(number),
			ThisUpdate:                now.Add(-time.Minute),
			NextUpdate:                now.Add(time.Hour),
			RevokedCertificateEntries: entries,
		}, ca, key)
		if err != nil {
			f.Fatalf("create CRL: %v", err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: raw})
	}
	one := crl(1, 100)
	two := append(crl(2, 200), crl(3, 300, 301)...)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return [][]byte{
		one,
		two,
		append(append([]byte(nil), certPEM...), one...),
		crl(4),
		pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: []byte("not DER")}),
		one[:len(one)/2],
		{},
	}
}

// FuzzParseCRLs feeds arbitrary bytes to parseCRLs. It must never panic, and
// the CRLs it accepts must re-encode to PEM and parse back to the same DER.
func FuzzParseCRLs(f *testing.F) {
	for _, seed := range crlFuzzSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		crls, err := parseCRLs(data)
		if err != nil {
			if crls != nil {
				t.Fatalf("refused input returned %d CRLs", len(crls))
			}
			return
		}
		var enc []byte
		for i, c := range crls {
			if c == nil {
				t.Fatalf("CRL %d is nil", i)
			}
			enc = append(enc, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: c.Raw})...)
		}
		again, err := parseCRLs(enc)
		if err != nil {
			t.Fatalf("re-encoded CRLs refused: %v", err)
		}
		if len(again) != len(crls) {
			t.Fatalf("re-encoded CRL count %d, want %d", len(again), len(crls))
		}
		for i := range crls {
			if !bytes.Equal(again[i].Raw, crls[i].Raw) {
				t.Fatalf("CRL %d DER changed on round trip", i)
			}
		}
	})
}
