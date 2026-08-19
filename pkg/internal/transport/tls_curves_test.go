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
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"
)

// handshakeDeadline bounds each side so a handshake that cannot complete fails
// the test in seconds instead of burning the package's -timeout. It is a
// backstop for a regression, not a synchronisation mechanism: every passing
// path here joins on a channel, and nothing waits for the clock.
const handshakeDeadline = 30 * time.Second

// handshakeWithCurves completes one TLS 1.3 mutually-authenticated handshake
// between a client offering clientCurves and a server offering serverCurves,
// and returns the first error either side reported.
//
// It runs over a loopback TCP listener rather than net.Pipe. A pipe is
// synchronous and unbuffered, and when the two curve lists differ the server
// answers with a HelloRetryRequest — an extra flight, alongside the TLS 1.3
// middlebox-compatibility ChangeCipherSpec records, that stops the exchange
// being strictly alternating. On a pipe both ends then block in Write and the
// test hangs; the kernel's socket buffers make the same exchange complete. The
// asymmetric case is the one these tests exist for, so the harness has to
// survive it.
func handshakeWithCurves(t *testing.T, clientCurves, serverCurves []tls.CurveID) error {
	t.Helper()

	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("AppendCertsFromPEM: CA bundle not accepted")
	}

	// Both configs mirror buildMTLSTLS: TLS 1.3 floor, mutual authentication
	// against the same pool, ALPN pinned. Only the curve lists differ between
	// the two roles, which is the thing under test.
	serverCfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		ClientCAs:        pool,
		ClientAuth:       tls.RequireAndVerifyClientCert,
		NextProtos:       []string{tlsALPN},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: serverCurves,
	}
	clientCfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		RootCAs:          pool,
		ServerName:       "localhost",
		NextProtos:       []string{tlsALPN},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: clientCurves,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	serverErr := make(chan error, 1)
	go func() {
		raw, acceptErr := ln.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(handshakeDeadline))
		conn := tls.Server(raw, serverCfg)
		serverErr <- conn.Handshake()
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(handshakeDeadline))
	clientHandshakeErr := tls.Client(raw, clientCfg).Handshake()

	// The server error is read first: when a curve list is refused, the client
	// sees the alert the server sent and the server sees the cause.
	if err := <-serverErr; err != nil {
		return err
	}
	return clientHandshakeErr
}

// TestHandshakeCurves_InteroperateAcrossBuilds is the rolling-upgrade claim
// that justifies both curve lists holding more than one group.
//
// A `-tags fips` build cannot offer X25519 and a standard build wants to, so
// the two lists are not equal — and TLS 1.3 needs a NON-EMPTY INTERSECTION,
// not equality. If either list were trimmed to a single group the intersection
// would be empty and every cross-build handshake would fail with
// `tls: no supported elliptic curves for ECDHE`, which would mean a cluster
// could only be converted to FIPS by stopping all of it at once.
//
// otherBuildCurves is the list the OTHER build offers, written literally
// because one binary can only compile one of the two. It cannot drift
// unnoticed: it is the same literal assertHandshakeCurves pins in that build's
// file, so changing one list fails this repository's tests under the other tag.
func TestHandshakeCurves_InteroperateAcrossBuilds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		client, server []tls.CurveID
	}{
		{"this build dials the other", handshakeCurves(), otherBuildCurves},
		{"the other build dials this", otherBuildCurves, handshakeCurves()},
		{"this build dials itself", handshakeCurves(), handshakeCurves()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := handshakeWithCurves(t, tc.client, tc.server); err != nil {
				t.Fatalf("handshake client=%v server=%v: %v", tc.client, tc.server, err)
			}
		})
	}
}

// TestHandshakeCurves_DisjointListsFail proves the test above is not vacuous:
// the handshake really does fail when the two lists share nothing, so its
// passing means an intersection exists rather than that the harness accepts
// anything.
func TestHandshakeCurves_DisjointListsFail(t *testing.T) {
	err := handshakeWithCurves(t, []tls.CurveID{tls.CurveP521}, []tls.CurveID{tls.CurveP256})
	if err == nil {
		t.Fatal("expected a handshake between disjoint curve lists to fail, got success")
	}
}
