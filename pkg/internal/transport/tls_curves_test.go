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

// handshakeWithCurves runs one mutual TLS 1.3 handshake with the given curve lists over loopback TCP (net.Pipe deadlocks on HelloRetryRequest) and returns the first error.
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

	// Both configs mirror buildMTLSTLS; only the curve lists differ.
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

	// The context bounds the dial and both handshakes; the SetDeadline calls
	// below bound the raw sockets underneath them. Same backstop as before,
	// expressed through the context-aware APIs the noctx linter requires.
	ctx, cancel := context.WithTimeout(context.Background(), handshakeDeadline)
	defer cancel()

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
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
		serverErr <- conn.HandshakeContext(ctx)
	}()

	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(handshakeDeadline))
	clientHandshakeErr := tls.Client(raw, clientCfg).HandshakeContext(ctx)

	// The server error is read first: when a curve list is refused, the client
	// sees the alert the server sent and the server sees the cause.
	if err := <-serverErr; err != nil {
		return err
	}
	return clientHandshakeErr
}

// TestHandshakeCurves_InteroperateAcrossBuilds: the fips and standard lists intersect, so a cluster can convert to FIPS node by node; otherBuildCurves mirrors the other build's pinned list.
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
