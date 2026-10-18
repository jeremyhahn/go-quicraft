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
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// curveListener is a QUIC listener whose server role offers its own curve
// list, built from buildMTLSTLS so everything but the groups is what the
// transport really runs: TLS 1.3 floor, mutual authentication, ALPN pinned.
type curveListener struct {
	ln     *quic.Listener
	client *tls.Config
	states chan tls.ConnectionState
}

// listenWithCurves starts a QUIC listener offering serverCurves and returns
// it with a client config, from the same mTLS material, offering
// clientCurves. Every accepted connection's server-side TLS state is sent on
// states once its handshake completes, then the connection is closed.
func listenWithCurves(tb testing.TB, clientCurves, serverCurves []tls.CurveID) *curveListener {
	tb.Helper()

	pair, err := buildMTLSTLS(testMTLSConfig(tb), nil)
	if err != nil {
		tb.Fatalf("buildMTLSTLS: %v", err)
	}
	serverCfg := pair.server.Clone()
	serverCfg.CurvePreferences = serverCurves
	clientCfg := pair.client.Clone()
	clientCfg.CurvePreferences = clientCurves
	// No session cache: every dial is a full handshake, which is the one
	// that carries a key share.
	clientCfg.ClientSessionCache = nil

	ln, err := quic.ListenAddr("127.0.0.1:0", serverCfg, &quic.Config{
		HandshakeIdleTimeout: handshakeDeadline,
	})
	if err != nil {
		tb.Fatalf("quic.ListenAddr: %v", err)
	}
	cl := &curveListener{ln: ln, client: clientCfg, states: make(chan tls.ConnectionState, 1)}
	go func() {
		for {
			conn, acceptErr := ln.Accept(context.Background())
			if acceptErr != nil {
				return
			}
			// The client may close first, which ends the connection before
			// the handshake completes here; that is not a state to report.
			select {
			case <-conn.HandshakeComplete():
			case <-conn.Context().Done():
				continue
			}
			select {
			case cl.states <- conn.ConnectionState().TLS:
			default:
			}
			_ = conn.CloseWithError(0, "")
		}
	}()
	tb.Cleanup(func() { _ = ln.Close() })
	return cl
}

// dial completes one QUIC handshake against the listener and returns the
// open connection; the caller closes it.
func (cl *curveListener) dial(ctx context.Context) (*quic.Conn, error) {
	return quic.DialAddr(ctx, cl.ln.Addr().String(), cl.client, &quic.Config{
		HandshakeIdleTimeout: handshakeDeadline,
	})
}

// TestHandshakeCurves_QUIC runs the cross-build matrix over the transport's
// real protocol and asserts the group each pairing lands on, from both ends.
//
// It is QUIC rather than the TCP harness on purpose: an ML-KEM-768 key share
// adds about 1.2 KB, so the ClientHello no longer fits in one Initial packet,
// and the handshake completing here is quic-go reassembling the CRYPTO frames
// across packets. wantHRR records which pairings cost a HelloRetryRequest —
// a standard client's first key share is for X25519MLKEM768, which a FIPS
// server does not offer, so that direction needs the extra round trip.
func TestHandshakeCurves_QUIC(t *testing.T) {
	for _, tc := range []struct {
		name           string
		client, server []tls.CurveID
		want           tls.CurveID
		wantHRR        bool
	}{
		{"this build dials itself", handshakeCurves(), handshakeCurves(), selfCurve, false},
		{"this build dials the other", handshakeCurves(), otherBuildCurves, crossBuildCurve,
			handshakeCurves()[0] != crossBuildCurve},
		{"the other build dials this", otherBuildCurves, handshakeCurves(), crossBuildCurve,
			otherBuildCurves[0] != crossBuildCurve},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := listenWithCurves(t, tc.client, tc.server)
			ctx, cancel := context.WithTimeout(context.Background(), handshakeDeadline)
			defer cancel()
			conn, err := cl.dial(ctx)
			if err != nil {
				t.Fatalf("handshake client=%v server=%v: %v", tc.client, tc.server, err)
			}
			// Held open until the server reports: the client finishes its
			// handshake first, and closing at once can end the connection
			// before the server has read the client's Finished.
			defer func() { _ = conn.CloseWithError(0, "") }()
			client := conn.ConnectionState().TLS
			var server tls.ConnectionState
			select {
			case server = <-cl.states:
			case <-ctx.Done():
				t.Fatal("server never completed the handshake")
			}
			for _, side := range []struct {
				role  string
				state tls.ConnectionState
			}{{"client", client}, {"server", server}} {
				if side.state.CurveID != tc.want {
					t.Errorf("%s negotiated %v, want %v", side.role, side.state.CurveID, tc.want)
				}
				if side.state.HelloRetryRequest != tc.wantHRR {
					t.Errorf("%s HelloRetryRequest=%v, want %v",
						side.role, side.state.HelloRetryRequest, tc.wantHRR)
				}
			}
		})
	}
}

// BenchmarkQUICHandshake measures one full mutually-authenticated QUIC
// handshake with this build's curve list on both sides: the cost a key
// exchange group adds to every connection the transport opens.
func BenchmarkQUICHandshake(b *testing.B) {
	cl := listenWithCurves(b, handshakeCurves(), handshakeCurves())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	b.ReportAllocs()
	for b.Loop() {
		conn, err := cl.dial(ctx)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		_ = conn.CloseWithError(0, "")
	}
}
