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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

// identityCapturingHandler records the PeerAuth.Identity the transport hands
// the handler for each inbound batch.
type identityCapturingHandler struct {
	testHandler
	mu         sync.Mutex
	identities []string
	got        chan struct{}
}

func newIdentityCapturingHandler() *identityCapturingHandler {
	h := &identityCapturingHandler{got: make(chan struct{}, 16)}
	h.batchNotify = make(chan struct{}, 100)
	h.snapshotNotify = make(chan struct{}, 100)
	return h
}

func (h *identityCapturingHandler) HandleMessage(batch proto.MessageBatch, peer PeerAuth) error {
	h.mu.Lock()
	h.identities = append(h.identities, peer.Identity)
	h.mu.Unlock()
	select {
	case h.got <- struct{}{}:
	default:
	}
	return h.testHandler.HandleMessage(batch, peer)
}

func (h *identityCapturingHandler) awaitIdentity(t *testing.T, d time.Duration) string {
	t.Helper()
	select {
	case <-h.got:
	case <-time.After(d):
		t.Fatal("no batch reached the handler")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.identities[len(h.identities)-1]
}

// TestInboundPeerAuthCarriesConnectionIdentity pins the binding the entire
// sender gate rests on.
//
// PeerAuth.Identity is what IsRemovedIdentity and the committed-sender check
// consult; it is the fix for the msg.From quorum-forgery defect. Yet no test
// asserted it was wired: all 41 PeerAuth references in this package's tests
// are either `_ PeerAuth` in a handler signature or a literal `PeerAuth{}`
// argument, so changing the construction site to `Identity: ""` left the suite
// green. On a shard with committed identities that regression fails loudly, but
// on an address-only shard it fails SILENTLY OPEN — a removed replica's
// tombstone simply stops being enforced.
//
// The batch below carries an in-band From naming a DIFFERENT replica, so the
// test also pins that the identity is taken from the connection rather than
// from anything in the frame.
func TestInboundPeerAuthCarriesConnectionIdentity(t *testing.T) {
	h := newIdentityCapturingHandler()
	recv, send, cleanup := transportPairWith(t, 42, h, newTestHandler())
	defer cleanup()

	want := send.LocalNodeIdentity()
	if want == "" {
		t.Fatal("the sending transport has no local identity; the assertion " +
			"below would pass against an empty string either way")
	}

	send.Send([]proto.Message{{
		Type: proto.Heartbeat, ShardID: 1, From: 99, To: 1,
	}})

	got := h.awaitIdentity(t, 5*time.Second)
	if got != want {
		t.Fatalf("PeerAuth.Identity = %q, want the sender's certificate "+
			"identity %q. The handler's authorization decisions are made on "+
			"this value; an empty or frame-derived one silently disables the "+
			"removed-replica deny list", got, want)
	}
	if got == "99" || got == "" {
		t.Fatalf("PeerAuth.Identity = %q: it must come from the verified "+
			"peer certificate of THIS connection, never from the batch", got)
	}
	_ = recv
}

// TestListenerRefusesUnauthenticatedClients pins mandatory mTLS end to end.
//
// The existing coverage asserts on the struct FIELDS of the *tls.Config that
// buildMTLSTLS returns — that ClientAuth is RequireAndVerifyClientCert, that
// RootCAs is non-nil. That proves the config was built; it says nothing about
// the config Start actually hands to Listen. Setting
// serverTLS.ClientAuth = tls.NoClientCert at that call left every test in the
// package passing, and no test anywhere dialled a live listener with a client
// that genuinely lacks the property.
//
// NOTE on the QUIC subtlety this test is built around: under TLS 1.3 the
// CLIENT finishes its handshake before the server has verified the client
// certificate, so a successful dial does NOT mean admission. The verdict is
// read from the server side — whether a batch reached the handler.
func TestListenerRefusesUnauthenticatedClients(t *testing.T) {
	mtls := testMTLSConfig(t)
	h := newIdentityCapturingHandler()
	recv, sender, cleanup := transportPairWith(t, 42, h, newTestHandler())
	defer cleanup()

	good, err := buildMTLSTLS(mtls, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS: %v", err)
	}
	foreign, err := buildMTLSTLS(testMTLSConfig(t), nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS(foreign CA): %v", err)
	}

	noCert := good.client.Clone()
	noCert.Certificates = nil

	foreignLeaf := foreign.client.Clone()
	foreignLeaf.RootCAs = good.client.RootCAs

	wrongALPN := good.client.Clone()
	wrongALPN.NextProtos = []string{"h3"}

	tls12 := good.client.Clone()
	tls12.MaxVersion = tls.VersionTLS12

	// CONTROL FIRST: a correctly-configured peer IS admitted, so the negative
	// cases below are verdicts rather than a blanket inability to connect.
	before := h.countIdentities()
	sender.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1}})
	if _, ok := waitForCount(h, before+1, 5*time.Second); !ok {
		t.Fatal("the control peer was never admitted: every negative case " +
			"below would then pass for the wrong reason")
	}

	target := recv.Addr().String()
	for name, cfg := range map[string]*tls.Config{
		"no-client-cert":  noCert,
		"foreign-ca-leaf": foreignLeaf,
		"wrong-alpn":      wrongALPN,
		"tls12-only":      tls12,
	} {
		t.Run(name, func(t *testing.T) {
			start := h.countIdentities()
			dialAndWriteBestEffort(t, cfg, target)
			// Allow a wrongly-admitted batch time to arrive.
			time.Sleep(300 * time.Millisecond)
			if got := h.countIdentities(); got != start {
				t.Fatalf("a %s client delivered a batch to the handler: "+
					"mandatory mTLS is enforced only in the struct the config "+
					"builder returns, not on the live listener", name)
			}
		})
	}
}

// dialAndWriteBestEffort attempts a QUIC dial with cfg and, if it connects,
// writes one well-formed message frame. Every failure is expected for the
// negative cases and is therefore not an assertion — the assertion is made on
// the SERVER side by the caller.
func dialAndWriteBestEffort(t *testing.T, cfg *tls.Config, target string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, target, cfg, nil)
	if err != nil {
		return // refused at handshake: the desired outcome
	}
	defer func() { _ = conn.CloseWithError(0, "test") }()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	defer func() { _ = stream.Close() }()

	batch := proto.MessageBatch{
		DeploymentID: 42,
		BinVer:       proto.WireVersion,
		Requests:     []proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1}},
	}
	payload := make([]byte, batch.Size())
	n, err := batch.MarshalTo(payload)
	if err != nil {
		return
	}
	frame := make([]byte, FrameHeaderSize+n)
	written, err := MarshalFrame(frame, payload[:n], FlagNone)
	if err != nil {
		return
	}
	_ = stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, _ = stream.Write(frame[:written])
}

func (h *identityCapturingHandler) countIdentities() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.identities)
}

func waitForCount(h *identityCapturingHandler, want int, d time.Duration) (int, bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got := h.countIdentities(); got >= want {
			return got, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h.countIdentities(), false
}

// peerVisibleCloseError returns the CONNECTION_CLOSE the REMOTE end sent for a
// connection, which is the only place the reason phrase a peer actually
// receives can be read. quic-go cancels the connection context with the close
// error as its cause.
func peerVisibleCloseError(t *testing.T, conn *quic.Conn, timeout time.Duration) *quic.ApplicationError {
	t.Helper()
	select {
	case <-conn.Context().Done():
	case <-time.After(timeout):
		t.Fatal("the connection was never closed, so no close error is observable")
	}
	var appErr *quic.ApplicationError
	if !errors.As(context.Cause(conn.Context()), &appErr) {
		t.Fatalf("close cause = %v, want a *quic.ApplicationError",
			context.Cause(conn.Context()))
	}
	if !appErr.Remote {
		t.Fatalf("close error is LOCAL (%+v): this must be read from the side "+
			"that RECEIVED the close, or the reason phrase under test is the "+
			"one this process wrote to itself", appErr)
	}
	return appErr
}

// TestAuthorizationCloseReasonIsNotDisclosedToPeer pins that an authorization
// verdict is not written out to the peer in the CONNECTION_CLOSE reason phrase.
//
// The phrase travels verbatim — quic-go's PackApplicationClose copies
// ErrorMessage straight onto the wire, and only TLS-alert (crypto) closes have
// their reason stripped — so "peer certificate revoked" was the readable answer
// to a question the peer should have to work for. The precise verdict is still
// logged locally at each close site; only the wire copy is removed.
//
// The numeric application error code is deliberately still distinct per verdict
// and is asserted, so this test also pins that the redaction did not go so far
// as to make a policy close indistinguishable from a resource limit.
func TestAuthorizationCloseReasonIsNotDisclosedToPeer(t *testing.T) {
	t.Run("revoked", func(t *testing.T) {
		t1, t2, cleanup := transportPairWithRevocation(t)
		defer cleanup()

		// t1 dials t2 and is the side that will publish the revocation, so the
		// peer-visible close is read from t2's INBOUND connection.
		outbound, err := t1.getConnection(t2.Addr().String(), "")
		if err != nil {
			t.Fatalf("getConnection: %v", err)
		}
		inbound, _ := awaitInboundConn(t, t2)

		certs := outbound.ConnectionState().TLS.PeerCertificates
		if len(certs) == 0 {
			t.Fatal("test setup: no peer certificate on an mTLS connection")
		}
		t1.revocationChecker.AddRevokedSerial(certs[0].SerialNumber.Text(16))
		t1.revokeCheckConnections()

		appErr := peerVisibleCloseError(t, inbound, 5*time.Second)
		if appErr.ErrorMessage != "" {
			t.Fatalf("the peer received the reason phrase %q for a revocation "+
				"close: the reason phrase is transmitted verbatim and tells an "+
				"attacker which of its certificates the cluster still accepts",
				appErr.ErrorMessage)
		}
		if appErr.ErrorCode != QUICErrRevoked {
			t.Fatalf("close code = %d, want QUICErrRevoked (%d)",
				appErr.ErrorCode, QUICErrRevoked)
		}
	})

	t.Run("removed-from-membership", func(t *testing.T) {
		receiver, sender, cleanup := inboundCertMatchPair(t)
		defer cleanup()

		sender.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 2, To: 1}})
		awaitInboundConn(t, receiver)

		// The peer-visible side is the SENDER's outbound connection.
		outbound, err := sender.getConnection(receiver.Addr().String(), "")
		if err != nil {
			t.Fatalf("getConnection: %v", err)
		}

		if n := receiver.closeInboundFromPeer("localhost:9999"); n == 0 {
			t.Fatal("no inbound connection was closed, so no close error is " +
				"observable and the assertion below would be vacuous")
		}

		appErr := peerVisibleCloseError(t, outbound, 5*time.Second)
		if appErr.ErrorMessage != "" {
			t.Fatalf("the peer received the reason phrase %q for a membership "+
				"removal close, disclosing the verdict verbatim", appErr.ErrorMessage)
		}
		if appErr.ErrorCode != QUICErrPeerRemoved {
			t.Fatalf("close code = %d, want QUICErrPeerRemoved (%d)",
				appErr.ErrorCode, QUICErrPeerRemoved)
		}
	})
}
