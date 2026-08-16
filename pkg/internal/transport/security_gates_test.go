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
