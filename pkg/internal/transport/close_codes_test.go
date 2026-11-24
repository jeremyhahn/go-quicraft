package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestInboundCloseCodes pins what the DIALING side of a refused or reclaimed
// inbound connection receives: the application error code is the protocol
// contract that lets a peer tell a policy close from a resource limit (see
// errors.go), and an authorization verdict travels with the redacted reason
// phrase closeReasonRedacted. Swapping two codes, or leaking the verdict into
// the reason, must fail here.
func TestInboundCloseCodes(t *testing.T) {
	t.Run("per-IP limit", func(t *testing.T) {
		mtls := testMTLSConfig(t)
		receiver := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler(),
			func(c *Config) { c.MaxConnectionsPerIP = 1 })
		addr := receiver.Addr().String()

		// The first sender takes the loopback address's only slot.
		first := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
		if _, err := first.getConnection(addr, ""); err != nil {
			t.Fatalf("first getConnection: %v", err)
		}
		pollUntil(t, 10*time.Second, time.Millisecond, "the first connection was never admitted",
			func() bool { return receiver.ipCountForTest("127.0.0.1") == 1 })

		second := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
		refused, err := second.getConnection(addr, "")
		if err != nil {
			t.Fatalf("second getConnection: %v", err)
		}
		wantRemoteClose(t, refused, QUICErrIPLimitReached, "per-IP connection limit reached")
	})

	t.Run("promiscuous certificate", func(t *testing.T) {
		mtls := testMTLSConfig(t)
		receiver, err := NewQUICTransport(Config{
			ListenAddress: "127.0.0.1:0",
			DeploymentID:  42,
			MTLSConfig:    mtls,
			DialTimeout:   testDialTimeout,
		}, newTestHandler(), registry.NewRegistry())
		if err != nil {
			t.Fatalf("NewQUICTransport(receiver): %v", err)
		}
		receiver.inboundRefusalForTest = func(*quic.Conn) error {
			return errors.New("peer certificate covers this node")
		}
		if err := receiver.Start(); err != nil {
			t.Fatalf("receiver.Start: %v", err)
		}
		defer func() { _ = receiver.Stop() }()

		sender := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
		refused, err := sender.getConnection(receiver.Addr().String(), "")
		if err != nil {
			t.Fatalf("getConnection: %v", err)
		}
		wantRemoteClose(t, refused, QUICErrPromiscuousCert, closeReasonRedacted)
	})

	t.Run("idle reclaim", func(t *testing.T) {
		t1, t2, _, handler2, cleanup := transportPair(t, 42)
		defer cleanup()

		t1.Send([]proto.Message{{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2}})
		handler2.waitBatch(t, 10*time.Second)
		awaitInboundConn(t, t2)

		dialed := t1.pooledConnForTest(t2.Addr().String())
		if dialed == nil {
			t.Fatalf("t1 holds no cached connection to %s after a delivered batch", t2.Addr())
		}
		t2.reapIdleInboundConns(time.Now().Add(inboundIdleRetention + time.Second))
		wantRemoteClose(t, dialed, QUICErrInboundIdle, closeReasonRedacted)
	})
}

// wantRemoteClose waits (bounded) for conn to close and asserts that the peer
// closed it with the given application error code and reason phrase.
func wantRemoteClose(t *testing.T, conn *quic.Conn, code quic.ApplicationErrorCode, reason string) {
	t.Helper()
	select {
	case <-conn.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("the connection was not closed within 10s; want code %d from the peer", code)
	}
	cause := context.Cause(conn.Context())
	var appErr *quic.ApplicationError
	if !errors.As(cause, &appErr) {
		t.Fatalf("connection closed with %v, want a QUIC application error with code %d", cause, code)
	}
	if !appErr.Remote {
		t.Fatalf("connection closed locally with %v, want the peer's close with code %d", appErr, code)
	}
	if appErr.ErrorCode != code {
		t.Fatalf("peer closed with code %d (%q), want %d", appErr.ErrorCode, appErr.ErrorMessage, code)
	}
	if appErr.ErrorMessage != reason {
		t.Fatalf("peer closed with code %d and reason %q, want reason %q", appErr.ErrorCode, appErr.ErrorMessage, reason)
	}
}
