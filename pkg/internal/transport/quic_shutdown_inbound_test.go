package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
)

// TestStop_AnnouncesConnectionStillInAcceptQueue checks that Stop announces
// its shutdown on an inbound connection whose handshake completed but which the
// accept loop never took off the listener's queue.
//
// The accept loop exits as soon as it sees t.stopped, and its Accept runs on
// t.shutdownCtx, which stop() cancels first, so a connection still queued in
// the listener when Stop runs is never published in inboundConns. The inbound
// sweep cannot reach it, quicTr.Close destroys it locally without a
// CONNECTION_CLOSE, and the dialer holds it until MaxIdleTimeout (30s).
//
// The test parks the accept loop on a first connection (inside the admission
// refusal hook), queues a second one behind it, starts Stop, and only lets the
// loop go once stopped is set: the loop then admits the first connection and
// exits with the second still queued. The queued connection must be closed by
// the peer with QUICErrShutdown: a CONNECTION_REFUSED from a handshake still
// in flight at Close is a transport error and fails the check.
func TestStop_AnnouncesConnectionStillInAcceptQueue(t *testing.T) {
	queued := stopWithQueuedConnections(t, 1, nil)
	wantRemoteClose(t, queued[0], QUICErrShutdown, "transport shutdown")
}

// TestStop_AnnouncesEveryQueuedConnectionWhenTheDrainOutlastsASecond checks
// that the drain of the accept queue is not bounded by a deadline. Each
// CloseWithError in the drain is synchronous; when the drain runs past a
// deadline, quic-go's Accept may pick the expired context over a nonempty
// queue, and stop() would then destroy the rest of the queue unannounced. The
// seam slows the first close past one second, and every queued connection
// must still be closed by the peer with QUICErrShutdown.
func TestStop_AnnouncesEveryQueuedConnectionWhenTheDrainOutlastsASecond(t *testing.T) {
	var once sync.Once
	slowFirstClose := func() {
		once.Do(func() { time.Sleep(1500 * time.Millisecond) })
	}
	queued := stopWithQueuedConnections(t, 8, slowFirstClose)
	for _, c := range queued {
		wantRemoteClose(t, c, QUICErrShutdown, "transport shutdown")
	}
}

// stopWithQueuedConnections parks a receiver's accept loop on a first
// connection, queues n more connections behind it, stops the receiver with
// beforeQueuedClose installed as its queued-close seam, and returns the
// queued connections' dialer ends once Stop has returned.
//
// Each queued connection is dialed with a qlog tracer and the dial waits for
// the dialer's handshake_confirmed event. The server sends HANDSHAKE_DONE
// only once its own handshake completed, which is what moves a connection into
// the accept queue, so the connection is queued, not handshaking, when Stop
// closes the listener.
func stopWithQueuedConnections(t *testing.T, n int, beforeQueuedClose func()) []*quic.Conn {
	t.Helper()
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
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	receiver.inboundRefusalForTest = func(*quic.Conn) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return nil
	}
	receiver.queuedCloseForTest = beforeQueuedClose
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start: %v", err)
	}
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = receiver.Stop()
	}()
	addr := receiver.Addr().String()

	sender := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
	held, err := sender.getConnection(addr, "")
	if err != nil {
		t.Fatalf("getConnection: %v", err)
	}
	waitForChan(t, entered, 10*time.Second, "the accept loop never took the first connection")

	queued := make([]*quic.Conn, 0, n)
	for i := 0; i < n; i++ {
		c := dialConfirmed(t, addr, sender)
		t.Cleanup(func() { _ = c.CloseWithError(0, "") })
		queued = append(queued, c)
	}

	stopped := make(chan struct{})
	go func() {
		_ = receiver.Stop()
		close(stopped)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !receiver.stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("Stop never set stopped")
		}
		time.Sleep(time.Millisecond)
	}
	released = true
	close(release)

	waitForChan(t, stopped, 10*time.Second, "Stop did not return")
	waitForChan(t, held.Context().Done(), 5*time.Second, "the admitted connection was not announced")
	return queued
}

// dialConfirmed dials addr with sender's client TLS configuration and
// returns once the dialer has seen the server confirm the handshake.
func dialConfirmed(t *testing.T, addr string, sender *QUICTransport) *quic.Conn {
	t.Helper()
	trace := &handshakeConfirmedTrace{confirmed: make(chan struct{})}
	cc := sender.currentTLS().client.Clone()
	cc.ServerName = "127.0.0.1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, addr, cc, &quic.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
			return trace
		},
	})
	if err != nil {
		t.Fatalf("quic.DialAddr: %v", err)
	}
	select {
	case <-trace.confirmed:
	case <-conn.Context().Done():
		t.Fatalf("the queued connection closed before the server confirmed its handshake: %v", context.Cause(conn.Context()))
	case <-time.After(5 * time.Second):
		t.Fatal("the server never confirmed the queued connection's handshake")
	}
	return conn
}

// handshakeConfirmedTrace is a qlog trace that closes confirmed when its
// connection records the handshake_confirmed state.
type handshakeConfirmedTrace struct {
	once      sync.Once
	confirmed chan struct{}
}

func (h *handshakeConfirmedTrace) AddProducer() qlogwriter.Recorder { return h }

func (h *handshakeConfirmedTrace) RecordEvent(e qlogwriter.Event) {
	if ev, ok := e.(qlog.ConnectionStateUpdated); ok && ev.State == qlog.ConnectionStateHandshakeConfirmed {
		h.once.Do(func() { close(h.confirmed) })
	}
}

func (h *handshakeConfirmedTrace) Close() error { return nil }
