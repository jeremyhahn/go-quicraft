package transport

import (
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

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
// exits with the second still queued.
func TestStop_AnnouncesConnectionStillInAcceptQueue(t *testing.T) {
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

	dial := func() *quic.Conn {
		t.Helper()
		s := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
		c, err := s.getConnection(addr, "")
		if err != nil {
			t.Fatalf("getConnection: %v", err)
		}
		return c
	}

	held := dial()
	waitForChan(t, entered, 10*time.Second, "the accept loop never took the first connection")
	queued := dial()
	// The dialer's handshake completes one flight before the listener's; give
	// the listener that flight so the second connection is in its accept queue
	// rather than still handshaking (a handshake in flight at Close is refused
	// by quic-go itself, which would hide the gap under test).
	time.Sleep(200 * time.Millisecond)

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
	waitForChan(t, queued.Context().Done(), 5*time.Second,
		"the connection left in the accept queue was not announced: the dialer holds it until MaxIdleTimeout")
}
