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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestStop_AnnouncesShutdownOnAcceptedConnections pins the half of Stop that
// was missing: the connections the transport ACCEPTED.
//
// stop() closed t.conns — the connections this node DIALED — and then
// t.quicTr.Close(), which destroys quic-go's remaining connections LOCALLY
// (Transport.close calls handler.destroy) without putting a CONNECTION_CLOSE on
// the wire. A peer that had dialed this node therefore learnt nothing at all: a
// poll on the sender's cached conn.Context() ran its full 10s bound and failed,
// so a leader went on enqueuing to a follower that had shut down cleanly until
// MaxIdleTimeout expired or its own next write happened to fail. Both of those
// can be much later than the shutdown, and cleanStaleConnections cannot evict a
// connection it never sees as stale.
//
// The assertion is deliberately in three parts, because "the context is done"
// alone is too weak: a LOCAL teardown of the sender satisfies it, and so does
// the idle timeout given enough patience. The test therefore also requires the
// cause to be a REMOTE quic.ApplicationError carrying QUICErrShutdown, which is
// only producible by a CONNECTION_CLOSE frame the peer deliberately sent, and it
// requires it PROMPTLY — the bound below exists to fail the test, not to be
// waited out. One frame over loopback takes milliseconds.
func TestStop_AnnouncesShutdownOnAcceptedConnections(t *testing.T) {
	tests := []struct {
		name string
		stop func(*QUICTransport)
	}{
		{
			name: "single Stop",
			stop: func(tr *QUICTransport) { _ = tr.Stop() },
		},
		{
			// Stop is documented as safe to call concurrently, with exactly one
			// caller running the teardown and the rest blocking on stopDone. The
			// announcement has to survive that: every caller must return only
			// after the inbound close has actually been issued, not as soon as
			// it sees the once already taken.
			name: "concurrent Stop",
			stop: func(tr *QUICTransport) {
				var wg sync.WaitGroup
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = tr.Stop()
					}()
				}
				wg.Wait()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t1, t2, _, handler2, cleanup := transportPair(t, 42)
			defer cleanup()

			// t1 dials t2, so the connection under test is INBOUND on t2 and is
			// reachable from t2.inboundConns and nowhere else. t2 never sends,
			// so t2.conns is empty and the outbound half of its stop() cannot
			// be what closes this connection.
			target := t2.Addr().String()
			t1.Send([]proto.Message{
				{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
			})
			handler2.waitBatch(t, 10*time.Second)

			t1.connMu.RLock()
			conn, ok := t1.conns[target]
			t1.connMu.RUnlock()
			if !ok {
				t.Fatalf("test setup: sender holds no connection to %s after a delivered batch", target)
			}
			select {
			case <-conn.Context().Done():
				t.Fatal("test setup: sender's connection was already dead before the peer was stopped")
			default:
			}

			tc.stop(t2)

			// Polled on the connection itself rather than on t1.conns: quic-go
			// cancels the context on close, but the map entry survives until a
			// later send or the cleanup sweep evicts it, and neither happens
			// here. See waitConnDown for the same reasoning.
			pollUntil(t, 5*time.Second, 2*time.Millisecond,
				"peer's graceful Stop was never announced: the sender's connection stayed live for the whole bound",
				func() bool { return conn.Context().Err() != nil })

			cause := context.Cause(conn.Context())
			var appErr *quic.ApplicationError
			if !errors.As(cause, &appErr) {
				t.Fatalf("sender's connection ended with %v; want a *quic.ApplicationError carried by the peer's CONNECTION_CLOSE", cause)
			}
			if !appErr.Remote {
				t.Fatalf("sender's connection was torn down LOCALLY (%v); the peer's shutdown was still not announced", appErr)
			}
			if appErr.ErrorCode != QUICErrShutdown {
				t.Fatalf("peer closed with application error code 0x%x, want QUICErrShutdown (0x%x)",
					appErr.ErrorCode, QUICErrShutdown)
			}
		})
	}
}

// TestStop_InboundCloseRunsBeforeTransportClose pins the ORDERING that makes
// the announcement reach the wire at all.
//
// t.quicTr.Close() destroys every remaining connection locally and tears down
// the shared UDP socket. quic-go's close is once-only, so a CloseWithError
// issued afterwards is a silent no-op: an inbound close sequenced BELOW the
// transport close would leave the defect exactly as it was while looking fixed
// in the diff. The test drives stop()'s two steps by hand in the WRONG order and
// requires the peer to observe nothing, which is what makes the assertion in
// TestStop_AnnouncesShutdownOnAcceptedConnections attributable to the ordering
// rather than to luck.
func TestStop_InboundCloseRunsBeforeTransportClose(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	t1.connMu.RLock()
	conn, ok := t1.conns[target]
	t1.connMu.RUnlock()
	if !ok {
		t.Fatalf("test setup: sender holds no connection to %s after a delivered batch", target)
	}

	// Require the connection to be tracked first, so a later empty
	// inboundConns cannot be mistaken for the property under test.
	inbound := 0
	t2.inboundConns.Range(func(_, _ any) bool { inbound++; return true })
	if inbound == 0 {
		t.Fatal("test setup: no inbound connection was tracked on the peer")
	}

	// Socket first, announcement second: stop()'s steps inverted. Everything
	// after this is the REAL stop(), whose closeInboundOnShutdown now runs on
	// connections quic-go has already destroyed.
	if err := t2.quicTr.Close(); err != nil {
		t.Fatalf("test setup: quicTr.Close: %v", err)
	}
	if err := t2.Stop(); err != nil {
		t.Fatalf("t2.Stop: %v", err)
	}

	// The peer's connection is destroyed locally by quicTr.Close, so t1 may or
	// may not see its own context die from the socket going away — what it must
	// NOT see is a remote QUICErrShutdown, because no frame could have left.
	assertNever(t, 200*time.Millisecond, 10*time.Millisecond,
		"a CONNECTION_CLOSE reached the peer after the QUIC transport was closed; the ordering constraint in stop() is not what makes the announcement work",
		func() bool {
			var appErr *quic.ApplicationError
			if !errors.As(context.Cause(conn.Context()), &appErr) {
				return false
			}
			return appErr.Remote && appErr.ErrorCode == QUICErrShutdown
		})
}

// TestStop_WaitsForTheAcceptLoopBeforeClosingTheTransport pins the barrier that
// makes the shutdown announcement complete instead of best-effort.
//
// Publishing an admitted connection into inboundConns is not by itself enough.
// Listener.Close does not discard the connections already queued inside quic-go
// — baseServer.accept drains connQueue after stopAccepting is closed — so the
// accept loop can still admit one after the shutdown has begun. If stop() swept
// the map and called quicTr.Close() while that was possible, the late-admitted
// connection would be destroyed LOCALLY and its peer left holding it: the exact
// defect this file exists to prevent, merely moved into a narrower window.
//
// stop() therefore joins the accept loop first. Because acceptLoopIteration
// publishes synchronously, on that goroutine, "the accept loop has returned"
// means "every connection it admitted is in the map", so the sweep after the
// join cannot miss one.
//
// The test drives that with the real Stop, no sleeps and no test hook: it holds
// acceptWG to stand for a loop that has not returned yet, requires Stop to make
// no progress past the join, publishes the late admission the way the loop
// would, releases the barrier, and requires the peer to have been told.
func TestStop_WaitsForTheAcceptLoopBeforeClosingTheTransport(t *testing.T) {
	t1, t2, _, handler2, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()
	t1.Send([]proto.Message{
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1},
	})
	handler2.waitBatch(t, 10*time.Second)

	t1.connMu.RLock()
	senderConn, ok := t1.conns[target]
	t1.connMu.RUnlock()
	if !ok {
		t.Fatalf("test setup: sender holds no connection to %s after a delivered batch", target)
	}

	var inbound *quic.Conn
	t2.inboundConns.Range(func(k, _ any) bool {
		inbound, _ = k.(*quic.Conn)
		return inbound == nil
	})
	if inbound == nil {
		t.Fatal("test setup: no inbound connection was tracked on the peer")
	}

	// Stand in for an accept loop that has not returned yet, and un-publish the
	// connection so it is in the state a just-accepted one is: live, admitted,
	// not yet visible to the sweep. Add runs while the real loop still holds a
	// count of its own, so the counter never goes from zero here.
	t2.acceptWG.Add(1)
	t2.inboundConns.Delete(inbound)

	done := make(chan struct{})
	go func() {
		_ = t2.Stop()
		close(done)
	}()

	// Nothing past the join may run while admissions are still possible — in
	// particular not quicTr.Close(), which would make the announcement below
	// impossible to deliver.
	assertNever(t, 200*time.Millisecond, 10*time.Millisecond,
		"Stop ran past its accept-loop join while a connection could still be admitted; a late admission can still be destroyed unannounced",
		func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		})

	// The late admission, published the way acceptLoopIteration publishes it.
	t2.publishInboundConn(inbound)
	t2.acceptWG.Done()

	waitForChan(t, done, 10*time.Second, "Stop never returned after the accept loop was released")

	// Stop returning means the frame was SENT; the sender still has to receive
	// it. Poll rather than read the cause straight away — the bound is there to
	// fail the test, not to be waited out.
	pollUntil(t, 5*time.Second, 2*time.Millisecond,
		"the late-admitted connection was never announced: the sender's connection stayed live for the whole bound",
		func() bool { return senderConn.Context().Err() != nil })

	cause := context.Cause(senderConn.Context())
	var appErr *quic.ApplicationError
	if !errors.As(cause, &appErr) {
		t.Fatalf("sender's connection ended with %v; want a *quic.ApplicationError carried by the peer's CONNECTION_CLOSE", cause)
	}
	if !appErr.Remote {
		t.Fatalf("sender's connection was torn down LOCALLY (%v); the late-admitted connection was never announced", appErr)
	}
	if appErr.ErrorCode != QUICErrShutdown {
		t.Fatalf("peer closed with application error code 0x%x, want QUICErrShutdown (0x%x)",
			appErr.ErrorCode, QUICErrShutdown)
	}
}
