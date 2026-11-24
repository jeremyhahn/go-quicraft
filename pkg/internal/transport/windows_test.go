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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Critical windows: user code between a state advance and its settlement
//
// Each test drives one window with user code — a slog handler, an
// EventListener callback — that panics or calls runtime.Goexit, and checks
// that what the window had advanced was settled all the same.
// ---------------------------------------------------------------------------

// newStartedTransport builds and starts a loopback transport, stopped when the
// test ends unless the test stops it itself.
func newStartedTransport(t *testing.T, mtls *MTLSConfig, reg *registry.Registry, handler MessageHandler, opts ...func(*Config)) *QUICTransport {
	t.Helper()
	cfg := Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		DialTimeout:   testDialTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	tr, err := NewQUICTransport(cfg, handler, reg)
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = tr.Stop() })
	return tr
}

// ipCount returns the accept loop's per-IP admission count for ip.
func (t *QUICTransport) ipCountForTest(ip string) int {
	t.acceptIPs.mu.Lock()
	defer t.acceptIPs.mu.Unlock()
	return t.acceptIPs.counts[ip]
}

// pooledConn returns the pooled outbound connection to target, or nil.
func (t *QUICTransport) pooledConnForTest(target string) *quic.Conn {
	t.connMu.RLock()
	defer t.connMu.RUnlock()
	return t.conns[target]
}

// pooledStreamsForTest counts the pooled streams to target.
func (t *QUICTransport) pooledStreamsForTest(target string) int {
	n := 0
	for i := range t.streamShards {
		shard := &t.streamShards[i]
		shard.mu.Lock()
		for k := range shard.streams {
			if k.target == target {
				n++
			}
		}
		shard.mu.Unlock()
	}
	return n
}

// dialForTest dials and pools a connection to target without identity
// pinning, and delivers its connection event, as getConnection's dial does.
func (t *QUICTransport) dialForTest(target string) (*quic.Conn, error) {
	conn, report, err := t.dialAndPool(target, "")
	report()
	return conn, err
}

// TestAcceptRejection_SettlesBeforeLogging: a connection refused at admission —
// over its address's per-IP limit, or for a promiscuous certificate — is
// closed and leaves the per-IP tracker and the global semaphore exactly as it
// found them, whatever the rejection's log handler does. The per-IP warning
// used to run before the close, leaking the accepted connection outside
// MaxIncomingConnections and inboundConns; the promiscuous-certificate warning
// ran after the per-IP slot was taken and before it was released, leaking the
// address's quota for good.
func TestAcceptRejection_SettlesBeforeLogging(t *testing.T) {
	type branch struct {
		record string
		setup  func(t *testing.T, receiver *QUICTransport, newSender func() *QUICTransport, dial func(*QUICTransport) *quic.Conn)
		// wantIP and wantSem are the admission counts once the refusal has
		// settled.
		wantIP, wantSem int
		opts            []func(*Config)
	}
	branches := map[string]branch{
		"per-IP limit": {
			record: "per-IP connection limit reached",
			setup: func(t *testing.T, receiver *QUICTransport, newSender func() *QUICTransport, dial func(*QUICTransport) *quic.Conn) {
				// The first sender takes the address's only slot.
				dial(newSender())
				pollUntil(t, 10*time.Second, time.Millisecond, "the first connection was never admitted",
					func() bool { return receiver.ipCountForTest("127.0.0.1") == 1 })
			},
			wantIP: 1, wantSem: 1,
			opts: []func(*Config){func(c *Config) { c.MaxConnectionsPerIP = 1 }},
		},
		"promiscuous certificate": {
			record: "refusing inbound connection",
			setup:  func(*testing.T, *QUICTransport, func() *QUICTransport, func(*QUICTransport) *quic.Conn) {},
			wantIP: 0, wantSem: 0,
		},
	}
	for branchName, b := range branches {
		for exitName, exit := range hostileLogExits {
			t.Run(branchName+"/"+exitName, func(t *testing.T) {
				mtls := testMTLSConfig(t)
				receiver, err := NewQUICTransport(func() Config {
					c := Config{ListenAddress: "127.0.0.1:0", DeploymentID: 42, MTLSConfig: mtls, DialTimeout: testDialTimeout}
					for _, opt := range b.opts {
						opt(&c)
					}
					return c
				}(), newTestHandler(), registry.NewRegistry())
				if err != nil {
					t.Fatalf("NewQUICTransport(receiver): %v", err)
				}
				if branchName == "promiscuous certificate" {
					receiver.inboundRefusalForTest = func(*quic.Conn) error { return errors.New("peer certificate covers this node") }
				}
				if err := receiver.Start(); err != nil {
					t.Fatalf("receiver.Start: %v", err)
				}
				defer receiver.Stop()
				addr := receiver.Addr().String()

				newSender := func() *QUICTransport {
					return newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
				}
				dial := func(s *QUICTransport) *quic.Conn {
					c, err := s.getConnection(addr, "")
					if err != nil {
						t.Fatalf("getConnection: %v", err)
					}
					return c
				}
				b.setup(t, receiver, newSender, dial)

				refused := installHostileRecords(t, b.record, exit)
				rejected := dial(newSender())
				awaitCount(t, refused, 1, "the connection was never refused")

				select {
				case <-rejected.Context().Done():
				case <-time.After(5 * time.Second):
					t.Fatal("the refused connection was never closed")
				}
				pollUntil(t, 5*time.Second, time.Millisecond, "the refusal leaked admission accounting",
					func() bool {
						return receiver.ipCountForTest("127.0.0.1") == b.wantIP && len(receiver.acceptSem) == b.wantSem
					})
			})
		}
	}
}

// TestCleanStaleConnections_SettlesEveryDeadTarget: the cleanup sweep evicts
// every dead target's streams, and delivers OnConnectionFailed for every one,
// when the first target's callback panics or calls runtime.Goexit. The sweep
// used to evict and notify target by target, so an abnormal exit lost the
// remaining targets' stream eviction and callbacks; their connections were
// already out of the pool, so no later sweep would find them again.
func TestCleanStaleConnections_SettlesEveryDeadTarget(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			var (
				armed    atomic.Bool
				mu       sync.Mutex
				notified []string
			)
			reg := registry.NewRegistry()
			sender := newStartedTransport(t, mtls, reg, newTestHandler(), func(c *Config) {
				c.EventListener = &config.EventListener{
					OnConnectionFailed: func(info config.ConnectionInfo) {
						if !armed.Load() {
							return
						}
						mu.Lock()
						notified = append(notified, info.Address)
						first := len(notified) == 1
						mu.Unlock()
						if first {
							exit()
						}
					},
				}
			})

			const peers = 3
			addrs := make([]string, 0, peers)
			for i := range peers {
				received := newTestHandler()
				peer := newStartedTransport(t, mtls, registry.NewRegistry(), received)
				addr := peer.Addr().String()
				replica := uint64(2 + i)
				reg.Register(1, replica, addr)
				sender.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: replica, Term: 1}})
				received.waitBatch(t, 10*time.Second)
				if err := peer.Stop(); err != nil {
					t.Fatalf("peer.Stop: %v", err)
				}
				addrs = append(addrs, addr)
			}
			for _, addr := range addrs {
				pollUntil(t, 10*time.Second, time.Millisecond, "a stopped peer's connection never died", func() bool {
					c := sender.pooledConnForTest(addr)
					return c != nil && c.Context().Err() != nil
				})
				if sender.pooledStreamsForTest(addr) == 0 {
					t.Fatalf("fixture: no pooled stream to %s", addr)
				}
			}

			armed.Store(true)
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				sender.cleanStaleConnections()
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the cleanup sweep never returned")
			}

			for _, addr := range addrs {
				if n := sender.pooledStreamsForTest(addr); n != 0 {
					t.Errorf("%d pooled streams to dead target %s survived the sweep", n, addr)
				}
				if c := sender.pooledConnForTest(addr); c != nil {
					t.Errorf("dead target %s is still pooled", addr)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(notified) != peers {
				t.Fatalf("OnConnectionFailed delivered for %v, want all %d dead targets", notified, peers)
			}
		})
	}
}

// TestSnapshotStreamRefusal_ReleasesItsSlotOnce: a snapshot stream refused for
// the per-connection snapshot limit gives its data-stream slot back exactly
// once, whatever the refusal's log handler does. The slot was released before
// the warning and marked released only after it, so an abnormal exit made the
// stream's deferred cleanup release it a second time: it blocked forever on
// the empty semaphore, and Stop waited on that goroutine for good.
func TestSnapshotStreamRefusal_ReleasesItsSlotOnce(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			receiver, err := NewQUICTransport(Config{
				ListenAddress:             "127.0.0.1:0",
				DeploymentID:              42,
				MTLSConfig:                mtls,
				DialTimeout:               testDialTimeout,
				MaxConcurrentSnapshotRecv: 1,
			}, newTestHandler(), registry.NewRegistry())
			if err != nil {
				t.Fatalf("NewQUICTransport(receiver): %v", err)
			}
			if err := receiver.Start(); err != nil {
				t.Fatalf("receiver.Start: %v", err)
			}
			addr := receiver.Addr().String()
			sender := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
			conn, err := sender.getConnection(addr, "")
			if err != nil {
				t.Fatalf("getConnection: %v", err)
			}
			// refusable marks a stream the receiver may refuse. The receiver
			// decides on the frame header alone and cancels the stream at
			// once, so its STOP_SENDING can land between the header's two
			// writes (WriteFrame writes the frame header, then the payload)
			// and fail the second one. That cancel is the refusal itself, not
			// a test failure; any other write error still is.
			openSnapshot := func(refusable bool) *quic.Stream {
				s, err := conn.OpenStreamSync(context.Background())
				if err != nil {
					t.Fatalf("OpenStreamSync: %v", err)
				}
				chunk := proto.SnapshotChunk{ShardID: 1, ReplicaID: 2, From: 1, Index: 10, Term: 1, ChunkCount: 2}
				if err := writeSnapshotHeader(s, &chunk, WireVersion); err != nil {
					var serr *quic.StreamError
					if !refusable || !errors.As(err, &serr) || !serr.Remote ||
						serr.ErrorCode != QUICStreamErrReceiverGone {
						t.Fatalf("writeSnapshotHeader: %v", err)
					}
				}
				return s
			}
			// The first transfer takes the connection's only snapshot slot and
			// waits for chunks that never come.
			first := openSnapshot(false)
			defer first.CancelWrite(0)
			pollUntil(t, 10*time.Second, time.Millisecond, "the first snapshot never took its slot",
				func() bool { return receiver.snapshotTracker.InFlight() == 1 })

			refused := installHostileRecords(t, "snapshot stream dropped", exit)
			second := openSnapshot(true)
			defer second.CancelWrite(0)
			awaitCount(t, refused, 1, "the second snapshot stream was never refused")

			stopped := make(chan error, 1)
			go func() { stopped <- receiver.Stop() }()
			select {
			case <-stopped:
			case <-time.After(15 * time.Second):
				t.Fatal("Stop hung: the refused stream's slot was released twice")
			}
		})
	}
}

// TestDialEvents_RunOutsideTheDial: a connection event's listener runs after
// the dial has settled, isolated from the send that triggered it. The events
// used to fire inside the coalesced dial, so a callback that panicked or
// called runtime.Goexit re-raised it in every sender waiting on that dial and
// lost the batch that was about to use the new connection.
func TestDialEvents_RunOutsideTheDial(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			received := newTestHandler()
			receiver := newStartedTransport(t, mtls, registry.NewRegistry(), received)
			reg := registry.NewRegistry()
			reg.Register(1, 2, receiver.Addr().String())
			var established atomic.Int32
			sender := newStartedTransport(t, mtls, reg, newTestHandler(), func(c *Config) {
				c.EventListener = &config.EventListener{
					OnConnectionEstablished: func(config.ConnectionInfo) {
						if established.Add(1) == 1 {
							exit()
						}
					},
				}
			})
			sender.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
			received.waitBatch(t, 5*time.Second)
			if established.Load() != 1 {
				t.Fatalf("OnConnectionEstablished fired %d times, want 1", established.Load())
			}
		})
	}
}

// TestSendSnapshot_CompletionIsolatedFromListeners: a snapshot fully on the
// wire is reported sent, whatever OnSnapshotSent does. The completion events
// ran inline after the success was recorded, so a listener that panicked or
// called runtime.Goexit turned a delivered snapshot into a failed send — and
// the host into reporting it rejected.
func TestSendSnapshot_CompletionIsolatedFromListeners(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			receiver := newStartedTransport(t, mtls, registry.NewRegistry(), newTestHandler())
			reg := registry.NewRegistry()
			reg.Register(1, 2, receiver.Addr().String())
			var aborted atomic.Bool
			sender := newStartedTransport(t, mtls, reg, newTestHandler(), func(c *Config) {
				c.EventListener = &config.EventListener{
					OnSnapshotSent:        func(config.SnapshotInfo) { exit() },
					OnSnapshotSendAborted: func(config.SnapshotInfo) { aborted.Store(true) },
				}
			})
			chunks := []proto.SnapshotChunk{{ShardID: 1, ReplicaID: 2, From: 1, Index: 10, Term: 1, ChunkCount: 1, Data: []byte("state")}}
			result := make(chan error, 1)
			go func() {
				returned := false
				defer func() {
					if r := recover(); r != nil {
						result <- fmt.Errorf("SendSnapshot panicked: %v", r)
						return
					}
					if !returned {
						result <- errors.New("SendSnapshot did not return")
					}
				}()
				err := sender.SendSnapshot(chunks)
				returned = true
				result <- err
			}()
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("SendSnapshot = %v, want nil: the snapshot was delivered", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("SendSnapshot never finished")
			}
			if aborted.Load() {
				t.Fatal("OnSnapshotSendAborted fired for a delivered snapshot")
			}
		})
	}
}
