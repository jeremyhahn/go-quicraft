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
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Settle first, log last, on every transport recovery path
//
// A transport goroutine's slog handler is user code (slog.SetDefault). Each
// recovery used to log inline from its deferred recover: a handler that
// panicked there crashed the process, and one that called runtime.Goexit
// ended the goroutine — for the send-queue worker, the connection cleanup and
// the accept loop, for good, since a Goexit is not a panic their restart loops
// recover. A Goexit in the code those loops run (a resolver, a handler) ended
// them the same way. These tests drive each path with a handler that exits on
// the path's own records and check the path is still working afterwards.
// ---------------------------------------------------------------------------

// hostileRecordHandler counts the records whose message starts with prefix and
// runs act (a panic or runtime.Goexit) on each of them; every other record is
// discarded.
type hostileRecordHandler struct {
	prefix string
	act    func()
	n      *atomic.Int32
}

func (h hostileRecordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h hostileRecordHandler) Handle(_ context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, h.prefix) {
		h.n.Add(1)
		h.act()
	}
	return nil
}
func (h hostileRecordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h hostileRecordHandler) WithGroup(string) slog.Handler      { return h }

// installHostileRecords makes the default slog handler run act on every record
// whose message starts with prefix, until the test ends, and returns the count
// of such records.
func installHostileRecords(t *testing.T, prefix string, act func()) *atomic.Int32 {
	t.Helper()
	n := new(atomic.Int32)
	prev := slog.Default()
	slog.SetDefault(slog.New(hostileRecordHandler{prefix: prefix, act: act, n: n}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return n
}

// hostileLogExits are the two ways a slog handler can leave its caller.
var hostileLogExits = map[string]func(){
	"panic":  func() { panic("slog handler panicked") },
	"Goexit": runtime.Goexit,
}

// awaitCount fails the test unless n reaches want within five seconds.
func awaitCount(t *testing.T, n *atomic.Int32, want int32, what string) {
	t.Helper()
	pollUntil(t, 5*time.Second, time.Millisecond, what, func() bool { return n.Load() >= want })
}

// TestSendQueueWorker_SurvivesAbnormalExits: the send-queue worker keeps
// draining its queue after the code it runs — here the replica-identity
// resolver — panics while the recovery's log handler panics or exits, and
// after the resolver itself calls runtime.Goexit.
func TestSendQueueWorker_SurvivesAbnormalExits(t *testing.T) {
	type exitCase struct {
		resolver func() // how the resolver's first call ends
		logExit  func() // how the recovery's log handler ends
	}
	cases := map[string]exitCase{
		"resolver panic/log panic":  {func() { panic("resolver panicked") }, hostileLogExits["panic"]},
		"resolver panic/log Goexit": {func() { panic("resolver panicked") }, hostileLogExits["Goexit"]},
		"resolver Goexit":           {runtime.Goexit, func() {}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			exiting := make(chan struct{})
			resolved := make(chan struct{}, 1)
			tr, err := NewQUICTransport(Config{
				ListenAddress: "127.0.0.1:0",
				DeploymentID:  42,
				MTLSConfig:    testMTLSConfig(t),
				ReplicaIdentity: func(uint64, uint64) string {
					if calls.Add(1) == 1 {
						close(exiting)
						c.resolver()
					}
					select {
					case resolved <- struct{}{}:
					default:
					}
					return "" // unbound: dropped without a dial
				},
			}, newTestHandler(), registry.NewRegistry())
			if err != nil {
				t.Fatalf("NewQUICTransport: %v", err)
			}
			if err := tr.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer tr.Stop()
			installHostileRecords(t, "sendQueueWorker panic recovered", c.logExit)

			sq := tr.getOrCreateSendQueue("127.0.0.1:9")
			sq.ch <- proto.Message{Type: proto.Replicate, ShardID: 1, From: 1, To: 2}
			<-exiting
			sq.ch <- proto.Message{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 2}
			select {
			case <-resolved:
			case <-time.After(5 * time.Second):
				t.Fatal("the send-queue worker never resumed draining its queue")
			}
		})
	}
}

// TestConnectionCleanup_SurvivesHostileRecoveryLog: the connection cleanup
// loop keeps sweeping after a sweep panics and the recovery's log handler
// panics or exits.
func TestConnectionCleanup_SurvivesHostileRecoveryLog(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			tr, err := NewQUICTransport(Config{
				ListenAddress: "127.0.0.1:0",
				DeploymentID:  42,
				MTLSConfig:    testMTLSConfig(t),
			}, newTestHandler(), registry.NewRegistry())
			if err != nil {
				t.Fatalf("NewQUICTransport: %v", err)
			}
			tr.cleanupInterval = time.Millisecond
			if err := tr.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer tr.Stop()
			recovered := installHostileRecords(t, "connectionCleanup panic recovered", exit)

			// A nil pooled connection panics every sweep that reaches it.
			tr.connMu.Lock()
			tr.conns["nil-conn"] = nil
			tr.connMu.Unlock()
			defer func() {
				tr.connMu.Lock()
				delete(tr.conns, "nil-conn")
				tr.connMu.Unlock()
			}()

			awaitCount(t, recovered, 2, "the connection cleanup loop did not sweep again after its recovery's log handler exited")
		})
	}
}

// TestAcceptLoop_SurvivesAbnormalExits: the accept loop keeps admitting (and
// refusing) connections after its own log handler — run inline for the
// per-IP limit warning — panics, with the recovery's handler panicking or
// exiting in turn, or calls runtime.Goexit.
func TestAcceptLoop_SurvivesAbnormalExits(t *testing.T) {
	type exitCase struct {
		prefix string
		act    func()
	}
	const (
		limitRecord     = "per-IP connection limit reached"
		recoveredRecord = "panic recovered in acceptLoop"
	)
	cases := map[string]exitCase{
		// The warning's handler panics; the iteration's recovery logs, and its
		// handler panics too (the empty prefix selects anyOfHandler).
		"limit panic/log panic": {"", func() { panic("slog handler panicked") }},
		// The warning's handler calls runtime.Goexit inside the accept loop.
		"limit Goexit": {limitRecord, runtime.Goexit},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			mtls := testMTLSConfig(t)
			received := newTestHandler()
			receiver, err := NewQUICTransport(Config{
				ListenAddress:       "127.0.0.1:0",
				DeploymentID:        42,
				MTLSConfig:          mtls,
				MaxConnectionsPerIP: 1,
				DialTimeout:         testDialTimeout,
			}, received, registry.NewRegistry())
			if err != nil {
				t.Fatalf("NewQUICTransport(receiver): %v", err)
			}
			if err := receiver.Start(); err != nil {
				t.Fatalf("receiver.Start: %v", err)
			}
			defer receiver.Stop()
			addr := receiver.Addr().String()

			newSender := func() *QUICTransport {
				reg := registry.NewRegistry()
				reg.Register(1, 2, addr)
				s, err := NewQUICTransport(Config{
					ListenAddress: "127.0.0.1:0",
					DeploymentID:  42,
					MTLSConfig:    mtls,
					DialTimeout:   testDialTimeout,
				}, newTestHandler(), reg)
				if err != nil {
					t.Fatalf("NewQUICTransport(sender): %v", err)
				}
				if err := s.Start(); err != nil {
					t.Fatalf("sender.Start: %v", err)
				}
				t.Cleanup(func() { _ = s.Stop() })
				return s
			}
			send := func(s *QUICTransport) {
				s.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
			}

			// The first sender takes the address's only admission slot.
			first := newSender()
			send(first)
			received.waitBatch(t, 10*time.Second)

			var limited *atomic.Int32
			if c.prefix == "" {
				// Both records exit: the warning (inline in the loop) and the
				// recovery's own diagnostic.
				limited = new(atomic.Int32)
				prev := slog.Default()
				slog.SetDefault(slog.New(anyOfHandler{
					prefixes: []string{limitRecord, recoveredRecord},
					counted:  limitRecord,
					act:      c.act,
					n:        limited,
				}))
				t.Cleanup(func() { slog.SetDefault(prev) })
			} else {
				limited = installHostileRecords(t, c.prefix, c.act)
			}

			send(newSender())
			awaitCount(t, limited, 1, "the second connection was never refused")
			send(newSender())
			awaitCount(t, limited, 2, "the accept loop did not admit (or refuse) another connection after its handler exited")
		})
	}
}

// anyOfHandler runs act on every record whose message starts with one of
// prefixes, counting those that start with counted.
type anyOfHandler struct {
	prefixes []string
	counted  string
	act      func()
	n        *atomic.Int32
}

func (h anyOfHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h anyOfHandler) Handle(_ context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, h.counted) {
		h.n.Add(1)
	}
	for _, p := range h.prefixes {
		if strings.HasPrefix(r.Message, p) {
			h.act()
		}
	}
	return nil
}
func (h anyOfHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h anyOfHandler) WithGroup(string) slog.Handler      { return h }

// TestHandleStream_SurvivesHostileRecoveryLog: a stream whose message handler
// panics is recovered, and a recovery log handler that panics or exits does
// not take the receiver down.
func TestHandleStream_SurvivesHostileRecoveryLog(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			sender, receiver, cleanup := transportPairWith(t, 42, newTestHandler(), &panicOnHandleHandler{})
			defer cleanup()
			recovered := installHostileRecords(t, "panic recovered in handleStream", exit)

			sender.Send([]proto.Message{{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1}})
			awaitCount(t, recovered, 1, "the stream's panic was never recovered")
			if receiver.stopped.Load() {
				t.Fatal("the receiver stopped after a stream's recovery")
			}
		})
	}
}

// TestPreOpenHeartbeatStream_SurvivesHostileRecoveryLog: a heartbeat
// pre-open that panics releases its WaitGroup slot, and a recovery log
// handler that panics or exits does not take the process down.
func TestPreOpenHeartbeatStream_SurvivesHostileRecoveryLog(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			tr, err := NewQUICTransport(Config{
				ListenAddress: "127.0.0.1:0",
				DeploymentID:  42,
				MTLSConfig:    testMTLSConfig(t),
			}, newTestHandler(), registry.NewRegistry())
			if err != nil {
				t.Fatalf("NewQUICTransport: %v", err)
			}
			recovered := installHostileRecords(t, "preOpenHeartbeatStream panic recovered", exit)

			done := make(chan struct{})
			tr.wg.Add(1)
			go func() {
				defer close(done)
				tr.preOpenHeartbeatStream("nil-conn", nil) // a nil connection panics
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("preOpenHeartbeatStream never returned")
			}
			tr.wg.Wait()
			awaitCount(t, recovered, 1, "the pre-open's panic was never logged")
		})
	}
}
