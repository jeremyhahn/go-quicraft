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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestPeerRejectedHeartbeatFrameKeepsConnection is the heartbeat-stream twin of
// TestPeerRejectedFrameKeepsConnection, and it matters MORE than the data-path
// case it mirrors.
//
// Heartbeats are the one thing a node emits unconditionally, every tick, to
// every peer. If a peer's decoder refuses those frames — a deployment-ID
// mismatch here, equally a wire-version skew — and the sender reads the
// resulting stream reset as a dead connection, then every heartbeat tick costs
// a connection teardown plus a re-dial, to every peer, forever. That is the
// dial storm the frame-rejection branch exists to prevent, and it is reached
// through the heartbeat path first and most often.
//
// A rejected frame means "that frame was unacceptable", not "this connection is
// dead": the stream is replaced, the connection is kept, and the drop is
// counted so a silent, permanently-rejecting peer is still visible to an
// operator.
func TestPeerRejectedHeartbeatFrameKeepsConnection(t *testing.T) {
	mtls := testMTLSConfig(t)

	// Receiver in a DIFFERENT deployment: it authenticates the sender at the
	// TLS layer, then refuses every batch it decodes.
	receiver, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  43,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(receiver) failed: %v", err)
	}
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver.Start failed: %v", err)
	}
	defer func() { _ = receiver.Stop() }()

	dials := &atomic.Int64{}
	sendReg := registry.NewRegistry()
	metrics := &dropRecorder{}
	sender, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		Metrics:       metrics,
		EventListener: &config.EventListener{
			OnConnectionEstablished: func(config.ConnectionInfo) { dials.Add(1) },
		},
	}, newTestHandler(), sendReg)
	if err != nil {
		t.Fatalf("NewQUICTransport(sender) failed: %v", err)
	}
	if err := sender.Start(); err != nil {
		t.Fatalf("sender.Start failed: %v", err)
	}
	defer func() { _ = sender.Stop() }()

	sendReg.Register(1, 2, receiver.Addr().String())

	// A bare Heartbeat carries neither entries nor a snapshot, so Send routes
	// it through the heartbeat fast path and its dedicated stream (index 0).
	const attempts = 20
	for range attempts {
		sender.Send([]proto.Message{{
			Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1,
		}})
		time.Sleep(50 * time.Millisecond)
	}

	if got := dials.Load(); got != 1 {
		t.Fatalf("a rejected heartbeat frame caused re-dialing: %d dials after "+
			"%d heartbeats (want 1). Heartbeats are emitted every tick to every "+
			"peer, so treating a frame rejection as a dead connection turns a "+
			"wire-version or deployment mismatch into a permanent dial storm",
			got, attempts)
	}
	if sender.PeerFrameRejections() == 0 {
		t.Fatal("heartbeat frames rejected by the peer were not counted: the " +
			"only signal an operator gets for a peer that silently refuses " +
			"every frame is this counter and its rate-limited warning")
	}

	var rejected int
	for _, d := range metrics.drops() {
		if d.reason == dropReasonPeerRejected {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatalf("no heartbeat drop was reported to the metrics collector with "+
			"reason %q; drops recorded: %v", dropReasonPeerRejected, metrics.drops())
	}
}
