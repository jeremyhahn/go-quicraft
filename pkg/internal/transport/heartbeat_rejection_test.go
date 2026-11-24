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

// TestPeerRejectedHeartbeatFrameKeepsConnection verifies a rejected heartbeat frame replaces the stream, not the connection.
func TestPeerRejectedHeartbeatFrameKeepsConnection(t *testing.T) {
	mtls := testMTLSConfig(t)

	// A receiver in another deployment refuses every batch.
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

	// A bare Heartbeat goes through the heartbeat fast path on stream 0.
	const wantRejections = 3
	attempts := 0
	pollUntil(t, 10*time.Second, 50*time.Millisecond,
		"heartbeat frames rejected by the peer were not counted: the only "+
			"signal an operator gets for a peer that silently refuses every "+
			"frame is this counter and its rate-limited warning",
		func() bool {
			if sender.PeerFrameRejections() >= wantRejections {
				return true
			}
			attempts++
			sender.Send([]proto.Message{{
				Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2, Term: 1,
			}})
			return false
		})

	if got := dials.Load(); got != 1 {
		t.Fatalf("a rejected heartbeat frame caused re-dialing: %d dials after "+
			"%d heartbeats (want 1). Heartbeats are emitted every tick to every "+
			"peer, so treating a frame rejection as a dead connection turns a "+
			"wire-version or deployment mismatch into a permanent dial storm",
			got, attempts)
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
