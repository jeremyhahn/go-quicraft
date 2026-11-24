// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// BenchmarkPeerHeartbeatDrainCommitPending measures a follower stepping a heartbeat and draining free-order messages during a WAL write.
func BenchmarkPeerHeartbeatDrainCommitPending(b *testing.B) {
	b.ReportAllocs()
	cfg := testConfig()
	lr := newTestLogReader(nil, proto.State{Term: 5})
	lr.membership = proto.Membership{Addresses: map[uint64]string{
		1: "a1", 2: "a2", 3: "a3",
	}}
	p, err := NewPeer(cfg, lr, nil)
	if err != nil {
		b.Fatalf("NewPeer: %v", err)
	}
	hb := proto.Message{Type: proto.Heartbeat, From: 2, To: 1, Term: 5}
	for b.Loop() {
		if err := p.Handle(hb); err != nil {
			b.Fatalf("Handle: %v", err)
		}
		_ = p.DrainFreeOrderMessages()
		p.r.msgs = p.r.msgs[:0]
	}
}
