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

package quicraft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// recordingDeliverer is a fake engineDeliverer that returns a fixed node and
// records the messages handed to DeliverMessage, so a test can assert whether a
// message reached the raft inbox or was dropped by the handler.
type recordingDeliverer struct {
	node      *engine.Node
	delivered []proto.Message
}

func (r *recordingDeliverer) DeliverMessage(_ uint64, msg proto.Message) bool {
	r.delivered = append(r.delivered, msg)
	return true
}
func (r *recordingDeliverer) DeliverLocal(_ uint64, _ proto.Message) bool         { return true }
func (r *recordingDeliverer) DeliverLocalTo(_ *engine.Node, _ proto.Message) bool { return true }
func (r *recordingDeliverer) NotifyWork(_ uint64)                                 {}
func (r *recordingDeliverer) GetNode(_ uint64) *engine.Node                       { return r.node }
func (r *recordingDeliverer) RequestSnapshotRecovery(_, _ uint64, _ *engine.Node) error {
	return nil
}

// TestHostMessageHandler_HandleMessage_ForgedFromDropped is the regression
// test: msg.From is the replica ID raft counts toward election/commit quorum,
// so it must be bound to the authenticated peer. A forged-From injection must be
// dropped BEFORE it reaches the engine inbox — otherwise any authenticated
// cluster node could manufacture a false quorum with another member's
// ReplicateResp/RequestVoteResp.
//
// The senderAuthorized closure is per inbound CONNECTION: it answers for the
// leaf certificate of the peer that opened it. The forger therefore holds its
// OWN certificate, which is what an authenticated-but-compromised cluster node
// has. A closure that answers true for the victim's address models an attacker
// in possession of the victim's private key, which no receive-side check can
// defend against and which would make this test vacuous.
func TestHostMessageHandler_HandleMessage_ForgedFromDropped(t *testing.T) {
	const memberAddr = "127.0.0.1:5002"
	const forgerAddr = "10.0.0.9:9999"

	// Membership {1, 2} for shard 1; local replica is 1, so From=2 is a member.
	node := newEngineNodeWithMembership(t, 1, 1, map[uint64]string{
		1: "127.0.0.1:5001",
		2: memberAddr,
	})

	newRig := func() (*recordingDeliverer, *hostMessageHandler) {
		reg := registry.NewRegistry()
		reg.Register(1, 2, memberAddr)
		eng := &recordingDeliverer{node: node}
		h := &Host{registry: reg}
		return eng, &hostMessageHandler{engine: eng, host: h}
	}

	t.Run("forged From with cleared source address is dropped", func(t *testing.T) {
		eng, handler := newRig()
		// The transport clears a source address the sender's cert does not
		// cover, so a forged-From injection arrives with SourceAddress == "".
		batch := proto.MessageBatch{
			SourceAddress: "",
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp},
			},
		}
		if err := handler.HandleMessage(batch, certCovering(forgerAddr)); err != nil {
			t.Fatalf("HandleMessage = %v", err)
		}
		if len(eng.delivered) != 0 {
			t.Fatalf("forged-From message was delivered (%d), want dropped", len(eng.delivered))
		}
	})

	t.Run("forged From from a peer holding its own certificate is dropped", func(t *testing.T) {
		eng, handler := newRig()
		batch := proto.MessageBatch{
			SourceAddress: forgerAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.RequestVoteResp},
			},
		}
		if err := handler.HandleMessage(batch, certCovering(forgerAddr)); err != nil {
			t.Fatalf("HandleMessage = %v", err)
		}
		if len(eng.delivered) != 0 {
			t.Fatalf("forged-From message was delivered (%d), want dropped", len(eng.delivered))
		}
	})

	t.Run("nil senderAuthorized drops every quorum-bearing message", func(t *testing.T) {
		eng, handler := newRig()
		batch := proto.MessageBatch{
			SourceAddress: memberAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp},
			},
		}
		if err := handler.HandleMessage(batch, transport.PeerAuth{}); err != nil {
			t.Fatalf("HandleMessage = %v", err)
		}
		if len(eng.delivered) != 0 {
			t.Fatalf("a message was delivered (%d) with no way to check the peer "+
				"certificate, want dropped", len(eng.delivered))
		}
	})

	t.Run("genuine member under its own certificate is delivered", func(t *testing.T) {
		eng, handler := newRig()
		batch := proto.MessageBatch{
			SourceAddress: memberAddr,
			Requests: []proto.Message{
				{ShardID: 1, From: 2, To: 1, Type: proto.ReplicateResp},
			},
		}
		// The member's own keypair: newEngineNodeWithMembership commits
		// fixtureIdentity for every member, and that is what the gate reads.
		if err := handler.HandleMessage(batch, certCoveringAs(fixtureIdentity, memberAddr)); err != nil {
			t.Fatalf("HandleMessage = %v", err)
		}
		if len(eng.delivered) != 1 {
			t.Fatalf("genuine member message delivered %d times, want 1", len(eng.delivered))
		}
	})
}
