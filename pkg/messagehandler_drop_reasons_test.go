package quicraft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// unauthorizedDrop is one IncUnauthorizedMessageDropped call.
type unauthorizedDrop struct {
	shardID uint64
	from    uint64
	reason  string
}

// recordingDropMetrics records every unauthorized-drop count with its labels,
// so a test can assert the count, the reason, and the shard and sender it
// carries.
type recordingDropMetrics struct {
	NoOpMetricsCollector
	drops []unauthorizedDrop
}

func (m *recordingDropMetrics) IncUnauthorizedMessageDropped(shardID, from uint64, reason string) {
	m.drops = append(m.drops, unauthorizedDrop{shardID: shardID, from: from, reason: reason})
}

const (
	dropReasonShard     = uint64(1)
	dropReasonAddr1     = "10.0.0.1:5001"
	dropReasonAddr2     = "10.0.0.2:5001"
	dropReasonAddr3     = "10.0.0.3:5001"
	dropReasonAddr4     = "10.0.0.4:5001"
	dropReasonID2       = "spki:2222222222222222222222222222222222222222222222222222222222222222"
	dropReasonID3       = "spki:3333333333333333333333333333333333333333333333333333333333333333"
	dropReasonRemovedID = "spki:4444444444444444444444444444444444444444444444444444444444444444"
)

// dropReasonRig builds a handler for replica 1 of shard 1 whose committed
// membership holds current members 1, 2 and 3 (2 and 3 with identities) and a
// committed removal of replica 4, whose identity is retained as removed.
func dropReasonRig(t *testing.T) (*recordingDeliverer, *hostMessageHandler, *recordingDropMetrics) {
	t.Helper()
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: dropReasonAddr1,
			2: dropReasonAddr2,
			3: dropReasonAddr3,
		},
		Identities:        map[uint64]string{2: dropReasonID2, 3: dropReasonID3},
		Removed:           map[uint64]bool{4: true},
		RemovedIdentities: map[uint64]string{4: dropReasonRemovedID},
	}
	node := newEngineNodeWithFullMembership(t, dropReasonShard, 1, ms)
	if !node.IsRemoved(4) || !node.IsRemovedIdentity(dropReasonRemovedID) {
		t.Fatal("fixture: replica 4 and its identity must be committed as removed")
	}
	eng := &recordingDeliverer{node: node}
	metrics := &recordingDropMetrics{}
	host := &Host{
		cfg:      config.HostConfig{},
		registry: registry.NewRegistry(),
		metrics:  metrics,
	}
	return eng, &hostMessageHandler{engine: eng, host: host}, metrics
}

// TestHandleMessage_DropReasonsAreCountedAsDocumented drives a message to each
// of the removed_replica, removed_identity and misrouted drop reasons that
// docs/observability/architecture.md documents for
// SecurityMetrics.IncUnauthorizedMessageDropped, and checks that a valid
// message from a current member in the same batch is still delivered.
func TestHandleMessage_DropReasonsAreCountedAsDocumented(t *testing.T) {
	removedPeer := certCoveringAs(dropReasonRemovedID, dropReasonAddr4)
	member2Peer := certCoveringAs(dropReasonID2, dropReasonAddr2)
	member3Peer := certCoveringAs(dropReasonID3, dropReasonAddr3)

	cases := []struct {
		name   string
		source string
		peer   transport.PeerAuth
		bad    proto.Message
		reason string
	}{
		{
			// (a) A removed replica that has not learned of its removal keeps
			// addressing the shard under its own ID.
			name:   "removed_replica",
			source: dropReasonAddr4,
			peer:   removedPeer,
			bad:    proto.Message{ShardID: dropReasonShard, From: 4, To: 1, Type: proto.Heartbeat, Term: 5},
			reason: dropRemovedReplica,
		},
		{
			// (b) The removed replica's keypair, claiming a current member's ID.
			name:   "removed_identity",
			source: dropReasonAddr4,
			peer:   removedPeer,
			bad:    proto.Message{ShardID: dropReasonShard, From: 2, To: 1, Type: proto.ReplicateResp, Term: 5},
			reason: dropRemovedIdentity,
		},
		{
			// (c) A current member, correctly authenticated, addressing a
			// different replica of this shard.
			name:   "misrouted",
			source: dropReasonAddr2,
			peer:   member2Peer,
			bad:    proto.Message{ShardID: dropReasonShard, From: 2, To: 3, Type: proto.HeartbeatResp, Term: 5},
			reason: dropMisrouted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng, handler, metrics := dropReasonRig(t)

			// The bad message alone.
			if err := handler.HandleMessage(proto.MessageBatch{
				SourceAddress: tc.source,
				Requests:      []proto.Message{tc.bad},
			}, tc.peer); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 0 {
				t.Fatalf("a message that must be dropped as %s reached raft: %+v",
					tc.reason, eng.delivered)
			}
			if len(metrics.drops) != 1 {
				t.Fatalf("unauthorized drops = %+v, want exactly one", metrics.drops)
			}
			// (d) The count carries the reason, shard and claimed From.
			want := unauthorizedDrop{shardID: tc.bad.ShardID, from: tc.bad.From, reason: tc.reason}
			if got := metrics.drops[0]; got != want {
				t.Fatalf("drop = %+v, want %+v", got, want)
			}
			if tc.reason == dropRemovedIdentity && metrics.drops[0].reason == dropIdentityMismatch {
				t.Fatal("a removed replica's identity claiming a member's ID was counted " +
					"as identity_mismatch")
			}
		})

		// (d) A batch carrying the bad message and a valid one from a current
		// member: the valid one is delivered and counts nothing.
		t.Run(tc.name+"/with_valid_in_batch", func(t *testing.T) {
			eng, handler, metrics := dropReasonRig(t)
			valid := proto.Message{ShardID: dropReasonShard, From: 3, To: 1, Type: proto.HeartbeatResp, Term: 5}

			// The valid message alone counts nothing, so any count in the
			// mixed batch is the bad message's.
			if err := handler.HandleMessage(proto.MessageBatch{
				SourceAddress: dropReasonAddr3,
				Requests:      []proto.Message{valid},
			}, member3Peer); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(eng.delivered) != 1 || len(metrics.drops) != 0 {
				t.Fatalf("a valid message from a current member: delivered %d, drops %+v; "+
					"want 1 delivered, no drops", len(eng.delivered), metrics.drops)
			}
			eng.delivered = nil

			// A batch is one connection's. The misrouted message comes from a
			// current member, so its batch also carries a valid message from
			// that member. A removed replica's connection has no valid
			// message to send, so for (a) and (b) the valid message arrives
			// in the next batch, from a current member, on the same handler.
			peer, source := tc.peer, tc.source
			bad := tc.bad
			if tc.reason != dropMisrouted {
				if err := handler.HandleMessage(proto.MessageBatch{
					SourceAddress: source,
					Requests:      []proto.Message{bad},
				}, peer); err != nil {
					t.Fatalf("HandleMessage: %v", err)
				}
				peer, source = member3Peer, dropReasonAddr3
				if err := handler.HandleMessage(proto.MessageBatch{
					SourceAddress: source,
					Requests:      []proto.Message{valid},
				}, peer); err != nil {
					t.Fatalf("HandleMessage: %v", err)
				}
			} else {
				valid2 := proto.Message{ShardID: dropReasonShard, From: 2, To: 1, Type: proto.HeartbeatResp, Term: 5}
				if err := handler.HandleMessage(proto.MessageBatch{
					SourceAddress: source,
					Requests:      []proto.Message{bad, valid2},
				}, peer); err != nil {
					t.Fatalf("HandleMessage: %v", err)
				}
				valid = valid2
			}
			if len(eng.delivered) != 1 || eng.delivered[0].From != valid.From ||
				eng.delivered[0].To != valid.To {
				t.Fatalf("delivered = %+v, want only the valid message %+v", eng.delivered, valid)
			}
			want := []unauthorizedDrop{{shardID: bad.ShardID, from: bad.From, reason: tc.reason}}
			if len(metrics.drops) != 1 || metrics.drops[0] != want[0] {
				t.Fatalf("drops = %+v, want %+v", metrics.drops, want)
			}
		})
	}
}
