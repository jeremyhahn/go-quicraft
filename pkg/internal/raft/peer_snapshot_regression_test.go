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

package raft

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// installSnapshotMsg builds a local-origin InstallSnapshot from leader 2 to replica 1, the only way one reaches raft.
func installSnapshotMsg(ss proto.Snapshot) proto.Message {
	return proto.Message{
		Type:     proto.InstallSnapshot,
		Origin:   proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     ss.Term,
		Snapshot: ss,
	}
}

// localInstallSnapshotMsg builds the InstallSnapshot exactly as
// hostMessageHandler.HandleSnapshot does: locally injected, with NO term.
func localInstallSnapshotMsg(ss proto.Snapshot) proto.Message {
	msg := installSnapshotMsg(ss)
	msg.Term = 0
	return msg
}

// TestPeer_InstallSnapshot_PendingSnapshotIsNeverSuperseded verifies a second snapshot in the same batch never leaves an obsolete pending snapshot.
func TestPeer_InstallSnapshot_PendingSnapshotIsNeverSuperseded(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	// A diverged local log: entries 1..12 from an older term.
	p.r.log.inmem.merge(makeEntries(1, 13, 2))

	// 1. Snapshot A at index 5 conflicts with the local log (term 9 vs
	// term 2), so the log is fully replaced and A becomes pending.
	snapA := proto.Snapshot{
		Index: 5,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	if err := p.Handle(installSnapshotMsg(snapA)); err != nil {
		t.Fatalf("InstallSnapshot A: %v", err)
	}
	if ss := p.r.log.pendingSnapshot(); ss == nil || ss.Index != snapA.Index {
		t.Fatalf("precondition: snapshot %d must be pending after the restore, got %+v",
			snapA.Index, ss)
	}

	// 2. The leader's follow-up entries 6..12 land in the same batch and
	// extend the restored log.
	if err := p.Handle(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       1,
		ShardID:  1,
		Term:     9,
		LogIndex: snapA.Index,
		LogTerm:  snapA.Term,
		Commit:   snapA.Index,
		Entries:  makeEntries(6, 13, 9),
	}); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// Snapshot B at index 10 matches the log before the engine has snapshot A.
	snapB := proto.Snapshot{
		Index: 10,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 8,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	if err := p.Handle(installSnapshotMsg(snapB)); err != nil {
		t.Fatalf("InstallSnapshot B: %v", err)
	}

	if ss := p.r.log.pendingSnapshot(); ss == nil || ss.Index != snapB.Index {
		t.Fatalf("pending snapshot must advance to the newest snapshot %d, got %+v",
			snapB.Index, ss)
	}

	update, err := p.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if update.Snapshot.Index != snapB.Index {
		t.Fatalf("GetUpdate emitted snapshot %d, want the newest snapshot %d "+
			"(processed=%d): a superseded snapshot rewinds the LogReader and "+
			"fails the shard", update.Snapshot.Index, snapB.Index, p.r.log.processed)
	}
	// The emitted snapshot must cover everything the log already counts as
	// applied; anything below it would leave a permanent apply hole.
	if update.Snapshot.Index < p.r.log.processed {
		t.Fatalf("emitted snapshot %d is behind the processed marker %d",
			update.Snapshot.Index, p.r.log.processed)
	}
}

// TestPeer_InstallSnapshot_RejectedSnapshotKeepsMembership verifies a rejected snapshot never overwrites membership while another is pending.
func TestPeer_InstallSnapshot_RejectedSnapshotKeepsMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	// A diverged local log: entries 1..12 from an older term.
	p.r.log.inmem.merge(makeEntries(1, 13, 2))

	// The current snapshot at index 10 conflicts with the local log and
	// restores it, adding replica 4 to the membership. It stays pending
	// until the engine acknowledges its persistence.
	current := proto.Snapshot{
		Index: 10,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 8,
			Addresses: map[uint64]string{
				1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4",
			},
		},
	}
	if err := p.Handle(installSnapshotMsg(current)); err != nil {
		t.Fatalf("InstallSnapshot current: %v", err)
	}
	if p.r.log.pendingSnapshot() == nil {
		t.Fatal("precondition: the restored snapshot must still be pending")
	}

	// A retransmitted older snapshot in the same batch is rejected.
	stale := proto.Snapshot{
		Index: 5,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 3,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
		},
	}
	if err := p.Handle(installSnapshotMsg(stale)); err != nil {
		t.Fatalf("InstallSnapshot stale: %v", err)
	}

	m := p.GetMembership()
	if _, ok := m.Addresses[4]; !ok {
		t.Fatalf("membership rolled back by a rejected snapshot: %v", m.Addresses)
	}
	if m.ConfigChangeID != current.Membership.ConfigChangeID {
		t.Fatalf("expected configChangeID %d, got %d",
			current.Membership.ConfigChangeID, m.ConfigChangeID)
	}
	if _, ok := p.r.remotes[4]; !ok {
		t.Fatal("raft remotes rolled back by a rejected snapshot")
	}
}

// TestPeer_InstallSnapshot_StaleTermDoesNotRewriteMembership verifies a stale-term InstallSnapshot does not change membership.
func TestPeer_InstallSnapshot_StaleTermDoesNotRewriteMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	if len(p.membership.addresses) != 3 {
		t.Fatalf("precondition: expected 3 voters, got %d", len(p.membership.addresses))
	}

	// A deposed leader (or any shard member) at term 3 sends a snapshot
	// carrying a configuration of its choosing, with a ConfigChangeID high
	// enough to beat the strictly-newer test.
	forged := proto.Snapshot{
		Index: 50,
		Term:  3,
		Membership: proto.Membership{
			ConfigChangeID: 9999,
			Addresses:      map[uint64]string{2: "addr2"},
		},
	}
	msg := installSnapshotMsg(forged)
	msg.Term = 3 // stale: below the local term 9

	if err := p.Handle(msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(p.membership.addresses) != 3 {
		t.Fatalf("membership rewritten to %v by a STALE-TERM InstallSnapshot "+
			"that raft dropped without acting on: quorum() is now computed "+
			"over a configuration no committed entry ever established",
			p.membership.addresses)
	}
	if p.membership.configChangeID == forged.Membership.ConfigChangeID {
		t.Fatalf("adopted ConfigChangeID %d from a stale-term InstallSnapshot",
			p.membership.configChangeID)
	}
	if p.r.log.committed != 0 {
		t.Fatalf("committed advanced to %d on a dropped message", p.r.log.committed)
	}
}

// A local term-less InstallSnapshot must still install its membership.
func TestPeer_InstallSnapshot_LocalInjectionAdoptsMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)

	msg := localInstallSnapshotMsg(proto.Snapshot{
		Index: 50,
		Term:  9,
		Membership: proto.Membership{
			ConfigChangeID: 7,
			Addresses:      map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3", 4: "addr4"},
		},
	})

	if err := p.Handle(msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(p.membership.addresses) != 4 {
		t.Fatalf("locally injected snapshot membership not adopted: %v", p.membership.addresses)
	}
	if p.membership.configChangeID != 7 {
		t.Fatalf("local snapshot ConfigChangeID not adopted: %d", p.membership.configChangeID)
	}
}

// A wire-origin InstallSnapshot is always forged and must be refused; this one has Term 0 and an index below committed.
func TestPeer_InstallSnapshot_WireDeliveredIsRefused(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeer(t, cfg, proto.State{}, threeNodeMembership())
	p.r.becomeFollower(9, 2)
	p.r.log.append(makeEntries(1, 11, 9))
	p.r.log.commitTo(10)
	leaderBefore := p.r.leaderID
	p.r.electionTick = 7

	forged := proto.Message{
		Type:    proto.InstallSnapshot,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    0,
		Snapshot: proto.Snapshot{
			Index: 1, // at or below committed: handleSnapshot returns nil
			Term:  9,
			Membership: proto.Membership{
				ConfigChangeID: 1 << 62,
				Addresses:      map[uint64]string{2: "attacker"},
			},
		},
		// Origin left at its zero value: proto.OriginRemote, which is what
		// UnmarshalFrom produces for anything decoded off the wire.
	}

	err := p.Handle(forged)
	if !errors.Is(err, ErrWireLocalMessage) {
		t.Fatalf("expected ErrWireLocalMessage for a wire InstallSnapshot, got %v", err)
	}
	if len(p.membership.addresses) != 3 {
		t.Fatalf("membership rewritten to %v by a WIRE InstallSnapshot: quorum() "+
			"is now computed over a configuration the attacker chose",
			p.membership.addresses)
	}
	if p.membership.configChangeID == 1<<62 {
		t.Fatalf("adopted attacker ConfigChangeID %d", p.membership.configChangeID)
	}
	if p.r.leaderID != leaderBefore {
		t.Fatalf("wire InstallSnapshot installed leader %d (was %d)",
			p.r.leaderID, leaderBefore)
	}
	if p.r.electionTick != 7 {
		t.Fatalf("wire InstallSnapshot reset the election timer to %d", p.r.electionTick)
	}
}

// Joining-node variant: a joiner's adoption is gated on the delivery path too.
func TestPeer_InstallSnapshot_JoiningNodeRefusesWireDelivery(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	// Seed only this replica, the genuinely joining state.
	p := newTestPeer(t, cfg, proto.State{},
		proto.Membership{Addresses: map[uint64]string{1: "addr1"}})
	p.MarkJoining()
	if !p.joining.Load() {
		t.Fatal("fixture: the peer must actually be joining for this test to mean anything")
	}
	p.r.becomeFollower(9, 2)

	forged := proto.Message{
		Type:    proto.InstallSnapshot,
		From:    2,
		To:      1,
		ShardID: 1,
		Snapshot: proto.Snapshot{
			Index:      1,
			Term:       9,
			Membership: proto.Membership{Addresses: map[uint64]string{2: "attacker"}},
		},
	}
	if err := p.Handle(forged); !errors.Is(err, ErrWireLocalMessage) {
		t.Fatalf("expected ErrWireLocalMessage, got %v", err)
	}
	if _, forgedIn := p.membership.addresses[2]; forgedIn {
		t.Fatalf("joining node adopted a wire membership: %v", p.membership.addresses)
	}
	if _, self := p.membership.addresses[1]; !self || len(p.membership.addresses) != 1 {
		t.Fatalf("joining node's configuration was rewritten by a wire message: %v",
			p.membership.addresses)
	}
	if !p.joining.Load() {
		t.Fatal("a wire InstallSnapshot cleared the joining flag, re-enabling " +
			"campaigning and per-shard authorization with an attacker configuration")
	}
}

// TestPeer_InstallSnapshot_RejectedSnapshotDoesNotRedefineMembership verifies a snapshot raft rejects does not change membership.
func TestPeer_InstallSnapshot_RejectedSnapshotDoesNotRedefineMembership(t *testing.T) {
	// The joining path bypasses the ConfigChangeID check, so it is tested too.
	t.Run("joining replica", func(t *testing.T) {
		p := newTestPeerWithLog(t, testConfigWithID(1, 6), contiguousEntries(10),
			proto.State{Term: 5, Commit: 10},
			proto.Membership{Addresses: map[uint64]string{6: "self"}})
		p.MarkJoining()
		if !p.joining.Load() {
			t.Fatal("fixture: the replica must actually be joining")
		}

		// Index 3 is below committed 10, delivered locally.
		err := p.Handle(proto.Message{
			Type: proto.InstallSnapshot, From: 2, To: 6, ShardID: 1, Term: 5,
			Origin: proto.OriginLocal,
			Snapshot: proto.Snapshot{
				Index: 3, Term: 5,
				Membership: proto.Membership{
					Addresses: map[uint64]string{9: "stale-only"},
				},
			},
		})
		if err != nil {
			t.Fatalf("Handle: %v", err)
		}
		if _, adopted := p.membership.addresses[9]; adopted {
			t.Fatalf("membership adopted from a REJECTED snapshot: %v.\n"+
				"raft refused this snapshot as behind the committed index, yet its "+
				"configuration replaced the local one — and it does not name replica "+
				"6 at all, leaving this replica self-removed, permanently unelectable, "+
				"and computing quorum over a stale voting set.",
				p.membership.addresses)
		}
		if _, self := p.membership.addresses[6]; !self {
			t.Fatalf("replica removed itself from its own configuration: %v",
				p.membership.addresses)
		}
	})

	// The non-joining path was reachable too, through a rejected snapshot
	// carrying a ConfigChangeID higher than the local one.
	t.Run("established replica", func(t *testing.T) {
		p := newTestPeerWithLog(t, testConfigWithID(1, 1), contiguousEntries(10),
			proto.State{Term: 5, Commit: 10},
			proto.Membership{
				Addresses:      map[uint64]string{1: "a", 2: "b", 3: "c"},
				ConfigChangeID: 4,
			})

		err := p.Handle(proto.Message{
			Type: proto.InstallSnapshot, From: 2, To: 1, ShardID: 1, Term: 5,
			Origin: proto.OriginLocal,
			Snapshot: proto.Snapshot{
				Index: 3, Term: 5,
				Membership: proto.Membership{
					Addresses:      map[uint64]string{7: "elsewhere"},
					ConfigChangeID: 99,
				},
			},
		})
		if err != nil {
			t.Fatalf("Handle: %v", err)
		}
		if _, adopted := p.membership.addresses[7]; adopted {
			t.Fatalf("membership adopted from a REJECTED snapshot via the "+
				"ConfigChangeID branch: %v", p.membership.addresses)
		}
		if len(p.membership.addresses) != 3 {
			t.Fatalf("configuration changed: %v", p.membership.addresses)
		}
	})
}

// TestPeer_InstallSnapshot_LeaderDoesNotAdoptUnprocessedMembership verifies a leader, which drops InstallSnapshot, adopts no membership from it.
func TestPeer_InstallSnapshot_LeaderDoesNotAdoptUnprocessedMembership(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	p := newTestPeerWithLog(t, cfg, contiguousEntries(10),
		proto.State{Term: 5, Commit: 10},
		proto.Membership{
			Addresses:      map[uint64]string{1: "a", 2: "b", 3: "c"},
			ConfigChangeID: 4,
		})

	p.r.becomeCandidate()
	p.r.becomeLeader()
	if p.r.state != leader {
		t.Fatalf("fixture: expected leader, got %v", p.r.state)
	}
	committedBefore := p.r.log.committed
	lastBefore := p.r.log.lastIndex()

	// A locally injected InstallSnapshot (Term 0, OriginLocal -- exactly what
	// hostMessageHandler.HandleSnapshot builds after a snapshot stream is
	// received and persisted) carrying a configuration of the sender's choosing.
	err := p.Handle(proto.Message{
		Type: proto.InstallSnapshot, From: 2, To: 1, ShardID: 1,
		Origin: proto.OriginLocal,
		Snapshot: proto.Snapshot{
			Index: committedBefore + 100, Term: 5,
			Membership: proto.Membership{
				Addresses:      map[uint64]string{7: "attacker"},
				ConfigChangeID: 1 << 62,
			},
		},
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// raft did nothing: no restore, no commit advance, no pending snapshot.
	if p.r.log.committed != committedBefore {
		t.Fatalf("committed moved to %d (raft DID process it)", p.r.log.committed)
	}
	if p.r.log.pendingSnapshot() != nil {
		t.Fatal("a snapshot became pending (raft DID process it)")
	}
	if p.r.log.lastIndex() != lastBefore {
		t.Fatalf("log changed (raft DID process it): last=%d", p.r.log.lastIndex())
	}

	if _, adopted := p.membership.addresses[7]; adopted {
		t.Fatalf("LEADER adopted membership %v from an InstallSnapshot that raft "+
			"never processed (stepLeader has no InstallSnapshot case, so step "+
			"returned nil). configChangeID=%d, selfRemoved=%v, remotes=%v -- the "+
			"leader now computes quorum over a configuration no committed entry "+
			"ever established, and it is no longer in it.",
			p.membership.addresses, p.membership.configChangeID,
			p.r.selfRemoved, p.DiagRemotes())
	}
}
