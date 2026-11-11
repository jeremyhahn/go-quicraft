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

// installSnapshotMsg builds an InstallSnapshot message from leader 2 to
// replica 1 carrying the given snapshot at the snapshot's term.
//
// It is stamped proto.OriginLocal because that is the only way an
// InstallSnapshot ever reaches raft: the transport streams a snapshot as
// chunks and the receiving host rebuilds this message itself once the
// artifact is persisted and verified (hostMessageHandler.HandleSnapshot ->
// deliverSnapshotControl -> Engine.DeliverLocal). A test that built it
// without the stamp would be modelling a message the system cannot produce.
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

// TestPeer_InstallSnapshot_PendingSnapshotIsNeverSuperseded verifies that a
// second InstallSnapshot processed in the SAME step-worker batch as a restore
// never leaves the log holding an obsolete pending snapshot.
//
// The step worker drains the whole inbox through Peer.Handle before it calls
// GetUpdate once, so a restore (snapshot A), the leader's follow-up entries,
// and a newer snapshot (snapshot B) can all be stepped before the engine ever
// sees snapshot A. Snapshot B then matches the log at its index, and the
// matching-log branch fast-forwards committed AND processed past snapshot A —
// while inmem still holds snapshot A, because the pending snapshot is cleared
// only by the engine's persistence ack (commitUpdate -> savedSnapshot).
//
// GetUpdate would then ship the superseded snapshot A: LogReader.ApplySnapshot
// rewinds the tracked range behind the apply frontier (or rejects it outright
// with ErrSnapshotOutOfDate, which fails the shard in Peer.Commit), and the
// entries in (A.Index, B.Index] are never emitted for apply because processed
// already moved past them.
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

	// 3. Snapshot B at index 10 now MATCHES the log (index 10 is at term 9),
	// still before the engine has been handed snapshot A.
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

// TestPeer_InstallSnapshot_RejectedSnapshotKeepsMembership verifies that an
// InstallSnapshot rejected by handleSnapshot (its index is at or below the
// committed index) never overwrites the local membership, even when an
// unrelated snapshot is still pending persistence.
//
// Peer.Handle adopts the snapshot's membership unconditionally when the log
// was restored, because a restore fast-forwards the processed marker past the
// config changes the snapshot subsumes. Deriving "restored" from a merely
// non-nil pending snapshot makes a REJECTED retransmission of an older
// snapshot inherit the newer snapshot's restore and roll the membership (and
// the raft remotes derived from it) backwards.
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

	// A retransmission of the leader's PREVIOUS snapshot arrives in the same
	// batch. It is at or below the committed index, so handleSnapshot rejects
	// it and the log keeps everything the newer snapshot installed.
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

// TestPeer_InstallSnapshot_StaleTermDoesNotRewriteMembership pins that a
// stale-term InstallSnapshot does not rewrite the membership.
//
// raft.step DROPS a message whose term is behind the local term and returns
// nil. If Peer.Handle gated membership adoption on `err == nil` alone, a
// stale-term InstallSnapshot — one raft refused to act on — would still
// rewrite the membership: the set quorum() is computed over, replaced with no
// term check, no log proof and no committed entry behind it. The only guard
// left would be the ConfigChangeID carried in the message itself, which the
// sender chooses.
//
// Any authenticated member of the shard could then hand a peer an arbitrary
// configuration — for example one in which it is the sole voter.
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

// The LOCAL snapshot-receive path builds its InstallSnapshot exactly as
// hostMessageHandler.HandleSnapshot does — no term at all — and injects it
// through the local delivery API. That message must still install its
// membership: it is the ONLY thing that can repair a joining node's
// incomplete membership (the bootstrap confState is not in the log).
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

// The same message arriving from the WIRE must be refused outright.
//
// InstallSnapshot never travels as a message between replicas — a snapshot is
// streamed as a SnapshotHeader plus chunks and the RECEIVER rebuilds the
// message locally after persisting and verifying the artifact — so an
// InstallSnapshot in a MessageBatch is always forged. It is also unconstrained
// by anything the protocol checks: it carries a full Membership (the set
// quorum() is computed over) and its own ConfigChangeID, both chosen by the
// sender, with no term, no log match and no committed entry behind them.
//
// The forgery below is the published attack: Term 0 so step's term-transition
// block is skipped entirely, and an Index deliberately BELOW committed so
// handleSnapshot "rejects" it and returns nil — the nil that the previous
// guard read as "raft acted on this".
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

// Joining-node variant. A joining node adopts membership UNCONDITIONALLY
// (its own ConfigChangeID equals the snapshot's, so the strictly-newer test
// cannot protect it), which makes it the softest target for the forgery. Its
// adoption must be gated on the delivery path just the same.
func TestPeer_InstallSnapshot_JoiningNodeRefusesWireDelivery(t *testing.T) {
	cfg := testConfig()
	cfg.PreVote = config.BoolPtr(false)
	// The seeded configuration names only this replica, which is the state in
	// which a node is genuinely still joining: knowing itself ALONGSIDE another
	// member ends the joining window (clearJoiningIfKnown), so seeding the full
	// {1,2,3} configuration here would assert an unreachable state and the
	// joining assertion below would fail for a reason unrelated to the forgery.
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

// TestPeer_InstallSnapshot_RejectedSnapshotDoesNotRedefineMembership pins the
// gate on raft's snapshot REJECTION path. handleSnapshot refuses a snapshot at
// or below the committed index and returns nil, so a nil error from step never
// meant the snapshot was installed. Membership was adopted anyway.
//
// A rejected snapshot's configuration is by definition older than what this
// replica has already committed, so adopting it rolls the voting set backwards.
func TestPeer_InstallSnapshot_RejectedSnapshotDoesNotRedefineMembership(t *testing.T) {
	// The joining path is the sharp one: it deliberately bypasses the
	// ConfigChangeID staleness test, so it adopted EVERY rejected
	// retransmission — including configurations that do not name this replica.
	t.Run("joining replica", func(t *testing.T) {
		p := newTestPeerWithLog(t, testConfigWithID(1, 6), contiguousEntries(10),
			proto.State{Term: 5, Commit: 10},
			proto.Membership{Addresses: map[uint64]string{6: "self"}})
		p.MarkJoining()
		if !p.joining.Load() {
			t.Fatal("fixture: the replica must actually be joining")
		}

		// Index 3 is far below committed=10, so handleSnapshot rejects it.
		// Delivered LOCALLY, so the wire-origin gate is not what stops it.
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

// TestPeer_InstallSnapshot_LeaderDoesNotAdoptUnprocessedMembership covers the
// second way raft.step returns nil without installing a snapshot: stepLeader has
// NO InstallSnapshot case at all, so on a leader the message is silently
// dropped. The wire-origin and stale-term gates both pass for a locally injected
// snapshot above the committed index, so the adopt branch ran on a snapshot raft
// never touched -- letting a leader compute quorum over a configuration no
// committed entry ever established, and one that need not even contain itself.
//
// The gate is a POSITIVE test that raft took the snapshot (the committed index
// advanced), which covers this and the rejection path together.
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
