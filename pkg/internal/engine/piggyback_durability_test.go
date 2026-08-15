package engine

import (
	"errors"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// maxPersistedEntryIndex returns the highest log index this LogDB has ever been
// asked to persist. It is the recording half of the durability assertions
// below: testLogDB.saved accumulates every logdb.Update handed to SaveState, so
// an index above this watermark exists ONLY in the replica's in-memory log and
// would not survive a crash.
//
// Read it as a number at the moment of interest, never later: the Entries
// slices in saved alias commitWorker.flatBuf, which processBatch reuses.
func (t *testLogDB) maxPersistedEntryIndex() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var high uint64
	for i := range t.saved {
		for j := range t.saved[i].Entries {
			if idx := t.saved[i].Entries[j].Index; idx > high {
				high = idx
			}
		}
	}
	return high
}

// newPiggybackFollower builds a real follower Peer in a three-voter shard,
// backed by the caller's recording LogDB so that "what has been persisted" and
// "what the replica advertises" can be compared against the same store.
func newPiggybackFollower(t *testing.T, ldb *testLogDB, shardID, replicaID uint64) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	if err := lr.ApplySnapshot(proto.Snapshot{
		Membership: proto.Membership{Addresses: map[uint64]string{
			1: "localhost:0", 2: "localhost:0", 3: "localhost:0",
		}},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	return peer, lr
}

// newPiggybackCommitWorker builds a commit worker wired to the given LogDB and
// sender, with the retry backoff disabled so failure paths run promptly.
func newPiggybackCommitWorker(
	t *testing.T, ldb *testLogDB, sender *testSender,
) (*commitWorker, chan *applyItem) {
	t.Helper()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() any { return &applyItem{} }}
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool,
		NewWorkSignal(64), nil, stopC, nil)
	cw.noRetryDelay = true
	return cw, applyC
}

// replicateTo steps a Replicate from the leader into the follower, exactly as
// the step worker's inbox processing does.
func replicateTo(t *testing.T, peer *raft.Peer, prevIndex, prevTerm, leaderCommit uint64,
	entries []proto.Entry) {
	t.Helper()
	if err := peer.Handle(proto.Message{
		Type:     proto.Replicate,
		ShardID:  1,
		From:     2,
		To:       1,
		Term:     1,
		LogIndex: prevIndex,
		LogTerm:  prevTerm,
		Commit:   leaderCommit,
		Entries:  entries,
	}); err != nil {
		t.Fatalf("Handle(Replicate) failed: %v", err)
	}
}

// TestPiggyback_WithholdsAckForUnsavedEntries pins the Figure 2 rule that
// persistent state is "updated on stable storage before responding to RPCs" on
// the piggyback fast path.
//
// The mechanism it guards: while commitPending is set, processCommitPendingInbox
// steps inbound Replicate messages (only InstallSnapshot is filtered out), and
// handleAppend answers each one with an ORDERED ReplicateResp naming the new
// last index — while the entries themselves are still only in the in-memory
// log. GetPiggybackUpdate then hands the engine an Update whose SaveState
// writes hard state ONLY. If that Update carries the ReplicateResp, the
// follower has told its leader it holds an index that would vanish in a crash,
// and the leader counts that match toward its commit quorum.
//
// The property asserted is an ordering one, not an implementation detail: no
// ReplicateResp handed to the sender may advertise a log index above the
// highest index the LogDB has actually been asked to persist. "Not persisted"
// is established by the recording testLogDB — it keeps every logdb.Update given
// to SaveState, and entries 4-5 appear in none of them at the moment the
// piggyback releases its messages.
//
// It deliberately drives the REAL producer, Peer.GetPiggybackUpdate, against a
// real r.msgs and a real non-empty entriesToSave(). A test that hand-builds the
// proto.Update cannot observe this class of defect at all.
func TestPiggyback_WithholdsAckForUnsavedEntries(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	cw, applyC := newPiggybackCommitWorker(t, ldb, sender)

	peer, lr := newPiggybackFollower(t, ldb, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, 100, nil, "", nil)

	// Cycle 1: the leader replicates entries 1-3 and commits nothing yet.
	// Running them through the real processBatch is what makes indices 1-3
	// durable: it converts the Update, calls SaveState with the entries, and
	// only then calls Peer.Commit, which advances the peer's saved marker.
	replicateTo(t, peer, 0, 0, 0, []proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	})
	saveCycle, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate (cycle 1): %v", err)
	}
	if len(saveCycle.EntriesToSave) != 3 {
		t.Fatalf("fixture: expected 3 entries to save, got %d", len(saveCycle.EntriesToSave))
	}
	node.commitPending.Store(true)
	cw.batch = append(cw.batch[:0], saveCycle)
	cw.batchNodes = []*Node{node}
	cw.processBatch()

	durable := ldb.maxPersistedEntryIndex()
	if durable != 3 {
		t.Fatalf("fixture: LogDB holds entries through %d, want 3; the "+
			"comparison below would prove nothing", durable)
	}

	// Cycle 2, inside the commitPending window: the leader commits 3 and
	// replicates 4-5 in the same message. handleAppend appends 4-5 to the
	// IN-MEMORY log and queues ReplicateResp{LogIndex: 5}.
	node.commitPending.Store(true)
	replicateTo(t, peer, 3, 1, 3, []proto.Entry{
		{Index: 4, Term: 1, Cmd: []byte("d")},
		{Index: 5, Term: 1, Cmd: []byte("e")},
	})

	piggyback, err := peer.GetPiggybackUpdate(node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetPiggybackUpdate: %v", err)
	}
	if len(piggyback.CommittedEntries) == 0 {
		t.Fatal("fixture: the piggyback carried no committed entries, so " +
			"executePiggyback would not run the path under test")
	}

	before := sender.sentCount()
	if !cw.executePiggyback(node, piggyback) {
		t.Fatal("executePiggyback reported shutdown")
	}
	select {
	case <-applyC:
	default:
		t.Fatal("fixture: the piggyback never reached the apply worker")
	}

	// The state-only SaveState of the piggyback must not have made 4-5 durable.
	if got := ldb.maxPersistedEntryIndex(); got != durable {
		t.Fatalf("piggyback persisted entries through %d (was %d): the state-only "+
			"SaveState is no longer state-only, so this test no longer covers "+
			"the ordering rule it was written for", got, durable)
	}

	for _, msg := range sender.sentMessages()[before:] {
		if msg.Type != proto.ReplicateResp || msg.Reject {
			continue
		}
		if msg.LogIndex > durable {
			t.Fatalf("follower acknowledged log index %d to replica %d while the "+
				"LogDB holds only through %d: entries (%d, %d] exist solely in "+
				"RAM, yet the leader will count this match toward its commit "+
				"quorum and may report those entries committed to a client "+
				"(Raft Figure 2: persistent state is updated on stable storage "+
				"before responding to RPCs)",
				msg.LogIndex, msg.To, durable, durable, msg.LogIndex)
		}
	}

	// Withheld, never dropped. The acknowledgement must still be delivered —
	// by the next cycle, which is the one that actually persists 4-5. Asserting
	// both facts on the SAME update is the point: the ordered response and the
	// entries it names travel together through SaveState.
	if !peer.HasUpdate(node.lastApplied.Load()) {
		t.Fatal("the withheld ReplicateResp was dropped: the peer reports no " +
			"further work, so the leader never learns this follower holds 4-5")
	}
	next, err := peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate (release cycle): %v", err)
	}
	if len(next.EntriesToSave) == 0 ||
		next.EntriesToSave[len(next.EntriesToSave)-1].Index != 5 {
		t.Fatalf("release cycle carried EntriesToSave=%v, want it to persist "+
			"through index 5", next.EntriesToSave)
	}
	var released bool
	for _, msg := range next.Messages {
		if msg.Type == proto.ReplicateResp && !msg.Reject && msg.LogIndex == 5 {
			released = true
		}
	}
	if !released {
		t.Fatalf("the ReplicateResp for index 5 never reappeared; messages=%v",
			next.Messages)
	}
}

// TestPiggyback_FailedStatePersistKeepsStateUnacknowledged pins the two
// obligations of the piggyback SaveState failure path.
//
// The path is documented as SELF-HEALING, and that is true of the entries: the
// processed marker has not moved, so they are re-offered. It was NOT true of
// the hard state. GetPiggybackUpdate used to advance prevState before the
// engine attempted the persist, so a failed SaveState left the peer believing a
// term/vote it had never written was durable. hasStateChanged() then reported
// no change, a later update could be ReadOnly, and the step worker's ReadOnly
// bypass transmits messages with no LogDB write at all — a granted vote leaving
// the replica with nothing on disk to stop it voting again after a crash.
// The same failure path also dropped the ordered messages it had drained.
//
// Both are asserted through the real producer: a real vote is granted into a
// real r.msgs, and the real GetPiggybackUpdate drains it.
func TestPiggyback_FailedStatePersistKeepsStateUnacknowledged(t *testing.T) {
	ldb := newTestLogDB()
	sender := newTestSender()
	cw, _ := newPiggybackCommitWorker(t, ldb, sender)

	peer, lr := newPiggybackFollower(t, ldb, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, 100, nil, "", nil)

	// Make entries 1-3 durable, leaving entriesToSave() empty. That matters:
	// with unsaved entries present the ordered messages would be withheld by
	// the durability split and this path would never drain them.
	replicateTo(t, peer, 0, 0, 0, []proto.Entry{
		{Index: 1, Term: 1, Cmd: []byte("a")},
		{Index: 2, Term: 1, Cmd: []byte("b")},
		{Index: 3, Term: 1, Cmd: []byte("c")},
	})
	saveCycle, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate (cycle 1): %v", err)
	}
	node.commitPending.Store(true)
	cw.batch = append(cw.batch[:0], saveCycle)
	cw.batchNodes = []*Node{node}
	cw.processBatch()

	// Commit 3 without shipping new entries, so there is something to
	// piggyback and nothing unsaved.
	node.commitPending.Store(true)
	replicateTo(t, peer, 3, 1, 3, nil)

	// Grant a real vote to replica 3 at term 2. Hint == From marks it a
	// leadership-transfer vote, which is exempt from the known-leader lease —
	// without that the follower would refuse, having just heard from replica 2.
	if err := peer.Handle(proto.Message{
		Type:     proto.RequestVote,
		ShardID:  1,
		From:     3,
		To:       1,
		Term:     2,
		LogIndex: 3,
		LogTerm:  1,
		Hint:     3,
	}); err != nil {
		t.Fatalf("Handle(RequestVote): %v", err)
	}

	piggyback, err := peer.GetPiggybackUpdate(node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetPiggybackUpdate: %v", err)
	}
	if len(piggyback.CommittedEntries) == 0 {
		t.Fatal("fixture: the piggyback carried no committed entries, so " +
			"executePiggyback would not run the path under test")
	}
	var drainedVoteResp bool
	for _, msg := range piggyback.Messages {
		if msg.Type == proto.RequestVoteResp && !msg.Reject {
			drainedVoteResp = true
		}
	}
	if !drainedVoteResp {
		t.Fatalf("fixture: no granted RequestVoteResp was drained into the "+
			"piggyback, so the drop below would prove nothing; messages=%v",
			piggyback.Messages)
	}
	if piggyback.State.Vote != 3 || piggyback.State.Term != 2 {
		t.Fatalf("fixture: piggyback hard state is %+v, want term 2 vote 3",
			piggyback.State)
	}

	errSave := errors.New("piggyback state persist failed")
	ldb.setSaveError(errSave)

	if !cw.executePiggyback(node, piggyback) {
		t.Fatal("executePiggyback reported shutdown")
	}

	// Obligation 1: the ordered messages come back. Dropping a ReplicateResp
	// costs a retransmit; dropping this VoteResp stalls the candidate for a
	// full election timeout.
	//
	// Obligation 2: ReadOnly must be false. It is the observable form of "the
	// peer still considers this term/vote unpersisted" — the step worker's
	// ReadOnly bypass sends an update's messages WITHOUT routing it through the
	// commit pipeline, so a ReadOnly update carrying a vote response is a vote
	// leaving this replica with no WAL write behind it. moreEntriesToApply is
	// false here for the same reason the step worker passes false during a
	// snapshot: it isolates the hard state as the only thing that could mark
	// the update non-ReadOnly.
	after, err := peer.GetUpdate(false, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate (after failed persist): %v", err)
	}
	var requeuedVoteResp bool
	for _, msg := range after.Messages {
		if msg.Type == proto.RequestVoteResp && !msg.Reject {
			requeuedVoteResp = true
		}
	}
	if !requeuedVoteResp {
		t.Fatalf("the granted RequestVoteResp was dropped by the failed "+
			"piggyback persist; candidate 3 waits out a full election timeout "+
			"for a vote this replica already cast. messages=%v", after.Messages)
	}
	if after.ReadOnly {
		t.Fatalf("update is ReadOnly after a FAILED piggyback SaveState: the "+
			"peer believes term %d vote %d is durable when no LogDB write "+
			"succeeded. The step worker's ReadOnly bypass will now transmit "+
			"this RequestVoteResp with no WAL write at all, so a crash lets "+
			"this replica vote again in term %d — Raft Figure 2 forbids "+
			"responding before the vote is on stable storage",
			after.State.Term, after.State.Vote, after.State.Term)
	}
	if after.State.Vote != 3 || after.State.Term != 2 {
		t.Fatalf("the update that must re-persist the vote carries %+v, "+
			"want term 2 vote 3", after.State)
	}
}
