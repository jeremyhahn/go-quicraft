package engine

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestStepWorker_Tick_StepsInboxBeforeCheckQuorum verifies a tick steps queued replies before evaluating CheckQuorum.
func TestStepWorker_Tick_StepsInboxBeforeCheckQuorum(t *testing.T) {
	for _, tc := range []struct {
		name          string
		commitPending bool
	}{
		{name: "idle", commitPending: false},
		{name: "commitPending", commitPending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commitC := make(chan *commitItem, 64)
			stopC := make(chan struct{})
			defer close(stopC)

			sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), commitC, newTestCallback(), stopC, nil)
			peer, lr := newTestVoterPeer(t, 1, 1, 3)
			cfg := config.Config{ShardID: 1, ReplicaID: 1}
			cfg.SetDefaults()
			node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
			sw.loadNode(node)

			driveToLeader(t, peer, 1, 2)
			if tc.commitPending {
				// Hand the leader's first update to the (absent) commit
				// worker: commitPending stays set for the rest of the test.
				sw.processReady()
				drainCommitC(commitC)
				if !node.commitPending.Load() {
					t.Fatal("commitPending not set; test setup is wrong")
				}
			}
			term := peer.Term()

			// Several CheckQuorum windows (ElectionRTT ticks each). Every
			// tick finds a fresh HeartbeatResp from both followers already
			// in the inbox.
			for i := range 4 * int(cfg.ElectionRTT) {
				for _, from := range []uint64{2, 3} {
					node.Deliver(proto.Message{
						Type:    proto.HeartbeatResp,
						ShardID: 1,
						From:    from,
						To:      1,
						Term:    term,
					})
				}
				sw.tick()
				if peer.LeaderID() != 1 {
					t.Fatalf("leader stepped down under CheckQuorum at tick %d "+
						"with both followers' HeartbeatResp in its inbox", i+1)
				}
			}
		})
	}
}

// BenchmarkStepWorker_TickCycle measures one ticker wake on an idle leader:
// tick() (which now steps the inbox first) followed by processReady().
func BenchmarkStepWorker_TickCycle(b *testing.B) {
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1), commitC, newTestCallback(), stopC, nil)
	peer, lr := newTestVoterPeer(b, 1, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)
	for range 200 {
		if peer.LeaderID() == 1 {
			break
		}
		peer.Tick()
	}
	if peer.LeaderID() != 1 {
		b.Fatal("single-voter peer did not become leader")
	}
	sw.processReady()
	drainCommitC(commitC)

	b.ReportAllocs()
	for b.Loop() {
		sw.tick()
		sw.processReady()
		drainCommitC(commitC)
	}
}
