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
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Leader-executed requests, driven deterministically.
//
// Three bare hosts (replicas 1, 2, 3 of shard 1) share an in-memory network:
// every message a host sends lands in one queue, and the TEST decides when,
// whether and in what order each is delivered to its target's receive path.
// The leadership view of each replica is set by hand through the same
// updateNodeLeader/markLeaderReady calls the engine's callbacks make, so a
// leader change lands at an exact point of a request's life. Handlers record
// every execution by (term, replica): "never twice under one term" is checked
// against that record, not inferred.
// ---------------------------------------------------------------------------

const execShard uint64 = 1

// execSender is the hosts' engine.MessageSender: it queues instead of sending.
type execSender struct {
	noopSender
	msgs chan proto.Message
}

func (s *execSender) Send(msgs []proto.Message) {
	for _, m := range msgs {
		m.Entries = slices.Clone(m.Entries)
		s.msgs <- m
	}
}

type execCluster struct {
	t       *testing.T
	msgs    chan proto.Message
	hosts   map[uint64]*Host
	nodes   map[uint64]*nodeState
	started chan uint64

	mu   sync.Mutex
	runs map[uint64][]uint64 // term -> replicas that executed under it
}

type execResult struct {
	reply []byte
	err   error
}

func newExecCluster(t *testing.T) *execCluster {
	t.Helper()
	c := &execCluster{
		t:       t,
		msgs:    make(chan proto.Message, 64),
		hosts:   make(map[uint64]*Host),
		nodes:   make(map[uint64]*nodeState),
		started: make(chan uint64, 64),
		runs:    make(map[uint64][]uint64),
	}
	for id := uint64(1); id <= 3; id++ {
		h := &Host{sender: &execSender{msgs: c.msgs}, leaderExec: newLeaderExecRouter()}
		ns := &nodeState{shardID: execShard, replicaID: id}
		ns.cfg.MaxEntrySize = 1024
		h.nodes.Store(execShard, ns)
		// An engine that is never started, holding the replica's node: the
		// failure latch and the Raft leadership state are the engine node's,
		// and the host's engine callback is what OnShardFailed reaches.
		hcfg := config.HostConfig{
			WALDir:       t.TempDir(),
			NodeHostDir:  t.TempDir(),
			RaftAddress:  "127.0.0.1:5000",
			DeploymentID: 1,
		}
		hcfg.SetDefaults()
		h.engine = engine.NewEngine(hcfg, nil, &noopSender{}, nil, &hostEngineCallback{h: h}, nil)
		h.engine.LoadNode(newExecNode(t, id))
		c.hosts[id], c.nodes[id] = h, ns
	}
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.leaderExec.close()
			h.engine.Stop()
		}
	})
	return c
}

// execVoters is the harness shard's voting membership.
var execVoters = map[uint64]string{1: "127.0.0.1:5001", 2: "127.0.0.1:5002", 3: "127.0.0.1:5003"}

// newExecNode is replica id's engine node, with a real Raft peer of the three
// voters. Pre-vote is off so execCluster.raft can elect it by stepping votes
// straight in.
func newExecNode(t *testing.T, id uint64) *engine.Node {
	t.Helper()
	lr := logdb.NewLogReader(execShard, id, memdb.New())
	ms := proto.Membership{Addresses: execVoters, Identities: fixtureIdentities(execVoters)}
	if err := lr.ApplySnapshot(proto.Snapshot{Membership: ms}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	cfg := config.Config{ShardID: execShard, ReplicaID: id, PreVote: config.BoolPtr(false)}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	return engine.NewNode(peer, nil, lr, cfg, nil, t.TempDir(), nil)
}

// serve registers, on every host, a handler that records the execution and
// answers "<replica>@<term>:<request>". A host with a gate holds its handler
// until the gate closes (or its context ends).
func (c *execCluster) serve(gates map[uint64]chan struct{}) {
	for id, h := range c.hosts {
		gate := gates[id]
		err := h.RegisterLeaderHandler(execShard, func(ctx context.Context, term uint64, req []byte) ([]byte, error) {
			c.mu.Lock()
			c.runs[term] = append(c.runs[term], id)
			c.mu.Unlock()
			c.started <- id
			if gate != nil {
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return fmt.Appendf(nil, "%d@%d:%s", id, term, req), nil
		})
		if err != nil {
			c.t.Fatalf("RegisterLeaderHandler on %d: %v", id, err)
		}
	}
}

// view sets the leadership view replica id holds, as OnLeaderUpdated does.
func (c *execCluster) view(id, leader, term uint64) {
	c.raft(id, leader, term)
	c.hosts[id].updateNodeLeader(execShard, leader, term)
}

// raft moves replica id's Raft to (leader, term), ahead of its view, by
// stepping in what it would hear: the leader's heartbeat on a follower; on the
// leader, a heartbeat of the term before, an election timeout and the other
// voters' votes. Raft already past term is left where it is.
func (c *execCluster) raft(id, leader, term uint64) {
	c.t.Helper()
	p := c.hosts[id].engine.GetNode(execShard).Peer()
	if gotLeader, gotTerm := p.LeaderAndTerm(); leader == 0 || gotTerm > term || (gotLeader == leader && gotTerm == term) {
		return
	}
	step := func(m proto.Message) {
		c.t.Helper()
		m.ShardID, m.To = execShard, id
		if err := p.Handle(m); err != nil {
			c.t.Fatalf("replica %d Raft: step %s: %v", id, proto.MessageTypeName(m.Type), err)
		}
	}
	if leader != id {
		step(proto.Message{Type: proto.Heartbeat, From: leader, Term: term})
	} else {
		others := make([]uint64, 0, len(execVoters)-1)
		for v := range execVoters {
			if v != id {
				others = append(others, v)
			}
		}
		if p.Term() < term-1 {
			step(proto.Message{Type: proto.Heartbeat, From: others[0], Term: term - 1})
		}
		for i := 0; i < 100 && p.Term() < term; i++ {
			p.Tick()
		}
		for _, v := range others {
			step(proto.Message{Type: proto.RequestVoteResp, From: v, Term: term})
		}
	}
	if gotLeader, gotTerm := p.LeaderAndTerm(); gotLeader != leader || gotTerm != term {
		c.t.Fatalf("replica %d Raft is at %d@%d, want %d@%d", id, gotLeader, gotTerm, leader, term)
	}
}

// ready marks replica id's current term read-ready, as OnLeaderReady does.
func (c *execCluster) ready(id uint64) {
	ns := c.nodes[id]
	leader, term, _ := ns.leaderAndTerm()
	if !ns.markLeaderReady(leader, term) {
		c.t.Fatalf("markLeaderReady(%d, %d) on %d refused", leader, term, id)
	}
}

// elect has every listed replica see leader at term, and the leader ready.
func (c *execCluster) elect(leader, term uint64, ids ...uint64) {
	for _, id := range ids {
		c.view(id, leader, term)
		if id == leader {
			c.ready(id)
		}
	}
}

// quietView changes replica id's view WITHOUT waking its waiters: the view
// moved, and its notification has not been observed yet.
func (c *execCluster) quietView(id, leader, term uint64) {
	c.raft(id, leader, term)
	ns := c.nodes[id]
	ns.leaderSeq.Add(1)
	ns.leaderID.Store(leader)
	ns.term.Store(term)
	ns.isLeader.Store(leader == ns.replicaID)
	ns.readReady.Store(false)
	ns.leaderSeq.Add(1)
}

// fail trips replica id's engine failure latch, as an apply or persistence
// failure does. Its leadership view is left exactly as it was.
func (c *execCluster) fail(id uint64) {
	eng := c.hosts[id].engine
	eng.FailShard(eng.GetNode(execShard), errors.New("apply failed"))
}

func (c *execCluster) execute(ctx context.Context, id uint64, req string) <-chan execResult {
	out := make(chan execResult, 1)
	go func() {
		reply, err := c.hosts[id].ExecuteOnLeader(ctx, execShard, []byte(req))
		out <- execResult{reply, err}
	}()
	return out
}

func (c *execCluster) result(ch <-chan execResult) execResult {
	c.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		c.t.Fatal("ExecuteOnLeader did not return")
		return execResult{}
	}
}

func (c *execCluster) recv() proto.Message {
	c.t.Helper()
	select {
	case m := <-c.msgs:
		return m
	case <-time.After(10 * time.Second):
		c.t.Fatal("no message was sent")
		return proto.Message{}
	}
}

// expect receives the next message and checks its routing fields.
func (c *execCluster) expect(msgType, from, to, term uint64) proto.Message {
	c.t.Helper()
	m := c.recv()
	if m.Type != msgType || m.From != from || m.To != to || m.Term != term || m.ShardID != execShard {
		c.t.Fatalf("sent %s %d->%d term %d shard %d, want %s %d->%d term %d shard %d",
			proto.MessageTypeName(m.Type), m.From, m.To, m.Term, m.ShardID,
			proto.MessageTypeName(msgType), from, to, term, execShard)
	}
	return m
}

func (c *execCluster) deliver(m proto.Message) {
	leaderExecReceivers[m.Type](c.hosts[m.To], &m)
}

func (c *execCluster) quiet() {
	c.t.Helper()
	select {
	case m := <-c.msgs:
		c.t.Fatalf("unexpected %s %d->%d term %d", proto.MessageTypeName(m.Type), m.From, m.To, m.Term)
	default:
	}
}

func (c *execCluster) awaitStarted(id uint64) {
	c.t.Helper()
	select {
	case got := <-c.started:
		if got != id {
			c.t.Fatalf("handler started on replica %d, want %d", got, id)
		}
	case <-time.After(10 * time.Second):
		c.t.Fatalf("handler on replica %d never started", id)
	}
}

// wantRuns checks the complete execution record.
func (c *execCluster) wantRuns(want map[uint64][]uint64) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !maps.EqualFunc(c.runs, want, slices.Equal[[]uint64]) {
		c.t.Fatalf("executions by term = %v, want %v", c.runs, want)
	}
}

func wantReply(t *testing.T, r execResult, want string) {
	t.Helper()
	if r.err != nil || string(r.reply) != want {
		t.Fatalf("ExecuteOnLeader = (%q, %v), want (%q, nil)", r.reply, r.err, want)
	}
}

// TestExecuteOnLeader_FollowerRequestIsExecutedByTheLeader is the base path:
// a request made on a follower is forwarded to the leader, executed there
// under the leader's term, and its reply returned to the follower's caller.
func TestExecuteOnLeader_FollowerRequestIsExecutedByTheLeader(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := c.execute(ctx, 2, "renew")

	req := c.expect(proto.LeaderExec, 2, 1, 5)
	if req.Hint == 0 || req.LogIndex == 0 || req.LogIndex > uint64(time.Minute) {
		t.Fatalf("LeaderExec Hint %d budget %d: want a request ID and the caller's remaining budget",
			req.Hint, req.LogIndex)
	}
	if len(req.Entries) != 1 || string(req.Entries[0].Cmd) != "renew" {
		t.Fatalf("LeaderExec entries = %v, want the request", req.Entries)
	}
	c.deliver(req)

	resp := c.expect(proto.LeaderExecResp, 1, 2, 5)
	if resp.Hint != req.Hint || resp.Reject {
		t.Fatalf("LeaderExecResp Hint %d Reject %v, want Hint %d and an accepted reply",
			resp.Hint, resp.Reject, req.Hint)
	}
	c.deliver(resp)

	wantReply(t, c.result(res), "1@5:renew")
	c.wantRuns(map[uint64][]uint64{5: {1}})
	c.quiet()
}

// TestExecuteOnLeader_OnTheLeaderRunsInProcess: no message crosses the
// network when the caller's own replica leads.
func TestExecuteOnLeader_OnTheLeaderRunsInProcess(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)

	wantReply(t, c.result(c.execute(context.Background(), 1, "local")), "1@5:local")
	c.wantRuns(map[uint64][]uint64{5: {1}})
	c.quiet()
}

// TestExecuteOnLeader_WaitsForTheLeaderToBeReady: a leader that has not yet
// committed an entry of its term does not execute; it waits for that, then
// executes under the term.
func TestExecuteOnLeader_WaitsForTheLeaderToBeReady(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	for id := uint64(1); id <= 3; id++ {
		c.view(id, 1, 5)
	}

	res := c.execute(context.Background(), 2, "early")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.wantRuns(map[uint64][]uint64{})

	c.ready(1)
	c.deliver(c.expect(proto.LeaderExecResp, 1, 2, 5))
	wantReply(t, c.result(res), "1@5:early")
	c.wantRuns(map[uint64][]uint64{5: {1}})
}

// TestExecuteOnLeader_UnreadyLeaderDeposedNeverExecutes: a leader deposed
// while it waits to become ready refuses the request without running it, and
// the caller takes it to the new leader.
func TestExecuteOnLeader_UnreadyLeaderDeposedNeverExecutes(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	for id := uint64(1); id <= 3; id++ {
		c.view(id, 1, 5)
	}

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.view(1, 3, 6)

	refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
	if !refusal.Reject || refusal.LogIndex != leaderExecNotLeader {
		t.Fatalf("deposed unready leader answered Reject %v status %d, want status %d",
			refusal.Reject, refusal.LogIndex, leaderExecNotLeader)
	}
	c.deliver(refusal)

	c.elect(3, 6, 2, 3)
	c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
	c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
	wantReply(t, c.result(res), "3@6:req")
	c.wantRuns(map[uint64][]uint64{6: {3}})
}

// TestExecuteOnLeader_DeposedLeaderReplyIsRejectedAndRetried: the leader
// learns of a later term while its handler runs. Its reply is withheld, the
// caller spends the term, and the request runs once on the new leader under
// the new term. The caller's own view still names the old leader when the
// refusal arrives, and it does not offer term 5 again.
func TestExecuteOnLeader_DeposedLeaderReplyIsRejectedAndRetried(t *testing.T) {
	c := newExecCluster(t)
	gate1 := make(chan struct{})
	c.serve(map[uint64]chan struct{}{1: gate1})
	c.elect(1, 5, 1, 2, 3)

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.awaitStarted(1)

	c.view(1, 3, 6)
	close(gate1)
	refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
	if !refusal.Reject || refusal.LogIndex != leaderExecDeposed {
		t.Fatalf("deposed leader answered Reject %v status %d, want status %d",
			refusal.Reject, refusal.LogIndex, leaderExecDeposed)
	}
	c.deliver(refusal)
	c.quiet()

	c.elect(3, 6, 2, 3)
	c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
	c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
	wantReply(t, c.result(res), "3@6:req")
	c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
}

// TestExecuteOnLeader_LeaderChangeMidRequestIsRetriedOncePerTerm: the
// caller's view moves to a new leader while the old one (which has not yet
// heard) is still executing. The caller abandons the attempt without
// resending it, retries on the new leader, and when the stale leader's
// reply finally lands it is dropped. Each term executed the request once.
func TestExecuteOnLeader_LeaderChangeMidRequestIsRetriedOncePerTerm(t *testing.T) {
	c := newExecCluster(t)
	gate1 := make(chan struct{})
	c.serve(map[uint64]chan struct{}{1: gate1})
	c.elect(1, 5, 1, 2, 3)

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.awaitStarted(1)

	c.elect(3, 6, 2, 3)
	c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
	c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
	wantReply(t, c.result(res), "3@6:req")

	close(gate1)
	late := c.expect(proto.LeaderExecResp, 1, 2, 5)
	if late.Reject {
		t.Fatalf("stale leader refused (status %d); it still believes it leads term 5", late.LogIndex)
	}
	c.deliver(late)
	c.quiet()
	c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
}

// TestExecuteOnLeader_ReplyFromAnEarlierTermIsRejected: the leader's reply
// arrives after the caller's view has already moved past its term (the view
// change's wakeup not yet observed). The reply is from a deposed leader, so
// it is discarded and the request retried under the new term.
func TestExecuteOnLeader_ReplyFromAnEarlierTermIsRejected(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	reply := c.expect(proto.LeaderExecResp, 1, 2, 5)

	c.elect(3, 6, 3)
	c.quietView(2, 3, 6)
	c.deliver(reply)

	c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
	c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
	wantReply(t, c.result(res), "3@6:req")
	c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
}

// TestExecuteOnLeader_RefusalFromAnEarlierTermIsRetried: as above, for every
// refusal. A handler failure, a missing handler, a busy leader or a "not
// leader" from a term the caller's view has already moved past is the answer
// of a deposed leader. It is discarded, not returned, and the request is
// retried under the new term.
func TestExecuteOnLeader_RefusalFromAnEarlierTermIsRetried(t *testing.T) {
	for _, status := range []uint64{leaderExecNotLeader, leaderExecNoHandler, leaderExecHandlerFailed, leaderExecBusy} {
		t.Run(fmt.Sprintf("status%d", status), func(t *testing.T) {
			c := newExecCluster(t)
			c.serve(nil)
			c.elect(1, 5, 1, 2, 3)

			res := c.execute(context.Background(), 2, "req")
			req := c.expect(proto.LeaderExec, 2, 1, 5)

			c.elect(3, 6, 3)
			c.quietView(2, 3, 6)
			c.deliver(proto.Message{
				Type: proto.LeaderExecResp, ShardID: execShard, From: 1, To: 2, Term: 5,
				Hint: req.Hint, Reject: true, LogIndex: status,
				Entries: []proto.Entry{{Cmd: []byte("stale")}},
			})

			c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
			c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
			wantReply(t, c.result(res), "3@6:req")
			c.wantRuns(map[uint64][]uint64{6: {3}})
		})
	}
}

// TestExecuteOnLeader_NoLeader: with no leader known, the request waits for
// one and reports ErrNoLeader when its context ends first.
func TestExecuteOnLeader_NoLeader(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := c.result(c.execute(ctx, 2, "req")).err

	var unavailable *LeaderUnavailableError
	if !errors.As(err, &unavailable) || unavailable.ShardID != execShard {
		t.Fatalf("err = %v, want *LeaderUnavailableError for shard %d", err, execShard)
	}
	if !errors.Is(err, ErrNoLeader) || !errors.Is(err, ErrTimeout) || errors.Is(err, ErrLeadershipChanged) {
		t.Fatalf("err = %v, want ErrNoLeader and ErrTimeout", err)
	}
	if !Temporary(err) {
		t.Fatalf("Temporary(%v) = false, want true", err)
	}
	c.quiet()
	c.wantRuns(map[uint64][]uint64{})
}

// TestExecuteOnLeader_LeadershipChanged: the replica the caller knows as
// leader refuses (it has moved on), and the caller's view does not settle
// before its context ends.
func TestExecuteOnLeader_LeadershipChanged(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.view(2, 1, 5)
	c.view(1, 3, 6)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res := c.execute(ctx, 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.deliver(c.expect(proto.LeaderExecResp, 1, 2, 5))

	err := c.result(res).err
	var unavailable *LeaderUnavailableError
	if !errors.As(err, &unavailable) || !errors.Is(err, ErrLeadershipChanged) || !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want *LeaderUnavailableError with ErrLeadershipChanged and ErrTimeout", err)
	}
	c.wantRuns(map[uint64][]uint64{})
}

// TestExecuteOnLeader_DeadlineWithAnAttemptInFlight: a caller whose context
// ends while the leader it still sees has not answered gets the context's
// error alone; leadership did not change.
func TestExecuteOnLeader_DeadlineWithAnAttemptInFlight(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	res := c.execute(ctx, 2, "req")
	c.expect(proto.LeaderExec, 2, 1, 5) // lost

	err := c.result(res).err
	var unavailable *LeaderUnavailableError
	if !errors.Is(err, ErrTimeout) || errors.As(err, &unavailable) {
		t.Fatalf("err = %v, want ErrTimeout alone", err)
	}
}

// TestExecuteOnLeader_HandlerNotRegistered covers both the local and the
// forwarded path, and that the error is permanent.
func TestExecuteOnLeader_HandlerNotRegistered(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.hosts[1].UnregisterLeaderHandler(execShard)
	c.elect(1, 5, 1, 2, 3)

	if _, err := c.hosts[1].ExecuteOnLeader(context.Background(), execShard, []byte("x")); !errors.Is(err, ErrLeaderHandlerNotRegistered) {
		t.Fatalf("local: err = %v, want ErrLeaderHandlerNotRegistered", err)
	}

	res := c.execute(context.Background(), 2, "x")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.deliver(c.expect(proto.LeaderExecResp, 1, 2, 5))
	err := c.result(res).err
	if !errors.Is(err, ErrLeaderHandlerNotRegistered) || Temporary(err) {
		t.Fatalf("remote: err = %v, want permanent ErrLeaderHandlerNotRegistered", err)
	}
}

// TestExecuteOnLeader_HandlerNotRegisteredOffTheTermIsNotLeader: a replica
// without a handler that does not lead the shard in the request's term says
// so. Only the leader of that term may report the missing handler, which is
// permanent; anything else is retried on the leader the view moves to.
func TestExecuteOnLeader_HandlerNotRegisteredOffTheTermIsNotLeader(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.hosts[1].UnregisterLeaderHandler(execShard)
	c.view(2, 1, 5)
	c.view(1, 3, 6)

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
	if !refusal.Reject || refusal.LogIndex != leaderExecNotLeader {
		t.Fatalf("replica off the term answered Reject %v status %d, want status %d",
			refusal.Reject, refusal.LogIndex, leaderExecNotLeader)
	}
	c.deliver(refusal)

	c.elect(3, 6, 2, 3)
	c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
	c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
	wantReply(t, c.result(res), "3@6:req")
	c.wantRuns(map[uint64][]uint64{6: {3}})
}

// TestRegisterLeaderHandler_Validation: a nil handler, and a closed host.
func TestRegisterLeaderHandler_Validation(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[1]
	if err := h.RegisterLeaderHandler(execShard, nil); !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("nil handler: err = %v, want ErrInvalidOperation", err)
	}
	h.closed.Store(true)
	ok := func(context.Context, uint64, []byte) ([]byte, error) { return nil, nil }
	if err := h.RegisterLeaderHandler(execShard, ok); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed host: err = %v, want ErrClosed", err)
	}
}

// TestExecuteOnLeader_HandlerFailures: an error, a panic, an over-long error
// text and an oversized reply all reach the caller as *LeaderHandlerError
// naming the leader and term that executed the request.
func TestExecuteOnLeader_HandlerFailures(t *testing.T) {
	long := strings.Repeat("x", 4*leaderExecMaxFailureText)
	for _, tc := range []struct {
		name    string
		handler LeaderHandler
		check   func(string) bool
	}{
		{"error", func(context.Context, uint64, []byte) ([]byte, error) {
			return nil, errors.New("lease held by 7")
		}, func(m string) bool { return m == "lease held by 7" }},
		{"panic", func(context.Context, uint64, []byte) ([]byte, error) {
			panic("boom")
		}, func(m string) bool { return m == "panic: boom" }},
		{"long error", func(context.Context, uint64, []byte) ([]byte, error) {
			return nil, errors.New(long)
		}, func(m string) bool { return m == long[:leaderExecMaxFailureText] }},
		{"oversized reply", func(context.Context, uint64, []byte) ([]byte, error) {
			return make([]byte, 2048), nil
		}, func(m string) bool { return m == (&EntryTooLargeError{Size: 2048, MaxSize: 1024}).Error() }},
	} {
		for _, caller := range []uint64{1, 2} {
			t.Run(fmt.Sprintf("%s/caller%d", tc.name, caller), func(t *testing.T) {
				c := newExecCluster(t)
				if err := c.hosts[1].RegisterLeaderHandler(execShard, tc.handler); err != nil {
					t.Fatal(err)
				}
				c.elect(1, 5, 1, 2, 3)
				res := c.execute(context.Background(), caller, "req")
				if caller != 1 {
					c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
					c.deliver(c.expect(proto.LeaderExecResp, 1, 2, 5))
				}
				err := c.result(res).err
				var failed *LeaderHandlerError
				if !errors.As(err, &failed) || failed.ShardID != execShard ||
					failed.LeaderID != 1 || failed.Term != 5 || !tc.check(failed.Message) {
					t.Fatalf("err = %#v, want *LeaderHandlerError from replica 1 term 5", err)
				}
			})
		}
	}
}

// TestExecuteOnLeader_BusyLeaderRefuses: a leader already executing
// leaderExecMaxInflight remote requests refuses the next without running it.
func TestExecuteOnLeader_BusyLeaderRefuses(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)
	r := c.hosts[1].leaderExec
	for range leaderExecMaxInflight {
		if !r.acquire() {
			t.Fatal("acquire failed below the limit")
		}
	}
	defer func() {
		for range leaderExecMaxInflight {
			r.release()
		}
	}()

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.deliver(c.expect(proto.LeaderExecResp, 1, 2, 5))
	if err := c.result(res).err; !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	c.wantRuns(map[uint64][]uint64{})
}

// TestExecuteOnLeader_PausedOrSealedLeaderRefuses: a leader paused without
// handing leadership over still leads in its frozen Raft, so the remote path
// must apply the same gates as the local preflight. It answers before running
// the handler, and the caller gets the error its own replica would give.
func TestExecuteOnLeader_PausedOrSealedLeaderRefuses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status uint64
		want   error
		gate   func(*nodeState)
	}{
		{"paused", leaderExecPaused, ErrShardPaused, func(ns *nodeState) { ns.paused.Store(true) }},
		{"sealed", leaderExecSealed, ErrBarrierSealed, func(ns *nodeState) {
			b := newTestHostBarrier(1)
			b.sealedVal = true
			ns.barrier = b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newExecCluster(t)
			c.serve(nil)
			c.elect(1, 5, 1, 2, 3)
			tc.gate(c.nodes[1])

			res := c.execute(context.Background(), 2, "req")
			c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
			refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
			if !refusal.Reject || refusal.LogIndex != tc.status {
				t.Fatalf("%s leader answered Reject %v status %d, want status %d",
					tc.name, refusal.Reject, refusal.LogIndex, tc.status)
			}
			c.deliver(refusal)
			if err := c.result(res).err; !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			c.quiet()
			c.wantRuns(map[uint64][]uint64{})
		})
	}
}

// TestExecuteOnLeader_Preflight covers the checks made before anything is
// sent or executed.
func TestExecuteOnLeader_Preflight(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)
	h := c.hosts[2]

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.ExecuteOnLeader(canceled, execShard, nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled context: err = %v, want ErrCanceled", err)
	}
	if _, err := h.ExecuteOnLeader(context.Background(), 99, nil); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("unknown shard: err = %v, want ErrShardNotFound", err)
	}
	var tooLarge *EntryTooLargeError
	if _, err := h.ExecuteOnLeader(context.Background(), execShard, make([]byte, 1025)); !errors.As(err, &tooLarge) {
		t.Fatalf("oversized request: err = %v, want *EntryTooLargeError", err)
	}
	h.draining.Store(true)
	if _, err := h.ExecuteOnLeader(context.Background(), execShard, nil); !errors.Is(err, ErrDraining) {
		t.Fatalf("draining: err = %v, want ErrDraining", err)
	}
	h.closed.Store(true)
	if _, err := h.ExecuteOnLeader(context.Background(), execShard, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: err = %v, want ErrClosed", err)
	}
	c.quiet()
	c.wantRuns(map[uint64][]uint64{})
}

// TestExecuteOnLeader_ShardStopEndsTheWait: waiting for a leader, and with an
// attempt in flight.
func TestExecuteOnLeader_ShardStopEndsTheWait(t *testing.T) {
	t.Run("waiting", func(t *testing.T) {
		c := newExecCluster(t)
		h, ns := c.hosts[2], c.nodes[2]
		done := make(chan error, 1)
		go func() {
			_, err := h.executeOnLeader(context.Background(), ns, nil)
			done <- err
		}()
		ns.stopped.Store(true)
		ns.signalLeaderChange()
		if err := c.result(asResult(done)).err; !errors.Is(err, ErrStopped) {
			t.Fatalf("err = %v, want ErrStopped", err)
		}
	})
	t.Run("in flight", func(t *testing.T) {
		c := newExecCluster(t)
		c.serve(nil)
		c.elect(1, 5, 1, 2, 3)
		res := c.execute(context.Background(), 2, "req")
		c.expect(proto.LeaderExec, 2, 1, 5)
		c.nodes[2].stopped.Store(true)
		c.nodes[2].signalLeaderChange()
		if err := c.result(res).err; !errors.Is(err, ErrStopped) {
			t.Fatalf("err = %v, want ErrStopped", err)
		}
	})
}

// TestExecuteOnLeader_LeaderStoppedDuringTheHandlerWithholdsTheReply: a
// leader whose shard stops while the handler runs no longer leads it when the
// handler returns, though its cached view still says it does. The reply is
// withheld: a local caller gets ErrStopped, and a remote caller is answered
// leaderExecDeposed and takes the request to the next leader.
func TestExecuteOnLeader_LeaderStoppedDuringTheHandlerWithholdsTheReply(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		c := newExecCluster(t)
		gate1 := make(chan struct{})
		c.serve(map[uint64]chan struct{}{1: gate1})
		c.elect(1, 5, 1, 2, 3)

		res := c.execute(context.Background(), 1, "req")
		c.awaitStarted(1)
		c.nodes[1].stopped.Store(true)
		c.nodes[1].signalLeaderChange()
		close(gate1)

		if r := c.result(res); !errors.Is(r.err, ErrStopped) {
			t.Fatalf("ExecuteOnLeader = (%q, %v), want ErrStopped", r.reply, r.err)
		}
		c.quiet()
	})
	t.Run("remote", func(t *testing.T) {
		c := newExecCluster(t)
		gate1 := make(chan struct{})
		c.serve(map[uint64]chan struct{}{1: gate1})
		c.elect(1, 5, 1, 2, 3)

		res := c.execute(context.Background(), 2, "req")
		c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
		c.awaitStarted(1)
		c.nodes[1].stopped.Store(true)
		c.nodes[1].signalLeaderChange()
		close(gate1)

		refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
		if !refusal.Reject || refusal.LogIndex != leaderExecDeposed {
			t.Fatalf("stopped leader answered Reject %v status %d, want status %d",
				refusal.Reject, refusal.LogIndex, leaderExecDeposed)
		}
		c.deliver(refusal)
		c.quiet()

		c.elect(3, 6, 2, 3)
		c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
		c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
		wantReply(t, c.result(res), "3@6:req")
		c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
	})
}

// TestExecuteOnLeader_FailedLeaderRefuses: a leader whose engine node has
// failed steps no further Raft, and the latch leaves its leadership view as
// it was. It neither executes nor answers with a reply: a remote caller is
// refused as by a replica that does not lead, and retries on the leader the
// others elect; a local caller gets ErrShardFailed.
func TestExecuteOnLeader_FailedLeaderRefuses(t *testing.T) {
	t.Run("remote before execution", func(t *testing.T) {
		c := newExecCluster(t)
		c.serve(nil)
		c.elect(1, 5, 1, 2, 3)
		c.fail(1)

		res := c.execute(context.Background(), 2, "req")
		c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
		refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
		if !refusal.Reject || refusal.LogIndex != leaderExecNotLeader {
			t.Fatalf("failed leader answered Reject %v status %d, want status %d",
				refusal.Reject, refusal.LogIndex, leaderExecNotLeader)
		}
		c.deliver(refusal)
		c.wantRuns(map[uint64][]uint64{})

		c.elect(3, 6, 2, 3)
		c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
		c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
		wantReply(t, c.result(res), "3@6:req")
		c.wantRuns(map[uint64][]uint64{6: {3}})
	})
	t.Run("remote while waiting to be ready", func(t *testing.T) {
		c := newExecCluster(t)
		c.serve(nil)
		for id := uint64(1); id <= 3; id++ {
			c.view(id, 1, 5)
		}

		res := c.execute(context.Background(), 2, "req")
		c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
		c.fail(1)
		refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
		if !refusal.Reject || refusal.LogIndex != leaderExecNotLeader {
			t.Fatalf("leader failed while unready answered Reject %v status %d, want status %d",
				refusal.Reject, refusal.LogIndex, leaderExecNotLeader)
		}
		c.deliver(refusal)

		c.elect(3, 6, 2, 3)
		c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
		c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
		wantReply(t, c.result(res), "3@6:req")
		c.wantRuns(map[uint64][]uint64{6: {3}})
	})
	t.Run("remote during the handler", func(t *testing.T) {
		c := newExecCluster(t)
		gate1 := make(chan struct{})
		c.serve(map[uint64]chan struct{}{1: gate1})
		c.elect(1, 5, 1, 2, 3)

		res := c.execute(context.Background(), 2, "req")
		c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
		c.awaitStarted(1)
		c.fail(1)
		close(gate1)

		refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
		if !refusal.Reject || refusal.LogIndex != leaderExecDeposed {
			t.Fatalf("leader failed mid-handler answered Reject %v status %d, want status %d",
				refusal.Reject, refusal.LogIndex, leaderExecDeposed)
		}
		c.deliver(refusal)
		c.quiet()

		c.elect(3, 6, 2, 3)
		c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
		c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
		wantReply(t, c.result(res), "3@6:req")
		c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
	})
	t.Run("local before execution", func(t *testing.T) {
		c := newExecCluster(t)
		c.serve(nil)
		c.elect(1, 5, 1, 2, 3)
		c.fail(1)

		if r := c.result(c.execute(context.Background(), 1, "req")); !errors.Is(r.err, ErrShardFailed) {
			t.Fatalf("ExecuteOnLeader = (%q, %v), want ErrShardFailed", r.reply, r.err)
		}
		c.quiet()
		c.wantRuns(map[uint64][]uint64{})
	})
	t.Run("local during the handler", func(t *testing.T) {
		c := newExecCluster(t)
		gate1 := make(chan struct{})
		c.serve(map[uint64]chan struct{}{1: gate1})
		c.elect(1, 5, 1, 2, 3)

		res := c.execute(context.Background(), 1, "req")
		c.awaitStarted(1)
		c.fail(1)
		if r := c.result(res); !errors.Is(r.err, ErrShardFailed) {
			t.Fatalf("ExecuteOnLeader = (%q, %v), want ErrShardFailed", r.reply, r.err)
		}
		close(gate1)
		c.quiet()
	})
}

// The view is what the listener dispatcher last delivered, and Raft can be
// ahead of it: here replica 1's Raft has heard of term 6 and stepped down
// while its view still says it leads term 5. It must refuse on Raft's word,
// before the handler as not leading and after it as deposed.
func TestExecuteOnLeader_RaftSteppedDownAheadOfTheViewRefuses(t *testing.T) {
	t.Run("remote before execution", func(t *testing.T) {
		c := newExecCluster(t)
		c.serve(nil)
		c.elect(1, 5, 1, 2, 3)
		c.raft(1, 3, 6)

		res := c.execute(context.Background(), 2, "req")
		c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
		refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
		if !refusal.Reject || refusal.LogIndex != leaderExecNotLeader {
			t.Fatalf("leader whose Raft stepped down answered Reject %v status %d, want status %d",
				refusal.Reject, refusal.LogIndex, leaderExecNotLeader)
		}
		c.deliver(refusal)
		c.wantRuns(map[uint64][]uint64{})

		c.elect(3, 6, 2, 3)
		c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
		c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
		wantReply(t, c.result(res), "3@6:req")
		c.wantRuns(map[uint64][]uint64{6: {3}})
	})
	t.Run("remote during the handler", func(t *testing.T) {
		c := newExecCluster(t)
		gate1 := make(chan struct{})
		c.serve(map[uint64]chan struct{}{1: gate1})
		c.elect(1, 5, 1, 2, 3)

		res := c.execute(context.Background(), 2, "req")
		c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
		c.awaitStarted(1)
		c.raft(1, 3, 6)
		close(gate1)

		refusal := c.expect(proto.LeaderExecResp, 1, 2, 5)
		if !refusal.Reject || refusal.LogIndex != leaderExecDeposed {
			t.Fatalf("leader whose Raft stepped down mid-handler answered Reject %v status %d, want status %d",
				refusal.Reject, refusal.LogIndex, leaderExecDeposed)
		}
		c.deliver(refusal)
		c.quiet()

		c.elect(3, 6, 2, 3)
		c.deliver(c.expect(proto.LeaderExec, 2, 3, 6))
		c.deliver(c.expect(proto.LeaderExecResp, 3, 2, 6))
		wantReply(t, c.result(res), "3@6:req")
		c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
	})
}

// TestExecuteOnLeader_LocalAttemptIsAbandonedOnLeaderChange: the caller's
// own replica leads, and its handler is still running when leadership moves.
// The caller does not wait for it: it abandons the attempt, spends the term,
// and takes the request to the new leader. The abandoned handler's outcome is
// discarded, and each term executed the request once.
func TestExecuteOnLeader_LocalAttemptIsAbandonedOnLeaderChange(t *testing.T) {
	c := newExecCluster(t)
	gate1 := make(chan struct{})
	c.serve(map[uint64]chan struct{}{1: gate1})
	c.elect(1, 5, 1, 2, 3)

	res := c.execute(context.Background(), 1, "req")
	c.awaitStarted(1)

	c.elect(3, 6, 1, 3)
	c.deliver(c.expect(proto.LeaderExec, 1, 3, 6))
	c.deliver(c.expect(proto.LeaderExecResp, 3, 1, 6))
	wantReply(t, c.result(res), "3@6:req")

	close(gate1)
	c.hosts[1].leaderExec.close()
	c.quiet()
	c.wantRuns(map[uint64][]uint64{5: {1}, 6: {3}})
}

// A local execution can outlive its caller's wait, abandoned here on a leader
// change, so it must own its copy of the request: the caller may reuse its
// buffer as soon as it returns, and a handler that edits its input must not
// edit the payload the caller retries with.
func TestExecuteOnLeader_LocalExecutionOwnsItsRequest(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	h1 := c.hosts[1]
	h1.UnregisterLeaderHandler(execShard)
	gate := make(chan struct{})
	saw := make(chan string, 1)
	err := h1.RegisterLeaderHandler(execShard, func(ctx context.Context, _ uint64, req []byte) ([]byte, error) {
		req[0] = 'X'
		c.started <- 1
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		saw <- string(req)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("RegisterLeaderHandler: %v", err)
	}
	c.elect(1, 5, 1, 2, 3)

	buf := []byte("req")
	res := make(chan execResult, 1)
	go func() {
		reply, err := h1.ExecuteOnLeader(context.Background(), execShard, buf)
		res <- execResult{reply, err}
	}()
	c.awaitStarted(1)

	c.elect(3, 6, 1, 3)
	retry := c.expect(proto.LeaderExec, 1, 3, 6)
	if got := string(retry.Entries[0].Cmd); got != "req" {
		t.Fatalf("the retry carried %q, want %q: the abandoned handler's edit reached it", got, "req")
	}
	c.deliver(retry)
	c.deliver(c.expect(proto.LeaderExecResp, 3, 1, 6))
	wantReply(t, c.result(res), "3@6:req")
	if string(buf) != "req" {
		t.Fatalf("the caller's buffer became %q, want %q", buf, "req")
	}

	buf[0] = 'Z' // the caller reuses its buffer while the handler still runs
	close(gate)
	if got := <-saw; got != "Xeq" {
		t.Fatalf("the handler read %q, want %q: it shares the caller's buffer", got, "Xeq")
	}
}

// TestLeaderExecRouter_CloseWaitsForLocalExecutions: an in-process execution
// runs on its own goroutine, and close does not return before it does, even
// for a handler that does not return the moment its context ends.
func TestLeaderExecRouter_CloseWaitsForLocalExecutions(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[1]
	started, ended, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var returned atomic.Bool
	err := h.RegisterLeaderHandler(execShard, func(ctx context.Context, _ uint64, _ []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		close(ended)
		<-release
		returned.Store(true)
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	c.elect(1, 5, 1)

	res := c.execute(context.Background(), 1, "req")
	<-started
	closed := make(chan struct{})
	go func() {
		h.leaderExec.close()
		close(closed)
	}()
	<-ended
	close(release)
	<-closed
	if !returned.Load() {
		t.Fatal("close returned while an in-process execution was still running")
	}
	if err := c.result(res).err; !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
}

// settleAttempts runs the in-process execution of one request n times, each
// to completion, and only then lets awaitAttempt wait for it: its outcome is
// already in done when the select runs, so every case ctx has ended is ready
// alongside it, and select's random choice is exercised on every attempt.
func settleAttempts(ctx context.Context, t *testing.T, c *execCluster, n int,
	check func(out leaderExecOutcome, err error)) {

	t.Helper()
	h, ns := c.hosts[1], c.nodes[1]
	for range n {
		execCtx, cancel := h.leaderExec.bind(ctx)
		done := make(chan leaderExecOutcome, 1)
		done <- h.executeLeaderRequest(execCtx, ns, 5, []byte("req"))
		cancel()
		out, _, err := h.awaitAttempt(ctx, ns, 1, 5, done, ns.leaderChanged())
		check(out, err)
	}
}

func TestExecuteOnLeader_HostCloseCancellationIsErrStopped(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[1]
	var ran atomic.Int64
	err := h.RegisterLeaderHandler(execShard, func(ctx context.Context, _ uint64, _ []byte) ([]byte, error) {
		ran.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	c.elect(1, 5, 1)
	h.leaderExec.close()

	settleAttempts(context.Background(), t, c, 64, func(_ leaderExecOutcome, err error) {
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("err = %v, want ErrStopped", err)
		}
	})
	if got := ran.Load(); got != 64 {
		t.Fatalf("handler ran %d times, want 64", got)
	}
}

func TestExecuteOnLeader_ReplyCompletedBeforeHostCloseIsKept(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[1]
	err := h.RegisterLeaderHandler(execShard, func(context.Context, uint64, []byte) ([]byte, error) {
		return []byte("done"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c.elect(1, 5, 1)
	h.leaderExec.close()

	settleAttempts(context.Background(), t, c, 64, func(out leaderExecOutcome, err error) {
		if err != nil || out.status != leaderExecOK || string(out.payload) != "done" {
			t.Fatalf("outcome = %d %q, err = %v, want the reply", out.status, out.payload, err)
		}
	})
}

func TestExecuteOnLeader_HandlerFailureCompletedBeforeHostCloseIsKept(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[1]
	err := h.RegisterLeaderHandler(execShard, func(context.Context, uint64, []byte) ([]byte, error) {
		return nil, errors.New("refused")
	})
	if err != nil {
		t.Fatal(err)
	}
	c.elect(1, 5, 1)
	h.leaderExec.close()

	settleAttempts(context.Background(), t, c, 64, func(out leaderExecOutcome, err error) {
		if err != nil || out.status != leaderExecHandlerFailed || string(out.payload) != "refused" {
			t.Fatalf("outcome = %d %q, err = %v, want the handler's failure", out.status, out.payload, err)
		}
	})
}

func TestExecuteOnLeader_CallerCancellationIsTheCallersError(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[1]
	err := h.RegisterLeaderHandler(execShard, func(ctx context.Context, _ uint64, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	c.elect(1, 5, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	settleAttempts(ctx, t, c, 64, func(_ leaderExecOutcome, err error) {
		if !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want ErrCanceled and context.Canceled", err)
		}
	})
}

func asResult(errs <-chan error) <-chan execResult {
	out := make(chan execResult, 1)
	go func() { out <- execResult{err: <-errs} }()
	return out
}

// TestExecuteOnLeader_HostCloseEndsTheWait: a caller with an attempt in flight
// returns ErrStopped when its own host closes.
func TestExecuteOnLeader_HostCloseEndsTheWait(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)
	res := c.execute(context.Background(), 2, "req")
	c.expect(proto.LeaderExec, 2, 1, 5)
	c.hosts[2].leaderExec.close()
	if err := c.result(res).err; !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
}

// TestLeaderExecRouter_CloseWaitsForRemoteExecutions: closing the leader's
// host ends the handler's context and returns only after the execution has
// finished and answered.
func TestLeaderExecRouter_CloseWaitsForRemoteExecutions(t *testing.T) {
	c := newExecCluster(t)
	c.serve(map[uint64]chan struct{}{1: make(chan struct{})})
	c.elect(1, 5, 1, 2, 3)

	res := c.execute(context.Background(), 2, "req")
	c.deliver(c.expect(proto.LeaderExec, 2, 1, 5))
	c.awaitStarted(1)

	c.hosts[1].leaderExec.close()
	answer := c.expect(proto.LeaderExecResp, 1, 2, 5)
	if !answer.Reject || answer.LogIndex != leaderExecHandlerFailed {
		t.Fatalf("answer Reject %v status %d, want the handler's context failure", answer.Reject, answer.LogIndex)
	}
	c.deliver(answer)
	var failed *LeaderHandlerError
	if err := c.result(res).err; !errors.As(err, &failed) {
		t.Fatalf("err = %v, want *LeaderHandlerError", err)
	}
}

// TestLeaderExecRouter_CloseIsNilSafeAndIdempotent, and refuses new work.
func TestLeaderExecRouter_CloseIsNilSafeAndIdempotent(t *testing.T) {
	var none *leaderExecRouter
	none.close()

	r := newLeaderExecRouter()
	r.close()
	r.close()
	if r.acquire() {
		t.Fatal("acquire succeeded on a closed router")
	}
	if r.ctx.Err() == nil {
		t.Fatal("closed router's context is live")
	}
}

// TestServeLeaderExec_DropsWhatItCannotAnswer: a malformed request, one for
// a shard this host does not hold, and any request after close.
func TestServeLeaderExec_DropsWhatItCannotAnswer(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)
	h := c.hosts[1]

	h.serveLeaderExec(&proto.Message{Type: proto.LeaderExec, ShardID: execShard, From: 2, To: 1, Term: 5, Hint: 1})
	h.serveLeaderExec(&proto.Message{Type: proto.LeaderExec, ShardID: 99, From: 2, To: 1, Term: 5, Hint: 1,
		Entries: []proto.Entry{{}}})
	h.leaderExec.close()
	h.serveLeaderExec(&proto.Message{Type: proto.LeaderExec, ShardID: execShard, From: 2, To: 1, Term: 5, Hint: 1,
		Entries: []proto.Entry{{}}})
	c.quiet()
	c.wantRuns(map[uint64][]uint64{})
}

// TestServeLeaderExec_AnswersFromItsOwnReplicaID: the receive gate admits
// To == 0, and the caller accepts an answer only from the replica it asked.
func TestServeLeaderExec_AnswersFromItsOwnReplicaID(t *testing.T) {
	c := newExecCluster(t)
	c.serve(nil)
	c.elect(1, 5, 1, 2, 3)
	c.hosts[1].serveLeaderExec(&proto.Message{Type: proto.LeaderExec, ShardID: execShard, From: 2, Term: 5,
		Hint: 9, Entries: []proto.Entry{{Cmd: []byte("x")}}})
	if m := c.expect(proto.LeaderExecResp, 1, 2, 5); m.Hint != 9 || m.Reject {
		t.Fatalf("answer Hint %d Reject %v, want Hint 9 accepted", m.Hint, m.Reject)
	}
}

// TestCompleteLeaderExec_AcceptsOnlyTheAnswerItAskedFor: an answer must come
// from the replica the attempt went to, for its shard and term; it completes
// the attempt once; an unknown refusal status is taken as "may have run".
func TestCompleteLeaderExec_AcceptsOnlyTheAnswerItAskedFor(t *testing.T) {
	c := newExecCluster(t)
	h := c.hosts[2]
	r := h.leaderExec
	call := &leaderExecCall{shardID: execShard, target: 1, term: 5, done: make(chan leaderExecOutcome, 1)}
	id := r.register(call)
	valid := proto.Message{Type: proto.LeaderExecResp, ShardID: execShard, From: 1, To: 2, Term: 5, Hint: id,
		Reject: true, LogIndex: 99, Entries: []proto.Entry{{}}}

	for name, mutate := range map[string]func(*proto.Message){
		"unknown request": func(m *proto.Message) { m.Hint = id + 1 },
		"other replica":   func(m *proto.Message) { m.From = 3 },
		"other term":      func(m *proto.Message) { m.Term = 6 },
		"other shard":     func(m *proto.Message) { m.ShardID = 2 },
		"no entry":        func(m *proto.Message) { m.Entries = nil },
	} {
		m := valid
		mutate(&m)
		h.completeLeaderExec(&m)
		select {
		case out := <-call.done:
			t.Fatalf("%s: completed the attempt with %+v", name, out)
		default:
		}
		if _, ok := r.calls.Load(id); !ok {
			t.Fatalf("%s: removed the attempt", name)
		}
	}

	h.completeLeaderExec(&valid)
	if out := <-call.done; out.status != leaderExecDeposed {
		t.Fatalf("unknown status completed as %d, want leaderExecDeposed", out.status)
	}
	if _, ok := r.calls.Load(id); ok {
		t.Fatal("completed attempt is still registered")
	}
	h.completeLeaderExec(&valid) // a duplicate finds nothing to complete

	accepted := &leaderExecCall{shardID: execShard, target: 1, term: 5, done: make(chan leaderExecOutcome, 1)}
	reply := valid
	reply.Hint, reply.Reject, reply.LogIndex = r.register(accepted), false, 0
	reply.Entries = []proto.Entry{{Cmd: []byte("ok")}}
	h.completeLeaderExec(&reply)
	if out := <-accepted.done; out.status != leaderExecOK || !bytes.Equal(out.payload, []byte("ok")) {
		t.Fatalf("reply completed as %+v, want the payload", out)
	}
}

// TestHandleMessage_LeaderExecTrafficIsBoundToASender: the two leader-exec
// types pass the same sender binding as every raft message, then go to the
// host's router instead of raft.
func TestHandleMessage_LeaderExecTrafficIsBoundToASender(t *testing.T) {
	const memberAddr, strangerAddr = "127.0.0.1:5002", "127.0.0.1:5099"
	const memberID = "spki:2222222222222222222222222222222222222222222222222222222222222222"
	const strangerID = "spki:9999999999999999999999999999999999999999999999999999999999999999"
	ms := proto.Membership{
		Addresses:  map[uint64]string{1: "127.0.0.1:5001", 2: memberAddr},
		Identities: map[uint64]string{1: fixtureIdentity, 2: memberID},
	}
	eng, handler := fromGateRig(t, ms, map[uint64]string{2: memberAddr})
	host := handler.host
	host.leaderExec = newLeaderExecRouter()
	defer host.leaderExec.close()
	msgs := make(chan proto.Message, 4)
	host.sender = &execSender{msgs: msgs}
	call := &leaderExecCall{shardID: 1, target: 2, term: 5, done: make(chan leaderExecOutcome, 1)}
	id := host.leaderExec.register(call)
	answer := proto.Message{Type: proto.LeaderExecResp, ShardID: 1, From: 2, To: 1, Term: 5, Hint: id,
		Entries: []proto.Entry{{Cmd: []byte("ok")}}}

	forged := proto.MessageBatch{SourceAddress: strangerAddr, Requests: []proto.Message{answer}}
	if err := handler.HandleMessage(forged, certCoveringAs(strangerID, strangerAddr)); err != nil {
		t.Fatalf("HandleMessage(forged): %v", err)
	}
	select {
	case <-call.done:
		t.Fatal("an answer claiming member 2's From was accepted from another identity")
	default:
	}

	genuine := proto.MessageBatch{SourceAddress: memberAddr, Requests: []proto.Message{answer}}
	if err := handler.HandleMessage(genuine, certCoveringAs(memberID, memberAddr)); err != nil {
		t.Fatalf("HandleMessage(genuine): %v", err)
	}
	if out := <-call.done; out.status != leaderExecOK || string(out.payload) != "ok" {
		t.Fatalf("genuine answer completed as %+v", out)
	}
	if len(eng.delivered) != 0 {
		t.Fatalf("%d leader-exec messages reached raft", len(eng.delivered))
	}
}

func TestLeaderExecBudget(t *testing.T) {
	for _, tc := range []struct {
		nanos uint64
		want  time.Duration
	}{
		{0, defaultPendingRequestTimeout},
		{uint64(time.Second), time.Second},
		{uint64(defaultPendingRequestTimeout) + 1, defaultPendingRequestTimeout},
		{math.MaxUint64, defaultPendingRequestTimeout},
	} {
		if got := leaderExecBudget(tc.nanos); got != tc.want {
			t.Errorf("leaderExecBudget(%d) = %v, want %v", tc.nanos, got, tc.want)
		}
	}
}

func TestLeaderExecErrors(t *testing.T) {
	unavailable := &LeaderUnavailableError{ShardID: 4, Cause: ErrNoLeader, Err: ErrCanceled}
	if !errors.Is(unavailable, ErrNoLeader) || !errors.Is(unavailable, ErrCanceled) || !Temporary(unavailable) {
		t.Fatalf("LeaderUnavailableError does not unwrap to its cause and context error: %v", unavailable)
	}
	if msg := unavailable.Error(); !strings.Contains(msg, "shard 4") ||
		!strings.Contains(msg, ErrNoLeader.Error()) || !strings.Contains(msg, ErrCanceled.Error()) {
		t.Fatalf("LeaderUnavailableError.Error() = %q", msg)
	}
	failed := &LeaderHandlerError{ShardID: 4, LeaderID: 2, Term: 9, Message: "lease held"}
	if msg := failed.Error(); !strings.Contains(msg, "shard 4") || !strings.Contains(msg, "replica 2") ||
		!strings.Contains(msg, "term 9") || !strings.HasSuffix(msg, "lease held") {
		t.Fatalf("LeaderHandlerError.Error() = %q", msg)
	}
}
