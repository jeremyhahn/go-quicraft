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

package etcd_perf

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/benchutil"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// clusterLeaderTimeout is the maximum time to wait for leader election
// in a multi-node cluster.
const clusterLeaderTimeout = 30 * time.Second

// clusterConfChangeTimeout is the maximum time to wait for initial
// configuration changes to be applied across all nodes.
const clusterConfChangeTimeout = 10 * time.Second

// etcdClusterNode bundles a single node in a multi-node etcd/raft cluster.
// Each node has its own WAL, state machine, and event loop goroutine that
// processes Ready structs and delivers messages through its slot's TCP+TLS
// transport.
type etcdClusterNode struct {
	id           uint64
	node         raft.Node
	storage      *raft.MemoryStorage
	wal          *walWriter
	sm           *benchStateMachineEtcd
	transport    *raftTransport
	confDone     *confChangeTracker
	proposals    *waiters
	reads        *waiters
	appliedIndex atomic.Uint64
	stopCh       chan struct{}
	loopDone     sync.WaitGroup
	tickDone     chan struct{}
	stopped      atomic.Bool
}

// etcdClusterInfra is the infrastructure of an N-node cluster that outlives
// any one set of raft nodes: the TLS material, the reserved loopback
// addresses, and one transport (TLS listener plus cached peer connections) per
// node slot. It is the counterpart of QuicRaft's and Dragonboat's hosts, which
// the election benchmarks create once, outside the timer.
type etcdClusterInfra struct {
	addrs      []string
	transports []*raftTransport
	// current is the node each slot's inbound messages are delivered to; nil
	// while no node is running in the slot.
	current []atomic.Pointer[etcdClusterNode]
}

// newEtcdClusterInfra generates the shared mutual-TLS configuration, reserves
// one address per node and binds a transport listener on each.
func newEtcdClusterInfra(b *testing.B, numNodes int) *etcdClusterInfra {
	b.Helper()

	tlsCfg, err := generateSharedTLS()
	if err != nil {
		b.Fatalf("generateSharedTLS failed: %v", err)
	}

	infra := &etcdClusterInfra{
		addrs:      make([]string, numNodes),
		transports: make([]*raftTransport, 0, numNodes),
		current:    make([]atomic.Pointer[etcdClusterNode], numNodes),
	}
	for slot := range numNodes {
		infra.addrs[slot] = freeTCPPort(b)
	}
	for slot := range numNodes {
		t, err := newRaftTransport(infra.addrs[slot], tlsCfg, func(msg *raftpb.Message) {
			infra.deliver(slot, msg)
		})
		if err != nil {
			reportClose(b, "cluster infrastructure", infra.close())
			b.Fatalf("newRaftTransport for slot %d: %v", slot, err)
		}
		infra.transports = append(infra.transports, t)
	}
	return infra
}

// deliver steps msg into the node currently running in slot. A message that
// arrives before the slot's node has started, or that is addressed to a node
// of an earlier election round, is dropped, as a network drops traffic for a
// replica that is not running; raft retransmits.
func (infra *etcdClusterInfra) deliver(slot int, msg *raftpb.Message) {
	cn := infra.current[slot].Load()
	if cn == nil || msg.GetTo() != cn.id {
		return
	}
	// Step fails only once the node is stopping (raft.ErrStopped). The
	// message is then dropped like any other that reaches a replica that is
	// not running, so there is nothing to act on.
	_ = cn.node.Step(context.Background(), msg)
}

// close closes every transport and returns their close errors, joined.
func (infra *etcdClusterInfra) close() error {
	var errs []error
	for _, t := range infra.transports {
		errs = append(errs, t.close())
	}
	return errors.Join(errs...)
}

// EtcdCluster holds a multi-node etcd/raft cluster for benchmarks.
// Nodes communicate via TCP+TLS transport with length-prefixed protobuf
// framing.
type EtcdCluster struct {
	Nodes []*etcdClusterNode
	infra *etcdClusterInfra
}

// NoLeaderError indicates that no node in the cluster currently reports
// itself as the raft leader.
type NoLeaderError struct{}

func (e NoLeaderError) Error() string {
	return "no leader available in cluster"
}

// CurrentLeaderNode polls all nodes' Status() and returns the node
// that currently reports itself as the raft leader. Returns nil and
// NoLeaderError if no node is currently the leader.
func (c *EtcdCluster) CurrentLeaderNode() (*etcdClusterNode, error) {
	for _, cn := range c.Nodes {
		status := cn.node.Status()
		if status.Lead == cn.id && status.RaftState == raft.StateLeader {
			return cn, nil
		}
	}
	return nil, NoLeaderError{}
}

// waitForLeader polls all nodes until one reports itself as the current
// leader, returning that node.
func (c *EtcdCluster) waitForLeader(timeout time.Duration) (*etcdClusterNode, error) {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if cn, err := c.CurrentLeaderNode(); err == nil {
			return cn, nil
		}
		select {
		case <-deadline.C:
			return nil, NoLeaderError{}
		case <-ticker.C:
		}
	}
}

// proposeAndWait proposes cmd (built by newRequest with id) on leader and
// waits for its own entry to apply there. If raft rejects the proposal, for
// instance because leadership moved, the current leader is re-discovered and
// the proposal is retried on it once. It returns the node the proposal
// finally went to.
func (c *EtcdCluster) proposeAndWait(leader *etcdClusterNode, id uint64, cmd []byte) (*etcdClusterNode, error) {
	err := proposeAndWait(leader.node, leader.proposals, id, cmd)
	if err == nil || errors.Is(err, errApplyTimeout) {
		return leader, err
	}
	newLeader, reErr := c.waitForLeader(5 * time.Second)
	if reErr != nil {
		return leader, err
	}
	return newLeader, proposeAndWait(newLeader.node, newLeader.proposals, id, cmd)
}

// freeTCPPort reserves a loopback address for a cluster node's transport
// listener. The node binds by address, not by inherited listener, so the port
// must be chosen before the node exists; benchutil.ReserveTCPPort picks one
// from outside the kernel's ephemeral range so no outbound connection can be
// assigned it in the gap. See benchutil/port.go for the failure this replaced.
func freeTCPPort(b *testing.B) string {
	b.Helper()
	return benchutil.MustReserveTCPPort(b.Fatalf)
}

// newEtcdCluster prepares, without starting, one raft node per infra slot,
// with raft ids firstID+1 .. firstID+N: its MemoryStorage, WAL file, state
// machine and trackers, and points every slot's transport at the new ids.
// Nothing here is part of an election measurement; start is.
func newEtcdCluster(b *testing.B, infra *etcdClusterInfra, firstID uint64) *EtcdCluster {
	b.Helper()

	numNodes := len(infra.addrs)
	peerAddrs := make(map[uint64]string, numNodes)
	for slot, addr := range infra.addrs {
		peerAddrs[firstID+uint64(slot)+1] = addr
	}

	nodes := make([]*etcdClusterNode, numNodes)
	for slot := range numNodes {
		nodeID := firstID + uint64(slot) + 1

		wal, err := newWALWriter(b.TempDir())
		if err != nil {
			for _, cn := range nodes[:slot] {
				reportClose(b, "WAL", cn.wal.close())
			}
			b.Fatalf("newWALWriter for node %d: %v", nodeID, err)
		}

		transport := infra.transports[slot]
		transport.setPeers(peerAddrs)

		nodes[slot] = &etcdClusterNode{
			id:        nodeID,
			storage:   raft.NewMemoryStorage(),
			wal:       wal,
			sm:        newBenchStateMachineEtcd(),
			transport: transport,
			confDone:  newConfChangeTrackerWithCount(numNodes),
			proposals: newWaiters(),
			reads:     newWaiters(),
			stopCh:    make(chan struct{}),
			tickDone:  make(chan struct{}),
		}
	}
	return &EtcdCluster{Nodes: nodes, infra: infra}
}

// start starts every prepared node: raft.StartNode, the Ready event loop and
// the 1 ms ticker, and makes the node its slot's message recipient. Starting
// the replicas and waiting for a leader is all an election benchmark times.
func (c *EtcdCluster) start() {
	peers := make([]raft.Peer, len(c.Nodes))
	for i, cn := range c.Nodes {
		peers[i] = raft.Peer{ID: cn.id}
	}
	for slot, cn := range c.Nodes {
		cn.node = raft.StartNode(benchRaftConfig(cn.id, cn.storage), peers)
		c.infra.current[slot].Store(cn)

		cn.loopDone.Add(1)
		go func() {
			defer cn.loopDone.Done()
			cn.clusterEventLoop()
		}()
		go runTicker(cn.node, cn.stopCh, cn.tickDone)
	}
}

// SetupEtcdCluster creates an N-node etcd/raft cluster with TCP+TLS
// transport, WAL persistence, and state machines. It waits for initial
// configuration changes to be applied on all nodes, then waits for
// natural leader election (no Campaign shortcut), then runs the setup
// warmup (10 proposals and one linearizable read) on the leader.
func SetupEtcdCluster(b *testing.B, numNodes int) *EtcdCluster {
	b.Helper()

	infra := newEtcdClusterInfra(b, numNodes)
	cluster := newEtcdCluster(b, infra, 0)
	cluster.start()

	for _, cn := range cluster.Nodes {
		if !cn.confDone.wait(clusterConfChangeTimeout) {
			reportClose(b, "cluster", cluster.Close())
			b.Fatalf("initial ConfChange not applied for node %d", cn.id)
		}
	}

	leader, err := cluster.waitForLeader(clusterLeaderTimeout)
	if err != nil {
		reportClose(b, "cluster", cluster.Close())
		b.Fatalf("cluster leader election failed: %v", err)
	}

	if err := setupWarmup(leader.node, leader.proposals, leader.reads, &leader.appliedIndex, leader.sm); err != nil {
		reportClose(b, "cluster", cluster.Close())
		b.Fatalf("cluster setup warmup failed: %v", err)
	}

	return cluster
}

// clusterEventLoop processes Ready structs from the raft node, persisting
// entries and hard state to the WAL, appending to MemoryStorage, sending
// messages to peers via the transport (a leader in parallel with the WAL
// write, a follower after it), and applying committed entries through the
// state machine.
func (cn *etcdClusterNode) clusterEventLoop() {
	isLeader := false
	for {
		select {
		case rd, ok := <-cn.node.Ready():
			if !ok {
				return
			}

			// SoftState is non-nil only when it changed; it reflects this
			// Ready, so a leader that steps down in it is not a leader here.
			if rd.SoftState != nil {
				isLeader = rd.RaftState == raft.StateLeader
			}

			// A leader sends its messages in parallel with persisting, as
			// the raft package documents (go.etcd.io/raft/v3 doc.go and
			// README, "Usage" step 2: "an optimization can be applied to
			// make leader write to disk in parallel with its followers (as
			// explained at section 10.2.1 in Raft thesis)") and as the etcd
			// server's raftNode does. A follower persists first, because its
			// messages (append and vote responses) promise durable state.
			// Either way an entry is applied, and so acknowledged, only after
			// this Ready's single fsync below, and the send finishes before
			// Advance so no message is marshalled while the next Ready's
			// entries are persisted.
			var leaderSend sync.WaitGroup
			if isLeader && len(rd.Messages) > 0 {
				leaderSend.Add(1)
				go func() {
					defer leaderSend.Done()
					cn.transport.send(rd.Messages)
				}()
			}

			// Persist entries and hard state to WAL with a single fsync,
			// before anything from this Ready is applied, and on a follower
			// before anything is sent.
			hasEntries := len(rd.Entries) > 0
			hasHardState := !raft.IsEmptyHardState(rd.HardState)
			if cn.wal != nil && (hasEntries || hasHardState) {
				if err := cn.wal.persistReady(rd.Entries, rd.HardState); err != nil {
					leaderSend.Wait()
					return
				}
			}

			// Record hard state and entries in MemoryStorage for raft
			// internals.
			if err := saveToStorage(cn.storage, rd.HardState, rd.Entries); err != nil {
				leaderSend.Wait()
				return
			}

			// A follower sends only now that its state is durable.
			if !isLeader && len(rd.Messages) > 0 {
				cn.transport.send(rd.Messages)
			}

			// Apply committed entries through the state machine.
			if len(rd.CommittedEntries) > 0 {
				for _, entry := range rd.CommittedEntries {
					if entry.GetType() == raftpb.EntryConfChange {
						var cc raftpb.ConfChange
						if unmarshalErr := proto.Unmarshal(entry.Data, &cc); unmarshalErr == nil {
							cn.node.ApplyConfChange(&cc)
						}
						cn.confDone.signal()
					}
				}

				// Batch-apply all normal entries through state machine.
				cn.sm.apply(rd.CommittedEntries)
				cn.appliedIndex.Store(rd.CommittedEntries[len(rd.CommittedEntries)-1].GetIndex())

				// Wake each proposer whose own entry just applied.
				cn.proposals.completeEntries(rd.CommittedEntries)
			}

			// Hand each ReadIndex result to its reader.
			cn.reads.completeReadStates(rd.ReadStates)

			leaderSend.Wait()
			cn.node.Advance()

		case <-cn.stopCh:
			return
		}
	}
}

// stop shuts down the cluster node: detaches it from its slot's transport,
// stops the raft node, signals the event loop and ticker to exit, waits for
// goroutine cleanup, and closes the WAL file. The transport itself belongs
// to the cluster infrastructure and stays open. It returns the WAL's close
// error.
func (cn *etcdClusterNode) stop(slot *atomic.Pointer[etcdClusterNode]) error {
	if !cn.stopped.CompareAndSwap(false, true) {
		return nil
	}
	slot.CompareAndSwap(cn, nil)
	if cn.node != nil {
		cn.node.Stop()
	}
	close(cn.stopCh)
	cn.loopDone.Wait()
	if cn.node != nil {
		<-cn.tickDone
	}
	if cn.wal != nil {
		return cn.wal.close()
	}
	return nil
}

// stopNodes stops every node and leaves the infrastructure running, so an
// election benchmark can start the next round's nodes on the same
// transports. It returns every node's stop error, joined.
func (c *EtcdCluster) stopNodes() error {
	var errs []error
	for slot, cn := range c.Nodes {
		errs = append(errs, cn.stop(&c.infra.current[slot]))
	}
	return errors.Join(errs...)
}

// Close shuts down all nodes in the cluster and its infrastructure, and
// returns every shutdown error, joined.
func (c *EtcdCluster) Close() error {
	return errors.Join(c.stopNodes(), c.infra.close())
}
