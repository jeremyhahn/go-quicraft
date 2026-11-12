package raftio

import (
	"context"
	"sync"
	"testing"

	qcConfig "github.com/jeremyhahn/go-quicraft/pkg/config"
)

// mappingEvent is one recorded listener call: which listener, which
// method, and the payload it was given.
type mappingEvent struct {
	listener string
	method   string
	payload  any
}

// mappingLog is a shared, ordered record of listener calls.
type mappingLog struct {
	mu     sync.Mutex
	events []mappingEvent
}

func (l *mappingLog) add(listener, method string, payload any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, mappingEvent{listener: listener, method: method, payload: payload})
}

func (l *mappingLog) snapshot() []mappingEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]mappingEvent, len(l.events))
	copy(out, l.events)
	return out
}

// mappingRaftListener records every IRaftEventListener call.
type mappingRaftListener struct{ log *mappingLog }

func (r *mappingRaftListener) LeaderUpdated(info LeaderInfo) {
	r.log.add("raft", "LeaderUpdated", info)
}

// mappingSystemListener records every ISystemEventListener call.
type mappingSystemListener struct{ log *mappingLog }

func (s *mappingSystemListener) NodeHostShuttingDown() {
	s.log.add("sys", "NodeHostShuttingDown", nil)
}
func (s *mappingSystemListener) NodeUnloaded(info NodeInfo) { s.log.add("sys", "NodeUnloaded", info) }
func (s *mappingSystemListener) NodeDeleted(info NodeInfo)  { s.log.add("sys", "NodeDeleted", info) }
func (s *mappingSystemListener) NodeReady(info NodeInfo)    { s.log.add("sys", "NodeReady", info) }
func (s *mappingSystemListener) MembershipChanged(info NodeInfo) {
	s.log.add("sys", "MembershipChanged", info)
}
func (s *mappingSystemListener) ConnectionEstablished(info ConnectionInfo) {
	s.log.add("sys", "ConnectionEstablished", info)
}
func (s *mappingSystemListener) ConnectionFailed(info ConnectionInfo) {
	s.log.add("sys", "ConnectionFailed", info)
}
func (s *mappingSystemListener) SendSnapshotStarted(info SnapshotInfo) {
	s.log.add("sys", "SendSnapshotStarted", info)
}
func (s *mappingSystemListener) SendSnapshotCompleted(info SnapshotInfo) {
	s.log.add("sys", "SendSnapshotCompleted", info)
}
func (s *mappingSystemListener) SendSnapshotAborted(info SnapshotInfo) {
	s.log.add("sys", "SendSnapshotAborted", info)
}
func (s *mappingSystemListener) SnapshotReceived(info SnapshotInfo) {
	s.log.add("sys", "SnapshotReceived", info)
}
func (s *mappingSystemListener) SnapshotRecovered(info SnapshotInfo) {
	s.log.add("sys", "SnapshotRecovered", info)
}
func (s *mappingSystemListener) SnapshotCreated(info SnapshotInfo) {
	s.log.add("sys", "SnapshotCreated", info)
}
func (s *mappingSystemListener) SnapshotCompacted(info SnapshotInfo) {
	s.log.add("sys", "SnapshotCompacted", info)
}
func (s *mappingSystemListener) LogCompacted(info EntryInfo) { s.log.add("sys", "LogCompacted", info) }
func (s *mappingSystemListener) LogDBCompacted(info EntryInfo) {
	s.log.add("sys", "LogDBCompacted", info)
}

// newMappingListeners converts a pair of recording listeners that share
// one log and returns the converted quicraft EventListener with the log.
func newMappingListeners(ctx context.Context, t *testing.T) (*qcConfig.EventListener, *mappingLog) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before conversion: %v", err)
	}
	log := &mappingLog{}
	el := ConvertEventListeners(&mappingRaftListener{log: log}, &mappingSystemListener{log: log})
	if el == nil {
		t.Fatal("ConvertEventListeners returned nil")
	}
	return el, log
}

// requireSingleEvent asserts the log holds exactly one call, to the named
// listener and method, with a payload equal (==) to want.
func requireSingleEvent(ctx context.Context, t *testing.T, log *mappingLog, listener, method string, want any) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before check: %v", err)
	}
	events := log.snapshot()
	if len(events) != 1 {
		t.Fatalf("%s: got %d listener calls, want 1: %+v", method, len(events), events)
	}
	got := events[0]
	if got.listener != listener || got.method != method {
		t.Fatalf("got call %s.%s, want %s.%s", got.listener, got.method, listener, method)
	}
	if got.payload != want {
		t.Fatalf("%s payload = %+v, want %+v", method, got.payload, want)
	}
}

// TestConvertEventListeners_SnapshotInfoFieldMapping checks (a): every
// snapshot callback delivers a SnapshotInfo equal to the one given, with
// each field distinct so a swapped field is caught.
func TestConvertEventListeners_SnapshotInfoFieldMapping(t *testing.T) {
	ctx := context.Background()
	in := qcConfig.SnapshotInfo{ShardID: 11, ReplicaID: 22, From: 33, Index: 44}
	want := SnapshotInfo{ShardID: 11, ReplicaID: 22, From: 33, Index: 44}

	cases := []struct {
		method string
		get    func(*qcConfig.EventListener) func(qcConfig.SnapshotInfo)
	}{
		{"SendSnapshotStarted", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotSendStarted }},
		{"SendSnapshotCompleted", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotSent }},
		{"SendSnapshotAborted", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotSendAborted }},
		{"SnapshotReceived", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotReceived }},
		{"SnapshotRecovered", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotRecovered }},
		{"SnapshotCreated", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotCreated }},
		{"SnapshotCompacted", func(el *qcConfig.EventListener) func(qcConfig.SnapshotInfo) { return el.OnSnapshotCompacted }},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			el, log := newMappingListeners(ctx, t)
			cb := tc.get(el)
			if cb == nil {
				t.Fatalf("callback for %s is nil", tc.method)
			}
			cb(in)
			requireSingleEvent(ctx, t, log, "sys", tc.method, want)
		})
	}
}

// TestConvertEventListeners_MembershipChangedFieldMapping checks (b).
func TestConvertEventListeners_MembershipChangedFieldMapping(t *testing.T) {
	ctx := context.Background()
	el, log := newMappingListeners(ctx, t)
	if el.OnMembershipChanged == nil {
		t.Fatal("OnMembershipChanged is nil")
	}
	el.OnMembershipChanged(qcConfig.EventShardInfo{ShardID: 7, ReplicaID: 9})
	requireSingleEvent(ctx, t, log, "sys", "MembershipChanged", NodeInfo{ShardID: 7, ReplicaID: 9})
}

// TestConvertEventListeners_ConnectionInfoFieldMapping checks (c): both
// connection callbacks deliver the address with SnapshotConnection false.
func TestConvertEventListeners_ConnectionInfoFieldMapping(t *testing.T) {
	ctx := context.Background()
	const addr = "replica-b.example:4100"
	want := ConnectionInfo{Address: addr, SnapshotConnection: false}

	t.Run("ConnectionEstablished", func(t *testing.T) {
		el, log := newMappingListeners(ctx, t)
		if el.OnConnectionEstablished == nil {
			t.Fatal("OnConnectionEstablished is nil")
		}
		el.OnConnectionEstablished(qcConfig.ConnectionInfo{Address: addr})
		requireSingleEvent(ctx, t, log, "sys", "ConnectionEstablished", want)
	})
	t.Run("ConnectionFailed", func(t *testing.T) {
		el, log := newMappingListeners(ctx, t)
		if el.OnConnectionFailed == nil {
			t.Fatal("OnConnectionFailed is nil")
		}
		el.OnConnectionFailed(qcConfig.ConnectionInfo{Address: addr})
		requireSingleEvent(ctx, t, log, "sys", "ConnectionFailed", want)
	})
}

// TestConvertEventListeners_ListenerRoutingFieldMapping checks (d): with
// both listeners set, OnLeaderUpdated reaches only the raft listener and
// OnNodeReady reaches only the system listener, each with every field.
func TestConvertEventListeners_ListenerRoutingFieldMapping(t *testing.T) {
	ctx := context.Background()

	t.Run("OnLeaderUpdated", func(t *testing.T) {
		el, log := newMappingListeners(ctx, t)
		if el.OnLeaderUpdated == nil {
			t.Fatal("OnLeaderUpdated is nil")
		}
		el.OnLeaderUpdated(qcConfig.LeaderInfo{ShardID: 3, ReplicaID: 5, Term: 8, LeaderID: 13})
		requireSingleEvent(ctx, t, log, "raft", "LeaderUpdated",
			LeaderInfo{ShardID: 3, ReplicaID: 5, Term: 8, LeaderID: 13})
	})
	t.Run("OnNodeReady", func(t *testing.T) {
		el, log := newMappingListeners(ctx, t)
		if el.OnNodeReady == nil {
			t.Fatal("OnNodeReady is nil")
		}
		el.OnNodeReady(qcConfig.EventShardInfo{ShardID: 21, ReplicaID: 34})
		requireSingleEvent(ctx, t, log, "sys", "NodeReady", NodeInfo{ShardID: 21, ReplicaID: 34})
	})
}
