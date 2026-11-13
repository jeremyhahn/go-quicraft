package dragonboat

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
)

// deadlineCall is one NodeHost Sync call that takes a context, run
// against the given shard.
type deadlineCall struct {
	name string
	call func(ctx context.Context, nh *NodeHost, shardID uint64) error
}

// deadlineCalls lists every Sync call guarded by checkContextDeadline.
func deadlineCalls() []deadlineCall {
	return []deadlineCall{
		{"SyncPropose", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncPropose(ctx, nh.GetNoOPSession(shardID), []byte("x"))
			return err
		}},
		{"SyncRead", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncRead(ctx, shardID, nil)
			return err
		}},
		{"SyncGetSession", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncGetSession(ctx, shardID)
			return err
		}},
		{"SyncCloseSession", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncCloseSession(ctx, nh.GetNoOPSession(shardID))
		}},
		{"SyncGetShardMembership", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncGetShardMembership(ctx, shardID)
			return err
		}},
		{"SyncRequestAddReplica", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddReplica(ctx, shardID, 2, "localhost:63002", 0)
		}},
		{"SyncRequestAddReplicaWithIdentity", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddReplicaWithIdentity(ctx, shardID, 2, "localhost:63002", "node-2", 0)
		}},
		{"SyncRequestAddNonVotingWithIdentity", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddNonVotingWithIdentity(ctx, shardID, 3, "localhost:63003", "node-3", 0)
		}},
		{"SyncRequestAddWitnessWithIdentity", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddWitnessWithIdentity(ctx, shardID, 4, "localhost:63004", "node-4", 0)
		}},
		{"SyncRequestAddNonVoting", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddNonVoting(ctx, shardID, 3, "localhost:63003", 0)
		}},
		{"SyncRequestAddWitness", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddWitness(ctx, shardID, 4, "localhost:63004", 0)
		}},
		{"SyncRequestDeleteReplica", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestDeleteReplica(ctx, shardID, 1, 0)
		}},
		{"SyncRequestSnapshot", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncRequestSnapshot(ctx, shardID, DefaultSnapshotOption)
			return err
		}},
	}
}

// shardMembership reads a shard's membership with a valid deadline.
func shardMembership(ctx context.Context, t *testing.T, nh *NodeHost, shardID uint64) *Membership {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership(%d) = %v", shardID, err)
	}
	return m
}

// TestNodeHost_SyncCallsContextDeadline checks that every Sync call that
// takes a context refuses one with no deadline with ErrDeadlineNotSet and
// an expired one with ErrInvalidDeadline, on a running shard and on one
// the host does not run, and that a refused call changes nothing.
func TestNodeHost_SyncCallsContextDeadline(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	before := shardMembership(context.Background(), t, nh, 1)

	calls := deadlineCalls()
	if len(calls) != 13 {
		t.Fatalf("table has %d calls, want 13", len(calls))
	}
	for _, shardID := range []uint64{1, 99} {
		for _, c := range calls {
			t.Run(fmt.Sprintf("shard%d/%s", shardID, c.name), func(t *testing.T) {
				if err := c.call(context.Background(), nh, shardID); !errors.Is(err, ErrDeadlineNotSet) {
					t.Errorf("shard %d, no deadline: err = %v, want ErrDeadlineNotSet", shardID, err)
				}
				expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				if err := c.call(expired, nh, shardID); !errors.Is(err, ErrInvalidDeadline) {
					t.Errorf("shard %d, expired deadline: err = %v, want ErrInvalidDeadline", shardID, err)
				}
			})
		}
	}

	after := shardMembership(context.Background(), t, nh, 1)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("membership changed: before %+v, after %+v", before, after)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(1), []byte("after")); err != nil {
		t.Errorf("SyncPropose with a valid deadline = %v, want nil", err)
	}
}

// detachedLookupSM is a state machine whose Lookup and NALookup panic with
// panicWith when it is set, and otherwise wait for release to close and
// then record the query bytes they see on seen (without blocking).
type detachedLookupSM struct {
	testStateMachine
	panicWith interface{}
	release   chan struct{}
	seen      chan string
}

func (sm *detachedLookupSM) record(b []byte) {
	select {
	case sm.seen <- string(b):
	default:
	}
}

func (sm *detachedLookupSM) Lookup(query interface{}) (interface{}, error) {
	if sm.panicWith != nil {
		panic(sm.panicWith)
	}
	<-sm.release
	b, _ := query.([]byte)
	sm.record(b)
	return nil, nil
}

func (sm *detachedLookupSM) NALookup(key []byte) ([]byte, error) {
	if sm.panicWith != nil {
		panic(sm.panicWith)
	}
	<-sm.release
	sm.record(key)
	return nil, nil
}

func startDetachedLookupShard(t *testing.T, sm *detachedLookupSM, timeout time.Duration) *NodeHost {
	t.Helper()
	nh := newTestNodeHost(t)
	startTestShardWith(t, nh, 1, 1, func(_, _ uint64) dbSM.IStateMachine { return sm })
	nh.SetQueryTimeout(timeout)
	return nh
}

// A Lookup panic under a positive QueryTimeout reaches the caller's
// goroutine, where the caller's recover can contain it, instead of
// terminating the process from the worker goroutine.
func TestNodeHost_SyncCallsContextDeadline_LookupPanicReachesCaller(t *testing.T) {
	for _, na := range []bool{false, true} {
		t.Run(fmt.Sprintf("na=%v", na), func(t *testing.T) {
			sm := &detachedLookupSM{panicWith: "lookup boom"}
			nh := startDetachedLookupShard(t, sm, 5*time.Second)
			rs := completedReadIndex(t, nh, 1)
			got := func() (p interface{}) {
				defer func() { p = recover() }()
				if na {
					_, _ = nh.NAReadLocalNode(rs, []byte("k"))
				} else {
					_, _ = nh.ReadLocalNode(rs, []byte("k"))
				}
				return nil
			}()
			if got != "lookup boom" {
				t.Fatalf("recovered %v, want the Lookup panic value", got)
			}
		})
	}
}

// After ErrTimeout the caller may reuse its query buffer: the lookup that
// outlives the deadline still sees the bytes the caller passed.
func TestNodeHost_SyncCallsContextDeadline_TimedOutQueryOwnsItsBuffer(t *testing.T) {
	for _, na := range []bool{false, true} {
		t.Run(fmt.Sprintf("na=%v", na), func(t *testing.T) {
			sm := &detachedLookupSM{release: make(chan struct{}), seen: make(chan string, 1)}
			nh := startDetachedLookupShard(t, sm, 50*time.Millisecond)
			rs := completedReadIndex(t, nh, 1)
			buf := []byte("original")
			var err error
			if na {
				_, err = nh.NAReadLocalNode(rs, buf)
			} else {
				_, err = nh.ReadLocalNode(rs, buf)
			}
			if !errors.Is(err, ErrTimeout) {
				close(sm.release)
				t.Fatalf("blocked lookup: err = %v, want ErrTimeout", err)
			}
			copy(buf, "reused!!")
			close(sm.release)
			select {
			case s := <-sm.seen:
				if s != "original" {
					t.Fatalf("delayed lookup saw %q, want %q", s, "original")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("delayed lookup did not finish within 5s")
			}
		})
	}
}

// Lookups that outlive their deadline hold capacity until they return:
// once maxDetachedLookups are in flight a timed read is refused with
// ErrSystemBusy, and capacity comes back when the lookups exit.
func TestNodeHost_SyncCallsContextDeadline_DetachedLookupsAreBounded(t *testing.T) {
	sm := &detachedLookupSM{release: make(chan struct{})}
	nh := startDetachedLookupShard(t, sm, 10*time.Millisecond)
	released := false
	defer func() {
		if !released {
			close(sm.release)
		}
	}()
	for i := 0; i < maxDetachedLookups; i++ {
		if _, err := nh.ReadLocalNode(completedReadIndex(t, nh, 1), nil); !errors.Is(err, ErrTimeout) {
			t.Fatalf("lookup %d: err = %v, want ErrTimeout", i, err)
		}
	}
	if _, err := nh.ReadLocalNode(completedReadIndex(t, nh, 1), nil); !errors.Is(err, ErrSystemBusy) {
		t.Fatalf("lookup past the bound: err = %v, want ErrSystemBusy", err)
	}
	close(sm.release)
	released = true
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := nh.ReadLocalNode(completedReadIndex(t, nh, 1), nil)
		if err == nil {
			return
		}
		if !errors.Is(err, ErrSystemBusy) && !errors.Is(err, ErrTimeout) {
			t.Fatalf("lookup after release: err = %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("capacity not returned within 5s: err = %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
