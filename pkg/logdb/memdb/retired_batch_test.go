package memdb

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSaveStateRetiredBatchStoresNothing checks that SaveState validates the
// whole batch before applying any of it: one update naming a retired replica
// refuses the batch, and the live update before it is not stored.
func TestSaveStateRetiredBatchStoresNothing(t *testing.T) {
	db := New()
	if err := db.RemoveNodeData(2, 1); err != nil {
		t.Fatalf("RemoveNodeData(2, 1): %v", err)
	}

	err := db.SaveState([]logdb.Update{
		{ShardID: 1, ReplicaID: 1, State: logdb.State{Term: 1},
			Entries: []logdb.Entry{{Index: 1, Term: 1}}},
		{ShardID: 2, ReplicaID: 1, State: logdb.State{Term: 1}},
	})
	if !errors.Is(err, logdb.ErrReplicaRetired) {
		t.Fatalf("SaveState = %v, want ErrReplicaRetired", err)
	}

	if _, _, err := db.ReadState(1, 1); !errors.Is(err, logdb.ErrNoState) {
		t.Fatalf("ReadState(1, 1) = %v, want ErrNoState", err)
	}
	first, count, err := db.EntryRange(1, 1)
	if err != nil || count != 0 {
		t.Fatalf("EntryRange(1, 1) = (%d, %d, %v), want no entries", first, count, err)
	}
	infos, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo: %v", err)
	}
	for _, ni := range infos {
		if ni.ShardID == 1 && ni.ReplicaID == 1 {
			t.Fatalf("ListNodeInfo lists (1,1) after a refused batch: %v", infos)
		}
	}
}

// TestRemoveNodeDataRetiredBatchNeverStored checks that RemoveNodeData retires
// a pair that was never stored: NodeRemoved reports it, every write door
// refuses it, and no node is created for it.
func TestRemoveNodeDataRetiredBatchNeverStored(t *testing.T) {
	db := New()
	if err := db.RemoveNodeData(99, 99); err != nil {
		t.Fatalf("RemoveNodeData(99, 99) = %v, want nil", err)
	}
	removed, err := db.NodeRemoved(99, 99)
	if err != nil || !removed {
		t.Fatalf("NodeRemoved(99, 99) = (%v, %v), want (true, nil)", removed, err)
	}

	writers := []struct {
		name  string
		write func() error
	}{
		{"SaveState", func() error {
			return db.SaveState([]logdb.Update{{ShardID: 99, ReplicaID: 99, State: logdb.State{Term: 1}}})
		}},
		{"SaveSnapshot", func() error {
			return db.SaveSnapshot(99, 99, logdb.Snapshot{Index: 5, Term: 1})
		}},
		{"SaveBootstrap", func() error {
			return db.SaveBootstrap(99, 99, logdb.Bootstrap{Type: 1})
		}},
	}
	for _, w := range writers {
		if err := w.write(); !errors.Is(err, logdb.ErrReplicaRetired) {
			t.Errorf("%s(99, 99) = %v, want ErrReplicaRetired", w.name, err)
		}
	}

	infos, err := db.ListNodeInfo()
	if err != nil {
		t.Fatalf("ListNodeInfo: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("ListNodeInfo = %v, want empty", infos)
	}
}
