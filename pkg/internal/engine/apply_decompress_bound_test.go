package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// declaredSnappyHeader returns a snappy block whose header declares n decoded
// bytes and carries no body, the shape of a decompression bomb.
func declaredSnappyHeader(n int) []byte {
	return binary.AppendUvarint(nil, uint64(n))
}

func TestDecompressBound_CheckDecompressedSize(t *testing.T) {
	const maxSize = 1024
	compressed := func(cmd []byte) proto.Entry {
		return proto.Entry{Index: 1, Term: 1, Type: proto.EntryCompressFlag, Cmd: cmd}
	}

	t.Run("declared above limit is a bomb", func(t *testing.T) {
		err := checkDecompressedSize([]proto.Entry{compressed(declaredSnappyHeader(maxSize + 1))}, maxSize)
		if !errors.Is(err, raft.ErrDecompressionBomb) {
			t.Fatalf("got %v, want ErrDecompressionBomb", err)
		}
	})
	t.Run("bomb after a valid entry is still found", func(t *testing.T) {
		entries := []proto.Entry{
			compressed(snappy.Encode(nil, []byte("ok"))),
			compressed(declaredSnappyHeader(maxSize + 1)),
		}
		if err := checkDecompressedSize(entries, maxSize); !errors.Is(err, raft.ErrDecompressionBomb) {
			t.Fatalf("got %v, want ErrDecompressionBomb", err)
		}
	})
	t.Run("declared exactly at limit passes", func(t *testing.T) {
		if err := checkDecompressedSize([]proto.Entry{compressed(declaredSnappyHeader(maxSize))}, maxSize); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		real := snappy.Encode(nil, bytes.Repeat([]byte{'x'}, maxSize))
		if err := checkDecompressedSize([]proto.Entry{compressed(real)}, maxSize); err != nil {
			t.Fatalf("real payload at limit: got %v, want nil", err)
		}
	})
	t.Run("uncompressed of any size passes", func(t *testing.T) {
		for _, n := range []int{0, maxSize, maxSize + 1, 16 * maxSize} {
			e := proto.Entry{Index: 1, Term: 1, Cmd: bytes.Repeat([]byte{0xff}, n)}
			if err := checkDecompressedSize([]proto.Entry{e}, maxSize); err != nil {
				t.Fatalf("uncompressed len %d: got %v, want nil", n, err)
			}
		}
	})
	t.Run("maxSize <= 0 disables the check", func(t *testing.T) {
		inputs := []proto.Entry{
			compressed(declaredSnappyHeader(1 << 30)),
			compressed([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
			compressed(nil),
		}
		for _, limit := range []int{0, -1} {
			if err := checkDecompressedSize(inputs, limit); err != nil {
				t.Fatalf("maxSize %d: got %v, want nil", limit, err)
			}
		}
	})
	t.Run("unreadable header is a DecompressError", func(t *testing.T) {
		for _, cmd := range [][]byte{nil, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff}} {
			err := checkDecompressedSize([]proto.Entry{compressed(cmd)}, maxSize)
			var de *raft.DecompressError
			if !errors.As(err, &de) {
				t.Fatalf("cmd %x: got %v (%T), want *raft.DecompressError", cmd, err, err)
			}
		}
	})
}

// cmdRecordingSM wraps testSM and records each applied command.
type cmdRecordingSM struct {
	*testSM
	cmdMu sync.Mutex
	cmds  [][]byte
}

func (s *cmdRecordingSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	s.cmdMu.Lock()
	for i := range entries {
		s.cmds = append(s.cmds, append([]byte(nil), entries[i].Cmd...))
	}
	s.cmdMu.Unlock()
	return s.testSM.Update(ctx, entries, results)
}

func (s *cmdRecordingSM) recorded() [][]byte {
	s.cmdMu.Lock()
	defer s.cmdMu.Unlock()
	return append([][]byte(nil), s.cmds...)
}

// runDecompressBoundBatch drives one committed batch through an apply worker
// on a node with MaxEntrySize maxSize seeded at lastApplied 5, and waits for
// the per-shard goroutine to finish it.
func runDecompressBoundBatch(ctx context.Context, t *testing.T, maxSize uint64, entries []proto.Entry) (*Node, *cmdRecordingSM, *testCallback) {
	t.Helper()
	applyC := make(chan *applyItem, 4)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	var wg sync.WaitGroup
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })

	cb := newTestCallback()
	aw := newApplyWorker(0, applyC, applyPool, 4, &wg, cb, nil, NewWorkSignal(1), stopC, nil)

	rec := &cmdRecordingSM{testSM: newTestSM()}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	cfg.MaxEntrySize = maxSize
	node := NewNode(nil, newTestApplier(rec), nil, cfg, nil, "", nil)
	node.SeedLastApplied(5, 1)

	go aw.run()

	node.commitPending.Store(true)
	item := applyPool.Get().(*applyItem)
	item.update = proto.Update{ShardID: 1, ReplicaID: 1, CommittedEntries: entries}
	item.node = node
	select {
	case applyC <- item:
	case <-ctx.Done():
		t.Fatal("context done before the batch was queued")
	}

	// commitPending is cleared by the per-shard goroutine's defer after the
	// batch is applied or failApply has returned.
	waitFor(t, 5*time.Second, time.Millisecond,
		"timed out waiting for the batch to be processed",
		func() bool { return !node.commitPending.Load() })
	return node, rec, cb
}

func TestDecompressBound_ApplyWorker_BombFailsShard(t *testing.T) {
	const maxSize = 1024
	entries := []proto.Entry{{
		Index: 6, Term: 1, Type: proto.EntryCompressFlag,
		Cmd: declaredSnappyHeader(maxSize + 1),
	}}
	node, rec, cb := runDecompressBoundBatch(context.Background(), t, maxSize, entries)

	if !node.IsFailed() {
		t.Fatal("node must be marked failed on a decompression bomb")
	}
	if got := rec.applyCount.Load(); got != 0 {
		t.Fatalf("Update calls = %d, want 0", got)
	}
	if got := len(rec.recorded()); got != 0 {
		t.Fatalf("recorded commands = %d, want 0", got)
	}
	if got := node.lastApplied.Load(); got != 5 {
		t.Fatalf("lastApplied = %d, want 5", got)
	}
	if got := cb.proposalFailedCount.Load(); got != 1 {
		t.Fatalf("OnProposalFailed calls = %d, want 1", got)
	}
	failed := cb.getProposalFailedCalls()
	if !errors.Is(failed[0].err, raft.ErrDecompressionBomb) {
		t.Fatalf("got %v, want ErrDecompressionBomb", failed[0].err)
	}
}

func TestDecompressBound_ApplyWorker_ValidCompressedApplied(t *testing.T) {
	const maxSize = 1024
	cmd := bytes.Repeat([]byte("payload-"), 64) // 512 bytes, under the limit
	entries := []proto.Entry{{
		Index: 6, Term: 1, Type: proto.EntryCompressFlag,
		Cmd: snappy.Encode(nil, cmd),
	}}
	node, rec, cb := runDecompressBoundBatch(context.Background(), t, maxSize, entries)

	if node.IsFailed() {
		t.Fatal("node must not fail on a valid compressed entry")
	}
	if got := cb.proposalFailedCount.Load(); got != 0 {
		t.Fatalf("OnProposalFailed calls = %d, want 0", got)
	}
	got := rec.recorded()
	if len(got) != 1 || !bytes.Equal(got[0], cmd) {
		t.Fatalf("applied commands = %q, want [%q]", got, cmd)
	}
	if la := node.lastApplied.Load(); la != 6 {
		t.Fatalf("lastApplied = %d, want 6", la)
	}
}
