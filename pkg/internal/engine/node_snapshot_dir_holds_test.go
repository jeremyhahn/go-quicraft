package engine

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// newSnapshotDirHoldsNode builds a node with a temporary snapshot root, as
// TestGCSnapshotDirs_SparesAHeldDirectory does.
func newSnapshotDirHoldsNode(ctx context.Context, t *testing.T) *Node {
	t.Helper()
	_ = ctx
	return NewNode(nil, nil, nil, config.Config{ShardID: 4, ReplicaID: 1}, nil, t.TempDir(), nil)
}

// sparedSnapshotDirHolds returns GCSparedSnapshotDirs sorted. The caller holds
// the snapshot-directory lock.
func sparedSnapshotDirHolds(ctx context.Context, t *testing.T, n *Node) []string {
	t.Helper()
	_ = ctx
	got := n.GCSparedSnapshotDirs()
	slices.Sort(got)
	return got
}

func TestSnapshotDirHolds_NewNodeHoldsNothing(t *testing.T) {
	ctx := context.Background()
	n := newSnapshotDirHoldsNode(ctx, t)
	n.LockSnapshotDir()
	defer n.UnlockSnapshotDir()

	if got := sparedSnapshotDirHolds(ctx, t, n); len(got) != 0 {
		t.Fatalf("new node spares %v, want none", got)
	}
	dir := n.SnapshotSaveDir(100)
	if n.IsSnapshotDirPinned(dir) {
		t.Fatalf("new node reports %s pinned", dir)
	}
	if n.IsSnapshotDirClaimed(100) {
		t.Fatal("new node reports index 100 claimed")
	}
}

func TestSnapshotDirHolds_PinsNest(t *testing.T) {
	ctx := context.Background()
	n := newSnapshotDirHoldsNode(ctx, t)
	n.LockSnapshotDir()
	defer n.UnlockSnapshotDir()

	dir := n.SnapshotSaveDir(100)
	n.PinSnapshotDir(dir)
	n.PinSnapshotDir(dir)
	if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{filepath.Clean(dir)}; !slices.Equal(got, want) {
		t.Fatalf("spared %v with two pins, want %v once", got, want)
	}

	n.UnpinSnapshotDir(dir)
	if !n.IsSnapshotDirPinned(dir) {
		t.Fatal("one unpin of two pins ended the hold")
	}
	if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{filepath.Clean(dir)}; !slices.Equal(got, want) {
		t.Fatalf("spared %v with one pin left, want %v", got, want)
	}

	n.UnpinSnapshotDir(dir)
	if n.IsSnapshotDirPinned(dir) {
		t.Fatal("two unpins of two pins left the directory pinned")
	}
	if got := sparedSnapshotDirHolds(ctx, t, n); len(got) != 0 {
		t.Fatalf("spared %v after every unpin, want none", got)
	}
}

func TestSnapshotDirHolds_PinCleansPath(t *testing.T) {
	ctx := context.Background()
	for _, suffix := range []string{"/", "/./"} {
		t.Run(suffix, func(t *testing.T) {
			n := newSnapshotDirHoldsNode(ctx, t)
			n.LockSnapshotDir()
			defer n.UnlockSnapshotDir()

			dir := filepath.Clean(n.SnapshotSaveDir(100))
			n.PinSnapshotDir(dir + suffix)
			if !n.IsSnapshotDirPinned(dir) {
				t.Fatalf("pin on %q not seen through %q", dir+suffix, dir)
			}
			if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{dir}; !slices.Equal(got, want) {
				t.Fatalf("spared %v, want the cleaned %v", got, want)
			}
			n.UnpinSnapshotDir(dir)
			if n.IsSnapshotDirPinned(dir + suffix) {
				t.Fatalf("unpin on %q did not end the pin on %q", dir, dir+suffix)
			}
			if got := sparedSnapshotDirHolds(ctx, t, n); len(got) != 0 {
				t.Fatalf("spared %v after the unpin, want none", got)
			}
		})
	}
}

func TestSnapshotDirHolds_UnpinWithoutPinIsNoOp(t *testing.T) {
	ctx := context.Background()
	n := newSnapshotDirHoldsNode(ctx, t)
	n.LockSnapshotDir()
	defer n.UnlockSnapshotDir()

	dir := n.SnapshotSaveDir(100)
	other := n.SnapshotSaveDir(200)
	n.PinSnapshotDir(other)
	n.UnpinSnapshotDir(dir) // never pinned
	if n.IsSnapshotDirPinned(dir) {
		t.Fatal("an unpin without a pin left the directory pinned")
	}
	if !n.IsSnapshotDirPinned(other) {
		t.Fatal("an unpin of another directory ended this pin")
	}
	if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{filepath.Clean(other)}; !slices.Equal(got, want) {
		t.Fatalf("spared %v, want %v", got, want)
	}

	n.PinSnapshotDir(dir)
	n.UnpinSnapshotDir(dir)
	if n.IsSnapshotDirPinned(dir) {
		t.Fatal("a single pin after a stray unpin needed more than one unpin")
	}
	if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{filepath.Clean(other)}; !slices.Equal(got, want) {
		t.Fatalf("spared %v, want %v", got, want)
	}
}

func TestSnapshotDirHolds_ClaimsListEachSaveDir(t *testing.T) {
	ctx := context.Background()
	n := newSnapshotDirHoldsNode(ctx, t)
	n.LockSnapshotDir()
	defer n.UnlockSnapshotDir()

	d100 := filepath.Clean(n.SnapshotSaveDir(100))
	d200 := filepath.Clean(n.SnapshotSaveDir(200))
	n.ClaimSnapshotDir(100)
	n.ClaimSnapshotDir(200)
	want := []string{d100, d200}
	slices.Sort(want)
	if got := sparedSnapshotDirHolds(ctx, t, n); !slices.Equal(got, want) {
		t.Fatalf("spared %v with claims on 100 and 200, want %v", got, want)
	}

	n.ReleaseSnapshotDir(100)
	if n.IsSnapshotDirClaimed(100) {
		t.Fatal("index 100 still claimed after its release")
	}
	if !n.IsSnapshotDirClaimed(200) {
		t.Fatal("releasing 100 ended the claim on 200")
	}
	if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{d200}; !slices.Equal(got, want) {
		t.Fatalf("spared %v after releasing 100, want %v", got, want)
	}

	n.ReleaseSnapshotDir(300) // never claimed
	if !n.IsSnapshotDirClaimed(200) {
		t.Fatal("releasing an unclaimed index ended the claim on 200")
	}
	if got, want := sparedSnapshotDirHolds(ctx, t, n), []string{d200}; !slices.Equal(got, want) {
		t.Fatalf("spared %v after releasing an unclaimed index, want %v", got, want)
	}
}

func TestSnapshotDirHolds_PinAndClaimUnion(t *testing.T) {
	ctx := context.Background()
	n := newSnapshotDirHoldsNode(ctx, t)
	n.LockSnapshotDir()
	defer n.UnlockSnapshotDir()

	pinned := filepath.Clean(n.SnapshotSaveDir(100))
	claimed := filepath.Clean(n.SnapshotSaveDir(200))
	n.PinSnapshotDir(pinned)
	n.ClaimSnapshotDir(200)
	want := []string{pinned, claimed}
	slices.Sort(want)
	if got := sparedSnapshotDirHolds(ctx, t, n); !slices.Equal(got, want) {
		t.Fatalf("spared %v with a pin and a claim, want the union %v", got, want)
	}
}
