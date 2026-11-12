package waldb

import (
	"context"
	"errors"
	"testing"
)

// The geometry is small and multi-block so a record near the segment's
// capacity spans several blocks, crosses block padding and pays one header per
// chunk — every term of the framed size CanFit computes.
const (
	canFitBlockSize   = 512
	canFitMaxFileSize = 8 * canFitBlockSize
)

// newCanFitSegment returns a fresh segment of the canFit geometry after a
// Write of prefix bytes (none when prefix is 0). Two calls with the same
// prefix return segments in byte-for-byte the same state.
func newCanFitSegment(ctx context.Context, t *testing.T, prefix int) *segment {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	s, _, err := createSegment(defaultFS(), t.TempDir(), 1, canFitBlockSize, canFitMaxFileSize)
	if err != nil {
		t.Fatalf("createSegment: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil && !errors.Is(cerr, ErrClosed) {
			t.Errorf("Close: %v", cerr)
		}
	})
	if prefix > 0 {
		if _, err := s.Write(make([]byte, prefix)); err != nil {
			t.Fatalf("prefix Write(%d): %v", prefix, err)
		}
	}
	return s
}

// largestCanFit returns the largest data length for which CanFit is nil,
// scanning every length up to the segment size. It also asserts the verdict
// is monotonic: once a length is refused, every longer one is refused too.
func largestCanFit(ctx context.Context, t *testing.T, s *segment) int {
	t.Helper()
	largest := -1
	for n := 0; n <= canFitMaxFileSize; n++ {
		if err := ctx.Err(); err != nil {
			t.Fatalf("context: %v", err)
		}
		err := s.CanFit(n)
		switch {
		case err == nil && largest == n-1:
			largest = n
		case err == nil:
			t.Fatalf("CanFit(%d) = nil after CanFit(%d) refused: verdict is not monotonic", n, largest+1)
		}
	}
	if largest <= 0 {
		t.Fatalf("no positive length fits (largest %d)", largest)
	}
	return largest
}

// checkCanFitBoundary asserts, for segments in the state a prefix Write of
// prefix bytes leaves, that the largest length CanFit accepts is written and
// synced within maxFileSize, and that one byte more is refused by CanFit and
// by Write with the same error, wantOver, without changing the segment.
func checkCanFitBoundary(ctx context.Context, t *testing.T, prefix int, wantOver error) {
	t.Helper()
	s := newCanFitSegment(ctx, t, prefix)
	largest := largestCanFit(ctx, t, s)

	if _, err := s.Write(make([]byte, largest)); err != nil {
		t.Fatalf("Write(%d) after CanFit(%d) = nil: %v", largest, largest, err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync after Write(%d): %v", largest, err)
	}
	if got := s.Size(); got > canFitMaxFileSize {
		t.Fatalf("Size after Write(%d)+Sync = %d, exceeds maxFileSize %d", largest, got, canFitMaxFileSize)
	}

	over := newCanFitSegment(ctx, t, prefix)
	if err := over.CanFit(largest + 1); !errors.Is(err, wantOver) {
		t.Fatalf("CanFit(%d) = %v, want %v", largest+1, err, wantOver)
	}
	size, count := over.Size(), over.RecordCount()
	if _, err := over.Write(make([]byte, largest+1)); !errors.Is(err, wantOver) {
		t.Fatalf("Write(%d) = %v, want %v (the verdict CanFit gave)", largest+1, err, wantOver)
	}
	if got := over.Size(); got != size {
		t.Fatalf("refused Write changed Size: %d -> %d", size, got)
	}
	if got := over.RecordCount(); got != count {
		t.Fatalf("refused Write changed RecordCount: %d -> %d", count, got)
	}
}

// (a) A prior record crossed a block boundary, so the segment has bytes on
// disk and in the open block.
func TestSegmentCanFitMatchesWrite_AfterFlushedBlock(t *testing.T) {
	ctx := context.Background()
	const prefix = canFitBlockSize + 100
	s := newCanFitSegment(ctx, t, prefix)
	if s.fileOffset == 0 || s.blockOffset == 0 {
		t.Fatalf("prefix %d left fileOffset %d, blockOffset %d; want both > 0", prefix, s.fileOffset, s.blockOffset)
	}
	checkCanFitBoundary(ctx, t, prefix, ErrSegmentFull)
}

// (b) A partial write left bytes only in the unflushed block buffer.
func TestSegmentCanFitMatchesWrite_AfterPartialBlock(t *testing.T) {
	ctx := context.Background()
	const prefix = 100
	s := newCanFitSegment(ctx, t, prefix)
	if s.fileOffset != 0 || s.blockOffset == 0 {
		t.Fatalf("prefix %d left fileOffset %d, blockOffset %d; want 0 and > 0", prefix, s.fileOffset, s.blockOffset)
	}
	checkCanFitBoundary(ctx, t, prefix, ErrSegmentFull)
}

// (c) On an empty segment one byte past the largest fitting length cannot fit
// any segment, so both CanFit and Write report ErrRecordTooLarge, never
// ErrSegmentFull; so does a length larger than the whole segment.
func TestSegmentCanFitMatchesWrite_RecordTooLarge(t *testing.T) {
	ctx := context.Background()
	checkCanFitBoundary(ctx, t, 0, ErrRecordTooLarge)

	s := newCanFitSegment(ctx, t, 0)
	for _, n := range []int{canFitMaxFileSize, canFitMaxFileSize + 1} {
		if err := s.CanFit(n); !errors.Is(err, ErrRecordTooLarge) {
			t.Fatalf("CanFit(%d) = %v, want ErrRecordTooLarge", n, err)
		}
		if _, err := s.Write(make([]byte, n)); !errors.Is(err, ErrRecordTooLarge) {
			t.Fatalf("Write(%d) = %v, want ErrRecordTooLarge", n, err)
		}
	}
}

// (d) A closed segment refuses with ErrClosed, as Write does.
func TestSegmentCanFitMatchesWrite_Closed(t *testing.T) {
	ctx := context.Background()
	s := newCanFitSegment(ctx, t, 100)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, n := range []int{0, 1, canFitMaxFileSize + 1} {
		if err := s.CanFit(n); !errors.Is(err, ErrClosed) {
			t.Fatalf("CanFit(%d) after Close = %v, want ErrClosed", n, err)
		}
		if _, err := s.Write(make([]byte, n)); !errors.Is(err, ErrClosed) {
			t.Fatalf("Write(%d) after Close = %v, want ErrClosed", n, err)
		}
	}
}
