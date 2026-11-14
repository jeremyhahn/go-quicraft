package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

// writeAndCloseFrames writes payload through a fresh frame writer into a
// buffer, closes it, and returns the writer and the buffer.
func writeAndCloseFrames(ctx context.Context, t *testing.T, payload []byte) (*snapshotFrameWriter, *bytes.Buffer) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	var buf bytes.Buffer
	w := newSnapshotFrameWriter(&buf, newTestBarrier(3), 2, 1, 42)
	t.Cleanup(w.ReleasePins)
	n, err := w.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write(%d bytes) = (%d, %v), want (%d, nil)", len(payload), n, err, len(payload))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	return w, &buf
}

func TestSnapshotFrameWriter_WriteAfterCloseIsRefused(t *testing.T) {
	ctx := context.Background()
	w, buf := writeAndCloseFrames(ctx, t, []byte("payload"))
	before := buf.Len()
	n, err := w.Write([]byte("x"))
	if n != 0 || !errors.Is(err, ErrSnapshotFrameWriterClosed) {
		t.Fatalf("Write after Close = (%d, %v), want (0, %v)", n, err, ErrSnapshotFrameWriterClosed)
	}
	if buf.Len() != before {
		t.Fatalf("buffer grew from %d to %d bytes on a refused Write", before, buf.Len())
	}
}

func TestSnapshotFrameWriter_SecondCloseEmitsNothing(t *testing.T) {
	ctx := context.Background()
	w, buf := writeAndCloseFrames(ctx, t, []byte("payload"))
	before := buf.Len()
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if buf.Len() != before {
		t.Fatalf("buffer grew from %d to %d bytes on a second Close", before, buf.Len())
	}
}

func TestSnapshotFrameWriter_ClosedArtifactOpensToTheBytesWritten(t *testing.T) {
	ctx := context.Background()
	sizes := []int{0, 100, snapshotEncryptChunkSize, 2*snapshotEncryptChunkSize + 1}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i*7 + 1)
			}
			w, buf := writeAndCloseFrames(ctx, t, payload)
			// A refused Write and a second Close must not disturb the artifact.
			if _, err := w.Write([]byte("x")); !errors.Is(err, ErrSnapshotFrameWriterClosed) {
				t.Fatalf("Write after Close = %v, want %v", err, ErrSnapshotFrameWriterClosed)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("second Close = %v, want nil", err)
			}
			var out bytes.Buffer
			n, err := OpenSnapshotFrames(newTestBarrier(3), 2, 1, 42, bytes.NewReader(buf.Bytes()), &out)
			if err != nil {
				t.Fatalf("OpenSnapshotFrames = %v", err)
			}
			if n != uint64(size) || !bytes.Equal(out.Bytes(), payload) {
				t.Fatalf("opened %d bytes (equal=%v), want the %d bytes written", n, bytes.Equal(out.Bytes(), payload), size)
			}
		})
	}
}

func TestSnapshotFrameWriter_LatchedFailureOutlivesClose(t *testing.T) {
	barrier := newPinCountBarrier(&failAfterNBarrier{testBarrier: newTestBarrier(7), failAfter: 0})
	w := newSnapshotFrameWriter(io.Discard, barrier, 2, 1, 42)
	_, werr := w.Write(make([]byte, 2*snapshotEncryptChunkSize))
	if werr == nil {
		t.Fatal("expected the injected sealing failure from Write")
	}
	if cerr := w.Close(); cerr != werr {
		t.Fatalf("Close = %v, want the latched Write failure %v", cerr, werr)
	}
	n, err := w.Write([]byte("x"))
	if n != 0 || err != werr {
		t.Fatalf("Write after Close = (%d, %v), want (0, latched %v)", n, err, werr)
	}
	if errors.Is(err, ErrSnapshotFrameWriterClosed) {
		t.Fatalf("Write after a failed Close reported %v instead of the latched failure", ErrSnapshotFrameWriterClosed)
	}
	barrier.assertBalanced(t, "frame writer latched failure after Close")
}
