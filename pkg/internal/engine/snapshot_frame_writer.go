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

package engine

import (
	"encoding/binary"
	"io"
)

// snapshotFrameWriter seals a snapshot into length-prefixed AEAD frames as it is written, with exactly one terminal frame; not safe for concurrent use.
type snapshotFrameWriter struct {
	dst  io.Writer
	pins *epochPinner
	aad  *SnapshotAAD

	// buf holds plaintext until a full chunk; the remainder is the terminal frame.
	buf []byte
	n   int

	seq      uint64
	minEpoch uint64
	plain    int64

	err    error
	closed bool
}

// newSnapshotFrameWriter returns a writer sealing into dst under barrier, which must not be nil.
func newSnapshotFrameWriter(
	dst io.Writer,
	barrier SnapshotBarrier,
	shardID, replicaID, index uint64,
) *snapshotFrameWriter {
	return &snapshotFrameWriter{
		dst:  dst,
		pins: newEpochPinner(barrier),
		aad:  NewSnapshotAAD(shardID, replicaID, index, barrier),
		buf:  make([]byte, snapshotEncryptChunkSize),
	}
}

// Write accepts plaintext, emitting the held full chunk when more arrives, and reports bytes consumed.
func (w *snapshotFrameWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, ErrSnapshotFrameWriterClosed
	}
	consumed := 0
	for len(p) > 0 {
		// A full buffer is emitted only now, once plaintext behind it proves it
		// is not the artifact's last frame.
		if w.n == len(w.buf) {
			if err := w.emit(false); err != nil {
				return consumed, err
			}
		}
		n := copy(w.buf[w.n:], p)
		w.n += n
		p = p[n:]
		consumed += n
		w.plain += int64(n)
	}
	return consumed, nil
}

// Close emits the terminal frame; it does not close dst and is idempotent.
func (w *snapshotFrameWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return nil
	}
	w.closed = true
	return w.emit(true)
}

// emit seals the buffered plaintext as one frame; on failure it releases every pin and latches the first error.
func (w *snapshotFrameWriter) emit(final bool) error {
	ct, epoch, err := w.pins.sealFrame(w.buf[:w.n], w.aad.Frame(w.seq, final))
	if err != nil {
		return w.fail(&SnapshotEncryptError{Op: "encrypt", Err: err})
	}
	w.seq++
	if w.minEpoch == 0 || epoch < w.minEpoch {
		w.minEpoch = epoch
	}

	var frameLen [4]byte
	binary.LittleEndian.PutUint32(frameLen[:], uint32(len(ct)))
	if _, werr := w.dst.Write(frameLen[:]); werr != nil {
		return w.fail(&SnapshotEncryptError{Op: "write_frame", Err: werr})
	}
	if _, werr := w.dst.Write(ct); werr != nil {
		return w.fail(&SnapshotEncryptError{Op: "write_data", Err: werr})
	}
	w.n = 0
	return nil
}

// fail latches err and releases the epoch pins.
func (w *snapshotFrameWriter) fail(err error) error {
	w.pins.releaseAll()
	w.err = err
	return err
}

// ReleasePins releases every epoch this writer pinned, for a caller abandoning the artifact.
func (w *snapshotFrameWriter) ReleasePins() { w.pins.releaseAll() }

// MinEpoch is the lowest epoch any frame was sealed under, the artifact's recorded epoch; 0 until a frame is sealed.
func (w *snapshotFrameWriter) MinEpoch() uint64 { return w.minEpoch }

// Epochs is every epoch a frame was sealed under, for the caller to hold
// pinned until the artifact's recorded epoch is durable. It is empty after a
// failure, because the pins were already released.
func (w *snapshotFrameWriter) Epochs() []uint64 { return w.pins.epochs() }

// PlaintextBytes is how much plaintext the writer consumed: the artifact's
// size before sealing, which is what MaxSnapshotSize is measured against.
func (w *snapshotFrameWriter) PlaintextBytes() int64 { return w.plain }

// SealSnapshotFrames seals src into dst as one artifact and returns the minimum epoch and the pinned epochs; on failure it returns 0 and nil.
func SealSnapshotFrames(barrier SnapshotBarrier, shardID, replicaID, index uint64,
	src io.Reader, dst io.Writer) (uint64, []uint64, error) {
	w := newSnapshotFrameWriter(dst, barrier, shardID, replicaID, index)
	if _, err := io.Copy(w, src); err != nil {
		w.ReleasePins()
		return 0, nil, err
	}
	if err := w.Close(); err != nil {
		return 0, nil, err
	}
	return w.MinEpoch(), w.Epochs(), nil
}
