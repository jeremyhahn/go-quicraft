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

// snapshotFrameWriter seals a snapshot into barrier frames AS IT IS WRITTEN.
//
// # Why a writer and not another loop
//
// The frame layout — `[4-byte LE length][AEAD frame]` per
// snapshotEncryptChunkSize chunk, each frame bound to (shardID, replicaID,
// index) plus its 0-based ordinal — has to be produced identically by every
// path that writes an artifact, because every path's output is read back by
// the same decrypt side. It used to be written out twice: once in
// encryptSnapshotFile, once on the receive path. This is the one
// implementation; both drive it, and encryptSnapshotFile is now a copy through
// it rather than a second copy of it.
//
// # What it closes (#116)
//
// The local save path could not use the read-then-encrypt form without leaving
// a window. It wrote the state machine to the canonical snapshot.dat in the
// clear, fsynced it, and encrypted in place afterwards, so between the
// plaintext fsync and the rename a COMPLETE PLAINTEXT STATE MACHINE was
// durable under the name every consumer, backup and operator reads. Measured
// live at 206ms and 461ms under load; permanent if the process was killed in
// the window, because the directory is then left with no snapshot.meta and the
// in-process discard guard never runs.
//
// A writer removes the window rather than narrowing it: no plaintext is ever
// handed to the file. The bytes reach the barrier before they reach the disk.
//
// # Ordering with compression
//
// The caller chains state machine -> snappy -> snapshotFrameWriter -> file.
// Compression happens BEFORE sealing, which is both the correct order
// (ciphertext does not compress) and the order the two-step form produced, so
// the artifact is byte-compatible with what the previous code wrote.
//
// # Not safe for concurrent use
//
// One save writes one artifact from one goroutine. The barrier underneath is
// concurrency-safe and may rotate mid-artifact, which is exactly why the epoch
// bookkeeping below tracks a minimum and pins every epoch it seals under.
type snapshotFrameWriter struct {
	dst  io.Writer
	pins *epochPinner
	aad  *SnapshotAAD

	// buf accumulates plaintext until it holds a full chunk. A frame is
	// emitted only when the buffer fills or on Close, so the chunking is
	// identical to the read-then-encrypt form's and does not depend on how the
	// state machine happens to size its writes.
	buf []byte
	n   int

	seq      uint64
	minEpoch uint64
	plain    int64

	err    error
	closed bool
}

// newSnapshotFrameWriter returns a writer that seals into dst under barrier.
//
// barrier MUST NOT be nil: a nil barrier is the configured plaintext
// deployment, and the caller writes to the file directly in that case rather
// than routing through a sealing writer that has nothing to seal with.
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

// Write accepts plaintext, sealing and emitting a frame each time a full chunk
// accumulates.
//
// It reports the number of plaintext bytes consumed, so it satisfies io.Writer
// for a state machine that knows nothing about framing.
func (w *snapshotFrameWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, ErrSnapshotFrameWriterClosed
	}
	consumed := 0
	for len(p) > 0 {
		n := copy(w.buf[w.n:], p)
		w.n += n
		p = p[n:]
		consumed += n
		w.plain += int64(n)
		if w.n == len(w.buf) {
			if err := w.emit(); err != nil {
				return consumed, err
			}
		}
	}
	return consumed, nil
}

// Close emits the trailing partial frame, if any. It does NOT close dst: the
// caller owns the file and still has to fsync it.
//
// Close is idempotent so a deferred Close after an explicit one is harmless.
func (w *snapshotFrameWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return nil
	}
	w.closed = true
	if w.n > 0 {
		return w.emit()
	}
	return nil
}

// emit seals the buffered plaintext as one frame and writes it.
//
// On any failure it releases every epoch pinned so far and latches the error,
// so a caller that keeps writing gets the FIRST failure rather than a later,
// less informative one — and so the pins cannot be leaked by a caller that
// abandons the writer on error.
func (w *snapshotFrameWriter) emit() error {
	ct, epoch, err := w.pins.sealFrame(w.buf[:w.n], w.aad.Frame(w.seq))
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

// ReleasePins releases every epoch this writer pinned.
//
// The writer releases them itself on a sealing or write failure. This is for
// the caller's own failures — a read error on the source it was copying from,
// say — where the pins are still held and the artifact is being abandoned.
func (w *snapshotFrameWriter) ReleasePins() { w.pins.releaseAll() }

// MinEpoch is the LOWEST epoch any frame was sealed under, which is the value
// the artifact records.
//
// A rotation can land between frames, so an artifact can span epochs E and
// E+1; each frame carries its own epoch in its header and decrypts correctly
// on its own, but the single recorded epoch has to be the minimum or the
// DEK-purge floor can drop a key a retained frame still needs.
//
// It is 0 when nothing was written, which the caller reads as "no frames" and
// treats the same way it treats a plaintext artifact.
func (w *snapshotFrameWriter) MinEpoch() uint64 { return w.minEpoch }

// Epochs is every epoch a frame was sealed under, for the caller to hold
// pinned until the artifact's recorded epoch is durable. It is empty after a
// failure, because the pins were already released.
func (w *snapshotFrameWriter) Epochs() []uint64 { return w.pins.epochs() }

// PlaintextBytes is how much plaintext the writer consumed, which is what the
// artifact would have measured under the old write-then-encrypt form.
func (w *snapshotFrameWriter) PlaintextBytes() int64 { return w.plain }
