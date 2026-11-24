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
	"errors"
	"io"
)

// snapshotFrameDecrypter is what OpenSnapshotFrames needs of a barrier: the
// capability NewSnapshotAAD resolves the context against, and the decrypt.
type snapshotFrameDecrypter interface {
	contextAADBarrier
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
}

// OpenSnapshotFrames authenticates and decrypts a sealed snapshot from src into dst, returning plaintext bytes written; truncation, appended frames and empty artifacts fail.
func OpenSnapshotFrames(barrier snapshotFrameDecrypter, shardID, replicaID, index uint64,
	src io.Reader, dst io.Writer) (uint64, error) {
	aad := NewSnapshotAAD(shardID, replicaID, index, barrier)
	cur, end, err := readSnapshotFrame(src)
	if err != nil {
		return 0, err
	}
	if end {
		return 0, &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
	}
	var (
		seq   uint64
		total uint64
	)
	for {
		next, final, err := readSnapshotFrame(src)
		if err != nil {
			return 0, err
		}
		plaintext, decErr := barrier.DecryptWithContext(nil, cur, aad.Frame(seq, final))
		if decErr != nil {
			return 0, &SnapshotEncryptError{Op: "decrypt", Err: decErr}
		}
		if _, werr := dst.Write(plaintext); werr != nil {
			return 0, &SnapshotEncryptError{Op: "write", Err: werr}
		}
		total += uint64(len(plaintext))
		if final {
			return total, nil
		}
		cur = next
		seq++
	}
}

// readSnapshotFrame reads one [4-byte LE length][AEAD frame] record; end is true at a clean boundary, and an oversized or short frame is ErrSnapshotCorrupt.
func readSnapshotFrame(src io.Reader) (frame []byte, end bool, err error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(src, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true, nil
		}
		return nil, false, snapshotFrameReadError(err)
	}
	frameLen := binary.LittleEndian.Uint32(lenBuf[:])
	if frameLen > snapshotEncryptChunkSize+maxSnapshotFrameOverhead {
		return nil, false, &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
	}
	frame = make([]byte, frameLen)
	if _, err := io.ReadFull(src, frame); err != nil {
		return nil, false, snapshotFrameReadError(err)
	}
	return frame, false, nil
}

// snapshotFrameReadError classifies a short read inside a frame as damage to
// the artifact and anything else as the source's own failure.
func snapshotFrameReadError(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
	}
	return &SnapshotEncryptError{Op: "read", Err: err}
}
