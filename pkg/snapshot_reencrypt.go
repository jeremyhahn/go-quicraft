// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"encoding/binary"
	"io"
	"path/filepath"
)

// snapshotFrameEpochs scans the framed snapshot ciphertext at src and returns
// the distinct epochs embedded in the frames' headers, in first-seen order,
// without decrypting anything. Each frame is [4-byte LE length][seal
// ciphertext], and the seal wire format places the epoch as a big-endian
// uint64 in the ciphertext's first 8 bytes explicitly so it can be identified
// without decryption (see seal.Barrier encryptLocked).
//
// The scan is best-effort: on a malformed or truncated frame it returns the
// epochs collected so far together with the error. Callers that only need the
// epochs for in-flight pinning should pin what was found and proceed — the
// authoritative frame validation (and the caller-visible error) belongs to
// decryptSnapshotFrames.
func snapshotFrameEpochs(src io.ReadSeeker) ([]uint64, error) {
	var frameLenBuf [4]byte
	var epochBuf [8]byte
	seen := make(map[uint64]struct{})
	var epochs []uint64
	for {
		if _, err := io.ReadFull(src, frameLenBuf[:]); err != nil {
			if err == io.EOF {
				return epochs, nil
			}
			return epochs, err
		}
		frameLen := binary.LittleEndian.Uint32(frameLenBuf[:])
		if frameLen > maxSnapshotFrameLen || frameLen < uint32(len(epochBuf)) {
			return epochs, ErrSnapshotCorrupt
		}
		if _, err := io.ReadFull(src, epochBuf[:]); err != nil {
			return epochs, err
		}
		epoch := binary.BigEndian.Uint64(epochBuf[:])
		if _, ok := seen[epoch]; !ok {
			seen[epoch] = struct{}{}
			epochs = append(epochs, epoch)
		}
		if _, err := src.Seek(int64(frameLen)-int64(len(epochBuf)), io.SeekCurrent); err != nil {
			return epochs, err
		}
	}
}

// ReencryptSnapshotFile re-encrypts the snapshot data file at srcPath into
// dstPath: every frame is decrypted under srcBarrier and re-encrypted under
// dstBarrier, streamed through an in-memory pipe so a large snapshot never
// loads fully into memory and its plaintext never lands on disk. The write is
// atomic and crash-safe — output goes to a temporary file in dstPath's
// directory, is fsynced, then renamed over dstPath, and the destination
// directory chain (every directory the initial MkdirAll may have created,
// plus the rename's directory entry) is fsynced before success is reported;
// on any failure before the rename the temp file is removed and
// srcPath/dstPath are left untouched.
//
// It returns the destination's MINIMUM at-rest epoch across the written frames,
// which the host records as the migrated snapshot's Epoch so the DEK-purge
// floor protects every frame. The epoch is 0 for an empty source or a plaintext
// destination.
//
// While the source is decrypted, every epoch referenced by its frames is
// pinned on srcBarrier via RegisterInFlightEpoch so a concurrent
// PurgeEpochsBefore cannot remove a DEK mid-migration (the same protection
// decryptSnapshotForSend takes around its decrypt).
//
// A NIL barrier denotes a plaintext side: a nil srcBarrier treats the source
// bytes as plaintext (no decryption); a nil dstBarrier writes plaintext.
//
// A non-nil but SEALED barrier is NOT a plaintext side. It means encryption is
// configured for that side but temporarily unavailable, and it returns
// ErrSnapshotBarrierSealed rather than falling through: reading an encrypted
// source under a sealed barrier would copy ciphertext and record it as
// plaintext, and a sealed destination would durably downgrade a snapshot that
// must be encrypted at rest. This is the same nil-means-off,
// sealed-means-fail-closed policy saveSnapshot, HandleSnapshot, and
// ImportSnapshot enforce.
//
// It is the migration counterpart of the receive-side
// reencryptReceivedSnapshot: that path encrypts just-received plaintext, this
// path re-keys an already-sealed snapshot from one barrier to another.
func ReencryptSnapshotFile(srcPath, dstPath string, srcBarrier, dstBarrier Barrier) (uint64, error) {
	return reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, srcBarrier, dstBarrier)
}

// reencryptSnapshotFile is ReencryptSnapshotFile over an injectable
// filesystem. The exported entry point pins the os-backed one; tests supply a
// fault-injecting implementation to exercise the crash-safety sequence.
func reencryptSnapshotFile(fsys snapshotFS, srcPath, dstPath string, srcBarrier, dstBarrier Barrier) (uint64, error) {
	// Refuse before opening anything. A CONFIGURED barrier that is sealed
	// means encryption is temporarily unavailable, never that the side is
	// plaintext. Treating a sealed srcBarrier as plaintext would read the
	// source's CIPHERTEXT as if it were plaintext and write it out recorded
	// as plaintext -- silent corruption, not merely a downgrade -- and a
	// sealed dstBarrier would durably write a plaintext destination for a
	// snapshot that must be encrypted at rest.
	if srcBarrier != nil && srcBarrier.IsSealed() {
		return 0, ErrSnapshotBarrierSealed
	}
	if dstBarrier != nil && dstBarrier.IsSealed() {
		return 0, ErrSnapshotBarrierSealed
	}

	src, err := fsys.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = src.Close() }()

	// Record the deepest pre-existing ancestor of the destination directory
	// BEFORE MkdirAll: the post-rename fsync chain must cover every directory
	// MkdirAll creates, or a crash could lose the new directories' entries
	// even though the renamed file's data was fsynced.
	dstDir := filepath.Dir(dstPath)
	syncRoot := deepestExistingAncestor(fsys, dstDir)
	if err := fsys.MkdirAll(dstDir, 0o750); err != nil {
		return 0, err
	}
	tmp, err := fsys.CreateTemp(dstDir, reencryptTempPattern)
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = fsys.Remove(tmpPath)
		}
	}()

	srcLive := srcBarrier != nil
	dstLive := dstBarrier != nil

	// Pin every epoch the source frames reference so a concurrent
	// PurgeEpochsBefore cannot remove a DEK mid-migration. The scan is
	// best-effort: on a malformed source it pins what it found and lets the
	// decrypt below surface the authoritative error.
	if srcLive {
		srcEpochs, _ := snapshotFrameEpochs(src)
		for _, e := range srcEpochs {
			srcBarrier.RegisterInFlightEpoch(e)
		}
		defer func() {
			for _, e := range srcEpochs {
				srcBarrier.DeregisterInFlightEpoch(e)
			}
		}()
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
	}

	// encryptSnapshotFrames pins every dest epoch it seals a frame under and
	// returns the set; hold those pins across this function's durable write
	// (Rename + syncDirChain below) so a concurrent rotate+purge on the DEST
	// barrier cannot orphan an intermediate frame's DEK mid-migration.
	var epoch uint64
	var dstPins []uint64
	defer func() {
		for _, e := range dstPins {
			dstBarrier.DeregisterInFlightEpoch(e)
		}
	}()
	switch {
	case srcLive && dstLive:
		// Framed -> framed. Decrypt in one goroutine, feeding plaintext through
		// an in-memory pipe into the encrypt side, so no plaintext is ever
		// written to disk.
		pr, pw := io.Pipe()
		go func() {
			_, decErr := decryptSnapshotFrames(srcBarrier, src, pw)
			_ = pw.CloseWithError(decErr)
		}()
		epoch, dstPins, err = encryptSnapshotFrames(dstBarrier, pr, tmp)
		// Unblock the decrypt goroutine if the encrypt side stopped early.
		_ = pr.CloseWithError(err)
	case srcLive && !dstLive:
		// Framed -> plaintext.
		_, err = decryptSnapshotFrames(srcBarrier, src, tmp)
	case !srcLive && dstLive:
		// Plaintext -> framed.
		epoch, dstPins, err = encryptSnapshotFrames(dstBarrier, src, tmp)
	default:
		// Plaintext -> plaintext.
		_, err = io.Copy(tmp, src)
	}
	if err != nil {
		return 0, err
	}

	if err := tmp.Sync(); err != nil {
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if err := fsys.Rename(tmpPath, dstPath); err != nil {
		return 0, err
	}
	// Make the rename (and any directories MkdirAll created) durable before
	// reporting success: without the directory fsync a crash could leave the
	// caller's durably-recorded migration pointing at a directory entry that
	// no longer exists (same hazard HandleSnapshot closes with SyncDirChain).
	if err := syncDirChain(fsys, dstDir, syncRoot); err != nil {
		return 0, err
	}
	committed = true
	return epoch, nil
}
