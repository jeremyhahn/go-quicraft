// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"io"
	"os"
	"path/filepath"
)

// ReencryptSnapshotFile re-encrypts the snapshot data file at srcPath into
// dstPath: every frame is decrypted under srcBarrier and re-encrypted under
// dstBarrier, streamed through an in-memory pipe so a large snapshot never
// loads fully into memory and its plaintext never lands on disk. The write is
// atomic and crash-safe — output goes to a temporary file in dstPath's
// directory, is fsynced, then renamed over dstPath; on any failure the temp
// file is removed and srcPath/dstPath are left untouched.
//
// It returns the destination's MINIMUM at-rest epoch across the written frames,
// which the host records as the migrated snapshot's Epoch so the DEK-purge
// floor protects every frame. The epoch is 0 for an empty source or a plaintext
// destination.
//
// A nil or sealed barrier denotes a PLAINTEXT side: a nil/sealed srcBarrier
// treats the source bytes as plaintext (no decryption); a nil/sealed dstBarrier
// writes plaintext. This mirrors the barrier convention of
// waldb.Reencrypt/SnapshotReencryptFunc, so a host can pass the same barriers it
// passes there. It is the migration counterpart of the receive-side
// reencryptReceivedSnapshot: that path encrypts just-received plaintext, this
// path re-keys an already-sealed snapshot from one barrier to another.
func ReencryptSnapshotFile(srcPath, dstPath string, srcBarrier, dstBarrier Barrier) (uint64, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = src.Close() }()

	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0o750); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(dstDir, ".reencrypt-snapshot-*.tmp")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	srcLive := srcBarrier != nil && !srcBarrier.IsSealed()
	dstLive := dstBarrier != nil && !dstBarrier.IsSealed()

	var epoch uint64
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
		epoch, err = encryptSnapshotFrames(dstBarrier, pr, tmp)
		// Unblock the decrypt goroutine if the encrypt side stopped early.
		_ = pr.CloseWithError(err)
	case srcLive && !dstLive:
		// Framed -> plaintext.
		_, err = decryptSnapshotFrames(srcBarrier, src, tmp)
	case !srcLive && dstLive:
		// Plaintext -> framed.
		epoch, err = encryptSnapshotFrames(dstBarrier, src, tmp)
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
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return 0, err
	}
	committed = true
	return epoch, nil
}
