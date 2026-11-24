// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// SnapshotDestinationError reports a ReencryptSnapshotFile destination refused before writing: Path holds a snapshot.dat (fs.ErrExist) or its lookup failed. Use a fresh directory.
type SnapshotDestinationError struct {
	// Path is the refused destination.
	Path string
	// Err is fs.ErrExist or the lookup failure.
	Err error
}

// Error returns a human-readable description of the refusal.
func (e *SnapshotDestinationError) Error() string {
	return fmt.Sprintf("quicraft: re-encrypt snapshot: destination %s refused: %v", e.Path, e.Err)
}

// Unwrap returns the underlying cause, fs.ErrExist or the lookup failure.
func (e *SnapshotDestinationError) Unwrap() error { return e.Err }

// SnapshotCommittedNotDurableError reports a re-encryption whose rename committed at Epoch but whose directory fsync failed; recover it with errors.As, record Epoch and re-verify Path before trusting it.
type SnapshotCommittedNotDurableError struct {
	// Path is the destination artifact, which already holds the migration.
	Path string
	// Epoch is the destination artifact's minimum at-rest epoch, 0 for a
	// plaintext destination. It is the value ReencryptSnapshotFile would have
	// returned on success.
	Epoch uint64
	// Err is the underlying durability failure.
	Err error
}

// Error returns a human-readable description of the durability failure.
func (e *SnapshotCommittedNotDurableError) Error() string {
	return fmt.Sprintf(
		"quicraft: snapshot re-encryption to %s is committed at epoch %d but not durable: %v",
		e.Path, e.Epoch, e.Err,
	)
}

// Unwrap returns the underlying durability failure.
func (e *SnapshotCommittedNotDurableError) Unwrap() error { return e.Err }

// snapshotFrameEpochs returns the distinct epochs in src's frame headers ([4-byte LE length][ciphertext], epoch in the first 8 bytes big-endian) in first-seen order, without decrypting; on a malformed frame it returns what it found and the error.
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

// ReencryptSnapshotFile streams srcPath into dstPath, decrypting under srcBarrier and sealing under dstBarrier (nil means plaintext, sealed fails with ErrSnapshotBarrierSealed), atomically via temp file, rename and directory fsyncs; it pins source epochs and returns the minimum destination epoch, which a caller must record even alongside an error. shardID, replicaID and index identify the snapshot for the frame AAD and are required.
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
	srcBarrier, dstBarrier Barrier) (uint64, error) {
	return reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath,
		shardID, replicaID, index, srcBarrier, dstBarrier)
}

// reencryptSnapshotFile is ReencryptSnapshotFile over an injectable
// filesystem. The exported entry point pins the os-backed one; tests supply a
// fault-injecting implementation to exercise the crash-safety sequence.
func reencryptSnapshotFile(fsys snapshotFS, srcPath, dstPath string, shardID, replicaID, index uint64,
	srcBarrier, dstBarrier Barrier) (uint64, error) {
	// Refuse a sealed barrier on either side before opening anything; sealed never means plaintext.
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

	// Record the deepest existing ancestor before MkdirAll so the fsync chain covers every created directory.
	dstDir := filepath.Dir(dstPath)
	syncRoot := fsutil.DeepestExistingAncestorWith(fsys.Stat, dstDir)
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

	// Pin every source epoch against a concurrent purge; on a malformed source pin what was found.
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

	// Hold the destination epoch pins across the durable write.
	var epoch uint64
	var dstPins []uint64
	defer func() {
		for _, e := range dstPins {
			dstBarrier.DeregisterInFlightEpoch(e)
		}
	}()
	// Verify the source against its existing snapshot.meta while copying, before anything is committed.
	srcMeta, err := readReencryptSourceMeta(fsys, srcPath, dstPath)
	if err != nil {
		return 0, err
	}
	// Refuse a destination that already holds snapshot.dat; only fs.ErrNotExist proves it absent.
	if srcMeta.present {
		_, statErr := fsys.Stat(dstPath)
		switch {
		case statErr == nil:
			return 0, &SnapshotDestinationError{Path: dstPath, Err: fs.ErrExist}
		case !errors.Is(statErr, fs.ErrNotExist):
			return 0, &SnapshotDestinationError{Path: dstPath, Err: statErr}
		}
	}
	srcReader, verifySrc := teeSourceVerifier(srcPath, srcMeta, src)

	epoch, dstPins, err = copySnapshotAcrossBarriers(srcReader, tmp, shardID, replicaID, index,
		srcBarrier, dstBarrier)
	if err != nil {
		return 0, err
	}
	if err := verifySrc(); err != nil {
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
	// The rename succeeded, so the write is committed even if the fsync below fails.
	committed = true

	// Restamp snapshot.meta for the re-encrypted bytes before the directory fsync; a failure returns the epoch too.
	if err := restampReencryptedSnapshotMeta(fsys, dstPath, srcMeta, epoch); err != nil {
		return epoch, &SnapshotCommittedNotDurableError{Path: dstPath, Epoch: epoch, Err: err}
	}

	// Make the rename durable before reporting success; on failure return the real epoch, also inside SnapshotCommittedNotDurableError.
	if err := syncDirChain(fsys, dstDir, syncRoot); err != nil {
		return epoch, &SnapshotCommittedNotDurableError{
			Path:  dstPath,
			Epoch: epoch,
			Err:   err,
		}
	}
	return epoch, nil
}

// teeSourceVerifier returns the source reader and a post-copy check that verifies the source against its snapshot.meta when one will be restamped; otherwise src and a no-op.
func teeSourceVerifier(srcPath string, srcMeta reencryptSourceMeta, src io.Reader) (io.Reader, func() error) {
	if !srcMeta.present {
		return src, func() error { return nil }
	}
	verifier := engine.NewSnapshotDataVerifierForMeta(filepath.Dir(srcPath), srcMeta.meta, srcMeta.err)
	tee := io.TeeReader(src, verifier)
	return tee, func() error {
		if _, err := io.Copy(io.Discard, tee); err != nil {
			return err
		}
		return verifier.Verify()
	}
}

// reencryptSourceMeta is the source's snapshot.meta, read once; present is false when there is none or dstPath is not a snapshot.dat, and err is a read or parse failure.
type reencryptSourceMeta struct {
	present bool
	meta    engine.SnapshotMeta
	err     error
}

// readReencryptSourceMeta reads the bounded snapshot.meta beside srcPath through fsys; only an open failure is returned as an error.
func readReencryptSourceMeta(fsys snapshotFS, srcPath, dstPath string) (reencryptSourceMeta, error) {
	if filepath.Base(dstPath) != "snapshot.dat" {
		return reencryptSourceMeta{}, nil
	}
	f, err := fsys.Open(filepath.Join(filepath.Dir(srcPath), "snapshot.meta"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return reencryptSourceMeta{}, nil
		}
		return reencryptSourceMeta{}, err
	}
	data, err := io.ReadAll(io.LimitReader(f, reencryptMetaReadLimit+1))
	// A read-only handle's Close error cannot indicate lost data.
	_ = f.Close()
	if err == nil && len(data) > reencryptMetaReadLimit {
		err = engine.ErrSnapshotMetaTooLarge
	}
	var meta engine.SnapshotMeta
	if err == nil {
		meta, err = engine.ParseSnapshotMeta(data)
	}
	return reencryptSourceMeta{present: true, meta: meta, err: err}, nil
}

// copySnapshotAcrossBarriers streams src into dst across barriers (nil means plaintext) and returns the at-rest epoch and the pinned destination epochs, both zero when dstBarrier is nil.
func copySnapshotAcrossBarriers(src io.Reader, dst io.Writer, shardID, replicaID, index uint64,
	srcBarrier, dstBarrier Barrier) (uint64, []uint64, error) {
	srcLive := srcBarrier != nil
	dstLive := dstBarrier != nil
	switch {
	case srcLive && dstLive:
		// Framed -> framed. Decrypt in one goroutine, feeding plaintext through
		// an in-memory pipe into the encrypt side, so no plaintext is ever
		// written to disk.
		pr, pw := io.Pipe()
		decDone := make(chan struct{})
		go func() {
			defer close(decDone)
			_, decErr := decryptSnapshotFrames(srcBarrier, shardID, replicaID, index, src, pw)
			_ = pw.CloseWithError(decErr)
		}()
		epoch, pins, err := encryptSnapshotFrames(dstBarrier, shardID, replicaID, index, pr, dst)
		// Unblock and join the decrypt goroutine before returning, since it uses src and the source pins.
		_ = pr.CloseWithError(err)
		<-decDone
		return epoch, pins, err
	case srcLive && !dstLive:
		// Framed -> plaintext.
		_, err := decryptSnapshotFrames(srcBarrier, shardID, replicaID, index, src, dst)
		return 0, nil, err
	case !srcLive && dstLive:
		// Plaintext -> framed.
		return encryptSnapshotFrames(dstBarrier, shardID, replicaID, index, src, dst)
	default:
		// Plaintext -> plaintext.
		_, err := io.Copy(dst, src)
		return 0, nil, err
	}
}

// reencryptMetaReadLimit bounds the source snapshot.meta read.
const reencryptMetaReadLimit = 1<<16 + 4096

// restampReencryptedSnapshotMeta writes a snapshot.meta beside dstPath with the source record's fields and the new epoch, length and hash; a no-op without a source record.
func restampReencryptedSnapshotMeta(fsys snapshotFS, dstPath string, srcMeta reencryptSourceMeta, epoch uint64) error {
	if !srcMeta.present {
		return nil
	}
	if srcMeta.err != nil {
		// The source check refuses such a record before the commit; this
		// guards a caller that skipped it.
		return srcMeta.err
	}
	// Hash the destination as stored.
	dataFile, err := fsys.Open(dstPath)
	if err != nil {
		return err
	}
	dataHash, dataLen, hashErr := engine.HashSnapshotReader(dataFile)
	// A read-only handle's Close error cannot indicate lost data.
	_ = dataFile.Close()
	if hashErr != nil {
		return hashErr
	}
	meta := srcMeta.meta
	buf := engine.MarshalSnapshotMeta(engine.SnapshotMeta{
		Index:       meta.Index,
		Term:        meta.Term,
		ShardID:     meta.ShardID,
		ReplicaID:   meta.ReplicaID,
		Epoch:       epoch,
		Compression: meta.Compression,
		DataLen:     dataLen,
		DataHash:    dataHash,
	})

	// Write the record to a temp file and rename it over snapshot.meta, so a crash leaves a whole record.
	dir := filepath.Dir(dstPath)
	tmp, err := fsys.CreateTemp(dir, "snapshot.meta.tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	_, writeErr := tmp.Write(buf)
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = fsys.Rename(tmpPath, filepath.Join(dir, "snapshot.meta"))
	}
	if writeErr != nil {
		_ = fsys.Remove(tmpPath)
		return writeErr
	}
	return nil
}
