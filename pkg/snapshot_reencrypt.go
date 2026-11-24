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

// SnapshotDestinationError reports a ReencryptSnapshotFile destination that
// was refused before anything was written: Path already holds a snapshot.dat
// (Err is fs.ErrExist, so errors.Is(err, fs.ErrExist) holds), or looking Path
// up failed with something other than not-exist (Err is that failure, since
// the destination may still hold a published pair). Re-key into a fresh
// directory.
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

// SnapshotCommittedNotDurableError reports a snapshot re-encryption whose
// destination is COMMITTED but not yet DURABLE: the rename succeeded, so
// dstPath already holds the migrated artifact sealed at Epoch, and only the
// directory fsync that makes the rename survive a crash failed.
//
// It exists because the (epoch, error) pair ReencryptSnapshotFile returns is
// unenforceable on its own. Every caller writes `if err != nil { return 0, err
// }`, which drops exactly the epoch this case delivers — and once the caller
// records no epoch, ReencryptSnapshotFile's deferred deregister releases the
// in-flight pin and the next PurgeEpochsBefore destroys the DEK the migrated
// artifact needs. Carrying the epoch INSIDE the error means errors.As recovers
// it however the caller handles err:
//
//	var nd *SnapshotCommittedNotDurableError
//	if errors.As(err, &nd) {
//		recordEpoch(nd.Epoch) // then re-verify Path before trusting it
//	}
//
// A caller that recovers Epoch must record it and treat the artifact at Path as
// unverified until it re-checks the destination.
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
// The ONE failure that does NOT leave dstPath untouched is a directory-fsync
// failure AFTER the rename: dstPath already holds the new artifact, so the
// error is returned together with the real epoch (committed, not yet durable).
// A caller that receives a non-zero epoch alongside an error must record that
// epoch — dropping it lets the next DEK purge orphan the artifact's key — and
// treat the migration as unverified until it re-checks the destination.
//
// It returns the destination's MINIMUM at-rest epoch across the written frames,
// which the host records as the migrated snapshot's Epoch so the DEK-purge
// floor protects every frame. The epoch is 0 for a plaintext destination; a
// sealed destination always has at least its terminal frame, so an empty
// plaintext source still yields a real epoch.
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
// It is the migration counterpart of the receive and import encryption: those
// paths seal plaintext as it streams in, this path re-keys an already-sealed
// snapshot from one barrier to another.
// shardID, replicaID and index identify the SNAPSHOT both paths hold: the
// migration re-keys one logical snapshot, so the source frames are read under
// that identity and the destination frames are sealed under the same one. They
// are required, not optional: snapshot frames are bound to their position
// through the AEAD context AAD (see engine.NewSnapshotAAD) and there is no
// weaker fallback for a caller that omits them.
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
	// When a snapshot.meta will be restamped beside the destination, check
	// the source against the record it already has FIRST. The restamp hashes
	// the destination as written, so a source damaged after it was recorded
	// (a same-length bit flip, invisible to the FileSize check) would come
	// out carrying a checksum that vouches for the damage, and recovery would
	// accept state VerifySnapshotArtifact refused. Every byte the copy reads
	// is teed into the verifier, the rest of the file is drained through it,
	// and a mismatch fails here, before anything is committed.
	srcMeta, err := readReencryptSourceMeta(fsys, srcPath, dstPath)
	if err != nil {
		return 0, err
	}
	// A destination that already holds a snapshot.dat is a published pair
	// (or half of one). Replacing it would rename new data over the old
	// entry and restamp snapshot.meta beside it in two separate durable
	// steps, and a crash between them could bring the old snapshot.dat back
	// with the new record: recovery would then refuse a snapshot that was
	// valid. Refuse before anything is copied; the deferred cleanup removes
	// the temp. Only fs.ErrNotExist establishes that the destination is
	// absent; any other lookup failure (EIO, EACCES) is returned, since the
	// destination may still hold a published pair.
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
	// The rename SUCCEEDED: dstPath now holds the migrated artifact sealed at
	// `epoch`, so the write is committed from here on even if the fsync below
	// fails. Flip committed before attempting it so the deferred cleanup can
	// never remove (a temp file that no longer exists at) a path that is now
	// the destination.
	committed = true

	// snapshot.meta records the epoch, length and hash of snapshot.dat AS
	// STORED, so the source's record no longer describes the re-encrypted
	// bytes: recovery verifies the artifact against it and refuses it as
	// corrupt. Restamp it beside the destination, before the directory
	// fsync below makes both durable. A failure comes back like the fsync
	// failure below, epoch included, for the reason given there.
	if err := restampReencryptedSnapshotMeta(fsys, dstPath, srcMeta, epoch); err != nil {
		return epoch, &SnapshotCommittedNotDurableError{Path: dstPath, Epoch: epoch, Err: err}
	}

	// Make the rename (and any directories MkdirAll created) durable before
	// reporting success: without the directory fsync a crash could leave the
	// caller's durably-recorded migration pointing at a directory entry that
	// no longer exists (same hazard HandleSnapshot closes with SyncDirChain).
	//
	// A failure here is a DURABILITY error, not a "nothing happened" error, and
	// it returns the real epoch alongside it. Returning 0 would tell the caller
	// the artifact is plaintext: it would record no epoch, the deferred
	// deregister would drop this epoch's in-flight pin, and the next
	// PurgeEpochsBefore would destroy the DEK the migrated snapshot needs —
	// permanently undecryptable, the exact outcome the pinning machinery exists
	// to prevent. The caller must record the epoch and then treat the artifact
	// as not yet durable.
	//
	// The epoch also travels INSIDE the error, because the return-value contract
	// alone cannot survive `if err != nil { return 0, err }`: see
	// SnapshotCommittedNotDurableError.
	if err := syncDirChain(fsys, dstDir, syncRoot); err != nil {
		return epoch, &SnapshotCommittedNotDurableError{
			Path:  dstPath,
			Epoch: epoch,
			Err:   err,
		}
	}
	return epoch, nil
}

// teeSourceVerifier returns the reader the copy consumes the source through
// and a check to run once the copy is done. When a snapshot.meta will be
// restamped beside dstPath and one sits beside srcPath, every byte read is
// teed into an engine.SnapshotDataVerifier, and the check drains the rest of
// the source through it and verifies it against that record; otherwise the
// reader is src and the check is a no-op.
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

// reencryptSourceMeta is the source's snapshot.meta, read once through the
// injected snapshotFS and used both to check the source and to restamp the
// destination. present is false when the destination is not a snapshot.dat
// or the source has no snapshot.meta; err is the read or parse failure of a
// record that exists, which the source check reports before anything is
// committed.
type reencryptSourceMeta struct {
	present bool
	meta    engine.SnapshotMeta
	err     error
}

// readReencryptSourceMeta reads and parses the snapshot.meta beside srcPath
// through fsys, bounded like every other snapshot.meta read. Only a failure
// to open an existing record is returned as an error; a read, size or parse
// failure is carried in the result for the source check to report.
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

// copySnapshotAcrossBarriers streams the snapshot data in src into dst,
// decrypting it under srcBarrier and sealing it under dstBarrier, where a
// nil barrier means that side is plaintext. It returns the at-rest epoch and
// the destination epochs encryptSnapshotFrames pinned, which the caller
// deregisters once its durable write is done; both are zero when dstBarrier
// is nil.
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
		// Unblock the decrypt goroutine if the encrypt side stopped early, then
		// JOIN it: it reads from src and holds srcBarrier epoch pins, both of
		// which the caller's deferred cleanup tears down on return. Returning
		// while it still runs would close src and deregister the pins underneath
		// a live decrypt.
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

// reencryptMetaReadLimit bounds the source snapshot.meta read in
// readReencryptSourceMeta. A record is a fixed header plus a body
// whose declared length engine bounds at 64 KiB; anything longer is not a
// record and is refused rather than read into memory.
const reencryptMetaReadLimit = 1<<16 + 4096

// restampReencryptedSnapshotMeta writes a snapshot.meta beside dstPath for
// the re-encrypted data file, carrying the source record's index, term,
// shard, replica and compression with the new epoch, length and hash. It
// does nothing when dstPath is not a snapshot.dat or no snapshot.meta sits
// beside srcPath.
func restampReencryptedSnapshotMeta(fsys snapshotFS, dstPath string, srcMeta reencryptSourceMeta, epoch uint64) error {
	if !srcMeta.present {
		return nil
	}
	if srcMeta.err != nil {
		// The source check refuses such a record before the commit; this
		// guards a caller that skipped it.
		return srcMeta.err
	}
	// Hash the destination AS STORED, so the record describes the bytes
	// every later consumer reads.
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

	// The destination directory is not an unpublished staging directory: a
	// snapshot.meta may already sit beside dstPath (the runbook copies the
	// source's). Truncating it in place would leave a torn or empty record
	// after a crash mid-write, so the new record goes to a temp file in the
	// same directory and is renamed over snapshot.meta: a crash leaves the
	// old record or the new one, whole. The caller's directory fsync makes
	// the rename durable.
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
