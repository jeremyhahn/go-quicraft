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

package waldb

import (
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

// On-disk record of the WAL's block geometry.
//
// Segment files carry no header of their own: a segment is a bare sequence of
// fixed-size blocks, and segment.ReadAll reconstructs that grid purely from the
// CURRENTLY CONFIGURED block size. The grid is therefore an unrecorded
// assumption shared by the writer and every future reader — change
// WithBlockSize on an existing data directory and the scanner misparses every
// block boundary that was written under the old size. What it finds there is
// garbage that fails CRC, i.e. "corruption", and the WAL's own repair machinery
// then does the damage: the operator gets either a fail-stop or, for the tail
// of the final segment, a truncation.
//
// One marker file in the WAL root records the geometry so that case is named
// precisely at Open instead of being discovered as corruption. It lives beside
// the shard directories rather than inside each segment because it must not
// perturb the block grid: a per-segment preamble would shift every record off
// the multiple-of-blockSize alignment that ReadAll strides over, which is a
// format break for the sake of a value that is constant for the whole database.
//
// Layout (16 bytes, little-endian to match the record format):
//
//	magic (8) || version (4) || blockSize (4)
const (
	// walFormatFilename is the marker's name in the WAL root directory. It is
	// not a %06d.log name, so recover() (which parses only segment filenames)
	// ignores it, and it sits in the root rather than a shard directory, so it
	// is never in a shard's ReadDir at all.
	walFormatFilename = "WALFORMAT"

	// walFormatTempFilename is the name the marker is written under before it
	// is renamed into place. It is a fixed name in the same directory, so the
	// rename is same-filesystem (and therefore atomic) and a leftover temp from
	// a crashed stamp is overwritten by the next one rather than accumulating.
	// Like the marker itself it is not a %06d.log name, so recover() ignores it.
	walFormatTempFilename = "WALFORMAT.tmp"

	// walFormatMagic tags the file so an unrelated or truncated file is not
	// mistaken for geometry.
	walFormatMagic = "QRWALFMT"

	// walFormatVersion is the version of the WAL on-disk format as a whole
	// (block framing plus record framing). It is compared for EXACT equality
	// before a single segment byte is read: a directory written under any other
	// framing misparses from its very first chunk, every chunk fails CRC, the
	// clean prefix is empty, and — with nothing decodable above the damage —
	// the torn-tail path would truncate the whole file to zero and delete every
	// segment above it. A silent misparse presented as a repair is the one
	// outcome this gate exists to make impossible.
	//
	// The number stays 1 for the whole pre-release period and moves for the
	// first production public release. Layout changes are made IN PLACE at the
	// pinned version and development data directories are wiped and re-synced
	// rather than migrated.
	//
	// That policy needs the individual layout changes to stay detectable, and
	// detection lives at the RECORD level rather than here. A record whose
	// layout changes is given the next free payload-type byte and its old byte
	// is retired and left unallocated, so a stale record is refused with
	// ErrUnknownPayloadType rather than misparsed in silence or reported as
	// corruption. A directory with stale records therefore passes this gate —
	// every build records 1 — and is refused when its first stale record is
	// replayed.
	//
	// The block framing is the 15-byte chunk header
	// [CRC32:4][Len:2][Type:1][DurSeq:4][GenStart:4].
	walFormatVersion uint32 = 1

	// previousWALFormatVersion is the second member of the closed read
	// window (docs/compatibility, "The accept sets"): the format the
	// previous MINOR release wrote. v1.0 has no previous release, so it
	// equals walFormatVersion and the set has one member.
	previousWALFormatVersion = walFormatVersion

	// walFormatSize is the exact on-disk length of the marker.
	walFormatSize = 8 + 4 + 4
)

// acceptsWALFormatVersion reports whether v is in the closed read set
// {walFormatVersion, previousWALFormatVersion}. It is a set, not a range, so
// it never widens when a constant moves.
func acceptsWALFormatVersion(v uint32) bool {
	return v == walFormatVersion || v == previousWALFormatVersion
}

// ErrWALFormatMalformed is returned when the WAL format marker exists but
// cannot be parsed: wrong length, wrong magic, or an unknown version. It fails
// the open rather than being ignored — a marker we cannot read is a geometry we
// cannot verify, and proceeding would be exactly the unchecked assumption the
// marker exists to remove.
var ErrWALFormatMalformed = errors.New("waldb: WAL format marker is malformed or from an unknown version")

// ErrWALFormatUnsupportedVersion is returned when the marker parses cleanly but
// records a format version this build cannot read. It is deliberately distinct
// from ErrWALFormatMalformed: the directory is not damaged, it was written by
// another version of this library, and the remedy is an upgrade procedure rather
// than a corruption investigation.
//
// There is no migration and no compatibility window. go-quicraft is pre-1.0 and
// makes no on-disk guarantee across minor versions; see docs/compatibility/architecture.md.
// Wipe the data directory and let the node
// re-sync from its peers, or restore from an exported snapshot.
var ErrWALFormatUnsupportedVersion = errors.New("waldb: WAL directory was written in an on-disk format this build cannot read; wipe the data directory and re-sync this node")

// ErrWALFormatMissing is returned when a WAL directory holds segments but no
// format marker.
//
// Every directory this build creates is marked before its first segment exists,
// so an unmarked but populated directory was written by a build that predates
// the marker — which is also a build that predates the durability sequence in
// the chunk header. Its segments cannot be parsed by this one. Opening anyway
// would fail every CRC from the first chunk, produce an empty clean prefix, and
// let the torn-tail repair truncate the entire log to zero: data destruction
// reported as a successful recovery.
//
// It is fatal for that reason alone. Adopting an absent marker would mean
// assuming the segments are in the current format, and that assumption is
// exactly what may not be made when being wrong destroys the log. Wipe the data
// directory and re-sync this node; see docs/compatibility/architecture.md.
var ErrWALFormatMissing = errors.New("waldb: WAL directory holds segments but no format marker, so it predates this on-disk format; wipe the data directory and re-sync this node")

// WALFormatVersionError reports a marker recording a version this build cannot
// read, naming both versions so the operator can see which way the mismatch
// runs. It unwraps to ErrWALFormatUnsupportedVersion.
type WALFormatVersionError struct {
	Dir       string
	Recorded  uint32
	Supported uint32
}

// Error implements the error interface.
func (e *WALFormatVersionError) Error() string {
	return "waldb: WAL directory " + e.Dir + " records on-disk format version " +
		itoa(int(e.Recorded)) + " but this build reads version " +
		itoa(int(e.Supported)) + "; " + ErrWALFormatUnsupportedVersion.Error()
}

// Unwrap returns the sentinel so errors.Is(err, ErrWALFormatUnsupportedVersion)
// holds.
func (e *WALFormatVersionError) Unwrap() error {
	return ErrWALFormatUnsupportedVersion
}

// ErrWALGeometryMismatch is returned when the configured block size differs
// from the one recorded in the WAL directory. Opening anyway would misparse
// every block boundary written under the recorded geometry and destroy the log
// as "corruption". Either restore the original block size or start from an
// empty data directory.
var ErrWALGeometryMismatch = errors.New("waldb: configured WAL block size does not match the block size this directory was written with")

// WALGeometryMismatchError reports a block-size mismatch with both values, so
// an operator can see which side to correct. It unwraps to
// ErrWALGeometryMismatch.
type WALGeometryMismatchError struct {
	Dir        string
	Recorded   int
	Configured int
}

// Error implements the error interface.
func (e *WALGeometryMismatchError) Error() string {
	return "waldb: WAL directory " + e.Dir + " was written with block size " +
		itoa(e.Recorded) + " but is configured with " + itoa(e.Configured) +
		"; " + ErrWALGeometryMismatch.Error()
}

// Unwrap returns the sentinel so errors.Is(err, ErrWALGeometryMismatch) holds.
func (e *WALGeometryMismatchError) Unwrap() error {
	return ErrWALGeometryMismatch
}

// readWALFormat reads the geometry marker from a WAL directory. It reports
// present=false when no marker exists — an absent marker is not an error,
// because directories written before the marker existed are still valid WALs.
//
// A marker that is entirely zero bytes (including a zero-LENGTH one) is
// reported as ABSENT, not malformed. That state is not a geometry we failed to
// parse; it is the residue of a stamp that never delivered its content — an
// O_CREAT that outlived the process, or a filesystem that made the new file's
// size durable ahead of its data. The correct magic is non-zero, so no valid
// marker can ever look like this, and treating it as fatal would make a WAL
// that replays perfectly unopenable with no documented way back. Reported
// absent, the next Open re-stamps it under the same evidence rule that governs
// any other unmarked directory.
//
// A marker with the right length but the wrong magic, version or geometry, or
// with a NON-ZERO prefix that is not our format, stays fatal: those bytes are
// somebody's data, not our absence, and adopting or overwriting them blindly is
// exactly the unchecked assumption the marker exists to remove.
func readWALFormat(filesystem FS, dir string) (blockSize int, present bool, err error) {
	f, err := filesystem.Open(filepath.Join(dir, walFormatFilename))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("waldb: closing WAL format marker failed",
				"dir", dir, "error", closeErr)
		}
	}()

	buf := make([]byte, walFormatSize)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, false, err
	}
	if isAllZero(buf[:n]) {
		return 0, false, nil
	}
	if n != walFormatSize || string(buf[:8]) != walFormatMagic {
		return 0, true, ErrWALFormatMalformed
	}
	// A well-formed marker recording another version is reported as such, not
	// as damage: the bytes are perfectly intelligible and name a format this
	// build does not read. Conflating the two would send an operator looking
	// for a corrupt file when the answer is an upgrade procedure.
	if recordedVersion := binary.LittleEndian.Uint32(buf[8:12]); !acceptsWALFormatVersion(recordedVersion) {
		return 0, true, &WALFormatVersionError{
			Dir:       dir,
			Recorded:  recordedVersion,
			Supported: walFormatVersion,
		}
	}
	recorded := int(binary.LittleEndian.Uint32(buf[12:16]))
	if recorded < recordHeaderSize+1 || !isPowerOfTwo(recorded) {
		return 0, true, ErrWALFormatMalformed
	}
	return recorded, true, nil
}

// writeWALFormat records the geometry in the WAL directory, atomically and
// durably: the 16 bytes are written to a temporary file, fsynced, renamed onto
// the marker's name, and the directory entry is fsynced last.
//
// The rename is what makes the marker safe, and it is not belt-and-braces. The
// alternative — create O_TRUNC, write, fsync — passes through two states a
// crash or an I/O failure can freeze forever: a file that exists with no
// content, and a file whose size is durable ahead of its data. readWALFormat
// adopts both rather than failing, but adoption is a repair, and a repair is
// only reached if the process gets to run again. Publishing by rename removes
// the states instead of tolerating them: the marker's name only ever refers to
// bytes that were complete and fsynced BEFORE the rename, so a reader sees
// either no marker at all or the whole verified value. There is no third state
// to classify, and no partially-written marker any code path has to reason
// about.
//
// Every error path unlinks the temporary file. A stamp that fails on ENOSPC or
// EIO must not leave debris in the WAL root that the next Open has to interpret
// — the whole point of a marker is that what is in the root is either geometry
// or nothing.
//
// Under noSync (tests only) the syncs are skipped but the rename is not: the
// publication is a correctness property, not a durability optimization, and
// keeping it identical in both modes is what lets the tests exercise the real
// sequence.
func writeWALFormat(filesystem FS, dir string, blockSize int, noSync bool) error {
	buf := make([]byte, walFormatSize)
	copy(buf[0:8], walFormatMagic)
	binary.LittleEndian.PutUint32(buf[8:12], walFormatVersion)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(blockSize))

	tmp := filepath.Join(dir, walFormatTempFilename)
	if err := writeMarkerTemp(filesystem, tmp, buf, noSync); err != nil {
		removeMarkerTemp(filesystem, tmp)
		return err
	}
	if err := filesystem.Rename(tmp, filepath.Join(dir, walFormatFilename)); err != nil {
		removeMarkerTemp(filesystem, tmp)
		return err
	}
	if noSync {
		return nil
	}
	// The rename is durable only once the directory it published into is.
	return syncDirFS(filesystem, dir)
}

// writeMarkerTemp fills the temporary marker file and leaves its contents
// durable, so the rename that follows publishes bytes that are already on the
// medium. It never leaves the file open, on any path.
func writeMarkerTemp(filesystem FS, tmp string, buf []byte, noSync bool) error {
	f, err := filesystem.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		closeMarkerTemp(f, tmp, "write")
		return err
	}
	if !noSync {
		if err := f.Sync(); err != nil {
			closeMarkerTemp(f, tmp, "sync")
			return err
		}
	}
	return f.Close()
}

// closeMarkerTemp closes the temporary marker file after a failure, logging a
// close error rather than masking the failure that got us here.
func closeMarkerTemp(f File, tmp, stage string) {
	if err := f.Close(); err != nil {
		slog.Debug("waldb: closing the temporary WAL format marker failed",
			"path", tmp, "stage", stage, "error", err)
	}
}

// removeMarkerTemp unlinks a temporary marker left by a failed stamp. A failure
// to unlink is logged and ignored: the temp name is fixed, so the next stamp
// truncates it, and it is not a name any reader consults.
func removeMarkerTemp(filesystem FS, tmp string) {
	if err := filesystem.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Debug("waldb: removing the temporary WAL format marker failed",
			"path", tmp, "error", err)
	}
}

// isAllZero reports whether b contains no non-zero byte. An empty slice is all
// zero by this definition, which is what makes a zero-LENGTH marker adoptable.
func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
