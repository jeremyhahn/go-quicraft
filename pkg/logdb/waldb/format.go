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

// WAL geometry marker: one file in the WAL root records the block size, so a changed WithBlockSize fails Open instead of misparsing segments. Layout (16 bytes, little-endian): magic (8) || version (4) || blockSize (4).
const (
	// walFormatFilename is the marker's name in the WAL root; recover() ignores it.
	walFormatFilename = "WALFORMAT"

	// walFormatTempFilename is the fixed temporary name the marker is written under before an atomic rename; recover() ignores it.
	walFormatTempFilename = "WALFORMAT.tmp"

	// walFormatMagic tags the file so an unrelated or truncated file is not
	// mistaken for geometry.
	walFormatMagic = "QRWALFMT"

	// walFormatVersion is the WAL on-disk format version, compared for exact equality before any segment is read; record layout changes take a new payload-type byte instead. Chunk header: [CRC32:4][Len:2][Type:1][DurSeq:4][GenStart:4].
	walFormatVersion uint32 = 1

	// previousWALFormatVersion is the previous minor release's format, the second member of the read window; equal to walFormatVersion at v1.0.
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

// ErrWALFormatMalformed is returned when the format marker exists but cannot be parsed.
var ErrWALFormatMalformed = errors.New("waldb: WAL format marker is malformed or from an unknown version")

// ErrWALFormatUnsupportedVersion is returned when the marker records a version this build cannot read; wipe and re-sync, or restore a snapshot (docs/compatibility/architecture.md).
var ErrWALFormatUnsupportedVersion = errors.New("waldb: WAL directory was written in an on-disk format this build cannot read; wipe the data directory and re-sync this node")

// ErrWALFormatMissing is returned when a WAL directory holds segments but no marker; wipe and re-sync (docs/compatibility/architecture.md).
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

// ErrWALGeometryMismatch is returned when the configured block size differs from the recorded one.
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

// readWALFormat reads the geometry marker; an absent or all-zero marker reports present=false, and any other unrecognised content is fatal.
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
	// A well-formed marker of another version is reported as unsupported, not malformed.
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

// writeWALFormat writes the marker to a temp file, fsyncs, renames it into place and fsyncs the directory; errors unlink the temp file. noSync skips syncs but not the rename.
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

// isAllZero reports whether b has no non-zero byte; an empty slice qualifies.
func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
