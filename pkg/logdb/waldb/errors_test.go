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
	"errors"
	"testing"
)

func TestWALWriteError_Error(t *testing.T) {
	t.Run("write error only", func(t *testing.T) {
		writeErr := errors.New("disk I/O failed")
		e := &WALWriteError{WriteErr: writeErr}
		want := "waldb: WAL write failed: disk I/O failed"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("write error with truncate error", func(t *testing.T) {
		writeErr := errors.New("write failed")
		truncateErr := errors.New("truncate failed")
		e := &WALWriteError{
			WriteErr:    writeErr,
			TruncateErr: truncateErr,
		}
		want := "waldb: WAL write failed: write failed; truncate failed: truncate failed"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("write error with switch error", func(t *testing.T) {
		writeErr := errors.New("write failed")
		switchErr := errors.New("cannot open new segment")
		e := &WALWriteError{
			WriteErr:  writeErr,
			SwitchErr: switchErr,
		}
		want := "waldb: WAL write failed: write failed; segment switch failed: cannot open new segment"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("write error with both truncate and switch errors", func(t *testing.T) {
		writeErr := errors.New("write failed")
		truncateErr := errors.New("truncate failed")
		switchErr := errors.New("switch failed")
		e := &WALWriteError{
			WriteErr:    writeErr,
			TruncateErr: truncateErr,
			SwitchErr:   switchErr,
		}
		want := "waldb: WAL write failed: write failed; truncate failed: truncate failed; segment switch failed: switch failed"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestWALWriteError_Unwrap(t *testing.T) {
	t.Run("returns underlying write error", func(t *testing.T) {
		writeErr := errors.New("underlying write error")
		e := &WALWriteError{WriteErr: writeErr}
		if got := e.Unwrap(); got != writeErr {
			t.Errorf("Unwrap() = %v, want %v", got, writeErr)
		}
	})
}

func TestWALEncryptError_Error(t *testing.T) {
	t.Run("formats encryption error message", func(t *testing.T) {
		inner := errors.New("cipher: message authentication failed")
		e := &WALEncryptError{Err: inner}
		want := "waldb: WAL record encryption failed: cipher: message authentication failed"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestWALEncryptError_Unwrap(t *testing.T) {
	t.Run("returns underlying error", func(t *testing.T) {
		inner := errors.New("encryption failed")
		e := &WALEncryptError{Err: inner}
		if got := e.Unwrap(); got != inner {
			t.Errorf("Unwrap() = %v, want %v", got, inner)
		}
	})
}

func TestWALDecryptError_Error(t *testing.T) {
	t.Run("formats decryption error with segment ID", func(t *testing.T) {
		inner := errors.New("authentication tag mismatch")
		e := &WALDecryptError{SegmentID: 42, Err: inner}
		want := "waldb: WAL record decryption failed for segment 42: authentication tag mismatch"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestWALDecryptError_Unwrap(t *testing.T) {
	t.Run("returns underlying error", func(t *testing.T) {
		inner := errors.New("decryption error")
		e := &WALDecryptError{SegmentID: 1, Err: inner}
		if got := e.Unwrap(); got != inner {
			t.Errorf("Unwrap() = %v, want %v", got, inner)
		}
	})
}

func TestDirSyncError_Error(t *testing.T) {
	t.Run("formats directory sync error", func(t *testing.T) {
		inner := errors.New("permission denied")
		e := &DirSyncError{Dir: "/var/lib/wal", Err: inner}
		want := "waldb: failed to sync directory /var/lib/wal: permission denied"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestDirSyncError_Unwrap(t *testing.T) {
	t.Run("returns underlying error", func(t *testing.T) {
		inner := errors.New("fsync failed")
		e := &DirSyncError{Dir: "/tmp", Err: inner}
		if got := e.Unwrap(); got != inner {
			t.Errorf("Unwrap() = %v, want %v", got, inner)
		}
	})
}

func TestDiskUsageError_Error(t *testing.T) {
	t.Run("formats disk usage error", func(t *testing.T) {
		inner := errors.New("statfs: no such file or directory")
		e := &DiskUsageError{Dir: "/data/wal", Err: inner}
		want := "waldb: failed to query disk usage for /data/wal: statfs: no such file or directory"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
}

func TestDiskUsageError_Unwrap(t *testing.T) {
	t.Run("returns underlying error", func(t *testing.T) {
		inner := errors.New("disk error")
		e := &DiskUsageError{Dir: "/tmp", Err: inner}
		if got := e.Unwrap(); got != inner {
			t.Errorf("Unwrap() = %v, want %v", got, inner)
		}
	})
}

func TestSegmentDiscardError(t *testing.T) {
	t.Run("error message includes segment ID", func(t *testing.T) {
		underlying := errors.New("truncate failed")
		e := &SegmentDiscardError{SegmentID: 42, Err: underlying}
		want := "waldb: failed to discard pending data for segment 42: truncate failed"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("unwrap returns underlying error", func(t *testing.T) {
		underlying := errors.New("seek failed")
		e := &SegmentDiscardError{SegmentID: 1, Err: underlying}
		if !errors.Is(e, underlying) {
			t.Error("Unwrap should return the underlying error")
		}
	})
}

func TestBatchWriteError(t *testing.T) {
	t.Run("error message with no discard errors", func(t *testing.T) {
		writeErr := errors.New("ENOSPC")
		e := &BatchWriteError{WriteErr: writeErr}
		want := "waldb: batch write failed: ENOSPC"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("error message with discard errors", func(t *testing.T) {
		writeErr := errors.New("ENOSPC")
		discardErr := errors.New("truncate failed")
		e := &BatchWriteError{
			WriteErr:      writeErr,
			DiscardErrors: []error{discardErr},
		}
		want := "waldb: batch write failed: ENOSPC; discard failed: truncate failed"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("unwrap returns write error", func(t *testing.T) {
		writeErr := errors.New("write failed")
		e := &BatchWriteError{WriteErr: writeErr}
		if !errors.Is(e, writeErr) {
			t.Error("Unwrap should return the write error")
		}
	})
}

func TestWriteBootstrapBatchModeError_Error(t *testing.T) {
	t.Run("returns descriptive message", func(t *testing.T) {
		e := &WriteBootstrapBatchModeError{}
		want := "waldb: writeBootstrap called in batchMode; bootstrap writes require inline fsync"
		if got := e.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("errors.As matches WriteBootstrapBatchModeError", func(t *testing.T) {
		var err error = &WriteBootstrapBatchModeError{}
		var target *WriteBootstrapBatchModeError
		if !errors.As(err, &target) {
			t.Error("errors.As should match WriteBootstrapBatchModeError")
		}
	})
}
