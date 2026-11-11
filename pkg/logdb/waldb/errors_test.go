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

// TestErrorTypes checks, for each typed error of the package, the exact
// Error() message and that errors.Is reaches the wrapped cause through Unwrap.
func TestErrorTypes(t *testing.T) {
	writeErr := errors.New("write failed")
	inner := errors.New("inner")
	tests := []struct {
		name  string
		err   error
		want  string
		cause error // nil: no cause to check
	}{
		{"WALWriteError write only", &WALWriteError{WriteErr: writeErr},
			"waldb: WAL write failed: write failed", writeErr},
		{"WALWriteError with truncate", &WALWriteError{WriteErr: writeErr, TruncateErr: errors.New("truncate failed")},
			"waldb: WAL write failed: write failed; truncate failed: truncate failed", writeErr},
		{"WALWriteError with switch", &WALWriteError{WriteErr: writeErr, SwitchErr: errors.New("cannot open new segment")},
			"waldb: WAL write failed: write failed; segment switch failed: cannot open new segment", writeErr},
		{"WALWriteError with truncate and switch", &WALWriteError{WriteErr: writeErr, TruncateErr: errors.New("truncate failed"), SwitchErr: errors.New("switch failed")},
			"waldb: WAL write failed: write failed; truncate failed: truncate failed; segment switch failed: switch failed", writeErr},
		{"WALEncryptError", &WALEncryptError{Err: inner},
			"waldb: WAL record encryption failed: inner", inner},
		{"WALDecryptError", &WALDecryptError{SegmentID: 42, Err: inner},
			"waldb: WAL record decryption failed for segment 42: inner", inner},
		{"DirSyncError", &DirSyncError{Dir: "/var/lib/wal", Err: inner},
			"waldb: failed to sync directory /var/lib/wal: inner", inner},
		{"DiskUsageError", &DiskUsageError{Dir: "/data/wal", Err: inner},
			"waldb: failed to query disk usage for /data/wal: inner", inner},
		{"SegmentDiscardError", &SegmentDiscardError{SegmentID: 42, Err: inner},
			"waldb: failed to discard pending data for segment 42: inner", inner},
		{"BatchWriteError no discard errors", &BatchWriteError{WriteErr: writeErr},
			"waldb: batch write failed: write failed", writeErr},
		{"BatchWriteError with discard errors", &BatchWriteError{WriteErr: errors.New("ENOSPC"), DiscardErrors: []error{errors.New("truncate failed")}},
			"waldb: batch write failed: ENOSPC; discard failed: truncate failed", nil},
		{"WriteBootstrapBatchModeError", &WriteBootstrapBatchModeError{},
			"waldb: writeBootstrap called in batchMode; bootstrap writes require inline fsync", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
			if tc.cause != nil && !errors.Is(tc.err, tc.cause) {
				t.Errorf("errors.Is(%T, cause) = false, want true", tc.err)
			}
		})
	}
}
