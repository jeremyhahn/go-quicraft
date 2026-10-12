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

package quicraft

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// Only the barrier's clean sealed refusal takes the commit worker's hold
// (#296); a sealed error that also carries a disk fault still latches.
func TestIsSealedLogWrite(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"bare sealed", ErrBarrierSealed, true},
		{"wal encrypt refusal", &waldb.WALEncryptError{Err: ErrBarrierSealed}, true},
		{"wrapped refusal", fmt.Errorf("save: %w", &waldb.WALEncryptError{Err: ErrBarrierSealed}), true},
		{"poisoned log", errors.Join(waldb.ErrDBPoisoned, ErrBarrierSealed), false},
		{"failed rollback", &waldb.BatchWriteError{WriteErr: ErrBarrierSealed, DiscardErrors: []error{errors.New("truncate")}}, false},
		{"failed truncate", &waldb.WALWriteError{WriteErr: ErrBarrierSealed, TruncateErr: errors.New("truncate")}, false},
		{"disk fault", errors.New("fsync: input/output error"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSealedLogWrite(tc.err); got != tc.want {
				t.Fatalf("isSealedLogWrite(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
