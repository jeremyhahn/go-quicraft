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
	"fmt"
	"testing"
)

// TestIsCleanRefusal pins IsCleanRefusal to its doc comment: only the cause,
// alone or wrapped, is clean; an error that also carries a disk fault
// (ErrDBPoisoned, BatchWriteError, WALWriteError) never is.
func TestIsCleanRefusal(t *testing.T) {
	cause := errors.New("refused before write")
	batch := &BatchWriteError{WriteErr: cause}
	wal := &WALWriteError{WriteErr: cause}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"cause", cause, true},
		{"wrapped cause", fmt.Errorf("x: %w", cause), true},
		{"nil", nil, false},
		{"unrelated", errors.New("unrelated"), false},
		{"joined with poisoned", errors.Join(cause, ErrDBPoisoned), false},
		{"BatchWriteError", batch, false},
		{"WALWriteError", wal, false},
		{"wrapped BatchWriteError", fmt.Errorf("x: %w", batch), false},
		{"wrapped WALWriteError", fmt.Errorf("x: %w", wal), false},
		{"BatchWriteError on wrapped cause", &BatchWriteError{WriteErr: fmt.Errorf("x: %w", cause)}, false},
		{"WALWriteError on wrapped cause", &WALWriteError{WriteErr: fmt.Errorf("x: %w", cause)}, false},
		{"BatchWriteError discard carries cause", &BatchWriteError{WriteErr: errors.New("w"), DiscardErrors: []error{cause}}, false},
	}
	for _, tt := range tests {
		if got := IsCleanRefusal(tt.err, cause); got != tt.want {
			t.Errorf("%s: IsCleanRefusal(%v) = %v, want %v", tt.name, tt.err, got, tt.want)
		}
	}
}
