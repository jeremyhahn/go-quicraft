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

package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSnapshotOption_Validate_CompactionIndexAlwaysRefused checks that any
// non-zero CompactionIndex is refused, with or without the overhead fields,
// and that options without a CompactionIndex are accepted.
func TestSnapshotOption_Validate_CompactionIndexAlwaysRefused(t *testing.T) {
	tests := []struct {
		name    string
		opt     SnapshotOption
		refused bool
	}{
		{"index-50-alone", SnapshotOption{CompactionIndex: 50}, true},
		{"index-1-alone", SnapshotOption{CompactionIndex: 1}, true},
		{"index-with-export", SnapshotOption{CompactionIndex: 50, ExportPath: t.TempDir(), Exported: true}, true},
		{"index-with-overhead", SnapshotOption{CompactionIndex: 50, CompactionOverhead: 100}, true},
		{"index-with-override", SnapshotOption{CompactionIndex: 50, OverrideCompactionOverhead: true}, true},
		{"zero-value", SnapshotOption{}, false},
		{"default", DefaultSnapshotOption, false},
		{"overhead-override", SnapshotOption{CompactionOverhead: 100, OverrideCompactionOverhead: true}, false},
		{"export-only", SnapshotOption{ExportPath: t.TempDir(), Exported: true}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opt.Validate()
			if !tc.refused {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			assertCompactionIndexRefused(t, err)
		})
	}
}

// TestNodeHost_SyncRequestSnapshot_CompactionIndexOnlyRefused checks that
// SyncRequestSnapshot and RequestSnapshot refuse a CompactionIndex set
// without any overhead field instead of silently ignoring it.
func TestNodeHost_SyncRequestSnapshot_CompactionIndexOnlyRefused(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	startTestShard(t, nh, shardID, 1)

	opt := SnapshotOption{CompactionIndex: 50}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := nh.SyncRequestSnapshot(ctx, shardID, opt)
	assertCompactionIndexRefused(t, err)
	if idx != 0 {
		t.Errorf("SyncRequestSnapshot index = %d, want 0", idx)
	}

	rs, err := nh.RequestSnapshot(shardID, opt, 5*time.Second)
	assertCompactionIndexRefused(t, err)
	if rs != nil {
		rs.Release()
		t.Error("RequestSnapshot returned a RequestState, want nil")
	}
}

func assertCompactionIndexRefused(t *testing.T, err error) {
	t.Helper()
	var target *CompactionIndexNotSupportedError
	if !errors.As(err, &target) {
		t.Fatalf("err = %v, want *CompactionIndexNotSupportedError", err)
	}
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("errors.Is(%v, ErrInvalidOption) = false, want true", err)
	}
}
