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
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SnapshotOption.IsExport
// ---------------------------------------------------------------------------

func TestSnapshotOption_IsExport(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  SnapshotOption
		want bool
	}{
		{"zero value", SnapshotOption{}, false},
		{"export path set", SnapshotOption{ExportPath: "/tmp/export"}, true},
		{"exported flag set", SnapshotOption{Exported: true}, true},
	} {
		if got := tc.opt.IsExport(); got != tc.want {
			t.Errorf("%s: IsExport() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Host.RequestSnapshot with SnapshotOption
// ---------------------------------------------------------------------------

func TestHost_RequestSnapshot_ExportPath(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	h.StartShard(members, false, testCreateFunc, scfg)

	exportDir := t.TempDir()
	rs, err := h.RequestSnapshot(context.Background(), 1, SnapshotOption{
		ExportPath: exportDir,
	})
	if err != nil {
		t.Fatalf("RequestSnapshot with ExportPath failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil RequestState")
	}
	rs.Release()
}

func TestHost_RequestSnapshot_CompactionOverhead(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	h.StartShard(members, false, testCreateFunc, scfg)

	// Override compaction overhead should be accepted without error.
	rs, err := h.RequestSnapshot(context.Background(), 1, SnapshotOption{
		CompactionOverhead:         1000,
		OverrideCompactionOverhead: true,
	})
	if err != nil {
		t.Fatalf("RequestSnapshot with CompactionOverhead failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil RequestState")
	}
	rs.Release()
}

func TestHost_SyncRequestSnapshot_WithOption_InvalidExportPath(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	h.StartShard(members, false, testCreateFunc, scfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{
		ExportPath: "not-absolute",
	})
	if err == nil {
		t.Fatal("SyncRequestSnapshot should fail with relative ExportPath")
	}
	var optErr *SnapshotOptionError
	if !errors.As(err, &optErr) {
		t.Fatalf("expected *SnapshotOptionError, got %T: %v", err, err)
	}
	if optErr.Field != "ExportPath" {
		t.Errorf("SnapshotOptionError.Field = %q, want %q", optErr.Field, "ExportPath")
	}
	if !errors.Is(err, ErrInvalidSnapshotOption) {
		t.Errorf("errors.Is(%v, ErrInvalidSnapshotOption) = false", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "ExportPath") {
		t.Errorf("error message %q does not name the field", msg)
	}
}
