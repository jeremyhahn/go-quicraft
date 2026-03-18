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
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SnapshotOption Validation
// ---------------------------------------------------------------------------

func TestSnapshotOption_Validate_ZeroValue(t *testing.T) {
	opt := SnapshotOption{}
	if err := opt.Validate(); err != nil {
		t.Fatalf("zero-value SnapshotOption should be valid, got: %v", err)
	}
}

func TestSnapshotOption_Validate_AbsoluteExportPath(t *testing.T) {
	opt := SnapshotOption{
		ExportPath: "/tmp/export-test",
	}
	if err := opt.Validate(); err != nil {
		t.Fatalf("absolute ExportPath should be valid, got: %v", err)
	}
}

func TestSnapshotOption_Validate_RelativeExportPath(t *testing.T) {
	opt := SnapshotOption{
		ExportPath: "relative/path",
	}
	err := opt.Validate()
	if err == nil {
		t.Fatal("relative ExportPath should fail validation")
	}
	var optErr *SnapshotOptionError
	if !errors.As(err, &optErr) {
		t.Fatalf("expected *SnapshotOptionError, got %T: %v", err, err)
	}
	if optErr.Field != "ExportPath" {
		t.Errorf("SnapshotOptionError.Field = %q, want %q", optErr.Field, "ExportPath")
	}
}

func TestSnapshotOption_Validate_EmptyExportPath(t *testing.T) {
	opt := SnapshotOption{
		ExportPath: "",
	}
	if err := opt.Validate(); err != nil {
		t.Fatalf("empty ExportPath should be valid (means no export), got: %v", err)
	}
}

func TestSnapshotOption_IsExport_WithExportPath(t *testing.T) {
	opt := SnapshotOption{ExportPath: "/tmp/export"}
	if !opt.IsExport() {
		t.Error("IsExport() should return true when ExportPath is set")
	}
}

func TestSnapshotOption_IsExport_WithExportedFlag(t *testing.T) {
	opt := SnapshotOption{Exported: true}
	if !opt.IsExport() {
		t.Error("IsExport() should return true when Exported is true")
	}
}

func TestSnapshotOption_IsExport_ZeroValue(t *testing.T) {
	opt := SnapshotOption{}
	if opt.IsExport() {
		t.Error("IsExport() should return false for zero-value SnapshotOption")
	}
}

func TestSnapshotOption_DefaultSnapshotOption(t *testing.T) {
	if DefaultSnapshotOption.ExportPath != "" {
		t.Error("DefaultSnapshotOption.ExportPath should be empty")
	}
	if DefaultSnapshotOption.CompactionOverhead != 0 {
		t.Error("DefaultSnapshotOption.CompactionOverhead should be 0")
	}
	if DefaultSnapshotOption.OverrideCompactionOverhead {
		t.Error("DefaultSnapshotOption.OverrideCompactionOverhead should be false")
	}
	if DefaultSnapshotOption.Exported {
		t.Error("DefaultSnapshotOption.Exported should be false")
	}
}

func TestSnapshotOptionError_Is(t *testing.T) {
	err := &SnapshotOptionError{Field: "ExportPath", Reason: "must be absolute"}
	if !errors.Is(err, ErrInvalidSnapshotOption) {
		t.Error("SnapshotOptionError should match ErrInvalidSnapshotOption via errors.Is")
	}
}

func TestSnapshotOptionError_Error(t *testing.T) {
	err := &SnapshotOptionError{Field: "ExportPath", Reason: "must be an absolute path"}
	msg := err.Error()
	if msg == "" {
		t.Fatal("SnapshotOptionError.Error() should not be empty")
	}
}

// ---------------------------------------------------------------------------
// Host.RequestSnapshot with SnapshotOption
// ---------------------------------------------------------------------------

func TestHost_RequestSnapshot_DefaultOption(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	h.StartShard(members, false, testCreateFunc, scfg)

	// Zero-value option should work identically to the previous no-option behavior.
	rs, err := h.RequestSnapshot(context.Background(), 1, SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot with default option failed: %v", err)
	}
	if rs == nil {
		t.Fatal("RequestSnapshot returned nil RequestState")
	}
	rs.Release()
}

func TestHost_RequestSnapshot_InvalidExportPath(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	h.StartShard(members, false, testCreateFunc, scfg)

	// Relative export path should be rejected at validation time.
	_, err := h.RequestSnapshot(context.Background(), 1, SnapshotOption{
		ExportPath: "relative/path",
	})
	if err == nil {
		t.Fatal("RequestSnapshot should fail with relative ExportPath")
	}
	var optErr *SnapshotOptionError
	if !errors.As(err, &optErr) {
		t.Fatalf("expected *SnapshotOptionError, got %T: %v", err, err)
	}
}

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

func TestHost_RequestSnapshot_ShardNotFound(t *testing.T) {
	h := newTestHost(t)

	_, err := h.RequestSnapshot(context.Background(), 999, SnapshotOption{})
	if !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("RequestSnapshot error = %v, want ErrShardNotFound", err)
	}
}

func TestHost_RequestSnapshot_ClosedHost(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	h.Close()

	_, err = h.RequestSnapshot(context.Background(), 1, SnapshotOption{})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("RequestSnapshot error = %v, want ErrClosed", err)
	}
}

// ---------------------------------------------------------------------------
// Host.SyncRequestSnapshot with SnapshotOption
// ---------------------------------------------------------------------------

func TestHost_SyncRequestSnapshot_WithOption_ClosedHost(t *testing.T) {
	h := newTestHost(t)
	h.closed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("SyncRequestSnapshot closed: got %v, want ErrClosed", err)
	}
}

func TestHost_SyncRequestSnapshot_WithOption_ShardNotFound(t *testing.T) {
	h := newTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := h.SyncRequestSnapshot(ctx, 999, SnapshotOption{})
	if !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("SyncRequestSnapshot not found: got %v, want ErrShardNotFound", err)
	}
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
}

func TestHost_SyncRequestSnapshot_WithOption_CompactionOverride(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	h.StartShard(members, false, testCreateFunc, scfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This will submit the request; the snapshot pool will process it.
	// We cannot easily wait for the full pipeline without a running raft,
	// but the key assertion is that the option is accepted without error
	// and the request is properly submitted.
	_, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{
		CompactionOverhead:         500,
		OverrideCompactionOverhead: true,
	})
	// Expected: either success or timeout (no leader for single node
	// without running the engine tick loop). Both are acceptable here;
	// the key test is that the option validation passed and the request
	// was submitted to the engine.
	if err != nil && !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrCanceled) {
		t.Fatalf("SyncRequestSnapshot with CompactionOverride: unexpected error %v", err)
	}
}
