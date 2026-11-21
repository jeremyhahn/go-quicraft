// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"errors"
	"fmt"
	"testing"
)

// TestDataDirChangedError_IsErrDataDirChanged pins that StartShard's
// changed-DataDir refusal matches the exported ErrDataDirChanged sentinel,
// directly and through a %w wrap, so a caller need not read its message.
func TestDataDirChangedError_IsErrDataDirChanged(t *testing.T) {
	err := error(&dataDirChangedError{
		ShardID:   7,
		ReplicaID: 3,
		Recorded:  "/data/a",
		Requested: "/data/b",
	})
	if !errors.Is(err, ErrDataDirChanged) {
		t.Fatalf("errors.Is(%v, ErrDataDirChanged) = false, want true", err)
	}
	wrapped := fmt.Errorf("start shard: %w", err)
	if !errors.Is(wrapped, ErrDataDirChanged) {
		t.Fatalf("errors.Is(%v, ErrDataDirChanged) = false, want true", wrapped)
	}
	if errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("errors.Is(%v, ErrInvalidConfig) = true, want false", err)
	}
	var dc *dataDirChangedError
	if !errors.As(wrapped, &dc) || dc.Recorded != "/data/a" || dc.Requested != "/data/b" {
		t.Fatalf("errors.As(%v) = %+v, want the original fields", wrapped, dc)
	}
}
