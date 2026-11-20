// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
)

// TestTranslateProposalError_StaleConfigChange verifies that the leader
// worker's refusal of a bound add reaches the caller as the public
// *StaleConfigChangeError, carrying the shard and both indexes, and not as
// the internal raft error or a pending-change rejection.
func TestTranslateProposalError_StaleConfigChange(t *testing.T) {
	h := &Host{}
	ns := &nodeState{shardID: 9}
	in := fmt.Errorf("propose: %w", &raft.StaleConfigChangeError{Expected: 12, Got: 7})
	err := h.translateProposalError(ns, in)
	if !errors.Is(err, ErrStaleConfigChange) {
		t.Fatalf("translateProposalError = %v, want ErrStaleConfigChange", err)
	}
	if errors.Is(err, ErrRejected) {
		t.Fatalf("translateProposalError = %v, must not be ErrRejected", err)
	}
	var stale *StaleConfigChangeError
	if !errors.As(err, &stale) {
		t.Fatalf("translateProposalError = %T, want *StaleConfigChangeError", err)
	}
	if stale.ShardID != 9 || stale.Expected != 12 || stale.Got != 7 {
		t.Fatalf("got %+v, want ShardID 9, Expected 12, Got 7", *stale)
	}
}
