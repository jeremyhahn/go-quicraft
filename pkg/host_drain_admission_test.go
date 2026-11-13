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
	"testing"
	"time"
)

// TestHost_Drain_AdmissionAfterDrainIsRefused reproduces the check-then-count
// window of #1084 deterministically. A proposer passes Propose's draining
// check, is preempted, and Drain runs to completion: it raises the flag, sees
// no pending operation and returns nil. The proposer then resumes and reaches
// the registration that counts the operation. That registration must refuse
// with ErrDraining and leave pendingOps at 0; admitting it would make Drain's
// nil a lie, because an operation is pending after Drain said none was.
func TestHost_Drain_AdmissionAfterDrainIsRefused(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer func() { _ = h.Close() }()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	// The preempted proposer has already passed the draining check and built
	// its proposal: everything up to the registration is done.
	ns := testLoadNode(h, 1)
	engNode := h.engine.GetNode(1)
	key := nextRequestKey()
	deadline := time.Now().Add(time.Minute)
	rs := newRequestState(ns.requestPool, key, deadline, false)
	pp := &pendingProposal{rs: rs, deadline: deadline}

	// Drain runs entirely inside the window.
	if err := h.Drain(); err != nil {
		t.Fatalf("Drain = %v, want nil on an idle host", err)
	}

	// The proposer resumes.
	if err := h.registerPendingProposal(ns, engNode, key, pp); !errors.Is(err, ErrDraining) {
		t.Errorf("registration after Drain returned nil = %v, want ErrDraining", err)
	}
	if n := h.pendingOps.Load(); n != 0 {
		t.Errorf("pendingOps = %d after Drain returned nil, want 0: an operation was admitted after Drain", n)
	}
	if _, ok := ns.pendingProposals.Load(key); ok {
		t.Error("the refused proposal is still in the pending map")
	}
}
