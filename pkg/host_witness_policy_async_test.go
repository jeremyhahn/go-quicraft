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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// asyncWitnessPolicyRun records what one RequestUpdateWitnessPolicy call
// returned: how many RequestStates the call handed out and how many of them
// the test released.
type asyncWitnessPolicyRun struct {
	states   int
	releases int
}

// requestPolicy proposes policy through RequestUpdateWitnessPolicy on h and
// waits for the RequestState's Result. Every RequestState the call returns
// is released exactly once: it is checked to be unreleased before the
// test's single Release and released after it.
func (r *asyncWitnessPolicyRun) requestPolicy(ctx context.Context, t *testing.T, h *Host, max int, rfrm bool, ccid uint64) error {
	t.Helper()
	rs, err := h.RequestUpdateWitnessPolicy(ctx, witnessPolicyShard, config.WitnessConfig{
		MaxWitnesses:               max,
		RequireFullReplicaMajority: config.BoolPtr(rfrm),
	}, ccid)
	if err != nil {
		if rs != nil {
			t.Fatalf("RequestUpdateWitnessPolicy returned a RequestState with the error %v", err)
		}
		return err
	}
	if rs == nil {
		t.Fatal("RequestUpdateWitnessPolicy returned neither a RequestState nor an error")
	}
	r.states++
	_, err = awaitResult(ctx, rs)
	if rs.released.Load() {
		t.Fatal("the RequestState was released before the caller released it")
	}
	rs.Release()
	r.releases++
	if !rs.released.Load() {
		t.Fatal("the RequestState is not released after Release")
	}
	return err
}

// TestWitnessPolicy_AsyncRequestCompletesAndRefusesAViolatedPolicyOrAStaleIndex
// checks docs/membership-changes/api.md (Capability: change the witness
// policy) for the async form: RequestUpdateWitnessPolicy hands back a
// RequestState whose result completes with the effect of the sync form, a
// policy the current membership violates is refused with ErrTooManyWitnesses
// and leaves the old policy in place, and a stale configChangeIndex is
// refused with *StaleConfigChangeError.
func TestWitnessPolicy_AsyncRequestCompletesAndRefusesAViolatedPolicyOrAStaleIndex(t *testing.T) {
	c := newWitnessPolicyCluster(t, 2)

	// Seed a tight policy through the sync form, so (a) can loosen it.
	if err := c.updatePolicy(1, true); err != nil {
		t.Fatalf("seeding MaxWitnesses=1 RequireFullReplicaMajority=true: %v", err)
	}
	c.expectEverywhere("after the seed", witnessPolicyView{max: 1, rfrm: true})

	// (a) A loosening policy completes through its RequestState and every
	// replica adopts it.
	var a asyncWitnessPolicyRun
	if err := c.change(func(ctx context.Context, h *Host, ccid uint64) error {
		return a.requestPolicy(ctx, t, h, 2, false, ccid)
	}); err != nil {
		t.Fatalf("(a) RequestUpdateWitnessPolicy(MaxWitnesses=2, RequireFullReplicaMajority=false): %v", err)
	}
	if a.states == 0 || a.releases != a.states {
		t.Fatalf("(a) %d RequestStates returned, %d released: want each released once", a.states, a.releases)
	}
	c.expectEverywhere("(a) after the async update", witnessPolicyView{max: 2})

	// Admit two witnesses, which the loosened policy allows.
	if err := c.addWitness(0, 4); err != nil {
		t.Fatalf("adding the first witness under MaxWitnesses=2: %v", err)
	}
	if err := c.addWitness(1, 5); err != nil {
		t.Fatalf("adding the second witness under MaxWitnesses=2: %v", err)
	}
	c.expectEverywhere("with two witnesses", witnessPolicyView{max: 2, witness: 2})

	// (b) Tightening below the current witness count is refused, from the
	// call or from the result, and every replica keeps the old policy.
	var b asyncWitnessPolicyRun
	err := c.change(func(ctx context.Context, h *Host, ccid uint64) error {
		return b.requestPolicy(ctx, t, h, 1, false, ccid)
	})
	if !errors.Is(err, ErrTooManyWitnesses) && !errors.Is(err, ErrBreaksFullReplicaMajority) {
		t.Fatalf("(b) tightening to MaxWitnesses=1 with two witnesses: got %v, want ErrTooManyWitnesses or ErrBreaksFullReplicaMajority", err)
	}
	if !errors.Is(err, ErrTooManyWitnesses) {
		t.Fatalf("(b) tightening the witness cap: got %v, want ErrTooManyWitnesses", err)
	}
	if b.releases != b.states {
		t.Fatalf("(b) %d RequestStates returned, %d released: want each released once", b.states, b.releases)
	}
	c.expectEverywhere("(b) after the refused tightening", witnessPolicyView{max: 2, witness: 2})

	// (c) A non-zero configChangeIndex read before the last change is
	// refused and the policy stays as the last change left it.
	var cr asyncWitnessPolicyRun
	stale := uint64(0)
	err = c.change(func(ctx context.Context, h *Host, ccid uint64) error {
		if stale == 0 {
			stale = ccid
			if err := cr.requestPolicy(ctx, t, h, 3, false, ccid); err != nil {
				stale = 0
				return err
			}
		}
		return cr.requestPolicy(ctx, t, h, 2, true, stale)
	})
	if stale == 0 {
		t.Fatalf("(c) the update that moves the index: %v", err)
	}
	var sce *StaleConfigChangeError
	if !errors.As(err, &sce) {
		t.Fatalf("(c) an update with the configChangeIndex %d from before the last change: got %v, want *StaleConfigChangeError", stale, err)
	}
	if cr.states == 0 || cr.releases != cr.states {
		t.Fatalf("(c) %d RequestStates returned, %d released: want each released once", cr.states, cr.releases)
	}
	c.expectEverywhere("(c) after the stale update", witnessPolicyView{max: 3, witness: 2})
}
