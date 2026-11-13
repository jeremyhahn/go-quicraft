// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

import (
	"context"
	"testing"
)

// TestContextBindingValues checks Resolved, Binds and String for every
// ContextBinding constant and for one value past the last constant, against
// their documentation in barrier_binding.go.
func TestContextBindingValues(t *testing.T) {
	cases := []struct {
		binding  ContextBinding
		resolved bool
		binds    bool
		name     string
	}{
		{ContextBindingUnresolved, false, false, "unresolved"},
		{ContextBindingBarrier, true, true, "barrier"},
		{ContextBindingDelegated, true, true, "delegated"},
		{ContextBindingUnsupported, true, false, "unsupported"},
		{ContextBindingPlaintext, true, false, "plaintext"},
		{ContextBindingPlaintext + 1, true, false, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.binding.Resolved(); got != tc.resolved {
				t.Errorf("ContextBinding(%d).Resolved() = %v, want %v", uint8(tc.binding), got, tc.resolved)
			}
			if got := tc.binding.Binds(); got != tc.binds {
				t.Errorf("ContextBinding(%d).Binds() = %v, want %v", uint8(tc.binding), got, tc.binds)
			}
			if got := tc.binding.String(); got != tc.name {
				t.Errorf("ContextBinding(%d).String() = %q, want %q", uint8(tc.binding), got, tc.name)
			}
		})
	}
}

// TestContextBindingValuesBarrierLifecycle checks that a software barrier's
// ContextBinding is unresolved whenever the barrier is not unsealed and is
// ContextBindingBarrier whenever it is.
func TestContextBindingValuesBarrierLifecycle(t *testing.T) {
	ctx := context.Background()
	b := MustNewBarrier(testBarrierConfig(1))
	s := testStrategy(t)

	requireContextBinding(ctx, t, b, "before Initialize", ContextBindingUnresolved)

	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	requireContextBinding(ctx, t, b, "after Initialize", ContextBindingBarrier)

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	requireContextBinding(ctx, t, b, "after Seal", ContextBindingUnresolved)

	if err := b.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	requireContextBinding(ctx, t, b, "after Unseal", ContextBindingBarrier)
}

// requireContextBinding fails the test unless b reports want, and unless the
// derived predicates SupportsContextAAD and ContextBindingResolved agree with
// want.Binds and want.Resolved.
func requireContextBinding(ctx context.Context, t *testing.T, b *Barrier, phase string, want ContextBinding) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("%s: context: %v", phase, err)
	}
	if got := b.ContextBinding(); got != want {
		t.Fatalf("%s: ContextBinding() = %v, want %v", phase, got, want)
	}
	if got := b.SupportsContextAAD(); got != want.Binds() {
		t.Errorf("%s: SupportsContextAAD() = %v, want %v", phase, got, want.Binds())
	}
	if got := b.ContextBindingResolved(); got != want.Resolved() {
		t.Errorf("%s: ContextBindingResolved() = %v, want %v", phase, got, want.Resolved())
	}
}
