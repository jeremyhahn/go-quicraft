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

package seal

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// Nothing in this module implements ShareSplitter/ShareAccumulator or their
// protected counterparts — custody lives in go-qrdb, and these are shapes an
// embedder satisfies. That is exactly why the contract is pinned here: a
// signature drifting away from what consumers implement would otherwise be
// invisible from inside this repository until it broke their build.
//
// The fixtures below are also the answer to #35's first acceptance criterion,
// "a consumer can implement the protected interface without ever materialising a
// share as a string" — demonstrated by construction rather than asserted in
// prose. There is no `string` anywhere in this file's custody path.

// protectedSplitter implements ProtectedShareSplitter by delegating to the
// primitive. Share values go from the polynomial straight into locked memory.
type protectedSplitter struct{}

func (protectedSplitter) SplitProtected(secret []byte, threshold, total int) ([]*shamir.ProtectedShare, error) {
	return shamir.SplitProtected(secret, threshold, total)
}

// protectedAccumulator implements ProtectedShareAccumulator, taking ownership of
// every share handed to it exactly as the interface documents.
type protectedAccumulator struct {
	threshold int
	shares    []*shamir.ProtectedShare
	deadline  time.Time
}

func newProtectedAccumulator(threshold int) *protectedAccumulator {
	return &protectedAccumulator{threshold: threshold, deadline: time.Now().Add(DefaultQuorumTTL)}
}

func (a *protectedAccumulator) AddProtectedShare(_ context.Context, share *shamir.ProtectedShare) (*QuorumProgress, error) {
	if share == nil {
		return nil, errors.New("nil share")
	}
	// Ownership transferred: a rejected share must still be destroyed here, or
	// it leaks a locked allocation for the life of the process.
	//
	// And on expiry the accumulator releases EVERYTHING it holds, not merely
	// the share being refused. The contract makes it responsible for accepted
	// shares "whether or not the quorum ever completes", and a timed-out
	// ceremony is the main way one does not complete — leaving the earlier
	// shares locked until some caller remembers to Reset would be exactly the
	// leak this type exists to prevent.
	if a.Expired() {
		share.Destroy()
		a.Reset()
		return nil, errors.New("quorum expired")
	}
	a.shares = append(a.shares, share)
	return a.GetProgress(), nil
}

func (a *protectedAccumulator) GetProgress() *QuorumProgress {
	return &QuorumProgress{
		Required:  a.threshold,
		Submitted: len(a.shares),
		Complete:  len(a.shares) >= a.threshold,
	}
}

func (a *protectedAccumulator) ReconstructProtected(_ context.Context) (*enclave.MemoryEnclave, error) {
	if len(a.shares) < a.threshold {
		return nil, errors.New("threshold not met")
	}
	return shamir.CombineProtected(a.shares)
}

func (a *protectedAccumulator) Reset() {
	for _, s := range a.shares {
		s.Destroy()
	}
	a.shares = nil
	a.deadline = time.Now().Add(DefaultQuorumTTL)
}

func (a *protectedAccumulator) Expired() bool { return time.Now().After(a.deadline) }

// legacySplitter is the string-valued form. It is here to prove #35's second
// acceptance criterion — that existing implementations still compile — which is
// the whole reason this landed as additive siblings rather than a retype.
type legacySplitter struct{}

func (legacySplitter) Split(secret []byte, threshold, total int) ([]string, error) {
	shares, err := shamir.Split(secret, threshold, total) //nolint:staticcheck // SA1019 deliberate: this fixture exists to prove the deprecated shape still compiles
	if err != nil {
		return nil, err
	}
	out := make([]string, len(shares))
	for i, s := range shares {
		out[i] = s.Value //nolint:staticcheck // SA1019 deliberate: same reason as above
	}
	return out, nil
}

// legacyAccumulator pins the other half of the deprecated surface. Without it
// only ShareSplitter would be held in place, and a retype of ShareAccumulator
// would break consumers with nothing here noticing.
type legacyAccumulator struct{ shares []string }

func (a *legacyAccumulator) AddShare(_ context.Context, share string) (*QuorumProgress, error) {
	a.shares = append(a.shares, share)
	return a.GetProgress(), nil
}

func (a *legacyAccumulator) GetProgress() *QuorumProgress {
	return &QuorumProgress{Required: 2, Submitted: len(a.shares), Complete: len(a.shares) >= 2}
}

func (a *legacyAccumulator) Reconstruct(_ context.Context) ([]byte, error) {
	return nil, errors.New("not needed for the conformance pin")
}

func (a *legacyAccumulator) Reset()        { a.shares = nil }
func (a *legacyAccumulator) Expired() bool { return false }

// Signature drift in either family fails the build here rather than in a
// consumer that cannot change these interfaces.
var (
	_ ProtectedShareSplitter    = protectedSplitter{}
	_ ProtectedShareAccumulator = (*protectedAccumulator)(nil)
	_ ShareSplitter             = legacySplitter{}
	_ ShareAccumulator          = (*legacyAccumulator)(nil)
)

func TestProtectedCustodyRoundTripsWithoutEverBecomingAString(t *testing.T) {
	ctx := context.Background()
	secret := []byte("a 32-byte root key for testing..")

	shares, err := protectedSplitter{}.SplitProtected(secret, 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	if len(shares) != 5 {
		t.Fatalf("expected 5 shares, got %d", len(shares))
	}

	acc := newProtectedAccumulator(3)
	defer acc.Reset()

	// Hand over the threshold. Ownership moves with each share; the loop keeps
	// no reference it would later read.
	for _, s := range shares[:3] {
		progress, err := acc.AddProtectedShare(ctx, s)
		if err != nil {
			t.Fatalf("AddProtectedShare: %v", err)
		}
		if progress.Required != 3 {
			t.Fatalf("Required should be the threshold, got %d", progress.Required)
		}
	}
	// The two the accumulator never received are still the caller's to destroy.
	for _, s := range shares[3:] {
		s.Destroy()
	}

	if got := acc.GetProgress(); got.Submitted != 3 {
		t.Fatalf("expected 3 submitted, got %d", got.Submitted)
	}

	recovered, err := acc.ReconstructProtected(ctx)
	if err != nil {
		t.Fatalf("ReconstructProtected: %v", err)
	}
	defer recovered.Destroy()

	// Compared inside the borrow: the reconstructed secret never leaves locked
	// memory to be checked.
	if err := recovered.WithBytes(func(b []byte) error {
		if !bytes.Equal(b, secret) {
			return errors.New("reconstructed secret does not match the original")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestProtectedAccumulatorRefusesBelowThreshold(t *testing.T) {
	ctx := context.Background()
	shares, err := shamir.SplitProtected([]byte("secret"), 3, 5)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	acc := newProtectedAccumulator(3)
	defer acc.Reset()

	for _, s := range shares[:2] {
		if _, err := acc.AddProtectedShare(ctx, s); err != nil {
			t.Fatalf("AddProtectedShare: %v", err)
		}
	}
	for _, s := range shares[2:] {
		s.Destroy()
	}

	if _, err := acc.ReconstructProtected(ctx); err == nil {
		t.Fatal("reconstruct must refuse below the threshold")
	}
}

// Reset is the teardown path a cancelled or timed-out ceremony takes, and it
// must genuinely release the locked memory rather than merely dropping the
// references — that is the property the whole protected family exists for.
func TestProtectedAccumulatorResetDestroysWhatItHolds(t *testing.T) {
	ctx := context.Background()
	shares, err := shamir.SplitProtected([]byte("secret"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	acc := newProtectedAccumulator(2)
	held := shares[0]
	if _, err := acc.AddProtectedShare(ctx, held); err != nil {
		t.Fatalf("AddProtectedShare: %v", err)
	}
	for _, s := range shares[1:] {
		s.Destroy()
	}

	acc.Reset()

	if got := acc.GetProgress(); got.Submitted != 0 {
		t.Fatalf("Reset must clear the accumulated shares, got %d", got.Submitted)
	}
	// A destroyed share reports a nil value; that is the observable signal the
	// wipe actually happened rather than the reference merely being dropped.
	if v := held.Value(); v != nil {
		t.Fatal("Reset must Destroy the shares it took ownership of")
	}
}

// GetProgress must report Complete once the threshold is met. A fixture that
// never sets it would let a consumer copy an accumulator that reports progress
// which never completes, so the field is pinned rather than assumed.
func TestProtectedAccumulatorReportsCompletionAtTheThreshold(t *testing.T) {
	ctx := context.Background()
	shares, err := shamir.SplitProtected([]byte("secret"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	acc := newProtectedAccumulator(2)
	defer acc.Reset()

	p, err := acc.AddProtectedShare(ctx, shares[0])
	if err != nil {
		t.Fatalf("AddProtectedShare: %v", err)
	}
	if p.Complete {
		t.Fatal("must not report Complete below the threshold")
	}

	p, err = acc.AddProtectedShare(ctx, shares[1])
	if err != nil {
		t.Fatalf("AddProtectedShare: %v", err)
	}
	if !p.Complete {
		t.Fatal("must report Complete once the threshold is met")
	}
	shares[2].Destroy()
}

// The TTL path is where the ownership contract is easiest to get wrong: an
// expired ceremony must release everything it holds, not merely the share it is
// refusing. Leaving the earlier shares locked until someone remembers to Reset
// is the leak this type exists to prevent.
func TestProtectedAccumulatorExpiryReleasesEverythingItHolds(t *testing.T) {
	ctx := context.Background()
	shares, err := shamir.SplitProtected([]byte("secret"), 2, 3)
	if err != nil {
		t.Fatalf("SplitProtected: %v", err)
	}
	acc := newProtectedAccumulator(2)

	accepted := shares[0]
	if _, err := acc.AddProtectedShare(ctx, accepted); err != nil {
		t.Fatalf("AddProtectedShare: %v", err)
	}

	// Force the deadline into the past, then submit again.
	acc.deadline = time.Now().Add(-time.Second)
	refused := shares[1]
	if _, err := acc.AddProtectedShare(ctx, refused); err == nil {
		t.Fatal("an expired accumulator must refuse the submission")
	}

	if v := refused.Value(); v != nil {
		t.Fatal("the refused share must be destroyed — ownership transferred with the call")
	}
	if v := accepted.Value(); v != nil {
		t.Fatal("an expired ceremony must also release the shares it already accepted")
	}
	if got := acc.GetProgress(); got.Submitted != 0 {
		t.Fatalf("expiry must clear accumulated shares, got %d", got.Submitted)
	}
	shares[2].Destroy()
}
