// Package crypto re-exports seal types for backward compatibility.
package crypto

import (
	"context"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// Barrier wraps go-quicraft/pkg/seal.Barrier.
type Barrier = seal.Barrier

// BarrierConfig wraps go-quicraft/pkg/seal.BarrierConfig.
type BarrierConfig = seal.BarrierConfig

// SealingStrategy wraps go-quicraft/pkg/seal.SealingStrategy.
type SealingStrategy = seal.SealingStrategy

// Credentials wraps go-quicraft/pkg/seal.Credentials.
type Credentials = seal.Credentials

// SealedRootKey wraps go-quicraft/pkg/seal.SealedRootKey.
type SealedRootKey = seal.SealedRootKey

// NewBarrier calls seal.NewBarrier.
//
// The re-exports below are FUNCTIONS rather than package-level variables holding
// function values. A `var NewBarrier = seal.NewBarrier` is writable by every
// package that can import this one, so an init() anywhere in a consumer's
// transitive graph could substitute the constructor for the barrier that holds
// the root key, with no call site changed to notice. The pkg/seal originals are
// functions for the same reason. Callers are unaffected: a call expression
// reads identically either way.
func NewBarrier(config BarrierConfig) (*Barrier, error) { return seal.NewBarrier(config) }

// MustNewBarrier calls seal.MustNewBarrier. It panics on an invalid config; see
// seal.MustNewBarrier for when that is the right form.
func MustNewBarrier(config BarrierConfig) *Barrier { return seal.MustNewBarrier(config) }

// NewCredentials calls seal.NewCredentials, CONSUMING the caller's slice.
//
// Credentials is re-exported above, and its Secret field is an
// *enclave.Sealed, so seal.NewCredentials is the only supported way to
// populate one. Without this re-export the package would expose a type its own
// consumers could not construct.
func NewCredentials(ctx context.Context, secret []byte) (Credentials, error) {
	return seal.NewCredentials(ctx, secret)
}
