// Package crypto re-exports seal types for backward compatibility.
package crypto

import "github.com/jeremyhahn/go-quicraft/pkg/seal"

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
// the root key, with no call site changed to notice (#95). The same change is
// made to the pkg/seal originals. Callers are unaffected: a call expression
// reads identically either way.
func NewBarrier(config BarrierConfig) (*Barrier, error) { return seal.NewBarrier(config) }

// MustNewBarrier calls seal.MustNewBarrier. It panics on an invalid config; see
// seal.MustNewBarrier for when that is the right form.
func MustNewBarrier(config BarrierConfig) *Barrier { return seal.MustNewBarrier(config) }

// NewCredentials calls seal.NewCredentials, CONSUMING the caller's slice.
//
// It completes the shim. Credentials is re-exported above, and #41 retyped its
// Secret field to an *enclave.SealedEnclave, so seal.NewCredentials became the
// only supported way to populate one — which left this package exposing a type
// its own consumers could not construct (#95). The break is deliberate; the gap
// was not.
//
// Deleting Credentials instead would retire the shim, and retiring it is not
// this change's call to make: nothing in this repository imports pkg/crypto, so
// the only consumers are external, and removing a re-exported type breaks them
// where adding a constructor cannot.
func NewCredentials(secret []byte) (Credentials, error) { return seal.NewCredentials(secret) }
