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

// NewBarrier re-exports seal.NewBarrier.
var NewBarrier = seal.NewBarrier
