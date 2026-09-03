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
	"encoding/json"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// jsonNull is the JSON literal a nil slice marshals to; an init result decoded
// from it — or from an absent "shares" — holds no shares rather than being
// rejected as an empty set.
var jsonNull = []byte("null")

// DefaultQuorumTTL is the default time-to-live for a quorum accumulator session.
// If the threshold is not met within this duration, the accumulated shares are
// discarded and custodians must restart the unseal process.
const DefaultQuorumTTL = 5 * time.Minute

// ProtectedShareSplitter splits a secret into M-of-N Shamir secret shares whose
// values live in protected memory.
//
// It is the ONLY splitter shape this package defines. A string-valued sibling
// existed through v0.8.0-alpha and is gone: an implementation that split into
// locked memory still had to copy every share back out into an immortal heap
// string to satisfy that signature, so the exposure the ProtectedShare family
// exists to close survived a migration that satisfied the linter (reported by
// go-qrdb, #35; retyped under the owner ruling on #37).
//
// Nothing in this module implements or calls this — it is a SHAPE for
// embedders. Custody itself lives in go-qrdb; see the package doc.
type ProtectedShareSplitter interface {
	// SplitProtected divides the secret into total shares with the given
	// threshold, each share's value held in an mlock'd, guard-paged buffer.
	//
	// The caller owns the returned shares and MUST Destroy every one of them —
	// including on the error paths of whatever it does next. Locked memory is a
	// finite kernel resource and each share is one allocation of it.
	SplitProtected(secret []byte, threshold, total int) ([]*shamir.ProtectedShare, error)
}

// ProtectedShareAccumulator collects shares from individual custodians until the
// threshold is met, holding every one of them in protected memory.
// Implementations handle deduplication, ordering, and TTL expiration. The
// accumulator is not safe for concurrent use without external synchronization
// (the Barrier provides this via its mutex).
//
// It is the ONLY accumulator shape this package defines; the string-valued
// sibling that existed through v0.8.0-alpha is gone for the reason given on
// ProtectedShareSplitter.
//
// Both ends are protected deliberately. AddProtectedShare takes ownership of a
// share whose value already lives in locked memory, so a transport can decode
// into a wipeable buffer, hand it over through NewProtectedShare (which wipes
// its source), and own nothing afterwards — the share never becomes a string on
// the way in. ReconstructProtected returns the secret in an enclave rather than
// an ordinary []byte, so it never becomes an un-wipeable heap allocation on the
// way out either. An accumulator that protected only one end would leave the
// exposure intact at the other.
type ProtectedShareAccumulator interface {
	// AddProtectedShare adds one share to the accumulator, TAKING OWNERSHIP of
	// it: the accumulator is responsible for Destroying it, whether or not the
	// share is accepted and whether or not the quorum ever completes. A caller
	// that keeps using the share after this returns is reading memory the
	// accumulator may already have wiped.
	AddProtectedShare(ctx context.Context, share *shamir.ProtectedShare) (*QuorumProgress, error)

	// GetProgress returns the current quorum progress without modifying state.
	GetProgress() *QuorumProgress

	// ReconstructProtected reconstructs the secret from the accumulated shares
	// into locked memory. It returns an error if the threshold has not been
	// met. The caller owns the returned enclave and must Destroy it.
	ReconstructProtected(ctx context.Context) (*enclave.MemoryEnclave, error)

	// Reset destroys every accumulated share and resets the TTL timer.
	Reset()

	// Expired reports whether the accumulator session has exceeded its TTL.
	Expired() bool
}

// ShamirConfig configures Shamir secret sharing for barrier operations.
type ShamirConfig struct {
	// Threshold is the minimum number of shares required to reconstruct
	// the root key (M in M-of-N).
	Threshold int `json:"threshold"`

	// TotalShares is the total number of shares to generate (N in M-of-N).
	TotalShares int `json:"total_shares"`

	// QuorumTTL is the maximum duration to wait for all shares to arrive.
	// Defaults to DefaultQuorumTTL if zero.
	QuorumTTL time.Duration `json:"quorum_ttl,omitempty"`
}

// ShamirInitResult contains the output of a Shamir-based initialization — the
// moment a ceremony holds all N shares at once, which is the point of widest
// exposure in the whole scheme.
//
// The result OWNS its shares. Destroy it (or every share individually) as soon
// as the shares have been distributed: each one is a locked allocation charged
// against RLIMIT_MEMLOCK for as long as it is held, and an abandoned result holds
// N of them for the life of the process — N locked pages no other caller can
// reclaim, and the next one to reach the ceiling is refused with
// ErrLockedMemoryUnavailable. Under memguard reaching it terminated the process
// outright.
//
// SERIALISATION, stated rather than left to be inferred from the tags, in two
// parts.
//
// The WIRE SHAPE CHANGES with this retype, and does not merely become
// protected. Through v0.8.0-alpha this field was a []string and `"shares"`
// marshalled as an array of bare share values; it is now an array of share
// OBJECTS — the `{"index","threshold","total","value"}` encoding that
// shamir.Share and shamir.ProtectedShare both already produce, which is the
// shape shares are persisted and distributed in everywhere else in the stack.
// So this result gains no third encoding, but a stored or in-flight init result
// written by an older build does not decode into this type. Pre-release, that
// change lands clean rather than behind a dual-format reader
// (release-versioning.md §1); TestShamirInitResult_JSONMatchesTheShareObjectShape
// pins what it now emits.
//
// The second part is what marshalling COSTS. It puts a copy of every share
// value in an ordinary heap buffer: ProtectedShare.MarshalJSON is careful to
// build its own output without minting a string, but encoding/json then appends
// that output into its internal encode buffer, which grows by reallocation and
// strands un-wiped copies of whatever it already held. So marshal at the
// transport boundary, immediately before the bytes leave, wipe the marshalled
// bytes once they are written, and Destroy the result on the next line. A
// marshalled result held open is the share set held twice: once wipeable and
// once not.
type ShamirInitResult struct {
	// Shares contains the generated Shamir shares, one per custodian, each
	// value in an mlock'd, guard-paged buffer. The result owns them; see
	// Destroy.
	Shares []*shamir.ProtectedShare `json:"shares"`

	// Threshold is the minimum number of shares required for reconstruction.
	Threshold int `json:"threshold"`

	// TotalShares is the total number of shares generated.
	TotalShares int `json:"total_shares"`
}

// Destroy wipes and releases every share the result holds, then clears the
// slice so a second call — or a marshal — cannot reach freed memory. It is safe
// on a nil receiver and safe to call more than once, which is what lets a
// ceremony carry `defer result.Destroy()` from the line the result exists.
func (r *ShamirInitResult) Destroy() {
	if r == nil {
		return
	}
	for _, s := range r.Shares {
		s.Destroy()
	}
	r.Shares = nil
}

// UnmarshalJSON decodes an init result while keeping the share-set bound that a
// generic decode of this type would bypass.
//
// Retyping Shares to []*shamir.ProtectedShare turned the default decoder into a
// denial-of-service path: encoding/json calls ProtectedShare.UnmarshalJSON once
// per array element AS IT VISITS THEM, and each call locks a page of memory, so
// an attacker-controlled or malformed document with an oversized "shares" array
// dictates how much of the finite RLIMIT_MEMLOCK budget is taken, denying it to
// every other caller in the process until the decode is torn down — and under
// memguard it was worse still, since a lock the kernel refused terminated the
// process. The scheme's own count bound in shamir.CombineProtected runs on
// shares already allocated, far too late to help. So the shares are held first
// as raw JSON and decoded through
// shamir.UnmarshalShareSet, which bounds the set (at most maxShares elements and
// MaxShareSetBytes of JSON) BEFORE it locks anything.
//
// Ownership: every step that can fail runs before the receiver is touched, and
// UnmarshalShareSet destroys every share it built if a later element is rejected
// — so a rejected document leaves the receiver unchanged and leaks no locked
// memory. On success a receiver that already held shares (decoding into a reused
// result) has them destroyed before the new set replaces them. The copy this
// method makes of the "shares" bytes is share material and is wiped on the way
// out; the caller's own data buffer is not this method's to wipe.
func (r *ShamirInitResult) UnmarshalJSON(data []byte) error {
	var aux struct {
		Shares      json.RawMessage `json:"shares"`
		Threshold   int             `json:"threshold"`
		TotalShares int             `json:"total_shares"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	// aux.Shares is a heap copy of the shares array — share material — so it is
	// wiped on every path out. UnmarshalShareSet wipes only the internal copies
	// it makes, not the slice it is handed.
	defer wipeBytes(aux.Shares)

	var shares []*shamir.ProtectedShare
	// An absent or explicitly null "shares" decodes to no shares, mirroring the
	// zero-value round trip; an empty array is a malformed set and
	// UnmarshalShareSet rejects it. Anything else is bounded and decoded there.
	if trimmed := bytes.TrimSpace(aux.Shares); len(trimmed) > 0 && !bytes.Equal(trimmed, jsonNull) {
		decoded, err := shamir.UnmarshalShareSet(aux.Shares)
		if err != nil {
			return err
		}
		shares = decoded
	}

	// Assign last: destroy anything a reused receiver already held so decoding
	// into a non-empty result strands no locked buffer, then take the new set.
	r.Destroy()
	r.Shares = shares
	r.Threshold = aux.Threshold
	r.TotalShares = aux.TotalShares
	return nil
}

// QuorumProgress reports the current state of a threshold quorum accumulator.
type QuorumProgress struct {
	// Required is the number of shares needed to reach the threshold.
	Required int `json:"required"`

	// Submitted is the number of shares submitted so far.
	Submitted int `json:"submitted"`

	// Complete indicates whether the threshold has been met and the
	// secret can be reconstructed.
	Complete bool `json:"complete"`
}
