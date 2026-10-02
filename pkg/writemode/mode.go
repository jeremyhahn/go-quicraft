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

// Package writemode defines write durability modes for Raft proposals.
//
// Three modes are supported:
//   - Sync: waits for quorum commit (strongest durability).
//   - WALDurable: waits only for local WAL persistence (lower latency).
//   - Batched: collects writes and proposes as a single Raft entry (amortized cost).
package writemode

import "fmt"

// Mode represents the durability guarantee level for write operations.
//
// Mode is an open uint8, so a value outside the three constants below is
// representable — a field decoded from configuration or a wire format is the
// ordinary way one arrives. Such a value describes NO durability guarantee:
// String reports "unknown(N)", and IsSynchronous, IsAsync and RequiresQuorum
// all report false, so an embedder that dispatches on RequiresQuorum takes the
// "quorum not required" branch for a Mode that in fact means nothing at all.
// The predicates cannot report an error, so Validate is the boundary check, and
// callers accepting a Mode from outside their own code MUST run it there.
// TestMode_UnknownValueGuaranteesNothing pins that behaviour so it cannot
// change silently.
type Mode uint8

const (
	// Sync waits for the write to be replicated to a quorum and committed.
	// Strongest durability (~10-30ms per write). Uses Host.SyncPropose().
	Sync Mode = iota

	// WALDurable waits only for local WAL persistence.
	// Faster (~1-2ms) but risks data loss if leader crashes before replication.
	// Uses Host.Propose() + RequestState.CommittedC().
	WALDurable

	// Batched collects writes and proposes them as a single Raft entry.
	// Amortized cost (~0.1-0.3ms per op), and the same QUORUM durability as
	// Sync once the batch commits.
	//
	// DELIVERY IS AT-LEAST-ONCE, NOT at-most-once. The aggregator proposes a
	// raw encoded batch with NO client session (batch.Proposer has no session
	// parameter), so nothing deduplicates a retry. A flush that fails with
	// batch.FlushError.Indeterminate may nonetheless have committed, and
	// retrying re-applies every command in that batch — up to MaxBatchSize
	// commands from mutually unaware callers.
	//
	// A batched proposal also cannot report a per-item apply failure: the
	// aggregate result is fanned out to every co-batched caller, so a command
	// the state machine REJECTED is reported to its submitter as successful.
	//
	// Use Sync with a client session for any operation that is not idempotent.
	Batched
)

// modeSpec is everything this package knows about one Mode: its canonical
// spelling and whether it waits for quorum commit before acknowledging.
//
// It exists so that the set of valid Modes is defined exactly once. String,
// Validate and all three durability predicates read it, which is what makes
// them agree by construction: predicates that each restated the membership
// (`m == Sync || m == Batched`) would mean adding a Mode requires remembering
// every one of them, and forgetting one would produce a Mode that answers false
// to every durability question while validating cleanly.
type modeSpec struct {
	name string
	// quorum reports that the mode does not acknowledge a write until it is
	// committed by a quorum. Its negation is the WAL-durable-only posture, so
	// one bool distinguishes all three modes and IsAsync cannot disagree with
	// IsSynchronous about the same Mode.
	quorum bool
}

// modeSpecs is the single source of truth for which Mode values are valid.
var modeSpecs = map[Mode]modeSpec{
	Sync:       {name: "sync", quorum: true},
	WALDurable: {name: "wal_durable", quorum: false},
	Batched:    {name: "batched", quorum: true},
}

// parseModes maps string representations (including aliases) to Mode values.
// It is many-to-one and therefore separate from modeSpecs; FromString checks
// the two against each other on every parse.
var parseModes = map[string]Mode{
	"":            Sync,
	"sync":        Sync,
	"wal_durable": WALDurable,
	"wal":         WALDurable,
	"async":       WALDurable,
	"batched":     Batched,
	"batch":       Batched,
}

// String returns the canonical string representation of the Mode.
func (m Mode) String() string {
	if spec, ok := modeSpecs[m]; ok {
		return spec.name
	}
	return fmt.Sprintf("unknown(%d)", m)
}

// FromString parses a Mode from its string representation.
// An empty string defaults to Sync. The following aliases are accepted:
//   - "wal" and "async" for WALDurable
//   - "batch" for Batched
//
// On failure it returns Sync alongside the error. Sync is the zero value and
// the STRONGEST of the three modes, so a caller that ignores the error is
// pushed toward more durability rather than less — but the error is the answer,
// and the Mode is only meaningful when it is nil.
func FromString(s string) (Mode, error) {
	mode, ok := parseModes[s]
	if !ok {
		return Sync, &InvalidModeError{Value: s}
	}
	// Nothing but this keeps parseModes and modeSpecs agreeing. An alias added
	// for a Mode that modeSpecs does not describe would otherwise parse
	// happily and then answer "unknown(N)" from String and false from every
	// durability predicate: a configuration string this package ACCEPTS while
	// naming no durability guarantee at all. Validate is that check, and this
	// is its production caller.
	if err := mode.Validate(); err != nil {
		return Sync, err
	}
	return mode, nil
}

// Validate reports whether the Mode is one this package describes.
//
// It is the boundary check for a Mode that came from outside the caller's own
// code — a configuration file, a wire field, an integer cast. The durability
// predicates below cannot signal an invalid Mode (they return a bare bool, and
// answer false to everything for one), so an unvalidated Mode reads as a write
// that needs no quorum. See the Mode doc comment.
func (m Mode) Validate() error {
	if _, ok := modeSpecs[m]; !ok {
		return &InvalidModeError{Value: fmt.Sprintf("%d", m)}
	}
	return nil
}

// IsSynchronous returns true if the mode waits for quorum commit
// before acknowledging the write to the caller. An unrecognized Mode reports
// false; see Validate.
func (m Mode) IsSynchronous() bool {
	spec, ok := modeSpecs[m]
	return ok && spec.quorum
}

// IsAsync returns true if the mode returns before quorum commit,
// relying only on local WAL durability. An unrecognized Mode reports false;
// see Validate.
func (m Mode) IsAsync() bool {
	spec, ok := modeSpecs[m]
	return ok && !spec.quorum
}

// RequiresQuorum returns true if the mode requires quorum acknowledgment
// before the write is considered durable. An unrecognized Mode reports false;
// see Validate.
func (m Mode) RequiresQuorum() bool {
	spec, ok := modeSpecs[m]
	return ok && spec.quorum
}
