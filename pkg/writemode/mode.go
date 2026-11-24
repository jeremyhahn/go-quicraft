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

// Package writemode defines write durability modes: Sync (quorum commit), WALDurable (local WAL persist) and Batched (one Raft entry per batch).
package writemode

import "fmt"

// Mode is a write durability level; a value outside the constants guarantees nothing, so validate external Modes with Validate.
type Mode uint8

const (
	// Sync waits for the write to be replicated to a quorum and committed.
	// Strongest durability (~10-30ms per write). Uses Host.SyncPropose().
	Sync Mode = iota

	// WALDurable waits only for local WAL persistence.
	// Faster (~1-2ms) but risks data loss if leader crashes before replication.
	// Uses Host.Propose() + RequestState.CommittedC().
	WALDurable

	// Batched proposes collected writes as one Raft entry with quorum durability; delivery is at-least-once and per-item failures are not reported, so use Sync with a session for non-idempotent writes.
	Batched
)

// modeSpec is a Mode's canonical name and whether it waits for quorum; the single definition String, Validate and the predicates read.
type modeSpec struct {
	name string
	// quorum reports that the mode acknowledges only after quorum commit.
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

// FromString parses a Mode; "" is Sync, "wal" and "async" are WALDurable, "batch" is Batched. On error it returns Sync.
func FromString(s string) (Mode, error) {
	mode, ok := parseModes[s]
	if !ok {
		return Sync, &InvalidModeError{Value: s}
	}
	// Validate keeps parseModes and modeSpecs in agreement.
	if err := mode.Validate(); err != nil {
		return Sync, err
	}
	return mode, nil
}

// Validate reports whether m is a Mode this package describes; run it on Modes from outside the caller's code.
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
