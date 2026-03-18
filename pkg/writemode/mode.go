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
type Mode uint8

const (
	// Sync waits for the write to be replicated to a quorum and committed.
	// Strongest durability (~10-30ms per write). Uses Host.SyncPropose().
	Sync Mode = iota

	// WALDurable waits only for local WAL persistence.
	// Faster (~1-2ms) but risks data loss if leader crashes before replication.
	// Uses Host.Propose() + RequestState.CommittedC().
	WALDurable

	// Batched collects writes and proposes as a single Raft entry.
	// Same durability as Sync but amortized cost (~0.1-0.3ms per op).
	// Uses batch.Aggregator.Submit().
	Batched
)

// modeNames maps each valid Mode to its canonical string representation.
var modeNames = map[Mode]string{
	Sync:       "sync",
	WALDurable: "wal_durable",
	Batched:    "batched",
}

// parseModes maps string representations (including aliases) to Mode values.
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
	if name, ok := modeNames[m]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", m)
}

// FromString parses a Mode from its string representation.
// An empty string defaults to Sync. The following aliases are accepted:
//   - "wal" and "async" for WALDurable
//   - "batch" for Batched
func FromString(s string) (Mode, error) {
	if mode, ok := parseModes[s]; ok {
		return mode, nil
	}
	return Sync, &InvalidModeError{Value: s}
}

// Validate checks whether the Mode is within the valid range.
func (m Mode) Validate() error {
	if _, ok := modeNames[m]; !ok {
		return &InvalidModeError{Value: fmt.Sprintf("%d", m)}
	}
	return nil
}

// IsSynchronous returns true if the mode waits for quorum commit
// before acknowledging the write to the caller.
func (m Mode) IsSynchronous() bool {
	return m == Sync || m == Batched
}

// IsAsync returns true if the mode returns before quorum commit,
// relying only on local WAL durability.
func (m Mode) IsAsync() bool {
	return m == WALDurable
}

// RequiresQuorum returns true if the mode requires quorum acknowledgment
// before the write is considered durable.
func (m Mode) RequiresQuorum() bool {
	return m == Sync || m == Batched
}
