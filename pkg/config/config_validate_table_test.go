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

package config

import (
	"errors"
	"math"
	"math/bits"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// errKind names the documented error type a rejection row expects.
type errKind int

const (
	kindValidation errKind = iota
	kindProposalPayload
	kindEntrySize
	kindApplyEntrySize
	kindSessionExpiry
	kindLeaseRead
)

// matchErrKind reports whether err is (or wraps) the documented type.
func matchErrKind(err error, kind errKind) bool {
	switch kind {
	case kindValidation:
		var e *ValidationError
		return errors.As(err, &e)
	case kindProposalPayload:
		var e *MaxProposalPayloadSizeExceededError
		return errors.As(err, &e)
	case kindEntrySize:
		var e *MaxEntrySizeExceededError
		return errors.As(err, &e)
	case kindApplyEntrySize:
		var e *MaxApplyEntrySizeExceededError
		return errors.As(err, &e)
	case kindSessionExpiry:
		var e *SessionExpiryEntriesTooSmallError
		return errors.As(err, &e)
	case kindLeaseRead:
		var e *LeaseReadRequiresCheckQuorumError
		return errors.As(err, &e) && errors.Is(err, ErrLeaseReadRequiresCheckQuorum)
	}
	return false
}

// TestConfig_Validate_Table starts every row from validConfig, changes one
// field (plus whatever the row must hold steady so an earlier guard does not
// fire first), and checks Validate's verdict. A rejection must carry the
// documented error type and name the field in its text; a *ValidationError
// must also carry the field in Field.
func TestConfig_Validate_Table(t *testing.T) {
	protoMax := uint64(proto.MaxEntrySize)
	tests := []struct {
		name   string
		mutate func(c *Config)
		reject bool
		kind   errKind
		field  string
	}{
		// validateIdentity
		{name: "ShardID zero", mutate: func(c *Config) { c.ShardID = 0 }, reject: true, field: "ShardID"},
		{name: "ShardID one", mutate: func(c *Config) { c.ShardID = 1 }},
		{name: "ShardID max", mutate: func(c *Config) { c.ShardID = math.MaxUint64 }},
		{name: "ReplicaID zero", mutate: func(c *Config) { c.ReplicaID = 0 }, reject: true, field: "ReplicaID"},
		{name: "ReplicaID above MaxUint32", mutate: func(c *Config) { c.ReplicaID = math.MaxUint32 + 1 }, reject: true, field: "ReplicaID"},
		{name: "ReplicaID max", mutate: func(c *Config) { c.ReplicaID = math.MaxUint64 }, reject: true, field: "ReplicaID"},
		{name: "ReplicaID at MaxUint32", mutate: func(c *Config) { c.ReplicaID = math.MaxUint32 }},
		{name: "observer and witness", mutate: func(c *Config) { c.IsObserver, c.IsWitness = true, true }, reject: true, field: "IsObserver"},
		{name: "observer only", mutate: func(c *Config) { c.IsObserver = true }},
		{name: "witness only", mutate: func(c *Config) { c.IsWitness = true }},
		{name: "MaxWitnesses negative", mutate: func(c *Config) { c.Witness.MaxWitnesses = -1 }, reject: true, field: "Witness.MaxWitnesses"},
		{name: "MaxWitnesses min int", mutate: func(c *Config) { c.Witness.MaxWitnesses = math.MinInt }, reject: true, field: "Witness.MaxWitnesses"},
		{name: "MaxWitnesses zero", mutate: func(c *Config) { c.Witness.MaxWitnesses = 0 }},

		// validateTiming
		{name: "HeartbeatRTT zero", mutate: func(c *Config) { c.HeartbeatRTT = 0 }, reject: true, field: "HeartbeatRTT"},
		{name: "ElectionRTT four", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 1, 4 }, reject: true, field: "ElectionRTT"},
		{name: "ElectionRTT zero", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 1, 0 }, reject: true, field: "ElectionRTT"},
		{name: "ElectionRTT five", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 1, 5 }},
		{
			name:   "HeartbeatRTT above MaxInt32",
			mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = math.MaxInt32+1, math.MaxUint64 },
			reject: true, field: "HeartbeatRTT",
		},
		{
			name:   "HeartbeatRTT max",
			mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = math.MaxUint64, math.MaxUint64 },
			reject: true, field: "HeartbeatRTT",
		},
		{
			name:   "ElectionRTT above MaxInt32",
			mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 1, math.MaxInt32+1 },
			reject: true, field: "ElectionRTT",
		},
		{
			name:   "ElectionRTT max",
			mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 1, math.MaxUint64 },
			reject: true, field: "ElectionRTT",
		},
		{name: "ElectionRTT at MaxInt32", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 1, math.MaxInt32 }},
		{
			name:   "HeartbeatRTT at MaxInt32 with ElectionRTT at MaxInt32",
			mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = math.MaxInt32, math.MaxInt32 },
			reject: true, field: "ElectionRTT",
		},
		{
			name:   "HeartbeatRTT at the largest 2x margin under MaxInt32",
			mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = math.MaxInt32/2, math.MaxInt32 },
		},
		{name: "ratio below 2", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 3, 5 }, reject: true, field: "ElectionRTT"},
		{name: "ratio below 2 odd", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 6, 11 }, reject: true, field: "ElectionRTT"},
		{name: "ratio exactly 2", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 5, 10 }},
		{name: "ratio 2 by floor", mutate: func(c *Config) { c.HeartbeatRTT, c.ElectionRTT = 2, 5 }},

		// validateSizeLimits
		{
			name:   "SnapshotEntries equal to CompactionOverhead",
			mutate: func(c *Config) { c.SnapshotEntries, c.CompactionOverhead = 10, 10 },
			reject: true, field: "SnapshotEntries",
		},
		{
			name:   "SnapshotEntries below CompactionOverhead",
			mutate: func(c *Config) { c.SnapshotEntries, c.CompactionOverhead = 1, 10 },
			reject: true, field: "SnapshotEntries",
		},
		{name: "SnapshotEntries one above CompactionOverhead", mutate: func(c *Config) { c.SnapshotEntries, c.CompactionOverhead = 11, 10 }},
		{name: "SnapshotEntries zero", mutate: func(c *Config) { c.SnapshotEntries = 0 }},
		{name: "SnapshotsDisabled", mutate: func(c *Config) { c.SnapshotEntries = SnapshotsDisabled }},
		{name: "MaxProposalPayloadSize zero", mutate: func(c *Config) { c.MaxProposalPayloadSize = 0 }, reject: true, field: "MaxProposalPayloadSize"},
		{
			name:   "MaxProposalPayloadSize above protocol max",
			mutate: func(c *Config) { c.MaxProposalPayloadSize = protoMax + 1 },
			reject: true, kind: kindProposalPayload, field: "MaxProposalPayloadSize",
		},
		{
			name:   "MaxProposalPayloadSize max",
			mutate: func(c *Config) { c.MaxProposalPayloadSize = math.MaxUint64 },
			reject: true, kind: kindProposalPayload, field: "MaxProposalPayloadSize",
		},
		{name: "MaxProposalPayloadSize at protocol max", mutate: func(c *Config) { c.MaxProposalPayloadSize = protoMax }},
		{name: "MaxProposalPayloadSize one", mutate: func(c *Config) { c.MaxProposalPayloadSize = 1 }},
		{name: "MaxEntrySize zero", mutate: func(c *Config) { c.MaxEntrySize = 0 }, reject: true, field: "MaxEntrySize"},
		{
			name:   "MaxEntrySize above protocol max",
			mutate: func(c *Config) { c.MaxEntrySize = protoMax + 1 },
			reject: true, kind: kindEntrySize, field: "MaxEntrySize",
		},
		{name: "MaxEntrySize at protocol max", mutate: func(c *Config) { c.MaxEntrySize = protoMax }},
		{name: "MaxEntrySize one", mutate: func(c *Config) { c.MaxEntrySize = 1 }},
		{name: "MaxApplyEntrySize zero", mutate: func(c *Config) { c.MaxApplyEntrySize = 0 }, reject: true, field: "MaxApplyEntrySize"},
		{
			name:   "MaxApplyEntrySize above frame max",
			mutate: func(c *Config) { c.MaxApplyEntrySize = MaxReplicationPayloadBytes + 1 },
			reject: true, kind: kindApplyEntrySize, field: "MaxApplyEntrySize",
		},
		{name: "MaxApplyEntrySize at frame max", mutate: func(c *Config) { c.MaxApplyEntrySize = MaxReplicationPayloadBytes }},
		{name: "MaxApplyEntrySize one", mutate: func(c *Config) { c.MaxApplyEntrySize = 1 }},

		// validateSessions
		{name: "MaxResponsesPerSession zero", mutate: func(c *Config) { c.MaxResponsesPerSession = 0 }, reject: true, field: "MaxResponsesPerSession"},
		{name: "MaxResponsesPerSession one", mutate: func(c *Config) { c.MaxResponsesPerSession = 1 }},
		{name: "MaxTotalSessions zero", mutate: func(c *Config) { c.MaxTotalSessions = 0 }, reject: true, field: "MaxTotalSessions"},
		{name: "MaxTotalSessions one", mutate: func(c *Config) { c.MaxTotalSessions = 1 }},
		{
			name:   "SessionExpiryEntries one",
			mutate: func(c *Config) { c.SessionExpiryEntries = 1 },
			reject: true, kind: kindSessionExpiry, field: "SessionExpiryEntries",
		},
		{
			name:   "SessionExpiryEntries 99",
			mutate: func(c *Config) { c.SessionExpiryEntries = 99 },
			reject: true, kind: kindSessionExpiry, field: "SessionExpiryEntries",
		},
		{name: "SessionExpiryEntries 100", mutate: func(c *Config) { c.SessionExpiryEntries = 100 }},
		{name: "SessionExpiryEntries zero", mutate: func(c *Config) { c.SessionExpiryEntries = 0 }},
		{name: "SessionExpiryDisabled", mutate: func(c *Config) { c.SessionExpiryEntries = SessionExpiryDisabled }},
		{
			name:   "SessionExpiryEntries above SnapshotEntries warns only",
			mutate: func(c *Config) { c.SessionExpiryEntries, c.SnapshotEntries = 200_000, 100_000 },
		},

		// validateCompression
		{
			name:   "SnapshotCompressionType unknown",
			mutate: func(c *Config) { c.SnapshotCompressionType = CompressionType(math.MaxUint16) },
			reject: true, field: "SnapshotCompressionType",
		},
		{
			name:   "EntryCompressionType unknown",
			mutate: func(c *Config) { c.EntryCompressionType = CompressionType(math.MaxUint16) },
			reject: true, field: "EntryCompressionType",
		},
		{name: "Snappy compression", mutate: func(c *Config) { c.SnapshotCompressionType, c.EntryCompressionType = Snappy, Snappy }},
		{name: "no compression", mutate: func(c *Config) { c.SnapshotCompressionType, c.EntryCompressionType = NoCompression, NoCompression }},

		// validateLeaseRead
		{
			name:   "LeaseRead without CheckQuorum",
			mutate: func(c *Config) { c.LeaseRead, c.CheckQuorum = true, BoolPtr(false) },
			reject: true, kind: kindLeaseRead, field: "LeaseRead",
		},
		{name: "LeaseRead with CheckQuorum", mutate: func(c *Config) { c.LeaseRead = true }},
		{name: "LeaseRead at ElectionRTT five", mutate: func(c *Config) { c.LeaseRead, c.HeartbeatRTT, c.ElectionRTT = true, 1, 5 }},
		{name: "CheckQuorum off without LeaseRead", mutate: func(c *Config) { c.CheckQuorum = BoolPtr(false) }},
		{name: "MaxClockDrift negative", mutate: func(c *Config) { c.MaxClockDrift = -0.01 }, reject: true, field: "MaxClockDrift"},
		{name: "MaxClockDrift below min", mutate: func(c *Config) { c.MaxClockDrift = MaxClockDriftMin / 2 }, reject: true, field: "MaxClockDrift"},
		{name: "MaxClockDrift above max", mutate: func(c *Config) { c.MaxClockDrift = math.Nextafter(MaxClockDriftMax, 1) }, reject: true, field: "MaxClockDrift"},
		{name: "MaxClockDrift NaN", mutate: func(c *Config) { c.MaxClockDrift = math.NaN() }, reject: true, field: "MaxClockDrift"},
		{name: "MaxClockDrift +Inf", mutate: func(c *Config) { c.MaxClockDrift = math.Inf(1) }, reject: true, field: "MaxClockDrift"},
		{name: "MaxClockDrift -Inf", mutate: func(c *Config) { c.MaxClockDrift = math.Inf(-1) }, reject: true, field: "MaxClockDrift"},
		{
			name:   "MaxClockDrift out of range with LeaseRead",
			mutate: func(c *Config) { c.LeaseRead, c.MaxClockDrift = true, 1 },
			reject: true, field: "MaxClockDrift",
		},
		{name: "MaxClockDrift zero", mutate: func(c *Config) { c.MaxClockDrift = 0 }},
		{name: "MaxClockDrift at min", mutate: func(c *Config) { c.MaxClockDrift = MaxClockDriftMin }},
		{name: "MaxClockDrift at max", mutate: func(c *Config) { c.MaxClockDrift = MaxClockDriftMax }},

		// validateDataDir
		{name: "DataDir relative", mutate: func(c *Config) { c.DataDir = "data/shard" }, reject: true, field: "DataDir"},
		{name: "DataDir dot", mutate: func(c *Config) { c.DataDir = "." }, reject: true, field: "DataDir"},
		{name: "DataDir empty", mutate: func(c *Config) { c.DataDir = "" }},
		{name: "DataDir absolute", mutate: func(c *Config) { c.DataDir = "/var/lib/quicraft" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.Validate()
			if !tt.reject {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			assertNamedRejection(t, err, tt.kind, tt.field)
		})
	}
}

// assertNamedRejection checks err has the documented type and names field.
func assertNamedRejection(t *testing.T, err error, kind errKind, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Validate() = nil, want a rejection naming %q", field)
	}
	if !matchErrKind(err, kind) {
		t.Fatalf("Validate() = %T (%v), want error kind %d", err, err, kind)
	}
	if kind == kindValidation {
		var ve *ValidationError
		errors.As(err, &ve)
		if ve.Field != field {
			t.Errorf("ValidationError.Field = %q, want %q", ve.Field, field)
		}
	}
	if !strings.Contains(err.Error(), field) {
		t.Errorf("error text %q does not name %q", err.Error(), field)
	}
}

// TestConfig_ValidateElectionTimeout_Table pins the overflow guard on
// ElectionRTT * RTTMillisecond in time.Duration at its exact boundary.
func TestConfig_ValidateElectionTimeout_Table(t *testing.T) {
	const nanosPerMilli = 1_000_000
	limit := func(rtt uint64) uint64 { return uint64(math.MaxInt64) / nanosPerMilli / rtt }
	tests := []struct {
		name     string
		election uint64
		rtt      uint64
		reject   bool
	}{
		{name: "rtt zero never overflows", election: math.MaxUint64, rtt: 0},
		{name: "default", election: DefaultElectionRTT, rtt: 1},
		{name: "rtt 1 at limit", election: limit(1), rtt: 1},
		{name: "rtt 1 past limit", election: limit(1) + 1, rtt: 1, reject: true},
		{name: "rtt 1000 at limit", election: limit(1000), rtt: 1000},
		{name: "rtt 1000 past limit", election: limit(1000) + 1, rtt: 1000, reject: true},
		{name: "rtt max with ElectionRTT one", election: 1, rtt: math.MaxUint64, reject: true},
		{name: "rtt max with ElectionRTT zero", election: 0, rtt: math.MaxUint64},
		{name: "ElectionRTT at MaxInt32 rtt 1", election: math.MaxInt32, rtt: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			c.ElectionRTT = tt.election
			err := c.ValidateElectionTimeout(tt.rtt)
			if !tt.reject {
				if err != nil {
					t.Fatalf("ValidateElectionTimeout(%d) = %v, want nil", tt.rtt, err)
				}
				// An accepted pair must fit in time.Duration exactly.
				hi, ms := bits.Mul64(tt.election, tt.rtt)
				hi2, ns := bits.Mul64(ms, nanosPerMilli)
				if hi != 0 || hi2 != 0 || ns > math.MaxInt64 {
					t.Fatalf("accepted pair %d*%d ms overflows time.Duration", tt.election, tt.rtt)
				}
				return
			}
			assertNamedRejection(t, err, kindValidation, "ElectionRTT")
		})
	}
}
