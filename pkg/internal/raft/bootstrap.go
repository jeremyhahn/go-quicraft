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

package raft

import (
	"errors"
	"log/slog"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Config-in-log bootstrap.
//
// Injecting a shard's initial configuration out-of-band (from the caller's
// members map) would make a replica's membership NOT a function of its durable
// log: a joining replica could learn it only from a snapshot, and if no
// snapshot ever arrived it would never learn the full configuration at all.
// Instead, the initial configuration is written to the log itself as one committed entry at index 1, and every
// replica reconstructs its membership by replaying the committed log. Membership
// becomes a pure fold over the log — the property the whole design turns on.
//
// A SINGLE entry carrying the full Membership is used rather than one
// ConfigChange per member (etcd's model) so the initial log occupies exactly
// index 1 and the first real proposal lands at index 2 — the smallest possible
// perturbation of the index arithmetic the recovery clamps and marker logic
// depend on.

const (
	// BootstrapMembershipIndex is the log index of the initial-membership entry.
	// It is the first index of every shard's log; the first real proposal (and
	// the leader's no-op) land at BootstrapMembershipIndex+1 or above.
	BootstrapMembershipIndex uint64 = 1

	// bootstrapMembershipTerm is the term stamped on the initial-membership
	// entry. Term 1 is the lowest term any leader can hold, so the entry is
	// never ahead of the term of the leader that first commits above it.
	bootstrapMembershipTerm uint64 = 1
)

// BuildInitialMembershipEntry returns the deterministic bootstrap entry that
// carries the full initial configuration in its Cmd (a marshaled
// proto.Membership).
//
// Given the same membership it produces byte-identical output on every replica:
// proto.Membership marshals its maps with sorted keys, so there is no
// map-iteration nondeterminism. That is required — every replica must hold an
// identical entry at BootstrapMembershipIndex or the Log Matching property is
// violated the moment two of them compare logs.
//
// CONTRACT: every founding member commits this entry at index 1 INDEPENDENTLY,
// so byte-identity is only a guarantee when every founder is given identical
// bootstrap INPUT. That input is the whole Membership, not only its address map:
// MaxWitnesses and RequireFullReplicaMajority come from each founder's local
// Config.Witness (see Host.StartShard), so founders configured with different
// witness policy — or different member addresses — produce byte-DIFFERENT index-1
// entries at the same (index=1, term=1). AppendEntries matches on (index, term)
// only, so such a divergence is never reconciled and each replica then computes
// quorum over its own policy. Founders MUST be configured identically (members
// map AND witness policy); this is the same unenforced operator contract the
// initial member list has always carried, now extended to the policy fields.
//
// The membership's ConfigChangeID is left at 0: the initial configuration
// predates any config change, so a caller observes ConfigChangeID 0 until the
// first real membership change — exactly as the previous out-of-band seed
// reported. Stamping it with the entry's index instead would shift the
// observable initial value from 0 to 1 and break a compare-and-swap membership
// change that read 0. It is constant across replicas either way, so determinism
// is unaffected.
func BuildInitialMembershipEntry(ms proto.Membership) (proto.Entry, error) {
	ms.ConfigChangeID = 0
	buf := make([]byte, ms.Size())
	if _, err := ms.MarshalTo(buf); err != nil {
		return proto.Entry{}, err
	}
	return proto.Entry{
		Index: BootstrapMembershipIndex,
		Term:  bootstrapMembershipTerm,
		Type:  proto.EntryInitialMembership,
		Cmd:   buf,
	}, nil
}

// initialMembershipFromEntry decodes the Membership carried by a bootstrap
// entry. It is the inverse of BuildInitialMembershipEntry and is used by the
// replay/commit paths to adopt the initial configuration.
func initialMembershipFromEntry(e proto.Entry) (proto.Membership, error) {
	var ms proto.Membership
	if _, err := ms.UnmarshalFrom(e.Cmd); err != nil {
		return proto.Membership{}, err
	}
	return ms, nil
}

// ReconstructMembership folds a run of committed config-carrying entries into
// the membership they define. It is the recovery-time reconstruction of
// config-in-log: a restarting replica whose membership is not carried by a
// snapshot rebuilds it from the durable log rather than re-seeding from local
// config.
//
// entries must be in ascending index order. The bootstrap EntryInitialMembership
// (index 1) is adopted wholesale; each EntryConfigChange above it is applied
// convergently, exactly as the runtime commit path does, so a deterministic
// refusal (a Removed tombstone re-add, a role only a committed change could
// have set) is evaluated identically here and ignored. Non-config entry types
// are skipped. A run that begins above a compaction floor yields only the
// changes it contains, which is why the caller prefers a snapshot's membership
// when one exists.
//
// A committed config-carrying entry whose payload does NOT DECODE is fatal, for
// BOTH entry types: the return is ErrCorruptConfigEntry joined with the decoder's
// own error, so errors.Is finds the sentinel and the cause alike. This is the
// same decision the runtime commit path makes (peer.applyMembershipEntry), and
// the two must agree: the bytes are committed replicated state that every other
// replica can read, so skipping one here reconstructs a membership that differs
// from the rest of the shard — a replica computing quorum over its own voting
// set, silently, and durably once the next snapshot persists it. There is no
// "recover as much as possible" answer for a configuration; a partial fold is
// not a degraded membership, it is a DIFFERENT one, and a replica that adopted
// it could commit on a quorum the real configuration never granted.
//
// A ZERO-LENGTH EntryConfigChange payload is deliberately not in that class. It
// is the silent no-op it has always been on the runtime path too: every replica
// reads the same nothing and skips identically, so it cannot diverge them, and
// no legitimate producer emits one (the propose-time gate refuses any payload
// that does not decode).
func ReconstructMembership(entries []proto.Entry) (proto.Membership, error) {
	m := newMembership(proto.Membership{})
	for i := range entries {
		switch entries[i].Type & proto.EntryTypeMask {
		case proto.EntryInitialMembership:
			// The founding configuration is defined to live at exactly
			// BootstrapMembershipIndex. Adopting one anywhere else would
			// REPLACE the whole membership and roll the voting set back to the
			// founding members — a configuration no live replica shares. The
			// runtime commit path refuses it (peer.applyMembershipEntry); make
			// recovery reach the identical decision so the two cannot diverge.
			// Today's sole caller pre-checks, but the guard must not depend on
			// a caller-side invariant asserted in another file.
			if entries[i].Index != BootstrapMembershipIndex {
				continue
			}
			ms, err := initialMembershipFromEntry(entries[i])
			if err != nil {
				slog.Error("committed initial-membership entry does not decode during recovery",
					"entry_index", entries[i].Index,
					"entry_term", entries[i].Term,
					"entry_type", entries[i].Type,
					"error", err,
				)
				return proto.Membership{}, errors.Join(ErrCorruptConfigEntry, err)
			}
			m = newMembership(ms)
		case proto.EntryConfigChange:
			if len(entries[i].Cmd) == 0 {
				continue
			}
			var cc proto.ConfigChange
			if _, err := cc.UnmarshalFrom(entries[i].Cmd); err != nil {
				slog.Error("committed config change entry does not decode during recovery",
					"entry_index", entries[i].Index,
					"entry_term", entries[i].Term,
					"entry_type", entries[i].Type,
					"error", err,
				)
				return proto.Membership{}, errors.Join(ErrCorruptConfigEntry, err)
			}
			_ = m.apply(cc, entries[i].Index)
		}
	}
	return m.snapshot().toProto(), nil
}
