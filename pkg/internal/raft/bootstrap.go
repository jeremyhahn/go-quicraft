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
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// The initial configuration is one committed entry at index 1, so membership is a fold over the committed log.

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

// BuildInitialMembershipEntry returns the deterministic index-1 entry carrying the initial membership; founders must use identical members and witness policy.
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

// ReconstructMembership folds committed config entries in index order into a membership; an undecodable payload is ErrCorruptConfigEntry, an empty one is skipped.
func ReconstructMembership(entries []proto.Entry) (proto.Membership, error) {
	m := newMembership(proto.Membership{})
	for i := range entries {
		switch entries[i].Type & proto.EntryTypeMask {
		case proto.EntryInitialMembership:
			// The founding configuration is valid only at BootstrapMembershipIndex, as on the commit path.
			if entries[i].Index != BootstrapMembershipIndex {
				continue
			}
			ms, err := initialMembershipFromEntry(entries[i])
			if err != nil {
				return proto.Membership{}, errors.Join(ErrCorruptConfigEntry, err)
			}
			m = newMembership(ms)
		case proto.EntryConfigChange:
			if len(entries[i].Cmd) == 0 {
				continue
			}
			var cc proto.ConfigChange
			if _, err := cc.UnmarshalFrom(entries[i].Cmd); err != nil {
				return proto.Membership{}, errors.Join(ErrCorruptConfigEntry,
					fmt.Errorf("config change entry %d (term %d): %w", entries[i].Index, entries[i].Term, err))
			}
			_ = m.apply(cc, entries[i].Index)
		}
	}
	return m.snapshot().toProto(), nil
}
