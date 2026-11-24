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

package quicraft

import (
	"context"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// wireSwitchRetryInterval bounds how often a leader re-examines a shard for
// the wire-version switch: it copies the membership to do so, and a proposal
// that was lost is retried no sooner than this.
const wireSwitchRetryInterval = time.Second

// wireSwitchEnabled reports whether this build's window spans two versions.
// A build whose current and previous versions are the same has nothing to
// switch to, so it neither records what peers advertise nor proposes.
func (h *Host) wireSwitchEnabled() bool {
	return h.wire.Proto.Current != h.wire.Proto.Previous
}

// notePeerWireVersion records the MaxBinVer replica from advertised for
// shardID on its latest batch, then gives the shard's leader the chance to
// propose the switch. The latest value is kept, not the highest: a member
// rolled back to an older build must stop counting as upgraded.
func (h *Host) notePeerWireVersion(shardID, from, maxBinVer uint64) {
	ns, err := h.getNode(shardID)
	if err != nil {
		return
	}
	ns.peerWireMu.Lock()
	if ns.peerWire == nil {
		ns.peerWire = make(map[uint64]uint64)
	}
	ns.peerWire[from] = maxBinVer
	ns.peerWireMu.Unlock()
	h.maybeProposeWireSwitch(ns)
}

// maybeProposeWireSwitch proposes UpdateWireVersion to this build's current
// version when this replica leads the shard, the shard has not switched to
// it, and every other voter, non-voting member and witness last advertised
// a MaxBinVer at or above it. A member never heard from counts as previous.
// The proposal passes the same validation as any config change, so a value
// the committed version already reached is refused there.
func (h *Host) maybeProposeWireSwitch(ns *nodeState) {
	if !ns.isLeader.Load() || h.engine == nil {
		return
	}
	engNode := h.engine.GetNode(ns.shardID)
	if engNode == nil || engNode.Peer() == nil {
		return
	}
	target := h.wire.Proto.Current
	if proto.EffectiveWireVersion(engNode.Peer().WireVersion()) >= target {
		return
	}
	now := time.Now().UnixNano()
	last := ns.wireSwitchAt.Load()
	if now-last < int64(wireSwitchRetryInterval) || !ns.wireSwitchAt.CompareAndSwap(last, now) {
		return
	}
	m := engNode.Peer().GetMembership()
	if !ns.membersAdvertise(m, target) {
		return
	}
	// The proposal names the membership just checked. The step worker refuses
	// it if any config change has applied since, so a member added in between
	// is never switched before it has advertised the version.
	cc := proto.ConfigChange{
		Type:           proto.UpdateWireVersion,
		WireVersion:    target,
		ConfigChangeID: m.ConfigChangeID,
	}
	var rs *RequestState
	var err error
	if h.wireSwitchFault != nil {
		err = h.wireSwitchFault()
	}
	if err == nil {
		rs, err = h.requestConfigChange(context.Background(), ns.shardID, cc)
	}
	if err != nil {
		h.logger.Debug("wire-version switch not proposed",
			"shard_id", ns.shardID, "wire_version", target, "error", err)
		return
	}
	rs.Release()
	h.logger.Info("proposed wire-version switch",
		"shard_id", ns.shardID, "wire_version", target)
}

// retryWireSwitch gives a shard this replica leads in term its own attempts
// at the switch, one per wireSwitchRetryInterval. A shard with no other
// member receives no batch to prompt one, so without it a refusal on the
// first attempt (a busy or paused shard, a membership that changed under
// the check) would leave the shard on the previous version for the term. It
// stops once the shard has switched or gained a member, whose batches then
// prompt the leader, when this replica stops leading it in term, and when
// the shard or host stops. Close joins it through wireSwitchWG.
func (h *Host) retryWireSwitch(ns *nodeState, term uint64) {
	defer h.wireSwitchWG.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-h.sweepStopC:
			return
		case <-timer.C:
		}
		if !h.wireSwitchPending(ns, term) {
			return
		}
		h.maybeProposeWireSwitch(ns)
		// Reset after the attempt so the next one clears its rate limit.
		timer.Reset(wireSwitchRetryInterval)
	}
}

// wireSwitchPending reports whether retryWireSwitch should keep trying: this
// replica still leads the shard in term, the shard has no other member, and
// it has not switched to this build's current version.
func (h *Host) wireSwitchPending(ns *nodeState, term uint64) bool {
	if h.closed.Load() || h.engine == nil || ns.stopped.Load() || !ns.isLeader.Load() || ns.term.Load() != term {
		return false
	}
	engNode := h.engine.GetNode(ns.shardID)
	if engNode == nil || engNode.Peer() == nil {
		return false
	}
	if proto.EffectiveWireVersion(engNode.Peer().WireVersion()) >= h.wire.Proto.Current {
		return false
	}
	m := engNode.Peer().GetMembership()
	return len(m.Addresses)+len(m.Observers)+len(m.Witnesses) <= 1
}

// membersAdvertise reports whether every member of m other than this replica
// last advertised a MaxBinVer of at least target.
func (ns *nodeState) membersAdvertise(m proto.Membership, target uint64) bool {
	ns.peerWireMu.Lock()
	defer ns.peerWireMu.Unlock()
	for _, members := range []map[uint64]string{m.Addresses, m.Observers, m.Witnesses} {
		for id := range members {
			if id == ns.replicaID {
				continue
			}
			if v, ok := ns.peerWire[id]; !ok || v < target {
				return false
			}
		}
	}
	return true
}
