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

package engine

// epochPinner seals snapshot frames and pins every distinct epoch used, so a rotation and purge cannot retire a needed DEK; not safe for concurrent use.
type epochPinner struct {
	barrier SnapshotBarrier
	seen    map[uint64]struct{}
	order   []uint64
}

// newEpochPinner returns a pinner for one encrypt loop over barrier.
func newEpochPinner(barrier SnapshotBarrier) *epochPinner {
	return &epochPinner{
		barrier: barrier,
		seen:    make(map[uint64]struct{}),
	}
}

// track records an epoch the barrier has just pinned on this pinner's behalf.
// A duplicate reference is released immediately, so the count stays at exactly
// one per distinct epoch.
func (p *epochPinner) track(epoch uint64) {
	if epoch == 0 {
		return
	}
	if _, dup := p.seen[epoch]; dup {
		p.barrier.DeregisterInFlightEpoch(epoch)
		return
	}
	p.seen[epoch] = struct{}{}
	p.order = append(p.order, epoch)
}

// sealFrame encrypts one frame bound to contextAAD and pins its epoch.
func (p *epochPinner) sealFrame(plain, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := p.barrier.EncryptWithEpochPinnedContext(nil, plain, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	p.track(epoch)
	return ct, epoch, nil
}

// epochs returns the distinct epochs pinned, in first-seen order. The caller
// must release each exactly once once the artifact's epoch is durable.
func (p *epochPinner) epochs() []uint64 { return p.order }

// releaseAll drops every pin this pinner took. Used on the error paths, where
// the caller never receives the set.
func (p *epochPinner) releaseAll() {
	for _, e := range p.order {
		p.barrier.DeregisterInFlightEpoch(e)
	}
	p.order = nil
	p.seen = make(map[uint64]struct{})
}
