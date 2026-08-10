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

// epochPinner seals snapshot frames while keeping EVERY epoch they are sealed
// under pinned in-flight on the barrier, so a concurrent key rotation +
// PurgeEpochsBefore cannot retire the DEK of a frame that is already in the
// artifact — which would leave that snapshot permanently undecryptable.
//
// Two hazards, both closed here:
//
//   - Pinning only the MINIMUM epoch leaves an INTERMEDIATE epoch exposed once
//     two rotations land during one encrypt loop. Every distinct epoch is pinned.
//   - EncryptWithEpoch releases the barrier lock before returning, so a rotation
//     plus a purge can complete in the gap before the caller pins the returned
//     epoch. When the barrier implements atomicEpochPinner, the seal and the pin
//     happen under one lock and no purge can interleave; otherwise the loop falls
//     back to pin-before + pin-after, which covers everything except that gap.
//
// Each distinct epoch is pinned exactly once, so the caller releases each
// exactly once via releaseAll — after the snapshot's epoch is durably recorded.
// Not safe for concurrent use; one pinner belongs to one encrypt loop.
type epochPinner struct {
	barrier SnapshotBarrier
	atomic  atomicEpochPinner
	hasAtom bool
	seen    map[uint64]struct{}
	order   []uint64
}

// newEpochPinner returns a pinner for one encrypt loop over barrier.
func newEpochPinner(barrier SnapshotBarrier) *epochPinner {
	a, ok := barrier.(atomicEpochPinner)
	return &epochPinner{
		barrier: barrier,
		atomic:  a,
		hasAtom: ok,
		seen:    make(map[uint64]struct{}),
	}
}

// track records an epoch this pinner is now responsible for releasing.
// alreadyPinned reports whether the barrier has already taken the reference (the
// atomic path); a duplicate reference is released immediately so the count stays
// at exactly one per distinct epoch.
func (p *epochPinner) track(epoch uint64, alreadyPinned bool) {
	if epoch == 0 {
		return
	}
	if _, dup := p.seen[epoch]; dup {
		if alreadyPinned {
			p.barrier.DeregisterInFlightEpoch(epoch)
		}
		return
	}
	if !alreadyPinned {
		p.barrier.RegisterInFlightEpoch(epoch)
	}
	p.seen[epoch] = struct{}{}
	p.order = append(p.order, epoch)
}

// sealFrame encrypts one frame and pins the epoch it was sealed under.
//
// contextAAD is the frame's positional context (see engine.SnapshotAAD); it is
// folded into the AEAD AAD alongside the epoch so a frame cannot be replayed at
// a different ordinal, snapshot index, replica or shard. A nil context is
// byte-identical to the epoch-only binding and is what a hardware-mode barrier
// requires.
func (p *epochPinner) sealFrame(plain, contextAAD []byte) ([]byte, uint64, error) {
	if p.hasAtom {
		ct, epoch, err := p.atomic.EncryptWithEpochPinnedContext(nil, plain, contextAAD)
		if err != nil {
			return nil, 0, err
		}
		p.track(epoch, true)
		return ct, epoch, nil
	}
	// Pin the current epoch before sealing so it cannot be purged in between,
	// then pin the epoch actually used in case a rotation landed mid-call.
	p.track(p.barrier.CurrentEpoch(), false)
	ct, epoch, err := p.barrier.EncryptWithEpochContext(nil, plain, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	p.track(epoch, false)
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
