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

package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
)

const (
	// maxSnapshotFilepathLen is the maximum filepath length for snapshot
	// chunks. Corresponds to POSIX PATH_MAX.
	maxSnapshotFilepathLen = 4096

	// maxSnapshotChunkDataLen is the decoder's chunk Data limit (ErrChunkDataTooLong).
	maxSnapshotChunkDataLen = int(proto.MaxSnapshotChunkDataLen)
)

var (
	// snapshotChunkStructBytes is the resident size of one retained proto.SnapshotChunk, charged with each chunk.
	snapshotChunkStructBytes = int64(unsafe.Sizeof(proto.SnapshotChunk{}))

	// minSnapshotChunkResidentBytes is the least memory one chunk occupies: the smallest frame plus its struct.
	minSnapshotChunkResidentBytes = int64((&proto.SnapshotChunk{}).Size()) + snapshotChunkStructBytes

	// maxSnapshotChunkResidentBytes is the most memory one chunk occupies; it floors the per-peer memory ceiling.
	maxSnapshotChunkResidentBytes = minSnapshotChunkResidentBytes +
		int64(maxSnapshotChunkDataLen) + int64(maxSnapshotFilepathLen)
)

// maxRateLimitChunkSleep caps one rate-limit sleep; the rate still holds on average.
const maxRateLimitChunkSleep = 5 * time.Second

// Whole-transfer liveness bounds for an inbound snapshot; the per-frame deadline alone bounds nothing.
const (
	// maxSnapshotTransferDuration is the absolute ceiling on one transfer; it clamps transferBudget.
	maxSnapshotTransferDuration = time.Hour

	// minSnapshotTransferDuration is the floor on the per-transfer budget.
	minSnapshotTransferDuration = 5 * time.Minute

	// snapshotTransferRateMultiple is how many times the MaxSnapshotReceiveRate time a transfer may take.
	snapshotTransferRateMultiple = 16

	// snapshotProgressGracePeriod is how long a transfer may run before the
	// throughput floor is enforced, absorbing a slow start (stream setup,
	// sender disk reads) without failing a legitimate transfer.
	snapshotProgressGracePeriod = 60 * time.Second

	// minSnapshotReceiveBytesPerSec is the average throughput a transfer must
	// sustain past the grace period. Far below any real link, it exists only to
	// evict a peer trickling bytes to occupy a concurrency slot.
	minSnapshotReceiveBytesPerSec = 1024

	// maxConsecutiveZeroLengthFrames bounds zero-length chunk frames in a row; senders never emit them.
	maxConsecutiveZeroLengthFrames = 8
)

// chunkReader abstracts the stream operations needed for receiving snapshot
// chunks: setting a read deadline and reading bytes. This enables testing
// with mock streams.
type chunkReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

// EpochTracker manages in-flight epoch reference counts to prevent
// premature purge of encryption keys while snapshot transfers are
// in progress. The concrete implementation is seal.Barrier.
type EpochTracker interface {
	// RegisterInFlightEpoch increments the in-flight reference count.
	RegisterInFlightEpoch(epoch uint64)
	// DeregisterInFlightEpoch decrements the in-flight reference count.
	DeregisterInFlightEpoch(epoch uint64)
}

// snapshotReceiver manages concurrent snapshot receive operations with
// concurrency limits, memory budgets, and rate limiting.
type snapshotReceiver struct {
	// sem is a counting semaphore limiting concurrent snapshot receives
	// host-wide, across every peer.
	sem chan struct{}

	// peerMu protects peerUse.
	peerMu sync.Mutex

	// peerUse tracks snapshot-receive resources per peer identity (see snapshotFairnessKey).
	peerUse map[string]*peerSnapshotUse

	// maxPerPeer is the ceiling on concurrent receives from a single peer
	// identity: ceil(maxConcurrent/2), so no peer can occupy the whole budget.
	maxPerPeer int

	// maxPerPeerMemory caps one peer's in-flight receive memory, in proportion to maxPerPeer.
	maxPerPeerMemory int64

	// Whole-transfer liveness bounds, seeded from the constants; fields so tests can shorten them.
	maxTransferDuration time.Duration
	minTransferDuration time.Duration
	progressGracePeriod time.Duration
	minBytesPerSec      int64

	// memoryBudget tracks the remaining memory budget in bytes. Atomic
	// operations provide lock-free updates.
	memoryBudget atomic.Int64

	// maxMemory is the total memory budget configured at construction.
	maxMemory int64

	// bytesPerSec is the maximum receive rate for snapshot data.
	bytesPerSec int64

	// deploymentID is the expected deployment identifier. Snapshot chunks
	// from a different deployment are rejected to prevent cross-cluster
	// data corruption.
	deploymentID uint64
	// wire is the accept set for snapshot frames and chunks.
	wire WireWindow

	// handler processes received snapshot chunks.
	handler MessageHandler

	// epochTracker manages in-flight epoch references to prevent key
	// purge during snapshot receive. Nil when encryption is disabled.
	epochTracker EpochTracker

	// epochTrackerForShard resolves the in-flight epoch tracker per shard and takes precedence over epochTracker; nil with a single barrier.
	epochTrackerForShard func(shardID uint64) EpochTracker

	// snapshotTracker tracks in-flight snapshot receives so Stop() can
	// atomically reject new operations and wait for existing ones to
	// complete before closing connections. Nil when not set.
	snapshotTracker *shutdownTracker

	// listener holds optional event callbacks. May be nil.
	listener *config.EventListener

	// dispatchListener delivers OnSnapshotRejected on the host's listener
	// dispatcher (see setListenerDispatcher). Nil only for a receiver built
	// standalone by this package's tests.
	dispatchListener func(fn func()) bool

	// metrics collects transport-level metrics. Nil when disabled.
	metrics Metrics
}

// newSnapshotReceiver creates a snapshot receiver with the specified limits.
func newSnapshotReceiver(
	maxConcurrent int,
	maxMemory int64,
	bytesPerSec int64,
	deploymentID uint64,
	handler MessageHandler,
) *snapshotReceiver {
	maxPerPeer := (maxConcurrent + 1) / 2
	if maxPerPeer < 1 {
		maxPerPeer = 1
	}
	sr := &snapshotReceiver{
		sem:                 make(chan struct{}, maxConcurrent),
		peerUse:             make(map[string]*peerSnapshotUse),
		maxPerPeer:          maxPerPeer,
		maxPerPeerMemory:    perPeerSnapshotMemory(maxMemory, maxConcurrent, maxPerPeer),
		maxTransferDuration: maxSnapshotTransferDuration,
		minTransferDuration: minSnapshotTransferDuration,
		progressGracePeriod: snapshotProgressGracePeriod,
		minBytesPerSec:      minSnapshotReceiveBytesPerSec,
		maxMemory:           maxMemory,
		bytesPerSec:         bytesPerSec,
		deploymentID:        deploymentID,
		wire:                DefaultWireWindow(),
		handler:             handler,
	}
	sr.memoryBudget.Store(maxMemory)
	return sr
}

// peerSnapshotUse is one peer's held transfers and reserved memory, guarded by peerMu; memory is always refunded before its slot is released.
type peerSnapshotUse struct {
	slots    int
	reserved int64
}

// perPeerSnapshotMemory derives the per-peer memory ceiling in the slot proportion, floored at one chunk and clamped to the global budget.
func perPeerSnapshotMemory(maxMemory int64, maxConcurrent, maxPerPeer int) int64 {
	if maxMemory <= 0 {
		return maxMemory
	}
	limit := maxMemory
	if maxConcurrent > 0 && maxPerPeer < maxConcurrent {
		limit = maxMemory / int64(maxConcurrent) * int64(maxPerPeer)
	}
	if limit < maxSnapshotChunkResidentBytes {
		limit = maxSnapshotChunkResidentBytes
	}
	if limit > maxMemory {
		limit = maxMemory
	}
	return limit
}

// acquirePeerSlot reserves one of peerKey's concurrent-receive slots; peerKey comes from snapshotFairnessKey, and empty is not accounted.
func (sr *snapshotReceiver) acquirePeerSlot(peerKey string) bool {
	if peerKey == "" {
		return true
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	use := sr.peerUse[peerKey]
	if use == nil {
		use = &peerSnapshotUse{}
		sr.peerUse[peerKey] = use
	}
	if use.slots >= sr.maxPerPeer {
		if use.slots == 0 && use.reserved == 0 {
			delete(sr.peerUse, peerKey)
		}
		return false
	}
	use.slots++
	return true
}

// releasePeerSlot returns a peer identity's concurrent-receive slot, pruning
// the map entry once the peer holds neither a slot nor a reservation so it does
// not grow across peer churn.
func (sr *snapshotReceiver) releasePeerSlot(peerKey string) {
	if peerKey == "" {
		return
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	use, ok := sr.peerUse[peerKey]
	if !ok {
		return
	}
	if use.slots > 0 {
		use.slots--
	}
	if use.slots == 0 && use.reserved == 0 {
		delete(sr.peerUse, peerKey)
	}
}

// reservePeerMemory charges n bytes to peerKey's memory budget and reports whether it fit; empty is not accounted.
func (sr *snapshotReceiver) reservePeerMemory(peerKey string, n int64) bool {
	if peerKey == "" || sr.maxPerPeerMemory <= 0 {
		return true
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	use := sr.peerUse[peerKey]
	if use == nil {
		use = &peerSnapshotUse{}
		sr.peerUse[peerKey] = use
	}
	if use.reserved+n > sr.maxPerPeerMemory {
		if use.slots == 0 && use.reserved == 0 {
			delete(sr.peerUse, peerKey)
		}
		return false
	}
	use.reserved += n
	return true
}

// releasePeerMemory returns n charged bytes to peerKey's memory budget.
func (sr *snapshotReceiver) releasePeerMemory(peerKey string, n int64) {
	if peerKey == "" || sr.maxPerPeerMemory <= 0 {
		return
	}
	sr.peerMu.Lock()
	defer sr.peerMu.Unlock()
	use, ok := sr.peerUse[peerKey]
	if !ok {
		return
	}
	use.reserved -= n
	if use.reserved < 0 {
		use.reserved = 0
	}
	if use.slots == 0 && use.reserved == 0 {
		delete(sr.peerUse, peerKey)
	}
}

// chunkMemoryRejection names the ceiling that refused one chunk's reservation,
// so the caller can report which of the two budgets ran out rather than
// attributing every exhaustion to the host-global one.
type chunkMemoryRejection struct {
	detail string
	budget int64
}

// reserveChunkMemory charges n bytes against the global and per-peer budgets, leaving nothing charged on refusal; pair with releaseChunkMemory.
func (sr *snapshotReceiver) reserveChunkMemory(peerKey string, n int64) *chunkMemoryRejection {
	if remaining := sr.memoryBudget.Add(-n); remaining < 0 {
		sr.memoryBudget.Add(n)
		return &chunkMemoryRejection{detail: "receive memory budget exhausted", budget: sr.maxMemory}
	}
	if !sr.reservePeerMemory(peerKey, n) {
		sr.memoryBudget.Add(n)
		return &chunkMemoryRejection{detail: "per-peer receive memory budget exhausted", budget: sr.maxPerPeerMemory}
	}
	return nil
}

// releaseChunkMemory returns n previously-reserved bytes to both budgets.
func (sr *snapshotReceiver) releaseChunkMemory(peerKey string, n int64) {
	sr.memoryBudget.Add(n)
	sr.releasePeerMemory(peerKey, n)
}

// transferBudget returns how long a transfer of chunkCount maximal chunks may run at snapshotTransferRateMultiple times the rate, plus the grace period, clamped; a non-positive max disables it.
func (sr *snapshotReceiver) transferBudget(chunkCount uint64) time.Duration {
	if sr.maxTransferDuration <= 0 {
		return 0
	}
	if sr.bytesPerSec <= 0 {
		// No configured rate to derive from; fall back to the absolute ceiling.
		return sr.maxTransferDuration
	}

	// Saturate at the protocol ceiling. processSnapshotHeader already refuses a
	// larger declaration, but the arithmetic below must not depend on a caller
	// having done that: an unsaturated wire value overflows the product.
	if chunkCount > uint64(defaultMaxSnapshotChunkCount) {
		chunkCount = uint64(defaultMaxSnapshotChunkCount)
	}
	declaredBytes := int64(chunkCount) * int64(maxSnapshotChunkDataLen)

	// Compute in seconds and clamp before converting, or nanoseconds overflow.
	seconds := declaredBytes / sr.bytesPerSec
	if seconds > int64(sr.maxTransferDuration/time.Second)/snapshotTransferRateMultiple {
		return sr.maxTransferDuration
	}
	budget := sr.progressGracePeriod +
		time.Duration(seconds*snapshotTransferRateMultiple)*time.Second

	if budget < sr.minTransferDuration {
		budget = sr.minTransferDuration
	}
	if budget > sr.maxTransferDuration {
		budget = sr.maxTransferDuration
	}
	return budget
}

// cancelSnapshotReadOnStop resets the stream's read side when the transport stops; call the returned release when the transfer ends.
func cancelSnapshotReadOnStop(stream *quic.Stream, stopC <-chan struct{}) func() {
	if stream == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-stopC:
			stream.CancelRead(QUICStreamErrShutdown)
		case <-done:
		}
	}()
	return func() { close(done) }
}

// receiveSnapshot admits a snapshot stream, pins its non-zero epoch in flight and runs receiveChunks.
func (sr *snapshotReceiver) receiveSnapshot(stream *quic.Stream, hdr *SnapshotHeader, peerKey string, peer PeerAuth, stopC <-chan struct{}) {
	// Atomically register this receive operation. TryAdd returns false
	// if Stop() has already initiated shutdown, preventing the race
	// between WaitGroup.Add(1) and WaitGroup.Wait().
	if sr.snapshotTracker != nil {
		if !sr.snapshotTracker.TryAdd() {
			sr.rejectSnapshotStream(snapshotRejectShutdown, "transport shutting down", hdr, nil)
			return
		}
		defer sr.snapshotTracker.Done()
	}

	select {
	case <-stopC:
		sr.rejectSnapshotStream(snapshotRejectShutdown, "transport shutting down", hdr, nil)
		return
	default:
	}

	// Authorize the sender from the header before taking a slot or memory.
	if err := sr.handler.AuthorizeSnapshot(hdr.ShardID, hdr.ReplicaID, hdr.From, hdr.Index, peer); err != nil {
		if errors.Is(err, ErrSnapshotNotAccepting) {
			sr.rejectStream(snapshotRejectNotAccepting, "shard is not accepting snapshots", hdr, 0, err, slog.LevelDebug)
			return
		}
		sr.rejectSnapshotStream(snapshotRejectUnauthorized, "sender not authorized for this snapshot", hdr, err)
		return
	}

	// Per-peer fairness first; peerKey is the authenticated identity (see snapshotFairnessKey).
	if !sr.acquirePeerSlot(peerKey) {
		sr.rejectAuthorizedStream(snapshotRejectConcurrency, "per-peer snapshot concurrency limit reached", hdr)
		return
	}
	defer sr.releasePeerSlot(peerKey)

	// Try to acquire the host-global concurrency semaphore.
	select {
	case sr.sem <- struct{}{}:
		defer func() { <-sr.sem }()
	default:
		// At concurrency limit, drop the snapshot.
		sr.rejectAuthorizedStream(snapshotRejectConcurrency, "global snapshot concurrency limit reached", hdr)
		return
	}

	// Reset the stream's read side on shutdown so a transfer blocked in a
	// frame read cannot hold Stop() for the full per-frame deadline.
	defer cancelSnapshotReadOnStop(stream, stopC)()

	// Pin a non-zero epoch in flight, on the shard's barrier, so PurgeEpochsBefore keeps its DEK.
	if tracker := sr.trackerForShard(hdr.ShardID); tracker != nil && hdr.Epoch > 0 {
		tracker.RegisterInFlightEpoch(hdr.Epoch)
		defer tracker.DeregisterInFlightEpoch(hdr.Epoch)
	}

	sr.receiveChunks(stream, hdr, peerKey, peer, stopC)
}

// rateLimitChunkSleep returns the sleep for length bytes at bytesPerSec (> 0), capped at maxRateLimitChunkSleep.
func rateLimitChunkSleep(length uint32, bytesPerSec int64) time.Duration {
	d := time.Duration(length) * time.Second / time.Duration(bytesPerSec)
	if d > maxRateLimitChunkSleep {
		return maxRateLimitChunkSleep
	}
	return d
}

// receiveChunks reads snapshot chunks from the stream, enforces the memory
// budget, applies rate limiting, and dispatches chunks to the handler.
//
//nolint:gocyclo // chunk streaming interleaves budget reservation with four distinct release paths, liveness accounting and validation; the budget charge and its release must stay in one function, since a missed release is a permanent leak that eventually rejects every inbound snapshot host-wide
func (sr *snapshotReceiver) receiveChunks(stream chunkReader, hdr *SnapshotHeader, peerKey string, peer PeerAuth, stopC <-chan struct{}) { //nolint:gocognit // metrics instrumentation
	recvStart := time.Now()
	// Cap initial allocation to prevent a malicious header from causing an
	// oversized pre-allocation. The slice grows dynamically beyond this cap.
	const maxInitialChunkCap = 1024
	initCap := hdr.ChunkCount
	if initCap > maxInitialChunkCap {
		initCap = maxInitialChunkCap
	}
	chunks := make([]proto.SnapshotChunk, 0, initCap)
	// totalBytes counts wire bytes; charged also counts each retained chunk struct.
	var totalBytes, charged int64

	// Restore all reserved memory on every exit.
	defer func() {
		if charged > 0 {
			sr.releaseChunkMemory(peerKey, charged)
		}
	}()

	// Pre-allocate a stopped timer for rate limiting to avoid per-chunk
	// allocations that create GC pressure on large snapshots.
	rateLimitTimer := time.NewTimer(0)
	if !rateLimitTimer.Stop() {
		<-rateLimitTimer.C
	}
	defer rateLimitTimer.Stop()

	// The deadline derives from the declared work.
	transferBudget := sr.transferBudget(hdr.ChunkCount)

	// truncated marks a transfer that ended before all chunks arrived; it is never committed.
	truncated := false

	// zeroFrames counts consecutive zero-length chunk frames, which no
	// legitimate sender produces.
	zeroFrames := 0

	for i := uint64(0); i < hdr.ChunkCount; i++ {
		// Bound the transfer as a whole. The per-frame deadline below is reset
		// on every iteration, so without these a peer can trickle one frame
		// header per deadline and hold its concurrency slot for years.
		elapsed := time.Since(recvStart)
		if transferBudget > 0 && elapsed > transferBudget {
			sr.rejectSnapshot(hdr, snapshotRejectLiveness, "transfer exceeded maximum duration", totalBytes,
				&SnapshotTransferTimeoutError{
					Elapsed:       elapsed,
					Limit:         transferBudget,
					ReceivedBytes: totalBytes,
				})
			return
		}
		if sr.minBytesPerSec > 0 && elapsed > sr.progressGracePeriod &&
			totalBytes < int64(elapsed/time.Second)*sr.minBytesPerSec {
			sr.rejectSnapshot(hdr, snapshotRejectLiveness, "transfer below minimum throughput", totalBytes,
				&SnapshotProgressStalledError{
					Elapsed:        elapsed,
					ReceivedBytes:  totalBytes,
					MinBytesPerSec: sr.minBytesPerSec,
				})
			return
		}

		select {
		case <-stopC:
			// Stop requested: mark truncated and exit the loop. (A bare
			// break here would only break the select, not the for loop,
			// so set the flag and break the loop explicitly.)
			truncated = true
		default:
		}
		if truncated {
			break
		}

		if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			truncated = true
			break
		}

		length, _, err := readFrameHeaderWindow(stream, sr.wire)
		if err != nil {
			if errors.Is(err, io.EOF) {
				truncated = true
				break
			}
			sr.rejectSnapshot(hdr, snapshotRejectIO, "chunk frame read failed", totalBytes, err)
			return
		}

		if length == 0 {
			zeroFrames++
			if zeroFrames > maxConsecutiveZeroLengthFrames {
				sr.rejectSnapshot(hdr, snapshotRejectLiveness, "zero-length chunk frame flood", totalBytes,
					&SnapshotZeroLengthFrameFloodError{
						Count: zeroFrames,
						Max:   maxConsecutiveZeroLengthFrames,
					})
				return
			}
			continue
		}
		zeroFrames = 0

		// Charge the frame and its retained chunk struct before allocating.
		chunkCharge := int64(length) + snapshotChunkStructBytes
		if rejection := sr.reserveChunkMemory(peerKey, chunkCharge); rejection != nil {
			// Over budget. reserveChunkMemory leaves nothing charged on
			// refusal, so there is no inline restore to do here.
			sr.rejectSnapshot(hdr, snapshotRejectMemory, rejection.detail, totalBytes+int64(length),
				&SnapshotMemoryBudgetExceededError{
					Accumulated: charged + chunkCharge,
					Budget:      rejection.budget,
				})
			return
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(stream, buf); err != nil {
			// Read failed. This chunk was deducted from budget but
			// not yet added to charged, so restore it inline.
			sr.releaseChunkMemory(peerKey, chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectIO, "chunk body read failed", totalBytes, err)
			return
		}

		var chunk proto.SnapshotChunk
		if _, err := chunk.UnmarshalFromWindow(buf, sr.wire.Proto); err != nil {
			// Unmarshal failed. Same as above: restore inline since
			// this chunk is not yet tracked in charged.
			sr.releaseChunkMemory(peerKey, chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectMalformed, "chunk unmarshal failed", totalBytes, err)
			return
		}

		// Validate chunk fields (including consistency with the stream
		// header used for epoch pinning) to prevent oversized allocations
		// and cross-cluster data corruption.
		if err := sr.validateChunk(&chunk, hdr); err != nil {
			sr.releaseChunkMemory(peerKey, chunkCharge)
			sr.rejectSnapshot(hdr, snapshotRejectMalformed, "chunk validation failed", totalBytes, err)
			return
		}

		chunks = append(chunks, chunk)
		totalBytes += int64(length)
		charged += chunkCharge

		// Bound one receive's memory as well as the global budget.
		if charged > sr.maxMemory {
			sr.rejectSnapshot(hdr, snapshotRejectMemory, "per-receive memory budget exceeded", totalBytes,
				&SnapshotMemoryBudgetExceededError{
					Accumulated: charged,
					Budget:      sr.maxMemory,
				})
			return
		}

		// Apply rate limiting: sleep proportional to chunk size, capped so a
		// pathologically small configured rate cannot sleep for days while
		// holding the concurrency semaphore and memory budget.
		if sr.bytesPerSec > 0 {
			chunkDuration := rateLimitChunkSleep(length, sr.bytesPerSec)
			if chunkDuration > time.Microsecond {
				rateLimitTimer.Reset(chunkDuration)
				select {
				case <-stopC:
					// Shutdown during the rate-limit sleep: flow into the
					// truncated-transfer path below so the discard is logged
					// and reported like every other incomplete transfer.
					rateLimitTimer.Stop()
					truncated = true
				case <-rateLimitTimer.C:
				}
			}
		}
		if truncated {
			break
		}
	}

	// Commit only a complete transfer; a truncated or empty one is rejected.
	if truncated || hdr.ChunkCount == 0 || uint64(len(chunks)) != hdr.ChunkCount {
		// The rejection is reported before its diagnostic, which is
		// contained (see rejectStream).
		sr.notifyRejected(config.SnapshotInfo{
			ShardID:   hdr.ShardID,
			ReplicaID: hdr.ReplicaID,
			Index:     hdr.Index,
			From:      hdr.From,
		})
		received := len(chunks)
		supervise.LogContained(func() {
			slog.Warn("snapshot receive incomplete, discarding partial transfer",
				"shard", hdr.ShardID,
				"replica", hdr.ReplicaID,
				"index", hdr.Index,
				"received_chunks", received,
				"expected_chunks", hdr.ChunkCount,
			)
		})
		return
	}

	if len(chunks) > 0 {
		if err := sr.handler.HandleSnapshot(chunks, peer); err != nil {
			n := len(chunks)
			supervise.LogContained(func() {
				slog.Warn("snapshot handler failed",
					"shard", hdr.ShardID, "replica", hdr.ReplicaID, "chunks", n, "error", err)
			})
		} else {
			// The snapshot is handed off; its report is diagnostics,
			// contained, then OnSnapshotReceived and the metric, each
			// isolated so neither can end this receive goroutine.
			n := len(chunks)
			supervise.LogContained(func() {
				slog.Info("snapshot received",
					"shard", hdr.ShardID, "replica", hdr.ReplicaID, "index", hdr.Index, "chunks", n)
			})
			info := config.SnapshotInfo{ShardID: hdr.ShardID, ReplicaID: hdr.ReplicaID, Index: hdr.Index, From: hdr.From}
			var notify, observe func()
			if l := sr.listener; l != nil && l.OnSnapshotReceived != nil {
				cb := l.OnSnapshotReceived
				notify = func() { cb(info) }
			}
			if m := sr.metrics; m != nil {
				elapsed := time.Since(recvStart)
				observe = func() { m.IncSnapshotsReceived(hdr.ShardID, totalBytes, elapsed) }
			}
			deliverEvent(notify, observe)
		}
	}
}

// rejectSnapshot reports a discarded transfer: listener first, then a contained warning, then the counter by reason.
func (sr *snapshotReceiver) rejectSnapshot(hdr *SnapshotHeader, reason snapshotRejectReason, detail string, receivedBytes int64, err error) {
	sr.notifyRejected(config.SnapshotInfo{
		ShardID:   hdr.ShardID,
		ReplicaID: hdr.ReplicaID,
		Index:     hdr.Index,
		From:      hdr.From,
		Err:       err,
	})
	shardID, replicaID, index, chunkCount := hdr.ShardID, hdr.ReplicaID, hdr.Index, hdr.ChunkCount
	supervise.LogContained(func() {
		slog.Warn("snapshot receive rejected, discarding transfer",
			"reason", reason,
			"detail", detail,
			"shard", shardID,
			"replica", replicaID,
			"index", index,
			"received_bytes", receivedBytes,
			"expected_chunks", chunkCount,
			"error", err,
		)
	})
	if sr.metrics != nil {
		sr.metrics.IncSnapshotsRejected(shardID, string(reason))
	}
}

// rejectSnapshotStream reports a transfer discarded before any chunk was read; hdr may be nil. The counter uses shard 0 since the sender is unauthorized.
func (sr *snapshotReceiver) rejectSnapshotStream(reason snapshotRejectReason, detail string, hdr *SnapshotHeader, err error) {
	sr.rejectStream(reason, detail, hdr, 0, err, slog.LevelWarn)
}

// rejectAuthorizedStream is rejectSnapshotStream for a transfer whose sender
// AuthorizeSnapshot has admitted: its shard is verified, and is counted.
func (sr *snapshotReceiver) rejectAuthorizedStream(reason snapshotRejectReason, detail string, hdr *SnapshotHeader) {
	sr.rejectStream(reason, detail, hdr, hdr.ShardID, nil, slog.LevelWarn)
}

// rejectStream records a stream-level rejection, counting it under
// countedShard and logging it at level.
func (sr *snapshotReceiver) rejectStream(reason snapshotRejectReason, detail string, hdr *SnapshotHeader, countedShard uint64, err error, level slog.Level) {
	info := config.SnapshotInfo{Err: err}
	if hdr != nil {
		info.ShardID, info.ReplicaID, info.Index, info.From = hdr.ShardID, hdr.ReplicaID, hdr.Index, hdr.From
	}
	// Reported first, then the diagnostic (contained) and the metric: see
	// rejectSnapshot.
	sr.notifyRejected(info)
	supervise.LogContained(func() {
		slog.Log(context.Background(), level, "snapshot stream rejected, discarding transfer",
			"reason", reason,
			"detail", detail,
			"shard", info.ShardID,
			"replica", info.ReplicaID,
			"index", info.Index,
			"error", err,
		)
	})
	if sr.metrics != nil {
		sr.metrics.IncSnapshotsRejected(countedShard, string(reason))
	}
}

// notifyRejected delivers OnSnapshotRejected through the host's listener dispatcher, or inline when none is installed.
func (sr *snapshotReceiver) notifyRejected(info config.SnapshotInfo) {
	if sr.listener == nil || sr.listener.OnSnapshotRejected == nil {
		return
	}
	onRejected := sr.listener.OnSnapshotRejected
	deliver := func() { onRejected(info) }
	if sr.dispatchListener == nil {
		deliverEvent(deliver)
		return
	}
	if !sr.dispatchListener(deliver) {
		// Dropped on a full queue, as the engine drops its own notifications
		// (counted in ListenerNotificationsDropped); named so an operator can
		// tell which event a shard never delivered.
		supervise.LogContained(func() {
			slog.Error("listener notification dropped: the EventListener is not consuming",
				"event", "OnSnapshotRejected",
				"shard_id", info.ShardID)
		})
	}
}

// setListenerDispatcher installs the function that delivers OnSnapshotRejected
// off the receive goroutine. Must be called before any snapshot receives begin.
func (sr *snapshotReceiver) setListenerDispatcher(dispatch func(fn func()) bool) {
	sr.dispatchListener = dispatch
}

// SetEpochTracker sets the in-flight epoch tracker; call before any receive begins.
func (sr *snapshotReceiver) SetEpochTracker(tracker EpochTracker) {
	sr.epochTracker = tracker
}

// SetShardEpochTrackers sets a per-shard tracker resolver that takes precedence over the single tracker; call before any receive begins.
func (sr *snapshotReceiver) SetShardEpochTrackers(fn func(shardID uint64) EpochTracker) {
	sr.epochTrackerForShard = fn
}

// trackerForShard returns the shard's epoch tracker, falling back to the single one, or nil when none is configured.
func (sr *snapshotReceiver) trackerForShard(shardID uint64) EpochTracker {
	if sr.epochTrackerForShard != nil {
		if t := sr.epochTrackerForShard(shardID); t != nil {
			return t
		}
	}
	return sr.epochTracker
}

// SetSnapshotTracker sets the tracker Stop() uses to refuse new receives and wait for running ones.
func (sr *snapshotReceiver) SetSnapshotTracker(tracker *shutdownTracker) {
	sr.snapshotTracker = tracker
}

// validateChunk bounds-checks a chunk, checks its DeploymentID and requires its ShardID, Index and Epoch to match hdr.
func (sr *snapshotReceiver) validateChunk(chunk *proto.SnapshotChunk, hdr *SnapshotHeader) error {
	if len(chunk.Filepath) > maxSnapshotFilepathLen {
		return &ChunkFilepathTooLongError{
			ShardID: chunk.ShardID,
			Length:  len(chunk.Filepath),
			Max:     maxSnapshotFilepathLen,
		}
	}
	if chunk.Filepath != "" {
		// Filepath is informational and never written to; reject ".." traversal, and sanitize it if it ever becomes a write path.
		cleaned := filepath.Clean(chunk.Filepath)
		if cleaned == ".." ||
			strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) ||
			strings.Contains(cleaned, string(filepath.Separator)+".."+string(filepath.Separator)) {
			return &ChunkFilepathTraversalError{
				ShardID:  chunk.ShardID,
				Filepath: chunk.Filepath,
			}
		}
	}
	if len(chunk.Data) > maxSnapshotChunkDataLen {
		return &ChunkDataTooLargeError{
			ShardID: chunk.ShardID,
			Length:  len(chunk.Data),
			Max:     maxSnapshotChunkDataLen,
		}
	}
	if sr.deploymentID != 0 && chunk.DeploymentID != sr.deploymentID {
		return &ChunkDeploymentIDMismatchError{
			ShardID:  chunk.ShardID,
			Expected: sr.deploymentID,
			Got:      chunk.DeploymentID,
		}
	}
	if hdr != nil {
		if chunk.ShardID != hdr.ShardID {
			return &ChunkHeaderMismatchError{
				Field:       "shard_id",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.ShardID,
				ChunkValue:  chunk.ShardID,
			}
		}
		if chunk.Index != hdr.Index {
			return &ChunkHeaderMismatchError{
				Field:       "index",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.Index,
				ChunkValue:  chunk.Index,
			}
		}
		if chunk.Epoch != hdr.Epoch {
			return &ChunkHeaderMismatchError{
				Field:       "epoch",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.Epoch,
				ChunkValue:  chunk.Epoch,
			}
		}
		// The header's sender and target are what AuthorizeSnapshot admitted;
		// a chunk naming others would be persisted on an authorization it
		// never received.
		if chunk.From != hdr.From {
			return &ChunkHeaderMismatchError{
				Field:       "from",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.From,
				ChunkValue:  chunk.From,
			}
		}
		if chunk.ReplicaID != hdr.ReplicaID {
			return &ChunkHeaderMismatchError{
				Field:       "replica_id",
				ChunkID:     chunk.ChunkID,
				HeaderValue: hdr.ReplicaID,
				ChunkValue:  chunk.ReplicaID,
			}
		}
	}
	return nil
}

// maxAdmissibleChunkCount returns the largest ChunkCount this receiver admits: the protocol ceiling, lowered to what the memory budget can hold.
func (sr *snapshotReceiver) maxAdmissibleChunkCount() uint64 {
	limit := uint64(defaultMaxSnapshotChunkCount)
	if sr.maxMemory > 0 {
		if budgeted := uint64(sr.maxMemory / minSnapshotChunkResidentBytes); budgeted < limit {
			limit = budgeted
		}
	}
	return limit
}

// AvailableMemory returns the remaining memory budget for snapshot receives.
// Used for testing and monitoring.
func (sr *snapshotReceiver) AvailableMemory() int64 {
	return sr.memoryBudget.Load()
}
