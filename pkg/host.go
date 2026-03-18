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
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/rsm"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

const (
	// hostIDFileName is the file that stores the host's unique identifier.
	hostIDFileName = "host.id"

	// hostIDLength is the expected length of the host ID hex string.
	hostIDLength = 32

	// hostIDFilePerms is the file permissions for the host ID file.
	hostIDFilePerms = 0o600

	// pendingSweepInterval is how often the background sweeper scans
	// all node states for orphaned pending proposals, reads, and
	// snapshots whose deadlines have expired. Expired entries are
	// completed with ErrTimeout to prevent callers from blocking
	// indefinitely when entries are lost (leader crash, partition).
	pendingSweepInterval = 10 * time.Second

	// loopPanicRestartDelay is the delay before restarting a panicked
	// background loop. This prevents tight panic-restart loops from
	// consuming CPU and flooding logs when a loop hits a persistent
	// panic trigger.
	loopPanicRestartDelay = 100 * time.Millisecond
)

// Barrier is the interface for at-rest encryption barriers. This interface
// is defined in the quicraft package to break the import cycle between
// quicraft and quicraft/crypto. The crypto.Barrier type satisfies this
// interface.
type Barrier interface {
	// IsSealed returns true if the barrier is in the sealed state.
	IsSealed() bool

	// Seal transitions the barrier to the sealed state, zeroing all
	// key material from memory.
	Seal() error

	// Encrypt encrypts plaintext using the current epoch's DEK.
	// dst may be provided as a pre-allocated buffer to reduce allocations.
	Encrypt(dst, plaintext []byte) ([]byte, error)

	// Decrypt decrypts ciphertext. The epoch is extracted from the
	// ciphertext prefix and the corresponding DEK is used.
	Decrypt(dst, ciphertext []byte) ([]byte, error)

	// EncryptWithEpoch encrypts plaintext and returns both the ciphertext
	// and the epoch used for encryption.
	EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

	// DecryptForEpoch decrypts ciphertext using a specific epoch's DEK.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// Rotate increments the epoch, derives a new DEK, and installs it
	// as the current cipher. Returns the new epoch number.
	Rotate() (uint64, error)

	// PurgeEpochsBefore removes DEKs for epochs strictly less than minEpoch.
	// Returns the number of epochs actually purged.
	PurgeEpochsBefore(minEpoch uint64) (int, error)

	// RegisterInFlightEpoch increments the in-flight reference count for
	// an epoch, preventing PurgeEpochsBefore from removing it.
	RegisterInFlightEpoch(epoch uint64)

	// DeregisterInFlightEpoch decrements the in-flight reference count
	// for an epoch.
	DeregisterInFlightEpoch(epoch uint64)

	// CurrentEpoch returns the current key rotation epoch.
	CurrentEpoch() uint64

	// ShouldRotateKey returns true when the AEAD invocation counter has
	// reached its rotation threshold percentage. Returns false if invocation
	// counting is disabled or the barrier is sealed.
	ShouldRotateKey() bool
}

// hostOptions holds optional dependencies injected via HostOption functions.
type hostOptions struct {
	logdb            logdb.LogDB
	barrier          Barrier
	metrics          MetricsCollector
	logger           *slog.Logger
	disableTransport bool
}

// HostOption configures optional Host dependencies.
type HostOption func(*hostOptions)

// WithLogDB provides a custom LogDB implementation. When not set, the
// default WAL-based LogDB is created from the HostConfig directories.
func WithLogDB(l logdb.LogDB) HostOption {
	return func(o *hostOptions) {
		o.logdb = l
	}
}

// WithBarrier enables at-rest encryption with the given barrier.
// The barrier must already be initialized and unsealed.
func WithBarrier(b Barrier) HostOption {
	return func(o *hostOptions) {
		o.barrier = b
	}
}

// WithMetrics enables metrics collection with the given collector. The
// collector's sub-interfaces are threaded into the engine (RaftMetrics),
// transport (TransportMetrics), and WAL LogDB (LogDBMetrics) pipelines.
func WithMetrics(c MetricsCollector) HostOption {
	return func(o *hostOptions) {
		o.metrics = c
	}
}

// WithMemoryLogDB uses an in-memory LogDB instead of the default
// WAL-based storage. Suitable for benchmarks, CI/CD, and ephemeral
// nodes where durability is not required.
func WithMemoryLogDB() HostOption {
	return func(o *hostOptions) {
		o.logdb = memdb.New()
	}
}

// WithLogger provides a pre-configured *slog.Logger. When set, the
// Host uses this logger instead of configuring one from LogConfig.
// Useful for testing or when the caller needs a custom slog.Handler.
func WithLogger(l *slog.Logger) HostOption {
	return func(o *hostOptions) {
		o.logger = l
	}
}

// WithoutTransport disables the QUIC transport layer. Messages are
// silently dropped via a no-op sender. Use for single-node testing
// only, where no remote message delivery is needed.
func WithoutTransport() HostOption {
	return func(o *hostOptions) {
		o.disableTransport = true
	}
}

// noopSender is a no-op MessageSender used when no transport is
// configured. Messages are silently dropped. This is safe because
// in a single-node cluster no remote delivery is needed, and the
// transport will be wired in a later phase.
type noopSender struct{}

// Send discards all messages. Raft handles retransmission, so dropped
// messages do not affect correctness.
func (s *noopSender) Send(_ []proto.Message) {}

// SendSnapshot discards snapshot messages. In a single-node cluster
// there are no followers that need snapshot transfers.
func (s *noopSender) SendSnapshot(_ proto.Message) {}

// Compile-time assertion that noopSender satisfies engine.MessageSender.
var _ engine.MessageSender = (*noopSender)(nil)

// snapshotChunkSize is the byte size of each chunk when splitting a
// snapshot file for transport. Matches dragonboat's default of 2MB.
const snapshotChunkSize = 2 * 1024 * 1024

// transportSender wraps a QUICTransport to implement the full
// engine.MessageSender interface, including the snapshot streaming
// path. It bridges the gap between the engine's message-level API
// and the transport's chunk-level snapshot API.
type transportSender struct {
	transport  *transport.QUICTransport
	engine     *engine.Engine
	barrier    Barrier
	logger     *slog.Logger
	snapshotWg sync.WaitGroup // tracks in-flight snapshot sends
}

// Send delegates regular message delivery to the transport.
func (s *transportSender) Send(msgs []proto.Message) {
	s.transport.Send(msgs)
}

// SendSnapshot initiates an asynchronous snapshot file transfer. It
// splits the InstallSnapshot message into chunks by reading the
// snapshot data file from disk, sends them via the transport's chunked
// streaming path, and delivers a SnapshotStatus message back to the
// raft layer on completion.
//
// The transfer runs in a background goroutine so the step worker is
// not blocked. On success, a non-rejected SnapshotStatus is injected;
// on failure, a rejected SnapshotStatus tells the leader to transition
// the remote out of snapshot state so replication can be retried.
func (s *transportSender) SendSnapshot(msg proto.Message) {
	s.snapshotWg.Add(1)
	go func() {
		defer s.snapshotWg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic recovered in doSendSnapshot",
					"shard", msg.ShardID,
					"from", msg.From,
					"to", msg.To,
					"panic", fmt.Sprint(r),
					"stack", string(debug.Stack()),
				)
				// Notify the raft layer that the snapshot was rejected so
				// the leader transitions the remote out of snapshot state.
				// Without this, the follower is permanently stuck.
				s.safeNotifySnapshotRejected(msg.ShardID, msg.From, msg.To)
			}
		}()
		s.doSendSnapshot(msg)
	}()
}

// safeNotifySnapshotRejected attempts to deliver a rejected SnapshotStatus
// notification, recovering from any secondary panic (e.g., if the engine
// is already torn down). This prevents a double-panic in the deferred
// recovery handler of SendSnapshot from crashing the process.
func (s *transportSender) safeNotifySnapshotRejected(shardID, from, to uint64) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in snapshot rejection notification",
				"shard", shardID,
				"from", from,
				"to", to,
				"panic", fmt.Sprint(r),
			)
		}
	}()
	s.notifySnapshotStatus(shardID, from, to, true)
}

// Wait blocks until all in-flight snapshot sends complete. Called by
// Host.Close() to drain pending snapshot transfers before shutting
// down the transport.
func (s *transportSender) Wait() {
	s.snapshotWg.Wait()
}

// doSendSnapshot performs the synchronous snapshot transfer work.
// It reads the file, splits it into chunks, and sends them via
// the transport. On completion, it injects a SnapshotStatus message
// into the sender's raft inbox.
//
// When the snapshot was encrypted at rest (Epoch > 0) and a barrier
// is available, the file is decrypted to a temporary file before
// sending. The receiver gets plaintext data and stores it with
// Epoch=0, so its own barrier (which has a different key) does not
// need to decrypt it. Each node's barrier encrypts independently
// for at-rest protection; the QUIC transport provides in-transit
// encryption via mTLS.
func (s *transportSender) doSendSnapshot(msg proto.Message) {
	// resourceCleanup tracks resources that must be cleaned up on exit,
	// whether by normal return, error, or panic. Resources are cleaned
	// up in reverse order of registration.
	var resourceCleanup []func()
	cleanup := func() {
		for i := len(resourceCleanup) - 1; i >= 0; i-- {
			resourceCleanup[i]()
		}
	}
	defer cleanup()

	// If the snapshot is encrypted, decrypt to a temp file so the
	// receiver (which has a different barrier key) can read it.
	if msg.Snapshot.Epoch > 0 && s.barrier != nil && !s.barrier.IsSealed() {
		decryptedPath, decryptedSize, err := s.decryptSnapshotForSend(msg)
		if err != nil {
			s.logger.Warn("snapshot decrypt-for-send failed",
				"shard", msg.ShardID,
				"to", msg.To,
				"epoch", msg.Snapshot.Epoch,
				"error", err,
			)
			s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, true)
			return
		}
		// Register temp file cleanup immediately after successful creation.
		// This ensures cleanup even if a panic occurs later in this function.
		tempPath := decryptedPath
		resourceCleanup = append(resourceCleanup, func() {
			if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
				s.logger.Debug("decrypted snapshot temp file removal failed",
					"path", tempPath,
					"error", err)
			}
		})
		// Update the message to reference the decrypted file.
		msg.Snapshot.Filepath = decryptedPath
		msg.Snapshot.FileSize = decryptedSize
		msg.Snapshot.Epoch = 0
	}

	// Stream snapshot chunks directly to the transport. The
	// forEachSnapshotChunk callback reads one chunk at a time from disk,
	// and SendSnapshotStreaming writes it to the wire immediately, so at
	// most one chunk (snapshotChunkSize = 2MB) is held in memory at any
	// point rather than the entire snapshot file.
	snap := &msg.Snapshot
	fileSize := snap.FileSize
	var chunkCount uint64
	if snap.Filepath == "" || fileSize == 0 {
		chunkCount = 1
	} else {
		chunkCount = (fileSize-1)/snapshotChunkSize + 1
	}

	meta := transport.SnapshotHeader{
		ShardID:    msg.ShardID,
		ReplicaID:  msg.To,
		Index:      snap.Index,
		Term:       snap.Term,
		ChunkCount: chunkCount,
		Epoch:      snap.Epoch,
	}

	producer := func(emit func(proto.SnapshotChunk) error) error {
		return forEachSnapshotChunk(msg, emit)
	}

	if err := s.transport.SendSnapshotStreaming(meta, producer); err != nil {
		s.logger.Warn("snapshot send failed",
			"shard", msg.ShardID,
			"to", msg.To,
			"error", err,
		)
		s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, true)
		return
	}

	s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, false)
}

// decryptSnapshotForSend decrypts an encrypted snapshot file to a
// temporary file for network transfer. The encrypted format is a
// sequence of frames: [4-byte LE length][encrypted chunk]...
// Returns the temp file path and the plaintext file size.
func (s *transportSender) decryptSnapshotForSend(msg proto.Message) (string, uint64, error) {
	srcFile, err := os.Open(msg.Snapshot.Filepath)
	if err != nil {
		return "", 0, err
	}
	// Close source file on all exit paths. Using a named variable allows
	// the defer to be registered immediately after the successful open,
	// eliminating the window for resource leaks on panic.
	srcClosed := false
	defer func() {
		if !srcClosed {
			if closeErr := srcFile.Close(); closeErr != nil {
				slog.Debug("snapshot source file close failed",
					"shard", msg.ShardID,
					"path", msg.Snapshot.Filepath,
					"error", closeErr)
			}
		}
	}()

	dir := filepath.Dir(msg.Snapshot.Filepath)
	tmpFile, err := os.CreateTemp(dir, ".send-decrypt-*.tmp")
	if err != nil {
		return "", 0, err
	}
	// Capture temp file path immediately and register cleanup in the same
	// statement block. The committed flag tracks whether the file should
	// be kept (on success) or cleaned up (on any error or panic).
	tmpPath := tmpFile.Name()
	committed := false
	defer func() {
		if !committed {
			// Close the file handle first, ignoring "already closed" errors
			// since we may have closed it explicitly before an error.
			if closeErr := tmpFile.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
				slog.Debug("snapshot temp file close failed",
					"shard", msg.ShardID,
					"path", tmpPath,
					"error", closeErr)
			}
			// Remove the temp file, ignoring "not exist" errors since
			// file creation may have been rolled back by the OS.
			if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
				slog.Debug("snapshot temp file removal failed",
					"shard", msg.ShardID,
					"path", tmpPath,
					"error", rmErr)
			}
		}
	}()

	// Read and decrypt each frame.
	epoch := msg.Snapshot.Epoch
	var frameLenBuf [4]byte
	var totalSize uint64
	for {
		_, readErr := io.ReadFull(srcFile, frameLenBuf[:])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", 0, readErr
		}
		frameLen := binary.LittleEndian.Uint32(frameLenBuf[:])

		frame := make([]byte, frameLen)
		if _, readErr = io.ReadFull(srcFile, frame); readErr != nil {
			return "", 0, readErr
		}

		plaintext, decErr := s.barrier.DecryptForEpoch(nil, frame, epoch)
		if decErr != nil {
			return "", 0, decErr
		}

		if _, writeErr := tmpFile.Write(plaintext); writeErr != nil {
			return "", 0, writeErr
		}
		totalSize += uint64(len(plaintext))
	}

	// Close source file early to release the handle.
	if err := srcFile.Close(); err != nil {
		return "", 0, err
	}
	srcClosed = true

	if err := tmpFile.Sync(); err != nil {
		return "", 0, err
	}
	if err := tmpFile.Close(); err != nil {
		return "", 0, err
	}
	committed = true
	return tmpPath, totalSize, nil
}

// notifySnapshotStatus injects a SnapshotStatus message into the
// sender's raft inbox to inform the leader of the transfer outcome.
// The From field is set to the target replica (the follower that
// received or rejected the snapshot) so the leader's raft layer
// can look up the correct remote entry.
func (s *transportSender) notifySnapshotStatus(shardID, localReplicaID, targetReplicaID uint64, rejected bool) {
	statusMsg := proto.Message{
		Type:    proto.SnapshotStatus,
		ShardID: shardID,
		From:    targetReplicaID,
		To:      localReplicaID,
		Reject:  rejected,
	}
	if !s.engine.DeliverMessage(shardID, statusMsg) {
		s.logger.Warn("failed to deliver SnapshotStatus",
			"shard", shardID,
			"target", targetReplicaID,
			"rejected", rejected,
		)
	}
	s.engine.NotifyWork(shardID)
}

// forEachSnapshotChunk reads the snapshot data file referenced by the
// InstallSnapshot message and streams transport-level chunks to the
// provided emit callback one at a time. Only a single chunk's worth of
// data (snapshotChunkSize = 2MB) is held in memory at any point,
// preventing OOM on large snapshots (up to DefaultMaxSnapshotSize = 4GB).
//
// The emit callback is invoked synchronously for each chunk. If emit
// returns an error, iteration stops and the error is returned to the
// caller. This streaming design allows the transport layer to send
// each chunk over the wire as it is produced, avoiding the O(fileSize)
// memory overhead of materializing all chunks in a slice.
func forEachSnapshotChunk(msg proto.Message, emit func(proto.SnapshotChunk) error) error {
	snap := &msg.Snapshot
	filePath := snap.Filepath
	fileSize := snap.FileSize

	if filePath == "" || fileSize == 0 {
		// Witness or empty snapshot: emit a single metadata-only chunk.
		return emit(proto.SnapshotChunk{
			ShardID:        msg.ShardID,
			ReplicaID:      msg.To,
			From:           msg.From,
			Index:          snap.Index,
			Term:           snap.Term,
			ChunkID:        0,
			ChunkCount:     1,
			ChunkSize:      0,
			FileSize:       0,
			Filepath:       filePath,
			FileChunkID:    0,
			FileChunkCount: 1,
			Membership:     snap.Membership,
			OnDiskIndex:    snap.OnDiskIndex,
			BinVer:         proto.WireVersion,
			Epoch:          snap.Epoch,
		})
	}

	// Calculate chunk count.
	chunkCount := (fileSize-1)/snapshotChunkSize + 1

	// Open the snapshot data file for streaming reads. Only one chunk's
	// worth of data is held in memory at a time, avoiding OOM on large
	// snapshots (up to DefaultMaxSnapshotSize = 4GB).
	//
	// The defer for cleanup is registered immediately after a successful
	// open to ensure the file handle is always closed, even on panic.
	// The fileClosed flag allows explicit early close while preventing
	// double-close errors in the deferred cleanup.
	f, err := os.Open(filePath)
	if err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       err,
		}
	}
	fileClosed := false
	defer func() {
		if !fileClosed {
			if closeErr := f.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
				slog.Debug("snapshot file close failed",
					"shard", msg.ShardID,
					"replica", msg.To,
					"path", filePath,
					"error", closeErr)
			}
		}
	}()

	// Verify the actual file size matches the declared size.
	info, err := f.Stat()
	if err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       err,
		}
	}
	if uint64(info.Size()) != fileSize {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       ErrSnapshotSizeMismatch,
		}
	}

	for i := uint64(0); i < chunkCount; i++ {
		remaining := fileSize - i*snapshotChunkSize
		chunkLen := uint64(snapshotChunkSize)
		if remaining < chunkLen {
			chunkLen = remaining
		}
		chunkData := make([]byte, chunkLen)
		if _, err := io.ReadFull(f, chunkData); err != nil {
			return &SnapshotSendError{
				ShardID:   msg.ShardID,
				ReplicaID: msg.To,
				Op:        "read",
				Err:       err,
			}
		}

		if err := emit(proto.SnapshotChunk{
			ShardID:        msg.ShardID,
			ReplicaID:      msg.To,
			From:           msg.From,
			Index:          snap.Index,
			Term:           snap.Term,
			ChunkID:        i,
			ChunkCount:     chunkCount,
			ChunkSize:      chunkLen,
			FileSize:       fileSize,
			Filepath:       filePath,
			FileChunkID:    i,
			FileChunkCount: chunkCount,
			Membership:     snap.Membership,
			OnDiskIndex:    snap.OnDiskIndex,
			BinVer:         proto.WireVersion,
			Epoch:          snap.Epoch,
			Data:           chunkData,
		}); err != nil {
			return err
		}
	}

	// Close the file early to release the handle immediately after reading
	// completes. The deferred cleanup will be a no-op due to fileClosed flag.
	if err := f.Close(); err != nil {
		return &SnapshotSendError{
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			Op:        "read",
			Err:       err,
		}
	}
	fileClosed = true

	return nil
}

// Compile-time assertion that transportSender satisfies MessageSender.
var _ engine.MessageSender = (*transportSender)(nil)

// pendingRead tracks a single ReadIndex request through the pipeline.
// The readIndex field is set once the leader confirms via heartbeat quorum.
//
// When ReadIndex batch coalescing is active, the first key in a batch is
// the "batch key" and its pendingRead carries an aliases slice of the
// other keys in the batch. When the batch key's ReadIndex is confirmed,
// all alias keys are resolved with the same readIndex value.
type pendingRead struct {
	rs        *RequestState
	readIndex uint64   // set when OnReadyToRead confirms
	confirmed bool     // true once readIndex is known
	aliases   []uint64 // other readKeys that share this ReadIndex result
}

// nodeState holds per-shard state managed by the Host. This is the
// Host's view of a running shard, independent of the engine.Node
// type which manages the Raft protocol. The Host maintains this state
// to serve local reads and routing without engine round-trips.
type nodeState struct {
	shardID           uint64
	replicaID         uint64
	cfg               config.Config
	createFn          sm.CreateFunc
	smType            uint64
	leaderID          atomic.Uint64
	term              atomic.Uint64
	lastApplied       atomic.Uint64
	configChangeIndex atomic.Uint64
	isLeader          atomic.Bool
	health            atomic.Uint32 // stores ShardHealth as uint32
	nodes             map[uint64]string
	nodesMu           sync.RWMutex
	requestPool       *sync.Pool
	stopped           atomic.Bool
	rsm               *rsm.StateMachine // for local reads (Lookup)

	// openStopper is closed to cancel an in-progress DiskStateMachine.Open()
	// call during Host.Close() or StopShard(). For in-memory state machines
	// it is closed immediately after the no-op Open returns.
	openStopper chan struct{}

	// openStopOnce ensures openStopper is closed at most once.
	openStopOnce sync.Once

	// pendingProposals maps proposal key to the *RequestState waiting for
	// that entry to be applied. Entries are added by Propose and
	// removed by OnApplied or OnLeaderUpdated (on leadership loss).
	// Uses sync.Map for lock-free reads on the hot OnApplied path
	// and sharded writes under concurrent propose load.
	pendingProposals sync.Map

	// pendingReadsMu protects pendingReads only.
	pendingReadsMu sync.Mutex

	// pendingReads maps read request key to the pendingRead tracking
	// the ReadIndex confirmation and SM catch-up. Entries are added by
	// ReadIndex and removed when lastApplied >= readIndex or on
	// leadership loss / shard stop.
	pendingReads map[uint64]*pendingRead

	// pendingSnapshotsMu protects pendingSnapshots only.
	pendingSnapshotsMu sync.Mutex

	// pendingSnapshots maps snapshot key to the RequestState waiting for
	// the snapshot to complete. Entries are added by RequestSnapshot and
	// removed by OnSnapshotCompleted.
	pendingSnapshots map[uint64]*RequestState
}

// pendingProposal bundles a RequestState with its deadline, captured
// at Store time. The sweep reads only pp.deadline (immutable after Store),
// never touching the RS fields that may be recycled by sync.Pool.
type pendingProposal struct {
	rs       *RequestState
	deadline time.Time
}

// hostEngineCallback implements engine.Callback by routing
// applied entries and leader changes back to the Host for proposal
// completion and leader state updates.
type hostEngineCallback struct {
	h *Host
}

// OnApplied completes pending proposals whose entries have been
// applied to the state machine. entries and results are parallel
// slices. For each entry with a registered RequestState, the result
// is delivered to the caller's channel.
//
//nolint:gocyclo // multi-branch dispatch for applied entry types is inherently complex
func (c *hostEngineCallback) OnApplied(shardID uint64, entries []proto.Entry, results []sm.Result) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnApplied type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnApplied nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Detect membership changes outside the lock.
	hasMembershipChange := false
	for _, entry := range entries {
		if entry.Type == proto.EntryConfigChange {
			hasMembershipChange = true
			break
		}
	}

	// Complete pending proposals. Uses sync.Map for lock-free lookups
	// on this hot path — no mutex contention with concurrent Propose.
	for i, entry := range entries {
		// Match by entry.Key (the proposal tracking key set by Host.Propose)
		// rather than entry.Index (assigned by the raft state machine).
		v, ok := ns.pendingProposals.LoadAndDelete(entry.Key)
		if ok {
			pp, ppOK := v.(*pendingProposal)
			if !ppOK {
				slog.Error("OnApplied pendingProposal type assertion failed",
					"shard_id", shardID,
					"entry_key", entry.Key,
					"error", &TypeAssertionError{
						Context:      "OnApplied pendingProposal lookup",
						ExpectedType: "*pendingProposal",
						ActualValue:  v,
					})
				continue
			}
			rs := pp.rs
			// Record end-to-end proposal latency (Propose -> OnApplied).
			if c.h.metrics != nil && !rs.createdAt.IsZero() {
				c.h.metrics.ObserveProposalLatency(shardID, time.Since(rs.createdAt))
			}
			// Translate reserved ResultSessionExpired into an error
			// so clients can distinguish expired sessions from
			// legitimate zero-value results (PhD Figure 6.1, step 3).
			if results[i].Value == sm.ResultSessionExpired {
				rs.complete(RequestResult{Err: &SessionExpiredError{
					ShardID:  shardID,
					ClientID: entry.ClientID,
				}})
			} else {
				rs.complete(RequestResult{Value: results[i].Value, Data: results[i].Data})
			}
			c.h.completePendingOp()
		}
	}

	// Complete confirmed pending reads whose readIndex has been reached.
	if len(entries) > 0 {
		newLastApplied := entries[len(entries)-1].Index
		ns.pendingReadsMu.Lock()
		for key, pr := range ns.pendingReads {
			if pr.confirmed && newLastApplied >= pr.readIndex {
				if c.h.metrics != nil && !pr.rs.createdAt.IsZero() {
					c.h.metrics.ObserveReadIndexLatency(shardID, time.Since(pr.rs.createdAt))
				}
				pr.rs.complete(RequestResult{Value: pr.readIndex})
				delete(ns.pendingReads, key)
				c.h.completePendingOp()
			}
		}
		ns.pendingReadsMu.Unlock()
	}
	// Update lastApplied on the host node state.
	if len(entries) > 0 {
		ns.lastApplied.Store(entries[len(entries)-1].Index)
	}
	// Fire OnMembershipChanged when a config change entry was applied,
	// then update the Host routing table (ns.nodes + registry) so that
	// the transport can resolve the new/removed replica addresses.
	if hasMembershipChange {
		if el := c.h.cfg.EventListener; el != nil && el.OnMembershipChanged != nil {
			el.OnMembershipChanged(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
			})
		}
		for _, entry := range entries {
			if entry.Type != proto.EntryConfigChange {
				continue
			}
			var cc proto.ConfigChange
			if _, err := cc.UnmarshalFrom(entry.Cmd); err != nil {
				c.h.logger.Error("failed to unmarshal config change",
					"shard", shardID,
					"index", entry.Index,
					"error", err,
				)
				continue
			}
			switch cc.Type {
			case proto.AddNode, proto.AddNonVoting, proto.AddWitness:
				ns.nodesMu.Lock()
				ns.nodes[cc.ReplicaID] = cc.Address
				ns.nodesMu.Unlock()
				c.h.registry.Register(shardID, cc.ReplicaID, cc.Address)
			case proto.RemoveNode:
				ns.nodesMu.Lock()
				delete(ns.nodes, cc.ReplicaID)
				ns.nodesMu.Unlock()
				c.h.registry.Remove(shardID, cc.ReplicaID)
			}
			ns.configChangeIndex.Store(entry.Index)
			c.h.logger.Info("membership changed",
				"shard", shardID,
				"replica", ns.replicaID,
				"type", proto.ConfigChangeTypeName(cc.Type),
				"target", cc.ReplicaID,
				"address", cc.Address,
			)
		}
	}
}

// OnReadyToRead processes confirmed read index requests from the step
// worker. For each ReadyToRead, it records the confirmed readIndex on
// the matching pendingRead and completes the request if lastApplied
// has already caught up. When the pendingRead has aliases (from batch
// coalescing), all alias keys are resolved with the same readIndex.
func (c *hostEngineCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnReadyToRead: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnReadyToRead nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	lastApplied := ns.lastApplied.Load()
	ns.pendingReadsMu.Lock()
	for _, rtr := range readyToRead {
		pr, ok := ns.pendingReads[rtr.Key]
		if !ok {
			continue
		}
		pr.readIndex = rtr.Index
		pr.confirmed = true
		if lastApplied >= rtr.Index {
			if c.h.metrics != nil && !pr.rs.createdAt.IsZero() {
				c.h.metrics.ObserveReadIndexLatency(shardID, time.Since(pr.rs.createdAt))
			}
			pr.rs.complete(RequestResult{Value: rtr.Index})
			delete(ns.pendingReads, rtr.Key)
			c.h.completePendingOp()
			// Complete all alias keys that share this batch's ReadIndex.
			for _, aliasKey := range pr.aliases {
				if aliasPR, aliasOK := ns.pendingReads[aliasKey]; aliasOK {
					if c.h.metrics != nil && !aliasPR.rs.createdAt.IsZero() {
						c.h.metrics.ObserveReadIndexLatency(shardID, time.Since(aliasPR.rs.createdAt))
					}
					aliasPR.rs.complete(RequestResult{Value: rtr.Index})
					delete(ns.pendingReads, aliasKey)
					c.h.completePendingOp()
				}
			}
		} else {
			// SM has not caught up yet. Mark all alias keys as confirmed
			// so OnApplied resolves them when lastApplied advances.
			for _, aliasKey := range pr.aliases {
				if aliasPR, aliasOK := ns.pendingReads[aliasKey]; aliasOK {
					aliasPR.readIndex = rtr.Index
					aliasPR.confirmed = true
				}
			}
		}
	}
	ns.pendingReadsMu.Unlock()
}

// OnReadIndexBatched registers alias keys that should complete when the
// batchKey's ReadIndex is confirmed. This is called by the step worker
// BEFORE submitting the ReadIndex to raft, ensuring aliases are in place
// before the raft layer can produce a readState. The batchKey is the
// first key in the batch (used as the raft ReadIndex hint), and
// aliasKeys are the remaining keys that piggyback on the same heartbeat
// quorum round.
func (c *hostEngineCallback) OnReadIndexBatched(shardID uint64, batchKey uint64, aliasKeys []uint64) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnReadIndexBatched: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnReadIndexBatched nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	ns.pendingReadsMu.Lock()
	batchPR, ok := ns.pendingReads[batchKey]
	if !ok {
		ns.pendingReadsMu.Unlock()
		return
	}
	// Append alias keys. Pre-allocate if this is the first batch for
	// this key, otherwise append to existing aliases (multiple
	// processReady cycles could theoretically add aliases, though in
	// practice a ReadIndex resolves within one cycle).
	if batchPR.aliases == nil {
		batchPR.aliases = make([]uint64, 0, len(aliasKeys))
	}
	batchPR.aliases = append(batchPR.aliases, aliasKeys...)
	ns.pendingReadsMu.Unlock()
}

// OnLeaderUpdated updates the Host's cached leader information for the
// shard. If the local replica lost leadership, all pending proposals
// are completed with a NotLeaderError so callers do not block forever.
func (c *hostEngineCallback) OnLeaderUpdated(shardID uint64, leaderID uint64, term uint64) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		c.h.updateNodeLeader(shardID, leaderID, term)
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnLeaderUpdated: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnLeaderUpdated nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Capture previous leadership state and leader identity before updating.
	wasLeader := ns.isLeader.Load()
	prevLeaderID := ns.leaderID.Load()
	c.h.updateNodeLeader(shardID, leaderID, term)

	// Log leadership transitions.
	if leaderID == ns.replicaID && !wasLeader {
		c.h.logger.Info("became leader",
			"shard", shardID,
			"replica", ns.replicaID,
			"term", term,
		)
	}

	// Fire OnLeaderUpdated event listener.
	if el := c.h.cfg.EventListener; el != nil && el.OnLeaderUpdated != nil {
		el.OnLeaderUpdated(config.LeaderInfo{
			ShardID:   shardID,
			ReplicaID: ns.replicaID,
			Term:      term,
			LeaderID:  leaderID,
		})
	}

	// Fail pending proposals only if we actually lost leadership
	// (were the leader before and no longer are).
	if wasLeader && leaderID != ns.replicaID {
		if el := c.h.cfg.EventListener; el != nil && el.OnLeaderLost != nil {
			el.OnLeaderLost(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
				LeaderID:  leaderID,
				Term:      term,
			})
		}
		c.h.logger.Info("leader lost",
			"shard", shardID,
			"replica", ns.replicaID,
			"new_leader", leaderID,
			"term", term,
		)
		notLeaderErr := &NotLeaderError{ShardID: shardID, LeaderID: leaderID}
		c.h.failPendingRequests(ns, notLeaderErr)
	}

	// Fail forwarded proposals on followers when the leader changes.
	// When a follower has proposals forwarded to leader L1, and the
	// leader changes to L2, those forwarded proposals will never be
	// completed by L1. Detect this by comparing the previous leader ID
	// with the new one. Only fail when:
	// 1. We were NOT the leader (follower path; leader loss is handled above)
	// 2. There was a known previous leader (prevLeaderID != 0)
	// 3. The leader actually changed (not the same leader re-elected)
	if !wasLeader && prevLeaderID != 0 && prevLeaderID != leaderID {
		c.h.logger.Debug("follower detected leader change, failing forwarded proposals",
			"shard", shardID,
			"replica", ns.replicaID,
			"prev_leader", prevLeaderID,
			"new_leader", leaderID,
			"term", term,
		)
		notLeaderErr := &NotLeaderError{ShardID: shardID, LeaderID: leaderID}
		c.h.failPendingRequests(ns, notLeaderErr)
	}
}

// OnSnapshotCompleted processes snapshot completion callbacks from the
// engine snapshot pool. It completes any pending snapshot RequestState
// and fires the OnSnapshotCreated event listener if configured.
func (c *hostEngineCallback) OnSnapshotCompleted(shardID uint64, index uint64, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnSnapshotCompleted: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnSnapshotCompleted nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Complete pending snapshot RequestStates.
	ns.pendingSnapshotsMu.Lock()
	for key, rs := range ns.pendingSnapshots {
		if err != nil {
			rs.complete(RequestResult{Err: err})
		} else {
			rs.complete(RequestResult{Value: index})
		}
		delete(ns.pendingSnapshots, key)
		c.h.completePendingOp()
	}
	ns.pendingSnapshotsMu.Unlock()

	// Reconcile nodeState.nodes from the snapshot membership on
	// successful snapshot recovery. The Raft Peer already applied
	// the snapshot membership to its internal state; we mirror it
	// here so the Host routing table (ns.nodes + registry) stays
	// in sync. This handles the case where config changes were
	// compacted into the snapshot and never individually observed
	// by OnApplied.
	if err == nil {
		c.reconcileMembershipFromPeer(shardID, ns)
	}

	// Fire event listener callbacks.
	if el := c.h.cfg.EventListener; el != nil {
		if err == nil && index > 0 {
			// Successful snapshot save.
			if el.OnSnapshotCreated != nil {
				el.OnSnapshotCreated(config.SnapshotInfo{
					ShardID:   shardID,
					ReplicaID: ns.replicaID,
					Index:     index,
				})
			}
			if el.OnSnapshotCompacted != nil {
				el.OnSnapshotCompacted(config.SnapshotInfo{
					ShardID:   shardID,
					ReplicaID: ns.replicaID,
					Index:     index,
				})
			}
			if el.OnLogCompacted != nil {
				el.OnLogCompacted(config.EventShardInfo{
					ShardID:   shardID,
					ReplicaID: ns.replicaID,
				})
			}
			if el.OnLogDBCompacted != nil {
				el.OnLogDBCompacted(config.EventShardInfo{
					ShardID:   shardID,
					ReplicaID: ns.replicaID,
				})
			}
		}
		if err == nil && index == 0 {
			// Successful snapshot recovery (index is 0 for recovers).
			if el.OnSnapshotRecovered != nil {
				el.OnSnapshotRecovered(config.SnapshotInfo{
					ShardID:   shardID,
					ReplicaID: ns.replicaID,
				})
			}
		}
		if err != nil && el.OnShardFailed != nil {
			el.OnShardFailed(config.ShardFailedInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
				Err:       err,
			})
		}
	}
}

// reconcileMembershipFromPeer reads the committed membership from the
// Raft Peer and synchronizes the Host's nodeState.nodes map and registry
// with it. This is called after a snapshot is successfully applied to
// ensure the Host routing table reflects any membership changes that
// were compacted into the snapshot.
func (c *hostEngineCallback) reconcileMembershipFromPeer(shardID uint64, ns *nodeState) {
	engNode := c.h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return
	}

	pm := engNode.Peer().GetMembership()

	// Build the full member set from all membership categories.
	newNodes := make(map[uint64]string,
		len(pm.Addresses)+len(pm.Observers)+len(pm.Witnesses))
	for k, v := range pm.Addresses {
		newNodes[k] = v
	}
	for k, v := range pm.Observers {
		newNodes[k] = v
	}
	for k, v := range pm.Witnesses {
		newNodes[k] = v
	}

	ns.nodesMu.Lock()
	// Remove nodes that are no longer in the membership.
	for replicaID := range ns.nodes {
		if _, exists := newNodes[replicaID]; !exists {
			delete(ns.nodes, replicaID)
			c.h.registry.Remove(shardID, replicaID)
		}
	}
	// Add or update nodes from the snapshot membership.
	for replicaID, addr := range newNodes {
		ns.nodes[replicaID] = addr
		c.h.registry.Register(shardID, replicaID, addr)
	}
	ns.nodesMu.Unlock()
}

// OnCampaignLaunched is called by the step worker when the raft state
// machine starts an election. This is a cold path.
func (c *hostEngineCallback) OnCampaignLaunched(shardID, replicaID, term uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnCampaignLaunched != nil {
		el.OnCampaignLaunched(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
		})
	}
	c.h.logger.Info("campaign launched",
		"shard", shardID,
		"replica", replicaID,
		"term", term,
	)
}

// OnCampaignSkipped is called by the step worker when an election is
// skipped because uncommitted config change entries exist between
// committed and lastIndex. This is a cold path.
func (c *hostEngineCallback) OnCampaignSkipped(shardID, replicaID, term uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnCampaignSkipped != nil {
		el.OnCampaignSkipped(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Term:      term,
		})
	}
	c.h.logger.Info("campaign skipped: pending config change",
		"shard", shardID,
		"replica", replicaID,
		"term", term,
	)
}

// OnSnapshotRejected is called when a snapshot request is rejected by
// the snapshot pool (duplicate in-flight or pool full).
func (c *hostEngineCallback) OnSnapshotRejected(shardID, replicaID uint64) {
	if el := c.h.cfg.EventListener; el != nil && el.OnSnapshotRejected != nil {
		el.OnSnapshotRejected(config.SnapshotInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
		})
	}
	c.h.logger.Info("snapshot rejected",
		"shard", shardID,
		"replica", replicaID,
	)
}

// OnCommitted delivers early WAL-durable notifications to pending
// proposals. Called by the commit worker after entries are persisted
// to LogDB but before state machine apply. Only entries with a
// registered RequestState (Key > 0) are notified.
func (c *hostEngineCallback) OnCommitted(shardID uint64, entries []proto.Entry) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnCommitted: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnCommitted nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}
	for _, entry := range entries {
		if entry.Key == 0 {
			continue
		}
		v, ok := ns.pendingProposals.Load(entry.Key)
		if ok {
			pp, ppOK := v.(*pendingProposal)
			if !ppOK {
				slog.Error("OnCommitted: pendingProposal type assertion failed",
					"shard_id", shardID,
					"entry_key", entry.Key,
					"error", &TypeAssertionError{
						Context:      "OnCommitted pendingProposal lookup",
						ExpectedType: "*pendingProposal",
						ActualValue:  v,
					})
				continue
			}
			pp.rs.commitNotify(RequestResult{Value: 0})
		}
	}
}

// OnProposalFailed completes pending proposals that the Raft peer rejected
// (e.g., no known leader, transfer in progress). This ensures callers of
// Host.Propose/SyncPropose get a fast error instead of waiting for timeout.
func (c *hostEngineCallback) OnProposalFailed(shardID uint64, entries []proto.Entry, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnProposalFailed: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnProposalFailed nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Convert internal raft errors to public API errors.
	apiErr := c.h.translateProposalError(ns, err)

	for _, entry := range entries {
		v, ok := ns.pendingProposals.LoadAndDelete(entry.Key)
		if ok {
			pp, ppOK := v.(*pendingProposal)
			if !ppOK {
				slog.Error("OnProposalFailed: pendingProposal type assertion failed",
					"shard_id", shardID,
					"entry_key", entry.Key,
					"error", &TypeAssertionError{
						Context:      "OnProposalFailed pendingProposal lookup",
						ExpectedType: "*pendingProposal",
						ActualValue:  v,
					})
				continue
			}
			pp.rs.complete(RequestResult{Err: apiErr})
			c.h.completePendingOp()
		}
	}
}

// OnReadIndexFailed completes pending read requests whose ReadIndex
// submission to the Raft peer failed. The keys slice contains all keys
// in the batch (batch key + alias keys). This prevents orphaned pending
// reads that would otherwise block until context deadline timeout.
func (c *hostEngineCallback) OnReadIndexFailed(shardID uint64, keys []uint64, err error) {
	v, exists := c.h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("OnReadIndexFailed: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "OnReadIndexFailed nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	// Convert internal raft errors to public API errors.
	apiErr := c.h.translateProposalError(ns, err)

	ns.pendingReadsMu.Lock()
	for _, key := range keys {
		pr, ok := ns.pendingReads[key]
		if ok {
			pr.rs.complete(RequestResult{Err: apiErr})
			delete(ns.pendingReads, key)
			c.h.completePendingOp()
		}
	}
	ns.pendingReadsMu.Unlock()
}

// translateProposalError converts internal Raft errors to public API errors.
// The internal raft.NotLeaderError is translated to the public
// quicraft.NotLeaderError with cached leader info from the nodeState.
// Internal raft.ErrReadIndexNotReady is mapped to the public
// ErrReadIndexNotReady so callers can match with errors.Is.
func (h *Host) translateProposalError(ns *nodeState, err error) error {
	var nle *raft.NotLeaderError
	if errors.As(err, &nle) {
		return h.buildNotLeaderError(ns)
	}
	if errors.Is(err, raft.ErrReadIndexOverloaded) {
		return ErrSystemBusy
	}
	if errors.Is(err, raft.ErrReadIndexNotReady) {
		return ErrReadIndexNotReady
	}
	return err
}

// Compile-time assertion that hostEngineCallback implements Callback.
var _ engine.Callback = (*hostEngineCallback)(nil)

// Host is the main entry point for the QuicRaft multi-group Raft library.
// One Host per process. It manages shard lifecycle, proposal routing,
// read operations, session management, and cluster membership.
type Host struct {
	cfg            config.HostConfig
	logger         *slog.Logger
	logdb          logdb.LogDB
	barrier        Barrier
	metrics        MetricsCollector
	engine         *engine.Engine
	registry       *registry.Registry
	sender         engine.MessageSender
	transport      *transport.QUICTransport // nil when transport is disabled
	nodes          sync.Map                 // map[uint64]*nodeState — lock-free reads for StaleRead/QueryLocalNode
	activeNodes    atomic.Int64             // tracks the number of loaded shards (replaces len(h.nodes))
	closed         atomic.Bool
	draining       atomic.Bool
	diskFull       *atomic.Bool // set by disk full callback in buildWALOptions
	id             string
	stopOnce       sync.Once
	proposalKeySeq atomic.Uint64 // monotonic key generator for proposal tracking
	readKeySeq     atomic.Uint64 // monotonic key generator for read request tracking
	snapshotKeySeq atomic.Uint64 // monotonic key generator for snapshot request tracking
	importSeq      atomic.Uint64 // monotonic counter for ImportSnapshot index uniqueness
	pendingOps     atomic.Int64  // tracks outstanding pending operations for drain
	keyRotStopC    chan struct{} // closed to stop the key rotation goroutine
	keyRotDone     chan struct{} // closed when the key rotation goroutine exits
	sweepStopC     chan struct{} // closed to stop the pending proposal sweeper goroutine
	sweepDone      chan struct{} // closed when the sweeper goroutine exits
	drainC         chan struct{} // closed when pendingOps reaches 0 during drain
	drainCMu       sync.Mutex    // protects drainC creation
}

// NewHost creates a new Host with the given configuration and options.
// It validates the configuration, generates or loads a host ID, and
// initializes all subsystems.
//
//nolint:gocyclo // subsystem initialization requires many sequential validation and setup steps
func NewHost(cfg config.HostConfig, opts ...HostOption) (*Host, error) {
	options := &hostOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Propagate transport disabled state to the config before validation
	// so that MTLSConfig is only required when transport is enabled.
	if options.disableTransport {
		cfg.TransportConfig.TransportDisabled = true
	}

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Build the Host logger. When the caller provides a logger via
	// WithLogger, use it directly. Otherwise, create one from the
	// HostConfig's LogConfig. The logger is stored on the Host struct
	// and used for all Host-level logging instead of mutating the
	// global slog default.
	var logger *slog.Logger
	if options.logger != nil {
		logger = options.logger
	} else {
		logger = config.NewLogger(cfg.LogConfig)
	}

	// Ensure directories exist.
	if err := os.MkdirAll(cfg.NodeHostDir, 0o750); err != nil {
		return nil, &HostInitError{Field: "NodeHostDir", Err: err}
	}
	if err := os.MkdirAll(cfg.WALDir, 0o750); err != nil {
		return nil, &HostInitError{Field: "WALDir", Err: err}
	}

	// Shared disk full flag set by the WAL's disk monitor callback and
	// read by Host.HealthCheck(). Allocated before the WAL is opened so
	// the callback closure captures the same atomic as the Host struct.
	var diskFullFlag atomic.Bool

	// Create the default WAL-based LogDB when no LogDB was provided.
	// This ensures all production hosts persist raft state to disk.
	if options.logdb == nil {
		walOpts := buildWALOptions(cfg, &diskFullFlag)
		// Wire barrier encryption into the WAL when a barrier is configured.
		if options.barrier != nil {
			walOpts = append(walOpts, waldb.WithBarrier(options.barrier))
		}
		// Wire LogDB metrics when a MetricsCollector is provided.
		if options.metrics != nil {
			walOpts = append(walOpts, waldb.WithMetrics(options.metrics))
		}
		db, err := waldb.Open(cfg.WALDir, walOpts...)
		if err != nil {
			return nil, &HostInitError{Field: "logdb", Err: err}
		}
		options.logdb = db
	}

	// Generate or load host ID.
	hostID, err := loadOrGenerateHostID(logger, cfg.NodeHostDir, cfg.HostID)
	if err != nil {
		return nil, err
	}

	// Create the registry for address resolution.
	reg := registry.NewRegistry()

	// Determine the message sender. When transport is disabled (unit
	// testing), use a no-op sender that silently drops messages. When
	// enabled, the QUIC transport itself satisfies MessageSender.
	var sender engine.MessageSender
	var tr *transport.QUICTransport
	var msgHandler *hostMessageHandler

	if !options.disableTransport {
		trCfg := buildTransportConfig(cfg)
		// Wire transport metrics when a MetricsCollector is provided. The
		// transport's internal TransportMetrics interface matches the
		// public MetricsCollector's TransportMetrics methods.
		if options.metrics != nil {
			trCfg.Metrics = options.metrics
		}

		// Create the message handler with nil engine and host references.
		// Both are wired after creation below. The transport is started
		// last (after all wiring) so no messages arrive before the
		// handler's engine and host fields are fully initialized.
		msgHandler = &hostMessageHandler{}

		var trErr error
		tr, trErr = transport.NewQUICTransport(trCfg, msgHandler, reg)
		if trErr != nil {
			return nil, &HostInitError{Field: "transport", Err: trErr}
		}

		// Wrap the transport in a transportSender that handles both
		// regular messages and snapshot streaming. The engine reference
		// is set after engine creation below.
		sender = &transportSender{transport: tr, barrier: options.barrier, logger: logger}
	} else {
		sender = &noopSender{}
	}

	// Create the engine with the resolved sender.
	// Pass the RaftMetrics sub-interface to the engine. The engine's
	// internal RaftMetrics interface matches the public MetricsCollector's
	// RaftMetrics methods, so the concrete implementation satisfies both.
	var raftMetrics engine.RaftMetrics
	if options.metrics != nil {
		raftMetrics = options.metrics
	}
	eng := engine.NewEngine(cfg, options.logdb, sender, options.barrier, nil, raftMetrics)

	// Wire the engine into the message handler so inbound messages
	// from the transport are delivered to the correct shard nodes.
	if msgHandler != nil {
		msgHandler.engine = eng
	}

	// Wire the engine into the transportSender so snapshot status
	// feedback can be delivered to the raft layer after transfers.
	if ts, ok := sender.(*transportSender); ok {
		ts.engine = eng
	}

	// Wire the barrier as the epoch tracker for in-flight snapshot
	// protection. This prevents PurgeEpochsBefore from removing DEKs
	// while snapshot transfers are in progress.
	if tr != nil && options.barrier != nil {
		tr.SetEpochTracker(options.barrier)
	}

	h := &Host{
		cfg:         cfg,
		logger:      logger,
		logdb:       options.logdb,
		barrier:     options.barrier,
		metrics:     options.metrics,
		engine:      eng,
		registry:    reg,
		sender:      sender,
		transport:   tr,
		diskFull:    &diskFullFlag,
		id:          hostID,
		keyRotStopC: make(chan struct{}),
		keyRotDone:  make(chan struct{}),
		sweepStopC:  make(chan struct{}),
		sweepDone:   make(chan struct{}),
	}

	// Wire the host into the message handler so HandleSnapshot can
	// access host configuration (snapshot dirs, logdb) for received
	// snapshot processing.
	if msgHandler != nil {
		msgHandler.host = h
	}

	// Wire the engine callback so applied entries and leader changes
	// flow back to the Host for proposal completion.
	cb := &hostEngineCallback{h: h}
	eng.SetCallback(cb)

	// Start the engine worker pipeline.
	eng.Start()

	// Start the QUIC transport listener. This MUST happen after ALL
	// wiring is complete: msgHandler.host, msgHandler.engine, the engine
	// callback, and eng.Start(). Once the transport starts, its recv
	// goroutines can immediately call HandleMessage/HandleSnapshot which
	// read msgHandler.host (registry, cfg, logdb) and msgHandler.engine.
	// Starting the transport before these fields are set causes a data
	// race between the recv goroutine reading them and NewHost writing them.
	if tr != nil {
		if startErr := tr.Start(); startErr != nil {
			eng.Stop()
			if stopErr := tr.Stop(); stopErr != nil {
				slog.Debug("transport stop failed after start error", "error", stopErr)
			}
			return nil, &HostInitError{Field: "transport_start", Err: startErr}
		}
	}

	// Start the key rotation loop when a barrier and rotation interval
	// are both configured. The goroutine exits when keyRotStopC is closed.
	if options.barrier != nil && cfg.KeyRotationInterval > 0 {
		go h.keyRotationLoop(cfg.KeyRotationInterval)
	} else {
		close(h.keyRotDone)
	}

	// Start the pending proposal sweeper goroutine. It periodically
	// scans all node states and completes expired pending proposals,
	// reads, and snapshots with ErrTimeout. This prevents orphaned
	// entries from accumulating when entries are lost due to leader
	// crashes or network partitions.
	go h.pendingSweepLoop()

	transportMode := "disabled"
	if tr != nil {
		transportMode = "quic"
	}

	h.logger.Info("host created",
		"id", hostID,
		"raft_address", cfg.RaftAddress,
		"listen_address", cfg.ListenAddress,
		"transport", transportMode,
	)

	return h, nil
}

// StartShard starts a Raft shard on this host. members maps replicaID to
// address for initial cluster members. join indicates whether this node
// is joining an existing cluster. create is the state machine factory.
// cfg provides per-shard Raft configuration.
//
// ShardID and ReplicaID are taken from cfg. Both must be greater than zero.
//
//nolint:gocyclo // shard startup has many validation and configuration branches
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error {
	if h.closed.Load() {
		return ErrClosed
	}

	if cfg.ShardID == 0 {
		return ErrInvalidConfig
	}
	if cfg.ReplicaID == 0 {
		return ErrInvalidConfig
	}

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}

	if create == nil {
		return ErrInvalidConfig
	}

	// Validate the initial members map when not joining an existing
	// cluster. A non-joining shard requires at least one member, and
	// the local replica must be included.
	if !join {
		if len(members) == 0 {
			return &EmptyMembersError{ShardID: cfg.ShardID}
		}
		if _, selfPresent := members[cfg.ReplicaID]; !selfPresent {
			return &SelfNotInMembersError{
				ShardID:   cfg.ShardID,
				ReplicaID: cfg.ReplicaID,
			}
		}
	}
	// Validate each member entry regardless of join mode.
	for replicaID, addr := range members {
		if replicaID == 0 {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   addr,
				Reason:    "replicaID must be non-zero",
			}
		}
		if addr == "" {
			return &InvalidMemberError{
				ShardID:   cfg.ShardID,
				ReplicaID: replicaID,
				Address:   addr,
				Reason:    "address must be non-empty",
			}
		}
	}

	// Early duplicate check. The authoritative check uses LoadOrStore
	// below, but this catches the common case without doing expensive
	// peer/SM initialization.
	if _, loaded := h.nodes.Load(cfg.ShardID); loaded {
		return ErrShardAlreadyExists
	}

	// Copy the members map to avoid aliasing.
	nodesCopy := make(map[uint64]string, len(members))
	for k, v := range members {
		nodesCopy[k] = v
	}

	// Create a LogReader backed by the host's LogDB (or a nil-LogDB
	// in-memory reader for testing). The LogReader provides the raft
	// layer with access to persisted log entries and state.
	lr := logdb.NewLogReader(cfg.ShardID, cfg.ReplicaID, h.logdb)

	// Recover LogReader state from LogDB on restart. On first boot the
	// LogDB will return ErrNoSnapshot / ErrNoState / empty entry range
	// and we fall through to the bootstrap membership below.
	if err := h.recoverLogReader(lr, cfg.ShardID, cfg.ReplicaID); err != nil {
		return &HostInitError{Field: "log_recovery", Err: err}
	}

	// Initialize the LogReader's membership with the bootstrap members
	// so the Raft state machine knows about all cluster members for
	// quorum calculations. Without this, each node would think it's a
	// single-node cluster and immediately self-elect. On recovery the
	// membership was already restored from the persisted snapshot above,
	// but applying the bootstrap config on top is safe because the Raft
	// layer reconciles membership from committed config-change entries.
	if !join {
		lr.SetMembership(proto.Membership{
			Addresses: nodesCopy,
		})
	}

	// Create the Raft peer. NewPeer validates the config and
	// initializes the raft state machine as a follower.
	peer, err := raft.NewPeer(cfg, lr, h.engine.GlobalRateLimiter())
	if err != nil {
		return &HostInitError{Field: "peer", Err: err}
	}
	// A joining node starts with incomplete membership. Mark it so
	// per-shard message auth is bypassed until a snapshot delivers
	// the full cluster membership.
	if join {
		peer.MarkJoining()
	}

	// Create the managed state machine (RSM) from the factory.
	snapshotDir := filepath.Join(h.cfg.NodeHostDir, "snapshots",
		fmt.Sprintf("shard-%d", cfg.ShardID))
	stateMachine, err := rsm.NewStateMachine(
		cfg.ShardID, cfg.ReplicaID, create, snapshotDir,
		cfg.MaxSessionsPerClient, cfg.MaxTotalSessions,
		cfg.SessionExpiryEntries,
		cfg.ZeroCopyEntryCmd,
	)
	if err != nil {
		// Map the rsm-internal error to the public sentinel so callers
		// can check errors.Is(err, ErrInvalidSMType) without knowing
		// about the internal rsm package.
		if errors.Is(err, rsm.ErrInvalidSMType) {
			return ErrInvalidSMType
		}
		return &HostInitError{Field: "state_machine", Err: err}
	}

	// Create the openStopper channel that can cancel a long-running
	// DiskStateMachine.Open() during Host.Close() or StopShard().
	openStopper := make(chan struct{})

	// Open the state machine. For on-disk SMs, this initializes persistent
	// storage and returns the last applied index for crash recovery.
	// For in-memory SMs, this is a no-op (returns 0).
	lastApplied, err := stateMachine.Open(openStopper)
	if err != nil {
		return &HostInitError{Field: "state_machine_open", Err: err}
	}

	// Recover in-memory SM state from persisted snapshot on restart.
	// On-disk SMs handle their own persistence through Open(), but
	// in-memory SMs start fresh and need the snapshot data restored.
	// The raft layer's processed index is initialized to the snapshot
	// index (in newRaftLog), so entries after the snapshot are
	// automatically re-applied by the engine.
	if h.logdb != nil {
		ss, ssErr := h.logdb.GetSnapshot(cfg.ShardID, cfg.ReplicaID)
		if ssErr == nil && ss.Index > 0 && ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")
			if f, openErr := os.Open(dataPath); openErr == nil {
				stopper := make(chan struct{})
				recoverErr := stateMachine.RecoverFromSnapshot(
					context.Background(), f, stopper,
				)
				if closeErr := f.Close(); closeErr != nil {
					return &HostInitError{Field: "snapshot_close", Err: closeErr}
				}
				if recoverErr != nil {
					return &HostInitError{Field: "snapshot_recovery", Err: recoverErr}
				}
			}
		}
	}

	// For on-disk SMs, Open() returns the last applied index. Set the
	// raft peer's processed marker to this value so the engine does not
	// wastefully re-apply entries that the SM has already persisted.
	if lastApplied > 0 {
		peer.SetProcessedIndex(lastApplied)
	}

	ns := &nodeState{
		shardID:          cfg.ShardID,
		replicaID:        cfg.ReplicaID,
		cfg:              cfg,
		createFn:         create,
		smType:           stateMachine.SMType().TypeCode(),
		nodes:            nodesCopy,
		requestPool:      NewRequestStatePool(),
		rsm:              stateMachine,
		openStopper:      openStopper,
		pendingReads:     make(map[uint64]*pendingRead),
		pendingSnapshots: make(map[uint64]*RequestState),
	}
	ns.health.Store(uint32(ShardHealthy))

	// Atomically store the nodeState, detecting concurrent StartShard
	// calls for the same shardID.
	if _, loaded := h.nodes.LoadOrStore(cfg.ShardID, ns); loaded {
		return ErrShardAlreadyExists
	}
	h.activeNodes.Add(1)

	// Save bootstrap info if we have a LogDB and this is not a join.
	if h.logdb != nil && !join {
		bootstrap := logdb.Bootstrap{
			Addresses: nodesCopy,
			Join:      join,
			Type:      ns.smType,
		}
		if err := h.logdb.SaveBootstrap(cfg.ShardID, cfg.ReplicaID, bootstrap); err != nil {
			h.nodes.Delete(cfg.ShardID)
			h.activeNodes.Add(-1)
			return ErrLogDBWrite
		}
	}

	// Create the engine Node and load it into the engine pipeline.
	engNode := engine.NewNode(
		peer,
		stateMachine, // rsm.StateMachine implements engine.Applier
		lr,
		cfg,
		h.cfg.MaxApplyRetries,
		stateMachine, // rsm.StateMachine implements engine.Snapshotter
		snapshotDir,
		h.cfg.EventListener,
	)
	engNode.SetNotifyCommit(h.cfg.NotifyCommit)
	h.engine.LoadNode(engNode)

	// Pre-bind metrics for this shard to avoid expensive label lookups
	// in hot paths. This must be called before any metrics are recorded.
	if h.metrics != nil {
		h.metrics.RegisterShard(cfg.ShardID)
	}

	// Register all members in the registry for address resolution.
	for replicaID, addr := range nodesCopy {
		h.registry.Register(cfg.ShardID, replicaID, addr)
	}

	// Notify the engine that this shard has work so the Raft peer
	// can begin election or heartbeat processing.
	h.engine.NotifyWork(cfg.ShardID)

	// Fire OnNodeReady event listener.
	if el := h.cfg.EventListener; el != nil && el.OnNodeReady != nil {
		el.OnNodeReady(config.EventShardInfo{
			ShardID:   cfg.ShardID,
			ReplicaID: cfg.ReplicaID,
		})
	}

	h.logger.Info("shard started",
		"shard_id", cfg.ShardID,
		"replica_id", cfg.ReplicaID,
		"sm_type", proto.StateMachineTypeName(ns.smType),
		"members", len(members),
		"join", join,
	)

	return nil
}

// StopShard stops all replicas of the given shard on this host.
func (h *Host) StopShard(shardID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	v, exists := h.nodes.Load(shardID)
	if !exists {
		return ErrShardNotFound
	}
	ns, ok := v.(*nodeState)
	if !ok {
		return &TypeAssertionError{
			Context:      "StopShard nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}

	ns.stopped.Store(true)
	ns.health.Store(uint32(ShardUnloaded))

	// Signal cancellation to any in-progress DiskStateMachine.Open()
	// or other operations blocking on the open stopper channel.
	ns.openStopOnce.Do(func() { close(ns.openStopper) })

	// Unload the engine node FIRST. This stops the node's participation
	// in the step/commit/apply pipeline and signals the apply worker to
	// close the per-shard channel. The nodeState must remain in h.nodes
	// during this phase so that any in-flight OnApplied callbacks can
	// still locate it and complete proposals normally rather than
	// discarding results.
	h.engine.UnloadNode(shardID)

	// Fail all remaining pending proposals, reads, and snapshots. Any
	// items completed by OnApplied during the drain are already removed
	// from these maps, so only truly orphaned items receive ErrAborted.
	h.failPendingRequests(ns, ErrAborted)

	// Remove nodeState last, after the engine is fully unloaded and all
	// pending items have been resolved. No more callbacks will reference
	// this shardID.
	h.nodes.Delete(shardID)
	h.activeNodes.Add(-1)

	// Remove all registered members for this shard from the registry.
	ns.nodesMu.RLock()
	for replicaID := range ns.nodes {
		h.registry.Remove(shardID, replicaID)
	}
	ns.nodesMu.RUnlock()

	// Unregister pre-bound metrics for this shard.
	if h.metrics != nil {
		h.metrics.UnregisterShard(shardID)
	}

	// Fire OnNodeUnloaded event listener.
	if el := h.cfg.EventListener; el != nil && el.OnNodeUnloaded != nil {
		el.OnNodeUnloaded(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: ns.replicaID,
		})
	}

	h.logger.Info("shard stopped", "shard_id", shardID)
	return nil
}

// SyncPropose proposes cmd through Raft consensus and blocks until the
// result is available or ctx is cancelled. Handles RequestState lifecycle
// internally.
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error) {
	rs, err := h.Propose(ctx, shardID, cmd)
	if err != nil {
		return sm.Result{}, err
	}
	defer rs.Release()

	// Use ctx.Done() directly instead of rs.Result() to avoid allocating
	// a time.NewTimer per SyncPropose call.
	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return sm.Result{}, result.Err
		}
		return sm.Result{Value: result.Value, Data: result.Data}, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return sm.Result{}, ErrTimeout
		}
		return sm.Result{}, ErrCanceled
	}
}

// SyncRead performs a linearizable read in a single call. It confirms
// leadership via ReadIndex, waits for the local SM to catch up, then
// executes the query. This provides the strongest consistency guarantee:
// the read is guaranteed to reflect all writes committed before it.
//
// The flow is:
//  1. Submit ReadIndex to confirm leadership via heartbeat quorum
//  2. Wait for the RequestState to resolve (lastApplied >= readIndex)
//  3. Execute SM.Lookup(query) on the now-caught-up state machine
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	// Step 1: Get a confirmed read index via ReadIndex.
	rs, err := h.ReadIndex(ctx, shardID)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Step 2: Wait for the SM to catch up to the read index.
	// Try a non-blocking receive first to catch immediately available
	// results (common for single-node ReadIndex via ReadOnly bypass).
	resultC := rs.ResultC()
	select {
	case result := <-resultC:
		if result.Err != nil {
			return nil, result.Err
		}
		goto lookup
	default:
	}
	// Result not immediately available. Block on the channel.
	select {
	case result := <-resultC:
		if result.Err != nil {
			return nil, result.Err
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrTimeout
		}
		return nil, ErrCanceled
	}
lookup:

	// Re-check context after the goto label. The non-blocking receive
	// path above jumps here without passing through the ctx.Done()
	// select case, so the context may have been canceled in between.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, mapContextError(ctxErr)
	}

	// Step 3: Execute the query on the caught-up state machine.
	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}
	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}
	return ns.rsm.Lookup(ctx, query)
}

// SyncReadBuf is the pooled-buffer variant of SyncRead for []byte key queries.
// It performs a linearizable read and returns the result in a buffer acquired
// from an internal pool. The caller MUST call release() when done with result,
// after which result MUST NOT be used.
//
// Use SyncReadBuf on hot paths where the result is immediately consumed (e.g.,
// serialized into a response) to eliminate per-call heap allocation. For
// queries that are not []byte or where the underlying SM does not implement
// sm.NALookupInto, the method falls back to a standard allocation.
//
// Example:
//
//	result, release, err := host.SyncReadBuf(ctx, shardID, key)
//	if err != nil { return err }
//	defer release()
//	// use result before returning
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error) {
	rs, readErr := h.ReadIndex(ctx, shardID)
	if readErr != nil {
		return nil, nopHostRelease, readErr
	}
	defer rs.Release()

	resultC := rs.ResultC()
	select {
	case r := <-resultC:
		if r.Err != nil {
			return nil, nopHostRelease, r.Err
		}
		goto lookup
	default:
	}
	select {
	case r := <-resultC:
		if r.Err != nil {
			return nil, nopHostRelease, r.Err
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, nopHostRelease, ErrTimeout
		}
		return nil, nopHostRelease, ErrCanceled
	}
lookup:
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, nopHostRelease, mapContextError(ctxErr)
	}
	ns, nsErr := h.getNode(shardID)
	if nsErr != nil {
		return nil, nopHostRelease, nsErr
	}
	if ns.rsm == nil {
		return nil, nopHostRelease, ErrShardNotReady
	}
	buf, rel, supported, lookupErr := ns.rsm.NALookupBuf(key)
	if lookupErr != nil {
		return nil, nopHostRelease, lookupErr
	}
	if supported {
		return buf, rel, nil
	}
	// Fall back to the generic Lookup path (result is []byte or interface{}).
	v, lookupErr2 := ns.rsm.Lookup(ctx, key)
	if lookupErr2 != nil {
		return nil, nopHostRelease, lookupErr2
	}
	if b, ok := v.([]byte); ok {
		return b, nopHostRelease, nil
	}
	// SM returned a non-[]byte result. The caller should use SyncRead instead.
	return nil, nopHostRelease, ErrInvalidOperation
}

// StaleReadBuf is the pooled-buffer variant of StaleRead for []byte key queries.
// It reads local SM state without any consensus check; the result may be stale.
// The caller MUST call release() when done with result, after which result MUST
// NOT be used.
//
// See SyncReadBuf for usage details and the pooling contract.
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error) {
	if h.closed.Load() {
		return nil, nopHostRelease, ErrClosed
	}
	ns, nsErr := h.getNode(shardID)
	if nsErr != nil {
		return nil, nopHostRelease, nsErr
	}
	if ns.rsm == nil {
		return nil, nopHostRelease, ErrShardNotReady
	}
	buf, rel, supported, lookupErr := ns.rsm.NALookupBuf(key)
	if lookupErr != nil {
		return nil, nopHostRelease, lookupErr
	}
	if supported {
		return buf, rel, nil
	}
	v, lookupErr2 := ns.rsm.Lookup(ctx, key)
	if lookupErr2 != nil {
		return nil, nopHostRelease, lookupErr2
	}
	if b, ok := v.([]byte); ok {
		return b, nopHostRelease, nil
	}
	// SM returned a non-[]byte result. The caller should use StaleRead instead.
	return nil, nopHostRelease, ErrInvalidOperation
}

// nopHostRelease is a no-op release function returned when no pool buffer was
// used on the SyncReadBuf / StaleReadBuf path.
func nopHostRelease() {}

// Propose submits an asynchronous proposal through Raft. Returns a
// RequestState that the caller must poll via Result and release via Release.
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot propose.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Reject proposals when the WAL disk is full to prevent unbounded
	// log growth and potential data loss.
	if h.diskFull != nil && h.diskFull.Load() {
		return nil, ErrDiskFull
	}

	// Validate entry size. A proposal must not exceed EITHER the
	// MaxProposalPayloadSize (user-facing limit) or MaxEntrySize
	// (transport/storage limit). Use the effective minimum of whichever
	// limits are configured.
	effectiveMax := effectiveMaxPayload(ns.cfg.MaxProposalPayloadSize, ns.cfg.MaxEntrySize)
	if uint64(len(cmd)) > effectiveMax {
		return nil, &EntryTooLargeError{
			Size:    uint64(len(cmd)),
			MaxSize: effectiveMax,
		}
	}

	// If there is no known leader, fail fast. When a leader is known,
	// allow the Raft engine to forward the proposal automatically.
	if !ns.isLeader.Load() && ns.leaderID.Load() == 0 {
		return nil, h.buildNotLeaderError(ns)
	}

	entry := proto.Entry{Cmd: cmd}
	return h.proposeEntry(ctx, ns, shardID, entry)
}

// ProposeWithSession submits a session-managed proposal with at-most-once
// delivery semantics. The session must have been obtained via GetNewSession
// and must be in the normal proposal state (after registration completes).
//
// The caller MUST call session.ProposalCompleted() after receiving a
// successful result via RequestState.Result() or RequestState.ResultC().
// Failing to do so will cause the session's seriesID to become stale,
// preventing further proposals.
//
// On error, the session state is unchanged and the proposal may be retried
// with the same session.
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}
	if session == nil {
		return nil, ErrInvalidSession
	}

	shardID := session.ShardID()

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot propose.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	if !session.ValidForProposal(shardID) {
		return nil, ErrInvalidSession
	}

	// Reject proposals when the WAL disk is full to prevent unbounded
	// log growth and potential data loss.
	if h.diskFull != nil && h.diskFull.Load() {
		return nil, ErrDiskFull
	}

	// Validate entry size. A proposal must not exceed EITHER the
	// MaxProposalPayloadSize (user-facing limit) or MaxEntrySize
	// (transport/storage limit). Use the effective minimum of whichever
	// limits are configured.
	effectiveMax := effectiveMaxPayload(ns.cfg.MaxProposalPayloadSize, ns.cfg.MaxEntrySize)
	if uint64(len(cmd)) > effectiveMax {
		return nil, &EntryTooLargeError{
			Size:    uint64(len(cmd)),
			MaxSize: effectiveMax,
		}
	}

	// If there is no known leader, fail fast. When a leader is known,
	// allow the Raft engine to forward the proposal automatically.
	if !ns.isLeader.Load() && ns.leaderID.Load() == 0 {
		return nil, h.buildNotLeaderError(ns)
	}

	entry := proto.Entry{
		Cmd:         cmd,
		ClientID:    session.ClientID(),
		SeriesID:    session.SeriesID(),
		RespondedTo: session.RespondedTo(),
	}
	return h.proposeEntry(ctx, ns, shardID, entry)
}

// SyncProposeWithSession blocks until a session-managed proposal is applied.
// On success, session.ProposalCompleted() is called automatically before
// returning, advancing the session for the next proposal.
//
// On error, the session state is unchanged and the proposal may be retried
// with the same session.
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error) {
	rs, err := h.ProposeWithSession(ctx, session, cmd)
	if err != nil {
		return sm.Result{}, err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return sm.Result{}, result.Err
		}
		session.ProposalCompleted()
		return sm.Result{Value: result.Value, Data: result.Data}, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return sm.Result{}, ErrTimeout
		}
		return sm.Result{}, ErrCanceled
	}
}

// proposeEntry is the shared proposal pipeline used by Propose,
// ProposeWithSession, and proposeSession. It handles rate limiting,
// engine node validation, key generation, pending map registration,
// delivery, and queue-full cleanup.
func (h *Host) proposeEntry(ctx context.Context, ns *nodeState, shardID uint64, entry proto.Entry) (*RequestState, error) {
	// Check global rate limiter.
	if globalRL := h.engine.GlobalRateLimiter(); globalRL != nil && globalRL.RateLimited() {
		return nil, ErrSystemBusy
	}

	// Verify the engine node exists before creating state.
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Check per-shard rate limiter.
	if peer := engNode.Peer(); peer != nil {
		if shardRL := peer.RateLimiter(); shardRL != nil && shardRL.IsLimited() {
			return nil, &BusyError{
				ShardID:     shardID,
				CurrentSize: shardRL.Get(),
				MaxSize:     shardRL.MaxSize(),
			}
		}
	}

	// Generate a unique proposal key for tracking this request through
	// the commit/apply pipeline. The key is set on the entry before
	// delivery and preserved through the raft log, so the OnApplied
	// callback can match applied entries back to pending RequestStates.
	proposalKey := h.proposalKeySeq.Add(1)
	entry.Key = proposalKey

	// Create request state with deadline from context.
	deadline, _ := ctx.Deadline()
	rs := newRequestState(ns.requestPool, proposalKey, deadline)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the proposal in the pending map BEFORE delivering the
	// message to the inbox. This ensures that if the step worker
	// processes and applies the entry extremely fast, the OnApplied
	// callback will find the RS.
	ns.pendingProposals.Store(proposalKey, &pendingProposal{rs: rs, deadline: deadline})

	// Deliver the proposal to the MPSC proposal queue. The step worker
	// is the only goroutine that touches the Raft state machine, so
	// proposals must be routed through the queue to respect the
	// single-owner invariant on the Peer. Using the dedicated proposal
	// queue avoids channel serialization under high concurrent load.
	if !engNode.DeliverProposal([]proto.Entry{entry}) {
		// Queue full — remove the RS and let the caller retry.
		ns.pendingProposals.Delete(proposalKey)
		rs.Release()
		h.completePendingOp()
		if h.metrics != nil {
			h.metrics.IncProposalDropped(shardID)
		}
		if el := h.cfg.EventListener; el != nil && el.OnProposalDropped != nil {
			el.OnProposalDropped(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
			})
		}
		return nil, ErrSystemBusy
	}

	// Wake the step worker so it processes the proposal queue.
	h.engine.NotifyWork(shardID)

	return rs, nil
}

// ReadIndex submits a ReadIndex request to confirm linearizability.
// The returned RequestState resolves when the local SM has applied
// up to the confirmed commit index. The result's Value field carries
// the confirmed read index.
//
// Witnesses cannot serve reads (they don't store log data).
// Observers and followers forward the ReadIndex to the leader.
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot serve reads.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Verify the engine node and Peer exist.
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Generate a unique key for correlating this read request through
	// the raft pipeline. The key is carried in the ReadIndex message's
	// Hint field and returned in ReadyToRead.Key. Uses a dedicated
	// counter separate from proposals and snapshots to avoid contention.
	readKey := h.readKeySeq.Add(1)

	deadline, _ := ctx.Deadline()
	rs := newRequestState(ns.requestPool, readKey, deadline)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the pending read BEFORE delivering the message to avoid
	// a race where OnReadyToRead fires before the map entry exists.
	ns.pendingReadsMu.Lock()
	if ns.pendingReads == nil {
		ns.pendingReads = make(map[uint64]*pendingRead)
	}
	ns.pendingReads[readKey] = &pendingRead{rs: rs}
	ns.pendingReadsMu.Unlock()

	// Deliver the ReadIndex key to the MPSC ReadIndex queue. The step
	// worker drains all accumulated keys and submits a single batched
	// ReadIndex operation, coalescing N concurrent callers into 1
	// heartbeat quorum round. This is the primary performance
	// optimization for multi-node ReadIndex workloads.
	if !engNode.DeliverReadIndex(readKey) {
		ns.pendingReadsMu.Lock()
		delete(ns.pendingReads, readKey)
		ns.pendingReadsMu.Unlock()
		rs.Release()
		h.completePendingOp()
		if el := h.cfg.EventListener; el != nil && el.OnReadIndexDropped != nil {
			el.OnReadIndexDropped(config.EventShardInfo{
				ShardID:   shardID,
				ReplicaID: ns.replicaID,
			})
		}
		return nil, ErrSystemBusy
	}

	// Wake the step worker so it processes the ReadIndex queue.
	h.engine.NotifyWork(shardID)

	return rs, nil
}

// QueryLocalNode reads local SM state via SM.Lookup. NOT linearizable on
// its own. For linearizable reads, prefer SyncRead.
//
// QueryLocalNode and StaleRead are semantically equivalent: both perform
// an unguarded local SM lookup without any consensus check. They exist as
// separate methods for API compatibility with dragonboat's NodeHost
// interface, where QueryLocalNode was the original name and StaleRead
// was added later as a more descriptive alias.
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}

	return ns.rsm.Lookup(ctx, query)
}

// StaleRead reads local SM state directly without any consensus check.
// The result may be arbitrarily stale.
//
// StaleRead and QueryLocalNode are semantically equivalent: both perform
// an unguarded local SM lookup. They exist as separate methods for API
// compatibility. StaleRead is the preferred name as it clearly
// communicates the staleness guarantee (or lack thereof).
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}

	return ns.rsm.Lookup(ctx, query)
}

// GetNewSession creates a new client session for at-most-once proposal
// semantics. The session registration is proposed through Raft consensus
// so all replicas register the session. Blocks until the registration
// is committed and applied, or ctx is cancelled.
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Generate a unique client ID from cryptographic random bytes.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, &HostInitError{Field: "session_rand", Err: err}
	}
	clientID := uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 |
		uint64(buf[3])<<24 | uint64(buf[4])<<32 | uint64(buf[5])<<40 |
		uint64(buf[6])<<48 | uint64(buf[7])<<56

	session := newSession(shardID, clientID)
	session.PrepareForRegister()

	// Propose session registration through Raft.
	rs, err := h.proposeSession(ctx, ns, session)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Wait for the registration to be committed and applied.
	result, err := rs.Result()
	if err != nil {
		return nil, err
	}
	if result.Value != clientID {
		return nil, &SessionError{
			ClientID: clientID,
			Op:       "register",
			Err:      ErrInvalidOperation,
		}
	}

	// Registration succeeded — prepare session for normal proposals.
	session.PrepareForPropose()
	return session, nil
}

// RegisterSession registers the given session through Raft consensus,
// preserving its existing client ID. Unlike GetNewSession, which generates
// a new random client ID, RegisterSession uses the client ID already set
// on the provided session. This is used by adapters that create sessions
// with caller-specified client IDs (e.g., the dragonboat compatibility
// layer).
//
// The session must have a non-zero client ID and must belong to the
// specified shard. On success the session is prepared for normal proposals.
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	if session == nil {
		return nil, ErrInvalidSession
	}

	clientID := session.ClientID()
	if clientID == 0 {
		return nil, &SessionError{
			ClientID: clientID,
			Op:       "register",
			Err:      ErrInvalidSession,
		}
	}

	shardID := session.ShardID()
	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	session.PrepareForRegister()

	// Propose session registration through Raft.
	rs, err := h.proposeSession(ctx, ns, session)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Wait for the registration to be committed and applied.
	result, err := rs.Result()
	if err != nil {
		return nil, err
	}
	if result.Value != clientID {
		return nil, &SessionError{
			ClientID: clientID,
			Op:       "register",
			Err:      ErrInvalidOperation,
		}
	}

	// Registration succeeded — prepare session for normal proposals.
	session.PrepareForPropose()
	return session, nil
}

// CloseSession closes a client session by proposing unregistration through
// Raft consensus. Returns a RequestState that resolves when the
// unregistration is committed and applied.
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	if session == nil {
		return nil, ErrInvalidOperation
	}

	if !session.ValidForSessionOp(shardID) {
		return nil, ErrInvalidSession
	}

	session.PrepareForUnregister()

	return h.proposeSession(ctx, ns, session)
}

// proposeSession proposes a session register or unregister operation through
// Raft consensus. The session's SeriesID must already be set to the
// appropriate sentinel value via PrepareForRegister/PrepareForUnregister.
//
// The pattern mirrors Propose(): generate a key, register in pendingProposals,
// create a proto.Entry with the session fields, deliver via engine inbox,
// and return a RequestState the caller can wait on.
func (h *Host) proposeSession(ctx context.Context, ns *nodeState, session *Session) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if h.draining.Load() {
		return nil, ErrDraining
	}

	// If there is no known leader, fail fast. When a leader is known,
	// allow the Raft engine to forward the proposal automatically.
	if !ns.isLeader.Load() && ns.leaderID.Load() == 0 {
		return nil, h.buildNotLeaderError(ns)
	}

	// Build a session entry: empty Cmd, session fields populated.
	entry := proto.Entry{
		ClientID:    session.ClientID(),
		SeriesID:    session.SeriesID(),
		RespondedTo: session.RespondedTo(),
	}
	return h.proposeEntry(ctx, ns, session.ShardID(), entry)
}

// GetNoOPSession returns a no-op session for proposals that do not require
// at-most-once semantics. The no-op session has a fixed clientID of 0.
// Use for idempotent operations only.
func (h *Host) GetNoOPSession(shardID uint64) *Session {
	return NewNoOPSession(shardID)
}

// SyncGetShardMembership returns the committed membership of a shard after
// confirming leadership via ReadIndex. This provides a linearizable view of
// the membership configuration: all config changes committed before this call
// are guaranteed to be reflected in the result.
//
// The returned Membership includes the ConfigChangeID that produced it,
// which callers should pass to RequestAddNode / RequestRemoveNode to detect
// stale membership and serialize concurrent config changes.
//
// The context must carry a deadline or timeout to bound the read.
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error) {
	// Step 1: Linearizable fence via ReadIndex.
	rs, err := h.ReadIndex(ctx, shardID)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	// Step 2: Wait for the SM to catch up to the confirmed read index,
	// respecting context cancellation and deadlines.
	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return nil, result.Err
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrTimeout
		}
		return nil, ErrCanceled
	}

	// Step 3: Read membership from the Raft peer. GetMembership returns a
	// deep copy (all maps are copied), so this is safe outside the step
	// worker goroutine.
	return h.getMembershipFromPeer(shardID)
}

// GetShardMembership returns the current membership of a shard from local
// in-memory state. The result may be stale if a config change has been
// committed but not yet observed by this host. For a linearizable view,
// use SyncGetShardMembership.
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	return h.getMembershipFromPeer(shardID)
}

// getMembershipFromPeer reads the committed membership from the engine node's
// Raft peer and converts it to the public Membership type. The Peer's
// GetMembership method returns deep copies of all maps, so this is safe to
// call from any goroutine.
func (h *Host) getMembershipFromPeer(shardID uint64) (*Membership, error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	pm := engNode.Peer().GetMembership()
	return protoMembershipToPublic(&pm), nil
}

// protoMembershipToPublic converts a proto.Membership to the public
// Membership type. The proto type uses map[uint64]bool for Removed,
// while the public type uses map[uint64]struct{} for memory efficiency.
// All maps are copied to prevent aliasing.
func protoMembershipToPublic(pm *proto.Membership) *Membership {
	m := &Membership{
		ConfigChangeID: pm.ConfigChangeID,
		Nodes:          make(map[uint64]string, len(pm.Addresses)),
		Observers:      make(map[uint64]string, len(pm.Observers)),
		Witnesses:      make(map[uint64]string, len(pm.Witnesses)),
		Removed:        make(map[uint64]struct{}, len(pm.Removed)),
	}
	for k, v := range pm.Addresses {
		m.Nodes[k] = v
	}
	for k, v := range pm.Observers {
		m.Observers[k] = v
	}
	for k, v := range pm.Witnesses {
		m.Witnesses[k] = v
	}
	for k := range pm.Removed {
		m.Removed[k] = struct{}{}
	}
	return m
}

// GetShardInfo returns a point-in-time snapshot of the shard's status.
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	return h.buildShardInfo(ns), nil
}

// GetLeaderID returns the current leader's replicaID for the given shard
// and whether a leader is known. The result is from local in-memory state
// and may be stale.
func (h *Host) GetLeaderID(shardID uint64) (uint64, bool, error) {
	if h.closed.Load() {
		return 0, false, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return 0, false, err
	}

	leaderID := ns.leaderID.Load()
	return leaderID, leaderID != 0, nil
}

// GetLeaderIDAndTerm returns the current leader's replicaID, the current term,
// whether a leader is known, and any error for the given shard. Both leaderID
// and term are read from the same nodeState in a single call, eliminating
// the TOCTOU window that exists when calling GetLeaderID and GetShardInfo
// separately. The result is from local in-memory state and may be stale.
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (uint64, uint64, bool, error) {
	if h.closed.Load() {
		return 0, 0, false, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return 0, 0, false, err
	}

	leaderID := ns.leaderID.Load()
	term := ns.term.Load()
	return leaderID, term, leaderID != 0, nil
}

// RequestLeaderTransfer requests leadership transfer to a specific replica.
// Pass targetReplicaID=0 to transfer to the most up-to-date follower.
// This is fire-and-forget: the method returns immediately, and the
// transfer completes asynchronously via the raft state machine.
// The context is checked for cancellation before delivery.
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error {
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}
	if h.closed.Load() {
		return ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return err
	}

	if !ns.isLeader.Load() {
		return h.buildNotLeaderError(ns)
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return ErrShardNotReady
	}

	// Check context again before delivery.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// Deliver the leader transfer via the engine inbox to respect the
	// single-owner invariant on the Peer.
	msg := proto.Message{
		Type: proto.LeaderTransfer,
		From: ns.replicaID,
		Hint: targetReplicaID,
	}
	if !engNode.Deliver(msg) {
		return ErrSystemBusy
	}
	h.engine.NotifyWork(shardID)

	h.logger.Info("leader transfer requested",
		"shard_id", shardID,
		"target_replica_id", targetReplicaID,
	)
	return nil
}

// RequestSnapshot requests a snapshot for the given shard with the
// specified options. The returned RequestState resolves when the snapshot
// completes. The result's Value field carries the snapshot index on
// success.
//
// A zero-value SnapshotOption{} uses shard-level defaults, equivalent
// to the previous no-option behavior. When SnapshotOption.ExportPath is
// set, the snapshot is additionally written to the specified directory.
// When SnapshotOption.OverrideCompactionOverhead is true, the provided
// CompactionOverhead replaces the shard-level value for this snapshot's
// log compaction.
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	// Validate per-request options before acquiring any resources.
	if err := opt.Validate(); err != nil {
		return nil, err
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotReady
	}

	// Atomically claim the snapshotting slot for this shard.
	if !engNode.TryStartSnapshot() {
		return nil, ErrSnapshotInProgress
	}

	// Generate a unique key for tracking this snapshot request.
	// Uses a dedicated counter separate from proposals and reads.
	snapshotKey := h.snapshotKeySeq.Add(1)

	deadline, _ := ctx.Deadline()
	rs := newRequestState(ns.requestPool, snapshotKey, deadline)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the pending snapshot before submitting to avoid a race
	// where OnSnapshotCompleted fires before the map entry exists.
	ns.pendingSnapshotsMu.Lock()
	ns.pendingSnapshots[snapshotKey] = rs
	ns.pendingSnapshotsMu.Unlock()

	// Build engine-level option from the public SnapshotOption.
	engineOpt := engine.SnapshotRequestOption{
		ExportPath:         opt.ExportPath,
		CompactionOverhead: opt.CompactionOverhead,
		OverrideCompaction: opt.OverrideCompactionOverhead,
		Exported:           opt.IsExport(),
	}

	// Submit the snapshot request to the engine's snapshot pool.
	if submitErr := h.engine.RequestSnapshot(shardID, ns.replicaID, engNode, engineOpt); submitErr != nil {
		// Snapshot pool rejected (duplicate or full) -- clean up.
		engNode.ClearSnapshotting()
		ns.pendingSnapshotsMu.Lock()
		delete(ns.pendingSnapshots, snapshotKey)
		ns.pendingSnapshotsMu.Unlock()
		rs.Release()
		h.completePendingOp()
		return nil, ErrSnapshotInProgress
	}

	return rs, nil
}

// ExportSnapshot streams the latest snapshot data for the given shard
// to the provided writer. The shard must have at least one snapshot
// available in LogDB.
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error {
	if h.closed.Load() {
		return ErrClosed
	}

	if h.logdb == nil {
		return ErrSnapshotNotFound
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return err
	}

	logdbSnap, err := h.logdb.GetSnapshot(shardID, ns.replicaID)
	if err != nil {
		return err
	}
	if logdbSnap.Index == 0 {
		return ErrSnapshotNotFound
	}

	snapDir := logdbSnap.Filepath
	if snapDir == "" {
		return ErrSnapshotNotFound
	}

	dataPath := filepath.Join(snapDir, "snapshot.dat")
	f, err := os.Open(dataPath)
	if err != nil {
		return ErrSnapshotNotFound
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot export file close failed",
				"shard", shardID,
				"path", dataPath,
				"error", closeErr)
		}
	}()

	if err = copyWithContext(ctx, w, f); err != nil {
		return err
	}
	return nil
}

// ImportSnapshot imports a snapshot from the reader for the given shard.
// The shard must be stopped (not loaded on this host) before calling
// this method. The snapshot is written to disk and recorded in LogDB
// so the next StartShard can recover from it. The context is checked
// for cancellation at key blocking points during the import.
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.closed.Load() {
		return ErrClosed
	}

	// Verify shard is NOT running.
	_, running := h.nodes.Load(shardID)
	if running {
		return ErrInvalidOperation
	}

	if h.logdb == nil {
		return ErrInvalidOperation
	}

	// Determine the replicaID from LogDB bootstrap info.
	nodes, err := h.logdb.ListNodeInfo()
	if err != nil {
		return err
	}
	var replicaID uint64
	for _, ni := range nodes {
		if ni.ShardID == shardID {
			replicaID = ni.ReplicaID
			break
		}
	}
	if replicaID == 0 {
		return ErrShardNotFound
	}

	// Use a monotonic atomic counter to guarantee uniqueness across
	// rapid sequential calls, immune to clock rollback.
	importIndex := h.importSeq.Add(1)

	// Check context before performing I/O.
	if err := ctx.Err(); err != nil {
		return err
	}

	snapDir := filepath.Join(h.cfg.NodeHostDir, "snapshots",
		fmt.Sprintf("shard-%d", shardID),
		fmt.Sprintf("replica-%d", replicaID),
		fmt.Sprintf("snapshot-%020d", importIndex))

	if mkdirErr := os.MkdirAll(snapDir, 0o750); mkdirErr != nil {
		return mkdirErr
	}

	// Write snapshot data file.
	dataPath := filepath.Join(snapDir, "snapshot.dat")
	f, createErr := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if createErr != nil {
		return createErr
	}
	if _, copyErr := io.Copy(f, r); copyErr != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot import file close failed after copy error",
				"shard", shardID,
				"path", dataPath,
				"error", closeErr)
		}
		return copyErr
	}
	if syncErr := f.Sync(); syncErr != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot import file close failed after sync error",
				"shard", shardID,
				"path", dataPath,
				"error", closeErr)
		}
		return syncErr
	}
	if closeErr := f.Close(); closeErr != nil {
		slog.Debug("snapshot import file close failed",
			"shard", shardID,
			"path", dataPath,
			"error", closeErr)
	}

	// Check context before committing to LogDB.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Update LogDB with the imported snapshot metadata. Term must be at
	// least 1 (the minimum valid term) so the snapshot is not treated as
	// uninitialized by the Raft engine.
	logdbSnap := logdb.Snapshot{
		Index:    importIndex,
		Term:     1,
		Filepath: snapDir,
	}
	return h.logdb.SaveSnapshot(shardID, replicaID, logdbSnap)
}

// GetNodeHostInfo returns a snapshot of the host's state for monitoring.
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	info := &NodeHostInfo{
		RaftAddress: h.cfg.RaftAddress,
	}

	info.ShardInfoList = make([]ShardInfo, 0, int(h.activeNodes.Load()))
	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			slog.Error("GetNodeHostInfo: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "GetNodeHostInfo nodes range",
					ExpectedType: "*nodeState",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		info.ShardInfoList = append(info.ShardInfoList, *h.buildShardInfo(ns))
		return true
	})

	if !opt.SkipLogInfo && h.logdb != nil {
		logInfo, err := h.logdb.ListNodeInfo()
		if err == nil {
			info.LogInfo = logInfo
		}
	}

	return info
}

// DiagPipelineState returns diagnostic pipeline state for the given shard.
// Used for debugging pipeline stalls.
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return false, 0, 0, 0, ErrShardNotReady
	}
	cp, pq, inbox, _ := engNode.DiagState() // readIndexQLen not exposed in DiagPipelineState
	v, exists := h.nodes.Load(shardID)
	pp := 0
	if exists {
		ns, ok := v.(*nodeState)
		if !ok {
			slog.Error("DiagPipelineState: nodeState type assertion failed",
				"shard_id", shardID,
				"error", &TypeAssertionError{
					Context:      "DiagPipelineState nodeState lookup",
					ExpectedType: "*nodeState",
					ActualValue:  v,
				})
		} else {
			ns.pendingProposals.Range(func(_, _ any) bool {
				pp++
				return true
			})
		}
	}
	return cp, pq, inbox, pp, nil
}

// DiagRaftState returns diagnostic raft state for the given shard.
// Used for debugging pipeline stalls.
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return 0, 0, 0, 0, 0, "", ErrShardNotReady
	}
	t, c, p, li, m, s := engNode.DiagRaftState()
	return t, c, p, li, m, s, nil
}

// DiagRemotes returns diagnostic remote state for debugging commit stalls.
func (h *Host) DiagRemotes(shardID uint64) (string, error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return "", ErrShardNotReady
	}
	return engNode.DiagRemotes(), nil
}

// Close shuts down the host, stopping all shards and releasing resources.
// Close is idempotent. Close calls Drain() first to allow in-flight work
// to complete gracefully before tearing down subsystems.
func (h *Host) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Fire OnNodeHostShuttingDown before any shutdown work begins.
	if el := h.cfg.EventListener; el != nil && el.OnNodeHostShuttingDown != nil {
		el.OnNodeHostShuttingDown()
	}

	var closeErr error
	h.stopOnce.Do(func() {
		// Drain in-flight work first. If drain times out, log a
		// warning but proceed with shutdown anyway.
		if drainErr := h.Drain(); drainErr != nil {
			h.logger.Warn("drain incomplete", "error", drainErr)
		}

		// Stop the key rotation goroutine and wait for it to exit
		// before stopping the engine to prevent rotation during shutdown.
		close(h.keyRotStopC)
		<-h.keyRotDone

		// Stop the pending proposal sweeper and wait for it to exit.
		close(h.sweepStopC)
		<-h.sweepDone

		// Wait for in-flight snapshot sends to complete before
		// stopping the transport to avoid orphaned goroutines.
		if ts, ok := h.sender.(*transportSender); ok {
			ts.Wait()
		}

		// Stop the transport before the engine so no new inbound
		// messages arrive while workers are draining.
		if h.transport != nil {
			if err := h.transport.Stop(); err != nil {
				slog.Debug("transport stop failed during host close", "error", err)
			}
		}

		// Stop the engine to ensure the worker pipeline drains
		// before we clean up shard state and close LogDB.
		if h.engine != nil {
			h.engine.Stop()
		}

		// Stop all nodes. Complete any remaining pending proposals,
		// reads, and snapshots with ErrClosed so callers are not left
		// blocking forever on channels that will never be written.
		//
		// Collect nodeState pointers so a second sweep can catch any
		// proposals registered between the first sweep visiting a shard
		// and Propose/ReadIndex finishing its registration (TOCTOU race
		// between h.closed.Load() in Propose and this sweep).
		var stoppedNodes []*nodeState
		h.nodes.Range(func(key, value any) bool {
			shardID, ok := key.(uint64)
			if !ok {
				slog.Error("Close: nodes map key type assertion failed",
					"error", &TypeAssertionError{
						Context:      "Close nodes range key",
						ExpectedType: "uint64",
						ActualValue:  key,
					})
				return true // skip malformed entry
			}
			ns, ok := value.(*nodeState)
			if !ok {
				slog.Error("Close: nodes map value type assertion failed",
					"shard_id", shardID,
					"error", &TypeAssertionError{
						Context:      "Close nodes range value",
						ExpectedType: "*nodeState",
						ActualValue:  value,
					})
				return true // skip malformed entry
			}
			ns.stopped.Store(true)
			ns.health.Store(uint32(ShardUnloaded))
			// Signal cancellation to any in-progress DiskStateMachine.Open().
			ns.openStopOnce.Do(func() { close(ns.openStopper) })
			h.failPendingRequests(ns, ErrClosed)
			stoppedNodes = append(stoppedNodes, ns)
			h.nodes.Delete(shardID)
			h.activeNodes.Add(-1)
			return true
		})

		// Second sweep: catch proposals or reads that were registered
		// after the first sweep visited their shard but before the
		// engine fully stopped. Goroutines that passed h.closed.Load()
		// and getNode() before the first sweep may have registered new
		// pending entries between the first sweep and now.
		// RequestState.complete is CAS-guarded, so re-completing
		// already-completed entries is a safe no-op.
		for _, ns := range stoppedNodes {
			h.failPendingRequests(ns, ErrClosed)
		}

		// Close LogDB.
		if h.logdb != nil {
			if err := h.logdb.Close(); err != nil {
				closeErr = err
			}
		}

		// Seal barrier if present.
		if h.barrier != nil {
			if err := h.barrier.Seal(); err != nil {
				// Already sealed is not an error during shutdown.
				if err != ErrBarrierSealed {
					if closeErr == nil {
						closeErr = err
					}
				}
			}
		}

		h.logger.Info("host closed", "id", h.id)
	})

	return closeErr
}

// Drain transitions the host into drain mode as part of graceful shutdown.
// Once called, new proposals and ReadIndex requests are permanently rejected
// with ErrDraining. In-flight proposals, reads, and snapshots are allowed to
// complete. Drain blocks until all in-flight work has completed or the
// configured ShutdownTimeout expires. If the timeout expires before all work
// completes, ErrShutdownTimeout is returned.
//
// Drain is irreversible: the draining state cannot be cleared without stopping
// and restarting the host. It is intended to be called immediately before Close
// as the first step of a graceful shutdown sequence. Callers that need to
// resume accepting proposals must create a new Host instance.
//
// Drain is idempotent: calling it multiple times is safe. The second call
// returns immediately since draining is already true and all pending work
// was either already drained or will be drained by the first call.
//
// The implementation is event-driven: instead of polling at fixed intervals,
// it uses an atomic counter (pendingOps) decremented by operation completion
// callbacks. When the counter reaches 0, a notification channel is closed.
func (h *Host) Drain() error {
	// Set draining flag. If already draining, return immediately since
	// the initial Drain() call is handling the drain wait.
	if !h.draining.CompareAndSwap(false, true) {
		return nil
	}

	// Fast path: no pending operations.
	if h.pendingOps.Load() <= 0 {
		return nil
	}

	// Create the drain notification channel. CompletePendingOp will
	// close it when pendingOps reaches 0.
	h.drainCMu.Lock()
	if h.drainC == nil {
		h.drainC = make(chan struct{})
	}
	drainCh := h.drainC
	h.drainCMu.Unlock()

	// Check again after channel creation to avoid a race.
	if h.pendingOps.Load() <= 0 {
		return nil
	}

	timer := time.NewTimer(h.cfg.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-drainCh:
		return nil
	case <-timer.C:
		return ErrShutdownTimeout
	}
}

// addPendingOp increments the outstanding pending operations counter.
// Called when a proposal, read, or snapshot operation is registered.
func (h *Host) addPendingOp() {
	h.pendingOps.Add(1)
}

// completePendingOp decrements the outstanding pending operations counter.
// When the counter reaches 0 and the host is draining, the drain
// notification channel is closed to unblock Drain().
//
// The CAS loop prevents the counter from going negative on double-completion.
// A RequestState can be completed twice when both the normal completion path
// (OnApplied) and the sweep/timeout path race. Without this guard, each
// extra completion would push the counter below zero, causing Drain() to
// close drainC prematurely on a future addPendingOp/completePendingOp cycle.
func (h *Host) completePendingOp() {
	for {
		current := h.pendingOps.Load()
		if current <= 0 {
			// Already at zero or below; do not decrement further.
			return
		}
		if h.pendingOps.CompareAndSwap(current, current-1) {
			if current-1 <= 0 && h.draining.Load() {
				h.drainCMu.Lock()
				if h.drainC != nil {
					select {
					case <-h.drainC:
						// Already closed.
					default:
						close(h.drainC)
					}
				}
				h.drainCMu.Unlock()
			}
			return
		}
		// Yield the processor on CAS failure to prevent CPU spinning
		// under extreme contention from concurrent completePendingOp
		// callers.
		runtime.Gosched()
	}
}

// failPendingRequests completes all pending proposals, reads, and snapshots
// on the given nodeState with the provided error. This is safe to call
// multiple times; RequestState.complete uses CAS so duplicate completions
// are no-ops.
func (h *Host) failPendingRequests(ns *nodeState, err error) {
	ns.pendingProposals.Range(func(key, value any) bool {
		pp, ok := value.(*pendingProposal)
		if !ok {
			slog.Error("failPendingRequests: pendingProposals value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "failPendingRequests pendingProposals range",
					ExpectedType: "*pendingProposal",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		pp.rs.complete(RequestResult{Err: err})
		ns.pendingProposals.Delete(key)
		h.completePendingOp()
		return true
	})

	ns.pendingReadsMu.Lock()
	for k, pr := range ns.pendingReads {
		pr.rs.complete(RequestResult{Err: err})
		delete(ns.pendingReads, k)
		h.completePendingOp()
	}
	ns.pendingReadsMu.Unlock()

	ns.pendingSnapshotsMu.Lock()
	for k, rs := range ns.pendingSnapshots {
		rs.complete(RequestResult{Err: err})
		delete(ns.pendingSnapshots, k)
		h.completePendingOp()
	}
	ns.pendingSnapshotsMu.Unlock()
}

// IsDraining reports whether the host is in drain mode.
func (h *Host) IsDraining() bool {
	return h.draining.Load()
}

// keyRotationLoop periodically rotates the barrier epoch. It runs as a
// background goroutine and exits when keyRotStopC is closed. After each
// rotation, it logs the new epoch. The loop is a simple ticker-based
// design; callers handle the complexity of coordinating WAL segment
// rotation externally.
//
// In addition to the scheduled interval, a faster threshold-check ticker
// detects when the AEAD invocation counter has reached its rotation threshold
// and triggers an immediate rotation to prevent exceeding NIST SP 800-38D limits.
func (h *Host) keyRotationLoop(interval time.Duration) {
	defer close(h.keyRotDone)
	for {
		panicked := h.keyRotationLoopOnce(interval)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-h.keyRotStopC:
			return
		default:
		}
		t := time.NewTimer(loopPanicRestartDelay)
		select {
		case <-h.keyRotStopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// keyRotationLoopOnce runs the key rotation loop body and recovers from
// panics. Returns true if the loop panicked, false if it returned
// normally (stop channel closed).
func (h *Host) keyRotationLoopOnce(interval time.Duration) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &LoopPanicError{
				LoopName: "key-rotation",
				Value:    r,
				Stack:    stack,
			}
			slog.Error("key rotation loop panic recovered, restarting",
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Threshold check runs at a faster cadence to detect invocation-based
	// rotation needs between scheduled rotations.
	thresholdCheck := time.NewTicker(30 * time.Second)
	defer thresholdCheck.Stop()

	for {
		select {
		case <-h.keyRotStopC:
			return false
		case <-ticker.C:
			h.rotateBarrierKey()
		case <-thresholdCheck.C:
			if h.barrier != nil && !h.barrier.IsSealed() && h.barrier.ShouldRotateKey() {
				h.logger.Info("invocation threshold reached, triggering key rotation")
				h.rotateBarrierKey()
			}
		}
	}
}

// rotateBarrierKey performs a barrier key rotation and optionally purges old
// DEKs when MaxRetainedEpochs is configured. Extracted from keyRotationLoop
// to avoid duplication between scheduled and threshold-triggered rotations.
func (h *Host) rotateBarrierKey() {
	if h.barrier == nil || h.barrier.IsSealed() {
		return
	}
	newEpoch, err := h.barrier.Rotate()
	if err != nil {
		h.logger.Error("key rotation failed", "error", err)
		return
	}
	h.logger.Info("key rotation completed", "epoch", newEpoch)

	if h.cfg.MaxRetainedEpochs > 0 && newEpoch > h.cfg.MaxRetainedEpochs {
		purgeThreshold := newEpoch - h.cfg.MaxRetainedEpochs
		purged, purgeErr := h.barrier.PurgeEpochsBefore(purgeThreshold)
		if purgeErr != nil {
			h.logger.Error("epoch purge after rotation failed",
				"purge_threshold", purgeThreshold,
				"error", purgeErr,
			)
		} else if purged > 0 {
			h.logger.Info("purged old encryption epochs",
				"purged", purged,
				"min_retained_epoch", purgeThreshold,
			)
		}
	}
}

// pendingSweepLoop periodically scans all node states and completes
// pending proposals, reads, and snapshots whose deadlines have expired.
// This prevents orphaned entries from accumulating indefinitely when
// entries are lost due to leader crashes or network partitions. Callers
// using context.Background() with async Propose would otherwise block
// forever on an entry that will never be applied.
//
// The sweeper runs every pendingSweepInterval and exits when sweepStopC
// is closed. It is started by NewHost and stopped by Close.
func (h *Host) pendingSweepLoop() {
	defer close(h.sweepDone)
	for {
		panicked := h.pendingSweepLoopOnce(pendingSweepInterval)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-h.sweepStopC:
			return
		default:
		}
		t := time.NewTimer(loopPanicRestartDelay)
		select {
		case <-h.sweepStopC:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// pendingSweepLoopOnce runs the pending sweep loop body and recovers
// from panics. Returns true if the loop panicked, false if it returned
// normally (stop channel closed). The interval parameter controls the
// sweep ticker period.
func (h *Host) pendingSweepLoopOnce(interval time.Duration) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &LoopPanicError{
				LoopName: "pending-sweep",
				Value:    r,
				Stack:    stack,
			}
			slog.Error("pending sweep loop panic recovered, restarting",
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.sweepStopC:
			return false
		case <-ticker.C:
			h.sweepExpiredPending()
		}
	}
}

// sweepExpiredPending iterates all loaded node states and completes
// pending proposals, reads, and snapshots whose RS deadline has passed.
// Each expired entry is completed with ErrTimeout and removed from its
// pending map. The method is safe to call concurrently with proposal
// registration and completion; it acquires the per-map mutex for each
// scan. RequestState.complete uses CAS, so racing with other completion
// paths is a harmless no-op.
func (h *Host) sweepExpiredPending() {
	now := time.Now()

	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			slog.Error("sweepExpiredPending: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "sweepExpiredPending nodes range",
					ExpectedType: "*nodeState",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		if ns.stopped.Load() {
			return true
		}
		h.sweepPendingProposals(ns, now)
		h.sweepPendingReads(ns, now)
		h.sweepPendingSnapshots(ns, now)
		return true
	})
}

// sweepPendingProposals expires proposals whose deadline has passed.
func (h *Host) sweepPendingProposals(ns *nodeState, now time.Time) {
	ns.pendingProposals.Range(func(key, value any) bool {
		pp, ok := value.(*pendingProposal)
		if !ok {
			slog.Error("sweepPendingProposals: pendingProposals value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "sweepPendingProposals pendingProposals range",
					ExpectedType: "*pendingProposal",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		if pp.deadline.IsZero() || now.Before(pp.deadline) {
			return true
		}
		pp.rs.complete(RequestResult{Err: ErrTimeout})
		ns.pendingProposals.Delete(key)
		h.completePendingOp()
		return true
	})
}

// sweepPendingReads expires read requests whose deadline has passed.
func (h *Host) sweepPendingReads(ns *nodeState, now time.Time) {
	ns.pendingReadsMu.Lock()
	for key, pr := range ns.pendingReads {
		deadline := pr.rs.Deadline()
		if deadline.IsZero() || now.Before(deadline) {
			continue
		}
		pr.rs.complete(RequestResult{Err: ErrTimeout})
		delete(ns.pendingReads, key)
		h.completePendingOp()
	}
	ns.pendingReadsMu.Unlock()
}

// sweepPendingSnapshots expires snapshot requests whose deadline has passed.
func (h *Host) sweepPendingSnapshots(ns *nodeState, now time.Time) {
	ns.pendingSnapshotsMu.Lock()
	for key, rs := range ns.pendingSnapshots {
		deadline := rs.Deadline()
		if deadline.IsZero() || now.Before(deadline) {
			continue
		}
		rs.complete(RequestResult{Err: ErrTimeout})
		delete(ns.pendingSnapshots, key)
		h.completePendingOp()
	}
	ns.pendingSnapshotsMu.Unlock()
}

// ID returns the unique host identifier.
func (h *Host) ID() string {
	return h.id
}

// RaftAddress returns this host's Raft network address.
func (h *Host) RaftAddress() string {
	return h.cfg.RaftAddress
}

// EmergencySeal immediately seals the barrier without waiting for in-flight
// operations to complete. All in-flight operations will fail with
// ErrBarrierSealed. This is for security incident response where speed
// matters more than graceful shutdown.
func (h *Host) EmergencySeal() error {
	if h.barrier == nil {
		return ErrBarrierNotInit
	}
	return h.barrier.Seal()
}

// IsSealed returns true if the barrier is sealed. Returns false if no
// barrier is configured.
func (h *Host) IsSealed() bool {
	if h.barrier == nil {
		return false
	}
	return h.barrier.IsSealed()
}

// ShardCount returns the number of shards loaded on this host.
func (h *Host) ShardCount() int {
	return int(h.activeNodes.Load())
}

// HealthCheck returns a point-in-time health assessment of the host.
// The returned HealthStatus includes barrier state, disk condition,
// shard count, and per-shard health information.
//
// Status values:
//   - "sealed"    if a barrier is configured and sealed (HTTP 503)
//   - "unhealthy" if no shards are loaded (HTTP 503)
//   - "degraded"  if any shard has ShardError health (HTTP 503)
//   - "ok"        if all shards are healthy (HTTP 200)
func (h *Host) HealthCheck() *HealthStatus {
	sealed := h.barrier != nil && h.barrier.IsSealed()
	diskOK := h.diskFull == nil || !h.diskFull.Load()

	shards := make([]ShardInfo, 0, int(h.activeNodes.Load()))
	h.nodes.Range(func(_, value any) bool {
		ns, ok := value.(*nodeState)
		if !ok {
			slog.Error("HealthStatus: nodes map value type assertion failed",
				"error", &TypeAssertionError{
					Context:      "HealthStatus nodes range",
					ExpectedType: "*nodeState",
					ActualValue:  value,
				})
			return true // skip malformed entry
		}
		shards = append(shards, *h.buildShardInfo(ns))
		return true
	})

	status := &HealthStatus{
		BarrierSealed: sealed,
		DiskOK:        diskOK,
		ShardCount:    len(shards),
		Shards:        shards,
	}

	switch {
	case sealed:
		status.Status = "sealed"
	case len(shards) == 0:
		status.Status = "unhealthy"
	default:
		status.Status = "ok"
		for i := range shards {
			if shards[i].Health == ShardError {
				status.Status = "degraded"
				break
			}
		}
	}

	return status
}

// RequestAddNode proposes adding a replica to the shard via a Raft
// config change. The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID to ensure ordered config
// changes. Pass 0 to skip the ordering check.
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, replicaID, addr, proto.AddNode, configChangeIndex)
}

// RequestRemoveNode requests removing a replica from a shard via Raft
// config change. The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID.
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, replicaID, "", proto.RemoveNode, configChangeIndex)
}

// SyncRequestAddNode adds a replica to the shard's membership and blocks
// until the membership change is committed. This is a convenience wrapper
// around RequestAddNode for callers that prefer a synchronous API.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error {
	rs, err := h.RequestAddNode(ctx, shardID, replicaID, addr, configChangeIndex)
	if err != nil {
		return err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return result.Err
		}
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	}
}

// SyncRequestDeleteNode removes a replica from the shard's membership and
// blocks until the membership change is committed. This is a convenience
// wrapper around RequestRemoveNode for callers that prefer a synchronous API.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error {
	rs, err := h.RequestRemoveNode(ctx, shardID, replicaID, configChangeIndex)
	if err != nil {
		return err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return result.Err
		}
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	}
}

// SyncRequestAddObserver adds a non-voting observer to a shard and blocks
// until the membership change is committed.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error {
	rs, err := h.RequestAddObserver(ctx, shardID, replicaID, addr, configChangeIndex)
	if err != nil {
		return err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return result.Err
		}
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	}
}

// SyncRequestAddWitness adds a voting witness to a shard and blocks
// until the membership change is committed.
//
// The context must carry a deadline or timeout to bound the wait.
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error {
	rs, err := h.RequestAddWitness(ctx, shardID, replicaID, addr, configChangeIndex)
	if err != nil {
		return err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return result.Err
		}
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	}
}

// RequestAddObserver requests adding a non-voting observer to a shard.
// Observers receive log entries but do not participate in quorum.
// The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID.
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, replicaID, addr, proto.AddNonVoting, configChangeIndex)
}

// RequestAddWitness requests adding a voting witness to a shard.
// Witnesses participate in quorum but do not store a state machine.
// The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID.
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, replicaID, addr, proto.AddWitness, configChangeIndex)
}

// SyncRequestSnapshot requests a snapshot of the shard with the
// specified options and blocks until the snapshot is created or the
// context is canceled. Returns the snapshot index on success.
//
// A zero-value SnapshotOption{} uses shard-level defaults.
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error) {
	rs, err := h.RequestSnapshot(ctx, shardID, opt)
	if err != nil {
		return 0, err
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			return 0, result.Err
		}
		return result.Value, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return 0, ErrTimeout
		}
		return 0, ErrCanceled
	}
}

// RemoveData removes all LogDB data for the given shard/replica pair.
// The shard must be stopped on this host before calling RemoveData.
// This is used to clean up persistent state after a node has been
// permanently removed from a shard.
func (h *Host) RemoveData(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	// Check if the shard is still loaded on this host.
	if v, loaded := h.nodes.Load(shardID); loaded {
		ns, ok := v.(*nodeState)
		if !ok {
			slog.Error("RemoveData: nodeState type assertion failed",
				"shard_id", shardID,
				"error", &TypeAssertionError{
					Context:      "RemoveData nodeState lookup",
					ExpectedType: "*nodeState",
					ActualValue:  v,
				})
		} else if !ns.stopped.Load() && ns.replicaID == replicaID {
			return &ShardNotStoppedError{ShardID: shardID, ReplicaID: replicaID}
		}
	}

	if err := h.logdb.RemoveNodeData(shardID, replicaID); err != nil {
		return err
	}

	// Fire OnNodeDeleted after successfully removing node data.
	if el := h.cfg.EventListener; el != nil && el.OnNodeDeleted != nil {
		el.OnNodeDeleted(config.EventShardInfo{
			ShardID:   shardID,
			ReplicaID: replicaID,
		})
	}

	return nil
}

// SyncRemoveData is a synchronous alias for RemoveData. Since LogDB
// operations are already blocking, it behaves identically. Provided
// for API consistency with other Sync* methods.
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error {
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	default:
	}
	return h.RemoveData(shardID, replicaID)
}

// StopReplica stops a specific replica on a shard. Since each host runs
// at most one replica per shard, this validates the replicaID matches
// and delegates to the shard stop logic.
func (h *Host) StopReplica(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	v, exists := h.nodes.Load(shardID)
	if !exists {
		return ErrShardNotFound
	}
	ns, ok := v.(*nodeState)
	if !ok {
		return &TypeAssertionError{
			Context:      "StopReplica nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}
	if ns.replicaID != replicaID {
		return &ReplicaNotFoundError{ShardID: shardID, ReplicaID: replicaID}
	}

	return h.StopShard(shardID)
}

// RequestCompaction triggers explicit log compaction for the given
// shard/replica pair. This reclaims disk space by removing entries
// that have been superseded by snapshots.
func (h *Host) RequestCompaction(shardID, replicaID uint64) error {
	if h.closed.Load() {
		return ErrClosed
	}

	v, exists := h.nodes.Load(shardID)
	if !exists {
		return ErrShardNotFound
	}
	ns, ok := v.(*nodeState)
	if !ok {
		return &TypeAssertionError{
			Context:      "RequestCompaction nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}
	if ns.replicaID != replicaID {
		return &ReplicaNotFoundError{ShardID: shardID, ReplicaID: replicaID}
	}

	return h.logdb.Compact(shardID, replicaID)
}

// HasNodeInfo checks whether bootstrap information exists for the given
// shard/replica pair in the LogDB. Returns false if the host is closed
// or the node has never been bootstrapped on this host.
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool {
	if h.closed.Load() {
		return false
	}

	nodes, err := h.logdb.ListNodeInfo()
	if err != nil {
		return false
	}
	for _, ni := range nodes {
		if ni.ShardID == shardID && ni.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

// GetLogReader returns a read-only view of the Raft log for the given
// shard. The returned ReadonlyLogReader provides access to log range
// and node state without permitting mutations.
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotFound
	}

	return &logReaderAdapter{reader: engNode.LogReader()}, nil
}

// LogRange represents the valid index range of a shard's Raft log.
// FirstIndex is the first available entry index (inclusive) and
// LastIndex is the last available entry index (inclusive).
type LogRange struct {
	FirstIndex uint64
	LastIndex  uint64
}

// QueryRaftLog returns Raft log entries in the half-open range
// [firstIndex, lastIndex) for the given shard, up to maxSize bytes
// total. This is a local operation that reads directly from the
// host's LogDB and does not require leadership.
//
// Returns ErrClosed if the host is closed, ErrShardNotFound if the
// shard does not exist on this host, and ErrInvalidRange if
// firstIndex >= lastIndex or maxSize is zero.
func (h *Host) QueryRaftLog(ctx context.Context, shardID uint64, firstIndex uint64, lastIndex uint64, maxSize uint64) ([]proto.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if firstIndex >= lastIndex {
		return nil, ErrInvalidRange
	}
	if maxSize == 0 {
		return nil, ErrInvalidRange
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotFound
	}

	lr := engNode.LogReader()
	if lr == nil {
		return nil, ErrShardNotFound
	}

	// Use the LogReader's GetRange which handles compacted/unavailable checks
	// and converts logdb entries to proto entries.
	entries, err := lr.GetRange(firstIndex, lastIndex, maxSize)
	if err != nil {
		return nil, err
	}

	// Deep-copy entries so the caller owns the data. LogReader's GetRange
	// returns entries backed by reusable internal buffers that may be
	// overwritten on the next call.
	result := make([]proto.Entry, len(entries))
	for i := range entries {
		result[i] = proto.Entry{
			Term:        entries[i].Term,
			Index:       entries[i].Index,
			Type:        entries[i].Type,
			Key:         entries[i].Key,
			ClientID:    entries[i].ClientID,
			SeriesID:    entries[i].SeriesID,
			RespondedTo: entries[i].RespondedTo,
		}
		if len(entries[i].Cmd) > 0 {
			result[i].Cmd = make([]byte, len(entries[i].Cmd))
			copy(result[i].Cmd, entries[i].Cmd)
		}
	}

	return result, nil
}

// GetLogRange returns the available log index range for the given shard.
// The returned LogRange contains the first and last available entry indices
// (both inclusive). Returns ErrClosed if the host is closed, or
// ErrShardNotFound if the shard does not exist on this host.
func (h *Host) GetLogRange(shardID uint64) (LogRange, error) {
	if h.closed.Load() {
		return LogRange{}, ErrClosed
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return LogRange{}, ErrShardNotFound
	}

	lr := engNode.LogReader()
	if lr == nil {
		return LogRange{}, ErrShardNotFound
	}

	first, last := lr.IndexRange()
	return LogRange{
		FirstIndex: first,
		LastIndex:  last,
	}, nil
}

// requestConfigChange is the shared implementation for all membership
// operations. It serializes the ConfigChange, wraps it in an
// EntryConfigChange entry, and delivers it via the engine inbox.
func (h *Host) requestConfigChange(ctx context.Context, shardID, replicaID uint64, addr string, changeType uint64, configChangeIndex uint64) (*RequestState, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	if !ns.isLeader.Load() {
		return nil, h.buildNotLeaderError(ns)
	}

	// Validate ConfigChangeID staleness. When the caller provides a non-zero
	// configChangeIndex (obtained from SyncGetShardMembership), verify it
	// matches the shard's current configuration version. This prevents
	// stale config changes from being proposed when two clients race to
	// modify the membership (PhD thesis Section 4.1).
	//
	// NOTE: This check is intentionally conservative. The atomic load of
	// configChangeIndex is not linearizable with the Raft log, so a
	// concurrent config change that commits between the caller's
	// SyncGetShardMembership and this check may cause a false rejection
	// (false positive). This is safe: the caller simply re-reads the
	// membership and retries. False negatives (accepting a truly stale
	// index) cannot occur because configChangeIndex is monotonically
	// updated on apply and this check requires an exact match.
	if configChangeIndex != 0 {
		current := ns.configChangeIndex.Load()
		if configChangeIndex != current {
			return nil, &StaleConfigChangeError{
				ShardID:  shardID,
				Expected: current,
				Got:      configChangeIndex,
			}
		}
	}

	// Check global rate limiter.
	if globalRL := h.engine.GlobalRateLimiter(); globalRL != nil && globalRL.RateLimited() {
		return nil, ErrSystemBusy
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil || engNode.Peer() == nil {
		return nil, ErrShardNotReady
	}

	// Pre-register the address in the transport registry for Add operations.
	// Per Raft PhD thesis 4.1, config changes take effect on log append. When
	// the raft layer appends the config change entry, it immediately adds the
	// remote and calls sendAppend/broadcastAppend. The transport must be able
	// to resolve the new node's address at that point, not later when the
	// entry is applied. Without this, the first Replicate messages are silently
	// dropped (registry miss), causing the new node to never receive entries
	// when no subsequent proposals create new log entries to trigger resends.
	if addr != "" && (changeType == proto.AddNode || changeType == proto.AddNonVoting || changeType == proto.AddWitness) {
		h.registry.Register(shardID, replicaID, addr)
	}

	// Serialize the config change into a binary payload.
	cc := proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           changeType,
		ReplicaID:      replicaID,
		Address:        addr,
	}
	buf := make([]byte, cc.Size())
	if _, marshalErr := cc.MarshalTo(buf); marshalErr != nil {
		return nil, &HostInitError{Field: "config_change_marshal", Err: marshalErr}
	}

	proposalKey := h.proposalKeySeq.Add(1)

	deadline, _ := ctx.Deadline()
	rs := newRequestState(ns.requestPool, proposalKey, deadline)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register pending before delivery to avoid a race where OnApplied
	// fires before the map entry exists.
	ns.pendingProposals.Store(proposalKey, &pendingProposal{rs: rs, deadline: deadline})

	entry := proto.Entry{
		Type: proto.EntryConfigChange,
		Key:  proposalKey,
		Cmd:  buf,
	}
	if !engNode.DeliverProposal([]proto.Entry{entry}) {
		ns.pendingProposals.Delete(proposalKey)
		rs.Release()
		h.completePendingOp()
		return nil, ErrSystemBusy
	}

	h.engine.NotifyWork(shardID)
	return rs, nil
}

// getNode retrieves the nodeState for a shard, returning ErrShardNotFound
// if the shard is not loaded.
func (h *Host) getNode(shardID uint64) (*nodeState, error) {
	v, ok := h.nodes.Load(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	ns, nsOK := v.(*nodeState)
	if !nsOK {
		return nil, &TypeAssertionError{
			Context:      "getNode nodeState lookup",
			ExpectedType: "*nodeState",
			ActualValue:  v,
		}
	}
	if ns.stopped.Load() {
		return nil, ErrShardNotFound
	}
	return ns, nil
}

// buildShardInfo creates a ShardInfo snapshot from a nodeState.
func (h *Host) buildShardInfo(ns *nodeState) *ShardInfo {
	ns.nodesMu.RLock()
	nodesCopy := make(map[uint64]string, len(ns.nodes))
	for k, v := range ns.nodes {
		nodesCopy[k] = v
	}
	ns.nodesMu.RUnlock()

	// Count pending proposals for the Pending field.
	var pending uint64
	ns.pendingProposals.Range(func(_, _ any) bool {
		pending++
		return true
	})

	return &ShardInfo{
		ShardID:              ns.shardID,
		ReplicaID:            ns.replicaID,
		LeaderID:             ns.leaderID.Load(),
		Term:                 ns.term.Load(),
		IsLeader:             ns.isLeader.Load(),
		IsObserver:           ns.cfg.IsObserver,
		IsWitness:            ns.cfg.IsWitness,
		StateMachineType:     ns.smType,
		StateMachineTypeName: proto.StateMachineTypeName(ns.smType),
		Pending:              pending,
		Nodes:                nodesCopy,
		Health:               ShardHealth(ns.health.Load()),
		LastApplied:          ns.lastApplied.Load(),
		ConfigChangeIndex:    ns.configChangeIndex.Load(),
	}
}

// buildNotLeaderError creates a NotLeaderError with leader info from the
// node's cached state.
func (h *Host) buildNotLeaderError(ns *nodeState) error {
	leaderID := ns.leaderID.Load()
	var leaderAddr string
	if leaderID != 0 {
		ns.nodesMu.RLock()
		leaderAddr = ns.nodes[leaderID]
		ns.nodesMu.RUnlock()
	}
	return &NotLeaderError{
		ShardID:       ns.shardID,
		LeaderID:      leaderID,
		LeaderAddress: leaderAddr,
	}
}

// loadOrGenerateHostID loads an existing host ID from disk or generates a
// new one. If overrideID is non-empty, it is used as-is. The logger is
// used for warnings about insecure file permissions or invalid formats.
func loadOrGenerateHostID(logger *slog.Logger, dir, overrideID string) (string, error) {
	if overrideID != "" {
		return overrideID, nil
	}

	idPath := filepath.Join(dir, hostIDFileName)

	// Try to load existing ID.
	data, err := os.ReadFile(idPath)
	if err == nil {
		id := string(data)
		if len(id) == hostIDLength {
			// Validate hex encoding.
			if _, err := hex.DecodeString(id); err == nil {
				// Check file permissions.
				info, statErr := os.Stat(idPath)
				if statErr == nil && info.Mode().Perm() != hostIDFilePerms {
					logger.Warn("host ID file has insecure permissions",
						"current", fmt.Sprintf("%04o", info.Mode().Perm()),
						"expected", fmt.Sprintf("%04o", hostIDFilePerms),
					)
					if chmodErr := os.Chmod(idPath, hostIDFilePerms); chmodErr != nil {
						logger.Warn("failed to fix host ID file permissions",
							"path", idPath,
							"error", chmodErr,
						)
					}
				}
				return id, nil
			}
		}
		// Invalid format: regenerate.
		logger.Warn("host ID file has invalid format, regenerating", "path", idPath)
	}

	// Generate new ID.
	buf := make([]byte, hostIDLength/2)
	if _, err := rand.Read(buf); err != nil {
		return "", &HostInitError{Field: "host_id", Err: err}
	}
	id := hex.EncodeToString(buf)

	if err := os.WriteFile(idPath, []byte(id), hostIDFilePerms); err != nil {
		return "", &HostInitError{Field: "host_id", Err: err}
	}

	return id, nil
}

// buildTransportConfig maps a HostConfig into the internal
// transport.Config, combining the host-level ListenAddress
// and DeploymentID with the user-facing config.TransportConfig tuning
// parameters.
func buildTransportConfig(hc config.HostConfig) transport.Config {
	tc := hc.TransportConfig
	var mtls *transport.MTLSConfig
	if tc.MTLSConfig != nil {
		mtls = &transport.MTLSConfig{
			CACert: tc.MTLSConfig.CACert,
			Cert:   tc.MTLSConfig.Cert,
			Key:    tc.MTLSConfig.Key,
		}
	}
	// Map the public RevocationConfig to the internal type.
	var revCfg *revocation.Config
	if tc.RevocationConfig != nil {
		rc := tc.RevocationConfig
		revCfg = &revocation.Config{
			CRLPaths:           rc.CRLPaths,
			OCSPResponderURL:   rc.OCSPResponderURL,
			OCSPCacheSeconds:   rc.OCSPCacheSeconds,
			OCSPTimeoutSeconds: rc.OCSPTimeoutSeconds,
			CheckInterval:      rc.CheckInterval,
			Mode:               revocation.Mode(rc.Mode),
			EnforceRevocation:  rc.EnforceRevocation,
			OnCertRevoked:      rc.OnCertRevoked,
		}
	}

	cfg := transport.Config{
		ListenAddress:             hc.ListenAddress,
		DeploymentID:              hc.DeploymentID,
		Enable0RTT:                tc.Enable0RTT,
		StreamPoolSize:            tc.StreamPoolSize,
		MaxStreamPoolSize:         tc.MaxStreamPoolSize,
		MaxSnapshotReceiveRate:    tc.MaxSnapshotReceiveRate,
		MaxConcurrentSnapshotRecv: tc.MaxConcurrentSnapshotReceives,
		MaxSnapshotReceiveMemory:  tc.MaxSnapshotReceiveMemory,
		SendBatchMaxSize:          tc.SendBatchMaxSize,
		MaxDecompressedSize:       tc.MaxDecompressedSize,
		MaxIncomingConnections:    tc.MaxIncomingConnections,
		MaxConnectionsPerIP:       tc.MaxConnectionsPerIP,
		UDPRecvBufSize:            tc.UDPRecvBufSize,
		UDPSendBufSize:            tc.UDPSendBufSize,
		DisableCompression:        tc.DisableCompression,
		MTLSConfig:                mtls,
		RevocationConfig:          revCfg,
		EventListener:             hc.EventListener,
	}
	cfg.SetDefaults()
	return cfg
}

// recoverLogReader initializes a LogReader from persisted LogDB state on
// restart. It applies the latest snapshot (setting the compaction boundary
// and membership), restores the hard state, and sets the entry range so
// the raft layer can see all pre-existing log entries.
//
// On first boot (no persisted data) every step is a no-op and the
// LogReader remains in its default empty state.
func (h *Host) recoverLogReader(lr *logdb.LogReader, shardID, replicaID uint64) error {
	if h.logdb == nil {
		return nil
	}

	// Step 1: Restore snapshot metadata. This sets markerIndex,
	// markerTerm, and membership on the LogReader.
	ss, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil && err != logdb.ErrNoSnapshot {
		return err
	}
	if err == nil && ss.Index > 0 {
		// Resolve the snapshot data file path and size so the raft
		// layer can send InstallSnapshot to followers after recovery.
		// The LogDB stores the snapshot directory; the data file is
		// always "snapshot.dat" inside that directory.
		var snapFilePath string
		var snapFileSize uint64
		if ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")
			if fi, statErr := os.Stat(dataPath); statErr == nil {
				snapFilePath = dataPath
				snapFileSize = uint64(fi.Size())
			}
		}
		protoSS := proto.Snapshot{
			ShardID:    shardID,
			ReplicaID:  replicaID,
			Index:      ss.Index,
			Term:       ss.Term,
			Membership: logdbMembershipToProto(ss.Membership),
			Filepath:   snapFilePath,
			FileSize:   snapFileSize,
			Epoch:      ss.Epoch,
		}
		if applyErr := lr.ApplySnapshot(protoSS); applyErr != nil {
			return applyErr
		}
	}

	// Step 2: Restore hard state (term, vote, commit).
	state, membership, err := h.logdb.ReadState(shardID, replicaID)
	if err != nil && err != logdb.ErrNoState {
		return err
	}
	if err == nil {
		lr.SetState(proto.State{
			Term:   state.Term,
			Vote:   state.Vote,
			Commit: state.Commit,
		})
		// If ReadState returned a membership (from the latest snapshot
		// in waldb), apply it. This is the same membership that
		// ApplySnapshot already set, but ReadState may return it even
		// when there is no separate snapshot record.
		if membership.ConfigChangeID > 0 || len(membership.Addresses) > 0 {
			lr.SetMembership(logdbMembershipToProto(membership))
		}
	}

	// Step 3: Restore the tracked entry range so LogReader knows which
	// indices are available in LogDB. This is O(1) -- it does not load
	// entry payloads.
	firstIndex, entryCount, err := h.logdb.EntryRange(shardID, replicaID)
	if err != nil {
		return err
	}
	if entryCount > 0 {
		if rangeErr := lr.SetRange(firstIndex, entryCount); rangeErr != nil {
			return rangeErr
		}
	}

	return nil
}

// logdbMembershipToProto converts a logdb.Membership to a proto.Membership.
// Both types have identical field layouts; this function performs a deep copy
// of all maps to prevent aliasing between the LogDB storage layer and the
// in-memory raft state.
func logdbMembershipToProto(m logdb.Membership) proto.Membership {
	result := proto.Membership{
		ConfigChangeID: m.ConfigChangeID,
	}
	if len(m.Addresses) > 0 {
		result.Addresses = make(map[uint64]string, len(m.Addresses))
		for k, v := range m.Addresses {
			result.Addresses[k] = v
		}
	}
	if len(m.Observers) > 0 {
		result.Observers = make(map[uint64]string, len(m.Observers))
		for k, v := range m.Observers {
			result.Observers[k] = v
		}
	}
	if len(m.Witnesses) > 0 {
		result.Witnesses = make(map[uint64]string, len(m.Witnesses))
		for k, v := range m.Witnesses {
			result.Witnesses[k] = v
		}
	}
	if len(m.Removed) > 0 {
		result.Removed = make(map[uint64]bool, len(m.Removed))
		for k, v := range m.Removed {
			result.Removed[k] = v
		}
	}
	return result
}

// HostInitError is returned when Host initialization fails for a specific
// component or configuration field.
type HostInitError struct {
	Field string
	Err   error
}

// Error returns a human-readable description of the initialization failure.
func (e *HostInitError) Error() string {
	return fmt.Sprintf("quicraft: host init failed for %s: %v", e.Field, e.Err)
}

// Unwrap returns the underlying error.
func (e *HostInitError) Unwrap() error {
	return e.Err
}

// updateNodeLeader updates the leader information for a node. This is
// called by the engine when a leader change is detected.
func (h *Host) updateNodeLeader(shardID, leaderID, term uint64) {
	v, exists := h.nodes.Load(shardID)
	if !exists {
		return
	}
	ns, ok := v.(*nodeState)
	if !ok {
		slog.Error("updateNodeLeader: nodeState type assertion failed",
			"shard_id", shardID,
			"error", &TypeAssertionError{
				Context:      "updateNodeLeader nodeState lookup",
				ExpectedType: "*nodeState",
				ActualValue:  v,
			})
		return
	}

	ns.leaderID.Store(leaderID)
	ns.term.Store(term)
	ns.isLeader.Store(leaderID == ns.replicaID)
}

// buildWALOptions constructs waldb.Option values from the HostConfig.
// When MaxWALDiskSize is set and/or EventListener disk callbacks are
// configured, a DiskMonitor is attached to the WAL database. The
// diskFullFlag is atomically set to true when disk full is detected
// and cleared when usage drops below the full threshold.
func buildWALOptions(cfg config.HostConfig, diskFullFlag *atomic.Bool) []waldb.Option {
	var opts []waldb.Option

	// Only configure disk monitoring if there is a size limit or
	// listener callbacks that need disk usage events.
	hasCallbacks := cfg.EventListener != nil &&
		(cfg.EventListener.OnLogDBDiskWarning != nil || cfg.EventListener.OnLogDBDiskFull != nil)

	if cfg.MaxWALDiskSize > 0 || hasCallbacks {
		maxBytes := cfg.MaxWALDiskSize
		// If callbacks are set but no size limit, we still need a
		// maxBytes to compute thresholds. Default to 0 which
		// effectively disables the monitor in NewDiskMonitor.
		if maxBytes == 0 {
			return opts
		}

		var monOpts []waldb.DiskMonitorOption

		// Always wire a recovery callback to clear diskFullFlag when
		// disk usage drops below the full threshold.
		monOpts = append(monOpts, waldb.WithOnRecovery(func(_, _ uint64) {
			diskFullFlag.Store(false)
		}))

		if cfg.EventListener != nil {
			walDir := cfg.WALDir
			if cfg.EventListener.OnLogDBDiskWarning != nil {
				warningCb := cfg.EventListener.OnLogDBDiskWarning
				monOpts = append(monOpts, waldb.WithOnWarning(func(used, maxBytes uint64) {
					warningCb(config.DiskInfo{
						WALDir:    walDir,
						UsedBytes: used,
						MaxBytes:  maxBytes,
					})
				}))
			}
			if cfg.EventListener.OnLogDBDiskFull != nil {
				fullCb := cfg.EventListener.OnLogDBDiskFull
				monOpts = append(monOpts, waldb.WithOnFull(func(used, maxBytes uint64) {
					diskFullFlag.Store(true)
					fullCb(config.DiskInfo{
						WALDir:    walDir,
						UsedBytes: used,
						MaxBytes:  maxBytes,
					})
				}))
			} else {
				// No user callback but disk monitoring is active.
				// Still set the diskFull flag for HealthCheck.
				monOpts = append(monOpts, waldb.WithOnFull(func(_, _ uint64) {
					diskFullFlag.Store(true)
				}))
			}
		} else {
			// No event listener but disk monitoring is active.
			// Set the diskFull flag for HealthCheck.
			monOpts = append(monOpts, waldb.WithOnFull(func(_, _ uint64) {
				diskFullFlag.Store(true)
			}))
		}

		opts = append(opts, waldb.WithDiskMonitorOpts(maxBytes, monOpts...))
	}

	return opts
}

// mapContextError translates a context error into the corresponding
// quicraft sentinel error. This ensures callers always see ErrCanceled
// or ErrTimeout rather than raw context.Canceled / context.DeadlineExceeded.
func mapContextError(err error) error {
	if err == context.DeadlineExceeded {
		return ErrTimeout
	}
	if err == context.Canceled {
		return ErrCanceled
	}
	return err
}

// effectiveMaxPayload returns the effective maximum proposal payload size
// given the two configurable limits. If both are non-zero, the minimum is
// used because a proposal must satisfy BOTH limits: MaxProposalPayloadSize
// (the user-facing cap) and MaxEntrySize (the transport/storage cap). If
// only one is set, that value is used.
func effectiveMaxPayload(maxProposal, maxEntry uint64) uint64 {
	switch {
	case maxProposal > 0 && maxEntry > 0:
		if maxProposal < maxEntry {
			return maxProposal
		}
		return maxEntry
	case maxProposal > 0:
		return maxProposal
	default:
		return maxEntry
	}
}

// copyWithContextChunkSize is the number of bytes copied between context
// cancellation checks in copyWithContext.
const copyWithContextChunkSize = 1 << 20 // 1 MB

// copyWithContext copies from src to dst while periodically checking the
// context for cancellation. The context is checked every 1 MB of data
// copied to balance throughput (fewer syscalls) against cancellation
// responsiveness. Returns mapContextError(ctx.Err()) on cancellation,
// or any I/O error encountered during the copy.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) error {
	buf := make([]byte, copyWithContextChunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return mapContextError(err)
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return nil
			}
			return readErr
		}
	}
}
