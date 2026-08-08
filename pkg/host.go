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
	"reflect"
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
	shardBarriers    func(shardID uint64) Barrier
	metrics          MetricsCollector
	logger           *slog.Logger
	disableTransport bool
	fsys             snapshotFS
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

// WithShardBarriers enables PER-SHARD at-rest encryption: each Raft shard can
// be sealed under its OWN barrier (per-tenant WAL/snapshot keys) instead of a
// single host-wide barrier. The resolver fn maps a shardID to the barrier that
// must encrypt that shard's snapshots and WAL records; returning nil for a
// shard falls back to the host-wide barrier configured via WithBarrier.
//
// This option is purely ADDITIVE and backward compatible. When it is not used,
// the host behaves exactly as before: WithBarrier installs a single barrier
// that encrypts every shard, and the snapshot pool's DEK-purge floor is global.
// When WithShardBarriers IS used, snapshot encrypt/decrypt, the snapshot
// transport (decrypt-for-send and in-flight epoch protection), and the
// snapshot-pool DEK purge all dispatch per shard, so purging one tenant's
// barrier can never remove epochs still live under another tenant's barrier.
//
// The caller that owns the per-shard barriers is responsible for their key
// rotation; the host-wide key-rotation loop only rotates the WithBarrier
// barrier. For per-shard barriers to also carry their own WAL records, the
// caller supplies a multiplexing LogDB via WithLogDB that implements
// logdb.PerBarrierEpochAwareLogDB (one dedicated single-barrier WAL per
// tenant). All barriers must be initialized and unsealed.
func WithShardBarriers(fn func(shardID uint64) Barrier) HostOption {
	return func(o *hostOptions) {
		o.shardBarriers = fn
	}
}

// WithMetrics enables metrics collection with the given collector. The
// collector's sub-interfaces are threaded into the engine (RaftMetrics),
// transport (TransportMetrics), and WAL LogDB (LogDBMetrics) pipelines.
func WithMetrics(c MetricsCollector) HostOption {
	return func(o *hostOptions) {
		// Normalize a typed-nil collector — e.g. a caller "disabling" metrics
		// with WithMetrics((*PrometheusMetricsCollector)(nil)) — to a real nil.
		// Otherwise the interface is non-nil (it wraps a nil pointer), every
		// `metrics != nil` guard in the host passes, and the first hot-path
		// metric call dereferences the nil receiver and panics.
		if isNilMetricsCollector(c) {
			o.metrics = nil
			return
		}
		o.metrics = c
	}
}

// isNilMetricsCollector reports whether c is nil or a typed-nil interface
// wrapping a nil pointer/map/etc.
func isNilMetricsCollector(c MetricsCollector) bool {
	if c == nil {
		return true
	}
	rv := reflect.ValueOf(c)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.Slice, reflect.Interface:
		return rv.IsNil()
	default:
		return false
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

// maxSnapshotFrameLen bounds a single encrypted snapshot frame read during
// send/export decryption. Frames are written by the engine at a 64 KiB
// plaintext chunk size plus AEAD framing overhead; 64 KiB + 256 is a safe
// ceiling that rejects a corrupt length prefix before allocation.
const maxSnapshotFrameLen = 64*1024 + 256

// snapshotEncryptChunkSize is the plaintext chunk size for receive-side snapshot
// re-encryption. It matches engine.snapshotEncryptChunkSize so frames stay
// within maxSnapshotFrameLen (chunk + AEAD overhead) and the on-disk frame
// format is identical to save-time encryption (decryptable by the recover path).
const snapshotEncryptChunkSize = 64 * 1024

// transportSender wraps a QUICTransport to implement the full
// engine.MessageSender interface, including the snapshot streaming
// path. It bridges the gap between the engine's message-level API
// and the transport's chunk-level snapshot API.
type transportSender struct {
	transport *transport.QUICTransport
	engine    *engine.Engine
	barrier   Barrier
	// shardBarriers, when non-nil, routes snapshot decrypt-for-send to a
	// per-shard barrier. When nil the single host-wide barrier is used,
	// preserving the original behavior exactly.
	shardBarriers func(shardID uint64) Barrier
	logger        *slog.Logger
	snapshotWg    sync.WaitGroup // tracks in-flight snapshot sends
	// fsys routes snapshot decrypt-for-send I/O; nil means the real
	// filesystem. Test-only injection, mirroring Host.fsys.
	fsys snapshotFS
}

// barrierFor returns the barrier that encrypts the given shard's snapshots.
// With per-shard barriers it routes by shardID, falling back to the host-wide
// barrier when the resolver returns nil. Without per-shard barriers it always
// returns the host-wide barrier.
func (s *transportSender) barrierFor(shardID uint64) Barrier {
	if s.shardBarriers != nil {
		if b := s.shardBarriers(shardID); b != nil {
			return b
		}
	}
	return s.barrier
}

// logDBAtRestProtection reports a LogDB's declared at-rest guarantee. A LogDB
// that does not implement logdb.AtRestProtectedLogDB declares nothing, and a
// nil LogDB is not a store at all; both read as UNPROTECTED so every caller
// fails closed on silence.
func logDBAtRestProtection(db logdb.LogDB) logdb.AtRestProtection {
	p, ok := db.(logdb.AtRestProtectedLogDB)
	if !ok {
		return logdb.AtRestUnprotected
	}
	return p.AtRestProtection()
}

// requireEncryptedLogDB refuses a LogDB whose Raft log would be readable off
// the machine's storage. It backs ErrShardBarriersPlaintextWAL: with per-shard
// barriers and no host-wide barrier, nothing else in the host encrypts the log.
//
// AtRestNonDurable is accepted for the reason it is a distinct state: an
// in-memory LogDB writes no file, so there is no at-rest artifact to protect
// and no crash-surviving copy of a user command. That is a property of the
// STORE, not a property of the caller having passed WithLogDB.
func requireEncryptedLogDB(db logdb.LogDB) error {
	if db == nil {
		// The default WAL is about to be created and no barrier was configured
		// for it, so it would write plaintext segments.
		return ErrShardBarriersPlaintextWAL
	}
	switch logDBAtRestProtection(db) {
	case logdb.AtRestBarrierEncrypted, logdb.AtRestNonDurable:
		return nil
	default:
		return ErrShardBarriersPlaintextWAL
	}
}

// barrierForShard resolves the at-rest barrier for a shard, mirroring
// transportSender.barrierFor on the send path: the per-shard barrier when
// WithShardBarriers is configured and returns non-nil, otherwise the host-wide
// barrier. This is what lets a follower re-encrypt a received snapshot under
// the SAME per-shard barrier it will later read it through, so per-node/
// per-shard keys stay consistent across the receive→recover→read cycle.
//
// It fails CLOSED with ErrShardBarrierUnresolved when WithShardBarriers is
// configured, the resolver returns nil, and there is no host-wide barrier to
// fall back on. That combination has no encrypting outcome: the caller asked
// for per-shard encryption and this shard resolved to nothing, so writing it in
// plaintext would downgrade exactly one tenant invisibly. Note the asymmetry
// this removes -- a SEALED barrier already fails closed everywhere, while an
// ABSENT one used to fail open.
//
// A nil return with a nil error is the legitimate unencrypted deployment:
// WithShardBarriers was never configured and no host-wide barrier exists.
func (h *Host) barrierForShard(shardID uint64) (Barrier, error) {
	if h.shardBarriers == nil {
		return h.barrier, nil
	}
	if b := h.shardBarriers(shardID); b != nil {
		return b, nil
	}
	if h.barrier == nil {
		return nil, ErrShardBarrierUnresolved
	}
	// Documented fallback: WithShardBarriers says a nil result uses the
	// host-wide barrier, which still encrypts. Warn once per shard so an
	// unintended resolver miss is observable rather than merely legal.
	h.warnBarrierFallback(shardID)
	return h.barrier, nil
}

// warnBarrierFallback logs the host-wide-barrier fallback once per shard. The
// resolution runs on every snapshot save, recovery, send, receive and purge, so
// an unconditional warning would bury the first (and only informative) one.
func (h *Host) warnBarrierFallback(shardID uint64) {
	if _, warned := h.barrierFallbackWarned.LoadOrStore(shardID, struct{}{}); warned {
		return
	}
	h.logger.Warn("per-shard barrier resolver returned no barrier; "+
		"falling back to the host-wide barrier",
		"shard_id", shardID)
}

// verifyShardLogDBBarrier refuses to start a shard whose Raft log the supplied
// LogDB does not store under the shard's OWN barrier.
//
// It closes the second half of the per-shard axis. requireEncryptedLogDB proves
// the log is encrypted; it does not prove it is encrypted under the key this
// shard resolved to. A waldb opened with tenant A's barrier and a resolver that
// hands shard 7 tenant B's barrier is encrypted at rest AND stores tenant B's
// commands under tenant A's key -- exactly the cross-tenant key mixing
// WithShardBarriers exists to prevent.
//
// The check applies only when there is no host-wide barrier. With WithBarrier
// the WAL is encrypted host-wide by design (documented, and warned about at
// construction) and the per-shard barriers cover snapshots only, so demanding
// per-shard WAL ownership would refuse a supported configuration.
//
// logdb.PerBarrierEpochAwareLogDB answers by barrier IDENTITY: known == true
// means "my records for this barrier are these", which for waldb.DB is decided
// by the barrier it was opened with. A LogDB that cannot answer cannot prove
// isolation, so it is refused rather than trusted.
func (h *Host) verifyShardLogDBBarrier(shardID uint64, barrier Barrier) error {
	if h.shardBarriers == nil || h.barrier != nil || h.logdb == nil {
		return nil
	}
	if logDBAtRestProtection(h.logdb) != logdb.AtRestBarrierEncrypted {
		// AtRestNonDurable has no at-rest artifact to attribute to a barrier;
		// AtRestUnprotected was already refused by NewHost.
		return nil
	}
	perBarrier, ok := h.logdb.(logdb.PerBarrierEpochAwareLogDB)
	if !ok {
		h.logger.Error("refusing to start shard: the supplied LogDB cannot "+
			"attribute its records to a barrier, so it cannot prove this shard's "+
			"log is stored under this shard's own key",
			"shard_id", shardID)
		return ErrShardBarrierLogDBMismatch
	}
	_, known, err := perBarrier.MinLiveEpochForBarrier(barrier)
	if err != nil {
		return err
	}
	if !known {
		h.logger.Error("refusing to start shard: the supplied LogDB does not "+
			"store this shard's log under the barrier the resolver returned for "+
			"it; the log would be encrypted under another tenant's key",
			"shard_id", shardID)
		return ErrShardBarrierLogDBMismatch
	}
	return nil
}

// snapshotBarrierForShard resolves the shard's at-rest barrier for a path that
// is about to WRITE a snapshot artifact, collapsing the two ways encryption can
// be unavailable into one error:
//
//   - ErrShardBarrierUnresolved  — per-shard encryption is configured and this
//     shard resolved to nothing (see barrierForShard).
//   - ErrSnapshotBarrierSealed   — the shard HAS a barrier and it is sealed, so
//     encryption is temporarily unavailable.
//
// A nil barrier with a nil error is the configured plaintext deployment.
func (h *Host) snapshotBarrierForShard(shardID uint64) (Barrier, error) {
	b, err := h.barrierForShard(shardID)
	if err != nil {
		return nil, err
	}
	if b != nil && b.IsSealed() {
		return nil, ErrSnapshotBarrierSealed
	}
	return b, nil
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
				s.safeNotifySnapshotRejected(msg.ShardID, msg.From, msg.To, msg.Hint)
			}
		}()
		s.doSendSnapshot(msg)
	}()
}

// safeNotifySnapshotRejected attempts to deliver a rejected SnapshotStatus
// notification, recovering from any secondary panic (e.g., if the engine
// is already torn down). This prevents a double-panic in the deferred
// recovery handler of SendSnapshot from crashing the process.
func (s *transportSender) safeNotifySnapshotRejected(shardID, from, to, transferSeq uint64) {
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
	s.notifySnapshotStatus(shardID, from, to, transferSeq, true)
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
	// receiver (which has a different barrier key) can read it. The barrier
	// is routed by shardID so per-tenant snapshots decrypt under the correct
	// barrier; without per-shard barriers this is the host-wide barrier.
	sendBarrier := s.barrierFor(msg.ShardID)
	if msg.Snapshot.Epoch > 0 {
		// The snapshot is encrypted at rest and MUST be decrypted to plaintext
		// before it goes on the wire: the receiver's barrier holds a different
		// key and can never decrypt our ciphertext. If the barrier that sealed
		// it is unavailable (sealed, or absent), refuse the send and notify the
		// leader rejected. Shipping ciphertext the peer cannot decrypt — while
		// pinning a meaningless at-rest epoch on the receiver — would leave the
		// follower permanently unable to apply the snapshot and stuck in
		// snapshot state. A rejection lets the leader retry once the barrier is
		// unsealed.
		if sendBarrier == nil || sendBarrier.IsSealed() {
			s.logger.Warn("snapshot send refused: encrypted snapshot but barrier unavailable",
				"shard", msg.ShardID,
				"to", msg.To,
				"epoch", msg.Snapshot.Epoch,
				"barrier_nil", sendBarrier == nil,
			)
			s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
			return
		}
		decryptedPath, decryptedSize, err := s.decryptSnapshotForSend(msg, sendBarrier)
		if err != nil {
			s.logger.Warn("snapshot decrypt-for-send failed",
				"shard", msg.ShardID,
				"to", msg.To,
				"epoch", msg.Snapshot.Epoch,
				"error", err,
			)
			s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
			return
		}
		// Register temp file cleanup immediately after successful creation.
		// This ensures cleanup even if a panic occurs later in this function.
		tempPath := decryptedPath
		resourceCleanup = append(resourceCleanup, func() {
			if err := s.snapshotFsys().Remove(tempPath); err != nil && !os.IsNotExist(err) {
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
		return forEachSnapshotChunk(s.snapshotFsys(), msg, emit)
	}

	if err := s.transport.SendSnapshotStreaming(meta, producer); err != nil {
		s.logger.Warn("snapshot send failed",
			"shard", msg.ShardID,
			"to", msg.To,
			"error", err,
		)
		s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, true)
		return
	}

	s.notifySnapshotStatus(msg.ShardID, msg.From, msg.To, msg.Hint, false)
}

// decryptSnapshotForSend decrypts an encrypted snapshot file to a
// temporary file for network transfer. The encrypted format is a
// sequence of frames: [4-byte LE length][encrypted chunk]...
// Returns the temp file path and the plaintext file size.
func (s *transportSender) decryptSnapshotForSend(msg proto.Message, barrier Barrier) (string, uint64, error) {
	return decryptSnapshotToTemp(s.snapshotFsys(), msg.Snapshot.Filepath,
		sendDecryptTempPattern, msg.Snapshot.Epoch, barrier)
}

// sendDecryptTempPattern names the scratch file the send path decrypts into,
// and recoverDecryptTempPattern the one the restart-recovery path uses. Both
// are taken from the engine's shared constants, which is what guarantees
// engine.SweepStaleDecryptTmp reclaims either file when a crash orphans it: a
// send-path scratch file the sweeper did not match was a complete plaintext copy
// of the state machine left beside the ciphertext for the life of the snapshot
// directory.
//
// recvEncryptTempPattern and reencryptTempPattern name the ENCRYPT-side
// scratch files (an in-place re-encryption of a received/imported artifact, and
// a barrier-to-barrier migration). They come from the same constants for the
// same structural reason. Their orphans hold CIPHERTEXT, so the exposure is a
// disk leak rather than a confidentiality one — but the invariant the sweeper
// rests on is that a name which can be CREATED is a name that can be RECLAIMED,
// and a locally-spelled pattern is exactly how a name escapes it.
const (
	sendDecryptTempPattern    = engine.SendDecryptTempPattern
	recoverDecryptTempPattern = engine.RecoverDecryptTempPattern
	recvEncryptTempPattern    = engine.RecvEncryptTempPattern
	reencryptTempPattern      = engine.ReencryptTempPattern
)

// shardSnapshotBaseDir returns the BASE snapshot directory for a shard — the
// directory engine.Node.SnapshotDir() reports and that every snapshot path
// (save, receive, import, garbage collection, and the DEK purge-floor scan)
// resolves against via snapshotSaveDir/listSnapshotDirs.
//
// It is the single definition of that layout. Any path that builds it by hand
// risks writing snapshot artifacts into a tree the GC and the purge-floor scan
// never enumerate; because the purge floor is computed by SCANNING these
// directories, an artifact outside the tree contributes no floor and its DEK is
// legitimately destroyed by the next PurgeEpochsBefore, leaving the snapshot
// permanently undecryptable.
//
// dataDir is the shard's optional Config.DataDir: when set, the shard's
// snapshot tree roots on the tenant's own volume instead of under NodeHostDir.
func shardSnapshotBaseDir(nodeHostDir, dataDir string, shardID uint64) string {
	if dataDir != "" {
		return filepath.Join(dataDir, fmt.Sprintf("shard-%d", shardID))
	}
	return filepath.Join(nodeHostDir, "snapshots", fmt.Sprintf("shard-%d", shardID))
}

// decryptSnapshotToTemp decrypts the framed snapshot ciphertext at srcPath into
// a NEW sibling temp file created from pattern, returning that file's path and
// the plaintext byte count. srcPath is never modified: the canonical encrypted
// snapshot.dat stays the only durable artifact, which is what keeps a failed
// decrypt retryable. Decrypting in place would leave plaintext on disk while
// LogDB still records Epoch>0, so every later read would re-attempt decryption
// on plaintext and fail permanently.
//
// The caller owns the returned file and must remove it. On any error the temp
// file is closed and removed before returning.
//
// Each frame is decrypted with the epoch embedded in its own ciphertext header;
// epoch is only pinned live for the duration of the read so a concurrent purge
// cannot remove the oldest DEK a frame may reference mid-decrypt. This mirrors
// the belt-and-suspenders RegisterInFlightEpoch saveSnapshot takes around its
// own encrypt: the recorded epoch is the minimum across the frames.
//
// Shared by the transport send path (decryptSnapshotForSend) and StartShard's
// local-snapshot recovery so both consume identical plaintext.
func decryptSnapshotToTemp(fsys snapshotFS, srcPath, pattern string, epoch uint64, barrier Barrier) (string, uint64, error) {
	barrier.RegisterInFlightEpoch(epoch)
	defer barrier.DeregisterInFlightEpoch(epoch)

	srcFile, err := fsys.Open(srcPath)
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
					"path", srcPath,
					"error", closeErr)
			}
		}
	}()

	tmpFile, err := fsys.CreateTemp(filepath.Dir(srcPath), pattern)
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
					"path", tmpPath,
					"error", closeErr)
			}
			// Remove the temp file, ignoring "not exist" errors since
			// file creation may have been rolled back by the OS.
			if rmErr := fsys.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
				slog.Debug("snapshot temp file removal failed",
					"path", tmpPath,
					"error", rmErr)
			}
		}
	}()

	// Read and decrypt each frame.
	totalSize, err := decryptSnapshotFrames(barrier, srcFile, tmpFile)
	if err != nil {
		return "", 0, err
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

// decryptSnapshotFrames reads the encrypted snapshot frame format from src,
// decrypts each frame, and writes the plaintext to dst. The on-disk encrypted
// format is a sequence of frames: [4-byte LE length][encrypted chunk]...
// terminated by EOF. Returns the total number of plaintext bytes written.
//
// Each frame is decrypted with the epoch EMBEDDED in its own ciphertext header
// (barrier.Decrypt), not a single snapshot-wide epoch: a snapshot saved across
// a key rotation contains frames sealed at different epochs, and using one
// epoch for all of them would fail AEAD authentication on the mismatched
// frames. Shared by the transport send path (decryptSnapshotForSend) and the
// ExportSnapshot API so both emit identical plaintext.
// encryptSnapshotFrames reads plaintext snapshot data from src and writes it to
// dst as a sequence of frames [4-byte LE length][AEAD ciphertext], one per
// snapshotEncryptChunkSize-byte chunk, encrypted under the given barrier's
// current epoch DEK. It returns the MINIMUM epoch across the frames (a
// concurrent rotation can advance the epoch mid-stream), which the caller
// records as the snapshot's at-rest epoch so the DEK-purge floor protects every
// frame. The frame format matches engine.snapshotPool.encryptSnapshotFile and is
// decryptable by decryptSnapshotFrames / the recover path, so a snapshot
// RE-ENCRYPTED on receipt under the RECEIVER's own (per-node) barrier recovers
// correctly with that same barrier — the basis for per-node at-rest encryption
// of received snapshots without any shared cross-node key.
func encryptSnapshotFrames(barrier Barrier, src io.Reader, dst io.Writer) (uint64, error) {
	chunkBuf := make([]byte, snapshotEncryptChunkSize)
	var frameLenBuf [4]byte
	var minEpoch uint64
	for {
		n, readErr := src.Read(chunkBuf)
		if n > 0 {
			ct, epoch, encErr := barrier.EncryptWithEpoch(nil, chunkBuf[:n])
			if encErr != nil {
				return 0, encErr
			}
			if minEpoch == 0 || epoch < minEpoch {
				minEpoch = epoch
			}
			binary.LittleEndian.PutUint32(frameLenBuf[:], uint32(len(ct)))
			if _, werr := dst.Write(frameLenBuf[:]); werr != nil {
				return 0, werr
			}
			if _, werr := dst.Write(ct); werr != nil {
				return 0, werr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
	}
	return minEpoch, nil
}

// reencryptReceivedSnapshot re-encrypts a just-written PLAINTEXT snapshot.dat at
// path under the given (receiver's) barrier, in place and crash-safely (encrypt
// to a temp sibling, fsync, atomic rename). It returns the recorded min epoch.
// The received wire bytes are plaintext (the sender decrypts before streaming —
// see doSendSnapshot), so this is what makes the follower's at-rest snapshot
// encrypted under its OWN per-node key.
//
// A NIL barrier means encryption is disabled for the shard: the snapshot stays
// plaintext and epoch 0 is returned. A CONFIGURED but SEALED barrier is the
// distinct "encryption unavailable" state and fails with
// ErrSnapshotBarrierSealed — writing the received bytes as plaintext would
// durably downgrade a shard whose data must be encrypted at rest, at exactly
// the moment an operator sealed the host.
func reencryptReceivedSnapshot(fsys snapshotFS, path string, barrier Barrier) (uint64, error) {
	if barrier == nil {
		return 0, nil
	}
	if barrier.IsSealed() {
		return 0, ErrSnapshotBarrierSealed
	}
	src, err := fsys.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = src.Close() }()

	tmp, err := fsys.CreateTemp(filepath.Dir(path), recvEncryptTempPattern)
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = fsys.Remove(tmpPath)
		}
	}()

	epoch, err := encryptSnapshotFrames(barrier, src, tmp)
	if err != nil {
		return 0, err
	}
	if err := tmp.Sync(); err != nil {
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if err := src.Close(); err != nil {
		return 0, err
	}
	if err := fsys.Rename(tmpPath, path); err != nil {
		return 0, err
	}
	committed = true
	return epoch, nil
}

func decryptSnapshotFrames(barrier Barrier, src io.Reader, dst io.Writer) (uint64, error) {
	var frameLenBuf [4]byte
	var totalSize uint64
	for {
		_, readErr := io.ReadFull(src, frameLenBuf[:])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
		frameLen := binary.LittleEndian.Uint32(frameLenBuf[:])
		// Reject a corrupt/oversized length prefix before allocating. A frame
		// is one encrypted 64 KiB chunk plus small AEAD framing overhead.
		if frameLen > maxSnapshotFrameLen {
			return 0, ErrSnapshotCorrupt
		}

		frame := make([]byte, frameLen)
		if _, readErr = io.ReadFull(src, frame); readErr != nil {
			return 0, readErr
		}

		plaintext, decErr := barrier.Decrypt(nil, frame)
		if decErr != nil {
			return 0, decErr
		}

		if _, writeErr := dst.Write(plaintext); writeErr != nil {
			return 0, writeErr
		}
		totalSize += uint64(len(plaintext))
	}
	return totalSize, nil
}

// notifySnapshotStatus injects a SnapshotStatus message into the
// sender's raft inbox to inform the leader of the transfer outcome.
// The From field is set to the target replica (the follower that
// received or rejected the snapshot) so the leader's raft layer
// can look up the correct remote entry. Hint echoes the transfer
// sequence the leader stamped on the InstallSnapshot (raft
// snapshotSeq), so handleSnapshotStatus can discard a stale status
// from a superseded transfer attempt — even one re-sending the same
// snapshot index — instead of arming the ack countdown (or forcing a
// backoff) for a newer in-flight one. Delivery is retried briefly:
// this is a one-shot message with no protocol-level retransmission,
// and losing it leaves the remote paused with no countdown armed.
func (s *transportSender) notifySnapshotStatus(shardID, localReplicaID, targetReplicaID, transferSeq uint64, rejected bool) {
	statusMsg := proto.Message{
		Type:    proto.SnapshotStatus,
		ShardID: shardID,
		From:    targetReplicaID,
		To:      localReplicaID,
		Hint:    transferSeq,
		Reject:  rejected,
	}
	if !deliverSnapshotControl(s.engine, shardID, statusMsg) {
		s.logger.Warn("failed to deliver SnapshotStatus",
			"shard", shardID,
			"target", targetReplicaID,
			"transfer_seq", transferSeq,
			"rejected", rejected,
		)
	}
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
// emptySnapshotChunk builds the single metadata-only chunk that represents a
// snapshot with no data: a witness snapshot (no Filepath) or a data file that
// is genuinely zero bytes.
func emptySnapshotChunk(msg proto.Message, snap *proto.Snapshot) proto.SnapshotChunk {
	return proto.SnapshotChunk{
		ShardID:        msg.ShardID,
		ReplicaID:      msg.To,
		From:           msg.From,
		Index:          snap.Index,
		Term:           snap.Term,
		ChunkID:        0,
		ChunkCount:     1,
		ChunkSize:      0,
		FileSize:       0,
		Filepath:       snap.Filepath,
		FileChunkID:    0,
		FileChunkCount: 1,
		Membership:     snap.Membership,
		OnDiskIndex:    snap.OnDiskIndex,
		BinVer:         proto.WireVersion,
		Epoch:          snap.Epoch,
	}
}

func forEachSnapshotChunk(fsys snapshotFS, msg proto.Message, emit func(proto.SnapshotChunk) error) error {
	snap := &msg.Snapshot
	filePath := snap.Filepath
	fileSize := snap.FileSize

	// A snapshot with NO data file — a witness, or a state machine that saved
	// nothing — is the only legitimate metadata-only transfer. A DECLARED size
	// of zero is not: the file exists, and treating a metadata/artifact
	// disagreement as "empty" ships a snapshot with no data, which the
	// receiving follower recovers from and then marks every index at or below
	// the snapshot index applied. That is silent, permanent state loss on the
	// follower, so a non-empty Filepath is always opened and verified below.
	if filePath == "" {
		return emit(emptySnapshotChunk(msg, snap))
	}

	// Open the snapshot data file for streaming reads. Only one chunk's
	// worth of data is held in memory at a time, avoiding OOM on large
	// snapshots (up to DefaultMaxSnapshotSize = 4GB).
	//
	// The defer for cleanup is registered immediately after a successful
	// open to ensure the file handle is always closed, even on panic.
	// The fileClosed flag allows explicit early close while preventing
	// double-close errors in the deferred cleanup.
	f, err := fsys.Open(filePath)
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

	// A genuinely empty data file: metadata agrees with disk, so there is
	// nothing to stream. Computing chunkCount here would underflow.
	if fileSize == 0 {
		return emit(emptySnapshotChunk(msg, snap))
	}

	chunkCount := (fileSize-1)/snapshotChunkSize + 1

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

	// onFailure runs exactly once, at the completion site that claims this
	// entry with an error (OnProposalFailed, failPendingRequests, the
	// deadline sweep). It is NOT run on the applied path: an applied entry
	// committed, so any speculative side effect it owns is now legitimate.
	// Used by requestConfigChange to undo its speculative address
	// pre-registration for an entry that was appended but never committed
	// (leader deposed, log truncated). Nil for ordinary proposals.
	onFailure func()
}

// failed runs the entry's failure hook. Safe when no hook is set.
func (pp *pendingProposal) failed() {
	if pp.onFailure != nil {
		pp.onFailure()
	}
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
			rs.releaseRef()
			c.h.completePendingOp()
		}
	}

	// Advance lastApplied and complete confirmed pending reads whose readIndex
	// has been reached. The lastApplied store MUST happen under pendingReadsMu,
	// before the read scan, and OnReadyToRead must read lastApplied under the
	// same lock. Otherwise a read confirmed by OnReadyToRead in the window
	// between this scan and a post-unlock store would be completed by neither
	// path (OnReadyToRead sees a stale lastApplied; this scan ran before the
	// read was confirmed) and would hang until its deadline.
	if len(entries) > 0 {
		newLastApplied := entries[len(entries)-1].Index
		ns.pendingReadsMu.Lock()
		ns.lastApplied.Store(newLastApplied)
		for key, pr := range ns.pendingReads {
			if pr.confirmed && newLastApplied >= pr.readIndex {
				if c.h.metrics != nil && !pr.rs.createdAt.IsZero() {
					c.h.metrics.ObserveReadIndexLatency(shardID, time.Since(pr.rs.createdAt))
				}
				pr.rs.complete(RequestResult{Value: pr.readIndex})
				delete(ns.pendingReads, key)
				pr.rs.releaseRef()
				c.h.completePendingOp()
			}
		}
		ns.pendingReadsMu.Unlock()
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
	ns.pendingReadsMu.Lock()
	// Load lastApplied under pendingReadsMu so it is consistent with
	// OnApplied's store (which also happens under this lock). Loading it
	// before the lock would risk seeing a value older than an OnApplied that
	// already scanned (and skipped) this not-yet-confirmed read, leaving the
	// read completed by neither path. See OnApplied.
	lastApplied := ns.lastApplied.Load()
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
			pr.rs.releaseRef()
			c.h.completePendingOp()
			// Complete all alias keys that share this batch's ReadIndex.
			for _, aliasKey := range pr.aliases {
				if aliasPR, aliasOK := ns.pendingReads[aliasKey]; aliasOK {
					if c.h.metrics != nil && !aliasPR.rs.createdAt.IsZero() {
						c.h.metrics.ObserveReadIndexLatency(shardID, time.Since(aliasPR.rs.createdAt))
					}
					aliasPR.rs.complete(RequestResult{Value: rtr.Index})
					delete(ns.pendingReads, aliasKey)
					aliasPR.rs.releaseRef()
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
		// The batch key was already removed (expired by the sweep or failed
		// by a close/leadership sweep) before the aliases could attach.
		// Alias reads are reachable ONLY through the batch entry — the raft
		// confirmation arrives keyed by batchKey — so without it they could
		// never complete. Fail them now instead of leaving deadline-less
		// callers blocked forever.
		for _, aliasKey := range aliasKeys {
			aliasPR, aliasOK := ns.pendingReads[aliasKey]
			if !aliasOK {
				continue
			}
			aliasPR.rs.complete(RequestResult{Err: ErrTimeout})
			delete(ns.pendingReads, aliasKey)
			aliasPR.rs.releaseRef()
			c.h.completePendingOp()
		}
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
	// 2. The leader actually changed (not the same leader re-elected)
	// 3. The new leader is not US — proposals we are about to commit
	//    ourselves must not be failed out from under the caller.
	//
	// prevLeaderID == 0 is INCLUDED deliberately. A dying leader takes the
	// shard through leaderID 0 first, so by the time the new leader is
	// observed prevLeaderID is already 0. Requiring prevLeaderID != 0 here
	// meant every proposal submitted during the leaderless window stayed
	// pending after a leader existed again, and the caller blocked until its
	// OWN deadline — measured as a ~5s tail on an election that completed in
	// ~530ms. Those proposals were never forwarded anywhere and cannot
	// complete, so the new leader's arrival is exactly when to fail them and
	// let the caller retry against it.
	if !wasLeader && prevLeaderID != leaderID && leaderID != ns.replicaID {
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
		rs.releaseRef()
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
			// Unlike the completion paths, OnCommitted must leave the map
			// entry in place for the later OnApplied, so it cannot claim
			// via LoadAndDelete. A bare commitNotify after Load would race
			// the claiming paths (OnApplied/OnProposalFailed/
			// failPendingRequests/sweep): they can remove, complete, and
			// releaseRef between the Load and the notify, recycle the
			// RequestState into a brand-new request, and the stale notify
			// would then win the fresh request's committedDone CAS and
			// push a bogus WAL-durable notification into it. Instead, pin
			// the RS with tryAcquireRef (safe against recycling — it never
			// increments from zero), then re-verify the map still holds
			// the SAME pendingProposal. Keys are host-wide monotonic and
			// never reused, so a matching re-Load proves the entry — and
			// therefore the map's own reference — was held continuously
			// across the acquire, i.e. the pinned RS is still this
			// request's. If the verify fails the request was claimed (or
			// the count belonged to a recycled successor); drop the
			// balanced reference and skip.
			if !pp.rs.tryAcquireRef() {
				continue
			}
			if v2, still := ns.pendingProposals.Load(entry.Key); !still || v2 != v {
				pp.rs.releaseRef()
				continue
			}
			pp.rs.commitNotify(RequestResult{Value: 0})
			pp.rs.releaseRef()
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
			pp.failed()
			pp.rs.complete(RequestResult{Err: apiErr})
			pp.rs.releaseRef()
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
			pr.rs.releaseRef()
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
	if errors.Is(err, raft.ErrPendingConfigChange) {
		// A config change proposed while another is still uncommitted, or
		// two batched into one Propose (PhD 4.1 permits only one in flight).
		// Without this the internal error escapes verbatim, and since it
		// lives in an internal package no caller can match it with
		// errors.Is — every consumer falls through to its own catch-all and
		// reports a retryable rejection as a fatal error. It IS retryable:
		// nothing entered the log, so re-proposing once the pending change
		// commits succeeds.
		return ErrRejected
	}
	return err
}

// Compile-time assertion that hostEngineCallback implements Callback.
var _ engine.Callback = (*hostEngineCallback)(nil)

// Host is the main entry point for the QuicRaft multi-group Raft library.
// One Host per process. It manages shard lifecycle, proposal routing,
// read operations, session management, and cluster membership.
type Host struct {
	cfg           config.HostConfig
	logger        *slog.Logger
	logdb         logdb.LogDB
	barrier       Barrier
	shardBarriers func(shardID uint64) Barrier // per-shard at-rest barrier resolver; nil when WithShardBarriers unused
	// barrierFallbackWarned records the shards already warned about falling
	// back from an unresolved per-shard barrier to the host-wide one, so the
	// warning fires once per shard instead of once per snapshot operation.
	barrierFallbackWarned sync.Map // map[uint64]struct{}
	metrics               MetricsCollector
	engine                *engine.Engine
	registry              *registry.Registry
	sender                engine.MessageSender
	transport             *transport.QUICTransport // nil when transport is disabled
	nodes                 sync.Map                 // map[uint64]*nodeState — lock-free reads for StaleRead/QueryLocalNode
	activeNodes           atomic.Int64             // tracks the number of loaded shards (replaces len(h.nodes))
	closed                atomic.Bool
	draining              atomic.Bool
	diskFull              *atomic.Bool // set by disk full callback in buildWALOptions
	id                    string
	stopOnce              sync.Once
	proposalKeySeq        atomic.Uint64 // monotonic key generator for proposal tracking
	readKeySeq            atomic.Uint64 // monotonic key generator for read request tracking
	snapshotKeySeq        atomic.Uint64 // monotonic key generator for snapshot request tracking
	pendingOps            atomic.Int64  // tracks outstanding pending operations for drain
	keyRotStopC           chan struct{} // closed to stop the key rotation goroutine
	keyRotDone            chan struct{} // closed when the key rotation goroutine exits
	sweepStopC            chan struct{} // closed to stop the pending proposal sweeper goroutine
	sweepDone             chan struct{} // closed when the sweeper goroutine exits
	drainC                chan struct{} // closed when pendingOps reaches 0 during drain
	drainCMu              sync.Mutex    // protects drainC creation
	// fsys routes snapshot persistence I/O. nil means the real filesystem;
	// only tests set it (withSnapshotFS), so the injection never reaches the
	// public API.
	fsys snapshotFS

	// joiningShards holds the shards started with join=true whose bootstrap
	// snapshot has not arrived yet (map[uint64]struct{}). Such a shard has no
	// membership and no registered peer addresses, so the leader's first
	// snapshot — the only carrier of the full membership — cannot be
	// authorized against the registry. The entry is removed as soon as a
	// snapshot is accepted for the shard, closing the window.
	joiningShards sync.Map

	// importLocks serialises ImportSnapshot per shard
	// (map[uint64]chan struct{}, each of capacity one).
	//
	// Nothing else serialises imports, and two concurrent imports of the same
	// shard race over the SAME on-disk directory: the artifact index names it,
	// so two calls carrying the same index resolve to one path, and both the
	// staleness check and the destination-exists check below would otherwise be
	// evaluated against a tree the other call is midway through changing. The
	// lock makes the check-then-write sequence atomic per shard.
	importLocks sync.Map
}

// acquireImportLock takes the shard's exclusive import lock, waiting until it
// is free or ctx is done. The returned function releases it.
//
// The channel is the lock: a buffered channel of capacity one admits exactly
// one holder and, unlike a sync.Mutex, lets a caller abandon the wait when its
// context is cancelled — an import can run for as long as it takes to write a
// multi-gigabyte artifact, so blocking uninterruptibly behind one is not an
// acceptable answer to a cancelled request.
func (h *Host) acquireImportLock(ctx context.Context, shardID uint64) (func(), error) {
	v, _ := h.importLocks.LoadOrStore(shardID, make(chan struct{}, 1))
	lock, ok := v.(chan struct{})
	if !ok {
		return nil, &TypeAssertionError{
			Context:      "acquireImportLock importLocks lookup",
			ExpectedType: "chan struct{}",
			ActualValue:  v,
		}
	}
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, mapContextError(ctx.Err())
	}
}

// tryAcquireImportLock takes the shard's exclusive import lock if it is free,
// reporting whether it did. The returned function releases it and is nil when
// the lock was not taken.
//
// It exists for callers that must never WAIT for an import — StartShard sweeps
// orphaned import staging directories and an import can run for as long as it
// takes to write a multi-gigabyte artifact — and for which "an import holds the
// lock" is itself the answer: whatever the sweep would have found belongs to
// that import.
func (h *Host) tryAcquireImportLock(shardID uint64) (func(), bool) {
	v, _ := h.importLocks.LoadOrStore(shardID, make(chan struct{}, 1))
	lock, ok := v.(chan struct{})
	if !ok {
		return nil, false
	}
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, true
	default:
		return nil, false
	}
}

// markShardJoining records that a shard is in its join bootstrap window.
func (h *Host) markShardJoining(shardID uint64) {
	h.joiningShards.Store(shardID, struct{}{})
}

// clearShardJoining ends a shard's join bootstrap window. It is idempotent.
func (h *Host) clearShardJoining(shardID uint64) {
	h.joiningShards.Delete(shardID)
}

// isShardJoining reports whether a shard is still in its join bootstrap
// window: started with join=true and not yet given a snapshot.
func (h *Host) isShardJoining(shardID uint64) bool {
	_, ok := h.joiningShards.Load(shardID)
	return ok
}

// IsShardJoining reports whether shardID is still in its join bootstrap
// window: started locally with join=true and not yet given its first
// snapshot. Once a snapshot is accepted the shard leaves this window
// permanently for the life of the process. Callers (e.g. qrdb's placement
// reconciler) use this to know when a dynamically added replica has caught
// up enough to be treated as an established member on future restarts.
func (h *Host) IsShardJoining(shardID uint64) bool {
	return h.isShardJoining(shardID)
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
	// TransportDisabled is an exported field, so a caller can set it directly
	// without going through WithoutTransport. Config validation then skips
	// the MTLSConfig requirement, so transport creation must honour the same
	// flag: otherwise the field would silently do nothing but disable the
	// mTLS check, which is exactly backwards.
	transportDisabled := options.disableTransport || cfg.TransportConfig.TransportDisabled

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

	// Ensure the host directories exist AND are durable. A directory entry
	// created by MkdirAll survives a crash only once every directory in the
	// created chain is fsynced; without this a crash can lose the directory a
	// durably-written WAL lives in, which is the identical defect the WAL
	// layer closes one level down for its own subdirectories.
	if err := mkdirAllSynced(cfg.NodeHostDir); err != nil {
		return nil, &HostInitError{Field: "NodeHostDir", Err: err}
	}
	if err := mkdirAllSynced(cfg.WALDir); err != nil {
		return nil, &HostInitError{Field: "WALDir", Err: err}
	}

	// Shared disk full flag set by the WAL's disk monitor callback and
	// read by Host.HealthCheck(). Allocated before the WAL is opened so
	// the callback closure captures the same atomic as the Host struct.
	var diskFullFlag atomic.Bool

	// Per-shard barriers with no host-wide barrier require a Raft log that is
	// already encrypted at rest. Without one, configuring per-shard encryption
	// yields encrypted snapshots over a PLAINTEXT log -- every user command
	// readable on disk, while the operator's spot-check of the snapshot
	// directory says encryption is on. Refuse rather than silently downgrade.
	//
	// The predicate is what the LogDB IS, not whether one was supplied. The
	// default WAL takes ONE barrier and none was configured, so it would be
	// plaintext; a supplied LogDB must declare its at-rest guarantee. Deciding
	// on "did you call WithLogDB" instead accepted an unencrypted
	// waldb.Open(walDir) -- the exact state this refusal exists to prevent,
	// reached by the one-line change an integrator makes to relocate the WAL.
	if options.shardBarriers != nil && options.barrier == nil {
		if err := requireEncryptedLogDB(options.logdb); err != nil {
			return nil, err
		}
	}

	// Create the default WAL-based LogDB when no LogDB was provided.
	// This ensures all production hosts persist raft state to disk.
	if options.logdb == nil {
		walOpts := buildWALOptions(cfg, &diskFullFlag)
		// Wire barrier encryption into the WAL when a barrier is configured.
		if options.barrier != nil {
			walOpts = append(walOpts, waldb.WithBarrier(options.barrier))
			// Per-shard barriers plus the default WAL is legal but weaker
			// than it looks: the log is encrypted under the HOST-WIDE key,
			// so the per-tenant key isolation WithShardBarriers exists to
			// provide covers snapshots only.
			if options.shardBarriers != nil {
				slog.Warn("per-shard barriers do not cover the default WAL; " +
					"log entries are encrypted under the host-wide barrier, not " +
					"per-shard keys. Supply WithLogDB with a multiplexing LogDB " +
					"for per-shard WAL encryption.")
			}
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

	if !transportDisabled {
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
		// is set after engine creation below. shardBarriers, when set, routes
		// snapshot decrypt-for-send to the per-shard barrier; otherwise the
		// single host-wide barrier is used (unchanged behavior).
		sender = &transportSender{
			transport:     tr,
			barrier:       options.barrier,
			shardBarriers: options.shardBarriers,
			logger:        logger,
			fsys:          options.fsys,
		}
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

	// Wire per-shard barriers into the snapshot pool when configured. This is
	// additive: when WithShardBarriers is unset, the snapshot pool keeps using
	// the single host-wide barrier passed above. The adapter bridges the
	// host-level Barrier (a superset interface) to the engine's SnapshotBarrier
	// and preserves a true nil for shards that fall back to the host-wide
	// barrier.
	if options.shardBarriers != nil {
		shardFn := options.shardBarriers
		eng.SetShardBarriers(func(shardID uint64) engine.SnapshotBarrier {
			b := shardFn(shardID)
			if b == nil {
				return nil
			}
			return b
		})
	}

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
	// while snapshot transfers are in progress. With per-shard barriers the
	// tracker is routed by shardID so each received snapshot pins the epoch on
	// the correct tenant barrier; otherwise the single host-wide barrier is
	// used (unchanged behavior).
	if tr != nil {
		if options.shardBarriers != nil {
			shardFn := options.shardBarriers
			tr.SetShardEpochTrackers(func(shardID uint64) transport.EpochTracker {
				b := shardFn(shardID)
				if b == nil {
					if options.barrier != nil {
						return options.barrier
					}
					return nil
				}
				return b
			})
		} else if options.barrier != nil {
			tr.SetEpochTracker(options.barrier)
		}
	}

	h := &Host{
		cfg:           cfg,
		logger:        logger,
		logdb:         options.logdb,
		barrier:       options.barrier,
		shardBarriers: options.shardBarriers,
		metrics:       options.metrics,
		engine:        eng,
		registry:      reg,
		sender:        sender,
		transport:     tr,
		diskFull:      &diskFullFlag,
		id:            hostID,
		keyRotStopC:   make(chan struct{}),
		keyRotDone:    make(chan struct{}),
		sweepStopC:    make(chan struct{}),
		sweepDone:     make(chan struct{}),
		fsys:          options.fsys,
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

	// Wire the per-shard import lock into the snapshot pool. Snapshot recovery
	// reclaims orphaned import staging directories, which are indistinguishable
	// by name from the one a RUNNING import is writing its plaintext state
	// machine into; the lock is the only thing that tells them apart, and the
	// Host owns it.
	eng.SetImportLocker(h.tryAcquireImportLock)

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

	// Start the key rotation loop whenever a barrier is configured. With
	// KeyRotationInterval == 0 scheduled rotation is disabled, but the
	// invocation-threshold check must still run: it is what rotates the
	// epoch when the AEAD invocation budget nears its NIST limit
	// (including the pessimistic post-restart seeding done by Unseal).
	// The goroutine exits when keyRotStopC is closed.
	if options.barrier != nil {
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

// verifyMemberCertificateExclusivity rejects a configuration whose hosts this
// node's own leaf certificate also covers. See
// transport.QUICTransport.VerifyPeerAddressExclusivity for why SAN PRESENCE is
// only an authorization when SANs are EXCLUSIVE.
//
// Peers that share this node's machine are not flagged: a certificate is
// matched on host alone, so they are indistinguishable by construction and no
// certificate policy can separate them (transport.isCoLocatedCertHost defines
// the two forms — the same host under any spelling, and any loopback address).
// With no transport there is no in-band authorization to collapse, so the check
// does not apply.
//
// It is called from StartShard with the initial members and from
// requestConfigChange with the single address a membership add introduces; the
// latter is what covers the grow-by-join flow, whose initial members map is
// empty on every node.
func (h *Host) verifyMemberCertificateExclusivity(members map[uint64]string) error {
	if h.transport == nil || len(members) == 0 {
		return nil
	}
	peers := make([]string, 0, len(members))
	for _, addr := range members {
		if addr != "" && addr != h.cfg.RaftAddress {
			peers = append(peers, addr)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	return h.transport.VerifyPeerAddressExclusivity(h.cfg.RaftAddress, peers)
}

// StartShard starts a Raft shard on this host. members maps replicaID to
// address for initial cluster members. join indicates whether this node
// is joining an existing cluster. create is the state machine factory.
// cfg provides per-shard Raft configuration.
//
// ShardID and ReplicaID are taken from cfg. Both must be greater than zero.
//
// A replica configured as a witness (cfg.IsWitness) or an observer
// (cfg.IsObserver) must NOT appear in members: that map seeds the full voting
// membership, so it is rejected with *NonVotingRoleInMembersError. Bootstrap
// the voters first, then start such a replica with join=true and add it with
// RequestAddWitness / RequestAddObserver.
//
//nolint:gocyclo // shard startup has many validation and configuration branches
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error {
	if h.closed.Load() {
		return ErrClosed
	}
	// Reject new shards once the host is draining: a graceful shutdown is
	// in progress, so the SM Open() work would be wasted and the lifecycle
	// events misleading. Draining is irreversible (see Drain).
	if h.draining.Load() {
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

	// Enforce per-node certificate SANs before the shard exists.
	//
	// The transport's in-band authorization (spoofed-SourceAddress clearing,
	// peerEpoch mutation / live-outbound eviction, snapshot-sender binding) all
	// reduce to "is the claimed host in the sender's verified certificate".
	// That is only an authorization if SANs are exclusive per node; a wildcard
	// or shared cluster certificate makes it true for every peer and lets one
	// compromised replica drive cluster-wide replication stalls.
	//
	// This fails CLOSED. A warning is worthless here: the deployment is
	// silently unauthenticated for every one of those decisions, the operator
	// cannot observe the weakness from the running cluster, and the only
	// remediation is re-issuing certificates — something that must happen
	// before the cluster carries data, not after an incident. Refusing to start
	// is also cheap to fix and impossible to miss.
	if err := h.verifyMemberCertificateExclusivity(members); err != nil {
		return err
	}

	// Resolve this shard's at-rest barrier before anything is written for it.
	//
	// Both failures below are invisible once the shard is running: the log and
	// the snapshots keep being written, just not under the key the operator
	// configured, and every observable signal (the snapshot directory, the
	// absence of errors) says encryption is on. They are also decidable here,
	// where the shardID that selects the barrier first exists, and nothing has
	// been persisted yet.
	//
	//  1. The resolver returned no barrier and there is no host-wide fallback:
	//     this tenant's shard would be written in plaintext alone.
	//  2. The LogDB holds this shard's records under some OTHER barrier: the
	//     shard is encrypted, under the wrong tenant's key.
	if h.shardBarriers != nil {
		shardBarrier, barrierErr := h.barrierForShard(cfg.ShardID)
		if barrierErr != nil {
			return barrierErr
		}
		if err := h.verifyShardLogDBBarrier(cfg.ShardID, shardBarrier); err != nil {
			return err
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
	hasPersistedState, err := h.recoverLogReader(lr, cfg.ShardID, cfg.ReplicaID)
	if err != nil {
		return &HostInitError{Field: "log_recovery", Err: err}
	}

	// Initialize the LogReader's membership with the bootstrap members
	// so the Raft state machine knows about all cluster members for
	// quorum calculations. Without this, each node would think it's a
	// single-node cluster and immediately self-elect. On recovery the
	// membership was already restored from the persisted snapshot above,
	// but applying the bootstrap config on top is safe because the Raft
	// layer reconciles membership from committed config-change entries.
	//
	// For joining nodes (join=true) we also set the membership so that
	// quorum requires more than just the joining node itself. Without this,
	// a joining node with empty membership trivially wins its own election
	// (0 quorum needed) before the existing leader can contact it. Setting
	// the known members here prevents self-election: the joining node needs
	// votes from the existing members, which it cannot get in term 0/1
	// since those members are already in a higher term. The leader's first
	// AppendEntries will update the joining node's term and make it a
	// follower. The membership is later updated to the full cluster config
	// when a snapshot is applied (handleSnapshot).
	//
	// CRITICAL: only seed when recovery restored NOTHING. Membership is
	// replicated state; the recovered snapshot is authoritative and must
	// never be overwritten by the caller's bootstrap map. Config changes
	// at or below the snapshot index are compacted out of the log, and an
	// on-disk state machine's processed marker is seeded to its own
	// lastApplied (see SetProcessedIndex below), so commit-time replay
	// cannot reconstruct them. Overwriting here would drop witnesses,
	// observers and Removed tombstones and silently revert the REPLICATED
	// witness policy to this node's local config — a restarted replica
	// would then compute quorum over a smaller voting set than its peers
	// and two leaders could be elected in the same term.
	_, recovered := lr.NodeState()
	nothingRecovered := recovered.ConfigChangeID == 0 &&
		len(recovered.Addresses) == 0 &&
		len(recovered.Observers) == 0 &&
		len(recovered.Witnesses) == 0

	// A witness or an observer must never be seeded through the members map —
	// but ONLY when this call is actually BOOTSTRAPPING the shard.
	//
	// members seeds proto.Membership.Addresses — the FULL VOTER set — and
	// membership is REPLICATED state: every peer seeds the same replica from
	// its own copy of the map, so this replica cannot hold a different role for
	// itself than its peers hold for it. The local Config.IsWitness /
	// Config.IsObserver bit would then contradict the configuration the whole
	// shard agrees on: the leader sends this replica FULL entry payloads
	// (sendAppend, not sendWitnessAppend), it counts as a full replica for the
	// RequireFullReplicaMajority policy and toward every quorum, and the only
	// witness-like behaviour left is this host's local read gate refusing the
	// reads the operator asked for. raft.checkSelfRole latches that
	// disagreement permanently for a witness.
	//
	// Fail closed. Seeding the local node into Membership.Witnesses instead
	// would be worse — a replica-local membership that no peer shares — and
	// suppressing the latch would hide a real misconfiguration. The supported
	// flow is two-step (as for etcd/dragonboat learners): bootstrap the voters,
	// then start this replica with join=true and add it with RequestAddWitness
	// / RequestAddObserver so the role enters membership through a committed
	// config change.
	//
	// # Why this is gated on firstStart
	//
	// The refusal must apply to a BOOTSTRAP and to nothing else. Refusing on a
	// restart left a legitimately-added witness with NO working restart:
	// join=false with itself listed hit this error, join=false without itself
	// hit SelfNotInMembersError, and the only form that returned nil — join=true
	// — set the joining marker, which is cleared in exactly ONE place (the
	// InstallSnapshot adopt branch in raft). A caught-up witness receives
	// Replicate and never InstallSnapshot, so the marker became PERMANENT for
	// the process: Peer.IsMember reports true for every replica ID (per-shard
	// message authorization off), the handler skips SourceAddress binding, and
	// Peer.Tick returns early — no election timer and no CheckQuorum. The gate
	// meant to prevent a misconfiguration was itself producing a far worse one.
	//
	// The discriminator is PERSISTED STATE, not recovered membership. Membership
	// is written only alongside a snapshot record (both LogDB backends set it
	// inside the u.Snapshot.Index > 0 branch), so a caught-up but never
	// snapshotted witness — the common case, since a witness stores no entry
	// payloads and a young shard may never reach SnapshotEntries — recovers no
	// membership and looked exactly like a bootstrap. It hit the refusal, and
	// with it every one of the outcomes above.
	//
	// A restart whose membership WAS recovered is additionally protected by the
	// seeding gate below: the map cannot make this replica a voter because the
	// map is not applied at all. A restart WITHOUT a snapshot has no such
	// protection — the map is still the only membership it has — so the seed
	// does place this replica, and every other non-voter the operator listed,
	// in Addresses. That placement is PROVISIONAL and is repaired by the
	// committed AddWitness / AddNonVoting entries, which are necessarily still
	// in the log (config changes are compacted only behind a snapshot, and a
	// snapshot would have restored the membership instead) and are re-applied
	// at commit time on restart.
	//
	// The repair is the business of raft.membership.apply, which CONVERGES on
	// the role each committed entry names instead of refusing an entry whose
	// target is already a member — see the "Convergent application" comment in
	// internal/raft/membership.go. Nothing here may rely on checkSelfRole for
	// it: that compares only the WITNESS bit, so it does not see an observer
	// seeded as a voter at all, and it is a latch, not a repair — it would
	// leave the membership itself wrong on every replica that restarted this
	// way, and a snapshot taken afterwards would make it durable.
	//
	// Refusing to start instead is not an option: it has no remedy at all.
	firstStart := !hasPersistedState
	if firstStart && (cfg.IsWitness || cfg.IsObserver) {
		if _, listed := members[cfg.ReplicaID]; listed {
			role := "observer"
			if cfg.IsWitness {
				role = "witness"
			}
			return &NonVotingRoleInMembersError{
				ShardID:   cfg.ShardID,
				ReplicaID: cfg.ReplicaID,
				Role:      role,
			}
		}
	}

	if nothingRecovered && (!join || len(nodesCopy) > 0) {
		// Seed the replicated witness policy from this node's config. This
		// is the ONLY point where config.WitnessConfig enters the shard:
		// from here on the policy lives in replicated membership state
		// (snapshot + log) and changes only via UpdateWitnessPolicy config
		// changes, so every replica enforces the same policy regardless of
		// local configuration.
		lr.SetMembership(proto.Membership{
			Addresses:                  nodesCopy,
			MaxWitnesses:               uint64(max(0, cfg.Witness.MaxWitnesses)),
			RequireFullReplicaMajority: cfg.Witness.GetRequireFullReplicaMajority(),
		})
	}

	// Address routing must follow the EFFECTIVE membership, not the
	// bootstrap map: after a restart the recovered membership may contain
	// observers and witnesses the caller never listed, and the transport
	// cannot resolve a replica it has no address for.
	effectiveNodes := nodesCopy
	if !nothingRecovered {
		_, m := lr.NodeState()
		effectiveNodes = make(map[uint64]string,
			len(m.Addresses)+len(m.Observers)+len(m.Witnesses))
		for _, group := range []map[uint64]string{m.Addresses, m.Observers, m.Witnesses} {
			for id, addr := range group {
				effectiveNodes[id] = addr
			}
		}
		// A member recorded without an address (possible for entries that
		// predate address tracking) falls back to the caller's map.
		for id, addr := range nodesCopy {
			if _, ok := effectiveNodes[id]; !ok {
				effectiveNodes[id] = addr
			}
		}
	}

	// Create the Raft peer. NewPeer validates the config and
	// initializes the raft state machine as a follower.
	peer, err := raft.NewPeer(cfg, lr, h.engine.GlobalRateLimiter())
	if err != nil {
		return &HostInitError{Field: "peer", Err: err}
	}
	// A joining node starts with incomplete membership. Mark it so
	// per-shard message auth is bypassed until a snapshot delivers
	// the full cluster membership. The Host keeps its own marker: the
	// snapshot receive path needs it to authorize the bootstrap snapshot
	// against the sender's certificate instead of an address registry that
	// is necessarily empty here.
	if join {
		peer.MarkJoining()
		h.markShardJoining(cfg.ShardID)
	}

	// Create the managed state machine (RSM) from the factory.
	//
	// The on-disk state machine opens its persistent store at snapshotDir and
	// the engine stages snapshot artifacts beneath it. By default this roots
	// under the host NodeHostDir, preserving the historical layout. When the
	// shard supplies an explicit cfg.DataDir, root the shard's on-disk
	// state-machine data there instead so a per-tenant shard can live on the
	// tenant's own volume (dragonboat's "the on-disk SM owns its storage
	// location" behavior). The Raft log/WAL is unaffected: it always remains
	// in the shared host-wide LogDB.
	snapshotDir := shardSnapshotBaseDir(h.cfg.NodeHostDir, cfg.DataDir, cfg.ShardID)

	// Reclaim decrypt scratch files orphaned by a crash BEFORE anything else
	// touches the shard's snapshot tree. A leader killed mid-snapshot-send (or
	// mid-recovery) leaves a COMPLETE PLAINTEXT COPY of the state machine beside
	// the ciphertext; the snapshot directory is retained indefinitely, so without
	// a sweep here the plaintext survives restarts and backups while snapshot.dat
	// next to it still reads as ciphertext. This runs on EVERY start, including
	// the on-disk-state-machine path below that skips snapshot recovery entirely
	// and the case where no snapshot record exists at all.
	engine.SweepStaleDecryptTmpTree(snapshotDir, cfg.ShardID, cfg.ReplicaID)

	// Reclaim an import staging directory orphaned by a crash. ImportSnapshot
	// writes the artifact PLAINTEXT and re-encrypts it immediately afterwards;
	// a process killed between the two leaves the complete plaintext state
	// machine under a name NOTHING else on the host enumerates (that invisibility
	// is what keeps a half-written import out of snapshot GC and the DEK
	// purge-floor scan), so without this sweep the exposure is permanent.
	//
	// A live staging directory is indistinguishable from an orphaned one by
	// name, so the sweep runs only when this call can prove no import is in
	// flight for the shard: the per-shard import lock. Taking it is a TRY —
	// StartShard must not block behind a multi-gigabyte restore, and an import
	// that is running owns its staging directory and removes it itself on
	// failure.
	if release, ok := h.tryAcquireImportLock(cfg.ShardID); ok {
		engine.SweepStaleImportStageDirs(snapshotDir, cfg.ShardID, cfg.ReplicaID)
		release()
	}

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

	// Open acquires persistent resources (e.g. a Pebble file lock for an on-disk
	// SM). Close them on EVERY failure path until the node is successfully
	// published into the engine, so a failed StartShard does not leak the open
	// store and brick subsequent retries on the same DataDir with "lock held by
	// current process". Once published (smPublished=true, set after LoadNode),
	// the node's lifecycle — including rsm.Close — is owned by StopShard/Close,
	// and every error path between publication and LoadNode removes the
	// nodeState first, so this never double-closes.
	smPublished := false
	defer func() {
		if !smPublished {
			if cerr := stateMachine.Close(); cerr != nil {
				h.logger.Warn("state machine close failed during StartShard rollback",
					"shard_id", cfg.ShardID, "error", cerr)
			}
		}
	}()

	// recoveredIndex is the highest log index whose effects are durably
	// reflected in the state machine after Open()/recovery. It is used to
	// seed the applied markers below so reads can be served immediately on
	// restart without waiting for a new write. For a fresh start it is 0.
	// recoveredTerm is the Raft term of recoveredIndex (from the recovered
	// snapshot record); it seeds lastAppliedTerm so a snapshot taken right
	// after recovery — before any apply — records a consistent (index, term).
	var recoveredIndex uint64
	var recoveredTerm uint64

	// Recover SM state from a persisted snapshot on restart.
	//
	// There are three cases, distinguished by SM kind and Open()'s result:
	//
	//  1. On-disk SM whose Open() returns lastApplied > 0. Its database is
	//     authoritative and already reflects every applied entry. The local
	//     LogDB snapshot record sits at an OLDER index (an on-disk SM keeps
	//     applying past its last snapshot), so calling RecoverFromSnapshot
	//     would REPLACE its database files and roll it back to the snapshot
	//     index — permanent data loss for entries between the snapshot and
	//     lastApplied. Open() is the sole source of recovered state.
	//
	//  2. On-disk SM whose Open() returns 0 AND a local LogDB snapshot record
	//     exists (ss.Index > 0 && ss.Filepath != ""). This is the
	//     import/bootstrap case: ImportSnapshot wrote snapshot.dat plus a
	//     LogDB snapshot record, expecting StartShard to reconstruct the
	//     on-disk database from it. The SM came up empty (no DB files), so it
	//     is safe — and required — to call RecoverFromSnapshot exactly once.
	//     Without this the SM stays empty while raft believes a snapshot
	//     exists at ss.Index: cross-host restore data loss.
	//
	//  3. In-memory SM. It always starts empty and must have its snapshot
	//     data restored. The raft processed index is initialized to the
	//     snapshot index (in newRaftLog), so entries after the snapshot are
	//     re-applied by the engine.
	//
	// Cases 2 and 3 share the same recovery mechanics, so they are handled by
	// the same recoverFromLocalSnapshot path below. Only case 1 skips it.
	onDiskRecovered := stateMachine.IsOnDisk() && lastApplied > 0
	if onDiskRecovered {
		// On-disk SM with durable state: Open() is the sole source.
		recoveredIndex = lastApplied
	} else if h.logdb != nil {
		ss, ssErr := h.logdb.GetSnapshot(cfg.ShardID, cfg.ReplicaID)
		if ssErr == nil && ss.Index > 0 && ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")

			// snapshot.dat is stored ENCRYPTED whenever the record carries a
			// non-zero epoch, so it must be decrypted before the state machine
			// sees it — feeding it the raw frames either fails recovery or
			// installs corrupt state. Decryption goes to a throwaway sibling
			// file, removed on every path below, leaving the canonical
			// encrypted artifact untouched; this mirrors the runtime path
			// (snapshotPool.recoverSnapshot) exactly.
			//
			// The barrier is resolved PER SHARD: with WithShardBarriers the
			// snapshot was encrypted under the shard's own key, and the
			// host-wide barrier would be the wrong key.
			//
			// An encrypted snapshot with no usable barrier fails closed. A node
			// that cannot decrypt its own snapshot must say so at startup
			// rather than recover a state machine from ciphertext.
			recoverPath := dataPath
			if ss.Epoch > 0 {
				barrier, barrierErr := h.barrierForShard(cfg.ShardID)
				if barrierErr != nil {
					return &HostInitError{Field: "snapshot_decrypt", Err: barrierErr}
				}
				if barrier == nil || barrier.IsSealed() {
					return &HostInitError{
						Field: "snapshot_decrypt",
						Err:   ErrSnapshotEncryptedBarrierUnavailable,
					}
				}
				plainPath, _, decErr := decryptSnapshotToTemp(
					h.snapshotFsys(), dataPath, recoverDecryptTempPattern, ss.Epoch, barrier,
				)
				if decErr != nil {
					return &HostInitError{Field: "snapshot_decrypt", Err: decErr}
				}
				defer func() {
					if rmErr := h.snapshotFsys().Remove(plainPath); rmErr != nil && !os.IsNotExist(rmErr) {
						h.logger.Debug("failed to remove decrypted snapshot scratch file",
							"shard_id", cfg.ShardID,
							"replica_id", cfg.ReplicaID,
							"path", plainPath,
							"error", rmErr)
					}
				}()
				recoverPath = plainPath
			}

			f, openErr := h.snapshotFsys().Open(recoverPath)
			if openErr != nil {
				// A snapshot record exists in LogDB but its data file is
				// missing or unreadable. We must NOT silently continue: the
				// raft layer initializes the processed index to the snapshot
				// index, so skipping recovery would bring the state machine up
				// empty and it would never replay the pre-snapshot entries
				// (they are compacted away). This is fatal for BOTH the
				// in-memory path and the on-disk bootstrap path. Fail StartShard.
				return &HostInitError{Field: "snapshot_open", Err: openErr}
			}
			// The reader chain must mirror the chain the ARTIFACT was written
			// through, which the snapshot record carries. Deriving it from
			// cfg instead reads the artifact wrong whenever the setting has
			// changed since the save, or whenever the snapshot was streamed
			// by a leader configured differently. Decompression sits INSIDE
			// decryption: the save path compresses first and encrypts the
			// resulting file, so recovery decrypts first (above) and
			// decompresses here.
			recoverReader, readerErr := engine.DecompressingSnapshotReader(f, ss.Compression)
			if readerErr != nil {
				if closeErr := f.Close(); closeErr != nil {
					h.logger.Debug("snapshot file close failed after reader build error",
						"shard_id", cfg.ShardID,
						"replica_id", cfg.ReplicaID,
						"error", closeErr)
				}
				return &HostInitError{Field: "snapshot_compression", Err: readerErr}
			}
			stopper := make(chan struct{})
			recoverErr := stateMachine.RecoverFromSnapshot(
				context.Background(), recoverReader, stopper,
			)
			if closeErr := f.Close(); closeErr != nil {
				return &HostInitError{Field: "snapshot_close", Err: closeErr}
			}
			if recoverErr != nil {
				return &HostInitError{Field: "snapshot_recovery", Err: recoverErr}
			}
			// Seed to the snapshot's RAFT LOG INDEX (ss.Index), not its
			// OnDiskIndex. recoveredIndex is the raft processed marker (and
			// the host/engine applied markers below), all of which live in the
			// raft log-index namespace. newRaftLog already initializes
			// processed = ss.Index, and the runtime recovery path
			// (snapshotPool.recoverSnapshot) sets lastApplied/lastSnapshotIndex
			// to logdbSnap.Index — so ss.Index keeps every marker consistent
			// and the engine replays exactly the entries after the snapshot.
			// OnDiskIndex is the SM's own internal applied counter, which
			// RecoverFromSnapshot reconstructs inside the SM; it is not a raft
			// log index and must not seed these markers.
			recoveredIndex = ss.Index
			recoveredTerm = ss.Term
		}
	}

	// For on-disk SMs that recovered durable state via Open(), set the raft
	// peer's processed marker to lastApplied so the engine does not wastefully
	// re-apply entries the SM has already persisted. For the on-disk bootstrap
	// path (case 2) and the in-memory path (case 3), newRaftLog already
	// initialized processed to ss.Index, which equals recoveredIndex; calling
	// SetProcessedIndex(recoveredIndex) there is a consistent no-op-equivalent
	// that keeps the marker explicit. A fresh start leaves processed at 0.
	if recoveredIndex > 0 {
		// Adopt the value raft actually applied. It clamps to the recovered
		// commit index, and the engine markers below MUST agree with it —
		// seeding lastApplied above raft's processed makes the first Commit
		// trip commitUpdate's assertion and take the commit worker down.
		recoveredIndex = peer.SetProcessedIndex(recoveredIndex)
	}

	ns := &nodeState{
		shardID:          cfg.ShardID,
		replicaID:        cfg.ReplicaID,
		cfg:              cfg,
		createFn:         create,
		smType:           stateMachine.SMType().TypeCode(),
		nodes:            effectiveNodes,
		requestPool:      NewRequestStatePool(),
		rsm:              stateMachine,
		openStopper:      openStopper,
		pendingReads:     make(map[uint64]*pendingRead),
		pendingSnapshots: make(map[uint64]*RequestState),
	}
	ns.health.Store(uint32(ShardHealthy))

	// Seed the host-side applied marker to the recovered index so that
	// FollowerRead and the OnReadyToRead/OnApplied read-completion gates
	// see lastApplied == raft's processed marker immediately on restart.
	// Without this, a freshly-restarted node with no new writes leaves
	// ns.lastApplied at 0 (OnApplied never fires when there are no entries
	// above processed), so reads poll to their deadline. Stored before the
	// nodeState is published via LoadOrStore so it is visible before any
	// read is served. Harmless when recoveredIndex == 0.
	if recoveredIndex > 0 {
		ns.lastApplied.Store(recoveredIndex)
	}

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
			// Record where this shard's on-disk tree is rooted. Operations
			// that run while the shard is STOPPED — Host.ImportSnapshot — have
			// no other way to discover it, and writing an artifact outside the
			// tree costs the artifact's DEK at the next purge.
			DataDir: cfg.DataDir,
		}
		if err := h.logdb.SaveBootstrap(cfg.ShardID, cfg.ReplicaID, bootstrap); err != nil {
			// Mark the nodeState stopped BEFORE removing it from h.nodes
			// (matching StopShard/Close ordering): a reader that resolved ns
			// just before the Delete must fail its stopped check rather than
			// call into the state machine the deferred rollback is about to
			// close.
			ns.stopped.Store(true)
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
	// Seed the engine node's applied/snapshot markers to the recovered
	// index so they match raft's processed marker before the node enters
	// the pipeline. Mirrors the runtime recover path in snapshot_pool.go,
	// which sets node.lastApplied/lastSnapshotIndex after RecoverFromSnapshot.
	engNode.SeedLastApplied(recoveredIndex, recoveredTerm)
	h.engine.LoadNode(engNode)
	// The node is now live in the engine; StopShard/Close own its rsm.Close from
	// here, so the StartShard rollback close must not also run.
	smPublished = true

	// Close-vs-Start TOCTOU guard. The entry check above can be passed
	// microseconds before Host.Close sets closed and runs its h.nodes sweep,
	// and Close stops the engine BEFORE that sweep. A shard published into
	// that window is invisible to the sweep and permanently inert: it sits in
	// h.nodes with activeNodes incremented, inside a stopped engine, holding
	// an open state machine nobody will ever close — whose file lock bricks
	// the next host start on the same DataDir. Because both flags are set
	// before the sweep reads h.nodes, a sweep that missed this shard is
	// guaranteed to be visible here (the same reasoning proposeEntry,
	// ReadIndex, RequestSnapshot and requestConfigChange rely on). Losing the
	// ns.stopped CAS means a concurrent Close/StopShard claimed teardown and
	// owns it; this call must not touch the shard again.
	if h.closed.Load() || h.draining.Load() {
		if ns.stopped.CompareAndSwap(false, true) {
			h.teardownShard(ns, cfg.ShardID, ErrClosed)
		}
		h.logger.Warn("shard start aborted: host is closing",
			"shard_id", cfg.ShardID, "replica_id", cfg.ReplicaID)
		return ErrClosed
	}

	// Pre-bind metrics for this shard to avoid expensive label lookups
	// in hot paths. This must be called before any metrics are recorded.
	if h.metrics != nil {
		h.metrics.RegisterShard(cfg.ShardID)
	}

	// Register all members in the registry for address resolution.
	for replicaID, addr := range effectiveNodes {
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

	// Atomically claim teardown: only the goroutine that flips stopped from
	// false to true performs UnloadNode/rsm.Close/Delete and decrements
	// activeNodes. Without this, two concurrent StopShard calls (or a StopShard
	// racing Close) for the same shard both pass the existence check above and
	// both decrement activeNodes — driving it negative. The counter is later
	// used as a slice capacity (GetNodeHostInfo, HealthCheck), where a negative
	// value panics with "makeslice: cap out of range", and the shard would be
	// double-unloaded and its state machine closed twice (e.g. a Pebble
	// close-after-close).
	if !ns.stopped.CompareAndSwap(false, true) {
		return ErrShardNotFound // another StopShard/Close already owns teardown
	}
	h.teardownShard(ns, shardID, ErrAborted)

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

// teardownShard releases every resource a loaded shard holds. The caller
// MUST have claimed teardown by winning ns.stopped's CompareAndSwap, which
// makes this run exactly once per shard incarnation: StopShard, Host.Close,
// and the StartShard close-race rollback all funnel through it, so none of
// them can double-unload a node, double-close a state machine, or drive
// activeNodes negative.
//
// pendingErr is the error delivered to the shard's still-pending proposals,
// reads, and snapshots.
func (h *Host) teardownShard(ns *nodeState, shardID uint64, pendingErr error) {
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
	// from these maps, so only truly orphaned items receive the error.
	h.failPendingRequests(ns, pendingErr)

	// Remove nodeState last, after the engine is fully unloaded and all
	// pending items have been resolved. No more callbacks will reference
	// this shardID.
	h.nodes.Delete(shardID)
	h.activeNodes.Add(-1)

	// Close the state machine to release any resources it holds (e.g.,
	// Pebble file locks). Without this a stopped shard leaves its
	// DiskStateMachine open and a subsequent StartShard on the same data
	// directory fails with "lock held by current process".
	if ns.rsm != nil {
		if err := ns.rsm.Close(); err != nil {
			h.logger.Warn("state machine close failed during shard teardown",
				"shard_id", shardID, "error", err)
		}
		// Do NOT nil ns.rsm: a concurrent reader (SyncRead/StaleRead/etc.)
		// may still hold this nodeState and read the field. The node is
		// removed from h.nodes and ns.stopped is set, so no new reader can
		// reach it and existing readers gate on ns.stopped. Writing the
		// field here would be a data race (mirrors Host.Close).
	}

	// Remove all registered members for this shard from the registry.
	ns.nodesMu.RLock()
	for replicaID := range ns.nodes {
		h.registry.Remove(shardID, replicaID)
	}
	ns.nodesMu.RUnlock()
	// The join bootstrap window belongs to the loaded shard; a stopped shard
	// must not leave a relaxed snapshot authorization behind.
	h.clearShardJoining(shardID)

	// Unregister pre-bound metrics for this shard.
	if h.metrics != nil {
		h.metrics.UnregisterShard(shardID)
	}
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
	// Bail if the shard is stopping/stopped: its state machine may be closing
	// (Host.Close sets stopped before closing the SM). This avoids issuing a
	// Lookup against a state machine that is being torn down.
	if ns.stopped.Load() {
		return nil, ErrClosed
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
	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, nopHostRelease, ErrClosed
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
	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, nopHostRelease, ErrClosed
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

	// An empty command is legal and is proposed normally, matching
	// dragonboat (proposalShard.propose types a zero-length cmd as a plain
	// application entry). It commits and advances lastApplied, but the
	// apply path skips empty-Cmd non-session entries before they reach the
	// user state machine — the same treatment the leader's election no-op
	// gets. That makes it a commit barrier: the way to force a
	// current-term committed entry after an election (PhD 6.4 ReadIndex
	// warmup) without inventing a throwaway payload.

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
	rs := newRequestState(ns.requestPool, proposalKey, deadline, h.metrics != nil)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the proposal in the pending map BEFORE delivering the
	// message to the inbox. This ensures that if the step worker
	// processes and applies the entry extremely fast, the OnApplied
	// callback will find the RS. acquireRef pins the RS so it is not
	// recycled to the pool until this map entry is removed, even if the
	// caller releases first (e.g., Sync* timeout).
	rs.acquireRef()
	ns.pendingProposals.Store(proposalKey, &pendingProposal{rs: rs, deadline: deadline})

	// Close-vs-Propose TOCTOU guard: the host may have begun closing (and run
	// its pending-request sweeps) between the top-of-Propose closed check and
	// this Store, which would orphan the RS in a deleted nodeState's map — a
	// deadline-less caller would then block on rs.Result() forever. Re-check
	// after registering and roll back if shutting down. LoadAndDelete makes the
	// rollback exactly-once relative to a concurrent Close sweep.
	if h.closed.Load() || ns.stopped.Load() {
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		// The caller's own reference is dropped whether or not the claim
		// won: a sweep that beat us to the entry drops only ITS reference,
		// so leaving Release() inside the claim would strand the object
		// outside the pool.
		rs.Release()
		return nil, ErrClosed
	}

	// Deliver the proposal to the MPSC proposal queue. The step worker
	// is the only goroutine that touches the Raft state machine, so
	// proposals must be routed through the queue to respect the
	// single-owner invariant on the Peer. Using the dedicated proposal
	// queue avoids channel serialization under high concurrent load.
	if !engNode.DeliverProposal([]proto.Entry{entry}) {
		// Queue full — remove the RS and let the caller retry. The entry
		// must be CLAIMED with LoadAndDelete: the sweep and
		// failPendingRequests also remove entries with claims, and an
		// unconditional Delete pairing with a sweep that already completed
		// this key would double releaseRef (driving outstanding negative —
		// the RS is permanently lost from the pool) and double
		// completePendingOp (undercounting pendingOps so Drain can close
		// early). Only the claimant rolls back the map reference;
		// rs.Release() is the caller's own reference and is dropped
		// unconditionally.
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
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
	rs := newRequestState(ns.requestPool, readKey, deadline, h.metrics != nil)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the pending read BEFORE delivering the message to avoid
	// a race where OnReadyToRead fires before the map entry exists.
	// acquireRef pins the RS until this map entry is removed.
	rs.acquireRef()
	ns.pendingReadsMu.Lock()
	if ns.pendingReads == nil {
		ns.pendingReads = make(map[uint64]*pendingRead)
	}
	ns.pendingReads[readKey] = &pendingRead{rs: rs}
	ns.pendingReadsMu.Unlock()

	// Close-vs-ReadIndex TOCTOU guard: the host may have begun closing (and
	// run its pending-request sweeps) between the top-of-ReadIndex closed
	// check and the store above, which would orphan the RS in a deleted
	// nodeState's map — a deadline-less caller would then block on
	// rs.Result() forever. Re-check after registering and roll back if
	// shutting down. The delete-under-mutex claim makes the rollback
	// exactly-once relative to a concurrent Close sweep (mirrors
	// proposeEntry's LoadAndDelete pattern).
	if h.closed.Load() || ns.stopped.Load() {
		ns.pendingReadsMu.Lock()
		_, present := ns.pendingReads[readKey]
		if present {
			delete(ns.pendingReads, readKey)
		}
		ns.pendingReadsMu.Unlock()
		if present {
			rs.releaseRef()
			h.completePendingOp()
		}
		// The caller's own reference is dropped whether or not the claim
		// won; a sweep that beat us drops only its own.
		rs.Release()
		return nil, ErrClosed
	}

	// Deliver the ReadIndex key to the MPSC ReadIndex queue. The step
	// worker drains all accumulated keys and submits a single batched
	// ReadIndex operation, coalescing N concurrent callers into 1
	// heartbeat quorum round. This is the primary performance
	// optimization for multi-node ReadIndex workloads.
	if !engNode.DeliverReadIndex(readKey) {
		// Claim the entry before rolling back. failPendingRequests and
		// sweepPendingReads remove entries under the same mutex; an
		// unconditional delete here could pair with a concurrent completion
		// that already removed and released this entry, double-counting
		// releaseRef (driving outstanding negative, so tryRecycle's
		// outstanding==0 check never holds again and the object leaks out of
		// the pool) and completePendingOp (undercounting pendingOps so Drain
		// can close drainC with work still in flight). rs.Release() is the
		// caller's own reference and is dropped unconditionally.
		ns.pendingReadsMu.Lock()
		_, claimed := ns.pendingReads[readKey]
		if claimed {
			delete(ns.pendingReads, readKey)
		}
		ns.pendingReadsMu.Unlock()
		if claimed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
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

	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, ErrClosed
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

	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, ErrClosed
	}
	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}

	return ns.rsm.Lookup(ctx, query)
}

// FollowerRead reads from the local state machine after ensuring the local
// applied index has caught up to the committed index. This allows any node
// (leader or follower) to serve reads without a ReadIndex round-trip,
// providing a consistency guarantee stronger than StaleRead: the result
// reflects all entries committed at the time the read was initiated.
//
// The read waits until lastApplied >= committedIndex. If the node is
// significantly behind, this may block until the SM catches up or the
// context expires.
//
// Compared to SyncRead, FollowerRead does not confirm leadership via a
// heartbeat quorum, so it may return slightly stale data during a network
// partition (at most the data that was committed before the read). For
// strict linearizability, use SyncRead.
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}

	ns, err := h.getNode(shardID)
	if err != nil {
		return nil, err
	}

	// Witnesses vote but don't store log data; they cannot serve reads.
	if ns.cfg.IsWitness {
		return nil, ErrInvalidOperation
	}

	// Shard is stopping/stopped: its state machine may be closing.
	if ns.stopped.Load() {
		return nil, ErrClosed
	}
	if ns.rsm == nil {
		return nil, ErrShardNotReady
	}

	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return nil, ErrShardNotReady
	}

	committed := engNode.CommittedIndex()

	// Fast path: SM has already applied up to the committed index.
	if ns.lastApplied.Load() >= committed {
		return ns.rsm.Lookup(ctx, query)
	}

	// Slow path: poll until lastApplied catches up, the shard or host shuts
	// down, or the context expires. Shutdown must terminate the loop on its
	// own: a caller passing a deadline-less context on a partitioned follower
	// would otherwise spin at 1000Hz forever and Host.Close could not
	// interrupt it. The interval backs off after the initial burst so a long
	// catch-up costs a handful of wakeups per second instead of a thousand.
	const (
		followerReadPollInterval = time.Millisecond
		followerReadFastPolls    = 50
		followerReadSlowInterval = 20 * time.Millisecond
	)
	ticker := time.NewTicker(followerReadPollInterval)
	defer ticker.Stop()
	polls := 0
	for {
		select {
		case <-ticker.C:
			if ns.lastApplied.Load() >= committed {
				return ns.rsm.Lookup(ctx, query)
			}
			if ns.stopped.Load() || h.closed.Load() {
				return nil, ErrClosed
			}
			polls++
			if polls == followerReadFastPolls {
				ticker.Reset(followerReadSlowInterval)
			}
		case <-ctx.Done():
			return nil, mapContextError(ctx.Err())
		}
	}
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

	// Wait for the registration to be committed and applied, respecting
	// ctx cancellation/deadline so a cancelable-but-deadline-less context
	// does not block forever on quorum loss.
	result, err := waitResultCtx(ctx, rs)
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

// waitResultCtx waits for a RequestState to complete while honoring the
// caller's context. It bridges the gap for request states that carry no
// deadline of their own: without this, RequestState.Result blocks
// indefinitely and a cancelable-but-deadline-less context (common for
// session register/unregister) cannot unblock a quorum-loss stall.
func waitResultCtx(ctx context.Context, rs *RequestState) (RequestResult, error) {
	select {
	case result := <-rs.ResultC():
		return result, result.Err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return RequestResult{}, ErrTimeout
		}
		return RequestResult{}, ErrCanceled
	}
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

	// Wait for the registration to be committed and applied, respecting
	// ctx cancellation/deadline (see waitResultCtx).
	result, err := waitResultCtx(ctx, rs)
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
		ConfigChangeID:             pm.ConfigChangeID,
		MaxWitnesses:               pm.MaxWitnesses,
		RequireFullReplicaMajority: pm.RequireFullReplicaMajority,
		Nodes:                      make(map[uint64]string, len(pm.Addresses)),
		Observers:                  make(map[uint64]string, len(pm.Observers)),
		Witnesses:                  make(map[uint64]string, len(pm.Witnesses)),
		Removed:                    make(map[uint64]struct{}, len(pm.Removed)),
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

	// Pre-validate the transfer target. The raft layer validates too, but
	// its error surfaces asynchronously in the step worker where it can
	// only be logged — returning nil here for an invalid target would
	// falsely report success to the caller.
	if targetReplicaID == 0 {
		return &InvalidMemberError{
			ShardID:   shardID,
			ReplicaID: targetReplicaID,
			Reason:    "leader transfer target must be non-zero",
		}
	}
	if targetReplicaID == ns.replicaID {
		// Transfer to self: the target already holds leadership, so the
		// requested end state is already true. Idempotent no-op success.
		return nil
	}
	membership := engNode.Peer().GetMembership()
	if _, isVoter := membership.Addresses[targetReplicaID]; !isVoter {
		if _, removed := membership.Removed[targetReplicaID]; removed {
			return &InvalidMemberError{
				ShardID:   shardID,
				ReplicaID: targetReplicaID,
				Reason:    "leader transfer target was removed from the shard",
			}
		}
		if _, isObserver := membership.Observers[targetReplicaID]; isObserver {
			return &InvalidMemberError{
				ShardID:   shardID,
				ReplicaID: targetReplicaID,
				Reason:    "leader transfer target is a non-voting observer",
			}
		}
		if _, isWitness := membership.Witnesses[targetReplicaID]; isWitness {
			return &InvalidMemberError{
				ShardID:   shardID,
				ReplicaID: targetReplicaID,
				Reason:    "leader transfer target is a witness",
			}
		}
		return &ReplicaNotFoundError{ShardID: shardID, ReplicaID: targetReplicaID}
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
	if !engNode.DeliverLocal(msg) {
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
	rs := newRequestState(ns.requestPool, snapshotKey, deadline, h.metrics != nil)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register the pending snapshot before submitting to avoid a race
	// where OnSnapshotCompleted fires before the map entry exists.
	// acquireRef pins the RS until this map entry is removed.
	rs.acquireRef()
	ns.pendingSnapshotsMu.Lock()
	ns.pendingSnapshots[snapshotKey] = rs
	ns.pendingSnapshotsMu.Unlock()

	// Close-vs-RequestSnapshot TOCTOU guard: the host may have begun closing
	// (and run its pending-request sweeps) between the top-of-method closed
	// check and the store above, which would orphan the RS in a deleted
	// nodeState's map — a deadline-less caller would then block on
	// rs.Result() forever. Re-check after registering and roll back if
	// shutting down. The delete-under-mutex claim makes the rollback
	// exactly-once relative to a concurrent Close sweep (mirrors
	// proposeEntry's LoadAndDelete pattern).
	if h.closed.Load() || ns.stopped.Load() {
		ns.pendingSnapshotsMu.Lock()
		_, present := ns.pendingSnapshots[snapshotKey]
		if present {
			delete(ns.pendingSnapshots, snapshotKey)
		}
		ns.pendingSnapshotsMu.Unlock()
		if present {
			rs.releaseRef()
			h.completePendingOp()
		}
		// The caller's own reference is dropped whether or not the claim
		// won; a sweep that beat us drops only its own.
		rs.Release()
		engNode.ClearSnapshotting()
		return nil, ErrClosed
	}

	// Build engine-level option from the public SnapshotOption.
	engineOpt := engine.SnapshotRequestOption{
		ExportPath:         opt.ExportPath,
		CompactionOverhead: opt.CompactionOverhead,
		OverrideCompaction: opt.OverrideCompactionOverhead,
		Exported:           opt.IsExport(),
	}

	// Submit the snapshot request to the engine's snapshot pool.
	if submitErr := h.engine.RequestSnapshot(shardID, ns.replicaID, engNode, engineOpt); submitErr != nil {
		// Snapshot pool rejected (duplicate or full) -- clean up. Claim the
		// entry first: OnSnapshotCompleted drains the ENTIRE pendingSnapshots
		// map on any completion for this shard, so a concurrent completion
		// may already have completed, removed and released this entry between
		// the Store above and this rollback. An unconditional
		// releaseRef/completePendingOp would then double-count — outstanding
		// goes negative so tryRecycle's outstanding==0 check never holds and
		// the object is permanently lost from the pool, and pendingOps is
		// undercounted so Drain can close drainC with work in flight.
		// rs.Release() is the caller's own reference and is dropped
		// unconditionally.
		engNode.ClearSnapshotting()
		ns.pendingSnapshotsMu.Lock()
		_, claimed := ns.pendingSnapshots[snapshotKey]
		if claimed {
			delete(ns.pendingSnapshots, snapshotKey)
		}
		ns.pendingSnapshotsMu.Unlock()
		if claimed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		return nil, ErrSnapshotInProgress
	}

	return rs, nil
}

// ErrSnapshotEncryptedBarrierUnavailable indicates that a snapshot encrypted
// at rest (Epoch > 0) cannot be read because the barrier required to decrypt
// it is unavailable (either no barrier is configured for the shard or the
// barrier is sealed).
//
// ExportSnapshot returns it because exported snapshots must be plaintext, so
// emitting the raw ciphertext would corrupt the export -> import round-trip.
// StartShard returns it (wrapped in HostInitError{Field: "snapshot_decrypt"})
// because recovering a state machine from ciphertext would either fail inside
// the state machine or install corrupt state; a node that cannot decrypt its
// own snapshot must fail closed at startup. The runtime recovery path
// (snapshotPool.recoverSnapshot) returns the same sentinel on the same
// predicate, which is why it is declared in the engine and re-exported here:
// both consumers of the artifact must be distinguishable by one errors.Is.
var ErrSnapshotEncryptedBarrierUnavailable = engine.ErrSnapshotEncryptedBarrierUnavailable

// ErrSnapshotBarrierSealed indicates that a snapshot could not be WRITTEN
// because the shard's barrier is configured but sealed. It is the write-side
// counterpart of ErrSnapshotEncryptedBarrierUnavailable: the local save path
// (snapshotPool.saveSnapshot) and the receive path (HandleSnapshot, via
// reencryptReceivedSnapshot) both refuse rather than produce a plaintext
// artifact for a shard that is configured to be encrypted at rest. A shard
// with NO barrier is unaffected — plaintext is its configured outcome.
//
// It is declared in the engine and re-exported here for the same reason
// ErrSnapshotEncryptedBarrierUnavailable is: both packages must fail with one
// error identity.
var ErrSnapshotBarrierSealed = engine.ErrSnapshotBarrierSealed

// snapshotArtifactMagic identifies a QuicRaft exported-snapshot artifact.
// It is written as the first 8 bytes of every export so that ImportSnapshot
// can detect a well-formed artifact (and reject foreign/legacy data).
var snapshotArtifactMagic = [8]byte{'Q', 'R', 'S', 'N', 'A', 'P', '0', '1'}

// snapshotArtifactVersion is the current artifact header version. It is
// bumped when the header layout changes so older/newer readers can detect
// incompatibility instead of misparsing.
// Version 2 added the Compression field: a v1 artifact carries no statement
// of how its data was encoded, so a destination host whose own config differs
// from the exporter's would decode it wrong. Such an artifact is rejected
// rather than assumed uncompressed.
const snapshotArtifactVersion uint32 = 2

// snapshotArtifactFixedHeaderSize is the size of the fixed portion of the
// artifact header that precedes the variable-length membership block:
//
//	magic[8] | version:u32 | index:u64 | term:u64 | onDiskIndex:u64 |
//	compression:u16 | membLen:u32
const snapshotArtifactFixedHeaderSize = 8 + 4 + 8 + 8 + 8 + 2 + 4

// maxSnapshotArtifactMembLen bounds the membership block declared by an
// imported artifact header before it is allocated, so a crafted/corrupt
// membLen cannot trigger a multi-GB allocation (OOM/DoS) ahead of the
// io.ReadFull that would otherwise fail. A real membership — four maps of at
// most maxMembershipEntries entries, each entry an id plus a bounded address —
// is far below this 16 MiB ceiling; the inner proto decoder enforces the exact
// per-map/per-field bounds.
const maxSnapshotArtifactMembLen = 16 * 1024 * 1024

// snapshotArtifactHeader is the self-describing metadata that precedes the
// plaintext snapshot data in an exported artifact. It carries the original
// logical snapshot identity (Index/Term), the on-disk state machine index,
// and the cluster membership, so that ImportSnapshot can reconstruct the
// SAME logical snapshot on the destination host. Without this, an imported
// snapshot would lose its Raft index/term and membership, corrupting
// recovery (compaction boundary, term, and routing table).
//
// The Epoch is deliberately not carried: exported artifacts are always
// PLAINTEXT (ExportSnapshot decrypts), and imports persist Epoch=0 so the
// recovery path does not attempt to decrypt with a foreign barrier key.
// Compression IS carried: exports are decrypted but never decompressed, so
// the destination host must be told how the payload is encoded. Without it an
// import between hosts whose SnapshotCompressionType differs recovers the
// state machine from bytes in the wrong encoding.
type snapshotArtifactHeader struct {
	Index       uint64
	Term        uint64
	OnDiskIndex uint64
	Compression config.CompressionType
	Membership  proto.Membership
}

// marshalSnapshotArtifactHeader encodes the header into a freshly allocated
// byte slice using the fixed little-endian layout. The membership is encoded
// via proto.Membership.MarshalTo and length-prefixed.
func marshalSnapshotArtifactHeader(h snapshotArtifactHeader) ([]byte, error) {
	membSize := h.Membership.Size()
	buf := make([]byte, snapshotArtifactFixedHeaderSize+membSize)
	copy(buf[0:8], snapshotArtifactMagic[:])
	binary.LittleEndian.PutUint32(buf[8:12], snapshotArtifactVersion)
	binary.LittleEndian.PutUint64(buf[12:20], h.Index)
	binary.LittleEndian.PutUint64(buf[20:28], h.Term)
	binary.LittleEndian.PutUint64(buf[28:36], h.OnDiskIndex)
	binary.LittleEndian.PutUint16(buf[36:38], uint16(h.Compression))
	binary.LittleEndian.PutUint32(buf[38:42], uint32(membSize))
	if membSize > 0 {
		if _, err := h.Membership.MarshalTo(buf[snapshotArtifactFixedHeaderSize:]); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// readSnapshotArtifactHeader reads and validates the artifact header from r,
// returning the parsed header. It verifies the magic and version so that
// malformed, legacy, or foreign streams are rejected with a clear error
// rather than being misinterpreted as snapshot data.
func readSnapshotArtifactHeader(r io.Reader) (snapshotArtifactHeader, error) {
	var fixed [snapshotArtifactFixedHeaderSize]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return snapshotArtifactHeader{}, &SnapshotArtifactError{Reason: "read header", Err: err}
	}
	if !bytesEqual(fixed[0:8], snapshotArtifactMagic[:]) {
		return snapshotArtifactHeader{}, &SnapshotArtifactError{Reason: "bad magic"}
	}
	version := binary.LittleEndian.Uint32(fixed[8:12])
	if version != snapshotArtifactVersion {
		return snapshotArtifactHeader{}, &SnapshotArtifactError{
			Reason: fmt.Sprintf("unsupported version %d (want %d)", version, snapshotArtifactVersion),
		}
	}
	hdr := snapshotArtifactHeader{
		Index:       binary.LittleEndian.Uint64(fixed[12:20]),
		Term:        binary.LittleEndian.Uint64(fixed[20:28]),
		OnDiskIndex: binary.LittleEndian.Uint64(fixed[28:36]),
		Compression: config.CompressionType(binary.LittleEndian.Uint16(fixed[36:38])),
	}
	// Index 0 is "no snapshot", not a snapshot at index zero. It is also the
	// artifact's on-disk NAME: snapshot-00000000000000000000 sorts FIRST among
	// the shard's snapshot directories, so such an import would be the first
	// thing gcSnapshotDirs destroys and would never be chosen by
	// latestSnapshotDir, the recovery fallback. Recovery would additionally
	// read it as an empty compaction boundary.
	if hdr.Index == 0 {
		return snapshotArtifactHeader{}, &SnapshotArtifactError{
			Reason: "snapshot index is zero",
		}
	}
	// An algorithm this binary cannot decompress makes the payload
	// unreadable; reject the artifact rather than import bytes that will be
	// handed to a state machine in an unknown encoding.
	if !hdr.Compression.IsValid() {
		return snapshotArtifactHeader{}, &SnapshotArtifactError{
			Reason: fmt.Sprintf("unsupported compression %d", uint16(hdr.Compression)),
		}
	}
	membLen := binary.LittleEndian.Uint32(fixed[38:42])
	if membLen > maxSnapshotArtifactMembLen {
		return snapshotArtifactHeader{}, &SnapshotArtifactError{
			Reason: fmt.Sprintf("membership length %d exceeds maximum %d", membLen, maxSnapshotArtifactMembLen),
		}
	}
	if membLen > 0 {
		membBuf := make([]byte, membLen)
		if _, err := io.ReadFull(r, membBuf); err != nil {
			return snapshotArtifactHeader{}, &SnapshotArtifactError{Reason: "read membership", Err: err}
		}
		if _, err := hdr.Membership.UnmarshalFrom(membBuf); err != nil {
			return snapshotArtifactHeader{}, &SnapshotArtifactError{Reason: "decode membership", Err: err}
		}
	}
	return hdr, nil
}

// bytesEqual reports whether two byte slices are equal. Used for the
// fixed-length magic comparison without pulling in bytes.Equal at the
// single call site.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SnapshotArtifactError indicates that an exported snapshot artifact could
// not be parsed during ImportSnapshot, for example because the magic or
// version did not match or the embedded membership was malformed.
type SnapshotArtifactError struct {
	Reason string
	Err    error
}

// Error returns a human-readable description of the artifact parse failure.
func (e *SnapshotArtifactError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("quicraft: invalid snapshot artifact: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("quicraft: invalid snapshot artifact: %s", e.Reason)
}

// Unwrap returns the underlying error, if any.
func (e *SnapshotArtifactError) Unwrap() error { return e.Err }

// ExportSnapshot streams a self-describing snapshot artifact for the given
// shard to the provided writer. The shard must have at least one snapshot
// available in LogDB.
//
// The artifact is a versioned header (magic, version, original Index, Term,
// OnDiskIndex, and Membership) followed by the PLAINTEXT snapshot data. The
// header preserves the logical snapshot identity so ImportSnapshot can
// reconstruct the SAME snapshot on the destination host: recovery relies on
// Index (compaction boundary), Term, Membership (routing table), and
// OnDiskIndex (on-disk state machine cursor). Emitting only the raw data —
// as an earlier version did — silently dropped membership and reset the
// Raft index/term, so the imported snapshot was a different logical
// snapshot.
//
// Exported snapshot data is always PLAINTEXT. When a barrier is configured
// the on-disk snapshot.dat is stored in the encrypted frame format; this
// method decrypts it before writing so the writer receives usable plaintext.
// This matches ImportSnapshot, which stores imported bytes verbatim with
// Epoch=0 (the no-decrypt recovery path), making export -> import a lossless
// round-trip across hosts with independent barrier keys.
//
// The decrypting barrier is resolved per shard via barrierForShard — the
// same resolver the snapshot receive/recover paths use — so a snapshot
// encrypted under a WithShardBarriers per-shard key exports correctly and
// is never fed to the (wrong) host-wide barrier.
//
// When the snapshot is encrypted (Epoch > 0) but the barrier cannot decrypt
// it (no barrier is configured or the barrier is sealed), ExportSnapshot
// returns ErrSnapshotEncryptedBarrierUnavailable rather than silently
// emitting ciphertext.
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
	f, err := h.snapshotFsys().Open(dataPath)
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

	// Resolve the at-rest barrier for THIS shard. With WithShardBarriers the
	// snapshot was encrypted under the per-shard key, so the host-wide
	// barrier is the wrong (or absent) key for it; barrierForShard mirrors
	// the receive/recover paths.
	barrier, barrierErr := h.barrierForShard(shardID)
	if barrierErr != nil {
		return barrierErr
	}

	// Refuse early if the snapshot is encrypted but cannot be decrypted, so
	// we never emit a (partial) artifact header for an unrecoverable export.
	if logdbSnap.Epoch > 0 && (barrier == nil || barrier.IsSealed()) {
		return ErrSnapshotEncryptedBarrierUnavailable
	}
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// Build the self-describing artifact header carrying the original
	// snapshot identity (Index/Term/OnDiskIndex/Membership). ImportSnapshot
	// parses this to persist the SAME logical snapshot.
	headerBytes, err := marshalSnapshotArtifactHeader(snapshotArtifactHeader{
		Index:       logdbSnap.Index,
		Term:        logdbSnap.Term,
		OnDiskIndex: logdbSnap.OnDiskIndex,
		Compression: logdbSnap.Compression,
		Membership:  logdbMembershipToProto(logdbSnap.Membership),
	})
	if err != nil {
		return err
	}

	// When the snapshot was encrypted at rest (Epoch > 0) it must be
	// decrypted to plaintext before writing. Without this the writer would
	// receive raw ciphertext that a later ImportSnapshot stores with
	// Epoch=0, feeding undecryptable bytes into RecoverFromSnapshot.
	//
	// The header is held back until the first frame decrypts successfully
	// (deferredHeaderWriter): a wrong or rotated key fails AEAD on the very
	// first frame, and emitting the header eagerly would leave the caller
	// holding a truncated header-only artifact for a failed export.
	if logdbSnap.Epoch > 0 {
		// Pin the recorded snapshot epoch live for the duration of the read
		// so a concurrent keyRotationLoop -> PurgeEpochsBefore cannot remove
		// its DEK mid-stream and fail AEAD partway through a multi-GB export
		// (GlobalSafePurgeFloor does not account for an in-progress export).
		// This mirrors decryptSnapshotForSend: frames embed their own epoch,
		// and the recorded epoch is the minimum across them, so pinning it
		// protects the oldest DEK any frame may reference.
		barrier.RegisterInFlightEpoch(logdbSnap.Epoch)
		defer barrier.DeregisterInFlightEpoch(logdbSnap.Epoch)

		hw := &deferredHeaderWriter{w: w, header: headerBytes}
		if _, err = decryptSnapshotFrames(barrier, f, hw); err != nil {
			return err
		}
		// A frameless (empty-data) stream never triggered a write; the
		// header alone is still a complete, importable artifact.
		return hw.flushHeader()
	}

	// Epoch == 0: the on-disk snapshot is already plaintext, so a raw copy
	// is correct. copyWithContext performs periodic cancellation checks.
	if _, err = w.Write(headerBytes); err != nil {
		return err
	}
	if err = copyWithContext(ctx, w, f); err != nil {
		return err
	}
	return nil
}

// deferredHeaderWriter withholds a header from the underlying writer until
// the first payload write arrives, so producers that can fail before
// yielding any payload (AEAD decryption with a wrong key) fail before ANY
// byte reaches the destination instead of after a dangling header.
// flushHeader must be called after a successful payload-less production to
// emit the header for empty streams.
type deferredHeaderWriter struct {
	w      io.Writer
	header []byte // pending header; nil once written
}

// Write emits the pending header (once) before the first payload bytes.
func (d *deferredHeaderWriter) Write(p []byte) (int, error) {
	if err := d.flushHeader(); err != nil {
		return 0, err
	}
	return d.w.Write(p)
}

// flushHeader writes the header if it has not been written yet. Idempotent.
func (d *deferredHeaderWriter) flushHeader() error {
	if d.header == nil {
		return nil
	}
	header := d.header
	d.header = nil
	_, err := d.w.Write(header)
	return err
}

// protoMembershipToLogDB converts a proto.Membership to a logdb.Membership.
// Both types have identical field layouts; all maps are deep-copied to
// prevent aliasing between the decoded header and LogDB storage.
func protoMembershipToLogDB(m proto.Membership) logdb.Membership {
	result := logdb.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
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

// ImportOption configures ImportSnapshot.
type ImportOption func(*importOptions)

// importOptions carries an explicit operator assertion about the shard's
// on-disk layout, used to CROSS-CHECK what ImportSnapshot discovers from the
// shard's persisted bootstrap record.
type importOptions struct {
	dataDir    string
	dataDirSet bool
}

// WithImportDataDir asserts that the shard will be started with this
// Config.DataDir. Passing "" asserts that the shard has none and roots its
// snapshot tree under the host's NodeHostDir.
//
// It is no longer REQUIRED: StartShard records the shard's DataDir in its
// LogDB bootstrap record, and ImportSnapshot reads it back, so the layout is
// discovered rather than declared. When the option IS supplied it must match
// the recorded value exactly; a disagreement is refused with
// *ImportDataDirMismatchError rather than silently resolved, because either
// side being wrong puts the artifact outside the tree snapshot garbage
// collection and the DEK purge-floor scan enumerate — which leaks it and lets
// the next PurgeEpochsBefore destroy the DEK it needs.
//
// The option remains meaningful for a shard whose layout cannot be discovered:
// one with NO bootstrap record (started with join=true, which records none),
// and one whose record PREDATES the DataDir field. Both fail closed with
// ErrImportLayoutUnknown unless the caller states the layout here, and in both
// cases the stated value is trusted because there is nothing to cross-check it
// against.
func WithImportDataDir(dir string) ImportOption {
	return func(o *importOptions) {
		o.dataDir = dir
		o.dataDirSet = true
	}
}

// resolveImportDataDir determines the shard's snapshot-tree root for an import,
// preferring the value StartShard recorded in the bootstrap record over the
// caller's assertion, and refusing to guess.
//
// The previous contract — "a shard configured with Config.DataDir must pass
// WithImportDataDir" — was unenforceable: nothing validated it and
// ImportSnapshot could not detect the omission, so omitting it wrote the
// artifact under NodeHostDir while StartShard pointed the shard's SnapshotDir
// at DataDir/shard-N. listSnapshotDirs then never enumerated the artifact, it
// contributed no floor to the DEK purge, and the next PurgeEpochsBefore
// legitimately destroyed the key it needed: the imported disaster-recovery
// snapshot became permanently undecryptable, silently, behind a caller opt-in.
func (h *Host) resolveImportDataDir(shardID, replicaID uint64, iopts importOptions) (string, error) {
	bs, ok, err := h.logdb.GetBootstrap(shardID, replicaID)
	if err != nil {
		return "", err
	}
	// A record that PREDATES the DataDir field proves nothing about the layout:
	// it decodes the field as "" and that is indistinguishable from a shard
	// that genuinely has none, so it is treated exactly like no record at all.
	// Reading it as proof was wrong in both directions — it silently resolved a
	// DataDir shard's import to the NodeHostDir tree (the DEK destruction this
	// whole mechanism exists to prevent), AND it refused the documented
	// WithImportDataDir escape hatch as a contradiction, leaving the operator no
	// way through.
	if !ok || !bs.DataDirRecorded {
		// No recorded layout. Only the caller can supply it, and a wrong
		// guess costs the artifact's DEK, so refuse rather than assume.
		if !iopts.dataDirSet {
			return "", &ImportLayoutUnknownError{ShardID: shardID, ReplicaID: replicaID}
		}
		return iopts.dataDir, nil
	}
	if iopts.dataDirSet && iopts.dataDir != bs.DataDir {
		return "", &ImportDataDirMismatchError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Declared:  iopts.dataDir,
			Recorded:  bs.DataDir,
		}
	}
	return bs.DataDir, nil
}

// ImportSnapshot imports a snapshot artifact from the reader for the given
// shard. The shard must be stopped (not loaded on this host) before calling
// this method. The artifact is the versioned header + plaintext data
// produced by ExportSnapshot; the header is parsed first to recover the
// ORIGINAL snapshot identity (Index/Term/OnDiskIndex/Membership), and the
// remaining bytes are written verbatim as snapshot.dat. The snapshot is
// recorded in LogDB so the next StartShard recovers the SAME logical
// snapshot. The context is checked for cancellation at key blocking points.
//
// The artifact is written to the canonical snapshot path — the same
// shardSnapshotBaseDir + engine.SnapshotSaveDir construction StartShard, the
// snapshot pool and the receive path use — so it is visible to snapshot garbage
// collection and, critically, to the directory scan that computes the DEK purge
// floor.
//
// The shard's snapshot-tree root is DISCOVERED from the Config.DataDir
// StartShard recorded in the LogDB bootstrap record. When there is no such
// record (a shard started with join=true, or one this host has never started —
// the disaster-recovery case), or when the record predates the DataDir field
// and therefore carries none, the layout cannot be discovered and the import
// fails closed with ErrImportLayoutUnknown; pass WithImportDataDir to state it,
// using WithImportDataDir("") to assert the shard has no DataDir. A supplied
// value that contradicts the recorded one is refused with
// ErrImportDataDirMismatch rather than silently resolved.
//
// The on-disk directory is named by the artifact's own snapshot INDEX, the
// same way every other snapshot writer names one: the snapshot tree is ORDERED
// by that name, so an import must take its rightful place among the shard's
// existing snapshots rather than sorting as the oldest.
//
// # Why the artifact is staged and renamed
//
// Naming the directory by the artifact's index means an import can resolve to a
// path it does NOT own: an operator retry of the same artifact, a
// disaster-recovery artifact exported from a peer at an index this replica also
// reached (snapshot indices are largely deterministic), or a concurrent second
// import. So the import never writes through that path directly. It refuses a
// destination that already exists, stages everything in a directory MkdirTemp
// created — one this call provably owns, and the only thing a failure is ever
// allowed to delete — and renames it into place once the artifact is complete
// and durable. Writing in place would have truncated a LIVE snapshot.dat before
// anything was validated, leaving the shard's LogDB record describing a file it
// no longer matched: a short FileSize ships a truncated snapshot that erases the
// receiving follower's state, and a stale Epoch makes StartShard fail closed on
// an undecryptable snapshot.
//
// Two refusals follow from that, both raised before the tree is touched:
//
//   - an artifact whose index does not ADVANCE the shard's recorded snapshot is
//     refused with *ImportSnapshotOutOfDateError, which matches
//     logdb.ErrSnapshotOutOfDate — the same answer the LogDB would have given
//     after the damage was done, and
//   - a destination directory that already exists is refused with
//     *ImportSnapshotDirExistsError. Clearing it is an operator decision; an
//     import never removes a directory it did not create.
//
// Concurrent imports of one shard are serialised, so the checks above cannot be
// interleaved with another import's writes.
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error {
	replicaID, err := h.importPreflight(ctx, shardID)
	if err != nil {
		return err
	}

	// Parse the self-describing artifact header BEFORE touching disk so a
	// malformed/foreign stream is rejected without creating partial state.
	// The header carries the original logical snapshot identity; the bytes
	// that follow are the plaintext snapshot data.
	header, err := readSnapshotArtifactHeader(r)
	if err != nil {
		return err
	}

	// Serialise imports for this shard. Everything from here on reasons about
	// the shard's recorded snapshot and its on-disk tree, and both are resolved
	// from the artifact's index, so a second concurrent import of the same
	// index would otherwise interleave with this one's checks.
	release, err := h.acquireImportLock(ctx, shardID)
	if err != nil {
		return err
	}
	defer release()

	// Refuse an artifact that does not ADVANCE the shard's recorded snapshot,
	// before the tree is touched. Every production LogDB refuses such a save
	// with logdb.ErrSnapshotOutOfDate, so deferring the check to the final
	// SaveSnapshot meant the refusal arrived after the artifact had already
	// been written over the directory the recorded snapshot lives in.
	if err := h.checkImportAdvancesSnapshot(shardID, replicaID, header.Index); err != nil {
		return err
	}

	// Check context before performing I/O.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	snapDir, err := h.importSnapshotDir(shardID, replicaID, header.Index, opts)
	if err != nil {
		return err
	}

	fsys := h.snapshotFsys()
	// A destination that already exists is a directory this import did not
	// create. It may be a retained snapshot the shard still recovers from, so
	// neither writing into it nor removing it is permissible.
	if _, statErr := fsys.Stat(snapDir); statErr == nil {
		return &ImportSnapshotDirExistsError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Index:     header.Index,
			Dir:       snapDir,
		}
	} else if !os.IsNotExist(statErr) {
		// The destination's existence is indeterminate; fail closed rather
		// than proceed towards a rename that might clobber it.
		return statErr
	}

	// Record the deepest pre-existing ancestor BEFORE MkdirAll: the directory
	// fsync below must cover every directory this call creates (same
	// bookkeeping as ReencryptSnapshotFile).
	syncRoot := deepestExistingAncestor(fsys, snapDir)
	stageParent := filepath.Dir(snapDir)
	if mkdirErr := fsys.MkdirAll(stageParent, 0o750); mkdirErr != nil {
		return mkdirErr
	}

	// Stage in a directory this call provably owns. The name cannot parse as
	// "snapshot-<index>", so engine.listSnapshotDirs — and with it snapshot GC,
	// the recovery fallback and the DEK purge-floor scan — never sees a
	// half-written import.
	stageDir, tempErr := fsys.MkdirTemp(stageParent, importStageDirPattern)
	if tempErr != nil {
		return tempErr
	}

	dataPath := filepath.Join(stageDir, "snapshot.dat")
	if writeErr := h.writeImportedSnapshotData(fsys, shardID, dataPath, r); writeErr != nil {
		// Do not leave a partial (possibly multi-gigabyte) artifact behind
		// for an import that will never be recorded in LogDB.
		discardImportDir(fsys, shardID, stageDir)
		return writeErr
	}

	// Re-encrypt the imported snapshot under THIS host's own barrier so it is
	// encrypted at rest with the same key every other on-disk snapshot for the
	// shard uses. The artifact's bytes are plaintext (ExportSnapshot decrypts
	// before writing, so an import never carries ciphertext), so this is what
	// closes the at-rest gap for a shard whose barrier is configured — the
	// same role reencryptReceivedSnapshot plays for a network-received
	// snapshot. A nil barrier is a no-op (snapshot stays plaintext, epoch 0);
	// a barrier sealed after the precheck above fails closed here rather than
	// leaving the plaintext artifact durably recorded, as does a per-shard
	// barrier that stopped resolving since the precheck.
	importBarrier, barrierErr := h.barrierForShard(shardID)
	if barrierErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return barrierErr
	}
	epoch, encErr := reencryptReceivedSnapshot(fsys, dataPath, importBarrier)
	if encErr != nil {
		// Never recorded in LogDB, so the partial artifact must not be left
		// behind for a later scan to trip over.
		discardImportDir(fsys, shardID, stageDir)
		return encErr
	}

	// Write the snapshot metadata file beside the artifact, recording the epoch
	// the re-encryption above actually used.
	//
	// Without it the directory is anonymous to every scan: engine's purge-floor
	// computation reads snapshot.meta for each snapshot directory and treats a
	// MISSING file as "GC'd, imposes no floor", so the imported snapshot's DEK
	// is legitimately dropped by the next PurgeEpochsBefore and the restored
	// state becomes permanently undecryptable — StartShard then fails closed.
	// For a disaster-recovery import that is total loss of the restore.
	term := header.Term
	if term == 0 {
		term = 1
	}
	if metaErr := writeReceivedSnapshotMetadata(fsys, stageDir, header.Index, term,
		shardID, replicaID, epoch, header.Compression); metaErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return metaErr
	}

	// Determine the data file size for recovery's InstallSnapshot path. This
	// is measured AFTER reencryption so FileSize always means "size of the
	// file at Filepath", matching the framed-ciphertext size when a barrier
	// is configured rather than the plaintext size written above. It is read
	// from the staging path, which is the same file the rename below publishes.
	var fileSize uint64
	if fi, statErr := fsys.Stat(dataPath); statErr == nil {
		fileSize = uint64(fi.Size())
	}

	// Make the staged directory's own entries durable before it is published:
	// the rename below makes the whole directory reachable at once, and a
	// directory whose entries are not durable would come back after a crash
	// under the canonical name with files missing.
	//
	// This is DEFENCE IN DEPTH, not the primary guarantee, and the distinction
	// matters to anyone tempted to delete it or to write a test for it. The
	// syncDirChain after the rename fsyncs snapDir FIRST and its parent second,
	// and a rename does not change the inode — so the leaf fsync already covers
	// these same entries, and snapDir's own entry does not become durable until
	// the parent fsync that follows it. No crash point between the two can
	// therefore expose an incomplete published directory, which is why deleting
	// this call leaves the suite (including
	// TestImportSnapshot_CrashAtEveryStepLeavesAConsistentTree, which walks the
	// crash point across every filesystem operation the import performs) green.
	// It is kept for the filesystem that makes a rename durable without covering
	// the entries created beneath the source before it — an ordering POSIX does
	// not forbid — at the cost of one fsync per operator-initiated import.
	if syncErr := syncDir(fsys, stageDir); syncErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return syncErr
	}

	// Publish. The destination did not exist when this call started and no
	// other import for this shard can have created it since (the per-shard
	// lock above), so this rename adds a name rather than replacing one — and
	// a rename onto a non-empty directory would fail rather than destroy it.
	if renameErr := fsys.Rename(stageDir, snapDir); renameErr != nil {
		discardImportDir(fsys, shardID, stageDir)
		return renameErr
	}

	// The canonical directory now exists and THIS import is what created it —
	// the destination-exists precheck above proved it did not exist, and the
	// per-shard import lock has been held across both. published records that
	// fact explicitly rather than leaving a later cleanup to infer it, because
	// the whole point of the precheck is that an import must never remove a
	// directory it did not create.
	//
	// Every remaining failure must take the publication back down. Leaving it
	// standing turns a transient error — an fsync failure, a cancelled context,
	// a LogDB write error during a disaster-recovery restore — into a permanent
	// lockout: the retry hits the destination-exists precheck and is refused
	// with *ImportSnapshotDirExistsError, whose remedy is a manual rm -rf
	// performed against an error message that says an import never removes a
	// directory it did not create. Before the staging rewrite the retry simply
	// overwrote the half-published directory and succeeded.
	published := true
	defer func() {
		if published {
			discardImportDir(fsys, shardID, snapDir)
		}
	}()

	// Make the directory entries durable BEFORE recording the snapshot in
	// LogDB. The data file is fsynced (both the original write and the
	// reencrypt rename above), but the entries the MkdirAll and the publishing
	// rename added are not durable until each directory in the chain is
	// fsynced; without this a crash right after the import leaves LogDB
	// pointing at a directory that no longer exists, and StartShard then fails
	// with snapshot_open. Same invariant HandleSnapshot enforces.
	if syncErr := syncDirChain(fsys, snapDir, syncRoot); syncErr != nil {
		return syncErr
	}

	// Check context before committing to LogDB.
	if err := ctx.Err(); err != nil {
		return mapContextError(err)
	}

	// Persist the ORIGINAL snapshot metadata from the artifact header so the
	// imported snapshot is the SAME logical snapshot: recovery uses Index as
	// the compaction boundary, Term for the snapshot term, Membership for the
	// routing table, and OnDiskIndex for on-disk state machines. Term is
	// floored at 1 (the minimum valid term) so the snapshot is never treated
	// as uninitialized by the Raft engine.
	//
	// Epoch records what reencryptReceivedSnapshot actually did: 0 when the
	// shard has no barrier configured (plaintext deployment, artifact stays
	// as imported), or the DEK epoch it was just re-encrypted under when a
	// barrier is configured and unsealed. Recovery reads Epoch>0 as "decrypt
	// before use", so this must reflect this host's own at-rest state, not
	// the artifact's. term was floored above and is shared with the on-disk
	// metadata so both descriptions of this snapshot agree.
	logdbSnap := logdb.Snapshot{
		Index:       header.Index,
		Term:        term,
		Membership:  protoMembershipToLogDB(header.Membership),
		Filepath:    snapDir,
		FileSize:    fileSize,
		OnDiskIndex: header.OnDiskIndex,
		Epoch:       epoch,
		// The artifact's own compression, NOT this host's config: the bytes
		// were written by the exporting host and are stored verbatim.
		Compression: header.Compression,
	}
	if saveErr := h.logdb.SaveSnapshot(shardID, replicaID, logdbSnap); saveErr != nil {
		return saveErr
	}
	// Recorded: the directory is now the shard's snapshot and the rollback above
	// must not run.
	published = false
	return nil
}

// importPreflight refuses an import that must not touch disk at all, and
// resolves the shard's replicaID from the LogDB bootstrap records.
//
// Every check here runs BEFORE any directory, data file or LogDB record
// exists, so a refusal leaves no partial state behind.
func (h *Host) importPreflight(ctx context.Context, shardID uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, mapContextError(err)
	}
	if h.closed.Load() {
		return 0, ErrClosed
	}

	// The import writes a full snapshot artifact to disk; starting one while
	// the WAL disk monitor reports the volume full would only deepen the
	// exhaustion and leave a truncated artifact behind.
	if h.diskFull != nil && h.diskFull.Load() {
		return 0, ErrDiskFull
	}

	// Verify shard is NOT running.
	if _, running := h.nodes.Load(shardID); running {
		return 0, ErrInvalidOperation
	}

	if h.logdb == nil {
		return 0, ErrInvalidOperation
	}

	// Determine the replicaID from LogDB bootstrap info.
	nodes, err := h.logdb.ListNodeInfo()
	if err != nil {
		return 0, err
	}
	var replicaID uint64
	for _, ni := range nodes {
		if ni.ShardID == shardID {
			replicaID = ni.ReplicaID
			break
		}
	}
	if replicaID == 0 {
		return 0, ErrShardNotFound
	}

	// A CONFIGURED but SEALED barrier means at-rest encryption is temporarily
	// unavailable for this shard, not that the shard is plaintext. Refuse
	// before any directory, data file, or LogDB record exists, mirroring the
	// same precheck HandleSnapshot enforces on the receive path: an imported
	// artifact's bytes are plaintext (ExportSnapshot decrypts before writing
	// them), so persisting them while sealed would durably downgrade a shard
	// whose data must be encrypted at rest. reencryptReceivedSnapshot in the
	// caller re-checks and covers a seal that lands mid-import.
	//
	// An UNRESOLVED per-shard barrier fails here for the same reason: the
	// artifact would be published in plaintext for a shard the deployment
	// asked to encrypt.
	if _, barrierErr := h.snapshotBarrierForShard(shardID); barrierErr != nil {
		return 0, barrierErr
	}
	return replicaID, nil
}

// importStageDirPattern names the directory an import stages its artifact in
// before publishing it under the canonical, index-derived name.
//
// The leading dot and the "import" word make an orphan (a process killed
// mid-import) self-identifying, and the name deliberately cannot parse as
// "snapshot-<index>": engine.listSnapshotDirs skips it, so a half-written
// import is invisible to snapshot GC, to latestSnapshotDir — the recovery
// fallback — and to the DEK purge-floor scan.
//
// That same invisibility is why the pattern is NOT defined here: an orphaned
// staging directory holds the imported state machine in PLAINTEXT and nothing
// else on the host will ever enumerate it, so the name a writer creates must be
// the name engine.SweepStaleImportStageDirs reclaims. The constant is shared
// with the sweeper for exactly the reason the decrypt scratch prefixes are.
const importStageDirPattern = engine.ImportStageDirPattern

// checkImportAdvancesSnapshot refuses an artifact whose index does not advance
// the shard's recorded snapshot.
//
// This is the SAME condition memdb.SaveSnapshot and waldb.DB.SaveSnapshot
// enforce with logdb.ErrSnapshotOutOfDate; running it here makes the refusal
// arrive BEFORE the shard's snapshot tree is touched. Snapshot indices are
// largely deterministic (a save is triggered at processed - lastSnapshotIndex
// >= SnapshotEntries), so an artifact exported from a peer routinely carries an
// index this replica also reached, and an idempotent operator retry carries the
// same index by construction — the collision is the expected case, not a rare
// one.
//
// A shard with no recorded snapshot imposes no constraint. Any error other than
// logdb.ErrNoSnapshot is returned: an import must not proceed on an
// indeterminate view of what it might be about to replace.
func (h *Host) checkImportAdvancesSnapshot(shardID, replicaID, index uint64) error {
	existing, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil {
		if errors.Is(err, logdb.ErrNoSnapshot) {
			return nil
		}
		return err
	}
	if index > existing.Index {
		return nil
	}
	return &ImportSnapshotOutOfDateError{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Index:     index,
		Recorded:  existing.Index,
	}
}

// importSnapshotDir resolves the canonical on-disk directory an imported
// artifact must be written to.
//
// The path is built exactly as every other snapshot writer builds one: the
// shard's canonical base directory plus the engine's save-dir construction.
// Hand-rolling it here produced a tree one level shallower than the canonical
// one, so listSnapshotDirs never saw the import: snapshot GC leaked it, and —
// far worse — the imported snapshot's epoch never reached the DEK purge floor
// (which is computed by SCANNING these directories), so the next
// PurgeEpochsBefore legitimately dropped the DEK and StartShard then failed
// closed on an undecryptable snapshot: total loss of a disaster-recovery
// restore.
//
// index is the artifact's OWN snapshot index and names the directory, exactly
// as every other snapshot writer names one. That name is not decoration:
// listSnapshotDirs PARSES it and ORDERS by it, gcSnapshotDirs deletes the
// lowest-ordered directories, and latestSnapshotDir — the recovery fallback —
// picks the highest. Naming an import by a per-host sequence number instead
// made a disaster-recovery restore sort as the OLDEST snapshot on a node that
// already had some: first in line for GC, and never chosen by the recovery
// fallback, which would prefer a stale but higher-numbered directory.
func (h *Host) importSnapshotDir(shardID, replicaID, index uint64, opts []ImportOption) (string, error) {
	var iopts importOptions
	for _, opt := range opts {
		opt(&iopts)
	}
	// The layout comes from what StartShard RECORDED, not from what the caller
	// remembered to declare. resolveImportDataDir fails closed when it can
	// prove neither.
	importDataDir, err := h.resolveImportDataDir(shardID, replicaID, iopts)
	if err != nil {
		return "", err
	}
	return engine.SnapshotSaveDir(
		shardSnapshotBaseDir(h.cfg.NodeHostDir, importDataDir, shardID),
		shardID, replicaID, index), nil
}

// writeImportedSnapshotData writes the artifact's post-header bytes to the
// snapshot data file and makes them durable. The file is always closed; on
// error the caller discards the whole directory.
func (h *Host) writeImportedSnapshotData(fsys snapshotFS, shardID uint64, dataPath string, r io.Reader) error {
	f, createErr := fsys.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if createErr != nil {
		return createErr
	}
	closeFile := func(when string) {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot import file close failed",
				"shard", shardID,
				"path", dataPath,
				"when", when,
				"error", closeErr)
		}
	}

	// Bound the copy by the maximum snapshot size. An unbounded io.Copy lets
	// any artifact — a malformed stream, or one produced by a host with a
	// larger limit — fill the volume before anything notices. The reader is
	// given one byte past the maximum so an artifact exactly at the limit
	// still imports while an oversized one is detected.
	//
	// The shard is stopped by definition here, so its per-shard
	// Config.MaxSnapshotSize is not available; the package default is the
	// applicable ceiling.
	maxSize := config.DefaultMaxSnapshotSize
	written, copyErr := io.Copy(f, io.LimitReader(r, int64(maxSize)+1))
	if copyErr == nil && uint64(written) > maxSize {
		copyErr = &SnapshotTooLargeError{ShardID: shardID, MaxSize: maxSize}
	}
	if copyErr != nil {
		closeFile("copy_error")
		return copyErr
	}
	if syncErr := f.Sync(); syncErr != nil {
		closeFile("sync_error")
		return syncErr
	}
	closeFile("write_complete")
	return nil
}

// discardImportDir removes an import's STAGING directory when the import will
// never be recorded in LogDB. A removal failure is logged and otherwise
// ignored: the caller is already returning the error that caused the discard.
//
// dir must always be a directory the failing import itself created: the staging
// directory (fsys.MkdirTemp), or the canonical destination once the publishing
// rename has moved that staging directory onto it. It was once called with the
// canonical, index-named destination UNCONDITIONALLY, which an import does not
// necessarily own: a failed import at a colliding index then deleted the shard's
// PRE-EXISTING snapshot outright while LogDB still referenced it. What makes the
// post-publish call safe is that the caller has PROVED ownership — the
// destination-exists precheck and the rename ran under one hold of the
// per-shard import lock — and tracks it in an explicit flag rather than
// inferring it from the path.
func discardImportDir(fsys snapshotFS, shardID uint64, dir string) {
	if rmErr := fsys.RemoveAll(dir); rmErr != nil {
		slog.Debug("snapshot import directory cleanup failed",
			"shard", shardID,
			"dir", dir,
			"error", rmErr)
	}
}

// clampNonNegative converts an activeNodes count to a slice capacity, flooring
// at 0. Teardown is claimed exactly once (CompareAndSwap in StopShard/Close), so
// activeNodes should never be negative; this is defense-in-depth so a monitoring
// call can never panic with "makeslice: cap out of range".
func clampNonNegative(n int64) int {
	if n < 0 {
		return 0
	}
	return int(n)
}

// GetNodeHostInfo returns a snapshot of the host's state for monitoring.
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	info := &NodeHostInfo{
		RaftAddress: h.cfg.RaftAddress,
	}

	info.ShardInfoList = make([]ShardInfo, 0, clampNonNegative(h.activeNodes.Load()))
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
// Used for debugging pipeline stalls. The snapshot gates that can also stall
// a shard are reported by DiagShardGates.
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return false, 0, 0, 0, ErrShardNotReady
	}
	d := engNode.DiagState() // readIndexQLen not exposed in DiagPipelineState
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
	return d.CommitPending, d.ProposalQLen, d.InboxLen, pp, nil
}

// DiagShardGates reports the snapshot gates that suspend a shard's state
// machine work, together with the number of apply closures still in flight.
//
// While recovering is true the shard's state machine is being replaced by a
// received snapshot: it applies nothing and extracts no committed entries,
// though it keeps voting and replicating. While saving is true a snapshot
// capture has quiesced applies. applyInFlight is the drain counter both
// gates wait on before touching the state machine; a non-zero value with a
// gate up means the drain has not completed yet.
//
// Both states are also surfaced through ShardInfo.Health as ShardRecovering
// and ShardSnapshotting.
func (h *Host) DiagShardGates(shardID uint64) (recovering, saving bool, applyInFlight int64, err error) {
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		return false, false, 0, ErrShardNotReady
	}
	d := engNode.DiagState()
	return d.Recovering, d.Saving, d.ApplyInFlight, nil
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

		// Stop the engine FIRST. The engine's step workers are the only
		// producers of snapshot sends (transportSender.SendSnapshot, which
		// does snapshotWg.Add(1)). engine.Stop() closes stepStopC and joins
		// the step workers, after which no new snapshotWg.Add can occur.
		// Stopping the engine before ts.Wait() eliminates the illegal
		// "WaitGroup.Add concurrent with Wait" race (a producer could call
		// Add(1) while Close was already blocked in Wait()).
		if h.engine != nil {
			h.engine.Stop()
		}

		// Drain in-flight snapshot sends that the (now-stopped) step workers
		// launched before exiting. The transport is still up, so these sends
		// complete normally. Because no new sends can be started after
		// engine.Stop(), this Wait() races with no concurrent Add.
		if ts, ok := h.sender.(*transportSender); ok {
			ts.Wait()
		}

		// Stop the transport last, after all snapshot sends have drained,
		// so no send goroutine touches a torn-down transport.
		if h.transport != nil {
			if err := h.transport.Stop(); err != nil {
				slog.Debug("transport stop failed during host close", "error", err)
			}
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
			// Atomically claim teardown so a shard already being torn down by a
			// concurrent StopShard is not unloaded/decremented twice (see the
			// CompareAndSwap rationale in StopShard).
			if !ns.stopped.CompareAndSwap(false, true) {
				return true // claimed by a concurrent StopShard; it owns teardown
			}
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

		// Close each node's state machine to release resources such as
		// on-disk file locks (e.g., Pebble). Without this, a stopped
		// shard would leave its DiskStateMachine open, causing "lock
		// held by current process" errors on Host restart.
		for _, ns := range stoppedNodes {
			if ns.rsm != nil {
				if err := ns.rsm.Close(); err != nil {
					h.logger.Warn("state machine close failed during host close",
						"shard_id", ns.shardID, "error", err)
				}
				// Do NOT nil ns.rsm here. An in-flight reader (SyncRead/
				// StaleRead) may still hold this nodeState and read the rsm
				// field concurrently; writing it would be a data race. The
				// node was already removed from h.nodes (sweep 1) so no new
				// caller can reach it, and Host.Close is sync.Once-guarded so
				// the SM is closed exactly once — nil-ing added nothing but
				// the race. Readers gate on ns.stopped (set in sweep 1).
			}
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
	ns.pendingProposals.Range(func(key, _ any) bool {
		// Claim the entry atomically. pendingProposals is a lock-free
		// sync.Map shared with OnApplied/OnProposalFailed (which use
		// LoadAndDelete). A plain Range+Delete+releaseRef would double-count
		// the ref when a concurrent completion removes the same key first,
		// driving outstanding negative and recycling the RequestState early.
		// Only the goroutine whose LoadAndDelete actually removes the entry
		// completes and releases it.
		value, removed := ns.pendingProposals.LoadAndDelete(key)
		if !removed {
			return true
		}
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
		pp.failed()
		pp.rs.complete(RequestResult{Err: err})
		pp.rs.releaseRef()
		h.completePendingOp()
		return true
	})

	ns.pendingReadsMu.Lock()
	for k, pr := range ns.pendingReads {
		pr.rs.complete(RequestResult{Err: err})
		delete(ns.pendingReads, k)
		pr.rs.releaseRef()
		h.completePendingOp()
	}
	ns.pendingReadsMu.Unlock()

	ns.pendingSnapshotsMu.Lock()
	for k, rs := range ns.pendingSnapshots {
		rs.complete(RequestResult{Err: err})
		delete(ns.pendingSnapshots, k)
		rs.releaseRef()
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

	// Scheduled rotation is optional (interval == 0 disables it); the
	// invocation-threshold check below always runs.
	var scheduledC <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		scheduledC = ticker.C
	}

	// Threshold check runs at a faster cadence to detect invocation-based
	// rotation needs between scheduled rotations.
	thresholdCheck := time.NewTicker(30 * time.Second)
	defer thresholdCheck.Stop()

	for {
		select {
		case <-h.keyRotStopC:
			return false
		case <-scheduledC:
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
		// Rotate fails closed when the barrier has no state persister: a
		// rotation that is not persisted would leave every record written
		// under the new epoch undecryptable after a restart. The condition is
		// a permanent misconfiguration, not a transient failure, so the
		// operator has to be told exactly what is broken and what it costs.
		// It is NOT fatal to the host: reads and writes continue under the
		// current epoch until the AEAD invocation limit is reached.
		h.logger.Error("at-rest key rotation is DISABLED: the barrier rejected the rotation; "+
			"configure a barrier state persister to enable it, otherwise the current epoch key "+
			"is never replaced and writes will eventually fail at the AEAD invocation limit",
			"error", err)
		return
	}
	h.logger.Info("key rotation completed", "epoch", newEpoch)

	if h.cfg.MaxRetainedEpochs > 0 && newEpoch > h.cfg.MaxRetainedEpochs {
		purgeThreshold := newEpoch - h.cfg.MaxRetainedEpochs

		// Clamp the retention-based threshold to the GLOBAL safe-purge
		// floor: the minimum epoch still needed by ANY shard's retained
		// encrypted snapshot or any live WAL record. Without this clamp,
		// rotation alone could purge a DEK still required to decrypt a
		// persisted snapshot or WAL, permanently bricking encrypted data.
		//
		// Fail safe: when the floor cannot be determined (errors scanning
		// snapshot dirs / MinLiveEpoch error, or nothing encrypted is
		// retained), skip purging entirely rather than purge on uncertainty.
		safeFloor, ok := h.engine.GlobalSafePurgeFloor()
		if !ok {
			h.logger.Debug("epoch purge after rotation skipped: global safe floor unknown",
				"purge_threshold", purgeThreshold,
			)
			return
		}
		purgeFloor := purgeThreshold
		if safeFloor < purgeFloor {
			purgeFloor = safeFloor
		}

		purged, purgeErr := h.barrier.PurgeEpochsBefore(purgeFloor)
		if purgeErr != nil {
			h.logger.Error("epoch purge after rotation failed",
				"purge_floor", purgeFloor,
				"retention_threshold", purgeThreshold,
				"global_safe_floor", safeFloor,
				"error", purgeErr,
			)
		} else if purged > 0 {
			h.logger.Info("purged old encryption epochs",
				"purged", purged,
				"purge_floor", purgeFloor,
				"retention_threshold", purgeThreshold,
				"global_safe_floor", safeFloor,
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
		// Claim atomically before completing/releasing: pendingProposals is a
		// lock-free sync.Map shared with OnApplied/OnProposalFailed. Without
		// the LoadAndDelete guard a concurrent completion of the same key
		// would cause a double releaseRef (outstanding goes negative, the
		// RequestState is recycled early). Only the remover acts.
		if _, removed := ns.pendingProposals.LoadAndDelete(key); !removed {
			return true
		}
		pp.failed()
		pp.rs.complete(RequestResult{Err: ErrTimeout})
		pp.rs.releaseRef()
		h.completePendingOp()
		return true
	})
}

// sweepPendingReads expires read requests whose deadline has passed.
//
// When an expired entry is a batch key with coalesced aliases, its
// UNCONFIRMED aliases are expired with it: an unconfirmed alias is
// reachable ONLY through the batch key's pendingRead (OnReadyToRead
// looks up the batch key and walks pr.aliases), so once the batch entry
// is removed a deadline-less alias could never complete and its
// RequestState would never return to the pool. Confirmed aliases are
// left in place — confirmation (which happens for the batch key and all
// aliases atomically under pendingReadsMu) makes each alias
// independently completable by the OnApplied read scan.
func (h *Host) sweepPendingReads(ns *nodeState, now time.Time) {
	ns.pendingReadsMu.Lock()
	for key, pr := range ns.pendingReads {
		deadline := pr.rs.Deadline()
		if deadline.IsZero() || now.Before(deadline) {
			continue
		}
		pr.rs.complete(RequestResult{Err: ErrTimeout})
		delete(ns.pendingReads, key)
		pr.rs.releaseRef()
		h.completePendingOp()
		for _, aliasKey := range pr.aliases {
			aliasPR, ok := ns.pendingReads[aliasKey]
			if !ok || aliasPR.confirmed {
				continue
			}
			aliasPR.rs.complete(RequestResult{Err: ErrTimeout})
			delete(ns.pendingReads, aliasKey)
			aliasPR.rs.releaseRef()
			h.completePendingOp()
		}
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
		rs.releaseRef()
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

	shards := make([]ShardInfo, 0, clampNonNegative(h.activeNodes.Load()))
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
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.AddNode,
		ReplicaID:      replicaID,
		Address:        addr,
	})
}

// RequestRemoveNode requests removing a replica from a shard via Raft
// config change. The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID.
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.RemoveNode,
		ReplicaID:      replicaID,
	})
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
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.AddNonVoting,
		ReplicaID:      replicaID,
		Address:        addr,
	})
}

// RequestAddWitness requests adding a voting witness to a shard.
// Witnesses participate in quorum but do not store a state machine.
// The configChangeIndex should be obtained from
// SyncGetShardMembership().ConfigChangeID.
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error) {
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID: configChangeIndex,
		Type:           proto.AddWitness,
		ReplicaID:      replicaID,
		Address:        addr,
	})
}

// RequestUpdateWitnessPolicy proposes replacing the shard's replicated
// witness policy (config.WitnessConfig semantics: MaxWitnesses caps the
// witness count, RequireFullReplicaMajority keeps the full replicas able
// to form a quorum on their own). The change goes through Raft like any
// membership change, so every replica adopts the new policy atomically at
// the same log position. The new policy must be satisfied by the current
// membership: tightening a policy the shard already violates is rejected
// with ErrTooManyWitnesses or ErrBreaksFullReplicaMajority — reshape the
// membership first, then tighten. configChangeIndex carries the usual
// staleness guard (0 to skip).
func (h *Host) RequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) (*RequestState, error) {
	if policy.MaxWitnesses < 0 {
		return nil, &HostInitError{
			Field: "witness_policy",
			Err:   fmt.Errorf("MaxWitnesses must be >= 0, got %d", policy.MaxWitnesses),
		}
	}
	return h.requestConfigChange(ctx, shardID, proto.ConfigChange{
		ConfigChangeID:             configChangeIndex,
		Type:                       proto.UpdateWitnessPolicy,
		MaxWitnesses:               uint64(policy.MaxWitnesses),
		RequireFullReplicaMajority: policy.GetRequireFullReplicaMajority(),
	})
}

// SyncRequestUpdateWitnessPolicy proposes a witness-policy change and
// blocks until it commits or the context is canceled. See
// RequestUpdateWitnessPolicy.
func (h *Host) SyncRequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) error {
	rs, err := h.RequestUpdateWitnessPolicy(ctx, shardID, policy, configChangeIndex)
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
		// Not loaded here: assume the replica was already retired via
		// RemoveData (which writes the tombstone and runs segment GC), and
		// reclaim whatever is still reclaimable. Returning success keeps the
		// canonical StopShard -> RemoveData -> RequestCompaction sequence
		// working instead of failing on a step whose work already happened.
		//
		// Deliberately NOT dragonboat's CompactEntriesTo(MaxUint64): this
		// host cannot distinguish "removed" from "merely stopped", and a
		// stopped replica's log is still live data it needs on restart —
		// compacting it to MaxUint64 would destroy a healthy replica.
		// RemoveEntriesTo is also unsafe post-RemoveData: it would re-create
		// the compactedTo and metaSegments entries that RemoveNodeData just
		// deleted, pinning a segment for a node that no longer exists.
		// Segment GC alone honors the existing markers and cannot lose data.
		return h.logdb.Compact(shardID, replicaID)
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
		return nil, mapContextError(err)
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
//
//nolint:gocyclo // one funnel for every membership change: role validation, witness-policy gating, cert exclusivity, claim/rollback and the pending-op bookkeeping; splitting it would move validation away from the rollback that pairs with it, which is exactly where this path has produced defects
func (h *Host) requestConfigChange(ctx context.Context, shardID uint64, cc proto.ConfigChange) (*RequestState, error) {
	configChangeIndex := cc.ConfigChangeID
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

	// Pre-validate the config change against the current membership before
	// delivering it to the engine. The Host proposal path reaches
	// membership.apply on commit (Peer.Commit -> processConfigChanges),
	// which deliberately does NOT re-check the voting-member cap so that
	// committed changes always replay. Without this pre-check an invalid
	// change (cap exceeded, duplicate add, last-voter removal) would either
	// have its apply error swallowed — leaving the caller's RequestState to
	// hang until deadline — or, for a 65th voting member, pass apply and then
	// panic rebuildReplicaToBit's invariant. Returning the validation error
	// here gives the caller a prompt, actionable failure instead.
	if err := engNode.Peer().ValidateConfigChange(cc); err != nil {
		return nil, err
	}

	// Re-run the certificate-exclusivity precondition for the address this
	// change introduces.
	//
	// StartShard checks it against the INITIAL members map, which leaves the
	// grow-by-join flow completely unchecked: the seed node starts with only
	// itself (so the peer list is empty after self-filtering) and every joiner
	// starts with an empty map, so no node ever evaluates it. Under a wildcard
	// or shared cluster certificate that means the deployment the check exists
	// to refuse comes up clean, and every in-band authorization the transport
	// performs (SourceAddress clearing, peerEpoch mutation and live-outbound
	// eviction, snapshot-sender binding) is satisfied by every peer for every
	// other peer's address.
	//
	// A membership add is the point at which a new peer host first becomes
	// known to this node, so it is where the precondition can be evaluated at
	// all. Failing here is also the least damaging place to fail: the shard
	// keeps running with its current membership and the operator gets the
	// error on the API call rather than at the next restart.
	if cc.Address != "" {
		if err := h.verifyMemberCertificateExclusivity(map[uint64]string{
			cc.ReplicaID: cc.Address,
		}); err != nil {
			return nil, err
		}
	}

	// Pre-register the address in the transport registry for Add operations.
	// Config changes take effect on commit, and the leader adds the new
	// remote (syncRaftRemotes in Peer.Commit) and starts replicating to it
	// the moment the entry commits. The transport must be able to resolve
	// the new node's address at that point, not later when the entry is
	// applied to the state machine. Without this, the first Replicate
	// messages are silently dropped (registry miss), causing the new node
	// to never receive entries when no subsequent proposals create new log
	// entries to trigger resends.
	//
	// Register overwrites (Swap), so the prior mapping is captured first:
	// AddNode on an EXISTING observer (observer -> voter promotion) targets a
	// replicaID that already has a live registry entry, and an unconditional
	// Remove on rollback would delete the address of a replica that is still
	// a member and still being replicated to. A follower/observer never
	// initiates traffic, so learnSourceAddress cannot heal the deletion and
	// the replica would fall behind until the next committed config change,
	// snapshot recovery, or host restart.
	preRegistered := cc.Address != "" && (cc.Type == proto.AddNode || cc.Type == proto.AddNonVoting || cc.Type == proto.AddWitness)
	var priorAddr string
	var hadPrior bool
	if preRegistered {
		if addr, resolveErr := h.registry.Resolve(shardID, cc.ReplicaID); resolveErr == nil && addr != "" {
			priorAddr, hadPrior = addr, true
		}
		h.registry.Register(shardID, cc.ReplicaID, cc.Address)
	}
	// Roll back the speculative registration if the proposal never reaches
	// the log or never commits. Leaving a stale (replicaID -> addr) mapping
	// for a config change that was never applied could misdirect later
	// traffic to a node that was never actually added. The rollback restores
	// the prior mapping when there was one rather than deleting it.
	//
	// The rollback is skipped when the current mapping is no longer the
	// address this call speculatively wrote: a later config change (or an
	// apply-path registration) has since claimed the entry and must win over
	// a late rollback from this superseded attempt.
	rollbackPreRegister := func() {
		if !preRegistered {
			return
		}
		if cur, resolveErr := h.registry.Resolve(shardID, cc.ReplicaID); resolveErr != nil || cur != cc.Address {
			return
		}
		if hadPrior {
			h.registry.Register(shardID, cc.ReplicaID, priorAddr)
			return
		}
		h.registry.Remove(shardID, cc.ReplicaID)
	}

	buf := make([]byte, cc.Size())
	if _, marshalErr := cc.MarshalTo(buf); marshalErr != nil {
		rollbackPreRegister()
		return nil, &HostInitError{Field: "config_change_marshal", Err: marshalErr}
	}

	proposalKey := h.proposalKeySeq.Add(1)

	deadline, _ := ctx.Deadline()
	rs := newRequestState(ns.requestPool, proposalKey, deadline, h.metrics != nil)

	// Track this operation for drain notification.
	h.addPendingOp()

	// Register pending before delivery to avoid a race where OnApplied
	// fires before the map entry exists. acquireRef pins the RS until
	// this map entry is removed.
	rs.acquireRef()
	ns.pendingProposals.Store(proposalKey, &pendingProposal{
		rs:       rs,
		deadline: deadline,
		// An appended-but-never-committed config change (leader deposed,
		// log truncated) must not leave a registry entry behind for a
		// replica that never became a member.
		onFailure: rollbackPreRegister,
	})

	// Close-vs-configchange TOCTOU guard: the host may have begun closing
	// (and run its pending-request sweeps) between the top-of-method closed
	// check and the Store above, which would orphan the RS in a deleted
	// nodeState's map — a deadline-less caller would then block on
	// rs.Result() forever. Re-check after registering and roll back if
	// shutting down. LoadAndDelete makes the rollback exactly-once relative
	// to a concurrent Close sweep (mirrors proposeEntry).
	if h.closed.Load() || ns.stopped.Load() {
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		// The caller's own reference is dropped whether or not the claim
		// won; a sweep that beat us drops only its own.
		rs.Release()
		rollbackPreRegister()
		return nil, ErrClosed
	}

	entry := proto.Entry{
		Type: proto.EntryConfigChange,
		Key:  proposalKey,
		Cmd:  buf,
	}
	if !engNode.DeliverProposal([]proto.Entry{entry}) {
		// Queue full — claim the entry before rolling back. The sweep and
		// failPendingRequests remove entries with LoadAndDelete-style
		// claims; an unconditional Delete here could pair with a sweep that
		// already completed the entry, double-counting releaseRef (driving
		// outstanding negative) and completePendingOp. Only the claimant
		// rolls back; rs.Release() is the caller's own reference and is
		// dropped unconditionally.
		if _, removed := ns.pendingProposals.LoadAndDelete(proposalKey); removed {
			rs.releaseRef()
			h.completePendingOp()
		}
		rs.Release()
		rollbackPreRegister()
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

	leaderID := ns.leaderID.Load()
	lastApplied := ns.lastApplied.Load()

	return &ShardInfo{
		ShardID:              ns.shardID,
		ReplicaID:            ns.replicaID,
		LeaderID:             leaderID,
		Term:                 ns.term.Load(),
		IsLeader:             ns.isLeader.Load(),
		IsObserver:           ns.cfg.IsObserver,
		IsWitness:            ns.cfg.IsWitness,
		StateMachineType:     ns.smType,
		StateMachineTypeName: proto.StateMachineTypeName(ns.smType),
		Pending:              pending,
		Nodes:                nodesCopy,
		Health:               h.shardHealth(ns, leaderID, lastApplied),
		LastApplied:          lastApplied,
		ConfigChangeIndex:    ns.configChangeIndex.Load(),
	}
}

// shardLaggingThreshold is the number of committed-but-unapplied entries at
// which a loaded, led shard is reported as ShardLagging. It is deliberately
// generous: a healthy shard under load carries a small apply backlog, so the
// threshold only trips when the state machine is falling meaningfully behind.
const shardLaggingThreshold uint64 = 1000

// shardHealth derives the reported health of a loaded shard. ns.health only
// records the coarse lifecycle state (healthy once started, unloaded once
// stopped); the failure, no-leader, and lagging conditions are live and are
// derived here from the engine node so that a wedged shard is not reported
// as healthy. Only a stored ShardHealthy is refined — ShardUnloaded is a
// terminal lifecycle state and must not be overridden.
func (h *Host) shardHealth(ns *nodeState, leaderID, lastApplied uint64) ShardHealth {
	stored := ShardHealth(ns.health.Load())
	if stored != ShardHealthy {
		return stored
	}

	engNode := h.engine.GetNode(ns.shardID)
	if engNode == nil {
		// No engine node to derive live state from; report the stored value.
		return stored
	}
	// The engine trips node.failed on a Peer.Commit error or an apply gap
	// (the apply circuit breaker). The shard is wedged at that point: every
	// proposal times out, so HealthCheck must report degraded rather than ok.
	if engNode.IsFailed() {
		return ShardError
	}
	// A snapshot gate is the most specific answer available: while it is up
	// the replica applies nothing, serves no up-to-date local read, and (for
	// recovery) extracts no committed entries at all. Reporting such a
	// replica as healthy is what makes a snapshot-recovery outage so hard to
	// diagnose — it looks fine from every angle the API exposes. Checked
	// before the leader and lag conditions, which are consequences of it.
	if engNode.IsRecovering() {
		return ShardRecovering
	}
	if engNode.IsSaving() {
		return ShardSnapshotting
	}
	if leaderID == 0 {
		return ShardNoLeader
	}
	// Witnesses do not store log data and never apply, so a committed-vs-
	// applied comparison is meaningless for them.
	if !ns.cfg.IsWitness {
		if committed := engNode.CommittedIndex(); committed > lastApplied &&
			committed-lastApplied >= shardLaggingThreshold {
			return ShardLagging
		}
	}
	return ShardHealthy
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
			CACert:      tc.MTLSConfig.CACert,
			Cert:        tc.MTLSConfig.Cert,
			Key:         tc.MTLSConfig.Key,
			Certificate: tc.MTLSConfig.Certificate,
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
			// Fail closed by default: the checker enforces unless the operator
			// explicitly opted into soft-fail. See RevocationConfig.AllowSoftFail.
			EnforceRevocation: !rc.AllowSoftFail,
			OnCertRevoked:     rc.OnCertRevoked,
		}
	}

	cfg := transport.Config{
		ListenAddress:             hc.ListenAddress,
		AdvertiseAddress:          hc.RaftAddress,
		DeploymentID:              hc.DeploymentID,
		Enable0RTT:                tc.Enable0RTT,
		StreamPoolSize:            tc.StreamPoolSize,
		MaxStreamPoolSize:         tc.MaxStreamPoolSize,
		MaxSnapshotReceiveRate:    tc.MaxSnapshotReceiveRate,
		MaxConcurrentSnapshotRecv: tc.MaxConcurrentSnapshotReceives,
		MaxSnapshotReceiveMemory:  tc.MaxSnapshotReceiveMemory,
		MaxMessageReceiveMemory:   tc.MaxMessageReceiveMemory,
		SendBatchMaxSize:          tc.SendBatchMaxSize,
		MaxDecompressedSize:       tc.MaxDecompressedSize,
		MaxIncomingConnections:    tc.MaxIncomingConnections,
		MaxConnectionsPerIP:       tc.MaxConnectionsPerIP,
		MaxStreamsPerConnection:   tc.MaxStreamsPerConnection,
		StreamReadIdleTimeout:     tc.StreamReadIdleTimeout,
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
//
// It reports whether the LogDB held ANY durable state for the replica, which is
// what tells a RESTART from a shard's first start. See the persisted variable
// for why recovered membership cannot answer that question.
func (h *Host) recoverLogReader(lr *logdb.LogReader, shardID, replicaID uint64) (bool, error) {
	if h.logdb == nil {
		return false, nil
	}

	// persisted reports whether the LogDB holds ANY durable Raft state for this
	// replica: a snapshot record, a hard state, or log entries. It is the
	// discriminator between the shard's FIRST start on this replica and a
	// RESTART, and membership cannot serve that purpose: membership is persisted
	// ONLY alongside a snapshot record (memdb.applyUpdate and waldb both set it
	// inside the u.Snapshot.Index > 0 branch), so a replica that has been
	// running — even one fully caught up — recovers no membership at all until
	// it snapshots, and is otherwise indistinguishable from a bootstrap.
	var persisted bool

	// Step 1: Restore snapshot metadata. This sets markerIndex,
	// markerTerm, and membership on the LogReader.
	ss, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil && err != logdb.ErrNoSnapshot {
		return false, err
	}
	if err == nil && ss.Index > 0 {
		persisted = true
		// Resolve the snapshot data file path and size so the raft
		// layer can send InstallSnapshot to followers after recovery.
		// The LogDB stores the snapshot directory; the data file is
		// always "snapshot.dat" inside that directory.
		var snapFilePath string
		var snapFileSize uint64
		if ss.Filepath != "" {
			dataPath := filepath.Join(ss.Filepath, "snapshot.dat")
			if fi, statErr := h.snapshotFsys().Stat(dataPath); statErr == nil {
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
			return false, applyErr
		}
	}

	// Step 2: Restore hard state (term, vote, commit).
	state, membership, err := h.logdb.ReadState(shardID, replicaID)
	if err != nil && err != logdb.ErrNoState {
		return false, err
	}
	if err == nil {
		persisted = true
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
		return false, err
	}
	if entryCount > 0 {
		persisted = true
		if rangeErr := lr.SetRange(firstIndex, entryCount); rangeErr != nil {
			return false, rangeErr
		}
	}

	return persisted, nil
}

// logdbMembershipToProto converts a logdb.Membership to a proto.Membership.
// Both types have identical field layouts; this function performs a deep copy
// of all maps to prevent aliasing between the LogDB storage layer and the
// in-memory raft state.
func logdbMembershipToProto(m logdb.Membership) proto.Membership {
	result := proto.Membership{
		ConfigChangeID:             m.ConfigChangeID,
		MaxWitnesses:               m.MaxWitnesses,
		RequireFullReplicaMajority: m.RequireFullReplicaMajority,
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

	// Disable fsync when NoSyncWAL is set (testing only — never production).
	if cfg.NoSyncWAL {
		opts = append(opts, waldb.WithNoSync(true))
	}

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
