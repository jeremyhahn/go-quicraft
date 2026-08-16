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

package waldb

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// DefaultShardCount is the number of WAL directory shards used to reduce
// fsync contention. ShardID is hashed to one of these shards.
const DefaultShardCount = 16

// ciphertextEpochSize is the byte size of the epoch prefix in an encrypted
// WAL record. The barrier writes the epoch as an 8-byte big-endian uint64
// at the start of each ciphertext, enabling epoch extraction without
// decryption.
const ciphertextEpochSize = 8

// nodeKey identifies a unique (shardID, replicaID) pair for map lookups.
type nodeKey struct {
	ShardID   uint64
	ReplicaID uint64
}

// metaSegments records the segment ID holding the latest durable State,
// Snapshot, Bootstrap, Compaction-marker, and RemoveNode-tombstone record for
// a single node. These record types are not tracked by any entry index, so the
// shard pins their segments here to prevent segment garbage collection from
// deleting the sole durable copy.
//
// Each record type carries an explicit presence flag rather than treating a
// zero ID as "none": segment IDs are 0-based, so segment 0 is a valid pin and
// must be distinguishable from the absence of a record.
type metaSegments struct {
	state        uint64
	snapshot     uint64
	bootstrap    uint64
	compact      uint64
	remove       uint64
	hasState     bool
	hasSnapshot  bool
	hasBootstrap bool
	hasCompact   bool
	hasRemove    bool
}

// WALBarrier is the encryption interface used by WAL segments. It mirrors
// the methods needed from crypto.Barrier without importing the crypto package.
type WALBarrier interface {
	// Encrypt encrypts plaintext using the current epoch's DEK.
	Encrypt(dst, plaintext []byte) ([]byte, error)

	// Decrypt decrypts ciphertext using the epoch embedded in the prefix.
	Decrypt(dst, ciphertext []byte) ([]byte, error)

	// EncryptWithEpoch encrypts plaintext and returns the ciphertext and epoch.
	EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

	// DecryptForEpoch decrypts ciphertext using a specific epoch's DEK.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// CurrentEpoch returns the current key rotation epoch.
	CurrentEpoch() uint64
}

// walContextBarrier is the context-AAD capability the WAL REQUIRES of any
// configured barrier. Every encrypted WAL record binds its position — WAL
// shard, segment and record ordinal — into the AEAD AAD (see
// walRecordContext), which the plain Encrypt/Decrypt pair cannot express.
//
// It is a separate interface asserted at Open rather than extra methods on
// WALBarrier because WALBarrier is part of the package's published surface and
// is satisfied structurally by the engine's own Barrier interface; widening it
// would break every embedder that passes a narrower barrier through. The
// assertion is on the DYNAMIC type, so the concrete *seal.Barrier every
// production path supplies satisfies it.
//
// The assertion FAILS CLOSED: a configured barrier that cannot bind context
// makes Open return ErrBarrierContextUnsupported. There is deliberately no
// epoch-only fallback — silently writing unbound records would reintroduce
// exactly the splice/reorder exposure the context exists to remove.
type walContextBarrier interface {
	// EncryptWithContext seals plaintext with contextAAD folded into the AEAD
	// AAD alongside the epoch. The context is not stored in the ciphertext.
	EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error)

	// DecryptWithContext opens a ciphertext sealed under the same contextAAD.
	// A different context — or none — fails authentication.
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)

	// IsHardwareMode reports whether the barrier delegates to a hardware-backed
	// encryptor. It says nothing about whether that encryptor carries AAD;
	// SupportsContextAAD is the question the WAL actually needs answered.
	IsHardwareMode() bool

	// SupportsContextAAD reports whether the barrier can ACTUALLY authenticate
	// the per-record context. It is true for a software barrier and for a
	// delegated encryptor that carries AAD end to end; false for a delegated
	// encryptor that cannot, and in no-encryption mode. Records are bound to
	// their (shard, segment, ordinal) position exactly when it is true.
	SupportsContextAAD() bool

	// ContextBindingResolved reports whether SupportsContextAAD is currently an
	// ANSWER rather than the absence of one. It is false while the barrier is
	// sealed or uninitialized: no encryptor is installed then, so the
	// capability does not exist yet.
	//
	// The WAL resolves the capability ONCE, at open, and every record it writes
	// for the life of the process depends on that answer. SupportsContextAAD
	// alone cannot be cached, because false collapses "cannot bind" and "cannot
	// say yet" into one bit — and caching the second as the first is precisely
	// how a node that starts sealed ends up writing epoch-only records forever,
	// even after being unsealed with a fully AAD-capable barrier. The WAL
	// therefore refuses to latch anything until this is true; see
	// ErrBarrierContextUnresolved.
	ContextBindingResolved() bool
}

// legacyContextBarrier is the method set walContextBarrier had BEFORE
// ContextBindingResolved was added to it. It exists only to make the assertion
// failure diagnosable.
//
// The assertion is on the dynamic type, so an out-of-tree barrier that
// satisfied the old set — which is every embedder that wrote one against the
// previous release — now fails it and gets a hard startup failure. "Barrier
// does not support context-bound encryption" is actively misleading in that
// case: the barrier supports it perfectly well and is missing one reporting
// method. Naming the method turns a mystifying refusal to boot into a one-line
// fix. See docs/operations/compatibility.md.
type legacyContextBarrier interface {
	EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error)
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
	IsHardwareMode() bool
	SupportsContextAAD() bool
}

// barrierContextAssertionError explains why a configured barrier failed the
// walContextBarrier assertion, distinguishing a barrier that is simply not
// context-capable from one that only lacks the newly required
// ContextBindingResolved method.
func barrierContextAssertionError(barrier WALBarrier) error {
	if _, ok := barrier.(legacyContextBarrier); ok {
		return ErrBarrierContextBindingUnreportable
	}
	return ErrBarrierContextUnsupported
}

// plaintextModeReporter is an OPTIONAL capability a barrier may implement to
// distinguish the two reasons it cannot bind a record's position:
// no-encryption mode, where there is no AEAD and so nothing a position could be
// bound to, versus a delegated encryptor that IS protecting the records but
// cannot authenticate where they belong. Only the second is a security
// limitation; only the Open-time warning differs. A barrier that does not
// implement this is reported as the latter, the conservative reading.
type plaintextModeReporter interface {
	IsPlaintextMode() bool
}

// maxCiphertextExpansion is the largest number of bytes the seal barrier's
// envelope can add to a record's plaintext. It bounds the framed length used to
// pick a record's landing segment BEFORE the record is sealed, which is what
// makes the segment ID in the context-AAD final at encryption time.
//
// COUPLING: this mirrors pkg/seal's envelope, whose per-record layout is
// [epoch:8][algID:1][nonce:N][ciphertext+tag]. The AEAD ciphertext is exactly
// as long as the plaintext, the tag is 16 bytes for every supported algorithm,
// and N is at most 24 (XChaCha20-Poly1305; AES-256-GCM and ChaCha20-Poly1305
// use 12). Those sizes are unexported in pkg/seal, so the bound lives here and
// TestCiphertextExpansionWithinBound pins it against a real barrier.
//
// Over-estimating is safe by construction: segment.CanFit is monotonic in the
// length it is given, so a slightly generous bound can only rotate a segment a
// few bytes early, never overflow one.
//
// DELEGATED BARRIERS: an encryptor that owns its own envelope (hardware, KMS)
// is not bound by this figure. Under-estimating it cannot corrupt anything —
// a record that no longer fits its reserved landing fails closed with
// ErrLandingSegmentOverflow rather than being written at a position its
// context-AAD does not name — but a delegated encryptor whose envelope exceeds
// this bound will surface as that error on records near a segment boundary.
// Such an encryptor should be paired with a segment size that leaves headroom.
const maxCiphertextExpansion = 8 + 1 + 24 + 16

// LogDBMetrics is the metrics interface consumed by the WAL DB for write
// latency, fsync latency, compaction latency, and byte throughput. When nil,
// all metric collection is skipped. The concrete implementation lives in
// the public quicraft package to avoid an import cycle.
type LogDBMetrics interface {
	ObserveWriteLatency(d time.Duration)
	ObserveFsyncLatency(d time.Duration)
	ObserveCompactionLatency(d time.Duration)
	IncWriteBytes(n int64)
}

// Option is a functional option for configuring a DB instance.
type Option func(*dbOptions)

// dbOptions holds configuration for a single WAL shard database.
type dbOptions struct {
	blockSize       int
	maxFileSize     int64
	noSync          bool
	barrier         WALBarrier
	diskMonMaxBytes uint64
	diskMonOpts     []DiskMonitorOption
	metrics         LogDBMetrics
	fs              FS
}

// defaultDBOptions returns options with production defaults.
func defaultDBOptions() dbOptions {
	return dbOptions{
		blockSize:   DefaultOptions.BlockSize,
		maxFileSize: DefaultOptions.MaxFileSize,
		noSync:      false,
		fs:          defaultFS(),
	}
}

// WithBlockSize sets the WAL block size.
func WithBlockSize(size int) Option {
	return func(o *dbOptions) {
		o.blockSize = size
	}
}

// WithMaxFileSize sets the maximum segment file size before rotation.
func WithMaxFileSize(size int64) Option {
	return func(o *dbOptions) {
		o.maxFileSize = size
	}
}

// WithNoSync disables fsync after writes. Only use for testing.
func WithNoSync(noSync bool) Option {
	return func(o *dbOptions) {
		o.noSync = noSync
	}
}

// WithBarrier routes every WAL record through the given barrier: sealed before
// writing, opened during recovery. The barrier must be initialized and unsealed
// before any WAL operations.
//
// It does NOT, on its own, mean the records are encrypted. A barrier running
// StrategyNone installs an identity pass-through, and the records reach the
// medium as plaintext. That configuration is deliberately still ACCEPTED here
// rather than refused: no-encryption mode is a supported barrier mode (Open
// already resolves and announces it -- see resolveBarrierContextBinding), the
// WAL is not the layer that decides whether a deployment requires at-rest
// encryption, and refusing it would break every caller that wires one barrier
// through unconditionally and selects the strategy elsewhere.
//
// What the WAL owes such a caller is the truth, not a refusal: AtRestProtection
// reports AtRestUnprotected for a plaintext-mode barrier, which is what lets the
// host's at-rest requirement fail closed at the layer that actually holds the
// policy.
func WithBarrier(b WALBarrier) Option {
	return func(o *dbOptions) {
		o.barrier = b
	}
}

// WithDiskMonitorOpts configures a DiskMonitor on the DB using the
// provided DiskMonitorOption values. The monitor is only created if
// maxBytes > 0. The WAL directory is supplied automatically.
func WithDiskMonitorOpts(maxBytes uint64, monOpts ...DiskMonitorOption) Option {
	return func(o *dbOptions) {
		o.diskMonMaxBytes = maxBytes
		o.diskMonOpts = monOpts
	}
}

// WithMetrics enables metrics collection on the WAL DB.
func WithMetrics(m LogDBMetrics) Option {
	return func(o *dbOptions) {
		o.metrics = m
	}
}

// WithFS sets the filesystem implementation used for all file and directory
// operations. Production code uses the default osFS which delegates to the
// os package with platform-optimized sync. Test code can inject alternative
// implementations for error injection and fault simulation.
func WithFS(fs FS) Option {
	return func(o *dbOptions) {
		o.fs = fs
	}
}

// DB is the WAL-based LogDB implementation. It stores Raft log entries,
// hard state, bootstrap, and snapshot metadata across sharded WAL directories.
//
// Design:
//   - 16 directory shards based on shardID hash for write parallelism.
//   - Each shard has its own active segment, entry index, and mutex.
//   - Entries are serialized using a fixed binary layout.
//   - The WAL provides durability; in-memory maps serve reads.
//   - On recovery, all state is reconstructed from WAL replay.
type DB struct {
	dir     string
	opts    dbOptions
	barrier WALBarrier
	shards  [DefaultShardCount]*walShard
	diskMon *DiskMonitor
	metrics LogDBMetrics
	closed  atomic.Bool
	// poisoned fail-stops the DB after an fsync failure. A failed
	// fdatasync leaves written-but-unsynced bytes whose durability the
	// kernel no longer guarantees (it may mark dirty pages clean without
	// writing them), so every subsequent write is refused with
	// ErrDBPoisoned and the process must restart into WAL crash recovery.
	poisoned atomic.Bool
}

// poison marks the DB fail-stopped after an fsync failure. Idempotent;
// the first call logs the cause at Error level.
func (db *DB) poison(cause error) {
	if db.poisoned.CompareAndSwap(false, true) {
		slog.Error("waldb: fsync failure — poisoning database (fail-stop); "+
			"all further writes will be refused, restart required",
			"dir", db.dir,
			"error", cause,
		)
	}
}

// writeGate returns the error that must abort a write operation: ErrClosed
// when the DB is closed, ErrDBPoisoned after a fail-stop.
func (db *DB) writeGate() error {
	if db.closed.Load() {
		return logdb.ErrClosed
	}
	if db.poisoned.Load() {
		return ErrDBPoisoned
	}
	return nil
}

// walShard is a single WAL directory shard. Each shard manages its own
// segment file rotation, in-memory entry store, state, bootstrap, and
// snapshot metadata.
//
// # Locking
//
// Two locks split the shard by what they actually protect:
//
//   - mu guards the SEGMENT FILE state — activeSegment, activeSegmentID,
//     nextSegmentID, knownSegments, marshalBuf, batchMode, closed — and
//     therefore every record write, rotation, fsync and rollback. It is the
//     writer lock: it makes a SaveState batch atomic per WAL segment across
//     its whole lifecycle (checkpoint -> write -> fsync -> commit/rollback).
//   - dataMu guards the IN-MEMORY maps that serve reads: indexes, entries,
//     states, bootstraps, snapshots, compactedTo, metaSegments,
//     committedSegmentHWM, minWriteEpoch and segmentEpochs.
//
// The split exists so a reader never waits on an fsync. Readers
// (IterateEntries, ReadState, EntryRange, GetSnapshot, GetBootstrap,
// CompactedTo, ListNodeInfo, MinLiveEpoch) touch only in-memory maps — never
// a segment — so they take dataMu.RLock ALONE. With 16 WAL shards serving
// 10k+ Raft groups, taking mu for a read would stall every group hashing to
// the shard for the duration of the batch's fdatasync.
//
// Every mutator of a dataMu field already holds mu (all of them are commit
// points of a durable write), so a mutator takes both, and a read performed
// while holding mu needs no dataMu. dataMu exists purely to exclude the
// lock-free readers.
//
// LOCK ORDER IS ALWAYS mu THEN dataMu, never the reverse: mu is never
// acquired while dataMu is held. Per-index locks (index.mu) are innermost and
// are only ever taken under mu and/or dataMu. dataMu is never held across any
// I/O — no write, fsync, rotation, directory scan or segment deletion.
type walShard struct {
	mu sync.Mutex

	// dataMu guards the in-memory maps only. See the Locking section above
	// for the field list and the mu -> dataMu ordering rule.
	dataMu sync.RWMutex

	fs   FS
	dir  string
	opts dbOptions

	// shardIdx is this shard's index within the owning DB (0..DefaultShardCount).
	// It is bound into every encrypted record's context-AAD: each shard is its
	// own directory with an INDEPENDENT segment-ID space while all shards share
	// ONE barrier, so without it a record could be spliced from shard-3's
	// 000007.log into shard-9's 000007.log undetected.
	shardIdx uint64

	barrier WALBarrier

	// sealer is barrier's context-AAD capability, resolved once at open. Nil
	// exactly when barrier is nil; openWALShard fails closed otherwise.
	sealer walContextBarrier

	// noContextBinding records that the configured barrier cannot authenticate
	// a caller context — a delegated encryptor without that capability, or
	// no-encryption mode — so records are sealed and opened with no context.
	// It is a DEPLOYMENT-time capability limitation resolved once at open and
	// logged once: never a per-record decision, and never entered because a
	// decrypt failed.
	noContextBinding bool

	// bindingDriftLogged keeps verifyBindingUnchanged's Error line to one per
	// shard. The drift it reports is permanent once it starts, and the check
	// runs on every write, so logging each time would bury the first line —
	// the only one that carries new information — under the rest.
	// Protected by mu, like every other write-path field.
	bindingDriftLogged bool

	// ctxBuf is the reusable scratch buffer for walRecordContext, so building a
	// record's context-AAD allocates nothing on the write hot path. The write
	// side uses it under mu; the read side uses it only from recover(), which
	// runs before the shard is reachable from any DB, so the two never overlap.
	ctxBuf [walContextSize]byte

	metrics         LogDBMetrics
	activeSegment   *segment
	activeSegmentID uint64
	nextSegmentID   uint64

	// poison fail-stops the owning DB after an fsync failure on this
	// shard (see DB.poison). Nil when the shard is used standalone in
	// tests.
	poison func(error)

	// diskMon is the owning DB's disk-usage monitor, used to force a
	// usage check when a segment pre-allocation fails. Nil when no
	// monitor is configured.
	diskMon *DiskMonitor

	// residuals records every generation-boundary residual this shard's
	// recovery raised (see logdb.RecoveryResidual). It is written ONLY by
	// recover(), which runs before the shard is reachable from any DB, and is
	// read-only for the rest of the shard's life: nothing clears it, because a
	// node that came up shorter than the log it acknowledged stays that way
	// until an operator reconciles it, and the operator may not look for hours.
	// Read under dataMu.RLock like any other served state.
	residuals []logdb.RecoveryResidual

	// Per-node entry indexes for range queries and compaction tracking.
	// The map itself is protected by dataMu; each index has its own lock.
	indexes map[nodeKey]*index

	// Per-node entry data store. The WAL provides durability; this map
	// provides fast reads without parsing WAL records. Protected by dataMu.
	entries map[nodeKey]map[uint64]logdb.Entry

	// Per-node hard state. Protected by dataMu.
	states map[nodeKey]logdb.State

	// Per-node bootstrap info. Protected by dataMu.
	bootstraps map[nodeKey]logdb.Bootstrap

	// Per-node snapshot metadata. Protected by dataMu.
	snapshots map[nodeKey]logdb.Snapshot

	// Per-node compaction markers. Protected by dataMu.
	compactedTo map[nodeKey]uint64

	// Set of all segment IDs that have ever been created in this shard.
	// Segment file state: protected by mu.
	knownSegments map[uint64]bool

	// openCreatedSegment names the segment ensureActiveSegment created during
	// openWALShard, and openCreatedSegmentValid says whether there is one. If
	// the Open as a whole then fails, that segment is a brand-new empty file
	// nobody will ever read; closeAfterFailedOpen unlinks it so repeated failed
	// opens — the expected experience while an operator works through a
	// fail-closed condition — do not accumulate one dead file per attempt.
	// Segment file state: protected by mu.
	openCreatedSegment      uint64
	openCreatedSegmentValid bool

	// minWriteEpoch tracks the minimum barrier encryption epoch across
	// all live (non-garbage-collected) records in this shard. It is
	// populated during WAL recovery from ciphertext headers and updated
	// on each encrypted write. After compaction + GC, it is recomputed
	// from the remaining segment epochs. Zero means no encrypted records
	// exist in this shard. Protected by dataMu — MinLiveEpoch reads it
	// without mu.
	minWriteEpoch uint64

	// segmentEpochs tracks the minimum epoch written to each segment ID.
	// This enables efficient recomputation of minWriteEpoch after segment
	// garbage collection without re-scanning ciphertext headers. Protected
	// by dataMu, alongside the minWriteEpoch it derives.
	segmentEpochs map[uint64]uint64

	// metaSegments tracks, per node, the segment ID that holds the latest
	// durable State, Snapshot, Bootstrap, Compaction-marker, and RemoveNode-
	// tombstone record. Unlike entries, these record types are never tracked
	// by any entry index, so without this pin a segment whose entries have all
	// been compacted away could be garbage-collected even though it still
	// holds the sole durable copy of a node's State/Snapshot/Bootstrap/Compact
	// marker or RemoveNode tombstone. Compact and RemoveNodeData fold these
	// IDs into their live-segment set before running GC. A zero ID means no
	// such record has landed for that node. Last write wins. Protected by
	// dataMu.
	metaSegments map[nodeKey]metaSegments

	// committedSegmentHWM is the highest segment ID whose batch has fully
	// completed Phase 3 of SaveState (applyTo) or a non-batch commit
	// (writeBootstrap). It closes a cross-goroutine window: SaveState runs
	// on the commit worker and releases shard.mu between its durable WAL
	// write (Phase 1, which may rotate to a new segment) and its in-memory
	// index/pin update (Phase 3). A concurrent same-shard Compact — driven
	// from the snapshot-pool goroutine or the public Host API — can acquire
	// shard.mu in that window, compute liveSegments without the
	// just-written-but-not-yet-indexed segment, and GC it. GarbageCollect
	// refuses to delete any segment ID greater than this high-water mark, so
	// a segment that is durably written but not yet reflected in the
	// in-memory index/pins is never reclaimed. Advanced only at the commit
	// points, monotonically, and always under shard.mu. Protected by dataMu
	// as well, so it commits atomically with the index/pin updates it
	// covers.
	committedSegmentHWM uint64

	// marshalBuf is a reusable buffer for encoding records.
	marshalBuf []byte

	// batchMode suppresses per-record fdatasync in writeRecord. When true,
	// the caller is responsible for calling syncSegment after the batch of
	// writes completes. This field is protected by mu.
	batchMode bool

	// closed marks the shard shut down. DB.Close only samples db.closed
	// at a writer's entry, so a writer already past that gate can still
	// reach the segment; this flag is set and checked under mu, making
	// shutdown atomic with respect to in-flight writers: once close()
	// holds the lock, no later writer can touch (or recreate) a segment.
	// Protected by mu.
	closed bool
}

// Compile-time interface check.
var _ logdb.LogDB = (*DB)(nil)

// validateOpenOptions checks the functional-option values against the same
// invariants as Options.Validate, before any of them can reach a segment.
//
// The block-size check is load bearing beyond hygiene: an unchecked block size
// smaller than a record header makes segment.bytesNeeded compute a negative
// payload and loop forever, hanging the commit worker inside SaveState.
//
// A segment must also span at least two blocks, or maxRecordPayload is zero and
// no record can ever be written. Beyond that the geometry is the operator's to
// choose: a large BATCH is split across records, and a lone entry too big for
// any record surfaces at the write as the permanent ErrRecordTooLarge.
// Requiring room for a theoretical maxCmdLen entry here would reject
// small-segment deployments that never write one.
func validateOpenOptions(dir string, o dbOptions) error {
	if dir == "" {
		return ErrEmptyWALDir
	}
	if o.blockSize < recordHeaderSize+1 || !isPowerOfTwo(o.blockSize) {
		return ErrInvalidBlockSize
	}
	if o.maxFileSize <= 0 {
		return ErrInvalidMaxFileSize
	}
	if o.maxFileSize/int64(o.blockSize) < 2 {
		return ErrSegmentTooSmallForEntry
	}
	return nil
}

// resolveBarrierContextBinding settles, once for the whole DB, whether a
// configured barrier can positionally bind the records it is about to seal, and
// announces the answer when it is a limitation.
//
// Every encrypted WAL record is positionally bound via context-AAD, so a
// configured barrier MUST be able to carry a context. This fails closed rather
// than degrading to epoch-only binding, and emits the hardware-mode warning
// exactly once per DB instead of once per shard.
//
// Called by Open BEFORE any shard is opened, because openWALShard LATCHES the
// capability into every shard it builds; the refusals below must therefore
// happen while there is still nothing to latch.
func resolveBarrierContextBinding(dir string, barrier WALBarrier) error {
	if barrier == nil {
		return nil
	}
	cb, ok := barrier.(walContextBarrier)
	if !ok {
		return barrierContextAssertionError(barrier)
	}
	// A sealed barrier has no encryptor and therefore no capability to report.
	// Refuse before anything is resolved or announced: latching "cannot bind"
	// here would silently downgrade every record the node ever writes, and the
	// warning below would fire against a barrier whose answer is simply not
	// available yet — noise that teaches operators to ignore the one line that
	// reports a real limitation.
	if !cb.ContextBindingResolved() {
		return ErrBarrierContextUnresolved
	}
	if cb.SupportsContextAAD() {
		return nil
	}
	if pm, isPlain := cb.(plaintextModeReporter); isPlain && pm.IsPlaintextMode() {
		slog.Warn("waldb: barrier performs no encryption (plaintext mode); "+
			"WAL records carry no positional binding because there is no "+
			"authentication to bind it to.",
			"dir", dir,
		)
		return nil
	}
	slog.Warn("waldb: barrier cannot authenticate caller context; WAL "+
		"records are bound to their epoch only, not to their segment and "+
		"position. An at-rest attacker with write access can reorder or "+
		"splice same-epoch records undetected.",
		"dir", dir,
	)
	return nil
}

// createWALRoot creates the WAL root directory and makes the directory entry of
// EVERY level MkdirAll created durable.
//
// POSIX durability requires fsyncing the parent of each newly created
// directory, and MkdirAll may create several levels: fsyncing only the root's
// immediate parent leaves the intermediate dirents un-durable, so a crash after
// an acked SaveState can take the whole tree with it (the node then boots with
// no term and no vote). The deepest PRE-EXISTING ancestor is recorded first so
// the sync walk covers exactly the created chain. Idempotent for an existing
// WAL.
//
// Under noSync the directory is left to be created lazily on first write, which
// is why a fresh noSync database may have no root to stamp geometry into yet.
func createWALRoot(o dbOptions, dir string) error {
	if o.noSync {
		return nil
	}
	existing := deepestExistingDir(o.fs, dir)
	if err := o.fs.MkdirAll(dir, 0o700); err != nil {
		return &DBOpenError{Dir: dir, Err: err}
	}
	if err := syncDirChainFS(o.fs, dir, existing); err != nil {
		return &DBOpenError{Dir: dir, Err: err}
	}
	return nil
}

// establishWALFormat settles what on-disk format a directory without a marker is
// in, BEFORE this Open writes anything into it. It is called only when Open
// found no marker.
//
// Two outcomes, and no third:
//
//   - The directory holds segments. Those bytes were written by a build that
//     predates the marker, and therefore predates the durability sequence in the
//     chunk header, so this build cannot parse them. Refuse
//     (ErrWALFormatMissing). This is a change of posture from when the marker
//     recorded only the block size: an unmarked populated directory used to be
//     adopted once recovery had proved its geometry, because being unmarked was
//     merely the status quo ante. It no longer is — guessing the record framing
//     wrong does not produce a detectable mismatch, it produces an empty clean
//     prefix that the torn-tail repair truncates the whole log to.
//   - The directory holds no segments. This process defines its format, so the
//     marker is written NOW, before the first segment exists.
//
// The ordering is the point. Stamping after the shards were opened left a window
// — a crash, or merely a failed stamp — in which segments existed with no marker
// above them, which the rule above then reads as a pre-marker directory and
// refuses FOREVER. Writing the marker first closes it: either the marker is
// there and segments may follow, or nothing was written at all.
//
// A stamp failure is fatal for the same reason. It used to be logged and ignored,
// on the grounds that refusing an otherwise healthy WAL over a 16-byte metadata
// file was the worse outcome. With the marker also recording the record framing
// that trade reverses: proceeding would create segments this build could never
// prove the format of, bricking the directory on the next Open with no record of
// why. Failing here leaves the directory empty and names the cause.
//
// Under noSync (tests only) createWALRoot leaves the root to be created lazily by
// the first write, so the directory may not exist yet. The root is created here
// rather than skipping the stamp: skipping it means a noSync directory carries no
// format for its whole first life. Only the root is created; the shard
// directories stay lazy.
func establishWALFormat(o dbOptions, dir string) error {
	populated, err := walDirHoldsSegments(o.fs, dir)
	if err != nil {
		return &DBOpenError{Dir: dir, Err: err}
	}
	if populated {
		slog.Error("waldb: refusing to open a WAL directory that holds segments "+
			"but records no on-disk format; it was written by a build that predates "+
			"the format marker and cannot be parsed by this one",
			"dir", dir)
		return &DBOpenError{Dir: dir, Err: ErrWALFormatMissing}
	}
	if _, statErr := o.fs.Stat(dir); statErr != nil {
		if !errors.Is(statErr, fs.ErrNotExist) {
			return &DBOpenError{Dir: dir, Err: statErr}
		}
		if mkErr := o.fs.MkdirAll(dir, 0o700); mkErr != nil {
			return &DBOpenError{Dir: dir, Err: mkErr}
		}
	}
	if wErr := writeWALFormat(o.fs, dir, o.blockSize, o.noSync); wErr != nil {
		return &DBOpenError{Dir: dir, Err: wErr}
	}
	return nil
}

// walDirHoldsSegments reports whether any shard directory under dir contains a
// %06d.log segment file. A WAL root that does not exist, or a shard directory
// that does not, holds nothing; any OTHER error is returned rather than read as
// absence, because mistaking an unreadable directory for an empty one is exactly
// how a populated WAL would get stamped with the wrong format.
func walDirHoldsSegments(filesystem FS, dir string) (bool, error) {
	for i := 0; i < DefaultShardCount; i++ {
		entries, err := filesystem.ReadDir(filepath.Join(dir, shardDirName(i)))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return false, err
		}
		for _, de := range entries {
			if de.IsDir() {
				continue
			}
			if _, ok := parseSegmentFilename(de.Name()); ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// Open creates or opens a WAL database at the given directory. The directory
// structure contains 16 sub-directories (shard-0 through shard-15), each
// holding its own WAL segment files.
//
// The order of the steps below is part of the contract, not an accident: the
// geometry check runs before any segment is touched, and the barrier's
// context-binding capability is resolved before any shard exists to latch it
// into. Both are fixes, not incidental sequencing — see the geometry comment
// inline below and resolveBarrierContextBinding.
func Open(dir string, opts ...Option) (*DB, error) {
	o := defaultDBOptions()
	for _, opt := range opts {
		opt(&o)
	}

	if err := validateOpenOptions(dir, o); err != nil {
		return nil, err
	}

	// Refuse a block size, or an on-disk format version, that differs from the
	// one this directory was written with, BEFORE touching a single segment.
	// Neither is recorded in the segments themselves, so getting either wrong
	// makes ReadAll misparse; that surfaces as CRC failures, and the repair
	// paths would then either fail-stop or truncate a healthy log. Naming the
	// real cause here is the difference between a one-line configuration fix (or
	// a documented upgrade) and a restore-from-peers. See format.go.
	recordedBlockSize, formatPresent, err := readWALFormat(o.fs, dir)
	if err != nil {
		return nil, &DBOpenError{Dir: dir, Err: err}
	}
	if formatPresent && recordedBlockSize != o.blockSize {
		return nil, &DBOpenError{Dir: dir, Err: &WALGeometryMismatchError{
			Dir:        dir,
			Recorded:   recordedBlockSize,
			Configured: o.blockSize,
		}}
	}

	if err := resolveBarrierContextBinding(dir, o.barrier); err != nil {
		return nil, err
	}

	db := &DB{
		dir:     dir,
		opts:    o,
		barrier: o.barrier,
		metrics: o.metrics,
	}

	// Create the DiskMonitor if maxBytes is configured.
	if o.diskMonMaxBytes > 0 {
		db.diskMon = NewDiskMonitor(dir, o.diskMonMaxBytes, o.diskMonOpts...)
	}

	if err := createWALRoot(o, dir); err != nil {
		return nil, err
	}

	// Settle the on-disk format before a single segment is created: either this
	// directory already records one (checked above), or it is empty and this
	// process records one for it now. See establishWALFormat.
	if !formatPresent {
		if err := establishWALFormat(o, dir); err != nil {
			return nil, err
		}
	}

	for i := 0; i < DefaultShardCount; i++ {
		shardDir := filepath.Join(dir, shardDirName(i))
		shard, err := openWALShard(i, shardDir, o, o.barrier, o.metrics)
		if err != nil {
			// Close any shards we already opened, and unlink the empty segment
			// each of them created for a DB that will never exist.
			for j := 0; j < i; j++ {
				db.shards[j].closeAfterFailedOpen()
			}
			return nil, err
		}
		shard.poison = db.poison
		shard.diskMon = db.diskMon
		db.shards[i] = shard
	}

	return db, nil
}

// openWALShard opens a single WAL shard directory. If the directory exists,
// existing segments are recovered and in-memory state is rebuilt. If the
// directory does not exist, the shard is initialized lazily -- no directory,
// segment file, or marshal buffer is created until the first write.
// idx is the shard's index in the owning DB and is bound into every encrypted
// record's context-AAD, so it must be the shard's permanent identity (the same
// index that names its directory), never a per-open ordinal.
func openWALShard(idx int, dir string, opts dbOptions, barrier WALBarrier, metrics LogDBMetrics) (*walShard, error) {
	s := &walShard{
		fs:            opts.fs,
		dir:           dir,
		opts:          opts,
		shardIdx:      uint64(idx),
		barrier:       barrier,
		metrics:       metrics,
		indexes:       make(map[nodeKey]*index),
		entries:       make(map[nodeKey]map[uint64]logdb.Entry),
		states:        make(map[nodeKey]logdb.State),
		bootstraps:    make(map[nodeKey]logdb.Bootstrap),
		snapshots:     make(map[nodeKey]logdb.Snapshot),
		compactedTo:   make(map[nodeKey]uint64),
		knownSegments: make(map[uint64]bool),
		segmentEpochs: make(map[uint64]uint64),
		metaSegments:  make(map[nodeKey]metaSegments),
		// marshalBuf is nil -- allocated lazily in getMarshalBuf.
	}

	// Resolve the barrier's context-AAD capability once, before any record is
	// written or replayed. Fail closed: see walContextBarrier.
	if barrier != nil {
		cb, ok := barrier.(walContextBarrier)
		if !ok {
			return nil, barrierContextAssertionError(barrier)
		}
		// Never latch an unresolved capability. Open already refused a sealed
		// barrier for the whole DB; this repeats the guard because the shard is
		// where the answer is actually recorded, and a caller reaching
		// openWALShard by another route must not be able to pin "no binding"
		// from a barrier that has not been unsealed yet.
		if !cb.ContextBindingResolved() {
			return nil, ErrBarrierContextUnresolved
		}
		s.sealer = cb
		s.noContextBinding = !cb.SupportsContextAAD()
	}

	// If the directory exists, recover any existing data. ONLY ErrNotExist may
	// be read as absence: EACCES, EIO, ENOTDIR, ELOOP or a stale handle leave
	// the directory (and its segments) in place, so treating them as absence
	// would skip recovery, leave nextSegmentID at 0, and let the first write
	// recreate 000000.log with O_TRUNC — erasing the node's term, vote and
	// entries. Fail the open instead.
	if _, err := s.fs.Stat(dir); err == nil {
		if err := s.recover(); err != nil {
			return nil, err
		}
		// Create active segment only if we recovered existing data,
		// so writes to recovered nodes can proceed immediately.
		if len(s.knownSegments) > 0 {
			if err := s.ensureActiveSegment(); err != nil {
				return nil, err
			}
			// Remember what this open created, so a failure in a LATER shard
			// can unlink it instead of leaving an empty segment behind. See
			// closeAfterFailedOpen.
			s.openCreatedSegment = s.activeSegmentID
			s.openCreatedSegmentValid = true
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, &DBOpenError{Dir: dir, Err: err}
	}
	// If directory doesn't exist, shard is lazy -- directory and segment
	// will be created on first write via ensureActiveSegment.

	return s, nil
}

// corruptionError attaches this shard's identity and the segment's clean-prefix
// end to a fail-closed corruption sentinel, and logs the repair triple at Error
// so it is in the node's log even if the caller only prints errors.Is results.
// See SegmentCorruptionError.
func (s *walShard) corruptionError(segID uint64, cleanEnd int64, sentinel error) error {
	err := &SegmentCorruptionError{
		ShardIndex: int(s.shardIdx),
		SegmentID:  segID,
		CleanEnd:   cleanEnd,
		Err:        sentinel,
	}
	slog.Error("waldb: refusing to open a WAL whose corruption is not a torn tail",
		"dir", s.dir,
		"shard_index", err.ShardIndex,
		"segment_id", segID,
		"clean_end", cleanEnd,
		"reason", sentinel.Error(),
	)
	return err
}

// recover scans the shard directory for existing segment files and replays
// their contents to rebuild the in-memory state.
//
// Neither lock is taken anywhere below this call: recover runs inside
// openWALShard, before the shard is reachable from any DB the caller can
// observe, so the entire replay (and the truncation/orphan-discard repair it
// may perform) is exclusive by construction. Locking per replayed record
// would cost a mutex round trip per WAL record on the recovery path and
// protect nothing. Every mutator reachable from a LIVE shard does take both
// locks; see the walShard Locking section.
func (s *walShard) recover() error {
	dirEntries, err := s.fs.ReadDir(s.dir)
	if err != nil {
		return &DBOpenError{Dir: s.dir, Err: err}
	}

	// Collect and sort segment IDs.
	var segIDs []uint64
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		segID, ok := parseSegmentFilename(de.Name())
		if !ok {
			continue
		}
		segIDs = append(segIDs, segID)
		s.knownSegments[segID] = true
		// Advance nextSegmentID past EVERY observed segment BEFORE any replay.
		// Recovery may truncate at a corrupt segment and discard the higher
		// orphan segments; if nextSegmentID only covered replayed segments,
		// new records would reuse the orphans' ID range, and any orphan that
		// lingers on disk (a failed delete, or a deletion resurrected by a
		// crash before the directory fsync) would be ordered AFTER freshly
		// written segments on the next recovery — its stale HardState would
		// win last-writer-wins replay and regress term/vote.
		if segID >= s.nextSegmentID {
			s.nextSegmentID = segID + 1
		}
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	// Replay each segment to rebuild all in-memory state.
	for i, segID := range segIDs {
		cleanEnd, err := s.replaySegment(segID)
		if err != nil {
			// Bytes above the damage that this writer cannot have produced.
			// Checked first: it is not a claim about which of this node's
			// records were lost, so it must not be reported as one.
			if errors.Is(err, errRecordSequenceRegression) {
				return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
					segID, cleanEnd, ErrRecordSequenceRegression)}
			}
			// Corruption below records that a completed fdatasync already
			// acknowledged is never an interrupted write, not even in the final
			// segment. Handled before the truncation path below so the
			// "highest-numbered segment" test cannot license destroying data
			// that provably reached the medium.
			if errors.Is(err, errMidSegmentCorruption) {
				return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
					segID, cleanEnd, ErrMidSegmentCorruption)}
			}
			if errors.Is(err, errRecoveryTruncated) {
				// Prefix-truncation is correct ONLY for a torn tail, which can
				// exist only in the HIGHEST-numbered segment: rotateSegment
				// fdatasyncs a segment in full before any higher one is created,
				// so a non-final segment was already durably acked end to end.
				// Corruption there is bit rot / a lying fsync / tampering — NOT a
				// torn tail — and every segment above it holds committed entries
				// and hard state. Truncating it and discarding the higher
				// segments would silently lose committed data and regress
				// term/vote (two votes in one term). Fail-stop instead and let
				// the operator restore from snapshot/peers, the same posture
				// WALDecryptError and ErrUnknownPayloadType already take.
				if i != len(segIDs)-1 {
					return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
						segID, cleanEnd, ErrNonFinalSegmentCorruption)}
				}
				// Corruption in the FINAL segment with NOTHING decodable after
				// it (the mid-segment case was rejected above) is a legal torn
				// tail: this segment's clean prefix is recovered; do NOT replay
				// higher segments (prefix-validity rule).
				//
				// The repair runs HIGH TO LOW, and the order is a crash-safety
				// requirement, not a preference.
				//
				// Any higher-numbered segments are unreachable under the
				// prefix-validity rule, so delete them FIRST (and fsync the
				// directory, which discardSegmentsAbove does and now reports the
				// failure of). Only then truncate the corrupt segment to its
				// recovered clean end, with its own fsync last. Every
				// intermediate crash state is then a valid prefix: the corrupt
				// segment still carries its damaged tail, so the next recovery
				// stops at exactly the same offset and repeats exactly the same
				// repair.
				//
				// Truncating first is what a crash cannot survive. Segment K cut
				// back to the clean end ends at a record boundary — no damage
				// left to detect — so a restart before the higher segments were
				// deleted replays K in full and then K+1..N, resurrecting the
				// very records the prefix rule had just decided must never come
				// back, with a hole where the cut region was.
				//
				// Truncating at all is what stops the corrupt tail being frozen
				// forever: without it every later restart re-detects the damage,
				// and the discard deletes every segment written since the
				// previous restart — committed entries, hard state (term/vote
				// regression, i.e. a double vote), snapshot metadata and
				// compaction markers all lost, recurring on every boot.
				//
				// A discard failure is fatal: an intact orphan left behind would
				// replay its stale records on the next restart.
				//
				// NOTE: reaching here means i == len(segIDs)-1 (the non-final
				// check above returned otherwise), so segIDs[i+1:] is always
				// EMPTY and this discard is a no-op today. It is kept because the
				// prefix rule and the discard are separate invariants: whatever
				// set of segments recovery decides is unreachable must be gone
				// before the low segment is cut, and encoding that here means a
				// future relaxation of the non-final rule cannot silently skip
				// it. Repair, which shares this code, does pass a non-empty set.
				if dErr := s.discardSegmentsAbove(segIDs[i+1:]); dErr != nil {
					return dErr
				}
				if tErr := s.truncateCorruptSegment(segID, cleanEnd); tErr != nil {
					return &RecoveryError{SegmentID: segID, Err: tErr}
				}
				// The deletions are made durable by discardSegmentsAbove's own
				// directory fsync and the truncation by the file fsync inside
				// truncateCorruptSegment, so this is belt and braces rather than
				// the only proof. It is kept because an unproven repair replays
				// as corruption again on the next restart, and a second fsync of
				// an already-clean directory costs one syscall once per boot.
				if sErr := syncDirFS(s.fs, s.dir); sErr != nil {
					return &RecoveryError{SegmentID: segID, Err: sErr}
				}
				break
			}
			return err
		}
		// Every segment whose replay completed is fully reflected in the
		// in-memory index and meta pins, so it is safe for GC to consider.
		// Seed the commit high-water mark to the highest such segment ID so a
		// Compact that runs before any new write on a freshly reopened DB is
		// not over-conservative and can still reclaim dead segments.
		s.advanceCommittedHWM(segID)
	}

	// Every segment has been replayed, so the per-replica ranges the residual
	// report names are the ranges the node is actually coming up with.
	s.finalizeResiduals()

	return s.reconcileCompactionFloors()
}

// reconcileCompactionFloors repairs (or refuses) a recovered state whose entry
// index starts ABOVE the recovered compaction floor.
//
// The floor is durable only as a WAL marker record, but its effect — the segment
// holding the compacted entries being unlinked — is irreversible. Tear the block
// holding that marker and replay comes back with a LOWER floor (often 0) while
// the entries it covered are physically gone. Nothing else re-derives it.
//
// That state FAILS OPEN, which is what makes it dangerous: the index itself is
// self-consistent (a missing PREFIX keeps span == count, unlike an interior hole),
// so EntryRange accepts it and the node starts. Reads below the real floor then
// return ErrLogGapDetected instead of ErrCompacted — and db.go's own comment on
// the monotonic guard spells out the consequence: "raft would never fall back to
// InstallSnapshot". The regressed floor also defeats that guard, so a LOWER floor
// can afterwards be accepted and made durable.
//
// The floor is re-derived from the REBUILT INDEX (firstIndex-1), never from the
// snapshot index: the snapshot pool deliberately retains CompactionOverhead
// entries below the snapshot for cheap follower catch-up, so anchoring at the
// snapshot would mark live, deliberately-kept entries unreadable and force a full
// InstallSnapshot on every lagging follower after any restart.
//
// The snapshot is the VALIDITY CHECK on that derivation, not its source: the
// advance is accepted only when a recorded snapshot actually covers the missing
// prefix. Otherwise the prefix is unexplained data loss, and deriving a floor
// from the index would let it masquerade as legitimate compaction — a replica
// would then acknowledge entries whose state nothing ever captured. That case
// fails closed, loudly.
func (s *walShard) reconcileCompactionFloors() error {
	for key, idx := range s.indexes {
		first, _, count := idx.Bounds()
		if count == 0 {
			continue
		}
		floor := s.compactedTo[key]
		if first.Index <= floor+1 {
			continue // contiguous with the floor: nothing missing
		}
		// A prefix is absent. Repair ONLY when a recorded snapshot covers it,
		// which is the lost-marker case (compaction always follows a snapshot).
		//
		// Deliberately NOT an error otherwise: an absent prefix with no snapshot
		// is INDISTINGUISHABLE from a node that legitimately starts at a high
		// index — RemoveNodeData followed by a rewrite produces exactly that
		// shape, and rejecting it would refuse to start healthy replicas
		// (TestRemoveNodeDataThenRewrite). Making that case fail closed needs the
		// floor to be durable independently of one WAL block, which is a format
		// change, not a recovery heuristic.
		derived := first.Index - 1
		snap, ok := s.snapshots[key]
		if !ok || snap.Index < derived {
			continue
		}
		slog.Warn("waldb: compaction floor lost below a snapshot-covered prefix; "+
			"re-deriving it from the recovered entry index",
			"shard_id", key.ShardID,
			"replica_id", key.ReplicaID,
			"recovered_floor", floor,
			"derived_floor", derived,
			"snapshot_index", snap.Index,
			"first_entry_index", first.Index)
		s.compactedTo[key] = derived
	}
	return nil
}

// discardSegmentsAbove removes WAL segment files left unreachable after a
// prefix-truncation recovery. Their IDs are dropped from knownSegments, any
// meta-segment pin pointing at a discarded ID is cleared, the files are
// deleted, and the directory is fsynced so the deletions survive a crash.
//
// An orphan that can be neither removed nor emptied is fatal (RecoveryError):
// an intact orphan holds CRC-valid records past the recovered prefix, and if
// it survives into the next restart its stale HardState replays and can
// regress term/vote (a node could vote twice in one term). When Remove fails,
// the fallback truncates the orphan to zero length and fsyncs it — a
// zero-length segment replays to nothing, which is equivalent to deletion for
// recovery purposes. recover() advances nextSegmentID past every observed
// segment before replay, so even a lingering orphan can never be ordered
// after freshly written segments.
//
// Clearing meta pins is purely tidiness. A pin set during replay of a
// now-discarded higher segment would otherwise survive as a phantom: a
// non-existent segment ID that addMetaSegmentsToLive folds into liveSegments,
// where GarbageCollect simply never matches it (no file with that ID exists).
// rollbackShard does NOT need this same cleanup: meta pins are set only at the
// Phase-3 commit point, after a successful durable write, so a rolled-back
// batch never reached the point of setting a pin in the first place.
func (s *walShard) discardSegmentsAbove(orphans []uint64) error {
	if len(orphans) == 0 {
		return nil
	}
	for _, segID := range orphans {
		delete(s.knownSegments, segID)
		s.clearMetaSegmentPins(segID)
		name := filepath.Join(s.dir, segmentFilename(segID))
		err := s.fs.Remove(name)
		if err == nil || errors.Is(err, fs.ErrNotExist) {
			continue
		}
		slog.Warn("waldb: failed to delete orphaned segment after truncation recovery; truncating to zero length",
			"dir", s.dir, "segment", segID, "error", err)
		if tErr := s.truncateOrphanToZero(name); tErr != nil {
			return &RecoveryError{SegmentID: segID, Err: tErr}
		}
	}
	// The unlinks are not durable until the directory entry updates are, and
	// this failure is FATAL rather than a warning. recover() happens to repeat
	// the directory fsync fatally a few lines later, but Repair does not: it
	// would return a RepairResult naming the segments it deleted while a crash
	// could still resurrect their directory entries — segments holding CRC-valid
	// records ABOVE the offset the operator authorized truncating at, which is
	// the term/vote regression (a node voting twice in one term) that the
	// directory fsync exists to prevent. Reporting it lets both callers stop.
	if err := syncDirFS(s.fs, s.dir); err != nil {
		slog.Error("waldb: could not make the discard of orphaned WAL segments "+
			"durable; their deletion may not survive a crash",
			"dir", s.dir, "orphans", orphans, "error", err)
		return err
	}
	return nil
}

// truncateCorruptSegment cuts a segment back to the clean prefix that recovery
// actually replayed and fsyncs the file, so the corrupt tail is gone from disk.
// A no-op when the file is already at or below cleanEnd.
//
// The segment is never written to again: recover() advances nextSegmentID past
// every observed segment ID before replay, so ensureActiveSegment always opens
// a fresh, higher-numbered segment. The truncated end therefore need not be
// block-aligned — ReadAll strides the block grid from offset 0 and clamps its
// final read to the file size.
func (s *walShard) truncateCorruptSegment(segID uint64, cleanEnd int64) error {
	name := filepath.Join(s.dir, segmentFilename(segID))
	f, err := s.fs.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if fi.Size() <= cleanEnd {
		return f.Close()
	}
	if tErr := f.Truncate(cleanEnd); tErr != nil {
		_ = f.Close()
		return tErr
	}
	if sErr := f.Sync(); sErr != nil {
		_ = f.Close()
		return sErr
	}
	slog.Warn("waldb: truncated corrupt WAL segment to its recovered clean prefix",
		"dir", s.dir, "segment", segID, "clean_end", cleanEnd, "previous_size", fi.Size())
	return f.Close()
}

// truncateOrphanToZero empties an orphaned segment file that could not be
// removed and fsyncs the result, so the orphan replays to nothing on the next
// recovery. Used as the discardSegmentsAbove fallback when Remove fails.
func (s *walShard) truncateOrphanToZero(name string) error {
	f, err := s.fs.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if tErr := f.Truncate(0); tErr != nil {
		_ = f.Close()
		return tErr
	}
	if sErr := f.Sync(); sErr != nil {
		_ = f.Close()
		return sErr
	}
	return f.Close()
}

// replaySegment opens a segment file read-only, reads all records, and
// replays them to rebuild in-memory state. It returns the clean-prefix end
// offset reported by ReadAll, which recover() uses to repair the segment on
// disk when the returned error is errRecoveryTruncated.
func (s *walShard) replaySegment(segID uint64) (int64, error) {
	name := filepath.Join(s.dir, segmentFilename(segID))
	f, err := s.fs.Open(name)
	if err != nil {
		return 0, &RecoveryError{SegmentID: segID, Err: err}
	}

	// Create a read-only segment wrapper just for ReadAll.
	readSeg := &segment{
		f:         f,
		id:        segID,
		dir:       s.dir,
		blockSize: s.opts.blockSize,
		blockBuf:  make([]byte, s.opts.blockSize),
	}

	records, cleanEnd, readErr := readSeg.ReadAll()

	// Close the file handle directly without segment.Close() which
	// would attempt Truncate on a read-only file.
	if closeErr := f.Close(); closeErr != nil {
		slog.Debug("waldb: file close failed during segment replay",
			"segment_id", segID,
			"dir", s.dir,
			"error", closeErr,
		)
	}

	if readErr != nil {
		return cleanEnd, &RecoveryError{SegmentID: segID, Err: readErr}
	}

	totalSkipped := 0
	for i, rec := range records {
		// Decrypt the record when a barrier is configured.
		replayData := rec
		if s.barrier != nil {
			// Extract the epoch from the ciphertext header (first 8 bytes,
			// big-endian) to maintain minWriteEpoch for safe epoch purging.
			if len(rec) >= ciphertextEpochSize {
				recEpoch := binary.BigEndian.Uint64(rec[:ciphertextEpochSize])
				if recEpoch > 0 {
					if s.minWriteEpoch == 0 || recEpoch < s.minWriteEpoch {
						s.minWriteEpoch = recEpoch
					}
					if existing, ok := s.segmentEpochs[segID]; !ok || recEpoch < existing {
						s.segmentEpochs[segID] = recEpoch
					}
				}
			}

			// Reproduce the record's positional context-AAD. ReadAll returns
			// ASSEMBLED LOGICAL records — First/Middle/Last chunks are joined
			// into one entry before being appended — so i counts exactly what
			// the write side counted with segment.RecordCount, one per
			// writeRecord call. ReadAll also stops at the first corrupt record
			// or zero hole, so the survivors are a clean prefix and no ordinal
			// is ever skipped.
			//
			// There is NO retry without the context if this fails: that
			// fallback would BE the vulnerability it is here to close. A
			// context mismatch is reported exactly like any other
			// authentication failure — fatal, as an undecryptable record
			// always has been.
			//
			// noContextBinding mirrors the write side exactly: a barrier that
			// cannot authenticate a context sealed these records with none and
			// must open them with none. That is the SAME open-time decision the
			// writer made, not a reaction to a failure.
			var contextAAD []byte
			if !s.noContextBinding {
				contextAAD = s.recordContext(segID, uint64(i))
			}
			decrypted, derr := s.sealer.DecryptWithContext(nil, rec, contextAAD)
			if derr != nil {
				// Decryption failures are fatal: silently skipping records
				// would cause undetected data loss. The caller must provide
				// the correct key or handle the error explicitly.
				return cleanEnd, &WALDecryptError{SegmentID: segID, Err: derr}
			}
			replayData = decrypted
		}

		// Every record here already passed its chunk CRC, so a decode failure
		// is corruption or a format skew, not something to skip: a dropped
		// State record lets an OLDER one win last-writer-wins (term/vote
		// regression => double vote) and a dropped Snapshot record boots the
		// node with empty membership. replayRecord therefore reports all such
		// failures as errors and recovery fails loudly.
		skipped, reason, rerr := s.replayRecord(replayData, segID)
		if rerr != nil {
			return cleanEnd, &RecoveryError{SegmentID: segID, Err: rerr}
		}
		if reason != "" {
			slog.Warn("waldb: skipped record during WAL replay",
				"segment_id", segID,
				"record_index", i,
				"reason", reason,
			)
		}
		totalSkipped += skipped
	}

	if totalSkipped > 0 {
		slog.Warn("waldb: WAL replay completed with skipped entries",
			"segment_id", segID,
			"skipped_count", totalSkipped,
		)
	}

	// A durability generation above the corruption that is lower than the clean
	// prefix's is not a state any writer of this segment can produce. Reported
	// on its own sentinel, ahead of the acknowledged-data case, because the
	// remedy differs: the bytes are foreign, so the question is what put them
	// there, not which of this node's records were lost.
	if readSeg.sequenceRegression {
		return cleanEnd, errRecordSequenceRegression
	}

	// The records ABOVE the corruption span a completed fdatasync, so they were
	// acknowledged. The corruption is not this segment's interrupted tail;
	// signal recover() to fail-stop rather than repair, exactly as for a
	// non-final segment. Checked before the plain stoppedAtCorruption case
	// because it is the stricter reading of it.
	if readSeg.ackedDataAfterCorruption {
		return cleanEnd, errMidSegmentCorruption
	}

	// If ReadAll halted at a corrupt/torn record, this segment's clean prefix
	// has been replayed but everything after the corruption — INCLUDING all
	// higher-numbered segments — must NOT be replayed (a WAL is valid only as
	// a prefix). Signal recover() to stop the segment loop here. Reaching this
	// point means the durability generations either found nothing above the
	// damage or placed everything there in the segment's final unsynced batch,
	// so the truncation recover() performs discards no acknowledged record.
	if readSeg.stoppedAtCorruption {
		// The truncation is licensed, but it is not always free: when the
		// records above the damage all carry a generation HIGHER than anything
		// in the clean prefix, an fsync completed inside the bytes that were
		// destroyed, and whatever older-generation records it acknowledged are
		// gone. Recovery proceeds by design; the fact must not evaporate with
		// the log line that announces it.
		if readSeg.generationBoundaryInDamage {
			s.noteGenerationBoundaryResidual(segID, cleanEnd,
				readSeg.prefixGeneration, readSeg.survivingGeneration)
		}
		slog.Warn("waldb: halting recovery after corruption; later segments skipped",
			"segment_id", segID)
		return cleanEnd, errRecoveryTruncated
	}

	return cleanEnd, nil
}

// errRecoveryTruncated signals that replaySegment halted at a corrupt record.
// recover() treats it as a stop condition (not a failure): the clean prefix up
// to and including this segment is recovered; no higher segment is replayed.
var errRecoveryTruncated = errors.New("waldb: recovery truncated at corruption")

// errMidSegmentCorruption signals that replaySegment halted at a corrupt
// record above which the surviving records span a completed fdatasync, so they
// were acknowledged. recover() turns it into ErrNonFinalSegmentCorruption's
// sibling, ErrMidSegmentCorruption, and fails the open.
var errMidSegmentCorruption = errors.New("waldb: corruption with acknowledged data after it")

// errRecordSequenceRegression signals that replaySegment found, above a
// corruption, a record whose durability generation is LOWER than one already
// replayed from the clean prefix. recover() turns it into
// ErrRecordSequenceRegression and fails the open.
var errRecordSequenceRegression = errors.New("waldb: record durability sequence regressed")

// ErrNonFinalSegmentCorruption is returned (wrapped in a RecoveryError) when
// recovery finds corruption in a segment that is NOT the highest-numbered one.
// A torn tail can only exist in the final segment — a lower segment was fully
// fdatasynced before any higher one was created and everything above it is
// committed — so prefix-truncating it would silently destroy committed entries
// and regress term/vote. Recovery fails-stop; restore from snapshot/peers.
var ErrNonFinalSegmentCorruption = errors.New("waldb: corruption in a non-final WAL segment; committed data above it would be lost — restore from snapshot/peers")

// ErrMidSegmentCorruption is returned (wrapped in a RecoveryError) when
// recovery finds corruption that is followed, in the SAME segment, by records
// that still decode. Being in the highest-numbered segment is not enough to
// call corruption a torn tail: a torn write leaves nothing readable behind it,
// so decodable bytes further down are records the medium accepted and the node
// acknowledged — bit rot, a misdirected write, a lying fsync or tampering hit
// the middle of a live segment. Truncating to the clean prefix would discard
// every one of them plus the newest HardState, so the node would boot with a
// shorter log than it acknowledged (Log Matching / Leader Completeness) and a
// stale term/vote it could then cast again (Election Safety). Recovery
// fail-stops instead; restore from snapshot/peers.
var ErrMidSegmentCorruption = errors.New("waldb: WAL corruption with acknowledged records after it; not an interrupted write — committed data would be lost — restore from snapshot/peers")

// ErrRecordSequenceRegression is returned (wrapped in a RecoveryError) when
// recovery finds, above a corruption, a record whose durability generation is
// LOWER than one already replayed below it. A segment's generation counter only
// ever rises, so no writer of this WAL produced that layout: the bytes are a
// misdirected write from elsewhere on the device, a partial restore of an older
// file over a newer one, or deliberate tampering. Recovery cannot classify the
// damage and refuses to guess — truncating could destroy acknowledged records,
// and replaying could admit records this node never wrote. Restore from
// snapshot/peers, or authorize the loss explicitly with Repair.
var ErrRecordSequenceRegression = errors.New("waldb: WAL record durability sequence goes backwards; these bytes were not written by this log — restore from snapshot/peers")

// SegmentCorruptionError carries the exact location of a corruption that
// recovery refused to repair on its own, so the operator has every value Repair
// needs without having to find them by hand.
//
// This exists because "restore from snapshot/peers" is not a complete answer: on
// a single-node deployment there are no peers to restore from. The node must
// therefore be told precisely which shard, which segment and which offset, so
// that an operator who decides the loss is acceptable can authorize exactly that
// truncation and nothing wider. It unwraps to the sentinel it was built with, so
// errors.Is checks against ErrMidSegmentCorruption,
// ErrNonFinalSegmentCorruption and ErrRecordSequenceRegression all work.
//
// It used to be reachable from an ordinary power loss, because a torn
// multi-block write and rot below acknowledged data were indistinguishable. The
// durability sequence separated them, and the torn write now repairs itself, so
// reaching this error means damage that an interrupted write does not explain.
type SegmentCorruptionError struct {
	// ShardIndex is the WAL shard directory holding the segment.
	ShardIndex int

	// SegmentID is the corrupt segment's numeric ID.
	SegmentID uint64

	// CleanEnd is the byte offset the segment's uncorrupted prefix ends at,
	// and the only offset Repair will accept for this segment.
	CleanEnd int64

	// Err is ErrMidSegmentCorruption, ErrNonFinalSegmentCorruption or
	// ErrRecordSequenceRegression.
	Err error
}

// Error implements the error interface.
func (e *SegmentCorruptionError) Error() string {
	return e.Err.Error() + " (shard " + itoa(e.ShardIndex) + ", segment " +
		itoa(int(e.SegmentID)) + ", clean prefix ends at " + itoa(int(e.CleanEnd)) +
		"); if the loss is acceptable, authorize it with waldb.Repair"
}

// Unwrap returns the sentinel so errors.Is against ErrMidSegmentCorruption or
// ErrNonFinalSegmentCorruption holds.
func (e *SegmentCorruptionError) Unwrap() error {
	return e.Err
}

// Payload type bytes that prefix each WAL record to identify its content.
const (
	// payloadTypeEntriesV1 is the legacy entry format (44-byte header,
	// no session fields). It is never written anymore; encountering it
	// during replay is a fatal recovery error (see replayRecord) because
	// replaying it would zero the session fields and silently break
	// at-most-once delivery. The byte value stays reserved so a V1 WAL
	// is detected explicitly instead of being misparsed.
	payloadTypeEntriesV1 byte = 0x01
	payloadTypeState     byte = 0x02
	// payloadTypeSnapshotV1 is the original snapshot format (no
	// MaxWitnesses/PolicyFlags in the membership section). It is never
	// written anymore; encountering it during replay is a fatal recovery
	// error (see replayRecord) because a V1 record has no version
	// discriminator of its own and misparsing it with the current layout
	// would garble the witness policy or silently drop the node's
	// membership. The byte value stays reserved so a V1 WAL is detected
	// explicitly instead of being misparsed.
	payloadTypeSnapshotV1 byte = 0x03
	payloadTypeBootstrap  byte = 0x04
	payloadTypeCompact    byte = 0x05
	payloadTypeRemoveNode byte = 0x06
	// payloadTypeEntries is the current entry format (76-byte header
	// including the four session fields).
	payloadTypeEntries byte = 0x07
	// payloadTypeSnapshotV2 is the witness-policy snapshot format, whose
	// membership section carries MaxWitnesses and PolicyFlags but no
	// leading version byte. It is never written anymore and is fatal on
	// replay for the same reason as V1: its membership section starts with
	// ConfigChangeID, so parsing it with the V3 layout would consume that
	// field's low byte as a version byte and shift every subsequent read.
	payloadTypeSnapshotV2 byte = 0x08
	// payloadTypeSnapshotV3 is the membership-version-byte snapshot format,
	// whose fixed region ends at FilePathLen with no Compression field. It
	// is never written anymore and is fatal on replay: its FilePathLen sits
	// where the V4 layout reads Compression, so parsing it with the current
	// layout would read the path length as a compression type and then slice
	// the record at the wrong offset.
	payloadTypeSnapshotV3 byte = 0x09
	// payloadTypeSnapshot is the current snapshot format, which adds the
	// artifact Compression field to the fixed region. The distinct payload
	// type is the record-level version discriminator, matching the
	// payloadTypeEntriesV1/payloadTypeEntries precedent.
	payloadTypeSnapshot byte = 0x0a
)

// ErrLegacyEntryFormat is returned when recovery encounters a WAL entry
// record written in the pre-session-field (V1) format. Such a WAL cannot
// be replayed safely: V1 records lack the Key/ClientID/SeriesID/
// RespondedTo fields, so replaying them would disable session dedup for
// every recovered entry. The node must be restored from a snapshot or
// re-synced from its peers.
var ErrLegacyEntryFormat = errors.New(
	"waldb: WAL contains legacy (pre-session-field) entry records; " +
		"restore from snapshot or re-sync this node from its peers")

// ErrLegacySnapshotFormat is returned when recovery encounters a WAL
// snapshot record written in the pre-witness-policy (V1) layout. Such a
// record cannot be replayed safely: it lacks the MaxWitnesses/PolicyFlags
// fields and carries no version discriminator, so parsing it with the
// current layout would either be rejected as truncated (silently starting
// the node with EMPTY membership and no snapshot) or misparse the
// membership maps. The node must be restored from a snapshot or re-synced
// from its peers.
var ErrLegacySnapshotFormat = errors.New(
	"waldb: WAL contains legacy (pre-witness-policy) snapshot records; " +
		"restore from snapshot or re-sync this node from its peers")

// ErrLegacySnapshotFormatV2 is returned when recovery encounters a WAL
// snapshot record written in the witness-policy (V2) layout, whose
// membership section has no leading version byte. Parsing it with the
// current layout would consume the ConfigChangeID low byte as the section
// version and shift every subsequent field, so it is rejected outright.
// The node must be restored from a snapshot or re-synced from its peers.
var ErrLegacySnapshotFormatV2 = errors.New(
	"waldb: WAL contains legacy (pre-membership-version-byte) snapshot " +
		"records; restore from snapshot or re-sync this node from its peers")

// ErrLegacySnapshotFormatV3 is returned when recovery encounters a WAL
// snapshot record written in the pre-compression (V3) layout. Its fixed
// region has no Compression field, so parsing it with the current layout
// would consume FilePathLen as the compression type and slice the filepath
// at the wrong offset. Recovering the snapshot's compression by guessing is
// exactly the failure this field exists to prevent, so the record is
// rejected outright.
var ErrLegacySnapshotFormatV3 = errors.New(
	"waldb: WAL contains legacy (pre-compression-field) snapshot records; " +
		"restore from snapshot or re-sync this node from its peers")

// ErrUnsupportedSnapshotCompression is returned when a snapshot record
// declares a compression algorithm this binary cannot decompress. Recovering
// the artifact would hand the state machine bytes in an unknown encoding, so
// replay fails instead.
var ErrUnsupportedSnapshotCompression = errors.New(
	"waldb: snapshot record declares an unsupported compression algorithm")

// ErrUnsupportedMembershipVersion is returned when a current-format
// snapshot record carries a membership section whose version byte is not
// membershipSectionVersion. Fatal: the section's fields cannot be located
// without knowing its layout, and accepting an empty membership instead
// would strip the node's configuration.
var ErrUnsupportedMembershipVersion = errors.New(
	"waldb: snapshot record membership section has unsupported version")

// replayRecord decodes a raw WAL record and updates the in-memory state.
// Returns the number of individual entries/records skipped due to
// malformed data, and the reason for any top-level skip. A non-nil error
// is fatal to recovery (the WAL cannot be replayed safely at all).
func (s *walShard) replayRecord(rec []byte, segID uint64) (skipped int, reason string, err error) {
	if len(rec) < 1 {
		return 1, "empty record", nil
	}

	payloadType := rec[0]
	data := rec[1:]

	switch payloadType {
	case payloadTypeEntries:
		// A malformed entry batch is fatal, not a skip: the record's CRC
		// already passed, so a mid-record decode failure is corruption
		// that would otherwise leave a silent hole in the replayed log
		// while later records still replay (Log Matching violation).
		if rerr := s.replayEntries(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeEntriesV1:
		// Gate the legacy on-disk format hard: misparsing a V1 record with
		// the V2 layout would garble indices and zero session fields.
		return 0, "", ErrLegacyEntryFormat
	case payloadTypeState:
		// Fatal for the same reason as an entry batch, only worse: a skipped
		// State record lets an OLDER state win last-writer-wins replay,
		// regressing Term/Vote and allowing a double vote in one term
		// (Election Safety violation).
		if rerr := s.replayState(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeSnapshot:
		// Fatal: a skipped snapshot record boots the node with empty
		// membership and no snapshot metadata.
		if rerr := s.replaySnapshot(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeSnapshotV1:
		// Gate the legacy snapshot layout hard: a V1 record parsed with
		// the current layout would be dropped as truncated, silently
		// booting the node with empty membership and no snapshot.
		return 0, "", ErrLegacySnapshotFormat
	case payloadTypeSnapshotV2:
		// Same gate for the witness-policy layout that predates the
		// membership version byte.
		return 0, "", ErrLegacySnapshotFormatV2
	case payloadTypeSnapshotV3:
		// Same gate for the layout that predates the artifact Compression
		// field: its fixed region is two bytes shorter, so every offset from
		// FilePathLen onward would be misread.
		return 0, "", ErrLegacySnapshotFormatV3
	case payloadTypeBootstrap:
		if rerr := s.replayBootstrap(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeCompact:
		if rerr := s.replayCompaction(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeRemoveNode:
		if rerr := s.replayRemoveNode(data, segID); rerr != nil {
			return 0, "", rerr
		}
	default:
		// Payload types are written only by this package and are covered by
		// the chunk CRC, so an unknown type is corruption or a format skew
		// and must fail-stop. The dominant real-world cause is opening a
		// KEYED WAL with barrier==nil: barrier ciphertext starts with an
		// 8-byte big-endian epoch whose first byte is 0x00 for every
		// realistic epoch, so with no decrypt attempted EVERY record would
		// decode as payload type 0x00. Skipping them booted the node as a
		// brand-new empty replica that votes with no memory of prior votes
		// and accepts a log it had already superseded.
		return 0, "", ErrUnknownPayloadType
	}
	return skipped, "", nil
}

// Entry encoding layout (payloadTypeEntries, format V2):
// [ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8]
// [Key:8][ClientID:8][SeriesID:8][RespondedTo:8][OwnerID:8][CmdLen:4][Cmd:N]
const entryRecordHeaderSize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 // 84 bytes

// maxCmdLen is the maximum allowed command length for a single WAL entry
// during replay. This matches proto.MaxEntrySize (8 MB) and prevents
// corrupted length prefixes from causing unbounded memory allocation.
const maxCmdLen = 8 * 1024 * 1024 // 8 MB

// replayEntries rebuilds in-memory entries from a serialized entry batch.
// Any malformed entry aborts recovery with a typed error: the batch was
// written as one CRC-protected record, so a mid-record decode failure is
// corruption the chunk CRC did not explain, and skipping the remainder
// while later records/segments still replay would leave a silent hole in
// the log (a Log Matching violation served to followers as a shortened,
// nil-error range). Entries decoded before the abort may already be stored;
// that is irrelevant because the caller fails the whole recovery.
func (s *walShard) replayEntries(data []byte, segID uint64) error {
	pos := 0
	// Track the batch's node key and index span so a leader-overwrite (the
	// first replayed index already present in the entry map) can prune the
	// now-orphaned higher indices after the batch is stored — mirroring the
	// write path (applyTo). idx.Append already truncates the superseded index
	// suffix, but the entry MAP would otherwise retain indices above the
	// overwrite's last index (memory-only, unreachable via the index-driven
	// range) until compaction. All entries in one record share a single node
	// key (writeEntryChunk writes one key's batch), so the first entry is
	// representative.
	var (
		batchKey      nodeKey
		batchStarted  bool
		overwrite     bool
		lastRaftIndex uint64
	)
	for pos < len(data) {
		if len(data)-pos < entryRecordHeaderSize {
			slog.Error("waldb: truncated entry header during replay",
				"segment_id", segID,
				"position", pos,
				"remaining", len(data)-pos,
				"required", entryRecordHeaderSize,
			)
			return ErrTruncatedEntryRecord
		}

		shardID := binary.LittleEndian.Uint64(data[pos:])
		replicaID := binary.LittleEndian.Uint64(data[pos+8:])
		raftIndex := binary.LittleEndian.Uint64(data[pos+16:])
		term := binary.LittleEndian.Uint64(data[pos+24:])
		entryType := binary.LittleEndian.Uint64(data[pos+32:])
		entryKey := binary.LittleEndian.Uint64(data[pos+40:])
		clientID := binary.LittleEndian.Uint64(data[pos+48:])
		seriesID := binary.LittleEndian.Uint64(data[pos+56:])
		respondedTo := binary.LittleEndian.Uint64(data[pos+64:])
		ownerID := binary.LittleEndian.Uint64(data[pos+72:])
		cmdLen := binary.LittleEndian.Uint32(data[pos+80:])

		// Validate cmdLen against the maximum allowed entry size to
		// prevent corrupted length prefixes from causing unbounded
		// memory allocation during recovery. The write path enforces the
		// same cap (writeEntries), so a stored violation is corruption.
		if cmdLen > maxCmdLen {
			slog.Error("waldb: entry cmdLen exceeds maximum during replay",
				"segment_id", segID,
				"position", pos,
				"cmd_len", cmdLen,
				"max_cmd_len", maxCmdLen,
				"shard_id", shardID,
				"replica_id", replicaID,
				"raft_index", raftIndex,
			)
			return ErrCmdTooLarge
		}

		totalSize := entryRecordHeaderSize + int(cmdLen)
		if len(data)-pos < totalSize {
			slog.Error("waldb: truncated entry command during replay",
				"segment_id", segID,
				"position", pos,
				"cmd_len", cmdLen,
				"remaining", len(data)-pos-entryRecordHeaderSize,
			)
			return ErrTruncatedEntryRecord
		}

		var cmd []byte
		if cmdLen > 0 {
			cmd = make([]byte, cmdLen)
			copy(cmd, data[pos+entryRecordHeaderSize:pos+entryRecordHeaderSize+int(cmdLen)])
		}

		key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

		// Update the entry index.
		idx := s.getOrCreateIndex(key)
		idx.Append(indexEntry{
			Index:     raftIndex,
			Term:      term,
			SegmentID: segID,
			Offset:    0, // Not used for reads.
			Size:      int64(totalSize),
		})

		// Store the full entry data.
		nodeEntries := s.entries[key]
		if nodeEntries == nil {
			nodeEntries = make(map[uint64]logdb.Entry)
			s.entries[key] = nodeEntries
		}
		// Detect a leader-overwrite once, on the first replayed index: if it is
		// already present the batch supersedes an earlier one at the same node.
		if !batchStarted {
			batchKey = key
			_, overwrite = nodeEntries[raftIndex]
			batchStarted = true
		}
		lastRaftIndex = raftIndex
		nodeEntries[raftIndex] = logdb.Entry{
			Index:       raftIndex,
			Term:        term,
			Type:        entryType,
			Key:         entryKey,
			ClientID:    clientID,
			SeriesID:    seriesID,
			RespondedTo: respondedTo,
			OwnerID:     ownerID,
			Cmd:         cmd,
		}

		pos += totalSize
	}
	// Prune entries above the overwrite's last index. idx.Append already
	// truncated the superseded index suffix; this keeps the entry map in sync
	// so a stale range (e.g. entries 61-100 left after a 50-60 conflict
	// rewrite) does not linger in memory. Runs only on an actual overwrite.
	if overwrite {
		nodeEntries := s.entries[batchKey]
		for k := range nodeEntries {
			if k > lastRaftIndex {
				delete(nodeEntries, k)
			}
		}
	}
	return nil
}

// State encoding layout: [ShardID:8][ReplicaID:8][Term:8][Vote:8][Commit:8]
const stateRecordSize = 8 + 8 + 8 + 8 + 8 // 40 bytes

// replayState rebuilds per-node hard state. The record is fixed-width, so
// any other length is corruption and returns ErrTruncatedStateRecord.
func (s *walShard) replayState(data []byte, segID uint64) error {
	if len(data) != stateRecordSize {
		slog.Error("waldb: malformed state record during replay",
			"segment_id", segID, "length", len(data), "expected", stateRecordSize)
		return ErrTruncatedStateRecord
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	term := binary.LittleEndian.Uint64(data[16:])
	vote := binary.LittleEndian.Uint64(data[24:])
	commit := binary.LittleEndian.Uint64(data[32:])

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.states[key] = logdb.State{Term: term, Vote: vote, Commit: commit}
	// Pin the segment so post-recovery compaction does not delete the sole
	// durable copy of this state. Segments replay in ascending ID order, so
	// the last replayed wins — matching last-writer-wins at write time.
	s.setStateSegment(key, segID)
	return nil
}

// Snapshot encoding layout:
// [ShardID:8][ReplicaID:8][Index:8][Term:8][FileSize:8][OnDiskIndex:8]
// [Epoch:8][Compression:2][FilePathLen:4][FilePath:N]
// [MembershipVersion:1][ConfigChangeID:8][MaxWitnesses:8][PolicyFlags:1]
// [NumAddresses:4]{ [ReplicaID:8][AddrLen:4][Addr:N] }...
// [NumObservers:4]{ [ReplicaID:8][AddrLen:4][Addr:N] }...
// [NumWitnesses:4]{ [ReplicaID:8][AddrLen:4][Addr:N] }...
// [NumRemoved:4]{ [ReplicaID:8] }...
// [NumIdentities:4]{ [ReplicaID:8][IdentityLen:4][Identity:N] }...
// [SessionExpiryEntries:8][MaxTotalSessions:8][MaxSessionsPerClient:8]
// [MaxSessionsPerOwner:8]
// [NumRemovedIdentities:4]{ [ReplicaID:8][IdentityLen:4][Identity:N] }...
const snapshotRecordBaseSize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 2 + 4 // 62 bytes

// membershipSectionV1 is the one layout revision this binary reads or writes
// for the membership section of a snapshot record.
//
// It is 1 because the project is PRE-RELEASE and every format in the repository
// is pinned at 1 until the first production release: a layout change is made in
// place, the consuming repos are rebuilt to match, and development clusters are
// wiped and re-synced rather than migrated. Earlier development commits wrote
// other layouts under other numbers; those bytes are gone with the clusters that
// held them. See the compatibility contract in the proto package comment.
const membershipSectionV1 byte = 1

// membershipSectionVersion is the layout revision this binary WRITES for the
// membership section of a snapshot record, stored as the section's first byte,
// and — replay gating on exact equality — the only one it READS. It mirrors the
// proto.Membership version byte and moves in lockstep with payloadTypeSnapshot:
// the payload type discriminates records, this byte discriminates the section
// within a record.
//
// The byte is retained through the pinned period rather than dropped, for the
// reason the proto package comment gives: a format with no in-band
// discriminator cannot be superseded at 1.0, and a section that skipped it
// would begin at ConfigChangeID, whose low byte is indistinguishable from a
// version. Refusing anything that is not this value turns a commit skew into a
// clean recovery failure instead of fields read at the wrong offsets.
const membershipSectionVersion byte = membershipSectionV1

// membershipFixedSize is the size of the membership section's fixed region:
// Version(1) + ConfigChangeID(8) + MaxWitnesses(8) + PolicyFlags(1).
//
// The replicated session policy is a TAIL here and part of the FIXED region in
// proto.Membership. The two layouts are independent by design (this section
// carries PolicyFlags as one byte where proto spends eight), and the difference
// is deliberate: this decoder parses straight-line from a fixed offset, so
// growing the fixed region moves every map, while appending to the tail does
// not.
const membershipFixedSize = 1 + 8 + 8 + 1 // 18 bytes

// minMembershipSize is the smallest well-formed membership section: the fixed
// region, six empty map count headers and the session policy. It is the length
// guard replay applies BEFORE reading the version byte, so that a section too
// short to hold the layout is reported as a torn record rather than being read
// past its own end.
//
// It equals membershipSize of an empty membership, and must: there is one
// layout, every field is written unconditionally, and the read floor and the
// written size are therefore the same number by construction.
const minMembershipSize = membershipFixedSize + 6*4 + membershipSessionPolicySize // 74 bytes

// membershipAddrMapSize returns the total serialized size for an address map:
// [NumEntries:4] + for each entry: [ReplicaID:8][AddrLen:4][Addr:N].
func membershipAddrMapSize(m map[uint64]string) int {
	size := 4 // NumEntries
	for _, addr := range m {
		size += 8 + 4 + len(addr) // ReplicaID + AddrLen + Addr
	}
	return size
}

// membershipRemovedMapSize returns the total serialized size for the removed
// map: [NumRemoved:4] + for each entry: [ReplicaID:8].
func membershipRemovedMapSize(m map[uint64]bool) int {
	return 4 + len(m)*8
}

// membershipSessionPolicySize is the size of the session-policy tail:
// SessionExpiryEntries(8) + MaxTotalSessions(8) + MaxSessionsPerClient(8) +
// MaxSessionsPerOwner(8).
const membershipSessionPolicySize = 4 * 8 // 32 bytes

// membershipSize returns the total serialized size of a logdb.Membership. Every
// field is unconditional: the size and the marshal site must agree exactly, or
// the record overruns its buffer.
func membershipSize(m logdb.Membership) int {
	return membershipFixedSize +
		membershipAddrMapSize(m.Addresses) +
		membershipAddrMapSize(m.Observers) +
		membershipAddrMapSize(m.Witnesses) +
		membershipRemovedMapSize(m.Removed) +
		membershipAddrMapSize(m.Identities) +
		membershipSessionPolicySize +
		membershipAddrMapSize(m.RemovedIdentities)
}

// marshalAddrMap serializes an address map into buf at the given offset.
// Returns the new offset after writing. Layout per entry:
// [ReplicaID:8][AddrLen:4][Addr:N].
func marshalAddrMap(buf []byte, pos int, m map[uint64]string) int {
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(m)))
	pos += 4
	for rid, addr := range m {
		binary.LittleEndian.PutUint64(buf[pos:], rid)
		binary.LittleEndian.PutUint32(buf[pos+8:], uint32(len(addr)))
		copy(buf[pos+12:], addr)
		pos += 12 + len(addr)
	}
	return pos
}

// marshalRemovedMap serializes the removed map into buf at the given offset.
// Returns the new offset after writing.
func marshalRemovedMap(buf []byte, pos int, m map[uint64]bool) int {
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(m)))
	pos += 4
	for rid := range m {
		binary.LittleEndian.PutUint64(buf[pos:], rid)
		pos += 8
	}
	return pos
}

// maxMembershipMapEntries caps the declared entry count of any WAL
// membership or bootstrap address/removed map before allocation. A
// membership map can never exceed the cluster size, which is far below
// this bound; this mirrors proto.MaxMembershipEntries (10000) and prevents
// a corrupted or tampered count prefix from forcing a multi-GB allocation
// during recovery. A record whose declared count exceeds this cap is
// treated as corrupt. The plaintext WAL path relies on this guard because
// chunk CRC32 only detects accidental corruption; the encrypted path is
// additionally AEAD-authenticated.
const maxMembershipMapEntries = 10000

// checkMembershipSize rejects a membership whose any map exceeds
// maxMembershipMapEntries, the cap replay treats as corruption. Keeping the
// write side symmetric with replay is the same rule ErrCmdTooLarge follows.
func checkMembershipSize(m logdb.Membership) error {
	if len(m.Addresses) > maxMembershipMapEntries ||
		len(m.Observers) > maxMembershipMapEntries ||
		len(m.Witnesses) > maxMembershipMapEntries ||
		len(m.Removed) > maxMembershipMapEntries ||
		// Identities and RemovedIdentities are written by the same encoder and
		// read back through the same per-map cap, so omitting them here broke
		// the symmetry this function exists to hold: an oversized map produced
		// a snapshot record that was durable and then rejected as corrupt on
		// replay.
		len(m.Identities) > maxMembershipMapEntries ||
		len(m.RemovedIdentities) > maxMembershipMapEntries {
		return ErrMembershipTooLarge
	}
	return nil
}

// addrMapEntryMinBytes is the minimum on-disk size of one address-map
// entry: [ReplicaID:8][AddrLen:4]. The address bytes are additional.
const addrMapEntryMinBytes = 12

// removedMapEntryMinBytes is the minimum on-disk size of one removed-map
// entry: [ReplicaID:8].
const removedMapEntryMinBytes = 8

// boundedMapHint returns a map preallocation hint that is never larger than
// what the remaining record bytes could physically contain. perEntryMinBytes
// is the smallest valid on-disk size of a single entry, so remaining bytes
// divided by it is a hard upper bound on the real entry count regardless of
// the declared count.
func boundedMapHint(count uint32, remaining, perEntryMinBytes int) int {
	hint := int(count)
	if max := remaining / perEntryMinBytes; hint > max {
		hint = max
	}
	return hint
}

// unmarshalAddrMap deserializes an address map from data at the given offset.
// Returns the populated map and new offset, or -1 if the data is truncated
// or the declared entry count exceeds maxMembershipMapEntries (corruption).
func unmarshalAddrMap(data []byte, pos int) (map[uint64]string, int) {
	if len(data)-pos < 4 {
		return nil, -1
	}
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	if count > maxMembershipMapEntries {
		return nil, -1
	}
	m := make(map[uint64]string, boundedMapHint(count, len(data)-pos, addrMapEntryMinBytes))
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 12 {
			return nil, -1
		}
		rid := binary.LittleEndian.Uint64(data[pos:])
		addrLen := binary.LittleEndian.Uint32(data[pos+8:])
		pos += 12
		// uint64 comparison: int(addrLen) sign-truncates on 32-bit for
		// addrLen >= 2^31, which would pass the guard and panic on the
		// slice below (same class as the replaySnapshot fpLen guard).
		if uint64(len(data)-pos) < uint64(addrLen) {
			return nil, -1
		}
		m[rid] = string(data[pos : pos+int(addrLen)])
		pos += int(addrLen)
	}
	return m, pos
}

// unmarshalRemovedMap deserializes the removed map from data at the given
// offset. Returns the populated map and new offset, or -1 if truncated or
// the declared entry count exceeds maxMembershipMapEntries (corruption).
func unmarshalRemovedMap(data []byte, pos int) (map[uint64]bool, int) {
	if len(data)-pos < 4 {
		return nil, -1
	}
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	if count > maxMembershipMapEntries {
		return nil, -1
	}
	m := make(map[uint64]bool, boundedMapHint(count, len(data)-pos, removedMapEntryMinBytes))
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 8 {
			return nil, -1
		}
		rid := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		m[rid] = true
	}
	return m, pos
}

// replaySnapshot rebuilds per-node snapshot metadata. A parse failure is
// corruption (the chunk CRC already passed) and returns
// ErrTruncatedSnapshotRecord; recovery treats it as fatal because a dropped
// snapshot record boots the node with empty membership.
func (s *walShard) replaySnapshot(data []byte, segID uint64) error {
	if len(data) < snapshotRecordBaseSize {
		slog.Error("waldb: truncated snapshot record during replay",
			"segment_id", segID, "length", len(data), "minimum", snapshotRecordBaseSize)
		return ErrTruncatedSnapshotRecord
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	idx := binary.LittleEndian.Uint64(data[16:])
	term := binary.LittleEndian.Uint64(data[24:])
	fileSize := binary.LittleEndian.Uint64(data[32:])
	onDiskIndex := binary.LittleEndian.Uint64(data[40:])
	epoch := binary.LittleEndian.Uint64(data[48:])
	compression := config.CompressionType(binary.LittleEndian.Uint16(data[56:]))
	fpLen := binary.LittleEndian.Uint32(data[58:])

	// A compression algorithm this binary cannot build a reader for makes the
	// artifact unrecoverable; accepting the record would defer the failure to
	// RecoverFromSnapshot, where it looks like state machine corruption.
	if !compression.IsValid() {
		slog.Error("waldb: snapshot record declares unsupported compression during replay",
			"segment_id", segID, "compression", uint16(compression))
		return ErrUnsupportedSnapshotCompression
	}

	// Bound-check in uint64: on 32-bit platforms int(fpLen) sign-truncates
	// for fpLen >= 2^31 and 60+fpLen wraps in uint32, so an int-domain
	// comparison would pass a corrupt length and panic on the slice below.
	// Once the guard holds, fpLen fits in int (it is <= len(data)).
	if uint64(len(data)) < uint64(snapshotRecordBaseSize)+uint64(fpLen) {
		slog.Error("waldb: snapshot record filepath exceeds record length during replay",
			"segment_id", segID, "length", len(data), "filepath_len", fpLen)
		return ErrTruncatedSnapshotRecord
	}
	fp := string(data[snapshotRecordBaseSize : snapshotRecordBaseSize+int(fpLen)])
	pos := snapshotRecordBaseSize + int(fpLen)

	// Deserialize membership. The writer ALWAYS emits the section, whose
	// minimum well-formed size is the fixed region plus the six map count
	// headers and the session policy. Anything shorter — including a wholly
	// absent section — is a torn record (the chunk CRC should already have
	// rejected it; this is defense in depth) and must NOT be silently accepted
	// as an empty membership.
	var membership logdb.Membership
	if len(data)-pos < minMembershipSize {
		slog.Error("waldb: snapshot record missing membership section during replay",
			"segment_id", segID, "remaining", len(data)-pos, "minimum", minMembershipSize)
		return ErrTruncatedSnapshotRecord
	}
	// Defense in depth behind the payload-type gate, on EXACT equality: a
	// section version other than the one this binary writes means the record is
	// corrupt or was produced by a binary built from a different pre-release
	// commit, and its fields must not be interpreted at this layout's offsets.
	sectionVersion := data[pos]
	if sectionVersion != membershipSectionVersion {
		slog.Error("waldb: snapshot record membership section has unsupported version",
			"segment_id", segID, "version", sectionVersion,
			"supported", membershipSectionVersion)
		return ErrUnsupportedMembershipVersion
	}
	pos++
	membership.ConfigChangeID = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	membership.MaxWitnesses = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	membership.RequireFullReplicaMajority = data[pos] == 1
	pos++

	var ok int
	membership.Addresses, ok = unmarshalAddrMap(data, pos)
	if ok == -1 {
		return ErrTruncatedSnapshotRecord
	}
	pos = ok

	membership.Observers, ok = unmarshalAddrMap(data, pos)
	if ok == -1 {
		return ErrTruncatedSnapshotRecord
	}
	pos = ok

	membership.Witnesses, ok = unmarshalAddrMap(data, pos)
	if ok == -1 {
		return ErrTruncatedSnapshotRecord
	}
	pos = ok

	membership.Removed, ok = unmarshalRemovedMap(data, pos)
	if ok == -1 {
		return ErrTruncatedSnapshotRecord
	}
	pos = ok

	// Identities is the receive-side quorum gate's only authorization input
	// where it is present, so a section that lost it would silently downgrade
	// every member it describes back to the address check.
	membership.Identities, ok = unmarshalAddrMap(data, pos)
	if ok == -1 {
		return ErrTruncatedSnapshotRecord
	}
	pos = ok

	// The session policy. Its length is checked explicitly rather than leaned
	// on minMembershipSize, which bounds the section as a whole and not the
	// remainder at this offset: a record whose maps consumed more than their
	// share would leave fewer than membershipSessionPolicySize bytes here, and
	// the fixed-width reads below would slice past the record.
	if len(data)-pos < membershipSessionPolicySize {
		slog.Error("waldb: snapshot record membership section is missing its session policy",
			"segment_id", segID, "remaining", len(data)-pos,
			"required", membershipSessionPolicySize)
		return ErrTruncatedSnapshotRecord
	}
	membership.SessionExpiryEntries = binary.LittleEndian.Uint64(data[pos:])
	membership.MaxTotalSessions = binary.LittleEndian.Uint64(data[pos+8:])
	membership.MaxSessionsPerClient = binary.LittleEndian.Uint64(data[pos+16:])
	membership.MaxSessionsPerOwner = binary.LittleEndian.Uint64(data[pos+24:])
	pos += membershipSessionPolicySize

	// RemovedIdentities closes the section. It is the deny-list the receive
	// gate evaluates against the AUTHENTICATED connection identity, so a
	// snapshot that lost it would not merely lose information — it would revoke
	// a revocation, and the replica would come back having forgotten which
	// keypairs it ejected.
	membership.RemovedIdentities, ok = unmarshalAddrMap(data, pos)
	if ok == -1 {
		return ErrTruncatedSnapshotRecord
	}
	pos = ok

	// The membership is the final field; any leftover bytes mean the
	// record did not parse cleanly (corruption).
	if pos != len(data) {
		slog.Error("waldb: snapshot record has trailing bytes during replay",
			"segment_id", segID, "parsed", pos, "length", len(data))
		return ErrTruncatedSnapshotRecord
	}

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.snapshots[key] = logdb.Snapshot{
		Index:       idx,
		Term:        term,
		Filepath:    fp,
		FileSize:    fileSize,
		OnDiskIndex: onDiskIndex,
		Epoch:       epoch,
		Compression: compression,
		Membership:  membership,
	}
	// Pin the segment so post-recovery compaction does not delete the sole
	// durable copy of this snapshot. Segments replay in ascending ID order,
	// so the last replayed wins — matching last-writer-wins at write time.
	s.setSnapshotSegment(key, segID)
	return nil
}

// Bootstrap encoding layout:
// [ShardID:8][ReplicaID:8][Join:1][Type:8][NumAddrs:4]
// For each addr: [ReplicaID:8][AddrLen:4][Addr:N]
// [DataDirLen:4][DataDir:N]
//
// The DataDir field is a TRAILING extension: records written before it existed
// end at the address map, and replayBootstrap treats that as DataDir "". A
// record written by this version always carries the length prefix, so a
// zero-length DataDir is still explicit on the wire.
const bootstrapRecordBaseSize = 8 + 8 + 1 + 8 + 4 // 29 bytes

// bootstrapDataDirLenSize is the length prefix of the trailing DataDir field.
const bootstrapDataDirLenSize = 4

// maxBootstrapDataDirLen bounds the DataDir a bootstrap record may declare,
// so a corrupt length cannot drive a huge allocation. It is far above any
// real filesystem path.
const maxBootstrapDataDirLen = 4096

// replayBootstrap rebuilds per-node bootstrap info. A parse failure is
// corruption (the chunk CRC already passed) and returns
// ErrTruncatedBootstrapRecord, which recovery treats as fatal.
func (s *walShard) replayBootstrap(data []byte, segID uint64) error {
	if len(data) < bootstrapRecordBaseSize {
		slog.Error("waldb: truncated bootstrap record during replay",
			"segment_id", segID, "length", len(data), "minimum", bootstrapRecordBaseSize)
		return ErrTruncatedBootstrapRecord
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	join := data[16] != 0
	smType := binary.LittleEndian.Uint64(data[17:])
	numAddrs := binary.LittleEndian.Uint32(data[25:])
	if numAddrs > maxMembershipMapEntries {
		return ErrTruncatedBootstrapRecord
	}

	pos := bootstrapRecordBaseSize
	addrs := make(map[uint64]string, boundedMapHint(numAddrs, len(data)-pos, addrMapEntryMinBytes))
	for i := uint32(0); i < numAddrs; i++ {
		if len(data)-pos < 12 {
			return ErrTruncatedBootstrapRecord
		}
		rid := binary.LittleEndian.Uint64(data[pos:])
		addrLen := binary.LittleEndian.Uint32(data[pos+8:])
		pos += 12
		// uint64 comparison: see unmarshalAddrMap for the 32-bit
		// sign-truncation hazard this avoids.
		if uint64(len(data)-pos) < uint64(addrLen) {
			return ErrTruncatedBootstrapRecord
		}
		addrs[rid] = string(data[pos : pos+int(addrLen)])
		pos += int(addrLen)
	}

	// DataDir is a trailing extension. A record that ends at the address map
	// predates it and carries no DataDir; anything else must parse as the
	// complete field, with no bytes left over (matching the replaySnapshot
	// trailing-byte check).
	//
	// dataDirRecorded reports which of the two it was. "Absent" and "empty" are
	// DIFFERENT statements — empty means the shard roots its snapshot tree under
	// the host's NodeHostDir, absent means nothing is known — and collapsing
	// them lets a caller act on a value no writer ever produced.
	var dataDir string
	var dataDirRecorded bool
	if pos != len(data) {
		if len(data)-pos < bootstrapDataDirLenSize {
			slog.Error("waldb: bootstrap record has trailing bytes during replay",
				"segment_id", segID, "parsed", pos, "length", len(data))
			return ErrTruncatedBootstrapRecord
		}
		dirLen := binary.LittleEndian.Uint32(data[pos:])
		pos += bootstrapDataDirLenSize
		if dirLen > maxBootstrapDataDirLen {
			return ErrTruncatedBootstrapRecord
		}
		// uint64 comparison: see unmarshalAddrMap for the 32-bit
		// sign-truncation hazard this avoids.
		if uint64(len(data)-pos) < uint64(dirLen) {
			return ErrTruncatedBootstrapRecord
		}
		dataDir = string(data[pos : pos+int(dirLen)])
		pos += int(dirLen)
		if pos != len(data) {
			slog.Error("waldb: bootstrap record has trailing bytes during replay",
				"segment_id", segID, "parsed", pos, "length", len(data))
			return ErrTruncatedBootstrapRecord
		}
		dataDirRecorded = true
	}

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.bootstraps[key] = logdb.Bootstrap{
		Addresses:       addrs,
		Join:            join,
		Type:            smType,
		DataDir:         dataDir,
		DataDirRecorded: dataDirRecorded,
	}
	// Pin the segment so post-recovery compaction does not delete the sole
	// durable copy of this bootstrap record.
	s.setBootstrapSegment(key, segID)
	return nil
}

// Compaction encoding layout: [ShardID:8][ReplicaID:8][CompactedTo:8]
const compactionRecordSize = 8 + 8 + 8 // 24 bytes

// replayCompaction applies a compaction marker from WAL replay. The record
// is fixed-width, so any other length is corruption and returns
// ErrTruncatedCompactionRecord, which recovery treats as fatal.
func (s *walShard) replayCompaction(data []byte, segID uint64) error {
	if len(data) != compactionRecordSize {
		slog.Error("waldb: malformed compaction record during replay",
			"segment_id", segID, "length", len(data), "expected", compactionRecordSize)
		return ErrTruncatedCompactionRecord
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	compactedTo := binary.LittleEndian.Uint64(data[16:])

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	// The floor is monotonic during replay too: a WAL written by an older
	// build (or replayed out of segment order) may hold a lower marker after a
	// higher one, and adopting it would resurrect a hole below entries that
	// are already gone. Skipping the marker also keeps the HIGHER marker's
	// segment pinned, so a post-recovery GC cannot reclaim the only record of
	// the real floor.
	if cur, ok := s.compactedTo[key]; ok && compactedTo <= cur {
		return nil
	}
	s.compactedTo[key] = compactedTo
	// Re-pin the marker's segment on recovery (last write wins), so a post-
	// recovery GC cannot reclaim it and reset compactedTo to 0 again.
	s.setCompactSegment(key, segID)

	// Remove compacted entries from the index.
	if idx, ok := s.indexes[key]; ok {
		idx.Compact(compactedTo)
	}

	// Remove compacted entries from the in-memory store.
	if nodeEntries, ok := s.entries[key]; ok {
		for raftIdx := range nodeEntries {
			if raftIdx <= compactedTo {
				delete(nodeEntries, raftIdx)
			}
		}
	}
	return nil
}

// RemoveNode encoding layout: [ShardID:8][ReplicaID:8]
const removeNodeRecordSize = 8 + 8 // 16 bytes

// replayRemoveNode removes all in-memory state for a (shardID, replicaID)
// pair during WAL recovery. This makes RemoveNodeData durable across restarts.
// The record is fixed-width, so any other length is corruption and returns
// ErrTruncatedRemoveNodeRecord, which recovery treats as fatal.
func (s *walShard) replayRemoveNode(data []byte, segID uint64) error {
	if len(data) != removeNodeRecordSize {
		slog.Error("waldb: malformed remove-node record during replay",
			"segment_id", segID, "length", len(data), "expected", removeNodeRecordSize)
		return ErrTruncatedRemoveNodeRecord
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	delete(s.indexes, key)
	delete(s.entries, key)
	delete(s.states, key)
	delete(s.bootstraps, key)
	delete(s.snapshots, key)
	delete(s.compactedTo, key)
	delete(s.metaSegments, key)
	// Re-pin the tombstone's segment on recovery (last write wins) so a
	// post-recovery GC cannot reclaim it while older segments still holding
	// the removed node's records survive under other nodes' pins — replay
	// without the tombstone would resurrect the removed replica (see
	// setRemoveSegment).
	s.setRemoveSegment(key, segID)
	return nil
}

// ensureActiveSegment creates the active segment if one does not exist.
// On first write to a lazy shard, this also creates the shard directory.
// Caller must hold s.mu.
func (s *walShard) ensureActiveSegment() error {
	// Re-check the shutdown gate under the lock: DB.Close samples
	// db.closed only at a writer's entry, so a writer that raced past the
	// entry gate must not resurrect a segment (or write to a recycled fd)
	// after close() tore the shard down.
	if s.closed {
		return logdb.ErrClosed
	}
	if s.activeSegment != nil {
		return nil
	}

	// Lazily create shard directory on first write, then fsync its PARENT (the
	// WAL root) so the shard directory's own entry is durable. The segment
	// file's dirent inside this shard directory is made durable by the
	// syncDirFS(s.dir) below; without also persisting the shard directory entry
	// itself, a crash on a filesystem without a global metadata journal could
	// lose the whole shard directory (and every Raft group hashing to it) even
	// though SaveState fsynced the segment and returned success.
	if err := s.fs.MkdirAll(s.dir, 0700); err != nil {
		return &DBOpenError{Dir: s.dir, Err: err}
	}
	if !s.opts.noSync {
		if err := syncDirFS(s.fs, filepath.Dir(s.dir)); err != nil {
			return &DBOpenError{Dir: s.dir, Err: err}
		}
	}

	segID := s.nextSegmentID
	seg, preallocFailed, err := createSegment(s.fs, s.dir, segID, s.opts.blockSize, s.opts.maxFileSize)
	if err != nil {
		return &SegmentCreateError{SegmentID: segID, Err: err}
	}
	seg.noSync = s.opts.noSync

	// A failed pre-allocation means the ENOSPC backstop is gone; force an
	// immediate disk-usage check so the DiskMonitor can trip ErrDiskFull
	// before writes start landing on a near-full filesystem.
	if preallocFailed && s.diskMon != nil {
		if checkErr := s.diskMon.ForceCheck(); checkErr != nil {
			slog.Warn("waldb: disk check after fallocate failure reports pressure",
				"dir", s.dir, "error", checkErr)
		}
	}

	// Fsync the parent directory to make the new segment's directory entry
	// durable. Without this, a crash on Linux ext4 could lose the new file's
	// directory entry, causing data written to it to disappear on recovery.
	if !s.opts.noSync {
		if err := syncDirFS(s.fs, s.dir); err != nil {
			// Close the segment we just created; the caller cannot use it
			// if we failed to make its directory entry durable.
			if closeErr := seg.Close(); closeErr != nil {
				slog.Debug("waldb: segment close failed after dir sync error",
					"segment_id", segID,
					"dir", s.dir,
					"error", closeErr,
				)
			}
			return err
		}
	}

	s.activeSegment = seg
	s.activeSegmentID = segID
	s.nextSegmentID = segID + 1
	s.knownSegments[segID] = true
	return nil
}

// rotateSegment syncs, closes the current active segment, and creates a new
// one. The explicit Sync before Close ensures all data written to the old
// segment is on stable storage (fdatasync) before the new segment begins
// receiving writes. Without this, a crash after rotation could lose the
// old segment's last records that were only in the OS page cache.
func (s *walShard) rotateSegment() error {
	if s.activeSegment != nil {
		// Sync to stable storage BEFORE closing. Close only flushes
		// the block buffer to the OS page cache and truncates; it does
		// NOT call fdatasync. In batchMode, this is the only path that
		// ensures the old segment's data survives a crash.
		if !s.opts.noSync {
			if err := s.activeSegment.Sync(); err != nil {
				if s.poison != nil {
					s.poison(err)
				}
				return err
			}
		}
		if err := s.activeSegment.Close(); err != nil {
			// segment.Close marks the segment closed before it can fail,
			// so keeping it installed would wedge the shard: every later
			// write would hit ErrClosed with no recovery path. Drop the
			// segment so the next write creates a fresh one via
			// ensureActiveSegment. The data is already durable from the
			// Sync above; a failed flush rewrites bytes Sync persisted,
			// and a failed truncate only leaves pre-allocated zero space,
			// which replays to nothing.
			s.activeSegment = nil
			slog.Warn("waldb: segment close failed during rotation; discarding segment handle",
				"dir", s.dir,
				"segment_id", s.activeSegmentID,
				"error", err,
			)
			return err
		}
		s.activeSegment = nil
	}
	return s.ensureActiveSegment()
}

// getOrCreateIndex returns the entry index for the given node, creating
// one if it does not exist. It mutates the indexes map, so the caller must
// hold s.dataMu for writing (and, on a live shard, s.mu).
func (s *walShard) getOrCreateIndex(key nodeKey) *index {
	idx, ok := s.indexes[key]
	if !ok {
		idx = newIndex()
		s.indexes[key] = idx
	}
	return idx
}

// recordLanding says where a written record came to rest: the segment it was
// appended to (post-rotation, so it is the segment that actually holds the
// bytes) and the barrier epoch its ciphertext header carries. A zero epoch
// means the record is not encrypted — no barrier is configured — and the
// landing contributes nothing to the shard's epoch floor.
//
// writeRecord returns a landing instead of applying it, so the caller can fold
// it into the floor at its own DURABILITY point. See applyEpochLocked.
type recordLanding struct {
	segID uint64
	epoch uint64
}

// applyEpochLocked folds a DURABLE record's write epoch into the shard's epoch
// floor: the minimum epoch overall and the minimum epoch per segment, which is
// what recomputeMinWriteEpoch re-derives the floor from after segment garbage
// collection.
//
// It is deliberately NOT called from writeRecord. Applying the epoch at the
// write would record epochs for records that never commit: a batch that rolls
// back leaves the floor pinned to an epoch no live record was ever encrypted
// under, because rollbackShard drops segmentEpochs only for the intermediate
// segments it unlinks and can restore neither minWriteEpoch nor the checkpoint
// segment's entry. Each writer therefore applies its own landing once its
// record is durable, and a rolled-back batch applies nothing.
//
// Caller must hold s.dataMu for writing (and, on a live shard, s.mu).
func (s *walShard) applyEpochLocked(l recordLanding) {
	if l.epoch == 0 {
		return
	}
	if s.minWriteEpoch == 0 || l.epoch < s.minWriteEpoch {
		s.minWriteEpoch = l.epoch
	}
	if existing, ok := s.segmentEpochs[l.segID]; !ok || l.epoch < existing {
		s.segmentEpochs[l.segID] = l.epoch
	}
}

// commitEpoch is applyEpochLocked for a caller that is not already inside a
// dataMu section. An unencrypted record carries no epoch and takes no lock at
// all, so the barrier-less path stays exactly as cheap as before.
func (s *walShard) commitEpoch(l recordLanding) {
	if l.epoch == 0 {
		return
	}
	s.dataMu.Lock()
	s.applyEpochLocked(l)
	s.dataMu.Unlock()
}

// recordContext builds the positional context-AAD for a record at (segID,
// recordSeq) in this shard, reusing the shard's scratch buffer so the WAL write
// hot path allocates nothing. Both the write side and the replay side call it,
// which is what keeps the two byte-identical.
//
// The returned slice aliases s.ctxBuf and is only valid until the next call.
// Callers hand it straight to the barrier, which reads it during the AEAD
// operation and never retains it. Caller must hold s.mu (or, during recover(),
// own the shard exclusively).
func (s *walShard) recordContext(segID, recordSeq uint64) []byte {
	return walRecordContext(s.ctxBuf[:], s.shardIdx, segID, recordSeq)
}

// verifyBindingUnchanged detects ONE direction of capability drift: a shard
// that latched "cannot bind context" at open, against a barrier that now says
// it can. It is called only from that branch of writeRecord and deliberately
// guards nothing else.
//
// The capability is a deployment property, but it is only VALID WHILE THE
// BARRIER STAYS UNSEALED: Host.EmergencySeal can seal the barrier at runtime,
// and an operator can unseal it again — potentially under a different strategy
// with a different capability. A latched "cannot bind" would then keep
// suppressing the context against a barrier that would have authenticated it,
// which is a downgrade no error surfaces on its own. That is the entire reason
// this exists.
//
// The OPPOSITE direction — a shard writing WITH context whose barrier stops
// being able to authenticate one — needs no check here and does not get one:
// that shard passes a non-empty contextAAD to EncryptWithContext, and a barrier
// that cannot authenticate a context it was handed fails the call rather than
// dropping it. The write fails closed on its own, at the only place that could
// have silently discarded the binding. Adding a symmetric probe would put a
// barrier interface call on the hot path of every correctly configured
// deployment to re-detect something already fatal.
//
// A barrier that is currently sealed reports nothing (not resolved) and is left
// to the encrypt call, which fails closed on its own; only a RESOLVED answer
// that contradicts the latch is treated as drift.
func (s *walShard) verifyBindingUnchanged() error {
	if s.sealer == nil || !s.sealer.ContextBindingResolved() {
		return nil
	}
	if !s.sealer.SupportsContextAAD() {
		return nil
	}
	// Log once. The condition is permanent for the life of the shard and this
	// runs on every subsequent write, so logging unconditionally turns one
	// diagnosis into a flood that buries it. The returned error carries the
	// same information to every caller regardless.
	if !s.bindingDriftLogged {
		s.bindingDriftLogged = true
		slog.Error("waldb: barrier now authenticates caller context but the WAL "+
			"resolved it as unable to at open; refusing to write further unbound records",
			"dir", s.dir)
	}
	return ErrBarrierContextBindingChanged
}

// reserveLanding makes the active segment the one a record of framedLen bytes
// will actually land in, rotating first when the record no longer fits. After
// it returns nil, s.activeSegmentID and s.activeSegment.RecordCount() name the
// record's final position, so they can be sealed into its context-AAD.
//
// framedLen may be an UPPER bound on the record's eventual length;
// segment.CanFit is monotonic in it, so a generous bound only rotates early.
// Caller must hold s.mu.
func (s *walShard) reserveLanding(framedLen int) error {
	err := s.activeSegment.CanFit(framedLen)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrSegmentFull) {
		// ErrRecordTooLarge is permanent and ErrClosed is terminal; neither is
		// helped by a fresh segment.
		return err
	}
	if rotErr := s.rotateSegment(); rotErr != nil {
		return rotErr
	}
	return s.activeSegment.CanFit(framedLen)
}

// writeRecord writes a raw record to the active segment, rotating if the
// segment is full. On a lazy shard, this triggers directory and segment
// creation via ensureActiveSegment. When a barrier is configured, the
// record data is encrypted before writing.
//
// It returns where the record landed. The landing is NOT applied to the epoch
// floor here: the record is not durable yet (in batchMode the fdatasync is the
// caller's, and even outside it the batch may still roll back), so the caller
// applies it at its own commit point. See applyEpochLocked.
//
// On error the returned landing is zero, with one deliberate exception: when
// the record was fully written and only the inline fdatasync failed, the epoch
// is applied here. That record may well be on the medium, and a floor above a
// record recovery still has to decrypt is the one direction that breaks the
// node (WALDecryptError at startup) rather than merely retaining a key.
func (s *walShard) writeRecord(data []byte) (recordLanding, error) {
	if err := s.ensureActiveSegment(); err != nil {
		return recordLanding{}, err
	}

	// Resolve the LANDING segment before encrypting. The record's context-AAD
	// binds the segment ID and the record's ordinal within that segment, so
	// both must be final at seal time; encrypting first and discovering the
	// rotation afterwards would bind the record to a segment it never reached.
	// The framed length is bounded by the plaintext plus the barrier's maximum
	// envelope expansion, which is all segment.CanFit needs (it is monotonic in
	// that length), so the fit decision costs no encryption and the record is
	// sealed exactly once — re-sealing after a rotation would burn a second
	// nonce against the NIST invocation cap for nothing.
	framedLen := len(data)
	if s.barrier != nil {
		framedLen += maxCiphertextExpansion
	}
	if err := s.reserveLanding(framedLen); err != nil {
		return recordLanding{}, err
	}

	// Encrypt the record data when a barrier is configured. The write epoch is
	// parsed out of the ciphertext's own 8-byte big-endian prefix, exactly as
	// the replay path does, but is attributed to a segment only AFTER the write
	// lands so recomputeMinWriteEpoch can never purge a DEK a live record still
	// needs.
	writeData := data
	var writeEpoch uint64
	var positional bool
	if s.barrier != nil {
		// A barrier that cannot authenticate a caller context seals the record
		// with none, keeping only whatever binding it applies on its own. That
		// is decided once at open (see noContextBinding), never per record and
		// never because something failed.
		var contextAAD []byte
		if !s.noContextBinding {
			contextAAD = s.recordContext(s.activeSegmentID, s.activeSegment.RecordCount())
			positional = true
		} else if err := s.verifyBindingUnchanged(); err != nil {
			// Suppressing the context is the only direction the barrier cannot
			// catch for us. The opposite drift — latched "binds" against a
			// barrier that no longer can — is already fail-closed, because
			// EncryptWithContext refuses a non-empty context it cannot
			// authenticate. So the re-check runs ONLY on this branch, which
			// keeps it off the hot path of every correctly configured
			// (software or AAD-capable) deployment.
			return recordLanding{}, &WALEncryptError{Err: err}
		}
		encrypted, err := s.sealer.EncryptWithContext(nil, data, contextAAD)
		if err != nil {
			return recordLanding{}, &WALEncryptError{Err: err}
		}
		writeData = encrypted
		if len(encrypted) >= ciphertextEpochSize {
			writeEpoch = binary.BigEndian.Uint64(encrypted[:ciphertextEpochSize])
		}
	}

	_, err := s.activeSegment.Write(writeData)
	if errors.Is(err, ErrSegmentFull) {
		if positional {
			// Unreachable: reserveLanding pre-checked this exact record against
			// a length bound that can only over-estimate. Moving the record to
			// another segment now would store a ciphertext bound to a position
			// it does not occupy, which recovery could never open — fail
			// instead of writing an unreadable record.
			return recordLanding{}, ErrLandingSegmentOverflow
		}
		// No context was bound (plaintext, or hardware mode), so these bytes
		// are position-independent and may be moved to a fresh segment AS IS.
		// Nothing is re-encrypted.
		if rotErr := s.rotateSegment(); rotErr != nil {
			return recordLanding{}, rotErr
		}
		_, err = s.activeSegment.Write(writeData)
	}
	if err != nil {
		return recordLanding{}, err
	}

	// s.activeSegmentID is the post-rotation segment when a rotation occurred
	// above, so this names the segment that actually holds the record.
	landing := recordLanding{segID: s.activeSegmentID, epoch: writeEpoch}

	if s.metrics != nil {
		s.metrics.IncWriteBytes(int64(len(writeData)))
	}

	if !s.opts.noSync && !s.batchMode {
		fsyncStart := time.Now()
		syncErr := s.activeSegment.Sync()
		if s.metrics != nil {
			s.metrics.ObserveFsyncLatency(time.Since(fsyncStart))
		}
		if syncErr != nil {
			// The record was written but its durability cannot be
			// established. Fail-stop: see DB.poison. The caller returns
			// without reaching its commit point, so apply the epoch here —
			// the record may be on disk and its DEK must stay retained.
			s.commitEpoch(landing)
			if s.poison != nil {
				s.poison(syncErr)
			}
			return landing, syncErr
		}
	}
	return landing, nil
}

// syncSegment flushes the active segment to stable storage. This is the
// "sync phase" of a batched write, called after all records have been
// buffered with batchMode enabled. The caller must hold s.mu.
//
// A sync failure here poisons the owning DB (fail-stop): the batch's
// records were already written to the segment, and after a failed
// fdatasync neither their durability nor the durability of any rollback
// truncation can be established (fsyncgate). Rolling back is therefore
// insufficient; the only safe continuation is crash recovery.
func (s *walShard) syncSegment() error {
	if s.opts.noSync || s.activeSegment == nil {
		return nil
	}
	fsyncStart := time.Now()
	syncErr := s.activeSegment.Sync()
	if s.metrics != nil {
		s.metrics.ObserveFsyncLatency(time.Since(fsyncStart))
	}
	if syncErr != nil && s.poison != nil {
		s.poison(syncErr)
	}
	return syncErr
}

// close closes the active segment and releases resources. It takes s.mu so
// shutdown serializes with in-flight writers: without the lock, a writer
// already past DB.writeGate could mutate activeSegment (or race on the
// segment's plain closed bool) while the segment is being torn down —
// a torn block write or a WriteAt onto a recycled fd. The closed flag set
// here keeps any later writer from recreating a segment (see
// ensureActiveSegment).
func (s *walShard) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.activeSegment != nil {
		err := s.activeSegment.Close()
		s.activeSegment = nil
		return err
	}
	return nil
}

// closeAfterFailedOpen closes a shard that was opened successfully as part of a
// DB open that then failed, and unlinks the segment this open created if it
// holds no records.
//
// Without the unlink, every failed Open leaves one empty segment file per shard
// that got that far, and recovery counts it: nextSegmentID advances past it, so
// the files accumulate rather than being reused. That was tolerable while a
// failed Open was rare, but the fail-closed conditions this package now raises
// (a geometry mismatch, an unresolved barrier binding, mid-segment corruption)
// are all conditions an operator retries against while diagnosing, which makes
// repeated failed opens the EXPECTED path rather than the exceptional one.
//
// Only a segment created by THIS open and holding zero records is removed, so
// there is no way for this to touch a segment that carries data. Every failure
// is logged and swallowed: the DB open is already failing, and a leftover file
// is a far smaller problem than masking the error the caller needs to see.
func (s *walShard) closeAfterFailedOpen() {
	s.mu.Lock()
	defer s.mu.Unlock()

	remove := s.openCreatedSegmentValid &&
		s.activeSegment != nil &&
		s.activeSegment.ID() == s.openCreatedSegment &&
		s.activeSegment.RecordCount() == 0
	segID := s.openCreatedSegment

	s.closed = true
	if s.activeSegment != nil {
		if err := s.activeSegment.Close(); err != nil {
			slog.Debug("waldb: shard close failed during failed-open cleanup",
				"shard_index", s.shardIdx, "dir", s.dir, "error", err)
		}
		s.activeSegment = nil
	}
	if !remove {
		return
	}
	if err := s.fs.Remove(filepath.Join(s.dir, segmentFilename(segID))); err != nil {
		slog.Debug("waldb: removing the empty segment left by a failed open failed",
			"shard_index", s.shardIdx, "dir", s.dir, "segment_id", segID, "error", err)
		return
	}
	if !s.opts.noSync {
		if err := syncDirFS(s.fs, s.dir); err != nil {
			slog.Debug("waldb: syncing the shard directory after removing an "+
				"empty segment failed",
				"shard_index", s.shardIdx, "dir", s.dir, "error", err)
		}
	}
}

// shardDirName returns the directory name for the given shard index.
func shardDirName(idx int) string {
	return "shard-" + itoa(idx)
}

// itoa converts a small non-negative int to a string without fmt.Sprintf.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// syncDirFS fsyncs a directory using the provided FS to make metadata
// changes (file creates, deletes, renames) durable. Returns a DirSyncError
// if the directory cannot be opened or synced.
//
// This uses SyncMetadata, NOT Sync. On Linux File.Sync is fdatasync, which is
// specified to skip metadata not needed to retrieve the data — and for a
// directory the metadata IS the point. A dirent removal often leaves the
// directory size unchanged, so XFS can skip the log force that ext4 happens to
// perform anyway, which is why no test catches the difference. The paths that
// depend on this are rollback segment deletion, orphan discard (whose own
// comment cites voting twice in one term) and GC — all of them Raft durability.
func syncDirFS(filesystem FS, dir string) error {
	d, err := filesystem.Open(dir)
	if err != nil {
		return &DirSyncError{Dir: dir, Err: err}
	}
	var syncErr error
	if ms, ok := d.(FileMetadataSyncer); ok {
		syncErr = ms.SyncMetadata()
	} else {
		syncErr = d.Sync()
	}
	closeErr := d.Close()
	if syncErr != nil {
		return &DirSyncError{Dir: dir, Err: syncErr}
	}
	if closeErr != nil {
		return &DirSyncError{Dir: dir, Err: closeErr}
	}
	return nil
}

// deepestExistingDir returns the deepest ancestor of dir (dir itself when it
// already exists) that is present on filesystem. Only ErrNotExist is read as
// absence; any other Stat error stops the walk at that level, which is the
// conservative choice because the following sync walk then covers MORE
// directories, never fewer.
func deepestExistingDir(filesystem FS, dir string) string {
	for p := filepath.Clean(dir); ; {
		if _, err := filesystem.Stat(p); err == nil || !errors.Is(err, fs.ErrNotExist) {
			return p
		}
		parent := filepath.Dir(p)
		if parent == p {
			return p
		}
		p = parent
	}
}

// syncDirChainFS fsyncs leaf and every ancestor up to and including stopAt, so
// each directory MkdirAll created below stopAt has its own dirent made durable
// by a sync of its parent. It is the FS-abstracted equivalent of the shared
// engine.SyncDirChainWith walk (pkg/internal/engine/fsync.go); the walk is
// duplicated rather than imported because waldb is the storage layer and must
// not depend on the consensus engine package.
func syncDirChainFS(filesystem FS, leaf, stopAt string) error {
	leaf = filepath.Clean(leaf)
	stopAt = filepath.Clean(stopAt)
	sep := string(filepath.Separator)
	if stopAt == "" || !strings.HasPrefix(leaf+sep, stopAt+sep) {
		return syncDirFS(filesystem, leaf)
	}
	for dir := leaf; ; dir = filepath.Dir(dir) {
		if err := syncDirFS(filesystem, dir); err != nil {
			return err
		}
		if dir == stopAt {
			return nil
		}
	}
}

// getShard returns the WAL shard for the given Raft shardID.
func (db *DB) getShard(shardID uint64) *walShard {
	return db.shards[shardID%DefaultShardCount]
}

// Name returns the name of this LogDB implementation.
func (db *DB) Name() string {
	return "waldb"
}

// SaveState persists a batch of Updates atomically per shard. Each Update
// contains entries and/or hard state for a single Raft shard.
//
// The method uses a two-phase write strategy to minimize fsync overhead:
//
//  1. Write phase: all records are buffered to their respective shard
//     segments without calling fdatasync (batchMode=true).
//  2. Sync phase: dirty shards are fsynced in parallel, amortizing the
//     cost of fdatasync across all shards touched in the batch.
//
// This reduces the number of fsyncs from O(records) to O(shards), where
// shards <= DefaultShardCount.
//
// Cross-shard atomicity:
//
// SaveState is NOT atomic across shards. If a write error occurs mid-batch
// (e.g., writing the 3rd of 5 shard updates fails), shards that were
// written before the failure have records buffered in their WAL segments
// but NOT fsynced (phase 2 is never reached). On crash, these buffered
// records are lost because the OS page cache is not durable. On a
// non-crashing error return, the caller sees the error and the buffered
// records remain in memory until the next successful sync or process exit.
//
// This partial-write behavior is safe for Raft because:
//
//  1. Raft re-proposes uncommitted entries on leader election, so any
//     entries lost due to a crash after a partial write are retransmitted.
//  2. Individual shard WAL records are self-describing with CRC32
//     checksums, so torn writes within a single record are detected and
//     the record is discarded on recovery. This guarantees per-record
//     atomicity even when cross-shard atomicity is not provided.
//  3. Hard state (Term, Vote, Commit) is idempotent to replay: writing
//     the same or newer hard state on recovery is always correct.
//
// Cleanup of already-written records on mid-batch failure is not feasible
// because WAL segments are append-only by design. Truncation would
// require tracking per-shard write offsets before the batch and rolling
// back on error, adding complexity and reducing performance on the hot
// path for a scenario that Raft's recovery protocol already handles.
// This is the same design trade-off made by dragonboat and most WAL
// implementations.
func (db *DB) SaveState(updates []logdb.Update) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	if len(updates) == 0 {
		return nil
	}

	// Check disk usage before writing. This is throttled internally
	// to avoid syscall overhead on every write.
	if err := db.diskMon.MaybeCheck(); err != nil {
		return err
	}

	writeStart := time.Now()

	// Stack-allocated per-shard bookkeeping, avoiding map allocation on the
	// hot path. dirtyShards/dirtyCount track shards that took a successful
	// write (fsync + commit targets); checkpoints capture each touched shard's
	// pre-batch segment state for rollback; pending stages in-memory updates
	// applied only after all WAL writes and sync succeed.
	var dirtyShards [DefaultShardCount]*walShard
	var checkpoints [DefaultShardCount]shardCheckpoint
	var pending [DefaultShardCount]pendingMemUpdates
	dirtyCount := 0

	// Lock every WAL shard this batch touches for the ENTIRE batch lifecycle:
	// checkpoint capture -> record write -> fsync -> in-memory commit (or
	// rollback). Holding each shard's lock across the whole lifecycle makes the
	// batch atomic per WAL segment. Previously SaveState released shard.mu
	// between its durable write and its rollback, so a concurrent same-shard
	// SaveState (two Raft shards that hash to one WAL shard are served by
	// different commit workers whenever NumWorkers does not divide
	// DefaultShardCount) could write, fsync, and COMMIT a record past this
	// batch's checkpoint; a later partial-failure rollback then truncated the
	// shared segment back to this batch's offset and silently erased the other
	// batch's already-acknowledged committed records. Locks are acquired in
	// ascending shard-index order so two batches with overlapping shard sets
	// share a canonical lock order and cannot deadlock; single-shard lockers
	// (Compact, SaveSnapshot, RemoveEntriesTo, RemoveNodeData) hold at most one
	// mu and cannot close a cycle. Reads take no mu at all — only dataMu, which
	// this batch acquires solely inside Phase 3 and always after mu.
	var touched [DefaultShardCount]bool
	for i := range updates {
		touched[uint16(updates[i].ShardID%DefaultShardCount)] = true
	}
	for idx := 0; idx < DefaultShardCount; idx++ {
		if touched[idx] {
			db.shards[idx].mu.Lock()
		}
	}
	defer func() {
		for idx := DefaultShardCount - 1; idx >= 0; idx-- {
			if touched[idx] {
				db.shards[idx].mu.Unlock()
			}
		}
	}()

	// Phase 1: Write all records to their shards WITHOUT fsyncing. All touched
	// shard locks are already held.
	for i := range updates {
		u := &updates[i]
		shardIdx := uint16(u.ShardID % DefaultShardCount)
		shard := db.shards[shardIdx]

		// Capture the segment checkpoint before the first write to this shard.
		// For non-lazy shards with an existing active segment, capture now;
		// lazy shards capture inside saveUpdate after ensureActiveSegment.
		// Flush the open block first so rollbackShard->DiscardPending can
		// reliably reload the pre-batch tail from disk (see
		// captureCheckpointAfterEnsure for the invariant).
		if !checkpoints[shardIdx].valid && shard.activeSegment != nil {
			if err := shard.activeSegment.flushBlock(); err != nil {
				return db.rollbackBatch(&checkpoints, &pending, err)
			}
			checkpoints[shardIdx] = shardCheckpoint{
				fileOffset:  shard.activeSegment.fileOffset,
				blockOffset: shard.activeSegment.blockOffset,
				recordCount: shard.activeSegment.RecordCount(),
				seg:         shard.activeSegment,
				segID:       shard.activeSegmentID,
				valid:       true,
			}
		}

		shard.batchMode = true
		err := shard.saveUpdate(u, &pending[shardIdx], &checkpoints[shardIdx])
		shard.batchMode = false
		if err != nil {
			return db.rollbackBatch(&checkpoints, &pending, err)
		}

		if dirtyShards[shardIdx] == nil {
			dirtyShards[shardIdx] = shard
			dirtyCount++
		}
	}

	// Phase 2: Sync all dirty shards to stable storage. A failure here has
	// already poisoned the DB (see syncSegment): the batch's records are
	// written but not provably durable, and a rollback truncation could not be
	// proven durable either after a failed fsync. Surface the fail-stop to the
	// caller alongside the underlying cause.
	if err := syncDirtyShardsLocked(&dirtyShards, dirtyCount); err != nil {
		db.poison(err)
		// Phase 3 never runs, so the batch's staged epochs would be dropped —
		// yet its records ARE written and nothing truncates them, so they can
		// replay on the next start. Retain their DEKs; see commitPendingEpochs.
		db.commitPendingEpochs(&pending)
		return errors.Join(ErrDBPoisoned, err)
	}

	// Phase 3: Apply in-memory updates now that all WAL data is durable. This
	// is the commit point — reads see these changes only after the data is
	// safely persisted. The shard mu locks are still held, so this commit is
	// atomic with the durable write relative to any concurrent same-shard
	// batch. applyTo additionally takes each shard's dataMu, which is what
	// makes the commit atomic for the lock-free readers; it is the ONLY point
	// in the batch where a reader can be blocked, and it holds no I/O.
	for i := range dirtyShards {
		if dirtyShards[i] != nil {
			pending[i].applyTo(dirtyShards[i])
		}
	}

	if db.metrics != nil {
		db.metrics.ObserveWriteLatency(time.Since(writeStart))
	}

	return nil
}

// shardCheckpoint captures the pre-batch segment state for a shard so we can
// discard buffered records on partial write failure.
type shardCheckpoint struct {
	fileOffset  int64
	blockOffset int
	// recordCount is the segment's logical-record ordinal at capture time.
	// Rollback rewinds the segment to it alongside the offsets, so the ordinal
	// bound into each encrypted record's context-AAD keeps matching the records
	// actually present in the file. See segment.DiscardPending.
	recordCount uint64
	// seg/segID identify the active segment when the checkpoint was captured.
	// Rollback compares them against the current active segment to detect a
	// mid-batch segment rotation, which needs a multi-segment rollback rather
	// than a simple in-place truncate.
	seg   *segment
	segID uint64
	valid bool
}

// pendingEntryUpdate captures a deferred entry map and index update.
type pendingEntryUpdate struct {
	key     nodeKey
	entries []logdb.Entry
	indexes []indexEntry
}

// pendingStateUpdate captures a deferred hard-state update. landedSegID is the
// segment the state record landed in (post-rotation), used to pin the segment
// against garbage collection at the commit point.
type pendingStateUpdate struct {
	key         nodeKey
	state       logdb.State
	landedSegID uint64
}

// pendingSnapshotUpdate captures a deferred snapshot metadata update.
// landedSegID is the segment the snapshot record landed in (post-rotation),
// used to pin the segment against garbage collection at the commit point.
type pendingSnapshotUpdate struct {
	key         nodeKey
	snapshot    logdb.Snapshot
	landedSegID uint64
}

// pendingMemUpdates accumulates in-memory updates during a batch write.
// Updates are applied atomically after all WAL writes and sync succeed,
// ensuring no phantom data is readable on mid-batch failure.
type pendingMemUpdates struct {
	entries   []pendingEntryUpdate
	states    []pendingStateUpdate
	snapshots []pendingSnapshotUpdate
	// epochs holds the landing of every encrypted record this batch wrote, so
	// the epoch floor is raised only at the commit point. A batch that rolls
	// back discards these and leaves the floor untouched.
	epochs []recordLanding
}

// stageEpoch defers a written record's epoch to the batch's commit point. A
// landing with no epoch (no barrier configured) is dropped, so an unencrypted
// batch stages nothing and allocates nothing.
func (p *pendingMemUpdates) stageEpoch(l recordLanding) {
	if l.epoch == 0 {
		return
	}
	p.epochs = append(p.epochs, l)
}

// applyTo applies all pending in-memory updates to the shard. The caller must
// hold shard.mu (Phase 3 of a SaveState batch, or a standalone SaveSnapshot).
//
// It additionally takes dataMu for the whole application, making the batch's
// index, entry, state, snapshot, pin and high-water-mark updates a single
// atomic commit for the lock-free readers: no reader can observe a batch
// half-applied. The section is pure memory work — no write, fsync or
// rotation happens under dataMu.
func (p *pendingMemUpdates) applyTo(s *walShard) {
	s.dataMu.Lock()
	defer s.dataMu.Unlock()

	for i := range p.entries {
		eu := &p.entries[i]
		idx := s.getOrCreateIndex(eu.key)
		idx.Append(eu.indexes...)

		// Advance the commit high-water mark to the segment these entries
		// landed in. All entries in one pendingEntryUpdate share a single
		// WAL record and therefore a single segment, so the first index
		// entry's SegmentID is representative.
		if len(eu.indexes) > 0 {
			s.advanceCommittedHWM(eu.indexes[0].SegmentID)
		}

		nodeEntries := s.entries[eu.key]
		if nodeEntries == nil {
			nodeEntries = make(map[uint64]logdb.Entry)
			s.entries[eu.key] = nodeEntries
		}
		if len(eu.entries) == 0 {
			continue
		}
		// Detect a leader-overwrite (the first new index already has an entry):
		// the index slice is truncated to the new suffix, but the entry MAP
		// would otherwise retain the now-orphaned higher indices until
		// compaction. Prune them so the leak is bounded. The O(n) scan runs
		// only on actual overwrites (leader churn), not on normal appends.
		firstIdx := eu.entries[0].Index
		_, overwrite := nodeEntries[firstIdx]
		for j := range eu.entries {
			nodeEntries[eu.entries[j].Index] = eu.entries[j]
		}
		if overwrite {
			lastIdx := eu.entries[len(eu.entries)-1].Index
			for k := range nodeEntries {
				if k > lastIdx {
					delete(nodeEntries, k)
				}
			}
		}
	}

	for i := range p.states {
		s.states[p.states[i].key] = p.states[i].state
		s.setStateSegment(p.states[i].key, p.states[i].landedSegID)
		s.advanceCommittedHWM(p.states[i].landedSegID)
	}

	for i := range p.snapshots {
		s.snapshots[p.snapshots[i].key] = p.snapshots[i].snapshot
		s.setSnapshotSegment(p.snapshots[i].key, p.snapshots[i].landedSegID)
		s.advanceCommittedHWM(p.snapshots[i].landedSegID)
	}

	// The batch's records are durable, so their epochs may now lower the
	// floor. This rides the dataMu section the commit already takes: no extra
	// lock acquisition, and a rolled-back batch never gets here.
	for i := range p.epochs {
		s.applyEpochLocked(p.epochs[i])
	}
}

// rollbackBatch rolls back every shard that captured a checkpoint in the
// current SaveState batch, discarding this batch's buffered/written records so
// no ghost records survive on the next successful SaveState. A valid checkpoint
// is captured before the first write to each shard, so iterating checkpoints
// covers both the already-written shards and the shard whose write just failed.
// The caller MUST hold every touched shard's mu (the SaveState batch holds them
// for its whole lifecycle), so rollbackShard is invoked directly without
// re-locking. writeErr is the originating Phase 1 failure; if rollback itself
// fails, both the write error and the discard errors are surfaced.
//
// The batch's staged epochs are discarded along with the records they belong
// to — that is the whole point of deferring them to the commit point — EXCEPT
// when the rollback could not be proven durable. See commitPendingEpochs.
func (db *DB) rollbackBatch(
	checkpoints *[DefaultShardCount]shardCheckpoint,
	pending *[DefaultShardCount]pendingMemUpdates,
	writeErr error,
) error {
	var discardErrors []error
	for j := range checkpoints {
		if !checkpoints[j].valid {
			continue
		}
		if dErr := rollbackShard(db.shards[j], &checkpoints[j]); dErr != nil {
			discardErrors = append(discardErrors, dErr)
		}
	}
	if db.poisoned.Load() {
		db.commitPendingEpochs(pending)
	}
	if len(discardErrors) > 0 {
		return &BatchWriteError{WriteErr: writeErr, DiscardErrors: discardErrors}
	}
	return writeErr
}

// commitPendingEpochs folds a batch's staged record landings into their shards'
// epoch floors WITHOUT applying any other in-memory state. It is the
// conservative fallback for a batch that neither committed nor could prove its
// records gone: after a failed fdatasync, or a rollback whose truncation could
// not be made durable, the records may still replay from disk, so the DEKs
// that decrypt them must stay retained.
//
// The asymmetry is deliberate. A floor that is too LOW only holds a key longer
// than necessary; a floor that is too HIGH lets the engine purge a DEK a
// surviving record needs, and the next start dies with a fatal
// WALDecryptError. When in doubt, retain.
//
// The caller holds every touched shard's mu, so this observes the mu -> dataMu
// order and holds no I/O under dataMu.
func (db *DB) commitPendingEpochs(pending *[DefaultShardCount]pendingMemUpdates) {
	for i := range pending {
		if len(pending[i].epochs) == 0 {
			continue
		}
		s := db.shards[i]
		s.dataMu.Lock()
		for j := range pending[i].epochs {
			s.applyEpochLocked(pending[i].epochs[j])
		}
		s.dataMu.Unlock()
	}
}

// rollbackShard rolls a shard's WAL back to the state captured in cp. The
// caller must hold ds.mu.
//
// Common case: no segment rotation occurred during the batch, so the active
// segment is the same one the checkpoint was taken against and a single
// in-place DiscardPending restores it.
//
// Rotation case: a write hit ErrSegmentFull mid-batch and rotateSegment
// installed one or more new segments (the old ones were sync+closed with
// batch data). DiscardPending with the old checkpoint offsets would truncate
// the WRONG (new) segment. Instead we work HIGH TO LOW: (1) delete any
// intermediate segments created entirely from batch data, (2) empty the
// current active segment, (3) fsync the directory, and only then (4) truncate
// the checkpoint segment back to its pre-batch durable boundary. This
// guarantees recovery replays only the pre-batch prefix, and — because the low
// segment is cut last — that a crash at any point in between leaves a valid
// prefix rather than a hole. See the ordering note in the body.
func rollbackShard(ds *walShard, cp *shardCheckpoint) (err error) {
	// Nothing was written against a known position, so there is nothing to
	// undo. An absent active segment is NOT such a case: rotateSegment drops
	// the handle when the post-Sync Close fails, and that Sync already made
	// this batch's records durable in the checkpoint segment.
	if ds.activeSegment == nil && !cp.valid {
		return nil
	}
	// Every rollback failure is a DURABILITY failure: the truncation that
	// erases this batch's records could not be proven to have reached the
	// medium. Leaving the DB writable would let the next batch append past an
	// un-durable truncation, and a crash would then resurrect records the
	// caller was already told had FAILED. Poison on any error so the failure
	// satisfies errors.Is(err, ErrDBPoisoned) all the way up through
	// BatchWriteError, matching the directory-fsync path below.
	defer func() {
		if err == nil || errors.Is(err, ErrDBPoisoned) {
			return
		}
		if ds.poison != nil {
			ds.poison(err)
		}
		err = errors.Join(ErrDBPoisoned, err)
	}()

	// No rotation: the active segment is the checkpoint segment.
	if ds.activeSegment != nil && (cp.seg == nil || ds.activeSegment == cp.seg) {
		return ds.activeSegment.DiscardPending(cp.fileOffset, cp.blockOffset, cp.recordCount)
	}

	// Rotation occurred. The rollback runs HIGH TO LOW — intermediates deleted,
	// active segment emptied, directory fsynced, and only then the checkpoint
	// segment truncated — so that every crash state in between is a valid
	// prefix of the pre-batch log.
	//
	// The reverse order is not: truncating the checkpoint segment first leaves
	// it ending cleanly at a record boundary with the higher segments still
	// holding their (fsynced, CRC-valid) batch records, so a crash there is
	// followed by a recovery that replays the checkpoint segment in full and
	// then every segment above it — resurrecting the batch records the caller
	// was told had FAILED, with a hole where the truncated ones were. Crashing
	// under the order below can only leave batch records in the checkpoint
	// segment's own tail, which is an ordinary interrupted write and is exactly
	// what a crash one instruction earlier would have left anyway.
	cleanEnd := cp.fileOffset + int64(cp.blockOffset)

	// Delete intermediate segments that contain only batch data.
	for id := cp.segID + 1; id < ds.activeSegmentID; id++ {
		name := filepath.Join(ds.dir, segmentFilename(id))
		if rmErr := ds.fs.Remove(name); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			return &SegmentDiscardError{SegmentID: id, Err: rmErr}
		}
		delete(ds.knownSegments, id)
		// The epoch map is dataMu state; take it for the delete alone so no
		// lock is held across the Remove above.
		ds.dataMu.Lock()
		delete(ds.segmentEpochs, id)
		ds.dataMu.Unlock()
	}

	// Empty the current segment (all its content is batch data). It remains
	// the active segment: a zero-length / all-zero file replays to nothing, so
	// recovery is correct. Two tolerated side effects: (1) its ID is now
	// non-contiguous with the checkpoint segment because the intermediates
	// were deleted — recovery sorts segment IDs, so a gap is fine; (2) the
	// truncation to 0 drops the fallocate reservation, which DiscardPending
	// re-takes.
	//
	// When the rotation's Close failed, rotateSegment dropped the handle and
	// left activeSegment nil while activeSegmentID still names the segment on
	// disk; truncate that closed file to zero instead, so its already-fsynced
	// batch records cannot replay.
	switch {
	case ds.activeSegment != nil:
		// Every record in this segment is batch data, so its ordinal rewinds to
		// zero along with its offsets.
		if err := ds.activeSegment.DiscardPending(0, 0, 0); err != nil {
			return err
		}
	case ds.activeSegmentID != cp.segID:
		if err := truncateClosedSegment(ds, ds.activeSegmentID, 0); err != nil {
			return err
		}
	}

	// Make the segment-file deletions durable BEFORE the checkpoint segment is
	// cut. Without a directory fsync a crash right after rollback could
	// resurrect a deleted intermediate segment's directory entry, and recovery
	// would replay its (CRC-valid) batch records that were supposed to be
	// discarded — resurrecting writes the caller was told had failed. A
	// directory-fsync failure here is therefore fail-stop, exactly like the
	// data-fsync failures in syncSegment/writeRecord: the rollback cannot be
	// proven durable, so the DB is poisoned and the error is surfaced through
	// rollbackBatch to the SaveState caller.
	if err := syncDirFS(ds.fs, ds.dir); err != nil {
		if ds.poison != nil {
			ds.poison(err)
		}
		return errors.Join(ErrDBPoisoned, err)
	}

	// The checkpoint segment last: it is the LOW segment, and until it is cut
	// the on-disk state is still a valid (longer) prefix. truncateClosedSegment
	// fsyncs the file, so the cut is durable on return.
	if err := truncateClosedSegment(ds, cp.segID, cleanEnd); err != nil {
		return err
	}
	return nil
}

// truncateClosedSegment reopens a (now-closed) segment file by ID and
// truncates it to size, discarding any batch data written past the
// checkpoint. Used by the rotation rollback path.
func truncateClosedSegment(ds *walShard, segID uint64, size int64) error {
	name := filepath.Join(ds.dir, segmentFilename(segID))
	f, err := ds.fs.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return &SegmentDiscardError{SegmentID: segID, Err: err}
	}
	if tErr := f.Truncate(size); tErr != nil {
		_ = f.Close()
		return &SegmentDiscardError{SegmentID: segID, Err: tErr}
	}
	if sErr := f.Sync(); sErr != nil {
		_ = f.Close()
		return &SegmentDiscardError{SegmentID: segID, Err: sErr}
	}
	return f.Close()
}

// syncDirtyShardsLocked fsyncs every dirty shard's active segment to stable
// storage. The caller MUST already hold every dirty shard's mu (the SaveState
// batch holds all touched shard locks for its whole lifecycle), so this never
// re-acquires them — doing so would deadlock. When a single shard is dirty the
// sync is inline to avoid goroutine overhead. For multi-shard batches the
// fdatasyncs run concurrently to bound batch latency; each goroutine touches a
// distinct shard whose lock the caller holds (excluding all other accessors),
// and the go statement establishes happens-before from the Phase 1 writes, so
// the concurrent syncSegment calls are race-free.
func syncDirtyShardsLocked(dirtyShards *[DefaultShardCount]*walShard, dirtyCount int) error {
	if dirtyCount == 1 {
		// Fast path: single shard, avoid goroutine overhead.
		for i := range dirtyShards {
			if dirtyShards[i] != nil {
				return dirtyShards[i].syncSegment()
			}
		}
		return nil
	}
	if dirtyCount > 1 {
		// Parallel sync across shards — each fdatasync runs concurrently.
		errCh := make(chan error, dirtyCount)
		for i := range dirtyShards {
			if dirtyShards[i] != nil {
				go func(s *walShard) {
					errCh <- s.syncSegment()
				}(dirtyShards[i])
			}
		}
		var firstErr error
		for range dirtyCount {
			if err := <-errCh; err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	return nil
}

// saveUpdate writes a single Update to this shard's WAL and stages in-memory
// updates into pending. The caller applies pending updates only after all
// shards succeed and sync completes, preventing phantom reads on failure.
//
// The checkpoint parameter is captured lazily for shards that had no active
// segment at the start of the batch (lazy shards). After ensureActiveSegment
// creates the segment, the checkpoint is captured before any data is written.
func (s *walShard) saveUpdate(u *logdb.Update, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	key := nodeKey{ShardID: u.ShardID, ReplicaID: u.ReplicaID}

	// Write entries.
	if len(u.Entries) > 0 {
		if err := s.writeEntries(key, u.ShardID, u.ReplicaID, u.Entries, pending, cp); err != nil {
			return err
		}
	}

	// Write hard state if non-zero.
	if u.State.Term != 0 || u.State.Vote != 0 || u.State.Commit != 0 {
		if err := s.writeState(key, u.ShardID, u.ReplicaID, u.State, pending, cp); err != nil {
			return err
		}
	}

	// Write the snapshot if present and not stale. Presence is keyed on
	// Index > 0 alone, matching memdb: a zero-index snapshot is no snapshot,
	// and writing one made GetSnapshot report a snapshot at index 0 and pinned
	// its segment against garbage collection permanently.
	if u.Snapshot.Index > 0 {
		// Check against both persisted snapshots and any pending snapshot
		// updates in this batch to avoid writing stale data. The snapshots map
		// is read under s.mu alone: every mutator of it holds s.mu too, so no
		// dataMu is needed for a read on this side of the lock.
		stale := false
		if existing, ok := s.snapshots[key]; ok && u.Snapshot.Index <= existing.Index {
			stale = true
		}
		if !stale {
			for i := range pending.snapshots {
				if pending.snapshots[i].key == key && u.Snapshot.Index <= pending.snapshots[i].snapshot.Index {
					stale = true
					break
				}
			}
		}
		if !stale {
			if err := s.writeSnapshot(key, u.ShardID, u.ReplicaID, u.Snapshot, pending, cp); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeEntries serializes and writes a batch of entries to the WAL, and
// stages the in-memory updates in pending for deferred application.
//
// In-memory state (index, entry map) is staged but NOT applied here.
// The caller applies pending updates only after all shards succeed and
// sync completes, preventing phantom reads on mid-batch failure.
func (s *walShard) writeEntries(key nodeKey, shardID, replicaID uint64, entries []logdb.Entry, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	// Enforce the same per-entry Cmd cap that replay enforces, BEFORE any
	// segment is touched. Without this write-side check the encoder would
	// happily store (and uint32-truncate, for >4GiB) a Cmd that recovery
	// then rejects as corrupt — an entry that is durable but unreadable.
	for i := range entries {
		if len(entries[i].Cmd) > maxCmdLen {
			return ErrCmdTooLarge
		}
	}

	// Ensure the active segment exists before capturing the segment ID,
	// since a lazy shard may not have created the directory or segment yet.
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}

	// Capture checkpoint for lazy shards after segment creation but
	// before any data is written. This ensures rollback can discard
	// data written to a newly created segment.
	if err := captureCheckpointAfterEnsure(s, cp); err != nil {
		return err
	}

	// A single WAL record must fit inside one segment, so a large batch is
	// split across several records. A record spends recordHeaderSize bytes in
	// every block it spans and may pad the block it starts in, so the usable
	// payload is strictly LESS than the segment size. Raft's batch cap counts
	// neither that framing nor the 76-byte per-entry header, so without
	// splitting a batch raft legitimately produced -- a lagging follower
	// catching up on many small entries is the realistic case -- would fail
	// permanently with ErrRecordTooLarge and this node could never persist it.
	//
	// Splitting is format-compatible: replay already handles several entry
	// records per Update. It is also crash-safe, because these records still
	// precede this Update's state record in log order and the WAL truncates at
	// a prefix. A torn tail can therefore only lose state while keeping
	// entries, never the reverse -- which would recover with a commit index
	// past the last entry.
	maxPayload := s.maxRecordPayload()

	for start := 0; start < len(entries); {
		// Grow the chunk while it still fits one record, always taking at
		// least one entry so the batch cannot stall.
		size := 1 // payload type byte
		end := start
		for end < len(entries) {
			need := entryRecordHeaderSize + len(entries[end].Cmd)
			if end > start && size+need > maxPayload {
				break
			}
			size += need
			end++
		}
		// A lone entry larger than one record is genuinely unpersistable;
		// ErrRecordTooLarge is permanent so the caller does not retry forever.
		if size > maxPayload {
			return ErrRecordTooLarge
		}
		if err := s.writeEntryChunk(key, shardID, replicaID, entries[start:end], size, pending); err != nil {
			return err
		}
		start = end
	}

	return nil
}

// maxRecordPayload is the largest record payload guaranteed to fit in a fresh
// segment, accounting for the per-block header every block a record spans
// costs and for the block the record may have to pad past to start.
func (s *walShard) maxRecordPayload() int {
	blocks := int(s.opts.maxFileSize / int64(s.opts.blockSize))
	if blocks <= 1 {
		return 0
	}
	return (blocks - 1) * (s.opts.blockSize - recordHeaderSize)
}

// writeEntryChunk marshals one chunk of entries into a single WAL record,
// writes it, and stages the resulting in-memory updates. totalSize is the
// chunk's exact marshalled size as computed by the caller.
func (s *walShard) writeEntryChunk(key nodeKey, shardID, replicaID uint64, entries []logdb.Entry, totalSize int, pending *pendingMemUpdates) error {
	buf := s.getMarshalBuf(totalSize)
	buf[0] = payloadTypeEntries
	pos := 1

	// Collect index and entry updates to stage into pending. The SegmentID is
	// filled in AFTER writeRecord succeeds: the write below may hit
	// ErrSegmentFull and rotate, landing this chunk in a NEW segment.
	// Capturing s.activeSegmentID beforehand would point the index at the old
	// segment, and Compact (which builds its live-segment set from index
	// SegmentIDs) could then GC the segment that actually holds these entries
	// -- silent data loss on recovery. With chunking, consecutive chunks of
	// one batch can legitimately land in different segments, so each chunk
	// must be stamped with its own.
	stagedIdx := make([]indexEntry, 0, len(entries))
	stagedEntries := make([]logdb.Entry, 0, len(entries))

	for i := range entries {
		e := &entries[i]
		binary.LittleEndian.PutUint64(buf[pos:], shardID)
		binary.LittleEndian.PutUint64(buf[pos+8:], replicaID)
		binary.LittleEndian.PutUint64(buf[pos+16:], e.Index)
		binary.LittleEndian.PutUint64(buf[pos+24:], e.Term)
		binary.LittleEndian.PutUint64(buf[pos+32:], e.Type)
		binary.LittleEndian.PutUint64(buf[pos+40:], e.Key)
		binary.LittleEndian.PutUint64(buf[pos+48:], e.ClientID)
		binary.LittleEndian.PutUint64(buf[pos+56:], e.SeriesID)
		binary.LittleEndian.PutUint64(buf[pos+64:], e.RespondedTo)
		binary.LittleEndian.PutUint64(buf[pos+72:], e.OwnerID)
		binary.LittleEndian.PutUint32(buf[pos+80:], uint32(len(e.Cmd)))
		copy(buf[pos+entryRecordHeaderSize:], e.Cmd)

		entrySize := entryRecordHeaderSize + len(e.Cmd)

		stagedIdx = append(stagedIdx, indexEntry{
			Index: e.Index,
			Term:  e.Term,
			Size:  int64(entrySize),
		})

		// Copy Cmd to avoid aliasing the caller's slice.
		var cmdCopy []byte
		if len(e.Cmd) > 0 {
			cmdCopy = make([]byte, len(e.Cmd))
			copy(cmdCopy, e.Cmd)
		}
		stagedEntries = append(stagedEntries, logdb.Entry{
			Index:       e.Index,
			Term:        e.Term,
			Type:        e.Type,
			Key:         e.Key,
			ClientID:    e.ClientID,
			SeriesID:    e.SeriesID,
			RespondedTo: e.RespondedTo,
			OwnerID:     e.OwnerID,
			Cmd:         cmdCopy,
		})

		pos += entrySize
	}

	// Persist to WAL. On failure, no in-memory state is affected.
	landing, err := s.writeRecord(buf[:pos])
	if err != nil {
		return err
	}
	pending.stageEpoch(landing)

	// Stamp the index entries with the segment this chunk actually landed in
	// (post-rotation if writeRecord rotated).
	for i := range stagedIdx {
		stagedIdx[i].SegmentID = landing.segID
	}

	// WAL write succeeded. Stage in-memory updates for deferred application.
	pending.entries = append(pending.entries, pendingEntryUpdate{
		key:     key,
		entries: stagedEntries,
		indexes: stagedIdx,
	})

	return nil
}

// captureShardCheckpoint prepares a standalone (non-batch) record write for
// rollback: it creates the active segment if the shard is still lazy, then
// captures the segment position the write starts from. Paired with
// DB.rollbackStandalone it gives SaveSnapshot/RemoveEntriesTo/RemoveNodeData
// the same "a failed write leaves nothing durable" guarantee SaveState has.
// The caller must hold s.mu.
func captureShardCheckpoint(s *walShard, cp *shardCheckpoint) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	return captureCheckpointAfterEnsure(s, cp)
}

// rollbackStandalone discards a failed standalone record write, returning the
// original write error (wrapped in a BatchWriteError when the rollback itself
// failed, so ErrDBPoisoned still reaches the caller). The caller must hold the
// shard's mu.
//
// A poisoned DB is left alone: after a failed fdatasync neither the record nor
// a rollback truncation can be proven durable (fsyncgate), so crash recovery —
// not truncation — is the only sound continuation, exactly as in SaveState's
// sync phase.
func (db *DB) rollbackStandalone(s *walShard, cp *shardCheckpoint, writeErr error) error {
	if !cp.valid || db.poisoned.Load() {
		return writeErr
	}
	if dErr := rollbackShard(s, cp); dErr != nil {
		return &BatchWriteError{WriteErr: writeErr, DiscardErrors: []error{dErr}}
	}
	return writeErr
}

// captureCheckpointAfterEnsure captures a checkpoint for a shard that
// just had its active segment created by ensureActiveSegment. This
// handles lazy shards that had no segment at batch start. The checkpoint
// is captured at the segment's current position (before any batch data).
//
// It first flushes the open block to disk. rollbackShard -> DiscardPending
// reloads the pre-batch open-block tail from disk by ReadAt, which is only
// correct if that tail is actually durable at capture time. Flushing here
// makes that invariant unconditional rather than relying on a prior Sync
// having happened to flush the same block, so any future write path that
// advances blockOffset before a batch cannot silently corrupt the rollback.
func captureCheckpointAfterEnsure(s *walShard, cp *shardCheckpoint) error {
	if cp.valid || s.activeSegment == nil {
		return nil
	}
	if err := s.activeSegment.flushBlock(); err != nil {
		return err
	}
	cp.fileOffset = s.activeSegment.fileOffset
	cp.blockOffset = s.activeSegment.blockOffset
	cp.recordCount = s.activeSegment.RecordCount()
	cp.seg = s.activeSegment
	cp.segID = s.activeSegmentID
	cp.valid = true
	return nil
}

// writeState serializes and writes a hard state record to the WAL.
// The in-memory state update is staged in pending for deferred application.
func (s *walShard) writeState(key nodeKey, shardID, replicaID uint64, state logdb.State, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	if err := captureCheckpointAfterEnsure(s, cp); err != nil {
		return err
	}

	buf := s.getMarshalBuf(1 + stateRecordSize)
	buf[0] = payloadTypeState
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	binary.LittleEndian.PutUint64(buf[17:], state.Term)
	binary.LittleEndian.PutUint64(buf[25:], state.Vote)
	binary.LittleEndian.PutUint64(buf[33:], state.Commit)

	landing, err := s.writeRecord(buf[:1+stateRecordSize])
	if err != nil {
		return err
	}
	pending.stageEpoch(landing)
	// Capture the segment the record landed in (post-rotation if writeRecord
	// rotated) so the commit point can pin it against garbage collection.
	pending.states = append(pending.states, pendingStateUpdate{
		key:         key,
		state:       state,
		landedSegID: landing.segID,
	})
	return nil
}

// writeSnapshot serializes and writes snapshot metadata to the WAL,
// including Epoch and full Membership state.
// The in-memory snapshot update is staged in pending for deferred application.
func (s *walShard) writeSnapshot(key nodeKey, shardID, replicaID uint64, ss logdb.Snapshot, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	// Enforce the same map-size cap replay enforces, BEFORE any segment is
	// touched: replay rejects an oversized map as corruption and fails the
	// whole recovery, so a record accepted here would be durable, acked and
	// unreadable — the node could never start again.
	if err := checkMembershipSize(ss.Membership); err != nil {
		return err
	}
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	if err := captureCheckpointAfterEnsure(s, cp); err != nil {
		return err
	}

	totalSize := 1 + snapshotRecordBaseSize + len(ss.Filepath) + membershipSize(ss.Membership)
	buf := s.getMarshalBuf(totalSize)
	buf[0] = payloadTypeSnapshot
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	binary.LittleEndian.PutUint64(buf[17:], ss.Index)
	binary.LittleEndian.PutUint64(buf[25:], ss.Term)
	binary.LittleEndian.PutUint64(buf[33:], ss.FileSize)
	binary.LittleEndian.PutUint64(buf[41:], ss.OnDiskIndex)
	binary.LittleEndian.PutUint64(buf[49:], ss.Epoch)
	binary.LittleEndian.PutUint16(buf[57:], uint16(ss.Compression))
	binary.LittleEndian.PutUint32(buf[59:], uint32(len(ss.Filepath)))
	copy(buf[63:], ss.Filepath)

	pos := 1 + snapshotRecordBaseSize + len(ss.Filepath)

	// Serialize membership.
	buf[pos] = membershipSectionVersion
	pos++
	binary.LittleEndian.PutUint64(buf[pos:], ss.Membership.ConfigChangeID)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:], ss.Membership.MaxWitnesses)
	pos += 8
	if ss.Membership.RequireFullReplicaMajority {
		buf[pos] = 1
	} else {
		buf[pos] = 0
	}
	pos++
	pos = marshalAddrMap(buf, pos, ss.Membership.Addresses)
	pos = marshalAddrMap(buf, pos, ss.Membership.Observers)
	pos = marshalAddrMap(buf, pos, ss.Membership.Witnesses)
	pos = marshalRemovedMap(buf, pos, ss.Membership.Removed)
	// Every remaining field is written unconditionally, so the section length
	// always matches membershipSize: an empty map is four zero bytes, not an
	// absent field, and an unset session policy is membershipSessionPolicySize
	// zero bytes rather than an absent one.
	pos = marshalAddrMap(buf, pos, ss.Membership.Identities)
	binary.LittleEndian.PutUint64(buf[pos:], ss.Membership.SessionExpiryEntries)
	binary.LittleEndian.PutUint64(buf[pos+8:], ss.Membership.MaxTotalSessions)
	binary.LittleEndian.PutUint64(buf[pos+16:], ss.Membership.MaxSessionsPerClient)
	binary.LittleEndian.PutUint64(buf[pos+24:], ss.Membership.MaxSessionsPerOwner)
	pos += membershipSessionPolicySize
	pos = marshalAddrMap(buf, pos, ss.Membership.RemovedIdentities)

	landing, err := s.writeRecord(buf[:pos])
	if err != nil {
		return err
	}
	pending.stageEpoch(landing)
	// Capture the segment the record landed in (post-rotation if writeRecord
	// rotated) so the commit point can pin it against garbage collection.
	// The snapshot is DEEP-COPIED before it is staged. ss is the caller's
	// value and its Membership carries six maps; staging it verbatim meant the
	// in-memory record waldb later serves from GetSnapshot aliased the
	// caller's maps in both directions, so a later mutation by the owning
	// layer silently rewrote a record already reported as persisted.
	//
	// memdb has done this since it was written (memdb.copyMembership, with a
	// comment explaining why), and the two are implementations of the SAME
	// LogDB interface: code correct against the test backend was wrong against
	// the production one. Identities and RemovedIdentities make it more than a
	// hygiene issue — they are the authorization state a replica recovering
	// from a snapshot has no other source for.
	pending.snapshots = append(pending.snapshots, pendingSnapshotUpdate{
		key:         key,
		snapshot:    copySnapshot(ss),
		landedSegID: landing.segID,
	})
	return nil
}

// writeBootstrap serializes and writes bootstrap info to the WAL.
//
// recordDataDir selects the on-wire shape. True appends the trailing DataDir
// length prefix, which is what every record this version writes carries and
// what replayBootstrap reads back as DataDirRecorded. False ends the record at
// the address map — byte-for-byte what a pre-DataDir writer produced — and is
// reserved for a migration copying a record that genuinely predates the field;
// see DB.saveBootstrapPreservingProvenance for why absence must be copyable.
//
// INVARIANT: This method MUST only be called outside of batchMode (i.e. from
// SaveBootstrap). In non-batch mode, writeRecord calls fdatasync inline before
// returning, so the immediate in-memory update below is safe — the data is
// already durable. If this method were ever called in batchMode, the immediate
// update would create a phantom read before fsync. Callers adding batch-mode
// bootstrap support must route through pendingMemUpdates instead.
func (s *walShard) writeBootstrap(key nodeKey, shardID, replicaID uint64, bs logdb.Bootstrap, recordDataDir bool) error {
	// Guard against accidental batch-mode usage. See invariant above.
	if s.batchMode {
		return &WriteBootstrapBatchModeError{}
	}
	// Symmetric with replayBootstrap, which rejects an oversized address count
	// as corruption: see checkMembershipSize.
	if len(bs.Addresses) > maxMembershipMapEntries {
		return ErrMembershipTooLarge
	}
	if len(bs.DataDir) > maxBootstrapDataDirLen {
		return ErrMembershipTooLarge
	}
	// Calculate total size.
	totalSize := 1 + bootstrapRecordBaseSize
	for _, addr := range bs.Addresses {
		totalSize += 8 + 4 + len(addr)
	}
	if recordDataDir {
		totalSize += bootstrapDataDirLenSize + len(bs.DataDir)
	}

	buf := s.getMarshalBuf(totalSize)
	buf[0] = payloadTypeBootstrap
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	if bs.Join {
		buf[17] = 1
	} else {
		buf[17] = 0
	}
	binary.LittleEndian.PutUint64(buf[18:], bs.Type)
	binary.LittleEndian.PutUint32(buf[26:], uint32(len(bs.Addresses)))

	pos := 1 + bootstrapRecordBaseSize
	for rid, addr := range bs.Addresses {
		binary.LittleEndian.PutUint64(buf[pos:], rid)
		binary.LittleEndian.PutUint32(buf[pos+8:], uint32(len(addr)))
		copy(buf[pos+12:], addr)
		pos += 12 + len(addr)
	}
	if recordDataDir {
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(bs.DataDir)))
		pos += bootstrapDataDirLenSize
		copy(buf[pos:], bs.DataDir)
		pos += len(bs.DataDir)
	}

	landing, err := s.writeRecord(buf[:pos])
	if err != nil {
		return err
	}
	// Commit point: publish the bootstrap record, pin the segment it landed in
	// (post-rotation if writeRecord rotated) against garbage collection, raise
	// the commit high-water mark and lower the epoch floor to this record's
	// epoch — atomically for readers, under dataMu. The write is already
	// durable here because non-batch writeRecord fdatasyncs inline.
	s.dataMu.Lock()
	// The in-memory copy must describe the BYTES just written: a GetBootstrap
	// served before the next restart has to agree with a replay of the very same
	// record, in both directions. A record written without the length prefix
	// replays as absent, so the in-memory copy says absent too.
	bs.DataDirRecorded = recordDataDir
	if !recordDataDir {
		bs.DataDir = ""
	}
	// Deep-copied for the same reason writeSnapshot is: bs is the caller's
	// value, its Addresses map is retained here and served from GetBootstrap,
	// and memdb copies. Storing the reference let the owning layer mutate a
	// record waldb had already reported as persisted.
	s.bootstraps[key] = copyBootstrap(bs)
	s.setBootstrapSegment(key, landing.segID)
	s.advanceCommittedHWM(landing.segID)
	s.applyEpochLocked(landing)
	s.dataMu.Unlock()
	return nil
}

// getMarshalBuf returns a slice from the reusable marshal buffer, allocating
// or growing it as needed. The initial allocation is deferred until the first
// call (lazy shard initialization).
func (s *walShard) getMarshalBuf(size int) []byte {
	if s.marshalBuf == nil {
		bufSize := 32 * 1024 // Start at WAL block size; grows on demand.
		if size > bufSize {
			bufSize = size * 2
		}
		s.marshalBuf = make([]byte, bufSize)
	} else if size > len(s.marshalBuf) {
		s.marshalBuf = make([]byte, size*2)
	}
	return s.marshalBuf[:size]
}

// IterateEntries returns log entries in the range [low, high) for the given
// node, up to maxSize bytes total. Results are appended to the entries slice.
// Error semantics and the maxSize budget (ie.Size == EntryOverheadBytes +
// len(Cmd) per entry) follow the LogDB interface contract.
//
// A stored range that does not begin exactly at low — for example when a
// compaction marker was lost and low sits below the true compaction floor —
// returns ErrLogGapDetected instead of silently serving a shifted range with
// a hole in it.
func (db *DB) IterateEntries(
	entries []logdb.Entry, size uint64,
	shardID, replicaID, low, high, maxSize uint64,
) ([]logdb.Entry, uint64, error) {
	if db.closed.Load() {
		return entries, size, logdb.ErrClosed
	}
	if low >= high {
		return entries, size, nil
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// Read lock on the in-memory maps ONLY. This path never touches a segment,
	// so it must not wait behind a concurrent batch's fdatasync on shard.mu;
	// dataMu still serializes it against the batch's atomic Phase-3 commit.
	shard.dataMu.RLock()
	defer shard.dataMu.RUnlock()

	compactedTo := shard.compactedTo[key]
	if low <= compactedTo {
		return entries, size, logdb.ErrCompacted
	}

	idx, ok := shard.indexes[key]
	if !ok {
		return entries, size, logdb.ErrUnavailable
	}

	nodeEntries := shard.entries[key]
	rangeEntries := idx.Range(low, high)
	if len(rangeEntries) == 0 {
		return entries, size, logdb.ErrUnavailable
	}
	if rangeEntries[0].Index != low {
		slog.Error("waldb: stored entries do not start at requested index",
			"shard_id", shardID,
			"replica_id", replicaID,
			"requested_low", low,
			"first_stored", rangeEntries[0].Index,
			"compacted_to", compactedTo,
		)
		return entries, size, logdb.ErrLogGapDetected
	}

	totalSize := size
	expected := low
	for _, ie := range rangeEntries {
		e, found := nodeEntries[ie.Index]
		if !found || ie.Index != expected {
			// An index/entry-map divergence or an interior hole must fail
			// loud, never be served as a silently shortened result.
			slog.Error("waldb: entry missing inside requested range",
				"shard_id", shardID,
				"replica_id", replicaID,
				"expected_index", expected,
				"index_entry", ie.Index,
				"in_entry_map", found,
			)
			return entries, size, logdb.ErrLogGapDetected
		}

		entryBytes := uint64(ie.Size)

		// Enforce maxSize: always include at least one entry, then stop
		// before an entry that would exceed the budget.
		if maxSize > 0 && totalSize+entryBytes > maxSize && len(entries) > 0 {
			break
		}

		entries = append(entries, e)
		totalSize += entryBytes
		expected++
	}

	return entries, totalSize, nil
}

// ReadState reads the persisted hard state and membership for the given node.
// The membership is sourced from the latest snapshot metadata for this node.
func (db *DB) ReadState(shardID, replicaID uint64) (logdb.State, logdb.Membership, error) {
	if db.closed.Load() {
		return logdb.State{}, logdb.Membership{}, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// In-memory read only; see IterateEntries for why this takes dataMu, not mu.
	shard.dataMu.RLock()
	state, ok := shard.states[key]
	ss := shard.snapshots[key]
	shard.dataMu.RUnlock()

	if !ok {
		return logdb.State{}, logdb.Membership{}, logdb.ErrNoState
	}
	// The membership is copied on the way out: ReadState hands back waldb's
	// own map otherwise, and a caller that mutates what it was given would
	// rewrite the record every later reader sees.
	return state, copyMembership(ss.Membership), nil
}

// EntryRange returns the first index and the contiguous span of available
// log entries for the given node. Returns (0, 0, nil) when no entries exist.
//
// The length is derived from the first/last indices (last-first+1), never
// from the raw entry count: a holed index would otherwise yield a short
// count, LogReader.SetRange would compute a short lastIndex, and the gap
// would be hidden instead of surfaced. A count/span mismatch returns
// ErrLogGapDetected.
func (db *DB) EntryRange(shardID, replicaID uint64) (uint64, uint64, error) {
	if db.closed.Load() {
		return 0, 0, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// In-memory read only; see IterateEntries for why this takes dataMu, not mu.
	shard.dataMu.RLock()
	idx, ok := shard.indexes[key]
	shard.dataMu.RUnlock()

	if !ok {
		return 0, 0, nil
	}

	first, last, count := idx.Bounds()
	if count == 0 {
		return 0, 0, nil
	}
	span := last.Index - first.Index + 1
	if span != uint64(count) {
		slog.Error("waldb: entry index span does not match entry count",
			"shard_id", shardID,
			"replica_id", replicaID,
			"first_index", first.Index,
			"last_index", last.Index,
			"count", count,
		)
		return 0, 0, logdb.ErrLogGapDetected
	}
	return first.Index, span, nil
}

// RemoveEntriesTo removes log entries up to and including the given index
// for the specified node.
func (db *DB) RemoveEntriesTo(shardID, replicaID, index uint64) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// The compaction floor is monotonic. Entries below an earlier, HIGHER
	// floor are already deleted, so a lower floor can never be honoured: reads
	// between the two floors would see a hard log hole (ErrLogGapDetected)
	// instead of ErrCompacted and raft would never fall back to
	// InstallSnapshot. The call is dropped before it reaches the WAL because
	// persisting the lower marker also re-pins garbage collection to it,
	// letting the higher marker's segment be reclaimed — the regression would
	// then survive restart. Callers can lower the floor legitimately only by
	// removing the node. Reading compactedTo under shard.mu alone is sufficient:
	// every mutator of it holds shard.mu as well.
	if cur, ok := shard.compactedTo[key]; ok && index <= cur {
		return nil
	}

	// Capture the pre-write segment position so a failed write leaves nothing
	// durable behind, matching SaveState's checkpoint/rollback discipline.
	var cp shardCheckpoint
	if err := captureShardCheckpoint(shard, &cp); err != nil {
		return err
	}

	// Write compaction marker to WAL for durability across restarts.
	buf := shard.getMarshalBuf(1 + compactionRecordSize)
	buf[0] = payloadTypeCompact
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	binary.LittleEndian.PutUint64(buf[17:], index)
	landing, err := shard.writeRecord(buf[:1+compactionRecordSize])
	if err != nil {
		return db.rollbackStandalone(shard, &cp, err)
	}

	// Commit point. Everything below is in-memory state the lock-free readers
	// consume, so it is applied as one atomic dataMu section: a reader must
	// never see the raised compaction floor without the matching index/entry
	// pruning (or the reverse, which reads as a log gap).
	//
	// The pin of the segment the compaction marker landed in and the commit
	// high-water mark mirror writeBootstrap. Without the pin, GC could reclaim
	// the marker's segment once its entries are compacted away, and compactedTo
	// would reset to 0 on recovery. The marker's own epoch joins the floor in
	// the same section. The write is durable here (non-batch writeRecord
	// fdatasyncs inline).
	shard.dataMu.Lock()
	defer shard.dataMu.Unlock()

	shard.setCompactSegment(key, landing.segID)
	shard.advanceCommittedHWM(landing.segID)
	shard.applyEpochLocked(landing)

	// Update in-memory compaction marker.
	shard.compactedTo[key] = index

	// Remove compacted entries from the index.
	if idx, ok := shard.indexes[key]; ok {
		idx.Compact(index)
	}

	// Remove compacted entries from the in-memory store.
	if nodeEntries, ok := shard.entries[key]; ok {
		for raftIdx := range nodeEntries {
			if raftIdx <= index {
				delete(nodeEntries, raftIdx)
			}
		}
	}

	return nil
}

// Compact triggers compaction for the given node. For the WAL implementation,
// this triggers garbage collection of obsolete segment files.
func (db *DB) Compact(shardID, _ uint64) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	compactStart := time.Now()

	shard := db.getShard(shardID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// The live-set survey reads indexes, metaSegments and committedSegmentHWM
	// without dataMu: every mutator of those fields also holds shard.mu, which
	// is held here, so mu alone already excludes them. dataMu exists only to
	// exclude the lock-free readers, which mutate nothing.
	comp := newCompactor(shard.fs, shard.dir, shard.opts.blockSize)
	liveSegments := make(map[uint64]bool)
	for _, idx := range shard.indexes {
		for segID := range idx.AllSegmentIDs() {
			liveSegments[segID] = true
		}
	}
	// Pin segments that hold the latest State/Snapshot/Bootstrap records.
	// These are never tracked by an entry index, so a segment whose entries
	// have all been compacted away could otherwise be deleted while still
	// holding the sole durable copy of a node's metadata.
	shard.addMetaSegmentsToLive(liveSegments)

	deleted, err := comp.GarbageCollect(liveSegments, shard.activeSegmentID, shard.committedSegmentHWM)

	// Fsync the parent directory so the segment-file deletions survive a
	// crash. Without this a metadata-lazy filesystem could resurrect a
	// deleted segment, replaying records that were supposed to be gone.
	// Best-effort for the deletions themselves: the next compaction retries.
	//
	// This MUST precede the epoch-floor recompute below. The recompute raises
	// the shard's minWriteEpoch on the strength of the unlinks, and the engine
	// purges the corresponding DEK once MinLiveEpoch reports the higher floor.
	// If the directory fsync failed (or the process crashed first) the unlink
	// is not durable, the segment reappears on restart, and replay hits a
	// fatal WALDecryptError with the DEK already gone — the node cannot start
	// at all. Deferring the floor raise is always safe; raising it on an
	// unproven deletion is not.
	dirSynced := true
	if deleted > 0 {
		if syncErr := syncDirFS(shard.fs, shard.dir); syncErr != nil {
			dirSynced = false
			slog.Warn("waldb: directory sync failed after compaction; "+
				"skipping epoch-floor recompute so an unproven deletion cannot purge a live DEK",
				"shard_dir", shard.dir,
				"shard_id", shardID,
				"error", syncErr,
			)
		}
	}

	// Recompute minWriteEpoch from the segments that actually survived ON
	// DISK — never from the intended liveSegments set. GC deletes files one at
	// a time and can abort partway, so a segment absent from liveSegments may
	// still exist; dropping its segmentEpochs entry would raise the epoch
	// floor past an epoch that segment still needs. The recompute is skipped
	// entirely when GC failed (conservative: a too-low floor only defers DEK
	// purging, never breaks recovery) and only needed when segments were
	// actually deleted and a barrier is configured (otherwise segmentEpochs is
	// empty).
	if err == nil && deleted > 0 && dirSynced && shard.barrier != nil {
		shard.recomputeMinWriteEpochFromDisk()
	}

	if db.metrics != nil {
		db.metrics.ObserveCompactionLatency(time.Since(compactStart))
	}

	return err
}

// SaveSnapshot persists snapshot metadata for the given node. Returns
// ErrSnapshotOutOfDate if the snapshot index is less than or equal to
// the currently stored snapshot index (stale snapshot rejection).
func (db *DB) SaveSnapshot(shardID, replicaID uint64, snapshot logdb.Snapshot) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// A snapshot at index 0 is not a snapshot: index 0 is the empty log, so it
	// covers nothing and can never be newer than what is stored. The staleness
	// rule below rejects it whenever a snapshot IS stored, but on a fresh node
	// there was nothing to compare against and it went straight through —
	// writing a WAL record, publishing a snapshot GetSnapshot reports as
	// existing at no index, and pinning the segment it landed in against
	// garbage collection for as long as no real snapshot replaces it. Extend
	// the same rule to the fresh node; the SaveState path has always required
	// Index > 0.
	if snapshot.Index == 0 {
		return logdb.ErrSnapshotOutOfDate
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Reject stale snapshots, consistent with MemDB behavior.
	if existing, ok := shard.snapshots[key]; ok && snapshot.Index <= existing.Index {
		return logdb.ErrSnapshotOutOfDate
	}

	// Standalone snapshot write — apply the in-memory update immediately since
	// there is no batch to fail, but roll the segment back on a write failure
	// so no partial or complete record stays durable after an error return.
	var p pendingMemUpdates
	var cp shardCheckpoint
	if err := captureShardCheckpoint(shard, &cp); err != nil {
		return err
	}
	if err := shard.writeSnapshot(key, shardID, replicaID, snapshot, &p, &cp); err != nil {
		return db.rollbackStandalone(shard, &cp, err)
	}
	p.applyTo(shard)
	return nil
}

// GetSnapshot returns the most recent snapshot metadata for the given node.
func (db *DB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if db.closed.Load() {
		return logdb.Snapshot{}, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// In-memory read only; see IterateEntries for why this takes dataMu, not mu.
	shard.dataMu.RLock()
	ss, ok := shard.snapshots[key]
	shard.dataMu.RUnlock()

	if !ok {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}
	// Deep-copied on the way OUT as well as in. Returning the stored value
	// hands the caller waldb's own maps: a caller that mutates the membership
	// it received — reasonably, since it was handed a value — rewrites the
	// record waldb still serves to everyone else, with no write and no error.
	// memdb copies on both sides; these are two implementations of one
	// interface and must not differ on who owns the memory.
	return copySnapshot(ss), nil
}

// RemoveNodeData durably removes all data for the given node. A remove-node
// record is written to the WAL so the removal persists across restarts.
// After clearing in-memory state, segment garbage collection is triggered
// to reclaim disk space from segments that no longer contain live data.
func (db *DB) RemoveNodeData(shardID, replicaID uint64) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Capture the pre-write segment position: a tombstone left durable after an
	// error return would erase the node on the next restart even though the
	// caller was told the removal failed.
	var cp shardCheckpoint
	if err := captureShardCheckpoint(shard, &cp); err != nil {
		return err
	}

	// Write the remove-node record to the WAL for durability.
	buf := shard.getMarshalBuf(1 + removeNodeRecordSize)
	buf[0] = payloadTypeRemoveNode
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	landing, err := shard.writeRecord(buf[:1+removeNodeRecordSize])
	if err != nil {
		return db.rollbackStandalone(shard, &cp, err)
	}
	// The tombstone landed in landing.segID (post-rotation). The record is
	// durable here (non-batch writeRecord fdatasyncs inline).
	tombstoneSegID := landing.segID

	// Commit point: clear all in-memory state for this node and re-pin the
	// tombstone in one atomic dataMu section, so a lock-free reader sees the
	// node either wholly present or wholly gone. Dropping the removed node's
	// meta tracking lets its now-obsolete State/Snapshot/Bootstrap segments be
	// reclaimed below if no surviving node references them.
	//
	// The tombstone's own segment is pinned against garbage collection and the
	// commit high-water mark advanced, mirroring the compaction-marker pin.
	// Without the pin, GC could reclaim the tombstone's segment while OLDER
	// segments holding this node's records survive (pinned by other nodes
	// sharing the WAL shard) — replay would then resurrect the removed replica
	// (see setRemoveSegment).
	shard.dataMu.Lock()
	delete(shard.indexes, key)
	delete(shard.entries, key)
	delete(shard.states, key)
	delete(shard.bootstraps, key)
	delete(shard.snapshots, key)
	delete(shard.compactedTo, key)
	delete(shard.metaSegments, key)
	shard.setRemoveSegment(key, tombstoneSegID)
	shard.advanceCommittedHWM(tombstoneSegID)
	// The tombstone is durable, so its epoch joins the floor — before the
	// recompute below, which would otherwise drop it.
	shard.applyEpochLocked(landing)
	shard.dataMu.Unlock()

	// Trigger segment garbage collection to reclaim disk space. Collect
	// the set of segments still referenced by remaining live nodes.
	liveSegments := make(map[uint64]bool)
	for _, idx := range shard.indexes {
		for segID := range idx.AllSegmentIDs() {
			liveSegments[segID] = true
		}
	}
	// Keep surviving nodes' State/Snapshot/Bootstrap segments and RemoveNode
	// tombstones pinned. These record types are not tracked by an entry
	// index, so they must be folded in explicitly before GC.
	shard.addMetaSegmentsToLive(liveSegments)
	comp := newCompactor(shard.fs, shard.dir, shard.opts.blockSize)
	// Best-effort GC: errors are non-fatal for the remove operation
	// but are logged for observability.
	_, gcErr := comp.GarbageCollect(liveSegments, shard.activeSegmentID, shard.committedSegmentHWM)
	if gcErr != nil {
		slog.Warn("waldb: garbage collection failed after RemoveNodeData",
			"shard_dir", shard.dir,
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", gcErr,
		)
	}

	// Fsync the parent directory to make segment deletions durable.
	// Best-effort for the deletions: errors are logged but do not fail the
	// remove operation, because the WAL remove-node record is already
	// persisted. It MUST run BEFORE the epoch-floor recompute below — see the
	// identical ordering rationale in Compact: raising the floor on an unlink
	// that a crash can undo lets the engine purge a DEK the resurrected
	// segment still needs, and the next restart fails with WALDecryptError.
	dirSynced := true
	if err := syncDirFS(shard.fs, shard.dir); err != nil {
		dirSynced = false
		slog.Warn("waldb: directory sync failed after RemoveNodeData; "+
			"skipping epoch-floor recompute so an unproven deletion cannot purge a live DEK",
			"shard_dir", shard.dir,
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", err,
		)
	}

	// Recompute minWriteEpoch from the segments that actually survived ON
	// DISK, and only after a fully successful GC. Without a recompute, the
	// minimum live epoch can stay pinned to an epoch whose segments were just
	// GC'd, indefinitely blocking DEK purging for that epoch (M9). Driving
	// the recompute from the intended liveSegments set instead of the on-disk
	// survivors would be unsafe: a GC that failed partway (or whose ReadDir
	// failed outright) leaves non-live segment files on disk, and dropping
	// their segmentEpochs entries would raise the floor past an epoch those
	// segments still need — the engine would purge the DEK and the next
	// restart would hit a fatal WALDecryptError. Skipping the recompute on GC
	// failure is conservative: a too-low floor only defers DEK purging.
	if gcErr == nil && dirSynced && shard.barrier != nil {
		shard.recomputeMinWriteEpochFromDisk()
	}

	return nil
}

// ListNodeInfo returns all NodeInfo entries in the LogDB.
func (db *DB) ListNodeInfo() ([]logdb.NodeInfo, error) {
	if db.closed.Load() {
		return nil, logdb.ErrClosed
	}

	seen := make(map[nodeKey]bool)
	for i := 0; i < DefaultShardCount; i++ {
		shard := db.shards[i]
		// In-memory read only; see IterateEntries for why this takes dataMu,
		// not mu. It matters most here: this walks EVERY shard, so taking mu
		// would serialize the scan behind up to DefaultShardCount fdatasyncs.
		shard.dataMu.RLock()
		for key := range shard.indexes {
			seen[key] = true
		}
		for key := range shard.states {
			seen[key] = true
		}
		for key := range shard.bootstraps {
			seen[key] = true
		}
		for key := range shard.snapshots {
			seen[key] = true
		}
		shard.dataMu.RUnlock()
	}

	result := make([]logdb.NodeInfo, 0, len(seen))
	for key := range seen {
		result = append(result, logdb.NodeInfo{
			ShardID:   key.ShardID,
			ReplicaID: key.ReplicaID,
		})
	}
	return result, nil
}

// SaveBootstrap persists bootstrap information for a node.
//
// The record always carries the DataDir field, so the stored bootstrap reports
// DataDirRecorded true regardless of what the caller passed: a caller states a
// DataDir, it does not get to state that nobody ever stated one. See
// saveBootstrapPreservingProvenance for the one path that must.
func (db *DB) SaveBootstrap(shardID, replicaID uint64, bootstrap logdb.Bootstrap) error {
	return db.saveBootstrap(shardID, replicaID, bootstrap, true)
}

// saveBootstrapPreservingProvenance persists a bootstrap record COPIED from
// another DB, keeping the source record's DataDir provenance intact.
//
// A record written before the DataDir field existed carries no DataDir at all,
// and "absent" is a different statement from "empty": empty means the shard
// roots its snapshot tree under the host's NodeHostDir, absent means nothing is
// known. Copying such a record through SaveBootstrap would stamp
// DataDirRecorded true and so turn a legacy record into an explicit
// DataDir="" — which Host.ImportSnapshot then acts on, writing the artifact
// under NodeHostDir for a shard whose tree is actually under its own DataDir
// (outside everything snapshot GC and the DEK purge-floor scan enumerate) while
// simultaneously refusing the operator's WithImportDataDir override as a
// contradiction. A re-encryption migration would silently create that state for
// every legacy shard it copied.
//
// It is deliberately not exported and not reachable through the logdb.LogDB
// interface: only a copy of an existing record may claim absence, and only
// because the record it came from claimed it.
func (db *DB) saveBootstrapPreservingProvenance(shardID, replicaID uint64, bootstrap logdb.Bootstrap) error {
	if !bootstrap.DataDirRecorded && bootstrap.DataDir != "" {
		// The source cannot have produced this: replay only fills DataDir when
		// the record carried the field. Fail closed rather than drop a DataDir
		// whose loss costs the artifact's DEK.
		return ErrBootstrapDataDirProvenance
	}
	return db.saveBootstrap(shardID, replicaID, bootstrap, bootstrap.DataDirRecorded)
}

// saveBootstrap writes a bootstrap record. recordDataDir selects the on-wire
// shape: true appends the DataDir length prefix (and so states the field as
// fact), false ends the record at the address map, which is exactly what a
// pre-DataDir writer produced and what replayBootstrap reads back as absent.
func (db *DB) saveBootstrap(shardID, replicaID uint64, bootstrap logdb.Bootstrap, recordDataDir bool) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Standalone bootstrap write. This was the ONE standalone writer without a
	// checkpoint — SaveSnapshot, RemoveEntriesTo and RemoveNodeData have all
	// had one — so a failed write left its bytes in the segment and returned an
	// error the caller reads as "nothing happened". Those bytes are a partial
	// record, and the next successful write lands directly on top of them: what
	// recovery then finds is a torn record with valid data above it, in the
	// middle of a live segment. That is the shape recovery must fail closed on,
	// so one failed SaveBootstrap could cost the whole shard's WAL on the next
	// restart.
	var cp shardCheckpoint
	if err := captureShardCheckpoint(shard, &cp); err != nil {
		return err
	}
	if err := shard.writeBootstrap(key, shardID, replicaID, bootstrap, recordDataDir); err != nil {
		return db.rollbackStandalone(shard, &cp, err)
	}
	return nil
}

// GetBootstrap returns the bootstrap information for a node.
func (db *DB) GetBootstrap(shardID, replicaID uint64) (logdb.Bootstrap, bool, error) {
	if db.closed.Load() {
		return logdb.Bootstrap{}, false, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// In-memory read only; see IterateEntries for why this takes dataMu, not mu.
	shard.dataMu.RLock()
	bs, ok := shard.bootstraps[key]
	shard.dataMu.RUnlock()

	if !ok {
		return logdb.Bootstrap{}, false, nil
	}
	return copyBootstrap(bs), true, nil
}

// Close closes the DB and releases all resources. Close is idempotent: a
// second call is a no-op returning nil, so the common `defer db.Close()`
// alongside an explicit Close does not surface a spurious error.
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		return nil
	}

	var firstErr error
	for i := 0; i < DefaultShardCount; i++ {
		if err := db.shards[i].close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SaveEntries persists entries for a single node (convenience method).
func (db *DB) SaveEntries(shardID, replicaID uint64, entries []logdb.Entry) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	return db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Entries:   entries,
	}})
}

// GetEntries returns entries in [low, high) up to maxSize bytes. It is a
// convenience wrapper over IterateEntries with lookup semantics: an unknown
// node or a range holding no entries yields (nil, nil) so callers can probe
// for presence without error handling. ErrCompacted and ErrLogGapDetected
// propagate unchanged — a compacted or holed range must never read as
// merely absent.
func (db *DB) GetEntries(shardID, replicaID, low, high, maxSize uint64) ([]logdb.Entry, error) {
	entries, _, err := db.IterateEntries(nil, 0, shardID, replicaID, low, high, maxSize)
	if err != nil {
		if errors.Is(err, logdb.ErrUnavailable) {
			return nil, nil
		}
		return nil, err
	}
	return entries, nil
}

// SaveNodeState persists the hard state for a single node (convenience
// method).
func (db *DB) SaveNodeState(shardID, replicaID uint64, state logdb.State) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	return db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     state,
	}})
}

// GetState returns the hard state for a node.
func (db *DB) GetState(shardID, replicaID uint64) (logdb.State, error) {
	state, _, err := db.ReadState(shardID, replicaID)
	return state, err
}

// CompactedTo returns the node's durable compaction floor: the highest log
// index removed by RemoveEntriesTo (or replayed from its WAL marker). Zero
// means the node has never been compacted. Reencrypt uses it to carry the
// floor to a migrated WAL so requests below the floor keep returning
// ErrCompacted instead of a gapped range.
func (db *DB) CompactedTo(shardID, replicaID uint64) (uint64, error) {
	if db.closed.Load() {
		return 0, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// In-memory read only; see IterateEntries for why this takes dataMu, not mu.
	shard.dataMu.RLock()
	compactedTo := shard.compactedTo[key]
	shard.dataMu.RUnlock()

	return compactedTo, nil
}

// MinLiveEpoch returns the minimum barrier encryption epoch across all
// live WAL records in all shards. Returns 0 when no barrier is configured
// or no encrypted records exist.
//
// This method is used by the snapshot pool's epoch purge logic to prevent
// premature DEK purging: WAL segments may still contain records encrypted
// at an older epoch than the oldest retained snapshot. Purging that
// epoch's DEK would cause WALDecryptError on crash recovery.
func (db *DB) MinLiveEpoch() (uint64, error) {
	if db.closed.Load() {
		return 0, logdb.ErrClosed
	}

	if db.barrier == nil {
		return 0, nil
	}

	var globalMin uint64
	for i := 0; i < DefaultShardCount; i++ {
		shard := db.shards[i]
		// In-memory read only; see IterateEntries for why this takes dataMu,
		// not mu. Like ListNodeInfo this walks every shard, so mu would put
		// the epoch query behind every in-flight fdatasync.
		shard.dataMu.RLock()
		shardMin := shard.minWriteEpoch
		shard.dataMu.RUnlock()

		if shardMin > 0 && (globalMin == 0 || shardMin < globalMin) {
			globalMin = shardMin
		}
	}
	return globalMin, nil
}

// MinLiveEpochForBarrier returns the minimum barrier encryption epoch across
// all live WAL records in this DB, but only when the given barrier handle is
// the SAME barrier this DB was opened with. A waldb.DB encrypts every record
// it stores with exactly one barrier (db.barrier), so it can answer the
// per-barrier query precisely by identity: when the requested barrier matches,
// the global MinLiveEpoch is exactly the min-epoch-for-that-barrier; when it
// does not match (or no barrier is configured), this DB holds no records under
// that barrier and the floor contribution is 0.
//
// This is the single-barrier-per-store building block. A multiplexing LogDB
// that fans shards across several dedicated WALs (one barrier each) implements
// PerBarrierEpochAwareLogDB by delegating to whichever underlying DB owns the
// requested barrier; each such DB answers via this method.
//
// The barrier argument is compared by interface identity. Both db.barrier and
// the argument box the same concrete *seal.Barrier pointer, so boxing to any
// normalizes them to (concrete type, value) and == is an exact pointer match
// regardless of the static interface type the caller holds (SnapshotBarrier,
// Barrier, WALBarrier).
//
// The return values follow the fail-safe contract of
// logdb.PerBarrierEpochAwareLogDB: this DB returns known == true only when the
// requested barrier is its OWN barrier (identity match), in which case the
// epoch is its global MinLiveEpoch (possibly 0 = a true non-constraint). For
// any other barrier — or when this DB has no barrier — it returns known ==
// false so the caller fails safe instead of mistaking "not my barrier" for
// "zero live records".
func (db *DB) MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error) {
	if db.closed.Load() {
		return 0, false, logdb.ErrClosed
	}
	if db.barrier == nil || barrier == nil {
		return 0, false, nil
	}
	if any(db.barrier) != barrier {
		// This DB's records are not encrypted under the requested barrier:
		// known == false (NOT a zero-epoch non-constraint).
		return 0, false, nil
	}
	min, mErr := db.MinLiveEpoch()
	if mErr != nil {
		return 0, false, mErr
	}
	return min, true, nil
}

// AtRestProtection reports whether this DB's on-disk records are encrypted.
//
// A waldb.DB writes every record it accepts to a WAL segment on disk, so the
// answer turns on whether the configured barrier ACTUALLY ENCRYPTS -- not on
// whether one was configured. Without a barrier every record, and therefore
// every user command inside every Raft log entry, lands on disk in cleartext.
//
// A barrier running StrategyNone lands them there too. Its "encryptor" is an
// identity pass-through, so `db.barrier != nil` says nothing about the bytes on
// the medium: the records are byte-for-byte their plaintext, and a grep over a
// segment file finds the commands. Answering AtRestBarrierEncrypted for it made
// the presence of an OBJECT stand in for the presence of ENCRYPTION and handed
// the host's requireEncryptedLogDB gate -- whose entire purpose is to refuse
// rather than silently downgrade -- exactly the store it exists to refuse. The
// barrier already knows the answer and reports it through plaintextModeReporter;
// this asks.
//
// A barrier that does not implement plaintextModeReporter is read as
// encrypting. That is the conservative direction for THIS question: the
// optional interface exists to declare an absence of encryption, so its own
// absence cannot be evidence of one, and treating every barrier that predates
// the interface as plaintext would refuse working encrypted deployments.
//
// This is deliberately NOT answerable by a type assertion on the LogDB: *DB
// implements PerBarrierEpochAwareLogDB in ALL of these cases, so a caller that
// treats the interface as evidence of encryption accepts the plaintext store.
func (db *DB) AtRestProtection() logdb.AtRestProtection {
	if db.barrier == nil {
		return logdb.AtRestUnprotected
	}
	if pm, ok := db.barrier.(plaintextModeReporter); ok && pm.IsPlaintextMode() {
		return logdb.AtRestUnprotected
	}
	return logdb.AtRestBarrierEncrypted
}

// Compile-time interface checks.
var _ logdb.EpochAwareLogDB = (*DB)(nil)
var _ logdb.PerBarrierEpochAwareLogDB = (*DB)(nil)
var _ logdb.AtRestProtectedLogDB = (*DB)(nil)

// setStateSegment records segID as the segment holding the latest durable
// State record for key. Last write wins. Caller must hold s.mu and
// s.dataMu for writing, except on the recover() path (see walShard).
func (s *walShard) setStateSegment(key nodeKey, segID uint64) {
	if s.metaSegments == nil {
		s.metaSegments = make(map[nodeKey]metaSegments)
	}
	m := s.metaSegments[key]
	m.state = segID
	m.hasState = true
	s.metaSegments[key] = m
}

// setSnapshotSegment records segID as the segment holding the latest durable
// Snapshot record for key. Last write wins. Caller must hold s.mu and
// s.dataMu for writing, except on the recover() path (see walShard).
func (s *walShard) setSnapshotSegment(key nodeKey, segID uint64) {
	if s.metaSegments == nil {
		s.metaSegments = make(map[nodeKey]metaSegments)
	}
	m := s.metaSegments[key]
	m.snapshot = segID
	m.hasSnapshot = true
	s.metaSegments[key] = m
}

// setBootstrapSegment records segID as the segment holding the durable
// Bootstrap record for key. Last write wins. Caller must hold s.mu and
// s.dataMu for writing, except on the recover() path (see walShard).
func (s *walShard) setBootstrapSegment(key nodeKey, segID uint64) {
	if s.metaSegments == nil {
		s.metaSegments = make(map[nodeKey]metaSegments)
	}
	m := s.metaSegments[key]
	m.bootstrap = segID
	m.hasBootstrap = true
	s.metaSegments[key] = m
}

// setRemoveSegment records segID as the segment holding the latest durable
// RemoveNode tombstone for key. The tombstone must outlive every lower-ID
// segment that still holds the removed node's records: WAL shards are shared
// by many Raft groups, so older segments containing the removed node's
// entries/state/snapshot can stay pinned by OTHER nodes long after the
// removal. If GC reclaimed the tombstone's segment while such an older
// segment survived, replay would resurrect the removed replica — a
// membership-safety violation. The pin is therefore held until a later
// RemoveNodeData for the same node (or its replay) rewrites it; it is never
// dropped based on GC progress.
//
// Re-adding the same (shardID, replicaID) does NOT release the pin either,
// even though later State/Bootstrap/Entries records supersede the tombstone.
// GarbageCollect deletes segments in ascending ID order but skips any segment
// pinned by ANOTHER node sharing this WAL shard, so a segment BELOW the
// tombstone can outlive it. Replaying that older segment without the
// intervening tombstone would resurrect the removed replica's entries above
// the re-added log's range. The cost of holding the pin is one retained
// segment per removed key — disk only, never correctness.
//
// Last write wins. Caller must hold s.mu and s.dataMu for writing, except on
// the recover() path (see walShard).
func (s *walShard) setRemoveSegment(key nodeKey, segID uint64) {
	if s.metaSegments == nil {
		s.metaSegments = make(map[nodeKey]metaSegments)
	}
	m := s.metaSegments[key]
	m.remove = segID
	m.hasRemove = true
	s.metaSegments[key] = m
}

// setCompactSegment records segID as the segment holding the latest durable
// compaction marker for key. A compaction marker is not tracked by any entry
// index, so without this pin a (rotated, non-active) segment whose entries have
// all been compacted away could be garbage-collected, discarding the marker —
// on recovery compactedTo would then reset to 0 and GetEntries/IterateEntries
// would return empty instead of ErrCompacted for compacted indices. Last write
// wins. Caller must hold s.mu and s.dataMu for writing, except on the recover()
// path (see walShard).
func (s *walShard) setCompactSegment(key nodeKey, segID uint64) {
	if s.metaSegments == nil {
		s.metaSegments = make(map[nodeKey]metaSegments)
	}
	m := s.metaSegments[key]
	m.compact = segID
	m.hasCompact = true
	s.metaSegments[key] = m
}

// clearMetaSegmentPins removes any State/Snapshot/Bootstrap/Compact/Remove
// pin that points at segID, resetting both the field and its presence flag.
// Used when a segment is discarded (truncation-recovery cleanup) so no
// phantom pin to a non-existent segment lingers in metaSegments. An entry
// whose pins all become absent is removed from the map. Called only from
// discardSegmentsAbove, i.e. from recover(), where the shard is not yet
// published and neither lock is required (see walShard).
func (s *walShard) clearMetaSegmentPins(segID uint64) {
	for key, m := range s.metaSegments {
		if m.hasState && m.state == segID {
			m.state = 0
			m.hasState = false
		}
		if m.hasSnapshot && m.snapshot == segID {
			m.snapshot = 0
			m.hasSnapshot = false
		}
		if m.hasBootstrap && m.bootstrap == segID {
			m.bootstrap = 0
			m.hasBootstrap = false
		}
		if m.hasCompact && m.compact == segID {
			m.compact = 0
			m.hasCompact = false
		}
		if m.hasRemove && m.remove == segID {
			m.remove = 0
			m.hasRemove = false
		}
		if !m.hasState && !m.hasSnapshot && !m.hasBootstrap && !m.hasCompact && !m.hasRemove {
			delete(s.metaSegments, key)
		} else {
			s.metaSegments[key] = m
		}
	}
}

// advanceCommittedHWM raises the commit high-water mark to segID if segID is
// higher. The mark is monotonic: a batch that landed in an earlier segment
// must never lower it below a later batch that already committed. Caller must
// hold s.mu and s.dataMu for writing, except on the recover() path (see
// walShard).
func (s *walShard) advanceCommittedHWM(segID uint64) {
	if segID > s.committedSegmentHWM {
		s.committedSegmentHWM = segID
	}
}

// addMetaSegmentsToLive folds every tracked State/Snapshot/Bootstrap/Compact/
// Remove segment ID for all live nodes into liveSegments. These record types
// are never tracked by an entry index, so they must be pinned explicitly
// before garbage collection or the sole durable copy of a node's
// State/Snapshot/Bootstrap — or of a removed node's tombstone — could be
// deleted. This only READS metaSegments, and every mutator of it holds s.mu,
// so holding s.mu alone is sufficient exclusion (see walShard).
func (s *walShard) addMetaSegmentsToLive(liveSegments map[uint64]bool) {
	for _, m := range s.metaSegments {
		if m.hasState {
			liveSegments[m.state] = true
		}
		if m.hasSnapshot {
			liveSegments[m.snapshot] = true
		}
		if m.hasBootstrap {
			liveSegments[m.bootstrap] = true
		}
		if m.hasCompact {
			liveSegments[m.compact] = true
		}
		if m.hasRemove {
			liveSegments[m.remove] = true
		}
	}
}

// recomputeMinWriteEpochFromDisk re-derives the shard's epoch floor from the
// segment files actually present in the shard directory. The floor must never
// be driven by the intended live-segment set handed to GarbageCollect: GC
// deletes files one at a time and can abort partway, so a segment absent from
// that set may still exist on disk — dropping its segmentEpochs entry would
// let the engine purge a DEK the surviving segment still needs, making the
// next restart fail with WALDecryptError. A ReadDir failure keeps the floor
// unchanged, which is conservative: the floor can only be too LOW, retaining
// DEKs longer than strictly necessary. Caller must hold s.mu; this takes
// s.dataMu itself for the map rewrite.
func (s *walShard) recomputeMinWriteEpochFromDisk() {
	dirEntries, err := s.fs.ReadDir(s.dir)
	if err != nil {
		slog.Warn("waldb: skipping minWriteEpoch recompute; cannot list shard directory",
			"dir", s.dir, "error", err)
		return
	}
	surviving := make(map[uint64]bool, len(dirEntries))
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		if segID, ok := parseSegmentFilename(de.Name()); ok {
			surviving[segID] = true
		}
	}
	// The directory scan above is I/O and stays outside dataMu; only the map
	// rewrite it feeds is taken under the lock.
	s.dataMu.Lock()
	s.recomputeMinWriteEpoch(surviving)
	s.dataMu.Unlock()
}

// recomputeMinWriteEpoch recalculates the shard's minWriteEpoch from the
// segmentEpochs map after garbage collection removes dead segments.
// liveSegments contains the set of segment IDs that survived GC. The active
// segment is always considered live.
//
// It rewrites segmentEpochs and minWriteEpoch, so the caller must hold s.mu
// and s.dataMu for writing.
func (s *walShard) recomputeMinWriteEpoch(liveSegments map[uint64]bool) {
	// Remove dead segments from the epoch map.
	for segID := range s.segmentEpochs {
		if !liveSegments[segID] && segID != s.activeSegmentID {
			delete(s.segmentEpochs, segID)
		}
	}

	// Recompute the minimum from remaining segments.
	var newMin uint64
	for _, epoch := range s.segmentEpochs {
		if epoch > 0 && (newMin == 0 || epoch < newMin) {
			newMin = epoch
		}
	}
	s.minWriteEpoch = newMin
}

// DBOpenError is returned when the database fails to open or create
// a directory.
type DBOpenError struct {
	Dir string
	Err error
}

// Error implements the error interface.
func (e *DBOpenError) Error() string {
	return "waldb: failed to open " + e.Dir + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *DBOpenError) Unwrap() error {
	return e.Err
}

// RecoveryError is returned when recovery fails on a specific segment.
type RecoveryError struct {
	SegmentID uint64
	Err       error
}

// Error implements the error interface.
func (e *RecoveryError) Error() string {
	return "waldb: recovery failed for segment " + itoa(int(e.SegmentID)) + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *RecoveryError) Unwrap() error {
	return e.Err
}

// SegmentCreateError is returned when a new segment cannot be created.
type SegmentCreateError struct {
	SegmentID uint64
	Err       error
}

// Error implements the error interface.
func (e *SegmentCreateError) Error() string {
	return "waldb: failed to create segment " + itoa(int(e.SegmentID)) + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *SegmentCreateError) Unwrap() error {
	return e.Err
}
