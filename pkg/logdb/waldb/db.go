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
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// DefaultShardCount is the number of WAL directory shards used to reduce
// fsync contention. ShardID is hashed to one of these shards.
const DefaultShardCount = 16

// ciphertextEpochSize is the size of the 8-byte big-endian epoch prefix of an encrypted record.
const ciphertextEpochSize = 8

// nodeKey identifies a unique (shardID, replicaID) pair for map lookups.
type nodeKey struct {
	ShardID   uint64
	ReplicaID uint64
}

// metaSegments pins, per node, the segments holding its latest State, Snapshot, Bootstrap and Compaction records, each with a presence flag since segment 0 is valid.
type metaSegments struct {
	state        uint64
	snapshot     uint64
	bootstrap    uint64
	compact      uint64
	hasState     bool
	hasSnapshot  bool
	hasBootstrap bool
	hasCompact   bool
}

// WALBarrier is the encryption interface used by WAL segments. It mirrors
// the methods needed from seal.Barrier without importing the seal package.
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

// walContextBarrier is the context-AAD capability the WAL requires of a barrier, asserted at Open on the dynamic type; Open fails with ErrBarrierContextUnsupported without it.
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

	// SupportsContextAAD reports whether the barrier authenticates the per-record context; records are position-bound exactly when true.
	SupportsContextAAD() bool

	// ContextBindingResolved reports whether SupportsContextAAD is an answer yet (false while sealed); the WAL latches nothing until it is (ErrBarrierContextUnresolved).
	ContextBindingResolved() bool
}

// legacyContextBarrier is walContextBarrier without ContextBindingResolved, used to name the missing method; see docs/compatibility/architecture.md.
type legacyContextBarrier interface {
	EncryptWithContext(dst, plaintext, contextAAD []byte) ([]byte, error)
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)
	IsHardwareMode() bool
	SupportsContextAAD() bool
}

// barrierContextAssertionError says whether a barrier is not context-capable or only lacks ContextBindingResolved.
func barrierContextAssertionError(barrier WALBarrier) error {
	if _, ok := barrier.(legacyContextBarrier); ok {
		return ErrBarrierContextBindingUnreportable
	}
	return ErrBarrierContextUnsupported
}

// plaintextModeReporter optionally tells no-encryption mode apart from a delegated encryptor that cannot bind position; only the Open warning differs.
type plaintextModeReporter interface {
	IsPlaintextMode() bool
}

// maxCiphertextExpansion bounds the seal envelope's overhead ([epoch:8][algID:1][nonce:<=24] plus a 16-byte tag), used to pick a record's segment before sealing; pinned by TestCiphertextExpansionWithinBound.
const maxCiphertextExpansion = 8 + 1 + 24 + 16

// LogDBMetrics receives WAL write, fsync and compaction latency and byte counts; nil skips collection.
type LogDBMetrics interface {
	// ObserveWriteLatency records the duration of one write.
	ObserveWriteLatency(d time.Duration)
	// ObserveFsyncLatency records the duration of one fsync.
	ObserveFsyncLatency(d time.Duration)
	// ObserveCompactionLatency records the duration of one compaction.
	ObserveCompactionLatency(d time.Duration)
	// IncWriteBytes adds n to the count of bytes written.
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

// WithBarrier seals every WAL record with barrier, which must be initialized and unsealed; a StrategyNone barrier is accepted and reported as AtRestHostRouted.
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

// WithFS sets the filesystem used for all file operations; tests inject faults through it.
func WithFS(fs FS) Option {
	return func(o *dbOptions) {
		o.fs = fs
	}
}

// DB is the WAL-based LogDB: 16 hash-sharded directories, each with its own segments, index and mutex; reads come from memory and recovery replays the WAL.
type DB struct {
	dir     string
	opts    dbOptions
	barrier WALBarrier
	shards  [DefaultShardCount]*walShard
	// retired is the retired-replica registry (retired.go): every node
	// RemoveNodeData removed. Replay drops their records and every writer
	// refuses them.
	retired *retiredRegistry
	diskMon *DiskMonitor
	metrics LogDBMetrics
	// lock is the exclusive lock on dir (walLockFilename), held from Open
	// to Close; nil when the FS is not a DirLocker.
	lock   io.Closer
	closed atomic.Bool
	// poisoned fail-stops the DB after an fsync failure; every later write gets ErrDBPoisoned until a restart recovers.
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

// walShard is one WAL directory shard. mu guards segment state and every write; dataMu guards the in-memory maps readers use. Order is mu then dataMu, and dataMu is never held across I/O.
type walShard struct {
	mu sync.Mutex

	// dataMu guards the in-memory maps only. See the Locking section above
	// for the field list and the mu -> dataMu ordering rule.
	dataMu sync.RWMutex

	fs   FS
	dir  string
	opts dbOptions

	// shardIdx is bound into each record's context AAD so records cannot be spliced between shards.
	shardIdx uint64

	barrier WALBarrier

	// sealer is barrier's context-AAD capability, resolved once at open. Nil
	// exactly when barrier is nil; openWALShard fails closed otherwise.
	sealer walContextBarrier

	// noContextBinding records, once at open, that the barrier cannot authenticate a context, so records carry none.
	noContextBinding bool

	// bindingDriftLogged limits verifyBindingUnchanged's error to one per shard. Protected by mu.
	bindingDriftLogged bool

	// ctxBuf is scratch for walRecordContext; written under mu, read only during recover().
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

	// durableFloors is the content of the shard's COMPACTED file. Protected by mu. See compaction_floor.go.
	durableFloors map[nodeKey]uint64

	// Set of all segment IDs that have ever been created in this shard.
	// Segment file state: protected by mu.
	knownSegments map[uint64]bool

	// openCreatedSegment names the segment openWALShard created, so closeAfterFailedOpen can unlink it. Protected by mu.
	openCreatedSegment      uint64
	openCreatedSegmentValid bool

	// minWriteEpoch is the minimum barrier epoch across live records in this shard, 0 when none are encrypted. Protected by dataMu.
	minWriteEpoch uint64

	// segmentEpochs is the minimum epoch per segment, used to recompute minWriteEpoch after GC. Protected by dataMu.
	segmentEpochs map[uint64]uint64

	// metaSegments pins, per node, the segment with its latest State, Snapshot, Bootstrap and Compaction records so GC keeps them. Protected by dataMu.
	metaSegments map[nodeKey]metaSegments

	// committedSegmentHWM is the highest segment fully reflected in the index and pins; GC never deletes above it. Advanced monotonically under mu. Protected by dataMu.
	committedSegmentHWM uint64

	// marshalBuf is a reusable buffer for encoding records.
	marshalBuf []byte

	// batchMode suppresses per-record fdatasync in writeRecord. When true,
	// the caller is responsible for calling syncSegment after the batch of
	// writes completes. This field is protected by mu.
	batchMode bool

	// closed marks the shard shut down, checked under mu so no writer reaches a segment after close. Protected by mu.
	closed bool
}

// Compile-time interface check.
var _ logdb.LogDB = (*DB)(nil)

// validateOpenOptions checks option values against Options.Validate's invariants, including a block size above a record header and segments of at least two blocks.
func validateOpenOptions(dir string, o dbOptions) error {
	if dir == "" {
		return ErrEmptyWALDir
	}
	if o.blockSize < recordHeaderSize+1 || !isPowerOfTwo(o.blockSize) {
		return ErrInvalidBlockSize
	}
	if o.maxFileSize <= 0 || o.maxFileSize > maxSegmentFileSize {
		return ErrInvalidMaxFileSize
	}
	if o.maxFileSize/int64(o.blockSize) < 2 {
		return ErrSegmentTooSmallForEntry
	}
	return nil
}

// resolveBarrierContextBinding decides once per DB, before any shard opens, whether the barrier can bind records, failing closed and warning once.
func resolveBarrierContextBinding(dir string, barrier WALBarrier) error {
	if barrier == nil {
		return nil
	}
	cb, ok := barrier.(walContextBarrier)
	if !ok {
		return barrierContextAssertionError(barrier)
	}
	// A sealed barrier has no capability to report yet; refuse before resolving.
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

// createWALRoot creates the WAL root and fsyncs the parent of every level MkdirAll created; under noSync the root is created on first write.
func createWALRoot(o dbOptions, dir string) error {
	if o.noSync {
		return nil
	}
	syncDir := func(d string) error { return syncDirFS(o.fs, d) }
	if err := fsutil.MkdirAllSyncedWith(o.fs, syncDir, dir, 0o700); err != nil {
		return &DBOpenError{Dir: dir, Err: err}
	}
	return nil
}

// establishWALFormat runs when no marker exists: a directory with segments is refused (ErrWALFormatMissing), an empty one gets the marker before any segment; a stamp failure is fatal.
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

// walDirHoldsSegments reports whether any shard directory under dir holds a segment; only not-exist counts as absence.
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

// Open creates or opens a WAL database in dir with 16 shard directories. It locks the directory, checks format and geometry, and resolves barrier context binding, in that order.
func Open(dir string, opts ...Option) (*DB, error) {
	o := defaultDBOptions()
	for _, opt := range opts {
		opt(&o)
	}

	if err := validateOpenOptions(dir, o); err != nil {
		return nil, err
	}

	if err := createWALRoot(o, dir); err != nil {
		return nil, err
	}
	// Under noSync createWALRoot leaves the root to the first write, but the
	// lock below is taken inside it, so a missing root is created here.
	if o.noSync {
		if _, statErr := o.fs.Stat(dir); statErr != nil {
			if !errors.Is(statErr, fs.ErrNotExist) {
				return nil, &DBOpenError{Dir: dir, Err: statErr}
			}
			if mkErr := o.fs.MkdirAll(dir, 0o700); mkErr != nil {
				return nil, &DBOpenError{Dir: dir, Err: mkErr}
			}
		}
	}

	// One writer per directory: take the lock before reading or recording the format; every failure below releases it.
	var lock io.Closer
	if locker, ok := o.fs.(DirLocker); ok {
		l, err := locker.LockDir(dir, walLockFilename)
		if err != nil {
			return nil, &DBOpenError{Dir: dir, Err: err}
		}
		lock = l
	}
	releaseLock := func() {
		if lock == nil {
			return
		}
		if err := lock.Close(); err != nil {
			slog.Debug("waldb: releasing the directory lock after a failed open failed",
				"dir", dir, "error", err)
		}
	}

	// Refuse a block size or format version that differs from the directory's before touching a segment; see format.go.
	recordedBlockSize, formatPresent, err := readWALFormat(o.fs, dir)
	if err != nil {
		releaseLock()
		return nil, &DBOpenError{Dir: dir, Err: err}
	}
	if formatPresent && recordedBlockSize != o.blockSize {
		releaseLock()
		return nil, &DBOpenError{Dir: dir, Err: &WALGeometryMismatchError{
			Dir:        dir,
			Recorded:   recordedBlockSize,
			Configured: o.blockSize,
		}}
	}

	if err := resolveBarrierContextBinding(dir, o.barrier); err != nil {
		releaseLock()
		return nil, err
	}

	db := &DB{
		dir:     dir,
		opts:    o,
		barrier: o.barrier,
		metrics: o.metrics,
		lock:    lock,
	}

	// Create the DiskMonitor if maxBytes is configured.
	if o.diskMonMaxBytes > 0 {
		db.diskMon = NewDiskMonitor(dir, o.diskMonMaxBytes, o.diskMonOpts...)
	}

	// Settle the on-disk format before a single segment is created: either this
	// directory already records one (checked above), or it is empty and this
	// process records one for it now, under the lock. See establishWALFormat.
	if !formatPresent {
		if err := establishWALFormat(o, dir); err != nil {
			releaseLock()
			return nil, err
		}
	}

	// The retirements are read before any segment is replayed, and applied to
	// what replay rebuilt before the DB is returned, so no caller ever sees a
	// record of a retired replica.
	retired, err := loadRetiredRegistry(o.fs, dir)
	if err != nil {
		releaseLock()
		return nil, &DBOpenError{Dir: dir, Err: err}
	}
	db.retired = retired

	for i := 0; i < DefaultShardCount; i++ {
		shardDir := filepath.Join(dir, shardDirName(i))
		shard, err := openWALShard(i, shardDir, o, o.barrier, o.metrics)
		if err != nil {
			// Close any shards we already opened, and unlink the empty segment
			// each of them created for a DB that will never exist.
			for j := 0; j < i; j++ {
				db.shards[j].closeAfterFailedOpen()
			}
			releaseLock()
			return nil, err
		}
		shard.poison = db.poison
		shard.diskMon = db.diskMon
		db.shards[i] = shard
	}
	db.purgeRetired()

	return db, nil
}

// purgeRetired drops replayed records of retired replicas and reclaims the segments that frees, each affected shard once.
func (db *DB) purgeRetired() {
	var affected [DefaultShardCount]bool
	for _, key := range db.retired.keys() {
		shard := db.getShard(key.ShardID)
		shard.mu.Lock()
		shard.dataMu.Lock()
		if shard.dropNodeLocked(key) {
			affected[key.ShardID%DefaultShardCount] = true
		}
		shard.dataMu.Unlock()
		shard.mu.Unlock()
	}
	for i, hit := range affected {
		if !hit {
			continue
		}
		shard := db.shards[i]
		shard.mu.Lock()
		shard.reclaimSegmentsLocked("purging retired replicas")
		shard.mu.Unlock()
	}
}

// dropNodeLocked removes every in-memory record of key and its pins, reporting whether any existed. Caller holds s.mu and s.dataMu.
func (s *walShard) dropNodeLocked(key nodeKey) bool {
	_, hasIndex := s.indexes[key]
	_, hasState := s.states[key]
	_, hasBootstrap := s.bootstraps[key]
	_, hasSnapshot := s.snapshots[key]
	_, hasFloor := s.compactedTo[key]
	_, hasPins := s.metaSegments[key]
	found := hasIndex || hasState || hasBootstrap || hasSnapshot || hasFloor || hasPins
	delete(s.indexes, key)
	delete(s.entries, key)
	delete(s.states, key)
	delete(s.bootstraps, key)
	delete(s.snapshots, key)
	delete(s.compactedTo, key)
	delete(s.metaSegments, key)
	return found
}

// validateBatchLocked refuses a whole batch that names a retired replica, has an out-of-bounds snapshot, or has entries at or below the compaction floor. Caller holds every touched shard's mu.
func (db *DB) validateBatchLocked(updates []logdb.Update) error {
	if err := db.refuseRetiredBatch(updates); err != nil {
		return err
	}
	for i := range updates {
		u := &updates[i]
		// A snapshot travels through SaveState as well as SaveSnapshot, and
		// the protocol bound has to hold on both doors (see SaveSnapshot).
		if u.Snapshot.Index > 0 {
			if err := u.Snapshot.CheckBounds(); err != nil {
				return err
			}
		}
		if len(u.Entries) == 0 {
			continue
		}
		shard := db.getShard(u.ShardID)
		key := nodeKey{ShardID: u.ShardID, ReplicaID: u.ReplicaID}
		floor, ok := shard.compactedTo[key]
		if !ok {
			continue
		}
		if first := u.Entries[0].Index; first <= floor {
			return &logdb.EntriesBelowCompactionFloorError{
				ShardID:     u.ShardID,
				ReplicaID:   u.ReplicaID,
				FirstIndex:  first,
				CompactedTo: floor,
			}
		}
	}
	return nil
}

// refuseRetiredBatch applies refuseRetired to every Update of a batch, so a
// batch naming a retired replica is refused whole, before anything is written.
func (db *DB) refuseRetiredBatch(updates []logdb.Update) error {
	if db.retired.size.Load() == 0 {
		return nil
	}
	for i := range updates {
		if err := db.refuseRetired(nodeKey{ShardID: updates[i].ShardID, ReplicaID: updates[i].ReplicaID}); err != nil {
			return err
		}
	}
	return nil
}

// refuseRetired returns logdb.ErrReplicaRetired when key was retired by RemoveNodeData; lock-free when nothing was retired.
func (db *DB) refuseRetired(key nodeKey) error {
	if db.retired.contains(key) {
		return logdb.ErrReplicaRetired
	}
	return nil
}

// openWALShard opens shard directory idx, recovering existing segments or initializing lazily; idx is bound into record AAD, so it must be the shard's permanent index.
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
		// Never latch an unresolved capability.
		if !cb.ContextBindingResolved() {
			return nil, ErrBarrierContextUnresolved
		}
		s.sealer = cb
		s.noContextBinding = !cb.SupportsContextAAD()
	}

	// Only ErrNotExist means absence; any other error fails the open.
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
			// Remember the created segment so closeAfterFailedOpen can unlink it.
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

// corruptionError adds the shard and clean-prefix end to a corruption sentinel and logs the repair triple; see SegmentCorruptionError.
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

// recover replays the shard's segments to rebuild in-memory state; it runs before the shard is reachable, so it takes no locks.
func (s *walShard) recover() error {
	dirEntries, err := s.fs.ReadDir(s.dir)
	if err != nil {
		return &DBOpenError{Dir: s.dir, Err: err}
	}
	// The durable floors are read before replay and applied after it (see
	// reconcileCompactionFloors). A file that does not parse refuses the
	// open: the floors it held cannot be proven any other way.
	floors, err := loadCompactionFloors(s.fs, s.dir)
	if err != nil {
		return &DBOpenError{Dir: s.dir, Err: err}
	}
	s.durableFloors = floors

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
		// Advance nextSegmentID past every observed segment before replay, so a lingering orphan is never ordered after new segments.
		if segID >= s.nextSegmentID {
			s.nextSegmentID = segID + 1
		}
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	// highest is the highest segment that survives on disk.
	var highest uint64
	for i, segID := range segIDs {
		highest = segID
		cleanEnd, err := s.replaySegment(segID)
		if err != nil {
			// Bytes above the damage that this writer cannot have produced.
			// Checked first: it is not a claim about which of this node's
			// records were lost, so it must not be reported as one.
			if errors.Is(err, errRecordSequenceRegression) {
				return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
					segID, cleanEnd, ErrRecordSequenceRegression)}
			}
			// A non-final segment was fully fdatasynced before rotation, so any damage in it is under acknowledged data.
			if i != len(segIDs)-1 && isCorruptionVerdict(err) {
				return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
					segID, cleanEnd, ErrNonFinalSegmentCorruption)}
			}
			// Corruption below fdatasync-acknowledged records is never a torn write.
			if errors.Is(err, errMidSegmentCorruption) {
				return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
					segID, cleanEnd, ErrMidSegmentCorruption)}
			}
			// Corruption below the acknowledged end the surviving records name.
			if errors.Is(err, errAcknowledgedDataLost) {
				return &RecoveryError{SegmentID: segID, Err: s.corruptionError(
					segID, cleanEnd, ErrAcknowledgedDataLost)}
			}
			if errors.Is(err, errRecoveryTruncated) {
				// A torn tail in the final segment: delete higher segments first, then truncate this one to its clean end, so every crash point repeats the same repair.
				if dErr := s.discardSegmentsAbove(segIDs[i+1:]); dErr != nil {
					return dErr
				}
				if tErr := s.truncateCorruptSegment(segID, cleanEnd); tErr != nil {
					return &RecoveryError{SegmentID: segID, Err: tErr}
				}
				// A second directory fsync proves the repair durable.
				if sErr := syncDirFS(s.fs, s.dir); sErr != nil {
					return &RecoveryError{SegmentID: segID, Err: sErr}
				}
				// The repaired segment is committed, so GC may consider it.
				s.advanceCommittedHWM(segID)
				break
			}
			return err
		}
		// Seed the commit high-water mark to the highest replayed segment.
		s.advanceCommittedHWM(segID)
	}

	// fdatasync the highest clean segment before it becomes non-final, since its last sync mark may only be in the page cache.
	if len(segIDs) > 0 && !s.opts.noSync {
		if err := s.syncSegmentFile(highest); err != nil {
			return err
		}
	}

	return s.reconcileCompactionFloors()
}

// syncSegmentFile fdatasyncs a segment file by name. See recover.
func (s *walShard) syncSegmentFile(segID uint64) error {
	name := filepath.Join(s.dir, segmentFilename(segID))
	f, err := s.fs.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return &RecoveryError{SegmentID: segID, Err: err}
	}
	if sErr := f.Sync(); sErr != nil {
		_ = f.Close()
		return &RecoveryError{SegmentID: segID, Err: sErr}
	}
	if cErr := f.Close(); cErr != nil {
		return &RecoveryError{SegmentID: segID, Err: cErr}
	}
	return nil
}

// reconcileCompactionFloors raises a replayed floor below the index start: from the durable COMPACTED file, else firstIndex-1 when a recorded snapshot covers the gap, else unchanged.
func (s *walShard) reconcileCompactionFloors() error {
	// The durable floor file first: it is the proof, and a floor it restores
	// leaves no missing prefix for the snapshot rule below to consider.
	s.applyDurableFloors()
	for key, idx := range s.indexes {
		first, _, count := idx.Bounds()
		if count == 0 {
			continue
		}
		floor := s.compactedTo[key]
		if first.Index <= floor+1 {
			continue // contiguous with the floor: nothing missing
		}
		// Repair a missing prefix only when a recorded snapshot covers it; otherwise the node may legitimately start high.
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

// discardSegmentsAbove removes segments unreachable after prefix truncation, clears their pins and fsyncs the directory; a segment that can be neither removed nor emptied is a fatal RecoveryError.
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
	// A directory fsync failure is fatal: the deletions are not durable without it.
	if err := syncDirFS(s.fs, s.dir); err != nil {
		slog.Error("waldb: could not make the discard of orphaned WAL segments "+
			"durable; their deletion may not survive a crash",
			"dir", s.dir, "orphans", orphans, "error", err)
		return err
	}
	return nil
}

// truncateCorruptSegment truncates a segment to cleanEnd and fsyncs it; never written again, so the end need not be block-aligned.
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

// replaySegment replays a segment's records and returns ReadAll's clean-prefix end for repair on errRecoveryTruncated.
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

			// Rebuild the record's positional AAD from i, the logical record ordinal; no retry without context, and noContextBinding mirrors the write side.
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

		// A record that passed CRC but does not decode is an error, never skipped.
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

	// A lower generation above the corruption is foreign bytes, reported on its own sentinel.
	if readSeg.sequenceRegression {
		return cleanEnd, errRecordSequenceRegression
	}

	// Records above the corruption span a completed fdatasync, so they were acknowledged: fail-stop.
	if readSeg.ackedDataAfterCorruption {
		return cleanEnd, errMidSegmentCorruption
	}

	// The surviving generation began above the corruption offset, so acknowledged bytes were destroyed: fail-stop.
	if readSeg.acknowledgedDataLost {
		return cleanEnd, errAcknowledgedDataLost
	}

	// A torn tail with nothing acknowledged lost: stop the segment loop here so the truncation discards no acknowledged record.
	if readSeg.stoppedAtCorruption {
		slog.Warn("waldb: halting recovery after corruption; later segments skipped",
			"segment_id", segID)
		return cleanEnd, errRecoveryTruncated
	}

	return cleanEnd, nil
}

// isCorruptionVerdict reports whether err is one of replaySegment's verdicts
// on damage found in a segment — as opposed to an I/O error, a decrypt
// failure or a malformed payload, which recover reports as themselves.
func isCorruptionVerdict(err error) bool {
	return errors.Is(err, errRecoveryTruncated) ||
		errors.Is(err, errMidSegmentCorruption) ||
		errors.Is(err, errAcknowledgedDataLost)
}

// errRecoveryTruncated signals that replaySegment halted at a corrupt record.
// recover() treats it as a stop condition (not a failure): the clean prefix up
// to and including this segment is recovered; no higher segment is replayed.
var errRecoveryTruncated = errors.New("waldb: recovery truncated at corruption")

// errMidSegmentCorruption signals acknowledged records above a corrupt record; recover() reports ErrMidSegmentCorruption.
var errMidSegmentCorruption = errors.New("waldb: corruption with acknowledged data after it")

// errAcknowledgedDataLost signals survivors naming an acknowledged end above the corruption; recover() reports ErrAcknowledgedDataLost.
var errAcknowledgedDataLost = errors.New("waldb: corruption below the acknowledged end")

// errRecordSequenceRegression signals a lower durability generation above a corruption; recover() reports ErrRecordSequenceRegression.
var errRecordSequenceRegression = errors.New("waldb: record durability sequence regressed")

// ErrNonFinalSegmentCorruption is returned, in a RecoveryError, for corruption in a segment that is not the highest; restore from snapshot or peers.
var ErrNonFinalSegmentCorruption = errors.New("waldb: corruption in a non-final WAL segment; committed data above it would be lost — restore from snapshot/peers")

// ErrMidSegmentCorruption is returned, in a RecoveryError, for corruption followed by decodable records in the same segment; restore from snapshot or peers.
var ErrMidSegmentCorruption = errors.New("waldb: WAL corruption with acknowledged records after it; not an interrupted write — committed data would be lost — restore from snapshot/peers")

// ErrAcknowledgedDataLost is returned, in a RecoveryError, when the surviving generation starts above the corruption, proving acknowledged bytes were lost; restore or authorize with Repair.
var ErrAcknowledgedDataLost = errors.New("waldb: WAL corruption below the acknowledged end; records this node reported durable were destroyed — restore from snapshot/peers")

// ErrRecordSequenceRegression is returned, in a RecoveryError, for a lower durability generation above a corruption; restore or authorize with Repair.
var ErrRecordSequenceRegression = errors.New("waldb: WAL record durability sequence goes backwards; these bytes were not written by this log — restore from snapshot/peers")

// SegmentCorruptionError gives the shard, segment and offset of a corruption recovery refused to repair, so Repair can be authorized exactly; it unwraps to its sentinel.
type SegmentCorruptionError struct {
	// ShardIndex is the WAL shard directory holding the segment.
	ShardIndex int

	// SegmentID is the corrupt segment's numeric ID.
	SegmentID uint64

	// CleanEnd is the byte offset the segment's uncorrupted prefix ends at,
	// and the only offset Repair will accept for this segment.
	CleanEnd int64

	// Err is ErrMidSegmentCorruption, ErrNonFinalSegmentCorruption,
	// ErrAcknowledgedDataLost or ErrRecordSequenceRegression.
	Err error
}

// Error implements the error interface.
func (e *SegmentCorruptionError) Error() string {
	return e.Err.Error() + " (shard " + itoa(e.ShardIndex) + ", segment " +
		itoa(int(e.SegmentID)) + ", clean prefix ends at " + itoa(int(e.CleanEnd)) +
		"); if the loss is acceptable, authorize it with waldb.Repair"
}

// Unwrap returns the sentinel so errors.Is against ErrMidSegmentCorruption,
// ErrNonFinalSegmentCorruption, ErrAcknowledgedDataLost or
// ErrRecordSequenceRegression holds.
func (e *SegmentCorruptionError) Unwrap() error {
	return e.Err
}

// Payload type bytes that prefix each WAL record to identify its content.
const (
	// Retired and never reallocated: 0x01, 0x02, 0x03, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0c; replayRecord refuses them with ErrUnknownPayloadType.
	// payloadTypeState is the hard-state format, with the vote window after Term, Vote and Commit (stateRecordSize).
	payloadTypeState     byte = 0x0d
	payloadTypeBootstrap byte = 0x04
	payloadTypeCompact   byte = 0x05
	// payloadTypeEntries is the current entry format (84-byte header --
	// entryRecordHeaderSize -- including the five session fields
	// Key, ClientID, SeriesID, RespondedTo and OwnerID).
	payloadTypeEntries byte = 0x0b
	// payloadTypeSnapshot is the current snapshot format, whose membership
	// section carries a four-field session policy tail followed by the
	// committed WireVersion.
	payloadTypeSnapshot byte = 0x0e
)

// ErrUnsupportedSnapshotCompression is returned when a snapshot record names a compression this binary cannot decompress.
var ErrUnsupportedSnapshotCompression = errors.New(
	"waldb: snapshot record declares an unsupported compression algorithm")

// ErrUnsupportedMembershipVersion is returned when a snapshot's membership section version is not membershipSectionVersion; fatal.
var ErrUnsupportedMembershipVersion = errors.New(
	"waldb: snapshot record membership section has unsupported version")

// replayRecord decodes a raw WAL record into in-memory state, returning skipped counts and reason; a non-nil error is fatal to recovery.
func (s *walShard) replayRecord(rec []byte, segID uint64) (skipped int, reason string, err error) {
	if len(rec) < 1 {
		return 1, "empty record", nil
	}

	payloadType := rec[0]
	data := rec[1:]

	switch payloadType {
	case payloadTypeEntries:
		// A malformed entry batch passed CRC, so it is corruption: fatal.
		if rerr := s.replayEntries(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeState:
		// A skipped State record would regress Term/Vote: fatal.
		if rerr := s.replayState(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeSnapshot:
		// Fatal: a skipped snapshot record boots the node with empty
		// membership and no snapshot metadata.
		if rerr := s.replaySnapshot(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeBootstrap:
		if rerr := s.replayBootstrap(data, segID); rerr != nil {
			return 0, "", rerr
		}
	case payloadTypeCompact:
		if rerr := s.replayCompaction(data, segID); rerr != nil {
			return 0, "", rerr
		}
	default:
		// An unknown payload type is corruption or format skew, typically a keyed WAL opened without its barrier: fail-stop.
		return 0, "", ErrUnknownPayloadType
	}
	return skipped, "", nil
}

// Entry encoding layout (payloadTypeEntries):
// [ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8]
// [Key:8][ClientID:8][SeriesID:8][RespondedTo:8][OwnerID:8][CmdLen:4][Cmd:N]
const entryRecordHeaderSize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 // 84 bytes

// maxCmdLen is the maximum allowed command length for a single WAL entry
// during replay. This matches proto.MaxEntrySize (8 MB) and prevents
// corrupted length prefixes from causing unbounded memory allocation.
const maxCmdLen = 8 * 1024 * 1024 // 8 MB

// replayEntries rebuilds entries from a batch record; any malformed entry aborts recovery with a typed error.
func (s *walShard) replayEntries(data []byte, segID uint64) error {
	pos := 0
	// Track the batch's node and span so an overwrite can prune orphaned higher entries, as applyTo does.
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

		// Cap cmdLen as the write path does; a larger value is corruption.
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
	// On an overwrite, prune map entries above its last index.
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

// State encoding layout: [ShardID:8][ReplicaID:8][Term:8][Vote:8][Commit:8][VoteWindow:8]
const stateRecordSize = 8 + 8 + 8 + 8 + 8 + 8 // 48 bytes

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
	voteWindow := binary.LittleEndian.Uint64(data[40:])

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.states[key] = logdb.State{Term: term, Vote: vote, Commit: commit, VoteWindow: voteWindow}
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
// [SessionExpiryEntries:8][MaxTotalSessions:8][MaxResponsesPerSession:8]
// [MaxSessionsPerOwner:8][WireVersion:8]
// [NumRemovedIdentities:4]{ [ReplicaID:8][IdentityLen:4][Identity:N] }...
const snapshotRecordBaseSize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 2 + 4 // 62 bytes

// membershipSectionV1 is the one membership section layout this binary reads and writes; see the compatibility contract in the proto package comment.
const membershipSectionV1 byte = 1

// membershipSectionVersion is the membership section version this binary writes and the only one it reads; it mirrors proto.Membership's version byte.
const membershipSectionVersion byte = membershipSectionV1

// previousMembershipSectionVersion is the previous minor release's section layout (docs/compatibility, "The accept sets"); equal to membershipSectionVersion at v1.0.
const previousMembershipSectionVersion byte = membershipSectionVersion

// acceptsMembershipSectionVersion reports whether v is in the closed read
// set. It is a set, not a range, so it never widens when a constant moves.
func acceptsMembershipSectionVersion(v byte) bool {
	return v == membershipSectionVersion || v == previousMembershipSectionVersion
}

// membershipFixedSize is the fixed region: Version(1) + ConfigChangeID(8) + MaxWitnesses(8) + PolicyFlags(1); the session policy is a tail.
const membershipFixedSize = 1 + 8 + 8 + 1 // 18 bytes

// minMembershipSize is the smallest well-formed membership section, equal to membershipSize of an empty membership; checked before the version byte.
const minMembershipSize = membershipFixedSize + 6*4 + membershipSessionPolicySize // 82 bytes

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

// membershipSessionPolicySize is the size of the replicated-policy tail:
// SessionExpiryEntries(8) + MaxTotalSessions(8) + MaxResponsesPerSession(8) +
// MaxSessionsPerOwner(8) + WireVersion(8).
const membershipSessionPolicySize = 5 * 8 // 40 bytes

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

// maxMembershipMapEntries caps a declared membership or bootstrap map count before allocation, mirroring proto.MaxMembershipEntries; larger is corrupt.
const maxMembershipMapEntries = 10000

// checkMembershipSize rejects a membership whose any map exceeds
// maxMembershipMapEntries, the cap replay treats as corruption. Keeping the
// write side symmetric with replay is the same rule ErrCmdTooLarge follows.
func checkMembershipSize(m logdb.Membership) error {
	if len(m.Addresses) > maxMembershipMapEntries ||
		len(m.Observers) > maxMembershipMapEntries ||
		len(m.Witnesses) > maxMembershipMapEntries ||
		len(m.Removed) > maxMembershipMapEntries ||
		// Identities and RemovedIdentities share the per-map cap with the decoder.
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

// boundedMapHint returns a map size hint no larger than the remaining bytes could hold at perEntryMinBytes each.
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

// replaySnapshot rebuilds per-node snapshot metadata; a parse failure returns ErrTruncatedSnapshotRecord and is fatal.
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

	// Bound-check in uint64 so 32-bit platforms cannot truncate or wrap fpLen.
	if uint64(len(data)) < uint64(snapshotRecordBaseSize)+uint64(fpLen) {
		slog.Error("waldb: snapshot record filepath exceeds record length during replay",
			"segment_id", segID, "length", len(data), "filepath_len", fpLen)
		return ErrTruncatedSnapshotRecord
	}
	fp := string(data[snapshotRecordBaseSize : snapshotRecordBaseSize+int(fpLen)])
	pos := snapshotRecordBaseSize + int(fpLen)

	// The membership section is always written; anything shorter than its minimum is a torn record.
	var membership logdb.Membership
	if len(data)-pos < minMembershipSize {
		slog.Error("waldb: snapshot record missing membership section during replay",
			"segment_id", segID, "remaining", len(data)-pos, "minimum", minMembershipSize)
		return ErrTruncatedSnapshotRecord
	}
	// Accept only the exact section version this binary writes.
	sectionVersion := data[pos]
	if !acceptsMembershipSectionVersion(sectionVersion) {
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

	// Check the remaining length before reading the session policy.
	if len(data)-pos < membershipSessionPolicySize {
		slog.Error("waldb: snapshot record membership section is missing its session policy",
			"segment_id", segID, "remaining", len(data)-pos,
			"required", membershipSessionPolicySize)
		return ErrTruncatedSnapshotRecord
	}
	membership.SessionExpiryEntries = binary.LittleEndian.Uint64(data[pos:])
	membership.MaxTotalSessions = binary.LittleEndian.Uint64(data[pos+8:])
	membership.MaxResponsesPerSession = binary.LittleEndian.Uint64(data[pos+16:])
	membership.MaxSessionsPerOwner = binary.LittleEndian.Uint64(data[pos+24:])
	membership.WireVersion = binary.LittleEndian.Uint64(data[pos+32:])
	pos += membershipSessionPolicySize

	// RemovedIdentities closes the section; it is the receive gate's identity deny-list.
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

// Bootstrap encoding layout: [ShardID:8][ReplicaID:8][Join:1][Type:8][NumAddrs:4], per address [ReplicaID:8][AddrLen:4][Addr:N], then [DataDirLen:4][DataDir:N].
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

	// DataDir follows the address map, possibly empty; a record without it is truncated, and no bytes may remain.
	if len(data)-pos < bootstrapDataDirLenSize {
		slog.Error("waldb: bootstrap record has no DataDir field during replay",
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
	dataDir := string(data[pos : pos+int(dirLen)])
	pos += int(dirLen)
	if pos != len(data) {
		slog.Error("waldb: bootstrap record has trailing bytes during replay",
			"segment_id", segID, "parsed", pos, "length", len(data))
		return ErrTruncatedBootstrapRecord
	}

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.bootstraps[key] = logdb.Bootstrap{
		Addresses: addrs,
		Join:      join,
		Type:      smType,
		DataDir:   dataDir,
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
	// The floor is monotonic during replay: skip a lower marker, keeping the higher one's segment pinned.
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

// ensureActiveSegment creates the active segment if one does not exist.
// On first write to a lazy shard, this also creates the shard directory.
// Caller must hold s.mu.
func (s *walShard) ensureActiveSegment() error {
	// Re-check closed under the lock.
	if s.closed {
		return logdb.ErrClosed
	}
	if s.activeSegment != nil {
		return nil
	}

	// Create the shard directory on first write and fsync the WAL root so its entry is durable.
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

// rotateSegment syncs and closes the active segment, so a non-final segment carries nothing unsynced, then creates a new one.
func (s *walShard) rotateSegment() error {
	if s.activeSegment != nil {
		// Sync before closing; in batchMode this is what makes the old segment durable.
		if !s.opts.noSync {
			if err := s.activeSegment.Sync(); err != nil {
				if s.poison != nil {
					s.poison(err)
				}
				return err
			}
		}
		if err := s.activeSegment.Close(); err != nil {
			// Drop the closed segment so the next write creates a new one; a failed closing fdatasync fail-stops like any Sync failure.
			s.activeSegment = nil
			if errors.Is(err, errSegmentCloseSync) && s.poison != nil {
				s.poison(err)
			}
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

// recordLanding is where a written record landed: its segment and ciphertext epoch (0 when unencrypted); the caller applies it at its durability point.
type recordLanding struct {
	segID uint64
	epoch uint64
}

// applyEpochLocked folds a durable record's epoch into the shard's overall and per-segment epoch floors; not called from writeRecord. Caller holds s.dataMu (and s.mu on a live shard).
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

// recordContext builds the positional AAD for (segID, recordSeq) in s.ctxBuf, valid until the next call. Caller holds s.mu or owns the shard during recover().
func (s *walShard) recordContext(segID, recordSeq uint64) []byte {
	return walRecordContext(s.ctxBuf[:], s.shardIdx, segID, recordSeq)
}

// verifyBindingUnchanged detects a shard latched to no context binding against a barrier that now resolves as able to bind.
func (s *walShard) verifyBindingUnchanged() error {
	if s.sealer == nil || !s.sealer.ContextBindingResolved() {
		return nil
	}
	if !s.sealer.SupportsContextAAD() {
		return nil
	}
	// Log once; the condition is permanent and the error carries it.
	if !s.bindingDriftLogged {
		s.bindingDriftLogged = true
		slog.Error("waldb: barrier now authenticates caller context but the WAL "+
			"resolved it as unable to at open; refusing to write further unbound records",
			"dir", s.dir)
	}
	return ErrBarrierContextBindingChanged
}

// reserveLanding makes the active segment the one a record of framedLen (an upper bound) bytes lands in, rotating if needed. Caller holds s.mu.
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

// writeRecord encrypts and writes data to the active segment and returns its landing; only when the inline fdatasync fails is the epoch applied here.
func (s *walShard) writeRecord(data []byte) (recordLanding, error) {
	if err := s.ensureActiveSegment(); err != nil {
		return recordLanding{}, err
	}

	// Resolve the landing segment before sealing, since the AAD binds segment and ordinal.
	framedLen := len(data)
	if s.barrier != nil {
		framedLen += maxCiphertextExpansion
	}
	if err := s.reserveLanding(framedLen); err != nil {
		return recordLanding{}, err
	}

	// Encrypt; the epoch comes from the ciphertext prefix and is attributed after the write lands.
	writeData := data
	var writeEpoch uint64
	var positional bool
	if s.barrier != nil {
		// No context binding was decided at open (see noContextBinding).
		var contextAAD []byte
		if !s.noContextBinding {
			contextAAD = s.recordContext(s.activeSegmentID, s.activeSegment.RecordCount())
			positional = true
		} else if err := s.verifyBindingUnchanged(); err != nil {
			// Only this branch needs the drift check; the other direction already fails closed.
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
			// Unreachable: reserveLanding over-estimated the length.
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
			// Written but not provably durable: fail-stop (see DB.poison) and keep the epoch.
			s.commitEpoch(landing)
			if s.poison != nil {
				s.poison(syncErr)
			}
			return landing, syncErr
		}
	}
	return landing, nil
}

// syncSegment fsyncs the active segment after a batched write. A failure poisons the DB. Caller holds s.mu.
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

// close closes the active segment under s.mu and sets closed so no later writer recreates it.
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

// closeAfterFailedOpen closes a shard from a failed DB open and unlinks the empty segment this open created; failures are logged.
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

// syncDirFS fsyncs directory dir through fsys (SyncMetadata when available) and returns a DirSyncError on failure.
func syncDirFS(filesystem FS, dir string) error {
	if err := fsutil.SyncDirWith(filesystem.Open, dir); err != nil {
		return &DirSyncError{Dir: dir, Err: err}
	}
	return nil
}

// getShard returns the WAL shard for the given Raft shardID.
func (db *DB) getShard(shardID uint64) *walShard {
	return db.shards[shardID%DefaultShardCount]
}

// Name returns the name of this LogDB implementation.
func (db *DB) Name() string {
	return "waldb"
}

// SaveState persists updates atomically per WAL shard: write all records, fsync dirty shards in parallel, then commit in memory. A write failure rolls back every touched shard; a sync failure poisons the DB.
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

	// Per-shard bookkeeping on the stack.
	var dirtyShards [DefaultShardCount]*walShard
	var checkpoints [DefaultShardCount]shardCheckpoint
	var pending [DefaultShardCount]pendingMemUpdates
	dirtyCount := 0

	// Hold every touched shard's mu, in ascending order, for the whole batch so it is atomic per segment.
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

	// Refuse the whole batch before writing if validateBatchLocked fails.
	if err := db.validateBatchLocked(updates); err != nil {
		return err
	}

	// Phase 1: write all records without fsync.
	for i := range updates {
		u := &updates[i]
		shardIdx := uint16(u.ShardID % DefaultShardCount)
		shard := db.shards[shardIdx]

		// Checkpoint before the first write to this shard, flushing the open block (see captureCheckpointAfterEnsure).
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

	// Phase 2: fsync dirty shards; a failure has already poisoned the DB.
	if err := syncDirtyShardsLocked(&dirtyShards, dirtyCount); err != nil {
		db.poison(err)
		// Phase 3 never runs, so the batch's staged epochs would be dropped —
		// yet its records ARE written and nothing truncates them, so they can
		// replay on the next start. Retain their DEKs; see commitPendingEpochs.
		db.commitPendingEpochs(&pending)
		return errors.Join(ErrDBPoisoned, err)
	}

	// Phase 3: commit in memory under each shard's dataMu, with no I/O.
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
	// recordCount is the segment's record ordinal at capture; rollback rewinds it with the offsets (see segment.DiscardPending).
	recordCount uint64
	// seg and segID name the active segment at capture, to detect a mid-batch rotation.
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

// applyTo applies the pending in-memory updates under dataMu as one atomic commit for readers, with no I/O. Caller holds shard.mu.
func (p *pendingMemUpdates) applyTo(s *walShard) {
	s.dataMu.Lock()
	defer s.dataMu.Unlock()

	for i := range p.entries {
		eu := &p.entries[i]
		idx := s.getOrCreateIndex(eu.key)
		idx.Append(eu.indexes...)

		// Advance the commit mark to the segment these entries landed in; they share one record.
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
		// On a leader overwrite, prune orphaned higher entries from the map.
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

// rollbackBatch rolls back every checkpointed shard in the batch and discards its staged epochs unless the rollback is not provably durable (see commitPendingEpochs). Caller holds every touched shard's mu.
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

// commitPendingEpochs folds a batch's staged landings into the epoch floors when its records may still replay; a low floor only retains a key longer. Caller holds every touched shard's mu.
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

// rollbackShard rolls the shard's WAL back to cp, in place or, after a rotation, high to low so every crash point leaves a valid prefix. Caller holds ds.mu.
func rollbackShard(ds *walShard, cp *shardCheckpoint) (err error) {
	// Nothing to undo; an absent active segment after a failed rotation Close is not this case.
	if ds.activeSegment == nil && !cp.valid {
		return nil
	}
	// Any rollback failure is a durability failure: poison the DB.
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

	// Rotation occurred: delete intermediates, empty the active segment, fsync the directory, then truncate the checkpoint segment.
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

	// Empty the current segment, which holds only batch data; truncate the closed file when rotation dropped the handle.
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

	// Make the deletions durable before cutting the checkpoint segment; failure poisons the DB.
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

// syncDirtyShardsLocked fsyncs every dirty shard's segment, concurrently when there are several. Caller holds every dirty shard's mu.
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

// saveUpdate writes u to the WAL and stages its in-memory updates in pending, capturing checkpoint after creating a lazy shard's segment.
func (s *walShard) saveUpdate(u *logdb.Update, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	key := nodeKey{ShardID: u.ShardID, ReplicaID: u.ReplicaID}

	// Write entries.
	if len(u.Entries) > 0 {
		if err := s.writeEntries(key, u.ShardID, u.ReplicaID, u.Entries, pending, cp); err != nil {
			return err
		}
	}

	// Write hard state if non-zero.
	if u.State != (logdb.State{}) {
		if err := s.writeState(key, u.ShardID, u.ReplicaID, u.State, pending, cp); err != nil {
			return err
		}
	}

	// Write the snapshot when Index > 0 and not stale.
	if u.Snapshot.Index > 0 {
		// Compare against stored and pending snapshots; s.mu suffices to read the map.
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

// writeEntries writes a batch of entries to the WAL and stages the index and entry updates in pending.
func (s *walShard) writeEntries(key nodeKey, shardID, replicaID uint64, entries []logdb.Entry, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	// Enforce replay's per-entry Cmd cap before touching a segment.
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

	// Split the batch across records that each fit a segment; they precede the state record, so a torn tail loses state, never entries.
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

	// Stamp each chunk's SegmentID after writeRecord, since the write may rotate.
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

// captureShardCheckpoint creates a lazy shard's segment and captures its position for a standalone write; pair with DB.rollbackStandalone. Caller holds s.mu.
func captureShardCheckpoint(s *walShard, cp *shardCheckpoint) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	return captureCheckpointAfterEnsure(s, cp)
}

// rollbackStandalone discards a failed standalone write and returns writeErr, in a BatchWriteError if the rollback failed; a poisoned DB is left alone. Caller holds the shard's mu.
func (db *DB) rollbackStandalone(s *walShard, cp *shardCheckpoint, writeErr error) error {
	if !cp.valid || db.poisoned.Load() {
		return writeErr
	}
	if dErr := rollbackShard(s, cp); dErr != nil {
		return &BatchWriteError{WriteErr: writeErr, DiscardErrors: []error{dErr}}
	}
	return writeErr
}

// captureCheckpointAfterEnsure flushes the open block and captures a checkpoint for a segment ensureActiveSegment just created.
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
	binary.LittleEndian.PutUint64(buf[41:], state.VoteWindow)

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
	// Enforce replay's map-size cap before touching a segment.
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
	// Every field is written, so the section length is always membershipSize.
	pos = marshalAddrMap(buf, pos, ss.Membership.Identities)
	binary.LittleEndian.PutUint64(buf[pos:], ss.Membership.SessionExpiryEntries)
	binary.LittleEndian.PutUint64(buf[pos+8:], ss.Membership.MaxTotalSessions)
	binary.LittleEndian.PutUint64(buf[pos+16:], ss.Membership.MaxResponsesPerSession)
	binary.LittleEndian.PutUint64(buf[pos+24:], ss.Membership.MaxSessionsPerOwner)
	binary.LittleEndian.PutUint64(buf[pos+32:], ss.Membership.WireVersion)
	pos += membershipSessionPolicySize
	pos = marshalAddrMap(buf, pos, ss.Membership.RemovedIdentities)

	landing, err := s.writeRecord(buf[:pos])
	if err != nil {
		return err
	}
	pending.stageEpoch(landing)
	// Pin the landing segment, and stage a deep copy so the stored record never aliases the caller's maps.
	pending.snapshots = append(pending.snapshots, pendingSnapshotUpdate{
		key:         key,
		snapshot:    copySnapshot(ss),
		landedSegID: landing.segID,
	})
	return nil
}

// writeBootstrap writes bs to the WAL, ending with the DataDir field; call it only outside batchMode, since it updates memory immediately.
func (s *walShard) writeBootstrap(key nodeKey, shardID, replicaID uint64, bs logdb.Bootstrap) error {
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
	totalSize += bootstrapDataDirLenSize + len(bs.DataDir)

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
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(bs.DataDir)))
	pos += bootstrapDataDirLenSize
	copy(buf[pos:], bs.DataDir)
	pos += len(bs.DataDir)

	landing, err := s.writeRecord(buf[:pos])
	if err != nil {
		return err
	}
	// Commit point: publish the record, pin its segment, raise the commit mark and apply the epoch under dataMu.
	s.dataMu.Lock()
	// Store a deep copy matching the bytes written.
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

// IterateEntries returns entries in [low, high) for the node, up to maxSize bytes, per the LogDB contract; a range not starting at low returns ErrLogGapDetected.
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

	// Read-lock dataMu only, so reads never wait on an fsync.
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

		// Copy each entry with its own payload (see logdb.CopyEntryCmd).
		entries = append(entries, logdb.CopyEntryCmd(e))
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

// EntryRange returns the first index and contiguous span of the node's entries, (0, 0, nil) when none, and ErrLogGapDetected on a hole.
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
	// A retired replica gets no further records.
	if err := db.refuseRetired(key); err != nil {
		return err
	}

	// The compaction floor is monotonic: a lower floor is dropped before reaching the WAL.
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

	// Commit point: raise the floor, prune, pin the marker's segment, raise the commit mark and apply the epoch in one dataMu section.
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

	// The survey reads under s.mu alone; every mutator of these fields holds it.
	comp := newCompactor(shard.fs, shard.dir, shard.opts.blockSize)
	liveSegments := make(map[uint64]bool)
	for _, idx := range shard.indexes {
		for segID := range idx.AllSegmentIDs() {
			liveSegments[segID] = true
		}
	}
	// Pin the segments holding the latest State, Snapshot and Bootstrap records.
	shard.addMetaSegmentsToLive(liveSegments)

	// No segment is unlinked until the floors that cover its entries are
	// durable outside the WAL: a torn marker block must not be able to lower
	// a floor whose entries are gone. Failing here unlinks nothing.
	if err := shard.publishCompactionFloorsLocked(); err != nil {
		return err
	}

	deleted, err := comp.GarbageCollect(liveSegments, shard.activeSegmentID, shard.committedSegmentHWM)

	// Fsync the directory so deletions survive a crash, before the epoch floor is raised below.
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

	// Recompute minWriteEpoch from segments still on disk, only after a successful GC that deleted something under a barrier.
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

	// Refuse a snapshot at index 0, which covers nothing.
	if snapshot.Index == 0 {
		return logdb.ErrSnapshotOutOfDate
	}
	// Refuse an index or term at or above proto.MaxLogIndex / proto.MaxTerm before it becomes durable.
	if err := snapshot.CheckBounds(); err != nil {
		return err
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if err := db.refuseRetired(key); err != nil {
		return err
	}

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
	// Return a deep copy so callers never mutate the stored record.
	return copySnapshot(ss), nil
}

// RemoveNodeData records the retirement in the retired-replica registry (retired.go), drops the node's in-memory records and reclaims unused segments; a crash after publishing finishes on the next open.
func (db *DB) RemoveNodeData(shardID, replicaID uint64) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	// Held across the retirement and the drop, so a writer serialized on this
	// shard either finished before the retirement (and is dropped below) or
	// finds the node retired (refuseRetired).
	shard.mu.Lock()
	defer shard.mu.Unlock()

	drop := func() {
		shard.dataMu.Lock()
		shard.dropNodeLocked(key)
		shard.dataMu.Unlock()
	}
	if published, err := db.retired.retire(db.opts.fs, db.dir, []nodeKey{key}, db.opts.noSync, drop); err != nil {
		if published {
			db.poison(err)
		}
		return err
	}
	shard.reclaimSegmentsLocked("RemoveNodeData", "shard_id", shardID, "replica_id", replicaID)
	return nil
}

// reclaimSegmentsLocked GCs segments no live node references, fsyncs the directory, then recomputes the epoch floor from disk; best-effort. Caller holds s.mu.
func (s *walShard) reclaimSegmentsLocked(op string, logAttrs ...any) {
	// Collect the set of segments still referenced by remaining live nodes.
	liveSegments := make(map[uint64]bool)
	for _, idx := range s.indexes {
		for segID := range idx.AllSegmentIDs() {
			liveSegments[segID] = true
		}
	}
	// Keep surviving nodes' State/Snapshot/Bootstrap/Compact segments pinned.
	// These record types are not tracked by an entry index, so they must be
	// folded in explicitly before GC.
	s.addMetaSegmentsToLive(liveSegments)
	// The surviving nodes' floors are published first, exactly as in Compact:
	// a reclaimed segment may hold their compacted entries too.
	if err := s.publishCompactionFloorsLocked(); err != nil {
		slog.Warn("waldb: publishing compaction floors failed after "+op+
			"; skipping garbage collection",
			append([]any{"shard_dir", s.dir, "error", err}, logAttrs...)...)
		return
	}
	comp := newCompactor(s.fs, s.dir, s.opts.blockSize)
	_, gcErr := comp.GarbageCollect(liveSegments, s.activeSegmentID, s.committedSegmentHWM)
	if gcErr != nil {
		slog.Warn("waldb: garbage collection failed after "+op,
			append([]any{"shard_dir", s.dir, "error", gcErr}, logAttrs...)...)
	}

	// Fsync the directory before the epoch-floor recompute, as in Compact.
	dirSynced := true
	if err := syncDirFS(s.fs, s.dir); err != nil {
		dirSynced = false
		slog.Warn("waldb: directory sync failed after "+op+"; "+
			"skipping epoch-floor recompute so an unproven deletion cannot purge a live DEK",
			append([]any{"shard_dir", s.dir, "error", err}, logAttrs...)...)
	}

	// Recompute minWriteEpoch from segments on disk, only after a fully successful GC.
	if gcErr == nil && dirSynced && s.barrier != nil {
		s.recomputeMinWriteEpochFromDisk()
	}
}

// NodeRemoved reports whether RemoveNodeData has removed the node, and so
// retired its replica ID. The fact lives in the retired-replica registry
// (retired.go), which survives every restart, compaction and migration.
func (db *DB) NodeRemoved(shardID, replicaID uint64) (bool, error) {
	if db.closed.Load() {
		return false, logdb.ErrClosed
	}
	return db.retired.contains(nodeKey{ShardID: shardID, ReplicaID: replicaID}), nil
}

// retireNodes records every key in the registry in one rewrite, for re-encryption migration into an empty destination.
func (db *DB) retireNodes(nodes []logdb.NodeInfo) error {
	if err := db.writeGate(); err != nil {
		return err
	}
	keys := make([]nodeKey, 0, len(nodes))
	for _, ni := range nodes {
		keys = append(keys, nodeKey{ShardID: ni.ShardID, ReplicaID: ni.ReplicaID})
	}
	published, err := db.retired.retire(db.opts.fs, db.dir, keys, db.opts.noSync, nil)
	if err != nil && published {
		db.poison(err)
	}
	return err
}

// removedNodes returns every node the registry has retired, for the
// re-encryption migration, which must carry the retirements across.
func (db *DB) removedNodes() []logdb.NodeInfo {
	keys := db.retired.keys()
	result := make([]logdb.NodeInfo, 0, len(keys))
	for _, k := range keys {
		result = append(result, logdb.NodeInfo{ShardID: k.ShardID, ReplicaID: k.ReplicaID})
	}
	return result
}

// ListNodeInfo returns all NodeInfo entries in the LogDB.
func (db *DB) ListNodeInfo() ([]logdb.NodeInfo, error) {
	if db.closed.Load() {
		return nil, logdb.ErrClosed
	}

	seen := make(map[nodeKey]bool)
	for i := 0; i < DefaultShardCount; i++ {
		shard := db.shards[i]
		// In-memory read under dataMu; see IterateEntries.
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

// SaveBootstrap persists bootstrap information for a node. The record always
// carries the DataDir field.
func (db *DB) SaveBootstrap(shardID, replicaID uint64, bootstrap logdb.Bootstrap) error {
	return db.saveBootstrap(shardID, replicaID, bootstrap)
}

// saveBootstrap writes a bootstrap record, DataDir field included.
func (db *DB) saveBootstrap(shardID, replicaID uint64, bootstrap logdb.Bootstrap) error {
	if err := db.writeGate(); err != nil {
		return err
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if err := db.refuseRetired(key); err != nil {
		return err
	}

	// Checkpoint the standalone bootstrap write so a failure leaves no partial record behind.
	var cp shardCheckpoint
	if err := captureShardCheckpoint(shard, &cp); err != nil {
		return err
	}
	if err := shard.writeBootstrap(key, shardID, replicaID, bootstrap); err != nil {
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
	// Released last, once nothing more will be written under it.
	if db.lock != nil {
		if err := db.lock.Close(); err != nil && firstErr == nil {
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

// GetEntries returns entries in [low, high) up to maxSize, or (nil, nil) for an unknown node or empty range; ErrCompacted and ErrLogGapDetected propagate.
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

// CompactedTo returns the node's durable compaction floor, 0 if never compacted; Reencrypt carries it to the migrated WAL.
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

// MinLiveEpoch returns the minimum barrier epoch across live WAL records in all shards, or 0 with no barrier or no encrypted records.
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

// MinLiveEpochForBarrier returns MinLiveEpoch with known true when barrier is this DB's own (logdb.SameBarrier), else known false.
func (db *DB) MinLiveEpochForBarrier(barrier any) (epoch uint64, known bool, err error) {
	if db.closed.Load() {
		return 0, false, logdb.ErrClosed
	}
	if db.barrier == nil || barrier == nil {
		return 0, false, nil
	}
	if !logdb.SameBarrier(db.barrier, barrier) {
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

// AtRestProtection reports AtRestBarrierEncrypted when the barrier encrypts, AtRestHostRouted for a plaintext-mode barrier, and AtRestUnprotected without one.
func (db *DB) AtRestProtection() logdb.AtRestProtection {
	if db.barrier == nil {
		return logdb.AtRestUnprotected
	}
	if pm, ok := db.barrier.(plaintextModeReporter); ok && pm.IsPlaintextMode() {
		return logdb.AtRestHostRouted
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

// setCompactSegment pins segID as holding key's latest compaction marker. Caller holds s.mu and s.dataMu, except during recover().
func (s *walShard) setCompactSegment(key nodeKey, segID uint64) {
	if s.metaSegments == nil {
		s.metaSegments = make(map[nodeKey]metaSegments)
	}
	m := s.metaSegments[key]
	m.compact = segID
	m.hasCompact = true
	s.metaSegments[key] = m
}

// clearMetaSegmentPins removes every pin to a discarded segID, dropping entries left empty; called only from recover().
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
		if !m.hasState && !m.hasSnapshot && !m.hasBootstrap && !m.hasCompact {
			delete(s.metaSegments, key)
		} else {
			s.metaSegments[key] = m
		}
	}
}

// advanceCommittedHWM raises the commit mark to segID if higher. Caller holds s.mu and s.dataMu, except during recover().
func (s *walShard) advanceCommittedHWM(segID uint64) {
	if segID > s.committedSegmentHWM {
		s.committedSegmentHWM = segID
	}
}

// addMetaSegmentsToLive adds every pinned metadata segment to liveSegments before GC. Caller holds s.mu.
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
	}
}

// recomputeMinWriteEpochFromDisk re-derives the epoch floor from segment files on disk, keeping it on a ReadDir failure. Caller holds s.mu; it takes s.dataMu.
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

// recomputeMinWriteEpoch recalculates minWriteEpoch from segmentEpochs for the surviving liveSegments plus the active segment. Caller holds s.mu and s.dataMu.
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
