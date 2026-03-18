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
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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

// WithBarrier enables at-rest encryption for all WAL records. When set,
// records are encrypted before writing and decrypted during recovery.
// The barrier must be initialized and unsealed before any WAL operations.
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
}

// walShard is a single WAL directory shard. Each shard manages its own
// segment file rotation, in-memory entry store, state, bootstrap, and
// snapshot metadata. The mutex protects all operations.
type walShard struct {
	mu sync.Mutex

	fs              FS
	dir             string
	opts            dbOptions
	barrier         WALBarrier
	metrics         LogDBMetrics
	activeSegment   *segment
	activeSegmentID uint64
	nextSegmentID   uint64

	// Per-node entry indexes for range queries and compaction tracking.
	indexes map[nodeKey]*index

	// Per-node entry data store. The WAL provides durability; this map
	// provides fast reads without parsing WAL records.
	entries map[nodeKey]map[uint64]logdb.Entry

	// Per-node hard state.
	states map[nodeKey]logdb.State

	// Per-node bootstrap info.
	bootstraps map[nodeKey]logdb.Bootstrap

	// Per-node snapshot metadata.
	snapshots map[nodeKey]logdb.Snapshot

	// Per-node compaction markers.
	compactedTo map[nodeKey]uint64

	// Set of all segment IDs that have ever been created in this shard.
	knownSegments map[uint64]bool

	// minWriteEpoch tracks the minimum barrier encryption epoch across
	// all live (non-garbage-collected) records in this shard. It is
	// populated during WAL recovery from ciphertext headers and updated
	// on each encrypted write. After compaction + GC, it is recomputed
	// from the remaining segment epochs. Zero means no encrypted records
	// exist in this shard.
	minWriteEpoch uint64

	// segmentEpochs tracks the minimum epoch written to each segment ID.
	// This enables efficient recomputation of minWriteEpoch after segment
	// garbage collection without re-scanning ciphertext headers.
	segmentEpochs map[uint64]uint64

	// marshalBuf is a reusable buffer for encoding records.
	marshalBuf []byte

	// batchMode suppresses per-record fdatasync in writeRecord. When true,
	// the caller is responsible for calling syncSegment after the batch of
	// writes completes. This field is protected by mu.
	batchMode bool
}

// Compile-time interface check.
var _ logdb.LogDB = (*DB)(nil)

// Open creates or opens a WAL database at the given directory. The directory
// structure contains 16 sub-directories (shard-0 through shard-15), each
// holding its own WAL segment files.
func Open(dir string, opts ...Option) (*DB, error) {
	o := defaultDBOptions()
	for _, opt := range opts {
		opt(&o)
	}

	if dir == "" {
		return nil, ErrEmptyWALDir
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

	for i := 0; i < DefaultShardCount; i++ {
		shardDir := filepath.Join(dir, shardDirName(i))
		shard, err := openWALShard(shardDir, o, o.barrier, o.metrics)
		if err != nil {
			// Close any shards we already opened.
			for j := 0; j < i; j++ {
				if closeErr := db.shards[j].close(); closeErr != nil {
					slog.Debug("waldb: shard close failed during cleanup",
						"shard_index", j,
						"dir", db.shards[j].dir,
						"error", closeErr,
					)
				}
			}
			return nil, err
		}
		db.shards[i] = shard
	}

	return db, nil
}

// openWALShard opens a single WAL shard directory. If the directory exists,
// existing segments are recovered and in-memory state is rebuilt. If the
// directory does not exist, the shard is initialized lazily -- no directory,
// segment file, or marshal buffer is created until the first write.
func openWALShard(dir string, opts dbOptions, barrier WALBarrier, metrics LogDBMetrics) (*walShard, error) {
	s := &walShard{
		fs:            opts.fs,
		dir:           dir,
		opts:          opts,
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
		// marshalBuf is nil -- allocated lazily in getMarshalBuf.
	}

	// If the directory exists, recover any existing data.
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
		}
	}
	// If directory doesn't exist, shard is lazy -- directory and segment
	// will be created on first write via ensureActiveSegment.

	return s, nil
}

// recover scans the shard directory for existing segment files and replays
// their contents to rebuild the in-memory state.
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
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	// Replay each segment to rebuild all in-memory state.
	for _, segID := range segIDs {
		if err := s.replaySegment(segID); err != nil {
			return err
		}
		if segID >= s.nextSegmentID {
			s.nextSegmentID = segID + 1
		}
	}

	return nil
}

// replaySegment opens a segment file read-only, reads all records, and
// replays them to rebuild in-memory state.
func (s *walShard) replaySegment(segID uint64) error {
	name := filepath.Join(s.dir, segmentFilename(segID))
	f, err := s.fs.Open(name)
	if err != nil {
		return &RecoveryError{SegmentID: segID, Err: err}
	}

	// Create a read-only segment wrapper just for ReadAll.
	readSeg := &segment{
		f:         f,
		id:        segID,
		dir:       s.dir,
		blockSize: s.opts.blockSize,
		blockBuf:  make([]byte, s.opts.blockSize),
	}

	records, readErr := readSeg.ReadAll()

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
		return &RecoveryError{SegmentID: segID, Err: readErr}
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

			decrypted, derr := s.barrier.Decrypt(nil, rec)
			if derr != nil {
				// Decryption failures are fatal: silently skipping records
				// would cause undetected data loss. The caller must provide
				// the correct key or handle the error explicitly.
				return &WALDecryptError{SegmentID: segID, Err: derr}
			}
			replayData = decrypted
		}

		// Errors during individual record replay are non-fatal (skip
		// corrupted records) but are logged for observability.
		skipped, reason := s.replayRecord(replayData, segID)
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

	return nil
}

// Payload type bytes that prefix each WAL record to identify its content.
const (
	payloadTypeEntries    byte = 0x01
	payloadTypeState      byte = 0x02
	payloadTypeSnapshot   byte = 0x03
	payloadTypeBootstrap  byte = 0x04
	payloadTypeCompact    byte = 0x05
	payloadTypeRemoveNode byte = 0x06
)

// replayRecord decodes a raw WAL record and updates the in-memory state.
// Returns the number of individual entries/records skipped due to
// malformed data, and the reason for any top-level skip.
func (s *walShard) replayRecord(rec []byte, segID uint64) (skipped int, reason string) {
	if len(rec) < 1 {
		return 1, "empty record"
	}

	payloadType := rec[0]
	data := rec[1:]

	switch payloadType {
	case payloadTypeEntries:
		skipped = s.replayEntries(data, segID)
	case payloadTypeState:
		if !s.replayState(data) {
			return 1, "truncated state record"
		}
	case payloadTypeSnapshot:
		if !s.replaySnapshot(data) {
			return 1, "truncated snapshot record"
		}
	case payloadTypeBootstrap:
		if !s.replayBootstrap(data) {
			return 1, "truncated bootstrap record"
		}
	case payloadTypeCompact:
		if !s.replayCompaction(data) {
			return 1, "truncated compaction record"
		}
	case payloadTypeRemoveNode:
		if !s.replayRemoveNode(data) {
			return 1, "truncated remove-node record"
		}
	default:
		return 1, "unknown payload type"
	}
	return skipped, ""
}

// Entry encoding layout:
// [ShardID:8][ReplicaID:8][Index:8][Term:8][Type:8][CmdLen:4][Cmd:N]
const entryRecordHeaderSize = 8 + 8 + 8 + 8 + 8 + 4 // 44 bytes

// maxCmdLen is the maximum allowed command length for a single WAL entry
// during replay. This matches proto.MaxEntrySize (8 MB) and prevents
// corrupted length prefixes from causing unbounded memory allocation.
const maxCmdLen = 8 * 1024 * 1024 // 8 MB

// replayEntries rebuilds in-memory entries from a serialized entry batch.
// Returns the number of entries skipped due to malformed data.
func (s *walShard) replayEntries(data []byte, segID uint64) (skipped int) {
	pos := 0
	for pos < len(data) {
		if len(data)-pos < entryRecordHeaderSize {
			skipped++
			slog.Warn("waldb: truncated entry header during replay",
				"segment_id", segID,
				"position", pos,
				"remaining", len(data)-pos,
				"required", entryRecordHeaderSize,
			)
			return skipped
		}

		shardID := binary.LittleEndian.Uint64(data[pos:])
		replicaID := binary.LittleEndian.Uint64(data[pos+8:])
		raftIndex := binary.LittleEndian.Uint64(data[pos+16:])
		term := binary.LittleEndian.Uint64(data[pos+24:])
		entryType := binary.LittleEndian.Uint64(data[pos+32:])
		cmdLen := binary.LittleEndian.Uint32(data[pos+40:])

		// Validate cmdLen against the maximum allowed entry size to
		// prevent corrupted length prefixes from causing unbounded
		// memory allocation during recovery.
		if cmdLen > maxCmdLen {
			skipped++
			slog.Warn("waldb: entry cmdLen exceeds maximum during replay",
				"segment_id", segID,
				"position", pos,
				"cmd_len", cmdLen,
				"max_cmd_len", maxCmdLen,
				"shard_id", shardID,
				"replica_id", replicaID,
				"raft_index", raftIndex,
			)
			return skipped
		}

		totalSize := entryRecordHeaderSize + int(cmdLen)
		if len(data)-pos < totalSize {
			skipped++
			slog.Warn("waldb: truncated entry command during replay",
				"segment_id", segID,
				"position", pos,
				"cmd_len", cmdLen,
				"remaining", len(data)-pos-entryRecordHeaderSize,
			)
			return skipped
		}

		var cmd []byte
		if cmdLen > 0 {
			cmd = make([]byte, cmdLen)
			copy(cmd, data[pos+44:pos+44+int(cmdLen)])
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
		nodeEntries[raftIndex] = logdb.Entry{
			Index: raftIndex,
			Term:  term,
			Type:  entryType,
			Cmd:   cmd,
		}

		pos += totalSize
	}
	return 0
}

// State encoding layout: [ShardID:8][ReplicaID:8][Term:8][Vote:8][Commit:8]
const stateRecordSize = 8 + 8 + 8 + 8 + 8 // 40 bytes

// replayState rebuilds per-node hard state. Returns false if the record
// is truncated and was skipped.
func (s *walShard) replayState(data []byte) bool {
	if len(data) < stateRecordSize {
		return false
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	term := binary.LittleEndian.Uint64(data[16:])
	vote := binary.LittleEndian.Uint64(data[24:])
	commit := binary.LittleEndian.Uint64(data[32:])

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.states[key] = logdb.State{Term: term, Vote: vote, Commit: commit}
	return true
}

// Snapshot encoding layout:
// [ShardID:8][ReplicaID:8][Index:8][Term:8][FileSize:8][OnDiskIndex:8]
// [Epoch:8][FilePathLen:4][FilePath:N]
// [ConfigChangeID:8]
// [NumAddresses:4]{ [ReplicaID:8][AddrLen:4][Addr:N] }...
// [NumObservers:4]{ [ReplicaID:8][AddrLen:4][Addr:N] }...
// [NumWitnesses:4]{ [ReplicaID:8][AddrLen:4][Addr:N] }...
// [NumRemoved:4]{ [ReplicaID:8] }...
const snapshotRecordBaseSize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 // 60 bytes

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

// membershipSize returns the total serialized size of a logdb.Membership.
func membershipSize(m logdb.Membership) int {
	return 8 + // ConfigChangeID
		membershipAddrMapSize(m.Addresses) +
		membershipAddrMapSize(m.Observers) +
		membershipAddrMapSize(m.Witnesses) +
		membershipRemovedMapSize(m.Removed)
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

// unmarshalAddrMap deserializes an address map from data at the given offset.
// Returns the populated map and new offset, or -1 if the data is truncated.
func unmarshalAddrMap(data []byte, pos int) (map[uint64]string, int) {
	if len(data)-pos < 4 {
		return nil, -1
	}
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	m := make(map[uint64]string, count)
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 12 {
			return nil, -1
		}
		rid := binary.LittleEndian.Uint64(data[pos:])
		addrLen := binary.LittleEndian.Uint32(data[pos+8:])
		pos += 12
		if len(data)-pos < int(addrLen) {
			return nil, -1
		}
		m[rid] = string(data[pos : pos+int(addrLen)])
		pos += int(addrLen)
	}
	return m, pos
}

// unmarshalRemovedMap deserializes the removed map from data at the given
// offset. Returns the populated map and new offset, or -1 if truncated.
func unmarshalRemovedMap(data []byte, pos int) (map[uint64]bool, int) {
	if len(data)-pos < 4 {
		return nil, -1
	}
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	m := make(map[uint64]bool, count)
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

// replaySnapshot rebuilds per-node snapshot metadata. Returns false if
// the record is truncated and was skipped.
func (s *walShard) replaySnapshot(data []byte) bool {
	if len(data) < snapshotRecordBaseSize {
		return false
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	idx := binary.LittleEndian.Uint64(data[16:])
	term := binary.LittleEndian.Uint64(data[24:])
	fileSize := binary.LittleEndian.Uint64(data[32:])
	onDiskIndex := binary.LittleEndian.Uint64(data[40:])
	epoch := binary.LittleEndian.Uint64(data[48:])
	fpLen := binary.LittleEndian.Uint32(data[56:])

	if len(data) < snapshotRecordBaseSize+int(fpLen) {
		return false
	}
	fp := string(data[60 : 60+fpLen])
	pos := snapshotRecordBaseSize + int(fpLen)

	// Deserialize membership (present in records written by the fixed format).
	var membership logdb.Membership
	if len(data)-pos >= 8 {
		membership.ConfigChangeID = binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		var ok int
		membership.Addresses, ok = unmarshalAddrMap(data, pos)
		if ok == -1 {
			return false
		}
		pos = ok

		membership.Observers, ok = unmarshalAddrMap(data, pos)
		if ok == -1 {
			return false
		}
		pos = ok

		membership.Witnesses, ok = unmarshalAddrMap(data, pos)
		if ok == -1 {
			return false
		}
		pos = ok

		membership.Removed, ok = unmarshalRemovedMap(data, pos)
		if ok == -1 {
			return false
		}
	}

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.snapshots[key] = logdb.Snapshot{
		Index:       idx,
		Term:        term,
		Filepath:    fp,
		FileSize:    fileSize,
		OnDiskIndex: onDiskIndex,
		Epoch:       epoch,
		Membership:  membership,
	}
	return true
}

// Bootstrap encoding layout:
// [ShardID:8][ReplicaID:8][Join:1][Type:8][NumAddrs:4]
// For each addr: [ReplicaID:8][AddrLen:4][Addr:N]
const bootstrapRecordBaseSize = 8 + 8 + 1 + 8 + 4 // 29 bytes

// replayBootstrap rebuilds per-node bootstrap info. Returns false if the
// record is truncated and was skipped.
func (s *walShard) replayBootstrap(data []byte) bool {
	if len(data) < bootstrapRecordBaseSize {
		return false
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	join := data[16] != 0
	smType := binary.LittleEndian.Uint64(data[17:])
	numAddrs := binary.LittleEndian.Uint32(data[25:])

	addrs := make(map[uint64]string, numAddrs)
	pos := bootstrapRecordBaseSize
	for i := uint32(0); i < numAddrs; i++ {
		if len(data)-pos < 12 {
			return false
		}
		rid := binary.LittleEndian.Uint64(data[pos:])
		addrLen := binary.LittleEndian.Uint32(data[pos+8:])
		pos += 12
		if len(data)-pos < int(addrLen) {
			return false
		}
		addrs[rid] = string(data[pos : pos+int(addrLen)])
		pos += int(addrLen)
	}

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.bootstraps[key] = logdb.Bootstrap{
		Addresses: addrs,
		Join:      join,
		Type:      smType,
	}
	return true
}

// Compaction encoding layout: [ShardID:8][ReplicaID:8][CompactedTo:8]
const compactionRecordSize = 8 + 8 + 8 // 24 bytes

// replayCompaction applies a compaction marker from WAL replay. Returns
// false if the record is truncated and was skipped.
func (s *walShard) replayCompaction(data []byte) bool {
	if len(data) < compactionRecordSize {
		return false
	}
	shardID := binary.LittleEndian.Uint64(data[0:])
	replicaID := binary.LittleEndian.Uint64(data[8:])
	compactedTo := binary.LittleEndian.Uint64(data[16:])

	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}
	s.compactedTo[key] = compactedTo

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
	return true
}

// RemoveNode encoding layout: [ShardID:8][ReplicaID:8]
const removeNodeRecordSize = 8 + 8 // 16 bytes

// replayRemoveNode removes all in-memory state for a (shardID, replicaID)
// pair during WAL recovery. This makes RemoveNodeData durable across restarts.
// Returns false if the record is truncated and was skipped.
func (s *walShard) replayRemoveNode(data []byte) bool {
	if len(data) < removeNodeRecordSize {
		return false
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
	return true
}

// ensureActiveSegment creates the active segment if one does not exist.
// On first write to a lazy shard, this also creates the shard directory.
func (s *walShard) ensureActiveSegment() error {
	if s.activeSegment != nil {
		return nil
	}

	// Lazily create shard directory on first write.
	if err := s.fs.MkdirAll(s.dir, 0700); err != nil {
		return &DBOpenError{Dir: s.dir, Err: err}
	}

	segID := s.nextSegmentID
	seg, err := createSegment(s.fs, s.dir, segID, s.opts.blockSize, s.opts.maxFileSize)
	if err != nil {
		return &SegmentCreateError{SegmentID: segID, Err: err}
	}
	seg.noSync = s.opts.noSync

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
				return err
			}
		}
		if err := s.activeSegment.Close(); err != nil {
			return err
		}
		s.activeSegment = nil
	}
	return s.ensureActiveSegment()
}

// getOrCreateIndex returns the entry index for the given node, creating
// one if it does not exist.
func (s *walShard) getOrCreateIndex(key nodeKey) *index {
	idx, ok := s.indexes[key]
	if !ok {
		idx = newIndex()
		s.indexes[key] = idx
	}
	return idx
}

// writeRecord writes a raw record to the active segment, rotating if the
// segment is full. On a lazy shard, this triggers directory and segment
// creation via ensureActiveSegment. When a barrier is configured, the
// record data is encrypted before writing.
func (s *walShard) writeRecord(data []byte) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}

	// Encrypt the record data when a barrier is configured.
	writeData := data
	if s.barrier != nil {
		encrypted, err := s.barrier.Encrypt(nil, data)
		if err != nil {
			return &WALEncryptError{Err: err}
		}
		writeData = encrypted

		// Track the minimum write epoch for safe epoch purging.
		// The epoch is in the first 8 bytes of the ciphertext (big-endian).
		if len(encrypted) >= ciphertextEpochSize {
			writeEpoch := binary.BigEndian.Uint64(encrypted[:ciphertextEpochSize])
			if writeEpoch > 0 {
				if s.minWriteEpoch == 0 || writeEpoch < s.minWriteEpoch {
					s.minWriteEpoch = writeEpoch
				}
				segID := s.activeSegmentID
				if existing, ok := s.segmentEpochs[segID]; !ok || writeEpoch < existing {
					s.segmentEpochs[segID] = writeEpoch
				}
			}
		}
	}

	_, err := s.activeSegment.Write(writeData)
	if err == ErrSegmentFull {
		if err := s.rotateSegment(); err != nil {
			return err
		}
		_, err = s.activeSegment.Write(writeData)
	}
	if err != nil {
		return err
	}

	if s.metrics != nil {
		s.metrics.IncWriteBytes(int64(len(writeData)))
	}

	if !s.opts.noSync && !s.batchMode {
		fsyncStart := time.Now()
		syncErr := s.activeSegment.Sync()
		if s.metrics != nil {
			s.metrics.ObserveFsyncLatency(time.Since(fsyncStart))
		}
		return syncErr
	}
	return nil
}

// syncSegment flushes the active segment to stable storage. This is the
// "sync phase" of a batched write, called after all records have been
// buffered with batchMode enabled. The caller must hold s.mu.
func (s *walShard) syncSegment() error {
	if s.opts.noSync || s.activeSegment == nil {
		return nil
	}
	fsyncStart := time.Now()
	syncErr := s.activeSegment.Sync()
	if s.metrics != nil {
		s.metrics.ObserveFsyncLatency(time.Since(fsyncStart))
	}
	return syncErr
}

// close closes the active segment and releases resources.
func (s *walShard) close() error {
	if s.activeSegment != nil {
		return s.activeSegment.Close()
	}
	return nil
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
func syncDirFS(filesystem FS, dir string) error {
	d, err := filesystem.Open(dir)
	if err != nil {
		return &DirSyncError{Dir: dir, Err: err}
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return &DirSyncError{Dir: dir, Err: syncErr}
	}
	if closeErr != nil {
		return &DirSyncError{Dir: dir, Err: closeErr}
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
	if db.closed.Load() {
		return logdb.ErrClosed
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

	// Phase 1: Write all records to their shards WITHOUT fsyncing.
	// Use a stack-allocated array to track dirty shards, avoiding
	// map allocation on the hot path.
	var dirtyShards [DefaultShardCount]*walShard
	dirtyCount := 0

	// Track pre-batch segment state for each shard so we can roll back
	// on partial failure. Stack-allocated to avoid heap allocation.
	var checkpoints [DefaultShardCount]shardCheckpoint

	// Accumulate in-memory updates per shard. Applied only after all
	// WAL writes and sync succeed, preventing phantom reads on failure.
	var pending [DefaultShardCount]pendingMemUpdates

	for i := range updates {
		u := &updates[i]
		shardIdx := uint16(u.ShardID % DefaultShardCount)
		shard := db.shards[shardIdx]

		shard.mu.Lock()

		// Capture the segment checkpoint before first write to this shard.
		// For non-lazy shards with an existing active segment, capture now.
		if !checkpoints[shardIdx].valid && shard.activeSegment != nil {
			checkpoints[shardIdx] = shardCheckpoint{
				fileOffset:  shard.activeSegment.fileOffset,
				blockOffset: shard.activeSegment.blockOffset,
				valid:       true,
			}
		}

		shard.batchMode = true
		err := shard.saveUpdate(u, &pending[shardIdx], &checkpoints[shardIdx])
		shard.batchMode = false
		if err != nil {
			shard.mu.Unlock()

			// Discard buffered data from all previously dirtied shards
			// to prevent ghost records on the next successful SaveState.
			discardErrors := discardDirtyShards(&dirtyShards, &checkpoints, shard, shardIdx)

			if len(discardErrors) > 0 {
				return &BatchWriteError{
					WriteErr:      err,
					DiscardErrors: discardErrors,
				}
			}
			return err
		}
		shard.mu.Unlock()

		if dirtyShards[shardIdx] == nil {
			dirtyShards[shardIdx] = shard
			dirtyCount++
		}
	}

	// Phase 2: Sync all dirty shards to stable storage.
	if err := syncDirtyShards(&dirtyShards, dirtyCount); err != nil {
		return err
	}

	// Phase 3: Apply in-memory updates now that all WAL data is durable.
	// This is the commit point — reads will see these changes only after
	// the data is safely persisted.
	for i := range dirtyShards {
		if dirtyShards[i] == nil {
			continue
		}
		s := dirtyShards[i]
		s.mu.Lock()
		pending[i].applyTo(s)
		s.mu.Unlock()
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
	valid       bool
}

// pendingEntryUpdate captures a deferred entry map and index update.
type pendingEntryUpdate struct {
	key     nodeKey
	entries []logdb.Entry
	indexes []indexEntry
}

// pendingStateUpdate captures a deferred hard-state update.
type pendingStateUpdate struct {
	key   nodeKey
	state logdb.State
}

// pendingSnapshotUpdate captures a deferred snapshot metadata update.
type pendingSnapshotUpdate struct {
	key      nodeKey
	snapshot logdb.Snapshot
}

// pendingMemUpdates accumulates in-memory updates during a batch write.
// Updates are applied atomically after all WAL writes and sync succeed,
// ensuring no phantom data is readable on mid-batch failure.
type pendingMemUpdates struct {
	entries   []pendingEntryUpdate
	states    []pendingStateUpdate
	snapshots []pendingSnapshotUpdate
}

// applyTo applies all pending in-memory updates to the shard. The caller
// must hold shard.mu.
func (p *pendingMemUpdates) applyTo(s *walShard) {
	for i := range p.entries {
		eu := &p.entries[i]
		idx := s.getOrCreateIndex(eu.key)
		idx.Append(eu.indexes...)

		nodeEntries := s.entries[eu.key]
		if nodeEntries == nil {
			nodeEntries = make(map[uint64]logdb.Entry)
			s.entries[eu.key] = nodeEntries
		}
		for j := range eu.entries {
			nodeEntries[eu.entries[j].Index] = eu.entries[j]
		}
	}

	for i := range p.states {
		s.states[p.states[i].key] = p.states[i].state
	}

	for i := range p.snapshots {
		s.snapshots[p.snapshots[i].key] = p.snapshots[i].snapshot
	}
}

// discardDirtyShards rolls back buffered WAL data for all previously dirtied
// shards and the failing shard. This prevents ghost records from persisting
// on the next successful SaveState. The caller must NOT hold any shard locks.
func discardDirtyShards(
	dirtyShards *[DefaultShardCount]*walShard,
	checkpoints *[DefaultShardCount]shardCheckpoint,
	failingShard *walShard,
	failingIdx uint16,
) []error {
	var discardErrors []error
	for j := range dirtyShards {
		if dirtyShards[j] == nil || !checkpoints[j].valid {
			continue
		}
		ds := dirtyShards[j]
		ds.mu.Lock()
		if ds.activeSegment != nil {
			if dErr := ds.activeSegment.DiscardPending(
				checkpoints[j].fileOffset,
				checkpoints[j].blockOffset,
			); dErr != nil {
				discardErrors = append(discardErrors, dErr)
			}
		}
		ds.mu.Unlock()
	}
	// Also discard the failing shard if it had a checkpoint.
	if checkpoints[failingIdx].valid {
		failingShard.mu.Lock()
		if failingShard.activeSegment != nil {
			if dErr := failingShard.activeSegment.DiscardPending(
				checkpoints[failingIdx].fileOffset,
				checkpoints[failingIdx].blockOffset,
			); dErr != nil {
				discardErrors = append(discardErrors, dErr)
			}
		}
		failingShard.mu.Unlock()
	}
	return discardErrors
}

// syncDirtyShards fsyncs all dirty shard segments to stable storage.
// When a single shard is dirty, the sync is performed inline to avoid
// goroutine overhead. Multiple dirty shards are synced in parallel.
func syncDirtyShards(dirtyShards *[DefaultShardCount]*walShard, dirtyCount int) error {
	if dirtyCount == 1 {
		// Fast path: single shard, avoid goroutine overhead.
		for i := range dirtyShards {
			if dirtyShards[i] != nil {
				dirtyShards[i].mu.Lock()
				err := dirtyShards[i].syncSegment()
				dirtyShards[i].mu.Unlock()
				return err
			}
		}
	} else if dirtyCount > 1 {
		// Parallel sync across shards — each fdatasync runs concurrently.
		errCh := make(chan error, dirtyCount)
		for i := range dirtyShards {
			if dirtyShards[i] != nil {
				go func(s *walShard) {
					s.mu.Lock()
					err := s.syncSegment()
					s.mu.Unlock()
					errCh <- err
				}(dirtyShards[i])
			}
		}
		var firstErr error
		for range dirtyCount {
			if err := <-errCh; err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			return firstErr
		}
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

	// Write snapshot if non-empty and not stale.
	if u.Snapshot.Index != 0 || u.Snapshot.Term != 0 {
		// Check against both persisted snapshots and any pending snapshot
		// updates in this batch to avoid writing stale data.
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
	// Ensure the active segment exists before capturing the segment ID,
	// since a lazy shard may not have created the directory or segment yet.
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}

	// Capture checkpoint for lazy shards after segment creation but
	// before any data is written. This ensures rollback can discard
	// data written to a newly created segment.
	captureCheckpointAfterEnsure(s, cp)

	// Calculate total size needed.
	totalSize := 1 // payload type byte
	for i := range entries {
		totalSize += entryRecordHeaderSize + len(entries[i].Cmd)
	}

	buf := s.getMarshalBuf(totalSize)
	buf[0] = payloadTypeEntries
	pos := 1

	segID := s.activeSegmentID

	// Collect index and entry updates to stage into pending.
	stagedIdx := make([]indexEntry, 0, len(entries))
	stagedEntries := make([]logdb.Entry, 0, len(entries))

	for i := range entries {
		e := &entries[i]
		binary.LittleEndian.PutUint64(buf[pos:], shardID)
		binary.LittleEndian.PutUint64(buf[pos+8:], replicaID)
		binary.LittleEndian.PutUint64(buf[pos+16:], e.Index)
		binary.LittleEndian.PutUint64(buf[pos+24:], e.Term)
		binary.LittleEndian.PutUint64(buf[pos+32:], e.Type)
		binary.LittleEndian.PutUint32(buf[pos+40:], uint32(len(e.Cmd)))
		copy(buf[pos+44:], e.Cmd)

		entrySize := entryRecordHeaderSize + len(e.Cmd)

		stagedIdx = append(stagedIdx, indexEntry{
			Index:     e.Index,
			Term:      e.Term,
			SegmentID: segID,
			Size:      int64(entrySize),
		})

		// Copy Cmd to avoid aliasing the caller's slice.
		var cmdCopy []byte
		if len(e.Cmd) > 0 {
			cmdCopy = make([]byte, len(e.Cmd))
			copy(cmdCopy, e.Cmd)
		}
		stagedEntries = append(stagedEntries, logdb.Entry{
			Index: e.Index,
			Term:  e.Term,
			Type:  e.Type,
			Cmd:   cmdCopy,
		})

		pos += entrySize
	}

	// Persist to WAL. On failure, no in-memory state is affected.
	if err := s.writeRecord(buf[:pos]); err != nil {
		return err
	}

	// WAL write succeeded. Stage in-memory updates for deferred application.
	pending.entries = append(pending.entries, pendingEntryUpdate{
		key:     key,
		entries: stagedEntries,
		indexes: stagedIdx,
	})

	return nil
}

// captureCheckpointAfterEnsure captures a checkpoint for a shard that
// just had its active segment created by ensureActiveSegment. This
// handles lazy shards that had no segment at batch start. The checkpoint
// is captured at the segment's current position (before any batch data).
func captureCheckpointAfterEnsure(s *walShard, cp *shardCheckpoint) {
	if cp.valid || s.activeSegment == nil {
		return
	}
	cp.fileOffset = s.activeSegment.fileOffset
	cp.blockOffset = s.activeSegment.blockOffset
	cp.valid = true
}

// writeState serializes and writes a hard state record to the WAL.
// The in-memory state update is staged in pending for deferred application.
func (s *walShard) writeState(key nodeKey, shardID, replicaID uint64, state logdb.State, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	captureCheckpointAfterEnsure(s, cp)

	buf := s.getMarshalBuf(1 + stateRecordSize)
	buf[0] = payloadTypeState
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	binary.LittleEndian.PutUint64(buf[17:], state.Term)
	binary.LittleEndian.PutUint64(buf[25:], state.Vote)
	binary.LittleEndian.PutUint64(buf[33:], state.Commit)

	if err := s.writeRecord(buf[:1+stateRecordSize]); err != nil {
		return err
	}
	pending.states = append(pending.states, pendingStateUpdate{key: key, state: state})
	return nil
}

// writeSnapshot serializes and writes snapshot metadata to the WAL,
// including Epoch and full Membership state.
// The in-memory snapshot update is staged in pending for deferred application.
func (s *walShard) writeSnapshot(key nodeKey, shardID, replicaID uint64, ss logdb.Snapshot, pending *pendingMemUpdates, cp *shardCheckpoint) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	captureCheckpointAfterEnsure(s, cp)

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
	binary.LittleEndian.PutUint32(buf[57:], uint32(len(ss.Filepath)))
	copy(buf[61:], ss.Filepath)

	pos := 1 + snapshotRecordBaseSize + len(ss.Filepath)

	// Serialize membership.
	binary.LittleEndian.PutUint64(buf[pos:], ss.Membership.ConfigChangeID)
	pos += 8
	pos = marshalAddrMap(buf, pos, ss.Membership.Addresses)
	pos = marshalAddrMap(buf, pos, ss.Membership.Observers)
	pos = marshalAddrMap(buf, pos, ss.Membership.Witnesses)
	pos = marshalRemovedMap(buf, pos, ss.Membership.Removed)

	if err := s.writeRecord(buf[:pos]); err != nil {
		return err
	}
	pending.snapshots = append(pending.snapshots, pendingSnapshotUpdate{key: key, snapshot: ss})
	return nil
}

// writeBootstrap serializes and writes bootstrap info to the WAL.
//
// INVARIANT: This method MUST only be called outside of batchMode (i.e. from
// SaveBootstrap). In non-batch mode, writeRecord calls fdatasync inline before
// returning, so the immediate in-memory update below is safe — the data is
// already durable. If this method were ever called in batchMode, the immediate
// update would create a phantom read before fsync. Callers adding batch-mode
// bootstrap support must route through pendingMemUpdates instead.
func (s *walShard) writeBootstrap(key nodeKey, shardID, replicaID uint64, bs logdb.Bootstrap) error {
	// Guard against accidental batch-mode usage. See invariant above.
	if s.batchMode {
		return &WriteBootstrapBatchModeError{}
	}
	// Calculate total size.
	totalSize := 1 + bootstrapRecordBaseSize
	for _, addr := range bs.Addresses {
		totalSize += 8 + 4 + len(addr)
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

	if err := s.writeRecord(buf[:pos]); err != nil {
		return err
	}
	s.bootstraps[key] = bs
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
func (db *DB) IterateEntries(
	entries []logdb.Entry, size uint64,
	shardID, replicaID, low, high, maxSize uint64,
) ([]logdb.Entry, uint64, error) {
	if db.closed.Load() {
		return entries, size, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx, ok := shard.indexes[key]
	if !ok {
		return entries, size, nil
	}

	compactedTo := shard.compactedTo[key]
	if low <= compactedTo {
		return entries, size, logdb.ErrCompacted
	}

	nodeEntries := shard.entries[key]
	rangeEntries := idx.Range(low, high)
	if len(rangeEntries) == 0 {
		return entries, size, nil
	}

	totalSize := size
	for _, ie := range rangeEntries {
		if maxSize > 0 && totalSize > 0 && totalSize >= maxSize {
			break
		}

		if nodeEntries != nil {
			if e, found := nodeEntries[ie.Index]; found {
				entries = append(entries, e)
				totalSize += uint64(ie.Size)
			}
		}
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

	shard.mu.Lock()
	state, ok := shard.states[key]
	ss := shard.snapshots[key]
	shard.mu.Unlock()

	if !ok {
		return logdb.State{}, logdb.Membership{}, logdb.ErrNoState
	}
	return state, ss.Membership, nil
}

// EntryRange returns the first index and total count of available log
// entries for the given node. Returns (0, 0, nil) when no entries exist.
func (db *DB) EntryRange(shardID, replicaID uint64) (uint64, uint64, error) {
	if db.closed.Load() {
		return 0, 0, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	idx, ok := shard.indexes[key]
	shard.mu.Unlock()

	if !ok {
		return 0, 0, nil
	}

	first, hasFirst := idx.First()
	if !hasFirst {
		return 0, 0, nil
	}
	return first.Index, uint64(idx.Len()), nil
}

// RemoveEntriesTo removes log entries up to and including the given index
// for the specified node.
func (db *DB) RemoveEntriesTo(shardID, replicaID, index uint64) error {
	if db.closed.Load() {
		return logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Write compaction marker to WAL for durability across restarts.
	buf := shard.getMarshalBuf(1 + compactionRecordSize)
	buf[0] = payloadTypeCompact
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	binary.LittleEndian.PutUint64(buf[17:], index)
	if err := shard.writeRecord(buf[:1+compactionRecordSize]); err != nil {
		return err
	}

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
	if db.closed.Load() {
		return logdb.ErrClosed
	}

	compactStart := time.Now()

	shard := db.getShard(shardID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	comp := newCompactor(shard.fs, shard.dir, shard.opts.blockSize)
	liveSegments := make(map[uint64]bool)
	for _, idx := range shard.indexes {
		for segID := range idx.AllSegmentIDs() {
			liveSegments[segID] = true
		}
	}

	deleted, err := comp.GarbageCollect(liveSegments, shard.activeSegmentID)

	// After GC, recompute minWriteEpoch from remaining segment epochs.
	// Only needed when segments were actually deleted and a barrier is
	// configured (otherwise segmentEpochs is empty).
	if deleted > 0 && shard.barrier != nil {
		shard.recomputeMinWriteEpoch(liveSegments)
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
	if db.closed.Load() {
		return logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Reject stale snapshots, consistent with MemDB behavior.
	if existing, ok := shard.snapshots[key]; ok && snapshot.Index <= existing.Index {
		return logdb.ErrSnapshotOutOfDate
	}

	// Standalone snapshot write — apply in-memory update immediately
	// since there is no batch to fail.
	var p pendingMemUpdates
	var cp shardCheckpoint
	if shard.activeSegment != nil {
		cp = shardCheckpoint{
			fileOffset:  shard.activeSegment.fileOffset,
			blockOffset: shard.activeSegment.blockOffset,
			valid:       true,
		}
	}
	if err := shard.writeSnapshot(key, shardID, replicaID, snapshot, &p, &cp); err != nil {
		return err
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

	shard.mu.Lock()
	ss, ok := shard.snapshots[key]
	shard.mu.Unlock()

	if !ok {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}
	return ss, nil
}

// RemoveNodeData durably removes all data for the given node. A remove-node
// record is written to the WAL so the removal persists across restarts.
// After clearing in-memory state, segment garbage collection is triggered
// to reclaim disk space from segments that no longer contain live data.
func (db *DB) RemoveNodeData(shardID, replicaID uint64) error {
	if db.closed.Load() {
		return logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Write the remove-node record to the WAL for durability.
	buf := shard.getMarshalBuf(1 + removeNodeRecordSize)
	buf[0] = payloadTypeRemoveNode
	binary.LittleEndian.PutUint64(buf[1:], shardID)
	binary.LittleEndian.PutUint64(buf[9:], replicaID)
	if err := shard.writeRecord(buf[:1+removeNodeRecordSize]); err != nil {
		return err
	}

	// Clear all in-memory state for this node.
	delete(shard.indexes, key)
	delete(shard.entries, key)
	delete(shard.states, key)
	delete(shard.bootstraps, key)
	delete(shard.snapshots, key)
	delete(shard.compactedTo, key)

	// Trigger segment garbage collection to reclaim disk space. Collect
	// the set of segments still referenced by remaining live nodes.
	liveSegments := make(map[uint64]bool)
	for _, idx := range shard.indexes {
		for segID := range idx.AllSegmentIDs() {
			liveSegments[segID] = true
		}
	}
	comp := newCompactor(shard.fs, shard.dir, shard.opts.blockSize)
	// Best-effort GC: errors are non-fatal for the remove operation
	// but are logged for observability.
	if _, gcErr := comp.GarbageCollect(liveSegments, shard.activeSegmentID); gcErr != nil {
		slog.Warn("waldb: garbage collection failed after RemoveNodeData",
			"shard_dir", shard.dir,
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", gcErr,
		)
	}

	// Fsync the parent directory to make segment deletions durable.
	// Best-effort: errors are logged but do not fail the remove operation,
	// because the WAL remove-node record is already persisted.
	if err := syncDirFS(shard.fs, shard.dir); err != nil {
		slog.Warn("waldb: directory sync failed after RemoveNodeData",
			"shard_dir", shard.dir,
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", err,
		)
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
		shard.mu.Lock()
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
		shard.mu.Unlock()
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
func (db *DB) SaveBootstrap(shardID, replicaID uint64, bootstrap logdb.Bootstrap) error {
	if db.closed.Load() {
		return logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.writeBootstrap(key, shardID, replicaID, bootstrap)
}

// GetBootstrap returns the bootstrap information for a node.
func (db *DB) GetBootstrap(shardID, replicaID uint64) (logdb.Bootstrap, bool, error) {
	if db.closed.Load() {
		return logdb.Bootstrap{}, false, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	bs, ok := shard.bootstraps[key]
	shard.mu.Unlock()

	if !ok {
		return logdb.Bootstrap{}, false, nil
	}
	return bs, true, nil
}

// Close closes the DB and releases all resources.
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		return logdb.ErrClosed
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
	if db.closed.Load() {
		return logdb.ErrClosed
	}

	return db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Entries:   entries,
	}})
}

// GetEntries returns entries in [low, high) up to maxSize bytes.
func (db *DB) GetEntries(shardID, replicaID, low, high, maxSize uint64) ([]logdb.Entry, error) {
	if db.closed.Load() {
		return nil, logdb.ErrClosed
	}

	shard := db.getShard(shardID)
	key := nodeKey{ShardID: shardID, ReplicaID: replicaID}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx, ok := shard.indexes[key]
	if !ok {
		return nil, nil
	}

	compactedTo := shard.compactedTo[key]
	if low <= compactedTo {
		return nil, logdb.ErrCompacted
	}

	nodeEntries := shard.entries[key]
	rangeEntries := idx.Range(low, high)
	if len(rangeEntries) == 0 {
		return nil, nil
	}

	var totalSize uint64
	result := make([]logdb.Entry, 0, len(rangeEntries))

	for _, ie := range rangeEntries {
		if maxSize > 0 && totalSize > 0 && totalSize >= maxSize {
			break
		}

		if nodeEntries != nil {
			if e, found := nodeEntries[ie.Index]; found {
				result = append(result, e)
				totalSize += uint64(ie.Size)
			}
		}
	}

	return result, nil
}

// SaveNodeState persists the hard state for a single node (convenience
// method).
func (db *DB) SaveNodeState(shardID, replicaID uint64, state logdb.State) error {
	if db.closed.Load() {
		return logdb.ErrClosed
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
		shard.mu.Lock()
		shardMin := shard.minWriteEpoch
		shard.mu.Unlock()

		if shardMin > 0 && (globalMin == 0 || shardMin < globalMin) {
			globalMin = shardMin
		}
	}
	return globalMin, nil
}

// Compile-time interface check for EpochAwareLogDB.
var _ logdb.EpochAwareLogDB = (*DB)(nil)

// recomputeMinWriteEpoch recalculates the shard's minWriteEpoch from the
// segmentEpochs map after garbage collection removes dead segments.
// liveSegments contains the set of segment IDs that survived GC. The
// active segment is always considered live.
//
// Caller must hold s.mu.
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
