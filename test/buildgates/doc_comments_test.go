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

package buildgates

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// docCommentRoots are the public trees whose exported API must be documented.
// contrib/dragonboat is a separate module; the gate parses its files directly,
// so it needs neither that module's requirements nor the network.
var docCommentRoots = []struct {
	dir      string
	minFiles int
}{
	{dir: filepath.Join(repoRoot, "pkg"), minFiles: 50},
	{dir: filepath.Join(repoRoot, "contrib", "dragonboat"), minFiles: 10},
}

// docCommentAllowlist names package directories (repository-relative) whose
// missing doc comments the gate does not yet enforce. An entry that no longer
// has anything missing fails the gate, so the list only shrinks.
var docCommentAllowlist = map[string]bool{}

// docFieldBaseline names, one entry per field, the exported struct fields
// that had neither a doc nor a trailing line comment when the gate began
// checking fields. Each key is "<repo-relative file>: <Type>.<Field>"; line
// numbers are not part of it. It only shrinks: an undocumented field it does
// not list fails and must be documented, and an entry that matches no
// undocumented field fails and must be deleted.
var docFieldBaseline = map[string]bool{
	"contrib/dragonboat/config/errors.go: FileReadError.Err":                       true,
	"contrib/dragonboat/config/errors.go: FileReadError.Path":                      true,
	"contrib/dragonboat/config/errors.go: ValidationError.Field":                   true,
	"contrib/dragonboat/config/errors.go: ValidationError.Message":                 true,
	"contrib/dragonboat/config/nodehost_config.go: ExpertConfig.Engine":            true,
	"contrib/dragonboat/config/nodehost_config.go: ExpertConfig.LogDB":             true,
	"contrib/dragonboat/config/nodehost_config.go: LogDBInfo.Busy":                 true,
	"contrib/dragonboat/config/nodehost_config.go: LogDBInfo.Shard":                true,
	"contrib/dragonboat/errors.go: AdapterError.Err":                               true,
	"contrib/dragonboat/errors.go: RemovalMarkerError.Err":                         true,
	"contrib/dragonboat/errors.go: RemovalMarkerError.Op":                          true,
	"contrib/dragonboat/errors.go: RemovalMarkerError.Path":                        true,
	"contrib/dragonboat/errors.go: ShardSettingsError.Err":                         true,
	"contrib/dragonboat/nodehost.go: LogNodeInfo.ReplicaID":                        true,
	"contrib/dragonboat/nodehost.go: LogNodeInfo.ShardID":                          true,
	"contrib/dragonboat/nodehost.go: NodeHostConstructionError.Err":                true,
	"contrib/dragonboat/nodehost.go: NodeHostConstructionError.Reason":             true,
	"contrib/dragonboat/raftio/events.go: ConnectionInfo.Address":                  true,
	"contrib/dragonboat/raftio/events.go: EntryInfo.Index":                         true,
	"contrib/dragonboat/raftio/events.go: EntryInfo.ReplicaID":                     true,
	"contrib/dragonboat/raftio/events.go: EntryInfo.ShardID":                       true,
	"contrib/dragonboat/raftio/events.go: LeaderInfo.LeaderID":                     true,
	"contrib/dragonboat/raftio/events.go: LeaderInfo.ReplicaID":                    true,
	"contrib/dragonboat/raftio/events.go: LeaderInfo.ShardID":                      true,
	"contrib/dragonboat/raftio/events.go: LeaderInfo.Term":                         true,
	"contrib/dragonboat/raftio/events.go: NodeInfo.ReplicaID":                      true,
	"contrib/dragonboat/raftio/events.go: NodeInfo.ShardID":                        true,
	"contrib/dragonboat/raftio/events.go: SnapshotInfo.From":                       true,
	"contrib/dragonboat/raftio/events.go: SnapshotInfo.Index":                      true,
	"contrib/dragonboat/raftio/events.go: SnapshotInfo.ReplicaID":                  true,
	"contrib/dragonboat/raftio/events.go: SnapshotInfo.ShardID":                    true,
	"contrib/dragonboat/statemachine/errors.go: AdapterCloseError.Cause":           true,
	"contrib/dragonboat/statemachine/errors.go: AdapterLookupError.Cause":          true,
	"contrib/dragonboat/statemachine/errors.go: AdapterOpenError.Cause":            true,
	"contrib/dragonboat/statemachine/errors.go: AdapterPrepareSnapshotError.Cause": true,
	"contrib/dragonboat/statemachine/errors.go: AdapterSnapshotRecoverError.Cause": true,
	"contrib/dragonboat/statemachine/errors.go: AdapterSnapshotSaveError.Cause":    true,
	"contrib/dragonboat/statemachine/errors.go: AdapterSyncError.Cause":            true,
	"pkg/batch/errors.go: DecodeError.Reason":                                      true,
	"pkg/batch/errors.go: EncodeError.Reason":                                      true,
	"pkg/batch/errors.go: FlushError.Err":                                          true,
	"pkg/batch/errors.go: FlushError.ShardID":                                      true,
	"pkg/batch/errors.go: QueueFullError.QueueSize":                                true,
	"pkg/config/errors.go: KeyRotationIntervalTooSmallError.Configured":            true,
	"pkg/config/errors.go: KeyRotationIntervalTooSmallError.Minimum":               true,
	"pkg/config/errors.go: LogValidationError.Field":                               true,
	"pkg/config/errors.go: LogValidationError.Message":                             true,
	"pkg/config/errors.go: LogValidationError.Value":                               true,
	"pkg/config/errors.go: MaxApplyEntrySizeExceededError.Configured":              true,
	"pkg/config/errors.go: MaxApplyEntrySizeExceededError.FrameMaximum":            true,
	"pkg/config/errors.go: MaxEntrySizeExceededError.Configured":                   true,
	"pkg/config/errors.go: MaxEntrySizeExceededError.ProtocolMaximum":              true,
	"pkg/config/errors.go: MaxMessageReceiveMemoryTooSmallError.Configured":        true,
	"pkg/config/errors.go: MaxMessageReceiveMemoryTooSmallError.Minimum":           true,
	"pkg/config/errors.go: MaxProposalPayloadSizeExceededError.Configured":         true,
	"pkg/config/errors.go: MaxProposalPayloadSizeExceededError.ProtocolMaximum":    true,
	"pkg/config/errors.go: SessionExpiryEntriesTooSmallError.Configured":           true,
	"pkg/config/errors.go: SessionExpiryEntriesTooSmallError.Minimum":              true,
	"pkg/config/errors.go: ValidationError.Field":                                  true,
	"pkg/config/errors.go: ValidationError.Message":                                true,
	"pkg/config/hostconfig.go: ConnectionInfo.Address":                             true,
	"pkg/config/hostconfig.go: ConnectionInfo.Err":                                 true,
	"pkg/config/hostconfig.go: DiskInfo.MaxBytes":                                  true,
	"pkg/config/hostconfig.go: DiskInfo.UsedBytes":                                 true,
	"pkg/config/hostconfig.go: DiskInfo.WALDir":                                    true,
	"pkg/config/hostconfig.go: EventShardInfo.LeaderID":                            true,
	"pkg/config/hostconfig.go: EventShardInfo.ReplicaID":                           true,
	"pkg/config/hostconfig.go: EventShardInfo.ShardID":                             true,
	"pkg/config/hostconfig.go: EventShardInfo.Term":                                true,
	"pkg/config/hostconfig.go: LeaderInfo.LeaderID":                                true,
	"pkg/config/hostconfig.go: LeaderInfo.ReplicaID":                               true,
	"pkg/config/hostconfig.go: LeaderInfo.ShardID":                                 true,
	"pkg/config/hostconfig.go: LeaderInfo.Term":                                    true,
	"pkg/config/hostconfig.go: ShardFailedInfo.Err":                                true,
	"pkg/config/hostconfig.go: ShardFailedInfo.ReplicaID":                          true,
	"pkg/config/hostconfig.go: ShardFailedInfo.ShardID":                            true,
	"pkg/config/hostconfig.go: SnapshotInfo.From":                                  true,
	"pkg/config/hostconfig.go: SnapshotInfo.Index":                                 true,
	"pkg/config/hostconfig.go: SnapshotInfo.ReplicaID":                             true,
	"pkg/config/hostconfig.go: SnapshotInfo.ShardID":                               true,
	"pkg/crypto/shamir/errors.go: CombineError.Reason":                             true,
	"pkg/crypto/shamir/errors.go: SplitError.Reason":                               true,
	"pkg/crypto/shamir/errors.go: ValidationError.Field":                           true,
	"pkg/crypto/shamir/errors.go: ValidationError.Reason":                          true,
	"pkg/errors.go: BusyError.CurrentSize":                                         true,
	"pkg/errors.go: BusyError.MaxSize":                                             true,
	"pkg/errors.go: BusyError.ShardID":                                             true,
	"pkg/errors.go: EmptyMembersError.ShardID":                                     true,
	"pkg/errors.go: EntryTooLargeError.MaxSize":                                    true,
	"pkg/errors.go: EntryTooLargeError.Size":                                       true,
	"pkg/errors.go: ImportDataDirMismatchError.Declared":                           true,
	"pkg/errors.go: ImportDataDirMismatchError.Recorded":                           true,
	"pkg/errors.go: ImportDataDirMismatchError.ReplicaID":                          true,
	"pkg/errors.go: ImportDataDirMismatchError.ShardID":                            true,
	"pkg/errors.go: ImportLayoutUnknownError.ReplicaID":                            true,
	"pkg/errors.go: ImportLayoutUnknownError.ShardID":                              true,
	"pkg/errors.go: ImportMembershipInvalidError.ReplicaID":                        true,
	"pkg/errors.go: ImportMembershipInvalidError.ShardID":                          true,
	"pkg/errors.go: ImportSnapshotDirExistsError.Dir":                              true,
	"pkg/errors.go: ImportSnapshotDirExistsError.Index":                            true,
	"pkg/errors.go: ImportSnapshotDirExistsError.ReplicaID":                        true,
	"pkg/errors.go: ImportSnapshotDirExistsError.ShardID":                          true,
	"pkg/errors.go: ImportSnapshotOutOfDateError.ReplicaID":                        true,
	"pkg/errors.go: ImportSnapshotOutOfDateError.ShardID":                          true,
	"pkg/errors.go: InvalidMemberError.Address":                                    true,
	"pkg/errors.go: InvalidMemberError.Reason":                                     true,
	"pkg/errors.go: InvalidMemberError.ReplicaID":                                  true,
	"pkg/errors.go: InvalidMemberError.ShardID":                                    true,
	"pkg/errors.go: LeaderHandlerError.LeaderID":                                   true,
	"pkg/errors.go: LeaderHandlerError.Message":                                    true,
	"pkg/errors.go: LeaderHandlerError.ShardID":                                    true,
	"pkg/errors.go: LeaderHandlerError.Term":                                       true,
	"pkg/errors.go: LeaderUnavailableError.Cause":                                  true,
	"pkg/errors.go: LeaderUnavailableError.Err":                                    true,
	"pkg/errors.go: LeaderUnavailableError.ShardID":                                true,
	"pkg/errors.go: LoopPanicError.LoopName":                                       true,
	"pkg/errors.go: LoopPanicError.Stack":                                          true,
	"pkg/errors.go: LoopPanicError.Value":                                          true,
	"pkg/errors.go: MaybeAppliedError.Err":                                         true,
	"pkg/errors.go: NonVotingRoleInMembersError.ReplicaID":                         true,
	"pkg/errors.go: NonVotingRoleInMembersError.ShardID":                           true,
	"pkg/errors.go: NotLeaderError.ShardID":                                        true,
	"pkg/errors.go: ReplicaNotFoundError.ReplicaID":                                true,
	"pkg/errors.go: ReplicaNotFoundError.ShardID":                                  true,
	"pkg/errors.go: ReplicaRetiredError.ReplicaID":                                 true,
	"pkg/errors.go: ReplicaRetiredError.ShardID":                                   true,
	"pkg/errors.go: SelfNotInMembersError.ReplicaID":                               true,
	"pkg/errors.go: SelfNotInMembersError.ShardID":                                 true,
	"pkg/errors.go: SessionError.ClientID":                                         true,
	"pkg/errors.go: SessionError.Err":                                              true,
	"pkg/errors.go: SessionError.Op":                                               true,
	"pkg/errors.go: SessionExpiredError.ClientID":                                  true,
	"pkg/errors.go: SessionExpiredError.ShardID":                                   true,
	"pkg/errors.go: ShardNotStoppedError.ReplicaID":                                true,
	"pkg/errors.go: ShardNotStoppedError.ShardID":                                  true,
	"pkg/errors.go: SnapshotChunkError.ChunkCount":                                 true,
	"pkg/errors.go: SnapshotChunkError.ChunkID":                                    true,
	"pkg/errors.go: SnapshotChunkError.Reason":                                     true,
	"pkg/errors.go: SnapshotFailedError.ReplicaID":                                 true,
	"pkg/errors.go: SnapshotFailedError.ShardID":                                   true,
	"pkg/errors.go: SnapshotReceiveError.Err":                                      true,
	"pkg/errors.go: SnapshotReceiveError.Op":                                       true,
	"pkg/errors.go: SnapshotReceiveError.ReplicaID":                                true,
	"pkg/errors.go: SnapshotReceiveError.ShardID":                                  true,
	"pkg/errors.go: SnapshotSendError.Err":                                         true,
	"pkg/errors.go: SnapshotSendError.Op":                                          true,
	"pkg/errors.go: SnapshotSendError.ReplicaID":                                   true,
	"pkg/errors.go: SnapshotSendError.ShardID":                                     true,
	"pkg/errors.go: SnapshotSizeMismatchError.Actual":                              true,
	"pkg/errors.go: SnapshotSizeMismatchError.Declared":                            true,
	"pkg/errors.go: SnapshotTooLargeError.MaxSize":                                 true,
	"pkg/errors.go: SnapshotTooLargeError.ShardID":                                 true,
	"pkg/errors.go: StaleConfigChangeError.Expected":                               true,
	"pkg/errors.go: StaleConfigChangeError.Got":                                    true,
	"pkg/errors.go: StaleConfigChangeError.ShardID":                                true,
	"pkg/errors.go: UnauthorizedMessageError.From":                                 true,
	"pkg/errors.go: UnauthorizedMessageError.MsgType":                              true,
	"pkg/errors.go: UnauthorizedMessageError.ShardID":                              true,
	"pkg/host.go: HostInitError.Err":                                               true,
	"pkg/host.go: HostInitError.Field":                                             true,
	"pkg/host.go: LogRange.FirstIndex":                                             true,
	"pkg/host.go: LogRange.LastIndex":                                              true,
	"pkg/host.go: SnapshotArtifactError.Err":                                       true,
	"pkg/host.go: SnapshotArtifactError.Reason":                                    true,
	"pkg/logdb/errors.go: EntriesBelowCompactionFloorError.ReplicaID":              true,
	"pkg/logdb/waldb/compaction.go: CompactionDeleteError.Err":                     true,
	"pkg/logdb/waldb/compaction.go: CompactionDeleteError.Path":                    true,
	"pkg/logdb/waldb/compaction.go: CompactionReadDirError.Dir":                    true,
	"pkg/logdb/waldb/compaction.go: CompactionReadDirError.Err":                    true,
	"pkg/logdb/waldb/db.go: DBOpenError.Dir":                                       true,
	"pkg/logdb/waldb/db.go: DBOpenError.Err":                                       true,
	"pkg/logdb/waldb/db.go: RecoveryError.Err":                                     true,
	"pkg/logdb/waldb/db.go: RecoveryError.SegmentID":                               true,
	"pkg/logdb/waldb/db.go: SegmentCreateError.Err":                                true,
	"pkg/logdb/waldb/db.go: SegmentCreateError.SegmentID":                          true,
	"pkg/logdb/waldb/errors.go: BatchWriteError.DiscardErrors":                     true,
	"pkg/logdb/waldb/errors.go: BatchWriteError.WriteErr":                          true,
	"pkg/logdb/waldb/errors.go: DirSyncError.Dir":                                  true,
	"pkg/logdb/waldb/errors.go: DirSyncError.Err":                                  true,
	"pkg/logdb/waldb/errors.go: DiskUsageError.Dir":                                true,
	"pkg/logdb/waldb/errors.go: DiskUsageError.Err":                                true,
	"pkg/logdb/waldb/errors.go: SegmentDiscardError.Err":                           true,
	"pkg/logdb/waldb/errors.go: SegmentDiscardError.SegmentID":                     true,
	"pkg/logdb/waldb/errors.go: SegmentReadError.Err":                              true,
	"pkg/logdb/waldb/errors.go: SegmentReadError.Offset":                           true,
	"pkg/logdb/waldb/errors.go: WALDecryptError.Err":                               true,
	"pkg/logdb/waldb/errors.go: WALDecryptError.SegmentID":                         true,
	"pkg/logdb/waldb/errors.go: WALEncryptError.Err":                               true,
	"pkg/logdb/waldb/format.go: WALFormatVersionError.Dir":                         true,
	"pkg/logdb/waldb/format.go: WALFormatVersionError.Recorded":                    true,
	"pkg/logdb/waldb/format.go: WALFormatVersionError.Supported":                   true,
	"pkg/logdb/waldb/format.go: WALGeometryMismatchError.Configured":               true,
	"pkg/logdb/waldb/format.go: WALGeometryMismatchError.Dir":                      true,
	"pkg/logdb/waldb/format.go: WALGeometryMismatchError.Recorded":                 true,
	"pkg/logdb/waldb/reencrypt.go: ReencryptError.Err":                             true,
	"pkg/logdb/waldb/reencrypt.go: ReencryptError.Op":                              true,
	"pkg/logdb/waldb/repair.go: RepairOffsetMismatchError.CleanEnd":                true,
	"pkg/logdb/waldb/repair.go: RepairOffsetMismatchError.Requested":               true,
	"pkg/logdb/waldb/repair.go: RepairOffsetMismatchError.SegmentID":               true,
	"pkg/logdb/waldb/repair.go: RepairOffsetMismatchError.ShardIndex":              true,
	"pkg/proto/constants.go: WireWindow.Current":                                   true,
	"pkg/proto/constants.go: WireWindow.Previous":                                  true,
	"pkg/seal/errors.go: CipherError.Err":                                          true,
	"pkg/seal/errors.go: Error.Err":                                                true,
	"pkg/seal/errors.go: Error.Strategy":                                           true,
	"pkg/seal/errors.go: KeyDerivationError.Epoch":                                 true,
	"pkg/seal/errors.go: KeyDerivationError.Err":                                   true,
	"pkg/seal/errors.go: UnsealError.Err":                                          true,
	"pkg/seal/errors.go: UnsealError.Strategy":                                     true,
	"pkg/sm/errors.go: CacheMaxEntriesError.Value":                                 true,
	"pkg/sm/errors.go: InvalidCacheConfigError.Reason":                             true,
	"pkg/sm/kv/errors.go: HashWriteError.Err":                                      true,
	"pkg/sm/kv/errors.go: InvalidCommandError.Reason":                              true,
	"pkg/sm/kv/errors.go: KeyNotFoundError.Key":                                    true,
	"pkg/sm/kv/errors.go: KeyTooLongError.Length":                                  true,
	"pkg/sm/kv/protocol.go: Command.Key":                                           true,
	"pkg/sm/kv/protocol.go: Command.Op":                                            true,
	"pkg/sm/kv/protocol.go: Command.Value":                                         true,
	"pkg/sm/kv/protocol.go: ListQuery.Prefix":                                      true,
	"pkg/sm/types.go: Result.Data":                                                 true,
	"pkg/sm/types.go: Result.Value":                                                true,
	"pkg/snapshot_option.go: SnapshotOptionError.Field":                            true,
	"pkg/snapshot_option.go: SnapshotOptionError.Reason":                           true,
	"pkg/writemode/errors.go: InvalidModeError.Value":                              true,
}

// docGap is one exported identifier or package with no doc comment.
type docGap struct {
	pkgDir string
	file   string
	line   int
	what   string
}

// TestExportedIdentifiersHaveDocComments fails for each exported type,
// function, method on an exported type, method declared in an exported
// interface, constant and variable in a public package under pkg/ or
// contrib/dragonboat/ that carries no doc comment, and for each such package
// with no package comment. Exported, named fields of an exported struct need
// a doc or a trailing line comment too; their gaps are held to the
// shrink-only docFieldBaseline, which names each grandfathered field by file,
// type and field (not by count), never to docCommentAllowlist.
//
// Public means not under an internal/ directory and not a _test.go file. A
// grouped const or var block's doc comment counts for its members; each type
// in a grouped type block and each interface method needs its own.
// Build tags are not evaluated: every file is parsed, so a declaration behind
// a tag is held to the same rule.
func TestExportedIdentifiersHaveDocComments(t *testing.T) {
	var gaps []docGap
	scanned := 0
	var parsed []parsedFile

	for _, root := range docCommentRoots {
		rootScanned := 0
		err := filepath.WalkDir(root.dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch d.Name() {
				case "internal", "testdata", "vendor":
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, rerr := repoRelative(path)
			if rerr != nil {
				return rerr
			}
			fset := token.NewFileSet()
			// ParseComments is required: without it every Doc field is nil
			// and the gate reports every identifier.
			file, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if perr != nil {
				return perr
			}
			rootScanned++

			dir := filepath.ToSlash(filepath.Dir(rel))
			parsed = append(parsed, parsedFile{rel: rel, fset: fset, file: file})
			for _, g := range undocumentedDecls(fset, rel, file) {
				g.pkgDir = dir
				gaps = append(gaps, g)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root.dir, err)
		}
		// A root that suddenly holds almost nothing means the tree moved and
		// the gate is passing over an empty set.
		if rootScanned < root.minFiles {
			t.Fatalf("scanned only %d files under %s, expected at least %d; "+
				"the tree moved and this gate is checking nothing",
				rootScanned, root.dir, root.minFiles)
		}
		scanned += rootScanned
	}

	gaps = append(gaps, packageDocGaps(parsed)...)

	var failing []docGap
	allowedHit := map[string]int{}
	var fieldGaps []docGap
	for _, g := range gaps {
		// Struct field gaps count against docFieldBaseline only.
		if strings.HasPrefix(g.what, "field ") {
			fieldGaps = append(fieldGaps, g)
			continue
		}
		if docCommentAllowlist[g.pkgDir] {
			allowedHit[g.pkgDir]++
			continue
		}
		failing = append(failing, g)
	}

	var stale []string
	for dir := range docCommentAllowlist {
		if allowedHit[dir] == 0 {
			stale = append(stale, dir)
		}
	}
	sort.Strings(stale)
	for _, dir := range stale {
		t.Errorf("docCommentAllowlist entry %s has nothing undocumented; remove it", dir)
	}

	// The field baseline names each grandfathered field and only shrinks: a
	// field it does not list must be documented, an entry no field matches
	// must be deleted.
	unlisted, staleFields := fieldBaselineViolations(fieldGaps, docFieldBaseline)
	sort.Slice(unlisted, func(i, j int) bool {
		if unlisted[i].file != unlisted[j].file {
			return unlisted[i].file < unlisted[j].file
		}
		return unlisted[i].line < unlisted[j].line
	})
	for _, g := range unlisted {
		t.Errorf("%s:%d: %s has no doc comment and is not in docFieldBaseline (key %q); "+
			"document it (a comment above it or a trailing line comment)",
			g.file, g.line, g.what, fieldBaselineKey(g))
	}
	for _, key := range staleFields {
		t.Errorf("docFieldBaseline entry %q matches no undocumented field; delete that entry", key)
	}

	if len(failing) > 0 {
		sort.Slice(failing, func(i, j int) bool {
			if failing[i].file != failing[j].file {
				return failing[i].file < failing[j].file
			}
			return failing[i].line < failing[j].line
		})
		var b strings.Builder
		b.WriteString("exported identifiers and packages without a doc comment:\n")
		for _, g := range failing {
			fmt.Fprintf(&b, "\t%s:%d: %s\n", g.file, g.line, g.what)
		}
		b.WriteString("\nAdd a comment directly above each, starting with its name, " +
			"saying what it does.")
		t.Errorf("%d missing doc comments\n%s", len(failing), b.String())
	}

	skipped := 0
	for _, n := range allowedHit {
		skipped += n
	}
	t.Logf("checked %d files; %d gaps in %d allowlisted packages not enforced; "+
		"%d grandfathered struct fields in docFieldBaseline",
		scanned, skipped, len(allowedHit), len(docFieldBaseline))
}

// parsedFile is one parsed source file: its repository-relative path, the
// file set its positions resolve against, and its syntax tree.
type parsedFile struct {
	rel  string
	fset *token.FileSet
	file *ast.File
}

// packageDocGaps reports, once per package directory, a package none of
// whose files carries a package comment, at the package line of the first
// file seen in that directory.
func packageDocGaps(files []parsedFile) []docGap {
	// pkgDoc records, per package directory, whether any file carries a
	// package comment; pkgFirst keeps a position to report against.
	pkgDoc := map[string]bool{}
	pkgFirst := map[string]docGap{}
	var dirs []string
	for _, f := range files {
		dir := filepath.ToSlash(filepath.Dir(f.rel))
		if _, seen := pkgDoc[dir]; !seen {
			pkgDoc[dir] = false
			pkgFirst[dir] = docGap{file: f.rel, line: f.fset.Position(f.file.Package).Line}
			dirs = append(dirs, dir)
		}
		if f.file.Doc != nil {
			pkgDoc[dir] = true
		}
	}
	var gaps []docGap
	for _, dir := range dirs {
		if !pkgDoc[dir] {
			first := pkgFirst[dir]
			gaps = append(gaps, docGap{pkgDir: dir, file: first.file, line: first.line, what: "package " + dir})
		}
	}
	return gaps
}

// fieldBaselineKey returns the docFieldBaseline key of a struct field gap:
// "<repo-relative file>: <Type>.<Field>", without the line number.
func fieldBaselineKey(g docGap) string {
	return g.file + ": " + strings.TrimPrefix(g.what, "field ")
}

// fieldBaselineViolations compares struct field gaps against the baseline
// set by name. A gap whose key is not in baseline is unlisted (a new
// undocumented field); a baseline entry no gap matches is stale (the field was
// documented or removed, so the entry must go). stale is sorted.
func fieldBaselineViolations(gaps []docGap, baseline map[string]bool) (unlisted []docGap, stale []string) {
	hit := map[string]bool{}
	for _, g := range gaps {
		key := fieldBaselineKey(g)
		if baseline[key] {
			hit[key] = true
			continue
		}
		unlisted = append(unlisted, g)
	}
	for key := range baseline {
		if !hit[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	return unlisted, stale
}

// undocumentedDecls returns the exported top-level declarations in file
// (repository-relative path rel) that carry no doc comment.
func undocumentedDecls(fset *token.FileSet, rel string, file *ast.File) []docGap {
	var gaps []docGap
	add := func(pos token.Pos, what string) {
		gaps = append(gaps, docGap{file: rel, line: fset.Position(pos).Line, what: what})
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if !d.Name.IsExported() || d.Doc != nil {
				continue
			}
			if d.Recv == nil {
				add(d.Pos(), "func "+d.Name.Name)
				continue
			}
			recv := receiverTypeName(d.Recv)
			if recv == "" || !ast.IsExported(recv) {
				continue
			}
			add(d.Pos(), "method "+recv+"."+d.Name.Name)
		case *ast.GenDecl:
			if d.Tok == token.IMPORT {
				continue
			}
			// Each exported method of an exported interface needs its
			// own doc, whatever the type's or the group's doc.
			if d.Tok == token.TYPE {
				for _, spec := range d.Specs {
					s, ok := spec.(*ast.TypeSpec)
					if !ok || !s.Name.IsExported() {
						continue
					}
					// Each exported, named field of an exported struct
					// needs a doc or a trailing line comment, as godoc
					// renders either. Embedded fields are skipped, and
					// fields of a nested anonymous struct type inside a
					// field are out of scope.
					if st, ok := s.Type.(*ast.StructType); ok && st.Fields != nil {
						for _, field := range st.Fields.List {
							if field.Doc != nil || field.Comment != nil {
								continue
							}
							for _, n := range field.Names {
								if n.IsExported() {
									add(n.Pos(), "field "+s.Name.Name+"."+n.Name)
								}
							}
						}
						continue
					}
					it, ok := s.Type.(*ast.InterfaceType)
					if !ok || it.Methods == nil {
						continue
					}
					for _, field := range it.Methods.List {
						if field.Doc != nil {
							continue
						}
						for _, n := range field.Names {
							if n.IsExported() {
								add(n.Pos(), "method "+s.Name.Name+"."+n.Name)
							}
						}
					}
				}
			}
			// A group doc covers a const or var group and a standalone
			// type; each type in a grouped type block needs its own.
			grouped := d.Tok == token.TYPE && d.Lparen.IsValid()
			if d.Doc != nil && !grouped {
				continue
			}
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if s.Name.IsExported() && s.Doc == nil {
						add(s.Pos(), "type "+s.Name.Name)
					}
				case *ast.ValueSpec:
					if s.Doc != nil {
						continue
					}
					for _, n := range s.Names {
						if n.IsExported() {
							add(n.Pos(), d.Tok.String()+" "+n.Name)
						}
					}
				}
			}
		}
	}
	return gaps
}

// TestUndocumentedDeclsGroupedTypes proves a doc comment on a grouped type
// block does not document the types inside it, while a doc comment on a
// const or var group, or on a standalone type, still covers its names.
func TestUndocumentedDeclsGroupedTypes(t *testing.T) {
	const src = `// Package fixture is a doc comment gate fixture.
package fixture

// Public types.
type (
	Foo struct{}
	Bar struct{}

	// Baz is documented.
	Baz struct{}
)

// Single is a standalone type.
type Single struct{}

// Limits of the fixture.
const (
	MaxA = 1
	MaxB = 2
)

// Defaults of the fixture.
var (
	DefA = 1
	DefB = 2
)
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing fixture: %v", err)
	}
	var got []string
	for _, g := range undocumentedDecls(fset, "fixture.go", file) {
		got = append(got, g.what)
	}
	want := []string{"type Foo", "type Bar"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("undocumentedDecls reported %q, want %q", got, want)
	}
}

// TestUndocumentedDeclsInterfaceMethods pins the interface method rule: each
// exported method of an exported interface needs its own doc comment, whether
// the interface is standalone or in a grouped type block, while embedded
// interfaces, unexported methods and unexported interfaces are not checked.
func TestUndocumentedDeclsInterfaceMethods(t *testing.T) {
	const src = `// Package fixture is a doc comment gate fixture.
package fixture

// Store is a documented standalone interface.
type Store interface {
	Get() int

	// Put is documented.
	Put(int)

	reset()

	Embedded
}

// Grouped types.
type (
	// Reader is documented.
	Reader interface {
		Read() int
	}
)

// hidden is unexported.
type hidden interface {
	Close() error
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing fixture: %v", err)
	}
	var got []string
	for _, g := range undocumentedDecls(fset, "fixture.go", file) {
		got = append(got, g.what)
	}
	want := []string{"method Store.Get", "method Reader.Read"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("undocumentedDecls reported %q, want %q", got, want)
	}
}

// TestUndocumentedDeclsStructFields checks that each exported, named field
// of an exported struct needs a doc or a trailing line comment, in a
// standalone type and in a grouped type block, while unexported fields,
// embedded fields and fields of an unexported struct are not reported.
func TestUndocumentedDeclsStructFields(t *testing.T) {
	const src = `// Package fixture is a doc comment gate fixture.
package fixture

// Info is a documented standalone struct.
type Info struct {
	ShardID uint64

	// ReplicaID is documented.
	ReplicaID uint64

	Term uint64 // Term is documented by a trailing comment.

	count int

	Embedded
}

// Grouped types.
type (
	// Entry is documented.
	Entry struct {
		Index uint64
	}
)

// hidden is unexported.
type hidden struct {
	Size int
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing fixture: %v", err)
	}
	var got []string
	for _, g := range undocumentedDecls(fset, "fixture.go", file) {
		got = append(got, g.what)
	}
	want := []string{"field Info.ShardID", "field Entry.Index"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("undocumentedDecls reported %q, want %q", got, want)
	}
}

// TestFieldBaselineByIdentifier checks that the field baseline is matched by
// file and field name, so swapping one grandfathered field for a new
// undocumented one in the same file fails, and so does a stale entry.
func TestFieldBaselineByIdentifier(t *testing.T) {
	gap := func(file, field string) docGap {
		return docGap{file: file, line: 1, what: "field " + field}
	}
	cases := []struct {
		name         string
		baseline     map[string]bool
		gaps         []docGap
		wantUnlisted []string
		wantStale    []string
	}{
		{
			name:         "same count, different field",
			baseline:     map[string]bool{"a.go: T.X": true},
			gaps:         []docGap{gap("a.go", "T.Y")},
			wantUnlisted: []string{"a.go: T.Y"},
			wantStale:    []string{"a.go: T.X"},
		},
		{
			name:     "listed field",
			baseline: map[string]bool{"a.go: T.X": true},
			gaps:     []docGap{gap("a.go", "T.X")},
		},
		{
			name:         "same field in another file",
			baseline:     map[string]bool{"a.go: T.X": true},
			gaps:         []docGap{gap("a.go", "T.X"), gap("b.go", "T.X")},
			wantUnlisted: []string{"b.go: T.X"},
		},
		{
			name:     "empty baseline, no gaps",
			baseline: map[string]bool{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			unlisted, stale := fieldBaselineViolations(tc.gaps, tc.baseline)
			var got []string
			for _, g := range unlisted {
				got = append(got, fieldBaselineKey(g))
			}
			if strings.Join(got, ",") != strings.Join(tc.wantUnlisted, ",") {
				t.Errorf("unlisted = %q, want %q", got, tc.wantUnlisted)
			}
			if strings.Join(stale, ",") != strings.Join(tc.wantStale, ",") {
				t.Errorf("stale = %q, want %q", stale, tc.wantStale)
			}
		})
	}
}

// receiverTypeName returns the base type name of a method receiver, looking
// through pointers and type parameters.
func receiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	expr := recv.List[0].Type
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}
