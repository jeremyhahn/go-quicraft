Reviewed at e616cd74, packages: pkg/logdb, pkg/fsutil, contrib/dragonboat, scripts, Makefile, .github/workflows

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|

pkg/logdb: reviewed waldb/record.go (decodeRecord), waldb/segment.go (ReadAll, Sync, writeSyncMark), waldb/db.go (SaveState, replaySegment, replayRecord, replayEntries, replayState, replayCompaction, unmarshalAddrMap, rotateSegment, discardSegmentsAbove, truncateCorruptSegment and their call order in recover and Repair, reclaimSegmentsLocked, addMetaSegmentsToLive, IterateEntries, syncDirFS), waldb/index.go, waldb/compaction.go, waldb/sync_linux.go, reader.go (getRangeLockedInto, setRangeLocked, termLocked), memdb/memdb.go (IterateEntries); not reached waldb/format.go, waldb/reencrypt.go, waldb/retired.go, waldb/repair.go (beyond the discard/truncate order), waldb/compaction_floor.go, waldb/diskmon*.go, waldb/fs.go, waldb/copy.go, waldb/options.go, waldb/errors.go, waldb/sync_other.go, logdb.go, types.go, errors.go, barrier_identity.go, the rest of db.go and memdb.go
