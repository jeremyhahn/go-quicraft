Reviewed at e616cd74, packages: pkg/logdb, pkg/fsutil, contrib/dragonboat, scripts, Makefile, .github/workflows

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|

pkg/logdb: reviewed waldb/format.go, waldb/reencrypt.go, waldb/retired.go, waldb/compaction_floor.go, waldb/diskmon.go, waldb/fs.go, waldb/sync_other.go, logdb.go, barrier_identity.go, db.go (Open, validateOpenOptions, establishWALFormat, walDirHoldsSegments, reconcileCompactionFloors, unmarshalRemovedMap, replayBootstrap, reserveLanding, writeRecord, writeEntries, writeEntryChunk, maxRecordPayload, ReadState, RemoveEntriesTo, Compact, RemoveNodeData, retireNodes), memdb/memdb.go (SaveState, withNode, applyUpdate, mergeEntries, RemoveEntriesTo); not reached waldb/repair.go (beyond the discard/truncate order), waldb/diskmon_linux.go, waldb/diskmon_other.go, waldb/copy.go, waldb/options.go, waldb/errors.go, types.go, errors.go, the rest of db.go and memdb.go
