Reviewed at e616cd74, packages: pkg/logdb, pkg/fsutil, contrib/dragonboat

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|

pkg/logdb: reviewed waldb/db.go (Open, openWALShard, membership map decoding, validateBatchLocked, saveUpdate, writeEntries, maxRecordPayload, pendingMemUpdates.applyTo, ensureActiveSegment, rotateSegment, syncSegment, syncDirtyShardsLocked, checkpoint capture, IterateEntries, EntryRange, GetEntries, SaveEntries, SaveSnapshot, saveBootstrap, GetBootstrap, RemoveNodeData, reclaimSegmentsLocked, NodeRemoved, CompactedTo, GetState, walShard.close, writeGate, poison, dropNodeLocked, verifyBindingUnchanged) and memdb/memdb.go (beginWrite, withNode, applyUpdate, copyMembership, mergeEntries, IterateEntries, EntryRange, SaveSnapshot, RemoveNodeData, SaveBootstrap, GetBootstrap, readNode, getOrCreateNode); not reached waldb/db.go reconcileCompactionFloors, discardSegmentsAbove, truncate* helpers, purgeRetired, refuseRetired*
