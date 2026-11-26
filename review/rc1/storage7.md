Reviewed at e616cd74, packages: pkg/logdb, contrib/dragonboat

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|

pkg/logdb: reviewed waldb/db.go reconcileCompactionFloors, discardSegmentsAbove, truncateCorruptSegment, truncateOrphanToZero, truncateClosedSegment, purgeRetired, refuseRetired, refuseRetiredBatch (with applyDurableFloors, RemoveNodeData and the refuseRetired call sites); no finding
