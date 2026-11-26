Reviewed at e616cd74, packages: pkg/logdb, pkg/fsutil, contrib/dragonboat, scripts, Makefile, .github/workflows

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|

pkg/logdb: reviewed waldb/db.go writeState, writeSnapshot, writeBootstrap, saveBootstrap, writeRecord, ensureActiveSegment, Compact, RemoveEntriesTo, Close, ListNodeInfo; memdb/memdb.go ReadState, GetSnapshot, ListNodeInfo, RemoveNodeData, SaveBootstrap, GetBootstrap, readNode; not reached the rest of waldb and memdb
