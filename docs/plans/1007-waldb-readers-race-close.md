# Plan: waldb readers racing Close (#1007)

Test gap under the v1.0.0 bar (tracked under #744). `TestCloseConcurrentWithWriters`
(pkg/logdb/waldb/gap_hardening_test.go) covers writers racing `Close`; nothing covers
the read paths.

## Scope

New file `pkg/logdb/waldb/close_readers_race_test.go` (package waldb). Readers under test:
`IterateEntries`, `ReadState`, `EntryRange`, `RemoveEntriesTo`, `GetSnapshot`,
`NodeRemoved`, `ListNodeInfo`, `GetBootstrap`, `GetEntries`, `CompactedTo`.

## Assertions

1. Four shards each hold entries, a hard state, a snapshot and a bootstrap. One
   goroutine per reader loops over the shards while `Close` runs; every call returns
   either data equal to what was saved or an error matching `logdb.ErrClosed`.
2. No goroutine panics, and all of them return before a context deadline.
3. A second `Close` returns nil, and each reader called after `Close` returns
   `logdb.ErrClosed`.

## Bugs

If the code diverges from its documentation: a failing test commit, then a fix commit
in pkg/logdb/waldb/db.go. A wrong doc comment over correct code is corrected in place.
