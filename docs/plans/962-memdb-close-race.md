# Plan: #962 MemDB readers and writers racing Close

Objective: prove that `memdb.MemDB`, documented as safe for concurrent use
with an idempotent `Close` after which every operation returns `ErrClosed`,
keeps that contract when many readers and writers on many shards race two
`Close` calls.

## Slices, in order

1. This plan.
2. `pkg/logdb/memdb/close_race_test.go`, `TestCloseRace*`:
   - (a) 8 shards pre-loaded with entries 1-10, a state and a snapshot; 16
     goroutines (a writer and a reader per shard) loop `SaveState` (entries
     plus state), `SaveSnapshot`, `RemoveEntriesTo`, `IterateEntries`,
     `ReadState`, `EntryRange`, `GetSnapshot` and `ListNodeInfo` until they
     see `ErrClosed`, while two goroutines call `Close` after a short start.
     Every error is nil or `ErrClosed`; a panic is recovered and fails.
   - (b) after both `Close` calls return nil, every method returns
     `ErrClosed`.
   - (c) a `SaveState` that returned nil is whole: a reader that read
     `EntryRange` after that nil and before `ErrClosed` saw the batch's last
     index.
   Run with `-race -count=3`.
3. Per divergence from the docs: a failing test commit, then a fix commit in
   `pkg/logdb/memdb/memdb.go`.

## Suspected divergence

`Close`'s comment says no operation can observe the released map because
every exported method checks the closed flag before it reaches the nodes. A
reader checks the flag and then resolves its node without holding a lock that
orders it against `Close`, so a reader that passes the check just before
`Close` finds an empty map and reports `ErrUnavailable`, `ErrNoState`,
`ErrNoSnapshot`, an empty `EntryRange` or an empty `ListNodeInfo` rather than
`ErrClosed`. Fix: re-check the flag under `mu` where a reader resolves its
node (Close sets the flag before it takes `mu` to release the map).

## Out of scope

waldb and other backends; any change to the on-disk or wire format; any
behaviour change outside the close race.
