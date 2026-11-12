# Plan: #1039 MemDB retired-replica batch and never-stored retirement

Test gap under #744 (v1.0.0 bar).

## Contract

- `logdb.LogDB.RemoveNodeData` retires the (shard, replica) pair: every later
  write naming it is refused with `ErrReplicaRetired`, and `NodeRemoved`
  reports it, whether or not the pair was ever stored.
- `MemDB.SaveState` validates the whole batch before applying any of it, so a
  batch with one retired update stores nothing.

## Tests (pkg/logdb/memdb/retired_batch_test.go)

1. `RemoveNodeData(2, 1)`, then `SaveState` with (1,1) state+entry first and
   (2,1) state second: `ErrReplicaRetired`; (1,1) has no state, no entries and
   is not listed by `ListNodeInfo`.
2. Fresh `New()`: `RemoveNodeData(99, 99)` is nil, `NodeRemoved(99, 99)` is
   (true, nil); `SaveState`, `SaveSnapshot` and `SaveBootstrap` for (99,99)
   each return `ErrReplicaRetired`.
3. `ListNodeInfo` stays empty after 2.

A divergence between code and doc is a bug: failing test commit, then a fix
commit in memdb.go.
