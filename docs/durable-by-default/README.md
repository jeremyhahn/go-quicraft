# durable-by-default

With the default `HostConfig` and the default write mode, a write is
acknowledged only after a quorum of the shard's voting members has written it
to the WAL with `fdatasync`. Nothing has to be set to get this behaviour.
Every setting that acknowledges earlier is opt-in, and you have to name it (#1303).

## What the default gives

- `HostConfig.NoSyncWAL` is `false`, so every WAL segment write is fsynced
  before the replica reports the entry as persisted.
- The zero `writemode.Mode` is `Sync`, and `writemode.FromString("")` parses
  an unset mode to `Sync`. `Sync` (through `Host.SyncPropose`) acknowledges
  after quorum commit. Raft commits an entry only once a quorum has persisted
  it.
- `HostConfig.NotifyCommit` is `false`, so `RequestState.CommittedC()` does
  not fire on local WAL persistence alone.

## Opt-in settings and what each gives up

| Setting | Acknowledged after | Gives up |
|---|---|---|
| `HostConfig.NoSyncWAL = true` | the WAL write reaches the OS page cache, with no fsync | Committed entries can be lost on power loss or an OS crash. `Validate` refuses it with `ErrNoSyncWALOutsideTest` outside a `go test` binary, so a production process cannot enable it. |
| `HostConfig.NotifyCommit = true` | `CommittedC()` fires after the local WAL persist (~1-2 ms) | A caller that treats `CommittedC` as the acknowledgement may see a write that a quorum has not yet stored, and a leader change can lose it. Use `ApplyResultC()` or `SyncPropose` for quorum durability. |
| `writemode.WALDurable` (aliases `wal_durable`, `wal`, `async`) | local WAL persistence (`Host.Propose` plus `CommittedC()`, which needs `NotifyCommit`) | The same as `NotifyCommit`: there is no quorum before the acknowledgement. |

`writemode.Batched` is not opt-in. Each write in a batch is acknowledged only
after a quorum has committed the batch that carries it.
[write-batching/architecture.md](../write-batching/architecture.md) describes
the modes.

## Tests

The row is in progress. The tests that enforce these defaults were part of
#1306. They were reverted with it after main went red, and they come back with
the re-land of #1303:

- A `HostConfig` test that fails if the zero `HostConfig`, or the
  `HostConfig` after `SetDefaults`, enables a setting that weakens durability.
  It also fails if any `bool` reachable from `HostConfig` has not been
  classified as weakening or neutral.
- A `writemode` test that pins the zero and unset mode to `Sync`. It also
  fails if a mode or alias that skips the quorum is not in the opt-in lists.

When you add an opt-in setting, add it to the table above.
