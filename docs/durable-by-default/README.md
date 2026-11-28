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

- `pkg/config/durable_default_test.go`
  - `TestHostConfig_DefaultIsQuorumDurable` fails if the zero
    `HostConfig`, the `HostConfig` after `SetDefaults`, or the tests'
    valid `HostConfig` enables a durability-weakening bool or carries a
    `writemode.Mode` that does not require a quorum.
  - `TestHostConfig_EveryBoolIsClassified` uses reflection to list every
    `bool` reachable from `HostConfig` through the package's own structs. It
    fails if any of them is missing from both `durabilityWeakeningFields`
    and `durabilityNeutralFields`. A new setting therefore cannot ship
    until someone has decided whether it weakens durability. A weakening
    setting must also default to off.
- `pkg/writemode/default_test.go`
  - `TestDefaultMode_IsQuorumDurable` pins the zero and unset mode.
  - `TestNonQuorumModes_AreOptIn` fails if a mode or alias that skips the
    quorum is not in the opt-in lists.
- `test/durable/durable_test.go` (`make integration-test-durable-by-default`)
  - `TestDefaultWriteAcknowledgedOnlyAfterQuorumFsync` starts three
    replicas with the default `HostConfig` and write mode, then holds the
    WAL fsync on both followers. The write is not acknowledged even though
    the leader has fsynced it and both followers are blocked in its fsync.
    When one follower's fsync is released, which makes a quorum of two, the
    write completes. The test acknowledges the write the way the default mode
    does, so it fails if the default becomes `writemode.WALDurable`.

When you add an opt-in setting, add it to the lists in those tests and to the
table above.
