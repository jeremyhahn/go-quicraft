# contrib/dragonboat

> **Status: REFERENCE** · Scope: the Dragonboat-compatible shim over QuicRaft —
> this directory's `nodehost.go`, `errors.go`, `membership.go`,
> `requeststate.go` · Last verified: 2026-09-15

This is a [Dragonboat](https://github.com/lni/dragonboat)-compatible shim for
[QuicRaft](../../README.md): a `NodeHost` that presents Dragonboat's method
names over a `quicraft.Host`, so code written against Dragonboat's API can run
on QuicRaft's engine and QUIC transport with the call sites unchanged. No
[pebble](https://github.com/cockroachdb/pebble) dependency.

## What it is, and is not

- **A nested Go module** (`github.com/jeremyhahn/go-quicraft/contrib/dragonboat`)
  that `replace`s the root with `../..`. It is **not published for consumption**:
  no tag carries it and no consumer imports it — go-qrdb and the other
  consumers use `pkg` directly. It exists to keep the Dragonboat surface honest
  and to drive the performance comparison (`test/performance/dragonboat`).
- **The surface**: `StartReplica`/`StartOnDiskReplica`/`StopReplica`/`Close`;
  `SyncPropose`/`Propose`; `SyncRead`/`ReadIndex`/`StaleRead`/`NAReadLocalNode`;
  sessions (`GetNoOPSession`, `SyncGetSession`, `SyncCloseSession`); membership
  (`SyncGetShardMembership`, `GetLeaderID`, `SyncRequestAddReplica`,
  `SyncRequestAddNonVoting`, `SyncRequestAddWitness`,
  `SyncRequestDeleteReplica`, `RequestAddReplica`, `RequestDeleteReplica`);
  `SyncRequestSnapshot`; `SyncRemoveData`.
- **Errors** are translated to Dragonboat's sentinels by `translateError`, and
  QuicRaft's `*MaybeAppliedError` wrapper survives the translation (#156), so a
  post-delivery leadership change and a pre-flight refusal do not both arrive
  as a bare `ErrShardNotReady`. `dragonboat.IsTempError` is the retry
  predicate the performance harness uses, and it classifies exactly the
  transient set.
- **Ten `Get`-prefixed names** on `quicraft.Host` exist for this shim's sake and
  are a recorded exception to the portfolio naming rule; see
  `docs/multi-group-host/api.md`.

## Testing

CI runs this module's unit and integration suites in the
`contrib/dragonboat [unit + integration]` job, and `make ci-modules` builds
every nested module so a root change that breaks the shim cannot land quietly
(`go` directive drift is the case that motivated it). The adapter suite routes
its config changes and leader-bound reads through leader-resolving helpers,
because a shim that retries against a stale leader hides the very forwarding it
is meant to exercise.

See the per-machine [QuicRaft vs Dragonboat](../../test/performance/profiles/ci-node-2/dragonboat.md) <!-- lab-string-ok: benchmark provenance: the profile directory names the machine that measured the numbers -->
document for performance comparisons; other machines are under
`test/performance/profiles/`.
