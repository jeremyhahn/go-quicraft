# Plan: #959 dragonboat Sync calls refuse a missing or expired deadline

## Objective

Every `contrib/dragonboat` `NodeHost.Sync*` call that is guarded by
`checkContextDeadline` (nodehost.go) refuses `context.Background()` with
`ErrDeadlineNotSet` and an already-expired context with `ErrInvalidDeadline`,
before it looks up the shard, as dragonboat does.

The thirteen guarded calls: SyncPropose, SyncRead, SyncGetSession,
SyncCloseSession, SyncGetShardMembership, SyncRequestAddReplica,
SyncRequestAddReplicaWithIdentity, SyncRequestAddNonVotingWithIdentity,
SyncRequestAddWitnessWithIdentity, SyncRequestAddNonVoting,
SyncRequestAddWitness, SyncRequestDeleteReplica, SyncRequestSnapshot.

## Slices

1. This plan.
2. `contrib/dragonboat/nodehost_context_deadline_test.go`:
   `TestNodeHost_SyncCallsContextDeadline`, one table of the thirteen calls on
   a host from `newTestNodeHost` with shard 1 started by `startTestShard`:
   - (a) no deadline → `errors.Is(err, ErrDeadlineNotSet)`;
   - (b) deadline one second in the past → `errors.Is(err, ErrInvalidDeadline)`;
   - (c) (a) and (b) again on shard 99, which the host does not run;
   - (d) afterwards shard 1's membership is unchanged and a SyncPropose with a
     valid deadline still succeeds.
3. Per divergence from the docs: a failing test commit, then a fix commit in
   `contrib/dragonboat/nodehost.go`.

## Out of scope

- Sync calls without the guard (e.g. SyncRemoveData).
- Any change to wire or on-disk formats.
