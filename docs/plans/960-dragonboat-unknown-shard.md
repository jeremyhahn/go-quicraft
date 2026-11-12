# Plan: #960 dragonboat NodeHost calls on an unknown shard

## Objective

Pin down what the dragonboat adapter's NodeHost returns when a call names a
shard this host does not run. Dragonboat answers with `ErrShardNotFound`
(contrib/dragonboat/errors.go, mapped by `translateError`); today only
`requireNoOPSessionOnDisk` and a few single calls are checked.

## Slices

1. This plan.
2. `contrib/dragonboat/nodehost_unknown_shard_test.go`
   (`TestNodeHost_UnknownShard`): one NodeHost from `newTestNodeHost`, shard 1
   started with `startTestShard`, every call naming shard 99 with a valid
   deadline or timeout, in one table:
   - (a) `errors.Is(err, ErrShardNotFound)`: SyncPropose and Propose (with
     `GetNoOPSession(99)`), SyncRead, ReadIndex, StaleRead,
     GetShardMembership, SyncGetShardMembership, GetLeaderID,
     RequestLeaderTransfer, RequestSnapshot, SyncRequestSnapshot, StopShard,
     GetNodeUser.
   - (b) calls whose doc comment names another result:
     GetLogReader gives `ErrLogDBNotCreatedOrClosed`; GetShardInfo gives
     `(nil, false, nil)` ("false when it is not loaded on this host");
     HasNodeInfo gives false (no LogDB data for the pair);
     RequestCompaction succeeds with a completed SysOpState (its doc treats
     an unloaded shard as already retired via RemoveData).
   - (c) shard 1 still answers SyncPropose and SyncRead afterwards.
   Helpers take `context.Context` first, before `t *testing.T`.
3. Per divergence between code and doc comment: a failing `test:` commit,
   then a `fix:` commit in contrib/dragonboat/nodehost.go (or the doc comment
   corrected there when the code is right).

## Out of scope

- Any change outside contrib/dragonboat/nodehost.go and the new test file.
- Unknown-replica cases on a known shard; wire or on-disk formats.
