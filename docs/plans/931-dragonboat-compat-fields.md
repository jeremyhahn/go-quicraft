# Plan: #931 dragonboat NodeHostConfig compatibility-only fields

## Objective

`contrib/dragonboat/config/nodehost_config.go` documents `EnableMetrics`,
`MaxSendQueueSize`, `MaxReceiveQueueSize`, `DefaultNodeRegistryEnabled`,
`Expert.Engine.CommitShards`, `SnapshotShards`, `CloseShards` and
`Expert.LogDB.Shards` as accepted and ignored by `ToQuicraft`, and says
`Validate` rejects a non-empty `GossipConfig`. None of this has a test.

## Slices

1. Test commit: `contrib/dragonboat/config/compat_fields_test.go`
   - `TestCompatFields_ToQuicraftIgnoresCompatibilityOnlyFields`: one
     sub-test per compatibility-only field plus one with all of them set;
     each `ToQuicraft` result is `reflect.DeepEqual` to the result from the
     same base config without the field, and `Validate` accepts it.
   - `TestCompatFields_ValidateRefusesAnyGossipField`: one sub-test per
     `GossipConfig` field (`BindAddress`, `AdvertiseAddress`, `Seed`,
     `Meta`); `Validate` returns a `*ValidationError` with `Field == "Gossip"`.
2. Per divergence from the docs: a failing test commit, then a fix commit in
   `nodehost_config.go`. Expected: `GossipConfig.IsEmpty` ignores `Meta`, so
   a Meta-only `GossipConfig` passes `Validate`, contrary to the `Gossip`
   field doc; fix `IsEmpty` to check `len(g.Meta) == 0`.

## Out of scope

- Any other dragonboat adapter field or package; wire or on-disk formats.
