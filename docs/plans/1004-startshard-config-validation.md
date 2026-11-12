# Plan: #1004 Host.StartShard refuses an invalid shard Config

Test gap for the v1.0.0 bar (tracked under #744).

`Host.StartShard` (pkg/host.go) calls `cfg.Validate()` and then
`cfg.ValidateElectionTimeout(h.cfg.RTTMillisecond)` before it registers the
shard. Both return a `*config.ValidationError` naming the offending field.

New file `pkg/host_startshard_config_test.go` (package quicraft), every test
name containing `StartShardConfig`, each case on a fresh shard ID:

- (a) `ElectionRTT` whose product with the host `RTTMillisecond` overflows
  `time.Duration` returns a `*config.ValidationError` with Field `ElectionRTT`.
- (b) `ElectionRTT` below `2*HeartbeatRTT` (Field `ElectionRTT`),
  `IsObserver` with `IsWitness` (Field `IsObserver`), and a relative
  `DataDir` (Field `DataDir`) each return a `*config.ValidationError`.
- (c) after each refusal the shard is absent from `ShardInfo` and
  `HealthCheck`, and `SyncPropose` returns `ErrShardNotFound`.
- (d) the same shard ID then starts with the corrected config and accepts a
  proposal.

A divergence between code and documentation is a bug: a failing test commit,
then a fix commit in pkg/host.go.
