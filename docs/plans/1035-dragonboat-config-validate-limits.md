# Plan: #1035 dragonboat Config.Validate limits

Test gap under #744 (v1.0.0 bar).

`contrib/dragonboat/config/config.go` `Config.Validate` refuses:

- `MaxInMemLogSize` > 0 and < `entryNonCmdFieldsSize`+1 (129) with Field `MaxInMemLogSize`;
- an unknown `SnapshotCompressionType` with Field `SnapshotCompressionType`;
- an unknown `EntryCompressionType` with Field `EntryCompressionType`.

`TestConfig_Validate_RejectsInvalid` stops before these checks.

## Change

New `contrib/dragonboat/config/config_validate_limits_test.go`, from a valid
base config (ShardID 1, ReplicaID 1, HeartbeatRTT 1, ElectionRTT 10):

- (a) `MaxInMemLogSize` 1 and 128 return `*ValidationError` Field `MaxInMemLogSize`; 129 and 0 return nil;
- (b) `SnapshotCompressionType: CompressionType(99)` returns Field `SnapshotCompressionType`;
- (c) `EntryCompressionType: CompressionType(99)` with a valid snapshot type returns Field `EntryCompressionType`;
- (d) `NoCompression` and `Snappy` in each field return nil.

A divergence between code and doc is a bug: failing test commit, then fix in `config.go`.

## Acceptance

`cd contrib/dragonboat && go test -race -count=1 -run ValidateLimits ./config/`,
`go vet ./config/`, `make lint` clean.
