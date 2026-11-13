# 744: NewCachedDiskStateMachine and GetHash typed errors

docs/state-machines/api.md:170 documents that `NewCachedDiskStateMachine`
returns `*CacheMaxEntriesError` when `MaxEntries` is not positive and
`*InvalidCacheConfigError` when `ExtractKV` is nil; api.md:178-180 documents
that `GetHash` returns `*InvalidCacheConfigError` when the inner store does not
implement `Hasher`; api.md:186-187 name the fields `Reason` and `Value`. The
existing tests only compare `Error()` strings or check `err != nil`. The new
file `pkg/sm/cached_errors_test.go` pins the typed contract with `errors.As`:
`MaxEntries` 0, -1 and `math.MinInt` give a `*CacheMaxEntriesError` whose
`Value` is the input and a nil machine; a nil `ExtractKV` gives
`*InvalidCacheConfigError` with `Reason` "ExtractKV must not be nil"; when both
are bad the `*CacheMaxEntriesError` wins; a valid config gives no error; and
`GetHash` on a non-`Hasher` inner store returns hash 0 and an
`*InvalidCacheConfigError`.
