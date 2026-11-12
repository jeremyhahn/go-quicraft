# #947: SetDefaults and MaxSessionsPerOwner

## Objective

Pin `Config.SetDefaults` (pkg/config/config.go:709-711) for
`MaxSessionsPerOwner`: zero becomes `DefaultMaxSessionsPerOwner` (128), an
explicit cap or `SessionsPerOwnerUnlimited` (`math.MaxUint64`) is kept.

## Slices

1. This plan.
2. pkg/config/sessions_per_owner_test.go (package config), on a config that
   otherwise passes `Validate`:
   - (a) 0 becomes `DefaultMaxSessionsPerOwner`, which equals 128;
   - (b) explicit 1, 7 and `MaxTotalSessions`+1 are kept;
   - (c) `SessionsPerOwnerUnlimited` is kept and equals `math.MaxUint64`;
   - (d) `Validate` returns nil for each after `SetDefaults`;
   - (e) a second `SetDefaults` changes nothing.
3. Per divergence from the documentation: a failing test commit, then a fix
   commit in pkg/config/config.go (or a doc correction if the code is right).

## Out of scope

Enforcement of the cap in the session manager; any other Config field;
wire or on-disk formats.
