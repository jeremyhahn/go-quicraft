# Plan: waldb refuses an oversized membership (#1001)

Test gap under the v1.0.0 bar (#744).

`pkg/logdb/waldb/db.go` `checkMembershipSize` caps every membership map
(Addresses, Observers, Witnesses, Removed, Identities, RemovedIdentities) at
`maxMembershipMapEntries` (10000); `writeSnapshot` calls it, and
`writeBootstrap` refuses more than 10000 addresses or a DataDir longer than
`maxBootstrapDataDirLen` (4096). Replay treats a record past either cap as
corruption, so the write side must refuse it with `ErrMembershipTooLarge` and
write nothing. Only Addresses through SaveSnapshot is covered today.

## Tests (pkg/logdb/waldb/membership_size_test.go)

1. For each of Observers, Witnesses, Removed, Identities and
   RemovedIdentities at 10001 entries: `SaveSnapshot` and `SaveState` return
   an error matching `ErrMembershipTooLarge`.
2. `SaveBootstrap` with 10001 addresses, and with a 4097-byte DataDir,
   returns `ErrMembershipTooLarge`.
3. After each refusal `GetSnapshot` / `GetBootstrap` return the record saved
   before it, also after Close and reopen.
4. Every map at exactly 10000 entries and a 4096-byte DataDir save and read
   back equal after a reopen.

A divergence between code and documentation is a bug: a failing test commit,
then a fix commit in `db.go`.
