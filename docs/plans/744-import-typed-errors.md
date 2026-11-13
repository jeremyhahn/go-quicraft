# 744: ImportSnapshot typed refusals

`pkg/host_import_typed_errors_test.go` adds `TestImportSnapshot_TypedRefusals`, which pins the typed
contract of the three ImportSnapshot refusals. It reuses the existing helpers
(`testHostConfig`, `newMockLogDB`, `buildSnapshotArtifact`, `snapshotEnvelope`,
`testShardConfig`, `testCreateFunc`, `newLiveImportFixture`, `importArtifact`) and does not copy them.

- **layout unknown**: there is no bootstrap record and no WithImportDataDir. The test checks for
  `*ImportLayoutUnknownError` with the imported ShardID and ReplicaID (two distinct values), and that
  the error Is only `ErrImportLayoutUnknown`. Error() must name the shard, the replica and
  WithImportDataDir.
- **DataDir mismatch**: the shard is started with DataDir A, then imported with
  `WithImportDataDir(B)`, where A and B are distinct and non-empty. The test checks for
  `*ImportDataDirMismatchError` with Declared=B and Recorded=A, and that the error Is only
  `ErrImportDataDirMismatch`. Error() must quote Recorded and then Declared, in the documented order.
- **out of date**: the live fixture imports index 900, then index 700. The test checks for
  `*ImportSnapshotOutOfDateError` with Index=700 and Recorded=900, and that the error Is only
  `logdb.ErrSnapshotOutOfDate`. Error() must name the recorded index and then the importing index.

Mutation check: swapping Declared and Recorded at host.go resolveImportDataDir makes the
mismatch subtest fail. The swap was not committed.
