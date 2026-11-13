# 744: MkdirStageDir contract tests

`MkdirStageDir` (pkg/internal/engine/snapshot_pool.go) creates every save,
receive and import staging directory. Its results are used by other tests, but
none pins the helper on its own. `TestMkdirStageDir` in
`pkg/internal/engine/snapshot_stage_dir_test.go` does, with one subtest per
property. Each subtest uses `t.TempDir()`. None calls `syscall.Umask`, which
is process-global.

**SaveStageDirPattern / RecvStageDirPattern.** For each pattern, the call
returns a directory directly under `parent`. Its base name is the pattern's
prefix followed only by decimal digits. The pattern's own matcher
(`IsSnapshotSaveStage` or `IsSnapshotRecvStage`) accepts the name and the
other matcher rejects it. This is the documented promise: a name that can be
created is, by construction, a name that can be reclaimed.

**SuffixKept.** The pattern `x-*.tmp` gives `x-<digits>.tmp`. Only the last
`*` is replaced, and the text after it is kept.

**Mode0750UnderUmask.** The stage directory's permission bits equal those of
a sibling made with `os.Mkdir(..., 0o750)` under the same umask. The test also
rejects a 0700 directory, the mode `os.MkdirTemp` would give, unless the umask
itself yields 0700.

**UniquePerCall.** 200 calls on one parent return 200 distinct paths. Each
path exists as a directory, and the parent holds exactly 200 entries, so no
call reused or removed another call's directory.

**MissingParent.** A parent that does not exist gives an error for which
`os.IsNotExist` is true and an empty path. Nothing is created: neither the
parent nor any entry in its own parent. This shows that only `os.IsExist` is
retried and that any other error is returned.

**InvisibleToListSnapshotDirs.** A shard/replica snapshot tree that holds only
save and receive stage directories lists nothing through `listSnapshotDirs`.
Garbage collection, recovery and the purge-floor scan therefore never treat a
stage directory as a snapshot.
