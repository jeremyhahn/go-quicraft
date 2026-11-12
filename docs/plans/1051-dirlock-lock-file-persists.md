# Plan: #1051 fsutil.DirLock lock file persistence

Test gap under the v1.0.0 bar (#744). `pkg/fsutil/lock.go` documents that the
lock file is never removed, that `LockDir` creates it only when absent, and
that the lock is per open file description. No test pins these.

## Change

New `pkg/fsutil/lock_file_test.go` (`//go:build unix || windows`):

- (a) after `Close` returns nil, the lock file still exists.
- (b) a relock in the same process after `Close` succeeds and locks the same
  file (`os.SameFile`), and a third `LockDir` while it is held fails with
  `ErrDirLocked`.
- (c) a pre-existing lock file (content "x", mode 0640) keeps its content and,
  on unix, its mode across `LockDir` + `Close`.

A divergence between code and documentation is a bug: a failing test commit,
then a fix commit in `pkg/fsutil/lock.go`.

## Acceptance

`go test -race -count=3 -run LockFile ./pkg/fsutil/`, `go vet ./pkg/fsutil/`,
`make lint` clean.
