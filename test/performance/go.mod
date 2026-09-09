module github.com/jeremyhahn/go-quicraft/test/performance

go 1.27.0

// benchutil's port reservation is test/netport's, not a copy of it (#163). The
// harnesses live in the go-quicraft repo and are always built against the
// parent tree they ship with, never a published version -- the same reason
// contrib/dragonboat and test/performance/quicraft carry this pair. Without it
// the module resolves only through a go.work outside the repo and cannot be
// built in CI.
//
// MVS reads go-quicraft's whole requirement list, not just the stdlib-only
// package actually imported, so this raises version floors in every module that
// requires this one. The notes in etcd/go.mod and dragonboat/go.mod record what
// moved in each.
require github.com/jeremyhahn/go-quicraft v0.0.0

replace github.com/jeremyhahn/go-quicraft => ../..
