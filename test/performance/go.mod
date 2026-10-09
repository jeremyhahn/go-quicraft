module github.com/jeremyhahn/go-quicraft/test/performance

go 1.27.2

// benchutil's port reservation comes from test/netport. The harnesses are
// always built against the parent tree they ship with, never a published
// version.
//
// MVS reads go-quicraft's whole requirement list, not just the stdlib-only
// package imported, so this raises version floors in every module that
// requires this one.
require github.com/jeremyhahn/go-quicraft v0.0.0

replace github.com/jeremyhahn/go-quicraft => ../..
