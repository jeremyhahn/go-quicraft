module github.com/jeremyhahn/go-quicraft/test/performance/etcd

go 1.27.0

// The perf module is unpublished; resolve it from the sibling directory so the
// benchmark builds standalone in Docker (without the external go.work).
replace github.com/jeremyhahn/go-quicraft/test/performance => ../

replace github.com/jeremyhahn/go-quicraft => ../../..

require (
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0
	go.etcd.io/raft/v3 v3.7.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cockroachdb/datadriven v1.0.3-0.20250407164829-2945557346d5 // indirect
	// go-quicraft is here only for test/netport, the one port-reservation
	// window benchutil delegates to. A `replace` binds the MAIN
	// module only, so the pair repeats in every module that reaches benchutil
	// rather than being inherited from test/performance.
	//
	// It is NOT free, and the cost is not the import. Only test/netport is
	// imported and it is stdlib-only, but MVS reads the required module's WHOLE
	// requirement list, so go-quicraft's own floors raise this module's
	// versions whether or not a line of that code is reached.
	//
	// Measured here: davecgh/go-spew drops out of the indirect set and nothing
	// in the runtime graph moves.
	github.com/jeremyhahn/go-quicraft v0.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
)
