module github.com/jeremyhahn/go-quicraft/test/performance/etcd

go 1.27.1

// The perf module is unpublished; resolve it from the sibling directory so the
// benchmark builds standalone in Docker.
replace github.com/jeremyhahn/go-quicraft/test/performance => ../

replace github.com/jeremyhahn/go-quicraft => ../../..

require (
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0
	go.etcd.io/raft/v3 v3.7.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cockroachdb/datadriven v1.0.3 // indirect
	// go-quicraft is here only for test/netport, which benchutil uses for port
	// reservation. A replace binds the main module only, so the pair repeats
	// in every module that reaches benchutil. MVS reads go-quicraft's whole
	// requirement list, so its floors can raise this module's versions.
	github.com/jeremyhahn/go-quicraft v0.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
)
