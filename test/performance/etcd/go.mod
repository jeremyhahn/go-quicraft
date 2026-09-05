module github.com/jeremyhahn/go-quicraft/test/performance/etcd

go 1.27.0

// The perf module is unpublished; resolve it from the sibling directory so the
// benchmark builds standalone in Docker (without the external go.work).
replace github.com/jeremyhahn/go-quicraft/test/performance => ../

require (
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0
	go.etcd.io/raft/v3 v3.7.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cockroachdb/datadriven v1.0.3-0.20250407164829-2945557346d5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
)
