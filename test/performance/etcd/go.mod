module github.com/jeremyhahn/go-quicraft/test/performance/etcd

go 1.27.0

// The perf module is unpublished; resolve it from the sibling directory so the
// benchmark builds standalone in Docker (without the external go.work).
replace github.com/jeremyhahn/go-quicraft/test/performance => ../

require (
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0

	// DOCUMENTED PIN (devops.md §9 — "a require held below latest must carry a
	// comment naming the reason"). Latest is v3.7.0; this benchmark stays on
	// v3.6.0 because v3.7.0 is an API break, not a version bump: raftpb moved
	// off gogo-protobuf to pointer-valued generated types, so Node.Step now
	// takes *raftpb.Message, Ready carries []*raftpb.Entry / *raftpb.HardState /
	// []*raftpb.Message, EntryType is *raftpb.EntryType in a type switch, and
	// the hand-rolled Marshal/Unmarshal methods this benchmark calls are gone.
	//
	// Adopting it means rewriting cluster.go and etcd_bench.go, which changes
	// what the etcd comparison baseline measures — a benchmark-correctness task
	// with its own before/after numbers, not something to fold into a
	// dependency-currency sweep. Tracked as #127; unpin when that lands.
	go.etcd.io/raft/v3 v3.6.0
)

require (
	github.com/cockroachdb/datadriven v1.0.3-0.20250407164829-2945557346d5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
