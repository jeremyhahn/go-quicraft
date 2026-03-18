module github.com/jeremyhahn/go-quicraft/test/performance/etcd

go 1.26.1

require (
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0
	go.etcd.io/raft/v3 v3.6.0
)

replace github.com/jeremyhahn/go-quicraft/test/performance => ../

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)
