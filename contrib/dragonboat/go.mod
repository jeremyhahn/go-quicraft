module github.com/jeremyhahn/go-quicraft/contrib/dragonboat

// This directive must not fall behind the parent's. The replace below pins this
// module to the parent tree, and Go evaluates the language version from the MAIN
// module — so a parent requiring a newer Go than this line fails the build with
// "module ../.. requires go >= X (running go Y)" rather than upgrading. Keep the
// two in lockstep; CI builds every module in the repo so a one-sided bump is
// caught at the commit that makes it.
go 1.26.6

require github.com/jeremyhahn/go-quicraft v0.0.0

// This adapter lives in the go-quicraft repo and is always built against the
// parent tree it ships with, never a published version. Without this the
// module only resolves through a go.work that lives outside the repo, so it
// cannot be built or tested in CI.
replace github.com/jeremyhahn/go-quicraft => ../..

require (
	github.com/awnumar/memcall v0.5.0 // indirect
	github.com/awnumar/memguard v0.23.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.68.0 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/quic-go/quic-go v0.59.1 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
