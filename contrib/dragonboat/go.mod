module github.com/jeremyhahn/go-quicraft/contrib/dragonboat

// Keep this in step with the parent's go directive. The replace below builds
// against the parent tree, and Go fails the build rather than upgrading when
// the parent requires a newer Go than this line.
go 1.27.3

// The release recipe (make release-alpha / release-candidate / release) pins
// this require to the version it cuts and tags contrib/dragonboat/<version>
// beside the root tag, so importers resolve the parent at that version.
require github.com/jeremyhahn/go-quicraft v0.24.2-alpha

// In-tree builds use the parent tree this module ships with. A consumer's
// build ignores this replace and follows the require above.
replace github.com/jeremyhahn/go-quicraft => ../..

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/jeremyhahn/go-enclave v0.11.1-alpha // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/quic-go/quic-go v0.64.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
