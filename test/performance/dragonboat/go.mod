module github.com/jeremyhahn/go-quicraft/test/performance/dragonboat

go 1.27.0

require (
	github.com/jeremyhahn/dragonboat/v4 v4.0.1
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/VictoriaMetrics/metrics v1.18.1 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/pebble v1.1.5 // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/memberlist v0.3.1 // indirect
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
	// Measured here: klauspost/compress moves v1.16.0 -> v1.19.1, floored by the
	// parent's prometheus/client_golang. Pebble uses it for sstable zstd, so
	// the baseline's compression library follows the parent's floor; re-run
	// the profiles when it moves rather than compare across the change.
	github.com/jeremyhahn/go-quicraft v0.0.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lni/goutils v1.4.0 // indirect
	github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// The benchmark measures a FORK of dragonboat, github.com/jeremyhahn/
// dragonboat/v4, and says so in the import paths rather than disguising it as
// upstream. The fork exists because the benchmark needs a dragonboat that
// compiles against PebbleDB v1.1.5, which upstream v4.0.0 does not.
//
// Requiring github.com/lni/dragonboat/v4 and redirecting it with a replace
// such as
//
//	github.com/lni/dragonboat/v4 => ../../../../dragonboat
//
// would escape the repository altogether: it resolves only on a machine that
// happens to have the fork checked out four levels up, and fails on every
// clean checkout.
//
// Pinning the fork by version rather than replacing upstream with it is not
// just tidier, it is the only form that works: once the fork declares its own
// module path, its internal imports resolve under that path too, and a
// `replace lni => jeremyhahn` makes go load one module version under two
// different paths, which is an error.
//
// The fork is published in two places on purpose, and both serve the same
// commit, so the one go.sum entry verifies either way. Public consumers
// resolve github.com/jeremyhahn/dragonboat/v4 from GitHub through
// proxy.golang.org. Development machines and the CI runner set
// GOPRIVATE=github.com/jeremyhahn/* and rewrite that prefix to the internal
// Gitea, so they resolve it from the mirror there instead — which is why the
// fork must be mirrored there even though the GitHub repository is public.
//
// The perf module is unpublished, so that one still resolves from the sibling
// directory.
replace github.com/jeremyhahn/go-quicraft/test/performance => ../

replace github.com/jeremyhahn/go-quicraft => ../../..
