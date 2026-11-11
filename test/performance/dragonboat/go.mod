module github.com/jeremyhahn/go-quicraft/test/performance/dragonboat

go 1.27.4

require (
	github.com/jeremyhahn/dragonboat/v4 v4.0.1
	github.com/jeremyhahn/go-quicraft/test/performance v0.0.0
)

require (
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/VictoriaMetrics/metrics v1.44.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/datadriven v1.0.3 // indirect
	github.com/cockroachdb/errors v1.14.0 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240816210425-c5d0cb0b6fc0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/pebble v1.1.5 // indirect
	github.com/cockroachdb/redact v1.1.8 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20250429170803-42689b6311bb // indirect
	github.com/getsentry/sentry-go v0.49.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/go-uuid v1.0.4 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/memberlist v0.7.0 // indirect
	// go-quicraft is here only for test/netport, which benchutil uses for port
	// reservation. A replace binds the main module only, so the pair repeats
	// in every module that reaches benchutil. MVS reads go-quicraft's whole
	// requirement list, so its floors can raise this module's versions:
	// klauspost/compress, which Pebble uses for sstable zstd, follows the
	// parent's prometheus/client_golang. Re-run the profiles when it moves
	// rather than compare across the change.
	github.com/jeremyhahn/go-quicraft v0.0.0 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lni/goutils v1.4.0 // indirect
	github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376 // indirect
	github.com/miekg/dns v1.1.73 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.32 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	golang.org/x/exp v0.0.0-20260908205506-85c1c2202aba // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// The benchmark measures a fork of dragonboat, github.com/jeremyhahn/
// dragonboat/v4, under its own import path. The fork compiles against
// PebbleDB v1.1.5, which upstream v4.0.0 does not. It is required by version:
// replacing github.com/lni/dragonboat/v4 with it would load one module under
// two paths, which is an error.
//
// The perf module is unpublished, so it resolves from the sibling directory.
replace github.com/jeremyhahn/go-quicraft/test/performance => ../

replace github.com/jeremyhahn/go-quicraft => ../../..
