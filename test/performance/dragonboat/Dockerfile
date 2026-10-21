# Dockerfile for building and running the QuicRaft vs Dragonboat benchmark.
#
# Build context must be the go-quicraft repository root. The dragonboat fork
# (github.com/jeremyhahn/dragonboat/v4) is an ordinary versioned dependency.
#
# Usage (via Makefile):
#   make perf-run PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,dragonboat

FROM golang:1.27.3 AS builder

WORKDIR /build

# Copy the full go-quicraft source tree (build context = repo root).
COPY . /build/go-quicraft

WORKDIR /build/go-quicraft/test/performance/dragonboat

# Download dependencies using the committed go.mod/go.sum. No tidy: it would
# re-resolve dependencies at build time and break reproducibility.
RUN go mod download

# Compile the test binary with the benchmark build tag.
RUN CGO_ENABLED=1 go test -tags=benchmark -c -o /build/benchmark.test .

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/benchmark.test /usr/local/bin/benchmark.test

WORKDIR /tmp

# Run all benchmarks with 5 second minimum duration per benchmark.
ENTRYPOINT ["/usr/local/bin/benchmark.test"]
CMD ["-test.bench=.", "-test.run=^$", "-test.benchtime=5s", "-test.timeout=600s", "-test.v"]
