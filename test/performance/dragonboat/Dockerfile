# Dockerfile for building and running the QuicRaft vs Dragonboat benchmark.
#
# Build context must be the go-quicraft repository root. The Makefile
# stages the dragonboat source into .build-cache/dragonboat before
# invoking this build.
#
# Usage (via Makefile):
#   make perf-compare-dragonboat

FROM golang:1.26.1 AS builder

WORKDIR /build

# Copy the full go-quicraft source tree (build context = repo root).
COPY . /build/go-quicraft

# Copy dragonboat source from the staged build cache directory.
# The Makefile copies it from DRAGONBOAT_SRC before docker build.
RUN test -d /build/go-quicraft/.build-cache/dragonboat || \
    { echo "ERROR: .build-cache/dragonboat not found. Run via 'make perf-compare-dragonboat'"; exit 1; }
RUN mv /build/go-quicraft/.build-cache/dragonboat /build/dragonboat

# Update the replace directives to use build-context paths.
RUN sed -i 's|=> ../../../|=> /build/go-quicraft/|' \
        /build/go-quicraft/test/performance/dragonboat/go.mod && \
    sed -i 's|github.com/lni/dragonboat/v4 =>.*|github.com/lni/dragonboat/v4 => /build/dragonboat|' \
        /build/go-quicraft/test/performance/dragonboat/go.mod

WORKDIR /build/go-quicraft/test/performance/dragonboat

# Regenerate go.sum after replace directive changes, then download dependencies.
RUN go mod tidy && go mod download

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
CMD ["-test.bench=.", "-test.benchtime=5s", "-test.timeout=600s", "-test.v"]
