# Dockerfile for building and running the OpenRaft benchmark binary.
#
# Build context must be the go-quicraft repository root so the full source
# tree is available.  The Makefile drives this build.
#
# Usage (via Makefile):
#   make perf-compare-openraft

FROM rust:latest AS builder

WORKDIR /src

# Copy only the openraft benchmark crate.  We do not need the rest of the
# repository for a pure Rust build.
COPY test/performance/openraft/ .

# Build the release binary.  Cargo resolves and fetches dependencies from
# crates.io on the first build, producing a lock file in the image layer.
RUN cargo build --release 2>&1

# ---------------------------------------------------------------------------
# Runtime stage - minimal Debian image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/openraft-bench /usr/local/bin/openraft-bench

ENTRYPOINT ["/usr/local/bin/openraft-bench"]
