FROM golang:1.26.6 AS builder
WORKDIR /src
COPY . .
WORKDIR /src/test/performance/quicraft
# go mod download (not tidy) so the committed go.mod/go.sum are used
# verbatim and builds stay reproducible.
RUN go mod download && go test -c -tags=benchmark -o /bench-quicraft .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates procps && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bench-quicraft /usr/local/bin/bench-quicraft

# Entrypoint script raises kernel UDP buffer limits (requires --privileged)
# and then execs the benchmark binary. Without these limits, multi-node QUIC
# benchmarks suffer packet drops under burst traffic.
COPY test/performance/quicraft/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["-test.bench=.", "-test.run=^$", "-test.benchtime=5s", "-test.timeout=600s", "-test.v"]
