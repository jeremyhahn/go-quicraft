#!/bin/sh
# quic-go needs UDP buffers of at least 7168 KiB: https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes
sysctl -w net.core.rmem_max=7500000 >/dev/null 2>&1 || true
sysctl -w net.core.wmem_max=7500000 >/dev/null 2>&1 || true

exec "$@"
