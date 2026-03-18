#!/bin/sh
# Increase UDP receive/send buffer limits for QUIC transport.
# quic-go requires at least 7168 KiB (~7.3 MB); we set 7500000 bytes
# to provide headroom. See:
# https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes
sysctl -w net.core.rmem_max=7500000 >/dev/null 2>&1 || true
sysctl -w net.core.wmem_max=7500000 >/dev/null 2>&1 || true

exec "$@"
