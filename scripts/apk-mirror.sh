#!/bin/sh
# apk-mirror: v1 (ci-template): apk through the LAN apk
# cache when the build has one. On a pool runner DEVCACHE_APK_MIRROR names the
# cache; the build passes it as the BuildKit secret apk_mirror (Makefile
# `--secret id=apk_mirror,env=DEVCACHE_APK_MIRROR`, or a compose build secret
# from DEVCACHE_APK_MIRROR), which each RUN mounts as APK_MIRROR; on a
# GitHub-hosted runner it is unset, the secret is empty and apk goes to
# dl-cdn.alpinelinux.org as before. A Dockerfile RUN bind-mounts this script
# (never COPYs it) and the repositories file is restored however apk ends, so
# no layer keeps the mirror; a secret, unlike a build argument, is in no
# history entry either. apk still checks the signed APKINDEX and every
# package's hash. apk-index-sum.sh v2 reads the indexes through it too.
#
#   usage (in a Dockerfile RUN): apk-mirror <apk arguments...>
#
# APK_REPOSITORIES overrides /etc/apk/repositories for the tests alone.
set -eu

repos=${APK_REPOSITORIES:-/etc/apk/repositories}
mirror=${APK_MIRROR:-}
if [ -z "$mirror" ]; then
	exec apk "$@"
fi
case $mirror in
http://* | https://*) ;;
*)
	echo "apk-mirror: APK_MIRROR '$mirror' is not an http(s) URL" >&2
	exit 2
	;;
esac
mirror=${mirror%/}
saved=$(mktemp)
cp "$repos" "$saved"
restore() { cat "$saved" >"$repos" && rm -f "$saved"; }
trap restore EXIT
sed -i "s#https\{0,1\}://dl-cdn\.alpinelinux\.org/alpine#$mirror#" "$repos"
apk "$@"
