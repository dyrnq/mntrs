#!/usr/bin/env bash
#
# tests/e2e/common/build-csi.sh
#
# Build the mntrs-csi binary and stage it into the docker/csi/
# context. Used by csi-integration.yml + csi-e2e.yml before the
# docker build-push-action step that publishes the CSI image.
#
# Usage:
#   . tests/e2e/common/build-csi.sh
#   build_csi <target> [docker_context]
#
#   defaults: target=x86_64-unknown-linux-musl, docker_context=docker/csi
#
# Four callers:
#   - csi-integration.yml:    musl (single binary used by both s3/hdfs matrix)
#   - csi-e2e.yml (S3/MinIO): musl
#   - csi-e2e.yml (HDFS):     musl
#   - csi-e2e.yml (HDFS-Krb): gnu  (musl static-pie is incompatible with
#                                   kerberos dynamic linking — see
#                                   [[kerberos-csi-fix-dlopen-auth]])
#
# Caller must pre-install any cross-compile toolchain (e.g.
# `musl-tools` for musl, `build-essential` for gnu) — this script
# only runs `rustup target add` (idempotent) + `cargo build`.

# Guard against double-include.
if [[ -n "${__BUILD_CSI_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__BUILD_CSI_LOADED=1

build_csi() {
    local target="${1:-x86_64-unknown-linux-musl}"
    local docker_context="${2:-docker/csi}"
    local binary="target/${target}/release/mntrs-csi"

    rustup target add "$target"
    cargo build --release --package mntrs-csi --target "$target"

    mkdir -p "$docker_context"
    cp "$binary" "$docker_context/"
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    build_csi "$@"
fi