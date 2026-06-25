#!/usr/bin/env bash
#
# tests/e2e/common/install-linux-deps.sh
#
# Shared FUSE/host dependency installer for Linux CI jobs.
#
# Modes:
#   standard  — host Rust build of mntrs needs libfuse3-dev + fuse3
#               (headers for the Rust crate + binary for runtime tests).
#   csi-e2e   — k3s node needs only the FUSE runtime (libfuse3-3), the
#               kernel module, and the /dev/fuse device node. mntrs-csi
#               links against gRPC, not libfuse, so dev headers are not
#               needed.
#
# Variants covered (see call-site mapping in the PR description):
#   A: standard + libfuse3-dev + fuse3 + protobuf-compiler
#   B: standard + krb5-user + libfuse3-dev + fuse3
#   D: csi-e2e  + libfuse3-3 + fuse3 + modprobe + mknod /dev/fuse
#   E: standard + libfuse3-dev + fuse3 (no protobuf) + GITHUB_PATH
#
# C (default-jdk only, ci.yml:134) is excluded — separate concern.
#
# Usage:
#   bash tests/e2e/common/install-linux-deps.sh [mode] [extra-pkgs] [with-protobuf] [with-gopath]
#   # defaults: standard, "", yes, no
#   # examples:
#   #   bash .../install-linux-deps.sh standard "" yes no           # A
#   #   bash .../install-linux-deps.sh standard "krb5-user" no no    # B
#   #   bash .../install-linux-deps.sh csi-e2e  "" no no             # D
#   #   bash .../install-linux-deps.sh standard "" no yes            # E

# Guard against double-include.
if [[ -n "${__INSTALL_LINUX_DEPS_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__INSTALL_LINUX_DEPS_LOADED=1

install_linux_deps() {
    local mode="${1:-standard}"
    local extra_pkgs="${2:-}"
    local with_protobuf="${3:-yes}"
    local with_gopath="${4:-no}"

    if [ "$mode" = "csi-e2e" ]; then
        # modprobe may fail if the kernel module is built in or already
        # loaded — match the original `set +e` semantics with || true.
        sudo modprobe fuse 2>/dev/null || true
    fi

    sudo apt-get update

    # shellcheck disable=SC2206  # intentional word-splitting
    local pkgs
    case "$mode" in
        csi-e2e) pkgs="fuse3 libfuse3-3" ;;
        *)       pkgs="libfuse3-dev fuse3" ;;
    esac
    [ "$with_protobuf" = "yes" ] && pkgs="$pkgs protobuf-compiler"
    [ -n "$extra_pkgs" ] && pkgs="$pkgs $extra_pkgs"

    # shellcheck disable=SC2086  # intentional word-splitting for $pkgs
    sudo apt-get install -y $pkgs

    if [ "$mode" = "csi-e2e" ] && [ ! -e /dev/fuse ]; then
        sudo mknod /dev/fuse c 10 229 || true
        sudo chmod 666 /dev/fuse || true
    fi

    if ! sudo grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null; then
        echo 'user_allow_other' | sudo tee -a /etc/fuse.conf >/dev/null
    fi

    if [ "$mode" = "csi-e2e" ]; then
        echo "--- /dev/fuse ---"; ls -la /dev/fuse
        echo "--- /etc/fuse.conf ---"; cat /etc/fuse.conf
        echo "--- fusermount3 ---"
        which fusermount3 && fusermount3 --version 2>&1 | head -1 || echo "fusermount3 missing"
    fi

    if [ "$with_gopath" = "yes" ]; then
        # GITHUB_PATH is set by GHA; guard for local dev invocation.
        if [ -n "${GITHUB_PATH:-}" ]; then
            echo "$HOME/go/bin" >> "$GITHUB_PATH"
        fi
    fi
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    install_linux_deps "${1:-standard}" "${2:-}" "${3:-yes}" "${4:-no}"
fi
