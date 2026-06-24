#!/usr/bin/env bash
#
# tests/e2e/common/hdfs-prep.sh
#
# Shared HDFS prep for tests that mount HDFS via opendal/hdfs-native.
# Solves a class of bug (issue #148 follow-up): the dyrnq/hdfs image
# boots HDFS with drwxr-xr-x hdfs:supergroup on /, so the GHA `runner`
# user (or any non-hdfs mount user) cannot create files at the FUSE
# mount root — every write to /mnt/.../ fails with
# AccessControlException on `ClientProtocol.create inode="/"`.
#
# Three variants cover the three prep paths in this repo:
#   - hdfs_prep_docker             integration.yml (docker exec)
#   - hdfs_prep_kubectl_simple     tests/e2e/csi/hdfs.sh (kubectl + su - hdfs)
#   - hdfs_prep_kubectl_kerberos   tests/e2e/csi/hdfs-kerberos.sh (kubectl + kinit)
#
# Each does the same three things:
#   1. chmod 777 /  (so the mount user can create at the HDFS root)
#   2. mkdir -p /test + chmod 777 /test  (so the test seed file is in
#      a subdir the mount user can also read+write)
#
# All ops use `2>/dev/null || true` so a transient HDFS-not-ready or
# permission-flush race doesn't fail the test. The calling test's own
# readiness check (e.g. dfsadmin -report) is the source of truth.
#
# Usage:
#   # from a script (sourced):
#   . "$(dirname "$0")/../common/hdfs-prep.sh"
#   hdfs_prep_docker
#
#   # from GHA / direct invocation:
#   bash tests/e2e/common/hdfs-prep.sh docker
#   bash tests/e2e/common/hdfs-prep.sh kubectl-simple csi-mntrs
#   bash tests/e2e/common/hdfs-prep.sh kubectl-kerberos csi-mntrs

# Guard against double-include.
if [[ -n "${__HDFS_PREP_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__HDFS_PREP_LOADED=1

_hdfs_docker_exec() {
    # $1 = hdfs dfs sub-args
    docker exec -u hdfs hdfs /opt/hadoop/bin/hdfs dfs "$@" 2>/dev/null || true
}

_hdfs_kubectl_simple_exec() {
    # $1 = hdfs dfs sub-args
    # In simple-auth mode the HDFS superuser is `hdfs`, but kubectl exec
    # runs as root. su - hdfs -c "<cmd>" so chmod 777 / is effective.
    local ns=$1; shift
    ${KUBECTL:-kubectl} -n "$ns" exec hdfs -- \
        su - hdfs -c "/opt/hadoop/bin/hdfs dfs $*" 2>/dev/null || true
}

_hdfs_kubectl_kerberos_exec() {
    # $1 = hdfs dfs sub-args
    # In kerberos mode the `hdfs` principal is already auth'd via kinit,
    # so kubectl exec hdfs -- hdfs dfs ... works directly.
    local ns=$1; shift
    ${KUBECTL:-kubectl} -n "$ns" exec hdfs -- \
        /opt/hadoop/bin/hdfs dfs "$@" 2>/dev/null || true
}

hdfs_prep_docker() {
    _hdfs_docker_exec -chmod 777 /
    _hdfs_docker_exec -mkdir -p /test
    _hdfs_docker_exec -chmod 777 /test
}

hdfs_prep_kubectl_simple() {
    local ns=$1
    _hdfs_kubectl_simple_exec "$ns" -chmod 777 /
    _hdfs_kubectl_simple_exec "$ns" -mkdir -p /test
    _hdfs_kubectl_simple_exec "$ns" -chmod 777 /test
}

hdfs_prep_kubectl_kerberos() {
    local ns=$1
    _hdfs_kubectl_kerberos_exec "$ns" -chmod 777 /
    _hdfs_kubectl_kerberos_exec "$ns" -mkdir -p /test
    _hdfs_kubectl_kerberos_exec "$ns" -chmod 777 /test
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    mode=${1:-}
    case "$mode" in
        docker)
            hdfs_prep_docker
            ;;
        kubectl-simple)
            ns=${2:?usage: $0 kubectl-simple <namespace>}
            hdfs_prep_kubectl_simple "$ns"
            ;;
        kubectl-kerberos)
            ns=${2:?usage: $0 kubectl-kerberos <namespace>}
            hdfs_prep_kubectl_kerberos "$ns"
            ;;
        *)
            echo "usage: $0 {docker|kubectl-simple NS|kubectl-kerberos NS}" >&2
            exit 2
            ;;
    esac
fi
