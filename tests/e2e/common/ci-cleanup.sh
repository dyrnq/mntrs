#!/usr/bin/env bash
#
# tests/e2e/common/ci-cleanup.sh
#
# Shared CI-cleanup helpers for integration.yml + csi-integration.yml
# + csi-e2e.yml. Three orthogonal concerns — FUSE mount, docker
# container, k8s pod/svc — split into three independent functions
# (rather than one mega-script) so each caller picks what it needs
# and unrelated ops stay decoupled.
#
# Usage:
#   # from a workflow step (sourced so functions are in scope):
#   . tests/e2e/common/ci-cleanup.sh
#   cleanup_fuse_mount /mnt/mntrs-test
#   cleanup_docker_container hdfs
#   cleanup_k8s_pod_svc hdfs csi-mntrs
#
# All ops use `|| true` (or `2>/dev/null || true`) so a transient
# "already gone" never fails the cleanup step. The cleanup step
# itself should be `if: always()` in the workflow so failures in
# upstream steps still trigger cleanup (the 7 cleanup steps already
# in this repo all have `if: always()`).

# Guard against double-include.
if [[ -n "${__CI_CLEANUP_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__CI_CLEANUP_LOADED=1

cleanup_fuse_mount() {
    # Unmount a FUSE mountpoint. Guard with `mount | grep` so we
    # don't error on an already-cleaned-up path; then try a clean
    # unmount and fall back to lazy (-z) if the FUSE session is
    # stuck (e.g. child process holding the FUSE fd open).
    local mnt="$1"
    if mount | grep -q "$mnt"; then
        sudo fusermount3 -u "$mnt" 2>/dev/null \
            || fusermount3 -uz "$mnt" 2>/dev/null \
            || true
    fi
}

cleanup_docker_container() {
    # `docker rm -f` is a no-op if the container doesn't exist
    # (with `|| true` absorbing the exit code).
    local name="$1"
    docker rm -f "$name" 2>/dev/null || true
}

cleanup_k8s_pod_svc() {
    # Force-delete a pod and its (optional) co-named svc in the
    # given namespace. --force --grace-period=0 avoids hanging on
    # a stuck pod (e.g. hdfs Kerberos race; dfsadmin hang).
    # --ignore-not-found makes this safe to call multiple times or
    # on resources that never got created. If the pod has no svc
    # (e.g. csi-integration's smoke-test pod), the svc delete is a
    # no-op via --ignore-not-found.
    local name="$1" ns="$2" kubeconfig="${3:-/tmp/kubeconfig}"
    kubectl --kubeconfig "$kubeconfig" -n "$ns" delete pod "$name" \
        --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    kubectl --kubeconfig "$kubeconfig" -n "$ns" delete svc "$name" \
        --ignore-not-found 2>/dev/null || true
}

# Direct invocation dispatch (when not sourced) — no-op (functions
# only, can't be invoked usefully from CLI; print usage).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    cat >&2 <<'USAGE'
ci-cleanup.sh: source this file or call its functions; don't exec directly.

  . tests/e2e/common/ci-cleanup.sh
  cleanup_fuse_mount /mnt/foo
  cleanup_docker_container hdfs
  cleanup_k8s_pod_svc hdfs csi-mntrs
USAGE
    exit 1
fi