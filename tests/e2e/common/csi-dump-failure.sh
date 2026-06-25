#!/usr/bin/env bash
#
# tests/e2e/common/csi-dump-failure.sh
#
# Dump CSI / kubelet state when a csi-e2e job fails. Used as the
# `Dump on failure` step in csi-e2e.yml (three callsites: s3, hdfs,
# hdfs-kerberos).
#
# Usage:
#   . tests/e2e/common/csi-dump-failure.sh
#   csi_dump_failure <kubeconfig> <namespace> <test_pod> [out_dir]
#
#   defaults: kubeconfig=/tmp/kubeconfig, namespace=csi-mntrs,
#             out_dir=/tmp/csi-dump
#
# Dumps to stdout (NOT to files in out_dir) — the original step
# dumped to stdout and the per-job Upload mount log artifact step
# already captures the entire job's stdout. Direct-to-files mode
# would require an additional upload-artifact step per dump, which
# the original didn't have.
#
# All kubectl calls are `|| true` because the resources may not
# exist yet if the failure was during csi-controller/nodeplugin
# startup (e.g. image pull failure).
#
# test_pod: the static-publish pod name. The dynamic-publish pod
# name is `<test_pod>-dyn`. csi-e2e jobs publish both a static
# (`tests/e2e/csi/static/<backend>.yaml`) and dynamic
# (`tests/e2e/csi/dynamic/<backend>.yaml`) PV, each consuming the
# same StorageClass; both pods run the same test workload and are
# scheduled concurrently for race detection.

# Guard against double-include.
if [[ -n "${__CSI_DUMP_FAILURE_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__CSI_DUMP_FAILURE_LOADED=1

csi_dump_failure() {
    local kubeconfig="${1:-/tmp/kubeconfig}"
    local namespace="${2:-csi-mntrs}"
    local test_pod="${3:-mntrs-csi-e2e}"
    local out_dir="${4:-/tmp/csi-dump}"

    mkdir -p "$out_dir"

    echo "=== csi-mntrs pods ==="
    kubectl --kubeconfig "$kubeconfig" -n "$namespace" get pods -o wide \
        > "$out_dir/pods.txt" 2>&1 || true
    cat "$out_dir/pods.txt"

    echo
    echo "=== csi-controller log (last 50) ==="
    kubectl --kubeconfig "$kubeconfig" -n "$namespace" \
        logs -l app=csi-controller-mntrs --tail=50 \
        > "$out_dir/controller.log" 2>&1 || true
    cat "$out_dir/controller.log"

    echo
    echo "=== csi-nodeplugin log (last 50) ==="
    kubectl --kubeconfig "$kubeconfig" -n "$namespace" \
        logs -l app=csi-nodeplugin-mntrs --tail=50 \
        > "$out_dir/nodeplugin.log" 2>&1 || true
    cat "$out_dir/nodeplugin.log"

    echo
    echo "=== test pod events (dynamic) ==="
    kubectl --kubeconfig "$kubeconfig" describe pod "${test_pod}-dyn" \
        > "$out_dir/test-pod-dyn.txt" 2>&1 || true
    cat "$out_dir/test-pod-dyn.txt"

    echo
    echo "=== test pod events (static) ==="
    kubectl --kubeconfig "$kubeconfig" describe pod "$test_pod" \
        > "$out_dir/test-pod.txt" 2>&1 || true
    cat "$out_dir/test-pod.txt"

    echo
    echo "=== recent events (last 30) ==="
    kubectl --kubeconfig "$kubeconfig" get events --sort-by=.lastTimestamp \
        > "$out_dir/events.txt" 2>&1 || true
    tail -30 "$out_dir/events.txt"
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    csi_dump_failure "$@"
fi