#!/usr/bin/env bash
#
# tests/e2e/common/deploy-hdfs-pod.sh
#
# Shared HDFS-pod deployer for csi-e2e workflows (simple + kerberos).
# Replaces two near-duplicate 30–70-line inline blocks in
# .github/workflows/csi-e2e.yml. Manifests (hdfs-pod.yaml /
# hdfs-kerberos-pod.yaml) and the envsubst fix for hostAliases were
# extracted in earlier commits (8a7c414 / df37e6c / cb47063); this
# script absorbs the remaining deploy orchestration:
#   - namespace create (idempotent)
#   - kubectl apply (with envsubst for kerberos hostAliases)
#   - pod Ready wait
#   - svc ClusterIP → /etc/hosts (hostNetwork nodeplugin)
#   - 30-iteration dfsadmin -report readiness loop with `Live datanodes`
#     gate, success/failure diagnostic tails
#
# Behaviour split:
#   - simple    : kubectl apply -f hdfs-pod.yaml
#                 dfsadmin runs as `su - hdfs -c "..."` (no auth)
#                 120s pod-Ready timeout
#   - kerberos  : envsubst < hdfs-kerberos-pod.yaml | kubectl apply -f -
#                 dfsadmin runs directly (kinit handles auth)
#                 300s pod-Ready timeout (cold first-pull of dyrnq/hdfs)
#                 kinit each iteration; transient failure absorbed by `|| true`
#
# Usage:
#   # from a workflow step (sourced so function is in scope):
#   . tests/e2e/common/deploy-hdfs-pod.sh
#   deploy_hdfs_pod simple   csi-mntrs /tmp/kubeconfig
#   deploy_hdfs_pod kerberos csi-mntrs /tmp/kubeconfig
#
#   # direct invocation (local dev):
#   bash tests/e2e/common/deploy-hdfs-pod.sh kerberos csi-mntrs /tmp/kubeconfig
#
# All six parameters are optional (defaults shown below).

# Guard against double-include.
if [[ -n "${__DEPLOY_HDFS_POD_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__DEPLOY_HDFS_POD_LOADED=1

deploy_hdfs_pod() {
    local mode="${1:-simple}"            # simple | kerberos
    local namespace="${2:-csi-mntrs}"
    local kubeconfig="${3:-/tmp/kubeconfig}"
    local manifest="${4:-}"              # empty → auto-pick by mode
    local timeout="${5:-120}"            # 120 simple, 300 kerberos
    local attempts="${6:-30}"

    # Default manifest to mode-appropriate path when caller didn't override.
    if [ -z "$manifest" ]; then
        if [ "$mode" = "kerberos" ]; then
            manifest="tests/e2e/csi/hdfs-kerberos-pod.yaml"
        else
            manifest="tests/e2e/csi/hdfs-pod.yaml"
        fi
    fi

    # Idempotent namespace create (create --dry-run=client + apply trick —
    # an earlier heredoc referenced `namespace: csi-mntrs` without ever
    # creating it, so apply failed with `namespaces "csi-mntrs" not found`
    # in csi-e2e run 28102259330, 2026-06-24).
    kubectl --kubeconfig "$kubeconfig" create namespace "$namespace" \
        --dry-run=client -o yaml \
        | kubectl --kubeconfig "$kubeconfig" apply -f -

    if [ "$mode" = "kerberos" ]; then
        # hostAliases needs the node's IP and name. hostNetwork makes the
        # HDFS pod share the runner's network namespace, so the DN binds
        # the node IP (not the Service ClusterIP, which on single-node
        # k3s is unreachable). See [[kerberos-single-node-k3s-hostnet]].
        local NODE_IP NODE_NAME
        NODE_IP=$(kubectl --kubeconfig "$kubeconfig" get nodes \
            -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
        NODE_NAME=$(kubectl --kubeconfig "$kubeconfig" get nodes \
            -o jsonpath='{.items[0].metadata.name}')
        export NODE_IP NODE_NAME
        echo "node: ${NODE_NAME} ip: ${NODE_IP}"
        # Tighten the substitution surface to only what hostAliases needs —
        # any $HDFS_HOSTNAME/$HDFS_REALM in the manifest is left literal.
        # shellcheck disable=SC2016  # intentional: limit envsubst to these 2 vars
        envsubst '$NODE_IP $NODE_NAME' < "$manifest" \
            | kubectl --kubeconfig "$kubeconfig" apply -f -
    else
        kubectl --kubeconfig "$kubeconfig" apply -f "$manifest"
    fi

    kubectl --kubeconfig "$kubeconfig" -n "$namespace" wait \
        --for=condition=Ready pod/hdfs --timeout="${timeout}s"

    # Service ClusterIP → /etc/hosts so the hostNetwork nodeplugin can
    # resolve `hdfs` without going through kube-dns (which has flaked on
    # single-node k3s).
    local SVC_IP
    SVC_IP=$(kubectl --kubeconfig "$kubeconfig" -n "$namespace" get svc hdfs \
        -o jsonpath='{.spec.clusterIP}')
    echo "$SVC_IP hdfs" | sudo tee -a /etc/hosts

    # Build the dfsadmin -report command. simple uses `su - hdfs -c`
    # (no auth); kerberos uses direct exec (already authenticated by
    # kinit above). Array form avoids eval / quoting pitfalls.
    local report_cmd
    if [ "$mode" = "kerberos" ]; then
        report_cmd=( kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec hdfs --
            /opt/hadoop/bin/hdfs dfsadmin -report )
    else
        report_cmd=( kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec hdfs --
            su - hdfs -c "/opt/hadoop/bin/hdfs dfsadmin -report" )
    fi

    # kerberos failures are noisier (auth race, kdc reachability), so give
    # more log context on the fail-path tail.
    local fail_tail=20
    [ "$mode" = "kerberos" ] && fail_tail=30

    local i
    for i in $(seq 1 "$attempts"); do
        if [ "$mode" = "kerberos" ]; then
            # Transient kinit failure must not abort the loop under
            # `set -e` (GHA's default for `run:` blocks); the real gate
            # is dfsadmin -report below. Without `|| true` the loop
            # aborts in ~0.3s with no diagnostic — see #175 commit 8b5ff6e.
            kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec hdfs -- \
                /usr/bin/kinit -kt /etc/hadoop/hdfs.keytab \
                "hdfs/${HDFS_HOSTNAME}@${HDFS_REALM}" 2>/dev/null || true
        fi
        if "${report_cmd[@]}" 2>&1 | grep "Live datanodes" >/dev/null; then
            echo "HDFS ready after ${i} attempts"
            kubectl --kubeconfig "$kubeconfig" -n "$namespace" logs hdfs 2>&1 | tail -5
            "${report_cmd[@]}" 2>&1 | head -10
            return 0
        fi
        echo "  attempt $i: waiting for HDFS..."
        if [ "$i" -eq "$attempts" ]; then
            echo "::error::HDFS not ready after $attempts attempts"
            "${report_cmd[@]}" 2>&1 | head -20
            kubectl --kubeconfig "$kubeconfig" -n "$namespace" logs hdfs 2>&1 | tail -"$fail_tail"
            return 1
        fi
        sleep 3
    done
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    deploy_hdfs_pod "${1:-simple}" "${2:-csi-mntrs}" "${3:-/tmp/kubeconfig}" \
        "${4:-}" "${5:-120}" "${6:-30}"
fi