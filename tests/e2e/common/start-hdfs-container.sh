#!/usr/bin/env bash
#
# tests/e2e/common/start-hdfs-container.sh
#
# Shared docker-run + readiness-wait for the dyrnq/hdfs:latest-debian
# test image. Replaces three inline 40–75-line blocks across
# integration.yml (×2) and csi-integration.yml (×1).
#
# Two modes:
#   - simple    : HADOOP_SECURITY_AUTHENTICATION=simple, --network host
#                 (so DN 9866 binds the host's loopback; docker-proxy
#                 ConnectionResets the long-lived block-read TCP connect
#                 — see [[issue-148-hdfs-k8s-datanode-ip]]). Readiness
#                 uses the write-probe pattern (more reliable than
#                 parsing dfsadmin text output across image versions).
#   - kerberos  : default image kerberos mode, --hostname + -p
#                 8020/88/749/9866/9864. Readiness is KDC port 88
#                 probe → kinit (absorbed via `|| true` because kinit
#                 is transient-flaky) → dfsadmin `Live datanodes`.
#
# Usage:
#   # from a workflow step (sourced so function is in scope):
#   . tests/e2e/common/start-hdfs-container.sh
#   start_hdfs_container simple   hdfs localhost
#   start_hdfs_container kerberos hdfs hdfs.test
#
#   # direct invocation (local dev):
#   bash tests/e2e/common/start-hdfs-container.sh simple hdfs localhost
#
# Returns 1 if HDFS does not become ready in 30 attempts — caller
# decides what to do. Note: the original csi-integration.yml inline
# loop was SILENT on timeout (no `exit 1`), leading to a confusing
# downstream failure from the seed-file step. Extracting here makes
# the behaviour consistent across all three callers.

# Guard against double-include.
if [[ -n "${__START_HDFS_CONTAINER_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__START_HDFS_CONTAINER_LOADED=1

start_hdfs_container() {
    local mode="${1:-simple}"           # simple | kerberos
    local container="${2:-hdfs}"
    local hostname="${3:-localhost}"

    case "$mode" in
        simple)
            # HADOOP_SECURITY_AUTHENTICATION=simple strips krb5kdc +
            # kadmind from the s6 tree; nn/dn start without auth.
            docker run -d --name "$container" --network host \
                -e HADOOP_SECURITY_AUTHENTICATION=simple \
                -e HDFS_HOSTNAME="$hostname" \
                dyrnq/hdfs:latest-debian
            ;;
        kerberos)
            docker run -d --name "$container" --hostname "$hostname" \
                -e HDFS_HOSTNAME="$hostname" \
                -p 8020:8020 -p 88:88 -p 749:749 \
                -p 9866:9866 -p 9864:9864 \
                dyrnq/hdfs:latest-debian

            # KDC must come up before kinit can succeed. Probe via
            # bash /dev/tcp — no nc / no third-party dependency.
            echo "=== Wait for KDC port 88 ==="
            local i
            for i in $(seq 1 30); do
                echo "  kdc attempt $i..."
                if docker exec "$container" \
                    bash -c 'echo > /dev/tcp/127.0.0.1/88' 2>/dev/null; then
                    echo "KDC port 88 up after ${i}s"
                    break
                fi
                if [ "$i" -eq 30 ]; then
                    echo "::error::KDC port 88 never came up"
                    docker logs "$container" 2>&1 | tail -40
                    return 1
                fi
                sleep 2
            done
            ;;
        *)
            echo "::error::start_hdfs_container: unknown mode '$mode' (simple|kerberos)" >&2
            return 2
            ;;
    esac

    # Readiness loop.
    local attempts=30
    local i
    for i in $(seq 1 "$attempts"); do
        if [ "$mode" = "kerberos" ]; then
            # kinit can fail transiently (KDC not fully bootstrapped,
            # kadmind still seeding principals). We absorb with `|| true`
            # — the real gate is dfsadmin -report finding Live datanodes.
            # Only after kinit succeeds do we check dfsadmin; otherwise
            # dfsadmin would always succeed (HDFS still serves superuser
            # access) and we'd think the cluster is healthy when kinit
            # is actually broken (which is the situation we need to
            # surface to the caller).
            local kinit_ok=0
            if docker exec -u hdfs "$container" \
                /usr/bin/kinit -kt /etc/hadoop/hdfs.keytab \
                "hdfs/${hostname}@TEST.LOCAL" 2>/dev/null; then
                kinit_ok=1
            fi
            if [ "$kinit_ok" -eq 1 ] \
                && docker exec "$container" /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 \
                    | grep -q "Live datanodes"; then
                echo "HDFS ready (kinit took attempt $i)"
                break
            fi
        else
            # write-probe pattern — more reliable than parsing dfsadmin
            # text output across image versions
            if echo "probe" | docker exec -i -u hdfs "$container" \
                    /opt/hadoop/bin/hdfs dfs -put - /_readiness_probe 2>/dev/null \
                && docker exec -u hdfs "$container" \
                    /opt/hadoop/bin/hdfs dfs -rm /_readiness_probe >/dev/null 2>&1; then
                echo "HDFS ready after ${i} attempts"
                break
            fi
        fi
        if [ "$i" -eq "$attempts" ]; then
            echo "::error::HDFS not ready after $attempts attempts"
            if [ "$mode" = "kerberos" ]; then
                # surface the kinit failure so the caller can see WHY
                docker exec -u hdfs "$container" \
                    /usr/bin/kinit -kt /etc/hadoop/hdfs.keytab \
                    "hdfs/${hostname}@TEST.LOCAL" 2>&1
                docker exec "$container" \
                    /opt/hadoop/bin/hdfs dfsadmin -report 2>&1
                docker logs "$container" 2>&1 | tail -30
            else
                docker exec -u hdfs "$container" \
                    /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 | head -20
                docker logs "$container" 2>&1 | tail -20
            fi
            return 1
        fi
        sleep 3
    done

    # Success diagnostics — mirrors the original mount-tests simple
    # behaviour. Cheap informational logs that help postmortem.
    docker logs "$container" 2>&1 | tail -5
    docker exec "$container" /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 | head -10
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    start_hdfs_container "${1:-simple}" "${2:-hdfs}" "${3:-localhost}"
fi