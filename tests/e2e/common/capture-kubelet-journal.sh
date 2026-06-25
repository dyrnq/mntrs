#!/usr/bin/env bash
#
# tests/e2e/common/capture-kubelet-journal.sh
#
# Capture the k3s kubelet systemd journal for postmortem. CI runners
# run k3s as a systemd service, so mount failures and CSI socket
# timeouts only show up here, not in `kubectl describe` (which is
# post-mortem on already-cleaned-up resources).
#
# Usage:
#   . tests/e2e/common/capture-kubelet-journal.sh
#   capture_kubelet_journal <out_path> [since]
#
#   defaults: out_path=/tmp/k3s-journal.log, since="10 minutes ago"
#
# Three callers in csi-e2e.yml (s3, hdfs, hdfs-kerberos) pass
# distinct out_paths (k3s-journal{,-hdfs,-hdfs-krb}.log) so the
# per-job upload-artifact steps don't collide.
#
# `|| true` on journalctl because the unit may not exist if k3s
# install failed mid-way (we still want the step to succeed so the
# subsequent upload-artifact step runs).

# Guard against double-include.
if [[ -n "${__CAPTURE_KUBELET_JOURNAL_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__CAPTURE_KUBELET_JOURNAL_LOADED=1

capture_kubelet_journal() {
    local out_path="${1:-/tmp/k3s-journal.log}"
    local since="${2:-10 minutes ago}"
    sudo journalctl -u k3s --since "$since" --no-pager > "$out_path" 2>&1 || true
    echo "k3s journal size: $(wc -l < "$out_path" 2>/dev/null || echo 0) lines"
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    capture_kubelet_journal "$@"
fi