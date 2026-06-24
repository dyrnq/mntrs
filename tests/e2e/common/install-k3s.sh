#!/usr/bin/env bash
#
# tests/e2e/common/install-k3s.sh
#
# Shared k3s installer for csi-integration and csi-e2e workflows.
# Defaults to 3 retries; csi-integration overrides to 1 (see PR notes).
# Writes kubeconfig to BOTH <kubeconfig> and ~/.kube/config so callers
# can use either (csi-e2e uses --kubeconfig, csi-integration uses default).
#
# Usage:
#   bash tests/e2e/common/install-k3s.sh [retries] [kubeconfig-path]
#   # default: 3 retries, /tmp/kubeconfig

# Guard against double-include.
if [[ -n "${__INSTALL_K3S_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__INSTALL_K3S_LOADED=1

install_k3s() {
    local retries="${1:-3}"
    local kubeconfig="${2:-/tmp/kubeconfig}"
    local i
    for i in $(seq 1 "$retries"); do
        echo "k3s install attempt $i..."
        if curl -sfL --retry 3 --retry-delay 5 https://get.k3s.io | sh -; then
            break
        fi
        if [ "$i" -eq "$retries" ]; then
            echo "::error::k3s install failed after $retries attempts"
            return 1
        fi
        echo "::warning::k3s install attempt $i failed, retrying in 10s"
        sleep 10
    done

    sudo mkdir -p /root/.kube
    sudo cp /etc/rancher/k3s/k3s.yaml "$kubeconfig"
    sudo chown "$(id -u)":"$(id -g)" "$kubeconfig"
    mkdir -p ~/.kube
    cp "$kubeconfig" ~/.kube/config
    kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready node --all --timeout=60s
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    install_k3s "${1:-3}" "${2:-/tmp/kubeconfig}"
fi
