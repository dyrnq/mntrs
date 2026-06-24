#!/usr/bin/env bash
#
# End-to-end test: mntrs CSI driver + HDFS backend on a real K8s cluster.
#
# What it does:
#   1. Builds mntrs-csi (glibc) + pushes it to $REGISTRY
#   2. Deploys csi-mntrs (namespace, RBAC, controller, nodeplugin) in $CSI_NAMESPACE
#      using the pushed image, with imagePullSecrets wired up
#   3. Creates test data in HDFS (via kubectl exec into the HDFS Pod)
#   4. Creates a static PV + PVC + test pod using the mntrs CSI driver
#   5. Runs read/write/append/cleanup operations against the FUSE mount
#   6. Tears everything down
#
# Assumes HDFS is already running as a k8s Pod named "hdfs" in $HDFS_NAMESPACE:
#   - Simple auth (HADOOP_SECURITY_AUTHENTICATION=simple)
#   - A k8s Service "hdfs" exposes the NameNode RPC port
#   - /etc/hosts has an entry mapping "hdfs" to the Service ClusterIP
#   - $KUBECONFIG is pointed at the cluster
#
# Required env:
#   KUBECONFIG                Path to kubeconfig
#
# Optional env (with defaults):
#   REGISTRY                  Private registry host:port (default: 10.5.26.11:5000)
#   REGISTRY_USER             Registry username (default: test)
#   REGISTRY_PASS             Registry password (default: test)
#   IMAGE_TAG                 Image tag (default: dev)
#   HDFS_HOST                 HDFS namenode host:port (default: hdfs:8020)
#   HDFS_NAMESPACE            Namespace for the HDFS Pod (default: csi-mntrs)
#   CSI_NAMESPACE             Namespace for csi-mntrs (default: csi-mntrs)
#   SKIP_BUILD                Set to 1 to skip building/pushing the image
#   SKIP_DEPLOY               Set to 1 to assume csi-mntrs is already deployed
#   KEEP_ON_FAIL              Set to 1 to keep resources on failure for debugging
#
# Exit codes:
#   0  success
#   1  setup failure (build, push, deploy)
#   2  test failure (e2e operation mismatch)
#
set -euo pipefail

# ---------- configuration ----------
KUBECONFIG="${KUBECONFIG:?KUBECONFIG must be set}"
REGISTRY="${REGISTRY:-10.5.26.11:5000}"
REGISTRY_USER="${REGISTRY_USER:-test}"
REGISTRY_PASS="${REGISTRY_PASS:-test}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
IMAGE="${IMAGE:-${REGISTRY}/mntrs-csi:${IMAGE_TAG}}"

HDFS_HOST="${HDFS_HOST:-hdfs:8020}"
HDFS_NAMESPACE="${HDFS_NAMESPACE:-csi-mntrs}"

CSI_NAMESPACE="${CSI_NAMESPACE:-csi-mntrs}"
E2E_NAMESPACE="${E2E_NAMESPACE:-default}"
E2E_PV_NAME="${E2E_PV_NAME:-mntrs-csi-e2e-hdfs-pv}"
E2E_PVC_NAME="${E2E_PVC_NAME:-mntrs-csi-e2e-hdfs-pvc}"
E2E_POD_NAME="${E2E_POD_NAME:-mntrs-csi-e2e-hdfs}"
E2E_VOLUME_ID="${E2E_VOLUME_ID:-e2e-hdfs-vol}"
# Optional: pin test pods to a specific node (needed for multi-node clusters
# where HDFS Pod is only reachable from one node). In CI (single-node k3s)
# this is unnecessary — all pods land on the same node.
NODE_NAME="${NODE_NAME:-}"
NODE_SELECTOR=""
if [[ -n "${NODE_NAME}" ]]; then
    NODE_SELECTOR="nodeName: ${NODE_NAME}"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd)"
KUBECTL="kubectl --kubeconfig ${KUBECONFIG}"
STORAGE_URL="hdfs://${HDFS_HOST}/"

# ---------- helpers ----------
log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }
pass() { printf '\033[1;32m[PASS]\033[0m %s\n' "$*" >&2; }

# shellcheck disable=SC2329  # invoked via trap below
cleanup() {
    local exit_code=$?
    log "cleanup (exit=$exit_code)..."
    ${KUBECTL} delete pod "${E2E_POD_NAME}" -n "${E2E_NAMESPACE}" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pvc "${E2E_PVC_NAME}" -n "${E2E_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pv "${E2E_PV_NAME}" --ignore-not-found 2>/dev/null || true
    # Dynamic provision e2e resources
    ${KUBECTL} delete pod "mntrs-csi-e2e-hdfs-dyn" -n "${E2E_NAMESPACE}" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pvc "mntrs-csi-e2e-hdfs-dyn" -n "${E2E_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete sc "mntrs-dyn-hdfs-e2e" --ignore-not-found 2>/dev/null || true
    if [[ "${SKIP_DEPLOY:-0}" != "1" && -n "${DEPLOY_DIR:-}" ]]; then
        ${KUBECTL} delete -f "${DEPLOY_DIR}/" --ignore-not-found 2>/dev/null || true
        rm -rf "${DEPLOY_DIR}"
    fi
    # Do NOT remove the HDFS Pod — it is managed by the caller
    # (CI workflow or manual setup).
    exit "$exit_code"
}
trap cleanup EXIT

# ---------- preflight ----------
log "preflight..."
command -v kubectl >/dev/null || fail "kubectl not found"
[[ -f "${KUBECONFIG}" ]] || fail "KUBECONFIG ${KUBECONFIG} not found"
${KUBECTL} get nodes >/dev/null || fail "cannot reach cluster"
log "  cluster reachable: $(${KUBECTL} get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')"

# Check HDFS Pod
log "checking HDFS Pod in namespace '${HDFS_NAMESPACE}'..."
if ! ${KUBECTL} -n "${HDFS_NAMESPACE}" get pod hdfs >/dev/null 2>&1; then
    fail "HDFS Pod 'hdfs' not found in namespace '${HDFS_NAMESPACE}' — deploy it first:
  kubectl apply -f - <<'EOF'
  apiVersion: v1
  kind: Service
  metadata: {name: hdfs, namespace: ${HDFS_NAMESPACE}}
  spec:
    type: ClusterIP
    selector: {app: hdfs}
    ports:
      - {name: nn-rpc, port: 8020, targetPort: 8020}
      - {name: dn-data, port: 9866, targetPort: 9866}
  ---
  apiVersion: v1
  kind: Pod
  metadata: {name: hdfs, namespace: ${HDFS_NAMESPACE}, labels: {app: hdfs}}
  spec:
    hostname: hdfs
    # Pin to the same node the hdfs Service alias points to (the hostAliases
    # below map hdfs -> <NODE_IP>; the NN advertises hdfs:8020). Multi-node
    # clusters MUST set NODE_NAME; single-node (k3s CI) can omit it.
    ${NODE_SELECTOR}
    hostNetwork: true
    dnsPolicy: ClusterFirstWithHostNet
    # Required only when pulling from a private registry (e.g. multi-node
    # cluster using 10.5.26.11:5000). Public CI doesn't need this — remove
    # the block when applying there.
    imagePullSecrets:
      - name: reg-cred
    hostAliases:
      # hdfs MUST resolve to the LOCAL pod's IP, otherwise dfs clients
      # (and the embedded DN's block-server registration) hit a different
      # node's HDFS instance. With hostNetwork=true the kernel hostname
      # is the node's (e.g. z13), not spec.hostname — and the k8s JVM's
      # getServerPrincipal() / getLocalHostName() calls that hostname.
      # Replace <HDFS_POD_IP> with the node IP where the pod actually runs.
      - hostnames: [hdfs, z11, z12, z13, z14, f11, f12, f13]
        ip: <HDFS_POD_IP>
    containers:
      - name: hdfs
        # dyrnq/hdfs:latest-debian (post-issues #13/#14) renders the XMLs
        # from an entrypoint heredoc and, in simple mode, AUTO-STRIPS the
        # kerberos-only properties (principal/keytab/spnego/
        # dfs.data.transfer.protection/hadoop.rpc.protection + disables
        # block.access.token.enable & security.authorization) via envtoxml's
        # `!remove` sentinel. So mntrs no longer passes any of those env
        # vars — HADOOP_SECURITY_AUTHENTICATION=simple alone strips them.
        #
        # BIND vs ADVERTISE split (the subtle part):
        # The image heredoc sets dfs.namenode.rpc-bind-host=0.0.0.0 (NN
        # bind/advertise decoupled) but has NO dfs.datanode.bind-host, so
        # dfs.datanode.address is BOTH bind and advertise. It expands from
        # __HDFS_HOSTNAME__, so:
        #   * HDFS_HOSTNAME=<FQDN>  -> datanode.address resolves the FQDN via
        #     kube-dns to the Service ClusterIP; under IPVS the ClusterIP is
        #     local (kube-ipvs0) so the DN BINDS its data server to the
        #     ClusterIP. But the NN records the DN's ip_addr as the node IP
        #     (registration RPC source, docker-hdfs#8), so clients connect to
        #     <node-ip>:9866 -> Connection refused -> every write fails with
        #     '1 node(s) are excluded'. Verified broken.
        #   * HDFS_HOSTNAME=hdfs (short) -> hostAliases resolves 'hdfs' to the
        #     node IP (a real local iface) -> the DN binds its data server to
        #     <node-ip>:9866, which is exactly where the NN tells clients to
        #     connect. Verified working.
        # So HDFS_HOSTNAME MUST be the short 'hdfs' (bind to node IP). The
        # ADVERTISED FQDN for cross-namespace clients is supplied separately
        # via the two env overrides below (rpc-address is advertise-only
        # because rpc-bind-host=0.0.0.0; datanode.hostname sets only the
        # DatanodeID.hostName field).
        image: dyrnq/hdfs:latest-debian
        imagePullPolicy: IfNotPresent
        env:
          - {name: HADOOP_SECURITY_AUTHENTICATION, value: simple}
          # SHORT name -> hostAliases -> node IP. Drives the BIND addresses
          # (datanode.address/http(s).address, fs.defaultFS). Do NOT set this
          # to the FQDN here — see the BIND vs ADVERTISE comment above.
          - {name: HDFS_HOSTNAME, value: hdfs}
          # JAVA_TOOL_OPTIONS=-Djava.net.preferIPv4Stack=true fixes the
          # IPv6-only DataNode info-server bind (docker-hdfs#12); the
          # image does NOT default this, hostNetwork pods opt in.
          - {name: JAVA_TOOL_OPTIONS, value: -Djava.net.preferIPv4Stack=true}
          # ADVERTISE the FQDN so cross-namespace clients (the csi-mntrs
          # nodeplugin, which lives in a different namespace) resolve the
          # NN/DN via kube-dns to THIS namespace's Service. Without these,
          # 'hdfs' resolves in the CLIENT's namespace to the wrong Service
          # (often the kerberos one) -> hdfs-native fetches blocks from the
          # wrong DN -> EIO on every read. Single-namespace setups (k3s CI)
          # can drop both overrides and rely on the short 'hdfs'.
          # Replace <HDFS_SERVICE_FQDN> with hdfs.<NS>.svc.cluster.local.
          - {name: HDFS-SITE.XML_dfs.namenode.rpc-address, value: <HDFS_SERVICE_FQDN>:8020}
          - {name: HDFS-SITE.XML_dfs.datanode.hostname, value: <HDFS_SERVICE_FQDN>}
          # hdfs-native hardcodes the DN ip_addr from the NN's LocatedBlock,
          # ignoring dfs.datanode.hostname. The image heredoc does NOT set
          # this client knob; setting it forces the client to re-resolve the
          # DN by hostname (the FQDN above, via kube-dns -> Service -> DNAT
          # to the node IP where the data server actually listens) — required
          # for cross-node block fetch. Harmless on single-node CI.
          - {name: HDFS-SITE.XML_dfs.client.use.datanode.hostname, value: 'true'}
        ports:
          - {containerPort: 8020, name: nn-rpc, hostPort: 8020}
          - {containerPort: 9866, name: dn-data, hostPort: 9866}
          - {containerPort: 9864, name: dn-http, hostPort: 9864}
  EOF

  Then add the Service ClusterIP to /etc/hosts:
    SVC_IP=\$(kubectl -n ${HDFS_NAMESPACE} get svc hdfs -o jsonpath='{.spec.clusterIP}')
    echo \"\$SVC_IP hdfs\" >> /etc/hosts" 1
fi

HDFS_POD_PHASE=$(${KUBECTL} -n "${HDFS_NAMESPACE}" get pod hdfs -o jsonpath='{.status.phase}')
[[ "${HDFS_POD_PHASE}" == "Running" ]] || fail "HDFS Pod is not Running (phase=${HDFS_POD_PHASE})" 1

# kubectl exec defaults to root, but in simple-auth mode the HDFS superuser is
# hdfs. Use su so the process identity matches the superuser for chown/chmod/put.
if ! ${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- su - hdfs -c "/opt/hadoop/bin/hdfs dfsadmin -report" 2>&1 | grep "Live datanodes" > /dev/null; then
    fail "HDFS Pod is not ready (no live datanode)" 1
fi
log "  HDFS ready ($(${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- su - hdfs -c "/opt/hadoop/bin/hdfs dfsadmin -report" 2>&1 | grep 'Live datanodes'))"

# ---------- 1. build & push image ----------
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    log "[1/7] skip build (SKIP_BUILD=1), using ${IMAGE}"
else
    log "[1/7] building mntrs-csi (glibc)..."
    rustup target add x86_64-unknown-linux-gnu 2>/dev/null || true
    (cd "${REPO_ROOT}" && cargo build --release --package mntrs-csi --target x86_64-unknown-linux-gnu)
    cp "${REPO_ROOT}/target/x86_64-unknown-linux-gnu/release/mntrs-csi" "${REPO_ROOT}/docker/csi/mntrs-csi"

    log "  building image ${IMAGE}..."
    (cd "${REPO_ROOT}/docker/csi" && docker build -t "${IMAGE}" .)

    log "  logging in to ${REGISTRY}..."
    echo "${REGISTRY_PASS}" | docker login "${REGISTRY}" -u "${REGISTRY_USER}" --password-stdin

    log "  pushing ${IMAGE}..."
    docker push "${IMAGE}"
fi

# ---------- 2. deploy csi-mntrs ----------
if [[ "${SKIP_DEPLOY:-0}" == "1" ]]; then
    log "[2/7] skip deploy (SKIP_DEPLOY=1)"
else
    log "[2/7] deploying csi-mntrs to ${CSI_NAMESPACE}..."

    DEPLOY_DIR="$(mktemp -d)"
    SRC="${REPO_ROOT}/csi/deploy/kubernetes/1.20"
    for f in 00-namespace.yaml 01-csidriver.yaml 02-controller-rbac.yaml \
             03-controller.yaml 04-nodeplugin-rbac.yaml 05-nodeplugin.yaml \
             06-storageclass.yaml; do
        sed "s|image: csi-mntrs:latest|image: ${IMAGE}|" \
            "${SRC}/${f}" > "${DEPLOY_DIR}/${f}"
    done
    # Inject imagePullSecrets into the pod spec of the StatefulSet and
    # DaemonSet so the apply below lands with the secret already wired up.
    for f in 03-controller.yaml 05-nodeplugin.yaml; do
        sed -i "/^      serviceAccountName: csi-/a\\
      imagePullSecrets:\\
        - name: reg-e2e" \
            "${DEPLOY_DIR}/${f}"
    done

    # Apply 00-namespace.yaml first so the reg-e2e secret we create next
    # lands in a real namespace. Then create the secret, then apply the
    # rest of the manifests.
    log "  creating namespace ${CSI_NAMESPACE}..."
    ${KUBECTL} apply -f "${DEPLOY_DIR}/00-namespace.yaml"

    log "  creating docker-registry secret reg-e2e in ${CSI_NAMESPACE}..."
    ${KUBECTL} -n "${CSI_NAMESPACE}" create secret docker-registry reg-e2e \
        --docker-server="${REGISTRY}" \
        --docker-username="${REGISTRY_USER}" \
        --docker-password="${REGISTRY_PASS}" \
        --docker-email=e2e@example.com \
        --dry-run=client -o yaml | ${KUBECTL} apply -f -

    ${KUBECTL} apply -f "${DEPLOY_DIR}/"

    log "  waiting for csi-mntrs pods Ready..."
    for label in app=csi-controller-mntrs app=csi-nodeplugin-mntrs; do
        for i in $(seq 1 30); do
            if ${KUBECTL} -n "${CSI_NAMESPACE}" get pod -l "$label" -o name 2>/dev/null | grep . > /dev/null; then
                break
            fi
            sleep 1
        done
        ${KUBECTL} -n "${CSI_NAMESPACE}" wait --for=condition=Ready pod -l "$label" --timeout=120s
    done
fi

# ---------- 3. create test data in HDFS ----------
log "[3/7] creating test data in HDFS..."
# The HDFS Pod boots with drwxr-xr-x on / (owner hdfs:supergroup).
# With simple auth, opendal authenticates as the OS user (root inside its
# container), so it can't write to / without this chmod. See
# tests/e2e/common/hdfs-prep.sh for the shared prep rationale.
# shellcheck source=tests/e2e/common/hdfs-prep.sh
. "$(cd "$(dirname "$0")" && pwd)/../common/hdfs-prep.sh"
hdfs_prep_kubectl_simple "${HDFS_NAMESPACE}"
echo "hello from csi hdfs e2e" | ${KUBECTL} -n "${HDFS_NAMESPACE}" exec -i hdfs -- su - hdfs -c "/opt/hadoop/bin/hdfs dfs -put -f - /test/pre-existing.txt" 2>/dev/null
${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- su - hdfs -c "/opt/hadoop/bin/hdfs dfs -chmod 644 /test/pre-existing.txt" 2>/dev/null || true
${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- su - hdfs -c "/opt/hadoop/bin/hdfs dfs -ls /test/" 2>/dev/null
log "  test data ready"

# ---------- 4. create PV + PVC + test pod ----------
log "[4/7] creating PV + PVC + test pod..."

cat <<EOF | ${KUBECTL} apply -f -
apiVersion: v1
kind: PersistentVolume
metadata: {name: ${E2E_PV_NAME}, labels: {name: ${E2E_PV_NAME}}}
spec:
  accessModes: [ReadWriteMany]
  capacity: {storage: 10Gi}
  storageClassName: mntrs
  csi:
    driver: csi-mntrs
    volumeHandle: ${E2E_VOLUME_ID}
    volumeAttributes:
      storage: "${STORAGE_URL}"
      dfs.client.use.datanode.hostname: "true"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ${E2E_PVC_NAME}, namespace: ${E2E_NAMESPACE}}
spec:
  accessModes: [ReadWriteMany]
  resources: {requests: {storage: 10Gi}}
  storageClassName: mntrs
  selector: {matchLabels: {name: ${E2E_PV_NAME}}}
---
apiVersion: v1
kind: Pod
metadata: {name: ${E2E_POD_NAME}, namespace: ${E2E_NAMESPACE}}
spec:
  restartPolicy: Never
  ${NODE_SELECTOR}
  containers:
  - name: test
    image: busybox:1.37
    command: ["sh", "-c", "sleep 600"]
    volumeMounts: [{name: data, mountPath: /data}]
  volumes:
  - {name: data, persistentVolumeClaim: {claimName: ${E2E_PVC_NAME}}}
EOF
log "  waiting for test pod Ready..."
${KUBECTL} -n "${E2E_NAMESPACE}" wait --for=condition=Ready pod/"${E2E_POD_NAME}" --timeout=120s

# ---------- 5. run e2e tests ----------
log "[5/7] running e2e tests in pod..."
PASS=0
FAIL=0

assert_in() {
    local desc="$1" needle="$2" haystack="$3"
    if echo "${haystack}" | grep -qF "${needle}"; then
        pass "  ${desc}"
        PASS=$((PASS+1))
    else
        echo "  expected: ${needle}" >&2
        echo "  actual:   ${haystack}" >&2
        fail "  ${desc}" 2
    fi
}

LS_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- ls -la /data)
assert_in "FUSE mount shows test directory" "test" "${LS_OUT}"

CAT_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- cat /data/test/pre-existing.txt)
assert_in "read pre-existing file" "hello from csi hdfs e2e" "${CAT_OUT}"

${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "echo 'hello from csi e2e' > /data/_ci_small.txt" >/dev/null
WRITE_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- cat /data/_ci_small.txt)
assert_in "write + read back small file" "hello from csi e2e" "${WRITE_OUT}"

${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "echo 'appended' >> /data/_ci_small.txt" >/dev/null
APPEND_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- cat /data/_ci_small.txt)
assert_in "append works" "appended" "${APPEND_OUT}"

${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "dd if=/dev/urandom of=/data/_ci_1m.bin bs=1M count=1 2>/dev/null && \
           dd if=/data/_ci_1m.bin of=/dev/null bs=64K 2>/dev/null && echo OK" >/dev/null
DD_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "dd if=/data/_ci_1m.bin of=/dev/null bs=64K 2>&1 | tail -1")
assert_in "1M random write+read" "bytes" "${DD_OUT}"

${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "rm -f /data/_ci_small.txt /data/_ci_1m.bin" >/dev/null
LS_FINAL=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- ls -la /data)
assert_in "cleanup removed test files" "test" "${LS_FINAL}"
if echo "${LS_FINAL}" | grep -q "_ci_"; then
    fail "  cleanup left _ci_* files behind" 2
fi
pass "  cleanup removed _ci_* files"

# ---------- 6. dynamic provision e2e ----------
log "[6/7] dynamic provision e2e (StorageClass.parameters + CreateVolume)..."
${KUBECTL} -n "${E2E_NAMESPACE}" delete pod "${E2E_POD_NAME}" --force --grace-period=0 --ignore-not-found 2>/dev/null
${KUBECTL} -n "${E2E_NAMESPACE}" delete pvc "${E2E_PVC_NAME}" --ignore-not-found 2>/dev/null
${KUBECTL} delete pv "${E2E_PV_NAME}" --ignore-not-found 2>/dev/null

DYN_SC_NAME="mntrs-dyn-hdfs-e2e"
DYN_PVC_NAME="mntrs-csi-e2e-hdfs-dyn"
# shellcheck disable=SC2034  # reserved for future use
DYN_PV_NAME="mntrs-csi-e2e-hdfs-dyn-pv"
DYN_POD_NAME="mntrs-csi-e2e-hdfs-dyn"

log "  creating StorageClass ${DYN_SC_NAME} with parameters..."
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: ${DYN_SC_NAME}}
provisioner: csi-mntrs
reclaimPolicy: Delete
volumeBindingMode: Immediate
parameters:
  storage: "${STORAGE_URL}"
  dfs.client.use.datanode.hostname: "true"
EOF

log "  creating PVC ${DYN_PVC_NAME} (no selector -> K8s will dynamic-provision)..."
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ${DYN_PVC_NAME}, namespace: ${E2E_NAMESPACE}}
spec:
  accessModes: [ReadWriteMany]
  resources: {requests: {storage: 1Gi}}
  storageClassName: ${DYN_SC_NAME}
EOF

log "  waiting for dynamic PV creation + binding..."
for i in $(seq 1 30); do
    PV_NAME=$(${KUBECTL} -n "${E2E_NAMESPACE}" get pvc "${DYN_PVC_NAME}" -o jsonpath='{.spec.volumeName}' 2>/dev/null)
    PV_PHASE=$(${KUBECTL} get pv "${PV_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null)
    if [[ "${PV_PHASE}" == "Bound" ]]; then
        log "  dynamic PV ${PV_NAME} bound in ${i}s"
        break
    fi
    sleep 2
done
[[ "${PV_PHASE}" == "Bound" ]] || fail "dynamic PV never bound (last phase=${PV_PHASE})"

# Verify the dynamically-created PV has the right driver and volumeAttributes
PV_DRIVER=$(${KUBECTL} get pv "${PV_NAME}" -o jsonpath='{.spec.csi.driver}')
assert_in "dynamic PV uses csi-mntrs driver" "csi-mntrs" "${PV_DRIVER}"
PV_HANDLE=$(${KUBECTL} get pv "${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
log "  volumeHandle: ${PV_HANDLE}"
[[ -n "${PV_HANDLE}" ]] || fail "dynamic PV has empty volumeHandle"

log "  creating test pod ${DYN_POD_NAME}..."
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: v1
kind: Pod
metadata: {name: ${DYN_POD_NAME}, namespace: ${E2E_NAMESPACE}}
spec:
  restartPolicy: Never
  ${NODE_SELECTOR}
  containers:
  - name: test
    image: busybox:1.37
    command: ["sh", "-c", "sleep 600"]
    volumeMounts: [{name: data, mountPath: /data}]
  volumes:
  - {name: data, persistentVolumeClaim: {claimName: ${DYN_PVC_NAME}}}
EOF
${KUBECTL} -n "${E2E_NAMESPACE}" wait --for=condition=Ready pod/"${DYN_POD_NAME}" --timeout=300s

# Re-run the same read/write suite against the dynamic volume
LS_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- ls -la /data)
assert_in "dynamic: FUSE mount shows test directory" "test" "${LS_OUT}"
CAT_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- cat /data/test/pre-existing.txt)
assert_in "dynamic: read pre-existing file" "hello from csi hdfs e2e" "${CAT_OUT}"
${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "echo 'hello from csi e2e' > /data/_ci_small.txt" >/dev/null
WRITE_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- cat /data/_ci_small.txt)
assert_in "dynamic: write + read back" "hello from csi e2e" "${WRITE_OUT}"
${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "echo 'appended' >> /data/_ci_small.txt" >/dev/null
APPEND_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- cat /data/_ci_small.txt)
assert_in "dynamic: append" "appended" "${APPEND_OUT}"
${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "dd if=/dev/urandom of=/data/_ci_1m.bin bs=1M count=1 2>/dev/null && \
           dd if=/data/_ci_1m.bin of=/dev/null bs=64K 2>/dev/null && echo OK" >/dev/null
DD_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "dd if=/data/_ci_1m.bin of=/dev/null bs=64K 2>&1 | tail -1")
assert_in "dynamic: 1M random write+read" "bytes" "${DD_OUT}"
${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "rm -f /data/_ci_small.txt /data/_ci_1m.bin" >/dev/null

# ---------- 7. summary ----------
log "[7/7] e2e done: ${PASS} passed, ${FAIL} failed"
trap - EXIT  # disable cleanup, leave resources for inspection if requested
[[ "${KEEP_ON_FAIL:-0}" == "1" && $FAIL -gt 0 ]] || exit 0
exit 2
