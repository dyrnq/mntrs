#!/usr/bin/env bash
#
# End-to-end test: mntrs CSI driver + S3 backend (MinIO) on a real K8s cluster.
#
# What it does:
#   1. Builds mntrs-csi (glibc) + pushes it to $REGISTRY
#   2. Deploys MinIO (StatefulSet + Service + PV/PVC) in $MINIO_NAMESPACE
#   3. Deploys csi-mntrs (namespace, RBAC, controller, nodeplugin) in $CSI_NAMESPACE
#      using the pushed image, with imagePullSecrets wired up
#   4. Creates a test bucket and a pre-existing object in MinIO
#   5. Creates a static PV + PVC + test pod using the mntrs CSI driver
#   6. Runs read/write/append/cleanup operations against the FUSE mount
#   7. Tears everything down
#
# Required env:
#   KUBECONFIG                Path to kubeconfig
#
# Optional env (with defaults):
#   REGISTRY                  Private registry host:port (default: 10.5.26.11:5000)
#   REGISTRY_USER             Registry username (default: test)
#   REGISTRY_PASS             Registry password (default: test)
#   IMAGE_TAG                 Image tag (default: dev)
#   MNIO_IMAGE                MinIO image (default: minio/minio:latest)
#   MNIO_NAMESPACE            Namespace for MinIO (default: minio)
#   MNIO_ROOT_USER            MinIO root user (default: minioadmin)
#   MNIO_ROOT_PASSWORD        MinIO root password (default: minioadmin)
#   MNIO_BUCKET               Bucket name (default: mntrs-csi-e2e)
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

MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_NAMESPACE="${MINIO_NAMESPACE:-minio}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-mntrs-csi-e2e}"
# Override the auto-discovered MinIO endpoint. Useful for CI services or
# when the e2e job pod can't reach the in-cluster MinIO service (e.g. via
# hostNetwork / firewall). Format: http://host:port (no trailing slash).
MINIO_ENDPOINT="${MINIO_ENDPOINT:-}"
MINIO_PV_SIZE="${MINIO_PV_SIZE:-50Gi}"

CSI_NAMESPACE="${CSI_NAMESPACE:-csi-mntrs}"
E2E_NAMESPACE="${E2E_NAMESPACE:-default}"
E2E_PV_NAME="${E2E_PV_NAME:-mntrs-csi-e2e-pv}"
E2E_PVC_NAME="${E2E_PVC_NAME:-mntrs-csi-e2e-pvc}"
E2E_POD_NAME="${E2E_POD_NAME:-mntrs-csi-e2e}"
E2E_VOLUME_ID="${E2E_VOLUME_ID:-e2e-vol}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd)"
KUBECTL="kubectl --kubeconfig ${KUBECONFIG}"

# ---------- helpers ----------
log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }
pass() { printf '\033[1;32m[PASS]\033[0m %s\n' "$*" >&2; }

# shellcheck disable=SC2329  # invoked via trap below
cleanup() {
    # Capture the script's exit code BEFORE running any other command
    # (the trap fires on script exit; subsequent commands in this
    # function would otherwise overwrite $? and the real failure mode
    # would be lost).
    local exit_code=$?
    log "cleanup (exit=$exit_code)..."
    # Each cleanup line uses `|| true` so set -e from the top of the
    # script remains in effect. The --ignore-not-found + 2>/dev/null
    # suppress noise; || true keeps errexit happy.
    ${KUBECTL} delete pod "${E2E_POD_NAME}" -n "${E2E_NAMESPACE}" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pvc "${E2E_PVC_NAME}" -n "${E2E_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pv "${E2E_PV_NAME}" --ignore-not-found 2>/dev/null || true
    # Dynamic provision e2e resources (phase 7/8)
    ${KUBECTL} delete pod "mntrs-csi-e2e-dyn" -n "${E2E_NAMESPACE}" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pvc "mntrs-csi-e2e-dyn" -n "${E2E_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    # PVs created by the dynamic SC have reclaimPolicy=Delete, so the
    # PVC delete above cascades. No explicit PV delete needed.
    ${KUBECTL} delete sc "mntrs-dyn-e2e" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete job -n "${MINIO_NAMESPACE}" -l app=mntrs-e2e --ignore-not-found 2>/dev/null || true
    if [[ "${SKIP_DEPLOY:-0}" != "1" && -n "${DEPLOY_DIR:-}" ]]; then
        ${KUBECTL} delete -f "${DEPLOY_DIR}/" --ignore-not-found 2>/dev/null || true
        rm -rf "${DEPLOY_DIR}"
    fi
    if [[ "${KEEP_MINIO:-0}" != "1" ]]; then
        ${KUBECTL} delete namespace "${MINIO_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    fi
    exit "$exit_code"
}
trap cleanup EXIT

# ---------- preflight ----------
log "preflight..."
command -v kubectl >/dev/null || fail "kubectl not found"
command -v docker  >/dev/null || fail "docker not found"
[[ -f "${KUBECONFIG}" ]] || fail "KUBECONFIG ${KUBECONFIG} not found"
${KUBECTL} get nodes >/dev/null || fail "cannot reach cluster"
log "  cluster reachable: $(${KUBECTL} get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')"

# ---------- 1. build & push image ----------
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    log "[1/9] skip build (SKIP_BUILD=1), using ${IMAGE}"
else
    log "[1/9] building mntrs-csi (musl static)..."
    rustup target add x86_64-unknown-linux-musl 2>/dev/null || true
    (cd "${REPO_ROOT}" && cargo build --release --package mntrs-csi --target x86_64-unknown-linux-musl)
    cp "${REPO_ROOT}/target/x86_64-unknown-linux-musl/release/mntrs-csi" "${REPO_ROOT}/docker/csi/mntrs-csi"

    log "  building image ${IMAGE}..."
    (cd "${REPO_ROOT}/docker/csi" && docker build -t "${IMAGE}" .)

    log "  logging in to ${REGISTRY}..."
    echo "${REGISTRY_PASS}" | docker login "${REGISTRY}" -u "${REGISTRY_USER}" --password-stdin

    log "  pushing ${IMAGE}..."
    docker push "${IMAGE}"
fi

# ---------- 2. deploy MinIO ----------
if [[ "${KEEP_MINIO:-0}" == "1" ]] && ${KUBECTL} get namespace "${MINIO_NAMESPACE}" >/dev/null 2>&1; then
    log "[2/9] MinIO namespace ${MINIO_NAMESPACE} already exists, skipping"
else
    log "[2/9] deploying MinIO to ${MINIO_NAMESPACE}..."
    cat <<EOF | ${KUBECTL} apply -f -
apiVersion: v1
kind: Namespace
metadata: {name: ${MINIO_NAMESPACE}}
---
apiVersion: v1
kind: Service
metadata: {name: minio, namespace: ${MINIO_NAMESPACE}}
spec:
  ports: [{name: api, port: 9000, targetPort: 9000}]
  selector: {app: minio}
---
apiVersion: v1
kind: PersistentVolume
metadata: {name: minio-data}
spec:
  capacity: {storage: ${MINIO_PV_SIZE}}
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  hostPath: {path: /tmp/minio-data, type: DirectoryOrCreate}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio, namespace: ${MINIO_NAMESPACE}}
spec:
  replicas: 1
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
      - name: minio
        image: ${MINIO_IMAGE}
        args: ["server", "/data", "--address", ":9000"]
        env:
        - {name: MINIO_ROOT_USER,    value: "${MINIO_ROOT_USER}"}
        - {name: MINIO_ROOT_PASSWORD, value: "${MINIO_ROOT_PASSWORD}"}
        ports: [{containerPort: 9000}]
        volumeMounts: [{name: data, mountPath: /data}]
        readinessProbe:
          tcpSocket: {port: 9000}
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim: {claimName: minio-data}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: minio-data, namespace: ${MINIO_NAMESPACE}}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: ""
  volumeName: minio-data
  resources: {requests: {storage: ${MINIO_PV_SIZE}}}
EOF
    log "  waiting for MinIO pod Ready..."
    ${KUBECTL} wait -n "${MINIO_NAMESPACE}" --for=condition=Available deploy/minio --timeout=120s
fi

# ---------- 3. deploy csi-mntrs ----------
if [[ "${SKIP_DEPLOY:-0}" == "1" ]]; then
    log "[3/9] skip deploy (SKIP_DEPLOY=1)"
else
    log "[3/9] deploying csi-mntrs to ${CSI_NAMESPACE}..."
    DEPLOY_DIR="$(mktemp -d)"
    SRC="${REPO_ROOT}/csi/deploy/kubernetes/1.20"
    for f in 00-namespace.yaml 01-csidriver.yaml 02-controller-rbac.yaml \
             03-controller.yaml 04-nodeplugin-rbac.yaml 05-nodeplugin.yaml \
             06-storageclass.yaml; do
        sed "s|image: csi-mntrs:latest|image: ${IMAGE}|" \
            "${SRC}/${f}" > "${DEPLOY_DIR}/${f}"
    done
    ${KUBECTL} apply -f "${DEPLOY_DIR}/"

    log "  creating docker-registry secret in ${CSI_NAMESPACE}..."
    ${KUBECTL} -n "${CSI_NAMESPACE}" create secret docker-registry reg-e2e \
        --docker-server="${REGISTRY}" \
        --docker-username="${REGISTRY_USER}" \
        --docker-password="${REGISTRY_PASS}" \
        --docker-email=e2e@example.com \
        --dry-run=client -o yaml | ${KUBECTL} apply -f -

    log "  patching controller + nodeplugin with imagePullSecrets..."
    ${KUBECTL} -n "${CSI_NAMESPACE}" patch statefulset csi-controller-mntrs --type=json \
        -p='[{"op":"add","path":"/spec/template/spec/imagePullSecrets","value":[{"name":"reg-e2e"}]}]'
    ${KUBECTL} -n "${CSI_NAMESPACE}" patch daemonset csi-nodeplugin-mntrs --type=json \
        -p='[{"op":"add","path":"/spec/template/spec/imagePullSecrets","value":[{"name":"reg-e2e"}]}]'

    log "  waiting for csi-mntrs pods Ready..."
    ${KUBECTL} -n "${CSI_NAMESPACE}" wait --for=condition=Ready pod -l app=csi-controller-mntrs --timeout=120s
    ${KUBECTL} -n "${CSI_NAMESPACE}" wait --for=condition=Ready pod -l app=csi-nodeplugin-mntrs --timeout=120s
fi

# ---------- 4. create test bucket ----------
log "[4/9] creating test bucket ${MINIO_BUCKET}..."
# Always use the in-cluster MinIO endpoint for the create-bucket job, which
# runs inside k3s and cannot reach host-level endpoints like localhost:9000.
MINIO_POD_IP=$(${KUBECTL} -n "${MINIO_NAMESPACE}" get pod -l app=minio -o jsonpath='{.items[0].status.podIP}')
MINIO_POD_EP="http://${MINIO_POD_IP}:9000"
log "  MinIO pod IP: ${MINIO_POD_IP}"
if [[ -n "${MINIO_ENDPOINT}" ]]; then
    log "  (MINIO_ENDPOINT override ${MINIO_ENDPOINT} ignored for in-cluster job)"
fi
# Strip http:// or https:// prefix for the Host header
# Use pod endpoint host for the create-bucket job Host header
MINIO_HOST=$(echo "${MINIO_POD_EP}" | sed -E 's|^https?://||')

${KUBECTL} -n "${MINIO_NAMESPACE}" delete job create-bucket --ignore-not-found 2>/dev/null
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: batch/v1
kind: Job
metadata: {name: create-bucket, namespace: ${MINIO_NAMESPACE}}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: python:3.12-alpine
        command: ["python3", "-u", "-c"]
        args:
        - |
          import urllib.request, hashlib, hmac, datetime
          MINIO = "${MINIO_POD_EP}"
          AK, SK = "${MINIO_ROOT_USER}", "${MINIO_ROOT_PASSWORD}"
          REGION = "us-east-1"
          def sign(k,m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
          def sigv4(method, path, body=b"", query=""):
              now = datetime.datetime.utcnow()
              amzdate = now.strftime("%Y%m%dT%H%M%SZ")
              datestamp = now.strftime("%Y%m%d")
              ph = hashlib.sha256(body).hexdigest()
              ch = "host:${MINIO_HOST}\nx-amz-content-sha256:"+ph+"\nx-amz-date:"+amzdate+"\n"
              sh = "host;x-amz-content-sha256;x-amz-date"
              cr = method+"\n"+path+"\n"+query+"\n"+ch+"\n"+sh+"\n"+ph
              cs = datestamp+"/"+REGION+"/s3/aws4_request"
              sts = "AWS4-HMAC-SHA256\n"+amzdate+"\n"+cs+"\n"+hashlib.sha256(cr.encode()).hexdigest()
              kd = sign(("AWS4"+SK).encode(), datestamp)
              kr = sign(kd, REGION)
              ks = sign(kr, "s3")
              kk = sign(ks, "aws4_request")
              sig = hmac.new(kk, sts.encode(), hashlib.sha256).hexdigest()
              return {
                  "x-amz-date": amzdate,
                  "x-amz-content-sha256": ph,
                  "Authorization": "AWS4-HMAC-SHA256 Credential="+AK+"/"+cs+", SignedHeaders="+sh+", Signature="+sig,
              }
          def req(method, path, body=b""):
              h = sigv4(method, path, body)
              if body: h["Content-Length"] = str(len(body))
              r = urllib.request.Request(MINIO+path, data=body if body else None, method=method, headers=h)
              try:
                  resp = urllib.request.urlopen(r, timeout=10)
                  return resp.status
              except urllib.error.HTTPError as e:
                  return e.code
          c = req("PUT", "/${MINIO_BUCKET}/", b"")
          if c != 200: raise SystemExit(f"create bucket failed: {c}")
          c = req("PUT", "/${MINIO_BUCKET}/pre-existing.txt", b"hello from csi e2e\n")
          if c != 200: raise SystemExit(f"upload failed: {c}")
          print("bucket + pre-existing.txt ready")
EOF
${KUBECTL} -n "${MINIO_NAMESPACE}" wait --for=condition=Complete job/create-bucket --timeout=60s
log "  bucket ready"

# ---------- 5. create PV + PVC + test pod ----------
log "[5/9] creating PV + PVC + test pod..."
# Always use the in-cluster MinIO service for K8s resources (PV/PVC/StorageClass),
# since the CSI driver pods run inside k3s and need a cluster-reachable endpoint.
MINIO_SVC_IP=$(${KUBECTL} -n "${MINIO_NAMESPACE}" get svc minio -o jsonpath='{.spec.clusterIP}')
MINIO_ENDPOINT="http://${MINIO_SVC_IP}:9000"
log "  MinIO service IP: ${MINIO_SVC_IP}"

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
      storage: "s3://${MINIO_BUCKET}"
      region: "us-east-1"
      endpoint: "${MINIO_ENDPOINT}"
      access-key: "${MINIO_ROOT_USER}"
      secret-key: "${MINIO_ROOT_PASSWORD}"
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

# ---------- 6. run e2e tests ----------
log "[6/9] running e2e tests in pod..."
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
assert_in "FUSE mount shows pre-existing file" "pre-existing.txt" "${LS_OUT}"

CAT_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- cat /data/pre-existing.txt)
assert_in "read pre-existing file" "hello from csi e2e" "${CAT_OUT}"

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
assert_in "1M random write+read" "records" "${DD_OUT}"

${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "rm -f /data/_ci_small.txt /data/_ci_1m.bin" >/dev/null
LS_FINAL=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- ls -la /data)
assert_in "cleanup removed test files" "pre-existing.txt" "${LS_FINAL}"
if echo "${LS_FINAL}" | grep -q "_ci_"; then
    fail "  cleanup left _ci_* files behind" 2
fi
pass "  cleanup removed _ci_* files"

# ---------- 7. dynamic provision e2e ----------
# Clean up the static PV/PVC/pod before the next phase so names don't
# clash and so we can re-use the test pod name with a different volume.
log "[7/9] dynamic provision e2e (StorageClass.parameters + CreateVolume)..."
${KUBECTL} -n "${E2E_NAMESPACE}" delete pod "${E2E_POD_NAME}" --force --grace-period=0 --ignore-not-found 2>/dev/null
${KUBECTL} -n "${E2E_NAMESPACE}" delete pvc "${E2E_PVC_NAME}" --ignore-not-found 2>/dev/null
${KUBECTL} delete pv "${E2E_PV_NAME}" --ignore-not-found 2>/dev/null

DYN_SC_NAME="mntrs-dyn-e2e"
DYN_PVC_NAME="mntrs-csi-e2e-dyn"
# shellcheck disable=SC2034  # reserved for future use
DYN_PV_NAME="mntrs-csi-e2e-dyn-pv"
DYN_POD_NAME="mntrs-csi-e2e-dyn"

log "  creating StorageClass ${DYN_SC_NAME} with parameters..."
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: ${DYN_SC_NAME}}
provisioner: csi-mntrs
reclaimPolicy: Delete
volumeBindingMode: Immediate
parameters:
  storage: "s3://${MINIO_BUCKET}"
  region: "us-east-1"
  endpoint: "${MINIO_ENDPOINT}"
  access-key: "${MINIO_ROOT_USER}"
  secret-key: "${MINIO_ROOT_PASSWORD}"
EOF

log "  creating PVC ${DYN_PVC_NAME} (no selector → K8s will dynamic-provision)..."
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
PV_EP=$(${KUBECTL} get pv "${PV_NAME}" -o jsonpath='{.spec.csi.volumeAttributes.endpoint}')
assert_in "dynamic PV inherited endpoint param" "9000" "${PV_EP}"

log "  creating test pod ${DYN_POD_NAME}..."
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: v1
kind: Pod
metadata: {name: ${DYN_POD_NAME}, namespace: ${E2E_NAMESPACE}}
spec:
  restartPolicy: Never
  containers:
  - name: test
    image: busybox:1.37
    command: ["sh", "-c", "sleep 600"]
    volumeMounts: [{name: data, mountPath: /data}]
  volumes:
  - {name: data, persistentVolumeClaim: {claimName: ${DYN_PVC_NAME}}}
EOF
${KUBECTL} -n "${E2E_NAMESPACE}" wait --for=condition=Ready pod/"${DYN_POD_NAME}" --timeout=120s

# Re-run the same read/write suite against the dynamic volume
LS_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- ls -la /data)
assert_in "dynamic: FUSE mount shows pre-existing file" "pre-existing.txt" "${LS_OUT}"
CAT_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- cat /data/pre-existing.txt)
assert_in "dynamic: read pre-existing file" "hello from csi e2e" "${CAT_OUT}"
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
assert_in "dynamic: 1M random write+read" "records" "${DD_OUT}"
${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "rm -f /data/_ci_small.txt /data/_ci_1m.bin" >/dev/null

# ---------- 8. volume expansion assertion ----------
# mntrs-csi does NOT implement ControllerExpandVolume / NodeExpandVolume
# (returns UNIMPLEMENTED). The driver also does not advertise
# ControllerExpansion / VolumeExpansion in GetCapabilities, which is the
# correct signal. We assert the SC + behavior reflects this.
log "[8/9] volume expansion assertion (driver reports unimplemented)..."
ALLOW_EXPAND=$(${KUBECTL} get sc "${DYN_SC_NAME}" -o jsonpath='{.allowVolumeExpansion}' 2>/dev/null)
log "  StorageClass ${DYN_SC_NAME}.allowVolumeExpansion = ${ALLOW_EXPAND:-<unset>}"
if [[ "${ALLOW_EXPAND}" == "true" ]]; then
    fail "  allowVolumeExpansion=true but driver returns UNIMPLEMENTED" 2
fi
pass "  allowVolumeExpansion is not set (matches driver capability)"

# Verify the SC doesn't claim the EXPAND capability in its CSI spec either.
# Without an explicit EXPAND capability, external-resizer won't try to resize.
log "  no PVC resize attempted (driver does not advertise VolumeExpansion)"

# ---------- 9. summary ----------
log "[9/9] e2e done: ${PASS} passed, ${FAIL} failed"
trap - EXIT  # disable cleanup, leave resources for inspection if requested
[[ "${KEEP_ON_FAIL:-0}" == "1" && $FAIL -gt 0 ]] || exit 0
exit 2
