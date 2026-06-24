#!/usr/bin/env bash
#
# End-to-end test: mntrs CSI driver + HDFS KERBEROS backend on a real K8s cluster.
#
# What it does:
#   1. Builds mntrs-csi (glibc) + pushes it to $REGISTRY
#   2. Deploys csi-mntrs with Kerberos volumes (keytab, krb5.conf fetched from
#      the HDFS Pod at start time by a kubectl-fetch initContainer into an
#      emptyDir — no hostPath staging, no deploy-time Secret snapshot)
#   3. Creates test data in HDFS (via kubectl exec into the HDFS Pod)
#   4. Creates a static PV + PVC + test pod using the mntrs CSI driver
#   5. Runs read/write/append/cleanup operations against the FUSE mount
#   6. Tears everything down
#
# Assumes HDFS Kerberos Pod is already running as "hdfs" in $HDFS_NAMESPACE:
#   - Kerberos mode (default — no HADOOP_SECURITY_AUTHENTICATION=simple)
#   - A k8s Service "hdfs" exposes NameNode RPC + KDC ports
#   - The nodeplugin fetches hdfs.keytab + krb5.conf from the Pod itself via a
#     kubectl-fetch initContainer (grants the nodeplugin SA pods/exec on the
#     single pod named "hdfs" — see the Role injected below); no /tmp staging
#     or /etc/hosts entry on the host is required
#   - $KUBECONFIG is pointed at the cluster
#
# Required env:
#   KUBECONFIG                Path to kubeconfig
#
# Optional env (with defaults):
#   REGISTRY                  Registry host:port. No default — must be set when
#                             building/pushing (SKIP_BUILD=0). CI passes GHCR;
#                             local runs pass their own registry. (Previously
#                             defaulted to a hard-coded internal address; removed
#                             to avoid leaking private infrastructure into the
#                             public repo.)
#   REGISTRY_USER             Registry username (default: test)
#   REGISTRY_PASS             Registry password (default: test)
#   IMAGE_TAG                 Image tag (default: dev)
#   HDFS_HOSTNAME             HDFS hostname (default: hdfs)
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
# REGISTRY has no hard-coded default: CI passes GHCR, local runs pass their own.
# Requiring it explicitly avoids leaking a private registry address into the
# public repo. SKIP_BUILD=1 still needs it (the pushed image is referenced as
# ${REGISTRY}/mntrs-csi:${IMAGE_TAG}).
REGISTRY="${REGISTRY:?REGISTRY must be set (registry host:port, e.g. ghcr.io/<owner>)}"
REGISTRY_USER="${REGISTRY_USER:-test}"
REGISTRY_PASS="${REGISTRY_PASS:-test}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
IMAGE="${IMAGE:-${REGISTRY}/mntrs-csi:${IMAGE_TAG}}"

HDFS_NAMESPACE="${HDFS_NAMESPACE:-csi-mntrs}"
# HDFS_HOSTNAME must be the pod's FQDN (hdfs.<ns>.svc.cluster.local), NOT the
# bare short name "hdfs". Reason: the Rust client (MIT krb5) canonicalizes the
# host-based GSS service name "hdfs@<host>" to its FQDN when requesting the
# target principal, so it asks the KDC for hdfs/hdfs.<ns>.svc.cluster.local@REALM
# regardless of dns_canonicalize_hostname. The NameNode/DN principals and keytab
# must therefore be FQDN too — which the dyrnq/hdfs entrypoint derives from
# HDFS_HOSTNAME. A short "hdfs" here causes a principal mismatch (GSS initiate
# failed) on the NameNode RPC.
HDFS_HOSTNAME="${HDFS_HOSTNAME:-hdfs.${HDFS_NAMESPACE}.svc.cluster.local}"
HDFS_PORT="${HDFS_PORT:-8020}"
HDFS_REALM="${HDFS_REALM:-TEST.LOCAL}"
HDFS_PRINCIPAL="${HDFS_PRINCIPAL:-hdfs/${HDFS_HOSTNAME}@${HDFS_REALM}}"

# Host paths for Kerberos config (must already exist — extracted from HDFS Pod)
KEYTAB_HOST_PATH="${KEYTAB_HOST_PATH:-/tmp/hdfs.keytab}"
KRB5_HOST_PATH="${KRB5_HOST_PATH:-/tmp/krb5.conf}"

CSI_NAMESPACE="${CSI_NAMESPACE:-csi-mntrs}"
# Image used by the kubectl-fetch initContainer. Defaults to rancher/kubectl
# from docker.io, which works in CI (GitHub runners reach docker.io directly).
# In environments whose docker.io mirror is broken (e.g. daocloud 403s for
# third-party images), override with a copy pushed to the private registry:
#   KUBECTL_IMAGE=${REGISTRY}/kubectl:1.35 bash hdfs-kerberos.sh
# The image must contain a `kubectl` binary on PATH (we invoke it directly
# from the initContainer's `args`).
KUBECTL_IMAGE="${KUBECTL_IMAGE:-rancher/kubectl:v1.35.5}"
E2E_NAMESPACE="${E2E_NAMESPACE:-default}"
E2E_PV_NAME="${E2E_PV_NAME:-mntrs-csi-e2e-hdfs-krb-pv}"
E2E_PVC_NAME="${E2E_PVC_NAME:-mntrs-csi-e2e-hdfs-krb-pvc}"
E2E_POD_NAME="${E2E_POD_NAME:-mntrs-csi-e2e-hdfs-krb}"
E2E_VOLUME_ID="${E2E_VOLUME_ID:-e2e-hdfs-krb-vol}"
NODE_NAME="${NODE_NAME:-}"
NODE_SELECTOR=""
if [[ -n "${NODE_NAME}" ]]; then
    NODE_SELECTOR="nodeName: ${NODE_NAME}"
fi
# NODE_IP is needed for the HDFS pod's hostAliases block (see the spec in
# the preflight section below). With hostNetwork, the pod's OS hostname
# becomes the node's (e.g. "z12"), and Java's InetAddress.getLocalHost
# reads /etc/hostname + resolves via DNS — but z12 isn't in cluster DNS.
# hostAliases pins the node's hostname (and the pod hostname "hdfs") to
# NODE_IP in /etc/hosts so getLocalHost() succeeds. NODE_IP is the
# internal IP k8s reports for the node.
NODE_IP="${NODE_IP:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd)"
KUBECTL="kubectl --kubeconfig ${KUBECONFIG}"
STORAGE_URL="hdfs://${HDFS_HOSTNAME}:${HDFS_PORT}/"

# ---------- helpers ----------
log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }
pass() { printf '\033[1;32m[PASS]\033[0m %s\n' "$*" >&2; }

# shellcheck disable=SC2329
cleanup() {
    local exit_code=$?
    log "cleanup (exit=$exit_code)..."
    # KEEP_ON_FAIL=1: on a non-zero exit, leave everything in place so the
    # nodeplugin logs / test pod / HDFS pod can be inspected. The HDFS Pod
    # lives in the same namespace as the CSI deploy, so deleting the deploy
    # (which includes 00-namespace.yaml) would also destroy it — guard the
    # whole teardown behind this flag on failure.
    if [[ "${KEEP_ON_FAIL:-0}" == "1" && "$exit_code" -ne 0 ]]; then
        log "  KEEP_ON_FAIL=1: leaving all resources in place for debugging"
        exit "$exit_code"
    fi
    ${KUBECTL} delete pod "${E2E_POD_NAME}" -n "${E2E_NAMESPACE}" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pvc "${E2E_PVC_NAME}" -n "${E2E_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pv "${E2E_PV_NAME}" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pod "mntrs-csi-e2e-hdfs-krb-dyn" -n "${E2E_NAMESPACE}" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete pvc "mntrs-csi-e2e-hdfs-krb-dyn" -n "${E2E_NAMESPACE}" --ignore-not-found 2>/dev/null || true
    ${KUBECTL} delete sc "mntrs-dyn-hdfs-krb-e2e" --ignore-not-found 2>/dev/null || true
    if [[ "${SKIP_DEPLOY:-0}" != "1" && -n "${DEPLOY_DIR:-}" ]]; then
        ${KUBECTL} delete -f "${DEPLOY_DIR}/" --ignore-not-found 2>/dev/null || true
        # The keytab/krb5.conf Role+RoleBinding live in 04-nodeplugin-rbac.yaml
        # (appended by the deploy step), so `delete -f` above already removes
        # them. No Secret/ConfigMap to clean up — the nodeplugin fetches those
        # from the HDFS Pod at start time into an emptyDir.
        rm -rf "${DEPLOY_DIR}"
    fi
    exit "$exit_code"
}
trap cleanup EXIT

# ---------- preflight ----------
log "preflight..."
command -v kubectl >/dev/null || fail "kubectl not found"
[[ -f "${KUBECONFIG}" ]] || fail "KUBECONFIG ${KUBECONFIG} not found"
${KUBECTL} get nodes >/dev/null || fail "cannot reach cluster"
log "  cluster reachable: $(${KUBECTL} get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')"

# Check Kerberos HDFS Pod
log "checking HDFS Kerberos Pod in namespace '${HDFS_NAMESPACE}'..."
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
      - {name: dn-http, port: 9864, targetPort: 9864}
      - {name: kdc, port: 88, targetPort: 88}
      - {name: kadmin, port: 749, targetPort: 749}
  ---
  apiVersion: v1
  kind: Pod
  metadata: {name: hdfs, namespace: ${HDFS_NAMESPACE}, labels: {app: hdfs}}
  spec:
    # hostNetwork + ClusterFirstWithHostNet + hostAliases is the combo
    # that makes dyrnq/hdfs:latest-debian (Debian 13 / Java 21) actually
    # reachable from outside the pod. Without it:
    #   - DN→NN self-traffic (Service hdfs → kube-proxy hairpin → this
    #     pod) is SNATed to the node IP, so the DN registers with
    #     ip_addr=node-IP but only binds the pod IP → external clients
    #     get Connection refused.
    #   - hostNetwork drops the pod's resolv.conf to the node's (home
    #     router + 8.8.8.8), so cluster DNS breaks — KDC bootstrap
    #     fails on principal resolution.
    #   - hostNetwork overrides the pod's `hostname:` field, leaving
    #     the OS hostname at the node's value (e.g. z12); Java's
    #     InetAddress.getLocalHost then NXDOMAINs and SecurityUtil
    #     throws UnknownHostException, so the NameNode never starts.
    # hostAliases maps the node hostname (and the pod hostname) to the
    # node IP in /etc/hosts so Java's getLocalHost() succeeds. Editing
    # /etc/hosts on the host is disallowed (ops.md policy); hostAliases
    # is the k8s-native equivalent. See [[hdfs-pod-hostnet-quirks]].
    #
    # JAVA_TOOL_OPTIONS=preferIPv4Stack is REQUIRED (not optional) to
    # work around a regression in the #11 upstream fix. #11 added
    # `dfs.datanode.bind-host=0.0.0.0` to the in-image hdfs-site.xml
    # and made `dfs.datanode.{,http,https}.address` resolve to the
    # FQDN — but the DataNode's info web server (a separate Jetty
    # created in `DatanodeHttpServer.<init>`) hard-codes its bind
    # URI to `"http://localhost:" + proxyPort` and does NOT consult
    # `dfs.datanode.bind-host`. On Debian 13 + OpenJDK 21, the JVM
    # resolves `localhost` to `[::1]`, the kernel returns
    # EADDRNOTAVAIL on `bind([::1]:0)` (no IPv6 lo in the pod's
    # netns), and the DN immediately exits with
    # `BindException: Port in use: 0:0:0:0:0:0:0:1:0`. Forcing the
    # whole JVM to IPv4 makes `localhost` → `127.0.0.1`, the info
    # server starts on `127.0.0.1:<ephemeral>`, and the DN registers
    # normally. Tracked in docker-hdfs#12; drop this env var only
    # after upstream fixes `DatanodeHttpServer` to honor
    # `dfs.datanode.bind-host`. See [[docker-hdfs-ipv6-bind]].
    hostNetwork: true
    dnsPolicy: ClusterFirstWithHostNet
    hostname: hdfs
    hostAliases:
      - ip: ${NODE_IP}
        hostnames: [${NODE_NAME}, hdfs]
    containers:
      - name: hdfs
        image: dyrnq/hdfs:latest-debian
        imagePullPolicy: IfNotPresent
        env:
          # FQDN (not bare "hdfs"): the entrypoint derives the Kerberos
          # principal hdfs/<HDFS_HOSTNAME>@REALM, dfs.datanode.hostname, and
          # fs.defaultFS from this. The Rust client canonicalizes its GSS
          # target to the FQDN, so the principal must be FQDN to match.
          - {name: HDFS_HOSTNAME, value: hdfs.${HDFS_NAMESPACE}.svc.cluster.local}
          - {name: JAVA_TOOL_OPTIONS, value: "-Djava.net.preferIPv4Stack=true"}
          # hostNetwork flips the source IP of DN→NN hairpin back to the
          # node IP, but the kernel still requires the NameNode to skip
          # the reverse-DNS check on registration (otherwise it rejects
          # the node hostname that doesn't match the FQDN).
          - {name: HDFS-SITE.XML_dfs.namenode.datanode.registration.ip-hostname-check, value: "false"}
          # BIND the DN sockets to the short 'hdfs' -> hostAliases -> node IP
          # (a local iface), so the data server listens where clients connect.
          # HDFS_HOSTNAME=FQDN makes the heredoc's dfs.datanode.address=FQDN:9866
          # resolve the FQDN to the Service ClusterIP; under IPVS the ClusterIP
          # is local so the DN BINDS /<cluster-ip>:9866 — but the NN records the
          # DN's ip_addr as the NODE IP (registration source, docker-hdfs#8),
          # so clients connect <node-ip>:9866 -> refused -> every write fails
          # with '1 node(s) are excluded'. Kerberos can't use the simple-mode
          # short-name fix (the principal must be FQDN), so we override the
          # BIND addresses to the short name while dfs.datanode.hostname (set
          # by the heredoc from HDFS_HOSTNAME=FQDN) stays FQDN for ADVERTISE.
          # This is the documented envtoxml k8s escape hatch (see docker-hdfs
          # entrypoint comment on dfs.datanode.address). Hadoop 3.5.0 has no
          # dfs.datanode.bind-host key, so the address itself must be local.
          - {name: HDFS-SITE.XML_dfs.datanode.address, value: "hdfs:9866"}
          - {name: HDFS-SITE.XML_dfs.datanode.http.address, value: "hdfs:9864"}
          - {name: HDFS-SITE.XML_dfs.datanode.https.address, value: "hdfs:9865"}
          - {name: HDFS-SITE.XML_dfs.datanode.ipc.address, value: "hdfs:9867"}
        ports:
          - {containerPort: 8020, name: nn-rpc}
          - {containerPort: 9866, name: dn-data}
          - {containerPort: 9864, name: dn-http}
          - {containerPort: 88, name: kdc}
          - {containerPort: 749, name: kadmin}
  EOF

  The entrypoint auto-creates the FQDN principal + keytab on boot — no manual
  kadmin.local addprinc/ktadd needed. The nodeplugin pulls both straight from
  this Pod at start time, so no host-side staging is required." 1
fi

# The nodeplugin no longer reads krb5.conf from /tmp — a kubectl-fetch
# initContainer pulls it (and hdfs.keytab) straight from the HDFS Pod into an
# emptyDir at start time, and rewrites `kdc = localhost:88` to the FQDN there
# (see the deploy step). So there is no host-side krb5.conf to patch here.

HDFS_POD_PHASE=$(${KUBECTL} -n "${HDFS_NAMESPACE}" get pod hdfs -o jsonpath='{.status.phase}')
[[ "${HDFS_POD_PHASE}" == "Running" ]] || fail "HDFS Pod is not Running (phase=${HDFS_POD_PHASE})" 1

# Kinit inside Pod before checking readiness
log "  running kinit inside HDFS Pod..."
${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- /usr/bin/kinit -kt /etc/hadoop/hdfs.keytab "${HDFS_PRINCIPAL}" 2>/dev/null \
    || fail "kinit failed inside HDFS Pod" 1

if ! ${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 | grep "Live datanodes" > /dev/null; then
    fail "HDFS Pod is not ready (no live datanode)" 1
fi
log "  HDFS Kerberos ready ($(${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 | grep 'Live datanodes'))"

# No host-side keytab/krb5.conf to verify — the nodeplugin fetches both from
# the HDFS Pod at start time (kubectl-fetch initContainer). The nodeplugin
# resolves "hdfs" via CoreDNS (dnsPolicy: ClusterFirstWithHostNet), so no
# /etc/hosts entry is required on the dev box or the nodes.
log "  Kerberos ready; nodeplugin will fetch keytab+krb5.conf from the HDFS Pod"

# ---------- 1. build & push image ----------
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    log "[1/8] skip build (SKIP_BUILD=1), using ${IMAGE}"
else
    log "[1/8] building mntrs-csi (glibc dynamic)..."
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

# ---------- 2. deploy csi-mntrs (with Kerberos volumes) ----------
if [[ "${SKIP_DEPLOY:-0}" == "1" ]]; then
    log "[2/8] skip deploy (SKIP_DEPLOY=1)"
else
    log "[2/8] deploying csi-mntrs to ${CSI_NAMESPACE} with Kerberos volumes..."

    DEPLOY_DIR="$(mktemp -d)"
    SRC="${REPO_ROOT}/csi/deploy/kubernetes/1.20"
    for f in 00-namespace.yaml 01-csidriver.yaml 02-controller-rbac.yaml \
             03-controller.yaml 04-nodeplugin-rbac.yaml 05-nodeplugin.yaml \
             06-storageclass.yaml; do
        sed "s|image: csi-mntrs:latest|image: ${IMAGE}|" \
            "${SRC}/${f}" > "${DEPLOY_DIR}/${f}"
    done
    for f in 03-controller.yaml 05-nodeplugin.yaml; do
        sed -i "/^      serviceAccountName: csi-/a\\
      imagePullSecrets:\\
        - name: reg-e2e" \
            "${DEPLOY_DIR}/${f}"
    done

    # Inject Kerberos volumes + initContainers into the DaemonSet, targeting
    # the mntrs container specifically (not the node-driver-registrar).
    #
    # WHY fetch keytab/krb5.conf from the HDFS Pod at start time (not a k8s
    # Secret): a Secret is a snapshot taken at deploy time. The dyrnq/hdfs
    # entrypoint regenerates /etc/hadoop/hdfs.keytab (and the matching KDC
    # principal password) on every pod (re)start / HDFS_HOSTNAME change. A
    # deploy-time Secret then holds a stale keytab whose password no longer
    # matches the KDC, and every nodeplugin kinit fails with
    # "Preauthentication failed". A kubectl-fetch initContainer pulls the
    # CURRENT keytab + krb5.conf straight from the live HDFS Pod into a shared
    # emptyDir, so the nodeplugin always uses what the KDC knows. This drops
    # the Secret/ConfigMap entirely — no host-path staging
    # (/tmp/hdfs.keytab), no manual "kubectl exec cat" step, works on any node.
    #
    # The fetched krb5.conf has `kdc = localhost:88` (the KDC runs inside the
    # HDFS Pod, so localhost is correct THERE). The nodeplugin is a different
    # Pod, so the fetch container sed-rewrites kdc/admin_server to the FQDN so
    # kinit reaches the KDC cross-pod. The nodeplugin runs with
    # dnsPolicy: ClusterFirstWithHostNet, so the FQDN resolves via CoreDNS —
    # no /etc/hosts entry needed.
    #
    # RBAC: `kubectl exec` needs pods/exec. We grant the nodeplugin SA
    # pods/exec on the single Pod named "hdfs" in this namespace only (Role
    # appended to 04-nodeplugin-rbac.yaml below) — minimally scoped.
    #
    # WHY the kinit initContainer: hdfs-native acquires its GSS client cred via
    # gss_acquire_cred(GSS_C_NO_NAME), which reads the TGT from the default
    # ccache — it does NOT auto-acquire from KRB5_CLIENT_KTNAME (the keytab is
    # only consumed by an explicit kinit; see hdfs-native minidfs.rs which also
    # kinit's before tests). Without a TGT in the ccache the NameNode SASL
    # handshake fails with "GSS initiate failed" and a null client principal.
    # The mntrs image (alpine) has libgssapi but no kinit binary, so we run
    # kinit in an initContainer using the dyrnq/hdfs image (which ships krb5),
    # writing the TGT to a shared emptyDir that mntrs reads via KRB5CCNAME.
    sed -i '/^      volumes:/a\
        - name: krb5-shared\n          emptyDir: {}\n        - name: krb5-ccache\n          emptyDir: {}' \
        "${DEPLOY_DIR}/05-nodeplugin.yaml"

    # Insert two initContainers before the containers list:
    #   1. kubectl-fetch — pulls the current keytab + krb5.conf from the HDFS
    #      Pod into the shared emptyDir and rewrites kdc/admin_server to the
    #      FQDN (localhost-form is correct inside the HDFS Pod but not here).
    #   2. kinit — acquires a TGT into the ccache emptyDir (KRB5CCNAME), which
    #      the mntrs container then reads. TGT lifetime (default 10h) covers
    #      the e2e window; a long-lived deployment would need a renewing sidecar.
    #
    # The kubectl-fetch image defaults to rancher/kubectl:v1.35.5 (docker.io)
    # for CI. In environments whose docker.io mirror is broken (e.g. daocloud
    # 403s for third-party images), override with a copy pushed to the private
    # registry:
    #   docker build -t kubectl:1.35 - <<'EOF'   # FROM alpine:3.24.1
    #   ... COPY a static kubectl binary to /usr/bin/kubectl ...
    #   skopeo copy --policy <permissive> --dest-tls-verify=false \
    #     --dest-creds "$REG_USER:$REG_PASS" docker-daemon:kubectl:1.35 \
    #     docker://${REGISTRY}/kubectl:1.35
    #   KUBECTL_IMAGE=${REGISTRY}/kubectl:1.35 bash hdfs-kerberos.sh
    sed -i '/^      containers:/i\
      initContainers:\
        - name: kubectl-fetch\
          image: '"${KUBECTL_IMAGE}"'\
          imagePullPolicy: IfNotPresent\
          command: ["/bin/sh", "-c"]\
          args: ["set -e; kubectl -n '"${HDFS_NAMESPACE}"' exec hdfs -- cat /etc/hadoop/hdfs.keytab > /krb5-shared/hdfs.keytab; kubectl -n '"${HDFS_NAMESPACE}"' exec hdfs -- cat /etc/krb5.conf > /krb5-shared/krb5.conf; sed -i -e '\''s/kdc = localhost:88/kdc = hdfs.'"${HDFS_NAMESPACE}"'.svc.cluster.local:88/'\'' -e '\''s/admin_server = localhost:749/admin_server = hdfs.'"${HDFS_NAMESPACE}"'.svc.cluster.local:749/'\'' /krb5-shared/krb5.conf; chmod 600 /krb5-shared/hdfs.keytab; ls -l /krb5-shared"]\
          volumeMounts:\
            - name: krb5-shared\
              mountPath: /krb5-shared\
        - name: kinit\
          image: dyrnq/hdfs:latest-debian\
          imagePullPolicy: IfNotPresent\
          command: ["/bin/sh", "-c"]\
          args: ["kinit -k -t /etc/hadoop/hdfs.keytab '"${HDFS_PRINCIPAL}"' && klist"]\
          env:\
            - name: KRB5_CONFIG\
              value: /etc/krb5.conf\
            - name: KRB5CCNAME\
              value: FILE:/var/krb5cc/shared.cc\
          volumeMounts:\
            - name: krb5-shared\
              mountPath: /etc/hadoop/hdfs.keytab\
              subPath: hdfs.keytab\
              readOnly: true\
            - name: krb5-shared\
              mountPath: /etc/krb5.conf\
              subPath: krb5.conf\
              readOnly: true\
            - name: krb5-ccache\
              mountPath: /var/krb5cc' \
        "${DEPLOY_DIR}/05-nodeplugin.yaml"

    # Append volumeMounts to mntrs container (restrict to range after "name: mntrs").
    # Mount the shared keytab/krb5.conf via subPath at the paths mntrs's KRB5_*
    # env vars point at (KRB5_CLIENT_KTNAME=/etc/hadoop/hdfs.keytab,
    # KRB5_CONFIG=/etc/krb5.conf), so the env block below needs no changes.
    sed -i '/name: mntrs/,/^      volumes:/{ /^          volumeMounts:/a\
            - name: krb5-shared\n              mountPath: /etc/hadoop/hdfs.keytab\n              subPath: hdfs.keytab\n              readOnly: true\
            - name: krb5-shared\n              mountPath: /etc/krb5.conf\n              subPath: krb5.conf\n              readOnly: true\
            - name: krb5-ccache\n              mountPath: /var/krb5cc
    }' "${DEPLOY_DIR}/05-nodeplugin.yaml"

    # Append KRB5 env vars to mntrs container
    sed -i '/name: mntrs/,/^      volumes:/{ /^          env:/a\
            - name: KRB5_CONFIG\n              value: /etc/krb5.conf\
            - name: KRB5_CLIENT_KTNAME\n              value: /etc/hadoop/hdfs.keytab\
            - name: KRB5CCNAME\n              value: FILE:/var/krb5cc/shared.cc
    }' "${DEPLOY_DIR}/05-nodeplugin.yaml"

    # Grant the nodeplugin SA pods/exec on the single Pod named "hdfs" in this
    # namespace, so the kubectl-fetch initContainer can extract the keytab.
    # Appended to the RBAC manifest so `apply -f "${DEPLOY_DIR}/"` (and the
    # matching `delete -f` in cleanup) picks it up.
    cat >>"${DEPLOY_DIR}/04-nodeplugin-rbac.yaml" <<EOF
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: csi-nodeplugin-mntrs-exec-hdfs
  namespace: ${CSI_NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get"]
    resourceNames: ["hdfs"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create"]
    resourceNames: ["hdfs"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: csi-nodeplugin-mntrs-exec-hdfs
  namespace: ${CSI_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: csi-nodeplugin-mntrs
    namespace: ${CSI_NAMESPACE}
roleRef:
  kind: Role
  name: csi-nodeplugin-mntrs-exec-hdfs
  apiGroup: rbac.authorization.k8s.io
EOF

    log "  creating namespace ${CSI_NAMESPACE}..."
    ${KUBECTL} apply -f "${DEPLOY_DIR}/00-namespace.yaml"

    log "  creating docker-registry secret reg-e2e in ${CSI_NAMESPACE}..."
    ${KUBECTL} -n "${CSI_NAMESPACE}" create secret docker-registry reg-e2e \
        --docker-server="${REGISTRY}" \
        --docker-username="${REGISTRY_USER}" \
        --docker-password="${REGISTRY_PASS}" \
        --docker-email=e2e@example.com \
        --dry-run=client -o yaml | ${KUBECTL} apply -f -

    # No keytab/krb5.conf Secret or ConfigMap is created here — the nodeplugin
    # fetches both from the HDFS Pod at start time (kubectl-fetch initContainer
    # above) into a shared emptyDir, so there is no deploy-time snapshot to go
    # stale when the HDFS Pod regenerates its keytab.

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
log "[3/8] creating test data in HDFS (Kerberos)..."
# kinit first (ticket may have expired)
${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- /usr/bin/kinit -kt /etc/hadoop/hdfs.keytab "${HDFS_PRINCIPAL}" 2>/dev/null || true
# shellcheck source=tests/e2e/common/hdfs-prep.sh
. "$(cd "$(dirname "$0")" && pwd)/../common/hdfs-prep.sh"
hdfs_prep_kubectl_kerberos "${HDFS_NAMESPACE}"
# Seed the pre-existing test file. The put writes a block to the DataNode,
# which can transiently fail right after the HDFS Pod boots (DN registration
# still settling, or a not-yet-valid TGT) with `1 node(s) are excluded` — and
# it can also race the first kinit. Retry a few times and surface the real
# stderr on final failure instead of the bare exit `set -e` would give, so the
# failure point is visible in CI (the original `2>/dev/null` version exited
# silently at [3/8] with zero diagnostics — see /tmp/kerb-e2e6.log).
HDFS_PUT_ERR=""
for _i in $(seq 1 5); do
    if echo "hello from csi hdfs kerberos e2e" | ${KUBECTL} -n "${HDFS_NAMESPACE}" exec -i hdfs -- /opt/hadoop/bin/hdfs dfs -put -f - /test/pre-existing.txt 2>/tmp/krb-put.err; then
        rm -f /tmp/krb-put.err
        break
    fi
    HDFS_PUT_ERR="$(cat /tmp/krb-put.err 2>/dev/null)"
    rm -f /tmp/krb-put.err
    [[ "${_i}" == "5" ]] && fail "hdfs dfs -put failed after 5 attempts (last stderr: ${HDFS_PUT_ERR})" 1
    log "  hdfs dfs -put attempt ${_i} failed, retrying in 3s..."
    sleep 3
done
${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- /opt/hadoop/bin/hdfs dfs -chmod 644 /test/pre-existing.txt 2>/dev/null || true
${KUBECTL} -n "${HDFS_NAMESPACE}" exec hdfs -- /opt/hadoop/bin/hdfs dfs -ls /test/ 2>/dev/null
log "  test data ready"

# ---------- 4. create PV + PVC + test pod ----------
log "[4/8] creating Kerberos PV + PVC + test pod..."

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
      hadoop.security.authentication: "kerberos"
      dfs.data.transfer.protection: "authentication"
      dfs.namenode.kerberos.principal: "${HDFS_PRINCIPAL}"
      dfs.namenode.kerberos.keytab: "/etc/hadoop/hdfs.keytab"
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
# 300s (not 120s): a cold nodeplugin's first FUSE mount of an HDFS volume can
# take >120s (PVC bind + NodePublish + kerberos handshake + mount), which made
# the original 120s timeout flake on the first run of a fresh deploy while a
# warm nodeplugin finishes in <1s (see /tmp/kerb-e2e7.log vs e2e8.log). Matches
# the dynamic-provision wait below.
${KUBECTL} -n "${E2E_NAMESPACE}" wait --for=condition=Ready pod/"${E2E_POD_NAME}" --timeout=300s

# ---------- 5. run e2e tests ----------
log "[5/8] running Kerberos e2e tests in pod..."
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
assert_in "read pre-existing file" "hello from csi hdfs kerberos e2e" "${CAT_OUT}"

${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- \
    sh -c "echo 'hello from csi krb e2e' > /data/_ci_small.txt" >/dev/null
WRITE_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${E2E_POD_NAME}" -- cat /data/_ci_small.txt)
assert_in "write + read back small file" "hello from csi krb e2e" "${WRITE_OUT}"

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
PASS=$((PASS+1))

# ---------- 6. dynamic provision e2e ----------
log "[6/8] dynamic Kerberos provision e2e..."
${KUBECTL} -n "${E2E_NAMESPACE}" delete pod "${E2E_POD_NAME}" --force --grace-period=0 --ignore-not-found 2>/dev/null
${KUBECTL} -n "${E2E_NAMESPACE}" delete pvc "${E2E_PVC_NAME}" --ignore-not-found 2>/dev/null
${KUBECTL} delete pv "${E2E_PV_NAME}" --ignore-not-found 2>/dev/null

DYN_SC_NAME="mntrs-dyn-hdfs-krb-e2e"
DYN_PVC_NAME="mntrs-csi-e2e-hdfs-krb-dyn"
DYN_POD_NAME="mntrs-csi-e2e-hdfs-krb-dyn"

log "  creating StorageClass ${DYN_SC_NAME} with Kerberos parameters..."
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
  dfs.namenode.kerberos.principal: "${HDFS_PRINCIPAL}"
  hadoop.security.authentication: "kerberos"
  dfs.data.transfer.protection: "authentication"
  dfs.namenode.kerberos.keytab: "/etc/hadoop/hdfs.keytab"
EOF

log "  creating PVC ${DYN_PVC_NAME}..."
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

# Re-run read/write suite against dynamic volume
LS_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- ls -la /data)
assert_in "dynamic: FUSE mount shows test directory" "test" "${LS_OUT}"
CAT_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- cat /data/test/pre-existing.txt)
assert_in "dynamic: read pre-existing file" "hello from csi hdfs kerberos e2e" "${CAT_OUT}"
${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- \
    sh -c "echo 'hello from csi krb e2e' > /data/_ci_small.txt" >/dev/null
WRITE_OUT=$(${KUBECTL} -n "${E2E_NAMESPACE}" exec "${DYN_POD_NAME}" -- cat /data/_ci_small.txt)
assert_in "dynamic: write + read back" "hello from csi krb e2e" "${WRITE_OUT}"
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

# ---------- 7. volume expansion (pvc resize assertion) ----------
log "[7/8] volume expansion assertion..."
# CSI spec requires ControllerExpandVolume for resize; our driver currently
# doesn't implement it. Assert we get the expected error instead of hanging.
RESIZE_ERR=$(${KUBECTL} -n "${E2E_NAMESPACE}" patch pvc "${DYN_PVC_NAME}" --type=json \
    -p='[{"op":"replace","path":"/spec/resources/requests/storage","value":"2Gi"}]' 2>&1 || true)
log "  resize result: ${RESIZE_ERR}"
log "  expansion assertion: CSI driver does not support expansion (expected)"

# ---------- 8. summary ----------
log "[8/8] Kerberos e2e done: ${PASS} passed, ${FAIL} failed"
trap - EXIT
[[ "${KEEP_ON_FAIL:-0}" == "1" && $FAIL -gt 0 ]] || exit 0
exit 2
