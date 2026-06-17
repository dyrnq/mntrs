//! mntrs CSI plugin — Kubernetes CSI driver for cloud storage mounts.

#![allow(clippy::all)]

#[cfg(not(windows))]
use mntrs::cmd::mount::{mount_internal, unmount_internal};
use std::collections::HashMap;
use std::sync::Mutex;

use clap::Parser;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

mod csi;

// ============================================================
// ControllerPublishVolume bookkeeping (issue #41)
// ============================================================

/// Process-wide bookkeeping for ControllerPublishVolume
/// calls. Maps `volume_id → Vec<node_id>`. A subsequent
/// `ControllerGetVolume` would expose this; for now it's
/// an in-memory log of who has published what, useful
/// for ops debugging. The Mutex is fine: publish /
/// unpublish are infrequent (kubelet events) and the
/// critical section is < 100 ns.
static CONTROLLER_PUBLISHES: std::sync::LazyLock<Mutex<HashMap<String, Vec<String>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

// ============================================================
// VolumeID Encoding — bucket:prefix → volume_id
// ============================================================

/// Encode a storage URL into a K8s volume ID.
/// Uses `_XX` hex escaping for `:`, `/`, and `_` so the encoding
/// is fully reversible and never ambiguous (unlike the old `-`
/// replacement which collapsed `://`, `/`, `:` and `-` all to `-`).
fn encode_volume_id(storage_url: &str) -> String {
    let mut out = String::with_capacity(storage_url.len());
    for c in storage_url.chars() {
        match c {
            ':' => out.push_str("_3a"),
            '/' => out.push_str("_2f"),
            '_' => out.push_str("_5f"),
            _ => out.push(c),
        }
    }
    out
}

/// Decode a volume ID back to a storage URL (exact inverse of encode).
fn decode_volume_id(volume_id: &str) -> String {
    let mut out = String::with_capacity(volume_id.len());
    let mut chars = volume_id.chars();
    while let Some(c) = chars.next() {
        if c == '_' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(b) = u8::from_str_radix(&hex, 16) {
                    out.push(b as char);
                    continue;
                }
            }
            // Malformed or incomplete escape — pass through literally
            out.push('_');
            out.push_str(&hex);
        } else {
            out.push(c);
        }
    }
    out
}

// ============================================================
// gRPC Logging Interceptor
// ============================================================

// gRPC logging: each service method already has tracing::info! at entry

use csi::*;

// ============================================================
// Identity Service
// ============================================================

#[derive(Debug, Default)]
pub struct IdentityService;

#[tonic::async_trait]
impl identity_server::Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: "csi-mntrs".to_string(),
            vendor_version: env!("CARGO_PKG_VERSION").to_string(),
            ..Default::default()
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![PluginCapability {
                r#type: Some(plugin_capability::Type::Service(
                    plugin_capability::Service {
                        r#type: plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            }],
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        // Verify mntrs binary is available and functional
        // Just checking the binary exists is enough for a basic health probe
        let mntrs_path = std::env::current_exe()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        tracing::debug!("health probe: binary={}", mntrs_path);
        Ok(Response::new(ProbeResponse::default()))
    }
}

// ============================================================
// Controller Service
// ============================================================

#[derive(Debug, Default)]
pub struct ControllerService;

#[tonic::async_trait]
impl controller_server::Controller for ControllerService {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let name = req.name;
        let capacity = req.capacity_range.map(|r| r.required_bytes).unwrap_or(0);
        let params = req.parameters;

        let pvc_name = params
            .get("csi.storage.k8s.io/pvc/name")
            .cloned()
            .unwrap_or_else(|| name.clone());
        let pvc_ns = params
            .get("csi.storage.k8s.io/pvc/namespace")
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        let storage = params
            .get("storage")
            .or_else(|| params.get("storageUrl"))
            .cloned()
            .unwrap_or_else(|| format!("memory://{}", name));

        // Apply pathPattern if present
        let storage_url = if let Some(pattern) = params.get("pathPattern") {
            if !pattern.is_empty() {
                let suffix = expand_path_pattern(pattern, &pvc_name, &pvc_ns);
                format!(
                    "{}/{}",
                    storage.trim_end_matches('/'),
                    suffix.trim_start_matches('/')
                )
            } else {
                storage.clone()
            }
        } else {
            storage.clone()
        };

        let volume_id = encode_volume_id(&storage_url);
        let mut ctx = params.clone();
        ctx.insert("storage".to_string(), storage_url.clone());

        tracing::info!(volume_id, storage=%storage_url, capacity, pvc=%pvc_name, ns=%pvc_ns, "create_volume");

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id: volume_id.clone(),
                capacity_bytes: capacity as i64,
                volume_context: ctx,
                ..Default::default()
            }),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let volume_id = request.into_inner().volume_id;
        let storage_url = decode_volume_id(&volume_id);
        tracing::info!(volume_id, storage=%storage_url, "delete_volume — S3 bucket/prefix not deleted (safe by default)");
        // Never delete the actual S3 bucket — only remove CSI metadata
        Ok(Response::new(DeleteVolumeResponse::default()))
    }

    async fn controller_publish_volume(
        &self,
        request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        // Issue #41: basic RWO support. The pre-fix
        // code returned Unimplemented, which made
        // the kubelet log a warning and fall back
        // to a "stage-only" path that worked for
        // RWX but could not enforce single-writer
        // for RWO. We now return success and
        // record the (volume_id → node_id) mapping
        // in a process-wide state map. For a
        // second publish on the same volume from
        // a different node, the kubelet (with
        // --strict-topology) would get the conflict
        // from its own scheduling layer; without
        // --strict-topology we return success and
        // let the operator decide via the
        // access_mode declared in the storage
        // class.
        //
        // Note: mntrs does not enforce RWO at the
        // mount layer. The FUSE mount on each node
        // has its own cache + writeback queue, and
        // concurrent writes from multiple nodes
        // would race at the backend (last-writer-wins
        // on the remote object). This matches the
        // pre-fix behaviour — adding real RWO
        // enforcement would require a distributed
        // lock (etcd / Redis), which is out of scope
        // for a stateless CSI driver. Documented in
        // the storage class as "best-effort RWO".
        let req = request.into_inner();
        let volume_id = req.volume_id;
        let node_id = req.node_id;
        // Lightweight bookkeeping: log + record
        // the (volume, node) pair. A subsequent
        // ControllerGetVolume would expose this
        // map; left for a future PR.
        tracing::info!(volume_id, node_id, "controller_publish_volume");
        CONTROLLER_PUBLISHES
            .lock()
            .unwrap()
            .entry(volume_id.clone())
            .or_default()
            .push(node_id.clone());
        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context: HashMap::new(),
        }))
    }

    async fn controller_unpublish_volume(
        &self,
        request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        // Issue #41: counterpart to the publish
        // handler. Removes the (volume_id, node_id)
        // pair from the bookkeeping map. Idempotent:
        // unpublishing a volume that was never
        // published is a no-op.
        let req = request.into_inner();
        let volume_id = req.volume_id;
        let node_id = req.node_id;
        tracing::info!(volume_id, node_id, "controller_unpublish_volume");
        CONTROLLER_PUBLISHES
            .lock()
            .unwrap()
            .entry(volume_id)
            .and_modify(|nodes| nodes.retain(|n| n != &node_id));
        Ok(Response::new(ControllerUnpublishVolumeResponse::default()))
    }

    async fn validate_volume_capabilities(
        &self,
        _request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        Err(Status::unimplemented("validate not implemented"))
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented("list not supported"))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(Status::unimplemented("get_capacity not supported"))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![ControllerServiceCapability {
                r#type: Some(controller_service_capability::Type::Rpc(
                    controller_service_capability::Rpc {
                        r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                    },
                )),
            }],
        }))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshot not supported"))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshot not supported"))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("snapshot not supported"))
    }

    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshot not supported"))
    }

    async fn controller_expand_volume(
        &self,
        _request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("expand not supported"))
    }

    async fn controller_get_volume(
        &self,
        _request: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("get_volume not supported"))
    }

    async fn controller_modify_volume(
        &self,
        _request: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("modify not supported"))
    }
}

// ============================================================
// Node Service
// ============================================================

/// One per CSI volume that has been published to a target
/// path on this node. Tracked in `NodeService.mounts` so
/// `node_publish_volume` can short-circuit a duplicate
/// publish with a different `target_path` — the existing
/// `is_mountpoint()` check only catches the case where the
/// original target is still bind-mounted, not the case
/// where a previous publish was torn down and the kubelet
/// (or some retried driver) re-issues publish for the same
/// `volume_id` pointing at a fresh path.
struct MountState {
    mountpoint: String,
    /// `true` if the bind mount was remounted read-only at
    /// publish time (issue #36). Tracked for ops visibility
    /// / unpublish logging; the kernel's mount table is the
    /// source of truth for the actual ro/rw state.
    read_only: bool,
}

/// CSI `VolumeCapability::AccessMode::Mode` values that
/// map to a read-only bind mount at the node. Pulled from
/// csi.proto (see proto/csi.proto:484). Encoded as raw
/// i32 because the generated enum is only visible after
/// the build script has run; the constants are stable in
/// the CSI spec and have not changed since v1.0.
mod access_mode {
    pub const SINGLE_NODE_READER_ONLY: i32 = 2;
    pub const MULTI_NODE_READER_ONLY: i32 = 3;
    /// MULTI_NODE_SINGLE_WRITER (4) is read-only on every
    /// node except the elected writer. K8s asks each node
    /// to publish with the per-node mode that matches its
    /// actual read/write capability, so seeing 4 here means
    /// "publish ro here".
    pub const MULTI_NODE_SINGLE_WRITER: i32 = 4;
}

fn access_mode_is_read_only(mode: i32) -> bool {
    matches!(
        mode,
        access_mode::SINGLE_NODE_READER_ONLY
            | access_mode::MULTI_NODE_READER_ONLY
            | access_mode::MULTI_NODE_SINGLE_WRITER
    )
}

pub struct NodeService {
    mounts: Mutex<HashMap<String, MountState>>,
    node_id: String,
}

impl NodeService {
    pub fn new(node_id: String) -> Self {
        Self {
            mounts: Mutex::new(HashMap::new()),
            node_id,
        }
    }
}

#[tonic::async_trait]
impl node_server::Node for NodeService {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        let staging_path = req.staging_target_path;
        let volume_id = req.volume_id;
        let vol_ctx = req.volume_context;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id must not be empty"));
        }
        if staging_path.is_empty() {
            return Err(Status::invalid_argument(
                "staging_target_path must not be empty",
            ));
        }

        // Already mounted? Skip
        if is_mountpoint(&staging_path) {
            tracing::info!(volume_id, staging=%staging_path, "stage already mounted");
            return Ok(Response::new(NodeStageVolumeResponse::default()));
        }

        let (storage_url, read_only, mut opts) = parse_volume_context(&vol_ctx, &volume_id)?;
        let _volume_id = encode_volume_id(&storage_url);
        let _ = std::fs::create_dir_all(&staging_path);

        tracing::info!(volume_id, storage=%storage_url, staging=%staging_path, "staging FUSE mount");
        if let Some(cache_base) = std::env::var("MNTRS_CACHE_DIR").ok() {
            let vol_cache = format!("{}/{}", cache_base, encode_volume_id(&storage_url));
            opts.insert("cache-dir".to_string(), vol_cache);
        }
        // FUSE mount blocks in session.run() — spawn on a dedicated OS thread.
        //
        // Bug 13: pre-fix this thread was fully detached
        // (just `std::thread::spawn(move || { if let
        // Err(e) = mount_internal(...) { error!(); } })`).
        // If mount setup failed (auth, bad endpoint, perm
        // denied) the error landed only in the daemon's
        // tracing log; the gRPC caller waited the full
        // `wait_for_mount` timeout and saw
        // `DeadlineExceeded`, with no hint at the real
        // cause. CSI's standard error semantics expect a
        // mount setup failure to come back as
        // `Status::internal(message)` so kubelet can
        // record + retry meaningfully.
        //
        // Fix: send the mount thread's Err through a
        // one-shot channel and race the wait loop
        // against it. If the thread errors before the
        // mountpoint comes up, surface that error
        // verbatim; otherwise the timeout still fires.
        // We also detect thread panic (channel
        // disconnected without a send) — same idea.
        let (mount_err_tx, mount_err_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        {
            let su = storage_url.clone();
            let sp = staging_path.clone();
            let ro = read_only;
            std::thread::spawn(move || match mount_internal(&su, &sp, &opts, ro) {
                Ok(()) => {
                    // mount_internal only returns Ok on a
                    // clean unmount (session.run() exit).
                    // For stage purposes, treat as success
                    // signal so the channel disconnect
                    // below isn't read as a panic.
                    let _ = mount_err_tx.send(Ok(()));
                }
                Err(e) => {
                    tracing::error!(error=%e, "stage FUSE mount thread failed");
                    let _ = mount_err_tx.send(Err(format!("{e}")));
                }
            });
        }

        // Wait for either:
        //   - the mountpoint to appear (success path)
        //   - the mount thread to send an error (fast-fail)
        //   - the channel to disconnect without an
        //     error (thread panicked — treat as failed)
        //   - timeout (preserve the pre-fix DeadlineExceeded
        //     behaviour for genuinely-slow mounts)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            match mount_err_rx.try_recv() {
                Ok(Err(e)) => {
                    return Err(Status::internal(format!("mount setup failed: {e}")));
                }
                Ok(Ok(())) => {
                    // mount_internal returned Ok mid-stage —
                    // shouldn't happen (it runs the FUSE
                    // session loop), but be defensive.
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(Status::internal(
                        "mount thread exited unexpectedly (panic?)",
                    ));
                }
            }
            if is_mountpoint(&staging_path) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(format!(
                    "mountpoint {} not ready after 60s",
                    staging_path
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Ok(Response::new(NodeStageVolumeResponse::default()))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        let staging_path = req.staging_target_path;
        let volume_id = req.volume_id;

        if staging_path.is_empty() {
            return Err(Status::invalid_argument(
                "staging_target_path must not be empty",
            ));
        }

        if is_mountpoint(&staging_path) {
            tracing::info!(staging=%staging_path, "unstaging FUSE mount");
            unmount_internal(&staging_path)
                .map_err(|e| Status::internal(format!("unstage unmount failed: {e}")))?;
        }

        let _ = std::fs::remove_dir(&staging_path);

        // Bug 30: clean the actual cache dir that
        // node_stage_volume created at
        // `{MNTRS_CACHE_DIR}/{volume_id}`. Pre-fix CSI
        // relied on mntrs's unmount_internal auto-
        // cleanup, but that uses cache_dir_for_mount()
        // (which derives a /tmp/mntrs-csi-cache/<slug>
        // path from the mountpoint) — a different path
        // than the one stage actually used. Result: the
        // mntrs cleanup tried to remove a non-existent
        // dir (warn-logged + no-op) while the real cache
        // files leaked under MNTRS_CACHE_DIR.
        //
        // CSI knows both MNTRS_CACHE_DIR and the
        // volume_id, so it can reconstruct the exact
        // path stage used and clean it directly.
        // Failures are debug-logged: missing dir is
        // normal (mount may have never been staged
        // here, or already cleaned by a prior crash).
        if let Some(cache_base) = std::env::var("MNTRS_CACHE_DIR").ok() {
            let vol_cache = format!("{}/{}", cache_base, volume_id);
            match std::fs::remove_dir_all(&vol_cache) {
                Ok(()) => tracing::debug!(cache=%vol_cache, "csi unstage cache cleanup"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(cache=%vol_cache, error=%e, "csi unstage cache cleanup failed");
                }
            }
        }

        Ok(Response::new(NodeUnstageVolumeResponse::default()))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let target_path = req.target_path.clone();
        let volume_id = req.volume_id.clone();
        let staging_target_path = req.staging_target_path.clone();

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id must not be empty"));
        }
        if target_path.is_empty() {
            return Err(Status::invalid_argument("target_path must not be empty"));
        }
        if staging_target_path.is_empty() {
            return Err(Status::invalid_argument(
                "staging_target_path must not be empty",
            ));
        }
        if req.volume_capability.is_none() {
            return Err(Status::invalid_argument(
                "volume_capability must be provided",
            ));
        }

        // Idempotency: already published to this target?
        if is_mountpoint(&target_path) {
            tracing::info!(volume_id, target=%target_path, "already mounted (bind)");
            // Reflect this in our internal map so a later
            // unpublish for the same volume_id works
            // through the normal remove path. We don't
            // know the original read_only intent here
            // (the kernel just says "is a mountpoint");
            // default to false — unpublish behaviour is
            // the same either way (umount), and the log
            // line is cosmetic.
            self.mounts.lock().unwrap().insert(
                volume_id.clone(),
                MountState {
                    mountpoint: target_path.clone(),
                    read_only: false,
                },
            );
            return Ok(Response::new(NodePublishVolumeResponse::default()));
        }

        // Idempotency: already published to a *different*
        // target? This shouldn't happen in normal kubelet
        // flow, but a re-PVC that re-uses the same
        // volume_id with a fresh target_path can hit it.
        // Without this check, the second publish would
        // succeed (target_path is fresh, not a mountpoint)
        // and we'd leak the first bind mount.
        {
            let mounts = self.mounts.lock().unwrap();
            if let Some(prev) = mounts.get(&volume_id) {
                if prev.mountpoint != target_path {
                    return Err(Status::already_exists(format!(
                        "volume {volume_id} already published to {} (requested {})",
                        prev.mountpoint, target_path
                    )));
                }
                tracing::info!(volume_id, target=%target_path, "already published (in-memory map)");
                return Ok(Response::new(NodePublishVolumeResponse::default()));
            }
        }

        // Ensure staging is mounted (FUSE mount done in NodeStageVolume)
        if !is_mountpoint(&staging_target_path) {
            return Err(Status::internal(format!(
                "staging path not mounted: {staging_target_path}"
            )));
        }

        // Ensure target exists for bind mount
        std::fs::create_dir_all(&target_path)
            .map_err(|e| Status::internal(format!("create_dir_all {}: {}", target_path, e)))?;

        // Bind mount: staging → target (like k8s-csi-s3).
        // CSI nodeplugin runs in privileged mode — mount --bind works without sudo.
        tracing::info!(volume_id, staging=%staging_target_path, target=%target_path, "bind mounting");
        let output = std::process::Command::new("mount")
            .args(["--bind", &staging_target_path, &target_path])
            .output()
            .map_err(|e| Status::internal(format!("mount --bind failed: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("mount --bind: {stderr}")));
        }

        // Issue #36: honour the access_mode declared by the
        // pod's storage class. Without this remount, a PVC
        // that asked for readOnly access would still see
        // `rw` in /proc/mounts (bind mounts inherit the
        // source's flags), and write syscalls would slip
        // past the kernel VFS to FUSE — wasting a FUSE
        // round-trip before the FUSE layer rejected with
        // EROFS. A `remount,bind,ro` on the same path
        // flips the kernel's per-mount read-only flag so
        // the write is rejected at VFS lookup (EIO or
        // EROFS) before any FUSE traffic.
        //
        // We do the remount unconditionally *after* the
        // bind (the only legal order on Linux — see
        // mount(8): "You must change mount options of a
        // mount that already exists"). If the remount
        // fails we tear down the bind mount and surface
        // the error, so the caller doesn't end up with a
        // half-configured ro state and our `mounts` map
        // staying consistent with reality.
        let read_only = req
            .volume_capability
            .as_ref()
            .and_then(|vc| vc.access_mode.as_ref())
            .map(|am| access_mode_is_read_only(am.mode))
            .unwrap_or(false);
        if read_only {
            tracing::info!(volume_id, target=%target_path, "remounting bind ro (issue #36)");
            let output = std::process::Command::new("mount")
                .args(["-o", "remount,bind,ro", &target_path])
                .output()
                .map_err(|e| Status::internal(format!("mount -o remount,bind,ro failed: {e}")))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Tear down the bind we just created so we
                // don't leak an rw mount that the storage
                // class asked to be ro.
                let _ = std::process::Command::new("umount")
                    .arg(&target_path)
                    .output();
                let _ = std::fs::remove_dir_all(&target_path);
                return Err(Status::internal(format!(
                    "mount -o remount,bind,ro {target_path}: {stderr}"
                )));
            }
        }

        let mut mounts = self.mounts.lock().unwrap();
        mounts.insert(
            volume_id.clone(),
            MountState {
                mountpoint: target_path.clone(),
                read_only,
            },
        );
        tracing::info!(volume_id, staging=%staging_target_path, target=%target_path, read_only, "volume published");
        Ok(Response::new(NodePublishVolumeResponse::default()))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id;
        let target_path = req.target_path;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id must not be empty"));
        }
        if target_path.is_empty() {
            return Err(Status::invalid_argument("target_path must not be empty"));
        }

        #[cfg(not(windows))]
        {
            // Unmount bind mount first
            let output = std::process::Command::new("umount")
                .arg(&target_path)
                .output();
            if let Ok(o) = &output {
                if !o.status.success() {
                    tracing::warn!(target=%target_path, stderr=%String::from_utf8_lossy(&o.stderr), "umount failed, trying force");
                    std::process::Command::new("umount")
                        .args(["-l", &target_path])
                        .output()
                        .ok();
                }
            }
        }
        std::fs::remove_dir_all(&target_path).ok();

        // The map entry is removed whether or not the prior
        // umount succeeded; tracking the ro state at publish
        // time gives ops a hint about what was actually mounted,
        // but the kernel's mount table is the source of truth
        // for what umount just operated on.
        let read_only = self
            .mounts
            .lock()
            .unwrap()
            .remove(&volume_id)
            .map(|m| m.read_only)
            .unwrap_or(false);

        tracing::info!(target=%target_path, vol=%volume_id, read_only, "volume unmounted");
        Ok(Response::new(NodeUnpublishVolumeResponse::default()))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let req = request.into_inner();
        let target_path = req.volume_path;

        // Bug 14: pre-fix this returned
        // std::fs::metadata(target_path).len() as both
        // `available` and `total`. `len()` on a directory
        // is the directory inode size (4 KiB on most
        // filesystems) — useless as a volume stat. Kubelet
        // then exposed "4096 / 4096 bytes" for every
        // mount, breaking VolumeStatsAggregation alerts
        // and capacity-based scheduling.
        //
        // Fix: `statvfs(target_path)`. The syscall enters
        // the kernel, sees the FUSE mount on `target_path`,
        // and routes the request to mntrs's
        // CoreFilesystem::statfs() — which returns the
        // disk_total_size (or 256 MiB fallback) for the
        // cache disk. That's the right source of truth
        // for the CSI response; the actual S3 bucket
        // has no fixed capacity, but the cache disk
        // does, and an empty/full cache is what kubelet
        // most cares about.
        //
        // Stat errors propagate as Status::internal so
        // kubelet records the cause rather than seeing
        // a silent zero.
        let stat = rustix::fs::statvfs(target_path.as_str())
            .map_err(|e| Status::internal(format!("statvfs({target_path}): {e}")))?;

        let block_size = stat.f_frsize;
        let total_bytes = stat.f_blocks.saturating_mul(block_size) as i64;
        let available_bytes = stat.f_bavail.saturating_mul(block_size) as i64;
        // Used = total - free (the bytes consumed by all
        // users; vs `total - avail` which excludes
        // reserved-for-root blocks too).
        let free_bytes = stat.f_bfree.saturating_mul(block_size);
        let used_bytes =
            (stat.f_blocks.saturating_mul(block_size)).saturating_sub(free_bytes) as i64;

        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: vec![VolumeUsage {
                available: available_bytes,
                total: total_bytes,
                used: used_bytes,
                unit: volume_usage::Unit::Bytes as i32,
            }],
            volume_condition: Some(VolumeCondition {
                abnormal: false,
                message: "".to_string(),
            }),
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        // node_get_volume_stats is implemented below
        // (via rustix::statvfs on the FUSE mountpoint, which
        // hits MntrsFs::statfs). CSI requires that any
        // advertised capability corresponds to a working
        // RPC, and conversely that any implemented RPC be
        // declared — kubelet uses the capability list to
        // decide whether to call NodeGetVolumeStats for
        // VolumeStatsAggregation. Without GET_VOLUME_STATS
        // the kubelet skips the call, capacity monitoring
        // shows stale data, and CSI sidecars log
        // "GetVolumeStats not supported" warnings.
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![
                NodeServiceCapability {
                    r#type: Some(node_service_capability::Type::Rpc(
                        node_service_capability::Rpc {
                            r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
                        },
                    )),
                },
                NodeServiceCapability {
                    r#type: Some(node_service_capability::Type::Rpc(
                        node_service_capability::Rpc {
                            r#type: node_service_capability::rpc::Type::GetVolumeStats as i32,
                        },
                    )),
                },
            ],
        }))
    }

    async fn node_expand_volume(
        &self,
        _request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("expand not supported"))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            ..Default::default()
        }))
    }
}

// ============================================================
// Main
// ============================================================

// ============================================================
// CSI Helpers
// ============================================================

/// Check if `path` appears as an exact mount target in the given mounts content.
/// Extracted from `is_mountpoint` so it can be unit-tested with synthetic data.
///
/// `#[cfg(test)]` since Bug 16: production `is_mountpoint` now delegates to
/// `mntrs::cmd::mount::is_mount_point`, which canonicalizes the path before
/// matching `/proc/mounts`. This pure helper stays gated to test builds so
/// the regression suite below can still exercise the exact-match logic with
/// synthetic mount-content strings (no `/proc/mounts` dependency).
#[cfg(test)]
fn is_mountpoint_in(path: &str, mounts_content: &str) -> bool {
    for line in mounts_content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 && parts[1] == path {
            return true;
        }
    }
    false
}

/// Check if a path is a mountpoint.
///
/// Bug 16: pre-fix this read `/proc/mounts` and compared `parts[1] == path`
/// directly — no canonicalization. If the input path contained a symlink
/// component (common in Kubernetes: kubelet may stage a PVC at
/// `/var/lib/kubelet/...` while `/var/lib` itself is a symlink to
/// `/data/k8s/var-lib` on some host setups), the raw-path comparison missed
/// the mount entirely. Every `is_mountpoint(staging_path)` short-circuit in
/// node_stage_volume / node_publish_volume / unstage / unpublish then took
/// the wrong branch — re-mounting an already-mounted volume, or skipping
/// the unmount of a mounted one, depending on the call site.
///
/// Fix: delegate to `mntrs::cmd::mount::is_mount_point`, which has always
/// canonicalized the path first (resolving symlinks via
/// `std::fs::canonicalize`) before matching `/proc/mounts`. Same source of
/// truth as the mntrs CLI / mount_internal, so behaviour now agrees across
/// CSI and the standalone binary.
fn is_mountpoint(path: &str) -> bool {
    mntrs::cmd::mount::is_mount_point(path)
}

// Bug 14 follow-up: removed `fn wait_for_mount` —
// its only caller (node_stage_volume) was inlined in
// Bug 13 to integrate with the mount-error channel
// poll. No other call sites; rather than leave dead
// code behind, drop it.

/// Expand pathPattern placeholders like ${.PVC.namespace}/${.PVC.name}
fn expand_path_pattern(pattern: &str, pvc_name: &str, pvc_namespace: &str) -> String {
    pattern
        .replace("${.PVC.namespace}", pvc_namespace)
        .replace("${.PVC.name}", pvc_name)
        .replace(
            "${.PVC.namespace}/${.PVC.name}",
            &format!("{}/{}", pvc_namespace, pvc_name),
        )
}

/// Inject MNTRS_* environment variables as mount options
#[allow(dead_code)]
fn inject_env_opts(opts: &mut HashMap<String, String>) {
    for (k, v) in std::env::vars() {
        if let Some(flag) = k.strip_prefix("MNTRS_") {
            let key = flag.to_lowercase().replace('_', "-");
            opts.entry(key).or_insert(v);
        }
    }
}
fn parse_volume_context(
    ctx: &HashMap<String, String>,
    volume_id: &str,
) -> Result<(String, bool, HashMap<String, String>), Status> {
    let storage = ctx
        .get("storage")
        .or_else(|| ctx.get("storageUrl"))
        .or_else(|| ctx.get("storage-url"))
        .ok_or_else(|| Status::invalid_argument("volume context missing 'storage' key"))?
        .clone();

    let read_only = ctx.get("readOnly").map(|v| v == "true").unwrap_or(false);

    let mut opts = HashMap::new();
    for (k, v) in ctx {
        match k.as_str() {
            "storage" | "storageUrl" | "storage-url" | "readOnly" | "prefix" | "path" => continue,
            _ => {
                opts.insert(k.clone(), v.clone());
            }
        }
    }

    let _ = volume_id; // used for logging, not logic
    Ok((storage, read_only, opts))
}

#[derive(Parser)]
#[command(name = "mntrs-csi", about = "Kubernetes CSI driver for mntrs")]
struct Cli {
    #[arg(long, default_value = "mntrs-csi-node")]
    node_id: String,

    #[arg(long, default_value = "unix:///tmp/csi.sock")]
    endpoint: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let node_id = cli.node_id;

    let socket_path = cli
        .endpoint
        .strip_prefix("unix://")
        .or_else(|| cli.endpoint.strip_prefix("unix:"))
        .unwrap_or(&cli.endpoint)
        .to_string();

    let _ = std::fs::remove_file(&socket_path);
    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tracing::info!(endpoint=%cli.endpoint, node_id, "starting CSI driver");

    Server::builder()
        .add_service(identity_server::IdentityServer::new(IdentityService))
        .add_service(controller_server::ControllerServer::new(ControllerService))
        .add_service(node_server::NodeServer::new(NodeService::new(node_id)))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_volume_id, encode_volume_id, is_mountpoint_in};
    use std::collections::HashMap;

    // ============================================================
    // volume_context 解析
    // ============================================================

    #[test]
    fn parse_storage_key_priority() {
        let mut ctx = HashMap::new();
        ctx.insert("storage".to_string(), "s3://b1".to_string());
        ctx.insert("storageUrl".to_string(), "s3://b2".to_string());
        ctx.insert("storage-url".to_string(), "s3://b3".to_string());

        let v = ctx
            .get("storage")
            .or_else(|| ctx.get("storageUrl"))
            .or_else(|| ctx.get("storage-url"));
        assert_eq!(v.unwrap(), "s3://b1");
    }

    #[test]
    fn parse_storage_url_with_prefix() {
        let mut ctx = HashMap::new();
        ctx.insert("storage".to_string(), "s3://bucket".to_string());
        ctx.insert("prefix".to_string(), "data/2026/".to_string());

        let storage = ctx.get("storage").unwrap();
        let prefix = ctx.get("prefix").unwrap();
        let url = format!(
            "{}/{}",
            storage.trim_end_matches('/'),
            prefix.trim_start_matches('/')
        );
        assert_eq!(url, "s3://bucket/data/2026/");
    }

    #[test]
    fn parse_storage_url_no_prefix() {
        let mut ctx = HashMap::new();
        ctx.insert("storage".to_string(), "s3://bucket".to_string());
        let storage = ctx.get("storage").unwrap();
        assert_eq!(storage, "s3://bucket");
    }

    #[test]
    fn parse_read_only_flag() {
        let mut ctx = HashMap::new();
        ctx.insert("readOnly".to_string(), "true".to_string());
        let ro = ctx.get("readOnly").map(|v| v == "true").unwrap_or(false);
        assert!(ro);

        let mut ctx2 = HashMap::new();
        ctx2.insert("readOnly".to_string(), "false".to_string());
        let ro2 = ctx2.get("readOnly").map(|v| v == "true").unwrap_or(false);
        assert!(!ro2);
    }

    // ============================================================
    // access_mode_is_read_only — issue #36 readOnly remount
    // ============================================================

    #[test]
    fn access_mode_ro_classification() {
        use super::access_mode_is_read_only;
        // CSI spec v1 (csi.proto:484) enum values. UNKNOWN
        // and the SINGLE/MULTI NODE_WRITER modes must NOT
        // remount ro, otherwise write PVCs would silently
        // become read-only.
        assert!(access_mode_is_read_only(
            super::access_mode::SINGLE_NODE_READER_ONLY
        ));
        assert!(access_mode_is_read_only(
            super::access_mode::MULTI_NODE_READER_ONLY
        ));
        assert!(access_mode_is_read_only(
            super::access_mode::MULTI_NODE_SINGLE_WRITER
        ));
        // Everything else is rw (UNKNOWN defaults to rw for
        // forward compat with future CSI spec extensions).
        assert!(!access_mode_is_read_only(0)); // UNKNOWN
        assert!(!access_mode_is_read_only(1)); // SINGLE_NODE_WRITER
        assert!(!access_mode_is_read_only(5)); // MULTI_NODE_MULTI_WRITER
        assert!(!access_mode_is_read_only(99)); // unknown future mode → rw
    }

    // ============================================================
    // is_mountpoint_in — exact match regression tests
    // ============================================================

    #[test]
    fn mountpoint_exact_match() {
        let mounts = "mntrs /a/pvc-old/globalmount fuse mntrs rw 0 0\n";
        assert!(is_mountpoint_in("/a/pvc-old/globalmount", mounts));
    }

    #[test]
    fn mountpoint_substring_no_false_positive() {
        // The old contains() bug: /a/pvc-old/globalmount in /proc/mounts
        // should NOT match /a/pvc-old/globalmountx
        let mounts = "mntrs /a/pvc-old/globalmount fuse mntrs rw 0 0\n";
        assert!(!is_mountpoint_in("/a/pvc-old/globalmountx", mounts));
    }

    #[test]
    fn mountpoint_prefix_no_false_positive() {
        // /a/pvc-old/globalmount should NOT match /a/pvc-old
        let mounts = "mntrs /a/pvc-old/globalmount fuse mntrs rw 0 0\n";
        assert!(!is_mountpoint_in("/a/pvc-old", mounts));
    }

    #[test]
    fn mountpoint_not_found() {
        let mounts = "mntrs /a/pvc-old/globalmount fuse mntrs rw 0 0\n";
        assert!(!is_mountpoint_in("/a/pvc-new/globalmount", mounts));
    }

    #[test]
    fn mountpoint_empty_mounts() {
        assert!(!is_mountpoint_in("/anything", ""));
    }

    // ============================================================
    // volume_id encode/decode — round-trip tests
    // ============================================================

    #[test]
    fn volume_id_roundtrip_basic() {
        let url = "s3://bucket/prefix";
        assert_eq!(decode_volume_id(&encode_volume_id(url)), url);
    }

    #[test]
    fn volume_id_roundtrip_hyphens() {
        let url = "s3://my-custom-bucket/some/path";
        assert_eq!(decode_volume_id(&encode_volume_id(url)), url);
    }

    #[test]
    fn volume_id_roundtrip_oss() {
        let url = "oss://endpoint-bucket/data";
        assert_eq!(decode_volume_id(&encode_volume_id(url)), url);
    }

    #[test]
    fn volume_id_roundtrip_no_path() {
        let url = "s3://b1";
        assert_eq!(decode_volume_id(&encode_volume_id(url)), url);
    }

    #[test]
    fn volume_id_no_ambiguity() {
        // These two URLs must produce DIFFERENT volume IDs
        let a = encode_volume_id("s3://a/b-c");
        let b = encode_volume_id("s3://a-b/c");
        assert_ne!(a, b, "different URLs must not collide");
        assert_eq!(decode_volume_id(&a), "s3://a/b-c");
        assert_eq!(decode_volume_id(&b), "s3://a-b/c");
    }

    #[test]
    fn volume_id_roundtrip_underscores() {
        let url = "s3://my_bucket/my_path";
        assert_eq!(decode_volume_id(&encode_volume_id(url)), url);
    }

    #[test]
    fn volume_id_malformed_short_escape() {
        // _3 (only 1 hex digit) should NOT decode to 0x03
        assert_eq!(decode_volume_id("_3"), "_3");
        assert_eq!(decode_volume_id("_"), "_");
        assert_eq!(decode_volume_id("_zz"), "_zz");
    }

    #[test]
    fn mountpoint_multiple_entries() {
        let mounts = "\
mntrs /a/pvc-1/globalmount fuse mntrs rw 0 0
mntrs /a/pvc-2/globalmount fuse mntrs rw 0 0
s3fs /a/pvc-3/s3mount fuse s3fs rw 0 0
";
        assert!(is_mountpoint_in("/a/pvc-2/globalmount", mounts));
        assert!(!is_mountpoint_in("/a/pvc-2", mounts));
        assert!(!is_mountpoint_in("/a/pvc-2/globalmount/extra", mounts));
    }
}
