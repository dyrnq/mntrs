//! mntrs CSI plugin — Kubernetes CSI driver for cloud storage mounts.

#![allow(clippy::all)]

use std::collections::HashMap;
use std::sync::Mutex;

use clap::Parser;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

mod csi;
use csi::*;

// ============================================================
// Identity Service
// ============================================================

#[derive(Debug, Default)]
struct IdentityService;

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
        Ok(Response::new(ProbeResponse::default()))
    }
}

// ============================================================
// Controller Service
// ============================================================

#[derive(Debug, Default)]
struct ControllerService;

#[tonic::async_trait]
impl controller_server::Controller for ControllerService {
    async fn create_volume(
        &self,
        _request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        Err(Status::unimplemented("create_volume not supported"))
    }

    async fn delete_volume(
        &self,
        _request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        Ok(Response::new(DeleteVolumeResponse::default()))
    }

    async fn controller_publish_volume(
        &self,
        _request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("publish not supported"))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("unpublish not supported"))
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

#[allow(dead_code)]
struct MountState {
    storage_url: String,
    mountpoint: String,
}

struct NodeService {
    mounts: Mutex<HashMap<String, MountState>>,
}

impl NodeService {
    fn new() -> Self {
        Self { mounts: Mutex::new(HashMap::new()) }
    }
}

#[tonic::async_trait]
impl node_server::Node for NodeService {
    async fn node_stage_volume(
        &self,
        _request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Ok(Response::new(NodeStageVolumeResponse::default()))
    }

    async fn node_unstage_volume(
        &self,
        _request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Ok(Response::new(NodeUnstageVolumeResponse::default()))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let target_path = req.target_path.clone();
        let volume_id = req.volume_id.clone();
        let vol_ctx = req.volume_context;
        let read_only = req.readonly;

        let storage = vol_ctx.get("storage")
            .or_else(|| vol_ctx.get("storageUrl"))
            .or_else(|| vol_ctx.get("storage-url"))
            .ok_or_else(|| Status::invalid_argument("missing volume context: storage"))?;

        let prefix = vol_ctx.get("prefix").or_else(|| vol_ctx.get("path")).map(|s| s.as_str()).unwrap_or("");
        let storage_url = if prefix.is_empty() {
            storage.clone()
        } else {
            format!("{}/{}", storage.trim_end_matches('/'), prefix.trim_start_matches('/'))
        };

        let target = std::path::Path::new(&target_path);
        if let Err(e) = std::fs::create_dir_all(target) {
            return Err(Status::internal(format!("create_dir_all {target_path}: {e}")));
        }

        let mut opts = HashMap::new();
        for (k, v) in &vol_ctx {
            match k.as_str() {
                "storage" | "storageUrl" | "storage-url" | "prefix" | "path" => continue,
                _ => { opts.insert(k.clone(), v.clone()); }
            }
        }

        match mntrs::cmd::mount::mount_internal(&storage_url, &target_path, &opts, read_only) {
            Ok(()) => {
                let mut mounts = self.mounts.lock().unwrap();
                mounts.insert(volume_id.clone(), MountState {
                    storage_url: storage_url.clone(),
                    mountpoint: target_path.clone(),
                });
                tracing::info!(volume_id, storage=%storage_url, target=%target_path, "volume mounted");
                Ok(Response::new(NodePublishVolumeResponse::default()))
            }
            Err(e) => Err(Status::internal(format!("mntrs mount failed: {e}"))),
        }
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let target_path = req.target_path;

        if let Err(e) = mntrs::cmd::mount::unmount_internal(&target_path) {
            tracing::warn!(target=%target_path, error=%e, "unmount failed");
        }
        let _ = std::fs::remove_dir(&target_path);

        let mut mounts = self.mounts.lock().unwrap();
        mounts.remove(&req.volume_id);

        tracing::info!(target=%target_path, "volume unmounted");
        Ok(Response::new(NodeUnpublishVolumeResponse::default()))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let req = request.into_inner();
        let target_path = req.volume_path;

        let stat = match std::fs::metadata(&target_path) {
            Ok(m) => m,
            Err(e) => return Err(Status::internal(format!("stat {target_path}: {e}"))),
        };

        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: vec![VolumeUsage {
                available: stat.len() as i64,
                total: stat.len() as i64,
                used: 0,
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
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(
                    node_service_capability::Rpc {
                        r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
                    },
                )),
            }],
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
        Ok(Response::new(NodeGetInfoResponse::default()))
    }
}

// ============================================================
// Main
// ============================================================

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

    let socket_path = cli.endpoint
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
        .add_service(node_server::NodeServer::new(NodeService::new()))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}
