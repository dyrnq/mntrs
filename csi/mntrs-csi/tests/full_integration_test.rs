//! Full CSI lifecycle integration test — memory backend, no k3s needed.
#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

const TEST_SOCKET: &str = "/tmp/mntrs-csi-full-test.sock";

#[tokio::test]
async fn full_csi_lifecycle_memory_backend() {
    let _ = std::fs::remove_file(TEST_SOCKET);
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");
    let target = tmp.path().join("target");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::create_dir_all(&target).unwrap();

    // Start CSI server
    let listener = UnixListener::bind(TEST_SOCKET).unwrap();
    let stream = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        Server::builder()
            .add_service(mntrs_csi::csi::identity_server::IdentityServer::new(
                mntrs_csi::IdentityService,
            ))
            .add_service(mntrs_csi::csi::controller_server::ControllerServer::new(
                mntrs_csi::ControllerService,
            ))
            .add_service(mntrs_csi::csi::node_server::NodeServer::new(
                mntrs_csi::NodeService::new(),
            ))
            .serve_with_incoming(stream)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let endpoint = format!("unix://{}", TEST_SOCKET);

    // 1. Identity: Probe
    let mut id_client = mntrs_csi::csi::identity_client::IdentityClient::connect(endpoint.clone())
        .await
        .unwrap();
    let resp = id_client
        .probe(mntrs_csi::csi::ProbeRequest::default())
        .await;
    assert!(resp.is_ok(), "Probe failed");

    // 2. CreateVolume
    let mut ctrl_client =
        mntrs_csi::csi::controller_client::ControllerClient::connect(endpoint.clone())
            .await
            .unwrap();
    let mut params = HashMap::new();
    params.insert("storage".to_string(), "memory://".to_string());
    let vol = ctrl_client
        .create_volume(mntrs_csi::csi::CreateVolumeRequest {
            name: "test-vol".to_string(),
            capacity_range: Some(mntrs_csi::csi::CapacityRange {
                required_bytes: 1024 * 1024,
                limit_bytes: 0,
            }),
            parameters: params,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let volume_id = vol.volume.unwrap().volume_id;
    assert!(!volume_id.is_empty(), "volume_id is empty");

    // 3. NodeStageVolume
    let mut node_client = mntrs_csi::csi::node_client::NodeClient::connect(endpoint)
        .await
        .unwrap();
    let mut vol_ctx = HashMap::new();
    vol_ctx.insert("storage".to_string(), "memory://".to_string());
    let stage_resp = node_client
        .node_stage_volume(mntrs_csi::csi::NodeStageVolumeRequest {
            volume_id: volume_id.clone(),
            staging_target_path: staging.to_str().unwrap().to_string(),
            volume_capability: Some(mntrs_csi::csi::VolumeCapability::default()),
            volume_context: vol_ctx.clone(),
            ..Default::default()
        })
        .await;
    assert!(stage_resp.is_ok(), "Stage failed: {:?}", stage_resp.err());

    // 4. NodePublishVolume
    let pub_resp = node_client
        .node_publish_volume(mntrs_csi::csi::NodePublishVolumeRequest {
            volume_id: volume_id.clone(),
            target_path: target.to_str().unwrap().to_string(),
            staging_target_path: staging.to_str().unwrap().to_string(),
            volume_capability: Some(mntrs_csi::csi::VolumeCapability::default()),
            volume_context: vol_ctx.clone(),
            ..Default::default()
        })
        .await;
    assert!(pub_resp.is_ok(), "Publish failed: {:?}", pub_resp.err());

    // 5. Write file
    std::fs::write(target.join("hello.txt"), b"Hello CSI!").unwrap();

    // 6. Read file
    let content = std::fs::read_to_string(target.join("hello.txt")).unwrap();
    assert_eq!(content, "Hello CSI!");

    // 7. NodeUnpublishVolume
    let unpub_resp = node_client
        .node_unpublish_volume(mntrs_csi::csi::NodeUnpublishVolumeRequest {
            volume_id: volume_id.clone(),
            target_path: target.to_str().unwrap().to_string(),
        })
        .await;
    assert!(unpub_resp.is_ok(), "Unpublish failed");

    // 8. NodeUnstageVolume
    let unstage_resp = node_client
        .node_unstage_volume(mntrs_csi::csi::NodeUnstageVolumeRequest {
            volume_id: volume_id.clone(),
            staging_target_path: staging.to_str().unwrap().to_string(),
        })
        .await;
    assert!(unstage_resp.is_ok(), "Unstage failed");

    // 9. DeleteVolume
    let del_resp = ctrl_client
        .delete_volume(mntrs_csi::csi::DeleteVolumeRequest {
            volume_id: volume_id.clone(),
            ..Default::default()
        })
        .await;
    assert!(del_resp.is_ok(), "DeleteVolume failed");

    println!("✅ Full CSI lifecycle test PASSED");
}
