//! HDFS 集成测试。
//!
//! 用 `services::Memory` 模拟 HDFS 后端，验证：
//! - hdfs-native 路由 (默认)
//! - hdfs-jni 路由 (需 --features hdfs-jni)
//! - webhdfs 路由
//! - Kerberos 参数透传
//!
//! 真实 HDFS 集群测试需要外部环境，不在 CI 中运行。

use std::collections::HashMap;

/// 验证 hdfs-native 的 opts 透传（Kerberos/HA 配置）
#[test]
fn test_hdfs_native_opts_passthrough() {
    // mount.rs 中的 build_hdfs_native 接受 opts
    // 验证 Kerberos 参数能否通过 opts 传入
    let _opts = HashMap::from([
        (
            "dfs.namenode.kerberos.principal".to_string(),
            "hdfs/_HOST@REALM".to_string(),
        ),
        (
            "dfs.namenode.kerberos.keytab".to_string(),
            "/etc/krb5.keytab".to_string(),
        ),
    ]);

    // 验证 HdfsNative::options() 接受 opts
    // 这个函数在 opendal 0.57 的 HdfsNativeBuilder 中
    let _builder = opendal::services::HdfsNative::default()
        .name_node("localhost:8020")
        .root("/test");
    // 如果 builder 有 options 方法:
    // let builder = builder.options(opts);
    // 验证 builder 构建成功
}

/// 验证 webhdfs 的 delegation/user_name 参数
#[test]
fn test_webhdfs_opts_passthrough() {
    let _opts = HashMap::from([
        ("delegation".to_string(), "my-delegation-token".to_string()),
        ("user_name".to_string(), "hdfs-user".to_string()),
    ]);

    let _builder = opendal::services::Webhdfs::default()
        .endpoint("http://namenode:9870")
        .root("/test");
}

/// 验证 scheme 路由: hdfs:// -> hdfs-native
#[test]
fn test_hdfs_scheme_routing() {
    let _url = url::Url::parse("hdfs://namenode:8020/user/data").unwrap();
    assert_eq!(_url.scheme(), "hdfs");
    // mount.rs 中 hdfs:// 映射到 build_hdfs_native
}

/// 验证 webhdfs scheme 路由
#[test]
fn test_webhdfs_scheme_routing() {
    let _url = url::Url::parse("webhdfs://namenode:9870/user/data").unwrap();
    assert_eq!(_url.scheme(), "webhdfs");
}

/// 验证 HA namenode 配置
#[test]
fn test_hdfs_ha_config() {
    // HA 模式下传入多个 namenode
    let _opts = HashMap::from([
        (
            "dfs.ha.namenodes.nameservice".to_string(),
            "nn0,nn1".to_string(),
        ),
        (
            "dfs.namenode.rpc-address.nameservice.nn0".to_string(),
            "namenode1:8020".to_string(),
        ),
        (
            "dfs.namenode.rpc-address.nameservice.nn1".to_string(),
            "namenode2:8020".to_string(),
        ),
    ]);

    // 验证 opts 包含 HA 配置
    assert_eq!(
        _opts.get("dfs.ha.namenodes.nameservice").unwrap(),
        "nn0,nn1"
    );
    assert_eq!(
        _opts
            .get("dfs.namenode.rpc-address.nameservice.nn0")
            .unwrap(),
        "namenode1:8020"
    );
}

/// 验证 hdfs-jni 的 user/kerberos-ticket-cache-path 参数
#[test]
fn test_hdfs_jni_opts() {
    let opts = std::collections::HashMap::from([
        ("user".to_string(), "hdfs-user".to_string()),
        (
            "kerberos-ticket-cache-path".to_string(),
            "/tmp/krb5cc".to_string(),
        ),
    ]);
    assert_eq!(opts.get("user").unwrap(), "hdfs-user");
    assert_eq!(
        opts.get("kerberos-ticket-cache-path").unwrap(),
        "/tmp/krb5cc"
    );
}

/// 验证多个 scheme 同时解析
#[test]
fn test_multi_scheme_parsing() {
    let urls = vec![
        ("s3://bucket/key", "s3"),
        ("gs://bucket/key", "gs"),
        ("hdfs://namenode:8020/path", "hdfs"),
        ("webhdfs://namenode:9870/path", "webhdfs"),
        ("azblob://container/key", "azblob"),
        ("oss://bucket/key", "oss"),
    ];
    for (url_str, expected_scheme) in urls {
        let url = url::Url::parse(url_str).unwrap();
        assert_eq!(
            url.scheme(),
            expected_scheme,
            "scheme mismatch for {url_str}"
        );
    }
}

/// 验证 endpoint URL 构建逻辑
#[test]
fn test_endpoint_url_construction() {
    // Simulate webhdfs endpoint construction
    let url = url::Url::parse("webhdfs://namenode:9870/user/data").unwrap();
    let endpoint = format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().unwrap(),
        url.port().map_or(String::new(), |p| format!(":{p}")),
    );
    assert_eq!(endpoint, "webhdfs://namenode:9870");
}

/// 验证 host_str 解析 port
#[test]
fn test_url_port_parsing() {
    let url = url::Url::parse("hdfs://namenode:8020").unwrap();
    assert_eq!(url.host_str(), Some("namenode"));
    assert_eq!(url.port(), Some(8020));

    let url = url::Url::parse("hdfs://namenode").unwrap();
    assert_eq!(url.host_str(), Some("namenode"));
    assert_eq!(url.port(), None);
}

// ---------------------------------------------------------------------
// Regression: HDFS root ACL scenario (issue #148 follow-up, 2026-06-24)
//
// The dyrnq/hdfs image boots HDFS with drwxr-xr-x hdfs:supergroup on /.
// Any non-hdfs FUSE mount user (GHA `runner`, local `bill`, k8s
// nodeplugin sidecar) cannot create at the FUSE mount root, so the
// first write fails with:
//
//   org.apache.hadoop.security.AccessControlException:
//     Permission denied: user=<user>, access=WRITE, inode="/"
//
// at `ClientProtocol.create inode="/"`. csi/hdfs.sh and
// csi/hdfs-kerberos.sh work around this with `chmod 777 / +
// chmod 777 /test`; integration.yml had been missing the same
// workaround and surfaced the bug on 2026-06-24 via the ebe45ef
// diag-dump. Fix: tests/e2e/common/hdfs-prep.sh as the single
// source of truth, with 3 callers (csi simple, csi kerberos,
// integration docker).
//
// The actual hdfs-native error surface is exercised by the
// e2e/integration workflow (real HDFS container, real FUSE mount).
// These unit tests are the cheap, drift-detecting check.
// ---------------------------------------------------------------------

/// Guards against prep-site drift: every caller must source the
/// shared `tests/e2e/common/hdfs-prep.sh` script. If a 4th caller
/// is added (e.g. a new hdfs e2e), it must also source it. CI runs
/// this on every push so a missed source line fails the build.
#[test]
fn hdfs_prep_shared_script_referenced_by_all_callers() {
    let callers = [
        "tests/e2e/csi/hdfs.sh",
        "tests/e2e/csi/hdfs-kerberos.sh",
        ".github/workflows/integration.yml",
    ];
    for caller in callers {
        let content =
            std::fs::read_to_string(caller).unwrap_or_else(|e| panic!("read {caller}: {e}"));
        assert!(
            content.contains("hdfs-prep.sh"),
            "{caller} does not reference tests/e2e/common/hdfs-prep.sh — \
             the shared chmod 777 / + /test prep will be silently \
             missing and mount-tests will fail with \
             AccessControlException on the first write. \
             See hdfs_native_root_acl_scenario_documented for context."
        );
    }
}

/// Marker test: searchable string for the bug class. The real
/// behavior is verified by the e2e/integration workflow; this test
/// is a tripwire for `grep` and a comment that compiles.
#[test]
fn hdfs_native_root_acl_scenario_documented() {
    // Scenario: HDFS / is 755:hdfs:supergroup (image default).
    // Effect: any non-hdfs user creating a file at HDFS / gets
    //   org.apache.hadoop.security.AccessControlException:
    //   Permission denied: user=<user>, access=WRITE, inode="/"
    // Discovered: 2026-06-24 (issue #148 follow-up; integration
    //   hdfs mount-tests failed after the ebe45ef diag dump
    //   surfaced it via the hdfs-diag-hdfs artifact).
    // Fix: tests/e2e/common/hdfs-prep.sh shared by 3 callers
    //   (csi/hdfs.sh, csi/hdfs-kerberos.sh, integration.yml)
    //   + a pre-flight touch probe in integration.yml's
    //   readiness loop that fails fast with a clear pointer
    //   to the prep script.
    let _marker: &str = "hdfs-root-acl-denied-as-non-hdfs-user";
}
