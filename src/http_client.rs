//! Shared `reqwest::Client` for OpenDAL HTTP traffic.
//!
//! Why this exists:
//!
//! OpenDAL 0.57 ships a `GLOBAL_REWQEST_CLIENT: LazyLock<reqwest::Client>` in
//! `opendal_core::raw::http_util::client` that the default `HttpClient::default()`
//! returns. The first time it is instantiated it binds the underlying hyper
//! connector's TcpStream I/O to whichever tokio runtime the calling thread is
//! currently inside. All subsequent `.await`s on that client — including from
//! background writeback workers — must run in that same runtime, or hyper
//! silently deadlocks waiting for an I/O reactor that nobody is polling
//! (a 5-minute `unmount_internal` drain, then a CSI gRPC handler stuck
//! serving the next mount RPC, which is what made csi-e2e run
//! 27407577059 fail with `pending=3` and `use of closed network
//! connection`).
//!
//! The fix is to (a) always pass an explicit client to
//! `opendal::layers::HttpClientLayer::new(HttpClient::with(client))` so
//! we never touch opendal's `LazyLock` default, and (b) build that client
//! once, shared, from inside `crate::rt()` (so all `.await`s on the shared
//! client bind to the global runtime).
//!
//! This module is option (b). All `apply_operator_with_tls` callers in
//! `src/cmd/mount.rs` use the client returned by [`shared`] regardless of
//! whether the connection is TLS- or plain HTTP-only.

use std::sync::OnceLock;
use std::time::Duration;

static SHARED: OnceLock<reqwest::Client> = OnceLock::new();

/// Return the process-wide shared `reqwest::Client`.
///
/// First call constructs it. Must be reached from a thread whose
/// `tokio::runtime::Handle::current()` is `crate::rt()` (which is true for
/// every operator build path in `src/cmd/mount.rs` — they all run inside
/// `rt_block_on`, and `rt_block_on` now resolves to `crate::rt()` since
/// commit 9809e91). The init-time assertion below catches the case where
/// a future refactor calls `shared()` outside of `crate::rt()`, which
/// would silently re-introduce the cross-runtime deadlock.
pub fn shared() -> &'static reqwest::Client {
    SHARED.get_or_init(|| {
        // Assert: init must happen from inside `crate::rt()`. If the
        // surrounding context is a different (or no) tokio runtime,
        // panic at startup with a clear message — never silently bind
        // the hyper connector to the wrong runtime and then deadlock
        // hours later in writeback.
        let init_handle = tokio::runtime::Handle::current();
        let expected = crate::rt().handle();
        assert!(
            init_handle.id() == expected.id(),
            "crate::http_client::shared() must be initialized from inside crate::rt(); \
             got a different (or no) tokio runtime. Callers should reach this via \
             `apply_operator_with_tls`, which always runs inside `rt_block_on`."
        );

        reqwest::Client::builder()
            // 5s for TCP/TLS handshake. OpenDAL's `TimeoutLayer` below adds
            // per-I/O timeouts; the connect-timeout here only guards the
            // very first byte of the connection.
            .connect_timeout(Duration::from_secs(5))
            // 60s TCP keep-alive. CSI mounts can sit idle for tens of
            // minutes between reads; without this, intermediate NAT /
            // firewall devices may silently drop idle TCP connections,
            // and the next read then sees "connection reset by peer"
            // and pays a 1-2s reconnect cost.
            .tcp_keepalive(Some(Duration::from_secs(60)))
            // 5 min idle-pool timeout (reqwest default is 90s). Keeps
            // keep-alive connections warm across the typical 1-5 min
            // gap between CSI-mount reads. Matches rclone's VFS-mount
            // default.
            .pool_idle_timeout(Some(Duration::from_secs(300)))
            // 4 idle keep-alive connections per host
            // (issue #19). Pre-fix the pool was sized
            // at 16 per host — over a 30-iter mount/
            // unmount lifecycle stress that hits
            // multiple hosts (MinIO + HDFS WebHDFS +
            // auth endpoints), the keep-alive
            // connection pool accumulates FDs that
            // are not released on unmount (reqwest
            // owns the pool, not the mount). 4 is
            // enough for `ConcurrentLimitLayer`'s
            // 16 in-flight cap (4 idle warm slots is
            // plenty for the next burst) and bounds
            // the worst-case FD usage to a small
            // multiple of (hosts × 4). The mount/
            // unmount cycle doesn't tear down the
            // process, so the pool must be small
            // enough that the unmount-time FD count
            // is at-or-below the test threshold.
            .pool_max_idle_per_host(4)
            // Build a fresh client per process. `reqwest::Client` is
            // `Clone` (Arc internally), so callers below just clone the
            // `&'static` and pass it into opendal's `HttpClient::with`.
            .build()
            .expect("reqwest::Client::builder().build() must succeed")
    })
}
