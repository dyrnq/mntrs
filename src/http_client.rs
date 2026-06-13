//! Shared `reqwest::Client` for OpenDAL HTTP traffic.
//!
//! Why this exists:
//!
//! OpenDAL 0.57 ships a `GLOBAL_REQWEST_CLIENT: LazyLock<reqwest::Client>` in
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
/// commit 9809e91).
pub fn shared() -> &'static reqwest::Client {
    SHARED.get_or_init(|| {
        reqwest::Client::builder()
            // 5s for TCP/TLS handshake. OpenDAL's `TimeoutLayer` below adds
            // per-I/O timeouts; the connect-timeout here only guards the
            // very first byte of the connection.
            .connect_timeout(Duration::from_secs(5))
            // 16 idle keep-alive connections per host. The `ConcurrentLimitLayer`
            // in `apply_operator_with_tls` further caps in-flight requests at
            // 16; idle-pool sizing is independent.
            .pool_max_idle_per_host(16)
            // Build a fresh client per process. `reqwest::Client` is
            // `Clone` (Arc internally), so callers below just clone the
            // `&'static` and pass it into opendal's `HttpClient::with`.
            .build()
            .expect("reqwest::Client::builder().build() must succeed")
    })
}
