use clap::{Parser, Subcommand};
use std::collections::HashMap;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(
    name = "mntrs",
    about = "Mount remote storage to local directory via FUSE",
    version = VERSION,
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Mount storage to a local directory
    Mount {
        storage: String,
        mountpoint: String,
        /// Storage options: --opt endpoint=URL --opt access-key=KEY
        #[arg(long = "opt", value_name = "KEY=VAL", num_args = 0..)]
        opt: Vec<String>,
        /// Mount as read-only
        #[arg(long)]
        read_only: bool,
        /// Mount as network drive instead of fixed disk (Windows only)
        #[arg(long)]
        network_mode: bool,
        /// Directory cache TTL in seconds (default: 300, matches rclone 5m)
        #[arg(long, default_value = "300")]
        dir_cache_time: u64,
        /// Attribute cache TTL in seconds (default: 1)
        #[arg(long, default_value = "1")]
        attr_timeout: u64,
        #[arg(long, default_value = "10")]
        type_cache_ttl: u64,
        #[arg(long, default_value = "1")]
        stat_cache_ttl: u64,
        /// Allow other users to access the mount.
        /// ⚠️  Security: enables access for ALL local users.
        ///     Use with --uid/--gid to control file ownership.
        #[arg(long, verbatim_doc_comment)]
        allow_other: bool,
        /// Debug FUSE (print FUSE kernel requests)
        #[arg(long)]
        debug_fuse: bool,
        /// Volume name (shown in mount table)
        #[arg(long, default_value = "mntrs")]
        volname: String,
        /// Device name shown in mount table
        #[arg(long)]
        devname: Option<String>,
        /// Enable write-back caching (kernel buffers writes before sending to mntrs).
        /// **Linux / Windows only** — silently ignored on macOS; macFUSE manages
        /// its own write buffering outside the FUSE writeback capability. The
        /// flag stays accepted on macOS so mixed-fleet scripts don't fail at
        /// the CLI layer; a warning is logged at mount time when it is set.
        #[arg(long)]
        write_back_cache: bool,
        /// Raw FUSE option (repeatable), e.g. -o allow_other
        #[arg(short = 'o', long = "option", value_name = "OPT", num_args = 0..)]
        option: Vec<String>,
        /// Additional FUSE flags to pass to kernel (repeatable)
        #[arg(long = "fuse-flag", value_name = "FLAG", num_args = 0..)]
        fuse_flag: Vec<String>,
        /// Run as a background daemon (detach from terminal)
        #[arg(long)]
        daemon: bool,
        /// Wait for mount to be ready before returning (used with --daemon)
        #[arg(long)]
        daemon_wait: bool,
        /// Timeout in seconds for --daemon-wait (default: 10)
        #[arg(long, default_value = "10")]
        daemon_timeout: u64,
        /// Internal: set by parent when re-exec'ing daemon child
        #[arg(long, hide = true)]
        internal_daemon: bool,
        /// Allow root user to access the mount
        #[arg(long)]
        allow_root: bool,
        /// Allow UID/GID id mapping (Windows only)
        #[arg(long)]
        allow_idmap: bool,
        /// Permissions for symlinks (octal, default: 0777).
        /// **No effect in mntrs** — symlink permissions are governed by
        /// the platform. Accepted for rclone compat. See
        /// docs/vfs-cache-flags.md.
        #[arg(long, default_value = "777")]
        link_perms: u32,
        /// Max local cache size in MB (default: 0 = off, matches rclone)
        #[arg(long, default_value = "0")]
        vfs_cache_max_size: u64,
        #[arg(long, default_value = "256")]
        mem_limit: u64,
        /// Underlying `MemCache` impl: "dashmap" (default) or
        /// "moka". Both honor the same `MemCache` trait, so
        /// the choice is transparent to callers; it only
        /// changes eviction policy (FIFO vs TinyLFU). Use
        /// with `--mem-cache-metrics-interval` for a
        /// head-to-head A/B.
        #[arg(long, default_value = "dashmap", value_parser = ["dashmap", "moka", "foyer"])]
        mem_cache_impl: String,
        /// Emit mem_cache stats (hits/misses/inserts/evictions/
        /// entries/used/capacity) as one structured tracing
        /// event every N seconds. 0 = off (no background thread
        /// spawned). The numbers come from `MemCache::stats()`
        /// and use the same shape across all implementations
        /// (DashMap today, moka once it lands) so a
        /// head-to-head comparison is one log filter away.
        #[arg(long, default_value = "0")]
        mem_cache_metrics_interval: u64,
        /// Write-back delay in seconds before uploading dirty cache files (default: 5)
        #[arg(long, default_value = "5")]
        vfs_write_back: u64,
        /// Issue #202: files below this size (bytes) upload
        /// immediately on close, bypassing the vfs-write-back delay.
        /// Set to 0 to disable immediate upload entirely.
        /// Default: 1048576 (1 MiB).
        #[arg(long, default_value = "1048576")]
        writeback_immediate_threshold: u64,
        /// VFS cache mode: off, writes, full (default: off, matches rclone).
        /// **No effect in mntrs** — this is a **deprecation alias** for
        /// the four-knob composition `--attr-cache-ttl 0
        /// --dir-cache-ttl 0 --cache-max-size 0 --writeback-immediate`
        /// (Interpretation 1, user-signed-off canonical 2026-06-26).
        /// See docs/vfs-cache-flags.md for the decision matrix and
        /// why Interpretations 2/3 were rejected.
        #[arg(long, default_value = "off")]
        vfs_cache_mode: String,
        /// Read-ahead size in bytes (default: 0 = off, matches rclone).
        /// **No effect in mntrs** — use `--vfs-prefetch-threshold` /
        /// `--vfs-prefetch-queue-mb` instead. See docs/vfs-cache-flags.md.
        #[arg(long, default_value = "0")]
        vfs_read_ahead: u64,
        /// Read chunk size in bytes (default: 128MiB, matches rclone)
        #[arg(long, default_value = "134217728")]
        vfs_read_chunk_size: u64,
        /// Enable kernel permission checking (default_permissions FUSE flag)
        #[arg(long)]
        default_permissions: bool,
        /// Override UID for all files
        #[arg(long)]
        uid: Option<u32>,
        /// Override GID for all files
        #[arg(long)]
        gid: Option<u32>,
        /// Override umask (e.g. 022)
        #[arg(long)]
        umask: Option<u32>,
        /// Directory permissions (default: 0755)
        #[arg(long)]
        dir_perms: Option<u32>,
        /// File permissions (default: 0644)
        #[arg(long)]
        file_perms: Option<u32>,
        /// Allow mounting on a non-empty directory
        #[arg(long)]
        allow_non_empty: bool,
        /// Custom cache directory path
        #[arg(long)]
        cache_dir: Option<String>,
        /// Disable local caching, read/write directly to remote
        #[arg(long)]
        direct_io: bool,
        /// Remote polling interval in seconds (default: 60).
        /// **Deprecated**: use `--vfs-cache-poll-interval` instead.
        #[arg(long)]
        poll_interval: Option<u64>,
        /// Max age of cached files in seconds (default: 3600, 0 to disable).
        /// **No effect in mntrs** — TTLs are per-layer (`--attr-cache-ttl`,
        /// `--dir-cache-ttl`). See docs/vfs-cache-flags.md.
        #[arg(long, default_value = "3600")]
        vfs_cache_max_age: u64,
        /// Minimum free disk space before triggering cache eviction (MB, default: 0 = off, matches rclone)
        #[arg(long, default_value = "0")]
        vfs_cache_min_free_space: u64,
        /// Glob pattern to exclude (repeatable)
        #[arg(long = "exclude", value_name = "PATTERN", num_args = 0..)]
        exclude: Vec<String>,
        /// Glob pattern to include (repeatable, overrides exclude)
        #[arg(long = "include", value_name = "PATTERN", num_args = 0..)]
        include: Vec<String>,
        /// Max file size in bytes
        #[arg(long)]
        max_size: Option<u64>,
        /// Min file size in bytes
        #[arg(long)]
        min_size: Option<u64>,
        /// Max directory depth (1 = shallow)
        #[arg(long)]
        max_depth: Option<usize>,
        /// Case-insensitive filtering
        #[arg(long)]
        ignore_case: bool,
        /// Don't read/write modification times
        #[arg(long)]
        no_modtime: bool,
        /// Use server-side modification time (last_modified) instead of epoch
        #[arg(long)]
        use_server_modtime: bool,
        /// Don't compare checksums
        #[arg(long)]
        no_checksum: bool,
        /// Don't allow seeking in files
        #[arg(long)]
        no_seek: bool,
        /// Translate symlinks
        #[arg(long)]
        links: bool,
        /// macOS: ignore Apple Double files (._ prefix, rclone defaults to true)
        #[arg(long, default_value_t = true)]
        noapple_double: bool,
        /// macOS: ignore Apple extended attributes (rclone defaults to true)
        #[arg(long, default_value_t = true)]
        noapple_xattr: bool,
        /// macOS: skip Finder / Spotlight / FSEvents metadata entries
        /// (.DS_Store per-directory, .Trashes / .fseventsd /
        /// .Spotlight-V100 / .TemporaryItems / .DocumentRevisions-V100
        /// at volume root). Default true so a normal Finder browsing
        /// session doesn't write a `.DS_Store` per directory into the
        /// backend. Matches the precedent set by `--noapple-double` /
        /// `--noapple-xattr` (rclone parity). Library users without the
        /// CLI default get `false` (no filtering) for least-surprise.
        #[arg(long, default_value_t = true)]
        no_macos_metadata: bool,
        /// Consistent hash-based sharding: k of n (e.g. --hash-filter 1/4).
        /// **No effect in mntrs** — accepted for rclone compat, no
        /// backend currently implements consistent hash sharding in
        /// the dispatch path. See docs/vfs-cache-flags.md.
        #[arg(long, value_name = "K/N")]
        hash_filter: Option<String>,
        /// macOS: tell OS the mount is case-insensitive
        #[arg(long)]
        mount_case_insensitive: bool,
        /// macOS: volume name shown in Finder sidebar / `diskutil list`.
        /// Default (when unset): `mntrs-<basename(mountpoint)>`, truncated
        /// to 64 chars (macFUSE hard limit). Ignored on Linux/Windows.
        #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
        #[arg(long, value_name = "NAME")]
        volume_name: Option<String>,
        /// macOS: pass `-o local` to macFUSE so the kernel treats the
        /// mount like a local APFS volume (faster path for repeated
        /// small-file ops). Default true; ignored on Linux/Windows.
        #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
        #[cfg_attr(not(target_os = "macos"), allow(unused_variables))]
        #[arg(long, default_value_t = true)]
        finder_local: bool,
        /// Max read-ahead in bytes (default: 131072)
        #[arg(long, default_value = "131072")]
        max_read_ahead: u64,
        /// Read chunk size limit in bytes (default: 0 = unlimited)
        #[arg(long, default_value = "0")]
        vfs_read_chunk_size_limit: u64,
        /// Number of parallel read streams (default: 0 = serial, matches rclone)
        #[arg(long, default_value = "0")]
        vfs_read_chunk_streams: u32,
        /// Min file size (bytes) to enable the read-path prefetcher
        /// (default: 16 MiB; 0 = disabled). Speeds up sequential
        /// reads on large files (cat, dd, head -c large) by issuing
        /// the next chunk in the background while the kernel reads
        /// the current one. 16 MiB matches the prefetcher chunk-size
        /// cap — any file at or above this size has ≥1 prefetchable
        /// chunk after the first read, so the threshold doesn't waste
        /// thread spawns on files too small to overlap.
        #[arg(long, default_value = "16777216")]
        vfs_prefetch_threshold: u64,
        /// Max prefetch in-memory queue size in MiB (default: 64).
        /// Caps memory cost when a large file is opened but only
        /// partially read.
        #[arg(long, default_value = "64")]
        vfs_prefetch_queue_mb: u64,
        /// Use fast fingerprint (size+mtime) instead of checksums.
        /// **No effect in mntrs** — use `--hash-filter K/N` instead.
        /// See docs/vfs-cache-flags.md.
        #[arg(long)]
        vfs_fast_fingerprint: bool,
        /// Use async reads (don't wait for full read before replying to kernel)
        #[arg(long)]
        async_read: bool,
        /// Refresh directory cache on mount
        #[arg(long)]
        vfs_refresh: bool,
        /// Case-insensitive file name matching.
        /// **No effect in mntrs** — case sensitivity is governed by the
        /// platform filesystem. See docs/vfs-cache-flags.md.
        #[arg(long)]
        vfs_case_insensitive: bool,
        #[arg(long)]
        no_implicit_dir: bool,
        /// Block Unicode normalization duplicates (NFC/NFD)
        #[arg(long)]
        vfs_block_norm_dupes: bool,
        /// Translate symlinks.
        /// **No effect in mntrs** — symlink support is governed by
        /// `--link-perms`. See docs/vfs-cache-flags.md.
        #[arg(long)]
        vfs_links: bool,
        /// Use file size for used space in statfs
        #[arg(long)]
        vfs_used_is_size: bool,
        /// Metadata file extension
        #[arg(long)]
        vfs_metadata_extension: Option<String>,
        /// S3-style storage class hint (e.g. STANDARD, GLACIER).
        /// **No effect in mntrs** — backend upload already picks the
        /// backend's default storage class. Accepted for rclone
        /// compat. See docs/vfs-cache-flags.md.
        #[arg(long)]
        storage_class: Option<String>,
        /// Write wait timeout in seconds (default: 1, matches rclone).
        /// **No effect in mntrs** — writeback is governed by
        /// `--writeback-immediate-threshold`. Accepted for rclone
        /// compat. See docs/vfs-cache-flags.md.
        #[arg(long, default_value = "1")]
        vfs_write_wait: u64,
        /// Read wait timeout in seconds (default: 1).
        /// **No effect in mntrs** — read backpressure is governed by
        /// `--vfs-prefetch-threshold`. Accepted for rclone compat.
        /// See docs/vfs-cache-flags.md.
        #[arg(long, default_value = "1")]
        vfs_read_wait: u64,
        /// Cache poll interval in seconds (default: 60)
        #[arg(long, default_value = "60")]
        vfs_cache_poll_interval: u64,
        /// Time in seconds to keep file handles open after last close for reuse (0 to disable, default: 0)
        #[arg(long, default_value = "0")]
        vfs_handle_caching: u64,
        /// Total disk space to report in statfs (TB, default: 0 = off, matches rclone)
        #[arg(long, default_value = "0")]
        vfs_disk_space_total_size: u64,
        /// Issue #257: when the backend read fails (network/auth/
        /// timeout), fall back to a partial on-disk cache file
        /// instead of returning EIO. Off by default — opt-in.
        /// Useful for read-heavy workloads that can tolerate
        /// stale data during backend outages.
        #[arg(long, default_value = "false")]
        vfs_read_stale_on_backend_error: bool,
        /// Issue #316a (WinFSP audit #305): number of WinFSP
        /// dispatcher threads to spawn (`FspFileSystemStartDispatcher`
        /// arg). 0 = driver default (8). >0 = pin to that count.
        /// **Windows only** — accepted but ignored on macOS/Linux
        /// (the unix FUSE backend has its own dispatcher pool).
        /// Use a small count (2-4) to verify concurrent IRP
        /// handling during e2e (mount-test.ps1 sub-test 9 runs 3
        /// parallel `Get-Content` against the same file); bump
        /// higher only if a slow backend (S3 GET on a cold object)
        /// blocks other open handles.
        #[arg(long, default_value = "0")]
        winfsp_dispatcher_threads: u32,
    },
    /// Unmount a mounted directory (use "all" to unmount all)
    Unmount { target: String },
    /// List active mounts
    List,
    /// List pending `.dirty` sidecars in a cache dir (issue #395 fix #1).
    ///
    /// After an upload failure the daemon leaves a `.dirty` sidecar
    /// next to the cache file. There's no other CLI surface for these —
    /// without this command the user has no way to discover that a
    /// write silently failed to upload. Always returns exit 0 even if
    /// the dir is empty (a clean cache is the healthy state).
    Dirty {
        /// Path to the cache dir to scan (e.g. /var/cache/mntrs)
        cache_dir: std::path::PathBuf,
    },
    /// Install systemd service
    Install {
        #[command(subcommand)]
        action: Option<InstallAction>,
    },
}

#[derive(Subcommand)]
enum InstallAction {
    /// Generate a systemd user service file to mount on login
    Systemd,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    mntrs::install_panic_logger();
    if let Some(limit) = mntrs::detect_cgroup_memory_limit() {
        tracing::info!(
            memory_limit_mb = limit / 1024 / 1024,
            "detected cgroup memory limit"
        );
    }
    let cli = Cli::parse();
    match cli.command {
        Commands::Mount {
            storage,
            mountpoint,
            opt,
            read_only,
            network_mode,
            dir_cache_time,
            attr_timeout,
            type_cache_ttl,
            stat_cache_ttl,
            allow_other,
            debug_fuse,
            volname,
            devname,
            write_back_cache,
            option,
            fuse_flag,
            daemon,
            daemon_wait,
            daemon_timeout,
            #[allow(unused_variables)]
            internal_daemon,
            allow_root,
            allow_idmap,
            link_perms,
            vfs_cache_max_size,
            mem_limit,
            mem_cache_impl,
            mem_cache_metrics_interval,
            vfs_write_back,
            writeback_immediate_threshold,
            vfs_cache_mode,
            vfs_read_ahead,
            vfs_read_chunk_size,
            default_permissions,
            uid,
            gid,
            umask,
            dir_perms,
            file_perms,
            allow_non_empty,
            cache_dir,
            direct_io,
            poll_interval,
            vfs_cache_max_age,
            vfs_cache_min_free_space,
            exclude,
            include,
            max_size,
            min_size,
            max_depth,
            ignore_case,
            no_modtime,
            use_server_modtime,
            no_checksum,
            no_seek,
            links,
            noapple_double,
            noapple_xattr,
            no_macos_metadata,
            hash_filter,
            mount_case_insensitive,
            volume_name,
            finder_local,
            max_read_ahead,
            vfs_read_chunk_size_limit,
            vfs_read_chunk_streams,
            vfs_prefetch_threshold,
            vfs_prefetch_queue_mb,
            vfs_fast_fingerprint,
            async_read,
            vfs_refresh,
            vfs_case_insensitive,
            no_implicit_dir,
            vfs_block_norm_dupes,
            vfs_links,
            vfs_used_is_size,
            vfs_metadata_extension,
            storage_class,
            vfs_write_wait,
            vfs_read_wait,
            vfs_cache_poll_interval,
            vfs_handle_caching,
            vfs_disk_space_total_size,
            vfs_read_stale_on_backend_error,
            winfsp_dispatcher_threads,
            ..
        } => {
            // Sprint 8 (#229): consolidated warn for rclone-compat
            // shadow flags. clap defaults are applied, so we
            // check non-default values to detect "user explicitly
            // set this." One line per mount, not nine.
            let mut shadow = Vec::new();
            if vfs_cache_mode != "off" {
                shadow.push("--vfs-cache-mode");
            }
            if vfs_cache_max_age != 3600 {
                shadow.push("--vfs-cache-max-age");
            }
            if vfs_read_ahead != 0 {
                shadow.push("--vfs-read-ahead");
            }
            if vfs_fast_fingerprint {
                shadow.push("--vfs-fast-fingerprint");
            }
            if vfs_case_insensitive {
                shadow.push("--vfs-case-insensitive");
            }
            if vfs_links {
                shadow.push("--vfs-links");
            }
            if vfs_used_is_size {
                shadow.push("--vfs-used-is-size");
            }
            if vfs_metadata_extension.is_some() {
                shadow.push("--vfs-metadata-extension");
            }
            // T3-12: 5 more rclone-compat flags accepted but with
            // no daemon effect (audit issue #455). Each only fires
            // the warn when the user explicitly moved it off its
            // default — clap applies the default in the destructure
            // above.
            if link_perms != 0o777 {
                shadow.push("--link-perms");
            }
            if hash_filter.is_some() {
                shadow.push("--hash-filter");
            }
            if storage_class.is_some() {
                shadow.push("--storage-class");
            }
            if vfs_write_wait != 1 {
                shadow.push("--vfs-write-wait");
            }
            if vfs_read_wait != 1 {
                shadow.push("--vfs-read-wait");
            }
            if !shadow.is_empty() {
                tracing::warn!(
                    "{}",
                    format!(
                        "mount: rclone-compat shadow flag(s) have no effect in mntrs (see docs/vfs-cache-flags.md): {}",
                        shadow.join(", ")
                    )
                );
            }

            let mut opts = HashMap::new();
            for kv in &opt {
                match kv.split_once('=') {
                    Some((k, v)) => {
                        opts.insert(k.to_string(), v.to_string());
                    }
                    None => {
                        return Err(anyhow::anyhow!("--opt value must be KEY=VAL, got: {kv:?}"));
                    }
                }
            }
            mntrs::cmd::mount::mount(
                &storage,
                &mountpoint,
                &opts,
                read_only,
                network_mode,
                dir_cache_time,
                attr_timeout,
                type_cache_ttl,
                stat_cache_ttl,
                allow_other,
                debug_fuse,
                &volname,
                devname.as_deref(),
                write_back_cache,
                &option,
                &fuse_flag,
                daemon,
                daemon_wait,
                daemon_timeout,
                allow_root,
                allow_idmap,
                vfs_cache_max_size,
                mem_limit,
                &mem_cache_impl,
                mem_cache_metrics_interval,
                vfs_write_back,
                writeback_immediate_threshold,
                &vfs_cache_mode,
                vfs_read_ahead,
                vfs_read_chunk_size,
                default_permissions,
                uid,
                gid,
                umask,
                dir_perms,
                file_perms,
                Some(link_perms),
                allow_non_empty,
                cache_dir.as_deref(),
                direct_io,
                poll_interval,
                vfs_cache_max_age,
                vfs_cache_min_free_space,
                exclude,
                include,
                max_size,
                min_size,
                max_depth,
                ignore_case,
                no_modtime,
                use_server_modtime,
                no_checksum,
                no_seek,
                links,
                noapple_double,
                noapple_xattr,
                no_macos_metadata,
                hash_filter,
                mount_case_insensitive,
                volume_name.as_deref(),
                finder_local,
                max_read_ahead,
                vfs_read_chunk_size_limit,
                vfs_read_chunk_streams,
                vfs_prefetch_threshold,
                vfs_prefetch_queue_mb,
                vfs_fast_fingerprint,
                async_read,
                vfs_refresh,
                vfs_case_insensitive,
                no_implicit_dir,
                vfs_block_norm_dupes,
                vfs_links,
                vfs_used_is_size,
                vfs_metadata_extension,
                storage_class.as_deref(),
                vfs_write_wait,
                vfs_read_wait,
                vfs_cache_poll_interval,
                vfs_handle_caching,
                vfs_disk_space_total_size,
                vfs_read_stale_on_backend_error,
                winfsp_dispatcher_threads,
            )?;
        }
        Commands::Unmount { target } => {
            mntrs::cmd::unmount::unmount(&target)?;
        }
        Commands::List => {
            mntrs::cmd::list::list()?;
        }
        Commands::Dirty { cache_dir } => {
            mntrs::cmd::dirty::list_dirty(&cache_dir)?;
        }
        Commands::Install { action } => match action {
            Some(InstallAction::Systemd) | None => {
                #[cfg(target_os = "linux")]
                {
                    mntrs::cmd::install::systemd()?;
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = action;
                    anyhow::bail!(
                        "`mntrs install` is only supported on Linux; \
                         this build targets a non-Linux OS"
                    );
                }
            }
        },
    }
    Ok(())
}
