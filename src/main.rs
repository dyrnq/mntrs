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
        /// Enable write-back caching (kernel buffers writes before sending to mntrs)
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
        /// Permissions for symlinks (octal, default: 0777)
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
        /// **No effect in mntrs** — see docs/vfs-cache-flags.md for the
        /// four-knob composition that maps to "no cache" intent.
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
        /// macOS: ignore Apple extended attributes
        #[arg(long)]
        noapple_xattr: bool,
        /// Consistent hash-based sharding: k of n (e.g. --hash-filter 1/4)
        #[arg(long, value_name = "K/N")]
        hash_filter: Option<String>,
        /// macOS: tell OS the mount is case-insensitive
        #[arg(long)]
        mount_case_insensitive: bool,
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
        #[arg(long)]
        storage_class: Option<String>,
        /// Write wait timeout in seconds (default: 1, matches rclone)
        #[arg(long, default_value = "1")]
        vfs_write_wait: u64,
        /// Read wait timeout in seconds (default: 1)
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
    },
    /// Unmount a mounted directory (use "all" to unmount all)
    Unmount { target: String },
    /// List active mounts
    List,
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
            allow_root,
            allow_idmap,
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
            link_perms,
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
            hash_filter,
            mount_case_insensitive,
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
                hash_filter,
                mount_case_insensitive,
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
            )?;
        }
        Commands::Unmount { target } => {
            mntrs::cmd::unmount::unmount(&target)?;
        }
        Commands::List => {
            mntrs::cmd::list::list()?;
        }
        Commands::Install { action } => match action {
            Some(InstallAction::Systemd) | None => {
                mntrs::cmd::install::systemd()?;
            }
        },
    }
    Ok(())
}
