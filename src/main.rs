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
        /// Directory cache TTL in seconds (default: 10)
        #[arg(long, default_value = "10")]
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
        /// Run as a background daemon (detach from terminal)
        #[arg(long)]
        daemon: bool,
        /// Wait for mount to be ready before returning (used with --daemon)
        #[arg(long)]
        daemon_wait: bool,
        /// Timeout in seconds for --daemon-wait (default: 10)
        #[arg(long, default_value = "10")]
        daemon_timeout: u64,
        /// Allow root user to access the mount
        #[arg(long)]
        allow_root: bool,
        /// Max local cache size in MB (default: 1024, 0 to disable)
        #[arg(long, default_value = "1024")]
        vfs_cache_max_size: u64,
        #[arg(long, default_value = "256")]
        mem_limit: u64,
        /// Write-back delay in seconds before uploading dirty cache files (default: 5)
        #[arg(long, default_value = "5")]
        vfs_write_back: u64,
        /// VFS cache mode: off, writes, full (default: writes)
        #[arg(long, default_value = "writes")]
        vfs_cache_mode: String,
        /// Read-ahead size in bytes (0 to disable, default: 131072)
        #[arg(long, default_value = "131072")]
        vfs_read_ahead: u64,
        /// Read chunk size in bytes (0 for unlimited, default: 0)
        #[arg(long, default_value = "0")]
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
        /// Remote polling interval in seconds (default: 60)
        #[arg(long, default_value = "60")]
        poll_interval: u64,
        /// Max age of cached files in seconds (default: 3600, 0 to disable)
        #[arg(long, default_value = "3600")]
        vfs_cache_max_age: u64,
        /// Minimum free disk space before triggering cache eviction (MB, default: 100)
        #[arg(long, default_value = "100")]
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
        /// macOS: ignore Apple Double files (._ prefix)
        #[arg(long)]
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
        /// Number of parallel read streams (default: 1)
        #[arg(long, default_value = "1")]
        vfs_read_chunk_streams: u32,
        /// Use fast fingerprint (size+mtime) instead of checksums
        #[arg(long)]
        vfs_fast_fingerprint: bool,
        /// Use async reads (don't wait for full read before replying to kernel)
        #[arg(long)]
        async_read: bool,
        /// Refresh directory cache on mount
        #[arg(long)]
        vfs_refresh: bool,
        /// Case-insensitive file name matching
        #[arg(long)]
        vfs_case_insensitive: bool,
        #[arg(long)]
        no_implicit_dir: bool,
        /// Block Unicode normalization duplicates (NFC/NFD)
        #[arg(long)]
        vfs_block_norm_dupes: bool,
        /// Translate symlinks
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
        /// Write wait timeout in seconds (default: 5)
        #[arg(long, default_value = "5")]
        vfs_write_wait: u64,
        /// Read wait timeout in seconds (default: 5)
        #[arg(long, default_value = "5")]
        vfs_read_wait: u64,
        /// Cache poll interval in seconds (default: 60)
        #[arg(long, default_value = "60")]
        vfs_cache_poll_interval: u64,
        /// Time in seconds to keep file handles open after last close for reuse (0 to disable, default: 0)
        #[arg(long, default_value = "0")]
        vfs_handle_caching: u64,
        /// Total disk space to report in statfs (TB, default: 1024)
        #[arg(long, default_value = "1024")]
        vfs_disk_space_total_size: u64,
    },
    /// Unmount a mounted directory (use "all" to unmount all)
    Unmount { target: String },
    /// List active mounts
    List,
    /// Serve storage as read-only HTTP server
    Serve {
        storage: String,
        #[arg(long, default_value = "8080")]
        port: u16,
        #[arg(long = "opt", value_name = "KEY=VAL", num_args = 0..)]
        opt: Vec<String>,
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
            dir_cache_time,
            attr_timeout,
            type_cache_ttl,
            stat_cache_ttl,
            allow_other,
            volname,
            devname,
            write_back_cache,
            option,
            daemon,
            daemon_wait,
            daemon_timeout,
            allow_root,
            vfs_cache_max_size,
            mem_limit,
            vfs_write_back,
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
            hash_filter,
            mount_case_insensitive,
            max_read_ahead,
            vfs_read_chunk_size_limit,
            vfs_read_chunk_streams,
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
        } => {
            let opts: HashMap<String, String> = opt
                .iter()
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            mntrs::cmd::mount::mount(
                &storage,
                &mountpoint,
                &opts,
                read_only,
                dir_cache_time,
                attr_timeout,
                type_cache_ttl,
                stat_cache_ttl,
                allow_other,
                &volname,
                devname.as_deref(),
                write_back_cache,
                &option,
                daemon,
                daemon_wait,
                daemon_timeout,
                allow_root,
                vfs_cache_max_size,
                mem_limit,
                vfs_write_back,
                &vfs_cache_mode,
                vfs_read_ahead,
                vfs_read_chunk_size,
                default_permissions,
                uid,
                gid,
                umask,
                dir_perms,
                file_perms,
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
        Commands::Serve { storage, port, opt } => {
            let opts: HashMap<String, String> = opt
                .iter()
                .filter_map(|kv| kv.split_once("="))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            mntrs::cmd::serve::serve(&storage, &opts, port)?;
        }
        Commands::Install { action } => match action {
            Some(InstallAction::Systemd) | None => {
                mntrs::cmd::install::systemd()?;
            }
        },
    }
    Ok(())
}
