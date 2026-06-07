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
        /// Allow other users to access the mount
        #[arg(long)]
        allow_other: bool,
        /// Volume name (shown in mount table)
        #[arg(long, default_value = "mntrs")]
        volname: String,
        /// Enable write-back caching (kernel buffers writes before sending to mntrs)
        #[arg(long)]
        write_back_cache: bool,
    },
    /// Unmount a mounted directory (use "all" to unmount all)
    Unmount {
        target: String,
    },
    /// List active mounts
    List,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Mount { storage, mountpoint, opt, read_only, dir_cache_time, attr_timeout, allow_other, volname, write_back_cache } => {
            let opts: HashMap<String, String> = opt.iter()
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            mntrs::cmd::mount::mount(
                &storage, &mountpoint, &opts, read_only,
                dir_cache_time, attr_timeout, allow_other, &volname, write_back_cache,
            )?;
        }
        Commands::Unmount { target } => {
            mntrs::cmd::unmount::unmount(&target)?;
        }
        Commands::List => {
            mntrs::cmd::list::list()?;
        }
    }
    Ok(())
}
